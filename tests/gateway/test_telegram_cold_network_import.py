"""Telegram discovery imports must not block adapter startup or the Gateway loop."""

import asyncio
import importlib
import subprocess
import sys
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DISCOVERY_MODULE = "plugins.platforms.telegram.telegram_discovery"

_CHILD = f"""
import sys
import plugins.platforms.telegram.adapter

name = {DISCOVERY_MODULE!r}
assert name not in sys.modules, "cold adapter import pulled in " + name
"""


def test_cold_adapter_import_does_not_import_telegram_discovery():
    result = subprocess.run(
        [sys.executable, "-c", _CHILD],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, (
        "`import plugins.platforms.telegram.adapter` must not import "
        f"{DISCOVERY_MODULE} at module scope (exit {result.returncode})\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )


@pytest.mark.asyncio
async def test_cold_discovery_import_is_bounded_off_event_loop(monkeypatch):
    from gateway.config import PlatformConfig
    from plugins.platforms.telegram import adapter as tg_adapter
    from plugins.platforms.telegram import telegram_network

    adapter = tg_adapter.TelegramAdapter(PlatformConfig(enabled=True, token="test-token"))
    monkeypatch.setattr(adapter, "_fallback_ips", lambda: [])
    monkeypatch.setenv("HERMES_TELEGRAM_FALLBACK_DISCOVERY_TIMEOUT", "1.0")
    monkeypatch.setattr(tg_adapter, "resolve_proxy_url", lambda *args, **kwargs: None)
    monkeypatch.setattr(telegram_network, "_resolve_proxy_url", lambda *args, **kwargs: None)

    class RecordingRequest:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        async def do_request(self, *args, **kwargs):
            return None

    monkeypatch.setattr(tg_adapter, "HTTPXRequest", RecordingRequest)

    async def no_network_discovery(**_kwargs):
        return list(telegram_network.SEED_FALLBACK_IPS)

    monkeypatch.setattr(telegram_network, "discover_fallback_ips", no_network_discovery, raising=False)

    import_entered = threading.Event()
    import_released = threading.Event()
    original_import = importlib.import_module
    discovery_module = SimpleNamespace(discover_fallback_ips=no_network_discovery)

    def blocked_import(name, *args, **kwargs):
        if name == DISCOVERY_MODULE:
            import_entered.set()
            if not import_released.wait(timeout=5):
                raise AssertionError("Telegram discovery import was not released")
            return discovery_module
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(importlib, "import_module", blocked_import)

    heartbeats = 0
    heartbeat_stop = asyncio.Event()

    async def heartbeat():
        nonlocal heartbeats
        while not heartbeat_stop.is_set():
            await asyncio.sleep(0.01)
            heartbeats += 1

    heartbeat_task = asyncio.create_task(heartbeat())
    build_task = asyncio.create_task(adapter._build_ptb_requests())
    try:
        deadline = asyncio.get_running_loop().time() + 2
        while not import_entered.is_set() and not build_task.done() and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.01)
        assert import_entered.is_set(), "the discovery module import never started in the offloaded worker"
        heartbeats_before = heartbeats
        await asyncio.sleep(0.05)
        assert not build_task.done()
        assert heartbeats > heartbeats_before

        request, get_updates_request = await asyncio.wait_for(build_task, timeout=4)
        assert not import_released.is_set()
        for built_request in (request, get_updates_request):
            transport = built_request.kwargs["httpx_kwargs"]["transport"]
            assert isinstance(transport, telegram_network.TelegramFallbackTransport)
            assert transport._fallback_ips == list(telegram_network.SEED_FALLBACK_IPS)
    finally:
        import_released.set()
        heartbeat_stop.set()
        if not build_task.done():
            build_task.cancel()
        await asyncio.gather(build_task, heartbeat_task, return_exceptions=True)

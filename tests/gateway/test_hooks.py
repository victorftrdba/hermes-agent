"""Tests for gateway/hooks.py — event hook system."""

import asyncio
import contextvars
import threading
from contextlib import suppress
from unittest.mock import patch

import pytest

from gateway.hooks import HookRegistry


def _create_hook(hooks_dir, hook_name, events, handler_code):
    """Helper to create a hook directory with HOOK.yaml and handler.py."""
    hook_dir = hooks_dir / hook_name
    hook_dir.mkdir(parents=True)
    (hook_dir / "HOOK.yaml").write_text(
        f"name: {hook_name}\n"
        f"description: Test hook\n"
        f"events: {events}\n"
    )
    (hook_dir / "handler.py").write_text(handler_code)
    return hook_dir


class TestHookRegistryInit:
    def test_empty_registry(self):
        reg = HookRegistry()
        assert reg.loaded_hooks == []
        assert reg._handlers == {}


def _patch_no_builtins(reg):
    """Suppress built-in hook registration so tests only exercise user-hook discovery."""
    return patch.object(reg, "_register_builtin_hooks")


class _BlockingProbe:
    """Records the thread it ran on, then blocks until released."""

    def __init__(self):
        self.entered = threading.Event()
        self.release = threading.Event()
        self.thread_id = None

    def __call__(self, *args, **kwargs):
        self.thread_id = threading.get_ident()
        self.entered.set()
        self.release.wait(timeout=5)
        return None


async def _drain_until_entered(probe):
    while not probe.entered.is_set():
        await asyncio.sleep(0)


def _start_ticker():
    ticks = [0]

    async def _ticker():
        while True:
            ticks[0] += 1
            await asyncio.sleep(0)

    return asyncio.create_task(_ticker()), ticks


async def _cancel(task):
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


class TestDiscoverAndLoad:
    @pytest.mark.asyncio
    async def test_loads_valid_hook(self, tmp_path):
        _create_hook(tmp_path, "my-hook", '["agent:start"]',
                      "def handle(event_type, context):\n    pass\n")

        reg = HookRegistry()
        with patch("gateway.hooks.HOOKS_DIR", tmp_path), _patch_no_builtins(reg):
            await reg.discover_and_load()

        assert len(reg.loaded_hooks) == 1
        assert reg.loaded_hooks[0]["name"] == "my-hook"
        assert "agent:start" in reg.loaded_hooks[0]["events"]

    @pytest.mark.asyncio
    async def test_skips_no_events(self, tmp_path):
        hook_dir = tmp_path / "empty-hook"
        hook_dir.mkdir()
        (hook_dir / "HOOK.yaml").write_text("name: empty\nevents: []\n")
        (hook_dir / "handler.py").write_text("def handle(e, c): pass\n")

        reg = HookRegistry()
        with patch("gateway.hooks.HOOKS_DIR", tmp_path), _patch_no_builtins(reg):
            await reg.discover_and_load()

        assert len(reg.loaded_hooks) == 0


class TestDiscoveryDoesNotBlockLoop:
    @pytest.mark.asyncio
    async def test_discovery_runs_off_event_loop_thread(self, tmp_path):
        (tmp_path / "hook-a").mkdir()
        probe = _BlockingProbe()
        probe.release.set()
        loop_thread = threading.get_ident()

        reg = HookRegistry()
        with patch("gateway.hooks.HOOKS_DIR", tmp_path), \
             patch("gateway.hooks._load_hook_dir", probe), _patch_no_builtins(reg):
            await reg.discover_and_load()

        assert probe.thread_id is not None
        assert probe.thread_id != loop_thread

    @pytest.mark.asyncio
    async def test_discovery_keeps_event_loop_progress_while_blocked(self, tmp_path):
        (tmp_path / "hook-a").mkdir()
        probe = _BlockingProbe()
        loop_thread = threading.get_ident()

        reg = HookRegistry()
        ticker, ticks = _start_ticker()
        load_task = None
        try:
            with patch("gateway.hooks.HOOKS_DIR", tmp_path), \
                 patch("gateway.hooks._load_hook_dir", probe), _patch_no_builtins(reg):
                load_task = asyncio.create_task(reg.discover_and_load())
                await asyncio.wait_for(_drain_until_entered(probe), timeout=5)

                assert probe.thread_id != loop_thread
                before = ticks[0]
                for _ in range(10):
                    await asyncio.sleep(0)
                assert ticks[0] > before

                probe.release.set()
                await asyncio.wait_for(load_task, timeout=5)
        finally:
            probe.release.set()
            if load_task is not None:
                await _cancel(load_task)
            await _cancel(ticker)


class TestEmit:

    @pytest.mark.asyncio
    async def test_emit_calls_async_handler(self, tmp_path):
        results = []

        hook_dir = tmp_path / "async-hook"
        hook_dir.mkdir()
        (hook_dir / "HOOK.yaml").write_text(
            "name: async-hook\nevents: ['agent:end']\n"
        )
        (hook_dir / "handler.py").write_text(
            "import asyncio\n"
            "results = []\n"
            "async def handle(event_type, context):\n"
            "    results.append(event_type)\n"
        )

        reg = HookRegistry()
        with patch("gateway.hooks.HOOKS_DIR", tmp_path):
            await reg.discover_and_load()

        handler_fn = reg._handlers["agent:end"][0]
        handler_fn.__globals__["results"] = results

        await reg.emit("agent:end", {})
        assert "agent:end" in results

    @pytest.mark.asyncio
    async def test_wildcard_matching(self, tmp_path):
        results = []

        _create_hook(tmp_path, "wildcard-hook", '["command:*"]',
                      "results = []\n"
                      "def handle(event_type, context):\n"
                      "    results.append(event_type)\n")

        reg = HookRegistry()
        with patch("gateway.hooks.HOOKS_DIR", tmp_path):
            await reg.discover_and_load()

        handler_fn = reg._handlers["command:*"][0]
        handler_fn.__globals__["results"] = results

        await reg.emit("command:reset", {})
        assert "command:reset" in results


class TestSyncHandlersOffEventLoop:
    @pytest.mark.asyncio
    async def test_sync_handler_runs_off_event_loop_thread(self):
        seen = {}
        reg = HookRegistry()

        def handler(_event_type, _context):
            seen["thread_id"] = threading.get_ident()
            return "ok"

        reg._handlers["command:x"] = [handler]
        loop_thread = threading.get_ident()

        assert await reg.emit_collect("command:x", {}) == ["ok"]
        assert seen["thread_id"] != loop_thread

    @pytest.mark.asyncio
    async def test_async_handler_stays_on_event_loop_thread(self):
        seen = {}
        reg = HookRegistry()

        async def handler(_event_type, _context):
            seen["thread_id"] = threading.get_ident()
            return "async-ok"

        reg._handlers["command:y"] = [handler]
        loop_thread = threading.get_ident()

        assert await reg.emit_collect("command:y", {}) == ["async-ok"]
        assert seen["thread_id"] == loop_thread

    @pytest.mark.asyncio
    async def test_blocked_sync_handler_keeps_event_loop_progress(self):
        probe = _BlockingProbe()
        reg = HookRegistry()
        reg._handlers["command:block"] = [probe]
        loop_thread = threading.get_ident()

        ticker, ticks = _start_ticker()
        emit_task = asyncio.create_task(reg.emit_collect("command:block", {}))
        try:
            await asyncio.wait_for(_drain_until_entered(probe), timeout=5)

            assert probe.thread_id != loop_thread
            before = ticks[0]
            for _ in range(10):
                await asyncio.sleep(0)
            assert ticks[0] > before

            probe.release.set()
            assert await asyncio.wait_for(emit_task, timeout=5) == []
        finally:
            probe.release.set()
            await _cancel(emit_task)
            await _cancel(ticker)

    @pytest.mark.asyncio
    async def test_preserves_registration_order_across_sync_and_async(self):
        order = []
        reg = HookRegistry()

        def sync_first(_event_type, _context):
            order.append("sync_first")
            return 1

        async def async_middle(_event_type, _context):
            order.append("async_middle")
            return 2

        def sync_last(_event_type, _context):
            order.append("sync_last")
            return 3

        reg._handlers["command:order"] = [sync_first, async_middle, sync_last]

        assert await reg.emit_collect("command:order", {}) == [1, 2, 3]
        assert order == ["sync_first", "async_middle", "sync_last"]

    @pytest.mark.asyncio
    async def test_contextvar_propagates_into_sync_handler_thread(self):
        var = contextvars.ContextVar("hook_test_var")
        seen = {}
        reg = HookRegistry()

        def handler(_event_type, _context):
            seen["value"] = var.get(None)

        reg._handlers["command:ctx"] = [handler]
        token = var.set("propagated")
        try:
            await reg.emit_collect("command:ctx", {})
        finally:
            var.reset(token)

        assert seen["value"] == "propagated"

    @pytest.mark.asyncio
    async def test_sync_handler_exception_is_nonfatal_and_preserves_order(self):
        order = []
        reg = HookRegistry()

        def broken(_event_type, _context):
            order.append("broken")
            raise RuntimeError("boom")

        def healthy(_event_type, _context):
            order.append("healthy")
            return "healthy"

        reg._handlers["command:err"] = [broken, healthy]

        assert await reg.emit_collect("command:err", {}) == ["healthy"]
        assert order == ["broken", "healthy"]


class TestEmitCollect:
    """Tests for emit_collect() — returns handler return values for decision-style hooks."""

    @pytest.mark.asyncio
    async def test_collects_sync_return_values(self):
        reg = HookRegistry()
        reg._handlers["command:status"] = [
            lambda _e, _c: {"decision": "allow"},
            lambda _e, _c: {"decision": "deny", "message": "nope"},
        ]

        results = await reg.emit_collect("command:status", {})

        assert results == [
            {"decision": "allow"},
            {"decision": "deny", "message": "nope"},
        ]


    @pytest.mark.asyncio
    async def test_drops_none_return_values(self):
        reg = HookRegistry()
        reg._handlers["command:x"] = [
            lambda _e, _c: None,  # fire-and-forget, returns nothing
            lambda _e, _c: {"decision": "deny"},
            lambda _e, _c: None,
        ]

        results = await reg.emit_collect("command:x", {})

        assert results == [{"decision": "deny"}]

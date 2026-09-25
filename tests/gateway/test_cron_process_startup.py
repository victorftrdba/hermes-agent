import asyncio
from types import SimpleNamespace

import pytest

import cron.scheduler_process as scheduler_process
import cron.scheduler_provider as scheduler_provider
import gateway.run as gateway_run


def _wait_for_stop(stop_event, **_kwargs):
    stop_event.wait()


@pytest.mark.asyncio
async def test_builtin_gateway_uses_spawn_manager_without_ticker_thread(tmp_path, monkeypatch):
    instances = []

    class FakeManager:
        process_isolated = True

        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.started = False
            instances.append(self)

        def start(self):
            self.started = True

    monkeypatch.setattr(scheduler_process, "CronProcessManager", FakeManager)
    monkeypatch.setattr(
        scheduler_provider, "resolve_cron_scheduler", lambda: scheduler_provider.InProcessCronScheduler()
    )
    monkeypatch.setattr(gateway_run, "get_hermes_home", lambda: tmp_path)
    monkeypatch.setattr(gateway_run, "_start_gateway_housekeeping", _wait_for_stop)
    runner = SimpleNamespace(
        config=SimpleNamespace(multiplex_profiles=False),
        adapters={},
        _profile_adapters={},
    )

    stop_event, provider, cron_thread, housekeeping_thread = (
        gateway_run._start_gateway_start_cron_and_housekeeping(runner)
    )
    try:
        assert provider is instances[0]
        assert provider.started is True
        assert provider.kwargs["profile_homes"] == [str(tmp_path)]
        assert provider.kwargs["default_profile"] == str(tmp_path)
        assert provider.kwargs["default_profile_name"] == "default"
        assert cron_thread is None
        assert runner._cron_process_manager is provider
    finally:
        stop_event.set()
        await asyncio.to_thread(housekeeping_thread.join, 1)


@pytest.mark.asyncio
async def test_external_provider_keeps_provider_thread_contract(monkeypatch):
    class ExternalProvider:
        name = "external"

        def start(self, stop_event, **kwargs):
            self.kwargs = kwargs
            stop_event.wait()

    provider = ExternalProvider()
    monkeypatch.setattr(scheduler_provider, "resolve_cron_scheduler", lambda: provider)
    monkeypatch.setattr(
        scheduler_provider, "scheduler_for_profile_mode", lambda current, **_kwargs: current
    )
    monkeypatch.setattr(gateway_run, "_start_gateway_housekeeping", _wait_for_stop)
    runner = SimpleNamespace(
        config=SimpleNamespace(multiplex_profiles=False),
        adapters={},
        _profile_adapters={},
        _cron_process_manager=None,
    )

    stop_event, active_provider, cron_thread, housekeeping_thread = (
        gateway_run._start_gateway_start_cron_and_housekeeping(runner)
    )
    try:
        assert active_provider is provider
        assert cron_thread is not None
        assert runner._cron_process_manager is None
    finally:
        stop_event.set()
        await asyncio.to_thread(cron_thread.join, 1)
        await asyncio.to_thread(housekeeping_thread.join, 1)


@pytest.mark.asyncio
async def test_shutdown_closes_and_clears_cron_process_manager(monkeypatch):
    calls = []

    class FakeManager:
        process_isolated = True

        def close(self):
            calls.append("close")

    class DeadThread:
        def is_alive(self):
            return False

        def join(self, timeout=None):
            calls.append(("join", timeout))

    class StopEvent:
        def set(self):
            calls.append("set")

    async def no_mcp_shutdown():
        return True

    provider = FakeManager()
    runner = SimpleNamespace(_cron_process_manager=provider)
    monkeypatch.setattr(gateway_run, "_best_effort", lambda *_args: None)
    monkeypatch.setattr(gateway_run, "_exit_with_failure_verdict", lambda _runner: False)
    monkeypatch.setattr(gateway_run, "_resolve_gateway_exit_verdict", lambda *_args: True)
    monkeypatch.setattr(gateway_run, "_shutdown_mcp_servers_nonblocking", no_mcp_shutdown)

    result = await gateway_run._start_gateway_shutdown_tail(
        runner,
        None,
        StopEvent(),
        provider,
        None,
        DeadThread(),
        StopEvent(),
        DeadThread(),
        [False],
    )

    assert result is True
    assert calls.count("close") == 1
    assert runner._cron_process_manager is None

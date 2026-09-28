"""Regression tests for parallel platform connect at gateway startup (#83791).

The old ``GatewayRunner.start()`` loop awaited each platform's connect()
(including its own timeout) in turn. A single slow/failing platform (e.g.
Telegram behind a dead proxy) therefore delayed every later platform's
connect by a full timeout window, cascading one platform's failure onto
WeChat/QQ/etc. These tests prove the connects now run concurrently.

Why event-order, not wall-clock timings
---------------------------------------
An earlier version of this test recorded ``time.monotonic()`` around each
connect() and asserted ``slow_start < fast_end``. That assertion is true in
BOTH the serial and the parallel world, so it proved nothing:

  serial:   slow_start=0, slow_end=0.300, fast_start=0.300, fast_end=0.300
            -> 0 < 0.300  (passes, but it's serial!)
  parallel: slow_start=0, fast_start=0,    fast_end=0.001, slow_end=0.300
            -> 0 < 0.001  (passes)

The only assertion that distinguishes them is ``fast_end`` occurring *before*
``slow_end`` (true only when the two connects overlap). We record the
connect start/end events in arrival order, which is fully independent of clock
resolution -- ``time.monotonic()`` has only ~15 ms resolution on Windows
(GetTickCount64), so parallel connects can land on the same tick and defeat any
wall-clock comparison. Event ordering cannot be defeated by a coarse clock.
"""

import asyncio
import threading

import pytest

from gateway.config import GatewayConfig, Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter
from gateway.run import GatewayRunner


class _OrderRecorder:
    """Collects connect start/end events in arrival order (clock-agnostic)."""

    events: list = []

    @classmethod
    def reset(cls) -> None:
        cls.events = []

    @classmethod
    def index_of(cls, platform_value: str, kind: str) -> int:
        for i, (name, evt) in enumerate(cls.events):
            if name == platform_value and evt == kind:
                return i
        return -1


class _TimingAdapter(BasePlatformAdapter):
    """Adapter whose ``connect()`` records an event and sleeps.

    Used to prove the startup connect loop launches every platform's
    connect() concurrently rather than serially.
    """

    def __init__(self, platform: Platform, sleep: float):
        super().__init__(PlatformConfig(enabled=True, token="***"), platform)
        self._sleep = sleep

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        _OrderRecorder.events.append((self.platform.value, "start"))
        await asyncio.sleep(self._sleep)
        _OrderRecorder.events.append((self.platform.value, "end"))
        return True

    async def disconnect(self) -> None:
        self._mark_disconnected()

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        raise NotImplementedError

    async def get_chat_info(self, chat_id):
        return {"id": chat_id}


@pytest.mark.asyncio
async def test_startup_connects_platforms_concurrently(monkeypatch, tmp_path):
    """A slow platform must not block a later platform at startup (#83791).

    "slow" (Telegram) is listed first so a serial loop would fully block
    "fast" (Discord). We prove the connect calls overlap by recording the
    order in which connects finish: under a serial loop the slow platform's
    connect ends *before* the fast one even begins, so the fast platform's
    end can never precede the slow platform's end. Only parallel execution
    puts ``fast_end`` before ``slow_end``.
    """
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    _OrderRecorder.reset()

    config = GatewayConfig(
        platforms={
            Platform.TELEGRAM: PlatformConfig(enabled=True, token="***"),
            Platform.DISCORD: PlatformConfig(enabled=True, token="***"),
        },
        sessions_dir=tmp_path / "sessions",
    )
    runner = GatewayRunner(config)

    def _make_adapter(platform, platform_config):
        sleep = 0.3 if platform is Platform.TELEGRAM else 0.0
        return _TimingAdapter(platform, sleep)

    monkeypatch.setattr(runner, "_create_adapter", _make_adapter)
    # Keep the rest of startup lightweight / non-fatal.
    monkeypatch.setattr(runner, "_start_secondary_profile_adapters", lambda: 0)

    await runner.start()

    events = _OrderRecorder.events
    assert events, "no connect() event was recorded"

    fast_end = _OrderRecorder.index_of(Platform.DISCORD.value, "end")
    slow_end = _OrderRecorder.index_of(Platform.TELEGRAM.value, "end")
    assert fast_end != -1 and slow_end != -1, f"missing end events: {events}"

    # Overlap proof: the fast platform finished before the slow one did,
    # which is only possible if the two connects ran at the same time.
    assert fast_end < slow_end, (
        f"connects did not overlap (serial loop?): events={events}"
    )
    # Both platforms should be registered once startup settles.
    assert Platform.TELEGRAM in runner.adapters
    assert Platform.DISCORD in runner.adapters


@pytest.mark.asyncio
async def test_startup_one_failing_platform_does_not_block_others(monkeypatch, tmp_path):
    """A failing/slow platform must not prevent others from connecting (#83791).

    Mirrors the reported Windows symptom: Telegram (dead proxy) must not keep
    WeChat/QQ offline. Here Telegram fails (returns False after a sleep) while
    Discord connects successfully and is registered.
    """

    class _FailingSlowAdapter(BasePlatformAdapter):
        def __init__(self):
            super().__init__(PlatformConfig(enabled=True, token="***"), Platform.TELEGRAM)

        async def connect(self, *, is_reconnect: bool = False) -> bool:
            await asyncio.sleep(0.3)
            self._set_fatal_error("telegram_proxy_dead", "proxy unreachable", retryable=True)
            return False

        async def disconnect(self) -> None:
            self._mark_disconnected()

        async def send(self, chat_id, content, reply_to=None, metadata=None):
            raise NotImplementedError

        async def get_chat_info(self, chat_id):
            return {"id": chat_id}

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    _OrderRecorder.reset()

    config = GatewayConfig(
        platforms={
            Platform.TELEGRAM: PlatformConfig(enabled=True, token="***"),
            Platform.DISCORD: PlatformConfig(enabled=True, token="***"),
        },
        sessions_dir=tmp_path / "sessions",
    )
    runner = GatewayRunner(config)

    def _make_adapter(platform, platform_config):
        if platform is Platform.TELEGRAM:
            return _FailingSlowAdapter()
        return _TimingAdapter(platform, 0.0)

    monkeypatch.setattr(runner, "_create_adapter", _make_adapter)
    monkeypatch.setattr(runner, "_start_secondary_profile_adapters", lambda: 0)

    await runner.start()

    # The healthy platform connected and is registered despite Telegram failing.
    assert Platform.DISCORD in runner.adapters
    # The failed platform is queued for retry, not silently dropped.
    assert Platform.TELEGRAM in runner._failed_platforms


class TestTelegramColdStartCap:
    """The initial (pre-`running`) Telegram connect uses a capped budget (#85993).

    The full 180s Telegram connect budget (#67498) still applies to reconnect
    watcher retries; only the cold-start attempt awaited before the gateway
    reaches `running` is capped, so an unreachable Telegram can't hold every
    other platform's serving state hostage for 3 minutes.
    """

    def _runner(self, tmp_path):
        config = GatewayConfig(
            platforms={}, sessions_dir=tmp_path / "sessions"
        )
        return GatewayRunner(config)

    def test_initial_telegram_budget_is_capped(self, tmp_path, monkeypatch):
        monkeypatch.delenv("HERMES_GATEWAY_PLATFORM_CONNECT_TIMEOUT", raising=False)
        runner = self._runner(tmp_path)
        initial = runner._platform_connect_timeout_secs(
            Platform.TELEGRAM, initial=True
        )
        full = runner._platform_connect_timeout_secs(Platform.TELEGRAM)
        assert initial < full, (
            "cold-start Telegram budget must be shorter than the reconnect "
            f"budget (initial={initial}, full={full})"
        )
        assert full == 180.0  # #67498 reconnect budget unchanged
        assert initial <= 60.0  # gateway reaches `running` within a minute

    def test_other_platforms_unchanged(self, tmp_path, monkeypatch):
        monkeypatch.delenv("HERMES_GATEWAY_PLATFORM_CONNECT_TIMEOUT", raising=False)
        runner = self._runner(tmp_path)
        assert runner._platform_connect_timeout_secs(
            Platform.DISCORD, initial=True
        ) == runner._platform_connect_timeout_secs(Platform.DISCORD)

    def test_env_override_applies_to_initial(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HERMES_GATEWAY_PLATFORM_CONNECT_TIMEOUT", "12")
        runner = self._runner(tmp_path)
        assert runner._platform_connect_timeout_secs(
            Platform.TELEGRAM, initial=True
        ) == 12.0

    @pytest.mark.asyncio
    async def test_initial_connect_times_out_at_cap_and_queues_retry(
        self, tmp_path, monkeypatch
    ):
        """A wedged Telegram connect is abandoned at the capped budget and the
        platform lands in the reconnect queue instead of blocking startup."""
        monkeypatch.setenv("HERMES_HOME", str(tmp_path))
        monkeypatch.delenv("HERMES_GATEWAY_PLATFORM_CONNECT_TIMEOUT", raising=False)

        class _WedgedAdapter(BasePlatformAdapter):
            def __init__(self):
                super().__init__(
                    PlatformConfig(enabled=True, token="***"), Platform.TELEGRAM
                )

            async def connect(self, *, is_reconnect: bool = False) -> bool:
                await asyncio.sleep(3600)
                return True

            async def disconnect(self) -> None:
                self._mark_disconnected()

            async def send(self, chat_id, content, reply_to=None, metadata=None):
                raise NotImplementedError

            async def get_chat_info(self, chat_id):
                return {"id": chat_id}

        config = GatewayConfig(
            platforms={
                Platform.TELEGRAM: PlatformConfig(enabled=True, token="***"),
                Platform.DISCORD: PlatformConfig(enabled=True, token="***"),
            },
            sessions_dir=tmp_path / "sessions",
        )
        runner = GatewayRunner(config)

        # Shrink the capped budget so the test is fast; the assertion is that
        # the INITIAL path (initial=True) is the one that fires, not the 180s
        # reconnect budget.
        import gateway.run as gateway_run

        monkeypatch.setattr(
            gateway_run, "_TELEGRAM_INITIAL_CONNECT_TIMEOUT_SECS_DEFAULT", 0.2
        )

        def _make_adapter(platform, platform_config):
            if platform is Platform.TELEGRAM:
                return _WedgedAdapter()
            return _TimingAdapter(platform, 0.0)

        monkeypatch.setattr(runner, "_create_adapter", _make_adapter)
        monkeypatch.setattr(runner, "_start_secondary_profile_adapters", lambda: 0)

        await asyncio.wait_for(runner.start(), timeout=30)

        # Discord served; Telegram queued for the watcher's full-budget retry.
        assert Platform.DISCORD in runner.adapters
        assert Platform.TELEGRAM not in runner.adapters
        assert Platform.TELEGRAM in runner._failed_platforms


@pytest.mark.asyncio
async def test_connecting_status_write_runs_off_event_loop(monkeypatch, tmp_path):
    """``_connect_one_startup``'s connecting status write must not run on the loop.

    The write is a synchronous read-modify-write against ``gateway_state.json``; running
    it inline would stall every concurrent platform connect (the #83791 regression
    class). The sync helper is parked on a ``threading.Event``, so an asyncio probe can
    only advance if the write left the loop.
    """
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    _OrderRecorder.reset()

    config = GatewayConfig(
        platforms={Platform.TELEGRAM: PlatformConfig(enabled=True, token="***")},
        sessions_dir=tmp_path / "sessions",
    )
    runner = GatewayRunner(config)

    writer_entered = threading.Event()
    writer_release = threading.Event()

    def _blocking_status_write(platform, **kwargs):
        writer_entered.set()
        assert writer_release.wait(timeout=30), "test never released the runtime-status write"

    monkeypatch.setattr(runner, "_update_platform_runtime_status", _blocking_status_write)

    adapter = _TimingAdapter(Platform.TELEGRAM, 0.0)
    probe_ran = asyncio.Event()

    async def _probe():
        await asyncio.to_thread(writer_entered.wait, 30)
        probe_ran.set()

    probe_task = asyncio.ensure_future(_probe())
    connect_task = asyncio.ensure_future(
        runner._start_connect_pending(
            [(Platform.TELEGRAM, config.platforms[Platform.TELEGRAM], adapter)]
        )
    )
    try:
        await asyncio.wait_for(probe_ran.wait(), timeout=10)
        assert writer_entered.is_set(), "status write never started"
        assert not connect_task.done(), "connect finished while the status writer was blocked"
    finally:
        writer_release.set()

    await probe_task
    results = await asyncio.wait_for(connect_task, timeout=30)

    assert results is not None, "connect loop aborted unexpectedly"
    assert results[0][3] == "ok"
    assert _OrderRecorder.index_of(Platform.TELEGRAM.value, "end") != -1


@pytest.mark.asyncio
async def test_async_platform_status_updates_complete_off_loop_and_retain_entries(
    monkeypatch, tmp_path
):
    """Concurrent async status updates both land off-loop and persist every platform entry.

    The sync write helper is observed through a wrapper while the real read-modify-write
    runs against the temp HERMES_HOME. Serialization comes from the RLock inside
    ``gateway.status.write_runtime_status``, so both platform entries must survive.
    """
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))

    import gateway.run as gateway_run
    from gateway.status import read_runtime_status

    config = GatewayConfig(platforms={}, sessions_dir=tmp_path / "sessions")
    runner = GatewayRunner(config)

    loop_thread = threading.get_ident()
    real_quiet_write = gateway_run._write_runtime_status_quiet
    writer_threads: list = []
    written_platforms: list = []
    calls_guard = threading.Lock()

    def _tracking_quiet_write(**fields):
        with calls_guard:
            writer_threads.append(threading.get_ident())
            written_platforms.append(str(fields.get("platform")))
        real_quiet_write(**fields)

    monkeypatch.setattr(gateway_run, "_write_runtime_status_quiet", _tracking_quiet_write)

    await asyncio.gather(
        runner._update_platform_runtime_status_async("telegram", platform_state="connecting"),
        runner._update_platform_runtime_status_async("discord", platform_state="connecting"),
    )

    assert sorted(written_platforms) == ["discord", "telegram"]
    assert all(tid != loop_thread for tid in writer_threads), "status write ran on the event loop"
    record = read_runtime_status()
    assert record is not None
    assert record["platforms"]["telegram"]["state"] == "connecting"
    assert record["platforms"]["discord"]["state"] == "connecting"

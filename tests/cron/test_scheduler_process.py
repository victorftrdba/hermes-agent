import os
import threading
import time
from types import SimpleNamespace

import pytest

import cron.scheduler_delivery as scheduler_delivery
import cron.scheduler_preflight as scheduler_preflight
import cron.scheduler_provider as scheduler_provider
from cron.scheduler_process import CronProcessManager, _scheduler_child_main
from gateway.config import Platform
from gateway.profile_routing import ProfileRoute


def _wait_until(predicate, timeout=15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return False


def test_spawned_scheduler_admits_fire_and_reaps(tmp_path):
    manager = CronProcessManager(
        profile_homes=[str(tmp_path)],
        default_profile=str(tmp_path),
        adapters={},
        loop=None,
        interval=3600,
        heartbeat_seconds=0.1,
        heartbeat_timeout=3.0,
        request_timeout=3.0,
        stop_timeout=1.0,
    )

    manager.start()
    try:
        assert _wait_until(lambda: manager.status.ready), manager.status
        first_pid = manager.status.pid
        assert first_pid != os.getpid()
        assert manager.fire("missing", profile_home=str(tmp_path))["status"] == "duplicate"
        manager.set_dispatch_enabled(False)
        assert manager.fire("missing", profile_home=str(tmp_path))["status"] == "error"
        manager.set_dispatch_enabled(True)

        manager._process.terminate()
        assert _wait_until(
            lambda: manager.status.ready and manager.status.pid not in (None, first_pid)
        ), manager.status
        assert manager.status.restart_count >= 1
    finally:
        manager.close()

    assert manager.status.pid is None
    assert manager.status.degraded is True


def test_child_scheduler_receives_logical_default_profile_name(tmp_path, monkeypatch):
    captured = {}
    started = threading.Event()

    def fake_start(_provider, stop_event, **kwargs):
        captured.update(kwargs)
        started.set()
        stop_event.wait(1)

    class FakeConnection:
        def send(self, _message):
            return None

        def poll(self, timeout):
            return started.wait(timeout)

        def recv(self):
            return {'type': 'stop'}

        def close(self):
            return None

    monkeypatch.setattr(scheduler_provider.InProcessCronScheduler, 'start', fake_start)
    _scheduler_child_main(
        FakeConnection(), [('default', tmp_path)], str(tmp_path), 'default', 60, 1.0, True)

    assert captured['default_profile'] == 'default'
    assert captured['default_profile'] != str(tmp_path)


def _multiplex_manager(tmp_path, *, profile_adapters):
    default_home = tmp_path / 'default'
    secondary_home = tmp_path / 'secondary'
    primary = {Platform.TELEGRAM: object()}
    manager = CronProcessManager(
        profile_homes=[('default', default_home), ('secondary', secondary_home)],
        default_profile=str(default_home),
        default_profile_name='default',
        adapters=primary,
        profile_adapters=profile_adapters,
        loop=None,
    )
    return manager, primary, default_home, secondary_home


def test_multiplex_transport_routes_default_and_exact_secondary(tmp_path):
    secondary = {Platform.TELEGRAM: object()}
    manager, primary, default_home, secondary_home = _multiplex_manager(
        tmp_path, profile_adapters={'secondary': secondary})

    assert manager._transport_adapters(str(default_home)) is primary
    assert manager._transport_adapters(str(secondary_home)) is secondary


def test_multiplex_transport_empty_secondary_uses_exact_primary_routes(tmp_path, monkeypatch):
    manager, primary, _default_home, secondary_home = _multiplex_manager(
        tmp_path, profile_adapters={'secondary': {}})
    route = ProfileRoute(
        name='secondary-chat', platform='telegram', profile='secondary', chat_id='chat-1')
    monkeypatch.setattr(
        scheduler_preflight, '_primary_profile_routes_for_current_home', lambda: [route])

    adapters = manager._transport_adapters(str(secondary_home))

    assert isinstance(adapters, scheduler_preflight.SharedRouteAdapters)
    assert adapters.get(Platform.TELEGRAM, {'chat_id': 'chat-1'}) is primary[Platform.TELEGRAM]
    assert adapters.get(Platform.TELEGRAM, {'chat_id': 'other'}) is None


def test_multiplex_transport_missing_secondary_fails_closed(tmp_path):
    manager, _primary, _default_home, secondary_home = _multiplex_manager(
        tmp_path, profile_adapters={})

    with pytest.raises(RuntimeError, match='adapters unavailable for profile: secondary'):
        manager._transport_adapters(str(secondary_home))


def test_transport_parent_returns_plain_persistence_action_without_writing(monkeypatch):
    target = SimpleNamespace(
        job={'id': 'job-1', 'name': 'Daily'},
        platform_name='slack',
        chat_id='C1',
        thread_id=None,
        origin={'chat_name': 'Ops', 'scope_id': 'T1'},
        origin_user_id='U1',
        is_dm_target=False,
        mirror_text='brief',
        mirror_this_target=True,
        in_channel_surface=True,
        inchannel_continuable=True,
        opened_thread_id=None,
        runtime_adapter=object(),
        where='slack:C1',
        is_relay=False,
    )
    monkeypatch.setattr(scheduler_delivery, '_live_route_metadata', lambda _target: (None, {}, {}))
    monkeypatch.setattr(
        scheduler_delivery, '_live_send_text',
        lambda *_args, **_kwargs: (True, False, 'message-123'))
    monkeypatch.setattr(
        scheduler_delivery, '_seed_live_delivery_sessions',
        lambda *_args: pytest.fail('gateway parent persisted cron session state'))
    actions = []

    delivered = scheduler_delivery._deliver_via_live_adapter(
        target, 'brief', [], target_errors=[], delivery_errors=[], unverified_targets=[],
        persist_context=False, persistence_actions=actions)

    assert delivered is True
    assert actions == [{
        'type': 'delivery_context',
        'lane': 'live',
        'job': {'id': 'job-1', 'name': 'Daily'},
        'platform_name': 'slack',
        'chat_id': 'C1',
        'thread_id': None,
        'origin': {'chat_name': 'Ops', 'scope_id': 'T1'},
        'origin_user_id': 'U1',
        'is_dm_target': False,
        'mirror_text': 'brief',
        'mirror_this_target': True,
        'in_channel_surface': True,
        'inchannel_continuable': True,
        'opened_thread_id': None,
        'delivered_message_id': 'message-123',
    }]


def test_child_persistence_handler_keeps_delivery_continuation_metadata(monkeypatch):
    action = {
        'type': 'delivery_context',
        'lane': 'live',
        'job': {'id': 'job-1', 'name': 'Daily'},
        'platform_name': 'slack',
        'chat_id': 'C1',
        'thread_id': None,
        'origin': {'chat_name': 'Ops', 'scope_id': 'T1'},
        'origin_user_id': 'U1',
        'is_dm_target': False,
        'mirror_text': 'brief',
        'mirror_this_target': True,
        'in_channel_surface': True,
        'inchannel_continuable': True,
        'opened_thread_id': None,
        'delivered_message_id': 'message-123',
    }
    store = object()
    calls = []

    def seed_channel(job, adapter, platform, chat_id, text, **kwargs):
        calls.append(('channel', adapter._session_store, job, platform, chat_id, text, kwargs))
        return True

    def seed_thread(job, adapter, platform, chat_id, thread_id, text, **kwargs):
        calls.append(
            ('thread', adapter._session_store, job, platform, chat_id, thread_id, text, kwargs))

    monkeypatch.setattr(scheduler_delivery, '_seed_cron_channel_session', seed_channel)
    monkeypatch.setattr(scheduler_delivery, '_seed_cron_thread_session', seed_thread)
    monkeypatch.setattr(
        scheduler_delivery, '_maybe_mirror_cron_delivery',
        lambda *_args, **kwargs: calls.append(('mirror', kwargs['enabled'])))

    scheduler_delivery._apply_delivery_persistence(action, store)

    assert calls[0][0:6] == ('channel', store, action['job'], 'slack', 'C1', 'brief')
    assert calls[0][6]['user_id'] == 'U1'
    assert calls[0][6]['scope_id'] == 'T1'
    assert calls[1][0:7] == (
        'thread', store, action['job'], 'slack', 'C1', 'message-123', 'brief')
    assert calls[2] == ('mirror', False)


def test_child_persistence_handler_rejects_malformed_action(monkeypatch):
    monkeypatch.setattr(
        scheduler_delivery,
        '_seed_cron_channel_session',
        lambda *_args, **_kwargs: pytest.fail('malformed action reached channel persistence'),
    )
    monkeypatch.setattr(
        scheduler_delivery,
        '_seed_cron_thread_session',
        lambda *_args, **_kwargs: pytest.fail('malformed action reached thread persistence'),
    )
    monkeypatch.setattr(
        scheduler_delivery,
        '_maybe_mirror_cron_delivery',
        lambda *_args, **_kwargs: pytest.fail('malformed action reached mirror persistence'),
    )

    scheduler_delivery._apply_delivery_persistence({
        'type': 'delivery_context',
        'lane': 'live',
        'job': {'id': 123, 'name': 'Daily'},
        'platform_name': 'slack',
        'chat_id': 'C1',
        'mirror_text': 'brief',
    }, object())

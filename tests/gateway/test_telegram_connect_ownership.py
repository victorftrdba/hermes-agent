import asyncio
import gc
import weakref
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import PlatformConfig
from plugins.platforms.telegram import adapter as tg_adapter
from plugins.platforms.telegram.adapter import TelegramAdapter


class _Builder:
    def __init__(self, app):
        self.app = app

    def token(self, _token):
        return self

    def request(self, _request):
        return self

    def get_updates_request(self, _request):
        return self

    def build(self):
        return self.app


def _app(start_polling):
    stop_event = asyncio.Event()
    updater = SimpleNamespace(
        start_polling=AsyncMock(side_effect=start_polling),
        stop=AsyncMock(),
        running=True,
    )
    setattr(updater, '_Updater__polling_task_stop_event', stop_event)
    return SimpleNamespace(
        bot=SimpleNamespace(username='lease-test-bot'),
        updater=updater,
        start=AsyncMock(),
        stop=AsyncMock(),
        shutdown=AsyncMock(),
        running=True,
    ), stop_event


def test_connect_ownership_finalizer_prunes_only_its_epoch():
    orphan = TelegramAdapter(PlatformConfig(enabled=True, token='gc-distinct-token'))
    orphan_app = object()
    orphan_epoch = orphan._claim_connect_ownership(orphan_app)
    orphan_key = orphan._connect_ownership_key
    orphan_ref = weakref.ref(orphan)
    assert orphan_key is not None
    assert tg_adapter._TELEGRAM_CONNECT_OWNERS[orphan_key].epoch == orphan_epoch

    del orphan
    gc.collect()

    assert orphan_ref() is None
    assert orphan_key not in tg_adapter._TELEGRAM_CONNECT_OWNERS

    predecessor = TelegramAdapter(PlatformConfig(enabled=True, token='gc-shared-token'))
    successor = TelegramAdapter(PlatformConfig(enabled=True, token='gc-shared-token'))
    predecessor_app = object()
    successor_app = object()
    predecessor._claim_connect_ownership(predecessor_app)
    shared_key = predecessor._connect_ownership_key
    assert shared_key is not None
    predecessor_owner_ref = tg_adapter._TELEGRAM_CONNECT_OWNERS[shared_key].owner_ref
    assert predecessor_owner_ref() is predecessor
    successor_epoch = successor._claim_connect_ownership(successor_app)

    del predecessor
    gc.collect()

    ownership = tg_adapter._TELEGRAM_CONNECT_OWNERS[shared_key]
    assert predecessor_owner_ref() is None
    assert ownership.owner_ref() is successor
    assert ownership.epoch == successor_epoch
    assert successor._release_connect_ownership(successor_epoch, successor_app)


@pytest.mark.asyncio
async def test_successor_fences_cancellation_resistant_connect(monkeypatch):
    old_started = asyncio.Event()
    release_old = asyncio.Event()

    async def old_start_polling(**_kwargs):
        old_started.set()
        while not release_old.is_set():
            try:
                await release_old.wait()
            except asyncio.CancelledError:
                continue

    async def new_start_polling(**_kwargs):
        return None

    old_app, old_stop_event = _app(old_start_polling)
    new_app, _new_stop_event = _app(new_start_polling)
    builders = iter((_Builder(old_app), _Builder(new_app)))
    monkeypatch.setattr(
        tg_adapter,
        'Application',
        SimpleNamespace(builder=lambda: next(builders)),
    )
    monkeypatch.setattr(
        TelegramAdapter,
        '_build_ptb_requests',
        AsyncMock(return_value=(MagicMock(), MagicMock())),
    )
    monkeypatch.setattr(TelegramAdapter, '_initialize_app_with_retries', AsyncMock())
    monkeypatch.setattr(TelegramAdapter, '_wire_plugin_handlers', MagicMock())
    monkeypatch.setattr(TelegramAdapter, '_register_handlers', MagicMock())
    monkeypatch.setattr(TelegramAdapter, '_acquire_platform_lock', lambda *_args: True)
    monkeypatch.setattr(TelegramAdapter, '_start_post_connect_housekeeping', MagicMock())

    async def start_polling_mode(adapter, *, is_reconnect):
        await adapter._app.updater.start_polling()

    monkeypatch.setattr(TelegramAdapter, '_start_polling_mode', start_polling_mode)

    old = TelegramAdapter(PlatformConfig(enabled=True, token='same-token'))
    replacement = TelegramAdapter(PlatformConfig(enabled=True, token='same-token'))
    old_release = MagicMock()
    replacement_release = MagicMock()
    monkeypatch.setattr(old, '_release_platform_lock', old_release)
    monkeypatch.setattr(replacement, '_release_platform_lock', replacement_release)

    old_connect = asyncio.create_task(old.connect())
    await asyncio.wait_for(old_started.wait(), timeout=1)
    old_connect.cancel()
    await asyncio.sleep(0)
    assert not old_connect.done()

    assert await replacement.connect() is True
    assert old_stop_event.is_set()
    assert replacement.is_connected
    assert not old.is_connected

    release_old.set()
    assert await asyncio.wait_for(old_connect, timeout=1) is False
    assert replacement.is_connected
    assert not old.is_connected
    assert not old.has_fatal_error

    await old.disconnect()
    assert replacement.is_connected
    assert old_release.call_count == 0
    assert replacement_release.call_count == 0

    await replacement.disconnect()
    assert replacement_release.call_count == 1

"""Concurrent bounded hooks preserve observations without multiplying hung workers."""

import contextvars
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from hermes_cli.plugins import PluginManager
from hermes_cli.plugins_dispatch import _HOOK_SKIPPED, _PRE_TOOL_CALL_TIMEOUT_BLOCK_MESSAGE


def _wait_for(predicate):
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    assert predicate()


def _queued(manager, callback, count, hook="post_tool_call"):
    with manager._hook_timeout_lock:
        state = manager._hook_running_callbacks.get((hook, id(callback)))
        return state is not None and len(state.waiters) == count


@pytest.mark.parametrize("count", [2, 3])
def test_healthy_overlap_is_fifo_and_keeps_each_callers_context(monkeypatch, count):
    monkeypatch.setattr("hermes_cli.plugins._resolve_hook_callback_timeout", lambda: 5.0)
    manager = PluginManager()
    started, release = threading.Event(), threading.Event()
    caller_context = contextvars.ContextVar("test_hook_caller", default=None)
    seen = []

    def observer(value):
        seen.append((value, caller_context.get()))
        if value == 0:
            started.set()
            assert release.wait(5.0)
        return value

    manager._hooks["post_tool_call"] = [observer]

    def invoke(value):
        caller_context.set(f"caller-{value}")
        return manager.invoke_hook("post_tool_call", value=value)

    with ThreadPoolExecutor(max_workers=count) as pool:
        futures = [pool.submit(invoke, 0)]
        try:
            assert started.wait(2.0)
            for value in range(1, count):
                futures.append(pool.submit(invoke, value))
                _wait_for(lambda: _queued(manager, observer, value))
            release.set()
            assert [future.result(2.0) for future in futures] == [[value] for value in range(count)]
        finally:
            release.set()
    assert seen == [(value, f"caller-{value}") for value in range(count)]
    assert manager._hook_running_callbacks == {}


def test_hung_overlap_starts_only_one_worker_and_keeps_owner_until_exit():
    manager = PluginManager()
    manager._hook_timeout_suppression_seconds = 0
    started, release, exited = threading.Event(), threading.Event(), threading.Event()
    starts = []

    def observer():
        starts.append(1)
        started.set()
        try:
            assert release.wait(5.0)
        finally:
            exited.set()

    call = lambda timeout: manager._run_hook_callback_bounded("post_tool_call", observer, {}, timeout)
    with ThreadPoolExecutor(max_workers=4) as pool:
        active = pool.submit(call, 0.5)
        try:
            assert started.wait(2.0)
            queued = [pool.submit(call, 3.0) for _ in range(3)]
            _wait_for(lambda: _queued(manager, observer, 3))
            assert active.result(2.0) is _HOOK_SKIPPED
            assert all(future.result(2.0) is _HOOK_SKIPPED for future in queued)
            for _ in range(10):
                assert call(1.0) is _HOOK_SKIPPED
            state = manager._hook_running_callbacks[("post_tool_call", id(observer))]
            assert state.active is not None and state.timed_out
            assert starts == [1]
        finally:
            release.set()
    assert exited.wait(2.0)
    _wait_for(lambda: not manager._hook_running_callbacks)
    assert call(1.0) is None
    assert starts == [1, 1]


def test_queue_expiry_does_not_poison_healthy_active_callback():
    manager = PluginManager()
    started, release = threading.Event(), threading.Event()
    seen = []

    def observer(value):
        seen.append(value)
        if value == "active":
            started.set()
            assert release.wait(5.0)
        return value

    def call(value, timeout):
        return manager._run_hook_callback_bounded("post_tool_call", observer, {"value": value}, timeout)

    with ThreadPoolExecutor(max_workers=3) as pool:
        active = pool.submit(call, "active", 5.0)
        try:
            assert started.wait(2.0)
            expiring = pool.submit(call, "expired", 0.2)
            _wait_for(lambda: _queued(manager, observer, 1))
            survivor = pool.submit(call, "survivor", 5.0)
            _wait_for(lambda: _queued(manager, observer, 2))
            assert expiring.result(2.0) is _HOOK_SKIPPED
            assert not manager._hook_timeout_suppressed_until
            release.set()
            assert active.result(2.0) == "active"
            assert survivor.result(2.0) == "survivor"
        finally:
            release.set()
    assert seen == ["active", "survivor"]
    assert not manager._hook_running_callbacks


def test_same_key_reentrancy_skips_without_deadlock_and_preserves_context(monkeypatch):
    monkeypatch.setattr("hermes_cli.plugins._resolve_hook_callback_timeout", lambda: 5.0)
    manager = PluginManager()
    current = contextvars.ContextVar("reentrant_hook_context", default=None)
    current.set("parent")
    nested = []

    def observer():
        nested.append(manager.invoke_hook("post_tool_call"))
        return current.get()

    manager._hooks["post_tool_call"] = [observer]
    assert manager.invoke_hook("post_tool_call") == ["parent"]
    assert nested == [[]]
    assert not manager._hook_timeout_suppressed_until
    assert not manager._hook_running_callbacks


def test_same_callback_on_another_manager_is_not_reentrant():
    outer, inner = PluginManager(), PluginManager()

    def observer(nested=False):
        if nested:
            return "inner"
        return inner._run_hook_callback_bounded("post_tool_call", observer, {"nested": True}, 2.0)

    assert outer._run_hook_callback_bounded("post_tool_call", observer, {}, 2.0) == "inner"


def test_raising_callback_releases_queued_successor(monkeypatch):
    monkeypatch.setattr("hermes_cli.plugins._resolve_hook_callback_timeout", lambda: 5.0)
    manager = PluginManager()
    started, release = threading.Event(), threading.Event()

    def observer(fail=False):
        if fail:
            started.set()
            assert release.wait(5.0)
            raise RuntimeError("test callback failure")
        return "survived"

    manager._hooks["post_tool_call"] = [observer]
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(manager.invoke_hook, "post_tool_call", fail=True)
        try:
            assert started.wait(2.0)
            second = pool.submit(manager.invoke_hook, "post_tool_call")
            _wait_for(lambda: _queued(manager, observer, 1))
            release.set()
            assert first.result(2.0) == []
            assert second.result(2.0) == ["survived"]
        finally:
            release.set()
    assert not manager._hook_running_callbacks


def test_worker_start_failure_wakes_queued_successor(monkeypatch):
    manager = PluginManager()
    starting, release = threading.Event(), threading.Event()
    real_start = threading.Thread.start
    starts = []

    def fail_first_worker(thread):
        if thread.name.startswith("hermes-hook-"):
            starts.append(thread)
            if len(starts) == 1:
                starting.set()
                assert release.wait(5.0)
                raise RuntimeError("test thread start failure")
        return real_start(thread)

    monkeypatch.setattr(threading.Thread, "start", fail_first_worker)
    observer = lambda: "ok"
    call = lambda: manager._run_hook_callback_bounded("post_tool_call", observer, {}, 5.0)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(call)
        try:
            assert starting.wait(2.0)
            second = pool.submit(call)
            _wait_for(lambda: _queued(manager, observer, 1))
            release.set()
            assert first.result(2.0) is _HOOK_SKIPPED
            assert second.result(2.0) == "ok"
        finally:
            release.set()
    assert not manager._hook_running_callbacks
    assert not manager._hook_timeout_suppressed_until


def test_concurrent_pretool_timeout_keeps_fail_closed_translation(monkeypatch):
    monkeypatch.setattr("hermes_cli.plugins._resolve_hook_callback_timeout", lambda: 0.5)
    manager = PluginManager()
    started, release = threading.Event(), threading.Event()
    starts = []

    def policy():
        starts.append(1)
        started.set()
        assert release.wait(5.0)

    manager._hooks["pre_tool_call"] = [policy]
    blocked = [{"action": "block", "message": _PRE_TOOL_CALL_TIMEOUT_BLOCK_MESSAGE}]
    with ThreadPoolExecutor(max_workers=3) as pool:
        first = pool.submit(manager.invoke_hook, "pre_tool_call")
        try:
            assert started.wait(2.0)
            others = [pool.submit(manager.invoke_hook, "pre_tool_call") for _ in range(2)]
            _wait_for(lambda: _queued(manager, policy, 2, "pre_tool_call"))
            assert first.result(2.0) == blocked
            assert [future.result(2.0) for future in others] == [blocked, blocked]
            assert starts == [1]
        finally:
            release.set()
    _wait_for(lambda: not manager._hook_running_callbacks)


def test_unload_invalidates_queued_calls_and_late_cleanup_keeps_successor():
    manager = PluginManager()
    old_started, old_release = threading.Event(), threading.Event()
    new_started, new_release = threading.Event(), threading.Event()

    def observer(value):
        if value == "old":
            old_started.set()
            assert old_release.wait(5.0)
        elif value == "new":
            new_started.set()
            assert new_release.wait(5.0)
        return value

    def call(value):
        return manager._run_hook_callback_bounded("post_tool_call", observer, {"value": value}, 5.0)

    with ThreadPoolExecutor(max_workers=3) as pool:
        old = pool.submit(call, "old")
        try:
            assert old_started.wait(2.0)
            queued = pool.submit(call, "queued")
            _wait_for(lambda: _queued(manager, observer, 1))
            manager.unload()
            new = pool.submit(call, "new")
            assert new_started.wait(2.0)
            state = manager._hook_running_callbacks[("post_tool_call", id(observer))]
            old_release.set()
            assert old.result(2.0) == "old"
            assert queued.result(2.0) is _HOOK_SKIPPED
            assert manager._hook_running_callbacks[("post_tool_call", id(observer))] is state
            new_release.set()
            assert new.result(2.0) == "new"
        finally:
            old_release.set()
            new_release.set()
    assert not manager._hook_running_callbacks

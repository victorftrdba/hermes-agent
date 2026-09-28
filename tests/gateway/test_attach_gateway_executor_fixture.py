"""Explicit executor fixture ownership and one-way lifecycle contracts."""

from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import pytest


def _bare_runner():
    from gateway.run import GatewayRunner

    return object.__new__(GatewayRunner)


def test_attach_is_idempotent_and_executor_is_usable(attach_gateway_executor):
    runner = _bare_runner()

    assert attach_gateway_executor(runner) is runner
    executor = runner._get_executor()
    assert attach_gateway_executor(runner) is runner
    assert runner._get_executor() is executor
    assert executor.submit(lambda: "ready").result(timeout=2) == "ready"


def test_existing_executor_is_preserved_and_not_owned(request):
    runner = _bare_runner()
    executor = ThreadPoolExecutor(max_workers=1)
    runner._executor = executor
    runner._executor_closing = False

    def verify_unowned_executor():
        try:
            assert runner._executor is executor
            assert executor.submit(lambda: "still open").result(timeout=2) == "still open"
        finally:
            executor.shutdown(wait=True)

    request.addfinalizer(verify_unowned_executor)
    attach = request.getfixturevalue("attach_gateway_executor")
    assert attach(runner) is runner
    assert runner._get_executor() is executor


@pytest.mark.parametrize("state", ["closing", "detached", "shutdown"])
def test_attach_refuses_closing_or_closed_runner(attach_gateway_executor, state):
    runner = _bare_runner()
    runner._executor_closing = state == "closing"
    executor = None
    if state == "shutdown":
        executor = ThreadPoolExecutor(max_workers=1)
        executor.shutdown(wait=True)
    runner._executor = executor

    with pytest.raises(RuntimeError, match="shutting-down|closed"):
        attach_gateway_executor(runner)

    assert runner._executor is executor
    assert runner._executor_closing is (state == "closing")


@pytest.mark.parametrize("state", ["attached", "detached", "replaced"])
def test_fixture_teardown_drains_only_owned_workers(request, state):
    runner = _bare_runner()
    executors = {}

    def verify_cleanup():
        owned = executors["owned"]
        assert all(not worker.is_alive() for worker in owned._threads)
        if state == "replaced":
            replacement = executors["replacement"]
            try:
                assert runner._executor is replacement
                assert replacement.submit(lambda: "open").result(timeout=2) == "open"
            finally:
                replacement.shutdown(wait=True)
        else:
            assert runner._executor is None
            assert runner._executor_closing

    request.addfinalizer(verify_cleanup)
    attach = request.getfixturevalue("attach_gateway_executor")
    attach(runner)
    owned = executors["owned"] = runner._get_executor()
    assert owned.submit(lambda: "done").result(timeout=2) == "done"
    assert any(worker.is_alive() for worker in owned._threads)

    if state == "detached":
        runner._shutdown_executor()
        with pytest.raises(RuntimeError, match="shutting-down"):
            attach(runner)
    elif state == "replaced":
        replacement = executors["replacement"] = ThreadPoolExecutor(max_workers=1)
        runner._executor = replacement
    runner._shutdown_executor = Mock(side_effect=AssertionError("patched shutdown"))

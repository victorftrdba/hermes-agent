from __future__ import annotations

import asyncio
import json
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from gateway.config import GatewayConfig, Platform
from gateway.session import SessionEntry, SessionSource, SessionStore
from gateway.session_db_recovery import (
    SessionDBPreparationManager,
    _load_profile_settings,
    _prepare,
)


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class _Process:
    def __init__(self, result: dict, *, blocked: bool = False, returncode: int = 0) -> None:
        self.result = result
        self.returncode = returncode
        self.release = threading.Event()
        self.terminated = False
        self.killed = False
        if not blocked:
            self.release.set()

    def communicate(self):
        self.release.wait(timeout=5)
        return json.dumps(self.result), ""

    def terminate(self) -> None:
        self.terminated = True

    def wait(self, timeout=None) -> int:
        if not self.release.wait(timeout=timeout):
            raise subprocess.TimeoutExpired("bootstrap", timeout)
        return self.returncode

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9
        self.release.set()


def _wait_until(predicate, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition did not become true")


def _entry(key: str, session_id: str) -> SessionEntry:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return SessionEntry(
        session_key=key,
        session_id=session_id,
        platform=Platform.TELEGRAM,
        chat_type="dm",
        created_at=now,
        updated_at=now,
        origin=SessionSource(platform=Platform.TELEGRAM, chat_id=session_id),
    )


def test_gateway_runner_and_store_construction_do_not_touch_sqlite(monkeypatch, tmp_path) -> None:
    import gateway.run as gateway_run
    import hermes_state
    import hermes_state_registry

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    sqlite_calls = []
    monkeypatch.setattr(hermes_state, "SessionDB", lambda *a, **k: sqlite_calls.append((a, k)))
    monkeypatch.setattr(
        hermes_state_registry, "acquire", lambda *a, **k: sqlite_calls.append((a, k)),
    )
    monkeypatch.setattr(gateway_run.GatewayRunner, "_init_startup_checks", lambda self: None)
    runner = gateway_run.GatewayRunner(
        GatewayConfig(platforms={}, sessions_dir=tmp_path / "sessions")
    )

    assert sqlite_calls == []
    assert runner.session_store._db_handles == {}
    assert runner._session_db_handles == {}


@pytest.mark.asyncio
async def test_blocked_bootstrap_does_not_delay_serving_startup(
    monkeypatch, tmp_path,
) -> None:
    import gateway.run as gateway_run

    db_path = (tmp_path / "state.db").resolve()
    process = _Process({"status": "ok", "db_path": str(db_path)}, blocked=True)
    manager = SessionDBPreparationManager(popen=lambda *a, **k: process)
    sequence = []

    class _Runner:
        def __init__(self, config):
            self.config = config
            self.adapters = {}
            self._running = False
            self.should_exit_cleanly = False
            self.exit_reason = None
            self.exit_code = None

        def start_session_db_preparation(self, lifecycle_evidence):
            sequence.append("bootstrap")
            manager.enable()
            return manager.request(
                db_path, profile_home=tmp_path, sessions_dir=tmp_path / "sessions",
                lifecycle_evidence=lifecycle_evidence,
            )

        async def start(self):
            sequence.append("platform")
            assert sequence[:2] == ["control", "bootstrap"]
            assert manager.status(db_path) == "pending"
            return False

    async def _control_socket(runner):
        sequence.append("control")
        return object()

    monkeypatch.setattr("hermes_cli.resource_limits.apply_nofile_soft_limit", lambda: None)
    monkeypatch.setattr("gateway.code_skew.record_boot_fingerprint", lambda: None)
    monkeypatch.setattr("gateway.status.get_running_pid", lambda: None)
    monkeypatch.setattr("hermes_cli.nous_auth_keepalive.start_nous_auth_keepalive", lambda: None)
    monkeypatch.setattr(gateway_run, "GatewayRunner", _Runner)
    monkeypatch.setattr(gateway_run, "_start_gateway_configure_logging", lambda verbosity: None)
    monkeypatch.setattr(gateway_run, "_enable_multiplex_log_routing", lambda config: None)
    monkeypatch.setattr(gateway_run, "_start_gateway_claim_pid_file", lambda: True)
    monkeypatch.setattr(
        gateway_run, "_start_gateway_record_lifecycle_startup",
        lambda runner=None: {"prior_pid": 1},
    )
    monkeypatch.setattr(gateway_run, "_start_gateway_start_control_socket", _control_socket)
    monkeypatch.setattr(gateway_run, "_ensure_windows_gateway_venv_imports", lambda: None)
    monkeypatch.setattr(gateway_run, "_discover_gateway_mcp_tools", lambda config: asyncio.sleep(0))
    monkeypatch.setattr(gateway_run, "_shutdown_gateway_health_export", lambda runner: None)
    monkeypatch.setattr(gateway_run, "_run_planned_stop_watcher", lambda *args: None)
    worker_thread = SimpleNamespace(name="test-worker")
    main_thread = SimpleNamespace(name="test-main")
    monkeypatch.setattr(gateway_run.threading, "current_thread", lambda: worker_thread)
    monkeypatch.setattr(gateway_run.threading, "main_thread", lambda: main_thread)

    try:
        result = await asyncio.wait_for(
            gateway_run.start_gateway(config=GatewayConfig(), verbosity=None), timeout=2,
        )
    finally:
        manager.close(timeout=0.01)

    assert result is False
    assert sequence == ["control", "bootstrap", "platform"]
    assert process.terminated is True


def test_fallback_routing_reconciles_after_child_success(monkeypatch, tmp_path) -> None:
    import hermes_state

    db_path = (tmp_path / "state.db").resolve()
    sessions_dir = tmp_path / "sessions"
    sessions_dir.mkdir()
    durable = _entry("agent:main:telegram:dm:durable", "durable")
    fallback = _entry("agent:main:telegram:dm:fallback", "fallback")
    database = hermes_state.SessionDB(db_path=db_path)
    database.save_gateway_routing_entry(
        durable.session_key, json.dumps(durable.to_dict()), scope=str(sessions_dir.resolve()),
    )
    database.close()
    (sessions_dir / "sessions.json").write_text(
        json.dumps({fallback.session_key: fallback.to_dict()}), encoding="utf-8",
    )
    process = _Process({"status": "ok", "db_path": str(db_path)}, blocked=True)
    manager = SessionDBPreparationManager(popen=lambda *a, **k: process)
    store = SessionStore(
        sessions_dir, GatewayConfig(sessions_dir=sessions_dir),
        eager_session_db=False, db_preparation=manager,
    )
    store._routing_home = tmp_path
    monkeypatch.setattr(hermes_state, "_default_db_path", lambda: db_path)
    store._ensure_loaded()
    assert set(store._entries) == {fallback.session_key}

    manager.enable()
    assert store._db is None
    process.release.set()
    _wait_until(lambda: durable.session_key in store._entries)

    def _routing_rows():
        return store._db.load_gateway_routing_entries(scope=str(sessions_dir.resolve()))

    _wait_until(
        lambda: set(_routing_rows()) == {durable.session_key, fallback.session_key},
    )

    assert set(store._entries) == {durable.session_key, fallback.session_key}
    rows = _routing_rows()
    assert set(rows) == {durable.session_key, fallback.session_key}
    manager.close()
    store.close_all_db_handles()


def test_failed_child_retries_after_backoff(tmp_path) -> None:
    clock = _Clock()
    db_path = (tmp_path / "state.db").resolve()
    processes = [
        _Process({"status": "error", "db_path": str(db_path), "error": "locked"}, returncode=1),
        _Process({"status": "ok", "db_path": str(db_path)}),
    ]
    launches = []

    def _popen(*args, **kwargs):
        launches.append(args[0])
        return processes[len(launches) - 1]

    manager = SessionDBPreparationManager(
        popen=_popen, clock=clock, initial_retry_delay=2, max_retry_delay=2,
    )
    manager.enable()
    request = lambda: manager.request(
        db_path, profile_home=tmp_path, sessions_dir=tmp_path / "sessions"
    )
    assert request() == "pending"
    _wait_until(lambda: manager.status(db_path) == "failed")
    assert request() == "failed"
    assert len(launches) == 1
    clock.now = 2
    assert request() == "pending"
    _wait_until(lambda: manager.status(db_path) == "ready")
    assert len(launches) == 2
    manager.close()


def test_failed_child_retains_lifecycle_evidence_until_success(monkeypatch, tmp_path) -> None:
    import gateway.lifecycle_ledger as lifecycle
    import hermes_state

    db_path = (tmp_path / "state.db").resolve()
    evidence = {"prior_pid": 42}
    processes = [
        _Process({"status": "error", "db_path": str(db_path), "error": "locked"}, returncode=1),
        _Process({"status": "ok", "db_path": str(db_path)}),
    ]
    commands = []

    def _popen(command, **kwargs):
        commands.append(command)
        return processes[len(commands) - 1]

    manager = SessionDBPreparationManager(
        popen=_popen, initial_retry_delay=0, max_retry_delay=0,
    )
    manager.enable()
    request = lambda lifecycle_evidence=None: manager.request(
        db_path, profile_home=tmp_path, sessions_dir=tmp_path / "sessions",
        lifecycle_evidence=lifecycle_evidence,
    )
    assert request(evidence) == "pending"
    _wait_until(lambda: manager.status(db_path) == "failed")
    assert request() == "pending"
    _wait_until(lambda: manager.status(db_path) == "ready")
    assert len(commands) == 2
    for command in commands:
        index = command.index("--lifecycle-evidence-json")
        assert json.loads(command[index + 1]) == evidence
    manager.close()

    reports = []
    attempts = 0

    class _DB:
        def retry_deferred_fts_recovery(self):
            return None

        def close(self):
            return None

    def _session_db(db_path):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("locked")
        return _DB()

    monkeypatch.setattr(hermes_state, "SessionDB", _session_db)
    monkeypatch.setattr(
        lifecycle, "report_unclean_exit",
        lambda value, home=None: reports.append((value, home)),
    )
    with pytest.raises(OSError, match="locked"):
        _prepare(db_path, tmp_path, tmp_path / "sessions", {}, evidence)
    assert reports == []
    _prepare(db_path, tmp_path, tmp_path / "sessions", {}, evidence)
    assert reports == [(evidence, tmp_path)]


def test_exact_profile_paths_never_cross_attach(tmp_path) -> None:
    processes = []
    commands = []

    def _popen(command, **kwargs):
        commands.append(command)
        path = Path(command[command.index("--db-path") + 1])
        process = _Process({"status": "ok", "db_path": str(path)}, blocked=True)
        processes.append(process)
        return process

    manager = SessionDBPreparationManager(popen=_popen)
    manager.enable()
    attached = []
    paths = [(tmp_path / name / "state.db").resolve() for name in ("a", "b")]
    for path in paths:
        assert manager.request(
            path, profile_home=path.parent, sessions_dir=path.parent / "sessions",
            on_ready=lambda ready_path, result: attached.append(ready_path),
        ) == "pending"
        manager.request(path, profile_home=path.parent, sessions_dir=path.parent / "sessions")
    assert len(commands) == 2

    processes[0].release.set()
    _wait_until(lambda: attached == [paths[0]])
    assert manager.status(paths[0]) == "ready"
    assert manager.status(paths[1]) == "pending"
    processes[1].release.set()
    _wait_until(lambda: attached == paths)
    manager.close()


def test_maintenance_launches_for_every_ready_profile_without_reattach(tmp_path) -> None:
    commands = []
    processes = []

    def _popen(command, **kwargs):
        commands.append(command)
        path = Path(command[command.index("--db-path") + 1])
        process = _Process({"status": "ok", "db_path": str(path)}, blocked=True)
        processes.append(process)
        return process

    manager = SessionDBPreparationManager(popen=_popen)
    manager.enable()
    attached = []
    paths = [(tmp_path / name / "state.db").resolve() for name in ("a", "b")]
    for path in paths:
        manager.request(
            path, profile_home=path.parent, sessions_dir=path.parent / "sessions",
            on_ready=lambda ready_path, result: attached.append(ready_path),
        )
    for process in processes[:2]:
        process.release.set()
    _wait_until(lambda: len(attached) == 2)

    states = manager.request_ready_maintenance()

    assert states == {path: "pending" for path in paths}
    assert len(commands) == 4
    maintenance = commands[2:]
    assert all("--maintenance-only" in command for command in maintenance)
    assert {
        Path(command[command.index("--db-path") + 1]) for command in maintenance
    } == set(paths)
    assert set(attached) == set(paths)
    for process in processes[2:]:
        process.release.set()
    _wait_until(
        lambda: all(
            manager.maintenance_state(path)["status"] == "ok" for path in paths
        )
    )
    assert len(attached) == 2
    manager.close()


def test_failed_maintenance_retries_without_demoting_ready_path(tmp_path) -> None:
    clock = _Clock()
    db_path = (tmp_path / "state.db").resolve()
    processes = [
        _Process({"status": "ok", "db_path": str(db_path)}, blocked=True),
        _Process(
            {"status": "error", "db_path": str(db_path), "error": "maintenance locked"},
            blocked=True, returncode=1,
        ),
        _Process({"status": "ok", "db_path": str(db_path)}, blocked=True),
    ]
    launches = []

    def _popen(command, **kwargs):
        launches.append(command)
        return processes[len(launches) - 1]

    attached = []
    manager = SessionDBPreparationManager(
        popen=_popen, clock=clock, initial_retry_delay=2, max_retry_delay=2,
    )
    manager.enable()
    manager.request(
        db_path, profile_home=tmp_path, sessions_dir=tmp_path / "sessions",
        on_ready=lambda path, result: attached.append(path),
    )
    processes[0].release.set()
    _wait_until(lambda: manager.status(db_path) == "ready")
    assert attached == [db_path]

    assert manager.request_ready_maintenance() == {db_path: "pending"}
    assert manager.maintenance_state(db_path)["status"] == "pending"
    processes[1].release.set()
    _wait_until(lambda: manager.maintenance_state(db_path)["status"] == "failed")
    state = manager.maintenance_state(db_path)
    assert manager.status(db_path) == "ready"
    assert state == {
        "status": "failed", "failures": 1, "error": "maintenance locked",
        "next_retry_at": 2.0,
    }
    assert manager.request_ready_maintenance() == {db_path: "failed"}
    assert len(launches) == 2

    clock.now = 2.0
    assert manager.request_ready_maintenance() == {db_path: "pending"}
    processes[2].release.set()
    _wait_until(lambda: manager.maintenance_state(db_path)["status"] == "ok")
    assert manager.status(db_path) == "ready"
    assert attached == [db_path]
    assert len(launches) == 3
    manager.close()


def test_secondary_profile_children_load_only_their_explicit_config(monkeypatch, tmp_path) -> None:
    root = tmp_path / "root"
    profile_a = tmp_path / "profile-a"
    profile_b = tmp_path / "profile-b"
    for home, config in (
        (root, {"sessions": {"auto_archive_days": 99}}),
        (profile_a, {
            "sessions": {"auto_archive": True, "auto_archive_days": 4},
            "checkpoints": {"auto_prune": True, "max_total_size_mb": 111},
        }),
        (profile_b, {
            "sessions": {"auto_prune": True, "retention_days": 22},
            "checkpoints": {"auto_prune": True, "max_total_size_mb": 222},
        }),
    ):
        home.mkdir()
        (home / "config.yaml").write_text(json.dumps(config), encoding="utf-8")
    monkeypatch.setenv("HERMES_HOME", str(root))

    settings_a = _load_profile_settings(profile_a)
    settings_b = _load_profile_settings(profile_b)

    assert settings_a["auto_archive_days"] == 4
    assert settings_a["checkpoints"]["max_total_size_mb"] == 111
    assert settings_b["retention_days"] == 22
    assert settings_b["checkpoints"]["max_total_size_mb"] == 222
    assert settings_a != settings_b

    commands = []
    processes = []

    def _popen(command, **kwargs):
        commands.append(command)
        path = Path(command[command.index("--db-path") + 1])
        process = _Process({"status": "ok", "db_path": str(path)}, blocked=True)
        processes.append(process)
        return process

    manager = SessionDBPreparationManager(popen=_popen)
    manager.enable()
    for home in (profile_a, profile_b):
        manager.request(
            home / "state.db", profile_home=home, sessions_dir=home / "sessions",
        )
    assert len(commands) == 2
    assert all("--settings-json" not in command for command in commands)
    assert {
        Path(command[command.index("--profile-home") + 1]) for command in commands
    } == {profile_a.resolve(), profile_b.resolve()}
    for process in processes:
        process.release.set()
    manager.close()


def test_shutdown_kills_reaps_and_prevents_late_attach(tmp_path) -> None:
    db_path = (tmp_path / "state.db").resolve()
    process = _Process({"status": "ok", "db_path": str(db_path)}, blocked=True)
    attached = []
    manager = SessionDBPreparationManager(popen=lambda *a, **k: process)
    manager.enable()
    manager.request(
        db_path, profile_home=tmp_path, sessions_dir=tmp_path / "sessions",
        on_ready=lambda path, result: attached.append(path),
    )

    manager.close(timeout=0.01)

    assert process.terminated is True
    assert process.killed is True
    assert process.release.is_set()
    assert attached == []
    assert manager.request(
        db_path, profile_home=tmp_path, sessions_dir=tmp_path / "sessions"
    ) == "unavailable"


def test_checkpoint_auto_prune_configuration_runs_in_child(monkeypatch, tmp_path) -> None:
    import gateway.run as gateway_run
    import hermes_cli.config as hermes_config
    import hermes_state
    import tools.checkpoint_manager as checkpoint_manager

    db_path = (tmp_path / "state.db").resolve()

    class _Manager:
        def enable(self):
            self.enabled = True

        def request(self, path, **kwargs):
            self.path = path
            self.kwargs = kwargs
            self.settings = kwargs["settings"]
            return "pending"

    manager = _Manager()
    runner = object.__new__(gateway_run.GatewayRunner)
    runner.config = GatewayConfig(sessions_dir=tmp_path / "sessions")
    runner._session_db_preparation = manager
    runner.session_store = object()
    monkeypatch.setattr(
        hermes_config, "load_config",
        lambda: {"checkpoints": {
            "auto_prune": True, "retention_days": 11,
            "min_interval_hours": 5, "max_total_size_mb": 321,
        }},
    )
    monkeypatch.setattr(hermes_state, "_default_db_path", lambda: db_path)

    assert runner.start_session_db_preparation() == "pending"
    assert manager.settings["checkpoints"] == {
        "auto_prune": True, "retention_days": 11,
        "min_interval_hours": 5, "max_total_size_mb": 321,
    }

    class _DB:
        def __init__(self, db_path):
            self.db_path = db_path

        def retry_deferred_fts_recovery(self):
            return None

        def close(self):
            return None

    prune_calls = []
    monkeypatch.setattr(hermes_state, "SessionDB", _DB)
    monkeypatch.setattr(
        checkpoint_manager, "maybe_auto_prune_checkpoints",
        lambda **kwargs: prune_calls.append(kwargs),
    )
    _prepare(
        db_path, tmp_path, tmp_path / "sessions", manager.settings, None,
    )

    assert prune_calls == [{
        "retention_days": 11,
        "min_interval_hours": 5,
        "delete_orphans": False,
        "checkpoint_base": tmp_path / "checkpoints",
        "max_total_size_mb": 321,
    }]


def test_lifecycle_and_housekeeping_sqlite_work_is_child_owned(monkeypatch, tmp_path) -> None:
    import gateway.run as gateway_run
    import gateway.lifecycle_ledger as lifecycle
    import hermes_state

    calls = []

    class _DB:
        def __init__(self, db_path):
            calls.append(("open", db_path))

        def maybe_auto_archive(self, **kwargs):
            calls.append(("archive", kwargs))

        def maybe_auto_prune_and_vacuum(self, **kwargs):
            calls.append(("prune", kwargs))

        def retry_deferred_fts_recovery(self):
            calls.append(("fts", None))

        def close(self):
            calls.append(("close", None))

    monkeypatch.setattr(hermes_state, "SessionDB", _DB)
    monkeypatch.setattr(
        lifecycle, "report_unclean_exit",
        lambda evidence, home=None: calls.append(("lifecycle", (evidence, home))),
    )
    _prepare(
        tmp_path / "state.db", tmp_path, tmp_path / "sessions",
        {"auto_archive": True, "auto_prune": True}, {"prior_pid": 1},
    )
    assert [name for name, _ in calls] == ["open", "archive", "prune", "fts", "close", "lifecycle"]

    class _Runner:
        def request_session_db_maintenance(self):
            calls.append(("requested", None))

    with patch("hermes_state_registry.acquire", side_effect=AssertionError("parent SQLite open")):
        gateway_run._housekeeping_auto_archive(_Runner())
        gateway_run._housekeeping_deferred_fts_retry(_Runner())
    assert [name for name, _ in calls[-2:]] == ["requested", "requested"]

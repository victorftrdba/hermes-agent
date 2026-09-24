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


def test_pending_transcript_spools_and_reconciles_without_metadata_loss(
    monkeypatch, tmp_path,
) -> None:
    import hermes_state
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override

    db_path = (tmp_path / "state.db").resolve()
    sessions_dir = tmp_path / "sessions"
    sessions_dir.mkdir()
    entry = _entry("agent:main:telegram:dm:fallback", "fallback-transcript")
    database = hermes_state.SessionDB(db_path=db_path)
    database.create_session(
        entry.session_id, source="telegram", session_key=entry.session_key,
    )
    database.close()
    process = _Process({"status": "ok", "db_path": str(db_path)}, blocked=True)
    manager = SessionDBPreparationManager(popen=lambda *a, **k: process)
    store = SessionStore(
        sessions_dir, GatewayConfig(sessions_dir=sessions_dir),
        eager_session_db=False, db_preparation=manager,
    )
    store._routing_home = tmp_path
    store._entries[entry.session_key] = entry
    store._loaded = True
    monkeypatch.setattr(hermes_state, "_default_db_path", lambda: db_path)
    manager.enable()
    total = store._MAX_PENDING_PER_SESSION + 5
    messages = [{
        "role": "user",
        "content": f"accepted while bootstrap is pending {index}",
        "platform_message_id": f"message-{index}",
        "display_metadata": {"gateway_input_owner": f"owner-{index}"},
    } for index in range(total)]
    token = set_hermes_home_override(str(tmp_path))
    try:
        for message in messages:
            store.append_to_transcript(entry.session_id, message)
        assert store.load_transcript(entry.session_id) == messages[-store._MAX_PENDING_PER_SESSION:]
        assert len(list((tmp_path / "pending_messages").glob("pending-*.json"))) == total
        process.release.set()

        def _rows():
            db = store._db_for_session_id(entry.session_id)
            return db.get_messages(entry.session_id) if db is not None else []

        _wait_until(lambda: len(_rows()) == total)
        rows = _rows()
        assert [row["content"] for row in rows] == [message["content"] for message in messages]
        assert [row["platform_message_id"] for row in rows] == [
            message["platform_message_id"] for message in messages
        ]
        assert [row["display_metadata"] for row in rows] == [
            message["display_metadata"] for message in messages
        ]
        assert list((tmp_path / "pending_messages").glob("pending-*.json")) == []
        assert store._dirty_transcripts == {}
    finally:
        reset_hermes_home_override(token)
        manager.close()
        store.close_all_db_handles()


def test_partial_bootstrap_reconciliation_retries_on_next_append(tmp_path) -> None:
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override

    sessions_dir = tmp_path / "sessions"
    sessions_dir.mkdir()
    store = SessionStore(
        sessions_dir, GatewayConfig(sessions_dir=sessions_dir), eager_session_db=False,
    )
    entry = _entry("agent:main:telegram:dm:retry", "retry-transcript")
    store._entries[entry.session_key] = entry
    store._loaded = True
    store._db = None
    old = [{
        "role": "user",
        "content": f"old-{index}",
        "display_metadata": {"gateway_input_owner": f"owner-{index}"},
    } for index in range(3)]
    token = set_hermes_home_override(str(tmp_path))
    try:
        for message in old:
            store.append_to_transcript(entry.session_id, message)

        class _DB:
            def __init__(self):
                self.rows = []
                self.failed = False

            def get_compression_tip(self, session_id):
                return session_id

            def has_gateway_input_owner(self, session_id, owner):
                return any(
                    row["session_id"] == session_id
                    and (row.get("display_metadata") or {}).get("gateway_input_owner") == owner
                    for row in self.rows
                )

            def get_session(self, session_id):
                return None

            def _is_compression_child_row(self, row):
                return False

            def append_message(self, **kwargs):
                if kwargs["content"] == "old-1" and not self.failed:
                    self.failed = True
                    raise RuntimeError("transient write failure")
                self.rows.append(kwargs)

        db = _DB()
        store._db = db
        assert store._reconcile_bootstrap_transcript_fallback(tmp_path, db) is False
        assert [row["content"] for row in db.rows] == ["old-0"]
        assert len(list((tmp_path / "pending_messages").glob("pending-*.json"))) == 2

        store.append_to_transcript(entry.session_id, {"role": "user", "content": "new"})

        assert [row["content"] for row in db.rows] == ["old-0", "old-1", "old-2", "new"]
        assert list((tmp_path / "pending_messages").glob("pending-*.json")) == []
        assert store._dirty_transcripts == {}
    finally:
        reset_hermes_home_override(token)
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


def test_agent_persisted_gate_uses_exact_secondary_session_handle(monkeypatch) -> None:
    import gateway.run as gateway_run

    calls = []
    backing_store = object()

    class _AsyncStore:
        _store = backing_store

        async def has_prepared_db_for_session(self, session_id):
            calls.append(("ready", session_id))
            return True

        async def append_to_transcript(self, session_id, message, **kwargs):
            calls.append((message["role"], session_id, kwargs.get("skip_db", False)))

        async def update_session(self, *args, **kwargs):
            return None

    runner = object.__new__(gateway_run.GatewayRunner)
    runner.session_store = backing_store
    runner._async_session_store = _AsyncStore()
    runner._session_db = None

    async def _refresh(*args):
        return None

    runner._refresh_agent_cache_message_count = _refresh
    entry = _entry("agent:secondary:telegram:dm:chat:user", "secondary-session")
    prepared = SimpleNamespace(
        history=[], persist_user_message="hello", message_text="hello",
        persist_user_timestamp=1.0, persist_user_display_kind=None,
        persistence_owner="owner", persistence_session_id=entry.session_id,
    )
    event = SimpleNamespace(message_id="message-1", internal=False)
    monkeypatch.setattr(gateway_run, "_resolve_gateway_model", lambda: "fixture")

    asyncio.run(runner._hmwa_persist_turn_transcript(
        event=event,
        source=entry.origin,
        session_entry=entry,
        session_key=entry.session_key,
        agent_result={"agent_persisted": True, "history_offset": 0},
        agent_messages=[{"role": "user", "content": "hello"}],
        prepared=prepared,
        response="",
        agent_failed_early=False,
        hidden_reasoning_incomplete=False,
        is_context_overflow_failure=False,
    ))

    assert calls[0] == ("ready", "secondary-session")
    assert ("user", "secondary-session", True) in calls


@pytest.mark.parametrize(
    ("agent_session_db_available", "expected_skip_db"),
    [(False, False), (True, True)],
    ids=["pending_then_attached", "constructed_ready"],
)
def test_mid_turn_attach_uses_agent_construction_time_for_persistence(
    monkeypatch, agent_session_db_available, expected_skip_db,
) -> None:
    import gateway.run as gateway_run
    from gateway.run_turn_runner import TurnRunner
    from gateway.turn_context import TurnContext

    calls = []
    backing_store = object()

    class _AsyncStore:
        _store = backing_store

        async def has_prepared_db_for_session(self, session_id):
            calls.append(("ready", session_id))
            return True

        async def append_to_transcript(self, session_id, message, **kwargs):
            calls.append((message["role"], session_id, kwargs.get("skip_db", False)))

        async def update_session(self, *args, **kwargs):
            return None

    runner = object.__new__(gateway_run.GatewayRunner)
    runner.session_store = backing_store
    runner._async_session_store = _AsyncStore()

    async def _refresh(*args):
        return None

    runner._refresh_agent_cache_message_count = _refresh
    entry = _entry("agent:secondary:telegram:dm:chat:user", "secondary-session")
    prepared = SimpleNamespace(
        history=[], persist_user_message="hello", message_text="hello",
        persist_user_timestamp=1.0, persist_user_display_kind=None,
        persistence_owner="owner", persistence_session_id=entry.session_id,
    )
    event = SimpleNamespace(message_id="message-1", internal=False)
    monkeypatch.setattr(gateway_run, "_resolve_gateway_model", lambda: "fixture")
    agent_result = {
        "agent_session_db_available": agent_session_db_available,
        "history_offset": 0,
    }
    if not agent_session_db_available:
        queued = []
        adapter = SimpleNamespace(queue_message=lambda key, message: queued.append((key, message)))
        runner._adapter_for_source = lambda source: adapter
        turn_ctx = TurnContext(
            source=entry.origin,
            session_id=entry.session_id,
            session_key=entry.session_key,
            run_generation=1,
            history=[],
            _interrupt_depth=runner._MAX_INTERRUPT_DEPTH,
            _status_thread_metadata={},
            result_holder=[None],
        )
        raw_result = {"final_response": "", "messages": [], "history_offset": 0}
        turn_runner = TurnRunner(runner, turn_ctx)
        turn_runner._agent_session_db_available = False
        turn_runner._finish_stream_consumer(raw_result, [], None)
        agent_result = asyncio.run(runner._run_agent_queued_followup(
            turn_ctx, adapter, "queued", None, "", raw_result, None,
        ))
        assert queued == [(entry.session_key, "queued")]

    asyncio.run(runner._hmwa_persist_turn_transcript(
        event=event,
        source=entry.origin,
        session_entry=entry,
        session_key=entry.session_key,
        agent_result=agent_result,
        agent_messages=[{"role": "user", "content": "hello"}],
        prepared=prepared,
        response="",
        agent_failed_early=False,
        hidden_reasoning_incomplete=False,
        is_context_overflow_failure=False,
    ))

    user_calls = [call for call in calls if call[0] == "user"]
    assert user_calls == [("user", "secondary-session", expected_skip_db)]


@pytest.mark.parametrize(
    ("agent_session_db_available", "prepared_db_available"),
    [(False, True), (True, False)],
    ids=["pending_then_attached", "ready_then_unavailable"],
)
def test_unavailable_db_rejects_codex_persisted_claim(
    monkeypatch, agent_session_db_available, prepared_db_available,
) -> None:
    import gateway.run as gateway_run

    calls = []
    backing_store = object()

    class _AsyncStore:
        _store = backing_store

        async def has_prepared_db_for_session(self, session_id):
            return prepared_db_available

        async def append_to_transcript(self, session_id, message, **kwargs):
            calls.append((message["role"], session_id, kwargs.get("skip_db", False)))

        async def update_session(self, *args, **kwargs):
            return None

    runner = object.__new__(gateway_run.GatewayRunner)
    runner.session_store = backing_store
    runner._async_session_store = _AsyncStore()

    async def _refresh(*args):
        return None

    runner._refresh_agent_cache_message_count = _refresh
    entry = _entry("agent:secondary:telegram:dm:chat:user", "secondary-session")
    prepared = SimpleNamespace(
        history=[], persist_user_message="hello", message_text="hello",
        persist_user_timestamp=1.0, persist_user_display_kind=None,
        persistence_owner="owner", persistence_session_id=entry.session_id,
    )
    event = SimpleNamespace(message_id="message-1", internal=False)
    monkeypatch.setattr(gateway_run, "_resolve_gateway_model", lambda: "fixture")

    asyncio.run(runner._hmwa_persist_turn_transcript(
        event=event,
        source=entry.origin,
        session_entry=entry,
        session_key=entry.session_key,
        agent_result={
            "agent_session_db_available": agent_session_db_available,
            "agent_persisted": True,
            "history_offset": 0,
        },
        agent_messages=[
            {"role": "user", "content": "hello"},
            {"role": "assistant", "content": "hi"},
        ],
        prepared=prepared,
        response="hi",
        agent_failed_early=False,
        hidden_reasoning_incomplete=False,
        is_context_overflow_failure=False,
    ))

    transcript_calls = [call for call in calls if call[0] in {"user", "assistant"}]
    assert transcript_calls == [
        ("user", "secondary-session", False),
        ("assistant", "secondary-session", False),
    ]

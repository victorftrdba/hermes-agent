"""Recoverable SessionDB handles and gateway bootstrap subprocesses."""

from __future__ import annotations

import contextlib
import json
import subprocess
import sys
import threading
import time
import weakref
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

_INITIAL_RETRY_DELAY_SECONDS = 1.0
_MAX_RETRY_DELAY_SECONDS = 60.0


@dataclass
class _Unavailable:
    failures: int = 0
    next_retry_at: float = 0.0
    in_flight: bool = False


class _HealthSource:
    """Weak-keyable identity for one cache's health entries."""


_health_lock = threading.Lock()
_health_states: weakref.WeakKeyDictionary[_HealthSource, dict[Path, str]]
_health_states = weakref.WeakKeyDictionary()


def _publish_health(source: _HealthSource, path: Path, state: str) -> None:
    """Publish one privacy-safe aggregate (no paths, no errors) across all live caches."""
    with _health_lock:
        _health_states.setdefault(source, {})[path] = state
        all_states = {value for item in _health_states.values() for value in item.values()}
        aggregate = next((s for s in ("retrying", "unavailable") if s in all_states), "ok")
    try:
        from gateway.status import write_runtime_status
        write_runtime_status(session_store={"status": aggregate})
    except Exception:
        pass  # Runtime health is diagnostic only; persistence must not depend on it.


class RecoverableHandleCache:
    """Cache handles by path while allowing failed opens to heal in-process.

    Opens run OUTSIDE ``lock`` (single-flight per path via ``in_flight``); ``close_all`` bumps
    ``_generation`` so a later-completing open is rejected instead of resurrecting the cache.
    """

    def __init__(
        self, *, handles: dict[Path, Any] | None = None, lock: threading.Lock | None = None,
        clock: Callable[[], float] = time.monotonic,
        initial_retry_delay: float = _INITIAL_RETRY_DELAY_SECONDS,
        max_retry_delay: float = _MAX_RETRY_DELAY_SECONDS,
    ) -> None:
        self.handles = handles if handles is not None else {}
        self.lock = lock if lock is not None else threading.Lock()
        self._clock = clock
        self._initial_retry_delay = max(0.0, float(initial_retry_delay))
        self._max_retry_delay = max(self._initial_retry_delay, float(max_retry_delay))
        self._unavailable: dict[Path, _Unavailable] = {}
        self._health_source = _HealthSource()
        self._generation = 0
        self._close_rejected: Callable[[Any], None] | None = None

    def _is_stale(self, path: Path, unavailable: _Unavailable, generation: int) -> bool:
        """Caller holds ``lock``: True when close_all ran or the slot was replaced mid-open."""
        return generation != self._generation or self._unavailable.get(path) is not unavailable

    def get(
        self, path: Path, opener: Callable[[], Any], *, raise_on_error: bool = False,
        on_recovered: Callable[[], None] | None = None,
        non_cacheable: Callable[[Exception], bool] | None = None,
    ) -> Any:
        """Return a cached handle or make one bounded, single-flight open attempt; None while
        a retry is in flight or backing off.  ``non_cacheable`` exceptions (e.g. a live-system
        guard) are re-raised without recording a failure so the next call retries at once."""
        path = Path(path)
        with self.lock:
            if path in self.handles:
                return self.handles[path]
            unavailable = self._unavailable.setdefault(path, _Unavailable())
            if unavailable.in_flight or self._clock() < unavailable.next_retry_at:
                return None
            unavailable.in_flight = True
            was_unavailable = unavailable.failures > 0
            generation = self._generation
        if was_unavailable:
            _publish_health(self._health_source, path, "retrying")

        try:
            handle = opener()
        except Exception as exc:
            uncacheable = non_cacheable is not None and non_cacheable(exc)
            with self.lock:
                stale = self._is_stale(path, unavailable, generation)
                if uncacheable:
                    if not stale:
                        self._unavailable.pop(path, None)
                    raise
                if not stale:
                    unavailable.failures += 1
                    backoff = self._initial_retry_delay * (2 ** min(unavailable.failures - 1, 30))
                    unavailable.next_retry_at = self._clock() + min(backoff, self._max_retry_delay)
                    unavailable.in_flight = False
            if not stale:
                _publish_health(self._health_source, path, "unavailable")
            if raise_on_error:
                raise
            return None

        with self.lock:
            stale = self._is_stale(path, unavailable, generation)
            if not stale:
                self.handles[path] = handle
                self._unavailable.pop(path, None)
            close_rejected = self._close_rejected if stale else None
        if stale:
            if close_rejected is not None:
                with contextlib.suppress(Exception):
                    close_rejected(handle)
            return None
        _publish_health(self._health_source, path, "ok")
        if was_unavailable and on_recovered is not None:
            on_recovered()
        return handle

    def close_all(self, close: Callable[[Any], None]) -> None:
        """Drain cached handles under the lock and close them outside it."""
        with self.lock:
            self._generation += 1
            self._close_rejected = close
            handles = list(self.handles.values())
            paths = set(self.handles) | set(self._unavailable)
            self.handles.clear()
            self._unavailable.clear()
        for handle in handles:
            with contextlib.suppress(Exception):
                close(handle)
        with _health_lock:
            states = _health_states.get(self._health_source, {})
            for path in paths:
                states.pop(path, None)


@dataclass
class _Preparation:
    profile_home: Path
    sessions_dir: Path
    settings: dict[str, Any] | None = None
    lifecycle_evidence: dict[str, Any] | None = None
    callbacks: list[Callable[[Path, dict[str, Any]], None]] = field(default_factory=list)
    process: Any = None
    watcher: threading.Thread | None = None
    ready: bool = False
    failures: int = 0
    next_retry_at: float = 0.0
    generation: int = 0
    error: str | None = None
    maintenance_failures: int = 0
    maintenance_next_retry_at: float = 0.0
    maintenance_error: str | None = None


class SessionDBPreparationManager:
    """Own one non-blocking bootstrap child for each exact state.db path."""

    def __init__(
        self, *, clock: Callable[[], float] = time.monotonic,
        popen: Callable[..., Any] = subprocess.Popen,
        initial_retry_delay: float = _INITIAL_RETRY_DELAY_SECONDS,
        max_retry_delay: float = _MAX_RETRY_DELAY_SECONDS,
    ) -> None:
        self._clock = clock
        self._popen = popen
        self._initial_retry_delay = max(0.0, float(initial_retry_delay))
        self._max_retry_delay = max(self._initial_retry_delay, float(max_retry_delay))
        self._lock = threading.RLock()
        self._preparations: dict[Path, _Preparation] = {}
        self._enabled = False
        self._closed = False
        self._health_source = _HealthSource()

    @staticmethod
    def _exact(path: Path) -> Path:
        return Path(path).expanduser().resolve()

    def enable(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._enabled = True

    def status(self, db_path: Path) -> str:
        path = self._exact(db_path)
        with self._lock:
            prep = self._preparations.get(path)
            if prep is None:
                return "unavailable"
            if prep.ready:
                return "ready"
            if prep.process is not None:
                return "pending"
            return "failed" if prep.failures else "unavailable"

    def request(
        self, db_path: Path, *, profile_home: Path, sessions_dir: Path,
        on_ready: Callable[[Path, dict[str, Any]], None] | None = None,
        lifecycle_evidence: dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
    ) -> str:
        path = self._exact(db_path)
        home = self._exact(profile_home)
        sessions = self._exact(sessions_dir)
        with self._lock:
            if self._closed:
                return "unavailable"
            prep = self._preparations.get(path)
            if prep is None:
                prep = _Preparation(profile_home=home, sessions_dir=sessions)
                self._preparations[path] = prep
            elif prep.profile_home != home or prep.sessions_dir != sessions:
                return "unavailable"
            if settings is not None:
                if prep.settings is not None and prep.settings != settings:
                    return "unavailable"
                prep.settings = dict(settings)
            if lifecycle_evidence is not None and prep.lifecycle_evidence is None:
                prep.lifecycle_evidence = dict(lifecycle_evidence)
            if on_ready is not None and on_ready not in prep.callbacks:
                prep.callbacks.append(on_ready)
            if prep.ready:
                return "ready"
            if not self._enabled:
                return "unavailable"
            if prep.process is not None:
                return "pending"
            if self._clock() < prep.next_retry_at:
                return "failed"
            self._launch_locked(path, prep)
            return "pending" if prep.process is not None else "failed"

    def request_maintenance(self, db_path: Path) -> str:
        path = self._exact(db_path)
        with self._lock:
            prep = self._preparations.get(path)
            if self._closed or not self._enabled or prep is None:
                return "unavailable"
            if prep.process is not None:
                return "pending"
            if not prep.ready:
                return "failed" if prep.failures else "unavailable"
            if self._clock() < prep.maintenance_next_retry_at:
                return "failed"
            self._launch_locked(path, prep, maintenance=True)
            return "pending" if prep.process is not None else "failed"

    def request_ready_maintenance(self) -> dict[Path, str]:
        with self._lock:
            paths = [path for path, prep in self._preparations.items() if prep.ready]
        return {path: self.request_maintenance(path) for path in paths}

    def maintenance_state(self, db_path: Path) -> dict[str, Any]:
        path = self._exact(db_path)
        with self._lock:
            prep = self._preparations.get(path)
            if prep is None or not prep.ready:
                return {"status": "unavailable", "failures": 0, "error": None}
            if prep.process is not None:
                status = "pending"
            elif prep.maintenance_failures:
                status = "failed"
            else:
                status = "ok"
            return {
                "status": status,
                "failures": prep.maintenance_failures,
                "error": prep.maintenance_error,
                "next_retry_at": prep.maintenance_next_retry_at,
            }

    def invalidate(self, db_path: Path, error: BaseException | str) -> None:
        path = self._exact(db_path)
        with self._lock:
            prep = self._preparations.get(path)
            if prep is None or self._closed:
                return
            prep.ready = False
            self._record_failure_locked(path, prep, str(error))

    def _record_failure_locked(self, path: Path, prep: _Preparation, error: str) -> None:
        prep.failures += 1
        delay = self._initial_retry_delay * (2 ** min(prep.failures - 1, 30))
        prep.next_retry_at = self._clock() + min(delay, self._max_retry_delay)
        prep.error = error
        _publish_health(self._health_source, path, "unavailable")

    def _record_maintenance_failure_locked(self, prep: _Preparation, error: str) -> None:
        prep.maintenance_failures += 1
        delay = self._initial_retry_delay * (2 ** min(prep.maintenance_failures - 1, 30))
        prep.maintenance_next_retry_at = self._clock() + min(delay, self._max_retry_delay)
        prep.maintenance_error = error

    def _launch_locked(
        self, path: Path, prep: _Preparation, *, maintenance: bool = False,
    ) -> None:
        command = [
            sys.executable, "-m", "gateway.session_db_recovery",
            "--db-path", str(path),
            "--profile-home", str(prep.profile_home),
            "--sessions-dir", str(prep.sessions_dir),
        ]
        if prep.settings is not None:
            command.extend([
                "--settings-json", json.dumps(prep.settings, separators=(",", ":")),
            ])
        if maintenance:
            command.append("--maintenance-only")
        lifecycle_evidence = None if maintenance else prep.lifecycle_evidence
        if lifecycle_evidence is not None:
            command.extend([
                "--lifecycle-evidence-json",
                json.dumps(lifecycle_evidence, separators=(",", ":"), default=str),
            ])
        prep.generation += 1
        generation = prep.generation
        try:
            prep.process = self._popen(
                command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            )
        except Exception as exc:
            prep.process = None
            error = f"{type(exc).__name__}: {exc}"
            if maintenance and prep.ready:
                self._record_maintenance_failure_locked(prep, error)
            else:
                self._record_failure_locked(path, prep, error)
            return
        if not prep.ready:
            _publish_health(self._health_source, path, "retrying" if prep.failures else "unavailable")
        prep.watcher = threading.Thread(
            target=self._watch, args=(path, prep.process, generation, maintenance),
            daemon=True, name="gateway-session-db-bootstrap",
        )
        prep.watcher.start()

    @staticmethod
    def _result(stdout: str) -> dict[str, Any] | None:
        for line in reversed((stdout or "").splitlines()):
            try:
                value = json.loads(line)
            except (TypeError, ValueError):
                continue
            if isinstance(value, dict):
                return value
        return None

    def _watch(self, path: Path, process: Any, generation: int, maintenance: bool) -> None:
        try:
            stdout, stderr = process.communicate()
            returncode = process.returncode
        except Exception as exc:
            stdout, stderr, returncode = "", str(exc), -1
        result = self._result(stdout)
        valid = bool(
            returncode == 0 and result and result.get("status") == "ok"
            and result.get("db_path") == str(path)
        )
        callbacks: list[Callable[[Path, dict[str, Any]], None]] = []
        with self._lock:
            prep = self._preparations.get(path)
            if prep is None or prep.generation != generation or prep.process is not process:
                return
            prep.process = None
            prep.watcher = None
            if self._closed:
                return
            if valid:
                if maintenance:
                    prep.maintenance_failures = 0
                    prep.maintenance_next_retry_at = 0.0
                    prep.maintenance_error = None
                else:
                    prep.ready = True
                    prep.lifecycle_evidence = None
                    prep.failures = 0
                    prep.next_retry_at = 0.0
                    prep.error = None
                    callbacks = list(prep.callbacks)
                    _publish_health(self._health_source, path, "ok")
            elif maintenance and prep.ready:
                detail = (result or {}).get("error") or (stderr or "").strip()[-500:]
                self._record_maintenance_failure_locked(
                    prep, detail or f"child exited {returncode}",
                )
            else:
                detail = (result or {}).get("error") or (stderr or "").strip()[-500:]
                self._record_failure_locked(path, prep, detail or f"child exited {returncode}")
        for callback in callbacks:
            with contextlib.suppress(Exception):
                callback(path, result or {})

    def close(self, timeout: float = 2.0) -> None:
        with self._lock:
            self._closed = True
            self._enabled = False
            running = [
                (prep.process, prep.watcher) for prep in self._preparations.values()
                if prep.process is not None
            ]
            for prep in self._preparations.values():
                prep.generation += 1
        for process, _watcher in running:
            with contextlib.suppress(Exception):
                process.terminate()
        deadline = self._clock() + max(0.0, timeout)
        for process, watcher in running:
            remaining = max(0.0, deadline - self._clock())
            try:
                process.wait(timeout=remaining)
            except Exception:
                with contextlib.suppress(Exception):
                    process.kill()
                with contextlib.suppress(Exception):
                    process.wait(timeout=0.5)
            if watcher is not None and watcher is not threading.current_thread():
                watcher.join(timeout=max(0.0, deadline - self._clock()))


def configured_settings(config: dict[str, Any]) -> dict[str, Any]:
    sessions = config.get("sessions") or {}
    checkpoints = config.get("checkpoints") or {}
    settings = {
        key: sessions[key] for key in (
            "auto_archive", "auto_archive_days", "auto_prune", "retention_days",
            "min_interval_hours", "min_vacuum_interval_days", "vacuum_after_prune",
        ) if key in sessions
    }
    settings["checkpoints"] = {
        key: checkpoints[key] for key in (
            "auto_prune", "retention_days", "min_interval_hours", "max_total_size_mb",
        ) if key in checkpoints
    }
    return settings


def _load_profile_settings(profile_home: Path) -> dict[str, Any]:
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override
    token = set_hermes_home_override(str(profile_home))
    try:
        from hermes_cli.config import load_config
        return configured_settings(load_config())
    finally:
        reset_hermes_home_override(token)


def _prepare(
    db_path: Path, profile_home: Path, sessions_dir: Path, settings: dict[str, Any],
    lifecycle_evidence: dict[str, Any] | None,
) -> dict[str, Any]:
    from hermes_state import SessionDB
    database = SessionDB(db_path=db_path)
    try:
        if settings.get("auto_archive", False):
            database.maybe_auto_archive(
                idle_days=float(settings.get("auto_archive_days", 3)),
                min_interval_hours=int(settings.get("min_interval_hours", 24)),
            )
        if settings.get("auto_prune", False):
            database.maybe_auto_prune_and_vacuum(
                retention_days=int(settings.get("retention_days", 90)),
                min_interval_hours=int(settings.get("min_interval_hours", 24)),
                min_vacuum_interval_days=int(settings.get("min_vacuum_interval_days", 30)),
                vacuum=bool(settings.get("vacuum_after_prune", True)),
                sessions_dir=sessions_dir,
            )
        retry_fts = getattr(database, "retry_deferred_fts_recovery", None)
        if callable(retry_fts):
            retry_fts()
    finally:
        database.close()
    checkpoint_settings = settings.get("checkpoints") or {}
    if checkpoint_settings.get("auto_prune", False):
        from tools.checkpoint_manager import maybe_auto_prune_checkpoints
        maybe_auto_prune_checkpoints(
            retention_days=int(checkpoint_settings.get("retention_days", 7)),
            min_interval_hours=int(checkpoint_settings.get("min_interval_hours", 24)),
            delete_orphans=False,
            checkpoint_base=profile_home / "checkpoints",
            max_total_size_mb=int(checkpoint_settings.get("max_total_size_mb", 500)),
        )
    if lifecycle_evidence is not None:
        from gateway.lifecycle_ledger import report_unclean_exit
        report_unclean_exit(lifecycle_evidence, home=profile_home)
    return {"status": "ok", "db_path": str(db_path)}


def main() -> int:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, required=True)
    parser.add_argument("--profile-home", type=Path, required=True)
    parser.add_argument("--sessions-dir", type=Path, required=True)
    parser.add_argument("--settings-json")
    parser.add_argument("--lifecycle-evidence-json")
    parser.add_argument("--maintenance-only", action="store_true")
    args = parser.parse_args()
    db_path = args.db_path.expanduser().resolve()
    profile_home = args.profile_home.expanduser().resolve()
    sessions_dir = args.sessions_dir.expanduser().resolve()
    try:
        settings = (
            json.loads(args.settings_json)
            if args.settings_json is not None
            else _load_profile_settings(profile_home)
        )
        evidence = (
            json.loads(args.lifecycle_evidence_json)
            if args.lifecycle_evidence_json else None
        )
        result = _prepare(db_path, profile_home, sessions_dir, settings, evidence)
    except Exception as exc:
        result = {
            "status": "error", "db_path": str(db_path),
            "error": f"{type(exc).__name__}: {exc}",
        }
    print(json.dumps(result, separators=(",", ":"), default=str), flush=True)
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())

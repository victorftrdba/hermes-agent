from __future__ import annotations

import asyncio
import contextlib
import multiprocessing
import os
import threading
import time
import uuid
from dataclasses import dataclass
from multiprocessing.connection import Connection
from multiprocessing.context import SpawnContext, SpawnProcess
from pathlib import Path
from typing import Any, Dict, Optional


def _send_child(connection: Connection, lock: threading.Lock, message: Dict[str, Any]) -> bool:
    try:
        with lock:
            connection.send(message)
        return True
    except (BrokenPipeError, EOFError, OSError):
        return False


def _scheduler_child_main(
    connection: Connection,
    profile_homes: list[Any],
    default_profile_home: str,
    default_profile_name: str,
    interval: int,
    heartbeat_seconds: float,
    dispatch_enabled: bool,
) -> None:
    send_lock = threading.Lock()
    stop_event = threading.Event()
    dispatch_gate = threading.Event()
    if dispatch_enabled:
        dispatch_gate.set()
    transport_condition = threading.Condition()
    transport_results: Dict[str, Dict[str, Any]] = {}
    session_stores: Dict[str, Any] = {}
    session_stores_lock = threading.Lock()

    try:
        from cron.scheduler import drain_delivery_queue, get_running_job_ids
        from cron.scheduler_delivery import (
            _apply_delivery_persistence, set_process_transport_handler)
        from cron.scheduler_provider import InProcessCronScheduler, _profile_cron_scope
        from hermes_constants import get_hermes_home

        def persistence_store():
            profile_home = str(get_hermes_home())
            with session_stores_lock:
                store = session_stores.get(profile_home)
                if store is None:
                    from gateway.config import load_gateway_config
                    from gateway.session import SessionStore

                    config = load_gateway_config()
                    store = SessionStore(config.sessions_dir, config)
                    session_stores[profile_home] = store
                return store

        provider = InProcessCronScheduler()

        def transport_handler(job: Dict[str, Any], content: str, for_failure: bool) -> Optional[str]:
            request_id = uuid.uuid4().hex
            if not _send_child(
                connection,
                send_lock,
                {
                    'type': 'transport_request',
                    'id': request_id,
                    'job': job,
                    'content': content,
                    'for_failure': for_failure,
                    'profile_home': str(get_hermes_home()),
                },
            ):
                return 'cron transport unavailable'
            deadline = time.monotonic() + 65.0
            with transport_condition:
                while request_id not in transport_results and not stop_event.is_set():
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return 'cron transport timed out'
                    transport_condition.wait(timeout=min(remaining, 0.5))
                response = transport_results.pop(request_id, None)
            if response is None:
                return 'cron transport stopped'
            for action in response.get('persistence_actions') or []:
                with contextlib.suppress(Exception):
                    _apply_delivery_persistence(action, persistence_store())
            unverified_targets = response.get('unverified_targets')
            if isinstance(unverified_targets, list):
                from cron.scheduler_delivery import _record_delivery_verification

                _record_delivery_verification(job, unverified_targets)
            error = response.get('error')
            return str(error) if error else None

        set_process_transport_handler(transport_handler)

        def run_scheduler() -> None:
            try:
                provider.start(
                    stop_event,
                    interval=interval,
                    profile_homes=profile_homes,
                    profile_adapters={},
                    default_profile=default_profile_name,
                    can_dispatch=dispatch_gate.is_set,
                )
            except BaseException as exc:
                _send_child(connection, send_lock, {'type': 'error', 'error': str(exc)})
                stop_event.set()

        def drain_deliveries() -> None:
            while not stop_event.wait(0.25):
                for profile_entry in profile_homes:
                    profile_home = profile_entry[1] if isinstance(profile_entry, tuple) else profile_entry
                    with _profile_cron_scope(Path(profile_home)):
                        with contextlib.suppress(Exception):
                            drain_delivery_queue({}, None)

        scheduler_thread = threading.Thread(target=run_scheduler, name='cron-scheduler', daemon=True)
        delivery_thread = threading.Thread(target=drain_deliveries, name='cron-delivery', daemon=True)
        scheduler_thread.start()
        delivery_thread.start()
        _send_child(connection, send_lock, {'type': 'ready', 'pid': os.getpid()})

        next_heartbeat = time.monotonic() + heartbeat_seconds
        while not stop_event.is_set():
            timeout = max(0.0, min(0.25, next_heartbeat - time.monotonic()))
            if connection.poll(timeout):
                try:
                    command = connection.recv()
                except (EOFError, OSError):
                    break
                command_type = command.get('type')
                request_id = command.get('id')
                if command_type == 'stop':
                    stop_event.set()
                elif command_type == 'dispatch':
                    if command.get('enabled'):
                        dispatch_gate.set()
                    else:
                        dispatch_gate.clear()
                elif command_type == 'jobs_changed':
                    provider.on_jobs_changed()
                elif command_type == 'transport_result' and request_id:
                    with transport_condition:
                        transport_results[str(request_id)] = command
                        transport_condition.notify_all()
                elif command_type == 'status' and request_id:
                    _send_child(
                        connection,
                        send_lock,
                        {
                            'type': 'status',
                            'id': request_id,
                            'active_count': len(get_running_job_ids()),
                            'dispatch_enabled': dispatch_gate.is_set(),
                        },
                    )
                elif command_type == 'fire' and request_id:
                    job_id = str(command.get('job_id') or '')
                    profile_home = str(command.get('profile_home') or default_profile_home)
                    if not dispatch_gate.is_set():
                        _send_child(
                            connection,
                            send_lock,
                            {'type': 'fire', 'id': request_id, 'status': 'error', 'error': 'dispatch paused'},
                        )
                        continue
                    try:
                        with _profile_cron_scope(Path(profile_home)):
                            claim = provider.claim_fire(job_id)
                    except Exception as exc:
                        _send_child(
                            connection,
                            send_lock,
                            {'type': 'fire', 'id': request_id, 'status': 'error', 'error': str(exc)},
                        )
                        continue
                    if claim is None:
                        _send_child(
                            connection,
                            send_lock,
                            {'type': 'fire', 'id': request_id, 'status': 'duplicate'},
                        )
                        continue
                    _send_child(
                        connection,
                        send_lock,
                        {'type': 'fire', 'id': request_id, 'status': 'accepted'},
                    )

                    def fire_claimed(claimed: Any = claim, home: str = profile_home) -> None:
                        with _profile_cron_scope(Path(home)):
                            provider.fire_claimed(claimed)

                    threading.Thread(target=fire_claimed, name=f'cron-fire-{job_id}', daemon=True).start()

            if time.monotonic() >= next_heartbeat:
                _send_child(
                    connection,
                    send_lock,
                    {
                        'type': 'status',
                        'active_count': len(get_running_job_ids()),
                        'dispatch_enabled': dispatch_gate.is_set(),
                    },
                )
                next_heartbeat = time.monotonic() + heartbeat_seconds
    except BaseException as exc:
        _send_child(connection, send_lock, {'type': 'error', 'error': str(exc)})
    finally:
        stop_event.set()
        with transport_condition:
            transport_condition.notify_all()
        with contextlib.suppress(Exception):
            set_process_transport_handler(None)
        with contextlib.suppress(Exception):
            connection.close()


@dataclass(frozen=True)
class CronProcessStatus:
    ready: bool
    degraded: bool
    active_count: int
    restart_count: int
    error: Optional[str]
    pid: Optional[int]


class CronProcessManager:
    process_isolated = True

    def __init__(
        self,
        *,
        profile_homes: list[Any],
        default_profile: str,
        adapters: Dict[str, Any],
        loop: Optional[asyncio.AbstractEventLoop],
        default_profile_name: str = 'default',
        profile_adapters: Optional[Dict[str, Dict[str, Any]]] = None,
        interval: int = 60,
        heartbeat_seconds: float = 2.0,
        heartbeat_timeout: float = 8.0,
        request_timeout: float = 10.0,
        stop_timeout: float = 5.0,
        restart_backoff_initial: float = 0.25,
        restart_backoff_max: float = 8.0,
        context: Optional[SpawnContext] = None,
    ) -> None:
        self._profile_homes = profile_homes
        self._default_profile_home = default_profile
        self._default_profile_name = default_profile_name
        self._adapters = adapters
        self._profile_adapters = profile_adapters or {}
        self._loop = loop
        self._interval = interval
        self._heartbeat_seconds = heartbeat_seconds
        self._heartbeat_timeout = heartbeat_timeout
        self._request_timeout = request_timeout
        self._stop_timeout = stop_timeout
        self._restart_backoff_initial = restart_backoff_initial
        self._restart_backoff_max = restart_backoff_max
        self._context = context or multiprocessing.get_context('spawn')
        self._lock = threading.RLock()
        self._send_lock = threading.Lock()
        self._process_lock = threading.Lock()
        self._closing = threading.Event()
        self._force_restart = threading.Event()
        self._monitor: Optional[threading.Thread] = None
        self._process: Optional[SpawnProcess] = None
        self._connection: Optional[Connection] = None
        self._pending: Dict[str, tuple[threading.Event, Dict[str, Any]]] = {}
        self._ready = False
        self._degraded = True
        self._active_count = 0
        self._restart_count = 0
        self._error: Optional[str] = 'cron scheduler process is starting'
        self._last_message = 0.0
        self._spawned_at = 0.0
        self._dispatch_enabled = True

    @property
    def status(self) -> CronProcessStatus:
        with self._lock:
            process = self._process
            return CronProcessStatus(
                ready=self._ready,
                degraded=self._degraded,
                active_count=self._active_count,
                restart_count=self._restart_count,
                error=self._error,
                pid=process.pid if process is not None and process.is_alive() else None,
            )

    def start(self) -> None:
        with self._lock:
            if self._monitor is not None and self._monitor.is_alive():
                return
            self._closing.clear()
            self._monitor = threading.Thread(target=self._monitor_loop, name='cron-process-monitor', daemon=True)
            self._monitor.start()

    def _spawn(self) -> None:
        parent_connection, child_connection = self._context.Pipe(duplex=True)
        process = self._context.Process(
            target=_scheduler_child_main,
            args=(
                child_connection,
                self._profile_homes,
                self._default_profile_home,
                self._default_profile_name,
                self._interval,
                self._heartbeat_seconds,
                self._dispatch_enabled,
            ),
            name='hermes-cron-scheduler',
            daemon=False,
        )
        try:
            process.start()
        except BaseException:
            parent_connection.close()
            child_connection.close()
            raise
        child_connection.close()
        with self._lock:
            self._process = process
            self._connection = parent_connection
            self._ready = False
            self._degraded = True
            if self._restart_count == 0:
                self._error = 'cron scheduler process is starting'
            self._last_message = time.monotonic()
            self._spawned_at = self._last_message
            self._force_restart.clear()

    def _monitor_loop(self) -> None:
        backoff = self._restart_backoff_initial
        while not self._closing.is_set():
            try:
                self._spawn()
            except BaseException as exc:
                self._mark_degraded(str(exc))
                if self._closing.wait(backoff):
                    break
                backoff = min(self._restart_backoff_max, max(backoff * 2, self._restart_backoff_initial))
                with self._lock:
                    self._restart_count += 1
                continue

            process = self._process
            connection = self._connection
            assert process is not None and connection is not None
            while not self._closing.is_set() and process.is_alive():
                try:
                    if connection.poll(0.2):
                        event = connection.recv()
                        self._handle_event(event, connection)
                except (BrokenPipeError, EOFError, OSError) as exc:
                    self._mark_degraded(str(exc))
                    break
                with self._lock:
                    stale = time.monotonic() - self._last_message > self._heartbeat_timeout
                if stale:
                    self._mark_degraded('cron scheduler process heartbeat timed out')
                    break
                if self._force_restart.is_set():
                    break

            if self._closing.is_set():
                break
            if not process.is_alive():
                self._mark_degraded(f'cron scheduler process exited with code {process.exitcode}')
            self._fail_pending('cron scheduler process unavailable')
            self._stop_process(process)
            self._close_connection(connection)
            with self._lock:
                self._process = None
                self._connection = None
                stable = time.monotonic() - self._spawned_at >= self._heartbeat_timeout
                self._restart_count += 1
            if stable:
                backoff = self._restart_backoff_initial
            if self._closing.wait(backoff):
                break
            if not stable:
                backoff = min(self._restart_backoff_max, max(backoff * 2, self._restart_backoff_initial))

    def _handle_event(self, event: Dict[str, Any], connection: Connection) -> None:
        event_type = event.get('type')
        request_id = event.get('id')
        with self._lock:
            self._last_message = time.monotonic()
            if event_type == 'ready':
                self._ready = True
                self._degraded = False
                self._error = None
            elif event_type == 'error':
                self._ready = False
                self._degraded = True
                self._error = str(event.get('error') or 'cron scheduler process error')
            elif event_type == 'status':
                self._active_count = int(event.get('active_count') or 0)
                if self._ready:
                    self._degraded = False
                    self._error = None
            pending = self._pending.get(str(request_id)) if request_id else None
        if event_type == 'transport_request':
            threading.Thread(
                target=self._handle_transport,
                args=(event, connection),
                name='cron-process-transport',
                daemon=True,
            ).start()
        elif pending is not None:
            waiter, response = pending
            response.update(event)
            waiter.set()
        if event_type == 'error':
            self._force_restart.set()

    def _handle_transport(self, event: Dict[str, Any], connection: Connection) -> None:
        from cron.scheduler_delivery import _deliver_result
        from gateway.run import _profile_runtime_scope

        report: Dict[str, Any] = {}
        error: Optional[str]
        profile_home = str(event.get('profile_home') or self._default_profile_home)
        try:
            with _profile_runtime_scope(Path(profile_home)):
                adapters = self._transport_adapters(profile_home)
                error = _deliver_result(
                    event.get('job') or {},
                    str(event.get('content') or ''),
                    adapters,
                    self._loop,
                    for_failure=bool(event.get('for_failure')),
                    transport_only=True,
                    transport_report=report,
                )
        except BaseException as exc:
            error = str(exc)
        self._send_on(
            connection,
            {
                'type': 'transport_result',
                'id': event.get('id'),
                'error': error,
                'unverified_targets': report.get('unverified_targets', []),
                'persistence_actions': report.get('persistence_actions', []),
            },
        )

    def _transport_adapters(self, profile_home: str):
        from cron.scheduler_preflight import (
            SharedRouteAdapters, _primary_profile_routes_for_current_home)

        profile_name = next((
            str(entry[0]) for entry in self._profile_homes
            if isinstance(entry, tuple) and str(entry[1]) == profile_home
        ), None)
        if profile_name is None and profile_home == self._default_profile_home:
            profile_name = self._default_profile_name
        if profile_name is None:
            raise RuntimeError(f'cron transport profile not served: {profile_home}')
        if profile_name == self._default_profile_name:
            return self._adapters
        if profile_name not in self._profile_adapters:
            raise RuntimeError(f'cron transport adapters unavailable for profile: {profile_name}')
        adapters = self._profile_adapters[profile_name]
        if adapters:
            return adapters
        return SharedRouteAdapters(
            self._adapters, _primary_profile_routes_for_current_home())

    def _send_on(self, connection: Connection, message: Dict[str, Any]) -> bool:
        try:
            with self._send_lock:
                connection.send(message)
            return True
        except (BrokenPipeError, EOFError, OSError):
            return False

    def _send(self, message: Dict[str, Any]) -> bool:
        with self._lock:
            connection = self._connection
            ready = self._ready
        if connection is None or (message.get('type') not in {'stop', 'dispatch'} and not ready):
            return False
        return self._send_on(connection, message)

    def _request(self, message: Dict[str, Any], timeout: Optional[float] = None) -> Dict[str, Any]:
        request_id = uuid.uuid4().hex
        waiter = threading.Event()
        response: Dict[str, Any] = {}
        with self._lock:
            self._pending[request_id] = (waiter, response)
        message['id'] = request_id
        if not self._send(message):
            with self._lock:
                self._pending.pop(request_id, None)
            return {'status': 'error', 'error': 'cron scheduler process unavailable'}
        if not waiter.wait(timeout if timeout is not None else self._request_timeout):
            with self._lock:
                self._pending.pop(request_id, None)
            self._mark_degraded('cron scheduler process request timed out')
            self._force_restart.set()
            return {'status': 'error', 'error': 'cron scheduler process request timed out'}
        with self._lock:
            self._pending.pop(request_id, None)
        return response

    def fire(self, job_id: str, *, profile_home: Optional[str] = None) -> Dict[str, Any]:
        return self._request(
            {
                'type': 'fire',
                'job_id': job_id,
                'profile_home': profile_home or self._default_profile_home,
            }
        )

    def jobs_changed(self) -> None:
        self._send({'type': 'jobs_changed'})

    def notify_jobs_changed(self) -> None:
        self.jobs_changed()

    def set_dispatch_enabled(self, enabled: bool) -> None:
        with self._lock:
            self._dispatch_enabled = enabled
        self._send({'type': 'dispatch', 'enabled': enabled})

    def refresh_status(self) -> CronProcessStatus:
        response = self._request({'type': 'status'}, timeout=min(self._request_timeout, 2.0))
        if response.get('type') == 'status':
            with self._lock:
                self._active_count = int(response.get('active_count') or 0)
        return self.status

    def _mark_degraded(self, error: str) -> None:
        with self._lock:
            self._ready = False
            self._degraded = True
            if error:
                self._error = error

    def _fail_pending(self, error: str) -> None:
        with self._lock:
            pending = list(self._pending.values())
        for waiter, response in pending:
            response.update({'status': 'error', 'error': error})
            waiter.set()

    @staticmethod
    def _close_connection(connection: Connection) -> None:
        with contextlib.suppress(Exception):
            connection.close()

    def _stop_process(self, process: SpawnProcess) -> None:
        with self._process_lock:
            process.join(timeout=self._stop_timeout)
            if process.is_alive():
                process.terminate()
                process.join(timeout=1.0)
            if process.is_alive() and hasattr(process, 'kill'):
                process.kill()
                process.join(timeout=1.0)

    def stop(self) -> None:
        self.close()

    def close(self) -> None:
        self._closing.set()
        self.set_dispatch_enabled(False)
        with self._lock:
            process = self._process
            connection = self._connection
            monitor = self._monitor
        if connection is not None:
            self._send_on(connection, {'type': 'stop'})
        if process is not None:
            self._stop_process(process)
        if connection is not None:
            self._close_connection(connection)
        self._fail_pending('cron scheduler process stopped')
        if monitor is not None and monitor is not threading.current_thread():
            monitor.join(timeout=self._stop_timeout + 2.5)
        with self._lock:
            self._process = None
            self._connection = None
            self._ready = False
            self._degraded = True
            self._active_count = 0
            self._error = 'cron scheduler process stopped'

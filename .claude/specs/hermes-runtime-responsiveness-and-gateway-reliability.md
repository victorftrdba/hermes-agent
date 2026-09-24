# Hermes Runtime Responsiveness and Gateway Reliability

Status: Confirmed 2026-09-23

## Problem

Hermes experiences episodic conversation stalls, slow WebSocket writes, and Gateway watchdog reconnects. Runtime evidence shows that synchronous work can outlive its tool deadline and keep consuming process resources. In particular, `skill_view` recursively scans every configured skill root for each lookup, does not cooperatively observe worker interruption, and may continue after the caller has timed out. Under host CPU and memory pressure, these abandoned scans and YAML parsing have contended for the Python GIL long enough to starve the event loop. Gateway lifecycle hooks also invoke synchronous handlers inline on the event-loop thread.

Telegram recovery already retains a single owned retry task, Desktop resume already defers database hydration off the event loop, and the current SQLite/FTS structures open successfully with fallback behavior. Those paths receive regression coverage but no speculative production change.

## Acceptance criteria

1. `skill_view` reuses a complete discovery snapshot instead of recursively scanning unchanged skill roots for every lookup.
2. Cache invalidation preserves root configuration, project-tier precedence, aliases, plugin namespaces, collision detection, platform filtering, disabled skills, quarantine rules, and file existence checks.
3. Skill discovery observes the existing worker-interrupt signal, stops promptly, raises `InterruptedError`, and never publishes a partial snapshot.
4. Synchronous lifecycle hooks execute outside the event-loop thread while asynchronous hooks remain asynchronous. Handler order, non-`None` result collection, and nonfatal failure semantics remain unchanged.
5. Hook discovery/loading does not block Gateway startup event-loop progress, including previous-run recovery.
6. Regression tests demonstrate loop progress, hook ordering, cache reuse and invalidation, resolution collisions, isolation, and interrupted-scan behavior without leaked tasks or workers.
7. Existing Telegram tests continue to prove single recovery ownership and no overlapping pollers. Existing Desktop session-resume and SQLite fallback tests remain green. Production code in those areas changes only if a focused invariant fails.
8. Activation uses an exact validated commit, a bounded drain/restart, and a live smoke covering repeated skill loads, synchronous hooks, Desktop message/resume, Telegram health, WebSocket latency, and watchdog logs.
9. Operational preflight records current load, CPU, memory/swap, and active long-running work before activation. No unrelated task, container, or virtual machine is stopped without separate exact action-time confirmation.
10. Read-only database health checks succeed before activation. Backup, rebuild, vacuum, FTS repair, session deletion, or schema work is a separately gated operation only if a health check proves it necessary.
11. Telegram command-menu generation never performs skill discovery or skill-file reads on the Gateway event-loop thread, both after connect/reconnect and during lazy forum registration.
12. Unclean-exit lifecycle recovery claims the new process sentinel immediately and runs the potentially long `state.db` integrity diagnostic outside the Gateway event-loop/startup critical path. The result is still persisted and logged, while a stalled check cannot prevent health binding, platform startup, or loop progress.
13. Gateway construction is cache-only and never opens, migrates, checks, repairs, archives, prunes, vacuums, or rebuilds SQLite. After PID, lifecycle, and control-socket claims, one internal subprocess per exact profile DB path performs bootstrap plus configured database and checkpoint maintenance without gating adapters or health. Pending and failed paths remain on durable JSON/JSONL/spool fallback; success attaches only the prepared path off-loop and reconciles fallback state. Shutdown disables late attachment and terminates, kills if necessary, and reaps every bootstrap child.

## Non-goals

- Model or provider tuning.
- Increasing timeouts to hide stalls.
- Session deletion, history truncation, database migration, or speculative FTS repair.
- Telegram credential or network-policy changes without a failing recovery invariant.
- Desktop UI redesign or session-resume rewrite.
- Killing unrelated processes or stopping Docker/virtual machines.

## PR plan

### PR 1: Gateway hook isolation

- `gateway/hooks.py`
- `gateway/run_startup.py`
- `tests/gateway/test_hooks.py`
- `tests/gateway/test_startup_restart_race.py`

Move synchronous discovery and handlers off the event-loop thread while preserving serial ordering and existing async behavior.

### PR 2: Skill discovery cache and cooperative cancellation

- `tools/skills_tool.py`
- `agent/skill_utils.py`
- `tests/tools/test_skills_tool.py`
- `tests/tools/test_skills_tool_discovery_cache.py`
- `tests/agent/test_skill_utils.py`

Share a TTL/signature-scoped, collision-preserving discovery snapshot between list and view paths. Publish only complete scans, revalidate policy-sensitive state, and stop interrupted scans promptly.

### PR 4: Telegram menu skill-scan isolation

- `plugins/platforms/telegram/adapter.py`
- focused Telegram command-menu tests

Move command-menu generation, including any cache miss and skill-file scan, off the Gateway event-loop thread for post-connect housekeeping and lazy forum registration. Preserve command ordering, caps, scope registration, and failure handling.

### PR 5: Lifecycle integrity diagnostic isolation

- `gateway/lifecycle_ledger.py`
- `gateway/run.py`
- focused lifecycle/startup regression tests

Split the fast sentinel claim from the unclean-exit integrity report, retain the report as owned background work, and execute the SQLite check outside the Gateway event loop. Preserve synchronous `record_startup` compatibility and all diagnostic evidence.

### PR 6: Gateway SessionDB bootstrap process isolation

- `gateway/run.py`
- `gateway/run_startup.py`
- `gateway/run_shutdown.py`
- `gateway/session.py`
- `gateway/session_persistence.py`
- `gateway/session_db_recovery.py`
- focused Gateway bootstrap, fallback, multiplexing, and shutdown tests

Keep `GatewayRunner` and its `SessionStore` cache-only until the control socket is live. Bootstrap each exact profile path in an internal Python subprocess, preserve non-Gateway eager `SessionStore` compatibility, attach prepared handles off-loop, reconcile durable fallback data, and enforce bounded child termination and reaping during shutdown. Route lifecycle integrity and periodic SessionDB housekeeping across the same process boundary.

### Activation

Install the exact validated SHA, drain active work, restart the supervised Gateway, and collect live evidence separately from source, PR, and test evidence. Roll back to the prior exact SHA and restart if the bounded smoke regresses.

## Acceptance-to-test checklist

| Criterion | Verification |
| --- | --- |
| 1, 2 | Cache reuse, signature/TTL invalidation, alias, collision, precedence, isolation, disabled/platform/quarantine tests |
| 3 | Interrupted scan exits with `InterruptedError`, publishes no partial cache, and stops scanning |
| 4, 5 | Event-loop progress tests for synchronous hook discovery/handlers, async handler behavior, order, result collection, and startup recovery |
| 6 | Thread/task ownership assertions and repeated focused runs without retry masking |
| 7 | Existing Telegram polling/reconnect, Desktop resume, and SQLite/FTS fallback suites |
| 8, 9, 10 | Exact-SHA activation receipt, preflight snapshot, read-only DB checks, live smoke, and post-restart log comparison |
| 11 | Event-loop progress tests while Telegram command-menu skill discovery is deliberately blocked, covering post-connect and forum paths |
| 12 | A deliberately blocked integrity check cannot delay sentinel reclaim or event-loop heartbeat; release completes the existing diagnostic record and verdict |
| 13 | Constructor spies prove zero SQLite construction; blocked-child startup proves control/health/adapters progress; fallback/reconciliation, failure/backoff/retry with retained lifecycle evidence, exact-path profile isolation and maintenance, checkpoint pruning, shutdown reap/no-late-attach, and lifecycle/housekeeping process-boundary tests pass |

## Risks and mitigations

- A stale cache could hide a newly added skill. Mitigation: bounded TTL plus root/content signature invalidation and existence revalidation.
- Cancellation could publish an incomplete index. Mitigation: build locally and publish atomically only after a complete scan.
- Offloaded hooks could change ordering or context propagation. Mitigation: serial offload with explicit order/context regression tests.
- Arbitrary third-party synchronous hooks can still contend for the GIL. Mitigation: keep the known discovery path bounded and observable; do not claim process isolation that this scope does not provide.
- Restart can interrupt active conversations. Mitigation: bounded drain, exact preflight, supervised restart, and explicit rollback SHA.
- Host resource bursts can amplify product latency independently. Mitigation: correlate live smoke with host metrics and report product and host evidence separately.
- A cached skill map can still miss under profile/platform changes and trigger filesystem reads. Mitigation: keep menu generation off the event loop even when the cache is cold or invalidated.
- An integrity check can take far longer than expected on a multi-gigabyte WAL-backed store. Mitigation: reclaim lifecycle ownership first, run the diagnostic in the retained bootstrap subprocess, and surface failure without gating health or adapter startup.
- A bootstrap child can hang, fail repeatedly, or finish during shutdown; a multiplexed callback could otherwise attach the wrong profile. Mitigation: key ownership and result validation by resolved DB path, retain durable fallback until exact-path success, apply bounded retry backoff, disable callbacks before teardown, then terminate, kill, and reap each child.

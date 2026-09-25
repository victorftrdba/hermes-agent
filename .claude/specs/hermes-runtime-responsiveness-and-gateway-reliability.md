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
14. The built-in cron scheduler runs in a spawn-isolated child process and never opens the central `state.db` or cron delivery SQLite from the Gateway PID. The child preserves default and multiplex profile scoping, due-job claims, recovery, heartbeats, and durable restart-safe delivery persistence; the Gateway performs only child-requested live-adapter transport and returns the result to the child.
15. Authenticated cron-fire admission crosses the cron child boundary before the API acknowledges it. Accepted work retains `202` behavior, duplicate claims retain their existing response, and provider errors remain retryable without executing scheduler or ledger SQLite in the Gateway.
16. A missing, failed, hung, or repeatedly crashing cron child marks cron degraded and restarts with bounded backoff while Gateway health, control, Telegram, webhook, and conversation handling remain responsive. The Gateway never silently falls back to in-process cron execution.
17. Shutdown disables new cron dispatch, reports child active-work status, waits within the configured drain budget, then terminates, kills if necessary, and reaps the cron child without blocking the Gateway event loop. External cron providers preserve their current provider and loopback-fire contracts.
18. The idle async-delegation and process-notification watcher never reads, stats, expands, or parses gateway configuration on the event-loop thread. Environment/config mode changes remain observable without delaying webhook health, Telegram polling, or conversations.
19. Telegram request construction and reconnect never execute the macOS system-proxy probe on the Gateway event-loop thread. Repeated proxy resolution shares a bounded process-wide probe result, including failures, while explicit environment precedence, `NO_PROXY`, `gateway.trust_env`, non-macOS behavior, and refresh after the bounded TTL remain unchanged.
20. Telegram adapter import may load transport primitives but never imports the separate discovery-producer module. Request construction runs the complete fallback-IP discovery producer — its lazy module import, coroutine creation, `AsyncClient` construction, DNS and DoH — off the Gateway event-loop thread and under one configured discovery deadline. Import, construction, and discovery errors or expiry fail closed to seed IPv4 via the existing fallback transport; configured and disabled fallback-IP behavior is unchanged.

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

### PR 7: Gateway cron process isolation

- `cron/scheduler_process.py`
- `cron/scheduler_delivery.py` and/or `cron/delivery_queue.py`
- `gateway/run.py`
- `gateway/run_shutdown.py`
- `gateway/platforms/api_server.py`
- focused cron process, fire-webhook, multiplexing, liveness, and shutdown tests

Move the built-in ticker, central-ledger access, recovery, due-job admission, execution, and delivery persistence into a spawn-isolated child. Split transport from persistence so the child requests a live-adapter send from the Gateway and records its result, expose child health and active-work accounting, and fail closed with bounded restart backoff instead of falling back to the in-process scheduler. Preserve external-provider behavior.

### PR 8: Background-notification config isolation

- `gateway/run_notifications.py`
- `tests/gateway/test_background_process_notifications.py`

Move notification-mode config loading out of the Gateway event loop for both the idle completion drain and per-process watcher. Preserve live environment/config reads and every existing notification mode while proving loop progress when the loader is deliberately blocked.

### PR 9: macOS proxy probe isolation

- `gateway/platforms/base.py`
- `plugins/platforms/telegram/adapter.py`
- `tests/gateway/test_proxy_mode.py`

Cache only the macOS `scutil --proxy` result behind a short monotonic TTL, including failures, and move Telegram's unchanged proxy resolution call off the Gateway event loop. Preserve live environment and bypass evaluation outside the cache while proving that a blocked first probe cannot stall an independent loop heartbeat.

### PR 10: Telegram fallback discovery cold-producer isolation

- `plugins/platforms/telegram/adapter.py`
- `plugins/platforms/telegram/telegram_network.py`
- `plugins/platforms/telegram/telegram_discovery.py`
- `tests/gateway/test_telegram_polling_progress.py`
- `tests/gateway/test_telegram_cold_network_import.py`

Keep transport primitives in the eagerly imported network module and isolate the lazy discovery producer in its own module. Run its import, coroutine creation, `AsyncClient` construction, DNS and DoH inside one bounded worker. On any bounded producer error or expiry, use seed fallback IPs and the existing transport. Preserve configured and disabled fallback-IP behavior, timeout normalization, logs, proxy order, and monkeypatch seams.

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
| 14 | `tests/cron/test_scheduler_process.py` proves the child PID owns built-in ticker, central-state access, and delivery persistence; default and multiplex scopes remain exact; Gateway performs adapter transport without opening SQLite |
| 15 | `tests/gateway/test_cron_fire_webhook.py` proves authenticated admission waits for a correlated child response, accepted and duplicate responses remain compatible, and child/provider failures return retryable errors |
| 16 | `tests/cron/test_scheduler_process.py` crash, hung-child, exponential-backoff, degraded-health, and no-in-process-fallback cases plus `tests/cron/test_87033_cronjob_gateway_liveness.py` loop-progress coverage |
| 17 | `tests/gateway/test_gateway_shutdown.py` and cron-process lifecycle tests prove dispatch pause, active-count drain, graceful exit, terminate/kill escalation, full reap, and unchanged external-provider startup/fire behavior |
| 18 | `tests/gateway/test_background_process_notifications.py` blocks notification-mode loading and proves the Gateway event loop continues to advance for idle drains and per-process watchers |
| 19 | `tests/gateway/test_proxy_mode.py` proves bounded success/failure caching, TTL/reset behavior, non-macOS no-fork behavior, and event-loop progress while Telegram request construction waits on a deliberately blocked system-proxy probe |
| 20 | `tests/gateway/test_telegram_cold_network_import.py` proves adapter import excludes the discovery module and a blocked producer import leaves the loop responsive, then expires to seeded fallback transports; `tests/gateway/test_telegram_polling_progress.py` covers blocked discovery and normalized deadlines |

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
- A cron child can die after claiming work or while the host is heavily swapped. Mitigation: durable claims and delivery queues, correlated admission responses, bounded restart backoff, fail-closed dispatch, and lifecycle tests across crash points.
- Moving cron behind IPC can accidentally serialize live adapters or change external-provider semantics. Mitigation: keep adapters in the Gateway, keep delivery persistence in the child, exchange transport requests/results only, isolate the built-in provider, and retain existing external-provider paths under regression coverage.
- Offloading notification-mode reads could reorder completion handling if launched without ownership. Mitigation: await one owned off-loop read at each existing call site, preserve watcher order, and add blocked-loader loop-progress tests.
- A cached proxy can remain stale within its TTL, and an unbounded executor handoff could outlive request construction. Mitigation: cache only the operating-system probe for 60 seconds, keep environment and bypass checks live, await the owned thread result, expose a reset helper for deterministic refresh, and cover blocked-probe loop progress plus TTL/error refresh behavior.
- Running the fallback-discovery producer inside an off-loop worker moves coroutine creation and imports into that thread and changes where discovery raises. Mitigation: bridge the bounded worker with `run_bounded_sync`, raise `asyncio.TimeoutError` on expiry so the unchanged seed-fallback branch runs, re-raise operation exceptions, and prove an independent heartbeat advances while a synchronously blocked producer is awaited.
- The observed `35.8 GB` swap load can still slow any process after cron isolation. Mitigation: treat host pressure as an independent operational amplifier; acceptance requires eliminating Gateway-PID cron SQLite/GIL contention, not claiming that application code can repair system-wide swap exhaustion.

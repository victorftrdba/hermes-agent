# Gateway terminal-scope cache

## Problem

`build_profile_terminal_scope` parses profile `.env` and `config.yaml` on every runtime-scope entry. Under heavy macOS swap, PyYAML can hold the GIL long enough to stall the Gateway event loop and Telegram polling.

## Acceptance criteria

1. Unchanged profile policy is parsed once and reused safely across threads.
2. Changes to either `.env` or `config.yaml` invalidate the cached policy immediately.
3. Unreadable, malformed, or concurrently replaced files fail closed; no last-known-good or ambient policy is returned.
4. Callers receive independent dictionaries and cannot mutate cached policy.
5. Concurrent cache misses for one profile perform a single parse.

## Non-goals

- Change cron, heartbeat, adapter, or terminal authorization behavior.
- Cache general secrets or merged Hermes configuration.
- Change faulthandler signal behavior in this PR.

## PR plan

One PR changes the terminal-scope builder and its focused tests. The independent faulthandler signal fix is delivered separately.

## Risks

- Weak file signatures could serve stale authority.
- Caching parse failures could prevent recovery after an operator fixes a file.
- Mutable cached mappings could leak policy changes between callers.

## Cross-artifact checklist

- Criteria 1 and 5: repeated and concurrent-call tests.
- Criterion 2: `.env` and `config.yaml` mutation tests.
- Criterion 3: malformed and torn-read tests.
- Criterion 4: defensive-copy test.
- This PR maps to all five criteria; no orphan criterion or test is permitted.

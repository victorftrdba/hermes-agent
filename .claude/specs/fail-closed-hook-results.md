# Fail-closed shell hook results

## Problem

On native main `5cf79ed722`, a pre-tool hook configured with `fail_closed: true` can exit 1 with `{}`, an allow directive, or a modify directive and still permit execution. A malformed explicit modify directive is also silently ignored. These behaviors were reproduced through real hook subprocesses on 2026-09-11.

## Acceptance criteria

1. A failed blocking hook cannot authorize or modify a tool invocation, regardless of stdout. Preserve an explicit block reason when available.
2. A successful blocking hook with a malformed explicit directive fails closed. Successful empty output, `{}`, supported allow output and valid modifications keep their existing behavior.
3. Default fail-open hooks and nonblocking observer events retain compatibility.
4. Regression tests exercise real subprocess hooks and the native dispatch boundary with harmless tools under a temporary Hermes home. Blocked cases have zero tool dispatch.

## Non-goals

No new sandbox, approval protocol, providers, prompt changes, completion policy, hook trust policy, or production/gateway operations. Existing bounded verification reminders remain bounded.

## PR plan and tracking

Personal local task; one native PR for criteria 1-4, followed by reviewed merge to main and installed-source alignment. User has authorized testing, necessary adjustments, autonomy and delivery on main.

## Verification mapping

- Criteria 1-3: parameterized subprocess result contract, first run red on base.
- Criterion 4: registered-hook native dispatch test with harmless sentinel handler.
- Regression: canonical test runner, retries disabled, shell hooks, consent, verification and plugin aggregation tests; lint and compilation on touched Python.

## Risks

Previously failing hooks that emitted an allow/modify result will now block as requested by their fail-closed configuration. Preserve clean no-op and fail-open behavior to avoid breaking intentional observers.

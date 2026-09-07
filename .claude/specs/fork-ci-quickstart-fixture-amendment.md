# Approved CI Portability Amendment: Quickstart Test Isolation

Status: Approved by the user on 2026-09-07.

## Problem

The approved runner-portability PR now executes on standard fork runners. The initial full Python run reached 62% before supersession and recorded two failures in the unchanged `tests/hermes_cli/test_local_quickstart.py`: successful sequencing and satisfied-leg tests expected HTTP 200 but received HTTP 409. The route performs real hardware-fit and engine preflight before any install/download. Those two success-path tests do not request the file's existing `quickstart_ready` fixture, which isolates fit and engine availability for the single-flight contract test.

The test file, route and catalog are unchanged from personal main `4db892c487f00a031f96e2c1168ad7a81bbce5db`. The product's refusal guard must remain intact; a CI failure does not authorize weakening it.

Both the root reviewer and GPT worker independently reproduced the canonical five-test file locally: 3 passed / 2 failed, exit 1, with HTTP 409 instead of 200 in exactly those two success cases. No source files were changed during diagnosis.

## Requested Additional Scope

Permit a test-only change to `tests/hermes_cli/test_local_quickstart.py`, beyond the original workflow/policy-test file list. Reuse or minimally extend the existing readiness fixture for the two sequencing success paths so their outcomes do not depend on the runner's actual hardware or engine version. Keep the same PR because this is an environment-isolation prerequisite for its full CI gate, with no application behavior change.

## Acceptance Criteria

1. All five quickstart contract tests pass through the canonical isolated test wrapper on constrained hardware.
2. Install/download/activation order and satisfied-leg assertions remain unchanged.
3. Unknown-model, no-fit refusal and single-flight rejection checks remain effective; do not bypass them globally.
4. No product route, catalog, runtime guard, model, credential, network-download policy, dependency lock, test-selection rule or test timeout changes.
5. Independent GPT review approves the test-only change; full current-head PR CI/Nix and exact merged-main CI/Nix execute successfully before delivery is called complete.

## Criterion-to-Evidence Checklist

- AC1-AC3: canonical five-test file, isolated constrained-budget reproduction, and actual fixture/assertion diff review.
- AC4: exact changed-file inventory, Ruff, actionlint and policy/classifier suite.
- AC5: fresh GitHub job records at the reviewed PR head and resulting main commit, with skips and unrelated failures reported separately.

## Risks and Non-goals

Over-broad mocking could hide the no-fit refusal or engine compatibility contract. Limit readiness setup to success paths and preserve the independent guard tests. No production behavior, active Hermes installation, credentials, paid probes, upstream writes or Jira mutations are authorized by this proposal.

# Personal-Fork CI Portability

Status: Approved by the user on 2026-09-07.

## Problem

The personal fork's accounting fix is merged as `4db892c487f00a031f96e2c1168ad7a81bbce5db` and installed at that exact clean commit. Its pull-request checks include 18 successes and three unassigned jobs. Python, Windows, and Nix request organization-specific larger runners unavailable to the user-owned public fork. Three additional conditionally selected workflows use the same unsupported runner family. Waiting or rerunning an unchanged workflow does not correct runner selection.

The Docker workflow is successful only because its classifier ran; build and publishing jobs are intentionally upstream-only and were skipped. This proposal does not claim Docker build evidence or change those guards.

## Acceptance Criteria

1. The six affected workflows retain their existing larger-runner selections for the exact `NousResearch/hermes-agent` repository and select concrete standard hosted images for forks: `ubuntu-24.04` or `windows-2025`.
2. Python test parallelism fits the fork runner instead of requesting 96 simultaneous test files. Upstream parallelism remains unchanged.
3. Test discovery, test commands, dependency locks, lane classifiers, triggers, permissions, and publishing guards remain unchanged. No test is removed or newly skipped to obtain a green result.
4. Resource and timeout adjustments are isolated to forks and justified by observed runner capacity or execution evidence.
5. Static validation and regression tests cover both upstream and fork selections, and existing lane-classification tests pass.
6. Every selected job in the new PR and resulting main run receives a runner and executes. A failed, timed-out, or skipped job is reported separately; the pipeline is not called fixed until the applicable run succeeds.

## Non-goals

- Changing Hermes runtime behavior, model routing, provider accounts, credentials, billing plans, or production.
- Changing upstream-only Docker publishing or enabling new deployment workflows on the fork.
- Importing unrelated upstream commits, replacing the full test suite with focused tests, or suppressing baseline failures.
- Treating native Claude Code authentication as a CI concern.

## Approved File Scope

- `.github/workflows/tests.yml`
- `.github/workflows/tests-os.yml`
- `.github/workflows/nix.yml`
- `.github/workflows/js-tests.yml`
- `.github/workflows/rust-tests.yml`
- `.github/workflows/e2e-desktop.yml`
- `tests/ci/test_runner_policy.py`, following the existing pytest conventions.

## Criterion-to-Evidence Checklist

- AC1-AC2: runner-policy tests exercise exact upstream identity and personal-fork and lookalike identities.
- AC3: independent diff review and `tests/ci/test_classify_changes.py` preserve lane selection and suite commands.
- AC4: explicit resource calculation and completed hosted-job evidence.
- AC5: pinned actionlint, focused pytest, and `git diff --check` exit zero.
- AC6: exact-head GitHub job records, followed by the exact merged-main run.

## Resource Rationale

Public standard Linux and Windows hosted runners provide four CPUs. Python therefore runs four test-file workers on forks, while upstream retains 96. The existing Python workflow documents a whole-suite mean of 126 seconds at 96 workers. A conservative linear throughput extrapolation to four workers is `126 * 96 / 4 = 3024` seconds, or 50.4 minutes. The fork-only Python job timeout is 60 minutes to accommodate that estimate and setup; upstream retains 30 minutes. This is a capacity estimate, not an observed fork duration or a completion guarantee.

The existing JS workspace scheduler defaults to `min(units.length, availableParallelism())`, but each check can also start its own Vitest worker pool. Hosted job `101696543975` at PR head `0a17dcbcf0` ran ten checks with an outer limit of four. Web ran from 09:47:17 to 09:47:44 UTC and TUI from 09:46:58 to 09:47:59 UTC while desktop UI and desktop lint also remained active. Locked Vitest 4.1.10 defaults each non-watch pool to `availableParallelism() - 1`: up to three test workers per check, or nine across those three Vitest invocations, plus lint and coordinators on four CPUs.

Eight checks passed. Web's `SessionsPage.test.tsx` exceeded its existing 5000ms test timeout at 5126ms. TUI's `virtualHistoryOffsetCache.test.ts` observed no scroll compensation after its existing 40ms delay. Both unchanged files passed in isolation on macOS with locked dependencies: web 1/1 (695ms test duration), TUI 17/17. This supports resource contention as a contributor, but does not prove causality or a hosted fix.

Forks therefore pass the scheduler's existing `--concurrency 1` option. Upstream retains its argument-free invocation. The scheduler, discovery, inner test commands, assertions, failure aggregation, and 30-minute JS job timeout remain unchanged. The mitigation still requires a successful hosted run. Other workflow timeouts remain unchanged until execution provides evidence for an adjustment.

## One-PR Plan

Resolve personal ownership, create one isolated worktree from personal-fork main, implement with a GPT worker, run regression and static checks, obtain independent GPT review, and deliver one PR to personal-fork main. Preserve the active runtime and unrelated dirty checkouts. Cancel superseded runs only when needed to unblock the replacement run, retaining their historical records.

## Risks

Standard hosted runners have fewer resources; the full suite or Nix closure may expose capacity limitations or previously unexecuted baseline failures. Diagnose those from actual job output without silently reducing coverage. The six-workflow scope covers selected CI lanes, not every upstream-only deployment or Docker workflow.

## Current Evidence

- Accounting PR: https://github.com/victorftrdba/hermes-agent/pull/1
- Queued PR CI: https://github.com/victorftrdba/hermes-agent/actions/runs/34103111436
- Queued PR Nix: https://github.com/victorftrdba/hermes-agent/actions/runs/34103110433
- Pending merged-main CI: https://github.com/victorftrdba/hermes-agent/actions/runs/34103638722
- First assigned fork JS job: https://github.com/victorftrdba/hermes-agent/actions/runs/34107720734/job/101696543975
- GitHub runner availability: https://docs.github.com/en/actions/reference/runners/github-hosted-runners

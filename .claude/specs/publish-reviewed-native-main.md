# Publish reviewed native Hermes changes to the personal main branch

## Problem and authorization

The user explicitly requested delivery of all reviewed work to main. Configuration PR10 is already merged. The three reviewed native commits are active only on local main. The verified personal fork remains at 726db62671 and lacks the newer native module layout required by those commits. A temporary-index transplant failed because required modules did not exist. Publish to victorftrdba/hermes-agent; no NousResearch mutation.

## Scope

Synchronize the personal fork to the exact already-installed vendor revision and the reviewed native fixes at c82b7488bf. Preserve the fork's analytics accounting and hosted-runner portability behavior. Resolve the two merge-conflict files using current extracted modules; do not restore obsolete CLI implementations. Retain previous source and live-test receipts as evidence for their exact tested versions.

## Acceptance criteria

1. Personal remote main contains the reviewed native route, one-shot, and bounded callback fixes, with no dropped fork-specific behavior.
2. Analytics preprocessing binds usage to the correct first-turn session exactly once. Recovery and invalid-response accounting preserve the fork's validated semantics.
3. Focused native, accounting, preprocessing, and CI portability tests pass with test-file retries disabled. Independent critical review approves the actual integration diff against the installed base.
4. The normal PR is merged without a force push. Record the reviewed head, merged tree, applicable GitHub checks, and any first-attempt failures separately.
5. Installed local main is updated only after validation and is tree-identical to the delivered core. Existing user checkouts and protected runtime state remain preserved.

## Criterion-to-evidence mapping

- 1: Exact retained native-file comparisons and route/hook/one-shot regressions.
- 2: Current preprocessing and auxiliary usage behavioral tests.
- 3: Canonical test runner exits, lint, and independent review.
- 4: GitHub PR/main SHA and tree comparison with check results.
- 5: Clean installed main and protected-state comparison.

## Risks and limits

The fork's native architecture predates thousands of already-installed upstream commits. Treat this as a declared vendor synchronization plus a bounded preservation diff. Broad upstream changes are not newly authored or independently audited line by line here. Do not alter provider privacy, credentials, model selections, gateway services, or test retry settings. No new universal model-quality claim follows from publication.

## Local validation

The canonical 11-file suite passed on its first run: 398 passed, zero failed, one platform skip, with file retries disabled and one worker. Touched-file Ruff, the 2,091-pointer compatibility check, and the diff check exited zero. Full-repository Ruff also exited zero; its three invalid-noqa warnings are in files byte-identical to the installed base. The CLI facade and all seven reviewed routing/hook production files remain byte-identical to c82. Independent review and GitHub CI are recorded separately from these local checks.

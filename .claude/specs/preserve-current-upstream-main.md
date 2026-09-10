# Preserve the concurrently updated Hermes runtime

## Problem

During publication of native PR3, the installed checkout reset from the reviewed local runtime to official upstream commit 67764dc0863349a384c16425e73ee8571f3a94b7. The reset actor was not established. The new upstream history must be retained while restoring the custom native routing and hook commits now published in personal main a3c123651edeb5ef5f346267defea24623d86a85.

## Acceptance criteria and evidence

1. Merge both histories without resetting or force-pushing. Verify that the previous personal main, newer installed upstream, and all three custom native commits are ancestors of the resulting main.
2. Preserve named-route validation, child parameter delivery, bounded hook concurrency, and fork accounting. Inspect the actual merge and run the affected native, finite-delegation, accounting, and runner-policy tests with file retries disabled.
3. Require independent critical review, lint and compatibility checks, and exact-head hosted CI before merging the follow-up PR.
4. Fast-forward the clean installed checkout only after the personal main tree matches the reviewed tree. Record protected state immediately around activation and distinguish concurrent runtime changes from this operation.
5. Preserve the official remote as upstream, select the personal fork as origin, retain the existing fork alias, and track main against origin/main. Verify the updater recognizes the fork and preserves its additional commits.

## Scope and risks

The architecture pass and merge-tree rehearsal found no conflicts. The only overlap in the seven custom production files is upstream wording explaining synchronous result delivery when no later-result consumer exists. The newer vendor history was already installed independently; it is not claimed as newly authored or audited line by line. Configuration schema 42 normalization is a separate configuration-repository change with unchanged model and routing values. Do not run live migrations, change credentials or provider privacy, restart the gateway, or rewrite either repository's history.

## PR plan

One native follow-up PR preserves the current upstream history and closes criteria 1 through 5. The configuration PR records its own schema normalization, tests, and final delivery evidence. The first failed CI run and earlier live-test attempts remain immutable historical evidence.

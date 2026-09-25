# Non-fatal Gateway stack-dump signal

## Problem

The Gateway registers `SIGUSR2` with `faulthandler` using `chain=True`. On macOS the prior/default signal action terminates the process after writing the diagnostic stack dump, causing a misleading launchd restart during incident analysis.

## Acceptance criteria

1. `SIGUSR2` writes all-thread diagnostics without chaining to the terminating default action.
2. Existing faulthandler enablement and fallback logging behavior remain unchanged.
3. A focused test verifies registration uses the non-chaining mode.

## Non-goals

- Change watchdog exits, shutdown handling, or another signal.
- Exercise a real signal against the live Gateway during validation.

## PR plan

One PR changes the registration argument and adds a focused unit test.

## Risks

An incorrect mock could test the assertion without exercising Gateway startup code.

## Cross-artifact checklist

- Criteria 1 and 2 map to the registration unit test.
- Criterion 3 is the focused test itself.
- The single PR covers every criterion.

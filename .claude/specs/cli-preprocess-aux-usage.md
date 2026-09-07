# CLI Preprocessing Auxiliary Usage

Status: Approved for the bounded personal-fork remediation.

## Problem

CLI image preprocessing calls the auxiliary vision model before `AIAgent.run_conversation` publishes the ambient accounting context. Successful vision usage therefore has no `SessionDB` and session ID available to `record_aux_usage` and is omitted from session analytics. The central response-validation chokepoint must also record only responses it accepts; recording before structural validation creates phantom usage for malformed responses that are retried or rejected.

## Acceptance Criteria

1. CLI image preprocessing publishes its current `_session_db` and `session_id` for the entire preprocessing operation.
2. The previous accounting context is restored on every exit path.
3. One successful auxiliary vision response records exactly one `vision` row with the response model, provider, and token totals in the active CLI session.
4. Missing database or session context preserves existing preprocessing results and error handling without recording usage.
5. No persistence schema, public API, provider routing, or conversation behavior changes.
6. Normal and successfully recovered auxiliary responses record exactly once after acceptance, while malformed unrecoverable responses record nothing.

## Non-goals

- Changing auxiliary accounting storage or pricing.
- Refactoring the CLI image pipeline.
- Altering vision prompts, fallbacks, result text, or user-visible messages.

## Criterion-to-test Checklist

- AC1, AC3: temporary `SessionDB` integration test invokes `record_aux_usage` from the fake async vision call and asserts the stored row.
- AC2: nested ambient context test proves the caller context is restored after preprocessing.
- AC4: existing success and exception preprocessing tests continue to run with no session handles.
- AC5: focused auxiliary accounting suite, Ruff, byte compilation, and diff checks pass; schema remains untouched.
- AC6: accepted and recovered response tests assert one call count with route attribution; malformed response test asserts zero rows.

## One-PR Plan

1. Add failing CLI integration and context-restoration regressions.
2. Scope the existing auxiliary accounting context around image preprocessing with guaranteed cleanup.
3. Move central auxiliary accounting behind normal or recovered response acceptance.
4. Run focused tests and static checks; deliver one narrow diff.

## Risks

- A missing `finally` could leak a CLI session into later auxiliary calls.
- Re-recording outside the existing auxiliary validation chokepoint could double-count usage.
- Recording before response acceptance could persist phantom usage for a failed attempt that retry logic replaces.
- Broad exception changes could alter current retry guidance or no-context behavior.

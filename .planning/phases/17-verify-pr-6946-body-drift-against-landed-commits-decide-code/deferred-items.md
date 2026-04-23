# Deferred items (Phase 17)

Out-of-scope issues discovered during execution.

## Plan 05 (2026-04-22)

### SqlOptimiserTest 7 `testSampleByFromToBasicWhereOptimisation*` failures -- RESOLVED (2026-04-23)

During Plan 05 Task 2 verification, `mvn -Dtest='SqlOptimiserTest' test -pl core`
reported 7 failures in the `testSampleByFromToBasicWhereOptimisation*` cluster:

- `testSampleByFromToBasicWhereOptimisationGreaterThanOrEqualTo`
- `testSampleByFromToBasicWhereOptimisationWithExistingWhereGreaterThanOrEqualTo`
- `testSampleByFromToBasicWhereOptimisationWithExistingWhereLesserThan`
- `testSampleByFromToBasicWhereOptimisationLesserThan`
- `testSampleByFromToBasicWhereOptimisationWithExistingWhereBetween`
- `testSampleByFromToBasicWhereOptimisationBetween`
- `testSampleByFromToBasicWhereOptimisationNarrowing`

**Original Plan 05 disposition (incorrect):** "pre-existing" -- rationale was a
`git stash` of the Plan 05 Task 2 edit on top of its parent HEAD (`6bdb014f2d`).
The stash reverted only Plan 05 Task 2, not the full Phase 17 stack, so the
regression appeared to survive the stash.

**Correct disposition (2026-04-23):** Phase 17 regression introduced by Plan 02
commit `2a4070b851` ("Minor code hygiene in SAMPLE BY FILL codegen + cursor"),
specifically the **m7 symmetry fix** in `QueryModel.toSink0()` that began
emitting `offset '...'` when `fillOffset != null`. Bisection evidence:

| Commit          | Tests |
|-----------------|-------|
| `origin/master` | 7 / 7 PASS (no `setFillOffset` method on master)          |
| `95d10415e4`    | 7 / 7 PASS (branch HEAD pre-Phase-17 kickoff)             |
| `2a4070b851^`   | 7 / 7 PASS (just before m7 fix)                           |
| `2a4070b851`    | 7 / 7 FAIL (m7 applied; stale test expectations surface)  |
| `405933f77e`    | 7 / 7 FAIL (branch HEAD before follow-up)                 |

The production behaviour was already correct -- `setFillOffset(sampleByOffset)`
has been wired through via `6558835d0d` ("Propagate calendar offset to fill
cursor") since pre-Phase-17. Only the model-text serialization shifted, so
this was a pure test-expectation staleness, not a runtime regression.

**Closed by:** follow-up commit on branch `sm_fill_prev_fast_path` updating
the 11 stale expected-model strings in `SqlOptimiserTest.java:4106-4219` to
include ` offset '10:00'` before ` stride 5d`. The timezone variant at
`SqlOptimiserTest.java:5418` is correctly untouched because `setFillOffset`
does not fire under the timezone-present / no-sub-day-wrap guard.

Resolution verified by:
`mvn -Dtest='SqlOptimiserTest#testSampleByFromToBasicWhereOptimisation*' test -pl core`
-- `Tests run: 7, Failures: 0`.

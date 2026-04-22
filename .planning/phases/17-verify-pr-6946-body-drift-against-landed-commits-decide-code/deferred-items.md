# Deferred items (Phase 17)

Out-of-scope issues discovered during execution. Not fixed in phase.

## Plan 05 (2026-04-22)

### Pre-existing SqlOptimiserTest failures unrelated to D-31 upgrade

During Task 2 verification, `mvn -Dtest='SqlOptimiserTest' test -pl core`
reported 7 failures in the `testSampleByFromToBasicWhereOptimisation*` cluster:

- `testSampleByFromToBasicWhereOptimisationGreaterThanOrEqualTo`
- `testSampleByFromToBasicWhereOptimisationWithExistingWhereGreaterThanOrEqualTo`
- `testSampleByFromToBasicWhereOptimisationWithExistingWhereLesserThan`
- `testSampleByFromToBasicWhereOptimisationLesserThan`
- `testSampleByFromToBasicWhereOptimisationWithExistingWhereBetween`
- `testSampleByFromToBasicWhereOptimisationBetween`
- `testSampleByFromToBasicWhereOptimisationNarrowing`

Confirmed pre-existing by reverting the Plan 05 Task 2 edit (`git stash`) and
rerunning one of the failing tests (`testSampleByFromToBasicWhereOptimisationGreaterThanOrEqualTo`)
which still failed 1/1 without my edit. Unrelated to Plan 05 scope
(D-29 / D-30 / D-31 are all test-hygiene tightenings in specific tests;
none touch the WHERE-optimisation paths).

Touched test (`testSampleByFromToKeyedQuery`) passes 1/1 under the D-31 upgrade.
SampleByFillTest is 127/127 green.

Recommended disposition: file a separate investigation or create a follow-up
todo pinning these failures to a specific commit. Out of scope for Plan 05.

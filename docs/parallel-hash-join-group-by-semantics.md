# Fused hash join semantic and storage matrix (RFC task 9e)

[RFC 130](https://github.com/questdb/rfc/discussions/130), task 9e, expands the
qualification suite after tasks 9a–9d. This change adds eight tests in
`HashJoinGroupBySemanticTest`, strengthens the shared differential assertions,
and expands the three existing concurrent-query tests. No production code or
eligibility rule changes. The experimental switch remains false by default.
Task 10 must now repeat the end-to-end benchmark and make a rollout decision on
this implementation; the earlier task 10 timings remain historical.

## Assertions and fixtures

Every deterministic and seeded positive query checks fused selection and compares
against the same SQL compiled with fusion disabled. Each candidate factory runs
twice. The shared helper now compares every column's name and exact type even
when output is empty, in addition to ordered, type-annotated values and resolved
SYMBOL text. Valid excluded queries check exact ordinary-plan equality and the
same result comparisons. Invalid SQL checks the ordinary error message and
position with the experimental switch both off and on. The one nondeterministic
argument case compares plans and exact pair counts, consumes the random sum,
and checks its range instead of comparing independently generated random values.
All new methods run under the native leak helper; the differential helper checks
that execution releases query registration.

In the new matrix, `r` always denotes the **SQL LHS** and `p` the **SQL RHS**.
INNER and LEFT build `p`; RIGHT normalizes to probing `p` and building `r`.
The two inputs have same-named columns and different reordered/pruned projection
lists. Key roles and argument roles vary independently: no key, LHS-only,
RHS-only, or both-side keys, crossed with LHS-only, RHS-only, or both-side
arguments. Explicit aliases and repeated output references also run above the
aggregate, including expressions that read both inputs.

The fixed data contains duplicates on both sides, matches and distinct misses,
zero, negative and maximum/minimum non-null INT keys, the INT null sentinel,
null/mixed arguments and multiple SYMBOL dictionaries. It asserts that `shared`
has different IDs between inputs and between two SYMBOL columns of one input.
Quarter/half-valued numeric inputs keep SUM intermediates exact; AVG uses the
engine's existing null and intermediate sum/count contracts. Null keys follow
QuestDB's ordinary join behavior, rather than an assumed external SQL rule.

## Coverage map and closed gaps

Names without a class prefix refer to `HashJoinGroupBySemanticTest`. Existing
coverage below is rerun on this change, not inferred from earlier qualification.

| Dimension / prior gap | Named coverage and assertions |
| --- | --- |
| Independent SQL key and argument roles for every allowlisted pair | **New** `testColumnRolesAndEveryAggregateType`: SUM/AVG DOUBLE; COUNT INT/LONG/DOUBLE/SYMBOL and count(*)/count(); all key/argument role combinations, keyed/scalar INNER/LEFT/RIGHT, one/four configured workers and owner/sharded thresholds. Grouping includes join keys and argument columns. Repeated SUMs, repeated SYMBOL output aliases, `coalesce`, cast join-key AVG and cross-input arithmetic exercise pruning and mappings. |
| LHS-only, RHS-only and both-side SYMBOLs as keys and arguments | **New** `testSymbolRolesAcrossAllStoragePairs`: the independent role matrix with two SYMBOL columns per input, the same column grouped and counted, repeated counts, multiple symbol keys/arguments, `length(SYMBOL)` keys and COUNT/SUM/AVG arguments. Runs every native/mixed/Parquet pair, all three joins and both keyed thresholds, plus scalar. |
| Filter/expression-only columns | **New** `testSymbolRolesAcrossAllStoragePairs`: `f` exists only in a null-accepting filter, `s2` only in aggregate expressions, and probe timestamps only in an interval filter. All nine storage pairs and normalized RIGHT are covered. `HashJoinGroupByFunctionsTest.testProjectionAboveJoinAndFilterOnlyPayload` and planner projection/filter tests additionally check the function boundary and final expressions. |
| SYMBOL initialization, cloned views and stable text after reuse | **New** `testSymbolClonesRebindingAndDictionaryGrowth`: keyed/scalar INNER/LEFT/RIGHT, native/mixed/Parquet, owner/sharded thresholds; same factories see new symbols, rebinding inside a conditional SUM argument, truncate/repopulation, null-only dictionaries and empty inputs. Keyed cursors acquire independent clones for all four output SYMBOL columns and check A/B getters, numeric-key resolution, interleaved clone access and `toTop`. Clones close before their cursor. `HashJoinGroupByFunctionsTest.testInitializationStateIsOfferedOncePerExecution`, `testSlotRecordsKeepIndependentSymbolsAndMissState`, and `testProbeSymbolsWithReorderedProjection` cover owner donation, slot isolation, null extension and projected probe routing. |
| Empty inputs, nulls, fanout, extreme keys, ON versus WHERE | **New** `testExtremeKeysNullsAndRejectedRealMatches`: empty LHS/RHS/both, all-null arguments, no matches, all matches with duplicate fanout; INNER/LEFT/RIGHT, keyed/scalar. Build-only ON, null-accepting WHERE and WHERE rejecting every real match are compared separately. `HashJoinGroupByPlannerTest.testAllAggregateTypesKeyedAndUnkeyed` retains the all-function/type empty-input matrix; `AsyncHashJoinGroupByTest.testPostJoinFiltersDoNotManufactureMisses` and `testEmptyInnerSkipsProbeButOuterScans` check explicit semantic/scan invariants. |
| Join-key tops combined with payload/group/filter tops | **New** `testJoinKeyAndPayloadTopsAcrossStoragePairs`: both tables start with timestamp-only rows, then add the join key and every key/argument/filter column. One partition lacks them entirely and another has an internal top followed by data. Native, mixed and all-Parquet conversion on either/both inputs covers all nine pairs, keyed/scalar, all joins, both thresholds, ON filtering and accepted/rejected post-filters. Two-row Parquet groups and one/two-row frames cross row-group, top and partition boundaries. |
| Logical getters, conversions and storage/schema invalidation | `HashJoinGroupByQualificationTest.testAllPayloadTypesNativeParquetColumnTopsAndReuse` covers all enabled payload types on both inputs, SHORT-to-INT keys, FLOAT-to-DOUBLE and INT-to-LONG conversion; `testColumnsAddedAfterParquetConversion` covers absent Parquet columns. `testFilteredStorageChangesRequireRecompilationAndReuse` covers JIT on/off, intervals and normal stale-format guards. `testSourceInvalidationAndSymbolRebinding` covers schema changes on either source and successful recompilation. `AsyncHashJoinGroupByTest.testLogicalTypeConversion` independently checks the fused frame path. |
| Seeded interactions between symbol roles, storage and extreme keys | **New** `testSeededSymbolRoleStorageMatrix`, `Rnd(130,95)`: 18 datasets × 3 worker counts × 3 joins × 4 key roles = **648 SQL cases**, each candidate executed twice. All nine storage pairs occur with each keyed threshold. Randomized argument roles, dictionaries, nulls, fanout, key extremes and small frame sizes complement task 9's `testRandomizedDifferentialMatrix` (`Rnd(130,9)`, 720 SQL cases), which varies selectivity and predicate placement across five physical orientations. |
| Concurrent SYMBOL routing on real workers and owners | **Expanded** `HashJoinGroupByConcurrentTest.testConcurrentQueriesWithOwnerWorkStealing`, `testConcurrentQueriesWithLegacyWorkers`, `testConcurrentQueriesWithFiberWorkers`: two SYMBOL columns on each input, differing ID assignments, simultaneous grouping/counting/expression use, LEFT and normalized RIGHT, scalar, native/mixed/all-Parquet on both inputs, and both keyed thresholds. Three owners synchronize with live builds. **216 executions**, including **18 expected cancellations**, require unaffected peers, exact results, expected actual sharding, reread, partial close, released slots/registration and successful reuse. |
| Unsupported aggregate/function types on either SQL side | **New** `testExcludedFunctionsKeysAndBarriers`: keyed/scalar INNER/LEFT/RIGHT; SUM INT/LONG/FLOAT, AVG INT/LONG, COUNT TIMESTAMP/SHORT, MIN/MAX, FIRST/LAST, distinct count and KSUM. Direct FLOAT columns are tested separately from PostgreSQL-style `::float` (which means DOUBLE); a positive alias-cast case checks the compiled allowlist boundary. Unsupported random SUM arguments retain the ordinary plan. `HashJoinGroupByCandidateTest.testAggregateImplementations` and `testCompiledArgumentContracts` check compiled allowlist and parallel contracts directly. |
| Unsupported keys, residuals, joins and barriers | **New** `testExcludedFunctionsKeysAndBarriers`: composite, expression, LONG/SYMBOL/DOUBLE/TIMESTAMP keys across INNER/LEFT/RIGHT, intervening DISTINCT/LIMIT, full outer, preserved-side ON on LEFT/RIGHT, cross-input WHERE and multiple joins. `HashJoinGroupByQualificationTest.testUnsupportedAccessAndPayloadsKeepOrdinaryPlans` adds cross/asof, indexed/latest/union/table-function sources and unsupported referenced payload types; `testCoveringIndexInputsKeepOrdinaryPlans` checks covering indexes on both sources. `HashJoinGroupByPlannerTest.testSerialProbeFilterKeepsOrdinaryPlan` and `testSharedLateralInputKeepsOrdinaryPlan` cover additional access boundaries. |
| Invalid SQL | **New** `testInvalidSqlKeepsCompilationErrors`: invalid aggregate argument counts, unknown function and missing argument/join columns, with identical error positions and messages under both experimental settings. SQL that accepts an implicit cast is not treated as a compilation error. |
| Disabled gates and zero workers | `HashJoinGroupByPlannerTest.testConfigurationMatrix`: both independent configuration flags, zero/one/four workers, INNER/LEFT/RIGHT, keyed/scalar/count-only, ordinary results and plan selection. The default remains explicitly asserted false. |
| Final output, both merges and resources after 9a–9d | `AsyncHashJoinGroupByTest.testOrderedSolarQueryBothMergePaths`, `testTenThousandJoinedGroupsBothMergePaths`, `testHighCardinalityConcurrentMergeBothPaths`, `testScalarAllAggregateTypesConcurrent` and the initialization/build/probe/decoder/merge/output cancellation/failure tests are rerun. They retain acquired-slot concurrency, actual merge-path assertions, random access where advertised, native/query memory limits, mandatory drain and successful reuse. |

The matrix closes the documented 9e gaps through these new interactions plus the
rerun existing type, storage, capability and lifecycle tests. It does not claim a
full Cartesian product of every predicate, physical type, worker count and
schema mutation. Each enabled path and requested role/orientation/storage
boundary has named positive or negative evidence, including its interactions
with SYMBOL lifetime and column mapping. Unsupported cases remain excluded.

## Validation and reproduction

**2,392 Java tests passed across 71 suites**, with 26 existing conditional skips
and zero failures/errors (2,418 total). This combines the complete affected
regression run with the final semantic/concurrent fixture reruns, counting each
test once. The per-suite results, which the branch does not retain,
include the planner, join, aggregate, frame/Parquet, SYMBOL, query-memory and
cancellation/dispatcher regressions from task 9d plus the new semantic suite.

Initial fixture failures established normal boundaries: `sum(SYMBOL)` can compile
through implicit casting; truncating a filtered Parquet source can request normal
recompilation; and PostgreSQL-style `::float` means DOUBLE, unlike a FLOAT table
column. The final tests use invalid argument counts for compilation errors,
conditional aggregate arguments for uninterrupted dictionary/bind reuse, the
existing explicit format-invalidation tests, and separate direct-FLOAT/alias-cast
plan assertions. The FLOAT fixture uses supported integer-to-FLOAT assignments.
No production behavior was changed to satisfy these tests.

The benchmark package build passed, and the 100,000-row/four-worker smoke
comparison passed all **43 ordered result checks**, including all 40 measured
executions. The branch does not retain the smoke output, which captured
commands/configuration, both plans, ordered results and every sample.

Run with JDK 25 and Maven 3; the native profiles build task 9a's tracked decoder:

```bash
# The branch does not retain the per-suite results CSV that produced this list;
# supply the affected suite simple names, comma-separated.
semantic_test_suites=<comma-separated suite simple names>
mvn -pl core test -P build-rust-library,qdbr-release -Dtest="$semantic_test_suites"
mvn -pl benchmarks -am package -P build-rust-library,qdbr-release -DskipTests -Dmaven.test.skip=true
java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED \
  --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
  -cp benchmarks/target/benchmarks.jar org.questdb.HashJoinGroupByBenchmark \
  --rows=100000 --plants=1000 --workers=4 --warmups=1 --runs=10 --repetitions=2 \
  --revision=task9e-working-tree \
  '--candidate-compiler=org.questdb.HashJoinGroupByBenchmark$PlannerCandidateCompiler'
```

The smoke run is integration evidence. This tests/documentation change does not
rerun task 9c's allocation profiler or assert new allocation measurements; the
production implementation is unchanged. Task 10 still needs the fixed primary
gate and full performance matrix, including task 9d's breaker overhead, uncached
SYMBOL predicates and bounded decoder caches. Keep default enablement a separate
reviewable change; no build-size cutoff, runtime fallback or consumed-input
replay is introduced.

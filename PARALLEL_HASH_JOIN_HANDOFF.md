# Parallel hash join / fused aggregation handoff

Updated 2026-09-12. Branch: `puzpuzpuz_parallel_fused_hash_join`.
Draft PR: [questdb/questdb#7618](https://github.com/questdb/questdb/pull/7618).
Design and dependency order: [RFC 130](https://github.com/questdb/rfc/discussions/130).

## Completed

1. **Define eligibility and build the comparison harness** — commit `2a0fe6f6b3`.
   The pre-construction candidate API, function/type allowlist, predicate and
   mapping contracts, seeded end-to-end runner, and primary ordinary-plan baseline
   are in the branch. See [capabilities and reproduction commands](docs/parallel-hash-join-group-by.md)
   and [the 100-million-row baseline](docs/parallel-hash-join-group-by-baseline.md).
2. **Implement and validate the immutable build boundary** — commit `19c72c58a0`.
   `IntHashJoinBuild` owns tracked native INT hash slots, typed copied payloads,
   duplicate links, and a UTF-16 SYMBOL dictionary. `FrozenHashJoinBuild` supplies
   independent lookup/record/symbol views, execution-local handles, and a boundary
   usable by a later sharded implementation. Build/growth/probe cancellation and
   cleanup/reopen behavior are tested. See [layout, ownership and measurements](docs/parallel-hash-join-group-by-build.md)
   and [all storage samples](docs/parallel-hash-join-group-by-build-results.csv).

3. **Implement joined metadata, record access, and function initialization** — this update.
   `HashJoinGroupByMetadata` composes candidate base indexes with actual compiled
   input projections and copied payload order. `HashJoinGroupByRecord` supplies
   slot-local logical/null getters and symbol routing. `HashJoinGroupByFunctions`
   assembles grouping sinks, aggregate updaters and independent mutable functions,
   initializes owner state once and donates it to workers, prepares output symbols
   before parent initialization, and releases execution state through `cursorClosed()`.
   See [API, ownership and validation](docs/parallel-hash-join-group-by-functions.md).

The branch does not yet execute a fused operator. Default plans, configuration and
EXPLAIN selection remain unchanged. The RFC's 2× end-to-end gate is still pending.

## Next pending task: 4

**Implement the keyed factory, atom, cursor lifecycle, and frame reducer.**

- Add `AsyncHashJoinGroupByRecordCursorFactory` and atom/cursor state using the
  horizon-join and parallel-group-by patterns. Define child/function ownership,
  construction rollback, execution cleanup and reuse before transferring resources.
- Connect build → freeze/publication → probe → merge/output lifecycle. Consume the
  filtered build cursor once. Skip an empty build's probe scan only for INNER;
  LEFT and normalized RIGHT must still process preserved rows and WHERE filters.
- Use `UnorderedPageFrameSequence`, `PerWorkerLocks` and existing frame/filter
  helpers. Each acquired logical slot, including the owner, gets an independent
  frozen probe, joined record, grouping sink/updater and aggregate map fragment.
  Slot identity is independent of threads; preserve owner execution/work stealing.
- Bind the task-3 metadata to compiled child projections. Construct joined records
  and initialize functions after freeze, once per execution. Keep input filters
  and the candidate's build-only ON extraction in the input path; post-join filters
  run after matching/null extension. No replacement miss after rejected matches.
- Initialize logical frame/decoder access inside the slot-release cleanup scope.
  Use `PageFrameMemoryRecord` getters for every enabled format/conversion path.
  Check cancellation in probe-row and duplicate loops. Stop dispatch and drain
  tasks on failure/early close before releasing build or frame state.
- Keep SYMBOL backing through aggregate output and parent initialization. Call
  `functions.cursorClosed()` after draining and finishing output (also after init
  failure), clear joined records, and then release execution backing. Define result
  `toTop` and fresh execution state. Track all live map/frame/build allocations.

Completion requires forced keyed INNER/LEFT and normalized RIGHT results for
empty inputs, misses, duplicates, nulls, filters and expressions, plus concurrent
probing, cancellation in a long duplicate chain, exception-safe slot release,
early close and reuse after failure. Task 5 completes keyed merge/output paths;
task 6 wires planner selection/configuration/diagnostics. Default selection stays
unchanged until that integration and storage qualification exist.

## Contracts to preserve

`open(tracker, breaker)` binds tracking before any native build allocation.
`build(cursor, keyColumn)` consumes a borrowed source cursor once and freezes;
the caller closes that cursor. Freezing ends mutation but the caller must publish
through a task synchronization boundary. Give every acquired logical slot its own
probe and circuit breaker. Drain all probes and finish symbol output before
closing the build. Closing invalidates all handles and views; reopen creates a
fresh execution and dictionary. No build-size fallback or consumed-input replay.

The copied representation costs more build time and retained memory than the
light join's row IDs. Final component measurements cover 10k/100k/1m unique keys
and 10k keys with fanout 10, with both rounds and all regressions retained.
In particular, the 100k unique case probes more slowly. These measurements select
a safe initial boundary; they do not pass the later keyed pipeline's performance
gate or justify default enablement.

## Validation for task 3

```bash
mvn -pl core test \
  -Dtest=HashJoinGroupByFunctionsTest,HashJoinGroupByCandidateTest,IntHashJoinBuildTest,SqlCodeGeneratorWorkerFunctionExtractionTest,GroupByUtilsTest,GroupByRewriteTest,HashJoinTest,JoinRecordMetadataTest
mvn -pl benchmarks -am package -DskipTests -Dmaven.test.skip=true
```

124 tests passed (14 new); benchmark package built successfully. The new tests
compare constructed joined-pair aggregation with ordinary SQL for the owner and
three worker function slots, and also cover owner-only execution. They exercise
original grouping/SUM/AVG and parent ratio expressions, duplicates, all supported
typed/null getters, empty-build SYMBOLs, post-join filters/counts/coalesce,
filter-only payloads, swapped/reordered projections, probe and copied SYMBOLs,
A/B view isolation, bind rebinding and dictionary replacement. Injected functions
verify owner-state donation, initialization counts, partial compile cleanup,
context restoration and successful reuse after initialization failure.

Task 2's 98 tests and four final storage comparisons remain recorded in its
[report](docs/parallel-hash-join-group-by-build.md). Each storage comparison used
two rounds, three warmups and ten measured alternating executions per arm, with
matching pair counts/checksums throughout; all 160 measured samples remain
committed. Task 3 does not rerun or supersede those component measurements.

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
2. **Implement and validate the immutable build boundary** — this update.
   `IntHashJoinBuild` owns tracked native INT hash slots, typed copied payloads,
   duplicate links, and a UTF-16 SYMBOL dictionary. `FrozenHashJoinBuild` supplies
   independent lookup/record/symbol views, execution-local handles, and a boundary
   usable by a later sharded implementation. Build/growth/probe cancellation and
   cleanup/reopen behavior are tested. See [layout, ownership and measurements](docs/parallel-hash-join-group-by-build.md)
   and [all storage samples](docs/parallel-hash-join-group-by-build-results.csv).

The branch does not yet execute a fused operator. Default plans, configuration and
EXPLAIN selection remain unchanged. The RFC's 2× end-to-end gate is still pending.

## Next pending task: 3

**Implement joined metadata, record access, and function initialization.**

- Compile combined metadata and compose the candidate's base-table indexes with
  the actual input projection indexes. Preserve logical aliases/types after
  physical right-to-left normalization. `IntHashJoinBuild` constructor indexes
  address the compiled build record; payload-record indexes address the pruned
  payload list. Include aggregate, grouping and post-join-filter dependencies.
- Add a slot-local joined record that routes left logical frame getters and right
  payload getters. Reuse `FrozenHashJoinBuild.Probe.getRecord()` for copied values;
  integrate `OuterJoinRecord` / `NullRecordFactory` semantics for misses. Reset
  between matches and misses; empty-build SYMBOL null resolution is already
  available from the probe's symbol source.
- Assemble grouping and aggregate functions with `GroupByUtils` and existing
  per-worker compiler helpers. Use independent mutable functions/filters, the
  owner `offerStateTo` initialization contract, and per-execution bind rebinding.
  Do not initialize query-constant state per frame or independently per worker.
- Initialize against the combined symbol source only after the build's dictionary
  exists. Make output symbols available before parent projection/sort initialization,
  and keep dictionary backing alive until aggregate output is finished.
- Focused completion tests must evaluate the original year/month/country grouping
  and sum/avg expressions over constructed pairs, duplicate matches, null/empty
  right payloads, supported post-join filters, pruned/reordered swapped inputs,
  and bind-variable rebinding. Frame dispatch and full factory selection belong
  to tasks 4 and 6 respectively.

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

## Validation for task 2

```bash
mvn -pl core test \
  -Dtest=IntHashJoinBuildTest,HashJoinGroupByCandidateTest,HashJoinTest,LongChainTest,Unordered4MapTest,JoinMemoryTrackerTest
mvn -pl benchmarks -am package -DskipTests -Dmaven.test.skip=true
```

98 tests passed (16 new); benchmark package built successfully. New tests cover
native leaks, complete typed/null payloads, copied SYMBOL ownership and collisions,
randomized duplicates, four concurrent readers with independent flyweights,
byte-limit sweeps, exact growth peaks, cancellation throughout build and duplicate
iteration, source failures, empty builds and reuse. Four final storage comparisons
each ran two rounds, three warmups and ten measured alternating executions per
arm, with matching pair counts/checksums throughout. The report links all 160
measured samples and reproduction commands.

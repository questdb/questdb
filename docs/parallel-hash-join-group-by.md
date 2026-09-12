# Parallel hash join / group by: phase 0 contract

[RFC 130](https://github.com/questdb/rfc/discussions/130), implementation task 1.

This change defines eligibility and a comparison harness. It does not select a
fused execution operator. Default SQL plans, configuration, and EXPLAIN output
remain unchanged. `SqlCodeGenerator.getHashJoinGroupByCandidate()` is the entry
point to call on an optimized GROUP BY model **before** `generateSubQuery()`
constructs the ordinary join. Phase 1 construction and phase 1/task 6 planner
selection will consume this contract.

## Capability table

| Component | Initial contract |
| --- | --- |
| Join | Exactly one INNER or LEFT OUTER equality join. RIGHT OUTER is a candidate only with swapped probe/build inputs and physical LEFT OUTER semantics. No native right/full execution, temporal joins, cross joins, or additional joins in either input. |
| Keys | One direct INT column from each input, including direct aliases through projections. No composite, LONG, SYMBOL, or expression keys. Zero, negative and null keys follow existing QuestDB equality semantics; null keys can match. |
| Probe model | Base table, with direct projections and input filters. A compiled probe must additionally pass `supportsProbeFactory`: page frames directly, or a filter that can be stolen from a frame factory and passes compiled parallel checks. Table functions, aggregate, DISTINCT, window, latest-by, union, and LIMIT barriers are excluded. |
| Build input | A table with supported projections and filters, read once through its ordinary logical record cursor. No build-size threshold or runtime fallback. |
| Copied payload / grouping / referenced probe columns | BOOLEAN, BYTE, SHORT, CHAR, INT, LONG, DATE, TIMESTAMP (including nanosecond precision), FLOAT, DOUBLE, SYMBOL. No STRING, VARCHAR, BINARY, UUID, LONG128/256, DECIMAL, ARRAY, record or geohash payloads in the initial scope. Unreferenced columns do not affect eligibility. |
| SUM / AVG | Only `SumDoubleGroupByFunction` and `AvgDoubleGroupByFunction` with a compiled DOUBLE argument. These cover both fact values and the per-pair installed-capacity denominator. |
| COUNT | `CountLongConstGroupByFunction` (`count(*)` / `count()`), `CountIntGroupByFunction`, `CountLongGroupByFunction`, `CountDoubleGroupByFunction`, `CountSymbolGroupByFunction` with the corresponding INT/LONG/DOUBLE/SYMBOL argument. All return LONG. Additional overloads require an explicit extension. |
| Expressions | Scalar expressions over the supported columns, constants and bind variables, with a supported result type and compiled parallel execution capability. Includes `year(timestamp)`, `month(timestamp)`, arithmetic and `coalesce(double, double)`. Scalar subqueries, arrays, window expressions and functions unstable within an execution are excluded. Final expressions over aggregates, ordering, and LIMIT stay above aggregation. |
| Unsupported aggregates | Every implementation outside the exact class/type allowlist, including `first`, `last`, DISTINCT aggregates, MIN/MAX, integer SUM/AVG, and custom subclasses. Parallel GROUP BY support alone is insufficient to order duplicate joined pairs. |
| Wrappers | Resolve projected aliases recursively, without rewriting the optimized model. Never fuse across an intervening LIMIT, DISTINCT (including DISTINCT rewritten to GROUP BY), other aggregation, window or set operation. |

The descriptor borrows model references and is valid only until subsequent model
mutation/compiler reuse. Its key and payload indexes refer to the **base-table**
columns; construction must compose these with each compiled input projection's
mapping, not use them as projected record indexes. Logical SQL input order remains
unchanged. `getBuildModel`, `getProbeModel`, logical/physical join types and the
swap flag describe the proposed physical orientation without mutating the model.
The returned build column list includes aggregate, grouping and post-join filter
references. Build-only input-filter columns are not copied unless another consumer
needs them. This phase creates no persistent compiled functions or factory state.

## Predicates, initialization and ownership

The existing optimizer owns input-filter pushdown. A remaining ON residual is
accepted only when its compiled function is parallel-safe, stable within an execution, and
references the physical build alone (or no columns). The descriptor borrows that
ON expression as `getBuildOnFilter()` for later extraction without mutating the
model. Every other remaining ON residual rejects the candidate. In a physical left outer join, left WHERE and right ON
filters may be inputs when already safely extracted. Left ON cannot discard a
preserved row. Right WHERE remains post-join unless the optimizer has proved a
rewrite safe. These rules apply after a proposed RIGHT-to-LEFT swap as well.
Post-join conjuncts may each reference one input; cross-input residuals (including
cross-input OR) are rejected. They run after matching/null extension, before any
aggregate update. If all ON matches fail WHERE, there is no replacement null row.

Analysis borrows input factories/functions and never calls `halfClose`, initializes
functions, consumes a cursor, or takes ownership. A successful future constructor
must adopt child factories and functions exactly once, with rollback before
transfer. Only then may a filter wrapper be detached. Compile parallel-safe
slot-local filters, key functions, and aggregate arguments against joined logical
metadata. Initialize the owner once per execution and use the existing
`offerStateTo`/worker initialization contract for clones; bind values are rebound
on each execution, never independently evaluated for each frame.

Copy supported fixed-width values into query-owned build storage. For SYMBOL,
freeze an owned dictionary and encode all payloads with that dictionary's keys;
include the null key and an empty dictionary for empty-build outer joins. Do not
retain transient source-record strings or source symbol IDs after the build cursor
closes. The dictionary lives through aggregate output and final sorting. Task 2
chooses the concrete storage layout; this payload/type/lifetime scope is fixed
before that choice. Every build allocation, dictionary, duplicate link, slot map,
and simultaneous source/destination merge allocation must be tracked.

## Storage and cached-factory validity

The frame implementation contract is logical typed access through
`PageFrameMemoryRecord` and the existing frame-memory/decoder machinery. Native
partitions, column tops, Parquet (including mixed partitions), and logical column
conversion must all use their existing logical getters. Raw INT/DOUBLE page loads
without checking a frame's format and logical conversion are outside this contract.
The build uses the ordinary cursor and its logical typed getters.

Candidate analysis does not inspect the current partition formats and does not
certify storage execution. It acquires readers at the model metadata versions to
resolve types; normal stale-metadata recompilation applies to schema changes.
Partition conversion may occur without a schema-version change. Therefore the
future fused operator must dispatch on each execution's actual frame/decoder
capabilities, including partitions converted after compilation. Native and Parquet
logical access, column tops, and conversions are required qualification cases
before enabling that frame path. A compile-time observation that all current
partitions are native must never enable an unguarded cached factory.

Until those paths have been implemented and qualified, automatic selection stays
disabled. Unsupported frame factories retain ordinary execution. There is no
post-build fallback or input replay. Subsequent storage expansion must preserve
this per-execution contract and add reuse tests (format conversion, schema change,
bind rebinding, and changing symbol dictionaries).

## Reproduce the baseline

The [captured primary baseline](parallel-hash-join-group-by-baseline.md) includes
the existing plan, every measurement, and all 120 ordered result rows.

Build with JDK 25 and Maven 3:

```bash
mvn -pl core test -Dtest=HashJoinGroupByCandidateTest
mvn -pl benchmarks -am package -DskipTests -Dmaven.test.skip=true
java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED \
  --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow -Xmx8g \
  -cp benchmarks/target/benchmarks.jar org.questdb.HashJoinGroupByBenchmark \
  --revision="$(git rev-parse HEAD)" > /tmp/hash-join-baseline.txt
```

The primary case is fixed before comparison: seed 130, 100 million fact rows,
100,000 unique dimension keys, fanout 1, 10% selected keys (ES/IT), uniformly
sampled fact keys, five years starting 2020-01-01, monthly native partitions, four
query workers, three warmups, ten measured runs, two repetitions. Both energy and
capacity use binary fractions; results are compared using exact group keys and
`1e-10 * max(1, abs(expected))` tolerance for numeric values. Every measured run
acquires a fresh cursor and rebuilds the lookup, consuming the final ordered rows.
The baseline retains its parallel dimension filter. No preaggregation is applied.

The runner prints the exact generator SQL and seeds, original SQL, plans, complete
ordered results, revision, JDK/VM flags, OS/architecture/CPU count, heap size,
worker count, each elapsed time, median/min/max, and sampled memory. Record CPU
model and physical memory alongside a performance report (for example `lscpu`
and `free -h` on Linux). Each invocation creates a fresh temporary database and
prints its path; it is retained for inspection. Remove that printed directory
when finished. The primary dataset uses several GB of disk.

A quick harness check keeps the protocol but uses smaller data:

```bash
java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED \
  --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
  -cp benchmarks/target/benchmarks.jar org.questdb.HashJoinGroupByBenchmark \
  --rows=100000 --plants=1000 --warmups=1 --runs=10 --repetitions=2
```

Use `--workers=1`, `2`, `4`; `--plants`, `--selected-percent` (0–100), and `--fanout`
for explicitly labelled variants. These do not replace the predefined primary case.

The runner's `CandidateCompiler` interface accepts the same engine, context and
SQL and returns a factory compiled with the future experimental setting enabled.
Pass its public no-argument implementation as `--candidate-compiler=fully.qualified.Class`.
The adapter must restore any temporary setting before returning or throwing. Both
factories are compiled in the same JVM over the same data. Runs alternate order,
compare every result, and report each repetition separately. The candidate plan
must contain `Async Hash Join Group By`; the baseline must not. Phase 0 has no
candidate implementation and reports only baseline measurements, never a fabricated
speedup. Task 6 can supply the adapter once selection exists.

Memory is the sampled **process native allocation delta**, excluding mapped files
and Java heap, from before cursor acquisition through consumption. The 1 ms sampler
can miss brief peaks and includes shared native pools; this is not an exact query
memory-limit or per-phase measurement. It covers eager acquisition/build work.
Task 6 adds exact operator counters/per-phase instrumentation for the acceptance
gate. The small smoke run establishes reproducibility, not the RFC's 2x gate.

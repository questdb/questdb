# Parallel hash join / group by: capabilities and comparison harness

[RFC 130](https://github.com/questdb/rfc/discussions/130), implementation tasks 1–8 and 6a.

The branch includes the [immutable build boundary](parallel-hash-join-group-by-build.md),
[joined metadata and function initialization](parallel-hash-join-group-by-functions.md),
[keyed execution and merging](parallel-hash-join-group-by-execution.md),
and [scalar execution](parallel-hash-join-group-by-unkeyed.md).
Task 6 connects these components to ordinary SQL compilation behind an experimental
flag. The [planner and diagnostics guide](parallel-hash-join-group-by-planner.md)
documents selection, configuration, ownership, EXPLAIN and benchmark counters.
The [handoff](../PARALLEL_HASH_JOIN_HANDOFF.md) records completed tasks 1–8, the
passed keyed performance gate, and pending V1 qualification in task 9. The
experimental flag remains disabled by default.

## Capability table

| Component | Initial contract |
| --- | --- |
| Join | Exactly one INNER or LEFT OUTER equality join. RIGHT OUTER is a candidate only with swapped probe/build inputs and physical LEFT OUTER semantics. No native right/full execution, temporal joins, cross joins, or additional joins in either input. |
| Keys | One direct INT column from each input, including direct aliases through projections. No composite, LONG, SYMBOL, or expression keys. Zero, negative and null keys follow existing QuestDB equality semantics; null keys can match. |
| Probe model | Base table, with direct projections and input filters. Pure projections above a stealable filter are remapped into frame coordinates. A compiled probe must additionally pass `supportsProbeFactory`: page frames directly, or a filter that can be stolen from a frame factory and passes compiled parallel checks. Table functions, shared cursor models, aggregate, DISTINCT, window, latest-by, union, and LIMIT barriers are excluded. |
| Build input | A table with supported projections and filters, read once through its ordinary logical record cursor. No build-size threshold or runtime fallback. |
| Copied payload / grouping / referenced probe columns | BOOLEAN, BYTE, SHORT, CHAR, INT, LONG, DATE, TIMESTAMP (including nanosecond precision), FLOAT, DOUBLE, SYMBOL. No STRING, VARCHAR, BINARY, UUID, LONG128/256, DECIMAL, ARRAY, record or geohash payloads in the initial scope. Unreferenced columns do not affect eligibility. |
| SUM / AVG | Only `SumDoubleGroupByFunction` and `AvgDoubleGroupByFunction` with a compiled DOUBLE argument. These cover both fact values and the per-pair installed-capacity denominator. |
| COUNT | `CountLongConstGroupByFunction` (`count(*)` / `count()`), `CountIntGroupByFunction`, `CountLongGroupByFunction`, `CountDoubleGroupByFunction`, `CountSymbolGroupByFunction` with the corresponding INT/LONG/DOUBLE/SYMBOL argument. All return LONG. Additional overloads require an explicit extension. |
| Expressions | Scalar expressions over the supported columns, constants and bind variables, with a supported result type and compiled parallel execution capability. Includes `year(timestamp)`, `month(timestamp)`, arithmetic and `coalesce(double, double)`. Scalar subqueries, arrays, window expressions and functions unstable within an execution are excluded. Final expressions over aggregates, ordering, and LIMIT stay above aggregation. |
| Unsupported aggregates | Every implementation outside the exact class/type allowlist, including `first`, `last`, DISTINCT aggregates, MIN/MAX, integer SUM/AVG, and custom subclasses. Parallel GROUP BY support alone is insufficient to order duplicate joined pairs. |
| Aggregation | Keyed maps and unkeyed scalar partials share the build/probe pipeline. Scalar execution allocates no grouping maps and returns exactly one row for empty aggregate input: zero counts and null SUM/AVG. Both require the global parallel GROUP BY and experimental fused flags with positive query-worker slots. |
| Wrappers | Resolve projected aliases recursively during candidate analysis. Never fuse across an intervening LIMIT, DISTINCT (including DISTINCT rewritten to GROUP BY), other aggregation, window or set operation. |

The descriptor borrows model references and is valid only until subsequent model
mutation/compiler reuse. Its key and payload indexes refer to the **base-table**
columns; construction must compose these with each compiled input projection's
mapping, not use them as projected record indexes. Logical SQL input order remains
unchanged. `getBuildModel`, `getProbeModel`, logical/physical join types and the
swap flag describe the proposed physical orientation without mutating the model.
The returned build column list includes aggregate, grouping and post-join filter
references. Build-only input-filter columns are not copied unless another consumer
needs them. Candidate analysis creates no persistent compiled functions or factory state.

## Predicates, initialization and ownership

The existing optimizer supplies input-filter pushdown; fused construction also extracts single-input preserved-probe WHERE conjuncts after RIGHT normalization, before compiling the physical probe. A remaining ON residual is
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
functions, consumes a cursor, or takes ownership. Planner construction compiles and
checks children and functions before filter transfer. A filter wrapper remains
responsible for its handles until `halfClose()` succeeds; the new context and
factory then adopt the transferred resources, including on constructor failure. Compile parallel-safe
slot-local filters, key functions, and aggregate arguments against joined logical
metadata. Initialize the owner once per execution and use the existing
`offerStateTo`/worker initialization contract for clones; bind values are rebound
on each execution, never independently evaluated for each frame.

Copy supported fixed-width values into query-owned build storage. For SYMBOL,
freeze an owned dictionary and encode all payloads with that dictionary's keys;
include the null key and an empty dictionary for empty-build outer joins. Do not
retain transient source-record strings or source symbol IDs after the build cursor
closes. The dictionary lives through aggregate output and final sorting. Task 2
implements this contract in `IntHashJoinBuild`: aligned native payloads, long
duplicate offsets, and an owned UTF-16 dictionary with independent probe views.
Every build allocation, dictionary, duplicate link, slot map,
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
fused operator must dispatch on each execution's actual frame/decoder
capabilities, including partitions converted after compilation. Native and Parquet
logical access, column tops, and conversions are required qualification cases
before enabling that frame path. A compile-time observation that all current
partitions are native must never enable an unguarded cached factory.

The forced execution and planner tests cover logical native/Parquet reads, mixed
partitions, column tops and conversion. Existing child partition-format guards
still request normal stale-plan recompilation; fusion does not bypass them.
Unsupported frame factories retain ordinary execution. There is no
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
SQL and returns a factory compiled with the experimental setting enabled.
Pass its public no-argument implementation as `--candidate-compiler=fully.qualified.Class`.
The adapter must restore any temporary setting before returning or throwing. Both
factories are compiled in the same JVM over the same data. Runs alternate order,
compare every result, and report each repetition separately. The candidate plan
must contain `Async Hash Join Group By`; the baseline must not. Task 6 supplies
`org.questdb.HashJoinGroupByBenchmark$PlannerCandidateCompiler`. Add
`'--candidate-compiler=org.questdb.HashJoinGroupByBenchmark$PlannerCandidateCompiler'`
to the command above to compare the two plans. Omitting the option retains the
baseline-only mode. The runner now emits build/scan/match/filter/group counters
and build/init/probe/merge wall times for the candidate arm. See the
[diagnostics guide](parallel-hash-join-group-by-planner.md) for exact definitions.

Memory is the sampled **process native allocation delta**, excluding mapped files
and Java heap, from before cursor acquisition through consumption. The 1 ms sampler
can miss brief peaks and includes shared native pools; this is not an exact query
memory-limit or per-phase measurement. It covers eager acquisition/build work.
Task 6 adds exact operator counters/per-phase instrumentation for the acceptance
gate. The small smoke run establishes reproducibility, not the RFC's 2x gate.


## Run the task 7 acceptance gate and variants

[The keyed prototype report](parallel-hash-join-group-by-benchmark.md) records the
primary gate and worker/build/selectivity comparisons. Reproduce the sequential
matrix with JDK 25, Maven 3, Bash, and the Linux system-information tools:

```bash
mvn -pl benchmarks -am package -DskipTests -Dmaven.test.skip=true
bash benchmarks/parallel-hash-join-group-by.sh /tmp/hash-join-task7-results
```

The output directory must not already exist. Each case retains a separate fresh
database under `/tmp`; the matrix needs about 20 GB of data storage. Keep other
benchmarks, tests and builds stopped while measuring. The script records exact
commands, environment, source revision and artifact hashes, and retains every
sample, plan and ordered reference result. It runs the fixed four-worker primary
case first, followed by one/two workers, 10k/1m dimension keys, 1%/50% selected
keys, a small-input case, and a small-probe/large-build case. Variants are diagnostics, not substitute gate cases.

Use `--require-primary-gate=true` with `PlannerCandidateCompiler` to run only the
primary gate. The runner requires the fixed 100m/100k/10%/fanout-1/seed-130 input,
four workers, at least three warmups, ten measured runs and two repetitions. It
checks the plans and every ordered result before reporting samples. After all
repetitions it compares each **unrounded** median speedup with 2×, prints PASS/FAIL
and the minimum speedup, and exits unsuccessfully on failure. The displayed
speedup is rounded to three decimal places. A miss keeps Phase 1 experimental and blocks Phase 2
until profiling/revision and a successful rerun. Omitting the option preserves
ordinary baseline-only and non-gating variant runs.

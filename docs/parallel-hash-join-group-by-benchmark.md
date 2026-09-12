# Keyed fused hash join prototype benchmark

[RFC 130](https://github.com/questdb/rfc/discussions/130), task 7. Captured
2026-09-12. The fixed primary workload passes the early **2× median end-to-end**
gate in both repetitions. This permits task 8 (unkeyed aggregation); the
experimental flag remains false by default. Storage/concurrency/resource
qualification and completed-V1 benchmarking/rollout remain tasks 9–10.

## Workload and measurement protocol

The primary case was fixed in task 1 before a fused plan existed: seed 130,
100,000,000 fact rows, 100,000 unique dimension keys, fanout 1, 10% selected keys,
uniform fact keys, five years from 2020-01-01, monthly native partitions, four
query workers, and warm data. The SQL is the original ordered solar query,
including `sum(energy_kwh) / nullif(sum(installed_kwp), 0)` with capacity counted
once per joined reading. There is no preaggregation or prepared-build reuse.

Both plans compile in the same JVM over the same freshly generated dataset.
`DefaultCairoConfiguration` is unchanged: JIT and parallel filtering enabled,
parallel GROUP BY enabled, page frames of 1,000–1,000,000 rows, map capacity 1,024,
and sharding threshold 1,000. Only the candidate compilation temporarily enables
fused selection through `PlannerCandidateCompiler`. The owner can also execute
frame tasks, so one/two/four means configured query workers plus the query owner,
not a claim that exactly that many CPUs execute every phase. Workers are unpinned.

Each of two repetitions warms both arms three times, then records ten executions
per arm, alternating which arm runs first. Timing starts before cursor acquisition
and ends after all ordered rows have been consumed. It includes right filtering,
a fresh copy/freeze build, scan/probe/aggregate, merge, final projection/sort and
result consumption. Cursor cleanup and result comparison happen after timing.
The first warmup provides the ordered reference; every subsequent execution is
compared using exact keys and the existing `1e-10 * max(1, abs(expected))` numeric
tolerance. Samples print only after that check passes.

Every primary candidate execution scans 100,000,000 rows, builds 10,000 rows/keys,
finds 9,998,251 pairs (9.998251% of fact rows), has zero null extensions and sends
all matches to 120 final groups. The ordered primary reference also matches the
previously published [ordinary-plan baseline](parallel-hash-join-group-by-baseline.md).

## Source, environment and reproduction

The measured engine source is commit
`755ee06eac3c2c5c15df3624a26b986b29c4a41c`. The matrix labels it `+working-tree`
because this task adds only benchmark gate/reporting code to the Java runner;
there are no production engine changes. The runner in this commit is the measured
runner. [Environment and SHA-256 hashes](parallel-hash-join-group-by-benchmark/environment.txt)
identify both its source and the built jar; [exact invocations](parallel-hash-join-group-by-benchmark/commands.txt)
record each matrix case. Rebuilding a shaded jar may change its hash because of
build timestamps; source revision and the runner source hash identify the code.

Hardware: AMD Ryzen 9 7900, 12 cores/24 threads, 61 GiB physical memory, local
NVMe filesystem. Linux 7.0.0-30-generic, OpenJDK 25.0.4+7, 8 GiB maximum heap,
CPU governor `powersave`, boost enabled. This was a desktop workstation with
normal background services; no other benchmark, build or regression-test run was
started during measurements. The comparisons ran sequentially without CPU affinity.

```bash
mvn -pl benchmarks -am package -DskipTests -Dmaven.test.skip=true
bash benchmarks/parallel-hash-join-group-by.sh /tmp/hash-join-task7-results
```

The results directory must be new. The script records hardware/configuration,
revision and hashes, generates fresh data in a retained temporary directory per
case, and saves full output. It first invokes the fixed primary case with
`--require-primary-gate=true`, then runs the variants below sequentially. All cases
use three warmups, ten measured executions per arm, and two repetitions. Run only
the primary gate with:

```bash
java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED \
  --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow -Xmx8g \
  -cp benchmarks/target/benchmarks.jar org.questdb.HashJoinGroupByBenchmark \
  --revision="$(git rev-parse HEAD)" \
  '--candidate-compiler=org.questdb.HashJoinGroupByBenchmark$PlannerCandidateCompiler' \
  --require-primary-gate=true > /tmp/hash-join-primary.txt
```

The gate refuses changed primary dataset parameters, any worker count other than four, insufficient warmups/runs/repetitions, or a missing candidate.
It checks the selected plans and ordered results, finishes all repetitions, then
requires the **unrounded** median speedup in every repetition to be at least 2.
Failure returns a nonzero exit code and stops the matrix before Phase 2 work.
There is no timing assertion in ordinary unit tests and no change to planner gates.

## End-to-end results

| Case | Round | Ordinary ms (min–max) | Fused ms (min–max) | Speedup |
| --- | --- | --- | --- | --- |
| [Primary, 4 workers](parallel-hash-join-group-by-benchmark/primary-w4.txt) | 1 | 2323.627 (2317.991–2337.101) | 335.099 (330.697–339.781) | 6.934× |
| [Primary, 4 workers](parallel-hash-join-group-by-benchmark/primary-w4.txt) | 2 | 2321.217 (2316.401–2328.833) | 333.964 (330.445–342.458) | 6.951× |
| [Primary, 1 worker](parallel-hash-join-group-by-benchmark/primary-w1.txt) | 1 | 2238.712 (2235.075–2246.903) | 783.732 (772.243–793.546) | 2.856× |
| [Primary, 1 worker](parallel-hash-join-group-by-benchmark/primary-w1.txt) | 2 | 2239.082 (2231.251–2255.250) | 776.196 (769.478–782.142) | 2.885× |
| [Primary, 2 workers](parallel-hash-join-group-by-benchmark/primary-w2.txt) | 1 | 2314.926 (2304.468–2324.786) | 538.615 (530.588–549.816) | 4.298× |
| [Primary, 2 workers](parallel-hash-join-group-by-benchmark/primary-w2.txt) | 2 | 2310.473 (2302.393–2326.949) | 537.544 (533.811–545.375) | 4.298× |
| [10k dimension keys](parallel-hash-join-group-by-benchmark/build-10k.txt) | 1 | 1992.102 (1985.209–2011.534) | 361.013 (358.224–366.256) | 5.518× |
| [10k dimension keys](parallel-hash-join-group-by-benchmark/build-10k.txt) | 2 | 1996.171 (1988.324–2043.092) | 360.583 (357.337–363.832) | 5.536× |
| [1m dimension keys](parallel-hash-join-group-by-benchmark/build-1m.txt) | 1 | 2878.597 (2856.898–2924.541) | 467.216 (461.012–481.626) | 6.161× |
| [1m dimension keys](parallel-hash-join-group-by-benchmark/build-1m.txt) | 2 | 2877.612 (2849.101–2890.859) | 462.790 (456.677–475.952) | 6.218× |
| [1% selected keys](parallel-hash-join-group-by-benchmark/select-1pct.txt) | 1 | 1607.070 (1599.929–1614.514) | 301.194 (298.699–304.926) | 5.336× |
| [1% selected keys](parallel-hash-join-group-by-benchmark/select-1pct.txt) | 2 | 1607.798 (1605.957–1613.672) | 301.157 (298.008–305.577) | 5.339× |
| [50% selected keys](parallel-hash-join-group-by-benchmark/select-50pct.txt) | 1 | 4754.843 (4728.384–4815.702) | 771.627 (768.271–792.458) | 6.162× |
| [50% selected keys](parallel-hash-join-group-by-benchmark/select-50pct.txt) | 2 | 4748.498 (4733.616–4806.125) | 775.953 (771.824–786.994) | 6.120× |
| [100k fact / 1k dimension](parallel-hash-join-group-by-benchmark/small-input.txt) | 1 | 2.221 (2.041–3.891) | 1.115 (0.879–1.409) | 1.993× |
| [100k fact / 1k dimension](parallel-hash-join-group-by-benchmark/small-input.txt) | 2 | 2.008 (1.945–2.811) | 0.968 (0.791–1.129) | 2.074× |
| [100k fact / 1m dimension](parallel-hash-join-group-by-benchmark/build-heavy.txt) | 1 | 8.575 (7.572–9.911) | 9.680 (8.182–10.553) | 0.886× |
| [100k fact / 1m dimension](parallel-hash-join-group-by-benchmark/build-heavy.txt) | 2 | 7.899 (7.400–9.804) | 8.436 (7.207–10.375) | 0.936× |

Unless a case label overrides it, the primary dataset/configuration applies.
Each cell reports median milliseconds followed by the full minimum–maximum range;
no measured outliers were discarded. The two primary speedups clear the gate with
considerable margin. The runner prints its explicit primary PASS after both rounds.

An initial full comparison on clean `755ee06eac` preceded the gate-only runner
change: 2,333.950/330.907 ms (7.053×) and 2,344.545/330.564 ms (7.093×).
[Its complete output](parallel-hash-join-group-by-benchmark/initial-primary-w4.txt)
is retained as additional evidence, separate from the matrix. No engine tuning or
workload changes were made between that run and the published matrix.

## Phase timings and memory

| Case | Build rows/keys | Groups | Build ms | Init ms | Probe/aggregate ms | Merge ms |
| --- | --- | --- | --- | --- | --- | --- |
| Primary, 4 workers | 10000 | 120 | 0.987 | 0.088 | 333.247 | 0.191 |
| Primary, 1 worker | 10000 | 120 | 0.811 | 0.067 | 776.651 | 0.076 |
| Primary, 2 workers | 10000 | 120 | 1.008 | 0.085 | 537.229 | 0.090 |
| 10k dimension keys | 1000 | 120 | 0.518 | 0.074 | 359.866 | 0.187 |
| 1m dimension keys | 100000 | 120 | 8.250 | 0.099 | 456.293 | 0.195 |
| 1% selected keys | 1000 | 60 | 0.523 | 0.059 | 300.286 | 0.112 |
| 50% selected keys | 50000 | 120 | 2.811 | 0.074 | 772.642 | 0.176 |
| 100k fact / 1k dimension | 100 | 120 | 0.157 | 0.033 | 0.602 | 0.060 |
| 100k fact / 1m dimension | 100000 | 120 | 7.454 | 0.080 | 1.168 | 0.064 |

Phase cells are medians over all 20 candidate samples per case, in milliseconds.
They are wall-clock intervals: build includes right cursor acquisition/filtering,
copy/freeze and close; init prepares probe/functions; probe includes frame work
and drain; merge includes result-cursor preparation. Independently calculated
medians need not sum to the median total. Ordinary-plan phase fields are blank
because that path has no corresponding instrumentation; its total includes all
phases. Every individual phase/counter measurement remains in the raw output and
[combined sample CSV](parallel-hash-join-group-by-benchmark/samples.csv).

| Case | Ordinary sampled peak median / max MiB | Fused sampled peak median / max MiB | Ordinary / fused retained median MiB | Frozen build MiB |
| --- | --- | --- | --- | --- |
| Primary, 4 workers | 0.465 / 0.971 | 0.955 / 1.194 | 0.465 / 0.916 | 0.750 |
| Primary, 1 worker | 0.465 / 1.209 | 0.910 / 1.766 | 0.465 / 0.901 | 0.750 |
| Primary, 2 workers | 0.465 / 1.115 | 0.910 / 1.384 | 0.465 / 0.901 | 0.750 |
| 10k dimension keys | 0.301 / 0.301 | 0.268 / 0.354 | 0.301 / 0.229 | 0.063 |
| 1m dimension keys | 9.105 / 9.105 | 11.817 / 11.910 | 5.153 / 8.166 | 8.000 |
| 1% selected keys | 0.296 / 0.296 | 0.243 / 0.243 | 0.296 / 0.224 | 0.063 |
| 50% selected keys | 2.653 / 3.575 | 4.338 / 4.384 | 2.653 / 4.166 | 4.000 |
| 100k fact / 1k dimension | 0.273 / 0.273 | 0.151 / 0.170 | 0.273 / 0.151 | 0.008 |
| 100k fact / 1m dimension | 9.387 / 9.387 | 11.817 / 11.910 | 5.145 / 8.143 | 8.000 |

Memory cells are **1 ms sampled process native allocation deltas**, in MiB,
measured from before acquisition through final output. They exclude Java heap
and mapped table files, can miss short peaks, and can include shared-pool
allocations. They are not exact query high-water marks. Frozen build bytes are
exact allocated retained build capacity, not a peak for the whole query. Both
source/destination allocations can overlap during growth. The memory-limit tests
from earlier tasks, not these samples, verify exact query-tracker enforcement.

The primary candidate spends more than 99% of its time probing/aggregating;
owner build takes about 1 ms and merge about 0.2 ms. The one/two/four-worker
candidate medians fall from roughly 780 to 538 to 335 ms. These are end-to-end
comparisons of different operators; the measurements do not isolate how much of
the improvement comes from concurrency versus copied-payload access.

The filtered build grows from 1,000 to 10,000 to 100,000 rows across the dimension
size cases, consuming approximately 0.063, 0.750 and 8.000 MiB of frozen native
capacity. Primary retained native memory is 0.916 MiB fused versus 0.465 MiB
ordinary (1.97×); at 100,000 filtered keys it is 8.166 versus 5.153 MiB (1.58×).
Tiny builds can have lower total native memory in the fused pipeline because
other map/pool allocations also differ. Whole-query samples do not establish the
standalone payload representation's cost; the earlier [storage comparison](parallel-hash-join-group-by-build.md)
retains its build-time and row-ID memory regressions.

The 1% variant finds 1,000,554 pairs and produces 60 groups: with the generator's
country assignment, that selection contains only ES. At 50%, it finds 49,994,974
pairs and produces 120 groups. Thus selectivity also changes output cardinality
in the 1% case. Both remain above 5× in this matrix, but neither replaces the
fixed primary acceptance case.

The small-input case improves by about 2×, with visibly larger relative timing
spread. The **100k fact / 1m dimension** diagnostic regresses: fused medians are
9.680 versus 8.575 ms (12.9% slower) and 8.436 versus 7.899 ms (6.8% slower).
Its 100,000 filtered build rows cost a median 7.454 ms to filter/copy/freeze,
while probe/aggregation takes only 1.168 ms. This is consistent with serial build
cost dominating a small probe. The run ranges overlap and these millisecond
cases are noisier than the primary; preserve the observed regression rather than
claiming universal improvement. This evidence supports keeping rollout separate;
it does not introduce a build-size cutoff or runtime fallback.

## Plans, results and validation

Ordinary plan:

```text
Encode sort light
  keys: [country, yr, mo]
    VirtualRecord
      functions: [country,memoize(year),memoize(month),total_energy_kwh,avg_irradiance,total_energy_kwh/nullif(sum,0)]
        GroupBy vectorized: false
          keys: [country,year,month]
          values: [sum(energy_kwh),avg(irradiance_wm2),sum(installed_kwp)]
            SelectedRecord
                Hash Join Light
                  condition: p.plant_id=r.plant_id
                    PageFrame
                        Row forward scan
                        Interval forward scan on: fact_solar_readings
                          intervals: [("2020-01-01T00:00:00.000000Z","2024-12-31T23:59:59.999999Z")]
                    Hash
                        Async JIT Filter workers: 4
                          filter: country in [ES,IT]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: dim_plant

```

Fused plan:

```text
Encode sort light
  keys: [country, yr, mo]
    VirtualRecord
      functions: [country,memoize(year),memoize(month),total_energy_kwh,avg_irradiance,total_energy_kwh/nullif(sum,0)]
        Async Hash Join Group By workers: 4
          logicalJoinType: inner
          physicalJoinType: inner
          inputSwapped: false
          condition: r.plant_id=p.plant_id
          buildStrategy: shared
          keys: [country,year,month]
          keyFunctions: [year(r.reading_ts),month(r.reading_ts)]
          values: [sum(r.energy_kwh),avg(r.irradiance_wm2),sum(p.installed_kwp)]
            Probe
                PageFrame
                    Row forward scan
                    Interval forward scan on: fact_solar_readings
                      intervals: [("2020-01-01T00:00:00.000000Z","2024-12-31T23:59:59.999999Z")]
            Build
                Async JIT Filter workers: 4
                  filter: country in [ES,IT]
                    PageFrame
                        Row forward scan
                        Frame forward scan on: dim_plant
```

Raw files linked in the result table include the exact SQL/generator, both plans,
all ordered reference rows, individual samples and repetition summaries. The
full matrix contains **360 measured executions**; all result checks passed. Each
case also checks 11 of its 12 warmups against its first reference, for 459 explicit
comparisons across 468 total executions. The additional initial primary run
contains 40 more measured executions with matching results. Published text output
normalizes line endings and trailing whitespace; no samples or log lines were
discarded.

The benchmark package build passed. `bash -n` passed for the matrix script.
Eleven CLI rejection checks verified failure before any database was created:
changed rows, plants, selection, fanout, seed or workers; insufficient warmups,
runs or repetitions; an invalid gate boolean; and a missing candidate compiler.
The actual primary workload exercises the successful gate. No production code
changed in task 7; the prior task 6 regression result (853 passing cases and 23
conditional skips) remains historical validation, not a new test run here.

## Decision and remaining work

Task 7 is complete and task 8 can proceed. Keep the experimental default false.
The primary inner/native/low-cardinality result does not qualify unkeyed, outer,
normalized-right, cold/Parquet, high-cardinality, skew/fanout, concurrent-load or
near-memory-limit workloads for rollout. Those remain covered by the separate
V1 qualification and final benchmark matrix. There is no build-size threshold,
serial fallback, input replay, changed denominator or changed acceptance target.

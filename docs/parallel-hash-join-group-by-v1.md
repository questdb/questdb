# Completed-V1 fused hash join benchmark and rollout decision

[RFC 130](https://github.com/questdb/rfc/discussions/130), task 10. Captured
2026-09-12 UTC (handoff updated 2026-09-13 local time) on completed V1. The fixed primary acceptance gate passes in both
rounds. **Keep experimental selection disabled by default.** The wider matrix
exposes build-dominated and output-dominated costs; passing the motivating inner
query does not establish a benefit across V1's capability-based eligibility.
There is no configuration/planner change in this task. Default enablement remains
a separate reviewable decision; there is no build-size threshold, fallback or
consumed-input replay.

## Workloads and protocol

The primary uses exactly task 1's seed 130, 100,000,000 fact rows, 100,000
unique dimension keys, 10% country selection, fanout 1, uniform keys, five years
of monthly native partitions, original ordered solar SQL, and four workers.
Both repetitions have three warmups and ten measured executions per arm in
alternating order. The primary reference exactly matches the retained task 7
ordered reference. One- and two-worker comparisons repeat the same dataset/SQL.

The separate `HashJoinGroupByV1Benchmark` runs targeted diagnostics, normally
with 10,000,000 fact rows and 100,000 source keys, selecting the first 10,000
keys, with seed pairs 130/131 and 132/133. Both inputs have 60 monthly partitions.
Selected keys alternate ES/IT; remaining source keys are DE. Values are binary
fractions. The low-cardinality projection includes country/year/month, COUNT(*),
COUNT(capacity), SUM(energy), AVG(irradiance), and the original per-reading capacity
ratio. Scalar variants remove grouping columns; high-cardinality variants group
by plant/year/month. These variants do not replace or change the primary gate.

Every case retains its complete generator, SQL, actual plans, parameters and all
samples. Each arm compiles against the same data in the same JVM, with only
candidate compilation temporarily enabling the experimental flag. Plans assert
ordinary versus fused selection; V1 diagnostics also assert logical join type,
normalization, scalar/keyed mode and actual physical inputs. Column names/types
must agree. Every execution consumes all ordered rows, compares every key/count
exactly and DOUBLE values with `1e-10 * max(1, abs(expected))` tolerance, including
NaN/null contracts. Large references retain a SHA-256 and preview; comparison
still checks every cell, not only a checksum.

Timing begins immediately before cursor acquisition and ends after final ordered
consumption. It includes filtering, a **fresh build on every execution**, decoding,
probe/aggregation, merge, final projection/sort and materializing result values.
Compilation, data generation/conversion, cache eviction, result verification and
cursor close are outside individual latency. Three warmups precede each of two
rounds of ten measured executions per arm, with alternating first arm.

Concurrent cases use two/four independent contexts and factories, a barrier before
acquisition, and a **shared four-worker pool**. They run homogeneous ordinary or
fused batches, alternating arms. Per-query latency excludes the start barrier;
batch time additionally includes owner scheduling and cursor cleanup. Throughput
is owners divided by median batch time. Workers are unpinned and owners can steal
frame work: four configured workers does not mean exactly four CPUs participate.

Cold cases first warm code, then release idle table readers/writers and invoke
`parallel-hash-join-cold.py` before **each measured execution of either arm**.
The helper fsyncs and uses Linux `POSIX_FADV_DONTNEED` only on that fresh benchmark
directory, then checks `mincore` without touching mapped pages. More than 1%
remaining resident pages fails the case. This measures cold filesystem data with
warm code, not process startup or a reboot. Parquet decode buffers are invalidated
when their page-frame pool binds to a new execution; the existing cache configuration
is unchanged. Native, mixed and all-Parquet paths use the same logical schema.

## Source and reproduction

Measured engine revision: `39c741ac73fd8e70bcbcec7314e137667b4c10fc` (task 9;
production execution code is unchanged from task 8). The `+working-tree` suffix
identifies this task's benchmark-only changes. The retained
[environment](parallel-hash-join-group-by-v1/environment.txt) records source/jar
SHA-256 hashes and configuration; the [continuation environment](parallel-hash-join-group-by-v1/environment-continuation.txt) records the final reproduction script and cold-helper hashes; [commands](parallel-hash-join-group-by-v1/commands.txt)
record every invocation. The runner source hashes match the committed sources.

Hardware: AMD Ryzen 9 7900, 12 cores/24 threads, 64 MiB L3 across two caches,
one NUMA node, 61 GiB RAM, local NVMe; Linux 7.0.0-30-generic, OpenJDK 25.0.4+7,
8 GiB heap, powersave governor and boost enabled. This is a desktop with normal
background services. Measurements ran sequentially, without concurrent builds,
tests or other benchmark jobs. No CPU affinity or NUMA placement was imposed.
This host cannot establish multi-node NUMA behavior.

Default configuration retains JIT/parallel filtering, global parallel GROUP BY,
1,000–1,000,000-row frames, 1,024 map capacity, 1,000-group sharding threshold and
256 MiB per-pool Parquet cache budget. Only the explicitly named near-limit case
sets the query memory limit, to 88 MiB for both arms; otherwise it is unlimited.

```bash
mvn -pl benchmarks -am package -DskipTests -Dmaven.test.skip=true
bash benchmarks/parallel-hash-join-group-by-v1.sh /tmp/hash-join-v1-results
python3 benchmarks/summarize-hash-join-group-by-v1.py /tmp/hash-join-v1-results
```

The output directory must be new; generated data is retained under its printed
temporary path. Optional `CASE_PATTERN='^(primary-w4|cold-native)$'` selects cases
for reproduction. Only `primary-w4` is the acceptance gate; it rejects changed
primary parameters and requires at least 2× unrounded median speedup in every
round. The summary validator requires all samples, result checks, stable counters
and a passing gate whenever the primary file is present. It emits the retained
[summary CSV](parallel-hash-join-group-by-v1/summary.csv), including each round's
median/min/max, phase times, counters, memory samples and batch throughput.

The Parquet scalar RIGHT comparison uses **one million fact rows**, alongside
an otherwise matching native `scalar-right-1m` case. The initial ten-million-row
[pilot](parallel-hash-join-group-by-v1/pilots/parquet-scalar-right-10m.txt) was
stopped during warmups after approximately 3m18s process elapsed, with no measured
samples. Its full output and original command remain retained. The smaller
orientation/storage comparison was selected before collecting measured samples;
the primary gate and the ten-million-row INNER storage comparisons are unchanged.
A separate nonempty singleton build over a broad seeded key domain supplies
zero-match coverage in addition to the empty-build cases. The matrix is targeted
coverage of the RFC axes, not a full Cartesian product or production trace.

## Memory interpretation

The runner samples the existing **per-query native tracker every 1 ms**, starting
before acquisition, and samples again before cursor close. Binding/unbinding and
sampling synchronize so concurrent queries cannot be attributed to a recycled
tracker. These are sampled lower bounds on high-water marks; brief allocation
peaks can be missed. Tracker limits remain enforced at allocation time, regardless
of sampling. The near-limit case bounds actual tracked usage independently.

Process-native deltas include shared worker/decoder pools and transient source/
destination overlap; under concurrency they describe the whole batch and must
not be summed per owner. Query-tracked memory and process-native deltas cover
different allocations. Neither includes Java result objects/heap or mapped table
files. Frozen `build_bytes` measures exact retained build capacity, not total
execution memory. The raw primary retains its old process-memory columns and
adds sampled query memory; V1 files also record retained query bytes. Prior
qualification tests, not these sampled values, establish cleanup and exact limit
enforcement.


## End-to-end results

Every cell lists round 1 / round 2. Speedup is ordinary median divided by fused median; values below 1 regress. Min/max spreads and every sample remain in the linked raw files and summary CSV.

| Case | Ordinary median ms | Fused median ms | Speedup |
| --- | --- | --- | --- |
| [primary-w4](parallel-hash-join-group-by-v1/primary-w4.txt) | 2295.795 / 2296.946 | 319.808 / 321.527 | 7.179 / 7.144× |
| [primary-w1](parallel-hash-join-group-by-v1/primary-w1.txt) | 2228.141 / 2231.851 | 732.022 / 732.818 | 3.044 / 3.046× |
| [primary-w2](parallel-hash-join-group-by-v1/primary-w2.txt) | 2352.321 / 2353.214 | 496.498 / 495.845 | 4.738 / 4.746× |
| [inner](parallel-hash-join-group-by-v1/inner.txt) | 235.601 / 234.731 | 37.803 / 35.959 | 6.232 / 6.528× |
| [left](parallel-hash-join-group-by-v1/left.txt) | 595.161 / 595.269 | 97.275 / 96.368 | 6.118 / 6.177× |
| [right](parallel-hash-join-group-by-v1/right.txt) | 3820.615 / 3784.292 | 111.939 / 118.360 | 34.131 / 31.973× |
| [scalar-inner](parallel-hash-join-group-by-v1/scalar-inner.txt) | 216.608 / 215.438 | 29.808 / 29.751 | 7.267 / 7.241× |
| [scalar-left](parallel-hash-join-group-by-v1/scalar-left.txt) | 363.918 / 360.709 | 75.994 / 72.362 | 4.789 / 4.985× |
| [scalar-right](parallel-hash-join-group-by-v1/scalar-right.txt) | 2698.409 / 2680.711 | 60.802 / 59.373 | 44.380 / 45.150× |
| [inner-swapped](parallel-hash-join-group-by-v1/inner-swapped.txt) | 37.266 / 34.715 | 38.517 / 37.767 | 0.968 / 0.919× |
| [left-swapped](parallel-hash-join-group-by-v1/left-swapped.txt) | 41.128 / 35.075 | 33.730 / 33.309 | 1.219 / 1.053× |
| [right-swapped](parallel-hash-join-group-by-v1/right-swapped.txt) | 22.562 / 22.420 | 44.324 / 44.223 | 0.509 / 0.507× |
| [zero-matches](parallel-hash-join-group-by-v1/zero-matches.txt) | 59.092 / 58.453 | 7.648 / 7.360 | 7.726 / 7.942× |
| [empty-build](parallel-hash-join-group-by-v1/empty-build.txt) | 53.060 / 51.195 | 0.468 / 0.370 | 113.420 / 138.509× |
| [empty-build-left](parallel-hash-join-group-by-v1/empty-build-left.txt) | 443.012 / 441.775 | 81.959 / 81.514 | 5.405 / 5.420× |
| [singleton-build](parallel-hash-join-group-by-v1/singleton-build.txt) | 56.264 / 55.183 | 8.797 / 8.136 | 6.396 / 6.782× |
| [match-001pct](parallel-hash-join-group-by-v1/match-001pct.txt) | 74.240 / 74.623 | 12.323 / 12.149 | 6.025 / 6.142× |
| [match-1pct](parallel-hash-join-group-by-v1/match-1pct.txt) | 167.626 / 167.131 | 31.919 / 31.687 | 5.252 / 5.274× |
| [match-50pct](parallel-hash-join-group-by-v1/match-50pct.txt) | 486.335 / 488.853 | 79.929 / 79.750 | 6.085 / 6.130× |
| [match-100pct](parallel-hash-join-group-by-v1/match-100pct.txt) | 776.185 / 772.626 | 116.268 / 115.837 | 6.676 / 6.670× |
| [build-beyond-cache](parallel-hash-join-group-by-v1/build-beyond-cache.txt) | 3705.763 / 3633.386 | 693.058 / 688.479 | 5.347 / 5.277× |
| [build-source-10m](parallel-hash-join-group-by-v1/build-source-10m.txt) | 238.376 / 235.857 | 36.911 / 35.258 | 6.458 / 6.689× |
| [post-selective](parallel-hash-join-group-by-v1/post-selective.txt) | 264.502 / 263.316 | 38.258 / 37.383 | 6.914 / 7.044× |
| [post-null-accepting](parallel-hash-join-group-by-v1/post-null-accepting.txt) | 629.386 / 628.950 | 93.485 / 93.759 | 6.732 / 6.708× |
| [post-reject-all](parallel-hash-join-group-by-v1/post-reject-all.txt) | 218.619 / 217.504 | 30.089 / 29.812 | 7.266 / 7.296× |
| [fanout-10](parallel-hash-join-group-by-v1/fanout-10.txt) | 634.628 / 641.874 | 108.944 / 109.962 | 5.825 / 5.837× |
| [hot-key-90pct](parallel-hash-join-group-by-v1/hot-key-90pct.txt) | 471.561 / 470.352 | 83.204 / 82.490 | 5.668 / 5.702× |
| [hot-chain](parallel-hash-join-group-by-v1/hot-chain.txt) | 3681.775 / 3678.220 | 636.096 / 640.352 | 5.788 / 5.744× |
| [high-cardinality](parallel-hash-join-group-by-v1/high-cardinality.txt) | 432.269 / 426.773 | 234.223 / 221.315 | 1.846 / 1.928× |
| [compressed-groups](parallel-hash-join-group-by-v1/compressed-groups.txt) | 652.185 / 651.082 | 134.662 / 129.460 | 4.843 / 5.029× |
| [near-input-groups](parallel-hash-join-group-by-v1/near-input-groups.txt) | 37.080 / 35.916 | 32.139 / 32.081 | 1.154 / 1.120× |
| [small-input](parallel-hash-join-group-by-v1/small-input.txt) | 3.194 / 2.601 | 1.340 / 1.217 | 2.383 / 2.136× |
| [small-interval](parallel-hash-join-group-by-v1/small-interval.txt) | 1.137 / 0.549 | 1.325 / 0.858 | 0.858 / 0.640× |
| [build-heavy](parallel-hash-join-group-by-v1/build-heavy.txt) | 7.885 / 6.330 | 6.233 / 6.019 | 1.265 / 1.052× |
| [concurrent-2](parallel-hash-join-group-by-v1/concurrent-2.txt) | 236.714 / 235.767 | 58.486 / 59.295 | 4.047 / 3.976× |
| [concurrent-4](parallel-hash-join-group-by-v1/concurrent-4.txt) | 237.110 / 236.160 | 90.206 / 97.594 | 2.629 / 2.420× |
| [concurrent-scalar-left](parallel-hash-join-group-by-v1/concurrent-scalar-left.txt) | 337.656 / 339.042 | 208.253 / 207.466 | 1.621 / 1.634× |
| [mixed](parallel-hash-join-group-by-v1/mixed.txt) | 301.353 / 301.582 | 39.788 / 38.525 | 7.574 / 7.828× |
| [parquet](parallel-hash-join-group-by-v1/parquet.txt) | 321.873 / 321.592 | 43.167 / 42.168 | 7.457 / 7.627× |
| [scalar-right-1m](parallel-hash-join-group-by-v1/scalar-right-1m.txt) | 172.568 / 168.234 | 7.828 / 6.729 | 22.046 / 25.002× |
| [parquet-scalar-right](parallel-hash-join-group-by-v1/parquet-scalar-right.txt) | 242.516 / 244.761 | 9.620 / 7.925 | 25.209 / 30.886× |
| [cold-native](parallel-hash-join-group-by-v1/cold-native.txt) | 324.691 / 320.441 | 102.273 / 100.405 | 3.175 / 3.191× |
| [cold-parquet](parallel-hash-join-group-by-v1/cold-parquet.txt) | 397.791 / 392.231 | 74.929 / 73.534 | 5.309 / 5.334× |
| [near-memory-limit](parallel-hash-join-group-by-v1/near-memory-limit.txt) | 2580.335 / 2532.822 | 413.423 / 412.181 | 6.241 / 6.145× |

## Phase and memory results

Phase medians combine both rounds of candidate samples, in milliseconds. Query memory cells are median / maximum 1 ms sampled peaks across both rounds, in MiB. Frozen build capacity is exact. Each CSV round retains separate process-native measurements and retained-query values.

| Case | Build / probe / merge ms | Ordinary query MiB | Fused query MiB | Frozen build MiB |
| --- | --- | --- | --- | --- |
| primary-w4 | 1.064 / 319.367 / 0.179 | 0.447 / 0.447 | 0.924 / 1.001 | 0.750 |
| primary-w1 | 0.838 / 731.177 / 0.076 | 0.447 / 0.447 | 0.895 / 1.001 | 0.750 |
| primary-w2 | 0.876 / 494.791 / 0.096 | 0.447 / 0.447 | 0.895 / 1.001 | 0.750 |
| inner | 0.969 / 34.973 / 0.185 | 0.447 / 0.447 | 0.924 / 1.001 | 0.750 |
| left | 0.851 / 95.048 / 0.200 | 0.395 / 0.396 | 0.965 / 1.001 | 0.750 |
| right | 1.022 / 113.569 / 0.211 | 131.145 / 131.146 | 0.965 / 1.001 | 0.750 |
| scalar-inner | 0.718 / 28.731 / 0.018 | 0.313 / 0.313 | 0.751 / 0.875 | 0.750 |
| scalar-left | 0.710 / 73.264 / 0.019 | 0.250 / 0.250 | 0.751 / 0.875 | 0.750 |
| scalar-right | 0.745 / 59.258 / 0.018 | 131.000 / 131.000 | 0.751 / 0.875 | 0.750 |
| inner-swapped | 32.282 / 5.316 / 0.199 | 16.322 / 16.322 | 48.500 / 48.500 | 32.500 |
| left-swapped | 28.361 / 4.770 / 0.186 | 16.260 / 16.260 | 48.500 / 48.500 | 32.500 |
| right-swapped | 38.055 / 5.835 / 0.180 | 0.283 / 0.283 | 48.500 / 48.500 | 32.500 |
| zero-matches | 0.314 / 7.034 / 0.018 | 0.256 / 0.256 | 0.151 / 0.151 | 0.001 |
| empty-build | 0.303 / 0.000 / 0.021 | 0.256 / 0.256 | 0.131 / 0.131 | 0.001 |
| empty-build-left | 0.361 / 80.962 / 0.128 | 0.256 / 0.256 | 0.151 / 0.151 | 0.001 |
| singleton-build | 0.342 / 7.896 / 0.081 | 0.256 / 0.256 | 0.151 / 0.151 | 0.001 |
| match-001pct | 0.351 / 11.302 / 0.148 | 0.261 / 0.261 | 0.171 / 0.175 | 0.001 |
| match-1pct | 0.666 / 30.439 / 0.185 | 0.283 / 0.283 | 0.237 / 0.237 | 0.063 |
| match-50pct | 2.710 / 76.520 / 0.177 | 2.635 / 3.380 | 4.174 / 4.174 | 4.000 |
| match-100pct | 5.434 / 109.627 / 0.185 | 6.630 / 6.630 | 8.174 / 8.174 | 8.000 |
| build-beyond-cache | 269.239 / 420.684 / 0.205 | 104.130 / 104.130 | 160.001 / 160.001 | 128.000 |
| build-source-10m | 1.875 / 33.714 / 0.170 | 0.447 / 0.447 | 0.924 / 1.001 | 0.750 |
| post-selective | 0.967 / 35.918 / 0.173 | 0.385 / 0.385 | 0.924 / 1.001 | 0.750 |
| post-null-accepting | 0.961 / 91.508 / 0.229 | 0.395 / 0.395 | 0.965 / 1.001 | 0.750 |
| post-reject-all | 0.851 / 28.795 / 0.020 | 0.380 / 0.380 | 0.900 / 1.001 | 0.750 |
| fanout-10 | 3.656 / 104.918 / 0.184 | 2.322 / 2.323 | 4.674 / 6.501 | 4.500 |
| hot-key-90pct | 0.822 / 81.254 / 0.176 | 0.447 / 0.448 | 0.924 / 1.001 | 0.750 |
| hot-chain | 0.475 / 637.309 / 0.077 | 0.256 / 0.256 | 0.182 / 0.182 | 0.032 |
| high-cardinality | 0.851 / 58.359 / 8.132 | 56.313 / 56.313 | 92.340 / 93.930 | 0.750 |
| compressed-groups | 0.328 / 131.585 / 0.437 | 1.003 / 1.003 | 6.381 / 6.400 | 0.006 |
| near-input-groups | 4.263 / 3.860 / 1.051 | 19.000 / 19.000 | 27.892 / 28.577 | 6.000 |
| small-input | 0.348 / 0.503 / 0.069 | 0.263 / 0.263 | 0.143 / 0.162 | 0.008 |
| small-interval | 0.741 / 0.109 / 0.023 | 0.442 / 0.442 | 0.880 / 0.880 | 0.750 |
| build-heavy | 4.971 / 0.765 / 0.059 | 6.630 / 6.630 | 8.147 / 10.001 | 8.000 |
| concurrent-2 | 0.927 / 57.572 / 0.162 | 0.447 / 0.448 | 0.924 / 1.001 | 0.750 |
| concurrent-4 | 0.955 / 92.678 / 0.046 | 0.447 / 0.447 | 0.924 / 1.001 | 0.750 |
| concurrent-scalar-left | 0.763 / 206.880 / 0.012 | 0.250 / 0.313 | 0.751 / 0.875 | 0.750 |
| mixed | 0.994 / 37.358 / 0.142 | 10.971 / 10.971 | 12.510 / 13.466 | 0.750 |
| parquet | 0.992 / 40.716 / 0.121 | 10.971 / 10.971 | 12.640 / 13.450 | 0.750 |
| scalar-right-1m | 0.802 / 6.210 / 0.011 | 19.000 / 19.000 | 0.751 / 0.751 | 0.750 |
| parquet-scalar-right | 0.956 / 7.113 / 0.012 | 45.478 / 45.478 | 2.991 / 3.006 | 0.750 |
| cold-native | 3.832 / 96.095 / 0.202 | 0.447 / 0.447 | 0.924 / 1.001 | 0.750 |
| cold-parquet | 6.265 / 66.738 / 0.117 | 10.971 / 10.972 | 12.494 / 12.923 | 0.750 |
| near-memory-limit | 100.582 / 311.846 / 0.198 | 52.130 / 52.130 | 80.001 / 80.001 | 64.000 |

## Findings and rollout decision

The primary gate passes with **7.179× and 7.144×** median speedup. Each candidate
execution builds 10,000 rows/keys, scans 100,000,000 fact rows and aggregates
9,998,251 matched pairs into 120 groups. Candidate medians scale from roughly
732 ms at one configured worker to 496 ms at two and 321 ms at four. Primary
query-native sampled peaks are about 0.9–1.0 MiB fused versus 0.447 MiB ordinary;
the frozen copied build is 0.750 MiB.

The diagnostics explain why this does **not justify default enablement**:

- **Orientation can dominate both time and memory.** The fact-probing LEFT case
  improves about 6.1×. Its equivalent RIGHT spelling improves 32–34×, including
  the advantage of normalizing to a small dimension build: the ordinary RIGHT
  factory builds the large fact input. In the opposite `right-swapped` spelling,
  normalization instead copies one million fact rows for a small dimension
  probe. It takes 44.3/44.2 ms versus 22.6/22.4 ms ordinary, **96–97% slower** in
  both rounds. Its sampled query peak is **48.5 MiB versus 0.283 MiB**, reflecting
  the different physical build. This is not an isolated parallel-probe comparison.
  `inner-swapped` also regresses by 3–9%, although its run ranges overlap;
  `left-swapped` improves modestly. All actual plans remain available.
- **Small effective scans expose fixed costs.** Restricting the ten-million-row
  source to one hour scans only **229 rows**, finds 23 pairs and returns two
  groups, while retaining the 10,000-row build. Fused medians are 1.325/0.858 ms
  versus 1.137/0.549 ms ordinary, **17–56% slower**. These are noisy sub-millisecond
  differences in the second round, not evidence for a numeric planner cutoff.
  The 100,000-row/small-build case improves 2.1–2.4×. The current build-heavy
  diagnostic improves only 1.05–1.27×; task 7's different diagnostic generator/SQL
  and its historical regression are not substituted for these samples.
- **Large output reduces the end-to-end gain and raises aggregate memory.**
  486,759 final groups improve 1.85–1.93×, with sampled query peaks reaching
  93.93 MiB fused versus 56.31 MiB ordinary. The candidate spends about 58 ms in
  probing and 8 ms merging, with substantial additional ordered output and
  consumption cost. With 99,157 groups from 100,000 matching input rows, gains
  narrow to 1.12–1.15×. At 6,000 groups from ten million matches, gains are
  4.84–5.03×. There is no preaggregation rewrite in these comparisons.
- **Memory includes growth, not only frozen state.** The beyond-cache case builds
  two million keys into 128 MiB of frozen capacity and reaches a sampled
  160 MiB query peak; ordinary peaks at 104.13 MiB. It still improves 5.3×.
  The near-limit case's 64 MiB frozen build reaches **80.001 MiB sampled usage
  under the same 88 MiB limit for both arms**, about 91% of the budget, and all
  executions succeed. Its ordinary peak is 52.13 MiB. Limits are enforced by
  existing query accounting; sampled values are lower bounds, not exact peaks.
- **Concurrency consumes the available parallelism.** Four simultaneous keyed
  queries improve individual median latency 2.42–2.63×, but batch throughput is
  27.95/33.60 queries/s fused versus 16.68/16.83 ordinary. Four scalar LEFT
  queries deliver 14.85/15.70 versus 11.62/11.74 queries/s. Per-query latency
  spreads widen considerably; the report does not extrapolate isolated-query
  speedups to saturated load or claim a fairness guarantee.
- **Storage changes the limiting cost.** Mixed/all-Parquet INNER cases improve
  about 7.5–7.8×. Verified cold native improves 3.18–3.19× and cold Parquet
  5.31–5.33×. All **80 cold preparations** leave only two resident pages:
  at most 0.0029% native and 0.0073% Parquet. The repeated scalar RIGHT storage
  comparison is at one million rows; the interrupted ten-million-row pilot
  prevents a claim of completed large-Parquet-RIGHT scaling.

The remaining axes also pass ordered result checks. A nonempty singleton build
scans all ten million rows with zero matches; an empty inner build skips probing,
whereas empty-build LEFT scans and aggregates ten million null extensions. Match
rates around 0.01%, 1%, 50% and 100% are measured independently of the primary.
A ten-million-row build source produces the same 10,000-row filtered lookup as
the 100,000-row source; its median filter/build phase is about 1.9 ms. Fanout 10,
90% hot-key skew and a 1,000-payload hot chain improve about 5.7–5.8× while counting
every duplicate pair. LEFT post-filters keep the original 999,862 matches and
9,000,138 null extensions: selective filtering retains 101,937 rows,
null-accepting filtering retains 9,102,075, and reject-all retains zero.

**Decision:** complete experimental V1 and retain the current false default.
The primary acceptance target is met; explicit orientation, memory and small-scan
regressions make broad capability-only default selection premature on this
workstation evidence. Keep the diagnosis/disable switch:

```properties
cairo.sql.parallel.hash.join.groupby.enabled=false
```

`cairo.sql.parallel.groupby.enabled=false` also disables fused selection through
the global parallel-aggregation gate. Opt-in still requires both flags and
positive configured worker slots. No new size/group/selectivity thresholds,
runtime fallback or input replay are introduced. Any accepted default-enablement
proposal must be a separate configuration/planner change with applicable checks
and renewed performance evidence. Parallel radix build is RFC task 11, **after
V1**; its crossover and these remaining costs require new measurements.

## Validation and retained scope

The benchmark package build passed. Both shell scripts pass `bash -n`.
**16 pre-generation CLI rejection checks** passed, as did twelve concurrent
keyed/scalar smoke workloads covering all six join orientations (480 measured
executions; 516 explicit result comparisons including warmups). The artifact
validator accepts a complete primary result and rejects a failed gate, a missing
result check and a missing measured sample.

The final **44-case matrix contains 2,040 measured query executions**, with
**2,608 explicit ordered result comparisons** including warmups, across 2,652
total executions. Every measured and non-reference warmup result passed. All
candidate counters are stable within each case. The validator confirms both
primary repetitions, every sample/owner/run, completion counts and all cold-cache
checks. Raw logs retain all samples and log lines, with only line endings and
trailing whitespace normalized. The interrupted pilot is retained separately and
is excluded from completed-execution counts.

No production code or engine tests changed. Task 9's **1,790 passing tests across
41 suites**, three conditional skips and zero failures/errors remain historical
engine qualification, not a newly rerun test claim. See the
[qualification report](parallel-hash-join-group-by-qualification.md) for exact
storage, concurrency, cancellation, limit and reuse coverage. This task adds
benchmark tooling and evidence; it does not establish production latency SLOs,
NUMA behavior, cold process startup, exact allocation high-water marks, or the
unfinished ten-million-row Parquet RIGHT performance result.

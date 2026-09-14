# V1 benchmark rerun after tasks 9a–9e

[RFC 130](https://github.com/questdb/rfc/discussions/130), task 10, measured
2026-09-13. This report qualifies engine revision
`a7500b88c7606e47ff6cd5bd592cc66b087f0c38` after the memory tracking,
off-heap storage, allocation, cancellation and semantic work. The `+working-tree`
revision suffix covers this task's benchmark changes; production code is unchanged.

This is historical evidence: RFC task 9f was added after this report and requires
performance recovery followed by another task 10 rerun. These measurements do not
complete the updated V1 requirements.

The fixed primary gate passes at **3.312× median end-to-end speedup in both
rounds**. **Retain the false default.** The wider
matrix and its explicit regressions support opt-in use; broad capability-only
enablement remains premature.

## Workloads and measurement boundary

The fixed primary is unchanged: seed 130, 100,000,000 fact rows, 100,000 dimension
keys, 10% country selection, fanout one, five years of monthly native partitions,
the original ordered solar SQL and four configured query workers. The one/two-worker
cases repeat that dataset and SQL. The primary ordered reference exactly matches
the original task 7/task 10 reference. Every arm compiles and executes on the same
engine, data and configuration, with only the experimental selection flag differing.

The 44 earlier task 10 cases are repeated, including keyed/scalar INNER/LEFT and
normalized RIGHT, both physical orientations, filtered build footprints and source
scan costs, match rates, post-join filters, fanout/skew, output cardinality,
worker scaling/concurrency, native/mixed/Parquet, small effective inputs, cold
filesystem data and the 88 MiB query limit. Seven added cases target 9b/9d costs:

| Added case | Purpose |
| --- | --- |
| `symbol-build-like` | Ten-million-row build source, with LIKE predicates selecting exactly the same ES/IT rows as the IN control. |
| `symbol-build-like-parquet` | The same filtered build source in Parquet; compares normal predicate caching with fused uncached text evaluation. |
| `symbol-post-left` | Null-accepting post-join LIKE over the copied SYMBOL, after matching/null extension. |
| `symbol-post-left-mixed` | The same LEFT predicate across mixed native/Parquet inputs. |
| `symbol-post-right-parquet` | The equivalent normalized RIGHT with both inputs in Parquet, at one million fact rows. |
| `parquet-cache-small` | Keyed INNER with a 64 KiB configured decoder cache instead of 256 MiB. |
| `parquet-cache-small-scalar-left` | Scalar LEFT with the same constrained cache and ten million probe rows. |

The added SYMBOL cases use three country values, deliberately exposing repeated
uncached evaluation with many rows per dictionary value. They do not measure a
large distinct-SYMBOL performance curve; task 9b/9e retain the cardinality/resource
and semantic evidence. The cache budget controls retained unpinned buffers,
not total decoder memory: access hints scale it and pinned buffers can exceed it.
The small-cache cases force eviction/reuse while streaming frames. Repeated
execution still builds and decodes afresh; decoded buffers do not survive a new
execution binding.

Each case has two rounds, three warmups and ten measured executions per arm per
round, alternating first arm. Timing starts immediately before cursor acquisition
and includes timer reset, filtering, a fresh build, scanning/decoding, probing,
aggregation, merge, final projection/sort and complete ordered result consumption.
Compilation, data generation/conversion, cache preparation, result verification
and cursor close are outside individual latency. Concurrent batch latency also
includes owner scheduling and cursor cleanup. Owners share four worker threads
and can steal work; configured workers do not impose a strict CPU-count ceiling.

Plans assert ordinary/fused selection and the intended logical type, normalization,
scalar/keyed mode and physical inputs. Metadata names/types and every ordered
result cell are checked. Integral values and keys compare exactly; DOUBLE values
use the existing `1e-10 * max(1, abs(expected))` tolerance and null/NaN contracts.
Large outputs retain hashes/previews, but execution compares every cell.

### Active circuit breakers

The earlier runner installed `NOOP_CIRCUIT_BREAKER`. This rerun installs
`NetworkSqlExecutionCircuitBreaker` for each owner and supplies the same configuration
to worker wrappers: `circuit.breaker.throttle=2000000`, the current server default,
with unlimited timeout and no client socket (`fd=-1`). Timers reset inside each
measured execution. Query registration binds cancellation flags normally and each
worker uses its own breaker/throttle state. The embedded configuration's test-oriented
throttle of five is explicitly replaced for both arms before measurements.

This includes successful owner/worker loop checks, time reads, cancellation binding
and timer work. It does not include client disconnect socket syscalls, network
protocol handling or deliberate exception-reporting paths. Task 9d's deterministic
failure/reuse tests remain the cancellation correctness evidence. Historical
absolute timing differences combine implementation, active-breaker and host effects;
they are not an isolated measurement of breaker overhead.

The interrupted initial no-op run and its original environment/command are kept
under [preliminary](parallel-hash-join-group-by-v1-rerun/preliminary/).
It was stopped after the no-op context was discovered, before the matrix completed;
none of its samples enter the final summary or acceptance decision.

### Memory and cold data

Per-query native trackers are sampled every 1 ms from before acquisition through
pre-close consumption. Sampling and tracker rebinding synchronize. Reported peaks
are lower bounds: brief allocation peaks can be missed. Allocation-time limits
still enforce the configured budget independently. Frozen build capacity is exact;
process-native deltas include shared worker pools and concurrent overlap and must
not be summed across owners. Java harness/result objects and mapped files are
outside query-native counts. The end-to-end consumer materializes Java results;
this is not a new zero-GC measurement or a replacement for task 9c's counters/stacks.

Cold cases warm code, release idle readers/writers, fsync only their fresh dataset,
apply `POSIX_FADV_DONTNEED`, and verify `mincore` residency before every measured arm.
The helper fails above 1% residency. Cold means filesystem data with warm code,
not process startup. All 80 preparations passed: at most 0.0029% of native pages
and 0.0072% of Parquet pages remained resident.

## Reproduction and environment

The [environment](parallel-hash-join-group-by-v1-rerun/environment.txt) retains the
source revision, jar and runner/helper hashes, JVM flags, host and configuration.
The [commands](parallel-hash-join-group-by-v1-rerun/commands.txt) retain every exact
invocation; each raw case retains generator SQL, parameters, actual plans, reference
results, every sample, phase timings and counters. Generated data stays at each
printed temporary path. Browsable `.txt` logs normalize line endings and trailing
whitespace; the [raw output archive](parallel-hash-join-group-by-v1-rerun/raw-output.tar.gz)
preserves byte-exact final and preliminary logs. Results were collected sequentially without overlapping
builds, test suites or other benchmark jobs. Normal desktop services remained active.

AMD Ryzen 9 7900 (12 cores/24 threads, one NUMA node, 64 MiB L3 across two caches),
61 GiB RAM, local NVMe, Linux 7.0.0-30-generic, OpenJDK 25.0.4+7, 8 GiB maximum heap,
powersave governor and boost enabled. No CPU pinning or NUMA placement was imposed.
This single-host evidence does not establish multi-node NUMA or production-load behavior.

```bash
mvn -pl benchmarks -am package -P build-rust-library,qdbr-release -DskipTests -Dmaven.test.skip=true
bash benchmarks/parallel-hash-join-group-by-v1.sh /tmp/new-v1-rerun-results
python3 benchmarks/summarize-hash-join-group-by-v1.py /tmp/new-v1-rerun-results --require-complete
```

`CASE_PATTERN` optionally selects cases into a new directory. Omit `--require-complete`
when summarizing an intentional subset. Complete validation requires all named cases
from the reproduction script, two full rounds, ten runs per owner/arm, all result
checks, stable candidate counters, verified cold preparations and the primary gate.
It rejects missing, duplicated or incomplete samples. The primary gate rejects altered
workload parameters and requires at least 2× unrounded median speedup in both rounds.

## End-to-end results

Every cell lists round 1 / round 2. Speedup is ordinary median divided by fused
median; values below one regress. Raw samples and per-round min/max spreads are in
the [summary CSV](parallel-hash-join-group-by-v1-rerun/summary.csv) and linked cases.

| Case | Ordinary median ms | Fused median ms | Speedup |
| --- | --- | --- | --- |
| [primary-w4](parallel-hash-join-group-by-v1-rerun/primary-w4.txt) | 2,364.073 / 2,364.107 | 713.694 / 713.851 | 3.312 / 3.312× |
| [primary-w1](parallel-hash-join-group-by-v1-rerun/primary-w1.txt) | 2,256.547 / 2,261.973 | 1,687.366 / 1,664.359 | 1.337 / 1.359× |
| [primary-w2](parallel-hash-join-group-by-v1-rerun/primary-w2.txt) | 2,345.548 / 2,337.858 | 1,159.054 / 1,155.879 | 2.024 / 2.023× |
| [inner](parallel-hash-join-group-by-v1-rerun/inner.txt) | 244.247 / 242.466 | 75.802 / 74.514 | 3.222 / 3.254× |
| [left](parallel-hash-join-group-by-v1-rerun/left.txt) | 598.407 / 595.801 | 127.834 / 126.945 | 4.681 / 4.693× |
| [right](parallel-hash-join-group-by-v1-rerun/right.txt) | 2,980.030 / 2,988.260 | 140.237 / 141.385 | 21.250 / 21.136× |
| [scalar-inner](parallel-hash-join-group-by-v1-rerun/scalar-inner.txt) | 209.323 / 208.448 | 68.058 / 67.014 | 3.076 / 3.110× |
| [scalar-left](parallel-hash-join-group-by-v1-rerun/scalar-left.txt) | 369.887 / 370.728 | 89.462 / 88.987 | 4.135 / 4.166× |
| [scalar-right](parallel-hash-join-group-by-v1-rerun/scalar-right.txt) | 2,693.092 / 2,695.209 | 88.383 / 87.567 | 30.471 / 30.779× |
| [inner-swapped](parallel-hash-join-group-by-v1-rerun/inner-swapped.txt) | 38.509 / 38.339 | 33.497 / 33.112 | 1.150 / 1.158× |
| [left-swapped](parallel-hash-join-group-by-v1-rerun/left-swapped.txt) | 37.881 / 37.931 | 40.998 / 38.260 | 0.924 / 0.991× |
| [right-swapped](parallel-hash-join-group-by-v1-rerun/right-swapped.txt) | 23.266 / 22.539 | 34.859 / 33.271 | 0.667 / 0.677× |
| [zero-matches](parallel-hash-join-group-by-v1-rerun/zero-matches.txt) | 59.913 / 59.955 | 40.105 / 39.985 | 1.494 / 1.499× |
| [empty-build](parallel-hash-join-group-by-v1-rerun/empty-build.txt) | 54.469 / 52.499 | 0.466 / 0.323 | 117.009 / 162.662× |
| [empty-build-left](parallel-hash-join-group-by-v1-rerun/empty-build-left.txt) | 454.894 / 452.980 | 116.109 / 115.968 | 3.918 / 3.906× |
| [singleton-build](parallel-hash-join-group-by-v1-rerun/singleton-build.txt) | 54.754 / 54.413 | 40.703 / 40.518 | 1.345 / 1.343× |
| [match-001pct](parallel-hash-join-group-by-v1-rerun/match-001pct.txt) | 66.936 / 65.636 | 46.777 / 45.874 | 1.431 / 1.431× |
| [match-1pct](parallel-hash-join-group-by-v1-rerun/match-1pct.txt) | 164.049 / 164.687 | 66.561 / 66.531 | 2.465 / 2.475× |
| [match-50pct](parallel-hash-join-group-by-v1-rerun/match-50pct.txt) | 501.690 / 501.591 | 130.895 / 131.241 | 3.833 / 3.822× |
| [match-100pct](parallel-hash-join-group-by-v1-rerun/match-100pct.txt) | 779.673 / 778.104 | 197.327 / 196.490 | 3.951 / 3.960× |
| [build-beyond-cache](parallel-hash-join-group-by-v1-rerun/build-beyond-cache.txt) | 3,733.920 / 3,729.825 | 768.921 / 765.366 | 4.856 / 4.873× |
| [build-source-10m](parallel-hash-join-group-by-v1-rerun/build-source-10m.txt) | 234.402 / 234.604 | 77.172 / 76.261 | 3.037 / 3.076× |
| [post-selective](parallel-hash-join-group-by-v1-rerun/post-selective.txt) | 282.164 / 277.991 | 77.061 / 75.988 | 3.662 / 3.658× |
| [post-null-accepting](parallel-hash-join-group-by-v1-rerun/post-null-accepting.txt) | 648.303 / 641.867 | 135.933 / 134.936 | 4.769 / 4.757× |
| [post-reject-all](parallel-hash-join-group-by-v1-rerun/post-reject-all.txt) | 249.895 / 249.883 | 71.952 / 71.845 | 3.473 / 3.478× |
| [fanout-10](parallel-hash-join-group-by-v1-rerun/fanout-10.txt) | 719.765 / 716.928 | 183.347 / 181.660 | 3.926 / 3.947× |
| [hot-key-90pct](parallel-hash-join-group-by-v1-rerun/hot-key-90pct.txt) | 483.326 / 494.125 | 141.460 / 139.722 | 3.417 / 3.536× |
| [hot-chain](parallel-hash-join-group-by-v1-rerun/hot-chain.txt) | 3,648.654 / 3,648.196 | 1,015.934 / 1,015.346 | 3.591 / 3.593× |
| [high-cardinality](parallel-hash-join-group-by-v1-rerun/high-cardinality.txt) | 447.852 / 432.148 | 278.488 / 267.778 | 1.608 / 1.614× |
| [compressed-groups](parallel-hash-join-group-by-v1-rerun/compressed-groups.txt) | 658.526 / 657.026 | 193.004 / 191.975 | 3.412 / 3.422× |
| [near-input-groups](parallel-hash-join-group-by-v1-rerun/near-input-groups.txt) | 39.736 / 39.153 | 33.251 / 31.115 | 1.195 / 1.258× |
| [small-input](parallel-hash-join-group-by-v1-rerun/small-input.txt) | 2.787 / 2.274 | 1.862 / 1.311 | 1.497 / 1.734× |
| [small-interval](parallel-hash-join-group-by-v1-rerun/small-interval.txt) | 1.027 / 0.618 | 1.244 / 0.927 | 0.826 / 0.666× |
| [build-heavy](parallel-hash-join-group-by-v1-rerun/build-heavy.txt) | 7.484 / 6.703 | 7.285 / 7.556 | 1.027 / 0.887× |
| [concurrent-2](parallel-hash-join-group-by-v1-rerun/concurrent-2.txt) | 237.012 / 235.593 | 129.293 / 128.441 | 1.833 / 1.834× |
| [concurrent-4](parallel-hash-join-group-by-v1-rerun/concurrent-4.txt) | 238.806 / 238.142 | 215.183 / 199.501 | 1.110 / 1.194× |
| [concurrent-scalar-left](parallel-hash-join-group-by-v1-rerun/concurrent-scalar-left.txt) | 342.299 / 340.111 | 336.913 / 392.927 | 1.016 / 0.866× |
| [mixed](parallel-hash-join-group-by-v1-rerun/mixed.txt) | 316.770 / 312.916 | 78.014 / 77.210 | 4.060 / 4.053× |
| [parquet](parallel-hash-join-group-by-v1-rerun/parquet.txt) | 319.571 / 319.811 | 82.353 / 82.007 | 3.880 / 3.900× |
| [scalar-right-1m](parallel-hash-join-group-by-v1-rerun/scalar-right-1m.txt) | 141.578 / 153.276 | 11.049 / 10.062 | 12.813 / 15.233× |
| [parquet-scalar-right](parallel-hash-join-group-by-v1-rerun/parquet-scalar-right.txt) | 214.281 / 210.565 | 12.485 / 11.404 | 17.164 / 18.464× |
| [cold-native](parallel-hash-join-group-by-v1-rerun/cold-native.txt) | 322.386 / 320.415 | 140.775 / 139.889 | 2.290 / 2.291× |
| [cold-parquet](parallel-hash-join-group-by-v1-rerun/cold-parquet.txt) | 395.642 / 395.031 | 118.061 / 115.496 | 3.351 / 3.420× |
| [near-memory-limit](parallel-hash-join-group-by-v1-rerun/near-memory-limit.txt) | 2,629.922 / 2,618.575 | 484.026 / 487.102 | 5.433 / 5.376× |
| [symbol-build-like](parallel-hash-join-group-by-v1-rerun/symbol-build-like.txt) | 255.129 / 254.804 | 100.262 / 99.880 | 2.545 / 2.551× |
| [symbol-build-like-parquet](parallel-hash-join-group-by-v1-rerun/symbol-build-like-parquet.txt) | 243.964 / 242.596 | 100.959 / 101.085 | 2.416 / 2.400× |
| [symbol-post-left](parallel-hash-join-group-by-v1-rerun/symbol-post-left.txt) | 638.510 / 636.844 | 153.955 / 152.599 | 4.147 / 4.173× |
| [symbol-post-left-mixed](parallel-hash-join-group-by-v1-rerun/symbol-post-left-mixed.txt) | 722.742 / 720.640 | 143.767 / 141.861 | 5.027 / 5.080× |
| [symbol-post-right-parquet](parallel-hash-join-group-by-v1-rerun/symbol-post-right-parquet.txt) | 277.200 / 279.159 | 18.483 / 17.796 | 14.997 / 15.686× |
| [parquet-cache-small](parallel-hash-join-group-by-v1-rerun/parquet-cache-small.txt) | 4,895.385 / 4,886.639 | 85.150 / 83.983 | 57.491 / 58.186× |
| [parquet-cache-small-scalar-left](parallel-hash-join-group-by-v1-rerun/parquet-cache-small-scalar-left.txt) | 5,002.007 / 4,972.499 | 99.574 / 96.489 | 50.234 / 51.534× |

## Phase and query-memory measurements

Phase medians combine both candidate rounds, in milliseconds. Query-memory cells
show median / maximum sampled peaks, in MiB. Build capacity is exact frozen native
storage. The CSV retains each round and all process-native/retained-query columns.

| Case | Build / probe / merge ms | Ordinary query MiB | Fused query MiB | Frozen build MiB |
| --- | --- | --- | --- | --- |
| primary-w4 | 0.924 / 712.415 / 0.199 | 0.478 / 0.934 | 0.973 / 1.198 | 0.750 |
| primary-w1 | 0.918 / 1,666.662 / 0.082 | 0.478 / 1.218 | 0.919 / 1.771 | 0.750 |
| primary-w2 | 0.872 / 1,157.094 / 0.096 | 0.478 / 1.125 | 0.919 / 1.389 | 0.750 |
| inner | 0.915 / 73.393 / 0.190 | 0.472 / 0.931 | 0.973 / 1.376 | 0.750 |
| left | 0.928 / 125.844 / 0.213 | 0.419 / 0.774 | 1.014 / 1.428 | 0.750 |
| right | 0.962 / 138.422 / 0.216 | 131.574 / 131.574 | 1.014 / 1.389 | 0.750 |
| scalar-inner | 0.669 / 66.529 / 0.015 | 0.337 / 0.686 | 0.800 / 1.112 | 0.750 |
| scalar-left | 0.717 / 88.159 / 0.018 | 0.274 / 0.618 | 0.800 / 1.263 | 0.750 |
| scalar-right | 0.779 / 86.882 / 0.019 | 131.439 / 131.439 | 0.800 / 1.250 | 0.750 |
| inner-swapped | 27.832 / 4.747 / 0.206 | 16.395 / 16.395 | 48.508 / 48.508 | 32.500 |
| left-swapped | 34.202 / 4.098 / 0.196 | 16.333 / 16.333 | 48.508 / 48.508 | 32.500 |
| right-swapped | 28.579 / 4.706 / 0.217 | 0.308 / 0.314 | 48.508 / 48.508 | 32.500 |
| zero-matches | 0.313 / 39.595 / 0.018 | 0.281 / 0.281 | 0.200 / 0.200 | 0.001 |
| empty-build | 0.268 / 0.000 / 0.018 | 0.281 / 0.281 | 0.137 / 0.137 | 0.001 |
| empty-build-left | 0.327 / 115.274 / 0.127 | 0.280 / 0.280 | 0.199 / 0.199 | 0.001 |
| singleton-build | 0.287 / 39.958 / 0.082 | 0.281 / 0.281 | 0.200 / 0.200 | 0.001 |
| match-001pct | 0.316 / 45.489 / 0.152 | 0.286 / 0.286 | 0.220 / 0.221 | 0.001 |
| match-1pct | 0.556 / 65.314 / 0.182 | 0.308 / 0.493 | 0.286 / 0.286 | 0.063 |
| match-50pct | 2.823 / 127.415 / 0.192 | 2.659 / 3.464 | 4.223 / 4.234 | 4.000 |
| match-100pct | 5.839 / 190.393 / 0.207 | 6.714 / 6.714 | 8.286 / 10.118 | 8.000 |
| build-beyond-cache | 272.579 / 494.465 / 0.206 | 105.529 / 106.222 | 161.625 / 162.096 | 128.000 |
| build-source-10m | 1.717 / 74.284 / 0.184 | 3.329 / 10.695 | 3.511 / 11.247 | 0.750 |
| post-selective | 0.948 / 74.723 / 0.178 | 0.409 / 0.761 | 0.973 / 1.376 | 0.750 |
| post-null-accepting | 0.813 / 133.781 / 0.223 | 0.419 / 0.748 | 1.014 / 1.376 | 0.750 |
| post-reject-all | 0.895 / 70.582 / 0.022 | 0.404 / 0.769 | 0.949 / 1.376 | 0.750 |
| fanout-10 | 4.034 / 177.659 / 0.192 | 2.384 / 3.338 | 4.874 / 7.032 | 4.500 |
| hot-key-90pct | 0.878 / 139.126 / 0.191 | 0.472 / 0.824 | 0.973 / 1.402 | 0.750 |
| hot-chain | 0.473 / 1,014.502 / 0.082 | 0.281 / 0.281 | 0.194 / 0.194 | 0.032 |
| high-cardinality | 0.882 / 95.149 / 8.717 | 56.337 / 56.337 | 90.951 / 91.088 | 0.750 |
| compressed-groups | 0.334 / 190.062 / 0.488 | 1.027 / 1.027 | 6.430 / 6.449 | 0.006 |
| near-input-groups | 4.400 / 3.873 / 1.028 | 19.024 / 19.024 | 27.710 / 28.491 | 6.000 |
| small-input | 0.264 / 0.795 / 0.073 | 0.287 / 0.287 | 0.175 / 0.175 | 0.008 |
| small-interval | 0.862 / 0.127 / 0.023 | 0.461 / 0.824 | 0.886 / 1.113 | 0.750 |
| build-heavy | 5.318 / 1.550 / 0.072 | 7.009 / 7.104 | 8.599 / 10.405 | 8.000 |
| concurrent-2 | 0.944 / 127.731 / 0.129 | 0.472 / 0.931 | 0.973 / 1.402 | 0.750 |
| concurrent-4 | 1.066 / 204.994 / 0.070 | 0.472 / 0.811 | 0.973 / 1.376 | 0.750 |
| concurrent-scalar-left | 0.812 / 355.530 / 0.012 | 0.274 / 0.618 | 0.800 / 1.112 | 0.750 |
| mixed | 0.955 / 75.812 / 0.157 | 11.769 / 11.769 | 16.058 / 17.099 | 0.750 |
| parquet | 1.001 / 80.438 / 0.122 | 11.767 / 11.767 | 16.019 / 17.059 | 0.750 |
| scalar-right-1m | 0.741 / 9.306 / 0.012 | 19.439 / 19.439 | 0.800 / 1.112 | 0.750 |
| parquet-scalar-right | 0.880 / 10.608 / 0.011 | 45.707 / 45.718 | 3.644 / 3.662 | 0.750 |
| cold-native | 4.020 / 134.963 / 0.209 | 0.603 / 0.837 | 1.107 / 1.389 | 0.750 |
| cold-parquet | 6.476 / 109.832 / 0.131 | 11.767 / 11.767 | 16.678 / 17.059 | 0.750 |
| near-memory-limit | 105.033 / 380.174 / 0.209 | 52.859 / 53.113 | 80.922 / 80.955 | 64.000 |
| symbol-build-like | 27.473 / 71.752 / 0.187 | 0.508 / 0.630 | 0.973 / 1.180 | 0.750 |
| symbol-build-like-parquet | 27.802 / 72.399 / 0.194 | 8.338 / 10.179 | 9.077 / 9.728 | 0.750 |
| symbol-post-left | 0.925 / 151.520 / 0.187 | 0.409 / 0.769 | 0.973 / 1.376 | 0.750 |
| symbol-post-left-mixed | 1.018 / 140.977 / 0.143 | 11.706 / 11.707 | 17.099 / 17.100 | 0.750 |
| symbol-post-right-parquet | 1.009 / 16.593 / 0.081 | 45.837 / 45.845 | 3.797 / 3.816 | 0.750 |
| parquet-cache-small | 1.055 / 83.168 / 0.124 | 4.006 / 4.006 | 16.019 / 17.060 | 0.750 |
| parquet-cache-small-scalar-left | 0.939 / 95.975 / 0.019 | 3.809 / 3.809 | 16.906 / 16.906 | 0.750 |

## Findings and rollout decision

- **Primary acceptance and scaling.** The fixed four-worker gate passes at
  3.312× in both rounds: approximately 2,364 ms ordinary versus 714 ms fused.
  Every execution copies 10,000 build rows/keys, scans 100,000,000 fact rows,
  aggregates 9,998,251 matched pairs and returns 120 groups. Fused medians scale
  from 1,664–1,687 ms with one worker to 1,156–1,159 ms with two and 714 ms with
  four. The frozen build is 0.750 MiB; sampled query peaks reach 1.198 MiB fused
  versus 0.934 MiB ordinary. These are successful executions with active breakers.
- **Orientation still controls time and memory.** Normalized RIGHT improves
  21.1–21.3× when it changes a large ordinary fact build into a small copied
  dimension build. Reversing the orientation makes fused RIGHT **48–50% slower**:
  33.3–34.9 ms versus 22.5–23.3 ms ordinary. Its 32.5 MiB frozen build reaches
  48.508 MiB sampled query usage, versus 0.314 MiB ordinary. Swapped LEFT is
  slightly slower in both rounds, with one round close to parity.
- **Tiny effective scans and expensive builds need caution.** The one-hour scan
  sees just 229 fact rows, 23 pairs and two groups, but still builds 10,000 rows.
  It is 21–50% slower fused. The build-heavy case is near parity in its first
  round and 13% slower in its second. Sub-millisecond variation in the tiny case
  remains visible in the raw min/max spreads; these samples do not justify a
  new numeric planner cutoff.
- **Output costs reduce the gain.** The 486,759-group case improves 1.61×, with
  sampled peaks of 91.088 MiB fused versus 56.337 MiB ordinary. Its median probe
  and merge phases take 95.149 and 8.717 ms; ordered output and result consumption
  contribute substantially to total latency. With 99,157 groups from 100,000
  matching rows, gains narrow to 1.20–1.26×. Both owner and sharded merging remain
  in the measured matrix; no preaggregation rewrite changes these workloads.
- **Concurrency exposes throughput regressions.** Four simultaneous keyed queries
  have per-query median speedups of 1.11–1.19×, while batch throughput falls to
  **14.954 / 13.828 queries/s** from **16.580 / 16.716** ordinary. Four scalar LEFT
  queries reach **9.008 / 7.394 queries/s** versus **11.558 / 11.672** ordinary;
  their second-round median query latency is 15.5% slower. Candidate scalar
  latencies span about 103–669 ms. Broad enablement cannot be inferred from the
  isolated-query acceptance gate, and this host does not establish a fairness guarantee.
- **Memory includes live execution and growth.** The two-million-key build freezes
  at 128 MiB but reaches 162.096 MiB sampled query usage, versus 106.222 MiB
  ordinary, while improving 4.86–4.87×. The near-limit workload freezes at 64 MiB
  and peaks at a sampled **80.955 MiB under the 88 MiB limit** (about 92% of the
  budget), versus 53.113 MiB ordinary. All near-limit executions succeed. These
  samples supplement the allocation-time limit/failure tests; they are not exact
  high-water marks or new cleanup proofs.
- **Uncached SYMBOL evaluation has a measurable source cost.** The ten-million-row
  native LIKE source produces the same filtered lookup and final result as the IN
  control, but its median filter/build phase is **27.473 ms versus 1.717 ms**.
  Its end-to-end gain remains 2.55×. The LIKE Parquet source takes 27.802 ms in
  filter/build and improves 2.40–2.42× overall. Native null-accepting post-join
  LIKE improves 4.15–4.17× while preserving null extensions; mixed and normalized
  Parquet variants also pass. These comparisons include the compiled filter paths
  and scanning/decoding work, rather than isolating one predicate instruction.
- **Storage and cache behavior require separate measurements.** Default-budget
  mixed and all-Parquet INNER improve 4.05–4.06× and 3.88–3.90×. Verified cold
  native improves about 2.3× and cold Parquet 3.35–3.42×. At 64 KiB, keyed INNER
  takes 83.983–85.150 ms fused versus 4,886.639–4,895.385 ms ordinary; scalar LEFT
  takes 96.489–99.574 ms versus 4,972.499–5,002.007 ms. The large ordinary slowdown
  is consistent with repeated Parquet payload lookups under eviction, while the
  fused plan probes its copied build. These are deliberately constrained-cache
  diagnostics with the same budget for both arms; they do not replace default-budget
  measurements or show that the configured budget caps all live decoder memory.

**Rollout decision: complete experimental V1 and retain default false.** Tasks
9a–9e are complete and this rerun passes the primary gate on their implementation.
Swapped-RIGHT time/memory costs, tiny-scan regressions and concurrent throughput
regressions make broad capability-only default selection premature. No production
configuration or planner behavior changes in this task.

Keep the documented diagnosis/disable switch:

```properties
cairo.sql.parallel.hash.join.groupby.enabled=false
```

The global `cairo.sql.parallel.groupby.enabled=false` also disables fused selection;
opt-in requires both flags and positive configured query workers. Default enablement
remains a separate reviewable configuration/planner change. There is no build-size,
selectivity or output threshold, runtime fallback or consumed-input replay.

The earlier ten-million-row Parquet scalar RIGHT pilot remains incomplete; this
rerun repeats the predeclared one-million-row native/Parquet RIGHT pair. It makes
no claim about that large-pilot scaling boundary. The matrix covers the RFC axes
with targeted workloads rather than their full Cartesian product. Parallel radix
build is post-V1 task 11 and needs its own design/measurements; these results do
not infer its crossover or promise it resolves every regression.

## Validation

The [package build](parallel-hash-join-group-by-v1-rerun/build.log.gz) passed with
`build-rust-library,qdbr-release`. Eight targeted [smoke workloads](parallel-hash-join-group-by-v1-rerun/smoke/commands.log.gz)
passed **388 ordered result checks**, including two concurrent owners. Shell syntax,
five pre-generation CLI rejection checks and the artifact validator's positive/
negative checks passed. The [validation log](parallel-hash-join-group-by-v1-rerun/validation.log.gz)
records complete-matrix validation: **51 cases, 2,320 measured executions and
2,965 result comparisons**, with zero mismatches. All 80 cold preparations passed.

All **44 historical workload references** are unchanged. Added LIKE build and
constrained-cache cases also match their respective control references. The
measured jar, runner, reproduction-script and helper hashes match the final
sources. The validator rejects a missing matrix case, missing result checks,
duplicate samples and a failed primary gate.

No production engine code changed. Task 9e already passed 2,392 Java tests across
71 affected suites (26 conditional skips, zero failures/errors) at the measured
engine revision, including the prior memory, storage, cancellation and concurrency
regressions. That evidence is recorded in the [semantic coverage guide](parallel-hash-join-group-by-semantics.md)
and is not counted as a fresh test run in this benchmark task. Earlier allocation/
retained-heap reports keep their original setup and JVM boundaries.

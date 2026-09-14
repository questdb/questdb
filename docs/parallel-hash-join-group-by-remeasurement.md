# Fused hash join remeasurement (RFC task 9g)

Task **9g's latency and allocation measurements are complete** for implementation
`4a05a7eb242e1225e57b3f1bd7d3798059a2820c`. All **54 recovery bounds**, both primary
**2× speedup gates**, and all **24 C1 allocation cases** pass. Overall V1
qualification remains **blocked by an unresolved JVM crash during swapped-RIGHT
warmup**. A successful retry supplies timing samples, but does not resolve the
execution failure. Task 10 must wait for that reliability prerequisite.

This change publishes evidence; production code, workload settings, acceptance
bounds and the experimental default are unchanged. The design remains
[RFC 130](https://github.com/questdb/rfc/discussions/130), with the
[frame/shard circuit-breaker boundaries](parallel-hash-join-group-by-breaker-boundaries.md)
introduced at the measured revision.

## Fixed implementation and protocol

The user confirmed: “Yes, the machine is available; run both matrices”. The
[authorization](parallel-hash-join-group-by-remeasurement/authorization.txt) and
[timeline](parallel-hash-join-group-by-remeasurement/timeline.txt) retain that
coordination. Maven finished before latency collection. Latency and allocation
ran sequentially, with no tests, builds or allocation profiling overlapping
latency. Focused regressions ran after allocation completed.

The candidate jar was freshly packaged with JDK 25.0.4 and the release Rust
profile, then copied to an immutable measurement path. Its SHA-256 is
`7ff3efc4ea3c2baa56fbc598f06d43fb68d179468ff18260dcfca441c2a3b760`.
Both matrices use that jar. The
[build log](parallel-hash-join-group-by-remeasurement/build.txt.gz),
[source revision and tree IDs](parallel-hash-join-group-by-remeasurement/source.txt),
[source patch relative to `1586b7650f`](parallel-hash-join-group-by-remeasurement/source.patch.gz),
[jar/driver hashes](parallel-hash-join-group-by-remeasurement/artifacts.sha256), and
[input hashes](parallel-hash-join-group-by-remeasurement/inputs.sha256) identify
all inputs. The engine and benchmark source trees stayed unchanged.

The host remains AMD Ryzen 9 7900, 12 cores/24 threads, Linux 7.0.0-30-generic,
OpenJDK 25.0.4, and `-Xmx8g` for latency. The
[initial environment](parallel-hash-join-group-by-remeasurement/candidate/environment.txt)
and [continuation environment](parallel-hash-join-group-by-remeasurement/continuation-environment.txt)
record the exact settings. The original 27 recovery workloads, three warmups,
two rounds and ten measured executions per arm/owner are unchanged. Fresh build,
scan/probe/aggregation, merge, final projection/sort and complete ordered
consumption remain timed. Owners use the normal active network breaker with
throttle 2,000,000, unlimited timeout and no client socket; workers bind their
independent wrappers. This does not measure client-disconnect syscalls.

The reference remains the original task 10 engine `58b1dc04cc` with its documented
no-op breaker. This task reuses the **previously frozen reference matrix** from
[task 9f](parallel-hash-join-group-by-recovery/reference/), copied byte-for-byte;
it does not remeasure or replace it. Each candidate median is compared against
1.10 times the lower of the original published median and that frozen reproduced
median. The original primary dataset, SQL, seed 130, 100 million fact rows,
100,000 dimension keys, 10% selection and four workers are unchanged.

## Gate outcomes

| Requirement | Outcome |
| --- | --- |
| Individual recovery bounds | **54/54 PASS**; largest candidate/lower-reference ratio 1.0312, for `right` round 2, below 1.10. |
| Primary ordinary/fused speedup | **2/2 PASS**; 8.244× and 8.360×, above 2×. |
| Latency result/workload checks | **PASS**; 1,360 measured executions, 1,741 ordered result comparisons, unchanged logical counters and reference results. |
| C1 allocation cases | **24/24 PASS**; 234 executions, 405 owner/worker windows, zero unexplained bytes and all four workers participating in each census case. |
| Execution completion | **FAIL** on the initial swapped-RIGHT warmup: SIGSEGV; unchanged retry passed. Cause remains unresolved. |

The [complete gate list](parallel-hash-join-group-by-remeasurement/gates.csv),
[recovery comparison](parallel-hash-join-group-by-remeasurement/comparison.csv), and
[validator output](parallel-hash-join-group-by-remeasurement/comparison.txt)
retain individual outcomes. Successful timings do not cancel the crash.

| Primary round | Published reference, ms | Frozen reproduced reference, ms | Candidate, ms | Candidate / lower reference | Speedup |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 319.808 | 313.995 | 283.420 | 0.9026 | 8.244× |
| 2 | 321.527 | 312.123 | 279.123 | 0.8943 | 8.360× |

The three workloads that failed individual bounds at `d2e59fc832` now pass both
rounds:

| Case | Round | Candidate, ms | Lower reference, ms | Ratio | Outcome |
| --- | ---: | ---: | ---: | ---: | --- |
| compressed-groups | 1 | 120.398 | 134.662 | 0.8941 | PASS |
| compressed-groups | 2 | 119.157 | 129.460 | 0.9204 | PASS |
| concurrent-4 | 1 | 81.400 | 90.206 | 0.9024 | PASS |
| concurrent-4 | 2 | 83.509 | 90.538 | 0.9224 | PASS |
| high-cardinality | 1 | 213.437 | 234.223 | 0.9113 | PASS |
| high-cardinality | 2 | 208.127 | 212.276 | 0.9805 | PASS |

The [phase comparison](parallel-hash-join-group-by-remeasurement/focus-phase-medians.csv)
retains build, frame initialization, probe/aggregation, merge and remaining
end-to-end time for the primary and these three workloads, alongside the lower
reference and shared-throttle results. Remaining time is calculated per execution
before taking its median; it includes output, sorting and cleanup. These timers
do not by themselves identify instruction-level causes. Every workload's phase
measurements, min/max spread and sampled memory observations remain in the
[full summary](parallel-hash-join-group-by-remeasurement/candidate/summary.csv)
and adjacent case logs.

## Warmup crash and continuation

The initial matrix completed its first eleven cases, then the swapped-RIGHT JVM
aborted at **2026-09-14 10:49:31 UTC**, about 1.373 seconds after process start.
The ordinary ordered reference and both plans were printed, but no measured
sample was emitted for this case. The owner executor thread `pool-1-thread-1`
received SIGSEGV with instruction pointer `0x1`. The report contains a stack
return address in C1-compiled
`AsyncHashJoinGroupByRecordCursorFactory.update` and live fused-record objects;
it does not establish whether the cause is in the engine or the JVM. No core
dump was available.

The [crash report](parallel-hash-join-group-by-remeasurement/interrupted/hs_err_pid3158166.log.gz),
[failed case log](parallel-hash-join-group-by-remeasurement/interrupted/right-swapped.txt),
[initial matrix archive](parallel-hash-join-group-by-remeasurement/interrupted/initial-matrix.tar.gz), and
[interruption record](parallel-hash-join-group-by-remeasurement/interrupted/status.txt)
preserve the failed attempt. The same jar, JDK, flags, data-generation settings
and acceptance bounds were used to retry swapped-RIGHT and run the remaining
fifteen cases. The first eleven completed cases were retained without rerunning
or selecting samples. The retry completed both rounds and all 51 result checks.
The continuation contains no further crash. This is evidence of an intermittent
failure, not a fix or proof of reliability.

The [initial driver](parallel-hash-join-group-by-remeasurement/run.sh),
[continuation driver](parallel-hash-join-group-by-remeasurement/continue.sh), and
[exact case commands](parallel-hash-join-group-by-remeasurement/candidate/commands.txt)
retain both attempts. The
[raw archive](parallel-hash-join-group-by-remeasurement/raw-matrix.tar.gz)
includes initial and continuation output before whitespace normalization.

## Allocation and focused validation

The [allocation summary](parallel-hash-join-group-by-remeasurement/allocation/summary.csv)
and [validator output](parallel-hash-join-group-by-remeasurement/allocation.txt)
cover all 24 cases on the same frozen jar. The established
[C1 byte/site scope and exemptions](parallel-hash-join-group-by-allocation.md)
are unchanged: exact owner/worker byte counters are paired with allocation-site
census/stacks; compiler/setup, audited shared-framework bookkeeping, failure
allocation and application result formatting keep their documented boundaries.
The instrumentation uses synchronous C1, disabled escape analysis and disabled
TLABs. These results do not claim zero allocation under every JVM configuration.

The cases preserve native/mixed/Parquet storage, owner/sharded/scalar merges,
INNER/LEFT/normalized RIGHT, logical conversions, two owners, unseen SYMBOLs,
fresh native growth, output/close, cancellation cleanup and same-factory reuse.
Every participating thread and measurement window is retained in the compressed
case logs. The [allocation driver](parallel-hash-join-group-by-remeasurement/allocation.sh)
only redirects the existing runner to the frozen jar and repository directory.

After both matrices, **83 tests passed across four focused suites**,
with no failures or errors: fused execution, planner, concurrency and immutable
build storage. The [per-suite results](parallel-hash-join-group-by-remeasurement/regressions.csv),
[regression log](parallel-hash-join-group-by-remeasurement/regressions.txt.gz), and
[validation commands](parallel-hash-join-group-by-remeasurement/validation.txt)
are retained. The crash did not reproduce in those suites; their success does not
resolve it. The earlier 2,517-test/73-suite qualification at `4a05a7eb24` remains
historical evidence. No production fix was made in this measurement task.

## Next V1 work

The task 9f numerical recovery bounds and task 9g latency/allocation matrices now
pass on `4a05a7eb24`, but task 9's execution-reliability prerequisite is reopened
by the warmup crash. Investigate and resolve that failure, retain a regression
where feasible, and repeat affected qualification on the final implementation
before task 10. Further performance/allocation runs require explicit shared-host
confirmation. Keep the failed attempt even if a later implementation passes.

Task 10 remains the separate full **51-case rollout matrix**, including cold data,
small effective scans, large/build-dominated inputs, near-limit memory and
SYMBOL/cache controls. Default enablement remains a separate reviewable change;
`cairo.sql.parallel.hash.join.groupby.enabled=false` is retained. No build-size
threshold, runtime fallback or consumed-input replay is introduced. Post-V1
parallel radix build remains task 11.

The [original task 9f recovery](parallel-hash-join-group-by-recovery.md) at
`4ae9efb0f0`, the [shared-throttle measurement](parallel-hash-join-group-by-throttling.md)
at `d2e59fc832`, all failed trials, and prior interrupted runs remain unchanged.
The [artifact manifest](parallel-hash-join-group-by-remeasurement/artifacts-manifest.sha256)
covers the newly published evidence.

# Shared circuit-breaker throttling for parallel hash joins

**Historical implementation and benchmark at `d2e59fc832`.** The subsequent
[boundary-check simplification](https://github.com/questdb/questdb/blob/4a05a7eb242e1225e57b3f1bd7d3798059a2820c/core/src/main/java/io/questdb/cairo/sql/async/UnorderedPageFrameReduceJob.java)
moves probe scans to frame checks and removes collision and map-entry polling.
The measurements below apply to `d2e59fc832`. The subsequent
[task 9g remeasurement](parallel-hash-join-group-by-remeasurement.md) reports all
54 recovery bounds and 24 C1 allocation cases passing at `4a05a7eb24`, alongside
an unresolved swapped-RIGHT warmup crash that blocks rollout qualification.

This follow-up to task 9f removes the operator-specific throttle counters from
commit `4ae9efb0f0`. The fused join now calls the existing
`statefulThrowExceptionIfTripped()` API for scanned probe rows, copied build rows,
duplicate advances and collision steps. The bound breaker owns the interval and
counter. Owner and worker slots retain independently bound wrappers, and published
tasks still drain before native storage is released.

The slot's interval/remaining-row fields and the probe's duplicate/collision
counters are removed. `findUnchecked(int)` skips only the redundant lookup-entry
check: its caller checks the scan, and collision traversal checks the bound
breaker. Duplicate traversal uses the existing checked `next()` API, including
its execution-local handle result. Frozen lookup metadata, direct payload
positioning, matched-pair accounting and known-size native build reservation
remain. No query eligibility or configuration defaults change.

## Other parallel operators

The audit follows reducer calls into shared map helpers, and distinguishes checks
inside row/entry loops from checks before a frame, native call or scheduling wait.

| Path | Check placement and outcome |
| --- | --- |
| Parallel interpreted filters, count-only filtering and column pre-touch | Already use `statefulThrowExceptionIfTripped()` for each examined row. |
| Parallel JIT filters | Interpreted fallback loops already use the standard throttled API. The time-throttled call before native evaluation covers a frame or selected Parquet range. |
| Keyed/scalar horizon and multi-horizon joins | Existing Java hot loops use the standard throttled API. |
| Window joins, including the fast variants | Existing outer row and nested match loops use the standard throttled API. |
| Parallel keyed/scalar GROUP BY and Top-K reducers | Rely on the shared reducer's state check between frames. Their aggregation/Top-K row loops do not invoke a time-throttled network check for every row. Native/batched operations retain their existing interruption granularity. |
| GROUP BY map redistribution | Changed the per-entry time-throttled call to the standard throttled API. Per-shard allocation checks remain at shard boundaries. |
| Ordered, 4-byte, 8-byte and VARCHAR map merges | Changed the per-slot/entry time-throttled calls to the standard throttled API. This covers owner merges and shared parallel-merge helpers. |
| Shared post-aggregation cancellation channel | Its standard API reads the shared flag without modifying an inherited single-threaded counter. This channel has no clock or socket; the owner propagates timeout/cancellation to it while merge workers drain. |
| Cursor acquisition, frame preparation, native evaluation, queue publication and waits | Retain time-throttled or explicit checks at those boundaries. Replacing a wait-loop check with a large row-count throttle would delay timeout detection while no rows progress. |

A probe frame bounds input rows, but does not bound the number of duplicate
matches produced by a join. Moving the outer scan to frame-only checks is possible;
duplicate and collision loops must still remain independently interruptible. This
change retains the requested standard throttled call in the outer scan.

## Validation

**2,529 Java tests pass across 73 suites**, with 37 conditional skips and no
failures/errors (2,566 total). The 71-suite affected regression run is combined with
the additional parameterized map and VARCHAR-map suites, counting each suite once.
The [per-suite results](parallel-hash-join-group-by-throttling/regressions.csv),
[regression log](parallel-hash-join-group-by-throttling/regressions.txt.gz),
[map-test log](parallel-hash-join-group-by-throttling/map-tests.txt.gz) and
[benchmark package log](parallel-hash-join-group-by-throttling/build.txt.gz) are retained.
The [source patch](parallel-hash-join-group-by-throttling/source.patch.gz) is relative
to parent `4ae9efb0f0`; [source/jar hashes](parallel-hash-join-group-by-throttling/artifacts.sha256)
identify the tested implementation.

The new map test uses an active network breaker across
all four map types, disjoint and overlapping keys, cancellation, timeout and reuse.
It rejects a clock read for each entry. Existing collision/duplicate cadence tests
now configure and reset the breaker itself. The shared merge channel is tested
through both APIs across cancellation and reset.

## Confirmed benchmark run

The user confirmed that the machine was idle and authorized this run. The fixed
27-case recovery matrix ran sequentially on commit `d2e59fc832`, with active network
breakers, two rounds and ten measurements per arm/owner. No tests, external builds
or allocation profiling overlapped latency collection. The original reference,
workloads, warmups and individual bounds are unchanged.

**51 of 54 individual recovery bounds pass.** The largest candidate/reference
ratio is **1.1439**, against the 1.10 limit. Primary fused medians are
**327.088/325.963 ms**, with **7.080×/7.108×** ordinary/fused speedup. Both primary 2× gates
pass; task 9f's full recovery requalification **does not pass**.

| Round | Published reference, ms | Reproduced reference, ms | Shared-throttle candidate, ms | Candidate / lower reference | Speedup |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 319.808 | 313.995 | 327.088 | 1.0417 | 7.080× |
| 2 | 321.527 | 312.123 | 325.963 | 1.0443 | 7.108× |

The following bounds fail; successful cases do not offset them:

| Case | Round | Candidate, ms | Lower reference, ms | Ratio |
| --- | ---: | ---: | ---: | ---: |
| compressed-groups | 1 | 150.451 | 134.662 | 1.1172 |
| concurrent-4 | 1 | 103.189 | 90.206 | 1.1439 |
| high-cardinality | 2 | 234.938 | 212.276 | 1.1068 |

The [failed-case phase medians](parallel-hash-join-group-by-throttling/confirmed/failed-phase-medians.csv)
compare each failing round with the reference that supplies its bound. Compressed
groups and four-query concurrency regress in probe/aggregation. High cardinality
has slightly faster probe and merge medians, while the remaining timed work is
slower. That remaining interval includes final projection, sorting, ordered
consumption and cleanup; these timers alone do not identify a specific cause.

The [complete comparison](parallel-hash-join-group-by-throttling/confirmed/comparison.csv)
and [validator output](parallel-hash-join-group-by-throttling/confirmed/comparison.txt)
retain every bound. The candidate matrix contains **1,360 measured executions and
1,741 ordered result checks**. Logical work counters and ordered reference results
match the frozen reference. Every sample, plan, spread, phase timer and memory
observation is retained beside the
[candidate summary](parallel-hash-join-group-by-throttling/confirmed/candidate/summary.csv).
The [raw matrix archive](parallel-hash-join-group-by-throttling/confirmed/raw-matrix.tar.gz)
preserves output before whitespace normalization.

The C1 allocation matrix also passes: **24 cases, 234 measured executions and
405 owner/worker windows**, with zero unexplained bytes and all four workers in
every census case. See the
[allocation summary](parallel-hash-join-group-by-throttling/confirmed/allocation/summary.csv)
and [validation output](parallel-hash-join-group-by-throttling/confirmed/allocation.txt).
The established C1 byte/site scope and exemptions are unchanged; this does not
claim zero allocation under every JVM configuration or for application result lists.

Both matrices use the same frozen, previously tested jar. Its source patch exactly
matches `d2e59fc832` relative to `4ae9efb0f0`; the jar was built before the source
commit and retains its parent revision in build metadata. The
[source/jar hashes](parallel-hash-join-group-by-throttling/confirmed/artifacts.sha256),
[sequential driver](parallel-hash-join-group-by-throttling/confirmed/run.sh),
[latency commands](parallel-hash-join-group-by-throttling/confirmed/candidate/commands.txt)
and [allocation commands](parallel-hash-join-group-by-throttling/confirmed/allocation/commands.txt)
identify the measured implementation. The allocation driver only redirects the
existing script to that frozen jar and the repository working directory.

The earlier run was stopped during its first primary case because other agents
were using the machine. Its
[interruption record](parallel-hash-join-group-by-throttling/benchmark-status.txt)
and [partial raw run](parallel-hash-join-group-by-throttling/interrupted-latency.tar.gz)
remain excluded from acceptance evidence. The completed run has its own
[user-authorization record](parallel-hash-join-group-by-throttling/confirmed/authorization.txt).

**Obtain explicit user confirmation before starting further benchmarks.**
At this historical revision, task 9f still needed performance recovery. The
current task 9g report above records recovered latency and the new reliability
blocker. V1 is not complete, and the experimental flag remains false.

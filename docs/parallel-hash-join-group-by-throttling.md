# Shared circuit-breaker throttling for parallel hash joins

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

The preceding task 9f measurements remain in the
[recovery report](parallel-hash-join-group-by-recovery.md) and describe commit
`4ae9efb0f0`. New timings must use this implementation and the same fixed reference,
27 cases, two rounds, ten measurements per arm/owner and individual 1.10 bounds.

The latency run was stopped during its first primary case after the user explained
that other agents were using the same machine. **No completed performance result
or allocation benchmark is claimed for this follow-up.** The
[interruption record](parallel-hash-join-group-by-throttling/benchmark-status.txt) and
[partial raw run](parallel-hash-join-group-by-throttling/interrupted-latency.tar.gz)
are retained solely for provenance. They cannot qualify the recovery gate.

**Obtain explicit user confirmation before running any benchmark.** Once the
machine is available and the user confirms, rerun the fixed 27-case recovery
comparison and C1 allocation matrix on this implementation. Keep the original
reference and every per-round bound unchanged. Task 10 follows successful
requalification; V1 is not complete and the experimental flag remains false.

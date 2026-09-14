# Parallel hash join circuit-breaker boundaries

The fused reducer now follows parallel GROUP BY and Top-K's frame-level checks.
This follow-up to `d2e59fc832` removes polling inside hash-collision walks, map
redistribution and map merge loops. It also removes the probe scan's per-row
breaker and sequence-flag calls. The existing standard throttled API remains for
build rows and duplicate advances; there are no operator-specific countdowns.

## Current placement

| Work | Check placement |
| --- | --- |
| Probe input scan, including rejected predicates, INNER misses and outer null extensions | `UnorderedPageFrameReduceJob.reduce` checks the query before each frame, using the query start time and active cancellation binding. The fused owner checks again before merging, including after the last frame. |
| Duplicate matches | `FrozenHashJoinBuild.Probe.next()` calls `statefulThrowExceptionIfTripped()` for each advance. The sequence-active flag is also checked in the duplicate loop. A single input row can produce far more matches than its frame contains, so this uses the existing independently bound slot wrapper. |
| Build input | Standard throttled check per copied row. Known-size reservation, build growth, SYMBOL text work and chunked native copies retain their checks. |
| Hash collisions | No polling inside INT lookup, rehash insertion or SYMBOL lookup collision walks. Standalone checked `find()` still checks at lookup entry; fused `findUnchecked()` relies on the frame check. |
| Map redistribution | Ordinary `GroupByMapFragment.shard()` and ordinary map cursors, with no per-entry or sparse-slot checks. Reducer redistribution runs under the frame check; final `shardAll` checks before each fragment. |
| Owner map merge | Ordinary two-argument `Map.merge()`, with owner checks before and after the result merge phase. |
| Parallel shard merge | Existing owner publication/wait checks and worker cancellation-flag check before each shard task. Every published task drains before the owner releases shared execution storage. Individual map merges have no entry polling. |
| Scalar merge and output | Existing per-slot scalar checks; a common owner check after result merging and before output. Output cursor checks and sparse output-cursor handling remain. |
| Cursor acquisition, frame preparation, native decoding and scheduler waits | Existing checks at these boundaries remain. |

Native frame construction can absorb a short trailing frame. Parquet frames
follow row-group boundaries. Cancellation of a scan with rejected rows or misses
therefore completes at an actual frame boundary, rather than after a fixed number
of probe rows. Redistribution and an individual shard/map merge similarly finish
before the next boundary observes cancellation. There is no separate guarantee
for interruption inside a hash-collision walk. These are the established work
units used by the parallel GROUP BY implementation.

The removed plumbing includes the breaker-taking map merge and fragment shard
overloads, breaker propagation into individual shard merges, and the temporary
stateful overrides in `PostAggregationCircuitBreaker`. The shared worker channel
uses its existing non-mutating flag checks. No global network-breaker or wrapper
implementation changes are needed. Build ownership, slot release, task drain,
native accounting, query eligibility and the experimental default remain intact.

## Validation

**2,517 Java tests passed across 73 affected suites**, with 37 conditional skips and no failures/errors (2,554 total). The run covers fused semantics/storage/concurrency,
ordinary GROUP BY and Top-K, map implementations, memory accounting, breaker
binding and scheduler drain. Core compilation with the release Rust profile and
`git diff --check` pass. The [per-suite results](parallel-hash-join-group-by-breaker-boundaries/regressions.csv),
[complete log](parallel-hash-join-group-by-breaker-boundaries/regressions.txt.gz),
[commands](parallel-hash-join-group-by-breaker-boundaries/commands.txt) and
[source patch](parallel-hash-join-group-by-breaker-boundaries/source.patch.gz)
identify the tested change. The initial focused failure and successful rerun logs
are retained beside them.

The updated active-network tests count clock reads separately inside the fused
reducer and at the shared frame check. Rejected, all-miss and outer-null scans
exercise 100,000 rows across native, mixed and Parquet storage in keyed and scalar
modes. Successful scans perform no breaker clock reads inside the reducer;
cancellation and timeout triggered at row 32 are observed at a frame boundary.
A single-frame outer-miss case finishes all 1,000 rows and throws before output.
Duplicate tests retain their configured throttle bound within a 100,000-match
chain, including rejected post-join rows, cancellation, timeout and reuse.

Existing owner/sharded/scalar merge cancellation and injected-failure tests retain
slot/native-balance assertions, mandatory drain and successful factory reuse.
Standalone lookup rebinding still checks cancellation at checked lookup entry and
duplicate advance, with an unaffected peer. Collision correctness/growth tests
remain; tests requiring interruption inside the removed collision/map-entry loops
are removed with those APIs.

The first focused run exposed four assertions tied to the old contract: three
assumed the configured frame target or per-row bound, and one expected unchecked
collision lookup to throw. The frame assertions now account for native tail
coalescing and Parquet row groups; the unchecked lookup assertion checks a miss
without polling. No production change was needed in response to those failures.

The RFC's earlier per-row/inner-map polling instructions are updated to these
boundaries; the [RFC edit](parallel-hash-join-group-by-breaker-boundaries/rfc-130.patch.gz)
also adds pending V1 task 9g.

## Benchmark status

**No performance or allocation benchmarks were run for this change.** Obtain
explicit user confirmation before starting a new batch on this shared machine.
The last confirmed run measured `d2e59fc832`: 51/54 recovery bounds passed, primary
fused medians were 327.088/325.963 ms with 7.080×/7.108× speedup, and all 24 C1
allocation cases passed. Those results are retained in the
[historical shared-throttle report](parallel-hash-join-group-by-throttling.md) and
do not qualify this implementation.

New RFC **task 9g is pending for a separate benchmark session**, as requested by
the user. It repeats the fixed 27-case recovery and 24-case C1 allocation matrices
on the final source/jar and publishes every gate's outcome. Task 9f remains open
pending that performance requalification. Keep every original per-case/repetition 1.10 bound, the primary
2× gate, the frozen reference and the failed trials. Task 10's full 51-case rollout
matrix follows recovery. V1 is not complete; the experimental flag remains false.

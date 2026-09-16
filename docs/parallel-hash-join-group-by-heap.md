# Fused hash join: bounded retained heap (RFC task 9b)

[RFC 130](https://github.com/questdb/rfc/discussions/130) task 9b is complete.
Data-dependent execution storage uses the allocation-time query tracking from
[task 9a](parallel-hash-join-group-by-memory.md). The experimental flag remains
false. Tasks 9c (allocation-free execution), 9d (breakers), and 9e (expanded
semantic/storage/negative coverage) precede the repeated task 10 rollout gate.
These measurements concern retained heap, not allocation rate or query speed.

## Storage and ownership audit

| Execution state | Backing, bounds and lifetime |
| --- | --- |
| INT hash slots, copied typed build rows, duplicate links, copied SYMBOL hash slots/offsets/characters | Existing `IntHashJoinBuild` buffers are native and query tracked. Rehash charges old and new backing together. The frozen dictionary survives output consumption and is released on cursor close. |
| Frame lengths, formats, row groups/bounds, source row IDs and covered-key/bounds sentinels | `PageFrameAddressCache.frameMetadata` is a tracked `DirectLongList`, with nine longs per frame and initial capacity for 64 frames. It grows alongside the existing four tracked column-address/size vectors. Close releases all five vectors, including partial initialization. |
| Ordered build-filter and unordered probe frame counts | Both sequences read the native address cache's frame length; their redundant heap `LongList` stores are removed. |
| Per-frame decoder references | Table cursors resolve an already-open decoder from the borrowed reader by partition index; external Parquet cursors return their single decoder. Projection, extra-null-column and query-registration wrappers delegate that lookup. After enumeration the source remains open and stable until consumers drain. No copied Java reference per frame is needed. |
| Sparse decoder declarations and slice index | `PageFrameMemoryPool` uses tracked `DirectIntList` counts and a tracked `DirectLongList` frame-to-slice index. The selected row IDs were already tracked native storage. Rebinding, query release, new declarations and close free the new backing. |
| Decoded frames and buffers | Page/aux vectors, column tops, resource handles and Java/Rust decoder scratch retain task 9a tracking. Active decoder shells are capped at 4 for monotonic access or 256 for scattered access; at most 256 closed shells are pooled. Sparse declarations obey the same cap. An evicted sparse slice can be decoded again from the native index. |
| Source SYMBOL access | Build copying acquires one uncached symbol view per copied SYMBOL column, reads native source keys, and frees every view in `finally`. Build predicates and the owner probe predicate initialize with cloned, uncached views, as worker predicates already do. This prevents table-reader caches retaining one String per observed symbol. Joined/output views retain their existing uncached source/native frozen-dictionary access. |
| SYMBOL predicate helpers | During fused child/function compilation, LIKE/ILIKE, regex, SYMBOL equality and SYMBOL/timestamp comparison select their existing uncached implementations. No matching-key list, symbol translation array or hit/miss bitset grows with the source dictionary. The compiler setting is restored on success, rejection and failure. Ordinary compilation retains its existing defaults. Uncached patterns do not provide the key-set interface required by adaptive pattern-index plans. |
| Keyed aggregate state and merge destinations | Existing tracked native maps store entries and values. Owner/worker fragments contain one map shell plus up to 256 shard shells; sharded destinations contain 256 references. These bounds depend on fixed worker/shape configuration, not group cardinality. Clear frees backing after task/merge drain. |
| Scalar aggregate state and output | One tracked scalar value per owner/worker; no grouping table. Final record/cursor views borrow live state. Function lists, record views, column mappings and build offsets are bounded by schema, aggregate count and configured worker slots. Factory close releases shells; cursor close releases execution backing. |

Covered-index inputs keep the ordinary plan on either physical side. Their
per-frame descriptors and covered decoder caches are outside this V1 scan path.
The address cache retains compatibility lists for covered callers and cursors
without a decoder resolver; they stay empty in qualified fused scans. Source
native columns, source dictionaries and Parquet metadata mappings retain their
table-reader ownership. Codec control objects and column conversion maps are
bounded by schema and decoder slots; row/data/dictionary buffers are native.

The inline review covered the changed code and shared callers: ordinary frame
cursors, ordered filters, live views, projections, registration wrappers, covering
indexes, sparse sorted output, source-symbol initialization and speculative
compiler cleanup. The shared decoder cache cap can increase repeated decoding
for scattered sparse reads. Uncached predicates repeat string/pattern work for
repeated symbols. Both tradeoffs belong in task 10's rerun.

## Retained-heap experiment

`HashJoinGroupByHeapBenchmark` uses `Instrumentation.getObjectSize()` and walks
strong references from the compiled factory after task drain. It records exact
object/array sizes and per-class totals, including every array element. It fails
if a field cannot be inspected. This is a reachable-object measurement rather
than process used-heap sampling, so concurrent GC does not determine the result.

The boundary includes the factory, atoms, cursors, functions, slots, maps, frame
caches, decode pools and source SYMBOL caches. It excludes borrowed engine,
configuration, message bus, execution context/tracker, logging/files services,
threads/classes, static state and JVM reference/cleaner queues. Table-reader
shells and their source SYMBOL dictionaries/caches are included, while shared
partition, mmap and reader-pool metadata are excluded. Baseline SQL, data loading,
compilation infrastructure, profiling maps and checksum arrays belong to the
harness and are outside the graph. This boundary does not measure transient
allocation or prove task 9c.

Each fresh JVM has four real query workers, a fixed query shape, 64-row native
frames/Parquet row groups, two selected payload partitions, and an excluded
active sentinel partition. The fanout dimension has one probe row in one payload
partition. The runner checks physical partition formats and fused EXPLAIN
selection. Native, mixed and Parquet storage each run keyed owner merge, keyed
sharded merge and scalar aggregation. The query includes build-side LIKE,
probe-side ILIKE, copied symbols, duplicate enumeration and final SYMBOL access.

| Dimension | Increased from 1,024 through 8,192 and 65,536 to 262,144 | Fixed properties |
| --- | --- | --- |
| rows | Probe rows and frame count | 64 build rows/keys, 16 symbols/groups |
| keys | Distinct build/probe keys and rows | 16 symbols/groups |
| symbols_groups | Distinct keys, SYMBOL values and final keyed groups | Query shape, workers and columns |
| fanout | Build duplicates for one join key | One probe row, 16 symbols/groups |

Factories are compiled after each data/storage change, then each executes twice.
No data-sized heap pool is warmed before the first sample. Each execution is
sampled after the first output becomes ready (`live`), after consuming every
output row (`output`), and after cursor close (`closed`). The ordinary query runs
afterward and must match result count plus commutative sum/XOR checksums over all
result columns; repeated candidate executions must also match. Ordinary readers
are released between cases so baseline SYMBOL caches do not contaminate the
next candidate measurement. All numerical inputs are exactly representable ones.

For every fixed storage/mode/dimension/phase, the runner rejects retained heap
more than 64 KiB above the smallest cardinality and any individual array larger
than 64 KiB. It also rejects nonzero query-native bytes after close. The bounds
are guards against regression; the actual observed range is reported below.

All 864 snapshots passed, covering 288 candidate executions and 144 ordinary
reference executions. Every comparison passed, and all 288 cursor closes left
zero query-native bytes. The largest heap increase relative to the 1,024-row
sample at a fixed storage/mode/dimension/phase was **2,936 bytes**. The largest
individual Java array was **16,400 bytes**, independent of data cardinality.

| Storage | Merge | Retained heap min–max (bytes, all phases) | Largest live native sample (bytes) |
| --- | --- | ---: | ---: |
| native | owner | 329,776–332,752 | 47,480,864 |
| native | sharded | 1,534,960–1,537,936 | 47,480,832 |
| native | scalar | 258,496–263,872 | 13,402,752 |
| mixed | owner | 526,832–532,320 | 51,449,574 |
| mixed | sharded | 1,732,016–1,737,504 | 51,694,406 |
| mixed | scalar | 455,552–463,440 | 17,584,931 |
| parquet | owner | 527,600–532,376 | 52,741,222 |
| parquet | sharded | 1,732,784–1,737,560 | 52,739,142 |
| parquet | scalar | 456,320–463,496 | 18,661,027 |

The larger sharded control footprint comes from the fixed 256 map/shard shells
per slot and merge destinations; it does not scale with group count. Parquet
adds bounded decoder/schema shells. Native samples grow with data cardinality
and include all simultaneously retained state at the snapshot; they are not
peak-allocation measurements. The allocation/limit regressions cover transient
growth charges and failure cleanup.


The branch does not retain the raw artifacts: the per-snapshot measurements, the
per-class totals, and the environment/source/jar hashes.
The environment recorded the task 9a base commit and the Java diff hash used to
build the measured jar. Nine per-storage-mode logs captured every plan and
sample alongside the original engine output.

## Regression evidence and reproduction

New regression assertions cover:

- 4,096-frame native metadata growth, initial/growth budget rejection, unchanged
  live charges on failure, equality with native memory-tag growth and reuse.
- Projected native/Parquet frames in both directions, multiple source decoders,
  row-ID mapping and borrowed decoder identity after enumeration.
- 1,024 sparse Parquet frames with fail budgets at both new index allocations,
  exact combined native charges, a 256-shell maximum, forward/backward revisits,
  correct values after eviction, release and reuse.
- 8,192 distinct source symbols with cached source tables: build and owner-probe
  filters leave source cache sizes at zero, including repeated keyed/scalar runs.
- High-cardinality LIKE/ILIKE/regex/equality/timestamp predicates, owner/sharded
  and scalar results, fallback behavior, and restoration of the compiler setting.
- Covering-index plans on either input retain ordinary selection and results.

The final Java run passed 1,933 tests across 52 suites, with two existing
conditional skips and no failures/errors (1,935 total). The per-suite totals, which the branch does not retain,
include fused selection/results, ordinary callers, storage, concurrency,
cancellation, failure and reuse. Rust sources are unchanged in task 9b; the task
9a native build remains required. The benchmark package passed and
the 100,000-row/four-worker smoke run passed all 43 explicit ordered result
comparisons, including 40 measured executions. This is integration evidence,
not a repeat of the task 10 performance gate. The branch does not retain the smoke
output, which captured the plans, counters and result-check count.

Use JDK 25 and Maven 3 with the pinned Rust toolchain. Do not run Maven builds or
tests concurrently in one checkout: they share `target` output. The benchmark
runner can use the completed jar while Maven tests execute.

```bash
mvn -pl benchmarks -am package -P build-rust-library,qdbr-release -DskipTests -Dmaven.test.skip=true
# The retained-heap harness script ran next; the branch does not retain it.
```

```bash
mvn -pl core test -P build-rust-library,qdbr-release \
  -Dtest=HashJoinGroupByQualificationTest,HashJoinGroupByConcurrentTest,HashJoinGroupByPlannerTest,AsyncHashJoinGroupByTest,HashJoinGroupByFunctionsTest,HashJoinGroupByCandidateTest,IntHashJoinBuildTest,SqlCodeGeneratorWorkerFunctionExtractionTest,SqlCodeGeneratorTest,SqlOptimiserTest,GroupByUtilsTest,GroupByRewriteTest,GroupByTest,HashJoinTest,JoinRecordMetadataTest,GroupByMapFragmentTest,AsyncFilterContextTest,AsyncFilteredRecordCursorFactoryTest,QueryRegistryMemoryTrackerTest,ParallelGroupByMemoryTrackerTest,ParallelHorizonJoinMemoryTrackerTest,PostAggregationCircuitBreakerTest,QueryParallelFiberDispatcherTest,HorizonJoinTest,GroupByFunctionsUpdaterFactoryTest,SortedRunsMergeTest,AvgDoubleGroupByFunctionFactoryTest,CountSymbolGroupByFunctionFactoryTest,CountTest,CountColumnTest,OrderedMapTest,Unordered4MapTest,Unordered8MapTest,ParquetColumnTypeConversionTest,ParquetRowGroupPruningTest,ParquetMemoryTrackerTest,ParallelParquetMemoryTrackerTest,ReadParquetCancellationTest,ParallelFilterTest,DirectIntListTest,PageFrameReduceTaskTest,PageFrameAddressCacheTest,TimeFrameCursorTest,CoveringIndexParallelDecodeTest,SymbolPatternIndexTest,PageFrameCursorTest,PageFrameMemoryRecordTest,PageFrameRecordCursorImplFactoryTest,LiveViewPageFrameCursorTest,QueryProgressValidationTest,PageFrameCursorReleasePartitionTest,PageFrameScanCancellationTest
```

```bash
java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED \
  --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
  -cp benchmarks/target/benchmarks.jar org.questdb.HashJoinGroupByBenchmark \
  --rows=100000 --plants=1000 --workers=4 --warmups=1 --runs=10 --repetitions=2 \
  --revision=task9b-working-tree \
  '--candidate-compiler=org.questdb.HashJoinGroupByBenchmark$PlannerCandidateCompiler'
```

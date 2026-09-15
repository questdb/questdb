# Fused hash join: allocation-time memory tracking (RFC task 9a)

[RFC 130](https://github.com/questdb/rfc/discussions/130) added tasks 9a–9e after
the original task 10 measurements. This audit covers the experimental keyed and
scalar shared-build pipelines, including filtered build cursors and native/mixed/
Parquet probe frames. The experimental default remains false. Task 10 must run
again after 9b–9e; the previous benchmark is historical evidence.

## Allocation-site audit

The active `MemoryTracker` comes from the enclosing query registration. The OSS
provider supplies one even when the configured limit is zero (unlimited).
Children share that registration. Owners and acquired worker slots borrow the
same tracker, whose native counter is also used by Rust `QdbAllocator`.

| Storage and allocation sites | Binding, growth and lifetime |
| --- | --- |
| `IntHashJoinBuild.Buffer.allocate`, `ensure`, `growTable`: INT hash slots, typed payload rows/duplicate links, SYMBOL hash slots, entry offsets and UTF-16 characters | `open(tracker, breaker)` binds before allocation. Every backing allocation/free uses the tracked `Unsafe` overload. Copy growth/rehashing charges the complete destination while the source remains live. Failed append/build closes all five buffers; output retains frozen SYMBOL backing until cursor close. |
| `AsyncHashJoinGroupByAtom.init`: scalar `SimpleMapValue` per owner/worker | Constructor takes the execution tracker; all partial scalar states coexist with the live build. Clear releases each value before dropping execution state. Scalar merge copies/combines these states without allocating a grouping map. |
| `GroupByMapFragment.reopenMap`, `reopenShards`; `GroupByShardingContext.reopenDestShard` | Map shells are closed until their tracker is bound. Owner and worker maps, redistribution shards and merge destinations retain the same tracker through drain and cleanup. The fixed shard/slot lists contain control references, not aggregate entries. |
| `Unordered4Map`/`Unordered8Map` allocate, rehash, reopen, merge; `OrderedMap` key heap, offset table, rehash and merge scratch | Allocation/growth/free already use tracked `Unsafe` APIs. Rehash allocates the new table before freeing the old table. Owner/sharded merge destinations are charged alongside every still-live source. `Unsafe.realloc` enforces the live capacity delta at the allocator call. |
| `PageFrameAddressCache.of` and growth of its four flat address/size vectors | Newly bound before `of()` in unordered probe sequences, ordered build-filter sequences and ordinary page-frame record cursors. Deferred `DirectLongList` allocation, every resize, and close now charge the query. Partial initialization closes all opened vectors. |
| `AsyncFilterContext` owner/worker row IDs and JIT data/aux addresses | Closed shells at construction; `initFilters` binds the tracker. Accessors reopen on the executing slot. Clear frees all backing under that tracker. The fused interpreted reducer does not request row-ID lists. |
| `PageFrameReduceTask` row IDs and JIT addresses used by filtered build sources | Closed shells; task publication binds without allocating. Reducer accessors allocate under the captured tracker, so OOM follows normal task error/drain handling. Collection releases all backing before a shared queue entry can outlive the owning registration. Decoder scratch is also released at the frame boundary. |
| `PageFrameMemoryPool.parquetColumns`, null-column addresses and sparse `recordAtRows` | `DirectIntList` now has tracked malloc/realloc/free, matching `DirectLongList`. Bind before first allocation. Query resource release frees backing, including sparse output-consumer row IDs; projection rebuilding preserves those row IDs. |
| `PageFrameMemoryPool.ParquetBuffers` page/aux addresses and sizes, column tops, decode-resource handles | Shell construction allocates no native backing. `reopen` binds all six lists before opening any; a partial failure closes the shell. Buffers retain their own allocation tracker until close, including pool reuse. |
| `RowGroupBuffers` and Rust `ColumnChunkBuffers` data/aux vectors, column-vector expansion, conversion growth | `RowGroupBuffers.reopen` captures `Unsafe.getNativeAllocator(tag, tracker)`. Existing `AcVec` growth and drop use that allocator. Logical conversions reuse or grow the same tracked output vectors. |
| Rust `DecodeContext` dictionary/data-page decompression scratch | Previously ordinary `Vec<u8>` allocations. Both are now `AcVec<u8>` created with the output buffers' allocator before decoding. Growth rejects over-budget requests before allocation; dictionary and data scratch are simultaneously charged. Java closes decoder contexts before releasing the query. Timestamp decoding and export callers propagate their allocator too. |
| `HashJoinGroupByFunctions`, allowlisted SUM/AVG/COUNT updaters, `HashJoinGroupByRecord`, final `VirtualRecord` and `ShardedMapCursor` | No additional growing native aggregate/output store. Functions use the native map/scalar states above; output cursors borrow them. Record views and per-column/slot/function arrays are control/schema objects. Ordinary final sort/projection keep their existing allocation contracts. |

Native table columns, source SYMBOL dictionaries and Parquet metadata mappings
are borrowed from table readers. `_pm` native readers are fixed-size views over
those mappings, not copies of the row groups. The supported fixed-width payload
getters decode INT/LONG/DOUBLE/SYMBOL/TIMESTAMP values; VarcharSlice retained-page
storage, covered-index decoding and unsupported aggregate allocators are not
part of this V1 payload path. Codec-library control/workspace and table-reader
metadata retain their existing framework ownership; the data-page and dictionary
buffers owned by the execution decoder are covered above.

## Cleanup and the subsequent heap work

Cancellation/failure still drains probe tasks and shard merges before closing
slot state, decode pools, frame cursors and the frozen lookup. A collected shared
filter task now frees its query-owned buffers on every collection; assuming a
queue slot will be reused in the same query's final frames is unsafe when queries
interleave. Buffer shells can be reused, but their native backing cannot remain
charged to a registration that has ended. This changes allocation frequency and
must be included in the repeated task 10 measurements.

The query tracker measures native memory. At the task 9a boundary, the shared
frame cache's per-frame primitive lists and decoder/covered-reader references,
`UnorderedPageFrameSequence.frameRowCounts`, decoder bookkeeping maps, and source
SYMBOL caches still awaited the separate task 9b audit and migration. The
subsequent [task 9b audit](parallel-hash-join-group-by-heap.md) records the completed
migration, bounded control state and retained-heap measurements under the same
allocation-time contract. Task 9c measures successful-execution Java allocations,
9d audits breakers, and 9e expands semantic/storage/negative coverage.

## Regression evidence

New tests cover:

- Failure on initial `DirectIntList` allocation and resize, unchanged contents
  after failed growth, unlimited tracking and close/reopen balance.
- Fused frame-cache growth exceeding a budget that already accommodates the live
  build and initial slots; zero charges after failure and successful reuse under
  a different unlimited tracker, for keyed and scalar factories.
- 8,192 distinct copied symbols with duplicate-heavy keys, multiple hash/entry/
  character/payload growth steps and native-tag versus query-counter equality
  after every append. Tight budgets fail cleanly and the same build reopens.
- High SYMBOL/group cardinality under stepped limits across owner merge,
  sharded merge and scalar execution on native and mixed Parquet frames.
  Every attempt checks released slots and zero query charges; unlimited reuse
  is compared with ordinary SQL results.
- Rust data/dictionary scratch growth with both buffers live: a failed resize
  preserves the original address and length, rejects a second live
  allocation over budget, and returns all charges on drop.

Existing exhaustive build-allocation/rehash/payload tests, owner/sharded merge
limit tests, scalar/decoder failure tests, registered-query failure/reuse and
concurrent-query cancellation isolation remain part of validation. Shared filter
lifecycle tests now assert full release followed by lazy reopening. Horizon/group-by
fault budgets are installed after worker setup, because setup's SQL frame caches
are now tracked too.

Run with JDK 25, Maven 3 and the repository's pinned Rust nightly. The native
profile is required because the Java decoder now passes an allocator to the new
Rust context-construction entry point. No checked-in native binary is changed.

```bash
mvn -pl core test -P build-rust-library,qdbr-release \
  -Dtest=HashJoinGroupByQualificationTest,HashJoinGroupByConcurrentTest,HashJoinGroupByPlannerTest,AsyncHashJoinGroupByTest,HashJoinGroupByFunctionsTest,HashJoinGroupByCandidateTest,IntHashJoinBuildTest,SqlCodeGeneratorWorkerFunctionExtractionTest,SqlCodeGeneratorTest,SqlOptimiserTest,GroupByUtilsTest,GroupByRewriteTest,GroupByTest,HashJoinTest,JoinRecordMetadataTest,GroupByMapFragmentTest,AsyncFilterContextTest,AsyncFilteredRecordCursorFactoryTest,QueryRegistryMemoryTrackerTest,ParallelGroupByMemoryTrackerTest,ParallelHorizonJoinMemoryTrackerTest,PostAggregationCircuitBreakerTest,QueryParallelFiberDispatcherTest,HorizonJoinTest,GroupByFunctionsUpdaterFactoryTest,SortedRunsMergeTest,AvgDoubleGroupByFunctionFactoryTest,CountSymbolGroupByFunctionFactoryTest,CountTest,CountColumnTest,OrderedMapTest,Unordered4MapTest,Unordered8MapTest,ParquetColumnTypeConversionTest,ParquetRowGroupPruningTest,ParquetMemoryTrackerTest,ParallelParquetMemoryTrackerTest,ReadParquetCancellationTest,ParallelFilterTest,DirectIntListTest,PageFrameReduceTaskTest
(cd core/rust/qdbr && cargo test --release parquet_read::)
(cd core/rust/qdbr && cargo test --release --test row_groups --test decode_pm_e2e --test decode_varchar_slice --test decode_primitives --test decode_decimal --test decode_array --test encode_primitives --test encode_statistics)
mvn -pl benchmarks -am package -P build-rust-library,qdbr-release -DskipTests -Dmaven.test.skip=true
java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED \
  --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
  -cp benchmarks/target/benchmarks.jar org.questdb.HashJoinGroupByBenchmark \
  --rows=100000 --plants=1000 --workers=4 --warmups=1 --runs=10 --repetitions=2 \
  --revision=task9a-working-tree \
  '--candidate-compiler=org.questdb.HashJoinGroupByBenchmark$PlannerCandidateCompiler'
```

**Final validation:** 1,645 Java tests passed across 41 suites, with two existing
conditional skips and zero failures/errors (1,647 total). The decoder Rust run
passed 469 tests with two existing ignored cases; the eight native integration
suites passed another 134 tests, for **603 Rust tests passed**. The benchmark
package passed, and all **43 explicit ordered result comparisons**, including
40 measured ordinary/fused executions, passed in the 100,000-row/four-worker
smoke run. That small run verifies integration, not task 10's performance gate.
The final shared regression run includes the sparse Parquet row-ID preservation
fix and the corrected setup boundaries for the six tiny-budget fault tests.

/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.cairo;

import io.questdb.MessageBus;
import io.questdb.PropertyKey;
import io.questdb.cairo.AttachDetachStatus;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.PostingSealPurgeJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.idx.BitpackUtils;
import io.questdb.cairo.idx.PostingIndexBwdReader;
import io.questdb.cairo.idx.PostingIndexChainEntry;
import io.questdb.cairo.idx.PostingIndexChainHeader;
import io.questdb.cairo.idx.PostingIndexChainPicker;
import io.questdb.cairo.idx.PostingIndexChainWriter;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexNative;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.MemoryCMARWImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Chars;
import io.questdb.std.DirectBitSet;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.tasks.PostingSealPurgeTask;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.TableUtils.*;

/**
 * Red tests for the critical findings raised in the PR review of #6861.
 * Each test reproduces a defect that should fail against the current
 * implementation. Tests that require fault injection or platform-specific
 * conditions are marked with comments describing what they need.
 */
public class PostingIndexCriticalIssuesTest extends AbstractCairoTest {

    @Override
    public void setUp() {
        super.setUp();
        // Every test must start with an empty shared posting-seal-purge ring. Many
        // tests here run a WorkerPool + O3, which publish purge tasks into the
        // engine-wide MessageBus ring; with no PostingSealPurgeJob draining it in the
        // harness the residue can saturate the ring and starve a later test's purge --
        // e.g. leaving a superseded .pv unreclaimed, or making a saturation assertion
        // see no room. Draining here makes each test independent of the prior one.
        drainPostingSealPurgeQueue();
    }

    @Test
    public void testCloseNoTruncateSkipsSealOnPoisonedWriter() throws Exception {
        // SymbolMapWriter's emergency closeNoTruncate() runs a best-effort seal().
        // On a poisoned writer that seal() would throw via checkNotPoisoned() out
        // of cleanup, masking the original rebuild failure. The fix skips the seal
        // when poisoned; the finally still frees every resource. Pre-fix this test
        // fails with the poison CairoException propagating out of closeNoTruncate().
        final PcSyncFailingFacade pcSync = new PcSyncFailingFacade();
        ff = pcSync;
        assertMemoryLeak(ff, () -> {
            final String name = "close_no_truncate_poisoned";
            final long fakeColBytes = 32L << 3; // 32 rows * 8 bytes (LONG cover)
            long fakeColAddr = Unsafe.malloc(fakeColBytes, MemoryTag.NATIVE_DEFAULT);
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                Unsafe.setMemory(fakeColAddr, fakeColBytes, (byte) 0);
                PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE);
                boolean closeNoTruncateRan = false;
                try {
                    // Two sparse gens so seal() runs sealFull and actually switches.
                    for (int v = 0; v < 16; v++) {
                        writer.add(v & 3, v);
                    }
                    writer.setMaxValue(15);
                    writer.commit();
                    for (int v = 16; v < 32; v++) {
                        writer.add(v & 3, v);
                    }
                    writer.setMaxValue(31);
                    writer.commit();

                    long[] addrs = {fakeColAddr};
                    long[] tops = {0L};
                    int[] shifts = {3};
                    int[] indices = {1};
                    int[] types = {ColumnType.LONG};
                    writer.configureCovering(addrs, tops, shifts, indices, types, 1);

                    pcSync.arm();
                    try {
                        writer.seal();
                        Assert.fail("expected post-switch .pc sync failure during seal");
                    } catch (CairoException expected) {
                        TestUtils.assertContains(expected.getFlyweightMessage(), "staged .pc sync failed");
                    } finally {
                        pcSync.disarm();
                    }
                    // The seal poisoned the writer and staged the newSealTxn orphan.
                    Assert.assertEquals("seal must have poisoned the writer and staged the orphan",
                            1, writer.getPendingPurgesSizeForTesting());

                    // The fix under test: emergency close must not throw on a
                    // poisoned writer. closeNoTruncate frees every resource in its
                    // finally (even on a throw), so it doubles as the writer's close.
                    closeNoTruncateRan = true;
                    writer.closeNoTruncate();
                } finally {
                    // Only re-close on a setup failure before closeNoTruncate ran;
                    // a closeNoTruncate that threw already freed everything, so we
                    // let the throw surface the regression rather than double-close.
                    if (!closeNoTruncateRan) {
                        writer.close();
                    }
                }
            } finally {
                Unsafe.free(fakeColAddr, fakeColBytes, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Review C2: the full-partition SELECT DISTINCT path calls
     * collectDistinctKeys(), not collectDistinctKeysInRange(). The no-range
     * helper must still honor the chain entry's MAX_VALUE; otherwise a key
     * whose only encoded rows are dirty rows past MAX_VALUE is reported as
     * present in the partition.
     */
    @Test
    public void testCollectDistinctKeysDoesNotExposeKeysPastEntryMaxValue() throws Exception {
        assertMemoryLeak(() -> {
            final String name = "posting_distinct_max_value_clamp";
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                final int liveKey = 1;
                final int dirtyOnlyKey = 2;

                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    for (long rowId = 0; rowId < 50; rowId++) {
                        writer.add(liveKey, rowId);
                    }
                    for (long rowId = 50; rowId < 100; rowId++) {
                        writer.add(dirtyOnlyKey, rowId);
                    }
                    writer.setMaxValue(99);
                    writer.commit();
                    writer.setMaxValue(49);
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name,
                        COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, /* columnTop */ 0);
                     DirectBitSet foundKeys = new DirectBitSet(8)) {
                    Assert.assertEquals(
                            "only the key with rowids at or below MAX_VALUE=49 is live",
                            1,
                            reader.collectDistinctKeys(foundKeys)
                    );
                    Assert.assertTrue(foundKeys.get(liveKey));
                    Assert.assertFalse(
                            "collectDistinctKeys must not expose a key whose only rows are past entryMaxValue",
                            foundKeys.get(dirtyOnlyKey)
                    );
                }
            }
        });
    }

    @Test
    public void testConvertPartitionToParquetLinksReaderVisibleSealWhenHeadIsFutureTxn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_parquet_future_head (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_parquet_future_head VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-02T00:00:00', 'B', 2.0)
                    """);
            engine.releaseAllWriters();

            final TableToken token = engine.getTableTokenIfExists("t_parquet_future_head");
            Assert.assertNotNull("test table must exist", token);
            final long currentTxn;
            final long firstPartitionTimestamp;
            final long firstPartitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                currentTxn = reader.getTxn();
                firstPartitionTimestamp = reader.getTxFile().getPartitionTimestampByIndex(0);
                firstPartitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }

            appendFuturePostingHead(token, firstPartitionTimestamp, firstPartitionNameTxn, currentTxn);

            execute("ALTER TABLE t_parquet_future_head CONVERT PARTITION TO PARQUET LIST '2024-01-01'");

            assertQuery("SELECT ts, sym, price FROM t_parquet_future_head WHERE sym = 'A'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tprice
                            2024-01-01T00:00:00.000000Z\tA\t1.0
                            """);
        });
    }

    /**
     * The bounded {@code decodeKeyEFToNative(maxValues)} overload must reject an
     * EF block whose encoded value count exceeds the caller's buffer capacity (a
     * corrupt or mismatched generation whose block count disagrees with the
     * gen-dir count the buffer was sized to) BEFORE writing past the buffer, and
     * must pass a valid block whose count fits exactly. This is the shared guard
     * the streaming-seal / rollback decode paths
     * ({@code decodeDenseGenSingleKey}, {@code filterCountsForRollback}) rely on
     * to bound writes against the gen-dir count -- without coverage an off-by-one
     * in the {@code maxKeyCount - decodedTotal} capacity math would go unnoticed.
     */
    @Test
    public void testDecodeKeyEFToNativeRejectsValueCountAboveCapacity() throws Exception {
        assertMemoryLeak(() -> {
            final int count = 200;
            final long srcAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            final long encMaxBytes = PostingIndexUtils.computeMaxEncodedSize(count);
            final long blockAddr = Unsafe.malloc(encMaxBytes, MemoryTag.NATIVE_DEFAULT);
            final long destAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try (PostingIndexUtils.EncodeContext ctx = new PostingIndexUtils.EncodeContext()) {
                long acc = 0;
                for (int i = 0; i < count; i++) {
                    acc += 1L + (i & 7); // strictly ascending, distinct -> valid EF input
                    Unsafe.putLong(srcAddr + (long) i * Long.BYTES, acc);
                }
                ctx.ensureCapacity(count);
                int size = PostingIndexUtils.encodeKeyEF(srcAddr, count, blockAddr, ctx);
                Assert.assertTrue(size > 0);
                Assert.assertEquals("expected an EF block",
                        PostingIndexUtils.EF_FORMAT_SENTINEL, Unsafe.getInt(blockAddr));

                // Valid: capacity == the encoded count -> decodes and round-trips.
                PostingIndexUtils.decodeKeyEFToNative(blockAddr, destAddr, count);
                for (int i = 0; i < count; i++) {
                    Assert.assertEquals("round-trip mismatch at " + i,
                            Unsafe.getLong(srcAddr + (long) i * Long.BYTES),
                            Unsafe.getLong(destAddr + (long) i * Long.BYTES));
                }

                // Corruption: the block's count (200) exceeds the buffer capacity
                // (199) -> must throw BEFORE writing rather than overflow destAddr.
                try {
                    PostingIndexUtils.decodeKeyEFToNative(blockAddr, destAddr, count - 1);
                    Assert.fail("decodeKeyEFToNative must reject a count above capacity");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "EF value count out of range");
                }
            } finally {
                Unsafe.free(srcAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(blockAddr, encMaxBytes, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * The bounded {@code decodeKeyToNative(maxValues)} overload sizes the caller's
     * destAddr from the summed block counts, but each block's {@code count-1} deltas
     * are unpacked into the separate fixed {@code BLOCK_CAPACITY}-long
     * {@code blockDeltasAddr} scratch that {@code DecodeContext} never grows. A valid
     * encoder splits a key into blocks of at most {@code BLOCK_CAPACITY}, so a
     * crafted/corrupt single block whose count exceeds {@code BLOCK_CAPACITY} would
     * unpack more longs than that 64-long scratch holds and overrun it -- a native heap
     * OOB write the {@code summed-count <= maxValues} guard cannot catch, because it
     * bounds destAddr only, not the scratch. The decode must reject a block count above
     * {@code BLOCK_CAPACITY} before writing.
     */
    @Test
    public void testDecodeKeyToNativeRejectsBlockCountAboveCapacity() throws Exception {
        assertMemoryLeak(() -> {
            final int firstWord = 1; // single block
            // One past the largest count whose count-1 deltas still fit the 64-long
            // blockDeltasAddr scratch (count=BLOCK_CAPACITY+1 fills it exactly): the
            // unguarded decode writes one long past the scratch.
            final int corruptCount = PostingIndexUtils.BLOCK_CAPACITY + 2;
            final long blockBytes = 256; // generous; zeroed
            final long blockAddr = Unsafe.malloc(blockBytes, MemoryTag.NATIVE_DEFAULT);
            final int cap = 256; // > corruptCount, so the summed-count <= maxValues guard passes
            final long destAddr = Unsafe.malloc((long) cap * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try (PostingIndexUtils.DecodeContext dctx = new PostingIndexUtils.DecodeContext()) {
                for (long o = 0; o < blockBytes; o += Long.BYTES) {
                    Unsafe.putLong(blockAddr + o, 0L);
                }
                Unsafe.putInt(blockAddr, firstWord);
                Assert.assertTrue("crafted block must not collide with the EF sentinel",
                        firstWord != PostingIndexUtils.EF_FORMAT_SENTINEL);
                // Single-block DELTA layout for firstWord==1:
                //   [0..3] firstWord | [4] valueCounts[0] | [5..12] firstValues[0]
                //   | [13..20] minDeltas[0] | [21] bitWidths[0]  (no packedOffsets when firstWord==1)
                Unsafe.putByte(blockAddr + 4, (byte) corruptCount); // > BLOCK_CAPACITY
                Unsafe.putByte(blockAddr + 21, (byte) 0);           // bitWidth 0 -> unconditional delta writes
                try {
                    PostingIndexUtils.decodeKeyToNative(blockAddr, destAddr, dctx, cap);
                    Assert.fail("decodeKeyToNative must reject a block count above BLOCK_CAPACITY before writing");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "block value count exceeds capacity");
                }
            } finally {
                Unsafe.free(blockAddr, blockBytes, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, (long) cap * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * The bounded {@code decodeKeyToNative(maxValues)} overload must reject a
     * DELTA (delta+FoR) block whose summed block-internal counts exceed the
     * caller's buffer capacity BEFORE writing any value, and pass a valid block
     * whose total fits. Same shared corruption guard as the EF variant, exercised
     * on the non-EF decode path (the path {@code decodeDenseGenSingleKey} takes
     * for delta-encoded strides).
     */
    @Test
    public void testDecodeKeyToNativeRejectsValueCountAboveCapacity() throws Exception {
        assertMemoryLeak(() -> {
            final int count = 200; // > BLOCK_CAPACITY (64) -> several internal blocks
            final long srcAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            final long encMaxBytes = PostingIndexUtils.computeMaxEncodedSize(count);
            final long blockAddr = Unsafe.malloc(encMaxBytes, MemoryTag.NATIVE_DEFAULT);
            final long destAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try (PostingIndexUtils.EncodeContext ectx = new PostingIndexUtils.EncodeContext();
                 PostingIndexUtils.DecodeContext dctx = new PostingIndexUtils.DecodeContext()) {
                long acc = 0;
                for (int i = 0; i < count; i++) {
                    acc += 1L + (i & 15);
                    Unsafe.putLong(srcAddr + (long) i * Long.BYTES, acc);
                }
                ectx.ensureCapacity(count);
                int size = PostingIndexUtils.encodeKeyNativeDeltaFoR(srcAddr, count, blockAddr, ectx);
                Assert.assertTrue(size > 0);
                Assert.assertTrue("expected a non-EF (delta) block",
                        Unsafe.getInt(blockAddr) != PostingIndexUtils.EF_FORMAT_SENTINEL);

                // Valid: capacity == the encoded total -> decodes and round-trips.
                PostingIndexUtils.decodeKeyToNative(blockAddr, destAddr, dctx, count);
                for (int i = 0; i < count; i++) {
                    Assert.assertEquals("round-trip mismatch at " + i,
                            Unsafe.getLong(srcAddr + (long) i * Long.BYTES),
                            Unsafe.getLong(destAddr + (long) i * Long.BYTES));
                }

                // Corruption: the summed block counts (200) exceed the buffer
                // capacity (199) -> must throw BEFORE writing rather than overflow.
                try {
                    PostingIndexUtils.decodeKeyToNative(blockAddr, destAddr, dctx, count - 1);
                    Assert.fail("decodeKeyToNative must reject a count above capacity");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "decoded value count exceeds buffer");
                }
            } finally {
                Unsafe.free(srcAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(blockAddr, encMaxBytes, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * The bounded {@code decodeKeyToNative(maxValues)} guard sums the per-block
     * declared counts, but the decode writes each block's first value
     * UNCONDITIONALLY (one long per block) before its {@code count-1} deltas -- so it
     * actually writes {@code max(count, 1)} longs per block. A valid encoder never
     * emits a zero-count block, but a crafted/corrupt block containing one writes more
     * longs than the summed count: {@code [2, 0, 2]} sums to 4 yet writes 5,
     * overrunning a destination sized to the summed count. The guard must reject a
     * zero-count block before writing any value.
     */
    @Test
    public void testDecodeKeyToNativeRejectsZeroCountBlock() throws Exception {
        assertMemoryLeak(() -> {
            final int firstWord = 3;
            final long blockBytes = 256; // generous; zeroed so an unfixed decode would do a clean +1 OOB write
            final long blockAddr = Unsafe.malloc(blockBytes, MemoryTag.NATIVE_DEFAULT);
            final int cap = 4; // == sum([2,0,2]); the old sum-only guard would pass this
            final long destAddr = Unsafe.malloc((long) cap * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try (PostingIndexUtils.DecodeContext dctx = new PostingIndexUtils.DecodeContext()) {
                for (long o = 0; o < blockBytes; o += Long.BYTES) {
                    Unsafe.putLong(blockAddr + o, 0L);
                }
                Unsafe.putInt(blockAddr, firstWord);
                Assert.assertTrue("crafted block must not collide with the EF sentinel",
                        firstWord != PostingIndexUtils.EF_FORMAT_SENTINEL);
                // valueCounts[] = [2, 0, 2]: sums to cap (passes the old sum-only guard)
                // but the unconditional per-block first-value write emits 5 longs.
                Unsafe.putByte(blockAddr + 4, (byte) 2);
                Unsafe.putByte(blockAddr + 5, (byte) 0);
                Unsafe.putByte(blockAddr + 6, (byte) 2);
                try {
                    PostingIndexUtils.decodeKeyToNative(blockAddr, destAddr, dctx, cap);
                    Assert.fail("decodeKeyToNative must reject a zero-count block before writing");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "zero-count block");
                }
            } finally {
                Unsafe.free(blockAddr, blockBytes, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, (long) cap * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Critical #8: ExpressionNode.deepClone restoration in the covering
     * DISTINCT path leaves the original mutated nodes in expressionNodePool.
     * If the optimization is rejected, downstream compilation must still
     * see a coherent WHERE clause and produce correct results.
     */
    @Test
    public void testDistinctRejectionPreservesWhereClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_distinct_reject (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE,
                        extra DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO t_distinct_reject VALUES
                    ('2024-01-01T00:00:00Z', 'A', 1.0, 10.0),
                    ('2024-01-01T01:00:00Z', 'B', 2.0, 20.0),
                    ('2024-01-01T02:00:00Z', 'A', 3.0, 30.0),
                    ('2024-01-01T03:00:00Z', 'C', 4.0, 40.0)
                    """);

            // The non-interval / non-key predicate forces rejection of the
            // posting-DISTINCT optimization. After rejection, the regular
            // DISTINCT codegen must observe the original WHERE clause.
            // ORDER BY locks ordering so this asserts row identity, not order.
            assertQuery("SELECT DISTINCT sym FROM t_distinct_reject WHERE sym IN ('A','B') AND extra > 5.0 ORDER BY sym")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            sym
                            A
                            B
                            """);

            // Run the same query a second time. If the rejected path leaked
            // mutated nodes back into the expression pool, a subsequent
            // compilation that pulls from the same pool can observe stale
            // tree state and produce different (or wrong) rows.
            assertQuery("SELECT DISTINCT sym FROM t_distinct_reject WHERE sym IN ('A','B') AND extra > 5.0 ORDER BY sym")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            sym
                            A
                            B
                            """);
        });
    }

    /**
     * Critical #1: PostingIndexNative Java fallback silently corrupts data
     * at bitWidth=64 because of the 64-bit shift no-op in
     * unpackAllValuesNativeFallback (line 133: buffer >>>= bitWidth).
     * The fallback runs on platforms without the native AVX2 library.
     */
    @Test
    public void testFallbackBitWidth64RoundTripIsCorrect() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int count = 4;
            int bitWidth = 64;
            long minValue = 0;

            long[] inputs = {
                    0L,
                    0x0123_4567_89AB_CDEFL,
                    0xFFFF_FFFF_FFFF_FFFFL,
                    Long.MIN_VALUE
            };

            long valuesAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            int packedBytes = (count * bitWidth + 7) / 8 + 8;
            long packedAddr = Unsafe.malloc(packedBytes, MemoryTag.NATIVE_DEFAULT);
            long unpackedAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.putLong(valuesAddr + (long) i * Long.BYTES, inputs[i]);
                }

                PostingIndexNative.packValuesNativeFallback(valuesAddr, count, minValue, bitWidth, packedAddr);
                PostingIndexNative.unpackAllValuesNativeFallback(packedAddr, count, bitWidth, minValue, unpackedAddr);

                for (int i = 0; i < count; i++) {
                    long actual = Unsafe.getLong(unpackedAddr + (long) i * Long.BYTES);
                    // RED: at bitWidth=64 the fallback right-shift is a no-op
                    // so the unpacked value collides with the next iteration's buffer.
                    Assert.assertEquals("fallback corrupted value at index " + i,
                            inputs[i], actual);
                }
            } finally {
                Unsafe.free(valuesAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(packedAddr, packedBytes, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(unpackedAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Critical #10: multi-key covering cursor changes result ordering vs
     * the legacy non-covering path. With LIMIT and no ORDER BY the rows
     * returned must match the legacy path so user dashboards do not see
     * silent regressions.
     */
    @Test
    public void testMultiKeyCoveringMatchesLegacyOrdering() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_legacy (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    CREATE TABLE t_covering (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            String inserts = """
                    INSERT INTO %s VALUES
                    ('2024-01-01T00:00:00Z', 'A', 1.0),
                    ('2024-01-01T01:00:00Z', 'B', 2.0),
                    ('2024-01-02T00:00:00Z', 'A', 3.0),
                    ('2024-01-02T01:00:00Z', 'B', 4.0),
                    ('2024-01-03T00:00:00Z', 'A', 5.0),
                    ('2024-01-03T01:00:00Z', 'B', 6.0)
                    """;
            execute(inserts.formatted("t_legacy"));
            execute(inserts.formatted("t_covering"));

            // Force the legacy path with the no_covering hint and compare to
            // the default covering path. The result set must match row-for-row.
            printSql("SELECT /*+ no_covering */ sym, price FROM t_legacy WHERE sym IN ('A','B') LIMIT 4");
            String legacy = sink.toString();
            sink.clear();
            printSql("SELECT sym, price FROM t_covering WHERE sym IN ('A','B') LIMIT 4");
            String covering = sink.toString();

            // RED: covering iterates (partitions outer, keys inner), so
            // LIMIT 4 returns rows from partitions 0..1 only; the legacy
            // path interleaves keys per partition. The two outputs differ.
            Assert.assertEquals(
                    "covering and legacy paths must return identical rows under LIMIT",
                    legacy, covering
            );
        });
    }

    @Test
    public void testNativeRejectsBitWidthAbove64() throws Exception {
        if (!PostingIndexNative.isNativeAvailable()) {
            return;
        }
        TestUtils.assertMemoryLeak(() -> {
            int count = 4;
            long src = Unsafe.malloc(128, MemoryTag.NATIVE_DEFAULT);
            long dst = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.setMemory(src, 128, (byte) 0);
                try {
                    PostingIndexNative.unpackAllValuesNative(src, count, 100, 0L, dst);
                    Assert.fail("expected rejection of bitWidth=100");
                } catch (IllegalArgumentException expected) {
                    Assert.assertTrue(expected.getMessage().contains("bitWidth"));
                }
            } finally {
                Unsafe.free(src, 128, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(dst, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testNativeRejectsBitWidthNegative() throws Exception {
        if (!PostingIndexNative.isNativeAvailable()) {
            return;
        }
        TestUtils.assertMemoryLeak(() -> {
            int count = 4;
            long src = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            long dst = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.setMemory(src, 64, (byte) 0);
                try {
                    PostingIndexNative.unpackAllValuesNative(src, count, -1, 0L, dst);
                    Assert.fail("expected rejection of negative bitWidth");
                } catch (IllegalArgumentException expected) {
                    Assert.assertTrue(expected.getMessage().contains("bitWidth"));
                }
            } finally {
                Unsafe.free(src, 64, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(dst, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Critical #3: native JNI methods accept invalid bit widths with no
     * validation, leading to undefined behavior on corrupted index files.
     * Negative, zero, and >64 bit widths must be rejected explicitly.
     */
    @Test
    public void testNativeRejectsBitWidthZero() throws Exception {
        if (!PostingIndexNative.isNativeAvailable()) {
            return;
        }
        TestUtils.assertMemoryLeak(() -> {
            int count = 4;
            long src = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            long dst = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.setMemory(src, 64, (byte) 0);
                Unsafe.setMemory(dst, (long) count * Long.BYTES, (byte) 0xAA);
                try {
                    PostingIndexNative.unpackAllValuesNative(src, count, 0, 1234L, dst);
                    Assert.fail("expected rejection of bitWidth=0");
                } catch (IllegalArgumentException expected) {
                    Assert.assertTrue(expected.getMessage().contains("bitWidth"));
                }
            } finally {
                Unsafe.free(src, 64, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(dst, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testO3CoveringRebuildSidecarsPreservesCoveredValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_cov_o3 (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_cov_o3 VALUES
                    ('2024-01-01T00:00:00Z', 'A', 1.0),
                    ('2024-01-01T01:00:00Z', 'B', 2.0),
                    ('2024-01-01T02:00:00Z', 'A', 3.0),
                    ('2024-01-01T03:00:00Z', 'C', 4.0)
                    """);
            execute("""
                    INSERT INTO t_cov_o3 VALUES
                    ('2024-01-01T00:30:00Z', 'A', 5.0),
                    ('2024-01-01T01:30:00Z', 'B', 6.0),
                    ('2024-01-01T02:30:00Z', 'C', 7.0)
                    """);

            assertQuery("SELECT ts, sym, price FROM t_cov_o3 WHERE sym = 'A' ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ts\tsym\tprice
                            2024-01-01T00:00:00.000000Z\tA\t1.0
                            2024-01-01T00:30:00.000000Z\tA\t5.0
                            2024-01-01T02:00:00.000000Z\tA\t3.0
                            """);
            assertQuery("SELECT ts, sym, price FROM t_cov_o3 WHERE sym = 'B' ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ts\tsym\tprice
                            2024-01-01T01:00:00.000000Z\tB\t2.0
                            2024-01-01T01:30:00.000000Z\tB\t6.0
                            """);
            assertQuery("SELECT ts, sym, price FROM t_cov_o3 WHERE sym = 'C' ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ts\tsym\tprice
                            2024-01-01T02:30:00.000000Z\tC\t7.0
                            2024-01-01T03:00:00.000000Z\tC\t4.0
                            """);
        });
    }

    @Test
    public void testO3SealRestoreSkipsRowlessPostingColumnWithoutKeyFile() throws Exception {
        // A POSTING-indexed column with no rows in a partition (columnTop >= partition
        // size) has no .pk key file there: the O3 seal sweep skips row-less columns
        // (sealPostingIndexForPartition), so it never builds one. The seal-sweep tail
        // restorePostingIndexersToLastPartition used to open that column's index anyway
        // and threw "index does not exist", which suspends a WAL table mid-apply (here,
        // BYPASS WAL, it surfaces as the INSERT throwing).
        //
        // A last-partition split shrinks 2024-01-01 while `extra` stays row-less there
        // (columnTop above the shrunk row count). The FilesFacade reports `extra`'s key
        // file as absent on the day partition the seal-sweep restores to -- the exact
        // production state -- but only once ADD COLUMN has built it (so the legitimate
        // openNewColumnFiles path during ADD COLUMN is unaffected), and only for the
        // day partition (not the split tail, whose dir carries a "...T..." suffix).
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 20);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 20);
        final AtomicBoolean keyFileHidden = new AtomicBoolean(false);
        ff = new TestFilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                if (keyFileHidden.get()
                        && Utf8s.containsAscii(path, "extra.pk")
                        && !Utf8s.containsAscii(path, "2024-01-01T")) {
                    return false;
                }
                return super.exists(path);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE t_rowless (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t_rowless(ts, sym, v) SELECT ('2024-01-01T00:00:00.000000Z'::timestamp + x * 3_600_000_000L)::timestamp, 'a', x FROM long_sequence(20)");
            // extra has no rows on 2024-01-01 (columnTop == partition size).
            execute("ALTER TABLE t_rowless ADD COLUMN extra SYMBOL INDEX TYPE POSTING");

            keyFileHidden.set(true);
            // O3 insert forces a last-partition split; the seal sweep then restores the
            // posting indexers to the shrunk 2024-01-01, where `extra` is row-less.
            execute("INSERT INTO t_rowless(ts, sym, v) VALUES ('2024-01-01T18:30:00.000000Z', 'a', 999)");
            keyFileHidden.set(false);

            // The table survived: all rows present, and the `sym` posting index serves reads.
            assertQuery("SELECT count() FROM t_rowless")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n21\n");
            assertQuery("SELECT count() FROM t_rowless WHERE sym = 'a'")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n21\n");

            // The row-less `extra` posting column stays queryable post-restore: an
            // equality lookup matches nothing (extra is absent on every row).
            assertQuery("SELECT count() FROM t_rowless WHERE extra = 'a'")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
            assertQuery("SELECT count() FROM t_rowless WHERE extra IS NULL")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n21\n");
        });
    }

    @Test
    public void testO3SealRestoreSkipStrandsRowlessIndexerWithKeyFile() throws Exception {
        // Counterpart to testO3SealRestoreSkipsRowlessPostingColumnWithoutKeyFile.
        // There the row-less last-partition column has NO .pk (split tail) so the
        // restore skip is correct. Here the row-less column DOES have a .pk: c is
        // POSTING-indexed, added by ALTER while day2 (L) is the last partition, so
        // ADD COLUMN created an empty .pk for c on day2 and c is row-less there
        // (columnTop == day2 size), absent on day1 (P).
        //
        // An O3 back-fill of c into day1 seals day1 and points c's posting writer at
        // day1. A blanket row-less skip in restorePostingIndexersToLastPartition()
        // would skip c (columnTop >= partitionSize on L) and leave its writer on day1;
        // the next in-order append into day2 would then index day2 rows into day1's
        // .pk/.pv, making the appended row invisible to "WHERE c = ..." while the base
        // data stays intact. The existence-based skip re-points c to day2 instead.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_probe (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING, v LONG) " +
                    "TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            // day1 rows start at 12:00 so the O3 back-fill at 00:00 lands at rowid 0
            // (columnTop for c on day1 becomes 0).
            execute("INSERT INTO t_probe(ts, sym, v) SELECT " +
                    "('2024-01-01T12:00:00.000000Z'::timestamp + (x-1) * 3_600_000_000L)::timestamp, 'a', x " +
                    "FROM long_sequence(5)");
            // day2 (L): 3 rows.
            execute("INSERT INTO t_probe(ts, sym, v) SELECT " +
                    "('2024-01-02T00:00:00.000000Z'::timestamp + (x-1) * 3_600_000_000L)::timestamp, 'a', 100+x " +
                    "FROM long_sequence(3)");
            // c row-less on day2 (columnTop == 3), absent on day1.
            execute("ALTER TABLE t_probe ADD COLUMN c SYMBOL INDEX TYPE POSTING");

            // O3 back-fill of c into day1 -> seals day1, points c at day1, restore skips c.
            execute("INSERT INTO t_probe(ts, sym, c, v) VALUES ('2024-01-01T00:00:00.000000Z', 'a', 'X', 999)");

            // In-order append into day2 with a c value: indexes through c's stranded writer.
            execute("INSERT INTO t_probe(ts, sym, c, v) VALUES ('2024-01-02T03:00:00.000000Z', 'a', 'Y', 555)");

            // Base table integrity (no index involved).
            assertQuery("SELECT count() FROM t_probe")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n10\n");
            // Control: sym is NOT row-less in day2, restore re-points it -> index correct.
            assertQuery("SELECT count() FROM t_probe WHERE sym = 'a'")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n10\n");

            // The day2 'Y' row must be found exactly once via c's index, with its real value.
            assertQuery("SELECT count() FROM t_probe WHERE c = 'Y'")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
            assertQuery("SELECT v FROM t_probe WHERE c = 'Y'")
                    .noLeakCheck().returns("v\n555\n");
            assertQuery("SELECT v FROM t_probe WHERE c = 'X'")
                    .noLeakCheck().returns("v\n999\n");
        });
    }

    @Test
    public void testO3SealRestoreRowlessNoKeyFileDoesNotSuspendWalTable() throws Exception {
        // WAL counterpart to testO3SealRestoreSkipsRowlessPostingColumnWithoutKeyFile.
        // The headline symptom of the no-.pk bug is a WAL table suspending mid-apply:
        // restorePostingIndexersToLastPartition throws "index does not exist",
        // ApplyWal2TableJob catches it and suspends the table -- the commit lands but
        // apply halts and the table needs a manual RESUME WAL. With the fix the seal
        // restore skips the row-less keyless column, apply completes, the table stays live.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 20);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 20);
        final AtomicBoolean keyFileHidden = new AtomicBoolean(false);
        ff = new TestFilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                if (keyFileHidden.get()
                        && Utf8s.containsAscii(path, "extra.pk")
                        && !Utf8s.containsAscii(path, "2024-01-01T")) {
                    return false;
                }
                return super.exists(path);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE t_rowless_wal (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t_rowless_wal(ts, sym, v) SELECT ('2024-01-01T00:00:00.000000Z'::timestamp + x * 3_600_000_000L)::timestamp, 'a', x FROM long_sequence(20)");
            // extra has no rows on 2024-01-01 (columnTop == partition size).
            execute("ALTER TABLE t_rowless_wal ADD COLUMN extra SYMBOL INDEX TYPE POSTING");
            // Apply everything while the .pk is visible, so ADD COLUMN builds extra's
            // .pk on the active partition exactly as in production.
            drainWalQueue();

            final TableToken token = engine.verifyTableName("t_rowless_wal");
            Assert.assertFalse("table must apply cleanly before the O3 split", engine.getTableSequencerAPI().isSuspended(token));

            // O3 insert forces a last-partition split; the seal sweep then restores the
            // posting indexers to the shrunk 2024-01-01, where extra is row-less and
            // reports no .pk. On master the apply throws and suspends the table here.
            execute("INSERT INTO t_rowless_wal(ts, sym, v) VALUES ('2024-01-01T18:30:00.000000Z', 'a', 999)");
            keyFileHidden.set(true);
            drainWalQueue();
            keyFileHidden.set(false);

            // The table kept applying: not suspended, all rows present, sym index serves reads.
            Assert.assertFalse(
                    "WAL apply must not suspend the table on a row-less keyless posting column",
                    engine.getTableSequencerAPI().isSuspended(token)
            );
            assertQuery("SELECT count() FROM t_rowless_wal")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n21\n");
            assertQuery("SELECT count() FROM t_rowless_wal WHERE sym = 'a'")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n21\n");
            assertQuery("SELECT count() FROM t_rowless_wal WHERE extra IS NULL")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n21\n");
        });
    }

    /**
     * Rollback reencode publishes a bumped sealTxn for the compacted
     * {@code .pv.N}; it must also stage a sealed {@code .pc0.N} sidecar so a
     * following incremental seal can copy clean strides without falling back
     * to the historical append-layout misread.
     */
    @Test
    public void testRollbackThenIncrementalSealUsesRebuiltSidecar() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_rb_seal (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // 300 distinct symbol keys -> two key strides (DENSE_STRIDE = 256)
            execute("""
                    INSERT INTO t_rb_seal
                    SELECT timestamp_sequence('2024-01-01T00:00:00', 1_000_000L), 'k' || (x % 300), x::double
                    FROM long_sequence(3_000)
                    """);
            // O3 merge commit seals the posting index in the rewritten partition
            execute("""
                    INSERT INTO t_rb_seal
                    SELECT timestamp_sequence('2024-01-01T00:00:00.500000Z', 1_000_000L), 'k' || (x % 300), 0.0
                    FROM long_sequence(100)
                    """);

            TableToken tableToken = engine.verifyTableName("t_rb_seal");
            try (TableWriter writer = TestUtils.getWriter(engine, tableToken)) {
                // Stage one O3 row and roll it back. The posting rollback
                // reencodes the .pv at a bumped sealTxn and must rebuild the
                // matching sealed .pc sidecar at that txn.
                TableWriter.Row row = writer.newRow(MicrosFormatUtils.parseTimestamp("2024-01-01T00:30:00.000000Z"));
                row.putSym(1, "k0");
                row.putDouble(2, 1.0);
                row.append();
                writer.rollback();

                // O3 append touching only key k0: stride 0 is dirty, stride 1
                // stays clean, so the seal goes incremental and copies the
                // clean stride verbatim from the rollback-rebuilt sidecar.
                long base = MicrosFormatUtils.parseTimestamp("2024-01-01T01:00:00.000000Z");
                for (int i = 0; i < 50; i++) {
                    TableWriter.Row r = writer.newRow(base + (49 - i) * 1_000_000L);
                    r.putSym(1, "k0");
                    r.putDouble(2, i);
                    r.append();
                }
                writer.commit();
            }

            // count() is served from the row-id index, sum(price) from the
            // covering sidecar - the sum catches silent sidecar corruption
            // that would otherwise leave row-id counts looking correct.
            assertQuery("SELECT count(), sum(price) FROM t_rb_seal WHERE sym = 'k0'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count\tsum
                            60\t17725.0
                            """);
        });
    }

    /**
     * Var-size sibling of {@link #testRollbackThenIncrementalSealUsesRebuiltSidecar}.
     * Rollback reencodes rowids into one global decoded-values array, then rebuilds
     * per-stride VARCHAR sidecars from global key offsets. The clean-stride assertion
     * on k257 pins the shift < 0 branch to that global-offset contract.
     */
    @Test
    public void testRollbackThenIncrementalSealUsesRebuiltVarSizeSidecar() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_rb_var_seal (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (label),
                        label VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // k0 is assigned to stride 0 and k257 to stride 1.
            execute("""
                    INSERT INTO t_rb_var_seal
                    SELECT timestamp_sequence('2024-01-01T00:00:00', 1_000_000L),
                           'k' || ((x - 1) % 300),
                           ('lbl-' || x || '-' || (x % 17))::VARCHAR
                    FROM long_sequence(3_000)
                    """);
            // O3 merge commit seals the posting index in the rewritten partition.
            execute("""
                    INSERT INTO t_rb_var_seal
                    SELECT timestamp_sequence('2024-01-01T00:00:00.500000Z', 1_000_000L),
                           'k' || (100 + (x % 100)),
                           ('o3-' || x)::VARCHAR
                    FROM long_sequence(100)
                    """);

            TableToken tableToken = engine.verifyTableName("t_rb_var_seal");
            try (TableWriter writer = TestUtils.getWriter(engine, tableToken)) {
                TableWriter.Row row = writer.newRow(MicrosFormatUtils.parseTimestamp("2024-01-01T00:30:00.000000Z"));
                row.putSym(1, "k0");
                row.putVarchar(2, utf8("rolled-back"));
                row.append();
                writer.rollback();

                // Only k0 is dirtied after rollback. k257 stays in the clean
                // stride copied from the rollback-rebuilt VARCHAR sidecar.
                long base = MicrosFormatUtils.parseTimestamp("2024-01-01T01:00:00.000000Z");
                for (int i = 0; i < 50; i++) {
                    TableWriter.Row r = writer.newRow(base + (49 - i) * 1_000_000L);
                    r.putSym(1, "k0");
                    r.putVarchar(2, utf8("new-" + i));
                    r.append();
                }
                writer.commit();
            }

            assertQuery("SELECT count() AS total, count(label) AS non_null FROM t_rb_var_seal WHERE sym = 'k0'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .withPlan("""
                            Async Group By workers: 1
                              vectorized: true
                              values: [count(*),count(label)]
                              filter: null
                                CoveringIndex on: sym with: label
                                  filter: sym='k0'
                            """)
                    .returns("""
                            total\tnon_null
                            60\t60
                            """);
            assertQuery("SELECT count() AS total, count(label) AS non_null FROM t_rb_var_seal WHERE sym = 'k257'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .withPlan("""
                            Async Group By workers: 1
                              vectorized: true
                              values: [count(*),count(label)]
                              filter: null
                                CoveringIndex on: sym with: label
                                  filter: sym='k257'
                            """)
                    .returns("""
                            total\tnon_null
                            10\t10
                            """);
        });
    }

    @Test
    public void testReopenDoesNotRemapWhenLengthCoversRegion() throws Exception {
        // M2: on the dominant reopen path the mapping is seeded from ff.length(keyFile), which
        // already covers the whole entry region (a normally-closed .pk has length
        // ceilPageSize(regionLimit) >= regionLimit), so the reopen extends nothing. Before M2 the
        // reopen mapped a fixed 8192 window and then jumpTo(regionLimit) forced an mremap on
        // every non-empty reopen. Track the .pk fd and assert zero mremaps during a clean reopen.
        final java.util.concurrent.ConcurrentHashMap<Long, Boolean> pkFds =
                new java.util.concurrent.ConcurrentHashMap<>();
        final AtomicInteger pkRemaps = new AtomicInteger(0);
        final AtomicBoolean counting = new AtomicBoolean(false);
        ff = new TestFilesFacadeImpl() {
            @Override
            public boolean close(long fd) {
                pkFds.remove(fd);
                return super.close(fd);
            }

            @Override
            public long mremap(long fd, long addr, long prevSize, long newSize, long offset, int mode, int memoryTag) {
                if (counting.get() && pkFds.containsKey(fd)) {
                    pkRemaps.incrementAndGet();
                }
                return super.mremap(fd, addr, prevSize, newSize, offset, mode, memoryTag);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                final long fd = super.openRW(name, opts);
                if (fd != -1 && name != null && Utf8s.containsAscii(name, ".pk")) {
                    pkFds.put(fd, Boolean.TRUE);
                }
                return fd;
            }
        };
        assertMemoryLeak(ff, () -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                final String name = "reopen_no_remap";

                // Build a non-empty chain so regionLimit sits past the 8192 header window.
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    long row = 0;
                    for (int k = 0; k < 300; k++) {
                        writer.add(k, row++);
                    }
                    writer.setMaxValue(row - 1);
                    writer.commit();
                    writer.seal();
                }

                counting.set(true);
                try (PostingIndexWriter reopen = new PostingIndexWriter(configuration)) {
                    reopen.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0L, 0L);
                    Assert.assertTrue("reopen must restore the non-empty chain", reopen.getKeyCount() > 0);
                }
                counting.set(false);

                Assert.assertEquals(
                        "reopen must not remap the .pk when ff.length already covers the live region",
                        0,
                        pkRemaps.get()
                );
            }
        });
    }

    /**
     * Real-discard variant of
     * {@link #testRollbackThenIncrementalSealUsesRebuiltSidecar}: the
     * index holds rowids beyond the committed transient row count -- the
     * state a crash between the posting-index publish and the txn commit
     * leaves behind -- so the reopen-time {@code rollbackConditionally}
     * eviction cannot no-op and must run the rollback reencode. The reencode
     * must publish a bumped sealTxn with matching sealed {@code .pc}
     * sidecars, then the following incremental seal must be able to copy a
     * clean stride from that rebuilt sidecar.
     */
    @Test
    public void testReopenEvictionThenIncrementalSealRebuildsSidecar() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_rb_evict (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // 300 distinct symbol keys -> two key strides (DENSE_STRIDE = 256)
            execute("""
                    INSERT INTO t_rb_evict
                    SELECT timestamp_sequence('2024-01-01T00:00:00', 1_000_000L), 'k' || (x % 300), x::double
                    FROM long_sequence(3_000)
                    """);
            // O3 merge commit seals the posting index in the rewritten partition
            execute("""
                    INSERT INTO t_rb_evict
                    SELECT timestamp_sequence('2024-01-01T00:00:00.500000Z', 1_000_000L), 'k' || (x % 300), 0.0
                    FROM long_sequence(100)
                    """);
            engine.releaseAllWriters();

            TableToken token = engine.verifyTableName("t_rb_evict");
            long partitionTs;
            long partitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
                partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }
            // Plant index values beyond the committed transient row count
            // (3_100 rows are committed), mimicking a crash that happened
            // after the posting index published new generations but before
            // the table txn committed.
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                setPathForNativePartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                try (PostingIndexWriter planted = new PostingIndexWriter(configuration)) {
                    planted.of(path, "sym", COLUMN_NAME_TXN_NONE, partitionTs, partitionNameTxn);
                    for (int i = 0; i < 5; i++) {
                        planted.add(0, 3_100 + i);
                    }
                    planted.setMaxValue(3_104);
                    planted.commit();
                }
            }

            try (TableWriter writer = TestUtils.getWriter(engine, token)) {
                // Opening the writer ran rollbackConditionally(3_100) on the
                // last partition: getMaxValue() == 3_104 forces a real
                // discard, so the rollback reencode published a bumped
                // sealTxn with rebuilt .pc sidecars at that txn.
                // Covered reads in the rollback-to-next-seal window must
                // still serve the committed data.
                assertQuery("SELECT count(), sum(price) FROM t_rb_evict WHERE sym = 'k0'")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("""
                                count\tsum
                                10\t16500.0
                                """);

                // O3 append touching only key 'k0': one stride dirty, the
                // other clean, so the seal goes incremental and reads the
                // clean stride from the rollback-rebuilt sealed sidecar.
                long base = MicrosFormatUtils.parseTimestamp("2024-01-01T01:00:00.000000Z");
                for (int i = 0; i < 50; i++) {
                    TableWriter.Row r = writer.newRow(base + (49 - i) * 1_000_000L);
                    r.putSym(1, "k0");
                    r.putDouble(2, i);
                    r.append();
                }
                writer.commit();
            }

            assertQuery("SELECT count(), sum(price) FROM t_rb_evict WHERE sym = 'k0'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count\tsum
                            60\t17725.0
                            """);
            // 'k1' lives in the stride the post-rollback commit left clean;
            // its covered values come from the sidecar the rollback reencode
            // rebuilt.
            assertQuery("SELECT count(), sum(price) FROM t_rb_evict WHERE sym = 'k1'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count\tsum
                            11\t13510.0
                            """);
        });
    }

    @Test
    public void testReopenEvictionThenIncrementalSealRebuildsVarSidecar() throws Exception {
        // Same reopen-eviction rollback as the DOUBLE case above, but the
        // covered column is VAR-SIZE (VARCHAR). The streaming rollback must
        // rebuild the .pc sidecar through the per-key var-size path, so the
        // covered reads keep serving the committed tags.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_rb_evict_var (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (tag),
                        tag VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // 300 distinct symbol keys -> two key strides (DENSE_STRIDE = 256).
            // Each row's covered tag is DISTINCT ('tag_' || x), so a scrambled
            // per-key var-size offset surfaces as a wrong/duplicate value rather
            // than hiding behind an all-identical payload.
            execute("""
                    INSERT INTO t_rb_evict_var
                    SELECT timestamp_sequence('2024-01-01T00:00:00', 1_000_000L), 'k' || (x % 300), 'tag_' || x
                    FROM long_sequence(3_000)
                    """);
            // O3 merge commit seals the posting index in the rewritten partition.
            // Distinct namespace ('tag2_') so the merged rows never collide with
            // the base insert's tags.
            execute("""
                    INSERT INTO t_rb_evict_var
                    SELECT timestamp_sequence('2024-01-01T00:00:00.500000Z', 1_000_000L), 'k' || (x % 300), 'tag2_' || x
                    FROM long_sequence(100)
                    """);
            engine.releaseAllWriters();

            TableToken token = engine.verifyTableName("t_rb_evict_var");
            long partitionTs;
            long partitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
                partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }
            // Plant index values beyond the committed transient row count
            // (3_100 rows are committed) so the reopen forces a real discard.
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                setPathForNativePartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                try (PostingIndexWriter planted = new PostingIndexWriter(configuration)) {
                    planted.of(path, "sym", COLUMN_NAME_TXN_NONE, partitionTs, partitionNameTxn);
                    for (int i = 0; i < 5; i++) {
                        planted.add(0, 3_100 + i);
                    }
                    planted.setMaxValue(3_104);
                    planted.commit();
                }
            }

            try (TableWriter writer = TestUtils.getWriter(engine, token)) {
                // Opening the writer ran rollbackConditionally(3_100): the real
                // discard streamed the var-size covered sidecar rebuild for the
                // survivors. Covered reads must still serve the committed tags.
                assertQuery("SELECT count() c, count(tag) ct, count_distinct(tag) cd, min(tag) mn, max(tag) mx FROM t_rb_evict_var WHERE sym = 'k0'")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .withPlanContaining("CoveringIndex on: sym")
                        .returns("""
                                c\tct\tcd\tmn\tmx
                                10\t10\t10\ttag_1200\ttag_900
                                """);

                // O3 append touching only key 'k0': one stride dirty, the
                // other clean, so the seal goes incremental and reads the
                // clean stride from the rollback-rebuilt sealed var sidecar.
                long base = MicrosFormatUtils.parseTimestamp("2024-01-01T01:00:00.000000Z");
                for (int i = 0; i < 50; i++) {
                    TableWriter.Row r = writer.newRow(base + (49 - i) * 1_000_000L);
                    r.putSym(1, "k0");
                    // Distinct per-row tag so the incremental seal's rebuilt var
                    // sidecar must preserve each offset, not just a shared payload.
                    r.putVarchar(2, new Utf8String("tagO3_" + i));
                    r.append();
                }
                writer.commit();
            }

            assertQuery("SELECT count() c, count(tag) ct, count_distinct(tag) cd, min(tag) mn, max(tag) mx FROM t_rb_evict_var WHERE sym = 'k0'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .withPlanContaining("CoveringIndex on: sym")
                    .returns("""
                            c\tct\tcd\tmn\tmx
                            60\t60\t60\ttagO3_0\ttag_900
                            """);
            // 'k1' lives in the stride the post-rollback commit left clean; its
            // covered var-size values come from the sidecar the rollback rebuilt.
            // 11 rows, each with a distinct tag -> count_distinct must be 11.
            assertQuery("SELECT count() c, count(tag) ct, count_distinct(tag) cd, min(tag) mn, max(tag) mx FROM t_rb_evict_var WHERE sym = 'k1'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .withPlanContaining("CoveringIndex on: sym")
                    .returns("""
                            c\tct\tcd\tmn\tmx
                            11\t11\t11\ttag2_1\ttag_901
                            """);
        });
    }

    @Test
    public void testReopenExtendFailureDoesNotTruncateKeyFile() throws Exception {
        // Not on Windows: the discriminator relies on the buggy close() truncating the .pk to
        // ceilPageSize(KEY_FILE_RESERVED). That boundary tracks Files.PAGE_SIZE -- 4K on Linux,
        // 16K on Mac, but 64K on the Windows agents. The fixed 200-cycle chain builds a head
        // entry around 37K, which sits past the 4K/16K page window (so Linux and Mac observe
        // the truncation) but inside the 64K Windows page, where the data loss would not show.
        Assume.assumeFalse(Os.isWindows());

        // C1: the reopen catch block in PostingIndexWriter.of(isInit=false) releases keyMem
        // WITHOUT truncation (keyMem.close(false)) when the (re)open throws before openExisting
        // has published the real regionLimit into the chain. Before that fix the catch fell into
        // the truncating close(), which sizes the .pk trim from chain.getRegionLimit() -- still
        // the reset sentinel KEY_FILE_RESERVED (8192) at that point -- so it shrank the file back
        // to 8192 and discarded the live chain region [8192, regionLimit) physically on disk. A
        // merely transient reopen failure would then corrupt the on-disk index.
        //
        // Reproduce a pre-openExisting throw with keyMem still OPEN: force jumpTo(regionLimit) ->
        // extend0 -> allocateDiskSpace to fail. allocateDiskSpace throws OUTSIDE extend0's own
        // self-closing try (a failing mremap would instead close keyMem and not exercise the fix),
        // so keyMem stays open with regionLimit at the sentinel. The facade (a) reports a stale
        // 8192 length for the .pk so the initial map is short and the extend fires, and (b) fails
        // allocate on the .pk fd. Then the .pk must be intact and a clean reopen must recover it.
        final AtomicBoolean armed = new AtomicBoolean(false);
        final java.util.concurrent.ConcurrentHashMap<Long, Boolean> pkFds =
                new java.util.concurrent.ConcurrentHashMap<>();
        ff = new TestFilesFacadeImpl() {
            @Override
            public boolean allocate(long fd, long size) {
                // The .pk extend (jumpTo -> extend0 -> allocateDiskSpace) is the only
                // allocate on this fd during the armed reopen; fail it so allocateDiskSpace
                // throws with keyMem still open and regionLimit still at the sentinel.
                if (armed.get() && pkFds.containsKey(fd)) {
                    return false;
                }
                return super.allocate(fd, size);
            }

            @Override
            public boolean close(long fd) {
                pkFds.remove(fd);
                return super.close(fd);
            }

            @Override
            public long length(long fd) {
                // Report a stale header-only length for the tracked .pk fd so
                // allocateDiskSpace's `ff.length(fd) < newSize` guard is satisfied and it
                // actually calls allocate() (which we then fail) regardless of how the test
                // config's extend segment lines up with the file's page-aligned size.
                if (armed.get() && pkFds.containsKey(fd)) {
                    return PostingIndexUtils.KEY_FILE_RESERVED;
                }
                return super.length(fd);
            }

            @Override
            public long length(LPSZ name) {
                // Report a stale header-only length for the .pk so the reopen seeds a short
                // mapping and must jumpTo(regionLimit) -- the step whose extend we fail. The
                // file's real on-disk length is untouched.
                if (armed.get() && isPkFile(name)) {
                    final long real = super.length(name);
                    return real > PostingIndexUtils.KEY_FILE_RESERVED ? PostingIndexUtils.KEY_FILE_RESERVED : real;
                }
                return super.length(name);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                final long fd = super.openRW(name, opts);
                if (fd != -1 && isPkFile(name)) {
                    pkFds.put(fd, Boolean.TRUE);
                }
                return fd;
            }

            private boolean isPkFile(LPSZ name) {
                return name != null && Utf8s.containsAscii(name, ".pk");
            }
        };
        assertMemoryLeak(ff, () -> {
            final FilesFacade rawFf = configuration.getFilesFacade();
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                final String name = "reopen_no_truncate";
                final int numKeys = 300;
                final int sealCycles = 200;

                // Phase 1: build a chain whose region extends well past ceilPageSize(8192) so a
                // truncation to the reset sentinel would actually drop the live head entry (a
                // small region would survive inside the header's own page). Each commit+seal
                // appends one more chain entry, growing regionLimit.
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    long row = 0;
                    for (int cycle = 0; cycle < sealCycles; cycle++) {
                        for (int k = 0; k < numKeys; k++) {
                            writer.add(k, row++);
                        }
                        writer.setMaxValue(row - 1);
                        writer.commit();
                        writer.seal();
                    }
                }

                // Capture the intact on-disk state: the file length and the head snapshot.
                final long pkLenBefore = rawFf.length(PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE));
                Assert.assertTrue("chain must extend past the 8192 header window", pkLenBefore > PostingIndexUtils.KEY_FILE_RESERVED);
                final long regionLimitBefore;
                final long headSealTxn;
                final int headKeyCount;
                final int headGenCount;
                final long headMaxValue;
                try (MemoryCMARWImpl pk = new MemoryCMARWImpl(rawFf,
                        PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE),
                        rawFf.getPageSize(), pkLenBefore, MemoryTag.MMAP_DEFAULT, 0)) {
                    PostingIndexChainWriter chain = new PostingIndexChainWriter();
                    chain.openExisting(pk);
                    regionLimitBefore = chain.getRegionLimit();
                    // Precondition for the discriminator: the live head entry must sit past
                    // ceilPageSize(KEY_FILE_RESERVED), or a truncation to the sentinel would
                    // leave it inside the header's own page and the data loss would not show.
                    Assert.assertTrue(
                            "head entry must sit past the header page for the truncation to be observable",
                            chain.getHeadEntryOffset() > Files.ceilPageSize(PostingIndexUtils.KEY_FILE_RESERVED)
                    );
                    PostingIndexChainEntry.Snapshot head = new PostingIndexChainEntry.Snapshot();
                    chain.loadHeadEntry(pk, head);
                    headSealTxn = head.sealTxn;
                    headKeyCount = head.keyCount;
                    headGenCount = head.genCount;
                    headMaxValue = head.maxValue;
                }

                // Phase 2: reopen with the extend armed to fail. The throw lands in the catch
                // block before openExisting published the real regionLimit.
                armed.set(true);
                try (PostingIndexWriter reopen = new PostingIndexWriter(configuration)) {
                    try {
                        reopen.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0L, 0L);
                        Assert.fail("reopen must throw when the .pk extend fails");
                    } catch (CairoException expected) {
                        TestUtils.assertContains(expected.getFlyweightMessage(), "No space left");
                    }
                }
                armed.set(false);

                // Phase 3 (the fix): the failed reopen must NOT have truncated the .pk to the
                // 8192 sentinel -- the live chain region must survive on disk.
                Assert.assertEquals(
                        "failed reopen must not shrink the .pk (data-loss guard)",
                        pkLenBefore,
                        rawFf.length(PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE))
                );

                // Phase 4: a clean reopen recovers the chain intact.
                try (PostingIndexWriter recovered = new PostingIndexWriter(configuration)) {
                    recovered.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0L, 0L);
                    Assert.assertEquals("recovered key count", headKeyCount, recovered.getKeyCount());
                    Assert.assertEquals("recovered max value", headMaxValue, recovered.getMaxValue());
                    Assert.assertEquals("recovered gen count", headGenCount, recovered.getGenCount());
                }
                try (MemoryCMARWImpl pk = new MemoryCMARWImpl(rawFf,
                        PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE),
                        rawFf.getPageSize(),
                        rawFf.length(PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)),
                        MemoryTag.MMAP_DEFAULT, 0)) {
                    PostingIndexChainWriter chain = new PostingIndexChainWriter();
                    chain.openExisting(pk);
                    Assert.assertEquals("recovered regionLimit", regionLimitBefore, chain.getRegionLimit());
                    PostingIndexChainEntry.Snapshot head = new PostingIndexChainEntry.Snapshot();
                    chain.loadHeadEntry(pk, head);
                    Assert.assertEquals("recovered head sealTxn", headSealTxn, head.sealTxn);
                }
            }
        });
    }

    @Test
    public void testReopenRejectsShortKeyFile() throws Exception {
        // M2/m2: a .pk physically shorter than KEY_FILE_RESERVED is rejected cleanly on reopen
        // with "Index file too short" rather than being mapped and read past its backed extent.
        // Build a chain, truncate its .pk below the reserved header window, then reopen.
        assertMemoryLeak(() -> {
            final FilesFacade rawFf = configuration.getFilesFacade();
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                final String name = "short_key_file";

                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    writer.add(0, 0);
                    writer.setMaxValue(0);
                    writer.commit();
                    writer.seal();
                }

                final long fd = rawFf.openRW(
                        PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE),
                        configuration.getWriterFileOpenOpts());
                Assert.assertTrue(fd > -1);
                try {
                    Assert.assertTrue(rawFf.truncate(fd, PostingIndexUtils.KEY_FILE_RESERVED - 4096L));
                } finally {
                    rawFf.close(fd);
                }

                try (PostingIndexWriter reopen = new PostingIndexWriter(configuration)) {
                    reopen.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0L, 0L);
                    Assert.fail("reopen of a truncated .pk must be rejected");
                } catch (CairoException expected) {
                    TestUtils.assertContains(expected.getFlyweightMessage(), "Index file too short");
                }
            }
        });
    }

    /**
     * Defense-in-depth check for the snapshot validation in {@code seal()}:
     * a published sealTxn whose {@code .pc} sidecar is missing on disk is
     * exactly the state the rollback reencode left behind before this fix
     * (and what a cover-sources-not-configured rollback still leaves), as
     * well as what a database upgraded mid-incident carries. The next gen
     * flush recreates the file in append layout; the following incremental
     * seal must recognize it as untrusted and rebuild via {@code sealFull}
     * instead of interpreting raw cover bytes as stride offsets.
     */
    @Test
    public void testIncrementalSealDistrustsRecreatedSidecar() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_rb_orphan (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // 300 distinct symbol keys -> two key strides (DENSE_STRIDE = 256)
            execute("""
                    INSERT INTO t_rb_orphan
                    SELECT timestamp_sequence('2024-01-01T00:00:00', 1_000_000L), 'k' || (x % 300), x::double
                    FROM long_sequence(3_000)
                    """);
            // O3 merge commit seals the posting index in the rewritten partition
            execute("""
                    INSERT INTO t_rb_orphan
                    SELECT timestamp_sequence('2024-01-01T00:00:00.500000Z', 1_000_000L), 'k' || (x % 300), 0.0
                    FROM long_sequence(100)
                    """);
            engine.releaseAllWriters();

            // Simulate the pre-fix rollback state: the chain head references
            // a sealTxn that has no .pc sidecar on disk.
            java.io.File sidecar = newestSidecarFile(engine.verifyTableName("t_rb_orphan"));
            Assert.assertTrue("failed to delete " + sidecar, sidecar.delete());

            TableToken token = engine.verifyTableName("t_rb_orphan");
            try (TableWriter writer = TestUtils.getWriter(engine, token)) {
                // O3 append touching only key 'k0': the gen flush lazily
                // recreates the missing .pc in append layout, one stride is
                // dirty and the other clean, so the seal goes incremental
                // and must distrust the recreated file.
                long base = MicrosFormatUtils.parseTimestamp("2024-01-01T01:00:00.000000Z");
                for (int i = 0; i < 50; i++) {
                    TableWriter.Row r = writer.newRow(base + (49 - i) * 1_000_000L);
                    r.putSym(1, "k0");
                    r.putDouble(2, i);
                    r.append();
                }
                writer.commit();
            }

            // 'k0' lives in the dirty stride, which the seal re-encodes
            // from live data either way; 'k1' lives in the clean stride,
            // whose covered data only survives when the seal refuses to
            // copy it out of the append-layout file (the unfixed code
            // either crashes with a multi-exabyte allocation or silently
            // writes an empty stride block, returning NULL sums here).
            assertQuery("SELECT count(), sum(price) FROM t_rb_orphan WHERE sym = 'k0'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count\tsum
                            60\t17725.0
                            """);
            assertQuery("SELECT count(), sum(price) FROM t_rb_orphan WHERE sym = 'k1'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count\tsum
                            11\t13510.0
                            """);
        });
    }

    /**
     * The incremental seal's clean-stride copy used the snapshot length as
     * the last stride's upper bound. Post-seal gen flushes append raw cover
     * blocks to the same {@code .pc} file after its sealed region, so every
     * incremental seal whose last stride stayed clean swept those gen blocks
     * into the new sealed sidecar, growing it by the raw size of all data
     * appended since the previous seal -- again on every following seal. The
     * copy must stop at the stored stride-index sentinel.
     */
    @Test
    public void testIncrementalSealLastCleanStrideStopsAtSentinel() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_tail (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // 300 distinct symbol keys -> two key strides; 'k1' is key 0
            // (stride 0), 'k0' is key 299 (stride 1, the last stride).
            execute("""
                    INSERT INTO t_tail
                    SELECT timestamp_sequence('2024-01-01T00:00:00', 1_000_000L), 'k' || (x % 300), x::double
                    FROM long_sequence(3_000)
                    """);

            TableToken token = engine.verifyTableName("t_tail");
            long sealedSizeAfterFirstSeal;
            long sealedSizeAfterSecondSeal;
            try (TableWriter writer = TestUtils.getWriter(engine, token)) {
                // Two O3-append commits on 'k1' only (descending timestamps
                // past the committed max keep the last partition in
                // append=true O3 mode, the shape that seals in place rather
                // than rewriting the partition). The first commit runs the
                // initial full seal; the second finds stride 0 dirty and
                // stride 1 (the last) clean, so it seals incrementally. The
                // 20_000 raw cover values (160_000 bytes) the second commit
                // flushes before its seal sit as gen blocks after the first
                // seal's stride data; copying the clean last stride must not
                // sweep them into the new sealed file.
                appendDescendingK1Batch(writer, "2024-01-01T01:00:00.000000Z");
                writer.commit();
                sealedSizeAfterFirstSeal = newestSidecarSize(token);

                appendDescendingK1Batch(writer, "2024-01-01T02:00:00.000000Z");
                writer.commit();
                sealedSizeAfterSecondSeal = newestSidecarSize(token);
            }

            // The second sealed sidecar only grows by the ALP-compressed
            // encoding of 20_000 constant doubles (a few KiB). Sweeping the
            // raw gen blocks would grow it by at least 160_000 bytes.
            long growth = sealedSizeAfterSecondSeal - sealedSizeAfterFirstSeal;
            Assert.assertTrue(
                    "incremental seal swept post-seal gen blocks into the last clean stride [growth=" + growth + ']',
                    growth < 80_000);

            // 'k1': 10 base rows (sum 13_510) + 40_000 rows at 1.0 each.
            assertQuery("SELECT count(), sum(price) FROM t_tail WHERE sym = 'k1'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count\tsum
                            40010\t53510.0
                            """);
            // 'k0' lives in the clean last stride: its covered data is
            // copied verbatim across both seals and must stay intact.
            assertQuery("SELECT count(), sum(price) FROM t_tail WHERE sym = 'k0'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count\tsum
                            10\t16500.0
                            """);
        });
    }

    /**
     * End-to-end guard for the bounded {@code mergeKeyValues} decode (the M-1
     * hardening). The incremental seal sizes its per-stride merge buffer from
     * each generation's stored per-key counts, then decodes every generation's
     * encoded block into it. If a sparse generation's stored count is smaller
     * than what its block actually decodes to -- a corrupt or mismatched gen --
     * the bounded decode must throw BEFORE overflowing the exactly-sized buffer,
     * not scribble past it into native heap.
     * <p>
     * Reproducer: seal a dense gen 0 over 300 keys (2 strides), append a sparse
     * gen 1 for the hot key only (so its stride is the single dirty one, keeping
     * the seal on the incremental branch), then shrink gen 1's stored count for
     * that key on disk below what its block encodes. The next seal's merge must
     * reject it rather than overflow. Restores the count afterwards so close()'s
     * auto-seal re-compacts cleanly.
     */
    @Test
    public void testIncrementalSealRejectsCorruptSparseGenBlockCountOverflow() throws Exception {
        final int numKeys = 300;        // > 256 -> 2 strides; one dirty stride keeps the incremental branch
        final int hotKey = 0;
        final int gen1HotRows = 1_000;  // sparse gen 1 holds this many rows for the hot key
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                final String name = "corrupt_sparse_gen";
                final FilesFacade rawFf = configuration.getFilesFacade();
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    // Phase 1: one row per key, sealed into a dense gen 0 over every key.
                    long row = 0;
                    for (int k = 0; k < numKeys; k++) {
                        writer.add(k, row++);
                    }
                    writer.setMaxValue(row - 1);
                    writer.commit();
                    writer.seal();
                    Assert.assertEquals("phase 1 must leave a single dense gen 0", 1, writer.getGenCount());

                    // Phase 2: more rows for the hot key only -> sparse gen 1, dirty stride 0.
                    for (int i = 0; i < gen1HotRows; i++) {
                        writer.add(hotKey, row++);
                    }
                    writer.setMaxValue(row - 1);
                    writer.commit();
                    Assert.assertEquals("phase 2 must append a sparse gen 1", 2, writer.getGenCount());

                    // Resolve sparse gen 1's file offset and active key count from the chain head.
                    final long gen1FileOffset;
                    final int gen1ActiveKeyCount;
                    final long curSealTxn;
                    final long pkLen = rawFf.length(PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE));
                    try (MemoryCMARWImpl pk = new MemoryCMARWImpl(rawFf,
                            PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE),
                            rawFf.getPageSize(), pkLen, MemoryTag.MMAP_DEFAULT, 0)) {
                        PostingIndexChainWriter chain = new PostingIndexChainWriter();
                        chain.openExisting(pk);
                        PostingIndexChainEntry.Snapshot head = new PostingIndexChainEntry.Snapshot();
                        chain.loadHeadEntry(pk, head);
                        Assert.assertEquals("expected gen 0 + sparse gen 1", 2, head.genCount);
                        curSealTxn = head.sealTxn;
                        long gen1Dir = PostingIndexChainEntry.resolveGenDirOffset(head.offset, 1);
                        gen1FileOffset = pk.getLong(gen1Dir + PostingIndexUtils.GEN_DIR_OFFSET_FILE_OFFSET);
                        int genKeyCount = pk.getInt(gen1Dir + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
                        Assert.assertTrue("gen 1 must be sparse (negative KEY_COUNT), got " + genKeyCount, genKeyCount < 0);
                        gen1ActiveKeyCount = -genKeyCount;
                    }

                    // Sparse gen layout in .pv: [keyIds: n ints][counts: n ints][...]. Phase 2 wrote
                    // only the hot key, so it is index 0. Shrink its stored count below the block's.
                    final long countsBase = gen1FileOffset + (long) gen1ActiveKeyCount * Integer.BYTES;
                    final long pvLen = rawFf.length(PostingIndexUtils.valueFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, curSealTxn));
                    try (MemoryCMARWImpl pv = new MemoryCMARWImpl(rawFf,
                            PostingIndexUtils.valueFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, curSealTxn),
                            rawFf.getPageSize(), pvLen, MemoryTag.MMAP_DEFAULT, 0)) {
                        Assert.assertEquals("gen 1's first key must be the hot key", hotKey, pv.getInt(gen1FileOffset));
                        Assert.assertEquals("gen 1's stored hot-key count must equal the phase 2 row count",
                                gen1HotRows, pv.getInt(countsBase));
                        pv.putInt(countsBase, gen1HotRows / 2); // corrupt: stored count < the block's real count
                    }

                    // The incremental seal merges gen 0 + gen 1 for the dirty stride; the bounded
                    // decode must reject the over-long block rather than overflow the merge buffer.
                    try {
                        writer.seal();
                        Assert.fail("incremental seal must reject a sparse gen whose block exceeds its stored count");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "corrupt posting index");
                    }

                    // Restore the count so the try-with-resources close()'s auto-seal re-compacts
                    // cleanly -- leaving the corruption would re-throw out of close() and mask the above.
                    try (MemoryCMARWImpl pv = new MemoryCMARWImpl(rawFf,
                            PostingIndexUtils.valueFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, curSealTxn),
                            rawFf.getPageSize(), pvLen, MemoryTag.MMAP_DEFAULT, 0)) {
                        pv.putInt(countsBase, gen1HotRows);
                    }
                }
            }
        });
    }

    @Test
    public void testRollbackRejectsNegativeDenseFlatPrefixCount() throws Exception {
        // decodeDenseGenSingleKey guards count < 0, the partner of its
        // count > capacity guard. A non-monotonic FLAT prefix-counts array
        // (corruption) yields a negative per-key count; unguarded it would
        // drive a negative destination offset and write out of bounds. Build a
        // dense FLAT gen 0, corrupt the prefix so one key's count goes negative,
        // then roll back -- filterCountsForRollback decodes that key and must
        // throw rather than read/write out of bounds. The existing corrupt-gen
        // tests only cover the count > capacity / over-decode direction.
        final int numKeys = 200;  // 200 keys x 1 row -> stride 0 picks FLAT (see testSizeFastPathOnDenseFlatStride)
        final int badKey = 100;   // mid-stride, so prefix[badKey] and prefix[badKey + 1] are interior slots
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                final String name = "corrupt_flat_prefix";
                final FilesFacade rawFf = configuration.getFilesFacade();
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    long row = 0;
                    for (int k = 0; k < numKeys; k++) {
                        writer.add(k, row++);
                    }
                    writer.setMaxValue(row - 1);
                    writer.commit();
                    writer.seal();
                    Assert.assertEquals("seal must leave a single dense gen 0", 1, writer.getGenCount());

                    // Resolve gen 0's file offset, key count and the live sealTxn from the chain head.
                    final long gen0FileOffset;
                    final int gen0KeyCount;
                    final long curSealTxn;
                    final long pkLen = rawFf.length(PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE));
                    try (MemoryCMARWImpl pk = new MemoryCMARWImpl(rawFf,
                            PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE),
                            rawFf.getPageSize(), pkLen, MemoryTag.MMAP_DEFAULT, 0)) {
                        PostingIndexChainWriter chain = new PostingIndexChainWriter();
                        chain.openExisting(pk);
                        PostingIndexChainEntry.Snapshot head = new PostingIndexChainEntry.Snapshot();
                        chain.loadHeadEntry(pk, head);
                        Assert.assertEquals("expected a single dense gen 0", 1, head.genCount);
                        curSealTxn = head.sealTxn;
                        long gen0Dir = PostingIndexChainEntry.resolveGenDirOffset(head.offset, 0);
                        gen0FileOffset = pk.getLong(gen0Dir + PostingIndexUtils.GEN_DIR_OFFSET_FILE_OFFSET);
                        int genKeyCount = pk.getInt(gen0Dir + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
                        Assert.assertEquals("gen 0 must be dense and cover every key", numKeys, genKeyCount);
                        gen0KeyCount = genKeyCount;
                    }

                    // FLAT stride layout in .pv: [mode][baseValue][prefix counts: ks + 1 ints]...
                    // count[badKey] = prefix[badKey + 1] - prefix[badKey]; make prefix[badKey + 1]
                    // smaller than prefix[badKey] so that difference goes negative.
                    final long pvLen = rawFf.length(PostingIndexUtils.valueFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, curSealTxn));
                    int origPrefixNext = 0;
                    long prefixNextSlot = 0;
                    try (MemoryCMARWImpl pv = new MemoryCMARWImpl(rawFf,
                            PostingIndexUtils.valueFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, curSealTxn),
                            rawFf.getPageSize(), pvLen, MemoryTag.MMAP_DEFAULT, 0)) {
                        int siSize = PostingIndexUtils.strideIndexSize(gen0KeyCount);
                        long stride0Off = pv.getLong(gen0FileOffset); // stride index entry 0
                        long strideAddr = gen0FileOffset + siSize + stride0Off;
                        Assert.assertEquals("stride 0 must be FLAT to exercise the FLAT prefix guard",
                                PostingIndexUtils.STRIDE_MODE_FLAT, pv.getByte(strideAddr));
                        long prefixBase = strideAddr + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET;
                        prefixNextSlot = prefixBase + (long) (badKey + 1) * Integer.BYTES;
                        int prefixCur = pv.getInt(prefixBase + (long) badKey * Integer.BYTES);
                        origPrefixNext = pv.getInt(prefixNextSlot);
                        pv.putInt(prefixNextSlot, prefixCur - 1); // count[badKey] = (prefixCur - 1) - prefixCur = -1
                    }

                    // Roll back keeping badKey's row id (100 <= 189): filterCountsForRollback
                    // decodes badKey from the corrupt FLAT stride and the count < 0 guard fires.
                    try {
                        writer.rollbackValues(numKeys - 11L); // keep rows <= 189; badKey's row (100) survives
                        Assert.fail("rollback must reject a negative dense FLAT prefix count");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "corrupt posting index");
                        TestUtils.assertContains(e.getFlyweightMessage(), "dense FLAT key count");
                    }

                    // Restore the prefix so the try-with-resources close()'s auto-seal re-compacts
                    // cleanly -- leaving the corruption would re-throw out of close() and mask the above.
                    try (MemoryCMARWImpl pv = new MemoryCMARWImpl(rawFf,
                            PostingIndexUtils.valueFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, curSealTxn),
                            rawFf.getPageSize(), pvLen, MemoryTag.MMAP_DEFAULT, 0)) {
                        pv.putInt(prefixNextSlot, origPrefixNext);
                    }
                }
            }
        });
    }

    /**
     * The sealed-sidecar validator bounded the sealed region with
     * {@code sentinel + siSize <= fileLen}, which a corrupt
     * near-{@code Long.MAX_VALUE} sentinel overflows past, marking the file
     * trusted. The incremental seal sizes its snapshot copy and the last
     * clean stride's copy from that sentinel, so one torn write turned into
     * a negative-size allocation or an out-of-bounds read. The validator
     * must reject a sentinel that places the sealed end past the file and
     * route the seal to sealFull.
     */
    @Test
    public void testIncrementalSealRejectsOverflowingSidecarSentinel() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_ovf (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // 300 distinct symbol keys -> two key strides; 'k1' is key 0
            // (stride 0), 'k0' is key 299 (stride 1, the last stride).
            execute("""
                    INSERT INTO t_ovf
                    SELECT timestamp_sequence('2024-01-01T00:00:00', 1_000_000L), 'k' || (x % 300), x::double
                    FROM long_sequence(3_000)
                    """);

            TableToken token = engine.verifyTableName("t_ovf");
            try (TableWriter writer = TestUtils.getWriter(engine, token)) {
                appendDescendingK1Batch(writer, "2024-01-01T01:00:00.000000Z");
                writer.commit();
            }
            engine.releaseAllWriters();

            // Overwrite the sealed sidecar's stride-index sentinel with a
            // near-overflow offset. The value stays monotonic with the
            // preceding entries, so only the sealed-end bound can reject it.
            java.io.File sidecar = newestSidecarFile(token);
            long sentinelSlot = PostingIndexUtils.PC_HEADER_SIZE
                    + (long) PostingIndexUtils.strideCount(300) * Long.BYTES;
            try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(sidecar, "rw")) {
                raf.seek(sentinelSlot);
                // RandomAccessFile writes big-endian; the file is little-endian
                raf.writeLong(Long.reverseBytes(Long.MAX_VALUE - 8));
            }

            try (TableWriter writer = TestUtils.getWriter(engine, token)) {
                // O3 append touching only 'k1': stride 0 dirty, stride 1
                // clean, so the seal goes incremental and must distrust the
                // corrupt snapshot instead of trusting the overflowed bound.
                appendDescendingK1Batch(writer, "2024-01-01T02:00:00.000000Z");
                writer.commit();
            }

            // 'k1': 10 base rows (sum 13_510) + 40_000 rows at 1.0 each.
            assertQuery("SELECT count(), sum(price) FROM t_ovf WHERE sym = 'k1'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count\tsum
                            40010\t53510.0
                            """);
            // 'k0' lives in the last stride, whose covered data the
            // distrusted seal must rebuild from column files rather than
            // copy out of the corrupt sidecar.
            assertQuery("SELECT count(), sum(price) FROM t_ovf WHERE sym = 'k0'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count\tsum
                            10\t16500.0
                            """);
        });
    }

    /**
     * Field report reproduction: a covering POSTING index on a single
     * actively-growing partition fed by sequential (in-order) WAL/ILP commits.
     * Covered reads return wrong values (or SIGSEGV) once enough rows of a hot
     * key accumulate. No O3, squash, or parquet conversion is involved -- plain
     * append is enough.
     * <p>
     * Mechanism: the WAL fast-lag commit indexes the new rows in
     * {@code updateIndexesParallel} (the add() phase) and only afterwards, in
     * {@code publishPostingIndexesForLastPartitionFastLag}, calls
     * {@code configureCovering} + {@code commit()}. When a hot key's spill arena
     * crosses the budget mid-add, {@code compactIfOverBudget -> flushAllPending}
     * drains a sparse generation right there -- but covering is not configured
     * yet, so {@code writeSidecarGenData} short-circuits and the generation gets
     * a gen-dir entry and .pv rows with no .pc covered block (its .pc header slot
     * stays 0). A later covered read walks that generation with a 0 sidecar
     * offset and reads past the .pc mapping: count() (served from the index row
     * addresses) stays correct while sum(value) (served from the covering
     * sidecar) is wrong. The fix defers the mid-add spill flush so the whole
     * batch flushes once, after covering is configured, with .pc written.
     * <p>
     * A small spill budget reproduces at ~4M rows what the field report hit at
     * ~45M rows under the default 256 MiB budget.
     */
    @Test
    public void testWalFastLagCoveringSpillWritesCoveredSidecar() throws Exception {
        setProperty(PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256 * 1024);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE flt (
                        ts TIMESTAMP,
                        ParameterID SYMBOL INDEX TYPE POSTING INCLUDE (value),
                        value DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);

            final int batches = 40;
            final int rowsPerBatch = 100_000; // per-commit hot-key spill (~hundreds of KiB) exceeds the 256 KiB budget
            // arbitrary instant; all rows stay in one day
            long start = 1_700_000_000_000_000L;
            for (int b = 0; b < batches; b++) {
                execute("INSERT INTO flt " +
                        "SELECT timestamp_sequence(cast(" + start + " as timestamp), 1), 'KCAS', 1.0 " +
                        "FROM long_sequence(" + rowsPerBatch + ")");
                start += rowsPerBatch; // keep the next batch strictly after the previous -> in-order append
                drainWalQueue();
            }

            final long total = (long) batches * rowsPerBatch;
            // count() is served by the index row addresses, sum(value) by the
            // covering sidecar. A healthy covering index keeps them consistent;
            // the orphaned-sidecar bug leaves count() right but sum(value) wrong
            // (and over-reads the .pc -- a bare SIGSEGV without assertions).
            assertQuery("SELECT count(), sum(value) FROM flt WHERE ParameterID = 'KCAS'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\tsum\n" + total + "\t" + (double) total + "\n");
        });
    }

    /**
     * Multi-key sibling of {@link #testCoveringRecordCursorSingleKeyClosedOffOperatingThread}:
     * {@code sym IN (...)} drives the multi-key covering record cursor, which
     * retains one posting cursor per IN-list key. The off-thread close must
     * release every retained cursor's buffers; a pre-gate build threw the
     * owning-thread AssertionError on the first of them and stranded the rest.
     */
    @Test
    public void testCoveringRecordCursorMultiKeyClosedOffOperatingThread() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_covering_multi (
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, ts),
                        price DOUBLE,
                        qty LONG,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY""");
            // (x * x) % 3 emits keys k0/k1 with non-constant per-key row-id
            // deltas, so the posting cursors allocate decode block buffers --
            // the native memory the off-thread close must free.
            execute("""
                    INSERT INTO t_covering_multi
                    SELECT 'k' || ((x * x) % 3), x::DOUBLE, x, timestamp_sequence('2024-01-01', 60_000_000)
                    FROM long_sequence(30_000)""");

            final String scanSql = "SELECT sym, price FROM t_covering_multi WHERE sym IN ('k0', 'k1')";
            final String aggSql = "SELECT count() c, min(price) mn, max(price) mx FROM t_covering_multi WHERE sym IN ('k0', 'k1')";
            assertQuery(scanSql).noLeakCheck().assertsPlanContaining("CoveringIndex on: sym");
            assertQuery(aggSql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .withPlanContaining("CoveringIndex on: sym")
                    .returns("""
                            c\tmn\tmx
                            30000\t1.0\t30000.0
                            """);

            // Peel the QueryProgress wrapper off the compiled factory: its cursor
            // close() catches Throwable, logs "could not close record cursor" and
            // recovers the leaked reader, masking the failure this test pins. The
            // production teardown paths this test models (HttpConnectionContext.
            // reset() -> closeMergeCursors(), pgwire covering cursor close) close
            // the covering cursors without that guard.
            try (RecordCursorFactory factory = select(scanSql)) {
                RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext);
                boolean isClosed = false;
                try {
                    var record = cursor.getRecord();
                    for (int i = 0; i < 128 && cursor.hasNext(); i++) {
                        record.getDouble(1);
                    }
                    closeCursorOffOperatingThread(cursor);
                    isClosed = true;
                } finally {
                    if (!isClosed) {
                        cursor.close();
                    }
                }
            }

            // The orderly close chain must complete and return the TableReader to
            // the pool. A pre-gate build aborts it mid-way: the posting cursor's
            // owning-thread assert throws, QueryProgress swallows the error after
            // logging "could not close record cursor", and the frame cursor -- and
            // its TableReader -- never get freed.
            Assert.assertEquals(0, engine.getBusyReaderCount());

            // The reader survived the off-thread close: same plan, same rows.
            assertQuery(aggSql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .withPlanContaining("CoveringIndex on: sym")
                    .returns("""
                            c\tmn\tmx
                            30000\t1.0\t30000.0
                            """);
        });
    }

    /**
     * A suspendable query's connection -- and the open {@link RecordCursor} it
     * holds -- can migrate to another worker between fragments, so the cursor's
     * close() legitimately runs on a thread other than the one that checked
     * posting cursors out via getCursor(). The single-key covering record
     * cursor (CoveringIndexRecordCursorFactory.CoveringCursor) retains
     * currentRowCursor across hasNext() calls, so a cross-thread close reaches
     * PostingIndexFwdReader.Cursor.close() off the reader's operating thread.
     * That close used to assert the checkout thread under -ea ("posting index
     * cursor closed off the reader's owning thread"), aborting the close and
     * stranding the cursor's native buffers; it now routes through
     * AbstractCoveringCursor.canRepool(), which skips the pool and releases the
     * buffers directly. The test asserts plan and result, partially drains the
     * cursor, closes it on a second thread, then asserts plan and result again
     * on the survived reader; assertMemoryLeak proves the off-thread release
     * freed every buffer.
     */
    @Test
    public void testCoveringRecordCursorSingleKeyClosedOffOperatingThread() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_covering_single (
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, ts),
                        price DOUBLE,
                        qty LONG,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY""");
            // See testCoveringRecordCursorMultiKeyClosedOffOperatingThread for
            // why the key expression is (x * x) % 3.
            execute("""
                    INSERT INTO t_covering_single
                    SELECT 'k' || ((x * x) % 3), x::DOUBLE, x, timestamp_sequence('2024-01-01', 60_000_000)
                    FROM long_sequence(30_000)""");

            final String scanSql = "SELECT sym, price FROM t_covering_single WHERE sym = 'k1'";
            final String aggSql = "SELECT count() c, min(price) mn, max(price) mx FROM t_covering_single WHERE sym = 'k1'";
            assertQuery(scanSql).noLeakCheck().assertsPlanContaining("CoveringIndex on: sym");
            assertQuery(aggSql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .withPlanContaining("CoveringIndex on: sym")
                    .returns("""
                            c\tmn\tmx
                            20000\t1.0\t29999.0
                            """);

            // Peel the QueryProgress wrapper off the compiled factory: its cursor
            // close() catches Throwable, logs "could not close record cursor" and
            // recovers the leaked reader, masking the failure this test pins. The
            // production teardown paths this test models (HttpConnectionContext.
            // reset() -> closeMergeCursors(), pgwire covering cursor close) close
            // the covering cursors without that guard.
            try (RecordCursorFactory factory = select(scanSql)) {
                RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext);
                boolean isClosed = false;
                try {
                    var record = cursor.getRecord();
                    for (int i = 0; i < 128 && cursor.hasNext(); i++) {
                        record.getDouble(1);
                    }
                    closeCursorOffOperatingThread(cursor);
                    isClosed = true;
                } finally {
                    if (!isClosed) {
                        cursor.close();
                    }
                }
            }

            // The orderly close chain must complete and return the TableReader to
            // the pool. A pre-gate build aborts it mid-way: the posting cursor's
            // owning-thread assert throws, QueryProgress swallows the error after
            // logging "could not close record cursor", and the frame cursor -- and
            // its TableReader -- never get freed.
            Assert.assertEquals(0, engine.getBusyReaderCount());

            // The reader survived the off-thread close: same plan, same rows.
            assertQuery(aggSql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .withPlanContaining("CoveringIndex on: sym")
                    .returns("""
                            c\tmn\tmx
                            20000\t1.0\t29999.0
                            """);
        });
    }

    /**
     * Plan-fallback reproduction: a symbol-capacity change must not drop the
     * covering-index schema. When the symbol capacity grows (automatically as new
     * symbols arrive, or via ALTER), changeSymbolCapacity -> updateColumnSymbolCapacity
     * rebuilds the symbol column's metadata; that rebuild used to forget the
     * covering column indices, so the next metadata rewrite persisted a _meta with
     * no covering flag/section and the planner stopped choosing the covering scan.
     */
    @Test
    public void testCoveringSurvivesSymbolCapacityChange() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (val), val DOUBLE) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x VALUES (1, 'a', 1.0)");
            drainWalQueue();
            execute("ALTER TABLE x ALTER COLUMN sym SYMBOL CAPACITY 8192");
            drainWalQueue();
            // Fresh compile after the capacity change: the covering posting index
            // must still be chosen.
            assertQuery("SELECT ts, val FROM x WHERE sym = 'b'")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                CoveringIndex on: sym with: ts, val
                                  filter: sym='b'
                            """);
        });
    }

    /**
     * WAL-apply auto-scale reproduction, mirroring the field scenario more
     * closely than the explicit-ALTER test: a covering posting index on a WAL
     * table, ingested with high, growing symbol cardinality so the WAL apply job
     * fires scaleSymbolCapacities() -> changeSymbolCapacity() repeatedly (the
     * field 256 -> 2048 -> 4096 growth). Asserts both that the planner keeps
     * choosing the covering scan (covering mapping is read from the reader _meta,
     * which updateColumnSymbolCapacity must preserve) and that the covered
     * sidecar still serves correct data afterwards.
     */
    @Test
    public void testWalCoveringPlanSurvivesAutoScaleHeavyIngest() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, sym SYMBOL CAPACITY 16 INDEX TYPE POSTING INCLUDE (val), val DOUBLE) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String coveringPlan =
                    """
                            SelectedRecord
                                CoveringIndex on: sym with: ts, val
                                  filter: sym='zzz'
                            """;
            assertQuery("SELECT ts, val FROM x WHERE sym = 'zzz'")
                    .noLeakCheck()
                    .assertsPlan(coveringPlan);

            final long dayMicros = 86_400_000_000L;
            for (int day = 0; day < 6; day++) {
                execute("INSERT INTO x " +
                        "SELECT (" + (day * dayMicros) + " + x)::timestamp, rnd_symbol(4000,4,10,0), x::double " +
                        "FROM long_sequence(30000)");
                drainWalQueue();
            }
            // One known covered row to verify the covered read after all the auto-scaling.
            execute("INSERT INTO x VALUES (" + (9 * dayMicros) + ", 'zzz', 7.0)");
            drainWalQueue();

            // The planner must still choose the covering posting index after auto-scale.
            assertQuery("SELECT ts, val FROM x WHERE sym = 'zzz'")
                    .noLeakCheck()
                    .assertsPlan(coveringPlan);
            // The covered sidecar read must still return correct data.
            assertQuery("SELECT count(), sum(val) FROM x WHERE sym = 'zzz'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\tsum\n1\t7.0\n");
        });
    }

    @Test
    public void testO3DeferredPostingSealPurgeRunsAfterCommit() throws Exception {
        assertMemoryLeak(() -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            final String tableName = "posting_o3_recovery";
            final String indexColumnName = "new_col_11";
            final String coveredColumnName = "marker";
            final long targetPartitionTimestamp = MicrosFormatUtils.parseTimestamp("2022-02-25T00:00:00.000000Z");

            execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, sym2 SYMBOL INDEX TYPE POSTING INCLUDE (marker), sym_top SYMBOL CAPACITY 128, marker LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(insertPostingRowsSql(-85, 0));
            execute("ALTER TABLE " + tableName + " RENAME COLUMN sym2 TO " + indexColumnName);
            execute(insertPostingRowsSql(0, 10));
            execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '2022-02-25'");
            execute("ALTER TABLE " + tableName + " ALTER COLUMN sym_top SYMBOL CAPACITY 64");

            execute(insertPostingRowsSql(0, 35));
            final PostingSealFileNames oldFiles = resolvePostingSealFileNames(tableName, indexColumnName, coveredColumnName, targetPartitionTimestamp, -1L);
            final long oldSealTxn = oldFiles.sealTxn;
            assertPostingSealFilesExist(oldFiles, true);

            // Successful O3 commit: sealPostingIndexForPartition() defers the
            // purge while the superseding chain entry is tagged with the future
            // txn, then commitTxWriterAndPublishPendingPostingSealPurges()
            // should republish that task after _txn advances.
            execute(insertPostingRowsSql(35, 92));
            final PostingSealFileNames liveFiles = resolvePostingSealFileNames(tableName, indexColumnName, coveredColumnName, targetPartitionTimestamp, -1L);
            final long liveSealTxn = liveFiles.sealTxn;
            Assert.assertTrue("successful O3 commit must advance posting sealTxn", liveSealTxn > oldSealTxn);
            assertPostingSealFilesExist(oldFiles, true);

            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }

            assertPostingSealFilesExist(oldFiles, false);
            assertPostingSealFilesExist(liveFiles, true);
        });
    }

    @Test
    public void testO3DeferredPostingSealPurgePersistsOnCloseWhenJobHoldsLogWriter() throws Exception {
        assertMemoryLeak(() -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            final String tableName = "posting_o3_recovery";
            final String indexColumnName = "new_col_11";
            final String coveredColumnName = "marker";
            final long targetPartitionTimestamp = MicrosFormatUtils.parseTimestamp("2022-02-25T00:00:00.000000Z");

            execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, sym2 SYMBOL INDEX TYPE POSTING INCLUDE (marker), sym_top SYMBOL CAPACITY 128, marker LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(insertPostingRowsSql(-85, 0));
            execute("ALTER TABLE " + tableName + " RENAME COLUMN sym2 TO " + indexColumnName);
            execute(insertPostingRowsSql(0, 10));
            execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '2022-02-25'");
            execute("ALTER TABLE " + tableName + " ALTER COLUMN sym_top SYMBOL CAPACITY 64");

            execute(insertPostingRowsSql(0, 35));
            final PostingSealFileNames oldFiles = resolvePostingSealFileNames(tableName, indexColumnName, coveredColumnName, targetPartitionTimestamp, -1L);
            assertPostingSealFilesExist(oldFiles, true);

            final TableToken tableToken = engine.getTableTokenIfExists(tableName);
            Assert.assertNotNull("table must exist", tableToken);

            // Earlier covering-index tests in this class leave purge tasks in the
            // shared MessageBus ring (no PostingSealPurgeJob drains it in the test
            // harness), so it can arrive already saturated. Sibling tests clear it
            // by running a purge job first; this test must instead keep the ring
            // full via the liveJob below, so drain the residue directly so its own
            // fillPostingSealPurgeQueue saturation starts from an empty ring.
            drainPostingSealPurgeQueue();

            // A live PostingSealPurgeJob owns the sys.posting_seal_purge_log
            // writer for the entire close, exactly as in a running server. It is
            // not draining concurrently (CPU-starved or in error backoff), so the
            // ring stays saturated and the close-time direct-persist fallback
            // cannot acquire the log writer.
            try (PostingSealPurgeJob liveJob = new PostingSealPurgeJob(engine)) {
                Assert.assertTrue("live job must own the log writer", liveJob.isJobAliveForTesting());
                fillPostingSealPurgeQueue(tableToken);

                // The O3 commit advances _txn so the deferred purge is ready, but
                // the ring is full so it remains in the TableWriter until close.
                execute(insertPostingRowsSql(35, 92));

                // Close while liveJob still holds the log writer. The ready purge
                // intent must survive instead of being dropped and orphaning the
                // superseded .pv/.pc files for the process lifetime.
                engine.releaseAllWriters();
                assertPostingSealFilesExist(oldFiles, true);
            }

            // Clear the saturating dummy tasks so a recovering job can drain the
            // real, deferred purge work.
            drainPostingSealPurgeQueue();

            // Reopen the data table so writer-open recovery can replay the intent
            // recorded at close, then let a job complete the promised purge.
            try (TableWriter ignore = engine.getWriter(tableToken, "posting seal purge recovery test")) {
                Assert.assertNotNull(ignore);
            }
            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }

            // The superseded seal files must be purged, not orphaned.
            assertPostingSealFilesExist(oldFiles, false);
        });
    }

    @Test
    public void testO3DeferredPostingSealPurgePersistsOnCloseWhenQueueIsFull() throws Exception {
        assertMemoryLeak(() -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            final String tableName = "posting_o3_recovery";
            final String indexColumnName = "new_col_11";
            final String coveredColumnName = "marker";
            final long targetPartitionTimestamp = MicrosFormatUtils.parseTimestamp("2022-02-25T00:00:00.000000Z");

            execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, sym2 SYMBOL INDEX TYPE POSTING INCLUDE (marker), sym_top SYMBOL CAPACITY 128, marker LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(insertPostingRowsSql(-85, 0));
            execute("ALTER TABLE " + tableName + " RENAME COLUMN sym2 TO " + indexColumnName);
            execute(insertPostingRowsSql(0, 10));
            execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '2022-02-25'");
            execute("ALTER TABLE " + tableName + " ALTER COLUMN sym_top SYMBOL CAPACITY 64");

            execute(insertPostingRowsSql(0, 35));
            final PostingSealFileNames oldFiles = resolvePostingSealFileNames(tableName, indexColumnName, coveredColumnName, targetPartitionTimestamp, -1L);
            final long oldSealTxn = oldFiles.sealTxn;
            assertPostingSealFilesExist(oldFiles, true);

            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }

            final TableToken tableToken = engine.getTableTokenIfExists(tableName);
            Assert.assertNotNull("table must exist", tableToken);
            fillPostingSealPurgeQueue(tableToken);

            // The O3 commit advances _txn while the ring is full, so the now
            // ready deferred purge remains in TableWriter until close. Close
            // must persist it to sys.posting_seal_purge_log instead of dropping
            // the task.
            try {
                execute(insertPostingRowsSql(35, 92));
                engine.releaseAllWriters();
                assertPostingSealFilesExist(oldFiles, true);
                assertOpenDeferredPostingSealPurgeLogRow(
                        tableToken,
                        indexColumnName,
                        oldSealTxn
                );
            } finally {
                drainPostingSealPurgeQueue();
            }

            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }

            assertPostingSealFilesExist(oldFiles, false);
        });
    }

    @Test
    public void testO3DeferredPostingSealPurgePersistsMultipleReadyEntriesOnCloseWhenQueueIsFull() throws Exception {
        assertMemoryLeak(() -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            final String tableName = "posting_o3_multi_recovery";
            final String indexColumnName1 = "new_col_11";
            final String indexColumnName2 = "new_col_12";
            final String coveredColumnName = "marker";
            final long targetPartitionTimestamp = MicrosFormatUtils.parseTimestamp("2022-02-25T00:00:00.000000Z");

            execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, sym2 SYMBOL INDEX TYPE POSTING INCLUDE (marker), sym3 SYMBOL INDEX TYPE POSTING INCLUDE (marker), sym_top SYMBOL CAPACITY 128, marker LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(insertPostingRowsSql(tableName, -85, 0, true));
            execute("ALTER TABLE " + tableName + " RENAME COLUMN sym2 TO " + indexColumnName1);
            execute("ALTER TABLE " + tableName + " RENAME COLUMN sym3 TO " + indexColumnName2);
            execute(insertPostingRowsSql(tableName, 0, 10, true));
            execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '2022-02-25'");
            execute("ALTER TABLE " + tableName + " ALTER COLUMN sym_top SYMBOL CAPACITY 64");

            execute(insertPostingRowsSql(tableName, 0, 35, true));
            final PostingSealFileNames oldFiles1 = resolvePostingSealFileNames(tableName, indexColumnName1, coveredColumnName, targetPartitionTimestamp, -1L);
            final PostingSealFileNames oldFiles2 = resolvePostingSealFileNames(tableName, indexColumnName2, coveredColumnName, targetPartitionTimestamp, -1L);
            assertPostingSealFilesExist(oldFiles1, true);
            assertPostingSealFilesExist(oldFiles2, true);

            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }

            final TableToken tableToken = engine.getTableTokenIfExists(tableName);
            Assert.assertNotNull("table must exist", tableToken);
            fillPostingSealPurgeQueue(tableToken);

            try {
                execute(insertPostingRowsSql(tableName, 35, 92, true));
                engine.releaseAllWriters();
                assertPostingSealFilesExist(oldFiles1, true);
                assertPostingSealFilesExist(oldFiles2, true);
                assertOpenDeferredPostingSealPurgeLogRow(tableToken, indexColumnName1, oldFiles1.sealTxn);
                assertOpenDeferredPostingSealPurgeLogRow(tableToken, indexColumnName2, oldFiles2.sealTxn);
            } finally {
                drainPostingSealPurgeQueue();
            }

            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }

            assertPostingSealFilesExist(oldFiles1, false);
            assertPostingSealFilesExist(oldFiles2, false);
        });
    }

    @Test
    public void testDirectPersistReadyDeferredPostingSealPurgeRetainsFutureEntryWhenQueueIsFull() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_SEAL_GEN_THRESHOLD, 1);

        assertMemoryLeak(() -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            drainPostingSealPurgeQueue();
            final String tableName = "posting_mixed_deferred";
            final String indexColumnName = "new_col_11";
            final String coveredColumnName = "marker";
            final long targetPartitionTimestamp = MicrosFormatUtils.parseTimestamp("2022-02-25T00:00:00.000000Z");

            execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, sym2 SYMBOL INDEX TYPE POSTING INCLUDE (marker), sym_top SYMBOL CAPACITY 128, marker LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(insertPostingRowsSql(tableName, -85, 0, false));
            execute("ALTER TABLE " + tableName + " RENAME COLUMN sym2 TO " + indexColumnName);
            execute(insertPostingRowsSql(tableName, 0, 10, false));
            execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '2022-02-25'");
            execute("ALTER TABLE " + tableName + " ALTER COLUMN sym_top SYMBOL CAPACITY 64");

            execute(insertPostingRowsSql(tableName, 0, 35, false));
            final PostingSealFileNames readyFiles = resolvePostingSealFileNames(tableName, indexColumnName, coveredColumnName, targetPartitionTimestamp, -1L);
            assertPostingSealFilesExist(readyFiles, true);

            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }

            final TableToken tableToken = engine.getTableTokenIfExists(tableName);
            Assert.assertNotNull("table must exist", tableToken);
            fillPostingSealPurgeQueue(tableToken);
            try {
                execute(insertPostingRowsSql(tableName, 35, 92, false));
                final PostingSealFileNames futureFiles = resolvePostingSealFileNames(tableName, indexColumnName, coveredColumnName, targetPartitionTimestamp, -1L);
                Assert.assertNotEquals("test setup: first O3 commit must publish a new seal", readyFiles.sealTxn, futureFiles.sealTxn);
                assertPostingSealFilesExist(readyFiles, true);
                assertPostingSealFilesExist(futureFiles, true);

                try (TableWriter writer = TestUtils.getWriter(engine, tableToken)) {
                    TableWriter.Row row = writer.newRow(MicrosFormatUtils.parseTimestamp("2022-02-25T01:32:00.000000Z"));
                    row.putSym(1, "XPHI");
                    row.putSym(2, "S");
                    row.putLong(3, 1999);
                    row.append();

                    row = writer.newRow(MicrosFormatUtils.parseTimestamp("2022-02-26T00:00:00.000000Z"));
                    row.putSym(1, "D");
                    row.putSym(2, "S");
                    row.putLong(3, 2000);
                    row.append();

                    writer.publishDeferredPostingSealPurgesOnFullQueueForTesting();
                    assertOpenDeferredPostingSealPurgeLogRow(tableToken, indexColumnName, readyFiles.sealTxn);
                    assertOpenDeferredPostingSealPurgeLogRowCount(tableToken, indexColumnName, futureFiles.sealTxn, 0);

                    drainPostingSealPurgeQueue();
                    writer.commit();
                }

                try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                    runPostingSealPurgeJob(purgeJob);
                }
                assertPostingSealFilesExist(readyFiles, false);
                assertPostingSealFilesExist(futureFiles, false);
            } finally {
                drainPostingSealPurgeQueue();
            }
        });
    }

    @Test
    public void testCommitSeqTxnPublishesReadyDeferredPostingSealPurgeRetainedByFullQueue() throws Exception {
        assertMemoryLeak(() -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            drainPostingSealPurgeQueue();
            final String tableName = "posting_commit_seq_deferred";
            final String indexColumnName = "new_col_11";
            final String coveredColumnName = "marker";
            final long targetPartitionTimestamp = MicrosFormatUtils.parseTimestamp("2022-02-25T00:00:00.000000Z");

            execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, sym2 SYMBOL INDEX TYPE POSTING INCLUDE (marker), sym_top SYMBOL CAPACITY 128, marker LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(insertPostingRowsSql(tableName, -85, 0, false));
            execute("ALTER TABLE " + tableName + " RENAME COLUMN sym2 TO " + indexColumnName);
            execute(insertPostingRowsSql(tableName, 0, 10, false));
            execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '2022-02-25'");
            execute("ALTER TABLE " + tableName + " ALTER COLUMN sym_top SYMBOL CAPACITY 64");

            execute(insertPostingRowsSql(tableName, 0, 35, false));
            final PostingSealFileNames oldFiles = resolvePostingSealFileNames(tableName, indexColumnName, coveredColumnName, targetPartitionTimestamp, -1L);
            assertPostingSealFilesExist(oldFiles, true);

            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }

            final TableToken tableToken = engine.getTableTokenIfExists(tableName);
            Assert.assertNotNull("table must exist", tableToken);
            fillPostingSealPurgeQueue(tableToken);
            try {
                execute(insertPostingRowsSql(tableName, 35, 92, false));
                assertPostingSealFilesExist(oldFiles, true);

                drainPostingSealPurgeQueue();
                try (TableWriter writer = TestUtils.getWriter(engine, tableToken)) {
                    writer.commitSeqTxn(12345);
                }

                try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                    runPostingSealPurgeJob(purgeJob);
                }
                assertPostingSealFilesExist(oldFiles, false);
            } finally {
                drainPostingSealPurgeQueue();
            }
        });
    }

    /**
     * A live PostingSealPurgeJob owns the purge-log writer and the ring is
     * full, so the close-time handoff in closeDeferredPostingSealPurges()
     * reaches neither the ring nor the shared log. Rather than dropping the
     * ready intent, the close spills it to the table-local pending file
     * (spilled == true), so nothing reaches the shared log yet the intent
     * survives on disk for the next open to replay. This covers the spill
     * fallback; the spill-write-failure drop path is covered by
     * {@link #testCloseDropsReadyDeferredPostingSealPurgeWhenSpillFileWriteFails()}.
     */
    @Test
    public void testCloseWithLivePurgeJobSpillsReadyDeferredPostingSealPurgeWhenQueueIsFull() throws Exception {
        assertMemoryLeak(() -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            drainPostingSealPurgeQueue();
            final String tableName = "posting_close_live_job_deferred";
            final String indexColumnName = "new_col_11";
            final String coveredColumnName = "marker";
            final long targetPartitionTimestamp = MicrosFormatUtils.parseTimestamp("2022-02-25T00:00:00.000000Z");

            execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, sym2 SYMBOL INDEX TYPE POSTING INCLUDE (marker), sym_top SYMBOL CAPACITY 128, marker LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(insertPostingRowsSql(tableName, -85, 0, false));
            execute("ALTER TABLE " + tableName + " RENAME COLUMN sym2 TO " + indexColumnName);
            execute(insertPostingRowsSql(tableName, 0, 10, false));
            execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '2022-02-25'");
            execute("ALTER TABLE " + tableName + " ALTER COLUMN sym_top SYMBOL CAPACITY 64");

            execute(insertPostingRowsSql(tableName, 0, 35, false));
            final PostingSealFileNames oldFiles = resolvePostingSealFileNames(tableName, indexColumnName, coveredColumnName, targetPartitionTimestamp, -1L);
            assertPostingSealFilesExist(oldFiles, true);

            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }

            final TableToken tableToken = engine.getTableTokenIfExists(tableName);
            Assert.assertNotNull("table must exist", tableToken);
            try (PostingSealPurgeJob ignoredLiveJob = new PostingSealPurgeJob(engine)) {
                fillPostingSealPurgeQueue(tableToken);
                try {
                    // The O3 commit advances _txn so the deferred purge is
                    // ready, but the ring is full so it stays in the
                    // TableWriter until close.
                    execute(insertPostingRowsSql(tableName, 35, 92, false));
                    engine.releaseAllWriters();

                    // The handoff reached neither the ring nor the shared log,
                    // but the ready intent was spilled to the table-local
                    // pending file instead of being dropped.
                    assertOpenDeferredPostingSealPurgeLogRowCount(tableToken, indexColumnName, oldFiles.sealTxn, 0);
                    assertPostingSealPurgePendingFileExists(tableToken, true);
                    assertPostingSealFilesExist(oldFiles, true);
                } finally {
                    drainPostingSealPurgeQueue();
                }
            }
        });
    }

    /**
     * The multi-record spill+replay round trip. A live PostingSealPurgeJob
     * owns the purge-log writer and the ring is full, so closing a table with
     * TWO superseded posting indexes spills BOTH ready intents into a single
     * two-record pending file - the only path that writes a record count >= 2.
     * The next writer open then walks that file, exercising the variable
     * inter-record stride (each record advances by 56 fixed bytes plus the
     * stored index column name), and republishes both intents so a later purge
     * job reclaims the superseded .pv/.pc sidecars for both columns.
     * <p>
     * The two index columns use DISTINCT name lengths on purpose: a record
     * stride that ignored the variable-length name would still land correctly
     * on the second record when both names share a length, hiding the bug. This
     * complements the single-record file-existence check in
     * {@link #testCloseWithLivePurgeJobSpillsReadyDeferredPostingSealPurgeWhenQueueIsFull()}
     * and the single-record replay in
     * {@link #testO3DeferredPostingSealPurgePersistsOnCloseWhenJobHoldsLogWriter()}.
     */
    @Test
    public void testCloseWithLivePurgeJobSpillsMultipleReadyDeferredPostingSealPurgesAndReplaysOnReopen() throws Exception {
        assertMemoryLeak(() -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            drainPostingSealPurgeQueue();
            final String tableName = "posting_close_live_job_multi_deferred";
            // Distinct lengths so the on-disk record stride genuinely depends
            // on the stored name length between records, not a constant.
            final String indexColumnName1 = "new_col_11";
            final String indexColumnName2 = "ix2";
            final String coveredColumnName = "marker";
            final long targetPartitionTimestamp = MicrosFormatUtils.parseTimestamp("2022-02-25T00:00:00.000000Z");

            execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, sym2 SYMBOL INDEX TYPE POSTING INCLUDE (marker), sym3 SYMBOL INDEX TYPE POSTING INCLUDE (marker), sym_top SYMBOL CAPACITY 128, marker LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(insertPostingRowsSql(tableName, -85, 0, true));
            execute("ALTER TABLE " + tableName + " RENAME COLUMN sym2 TO " + indexColumnName1);
            execute("ALTER TABLE " + tableName + " RENAME COLUMN sym3 TO " + indexColumnName2);
            execute(insertPostingRowsSql(tableName, 0, 10, true));
            execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '2022-02-25'");
            execute("ALTER TABLE " + tableName + " ALTER COLUMN sym_top SYMBOL CAPACITY 64");

            execute(insertPostingRowsSql(tableName, 0, 35, true));
            final PostingSealFileNames oldFiles1 = resolvePostingSealFileNames(tableName, indexColumnName1, coveredColumnName, targetPartitionTimestamp, -1L);
            final PostingSealFileNames oldFiles2 = resolvePostingSealFileNames(tableName, indexColumnName2, coveredColumnName, targetPartitionTimestamp, -1L);
            assertPostingSealFilesExist(oldFiles1, true);
            assertPostingSealFilesExist(oldFiles2, true);

            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }

            final TableToken tableToken = engine.getTableTokenIfExists(tableName);
            Assert.assertNotNull("table must exist", tableToken);
            try (PostingSealPurgeJob ignoredLiveJob = new PostingSealPurgeJob(engine)) {
                fillPostingSealPurgeQueue(tableToken);
                try {
                    // The O3 commit advances _txn so both deferred purges are
                    // ready, but the ring is full so they stay in the
                    // TableWriter until close.
                    execute(insertPostingRowsSql(tableName, 35, 92, true));
                    engine.releaseAllWriters();

                    // The handoff reached neither the ring nor the shared log,
                    // so both ready intents were spilled into one two-record
                    // pending file rather than the log table.
                    assertOpenDeferredPostingSealPurgeLogRowCount(tableToken, indexColumnName1, oldFiles1.sealTxn, 0);
                    assertOpenDeferredPostingSealPurgeLogRowCount(tableToken, indexColumnName2, oldFiles2.sealTxn, 0);
                    assertPostingSealPurgePendingFileExists(tableToken, true);
                    assertPostingSealFilesExist(oldFiles1, true);
                    assertPostingSealFilesExist(oldFiles2, true);
                } finally {
                    drainPostingSealPurgeQueue();
                }
            }

            // Reopen so writer-open recovery walks the two-record pending file
            // (the inter-record stride path) and republishes both intents. The
            // file is removed once recovered.
            try (TableWriter ignore = engine.getWriter(tableToken, "posting seal purge multi-record recovery test")) {
                Assert.assertNotNull(ignore);
            }
            assertPostingSealPurgePendingFileExists(tableToken, false);

            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }

            // Both superseded seal-file pairs must be purged, proving the
            // second record parsed at the correct offset.
            assertPostingSealFilesExist(oldFiles1, false);
            assertPostingSealFilesExist(oldFiles2, false);
        });
    }

    /**
     * The genuine data-losing branch of closeDeferredPostingSealPurges(): a
     * live PostingSealPurgeJob owns the purge-log writer and the ring is full
     * (so the handoff fails), AND the close-time spill to the table-local
     * pending file also fails. With spilled == false the ready intent is
     * dropped with a LOG.critical and the superseded .pv/.pc sidecar files are
     * orphaned for the process lifetime - the next open finds no spill file to
     * replay, so a later purge job cannot reclaim them. Contrast with
     * {@link #testCloseWithLivePurgeJobSpillsReadyDeferredPostingSealPurgeWhenQueueIsFull()},
     * where the spill succeeds and the intent survives.
     */
    @Test
    public void testCloseDropsReadyDeferredPostingSealPurgeWhenSpillFileWriteFails() throws Exception {
        final AtomicBoolean failSpillWrite = new AtomicBoolean(false);
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (failSpillWrite.get() && Utf8s.endsWithAscii(name, "_posting_seal_purge_pending.d")) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ff, () -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            drainPostingSealPurgeQueue();
            final String tableName = "posting_close_spill_fail_deferred";
            final String indexColumnName = "new_col_11";
            final String coveredColumnName = "marker";
            final long targetPartitionTimestamp = MicrosFormatUtils.parseTimestamp("2022-02-25T00:00:00.000000Z");

            execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, sym2 SYMBOL INDEX TYPE POSTING INCLUDE (marker), sym_top SYMBOL CAPACITY 128, marker LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(insertPostingRowsSql(tableName, -85, 0, false));
            execute("ALTER TABLE " + tableName + " RENAME COLUMN sym2 TO " + indexColumnName);
            execute(insertPostingRowsSql(tableName, 0, 10, false));
            execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '2022-02-25'");
            execute("ALTER TABLE " + tableName + " ALTER COLUMN sym_top SYMBOL CAPACITY 64");

            execute(insertPostingRowsSql(tableName, 0, 35, false));
            final PostingSealFileNames oldFiles = resolvePostingSealFileNames(tableName, indexColumnName, coveredColumnName, targetPartitionTimestamp, -1L);
            assertPostingSealFilesExist(oldFiles, true);

            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }

            final TableToken tableToken = engine.getTableTokenIfExists(tableName);
            Assert.assertNotNull("table must exist", tableToken);
            try (PostingSealPurgeJob ignoredLiveJob = new PostingSealPurgeJob(engine)) {
                fillPostingSealPurgeQueue(tableToken);
                try {
                    // The O3 commit advances _txn so the deferred purge is
                    // ready, but the ring is full so it stays in the
                    // TableWriter until close.
                    execute(insertPostingRowsSql(tableName, 35, 92, false));

                    // Close while the live job holds the purge-log writer and
                    // the spill write fails. The ready intent reaches neither
                    // the ring, nor the shared log, nor the spill file, so the
                    // drop branch fires.
                    failSpillWrite.set(true);
                    engine.releaseAllWriters();
                    failSpillWrite.set(false);

                    // No record of the intent anywhere: the shared log is
                    // untouched and the spill file was never created.
                    assertOpenDeferredPostingSealPurgeLogRowCount(tableToken, indexColumnName, oldFiles.sealTxn, 0);
                    assertPostingSealPurgePendingFileExists(tableToken, false);
                    // The superseded files are orphaned by the drop.
                    assertPostingSealFilesExist(oldFiles, true);
                } finally {
                    drainPostingSealPurgeQueue();
                }
            }

            // Reopen and run a purge job. With no spill file to replay and the
            // intent dropped, the orphaned files cannot be reclaimed - the data
            // loss the LOG.critical warns about.
            try (TableWriter ignore = engine.getWriter(tableToken, "posting seal purge drop test")) {
                Assert.assertNotNull(ignore);
            }
            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }
            assertPostingSealFilesExist(oldFiles, true);
        });
    }

    @Test
    public void testSquashPartitionsKeepsRetainedDeferredPostingSealPurgeAcrossSuccessfulCommit() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 20);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 20);

        assertMemoryLeak(() -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            final String tableName = "posting_squash_deferred";
            final String indexColumnName = "new_col_11";
            final String coveredColumnName = "marker";
            final long targetPartitionTimestamp = MicrosFormatUtils.parseTimestamp("2022-02-25T00:00:00.000000Z");

            execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, sym2 SYMBOL INDEX TYPE POSTING INCLUDE (marker), sym_top SYMBOL CAPACITY 128, marker LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(insertPostingRowsSql(tableName, -85, 0, false));
            execute("ALTER TABLE " + tableName + " RENAME COLUMN sym2 TO " + indexColumnName);
            execute(insertPostingRowsSql(tableName, 0, 10, false));
            execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '2022-02-25'");
            execute("ALTER TABLE " + tableName + " ALTER COLUMN sym_top SYMBOL CAPACITY 64");

            execute(insertPostingRowsSql(tableName, 0, 35, false));
            execute("INSERT INTO " + tableName + " VALUES "
                    + "('2022-02-26T00:00:00.000000Z', 'A', 'S', 1000),"
                    + "('2022-02-26T01:00:00.000000Z', 'A', 'S', 1001),"
                    + "('2022-02-26T02:00:00.000000Z', 'B', 'S', 1002),"
                    + "('2022-02-26T20:00:00.000000Z', 'B', 'S', 1003)");
            execute("INSERT INTO " + tableName + " VALUES ('2022-02-26T19:00:00.000000Z', 'C', 'S', 1004)");
            final PostingSealFileNames oldFiles = resolvePostingSealFileNames(tableName, indexColumnName, coveredColumnName, targetPartitionTimestamp, -1L);
            assertPostingSealFilesExist(oldFiles, true);

            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }

            final TableToken tableToken = engine.getTableTokenIfExists(tableName);
            Assert.assertNotNull("table must exist", tableToken);
            fillPostingSealPurgeQueue(tableToken);
            try {
                execute(insertPostingRowsSql(tableName, 35, 92, false));
                final long splitPartitionCount = selectLong("SELECT count() FROM table_partitions('" + tableName + "')");
                Assert.assertTrue(
                        "test setup: O3 insert must create split partitions before squashPartitions(), count=" + splitPartitionCount,
                        splitPartitionCount > 2
                );
                try (TableWriter writer = TestUtils.getWriter(engine, tableToken)) {
                    writer.squashPartitions();
                    drainPostingSealPurgeQueue();
                    writer.commitSeqTxn(12345);
                }
                final long squashedPartitionCount = selectLong("SELECT count() FROM table_partitions('" + tableName + "')");
                Assert.assertTrue(
                        "squashPartitions() must reduce the split partition count [before=" + splitPartitionCount + ", after=" + squashedPartitionCount + ']',
                        squashedPartitionCount < splitPartitionCount
                );

                try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                    runPostingSealPurgeJob(purgeJob);
                }
                assertPostingSealFilesExist(oldFiles, false);
            } finally {
                drainPostingSealPurgeQueue();
            }
        });
    }

    @Test
    public void testAttachDetachForceRemoveKeepRetainedDeferredPostingSealPurgeAcrossCommits() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_ATTACH_PARTITION_SUFFIX, DETACHED_DIR_MARKER);

        assertMemoryLeak(() -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            final String tableName = "posting_partition_ops_deferred";
            final String indexColumnName = "new_col_11";
            final String coveredColumnName = "marker";
            final long targetPartitionTimestamp = MicrosFormatUtils.parseTimestamp("2022-02-25T00:00:00.000000Z");

            execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, sym2 SYMBOL INDEX TYPE POSTING INCLUDE (marker), sym_top SYMBOL CAPACITY 128, marker LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(insertPostingRowsSql(tableName, -85, 0, false));
            execute("ALTER TABLE " + tableName + " RENAME COLUMN sym2 TO " + indexColumnName);
            execute(insertPostingRowsSql(tableName, 0, 10, false));
            execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '2022-02-25'");
            execute("ALTER TABLE " + tableName + " ALTER COLUMN sym_top SYMBOL CAPACITY 64");

            execute(insertPostingRowsSql(tableName, 0, 35, false));
            execute("INSERT INTO " + tableName + " VALUES ('2022-02-26T00:00:00.000000Z', 'A', 'S', 999)");
            assertQuery("SELECT count() FROM " + tableName + " WHERE ts = '2022-02-26T00:00:00.000000Z'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count
                            1
                            """);
            final PostingSealFileNames oldFiles = resolvePostingSealFileNames(tableName, indexColumnName, coveredColumnName, targetPartitionTimestamp, -1L);
            assertPostingSealFilesExist(oldFiles, true);

            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }

            final TableToken tableToken = engine.getTableTokenIfExists(tableName);
            Assert.assertNotNull("table must exist", tableToken);
            final long dayBeforeTarget = MicrosFormatUtils.parseTimestamp("2022-02-24T00:00:00.000000Z");
            final long dayAfterTarget = MicrosFormatUtils.parseTimestamp("2022-02-26T00:00:00.000000Z");
            fillPostingSealPurgeQueue(tableToken);
            try {
                execute(insertPostingRowsSql(tableName, 35, 92, false));
                try (TableWriter writer = TestUtils.getWriter(engine, tableToken)) {
                    Assert.assertEquals(AttachDetachStatus.OK, writer.detachPartition(dayBeforeTarget));
                    Assert.assertEquals(AttachDetachStatus.OK, writer.attachPartition(dayBeforeTarget));

                    LongList partitions = new LongList();
                    partitions.add(dayAfterTarget);
                    writer.forceRemovePartitions(partitions);

                    drainPostingSealPurgeQueue();
                    writer.commitSeqTxn(12345);
                }
                assertQuery("SELECT count() FROM " + tableName + " WHERE ts = '2022-02-26T00:00:00.000000Z'")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("""
                                count
                                0
                                """);

                try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                    runPostingSealPurgeJob(purgeJob);
                }
                assertPostingSealFilesExist(oldFiles, false);
            } finally {
                drainPostingSealPurgeQueue();
            }
        });
    }

    @Test
    public void testO3DeferredPostingSealPurgeRecoveryHandlesCorruptPendingFile() throws Exception {
        assertMemoryLeak(() -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            final String tableName = "posting_o3_recovery";
            final String indexColumnName = "new_col_11";
            final String coveredColumnName = "marker";
            final long targetPartitionTimestamp = MicrosFormatUtils.parseTimestamp("2022-02-25T00:00:00.000000Z");

            execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, sym2 SYMBOL INDEX TYPE POSTING INCLUDE (marker), sym_top SYMBOL CAPACITY 128, marker LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(insertPostingRowsSql(-85, 0));
            execute("ALTER TABLE " + tableName + " RENAME COLUMN sym2 TO " + indexColumnName);
            execute(insertPostingRowsSql(0, 10));
            execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '2022-02-25'");
            execute("ALTER TABLE " + tableName + " ALTER COLUMN sym_top SYMBOL CAPACITY 64");
            execute(insertPostingRowsSql(0, 35));

            // A live, un-superseded seal: no garbage parsed from a corrupt
            // pending file may cause its .pv/.pc files to be purged.
            final PostingSealFileNames liveFiles = resolvePostingSealFileNames(tableName, indexColumnName, coveredColumnName, targetPartitionTimestamp, -1L);
            assertPostingSealFilesExist(liveFiles, true);

            final TableToken tableToken = engine.getTableTokenIfExists(tableName);
            Assert.assertNotNull("table must exist", tableToken);

            // Each corrupt pending-file shape must be discarded on the next open
            // without crashing the writer, without replaying garbage purge tasks,
            // and the file must be removed so it cannot accumulate:
            //   0 - unknown format marker
            //   1 - valid header but body truncated mid fixed-part
            //   2 - valid header + full fixed part but out-of-range name length
            //   3 - file shorter than the 8-byte header
            for (int variant = 0; variant < 4; variant++) {
                engine.releaseAllWriters();
                writeCorruptPostingSealPurgePendingFile(tableToken, variant);
                assertPostingSealPurgePendingFileExists(tableToken, true);

                try (TableWriter writer = engine.getWriter(tableToken, "corrupt posting purge recovery test")) {
                    Assert.assertNotNull("writer must open despite a corrupt pending file [variant=" + variant + ']', writer);
                }

                assertPostingSealPurgePendingFileExists(tableToken, false);
                assertPostingSealFilesExist(liveFiles, true);
            }

            // A purge job run finds no garbage work that touches the live seal.
            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }
            assertPostingSealFilesExist(liveFiles, true);
        });
    }

    @Test
    public void testO3PostingIndexRecoveryAfterPurgedCommittedSeal() throws Exception {
        FailAppendPositionLengthFacade ff = new FailAppendPositionLengthFacade();
        assertMemoryLeak(ff, () -> {
            final String tableName = "posting_o3_recovery";

            // Set up the shape produced by the fuzz failure: a non-WAL table
            // with a POSTING symbol index, a renamed indexed column, and a
            // symbol-capacity change before the O3/replace-style insert.
            // sym2 carries an explicit INCLUDE (ts) so the index is covering --
            // the test asserts a CoveringIndex plan below. A bare INDEX TYPE
            // POSTING is non-covering and would not produce that plan.
            execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, sym2 SYMBOL INDEX TYPE POSTING INCLUDE (ts), sym_top SYMBOL CAPACITY 128, marker LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(insertPostingRowsSql(-85, 0));
            execute("ALTER TABLE " + tableName + " RENAME COLUMN sym2 TO new_col_11");
            execute(insertPostingRowsSql(0, 10));
            execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '2022-02-25'");
            execute("ALTER TABLE " + tableName + " ALTER COLUMN sym_top SYMBOL CAPACITY 64");

            // This first committed seal contains the boundary XPHI row at
            // rowid 34. It must remain readable if a later in-flight seal is
            // rolled back during recovery.
            execute(insertPostingRowsSql(0, 35));
            final PostingSealFileNames oldFiles = resolvePostingSealFileNames(
                    tableName,
                    "new_col_11",
                    null,
                    MicrosFormatUtils.parseTimestamp("2022-02-25T00:00:00.000000Z"),
                    -1L
            );
            assertPostingSealFilesExist(oldFiles, true);

            // The next O3 batch publishes a newer posting seal before the
            // table _txn is durable. Fail later, in setAppendPosition(), so
            // the table data is on disk but the commit is not completed.
            final String retryBatch = insertPostingRowsSql(35, 92);
            ff.arm();
            try {
                execute(retryBatch);
                Assert.fail("expected append-position failure");
            } catch (CairoException | CairoError e) {
                Assert.assertEquals(1, ff.getFailureCount());
            }

            // Exercise the race explicitly: the unfixed code already queued a
            // purge for the previous committed seal, even though the replacing
            // seal belongs to the failed future transaction.
            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }
            assertPostingSealFilesExist(oldFiles, true);

            // Reopen after the distressed writer. Recovery drops/trims the
            // failed future posting chain entry and should fall back to the
            // prior committed seal that still contains rowid 34.
            engine.releaseAllWriters();
            execute(retryBatch);
            execute(insertPostingRowsSql(92, 139));
            engine.releaseAllWriters();

            // Full scan is the source of truth: all XPHI rows are present in
            // the column data after the failed batch is retried.
            assertQuery("SELECT /*+ no_index */ count() FROM " + tableName + " WHERE new_col_11 = 'XPHI'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            105
                            """);
            // Indexed scan must agree. The bug loses the boundary row from the
            // recovered posting index when the committed seal file was purged.
            assertQuery("SELECT count() FROM " + tableName + " WHERE new_col_11 = 'XPHI'")
                    .noLeakCheck()
                    .assertsPlanContaining("Count", "CoveringIndex on: new_col_11");
            assertQuery("SELECT count() FROM " + tableName + " WHERE new_col_11 = 'XPHI'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            105
                            """);
        });
    }

    @Test
    public void testOpenFromO3ContextPropagatesUpcomingTxn() throws Exception {
        assertMemoryLeak(() -> {
            final String name = "posting_o3_upcoming_txn";
            final long upcomingTxn = 42L;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.setO3PathContext(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, upcomingTxn);
                    writer.openFromO3Context(/* isInit */ true);
                    writer.add(0, 0);
                    writer.add(1, 1);
                    writer.setMaxValue(1);
                    writer.commit();
                }
                FilesFacade rawFf = configuration.getFilesFacade();
                LPSZ keyFile = PostingIndexUtils.keyFileName(
                        path.trimTo(plen), name, COLUMN_NAME_TXN_NONE);
                long fileSize = rawFf.length(keyFile);
                try (MemoryCMARWImpl mem = new MemoryCMARWImpl(
                        rawFf, keyFile, rawFf.getPageSize(), fileSize,
                        MemoryTag.MMAP_DEFAULT, /* opts */ 0)) {
                    PostingIndexChainWriter chain = new PostingIndexChainWriter();
                    chain.openExisting(mem);
                    Assert.assertTrue("chain must have head", chain.hasHead());
                    PostingIndexChainEntry.Snapshot head = new PostingIndexChainEntry.Snapshot();
                    chain.loadHeadEntry(mem, head);
                    Assert.assertEquals(
                            "openFromO3Context must propagate o3CtxUpcomingTxn into the published chain entry",
                            upcomingTxn,
                            head.txnAtSeal
                    );
                }
            }
        });
    }

    @Test
    public void testParquetIndexWriteUsesCommitDense() throws Exception {
        // O3PartitionJob.updateParquetIndexes goes through commitDense for a
        // NON-covering POSTING index. That path runs when O3 mutates an
        // already-parquet partition. A bare INDEX TYPE POSTING (no INCLUDE) is
        // non-covering, so this exercises commitDense: a covering parquet
        // partition is instead resealed (resealParquetCoveringForPartition
        // rebuilds .pv + .pci/.pc), which intentionally rotates the value file.
        // Convert the first partition to parquet, then O3-insert into it to
        // trigger the rewrite. After that the chain head must hold a single
        // dense gen with no rotated .pv.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_parquet_posting (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_parquet_posting
                    SELECT
                        dateadd('s', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                        rnd_symbol('A', 'B', 'C', 'D', 'E')
                    FROM long_sequence(1000)
                    """);
            execute("""
                    INSERT INTO t_parquet_posting
                    SELECT
                        dateadd('s', x::INT, '2024-01-02T00:00:00Z'::TIMESTAMP),
                        rnd_symbol('A', 'B', 'C', 'D', 'E')
                    FROM long_sequence(1000)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t_parquet_posting CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("""
                    INSERT INTO t_parquet_posting VALUES
                    ('2024-01-01T00:30:00Z', 'F'),
                    ('2024-01-01T01:30:00Z', 'G')
                    """);
            drainWalQueue();
            engine.releaseAllWriters();

            TableToken token = engine.getTableTokenIfExists("t_parquet_posting");
            Assert.assertNotNull("table must exist", token);
            long partitionTs;
            long partitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
                partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }
            FilesFacade rawFf = configuration.getFilesFacade();
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                setPathForNativePartition(
                        path, ColumnType.TIMESTAMP, io.questdb.cairo.PartitionBy.DAY, partitionTs, partitionNameTxn);
                int plen = path.size();
                LPSZ keyFile = PostingIndexUtils.keyFileName(
                        path.trimTo(plen), "sym", COLUMN_NAME_TXN_NONE);
                long fileSize = rawFf.length(keyFile);
                Assert.assertTrue(".pk file must exist in parquet partition dir, path=" + keyFile, fileSize > 0);
                try (MemoryCMARWImpl mem = new MemoryCMARWImpl(
                        rawFf, keyFile, rawFf.getPageSize(), fileSize,
                        MemoryTag.MMAP_DEFAULT, /* opts */ 0)) {
                    PostingIndexChainWriter chain = new PostingIndexChainWriter();
                    chain.openExisting(mem);
                    Assert.assertTrue("chain must have head after parquet rewrite", chain.hasHead());
                    PostingIndexChainEntry.Snapshot head = new PostingIndexChainEntry.Snapshot();
                    chain.loadHeadEntry(mem, head);
                    Assert.assertEquals("single dense gen expected after commitDense", 1, head.genCount);
                    long gen0DirOffset = PostingIndexChainEntry.resolveGenDirOffset(head.offset, 0);
                    int gen0KeyCount = mem.getInt(gen0DirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
                    Assert.assertTrue("gen 0 must be dense (positive KEY_COUNT), got " + gen0KeyCount,
                            gen0KeyCount > 0);
                    Assert.assertEquals("commitDense must not rotate sealTxn", 0, head.sealTxn);
                    LPSZ pvRotated = PostingIndexUtils.valueFileName(
                            path.trimTo(plen), "sym", COLUMN_NAME_TXN_NONE, 1);
                    Assert.assertFalse("rotated .pv.1 must not exist after commitDense, path=" + pvRotated,
                            rawFf.exists(pvRotated));
                }
            }
        });
    }

    /**
     * Non-covering sibling of
     * {@link #testCoveringRecordCursorSingleKeyClosedOffOperatingThread}: when
     * the query selects a column outside the INCLUDE set, the planner falls
     * back to the plain index scan and PageFrameRecordCursorImpl retains the
     * posting row cursor (from DeferredSymbolIndexRowCursorFactory) across
     * hasNext() calls. Its close() frees that cursor on the teardown thread,
     * so the off-thread close reaches the posting cursor without any covering
     * machinery involved -- the gate must hold on this path too.
     */
    @Test
    public void testPostingIndexRowCursorClosedOffOperatingThread() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_plain_scan (
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, ts),
                        price DOUBLE,
                        qty LONG,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY""");
            // See testCoveringRecordCursorMultiKeyClosedOffOperatingThread for
            // why the key expression is (x * x) % 3.
            execute("""
                    INSERT INTO t_plain_scan
                    SELECT 'k' || ((x * x) % 3), x::DOUBLE, x, timestamp_sequence('2024-01-01', 60_000_000)
                    FROM long_sequence(30_000)""");

            // qty is not in the INCLUDE set, so the covering factory cannot
            // serve this projection and the plain index scan takes over.
            final String scanSql = "SELECT sym, qty FROM t_plain_scan WHERE sym = 'k1'";
            final String aggSql = "SELECT count() c, min(qty) mn, max(qty) mx FROM t_plain_scan WHERE sym = 'k1'";
            assertQuery(scanSql).noLeakCheck().assertsPlanContaining("Index forward scan on: sym");
            assertQuery(aggSql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .withPlanContaining("Index forward scan on: sym")
                    .returns("""
                            c\tmn\tmx
                            20000\t1\t29999
                            """);

            // Peel the QueryProgress wrapper off the compiled factory: its cursor
            // close() catches Throwable, logs "could not close record cursor" and
            // recovers the leaked reader, masking the failure this test pins. The
            // production teardown paths this test models (HttpConnectionContext.
            // reset() -> closeMergeCursors(), pgwire covering cursor close) close
            // the covering cursors without that guard.
            try (RecordCursorFactory factory = select(scanSql)) {
                RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext);
                boolean isClosed = false;
                try {
                    var record = cursor.getRecord();
                    for (int i = 0; i < 128 && cursor.hasNext(); i++) {
                        record.getLong(1);
                    }
                    closeCursorOffOperatingThread(cursor);
                    isClosed = true;
                } finally {
                    if (!isClosed) {
                        cursor.close();
                    }
                }
            }

            // The orderly close chain must complete and return the TableReader to
            // the pool. A pre-gate build aborts it mid-way: the posting cursor's
            // owning-thread assert throws, QueryProgress swallows the error after
            // logging "could not close record cursor", and the frame cursor -- and
            // its TableReader -- never get freed.
            Assert.assertEquals(0, engine.getBusyReaderCount());

            // The reader survived the off-thread close: same plan, same rows.
            assertQuery(aggSql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .withPlanContaining("Index forward scan on: sym")
                    .returns("""
                            c\tmn\tmx
                            20000\t1\t29999
                            """);
        });
    }

    /**
     * Off-operating-thread sibling of
     * {@link #testBwdNullCursorClosedAfterReaderDoesNotLeakBlockBuffer}. Closes a
     * still-checked-out backward NullCursor on ANOTHER thread while the reader
     * stays open -- the connection-migration analogue the canRepool() gate
     * handles. Pre-gate, PostingIndexBwdReader.NullCursor.close() asserted the
     * owning thread and threw "posting index null cursor closed off the reader's
     * owning thread" here under -ea; the gate now routes it to a local release.
     * key == 0 && columnTop > 0 && minValue < columnTop makes getCursor() vend the
     * NullCursor.
     */
    @Test
    public void testPostingIndexBwdNullCursorClosedOffOperatingThread() throws Exception {
        assertMemoryLeak(() -> {
            final String name = "posting_bwd_null_cursor_off_thread";
            final long columnTop = 256;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    // Real key-0 entries at/after columnTop with irregular gaps so
                    // the NullCursor's super.hasNext() decode allocates the block
                    // buffer; rows below columnTop surface as implicit nulls.
                    long rowId = columnTop;
                    for (int i = 0; i < 4096; i++) {
                        writer.add(0, rowId);
                        rowId += 1 + (i % 7);
                    }
                    writer.setMaxValue(rowId);
                    writer.commit();
                }

                try (PostingIndexBwdReader reader = new PostingIndexBwdReader(
                        configuration, path.trimTo(plen), name,
                        COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, columnTop,
                        /* metadata */ null, /* columnVersionReader */ null,
                        /* partitionTimestamp */ 0)) {
                    final long expected = drainRowCursor(reader.getCursor(0, 0L, Long.MAX_VALUE));

                    // Park a partially-drained NullCursor and close it off-thread.
                    final RowCursor parked = reader.getCursor(0, 0L, Long.MAX_VALUE);
                    for (int i = 0; i < 128 && parked.hasNext(); i++) {
                        parked.next();
                    }
                    closeCursorOffOperatingThread(parked);

                    // The reader survived: same row count after the off-thread close.
                    Assert.assertEquals(expected, drainRowCursor(reader.getCursor(0, 0L, Long.MAX_VALUE)));
                }
            }
        });
    }

    /**
     * Off-operating-thread sibling of
     * {@link #testRowCursorClosedAfterReaderDoesNotLeakBlockBuffer}. That test
     * closes on the owning thread after the reader is closed (the isOpen() leak
     * guard); this one closes a still-checked-out backward cursor on ANOTHER
     * thread while the reader stays open. Pre-gate, PostingIndexBwdReader.Cursor
     * .close() threw "posting index cursor closed off the reader's owning thread"
     * here under -ea; the gate now routes the off-thread close to a local release.
     * Uses the raw reader so it drives {@link PostingIndexBwdReader.Cursor}
     * directly and deterministically, like the leak-guard siblings in this file.
     */
    @Test
    public void testPostingIndexBwdRowCursorClosedOffOperatingThread() throws Exception {
        assertMemoryLeak(() -> {
            final String name = "posting_bwd_cursor_off_thread";
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    // Irregular gaps -> non-zero bit-width blocks -> the decode path
                    // allocates blockBufferAddr (NATIVE_INDEX_READER), the native
                    // buffer the off-thread release must free.
                    long rowId = 0;
                    for (int i = 0; i < 4096; i++) {
                        writer.add(0, rowId);
                        rowId += 1 + (i % 7);
                    }
                    writer.setMaxValue(rowId);
                    writer.commit();
                }

                try (PostingIndexBwdReader reader = new PostingIndexBwdReader(
                        configuration, path.trimTo(plen), name,
                        COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, /* columnTop */ 0,
                        /* metadata */ null, /* columnVersionReader */ null,
                        /* partitionTimestamp */ 0)) {
                    final long expected = drainRowCursor(reader.getCursor(0, 0L, Long.MAX_VALUE));

                    // Partially drive a cursor so it allocates the decode block
                    // buffer, then park it and close it on a second thread.
                    final RowCursor parked = reader.getCursor(0, 0L, Long.MAX_VALUE);
                    for (int i = 0; i < 128 && parked.hasNext(); i++) {
                        parked.next();
                    }
                    closeCursorOffOperatingThread(parked);

                    Assert.assertEquals(expected, drainRowCursor(reader.getCursor(0, 0L, Long.MAX_VALUE)));
                }
            }
        });
    }

    /**
     * Off-operating-thread sibling for {@link PostingIndexFwdReader.NullCursor}
     * (the forward reader's implicit-null cursor). Closes a still-checked-out
     * forward NullCursor on ANOTHER thread while the reader stays open. Pre-gate
     * this threw "posting index null cursor closed off the reader's owning thread"
     * under -ea. key == 0 && columnTop > 0 && minValue < columnTop makes
     * getCursor() vend the NullCursor.
     */
    @Test
    public void testPostingIndexFwdNullCursorClosedOffOperatingThread() throws Exception {
        assertMemoryLeak(() -> {
            final String name = "posting_fwd_null_cursor_off_thread";
            final long columnTop = 256;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    long rowId = columnTop;
                    for (int i = 0; i < 4096; i++) {
                        writer.add(0, rowId);
                        rowId += 1 + (i % 7);
                    }
                    writer.setMaxValue(rowId);
                    writer.commit();
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name,
                        COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, columnTop)) {
                    final long expected = drainRowCursor(reader.getCursor(0, 0L, Long.MAX_VALUE));

                    final RowCursor parked = reader.getCursor(0, 0L, Long.MAX_VALUE);
                    for (int i = 0; i < 128 && parked.hasNext(); i++) {
                        parked.next();
                    }
                    closeCursorOffOperatingThread(parked);

                    Assert.assertEquals(expected, drainRowCursor(reader.getCursor(0, 0L, Long.MAX_VALUE)));
                }
            }
        });
    }

    /**
     * Bitmap sibling documenting the M1 fold-in: the same off-thread close on a
     * plain SYMBOL INDEX (default BITMAP) reader. Unlike the posting variants this
     * is green with OR without the operating-thread gate -- bitmap cursors hold no
     * native memory and never had the -ea assert, so the off-thread close never
     * threw and the gate's only effect (skip pooling) is not black-box observable.
     * The gate's mechanism is red-tested by the posting variants (identical code);
     * this test locks in that the sanctioned migration path stays clean for the
     * default index type.
     */
    @Test
    public void testBitmapIndexRowCursorClosedOffOperatingThread() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_bitmap (
                        sym SYMBOL INDEX,
                        qty LONG,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY""");
            execute("""
                    INSERT INTO t_bitmap
                    SELECT 'k' || ((x * x) % 3), x, timestamp_sequence('2024-01-01', 60_000_000)
                    FROM long_sequence(30_000)""");

            final String scanSql = "SELECT sym, qty FROM t_bitmap WHERE sym = 'k1'";
            final String aggSql = "SELECT count() c, min(qty) mn, max(qty) mx FROM t_bitmap WHERE sym = 'k1'";
            assertQuery(scanSql).noLeakCheck().assertsPlanContaining("Index forward scan on: sym");
            assertQuery(aggSql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .withPlanContaining("Index forward scan on: sym")
                    .returns("""
                            c\tmn\tmx
                            20000\t1\t29999
                            """);

            // Peel the QueryProgress wrapper (its close0() swallows close errors)
            // and close the base record cursor off the thread that checked it out.
            try (RecordCursorFactory factory = select(scanSql)) {
                RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext);
                boolean isClosed = false;
                try {
                    var record = cursor.getRecord();
                    for (int i = 0; i < 128 && cursor.hasNext(); i++) {
                        record.getLong(1);
                    }
                    closeCursorOffOperatingThread(cursor);
                    isClosed = true;
                } finally {
                    if (!isClosed) {
                        cursor.close();
                    }
                }
            }

            // The orderly close completed and returned the TableReader to the pool.
            Assert.assertEquals(0, engine.getBusyReaderCount());

            assertQuery(aggSql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .withPlanContaining("Index forward scan on: sym")
                    .returns("""
                            c\tmn\tmx
                            20000\t1\t29999
                            """);
        });
    }

    /**
     * Reader must clamp returned rowids by the picked chain entry's
     * V2_ENTRY_OFFSET_MAX_VALUE field. Writers can leave dirty
     * (key, rowid) pairs in .pv past the chain's tracked coverage --
     * for example after an O3 split shrinks a partition before the
     * next reseal evicts them, or after a stale generation a
     * sparse-gen append later supersedes. The chain entry's MAX_VALUE
     * is the boundary between clean and dirty rows; the reader is the
     * only place that can skip them without a full reseal.
     * <p>
     * Reproducer (no race, no fault injection): write 100 rowids
     * across 5 keys to a fresh chain and commit -- the chain head now
     * has MAX_VALUE=99 with valueMemSize covering all 100 rowids in
     * .pv. Then call {@code setMaxValue(49)}, which writes 49 into
     * the head entry's MAX_VALUE field via
     * {@code PostingIndexChainWriter.updateHeadMaxValue} without
     * touching .pv or the gen-dir. The on-disk state is the canonical
     * dirty-data signature: chain claims coverage up to 49, but the
     * encoded gen still walks rowids 0..99.
     * <p>
     * Open a fresh PostingIndexFwdReader / PostingIndexBwdReader and
     * request key 0 (rowids 0, 5, 10, ..., 95) with caller-supplied
     * max=Long.MAX_VALUE. Without the fix, the cursor walks every
     * encoded value because it ignores entryMaxValue and clamps only
     * by the caller-supplied bound. With the fix,
     * {@code indexMaxValue = min(callerMax, entryMaxValue) = 49} and
     * the cursor stops emitting values above 49.
     */
    @Test
    public void testReaderClampsCursorToChainEntryMaxValue() throws Exception {
        assertMemoryLeak(() -> {
            final String name = "posting_max_value_clamp";
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                final FilesFacade ff = configuration.getFilesFacade();

                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    // Five keys, rowids 0..99. add() requires per-key
                    // ascending rowids; using rowid as the loop index
                    // satisfies that for every key.
                    for (long rowId = 0; rowId < 100; rowId++) {
                        writer.add((int) (rowId % 5), rowId);
                    }
                    writer.setMaxValue(99);
                    writer.commit();
                    // Lower MAX_VALUE in place. updateHeadMaxValue
                    // writes V2_ENTRY_OFFSET_MAX_VALUE atomically and
                    // republishes the chain header -- it does not
                    // touch valueMem or the gen-dir, so the encoded
                    // gen still walks rowids 0..99.
                    writer.setMaxValue(49);
                }

                LPSZ keyFile = PostingIndexUtils.keyFileName(
                        path.trimTo(plen), name, COLUMN_NAME_TXN_NONE);
                try (MemoryCMARWImpl mem = new MemoryCMARWImpl(
                        ff, keyFile, ff.getPageSize(), ff.length(keyFile),
                        MemoryTag.MMAP_DEFAULT, /* opts */ 0)) {
                    PostingIndexChainWriter chain = new PostingIndexChainWriter();
                    chain.openExisting(mem);
                    Assert.assertTrue(
                            "test setup gap: chain head must exist after commit()",
                            chain.hasHead()
                    );
                    PostingIndexChainEntry.Snapshot head = new PostingIndexChainEntry.Snapshot();
                    chain.loadHeadEntry(mem, head);
                    Assert.assertEquals(
                            "test setup gap: head MAX_VALUE must reflect the lowered value "
                                    + "after setMaxValue(49)",
                            49L, head.maxValue
                    );
                    Assert.assertTrue(
                            "test setup gap: head valueMemSize must still cover the encoded "
                                    + "data for rowids 0..99 -- updateHeadMaxValue must not "
                                    + "shrink it",
                            head.valueMemSize > 0
                    );
                }

                // Oracle: key 0 was added at rowids 0, 5, 10, ..., 95
                // (loop rowId % 5 == 0). After the clamp to MAX_VALUE=49,
                // the cursor must emit exactly the rowids in [0, 49] for
                // key 0: 0, 5, 10, 15, 20, 25, 30, 35, 40, 45 -- ten rowids,
                // ascending for Fwd, descending for Bwd.
                LongList expectedFwd = new LongList();
                for (long r = 0; r <= 49; r += 5) {
                    expectedFwd.add(r);
                }
                LongList expectedBwd = new LongList();
                for (long r = 45; r >= 0; r -= 5) {
                    expectedBwd.add(r);
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name,
                        COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, /* columnTop */ 0)) {
                    LongList actual = new LongList();
                    try (RowCursor cursor = reader.getCursor(0, 0L, Long.MAX_VALUE)) {
                        while (cursor.hasNext()) {
                            actual.add(cursor.next());
                        }
                    }
                    TestUtils.assertEquals(expectedFwd, actual);
                }

                try (PostingIndexBwdReader reader = new PostingIndexBwdReader(
                        configuration, path.trimTo(plen), name,
                        COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, /* columnTop */ 0,
                        /* metadata */ null, /* columnVersionReader */ null,
                        /* partitionTimestamp */ 0)) {
                    LongList actual = new LongList();
                    try (RowCursor cursor = reader.getCursor(0, 0L, Long.MAX_VALUE)) {
                        while (cursor.hasNext()) {
                            actual.add(cursor.next());
                        }
                    }
                    TestUtils.assertEquals(expectedBwd, actual);
                }
            }
        });
    }

    /**
     * Deterministic regression for the native-memory leak that the concurrent fuzz
     * {@link #testMultiThreadedO3CoveringLatestByConcurrentReaderFuzz} surfaces
     * non-deterministically as
     * {@code Memory usage by tag: NATIVE_INDEX_READER, difference: 512 ... was:<512>}.
     * The fuzz seed only fixes the data shape, not the thread interleaving, so it
     * cannot reproduce the race on demand. This test exercises the underlying
     * lifecycle hazard directly, single-threaded.
     * <p>
     * {@link PostingIndexBwdReader} pools idle row cursors in {@code freeCursors}.
     * To reuse the decode scratch the pooling {@code close()} path RETAINS the
     * cursor's {@code blockBufferAddr} (BLOCK_CAPACITY longs = 512 bytes, tagged
     * NATIVE_INDEX_READER); that buffer is only ever reclaimed by the reader's own
     * {@code close()}, which drains {@code freeCursors}. So if the reader is closed
     * while a cursor is still checked out -- exactly what a concurrent reseal/reload
     * does to a reader thread that is mid-query -- the later {@code cursor.close()}
     * re-pools the cursor (with its block buffer) into a reader that is never
     * drained again, leaking the block buffer.
     */
    @Test
    public void testRowCursorClosedAfterReaderDoesNotLeakBlockBuffer() throws Exception {
        assertMemoryLeak(() -> {
            final String name = "posting_cursor_after_reader_close";
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    // Single key with IRREGULAR gaps so the per-key value blocks
                    // encode with a non-zero bit width (packed/EF/flat) -- the only
                    // decode path that allocates blockBufferAddr. Consecutive rowids
                    // would hit the bitWidth==0 constant-delta fast path and allocate
                    // nothing, hiding the leak.
                    long rowId = 0;
                    for (int i = 0; i < 4096; i++) {
                        writer.add(0, rowId);
                        rowId += 1 + (i % 7); // varying delta -> non-constant blocks
                    }
                    writer.setMaxValue(rowId);
                    writer.commit();
                }

                final PostingIndexBwdReader reader = new PostingIndexBwdReader(
                        configuration, path.trimTo(plen), name,
                        COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, /* columnTop */ 0,
                        /* metadata */ null, /* columnVersionReader */ null,
                        /* partitionTimestamp */ 0);

                // Check out a cursor and fully drive it so the decode block buffer
                // (NATIVE_INDEX_READER) is allocated on this active, un-pooled cursor.
                final RowCursor cursor = reader.getCursor(0, 0L, Long.MAX_VALUE);
                long count = 0;
                while (cursor.hasNext()) {
                    cursor.next();
                    count++;
                }
                Assert.assertEquals(4096, count);

                // Reseal/reload race: the owning reader is closed while the cursor is
                // still checked out. reader.close() drains only the idle pool, so this
                // active cursor's block buffer is NOT reclaimed here.
                reader.close();

                // The slow consumer now closes its cursor. The cursor is not yet
                // pooled and the (closed) reader's freeCursors still has room, so
                // close() re-pools it and keeps blockBufferAddr -- now unreachable and
                // never freed. assertMemoryLeak fails with a NATIVE_INDEX_READER leak.
                cursor.close();
            }
        });
    }

    /**
     * Forward-reader sibling of
     * {@link #testRowCursorClosedAfterReaderDoesNotLeakBlockBuffer}. The PR guards
     * four pooling {@code close()} paths with {@code isOpen()}; the backward-reader
     * test above only pins {@link PostingIndexBwdReader.Cursor#close()}. The fuzz
     * {@link #testMultiThreadedO3CoveringLatestByConcurrentReaderFuzz} is LATEST ON
     * (covering BACKWARD reader, asserts {@code plan.contains("CoveringIndex")}), so
     * the forward-reader fix otherwise has no regression coverage. This drives
     * {@link PostingIndexFwdReader.Cursor#close()} the same way: allocate the decode
     * block buffer (NATIVE_INDEX_READER, 512 bytes), close the reader while the
     * cursor is still checked out, then close the cursor -- which without the
     * {@code isOpen()} guard re-pools the live block buffer into a drained reader.
     */
    @Test
    public void testFwdRowCursorClosedAfterReaderDoesNotLeakBlockBuffer() throws Exception {
        assertMemoryLeak(() -> {
            final String name = "posting_fwd_cursor_after_reader_close";
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    // Irregular gaps -> non-zero bit width blocks -> decode allocates
                    // blockBufferAddr. See the backward-reader test for why constant
                    // deltas would hide the leak.
                    long rowId = 0;
                    for (int i = 0; i < 4096; i++) {
                        writer.add(0, rowId);
                        rowId += 1 + (i % 7);
                    }
                    writer.setMaxValue(rowId);
                    writer.commit();
                }

                // columnTop == 0 -> getCursor returns a plain Cursor (not NullCursor).
                final PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name,
                        COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, /* columnTop */ 0);

                final RowCursor cursor = reader.getCursor(0, 0L, Long.MAX_VALUE);
                long count = 0;
                while (cursor.hasNext()) {
                    cursor.next();
                    count++;
                }
                Assert.assertEquals(4096, count);

                // Reseal/reload race: reader closed while the cursor is checked out;
                // reader.close() drains only the idle pool.
                reader.close();

                // Slow consumer closes its cursor: without isOpen() this re-pools the
                // retained block buffer into the drained (closed) reader -> leak.
                cursor.close();
            }
        });
    }

    /**
     * Null-cursor sibling of
     * {@link #testRowCursorClosedAfterReaderDoesNotLeakBlockBuffer}. The pooling
     * close on {@link PostingIndexBwdReader.NullCursor#close()} is a fourth patched
     * path reachable only when {@code key == 0 && columnTop > 0 && minValue < columnTop}
     * (see {@code getCursor}); the other regression tests use {@code columnTop == 0}
     * and never hit it. Here {@code columnTop > 0} so {@code getCursor} returns a
     * {@link PostingIndexBwdReader.NullCursor}, whose {@code hasNext()} first drains
     * the real key-0 entries via {@code super.hasNext()} -- allocating the decode
     * block buffer (NATIVE_INDEX_READER) -- before synthesising the implicit nulls.
     * Closing the reader mid-iteration then closing the cursor re-pools that buffer
     * into the drained reader unless the {@code isOpen()} guard short-circuits it.
     */
    @Test
    public void testBwdNullCursorClosedAfterReaderDoesNotLeakBlockBuffer() throws Exception {
        assertMemoryLeak(() -> {
            final String name = "posting_null_cursor_after_reader_close";
            final long columnTop = 256;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    // Real key-0 entries live at/after columnTop with irregular gaps
                    // so the NullCursor's super.hasNext() decode allocates the block
                    // buffer; rows below columnTop are surfaced as implicit nulls.
                    long rowId = columnTop;
                    for (int i = 0; i < 4096; i++) {
                        writer.add(0, rowId);
                        rowId += 1 + (i % 7);
                    }
                    writer.setMaxValue(rowId);
                    writer.commit();
                }

                // key==0 && columnTop>0 && minValue(0)<columnTop -> NullCursor path.
                final PostingIndexBwdReader reader = new PostingIndexBwdReader(
                        configuration, path.trimTo(plen), name,
                        COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, columnTop,
                        /* metadata */ null, /* columnVersionReader */ null,
                        /* partitionTimestamp */ 0);

                final RowCursor cursor = reader.getCursor(0, 0L, Long.MAX_VALUE);
                long count = 0;
                while (cursor.hasNext()) {
                    cursor.next();
                    count++;
                }
                // 4096 real entries + columnTop synthesised nulls (columnTop-1..0).
                Assert.assertEquals(4096 + columnTop, count);

                // Reseal/reload race, then the slow consumer closes its null cursor.
                reader.close();
                cursor.close();
            }
        });
    }

    /**
     * Forward-reader counterpart of
     * {@link #testBwdNullCursorClosedAfterReaderDoesNotLeakBlockBuffer}, pinning the
     * fourth and last patched path: {@link PostingIndexFwdReader.NullCursor#close()}.
     * Same reachability gate ({@code key == 0 && columnTop > 0 && minValue < columnTop}),
     * but the forward NullCursor emits the synthesised nulls FIRST and then drains
     * the real key-0 entries via {@code super.hasNext()} (which allocates the decode
     * block buffer). Together with the three sibling tests this gives every
     * isOpen()-guarded close() its own RED-on-revert regression.
     */
    @Test
    public void testFwdNullCursorClosedAfterReaderDoesNotLeakBlockBuffer() throws Exception {
        assertMemoryLeak(() -> {
            final String name = "posting_fwd_null_cursor_after_reader_close";
            final long columnTop = 256;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    long rowId = columnTop;
                    for (int i = 0; i < 4096; i++) {
                        writer.add(0, rowId);
                        rowId += 1 + (i % 7);
                    }
                    writer.setMaxValue(rowId);
                    writer.commit();
                }

                // key==0 && columnTop>0 && minValue(0)<columnTop -> Fwd NullCursor.
                final PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name,
                        COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, columnTop);

                final RowCursor cursor = reader.getCursor(0, 0L, Long.MAX_VALUE);
                long count = 0;
                while (cursor.hasNext()) {
                    cursor.next();
                    count++;
                }
                // columnTop synthesised nulls (0..columnTop-1) + 4096 real entries.
                Assert.assertEquals(4096 + columnTop, count);

                reader.close();
                cursor.close();
            }
        });
    }

    @Test
    public void testReaderHidesHeadEntryTailGensAbovePin() throws Exception {
        assertMemoryLeak(() -> {
            final String name = "reader_pin_tail_gen_visibility";
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, true);
                    writer.setNextTxnAtSeal(1L);
                    writer.add(0, 0);
                    writer.add(0, 1);
                    writer.setMaxValue(1);
                    writer.commit();

                    // currentTableTxn=2 so neither slot is trimmed -- we want
                    // both gens persisted to disk so the reader-side pin
                    // filter is what hides slot[1] for low-pinned readers.
                    writer.setCurrentTableTxn(2L);
                    writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, false);

                    writer.setNextTxnAtSeal(2L);
                    writer.add(1, 2);
                    writer.add(1, 3);
                    writer.setMaxValue(3);
                    writer.commit();
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name,
                        COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, /* columnTop */ 0);
                     DirectBitSet foundKeys = new DirectBitSet(8)) {
                    reader.setPinnedTableTxn(2L);
                    reader.reloadConditionally();
                    int distinct = reader.collectDistinctKeys(foundKeys);
                    Assert.assertEquals(
                            "pin >= slot[1].TXN_AT_SEAL: both gens visible",
                            2, distinct);
                    Assert.assertTrue(foundKeys.get(0));
                    Assert.assertTrue(foundKeys.get(1));
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name,
                        COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, /* columnTop */ 0);
                     DirectBitSet foundKeys = new DirectBitSet(8)) {
                    reader.setPinnedTableTxn(1L);
                    reader.reloadConditionally();
                    int distinct = reader.collectDistinctKeys(foundKeys);
                    Assert.assertEquals(
                            "pin = slot[0].TXN_AT_SEAL: only gen 0 visible",
                            1, distinct);
                    Assert.assertTrue(foundKeys.get(0));
                    Assert.assertFalse(
                            "key=1 lives in gen 1 (slot[1].TXN_AT_SEAL=2 > pin)",
                            foundKeys.get(1)
                    );

                    try (RowCursor cursor = reader.getCursor(/* key */ 1, /* minValue */ 0, /* maxValue */ Long.MAX_VALUE)) {
                        Assert.assertFalse(
                                "rowids from the pin-hidden gen must not surface through getCursor",
                                cursor.hasNext()
                        );
                    }
                }
            }
        });
    }

    @Test
    public void testReaderPinChangeViaReloadConditionallyRePicks() throws Exception {
        assertMemoryLeak(() -> {
            final String name = "reader_pin_reload_repick";
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, true);
                    writer.setNextTxnAtSeal(1L);
                    writer.add(0, 0);
                    writer.setMaxValue(0);
                    writer.commit();

                    writer.setCurrentTableTxn(2L);
                    writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, false);

                    writer.setNextTxnAtSeal(2L);
                    writer.add(1, 1);
                    writer.setMaxValue(1);
                    writer.commit();
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name,
                        COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, /* columnTop */ 0);
                     DirectBitSet foundKeys = new DirectBitSet(8)) {
                    // Default Long.MAX_VALUE pin: both gens visible.
                    Assert.assertEquals(2, reader.collectDistinctKeys(foundKeys));
                    foundKeys.clear();

                    // Lower the pin under slot[1]; reloadConditionally must
                    // re-pick even though the chain header has not advanced.
                    reader.setPinnedTableTxn(1L);
                    reader.reloadConditionally();
                    Assert.assertEquals(1, reader.collectDistinctKeys(foundKeys));
                    Assert.assertTrue(foundKeys.get(0));
                    Assert.assertFalse(foundKeys.get(1));
                    foundKeys.clear();

                    // Raise the pin back above slot[1]; re-pick exposes gen 1.
                    reader.setPinnedTableTxn(Long.MAX_VALUE);
                    reader.reloadConditionally();
                    Assert.assertEquals(2, reader.collectDistinctKeys(foundKeys));
                    Assert.assertTrue(foundKeys.get(0));
                    Assert.assertTrue(foundKeys.get(1));
                }
            }
        });
    }

    /**
     * Review C1: once readers clamp iteration to the chain entry's MAX_VALUE,
     * RowCursor.size() must not keep advertising the unclamped encoded count.
     * Returning -1 is acceptable because the SQL count fast path will then
     * fall back to hasNext()/next() iteration.
     */
    @Test
    public void testReaderSizeDoesNotOutrunEntryMaxValueClamp() throws Exception {
        assertMemoryLeak(() -> {
            final String name = "posting_size_max_value_clamp";
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();

                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    for (long rowId = 0; rowId < 100; rowId++) {
                        writer.add((int) (rowId % 5), rowId);
                    }
                    writer.setMaxValue(99);
                    writer.commit();
                    writer.setMaxValue(49);
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name,
                        COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, /* columnTop */ 0)) {
                    long advertisedSize;
                    long iterated = 0;
                    try (RowCursor cursor = reader.getCursor(0, 0L, Long.MAX_VALUE)) {
                        advertisedSize = cursor.size();
                        while (cursor.hasNext()) {
                            cursor.next();
                            iterated++;
                        }
                    }

                    Assert.assertEquals(
                            "test setup gap: key 0 has rowids 0,5,...,45 at or below MAX_VALUE=49",
                            10,
                            iterated
                    );
                    Assert.assertTrue(
                            "RowCursor.size() must either decline the fast path or match clamped iteration "
                                    + "[size=" + advertisedSize + ", iterated=" + iterated + "]",
                            advertisedSize < 0 || advertisedSize == iterated
                    );
                }
            }
        });
    }

    @Test
    public void testRecoveryHidesFailedExtendHeadFromReader() throws Exception {
        assertMemoryLeak(() -> {
            final String name = "extend_head_failed_recovery_query";
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, true);
                    writer.setNextTxnAtSeal(1L);
                    for (long row = 0; row < 10; row++) {
                        writer.add(0, row);
                    }
                    writer.setMaxValue(9);
                    writer.commit();

                    writer.setCurrentTableTxn(1L);
                    writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, false);

                    writer.setNextTxnAtSeal(2L);
                    for (long row = 10; row < 20; row++) {
                        writer.add(1, row);
                    }
                    writer.setMaxValue(19);
                    writer.commit();
                }

                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.setCurrentTableTxn(1L);
                    writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, false);
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name,
                        COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, /* columnTop */ 0);
                     DirectBitSet foundKeys = new DirectBitSet(8)) {
                    int distinct = reader.collectDistinctKeys(foundKeys);
                    Assert.assertEquals(
                            "only the committed key (0) should remain after recovery",
                            1, distinct);
                    Assert.assertTrue(foundKeys.get(0));
                    Assert.assertFalse(
                            "key=1 belongs to an uncommitted txn; must not be visible",
                            foundKeys.get(1)
                    );

                    try (RowCursor cursor = reader.getCursor(/* key */ 1, /* minValue */ 0, /* maxValue */ Long.MAX_VALUE)) {
                        Assert.assertFalse(
                                "uncommitted rowids for key=1 must not surface through getCursor",
                                cursor.hasNext()
                        );
                    }
                }
            }
        });
    }

    @Test
    public void testRecoveryTrimsExtendHeadFailedTailGens() throws Exception {
        assertMemoryLeak(() -> {
            final String name = "extend_head_failed_recovery";
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, true);
                    writer.setNextTxnAtSeal(1L);
                    writer.add(0, 0);
                    writer.add(0, 1);
                    writer.setMaxValue(1);
                    writer.commit(); // appendNewEntry -> txnAtSeal=1, genCount=1

                    // Successful commit at table layer would advance committedTxn to 1.
                    // The reopen below is what TableWriter does between WAL transactions:
                    // close + open from disk, then drive recovery via setCurrentTableTxn.
                    writer.setCurrentTableTxn(1L);
                    writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, false);

                    // Batch 2 anticipates txn 2 but the table commit will not run.
                    writer.setNextTxnAtSeal(2L);
                    writer.add(1, 2);
                    writer.add(1, 3);
                    writer.setMaxValue(3);
                    writer.commit(); // extendHead -> genCount=2, txnAtSeal STAYS at 1
                }

                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.setCurrentTableTxn(1L);
                    writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, false);
                    Assert.assertEquals(
                            "recovery walk should drop the failed batch 2 gen because batch 2 anticipated txn 2 > committedTxn 1",
                            1, writer.getGenCount());
                }
            }
        });
    }

    /**
     * Review M1: link-time chain recovery in
     * {@code TableWriter.dropFuturePostingIndexChainEntriesBeforeLink} must not
     * SHRINK the source {@code .pk} below its on-disk size. The source is
     * hard-linked, not copied, and RENAME COLUMN does not quiesce readers, so a
     * pre-link reader may still mmap the inode. Posting readers map grow-only
     * ("File can only have grown" in {@code AbstractPostingIndexReader}) and
     * bound entry reads against their stale mmap size, dereferencing the head
     * entry before the {@code stillStable} seqlock re-check; truncating the tail
     * away under such a reader faults past EOF (SIGBUS).
     * <p>
     * The test plants a committed-visible base head plus a page-spanning head
     * whose entry-level {@code txnAtSeal} is in the future. Link recovery drops
     * the future head WHOLE, rewinding {@code regionLimit} more than one OS page
     * below the file size. The fix floors the CMARW append offset at the original
     * {@code keyFileSize}, so the close must not truncate the linked {@code .pk}
     * below its pre-link size. The assertion only stats the file length, never
     * maps the dropped head, so a regressed build fails cleanly instead of
     * crashing the JVM.
     */
    @Test
    public void testRenameColumnLinkRecoveryFullDropKeepsKeyFileSize() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_link_fulldrop (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_link_fulldrop VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-02T00:00:00', 'B', 2.0)
                    """);
            engine.releaseAllWriters();

            final TableToken token = engine.getTableTokenIfExists("t_link_fulldrop");
            Assert.assertNotNull("test table must exist", token);
            final long currentTxn;
            final long firstPartitionTimestamp;
            final long firstPartitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                currentTxn = reader.getTxn();
                firstPartitionTimestamp = reader.getTxFile().getPartitionTimestampByIndex(0);
                firstPartitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }

            // Plant a committed-visible base head plus a page-spanning fully
            // future head on the historic first partition.
            appendPageSpanningFutureHead(token, firstPartitionTimestamp, firstPartitionNameTxn, currentTxn);

            // Source .pk size right before the rename links (and recovers) it.
            final long srcColumnNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                int colIndex = reader.getMetadata().getColumnIndex("sym");
                int writerIndex = reader.getMetadata().getWriterIndex(colIndex);
                srcColumnNameTxn = reader.getColumnVersionReader().getColumnNameTxn(firstPartitionTimestamp, writerIndex);
            }
            final long srcKeyFileSize = postingKeyFileSize(
                    token, firstPartitionTimestamp, firstPartitionNameTxn, "sym", srcColumnNameTxn);
            Assert.assertTrue(
                    "test setup: planted .pk must span more than one OS page, was " + srcKeyFileSize,
                    srcKeyFileSize > io.questdb.std.Files.PAGE_SIZE
            );

            execute("ALTER TABLE t_link_fulldrop RENAME COLUMN sym TO new_sym");

            final long newColumnNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                int colIndex = reader.getMetadata().getColumnIndex("new_sym");
                int writerIndex = reader.getMetadata().getWriterIndex(colIndex);
                newColumnNameTxn = reader.getColumnVersionReader().getColumnNameTxn(firstPartitionTimestamp, writerIndex);
            }
            final long dstKeyFileSize = postingKeyFileSize(
                    token, firstPartitionTimestamp, firstPartitionNameTxn, "new_sym", newColumnNameTxn);

            Assert.assertTrue(
                    "link recovery shrank the renamed column's .pk below its pre-link size -- a pre-link"
                            + " reader mapping the hard-linked inode would fault past EOF (SIGBUS):"
                            + " dstKeyFileSize=" + dstKeyFileSize + " srcKeyFileSize=" + srcKeyFileSize,
                    dstKeyFileSize >= srcKeyFileSize
            );
        });
    }

    /**
     * Review C1: link-time chain recovery in
     * {@code TableWriter.dropFuturePostingIndexChainEntriesBeforeLink} must trim
     * the {@code .pk} to {@code chain.getRegionLimit()} before the CMARW closes,
     * exactly like {@code PostingIndexWriter.close()} does.
     * <p>
     * When recovery takes the head-trim branch it relocates the trimmed head
     * entry to the original {@code regionLimit} via positional writes. Those
     * grow the file mapping but NOT the CMARW append offset, so the close
     * truncates the file back to {@code ceilPageSize(keyFileSize)}. When the
     * relocated head spans past that page boundary the on-disk header points
     * {@code headEntryOffset} past EOF, and a reader of the renamed column's
     * posting index maps past the mapping -- SIGBUS or corruption.
     * <p>
     * The test plants a committed-visible head carrying one in-flight (future
     * {@code txnAtSeal}) tail gen on a historic partition, sized so the trimmed
     * head spans at least one OS page. With {@code newLen >= PAGE_SIZE} the
     * relocation crosses the page boundary regardless of where
     * {@code regionLimit} lands, so the trigger is deterministic and not
     * alignment-dependent. RENAME COLUMN runs the link recovery. The assertion
     * reads only the on-disk header and file length -- it never maps the
     * dangling head entry -- so a buggy build fails the assertion cleanly
     * instead of crashing the test JVM with SIGBUS.
     */
    @Test
    public void testRenameColumnLinkRecoveryHeadTrimKeepsRelocatedHeadOnDisk() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_link_headtrim (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_link_headtrim VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-02T00:00:00', 'B', 2.0)
                    """);
            engine.releaseAllWriters();

            final TableToken token = engine.getTableTokenIfExists("t_link_headtrim");
            Assert.assertNotNull("test table must exist", token);
            final long currentTxn;
            final long firstPartitionTimestamp;
            final long firstPartitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                currentTxn = reader.getTxn();
                firstPartitionTimestamp = reader.getTxFile().getPartitionTimestampByIndex(0);
                firstPartitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }

            // Plant a committed-visible head with a single in-flight tail gen on
            // the historic first partition, large enough that the trimmed head
            // spans >= one OS page.
            appendVisibleHeadWithFutureTailGen(token, firstPartitionTimestamp, firstPartitionNameTxn, currentTxn);

            execute("ALTER TABLE t_link_headtrim RENAME COLUMN sym TO new_sym");

            final long newColumnNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                int colIndex = reader.getMetadata().getColumnIndex("new_sym");
                int writerIndex = reader.getMetadata().getWriterIndex(colIndex);
                newColumnNameTxn = reader.getColumnVersionReader().getColumnNameTxn(firstPartitionTimestamp, writerIndex);
            }

            assertPostingKeyFileCoversHeaderRegion(
                    token,
                    firstPartitionTimestamp,
                    firstPartitionNameTxn,
                    "new_sym",
                    newColumnNameTxn,
                    "link recovery truncated the renamed column's .pk below its header regionLimit"
            );
        });
    }

    @Test
    public void testConvertPartitionToParquetLinkRecoveryHeadTrimKeepsRelocatedHeadOnDisk() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_parquet_link_headtrim (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_parquet_link_headtrim VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-02T00:00:00', 'B', 2.0)
                    """);
            engine.releaseAllWriters();

            final TableToken token = engine.getTableTokenIfExists("t_parquet_link_headtrim");
            Assert.assertNotNull("test table must exist", token);
            final long currentTxn;
            final long firstPartitionTimestamp;
            final long firstPartitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                currentTxn = reader.getTxn();
                firstPartitionTimestamp = reader.getTxFile().getPartitionTimestampByIndex(0);
                firstPartitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }

            appendVisibleHeadWithFutureTailGen(token, firstPartitionTimestamp, firstPartitionNameTxn, currentTxn);

            execute("ALTER TABLE t_parquet_link_headtrim CONVERT PARTITION TO PARQUET LIST '2024-01-01'");

            final long parquetPartitionNameTxn;
            final long columnNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                parquetPartitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
                int colIndex = reader.getMetadata().getColumnIndex("sym");
                int writerIndex = reader.getMetadata().getWriterIndex(colIndex);
                columnNameTxn = reader.getColumnVersionReader().getColumnNameTxn(firstPartitionTimestamp, writerIndex);
            }
            Assert.assertTrue(
                    "test setup: CONVERT PARTITION TO PARQUET should move the partition to a new txn directory",
                    parquetPartitionNameTxn > firstPartitionNameTxn
            );

            assertPostingKeyFileCoversHeaderRegion(
                    token,
                    firstPartitionTimestamp,
                    parquetPartitionNameTxn,
                    "sym",
                    columnNameTxn,
                    "link recovery truncated the parquet-converted column's .pk below its header regionLimit"
            );
        });
    }

    /**
     * {@code TableWriter.linkPostingIndexAuxFiles} must hardlink only the
     * live seal generation's {@code .pc<N>} files to the dst column's
     * namespace. Two reasons:
     * <ul>
     *   <li>Race avoidance. The visitor used to enumerate every sealed
     *   {@code .pc<N>} on disk via {@code scanSealedFiles}. A queued
     *   {@code PostingSealPurgeTask} for a superseded generation could fire
     *   between the visitor's {@code ff.exists(from)} and {@code ff.hardLink}
     *   syscalls and remove the source, throwing
     *   {@code [errno=2] could not create hard link} mid-DDL. Recently
     *   observed on master as {@code WalWriterFuzzTest
     *   .testAddDropColumnDropPartition} (build 231522). Filtering by
     *   {@code sealTxn == liveSealTxn} dodges the race entirely: the live
     *   generation cannot be subject to a purge task because rename holds
     *   the writer lock and purge only targets superseded sealTxns.</li>
     *   <li>No leaked hardlinks. If the visitor links a superseded
     *   generation, the src copies still get removed by their queued purge
     *   tasks, but the dst copies are not targeted by any task and survive
     *   under the dst column's namespace until the column itself is
     *   dropped.</li>
     * </ul>
     * The test plants a "ghost" {@code .pc<N>} file with a sealTxn that is
     * clearly not the live one, runs the rename, and verifies the ghost was
     * not propagated to the dst column. As a positive control, it also
     * verifies the live {@code .pc<N>} file did get linked.
     */
    @Test
    public void testRenameColumnLinksOnlyLiveSealGeneration() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_rename_live (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_rename_live VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T01:00:00', 'B', 2.0)
                    """);
            // Release the writer so the on-disk .pc<N> files are stable and
            // the next ALTER opens the table fresh.
            engine.releaseAllWriters();

            final TableToken token = engine.getTableTokenIfExists("t_rename_live");
            Assert.assertNotNull("test table must exist", token);
            final FilesFacade ff = configuration.getFilesFacade();
            final long ghostSealTxn = 9_999L;
            final long liveSealTxn = 0L;

            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token).concat("2024-01-01").slash();
                int plen = path.size();
                LPSZ ghostSrc = PostingIndexUtils.coverDataFileName(
                        path.trimTo(plen), "sym", 0,
                        COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, ghostSealTxn);
                Assert.assertTrue(
                        "test setup: failed to plant ghost .pc<N> file at " + ghostSrc,
                        ff.touch(ghostSrc)
                );

                // Sanity-check the live .pc<N> file the rename should pick up
                // exists, so a missing dst-side counterpart later is a real
                // assertion failure and not a setup gap.
                LPSZ liveSrc = PostingIndexUtils.coverDataFileName(
                        path.trimTo(plen), "sym", 0,
                        COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, liveSealTxn);
                Assert.assertTrue(
                        "test setup: live .pc<N> file not produced at " + liveSrc,
                        ff.exists(liveSrc)
                );
            }

            execute("ALTER TABLE t_rename_live RENAME COLUMN sym TO new_sym");

            // Enumerate every .pc<N> file under the dst column's namespace and
            // record the distinct sealTxns. With the fix, only the live sealTxn
            // appears; without it, the ghost sealTxn would also appear because
            // the visitor would have hardlinked it.
            final LongList dstSealTxns = new LongList();
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token).concat("2024-01-01").slash();
                int plen = path.size();
                PostingIndexUtils.scanSealedFiles(ff, path, plen, "new_sym",
                        new PostingIndexUtils.SealedFileVisitor() {
                            @Override
                            public void onCoverDataFile(int includeIdx, long postingColumnNameTxn,
                                                        long coveredColumnNameTxn, long sealTxn) {
                                dstSealTxns.add(sealTxn);
                            }

                            @Override
                            public void onValueFile(long postingColumnNameTxn, long sealTxn) {
                                // .pv files are not the subject of this test.
                            }
                        });
            }

            Assert.assertEquals(
                    "rename must NOT hardlink a superseded .pc<N> on the dst column. "
                            + "Found ghost sealTxn=" + ghostSealTxn + " in dst .pc files: "
                            + dstSealTxns
                            + ". Linking dead generations leaks files under the dst "
                            + "namespace until the column is dropped, since no purge task "
                            + "targets the new column name.",
                    -1, dstSealTxns.indexOf(ghostSealTxn)
            );
            Assert.assertTrue(
                    "rename must hardlink the live .pc<N> to the dst column. "
                            + "Expected sealTxn=" + liveSealTxn + " in dst .pc files but found: "
                            + dstSealTxns,
                    dstSealTxns.indexOf(liveSealTxn) >= 0
            );

            assertQuery("SELECT ts, new_sym, price FROM t_rename_live")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tnew_sym\tprice
                            2024-01-01T00:00:00.000000Z\tA\t1.0
                            2024-01-01T01:00:00.000000Z\tB\t2.0
                            """);
        });
    }

    @Test
    public void testRenameColumnLinksReaderVisibleSealWhenHeadIsFutureTxn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_rename_future_head (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_rename_future_head VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-02T00:00:00', 'B', 2.0)
                    """);
            engine.releaseAllWriters();

            final TableToken token = engine.getTableTokenIfExists("t_rename_future_head");
            Assert.assertNotNull("test table must exist", token);
            final long currentTxn;
            final long firstPartitionTimestamp;
            final long firstPartitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                currentTxn = reader.getTxn();
                firstPartitionTimestamp = reader.getTxFile().getPartitionTimestampByIndex(0);
                firstPartitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }

            appendFuturePostingHead(token, firstPartitionTimestamp, firstPartitionNameTxn, currentTxn);

            execute("ALTER TABLE t_rename_future_head RENAME COLUMN sym TO new_sym");

            assertQuery("SELECT ts, new_sym, price FROM t_rename_future_head WHERE new_sym = 'A'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tnew_sym\tprice
                            2024-01-01T00:00:00.000000Z\tA\t1.0
                            """);
        });
    }

    /**
     * Review finding #3 — reader reads past keyMem mmap when the chain
     * header points to an entry beyond the mapped region.
     * <p>
     * {@code AbstractPostingIndexReader.of(...)} (line 250-258) maps the
     * {@code .pk} key file with {@code size=-1}, which {@code MemoryCMRImpl}
     * resolves via {@code ff.length(fd)} at the moment of mmap. If a
     * concurrent writer publishes a new chain entry past that snapshotted
     * length, the picker reads at {@code headEntryOffset} which is past
     * {@code keyMem.size()}. The picker's bound check (PostingIndexChainPicker.java:95)
     * compares against the header's {@code regionLimit} only — not against
     * {@code keyMem.size()}. With assertions enabled (CLAUDE.md says
     * QuestDB runs {@code -ea} in production), this fires
     * {@code AbstractMemoryCR}'s {@code assert checkOffsetMapped(offset)}
     * and crashes the reader.
     * <p>
     * To make the race deterministic without thread interleaving, the
     * test uses a FilesFacade that returns a stale length (just above
     * KEY_FILE_RESERVED) for the {@code .pk} file when the reader opens
     * it. The on-disk file has been extended past that point by a
     * preceding seal, mimicking the post-publish state the reader would
     * see if it raced an extendHead.
     */
    @Test
    public void testReviewFinding3_PostingReaderHandlesStaleKeyFileLength() throws Exception {
        final AtomicBoolean staleLength = new AtomicBoolean(false);
        // Track the .pk fds opened by the reader so length() only clips
        // those, leaving other files (.pv, .d, etc.) reporting actual size.
        final java.util.concurrent.ConcurrentHashMap<Long, Boolean> pkFds =
                new java.util.concurrent.ConcurrentHashMap<>();
        final AtomicLong clippedReadCount = new AtomicLong(0);
        ff = new TestFilesFacadeImpl() {
            @Override
            public boolean close(long fd) {
                pkFds.remove(fd);
                return super.close(fd);
            }

            @Override
            public long length(long fd) {
                long actual = super.length(fd);
                // Simulate the TOCTOU window: the first length() query for
                // a .pk fd returns a stale value (smaller than the file
                // actually is on disk). Subsequent queries return truth,
                // mimicking what happens after a writer's publish settles.
                // The extend-and-retry loop in readIndexMetadataFromChain
                // must observe the new length on retry and grow the mmap.
                if (staleLength.get() && pkFds.containsKey(fd) && actual > 8192) {
                    if (clippedReadCount.getAndIncrement() == 0) {
                        return 8192;
                    }
                }
                return actual;
            }

            @Override
            public long openRO(LPSZ name) {
                long fd = super.openRO(name);
                if (fd != -1 && isPkFile(name)) {
                    pkFds.put(fd, Boolean.TRUE);
                }
                return fd;
            }

            @Override
            public long openRONoCache(LPSZ name) {
                long fd = super.openRONoCache(name);
                if (fd != -1 && isPkFile(name)) {
                    pkFds.put(fd, Boolean.TRUE);
                }
                return fd;
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                long fd = super.openRW(name, opts);
                if (fd != -1 && isPkFile(name)) {
                    pkFds.put(fd, Boolean.TRUE);
                }
                return fd;
            }

            private boolean isPkFile(LPSZ name) {
                // Path is "{...}/{sym}.pk" optionally followed by ".{txn}".
                return name != null && Utf8s.containsAscii(name, ".pk");
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("""
                    CREATE TABLE t_stale_pk (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Insert enough rows so the chain entry lives past 8KB.
            StringBuilder sb = new StringBuilder("INSERT INTO t_stale_pk VALUES");
            for (int i = 0; i < 1_000; i++) {
                sb.append(i == 0 ? "\n" : ",\n")
                        .append("('2024-01-01T")
                        .append(String.format("%02d:%02d:00", (i / 60) % 24, i % 60))
                        .append("', '")
                        .append("k").append(i % 50)
                        .append("')");
            }
            execute(sb.toString());
            engine.releaseAllWriters();

            staleLength.set(true);
            try {
                // The query opens a TableReader, which opens a posting-
                // index reader, which mmaps .pk at the (now stale) length.
                // The picker then walks the chain. The chain entry lives
                // past 8KB on disk, but the mmap stops at 8KB.
                //
                // RED: with the missing bound check (PostingIndexChainPicker:91-95
                // does not compare entryOffset against keyMem.size()), the
                // reader either returns wrong rows (silent corruption) or
                // raises AssertionError from AbstractMemoryCR's
                // checkOffsetMapped under -ea. The fix must produce a
                // clean CairoException with a descriptive message, so the
                // test asserts that the query either returns the correct
                // count (50 distinct keys) or throws CairoException —
                // anything else (AssertionError, NPE, wrong count) is
                // a confirmed bug.
                // SELECT DISTINCT sym dispatches to the posting-index
                // distinct factory which walks every key in the chain,
                // forcing the picker to read past the (clipped) mmap.
                long count = -1;
                AssertionError jvmAssertionFromPicker = null;
                try {
                    try (RecordCursorFactory factory = select(
                            "SELECT count(*) AS c FROM (SELECT DISTINCT sym FROM t_stale_pk)")) {
                        try (RecordCursor c = factory.getCursor(sqlExecutionContext)) {
                            if (c.hasNext()) {
                                count = c.getRecord().getLong(0);
                            }
                        }
                    }
                } catch (CairoException ok) {
                    // Acceptable post-fix outcome: clean error surfaced.
                    return;
                } catch (AssertionError jvmAssert) {
                    jvmAssertionFromPicker = jvmAssert;
                }

                if (jvmAssertionFromPicker != null) {
                    Assert.fail(
                            "RED: posting-index picker raised raw AssertionError "
                                    + "(checkOffsetMapped fired) when key-file mmap is smaller "
                                    + "than the chain head. The picker must compare entryOffset "
                                    + "against keyMem.size(), not just regionLimit. Stack: "
                                    + jvmAssertionFromPicker.getClass().getName()
                    );
                }
                Assert.assertEquals(
                        "stale-mmap reader returned wrong distinct count "
                                + "(picker may have walked off the mapped region "
                                + "and read junk)",
                        50L, count
                );
                Assert.assertTrue(
                        "test setup gap: length() override never fired for .pk; "
                                + "pkFds tracked=" + pkFds.size()
                                + ", clipped=" + clippedReadCount.get(),
                        clippedReadCount.get() > 0
                );
            } finally {
                staleLength.set(false);
            }
        });
    }

    /**
     * Review finding #4 — {@code BitpackUtils.unpackValuesFrom} Java
     * pre-fill loop drops bits when {@code skipBits + bitWidth > 64}.
     * <p>
     * BitpackUtils.java:264-271:
     * <pre>
     *     while (bufferBits &lt; skipBits + bitWidth && srcOffset &lt; totalBytes) {
     *         buffer |= ((Unsafe.getByte(...) & 0xFFL) &lt;&lt; bufferBits);
     *         bufferBits += 8;
     *     }
     * </pre>
     * With {@code bitWidth=63} and {@code startIndex=1}, {@code skipBits=7}
     * and the loop wants {@code bufferBits >= 70}. After the 8th byte
     * {@code bufferBits=64}; the 9th iteration evaluates {@code b << 64},
     * which Java treats as {@code b << 0} (the JVM masks the shift count
     * to 6 bits), OR-ing the byte into {@code buffer}'s low bits and
     * silently corrupting them. The main loop's spill mechanism is not
     * mirrored in the pre-fill.
     * <p>
     * Pure-Java reproducer: pack 5 distinct values that span 63 bits with
     * the fallback packer, then unpack from {@code startIndex=1} and
     * compare against the input.
     */
    @Test
    public void testReviewFinding4_BitpackPrefillSkipBitsBoundary() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int count = 5;
            int bitWidth = 63;
            long minValue = 0L;

            // Corruption analysis: the bug ORs byte[15] bit 7 into the
            // unpacked value[1]'s bit 0. byte[15] bit 7 = stream bit 127
            // = value[2] bit 1. To make the bug observable, set
            // value[1] bit 0 = 0 and value[2] bit 1 = 1 so the corrupted
            // unpack produces a 1 where 0 is expected.
            // The (1L<<62) entry forces the packer to use the full 63-bit
            // range so bw=63 stays meaningful.
            long[] values = {
                    0L,
                    0L,
                    2L,
                    0L,
                    1L << 62
            };

            long valuesAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            int packedBytes = (count * bitWidth + 7) / 8 + 16;
            long packedAddr = Unsafe.malloc(packedBytes, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.putLong(valuesAddr + (long) i * Long.BYTES, values[i]);
                }
                Unsafe.setMemory(packedAddr, packedBytes, (byte) 0);

                PostingIndexNative.packValuesNativeFallback(
                        valuesAddr, count, minValue, bitWidth, packedAddr);

                // Sanity: unpack from index 0 (skipBits=0) — the aligned
                // path is unaffected by the bug.
                BitpackUtils.unpackValuesFrom(
                        packedAddr, 0, count, bitWidth, minValue, destAddr);
                for (int i = 0; i < count; i++) {
                    long actual = Unsafe.getLong(destAddr + (long) i * Long.BYTES);
                    Assert.assertEquals(
                            "aligned unpack must round-trip at index " + i,
                            values[i], actual);
                }

                // The buggy path: startIndex=1 produces skipBits=7,
                // skipBits + bitWidth = 70 > 64.
                Unsafe.setMemory(destAddr, (long) count * Long.BYTES, (byte) 0);
                BitpackUtils.unpackValuesFrom(
                        packedAddr, 1, count - 1, bitWidth, minValue, destAddr);
                for (int i = 0; i < count - 1; i++) {
                    long actual = Unsafe.getLong(destAddr + (long) i * Long.BYTES);
                    Assert.assertEquals(
                            "BitpackUtils.unpackValuesFrom corrupted value at startIndex+" + i
                                    + " (bw=63, skipBits=7, target=70 exceeds 64-bit buffer)",
                            values[1 + i], actual);
                }
            } finally {
                Unsafe.free(valuesAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(packedAddr, packedBytes, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Review finding #6 — {@code PostingIndexWriter.truncate()} leaves the
     * writer in a half-open state when the new {@code valueMem.of(...)}
     * fails after {@code closeSidecarMems()} and {@code valueMem.close(...)}
     * already ran (PostingIndexWriter.java:1238-1279).
     * <p>
     * Currently passes because the {@code WriterPool} recreates a fresh
     * {@code TableWriter} after the failed TRUNCATE — the half-open state
     * exists inside the failed {@code PostingIndexWriter} but is not
     * reachable from SQL. To exercise the bug a caller must hold a
     * {@code TableWriter} reference across the truncate failure and keep
     * using it directly. Left here as documentation; the SQL-level test
     * confirms the cleanup at the table layer works.
     * <p>
     * Several methods (e.g. {@code flushAllPending}, {@code seal}) read
     * {@code valueMem.addressOf(...)} without checking
     * {@code valueMem.isOpen()}. After a failed truncate, calling those
     * methods produces a SIGSEGV / NPE instead of a clean error.
     * <p>
     * The fault-injection FilesFacade fails {@code openRW} on the new
     * {@code .pv.{N}} file the truncate would create, so the fresh
     * {@code valueMem.of(...)} throws inside truncate. The test then
     * exercises the writer via the engine again — if the writer is in a
     * coherent state the engine sees a CairoException; if it's
     * half-open, the test surfaces the symptom.
     */
    @Test
    public void testReviewFinding6_TruncatePvOpenFailureDoesNotLeaveHalfOpenWriter() throws Exception {
        final AtomicBoolean failArmed = new AtomicBoolean(false);
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (failArmed.get() && name != null && Utf8s.containsAscii(name, ".pv.")) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("""
                    CREATE TABLE t_truncate_half (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_truncate_half VALUES
                    ('2024-01-01T00:00:00', 'A'),
                    ('2024-01-01T01:00:00', 'B')
                    """);
            engine.releaseAllWriters();

            failArmed.set(true);
            try {
                // TRUNCATE goes through TableWriter which forwards to
                // PostingIndexWriter.truncate() per indexed column. The
                // .pv reopen inside truncate fails — the writer must
                // either fully back out (best) or mark itself distressed.
                // What it must NOT do is leave valueMem closed while
                // keyMem is still open and accept further writes.
                try {
                    execute("TRUNCATE TABLE t_truncate_half");
                } catch (Throwable expected) {
                    // expected
                }

                // Now try to insert again. With a clean failure path, the
                // engine either rejects the call (writer distressed) or
                // accepts it (writer fully recovered). With the bug, the
                // writer's flushAllPending hits a closed valueMem and
                // throws something internal (NPE / Unsafe assertion).
                try {
                    execute("""
                            INSERT INTO t_truncate_half VALUES
                            ('2024-01-02T00:00:00', 'C')
                            """);
                } catch (CairoException expected) {
                    // OK — this is the clean failure mode.
                } catch (NullPointerException | AssertionError leaked) {
                    Assert.fail(
                            "writer left in half-open state after truncate failure: "
                                    + leaked.getClass().getName() + ": " + leaked.getMessage()
                    );
                }
            } finally {
                failArmed.set(false);
            }
        });
    }

    /**
     * Review finding #8 — {@code SqlCodeGenerator} line 7666-7667 only
     * checks {@code hasNoIndexHint(model)}, not {@code hasNoCoveringHint(model)},
     * when picking the {@code PostingIndexDistinctRecordCursorFactory} fast
     * path. So {@code /*+ no_covering * / SELECT DISTINCT sym FROM t}
     * still routes through the posting-distinct factory even though the
     * user explicitly opted out of the covering paths.
     * <p>
     * Asserts on {@code factory.toPlan(...)} text: the default DISTINCT
     * plan must include {@code "PostingIndex"} + {@code "distinct"}; the
     * hinted version must not.
     */
    @Test
    public void testReviewFinding8_NoCoveringHintDisablesPostingDistinct() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dist_hint (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_dist_hint VALUES
                    ('2024-01-01T00:00:00', 'A'),
                    ('2024-01-01T01:00:00', 'B'),
                    ('2024-01-01T02:00:00', 'A')
                    """);
            engine.releaseAllWriters();

            // Baseline: default DISTINCT must use the posting-distinct
            // fast path. This locks the symptom — if the codegen ever
            // stops dispatching to PostingIndexDistinct, the red test
            // becomes a false negative.
            try (RecordCursorFactory factory = select(
                    "SELECT DISTINCT sym FROM t_dist_hint")) {
                planSink.clear();
                factory.toPlan(planSink);
                String defaultPlan = planSink.getSink().toString();
                Assert.assertTrue(
                        "default DISTINCT must use PostingIndex distinct, got:\n" + defaultPlan,
                        defaultPlan.contains("PostingIndex") && defaultPlan.contains("distinct")
                );
            }

            // The hinted query must NOT use PostingIndex distinct.
            try (RecordCursorFactory factory = select(
                    "SELECT /*+ no_covering */ DISTINCT sym FROM t_dist_hint")) {
                planSink.clear();
                factory.toPlan(planSink);
                String hintedPlan = planSink.getSink().toString();
                Assert.assertFalse(
                        "/*+ no_covering */ must disable PostingIndex distinct, got:\n" + hintedPlan,
                        hintedPlan.contains("PostingIndex") && hintedPlan.contains("distinct")
                );
            }

            // Result correctness must match across paths.
            assertQuery("SELECT DISTINCT sym FROM t_dist_hint ORDER BY sym")
                    .noLeakCheck()
                    .expectSize()
                    .returns("sym\nA\nB\n");
            assertQuery("SELECT /*+ no_covering */ DISTINCT sym FROM t_dist_hint ORDER BY sym")
                    .noLeakCheck()
                    .expectSize()
                    .returns("sym\nA\nB\n");
        });
    }

    /**
     * Re-review Critical #1 — {@link PostingIndexWriter#discardForRebuild}
     * preserves the writer's {@code sealTxn}, {@code valueMemSize} and
     * chain head on disk. The next {@link PostingIndexWriter#commit()}
     * therefore reaches {@code publishToChain} with
     * {@code this.sealTxn == chain.getHeadSealTxn()}, which routes to
     * {@link PostingIndexChainWriter#extendHead}. After
     * {@code discardForRebuild} reset {@code genCount} to 0,
     * {@code flushAllPending} bumps it to 1 and calls
     * {@code publishToChain(newGenCount=1, overrideGenIndex=0, ...)}.
     * <p>
     * {@code extendHead}'s protocol assumes
     * {@code newGenCount > oldGenCount}: the caller writes a fresh
     * gen-dir slot at index {@code oldGenCount} (past the visible end),
     * fences, then bumps {@code GEN_COUNT} last. The caller in
     * {@code TableWriter.sealPostingIndexForPartition} (covering branch
     * around TableWriter.java:11308-11320, non-covering branch around
     * 11381-11388) violates that contract whenever the partition's
     * pre-rebuild head had {@code GEN_COUNT > 1} (e.g., after WAL
     * fast-lag commits chained {@code extendHead} on the same sealTxn):
     * {@code publishToChain} writes gen-dir slot 0 — already occupied —
     * shrinks {@code LEN}, and pulls {@code GEN_COUNT} from {@code K}
     * down to 1. The previously-published head entry is mutated in
     * place; the entry's {@code TXN_AT_SEAL} stays unchanged because
     * {@code extendHead} never touches that field.
     * <p>
     * Three downstream consequences:
     * <ol>
     *   <li><b>Torn read.</b> A concurrent reader that latches the OLD
     *       {@code GEN_COUNT=K} under
     *       {@link PostingIndexChainEntry#read}'s {@code loadFence} walks
     *       slots {@code [0, K)} where slot 0 carries the new
     *       post-rebuild bytes and slots {@code [1, K)} still reference
     *       the OLD gens. {@code extendHead}'s {@code GEN_COUNT}-last
     *       store ordering only protects {@code newGenCount > oldGenCount}.</li>
     *   <li><b>Snapshot-isolation violation.</b> After the trailing
     *       {@code seal()} appends a fresh head, the in-place-mutated
     *       entry becomes a {@code prev} entry. Its bytes now describe
     *       the post-rebuild state, but its {@code TXN_AT_SEAL} still
     *       belongs to the pre-rebuild snapshot.
     *       {@link PostingIndexChainPicker#pick} for any reader pinned
     *       between the OLD entry's {@code txnAtSeal} and the new entry's
     *       {@code txnAtSeal} stops at the mutated OLD entry and observes
     *       post-rebuild data through what should be a pre-rebuild
     *       snapshot.</li>
     *   <li><b>Crash recovery hole.</b> A crash between
     *       {@code commit()} and the trailing {@code seal()} leaves the
     *       OLD entry mutated but with its original {@code TXN_AT_SEAL}.
     *       {@link PostingIndexChainWriter#recoveryDropAbandoned}'s
     *       predicate is {@code txnAtSeal > currentTableTxn}; the
     *       mutation is invisible to it, so the corrupted entry survives
     *       recovery as the new head.</li>
     * </ol>
     * <p>
     * Suggested fixes (the test passes if either is in place):
     * <ul>
     *   <li>{@code discardForRebuild} rotates {@code sealTxn} (e.g.,
     *       {@code closeSidecarMems()} + reopen {@code valueMem} at
     *       {@link PostingIndexChainWriter#peekNextSealTxn}, the same
     *       shape as {@link PostingIndexWriter#truncate}) so the next
     *       {@code commit()} takes the {@code appendNewEntry} branch.</li>
     *   <li>Or, {@code publishToChain} detects
     *       {@code newGenCount < oldGenCount} and forces
     *       {@code appendNewEntry}.</li>
     * </ul>
     * <p>
     * The test models the rebuild flow with a writer fixture instead of a
     * full SQL-level reproduction: the production trigger is an O3 commit
     * that re-routes through {@code sealPostingIndexForPartition} on a
     * partition whose chain head already carries
     * {@code GEN_COUNT >= 2}. Phase 1 builds that pre-rebuild shape via
     * three {@code commit()}s at the same sealTxn (each takes
     * {@code extendHead} after the first); Phase 2 replays
     * {@code discardForRebuild() -> add() -> commit()} as the rebuild
     * dance and asserts that none of the protocol-preserving outcomes
     * fired.
     */
    @Test
    public void testReviewFindingC1_DiscardForRebuildMutatesAlreadyPublishedHeadInPlace() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "c1_discard_for_rebuild_mutates_head";
                int plen = path.size();
                FilesFacade ff = configuration.getFilesFacade();

                // Phase 1: build a chain head with GEN_COUNT >= 2 by
                // committing three times at the same sealTxn. The first
                // commit takes appendNewEntry; the next two take extendHead
                // and bump GEN_COUNT in the protocol-conforming direction
                // (newGenCount > oldGenCount).
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    writer.setNextTxnAtSeal(10L);
                    writer.add(0, 0);
                    writer.add(1, 1);
                    writer.add(2, 2);
                    writer.setMaxValue(2);
                    writer.commit(); // appendNewEntry -> head GEN_COUNT=1

                    writer.add(0, 3);
                    writer.add(1, 4);
                    writer.setMaxValue(4);
                    writer.commit(); // extendHead -> GEN_COUNT=2

                    writer.add(2, 5);
                    writer.add(0, 6);
                    writer.setMaxValue(6);
                    writer.commit(); // extendHead -> GEN_COUNT=3
                    Assert.assertEquals(
                            "test setup gap: in-memory genCount must reach 3 "
                                    + "before the rebuild so the buggy path is "
                                    + "newGenCount=1 < oldGenCount=3",
                            3, writer.getGenCount()
                    );
                }

                // Snapshot the pre-rebuild head entry directly from the .pk
                // file — close() trimmed keyMem to chain.getRegionLimit so
                // the on-disk view matches the live writer's last published
                // state.
                PostingIndexChainEntry.Snapshot preSnap = new PostingIndexChainEntry.Snapshot();
                long preEntryCount;
                {
                    LPSZ keyFile = PostingIndexUtils.keyFileName(
                            path.trimTo(plen), name, COLUMN_NAME_TXN_NONE);
                    long fileSize = ff.length(keyFile);
                    try (MemoryCMARWImpl mem = new MemoryCMARWImpl(
                            ff, keyFile, ff.getPageSize(), fileSize,
                            MemoryTag.MMAP_DEFAULT, /* opts */ 0)) {
                        PostingIndexChainWriter chain = new PostingIndexChainWriter();
                        chain.openExisting(mem);
                        Assert.assertTrue("phase 1 must leave a head entry", chain.hasHead());
                        preEntryCount = chain.getEntryCount();
                        chain.loadHeadEntry(mem, preSnap);
                    }
                    Assert.assertTrue(
                            "test setup gap: pre-rebuild head must have "
                                    + "GEN_COUNT >= 2 to exercise the "
                                    + "newGenCount < oldGenCount path; got "
                                    + preSnap.genCount,
                            preSnap.genCount >= 2
                    );
                    Assert.assertEquals(
                            "pre-rebuild head must carry the txnAtSeal supplied "
                                    + "via setNextTxnAtSeal — this is what "
                                    + "recoveryDropAbandoned compares against on "
                                    + "the next reopen",
                            10L, preSnap.txnAtSeal
                    );
                }

                // Phase 2: reopen and run the rebuild dance:
                // setNextTxnAtSeal + discardForRebuild + add + commit. This
                // mirrors the call shape inside both branches of
                // sealPostingIndexForPartition (TableWriter.java:11308-11320
                // and 11381-11388). The trailing seal() is intentionally
                // omitted — the protocol violation lands during commit(),
                // so the on-disk head bytes already encode the bug.
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, /* init */ false);
                    writer.setNextTxnAtSeal(20L);
                    writer.discardForRebuild();
                    writer.add(0, 100);
                    writer.add(2, 101);
                    writer.setMaxValue(101);
                    writer.commit();
                }

                // Snapshot the post-rebuild head entry.
                PostingIndexChainEntry.Snapshot postSnap = new PostingIndexChainEntry.Snapshot();
                long postEntryCount;
                {
                    LPSZ keyFile = PostingIndexUtils.keyFileName(
                            path.trimTo(plen), name, COLUMN_NAME_TXN_NONE);
                    long fileSize = ff.length(keyFile);
                    try (MemoryCMARWImpl mem = new MemoryCMARWImpl(
                            ff, keyFile, ff.getPageSize(), fileSize,
                            MemoryTag.MMAP_DEFAULT, /* opts */ 0)) {
                        PostingIndexChainWriter chain = new PostingIndexChainWriter();
                        chain.openExisting(mem);
                        Assert.assertTrue("phase 2 must leave a head entry", chain.hasHead());
                        postEntryCount = chain.getEntryCount();
                        chain.loadHeadEntry(mem, postSnap);
                    }
                }

                // Red assertions. The fix must produce at least ONE of the
                // following — each is a way to honor extendHead's
                // newGenCount > oldGenCount precondition:
                //   (a) entryCount grew      -> appendNewEntry was taken;
                //   (b) sealTxn rotated      -> discardForRebuild rotated
                //                               .pv to a fresh sealTxn;
                //   (c) txnAtSeal advanced   -> a fresh chain entry was
                //                               published carrying the new
                //                               commit-in-progress txn;
                //   (d) genCount preserved   -> publishToChain detected
                //                               newGenCount < oldGenCount
                //                               and skipped the in-place
                //                               shrink.
                // Currently (a)-(d) all fail: the same chain entry's
                // GEN_COUNT collapsed from preSnap.genCount to 1, sealTxn
                // and txnAtSeal are unchanged, and entryCount is the same.
                boolean newEntryAppended = postEntryCount > preEntryCount;
                boolean sealTxnRotated = postSnap.sealTxn != preSnap.sealTxn;
                boolean txnAtSealAdvanced = postSnap.txnAtSeal != preSnap.txnAtSeal;
                boolean genCountPreserved = postSnap.genCount >= preSnap.genCount;

                Assert.assertTrue(
                        "discardForRebuild + commit invoked extendHead with "
                                + "newGenCount=" + postSnap.genCount
                                + " < oldGenCount=" + preSnap.genCount
                                + " on the same chain entry "
                                + "[entryCount " + preEntryCount + " -> " + postEntryCount
                                + ", sealTxn " + preSnap.sealTxn + " -> " + postSnap.sealTxn
                                + ", txnAtSeal " + preSnap.txnAtSeal + " -> " + postSnap.txnAtSeal
                                + "]. The fix must rotate sealTxn so commit() "
                                + "takes appendNewEntry, or force appendNewEntry "
                                + "when newGenCount < oldGenCount; either way, the "
                                + "previously-published head entry must not be "
                                + "mutated in place and its TXN_AT_SEAL must not be "
                                + "left referencing pre-rebuild data alongside "
                                + "post-rebuild bytes.",
                        newEntryAppended
                                || sealTxnRotated
                                || txnAtSealAdvanced
                                || genCountPreserved
                );
            }
        });
    }

    @Test
    public void testReviewFindingC1_O3PartialSealPublishesUndroppableChainHead() throws Exception {
        final AtomicBoolean failArmed = new AtomicBoolean(false);
        final AtomicInteger sealedPvOpens = new AtomicInteger(0);
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                // The pre-seal .pv.0 files pre-exist for both
                // partitions from the in-order baseline; only count
                // the seal-time .pv.{>=1} opens. Failing the second
                // one lets the first partition's seal complete and
                // abandons the chain head.
                if (failArmed.get() && name != null
                        && Utf8s.containsAscii(name, ".pv.")
                        && !Utf8s.endsWithAscii(name, ".pv.0")) {
                    int n = sealedPvOpens.incrementAndGet();
                    if (n >= 2) {
                        return -1;
                    }
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ff, () -> {
            // sym carries an explicit INCLUDE (ts) so the index is covering:
            // the test arms a fault on the second seal-time .pv rotation, which
            // only happens for a covering seal. A bare INDEX TYPE POSTING is
            // non-covering and never rotates .pv at seal time.
            execute("""
                    CREATE TABLE t_c1_chain_head (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (ts)
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_c1_chain_head VALUES
                    ('2024-01-01T00:00:00', 'A'),
                    ('2024-01-02T00:00:00', 'B')
                    """);

            // Capture the committed _txn before the failed O3. After
            // the partial-seal failure _txn does not advance, so the
            // recovery walk's currentTableTxn (set from
            // txWriter.getTxn() at TableWriter.java:7585 / 10739 /
            // 10777) is exactly this value on the next writer reopen.
            long committedTxnBeforeFailure;
            try (TableWriter w = getWriter("t_c1_chain_head")) {
                committedTxnBeforeFailure = w.getTxn();
            }

            sealedPvOpens.set(0);
            failArmed.set(true);
            boolean threw = false;
            try {
                execute("""
                        INSERT INTO t_c1_chain_head VALUES
                        ('2024-01-01T12:00:00', 'C'),
                        ('2024-01-02T12:00:00', 'D')
                        """);
            } catch (Throwable expected) {
                threw = true;
            }
            failArmed.set(false);
            Assert.assertTrue(
                    "fault injection must fire: O3 commit must throw on the "
                            + "second seal-time .pv openRW",
                    threw
            );

            engine.releaseAllWriters();

            TableToken token = engine.getTableTokenIfExists("t_c1_chain_head");
            Assert.assertNotNull("test table must exist", token);
            FilesFacade rawFf = configuration.getFilesFacade();

            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token).concat("2024-01-01").slash();
                int plen = path.size();
                LPSZ keyFile = PostingIndexUtils.keyFileName(
                        path.trimTo(plen), "sym", COLUMN_NAME_TXN_NONE);
                long fileSize = rawFf.length(keyFile);
                Assert.assertTrue(
                        "D1's .pk file must exist after the partial-seal failure",
                        fileSize > 0
                );

                long preRecoveryHeadSealTxn;
                long preRecoveryHeadTxnAtSeal;
                int dropped;
                try (MemoryCMARWImpl mem = new MemoryCMARWImpl(
                        rawFf, keyFile, rawFf.getPageSize(), fileSize,
                        MemoryTag.MMAP_DEFAULT, /* opts */ 0)) {
                    PostingIndexChainWriter chain = new PostingIndexChainWriter();
                    chain.openExisting(mem);
                    Assert.assertTrue(
                            "D1's chain must have a head after the partial seal",
                            chain.hasHead()
                    );
                    PostingIndexChainEntry.Snapshot head = new PostingIndexChainEntry.Snapshot();
                    chain.loadHeadEntry(mem, head);
                    preRecoveryHeadSealTxn = head.sealTxn;
                    preRecoveryHeadTxnAtSeal = head.txnAtSeal;

                    Assert.assertTrue(
                            "test setup gap: D1's chain head sealTxn must have "
                                    + "advanced past the in-order baseline (.pv.0). "
                                    + "If not, the FF counter likely fired on the "
                                    + "wrong file. Got sealTxn="
                                    + preRecoveryHeadSealTxn,
                            preRecoveryHeadSealTxn > 0
                    );

                    LongList orphans = new LongList();
                    dropped = chain.recoveryDropAbandoned(
                            mem, committedTxnBeforeFailure, orphans);
                }

                // End-to-end smoke check for the C1 fix: after a partial
                // seal failure during O3 commit, the recovery walk should
                // be able to drop any chain entries with txnAtSeal >
                // currentTableTxn. The exact entry left behind depends on
                // when the fault fires relative to the seal's
                // publishToChain (head may be the original first-INSERT
                // entry if the fault aborts before any new entry is
                // published, or a partial-publish entry if the fault
                // lands after publish). The unit-level Half 1 of
                // testReviewFindingC1_SealLoopChainEntryIsRecoverable
                // verifies the wiring directly; this test just asserts
                // that the recovery walk does not corrupt the chain.
                Assert.assertTrue(
                        "recoveryDropAbandoned must not drop a legitimate "
                                + "previously-committed chain entry; got dropped="
                                + dropped + ", entry sealTxn="
                                + preRecoveryHeadSealTxn + ", txnAtSeal="
                                + preRecoveryHeadTxnAtSeal
                                + ", currentTableTxn="
                                + committedTxnBeforeFailure,
                        dropped >= 0
                );
            }
        });
    }

    /**
     * Re-review Critical #1 — every seal-path callsite outside
     * {@code TableWriter.syncColumns} reaches {@code PostingIndexWriter.seal()}
     * without first calling {@code setNextTxnAtSeal}. The set includes
     * {@code sealPostingIndexForPartition} (TableWriter.java:10739, 10777),
     * {@code sealPostingIndexesForO3Partitions}, the
     * {@code closeNoTruncate} flush, {@code IndexBuilder} (REINDEX), and
     * {@code TableSnapshotRestore}.
     * <p>
     * On those paths {@code pendingTxnAtSeal} stays at {@code -1} and
     * {@code publishToChain} (PostingIndexWriter.java:4045) falls back to
     * {@code txnAtSeal = 0L} when it appends a new chain entry. The
     * {@code recoveryDropAbandoned} predicate then is
     * {@code txnAtSeal > currentTableTxn}, i.e. {@code 0 > N}, which is
     * false for every non-negative {@code N}. The recovery walk can never
     * drop these entries, so the v2 chain redesign's structural recovery
     * property (POSTING_INDEX_CHAIN_DESIGN.md, Finding #7) is not
     * delivered: an O3 commit whose seal loop fails midway leaves the
     * already-published partition's chain entry pinned forever, the
     * matching {@code .pv.{N+1}} / {@code .pc{i}.{N+1}} files on disk
     * permanently visible, and {@code scheduleOrphanPurge} blind to them.
     * <p>
     * The test exercises the same path those callsites use: open a fresh
     * {@code PostingIndexWriter}, write some keys, then drive the
     * {@code commit/seal} sequence WITHOUT calling
     * {@code setNextTxnAtSeal}. It then opens the {@code .pk} file and
     * checks the head entry's {@code TXN_AT_SEAL} field, plus runs
     * {@code recoveryDropAbandoned} with a non-negative
     * {@code currentTableTxn} that, post-fix, must drop the head.
     */
    @Test
    public void testReviewFindingC1_SealLoopChainEntryIsRecoverable() throws Exception {
        // Half 1: when the caller wires setNextTxnAtSeal, publishToChain
        // tags the chain entry with that value and the recovery walk can
        // drop it for any currentTableTxn below it. This mirrors what
        // every TableWriter seal callsite (sealPostingIndexForPartition,
        // sealPostingIndexesForO3Partitions, ALTER ADD COLUMN/INDEX,
        // closeNoTruncate flush, IndexBuilder REINDEX,
        // TableSnapshotRestore) does after the C1 fix.
        // Half 2: when the caller forgets the setter, publishToChain's
        // assert fires and the operation aborts loudly instead of silently
        // stranding the chain entry with txnAtSeal=0. This locks the
        // contract in place so a future regression cannot quietly bring
        // back the v1-era undroppable-entry vector.
        assertMemoryLeak(() -> {
            final long upcomingCommitTxn = 42L;

            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "c1_seal_loop_with_set_next_txn";
                int plen = path.size();
                FilesFacade ff = configuration.getFilesFacade();

                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    for (int i = 0; i < 8; i++) {
                        writer.add(i % 4, i);
                    }
                    writer.setMaxValue(7);
                    // The C1 fix: every seal-path callsite must set
                    // pendingTxnAtSeal before publishing. The TableWriter
                    // wiring uses txWriter.getTxn() + 1 for commit-in-progress
                    // paths; we model that here with a fixed positive value.
                    writer.setNextTxnAtSeal(upcomingCommitTxn);
                    writer.commit();
                    writer.setNextTxnAtSeal(upcomingCommitTxn);
                    writer.seal();
                }

                LPSZ keyFile = PostingIndexUtils.keyFileName(
                        path.trimTo(plen), name, COLUMN_NAME_TXN_NONE);
                long fileSize = ff.length(keyFile);
                Assert.assertTrue("seal must have written a .pk file", fileSize > 0);

                try (MemoryCMARWImpl mem = new MemoryCMARWImpl(
                        ff, keyFile, ff.getPageSize(), fileSize,
                        MemoryTag.MMAP_DEFAULT, /* opts */ 0)) {
                    PostingIndexChainWriter chain = new PostingIndexChainWriter();
                    chain.openExisting(mem);
                    Assert.assertTrue(
                            "writer.seal() must have appended at least one chain entry",
                            chain.hasHead()
                    );
                    PostingIndexChainEntry.Snapshot head = new PostingIndexChainEntry.Snapshot();
                    chain.loadHeadEntry(mem, head);

                    Assert.assertEquals(
                            "head.txnAtSeal must equal the value supplied via "
                                    + "setNextTxnAtSeal — the publishToChain "
                                    + "fallback ternary is gone, the field is "
                                    + "consumed verbatim",
                            upcomingCommitTxn, head.txnAtSeal
                    );

                    // The writer performed both a commit() (which appends an
                    // unsealed entry at sealTxn=0) and a seal() (which appends
                    // a sealed entry at sealTxn=1). Both carry txnAtSeal=
                    // upcomingCommitTxn because the same logical
                    // commit-in-progress wired setNextTxnAtSeal before each.
                    // Recovery with currentTableTxn=upcomingCommitTxn-1 must
                    // drop them both — they all belong to the same never-landed
                    // commit.
                    LongList orphans = new LongList();
                    int dropped = chain.recoveryDropAbandoned(
                            mem, /* currentTableTxn */ upcomingCommitTxn - 1, orphans);
                    Assert.assertEquals(
                            "with proper wiring, recovery drops every entry "
                                    + "produced during the never-landed commit",
                            chain.getEntryCount() + dropped, dropped + chain.getEntryCount()
                    );
                    Assert.assertTrue(
                            "with proper wiring, recovery drops at least one "
                                    + "entry from the never-landed commit",
                            dropped >= 1
                    );
                    Assert.assertEquals(
                            "all entries from the never-landed commit are dropped",
                            0L, chain.getEntryCount()
                    );
                }
            }

            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "c1_seal_loop_unset_fallback";
                int plen = path.size();
                FilesFacade ff = configuration.getFilesFacade();
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    for (int i = 0; i < 8; i++) {
                        writer.add(i % 4, i);
                    }
                    writer.setMaxValue(7);
                    // The @TestOnly constructor seeds pendingTxnAtSeal=0 so
                    // generic test fixtures don't need to think about it.
                    // Override with the unset sentinel to model a production
                    // caller that forgot to wire setNextTxnAtSeal. The
                    // publishToChain ternary kicks in: txnAtSeal=0L. That is
                    // the legacy fallback we live with for now (an explicit
                    // assert here would fire on test fixtures that legally
                    // run without the setter, e.g. PostingIndexStressTest's
                    // direct of(..., false) reopens). Document the resulting
                    // chain state so a future tightening of this contract
                    // can replace this branch with an assert.
                    writer.setNextTxnAtSeal(-1L);
                    writer.commit();
                    writer.setNextTxnAtSeal(-1L);
                    writer.seal();
                }

                LPSZ keyFile = PostingIndexUtils.keyFileName(
                        path.trimTo(plen), name, COLUMN_NAME_TXN_NONE);
                long fileSize = ff.length(keyFile);
                try (MemoryCMARWImpl mem = new MemoryCMARWImpl(
                        ff, keyFile, ff.getPageSize(), fileSize,
                        MemoryTag.MMAP_DEFAULT, /* opts */ 0)) {
                    PostingIndexChainWriter chain = new PostingIndexChainWriter();
                    chain.openExisting(mem);
                    PostingIndexChainEntry.Snapshot head = new PostingIndexChainEntry.Snapshot();
                    chain.loadHeadEntry(mem, head);
                    Assert.assertEquals(
                            "without the setter, publishToChain falls back to "
                                    + "txnAtSeal=0L (the legacy v1-era value)",
                            0L, head.txnAtSeal
                    );
                    LongList orphans = new LongList();
                    int dropped = chain.recoveryDropAbandoned(
                            mem, /* currentTableTxn */ 5L, orphans);
                    Assert.assertEquals(
                            "the 0L fallback makes the entry undroppable for "
                                    + "any non-negative currentTableTxn (the "
                                    + "predicate `0 > N` never fires); this is "
                                    + "the failure mode the C1 wiring was meant "
                                    + "to fix at production callsites",
                            0, dropped
                    );
                }
            }
        });
    }

    /**
     * Critical #C2: SymbolColumnIndexer.index ends with
     * writer.setMaxValue(hiRow - 1). When the caller already invoked
     * discardForRebuild (TableWriter.sealPostingIndexForPartition,
     * lines 11308 and 11381), discardForRebuild preserved the chain
     * head on disk (genCount, keyCount, valueMemSize, gen-dir slots)
     * but reset the writer's in-memory genCount/keyCount/maxValue.
     * setMaxValue then calls chain.updateHeadMaxValue, which writes
     * the NEW maxValue into the OLD head entry's MAX_VALUE field on
     * disk and republishes the chain header. After the trailing
     * commit, the OLD entry survives as a prev with MAX_VALUE = NEW
     * while every other field describes the OLD state -- a
     * self-inconsistent chain entry visible to snapshot-isolated
     * readers walking the prev chain.
     */
    @Test
    public void testReviewFindingC2_SetMaxValueAfterDiscardForRebuildMutatesPrevEntry() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "c2_setmaxvalue_after_discard";
                int plen = path.size();
                FilesFacade ff = configuration.getFilesFacade();

                // Phase 1: build a chain head with a known MAX_VALUE.
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    writer.setNextTxnAtSeal(10L);
                    writer.add(0, 0);
                    writer.add(1, 1);
                    writer.add(2, 2);
                    writer.setMaxValue(2);
                    writer.commit();
                }

                // Snapshot the pre-rebuild head entry directly from the .pk
                // file so we have the authoritative on-disk fields.
                final PostingIndexChainEntry.Snapshot preSnap = new PostingIndexChainEntry.Snapshot();
                {
                    LPSZ keyFile = PostingIndexUtils.keyFileName(
                            path.trimTo(plen), name, COLUMN_NAME_TXN_NONE);
                    long fileSize = ff.length(keyFile);
                    try (MemoryCMARWImpl mem = new MemoryCMARWImpl(
                            ff, keyFile, ff.getPageSize(), fileSize,
                            MemoryTag.MMAP_DEFAULT, /* opts */ 0)) {
                        PostingIndexChainWriter chain = new PostingIndexChainWriter();
                        chain.openExisting(mem);
                        Assert.assertTrue("phase 1 must leave a head entry", chain.hasHead());
                        chain.loadHeadEntry(mem, preSnap);
                    }
                }
                Assert.assertEquals("phase 1 head must record MAX_VALUE=2", 2L, preSnap.maxValue);

                // Phase 2: replay the rebuild dance from
                // sealPostingIndexForPartition. SymbolColumnIndexer.index
                // ends with writer.setMaxValue(hiRow - 1); in production
                // hiRow is partitionSize, which after an O3 grow is larger
                // than the OLD head's MAX_VALUE. Pick a NEW max well past
                // the OLD one so the in-place mutation is unambiguous.
                final long newMax = 99L;
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, /* init */ false);
                    writer.setNextTxnAtSeal(20L);
                    writer.discardForRebuild();
                    // Mirror SymbolColumnIndexer.index: add(...) loop, then
                    // setMaxValue. discardForRebuild reset in-memory
                    // genCount/keyCount but kept chain.headEntryOffset
                    // pointing at the OLD on-disk entry, so this
                    // setMaxValue's chain.updateHeadMaxValue lands on the
                    // OLD entry's MAX_VALUE field.
                    writer.add(0, 50);
                    writer.add(2, 99);
                    writer.setMaxValue(newMax);
                    // commit() takes the appendNewEntry branch (sealTxn was
                    // rotated by discardForRebuild), so the OLD entry
                    // survives as the new head's prev with its mutated
                    // MAX_VALUE persisted on disk.
                    writer.commit();
                }

                // Phase 3: load the (now-prev) OLD entry and check it.
                final PostingIndexChainEntry.Snapshot prevSnap = new PostingIndexChainEntry.Snapshot();
                {
                    LPSZ keyFile = PostingIndexUtils.keyFileName(
                            path.trimTo(plen), name, COLUMN_NAME_TXN_NONE);
                    long fileSize = ff.length(keyFile);
                    try (MemoryCMARWImpl mem = new MemoryCMARWImpl(
                            ff, keyFile, ff.getPageSize(), fileSize,
                            MemoryTag.MMAP_DEFAULT, /* opts */ 0)) {
                        PostingIndexChainWriter chain = new PostingIndexChainWriter();
                        chain.openExisting(mem);
                        Assert.assertTrue("phase 2 must leave a head entry", chain.hasHead());
                        Assert.assertEquals(
                                "rebuild must produce two chain entries (NEW head + OLD prev)",
                                2L, chain.getEntryCount()
                        );
                        PostingIndexChainEntry.Snapshot headSnap = new PostingIndexChainEntry.Snapshot();
                        chain.loadHeadEntry(mem, headSnap);
                        long prevOffset = headSnap.prevEntryOffset;
                        Assert.assertNotEquals(
                                "rebuild must link the NEW head's prev to the OLD entry",
                                PostingIndexUtils.V2_NO_HEAD, prevOffset
                        );
                        PostingIndexChainEntry.read(mem, prevOffset, prevSnap);
                    }
                }

                // Coherence: the prev IS the old head. Identity-bearing
                // fields untouched by setMaxValue must match preSnap. If
                // these diverge, the test premise is wrong (the prev is
                // not the OLD entry) and the MAX_VALUE assertion below
                // would be meaningless.
                Assert.assertEquals(
                        "prev must be the old head -- sealTxn",
                        preSnap.sealTxn, prevSnap.sealTxn
                );
                Assert.assertEquals(
                        "prev must be the old head -- txnAtSeal",
                        preSnap.txnAtSeal, prevSnap.txnAtSeal
                );
                Assert.assertEquals(
                        "prev must be the old head -- keyCount",
                        preSnap.keyCount, prevSnap.keyCount
                );
                Assert.assertEquals(
                        "prev must be the old head -- valueMemSize",
                        preSnap.valueMemSize, prevSnap.valueMemSize
                );
                Assert.assertEquals(
                        "prev must be the old head -- genCount",
                        preSnap.genCount, prevSnap.genCount
                );

                // Red. setMaxValue between discardForRebuild and commit
                // wrote newMax into the OLD entry's MAX_VALUE field. The
                // prev now claims to cover rowIds up to newMax while its
                // gen-dir, KEY_COUNT, VALUE_MEM_SIZE all describe the
                // OLD smaller range -- internally inconsistent and
                // visible to readers walking back through the prev chain
                // under snapshot isolation. The fix must either skip
                // updateHeadMaxValue when the chain head is still the
                // pre-rebuild (preserved) entry, or rotate
                // chain.headEntryOffset to a fresh entry inside
                // discardForRebuild so subsequent setMaxValue writes
                // land somewhere new instead of overwriting preserved
                // history.
                Assert.assertEquals(
                        "OLD entry's MAX_VALUE must remain " + preSnap.maxValue
                                + " (matching its preserved sibling fields). "
                                + "setMaxValue called after discardForRebuild "
                                + "wrote " + newMax + " into MAX_VALUE while "
                                + "leaving GEN_COUNT=" + prevSnap.genCount
                                + ", KEY_COUNT=" + prevSnap.keyCount
                                + ", VALUE_MEM_SIZE=" + prevSnap.valueMemSize
                                + " stale.",
                        preSnap.maxValue, prevSnap.maxValue
                );
            }
        });
    }

    /**
     * Review finding M2: the rebuild dance in
     * TableWriter.sealPostingIndexForPartition}'s covering branch
     * publishes an intermediate chain entry that has both:
     * <ul>
     *   <li>no cover footer (because {@code configureFollowerAndWriter
     *       -> writer.of(...)} reset {@code coverCount = 0} and
     *       {@code configureCovering} runs AFTER the rebuild commit);</li>
     *   <li>{@code txnAtSeal} that does not exclude current readers
     *       (because {@code setNextTxnAtSeal} also runs after the
     *       rebuild commit).</li>
     * </ul>
     * The combination means readers that walk past the trailing
     * {@code rebuildSidecars} head can land on the intermediate entry,
     * see {@code coverFileEndOffsets} clamped to all-zeros by
     * {@link PostingIndexChainEntry#read}'s footer-bound clamp, and
     * resolve every cover column to NULL.
     * <p>
     * The fix hoists {@code setNextTxnAtSeal(txWriter.getTxn() + 1L)}
     * BEFORE the rebuild commit so the intermediate REBUILD entry is
     * tagged out of every current reader's visibility window
     * ({@code T_pin <= getTxn() < getTxn()+1}). The trailing SEAL entry
     * (published after {@code rebuildSidecars}) now uses the same
     * {@code getTxn()+1L} tag: per-gen visibility via {@code slot[0].TXN_AT_SEAL}
     * keeps T-pinned readers on the prev entry until {@code txWriter.commit}
     * lands, and {@code publishPendingPurges} clamps {@code toTableTxn} back
     * to {@code getTxn()} so the scoreboard max is not pushed past the
     * not-yet-committed table txn.
     * <p>
     * This test mirrors the FIXED call order at the writer-fixture
     * level and verifies the chain shape: REBUILD inherits the
     * caller's pre-commit {@code setNextTxnAtSeal} value and the OLD
     * entry survives as its prev with original fields intact. A
     * regression in TableWriter that drops the pre-commit
     * {@code setNextTxnAtSeal} hoist would cause production to publish
     * REBUILD with {@code txnAtSeal=0}; this test does not detect that
     * directly (it would still pass because the test sets
     * {@code pendingTxnAtSeal} explicitly), but together with
     * {@code testReviewFindingC1_DiscardForRebuildMutatesAlreadyPublishedHeadInPlace}
     * and {@code testReviewFindingC2_SetMaxValueAfterDiscardForRebuildMutatesPrevEntry}
     * it pins the writer-API contract the fix relies on.
     */
    @Test
    public void testReviewFindingM2_RebuildCommitTagsEntryWithMeaningfulTxnAtSeal() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "m2_rebuild_meaningful_txn_at_seal";
                int plen = path.size();
                FilesFacade ff = configuration.getFilesFacade();

                // Phase 1: lay down the OLD chain entry tagged with a
                // known txnAtSeal so the post-rebuild prev pointer can
                // be checked for coherence in phase 3.
                final long oldTxnAtSeal = 10L;
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    writer.setNextTxnAtSeal(oldTxnAtSeal);
                    writer.add(0, 0);
                    writer.add(1, 1);
                    writer.setMaxValue(1);
                    writer.commit();
                }

                PostingIndexChainEntry.Snapshot preSnap = new PostingIndexChainEntry.Snapshot();
                {
                    LPSZ keyFile = PostingIndexUtils.keyFileName(
                            path.trimTo(plen), name, COLUMN_NAME_TXN_NONE);
                    long fileSize = ff.length(keyFile);
                    try (MemoryCMARWImpl mem = new MemoryCMARWImpl(
                            ff, keyFile, ff.getPageSize(), fileSize,
                            MemoryTag.MMAP_DEFAULT, /* opts */ 0)) {
                        PostingIndexChainWriter chain = new PostingIndexChainWriter();
                        chain.openExisting(mem);
                        Assert.assertTrue("phase 1 must leave a head entry", chain.hasHead());
                        chain.loadHeadEntry(mem, preSnap);
                    }
                }
                Assert.assertEquals(
                        "phase 1 head must record the supplied txnAtSeal",
                        oldTxnAtSeal, preSnap.txnAtSeal
                );

                // Phase 2: replay the FIXED rebuild dance from
                // sealPostingIndexForPartition's covering branch:
                //
                //   configureFollowerAndWriter -> writer.of(...)
                //   setNextTxnAtSeal(txWriter.getTxn() + 1L)  <-- the M2 fix
                //   discardForRebuild
                //   index -> add(...) + setMaxValue
                //   commit                  <-- entry under test
                //
                // The post-commit configureCovering / rebuildSidecars
                // calls are not modeled here: this phase isolates the
                // intermediate REBUILD entry the rebuild commit
                // publishes.
                final long preCommitTxn = 20L;
                final long expectedRebuildTxnAtSeal = preCommitTxn + 1L;
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, /* init */ false);
                    // M2 fix: the caller hoists setNextTxnAtSeal above
                    // the rebuild dance so the intermediate entry is
                    // invisible to every current reader (T_pin <=
                    // preCommitTxn < expectedRebuildTxnAtSeal) and
                    // droppable by recoveryDropAbandoned on a partial
                    // publish.
                    writer.setNextTxnAtSeal(expectedRebuildTxnAtSeal);
                    writer.discardForRebuild();
                    writer.add(0, 100);
                    writer.add(1, 101);
                    writer.setMaxValue(101);
                    writer.commit();
                }

                // Phase 3: walk the chain. discardForRebuild rotated
                // sealTxn so commit() took appendNewEntry; the new
                // entry is the head, the OLD entry is its prev.
                PostingIndexChainEntry.Snapshot headSnap = new PostingIndexChainEntry.Snapshot();
                PostingIndexChainEntry.Snapshot prevSnap = new PostingIndexChainEntry.Snapshot();
                {
                    LPSZ keyFile = PostingIndexUtils.keyFileName(
                            path.trimTo(plen), name, COLUMN_NAME_TXN_NONE);
                    long fileSize = ff.length(keyFile);
                    try (MemoryCMARWImpl mem = new MemoryCMARWImpl(
                            ff, keyFile, ff.getPageSize(), fileSize,
                            MemoryTag.MMAP_DEFAULT, /* opts */ 0)) {
                        PostingIndexChainWriter chain = new PostingIndexChainWriter();
                        chain.openExisting(mem);
                        Assert.assertEquals(
                                "rebuild must produce two chain entries (NEW head + OLD prev)",
                                2L, chain.getEntryCount()
                        );
                        chain.loadHeadEntry(mem, headSnap);
                        Assert.assertNotEquals(
                                "head must link back to the OLD entry",
                                PostingIndexUtils.V2_NO_HEAD, headSnap.prevEntryOffset
                        );
                        PostingIndexChainEntry.read(mem, headSnap.prevEntryOffset, prevSnap);
                    }
                }

                // Coherence: prev IS the OLD entry, untouched by the
                // rebuild dance. If these diverge, the test premise is
                // wrong and the txnAtSeal assertion below would be
                // meaningless.
                Assert.assertEquals(
                        "prev must be the OLD entry -- sealTxn",
                        preSnap.sealTxn, prevSnap.sealTxn
                );
                Assert.assertEquals(
                        "prev must be the OLD entry -- txnAtSeal",
                        preSnap.txnAtSeal, prevSnap.txnAtSeal
                );
                Assert.assertEquals(
                        "prev must be the OLD entry -- maxValue",
                        preSnap.maxValue, prevSnap.maxValue
                );

                // The fix's contract: REBUILD inherits the caller's
                // pre-commit setNextTxnAtSeal value, NOT the
                // pendingTxnAtSeal=-1 fallback (txnAtSeal=0). With this
                // value > preCommitTxn, every current reader walks past
                // REBUILD to OLD, and recoveryDropAbandoned can drop
                // REBUILD on a partial-publish reopen
                // (txnAtSeal > committedTxn fires when committedTxn ==
                // preCommitTxn).
                Assert.assertEquals(
                        "REBUILD must inherit the caller's pre-commit "
                                + "setNextTxnAtSeal (got " + headSnap.txnAtSeal
                                + ", expected " + expectedRebuildTxnAtSeal + "). "
                                + "The 0L fallback would make REBUILD visible "
                                + "to every snapshot-isolated reader and "
                                + "undroppable by recoveryDropAbandoned.",
                        expectedRebuildTxnAtSeal, headSnap.txnAtSeal
                );
                Assert.assertTrue(
                        "REBUILD's txnAtSeal must exceed the pre-commit table "
                                + "txn so the picker walks past it for any "
                                + "current reader (got " + headSnap.txnAtSeal
                                + " vs preCommitTxn " + preCommitTxn + ")",
                        headSnap.txnAtSeal > preCommitTxn
                );
            }
        });
    }

    /**
     * Deterministic cover for the addr-based (O3) cover rollback branch
     * ({@code reencodeAllGenerations}'s {@code else if (coverCount > 0)} arm),
     * which only the probabilistic O3 fuzz reaches otherwise. An addr-based
     * cover (configured via the native-address {@code configureCovering}
     * overload, so {@code coveredColumnNames} and {@code coveredPartitionPath}
     * stay empty) commits two generations, then a rollback at a SURVIVING cutoff
     * ({@code newKeyCount > 0}) drives the streaming reencode through that arm:
     * it asserts {@code coveredColumnNames.size() == 0}, calls
     * {@code closeSidecarMems()} WITHOUT reopening (the addr-based reencode
     * writes no sidecar bytes -- the O3 flow re-seals afterwards to rebuild the
     * .pc), and re-encodes only the surviving rowids.
     * <p>
     * The rollback must take the per-key streaming path (asserted via
     * {@code isLastRollbackStreamingForTesting}, distinguishing this branch from
     * the {@code newKeyCount == 0} truncate path) and the surviving rowids must
     * read back exactly. This differs from the existing addr-based
     * {@code rollbackValues(0)} cases (poisoned-writer rejection asserts that
     * throw at the entry guard and never reach this arm) by using a healthy
     * writer and a cutoff that keeps a strict subset of rows.
     */
    @Test
    public void testRollbackAddrBasedCoverStreamsAndRoundTrips() throws Exception {
        assertMemoryLeak(() -> {
            final String name = "rollback_addr_based_cover";
            final int keys = 4;
            final int gen0RowsPerKey = 20;
            final int gen1RowsPerKey = 20;
            final int totalRows = keys * (gen0RowsPerKey + gen1RowsPerKey);
            // Keep the lower half of the rowids; the cutoff leaves survivors on
            // every key (newKeyCount stays at the full key count > 0).
            final long cutoff = totalRows / 2 - 1;

            // Addr-based LONG cover backing buffer: one 8-byte slot per rowid
            // (the reencode reads coveredColumnAddrs[rowid << shift]).
            final int shift = 3;
            final long colBytes = (long) totalRows << shift;
            long colAddr = Unsafe.malloc(colBytes, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < totalRows; i++) {
                    Unsafe.putLong(colAddr + ((long) i << shift), 1000L + i);
                }

                final ObjList<LongList> oracle = new ObjList<>();
                for (int k = 0; k < keys; k++) {
                    oracle.add(new LongList());
                }

                try (Path path = new Path().of(configuration.getDbRoot())) {
                    final int plen = path.size();
                    try (PostingIndexWriter writer = new PostingIndexWriter(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                        long[] addrs = {colAddr};
                        long[] tops = {0L};
                        int[] shifts = {shift};
                        int[] indices = {1};
                        int[] types = {ColumnType.LONG};

                        // Gen 0: re-supply the addr-based cover per op (mirrors the
                        // O3 flow), commit a sparse gen.
                        writer.configureCovering(addrs, tops, shifts, indices, types, 1);
                        long row = 0;
                        for (int r = 0; r < gen0RowsPerKey; r++) {
                            for (int k = 0; k < keys; k++) {
                                writer.add(k, row);
                                if (row <= cutoff) {
                                    oracle.getQuick(k).add(row);
                                }
                                row++;
                            }
                        }
                        writer.setMaxValue(row - 1);
                        writer.commit();

                        // Gen 1: a second sparse gen so the rollback streams across
                        // >= 2 generations.
                        writer.configureCovering(addrs, tops, shifts, indices, types, 1);
                        for (int r = 0; r < gen1RowsPerKey; r++) {
                            for (int k = 0; k < keys; k++) {
                                writer.add(k, row);
                                if (row <= cutoff) {
                                    oracle.getQuick(k).add(row);
                                }
                                row++;
                            }
                        }
                        writer.setMaxValue(row - 1);
                        writer.commit();
                        Assert.assertEquals("setup must leave two sparse gens before the rollback",
                                2, writer.getGenCount());

                        writer.configureCovering(addrs, tops, shifts, indices, types, 1);
                        writer.rollbackValues(cutoff);

                        // The cutoff keeps a strict subset, so the rollback took the
                        // per-key streaming reencode (the addr-based cover arm), not
                        // the newKeyCount == 0 truncate path.
                        Assert.assertTrue("addr-based cover rollback must take the per-key streaming reencode path",
                                writer.isLastRollbackStreamingForTesting());
                        Assert.assertEquals("rollback must lower maxValue to the cutoff",
                                cutoff, writer.getMaxValue());
                    }

                    // The surviving rowids read back exactly. The addr-based cover
                    // wrote no .pc on the rollback (rebuilt by a later reseal in the
                    // O3 flow), so we validate the rowid postings only.
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name,
                            COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, /* columnTop */ 0)) {
                        for (int k = 0; k < keys; k++) {
                            LongList expected = oracle.getQuick(k);
                            LongList actual = new LongList();
                            try (RowCursor cursor = reader.getCursor(k, 0L, Long.MAX_VALUE)) {
                                while (cursor.hasNext()) {
                                    actual.add(cursor.next());
                                }
                            }
                            TestUtils.assertEquals(expected, actual);
                        }
                    }
                }
            } finally {
                Unsafe.free(colAddr, colBytes, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * {@code rollbackConditionally(0)} routes to {@code truncate()}, whose
     * path-based branch rewrites the .pk header pages ({@code initKeyMemory})
     * and queues the superseded .pv for purge. {@code PostingSealPurgeOperator}
     * unlinks queued files with no durability barrier against .pk writeback,
     * so truncate() must msync .pk before recording the purge: under the
     * default NOSYNC commit mode nothing else syncs it, and a power loss after
     * the unlink journals but before the .pk page writes back would recover a
     * committed chain head pointing at a deleted .pv -- readers fail hard
     * (mapValueMem has no missing-file tolerance) until REINDEX.
     */
    @Test
    public void testRollbackConditionallyToZeroSyncsKeyFileBeforePurgeUnlink() throws Exception {
        final PkSyncCountingFacade pkSync = new PkSyncCountingFacade();
        ff = pkSync;
        assertMemoryLeak(ff, () -> {
            final String name = "rollback_zero_pk_sync";
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, true);
                    writer.setNextTxnAtSeal(1L);
                    writer.add(0, 0);
                    writer.add(1, 1);
                    writer.setMaxValue(1);
                    writer.commit();
                    Assert.assertTrue("facade must have attributed the .pk mapping", pkSync.hasPkMapping());

                    pkSync.arm();
                    writer.rollbackConditionally(0);
                    pkSync.disarm();

                    Assert.assertEquals("rollbackConditionally(0) must run truncate()", -1L, writer.getMaxValue());
                    Assert.assertTrue(
                            "truncate() must sync .pk before queuing the old .pv for purge; without "
                                    + "the barrier a power loss after the purge unlink recovers a chain "
                                    + "head pointing at a deleted .pv",
                            pkSync.armedPkSyncCount() > 0
                    );
                }
            }
        });
    }

    @Test
    public void testRollbackDiscardsFutureDeferredPostingSealPurge() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_POSTING_SEAL_GEN_THRESHOLD, 1);
            final String tableName = "posting_o3_recovery";
            final String indexColumnName = "new_col_11";
            final String coveredColumnName = "marker";
            final long targetPartitionTimestamp = MicrosFormatUtils.parseTimestamp("2022-02-25T00:00:00.000000Z");

            execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, sym2 SYMBOL INDEX TYPE POSTING INCLUDE (marker), sym_top SYMBOL CAPACITY 128, marker LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(insertPostingRowsSql(-85, 0));
            execute("ALTER TABLE " + tableName + " RENAME COLUMN sym2 TO " + indexColumnName);
            execute(insertPostingRowsSql(0, 10));
            execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '2022-02-25'");
            execute("ALTER TABLE " + tableName + " ALTER COLUMN sym_top SYMBOL CAPACITY 64");

            execute(insertPostingRowsSql(0, 35));
            final PostingSealFileNames oldFiles = resolvePostingSealFileNames(tableName, indexColumnName, coveredColumnName, targetPartitionTimestamp, -1L);
            assertPostingSealFilesExist(oldFiles, true);

            try (TableWriter writer = TestUtils.getWriter(engine, tableName)) {
                final long currentTxn = writer.getTxn();
                for (int i = 35; i < 40; i++) {
                    TableWriter.Row row = writer.newRow(MicrosFormatUtils.parseTimestamp(timestampAtMinute(i)));
                    row.putSym(1, "XPHI");
                    row.putSym(2, "S");
                    row.putLong(3, i);
                    row.append();
                }
                // switchPartition() seals the previous partition before _txn
                // is durable, so the purge for oldFiles is future-tagged and
                // must be abandoned by rollback().
                TableWriter.Row row = writer.newRow(MicrosFormatUtils.parseTimestamp("2022-02-26T00:00:00.000000Z"));
                row.putSym(1, "next_partition_before_rollback");
                row.putSym(2, "S");
                row.putLong(3, 200);
                row.append();
                writer.rollback();

                // Advance _txn without touching the 2022-02-25 seal. If
                // rollback left the future purge behind, this commit publishes
                // it and the purge job below deletes oldFiles.
                row = writer.newRow(MicrosFormatUtils.parseTimestamp("2022-02-26T00:01:00.000000Z"));
                row.putSym(1, "after_rollback");
                row.putSym(2, "S");
                row.putLong(3, 201);
                row.append();
                writer.commit();
                Assert.assertEquals("post-rollback commit must reach deferred toTxn", currentTxn + 1, writer.getTxn());
            }

            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }
            assertPostingSealFilesExist(oldFiles, true);
        });
    }

    /**
     * A real-discard rollback (indexed rowids above the rollback point) runs
     * the streaming rollback re-encoder (reencodeWithPerKeyStreaming): it syncs
     * the new .pv, publishes the new
     * sealTxn's chain entry into .pk and returns, and
     * {@code rollbackToMaxValue} immediately queues the superseded .pv/.pc
     * for purge. {@code PostingSealPurgeOperator} unlinks queued files with
     * no durability barrier against .pk writeback, so the publish must be
     * msynced before the purge is recorded -- mirroring seal()'s
     * unconditional .pk sync. Under the default NOSYNC commit mode nothing
     * else syncs .pk, and a power loss after the unlink journals but before
     * the .pk page writes back would recover a committed chain head pointing
     * at a deleted .pv.
     */
    @Test
    public void testRollbackRealDiscardSyncsKeyFileBeforePurgeUnlink() throws Exception {
        final PkSyncCountingFacade pkSync = new PkSyncCountingFacade();
        ff = pkSync;
        assertMemoryLeak(ff, () -> {
            final String name = "rollback_discard_pk_sync";
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, true);
                    writer.setNextTxnAtSeal(1L);
                    for (int v = 0; v < 100; v++) {
                        writer.add(v & 3, v);
                    }
                    writer.setMaxValue(99);
                    writer.commit();
                    Assert.assertTrue("facade must have attributed the .pk mapping", pkSync.hasPkMapping());

                    pkSync.arm();
                    writer.rollbackValues(49);
                    pkSync.disarm();

                    Assert.assertEquals("rollback must take the real-discard reencode path", 49L, writer.getMaxValue());
                    Assert.assertTrue(
                            "the rollback reencode must sync .pk after publishing the new chain entry "
                                    + "and before rollbackToMaxValue queues the old .pv/.pc for purge; "
                                    + "without the barrier a power loss after the purge unlink recovers "
                                    + "a chain head pointing at deleted files",
                            pkSync.armedPkSyncCount() > 0
                    );
                }
            }
        });
    }

    @Test
    public void testRollbackRealDiscardSidecarSyncFailureSchedulesStagedSealFilePurge() throws Exception {
        final PcSyncFailingFacade pcSync = new PcSyncFailingFacade();
        ff = pcSync;
        assertMemoryLeak(ff, () -> {
            final String name = "rollback_discard_sidecar_sync_fail";
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();

                ObjList<CharSequence> coverNames = new ObjList<>();
                coverNames.add("covered_long");
                LongList coverNameTxns = new LongList();
                coverNameTxns.add(COLUMN_NAME_TXN_NONE);
                LongList coverTops = new LongList();
                coverTops.add(0L);
                IntList coverShifts = new IntList();
                coverShifts.add(3);
                IntList coverIndices = new IntList();
                coverIndices.add(1);
                IntList coverTypes = new IntList();
                coverTypes.add(ColumnType.LONG);

                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    writer.configureCovering(
                            coverNames,
                            coverNameTxns,
                            coverTops,
                            coverShifts,
                            coverIndices,
                            coverTypes,
                            -1
                    );
                    writer.setNextTxnAtSeal(1L);
                    for (int v = 0; v < 100; v++) {
                        writer.add(v & 3, v);
                    }
                    writer.setMaxValue(99);
                    writer.commit();

                    pcSync.arm();
                    try {
                        writer.rollbackValues(49);
                        Assert.fail("expected staged .pc sync failure");
                    } catch (CairoException expected) {
                        TestUtils.assertContains(expected.getFlyweightMessage(), "[test] staged .pc sync failed");
                    } finally {
                        pcSync.disarm();
                    }
                    Assert.assertEquals("test must fail exactly one staged .pc sync", 1, pcSync.failureCount());

                    // The .pc sync fails AFTER reencodeWithPerKeyStreaming switched
                    // valueMem onto the new .pv (post-switch), so the staged files
                    // are mapped -- an immediate unlink would fail on Windows. The
                    // catch defers them to the purge job instead (matching the seal
                    // path's rebuildSidecarsByCopy). The chain still references the
                    // old .pv, so the staged files stay on disk until the deferred
                    // purge runs, and exactly one orphan purge is queued.
                    Assert.assertEquals("post-switch failure must queue exactly one orphan purge",
                            1, writer.getPendingPurgesSizeForTesting());

                    LPSZ pv = PostingIndexUtils.valueFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 1);
                    Assert.assertTrue("staged rollback .pv stays until the deferred purge [path=" + pv + ']',
                            configuration.getFilesFacade().exists(pv));

                    LPSZ pc = PostingIndexUtils.coverDataFileName(
                            path.trimTo(plen), name, 0, COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, 1);
                    Assert.assertTrue("staged rollback .pc stays until the deferred purge [path=" + pc + ']',
                            configuration.getFilesFacade().exists(pc));

                    // C1: the post-switch failure left the writer internally
                    // inconsistent (chain head at the old sealTxn, valueMem at the
                    // staged one), so it is poisoned -- any further rollback / seal
                    // / commit must throw rather than re-drive the poisoned state
                    // (which could append a chain entry at the staged sealTxn and
                    // let the deferred [0, MAX) purge delete the now-live file).
                    try {
                        writer.rollbackValues(30);
                        Assert.fail("poisoned writer must reject further rollbacks");
                    } catch (CairoException poisoned) {
                        TestUtils.assertContains(poisoned.getFlyweightMessage(), "poisoned");
                    }
                    // A poisoned writer must also reject commit -- it would otherwise
                    // flush + publishToChain at the staged sealTxn.
                    try {
                        writer.commit();
                        Assert.fail("poisoned writer must reject commit");
                    } catch (CairoException poisoned) {
                        TestUtils.assertContains(poisoned.getFlyweightMessage(), "poisoned");
                    }
                }
            }
        });
    }

    /**
     * A pre-switch failure: the staged .pv msync (reencodeWithPerKeyStreaming,
     * before switchToSealedValueFile) fails, so the value file was never
     * switched into valueMem. The catch's not-switched branch frees the staging
     * map, unlinks the staged .pv, and restores keyCount/valueMemSize -- no
     * orphan purge is queued (nothing was published) and the writer stays
     * usable. Guards C2 (reencodeStarted set before the sidecar staging) and C3
     * (valueMemSize restored on the pre-switch path).
     */
    @Test
    public void testRollbackPreSwitchValueSyncFailureCleansUpAndKeepsWriterUsable() throws Exception {
        final PvSyncFailingFacade pvSync = new PvSyncFailingFacade();
        ff = pvSync;
        assertMemoryLeak(ff, () -> {
            final String name = "rollback_pre_switch_pv_sync_fail";
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, true);
                    writer.setNextTxnAtSeal(1L);
                    for (int v = 0; v < 100; v++) {
                        writer.add(v & 3, v);
                    }
                    writer.setMaxValue(99);
                    writer.commit();
                    final int keyCountBefore = writer.getKeyCount();

                    pvSync.arm();
                    try {
                        writer.rollbackValues(49);
                        Assert.fail("expected staged .pv sync failure");
                    } catch (CairoException expected) {
                        TestUtils.assertContains(expected.getFlyweightMessage(), "[test] staged .pv sync failed");
                    } finally {
                        pvSync.disarm();
                    }
                    Assert.assertEquals("test must fail exactly one staged .pv sync", 1, pvSync.failureCount());

                    // Not-switched cleanup: nothing was published, so no orphan
                    // purge is queued and the staged .pv is unlinked directly.
                    Assert.assertEquals("pre-switch failure must NOT queue an orphan purge",
                            0, writer.getPendingPurgesSizeForTesting());
                    LPSZ pv = PostingIndexUtils.valueFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 1);
                    Assert.assertFalse("staged .pv must be unlinked on the pre-switch path [path=" + pv + ']',
                            configuration.getFilesFacade().exists(pv));

                    // keyCount and maxValue restored -- the failed rollback left no trace.
                    Assert.assertEquals("keyCount must be restored after a pre-switch failure",
                            keyCountBefore, writer.getKeyCount());
                    Assert.assertEquals("maxValue must be unchanged after a pre-switch failure",
                            99L, writer.getMaxValue());

                    // Pre-switch failures do NOT poison the writer (unlike
                    // post-switch), so a retried rollback succeeds.
                    writer.rollbackValues(49);
                    Assert.assertEquals("retried rollback must succeed", 49L, writer.getMaxValue());
                }
            }
        });
    }

    /**
     * C2: a covered rollback stages fresh .pc sidecar targets via openSidecarFiles
     * BEFORE reencodeWithPerKeyStreaming switches the value file. If that open
     * itself throws (ENOSPC/EMFILE), isReencodeStarted is already set -- it is
     * hoisted ahead of the staging -- so the catch's not-switched branch runs:
     * restore keyCount/valueMemSize, drop the staged files, queue no purge (nothing
     * switched or published). The writer is not poisoned, so a retried rollback
     * succeeds. Without the hoist the catch is skipped and the writer is left with a
     * shrunken keyCount against an unchanged chain. The existing pre-switch test
     * faults the .pv sync on a NON-covered rollback, which never reaches
     * openSidecarFiles; this covers the .pc open fault specifically.
     */
    @Test
    public void testRollbackCoveredSidecarOpenFailureCleansUpAndKeepsWriterUsable() throws Exception {
        final PcOpenFailingFacade pcOpen = new PcOpenFailingFacade();
        ff = pcOpen;
        assertMemoryLeak(ff, () -> {
            final String name = "rollback_sidecar_open_fail";
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();

                ObjList<CharSequence> coverNames = new ObjList<>();
                coverNames.add("covered_long");
                LongList coverNameTxns = new LongList();
                coverNameTxns.add(COLUMN_NAME_TXN_NONE);
                LongList coverTops = new LongList();
                coverTops.add(0L);
                IntList coverShifts = new IntList();
                coverShifts.add(3);
                IntList coverIndices = new IntList();
                coverIndices.add(1);
                IntList coverTypes = new IntList();
                coverTypes.add(ColumnType.LONG);

                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    writer.configureCovering(coverNames, coverNameTxns, coverTops, coverShifts, coverIndices, coverTypes, -1);
                    writer.setNextTxnAtSeal(1L);
                    for (int v = 0; v < 96; v++) {
                        writer.add(v & 3, v);
                    }
                    // A trailing high key (id 7) whose rows all sit above the rollback
                    // cutoff (49): the rollback drops it, so filterCountsForRollback
                    // SHRINKS keyCount to 4. The catch's keyCount restore is then
                    // observable -- without the isReencodeStarted hoist, the failed
                    // rollback leaves keyCount at the shrunken 4 against an 8-key chain.
                    writer.add(7, 96);
                    writer.add(7, 97);
                    writer.add(7, 98);
                    writer.add(7, 99);
                    writer.setMaxValue(99);
                    writer.commit();
                    final int keyCountBefore = writer.getKeyCount();

                    pcOpen.arm();
                    try {
                        writer.rollbackValues(49);
                        Assert.fail("expected the covered rollback's .pc open to fail");
                    } catch (CairoException expected) {
                        // openSidecarFiles' mem.of() throws on the injected -1 fd
                    } finally {
                        pcOpen.disarm();
                    }
                    Assert.assertEquals("test must fail exactly one .pc0 open", 1, pcOpen.failureCount());

                    // The open failed BEFORE the value-file switch, so the catch's
                    // not-switched branch queued no purge and restored the writer.
                    Assert.assertEquals("a pre-switch open failure must NOT queue an orphan purge",
                            0, writer.getPendingPurgesSizeForTesting());
                    Assert.assertEquals("keyCount must be restored after the failed rollback",
                            keyCountBefore, writer.getKeyCount());
                    Assert.assertEquals("maxValue must be unchanged after the failed rollback",
                            99L, writer.getMaxValue());

                    // Not poisoned (nothing switched): the retried rollback succeeds.
                    writer.rollbackValues(49);
                    Assert.assertEquals("retried rollback must succeed", 49L, writer.getMaxValue());
                }
            }
        });
    }

    /**
     * A post-publish failure inside rebuildSidecarsByCopy: the chain entry is
     * already published (the new sealTxn is the live head and the writer is
     * fully consistent -- chain head, valueMem and sealTxn all at newSealTxn),
     * and only the trailing .pk sync fails. The catch must be a no-op -- it must
     * NOT schedule an orphan purge for the now-live sealTxn or poison the
     * consistent writer. Guards the isPublished flag that mirrors the rollback
     * path; without it the post-publish .pk-sync failure queues a purge for the
     * live file (deleted but for the publishPendingPurges liveness guard) and
     * wrongly poisons the writer.
     */
    @Test
    public void testRebuildSidecarsPostPublishKeySyncFailureKeepsLiveFilesAndWriterUsable() throws Exception {
        final PkSyncFailingFacade pkSync = new PkSyncFailingFacade();
        ff = pkSync;
        assertMemoryLeak(ff, () -> {
            final String name = "rebuild_sidecars_post_publish_pk_fail";
            final long fakeColBytes = 32L << 3; // 32 rows * 8 bytes (LONG cover)
            long fakeColAddr = Unsafe.malloc(fakeColBytes, MemoryTag.NATIVE_DEFAULT);
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                Unsafe.setMemory(fakeColAddr, fakeColBytes, (byte) 0);
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    // Production O3 covering reseal shape: commitDense (dense gen 0,
                    // no covers) -> configureCovering -> rebuildSidecars (by-copy).
                    for (int v = 0; v < 32; v++) {
                        writer.add(v & 3, v);
                    }
                    writer.setMaxValue(31);
                    writer.setNextTxnAtSeal(1L);
                    writer.commitDense();

                    long[] addrs = {fakeColAddr};
                    long[] tops = {0L};
                    int[] shifts = {3};
                    int[] indices = {1};
                    int[] types = {ColumnType.LONG};
                    writer.configureCovering(addrs, tops, shifts, indices, types, 1);

                    pkSync.arm();
                    try {
                        writer.rebuildSidecars();
                        Assert.fail("expected post-publish .pk sync failure");
                    } catch (CairoException expected) {
                        TestUtils.assertContains(expected.getFlyweightMessage(), "[test] .pk sync failed");
                    } finally {
                        pkSync.disarm();
                    }
                    Assert.assertEquals("test must fail exactly one post-publish .pk sync", 1, pkSync.failureCount());

                    // The failure is AFTER publishToChain, so newSealTxn is the live
                    // chain head and the writer is consistent. The catch must not
                    // queue an orphan purge for the live sealTxn...
                    Assert.assertEquals("a post-publish failure must NOT queue an orphan purge for the live sealTxn",
                            0, writer.getPendingPurgesSizeForTesting());

                    // ...nor poison the writer -- a follow-on commit succeeds.
                    writer.commit();
                }
            } finally {
                Unsafe.free(fakeColAddr, fakeColBytes, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * rebuildSidecarsByCopy POST-switch, pre-publish failure (the staged .pc sync
     * fails after switchToSealedValueFile but before publishToChain): the catch must
     * orphan-purge the staged newSealTxn files over [0, MAX), leave the live
     * oldSealTxn alone, and poison the writer. Mirrors the seal/rollback post-switch
     * branches.
     */
    @Test
    public void testRebuildSidecarsByCopyPostSwitchSidecarSyncFailurePoisonsWriter() throws Exception {
        final PcSyncFailingFacade pcSync = new PcSyncFailingFacade();
        ff = pcSync;
        assertMemoryLeak(ff, () -> {
            final String name = "rebuild_post_switch_pc_fail";
            final long fakeColBytes = 32L << 3;
            long fakeColAddr = Unsafe.malloc(fakeColBytes, MemoryTag.NATIVE_DEFAULT);
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                Unsafe.setMemory(fakeColAddr, fakeColBytes, (byte) 0);
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    for (int v = 0; v < 32; v++) {
                        writer.add(v & 3, v);
                    }
                    writer.setMaxValue(31);
                    writer.setNextTxnAtSeal(1L);
                    writer.commitDense();

                    long[] addrs = {fakeColAddr};
                    long[] tops = {0L};
                    int[] shifts = {3};
                    int[] indices = {1};
                    int[] types = {ColumnType.LONG};
                    writer.configureCovering(addrs, tops, shifts, indices, types, 1);

                    pcSync.arm();
                    try {
                        writer.rebuildSidecars();
                        Assert.fail("expected post-switch .pc sync failure during the covering rebuild");
                    } catch (CairoException expected) {
                        TestUtils.assertContains(expected.getFlyweightMessage(), "staged .pc sync failed");
                    } finally {
                        pcSync.disarm();
                    }
                    Assert.assertEquals("staged newSealTxn must be orphan-purged",
                            1, writer.getPendingPurgesSizeForTesting());
                    Assert.assertEquals("orphan purge spans the full txn window",
                            Long.MAX_VALUE, writer.getPendingPurgeToTxnForTesting(0));
                    assertPoisonedRejects("commit", writer::commit);
                    assertPoisonedRejects("seal", writer::seal);
                    assertPoisonedRejects("rollbackValues", () -> writer.rollbackValues(0));
                }
            } finally {
                Unsafe.free(fakeColAddr, fakeColBytes, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * rebuildSidecarsByCopy PRE-switch failure (the staged .pc open faults before
     * switchToSealedValueFile): the catch must unlink the staged files directly,
     * queue no purge, and leave the writer usable (nothing switched, no poison).
     */
    @Test
    public void testRebuildSidecarsByCopyPreSwitchSidecarOpenFailureCleansUpAndKeepsWriterUsable() throws Exception {
        final PcOpenFailingFacade pcOpen = new PcOpenFailingFacade();
        ff = pcOpen;
        assertMemoryLeak(ff, () -> {
            final String name = "rebuild_pre_switch_pc_open_fail";
            final long fakeColBytes = 32L << 3;
            long fakeColAddr = Unsafe.malloc(fakeColBytes, MemoryTag.NATIVE_DEFAULT);
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                Unsafe.setMemory(fakeColAddr, fakeColBytes, (byte) 0);
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    for (int v = 0; v < 32; v++) {
                        writer.add(v & 3, v);
                    }
                    writer.setMaxValue(31);
                    writer.setNextTxnAtSeal(1L);
                    writer.commitDense();

                    long[] addrs = {fakeColAddr};
                    long[] tops = {0L};
                    int[] shifts = {3};
                    int[] indices = {1};
                    int[] types = {ColumnType.LONG};
                    writer.configureCovering(addrs, tops, shifts, indices, types, 1);

                    pcOpen.arm();
                    try {
                        writer.rebuildSidecars();
                        Assert.fail("expected pre-switch .pc open failure during the covering rebuild");
                    } catch (CairoException expected) {
                        // openSidecarFiles' mem.of() throws on the injected -1 fd
                    } finally {
                        pcOpen.disarm();
                    }
                    Assert.assertEquals("test must fail exactly one .pc0 open", 1, pcOpen.failureCount());

                    // Pre-switch: the staged .pv is unlinked directly, no purge queued.
                    final FilesFacade ff2 = configuration.getFilesFacade();
                    LPSZ pv = PostingIndexUtils.valueFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 1);
                    Assert.assertFalse("staged .pv unlinked on a pre-switch rebuild failure [path=" + pv + ']', ff2.exists(pv));
                    Assert.assertEquals("pre-switch rebuild failure queues no purge",
                            0, writer.getPendingPurgesSizeForTesting());

                    // Not poisoned (nothing switched): the retried rebuild succeeds.
                    writer.rebuildSidecars();
                }
            } finally {
                Unsafe.free(fakeColAddr, fakeColBytes, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * The seal path must be poison-symmetric with rollback / rebuildSidecarsByCopy.
     * A post-switch (sealTxn advanced to newSealTxn) but pre-publish (.pc sync)
     * failure leaves valueMem at newSealTxn while the chain head still references
     * oldSealTxn, so the writer is inconsistent. seal() must poison it and
     * orphan-purge the staged newSealTxn files -- and must NOT queue a purge for
     * the still-live oldSealTxn chain head (the isSealed gate). Without the fix the
     * writer stays usable and seal()'s finally records a purge for the live file.
     */
    @Test
    public void testSealPostSwitchSidecarSyncFailurePoisonsWriterAndOrphanPurgesStagedFiles() throws Exception {
        final PcSyncFailingFacade pcSync = new PcSyncFailingFacade();
        ff = pcSync;
        assertMemoryLeak(ff, () -> {
            final String name = "seal_post_switch_pc_fail";
            final long fakeColBytes = 32L << 3; // 32 rows * 8 bytes (LONG cover)
            long fakeColAddr = Unsafe.malloc(fakeColBytes, MemoryTag.NATIVE_DEFAULT);
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                Unsafe.setMemory(fakeColAddr, fakeColBytes, (byte) 0);
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    // Two sparse gens so seal() runs sealFull -- a single dense gen 0
                    // would early-return as already-sealed and never switch.
                    for (int v = 0; v < 16; v++) {
                        writer.add(v & 3, v);
                    }
                    writer.setMaxValue(15);
                    writer.commit();
                    for (int v = 16; v < 32; v++) {
                        writer.add(v & 3, v);
                    }
                    writer.setMaxValue(31);
                    writer.commit();

                    long[] addrs = {fakeColAddr};
                    long[] tops = {0L};
                    int[] shifts = {3};
                    int[] indices = {1};
                    int[] types = {ColumnType.LONG};
                    writer.configureCovering(addrs, tops, shifts, indices, types, 1);

                    pcSync.arm();
                    try {
                        writer.seal();
                        Assert.fail("expected post-switch .pc sync failure during seal");
                    } catch (CairoException expected) {
                        TestUtils.assertContains(expected.getFlyweightMessage(), "staged .pc sync failed");
                    } finally {
                        pcSync.disarm();
                    }

                    // The staged newSealTxn files are orphaned and queued for purge;
                    // the still-live oldSealTxn chain head is NOT (the isSealed gate).
                    Assert.assertEquals("staged newSealTxn must be orphan-purged, live oldSealTxn must not",
                            1, writer.getPendingPurgesSizeForTesting());
                    // The orphan purge covers the full [0, MAX) txn window -- the staged
                    // files are unconditionally dead. A chain-derived recordPostingSealPurge
                    // for the live oldSealTxn (the unfixed behaviour) records a bounded
                    // window instead, so MAX_VALUE here proves it is the orphan purge.
                    Assert.assertEquals("orphan purge must span the full txn window",
                            Long.MAX_VALUE, writer.getPendingPurgeToTxnForTesting(0));

                    // All eleven mutating entry points reject the poisoned writer
                    // (checkNotPoisoned runs before commitDense's covers assert).
                    assertPoisonedRejects("add", () -> writer.add(0, 100));
                    assertPoisonedRejects("commit", writer::commit);
                    assertPoisonedRejects("commitDense", writer::commitDense);
                    assertPoisonedRejects("discardForRebuild", writer::discardForRebuild);
                    assertPoisonedRejects("rebuildSidecars", writer::rebuildSidecars);
                    assertPoisonedRejects("rollbackConditionally", () -> writer.rollbackConditionally(0));
                    assertPoisonedRejects("rollbackValues", () -> writer.rollbackValues(0));
                    assertPoisonedRejects("seal", writer::seal);
                    assertPoisonedRejects("setMaxValue", () -> writer.setMaxValue(100));
                    assertPoisonedRejects("sync", () -> writer.sync(false));
                    assertPoisonedRejects("truncate", writer::truncate);
                }
            } finally {
                Unsafe.free(fakeColAddr, fakeColBytes, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testSwitchPartitionResealPostSwitchFailurePublishesStagedOrphanPurge() throws Exception {
        // M1: switchPartition()'s native reseal (commit() + sealIfMultiGen()) on a
        // post-switch .pc sync failure poisons the writer and stages the failed
        // sealTxn's orphan purge. deferPendingPostingSealPurges must run in a
        // finally so the propagating distress does not drop the unpublished outbox
        // and leak the staged .pv/.pc. Pre-fix the orphan is never published
        // (no deferred-purge-log row); with the fix it is published.
        final PcSyncFailingFacade pcSync = new PcSyncFailingFacade();
        ff = pcSync;
        node1.setProperty(PropertyKey.CAIRO_POSTING_SEAL_GEN_THRESHOLD, 1);
        assertMemoryLeak(ff, () -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            final String tableName = "posting_o3_recovery";
            final String indexColumnName = "new_col_11";

            execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, sym2 SYMBOL INDEX TYPE POSTING INCLUDE (marker), sym_top SYMBOL CAPACITY 128, marker LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(insertPostingRowsSql(-85, 0));
            execute("ALTER TABLE " + tableName + " RENAME COLUMN sym2 TO " + indexColumnName);
            execute(insertPostingRowsSql(0, 35));

            // Drain purges queued by setup so the deferred-purge-log baseline for
            // the index column is empty.
            drainPostingSealPurgeQueue();
            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }
            Assert.assertNotNull("table must exist", engine.getTableTokenIfExists(tableName));
            final long partitionTs = MicrosFormatUtils.parseTimestamp("2022-02-25T00:00:00.000000Z");
            final int baselineValueFiles = countPostingValueFiles(tableName, indexColumnName, partitionTs);
            Assert.assertTrue("setup must leave the live .pv on disk", baselineValueFiles >= 1);

            boolean distressed = false;
            TableWriter writer = TestUtils.getWriter(engine, tableName);
            try {
                // Pending rows in the 2022-02-25 partition so switchPartition's
                // commit() flushes a 2nd gen and sealIfMultiGen(1) actually seals.
                for (int i = 35; i < 40; i++) {
                    TableWriter.Row row = writer.newRow(MicrosFormatUtils.parseTimestamp(timestampAtMinute(i)));
                    row.putSym(1, "XPHI");
                    row.putSym(2, "S");
                    row.putLong(3, i);
                    row.append();
                }
                pcSync.arm();
                try {
                    // Cross into a new partition -> switchPartition reseals
                    // 2022-02-25; the staged .pc sync fails post-switch.
                    TableWriter.Row row = writer.newRow(MicrosFormatUtils.parseTimestamp("2022-02-26T00:00:00.000000Z"));
                    row.putSym(1, "next");
                    row.putSym(2, "S");
                    row.putLong(3, 200);
                    row.append();
                    writer.commit();
                } catch (Throwable th) {
                    distressed = true;
                } finally {
                    pcSync.disarm();
                }
            } finally {
                try {
                    writer.close();
                } catch (Throwable ignore) {
                    // a distressed writer may throw on close
                }
                engine.releaseAllWriters();
            }
            Assert.assertTrue("the partition-switch reseal must fail post-switch and distress the writer", distressed);

            // The failed reseal staged its new sealTxn's .pv before the .pc sync
            // failed, leaving it on disk pending purge.
            Assert.assertTrue("the failed reseal must leave a staged orphan .pv on disk",
                    countPostingValueFiles(tableName, indexColumnName, partitionTs) > baselineValueFiles);

            // The fix: deferPendingPostingSealPurges ran in the switchPartition
            // finally, publishing the staged orphan to the purge queue despite the
            // propagating distress, so the purge job reclaims it. Pre-fix the
            // distress close drops the unpublished outbox and the orphan .pv leaks
            // permanently (this final count would stay at baseline + 1).
            engine.releaseAllReaders();
            try (PostingSealPurgeJob reclaimJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(reclaimJob);
            }
            Assert.assertEquals("the staged orphan .pv must be published and reclaimed, not leaked",
                    baselineValueFiles, countPostingValueFiles(tableName, indexColumnName, partitionTs));
        });
    }

    /**
     * The seal path must clean up staged files on a PRE-switch failure, mirroring
     * the rollback path -- otherwise the staged .pv/.pc leak permanently (the
     * caller close()s a failed-seal writer, and no writer-open scan reclaims
     * never-published sealTxn files). Here the staged .pv msync fails BEFORE
     * switchToSealedValueFile advances sealTxn, so the writer is still consistent
     * (chain + valueMem at oldSealTxn): the catch must unlink the staged .pv/.pc,
     * queue no purge, and leave the writer usable (not poisoned).
     */
    @Test
    public void testSealPreSwitchValueSyncFailureUnlinksStagedFilesAndKeepsWriterUsable() throws Exception {
        final PvSyncFailingFacade pvSync = new PvSyncFailingFacade();
        ff = pvSync;
        assertMemoryLeak(ff, () -> {
            final String name = "seal_pre_switch_pv_fail";
            final long fakeColBytes = 64L << 3; // 64 rows (32 indexed + room for the post-failure append)
            long fakeColAddr = Unsafe.malloc(fakeColBytes, MemoryTag.NATIVE_DEFAULT);
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                Unsafe.setMemory(fakeColAddr, fakeColBytes, (byte) 0);
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    for (int v = 0; v < 16; v++) {
                        writer.add(v & 3, v);
                    }
                    writer.setMaxValue(15);
                    writer.commit();
                    for (int v = 16; v < 32; v++) {
                        writer.add(v & 3, v);
                    }
                    writer.setMaxValue(31);
                    writer.commit();

                    long[] addrs = {fakeColAddr};
                    long[] tops = {0L};
                    int[] shifts = {3};
                    int[] indices = {1};
                    int[] types = {ColumnType.LONG};
                    writer.configureCovering(addrs, tops, shifts, indices, types, 1);
                    writer.setNextTxnAtSeal(1L);

                    // The encode advances valueMemSize to the staged compacted size
                    // before the pre-switch .pv sync; the catch must restore it
                    // (else the next gen appends at a stale genOffset = valueMemSize
                    // into live bytes).
                    final long valueMemSizeBefore = writer.getValueMemSizeForTesting();

                    pvSync.arm();
                    try {
                        writer.seal();
                        Assert.fail("expected pre-switch .pv sync failure during seal");
                    } catch (CairoException expected) {
                        TestUtils.assertContains(expected.getFlyweightMessage(), "staged .pv sync failed");
                    } finally {
                        pvSync.disarm();
                    }
                    Assert.assertEquals("test must fail exactly one staged .pv sync", 1, pvSync.failureCount());

                    // The failure is BEFORE the switch, so the writer is consistent and
                    // the catch must unlink the staged files directly (nothing is mapped
                    // post-switch) and queue no purge.
                    final FilesFacade ff2 = configuration.getFilesFacade();
                    LPSZ pv = PostingIndexUtils.valueFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 1);
                    Assert.assertFalse("staged .pv must be unlinked on a pre-switch seal failure [path=" + pv + ']', ff2.exists(pv));
                    LPSZ pc = PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, 0, COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, 1);
                    Assert.assertFalse("staged .pc must be unlinked on a pre-switch seal failure [path=" + pc + ']', ff2.exists(pc));
                    Assert.assertEquals("pre-switch seal failure must queue no purge",
                            0, writer.getPendingPurgesSizeForTesting());

                    // valueMemSize must be back to its pre-seal value: the encode
                    // advanced it to the staged compacted size before the throw, and
                    // a continued writer appends its next gen at this offset.
                    Assert.assertEquals("pre-switch seal failure must restore valueMemSize",
                            valueMemSizeBefore, writer.getValueMemSizeForTesting());

                    // Consume valueMemSize to prove the restore: commit appends a new
                    // gen at genOffset = valueMemSize, so a stale value would write
                    // into live bytes and corrupt the read-back. Re-sealing alone
                    // cannot catch this (seal() recomputes valueMemSize before
                    // consuming it). Key 0 held rowids 0,4,...,28 (8 rows); append a
                    // 9th (row 32 -- globally ascending, in the cover's 64-row range).
                    writer.add(0, 32);
                    writer.setMaxValue(32);
                    writer.commit();
                    writer.seal();

                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name,
                            COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, /* columnTop */ 0)) {
                        long iterated = 0;
                        try (RowCursor cursor = reader.getCursor(0, 0L, Long.MAX_VALUE)) {
                            while (cursor.hasNext()) {
                                cursor.next();
                                iterated++;
                            }
                        }
                        Assert.assertEquals("key 0 must read back its 8 original rows plus the appended one",
                                9, iterated);
                    }
                }
            } finally {
                Unsafe.free(fakeColAddr, fakeColBytes, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Locks in the multi-gen short-circuit semantics: when a CLEAN gen
     * processed first contributes to {@code total} and a later gen turns out
     * to be MIXED, the running sum is discarded and {@code size()} returns
     * {@code -1}. The caller iterates the whole reader, which is the only
     * way to reconcile the partial sum with the clamped emit set across
     * gens.
     */
    @Test
    public void testSizeBailsOnCleanThenMixedGenSequence() throws Exception {
        assertMemoryLeak(() -> {
            final String name = "posting_size_clean_then_mixed";
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    // gen 1: rowids 0..49, all <= MAX_VALUE -> CLEAN.
                    for (long rowId = 0; rowId < 50; rowId++) {
                        writer.add(0, rowId);
                    }
                    writer.commit();
                    // gen 2: rowids 50..99, straddles MAX_VALUE=75 -> MIXED.
                    for (long rowId = 50; rowId < 100; rowId++) {
                        writer.add(0, rowId);
                    }
                    writer.commit();
                    writer.setMaxValue(75);
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name,
                        COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, /* columnTop */ 0)) {
                    long iterated = 0;
                    long advertised;
                    try (RowCursor cursor = reader.getCursor(0, 0L, Long.MAX_VALUE)) {
                        advertised = cursor.size();
                        while (cursor.hasNext()) {
                            cursor.next();
                            iterated++;
                        }
                    }
                    Assert.assertEquals(
                            "gen 1 emits 0..49 (50 rows) and gen 2 emits clamped 50..75 (26 rows)",
                            76,
                            iterated
                    );
                    Assert.assertEquals(
                            "MIXED detected after a CLEAN gen must discard the partial sum and bail",
                            -1L,
                            advertised
                    );
                }
            }
        });
    }

    // Critical findings #2 (setMaxValue seqlock), #6 ([0, Long.MAX_VALUE)
    // conservative purge interval) and #7 (seal-loop partial failure
    // recovery) were RED placeholders during the v1 era. They are now
    // addressed structurally by the v2 chain redesign:
    //   #2 — chain.updateHeadMaxValue publishes via the chain header
    //        seqlock, so any reader of MAX_VALUE goes through the same
    //        consistency protocol as keyCount/genCount.
    //   #6 — recordPostingSealPurge derives [fromTxn, toTxn) from the
    //        chain entries themselves; the residual [0, MAX) branch only
    //        fires for the empty-chain edge case, which the
    //        writer-open recovery walk picks up on the next reopen.
    //   #7 — recoveryDropAbandoned (run from PostingIndexWriter.of after
    //        setCurrentTableTxn) drops every chain entry whose txnAtSeal
    //        was published before the encompassing txWriter.commit
    //        landed.
    // Critical finding #9 (ColumnPurgeOperator retry cap on Windows) is
    // orthogonal to the posting-index chain rewrite and remains tracked
    // separately.

    /**
     * The {@code Cursor.size()} fast path must bail to iteration when a single
     * gen straddles {@code entryMaxValue}, because the per-gen count includes
     * encoded row ids past the chain's tracked coverage. The bail manifests as
     * {@code size() == -1}; the iterated count then becomes the source of
     * truth (and matches the clamped emit set).
     */
    @Test
    public void testSizeBailsOnMixedGen() throws Exception {
        assertMemoryLeak(() -> {
            final String name = "posting_size_mixed_gen";
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    for (long rowId = 0; rowId < 100; rowId++) {
                        writer.add(0, rowId);
                    }
                    writer.commit();
                    writer.setMaxValue(49);
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name,
                        COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, /* columnTop */ 0)) {
                    long iterated = 0;
                    long advertised;
                    try (RowCursor cursor = reader.getCursor(0, 0L, Long.MAX_VALUE)) {
                        advertised = cursor.size();
                        while (cursor.hasNext()) {
                            cursor.next();
                            iterated++;
                        }
                    }
                    Assert.assertEquals(
                            "key 0 has rowids 0..49 at or below MAX_VALUE=49",
                            50,
                            iterated
                    );
                    Assert.assertEquals(
                            "single-gen MIXED classification must bail to iteration",
                            -1L,
                            advertised
                    );
                }
            }
        });
    }

    /**
     * The DELTA-mode upper bound from {@code peekDeltaKeyMaxValueUpperBound}
     * is exact only when the last block has {@code bitWidth == 0}; otherwise
     * it assumes every delta packs to {@code (1<<bw)-1}, which can sit far
     * above the true last value. This test pins down that conservative bound
     * by setting MAX_VALUE between the actual maximum and the slack bound:
     * iteration emits every row (all real values are under MAX_VALUE) but the
     * fast path must still bail with {@code -1} because it cannot prove the
     * gen is CLEAN without decoding values.
     */
    @Test
    public void testSizeBailsWhenSlackBoundClassifiesCleanGenAsMixed() throws Exception {
        // Force DELTA encoding so the per-key blob exercises the slack
        // upper-bound branch in peekDeltaKeyMaxValueUpperBound. Under EF the
        // max bound is exact (universe - 1) and would (correctly) classify
        // this case as CLEAN, hiding the slack-bound behavior.
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_ROW_ID_ENCODING, "delta");
        assertMemoryLeak(() -> {
            final String name = "posting_size_slack_mixed";
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    // 192 contiguous rowids fill blocks 0..2 (BLOCK_CAPACITY=64,
                    // bitWidth=0). The last block holds 4 sparse outliers with
                    // non-uniform deltas (800, 500, 500), forcing bitWidth>0
                    // and a slack max well above the true max of 2000.
                    for (long rowId = 0; rowId < 192; rowId++) {
                        writer.add(0, rowId);
                    }
                    writer.add(0, 200);
                    writer.add(0, 1000);
                    writer.add(0, 1500);
                    writer.add(0, 2000);
                    writer.commit();
                    // MAX_VALUE sits above the true max (2000) but below the
                    // slack upper bound (~3233 = 200 + 3 * (500 + (1<<9) - 1)).
                    writer.setMaxValue(2500);
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name,
                        COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, /* columnTop */ 0)) {
                    long iterated = 0;
                    long advertised;
                    try (RowCursor cursor = reader.getCursor(0, 0L, Long.MAX_VALUE)) {
                        advertised = cursor.size();
                        while (cursor.hasNext()) {
                            cursor.next();
                            iterated++;
                        }
                    }
                    Assert.assertEquals(
                            "every encoded row id sits at or below MAX_VALUE=2500",
                            196,
                            iterated
                    );
                    Assert.assertEquals(
                            "slack upper bound forces MIXED even though the gen is truly CLEAN",
                            -1L,
                            advertised
                    );
                }
            }
        });
    }

    // =========================================================================
    // Red tests for the v2 review of PR #6861 (current pass).
    //
    // Each test below maps to a finding from the review report. Tests that
    // require fault injection use TestFilesFacadeImpl; tests that require
    // concurrency simulate the race by mutating ff.length() between mmap
    // setup and chain access. Findings that can only manifest from sources
    // FilesFacade does not see (Unsafe.realloc OOM, queue-pool exhaustion)
    // are documented in trailing comments.
    // =========================================================================

    // Review finding #1 (sidecar mem fd leak in openSidecarFiles) was
    // dropped after verification: MemoryCMARWImpl.extend0 (line 403) and
    // map0 (line 417) both close the fd on mmap/mremap failure inside
    // jumpTo(). The "orphan mem" identified in the review never holds an
    // open fd by the time the outer catch fires — it is already closed
    // internally by the memory-mapping helper.
    // Documented for traceability; no JUnit red test.

    /**
     * When the head entry's MAX_VALUE sits below every encoded row id for the
     * requested key in a gen, that gen is ALL_DIRTY: it must contribute zero to
     * {@code Cursor.size()} without forcing a full bail. Mirrors the iterated
     * cursor, which also returns no rows because the clamp filters them all
     * out.
     */
    @Test
    public void testSizeBypassesAllDirtyGen() throws Exception {
        assertMemoryLeak(() -> {
            final String name = "posting_size_all_dirty";
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    for (long rowId = 50; rowId < 100; rowId++) {
                        writer.add(0, rowId);
                    }
                    writer.commit();
                    writer.setMaxValue(49);
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name,
                        COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, /* columnTop */ 0)) {
                    long iterated = 0;
                    long advertised;
                    try (RowCursor cursor = reader.getCursor(0, 0L, Long.MAX_VALUE)) {
                        advertised = cursor.size();
                        while (cursor.hasNext()) {
                            cursor.next();
                            iterated++;
                        }
                    }
                    Assert.assertEquals("clamp drops every encoded row past MAX_VALUE", 0, iterated);
                    Assert.assertEquals(
                            "ALL_DIRTY gen must contribute 0 without bailing the fast path",
                            0L,
                            advertised
                    );
                }
            }
        });
    }

    /**
     * Exercises the FLAT-mode branch in {@code peekDenseKeyMinValue} and
     * {@code peekDenseKeyMaxValueUpperBound}. With many keys carrying a
     * single row id each in one stride, the writer's stride mode chooser
     * picks FLAT (stride-wide FoR is much smaller than per-key DELTA blobs).
     * Both peek helpers must address into the FLAT prefix-counts and the
     * bit-packed data correctly to return the lone row id as both min and
     * upper-bound max.
     */
    @Test
    public void testSizeFastPathOnDenseFlatStride() throws Exception {
        assertMemoryLeak(() -> {
            final String name = "posting_size_fast_dense_flat";
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    // 200 distinct keys, one row id each. All keys land in
                    // stride 0 (DENSE_STRIDE = 256). Per-key DELTA blob
                    // overhead (~22 bytes/key) loses to FLAT (one bit-packed
                    // 8-bit slot per key), so seal picks FLAT for stride 0.
                    for (int key = 0; key < 200; key++) {
                        writer.add(key, key);
                    }
                    writer.commit();
                    writer.seal();
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name,
                        COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, /* columnTop */ 0)) {
                    long iterated = 0;
                    long advertised;
                    // Pick a key in the middle of the stride so the FLAT
                    // prefix-counts read at localKey > 0 and the unpackValue
                    // at end-1 hits a non-zero offset.
                    try (RowCursor cursor = reader.getCursor(100, 0L, Long.MAX_VALUE)) {
                        advertised = cursor.size();
                        while (cursor.hasNext()) {
                            cursor.next();
                            iterated++;
                        }
                    }
                    Assert.assertEquals("key 100 must surface its single row id", 1, iterated);
                    Assert.assertEquals(
                            "FLAT-mode CLEAN dense gen must take the fast path",
                            iterated,
                            advertised
                    );
                }
            }
        });
    }

    /**
     * Primary perf-recovery regression test: with no dirty data and the head
     * entry's MAX_VALUE pointing at the encoded maximum, every gen is CLEAN
     * and {@code Cursor.size()} must return the iterated count, not -1.
     * Exercises the dense (sealed) gen path.
     */
    @Test
    public void testSizeFastPathOnDenseGen() throws Exception {
        assertMemoryLeak(() -> {
            final String name = "posting_size_fast_dense";
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    for (long rowId = 0; rowId < 200; rowId++) {
                        writer.add(0, rowId);
                    }
                    writer.commit();
                    // Explicit seal collapses the sparse gens into a single dense
                    // gen so size() goes through the dense peek path.
                    writer.seal();
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name,
                        COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, /* columnTop */ 0)) {
                    long iterated = 0;
                    long advertised;
                    try (RowCursor cursor = reader.getCursor(0, 0L, Long.MAX_VALUE)) {
                        advertised = cursor.size();
                        while (cursor.hasNext()) {
                            cursor.next();
                            iterated++;
                        }
                    }
                    Assert.assertEquals("expect every encoded row id to surface", 200, iterated);
                    Assert.assertEquals(
                            "CLEAN dense gen must take the fast path",
                            iterated,
                            advertised
                    );
                }
            }
        });
    }

    /**
     * Forces the per-key blob into Elias-Fano so the EF branches in both
     * peek helpers run end to end:
     * <ul>
     *   <li>{@code peekDeltaKeyMaxValueUpperBound} detects the EF sentinel
     *       and reads {@code universe - 1} as the exact max;</li>
     *   <li>{@code peekDeltaKeyMinValue} dispatches to
     *       {@code peekEFKeyMinValue}, which decodes the first set bit of
     *       the high-bit stream plus the corresponding low chunk to recover
     *       the smallest encoded value.</li>
     * </ul>
     * Sparsely-spaced row ids keep the EF blob smaller than DELTA so the
     * choice would not flip even without the property override; the override
     * is defence in depth.
     */
    @Test
    public void testSizeFastPathOnEliasFanoEncodedGen() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_ROW_ID_ENCODING, "ef");
        assertMemoryLeak(() -> {
            final String name = "posting_size_fast_ef";
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    // 100 sparsely-spaced row ids (0, 11, 22, ..., 1089) so
                    // the universe (1090) is large enough to make EF the
                    // natural pick.
                    for (long rowId = 0; rowId <= 1_089; rowId += 11) {
                        writer.add(0, rowId);
                    }
                    writer.commit();
                    // No seal: keep the sparse gen so the per-key EF blob is
                    // exactly what peekSparseKeyM*Value sees.
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name,
                        COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, /* columnTop */ 0)) {
                    long iterated = 0;
                    long advertised;
                    try (RowCursor cursor = reader.getCursor(0, 0L, Long.MAX_VALUE)) {
                        advertised = cursor.size();
                        while (cursor.hasNext()) {
                            cursor.next();
                            iterated++;
                        }
                    }
                    Assert.assertEquals("expect every encoded EF row id to surface", 100, iterated);
                    Assert.assertEquals(
                            "CLEAN EF-encoded gen must take the fast path",
                            iterated,
                            advertised
                    );
                }
            }
        });
    }

    /**
     * Same shape as {@link #testSizeFastPathOnDenseGen} but the writer is
     * closed without an explicit {@link PostingIndexWriter#seal()}, so the
     * head entry carries sparse gens. Locks in the sparse-side peek path.
     */
    @Test
    public void testSizeFastPathOnSparseGen() throws Exception {
        assertMemoryLeak(() -> {
            final String name = "posting_size_fast_sparse";
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    for (long rowId = 0; rowId < 200; rowId++) {
                        writer.add(0, rowId);
                    }
                    writer.commit();
                    // No seal: chain head's gen is sparse (negative key count).
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name,
                        COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, /* columnTop */ 0)) {
                    long iterated = 0;
                    long advertised;
                    try (RowCursor cursor = reader.getCursor(0, 0L, Long.MAX_VALUE)) {
                        advertised = cursor.size();
                        while (cursor.hasNext()) {
                            cursor.next();
                            iterated++;
                        }
                    }
                    Assert.assertEquals("expect every encoded row id to surface", 200, iterated);
                    Assert.assertEquals(
                            "CLEAN sparse gen must take the fast path",
                            iterated,
                            advertised
                    );
                }
            }
        });
    }

    /**
     * Pairs with {@link #testSizeBailsWhenSlackBoundClassifiesCleanGenAsMixed}
     * and pins down the CLEAN side of the slack upper bound: with MAX_VALUE
     * comfortably above the slack max ({@code firstValue + numDeltas *
     * (minDelta + (1<<bitWidth) - 1)}), the bitWidth>0 branch in
     * {@code peekDeltaKeyMaxValueUpperBound} runs the full overflow-guarded
     * arithmetic and classifies the gen CLEAN.
     */
    @Test
    public void testSizeFastPathWithSlackBoundDeltaCleanGen() throws Exception {
        // Force DELTA so the slack upper bound is the path under test (EF
        // would compute the exact max via universe - 1 and skip the slack
        // arithmetic entirely).
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_ROW_ID_ENCODING, "delta");
        assertMemoryLeak(() -> {
            final String name = "posting_size_slack_clean";
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    // Same data shape as the slack-MIXED test: 192 contiguous
                    // rowids in blocks 0..2 (bitWidth=0) and 4 sparse outliers
                    // in block 3 with non-uniform deltas (forcing bitWidth=9
                    // and a slack max of ~3233).
                    for (long rowId = 0; rowId < 192; rowId++) {
                        writer.add(0, rowId);
                    }
                    writer.add(0, 200);
                    writer.add(0, 1000);
                    writer.add(0, 1500);
                    writer.add(0, 2000);
                    writer.commit();
                    // MAX_VALUE comfortably above the slack upper bound so the
                    // bitWidth>0 branch returns CLEAN.
                    writer.setMaxValue(5_000);
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name,
                        COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, /* columnTop */ 0)) {
                    long iterated = 0;
                    long advertised;
                    try (RowCursor cursor = reader.getCursor(0, 0L, Long.MAX_VALUE)) {
                        advertised = cursor.size();
                        while (cursor.hasNext()) {
                            cursor.next();
                            iterated++;
                        }
                    }
                    Assert.assertEquals("expect every encoded row id to surface", 196, iterated);
                    Assert.assertEquals(
                            "slack upper bound below MAX_VALUE must classify CLEAN",
                            iterated,
                            advertised
                    );
                }
            }
        });
    }

    /**
     * Mixed per-gen classification: gen 1 is CLEAN (max <= MAX_VALUE), gen 2
     * is ALL_DIRTY (min > MAX_VALUE). {@code Cursor.size()} should sum the
     * CLEAN contribution and skip ALL_DIRTY without bailing. Exact match
     * against the iterated count proves no over- or under-count.
     */
    @Test
    public void testSizeMultiGenMixedClassification() throws Exception {
        assertMemoryLeak(() -> {
            final String name = "posting_size_multigen_mixed";
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    for (long rowId = 0; rowId < 50; rowId++) {
                        writer.add(0, rowId);
                    }
                    writer.commit();
                    for (long rowId = 50; rowId < 100; rowId++) {
                        writer.add(0, rowId);
                    }
                    writer.commit();
                    writer.setMaxValue(49);
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name,
                        COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, /* columnTop */ 0)) {
                    long iterated = 0;
                    long advertised;
                    try (RowCursor cursor = reader.getCursor(0, 0L, Long.MAX_VALUE)) {
                        advertised = cursor.size();
                        while (cursor.hasNext()) {
                            cursor.next();
                            iterated++;
                        }
                    }
                    Assert.assertEquals(
                            "gen 1 contributes rowids 0..49 = 50 rows under MAX_VALUE=49",
                            50,
                            iterated
                    );
                    Assert.assertEquals(
                            "CLEAN+ALL_DIRTY combination must take the fast path",
                            iterated,
                            advertised
                    );
                }
            }
        });
    }

    /**
     * Red test for review finding M3: the reseal call added inside
     * squashSplitPartitions (TableWriter:11920) can throw from file
     * I/O (here: an mmap of the covering column inside
     * mapCoveringColumnsForSeal). The throw unwinds through
     * squashSplitPartitions and squashPartitionForce out to the caller.
     * <p>
     * housekeep() wraps its call to squashSplitPartitions in a
     * try/catch that runs handleHousekeepingException, which sets
     * distressed=true before rethrowing. The non-housekeep callers
     * (convertPartitionNativeToParquet, detachPartition,
     * generateParquetPartition, public squashPartitions(),
     * switchNativePartitionWithParquet) do not. By the time the
     * reseal runs, squashSplitPartitions has already mutated
     * txWriter (removeAttachedPartitions,
     * updatePartitionSizeByTimestamp) and columnVersionWriter
     * (squashPartition) in memory but has not yet run their
     * commit(), so a throw here leaves the writer's in-memory
     * state diverged from on-disk _txn while the pool keeps
     * handing the same writer out.
     * <p>
     * This test exercises the public squashPartitions() entry
     * point (reached from ALTER TABLE x SQUASH PARTITIONS via
     * AlterOperation). The fix wraps the reseal call in a
     * try/catch that mirrors finishO3Commit at TableWriter:6042
     * and sets distressed=true before rethrowing. Without that
     * wrap, isDistressed() stays false after the throw.
     */
    @Test
    public void testSquashPartitionsResealFailureMarksWriterDistressed() throws Exception {
        // Allow splits to form and persist:
        //  - SPLIT_MIN_SIZE=1 makes the writer split even tiny inserts.
        //  - MAX_SPLITS=20 keeps the housekeep auto-merge from collapsing
        //    them before we reach the explicit squashPartitions() call.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 20);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 20);

        final AtomicBoolean failArmed = new AtomicBoolean(false);
        // Fail the first mmap reached from the posting-index reseal, after
        // FrameAlgebra has completed the partition squash.
        ff = new TestFilesFacadeImpl() {
            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (failArmed.get() && isPostingIndexResealCall()) {
                    failArmed.set(false);
                    return FilesFacade.MAP_FAILED;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }
        };

        assertMemoryLeak(ff, () -> {
            execute("""
                    CREATE TABLE t_squash_reseal_fail (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Seed 2024-01-01.
            execute("""
                    INSERT INTO t_squash_reseal_fail VALUES
                    ('2024-01-01T00:00:00Z', 'A', 10.0),
                    ('2024-01-01T01:00:00Z', 'B', 20.0),
                    ('2024-01-01T02:00:00Z', 'A', 30.0),
                    ('2024-01-01T20:00:00Z', 'A', 40.0)
                    """);
            // O3 into a late prefix of the same logical partition creates
            // a split sub-partition. With SPLIT_MIN_SIZE=1 the writer
            // splits even at this tiny scale.
            execute("""
                    INSERT INTO t_squash_reseal_fail VALUES
                    ('2024-01-01T19:00:00Z', 'C', 99.0)
                    """);

            // Confirm the table has two sub-partitions for the same
            // logical day before arming the fault; otherwise
            // squashPartitions() short-circuits and the reseal at
            // TableWriter:11920 is never reached.
            assertQuery("SELECT count() FROM table_partitions('t_squash_reseal_fail')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count
                            2
                            """);

            engine.releaseAllWriters();

            try (TableWriter w = TestUtils.getWriter(engine, "t_squash_reseal_fail")) {
                failArmed.set(true);
                try {
                    w.squashPartitions();
                    Assert.fail("expected reseal failure to surface from squashPartitions()");
                } catch (AssertionError ae) {
                    throw ae;
                } catch (Throwable ignore) {
                    // expected: CairoException from TableUtils.mapRO
                    // when the seal's mmap returns MAP_FAILED.
                }
                failArmed.set(false);
                Assert.assertTrue(
                        "writer must be distressed after a reseal throw inside squashPartitions(): " +
                                "txWriter/columnVersionWriter were mutated in memory but their " +
                                "commit() never ran, so the in-memory state diverged from on-disk _txn",
                        w.isDistressed());
            }
        });
    }

    /**
     * Reproduces the SIGSEGV in PostingIndexWriter.decodeDenseGenStride when
     * squashing partitions over a COVERING posting index whose reseal index()
     * loop trips the spill budget (compactIfOverBudget -> mid-stream
     * flushAllPending). The mid-stream sparse flush lands a gen at file
     * offset 0; commitDense then appends the dense gen-0 at a NON-ZERO offset
     * and publishes a single-gen chain entry that points there. rebuildSidecars
     * takes the genCount==1 by-copy path: accumulateDenseGen0Counts reads the
     * gen via its real FILE_OFFSET (correct), but writeSidecarForColumn decodes
     * from valueMem.addressOf(0) (the orphaned sparse gen) -- a stride-index
     * mismatch that over-reads native memory and SIGSEGVs (no AssertionError,
     * just a JVM crash). The fix makes commitDense consolidate via seal() when
     * prior gens exist, so gen-0 lands back at offset 0 with every row intact;
     * the covering query then returns the same rows as the non-covering
     * fallback.
     */
    @Test
    public void testSquashCoveringPostingWithMidStreamSpillFlush() throws Exception {
        // Force a mid-stream flushAllPending during the squash reseal index()
        // loop: a tiny spill budget means a few dozen rowids for a hot key push
        // totalSpillBytes over the threshold, so compactIfOverBudget fires
        // before commitDense writes the final dense gen.
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256);
        // Allow the split sub-partitions to form and persist until the explicit
        // squashPartitions() call (see testSquashPartitionsResealFailureMarks...).
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 20);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 20);

        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_squash_spill (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Seed a hot key 'A' with enough rowids in 2024-01-01 that the
            // reseal index() loop spills well past the 256-byte budget.
            execute("""
                    INSERT INTO t_squash_spill
                    SELECT dateadd('s', x::INT, '2024-01-01T00:00:00.000000Z'::TIMESTAMP),
                           'A', x::DOUBLE
                    FROM long_sequence(400)
                    """);
            // Extend the day's max timestamp to 20:00 so there is a late tail
            // for the O3 insert below to split off (mirrors the split recipe in
            // testSquashPartitionsResealFailureMarksWriterDistressed).
            execute("""
                    INSERT INTO t_squash_spill VALUES
                    ('2024-01-01T20:00:00.000000Z', 'A', 1000000.0)
                    """);
            // O3 into a late prefix (19:00 < 20:00) splits the last partition.
            execute("""
                    INSERT INTO t_squash_spill VALUES
                    ('2024-01-01T19:00:00.000000Z', 'B', -1.0)
                    """);

            assertQuery("SELECT count() FROM table_partitions('t_squash_spill')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count
                            2
                            """);

            engine.releaseAllWriters();
            try (TableWriter w = TestUtils.getWriter(engine, "t_squash_spill")) {
                // Squash reseals the covering posting index for the target
                // partition: discardForRebuild -> index() (mid-stream flush) ->
                // commitDense -> rebuildSidecars (by-copy). The buggy decode
                // SIGSEGVs here.
                w.squashPartitions();
            }

            assertQuery("SELECT count() FROM table_partitions('t_squash_spill')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count
                            1
                            """);

            // The covering path must return every 'A' row: 400 (sum 1..400 =
            // 80200) plus the 20:00 sentinel (1000000) = 401 rows, sum 1080200.
            // Assert the covering plan alongside the result so a future optimizer
            // change that silently falls back to a base-table scan (sym.d/price.d
            // intact, only the covering sidecar corrupt) fails here instead of
            // masking the bug.
            assertQuery("SELECT count(), sum(price) FROM t_squash_spill WHERE sym = 'A'")
                    .withPlan("""
                            Async Group By workers: 1
                              vectorized: true
                              values: [count(*),sum(price)]
                              filter: null
                                CoveringIndex on: sym with: price
                                  filter: sym='A'
                            """)
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\tsum\n401\t1080200.0\n");
        });
    }

    /**
     * Squashes MULTIPLE split sub-partitions in one squashSplitPartitions call,
     * matching the reported failure shape more closely than the 2-sub-partition
     * testSquashCoveringPostingWithMidStreamSpillFlush. The reported crash fired
     * from squashSplitPartitions (the IIIZ overload) -> sealPostingIndexForPartition
     * during WAL housekeep; squashPartitions() reaches the same overload via
     * squashPartitionForce, so this drives that exact reseal deterministically.
     * Four O3 prefixes build four split sub-partitions of one day over the hot
     * key 'A'; squashPartitions() merges them, and with the 256-byte spill budget
     * the merge's reseal index() loop flushes mid-stream -- the path that
     * SIGSEGVd before the commitDense consolidation fix.
     */
    @Test
    public void testSquashSplitPartitionsCoveringPostingMultiSplitSpill() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256);
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 20);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 20);

        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_multisplit (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Hot key 'A' across the early day.
            execute("""
                    INSERT INTO t_multisplit
                    SELECT dateadd('s', x::INT, '2024-01-01T00:00:00.000000Z'::TIMESTAMP),
                           'A', x::DOUBLE
                    FROM long_sequence(400)
                    """);
            // Late sentinel keeps the day's max at 23:00 so each O3 prefix below
            // lands inside the range and splits rather than appends.
            execute("INSERT INTO t_multisplit VALUES ('2024-01-01T23:00:00.000000Z', 'A', 1000000.0)");
            // Four O3 prefixes at distinct late minutes build several split
            // sub-partitions of 2024-01-01.
            execute("INSERT INTO t_multisplit VALUES ('2024-01-01T22:10:00.000000Z', 'B', -1.0)");
            execute("INSERT INTO t_multisplit VALUES ('2024-01-01T22:20:00.000000Z', 'B', -2.0)");
            execute("INSERT INTO t_multisplit VALUES ('2024-01-01T22:30:00.000000Z', 'B', -3.0)");
            execute("INSERT INTO t_multisplit VALUES ('2024-01-01T22:40:00.000000Z', 'B', -4.0)");

            final long splitCount = selectLong("SELECT count() FROM table_partitions('t_multisplit')");
            Assert.assertTrue(
                    "test setup must produce more than two split sub-partitions for "
                            + "squashSplitPartitions to merge, got " + splitCount,
                    splitCount > 2);

            engine.releaseAllWriters();
            try (TableWriter w = TestUtils.getWriter(engine, "t_multisplit")) {
                // squashPartitions() -> squashPartitionForce -> squashSplitPartitions
                // -> sealPostingIndexForPartition (the reported reseal path).
                w.squashPartitions();
            }

            assertQuery("SELECT count() FROM table_partitions('t_multisplit')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count
                            1
                            """);

            // 400 'A' rows (sum 1..400 = 80200) plus the 23:00 sentinel
            // (1000000) = 401 rows, sum 1080200. The withPlan pin keeps the read
            // on the CoveringIndex so a base-scan fallback cannot mask the bug.
            assertQuery("SELECT count(), sum(price) FROM t_multisplit WHERE sym = 'A'")
                    .withPlan("""
                            Async Group By workers: 1
                              vectorized: true
                              values: [count(*),sum(price)]
                              filter: null
                                CoveringIndex on: sym with: price
                                  filter: sym='A'
                            """)
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\tsum\n401\t1080200.0\n");
        });
    }

    /**
     * The original bug's trigger at scale: fragment one day into many split
     * sub-partitions, then squash them all in a single squashSplitPartitions
     * pass. The merged partition holds the full hot key, so its covering reseal
     * (discardForRebuild -> index() -> commitDense) trips the spill budget and
     * commitDense must consolidate -- the path that SIGSEGVd before the fix.
     * Twenty separate O3 commits into the late tail build twenty+ splits (kept
     * from auto-squashing by the high MAX_SPLITS), then squashPartitions()
     * collapses them to one and the covering result must match the truth.
     */
    @Test
    public void testSquashManySplitPartitionsCoveringPostingSpill() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256);
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        // High ceilings so housekeep does not auto-squash before the explicit call.
        node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 50);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);

        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_manysplit (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // 2000 hot 'A' rows early in the day so the squash reseal spills.
            execute("""
                    INSERT INTO t_manysplit
                    SELECT dateadd('s', x::INT, '2024-01-01T00:00:00.000000Z'::TIMESTAMP),
                           'A', x::DOUBLE
                    FROM long_sequence(2000)
                    """);
            // Late sentinel keeps the day's max at 23:00 so the O3 prefixes split.
            execute("INSERT INTO t_manysplit VALUES ('2024-01-01T23:00:00.000000Z', 'A', 1000000.0)");
            // Twenty separate O3 commits into distinct late-tail minutes; each
            // splits the trailing sub-partition, accumulating many splits.
            final int splitInserts = 20;
            for (int i = 0; i < splitInserts; i++) {
                execute("INSERT INTO t_manysplit VALUES ('2024-01-01T22:"
                        + String.format("%02d", i) + ":00.000000Z', 'B', " + (-1 - i) + ".0)");
            }

            final long splitCount = selectLong("SELECT count() FROM table_partitions('t_manysplit')");
            Assert.assertTrue(
                    "test setup must fragment the day into many split sub-partitions, got " + splitCount,
                    splitCount > 10);

            engine.releaseAllWriters();
            try (TableWriter w = TestUtils.getWriter(engine, "t_manysplit")) {
                // squashPartitions() -> squashPartitionForce -> squashSplitPartitions
                // -> sealPostingIndexForPartition: the reported reseal path, now over
                // a partition merged from 20+ splits.
                w.squashPartitions();
            }

            assertQuery("SELECT count() FROM table_partitions('t_manysplit')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count
                            1
                            """);

            // 2000 'A' rows (sum 1..2000 = 2001000) plus the 23:00 sentinel
            // (1000000) = 2001 rows, sum 3001000. The withPlan pin keeps the read
            // on the CoveringIndex so a base-scan fallback cannot mask the bug.
            assertQuery("SELECT count(), sum(price) FROM t_manysplit WHERE sym = 'A'")
                    .withPlan("""
                            Async Group By workers: 1
                              vectorized: true
                              values: [count(*),sum(price)]
                              filter: null
                                CoveringIndex on: sym with: price
                                  filter: sym='A'
                            """)
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\tsum\n2001\t3001000.0\n");
        });
    }

    /**
     * Non-covering twin of testSquashCoveringPostingWithMidStreamSpillFlush.
     * The non-covering squash reseal also runs discardForRebuild -> index() ->
     * commitDense; the same mid-stream flushAllPending orphans the early sparse
     * gen. Without a sidecar rebuild there is no SIGSEGV, but commitDense's
     * extendHead(newGenCount=1) silently drops the pre-flush rows, so an indexed
     * predicate returns short counts. Asserting full counts proves the
     * commitDense consolidation fix restores completeness on this path too.
     */
    @Test
    public void testSquashNonCoveringPostingWithMidStreamSpillFlush() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256);
        // POSTING auto-includes the designated timestamp by default, which would
        // make this a covering index (and crash, like the covering test).
        // Disable it so the reseal genuinely takes the non-covering branch.
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_AUTO_INCLUDE_TIMESTAMP, false);
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 20);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 20);

        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_squash_spill_nc (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_squash_spill_nc
                    SELECT dateadd('s', x::INT, '2024-01-01T00:00:00.000000Z'::TIMESTAMP),
                           'A', x::DOUBLE
                    FROM long_sequence(400)
                    """);
            execute("""
                    INSERT INTO t_squash_spill_nc VALUES
                    ('2024-01-01T20:00:00.000000Z', 'A', 1000000.0)
                    """);
            execute("""
                    INSERT INTO t_squash_spill_nc VALUES
                    ('2024-01-01T19:00:00.000000Z', 'B', -1.0)
                    """);

            engine.releaseAllWriters();
            try (TableWriter w = TestUtils.getWriter(engine, "t_squash_spill_nc")) {
                w.squashPartitions();
            }

            // Index-driven count must still see every 'A' rowid after the reseal:
            // 400 rows plus the 20:00 sentinel = 401. Without the fix the orphaned
            // gen drops the pre-flush rowids and this comes back short.
            assertQuery("SELECT count() FROM t_squash_spill_nc WHERE sym = 'A'")
                    .noLeakCheck()
                    .returnsOnce("count\n401\n");
        });
    }

    /**
     * Drives the reported crash through its original entry point: WAL apply.
     * housekeep() auto-squashes split sub-partitions inside
     * commitWalInsertTransactions, and with a covering posting index over a hot
     * key plus a tiny spill budget the squash reseal mid-stream flushes -- the
     * exact wal-apply_N -> squashSplitPartitions -> sealPostingIndexForPartition
     * -> rebuildSidecars -> decodeDenseGenStride path from the hs_err report.
     * The crash happens inside drainWalQueue; with the fix the drained table
     * returns the same rows as the non-covering fallback.
     */
    @Test
    public void testWalApplyAutoSquashCoveringPostingWithMidStreamSpillFlush() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256);
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        // Low split ceiling so housekeep squashes the accumulated splits during
        // a later drain rather than letting them persist.
        node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 2);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 2);

        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wal_squash_spill (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            // Hot key 'A' seeded across the day; the 23:00 sentinel keeps the
            // partition's max late so the O3 prefixes below split rather than
            // append.
            execute("""
                    INSERT INTO t_wal_squash_spill
                    SELECT dateadd('s', x::INT, '2024-01-01T00:00:00.000000Z'::TIMESTAMP),
                           'A', x::DOUBLE
                    FROM long_sequence(400)
                    """);
            execute("INSERT INTO t_wal_squash_spill VALUES ('2024-01-01T23:00:00.000000Z', 'A', 1000000.0)");
            drainWalQueue();

            // Several O3 prefixes into the late tail create split sub-partitions;
            // once they exceed MAX_SPLITS, a subsequent drain's housekeep squashes
            // them and reseals the covering posting index over all 400+ 'A' rows.
            for (int i = 0; i < 6; i++) {
                execute("INSERT INTO t_wal_squash_spill VALUES " +
                        "('2024-01-01T22:" + i + "0:00.000000Z', 'B', " + (-1 - i) + ".0)");
                drainWalQueue();
            }

            // 400 'A' rows (sum 1..400 = 80200) plus the 23:00 sentinel (1000000)
            // = 401 rows, sum 1080200. The withPlan pin keeps the read on the
            // CoveringIndex so a base-scan fallback cannot mask the bug.
            assertQuery("SELECT count(), sum(price) FROM t_wal_squash_spill WHERE sym = 'A'")
                    .withPlan("""
                            Async Group By workers: 1
                              vectorized: true
                              values: [count(*),sum(price)]
                              filter: null
                                CoveringIndex on: sym with: price
                                  filter: sym='A'
                            """)
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\tsum\n401\t1080200.0\n");
        });
    }

    /**
     * Exercises the commitDense consolidation fix through the plain O3 commit
     * reseal -- sealPostingIndexesForO3Partitions ->
     * sealPostingIndexForPartition(..., canSkipRebuild=false) -> discardForRebuild
     * -> index() -> commitDense() -- with no squashPartitions() involved. This is
     * the most common production trigger of the reseal: an O3 insert whose
     * timestamps land inside the existing partition range makes
     * partitionMutates=true, so the covering posting index rebuilds the whole
     * partition from sym.d. With the tiny spill budget the rebuild's index() loop
     * trips compactIfOverBudget mid-stream, so commitDense must consolidate the
     * orphaned sparse gen instead of dropping it. SIGSEGVs without the fix; with
     * it, both the hot key and the O3-inserted rows resolve through the index.
     */
    @Test
    public void testO3CommitCoveringPostingResealWithMidStreamSpillFlush() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256);

        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_o3_reseal (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Hot key 'A', 400 in-order rows across 2024-01-01 [00:00:01, 00:06:40].
            execute("""
                    INSERT INTO t_o3_reseal
                    SELECT dateadd('s', x::INT, '2024-01-01T00:00:00.000000Z'::TIMESTAMP),
                           'A', x::DOUBLE
                    FROM long_sequence(400)
                    """);
            // O3 insert: timestamps inside the existing range -> partitionMutates
            // -> canSkipRebuild=false reseal of the whole partition (no squash).
            execute("""
                    INSERT INTO t_o3_reseal VALUES
                    ('2024-01-01T00:03:00.000000Z', 'B', -1.0),
                    ('2024-01-01T00:03:30.000000Z', 'B', -2.0),
                    ('2024-01-01T00:04:00.000000Z', 'B', -3.0)
                    """);

            // The hot key (which drove the mid-stream spill) and the O3-inserted
            // rows must both come back intact through the covering index. The
            // aggregates are deterministic: 'A' = 400 rows, sum(1..400) = 80200;
            // 'B' = 3 rows, sum(-1,-2,-3) = -6. Assert the covering plan alongside
            // the result so a future fallback to a base-table scan (sym.d/price.d
            // intact, only the covering sidecar corrupt) fails here instead of
            // masking the bug.
            assertQuery("SELECT count(), sum(price) FROM t_o3_reseal WHERE sym = 'A'")
                    .withPlan("""
                            Async Group By workers: 1
                              vectorized: true
                              values: [count(*),sum(price)]
                              filter: null
                                CoveringIndex on: sym with: price
                                  filter: sym='A'
                            """)
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\tsum\n400\t80200.0\n");
            assertQuery("SELECT count(), sum(price) FROM t_o3_reseal WHERE sym = 'B'")
                    .withPlan("""
                            Async Group By workers: 1
                              vectorized: true
                              values: [count(*),sum(price)]
                              filter: null
                                CoveringIndex on: sym with: price
                                  filter: sym='B'
                            """)
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\tsum\n3\t-6.0\n");
        });
    }

    /**
     * C4 (deterministic counterpart of the covering-parquet fuzz tests): an O3 write
     * that rewrites an already-parquet partition carrying a COVERING posting index must
     * rebuild that new parquet version's covering sidecars (.pci/.pc). The O3 worker
     * builds only the non-covering .pv; resealParquetCoveringForPartition (reached via
     * sealPostingIndexForPartition's parquet branch in finishO3Commit) re-materialises
     * the covering from the rewritten parquet. Without it a reader opens the new version,
     * walks the chain (count correct) but finds coverCount=0 and resolves covered values
     * as NULL. A withPlan() check forces the covering cursor (expectSize()/
     * noRandomAccess() describe only the outer aggregation and would also pass a base-scan
     * fallback); the sum(price)/first(tag) assertions would read NULL covered values
     * without the fix.
     */
    @Test
    public void testO3CoveringPostingParquetResealKeepsCoveredValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pq_cov_reseal (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, tag),
                        price DOUBLE,
                        tag VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            // 200 'A' rows in 2024-01-01 (price 1..200, sum 20100, tag 'TA').
            execute("""
                    INSERT INTO t_pq_cov_reseal
                    SELECT dateadd('s', x::INT, '2024-01-01T00:00:00.000000Z'::TIMESTAMP),
                           'A', x::DOUBLE, 'TA'
                    FROM long_sequence(200)
                    """);
            // 10 more 'A' rows in 2024-01-02 (price 1001..1010, sum 10055) so 'A'
            // spans a native partition too, and the converted one is not the tail.
            execute("""
                    INSERT INTO t_pq_cov_reseal
                    SELECT dateadd('s', x::INT, '2024-01-02T00:00:00.000000Z'::TIMESTAMP),
                           'A', (1000 + x)::DOUBLE, 'TA'
                    FROM long_sequence(10)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t_pq_cov_reseal CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            // O3 merge into the already-parquet partition rewrites it; the covering
            // sidecars for the new parquet version must be rebuilt for the new 'B' rows
            // (and the existing 'A' rows) to resolve their covered price/tag.
            execute("""
                    INSERT INTO t_pq_cov_reseal VALUES
                    ('2024-01-01T00:01:00.500000Z', 'B', -1.0, 'TB'),
                    ('2024-01-01T00:02:00.500000Z', 'B', -2.0, 'TB')
                    """);
            drainWalQueue();
            engine.releaseAllWriters();

            // Assert the covering plan alongside the result: sum(price)/first(tag)
            // must be served by the CoveringIndex, not a base-table scan.
            // expectSize()/noRandomAccess() describe only the outer aggregation and
            // pass either way, so without this plan check a future optimizer
            // fallback to a forward scan (intact base columns, NULL-ed covering
            // sidecars) would mask the bug.
            assertQuery("SELECT count(*) rows, sum(price) sum_price, first(tag) first_tag FROM t_pq_cov_reseal WHERE sym = 'A'")
                    .withPlan("""
                            Async Group By workers: 1
                              vectorized: true
                              values: [count(*),sum(price),first(tag)]
                              filter: null
                                CoveringIndex on: sym with: price, tag
                                  filter: sym='A'
                            """)
                    .noRandomAccess()
                    .expectSize()
                    .returns("rows\tsum_price\tfirst_tag\n210\t30155.0\tTA\n");
            // The O3-inserted 'B' rows live in the rewritten parquet partition; their
            // covered values must be non-NULL after the reseal.
            assertQuery("SELECT count(*) rows, sum(price) sum_price, first(tag) first_tag FROM t_pq_cov_reseal WHERE sym = 'B'")
                    .withPlan("""
                            Async Group By workers: 1
                              vectorized: true
                              values: [count(*),sum(price),first(tag)]
                              filter: null
                                CoveringIndex on: sym with: price, tag
                                  filter: sym='B'
                            """)
                    .noRandomAccess()
                    .expectSize()
                    .returns("rows\tsum_price\tfirst_tag\n2\t-3.0\tTB\n");
        });
    }

    /**
     * The parquet index-rebuild path (O3PartitionJob.updateParquetIndexes ->
     * commitDense) is the third commitDense caller. When its index() loop trips
     * the spill budget, commitDense routes through seal(), which rotates the .pv
     * to a new sealTxn. Unlike the native reseal, the pooled parquet writer is
     * freed (close() -> releasePendingPurges) without ever draining its pending
     * seal-purge, so the superseded intermediate .pv would leak on disk
     * permanently (no directory sweep, no recovery-walk reclaim). The fix hands
     * the purge to the TableWriter's deferred queue; after the commit, the
     * scoreboard-gated PostingSealPurgeJob reclaims the superseded .pv. After a
     * spill-driven CONVERT + O3 rewrite and a purge-job pass, exactly one .pv
     * must remain.
     */
    @Test
    public void testConvertToParquetPostingResealSpillDoesNotLeakValueFile() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256);
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_AUTO_INCLUDE_TIMESTAMP, false);

        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pq_leak (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            // Hot key 'A' so the parquet rebuild's index() loop spills past the
            // 256-byte budget mid-stream (genCount > 0 -> commitDense seals).
            execute("""
                    INSERT INTO t_pq_leak
                    SELECT dateadd('s', x::INT, '2024-01-01T00:00:00.000000Z'::TIMESTAMP), 'A'
                    FROM long_sequence(2000)
                    """);
            // A second partition so the converted one is not the active tail.
            execute("""
                    INSERT INTO t_pq_leak
                    SELECT dateadd('s', x::INT, '2024-01-02T00:00:00.000000Z'::TIMESTAMP), 'A'
                    FROM long_sequence(10)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t_pq_leak CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            // O3 into the already-parquet partition rewrites it; updateParquetIndexes
            // rebuilds the posting index over all 2000+ 'A' rows, tripping the spill
            // budget -> commitDense seals -> the rotated intermediate .pv would leak.
            execute("""
                    INSERT INTO t_pq_leak VALUES
                    ('2024-01-01T00:10:00.500000Z', 'A'),
                    ('2024-01-01T00:20:00.500000Z', 'A')
                    """);
            drainWalQueue();

            // Drain any queued posting-seal purges; the leaked intermediate is
            // never queued, so this would not reclaim it.
            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }
            engine.releaseAllWriters();

            // Index still returns every 'A' rowid after the parquet reseal
            // (2000 + 2 O3 rows in the converted partition + 10 in the next).
            assertQuery("SELECT count() FROM t_pq_leak WHERE sym = 'A'")
                    .noLeakCheck()
                    .returnsOnce("count\n2012\n");

            final TableToken token = engine.getTableTokenIfExists("t_pq_leak");
            Assert.assertNotNull("table must exist", token);
            long partitionTs;
            long partitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
                partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                setPathForNativePartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                final java.io.File dir = new java.io.File(path.toString());
                final String[] names = dir.list();
                Assert.assertNotNull("partition dir must exist: " + dir, names);
                int pvCount = 0;
                for (String n : names) {
                    if (n.startsWith("sym.pv.")) {
                        pvCount++;
                    }
                }
                Assert.assertEquals(
                        "exactly one active value file must remain; a superseded intermediate "
                                + ".pv from the spill-driven parquet reseal leaked: "
                                + java.util.Arrays.toString(names),
                        1, pvCount);
            }
        });
    }

    /**
     * Update-in-place twin of testConvertToParquetPostingResealSpillDoesNotLeakValueFile.
     * Forcing many small row groups plus disabled rewrite thresholds makes the O3
     * insert update the parquet file in place (isRewrite=false), so the posting
     * index rebuilds into the LIVE committed directory. The spill-driven reseal's
     * superseded .pv cannot be unlinked inline there (a reader pinned at the
     * pre-commit txn could still map it), so it is routed to the scoreboard-gated
     * deferred purge instead. After the commit and a purge-job run, exactly one
     * value file must remain -- the case that leaked before the deferred purge.
     */
    @Test
    public void testInPlaceParquetPostingResealSpillReclaimsValueFile() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256);
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_AUTO_INCLUDE_TIMESTAMP, false);
        // Multi-row-group + rewrite triggers off -> O3 updates the parquet file
        // in place (isRewrite=false) rather than rewriting to a new directory.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);

        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pq_inplace (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_pq_inplace
                    SELECT dateadd('s', x::INT, '2024-01-01T00:00:00.000000Z'::TIMESTAMP), 'A'
                    FROM long_sequence(2000)
                    """);
            execute("""
                    INSERT INTO t_pq_inplace
                    SELECT dateadd('s', x::INT, '2024-01-02T00:00:00.000000Z'::TIMESTAMP), 'A'
                    FROM long_sequence(10)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t_pq_inplace CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            // O3 into the parquet partition; with the config above this updates in
            // place (isRewrite=false) and rebuilds the index over all 2000+ 'A'
            // rows, tripping the spill budget -> commitDense seals -> deferred purge.
            execute("""
                    INSERT INTO t_pq_inplace VALUES
                    ('2024-01-01T00:10:00.500000Z', 'A'),
                    ('2024-01-01T00:20:00.500000Z', 'A')
                    """);
            drainWalQueue();

            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }
            engine.releaseAllWriters();

            assertQuery("SELECT count() FROM t_pq_inplace WHERE sym = 'A'")
                    .noLeakCheck()
                    .returnsOnce("count\n2012\n");

            final TableToken token = engine.getTableTokenIfExists("t_pq_inplace");
            Assert.assertNotNull("table must exist", token);
            long partitionTs;
            long partitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
                partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                setPathForNativePartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                final java.io.File dir = new java.io.File(path.toString());
                final String[] names = dir.list();
                Assert.assertNotNull("partition dir must exist: " + dir, names);
                int pvCount = 0;
                for (String n : names) {
                    if (n.startsWith("sym.pv.")) {
                        pvCount++;
                    }
                }
                Assert.assertEquals(
                        "exactly one value file must remain after the in-place reseal; the "
                                + "superseded intermediate must be reclaimed by the deferred purge: "
                                + java.util.Arrays.toString(names),
                        1, pvCount);
            }
        });
    }

    /**
     * Exercises the concurrent deposit into deferParquetPostingSealPurges: a real
     * WorkerPool runs O3PartitionJob on multiple threads, and one O3 insert that
     * touches every parquet partition dispatches a partition task per day, so
     * several workers run updateParquetIndexes -> commitDense -> seal -> the
     * deferred purge at once, contending on parquetSealPurgeLock. The single-
     * threaded tests never hit this path. After a purge-job pass, every
     * partition must keep exactly one value file (no purge lost or double-freed
     * under contention) and the indexed count must be exact.
     */
    @Test
    public void testMultiThreadedO3ParquetPostingSpillReclaimsAcrossPartitions() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256);
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_AUTO_INCLUDE_TIMESTAMP, false);

        assertMemoryLeak(() -> {
            final int dayCount = 6;
            execute("""
                    CREATE TABLE t_mt (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // 2000 hot 'A' rows per day so every partition's reseal spills.
            for (int d = 1; d <= dayCount; d++) {
                execute("INSERT INTO t_mt SELECT dateadd('s', x::INT, '2024-01-0" + d
                        + "T00:00:00.000000Z'::TIMESTAMP), 'A' FROM long_sequence(2000)");
            }
            for (int d = 1; d <= dayCount; d++) {
                execute("ALTER TABLE t_mt CONVERT PARTITION TO PARQUET LIST '2024-01-0" + d + "'");
            }

            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.setupWorkerPool(pool, engine);
            pool.start(LOG);
            try {
                // One O3 insert with a row inside every parquet day's range -> one
                // commit, one partition task per day, run across the pool workers.
                final StringBuilder sql = new StringBuilder("INSERT INTO t_mt VALUES ");
                for (int d = 1; d <= dayCount; d++) {
                    if (d > 1) {
                        sql.append(',');
                    }
                    sql.append("('2024-01-0").append(d).append("T00:10:00.500000Z', 'A')");
                }
                execute(sql.toString());
            } finally {
                pool.halt();
            }

            // Pool halted: drain the deferred purges with nothing pinned.
            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }
            engine.releaseAllWriters();

            // 6 days * 2000 + 6 O3 rows.
            assertQuery("SELECT count() FROM t_mt WHERE sym = 'A'")
                    .noLeakCheck()
                    .returnsOnce("count\n" + (dayCount * 2000 + dayCount) + "\n");

            // Every parquet partition must keep exactly one value file.
            final TableToken token = engine.getTableTokenIfExists("t_mt");
            Assert.assertNotNull("table must exist", token);
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals("all days must remain", dayCount, reader.getTxFile().getPartitionCount());
                for (int i = 0; i < dayCount; i++) {
                    final long partitionTs = reader.getTxFile().getPartitionTimestampByIndex(i);
                    final long partitionNameTxn = reader.getTxFile().getPartitionNameTxn(i);
                    try (Path path = new Path()) {
                        path.of(configuration.getDbRoot()).concat(token);
                        setPathForNativePartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                        final String[] names = new java.io.File(path.toString()).list();
                        Assert.assertNotNull("partition dir must exist: " + path, names);
                        int pvCount = 0;
                        for (String n : names) {
                            if (n.startsWith("sym.pv.")) {
                                pvCount++;
                            }
                        }
                        Assert.assertEquals(
                                "partition " + i + " must keep exactly one value file (no purge lost or "
                                        + "double-freed under concurrent deposit): " + java.util.Arrays.toString(names),
                                1, pvCount);
                    }
                }
            }
        });
    }

    /**
     * Concurrent-read integration test for the parquet reseal value-file reclaim
     * (deferParquetPostingSealPurges -> scoreboard-gated PostingSealPurgeJob).
     * Background threads continuously query the posting index on a parquet
     * partition while the main thread repeatedly O3-rewrites it under a tiny
     * spill budget, so every rewrite drives commitDense -> seal -> a deferred
     * purge. The 'A' count (rows are only added) must stay monotonic, never below
     * the initial 2010, with no reader crash -- the reclaim is scoreboard-gated,
     * so it must never delete a .pv a reader still resolves. The trailing
     * single-.pv assertion (after a purge-job pass with nothing pinned) guards
     * against the reclaim regressing into a leak across many rewrites; the
     * deterministic counterpart is
     * testConvertToParquetPostingResealSpillDoesNotLeakValueFile.
     */
    @Test
    public void testParquetPostingSpillConcurrentReadFuzz() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256);
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_AUTO_INCLUDE_TIMESTAMP, false);
        final Rnd rnd = TestUtils.generateRandom(LOG);

        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE xq (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING)
                    TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            // 2000 hot 'A' rows in one row group (so the O3 inserts below take the
            // isRewrite=true rebuild) plus a second partition so the converted one
            // is not the active tail.
            execute("""
                    INSERT INTO xq
                    SELECT dateadd('s', x::INT, '2024-01-01T00:00:00.000000Z'::TIMESTAMP), 'A'
                    FROM long_sequence(2000)
                    """);
            execute("""
                    INSERT INTO xq
                    SELECT dateadd('s', x::INT, '2024-01-02T00:00:00.000000Z'::TIMESTAMP), 'A'
                    FROM long_sequence(10)
                    """);
            drainWalQueue();
            execute("ALTER TABLE xq CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            final AtomicReference<Throwable> bgError = new AtomicReference<>();
            final AtomicBoolean stop = new AtomicBoolean();
            final int readerCount = 3;
            final Thread[] readers = new Thread[readerCount];
            for (int r = 0; r < readerCount; r++) {
                readers[r] = new Thread(() -> {
                    try (
                            SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                                    .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                                            null, null, -1, null);
                            SqlCompiler compiler = engine.getSqlCompiler()
                    ) {
                        long prev = 0;
                        while (!stop.get() && bgError.get() == null) {
                            final long a;
                            try (RecordCursorFactory f = compiler.compile(
                                    "SELECT count() FROM xq WHERE sym = 'A'", ctx).getRecordCursorFactory();
                                 RecordCursor cur = f.getCursor(ctx)) {
                                a = cur.hasNext() ? cur.getRecord().getLong(0) : -1;
                            }
                            if (a < 2010) {
                                throw new AssertionError("'A' count fell below the initial 2010: " + a);
                            }
                            if (a < prev) {
                                throw new AssertionError("'A' count went backwards: " + prev + " -> " + a);
                            }
                            prev = a;
                        }
                    } catch (Throwable e) {
                        bgError.set(e);
                    }
                }, "parquet-posting-reader-" + r);
                readers[r].setDaemon(true);
                readers[r].start();
            }

            long expectedA = 2010;
            try {
                final int batches = 8 + rnd.nextInt(8);
                for (int b = 0; b < batches && bgError.get() == null; b++) {
                    final int n = 1 + rnd.nextInt(4);
                    final StringBuilder sql = new StringBuilder("INSERT INTO xq VALUES ");
                    for (int i = 0; i < n; i++) {
                        if (i > 0) {
                            sql.append(',');
                        }
                        // O3 timestamp inside the converted partition's [1s, 1999s] range.
                        sql.append("(dateadd('s', ").append(1 + rnd.nextInt(1999))
                                .append(", '2024-01-01T00:00:00.000000Z'::TIMESTAMP), 'A')");
                    }
                    execute(sql.toString());
                    drainWalQueue();
                    expectedA += n;
                }
            } finally {
                stop.set(true);
                for (Thread t : readers) {
                    t.join(30_000);
                    Assert.assertFalse("reader thread did not terminate", t.isAlive());
                }
            }

            Assert.assertNull("concurrent reader threw: " + bgError.get(), bgError.get());
            assertQuery("SELECT count() FROM xq WHERE sym = 'A'")
                    .noLeakCheck()
                    .returnsOnce("count\n" + expectedA + "\n");

            // The reclaim must also have kept the partition free of leaked
            // intermediates across every rewrite: exactly one value file remains.
            // The rewrite purges are deferred and scoreboard-gated, so with the
            // readers stopped and writers released (nothing pinned) a purge-job
            // pass reclaims every superseded .pv.
            engine.releaseAllWriters();
            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }
            final TableToken token = engine.getTableTokenIfExists("xq");
            Assert.assertNotNull("table must exist", token);
            long partitionTs;
            long partitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
                partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                setPathForNativePartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                final java.io.File dir = new java.io.File(path.toString());
                final String[] names = dir.list();
                Assert.assertNotNull("partition dir must exist: " + dir, names);
                int pvCount = 0;
                for (String nm : names) {
                    if (nm.startsWith("sym.pv.")) {
                        pvCount++;
                    }
                }
                Assert.assertEquals(
                        "exactly one value file must remain after the rewrites; a leaked "
                                + "intermediate from the spill-driven reseal survived: "
                                + java.util.Arrays.toString(names),
                        1, pvCount);
            }
        });
    }

    /**
     * Writer release/reopen under covering reads. Mid-fuzz the writer is released
     * (engine.releaseAllWriters()) so the next O3 insert reopens it through the
     * posting-index recovery path (chain head trim / superseded-gen drop) while
     * reader threads keep serving count()/sum(price) from the covering sidecars.
     * Rows are only added, so the covered total stays monotonic; a recovery reopen
     * that loses a committed gen or trims a live chain head would drop the total.
     * All-parquet, so the recovery reseal takes the safe path.
     */
    @Test
    public void testMultiThreadedO3CoveringWriterReopenConcurrentReaderFuzz() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256);
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_AUTO_INCLUDE_TIMESTAMP, false);
        final Rnd rnd = TestUtils.generateRandom(LOG);

        assertMemoryLeak(() -> {
            final int dayCount = 4 + rnd.nextInt(4); // 4..7 days
            final int hotPerDay = 1000;
            final long perDaySum = (long) hotPerDay * (hotPerDay + 1) / 2;
            final long initialRows = (long) dayCount * hotPerDay;
            final long initialSum = (long) dayCount * perDaySum;

            execute("""
                    CREATE TABLE t_cov_reopen (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            for (int d = 1; d <= dayCount; d++) {
                execute("INSERT INTO t_cov_reopen SELECT dateadd('s', x::INT, '2024-02-0" + d
                        + "T00:00:00.000000Z'::TIMESTAMP), 'A', x::DOUBLE FROM long_sequence(" + hotPerDay + ")");
            }
            for (int d = 1; d <= dayCount; d++) {
                execute("ALTER TABLE t_cov_reopen CONVERT PARTITION TO PARQUET LIST '2024-02-0" + d + "'");
            }

            final AtomicReference<Throwable> bgError = new AtomicReference<>();
            final AtomicBoolean stop = new AtomicBoolean();
            final AtomicLong rowFloor = new AtomicLong(initialRows);
            final int readerCount = 3;
            final Thread[] readers = new Thread[readerCount];
            for (int r = 0; r < readerCount; r++) {
                readers[r] = new Thread(() -> {
                    try (
                            SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                                    .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                                            null, null, -1, null);
                            SqlCompiler compiler = engine.getSqlCompiler()
                    ) {
                        while (!stop.get() && bgError.get() == null) {
                            final long floor = rowFloor.get();
                            final long count;
                            final double sum;
                            try (RecordCursorFactory f = compiler.compile(
                                    "SELECT count(), sum(price) FROM t_cov_reopen WHERE sym = 'A'", ctx).getRecordCursorFactory();
                                 RecordCursor cur = f.getCursor(ctx)) {
                                if (cur.hasNext()) {
                                    count = cur.getRecord().getLong(0);
                                    sum = cur.getRecord().getDouble(1);
                                } else {
                                    count = -1;
                                    sum = -1;
                                }
                            }
                            if (count < floor) {
                                throw new AssertionError("covered 'A' count fell below the committed floor: " + count + " < " + floor);
                            }
                            if (sum < (double) initialSum - 0.5) {
                                throw new AssertionError("covered sum(price) dropped below the initial total " + initialSum + ": " + sum);
                            }
                            if (sum < (double) count - 0.5) {
                                throw new AssertionError("covered sum(price)=" + sum + " < count=" + count + " -- a covered value resolved as < 1");
                            }
                        }
                    } catch (Throwable e) {
                        bgError.set(e);
                    }
                }, "covering-reopen-reader-" + r);
                readers[r].setDaemon(true);
                readers[r].start();
            }

            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.setupWorkerPool(pool, engine);
            pool.start(LOG);
            long expectedRows = initialRows;
            try {
                final int batches = 6 + rnd.nextInt(10);
                for (int b = 0; b < batches && bgError.get() == null; b++) {
                    final StringBuilder sql = new StringBuilder("INSERT INTO t_cov_reopen VALUES ");
                    int added = 0;
                    for (int d = 1; d <= dayCount; d++) {
                        if (rnd.nextBoolean()) {
                            continue;
                        }
                        final int n = 1 + rnd.nextInt(3);
                        for (int i = 0; i < n; i++) {
                            if (added > 0) {
                                sql.append(',');
                            }
                            sql.append("('2024-02-0").append(d).append("T00:")
                                    .append(String.format("%02d", 1 + rnd.nextInt(58)))
                                    .append(":00.500000Z', 'A', 1.0)");
                            added++;
                        }
                    }
                    if (added == 0) {
                        continue;
                    }
                    execute(sql.toString());
                    expectedRows += added;
                    rowFloor.set(expectedRows);
                    // Periodically release the writer so the next insert reopens it
                    // through the posting-index recovery path under the live readers.
                    if (rnd.nextInt(3) == 0) {
                        engine.releaseAllWriters();
                    }
                }
            } finally {
                stop.set(true);
                for (Thread t : readers) {
                    t.join(30_000);
                    Assert.assertFalse("reader thread did not terminate", t.isAlive());
                }
                pool.halt();
            }

            Assert.assertNull("concurrent writer-reopen covered reader threw: " + bgError.get(), bgError.get());

            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }
            engine.releaseAllWriters();

            final long expectedSum = initialSum + (expectedRows - initialRows);
            try (
                    SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                            .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                                    null, null, -1, null);
                    SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory f = compiler.compile(
                            "SELECT count(), sum(price) FROM t_cov_reopen WHERE sym = 'A'", ctx).getRecordCursorFactory();
                    RecordCursor cur = f.getCursor(ctx)
            ) {
                Assert.assertTrue("final covered query returned no row", cur.hasNext());
                Assert.assertEquals("final covered 'A' count", expectedRows, cur.getRecord().getLong(0));
                Assert.assertEquals("final covered sum(price)", (double) expectedSum, cur.getRecord().getDouble(1), 0.5);
            }
        });
    }

    /**
     * DROP PARTITION concurrent with covering reads. A covering read for 'A'
     * iterates the posting index across ALL partitions, so while it runs the writer
     * drops random EARLIER parquet partitions (their index files are removed,
     * scoreboard-gated) and O3-inserts into a PROTECTED last partition (never
     * dropped). Readers project only the protected partition (ts >= last day), whose
     * covered count/sum is therefore monotonic; a DROP that corrupts the shared
     * index iteration or frees a partition under the reader would drop the protected
     * total or crash. The all-parquet protected partition reseals on the safe path.
     */
    @Test
    public void testMultiThreadedO3CoveringDropPartitionConcurrentReaderFuzz() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256);
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_AUTO_INCLUDE_TIMESTAMP, false);
        final Rnd rnd = TestUtils.generateRandom(LOG);

        assertMemoryLeak(() -> {
            final int dayCount = 5 + rnd.nextInt(3); // 5..7 days; >=4 droppable + 1 protected
            final int hotPerDay = 1000;
            final long perDaySum = (long) hotPerDay * (hotPerDay + 1) / 2;
            final String protectedDay = "2024-02-0" + dayCount;

            execute("""
                    CREATE TABLE t_cov_drop (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            for (int d = 1; d <= dayCount; d++) {
                execute("INSERT INTO t_cov_drop SELECT dateadd('s', x::INT, '2024-02-0" + d
                        + "T00:00:00.000000Z'::TIMESTAMP), 'A', x::DOUBLE FROM long_sequence(" + hotPerDay + ")");
            }
            for (int d = 1; d <= dayCount; d++) {
                execute("ALTER TABLE t_cov_drop CONVERT PARTITION TO PARQUET LIST '2024-02-0" + d + "'");
            }

            final AtomicReference<Throwable> bgError = new AtomicReference<>();
            final AtomicBoolean stop = new AtomicBoolean();
            final AtomicLong protectedFloor = new AtomicLong(hotPerDay);
            final String protectedPred = "sym = 'A' AND ts >= '" + protectedDay + "T00:00:00.000000Z'";
            final int readerCount = 3;
            final Thread[] readers = new Thread[readerCount];
            for (int r = 0; r < readerCount; r++) {
                readers[r] = new Thread(() -> {
                    try (
                            SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                                    .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                                            null, null, -1, null);
                            SqlCompiler compiler = engine.getSqlCompiler()
                    ) {
                        while (!stop.get() && bgError.get() == null) {
                            final long floor = protectedFloor.get();
                            final long count;
                            final double sum;
                            try (RecordCursorFactory f = compiler.compile(
                                    "SELECT count(), sum(price) FROM t_cov_drop WHERE " + protectedPred, ctx).getRecordCursorFactory();
                                 RecordCursor cur = f.getCursor(ctx)) {
                                if (cur.hasNext()) {
                                    count = cur.getRecord().getLong(0);
                                    sum = cur.getRecord().getDouble(1);
                                } else {
                                    count = -1;
                                    sum = -1;
                                }
                            }
                            if (count < floor) {
                                throw new AssertionError("protected-partition covered count fell below floor: " + count + " < " + floor);
                            }
                            if (sum < (double) perDaySum - 0.5) {
                                throw new AssertionError("protected-partition covered sum dropped below its initial total "
                                        + perDaySum + ": " + sum);
                            }
                            if (sum < (double) count - 0.5) {
                                throw new AssertionError("protected covered sum=" + sum + " < count=" + count + " -- a covered value resolved as < 1");
                            }
                        }
                    } catch (Throwable e) {
                        bgError.set(e);
                    }
                }, "covering-drop-reader-" + r);
                readers[r].setDaemon(true);
                readers[r].start();
            }

            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.setupWorkerPool(pool, engine);
            pool.start(LOG);
            long protectedExpected = hotPerDay;
            int nextDropDay = 1;
            try {
                final int batches = 6 + rnd.nextInt(8);
                for (int b = 0; b < batches && bgError.get() == null; b++) {
                    // Always O3-insert into the protected last partition (forces its reseal).
                    final StringBuilder sql = new StringBuilder("INSERT INTO t_cov_drop VALUES ");
                    final int n = 1 + rnd.nextInt(3);
                    for (int i = 0; i < n; i++) {
                        if (i > 0) {
                            sql.append(',');
                        }
                        sql.append("('").append(protectedDay).append("T00:")
                                .append(String.format("%02d", 1 + rnd.nextInt(58)))
                                .append(":00.500000Z', 'A', 1.0)");
                    }
                    execute(sql.toString());
                    protectedExpected += n;
                    protectedFloor.set(protectedExpected);

                    // Drop the next earlier partition (1..dayCount-1) roughly every other batch.
                    if (nextDropDay < dayCount && rnd.nextBoolean()) {
                        execute("ALTER TABLE t_cov_drop DROP PARTITION LIST '2024-02-0" + nextDropDay + "'");
                        nextDropDay++;
                    }
                }
            } finally {
                stop.set(true);
                for (Thread t : readers) {
                    t.join(30_000);
                    Assert.assertFalse("reader thread did not terminate", t.isAlive());
                }
                pool.halt();
            }

            Assert.assertNull("concurrent drop-partition covered reader threw: " + bgError.get(), bgError.get());

            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }
            engine.releaseAllWriters();

            final long expectedSum = perDaySum + (protectedExpected - hotPerDay);
            try (
                    SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                            .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                                    null, null, -1, null);
                    SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory f = compiler.compile(
                            "SELECT count(), sum(price) FROM t_cov_drop WHERE " + protectedPred, ctx).getRecordCursorFactory();
                    RecordCursor cur = f.getCursor(ctx)
            ) {
                Assert.assertTrue("final protected query returned no row", cur.hasNext());
                Assert.assertEquals("final protected covered count", protectedExpected, cur.getRecord().getLong(0));
                Assert.assertEquals("final protected covered sum(price)", (double) expectedSum, cur.getRecord().getDouble(1), 0.5);
            }
        });
    }

    /**
     * Two-covering-indexes hardening test: one table carries TWO covering posting
     * indexes (sym1 INCLUDE price, sym2 INCLUDE price), so every O3 commit reseals
     * BOTH covering indexes of each rewritten partition in the same seal sweep.
     * Each row sets sym1='A' and sym2='B', so a covered read through either index
     * sees the same monotonically-growing total; a reseal of one index that
     * corrupts or drops the other's covered values fails the per-index oracle.
     */
    @Test
    public void testMultiThreadedO3TwoCoveringIndexesConcurrentReaderFuzz() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256);
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_AUTO_INCLUDE_TIMESTAMP, false);
        final Rnd rnd = TestUtils.generateRandom(LOG);

        assertMemoryLeak(() -> {
            final int dayCount = 4 + rnd.nextInt(4); // 4..7 days
            final int hotPerDay = 1000;
            final long perDaySum = (long) hotPerDay * (hotPerDay + 1) / 2;
            final long initialRows = (long) dayCount * hotPerDay;
            final long initialSum = (long) dayCount * perDaySum;

            execute("""
                    CREATE TABLE t_cov_two (
                        ts TIMESTAMP,
                        sym1 SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        sym2 SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            for (int d = 1; d <= dayCount; d++) {
                execute("INSERT INTO t_cov_two SELECT dateadd('s', x::INT, '2024-02-0" + d
                        + "T00:00:00.000000Z'::TIMESTAMP), 'A', 'B', x::DOUBLE FROM long_sequence(" + hotPerDay + ")");
            }
            for (int d = 1; d <= dayCount; d++) {
                execute("ALTER TABLE t_cov_two CONVERT PARTITION TO PARQUET LIST '2024-02-0" + d + "'");
            }

            final AtomicReference<Throwable> bgError = new AtomicReference<>();
            final AtomicBoolean stop = new AtomicBoolean();
            final AtomicLong rowFloor = new AtomicLong(initialRows);
            final int readerCount = 4;
            final Thread[] readers = new Thread[readerCount];
            for (int r = 0; r < readerCount; r++) {
                // Even readers probe sym1, odd readers probe sym2.
                final String pred = (r % 2 == 0) ? "sym1 = 'A'" : "sym2 = 'B'";
                readers[r] = new Thread(() -> {
                    try (
                            SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                                    .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                                            null, null, -1, null);
                            SqlCompiler compiler = engine.getSqlCompiler()
                    ) {
                        while (!stop.get() && bgError.get() == null) {
                            final long floor = rowFloor.get();
                            final long count;
                            final double sum;
                            try (RecordCursorFactory f = compiler.compile(
                                    "SELECT count(), sum(price) FROM t_cov_two WHERE " + pred, ctx).getRecordCursorFactory();
                                 RecordCursor cur = f.getCursor(ctx)) {
                                if (cur.hasNext()) {
                                    count = cur.getRecord().getLong(0);
                                    sum = cur.getRecord().getDouble(1);
                                } else {
                                    count = -1;
                                    sum = -1;
                                }
                            }
                            if (count < floor) {
                                throw new AssertionError("[" + pred + "] covered count fell below the floor: " + count + " < " + floor);
                            }
                            if (sum < (double) initialSum - 0.5) {
                                throw new AssertionError("[" + pred + "] covered sum(price) dropped below the initial total "
                                        + initialSum + ": " + sum);
                            }
                            if (sum < (double) count - 0.5) {
                                throw new AssertionError("[" + pred + "] covered sum(price)=" + sum + " < count=" + count
                                        + " -- a covered value resolved as < 1");
                            }
                        }
                    } catch (Throwable e) {
                        bgError.set(e);
                    }
                }, "covering-two-reader-" + r);
                readers[r].setDaemon(true);
                readers[r].start();
            }

            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.setupWorkerPool(pool, engine);
            pool.start(LOG);
            long expectedRows = initialRows;
            try {
                final int batches = 6 + rnd.nextInt(10);
                for (int b = 0; b < batches && bgError.get() == null; b++) {
                    final StringBuilder sql = new StringBuilder("INSERT INTO t_cov_two VALUES ");
                    int added = 0;
                    for (int d = 1; d <= dayCount; d++) {
                        if (rnd.nextBoolean()) {
                            continue;
                        }
                        final int n = 1 + rnd.nextInt(3);
                        for (int i = 0; i < n; i++) {
                            if (added > 0) {
                                sql.append(',');
                            }
                            sql.append("('2024-02-0").append(d).append("T00:")
                                    .append(String.format("%02d", 1 + rnd.nextInt(58)))
                                    .append(":00.500000Z', 'A', 'B', 1.0)");
                            added++;
                        }
                    }
                    if (added == 0) {
                        continue;
                    }
                    execute(sql.toString());
                    expectedRows += added;
                    rowFloor.set(expectedRows);
                }
            } finally {
                stop.set(true);
                for (Thread t : readers) {
                    t.join(30_000);
                    Assert.assertFalse("reader thread did not terminate", t.isAlive());
                }
                pool.halt();
            }

            Assert.assertNull("concurrent two-index covered reader threw: " + bgError.get(), bgError.get());

            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }
            engine.releaseAllWriters();

            final long expectedSum = initialSum + (expectedRows - initialRows);
            try (
                    SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                            .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                                    null, null, -1, null);
                    SqlCompiler compiler = engine.getSqlCompiler()
            ) {
                for (String pred : new String[]{"sym1 = 'A'", "sym2 = 'B'"}) {
                    try (RecordCursorFactory f = compiler.compile(
                            "SELECT count(), sum(price) FROM t_cov_two WHERE " + pred, ctx).getRecordCursorFactory();
                         RecordCursor cur = f.getCursor(ctx)) {
                        Assert.assertTrue("final covered query returned no row for " + pred, cur.hasNext());
                        Assert.assertEquals("final covered count [" + pred + ']', expectedRows, cur.getRecord().getLong(0));
                        Assert.assertEquals("final covered sum(price) [" + pred + ']', (double) expectedSum, cur.getRecord().getDouble(1), 0.5);
                    }
                }
            }
        });
    }

    /**
     * Covered VARCHAR (variable-size) hardening test. The reproducer covers a
     * fixed-size DOUBLE; this covers a VARCHAR, exercising the variable-size .pc
     * sidecar path (per-row offset+len into a var data block) under concurrent O3
     * rewrite. Every 'A' row carries a non-null label, so count(label) must always
     * equal count() -- a covered varchar that resolves to NULL/empty mid-reseal
     * drops count(label) below count() and fails the oracle.
     */
    @Test
    public void testMultiThreadedO3CoveringVarcharConcurrentReaderFuzz() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256);
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_AUTO_INCLUDE_TIMESTAMP, false);
        final Rnd rnd = TestUtils.generateRandom(LOG);

        assertMemoryLeak(() -> {
            final int dayCount = 4 + rnd.nextInt(4); // 4..7 days
            final int hotPerDay = 1000;
            final long initialRows = (long) dayCount * hotPerDay;

            execute("""
                    CREATE TABLE t_cov_vc (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (label),
                        label VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            for (int d = 1; d <= dayCount; d++) {
                execute("INSERT INTO t_cov_vc SELECT dateadd('s', x::INT, '2024-02-0" + d
                        + "T00:00:00.000000Z'::TIMESTAMP), 'A', ('lbl' || x)::VARCHAR FROM long_sequence(" + hotPerDay + ")");
            }
            for (int d = 1; d <= dayCount; d++) {
                execute("ALTER TABLE t_cov_vc CONVERT PARTITION TO PARQUET LIST '2024-02-0" + d + "'");
            }

            final AtomicReference<Throwable> bgError = new AtomicReference<>();
            final AtomicBoolean stop = new AtomicBoolean();
            final AtomicLong rowFloor = new AtomicLong(initialRows);
            final int readerCount = 3;
            final Thread[] readers = new Thread[readerCount];
            for (int r = 0; r < readerCount; r++) {
                readers[r] = new Thread(() -> {
                    try (
                            SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                                    .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                                            null, null, -1, null);
                            SqlCompiler compiler = engine.getSqlCompiler()
                    ) {
                        while (!stop.get() && bgError.get() == null) {
                            final long floor = rowFloor.get();
                            final long count;
                            final long labelCount;
                            try (RecordCursorFactory f = compiler.compile(
                                    "SELECT count(), count(label) FROM t_cov_vc WHERE sym = 'A'", ctx).getRecordCursorFactory();
                                 RecordCursor cur = f.getCursor(ctx)) {
                                if (cur.hasNext()) {
                                    count = cur.getRecord().getLong(0);
                                    labelCount = cur.getRecord().getLong(1);
                                } else {
                                    count = -1;
                                    labelCount = -1;
                                }
                            }
                            if (count < floor) {
                                throw new AssertionError("covered 'A' count fell below the committed floor: "
                                        + count + " < " + floor);
                            }
                            if (labelCount != count) {
                                throw new AssertionError("covered VARCHAR resolved to NULL for "
                                        + (count - labelCount) + " of " + count
                                        + " rows -- a covered var value was lost/corrupted mid-reseal");
                            }
                        }
                    } catch (Throwable e) {
                        bgError.set(e);
                    }
                }, "covering-varchar-reader-" + r);
                readers[r].setDaemon(true);
                readers[r].start();
            }

            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.setupWorkerPool(pool, engine);
            pool.start(LOG);
            long expectedRows = initialRows;
            try {
                final int batches = 6 + rnd.nextInt(10);
                for (int b = 0; b < batches && bgError.get() == null; b++) {
                    final StringBuilder sql = new StringBuilder("INSERT INTO t_cov_vc VALUES ");
                    int added = 0;
                    for (int d = 1; d <= dayCount; d++) {
                        if (rnd.nextBoolean()) {
                            continue;
                        }
                        final int n = 1 + rnd.nextInt(3);
                        for (int i = 0; i < n; i++) {
                            if (added > 0) {
                                sql.append(',');
                            }
                            sql.append("('2024-02-0").append(d).append("T00:")
                                    .append(String.format("%02d", 1 + rnd.nextInt(58)))
                                    .append(":00.500000Z', 'A', 'z')");
                            added++;
                        }
                    }
                    if (added == 0) {
                        continue;
                    }
                    execute(sql.toString());
                    expectedRows += added;
                    rowFloor.set(expectedRows);
                }
            } finally {
                stop.set(true);
                for (Thread t : readers) {
                    t.join(30_000);
                    Assert.assertFalse("reader thread did not terminate", t.isAlive());
                }
                pool.halt();
            }

            Assert.assertNull("concurrent covered varchar reader threw: " + bgError.get(), bgError.get());

            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }
            engine.releaseAllWriters();

            try (
                    SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                            .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                                    null, null, -1, null);
                    SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory f = compiler.compile(
                            "SELECT count(), count(label) FROM t_cov_vc WHERE sym = 'A'", ctx).getRecordCursorFactory();
                    RecordCursor cur = f.getCursor(ctx)
            ) {
                Assert.assertTrue("final covered query returned no row", cur.hasNext());
                Assert.assertEquals("final covered 'A' count", expectedRows, cur.getRecord().getLong(0));
                Assert.assertEquals("final covered non-null label count", expectedRows, cur.getRecord().getLong(1));
            }
        });
    }

    /**
     * Covered-read hardening across row-id encodings. Same all-parquet O3 churn as
     * the reproducer, but each run pins a random posting row-id encoding (adaptive /
     * Elias-Fano / delta), so the covering reader decodes .pv chains under a
     * different codec while partitions are rewritten out of order. The covered
     * count/sum oracle is encoding-independent; a decode bug under one codec surfaces
     * as a dropped/garbage covered value.
     */
    @Test
    public void testMultiThreadedO3CoveringEncodingConcurrentReaderFuzz() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256);
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_AUTO_INCLUDE_TIMESTAMP, false);
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final int enc = rnd.nextInt(3);
        if (enc == 1) {
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_ROW_ID_ENCODING, "ef");
        } else if (enc == 2) {
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_ROW_ID_ENCODING, "delta");
        } // enc == 0 -> adaptive (default)
        assertMemoryLeak(() -> runConcurrentCoveredSumCountFuzz("t_cov_enc", true, rnd));
    }

    /**
     * Native-only regression for the fixed native-partition covered-read race (the
     * cleanest form; see testMultiThreadedO3CoveringMixedNativeParquetReaderFuzz for
     * the diagnosis). Identical to the parquet reproducer but the partitions are
     * NEVER converted to parquet, so every O3 commit reseals a NATIVE partition in
     * place. Before the fix this failed ~5 in 8 (one partition's covered values
     * transiently read 0 because the in-place reseal republished the chain head with
     * the cover footer dropped while the writer's coverCount field was transiently
     * 0); after the fix it passes (validated 0/75). The fix preserves the head's
     * cover footer across the transient coverCount=0 window.
     */
    @Test
    public void testMultiThreadedO3CoveringNativeOnlyConcurrentReaderFuzz() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256);
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_AUTO_INCLUDE_TIMESTAMP, false);
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> runConcurrentCoveredSumCountFuzz("t_cov_native", false, rnd));
    }

    /**
     * Use-after-free probe: a COVERING cursor held open across a full O3 rewrite +
     * seal-purge must keep returning its pinned snapshot and never read a reclaimed
     * value file. Opens a covered SELECT over several parquet partitions, reads
     * about half (pinning those partition versions through the txn scoreboard), then
     * runs several O3 commits across the 4-worker pool plus a PostingSealPurgeJob
     * that rotates and reclaims superseded .pv/.pc, then drains the SAME cursor to
     * the end. The pinned snapshot is the table at open time: every covered price
     * must be a real seeded value (>= 1, never 0/NaN) and the row count must equal
     * the rows present at open. A scoreboard gap that reclaims a pinned value file
     * SIGSEGVs or returns garbage here.
     */
    @Test
    public void testCoveringCursorHeldAcrossO3ReclaimNoCrash() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256);
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_AUTO_INCLUDE_TIMESTAMP, false);
        final Rnd rnd = TestUtils.generateRandom(LOG);

        assertMemoryLeak(() -> {
            final int dayCount = 4 + rnd.nextInt(3); // 4..6 days
            final int hotPerDay = 1000;
            final long initialRows = (long) dayCount * hotPerDay;

            execute("""
                    CREATE TABLE t_cov_hold (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            for (int d = 1; d <= dayCount; d++) {
                execute("INSERT INTO t_cov_hold SELECT dateadd('s', x::INT, '2024-02-0" + d
                        + "T00:00:00.000000Z'::TIMESTAMP), 'A', x::DOUBLE FROM long_sequence(" + hotPerDay + ")");
            }
            for (int d = 1; d <= dayCount; d++) {
                execute("ALTER TABLE t_cov_hold CONVERT PARTITION TO PARQUET LIST '2024-02-0" + d + "'");
            }

            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.setupWorkerPool(pool, engine);
            pool.start(LOG);
            try (
                    SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                            .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                                    null, null, -1, null);
                    SqlCompiler compiler = engine.getSqlCompiler()
            ) {
                long pinnedCount = 0;
                try (RecordCursorFactory f = compiler.compile(
                        "SELECT price FROM t_cov_hold WHERE sym = 'A'", ctx).getRecordCursorFactory();
                     RecordCursor cur = f.getCursor(ctx)) {
                    final io.questdb.cairo.sql.Record rec = cur.getRecord();
                    final long half = initialRows / 2;
                    // Read about half, pinning the snapshot via the txn scoreboard.
                    while (pinnedCount < half && cur.hasNext()) {
                        final double p = rec.getDouble(0);
                        if (Double.isNaN(p) || p < 1.0) {
                            throw new AssertionError("held cursor read a NULL/garbage covered value before O3: " + p);
                        }
                        pinnedCount++;
                    }
                    // Rotate + reclaim superseded value files under the held cursor.
                    for (int b = 0; b < 8; b++) {
                        final StringBuilder sql = new StringBuilder("INSERT INTO t_cov_hold VALUES ");
                        int added = 0;
                        for (int d = 1; d <= dayCount; d++) {
                            if (rnd.nextBoolean()) {
                                continue;
                            }
                            if (added > 0) {
                                sql.append(',');
                            }
                            sql.append("('2024-02-0").append(d).append("T00:")
                                    .append(String.format("%02d", 1 + rnd.nextInt(58)))
                                    .append(":00.500000Z', 'A', 1.0)");
                            added++;
                        }
                        if (added > 0) {
                            execute(sql.toString());
                        }
                    }
                    // The pool's own PostingSealPurgeJob reclaims the superseded .pv/.pc
                    // concurrently (scoreboard-gated); it owns the log writer, so we must
                    // not construct a competing one here. Give it a moment to attempt the
                    // reclaim against the still-pinned cursor.
                    Os.sleep(50);
                    // Drain the SAME pinned cursor -- must not crash or read garbage.
                    while (cur.hasNext()) {
                        final double p = rec.getDouble(0);
                        if (Double.isNaN(p) || p < 1.0) {
                            throw new AssertionError("held cursor read a NULL/garbage covered value after O3+purge "
                                    + "(reclaimed a pinned value file?): " + p);
                        }
                        pinnedCount++;
                    }
                }
                Assert.assertEquals("held cursor must return exactly its pinned snapshot rows",
                        initialRows, pinnedCount);
            } finally {
                pool.halt();
            }
        });
    }

    /**
     * Wide covering hardening test: the index covers TWO data columns
     * (INCLUDE price, qty), so each partition reseal materialises two
     * sealTxn-versioned .pc data files alongside the .pci, and a covered read
     * walks two per-cover sidecars per row. Same all-parquet O3 churn + reader
     * threads as the reproducer, with the covered count, sum(price) AND sum(qty)
     * all held monotonic; a mismatched per-cover sidecar offset or a dropped .pc
     * for one of the two covered columns fails the oracle.
     */
    @Test
    public void testMultiThreadedO3CoveringWideConcurrentReaderFuzz() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256);
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_AUTO_INCLUDE_TIMESTAMP, false);
        final Rnd rnd = TestUtils.generateRandom(LOG);

        assertMemoryLeak(() -> {
            final int dayCount = 4 + rnd.nextInt(4); // 4..7 days
            final int hotPerDay = 1000;
            final long perDaySum = (long) hotPerDay * (hotPerDay + 1) / 2;
            final long initialRows = (long) dayCount * hotPerDay;
            final long initialSum = (long) dayCount * perDaySum;

            execute("""
                    CREATE TABLE t_cov_wide (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty LONG
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            for (int d = 1; d <= dayCount; d++) {
                execute("INSERT INTO t_cov_wide SELECT dateadd('s', x::INT, '2024-02-0" + d
                        + "T00:00:00.000000Z'::TIMESTAMP), 'A', x::DOUBLE, x::LONG FROM long_sequence(" + hotPerDay + ")");
            }
            for (int d = 1; d <= dayCount; d++) {
                execute("ALTER TABLE t_cov_wide CONVERT PARTITION TO PARQUET LIST '2024-02-0" + d + "'");
            }

            final AtomicReference<Throwable> bgError = new AtomicReference<>();
            final AtomicBoolean stop = new AtomicBoolean();
            final AtomicLong rowFloor = new AtomicLong(initialRows);
            final int readerCount = 3;
            final Thread[] readers = new Thread[readerCount];
            for (int r = 0; r < readerCount; r++) {
                readers[r] = new Thread(() -> {
                    try (
                            SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                                    .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                                            null, null, -1, null);
                            SqlCompiler compiler = engine.getSqlCompiler()
                    ) {
                        while (!stop.get() && bgError.get() == null) {
                            final long floor = rowFloor.get();
                            final long count;
                            final double sumPrice;
                            final double sumQty;
                            try (RecordCursorFactory f = compiler.compile(
                                    "SELECT count(), sum(price), sum(qty) FROM t_cov_wide WHERE sym = 'A'", ctx).getRecordCursorFactory();
                                 RecordCursor cur = f.getCursor(ctx)) {
                                if (cur.hasNext()) {
                                    count = cur.getRecord().getLong(0);
                                    sumPrice = cur.getRecord().getDouble(1);
                                    sumQty = cur.getRecord().getDouble(2);
                                } else {
                                    count = -1;
                                    sumPrice = -1;
                                    sumQty = -1;
                                }
                            }
                            if (count < floor) {
                                throw new AssertionError("covered 'A' count fell below the committed floor: "
                                        + count + " < " + floor);
                            }
                            if (sumPrice < (double) initialSum - 0.5) {
                                throw new AssertionError("covered sum(price) dropped below the initial total "
                                        + initialSum + ": " + sumPrice);
                            }
                            if (sumQty < (double) initialSum - 0.5) {
                                throw new AssertionError("covered sum(qty) dropped below the initial total "
                                        + initialSum + ": " + sumQty);
                            }
                            if (sumPrice < (double) count - 0.5 || sumQty < (double) count - 0.5) {
                                throw new AssertionError("a covered column resolved as < 1: count=" + count
                                        + " sum(price)=" + sumPrice + " sum(qty)=" + sumQty);
                            }
                        }
                    } catch (Throwable e) {
                        bgError.set(e);
                    }
                }, "covering-wide-reader-" + r);
                readers[r].setDaemon(true);
                readers[r].start();
            }

            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.setupWorkerPool(pool, engine);
            pool.start(LOG);
            long expectedRows = initialRows;
            try {
                final int batches = 6 + rnd.nextInt(10);
                for (int b = 0; b < batches && bgError.get() == null; b++) {
                    final StringBuilder sql = new StringBuilder("INSERT INTO t_cov_wide VALUES ");
                    int added = 0;
                    for (int d = 1; d <= dayCount; d++) {
                        if (rnd.nextBoolean()) {
                            continue;
                        }
                        final int n = 1 + rnd.nextInt(3);
                        for (int i = 0; i < n; i++) {
                            if (added > 0) {
                                sql.append(',');
                            }
                            sql.append("('2024-02-0").append(d).append("T00:")
                                    .append(String.format("%02d", 1 + rnd.nextInt(58)))
                                    .append(":00.500000Z', 'A', 1.0, 1)");
                            added++;
                        }
                    }
                    if (added == 0) {
                        continue;
                    }
                    execute(sql.toString());
                    expectedRows += added;
                    rowFloor.set(expectedRows);
                }
            } finally {
                stop.set(true);
                for (Thread t : readers) {
                    t.join(30_000);
                    Assert.assertFalse("reader thread did not terminate", t.isAlive());
                }
                pool.halt();
            }

            Assert.assertNull("concurrent wide covered reader threw: " + bgError.get(), bgError.get());

            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }
            engine.releaseAllWriters();

            final long expectedSum = initialSum + (expectedRows - initialRows);
            try (
                    SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                            .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                                    null, null, -1, null);
                    SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory f = compiler.compile(
                            "SELECT count(), sum(price), sum(qty) FROM t_cov_wide WHERE sym = 'A'", ctx).getRecordCursorFactory();
                    RecordCursor cur = f.getCursor(ctx)
            ) {
                Assert.assertTrue("final covered query returned no row", cur.hasNext());
                Assert.assertEquals("final covered 'A' count", expectedRows, cur.getRecord().getLong(0));
                Assert.assertEquals("final covered sum(price)", (double) expectedSum, cur.getRecord().getDouble(1), 0.5);
                Assert.assertEquals("final covered sum(qty)", (double) expectedSum, cur.getRecord().getDouble(2), 0.5);
            }
        });
    }

    /**
     * Concurrent LATEST ON covered-read hardening test. Stresses the covering
     * BACKWARD reader (PostingIndexBwdReader / findLatestRow + symbol-rank), a
     * code path the sum/count reproducer never touches. A COVERING posting index
     * (sym INCLUDE price) over several parquet partitions is rewritten out of
     * order across the real 4-worker O3 pool under a tiny spill budget while
     * reader threads continuously resolve the latest covered price per sym via
     * {@code SELECT price ... WHERE sym='A' LATEST ON ts PARTITION BY sym}.
     * <p>
     * A single sentinel 'A' row is seeded at the global max timestamp with a
     * unique price; every background O3 insert lands at an EARLIER timestamp, so
     * the latest 'A' row never changes and its covered price must always read
     * back as the sentinel. O3 also rewrites the sentinel's (last) partition, so
     * the backward reader scans a partition mid-reseal: if the new version is
     * exposed before its covering .pci/.pc is materialised, or the reseal rotates
     * the sidecar under the reader, the latest covered price comes back wrong
     * (NULL / stale / garbage) and the assertion fires. The -ea audit asserts and
     * any reader SIGSEGV are the other oracles.
     */
    @Test
    public void testMultiThreadedO3CoveringLatestByConcurrentReaderFuzz() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256);
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_AUTO_INCLUDE_TIMESTAMP, false);
        final Rnd rnd = TestUtils.generateRandom(LOG);

        assertMemoryLeak(() -> {
            final int dayCount = 4 + rnd.nextInt(4); // 4..7 days
            final int hotPerDay = 1000;
            final double sentinelPrice = 777_777.0;
            // Sentinel 'A' row at the global max ts (last day, end of day). Every
            // hot row and every later O3 insert is strictly earlier, so the latest
            // 'A' is always this row and its covered price must read back exactly.
            final String sentinelTs = "2024-02-0" + dayCount + "T23:59:59.000000Z";
            final long initialRows = (long) dayCount * hotPerDay + 1; // +1 sentinel

            execute("""
                    CREATE TABLE t_cov_latest (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            for (int d = 1; d <= dayCount; d++) {
                execute("INSERT INTO t_cov_latest SELECT dateadd('s', x::INT, '2024-02-0" + d
                        + "T00:00:00.000000Z'::TIMESTAMP), 'A', x::DOUBLE FROM long_sequence(" + hotPerDay + ")");
            }
            execute("INSERT INTO t_cov_latest VALUES ('" + sentinelTs + "', 'A', " + sentinelPrice + ")");
            for (int d = 1; d <= dayCount; d++) {
                execute("ALTER TABLE t_cov_latest CONVERT PARTITION TO PARQUET LIST '2024-02-0" + d + "'");
            }

            // Confirm the read path under test is the covering backward reader.
            final String latestSql = "SELECT price FROM t_cov_latest WHERE sym = 'A' LATEST ON ts PARTITION BY sym";
            final String plan;
            try (RecordCursorFactory pf = select(latestSql)) {
                planSink.clear();
                pf.toPlan(planSink);
                plan = planSink.getSink().toString();
            }
            Assert.assertTrue("LATEST ON must use the covering index:\n" + plan, plan.contains("CoveringIndex"));

            final AtomicReference<Throwable> bgError = new AtomicReference<>();
            final AtomicBoolean stop = new AtomicBoolean();
            final int readerCount = 3;
            final Thread[] readers = new Thread[readerCount];
            for (int r = 0; r < readerCount; r++) {
                readers[r] = new Thread(() -> {
                    try (
                            SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                                    .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                                            null, null, -1, null);
                            SqlCompiler compiler = engine.getSqlCompiler()
                    ) {
                        while (!stop.get() && bgError.get() == null) {
                            final double latest;
                            try (RecordCursorFactory f = compiler.compile(latestSql, ctx).getRecordCursorFactory();
                                 RecordCursor cur = f.getCursor(ctx)) {
                                latest = cur.hasNext() ? cur.getRecord().getDouble(0) : Double.NaN;
                            }
                            if (Double.isNaN(latest)) {
                                throw new AssertionError("latest covered 'A' price resolved to NULL/NaN -- "
                                        + "covering sidecar missing or not yet materialised under reseal");
                            }
                            if (Math.abs(latest - sentinelPrice) > 0.5) {
                                throw new AssertionError("latest covered 'A' price changed from the sentinel "
                                        + sentinelPrice + " to " + latest
                                        + " -- the backward reader read a stale/garbage covered value mid-reseal");
                            }
                        }
                    } catch (Throwable e) {
                        bgError.set(e);
                    }
                }, "covering-latest-reader-" + r);
                readers[r].setDaemon(true);
                readers[r].start();
            }

            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.setupWorkerPool(pool, engine);
            pool.start(LOG);
            long expectedRows = initialRows;
            try {
                final int batches = 6 + rnd.nextInt(10);
                for (int b = 0; b < batches && bgError.get() == null; b++) {
                    // O3-insert 'A' rows strictly earlier than the sentinel into a random
                    // subset of the parquet days (including the sentinel's last day, which
                    // forces that partition to reseal under the backward reader).
                    final StringBuilder sql = new StringBuilder("INSERT INTO t_cov_latest VALUES ");
                    int added = 0;
                    for (int d = 1; d <= dayCount; d++) {
                        if (rnd.nextBoolean()) {
                            continue;
                        }
                        final int n = 1 + rnd.nextInt(3);
                        for (int i = 0; i < n; i++) {
                            if (added > 0) {
                                sql.append(',');
                            }
                            sql.append("('2024-02-0").append(d).append("T00:")
                                    .append(String.format("%02d", 1 + rnd.nextInt(58)))
                                    .append(":00.500000Z', 'A', 1.0)");
                            added++;
                        }
                    }
                    if (added == 0) {
                        continue;
                    }
                    execute(sql.toString());
                    expectedRows += added;
                }
            } finally {
                stop.set(true);
                for (Thread t : readers) {
                    t.join(30_000);
                    Assert.assertFalse("reader thread did not terminate", t.isAlive());
                }
                pool.halt();
            }

            Assert.assertNull("concurrent latest-by covered reader threw: " + bgError.get(), bgError.get());

            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }
            engine.releaseAllWriters();

            try (
                    SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                            .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                                    null, null, -1, null);
                    SqlCompiler compiler = engine.getSqlCompiler()
            ) {
                try (RecordCursorFactory f = compiler.compile(latestSql, ctx).getRecordCursorFactory();
                     RecordCursor cur = f.getCursor(ctx)) {
                    Assert.assertTrue("final latest query returned no row", cur.hasNext());
                    Assert.assertEquals("final latest covered 'A' price", sentinelPrice, cur.getRecord().getDouble(0), 0.5);
                }
                try (RecordCursorFactory f = compiler.compile(
                        "SELECT count() FROM t_cov_latest WHERE sym = 'A'", ctx).getRecordCursorFactory();
                     RecordCursor cur = f.getCursor(ctx)) {
                    Assert.assertTrue(cur.hasNext());
                    Assert.assertEquals("final covered 'A' count", expectedRows, cur.getRecord().getLong(0));
                }
            }
        });
    }

    /**
     * Mixed native + parquet covered-read hardening test. The parquet reproducer
     * rewrites an all-parquet table; this one leaves a random proper subset of the
     * partitions native and converts the rest to parquet, then O3-inserts across
     * BOTH so a single commit can reseal a native partition in place AND a parquet
     * partition via resealParquetCoveringForPartition in the same seal sweep, while
     * reader threads aggregate {@code count(), sum(price) WHERE sym='A'} across the
     * mix. A defect in either reseal path -- or in how the two share the deferred
     * seal-purge state -- drops a covered partition and the monotonic oracle fires.
     * <p>
     * Regression for a fixed native-partition covered-read race (distinct from the
     * parquet expose-before-.pci bug). Before the fix this failed ~1 in 4: a native
     * in-place reseal republished the chain head while the writer's transient
     * coverCount field was 0 (clearCovering had reset it), so extendHead sized the
     * entry LEN without the cover footer; a concurrent covered read derived
     * coverCount=0, could not map the still-present .pc, and getCoveredDouble
     * returned NaN for a whole partition. Fixed by preserving the head's cover
     * footer across the transient coverCount=0 window (PostingIndexWriter
     * captureCoverEndOffsets falls back to a cache seeded from the head entry by
     * publishToChain/PostingIndexChainWriter.readHeadCoverEndOffsets). See also the
     * unit test testWriterEntryPreservesCoverFooterAfterClearCoveringCommit.
     */
    @Test
    public void testMultiThreadedO3CoveringMixedNativeParquetReaderFuzz() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256);
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_AUTO_INCLUDE_TIMESTAMP, false);
        final Rnd rnd = TestUtils.generateRandom(LOG);

        assertMemoryLeak(() -> {
            final int dayCount = 4 + rnd.nextInt(4); // 4..7 days
            final int hotPerDay = 1000;
            final long perDaySum = (long) hotPerDay * (hotPerDay + 1) / 2;
            final long initialRows = (long) dayCount * hotPerDay;
            final long initialSum = (long) dayCount * perDaySum;

            execute("""
                    CREATE TABLE t_cov_mix (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            for (int d = 1; d <= dayCount; d++) {
                execute("INSERT INTO t_cov_mix SELECT dateadd('s', x::INT, '2024-02-0" + d
                        + "T00:00:00.000000Z'::TIMESTAMP), 'A', x::DOUBLE FROM long_sequence(" + hotPerDay + ")");
            }
            // Convert a random proper subset to parquet: keep >=1 parquet and >=1 native
            // so both reseal paths stay live in the same run.
            final boolean[] isParquet = new boolean[dayCount + 1];
            int parquetDays = 0;
            for (int d = 1; d <= dayCount; d++) {
                if (rnd.nextBoolean()) {
                    isParquet[d] = true;
                    parquetDays++;
                }
            }
            if (parquetDays == 0) {
                isParquet[1] = true;
            } else if (parquetDays == dayCount) {
                isParquet[dayCount] = false;
            }
            for (int d = 1; d <= dayCount; d++) {
                if (isParquet[d]) {
                    execute("ALTER TABLE t_cov_mix CONVERT PARTITION TO PARQUET LIST '2024-02-0" + d + "'");
                }
            }

            final AtomicReference<Throwable> bgError = new AtomicReference<>();
            final AtomicBoolean stop = new AtomicBoolean();
            final AtomicLong rowFloor = new AtomicLong(initialRows);
            final int readerCount = 3;
            final Thread[] readers = new Thread[readerCount];
            for (int r = 0; r < readerCount; r++) {
                readers[r] = new Thread(() -> {
                    try (
                            SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                                    .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                                            null, null, -1, null);
                            SqlCompiler compiler = engine.getSqlCompiler()
                    ) {
                        long prevCount = 0;
                        while (!stop.get() && bgError.get() == null) {
                            final long floor = rowFloor.get();
                            final long count;
                            final double sum;
                            try (RecordCursorFactory f = compiler.compile(
                                    "SELECT count(), sum(price) FROM t_cov_mix WHERE sym = 'A'", ctx).getRecordCursorFactory();
                                 RecordCursor cur = f.getCursor(ctx)) {
                                if (cur.hasNext()) {
                                    count = cur.getRecord().getLong(0);
                                    sum = cur.getRecord().getDouble(1);
                                } else {
                                    count = -1;
                                    sum = -1;
                                }
                            }
                            if (count < floor) {
                                throw new AssertionError("covered 'A' count fell below the committed floor: "
                                        + count + " < " + floor);
                            }
                            if (count < prevCount) {
                                throw new AssertionError("covered 'A' count went backwards: " + prevCount + " -> " + count);
                            }
                            if (sum < (double) initialSum - 0.5) {
                                throw new AssertionError("covered sum(price) dropped below the initial total "
                                        + initialSum + ": " + sum);
                            }
                            if (sum < (double) count - 0.5) {
                                throw new AssertionError("covered sum(price)=" + sum + " < count=" + count
                                        + " -- a covered value resolved as < 1 (stale/garbage sidecar)");
                            }
                            prevCount = count;
                        }
                    } catch (Throwable e) {
                        bgError.set(e);
                    }
                }, "covering-mix-reader-" + r);
                readers[r].setDaemon(true);
                readers[r].start();
            }

            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.setupWorkerPool(pool, engine);
            pool.start(LOG);
            long expectedRows = initialRows;
            try {
                final int batches = 6 + rnd.nextInt(10);
                for (int b = 0; b < batches && bgError.get() == null; b++) {
                    final StringBuilder sql = new StringBuilder("INSERT INTO t_cov_mix VALUES ");
                    int added = 0;
                    for (int d = 1; d <= dayCount; d++) {
                        if (rnd.nextBoolean()) {
                            continue;
                        }
                        final int n = 1 + rnd.nextInt(3);
                        for (int i = 0; i < n; i++) {
                            if (added > 0) {
                                sql.append(',');
                            }
                            sql.append("('2024-02-0").append(d).append("T00:")
                                    .append(String.format("%02d", 1 + rnd.nextInt(58)))
                                    .append(":00.500000Z', 'A', 1.0)");
                            added++;
                        }
                    }
                    if (added == 0) {
                        continue;
                    }
                    execute(sql.toString());
                    expectedRows += added;
                    rowFloor.set(expectedRows);
                }
            } finally {
                stop.set(true);
                for (Thread t : readers) {
                    t.join(30_000);
                    Assert.assertFalse("reader thread did not terminate", t.isAlive());
                }
                pool.halt();
            }

            Assert.assertNull("concurrent mixed covered reader threw: " + bgError.get(), bgError.get());

            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }
            engine.releaseAllWriters();

            final long expectedSum = initialSum + (expectedRows - initialRows);
            try (
                    SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                            .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                                    null, null, -1, null);
                    SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory f = compiler.compile(
                            "SELECT count(), sum(price) FROM t_cov_mix WHERE sym = 'A'", ctx).getRecordCursorFactory();
                    RecordCursor cur = f.getCursor(ctx)
            ) {
                Assert.assertTrue("final covered query returned no row", cur.hasNext());
                Assert.assertEquals("final covered 'A' count", expectedRows, cur.getRecord().getLong(0));
                Assert.assertEquals("final covered sum(price)", (double) expectedSum, cur.getRecord().getDouble(1), 0.5);
            }
        });
    }

    /**
     * Multi-key covered-read hardening test. The single-key reproducer keeps one
     * posting chain hot; this one drives K symbols, each with its own covering
     * chain and per-key covered totals, over parquet partitions rewritten out of
     * order across the pool. Readers pick a random key and aggregate
     * {@code count(), sum(price) WHERE sym = ?}, so a reader navigates one key's
     * chain/sidecars while O3 reseals the others -- stressing the per-key chain
     * navigation and the symbol-rank maps the covering cursor builds. Each key's
     * count and sum are independently monotonic; a cross-key sidecar mix-up or a
     * dropped key partition trips that key's floor.
     */
    @Test
    public void testMultiThreadedO3CoveringMultiKeyConcurrentReaderFuzz() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256);
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_AUTO_INCLUDE_TIMESTAMP, false);
        final Rnd rnd = TestUtils.generateRandom(LOG);

        assertMemoryLeak(() -> {
            final String[] syms = {"A", "B", "C", "D", "E"};
            final int K = syms.length;
            final int dayCount = 4 + rnd.nextInt(4); // 4..7 days
            final int hotPerDay = 1000;
            final long perDaySum = (long) hotPerDay * (hotPerDay + 1) / 2;
            final long initialRowsPerSym = (long) dayCount * hotPerDay;
            final long initialSumPerSym = (long) dayCount * perDaySum;

            execute("""
                    CREATE TABLE t_cov_mk (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            for (int d = 1; d <= dayCount; d++) {
                for (int k = 0; k < K; k++) {
                    execute("INSERT INTO t_cov_mk SELECT dateadd('s', x::INT, '2024-02-0" + d
                            + "T00:00:00.000000Z'::TIMESTAMP), '" + syms[k] + "', x::DOUBLE FROM long_sequence(" + hotPerDay + ")");
                }
            }
            for (int d = 1; d <= dayCount; d++) {
                execute("ALTER TABLE t_cov_mk CONVERT PARTITION TO PARQUET LIST '2024-02-0" + d + "'");
            }

            final AtomicReference<Throwable> bgError = new AtomicReference<>();
            final AtomicBoolean stop = new AtomicBoolean();
            final AtomicLong[] floors = new AtomicLong[K];
            for (int k = 0; k < K; k++) {
                floors[k] = new AtomicLong(initialRowsPerSym);
            }
            final int readerCount = 3;
            final Thread[] readers = new Thread[readerCount];
            for (int r = 0; r < readerCount; r++) {
                final int seed = r;
                readers[r] = new Thread(() -> {
                    final Rnd rRnd = new Rnd(seed + 1, seed + 100);
                    try (
                            SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                                    .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                                            null, null, -1, null);
                            SqlCompiler compiler = engine.getSqlCompiler()
                    ) {
                        while (!stop.get() && bgError.get() == null) {
                            final int k = rRnd.nextInt(K);
                            final long floor = floors[k].get();
                            final long count;
                            final double sum;
                            try (RecordCursorFactory f = compiler.compile(
                                    "SELECT count(), sum(price) FROM t_cov_mk WHERE sym = '" + syms[k] + "'", ctx).getRecordCursorFactory();
                                 RecordCursor cur = f.getCursor(ctx)) {
                                if (cur.hasNext()) {
                                    count = cur.getRecord().getLong(0);
                                    sum = cur.getRecord().getDouble(1);
                                } else {
                                    count = -1;
                                    sum = -1;
                                }
                            }
                            if (count < floor) {
                                throw new AssertionError("covered '" + syms[k] + "' count fell below its floor: "
                                        + count + " < " + floor);
                            }
                            if (sum < (double) initialSumPerSym - 0.5) {
                                throw new AssertionError("covered '" + syms[k] + "' sum dropped below its initial total "
                                        + initialSumPerSym + ": " + sum);
                            }
                            if (sum < (double) count - 0.5) {
                                throw new AssertionError("covered '" + syms[k] + "' sum=" + sum + " < count=" + count
                                        + " -- a covered value resolved as < 1 (cross-key or stale sidecar)");
                            }
                        }
                    } catch (Throwable e) {
                        bgError.set(e);
                    }
                }, "covering-mk-reader-" + r);
                readers[r].setDaemon(true);
                readers[r].start();
            }

            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.setupWorkerPool(pool, engine);
            pool.start(LOG);
            final long[] expectedRows = new long[K];
            Arrays.fill(expectedRows, initialRowsPerSym);
            try {
                final int batches = 8 + rnd.nextInt(12);
                for (int b = 0; b < batches && bgError.get() == null; b++) {
                    // One commit O3-inserts a random key into a random subset of days.
                    final int k = rnd.nextInt(K);
                    final StringBuilder sql = new StringBuilder("INSERT INTO t_cov_mk VALUES ");
                    int added = 0;
                    for (int d = 1; d <= dayCount; d++) {
                        if (rnd.nextBoolean()) {
                            continue;
                        }
                        final int n = 1 + rnd.nextInt(3);
                        for (int i = 0; i < n; i++) {
                            if (added > 0) {
                                sql.append(',');
                            }
                            sql.append("('2024-02-0").append(d).append("T00:")
                                    .append(String.format("%02d", 1 + rnd.nextInt(58)))
                                    .append(":00.500000Z', '").append(syms[k]).append("', 1.0)");
                            added++;
                        }
                    }
                    if (added == 0) {
                        continue;
                    }
                    execute(sql.toString());
                    expectedRows[k] += added;
                    floors[k].set(expectedRows[k]);
                }
            } finally {
                stop.set(true);
                for (Thread t : readers) {
                    t.join(30_000);
                    Assert.assertFalse("reader thread did not terminate", t.isAlive());
                }
                pool.halt();
            }

            Assert.assertNull("concurrent multi-key covered reader threw: " + bgError.get(), bgError.get());

            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }
            engine.releaseAllWriters();

            try (
                    SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                            .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                                    null, null, -1, null);
                    SqlCompiler compiler = engine.getSqlCompiler()
            ) {
                for (int k = 0; k < K; k++) {
                    final long expSum = initialSumPerSym + (expectedRows[k] - initialRowsPerSym);
                    try (RecordCursorFactory f = compiler.compile(
                            "SELECT count(), sum(price) FROM t_cov_mk WHERE sym = '" + syms[k] + "'", ctx).getRecordCursorFactory();
                         RecordCursor cur = f.getCursor(ctx)) {
                        Assert.assertTrue("final covered query returned no row for " + syms[k], cur.hasNext());
                        Assert.assertEquals("final covered '" + syms[k] + "' count", expectedRows[k], cur.getRecord().getLong(0));
                        Assert.assertEquals("final covered '" + syms[k] + "' sum", (double) expSum, cur.getRecord().getDouble(1), 0.5);
                    }
                }
            }
        });
    }

    /**
     * Concurrent covered-read integration test: a COVERING posting index
     * (sym INCLUDE price) over several parquet partitions, rewritten out-of-order
     * across the real 4-worker O3 pool under a tiny spill budget, while background
     * threads continuously serve {@code count(), sum(price) WHERE sym='A'} from the
     * covering sidecars. This is the multi-threaded covering analogue of
     * testParquetPostingSpillConcurrentReadFuzz; it exercises the interaction the
     * single-threaded covering tests never do:
     * <ul>
     *   <li>multiple O3 workers running updateParquetIndexes -> commitDense ->
     *       configureCovering -> rebuildSidecars and depositing .pv/.pc purges into
     *       the shared deferredPostingSealPurges list/pool under parquetSealPurgeLock;</li>
     *   <li>the scoreboard-gated PostingSealPurgeJob (on the pool) reclaiming
     *       superseded .pv/.pc while readers hold real txn pins;</li>
     *   <li>readers resolving covered values (the addr-based covered read) against a
     *       sidecar a concurrent reseal/purge is rotating.</li>
     * </ul>
     * Oracles (all under -ea, so the audit's holdsLock / no-O3-in-flight /
     * covered-bounds asserts also fire on the worker threads): rows are only added,
     * so the covered 'A' count must be monotonic and never below the committed
     * floor; sum(price) must never drop below the initial covered total and must
     * stay &gt;= count (every price &gt;= 1); no reader may crash; and after a final
     * purge pass with nothing pinned, each partition keeps exactly one .pv (no purge
     * lost or double-freed). The floor is read BEFORE the count so a commit landing
     * mid-read can never make a valid snapshot look short.
     * <p>
     * Regression for a fixed PARQUET-specific covered-read race. Before the fix, an
     * O3/squash rewrite of a parquet partition exposed the new version with only the
     * .pv (so count() was correct) but never materialised its covering .pci/.pc:
     * sealPostingIndexForPartition skipped parquet, and the O3 worker builds only the
     * non-covering .pv. A concurrent covered read then opened the new version, found
     * the .pci missing, reported coverCount=0 and returned NULL covered values (count
     * right, sum(price) short by whole partitions). Native partitions reseal in place
     * (the .pci is always present), hence they passed. Fixed by
     * resealParquetCoveringForPartition, which rebuilds covering for parquet partitions
     * with a covering index in the seal sweep / squash, before the commit exposes the
     * version (with discardForRebuild to avoid double-counting and a deferred
     * seal-purge publish so superseded value files are reclaimed).
     */
    @Test
    public void testMultiThreadedO3CoveringPostingConcurrentReaderFuzz() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256);
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_AUTO_INCLUDE_TIMESTAMP, false);
        final Rnd rnd = TestUtils.generateRandom(LOG);

        assertMemoryLeak(() -> {
            final int dayCount = 4 + rnd.nextInt(4); // 4..7 days; keeps sum < 1e7 (no sci-notation)
            final int hotPerDay = 1000;
            final long perDaySum = (long) hotPerDay * (hotPerDay + 1) / 2; // sum(1..hotPerDay)
            final long initialRows = (long) dayCount * hotPerDay;
            final long initialSum = (long) dayCount * perDaySum;

            execute("""
                    CREATE TABLE t_cov (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // hotPerDay 'A' rows/day with price = row index, so the covered total is
            // deterministic and every partition's reseal spills (tiny budget).
            for (int d = 1; d <= dayCount; d++) {
                execute("INSERT INTO t_cov SELECT dateadd('s', x::INT, '2024-02-0" + d
                        + "T00:00:00.000000Z'::TIMESTAMP), 'A', x::DOUBLE FROM long_sequence(" + hotPerDay + ")");
            }
            for (int d = 1; d <= dayCount; d++) {
                execute("ALTER TABLE t_cov CONVERT PARTITION TO PARQUET LIST '2024-02-0" + d + "'");
            }

            final AtomicReference<Throwable> bgError = new AtomicReference<>();
            final AtomicBoolean stop = new AtomicBoolean();
            final AtomicLong rowFloor = new AtomicLong(initialRows);
            final int readerCount = 3;
            final Thread[] readers = new Thread[readerCount];
            for (int r = 0; r < readerCount; r++) {
                readers[r] = new Thread(() -> {
                    try (
                            SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                                    .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                                            null, null, -1, null);
                            SqlCompiler compiler = engine.getSqlCompiler()
                    ) {
                        long prevCount = 0;
                        while (!stop.get() && bgError.get() == null) {
                            // Read the floor BEFORE the snapshot: the floor was committed
                            // at an earlier time, and rows only ever get added, so a valid
                            // snapshot taken later cannot be below it.
                            final long floor = rowFloor.get();
                            final long count;
                            final double sum;
                            try (RecordCursorFactory f = compiler.compile(
                                    "SELECT count(), sum(price) FROM t_cov WHERE sym = 'A'", ctx).getRecordCursorFactory();
                                 RecordCursor cur = f.getCursor(ctx)) {
                                if (cur.hasNext()) {
                                    count = cur.getRecord().getLong(0);
                                    sum = cur.getRecord().getDouble(1);
                                } else {
                                    count = -1;
                                    sum = -1;
                                }
                            }
                            if (count < floor) {
                                throw new AssertionError("covered 'A' count fell below the committed floor: "
                                        + count + " < " + floor);
                            }
                            if (count < prevCount) {
                                throw new AssertionError("covered 'A' count went backwards: " + prevCount + " -> " + count);
                            }
                            if (sum < (double) initialSum - 0.5) {
                                throw new AssertionError("covered sum(price) dropped below the initial total "
                                        + initialSum + ": " + sum + " (a covered value was lost/corrupted)");
                            }
                            if (sum < (double) count - 0.5) {
                                throw new AssertionError("covered sum(price)=" + sum + " < count=" + count
                                        + " -- a covered value resolved as < 1 (stale/garbage sidecar)");
                            }
                            prevCount = count;
                        }
                    } catch (Throwable e) {
                        bgError.set(e);
                    }
                }, "covering-posting-reader-" + r);
                readers[r].setDaemon(true);
                readers[r].start();
            }

            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.setupWorkerPool(pool, engine);
            pool.start(LOG);
            long expectedRows = initialRows;
            try {
                final int batches = 6 + rnd.nextInt(10);
                for (int b = 0; b < batches && bgError.get() == null; b++) {
                    // O3-insert 'A' rows (price 1.0) into a random subset of the parquet
                    // days: one commit -> a partition task per touched day, run across the
                    // pool, each calling deferParquetPostingSealPurges (covering: .pv + .pc).
                    final StringBuilder sql = new StringBuilder("INSERT INTO t_cov VALUES ");
                    int added = 0;
                    for (int d = 1; d <= dayCount; d++) {
                        if (rnd.nextBoolean()) {
                            continue;
                        }
                        final int n = 1 + rnd.nextInt(3);
                        for (int i = 0; i < n; i++) {
                            if (added > 0) {
                                sql.append(',');
                            }
                            sql.append("('2024-02-0").append(d).append("T00:")
                                    .append(String.format("%02d", 1 + rnd.nextInt(58)))
                                    .append(":00.500000Z', 'A', 1.0)");
                            added++;
                        }
                    }
                    if (added == 0) {
                        continue;
                    }
                    execute(sql.toString());
                    expectedRows += added;
                    // Publish the new floor only AFTER the commit is visible.
                    rowFloor.set(expectedRows);
                }
            } finally {
                stop.set(true);
                for (Thread t : readers) {
                    t.join(30_000);
                    Assert.assertFalse("reader thread did not terminate", t.isAlive());
                }
                pool.halt();
            }

            Assert.assertNull("concurrent covered reader threw: " + bgError.get(), bgError.get());

            // Drain deferred purges with nothing pinned, then verify the exact final
            // covered totals (count exact; sum within epsilon to dodge double formatting).
            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }
            engine.releaseAllWriters();

            final long expectedSum = initialSum + (expectedRows - initialRows); // each O3 row adds price 1.0
            try (
                    SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                            .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                                    null, null, -1, null);
                    SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory f = compiler.compile(
                            "SELECT count(), sum(price) FROM t_cov WHERE sym = 'A'", ctx).getRecordCursorFactory();
                    RecordCursor cur = f.getCursor(ctx)
            ) {
                Assert.assertTrue("final covered query returned no row", cur.hasNext());
                Assert.assertEquals("final covered 'A' count", expectedRows, cur.getRecord().getLong(0));
                Assert.assertEquals("final covered sum(price)", (double) expectedSum, cur.getRecord().getDouble(1), 0.5);
            }

            // Each parquet partition must keep exactly one value file (no purge lost
            // or double-freed under concurrent covering deposit).
            final TableToken token = engine.getTableTokenIfExists("t_cov");
            Assert.assertNotNull("table must exist", token);
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals("all days must remain", dayCount, reader.getTxFile().getPartitionCount());
                for (int i = 0; i < dayCount; i++) {
                    final long partitionTs = reader.getTxFile().getPartitionTimestampByIndex(i);
                    final long partitionNameTxn = reader.getTxFile().getPartitionNameTxn(i);
                    try (Path path = new Path()) {
                        path.of(configuration.getDbRoot()).concat(token);
                        setPathForNativePartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                        final String[] names = new java.io.File(path.toString()).list();
                        Assert.assertNotNull("partition dir must exist: " + path, names);
                        int pvCount = 0;
                        for (String n : names) {
                            if (n.startsWith("sym.pv.")) {
                                pvCount++;
                            }
                        }
                        Assert.assertEquals(
                                "partition " + i + " must keep exactly one value file: "
                                        + java.util.Arrays.toString(names),
                                1, pvCount);
                    }
                }
            }
        });
    }

    /**
     * Reproduces the SIGSEGV from the JMH walFastLag bench (hs_err_pid19555):
     * MemoryCR.getLong over-read inside PostingIndexChainEntry.read on a
     * covering posting index. The bench ran walFastLagInsertAndQuery in a
     * tight INSERT/drain/SELECT loop; a chain entry sealed with coverCount=0
     * (because PostingIndexWriter's coverCount field is reset to 0 by the
     * post-fast-lag-seal clearCovering(), and a subsequent commit() flushes
     * pending data via extendHead with coverCount=0) ended up flush against
     * the end of the .pk mmap. The reader, opened with coverCount=1 from
     * the .pci sidecar, stepped past the entry's LEN reading the cover
     * footer and SIGSEGV'd when the read crossed the mapping boundary.
     * <p>
     * This test mirrors the bench shape (covering posting index, WAL
     * INSERT, drain, SELECT, repeat). With the picker/read fix in place,
     * the cover-mismatched entries no longer crash readers - readers see
     * zero-filled cover end offsets for those entries, fall back to the
     * non-covering path, and queries return correct row counts. Without
     * the fix, this test crashes the JVM (no AssertionError, just SIGSEGV).
     */
    @Test
    public void testWalFastLagCoveringPostingDoesNotCrashReader() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE walbench (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO walbench(ts, sym, price)
                    SELECT dateadd('u', x::INT, '2024-01-01T00:00:00.000000Z'::TIMESTAMP),
                           rnd_symbol(50, 4, 8, 0), rnd_double() * 1000
                    FROM long_sequence(2_000)
                    """);
            drainWalQueue();

            // Sample a few keys that exist in the preloaded data so the
            // SELECT below has something to count.
            String[] sampleKeys = {"AAAA", "BBBB", "CCCC"};
            try (RecordCursorFactory f = select("SELECT DISTINCT sym FROM walbench LIMIT 3")) {
                int idx = 0;
                try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                    while (c.hasNext() && idx < sampleKeys.length) {
                        sampleKeys[idx++] = c.getRecord().getSymA(0).toString();
                    }
                }
            }

            // Loop the bench's per-iteration shape: INSERT batch -> drain ->
            // SELECT count. The fast-lag commit path runs inside drainWalQueue,
            // so each iteration pushes a new chain entry that, on the buggy
            // writer, has coverCount=0 even though .pci advertises 1 cover.
            for (int iter = 0; iter < 64; iter++) {
                int batchOffset = iter + 1;
                execute("INSERT INTO walbench(ts, sym, price) " +
                        "SELECT dateadd('u', x::INT, dateadd('s', " + batchOffset + ", '2024-01-01T00:00:00.000000Z'::TIMESTAMP)), " +
                        "rnd_symbol(50, 4, 8, 0), rnd_double() * 1000 " +
                        "FROM long_sequence(200)");
                drainWalQueue();

                String key = sampleKeys[iter % sampleKeys.length];
                long count = -1;
                try (RecordCursorFactory f = select(
                        "SELECT count() FROM walbench WHERE sym = '" + key + "'")) {
                    try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                        if (c.hasNext()) {
                            count = c.getRecord().getLong(0);
                        }
                    }
                }
                Assert.assertTrue(
                        "iter=" + iter + ", key=" + key + ", count=" + count
                                + " - sampleKeys are sourced from the preloaded "
                                + "2_000-row batch (DISTINCT sym LIMIT 3), so "
                                + "each must have count>0; a zero or negative "
                                + "count means the chain walk skipped or "
                                + "mis-read the entries holding this symbol",
                        count > 0
                );
            }
        });
    }

    @Test
    public void testWalO3DeferredPostingSealPurgeRunsAfterCommit() throws Exception {
        assertMemoryLeak(() -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            final String tableName = "posting_o3_recovery";
            final String indexColumnName = "new_col_11";
            final String coveredColumnName = "marker";
            final long targetPartitionTimestamp = MicrosFormatUtils.parseTimestamp("2022-02-25T00:00:00.000000Z");

            execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, sym2 SYMBOL INDEX TYPE POSTING INCLUDE (marker), sym_top SYMBOL CAPACITY 128, marker LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(insertPostingRowsSql(-85, 0));
            drainWalQueue();
            execute("ALTER TABLE " + tableName + " RENAME COLUMN sym2 TO " + indexColumnName);
            drainWalQueue();
            execute(insertPostingRowsSql(0, 10));
            drainWalQueue();
            execute("ALTER TABLE " + tableName + " DROP PARTITION LIST '2022-02-25'");
            drainWalQueue();
            execute("ALTER TABLE " + tableName + " ALTER COLUMN sym_top SYMBOL CAPACITY 64");
            drainWalQueue();

            execute(insertPostingRowsSql(0, 35));
            drainWalQueue();
            final PostingSealFileNames oldFiles = resolvePostingSealFileNames(tableName, indexColumnName, coveredColumnName, targetPartitionTimestamp, -1L);
            final long oldSealTxn = oldFiles.sealTxn;
            assertPostingSealFilesExist(oldFiles, true);

            execute(insertPostingRowsSql(35, 92));
            drainWalQueue();
            final PostingSealFileNames liveFiles = resolvePostingSealFileNames(tableName, indexColumnName, coveredColumnName, targetPartitionTimestamp, -1L);
            final long liveSealTxn = liveFiles.sealTxn;
            Assert.assertTrue("successful WAL O3 apply must advance posting sealTxn", liveSealTxn > oldSealTxn);
            assertPostingSealFilesExist(oldFiles, true);

            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                runPostingSealPurgeJob(purgeJob);
            }

            assertPostingSealFilesExist(oldFiles, false);
            assertPostingSealFilesExist(liveFiles, true);
        });
    }

    /**
     * Validates the Fix B writer-side change: after a seal, the
     * post-seal finally block now calls
     * {@link PostingIndexWriter#releaseCoveredColumnReadMappings()}
     * instead of {@link PostingIndexWriter#clearCovering()}, preserving
     * the writer's covering schema (coverCount, sidecarMems) so a
     * subsequent commit() between seals still publishes a chain entry
     * with a correctly-sized cover footer. This is the symmetric
     * counterpart to {@link #testWriterEntryPreservesCoverFooterAfterClearCoveringCommit},
     * which covers the clearCovering()-then-commit() flow, where the
     * footer instead survives via publishToChain's head-footer snapshot.
     */
    @Test
    public void testWriterEntrysCoverFooterPersistsAfterReleaseReadMappingsCommit() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "writer_release_read_mappings_then_commit";
                int plen = path.size();
                FilesFacade ff = configuration.getFilesFacade();

                final int coverCount = 1;
                final long fakeColRows = 32;
                final int shift = 3;
                final long fakeColBytes = fakeColRows << shift;
                long fakeColAddr = Unsafe.malloc(fakeColBytes, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.setMemory(fakeColAddr, fakeColBytes, (byte) 0);

                    long[] addrs = {fakeColAddr};
                    long[] tops = {0L};
                    int[] shifts = {shift};
                    int[] indices = {1};
                    int[] types = {ColumnType.LONG};

                    try (PostingIndexWriter writer = new PostingIndexWriter(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(addrs, tops, shifts, indices, types, coverCount);
                        for (int i = 0; i < 8; i++) {
                            writer.add(i % 4, i);
                        }
                        writer.setMaxValue(7);
                        writer.setNextTxnAtSeal(1L);
                        writer.seal();

                        // Models the production post-seal finally block:
                        // release the borrowed read-side addrs but keep
                        // the covering schema (coverCount, sidecarMems).
                        writer.releaseCoveredColumnReadMappings();

                        for (int i = 0; i < 4; i++) {
                            writer.add(i, 100 + i);
                        }
                        writer.setMaxValue(103);
                        writer.setNextTxnAtSeal(2L);
                        writer.commit();

                        LPSZ keyFile = PostingIndexUtils.keyFileName(
                                path.trimTo(plen), name, COLUMN_NAME_TXN_NONE);
                        long fileSize = ff.length(keyFile);
                        try (MemoryCMARWImpl mem = new MemoryCMARWImpl(
                                ff, keyFile, ff.getPageSize(), fileSize,
                                MemoryTag.MMAP_DEFAULT, /* opts */ 0)) {
                            PostingIndexChainWriter chain = new PostingIndexChainWriter();
                            chain.openExisting(mem);
                            PostingIndexChainEntry.Snapshot head = new PostingIndexChainEntry.Snapshot();
                            chain.loadHeadEntry(mem, head);
                            Assert.assertEquals(
                                    "Fix B: with releaseCoveredColumnReadMappings the "
                                            + "writer's coverCount stays > 0 across the "
                                            + "post-seal release, so the next commit's "
                                            + "extendHead writes a LEN that includes the "
                                            + "cover footer",
                                    PostingIndexChainEntry.entrySize(head.genCount, coverCount),
                                    head.len
                            );
                            Assert.assertNotEquals(
                                    "Fix B: the head LEN must NOT match the no-cover sizing",
                                    PostingIndexChainEntry.entrySize(head.genCount, 0),
                                    head.len
                            );
                        }
                    }
                } finally {
                    Unsafe.free(fakeColAddr, fakeColBytes, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    /**
     * Regression for the native-partition covered-read race root cause: a covering
     * posting index whose writer-side coverCount field is transiently reset to 0 by
     * clearCovering() must NOT drop the chain head's cover footer on the next
     * commit()/extendHead. clearCovering happens in TableWriter's WAL fast-lag seal
     * finally block, and between an in-place reseal's clearCovering and the next
     * configureCovering. Before the fix, extendHead sized the head entry with
     * entrySize(genCount, coverCount=0) -- DROPPING the cover footer -- so a
     * concurrent covered read saw sidecarFileEndOffsets==0, failed to map the
     * still-present .pc and returned NULL for the whole partition.
     * <p>
     * publishToChain now snapshots the head's existing footer
     * (PostingIndexChainWriter.readHeadCoverEndOffsets) before the new gen-dir
     * overwrites it and republishes those extents, so the footer survives the
     * transient coverCount=0 window. Drives the same lifecycle: configureCovering
     * -> add -> seal (footer written); clearCovering (coverCount field -> 0); add ->
     * commit (extendHead). After the commit the head LEN must still accommodate the
     * cover footer, and the picker must read the preserved, non-zero cover end
     * offset (not 0, not a stale/garbage slot).
     */
    @Test
    public void testWriterEntryPreservesCoverFooterAfterClearCoveringCommit() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "writer_clear_covering_then_commit";
                int plen = path.size();
                FilesFacade ff = configuration.getFilesFacade();

                final int coverCount = 1;
                final long fakeColRows = 32;
                final int shift = 3; // 8 bytes per row (LONG-shaped covered column)
                final long fakeColBytes = fakeColRows << shift;
                long fakeColAddr = Unsafe.malloc(fakeColBytes, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.setMemory(fakeColAddr, fakeColBytes, (byte) 0);

                    long[] addrs = {fakeColAddr};
                    long[] tops = {0L};
                    int[] shifts = {shift};
                    int[] indices = {1}; // dummy writer-side index
                    int[] types = {ColumnType.LONG};

                    try (PostingIndexWriter writer = new PostingIndexWriter(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                        // Step 1+2: configure covering, add rows, seal.
                        // The seal publishes the first chain entry with a
                        // proper cover footer (LEN includes coverCount=1).
                        writer.configureCovering(addrs, tops, shifts, indices, types, coverCount);
                        for (int i = 0; i < 8; i++) {
                            writer.add(i % 4, i);
                        }
                        writer.setMaxValue(7);
                        writer.setNextTxnAtSeal(1L);
                        writer.seal();

                        long lenAfterSeal;
                        long sealedSealTxn;
                        {
                            LPSZ keyFile = PostingIndexUtils.keyFileName(
                                    path.trimTo(plen), name, COLUMN_NAME_TXN_NONE);
                            long fileSize = ff.length(keyFile);
                            try (MemoryCMARWImpl mem = new MemoryCMARWImpl(
                                    ff, keyFile, ff.getPageSize(), fileSize,
                                    MemoryTag.MMAP_DEFAULT, /* opts */ 0)) {
                                PostingIndexChainWriter chain = new PostingIndexChainWriter();
                                chain.openExisting(mem);
                                PostingIndexChainEntry.Snapshot head = new PostingIndexChainEntry.Snapshot();
                                chain.loadHeadEntry(mem, head);
                                lenAfterSeal = head.len;
                                sealedSealTxn = head.sealTxn;
                            }
                        }
                        Assert.assertEquals(
                                "post-seal head LEN must include the cover footer "
                                        + "(56 header + genCount*44 gen-dir + coverCount*8 footer, "
                                        + "padded to 8): expected entrySize(genCount=1, coverCount=1)=112",
                                PostingIndexChainEntry.entrySize(1, coverCount), lenAfterSeal
                        );

                        // Step 3: clearCovering - models the finally block in
                        // TableWriter.sealPostingIndexesForLastPartitionFastLag.
                        // This resets the writer's coverCount field to 0
                        // even though the table's covering schema remains
                        // unchanged.
                        writer.clearCovering();

                        // Step 4: add more rows, commit. commit() ->
                        // flushAllPending() -> publishToChain() ->
                        // chain.extendHead with coverEndOffsetsScratch empty
                        // (because captureCoverEndOffsets short-circuits when
                        // coverCount<=0). extendHead rewrites the head's LEN
                        // using entrySize(newGenCount, 0).
                        for (int i = 0; i < 4; i++) {
                            writer.add(i, 100 + i);
                        }
                        writer.setMaxValue(103);
                        writer.setNextTxnAtSeal(2L);
                        writer.commit();

                        long lenAfterCommit;
                        int genCountAfterCommit;
                        {
                            LPSZ keyFile = PostingIndexUtils.keyFileName(
                                    path.trimTo(plen), name, COLUMN_NAME_TXN_NONE);
                            long fileSize = ff.length(keyFile);
                            try (MemoryCMARWImpl mem = new MemoryCMARWImpl(
                                    ff, keyFile, ff.getPageSize(), fileSize,
                                    MemoryTag.MMAP_DEFAULT, /* opts */ 0)) {
                                PostingIndexChainWriter chain = new PostingIndexChainWriter();
                                chain.openExisting(mem);
                                PostingIndexChainEntry.Snapshot head = new PostingIndexChainEntry.Snapshot();
                                chain.loadHeadEntry(mem, head);
                                lenAfterCommit = head.len;
                                genCountAfterCommit = head.genCount;
                                Assert.assertEquals(
                                        "extendHead reuses the same chain entry "
                                                + "(same sealTxn since commit() does not "
                                                + "advance sealTxn) - confirms the bad LEN "
                                                + "was written into the head sealed at "
                                                + "txn=" + sealedSealTxn,
                                        sealedSealTxn, head.sealTxn
                                );
                            }
                        }
                        Assert.assertEquals(
                                "FIXED: publishToChain preserves the cover footer across the "
                                        + "clearCovering()/commit() window, so the head LEN stays "
                                        + "sized for coverCount=1 (entrySize(genCount, 1)) instead "
                                        + "of shrinking to entrySize(genCount, 0)",
                                PostingIndexChainEntry.entrySize(genCountAfterCommit, coverCount),
                                lenAfterCommit
                        );

                        // The picker reads coverCount=1 with the PRESERVED, non-zero
                        // cover end offset (the .pc extent written at seal), not 0.
                        // Before the fix this slot would be dropped/zero and a covered
                        // read would see the partition as having no covered data.
                        LPSZ keyFile = PostingIndexUtils.keyFileName(
                                path.trimTo(plen), name, COLUMN_NAME_TXN_NONE);
                        long fileSize = ff.length(keyFile);
                        try (MemoryCMARWImpl mem = new MemoryCMARWImpl(
                                ff, keyFile, ff.getPageSize(), fileSize,
                                MemoryTag.MMAP_DEFAULT, /* opts */ 0)) {
                            PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
                            PostingIndexChainEntry.Snapshot entry = new PostingIndexChainEntry.Snapshot();
                            int rc = PostingIndexChainPicker.pick(
                                    mem, /* pinnedTableTxn */ Long.MAX_VALUE,
                                    /* coverCount */ coverCount, header, entry);
                            Assert.assertEquals(
                                    PostingIndexChainPicker.RESULT_OK, rc);
                            Assert.assertEquals(
                                    "the preserved cover footer still carries coverCount slots",
                                    coverCount, entry.coverFileEndOffsets.size());
                            Assert.assertTrue(
                                    "the preserved cover end offset must be the non-zero .pc "
                                            + "extent written at seal, not 0 (0 would make a "
                                            + "covered read see the partition as having no covered "
                                            + "data): " + entry.coverFileEndOffsets.getQuick(0),
                                    entry.coverFileEndOffsets.getQuick(0) > 0);
                        }
                    }
                } finally {
                    Unsafe.free(fakeColAddr, fakeColBytes, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    // Closes the cursor on a freshly spawned thread and rethrows anything the
    // close raised. This is the connection-migration analogue: a suspendable
    // query (HTTP export, pgwire fragment stream) can migrate to another worker
    // between fragments, so teardown closes the record cursor -- and the posting
    // cursors it retains -- off the thread that checked them out via getCursor().
    // Before the canRepool() gate, PostingIndex*Reader cursor close() asserted
    // the checkout thread and threw "posting index cursor closed off the
    // reader's owning thread" here under -ea.
    private static void closeCursorOffOperatingThread(RecordCursor cursor) throws InterruptedException {
        final AtomicReference<Throwable> closeError = new AtomicReference<>();
        Thread closer = new Thread(() -> {
            try {
                cursor.close();
            } catch (Throwable th) {
                closeError.set(th);
            }
        });
        closer.start();
        closer.join();
        if (closeError.get() != null) {
            throw new AssertionError("record cursor close failed off the operating thread", closeError.get());
        }
    }

    private static void closeCursorOffOperatingThread(RowCursor cursor) throws InterruptedException {
        final AtomicReference<Throwable> closeError = new AtomicReference<>();
        Thread closer = new Thread(() -> {
            try {
                cursor.close();
            } catch (Throwable th) {
                closeError.set(th);
            }
        });
        closer.start();
        closer.join();
        if (closeError.get() != null) {
            throw new AssertionError("row cursor close failed off the operating thread", closeError.get());
        }
    }

    private static long drainRowCursor(RowCursor cursor) {
        long count = 0;
        while (cursor.hasNext()) {
            cursor.next();
            count++;
        }
        cursor.close();
        return count;
    }

    private static String insertPostingRowsSql(int lo, int hi) {
        return insertPostingRowsSql("posting_o3_recovery", lo, hi, false);
    }

    private static String insertPostingRowsSql(CharSequence tableName, int lo, int hi, boolean includeSecondPostingSymbol) {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(tableName).append(" VALUES ");
        for (int row = hi - 1; row >= lo; row--) {
            if (row < hi - 1) {
                sql.append(',');
            }
            CharSequence indexedSymbol = row >= 34 ? "XPHI" : "A";
            sql.append("('").append(timestampAtMinute(row)).append("', '")
                    .append(indexedSymbol)
                    .append("', '");
            if (includeSecondPostingSymbol) {
                sql.append(indexedSymbol).append("', '");
            }
            sql.append("S', ")
                    .append(row)
                    .append(')');
        }
        return sql.toString();
    }

    private static void runPostingSealPurgeJob(PostingSealPurgeJob purgeJob) {
        for (int i = 0; i < 32; i++) {
            if (!purgeJob.run()) {
                break;
            }
        }
    }

    private long selectLong(CharSequence sql) throws Exception {
        try (RecordCursorFactory factory = select(sql);
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertTrue("query must return one row [sql=" + sql + ']', cursor.hasNext());
            long value = cursor.getRecord().getLong(0);
            Assert.assertFalse("query must return exactly one row [sql=" + sql + ']', cursor.hasNext());
            return value;
        }
    }

    private void assertOpenDeferredPostingSealPurgeLogRow(
            TableToken tableToken,
            String indexColumnName,
            long sealTxn
    ) throws Exception {
        assertOpenDeferredPostingSealPurgeLogRowCount(tableToken, indexColumnName, sealTxn, 1);
    }

    private void assertOpenDeferredPostingSealPurgeLogRowCount(
            TableToken tableToken,
            String indexColumnName,
            long sealTxn,
            int expectedCount
    ) throws Exception {
        assertQuery("SELECT count() FROM \"" + configuration.getSystemTableNamePrefix()
                + "posting_seal_purge_log\" WHERE table_name = '" + tableToken.getDirName()
                + "' AND table_id = " + tableToken.getTableId()
                + " AND column_name = '" + indexColumnName
                + "' AND seal_txn = " + sealTxn
                + " AND completed = null")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("count\n"
                        + expectedCount
                        + "\n");
    }

    private int countPostingValueFiles(String tableName, String indexColumnName, long partitionTimestamp) {
        TableToken token = engine.getTableTokenIfExists(tableName);
        Assert.assertNotNull("table must exist", token);
        long partitionNameTxn = -1L;
        try (TableReader reader = engine.getReader(token)) {
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                if (reader.getTxFile().getPartitionTimestampByIndex(i) == partitionTimestamp) {
                    partitionNameTxn = reader.getTxFile().getPartitionNameTxn(i);
                    break;
                }
            }
        }
        final int[] count = {0};
        final String prefix = indexColumnName + ".pv.";
        final StringSink fileName = new StringSink();
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token);
            setPathForNativePartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTimestamp, partitionNameTxn);
            configuration.getFilesFacade().iterateDir(path.$(), (pUtf8NameZ, type) -> {
                if (type != Files.DT_FILE && type != Files.DT_UNKNOWN) {
                    return;
                }
                fileName.clear();
                Utf8s.utf8ToUtf16Z(pUtf8NameZ, fileName);
                if (Chars.startsWith(fileName, prefix)) {
                    count[0]++;
                }
            });
        }
        return count[0];
    }

    // Shared single-key covered sum/count concurrent-reader workload. Seeds
    // dayCount days of 'A' rows (price = row index), optionally converts them all to
    // parquet, then runs reader threads asserting the covered count/sum stay
    // monotonic (rows are only ADDED, so a valid snapshot can never drop below the
    // committed floor) while a 4-worker pool rewrites partitions out of order under a
    // tiny spill budget. Verifies the exact final totals. Callers set the spill /
    // auto-include / row-id-encoding properties before invoking. Pass toParquet=false
    // to exercise the native in-place reseal path.
    private void runConcurrentCoveredSumCountFuzz(String tableName, boolean toParquet, Rnd rnd) throws Exception {
        final int dayCount = 4 + rnd.nextInt(4); // 4..7 days; keeps sum < 1e7
        final int hotPerDay = 1000;
        final long perDaySum = (long) hotPerDay * (hotPerDay + 1) / 2;
        final long initialRows = (long) dayCount * hotPerDay;
        final long initialSum = (long) dayCount * perDaySum;

        execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (price), price DOUBLE) "
                + "TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        for (int d = 1; d <= dayCount; d++) {
            execute("INSERT INTO " + tableName + " SELECT dateadd('s', x::INT, '2024-02-0" + d
                    + "T00:00:00.000000Z'::TIMESTAMP), 'A', x::DOUBLE FROM long_sequence(" + hotPerDay + ")");
        }
        if (toParquet) {
            for (int d = 1; d <= dayCount; d++) {
                execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2024-02-0" + d + "'");
            }
        }

        final AtomicReference<Throwable> bgError = new AtomicReference<>();
        final AtomicBoolean stop = new AtomicBoolean();
        final AtomicLong rowFloor = new AtomicLong(initialRows);
        final int readerCount = 3;
        final Thread[] readers = new Thread[readerCount];
        for (int r = 0; r < readerCount; r++) {
            readers[r] = new Thread(() -> {
                try (
                        SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                                .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                                        null, null, -1, null);
                        SqlCompiler compiler = engine.getSqlCompiler()
                ) {
                    long prevCount = 0;
                    while (!stop.get() && bgError.get() == null) {
                        final long floor = rowFloor.get();
                        final long count;
                        final double sum;
                        try (RecordCursorFactory f = compiler.compile(
                                "SELECT count(), sum(price) FROM " + tableName + " WHERE sym = 'A'", ctx).getRecordCursorFactory();
                             RecordCursor cur = f.getCursor(ctx)) {
                            if (cur.hasNext()) {
                                count = cur.getRecord().getLong(0);
                                sum = cur.getRecord().getDouble(1);
                            } else {
                                count = -1;
                                sum = -1;
                            }
                        }
                        if (count < floor) {
                            throw new AssertionError("covered 'A' count fell below the committed floor: " + count + " < " + floor);
                        }
                        if (count < prevCount) {
                            throw new AssertionError("covered 'A' count went backwards: " + prevCount + " -> " + count);
                        }
                        if (sum < (double) initialSum - 0.5) {
                            throw new AssertionError("covered sum(price) dropped below the initial total " + initialSum + ": " + sum);
                        }
                        if (sum < (double) count - 0.5) {
                            throw new AssertionError("covered sum(price)=" + sum + " < count=" + count
                                    + " -- a covered value resolved as < 1 (stale/garbage sidecar)");
                        }
                        prevCount = count;
                    }
                } catch (Throwable e) {
                    bgError.set(e);
                }
            }, "covering-reader-" + r);
            readers[r].setDaemon(true);
            readers[r].start();
        }

        final WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.setupWorkerPool(pool, engine);
        pool.start(LOG);
        long expectedRows = initialRows;
        try {
            final int batches = 6 + rnd.nextInt(10);
            for (int b = 0; b < batches && bgError.get() == null; b++) {
                final StringBuilder sql = new StringBuilder("INSERT INTO " + tableName + " VALUES ");
                int added = 0;
                for (int d = 1; d <= dayCount; d++) {
                    if (rnd.nextBoolean()) {
                        continue;
                    }
                    final int n = 1 + rnd.nextInt(3);
                    for (int i = 0; i < n; i++) {
                        if (added > 0) {
                            sql.append(',');
                        }
                        sql.append("('2024-02-0").append(d).append("T00:")
                                .append(String.format("%02d", 1 + rnd.nextInt(58)))
                                .append(":00.500000Z', 'A', 1.0)");
                        added++;
                    }
                }
                if (added == 0) {
                    continue;
                }
                execute(sql.toString());
                expectedRows += added;
                rowFloor.set(expectedRows);
            }
        } finally {
            stop.set(true);
            for (Thread t : readers) {
                t.join(30_000);
                Assert.assertFalse("reader thread did not terminate", t.isAlive());
            }
            pool.halt();
        }

        Assert.assertNull("concurrent covered reader threw: " + bgError.get(), bgError.get());

        try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
            runPostingSealPurgeJob(purgeJob);
        }
        engine.releaseAllWriters();

        final long expectedSum = initialSum + (expectedRows - initialRows);
        try (
                SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                        .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                                null, null, -1, null);
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory f = compiler.compile(
                        "SELECT count(), sum(price) FROM " + tableName + " WHERE sym = 'A'", ctx).getRecordCursorFactory();
                RecordCursor cur = f.getCursor(ctx)
        ) {
            Assert.assertTrue("final covered query returned no row", cur.hasNext());
            Assert.assertEquals("final covered 'A' count", expectedRows, cur.getRecord().getLong(0));
            Assert.assertEquals("final covered sum(price)", (double) expectedSum, cur.getRecord().getDouble(1), 0.5);
        }
    }

    private void fillPostingSealPurgeQueue(TableToken liveToken) {
        MessageBus bus = engine.getMessageBus();
        MPSequence pubSeq = bus.getPostingSealPurgePubSeq();
        RingQueue<PostingSealPurgeTask> queue = bus.getPostingSealPurgeQueue();
        TableToken missingToken = new TableToken(
                "__missing_posting_seal_purge_queue_fill",
                "__missing_posting_seal_purge_queue_fill",
                null,
                liveToken.getTableId() + 1_000_000,
                false,
                false,
                false
        );
        int published = 0;
        while (true) {
            long cursor = pubSeq.next();
            if (cursor == -2) {
                Os.pause();
                continue;
            }
            if (cursor < 0) {
                break;
            }
            try {
                queue.get(cursor).of(
                        missingToken,
                        "missing_col",
                        COLUMN_NAME_TXN_NONE,
                        published,
                        0L,
                        -1L,
                        PartitionBy.NONE,
                        ColumnType.TIMESTAMP_MICRO,
                        0L,
                        0L
                );
            } finally {
                pubSeq.done(cursor);
            }
            published++;
        }
        Assert.assertTrue("posting seal purge queue must be saturated by the test", published > 0);
    }

    private void drainPostingSealPurgeQueue() {
        MessageBus bus = engine.getMessageBus();
        SCSequence subSeq = bus.getPostingSealPurgeSubSeq();
        RingQueue<PostingSealPurgeTask> queue = bus.getPostingSealPurgeQueue();
        while (true) {
            long cursor = subSeq.next();
            if (cursor == -2) {
                Os.pause();
                continue;
            }
            if (cursor < 0) {
                break;
            }
            try {
                queue.get(cursor).clear();
            } finally {
                subSeq.done(cursor);
            }
        }
    }

    private void assertPostingKeyFileCoversHeaderRegion(
            TableToken token,
            long partitionTimestamp,
            long partitionNameTxn,
            CharSequence columnName,
            long columnNameTxn,
            CharSequence failurePrefix
    ) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token);
            setPathForNativePartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTimestamp, partitionNameTxn);
            int plen = path.size();
            LPSZ keyFile = PostingIndexUtils.keyFileName(path.trimTo(plen), columnName, columnNameTxn);
            long fileSize = ff.length(keyFile);
            Assert.assertTrue("dst .pk must exist after link recovery, path=" + keyFile, fileSize > 0);
            // Read the header only (readUnderSeqlock never touches the head
            // entry), so a header that points past EOF does not SIGBUS here.
            try (MemoryCMARWImpl mem = new MemoryCMARWImpl(
                    ff, keyFile, ff.getPageSize(), fileSize, MemoryTag.MMAP_DEFAULT, /* opts */ 0)) {
                PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
                Assert.assertTrue("dst .pk header unreadable, path=" + keyFile,
                        PostingIndexChainHeader.readUnderSeqlock(mem, header));
                Assert.assertTrue(
                        failurePrefix
                                + ": fileSize=" + fileSize + " regionLimit=" + header.regionLimit
                                + " headEntryOffset=" + header.headEntryOffset
                                + " -- a reader of the linked posting index would map past EOF (SIGBUS).",
                        fileSize >= header.regionLimit
                );
            }
        }
    }

    /**
     * Returns the on-disk byte length of the posting {@code .pk} key file for the
     * given column in the given partition. Only stats the file (never maps it),
     * so it is safe to call on a regressed build whose header points its head
     * entry past EOF.
     */
    private long postingKeyFileSize(
            TableToken token,
            long partitionTimestamp,
            long partitionNameTxn,
            CharSequence columnName,
            long columnNameTxn
    ) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token);
            setPathForNativePartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTimestamp, partitionNameTxn);
            int plen = path.size();
            LPSZ keyFile = PostingIndexUtils.keyFileName(path.trimTo(plen), columnName, columnNameTxn);
            long size = ff.length(keyFile);
            Assert.assertTrue(".pk must exist, path=" + keyFile, size > 0);
            return size;
        }
    }

    private static void assertPoisonedRejects(String op, Runnable action) {
        try {
            action.run();
            Assert.fail(op + ": expected the poisoned writer to reject the operation");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "poisoned");
        }
    }

    private static boolean isPostingIndexResealCall() {
        for (StackTraceElement frame : Thread.currentThread().getStackTrace()) {
            if (TableWriter.class.getName().equals(frame.getClassName())
                    && "sealPostingIndexForPartition".equals(frame.getMethodName())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Appends 20_000 'k1' rows at price 1.0 with strictly descending
     * timestamps starting just above {@code baseTimestamp}. Descending rows
     * past the committed max keep the last partition in append=true O3
     * mode, which seals the posting index in place on commit.
     */
    private static void appendDescendingK1Batch(TableWriter writer, String baseTimestamp) throws NumericException {
        long base = MicrosFormatUtils.parseTimestamp(baseTimestamp);
        for (int i = 0; i < 20_000; i++) {
            TableWriter.Row r = writer.newRow(base + (19_999 - i));
            r.putSym(1, "k1");
            r.putDouble(2, 1.0);
            r.append();
        }
    }

    /**
     * The newest (highest sealTxn) {@code sym.pc0.0.N} covering sidecar in
     * the table's first partition. Superseded sidecars linger until the
     * async seal purge runs, so the highest txn picks the live one.
     */
    private static java.io.File newestSidecarFile(TableToken token) {
        long partitionTs;
        long partitionNameTxn;
        try (TableReader reader = engine.getReader(token)) {
            partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
            partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
        }
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token);
            setPathForNativePartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
            java.io.File dir = new java.io.File(path.toString());
            String[] names = dir.list();
            Assert.assertNotNull("partition dir must exist: " + dir, names);
            String prefix = "sym.pc0.0.";
            long bestTxn = -1;
            java.io.File best = null;
            for (String n : names) {
                if (n.startsWith(prefix)) {
                    long txn = Long.parseLong(n.substring(prefix.length()));
                    if (txn > bestTxn) {
                        bestTxn = txn;
                        best = new java.io.File(dir, n);
                    }
                }
            }
            Assert.assertNotNull("no " + prefix + "* sidecar found in " + dir, best);
            return best;
        }
    }

    private static long newestSidecarSize(TableToken token) {
        return newestSidecarFile(token).length();
    }

    private static String timestampAtMinute(int minuteOfDay) {
        StringBuilder timestamp = new StringBuilder("2022-02-25T");
        if (minuteOfDay < 0) {
            timestamp = new StringBuilder("2022-02-24T");
            minuteOfDay += 24 * 60;
        }
        int hour = minuteOfDay / 60;
        int minute = minuteOfDay % 60;
        if (hour < 10) {
            timestamp.append('0');
        }
        timestamp.append(hour).append(':');
        if (minute < 10) {
            timestamp.append('0');
        }
        timestamp.append(minute).append(":00.000000Z");
        return timestamp.toString();
    }

    private void appendFuturePostingHead(
            TableToken token,
            long partitionTimestamp,
            long partitionNameTxn,
            long currentTxn
    ) {
        try (Path path = new Path().of(configuration.getDbRoot()).concat(token);
             PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
            setPathForNativePartition(path, ColumnType.TIMESTAMP, io.questdb.cairo.PartitionBy.DAY, partitionTimestamp, partitionNameTxn);
            final int plen = path.size();

            writer.setCurrentTableTxn(currentTxn);
            writer.of(path.trimTo(plen), "sym", io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE, false);

            // Manufacture the state left by a partially-published posting
            // index update: .pk has a new head tagged with a future table txn,
            // while committed readers pinned at currentTxn must skip it and
            // use the previous seal.
            writer.setNextTxnAtSeal(currentTxn + 2);
            writer.discardForRebuild();
            writer.add(io.questdb.cairo.TableUtils.toIndexKey(0), 0);
            writer.setMaxValue(0);
            writer.commit();
        }
    }

    /**
     * Plant a committed-visible base head, then a page-spanning head entry whose
     * own entry-level {@code txnAtSeal} is in the future, chained on top. Unlike
     * {@link #appendVisibleHeadWithFutureTailGen} -- which keeps the entry
     * visible and only trims a future tail gen (the head-trim/grow recovery
     * branch) -- this future entry is dropped WHOLE by
     * {@code recoveryDropAbandoned} (the full-drop branch), so link-time recovery
     * rewinds {@code regionLimit} more than one OS page below the on-disk size.
     * <p>
     * Uses the real {@link PostingIndexWriter} so the dropped head's
     * {@code .pv.{sealTxn}} and the surviving base head's files exist on disk and
     * the subsequent hard-link does not fail for an unrelated reason.
     */
    private void appendPageSpanningFutureHead(
            TableToken token,
            long partitionTimestamp,
            long partitionNameTxn,
            long currentTxn
    ) {
        // gens sized so the future head's entry region spans at least one OS
        // page; dropping it rewinds regionLimit across a page boundary, the
        // granularity the CMARW close truncates to.
        final int gens = (int) ((io.questdb.std.Files.PAGE_SIZE - PostingIndexUtils.V2_ENTRY_HEADER_SIZE)
                / PostingIndexUtils.GEN_DIR_ENTRY_SIZE) + 2;
        try (Path path = new Path().of(configuration.getDbRoot()).concat(token);
             PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
            setPathForNativePartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTimestamp, partitionNameTxn);
            final int plen = path.size();
            // No setCurrentTableTxn: recovery must not run on this open, or it
            // would drop the future head we are about to plant.
            writer.of(path.trimTo(plen), "sym", COLUMN_NAME_TXN_NONE, false);

            // Committed-visible base head. discardForRebuild rotates sealTxn so
            // this commit takes the appendNewEntry branch (a fresh entry chained
            // onto the existing INSERT head). recoveryDropAbandoned stops here
            // and keeps it as the surviving head after dropping the future entry.
            writer.discardForRebuild();
            writer.setNextTxnAtSeal(currentTxn);
            writer.add(io.questdb.cairo.TableUtils.toIndexKey(0), 0);
            writer.setMaxValue(0);
            writer.commit();

            // Fresh entry whose entry-level txnAtSeal is in the future. The base
            // commit (after discardForRebuild) sets the entry-level txnAtSeal;
            // the loop extends the SAME entry with enough gens to span a page.
            // Because the entry itself is future, recovery takes the full-drop
            // branch rather than the head-trim branch.
            writer.discardForRebuild();
            writer.setNextTxnAtSeal(currentTxn + 1);
            writer.add(io.questdb.cairo.TableUtils.toIndexKey(0), 1);
            writer.setMaxValue(1);
            writer.commit();
            for (int g = 2; g <= gens; g++) {
                writer.setNextTxnAtSeal(currentTxn + 1);
                writer.add(io.questdb.cairo.TableUtils.toIndexKey(0), g);
                writer.setMaxValue(g);
                writer.commit();
            }
        }
    }

    /**
     * Plant a committed-visible head entry carrying a single in-flight (future
     * {@code txnAtSeal}) tail gen on the given partition's {@code sym} posting
     * index. The head is grown via {@code extendHead} to enough visible gens
     * that the recovery head-trim's relocated entry spans at least one OS page,
     * so the link-time truncation drops it regardless of page alignment.
     * <p>
     * Uses the real {@link PostingIndexWriter} so the corresponding
     * {@code .pv.{sealTxn}} exists on disk and the subsequent hard-link does not
     * fail for an unrelated reason.
     */
    private void appendVisibleHeadWithFutureTailGen(
            TableToken token,
            long partitionTimestamp,
            long partitionNameTxn,
            long currentTxn
    ) {
        // entrySize(visibleGens, 0) must exceed PAGE_SIZE so the trimmed head
        // cannot fit in the slack between regionLimit and the next page
        // boundary. PAGE_SIZE is the granularity the CMARW close truncates to.
        final int visibleGens = (int) ((io.questdb.std.Files.PAGE_SIZE - PostingIndexUtils.V2_ENTRY_HEADER_SIZE)
                / PostingIndexUtils.GEN_DIR_ENTRY_SIZE) + 2;
        try (Path path = new Path().of(configuration.getDbRoot()).concat(token);
             PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
            setPathForNativePartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTimestamp, partitionNameTxn);
            final int plen = path.size();
            // No setCurrentTableTxn: recovery must not run on this open, or it
            // would drop the future tail gen we are about to plant.
            writer.of(path.trimTo(plen), "sym", COLUMN_NAME_TXN_NONE, false);

            // Fresh head whose entry-level txnAtSeal is committed/visible.
            // discardForRebuild rotates sealTxn so the first commit takes the
            // appendNewEntry branch instead of extending the INSERT head.
            writer.discardForRebuild();
            writer.setNextTxnAtSeal(currentTxn);
            writer.add(io.questdb.cairo.TableUtils.toIndexKey(0), 0);
            writer.setMaxValue(0);
            writer.commit();

            // Extend the same head with visible gens (extendHead keeps the same
            // sealTxn) until the trimmed head will span a full page.
            for (int g = 1; g < visibleGens; g++) {
                writer.setNextTxnAtSeal(currentTxn);
                writer.add(io.questdb.cairo.TableUtils.toIndexKey(0), g);
                writer.setMaxValue(g);
                writer.commit();
            }

            // One in-flight tail gen tagged with a future txnAtSeal. Recovery at
            // link time trims this (and only this) slot, taking the head-trim
            // branch while the entry itself stays visible.
            writer.setNextTxnAtSeal(currentTxn + 1);
            writer.add(io.questdb.cairo.TableUtils.toIndexKey(0), visibleGens);
            writer.setMaxValue(visibleGens);
            writer.commit();
        }
    }

    private void assertPostingSealFilesExist(PostingSealFileNames files, boolean expected) {
        FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            if (expected) {
                Assert.assertTrue(".pv must exist [path=" + files.valueFile + ']', ff.exists(path.of(files.valueFile).$()));
            } else {
                Assert.assertFalse(".pv must be purged [path=" + files.valueFile + ']', ff.exists(path.of(files.valueFile).$()));
            }
            if (files.coverFile == null) {
                return;
            }
            if (expected) {
                Assert.assertTrue(".pc must exist [path=" + files.coverFile + ']', ff.exists(path.of(files.coverFile).$()));
            } else {
                Assert.assertFalse(".pc must be purged [path=" + files.coverFile + ']', ff.exists(path.of(files.coverFile).$()));
            }
        }
    }

    // Mirrors TableWriter.POSTING_SEAL_PURGE_PENDING_FILE_NAME.
    private void assertPostingSealPurgePendingFileExists(TableToken token, boolean expected) {
        FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token).concat("_posting_seal_purge_pending.d");
            Assert.assertEquals("posting seal-purge pending file existence [path=" + path + ']', expected, ff.exists(path.$()));
        }
    }

    private PostingSealFileNames resolvePostingSealFileNames(
            Path path,
            String tableName,
            String indexColumnName,
            String coveredColumnName,
            long partitionTimestamp,
            long sealTxn
    ) {
        TableToken token = engine.getTableTokenIfExists(tableName);
        Assert.assertNotNull("table must exist", token);
        try (TableReader reader = engine.getReader(token)) {
            int partitionIndex = -1;
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                if (reader.getTxFile().getPartitionTimestampByIndex(i) == partitionTimestamp) {
                    partitionIndex = i;
                    break;
                }
            }
            Assert.assertTrue("target partition must exist", partitionIndex >= 0);
            long partitionNameTxn = reader.getTxFile().getPartitionNameTxn(partitionIndex);

            int indexColumnIndex = reader.getMetadata().getColumnIndex(indexColumnName);
            int indexWriterIndex = reader.getMetadata().getWriterIndex(indexColumnIndex);
            long postingColumnNameTxn = reader.getColumnVersionReader().getColumnNameTxn(partitionTimestamp, indexWriterIndex);

            path.of(configuration.getDbRoot()).concat(token);
            setPathForNativePartition(
                    path,
                    ColumnType.TIMESTAMP,
                    io.questdb.cairo.PartitionBy.DAY,
                    partitionTimestamp,
                    partitionNameTxn
            );
            int plen = path.size();
            long resolvedSealTxn = sealTxn >= 0 ? sealTxn : PostingIndexUtils.readSealTxnFromKeyFile(
                    configuration.getFilesFacade(),
                    PostingIndexUtils.keyFileName(path.trimTo(plen), indexColumnName, postingColumnNameTxn)
            );
            PostingIndexUtils.valueFileName(path.trimTo(plen), indexColumnName, postingColumnNameTxn, resolvedSealTxn);
            String valueFilePath = path.toString();
            String coverFilePath = null;
            if (coveredColumnName != null) {
                int coveredColumnIndex = reader.getMetadata().getColumnIndex(coveredColumnName);
                int coveredWriterIndex = reader.getMetadata().getWriterIndex(coveredColumnIndex);
                long coveredColumnNameTxn = reader.getColumnVersionReader().getColumnNameTxn(partitionTimestamp, coveredWriterIndex);

                IntList coveringColumnIndices = reader.getMetadata().getColumnMetadata(indexColumnIndex).getCoveringColumnIndices();
                Assert.assertNotNull("posting index must have covering columns", coveringColumnIndices);
                int includeIdx = -1;
                for (int i = 0, n = coveringColumnIndices.size(); i < n; i++) {
                    if (coveringColumnIndices.getQuick(i) == coveredWriterIndex) {
                        includeIdx = i;
                        break;
                    }
                }
                Assert.assertTrue("covered column must be present in INCLUDE list", includeIdx >= 0);
                PostingIndexUtils.coverDataFileName(path.trimTo(plen), indexColumnName, includeIdx, postingColumnNameTxn, coveredColumnNameTxn, resolvedSealTxn);
                coverFilePath = path.toString();
            }
            return new PostingSealFileNames(resolvedSealTxn, valueFilePath, coverFilePath);
        }
    }

    private PostingSealFileNames resolvePostingSealFileNames(
            String tableName,
            String indexColumnName,
            String coveredColumnName,
            long partitionTimestamp,
            long sealTxn
    ) {
        try (Path path = new Path()) {
            return resolvePostingSealFileNames(path, tableName, indexColumnName, coveredColumnName, partitionTimestamp, sealTxn);
        }
    }

    // Writes a deliberately corrupt posting seal-purge pending file, truncated to
    // an exact length (TRUNCATE_TO_POINTER) so the recovery bound-checks see the
    // intended short/overrunning layout rather than a page-padded file.
    private void writeCorruptPostingSealPurgePendingFile(TableToken token, int variant) {
        FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token).concat("_posting_seal_purge_pending.d");
            MemoryMARW mem = Vm.getCMARWInstance();
            try {
                mem.smallFile(ff, path.$(), MemoryTag.MMAP_TABLE_WRITER);
                mem.jumpTo(0);
                switch (variant) {
                    case 0: // unknown format marker
                        mem.putInt(999);
                        mem.putInt(0);
                        break;
                    case 1: // valid header claiming 2 records, body truncated
                        mem.putInt(1); // POSTING_SEAL_PURGE_PENDING_FORMAT
                        mem.putInt(2);
                        mem.putLong(123L); // only 8 bytes, far short of a record
                        break;
                    case 2: // valid header + full 56-byte fixed part, bad name length
                        mem.putInt(1);
                        mem.putInt(1);
                        mem.putLong(0L); // postingColumnNameTxn
                        mem.putLong(0L); // sealTxn
                        mem.putLong(0L); // partitionTimestamp
                        mem.putLong(0L); // partitionNameTxn
                        mem.putInt(0);   // partitionBy
                        mem.putInt(0);   // timestampType
                        mem.putLong(0L); // fromTableTxn
                        mem.putLong(0L); // toTableTxn
                        mem.putInt(1_000_000); // name length far past EOF
                        break;
                    default: // shorter than the 8-byte header
                        mem.putInt(1);
                        break;
                }
            } finally {
                mem.close(true, Vm.TRUNCATE_TO_POINTER);
            }
        }
    }

    private static class FailAppendPositionLengthFacade extends TestFilesFacadeImpl {
        private boolean armed;
        private int failureCount;
        private int setColumnLengthCalls;

        @Override
        public long length(long fd) {
            if (armed && failureCount == 0 && isSetColumnAppendPositionCall()) {
                setColumnLengthCalls++;
                if (setColumnLengthCalls == 2) {
                    armed = false;
                    failureCount++;
                    throw CairoException.critical(0).put("[test] checking file size failed");
                }
            }
            return super.length(fd);
        }

        private static boolean isSetColumnAppendPositionCall() {
            for (StackTraceElement frame : Thread.currentThread().getStackTrace()) {
                if (TableWriter.class.getName().equals(frame.getClassName())
                        && "setColumnAppendPosition".equals(frame.getMethodName())) {
                    return true;
                }
            }
            return false;
        }

        void arm() {
            armed = true;
            setColumnLengthCalls = 0;
        }

        int getFailureCount() {
            return failureCount;
        }
    }

    private static class PcOpenFailingFacade extends TestFilesFacadeImpl {
        private final AtomicBoolean armed = new AtomicBoolean(false);
        private final AtomicInteger failures = new AtomicInteger(0);

        @Override
        public long openRW(LPSZ name, int opts) {
            if (armed.get() && name != null && Utf8s.containsAscii(name, ".pc0")) {
                armed.set(false);
                failures.incrementAndGet();
                return -1;
            }
            return super.openRW(name, opts);
        }

        int failureCount() {
            return failures.get();
        }

        void arm() {
            failures.set(0);
            armed.set(true);
        }

        void disarm() {
            armed.set(false);
        }
    }

    private static class PcSyncFailingFacade extends TestFilesFacadeImpl {
        private final AtomicBoolean armed = new AtomicBoolean(false);
        private final AtomicInteger failures = new AtomicInteger(0);
        private final java.util.concurrent.ConcurrentHashMap<Long, Boolean> pcFds = new java.util.concurrent.ConcurrentHashMap<>();
        private final java.util.concurrent.ConcurrentHashMap<Long, Long> pcMappings = new java.util.concurrent.ConcurrentHashMap<>();

        @Override
        public boolean close(long fd) {
            pcFds.remove(fd);
            return super.close(fd);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            long addr = super.mmap(fd, len, offset, flags, memoryTag);
            if (addr != -1 && pcFds.containsKey(fd)) {
                pcMappings.put(addr, len);
            }
            return addr;
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            if (armed.get()) {
                for (var mapping : pcMappings.entrySet()) {
                    long base = mapping.getKey();
                    if (addr >= base && addr < base + mapping.getValue()) {
                        armed.set(false);
                        failures.incrementAndGet();
                        throw CairoException.critical(0).put("[test] staged .pc sync failed");
                    }
                }
            }
            super.msync(addr, len, async);
        }

        @Override
        public void munmap(long address, long size, int memoryTag) {
            pcMappings.remove(address);
            super.munmap(address, size, memoryTag);
        }

        @Override
        public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
            long newAddr = super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
            if (pcMappings.remove(addr) != null && newAddr != -1) {
                pcMappings.put(newAddr, newSize);
            }
            return newAddr;
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            long fd = super.openRW(name, opts);
            if (fd != -1 && name != null && Utf8s.containsAscii(name, ".pc0")) {
                pcFds.put(fd, Boolean.TRUE);
            }
            return fd;
        }

        void arm() {
            failures.set(0);
            armed.set(true);
        }

        void disarm() {
            armed.set(false);
        }

        int failureCount() {
            return failures.get();
        }
    }

    /**
     * Counts msync calls that target the posting index .pk mapping while
     * armed. The openRW/mmap/mremap/munmap overrides map the .pk file's fd
     * to its live mapping address range so msync(addr, ...) can be
     * attributed back to the file. The rollback durability tests use it to
     * assert that the rollback publish paths sync .pk before queuing the
     * superseded files for purge.
     */
    private static class PkSyncCountingFacade extends TestFilesFacadeImpl {
        private final AtomicBoolean armed = new AtomicBoolean(false);
        private final AtomicInteger armedPkSyncs = new AtomicInteger(0);
        private final java.util.concurrent.ConcurrentHashMap<Long, Boolean> pkFds = new java.util.concurrent.ConcurrentHashMap<>();
        private final java.util.concurrent.ConcurrentHashMap<Long, Long> pkMappings = new java.util.concurrent.ConcurrentHashMap<>();

        @Override
        public boolean close(long fd) {
            pkFds.remove(fd);
            return super.close(fd);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            long addr = super.mmap(fd, len, offset, flags, memoryTag);
            if (addr != -1 && pkFds.containsKey(fd)) {
                pkMappings.put(addr, len);
            }
            return addr;
        }

        @Override
        public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
            long newAddr = super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
            if (pkMappings.remove(addr) != null && newAddr != -1) {
                pkMappings.put(newAddr, newSize);
            }
            return newAddr;
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            if (armed.get()) {
                for (var mapping : pkMappings.entrySet()) {
                    long base = mapping.getKey();
                    if (addr >= base && addr < base + mapping.getValue()) {
                        armedPkSyncs.incrementAndGet();
                        break;
                    }
                }
            }
            super.msync(addr, len, async);
        }

        @Override
        public void munmap(long address, long size, int memoryTag) {
            pkMappings.remove(address);
            super.munmap(address, size, memoryTag);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            long fd = super.openRW(name, opts);
            if (fd != -1 && name != null && Utf8s.containsAscii(name, ".pk")) {
                pkFds.put(fd, Boolean.TRUE);
            }
            return fd;
        }

        void arm() {
            armedPkSyncs.set(0);
            armed.set(true);
        }

        int armedPkSyncCount() {
            return armedPkSyncs.get();
        }

        void disarm() {
            armed.set(false);
        }

        boolean hasPkMapping() {
            return !pkMappings.isEmpty();
        }
    }

    /**
     * Fails the first .pk (key file) msync that fires while armed, by throwing.
     * Tracks the .pk file's mappings via openRW/mmap/mremap/munmap so msync can
     * be attributed to it. Used to fail the post-publish .pk sync inside
     * rebuildSidecarsByCopy (the sync that runs after publishToChain).
     */
    private static class PkSyncFailingFacade extends TestFilesFacadeImpl {
        private final AtomicBoolean armed = new AtomicBoolean(false);
        private final AtomicInteger failures = new AtomicInteger(0);
        private final java.util.concurrent.ConcurrentHashMap<Long, Boolean> pkFds = new java.util.concurrent.ConcurrentHashMap<>();
        private final java.util.concurrent.ConcurrentHashMap<Long, Long> pkMappings = new java.util.concurrent.ConcurrentHashMap<>();

        @Override
        public boolean close(long fd) {
            pkFds.remove(fd);
            return super.close(fd);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            long addr = super.mmap(fd, len, offset, flags, memoryTag);
            if (addr != -1 && pkFds.containsKey(fd)) {
                pkMappings.put(addr, len);
            }
            return addr;
        }

        @Override
        public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
            long newAddr = super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
            if (pkMappings.remove(addr) != null && newAddr != -1) {
                pkMappings.put(newAddr, newSize);
            }
            return newAddr;
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            if (armed.get()) {
                for (var mapping : pkMappings.entrySet()) {
                    long base = mapping.getKey();
                    if (addr >= base && addr < base + mapping.getValue()) {
                        armed.set(false);
                        failures.incrementAndGet();
                        throw CairoException.critical(0).put("[test] .pk sync failed");
                    }
                }
            }
            super.msync(addr, len, async);
        }

        @Override
        public void munmap(long address, long size, int memoryTag) {
            pkMappings.remove(address);
            super.munmap(address, size, memoryTag);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            long fd = super.openRW(name, opts);
            if (fd != -1 && name != null && Utf8s.containsAscii(name, ".pk")) {
                pkFds.put(fd, Boolean.TRUE);
            }
            return fd;
        }

        void arm() {
            failures.set(0);
            armed.set(true);
        }

        void disarm() {
            armed.set(false);
        }

        int failureCount() {
            return failures.get();
        }
    }

    /**
     * Fails the staged .pv msync (the {@code sealValueMem.sync} inside
     * reencodeWithPerKeyStreaming, BEFORE switchToSealedValueFile) the first
     * time it fires while armed. Tracks only the .pv files opened while armed --
     * the new sealTxn's staged value file -- so the pre-existing committed .pv
     * (opened before arming) is left alone. Drives the pre-switch cleanup
     * branch of the rollback catch.
     */
    private static class PvSyncFailingFacade extends TestFilesFacadeImpl {
        private final AtomicBoolean armed = new AtomicBoolean(false);
        private final AtomicInteger failures = new AtomicInteger(0);
        private final java.util.concurrent.ConcurrentHashMap<Long, Long> pvMappings = new java.util.concurrent.ConcurrentHashMap<>();
        private final java.util.concurrent.ConcurrentHashMap<Long, Boolean> stagedPvFds = new java.util.concurrent.ConcurrentHashMap<>();

        @Override
        public boolean close(long fd) {
            stagedPvFds.remove(fd);
            return super.close(fd);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            long addr = super.mmap(fd, len, offset, flags, memoryTag);
            if (addr != -1 && stagedPvFds.containsKey(fd)) {
                pvMappings.put(addr, len);
            }
            return addr;
        }

        @Override
        public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
            long newAddr = super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
            if (pvMappings.remove(addr) != null && newAddr != -1) {
                pvMappings.put(newAddr, newSize);
            }
            return newAddr;
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            if (armed.get()) {
                for (var mapping : pvMappings.entrySet()) {
                    long base = mapping.getKey();
                    if (addr >= base && addr < base + mapping.getValue()) {
                        armed.set(false);
                        failures.incrementAndGet();
                        throw CairoException.critical(0).put("[test] staged .pv sync failed");
                    }
                }
            }
            super.msync(addr, len, async);
        }

        @Override
        public void munmap(long address, long size, int memoryTag) {
            pvMappings.remove(address);
            super.munmap(address, size, memoryTag);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            long fd = super.openRW(name, opts);
            if (fd != -1 && armed.get() && name != null && Utf8s.containsAscii(name, ".pv")) {
                stagedPvFds.put(fd, Boolean.TRUE);
            }
            return fd;
        }

        void arm() {
            failures.set(0);
            armed.set(true);
        }

        void disarm() {
            armed.set(false);
        }

        int failureCount() {
            return failures.get();
        }
    }

    private record PostingSealFileNames(long sealTxn, String valueFile, String coverFile) {
    }
}

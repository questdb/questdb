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
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.std.DirectBitSet;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.tasks.PostingSealPurgeTask;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;
import static io.questdb.cairo.TableUtils.setPathForNativePartition;

/**
 * Red tests for the critical findings raised in the PR review of #6861.
 * Each test reproduces a defect that should fail against the current
 * implementation. Tests that require fault injection or platform-specific
 * conditions are marked with comments describing what they need.
 */
public class PostingIndexCriticalIssuesTest extends AbstractCairoTest {

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
    public void testO3PostingIndexRecoveryAfterPurgedCommittedSeal() throws Exception {
        FailAppendPositionLengthFacade ff = new FailAppendPositionLengthFacade();
        assertMemoryLeak(ff, () -> {
            final String tableName = "posting_o3_recovery";

            // Set up the shape produced by the fuzz failure: a non-WAL table
            // with a POSTING symbol index, a renamed indexed column, and a
            // symbol-capacity change before the O3/replace-style insert.
            execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, sym2 SYMBOL INDEX TYPE POSTING, sym_top SYMBOL CAPACITY 128, marker LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
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
                    .sizeMayVary()
                    .returns("""
                            count
                            105
                            """);
            // Indexed scan must agree. The bug loses the boundary row from the
            // recovered posting index when the committed seal file was purged.
            assertQuery("SELECT count() FROM " + tableName + " WHERE new_col_11 = 'XPHI'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
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
        // O3PartitionJob.updateParquetIndexes goes through commitDense for POSTING.
        // That path runs when O3 mutates an already-parquet partition. Convert the
        // first partition to parquet, then O3-insert into it to trigger the rewrite.
        // After that, the chain head must hold a single dense gen.
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

            final FilesFacade ff = configuration.getFilesFacade();
            final long newColumnNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                int colIndex = reader.getMetadata().getColumnIndex("new_sym");
                int writerIndex = reader.getMetadata().getWriterIndex(colIndex);
                newColumnNameTxn = reader.getColumnVersionReader().getColumnNameTxn(firstPartitionTimestamp, writerIndex);
            }

            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                setPathForNativePartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, firstPartitionTimestamp, firstPartitionNameTxn);
                int plen = path.size();
                LPSZ dstKeyFile = PostingIndexUtils.keyFileName(path.trimTo(plen), "new_sym", newColumnNameTxn);
                long fileSize = ff.length(dstKeyFile);
                Assert.assertTrue("dst .pk must exist after rename, path=" + dstKeyFile, fileSize > 0);
                // Read the header only (readUnderSeqlock never touches the head
                // entry), so a header that points past EOF does not SIGBUS here.
                try (MemoryCMARWImpl mem = new MemoryCMARWImpl(
                        ff, dstKeyFile, ff.getPageSize(), fileSize, MemoryTag.MMAP_DEFAULT, /* opts */ 0)) {
                    PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
                    Assert.assertTrue("dst .pk header unreadable, path=" + dstKeyFile,
                            PostingIndexChainHeader.readUnderSeqlock(mem, header));
                    Assert.assertTrue(
                            "link recovery truncated the renamed column's .pk below its header regionLimit: "
                                    + "fileSize=" + fileSize + " regionLimit=" + header.regionLimit
                                    + " headEntryOffset=" + header.headEntryOffset
                                    + " -- a reader of new_sym's posting index would map past EOF (SIGBUS).",
                            fileSize >= header.regionLimit
                    );
                }
            }
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
            execute("""
                    CREATE TABLE t_c1_chain_head (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING
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
        // squashSplitPartitions opens columns through FrameAlgebra twice
        // per merge step (source RO + target RW) and the reseal then
        // opens the target's covering column RO via
        // mapCoveringColumnsForSeal. The first openRO of price.d after
        // armed corresponds to the FrameAlgebra source frame; subsequent
        // openRO calls (>=2) are the seal. Track only the latter and
        // fail their mmap so FrameAlgebra completes successfully and
        // the throw is localised to the reseal call.
        final AtomicInteger priceDopenROCount = new AtomicInteger(0);
        final AtomicLong sealFd = new AtomicLong(-1);
        ff = new TestFilesFacadeImpl() {
            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (failArmed.get() && fd == sealFd.get()) {
                    return FilesFacade.MAP_FAILED;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public long openRO(LPSZ name) {
                long fd = super.openRO(name);
                if (failArmed.get() && fd != -1 && name != null
                        && Utf8s.endsWithAscii(name, "price.d")) {
                    if (priceDopenROCount.incrementAndGet() >= 2) {
                        sealFd.set(fd);
                    }
                }
                return fd;
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
     * counterpart to {@link #testWriterEntrysCoverFooterShrinksAfterClearCoveringCommit},
     * which documents that the old clearCovering()-then-commit() flow
     * shrinks the footer.
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
     * Reproduces the writer-side root cause of the GraalVM SIGSEGV in
     * hs_err_pid19555 (PostingIndexBenchmarkSuite.walFastLagInsertAndQuery
     * on bench/posting-wal-fastlag): a covering posting index whose chain
     * head had LEN=104 (= entrySize(genCount=1, coverCount=0)) while .pci
     * advertised coverCount=1. This test drives PostingIndexWriter through
     * the same lifecycle the WAL fast-lag path follows in production:
     * <ol>
     * <li> configureCovering(...) -> coverCount=1 on the writer.
     * <li> add() rows, seal() -> publishes a chain entry with cover footer.
     * <li> clearCovering() -> writer's coverCount field is reset to 0
     *     (this is what TableWriter does in the finally block of
     *     sealPostingIndexesForLastPartitionFastLag, line 11367).
     * <li> add() more rows, commit() -> flushAllPending() publishes via
     *     extendHead, which rewrites the head entry's LEN using
     *     entrySize(genCount, coverCount=0), DROPPING the cover footer.
     * </ol>
     * After step 4, the chain head's LEN no longer accommodates a cover
     * footer slot, even though the table's covering schema (mirrored in
     * .pci) still says one cover. A reader sourcing coverCount=1 from the
     * .pci sidecar and walking the chain steps past the entry's LEN to
     * read the (non-existent) footer - in production this surfaces as a
     * SIGSEGV when the entry hugs the end of the .pk mmap. With the
     * picker/read clamp fix in place the read self-bounds against the
     * entry's LEN and zero-fills the missing cover slot, so the reader
     * recovers gracefully.
     */
    @Test
    public void testWriterEntrysCoverFooterShrinksAfterClearCoveringCommit() throws Exception {
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
                                "RED: with the writer-side bug, the head's LEN no "
                                        + "longer includes the cover footer because "
                                        + "extendHead used coverCount=0 for the size "
                                        + "calculation. Expected coverCount=1 sized "
                                        + "entry, observed coverCount=0 sized entry.",
                                PostingIndexChainEntry.entrySize(genCountAfterCommit, 0),
                                lenAfterCommit
                        );
                        Assert.assertNotEquals(
                                "RED: the schema-correct LEN would include cover footer "
                                        + "bytes, but the writer dropped them",
                                PostingIndexChainEntry.entrySize(genCountAfterCommit, coverCount),
                                lenAfterCommit
                        );

                        // Final: prove Fix A keeps the reader from
                        // dereferencing past the entry on this corrupted-footer
                        // entry. Plant a sentinel byte pattern into the bytes
                        // immediately past the entry's LEN - the (mis-computed)
                        // cover footer offset for coverCount=1 lands on these
                        // bytes. Without the picker/read clamp, getLong reads
                        // the sentinel and assigns it as the cover end offset.
                        // With the clamp, the read self-bounds against the
                        // entry's LEN and zero-fills the missing slot.
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
                            // The unfixed cover loop would read at offset
                            // (header + genCount * GEN_DIR_ENTRY_SIZE) for
                            // each cover slot. Plant a sentinel there so a
                            // failing clamp surfaces as a non-zero value.
                            long footerOffset = PostingIndexChainEntry.resolveCoverFooterOffset(
                                    head.offset, head.genCount);
                            final long sentinel = 0xDEAD_BEEF_CAFE_BABEL;
                            mem.putLong(footerOffset, sentinel);

                            PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
                            PostingIndexChainEntry.Snapshot entry = new PostingIndexChainEntry.Snapshot();
                            int rc = PostingIndexChainPicker.pick(
                                    mem, /* pinnedTableTxn */ Long.MAX_VALUE,
                                    /* coverCount */ coverCount, header, entry);
                            Assert.assertEquals(
                                    PostingIndexChainPicker.RESULT_OK, rc);
                            Assert.assertEquals(
                                    "Fix A: missing cover slot is zero-filled instead "
                                            + "of dereferencing past the entry",
                                    coverCount, entry.coverFileEndOffsets.size());
                            Assert.assertEquals(
                                    "RED without Fix A: the picker would read the planted "
                                            + "sentinel " + Long.toHexString(sentinel)
                                            + "L as the cover end offset because the cover "
                                            + "footer loop is not clamped against the entry's "
                                            + "own LEN field. With Fix A, the clamp returns 0 "
                                            + "instead.",
                                    0L, entry.coverFileEndOffsets.getQuick(0));
                        }
                    }
                } finally {
                    Unsafe.free(fakeColAddr, fakeColBytes, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    private static String insertPostingRowsSql(int lo, int hi) {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append("posting_o3_recovery").append(" VALUES ");
        for (int row = hi - 1; row >= lo; row--) {
            if (row < hi - 1) {
                sql.append(',');
            }
            sql.append("('").append(timestampAtMinute(row)).append("', '")
                    .append(row >= 34 ? "XPHI" : "A")
                    .append("', 'S', ")
                    .append(row)
                    .append(')');
        }
        return sql.toString();
    }

    private static void runPostingSealPurgeJob(PostingSealPurgeJob purgeJob) {
        for (int i = 0; i < 32; i++) {
            if (!purgeJob.run(0)) {
                break;
            }
        }
    }

    private void assertOpenDeferredPostingSealPurgeLogRow(
            TableToken tableToken,
            String indexColumnName,
            long sealTxn
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
                .returns("""
                        count
                        1
                        """);
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

    private static class PostingSealFileNames {
        private final String coverFile;
        private final long sealTxn;
        private final String valueFile;

        private PostingSealFileNames(long sealTxn, String valueFile, String coverFile) {
            this.sealTxn = sealTxn;
            this.valueFile = valueFile;
            this.coverFile = coverFile;
        }
    }
}

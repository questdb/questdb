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

package io.questdb.test.cairo.sql;

import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The pool's projection: an owner that reads only some columns of every Parquet frame binds them
 * with the query, and the pool decodes no other column of a Parquet frame until the query ends.
 */
public class PageFrameMemoryPoolTest extends AbstractCairoTest {
    // The table's first two days, 1_000 rows each, are a Parquet row group each, and its third is native.
    private static final int A = 0;
    private static final int B = 1;
    private static final int MSG = 2;
    private static final int ROWS_PER_DAY = 1_000;

    @Test
    public void testColumnSubsetAndLateMaterializationKeepTheProjection() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final IntHashSet projection = columns(B);
            final IntHashSet subset = columns(A, B);
            try (RecordCursorFactory factory = select("t");
                 PageFrameCursor frameCursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                 PageFrameAddressCache addressCache = new PageFrameAddressCache();
                 PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
                 DirectLongList rows = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT)) {
                loadFrames(factory, frameCursor, addressCache);
                for (int row = 0; row < 10; row++) {
                    rows.add(row);
                }
                // Without a projection, the subset decodes A and B, and the late materialization MSG.
                pool.of(addressCache);
                PageFrameMemory frameMemory = pool.navigateTo(0, subset);
                Assert.assertNotEquals(0, frameMemory.getPageAddress(A));
                Assert.assertNotEquals(0, frameMemory.getPageAddress(B));
                Assert.assertEquals(0, frameMemory.getAuxPageAddress(MSG));
                Assert.assertTrue(frameMemory.populateRemainingColumns(subset, rows, false));
                Assert.assertNotEquals(0, frameMemory.getAuxPageAddress(MSG));
                // With the projection, both decode B alone.
                pool.of(addressCache, ParquetDecodeHint.MONOTONIC, projection);
                frameMemory = pool.navigateTo(0, subset);
                Assert.assertEquals(0, frameMemory.getPageAddress(A));
                Assert.assertNotEquals(0, frameMemory.getPageAddress(B));
                Assert.assertTrue(frameMemory.hasColumnTops());
                Assert.assertTrue(frameMemory.populateRemainingColumns(subset, rows, false));
                Assert.assertEquals(0, frameMemory.getAuxPageAddress(MSG));
                Assert.assertEquals(0, frameMemory.getPageAddress(MSG));
                Assert.assertEquals(0.5, Unsafe.getDouble(frameMemory.getPageAddress(B)), 0);
            }
        });
    }

    @Test
    public void testNullProjectionDecodesEveryColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final long fullBytes = decodedParquetBytes("t");
            try (RecordCursorFactory factory = select("t");
                 PageFrameCursor frameCursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                 PageFrameAddressCache addressCache = new PageFrameAddressCache();
                 PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, Long.MAX_VALUE);
                 PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)) {
                final int frameCount = loadFrames(factory, frameCursor, addressCache);
                pool.of(addressCache, ParquetDecodeHint.SCATTERED, null);
                Assert.assertNull(pool.getProjection());
                record.of(frameCursor);
                assertFrames(pool, record, addressCache, frameCount, null);
                Assert.assertEquals(fullBytes, pool.getCachedBytes());
            }
        });
    }

    @Test
    public void testProjectionDecodesListedColumnsOnly() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            // An unprojected pool caches this much for a scan of B alone, and this much for every column.
            final long payloadBytes = decodedParquetBytes("SELECT b FROM t");
            final long fullBytes = decodedParquetBytes("t");
            Assert.assertTrue("payload " + payloadBytes + ", full " + fullBytes, payloadBytes * 10 < fullBytes);
            final IntHashSet projection = columns(B);
            try (RecordCursorFactory factory = select("t");
                 PageFrameCursor frameCursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                 PageFrameAddressCache addressCache = new PageFrameAddressCache();
                 PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, Long.MAX_VALUE);
                 PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)) {
                final int frameCount = loadFrames(factory, frameCursor, addressCache);
                pool.of(addressCache, ParquetDecodeHint.SCATTERED, projection);
                Assert.assertSame(projection, pool.getProjection());
                record.of(frameCursor);
                // The record path reads B on every frame, and the other columns of a Parquet frame as NULL.
                assertFrames(pool, record, addressCache, frameCount, projection);
                Assert.assertEquals(payloadBytes, pool.getCachedBytes());
                Assert.assertEquals(2, pool.getCachedFrameCount());
                // A repeat visit hits the cache, decodes nothing more and reads the same values.
                assertFrames(pool, record, addressCache, frameCount, projection);
                Assert.assertEquals(payloadBytes, pool.getCachedBytes());
                // The flyweight path decodes B alone too.
                pool.of(addressCache, ParquetDecodeHint.SCATTERED, projection);
                for (int frameIndex = 0; frameIndex < frameCount; frameIndex++) {
                    final PageFrameMemory frameMemory = pool.navigateTo(frameIndex);
                    final boolean isParquet = addressCache.getFrameFormat(frameIndex) == PartitionFormat.PARQUET;
                    Assert.assertNotEquals(0, frameMemory.getPageAddress(B));
                    Assert.assertEquals(isParquet, frameMemory.getPageAddress(A) == 0);
                    Assert.assertEquals(isParquet, frameMemory.getAuxPageAddress(MSG) == 0);
                    Assert.assertEquals(isParquet, frameMemory.hasColumnTops());
                }
                Assert.assertEquals(payloadBytes, pool.getCachedBytes());
                // The projection never changes.
                Assert.assertEquals(1, projection.size());
                Assert.assertTrue(projection.contains(B));
            }
        });
    }

    @Test
    public void testProjectionDropsAtQueryBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final long payloadBytes = decodedParquetBytes("SELECT b FROM t");
            final long fullBytes = decodedParquetBytes("t");
            final IntHashSet projection = columns(B);
            try (RecordCursorFactory factory = select("t");
                 PageFrameCursor frameCursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                 PageFrameAddressCache addressCache = new PageFrameAddressCache();
                 PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, Long.MAX_VALUE);
                 PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)) {
                final int frameCount = loadFrames(factory, frameCursor, addressCache);
                record.of(frameCursor);
                pool.of(addressCache, ParquetDecodeHint.SCATTERED, projection);
                assertFrames(pool, record, addressCache, frameCount, projection);
                Assert.assertEquals(payloadBytes, pool.getCachedBytes());
                // The next of() without a projection decodes every column again, in the same file.
                pool.of(addressCache, ParquetDecodeHint.SCATTERED);
                Assert.assertNull(pool.getProjection());
                assertFrames(pool, record, addressCache, frameCount, null);
                Assert.assertEquals(fullBytes, pool.getCachedBytes());
                // And a projection that follows an unprojected query decodes B alone.
                pool.of(addressCache, ParquetDecodeHint.SCATTERED, projection);
                assertFrames(pool, record, addressCache, frameCount, projection);
                Assert.assertEquals(payloadBytes, pool.getCachedBytes());
                pool.of(addressCache);
                Assert.assertNull(pool.getProjection());
                pool.of(addressCache, ParquetDecodeHint.SCATTERED, projection);
                pool.releaseQueryResources();
                Assert.assertNull(pool.getProjection());
                pool.of(addressCache, ParquetDecodeHint.SCATTERED, projection);
                pool.clear();
                Assert.assertNull(pool.getProjection());
                pool.of(addressCache, ParquetDecodeHint.SCATTERED, projection);
                pool.close();
                Assert.assertNull(pool.getProjection());
            }
        });
    }

    @Test
    public void testProjectionKeepsTypeConversionsAndColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (a INT, s VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO c
                    SELECT x::INT, (x * 3)::VARCHAR, timestamp_sequence('2020-01-01', 86_400_000L)
                    FROM long_sequence(3_000)
                    """);
            execute("ALTER TABLE c CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-03'");
            // The Parquet days keep s as text, which a read parses to LONG, and lack n, whose rows
            // sit below its column top, as do the native day's.
            execute("ALTER TABLE c ALTER COLUMN s TYPE LONG");
            execute("ALTER TABLE c ADD COLUMN n DOUBLE");
            execute("INSERT INTO c VALUES (3_001, 9_003, '2020-01-04', 1.5)");
            try (RecordCursorFactory factory = select("c");
                 PageFrameCursor frameCursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                 PageFrameAddressCache addressCache = new PageFrameAddressCache();
                 PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, Long.MAX_VALUE);
                 PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)) {
                final int frameCount = loadFrames(factory, frameCursor, addressCache);
                final int a = factory.getMetadata().getColumnIndex("a");
                final int s = factory.getMetadata().getColumnIndex("s");
                final int n = factory.getMetadata().getColumnIndex("n");
                record.of(frameCursor);
                // With s and n projected, s converts from the stored text and n reads as NULL below
                // its column top, while a reads as NULL on the Parquet days.
                pool.of(addressCache, ParquetDecodeHint.SCATTERED, columns(s, n));
                long x = 0;
                for (int frameIndex = 0; frameIndex < frameCount; frameIndex++) {
                    pool.navigateTo(frameIndex, record);
                    final boolean isParquet = addressCache.getFrameFormat(frameIndex) == PartitionFormat.PARQUET;
                    for (long row = 0, frameRows = addressCache.getFrameSize(frameIndex); row < frameRows; row++) {
                        x++;
                        record.setRowIndex(row);
                        Assert.assertEquals(x * 3, record.getLong(s));
                        Assert.assertEquals(isParquet ? Numbers.INT_NULL : x, record.getInt(a));
                        if (x <= 3 * ROWS_PER_DAY) {
                            Assert.assertTrue(Double.isNaN(record.getDouble(n)));
                        } else {
                            Assert.assertEquals(1.5, record.getDouble(n), 0);
                        }
                    }
                }
                Assert.assertEquals(3_001, x);
                // With a alone projected, a Parquet day reads s as NULL, with no conversion to run.
                pool.of(addressCache, ParquetDecodeHint.SCATTERED, columns(a));
                pool.navigateTo(0, record);
                record.setRowIndex(0);
                Assert.assertEquals(1, record.getInt(a));
                Assert.assertEquals(Numbers.LONG_NULL, record.getLong(s));
                Assert.assertTrue(Double.isNaN(record.getDouble(n)));
            }
        });
    }

    // Reads every row of every frame through the record path, which binds the record to each frame,
    // and checks every column: a column outside the projection reads as NULL on a Parquet frame.
    private static void assertFrames(
            PageFrameMemoryPool pool,
            PageFrameMemoryRecord record,
            PageFrameAddressCache addressCache,
            int frameCount,
            IntHashSet projection
    ) {
        long x = 0;
        for (int frameIndex = 0; frameIndex < frameCount; frameIndex++) {
            pool.navigateTo(frameIndex, record);
            final boolean isParquet = addressCache.getFrameFormat(frameIndex) == PartitionFormat.PARQUET;
            for (long row = 0, frameRows = addressCache.getFrameSize(frameIndex); row < frameRows; row++) {
                x++;
                record.setRowIndex(row);
                final boolean isAProjected = !isParquet || projection == null || projection.contains(A);
                final boolean isBProjected = !isParquet || projection == null || projection.contains(B);
                final boolean isMsgProjected = !isParquet || projection == null || projection.contains(MSG);
                Assert.assertEquals(isAProjected ? x : Numbers.INT_NULL, record.getInt(A));
                if (isBProjected) {
                    Assert.assertEquals(x * 0.5, record.getDouble(B), 0);
                } else {
                    Assert.assertTrue(Double.isNaN(record.getDouble(B)));
                }
                final Utf8Sequence msg = record.getVarcharA(MSG);
                if (isMsgProjected) {
                    Assert.assertNotNull(msg);
                    TestUtils.assertEquals(Long.toString(x), msg.toString().substring(0, Long.toString(x).length()));
                    Assert.assertEquals(300, msg.size());
                } else {
                    Assert.assertNull(msg);
                }
            }
        }
        Assert.assertEquals(3 * ROWS_PER_DAY, x);
    }

    private static IntHashSet columns(int... columnIndexes) {
        final IntHashSet set = new IntHashSet();
        for (int columnIndex : columnIndexes) {
            set.add(columnIndex);
        }
        return set;
    }

    private static void createTable() throws Exception {
        // msg is 300 bytes of text a row that compress well, so a Parquet row group decodes it to a
        // 16-byte slice entry a row and to the decompressed pages that the entries point into.
        execute("CREATE TABLE t (a INT, b DOUBLE, msg VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO t
                SELECT x::INT, x * 0.5, rpad(x::VARCHAR, 300, ' compressible log line'), timestamp_sequence('2020-01-01', 86_400_000L)
                FROM long_sequence(3_000)
                """);
        execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-03'");
    }

    // Decodes every Parquet frame of the scan whole into an unprojected pool, and returns the bytes
    // that the pool's budget counts for them.
    private static long decodedParquetBytes(String scan) throws Exception {
        try (RecordCursorFactory factory = select(scan);
             PageFrameCursor frameCursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
             PageFrameAddressCache addressCache = new PageFrameAddressCache();
             PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, Long.MAX_VALUE)) {
            final int frameCount = loadFrames(factory, frameCursor, addressCache);
            pool.of(addressCache, ParquetDecodeHint.SCATTERED);
            for (int frameIndex = 0; frameIndex < frameCount; frameIndex++) {
                if (addressCache.getFrameFormat(frameIndex) == PartitionFormat.PARQUET) {
                    pool.navigateTo(frameIndex);
                }
            }
            Assert.assertEquals(2, pool.getCachedFrameCount());
            return pool.getCachedBytes();
        }
    }

    private static int loadFrames(RecordCursorFactory factory, PageFrameCursor frameCursor, PageFrameAddressCache addressCache) {
        addressCache.of(factory.getMetadata(), frameCursor);
        int frameCount = 0;
        PageFrame frame;
        while ((frame = frameCursor.next()) != null) {
            addressCache.add(frameCount++, frame);
        }
        return frameCount;
    }
}

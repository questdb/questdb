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

package io.questdb.test.griffin.engine.table;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.AbstractPageFrameRecordCursor;
import io.questdb.griffin.engine.table.PageFrameRecordCursorImpl;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;

public class KeyOrderedPageFrameRecordCursorTest extends AbstractCairoTest {
    @Test
    public void testDenseGatherCancellationReleasesMemory() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 8);
        assertMemoryLeak(() -> {
            createGatherTable();
            final SqlExecutionCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine) {
                @Override
                public void statefulThrowExceptionIfTrippedTimeThrottledOrYield() {
                    if (PageFrameRecordCursorImpl.testDenseScanFrames.get() == 100) {
                        cancel();
                    }
                    super.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
                }
            };
            try (RecordCursorFactory factory = select("SELECT * FROM t WHERE ts IN '1970-01-01' ORDER BY s, ts")) {
                final SqlExecutionCircuitBreaker originalBreaker = sqlExecutionContext.getCircuitBreaker();
                ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
                PageFrameRecordCursorImpl.testDenseScanFrames.set(0);
                PageFrameRecordCursorImpl.isWalkCounterEnabled = true;
                try (RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                    try {
                        cursor.hasNext();
                        Assert.fail("expected cancellation inside dense gather");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isCancellation());
                        Assert.assertEquals(100, PageFrameRecordCursorImpl.testDenseScanFrames.get());
                    }
                } finally {
                    PageFrameRecordCursorImpl.isWalkCounterEnabled = false;
                    ((SqlExecutionContextImpl) sqlExecutionContext).with(originalBreaker);
                }
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    int count = 0;
                    while (cursor.hasNext()) {
                        count++;
                    }
                    Assert.assertEquals(2_080, count);
                }
            }
        });
    }

    @Test
    public void testDenseGatherFailsCleanlyAtQueryMemoryLimit() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 8);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        assertMemoryLeak(() -> {
            // 20_001 index keys over 40_000 rows: the key map of the dense gather is one of the larger allocations of
            // the read, so that a sweep of memory limits fails at each of them in turn
            execute("""
                    CREATE TABLE t AS (
                        SELECT x id, ('k' || (x % 20_000))::SYMBOL s, timestamp_sequence(0, 1_000) ts
                        FROM long_sequence(40_000)
                    ), INDEX(s) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("INSERT INTO t VALUES (40_001, NULL, '1970-01-02')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
            final String tail = "id, s, ts FROM t WHERE ts IN '1970-01-01' ORDER BY s, ts";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(t) */ " + tail, expected);
            final String expectedRows = expected.toString();
            final StringSink actual = new StringSink();
            int failureCount = 0;
            int successCount = 0;
            try (RecordCursorFactory factory = select("SELECT " + tail)) {
                for (long cacheBytes : new long[]{64 * 1024 * 1024L, 1024}) {
                    node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, cacheBytes);
                    for (long memoryLimit = 64 * 1024; memoryLimit < 8 * 1024 * 1024; memoryLimit += memoryLimit / 10) {
                        node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, memoryLimit);
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            TestUtils.assertCursor(expectedRows, cursor, factory.getMetadata(), true, actual);
                            successCount++;
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                            failureCount++;
                        }
                    }
                }
            } finally {
                node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
            }
            Assert.assertTrue("some limits are below the memory of the read", failureCount > 0);
            Assert.assertTrue("some limits are above the memory of the read", successCount > 0);
        });
    }

    @Test
    public void testDenseGatherOfFixedSizeColumnsMatchesSort() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        // NOT IN reads the 255 other keys of the column by the index
        node1.setProperty(PropertyKey.CAIRO_SQL_MAX_SYMBOL_NOT_EQUALS_COUNT, 1_000);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE g AS (
                        SELECT x id, (CASE WHEN x % 17 = 0 THEN NULL ELSE 'k' || (x % 256) END)::SYMBOL s, x * 0.5 v,
                            timestamp_sequence('2024-01-01', 1_000_000) ts
                        FROM long_sequence(2_080)
                    ), INDEX(s) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO g VALUES
                        (2_081, 'k1', 0.5, '2024-01-02'), (2_082, NULL, 1.0, '2024-01-02T00:00:01'),
                        (2_083, 'k7', 1.5, '2024-01-03'), (2_084, 'k200', 2.0, '2024-01-03T00:00:01'),
                        (2_085, 'k7', 2.5, '2024-01-03T00:00:02')
                    """);
            execute("ALTER TABLE g CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
            final StringSink values = new StringSink();
            final StringSink binds = new StringSink();
            values.put("s IN (NULL");
            binds.put("s IN (");
            for (int i = 0; i < 128; i++) {
                values.put(", 'k").put(i * 2).put('\'');
                binds.put('$').put(i + 1).put(", ");
                bindVariableService.setStr(i, "k" + i * 2);
            }
            values.put(')');
            binds.put("$129, $130)");
            bindVariableService.setStr(128, "absent");
            bindVariableService.setStr(129, null);
            final String[] predicates = {
                    "",
                    values.toString(),
                    values + " AND id % 3 = 0",
                    binds + " AND v > 10",
                    "s NOT IN ('k5', 'k6')",
                    "s NOT IN ('k5', 'k6') AND id % 3 = 0"
            };
            final StringSink expected = new StringSink();
            // a gather of rows and a gather of row ids over the row groups of the Parquet partition, whole or cut by
            // the intervals, and the passes of the keys over the native partition, which the read does not gather
            for (long cacheBytes : new long[]{64 * 1024 * 1024L, 1024}) {
                node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, cacheBytes);
                for (String interval : new String[]{"ts IN '2024-01-01T00:00:03;10m;20m;2'", "ts IN '2024-01-01'", "ts IN '2024-01-03'"}) {
                    for (String predicate : predicates) {
                        for (String order : new String[]{"s, ts", "s DESC, ts DESC", "s, ts DESC", "s DESC, ts"}) {
                            for (String limit : new String[]{"", " LIMIT 5", " LIMIT -7", " LIMIT 13, 14", " LIMIT 3, 700", " LIMIT -700, -3"}) {
                                final String tail = "id, s, v, ts FROM g WHERE " + interval
                                        + (predicate.isEmpty() ? "" : " AND " + predicate) + " ORDER BY " + order + limit;
                                printSql("SELECT /*+ no_index(g) */ " + tail, expected);
                                PageFrameRecordCursorImpl.testDenseScanFrames.set(0);
                                PageFrameRecordCursorImpl.isWalkCounterEnabled = true;
                                try {
                                    assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
                                } finally {
                                    PageFrameRecordCursorImpl.isWalkCounterEnabled = false;
                                }
                                // the windows of the other limits hold too few keys for the scan to pay
                                if (interval.startsWith("ts IN '2024-01-01") && (limit.isEmpty() || limit.contains("700"))) {
                                    Assert.assertTrue("the gather scans page frames densely", PageFrameRecordCursorImpl.testDenseScanFrames.get() > 0);
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testDenseGatherSkipsKeyMapOfManySymbols() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 8);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 64 * 1024 * 1024L);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT x id, ('k' || x)::SYMBOL s, timestamp_sequence(0, 1_000) ts
                        FROM long_sequence(4_096)
                    ), INDEX(s) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("INSERT INTO t VALUES (0, 'k1', '1970-01-02')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
            // the 64 keys outnumber the rows of each row group, but a map of the 4_097 index keys of the column would
            // take more memory than the entries the gather keeps for the 64 keys
            final StringSink tail = new StringSink();
            tail.put("id, s, ts FROM t WHERE ts IN '1970-01-01' AND s IN ('k1'");
            for (int i = 1; i < 64; i++) {
                tail.put(", 'k").put(i * 64).put('\'');
            }
            tail.put(") ORDER BY s, ts");
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(t) */ " + tail, expected);
            PageFrameRecordCursorImpl.testDenseScanFrames.set(0);
            PageFrameRecordCursorImpl.isWalkCounterEnabled = true;
            try {
                assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
            } finally {
                PageFrameRecordCursorImpl.isWalkCounterEnabled = false;
            }
            Assert.assertEquals(0, PageFrameRecordCursorImpl.testDenseScanFrames.get());
            try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                final long stateSize = cursor.preComputedStateSize();
                Assert.assertTrue(cursor.hasNext());
                Assert.assertNotEquals("the read gathers its rows", stateSize, cursor.preComputedStateSize());
            }
        });
    }

    @Test
    public void testGatherProbeCountDoesNotMultiplyKeysByFrames() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 8);
        assertMemoryLeak(() -> {
            createGatherTable();
            for (long cacheBytes : new long[]{64 * 1024 * 1024L, 1024}) {
                node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, cacheBytes);
                try (RecordCursorFactory scan = select("SELECT * FROM t WHERE ts IN '1970-01-01'");
                     PageFrameCursor frames = scan.getBaseFactory().getPageFrameCursor(sqlExecutionContext, ORDER_ASC)) {
                    int frameCount = 0;
                    while (frames.next() != null) {
                        frameCount++;
                    }
                    Assert.assertEquals(260, frameCount);
                }
                try (RecordCursorFactory factory = select("SELECT * FROM t WHERE ts IN '1970-01-01' ORDER BY s, ts")) {
                    PageFrameRecordCursorImpl.testDenseScanFrames.set(0);
                    PageFrameRecordCursorImpl.isWalkCounterEnabled = true;
                    try (RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                        int count = 0;
                        while (cursor.hasNext()) {
                            count++;
                        }
                        Assert.assertEquals(2_080, count);
                        // the 257 keys outnumber the 8 rows of each row group, which the gather scans rather than
                        // probing the index once per key
                        Assert.assertEquals(260, PageFrameRecordCursorImpl.testDenseScanFrames.get());
                        Assert.assertEquals(cacheBytes == 1024 ? ParquetDecodeHint.CYCLIC : ParquetDecodeHint.MONOTONIC,
                                ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool().getDecodeHint());
                    } finally {
                        PageFrameRecordCursorImpl.isWalkCounterEnabled = false;
                    }
                }
            }
        });
    }

    @Test
    public void testNonuniformWidthGatherDoesNotDecodeUnselectedPayload() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 8);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 64 * 1024 * 1024L);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (id LONG, s SYMBOL INDEX, payload VARCHAR, ts TIMESTAMP)
                    TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO t
                    SELECT x, ('k' || ((x - 1) % 256))::SYMBOL,
                        (CASE WHEN (x - 1) % 256 < 64 THEN 'a' ELSE rpad('w', 8192, 'w') END)::VARCHAR,
                        timestamp_sequence(0, 1000)
                    FROM long_sequence(4096)
                    """);
            execute("INSERT INTO t VALUES (0, 'z', NULL, '1970-01-02')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
            final StringSink tail = new StringSink();
            tail.put("id, s, payload, ts FROM t WHERE ts IN '1970-01-01' AND s IN ('k0'");
            for (int i = 1; i < 64; i++) {
                tail.put(", 'k").put(i).put("'");
            }
            tail.put(") ORDER BY s, ts");
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(t) */ " + tail, expected);
            assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
            PageFrameRecordCursorImpl.testDenseScanFrames.set(0);
            PageFrameRecordCursorImpl.isWalkCounterEnabled = true;
            try (RecordCursorFactory factory = select("SELECT " + tail);
                 RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                int count = 0;
                while (cursor.hasNext()) {
                    count++;
                }
                Assert.assertEquals(1024, count);
                Assert.assertFalse("the read gathers rows rather than row ids", cursor.getRecord() instanceof PageFrameMemoryRecord);
                final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
                Assert.assertEquals(ParquetDecodeHint.MONOTONIC, pool.getDecodeHint());
                Assert.assertEquals("payload decodes", 128, pool.getDecodeCount());
                Assert.assertEquals("partial-key variable-width gather must use the index", 0, PageFrameRecordCursorImpl.testDenseScanFrames.get());
            } finally {
                PageFrameRecordCursorImpl.isWalkCounterEnabled = false;
            }
        });
    }

    @Test
    public void testSparseWideGatherDoesNotDecodeUnselectedFrames() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 8);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (id LONG, s SYMBOL INDEX, payload VARCHAR, ts TIMESTAMP)
                    TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("INSERT INTO t SELECT x, ('k' || ((x - 1) % 256))::SYMBOL, '" + "a".repeat(8192)
                    + "'::VARCHAR, timestamp_sequence(0, 1000) FROM long_sequence(4096)");
            execute("INSERT INTO t VALUES (0, 'z', NULL, '1970-01-02')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
            for (long cacheBytes : new long[]{1024, 6 * 1024 * 1024L}) {
                node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, cacheBytes);
                try (RecordCursorFactory factory = select("""
                        SELECT id, s, payload, ts FROM t WHERE ts IN '1970-01-01'
                        AND s IN ('k0', 'k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8',
                                  'k9', 'k10', 'k11', 'k12', 'k13', 'k14', 'k15', 'k16')
                        ORDER BY s, ts
                        """); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                    final long stateSize = cursor.preComputedStateSize();
                    int count = 0;
                    while (cursor.hasNext()) {
                        count++;
                    }
                    Assert.assertEquals(272, count);
                    Assert.assertNotEquals("the read gathers its rows", stateSize, cursor.preComputedStateSize());
                    Assert.assertEquals(cacheBytes == 1024, cursor.getRecord() instanceof PageFrameMemoryRecord);
                    final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
                    Assert.assertEquals(cacheBytes == 1024 ? ParquetDecodeHint.CYCLIC : ParquetDecodeHint.MONOTONIC, pool.getDecodeHint());
                    Assert.assertEquals(cacheBytes == 1024 ? 272 : 48, pool.getDecodeCount());
                }
            }
        });
    }

    private static void createGatherTable() throws Exception {
        execute("""
                CREATE TABLE t AS (
                    SELECT x id, ('k' || (x % 256))::SYMBOL s, timestamp_sequence(0, 1_000) ts
                    FROM long_sequence(2_080)
                ), INDEX(s) TIMESTAMP(ts) PARTITION BY DAY
                """);
        execute("INSERT INTO t VALUES (2_081, NULL, '1970-01-02')");
        execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
    }
}

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
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.DelegatingRecord;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.AbstractPageFrameRecordCursor;
import io.questdb.griffin.engine.table.PageFrameRecordCursorImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;

public class KeyOrderedPageFrameRecordCursorTest extends AbstractCairoTest {
    @Test
    public void testCachedRowGroupsOfManyKeysGatherInOneWindow() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT x id, ('k' || (x % 300))::SYMBOL s, timestamp_sequence(0, 1_000_000) ts
                        FROM long_sequence(2_000)
                    ), INDEX(s) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("INSERT INTO t VALUES (2_001, 'k1', '1970-01-02')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
            // the 20 row groups stay decoded across the passes of the keys: the 300 keys outnumber their rows enough for
            // a dense scan of each to cost less than those passes, and their rows fit one window, the ten keys do not;
            // the passes of the keys of a filtered read stop at the end of the read, which a window would only bound
            final StringSink keys = new StringSink();
            keys.put(" AND s IN ('k0'");
            for (int i = 1; i < 150; i++) {
                keys.put(", 'k").put(i * 2).put('\'');
            }
            keys.put(") AND id % 3 = 0");
            final String[] predicates = {"", " AND s IN ('k0', 'k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8', 'k9')", keys.toString(), keys.toString()};
            final long[] limits = {RecordCursor.UNBOUNDED_ROW_COUNT, RecordCursor.UNBOUNDED_ROW_COUNT, RecordCursor.UNBOUNDED_ROW_COUNT, 100};
            final ParquetDecodeHint[] hints = {ParquetDecodeHint.MONOTONIC, ParquetDecodeHint.CYCLIC, ParquetDecodeHint.MONOTONIC, ParquetDecodeHint.CYCLIC};
            // the scans decode each row group, and so do the passes of the keys of the filtered read, the passes of
            // the ten keys only the row groups that hold them
            final long[] decodes = {20, 13, 20, 20};
            final StringSink expected = new StringSink();
            for (int i = 0; i < predicates.length; i++) {
                final String tail = "id, s, ts FROM t WHERE ts IN '1970-01-01'" + predicates[i] + " ORDER BY s, ts";
                printSql("SELECT /*+ no_index(t) */ " + tail, expected);
                assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
                try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                    final long stateSize = cursor.preComputedStateSize();
                    cursor.skipRows(new RecordCursor.Counter(), limits[i]);
                    long count = 0;
                    while (count < limits[i] && cursor.hasNext()) {
                        count++;
                    }
                    final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
                    Assert.assertEquals(decodes[i], pool.getDecodeCount());
                    Assert.assertEquals(hints[i], pool.getDecodeHint());
                    Assert.assertEquals("the read gathers its rows", hints[i] == ParquetDecodeHint.MONOTONIC, stateSize != cursor.preComputedStateSize());
                }
            }
        });
    }

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
            // a gather in one window and a gather in windows over the row groups of the Parquet partition, whole or cut
            // by the intervals, and the passes of the keys over the native partition, which the read does not gather
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
                                // the windows of the other limits, and those of the small decode cache, hold too few keys for
                                // the scan to pay
                                if (cacheBytes > 1024 && interval.startsWith("ts IN '2024-01-01") && (limit.isEmpty() || limit.contains("700"))) {
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
    public void testFilteredLimitGathersWindowOfItsRows() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            createWindowTable();
            final StringSink keys = new StringSink();
            keys.put(" AND s IN ('k0'");
            for (int i = 1; i < 500; i++) {
                keys.put(", 'k").put(i * 4).put('\'');
            }
            keys.put(')');
            final String tail = "id, s, ts FROM t WHERE ts IN '1970-01-01'" + keys + " AND id % 3 = 0 ORDER BY s, ts";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(t) */ " + tail + " LIMIT 100", expected);
            assertQuery("SELECT " + tail + " LIMIT 100").noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
            try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                cursor.skipRows(new RecordCursor.Counter(), 100);
                int count = 0;
                while (count < 100 && cursor.hasNext()) {
                    count++;
                }
                Assert.assertEquals(100, count);
                // the filter keeps a third of the rows of the keys, so the window of the first 100 rows takes the keys the
                // index counts about 300 rows for, and decodes the row groups that hold them rather than all 400
                Assert.assertEquals(100, ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool().getDecodeCount());
            }
        });
    }

    @Test
    public void testFilteredSkipLandsPastWindowThatShrankAfterSize() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 8);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 16 * 1024L);
        assertMemoryLeak(() -> {
            // the first 400 rows hold one-byte payloads and the 400 after them 4 KiB ones, in key order
            execute("""
                    CREATE TABLE t (id LONG, s SYMBOL INDEX, payload VARCHAR, ts TIMESTAMP)
                    TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO t
                    SELECT x, ('k' || lpad(((x - 1) / 4)::STRING, 3, '0'))::SYMBOL,
                        (CASE WHEN x <= 400 THEN 'a' ELSE rpad('w', 4_096, 'w') END)::VARCHAR, timestamp_sequence(0, 1_000)
                    FROM long_sequence(800)
                    """);
            execute("INSERT INTO t VALUES (801, 'k0', 'x', '1970-01-02')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
            final StringSink keys = new StringSink();
            keys.put("'k000'");
            for (int i = 1; i < 200; i++) {
                keys.put(", 'k").put(i / 100).put(i / 10 % 10).put(i % 10).put('\'');
            }
            final String tail = "id, s, payload, ts FROM t WHERE ts IN '1970-01-01' AND s IN (" + keys + ") AND id > 0 ORDER BY s, ts";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(t) */ " + tail + " LIMIT 150, 155", expected);
            final StringSink actual = new StringSink();
            node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 256 * 1024L);
            try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                // the size pass gathers the narrow rows in one window, and the wide rows in windows that the memory of
                // the gather plans anew, which the window of the narrow rows takes when the skip gathers it again
                final RecordCursor.Counter counter = new RecordCursor.Counter();
                cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                Assert.assertEquals(800, counter.get());
                cursor.toTop();
                counter.set(150);
                cursor.skipRows(counter, 5);
                CursorPrinter.println(factory.getMetadata(), actual);
                for (int i = 0; i < 5 && cursor.hasNext(); i++) {
                    CursorPrinter.println(cursor.getRecord(), factory.getMetadata(), actual);
                }
                TestUtils.assertEquals(expected, actual);
            } finally {
                node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
            }
        });
    }

    @Test
    public void testFilteredWindowsLandSkipAfterSize() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 16 * 1024L);
        assertMemoryLeak(() -> {
            createWindowTable();
            final StringSink keys = new StringSink();
            keys.put(" AND s IN ('k0'");
            for (int i = 1; i < 500; i++) {
                keys.put(", 'k").put(i * 4).put('\'');
            }
            keys.put(')');
            final String tail = "id, s, ts FROM t WHERE ts IN '1970-01-01'" + keys + " AND id % 3 = 0 ORDER BY s, ts";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(t) */ " + tail + " LIMIT -7", expected);
            assertQuery("SELECT " + tail + " LIMIT -7").noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
            try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                final RecordCursor.Counter counter = new RecordCursor.Counter();
                cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                final long rowCount = counter.get();
                final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
                final long sizeDecodes = pool.getDecodeCount();
                cursor.toTop();
                counter.set(rowCount - 7);
                cursor.skipRows(counter, RecordCursor.UNBOUNDED_ROW_COUNT);
                int count = 0;
                while (cursor.hasNext()) {
                    count++;
                }
                Assert.assertEquals(7, count);
                Assert.assertTrue("the rows outgrow the budget of the gather", cursor.getRecord() instanceof DelegatingRecord);
                // the size pass goes over every window, and the skip lands in the last of them, which the read holds
                Assert.assertEquals(840, sizeDecodes);
                Assert.assertEquals(sizeDecodes, pool.getDecodeCount());
            }
        });
    }

    @Test
    public void testGatherOfEveryKeyReservesItsRowsWithoutGrowth() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            createWindowTable();
            final String tail = "id, s, ts FROM t WHERE ts IN '1970-01-01' ORDER BY s, ts";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(t) */ " + tail, expected);
            assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
            try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                int count = 0;
                while (cursor.hasNext()) {
                    count++;
                }
                Assert.assertEquals(40_000, count);
                Assert.assertFalse("the rows fit the budget of the gather", cursor.getRecord() instanceof DelegatingRecord);
                // the 40_000 rows take 1.12 MB in the record chain, a link, a LONG, a SYMBOL and a TIMESTAMP each, which
                // a chain that doubled its memory as it grew would hold 2 MB for
                Assert.assertTrue("the record chain holds what its rows take", sqlExecutionContext.getMemoryTracker().getUsed() < 1_400_000);
            }
        });
    }

    @Test
    public void testGatherOfRowsWiderThanTheirSampleFitsQueryMemoryLimit() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 8);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 16 * 1024L);
        assertMemoryLeak(() -> {
            // the first row group holds one-byte payloads, which the gather samples, and the 99 after it hold 4 KiB ones
            execute("""
                    CREATE TABLE t (id LONG, s SYMBOL INDEX, payload VARCHAR, ts TIMESTAMP)
                    TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO t
                    SELECT x, ('k' || (x % 4))::SYMBOL, (CASE WHEN x <= 8 THEN 'a' ELSE rpad('w', 4_096, 'w') END)::VARCHAR,
                        timestamp_sequence(0, 1_000)
                    FROM long_sequence(800)
                    """);
            execute("INSERT INTO t VALUES (801, 'k0', 'x', '1970-01-02')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
            // the payloads grow with time and the keys run against it, so that the rows of the first keys of a window
            // come last in its pass
            execute("""
                    CREATE TABLE r (id LONG, s SYMBOL INDEX, payload VARCHAR, ts TIMESTAMP)
                    TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO r
                    SELECT x, ('k' || lpad((199 - (x - 1) / 4)::STRING, 3, '0'))::SYMBOL,
                        (CASE WHEN x <= 8 THEN 'a' ELSE rpad('w', (x * 16)::INT, 'w') END)::VARCHAR, timestamp_sequence(0, 1_000)
                    FROM long_sequence(800)
                    """);
            execute("INSERT INTO r VALUES (801, 'k0', 'x', '1970-01-02')");
            execute("ALTER TABLE r CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
            final String[] tails = {
                    "t WHERE ts IN '1970-01-01' ORDER BY s, ts",
                    "t WHERE ts IN '1970-01-01' ORDER BY s, ts LIMIT 300, 310",
                    "t WHERE ts IN '1970-01-01' ORDER BY s DESC, ts DESC",
                    "t WHERE ts IN '1970-01-01' AND s IN ('k0', 'k1', 'k2') ORDER BY s, ts",
                    "t WHERE ts IN '1970-01-01' AND s IN ('k0', 'k1', 'k2') AND id > 0 ORDER BY s, ts",
                    "t WHERE ts IN '1970-01-01' AND s IN ('k0', 'k1', 'k2') AND id > 0 ORDER BY s, ts LIMIT -7",
                    "t WHERE ts IN '1970-01-01' AND s IN ('k0', 'k1', 'k2') AND id > 0 ORDER BY s DESC, ts DESC",
                    "r WHERE ts IN '1970-01-01' ORDER BY s, ts",
                    "r WHERE ts IN '1970-01-01' AND s IN ('k000', 'k001', 'k002', 'k100', 'k199') ORDER BY s, ts",
                    "r WHERE ts IN '1970-01-01' AND s IN ('k000', 'k001', 'k002', 'k100', 'k199') AND id > 0 ORDER BY s, ts"
            };
            final StringSink expected = new StringSink();
            final StringSink actual = new StringSink();
            for (String tail : tails) {
                printSql("SELECT /*+ no_index(" + tail.charAt(0) + ") */ id, s, payload, ts FROM " + tail, expected);
                final String expectedRows = expected.toString();
                try (RecordCursorFactory factory = select("SELECT id, s, payload, ts FROM " + tail)) {
                    // a sort of the rows, which reads their payloads back through the decode cache, fits the limit
                    node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 256 * 1024L);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        TestUtils.assertCursor(expectedRows, cursor, factory.getMetadata(), true, actual);
                    } finally {
                        node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
                    }
                    assertQuery("SELECT id, s, payload, ts FROM " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expectedRows);
                }
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
                        // probing the index once per key, in one window, or in three over the small decode cache, the
                        // last of which ends at the last row group but one
                        Assert.assertEquals(cacheBytes == 1024 ? 779 : 260, PageFrameRecordCursorImpl.testDenseScanFrames.get());
                        Assert.assertEquals(ParquetDecodeHint.MONOTONIC, ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool().getDecodeHint());
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
            final long chainBytesLo = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN);
            try (RecordCursorFactory factory = select("SELECT " + tail);
                 RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                int count = 0;
                while (cursor.hasNext()) {
                    count++;
                }
                Assert.assertEquals(1024, count);
                // the record chain holds the 36 bytes each row takes past its payload, which a window of fewer rows does not
                Assert.assertTrue("the read gathers its rows in one window", Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN) - chainBytesLo >= 1024 * 36);
                final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
                Assert.assertEquals(ParquetDecodeHint.MONOTONIC, pool.getDecodeHint());
                Assert.assertEquals("payload decodes", 128, pool.getDecodeCount());
                Assert.assertEquals("the scan reads only the row groups the index finds the keys in", 128, PageFrameRecordCursorImpl.testDenseScanFrames.get());
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
                final long chainBytesLo = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN);
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
                    // one window holds the 8 KiB payloads of all 272 rows in the record chain
                    Assert.assertEquals("a small decode cache splits the read into windows", cacheBytes == 1024, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN) - chainBytesLo < 272 * 8192L);
                    final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
                    Assert.assertEquals(ParquetDecodeHint.MONOTONIC, pool.getDecodeHint());
                    // a window of the small decode cache holds a row, and decodes the row group that holds it
                    Assert.assertEquals(cacheBytes == 1024 ? 272 : 48, pool.getDecodeCount());
                }
            }
        });
    }

    @Test
    public void testWindowedGatherDecodesEachRowGroupOncePerWindow() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 64 * 1024L);
        assertMemoryLeak(() -> {
            createWindowTable();
            final StringSink keys = new StringSink();
            keys.put(" AND s IN ('k0'");
            for (int i = 1; i < 500; i++) {
                keys.put(", 'k").put(i * 4).put('\'');
            }
            keys.put(')');
            final StringSink expected = new StringSink();
            final StringSink actual = new StringSink();
            // every key, or a quarter of them, over the 400 row groups of the day
            final String[] predicates = {"", keys.toString()};
            final long[] rowCounts = {40_000, 10_000};
            final long[] decodes = {1_190, 500};
            for (int i = 0; i < predicates.length; i++) {
                final String tail = "id, s, ts FROM t WHERE ts IN '1970-01-01'" + predicates[i] + " ORDER BY s, ts";
                printSql("SELECT /*+ no_index(t) */ " + tail, expected);
                final String expectedRows = expected.toString();
                assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
                final long sortedBytes;
                try (RecordCursorFactory factory = select("SELECT /*+ no_index(t) */ " + tail); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    TestUtils.assertCursor(expectedRows, cursor, factory.getMetadata(), true, actual);
                    sortedBytes = sqlExecutionContext.getMemoryTracker().getUsed();
                }
                try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                    long count = 0;
                    while (cursor.hasNext()) {
                        count++;
                    }
                    Assert.assertEquals(rowCounts[i], count);
                    final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
                    // each window passes over the row groups once, and decodes those that hold its rows up to its last
                    Assert.assertEquals(decodes[i], pool.getDecodeCount());
                    Assert.assertEquals(ParquetDecodeHint.MONOTONIC, pool.getDecodeHint());
                    Assert.assertTrue("the rows outgrow the budget of the gather", cursor.getRecord() instanceof DelegatingRecord);
                }
                try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    TestUtils.assertCursor(expectedRows, cursor, factory.getMetadata(), true, actual);
                    Assert.assertTrue("a window holds less than a sort of the rows", sqlExecutionContext.getMemoryTracker().getUsed() < sortedBytes);
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

    private static void createWindowTable() throws Exception {
        execute("""
                CREATE TABLE t AS (
                    SELECT x id, ('k' || (x % 2_000))::SYMBOL s, timestamp_sequence(0, 1_000_000) ts
                    FROM long_sequence(40_000)
                ), INDEX(s) TIMESTAMP(ts) PARTITION BY DAY
                """);
        execute("INSERT INTO t VALUES (40_001, 'k1', '1970-01-02')");
        execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
    }
}

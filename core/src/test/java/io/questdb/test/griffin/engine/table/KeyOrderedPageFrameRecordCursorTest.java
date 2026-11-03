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
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.DelegatingRecord;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.test.TestLatchedCounterFunctionFactory;
import io.questdb.griffin.engine.table.AbstractPageFrameRecordCursor;
import io.questdb.griffin.engine.table.PageFrameRecordCursorImpl;
import io.questdb.griffin.engine.window.CachedWindowLightRecordCursorFactory;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;

public class KeyOrderedPageFrameRecordCursorTest extends AbstractCairoTest {
    // the memory a sort of the 8_000 rows of the table w takes: an entry of 16 bytes for each, and the 256 KiB decode
    // cache that it reads their payloads back through
    private static final long SORT_BYTES = 8_000 * 16 + 256 * 1024;
    // one-byte payloads, then wider ones in steps, the last far wider than the windows of the steps before it expect
    private static final String STAIRCASE_PAYLOAD = """
            CASE WHEN x <= 200 THEN 'a' WHEN x <= 4_700 THEN rpad('w', 46, 'w') WHEN x <= 7_400 THEN rpad('w', 88, 'w') ELSE rpad('w', 1_024, 'w') END""";

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
            // a dense scan of each to cost less than those passes, and the read returns their rows by row id from the
            // posting lists at no more decodes than a window of a gather, the ten keys do not; a filtered read collects
            // the rows of its keys in one pass over the row groups that stops at the end of the read, which a window
            // would only bound
            final StringSink keys = new StringSink();
            keys.put(" AND s IN ('k0'");
            for (int i = 1; i < 150; i++) {
                keys.put(", 'k").put(i * 2).put('\'');
            }
            keys.put(") AND id % 3 = 0");
            final String[] predicates = {"", " AND s IN ('k0', 'k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8', 'k9')", keys.toString(), keys.toString()};
            final long[] limits = {RecordCursor.UNBOUNDED_ROW_COUNT, RecordCursor.UNBOUNDED_ROW_COUNT, RecordCursor.UNBOUNDED_ROW_COUNT, 100};
            final ParquetDecodeHint[] hints = {ParquetDecodeHint.SCATTERED, ParquetDecodeHint.CYCLIC, ParquetDecodeHint.MONOTONIC, ParquetDecodeHint.CYCLIC};
            final boolean[] isPrecomputed = {false, false, true, true};
            // the scans decode each row group, and so does the collection of the filtered read, the passes of the ten
            // keys only the row groups that hold them
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
                    Assert.assertEquals("the read precomputes its rows", isPrecomputed[i], stateSize != cursor.preComputedStateSize());
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
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 64);
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
                    for (long memoryLimit = 64 * 1024, divisor = 10; memoryLimit < 8 * 1024 * 1024; memoryLimit += memoryLimit / divisor) {
                        node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, memoryLimit);
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            TestUtils.assertCursor(expectedRows, cursor, factory.getMetadata(), true, actual);
                            successCount++;
                            divisor = 1;
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
            final long[] cacheSizes = {64 * 1024 * 1024L, 1024};
            final String[] intervals = {"ts IN '2024-01-01T00:00:03;10m;20m;2'", "ts IN '2024-01-01'", "ts IN '2024-01-03'"};
            final String[] orders = {"s, ts", "s DESC, ts DESC", "s, ts DESC", "s DESC, ts"};
            final String[] limits = {"", " LIMIT 5", " LIMIT -7", " LIMIT 13, 14", " LIMIT 3, 700", " LIMIT -700, -3"};
            final StringSink expected = new StringSink();
            // a gather in one window and a gather in windows over the row groups of the Parquet partition, whole or cut
            // by the intervals, and the passes of the keys over the native partition, which the read does not gather
            for (int p = 0; p < predicates.length; p++) {
                for (int l = 0; l < limits.length; l++) {
                    final long cacheBytes = cacheSizes[(p + l) / orders.length % cacheSizes.length];
                    node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, cacheBytes);
                    final String interval = intervals[(p + 2 * l) % intervals.length];
                    final String predicate = predicates[p];
                    final String limit = limits[l];
                    final String tail = "id, s, v, ts FROM g WHERE " + interval
                            + (predicate.isEmpty() ? "" : " AND " + predicate) + " ORDER BY " + orders[(p + l) % orders.length] + limit;
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
            // the 64 keys outnumber the rows of each row group, but the read returns the row of each key by row id from
            // its posting list, which takes neither a dense scan nor a map of the 4_097 index keys of the column
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
                Assert.assertEquals("the read does not gather its rows", stateSize, cursor.preComputedStateSize());
            }
        });
    }

    @Test
    public void testDirectReadDecodesEachRowGroupOnce() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            createSeriesTable();
            final String tail = "id, s, payload, ts FROM t WHERE ts IN '1970-01-01' ORDER BY s, ts";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(t) */ " + tail, expected);
            assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
            PageFrameRecordCursorImpl.testDirectModelRuns.set(0);
            PageFrameRecordCursorImpl.isWalkCounterEnabled = true;
            try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                final long stateSize = cursor.preComputedStateSize();
                final long chainBytesLo = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN);
                int count = 0;
                while (cursor.hasNext()) {
                    count++;
                }
                Assert.assertEquals(40_000, count);
                Assert.assertFalse("the read returns its rows from the page frames", cursor.getRecord() instanceof DelegatingRecord);
                Assert.assertEquals("the read does not gather its rows", stateSize, cursor.preComputedStateSize());
                Assert.assertEquals("the read copies no row", chainBytesLo, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN));
                final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
                Assert.assertEquals(ParquetDecodeHint.SCATTERED, pool.getDecodeHint());
                // the keys of a hundred consecutive series share their twenty row groups, so each of the 400 row groups
                // decodes once, the first of them when the read samples the rows
                Assert.assertEquals(400, pool.getDecodeCount());
                // the decode cache keeps 256 of the 400 row groups, so the read models the direct read and the gather
                // once to choose between them
                Assert.assertEquals(1, PageFrameRecordCursorImpl.testDirectModelRuns.get());
            } finally {
                PageFrameRecordCursorImpl.isWalkCounterEnabled = false;
            }
        });
    }

    @Test
    public void testDirectReadFallsBackToGatherUnderTightMemoryLimit() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            createSeriesTable();
            final String tail = "id, s, payload, ts FROM t WHERE ts IN '1970-01-01' ORDER BY s, ts";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(t) */ " + tail, expected);
            final String expectedRows = expected.toString();
            final StringSink actual = new StringSink();
            // the 256 row groups that the decode cache of the direct read keeps take 3.5 MB, more than half of the limit
            node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 6 * 1024 * 1024L);
            PageFrameRecordCursorImpl.testDenseScanFrames.set(0);
            PageFrameRecordCursorImpl.testFrameKeyCounts.set(0);
            PageFrameRecordCursorImpl.isWalkCounterEnabled = true;
            try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final Record record = cursor.getRecord();
                Assert.assertTrue("the read gathers its rows", record instanceof DelegatingRecord);
                long firstRowId = -1;
                CursorPrinter.println(factory.getMetadata(), actual);
                while (cursor.hasNext()) {
                    if (firstRowId < 0) {
                        firstRowId = record.getRowId();
                    }
                    CursorPrinter.println(record, factory.getMetadata(), actual);
                }
                TestUtils.assertEquals(expectedRows, actual);
                // the windows scan their row groups densely, which the posting lists of their keys mark in one pass each
                // rather than a count of the index for every key of the window in each row group
                Assert.assertTrue("the gather scans row groups densely", PageFrameRecordCursorImpl.testDenseScanFrames.get() > 0);
                Assert.assertEquals(0, PageFrameRecordCursorImpl.testFrameKeyCounts.get());
                final long usedBytes = sqlExecutionContext.getMemoryTracker().getUsed();
                // the move to a row by its row id after the last row drops the last window before it decodes
                final Record recordB = cursor.getRecordB();
                cursor.recordAt(recordB, firstRowId);
                Assert.assertEquals(1, recordB.getLong(0));
                Assert.assertTrue("the read drops its last window", sqlExecutionContext.getMemoryTracker().getUsed() < usedBytes);
            } finally {
                PageFrameRecordCursorImpl.isWalkCounterEnabled = false;
                node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
            }
        });
    }

    @Test
    public void testDirectReadLandsSkipsByCountsOfKeys() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            createSeriesTable();
            // the position p in key order is the row p % 20 of the key p / 20, whose id is the key plus one plus 2_000
            // for each row of the key before it; the skips land past the keys the read counted, inside them and on the
            // last rows of the read
            final long[] skips = {15_000, 5_005, 15_000, 39_995, 40_000, 0};
            final long[] limits = {10, 3, 10, 10, 10, 2};
            final long[] counts = {10, 3, 10, 5, 0, 2};
            try (RecordCursorFactory factory = select("SELECT id, s, payload, ts FROM t WHERE ts IN '1970-01-01' ORDER BY s, ts");
                 RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                final RecordCursor.Counter counter = new RecordCursor.Counter();
                for (int i = 0; i < skips.length; i++) {
                    cursor.toTop();
                    counter.set(skips[i]);
                    cursor.skipRows(counter, limits[i]);
                    Assert.assertEquals(0, counter.get());
                    final Record record = cursor.getRecord();
                    Assert.assertFalse("the read returns its rows from the page frames", record instanceof DelegatingRecord);
                    long position = skips[i];
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(position / 20 + 1 + 2_000 * (position % 20), record.getLong(0));
                        position++;
                        count++;
                    }
                    Assert.assertEquals(counts[i], count);
                }
            }
        });
    }

    @Test
    public void testDirectReadLimitsDecodeOnlyRowGroupsOfTheirRows() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            createSeriesTable();
            final String query = "id, s, payload, ts FROM t WHERE ts IN '1970-01-01' ORDER BY s, ts";
            final StringSink expected = new StringSink();
            for (String limit : new String[]{" LIMIT 10", " LIMIT 15_000, 15_010", " LIMIT -10", " LIMIT -15_010, -15_000", " LIMIT 39_995, 40_005"}) {
                printSql("SELECT /*+ no_index(t) */ " + query + limit, expected);
                assertQuery("SELECT " + query + limit).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
            }
            // the read skips 15_000 rows, the twenty rows of each of the first 750 keys, and returns ten rows of the key
            // 0750, which lie in the row groups 7, 27 and so on, besides the row group the read samples
            final long[] skips = {0, 15_000};
            final long[] decodes = {10, 11};
            final long[] firstIds = {1, 751};
            for (int i = 0; i < skips.length; i++) {
                PageFrameRecordCursorImpl.testDirectModelRuns.set(0);
                PageFrameRecordCursorImpl.isWalkCounterEnabled = true;
                try (RecordCursorFactory factory = select("SELECT " + query); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                    final RecordCursor.Counter counter = new RecordCursor.Counter();
                    counter.set(skips[i]);
                    cursor.skipRows(counter, 10);
                    Assert.assertEquals(0, counter.get());
                    final Record record = cursor.getRecord();
                    Assert.assertFalse("the read returns its rows from the page frames", record instanceof DelegatingRecord);
                    long id = firstIds[i];
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(id, record.getLong(0));
                        id += 2_000;
                        count++;
                    }
                    Assert.assertEquals(10, count);
                    final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
                    Assert.assertEquals(decodes[i], pool.getDecodeCount());
                    // ten rows lie in no more row groups than the scan has, so the read returns them by row id without a
                    // model of the gather, and decodes their row groups under MONOTONIC
                    Assert.assertEquals(0, PageFrameRecordCursorImpl.testDirectModelRuns.get());
                    Assert.assertEquals(ParquetDecodeHint.MONOTONIC, pool.getDecodeHint());
                    cursor.toTop();
                    counter.clear();
                    cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                    Assert.assertEquals(40_000, counter.get());
                } finally {
                    PageFrameRecordCursorImpl.isWalkCounterEnabled = false;
                }
            }
        });
    }

    @Test
    public void testExactGatherOfFixedSizeRowsPastQueryMemoryLimitKeepsItsRecords() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 64 * 1024L);
        assertMemoryLeak(() -> {
            createWidePayloadTable("x % 4", "true");
            final String tail = "s FROM w WHERE ts IN '1970-01-01' AND s IN ('k0', 'k1', 'k2', 'k3') ORDER BY s";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(w) */ " + tail, expected);
            final StringSink actual = new StringSink();
            try (RecordCursorFactory factory = select("SELECT " + tail)) {
                // a link, the SYMBOL and the designated timestamp take 20 bytes a row, 160_000 in all: the first limit
                // leaves the read room for its rows, and the second leaves the record chain fewer of them, so that it holds
                // the record the consumer took before the read moves on to the passes of the keys
                for (long limit : new long[]{1024 * 1024, 96 * 1024}) {
                    node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limit);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, actual);
                    } finally {
                        node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
                    }
                }
            }
        });
    }

    @Test
    public void testExactWindowOfWideKeysHandsReadToKeyPasses() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 256 * 1024L);
        assertMemoryLeak(() -> {
            // no filter, so the index orders the read and counts its rows; the widths are those of the filtered reads of
            // testGatherOfRowsWiderThanTheirSampleReadsKeysWithinBudget and testWindowOfWideKeysHandsReadToKeyPasses
            final String[] keys = {"x % 4", "x % 8"};
            final String[] areNarrow = {"x <= 100", "x % 8 < 6"};
            final int[] widths = {256, 512};
            final String[] filters = {" AND s IN ('k0', 'k1', 'k2', 'k3')", " AND s IN ('k0', 'k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7')"};
            final long[] heldRowCounts = {-1, 100};
            final long[] maxDecodes = {5 * 80, 9 * 80};
            for (int i = 0; i < keys.length; i++) {
                createWidePayloadTable(keys[i], areNarrow[i], widths[i]);
                final String tail = "id, s, payload, ts FROM w WHERE ts IN '1970-01-01'" + filters[i] + " ORDER BY s, ts";
                final StringSink expected = new StringSink();
                for (String limit : new String[]{"", " LIMIT 6_395, 6_405", " LIMIT -7"}) {
                    printSql("SELECT /*+ no_index(w) */ " + tail + limit, expected);
                    assertQuery("SELECT " + tail + limit).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
                }
                final String descendingTail = tail.replace("ORDER BY s, ts", "ORDER BY s DESC, ts DESC");
                printSql("SELECT /*+ no_index(w) */ " + descendingTail, expected);
                assertQuery("SELECT " + descendingTail).noLeakCheck().sizeMayVary().returns(expected);
                printSql("SELECT /*+ no_index(w) */ " + tail, expected);
                final String expectedRows = expected.toString();
                printSql("SELECT /*+ no_index(w) */ " + tail + " LIMIT 6_000, 8_000", expected);
                final String expectedSkippedRows = expected.toString();
                final long chainBytesLo = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN);
                try (RecordCursorFactory factory = select("SELECT " + tail)) {
                    try (RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                        final LongList rowIds = new LongList();
                        assertRowIds(expectedRows, factory, cursor, rowIds, heldRowCounts[i], chainBytesLo);
                        Assert.assertEquals("the passes of the keys hold no record chain", chainBytesLo, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN));
                        final long decodeCount = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool().getDecodeCount();
                        Assert.assertTrue("decodes: " + decodeCount, decodeCount <= maxDecodes[i]);
                        assertRecordAt(expectedRows, factory, cursor, rowIds);
                        assertRereads(expectedRows, expectedSkippedRows, factory, cursor, 6_000, 8_000);
                    }
                    assertFreshCursors(expectedRows, expectedSkippedRows, factory, 6_000, 8_000);
                }
                execute("DROP TABLE w");
            }
        });
    }

    @Test
    public void testDiscardedRowGroupsOfGatherRestartWithItsRead() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 256 * 1024L);
        assertMemoryLeak(() -> {
            // the first row group samples one-byte payloads, and the keys k2 and k3 hold wider ones after it: the first
            // pass outgrows its record chain about 40 row groups in, the first window then holds the narrow rows of k0 and
            // k1, and the second window, of the wide rows, outgrows its record chain about 50 row groups in
            createWidePayloadTable("x % 4", "x <= 100 OR x % 4 < 2", 101);
            final String tail = "id, s, payload, ts FROM w WHERE ts IN '1970-01-01' AND s IN ('k0', 'k1', 'k2', 'k3') AND id > 0 ORDER BY s, ts";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(w) */ " + tail, expected);
            final String expectedRows = expected.toString();
            final StringSink actual = new StringSink();
            try (RecordCursorFactory factory = select("SELECT " + tail)) {
                // the row groups that the two passes of one read discarded and the windows it has left outweigh the passes
                // of its last three keys
                try (RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                    TestUtils.assertCursor(expectedRows, cursor, factory.getMetadata(), true, actual);
                    final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
                    Assert.assertEquals("the read moves on to the passes of its keys", ParquetDecodeHint.CYCLIC, pool.getDecodeHint());
                }
                // the read that a first row started does not count towards the read from the top: it keeps its windows
                try (RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    cursor.toTop();
                    TestUtils.assertCursor(expectedRows, cursor, factory.getMetadata(), true, actual);
                    final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
                    Assert.assertEquals("the read keeps gathering its rows", ParquetDecodeHint.MONOTONIC, pool.getDecodeHint());
                }
            }
        });
    }

    @Test
    public void testExhaustedGatherAfterWarmRead() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 1024L);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT x id, ('k' || (x % 2_000))::SYMBOL s, timestamp_sequence(0, 1_000_000) ts
                        FROM long_sequence(40_000)
                    ), INDEX(s) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("INSERT INTO t VALUES (40_001, 'k1', '1970-01-02')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
            try (
                    RecordCursorFactory factory = select("SELECT id, s, ts FROM t WHERE ts IN '1970-01-01' ORDER BY s, ts");
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                Assert.assertTrue(cursor.hasNext());
                cursor.toTop();
                final RecordCursor.Counter counter = new RecordCursor.Counter();
                counter.set(40_000);
                cursor.skipRows(counter, 5);
                Assert.assertEquals(0, counter.get());
                Assert.assertFalse(cursor.hasNext());
                cursor.toTop();
                long count = 0;
                while (cursor.hasNext()) {
                    count++;
                }
                Assert.assertEquals(40_000, count);
            }
        });
    }

    @Test
    public void testFilteredCollectionStaysWithinQueryMemoryLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT x id, ('k' || (x % 4))::SYMBOL s, timestamp_sequence(0, 100_000) ts
                        FROM long_sequence(100_000)
                    ), INDEX(s) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            sqlExecutionContext.changePageFrameSizes(1_000, 1_000);
            final String tail = "id, s, ts FROM t WHERE ts IN '1970-01-01' AND s IN ('k0', 'k1', 'k2', 'k3') AND id % 2 = 0 ORDER BY s, ts";
            final String[] limits = {"", " LIMIT -7", " LIMIT 30_000, 30_007"};
            final String[] expected = new String[limits.length];
            final StringSink rows = new StringSink();
            for (int i = 0; i < limits.length; i++) {
                printSql("SELECT /*+ no_index(t) */ " + tail + limits[i], rows);
                expected[i] = rows.toString();
            }
            // the 50_000 rows the filter keeps take 200 KB in the collection, past the half of the limit that it may
            // take, so the passes of the keys read their rows past the page frames the collection went over
            node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 256 * 1024L);
            try {
                for (int i = 0; i < limits.length; i++) {
                    assertQuery("SELECT " + tail + limits[i]).noLeakCheck().sizeMayVary().inferTimestamp().withPlanContaining("symbolOrder: asc").returns(expected[i]);
                }
                try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    TestUtils.assertCursor(expected[0], cursor, factory.getMetadata(), true, rows);
                    Assert.assertTrue("the collection holds fewer rows than the read returns", sqlExecutionContext.getMemoryTracker().getUsed() < 50_000 * Integer.BYTES);
                }
            } finally {
                node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
            }
        });
    }

    @Test
    public void testFilteredDenseReadOverGatherBudgetCollectsItsRows() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 1_000);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 64 * 1024 * 1024L);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT x id, ('k' || (x % 2_000))::SYMBOL s, timestamp_sequence(0, 10_000_000) ts
                        FROM long_sequence(4_000)
                    ), INDEX(s) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("INSERT INTO t VALUES (4_001, 'k1', '1970-01-02')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
            // the 500 keys outnumber the rows of each of the 4 row groups enough for a dense scan of each, but the
            // decode cache keeps those row groups under any hint, which leaves the gather no budget past the entries of
            // a sort of the rows, so the read collects the rows its filter keeps in one pass over the row groups instead
            final StringSink keys = new StringSink();
            keys.put(" AND s IN ('k0'");
            for (int i = 1; i < 500; i++) {
                keys.put(", 'k").put(i * 4).put('\'');
            }
            keys.put(')');
            final String tail = "id, s, ts FROM t WHERE ts IN '1970-01-01'" + keys + " AND id % 3 = 0 ORDER BY s, ts";
            final StringSink expected = new StringSink();
            for (String limit : new String[]{"", " LIMIT 10", " LIMIT -10"}) {
                printSql("SELECT /*+ no_index(t) */ " + tail + limit, expected);
                PageFrameRecordCursorImpl.testDenseScanFrames.set(0);
                PageFrameRecordCursorImpl.isWalkCounterEnabled = true;
                try {
                    assertQuery("SELECT " + tail + limit).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
                } finally {
                    PageFrameRecordCursorImpl.isWalkCounterEnabled = false;
                }
                Assert.assertEquals("the read scans no row group", 0, PageFrameRecordCursorImpl.testDenseScanFrames.get());
            }
            try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                final long stateSize = cursor.preComputedStateSize();
                Assert.assertTrue(cursor.hasNext());
                Assert.assertNotEquals("the read collects its rows", stateSize, cursor.preComputedStateSize());
                Assert.assertFalse("the read returns its rows from the page frames", cursor.getRecord() instanceof DelegatingRecord);
            }
        });
    }

    @Test
    public void testFilteredGatherOfClusteredKeysPassesOnlyTheirRowGroups() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 256 * 1024L);
        assertMemoryLeak(() -> {
            // each key holds 20 of the 80 row groups in turn; the first row group holds one-byte payloads, which the
            // gather samples, and the 79 after it 256-byte ones
            createWidePayloadTable("(x - 1) / 2_000", "x <= 100");
            final String tail = "id, s, payload, ts FROM w WHERE ts IN '1970-01-01' AND s IN ('k0', 'k1', 'k2', 'k3') AND id > 0"
                    + " AND test_latched_counter() ORDER BY s, ts";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(w) */ " + tail, expected);
            assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
            final ChainPeak chainPeak = new ChainPeak();
            TestLatchedCounterFunctionFactory.reset(chainPeak);
            try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, new StringSink());
                Assert.assertTrue("record chain peak: " + chainPeak.peak, chainPeak.peak <= SORT_BYTES);
                // the gather passes over row groups until its rows outgrow its budget, and the pass of each key then
                // decodes only the 20 row groups that hold its rows
                final long decodeCount = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool().getDecodeCount();
                Assert.assertTrue("decodes: " + decodeCount, decodeCount <= 80 + 80);
            } finally {
                TestLatchedCounterFunctionFactory.reset(null);
            }
        });
    }

    @Test
    public void testFilteredGatherPassLandsPastKeptRowsOfItsFirstKey() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 256 * 1024L);
        assertMemoryLeak(() -> {
            // the filter keeps every row of the row group that samples it, and half of the others, so the first of the
            // five windows the sample plans ends at the row 600 of the key k1, which the filter keeps 506 rows of; the key
            // k2 holds wide rows, so the second window outgrows its budget, and the read lands on the first row of k2
            createWidePayloadTable("x % 8", "x % 8 != 2", 1_016);
            final String tail = "id, s, payload, ts FROM w WHERE ts IN '1970-01-01'"
                    + " AND s IN ('k0', 'k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7') AND (id <= 100 OR id % 16 < 8)"
                    + " AND test_latched_counter() ORDER BY s, ts";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(w) */ " + tail, expected);
            assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
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
    public void testFilteredLimitOfLateFirstKeyFiltersFewRowsOfOtherKeys() throws Exception {
        assertMemoryLeak(() -> {
            // the first key holds rows only in the last of the ten page frames
            execute("""
                    CREATE TABLE t AS (
                        SELECT x id, ('k' || (CASE WHEN x > 9_000 THEN x % 4 ELSE 1 + x % 3 END))::SYMBOL s, x::TIMESTAMP ts
                        FROM long_sequence(10_000)
                    ), INDEX(s) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            sqlExecutionContext.changePageFrameSizes(1_000, 1_000);
            final String tail = "id, s, ts FROM t WHERE s IN ('k0', 'k1', 'k2', 'k3') AND ts >= 0 AND ts < 10_001"
                    + " AND test_latched_counter() AND id % 7 < 3 ORDER BY s, ts";
            for (String limit : new String[]{" LIMIT 3", " LIMIT 500, 503"}) {
                final StringSink expected = new StringSink();
                printSql("SELECT /*+ no_index(t) */ " + tail + limit, expected);
                assertQuery("SELECT " + tail + limit).noLeakCheck().sizeMayVary().inferTimestamp().withPlanContaining("symbolOrder: asc").returns(expected);
            }
            TestLatchedCounterFunctionFactory.reset(null);
            printSql("SELECT " + tail + " LIMIT 3", new StringSink());
            // a sort of the three rows takes less memory than the rows the other keys keep in the first page frame, so
            // the read stops storing them there, and reads the rows of the first key by the index
            Assert.assertTrue("the read filters fewer rows than a page frame holds", TestLatchedCounterFunctionFactory.getCount() < 1_000);
        });
    }

    @Test
    public void testFilteredNegativeLimitFiltersRowsOfLastKeysOnly() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT x id, ('k' || (x % 4))::SYMBOL s, timestamp_sequence(0, 100_000) ts
                        FROM long_sequence(100_000)
                    ), INDEX(s) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            sqlExecutionContext.changePageFrameSizes(1_000, 1_000);
            final String tail = "id, s, ts FROM t WHERE ts IN '1970-01-01' AND s IN ('k0', 'k1', 'k2', 'k3')"
                    + " AND test_latched_counter() AND id % 3 = 0 ORDER BY s, ts";
            final String[] limits = {" LIMIT -7", " LIMIT -10, -3", " LIMIT -9_000", " LIMIT -20_000"};
            // the 25_000 rows of a key, which the filter reads to find the last rows the LIMIT selects among them, or
            // all 100_000 rows, when the last rows reach so far into the keys before them that one pass of all keys
            // costs less
            final long[] filteredRowCounts = {25_000, 25_000, 50_000, -1};
            final String[] expected = new String[limits.length];
            final StringSink rows = new StringSink();
            for (int i = 0; i < limits.length; i++) {
                printSql("SELECT /*+ no_index(t) */ " + tail + limits[i], rows);
                expected[i] = rows.toString();
            }
            // the 33_333 rows the filter keeps take 133 KB in the collection, past the half of the limit it may take
            node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 256 * 1024L);
            try {
                for (int i = 0; i < limits.length; i++) {
                    assertQuery("SELECT " + tail + limits[i]).noLeakCheck().sizeMayVary().inferTimestamp().withPlanContaining("symbolOrder: asc").returns(expected[i]);
                    TestLatchedCounterFunctionFactory.reset(null);
                    printSql("SELECT " + tail + limits[i], rows);
                    TestUtils.assertEquals(expected[i], rows);
                    if (filteredRowCounts[i] > 0) {
                        Assert.assertEquals(filteredRowCounts[i], TestLatchedCounterFunctionFactory.getCount());
                    } else {
                        Assert.assertTrue("the read filters every row", TestLatchedCounterFunctionFactory.getCount() >= 100_000);
                    }
                }
            } finally {
                node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
            }
        });
    }

    @Test
    public void testFilteredNegativeLimitOfDenseGatherGathersItsRows() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 1_000);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 64 * 1024 * 1024L);
        assertMemoryLeak(() -> {
            createWindowTable();
            // the 500 keys outnumber the rows of each of the 40 row groups enough for a dense scan of each, which a
            // gather in one window takes; the passes of the keys from the last key back would find the last rows of the
            // 40 that the filter keeps only past a third of the rows of the keys, and a collection of the rows of each
            // key in each row group would probe the index 500 times per row group
            final StringSink keys = new StringSink();
            keys.put(" AND s IN ('k0'");
            for (int i = 1; i < 500; i++) {
                keys.put(", 'k").put(i * 4).put('\'');
            }
            keys.put(')');
            final String tail = "id, s, ts FROM t WHERE ts IN '1970-01-01'" + keys + " AND id % 1_000 = 8 ORDER BY s, ts";
            final StringSink expected = new StringSink();
            for (String limit : new String[]{" LIMIT -10", " LIMIT -15, -5"}) {
                printSql("SELECT /*+ no_index(t) */ " + tail + limit, expected);
                PageFrameRecordCursorImpl.testDenseScanFrames.set(0);
                PageFrameRecordCursorImpl.isWalkCounterEnabled = true;
                try {
                    assertQuery("SELECT " + tail + limit).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
                } finally {
                    PageFrameRecordCursorImpl.isWalkCounterEnabled = false;
                }
                Assert.assertTrue("the gather scans row groups densely", PageFrameRecordCursorImpl.testDenseScanFrames.get() > 0);
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
    public void testFilteredPassesOfClusteredKeysDecodeTheRowGroupsTheyShareOnce() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 256 * 1024L);
        assertMemoryLeak(() -> {
            // 50 keys of 160 rows each over the 80 row groups, so that 40 of them share a row group with the key before it. A
            // pass of each key counted alone takes 120 row groups, more than the windows take, and shared once it takes 80.
            // Payloads of 256 bytes from the first row plan those windows; one byte up to the 1_000th row plans one window, which
            // outgrows the budget of about 390 KB by the 23rd row group, and the passes then read all row groups
            final String[] areNarrow = {"false", "x <= 1_000"};
            final int[] maxDecodes = {80, 30 + 80};
            for (int i = 0; i < areNarrow.length; i++) {
                createWidePayloadTable("lpad(((x - 1) / 160)::STRING, 2, '0')", areNarrow[i]);
                final String keyFilter = "ts IN '1970-01-01' AND s IN (" + paddedKeys(50) + ")";
                final String tail = "id, s, payload, ts FROM w WHERE " + keyFilter + " AND id > 0 ORDER BY s, ts";
                final StringSink expected = new StringSink();
                printSql("SELECT /*+ no_index(w) */ " + tail + " LIMIT -7", expected);
                assertQuery("SELECT " + tail + " LIMIT -7").noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
                printSql("SELECT /*+ no_index(w) */ " + tail + " LIMIT 4_000, 8_000", expected);
                final String expectedSkipped = expected.toString();
                printSql("SELECT /*+ no_index(w) */ " + tail, expected);
                final String expectedRows = expected.toString();
                assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expectedRows);
                try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                    final long chainBytesLo = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN);
                    TestUtils.assertCursor(expectedRows, cursor, factory.getMetadata(), true, new StringSink());
                    final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
                    Assert.assertEquals("the passes of the keys keep the cyclic walk", ParquetDecodeHint.CYCLIC, pool.getDecodeHint());
                    Assert.assertEquals("the passes of the keys hold no record chain", chainBytesLo, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN));
                    Assert.assertTrue("decodes: " + pool.getDecodeCount(), pool.getDecodeCount() <= maxDecodes[i]);
                    assertRereads(expectedRows, expectedSkipped, factory, cursor, 4_000, 8_000);
                }
                try (RecordCursorFactory factory = select("SELECT " + tail)) {
                    assertFreshCursors(expectedRows, expectedSkipped, factory, 4_000, 8_000);
                }
                final String overWindow = "SELECT *, row_number() OVER (PARTITION BY s ORDER BY ts) rn FROM (SELECT %s " + tail + ")";
                printSql(overWindow.formatted("/*+ no_index(w) */"), expected);
                assertQuery(overWindow.formatted("")).noLeakCheck().sizeMayVary().returns(expected);
                final String unfiltered = "id, s, payload, ts FROM w WHERE " + keyFilter + " ORDER BY s, ts";
                printSql("SELECT /*+ no_index(w) */ " + unfiltered, expected);
                assertQuery("SELECT " + unfiltered).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
                execute("DROP TABLE w");
            }
        });
    }

    @Test
    public void testFilteredPassesOfClusteredKeysReleaseTheirModelWhenCancelled() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 256 * 1024L);
        assertMemoryLeak(() -> {
            createWidePayloadTable("lpad(((x - 1) / 160)::STRING, 2, '0')", "false");
            final String tail = "id, s, payload, ts FROM w WHERE ts IN '1970-01-01' AND s IN (" + paddedKeys(50) + ") AND id > 0 ORDER BY s, ts";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(w) */ " + tail, expected);
            final long[] checks = {0, Long.MAX_VALUE};
            final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine) {
                @Override
                public void statefulThrowExceptionIfTrippedTimeThrottledOrYield() {
                    if (++checks[0] == checks[1]) {
                        cancel();
                    }
                    super.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
                }
            };
            try (RecordCursorFactory factory = select("SELECT " + tail)) {
                final SqlExecutionCircuitBreaker originalBreaker = sqlExecutionContext.getCircuitBreaker();
                ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
                try {
                    // the model asks the breaker last when a cursor opens, once for each of the 80 row groups, so a
                    // cancellation inside it holds no more than one just before it, when it had yet to open its list
                    try (RecordCursor ignored = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                        Assert.assertTrue("checks: " + checks[0], checks[0] > 80);
                    }
                    final long checkCount = checks[0];
                    final long[] nativeBytes = new long[2];
                    for (int i = 0; i < 2; i++) {
                        checks[0] = 0;
                        checks[1] = checkCount - (i == 0 ? 80 : 1);
                        breaker.reset();
                        try (RecordCursor ignored = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                            Assert.fail("expected cancellation");
                        } catch (CairoException e) {
                            Assert.assertTrue(e.isCancellation());
                        }
                        Assert.assertEquals(checks[1], checks[0]);
                        nativeBytes[i] = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
                    }
                    Assert.assertEquals(nativeBytes[0], nativeBytes[1]);
                } finally {
                    ((SqlExecutionContextImpl) sqlExecutionContext).with(originalBreaker);
                }
                try (RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                    TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, new StringSink());
                }
            }
        });
    }

    @Test
    public void testFilteredPassesOfClusteredKeysSkipKeysWithoutRows() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 200);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 256 * 1024L);
        assertMemoryLeak(() -> {
            // a key of the next day sorts after each of the 50 keys of 160 rows that hold the rows of the read over 40 row
            // groups, so that the keys of a row group are not consecutive keys of the read, yet no pass between them moves the
            // decode cache on; counted alone they take 80 row groups, more than the windows take, and shared once they take 40
            execute("CREATE TABLE w (id LONG, s SYMBOL INDEX, payload VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO w
                    SELECT x, ('k' || lpad(((x - 1) / 160)::STRING, 2, '0'))::SYMBOL, rpad('w', 256, 'w')::VARCHAR, timestamp_sequence(0, 1_000)
                    FROM long_sequence(8_000)
                    """);
            execute("""
                    INSERT INTO w
                    SELECT x, ('k' || lpad((x - 1)::STRING, 2, '0') || 'z')::SYMBOL, 'x'::VARCHAR, timestamp_sequence('1970-01-02', 1_000)
                    FROM long_sequence(50)
                    """);
            execute("ALTER TABLE w CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
            final StringSink keys = new StringSink();
            keys.put(paddedKeys(50));
            for (int i = 0; i < 50; i++) {
                keys.put(", 'k").put(i / 10).put(i % 10).put("z'");
            }
            final String tail = "id, s, payload, ts FROM w WHERE ts IN '1970-01-01' AND s IN (" + keys + ") AND id > 0 ORDER BY s, ts";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(w) */ " + tail, expected);
            assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
            try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, new StringSink());
                final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
                Assert.assertEquals("the passes of the keys keep the cyclic walk", ParquetDecodeHint.CYCLIC, pool.getDecodeHint());
                Assert.assertEquals(40, pool.getDecodeCount());
            }
        });
    }

    @Test
    public void testFilteredPassesOfClusteredKeysWithinTheProbesOfTheModel() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 256 * 1024L);
        assertMemoryLeak(() -> {
            // 64 keys over 80 row groups take exactly the 5_120 probes of the model that decoding the 80 costs at least
            createWidePayloadTable("lpad(((x - 1) / 125)::STRING, 2, '0')", "false");
            final String tail = "id, s, payload, ts FROM w WHERE ts IN '1970-01-01' AND s IN (" + paddedKeys(64) + ") AND id > 0 ORDER BY s, ts";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(w) */ " + tail, expected);
            assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
            try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, new StringSink());
                final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
                Assert.assertEquals("the passes of the keys keep the cyclic walk", ParquetDecodeHint.CYCLIC, pool.getDecodeHint());
                Assert.assertEquals(80, pool.getDecodeCount());
            }
        });
    }

    @Test
    public void testFilteredPassesOfInterleavedKeysKeepTheCyclicWalk() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 256 * 1024L);
        assertMemoryLeak(() -> {
            // the 2 keys of the read hold the 300 rows of the first 3 row groups, 2 windows of 1 KiB payloads where each key
            // fills a window, so the passes and the windows both decode 6 row groups. The keys share those 3, past the 2 that a
            // decode cache of 256 KiB keeps of row groups of about 106 KB, so no credit chose the passes
            createWidePayloadTable("CASE WHEN x <= 300 THEN x % 2 ELSE 9 END", "false", 1_024);
            final String tail = "id, s, payload, ts FROM w WHERE ts IN '1970-01-01' AND s IN ('k0', 'k1') AND id > 0 ORDER BY s, ts";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(w) */ " + tail, expected);
            assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
            try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, new StringSink());
                final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
                Assert.assertEquals("the passes of the keys keep the cyclic walk", ParquetDecodeHint.CYCLIC, pool.getDecodeHint());
                Assert.assertTrue("decodes: " + pool.getDecodeCount(), pool.getDecodeCount() <= 6);
            }
        });
    }

    @Test
    public void testFilteredPassesOfKeysThatShareTheirRowGroupsKeepThemDecoded() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 200);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 256 * 1024L);
        assertMemoryLeak(() -> {
            // each 4 of the 80 keys hold the rows of a pair of the 40 row groups, which a decode cache of 4 row groups keeps
            // across their passes when it gives up the row group it used first; a cyclic walk would decode them for each key
            createWidePayloadTable("lpad(((x - 1) / 400 * 4 + (x - 1) % 4)::STRING, 2, '0')", "false");
            final String tail = "id, s, payload, ts FROM w WHERE ts IN '1970-01-01' AND s IN (" + paddedKeys(80) + ") AND id > 0 ORDER BY s, ts";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(w) */ " + tail, expected);
            assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
            try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, new StringSink());
                final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
                Assert.assertEquals("the passes of the keys read the rows", ParquetDecodeHint.SCATTERED, pool.getDecodeHint());
                Assert.assertEquals(40, pool.getDecodeCount());
            }
        });
    }

    @Test
    public void testFilteredWindowsOfClusteredKeysPastTheProbesOfTheModel() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 256 * 1024L);
        assertMemoryLeak(() -> {
            // 65 keys over 80 row groups take 5_200 probes of the model, over the 5_120 that decoding the 80 costs at least,
            // which is more than the 4_000 of half a probe per row, where 64 keys take the passes
            createWidePayloadTable("lpad(((x - 1) / 124)::STRING, 2, '0')", "false");
            final String tail = "id, s, payload, ts FROM w WHERE ts IN '1970-01-01' AND s IN (" + paddedKeys(65) + ") AND id > 0 ORDER BY s, ts";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(w) */ " + tail, expected);
            assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
            try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, new StringSink());
                final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
                Assert.assertEquals("the read keeps gathering its rows", ParquetDecodeHint.MONOTONIC, pool.getDecodeHint());
            }
        });
    }

    @Test
    public void testFilteredWindowsOfInterleavedKeysCountEachRowGroupPerKey() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 256 * 1024L);
        assertMemoryLeak(() -> {
            // each of the 40 keys holds rows in each of the 80 row groups, which the decode cache does not keep from one key to
            // the next, so the passes decode 3_200 row groups where each window decodes the 80 once
            createWidePayloadTable("lpad((x % 40)::STRING, 2, '0')", "false");
            final String tail = "id, s, payload, ts FROM w WHERE ts IN '1970-01-01' AND s IN (" + paddedKeys(40) + ") AND id > 0 ORDER BY s, ts";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(w) */ " + tail, expected);
            assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
            try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, new StringSink());
                final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
                Assert.assertEquals("the read keeps gathering its rows", ParquetDecodeHint.MONOTONIC, pool.getDecodeHint());
                Assert.assertTrue("decodes: " + pool.getDecodeCount(), pool.getDecodeCount() < 3_200 / 2);
            }
        });
    }

    @Test
    public void testFilteredWindowsOfKeysThatAlternateRowGroups() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 200);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 256 * 1024L);
        assertMemoryLeak(() -> {
            // each 10 of the 80 keys hold the rows of 5 of the 40 row groups, 2 keys to a row group and 5 keys apart, so that the
            // passes of the keys go round the 5 row groups twice past a decode cache of 4, and decode one for each key
            createWidePayloadTable("lpad(((x - 1) / 1_000 * 10 + (x - 1) / 200 % 5 + (x - 1) % 200 / 100 * 5)::STRING, 2, '0')", "false");
            final String tail = "id, s, payload, ts FROM w WHERE ts IN '1970-01-01' AND s IN (" + paddedKeys(80) + ") AND id > 0 ORDER BY s, ts";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(w) */ " + tail, expected);
            assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
            try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, new StringSink());
                final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
                Assert.assertEquals("the read keeps gathering its rows", ParquetDecodeHint.MONOTONIC, pool.getDecodeHint());
                Assert.assertTrue("decodes: " + pool.getDecodeCount(), pool.getDecodeCount() < 80);
            }
        });
    }

    @Test
    public void testFilteredWindowsOfManyKeysSurviveRowsWiderThanTheirSample() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 256 * 1024L);
        assertMemoryLeak(() -> {
            // payloads of one byte, then 64, then 256, so that the rows outgrow the window their sample plans, and then the
            // windows that the first rows to outgrow it plan; 100 of the 256 keys hold rows in each of the 80 row groups
            execute("CREATE TABLE w (id LONG, s SYMBOL INDEX, payload VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO w
                    SELECT x, ('k' || lpad((x % 256)::STRING, 3, '0'))::SYMBOL,
                        (CASE WHEN x <= 1_000 THEN 'a' WHEN x <= 4_000 THEN rpad('m', 64, 'm') ELSE rpad('w', 256, 'w') END)::VARCHAR,
                        timestamp_sequence(0, 1_000)
                    FROM long_sequence(8_000)
                    """);
            execute("INSERT INTO w VALUES (8_001, 'k000', 'x', '1970-01-02')");
            execute("ALTER TABLE w CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
            final StringSink keys = new StringSink();
            keys.put("'k000'");
            for (int i = 1; i < 256; i++) {
                keys.put(", 'k").put(i / 100).put(i / 10 % 10).put(i % 10).put('\'');
            }
            final String tail = "id, s, payload, ts FROM w WHERE ts IN '1970-01-01' AND s IN (" + keys + ") AND id > 0 ORDER BY s, ts";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(w) */ " + tail, expected);
            assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
            try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, new StringSink());
                final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
                Assert.assertEquals("the read keeps gathering its rows", ParquetDecodeHint.MONOTONIC, pool.getDecodeHint());
                // a pass of each key would decode each row group once for each of the 100 keys that hold rows in it
                Assert.assertTrue("decodes: " + pool.getDecodeCount(), pool.getDecodeCount() < 100 * 80);
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
            createWideningPayloadTable();
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
            for (int i = 0; i < tails.length; i++) {
                final String tail = tails[i];
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
                }
                if (i == 0 || i == tails.length - 1) {
                    assertQuery("SELECT id, s, payload, ts FROM " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expectedRows);
                }
            }
        });
    }

    @Test
    public void testBoundedReadOfRowsWiderThanTheirSampleKeepsItsRowGroupsWithinMemoryLimit() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 16 * 1024 * 1024L);
        assertMemoryLeak(() -> {
            // by the one-byte payloads of the first of the 50 row groups they stay decoded within a query memory limit of
            // 4 MiB; the 1 KiB payloads after it decode to about 5.3 MB, which fits the decode cache but not the limit
            createCachedRowGroupsTable(5_000, "x <= 100", 1_024);
            final String tail = "id, s, payload, ts FROM w WHERE ts IN '1970-01-01' AND s IN (" + cachedRowGroupKeys() + ") AND id > 0 ORDER BY s, ts LIMIT 4_000, 4_010";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(w) */ " + tail, expected);
            node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 4 * 1024 * 1024L);
            try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, new StringSink());
            } finally {
                node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
            }
        });
    }

    @Test
    public void testGatherOfRowsThatFitDecodeCacheDecodesEachRowGroupOnce() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 2_655_000L);
        assertMemoryLeak(() -> {
            // the 2_500 rows decode to 2.65 MB, 36 bytes of fixed-size values and a payload of 1 KiB each, which this decode
            // cache keeps; only their record chain also takes the header of 4 bytes of each payload, which would make them
            // 2.66 MB and cost a read in windows of each row group many times
            for (String filter : new String[]{" AND id > 0", ""}) {
                createCachedRowGroupsTable(2_500, "false", 1_024);
                final String tail = "id, s, payload, ts FROM w WHERE ts IN '1970-01-01' AND s IN (" + cachedRowGroupKeys() + ")" + filter + " ORDER BY s, ts";
                final StringSink expected = new StringSink();
                printSql("SELECT /*+ no_index(w) */ " + tail, expected);
                assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
                try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                    final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
                    TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, new StringSink());
                    Assert.assertEquals(25, pool.getDecodeCount());
                    cursor.toTop();
                    TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, new StringSink());
                    Assert.assertEquals(25, pool.getDecodeCount());
                }
                execute("DROP TABLE w");
            }
        });
    }

    @Test
    public void testGatherOfRowsThatOutgrowDecodeCacheKeepsItsWindows() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 256 * 1024L);
        assertMemoryLeak(() -> {
            // by the sizes of the column types the 25 row groups stay decoded, and by their 1 KiB payloads they do not: the
            // 2_500 rows take about 2.7 MB in the record chain, 20 windows of its 136 KB, where a pass of each of the 40 keys
            // would decode each row group once per key; a filtered read learns it when its first window outgrows its record
            // chain, and any read from a sample of those payloads
            final String[] areNarrow = {"x <= 100", "false", "false"};
            final String[] filters = {" AND id > 0", " AND id > 0", ""};
            for (int i = 0; i < areNarrow.length; i++) {
                createCachedRowGroupsTable(2_500, areNarrow[i], 1_024);
                final String tail = "id, s, payload, ts FROM w WHERE ts IN '1970-01-01' AND s IN (" + cachedRowGroupKeys() + ")" + filters[i] + " ORDER BY s, ts";
                final StringSink expected = new StringSink();
                for (String limit : new String[]{" LIMIT 100, 110", " LIMIT -7"}) {
                    printSql("SELECT /*+ no_index(w) */ " + tail + limit, expected);
                    assertQuery("SELECT " + tail + limit).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
                }
                printSql("SELECT /*+ no_index(w) */ " + tail, expected);
                final String expectedRows = expected.toString();
                assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expectedRows);
                try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                    TestUtils.assertCursor(expectedRows, cursor, factory.getMetadata(), true, new StringSink());
                    final long decodeCount = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool().getDecodeCount();
                    // 24 windows leave room for the rows of a key that fill a window unevenly; the passes would take 1_000
                    Assert.assertTrue("decodes: " + decodeCount, decodeCount <= 24 * 25);
                }
                execute("DROP TABLE w");
            }
        });
    }

    @Test
    public void testGatherOfRowsThatOutgrowMemoryCapKeepsItsWindows() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 16 * 1024 * 1024L);
        assertMemoryLeak(() -> {
            // the decode cache holds the 25 row groups of 1 KiB payloads, about 2.7 MB, but a query memory limit of 4 MiB leaves
            // half of it to them, so a read of each of the 40 keys would decode them for each key
            createCachedRowGroupsTable(2_500, "false", 1_024);
            final String tail = "id, s, payload, ts FROM w WHERE ts IN '1970-01-01' AND s IN (" + cachedRowGroupKeys() + ") AND id > 0 ORDER BY s, ts";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(w) */ " + tail, expected);
            final long chainBytesLo = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN);
            node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 4 * 1024 * 1024L);
            try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, new StringSink());
                Assert.assertTrue("the read keeps gathering its rows", Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN) > chainBytesLo);
            } finally {
                node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
            }
        });
    }

    @Test
    public void testGatherOfRowsWiderThanTheirSampleKeepsRowGroupsThatStayDecoded() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 256 * 1024L);
        assertMemoryLeak(() -> {
            // the one window that the one-byte payloads of the first of the 20 row groups plan outgrows its record chain
            // about half way through them, whose 64-byte payloads still stay decoded across the keys
            createCachedRowGroupsTable(2_000, "x <= 100", 64);
            final String tail = "id, s, payload, ts FROM w WHERE ts IN '1970-01-01' AND s IN (" + cachedRowGroupKeys() + ") AND id > 0 ORDER BY s, ts";
            final StringSink expected = new StringSink();
            for (String limit : new String[]{"", " LIMIT 1_000, 1_010", " LIMIT -7"}) {
                printSql("SELECT /*+ no_index(w) */ " + tail + limit, expected);
                assertQuery("SELECT " + tail + limit).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
            }
            printSql("SELECT /*+ no_index(w) */ " + tail + " LIMIT 1_000, 2_000", expected);
            final String expectedSkippedRows = expected.toString();
            printSql("SELECT /*+ no_index(w) */ " + tail, expected);
            final String expectedRows = expected.toString();
            final StringSink actual = new StringSink();
            try (RecordCursorFactory factory = select("SELECT " + tail)) {
                PageFrameRecordCursorImpl.testReadFrames.set(0);
                PageFrameRecordCursorImpl.isWalkCounterEnabled = true;
                try (RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                    final LongList rowIds = new LongList();
                    assertRowIds(expectedRows, factory, cursor, rowIds, -1, 0);
                    final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
                    // each row group decodes once, whichever pass reached it first
                    Assert.assertEquals(20, pool.getDecodeCount());
                    // the one pass of the collection goes over each of them once, where the passes of the keys go over them
                    // once for each of the 40 keys
                    Assert.assertEquals(20, PageFrameRecordCursorImpl.testReadFrames.get());
                    cursor.toTop();
                    TestUtils.assertCursor(expectedRows, cursor, factory.getMetadata(), true, actual);
                    Assert.assertEquals("the row groups stay decoded", 20, pool.getDecodeCount());
                    Assert.assertEquals("the read keeps the rows it collected", 20, PageFrameRecordCursorImpl.testReadFrames.get());
                    assertRecordAt(expectedRows, factory, cursor, rowIds);
                    assertRereads(expectedRows, expectedSkippedRows, factory, cursor, 1_000, 2_000);
                } finally {
                    PageFrameRecordCursorImpl.isWalkCounterEnabled = false;
                }
                assertFreshCursors(expectedRows, expectedSkippedRows, factory, 1_000, 2_000);
            }
        });
    }

    @Test
    public void testGatherOfRowsWiderThanTheirSampleOverCachedRowGroupsPassesItsKeys() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 256 * 1024L);
        assertMemoryLeak(() -> {
            // the 40 keys outnumber a third of the rows of each of the 12 row groups, so the read gathers them, in the one
            // window that the one-byte payloads of the first row group plan; the row groups of 187-byte payloads after it
            // outgrow that window, but they stay decoded across the passes of the keys, as they do under a sort of the rows
            execute("CREATE TABLE w (id LONG, s SYMBOL INDEX, payload VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO w
                    SELECT x, ('k' || lpad((x % 40)::STRING, 2, '0'))::SYMBOL,
                        (CASE WHEN x <= 100 THEN 'a' ELSE rpad('w', 187, 'w') END)::VARCHAR, timestamp_sequence(0, 1_000)
                    FROM long_sequence(1_200)
                    """);
            execute("INSERT INTO w VALUES (1_201, 'k00', 'x', '1970-01-02')");
            execute("ALTER TABLE w CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
            final StringSink keys = new StringSink();
            keys.put("'k00'");
            for (int i = 1; i < 40; i++) {
                keys.put(", 'k").put(i / 10).put(i % 10).put('\'');
            }
            final String tail = "id, s, payload, ts FROM w WHERE ts IN '1970-01-01' AND s IN (" + keys + ") AND id > 0 ORDER BY s, ts";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(w) */ " + tail, expected);
            final String expectedRows = expected.toString();
            assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expectedRows);
            final StringSink actual = new StringSink();
            try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                TestUtils.assertCursor(expectedRows, cursor, factory.getMetadata(), true, actual);
                final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
                final long decodeCount = pool.getDecodeCount();
                // the pass that outgrew its window, and one pass over the 12 row groups
                Assert.assertTrue("decodes: " + decodeCount, decodeCount <= 2 * 12);
                cursor.toTop();
                TestUtils.assertCursor(expectedRows, cursor, factory.getMetadata(), true, actual);
                Assert.assertEquals("the row groups stay decoded", decodeCount, pool.getDecodeCount());
            }
        });
    }

    @Test
    public void testGatherOfRowsWiderThanTheirSampleReadsKeysWithinBudget() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 256 * 1024L);
        assertMemoryLeak(() -> {
            // the first of the 80 row groups holds one-byte payloads, which the gather samples, and the 79 after it hold
            // 256-byte ones, so that the rows take seven windows of the budget the sample plans one for
            createWidePayloadTable("x % 4", "x <= 100");
            final String tail = "id, s, payload, ts FROM w WHERE ts IN '1970-01-01' AND s IN ('k0', 'k1', 'k2', 'k3') AND id > 0"
                    + " AND test_latched_counter() ORDER BY s, ts";
            final StringSink expected = new StringSink();
            for (String limit : new String[]{"", " LIMIT 3_000, 3_005", " LIMIT -7"}) {
                printSql("SELECT /*+ no_index(w) */ " + tail + limit, expected);
                assertQuery("SELECT " + tail + limit).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
            }
            final String descendingTail = tail.replace("ORDER BY s, ts", "ORDER BY s DESC, ts DESC");
            printSql("SELECT /*+ no_index(w) */ " + descendingTail, expected);
            assertQuery("SELECT " + descendingTail).noLeakCheck().sizeMayVary().returns(expected);
            printSql("SELECT /*+ no_index(w) */ " + tail, expected);
            final String expectedRows = expected.toString();
            printSql("SELECT /*+ no_index(w) */ " + tail + " LIMIT 5_000, 8_000", expected);
            final String expectedSkippedRows = expected.toString();
            final ChainPeak chainPeak = new ChainPeak();
            TestLatchedCounterFunctionFactory.reset(chainPeak);
            try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                final LongList rowIds = new LongList();
                assertRowIds(expectedRows, factory, cursor, rowIds, -1, chainPeak.chainBytesLo);
                Assert.assertTrue("record chain peak: " + chainPeak.peak, chainPeak.peak <= SORT_BYTES);
                Assert.assertEquals("the passes of the keys hold no record chain", chainPeak.chainBytesLo, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN));
                // a sort filters each of the 80 row groups once and reads the rows of each of the 4 keys back from them
                final long decodeCount = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool().getDecodeCount();
                Assert.assertTrue("decodes: " + decodeCount, decodeCount <= 5 * 80);
                assertRecordAt(expectedRows, factory, cursor, rowIds);
                assertRereads(expectedRows, expectedSkippedRows, factory, cursor, 5_000, 8_000);
            } finally {
                TestLatchedCounterFunctionFactory.reset(null);
            }
            try (RecordCursorFactory factory = select("SELECT " + tail)) {
                assertFreshCursors(expectedRows, expectedSkippedRows, factory, 5_000, 8_000);
            }
        });
    }

    @Test
    public void testGatherOfRowsWiderThanTheirSampleReleasesMemoryWhenPassFails() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 256 * 1024L);
        assertMemoryLeak(() -> {
            createWidePayloadTable("x % 4", "x <= 100");
            final String tail = "id, s, payload, ts FROM w WHERE ts IN '1970-01-01' AND s IN ('k0', 'k1', 'k2', 'k3') AND id > 0"
                    + " AND test_latched_counter() ORDER BY s, ts";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(w) */ " + tail, expected);
            // the record chain fills within 1_500 rows, so the filter fails in the pass of the second key
            final long chainBytesLo = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN);
            TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
                @Override
                public boolean onGet(Record rec, int count) {
                    if (count == 4_000) {
                        throw CairoException.nonCritical().put("filter failed");
                    }
                    return true;
                }
            });
            try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                try {
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                        // read until the filter fails
                    }
                    Assert.fail("expected the filter to fail");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "filter failed");
                }
                Assert.assertEquals("the passes of the keys hold no record chain", chainBytesLo, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN));
            } finally {
                TestLatchedCounterFunctionFactory.reset(null);
            }
            assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
        });
    }

    @Test
    public void testGatherOfStaircaseRowsCountsDiscardedRowGroupsAgainstItsWindows() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 256 * 1024L);
        assertMemoryLeak(() -> {
            // each step of the payloads comes after the windows that the step before it plans take few enough row groups
            // to keep gathering, so that each window outgrows its record chain late in its pass over the 80 row groups
            createPayloadTable("s SYMBOL INDEX", "x % 4", STAIRCASE_PAYLOAD);
            for (String filter : new String[]{" AND id > 0", ""}) {
                final String tail = "id, s, payload, ts FROM w WHERE ts IN '1970-01-01' AND s IN ('k0', 'k1', 'k2', 'k3')" + filter + " ORDER BY s, ts";
                final StringSink expected = new StringSink();
                printSql("SELECT /*+ no_index(w) */ " + tail, expected);
                final String expectedRows = expected.toString();
                assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expectedRows);
                final StringSink actual = new StringSink();
                long decodeCount = -1;
                try (RecordCursorFactory factory = select("SELECT " + tail)) {
                    for (int i = 0; i < 2; i++) {
                        try (RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                            TestUtils.assertCursor(expectedRows, cursor, factory.getMetadata(), true, actual);
                            final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
                            // the row groups that the windows discarded move the read on to the passes of its 4 keys
                            // before it decodes more than a sort, which scans the 80 row groups once and reads back the
                            // rows of each key from them
                            Assert.assertTrue("decodes: " + pool.getDecodeCount(), pool.getDecodeCount() <= (4 + 1) * 80);
                            Assert.assertTrue("a new read counts anew", i == 0 || pool.getDecodeCount() == decodeCount);
                            decodeCount = pool.getDecodeCount();
                            cursor.toTop();
                            TestUtils.assertCursor(expectedRows, cursor, factory.getMetadata(), true, actual);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testGatherOfStaircaseRowsWithoutCountsOfIndexStopsAtScanOfDiscardedRowGroups() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 256 * 1024L);
        assertMemoryLeak(() -> {
            // the index cannot count the rows of a key in a partition of a posting index, so the model of the windows
            // cannot place them, and the read moves on to the passes of its keys once they discarded a scan of row groups
            createPayloadTable("s SYMBOL INDEX TYPE POSTING", "x % 4", STAIRCASE_PAYLOAD);
            final String tail = "id, s, payload, ts FROM w WHERE ts IN '1970-01-01' AND s IN ('k0', 'k1', 'k2', 'k3') ORDER BY s, ts";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(w) */ " + tail, expected);
            final String expectedRows = expected.toString();
            assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expectedRows);
            printSql("SELECT /*+ no_index(w) */ " + tail + " LIMIT 5_000, 8_000", expected);
            final String expectedSkippedRows = expected.toString();
            final long chainBytesLo = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN);
            try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                TestUtils.assertCursor(expectedRows, cursor, factory.getMetadata(), true, new StringSink());
                Assert.assertEquals("the passes of the keys hold no record chain", chainBytesLo, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN));
                assertRereads(expectedRows, expectedSkippedRows, factory, cursor, 5_000, 8_000);
            }
        });
    }

    @Test
    public void testGatherOfUniformRowsKeepsOneWindow() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 256 * 1024L);
        assertMemoryLeak(() -> {
            // every payload takes one byte, as the first row group samples, so the rows fit the one window it plans
            createWidePayloadTable("x % 4", "true");
            final String tail = "id, s, payload, ts FROM w WHERE ts IN '1970-01-01' AND s IN ('k0', 'k1', 'k2', 'k3') AND id > 0 ORDER BY s, ts";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(w) */ " + tail, expected);
            assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
            final long chainBytesLo = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN);
            try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, new StringSink());
                Assert.assertTrue("the gather holds the rows", Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN) > chainBytesLo);
                // the sample decodes the first of the 80 row groups, and the one pass of the gather decodes the others
                Assert.assertEquals(80, ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool().getDecodeCount());
            }
        });
    }

    @Test
    public void testGatherOfUniformRowsTakesFewerWindowsThanKeys() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 256 * 1024L);
        assertMemoryLeak(() -> {
            // every payload takes 49 bytes, so the rows take three windows of the budget, fewer than the four keys
            createWidePayloadTable("x % 4", "false", 49);
            final String tail = "id, s, payload, ts FROM w WHERE ts IN '1970-01-01' AND s IN ('k0', 'k1', 'k2', 'k3') AND id > 0 ORDER BY s, ts";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(w) */ " + tail, expected);
            assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
            try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, new StringSink());
                // each of the three windows decodes each of the 80 row groups once, the first of them after the sample
                Assert.assertEquals(3 * 80, ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool().getDecodeCount());
            }
        });
    }

    @Test
    public void testGatherOfUniformRowsTiesWindowsWithKeyPasses() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 256 * 1024L);
        assertMemoryLeak(() -> {
            // every payload takes 93 bytes, so the rows take four windows of the budget, as many as the keys: both decode
            // each of the 80 row groups four times, and the passes copy no payload
            createWidePayloadTable("x % 4", "false", 93);
            final String tail = "id, s, payload, ts FROM w WHERE ts IN '1970-01-01' AND s IN ('k0', 'k1', 'k2', 'k3') AND id > 0"
                    + " AND test_latched_counter() ORDER BY s, ts";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(w) */ " + tail, expected);
            assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
            final ChainPeak chainPeak = new ChainPeak();
            TestLatchedCounterFunctionFactory.reset(chainPeak);
            try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, new StringSink());
                Assert.assertEquals("the passes copy no row", 0, chainPeak.peak);
                final long decodeCount = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool().getDecodeCount();
                Assert.assertTrue("decodes: " + decodeCount, decodeCount <= 4 * 80);
            } finally {
                TestLatchedCounterFunctionFactory.reset(null);
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
    public void testGatherRandomAccessFitsQueryMemoryLimit() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 8);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 16 * 1024L);
        assertMemoryLeak(() -> {
            createWideningPayloadTable();
            final String tail = "id, s, payload, ts FROM r WHERE ts IN '1970-01-01' AND s IN ('k000', 'k001', 'k002', 'k100', 'k199') AND id > 0 ORDER BY s, ts";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(r) */ " + tail, expected);
            final StringSink actual = new StringSink();
            final LongList rowIds = new LongList();
            // a sort of the rows fits the limit
            node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 224 * 1024L);
            try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final Record record = cursor.getRecord();
                Assert.assertTrue("the read gathers its rows", record instanceof DelegatingRecord);
                while (cursor.hasNext()) {
                    rowIds.add(record.getRowId());
                }
                final Record recordB = cursor.getRecordB();
                CursorPrinter.println(factory.getMetadata(), actual);
                for (int i = 0, n = rowIds.size(); i < n; i++) {
                    cursor.recordAt(recordB, rowIds.getQuick(i));
                    CursorPrinter.println(recordB, factory.getMetadata(), actual);
                }
                TestUtils.assertEquals(expected, actual);
            } finally {
                node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
            }
        });
    }

    @Test
    public void testKeysOfColumnAddedAfterRowsMatchNonIndexedRead() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            // the first day predates the column, so that its partition has no index of it, and the index cannot count
            // the rows of a key there; the third day is a Parquet partition
            execute("CREATE TABLE t (id LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT x, timestamp_sequence(0, 100_000_000) FROM long_sequence(800)");
            execute("ALTER TABLE t ADD COLUMN s SYMBOL INDEX");
            execute("""
                    INSERT INTO t
                    SELECT x + 800, timestamp_sequence(86_400_000_000, 108_000_000), ('k' || (x % 300))::SYMBOL
                    FROM long_sequence(1_600)
                    """);
            execute("INSERT INTO t VALUES (2_401, '1970-01-04', 'k1')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '1970-01-03'");
            // the native partitions of the first two days, which the passes of the keys read, and all three days, which
            // the read gathers
            final String[] intervals = {"ts < '1970-01-03'", "ts < '1970-01-04'"};
            final String[] predicates = {"", " AND s IN ('k1', 'k2', NULL)", " AND s IN ('k1', 'k2', NULL) AND id > 0"};
            final String[] limits = {"", " LIMIT 900, 910", " LIMIT -10", " LIMIT 5"};
            final StringSink expected = new StringSink();
            for (String interval : intervals) {
                for (String predicate : predicates) {
                    for (String limit : limits) {
                        final String tail = "id, s, ts FROM t WHERE " + interval + predicate + " ORDER BY s, ts" + limit;
                        printSql("SELECT /*+ no_index(t) */ " + tail, expected);
                        assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
                    }
                }
            }
        });
    }

    @Test
    public void testKeysOfPostingIndexMatchNonIndexedRead() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 1024L);
        assertMemoryLeak(() -> {
            // the index cannot count the rows of a key in a partition of a posting index, so that the read gathers the
            // rows of its 100 keys without their counts, and lands its skips row by row
            execute("""
                    CREATE TABLE t AS (
                        SELECT x id, ('k' || (x % 300))::SYMBOL s, timestamp_sequence(0, 100_000_000) ts
                        FROM long_sequence(1_600)
                    ), INDEX(s TYPE POSTING) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
            final StringSink keys = new StringSink();
            keys.put(" AND s IN ('k0'");
            for (int i = 1; i < 100; i++) {
                keys.put(", 'k").put(i * 3).put('\'');
            }
            keys.put(')');
            final String[] predicates = {keys.toString(), keys + " AND id > 0"};
            final String[] limits = {"", " LIMIT 100, 110", " LIMIT -10", " LIMIT 5"};
            final StringSink expected = new StringSink();
            for (String predicate : predicates) {
                for (String limit : limits) {
                    final String tail = "id, s, ts FROM t WHERE ts < '1970-01-02'" + predicate + " ORDER BY s, ts" + limit;
                    printSql("SELECT /*+ no_index(t) */ " + tail, expected);
                    assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().withPlanContaining("symbolOrder: asc").returns(expected);
                }
            }
        });
    }

    @Test
    public void testLimitFromLastRowOnMatchesNonIndexedRead() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 8);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 1024L);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT x id, ('k' || (x % 4))::SYMBOL s, timestamp_sequence(0, 864_000_000) ts
                        FROM long_sequence(200)
                    ), INDEX(s) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
            // the Parquet partition, which the read gathers, and the native partition, which the passes of the keys read
            final String[] intervals = {"ts IN '1970-01-01'", "ts IN '1970-01-02'"};
            final String[] predicates = {"", " AND s IN ('k0', 'k1')", " AND s IN ('k0', 'k1') AND id > 0"};
            final long[] rowCounts = {100, 50, 50};
            final String[] orders = {"s, ts", "s DESC, ts DESC"};
            final StringSink expected = new StringSink();
            final StringSink actual = new StringSink();
            for (int p = 0; p < predicates.length; p++) {
                final long rowCount = rowCounts[p];
                for (String interval : intervals) {
                    for (String order : orders) {
                        final String query = "id, s, ts FROM t WHERE " + interval + predicates[p] + " ORDER BY " + order;
                        final String[] limits = {
                                " LIMIT " + (rowCount - 1) + ", " + (rowCount + 4),
                                " LIMIT " + rowCount + ", " + (rowCount + 5),
                                " LIMIT " + (rowCount + 1) + ", " + (rowCount + 6),
                                " LIMIT -" + (rowCount + 5) + ", -" + rowCount
                        };
                        for (String limit : limits) {
                            printSql("SELECT /*+ no_index(t) */ " + query + limit, expected);
                            assertQuery("SELECT " + query + limit).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
                        }
                    }
                }
                // a read that a skip exhausted reads every row from its top
                final String query = "id, s, ts FROM t WHERE ts IN '1970-01-01'" + predicates[p] + " ORDER BY s, ts";
                printSql("SELECT /*+ no_index(t) */ " + query, expected);
                try (RecordCursorFactory factory = select("SELECT " + query); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final RecordCursor.Counter counter = new RecordCursor.Counter();
                    counter.set(rowCount);
                    cursor.skipRows(counter, 5);
                    Assert.assertEquals(0, counter.get());
                    Assert.assertFalse(cursor.hasNext());
                    cursor.toTop();
                    TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, actual);
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
                // the read returns the rows of the 64 keys by row id from their posting lists: it copies no row and scans
                // no row group, and decodes each of the 128 row groups that hold the rows once
                Assert.assertEquals("the read copies no row", chainBytesLo, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN));
                final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
                Assert.assertEquals(ParquetDecodeHint.SCATTERED, pool.getDecodeHint());
                Assert.assertEquals("payload decodes", 128, pool.getDecodeCount());
                Assert.assertEquals("the read scans no row group", 0, PageFrameRecordCursorImpl.testDenseScanFrames.get());
            } finally {
                PageFrameRecordCursorImpl.isWalkCounterEnabled = false;
            }
        });
    }

    @Test
    public void testRowIdsOfWaitingReadChooseModeForEveryRow() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            createSeriesTable();
            // a consumer that moves to rows by their row ids reads every row of the read, so that the read chooses its
            // mode by a model of the direct read and the gather before a skip that would bound it
            PageFrameRecordCursorImpl.testDirectModelRuns.set(0);
            PageFrameRecordCursorImpl.isWalkCounterEnabled = true;
            try (RecordCursorFactory factory = select("SELECT id, s, payload, ts FROM t WHERE ts IN '1970-01-01' ORDER BY s, ts");
                 RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                cursor.setRecordAtRows(null);
                Assert.assertEquals(1, PageFrameRecordCursorImpl.testDirectModelRuns.get());
                final RecordCursor.Counter counter = new RecordCursor.Counter();
                counter.set(15_000);
                cursor.skipRows(counter, 10);
                Assert.assertEquals(0, counter.get());
                final Record record = cursor.getRecord();
                long id = 751;
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(id, record.getLong(0));
                    id += 2_000;
                    count++;
                }
                Assert.assertEquals(10, count);
                Assert.assertEquals(1, PageFrameRecordCursorImpl.testDirectModelRuns.get());
                Assert.assertEquals(ParquetDecodeHint.MONOTONIC, ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool().getDecodeHint());
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
                    // the read returns the rows by row id from the posting lists of the keys, which copies no 8 KiB payload
                    Assert.assertEquals("the read does not gather its rows", stateSize, cursor.preComputedStateSize());
                    Assert.assertEquals("the read copies no row", chainBytesLo, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN));
                    final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
                    Assert.assertEquals(ParquetDecodeHint.SCATTERED, pool.getDecodeHint());
                    // the small decode cache keeps a single row group, so the read decodes the row group of each of the
                    // 272 rows besides the row group it samples; the larger one keeps the 48 row groups that hold the
                    // rows, each decoded once
                    Assert.assertEquals(cacheBytes == 1024 ? 1 + 272 : 48, pool.getDecodeCount());
                }
            }
        });
    }

    @Test
    public void testWindowFunctionOverKeyOrderedReadKeepsRowGroupsDecoded() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            createSeriesTable();
            // the window function over a subquery reads the rows back by their row ids, in time order and then in key
            // order
            final String[] queries = {
                    "SELECT *, row_number() OVER (PARTITION BY s ORDER BY ts) rn FROM (SELECT %s id, s, payload, ts FROM t WHERE ts IN '1970-01-01' ORDER BY s, ts)"
            };
            final StringSink expected = new StringSink();
            final StringSink actual = new StringSink();
            for (String query : queries) {
                printSql(query.formatted("/*+ no_index(t) */"), expected);
                final String expectedRows = expected.toString();
                final String sql = query.formatted("");
                assertQuery(sql).noLeakCheck().expectSize().returns(expectedRows);
                final long sortedBytes;
                try (RecordCursorFactory factory = select(query.formatted("/*+ no_index(t) */")); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    TestUtils.assertCursor(expectedRows, cursor, factory.getMetadata(), true, actual);
                    sortedBytes = sqlExecutionContext.getMemoryTracker().getUsed();
                }
                try (RecordCursorFactory factory = select(sql)) {
                    final CursorCapture capture = captureWindowBase(factory);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        TestUtils.assertCursor(expectedRows, cursor, factory.getMetadata(), true, actual);
                        Assert.assertFalse("the read returns its rows from the page frames", capture.cursor.getRecord() instanceof DelegatingRecord);
                        final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) capture.cursor).getFrameMemoryPool();
                        // the read, the pass of the window function in time order and its pass in key order each decode
                        // a row group at most once: the keys of a hundred consecutive series share their twenty row
                        // groups, where a decode cache of four row groups decodes one for nearly every row
                        Assert.assertTrue("decodes: " + pool.getDecodeCount(), pool.getDecodeCount() <= 3 * 400);
                        Assert.assertEquals(ParquetDecodeHint.SCATTERED, pool.getDecodeHint());
                        Assert.assertTrue("the decode cache holds less than a sort of the rows", sqlExecutionContext.getMemoryTracker().getUsed() < sortedBytes);
                    }
                    // a sort of the rows fails at this limit, which leaves the read to the windows of the gather, and the
                    // window function a decode cache within the limit
                    node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 6 * 1024 * 1024L);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        TestUtils.assertCursor(expectedRows, cursor, factory.getMetadata(), true, actual);
                        Assert.assertTrue("the read gathers its rows", capture.cursor.getRecord() instanceof DelegatingRecord);
                        final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) capture.cursor).getFrameMemoryPool();
                        Assert.assertTrue("decodes: " + pool.getDecodeCount(), pool.getDecodeCount() < 40_000 / 4);
                        Assert.assertEquals(ParquetDecodeHint.SCATTERED, pool.getDecodeHint());
                        // a read of the rows again decodes under the cache of its windows
                        capture.cursor.toTop();
                        int count = 0;
                        while (capture.cursor.hasNext()) {
                            count++;
                        }
                        Assert.assertEquals(40_000, count);
                        Assert.assertEquals(ParquetDecodeHint.MONOTONIC, pool.getDecodeHint());
                        Assert.assertEquals(4, pool.getCachedFrameCount());
                    } finally {
                        node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
                    }
                }
            }
        });
    }

    @Test
    public void testWindowFunctionStreamsOverKeyOrderedRead() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            createSeriesTable();
            // the read delivers the rows of each series in time order, so the window function streams over them
            final String query = "SELECT %s id, s, payload, ts, row_number() OVER (PARTITION BY s ORDER BY ts) rn FROM t WHERE ts IN '1970-01-01' ORDER BY s, ts";
            final StringSink expected = new StringSink();
            printSql(query.formatted("/*+ no_index(t) */"), expected);
            assertQuery(query.formatted(""))
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlanContaining("Window\n  functions: [row_number() over (partition by [s])]\n    SortedSymbolIndex")
                    .returns(expected);
            // a sort of the rows fails at this limit
            node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 6 * 1024 * 1024L);
            try {
                assertQuery(query.formatted("")).noLeakCheck().noRandomAccess().returns(expected);
            } finally {
                node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
            }
        });
    }

    @Test
    public void testWindowOfWideKeysHandsReadToKeyPasses() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 256 * 1024L);
        assertMemoryLeak(() -> {
            // the last two of the 8 keys hold 512-byte payloads and the others one-byte ones, so the sample of the first
            // row group plans five windows: the first four hold the narrow rows, and the fifth, which starts at the row
            // 400 of the key k6, outgrows its budget
            createWidePayloadTable("x % 8", "x % 8 < 6", 512);
            final String tail = "id, s, payload, ts FROM w WHERE ts IN '1970-01-01'"
                    + " AND s IN ('k0', 'k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7') AND id > 0 AND test_latched_counter() ORDER BY s, ts";
            final StringSink expected = new StringSink();
            for (String limit : new String[]{"", " LIMIT 6_395, 6_405", " LIMIT -7"}) {
                printSql("SELECT /*+ no_index(w) */ " + tail + limit, expected);
                assertQuery("SELECT " + tail + limit).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
            }
            final String descendingTail = tail.replace("ORDER BY s, ts", "ORDER BY s DESC, ts DESC");
            printSql("SELECT /*+ no_index(w) */ " + descendingTail, expected);
            assertQuery("SELECT " + descendingTail).noLeakCheck().sizeMayVary().returns(expected);
            printSql("SELECT /*+ no_index(w) */ " + tail, expected);
            final String expectedRows = expected.toString();
            printSql("SELECT /*+ no_index(w) */ " + tail + " LIMIT 6_000, 8_000", expected);
            final String expectedSkippedRows = expected.toString();
            final ChainPeak chainPeak = new ChainPeak();
            TestLatchedCounterFunctionFactory.reset(chainPeak);
            try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                // the rows of the first four windows carry their row ids, and those of the passes their own
                final LongList rowIds = new LongList();
                assertRowIds(expectedRows, factory, cursor, rowIds, 100, chainPeak.chainBytesLo);
                Assert.assertTrue("record chain peak: " + chainPeak.peak, chainPeak.peak <= SORT_BYTES);
                Assert.assertEquals("the passes of the keys hold no record chain", chainPeak.chainBytesLo, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN));
                // a sort filters each of the 80 row groups once and reads the rows of each of the 8 keys back from them
                final long decodeCount = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool().getDecodeCount();
                Assert.assertTrue("decodes: " + decodeCount, decodeCount <= 9 * 80);
                assertRecordAt(expectedRows, factory, cursor, rowIds);
                assertRereads(expectedRows, expectedSkippedRows, factory, cursor, 6_000, 8_000);
            } finally {
                TestLatchedCounterFunctionFactory.reset(null);
            }
            try (RecordCursorFactory factory = select("SELECT " + tail)) {
                assertFreshCursors(expectedRows, expectedSkippedRows, factory, 6_000, 8_000);
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

    private static CursorCapture captureWindowBase(RecordCursorFactory factory) {
        for (RecordCursorFactory current = factory; current != null; current = current.getBaseFactory()) {
            if (current instanceof CachedWindowLightRecordCursorFactory window) {
                final CursorCapture[] capture = new CursorCapture[1];
                window.wrapBaseFactory(base -> capture[0] = new CursorCapture(base));
                return capture[0];
            }
        }
        throw new AssertionError("expected a cached window over the read");
    }

    /**
     * Moves a cursor that did not read a row yet to the read of its keys by its size, and another by a skip.
     */
    private static void assertFreshCursors(
            String expected,
            String expectedSkipped,
            RecordCursorFactory factory,
            long skipCount,
            long rowCount
    ) throws SqlException {
        final StringSink actual = new StringSink();
        final RecordCursor.Counter counter = new RecordCursor.Counter();
        try (RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
            cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
            Assert.assertEquals(rowCount, counter.get());
            cursor.toTop();
            TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, actual);
        }
        try (RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
            counter.set(skipCount);
            cursor.skipRows(counter, RecordCursor.UNBOUNDED_ROW_COUNT);
            Assert.assertEquals(0, counter.get());
            TestUtils.assertCursor(expectedSkipped, cursor, factory.getMetadata(), true, actual);
        }
    }

    private static void assertRecordAt(String expected, RecordCursorFactory factory, RecordCursor cursor, LongList rowIds) {
        Assert.assertTrue(factory.recordCursorSupportsRandomAccess());
        final StringSink actual = new StringSink();
        final Record recordB = cursor.getRecordB();
        CursorPrinter.println(factory.getMetadata(), actual);
        for (int i = 0, n = rowIds.size(); i < n; i++) {
            cursor.recordAt(recordB, rowIds.getQuick(i));
            CursorPrinter.println(recordB, factory.getMetadata(), actual);
        }
        TestUtils.assertEquals(expected, actual);
    }

    private static void assertRereads(
            String expected,
            String expectedSkipped,
            RecordCursorFactory factory,
            RecordCursor cursor,
            long skipCount,
            long rowCount
    ) {
        final StringSink actual = new StringSink();
        cursor.toTop();
        TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, actual);
        cursor.toTop();
        final RecordCursor.Counter counter = new RecordCursor.Counter();
        cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
        Assert.assertEquals(rowCount, counter.get());
        cursor.toTop();
        counter.set(skipCount);
        cursor.skipRows(counter, RecordCursor.UNBOUNDED_ROW_COUNT);
        Assert.assertEquals(0, counter.get());
        TestUtils.assertCursor(expectedSkipped, cursor, factory.getMetadata(), true, actual);
        final LongList rowIds = new LongList();
        cursor.toTop();
        assertRowIds(expected, factory, cursor, rowIds, -1, 0);
        assertRecordAt(expected, factory, cursor, rowIds);
    }

    /**
     * Reads the rows of the cursor and their row ids, and checks, after {@code heldRowCount} rows, that the record chain
     * holds more than {@code chainBytesLo}.
     */
    private static void assertRowIds(
            String expected,
            RecordCursorFactory factory,
            RecordCursor cursor,
            LongList rowIds,
            long heldRowCount,
            long chainBytesLo
    ) {
        final StringSink actual = new StringSink();
        final Record record = cursor.getRecord();
        CursorPrinter.println(factory.getMetadata(), actual);
        while (cursor.hasNext()) {
            if (rowIds.size() == heldRowCount) {
                Assert.assertTrue("the first window holds its rows", Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN) > chainBytesLo);
            }
            rowIds.add(record.getRowId());
            CursorPrinter.println(record, factory.getMetadata(), actual);
        }
        TestUtils.assertEquals(expected, actual);
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

    private static void createSeriesTable() throws Exception {
        // 2_000 series sampled in turn, whose keys order as their ids, over 400 row groups of a hundred rows
        execute("""
                CREATE TABLE t AS (
                    SELECT x id, ('k' || lpad(((x - 1) % 2_000)::STRING, 4, '0'))::SYMBOL s, rpad('p', 100, 'p')::VARCHAR payload,
                        timestamp_sequence(0, 1_000_000) ts
                    FROM long_sequence(40_000)
                ), INDEX(s) TIMESTAMP(ts) PARTITION BY DAY
                """);
        execute("INSERT INTO t VALUES (40_001, 'k0001', 'p', '1970-01-02')");
        execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
    }

    private static void createWideningPayloadTable() throws Exception {
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
    }

    private static String cachedRowGroupKeys() {
        final StringSink keys = new StringSink();
        keys.put("'k00'");
        for (int i = 1; i < 40; i++) {
            keys.put(", 'k").put(i / 10).put(i % 10).put('\'');
        }
        return keys.toString();
    }

    /**
     * Creates the table w of {@code rowCount} rows over row groups of 100 rows of a Parquet partition, whose 40 keys
     * outnumber a third of those rows, and whose payloads are one byte long where {@code isNarrow} holds and
     * {@code width} bytes long elsewhere.
     */
    private static void createCachedRowGroupsTable(int rowCount, String isNarrow, int width) throws Exception {
        execute("CREATE TABLE w (id LONG, s SYMBOL INDEX, payload VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO w SELECT x, ('k' || lpad((x % 40)::STRING, 2, '0'))::SYMBOL, (CASE WHEN " + isNarrow + " THEN 'a' ELSE rpad('w', "
                + width + ", 'w') END)::VARCHAR, timestamp_sequence(0, 1_000) FROM long_sequence(" + rowCount + ")");
        execute("INSERT INTO w VALUES (" + (rowCount + 1) + ", 'k00', 'x', '1970-01-02')");
        execute("ALTER TABLE w CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
    }

    /**
     * Creates the table w of 8_000 rows over 80 row groups of a Parquet partition, the key of each row the symbol 'k'
     * followed by {@code key}, and its payload {@code payload}.
     */
    private static void createPayloadTable(String symbolColumn, String key, String payload) throws Exception {
        execute("CREATE TABLE w (id LONG, " + symbolColumn + ", payload VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO w SELECT x, ('k' || (" + key + "))::SYMBOL, (" + payload + ")::VARCHAR, timestamp_sequence(0, 1_000) FROM long_sequence(8_000)");
        execute("INSERT INTO w VALUES (8_001, 'k0', 'x', '1970-01-02')");
        execute("ALTER TABLE w CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
    }

    private static void createWidePayloadTable(String key, String isNarrow) throws Exception {
        createWidePayloadTable(key, isNarrow, 256);
    }

    /**
     * Creates the table w of 8_000 rows over 80 row groups of a Parquet partition, the key of each row the symbol 'k'
     * followed by {@code key}, and its payload one byte long where {@code isNarrow} holds and {@code width} bytes long
     * elsewhere.
     */
    private static void createWidePayloadTable(String key, String isNarrow, int width) throws Exception {
        createPayloadTable("s SYMBOL INDEX", key, "CASE WHEN " + isNarrow + " THEN 'a' ELSE rpad('w', " + width + ", 'w') END");
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

    /**
     * The symbols 'k00' to the key before {@code keyCount}, for an IN list.
     */
    private static String paddedKeys(int keyCount) {
        final StringSink keys = new StringSink();
        for (int i = 0; i < keyCount; i++) {
            keys.put(i > 0 ? ", 'k" : "'k").put(i / 10).put(i % 10).put('\'');
        }
        return keys.toString();
    }

    /**
     * Samples the memory of the record chains each time the filter reads a row, which the passes of a gather do as
     * they append rows to their record chain.
     */
    private static class ChainPeak implements TestLatchedCounterFunctionFactory.Callback {
        private final long chainBytesLo = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN);
        private long peak;

        @Override
        public boolean onGet(Record rec, int count) {
            peak = Math.max(peak, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN) - chainBytesLo);
            return true;
        }
    }

    /**
     * Returns the cursors of its base factory as they are, keeping the last one it returned.
     */
    private static class CursorCapture extends AbstractRecordCursorFactory {
        private final RecordCursorFactory base;
        private RecordCursor cursor;

        private CursorCapture(RecordCursorFactory base) {
            super(base.getMetadata());
            this.base = base;
        }

        @Override
        public boolean followedOrderByAdvice() {
            return base.followedOrderByAdvice();
        }

        @Override
        public RecordCursorFactory getBaseFactory() {
            return base;
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
            return cursor = base.getCursor(executionContext);
        }

        @Override
        public int getScanDirection() {
            return base.getScanDirection();
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return base.recordCursorSupportsRandomAccess();
        }

        @Override
        public void toPlan(PlanSink sink) {
            base.toPlan(sink);
        }

        @Override
        public boolean usesIndex() {
            return base.usesIndex();
        }

        @Override
        protected void _close() {
            Misc.free(base);
        }
    }
}

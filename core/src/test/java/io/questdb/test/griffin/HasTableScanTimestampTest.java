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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.parquet.ParquetExportMode;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.StaleViewCheckFactory;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * {@link RecordCursorFactory#hasTableScanTimestamp()} tells a Parquet export of a lazily type-converted column whether
 * it can write the plan's timestamp as the file's designated timestamp: the column must hold a table's designated
 * timestamp in every row and the rows must follow the direction the plan reports. Each shape below names, by its
 * EXPLAIN plan, the factory whose answer it checks; for every plan that answers true the test reads the rows and checks
 * both facts.
 */
public class HasTableScanTimestampTest extends AbstractCairoTest {
    // The value getScanDirection() is not asserted for: the answer of a plan that cannot stream does not depend on it.
    private static final int ANY_DIRECTION = -1;
    private static final int BACKWARD = RecordCursorFactory.SCAN_DIRECTION_BACKWARD;
    private static final int FORWARD = RecordCursorFactory.SCAN_DIRECTION_FORWARD;
    private static final int OTHER = RecordCursorFactory.SCAN_DIRECTION_OTHER;
    // The rows of t and r: two symbols alternate inside each of three days, and other_ts holds NULL in two rows.
    private static final String ROWS = """
            ('a', 'p', 1, '2023-06-05T00:00:00.000000Z', '2024-01-01T00:00:00.000000Z'),
            ('b', 'q', 2, NULL, '2024-01-01T01:00:00.000000Z'),
            ('a', 'q', 3, '2023-06-03T00:00:00.000000Z', '2024-01-01T02:00:00.000000Z'),
            ('b', 'p', 4, '2023-06-02T00:00:00.000000Z', '2024-01-01T03:00:00.000000Z'),
            ('a', 'p', 5, '2023-06-09T00:00:00.000000Z', '2024-01-02T00:00:00.000000Z'),
            ('b', 'q', 6, NULL, '2024-01-02T01:00:00.000000Z'),
            ('a', 'q', 7, '2023-06-07T00:00:00.000000Z', '2024-01-02T02:00:00.000000Z'),
            ('b', 'p', 8, '2023-06-06T00:00:00.000000Z', '2024-01-02T03:00:00.000000Z'),
            ('a', 'p', 9, '2023-06-10T00:00:00.000000Z', '2024-01-03T00:00:00.000000Z'),
            ('b', 'q', 10, '2023-06-11T00:00:00.000000Z', '2024-01-03T01:00:00.000000Z')
            """;
    private final StringBuilder failures = new StringBuilder();

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // read_parquet() reads the file that createTables() encodes from the table t
        inputRoot = root;
    }

    @Test
    public void testTableScanTimestampOfKeyFiltersUnderTimestampClaim() throws Exception {
        // FilterOnValues and FilterOnExcludedValues merge the rows of forward frames in table order, but a TIMESTAMP(col)
        // claim makes their timestamp other_ts, which holds NULL in two rows and does not ascend. Neither may answer
        // true, or an export of the retyped r would write other_ts as the file's designated timestamp.
        assertMemoryLeak(() -> {
            createTables();
            for (String tableName : new String[]{"t", "r"}) {
                final String inQuery = "SELECT * FROM " + tableName + " timestamp(other_ts) WHERE s IN ('a', 'b')";
                assertFalse(inQuery, FORWARD, "FilterOnValues", "Table-order scan", "Frame forward scan on: " + tableName);
                final String notEqualsQuery = "SELECT * FROM " + tableName + " timestamp(other_ts) WHERE s != 'a'";
                assertFalse(notEqualsQuery, FORWARD, "FilterOnExcludedValues", "Table-order scan", "Frame forward scan on: " + tableName);
            }
            assertExportMode("SELECT * FROM r timestamp(other_ts) WHERE s IN ('a', 'b')", ParquetExportMode.TEMP_TABLE);
            assertExportMode("SELECT * FROM r timestamp(other_ts) WHERE s != 'a'", ParquetExportMode.TEMP_TABLE);

            Assert.assertEquals("", failures.toString());
        });
    }

    @Test
    public void testTableScanTimestampOfLimitedSortsOnAnotherTimestamp() throws Exception {
        // A top K or a limited sort whose first key is other_ts marks other_ts as its timestamp, and NULL sorts first.
        // Its base scans the table and answers true for ts, so each sort must check that its timestamp is the base's,
        // or an export of the retyped r would write the NULLs as the file's designated timestamp. Each sort factory
        // runs under the settings that make the planner pick it.
        assertMemoryLeak(() -> {
            createTables();
            final String query = "SELECT * FROM t ORDER BY other_ts LIMIT 3";
            final String retypedQuery = "SELECT * FROM r ORDER BY other_ts LIMIT 3";
            assertFalse(query, FORWARD, "Async Top K lo: 3", "keys: [other_ts]", "Frame forward scan on: t");
            assertFalse(retypedQuery, FORWARD, "Async Top K lo: 3", "keys: [other_ts]", "Frame forward scan on: r");
            assertExportMode(retypedQuery, ParquetExportMode.TEMP_TABLE);
            final boolean isParallelTopKEnabled = sqlExecutionContext.isParallelTopKEnabled();
            sqlExecutionContext.setParallelTopKEnabled(false);
            try {
                assertFalse(query, FORWARD, "Encode sort light lo: 3", "keys: [other_ts]", "Frame forward scan on: t");
                assertFalse(retypedQuery, FORWARD, "Encode sort light lo: 3", "keys: [other_ts]", "Frame forward scan on: r");
                assertExportMode(retypedQuery, ParquetExportMode.TEMP_TABLE);
                node1.setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, false);
                try {
                    assertFalse(query, FORWARD, "Sort light lo: 3", "keys: [other_ts]", "Frame forward scan on: t");
                    assertFalse(retypedQuery, FORWARD, "Sort light lo: 3", "keys: [other_ts]", "Frame forward scan on: r");
                    assertExportMode(retypedQuery, ParquetExportMode.TEMP_TABLE);
                } finally {
                    node1.setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, true);
                }
            } finally {
                sqlExecutionContext.setParallelTopKEnabled(isParallelTopKEnabled);
            }

            Assert.assertEquals("", failures.toString());
        });
    }

    @Test
    public void testTableScanTimestampOfPlanShapes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();

            // Leaves: the scan of a table, in either direction, its interval scan, and its index scans.
            assertTrue("SELECT * FROM t", 10, FORWARD, "PageFrame", "Row forward scan", "Frame forward scan on: t");
            assertTrue("SELECT * FROM t WHERE ts IN '2024-01-02'", 4, FORWARD, "Row forward scan", "Interval forward scan on: t");
            assertTrue("SELECT * FROM t ORDER BY ts DESC", 10, BACKWARD, "Row backward scan", "Frame backward scan on: t");
            assertTrue("SELECT * FROM t WHERE ts IN '2024-01-02' ORDER BY ts DESC", 4, BACKWARD, "Row backward scan", "Interval backward scan on: t");
            assertTrue("SELECT * FROM t WHERE s = 'a'", 5, FORWARD, "DeferredSingleSymbolFilterPageFrame", "Index forward scan on: s", "Frame forward scan on: t");
            bindVariableService.clear();
            bindVariableService.setStr(0, "b");
            assertTrue("SELECT * FROM t WHERE s = $1", 5, FORWARD, "DeferredSingleSymbolFilterPageFrame", "Index forward scan on: s", "Frame forward scan on: t");
            bindVariableService.clear();
            assertTrue("SELECT * FROM t WHERE s = 'a' ORDER BY ts DESC", 5, BACKWARD, "DeferredSingleSymbolFilterPageFrame", "Index backward scan on: s", "Frame backward scan on: t");
            assertTrue("SELECT * FROM t WHERE s = 'a' AND x > 1", 4, FORWARD, "PageFrame", "Index forward scan on: s", "symbolFilter: s='a'", "Frame forward scan on: t");
            assertTrue("SELECT * FROM t WHERE s = 'a' AND x > 1 ORDER BY ts DESC", 4, BACKWARD, "PageFrame", "Index backward scan on: s", "symbolFilter: s='a'", "Frame backward scan on: t");
            assertTrue("SELECT * FROM t WHERE s IN ('a', 'b')", 10, FORWARD, "FilterOnValues", "Table-order scan", "Frame forward scan on: t");
            assertTrue("SELECT * FROM t WHERE s != 'a'", 5, FORWARD, "FilterOnExcludedValues", "Table-order scan", "Frame forward scan on: t");
            assertTrue("SELECT * FROM t WHERE s IN (SELECT s FROM k)", 10, FORWARD, "FilterOnSubQuery", "Frame forward scan on: t");
            assertTrue("SELECT * FROM t WHERE s LIKE 'a%'", 5, FORWARD, "AdaptiveSymbolPattern", "SymbolPatternIndex", "Frame forward scan on: t");
            assertTrue("SELECT s, x, ts FROM rc WHERE s = 'a'", 5, FORWARD, "CoveringIndex on: s");
            assertTrue("SELECT s, x, ts FROM rc WHERE s IN ('a', 'b')", 10, FORWARD, "CoveringIndex on: s", "filter: s IN ['a','b']");
            assertTrue("SELECT s, x, ts FROM rc WHERE s = 'a' AND x > 1", 4, FORWARD, "Async", "Filter", "CoveringIndex on: s");
            // A key whose value is known only once it is bound gives the covering scan a backup of the same rows.
            bindVariableService.clear();
            bindVariableService.setStr(0, "b");
            assertTrue("SELECT s, x, ts FROM rc WHERE s = $1", 5, FORWARD, "CoveringIndex", "backup: true", "on: s");
            assertTrue("SELECT s, x, ts FROM rc WHERE s IN ('a', $1)", 10, FORWARD, "CoveringIndex", "backup: true", "on: s");
            bindVariableService.clear();
            assertTrue("SELECT s, x, ts FROM rc WHERE s LIKE 'a%'", 5, FORWARD, "AdaptiveSymbolPattern", "CoveringIndex on: s");
            assertTrue("SELECT * FROM mv", 6, FORWARD, "PageFrame", "Frame forward scan on: mv");

            // Under ORDER BY ts DESC the index scan of several keys reads the frames backward key by key, so it
            // promises no order, and the sort above it orders the rows by the designated timestamp.
            assertTrue("SELECT * FROM t WHERE s IN ('a', 'b') ORDER BY ts DESC", 10, BACKWARD, "Encode sort light", "keys: [ts desc]", "FilterOnValues", "Cursor-order scan", "Frame backward scan on: t");
            assertLeaf("SELECT * FROM t WHERE s IN ('a', 'b') ORDER BY ts DESC", "FilterOnValuesRecordCursorFactory", true, OTHER);
            assertTrue("SELECT * FROM t WHERE s != 'a' ORDER BY ts DESC", 5, BACKWARD, "Encode sort light", "keys: [ts desc]", "FilterOnExcludedValues", "Cursor-order scan", "Frame backward scan on: t");
            assertLeaf("SELECT * FROM t WHERE s != 'a' ORDER BY ts DESC", "FilterOnExcludedValuesRecordCursorFactory", true, OTHER);

            // Filters, a runtime constant gate and LIMIT hand out the rows of their base in its order.
            assertTrue("SELECT * FROM t WHERE x > 1", 9, FORWARD, "Async JIT Filter", "filter: 1<x", "Frame forward scan on: t");
            assertTrue("SELECT * FROM t WHERE x > 1 ORDER BY ts DESC", 9, BACKWARD, "Async JIT Filter", "filter: 1<x", "Frame backward scan on: t");
            assertTrue("SELECT * FROM t WHERE x > 1 LIMIT -3", 3, FORWARD, "Async JIT Filter", "limit: 3", "filter: 1<x");
            final int jitMode = sqlExecutionContext.getJitMode();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            try {
                assertTrue("SELECT * FROM t WHERE x > 1", 9, FORWARD, "Async Filter", "filter: 1<x", "Frame forward scan on: t");
            } finally {
                sqlExecutionContext.setJitMode(jitMode);
            }
            sqlExecutionContext.setParallelFilterEnabled(false);
            try {
                assertTrue("SELECT * FROM t WHERE x > 1", 9, FORWARD, "Filter filter: 1<x", "Frame forward scan on: t");
            } finally {
                sqlExecutionContext.setParallelFilterEnabled(true);
            }
            assertTrue("SELECT * FROM t WHERE (SELECT b FROM flags LIMIT 1)", 10, FORWARD, "Filter filter: cursor", "Frame forward scan on: t");
            assertTrue("SELECT * FROM t LIMIT 3", 3, FORWARD, "Limit value: 3", "Frame forward scan on: t");
            assertTrue("SELECT * FROM t LIMIT -3", 3, FORWARD, "Limit value: -3", "Frame forward scan on: t");
            assertTrue("SELECT * FROM t LIMIT 1, 3", 2, FORWARD, "Limit left: 1 right: 3", "Frame forward scan on: t");
            assertTrue("SELECT * FROM t ORDER BY ts DESC LIMIT 3", 3, BACKWARD, "Limit value: 3", "Frame backward scan on: t");
            assertTrue("SELECT * FROM t WHERE s = 'a' ORDER BY ts DESC LIMIT 3", 3, BACKWARD, "Limit value: 3", "Index backward scan on: s");

            // Projections keep the designated timestamp under any name and in any position.
            assertTrue("SELECT x AS a, ts AS b FROM t WHERE x > 1", 9, FORWARD, "SelectedRecord", "Async JIT Filter");
            assertTrue("SELECT ts AS ts2, * FROM t", 10, FORWARD, "SelectedRecord", "Frame forward scan on: t");
            assertTrue("SELECT x + 1 AS y, ts FROM t", 10, FORWARD, "VirtualRecord", "functions: [x+1,ts]", "Frame forward scan on: t");
            assertTrue("SELECT x + 1 AS y, ts FROM t WHERE x > 1 ORDER BY ts DESC", 9, BACKWARD, "VirtualRecord", "Async JIT Filter", "Frame backward scan on: t");
            assertTrue("SELECT * FROM (SELECT * FROM t WHERE x > 1)", 9, FORWARD, "Async JIT Filter", "Frame forward scan on: t");
            assertTrue("WITH q AS (SELECT * FROM t WHERE x > 1) SELECT * FROM q ORDER BY ts DESC", 9, BACKWARD, "Async JIT Filter", "Frame backward scan on: t");

            // Views over the retyped table r, whose first day is a Parquet partition holding x in its old type.
            assertView("SELECT * FROM vr", 10, FORWARD, "Frame forward scan on: r");
            assertView("SELECT * FROM vrf", 9, FORWARD, "Async JIT Filter", "Frame forward scan on: r");
            assertView("SELECT * FROM vrv", 10, FORWARD, "Frame forward scan on: r");
            assertView("SELECT * FROM vr WHERE x > 1 ORDER BY ts DESC", 9, BACKWARD, "Async JIT Filter", "Frame backward scan on: r");
            assertView("SELECT x AS a, ts AS b FROM vrf", 9, FORWARD, "SelectedRecord", "Frame forward scan on: r");
            assertView("SELECT * FROM vr LIMIT 3", 3, FORWARD, "Limit value: 3", "Frame forward scan on: r");
            assertTrue("SELECT * FROM r WHERE s IN ('a', 'b')", 10, FORWARD, "FilterOnValues", "Frame forward scan on: r");
            assertTrue("SELECT x + 1 AS y, ts FROM r", 10, FORWARD, "VirtualRecord", "Frame forward scan on: r");

            // Sorts whose first key is the designated timestamp order the rows by it themselves, each sort factory
            // under the settings that make the planner pick it.
            assertTrue("SELECT * FROM t ORDER BY ts, x", 10, FORWARD, "Encode sort light", "keys: [ts, x]", "Frame forward scan on: t");
            assertTrue("SELECT * FROM t ORDER BY ts, x LIMIT 3", 3, FORWARD, "Encode sort light lo: 3", "keys: [ts, x]", "Frame forward scan on: t");
            assertTrue("SELECT ts, x, rnd_int() r FROM t ORDER BY ts, r", 10, FORWARD, "Encode sort", "keys: [ts, r]", "VirtualRecord");
            assertTrue("SELECT * FROM t WHERE x > 1 ORDER BY ts DESC, x LIMIT 3", 3, BACKWARD, "Top K lo: 3", "keys: [ts desc, x]", "Frame forward scan on: t");
            assertTrue("SELECT * FROM t WHERE s IN ('a', 'b') ORDER BY ts DESC LIMIT 3", 3, BACKWARD, "Encode sort light lo: 3", "keys: [ts desc]", "FilterOnValues");
            node1.setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, false);
            try {
                assertTrue("SELECT * FROM t WHERE s IN ('a', 'b') ORDER BY ts DESC", 10, BACKWARD, "Sort light", "keys: [ts desc]", "FilterOnValues");
                assertTrue("SELECT * FROM t ORDER BY ts, x LIMIT 3", 3, FORWARD, "Sort light lo: 3", "keys: [ts, x]", "Frame forward scan on: t");
                assertTrue("SELECT ts, x, rnd_int() r FROM t ORDER BY ts, r", 10, FORWARD, "Sort", "keys: [ts, r]", "VirtualRecord");
                assertTrue("SELECT ts, x, (x + 1) * (x + 2) c FROM t ORDER BY ts, c", 10, FORWARD, "Sort light", "keys: [ts, c]", "Materialize sort keys", "VirtualRecord");
                // The sort keys that a projection computes come first: on another timestamp column, and on a
                // timestamp that a TIMESTAMP(col) claim names.
                assertFalse("SELECT x, other_ts, ts, (x + 1) * (x + 2) c FROM t ORDER BY other_ts, c", FORWARD, "Sort light", "keys: [other_ts, c]", "Materialize sort keys");
                assertFalse("SELECT x, other_ts, (x + 1) * (x + 2) c FROM t timestamp(other_ts) ORDER BY other_ts, c", FORWARD, "Sort light", "keys: [other_ts, c]", "Materialize sort keys");
            } finally {
                node1.setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, true);
            }
            // The planner builds the single-threaded top K only over a GROUP BY, whose rows carry no table's
            // designated timestamp.
            assertFalse("SELECT ts, count() c FROM t GROUP BY ts ORDER BY ts DESC LIMIT 3", BACKWARD, "Long Top K lo: 3", "keys: [ts desc]");
            // A TIMESTAMP(col) claim on the GROUP BY gives the top K a base with the same timestamp, still not a table's.
            assertFalse("SELECT * FROM (SELECT ts, count() c FROM t GROUP BY ts) timestamp(ts) ORDER BY ts DESC LIMIT 3", BACKWARD, "Long Top K lo: 3", "keys: [ts desc]", "SelectedRecord", "Async Group By");

            // The timestamp is not a table's designated timestamp, or the rows do not follow the reported direction.
            assertFalse("SELECT * FROM t timestamp(other_ts)", FORWARD, "PageFrame", "Frame forward scan on: t");
            assertFalse("SELECT * FROM (SELECT x, other_ts, ts FROM t) timestamp(other_ts)", FORWARD, "SelectedRecord");
            assertFalse("SELECT * FROM (SELECT x, dateadd('s', 1, ts) ts1 FROM t) timestamp(ts1)", FORWARD, "VirtualRecord");
            assertFalse("SELECT * FROM (SELECT x + 1 y, other_ts, ts FROM t) timestamp(other_ts)", FORWARD, "VirtualRecord");
            assertFalse("SELECT * FROM t timestamp(other_ts) WHERE s IN (SELECT s FROM k)", FORWARD, "FilterOnSubQuery");
            assertFalse("SELECT * FROM t timestamp(other_ts) WHERE s LIKE 'a%'", ANY_DIRECTION, "AdaptiveSymbolPattern");
            assertFalse("SELECT s, x, other_ts FROM rc timestamp(other_ts) WHERE s = 'a'", FORWARD, "CoveringIndex on: s");
            assertFalse("SELECT * FROM (SELECT * FROM t WHERE s IN ('a', 'b') ORDER BY s) timestamp(ts)", FORWARD, "SelectedRecord", "keys: [s]");
            assertFalse("SELECT * FROM (SELECT * FROM t ORDER BY x DESC LIMIT 5) timestamp(ts)", BACKWARD, "SelectedRecord", "keys: [x desc]");
            assertFalse("SELECT * FROM t ORDER BY other_ts", FORWARD, "Encode sort light", "keys: [other_ts]");
            assertFalse("SELECT * FROM t WHERE s IN (SELECT s FROM k) ORDER BY ts DESC", BACKWARD, "Encode sort light", "keys: [ts desc]", "FilterOnSubQuery", "Frame backward scan on: t");
            assertLeaf("SELECT * FROM t WHERE s IN (SELECT s FROM k) ORDER BY ts DESC", "FilterOnSubQueryRecordCursorFactory", false, FORWARD);
            // The multi-key ORDER BY with a negative LIMIT forces a backward frame scan, while the index keeps
            // walking each frame forward.
            assertFalse("SELECT * FROM t WHERE s = 'a' ORDER BY ts, x LIMIT -3", ANY_DIRECTION, "Index forward scan on: s", "Frame backward scan on: t");
            assertLeaf("SELECT * FROM t WHERE s = 'a' ORDER BY ts, x LIMIT -3", "DeferredSingleSymbolFilterPageFrameRecordCursorFactory", false, BACKWARD);
            // The same plan is the backup of a covering scan whose key is known only once it is bound: an open that
            // selects the backup would hand out its rows, so the covering scan answers false as well.
            bindVariableService.clear();
            bindVariableService.setStr(0, "b");
            assertFalse("SELECT s, x, ts FROM rc WHERE s = $1 ORDER BY ts, x LIMIT -3", FORWARD, "Encode sort light", "keys: [ts, x]", "CoveringIndex backup: true on: s");
            assertLeaf("SELECT s, x, ts FROM rc WHERE s = $1 ORDER BY ts, x LIMIT -3", "CoveringIndexRecordCursorFactory", false, OTHER);
            bindVariableService.clear();
            assertFalse("SELECT * FROM t WHERE s = 'a' LATEST ON ts PARTITION BY s", FORWARD, "PageFrame", "Index backward scan on: s");
            assertFalse("SELECT * FROM t LATEST ON ts PARTITION BY s", ANY_DIRECTION, "LatestByAllIndexed");
            assertFalse("SELECT * FROM vrl", ANY_DIRECTION, "LatestByAllIndexed", "Frame backward scan on: r");
            assertFalse("SELECT s, x, ts FROM rc WHERE s IN ('a', 'b') LATEST ON ts PARTITION BY s", ANY_DIRECTION, "CoveringIndex op: latest");
            assertFalse("SELECT * FROM (SELECT s, x, rnd_int() r, ts FROM t) LATEST ON ts PARTITION BY s", ANY_DIRECTION, "LatestBy");
            assertFalse("SELECT ts, count() c FROM t SAMPLE BY 1d", ANY_DIRECTION, "Encode sort light", "Group");
            assertFalse("SELECT ts, count() c FROM t SAMPLE BY 1d FILL(NULL)", ANY_DIRECTION, "Fill");
            assertFalse("SELECT x, ts, row_number() OVER () rn FROM t", ANY_DIRECTION, "Window");
            assertFalse("SELECT s, x, ts, avg(x) OVER (PARTITION BY s) a FROM t WHERE s IN ('a', 'b')", ANY_DIRECTION, "CachedWindow");
            assertFalse("SELECT a.ts, a.s, first(b.y) fy FROM t a WINDOW JOIN d b RANGE BETWEEN 1 hour PRECEDING AND 1 hour FOLLOWING", ANY_DIRECTION, "Window Join");
            assertFalse("SELECT t.ts, t.x, d.y FROM t JOIN d ON (s)", ANY_DIRECTION, "Hash Join");
            assertFalse("SELECT * FROM generate_series('2024-01-01'::timestamp, '2024-01-03'::timestamp, '1d')", ANY_DIRECTION, "generate_series");
            assertFalse("SELECT * FROM read_parquet('ht.parquet')", ANY_DIRECTION, "parquet");

            // A plan without a timestamp gives the export nothing to designate, so it streams whatever it is.
            assertNoTimestamp("SELECT s, count() c FROM t", "Group");
            assertNoTimestamp("SELECT DISTINCT s, x FROM t", "Group");
            assertNoTimestamp("SELECT * FROM t UNION ALL SELECT * FROM t", "Union All");
            assertNoTimestamp("SELECT x FROM t ORDER BY other_ts", "keys: [other_ts]");
            assertNoTimestamp("SELECT count() c FROM t SAMPLE BY 1d", "Group");
            assertNoTimestamp("SELECT h.offset, count() c FROM t a HORIZON JOIN d b LIST (0s, 1s) AS h", "Horizon Join");
            assertNoTimestamp("SELECT x, s FROM t", "PageFrame", "Frame forward scan on: t");
            assertNoTimestamp("SELECT s, x FROM rc WHERE s = 'a'", "CoveringIndex on: s");
            assertNoTimestamp("SELECT x + 1 y FROM t", "VirtualRecord");
            assertNoTimestamp("SELECT * FROM t ORDER BY x", "keys: [x]");
            assertNoTimestamp("SELECT x::LONG lx, count() c FROM t GROUP BY lx ORDER BY lx DESC LIMIT 3", "Long Top K lo: 3");

            Assert.assertEquals("", failures.toString());
        });
    }

    private static void assertDirection(String query, int expectedDirection, RecordCursorFactory factory) {
        if (expectedDirection != ANY_DIRECTION) {
            Assert.assertEquals(query + ": getScanDirection()", expectedDirection, factory.getScanDirection());
        }
    }

    // The plan must name the factory a shape targets, so that a planner change fails the shape rather than leaving it
    // to check another factory.
    private static void assertPlan(String query, String... planFragments) throws SqlException {
        final StringSink plan = new StringSink();
        printSql("EXPLAIN " + query, plan);
        for (String fragment : planFragments) {
            if (!Chars.contains(plan, fragment)) {
                Assert.fail(query + ": the plan does not name " + fragment + ":\n" + plan);
            }
        }
    }

    // Reads the rows of a plan that answers true: the timestamp holds no NULL and follows the direction the plan
    // reports.
    private static void assertTableScanTimestampHolds(String query, RecordCursorFactory factory, int expectedRowCount) throws SqlException {
        final int timestampIndex = factory.getMetadata().getTimestampIndex();
        final int direction = factory.getScanDirection();
        int rowCount = 0;
        long previous = Numbers.LONG_NULL;
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                final long timestamp = record.getTimestamp(timestampIndex);
                Assert.assertNotEquals(query + ": NULL timestamp in row " + rowCount, Numbers.LONG_NULL, timestamp);
                if (rowCount > 0) {
                    if (direction == FORWARD) {
                        Assert.assertTrue(query + ": row " + rowCount + " descends", timestamp >= previous);
                    } else {
                        Assert.assertTrue(query + ": row " + rowCount + " ascends", timestamp <= previous);
                    }
                }
                previous = timestamp;
                rowCount++;
            }
        }
        Assert.assertEquals(query + ": row count", expectedRowCount, rowCount);
    }

    private static void createTables() throws Exception {
        execute("CREATE TABLE t (s SYMBOL INDEX, s2 SYMBOL, x INT, other_ts TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO t VALUES " + ROWS);
        // r holds the rows of t; its first day goes to Parquet before x turns from INT to LONG
        execute("CREATE TABLE r (s SYMBOL INDEX, s2 SYMBOL, x INT, other_ts TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO r VALUES " + ROWS);
        drainWalQueue();
        execute("ALTER TABLE r CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueue();
        execute("ALTER TABLE r ALTER COLUMN x TYPE LONG");
        drainWalQueue();
        // rc covers x and other_ts in its posting index on s, and retypes y over its Parquet partition
        execute("CREATE TABLE rc (s SYMBOL INDEX TYPE POSTING INCLUDE (x, other_ts), x INT, other_ts TIMESTAMP, y INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO rc SELECT s, x, other_ts, x, ts FROM t");
        drainWalQueue();
        execute("ALTER TABLE rc CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueue();
        execute("ALTER TABLE rc ALTER COLUMN y TYPE LONG");
        drainWalQueue();
        execute("CREATE TABLE k (s SYMBOL)");
        execute("INSERT INTO k VALUES ('a'), ('b')");
        execute("CREATE TABLE flags (b BOOLEAN)");
        execute("INSERT INTO flags VALUES (true)");
        execute("CREATE TABLE d (s SYMBOL, y INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO d VALUES ('a', 100, '2024-01-01T00:30:00.000000Z'), ('b', 200, '2024-01-01T01:30:00.000000Z')");
        execute("CREATE VIEW vr AS (SELECT * FROM r)");
        execute("CREATE VIEW vrf AS (SELECT * FROM r WHERE x > 1)");
        execute("CREATE VIEW vrl AS (SELECT * FROM r LATEST ON ts PARTITION BY s)");
        execute("CREATE MATERIALIZED VIEW mv AS (SELECT ts, s, sum(x) sx FROM r SAMPLE BY 1d)");
        drainWalAndViewQueues();
        execute("CREATE VIEW vrv AS (SELECT * FROM vr)");
        drainWalAndViewQueues();
        drainWalAndMatViewQueues();
        try (
                Path path = new Path();
                PartitionDescriptor descriptor = new PartitionDescriptor();
                TableReader reader = engine.getReader("t")
        ) {
            path.of(root).concat("ht.parquet");
            PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
            PartitionEncoder.encode(descriptor, path);
        }
    }

    // The export mode of a plan that reads the retyped column x of r.
    private void assertExportMode(String query, ParquetExportMode expectedMode) {
        try (RecordCursorFactory factory = select(query)) {
            Assert.assertTrue(query + ": the plan reads a retyped column", factory.hasParquetConvertedColumns(sqlExecutionContext));
            Assert.assertEquals(query + ": export mode", expectedMode, ParquetExportMode.determineExportMode(factory, false, sqlExecutionContext));
        } catch (Throwable th) {
            recordFailure(query, th);
        }
    }

    private void assertFalse(String query, int expectedDirection, String... planFragments) {
        try {
            assertPlan(query, planFragments);
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertTrue(query + ": the plan has a timestamp", factory.getMetadata().getTimestampIndex() > -1);
                Assert.assertFalse(query + ": hasTableScanTimestamp()", factory.hasTableScanTimestamp());
                Assert.assertFalse(query + ": hasStreamableTimestamp()", ParquetExportMode.hasStreamableTimestamp(factory));
                assertDirection(query, expectedDirection, factory);
            }
        } catch (Throwable th) {
            recordFailure(query, th);
        }
    }

    // Asserts the answer and the direction of the factory of the given class under the plan's top.
    private void assertLeaf(String query, String leafClassName, boolean expectedAnswer, int expectedDirection) {
        try (RecordCursorFactory factory = select(query)) {
            RecordCursorFactory leaf = factory;
            while (leaf != null && !leaf.getClass().getSimpleName().equals(leafClassName)) {
                leaf = leaf.getBaseFactory();
            }
            Assert.assertNotNull(query + ": no " + leafClassName + " in the plan", leaf);
            Assert.assertEquals(query + ": hasTableScanTimestamp() of " + leafClassName, expectedAnswer, leaf.hasTableScanTimestamp());
            Assert.assertEquals(query + ": getScanDirection() of " + leafClassName, expectedDirection, leaf.getScanDirection());
            Assert.assertFalse(query + ": hasStreamableTimestamp() of " + leafClassName, ParquetExportMode.hasStreamableTimestamp(leaf));
        } catch (Throwable th) {
            recordFailure(query, th);
        }
    }

    private void assertNoTimestamp(String query, String... planFragments) {
        try {
            assertPlan(query, planFragments);
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertEquals(query + ": timestamp index", -1, factory.getMetadata().getTimestampIndex());
                Assert.assertFalse(query + ": hasTableScanTimestamp()", factory.hasTableScanTimestamp());
                Assert.assertTrue(query + ": hasStreamableTimestamp()", ParquetExportMode.hasStreamableTimestamp(factory));
            }
        } catch (Throwable th) {
            recordFailure(query, th);
        }
    }

    private void assertTrue(String query, int expectedRowCount, int expectedDirection, String... planFragments) {
        try {
            assertPlan(query, planFragments);
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertTrue(query + ": hasTableScanTimestamp()", factory.hasTableScanTimestamp());
                assertDirection(query, expectedDirection, factory);
                Assert.assertTrue(query + ": hasStreamableTimestamp()", ParquetExportMode.hasStreamableTimestamp(factory));
                assertTableScanTimestampHolds(query, factory, expectedRowCount);
            }
        } catch (Throwable th) {
            recordFailure(query, th);
        }
    }

    // A view's plan names the plan the view wraps; the wrapper itself answers for it.
    private void assertView(String query, int expectedRowCount, int expectedDirection, String... planFragments) {
        try (RecordCursorFactory factory = select(query)) {
            Assert.assertTrue(query + ": the view check wraps the plan", ParquetExportMode.unwrapFactory(factory) instanceof StaleViewCheckFactory);
            Assert.assertTrue(query + ": the plan reads a retyped column", factory.hasParquetConvertedColumns(sqlExecutionContext));
        } catch (Throwable th) {
            recordFailure(query, th);
            return;
        }
        assertTrue(query, expectedRowCount, expectedDirection, planFragments);
    }

    private void recordFailure(String query, Throwable th) {
        failures.append(th instanceof AssertionError ? th.getMessage() : query + ": " + th).append('\n');
    }
}

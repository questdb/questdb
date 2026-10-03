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
import io.questdb.cairo.TableReader;
import io.questdb.griffin.engine.table.FwdTableReaderPageFrameCursor;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * {@code ORDER BY <indexed symbol>} over an index scan ({@code sym = ...}, {@code sym IN (...)},
 * or the sorted-symbol-index scan) may drop the sort only when the scan emits the result in key
 * order as a whole, not merely within each page frame. A partition larger than
 * {@code cairo.sql.page.frame.max.rows} is split into several page frames, so these tests shrink
 * the frame size until every partition spans several frames.
 * <p>
 * Fixture: row {@code x} (1..24) has {@code sym} A, B or C for {@code x % 3} = 1, 2, 0 and
 * {@code ts} = (x - 1) * 2h. Under {@code PARTITION BY DAY} that is two partitions of 12 rows,
 * 1970-01-01 (x 1..12) and 1970-01-02 (x 13..24).
 */
@RunWith(Parameterized.class)
public class OrderByIndexedSymbolKeyMajorTest extends AbstractCairoTest {
    private static final long HOUR = 3_600_000_000L;
    private static final int ROWS = 24;
    private final String indexType;

    public OrderByIndexedSymbolKeyMajorTest(String indexType) {
        this.indexType = indexType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"bitmap"},
                {"posting"},
        });
    }

    @Override
    public void setUp() {
        super.setUp();
        // 4 rows per page frame: a 24-row PARTITION BY NONE table is 6 frames and each
        // 12-row day partition is 3 frames. reset() in the next setUp() restores the defaults.
        sqlExecutionContext.changePageFrameSizes(1, 4);
    }

    @Test
    public void testInListBindVariablesOrderBySymNoneMultiFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            bindVariableService.clear();
            bindVariableService.setStr(0, "B");
            bindVariableService.setStr(1, "A");
            assertQuery("select sym, x, ts from t where sym in ($1, $2) order by sym")
                    .withPlanContaining("FilterOnValues")
                    .returns(expected(new String[]{"A", "B"}, false, 1, ROWS));
            assertKeyMajorPlan("select sym, x, ts from t where sym in ($1, $2) order by sym", true);
        });
    }

    @Test
    public void testInListBindVariablesOrderBySymTsNoneMultiFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            bindVariableService.clear();
            bindVariableService.setStr("a", "B");
            bindVariableService.setStr("b", "C");
            assertQuery("select sym, x, ts from t where sym in (:a, :b) order by sym, ts")
                    .withPlanContaining("FilterOnValues")
                    .returns(expected(new String[]{"B", "C"}, false, 1, ROWS));
            assertKeyMajorPlan("select sym, x, ts from t where sym in (:a, :b) order by sym, ts", true);
        });
    }

    @Test
    public void testInListOrderBySymDayMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            final String query = "select sym, x, ts from t where sym in ('A', 'B') order by sym";
            assertQuery(query)
                    .withPlanContaining("FilterOnValues")
                    .returns(expected(new String[]{"A", "B"}, false, 1, ROWS));
            // two partitions, but 2 keys x 6 frames is well under the cursor-open threshold
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testInListOrderBySymDayMultiPartitionWithInterval() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            final String query = "select sym, x, ts from t where sym in ('C', 'A') and ts >= '1970-01-01T06:00' order by sym, ts";
            assertQuery(query)
                    .withPlanContaining("FilterOnValues")
                    .returns(expected(new String[]{"A", "C"}, false, 4, ROWS));
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testInListOrderBySymDayOnePartitionInterval() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            final String query = "select sym, x, ts from t where sym in ('A', 'B') and ts in '1970-01-02' order by sym";
            assertQuery(query)
                    .withPlanContaining("FilterOnValues")
                    .returns(expected(new String[]{"A", "B"}, false, 13, ROWS));
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testInListOrderBySymDescNoneMultiFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            final String query = "select sym, x, ts from t where sym in ('A', 'B', 'C') order by sym desc";
            assertQuery(query)
                    .withPlanContaining("FilterOnValues")
                    .returns(expected(new String[]{"C", "B", "A"}, false, 1, ROWS));
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testInListOrderBySymDescTsDescNoneMultiFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            final String query = "select sym, x, ts from t where sym in ('A', 'B') order by sym desc, ts desc";
            assertQuery(query)
                    .withPlanContaining("FilterOnValues")
                    .returns(expected(new String[]{"B", "A"}, true, 1, ROWS));
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testInListOrderBySymNoneMultiFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            final String query = "select sym, x, ts from t where sym in ('A', 'B') order by sym";
            assertQuery(query)
                    .withPlanContaining("FilterOnValues")
                    .returns(expected(new String[]{"A", "B"}, false, 1, ROWS));
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testInListOrderBySymNoneMultiFrameWithFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            // x % 2 = 0 is a residual filter evaluated inside the index cursor
            final String query = "select sym, x, ts from t where sym in ('A', 'B') and x % 2 = 0 order by sym";
            final StringSink expected = new StringSink();
            expected.put("sym\tx\tts\n");
            appendRows(expected, "A", false, 1, ROWS, true);
            appendRows(expected, "B", false, 1, ROWS, true);
            assertQuery(query)
                    .withPlanContaining("FilterOnValues")
                    .returns(expected);
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testInListOrderBySymTsDescNoneMultiFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            final String query = "select sym, x, ts from t where sym in ('A', 'B') order by sym, ts desc";
            assertQuery(query)
                    .withPlanContaining("FilterOnValues")
                    .returns(expected(new String[]{"A", "B"}, true, 1, ROWS));
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testInListWindowOrderBySymDayMultiPartition() throws Exception {
        // the shape of NYSE TAQ query 50, which has no time filter: several partitions
        assertWindowOrderBySym("DAY");
    }

    @Test
    public void testInListWindowOrderBySymNoneMultiFrame() throws Exception {
        // The shape of NYSE TAQ query 50: a per-symbol moving window over an IN list, ordered by
        // symbol. The key-major scan feeds the window in symbol order, so there is no sort, and a
        // covering index is not used: its k-way merge into timestamp order would only be undone
        // by the sort.
        assertWindowOrderBySym("NONE");
    }


    @Test
    public void testInListWithLimitNoneMultiFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            final String query = "select sym, x, ts from t where sym in ('B', 'A') order by sym limit 6";
            // the sort knows its size, the index scan does not
            assertQuery(query).sizeMayVary().returns("""
                    sym\tx\tts
                    A\t1\t1970-01-01T00:00:00.000000Z
                    A\t4\t1970-01-01T06:00:00.000000Z
                    A\t7\t1970-01-01T12:00:00.000000Z
                    A\t10\t1970-01-01T18:00:00.000000Z
                    A\t13\t1970-01-02T00:00:00.000000Z
                    A\t16\t1970-01-02T06:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testInListOrderByTsDayMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            // timestamp order: the heap cursor merges the keys per frame, frames are in order
            final String query = "select sym, x, ts from t where sym in ('A', 'C') order by ts";
            final StringSink expected = new StringSink();
            expected.put("sym\tx\tts\n");
            for (int x = 1; x <= ROWS; x++) {
                if (!"B".equals(symOf(x))) {
                    appendRows(expected, symOf(x), false, x, x, false);
                }
            }
            assertQuery(query).timestamp("ts").returns(expected);
        });
    }

    @Test
    public void testInListOrderBySymCursorOpenThreshold() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            // the planner's estimate: 2 keys x the page frames of both partitions
            final long rowsPerFrame = FwdTableReaderPageFrameCursor.calculatePageFrameRowLimit(
                    0,
                    12,
                    sqlExecutionContext.getPageFrameMinRows(),
                    sqlExecutionContext.getPageFrameMaxRows(),
                    sqlExecutionContext.getSharedQueryWorkerCount()
            );
            final long cursorOpens = 2 * 2 * ((12 + rowsPerFrame - 1) / rowsPerFrame);
            final String expected = expected(new String[]{"A", "B"}, false, 1, ROWS);

            setProperty(PropertyKey.CAIRO_SQL_INDEX_KEY_MAJOR_MAX_CURSOR_OPENS, cursorOpens);
            final String atLimit = "select sym, x, ts from t where sym in ('A', 'B') order by sym";
            assertQuery(atLimit).returns(expected);
            assertKeyMajorPlan(atLimit, true);

            setProperty(PropertyKey.CAIRO_SQL_INDEX_KEY_MAJOR_MAX_CURSOR_OPENS, cursorOpens - 1);
            final String overLimit = "select sym, x, ts from t where sym in ('B', 'A') order by sym";
            assertQuery(overLimit).sizeMayVary().returns(expected);
            assertKeyMajorPlan(overLimit, false);

            // 0 turns the multi-partition key-major scan off, one partition is not affected
            setProperty(PropertyKey.CAIRO_SQL_INDEX_KEY_MAJOR_MAX_CURSOR_OPENS, 0);
            assertKeyMajorPlan(atLimit, false);
            assertKeyMajorPlan("select sym, x, ts from t where sym in ('A', 'B') and ts in '1970-01-01' order by sym", true);
        });
    }

    @Test
    public void testInListOrderBySymMultiPartitionWithParquetPartition() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            createTable("DAY");
            execute("alter table t convert partition to parquet list '1970-01-01'");
            // a Parquet partition anywhere in the table keeps the sort across partitions: each
            // row group would be decoded again for every key that misses the decode cache
            final String query = "select sym, x, ts from t where sym in ('A', 'C') order by sym, ts";
            assertQuery(query)
                    .withPlanContaining("FilterOnValues")
                    .returns(expected(new String[]{"A", "C"}, false, 1, ROWS));
            assertKeyMajorPlan(query, false);
        });
    }

    @Test
    public void testInListOrderBySymParquetPartition() throws Exception {
        // a Parquet partition is split into page frames by row group: 3 row groups of 4 rows
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            createTable("DAY");
            execute("alter table t convert partition to parquet list '1970-01-01'");
            final String query = "select sym, x, ts from t where sym in ('C', 'A') and ts in '1970-01-01' order by sym desc, ts desc";
            assertQuery(query)
                    .withPlanContaining("FilterOnValues")
                    .returns(expected(new String[]{"C", "A"}, true, 1, 12));
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testInListOrderBySymTsDescSplitPartitionMultiPartition() throws Exception {
        // as below, but over the whole table: three physical partitions, walked backward
        setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        assertMemoryLeak(() -> {
            createTable("DAY");
            try (TableReader ignore = getReader("t")) {
                execute("insert into t values ('B', 101, '1970-01-01T21:00'), ('A', 103, '1970-01-01T21:30')");
            }
            assertQuery("select count() from table_partitions('t')")
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n3\n");
            final String query = "select sym, x, ts from t where sym in ('B', 'A') order by sym desc, ts desc";
            assertQuery(query)
                    .withPlanContaining("FilterOnValues")
                    .returns("""
                            sym\tx\tts
                            B\t23\t1970-01-02T20:00:00.000000Z
                            B\t20\t1970-01-02T14:00:00.000000Z
                            B\t17\t1970-01-02T08:00:00.000000Z
                            B\t14\t1970-01-02T02:00:00.000000Z
                            B\t101\t1970-01-01T21:00:00.000000Z
                            B\t11\t1970-01-01T20:00:00.000000Z
                            B\t8\t1970-01-01T14:00:00.000000Z
                            B\t5\t1970-01-01T08:00:00.000000Z
                            B\t2\t1970-01-01T02:00:00.000000Z
                            A\t22\t1970-01-02T18:00:00.000000Z
                            A\t19\t1970-01-02T12:00:00.000000Z
                            A\t16\t1970-01-02T06:00:00.000000Z
                            A\t13\t1970-01-02T00:00:00.000000Z
                            A\t103\t1970-01-01T21:30:00.000000Z
                            A\t10\t1970-01-01T18:00:00.000000Z
                            A\t7\t1970-01-01T12:00:00.000000Z
                            A\t4\t1970-01-01T06:00:00.000000Z
                            A\t1\t1970-01-01T00:00:00.000000Z
                            """);
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testInListOrderBySymTsSplitPartition() throws Exception {
        // An O3 insert near the end of a partition, with a reader holding it open, splits the
        // partition in two. The interval below still hits one logical partition, so the sort is
        // dropped, but the scan spans two physical partitions: the key order must hold across them.
        setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        assertMemoryLeak(() -> {
            createTable("DAY");
            try (TableReader ignore = getReader("t")) {
                execute("insert into t values ('B', 101, '1970-01-01T21:00'), ('A', 103, '1970-01-01T21:30')");
            }
            // the split is what this test is about, so make sure it happened
            assertQuery("select count() from table_partitions('t')")
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n3\n");
            final String query = "select sym, x, ts from t where sym in ('B', 'A') and ts in '1970-01-01' order by sym, ts";
            assertQuery(query)
                    .withPlanContaining("FilterOnValues")
                    .returns("""
                            sym\tx\tts
                            A\t1\t1970-01-01T00:00:00.000000Z
                            A\t4\t1970-01-01T06:00:00.000000Z
                            A\t7\t1970-01-01T12:00:00.000000Z
                            A\t10\t1970-01-01T18:00:00.000000Z
                            A\t103\t1970-01-01T21:30:00.000000Z
                            B\t2\t1970-01-01T02:00:00.000000Z
                            B\t5\t1970-01-01T08:00:00.000000Z
                            B\t8\t1970-01-01T14:00:00.000000Z
                            B\t11\t1970-01-01T20:00:00.000000Z
                            B\t101\t1970-01-01T21:00:00.000000Z
                            """);
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testNotEqualsOrderBySymNoneMultiFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            final String query = "select sym, x, ts from t where sym != 'B' order by sym";
            assertQuery(query)
                    .withPlanContaining("FilterOnExcludedValues")
                    .returns(expected(new String[]{"A", "C"}, false, 1, ROWS));
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testNotInOrderBySymDayMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            final String query = "select sym, x, ts from t where sym not in ('B') order by sym";
            assertQuery(query)
                    .withPlanContaining("FilterOnExcludedValues")
                    .returns(expected(new String[]{"A", "C"}, false, 1, ROWS));
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testNotInOrderBySymDescTsDescNoneMultiFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            final String query = "select sym, x, ts from t where sym not in ('A') order by sym desc, ts desc";
            assertQuery(query)
                    .withPlanContaining("FilterOnExcludedValues")
                    .returns(expected(new String[]{"C", "B"}, true, 1, ROWS));
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testSingleKeyOrderBySymTsDescDayOnePartitionInterval() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            final String query = "select sym, x, ts from t where sym = 'C' and ts in '1970-01-01' order by sym, ts desc";
            assertQuery(query).returns(expected(new String[]{"C"}, true, 1, 12));
            // a backward index scan inside forward page frames is not descending across frames
            assertKeyMajorPlan(query, false);
        });
    }

    @Test
    public void testSingleKeyOrderByTsDescDayMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            final String query = "select sym, x, ts from t where sym = 'A' order by ts desc";
            assertQuery(query).timestampDesc("ts").returns(expected(new String[]{"A"}, true, 1, ROWS));
        });
    }

    @Test
    public void testSingleKeyOrderByTsDescNoneMultiFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            final String query = "select sym, x, ts from t where sym = 'B' order by ts desc";
            assertQuery(query).timestampDesc("ts").returns(expected(new String[]{"B"}, true, 1, ROWS));
        });
    }

    @Test
    public void testSingleKeyOrderBySymNoneMultiFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            final String query = "select sym, x, ts from t where sym = 'A' order by sym";
            assertQuery(query).returns(expected(new String[]{"A"}, false, 1, ROWS));
            // a single key is trivially key-major, whatever the frame count
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testSingleKeyOrderBySymTsDescNoneMultiFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            final String query = "select sym, x, ts from t where sym = 'A' order by sym, ts desc";
            assertQuery(query).returns(expected(new String[]{"A"}, true, 1, ROWS));
            // a backward index scan inside forward page frames is not descending across frames
            assertKeyMajorPlan(query, false);
        });
    }

    @Test
    public void testSingleKeyOrderBySymTsNoneMultiFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            final String query = "select sym, x, ts from t where sym = 'B' order by sym, ts";
            assertQuery(query).returns(expected(new String[]{"B"}, false, 1, ROWS));
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testSortedSymbolIndexOrderBySymOnePartitionInterval() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            // no key filter: with a bitmap index this is the SortedSymbolIndex scan, a posting
            // index sorts
            final String query = "select sym, x, ts from t where ts in '1970-01-01' order by sym";
            assertQuery(query).returns(expected(new String[]{"A", "B", "C"}, false, 1, 12));
            assertKeyMajorPlan(query, "bitmap".equals(indexType));
        });
    }

    @Test
    public void testSortedSymbolIndexOrderBySymTsDescOnePartitionInterval() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            final String query = "select sym, x, ts from t where ts in '1970-01-02' order by sym desc, ts desc";
            assertQuery(query).returns(expected(new String[]{"C", "B", "A"}, true, 13, ROWS));
            assertKeyMajorPlan(query, "bitmap".equals(indexType));
        });
    }

    private static void appendRows(StringSink sink, String sym, boolean desc, int xLo, int xHi, boolean evenOnly) {
        for (int i = 0, n = xHi - xLo + 1; i < n; i++) {
            final int x = desc ? xHi - i : xLo + i;
            if (!sym.equals(symOf(x)) || (evenOnly && x % 2 != 0)) {
                continue;
            }
            sink.put(sym).put('\t').put(x).put('\t');
            MicrosFormatUtils.appendDateTimeUSec(sink, (x - 1) * 2 * HOUR);
            sink.put('\n');
        }
    }

    private static String expected(String[] symOrder, boolean tsDesc, int xLo, int xHi) {
        final StringSink sink = new StringSink();
        sink.put("sym\tx\tts\n");
        for (String sym : symOrder) {
            appendRows(sink, sym, tsDesc, xLo, xHi, false);
        }
        return sink.toString();
    }

    private static String symOf(long x) {
        return switch ((int) (x % 3)) {
            case 1 -> "A";
            case 2 -> "B";
            default -> "C";
        };
    }

    private void assertWindowOrderBySym(String partitionBy) throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table q (sym symbol index type " + indexType
                            + ("posting".equals(indexType) ? " include (x)" : "")
                            + ", x long, ts timestamp) timestamp(ts) partition by " + partitionBy
            );
            execute(
                    "insert into q select" +
                            " case when x % 3 = 1 then 'A' when x % 3 = 2 then 'B' else 'C' end," +
                            " x," +
                            " ((x - 1) * " + (2 * HOUR) + ")::timestamp" +
                            " from long_sequence(" + ROWS + ")"
            );
            final String query = "select sym, x, mavg from (" +
                    " select sym, x, ts, avg(x) over (partition by sym rows between 4 preceding and current row) mavg" +
                    " from q where sym in ('B', 'A') order by sym)";
            final StringSink expected = new StringSink();
            expected.put("sym\tx\tmavg\n");
            for (String sym : new String[]{"A", "B"}) {
                final long[] window = new long[5];
                int n = 0;
                for (int x = 1; x <= ROWS; x++) {
                    if (sym.equals(symOf(x))) {
                        window[n++ % 5] = x;
                        long sum = 0;
                        for (int i = 0, m = Math.min(n, 5); i < m; i++) {
                            sum += window[i];
                        }
                        expected.put(sym).put('\t').put(x).put('\t').put((double) sum / Math.min(n, 5)).put('\n');
                    }
                }
            }
            assertQuery(query)
                    .withPlanContaining("Window", "FilterOnValues symbolOrder: asc")
                    .withPlanNotContaining("CoveringIndex")
                    .noRandomAccess()
                    .returns(expected);
            assertKeyMajorPlan(query, true);
        });
    }

    private void assertKeyMajorPlan(String query, boolean expectSortElided) throws Exception {
        final StringSink plan = new StringSink();
        printSql("explain " + query, plan);
        boolean hasSort = false;
        for (String line : plan.toString().split("\n")) {
            final String trimmed = line.trim();
            if (trimmed.startsWith("Sort") && !trimmed.startsWith("SortedSymbolIndex") || trimmed.startsWith("Encode sort")) {
                hasSort = true;
                break;
            }
        }
        Assert.assertEquals("sort elided [query=" + query + ", plan=\n" + plan + "]", expectSortElided, !hasSort);
    }

    private void createTable(String partitionBy) throws Exception {
        execute(
                "create table t (sym symbol index type " + indexType + ", x long, ts timestamp) timestamp(ts) partition by " + partitionBy
        );
        execute(
                "insert into t select" +
                        " case when x % 3 = 1 then 'A' when x % 3 = 2 then 'B' else 'C' end," +
                        " x," +
                        " ((x - 1) * " + (2 * HOUR) + ")::timestamp" +
                        " from long_sequence(" + ROWS + ")"
        );
    }
}

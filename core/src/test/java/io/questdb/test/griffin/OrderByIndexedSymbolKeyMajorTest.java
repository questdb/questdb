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
            assertKeyMajorPlan("select sym, x, ts from t where sym in ($1, $2) order by sym", false);
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
            assertKeyMajorPlan("select sym, x, ts from t where sym in (:a, :b) order by sym, ts", false);
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
            // two partitions: the key-major guarantee does not reach across partitions
            assertKeyMajorPlan(query, false);
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
            assertKeyMajorPlan(query, false);
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
            assertKeyMajorPlan(query, false);
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
            assertKeyMajorPlan(query, false);
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
            assertKeyMajorPlan(query, false);
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
            assertKeyMajorPlan(query, false);
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
            assertKeyMajorPlan(query, false);
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
            assertKeyMajorPlan(query, false);
        });
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
    public void testNotEqualsOrderBySymNoneMultiFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            final String query = "select sym, x, ts from t where sym != 'B' order by sym";
            assertQuery(query)
                    .withPlanContaining("FilterOnExcludedValues")
                    .returns(expected(new String[]{"A", "C"}, false, 1, ROWS));
            assertKeyMajorPlan(query, false);
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
            assertKeyMajorPlan(query, false);
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

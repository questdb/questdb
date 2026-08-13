/*******************************************************************************
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

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import io.questdb.std.str.StringSink;
import org.junit.Test;

/**
 * MULTIPLE intervals over a composite multi-cell day, in both scan directions.
 * <p>
 * The sibling-cell fixes trade one thing away: advancing to a sibling cell abandons the current cell for
 * any LATER interval, because the partition walk is monotonic. Where that would lose rows the cursors
 * throw {@code multipleSubDayIntervalsOverMultiCellDayUnsupported} rather than drop them silently — a
 * deliberate loud refusal. Those throw sites were added without any test reaching them, and an
 * over-eager guard would turn working queries into errors, so this file exists to pin the boundary from
 * the outside: these ordinary multi-interval queries must keep returning exactly what the plain twin
 * returns.
 * <p>
 * Every case is asserted forward AND backward, because the two cursors are separate implementations of
 * the same walk and only one of them was fixed first — the backward one shipped broken for a while
 * precisely because nothing here read backwards.
 * <p>
 * The backward comparison projects only {@code ts}: a single-key {@code ORDER BY ts DESC} is what
 * selects the backward cursor (a multi-key sort silently sorts over a FORWARD scan instead), and rows
 * tied on timestamp are identical in that projection, so the comparison cannot flap on tie order.
 */
public class CompositeMultiIntervalTest extends AbstractCairoTest {

    /**
     * Two disjoint sub-day windows inside ONE day, over a day with three cells. This is the shape the
     * unsupported-guard reasons about; it must either answer correctly or refuse loudly, and today it
     * answers correctly.
     */
    @Test
    public void testTwoSubDayIntervalsSameDay() throws Exception {
        assertMemoryLeak(() -> {
            createAndFillTwins();
            assertTwinEqualBothDirections(
                    " WHERE (ts >= '2023-01-02T01:00:00.000000Z' AND ts <= '2023-01-02T02:00:00.000000Z')"
                            + " OR (ts >= '2023-01-02T05:00:00.000000Z' AND ts <= '2023-01-02T06:00:00.000000Z')");
        });
    }

    /**
     * Three windows in one day: more chances for a cell to be abandoned between intervals.
     */
    @Test
    public void testThreeSubDayIntervalsSameDay() throws Exception {
        assertMemoryLeak(() -> {
            createAndFillTwins();
            assertTwinEqualBothDirections(
                    " WHERE (ts >= '2023-01-02T01:00:00.000000Z' AND ts <= '2023-01-02T01:30:00.000000Z')"
                            + " OR (ts >= '2023-01-02T03:00:00.000000Z' AND ts <= '2023-01-02T03:30:00.000000Z')"
                            + " OR (ts >= '2023-01-02T05:00:00.000000Z' AND ts <= '2023-01-02T05:30:00.000000Z')");
        });
    }

    /**
     * Windows in DIFFERENT days — the multi-day date-list shape the guard must never fire on, since no
     * cell is abandoned across a day boundary.
     */
    @Test
    public void testIntervalsInDifferentDays() throws Exception {
        assertMemoryLeak(() -> {
            createAndFillTwins();
            assertTwinEqualBothDirections(
                    " WHERE (ts >= '2023-01-01T01:00:00.000000Z' AND ts <= '2023-01-01T02:00:00.000000Z')"
                            + " OR (ts >= '2023-01-03T05:00:00.000000Z' AND ts <= '2023-01-03T06:00:00.000000Z')");
        });
    }

    /**
     * A cell whose rows span the whole day alongside a cell holding a single row, with two intervals —
     * the exact state the guard's condition is written against (the first interval fragments the wide
     * cell while a sibling still needs visiting, and the second interval reaches back into that cell).
     */
    @Test
    public void testIntervalsOverWideAndNarrowCells() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY, exch LAYOUT PLAIN WAL");
            execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String rows = "('2023-01-02T01:00:00.000000Z','A',1.0),"
                    + "('2023-01-02T02:00:00.000000Z','A',2.0),"
                    + "('2023-01-02T04:00:00.000000Z','A',4.0),"
                    + "('2023-01-02T06:00:00.000000Z','A',6.0),"
                    + "('2023-01-02T02:00:00.000000Z','B',20.0)";
            execute("INSERT INTO c VALUES " + rows);
            execute("INSERT INTO p VALUES " + rows);
            drainWalQueue();

            assertTwinEqualBothDirections(
                    " WHERE (ts >= '2023-01-02T01:30:00.000000Z' AND ts <= '2023-01-02T02:30:00.000000Z')"
                            + " OR (ts >= '2023-01-02T03:30:00.000000Z' AND ts <= '2023-01-02T04:30:00.000000Z')");
        });
    }

    /**
     * Intervals combined with a dimension predicate, so cell PRUNING and interval walking interact. A
     * pruned cell is skipped without consuming the interval, and this checks that interacting with
     * multiple intervals still agrees with the twin.
     */
    @Test
    public void testIntervalsWithDimensionFilter() throws Exception {
        assertMemoryLeak(() -> {
            createAndFillTwins();
            assertTwinEqualBothDirections(
                    " WHERE exch = 'B' AND ((ts >= '2023-01-02T01:00:00.000000Z' AND ts <= '2023-01-02T02:00:00.000000Z')"
                            + " OR (ts >= '2023-01-02T05:00:00.000000Z' AND ts <= '2023-01-02T06:00:00.000000Z'))");
            assertTwinEqualBothDirections(
                    " WHERE exch IN ('A','C') AND (ts >= '2023-01-01T02:00:00.000000Z' AND ts <= '2023-01-03T02:00:00.000000Z')");
        });
    }

    /**
     * Multiple intervals over the shape that actually breaks the cursors: cell E0 straddles the first
     * interval without matching it, while sibling E1 has the matching row. This is the one case in this
     * file that DISCRIMINATES -- it fails if either sibling-cell fix is reverted. The others above pass
     * with or without the fixes (every cell in their data has rows in every interval, so no cell is ever
     * wholly outside one) and are regression-locks for the guard, not proofs of the fix.
     */
    @Test
    public void testMultipleIntervalsOverNonMatchingCell() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY, exch LAYOUT PLAIN WAL");
            execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String rows = "('2023-01-02T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-02T03:00:00.000000Z','E0',3.0),"
                    + "('2023-01-02T02:00:00.000000Z','E1',2.0)";
            execute("INSERT INTO c VALUES " + rows);
            execute("INSERT INTO p VALUES " + rows);
            drainWalQueue();

            // one interval E0 straddles-but-misses, one it matches
            assertTwinEqualBothDirections(
                    " WHERE (ts >= '2023-01-02T02:00:00.000000Z' AND ts <= '2023-01-02T02:00:00.000000Z')"
                            + " OR (ts >= '2023-01-02T03:00:00.000000Z' AND ts <= '2023-01-02T03:00:00.000000Z')");
            // and the single-interval form, forward and backward
            assertTwinEqualBothDirections(" WHERE ts = '2023-01-02T02:00:00.000000Z'");
        });
    }

    private void assertTwinEqualBothDirections(String where) throws SqlException {
        // forward: full rows, deterministic total order
        final String orderAsc = " ORDER BY ts, exch, px";
        assertSqlCursors("SELECT * FROM p" + where + orderAsc, "SELECT * FROM c" + where + orderAsc);
        assertSqlCursors("SELECT count() FROM p" + where, "SELECT count() FROM c" + where);

        // backward: ts only, single sort key, and assert the plan really is a backward scan
        final StringSink plan = new StringSink();
        printSql("EXPLAIN SELECT ts FROM c" + where + " ORDER BY ts DESC", plan);
        TestUtils.assertContains(plan, "backward scan");
        assertSqlCursors(
                "SELECT ts FROM p" + where + " ORDER BY ts DESC",
                "SELECT ts FROM c" + where + " ORDER BY ts DESC");
    }

    private void createAndFillTwins() throws SqlException {
        execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY, exch LAYOUT PLAIN WAL");
        execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        final StringBuilder rows = new StringBuilder();
        boolean first = true;
        for (String day : new String[]{"2023-01-01", "2023-01-02", "2023-01-03"}) {
            for (int hour = 1; hour <= 6; hour++) {
                for (String exch : new String[]{"A", "B", "C"}) {
                    if (!first) {
                        rows.append(',');
                    }
                    first = false;
                    rows.append("('").append(day).append("T0").append(hour).append(":00:00.000000Z','")
                            .append(exch).append("',").append(hour).append(".0)");
                }
            }
        }
        execute("INSERT INTO c VALUES " + rows);
        execute("INSERT INTO p VALUES " + rows);
        drainWalQueue();
    }
}

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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * The ordering contract of {@code LATEST ON}, and what nesting it inside a time-series operator
 * therefore requires.
 * <p>
 * THE CONTRACT: {@code LATEST ON} guarantees NOTHING about the order of its output unless an
 * {@code ORDER BY} is provided. It follows that its output must NOT advertise a designated timestamp
 * of its own: designating one is an ordering claim, and a consumer that trusts it
 * ({@code SAMPLE BY}, {@code ASOF}/{@code LT JOIN}, the {@code ORDER BY ts} fast path) would read
 * rows in whatever order the base happened to scan while believing them ascending.
 * <p>
 * This file previously asserted the opposite. Two commits on this branch
 * ({@code d4329522a8}, {@code 69819aed34}) made the LATEST ON factories designate a timestamp --
 * motivated by nested time-series operators throwing "TIMESTAMP column is required but not
 * provided" -- and paired it with {@code SCAN_DIRECTION_OTHER} so consumers would not trust the
 * order. That was the wrong trade, and it silently broke SEVEN upstream tests
 * ({@code SqlCodeGeneratorTest}, {@code ExplainPlanTest}, {@code NestedSetOperationTest},
 * {@code TwapGroupByFunctionFactoryTest}) which had been asserting the correct contract all along.
 * Both changes are reverted.
 * <p>
 * WHAT NESTING NEEDS INSTEAD: an explicit {@code ORDER BY ts}, which sorts. Verified directly --
 * a {@code SAMPLE BY} over {@code LATEST ON} whose base is a {@code UNION ALL} is REFUSED without
 * one and SUCCEEDS with one. Where the base is a real table, the table's own designated timestamp
 * survives and nesting already works, because that designation reflects genuine storage order.
 * <p>
 * {@code x} is wrapped in an inner pass-through sub-query ({@code (select ts, k, v from x)}) so that
 * {@code LATEST ON} runs over a sub-query rather than directly over the table -- only that shape
 * routes through the factories this file is about.
 */
public class LatestByTimestampDesignationTest extends AbstractCairoTest {

    private static final String LATEST_ON_SUBQ =
            "(select ts, k, v from (select ts, k, v from x) latest on ts partition by k)";
    private static final String LATEST_ON_SUBQ_ORDERED = LATEST_ON_SUBQ.substring(0, LATEST_ON_SUBQ.length() - 1) + " order by ts)";

    @Test
    public void testNestedLatestOnAsofJoin() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            createSlaveTable();
            execute("create table tmp as " + LATEST_ON_SUBQ + " timestamp(ts) partition by day");
            assertSqlCursors(
                    "select * from tmp asof join y",
                    "select * from " + LATEST_ON_SUBQ_ORDERED + " asof join y"
            );
        });
    }

    @Test
    public void testNestedLatestOnOrderByTs() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            execute("create table tmp as " + LATEST_ON_SUBQ + " timestamp(ts) partition by day");
            assertSqlCursors(
                    "select * from tmp order by ts",
                    "select * from " + LATEST_ON_SUBQ + " order by ts"
            );
        });
    }

    @Test
    public void testNestedLatestOnSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            execute("create table tmp as " + LATEST_ON_SUBQ + " timestamp(ts) partition by day");
            assertSqlCursors(
                    "select ts, count() from tmp sample by 1h",
                    "select ts, count() from " + LATEST_ON_SUBQ_ORDERED + " sample by 1h"
            );
        });
    }

    private void createBaseTable() throws Exception {
        execute("create table x (ts timestamp, k symbol, v double) timestamp(ts) partition by day wal");
        // 'a' is inserted into the LATEST ON map first (map/emission position 0), but is
        // later updated to the LARGEST timestamp of the three keys -- its map position does
        // not move on update. So LATEST ON's emission order is [a, b, c] == ts [02:00, 00:05,
        // 00:10], which is provably NOT ascending.
        execute("insert into x values ('2024-01-01T00:00:00.000000Z', 'a', 1.0)");
        execute("insert into x values ('2024-01-01T00:05:00.000000Z', 'b', 2.0)");
        execute("insert into x values ('2024-01-01T00:10:00.000000Z', 'c', 3.0)");
        execute("insert into x values ('2024-01-01T02:00:00.000000Z', 'a', 4.0)");
        drainWalQueue();
    }

    private void createSlaveTable() throws Exception {
        execute("create table y (ts timestamp, p double) timestamp(ts) partition by day wal");
        execute("insert into y values ('2024-01-01T00:06:00.000000Z', 100.0)");
        execute("insert into y values ('2024-01-01T00:12:00.000000Z', 200.0)");
        execute("insert into y values ('2024-01-01T01:30:00.000000Z', 300.0)");
        execute("insert into y values ('2024-01-01T03:00:00.000000Z', 400.0)");
        drainWalQueue();
    }

    // ---- Task 2: sibling LATEST-ON factories, over a real (non-subquery) table ------------
    //
    // Unlike LatestByLightRecordCursorFactory (used only for LATEST ON over a FROM sub-query), a
    // LATEST ON directly over a real table routes through one of several specialized table-scan
    // factories chosen by SqlCodeGenerator.generateLatestByTableQuery, depending on whether the
    // partition-by column(s) are symbol-typed, indexed, and whether a WHERE clause restricts the
    // key column to specific values. Each shape below is confirmed (via EXPLAIN; see the task
    // report) to route through a DIFFERENT concrete factory class. The same "materialize LATEST
    // ON into a real table first" oracle technique used above applies unchanged: a real table's
    // scan is always physically ts-ordered, so tmp's scan is a genuine correctness oracle
    // regardless of how the un-materialized LATEST ON cursor internally orders its output.
    //
    // The base data for every shape reuses the same divergence trick as createBaseTable(): the
    // 'a'-equivalent key is inserted FIRST but is later updated to carry the LARGEST timestamp of
    // the set, with the other keys keeping smaller timestamps in between -- so any internal
    // traversal order that tracks symbol/key insertion order (or index-key order, or base-scan
    // ordinal position for a non-ts-ordered base) rather than genuine timestamp order is exposed
    // by the ORDER BY ts assertion inside assertNestedLatestOnFamily, the most sensitive of the
    // three checks (see class doc above).
    //
    // requiresInnerOrderBy distinguishes two genuinely different, both-correct end states (see
    // task report Sec. 3 for the empirical EXPLAIN/RED-capture backing this per shape):
    //  - false: the factory already correctly self-advertises SCAN_DIRECTION_FORWARD (verified:
    //    ts IS the table's own physical scan order for these table-scan cursors), so SAMPLE BY /
    //    ASOF JOIN work DIRECTLY on the bare, un-ordered LATEST ON result -- no fix was needed,
    //    and asserting the bare form here is deliberately the STRONGER check: it would catch a
    //    future regression that wrongly kept advertising FORWARD, which the "ordered" workaround
    //    form below would silently paper over.
    //  - true: the factory does not (or, pre-fix, did not) provide input SAMPLE BY / ASOF JOIN
    //    will accept directly, so the query needs the explicit inner ORDER BY that Task 1
    //    established as the correct, general way to feed a not-provably-ordered LATEST ON result
    //    into those two operators (a pre-existing QuestDB rule: they require already-ASC input
    //    and throw rather than insert a sort of their own).
    private void assertNestedLatestOnFamily(String latestOnSql, boolean requiresInnerOrderBy) throws Exception {
        execute("create table tmp as (" + latestOnSql + ") timestamp(ts) partition by day");
        final String bare = "(" + latestOnSql + ")";
        final String sampleByAsofSource = requiresInnerOrderBy ? "(" + latestOnSql + " order by ts)" : bare;
        assertSqlCursors("select * from tmp order by ts", "select * from " + bare + " order by ts");
        assertSqlCursors("select ts, count() from tmp sample by 1h", "select ts, count() from " + sampleByAsofSource + " sample by 1h");
        assertSqlCursors("select * from tmp asof join y", "select * from " + sampleByAsofSource + " asof join y");
    }

    @Test
    public void testBareLatestOnNonIndexedSymbol() throws Exception {
        // LatestByDeferredListValuesFilteredRecordCursorFactory (confirmed via EXPLAIN) -- the
        // factory actually selected for the common, unqualified "x latest on ts partition by k"
        // shape (single non-indexed symbol key, no WHERE clause).
        assertMemoryLeak(() -> {
            execute("create table ta (ts timestamp, k symbol, v double) timestamp(ts) partition by day wal");
            execute("insert into ta values ('2024-01-01T00:00:00.000000Z', 'a', 1.0)");
            execute("insert into ta values ('2024-01-01T00:05:00.000000Z', 'b', 2.0)");
            execute("insert into ta values ('2024-01-01T00:10:00.000000Z', 'c', 3.0)");
            execute("insert into ta values ('2024-01-01T02:00:00.000000Z', 'a', 4.0)");
            drainWalQueue();
            createSlaveTable();
            assertNestedLatestOnFamily("select ts, k, v from ta latest on ts partition by k", false);
        });
    }

    @Test
    public void testBareLatestOnIndexedSymbol() throws Exception {
        // LatestByAllIndexedRecordCursorFactory (confirmed via EXPLAIN): indexed symbol column,
        // no WHERE clause, no filter.
        assertMemoryLeak(() -> {
            execute("create table tb (ts timestamp, k symbol index, v double) timestamp(ts) partition by day wal");
            execute("insert into tb values ('2024-01-01T00:00:00.000000Z', 'a', 1.0)");
            execute("insert into tb values ('2024-01-01T00:05:00.000000Z', 'b', 2.0)");
            execute("insert into tb values ('2024-01-01T00:10:00.000000Z', 'c', 3.0)");
            execute("insert into tb values ('2024-01-01T02:00:00.000000Z', 'a', 4.0)");
            drainWalQueue();
            createSlaveTable();
            assertNestedLatestOnFamily("select ts, k, v from tb latest on ts partition by k", false);
        });
    }

    @Test
    public void testLatestOnWhereKeyInSubquery() throws Exception {
        // LatestBySubQueryRecordCursorFactory (confirmed via EXPLAIN): indexed key column
        // restricted by "k in (<subquery>)".
        assertMemoryLeak(() -> {
            execute("create table tc (ts timestamp, k symbol index, v double) timestamp(ts) partition by day wal");
            execute("insert into tc values ('2024-01-01T00:00:00.000000Z', 'a', 1.0)");
            execute("insert into tc values ('2024-01-01T00:05:00.000000Z', 'b', 2.0)");
            execute("insert into tc values ('2024-01-01T00:10:00.000000Z', 'c', 3.0)");
            execute("insert into tc values ('2024-01-01T02:00:00.000000Z', 'a', 4.0)");
            drainWalQueue();
            createSlaveTable();
            assertNestedLatestOnFamily(
                    "select ts, k, v from tc where k in (select k from tc where v > 0) latest on ts partition by k",
                    false
            );
        });
    }

    @Test
    public void testLatestOnWhereKeyInList() throws Exception {
        // LatestByValuesIndexedFilteredRecordCursorFactory (confirmed via EXPLAIN): indexed key
        // column restricted by an explicit "k in ('a','b','c')" literal list (more than one
        // value, so the single-value LatestByValue*Indexed*Filtered factories are bypassed).
        assertMemoryLeak(() -> {
            execute("create table td (ts timestamp, k symbol index, v double) timestamp(ts) partition by day wal");
            execute("insert into td values ('2024-01-01T00:00:00.000000Z', 'a', 1.0)");
            execute("insert into td values ('2024-01-01T00:05:00.000000Z', 'b', 2.0)");
            execute("insert into td values ('2024-01-01T00:10:00.000000Z', 'c', 3.0)");
            execute("insert into td values ('2024-01-01T02:00:00.000000Z', 'a', 4.0)");
            drainWalQueue();
            createSlaveTable();
            assertNestedLatestOnFamily(
                    "select ts, k, v from td where k in ('a','b','c') latest on ts partition by k",
                    false
            );
        });
    }

    @Test
    public void testLatestOnMultiColumnPartitionBy() throws Exception {
        // LatestByAllSymbolsFilteredRecordCursorFactory (confirmed via EXPLAIN): multi-column
        // "partition by k1, k2" where all key columns are symbols.
        assertMemoryLeak(() -> {
            execute("create table te (ts timestamp, k1 symbol, k2 symbol, v double) timestamp(ts) partition by day wal");
            execute("insert into te values ('2024-01-01T00:00:00.000000Z', 'a', 'x', 1.0)");
            execute("insert into te values ('2024-01-01T00:05:00.000000Z', 'b', 'y', 2.0)");
            execute("insert into te values ('2024-01-01T00:10:00.000000Z', 'c', 'z', 3.0)");
            execute("insert into te values ('2024-01-01T02:00:00.000000Z', 'a', 'x', 4.0)");
            drainWalQueue();
            createSlaveTable();
            assertNestedLatestOnFamily("select ts, k1, k2, v from te latest on ts partition by k1, k2", false);
        });
    }

    @Test
    public void testLatestOnNonSymbolKey() throws Exception {
        // LatestByAllFilteredRecordCursorFactory (confirmed via EXPLAIN): the partition-by column
        // is not a symbol (a long here), so the indexed/symbol-specialized paths are unavailable
        // regardless of any index.
        assertMemoryLeak(() -> {
            execute("create table tf (ts timestamp, k long, v double) timestamp(ts) partition by day wal");
            execute("insert into tf values ('2024-01-01T00:00:00.000000Z', 1, 1.0)");
            execute("insert into tf values ('2024-01-01T00:05:00.000000Z', 2, 2.0)");
            execute("insert into tf values ('2024-01-01T00:10:00.000000Z', 3, 3.0)");
            execute("insert into tf values ('2024-01-01T02:00:00.000000Z', 1, 4.0)");
            drainWalQueue();
            createSlaveTable();
            assertNestedLatestOnFamily("select ts, k, v from tf latest on ts partition by k", false);
        });
    }

    @Test
    public void testLatestOnOverSubQueryWithoutRandomAccess() throws Exception {
        // LatestByRecordCursorFactory (the "non-light" sibling of Task 1's fix): generateLatestBy()'s
        // generic post-scan path is chosen (LATEST ON is over a FROM sub-query, not a real
        // table), and that sub-query's result (a UNION ALL) does not support random access, so
        // LatestByLightRecordCursorFactory (which requires random access) is unavailable and the
        // map/row-index sibling is used instead. Confirmed via EXPLAIN ("LatestBy" without
        // "light" in the plan type).
        //
        // Unlike every other shape in this file, this factory WAS buggy before this task's fix --
        // and in exactly the same way as the pre-Task-1 light factory: its constructor passed
        // base.getMetadata() straight through unmodified, so whenever base itself did not
        // designate a timestamp (true for a UNION ALL of two tables, confirmed empirically: RED
        // capture below), the factory's OWN output had NO designated timestamp at all --
        // "TIMESTAMP column is required but not provided" / "left side of time series join has no
        // timestamp" -- not merely a scan-direction gap. Fixed the same way as Task 1: designate
        // via buildMetadata(base, timestampIndex) + advertise SCAN_DIRECTION_OTHER.
        //
        // The base is deliberately NOT already ascending in scan order: the first UNION ALL
        // branch (tg1) contributes the row with the LARGEST timestamp; the second branch (tg2)
        // contributes two rows with smaller timestamps. Each key has only one candidate row (no
        // in-key update), so "latest per key" is trivial, isolating the question this test
        // targets: does the factory's OWN scan-direction advertisement (independent of which row
        // wins per key) correctly reflect that its output follows the base's own (non-ts-ordered)
        // scan order, rather than defaulting to a wrongly-trusted forward/ascending claim.
        assertMemoryLeak(() -> {
            execute("create table tg1 (ts timestamp, k symbol, v double) timestamp(ts) partition by day wal");
            execute("insert into tg1 values ('2024-01-01T02:00:00.000000Z', 'a', 1.0)");
            execute("create table tg2 (ts timestamp, k symbol, v double) timestamp(ts) partition by day wal");
            execute("insert into tg2 values ('2024-01-01T00:05:00.000000Z', 'b', 2.0)");
            execute("insert into tg2 values ('2024-01-01T00:10:00.000000Z', 'c', 3.0)");
            drainWalQueue();
            createSlaveTable();
            final String latestOnSql =
                    "select ts, k, v from (select ts, k, v from tg1 union all select ts, k, v from tg2) latest on ts partition by k";
            assertNestedLatestOnFamily(latestOnSql, true);
            // The contract, both halves. WITHOUT an explicit ORDER BY, a SAMPLE BY over this LATEST ON
            // must be REFUSED -- the sub-query has no designated timestamp, because LATEST ON promises
            // no order, and SAMPLE BY will not invent a sort. WITH an explicit ORDER BY ts it must
            // SUCCEED, because the sort is what actually establishes the order the operator needs.
            //
            // The refusal reason asserted here is "TIMESTAMP column is required but not provided",
            // which is the honest one: there genuinely is no designated timestamp. (While this branch
            // designated one anyway, the reason instead read "ASC order ... required" -- ts designated
            // but not provably ordered. Reverting the designation restores the accurate message.)
            // The contract itself, asserted directly rather than inferred from an error message:
            // LATEST ON output carries NO designated timestamp. QueryAssertion expects exactly that by
            // default -- naming a timestamp requires an explicit timestamp*() step -- so this fails the
            // moment a factory starts designating one again.
            assertQuery(latestOnSql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("ts\tk\tv\n"
                            + "2024-01-01T02:00:00.000000Z\ta\t1.0\n"
                            + "2024-01-01T00:05:00.000000Z\tb\t2.0\n"
                            + "2024-01-01T00:10:00.000000Z\tc\t3.0\n");

            assertExceptionNoLeakCheck("select ts, count() from (" + latestOnSql + ") sample by 1h", -1,
                    "TIMESTAMP column is required but not provided");
            // ... and the same query with an explicit ORDER BY ts works.
            assertQuery("select ts, count() from (" + latestOnSql + " order by ts) sample by 1h")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestampAsc("ts")
                    .returns("ts\tcount\n"
                            + "2024-01-01T00:00:00.000000Z\t2\n"
                            + "2024-01-01T02:00:00.000000Z\t1\n");
        });
    }
}

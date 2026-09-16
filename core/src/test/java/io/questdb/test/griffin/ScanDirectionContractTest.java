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

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.table.FilterOnSubQueryRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * RecordCursorFactory.getScanDirection() is a positive assertion consumed by downstream
 * time-series joins/consumers to validate designated-timestamp order. A factory that claims
 * SCAN_DIRECTION_FORWARD while not actually emitting ascending-timestamp rows lets those
 * consumers skip the sort/validation they would otherwise apply, silently producing rows that
 * violate the consumer's ordering invariant (e.g. an ASOF join's b.ts &lt;= a.ts).
 * <p>
 * This class collects one contract test per factory that has been audited and found to lie
 * about (or, after correction, honestly report) its scan direction: for each such factory, a
 * consumer that requires ascending designated-timestamp order must refuse to compile a query
 * built over it, rather than silently accepting sym/other-ordered input.
 * <p>
 * SortedSymbolIndexRecordCursorFactory was the first of six families being audited this way; the
 * group-by family is the second, the union family the third. When the remaining ones are
 * corrected, add one test method per factory here rather than starting a new class or a new
 * assertion pattern - the shape below (sub-query re-attaches a designated timestamp with
 * {@code timestamp(col)}, then feeds an ASOF/LT/SPLICE join or other order-requiring consumer) is
 * deliberately reusable.
 * <p>
 * Each corrected factory gets a pair: a refusal test asserting the specific error message, and a
 * positive control proving the fixture still selects that factory and still returns every row. A
 * factory correctly declaring FORWARD gets the mirror image - an acceptance test - so that the
 * refusals stay evidence about the specific factory rather than about group-by in general.
 */
public class ScanDirectionContractTest extends AbstractCairoTest {

    private static final String FIVE_ROW_DDL =
            "create table t (ts timestamp, sym symbol, x long) timestamp(ts) partition by day";
    private static final String FIVE_ROW_ROWS =
            "insert into t values" +
                    " ('2024-01-01T00:00:00.000000Z','a',1)," +
                    " ('2024-01-01T00:00:01.000000Z','b',2)," +
                    " ('2024-01-01T00:00:02.000000Z','c',3)," +
                    " ('2024-01-01T00:00:03.000000Z','d',4)," +
                    " ('2024-01-01T00:00:04.000000Z','e',5)";
    private static final String HORIZON_SLAVE_DDL =
            "create table p (ts timestamp, sym symbol, price double) timestamp(ts) partition by day";
    private static final String HORIZON_SLAVE_ROWS =
            "insert into p values" +
                    " ('2024-01-01T00:00:00.000000Z','a',10.0)," +
                    " ('2024-01-01T00:00:01.000000Z','b',20.0)," +
                    " ('2024-01-01T00:00:02.000000Z','c',30.0)," +
                    " ('2024-01-01T00:00:03.000000Z','d',40.0)," +
                    " ('2024-01-01T00:00:04.000000Z','e',50.0)";
    /**
     * A latest-by fixture whose map key-insertion order is deliberately NOT timestamp order: keys
     * are first seen as a, b, c (ts 00, 01, 03) but their latest rows land at ts 04, 02, 03. Using
     * the FIVE_ROW fixture instead would make the light latest-by tests vacuous - there every
     * symbol occurs exactly once, so first-seen order and latest-row order coincide and the cursor
     * comes out ascending by accident.
     */
    private static final String LATEST_BY_DDL =
            "create table v (ts timestamp, sym symbol, x long) timestamp(ts) partition by day";
    private static final String LATEST_BY_ROWS =
            "insert into v values" +
                    " ('2024-01-01T00:00:00.000000Z','a',1)," +
                    " ('2024-01-01T00:00:01.000000Z','b',2)," +
                    " ('2024-01-01T00:00:02.000000Z','b',3)," +
                    " ('2024-01-01T00:00:03.000000Z','c',4)," +
                    " ('2024-01-01T00:00:04.000000Z','a',5)";
    private static final String HORIZON_SLAVE2_DDL =
            "create table q (ts timestamp, sym symbol, ask double) timestamp(ts) partition by day";
    private static final String HORIZON_SLAVE2_ROWS =
            "insert into q values" +
                    " ('2024-01-01T00:00:00.000000Z','a',11.0)," +
                    " ('2024-01-01T00:00:02.000000Z','c',31.0)," +
                    " ('2024-01-01T00:00:04.000000Z','e',51.0)";
    /**
     * Join slave for the non-light latest-by tests. Joining t to this table on a non-timestamp
     * column is how the fixture gets a base that does NOT support random access, which is the
     * condition that routes latest-by to LatestByRecordCursorFactory instead of the light variant.
     */
    private static final String JOIN_SLAVE_DDL =
            "create table r (rts timestamp, y long) timestamp(rts) partition by day";
    private static final String JOIN_SLAVE_ROWS =
            "insert into r values" +
                    " ('2024-01-01T00:00:00.000000Z',1)," +
                    " ('2024-01-01T00:00:01.000000Z',2)," +
                    " ('2024-01-01T00:00:02.000000Z',3)," +
                    " ('2024-01-01T00:00:03.000000Z',4)," +
                    " ('2024-01-01T00:00:04.000000Z',5)";
    /**
     * Fixture for the two FilterOnSubQuery tests. The SYMBOL column must be indexed - that is what
     * routes {@code sym in (<sub-query>)} to FilterOnSubQueryRecordCursorFactory at all - and the
     * rows must straddle more than one partition, because the emission the tests are about only
     * shows up ACROSS partition frames: within a single frame the heap cursor is ascending whatever
     * the frame order is. The x values double as row identity in the assertions below; 'c' at
     * 2024-01-02T01 is the one row the sub-query filter excludes, so a fixture that silently stopped
     * filtering would be caught too.
     */
    private static final String SUBQUERY_FILTERED_DDL =
            "create table w (ts timestamp, sym symbol index, x long) timestamp(ts) partition by day";
    private static final String SUBQUERY_FILTERED_ROWS =
            "insert into w values" +
                    " ('2024-01-01T00:00:00.000000Z','a',1)," +
                    " ('2024-01-01T01:00:00.000000Z','b',2)," +
                    " ('2024-01-02T00:00:00.000000Z','a',3)," +
                    " ('2024-01-02T01:00:00.000000Z','c',4)," +
                    " ('2024-01-03T00:00:00.000000Z','b',5)," +
                    " ('2024-01-03T01:00:00.000000Z','a',6)";
    private static final String SUBQUERY_KEYS_DDL =
            "create table s (sym symbol)";
    private static final String SUBQUERY_KEYS_ROWS =
            "insert into s values ('a'), ('b')";

    /**
     * Positive control for the two not-keyed group-by tests: a not-keyed aggregate is the one
     * member of the group-by family that keeps SCAN_DIRECTION_FORWARD, so the ASOF join over it
     * must keep compiling. If this ever started failing, the refusals asserted for the keyed
     * siblings would no longer be evidence about keyed-ness - they would just mean every
     * group-by had been declared OTHER. The single result row is the whole reason FORWARD is
     * still honest here: one row cannot be out of ascending order with anything.
     */
    @Test
    public void testNotKeyedGroupByAcceptedAsAsofJoinMaster() throws Exception {
        assertQuery("""
                select a.ts ats, b.ts bts
                from ((select count() c, max(ts) ts from (select * from t union all select * from t)) timestamp(ts)) a
                asof join t b
                """)
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS)
                .noLeakCheck()
                .timestamp("ats")
                .noRandomAccess()
                .expectSize()
                .withPlanContaining("GroupBy vectorized: false", "AsOf Join")
                .returns("""
                        ats\tbts
                        2024-01-01T00:00:04.000000Z\t2024-01-01T00:00:04.000000Z
                        """);
    }

    /**
     * As {@link #testNotKeyedGroupByAcceptedAsAsofJoinMaster}, for the parallel not-keyed variant
     * (AsyncGroupByNotKeyedRecordCursorFactory). Both not-keyed factories emit exactly one row -
     * hasNext() yields the single aggregated record once and size() is 1 - so no pair of emitted
     * rows exists that could be out of ascending order and FORWARD holds vacuously. The fix commit
     * stopped them delegating to base.getScanDirection() and declared that reasoning explicitly;
     * this test pins the answer that declaration must produce.
     */
    @Test
    public void testParallelNotKeyedGroupByAcceptedAsAsofJoinMaster() throws Exception {
        assertQuery("""
                select a.ts ats, b.ts bts
                from ((select count() c, max(ts) ts from t) timestamp(ts)) a
                asof join t b
                """)
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS)
                .noLeakCheck()
                .timestamp("ats")
                .noRandomAccess()
                .expectSize()
                .withPlanContaining("Async Group By", "AsOf Join")
                .returns("""
                        ats\tbts
                        2024-01-01T00:00:04.000000Z\t2024-01-01T00:00:04.000000Z
                        """);
    }

    /**
     * Positive control for {@link #testParallelKeyedGroupByRefusedAsAsofJoinMaster} and
     * {@link #testParallelKeyedGroupByOrderByTimestampIsSorted}: grouping by the designated
     * timestamp itself selects AsyncGroupByRecordCursorFactory, and that query must keep
     * compiling and returning every row. Note the emitted order - ts 00, 02, 01, 04, 03 - which is
     * the group-by map's order, not ascending timestamp order, even though every input row was
     * scanned forward and the group key IS the designated timestamp.
     */
    @Test
    public void testParallelKeyedGroupByAloneStillReturnsAllRows() throws Exception {
        assertQuery("select ts, count() c from t group by ts")
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS)
                .noLeakCheck()
                .expectSize()
                .withPlanContaining("Async Group By")
                .returns("""
                        ts\tc
                        2024-01-01T00:00:00.000000Z\t1
                        2024-01-01T00:00:02.000000Z\t1
                        2024-01-01T00:00:01.000000Z\t1
                        2024-01-01T00:00:04.000000Z\t1
                        2024-01-01T00:00:03.000000Z\t1
                        """);
    }

    /**
     * The second consumer of the claim: ORDER BY elision. AsyncGroupByRecordCursorFactory used to
     * delegate to base.getScanDirection(), so a forward-scanning base made it answer FORWARD, and
     * "ORDER BY ts" over the re-attached designated timestamp was elided as already-satisfied -
     * measured on master as no Sort node in the plan and 159 of 299 adjacent steps descending.
     * With the corrected OTHER answer the sort is planned and the rows come out ascending. Revert
     * the fix and the sort disappears from the plan and this test fails.
     */
    @Test
    public void testParallelKeyedGroupByOrderByTimestampIsSorted() throws Exception {
        assertQuery("select * from (select ts, count() c from t group by ts) timestamp(ts) order by ts")
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS)
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .withPlanContaining("sort", "Async Group By")
                .returns("""
                        ts\tc
                        2024-01-01T00:00:00.000000Z\t1
                        2024-01-01T00:00:01.000000Z\t1
                        2024-01-01T00:00:02.000000Z\t1
                        2024-01-01T00:00:03.000000Z\t1
                        2024-01-01T00:00:04.000000Z\t1
                        """);
    }

    /**
     * AsyncGroupByRecordCursorFactory (the parallel keyed group-by) drains a map - the owner map,
     * or a ShardedMapCursor concatenating the per-shard maps - so it emits in map order within a
     * shard and shard order across them, never designated-timestamp order. It used to delegate to
     * base.getScanDirection(), which is the propagation path that carries a base's honest FORWARD
     * to the top of a query tree through SelectedRecordCursorFactory/VirtualRecordCursorFactory.
     * Wrapping the sub-query in {@code timestamp(ts)} re-attaches a designated timestamp the
     * cursor cannot honour, and the only thing left standing is getScanDirection() answering
     * honestly. Revert the fix and this test must fail - the join compiles and emits rows whose
     * slave timestamp is ahead of the master's.
     */
    @Test
    public void testParallelKeyedGroupByRefusedAsAsofJoinMaster() throws Exception {
        assertQuery("""
                select a.ts ats, b.ts bts
                from ((select ts, count() c from t group by ts) timestamp(ts)) a
                asof join t b
                """)
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS)
                .noLeakCheck()
                .failsWith("ASC order over TIMESTAMP column is required but not provided");
    }

    /**
     * Positive control for {@link #testSerialKeyedGroupByRefusedAsAsofJoinMaster}. A union-all
     * base does not support page framing, which is what routes this shape to the serial
     * GroupByRecordCursorFactory ("GroupBy vectorized: false" with keys) instead of the parallel
     * or Rosti group-bys. The emitted order - sym e, c, d, b, a, so ts 04, 02, 03, 01, 00 - is the
     * group-by map's order.
     */
    @Test
    public void testSerialKeyedGroupByAloneStillReturnsAllRows() throws Exception {
        assertQuery("select sym, max(ts) ts from (select * from t union all select * from t) group by sym")
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS)
                .noLeakCheck()
                .expectSize()
                .withPlanContaining("GroupBy vectorized: false", "keys: [sym]")
                .returns("""
                        sym\tts
                        e\t2024-01-01T00:00:04.000000Z
                        c\t2024-01-01T00:00:02.000000Z
                        d\t2024-01-01T00:00:03.000000Z
                        b\t2024-01-01T00:00:01.000000Z
                        a\t2024-01-01T00:00:00.000000Z
                        """);
    }

    /**
     * GroupByRecordCursorFactory (the serial keyed group-by) emits the entries of a map built by
     * MapFactory.createUnorderedMap: hash-slot order for the fixed-width Unordered{2,4,8,16}Map
     * keys, key-insertion order for the OrderedMap fallback. Neither is designated-timestamp
     * order. As above, {@code timestamp(ts)} re-attaches a designated timestamp the cursor cannot
     * honour; before the fix the ASOF join accepted this master and matched master rows against
     * slave rows ahead of them in time. Revert the fix and this test must fail.
     */
    @Test
    public void testSerialKeyedGroupByRefusedAsAsofJoinMaster() throws Exception {
        assertQuery("""
                select a.sym, a.ts ats, b.ts bts
                from ((select sym, max(ts) ts from (select * from t union all select * from t) group by sym) timestamp(ts)) a
                asof join t b
                """)
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS)
                .noLeakCheck()
                .failsWith("ASC order over TIMESTAMP column is required but not provided");
    }

    /**
     * Positive control for {@link #testVectorizedKeyedGroupByRefusedAsAsofJoinMaster}: the plain
     * "group by symbol" shape, which selects the vectorized Rosti group-by
     * (io.questdb.griffin.engine.groupby.vect.GroupByRecordCursorFactory, "GroupBy vectorized:
     * true"). It must keep compiling and returning every row. The ORDER BY is not decoration: the
     * factory's own emission order is Rosti hash-slot order, which is not even stable run to run
     * (two consecutive runs of this fixture emitted c,a,b,e,d and then a,c,e,b,d), so it cannot be
     * asserted literally - and a cursor whose row order changes between executions of the same
     * query over the same data is about as far from a FORWARD scan-direction claim as it gets.
     */
    @Test
    public void testVectorizedKeyedGroupByAloneStillReturnsAllRows() throws Exception {
        assertQuery("select sym, max(ts) ts from t group by sym order by sym")
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS)
                .noLeakCheck()
                .expectSize()
                .withPlanContaining("GroupBy vectorized: true", "keys: [sym]")
                .returns("""
                        sym\tts
                        a\t2024-01-01T00:00:00.000000Z
                        b\t2024-01-01T00:00:01.000000Z
                        c\t2024-01-01T00:00:02.000000Z
                        d\t2024-01-01T00:00:03.000000Z
                        e\t2024-01-01T00:00:04.000000Z
                        """);
    }

    /**
     * The vectorized Rosti keyed group-by walks the hash table's control bytes from slot 0
     * upwards, so it emits in key-hash order; a timestamp in its output is an aggregate value
     * (max(ts)) carried along, not an ordering. This is the factory that actually serves the
     * plain "group by sym" shape, so it was the one producing the measured violation: before the
     * fix this exact query compiled and returned 5 rows of which 3 matched a slave row AHEAD of
     * the master row in time (a/00 and b/01 both matched b.ts=02, d/03 matched b.ts=04). Revert
     * the fix and this test must fail.
     */
    @Test
    public void testVectorizedKeyedGroupByRefusedAsAsofJoinMaster() throws Exception {
        assertQuery("""
                select a.sym, a.ts ats, b.ts bts
                from ((select sym, max(ts) ts from t group by sym) timestamp(ts)) a
                asof join t b
                """)
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS)
                .noLeakCheck()
                .failsWith("ASC order over TIMESTAMP column is required but not provided");
    }

    /**
     * Positive control for {@link #testSortedSymbolIndexRefusedAsAsofJoinMaster}: the inner
     * "ORDER BY sym, ts" query, used on its own (without re-attaching a designated timestamp),
     * is exactly the shape that selects SortedSymbolIndexRecordCursorFactory, and it must keep
     * compiling and returning every row. If this ever starts failing, or if the plan stops
     * showing SortedSymbolIndex, the refusal asserted below would be vacuous - it could be
     * passing because the fixture broke, not because the scan-direction contract held.
     */
    @Test
    public void testSortedSymbolIndexAloneStillReturnsAllRows() throws Exception {
        assertQuery("select ts, sym from u where ts in '2024-01-02' order by sym, ts")
                .ddl(
                        "create table u (ts timestamp, sym symbol index) timestamp(ts) partition by day",
                        "insert into u values" +
                                " ('2024-01-02T00:00:00.000000Z','b')," +
                                " ('2024-01-02T00:00:01.000000Z','a')," +
                                " ('2024-01-02T00:00:02.000000Z','c')," +
                                " ('2024-01-02T00:00:03.000000Z','a')," +
                                " ('2024-01-02T00:00:04.000000Z','b')," +
                                " ('2024-01-02T00:00:05.000000Z','c')"
                )
                .noLeakCheck()
                .withPlan("""
                        SortedSymbolIndex
                            Index forward scan on: sym
                              symbolOrder: asc
                            Interval forward scan on: u
                              intervals: [("2024-01-02T00:00:00.000000Z","2024-01-02T23:59:59.999999Z")]
                        """)
                .returns("""
                        ts\tsym
                        2024-01-02T00:00:01.000000Z\ta
                        2024-01-02T00:00:03.000000Z\ta
                        2024-01-02T00:00:00.000000Z\tb
                        2024-01-02T00:00:04.000000Z\tb
                        2024-01-02T00:00:02.000000Z\tc
                        2024-01-02T00:00:05.000000Z\tc
                        """);
    }

    /**
     * SortedSymbolIndexRecordCursorFactory serves "ORDER BY sym[, ts]": it walks symbol keys in
     * sorted order and, for each key, emits that symbol's whole index row range before moving to
     * the next key, so designated-timestamp order restarts at every symbol boundary (see the
     * positive control above: sym 'a' at ts=...01 then ...03, then sym 'b' back at ts=...00).
     * <p>
     * SqlCodeGenerator already defends the ordinary path by nulling the sub-query's designated
     * timestamp (queryMeta.setTimestampIndex(-1)) right after selecting this factory. But
     * getTimestampIndex() resolves {@code timestamp(col)} by column NAME, so a user who wraps the
     * sub-query in {@code timestamp(ts)} re-attaches a designated timestamp the factory cannot
     * honour - bypassing that defence entirely. The only thing left standing between that shape
     * and silently-wrong output is factory.getScanDirection() answering honestly. This is exactly
     * the shape measured in the fix commit: with SCAN_DIRECTION_FORWARD (the pre-fix answer), an
     * ASOF join fed by this re-attached timestamp produced 64,649 of 86,400 rows violating
     * b.ts &lt;= a.ts. Revert the fix and this test must fail.
     */
    @Test
    public void testSortedSymbolIndexRefusedAsAsofJoinMaster() throws Exception {
        assertQuery("""
                select a.ts ats, b.ts bts
                from ((select ts, sym from u where ts in '2024-01-02' order by sym, ts) timestamp(ts)) a
                asof join u b
                """)
                .ddl(
                        "create table u (ts timestamp, sym symbol index) timestamp(ts) partition by day",
                        "insert into u values" +
                                " ('2024-01-02T00:00:00.000000Z','b')," +
                                " ('2024-01-02T00:00:01.000000Z','a')," +
                                " ('2024-01-02T00:00:02.000000Z','c')," +
                                " ('2024-01-02T00:00:03.000000Z','a')," +
                                " ('2024-01-02T00:00:04.000000Z','b')," +
                                " ('2024-01-02T00:00:05.000000Z','c')"
                )
                .noLeakCheck()
                .failsWith("ASC order over TIMESTAMP column is required but not provided");
    }

    /**
     * Positive control for the three UNION ALL refusals below, and the direct evidence for them.
     * The fixture rows are ascending in the table, and each branch is scanned forward, yet the
     * concatenated output runs 00, 01, 02, 03, 04 and then restarts at 00 - one descending step at
     * the branch boundary. Scaled up, that shape measured 1 descending step over 195 rows and 97
     * of 195 ASOF invariant violations. The query itself must keep compiling and returning all ten
     * rows: nothing about UNION ALL's result changes, only what the factory claims about its order.
     */
    @Test
    public void testUnionAllAloneStillReturnsAllRows() throws Exception {
        assertQuery("select ts, x from t union all select ts, x from t")
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS)
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .withPlanContaining("Union All")
                .returns("""
                        ts\tx
                        2024-01-01T00:00:00.000000Z\t1
                        2024-01-01T00:00:01.000000Z\t2
                        2024-01-01T00:00:02.000000Z\t3
                        2024-01-01T00:00:03.000000Z\t4
                        2024-01-01T00:00:04.000000Z\t5
                        2024-01-01T00:00:00.000000Z\t1
                        2024-01-01T00:00:01.000000Z\t2
                        2024-01-01T00:00:02.000000Z\t3
                        2024-01-01T00:00:03.000000Z\t4
                        2024-01-01T00:00:04.000000Z\t5
                        """);
    }

    /**
     * UnionAllRecordCursorFactory concatenates: it drains branch A, then restarts at branch B, so
     * the designated timestamp restarts at the boundary. SqlCodeGenerator already strips the
     * sub-query's designated timestamp for a union (the positive control above reports no
     * timestamp), but {@code timestamp(ts)} re-attaches one by column NAME and bypasses that
     * defence, leaving getScanDirection() as the only thing standing. Before the fix this exact
     * query compiled and returned 10 rows of which 4 matched a slave row AHEAD of the master row in
     * time: every row of the second branch matched b.ts=00:00:04, because the ASOF join's slave
     * cursor had already been advanced to the end by the first branch and a forward-only scan
     * cannot go back. Revert the fix and this test must fail.
     */
    @Test
    public void testUnionAllRefusedAsAsofJoinMaster() throws Exception {
        assertQuery("""
                select a.ts ats, b.ts bts
                from ((select ts, x from t union all select ts, x from t) timestamp(ts)) a
                asof join t b
                """)
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS)
                .noLeakCheck()
                .failsWith("ASC order over TIMESTAMP column is required but not provided");
    }

    /**
     * As {@link #testUnionAllRefusedAsAsofJoinMaster}, but with a SYMBOL column surviving on both
     * branches. That shape puts UnionSymbolCastRecordCursorFactory directly above Union All, and it
     * is that node whose getScanDirection() the ASOF join consumes - so this test exists to prove
     * the correction propagates through the cast wrapper, which delegates to its base. Reading the
     * plan alone would be misleading here: the plan says UnionSymbolCast, not Union All, so a
     * reviewer checking "did the Union All fix take?" against plan text would conclude it had not.
     * Same measured shape: 10 rows, 4 of them matching a slave row ahead of the master.
     */
    @Test
    public void testUnionAllWithSymbolCastRefusedAsAsofJoinMaster() throws Exception {
        assertQuery("""
                select a.sym, a.ts ats, b.ts bts
                from ((select ts, sym from t union all select ts, sym from t) timestamp(ts)) a
                asof join t b
                """)
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS)
                .noLeakCheck()
                .failsWith("ASC order over TIMESTAMP column is required but not provided");
    }

    /**
     * Positive control for {@link #testUnionDistinctRefusedAsAsofJoinMaster}. UNION (distinct)
     * de-duplicates but still concatenates: branch A first, then whatever of branch B is new. The
     * query must keep compiling and returning every distinct row. Its size is not known ahead of
     * the scan (the set cursor cannot know how many of B's rows are new), which is why there is no
     * expectSize() here - worth checking per factory rather than copying from the sibling above.
     */
    @Test
    public void testUnionDistinctAloneStillReturnsAllRows() throws Exception {
        assertQuery("select ts, x from t union select ts, x from t where x > 2")
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS)
                .noLeakCheck()
                .noRandomAccess()
                .withPlanContaining("Union")
                .returns("""
                        ts\tx
                        2024-01-01T00:00:00.000000Z\t1
                        2024-01-01T00:00:01.000000Z\t2
                        2024-01-01T00:00:02.000000Z\t3
                        2024-01-01T00:00:03.000000Z\t4
                        2024-01-01T00:00:04.000000Z\t5
                        """);
    }

    /**
     * UnionRecordCursorFactory is UNION ALL plus a de-duplicating set: the branch boundary is still
     * there, so the designated timestamp still restarts, and de-duplication can only remove rows,
     * never reorder them into ascending order. Revert the fix and this test must fail.
     */
    @Test
    public void testUnionDistinctRefusedAsAsofJoinMaster() throws Exception {
        assertQuery("""
                select a.ts ats, b.ts bts
                from ((select ts, x from t union select ts, x from t where x > 2) timestamp(ts)) a
                asof join t b
                """)
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS)
                .noLeakCheck()
                .failsWith("ASC order over TIMESTAMP column is required but not provided");
    }

    /**
     * The mirror image of the two UNION refusals, and the reason no cursor needed fixing:
     * MergeUnionAllRecordCursorFactory merges the branches on the timestamp instead of
     * concatenating them, and it already answers FORWARD/BACKWARD honestly from its merge order.
     * "... union all ... order by ts" routes there, so a user who actually wants an ordered union
     * has a path that still compiles and still feeds an ASOF join - which is what makes the
     * refusals above a redirection rather than a dead end. Note the emitted order: each timestamp
     * appears twice, interleaved from the two branches, ascending throughout.
     */
    @Test
    public void testMergeUnionAllAcceptedAsAsofJoinMaster() throws Exception {
        assertQuery("""
                select a.ts ats, b.ts bts
                from ((select ts, x from t union all select ts, x from t order by ts) timestamp(ts)) a
                asof join t b
                """)
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS)
                .noLeakCheck()
                .timestamp("ats")
                .noRandomAccess()
                .expectSize()
                .withPlanContaining("Union All Merge", "AsOf Join")
                .returns("""
                        ats\tbts
                        2024-01-01T00:00:00.000000Z\t2024-01-01T00:00:00.000000Z
                        2024-01-01T00:00:00.000000Z\t2024-01-01T00:00:00.000000Z
                        2024-01-01T00:00:01.000000Z\t2024-01-01T00:00:01.000000Z
                        2024-01-01T00:00:01.000000Z\t2024-01-01T00:00:01.000000Z
                        2024-01-01T00:00:02.000000Z\t2024-01-01T00:00:02.000000Z
                        2024-01-01T00:00:02.000000Z\t2024-01-01T00:00:02.000000Z
                        2024-01-01T00:00:03.000000Z\t2024-01-01T00:00:03.000000Z
                        2024-01-01T00:00:03.000000Z\t2024-01-01T00:00:03.000000Z
                        2024-01-01T00:00:04.000000Z\t2024-01-01T00:00:04.000000Z
                        2024-01-01T00:00:04.000000Z\t2024-01-01T00:00:04.000000Z
                        """);
    }

    /**
     * The other mirror image, and the boundary of this commit. EXCEPT / EXCEPT ALL / INTERSECT /
     * INTERSECT ALL live in the same package and look like the same shape, but their cursors emit
     * a subset of branch A in A's order - branch B is only ever consulted through a hash set, never
     * emitted - so they delegate to factoryA.getScanDirection() and that delegation is honest. All
     * four were re-verified by execution at 0 descending steps and left untouched, and this test
     * keeps that distinction pinned: if a later sweep "tidied" them to OTHER on the grounds that
     * they are set operations too, this acceptance test would start failing.
     */
    @Test
    public void testExceptAcceptedAsAsofJoinMaster() throws Exception {
        assertQuery("""
                select a.ts ats, b.ts bts
                from (select ts, x from t except select ts, x from t where x > 3) a
                asof join t b
                """)
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS)
                .noLeakCheck()
                .timestamp("ats")
                .noRandomAccess()
                .withPlanContaining("Except", "AsOf Join")
                .returns("""
                        ats\tbts
                        2024-01-01T00:00:00.000000Z\t2024-01-01T00:00:00.000000Z
                        2024-01-01T00:00:01.000000Z\t2024-01-01T00:00:01.000000Z
                        2024-01-01T00:00:02.000000Z\t2024-01-01T00:00:02.000000Z
                        """);
    }

    /**
     * Positive control for {@link #testParallelKeyedHorizonJoinRefusedAsAsofJoinMaster}. The
     * parallel keyed horizon join is what serves this shape by default, so this is the fixture the
     * refusal below is really about, and it must keep compiling and returning every row.
     * <p>
     * The ORDER BY is not decoration. The factory's own emission order here was measured as ts 00,
     * 02, 01, 04, 03 - the aggregation map's order, not ascending timestamp order, even though the
     * group key IS the designated timestamp and every input row was scanned forward. It is not
     * asserted literally because it is a property of the map's capacity and of how many per-worker
     * shards a ShardedMapCursor concatenates, neither of which a test should pin.
     */
    @Test
    public void testParallelKeyedHorizonJoinAloneStillReturnsAllRows() throws Exception {
        sqlExecutionContext.setParallelHorizonJoinEnabled(true);
        assertQuery("""
                select t.ts, avg(p.price) ap
                from t horizon join p on (t.sym = p.sym) range from 0s to 0s step 1s as h
                order by ts
                """)
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS, HORIZON_SLAVE_DDL, HORIZON_SLAVE_ROWS)
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .withPlanContaining("Async Horizon Join", "keys: [ts]")
                .returns("""
                        ts\tap
                        2024-01-01T00:00:00.000000Z\t10.0
                        2024-01-01T00:00:01.000000Z\t20.0
                        2024-01-01T00:00:02.000000Z\t30.0
                        2024-01-01T00:00:03.000000Z\t40.0
                        2024-01-01T00:00:04.000000Z\t50.0
                        """);
    }

    /**
     * AsyncHorizonJoinRecordCursorFactory aggregates into a map and emits that map's entries, so it
     * emits in map order within a shard and shard order across them - never designated-timestamp
     * order. As elsewhere in this class, {@code timestamp(ts)} re-attaches by column NAME a
     * designated timestamp the cursor cannot honour, and getScanDirection() is the only thing left
     * standing. Measured 172 of 299 adjacent steps descending and 296 of 300 ASOF invariant
     * violations; on the five-row fixture here, 2 of 5 rows matched a slave row ahead of the master
     * (master ts 01 matched b.ts=02, master ts 03 matched b.ts=04) across 2 descending steps.
     * <p>
     * A fixture that comes out ascending is not evidence against this: with keys:[ts,sym] the map
     * happens to be an OrderedMap whose insertion order tracks the forward scan, and keys:[ts]
     * stays ascending for as long as the aggregates are still projected. Revert the fix and this
     * test must fail.
     */
    @Test
    public void testParallelKeyedHorizonJoinRefusedAsAsofJoinMaster() throws Exception {
        sqlExecutionContext.setParallelHorizonJoinEnabled(true);
        assertQuery("""
                select a.ts ats, b.ts bts
                from ((select t.ts, avg(p.price) ap
                       from t horizon join p on (t.sym = p.sym) range from 0s to 0s step 1s as h) timestamp(ts)) a
                asof join t b
                """)
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS, HORIZON_SLAVE_DDL, HORIZON_SLAVE_ROWS)
                .noLeakCheck()
                .failsWith("ASC order over TIMESTAMP column is required but not provided");
    }

    /**
     * Positive control for {@link #testSerialKeyedHorizonJoinRefusedAsAsofJoinMaster}. The serial
     * HorizonJoinRecordCursorFactory is only reachable with parallel horizon joins switched off, so
     * the switch is part of the fixture - without it this test would silently be a second copy of
     * the parallel one. The plan assertion is paired with withPlanNotContaining("Async") because
     * "Horizon Join" is a substring of "Async Horizon Join": a fragment check alone cannot tell the
     * two factories apart.
     */
    @Test
    public void testSerialKeyedHorizonJoinAloneStillReturnsAllRows() throws Exception {
        sqlExecutionContext.setParallelHorizonJoinEnabled(false);
        assertQuery("""
                select t.ts, avg(p.price) ap
                from t horizon join p on (t.sym = p.sym) range from 0s to 0s step 1s as h
                order by ts
                """)
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS, HORIZON_SLAVE_DDL, HORIZON_SLAVE_ROWS)
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .withPlanContaining("Horizon Join offsets: 1", "keys: [ts]")
                .withPlanNotContaining("Async")
                .returns("""
                        ts\tap
                        2024-01-01T00:00:00.000000Z\t10.0
                        2024-01-01T00:00:01.000000Z\t20.0
                        2024-01-01T00:00:02.000000Z\t30.0
                        2024-01-01T00:00:03.000000Z\t40.0
                        2024-01-01T00:00:04.000000Z\t50.0
                        """);
    }

    /**
     * HorizonJoinRecordCursorFactory, the serial keyed horizon join: same map-order emission as its
     * parallel sibling, minus the sharding. Same measured evidence - 172 of 299 descending steps,
     * 296 of 300 ASOF violations - and the same 2-of-5 violation on this fixture. Revert the fix
     * and this test must fail.
     */
    @Test
    public void testSerialKeyedHorizonJoinRefusedAsAsofJoinMaster() throws Exception {
        sqlExecutionContext.setParallelHorizonJoinEnabled(false);
        assertQuery("""
                select a.ts ats, b.ts bts
                from ((select t.ts, avg(p.price) ap
                       from t horizon join p on (t.sym = p.sym) range from 0s to 0s step 1s as h) timestamp(ts)) a
                asof join t b
                """)
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS, HORIZON_SLAVE_DDL, HORIZON_SLAVE_ROWS)
                .noLeakCheck()
                .failsWith("ASC order over TIMESTAMP column is required but not provided");
    }

    /**
     * Positive control for {@link #testParallelKeyedMultiHorizonJoinRefusedAsAsofJoinMaster}. A
     * second HORIZON JOIN clause switches to the multi-table factory ("tables: 2" in the plan),
     * which keeps its own state per slave table but still funnels everything into one aggregation
     * map - so joining more slaves changes which aggregates land in an entry, not the order the
     * entries come out in. The nulls are real: table q has no rows for sym b or d.
     */
    @Test
    public void testParallelKeyedMultiHorizonJoinAloneStillReturnsAllRows() throws Exception {
        sqlExecutionContext.setParallelHorizonJoinEnabled(true);
        assertQuery("""
                select t.ts, avg(p.price) ap, avg(q.ask) aq
                from t horizon join p on (t.sym = p.sym) horizon join q on (t.sym = q.sym) list (0) as h
                order by ts
                """)
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS, HORIZON_SLAVE_DDL, HORIZON_SLAVE_ROWS, HORIZON_SLAVE2_DDL, HORIZON_SLAVE2_ROWS)
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .withPlanContaining("Async Multi Horizon Join", "tables: 2", "keys: [ts]")
                .returns("""
                        ts\tap\taq
                        2024-01-01T00:00:00.000000Z\t10.0\t11.0
                        2024-01-01T00:00:01.000000Z\t20.0\tnull
                        2024-01-01T00:00:02.000000Z\t30.0\t31.0
                        2024-01-01T00:00:03.000000Z\t40.0\tnull
                        2024-01-01T00:00:04.000000Z\t50.0\t51.0
                        """);
    }

    /**
     * AsyncMultiHorizonJoinRecordCursorFactory: as
     * {@link #testParallelKeyedHorizonJoinRefusedAsAsofJoinMaster}, over two slave tables. Same
     * 2-of-5 violation on this fixture (master ts 01 matched b.ts=02, master ts 03 matched
     * b.ts=04). Revert the fix and this test must fail.
     */
    @Test
    public void testParallelKeyedMultiHorizonJoinRefusedAsAsofJoinMaster() throws Exception {
        sqlExecutionContext.setParallelHorizonJoinEnabled(true);
        assertQuery("""
                select a.ts ats, b.ts bts
                from ((select t.ts, avg(p.price) ap, avg(q.ask) aq
                       from t horizon join p on (t.sym = p.sym) horizon join q on (t.sym = q.sym) list (0) as h) timestamp(ts)) a
                asof join t b
                """)
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS, HORIZON_SLAVE_DDL, HORIZON_SLAVE_ROWS, HORIZON_SLAVE2_DDL, HORIZON_SLAVE2_ROWS)
                .noLeakCheck()
                .failsWith("ASC order over TIMESTAMP column is required but not provided");
    }

    /**
     * Positive control for {@link #testSerialKeyedMultiHorizonJoinRefusedAsAsofJoinMaster}; as the
     * parallel multi control, with parallel horizon joins switched off so the serial
     * MultiHorizonJoinRecordCursorFactory is the one under test.
     */
    @Test
    public void testSerialKeyedMultiHorizonJoinAloneStillReturnsAllRows() throws Exception {
        sqlExecutionContext.setParallelHorizonJoinEnabled(false);
        assertQuery("""
                select t.ts, avg(p.price) ap, avg(q.ask) aq
                from t horizon join p on (t.sym = p.sym) horizon join q on (t.sym = q.sym) list (0) as h
                order by ts
                """)
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS, HORIZON_SLAVE_DDL, HORIZON_SLAVE_ROWS, HORIZON_SLAVE2_DDL, HORIZON_SLAVE2_ROWS)
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .withPlanContaining("Multi Horizon Join offsets: 1", "tables: 2", "keys: [ts]")
                .withPlanNotContaining("Async")
                .returns("""
                        ts\tap\taq
                        2024-01-01T00:00:00.000000Z\t10.0\t11.0
                        2024-01-01T00:00:01.000000Z\t20.0\tnull
                        2024-01-01T00:00:02.000000Z\t30.0\t31.0
                        2024-01-01T00:00:03.000000Z\t40.0\tnull
                        2024-01-01T00:00:04.000000Z\t50.0\t51.0
                        """);
    }

    /**
     * MultiHorizonJoinRecordCursorFactory, the serial multi-table keyed horizon join. Revert the
     * fix and this test must fail.
     */
    @Test
    public void testSerialKeyedMultiHorizonJoinRefusedAsAsofJoinMaster() throws Exception {
        sqlExecutionContext.setParallelHorizonJoinEnabled(false);
        assertQuery("""
                select a.ts ats, b.ts bts
                from ((select t.ts, avg(p.price) ap, avg(q.ask) aq
                       from t horizon join p on (t.sym = p.sym) horizon join q on (t.sym = q.sym) list (0) as h) timestamp(ts)) a
                asof join t b
                """)
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS, HORIZON_SLAVE_DDL, HORIZON_SLAVE_ROWS, HORIZON_SLAVE2_DDL, HORIZON_SLAVE2_ROWS)
                .noLeakCheck()
                .failsWith("ASC order over TIMESTAMP column is required but not provided");
    }

    /**
     * The boundary of this commit, and the mirror image of the four horizon-join refusals. Dropping
     * the group key leaves a not-keyed horizon join, which emits exactly one aggregated row
     * (size() is 1), so no pair of emitted rows exists that could be out of ascending order and its
     * FORWARD claim is trivially true. The four *NotKeyed* siblings were audited and re-verified by
     * execution on that basis and deliberately left alone.
     * <p>
     * The boundary is thinner than it looks: projecting {@code h.offset} alongside the aggregates
     * makes the same query keyed and switches it to the factory this commit corrects. That is why
     * the refusals above are not evidence about HORIZON JOIN in general - only about the keyed
     * factories - and why this acceptance test has to exist alongside them.
     */
    @Test
    public void testNotKeyedHorizonJoinAcceptedAsAsofJoinMaster() throws Exception {
        sqlExecutionContext.setParallelHorizonJoinEnabled(true);
        assertQuery("""
                select a.ts ats, b.ts bts
                from ((select max(t.ts) ts, avg(p.price) ap
                       from t horizon join p range from 0s to 0s step 1s as h) timestamp(ts)) a
                asof join t b
                """)
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS, HORIZON_SLAVE_DDL, HORIZON_SLAVE_ROWS)
                .noLeakCheck()
                .timestamp("ats")
                .noRandomAccess()
                .expectSize()
                .withPlanContaining("Async Horizon Join", "AsOf Join")
                .withPlanNotContaining("keys:")
                .returns("""
                        ats\tbts
                        2024-01-01T00:00:04.000000Z\t2024-01-01T00:00:04.000000Z
                        """);
    }

    /**
     * Positive control for {@link #testLatestByLightRefusedAsAsofJoinMaster}, and the evidence for
     * it. The emitted order is ts 04, 02, 03 - the map's key-insertion order (a, b, c: the order
     * the keys were first SEEN by the forward base scan) carrying each key's LATEST row, which is a
     * different order entirely. One descending step in three rows. The query must keep compiling
     * and returning every row: latest-by's result is not changing, only what the factory claims
     * about its order.
     */
    @Test
    public void testLatestByLightAloneStillReturnsAllRows() throws Exception {
        assertQuery("select * from (select ts, sym, x from v where x > 0) latest on ts partition by sym")
                .ddl(LATEST_BY_DDL, LATEST_BY_ROWS)
                .noLeakCheck()
                .expectSize()
                .withPlanContaining("LatestBy light")
                .returns("""
                        ts\tsym\tx
                        2024-01-01T00:00:04.000000Z\ta\t5
                        2024-01-01T00:00:02.000000Z\tb\t3
                        2024-01-01T00:00:03.000000Z\tc\t4
                        """);
    }

    /**
     * LatestByLightRecordCursorFactory drains the latest-by map, emitting one row per partition key
     * in key-insertion order - the order the keys were first seen, not the order of the latest
     * timestamp retained for each. Measured 101 of 199 adjacent steps descending and 192 of 200
     * ASOF invariant violations; on the three-row fixture above, 2 of 3 rows matched a slave row
     * ahead of the master (ts 02 and ts 03 both matched b.ts=04).
     * <p>
     * The factory already strips the designated timestamp from its metadata, and an upstream
     * comment used to conclude from that that its scan direction was vacuous. It is not: the
     * timestamp(ts) wrapper below re-attaches a designated timestamp by column NAME and walks
     * straight past the stripping, at which point getScanDirection() is the only thing left. That
     * comment has been corrected along with the declaration - left standing it was a documented
     * argument for reverting this fix. Revert the fix and this test must fail.
     */
    @Test
    public void testLatestByLightRefusedAsAsofJoinMaster() throws Exception {
        assertQuery("""
                select a.ts ats, b.ts bts
                from ((select * from (select ts, sym, x from v where x > 0) latest on ts partition by sym) timestamp(ts)) a
                asof join v b
                """)
                .ddl(LATEST_BY_DDL, LATEST_BY_ROWS)
                .noLeakCheck()
                .failsWith("ASC order over TIMESTAMP column is required but not provided");
    }

    /**
     * Positive control for the three non-light latest-by tests below. Unlike the light variant,
     * LatestByRecordCursorFactory sorts the retained row indexes and REPLAYS the base cursor, so it
     * emits a subset of the base's rows in the base's own order - here a backward-scanning hash
     * join, so ts 04, 03, 02, 01, 00. That is not a defect in the cursor and this output does not
     * change: emitting in base order is the factory's contract. What was wrong was declaring that
     * order FORWARD.
     */
    @Test
    public void testLatestByOverBackwardBaseAloneStillReturnsAllRows() throws Exception {
        assertQuery("""
                select * from (select t.ts, t.sym, t.x from (t order by ts desc) t join r on (t.x = r.y))
                latest on ts partition by sym
                """)
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS, JOIN_SLAVE_DDL, JOIN_SLAVE_ROWS)
                .noLeakCheck()
                .timestampDesc("ts")
                .noRandomAccess()
                .expectSize()
                .withPlanContaining("LatestBy", "Row backward scan")
                .returns("""
                        ts\tsym\tx
                        2024-01-01T00:00:04.000000Z\te\t5
                        2024-01-01T00:00:03.000000Z\td\t4
                        2024-01-01T00:00:02.000000Z\tc\t3
                        2024-01-01T00:00:01.000000Z\tb\t2
                        2024-01-01T00:00:00.000000Z\ta\t1
                        """);
    }

    /**
     * The most exploitable of the six factories, and the only one whose false claim needed no
     * {@code timestamp(col)} re-attachment to reach a consumer. LatestByRecordCursorFactory keeps
     * base.getMetadata() verbatim, designated timestamp included, so the ORDER BY elision check
     * sees a designated timestamp AND a FORWARD claim and elides the sort as already-satisfied.
     * Measured on the pre-fix branch: no sort node in the plan for this exact query, and the rows
     * came back 04, 03, 02, 01, 00 - a plain "ORDER BY ts" returning strictly descending rows, with
     * nothing unusual in the query to warn the user. With the corrected answer the factory reports
     * its backward base's direction, the sort is planned, and the rows come out ascending.
     * <p>
     * This is the one place in this change where a user-visible RESULT changes rather than a query
     * being refused, and it changes from wrong to right. Revert the fix and the sort disappears
     * from the plan and this test fails.
     */
    @Test
    public void testLatestByOverBackwardBaseOrderByTimestampIsSorted() throws Exception {
        assertQuery("""
                select * from (select t.ts, t.sym, t.x from (t order by ts desc) t join r on (t.x = r.y))
                latest on ts partition by sym
                order by ts
                """)
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS, JOIN_SLAVE_DDL, JOIN_SLAVE_ROWS)
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .withPlanContaining("sort", "LatestBy")
                .returns("""
                        ts\tsym\tx
                        2024-01-01T00:00:00.000000Z\ta\t1
                        2024-01-01T00:00:01.000000Z\tb\t2
                        2024-01-01T00:00:02.000000Z\tc\t3
                        2024-01-01T00:00:03.000000Z\td\t4
                        2024-01-01T00:00:04.000000Z\te\t5
                        """);
    }

    /**
     * The same false claim reaching the other consumer. Before the fix this ASOF join compiled and
     * returned 5 rows of which 4 matched a slave row AHEAD of the master in time - every row after
     * the first matched b.ts=00:00:04, because the master ran backwards and the join's forward-only
     * slave cursor could not go back. Scaled up, the same shape measured 199 of 199 adjacent steps
     * descending and 199 of 200 violations. Revert the fix and this test must fail.
     * <p>
     * The refusal message differs from every other one in this class, and that difference is a
     * consequence of delegating rather than declaring OTHER: this factory now reports BACKWARD,
     * which is a specific enough answer for the ASOF join to say the master is ordered the wrong
     * way round ("left side of time series join doesn't have ASC timestamp order") rather than the
     * generic "ASC order over TIMESTAMP column is required but not provided" raised for a cursor
     * whose order is simply unknown. Assert the message the user actually sees, not the one the
     * sibling tests happen to use.
     */
    @Test
    public void testLatestByOverBackwardBaseRefusedAsAsofJoinMaster() throws Exception {
        assertQuery("""
                select a.ts ats, b.ts bts
                from ((select * from (select t.ts, t.sym, t.x from (t order by ts desc) t join r on (t.x = r.y))
                       latest on ts partition by sym) timestamp(ts)) a
                asof join t b
                """)
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS, JOIN_SLAVE_DDL, JOIN_SLAVE_ROWS)
                .noLeakCheck()
                .failsWith("left side of time series join doesn't have ASC timestamp order");
    }

    /**
     * The mirror image, and the reason this factory delegates rather than answering OTHER outright.
     * The only thing changed in the query below is {@code order by ts desc} becoming
     * {@code order by ts}, which makes the hash join's master a forward scan. LatestBy replays it
     * in that order, reports FORWARD because its base does, and the ASOF join still compiles and
     * still returns every row - no re-attachment needed, because this factory keeps the base's
     * designated timestamp. Had the fix declared OTHER unconditionally (as the other five
     * corrections do, their cursors having no base order to inherit), this test would fail and a
     * working query would have been refused for no reason.
     */
    @Test
    public void testLatestByOverForwardBaseAcceptedAsAsofJoinMaster() throws Exception {
        assertQuery("""
                select a.ts ats, b.ts bts
                from (select * from (select t.ts, t.sym, t.x from (t order by ts) t join r on (t.x = r.y))
                      latest on ts partition by sym) a
                asof join t b
                """)
                .ddl(FIVE_ROW_DDL, FIVE_ROW_ROWS, JOIN_SLAVE_DDL, JOIN_SLAVE_ROWS)
                .noLeakCheck()
                .timestamp("ats")
                .noRandomAccess()
                .expectSize()
                .withPlanContaining("LatestBy", "AsOf Join")
                .returns("""
                        ats\tbts
                        2024-01-01T00:00:00.000000Z\t2024-01-01T00:00:00.000000Z
                        2024-01-01T00:00:01.000000Z\t2024-01-01T00:00:01.000000Z
                        2024-01-01T00:00:02.000000Z\t2024-01-01T00:00:02.000000Z
                        2024-01-01T00:00:03.000000Z\t2024-01-01T00:00:03.000000Z
                        2024-01-01T00:00:04.000000Z\t2024-01-01T00:00:04.000000Z
                        """);
    }

    /**
     * Positive control for {@link #testFilterOnSubQueryOverBackwardFrameDeclaresOther}, and the
     * fixture proof for it. {@code sym in (<sub-query>)} over an indexed SYMBOL column selects
     * FilterOnSubQueryRecordCursorFactory, and "order by ts desc" makes the optimiser set
     * model.isForceBackwardScan(), which hands that factory an ORDER_DESC partition frame cursor -
     * "Frame backward scan on: w" in the plan below. The query must keep compiling and returning
     * every matching row, in descending order: nothing about the result changes here, only what the
     * factory claims about the order it emits in.
     * <p>
     * Note the Encode sort above the factory. It is there in BOTH the corrected and the pre-fix
     * build, because sort elision needs a BACKWARD claim and the factory answered FORWARD before the
     * fix and answers OTHER after it - neither matches "ts desc". That is precisely why the
     * companion test has to read the declaration off the factory rather than assert a refusal: the
     * only consumer this factory reaches with a DESC frame today is
     * UnionAllRecordCursorFactory, which declares SCAN_DIRECTION_OTHER unconditionally and so
     * swallows whatever its branches claim.
     */
    @Test
    public void testFilterOnSubQueryOverBackwardFrameAloneStillReturnsAllRows() throws Exception {
        assertQuery("select ts, sym, x from w where sym in (select sym from s) order by ts desc")
                .ddl(SUBQUERY_FILTERED_DDL, SUBQUERY_FILTERED_ROWS, SUBQUERY_KEYS_DDL, SUBQUERY_KEYS_ROWS)
                .noLeakCheck()
                .timestampDesc("ts")
                .withPlanContaining("Encode sort light", "FilterOnSubQuery", "Frame backward scan on: w")
                .returns("""
                        ts\tsym\tx
                        2024-01-03T01:00:00.000000Z\ta\t6
                        2024-01-03T00:00:00.000000Z\tb\t5
                        2024-01-02T00:00:00.000000Z\ta\t3
                        2024-01-01T01:00:00.000000Z\tb\t2
                        2024-01-01T00:00:00.000000Z\ta\t1
                        """);
    }

    /**
     * FilterOnSubQueryRecordCursorFactory merges one index row cursor per matching symbol key
     * through a HeapRowCursorFactory, so rows are ascending by row id WITHIN a partition frame but
     * come out in whatever order the partition frame cursor hands the frames over. The planner does
     * hand it an ORDER_DESC frame cursor (see the positive control above), and the emission is then
     * a sawtooth - partitions descending, rows ascending inside each - which is neither FORWARD nor
     * BACKWARD. Driven over 4 daily partitions with 46 matching rows it measured 42 ascending steps,
     * 3 descending and 0 equal; on the six-row fixture here it is x = 5, 6, 3, 1, 2, asserted below.
     * <p>
     * Its two structural siblings, FilterOnValuesRecordCursorFactory and
     * FilterOnExcludedValuesRecordCursorFactory, already consult
     * {@code partitionFrameCursorFactory.getOrder()} and answer SCAN_DIRECTION_OTHER when it is not
     * ORDER_ASC; they additionally test their {@code heapCursorUsed} flag because they can pick a
     * sequential per-symbol cursor instead. This class always builds a HeapRowCursorFactory, so the
     * frame order is the whole condition.
     * <p>
     * This is the one factory in this class with no query-level refusal to assert. Every other
     * corrected factory keeps a designated timestamp that {@code timestamp(col)} can re-attach and
     * so reaches an order-requiring consumer; here the DESC-framed factory is only ever reached
     * through a Sort (which declares its own direction) or as a direct branch of
     * UnionAllRecordCursorFactory (which declares OTHER unconditionally), so the false FORWARD is a
     * latent hazard rather than a live wrong answer - 17 candidate consumer shapes were executed
     * against both builds and none differed. Read the declaration off the factory instead. Revert
     * the guard in getScanDirection() and the first assertion below fails: the factory answers
     * SCAN_DIRECTION_FORWARD (1) while emitting 5, 6, 3, 1, 2.
     */
    @Test
    public void testFilterOnSubQueryOverBackwardFrameDeclaresOther() throws Exception {
        assertMemoryLeak(() -> {
            execute(SUBQUERY_FILTERED_DDL);
            execute(SUBQUERY_FILTERED_ROWS);
            execute(SUBQUERY_KEYS_DDL);
            execute(SUBQUERY_KEYS_ROWS);

            try (RecordCursorFactory top = select("select ts, sym, x from w where sym in (select sym from s) order by ts desc")) {
                final FilterOnSubQueryRecordCursorFactory factory = findFilterOnSubQuery(top);
                Assert.assertEquals(
                        "DESC frame order must not be claimed as an ascending scan",
                        RecordCursorFactory.SCAN_DIRECTION_OTHER,
                        factory.getScanDirection()
                );
                Assert.assertEquals("5,6,3,1,2", drainXColumn(factory));
            }

            // The mirror image, and the reason the guard tests the frame order instead of answering
            // OTHER outright: with no "order by ts desc" the frame cursor is ORDER_ASC, the
            // partitions come out in ascending order too, and FORWARD is honest. Had the fix
            // declared OTHER unconditionally, every ordinary "sym in (<sub-query>)" query would have
            // stopped being usable as an ASOF/LT/SPLICE master or a SAMPLE BY base for no reason.
            try (RecordCursorFactory top = select("select ts, sym, x from w where sym in (select sym from s)")) {
                final FilterOnSubQueryRecordCursorFactory factory = findFilterOnSubQuery(top);
                Assert.assertEquals(
                        "ASC frame order still emits an ascending designated timestamp",
                        RecordCursorFactory.SCAN_DIRECTION_FORWARD,
                        factory.getScanDirection()
                );
                Assert.assertEquals("1,2,3,5,6", drainXColumn(factory));
            }
        });
    }

    /**
     * Drains {@code factory} and returns its x column, in emission order, as a comma-separated
     * string. The x values are unique per row in the fixture, so the string is a faithful record of
     * the order the cursor produced - which is the evidence the scan-direction claim is about.
     */
    private static String drainXColumn(RecordCursorFactory factory) throws Exception {
        final StringBuilder sb = new StringBuilder();
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                if (sb.length() > 0) {
                    sb.append(',');
                }
                sb.append(record.getLong(2));
            }
        }
        return sb.toString();
    }

    /**
     * Walks the base-factory chain from the top of a compiled query down to the
     * FilterOnSubQueryRecordCursorFactory under it. Failing rather than returning null keeps the
     * assertions above from going vacuous if the planner ever stops selecting this factory for the
     * fixture query.
     */
    private static FilterOnSubQueryRecordCursorFactory findFilterOnSubQuery(RecordCursorFactory factory) {
        RecordCursorFactory f = factory;
        while (f != null) {
            if (f instanceof FilterOnSubQueryRecordCursorFactory fosq) {
                return fosq;
            }
            f = f.getBaseFactory();
        }
        Assert.fail("FilterOnSubQueryRecordCursorFactory not found in the compiled query tree");
        return null;
    }
}

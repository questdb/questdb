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
 * group-by family is the second. When the remaining ones are corrected, add one test method per
 * factory here rather than starting a new class or a new assertion pattern - the shape below
 * (sub-query re-attaches a designated timestamp with {@code timestamp(col)}, then feeds an
 * ASOF/LT/SPLICE join or other order-requiring consumer) is deliberately reusable.
 * <p>
 * Each corrected factory gets a pair: a refusal test asserting the specific error message, and a
 * positive control proving the fixture still selects that factory and still returns every row. A
 * factory correctly declaring FORWARD gets the mirror image - an acceptance test - so that the
 * refusals stay evidence about the specific factory rather than about group-by in general.
 */
public class ScanDirectionContractTest extends AbstractCairoTest {

    private static final String GROUP_BY_DDL =
            "create table t (ts timestamp, sym symbol, x long) timestamp(ts) partition by day";
    private static final String GROUP_BY_ROWS =
            "insert into t values" +
                    " ('2024-01-01T00:00:00.000000Z','a',1)," +
                    " ('2024-01-01T00:00:01.000000Z','b',2)," +
                    " ('2024-01-01T00:00:02.000000Z','c',3)," +
                    " ('2024-01-01T00:00:03.000000Z','d',4)," +
                    " ('2024-01-01T00:00:04.000000Z','e',5)";

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
                .ddl(GROUP_BY_DDL, GROUP_BY_ROWS)
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
                .ddl(GROUP_BY_DDL, GROUP_BY_ROWS)
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
                .ddl(GROUP_BY_DDL, GROUP_BY_ROWS)
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
                .ddl(GROUP_BY_DDL, GROUP_BY_ROWS)
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
                .ddl(GROUP_BY_DDL, GROUP_BY_ROWS)
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
                .ddl(GROUP_BY_DDL, GROUP_BY_ROWS)
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
                .ddl(GROUP_BY_DDL, GROUP_BY_ROWS)
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
                .ddl(GROUP_BY_DDL, GROUP_BY_ROWS)
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
                .ddl(GROUP_BY_DDL, GROUP_BY_ROWS)
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
}

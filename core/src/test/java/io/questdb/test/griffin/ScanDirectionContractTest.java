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
 * SortedSymbolIndexRecordCursorFactory is the first of six factories being audited this way.
 * When the remaining five are corrected, add one test method per factory here rather than
 * starting a new class or a new assertion pattern - the shape below (sub-query re-attaches a
 * designated timestamp with {@code timestamp(col)}, then feeds an ASOF/LT/SPLICE join or other
 * order-requiring consumer) is deliberately reusable.
 */
public class ScanDirectionContractTest extends AbstractCairoTest {

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

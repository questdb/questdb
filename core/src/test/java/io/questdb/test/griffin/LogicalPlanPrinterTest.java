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

public class LogicalPlanPrinterTest extends AbstractCairoTest {
    private static final String QUOTES = "CREATE TABLE quotes (sym SYMBOL, qty LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY";
    private static final String TRADES = "CREATE TABLE trades (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY";

    @Test
    public void testAggregateSortLimit() throws Exception {
        assertQuery("SELECT sym, sum(price) total FROM trades WHERE price > 1 GROUP BY sym ORDER BY total DESC LIMIT 3")
                .ddl(TRADES)
                .assertsLogicalPlan("""
                        Limit
                          lo: 3
                          Sort
                            keys: [total desc]
                            Project
                              columns: [sym, total]
                              Aggregate
                                keys: [sym]
                                values: [sum(price) AS total]
                                Filter
                                  predicate: price > 1
                                  Scan
                                    table: trades
                                    columns: [sym, price]
                        """);
    }

    @Test
    public void testInSubquery() throws Exception {
        assertQuery("SELECT sym FROM trades WHERE sym IN (SELECT sym FROM quotes WHERE qty > 10)")
                .ddl(TRADES, QUOTES)
                .assertsLogicalPlan("""
                        Project
                          columns: [sym]
                          Filter
                            predicate: in(sym, (subquery #1))
                            Scan
                              table: trades
                              columns: [sym]
                        Subquery #1:
                        Project
                          columns: [sym]
                          Filter
                            predicate: qty > 10
                            Scan
                              table: quotes
                              columns: [sym, qty]
                        """);
    }

    @Test
    public void testJoin() throws Exception {
        assertQuery("SELECT t.sym, q.qty FROM trades t JOIN quotes q ON t.sym = q.sym AND q.qty > t.price")
                .ddl(TRADES, QUOTES)
                .assertsLogicalPlan("""
                        Project
                          columns: [t.sym, q.qty]
                          Join
                            Master t
                              Scan
                                table: trades
                                columns: [sym, price]
                            INNER q
                              keys: [q.sym = t.sym]
                              on: q.qty > t.price
                              Scan
                                table: quotes
                                columns: [sym, qty]
                        """);
    }

    @Test
    public void testUnionWindow() throws Exception {
        assertQuery("""
                SELECT sym, row_number() OVER (PARTITION BY sym ORDER BY ts DESC) rn FROM trades
                UNION ALL
                SELECT sym, 0 FROM quotes
                """)
                .ddl(TRADES, QUOTES)
                .assertsLogicalPlan("""
                        Union All
                          Project
                            columns: [sym, rn]
                            Window
                              functions: [row_number() over (partition by [sym] order by [ts desc] range between unbounded preceding and current row) AS rn]
                              Scan
                                table: trades
                                columns: [sym, ts]
                          Project
                            columns: [sym, 0 AS 0]
                            Scan
                              table: quotes
                              columns: [sym]
                        """);
    }
}

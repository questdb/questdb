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

package io.questdb.test.griffin.engine.window;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class NestedLagSymbolTest extends AbstractCairoTest {

    @Test
    public void testLagLeadOverSymbolIgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE symbols (" +
                            "  sym SYMBOL," +
                            "  ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO symbols VALUES" +
                            " ('a', '2024-01-01T00:00:00.000000Z')," +
                            " (null, '2024-01-01T00:01:00.000000Z')," +
                            " ('b', '2024-01-01T00:02:00.000000Z')," +
                            " (null, '2024-01-01T00:03:00.000000Z')," +
                            " ('c', '2024-01-01T00:04:00.000000Z')"
            );

            assertQuery(
                    "SELECT sym," +
                            "   LAG(sym, 1) ignore nulls OVER (ORDER BY ts) AS prev_sym," +
                            "   LEAD(sym, 1) ignore nulls OVER (ORDER BY ts) AS next_sym" +
                            " FROM symbols"
            ).expectSize().returns(
                    "sym\tprev_sym\tnext_sym\n" +
                            "a\t\tb\n" +
                            "\ta\tb\n" +
                            "b\ta\tc\n" +
                            "\tb\tc\n" +
                            "c\tb\t\n"
            );
        });
    }

    @Test
    public void testLagLeadSymbolNonLightCachedWindow() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, false);
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE balances (" +
                            "  sym SYMBOL," +
                            "  quantity DOUBLE," +
                            "  ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO balances VALUES" +
                            " ('a', 1.0, '2024-01-01T00:00:00.000000Z')," +
                            " ('b', 2.0, '2024-01-01T00:01:00.000000Z')," +
                            " ('c', 3.0, '2024-01-01T00:02:00.000000Z')"
            );

            assertQuery(
                    "SELECT sym," +
                            "   LAG(sym, 1) OVER (ORDER BY ts) AS prev_sym," +
                            "   LEAD(sym, 1) OVER (ORDER BY ts) AS next_sym" +
                            " FROM balances"
            ).expectSize().returns(
                    "sym\tprev_sym\tnext_sym\n" +
                            "a\t\tb\n" +
                            "b\ta\tc\n" +
                            "c\tb\t\n"
            );
        });
    }

    @Test
    public void testLagLeadSymbolNullDefault() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE symbols (" +
                            "  sym SYMBOL," +
                            "  ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO symbols VALUES" +
                            " ('a', '2024-01-01T00:00:00.000000Z')," +
                            " ('b', '2024-01-01T00:01:00.000000Z')," +
                            " ('c', '2024-01-01T00:02:00.000000Z')"
            );

            assertQuery(
                    "SELECT sym," +
                            "   LAG(sym, 1, null) OVER (ORDER BY ts) AS prev_sym," +
                            "   LEAD(sym, 1, null) OVER (ORDER BY ts) AS next_sym" +
                            " FROM symbols"
            ).expectSize().returns(
                    "sym\tprev_sym\tnext_sym\n" +
                            "a\t\tb\n" +
                            "b\ta\tc\n" +
                            "c\tb\t\n"
            );
        });
    }

    @Test
    public void testLagOffsetOverSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE balances (" +
                            "  sym SYMBOL," +
                            "  quantity DOUBLE," +
                            "  ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO balances VALUES" +
                            " ('a', 1.0, '2024-01-01T00:00:00.000000Z')," +
                            " ('a', 2.0, '2024-01-01T00:01:00.000000Z')," +
                            " ('a', 3.0, '2024-01-01T00:02:00.000000Z')"
            );

            assertQuery(
                    "SELECT sym," +
                            "   LAG(sym, 1) OVER (PARTITION BY sym ORDER BY ts) AS prev_sym," +
                            "   LAG(sym, 2) OVER (PARTITION BY sym ORDER BY ts) AS prev_prev_sym" +
                            " FROM balances"
            ).noRandomAccess().expectSize().returns(
                    "sym\tprev_sym\tprev_prev_sym\n" +
                            "a\t\t\n" +
                            "a\ta\t\n" +
                            "a\ta\ta\n"
            );
        });
    }

    @Test
    public void testLeadOffsetOverSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE balances (" +
                            "  sym SYMBOL," +
                            "  quantity DOUBLE," +
                            "  ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO balances VALUES" +
                            " ('a', 1.0, '2024-01-01T00:00:00.000000Z')," +
                            " ('a', 2.0, '2024-01-01T00:01:00.000000Z')," +
                            " ('a', 3.0, '2024-01-01T00:02:00.000000Z')"
            );

            assertQuery(
                    "SELECT sym," +
                            "   LEAD(sym, 1) OVER (PARTITION BY sym ORDER BY ts) AS next_sym," +
                            "   LEAD(sym, 2) OVER (PARTITION BY sym ORDER BY ts) AS next_next_sym" +
                            " FROM balances"
            ).expectSize().returns(
                    "sym\tnext_sym\tnext_next_sym\n" +
                            "a\ta\ta\n" +
                            "a\ta\t\n" +
                            "a\t\t\n"
            );
        });
    }

    @Test
    public void testNestedLagOverSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE balances (" +
                            "  sym SYMBOL," +
                            "  quantity DOUBLE," +
                            "  ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO balances VALUES" +
                            " ('a', 1.0, '2024-01-01T00:00:00.000000Z')," +
                            " ('a', 2.0, '2024-01-01T00:01:00.000000Z')," +
                            " ('a', 3.0, '2024-01-01T00:02:00.000000Z')"
            );

            assertQuery(
                    "WITH step1 AS (" +
                            "  SELECT ts, sym," +
                            "    LAG(sym) OVER (PARTITION BY sym ORDER BY ts) AS prev_sym" +
                            "  FROM balances" +
                            ")" +
                            " SELECT sym, prev_sym," +
                            "   LAG(prev_sym) OVER (PARTITION BY sym ORDER BY ts) AS prev_prev_sym" +
                            " FROM step1"
            ).noRandomAccess().expectSize().returns(
                    "sym\tprev_sym\tprev_prev_sym\n" +
                            "a\t\t\n" +
                            "a\ta\t\n" +
                            "a\ta\ta\n"
            );
        });
    }

    @Test
    public void testRejectsNonNullSymbolDefault() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE symbols (" +
                            "  sym SYMBOL," +
                            "  ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );

            assertQuery("select lag(sym, 1, 'x') over () from symbols")
                    .noLeakCheck()
                    .fails(19, "non-null default value is not supported for symbol lag");

            assertQuery("select lead(sym, 1, 'x') over () from symbols")
                    .noLeakCheck()
                    .fails(20, "non-null default value is not supported for symbol lead");
        });
    }
}

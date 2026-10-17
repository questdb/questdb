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
 * An operator that fills map key and value types or column filters, then instantiates a function whose scalar
 * sub-query generates a keyed factory one generation depth deeper, still reads its own structures afterwards.
 */
public class GenerationFrameReentryTest extends AbstractCairoTest {
    private static final String KEYED_SUBQUERY = "(SELECT w FROM (SELECT w FROM r UNION SELECT w FROM r) ORDER BY w LIMIT 1)";

    @Test
    public void testSampleByAggregateReadsKeyedSubquery() throws Exception {
        assertMemoryLeak(() -> {
            createSubqueryTables();
            execute("CREATE TABLE t (sym SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        ('a', 1.0, '2024-01-01T00:00:00.000000Z'),
                        ('b', 2.0, '2024-01-01T00:30:00.000000Z'),
                        ('a', 3.0, '2024-01-01T01:00:00.000000Z'),
                        ('b', 4.0, '2024-01-01T01:30:00.000000Z')
                    """);
            assertQuery("SELECT ts, sym, f, s, c FROM (SELECT ts, sym, first(v) f, sum(CASE WHEN v * 10 > " + KEYED_SUBQUERY + " THEN v ELSE 0.0 END) s, count() c"
                    + " FROM t SAMPLE BY 1h ALIGN TO FIRST OBSERVATION) ORDER BY ts, sym")
                    .timestamp("ts")
                    .withPlanContaining("Sample By")
                    .returns("""
                            ts\tsym\tf\ts\tc
                            2024-01-01T00:00:00.000000Z\ta\t1.0\t0.0\t1
                            2024-01-01T00:00:00.000000Z\tb\t2.0\t2.0\t1
                            2024-01-01T01:00:00.000000Z\ta\t3.0\t3.0\t1
                            2024-01-01T01:00:00.000000Z\tb\t4.0\t4.0\t1
                            """);
        });
    }

    @Test
    public void testWindowJoinAggregateReadsKeyedSubquery() throws Exception {
        assertMemoryLeak(() -> {
            createSubqueryTables();
            createWindowJoinTables();
            assertQuery("SELECT t.sym, t.ts, sum(p.price) s, sum(CASE WHEN p.price - t.price > " + KEYED_SUBQUERY + " THEN p.price ELSE 0.0 END) w, count() c"
                    + " FROM trades t WINDOW JOIN prices p ON (t.sym = p.sym)"
                    + " RANGE BETWEEN 1 minute PRECEDING AND 1 minute FOLLOWING EXCLUDE PREVAILING ORDER BY t.ts")
                    .timestamp("ts")
                    .noRandomAccess()
                    .withPlanContaining("Window")
                    .returns("""
                            sym\tts\ts\tw\tc
                            a\t2024-01-01T00:01:00.000000Z\t21.0\t0.0\t2
                            b\t2024-01-01T00:02:00.000000Z\t41.0\t41.0\t2
                            """);
        });
    }

    private static void createSubqueryTables() throws Exception {
        execute("CREATE TABLE r (w DOUBLE)");
        execute("INSERT INTO r VALUES (10.0), (20.0)");
    }

    private static void createWindowJoinTables() throws Exception {
        execute("CREATE TABLE trades (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO trades VALUES
                    ('a', 1.0, '2024-01-01T00:01:00.000000Z'),
                    ('b', 2.0, '2024-01-01T00:02:00.000000Z')
                """);
        execute("CREATE TABLE prices (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO prices VALUES
                    ('a', 10.0, '2024-01-01T00:00:00.000000Z'),
                    ('a', 11.0, '2024-01-01T00:01:00.000000Z'),
                    ('b', 20.0, '2024-01-01T00:02:00.000000Z'),
                    ('b', 21.0, '2024-01-01T00:03:00.000000Z')
                """);
    }
}

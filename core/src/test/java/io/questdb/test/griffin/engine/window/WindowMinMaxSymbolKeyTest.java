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

import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * The Min/Max Filter partitioned by one SYMBOL column over a FLOAT or DOUBLE argument (its phase one through
 * the map's batch probe): the rows are those a GROUP BY's per-symbol minimum or maximum selects.
 */
public class WindowMinMaxSymbolKeyTest extends AbstractCairoTest {

    @Test
    public void testMinAndMaxBySymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT"
                    + " timestamp_sequence('2024-01-01', 1000) ts,"
                    + " rnd_symbol(300, 2, 6, 3) sym,"
                    + " (rnd_int(0, 2000, 0) / 8.0)::FLOAT f,"
                    + " CASE WHEN x % 53 = 0 THEN NULL ELSE rnd_int(0, 900, 0) / 4.0 END d"
                    + " FROM long_sequence(200_000)) TIMESTAMP(ts) PARTITION BY HOUR");
            for (String col : new String[]{"f", "d"}) {
                for (String fn : new String[]{"min", "max"}) {
                    final String sql = "SELECT ts, sym, " + col + " FROM (SELECT ts, sym, " + col + ", " + fn + "(" + col + ") OVER (PARTITION BY sym) w FROM t) WHERE " + col + " = w";
                    final StringSink plan = new StringSink();
                    printSql("EXPLAIN " + sql, plan);
                    TestUtils.assertContains(plan, "Min/Max Filter");
                    final StringSink actual = new StringSink();
                    final StringSink expected = new StringSink();
                    printSql(sql + " ORDER BY ts", actual);
                    printSql("SELECT t.ts, t.sym, t." + col + " FROM t JOIN (SELECT sym, " + fn + "(" + col + ") w FROM t) g ON t.sym = g.sym WHERE t." + col + " = g.w ORDER BY t.ts", expected);
                    TestUtils.assertEquals(expected, actual);
                }
            }
        });
    }
}

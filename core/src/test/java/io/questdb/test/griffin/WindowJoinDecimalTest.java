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
 * Tests window joins over decimal columns. A window join whose ON clause folds to constant false is
 * short-circuited into a cursor that keeps the master columns and pads the slave ones with nulls.
 */
public class WindowJoinDecimalTest extends AbstractCairoTest {
    private static final String CONSTANT_FALSE_JOIN = "select t.ts, first(p.d8) f8, first(p.d16) f16, first(p.d32) f32, " +
            "first(p.d64) f64, first(p.d128) f128, first(p.d256) f256 " +
            "from trades t " +
            "window join prices p on (1 = 0) " +
            "range between 1 minute preceding and 1 minute following";
    private static final String PRICES_DDL = """
            CREATE TABLE prices (ts TIMESTAMP, d8 DECIMAL(2, 1), d16 DECIMAL(4, 1), d32 DECIMAL(9, 2),
              d64 DECIMAL(18, 2), d128 DECIMAL(38, 2), d256 DECIMAL(60, 2)) TIMESTAMP(ts) PARTITION BY DAY""";
    private static final String PRICES_INSERT = """
            INSERT INTO prices VALUES
              ('2023-01-01T09:00:00.000000Z', 7.1m, 8.1m, 9.11m, 1.11m, 2.11m, 3.11m),
              ('2023-01-01T09:01:00.000000Z', 7.2m, 8.2m, 9.22m, 1.22m, 2.22m, 3.22m),
              ('2023-01-01T09:02:00.000000Z', 7.3m, 8.3m, 9.33m, 1.33m, 2.33m, 3.33m)""";
    private static final String TRADES_DDL = """
            CREATE TABLE trades (ts TIMESTAMP, d8 DECIMAL(2, 1), d16 DECIMAL(4, 1), d32 DECIMAL(9, 2),
              d64 DECIMAL(18, 2), d128 DECIMAL(38, 2), d256 DECIMAL(60, 2)) TIMESTAMP(ts) PARTITION BY DAY""";
    private static final String TRADES_INSERT = """
            INSERT INTO trades VALUES
              ('2023-01-01T09:00:00.000000Z', 1.1m, 2.1m, 3.11m, 4.11m, 5.11m, 6.11m),
              ('2023-01-01T09:01:00.000000Z', 1.2m, 2.2m, 3.22m, 4.22m, 5.22m, 6.22m),
              ('2023-01-01T09:02:00.000000Z', 1.3m, 2.3m, 3.33m, 4.33m, 5.33m, 6.33m)""";

    @Test
    public void testConstantFalseJoinDelegatesMasterDecimals() throws Exception {
        assertQuery("select t.d8, t.d16, t.d32, t.d64, t.d128, t.d256, count(p.ts) cnt " +
                "from trades t " +
                "window join prices p on (1 = 0) " +
                "range between 1 minute preceding and 1 minute following")
                .ddl(TRADES_DDL, PRICES_DDL, TRADES_INSERT, PRICES_INSERT)
                .expectSize()
                .returns("d8\td16\td32\td64\td128\td256\tcnt\n" +
                        "1.1\t2.1\t3.11\t4.11\t5.11\t6.11\tnull\n" +
                        "1.2\t2.2\t3.22\t4.22\t5.22\t6.22\tnull\n" +
                        "1.3\t2.3\t3.33\t4.33\t5.33\t6.33\tnull\n");
    }

    @Test
    public void testConstantFalseJoinPadsDecimalsWithNull() throws Exception {
        assertQuery(CONSTANT_FALSE_JOIN)
                .ddl(TRADES_DDL, PRICES_DDL, TRADES_INSERT, PRICES_INSERT)
                .timestamp("ts")
                .expectSize()
                .returns("ts\tf8\tf16\tf32\tf64\tf128\tf256\n" +
                        "2023-01-01T09:00:00.000000Z\t\t\t\t\t\t\n" +
                        "2023-01-01T09:01:00.000000Z\t\t\t\t\t\t\n" +
                        "2023-01-01T09:02:00.000000Z\t\t\t\t\t\t\n");

        assertQuery("select f8 is null n8, f16 is null n16, f32 is null n32," +
                " f64 is null n64, f128 is null n128, f256 is null n256 from (" + CONSTANT_FALSE_JOIN + ")")
                .expectSize()
                .returns("n8\tn16\tn32\tn64\tn128\tn256\n" +
                        "true\ttrue\ttrue\ttrue\ttrue\ttrue\n" +
                        "true\ttrue\ttrue\ttrue\ttrue\ttrue\n" +
                        "true\ttrue\ttrue\ttrue\ttrue\ttrue\n");
    }
}

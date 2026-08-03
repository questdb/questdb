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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Tests SAMPLE BY first()/last() over decimal columns. A FILL(NONE) query keyed by a single
 * indexed symbol value selects SampleByFirstLastRecordCursorFactory, which reads values straight
 * from page frame addresses. Its records must answer the fixed-width decimal getters; the factory
 * rejects DECIMAL128 and DECIMAL256, which do not fit the 8-byte read path.
 */
public class SampleByFirstLastDecimalTest extends AbstractCairoTest {
    private static final String FIRST_LAST_QUERY = """
            SELECT ts, sym,
                   first(d8) f8, last(d8) l8,
                   first(d16) f16, last(d16) l16,
                   first(d32) f32, last(d32) l32,
                   first(d64) f64, last(d64) l64,
                   last(v) lv
            FROM trades
            WHERE sym = 'A'
            SAMPLE BY 1h ALIGN TO FIRST OBSERVATION""";

    @Test
    public void testFirstLastOverDecimalColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE trades (
                      ts TIMESTAMP,
                      sym SYMBOL INDEX,
                      d8 DECIMAL(2, 1),
                      d16 DECIMAL(4, 1),
                      d32 DECIMAL(9, 2),
                      d64 DECIMAL(18, 2),
                      v LONG
                    ) TIMESTAMP(ts) PARTITION BY DAY""");
            execute("""
                    INSERT INTO trades VALUES
                      ('2023-01-01T00:00:00.000000Z', 'A', 1.1m, 11.1m, 111.11m, 1111.11m, 1),
                      ('2023-01-01T00:20:00.000000Z', 'A', 2.2m, 22.2m, 222.22m, 2222.22m, 2),
                      ('2023-01-01T00:40:00.000000Z', 'A', 3.3m, 33.3m, 333.33m, 3333.33m, 3),
                      ('2023-01-01T01:10:00.000000Z', 'B', 9.9m, 99.9m, 999.99m, 9999.99m, 4),
                      ('2023-01-01T02:00:00.000000Z', 'A', null, null, null, null, 5),
                      ('2023-01-01T02:30:00.000000Z', 'A', 4.4m, 44.4m, 444.44m, 4444.44m, 6),
                      ('2023-01-01T03:00:00.000000Z', 'A', 5.5m, 55.5m, 555.55m, 5555.55m, 7),
                      ('2023-01-01T03:30:00.000000Z', 'A', null, null, null, null, 8),
                      ('2023-01-01T04:00:00.000000Z', 'A', null, null, null, null, 9)""");

            // The 01:00 bucket holds only symbol B, so it is empty for this query and gets skipped.
            // 02:00 opens with a null, 03:00 closes with one, 04:00 is null throughout.
            assertQuery(FIRST_LAST_QUERY)
                    .timestamp("ts")
                    .noRandomAccess()
                    .withPlanContaining("SampleByFirstLast")
                    .returns("""
                            ts\tsym\tf8\tl8\tf16\tl16\tf32\tl32\tf64\tl64\tlv
                            2023-01-01T00:00:00.000000Z\tA\t1.1\t3.3\t11.1\t33.3\t111.11\t333.33\t1111.11\t3333.33\t3
                            2023-01-01T02:00:00.000000Z\tA\t\t4.4\t\t44.4\t\t444.44\t\t4444.44\t6
                            2023-01-01T03:00:00.000000Z\tA\t5.5\t\t55.5\t\t555.55\t\t5555.55\t\t8
                            2023-01-01T04:00:00.000000Z\tA\t\t\t\t\t\t\t\t\t9
                            """);
        });
    }

    @Test
    public void testFirstLastOverDecimalColumnsWithColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL INDEX, v LONG) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO trades VALUES
                      ('2023-01-01T00:00:00.000000Z', 'A', 1),
                      ('2023-01-01T01:00:00.000000Z', 'A', 2),
                      ('2023-01-01T02:00:00.000000Z', 'A', 3),
                      ('2023-01-01T03:00:00.000000Z', 'A', 4)""");
            execute("ALTER TABLE trades ADD COLUMN d8 DECIMAL(2, 1)");
            execute("ALTER TABLE trades ADD COLUMN d16 DECIMAL(4, 1)");
            execute("ALTER TABLE trades ADD COLUMN d32 DECIMAL(9, 2)");
            execute("ALTER TABLE trades ADD COLUMN d64 DECIMAL(18, 2)");
            execute("""
                    INSERT INTO trades VALUES
                      ('2023-01-02T00:00:00.000000Z', 'A', 5, 7.7m, 77.7m, 777.77m, 7777.77m),
                      ('2023-01-02T01:00:00.000000Z', 'A', 6, 8.8m, 88.8m, 888.88m, 8888.88m)""");

            // The first partition predates the decimal columns, so its buckets read no page at all
            // and must report null rather than a zero-valued decimal. Four buckets there route the
            // middle ones through the data record and the outer ones through the cross-frame record.
            assertQuery(FIRST_LAST_QUERY)
                    .timestamp("ts")
                    .noRandomAccess()
                    .withPlanContaining("SampleByFirstLast")
                    .returns("""
                            ts\tsym\tf8\tl8\tf16\tl16\tf32\tl32\tf64\tl64\tlv
                            2023-01-01T00:00:00.000000Z\tA\t\t\t\t\t\t\t\t\t1
                            2023-01-01T01:00:00.000000Z\tA\t\t\t\t\t\t\t\t\t2
                            2023-01-01T02:00:00.000000Z\tA\t\t\t\t\t\t\t\t\t3
                            2023-01-01T03:00:00.000000Z\tA\t\t\t\t\t\t\t\t\t4
                            2023-01-02T00:00:00.000000Z\tA\t7.7\t7.7\t77.7\t77.7\t777.77\t777.77\t7777.77\t7777.77\t5
                            2023-01-02T01:00:00.000000Z\tA\t8.8\t8.8\t88.8\t88.8\t888.88\t888.88\t8888.88\t8888.88\t6
                            """);
        });
    }

    @Test
    public void testFirstLastRejectsWideDecimalColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL INDEX, d128 DECIMAL(38, 2)," +
                    " d256 DECIMAL(60, 2)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO trades VALUES ('2023-01-01T00:00:00.000000Z', 'A', 1.11m, 2.22m)");

            assertExceptionNoLeakCheck(
                    "SELECT ts, sym, first(d128) FROM trades WHERE sym = 'A' SAMPLE BY 1h ALIGN TO FIRST OBSERVATION",
                    16,
                    "first(), last() is not supported on data type DECIMAL(38,2)"
            );
            assertExceptionNoLeakCheck(
                    "SELECT ts, sym, last(d256) FROM trades WHERE sym = 'A' SAMPLE BY 1h ALIGN TO FIRST OBSERVATION",
                    16,
                    "first(), last() is not supported on data type DECIMAL(60,2)"
            );
        });
    }
}

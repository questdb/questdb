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
 * Prunes by a monotonic function over the designated timestamp whose constant arguments have every type the
 * function accepts, on microsecond and nanosecond tables.
 */
public class MonotonicConstantArgumentTest extends AbstractCairoTest {
    private static final String[] TABLES = {"mca_micro", "mca_nano"};
    private static final String[] TIMESTAMPS = {
            "2023-12-31T20:00:00",
            "2024-01-01T00:00:00",
            "2024-01-01T05:00:00",
            "2024-01-01T12:00:00",
            "2024-01-01T23:30:00",
            "2024-01-02T03:00:00",
            "2024-01-02T12:00:00"
    };

    @Test
    public void testAddAmountTypes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String amount : new String[]{"3_600", "3_600::long", "3_600::short", "100::byte", "'5'"}) {
                assertRows("ts + " + amount + " > '2024-01-01T06:00'", 3, 4, 5, 6);
            }
            for (String amount : new String[]{"null", "null::long"}) {
                assertRows("ts + " + amount + " > '2024-01-01T06:00'");
            }
        });
    }

    @Test
    public void testDateaddAmountTypes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String amount : new String[]{"6", "6::int", "6::short", "6::byte", "'6'", "'6'::varchar"}) {
                assertRows("dateadd('h', " + amount + ", ts) > '2024-01-01T12:00'", 3, 4, 5, 6);
                assertRows("dateadd('h', " + amount + ", ts, 'Europe/Berlin') > '2024-01-01T12:00'", 3, 4, 5, 6);
            }
        });
    }

    @Test
    public void testDateaddTimezoneTypes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String timezone : new String[]{"'UTC'", "'UTC'::varchar", "'+02:00'", "'+02:00'::varchar", "'Europe/Berlin'::varchar", "'Europe/Berlin'::symbol"}) {
                assertRows("dateadd('h', 6, ts, " + timezone + ") > '2024-01-01T12:00'", 3, 4, 5, 6);
            }
        });
    }

    @Test
    public void testDateaddUnitTypes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String unit : new String[]{"'h'", "'h'::varchar", "'h'::string", "'h'::symbol"}) {
                assertRows("dateadd(" + unit + ", 6, ts) > '2024-01-01T12:00'", 3, 4, 5, 6);
                assertRows("dateadd(" + unit + ", 6, ts, 'UTC') > '2024-01-01T12:00'", 3, 4, 5, 6);
            }
        });
    }

    @Test
    public void testDateTruncUnitTypes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String unit : new String[]{"'day'", "'day'::varchar", "'day'::symbol"}) {
                assertRows("date_trunc(" + unit + ", ts) = '2024-01-01'", 1, 2, 3, 4);
            }
        });
    }

    @Test
    public void testFloorOffsetTypes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String offset : new String[]{"'06:00'", "'06:00'::varchar"}) {
                assertRows("timestamp_floor('1d', ts, null, " + offset + ", null) = '2024-01-01T06:00'", 3, 4, 5);
                assertRows("timestamp_floor_utc('1d', ts, null, " + offset + ", 'UTC') = '2024-01-01T06:00'", 3, 4, 5);
            }
            for (String offset : new String[]{"'00:00'", "'00:00'::varchar", "null", "null::varchar"}) {
                assertRows("timestamp_floor('1d', ts, null, " + offset + ", null) = '2024-01-01'", 1, 2, 3, 4);
            }
        });
    }

    @Test
    public void testFloorOriginTypes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String origin : new String[]{"0", "0::long", "0::date", "0::timestamp", "0::timestamp_ns", "null", "'1970-01-01'"}) {
                assertRows("timestamp_floor('1d', ts, " + origin + ") = '2024-01-01'", 1, 2, 3, 4);
                assertRows("timestamp_floor('1d', ts, " + origin + ", '00:00', null) = '2024-01-01'", 1, 2, 3, 4);
            }
            for (String origin : new String[]{"1_800_000::date", "1_800_000_000::timestamp", "1_800_000_000_000::timestamp_ns", "'1970-01-01T00:30:00.000000Z'"}) {
                assertRows("timestamp_floor('1d', ts, " + origin + ") = '2024-01-01T00:30'", 2, 3, 4);
                assertRows("timestamp_floor('1d', ts, " + origin + ", '00:00', null) = '2024-01-01T00:30'", 2, 3, 4);
            }
            for (String origin : new String[]{"1_800_000_000", "1_800_000_000::long"}) {
                assertTableRows(0, "timestamp_floor('1d', ts, " + origin + ") = '2024-01-01T00:30'", 2, 3, 4);
                assertTableRows(0, "timestamp_floor('1d', ts, " + origin + ", '00:00', null) = '2024-01-01T00:30'", 2, 3, 4);
            }
            assertTableRows(1, "timestamp_floor('1d', ts, 1_800_000_000_000) = '2024-01-01T00:30'", 2, 3, 4);
            assertTableRows(1, "timestamp_floor('1d', ts, 1_800_000_000) = '2024-01-01T00:00:01.8'", 2, 3, 4);
        });
    }

    @Test
    public void testFloorOriginTypesUtc() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String origin : new String[]{"0", "0::long", "0::date", "0::timestamp", "null"}) {
                assertRows("timestamp_floor_utc('1d', ts, " + origin + ", '00:00', 'UTC') = '2024-01-01'", 1, 2, 3, 4);
            }
        });
    }

    @Test
    public void testFloorStrideTypes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String stride : new String[]{"'d'", "'1d'", "'d'::varchar", "'1d'::varchar", "'d'::string", "'1d'::symbol"}) {
                assertRows("timestamp_floor(" + stride + ", ts) = '2024-01-01'", 1, 2, 3, 4);
                assertRows("timestamp_floor(" + stride + ", ts, 0) = '2024-01-01'", 1, 2, 3, 4);
                assertRows("timestamp_floor(" + stride + ", ts, null, '00:00', null) = '2024-01-01'", 1, 2, 3, 4);
            }
        });
    }

    @Test
    public void testFloorTimezoneTypes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String timezone : new String[]{"'UTC'", "'UTC'::varchar", "'Z'", "null", "null::varchar", "null::string"}) {
                assertRows("timestamp_floor('1d', ts, null, '00:00', " + timezone + ") = '2024-01-01'", 1, 2, 3, 4);
            }
            for (String timezone : new String[]{"'+02:00'", "'+02:00'::varchar", "'Europe/Berlin'", "'Europe/Berlin'::varchar", "'Europe/Berlin'::symbol"}) {
                assertRows("timestamp_floor('1d', ts, null, '00:00', " + timezone + ") = '2024-01-01'", 1, 2, 3);
            }
            for (String timezone : new String[]{"'UTC'", "'UTC'::varchar", "'+02:00'::varchar", "'Europe/Berlin'::symbol"}) {
                assertRows("timestamp_floor_utc('1h', ts, null, '00:00', " + timezone + ") > '2024-01-01T04:00'", 2, 3, 4, 5, 6);
            }
        });
    }

    @Test
    public void testReproductionFloorIntOrigin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('2024-01-01T01:00:00.000000Z', 1), ('2024-01-01T23:00:00.000000Z', 2), ('2024-01-02T01:00:00.000000Z', 3)");
            assertQuery("SELECT * FROM t WHERE timestamp_floor('1d', ts, 0) = '2024-01-01'")
                    .timestamp("ts")
                    .withPlanContaining("Interval forward scan on: t")
                    .returns("""
                            ts\tx
                            2024-01-01T01:00:00.000000Z\t1
                            2024-01-01T23:00:00.000000Z\t2
                            """);
        });
    }

    @Test
    public void testSubAmountTypes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String amount : new String[]{"3_600", "3_600::long", "3_600::short", "100::byte", "'5'"}) {
                assertRows("ts - " + amount + " < '2024-01-01T06:00'", 0, 1, 2);
            }
            for (String amount : new String[]{"null", "null::long"}) {
                assertRows("ts - " + amount + " < '2024-01-01T06:00'");
            }
        });
    }

    @Test
    public void testTimestampCeilUnitTypes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String unit : new String[]{"'h'", "'h'::varchar", "'h'::string", "'h'::symbol"}) {
                assertRows("timestamp_ceil(" + unit + ", ts) <= '2024-01-01T05:00'", 0, 1);
            }
        });
    }

    @Test
    public void testToTimezoneTypes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String timezone : new String[]{"'+02:00'", "'+02:00'::varchar", "'Europe/Berlin'", "'Europe/Berlin'::varchar", "'Europe/Berlin'::symbol"}) {
                assertRows("to_timezone(ts, " + timezone + ") >= '2024-01-01T06:00'", 2, 3, 4, 5, 6);
            }
        });
    }

    @Test
    public void testToUtcTypes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String timezone : new String[]{"'+02:00'", "'+02:00'::varchar", "'Europe/Berlin'", "'Europe/Berlin'::varchar", "'Europe/Berlin'::symbol"}) {
                assertRows("to_utc(ts, " + timezone + ") < '2024-01-01T06:00'", 0, 1, 2);
            }
        });
    }

    private static void createTables() throws Exception {
        final StringBuilder values = new StringBuilder();
        for (int i = 0; i < TIMESTAMPS.length; i++) {
            values.append(i == 0 ? "" : ", ").append("('").append(TIMESTAMPS[i]).append(".000000Z', ").append(i).append(')');
        }
        execute("CREATE TABLE mca_micro (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE mca_nano (ts TIMESTAMP_NS, x INT) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO mca_micro VALUES " + values);
        execute("INSERT INTO mca_nano VALUES " + values);
    }

    private static String expected(boolean isNano, int... rows) {
        final StringBuilder expected = new StringBuilder("ts\tx\n");
        for (int row : rows) {
            expected.append(TIMESTAMPS[row]).append(isNano ? ".000000000Z\t" : ".000000Z\t").append(row).append('\n');
        }
        return expected.toString();
    }

    private void assertRows(String predicate, int... rows) throws Exception {
        assertTableRows(0, predicate, rows);
        assertTableRows(1, predicate, rows);
    }

    private void assertTableRows(int table, String predicate, int... rows) throws Exception {
        assertQuery("SELECT * FROM " + TABLES[table] + " WHERE " + predicate).noLeakCheck().timestamp("ts").returns(expected(table == 1, rows));
    }
}

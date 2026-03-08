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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.cairo.ColumnType;
import io.questdb.std.Chars;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Locale;

import static io.questdb.cairo.ColumnType.*;


public class GroupByFunctionCaseTest extends AbstractCairoTest {

    private final StringSink planSink = new StringSink();
    private final StringSink sqlSink = new StringSink();

    @Test
    public void testAggregatesOnColumnWithNoKeyWorkRegardlessOfCase() throws Exception {
        assertMemoryLeak(() -> {
            String[] functions = {"KSum", "NSum", "Sum", "Avg", "Min", "Max"};
            String[][] expectedFunctions = {
                    {"ksum(val)", "nsum(val)", "sum(val)", "avg(val)", "min(val)", "max(val)"}, // byte
                    {"ksum(val)", "nsum(val)", "sum(val)", "avg(val)", "min(val)", "max(val)"}, // short
                    {null, null, null, null, "min(val)", "max(val)"}, // char
                    {"ksum(val)", "nsum(val)", "sum(val)", "avg(val)", "min(val)", "max(val)"}, // int
                    {"ksum(val)", "nsum(val)", "sum(val)", "avg(val)", "min(val)", "max(val)"}, // long
                    {null, null, null, null, "min(val)", "max(val)"}, // date
                    {null, null, null, null, "min(val)", "max(val)"}, // timestamp
                    {"ksum(val)", "nsum(val)", "sum(val)", "avg(val)", "min(val)", "max(val)"}, // float
                    {"ksum(val)", "nsum(val)", "sum(val)", "avg(val)", "min(val)", "max(val)"}, // double
            };
            // other types aren't accepted by aggregates at all (including string and symbol!)
            for (int t = BYTE; t <= DOUBLE; t++) {
                String typeName = name(ColumnType.nameOf(t));
                sqlSink.clear();
                sqlSink.put("create table test (key int, val ").put(typeName).put(");");
                execute(sqlSink);

                for (int f = 0; f < functions.length; f++) {
                    String function = functions[f];
                    String expectedFunction = expectedFunctions[t - BYTE][f];
                    if (expectedFunction == null) {
                        continue;
                    }

                    prepareExpectedPlan(t, f, null, function, expectedFunction);

                    sqlSink.clear();
                    sqlSink.put("select ").put(function.toLowerCase()).put("(val) agg from test");
                    assertExecutionPlan(sqlSink, typeName, function, planSink);

                    sqlSink.clear();
                    sqlSink.put("select ").put(function.toUpperCase()).put("(val) agg from test");
                    assertExecutionPlan(sqlSink, typeName, function, planSink);
                }

                execute("drop table test;");
            }
        });
    }

    @Test
    public void testAggregatesOnColumnWithSingleKeyWorkRegardlessOfCase() throws Exception {
        assertMemoryLeak(() -> {
            String[] functions = {"KSum", "NSum", "Sum", "Avg", "Min", "Max"};
            String[][] expectedFunctions = {
                    {"ksum(val)", "nsum(val)", "sum(val)", "avg(val)", "min(val)", "max(val)"}, // byte
                    {"ksum(val)", "nsum(val)", "sum(val)", "avg(val)", "min(val)", "max(val)"}, // short
                    {null, null, null, null, "min(val)", "max(val)"}, // char
                    {"ksum(val)", "nsum(val)", "sum(val)", "avg(val)", "min(val)", "max(val)"}, // int
                    {"ksum(val)", "nsum(val)", "sum(val)", "avg(val)", "min(val)", "max(val)"}, // long
                    {null, null, null, null, "min(val)", "max(val)"}, // date
                    {null, null, null, null, "min(val)", "max(val)"}, // timestamp
                    {"ksum(val)", "nsum(val)", "sum(val)", "avg(val)", "min(val)", "max(val)"}, // float
                    {"ksum(val)", "nsum(val)", "sum(val)", "avg(val)", "min(val)", "max(val)"}, // double
            };
            // other types aren't accepted by aggregates at all (including string and symbol!)
            for (int t = BYTE; t <= DOUBLE; t++) {
                String typeName = name(ColumnType.nameOf(t));
                sqlSink.clear();
                sqlSink.put("create table test (key int, val ").put(typeName).put(");");
                execute(sqlSink);

                for (int f = 0; f < functions.length; f++) {
                    String function = functions[f];
                    String expectedFunction = expectedFunctions[t - BYTE][f];
                    if (expectedFunction == null) {
                        continue;
                    }

                    prepareExpectedPlan(t, f, "key", function, expectedFunction);

                    sqlSink.clear();
                    sqlSink.put("select key, ").put(function.toLowerCase()).put("(val) agg from test group by key;");
                    assertExecutionPlan(sqlSink, typeName, function, planSink);

                    sqlSink.clear();
                    sqlSink.put("select key, ").put(function.toUpperCase()).put("(val) agg from test group by key;");
                    assertExecutionPlan(sqlSink, typeName, function, planSink);
                }

                execute("drop table test;");
            }
        });
    }

    @Test
    public void testAggregatesOnColumnWithTwoKeysAreNotVectorized() throws Exception {
        assertMemoryLeak(() -> {
            String[] functions = {"KSum", "NSum", "Sum", "Avg", "Min", "Max"};

            for (int t = BYTE; t <= DOUBLE; t++) {
                String typeName = name(ColumnType.nameOf(t));
                sqlSink.clear();
                sqlSink.put("create table test ( key1 byte, key2 byte, val ").put(typeName).put(");");
                execute(sqlSink);

                for (int f = 0; f < functions.length; f++) {
                    String function = functions[f];

                    // sum,avg don't work for data & timestamp if there's more than 1 key
                    if ((t == CHAR && f < 4) || ((t == DATE || t == TIMESTAMP) && f < 4)) {
                        continue;
                    }

                    sqlSink.clear();
                    sqlSink.put("select key1, key2, ").put(function).put("(val) agg from test group by key1, key2;");

                    try {
                        StringSink planSink = getPlanSink(sqlSink).getSink();
                        if (Chars.contains(planSink, "vectorized: false") || Chars.contains(planSink, "Async Group By workers: 1")) {
                            continue;
                        }
                        Assert.fail("vectorized execution is not expected");
                    } catch (Exception ae) {
                        throwWithContext(typeName, function, ae);
                    }
                }

                execute("drop table test;");
            }
        });
    }

    @Test
    public void testGetPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE spot_trades (\n" +
                    "  id LONG,\n" +
                    "  instrument_key SYMBOL capacity 256 CACHE,\n" +
                    "  venue SYMBOL capacity 256 CACHE,\n" +
                    "  base_ccy SYMBOL capacity 256 CACHE,\n" +
                    "  quote_ccy SYMBOL capacity 256 CACHE,\n" +
                    "  symbol SYMBOL capacity 256 CACHE index capacity 256,\n" +
                    "  created_timestamp TIMESTAMP,\n" +
                    "  trade_timestamp TIMESTAMP,\n" +
                    "  side SYMBOL capacity 256 CACHE,\n" +
                    "  qty DOUBLE,\n" +
                    "  price DOUBLE,\n" +
                    "  trade_id STRING,\n" +
                    "  notional_usd DOUBLE,\n" +
                    "  notional_base_ccy DOUBLE\n" +
                    ") timestamp (trade_timestamp) PARTITION BY DAY;");

            assertPlanNoLeakCheck(
                    "SELECT  \n" +
                            "    trade_timestamp as candle_st,\n" +
                            "    venue,\n" +
                            "    count(*) AS num_ticks,\n" +
                            "    SUM(qty*price) AS quote_volume,\n" +
                            "    SUM(qty*price)/SUM(qty) AS vwap\n" +
                            "  FROM 'spot_trades'\n" +
                            "  WHERE \n" +
                            "    instrument_key like 'ETH_USD_S_%'\n" +
                            "    AND trade_timestamp >= '2022-01-01 00:00'\n" +
                            "    AND venue in ('CBS', 'FUS', 'LMX', 'BTS')\n" +
                            "  SAMPLE BY 1h \n" +
                            "  ALIGN TO CALENDAR TIME ZONE 'UTC'",
                    "Radix sort light\n" +
                            "  keys: [candle_st]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [candle_st,venue,num_ticks,quote_volume,quote_volume/SUM]\n" +
                            "        Async Group By workers: 1\n" +
                            "          keys: [candle_st,venue]\n" +
                            "          values: [count(*),sum(qty*price),sum(qty)]\n" +
                            "          filter: (instrument_key ~ ETH.USD.S..*? [state-shared] and venue in [CBS,FUS,LMX,BTS])\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Interval forward scan on: spot_trades\n" +
                            "                  intervals: [(\"2022-01-01T00:00:00.000000Z\",\"MAX\")]\n"
            );
        });
    }

    private static String name(String type) {
        return Character.toUpperCase(type.charAt(0)) + type.substring(1).toLowerCase(Locale.ROOT);
    }

    private void assertExecutionPlan(StringSink sink, String typeName, String function, CharSequence expectedPlan) throws Exception {
        try {
            assertPlanNoLeakCheck(sink, expectedPlan);
        } catch (AssertionError ae) {
            throwWithContext(typeName, function, ae);
        }
    }

    private void prepareExpectedPlan(int t, int f, String keys, String function, String expectedFunction) {
        boolean rosti = (t >= INT && t <= TIMESTAMP && f > 1) || t == DOUBLE || (t == SHORT && !function.contains("KSum") && !function.contains("NSum"));

        planSink.clear();
        if (rosti) {
            planSink.put("GroupBy vectorized: true workers: 1\n");
        } else {
            planSink.put("Async Group By workers: 1\n");
        }
        if (keys != null) {
            planSink.put("  keys: [").put(keys).put("]\n");
        }
        planSink.put("  values: [").put(expectedFunction).put("]\n");
        if (!rosti) {
            planSink.put("  filter: null\n");
        }
        planSink.put("    PageFrame\n" +
                "        Row forward scan\n" +
                "        Frame forward scan on: test\n");
    }

    private void throwWithContext(String typeName, String function, Throwable ae) {
        AssertionError newAe = new AssertionError(ae.getMessage().replaceAll(">$", "") + "\n\nfor [columnType=" + typeName + ",function=" + function + "]>");
        newAe.setStackTrace(ae.getStackTrace());
        throw newAe;
    }
}

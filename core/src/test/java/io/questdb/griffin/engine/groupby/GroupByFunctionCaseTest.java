/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

import java.util.Locale;

import static io.questdb.cairo.ColumnType.*;


public class GroupByFunctionCaseTest extends AbstractGriffinTest {

    StringSink sqlSink = new StringSink();
    StringSink planSink = new StringSink();

    @Test
    public void testGetPlan() throws SqlException {
        compile("CREATE TABLE spot_trades (\n" +
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

        assertPlan("SELECT  \n" +
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
                "VirtualRecord\n" +
                        "  functions: [Timestamp(0),Symbol(1),Long(2),Double(3),Double(3)/Double(4)]\n" +
                        "    SampleByFillNone\n" +
                        "      functions: [Timestamp,MapSymbol,count(1),sum(Double(3)*Double(2)),sum(Double(3))]\n" +
                        "        SelectedRecord\n" +
                        "            async filter\n" +
                        "              filter: Symbol(4) ~ ETH.USD.S..*? and Symbol(1) in [CBS,FUS,LMX,BTS]\n" +
                        "              preTouch: true\n" +
                        "              workers: 1\n" +
                        "                Interval forward Scan on: spot_trades\n" +
                        "                  intervals: [static=[1640995200000000,9223372036854775807]\n");
    }

    @Test
    public void testAggregatesOnColumnWithNoKeyWorkRegardlessOfCase() throws Exception {
        assertMemoryLeak(() -> {
            String[] functions = {"KSum", "NSum", "Sum", "Avg", "Min", "Max"};
            String[][] expectedFunctions = {{"ksum(Byte(0))", "nsum(Byte(0))", "sum(Byte(0))", "avg(Byte(0))", "min(Byte(0))", "max(Byte(0))"},//byte
                    {"ksum(Short(0))", "nsum(Short(0))", "sum(Short(0))", "avg(Short(0))", "min(Short(0))", "max(Short(0))"},//short
                    {null, null, null, null, "min(Char(0))", "max(Char(0))"},//char
                    {"ksum(Int(0))", "nsum(Int(0))", "sum(Int(0))", "avg(Int(0))", "min(Int(0))", "max(Int(0))"},//int
                    {"ksum(Long(0))", "nsum(Long(0))", "sum(Long(0))", "avg(Long(0))", "min(Long(0))", "max(Long(0))"},//long
                    {null, null, "sum(Date(0))", "avg(Long(0))", "min(Date(0))", "max(Date(0))"},//date
                    {null, null, "sum(Timestamp(0))", "avg(Long(0))", "min(Timestamp(0))", "max(Timestamp(0))"},//timestamp
                    {"ksum(Float(0))", "nsum(Float(0))", "sum(Float(0))", "avg(Float(0))", "min(Float(0))", "max(Float(0))"}, //float
                    {"ksum(Double(0))", "nsum(Double(0))", "sum(Double(0))", "avg(Double(0))", "min(Double(0))", "max(Double(0))"}, //double
            };
            //other types aren't accepted by aggregates at all (including string and symbol!)
            for (int t = BYTE; t <= DOUBLE; t++) {
                String typeName = name(ColumnType.nameOf(t));
                sqlSink.clear();
                sqlSink.put("create table test ( key int, val ").put(typeName).put(");");
                compile(sqlSink);

                for (int f = 0; f < functions.length; f++) {
                    String function = functions[f];
                    String expectedFunction = expectedFunctions[t - BYTE][f];
                    if (expectedFunction == null) {
                        continue;
                    }

                    boolean vectorized = (t >= INT && t <= TIMESTAMP && f > 1) || t == DOUBLE;

                    planSink.clear();
                    planSink.put("GroupByNotKeyed vectorized: ").put(vectorized).put("\n")
                            .put("  groupByFunctions: [").put(expectedFunction).put("]\n")
                            .put("    Full forward scan on: test\n");

                    sqlSink.clear();
                    sqlSink.put("select ").put(function.toLowerCase()).put("(val) agg from test");
                    assertExecutionPlan(sqlSink, typeName, function, planSink);

                    sqlSink.clear();
                    sqlSink.put("select ").put(function.toUpperCase()).put("(val) agg from test");
                    assertExecutionPlan(sqlSink, typeName, function, planSink);
                }

                compile("drop table test;");
            }
        });
    }

    private void assertExecutionPlan(StringSink sink, String typeName, String function, CharSequence expectedPlan) throws SqlException {
        try {
            assertPlan(sink, expectedPlan);
        } catch (AssertionError ae) {
            throwWithContext(typeName, function, ae);
        }
    }

    @Test
    public void testAggregatesOnColumnWithSingleKeyWorkRegardlessOfCase() throws Exception {
        assertMemoryLeak(() -> {
            String[] functions = {"KSum", "NSum", "Sum", "Avg", "Min", "Max"};
            String[][] expectedFunctions = {{"ksum(Byte(1))", "nsum(Byte(1))", "sum(Byte(1))", "avg(Byte(1))", "min(Byte(1))", "max(Byte(1))"},//byte
                    {"ksum(Short(1))", "nsum(Short(1))", "sum(Short(1))", "avg(Short(1))", "min(Short(1))", "max(Short(1))"},//short
                    {null, null, null, null, "min(Char(1))", "max(Char(1))"},//char
                    {"ksum(Int(1))", "nsum(Int(1))", "sum(Int(1))", "avg(Int(1))", "min(Int(1))", "max(Int(1))"},//int
                    {"ksum(Long(1))", "nsum(Long(1))", "sum(Long(1))", "avg(Long(1))", "min(Long(1))", "max(Long(1))"},//long
                    {null, null, "sum(Date(1))", "avg(Long(1))", "min(Date(1))", "max(Date(1))"},//date
                    {null, null, "sum(Timestamp(1))", "avg(Long(1))", "min(Timestamp(1))", "max(Timestamp(1))"},//timestamp
                    {"ksum(Float(1))", "nsum(Float(1))", "sum(Float(1))", "avg(Float(1))", "min(Float(1))", "max(Float(1))"}, //float
                    {"ksum(Double(1))", "nsum(Double(1))", "sum(Double(1))", "avg(Double(1))", "min(Double(1))", "max(Double(1))"}, //double
            };
            //other types aren't accepted by aggregates at all (including string and symbol!)
            for (int t = BYTE; t <= DOUBLE; t++) {
                String typeName = name(ColumnType.nameOf(t));
                sqlSink.clear();
                sqlSink.put("create table test ( key int, val ").put(typeName).put(");");
                compile(sqlSink);

                for (int f = 0; f < functions.length; f++) {

                    String function = functions[f];
                    String expectedFunction = expectedFunctions[t - BYTE][f];
                    if (expectedFunction == null) {
                        continue;
                    }

                    boolean vectorized = (t >= INT && t <= TIMESTAMP && f > 1) || t == DOUBLE;
                    int keyPos = f < 2 ? 3 : f < 4 ? 2 : 1;
                    if (t == FLOAT && f == 2) {
                        keyPos = 1;
                    }

                    planSink.clear();
                    planSink.put("GroupByRecord vectorized: ").put(vectorized + "\n")
                            .put("  groupByFunctions: [").put(expectedFunction).put("]\n")
                            .put("  ").put((vectorized ? "keyColumnIndex: 0\n" : "recordFunctions: [Int(" + keyPos + ")," + expectedFunction + "]\n"))
                            .put(vectorized ? "  workers: 1\n" : "")
                            .put("    Full forward scan on: test\n");

                    sqlSink.clear();
                    sqlSink.put("select key, ").put(function.toLowerCase()).put("(val) agg from test group by key;");
                    assertExecutionPlan(sqlSink, typeName, function, planSink);

                    sqlSink.clear();
                    sqlSink.put("select key, ").put(function.toUpperCase()).put("(val) agg from test group by key;");
                    assertExecutionPlan(sqlSink, typeName, function, planSink);
                }

                compile("drop table test;");
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
                compile(sqlSink);

                for (int f = 0; f < functions.length; f++) {
                    String function = functions[f];

                    //sum,avg don't work for data & timestamp if there's more than 1 key
                    if ((t == CHAR && f < 4) || ((t == DATE || t == TIMESTAMP) && f < 4)) {
                        continue;
                    }

                    sqlSink.clear();
                    sqlSink.put("select key1, key2, ").put(function).put("(val) agg from test group by key1, key2;");

                    try {
                        Assert.assertTrue(Chars.contains(getPlanSink(sqlSink).getText(), "vectorized: false"));
                    } catch (Exception ae) {
                        throwWithContext(typeName, function, ae);
                    }
                }

                compile("drop table test;");
            }
        });
    }

    private void throwWithContext(String typeName, String function, Throwable ae) {
        AssertionError newAe = new AssertionError(ae.getMessage().replaceAll(">$", "") + "\n\nfor [columnType=" + typeName + ",function=" + function + "]>");
        newAe.setStackTrace(ae.getStackTrace());
        throw newAe;
    }

    private static String name(String type) {
        return Character.toUpperCase(type.charAt(0)) + type.substring(1).toLowerCase(Locale.ROOT);
    }

}

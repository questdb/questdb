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
    public void testAggregatesOnColumnWithNoKeyWorkRegardlessOfCase() throws Exception {
        assertMemoryLeak(() -> {
            String[] functions = {"KSum", "NSum", "Sum", "Avg", "Min", "Max"};
            String[][] expectedFunctions = {{"KSumDouble(ByteColumn(0))", "NSumDouble(ByteColumn(0))", "SumInt(ByteColumn(0))", "AvgDouble(ByteColumn(0))", "MinInt(ByteColumn(0))", "MaxInt(ByteColumn(0))"},//byte
                    {"KSumDouble(ShortColumn(0))", "NSumDouble(ShortColumn(0))", "SumInt(ShortColumn(0))", "AvgDouble(ShortColumn(0))", "MinInt(ShortColumn(0))", "MaxInt(ShortColumn(0))"},//short
                    {null, null, null, null, "MinChar(CharColumn(0))", "MaxChar(CharColumn(0))"},//char
                    {"KSumDouble(IntColumn(0))", "NSumDouble(IntColumn(0))", "SumIntVector(0)", "AvgIntVector(0)", "MinIntVector(0)", "MaxIntVector(0)"},//int
                    {"KSumDouble(LongColumn(0))", "NSumDouble(LongColumn(0))", "SumLongVector(0)", "AvgLongVector(0)", "MinLongVector(0)", "MaxLongVector(0)"},//long
                    {null, null, "SumDateVector(0)", "AvgLongVector(0)", "MinDateVector(0)", "MaxDateVector(0)"},//date
                    {null, null, "SumTimestampVector(0)", "AvgLongVector(0)", "MinTimestampVector(0)", "MaxTimestampVector(0)"},//timestamp
                    {"KSumDouble(FloatColumn(0))", "NSumDouble(FloatColumn(0))", "SumFloat(FloatColumn(0))", "AvgDouble(FloatColumn(0))", "MinFloat(FloatColumn(0))", "MaxFloat(FloatColumn(0))"}, //float
                    {"KSumDoubleVector(0)", "NSumDoubleVector(0)", "SumDoubleVector(0)", "AvgDoubleVector(0)", "MinDoubleVector(0)", "MaxDoubleVector(0)"}, //double
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
                    planSink.put("GroupByNotKeyed vectorized=").put(vectorized).put("\n")
                            .put("  groupByFunctions=[").put(expectedFunction).put("]\n")
                            .put("    DataFrameRecordCursorFactory\n")
                            .put("        FullFwdDataFrame\n")
                            .put("          tableName=test");

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
            String[][] expectedFunctions = {{"KSumDouble(ByteColumn(1))", "NSumDouble(ByteColumn(1))", "SumInt(ByteColumn(1))", "AvgDouble(ByteColumn(1))", "MinInt(ByteColumn(1))", "MaxInt(ByteColumn(1))"},//byte
                    {"KSumDouble(ShortColumn(1))", "NSumDouble(ShortColumn(1))", "SumInt(ShortColumn(1))", "AvgDouble(ShortColumn(1))", "MinInt(ShortColumn(1))", "MaxInt(ShortColumn(1))"},//short
                    {null, null, null, null, "MinChar(CharColumn(1))", "MaxChar(CharColumn(1))"},//char
                    {"KSumDouble(IntColumn(1))", "NSumDouble(IntColumn(1))", "SumIntVector(1)", "AvgIntVector(1)", "MinIntVector(1)", "MaxIntVector(1)"},//int
                    {"KSumDouble(LongColumn(1))", "NSumDouble(LongColumn(1))", "SumLongVector(1)", "AvgLongVector(1)", "MinLongVector(1)", "MaxLongVector(1)"},//long
                    {null, null, "SumDateVector(1)", "AvgLongVector(1)", "MinDateVector(1)", "MaxDateVector(1)"},//date
                    {null, null, "SumTimestampVector(1)", "AvgLongVector(1)", "MinTimestampVector(1)", "MaxTimestampVector(1)"},//timestamp
                    {"KSumDouble(FloatColumn(1))", "NSumDouble(FloatColumn(1))", "SumFloat(FloatColumn(1))", "AvgDouble(FloatColumn(1))", "MinFloat(FloatColumn(1))", "MaxFloat(FloatColumn(1))"}, //float
                    {"KSumDoubleVector(1)", "NSumDoubleVector(1)", "SumDoubleVector(1)", "AvgDoubleVector(1)", "MinDoubleVector(1)", "MaxDoubleVector(1)"}, //double
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
                    planSink.put("GroupByRecord vectorized=").put(vectorized + "\n")
                            .put("  groupByFunctions=[").put(expectedFunction).put("]\n")
                            .put("  ").put((vectorized ? "keyColumnIndex=0\n" : "recordFunctions=[IntColumn(" + keyPos + ")," + expectedFunction + "]\n"))
                            .put("    DataFrameRecordCursorFactory\n")
                            .put("        FullFwdDataFrame\n")
                            .put("          tableName=test");

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
                        Assert.assertTrue(Chars.contains(getPlan(sqlSink).getText(), "vectorized=false"));
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

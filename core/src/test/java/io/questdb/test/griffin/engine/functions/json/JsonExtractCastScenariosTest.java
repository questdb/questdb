/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.griffin.engine.functions.json;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.test.AbstractCairoTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests extracting JSON values as various SQL types.
 * <p>
 * The first column in the `scenarios` table represents the input JSON token.
 * The remaining columns represent the expected output when casting the JSON token to the corresponding SQL type.
 * <p>
 * The mechanics of the test are:
 * * setUp() creates a table with the document containing a JSON array of the first column of the `scenarios` table.
 * * Each test loops through each scenario and tests the extraction of the JSON token as a SQL type.
 * * It asserts by performing a SQL query.
 * * tearDown() drops the table.
 */
public class JsonExtractCastScenariosTest extends AbstractCairoTest {
    private static final String castsDoc;
    private static final String[][] scenarios = new String[][]{
            // json token, ::boolean, ::short, ::int, ::long, ::double, ::varchar
            {"null", "false", "0", "null", "null", "null", ""},
            {"true", "true", "1", "1", "1", "1.0", "true"},
            {"false", "false", "0", "0", "0", "0.0", "false"},
            {"1", "false", "1", "1", "1", "1.0", "1"},
            {"0", "false", "0", "0", "0", "0.0", "0"},
            {"-1", "false", "-1", "-1", "-1", "-1.0", "-1"},
            {"\"true\"", "false", "0", "null", "null", "null", "true"},
            {"\"false\"", "false", "0", "null", "null", "null", "false"},
            {"\"null\"", "false", "0", "null", "null", "null", "null"},
            {"\"1\"", "false", "0", "null", "null", "null", "1"},
            {"\"0\"", "false", "0", "null", "null", "null", "0"},
            {"\"\"", "false", "0", "null", "null", "null", ""},
            {"\" \"", "false", "0", "null", "null", "null", " "},
            {"\"  \"", "false", "0", "null", "null", "null", "  "},
            {"\"  true\"", "false", "0", "null", "null", "null", "  true"},
            {"\"true  \"", "false", "0", "null", "null", "null", "true  "},
            {"\"  true  \"", "false", "0", "null", "null", "null", "  true  "},
            {"\"  false\"", "false", "0", "null", "null", "null", "  false"},
            {"\"false  \"", "false", "0", "null", "null", "null", "false  "},
            {"\"  false  \"", "false", "0", "null", "null", "null", "  false  "},
            {"\"  null\"", "false", "0", "null", "null", "null", "  null"},
            {"\"null  \"", "false", "0", "null", "null", "null", "null  "},
            {"\"  null  \"", "false", "0", "null", "null", "null", "  null  "},
            {"\"  abc\"", "false", "0", "null", "null", "null", "  abc"},
            {"\"abc  \"", "false", "0", "null", "null", "null", "abc  "},
            {"\"  abc  \"", "false", "0", "null", "null", "null", "  abc  "},
            {"\"esc\\\"aping\"", "false", "0", "null", "null", "null", "esc\"aping"},
            {"0.0", "false", "0", "0", "0", "0.0", "0.0"},
            {"1.0", "false", "1", "1", "1", "1.0", "1.0"},
            {"1e1", "false", "10", "10", "10", "10.0", "1e1"},
            {"1e+1", "false", "10", "10", "10", "10.0", "1e+1"},
            {"1e-1", "false", "0", "0", "0", "0.1", "1e-1"},
            {"1e01", "false", "10", "10", "10", "10.0", "1e01"},
            {"1E1", "false", "10", "10", "10", "10.0", "1E1"},
            {"1E+1", "false", "10", "10", "10", "10.0", "1E+1"},
            {"1E-1", "false", "0", "0", "0", "0.1", "1E-1"},
            {"1E01", "false", "10", "10", "10", "10.0", "1E01"},
            {"1E+01", "false", "10", "10", "10", "10.0", "1E+01"},
            {"0.25", "false", "0", "0", "0", "0.25", "0.25"},
            {"1.25", "false", "1", "1", "1", "1.25", "1.25"},
            {"1.25e2", "false", "125", "125", "125", "125.0", "1.25e2"},
            {"1.25e+2", "false", "125", "125", "125", "125.0", "1.25e+2"},
            {"1.25e-2", "false", "0", "0", "0", "0.0125", "1.25e-2"},
            {"1.25e02", "false", "125", "125", "125", "125.0", "1.25e02"},
            {"1.25e+02", "false", "125", "125", "125", "125.0", "1.25e+02"},
            {"1.25e-02", "false", "0", "0", "0", "0.0125", "1.25e-02"},
            {"1.25e+02", "false", "125", "125", "125", "125.0", "1.25e+02"},
            {"2.0", "false", "2", "2", "2", "2.0", "2.0"},
            {"2.5", "false", "2", "2", "2", "2.5", "2.5"},
            {"2.75", "false", "2", "2", "2", "2.75", "2.75"},
            {"-2.0", "false", "-2", "-2", "-2", "-2.0", "-2.0"},
            {"-2.5", "false", "-2", "-2", "-2", "-2.5", "-2.5"},
            {"-2.75", "false", "-2", "-2", "-2", "-2.75", "-2.75"},
            {"-1.0", "false", "-1", "-1", "-1", "-1.0", "-1.0"},
            {"-0.25", "false", "0", "0", "0", "-0.25", "-0.25"},
            {"-1.25", "false", "-1", "-1", "-1", "-1.25", "-1.25"},
            {"-1.25e2", "false", "-125", "-125", "-125", "-125.0", "-1.25e2"},
            {"-1.25e+2", "false", "-125", "-125", "-125", "-125.0", "-1.25e+2"},
            {"-1.25e-2", "false", "0", "0", "0", "-0.0125", "-1.25e-2"},
            {"-1.25e02", "false", "-125", "-125", "-125", "-125.0", "-1.25e02"},
            {"-1.25e+02", "false", "-125", "-125", "-125", "-125.0", "-1.25e+02"},
            {"-1.25e-02", "false", "0", "0", "0", "-0.0125", "-1.25e-02"},
            {"-1.25e+02", "false", "-125", "-125", "-125", "-125.0", "-1.25e+02"},
            {"1e308", "false", "0", "null", "null", "1.0E308", "1e308"},
            {"1E308", "false", "0", "null", "null", "1.0E308", "1E308"},
            {"127", "false", "127", "127", "127", "127.0", "127"},
            {"128", "false", "128", "128", "128", "128.0", "128"},
            {"-128", "false", "-128", "-128", "-128", "-128.0", "-128"},
            {"-129", "false", "-129", "-129", "-129", "-129.0", "-129"},
            {"255", "false", "255", "255", "255", "255.0", "255"},
            {"256", "false", "256", "256", "256", "256.0", "256"},
            {"-256", "false", "-256", "-256", "-256", "-256.0", "-256"},
            {"-257", "false", "-257", "-257", "-257", "-257.0", "-257"},
            {"32767", "false", "32767", "32767", "32767", "32767.0", "32767"},
            {"32768", "false", "0", "32768", "32768", "32768.0", "32768"},
            {"-32768", "false", "-32768", "-32768", "-32768", "-32768.0", "-32768"},
            {"-32769", "false", "0", "-32769", "-32769", "-32769.0", "-32769"},
            {"65535", "false", "0", "65535", "65535", "65535.0", "65535"},
            {"65536", "false", "0", "65536", "65536", "65536.0", "65536"},
            {"-65536", "false", "0", "-65536", "-65536", "-65536.0", "-65536"},
            {"-65537", "false", "0", "-65537", "-65537", "-65537.0", "-65537"},
            {"2147483647", "false", "0", "2147483647", "2147483647", "2.147483647E9", "2147483647"},
            {"2147483648", "false", "0", "null", "2147483648", "2.147483648E9", "2147483648"},
            {"-2147483648", "false", "0", "null", "-2147483648", "-2.147483648E9", "-2147483648"},
            {"-2147483649", "false", "0", "null", "-2147483649", "-2.147483649E9", "-2147483649"},
            {"4294967295", "false", "0", "null", "4294967295", "4.294967295E9", "4294967295"},
            {"4294967296", "false", "0", "null", "4294967296", "4.294967296E9", "4294967296"},
            {"-4294967296", "false", "0", "null", "-4294967296", "-4.294967296E9", "-4294967296"},
            {"-4294967297", "false", "0", "null", "-4294967297", "-4.294967297E9", "-4294967297"},
            {"9223372036854775807", "false", "0", "null", "9223372036854775807", "9.223372036854776E18", "9223372036854775807"},
            {"9223372036854775808", "false", "0", "null", "null", "9.223372036854776E18", "9223372036854775808"},
            {"-9223372036854775808", "false", "0", "null", "null", "-9.223372036854776E18", "-9223372036854775808"},
            {"-9223372036854775809", "false", "0", "null", "null", "-9.223372036854776E18", "-9223372036854775809"},
            {"[]", "false", "0", "null", "null", "null", "[]"},
            {"[true]", "false", "0", "null", "null", "null", "[true]"},
            {"[false]", "false", "0", "null", "null", "null", "[false]"},
            {"[null]", "false", "0", "null", "null", "null", "[null]"},
            {"[1]", "false", "0", "null", "null", "null", "[1]"},
            {"[0]", "false", "0", "null", "null", "null", "[0]"},
            {"[\"true\"]", "false", "0", "null", "null", "null", "[\"true\"]"},
            {"[\"false\"]", "false", "0", "null", "null", "null", "[\"false\"]"},
            {"[1, 2]", "false", "0", "null", "null", "null", "[1, 2]"}
    };

    @Before
    public void setUp() {
        try {
            assertMemoryLeak(() -> ddl("create table json_test as (select '" + castsDoc + "'::varchar text)"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void tearDown() {
        try {
            assertMemoryLeak(() -> drop("drop table json_test"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testBoolean() throws Exception {
        testScenarios(ColumnType.BOOLEAN);
    }

    @Test
    public void testDouble() throws Exception {
        testScenarios(ColumnType.DOUBLE);
    }

    @Test
    public void testInt() throws Exception {
        testScenarios(ColumnType.INT);
    }

    @Test
    public void testLong() throws Exception {
        testScenarios(ColumnType.LONG);
    }

    public void testScenario(int type, int index) throws Exception {
        final int varcharColumn = selectScenarioColumn(ColumnType.VARCHAR);
        final int scenarioColumn = selectScenarioColumn(type);
        final String expectedValue = scenarios[index][scenarioColumn];
        final String expected = "x\n" + expectedValue + ":" + ColumnType.nameOf(type) + "\n";

        if (scenarioColumn < varcharColumn) {
            try {
                final String sql = "select json_extract(text, '[" + index + "]', " + type + ") as x from json_test";
                assertSqlWithTypes(sql, expected);
            } catch (AssertionError e) {
                throw new AssertionError(
                        "Failed JSON 3rd type arg call. Scenario: " + index +
                                ", Cast Type: " + ColumnType.nameOf(type) +
                                ", JSON: " + scenarios[index][0] +
                                ", Expected Value: " + expectedValue +
                                ", Error: " + e.getMessage(), e);
            } catch (CairoException e) {
                throw new RuntimeException(
                        "Failed JSON 3rd type arg call. Scenario: " + index +
                                ", Cast Type: " + ColumnType.nameOf(type) +
                                ", JSON: " + scenarios[index][0] +
                                ", Expected Value: " + expectedValue +
                                ", Error: " + e.getMessage(), e);
            }
        }

        try {
            final String sql = "select json_extract(text, '[" + index + "]')::" + ColumnType.nameOf(type) +
                    " as x from json_test";
            assertSqlWithTypes(sql, expected);
        } catch (AssertionError e) {
            throw new AssertionError(
                    "Failed intrusive cast call. Scenario: " + index +
                            ", Cast Type: " + ColumnType.nameOf(type) +
                            ", JSON: " + scenarios[index][0] +
                            ", Expected Value: " + expectedValue +
                            ", Error: " + e.getMessage(), e);
        } catch (CairoException e) {
            throw new RuntimeException(
                    "Failed intrusive cast call. Scenario: " + index +
                            ", Cast Type: " + ColumnType.nameOf(type) +
                            ", JSON: " + scenarios[index][0] +
                            ", Expected Value: " + expectedValue +
                            ", Error: " + e.getMessage(), e);
        }
    }

    @Test
    public void testShort() throws Exception {
        testScenarios(ColumnType.SHORT);
    }

    @Test
    public void testVarchar() throws Exception {
        testScenarios(ColumnType.VARCHAR);
    }

    private static int selectScenarioColumn(int type) {
        switch (type) {
            case ColumnType.BOOLEAN:
                return 1;
            case ColumnType.SHORT:
                return 2;
            case ColumnType.INT:
                return 3;
            case ColumnType.LONG:
                return 4;
            case ColumnType.DOUBLE:
                return 5;
            case ColumnType.VARCHAR:
                return 6;
            default:
                throw new RuntimeException("No scenario tests for type " + ColumnType.nameOf(type));
        }
    }

    private void testScenarios(int type) throws Exception {
        assertMemoryLeak(() -> {
            for (int index = 0; index < scenarios.length; index++) {
                testScenario(type, index);
            }
        });
    }

    static {
        // Writes out all the scenarios (column 0) into a single JSON array `castsDoc`.
        StringBuilder sb = new StringBuilder();
        sb.append("[\n");
        for (int i = 0; i < scenarios.length; i++) {
            if (i > 0) {
                sb.append(",\n");
            }
            sb.append(scenarios[i][0]);
        }
        sb.append("\n]");
        castsDoc = sb.toString();
    }
}

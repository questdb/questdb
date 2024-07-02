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

import io.questdb.cairo.ColumnType;
import io.questdb.test.AbstractCairoTest;
import org.junit.Before;
import org.junit.Test;

public class JsonPathCastScenariosTest extends AbstractCairoTest {
    private static final String castsDoc;
    private static final String[][] scenarios = new String[][]{
            // json token, ::boolean, ::short, ::long...
            {"null", "false"},
            {"true", "true"},
            {"false", "false"},
            {"1", "false"},
            {"0", "false"},
            {"-1", "false"},
            {"\"true\"", "true"},
            {"\"false\"", "false"},
            {"\"null\"", "false"},
            {"\"1\"", "false"},
            {"\"0\"", "false"},
            {"\"\"", "false"},
            {"\" \"", "false"},
            {"\"  \"", "false"},
            {"\"  true\"", "false"},
            {"\"true  \"", "false"},
            {"\"  true  \"", "false"},
            {"\"  false\"", "false"},
            {"\"false  \"", "false"},
            {"\"  false  \"", "false"},
            {"\"  null\"", "false"},
            {"\"null  \"", "false"},
            {"\"  null  \"", "false"},
            {"\"  abc\"", "false"},
            {"\"abc  \"", "false"},
            {"\"  abc  \"", "false"},
            {"0.0", "false"},
            {"1.0", "false"},
            {"1e1", "false"},
            {"1e+1", "false"},
            {"1e-1", "false"},
            {"1e01", "false"},
            {"1E1", "false"},
            {"1E+1", "false"},
            {"1E-1", "false"},
            {"1E01", "false"},
            {"1E+01", "false"},
            {"0.25", "false"},
            {"1.25", "false"},
            {"1.25e2", "false"},
            {"1.25e+2", "false"},
            {"1.25e-2", "false"},
            {"1.25e02", "false"},
            {"1.25e+02", "false"},
            {"1.25e-02", "false"},
            {"1.25e+02", "false"},
            {"-1.0", "false"},
            {"-0.25", "false"},
            {"-1.25", "false"},
            {"-1.25e2", "false"},
            {"-1.25e+2", "false"},
            {"-1.25e-2", "false"},
            {"-1.25e02", "false"},
            {"-1.25e+02", "false"},
            {"-1.25e-02", "false"},
            {"-1.25e+02", "false"},
            {"1e308", "false"},
            {"1E308", "false"},
            {"127", "false"},
            {"128", "false"},
            {"-128", "false"},
            {"-129", "false"},
            {"255", "false"},
            {"256", "false"},
            {"-256", "false"},
            {"-257", "false"},
            {"32767", "false"},
            {"32768", "false"},
            {"-32768", "false"},
            {"-32769", "false"},
            {"65535", "false"},
            {"65536", "false"},
            {"-65536", "false"},
            {"-65537", "false"},
            {"2147483647", "false"},
            {"2147483648", "false"},
            {"-2147483648", "false"},
            {"-2147483649", "false"},
            {"4294967295", "false"},
            {"4294967296", "false"},
            {"-4294967296", "false"},
            {"-4294967297", "false"},
            {"9223372036854775807", "false"},
            {"9223372036854775808", "false"},
            {"-9223372036854775808", "false"},
            {"-9223372036854775809", "false"},
            {"[]", "false"},
            {"[true]", "false"},
            {"[false]", "false"},
            {"[null]", "false"},
            {"[1]", "false"},
            {"[0]", "false"},
            {"[\"true\"]", "false"},
            {"[\"false\"]", "false"},
            {"[1, 2]", "false"}
    };

    @Before
    public void setUp() {
        try {
            assertMemoryLeak(() -> {
                ddl("create table json_test as (select '" + castsDoc + "'::varchar text)");
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void testScenario(int type, int index) throws Exception {
        final int scenarioColumn = selectScenarioColumn(type);
        final String expectedValue = scenarios[index][scenarioColumn];
        final String expected = "x\n" + expectedValue + ":" + ColumnType.nameOf(type) + "\n";

        try {
            final String viaVarcharCast =
                    "select cast(json_path(text, '[" + index + "]')::varchar as "
                            + ColumnType.nameOf(type) + ") as x from json_test";
            assertSqlWithTypes(viaVarcharCast, expected);
        } catch (AssertionError e) {
            throw new AssertionError(
                    "Failed via-varchar-cast. Scenario: " + index +
                            ", Type: " + ColumnType.nameOf(type) +
                            ", JSON: " + scenarios[index][0] +
                            ", Expected Value: " + expectedValue +
                            ", Error: " + e.getMessage(), e);
        }

        try {
            final String singleCastSql =
                    "select json_path(text, '[" + index + "]')::" + ColumnType.nameOf(type) +
                            " as x from json_test";
            assertSqlWithTypes(singleCastSql, expected);
        } catch (AssertionError e) {
            throw new AssertionError(
                    "Failed single-cast. Scenario: " + index +
                            ", Type: " + ColumnType.nameOf(type) +
                            ", JSON: " + scenarios[index][0] +
                            ", Expected Value: " + expectedValue +
                            ", Error: " + e.getMessage(), e);
        }
    }

    @Test
    public void testScenariosBoolean() throws Exception {
        assertMemoryLeak(() -> {
            for (int i = 0; i < scenarios.length; i++) {
                testScenario(ColumnType.BOOLEAN, i);
            }
        });
    }

    private static int selectScenarioColumn(int type) {
        switch (type) {
            case ColumnType.BOOLEAN:
                return 1;
            default:
                throw new RuntimeException("No scenario tests for type " + ColumnType.nameOf(type));
        }
    }

    static {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int i = 0; i < scenarios.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(scenarios[i][0]);
        }
        sb.append(']');
        castsDoc = sb.toString();
    }
}

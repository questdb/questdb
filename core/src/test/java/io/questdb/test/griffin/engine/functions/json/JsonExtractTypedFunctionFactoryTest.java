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

package io.questdb.test.griffin.engine.functions.json;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class JsonExtractTypedFunctionFactoryTest extends AbstractCairoTest {

    private final String[] brokenJsonScenarios = new String[]{
            "{",
            "[",
            "{]",
            "\"",
            "[1, \"]",
            "[1, 2]"  // valid, but path `[3]` does not exist
    };

    public void testBadJsonExtract(int columnType, String expected) throws Exception {
        final String typeName = ColumnType.nameOf(columnType);
        assertMemoryLeak(() -> {
            for (String json : brokenJsonScenarios) {
                final String sql = "select cast(json_extract('" + json + "', '[3]') as " + typeName + ")" +
                        " as x from long_sequence(1)";
                final String expectedTyped = "x\n" + expected + ":" + typeName + "\n";
                assertSqlWithTypes(expectedTyped, sql);
            }
        });
    }

    @Test
    public void testBadJsonExtract() throws Exception {
        testBadJsonExtract(ColumnType.BOOLEAN, "false");
        testBadJsonExtract(ColumnType.SHORT, "0");
        testBadJsonExtract(ColumnType.INT, "null");
        testBadJsonExtract(ColumnType.LONG, "null");
        testBadJsonExtract(ColumnType.FLOAT, "null");
        testBadJsonExtract(ColumnType.DOUBLE, "null");
        testBadJsonExtract(ColumnType.VARCHAR, "");
        testBadJsonExtract(ColumnType.SYMBOL, "");
        testBadJsonExtract(ColumnType.DATE, "");
        testBadJsonExtract(ColumnType.TIMESTAMP, "");
        testBadJsonExtract(ColumnType.IPv4, "");
    }

    @Test
    public void testColumnAsJsonPath() throws Exception {
        assertMemoryLeak(() -> {
            final String json = "'{\"path\": 0.0000000000000000000000000001}'";
            execute("create table json_test as (select " + json + "::varchar text, '.path' path)");
            assertException("select json_extract(text, path, 6) from json_test", 26, "constant or bind variable expected");
        });
    }

    @Test
    public void testExtractTimestampBadJson() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table json_test (text varchar)");
            execute("insert into json_test values ('{\"path\": [1,2,3]}')");
            execute("insert into json_test values ('{\"\"path\": [1,2]}')");
            execute("insert into json_test values ('{\"path\": []]}')");
            execute("insert into json_test values ('{\"path2\": \"4\"}')");
            execute("insert into json_test values ('{\"path\": \"1on1\"}')");
            assertSqlWithTypes(
                    "x\n" +
                            "1970-01-01T00:00:00.000003Z:TIMESTAMP\n" +
                            ":TIMESTAMP\n" +
                            ":TIMESTAMP\n" +
                            ":TIMESTAMP\n" +
                            ":TIMESTAMP\n",
                    "select json_extract(text, '.path[2]')::timestamp x from json_test order by 1 desc"
            );
        });
    }

    @Test
    public void testExtractTimestampFromDouble() throws SqlException {
        assertSql(
                "json_extract\n" +
                        "1970-01-01T00:00:00.000001Z\n",
                "select json_extract('{\"x\":1.0}', '.x')::timestamp"
        );
    }

    @Test
    public void testExtractTimestampNonExistingPlaces() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table json_test (text varchar)");
            execute("insert into json_test values ('{\"path\": [1,2,3]}')");
            execute("insert into json_test values ('{\"path\": [1,2]}')");
            execute("insert into json_test values ('{\"path\": []]}')");
            execute("insert into json_test values ('{\"path2\": \"4\"}')");
            execute("insert into json_test values ('{\"path\": \"1on1\"}')");
            assertSqlWithTypes(
                    "x\n" +
                            "1970-01-01T00:00:00.000003Z:TIMESTAMP\n" +
                            ":TIMESTAMP\n" +
                            ":TIMESTAMP\n" +
                            ":TIMESTAMP\n" +
                            ":TIMESTAMP\n",
                    "select json_extract(text, '.path[2]')::timestamp x from json_test order by 1 desc"
            );
        });
    }

    @Test
    public void testInvalid3rdArgCall() throws Exception {
        test3rdArgCallInvalid(ColumnType.UUID);
        test3rdArgCallInvalid(ColumnType.VARCHAR);
        test3rdArgCallInvalid(ColumnType.SYMBOL);
        test3rdArgCallInvalid(ColumnType.GEOHASH);
    }

    @Test
    public void testNullPath() throws Exception {
        testNullJson2ndArgCall(ColumnType.BOOLEAN, "false");
        testNullJson2ndArgCall(ColumnType.SHORT, "0");
        testNullJson2ndArgCall(ColumnType.INT, "null");
        testNullJson2ndArgCall(ColumnType.LONG, "null");
        testNullJson2ndArgCall(ColumnType.FLOAT, "null");
        testNullJson2ndArgCall(ColumnType.DOUBLE, "null");
        testNullJson2ndArgCall(ColumnType.DATE, "");
        testNullJson2ndArgCall(ColumnType.TIMESTAMP, "");
        testNullJson2ndArgCall(ColumnType.IPv4, "");
    }

    @Test
    public void testNullTyped() throws Exception {
        testNullJson3rdArgCall(ColumnType.BOOLEAN, "false");
        testNullJsonFunctionCast(ColumnType.BOOLEAN, "false");
        testNullJsonSuffixCast(ColumnType.BOOLEAN, "false");

        testNullJson3rdArgCall(ColumnType.SHORT, "0");
        testNullJsonFunctionCast(ColumnType.SHORT, "0");
        testNullJsonSuffixCast(ColumnType.SHORT, "0");

        testNullJson3rdArgCall(ColumnType.INT, "null");
        testNullJsonFunctionCast(ColumnType.INT, "null");
        testNullJsonSuffixCast(ColumnType.INT, "null");

        testNullJson3rdArgCall(ColumnType.LONG, "null");
        testNullJsonFunctionCast(ColumnType.LONG, "null");
        testNullJsonSuffixCast(ColumnType.LONG, "null");

        testNullJson3rdArgCall(ColumnType.FLOAT, "null", ColumnType.FLOAT);
        testNullJsonFunctionCast(ColumnType.FLOAT, "null", ColumnType.FLOAT);

        // Casting to `float` actually casts to double. This is done for compatibility with PG.
        testNullJsonSuffixCast(ColumnType.FLOAT, "null", ColumnType.DOUBLE);

        testNullJson3rdArgCall(ColumnType.DOUBLE, "null");
        testNullJsonFunctionCast(ColumnType.DOUBLE, "null");
        testNullJsonSuffixCast(ColumnType.DOUBLE, "null");

        testNullJson3rdArgCall(ColumnType.DATE, "");
        testNullJsonFunctionCast(ColumnType.DATE, "");
        testNullJsonSuffixCast(ColumnType.DATE, "");

        testNullJson3rdArgCall(ColumnType.TIMESTAMP, "");
        testNullJsonFunctionCast(ColumnType.TIMESTAMP, "");
        testNullJsonSuffixCast(ColumnType.TIMESTAMP, "");

        testNullJson3rdArgCall(ColumnType.IPv4, "");
        testNullJsonFunctionCast(ColumnType.IPv4, "");
        testNullJsonSuffixCast(ColumnType.IPv4, "");
    }

    private void test3rdArgCallInvalid(int columnType) throws Exception {
        assertMemoryLeak(() -> {
            final String sql = "select json_extract(NULL, '.x', " + columnType + ") as x from long_sequence(1)";
            final SqlException exc = Assert.assertThrows(
                    SqlException.class,
                    () -> execute(sql)
            );
            Assert.assertEquals(7, exc.getPosition());
            TestUtils.assertContains(exc.getMessage(), "please use json_extract(json,path)::type semantic");
        });
    }

    private void testNullJson2ndArgCall(int columnType, String expected) throws Exception {
        final String expectedTypeName = ColumnType.nameOf(columnType);
        assertMemoryLeak(() -> {
            assertSqlWithTypes(
                    "x\n" +
                            expected + ":" + expectedTypeName + "\n",
                    "select json_extract('{}', null, " + columnType + ") as x"
            );

            assertSqlWithTypes(
                    "x\n" +
                            expected + ":" + expectedTypeName + "\n",
                    "select json_extract('{}', '', " + columnType + ") as x"
            );
        });
    }

    private void testNullJson3rdArgCall(int columnType, String expected, int expectedType) throws Exception {
        final String expectedTypeName = ColumnType.nameOf(expectedType);
        assertMemoryLeak(() -> {
            final String sql = "select json_extract(NULL, '.x', " + columnType + ") as x from long_sequence(1)";
            assertSqlWithTypes(
                    "x\n" +
                            expected + ":" + expectedTypeName + "\n",
                    sql
            );
        });
    }

    private void testNullJson3rdArgCall(int columnType, String expected) throws Exception {
        testNullJson3rdArgCall(columnType, expected, columnType);
    }

    private void testNullJsonFunctionCast(int columnType, String expected, int expectedType) throws Exception {
        final String typeName = ColumnType.nameOf(columnType);
        final String expectedTypeName = ColumnType.nameOf(expectedType);
        assertMemoryLeak(() -> {
            final String sql = "select cast(json_extract(NULL, '.x') as " + typeName + ") as x from long_sequence(1)";
            assertSqlWithTypes(
                    "x\n" +
                            expected + ":" + expectedTypeName + "\n",
                    sql
            );
        });
    }

    private void testNullJsonFunctionCast(int columnType, String expected) throws Exception {
        testNullJsonFunctionCast(columnType, expected, columnType);
    }

    private void testNullJsonSuffixCast(int columnType, String expected, int expectedType) throws Exception {
        final String typeName = ColumnType.nameOf(columnType);
        final String expectedTypeName = ColumnType.nameOf(expectedType);
        assertMemoryLeak(() -> {
            final String sql = "select json_extract(NULL, '.x')::" + typeName + " as x from long_sequence(1)";
            assertSqlWithTypes(
                    "x\n" +
                            expected + ":" + expectedTypeName + "\n",
                    sql
            );
        });
    }

    private void testNullJsonSuffixCast(int columnType, String expected) throws Exception {
        testNullJsonSuffixCast(columnType, expected, columnType);
    }
}

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
import org.junit.Test;

public class JsonExtractTypedFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testExtractTimestampBadJson() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table json_test (text varchar)");
            insert("insert into json_test values ('{\"path\": [1,2,3]}')");
            insert("insert into json_test values ('{\"\"path\": [1,2]}')");
            insert("insert into json_test values ('{\"path\": []]}')");
            insert("insert into json_test values ('{\"path2\": \"4\"}')");
            insert("insert into json_test values ('{\"path\": \"1on1\"}')");
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
    public void testExtractTimestampNonExistingPlaces() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table json_test (text varchar)");
            insert("insert into json_test values ('{\"path\": [1,2,3]}')");
            insert("insert into json_test values ('{\"path\": [1,2]}')");
            insert("insert into json_test values ('{\"path\": []]}')");
            insert("insert into json_test values ('{\"path2\": \"4\"}')");
            insert("insert into json_test values ('{\"path\": \"1on1\"}')");
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
        // testNullJsonFunctionCast(ColumnType.FLOAT, "null", ColumnType.DOUBLE);  // TODO: re-enable me and fix me.
        testNullJsonSuffixCast(ColumnType.FLOAT, "null", ColumnType.DOUBLE);

        testNullJson3rdArgCall(ColumnType.DOUBLE, "null");
        testNullJsonFunctionCast(ColumnType.DOUBLE, "null");
        testNullJsonSuffixCast(ColumnType.DOUBLE, "null");
    }

    private void testNullJson3rdArgCall(int columnType, String expected) throws Exception {
        testNullJson3rdArgCall(columnType, expected, columnType);
    }

    private void testNullJson3rdArgCall(int columnType, String expected, int expectedType) throws Exception {
        final String typeName = ColumnType.nameOf(columnType);
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

    private void testNullJsonFunctionCast(int columnType, String expected) throws Exception {
        testNullJsonFunctionCast(columnType, expected, columnType);
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

    private void testNullJsonSuffixCast(int columnType, String expected) throws Exception {
        testNullJsonSuffixCast(columnType, expected, columnType);
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

//    public static class BrokenJsonScenario {
//        public BrokenJsonScenario(String brokenJson, int expectedError) {
//            this.brokenJson = brokenJson;
//            this.expectedError = expectedError;
//        }
//
//        public final String brokenJson;
//        public final int expectedError;
//    }
//
//    public static BrokenJsonScenario bjs(String brokenJson, int expectedError) {
//        return new BrokenJsonScenario(brokenJson, expectedError);
//    }
//
//    public final BrokenJsonScenario brokenJsons[] = new BrokenJsonScenario[] {
//        bjs("{", SimdJsonError.INCOMPLETE_ARRAY_OR_OBJECT),  // incomplete object
//        bjs("[", SimdJsonError.INCOMPLETE_ARRAY_OR_OBJECT),  // incomplete array
//        bjs("{]", SimdJsonError.),  // invalid object
//        bjs("\"", ),  // unterminated string
//        bjs("[1, \"]", ),  // unterminated string2
//    };
//
//    @Test
//    public void testBadJsonExtractVarchar() throws Exception {
//        assertMemoryLeak(() -> {
//            for (String json : brokenJsons) {
//                assertSql("baobab", "select json_extract('" + json + "', '$') as x from long_sequence(1)");
//            }
//        });
//    }
}
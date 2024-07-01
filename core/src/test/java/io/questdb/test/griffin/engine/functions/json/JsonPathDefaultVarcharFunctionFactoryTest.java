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

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class JsonPathDefaultVarcharFunctionFactoryTest extends AbstractCairoTest {
    @Test
    public void test10000() throws Exception {
        assertMemoryLeak(() -> {
            final String json = "'{\"path\": 10000.5}'";
            final String expected = "json_path\n" +
                    "10000.5\n";
            ddl("create table json_test as (select " + json + "::varchar text)");
            assertSql(expected, "select json_path(" + json + ", '.path')");
            assertSql(expected, "select json_path(text, '.path') from json_test"
            );
        });
    }

    @Test
    public void testSort() throws Exception {
        // todo: this test shows that VarcharFunction is broken. getVarcharA() and getVarcharB() under the covers use
        //     the same buffer and corrupt the output
        assertMemoryLeak(() -> {
            ddl("create table json_test (text varchar)");
            insert("insert into json_test values ('{\"path\": 10000.5}')");
            insert("insert into json_test values ('{\"path\": 30000.5}')");
            insert("insert into json_test values ('{\"path\": 20000.5}')");
            insert("insert into json_test values ('{\"path\": 40000.5}')");
            assertSql(
                    "",
                    "select json_path(text, '.path') x from json_test order by 1 desc"
            );
        });
    }

    @Test
    public void testArray() throws Exception {
        assertMemoryLeak(() -> {
            final String json = "'{\"path\": [1, 2, 3]}'";
            final String expected = "json_path\n" +
                    "[1, 2, 3]\n";
            ddl("create table json_test as (select " + json + "::varchar text)");
            assertSql(expected, "select json_path(" + json + ", '.path')");
            assertSql(expected, "select json_path(text, '.path') from json_test"
            );
        });
    }

    @Test
    public void testBigNumber() throws Exception {
        assertMemoryLeak(() -> {
            final String json = "'{\"path\": 100000000000000000000000000}'";
            final String expected = "json_path\n" +
                    "100000000000000000000000000\n";
            ddl("create table json_test as (select " + json + "::varchar text)");
            assertSql(expected, "select json_path(" + json + ", '.path')");
            assertSql(expected, "select json_path(text, '.path') from json_test"
            );
        });
    }

    @Test
    public void testDblPrecision() throws Exception {
        assertMemoryLeak(() -> {
            final String json = "'{\"path\": 0.0000000000000000000000000001}'";
            final String expected = "json_path\n" +
                    "0.0000000000000000000000000001\n";
            ddl("create table json_test as (select " + json + "::varchar text)");
            assertSql(expected, "select json_path(" + json + ", '.path')");
            assertSql(expected, "select json_path(text, '.path') from json_test"
            );
        });
    }

    @Test
    public void testDict() throws Exception {
        assertMemoryLeak(() -> {
            final String json = "'{\"path\": {\"a\": 1, \"b\": 2}}'";
            final String expected = "json_path\n" +
                    "{\"a\": 1, \"b\": 2}\n";
            ddl("create table json_test as (select " + json + "::varchar text)");
            assertSql(expected, "select json_path(" + json + ", '.path')");
            assertSql(expected, "select json_path(text, '.path') from json_test"
            );
        });
    }

    @Test
    public void testEmptyJson() throws Exception {
        assertMemoryLeak(() -> {
            final String json = "'{}'";
            final String expected = "json_path\n" +
                    "\n";
            ddl("create table json_test as (select " + json + "::varchar text)");
            assertSql(expected, "select json_path(" + json + ", '.path')");
            assertSql(expected, "select json_path(text, '.path') from json_test");
        });
    }

    @Test
    public void testInt() throws SqlException {
        assertSql(
                "json_path\n" +
                        "123\n",
                "select json_path('{\"path\": 123}'::varchar, '.path')"
        );
    }

    @Test
    public void testNegative() throws Exception {
        assertMemoryLeak(() -> {
            final String json = "'{\"path\": -123.5}'";
            final String expected = "json_path\n" +
                    "-123.5\n";
            ddl("create table json_test as (select " + json + "::varchar text)");
            assertSql(expected, "select json_path(" + json + ", '.path')");
            assertSql(expected, "select json_path(text, '.path') from json_test");
        });
    }

    @Test
    public void testNullJson() throws Exception {
        assertMemoryLeak(() -> {
            final String json = "null";
            final String expected = "json_path\n" +
                    "\n";
            ddl("create table json_test as (select " + json + "::varchar text)");
            assertSql(expected, "select json_path(" + json + ", '.path')");
            assertSql(expected, "select json_path(text, '.path') from json_test");
        });
    }

    @Test
    public void testNullJsonValue() throws Exception {
        assertMemoryLeak(() -> {
            final String json = "'{\"path\": null}'";
            final String expected = "json_path\n" +
                    "\n";
            ddl("create table json_test as (select " + json + "::varchar text)");
            assertSql(expected, "select json_path(" + json + ", '.path')");
            assertSql(expected, "select json_path(text, '.path') from json_test"
            );
        });
    }

    @Test
    public void testOne() throws Exception {
        assertMemoryLeak(() -> {
            final String json = "'{\"path\": 1}'";
            final String expected = "json_path\n" +
                    "1\n";
            ddl("create table json_test as (select " + json + "::varchar text)");
            assertSql(expected, "select json_path(" + json + ", '.path')");
            assertSql(expected, "select json_path(text, '.path') from json_test"
            );
        });
    }

    @Test
    public void testString() throws Exception {
        assertMemoryLeak(() -> {
            final String json = "'{\"path\": \"abc\"}'";
            final String expected = "json_path\n" +
                    "abc\n";
            ddl("create table json_test as (select " + json + "::varchar text)");
            assertSql(expected, "select json_path(" + json + ", '.path')");
            assertSql(expected, "select json_path(text, '.path') from json_test"
            );
        });
    }

    @Test
    public void testUnsigned64Bit() throws Exception {
        assertMemoryLeak(() -> {
            final String json = "'{\"path\": 9999999999999999999}'";
            final String expected = "json_path\n" +
                    "9999999999999999999\n";
            ddl("create table json_test as (select " + json + "::varchar text)");
            assertSql(expected, "select json_path(" + json + ", '.path')");
            assertSql(expected, "select json_path(text, '.path') from json_test"
            );
        });
    }
}

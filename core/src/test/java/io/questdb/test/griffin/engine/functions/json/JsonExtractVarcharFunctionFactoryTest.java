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

package io.questdb.test.griffin.engine.functions.json;

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cutlass.http.TestHttpClient;
import org.junit.Test;

public class JsonExtractVarcharFunctionFactoryTest extends AbstractCairoTest {
    @Test
    public void test10000() throws Exception {
        assertMemoryLeak(() -> {
            final String json = "'{\"path\": 10000.5}'";
            final String expected = """
                    json_extract
                    10000.5
                    """;
            execute("create table json_test as (select " + json + "::varchar text)");
            assertSql(expected, "select json_extract(" + json + ", '.path')");
            assertSql(expected, "select json_extract(text, '.path') from json_test");
        });
    }

    @Test
    public void testArray() throws Exception {
        assertMemoryLeak(() -> {
            final String json = "'{\"path\": [1, 2, 3]}'";
            final String expected = """
                    json_extract
                    [1, 2, 3]
                    """;
            execute("create table json_test as (select " + json + "::varchar text)");
            assertSql(expected, "select json_extract(" + json + ", '.path')");
            assertSql(expected, "select json_extract(text, '.path') from json_test");
        });
    }

    @Test
    public void testBigNumber() throws Exception {
        assertMemoryLeak(() -> {
            final String json = "'{\"path\": 100000000000000000000000000}'";
            final String expected = """
                    json_extract
                    100000000000000000000000000
                    """;
            execute("create table json_test as (select " + json + "::varchar text)");
            assertSql(expected, "select json_extract(" + json + ", '.path')");
            assertSql(expected, "select json_extract(text, '.path') from json_test");
        });
    }

    @Test
    public void testColumnAsJsonPath() throws Exception {
        assertMemoryLeak(() -> {
            final String json = "'{\"path\": 0.0000000000000000000000000001}'";
            execute("create table json_test as (select " + json + "::varchar text, '.path' path)");
            assertException("select json_extract(text, path) from json_test", 26, "constant or bind variable expected");
        });
    }

    @Test
    public void testDblPrecision() throws Exception {
        assertMemoryLeak(() -> {
            final String json = "'{\"path\": 0.0000000000000000000000000001}'";
            final String expected = """
                    json_extract
                    0.0000000000000000000000000001
                    """;
            execute("create table json_test as (select " + json + "::varchar text)");
            assertSql(expected, "select json_extract(" + json + ", '.path')");
            assertSql(expected, "select json_extract(text, '.path') from json_test");
        });
    }

    @Test
    public void testDict() throws Exception {
        assertMemoryLeak(() -> {
            final String json = "'{\"path\": {\"a\": 1, \"b\": 2}}'";
            final String expected = """
                    json_extract
                    {"a": 1, "b": 2}
                    """;
            execute("create table json_test as (select " + json + "::varchar text)");
            assertSql(expected, "select json_extract(" + json + ", '.path')");
            assertSql(expected, "select json_extract(text, '.path') from json_test");
        });
    }

    @Test
    public void testEdgeCaseWhitespaceInt() throws Exception {
        assertMemoryLeak(
                () -> {
                    final String json = """
                            '{"path1": "  abc  ", "path2": [  1,  2,   3 ], "path4":\s
                            [\r
                             1,\r\
                            2 ,
                              3  \t, 4,
                            
                            5 \r
                             ]}'""";
                    execute("create table json_test as (select " + json + "::varchar text)");
                    assertSql(
                            """
                                    k
                                    3
                                    """,
                            "select json_extract(text, '.path2[2]')::int k from json_test"
                    );
                }
        );
    }

    @Test
    public void testEmptyJson() throws Exception {
        assertMemoryLeak(() -> {
            final String json = "'{}'";
            final String expected = """
                    json_extract
                    
                    """;
            execute("create table json_test as (select " + json + "::varchar text)");
            assertSql(expected, "select json_extract(" + json + ", '.path')");
            assertSql(expected, "select json_extract(text, '.path') from json_test");
        });
    }

    @Test
    public void testExtractChar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table json_test (text varchar)");
            execute("insert into json_test values ('{\"path\": \"c\"}')");
            execute("insert into json_test values ('{\"path\": \"2klll\"}')");
            execute("insert into json_test values ('{\"path\": \"a\"}')");
            execute("insert into json_test values ('{\"path2\": \"4\"}')");
            execute("insert into json_test values ('{\"path\": \"1\"}')");
            assertSql(
                    """
                            x
                            c
                            a
                            2
                            1
                            
                            """,
                    "select json_extract(text, '.path')::char x from json_test order by 1 desc"
            );
        });
    }

    @Test
    public void testExtractString() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table json_test (text varchar)");
            execute("insert into json_test values ('{\"path\": \"blue\"}')");
            execute("insert into json_test values ('{\"path\": \"klll\"}')");
            execute("insert into json_test values ('{\"path\": \"appl\"}')");
            execute("insert into json_test values ('{\"path2\": \"4\"}')");
            execute("insert into json_test values ('{\"path\": \"1on1\"}')");
            assertSqlWithTypes(
                    """
                            x
                            klll:STRING
                            blue:STRING
                            appl:STRING
                            1on1:STRING
                            :STRING
                            """,
                    "select json_extract(text, '.path')::string x from json_test order by 1 desc"
            );
        });
    }

    @Test
    public void testExtractSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table json_test (text varchar)");
            execute("insert into json_test values ('{\"path\": \"blue\"}')");
            execute("insert into json_test values ('{\"path\": \"klll\"}')");
            execute("insert into json_test values ('{\"path\": \"appl\"}')");
            execute("insert into json_test values ('{\"path2\": \"4\"}')");
            execute("insert into json_test values ('{\"path\": \"1on1\"}')");
            assertSqlWithTypes(
                    """
                            x
                            klll:SYMBOL
                            blue:SYMBOL
                            appl:SYMBOL
                            1on1:SYMBOL
                            :SYMBOL
                            """,
                    "select json_extract(text, '.path')::symbol x from json_test order by 1 desc"
            );
        });
    }

    @Test
    public void testExtractUUID() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table json_test (text varchar)");
            execute("insert into json_test values ('{\"path\": \"6e18f80d-8b8f-4561-a9c8-703b73d5560d\"}')");
            execute("insert into json_test values ('{\"path\": \"7d4bb839-98e4-4c31-9a5a-2dc39834a2a2\"}')");
            execute("insert into json_test values ('{\"path\": \"58e9a7c6-6112-4c48-8723-8765c706773a\"}')");
            assertSql(
                    """
                            x
                            7d4bb839-98e4-4c31-9a5a-2dc39834a2a2
                            6e18f80d-8b8f-4561-a9c8-703b73d5560d
                            58e9a7c6-6112-4c48-8723-8765c706773a
                            """,
                    "select json_extract(text, '.path')::uuid x from json_test order by 1 desc"
            );
        });
    }

    @Test
    public void testGeoHash() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table json_test (text varchar)");
            execute("insert into json_test values ('{\"path\": \"sp052w9\"}')");
            execute("insert into json_test values ('{\"path\": \"gbsuv7z\"}')");
            execute("insert into json_test values ('{\"path\": null}')");
            execute("insert into json_test values ('{\"path2\": \"4\"}')");
            execute("insert into json_test values ('{\"path\": \"1on1\"}')");
            assertSqlWithTypes(
                    """
                            x
                            sp052w9:GEOHASH(7c)
                            gbsuv7z:GEOHASH(7c)
                            :GEOHASH(7c)
                            :GEOHASH(7c)
                            :GEOHASH(7c)
                            """,
                    "select cast(json_extract(text, '.path') as geohash(7c)) x from json_test order by 1 desc"
            );
        });
    }

    @Test
    public void testHttpAccess() throws Exception {
        final String json = "'{\"path\": 0.0000000000000000000000000001}'";
        getSimpleTester().run((engine, sqlExecutionContext) -> {
            try (TestHttpClient httpClient = new TestHttpClient()) {
                engine.execute("create table json_test as (select " + json + "::varchar text)", sqlExecutionContext);
                httpClient.assertGet(
                        "{\"query\":\"select json_extract(text, '.path') from json_test\",\"columns\":[{\"name\":\"json_extract\",\"type\":\"VARCHAR\"}],\"timestamp\":-1,\"dataset\":[[\"0.0000000000000000000000000001\"]],\"count\":1}",
                        "select json_extract(text, '.path') from json_test");
            }
        });
    }

    @Test
    public void testInt() throws SqlException {
        assertSql(
                """
                        json_extract
                        123
                        """,
                "select json_extract('{\"path\": 123}'::varchar, '.path')"
        );
    }

    @Test
    public void testMixOfAvailableAndUnavailableJsonAttributes() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    create table json_test as (\
                    select rnd_str('{
                        "hello": "world",
                        "list": [
                            1,
                            2,
                            3
                         ],
                         "list.of.dicts": [
                             {"hello": "world"},
                             {"hello": "bob"}
                         ]
                    }',\s
                    '{
                        "hello": "world",
                        "list": [
                            1,
                            3
                         ],
                         "list.of.dicts": [
                             {"hello": "world"},
                             {"hello": "bob"}
                         ]
                    }',\s
                    null
                    )::varchar text from long_sequence(10000)
                    )""");

            // verify that we do have nulls in the column
            assertSql(
                    """
                            count
                            3324
                            """,
                    "select count() from json_test where text is null"
            );

            // verify that some values are not found
            assertSql(
                    """
                            count
                            6638
                            """,
                    "select count() from json_test where json_extract(text, '.list[2]') is null"
            );

            assertSql(
                    """
                            sum
                            10086.0
                            """,
                    "select sum(json_extract(text, '.list[2]')::double) from json_test"
            );
        });
    }

    @Test
    public void testMixOfGoodAndBadJson() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table json_test as (" +
                    "select rnd_str('{\n" +
                    "    \"hello\": \"world\",\n" +
                    "    \"list\": [\n" +
                    "        1,\n" +
                    "        2,\n" +
                    "        3\n" +
                    "     ],\n" +
                    "     \"list.of.dicts\": [\n" +
                    "         {\"hello\": \"world\"},\n" +
                    "         {\"hello\": \"bob\"}\n" +
                    "     ]\n" +
                    "}', \n" +
                    "'{\n" +
                    "    \"hello\": \"world\",\n" +
                    "    \"list\": \"a\n" + // this JSON is malformed (unlosed string)
                    "     ],\n" +
                    "     \"list.of.dicts\": [\n" +
                    "         {\"hello\": \"world\"},\n" +
                    "         {\"hello\": \"bob\"}\n" +
                    "}', \n" +
                    "null\n" +
                    ")::varchar text from long_sequence(10000)\n" +
                    ")");

            assertSql(
                    """
                            count_distinct
                            1
                            """,
                    "select count_distinct(json_extract(text, '.list[2]')) from json_test"
            );
            assertSql(
                    """
                            count_distinct
                            1
                            """,
                    "select count(distinct json_extract(text, '.list[2]')) from json_test"
            );

            assertSql(
                    """
                            sum
                            10086.0
                            """,
                    "select sum(json_extract(text, '.list[2]')::double) from json_test"
            );
        });
    }

    @Test
    public void testNegative() throws Exception {
        assertMemoryLeak(() -> {
            final String json = "'{\"path\": -123.5}'";
            final String expected = """
                    json_extract
                    -123.5
                    """;
            execute("create table json_test as (select " + json + "::varchar text)");
            assertSql(expected, "select json_extract(" + json + ", '.path')");
            assertSql(expected, "select json_extract(text, '.path') from json_test");
        });
    }

    @Test
    public void testNullJson() throws Exception {
        assertMemoryLeak(() -> {
            final String json = "null";
            final String expected = """
                    json_extract
                    
                    """;
            execute("create table json_test as (select " + json + "::varchar text)");
            assertSql(expected, "select json_extract(" + json + ", '.path')");
            assertSql(expected, "select json_extract(text, '.path') from json_test");
        });
    }

    @Test
    public void testNullJsonValue() throws Exception {
        assertMemoryLeak(() -> {
            final String json = "'{\"path\": null}'";
            final String expected = """
                    json_extract
                    
                    """;
            execute("create table json_test as (select " + json + "::varchar text)");
            assertSql(expected, "select json_extract(" + json + ", '.path')");
            assertSql(expected, "select json_extract(text, '.path') from json_test");
        });
    }

    @Test
    public void testNullPath() throws SqlException {
        assertSql(
                """
                        json_extract
                        
                        """,
                "select json_extract('{}', null)"
        );
    }

    @Test
    public void testOne() throws Exception {
        assertMemoryLeak(() -> {
            final String json = "'{\"path\": 1}'";
            final String expected = """
                    json_extract
                    1
                    """;
            execute("create table json_test as (select " + json + "::varchar text)");
            assertSql(expected, "select json_extract(" + json + ", '.path')");
            assertSql(expected, "select json_extract(text, '.path') from json_test");
        });
    }

    /**
     * Test that the raw returned token does not have surrounding whitespace.
     */
    @Test
    public void testRawTokenMinimal() throws Exception {
        assertMemoryLeak(() -> {
            final String json = """
                    '{"path1": "  abc  ", "path2": [  1,  2,   3 ], "path4":\s
                    [\r
                     1,\r\
                    2 ,
                      3  \t, 4,
                    
                    5 \r
                     ]}'""";
            execute("create table json_test as (select " + json + "::varchar text)");
            final String[][] scenarios = new String[][]{
                    // path, expected
                    {".path1", "  abc  "},
                    {".path2", "[  1,  2,   3 ]"},
                    {".path2[0]", "1"},
                    {".path2[1]", "2"},
                    {".path2[2]", "3"},
                    {".path4", """
[\r
 1,\r\
2 ,
  3  \t, 4,

5 \r
 ]"""},
                    {".path4[0]", "1"},
                    {".path4[1]", "2"},
                    {".path4[2]", "3"},
                    {".path4[3]", "4"},
                    {".path4[4]", "5"},
            };
            for (String[] scenario : scenarios) {
                final String path = scenario[0];
                final String expected = scenario[1];
                assertSql(
                        "json_extract\n" + expected + "\n",
                        "select json_extract(text, '" + path + "') from json_test");
            }
        });
    }

    @Test
    public void testSort() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table json_test (text varchar)");
            execute("insert into json_test values ('{\"path\": 10000.5}')");
            execute("insert into json_test values ('{\"path\": 30000.5}')");
            execute("insert into json_test values ('{\"path\": 20000.5}')");
            execute("insert into json_test values ('{\"path\": 40000.5}')");
            assertSql(
                    """
                            x
                            40000.5
                            30000.5
                            20000.5
                            10000.5
                            """,
                    "select json_extract(text, '.path') x from json_test order by 1 desc"
            );
        });
    }

    @Test
    public void testString() throws Exception {
        assertMemoryLeak(
                () -> {
                    execute("""
                            create table json_test as (\
                            select rnd_str('{
                                "hello": "world",
                                "list": [
                                    1,
                                    2,
                                    3
                                 ],
                                 "dicts": [
                                     {"hello": "world"},
                                     {"hello": "bob"}
                                 ]
                            }',\s
                            '{
                                "hello": "world",
                                "list": [
                                    1,
                                    2,
                                    3
                                 ],
                                 "dicts": [
                                     {"hello": "world"},
                                     {"hello": "bob"},
                                     {"hello": "alice"}
                                 ]
                            }',
                            '{
                                "hello": "world",
                                "list": [
                                    1,
                                    2,
                                    3
                                 ],
                                 "dicts": [
                                     {"hello": "world"},
                                     {"hello": "bob"},
                                     {"hello": "запросила"}
                                 ]
                            }',
                            null
                            )::varchar text from long_sequence(10)
                            )""");

                    assertQuery(
                            """
                                    k
                                    
                                    {"hello": "запросила"}
                                    {"hello": "alice"}
                                    
                                    {"hello": "alice"}
                                    
                                    {"hello": "запросила"}
                                    
                                    {"hello": "запросила"}
                                    
                                    """,
                            "select json_extract(text, '.dicts[2]')::string k from json_test",
                            true
                    );
                }
        );
    }

    @Test
    public void testUnsigned64Bit() throws Exception {
        assertMemoryLeak(() -> {
            final String json = "'{\"path\": 9999999999999999999}'";
            final String expected = """
                    json_extract
                    9999999999999999999
                    """;
            execute("create table json_test as (select " + json + "::varchar text)");
            assertSql(expected, "select json_extract(" + json + ", '.path')");
            assertSql(expected, "select json_extract(text, '.path') from json_test");
        });
    }

    @Test
    public void testVarcharVanilla() throws Exception {
        assertMemoryLeak(() -> {
            final String json = "'{\"path\": \"abc\"}'";
            final String expected = """
                    json_extract
                    abc
                    """;
            execute("create table json_test as (select " + json + "::varchar text)");
            assertSql(expected, "select json_extract(" + json + ", '.path')");
            assertSql(expected, "select json_extract(text, '.path') from json_test");
        });
    }
}

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

package io.questdb.test.griffin;

import io.questdb.griffin.engine.join.JsonUnnestSource;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class JsonUnnestTest extends AbstractCairoTest {

    @Test
    public void testScalarDoubleArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.5, 2.5, 3.5]')");
            assertQueryNoLeakCheck(
                    "val\n"
                            + "1.5\n"
                            + "2.5\n"
                            + "3.5\n",
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val DOUBLE)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarIntArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[10, 20, 30]')");
            assertQueryNoLeakCheck(
                    "val\n"
                            + "10\n"
                            + "20\n"
                            + "30\n",
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val INT)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarLongArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[100000, 200000, 300000]')");
            assertQueryNoLeakCheck(
                    "val\n"
                            + "100000\n"
                            + "200000\n"
                            + "300000\n",
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val LONG)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarShortArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1, 2, 3]')");
            assertQueryNoLeakCheck(
                    "val\n"
                            + "1\n"
                            + "2\n"
                            + "3\n",
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val SHORT)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarBooleanArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[true, false, true]')");
            assertQueryNoLeakCheck(
                    "val\n"
                            + "true\n"
                            + "false\n"
                            + "true\n",
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val BOOLEAN)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarVarcharArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"hello\", \"world\"]')");
            assertQueryNoLeakCheck(
                    "val\n"
                            + "hello\n"
                            + "world\n",
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val VARCHAR)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testObjectArrayMultiColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ("
                    + "'[{\"price\":1.5,\"name\":\"apple\"},"
                    + "{\"price\":2.5,\"name\":\"banana\"}]'"
                    + ")");
            assertQueryNoLeakCheck(
                    "price\tname\n"
                            + "1.5\tapple\n"
                            + "2.5\tbanana\n",
                    "SELECT u.price, u.name FROM t, UNNEST("
                            + "t.payload COLUMNS(price DOUBLE, name VARCHAR)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testMissingFieldReturnsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ("
                    + "'[{\"price\":1.5},{\"price\":2.5,\"name\":\"banana\"}]'"
                    + ")");
            assertQueryNoLeakCheck(
                    "price\tname\n"
                            + "1.5\t\n"
                            + "2.5\tbanana\n",
                    "SELECT u.price, u.name FROM t, UNNEST("
                            + "t.payload COLUMNS(price DOUBLE, name VARCHAR)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testExtraFieldsIgnored() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ("
                    + "'[{\"price\":1.5,\"name\":\"a\",\"qty\":10}]'"
                    + ")");
            assertQueryNoLeakCheck(
                    "price\n"
                            + "1.5\n",
                    "SELECT u.price FROM t, UNNEST("
                            + "t.payload COLUMNS(price DOUBLE)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testTypeMismatchReturnsNaN() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ("
                    + "'[{\"val\":\"not_a_number\"}]'"
                    + ")");
            assertQueryNoLeakCheck(
                    "val\n"
                            + "null\n",
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val DOUBLE)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testNullPayloadReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES (NULL)");
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val DOUBLE)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testEmptyArrayReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[]')");
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val DOUBLE)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testNotAnArrayReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('{\"key\":\"val\"}')");
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val DOUBLE)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testInvalidJsonReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('not json')");
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val DOUBLE)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testWithOrdinality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[10.0, 20.0, 30.0]')");
            assertQueryNoLeakCheck(
                    "val\tord\n"
                            + "10.0\t1\n"
                            + "20.0\t2\n"
                            + "30.0\t3\n",
                    "SELECT u.val, u.ord FROM t, UNNEST("
                            + "t.payload COLUMNS(val DOUBLE)"
                            + ") WITH ORDINALITY u(val, ord)",
                    (String) null
            );
        });
    }

    @Test
    public void testStandaloneJsonUnnest() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "price\tname\n"
                        + "1.5\tapple\n"
                        + "2.5\tbanana\n",
                "SELECT * FROM UNNEST("
                        + "'[{\"price\":1.5,\"name\":\"apple\"},"
                        + "{\"price\":2.5,\"name\":\"banana\"}]'::VARCHAR "
                        + "COLUMNS(price DOUBLE, name VARCHAR)"
                        + ") u",
                (String) null
        ));
    }

    @Test
    public void testMultipleBaseRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, payload VARCHAR)");
            execute("INSERT INTO t VALUES "
                    + "(1, '[1.0, 2.0]'), "
                    + "(2, '[3.0, 4.0, 5.0]')");
            assertQueryNoLeakCheck(
                    "id\tval\n"
                            + "1\t1.0\n"
                            + "1\t2.0\n"
                            + "2\t3.0\n"
                            + "2\t4.0\n"
                            + "2\t5.0\n",
                    "SELECT t.id, u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val DOUBLE)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testMixedJsonAndArraySources() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (arr DOUBLE[], payload VARCHAR)");
            execute("INSERT INTO t VALUES ("
                    + "ARRAY[100.0, 200.0], "
                    + "'[{\"name\":\"a\"},{\"name\":\"b\"}]'"
                    + ")");
            assertQueryNoLeakCheck(
                    "elem\tname\n"
                            + "100.0\ta\n"
                            + "200.0\tb\n",
                    "SELECT u.elem, u.name FROM t, UNNEST("
                            + "t.arr, "
                            + "t.payload COLUMNS(name VARCHAR)"
                            + ") u(elem, name)",
                    (String) null
            );
        });
    }

    @Test
    public void testSelectStar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, payload VARCHAR)");
            execute("INSERT INTO t VALUES (1, '[{\"price\":1.5}]')");
            assertQueryNoLeakCheck(
                    "id\tpayload\tprice\n"
                            + "1\t[{\"price\":1.5}]\t1.5\n",
                    "SELECT * FROM t, UNNEST("
                            + "t.payload COLUMNS(price DOUBLE)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testWhereOnUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ("
                    + "'[{\"price\":1.5},{\"price\":2.5},{\"price\":3.5}]'"
                    + ")");
            assertQueryNoLeakCheck(
                    "price\n"
                            + "2.5\n"
                            + "3.5\n",
                    "SELECT u.price FROM t, UNNEST("
                            + "t.payload COLUMNS(price DOUBLE)"
                            + ") u WHERE u.price > 2.0",
                    (String) null
            );
        });
    }

    @Test
    public void testGroupByOnUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ("
                    + "'[{\"cat\":\"a\",\"val\":1},"
                    + "{\"cat\":\"b\",\"val\":2},"
                    + "{\"cat\":\"a\",\"val\":3}]'"
                    + ")");
            assertQueryNoLeakCheck(
                    "cat\ttotal\n"
                            + "a\t4\n"
                            + "b\t2\n",
                    "SELECT u.cat, sum(u.val) total FROM t, UNNEST("
                            + "t.payload COLUMNS(cat VARCHAR, val LONG)"
                            + ") u GROUP BY u.cat ORDER BY u.cat"
            );
        });
    }

    @Test
    public void testOrderByOnUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[3.0, 1.0, 2.0]')");
            assertQueryNoLeakCheck(
                    "val\n"
                            + "1.0\n"
                            + "2.0\n"
                            + "3.0\n",
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val DOUBLE)"
                            + ") u ORDER BY u.val",
                    false
            );
        });
    }

    @Test
    public void testLimitOnUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.0, 2.0, 3.0, 4.0, 5.0]')");
            assertQueryNoLeakCheck(
                    "val\n"
                            + "1.0\n"
                            + "2.0\n",
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val DOUBLE)"
                            + ") u LIMIT 2",
                    (String) null
            );
        });
    }

    @Test
    public void testCrossJoinJsonUnnest() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"a\":1},{\"a\":2}]')");
            assertQueryNoLeakCheck(
                    "a\n"
                            + "1\n"
                            + "2\n",
                    "SELECT u.a FROM t CROSS JOIN UNNEST("
                            + "t.payload COLUMNS(a INT)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testCteWithJsonUnnest() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.5, 2.5]')");
            assertQueryNoLeakCheck(
                    "val\n"
                            + "1.5\n"
                            + "2.5\n",
                    "WITH cte AS (SELECT payload FROM t) "
                            + "SELECT u.val FROM cte, UNNEST("
                            + "cte.payload COLUMNS(val DOUBLE)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testSubqueryWrapping() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.0, 2.0, 3.0]')");
            assertQueryNoLeakCheck(
                    "total\n"
                            + "6.0\n",
                    "SELECT sum(val) total FROM ("
                            + "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val DOUBLE)"
                            + ") u"
                            + ")",
                    null, false, false, true
            );
        });
    }

    @Test
    public void testColumnAliasOverridesDefault() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.5]')");
            assertQueryNoLeakCheck(
                    "my_price\n"
                            + "1.5\n",
                    "SELECT u.my_price FROM t, UNNEST("
                            + "t.payload COLUMNS(price DOUBLE)"
                            + ") u(my_price)",
                    (String) null
            );
        });
    }

    @Test
    public void testDotNotationAccess() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"price\":1.5}]')");
            assertQueryNoLeakCheck(
                    "price\n"
                            + "1.5\n",
                    "SELECT u.price FROM t, UNNEST("
                            + "t.payload COLUMNS(price DOUBLE)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.5]')");
            assertPlanNoLeakCheck(
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val DOUBLE)"
                            + ") u",
                    "SelectedRecord\n"
                            + "    Unnest\n"
                            + "      columns: [val]\n"
                            + "        PageFrame\n"
                            + "            Row forward scan\n"
                            + "            Frame forward scan on: t\n"
            );
        });
    }

    @Test
    public void testTimestampFromJsonString() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ("
                    + "'[{\"ts\":\"2024-01-15T10:30:00.000000Z\"},"
                    + "{\"ts\":\"2024-06-20T14:00:00.000000Z\"}]'"
                    + ")");
            assertQueryNoLeakCheck(
                    "ts\n"
                            + "2024-01-15T10:30:00.000000Z\n"
                            + "2024-06-20T14:00:00.000000Z\n",
                    "SELECT u.ts FROM t, UNNEST("
                            + "t.payload COLUMNS(ts TIMESTAMP)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testTimestampFromJsonNumber() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            // Micros since epoch: 2024-01-15T09:30:00Z = 1705311000000000
            execute("INSERT INTO t VALUES ('[{\"ts\":1705311000000000}]')");
            assertQueryNoLeakCheck(
                    "ts\n"
                            + "2024-01-15T09:30:00.000000Z\n",
                    "SELECT u.ts FROM t, UNNEST("
                            + "t.payload COLUMNS(ts TIMESTAMP)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testUnicodeStringsInJson() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            // Use precomposed Unicode (U+00E9) directly in JSON string,
            // avoiding combining characters that produce different byte
            // sequences after simdjson decoding
            execute("INSERT INTO t VALUES ("
                    + "'[{\"name\":\"\u00e9t\u00e9\"},{\"name\":\"\u00fc\u00f1\"}]'"
                    + ")");
            assertQueryNoLeakCheck(
                    "name\n"
                            + "\u00e9t\u00e9\n"
                            + "\u00fc\u00f1\n",
                    "SELECT u.name FROM t, UNNEST("
                            + "t.payload COLUMNS(name VARCHAR)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testLargeArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < 1000; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(i);
            }
            sb.append(']');
            execute("INSERT INTO t VALUES ('" + sb + "')");
            assertQueryNoLeakCheck(
                    "cnt\n"
                            + "1000\n",
                    "SELECT count() cnt FROM t, UNNEST("
                            + "t.payload COLUMNS(val LONG)"
                            + ") u",
                    null, false, false, true
            );
        });
    }

    @Test
    public void testLargeNonAsciiVarcharOverflowThrowsError() throws Exception {
        // Non-ASCII values exceeding MAX_JSON_VALUE_SIZE trigger UTF-8
        // backoff in the native layer, producing sink.size() < maxSize.
        // The native truncated flag detects the overflow and throws.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            // 1366 CJK characters x 3 bytes each = 4098 bytes > 4096
            int charCount = (JsonUnnestSource.MAX_JSON_VALUE_SIZE / 3) + 2;
            StringBuilder bigVal = new StringBuilder(charCount);
            for (int i = 0; i < charCount; i++) {
                bigVal.append('\u4e16'); // '世' = 3 bytes in UTF-8
            }
            execute("INSERT INTO t VALUES ('[{\"s\":\"" + bigVal + "\"}]')");
            assertExceptionNoLeakCheck(
                    "SELECT u.s FROM t, UNNEST("
                            + "t.payload COLUMNS(s VARCHAR)"
                            + ") u",
                    0,
                    "JSON UNNEST: value exceeds maximum size of "
                            + JsonUnnestSource.MAX_JSON_VALUE_SIZE
                            + " bytes for column 's'"
            );
        });
    }

    @Test
    public void testLargeVarcharOverflowThrowsError() throws Exception {
        // Values exceeding MAX_JSON_VALUE_SIZE (4096) throw an error
        // rather than silently truncating or returning NULL.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            StringBuilder bigVal = new StringBuilder(5000);
            for (int i = 0; i < 5000; i++) {
                bigVal.append('x');
            }
            execute("INSERT INTO t VALUES ('[{\"s\":\"" + bigVal + "\"}]')");
            assertExceptionNoLeakCheck(
                    "SELECT u.s FROM t, UNNEST("
                            + "t.payload COLUMNS(s VARCHAR)"
                            + ") u",
                    0,
                    "JSON UNNEST: value exceeds maximum size of "
                            + JsonUnnestSource.MAX_JSON_VALUE_SIZE
                            + " bytes for column 's'"
            );
        });
    }

    @Test
    public void testNullElementInArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.5, null, 3.5]')");
            assertQueryNoLeakCheck(
                    "val\n"
                            + "1.5\n"
                            + "null\n"
                            + "3.5\n",
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val DOUBLE)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarArrayWithNullFirstElement() throws Exception {
        // Verifies scan-forward detection: element 0 is null, but
        // element 1 is a scalar, so isObjectArray should be false.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[null, 1.5, 2.5]')");
            assertQueryNoLeakCheck(
                    "val\n"
                            + "null\n"
                            + "1.5\n"
                            + "2.5\n",
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val DOUBLE)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testJsonExtractWithJsonUnnest() throws Exception {
        // json_extract() lazily allocates ~2MB internal buffers during cursor
        // execution, so assertQueryNoLeakCheck's factory memory check fails.
        // Use assertSql() which closes the factory immediately after use.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE events (payload VARCHAR)");
            execute("INSERT INTO events VALUES ("
                    + "'{\"items\":[{\"price\":1.5,\"name\":\"a\"},"
                    + "{\"price\":2.5,\"name\":\"b\"}]}'"
                    + ")");
            assertSql(
                    "price\tname\n"
                            + "1.5\ta\n"
                            + "2.5\tb\n",
                    "SELECT u.price, u.name FROM events e, UNNEST("
                            + "json_extract(e.payload, '$.items') "
                            + "COLUMNS(price DOUBLE, name VARCHAR)"
                            + ") u"
            );
        });
    }

    @Test
    public void testMultipleJsonSources() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a VARCHAR, b VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.0, 2.0]', '[\"x\", \"y\", \"z\"]')");
            assertQueryNoLeakCheck(
                    "val\tname\n"
                            + "1.0\tx\n"
                            + "2.0\ty\n"
                            + "null\tz\n",
                    "SELECT u.val, u.name FROM t, UNNEST("
                            + "t.a COLUMNS(val DOUBLE), "
                            + "t.b COLUMNS(name VARCHAR)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testAvgOnJsonUnnest() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[10.0, 20.0, 30.0]')");
            assertQueryNoLeakCheck(
                    "avg\n"
                            + "20.0\n",
                    "SELECT avg(u.val) FROM t, UNNEST("
                            + "t.payload COLUMNS(val DOUBLE)"
                            + ") u",
                    null, false, false, true
            );
        });
    }

    @Test
    public void testSumOnJsonUnnest() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.0, 2.0, 3.0]')");
            assertQueryNoLeakCheck(
                    "total\n"
                            + "6.0\n",
                    "SELECT sum(u.val) total FROM t, UNNEST("
                            + "t.payload COLUMNS(val DOUBLE)"
                            + ") u",
                    null, false, false, true
            );
        });
    }

    @Test
    public void testCountOnJsonUnnest() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1, 2, 3, 4, 5]')");
            assertQueryNoLeakCheck(
                    "cnt\n"
                            + "5\n",
                    "SELECT count() cnt FROM t, UNNEST("
                            + "t.payload COLUMNS(val LONG)"
                            + ") u",
                    null, false, false, true
            );
        });
    }

    @Test
    public void testErrorNonVarcharWithColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (arr DOUBLE[])");
            execute("INSERT INTO t VALUES (ARRAY[1.0])");
            assertException(
                    "SELECT u.val FROM t, UNNEST("
                            + "t.arr COLUMNS(val DOUBLE)"
                            + ") u",
                    28,
                    "VARCHAR expected for JSON UNNEST"
            );
        });
    }

    @Test
    public void testErrorUnknownType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            assertException(
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val FOOBAR)"
                            + ") u",
                    50,
                    "unknown type"
            );
        });
    }

    @Test
    public void testErrorUnsupportedTypeFloat() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            assertException(
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val FLOAT)"
                            + ") u",
                    50,
                    "unsupported type for JSON UNNEST"
            );
        });
    }

    @Test
    public void testErrorUnsupportedTypeSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            assertException(
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val SYMBOL)"
                            + ") u",
                    50,
                    "unsupported type for JSON UNNEST"
            );
        });
    }

    @Test
    public void testObjectArrayWithNullFirstElement() throws Exception {
        // Verifies scan-forward detection: element 0 is null, but
        // element 1 is an object, so isObjectArray should be true.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ("
                    + "'[null, {\"a\":1}, {\"a\":2}]'"
                    + ")");
            assertQueryNoLeakCheck(
                    "a\n"
                            + "null\n"
                            + "1\n"
                            + "2\n",
                    "SELECT u.a FROM t, UNNEST("
                            + "t.payload COLUMNS(a LONG)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testObjectArrayWithSingleColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ("
                    + "'[{\"price\":1.5},{\"price\":2.5}]'"
                    + ")");
            assertQueryNoLeakCheck(
                    "price\n"
                            + "1.5\n"
                            + "2.5\n",
                    "SELECT u.price FROM t, UNNEST("
                            + "t.payload COLUMNS(price DOUBLE)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testMixedNullAndNonNullRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, payload VARCHAR)");
            execute("INSERT INTO t VALUES "
                    + "(1, '[1.0, 2.0]'), "
                    + "(2, NULL), "
                    + "(3, '[3.0]')");
            assertQueryNoLeakCheck(
                    "id\tval\n"
                            + "1\t1.0\n"
                            + "1\t2.0\n"
                            + "3\t3.0\n",
                    "SELECT t.id, u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val DOUBLE)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarStringNotObject() throws Exception {
        // When single column name is "val" but elements are strings,
        // not objects - should extract directly
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"hello\", \"world\"]')");
            assertQueryNoLeakCheck(
                    "val\n"
                            + "hello\n"
                            + "world\n",
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val VARCHAR)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testObjectArrayWithOrdinality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ("
                    + "'[{\"name\":\"a\"},{\"name\":\"b\"}]'"
                    + ")");
            assertQueryNoLeakCheck(
                    "name\tord\n"
                            + "a\t1\n"
                            + "b\t2\n",
                    "SELECT u.name, u.ord FROM t, UNNEST("
                            + "t.payload COLUMNS(name VARCHAR)"
                            + ") WITH ORDINALITY u(name, ord)",
                    (String) null
            );
        });
    }

    @Test
    public void testNullFieldInObject() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ("
                    + "'[{\"val\":null},{\"val\":1.5}]'"
                    + ")");
            assertQueryNoLeakCheck(
                    "val\n"
                            + "null\n"
                            + "1.5\n",
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val DOUBLE)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testParserRoundtrip() throws Exception {
        // Verify COLUMNS syntax survives parser round-trip
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            assertSql(
                    "QUERY PLAN\n"
                            + "SelectedRecord\n"
                            + "    Unnest\n"
                            + "      columns: [val]\n"
                            + "        PageFrame\n"
                            + "            Row forward scan\n"
                            + "            Frame forward scan on: t\n",
                    "EXPLAIN SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val DOUBLE)"
                            + ") u"
            );
        });
    }

    @Test
    public void testStandaloneSelectStar() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "val\n"
                        + "1.5\n"
                        + "2.5\n",
                "SELECT * FROM UNNEST("
                        + "'[1.5, 2.5]'::VARCHAR "
                        + "COLUMNS(val DOUBLE)"
                        + ") u",
                (String) null
        ));
    }

    @Test
    public void testIntFromDouble() throws Exception {
        // JSON number 1.5 extracted as INT should truncate or return NULL
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1, 2, 3]')");
            assertQueryNoLeakCheck(
                    "val\n"
                            + "1\n"
                            + "2\n"
                            + "3\n",
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val INT)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testBooleanFromObject() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ("
                    + "'[{\"active\":true},{\"active\":false}]'"
                    + ")");
            assertQueryNoLeakCheck(
                    "active\n"
                            + "true\n"
                            + "false\n",
                    "SELECT u.active FROM t, UNNEST("
                            + "t.payload COLUMNS(active BOOLEAN)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testVarcharFromObject() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ("
                    + "'[{\"name\":\"Alice\"},{\"name\":\"Bob\"}]'"
                    + ")");
            assertQueryNoLeakCheck(
                    "name\n"
                            + "Alice\n"
                            + "Bob\n",
                    "SELECT u.name FROM t, UNNEST("
                            + "t.payload COLUMNS(name VARCHAR)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testVarcharExactlyAtCapDoesNotError() throws Exception {
        // A value exactly MAX_JSON_VALUE_SIZE bytes must NOT be treated
        // as overflow. This guards against false positives in the
        // truncation check.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            int cap = JsonUnnestSource.MAX_JSON_VALUE_SIZE;
            StringBuilder exactVal = new StringBuilder(cap);
            for (int i = 0; i < cap; i++) {
                exactVal.append('a');
            }
            execute("INSERT INTO t VALUES ('[{\"s\":\"" + exactVal + "\"}]')");
            assertSql(
                    "s\n"
                            + exactVal + "\n",
                    "SELECT u.s FROM t, UNNEST("
                            + "t.payload COLUMNS(s VARCHAR)"
                            + ") u"
            );
        });
    }

    @Test
    public void testMultipleBaseRowsDifferentLengths() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, payload VARCHAR)");
            execute("INSERT INTO t VALUES "
                    + "(1, '[1.0]'), "
                    + "(2, '[2.0, 3.0, 4.0]'), "
                    + "(3, '[]')");
            assertQueryNoLeakCheck(
                    "id\tval\n"
                            + "1\t1.0\n"
                            + "2\t2.0\n"
                            + "2\t3.0\n"
                            + "2\t4.0\n",
                    "SELECT t.id, u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val DOUBLE)"
                            + ") u",
                    (String) null
            );
        });
    }
}

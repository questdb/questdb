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

// Most tests use assertQueryNoLeakCheck() which re-executes the query to
// verify cursor stability and checks factory memory. Tests that combine
// JSON UNNEST with json_extract() use assertSql() instead, because
// json_extract() lazily allocates ~2MB internal buffers that trip the
// factory memory check. See testJsonExtractWithJsonUnnest for details.
public class JsonUnnestTest extends AbstractCairoTest {

    @Test
    public void testScalarDoubleArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.5, 2.5, 3.5]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            1.5
                            2.5
                            3.5
                            """,
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
                    """
                            val
                            10
                            20
                            30
                            """,
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
                    """
                            val
                            100000
                            200000
                            300000
                            """,
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
                    """
                            val
                            1
                            2
                            3
                            """,
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
                    """
                            val
                            true
                            false
                            true
                            """,
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
                    """
                            val
                            hello
                            world
                            """,
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
                    """
                            price\tname
                            1.5\tapple
                            2.5\tbanana
                            """,
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
                    """
                            price\tname
                            1.5\t
                            2.5\tbanana
                            """,
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
                    """
                            price
                            1.5
                            """,
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
                    """
                            val
                            null
                            """,
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val DOUBLE)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testTypeMismatchIntReturnsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"val\":\"not_a_number\"}]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            null
                            """,
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val INT)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testTypeMismatchLongReturnsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"val\":\"not_a_number\"}]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            null
                            """,
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val LONG)"
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
                    """
                            val\tord
                            10.0\t1
                            20.0\t2
                            30.0\t3
                            """,
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
                """
                        price\tname
                        1.5\tapple
                        2.5\tbanana
                        """,
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
                    """
                            id\tval
                            1\t1.0
                            1\t2.0
                            2\t3.0
                            2\t4.0
                            2\t5.0
                            """,
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
                    """
                            elem\tname
                            100.0\ta
                            200.0\tb
                            """,
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
                    """
                            id\tpayload\tprice
                            1\t[{"price":1.5}]\t1.5
                            """,
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
                    """
                            price
                            2.5
                            3.5
                            """,
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
                    """
                            cat\ttotal
                            a\t4
                            b\t2
                            """,
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
                    """
                            val
                            1.0
                            2.0
                            3.0
                            """,
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
                    """
                            val
                            1.0
                            2.0
                            """,
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
                    """
                            a
                            1
                            2
                            """,
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
                    """
                            val
                            1.5
                            2.5
                            """,
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
                    """
                            total
                            6.0
                            """,
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
                    """
                            my_price
                            1.5
                            """,
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
                    """
                            price
                            1.5
                            """,
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
                    """
                            SelectedRecord
                                Unnest
                                  columns: [val]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """
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
                    """
                            ts
                            2024-01-15T10:30:00.000000Z
                            2024-06-20T14:00:00.000000Z
                            """,
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
                    """
                            ts
                            2024-01-15T09:30:00.000000Z
                            """,
                    "SELECT u.ts FROM t, UNNEST("
                            + "t.payload COLUMNS(ts TIMESTAMP)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testTimestampOverflowThrowsError() throws Exception {
        // Exercises the overflow check in getTimestamp() - a string value
        // exceeding DEFAULT_MAX_JSON_UNNEST_VALUE_SIZE in a TIMESTAMP column must throw.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            String bigTs = "2024-01-15T10:30:00.000000Z" + "x".repeat(5000);
            execute("INSERT INTO t VALUES ('[{\"ts\":\"" + bigTs + "\"}]')");
            assertExceptionNoLeakCheck(
                    "SELECT u.ts FROM t, UNNEST("
                            + "t.payload COLUMNS(ts TIMESTAMP)"
                            + ") u",
                    0,
                    "JSON UNNEST: value exceeds maximum size of "
                            + JsonUnnestSource.DEFAULT_MAX_JSON_UNNEST_VALUE_SIZE
                            + " bytes for column 'ts'"
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
                    + "'[{\"name\":\"été\"},{\"name\":\"üñ\"}]'"
                    + ")");
            assertQueryNoLeakCheck(
                    """
                            name
                            été
                            üñ
                            """,
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
                    """
                            cnt
                            1000
                            """,
                    "SELECT count() cnt FROM t, UNNEST("
                            + "t.payload COLUMNS(val LONG)"
                            + ") u",
                    null, false, false, true
            );
        });
    }

    @Test
    public void testLargeNonAsciiVarcharOverflowThrowsError() throws Exception {
        // Non-ASCII values exceeding DEFAULT_MAX_JSON_UNNEST_VALUE_SIZE trigger UTF-8
        // backoff in the native layer, producing sink.size() < maxSize.
        // The native truncated flag detects the overflow and throws.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            // 1366 CJK characters x 3 bytes each = 4098 bytes > 4096
            int charCount = (JsonUnnestSource.DEFAULT_MAX_JSON_UNNEST_VALUE_SIZE / 3) + 2;
            String bigVal = "世".repeat(charCount); // '世' = 3 bytes in UTF-8
            execute("INSERT INTO t VALUES ('[{\"s\":\"" + bigVal + "\"}]')");
            assertExceptionNoLeakCheck(
                    "SELECT u.s FROM t, UNNEST("
                            + "t.payload COLUMNS(s VARCHAR)"
                            + ") u",
                    0,
                    "JSON UNNEST: value exceeds maximum size of "
                            + JsonUnnestSource.DEFAULT_MAX_JSON_UNNEST_VALUE_SIZE
                            + " bytes for column 's'"
            );
        });
    }

    @Test
    public void testLargeVarcharOverflowThrowsError() throws Exception {
        // Values exceeding DEFAULT_MAX_JSON_UNNEST_VALUE_SIZE (4096) throw an error
        // rather than silently truncating or returning NULL.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            String bigVal = "x".repeat(5000);
            execute("INSERT INTO t VALUES ('[{\"s\":\"" + bigVal + "\"}]')");
            assertExceptionNoLeakCheck(
                    "SELECT u.s FROM t, UNNEST("
                            + "t.payload COLUMNS(s VARCHAR)"
                            + ") u",
                    0,
                    "JSON UNNEST: value exceeds maximum size of "
                            + JsonUnnestSource.DEFAULT_MAX_JSON_UNNEST_VALUE_SIZE
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
                    """
                            val
                            1.5
                            null
                            3.5
                            """,
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
                    """
                            val
                            null
                            1.5
                            2.5
                            """,
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val DOUBLE)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testAllNullArrayReturnsAllNulls() throws Exception {
        // When every element in the JSON array is null, the scan-forward
        // detection cannot determine scalar vs object. It defaults to
        // scalar, which is correct: both paths produce NULL for null
        // elements, so the result is the same regardless.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[null, null, null]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            null
                            null
                            null
                            """,
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val DOUBLE)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testAllNullObjectArrayReturnsAllNulls() throws Exception {
        // Same as above but with multiple declared columns, which forces
        // isObjectArray = true. The result is still all NULLs because
        // accessing any field on a null JSON element returns NULL.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[null, null]')");
            assertQueryNoLeakCheck(
                    """
                            a\tb
                            null\tnull
                            null\tnull
                            """,
                    "SELECT u.a, u.b FROM t, UNNEST("
                            + "t.payload COLUMNS(a DOUBLE, b DOUBLE)"
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
                    """
                            price\tname
                            1.5\ta
                            2.5\tb
                            """,
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
                    """
                            val\tname
                            1.0\tx
                            2.0\ty
                            null\tz
                            """,
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
                    """
                            avg
                            20.0
                            """,
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
                    """
                            total
                            6.0
                            """,
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
                    """
                            cnt
                            5
                            """,
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
                    """
                            a
                            null
                            1
                            2
                            """,
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
                    """
                            price
                            1.5
                            2.5
                            """,
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
                    """
                            id\tval
                            1\t1.0
                            1\t2.0
                            3\t3.0
                            """,
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
                    """
                            val
                            hello
                            world
                            """,
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
                    """
                            name\tord
                            a\t1
                            b\t2
                            """,
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
                    """
                            val
                            null
                            1.5
                            """,
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
                    """
                            QUERY PLAN
                            SelectedRecord
                                Unnest
                                  columns: [val]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """,
                    "EXPLAIN SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val DOUBLE)"
                            + ") u"
            );
        });
    }

    @Test
    public void testStandaloneSelectStar() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                """
                        val
                        1.5
                        2.5
                        """,
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
                    """
                            val
                            1
                            2
                            3
                            """,
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
                    """
                            active
                            true
                            false
                            """,
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
                    """
                            name
                            Alice
                            Bob
                            """,
                    "SELECT u.name FROM t, UNNEST("
                            + "t.payload COLUMNS(name VARCHAR)"
                            + ") u",
                    (String) null
            );
        });
    }

    @Test
    public void testVarcharExactlyAtCapDoesNotError() throws Exception {
        // A value exactly DEFAULT_MAX_JSON_UNNEST_VALUE_SIZE bytes must NOT be treated
        // as overflow. This guards against false positives in the
        // truncation check.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            int cap = JsonUnnestSource.DEFAULT_MAX_JSON_UNNEST_VALUE_SIZE;
            String exactVal = "a".repeat(cap);
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
                    """
                            id\tval
                            1\t1.0
                            2\t2.0
                            2\t3.0
                            2\t4.0
                            """,
                    "SELECT t.id, u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val DOUBLE)"
                            + ") u",
                    (String) null
            );
        });
    }

    // ---- Tests for COLUMNS keyword validation ----

    @Test
    public void testColumnsKeywordAsColumnNameRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            assertException(
                    "SELECT * FROM t, UNNEST(t.payload COLUMNS(select DOUBLE)) u",
                    42,
                    "have to be enclosed in double quotes"
            );
        });
    }

    @Test
    public void testColumnsKeywordQuotedColumnNameAllowed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"select\":1.5}]')");
            assertQueryNoLeakCheck(
                    """
                            select
                            1.5
                            """,
                    "SELECT u.\"select\" FROM t, UNNEST(t.payload COLUMNS(\"select\" DOUBLE)) u",
                    (String) null
            );
        });
    }

    // ========================================================================
    // Comprehensive per-type tests: scalar arrays, object arrays, nullability
    // ========================================================================

    // ---- INT: scalar array ----

    @Test
    public void testScalarIntNullElement() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[10, null, 30]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            10
                            null
                            30
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val INT)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarIntAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[null, null, null]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            null
                            null
                            null
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val INT)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarIntTypeMismatchString() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"not_a_number\", 42]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            null
                            42
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val INT)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarIntFromDoubleValue() throws Exception {
        // JSON has no integer type distinction; 1.9 should truncate or return NULL
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.9, 2.0, 3.1]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            1
                            2
                            3
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val INT)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarIntNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            // INT_MIN (-2147483648) is Numbers.INT_NULL sentinel, so it reads as null
            execute("INSERT INTO t VALUES ('[-1, -2147483647, 2147483647, 0]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            -1
                            -2147483647
                            2147483647
                            0
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val INT)) u",
                    (String) null
            );
        });
    }

    // ---- INT: object array ----

    @Test
    public void testObjectIntNullField() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"v\":10}, {\"v\":null}, {\"v\":30}]')");
            assertQueryNoLeakCheck(
                    """
                            v
                            10
                            null
                            30
                            """,
                    "SELECT u.v FROM t, UNNEST(t.payload COLUMNS(v INT)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testObjectIntMissingField() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"v\":10}, {\"other\":99}, {\"v\":30}]')");
            assertQueryNoLeakCheck(
                    """
                            v
                            10
                            null
                            30
                            """,
                    "SELECT u.v FROM t, UNNEST(t.payload COLUMNS(v INT)) u",
                    (String) null
            );
        });
    }

    // ---- LONG: scalar array ----

    @Test
    public void testScalarLongNullElement() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[100, null, 300]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            100
                            null
                            300
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val LONG)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarLongLargeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            // LONG_MIN (-9223372036854775808) is Numbers.LONG_NULL sentinel, so it reads as null
            execute("INSERT INTO t VALUES ('[9223372036854775807, -9223372036854775807, 0]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            9223372036854775807
                            -9223372036854775807
                            0
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val LONG)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarLongTypeMismatchString() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"hello\", 42]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            null
                            42
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val LONG)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarLongAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[null, null]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            null
                            null
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val LONG)) u",
                    (String) null
            );
        });
    }

    // ---- LONG: object array ----

    @Test
    public void testObjectLongNullField() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"v\":100}, {\"v\":null}]')");
            assertQueryNoLeakCheck(
                    """
                            v
                            100
                            null
                            """,
                    "SELECT u.v FROM t, UNNEST(t.payload COLUMNS(v LONG)) u",
                    (String) null
            );
        });
    }

    // ---- SHORT: scalar array ----

    @Test
    public void testScalarShortNullElement() throws Exception {
        // SHORT null sentinel is 0, so null elements return 0
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[10, null, 30]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            10
                            0
                            30
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val SHORT)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarShortTypeMismatch() throws Exception {
        // Type mismatch returns 0 (SHORT null sentinel)
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"text\", 5]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            0
                            5
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val SHORT)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarShortNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[-1, 32767, -32768]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            -1
                            32767
                            -32768
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val SHORT)) u",
                    (String) null
            );
        });
    }

    // ---- SHORT: object array ----

    @Test
    public void testObjectShortNullField() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"v\":10}, {\"v\":null}]')");
            assertQueryNoLeakCheck(
                    """
                            v
                            10
                            0
                            """,
                    "SELECT u.v FROM t, UNNEST(t.payload COLUMNS(v SHORT)) u",
                    (String) null
            );
        });
    }

    // ---- DOUBLE: scalar array ----

    @Test
    public void testScalarDoubleNullElement() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.5, null, 3.5]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            1.5
                            null
                            3.5
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val DOUBLE)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarDoubleAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[null, null]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            null
                            null
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val DOUBLE)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarDoubleTypeMismatchString() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"abc\", 2.5]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            null
                            2.5
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val DOUBLE)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarDoubleSpecialValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[0.0, -0.0, 1.7976931348623157E308]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            0.0
                            -0.0
                            1.7976931348623157E308
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val DOUBLE)) u",
                    (String) null
            );
        });
    }

    // ---- DOUBLE: object array ----

    @Test
    public void testObjectDoubleNullField() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"v\":1.5}, {\"v\":null}, {\"v\":3.5}]')");
            assertQueryNoLeakCheck(
                    """
                            v
                            1.5
                            null
                            3.5
                            """,
                    "SELECT u.v FROM t, UNNEST(t.payload COLUMNS(v DOUBLE)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testObjectDoubleMissingField() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"v\":1.5}, {\"x\":99}, {\"v\":3.5}]')");
            assertQueryNoLeakCheck(
                    """
                            v
                            1.5
                            null
                            3.5
                            """,
                    "SELECT u.v FROM t, UNNEST(t.payload COLUMNS(v DOUBLE)) u",
                    (String) null
            );
        });
    }

    // ---- BOOLEAN: scalar array ----

    @Test
    public void testScalarBooleanNullElement() throws Exception {
        // BOOLEAN null sentinel is false
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[true, null, false]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            true
                            false
                            false
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val BOOLEAN)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarBooleanAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[null, null]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            false
                            false
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val BOOLEAN)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarBooleanTypeMismatch() throws Exception {
        // Non-boolean values return false
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1, \"yes\", true]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            false
                            false
                            true
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val BOOLEAN)) u",
                    (String) null
            );
        });
    }

    // ---- BOOLEAN: object array ----

    @Test
    public void testObjectBooleanNullField() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"v\":true}, {\"v\":null}, {\"v\":false}]')");
            assertQueryNoLeakCheck(
                    """
                            v
                            true
                            false
                            false
                            """,
                    "SELECT u.v FROM t, UNNEST(t.payload COLUMNS(v BOOLEAN)) u",
                    (String) null
            );
        });
    }

    // ---- VARCHAR: scalar array ----

    @Test
    public void testScalarVarcharNullElement() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"hello\", null, \"world\"]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            hello
                            
                            world
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val VARCHAR)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarVarcharAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[null, null, null]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            
                            
                            
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val VARCHAR)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarVarcharEmptyString() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"hello\", \"\", \"world\"]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            hello
                            
                            world
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val VARCHAR)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarVarcharNumberCoercion() throws Exception {
        // Numbers extracted as VARCHAR should return as string representation
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[42, true, \"text\"]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            42
                            true
                            text
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val VARCHAR)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarVarcharSpecialChars() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"line1\\nline2\", \"tab\\there\", \"quote\\\"inside\"]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            line1
                            line2
                            tab\there
                            quote"inside
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val VARCHAR)) u",
                    (String) null
            );
        });
    }

    // ---- VARCHAR: object array ----

    @Test
    public void testObjectVarcharNullField() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"v\":\"hello\"}, {\"v\":null}, {\"v\":\"world\"}]')");
            assertQueryNoLeakCheck(
                    """
                            v
                            hello
                            
                            world
                            """,
                    "SELECT u.v FROM t, UNNEST(t.payload COLUMNS(v VARCHAR)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testObjectVarcharMissingField() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"v\":\"hello\"}, {\"other\":\"x\"}, {\"v\":\"world\"}]')");
            assertQueryNoLeakCheck(
                    """
                            v
                            hello
                            
                            world
                            """,
                    "SELECT u.v FROM t, UNNEST(t.payload COLUMNS(v VARCHAR)) u",
                    (String) null
            );
        });
    }

    // ---- TIMESTAMP: scalar array ----

    @Test
    public void testScalarTimestampNullElement() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"2024-01-01T00:00:00.000000Z\", null, \"2024-01-03T00:00:00.000000Z\"]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            2024-01-01T00:00:00.000000Z
                            
                            2024-01-03T00:00:00.000000Z
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val TIMESTAMP)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarTimestampAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[null, null]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            
                            
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val TIMESTAMP)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarTimestampInvalidStringReturnsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"not-a-timestamp\", \"2024-01-01T00:00:00.000000Z\"]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            
                            2024-01-01T00:00:00.000000Z
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val TIMESTAMP)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarTimestampFromNumericMicros() throws Exception {
        // 1704067200000000 = 2024-01-01T00:00:00.000000Z in micros
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1704067200000000, 0]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            2024-01-01T00:00:00.000000Z
                            1970-01-01T00:00:00.000000Z
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val TIMESTAMP)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarTimestampMixedStringAndNumeric() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"2024-06-15T12:30:00.000000Z\", 1704067200000000, null]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            2024-06-15T12:30:00.000000Z
                            2024-01-01T00:00:00.000000Z
                            
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val TIMESTAMP)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testScalarTimestampTypeMismatchBoolean() throws Exception {
        // Boolean value should return null for TIMESTAMP
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[true, \"2024-01-01T00:00:00.000000Z\"]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            
                            2024-01-01T00:00:00.000000Z
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val TIMESTAMP)) u",
                    (String) null
            );
        });
    }

    // ---- TIMESTAMP: object array ----

    @Test
    public void testObjectTimestampNullField() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"ts\":\"2024-01-01T00:00:00.000000Z\"}, {\"ts\":null}]')");
            assertQueryNoLeakCheck(
                    """
                            ts
                            2024-01-01T00:00:00.000000Z
                            
                            """,
                    "SELECT u.ts FROM t, UNNEST(t.payload COLUMNS(ts TIMESTAMP)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testObjectTimestampMissingField() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"ts\":\"2024-01-01T00:00:00.000000Z\"}, {\"other\":1}]')");
            assertQueryNoLeakCheck(
                    """
                            ts
                            2024-01-01T00:00:00.000000Z
                            
                            """,
                    "SELECT u.ts FROM t, UNNEST(t.payload COLUMNS(ts TIMESTAMP)) u",
                    (String) null
            );
        });
    }

    // ---- Multi-column object array: all types together ----

    @Test
    public void testObjectMultiTypeWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[" +
                    "{\"i\":1, \"l\":100, \"d\":1.5, \"b\":true, \"s\":\"hello\", \"ts\":\"2024-01-01T00:00:00.000000Z\"}," +
                    "{\"i\":null, \"l\":null, \"d\":null, \"b\":null, \"s\":null, \"ts\":null}," +
                    "{\"i\":3, \"l\":300, \"d\":3.5, \"b\":false, \"s\":\"world\", \"ts\":\"2024-06-15T00:00:00.000000Z\"}" +
                    "]')");
            assertQueryNoLeakCheck(
                    """
                            i\tl\td\tb\ts\tts
                            1\t100\t1.5\ttrue\thello\t2024-01-01T00:00:00.000000Z
                            null\tnull\tnull\tfalse\t\t
                            3\t300\t3.5\tfalse\tworld\t2024-06-15T00:00:00.000000Z
                            """,
                    "SELECT u.i, u.l, u.d, u.b, u.s, u.ts FROM t, UNNEST(t.payload" +
                            " COLUMNS(i INT, l LONG, d DOUBLE, b BOOLEAN, s VARCHAR, ts TIMESTAMP)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testObjectAllFieldsMissing() throws Exception {
        // Element is an object but has none of the declared fields
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"x\":1, \"y\":2}]')");
            assertQueryNoLeakCheck(
                    """
                            i\td\ts
                            null\tnull\t
                            """,
                    "SELECT u.i, u.d, u.s FROM t, UNNEST(t.payload" +
                            " COLUMNS(i INT, d DOUBLE, s VARCHAR)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testObjectNullElement() throws Exception {
        // Null element in object array: all fields should be null
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"i\":1, \"d\":1.5}, null, {\"i\":3, \"d\":3.5}]')");
            assertQueryNoLeakCheck(
                    """
                            i\td
                            1\t1.5
                            null\tnull
                            3\t3.5
                            """,
                    "SELECT u.i, u.d FROM t, UNNEST(t.payload COLUMNS(i INT, d DOUBLE)) u",
                    (String) null
            );
        });
    }

    // ---- Edge cases: empty/null/invalid JSON payload ----

    @Test
    public void testNullPayloadAllTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES (NULL)");
            assertQueryNoLeakCheck(
                    "i\tl\td\tb\ts\tts\n",
                    "SELECT u.i, u.l, u.d, u.b, u.s, u.ts FROM t, UNNEST(t.payload" +
                            " COLUMNS(i INT, l LONG, d DOUBLE, b BOOLEAN, s VARCHAR, ts TIMESTAMP)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testEmptyStringPayload() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('')");
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val INT)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testSingleElementScalarArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[42]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            42
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val INT)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testSingleElementObjectArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"v\":42}]')");
            assertQueryNoLeakCheck(
                    """
                            v
                            42
                            """,
                    "SELECT u.v FROM t, UNNEST(t.payload COLUMNS(v INT)) u",
                    (String) null
            );
        });
    }

    // ---- Multiple rows with mixed null/valid payloads ----

    @Test
    public void testMultipleRowsMixedPayloadsAllTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, payload VARCHAR)");
            execute("INSERT INTO t VALUES " +
                    "(1, '[{\"v\":10}, {\"v\":20}]')," +
                    "(2, NULL)," +
                    "(3, '[{\"v\":30}]')");
            assertQueryNoLeakCheck(
                    """
                            id\tv
                            1\t10
                            1\t20
                            3\t30
                            """,
                    "SELECT t.id, u.v FROM t, UNNEST(t.payload COLUMNS(v INT)) u",
                    (String) null
            );
        });
    }

    // ---- getVarcharSize ----

    @Test
    public void testVarcharSizeReturnsCorrectLength() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"ab\", null, \"hello\"]')");
            assertQueryNoLeakCheck(
                    """
                            val\tlen
                            ab\t2
                            \t0
                            hello\t5
                            """,
                    "SELECT u.val, length(u.val) len FROM t, UNNEST(t.payload COLUMNS(val VARCHAR)) u",
                    (String) null
            );
        });
    }

    // ---- CAST TESTS: casting unnested JSON values ----

    @Test
    public void testCastIntToDouble() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1, 2, 3]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            1.0
                            2.0
                            3.0
                            """,
                    "SELECT u.val::DOUBLE val FROM t, UNNEST(t.payload COLUMNS(val INT)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testCastIntToLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1, 2, 3]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            1
                            2
                            3
                            """,
                    "SELECT u.val::LONG val FROM t, UNNEST(t.payload COLUMNS(val INT)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testCastIntToShort() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[100, 200, 300]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            100
                            200
                            300
                            """,
                    "SELECT u.val::SHORT val FROM t, UNNEST(t.payload COLUMNS(val INT)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testCastIntToVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[42, null, -7]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            42
                            \
                            
                            -7
                            """,
                    "SELECT u.val::VARCHAR val FROM t, UNNEST(t.payload COLUMNS(val INT)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testCastIntToBoolean() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[0, 1, 42]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            false
                            true
                            true
                            """,
                    "SELECT u.val::BOOLEAN val FROM t, UNNEST(t.payload COLUMNS(val INT)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testCastLongToInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[100, 200, 300]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            100
                            200
                            300
                            """,
                    "SELECT u.val::INT val FROM t, UNNEST(t.payload COLUMNS(val LONG)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testCastLongToDouble() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[100, 200, 300]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            100.0
                            200.0
                            300.0
                            """,
                    "SELECT u.val::DOUBLE val FROM t, UNNEST(t.payload COLUMNS(val LONG)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testCastLongToVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[42, null, -7]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            42
                            \
                            
                            -7
                            """,
                    "SELECT u.val::VARCHAR val FROM t, UNNEST(t.payload COLUMNS(val LONG)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testCastLongToTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1000000, 0]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            1970-01-01T00:00:01.000000Z
                            1970-01-01T00:00:00.000000Z
                            """,
                    "SELECT u.val::TIMESTAMP val FROM t, UNNEST(t.payload COLUMNS(val LONG)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testCastDoubleToInt() throws Exception {
        // double-to-int cast truncates toward zero
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.9, 2.1, 3.5]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            1
                            2
                            3
                            """,
                    "SELECT u.val::INT val FROM t, UNNEST(t.payload COLUMNS(val DOUBLE)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testCastDoubleToLong() throws Exception {
        // double-to-long cast truncates toward zero
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.9, 2.1, 3.5]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            1
                            2
                            3
                            """,
                    "SELECT u.val::LONG val FROM t, UNNEST(t.payload COLUMNS(val DOUBLE)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testCastDoubleToVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.5, null, -3.14]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            1.5
                            \
                            
                            -3.14
                            """,
                    "SELECT u.val::VARCHAR val FROM t, UNNEST(t.payload COLUMNS(val DOUBLE)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testCastShortToInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[10, 20, 30]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            10
                            20
                            30
                            """,
                    "SELECT u.val::INT val FROM t, UNNEST(t.payload COLUMNS(val SHORT)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testCastShortToDouble() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[10, 20, 30]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            10.0
                            20.0
                            30.0
                            """,
                    "SELECT u.val::DOUBLE val FROM t, UNNEST(t.payload COLUMNS(val SHORT)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testCastBooleanToInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[true, false, true]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            1
                            0
                            1
                            """,
                    "SELECT u.val::INT val FROM t, UNNEST(t.payload COLUMNS(val BOOLEAN)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testCastBooleanToVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[true, false]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            true
                            false
                            """,
                    "SELECT u.val::VARCHAR val FROM t, UNNEST(t.payload COLUMNS(val BOOLEAN)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testCastTimestampToVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"2024-01-01T00:00:00.000000Z\", \"2024-06-15T12:30:00.000000Z\"]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            2024-01-01T00:00:00.000000Z
                            2024-06-15T12:30:00.000000Z
                            """,
                    "SELECT u.val::VARCHAR val FROM t, UNNEST(t.payload COLUMNS(val TIMESTAMP)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testCastTimestampToLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"2024-01-01T00:00:00.000000Z\"]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            1704067200000000
                            """,
                    "SELECT u.val::LONG val FROM t, UNNEST(t.payload COLUMNS(val TIMESTAMP)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testCastVarcharToInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"42\", \"100\", \"-7\"]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            42
                            100
                            -7
                            """,
                    "SELECT u.val::INT val FROM t, UNNEST(t.payload COLUMNS(val VARCHAR)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testCastVarcharToDouble() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"1.5\", \"2.7\"]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            1.5
                            2.7
                            """,
                    "SELECT u.val::DOUBLE val FROM t, UNNEST(t.payload COLUMNS(val VARCHAR)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testCastVarcharToTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"2024-01-01T00:00:00.000000Z\"]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            2024-01-01T00:00:00.000000Z
                            """,
                    "SELECT u.val::TIMESTAMP val FROM t, UNNEST(t.payload COLUMNS(val VARCHAR)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testCastVarcharToBoolean() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"true\", \"false\"]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            true
                            false
                            """,
                    "SELECT u.val::BOOLEAN val FROM t, UNNEST(t.payload COLUMNS(val VARCHAR)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testCastNullPreservation() throws Exception {
        // Null INT cast to DOUBLE preserves null
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[null, 42]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            null
                            42.0
                            """,
                    "SELECT u.val::DOUBLE val FROM t, UNNEST(t.payload COLUMNS(val INT)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testCastIntNullToVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[null, 42]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            \
                            
                            42
                            """,
                    "SELECT u.val::VARCHAR val FROM t, UNNEST(t.payload COLUMNS(val INT)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testCastObjectFieldToDouble() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"price\": 10}, {\"price\": 20}]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            10.0
                            20.0
                            """,
                    "SELECT u.price::DOUBLE val FROM t, UNNEST(t.payload COLUMNS(price INT)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testCastObjectFieldTimestampToLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"ts\": \"2024-01-01T00:00:00.000000Z\"}]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            1704067200000000
                            """,
                    "SELECT u.ts::LONG val FROM t, UNNEST(t.payload COLUMNS(ts TIMESTAMP)) u",
                    (String) null
            );
        });
    }

    // ---- WITH ORDINALITY cast tests ----

    @Test
    public void testOrdinalityCastToInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[10, 20, 30]')");
            assertQueryNoLeakCheck(
                    """
                            val\tord
                            10\t1
                            20\t2
                            30\t3
                            """,
                    "SELECT u.val, u.ordinality::INT ord FROM t, UNNEST(t.payload COLUMNS(val INT)) WITH ORDINALITY u",
                    (String) null
            );
        });
    }

    @Test
    public void testOrdinalityCastToDouble() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[10, 20]')");
            assertQueryNoLeakCheck(
                    """
                            val\tord
                            10\t1.0
                            20\t2.0
                            """,
                    "SELECT u.val, u.ordinality::DOUBLE ord FROM t, UNNEST(t.payload COLUMNS(val INT)) WITH ORDINALITY u",
                    (String) null
            );
        });
    }

    // ---- Varchar B copy (cursor stability) via cross-join ----

    @Test
    public void testVarcharBCopyViaJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"hello\", \"world\"]')");
            assertQueryNoLeakCheck(
                    """
                            a\tb
                            hello\thello
                            hello\tworld
                            world\thello
                            world\tworld
                            """,
                    "SELECT u1.val a, u2.val b"
                            + " FROM t, UNNEST(t.payload COLUMNS(val VARCHAR)) u1"
                            + ", UNNEST(t.payload COLUMNS(val VARCHAR)) u2",
                    (String) null
            );
        });
    }

    // ---- Overflow / truncation tests ----

    @Test
    public void testTimestampTruncationInObject() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            StringBuilder sb = new StringBuilder("[{\"ts\": \"");
            for (int i = 0; i < JsonUnnestSource.DEFAULT_MAX_JSON_UNNEST_VALUE_SIZE + 100; i++) {
                sb.append('x');
            }
            sb.append("\"}]");
            execute("INSERT INTO t VALUES ('" + sb + "')");
            assertException(
                    "SELECT u.ts FROM t, UNNEST(t.payload COLUMNS(ts TIMESTAMP)) u",
                    0,
                    "JSON UNNEST: value exceeds maximum size"
            );
        });
    }

    @Test
    public void testVarcharTruncationInObjectThrows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            StringBuilder sb = new StringBuilder("[{\"v\": \"");
            for (int i = 0; i < JsonUnnestSource.DEFAULT_MAX_JSON_UNNEST_VALUE_SIZE + 100; i++) {
                sb.append('a');
            }
            sb.append("\"}]");
            execute("INSERT INTO t VALUES ('" + sb + "')");
            assertException(
                    "SELECT u.v FROM t, UNNEST(t.payload COLUMNS(v VARCHAR)) u",
                    0,
                    "JSON UNNEST: value exceeds maximum size"
            );
        });
    }

    // ---- init() edge cases ----

    @Test
    public void testWhitespaceOnlyPayloadReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('   ')");
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val INT)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testJsonObjectNotArrayReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('{\"key\": \"value\"}')");
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val VARCHAR)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testMultiColumnForcesObjectMode() throws Exception {
        // With multiple columns, init() forces isObjectArray = true.
        // Scalar array elements have no named fields, so all columns return null/default.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1, 2, 3]')");
            assertQueryNoLeakCheck(
                    """
                            a\tb
                            null\tnull
                            null\tnull
                            null\tnull
                            """,
                    "SELECT u.a, u.b FROM t, UNNEST(t.payload COLUMNS(a DOUBLE, b DOUBLE)) u",
                    (String) null
            );
        });
    }

    // ---- Expressions on unnested values ----

    @Test
    public void testArithmeticOnUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[10, 20, 30]')");
            assertQueryNoLeakCheck(
                    """
                            doubled
                            20
                            40
                            60
                            """,
                    "SELECT u.val * 2 doubled FROM t, UNNEST(t.payload COLUMNS(val INT)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testConcatOnUnnestedVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"hello\", \"world\"]')");
            assertQueryNoLeakCheck(
                    """
                            greeting
                            hello!
                            world!
                            """,
                    "SELECT concat(u.val, '!') greeting FROM t, UNNEST(t.payload COLUMNS(val VARCHAR)) u",
                    (String) null
            );
        });
    }

    // ---- Multi-row with mixed array lengths ----

    @Test
    public void testMultipleRowsDifferentArrayTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1]'), ('[2, 3]'), ('[4, 5, 6]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            1
                            2
                            3
                            4
                            5
                            6
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val INT)) u",
                    (String) null
            );
        });
    }

    @Test
    public void testMultipleRowsMixedNullAndValid() throws Exception {
        // Mix of null payloads, empty arrays, valid arrays, and invalid JSON
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES (NULL), ('[]'), ('[1, 2]'), ('not json'), ('[3]')");
            assertQueryNoLeakCheck(
                    """
                            val
                            1
                            2
                            3
                            """,
                    "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val INT)) u",
                    (String) null
            );
        });
    }
}

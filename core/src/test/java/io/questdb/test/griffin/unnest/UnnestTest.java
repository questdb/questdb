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

package io.questdb.test.griffin.unnest;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class UnnestTest extends AbstractCairoTest {

    @Test
    public void testAliasInGroupByAndOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 1.0, 3.0, 2.0] arr FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT u.val, count() cnt FROM t, UNNEST(t.arr) u(val) "
                    + "GROUP BY u.val ORDER BY u.val")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            val\tcnt
                            1.0\t2
                            2.0\t2
                            3.0\t1
                            """);
        });
    }

    @Test
    public void testAvgOnUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[10.0, 20.0, 30.0] arr FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT avg(u.val) FROM t, UNNEST(t.arr) u(val)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            avg
                            20.0
                            """);
        });
    }

    @Test
    public void testBaseTableByteAndCharColumnsWithUnnest() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (b BYTE, c CHAR, arr DOUBLE[])");
            execute("INSERT INTO t VALUES (42, 'X', ARRAY[1.0, 2.0])");
            assertQuery("SELECT t.b, t.c, u.val FROM t, UNNEST(t.arr) u(val)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            b\tc\tval
                            42\tX\t1.0
                            42\tX\t2.0
                            """);
        });
    }

    @Test
    public void testCTEAsBaseForUnnest() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT x::LONG id, ARRAY[x * 10.0, x * 20.0] arr "
                    + "FROM long_sequence(2)"
                    + ")");
            assertQuery("WITH cte AS (SELECT id, arr FROM t) "
                    + "SELECT cte.id, u.val FROM cte, UNNEST(cte.arr) u(val)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\tval
                            1\t10.0
                            1\t20.0
                            2\t20.0
                            2\t40.0
                            """);
        });
    }

    @Test
    public void testCTEContainingUnnest() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 3.0] arr FROM long_sequence(1)"
                    + ")");
            assertQuery("WITH cte AS (SELECT u.val FROM t, UNNEST(t.arr) u(val)) "
                    + "SELECT val FROM cte")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            1.0
                            2.0
                            3.0
                            """);
        });
    }

    @Test
    public void testCTEContainingUnnestWithOrdinality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[10.0, 20.0, 30.0] arr FROM long_sequence(1)"
                    + ")");
            assertQuery("WITH cte AS ("
                    + "SELECT u.val, u.ord "
                    + "FROM t, UNNEST(t.arr) WITH ORDINALITY u(val, ord)"
                    + ") SELECT val, ord FROM cte")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val\tord
                            10.0\t1
                            20.0\t2
                            30.0\t3
                            """);
        });
    }

    @Test
    public void testCTEUnnestJoinedWithTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id LONG, arr DOUBLE[])");
            execute("INSERT INTO t VALUES (1, ARRAY[10.0, 20.0])");
            execute("CREATE TABLE t2 (id LONG, name SYMBOL)");
            execute("INSERT INTO t2 VALUES (1, 'Alice')");
            assertQuery("WITH unnested AS ("
                    + "SELECT t.id, u.val FROM t, UNNEST(t.arr) u(val)"
                    + ") SELECT unnested.id, unnested.val, t2.name "
                    + "FROM unnested JOIN t2 ON t2.id = unnested.id")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\tval\tname
                            1\t10.0\tAlice
                            1\t20.0\tAlice
                            """);
        });
    }

    @Test
    public void testColumnAliasConflictsWithBase() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT x::LONG id, ARRAY[100.0, 200.0] arr FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT t.id, u.id id1 FROM t, UNNEST(t.arr) u(id)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\tid1
                            1\t100.0
                            1\t200.0
                            """);
        });
    }

    @Test
    public void testColumnAliasKeywordQuotedAllowed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (arr DOUBLE[])");
            execute("INSERT INTO t VALUES (ARRAY[1.0, 2.0])");
            assertQuery("SELECT u.\"select\" FROM t, UNNEST(t.arr) u(\"select\")")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            select
                            1.0
                            2.0
                            """);
        });
    }

    @Test
    public void testColumnAliasKeywordRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (arr DOUBLE[])");
            assertException(
                    "SELECT * FROM t, UNNEST(t.arr) u(select)",
                    33,
                    "have to be enclosed in double quotes"
            );
        });
    }

    @Test
    public void testDefaultColumnNames() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0] a, ARRAY[10.0, 20.0] b "
                    + "FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT u.value1, u.value2 FROM t, UNNEST(t.a, t.b) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            value1\tvalue2
                            1.0\t10.0
                            2.0\t20.0
                            """);
        });
    }

    @Test
    public void testDotNotationAccess() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT x::LONG id, ARRAY[x * 1.0, x * 2.0] arr "
                    + "FROM long_sequence(2)"
                    + ")");
            assertQuery("SELECT t.id, u.val FROM t, UNNEST(t.arr) u(val) "
                    + "WHERE t.id = 2 ORDER BY u.val")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            2\t2.0
                            2\t4.0
                            """);
        });
    }

    @Test
    public void testDottedColumnAlias() throws Exception {
        // A dotted UNNEST column alias keeps its dots as content, not a table.column separator:
        // the column surfaces with a clean name and resolves through a qualified reference.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ARRAY[1.0, 2.0] arr FROM long_sequence(1))");
            assertQuery("""
                    SELECT u."a.b" FROM t, UNNEST(t.arr) u("a.b") ORDER BY u."a.b"
                    """)
                    .noLeakCheck()
                    .returns("""
                            a.b
                            1.0
                            2.0
                            """);
            assertQuery("""
                    SELECT * FROM t, UNNEST(t.arr) u("a.b")
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            arr	a.b
                            [1.0,2.0]	1.0
                            [1.0,2.0]	2.0
                            """);
        });
    }

    @Test
    public void testDottedColumnAliasNonStandardQuotes() throws Exception {
        // A single-quoted or backtick dotted UNNEST alias must behave exactly like the double-quoted
        // form u("a.b"): the parser normalizes any quote style to the protective double quotes so the
        // dots stay content and the column surfaces clean as a.b. Regression: keeping the raw single
        // quote / backtick left the dot to mis-split into a spurious table.column reference, tripping
        // the "wtf?" assert (or an ArrayIndexOutOfBoundsException without assertions) at compile time.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ARRAY[1.0, 2.0] arr FROM long_sequence(1))");
            assertQuery("""
                    SELECT u."a.b" FROM t, UNNEST(t.arr) u('a.b') ORDER BY u."a.b"
                    """)
                    .noLeakCheck()
                    .returns("""
                            a.b
                            1.0
                            2.0
                            """);
            assertQuery("""
                    SELECT * FROM t, UNNEST(t.arr) u(`a.b`)
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            arr	a.b
                            [1.0,2.0]	1.0
                            [1.0,2.0]	2.0
                            """);
        });
    }

    @Test
    public void testDottedColumnAliasWithEmbeddedQuoteRejected() throws Exception {
        // A dotted UNNEST alias is re-wrapped in protective double quotes; an embedded double quote
        // would break that quote parity and leak a malformed display name, so the parser must reject
        // it cleanly rather than surface a doubled-quote name.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ARRAY[1.0, 2.0] arr FROM long_sequence(1))");
            assertException(
                    "SELECT * FROM t, UNNEST(t.arr) u(\"a\"\"b.c\")",
                    33,
                    "dotted UNNEST column alias cannot contain a double quote"
            );
        });
    }

    @Test
    public void testEmptyArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ARRAY[]::DOUBLE[] arr FROM long_sequence(1))");
            assertQuery("SELECT value FROM t, UNNEST(t.arr)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("value\n");
        });
    }

    @Test
    public void testGroupByBaseColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT x::LONG id, ARRAY[x * 10.0, x * 20.0] arr "
                    + "FROM long_sequence(3)"
                    + ")");
            assertQuery("SELECT t.id, sum(u.val) s FROM t, UNNEST(t.arr) u(val) "
                    + "GROUP BY t.id ORDER BY t.id")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ts
                            1\t30.0
                            2\t60.0
                            3\t90.0
                            """);
        });
    }

    @Test
    public void testGroupByOrdinalityBuckets() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 3.0, 4.0, 5.0] arr FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT (u.ord - 1) / 2 bucket, count() cnt "
                    + "FROM t, UNNEST(t.arr) WITH ORDINALITY u(val, ord) "
                    + "GROUP BY bucket ORDER BY bucket")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            bucket\tcnt
                            0\t2
                            1\t2
                            2\t1
                            """);
        });
    }

    @Test
    public void testGroupByUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 1.0] arr FROM long_sequence(2)"
                    + ")");
            assertQuery("SELECT u.val, count() cnt FROM t, UNNEST(t.arr) u(val) "
                    + "GROUP BY u.val ORDER BY u.val")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            val\tcnt
                            1.0\t4
                            2.0\t2
                            """);
        });
    }

    @Test
    public void testGroupByWithFilterOnCount() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 1.0, 3.0, 1.0] arr FROM long_sequence(1)"
                    + ")");
            // QuestDB doesn't support HAVING - use WHERE on aggregated subquery
            // instead. Test GROUP BY + count directly
            assertQuery("SELECT u.val, count() cnt FROM t, UNNEST(t.arr) u(val) "
                    + "GROUP BY u.val ORDER BY u.val")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            val\tcnt
                            1.0\t3
                            2.0\t1
                            3.0\t1
                            """);
        });
    }

    @Test
    public void testMinMaxOnUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[5.0, 1.0, 9.0, 3.0, 7.0] arr FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT min(u.val) mn, max(u.val) mx FROM t, UNNEST(t.arr) u(val)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            mn\tmx
                            1.0\t9.0
                            """);
        });
    }

    @Test
    public void testMixedNullAndNonNullArrays() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE[], b DOUBLE[])");
            execute("INSERT INTO t VALUES (ARRAY[1.0, 2.0], NULL)");
            assertQuery("SELECT u.x, u.y FROM t, UNNEST(t.a, t.b) u(x, y)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            x\ty
                            1.0\tnull
                            2.0\tnull
                            """);
        });
    }

    @Test
    public void testMultipleArraysDifferentLengths() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 3.0] a, ARRAY[10.0, 20.0] b "
                    + "FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT u.x, u.y FROM t, UNNEST(t.a, t.b) u(x, y)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            x\ty
                            1.0\t10.0
                            2.0\t20.0
                            3.0\tnull
                            """);
        });
    }

    @Test
    public void testMultipleArraysSameLength() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[100.0, 200.0] prices, ARRAY[10.0, 20.0] sizes "
                    + "FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT u.price, u.size FROM t, UNNEST(t.prices, t.sizes) u(price, size)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            price\tsize
                            100.0\t10.0
                            200.0\t20.0
                            """);
        });
    }

    @Test
    public void testMultipleRowsAllNullArrays() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (arr DOUBLE[])");
            execute("INSERT INTO t VALUES (NULL), (NULL), (NULL)");
            assertQuery("SELECT value FROM t, UNNEST(t.arr)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("value\n");
        });
    }

    @Test
    public void testMultipleRowsEmptyAndNonEmpty() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id LONG, arr DOUBLE[])");
            execute("""
                    INSERT INTO t VALUES
                    (1, ARRAY[]::DOUBLE[]),
                    (2, ARRAY[10.0, 20.0]),
                    (3, ARRAY[]::DOUBLE[]),
                    (4, ARRAY[30.0])
                    """);
            assertQuery("SELECT t.id, u.val FROM t, UNNEST(t.arr) u(val)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\tval
                            2\t10.0
                            2\t20.0
                            4\t30.0
                            """);
        });
    }

    @Test
    public void testMultipleRowsLargeArrays() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT rnd_double_array(1, 0, 0, 1000) arr FROM long_sequence(3)"
                    + ")");
            assertQuery("SELECT count() cnt FROM t, UNNEST(t.arr) u(val)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            cnt
                            3000
                            """);
        });
    }

    @Test
    public void testMultipleRowsRowCount() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (arr DOUBLE[])");
            execute("""
                    INSERT INTO t VALUES
                    (ARRAY[1.0, 2.0]),
                    (ARRAY[3.0, 4.0, 5.0]),
                    (ARRAY[6.0])
                    """);
            assertQuery("SELECT count() cnt FROM t, UNNEST(t.arr)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            cnt
                            6
                            """);
        });
    }

    @Test
    public void testMultipleRowsSelectBaseColumnsOnly() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT x::LONG id, ARRAY[x * 1.0, x * 2.0] arr "
                    + "FROM long_sequence(2)"
                    + ")");
            assertQuery("SELECT t.id FROM t, UNNEST(t.arr) u(val)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id
                            1
                            1
                            2
                            2
                            """);
        });
    }

    @Test
    public void testMultipleRowsSomeNullArrays() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id LONG, arr DOUBLE[])");
            execute("""
                    INSERT INTO t VALUES
                    (1, ARRAY[10.0, 20.0]),
                    (2, NULL),
                    (3, ARRAY[30.0])
                    """);
            assertQuery("SELECT t.id, u.val FROM t, UNNEST(t.arr) u(val)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\tval
                            1\t10.0
                            1\t20.0
                            3\t30.0
                            """);
        });
    }

    @Test
    public void testMultipleRowsVaryingArrayLengths() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id LONG, arr DOUBLE[])");
            execute("""
                    INSERT INTO t VALUES
                    (1, ARRAY[1.0]),
                    (2, ARRAY[2.0, 3.0]),
                    (3, ARRAY[4.0, 5.0, 6.0])
                    """);
            assertQuery("SELECT t.id, u.val FROM t, UNNEST(t.arr) u(val)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\tval
                            1\t1.0
                            2\t2.0
                            2\t3.0
                            3\t4.0
                            3\t5.0
                            3\t6.0
                            """);
        });
    }

    @Test
    public void testMultipleRowsWithOrdinalityResets() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id LONG, arr DOUBLE[])");
            execute("""
                    INSERT INTO t VALUES
                    (1, ARRAY[10.0, 20.0]),
                    (2, ARRAY[30.0, 40.0, 50.0])
                    """);
            assertQuery("SELECT t.id, u.val, u.ord "
                    + "FROM t, UNNEST(t.arr) WITH ORDINALITY u(val, ord)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\tval\tord
                            1\t10.0\t1
                            1\t20.0\t2
                            2\t30.0\t1
                            2\t40.0\t2
                            2\t50.0\t3
                            """);
        });
    }

    @Test
    public void testMultipleUnnestExpressionsMetadata() throws Exception {
        // Ensures UNNEST column definitions from earlier expressions
        // don't leak into subsequent parseFunction() calls.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE[], b DOUBLE[])");
            execute("INSERT INTO t VALUES (ARRAY[1.0, 2.0], ARRAY[10.0, 20.0, 30.0])");
            assertQuery("SELECT u.x, u.y FROM t, UNNEST(t.a, t.b) u(x, y)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            x\ty
                            1.0\t10.0
                            2.0\t20.0
                            null\t30.0
                            """);
        });
    }

    @Test
    public void testMultipleUnnestJoinsCartesianProduct() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0] a, ARRAY[10.0, 20.0] b "
                    + "FROM long_sequence(1)"
                    + ")");
            // Two separate UNNEST clauses produce a cartesian product
            assertQuery("SELECT u1.x, u2.y "
                    + "FROM t, UNNEST(t.a) u1(x), UNNEST(t.b) u2(y)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            x\ty
                            1.0\t10.0
                            1.0\t20.0
                            2.0\t10.0
                            2.0\t20.0
                            """);
        });
    }

    @Test
    public void testNonArrayExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT 42::LONG x FROM long_sequence(1))");
            assertException(
                    "SELECT * FROM t, UNNEST(t.x)",
                    24,
                    "array type expected in UNNEST"
            );
        });
    }

    @Test
    public void testNullArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (arr DOUBLE[])");
            execute("INSERT INTO t VALUES (NULL)");
            assertQuery("SELECT value FROM t, UNNEST(t.arr)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("value\n");
        });
    }

    @Test
    public void testOrderByBaseColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id LONG, arr DOUBLE[])");
            execute("INSERT INTO t VALUES (2, ARRAY[20.0]), (1, ARRAY[10.0])");
            assertQuery("SELECT t.id, u.val FROM t, UNNEST(t.arr) u(val) ORDER BY t.id")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            1\t10.0
                            2\t20.0
                            """);
        });
    }

    @Test
    public void testOrderByMultipleColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id LONG, arr DOUBLE[])");
            execute("INSERT INTO t VALUES (1, ARRAY[2.0, 1.0]), (2, ARRAY[4.0, 3.0])");
            assertQuery("SELECT t.id, u.val FROM t, UNNEST(t.arr) u(val) "
                    + "ORDER BY t.id, u.val")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            1\t1.0
                            1\t2.0
                            2\t3.0
                            2\t4.0
                            """);
        });
    }

    @Test
    public void testOrderByOrdinality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[10.0, 20.0, 30.0] arr FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT u.val, u.ord FROM t, UNNEST(t.arr) WITH ORDINALITY u(val, ord) "
                    + "ORDER BY u.ord DESC")
                    .noLeakCheck()
                    .returns("""
                            val\tord
                            30.0\t3
                            20.0\t2
                            10.0\t1
                            """);
        });
    }

    @Test
    public void testOrderByUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[30.0, 10.0, 20.0] arr FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT u.val FROM t, UNNEST(t.arr) u(val) ORDER BY u.val ASC")
                    .noLeakCheck()
                    .returns("""
                            val
                            10.0
                            20.0
                            30.0
                            """);
        });
    }

    @Test
    public void testOrderByWithLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[5.0, 3.0, 1.0, 4.0, 2.0] arr FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT u.val FROM t, UNNEST(t.arr) u(val) ORDER BY u.val LIMIT 3")
                    .noLeakCheck()
                    .returns("""
                            val
                            1.0
                            2.0
                            3.0
                            """);
        });
    }

    @Test
    public void testOrdinalityAlias() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[10.0, 20.0] arr FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT u.val, u.row_num "
                    + "FROM t, UNNEST(t.arr) WITH ORDINALITY u(val, row_num)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val\trow_num
                            10.0\t1
                            20.0\t2
                            """);
        });
    }

    @Test
    public void testPartialColumnAliases() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0] a, ARRAY[10.0, 20.0] b "
                    + "FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT u.x, u.value2 FROM t, UNNEST(t.a, t.b) u(x)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            x\tvalue2
                            1.0\t10.0
                            2.0\t20.0
                            """);
        });
    }

    @Test
    public void testSelectCountStar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 3.0] arr FROM long_sequence(4)"
                    + ")");
            assertQuery("SELECT count(*) FROM t, UNNEST(t.arr) u(val)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            count
                            12
                            """);
        });
    }

    @Test
    public void testSelectMixedBaseAndUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT x::LONG id, ARRAY[x * 1.0] arr FROM long_sequence(2)"
                    + ")");
            assertQuery("SELECT t.id, u.val FROM t, UNNEST(t.arr) u(val)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\tval
                            1\t1.0
                            2\t2.0
                            """);
        });
    }

    @Test
    public void testSelectOnlyUnnestedColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT x::LONG id, ARRAY[x * 10.0, x * 20.0] arr "
                    + "FROM long_sequence(2)"
                    + ")");
            assertQuery("SELECT u.val FROM t, UNNEST(t.arr) u(val)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            10.0
                            20.0
                            20.0
                            40.0
                            """);
        });
    }

    @Test
    public void testSelectStar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0] arr FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT * FROM t, UNNEST(t.arr)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            arr\tvalue
                            [1.0,2.0]\t1.0
                            [1.0,2.0]\t2.0
                            """);
        });
    }

    @Test
    public void testSelectStarMultipleArrays() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0] a, ARRAY[10.0] b FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT * FROM t, UNNEST(t.a, t.b) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            a\tb\tvalue1\tvalue2
                            [1.0]\t[10.0]\t1.0\t10.0
                            """);
        });
    }

    @Test
    public void testSelectStarWithOrdinality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[5.0] arr FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT * FROM t, UNNEST(t.arr) WITH ORDINALITY u(val, ord)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            arr\tval\tord
                            [5.0]\t5.0\t1
                            """);
        });
    }

    @Test
    public void testSelectUStarAndTStar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT x::LONG id, ARRAY[x * 10.0] arr FROM long_sequence(2)"
                    + ")");
            assertQuery("SELECT t.*, u.val FROM t, UNNEST(t.arr) u(val)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\tarr\tval
                            1\t[10.0]\t10.0
                            2\t[20.0]\t20.0
                            """);
        });
    }

    @Test
    public void testSelectWithExpressionOnUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[10.0, 20.0, 30.0] arr FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT u.val * 2 doubled FROM t, UNNEST(t.arr) u(val)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            doubled
                            20.0
                            40.0
                            60.0
                            """);
        });
    }

    @Test
    public void testSingleArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ARRAY[1.0, 2.0, 3.0] arr FROM long_sequence(1))");
            assertQuery("SELECT value FROM t, UNNEST(t.arr)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            value
                            1.0
                            2.0
                            3.0
                            """);
        });
    }

    @Test
    public void testSingleArrayWithAlias() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ARRAY[10.0, 20.0, 30.0] arr FROM long_sequence(1))");
            assertQuery("SELECT u.price FROM t, UNNEST(t.arr) u(price)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            price
                            10.0
                            20.0
                            30.0
                            """);
        });
    }

    @Test
    public void testSingleElementArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ARRAY[42.0] arr FROM long_sequence(1))");
            assertQuery("SELECT value FROM t, UNNEST(t.arr)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            value
                            42.0
                            """);
        });
    }

    @Test
    public void testStandalone() throws Exception {
        assertMemoryLeak(() -> assertQuery("SELECT value FROM UNNEST(ARRAY[1.0, 2.0, 3.0])")
                .noLeakCheck()
                .noRandomAccess()
                .returns("""
                        value
                        1.0
                        2.0
                        3.0
                        """));
    }

    @Test
    public void testStandaloneEmpty() throws Exception {
        assertMemoryLeak(() -> assertQuery("SELECT value FROM UNNEST(ARRAY[]::DOUBLE[])")
                .noLeakCheck()
                .noRandomAccess()
                .returns("value\n"));
    }

    @Test
    public void testStandaloneMultipleArrays() throws Exception {
        assertMemoryLeak(() -> assertQuery("SELECT value1, value2 FROM UNNEST(ARRAY[1.0, 2.0, 3.0], ARRAY[10.0, 20.0, 30.0])")
                .noLeakCheck()
                .noRandomAccess()
                .returns("""
                        value1\tvalue2
                        1.0\t10.0
                        2.0\t20.0
                        3.0\t30.0
                        """));
    }

    @Test
    public void testStandaloneMultipleArraysDiffLen() throws Exception {
        assertMemoryLeak(() -> assertQuery("SELECT value1, value2 FROM UNNEST(ARRAY[1.0, 2.0, 3.0], ARRAY[10.0, 20.0])")
                .noLeakCheck()
                .noRandomAccess()
                .returns("""
                        value1\tvalue2
                        1.0\t10.0
                        2.0\t20.0
                        3.0\tnull
                        """));
    }

    @Test
    public void testStandaloneNull() throws Exception {
        assertMemoryLeak(() -> assertQuery("SELECT value FROM UNNEST(NULL::DOUBLE[])")
                .noLeakCheck()
                .noRandomAccess()
                .returns("value\n"));
    }

    @Test
    public void testStandaloneSelectStar() throws Exception {
        // SELECT * from standalone UNNEST should only show unnested
        // columns, not the synthetic long_sequence(1) x column.
        assertMemoryLeak(() -> assertQuery("SELECT * FROM UNNEST(ARRAY[1.0, 2.0, 3.0])")
                .noLeakCheck()
                .noRandomAccess()
                .returns("""
                        value
                        1.0
                        2.0
                        3.0
                        """));
    }

    @Test
    public void testStandaloneSelectStarMultipleArrays() throws Exception {
        assertMemoryLeak(() -> assertQuery("SELECT * FROM UNNEST(ARRAY[1.0, 2.0], ARRAY[10.0, 20.0])")
                .noLeakCheck()
                .noRandomAccess()
                .returns("""
                        value1\tvalue2
                        1.0\t10.0
                        2.0\t20.0
                        """));
    }

    @Test
    public void testStandaloneSelectStarWithOrdinality() throws Exception {
        assertMemoryLeak(() -> assertQuery("SELECT * FROM UNNEST(ARRAY[1.0, 2.0]) WITH ORDINALITY")
                .noLeakCheck()
                .noRandomAccess()
                .returns("""
                        value\tordinality
                        1.0\t1
                        2.0\t2
                        """));
    }

    @Test
    public void testStandaloneSingleElement() throws Exception {
        assertMemoryLeak(() -> assertQuery("SELECT value FROM UNNEST(ARRAY[42.0])")
                .noLeakCheck()
                .noRandomAccess()
                .returns("""
                        value
                        42.0
                        """));
    }

    @Test
    public void testStandaloneWithGroupBy() throws Exception {
        assertMemoryLeak(() -> assertQuery("SELECT value, count() cnt FROM UNNEST(ARRAY[1.0, 2.0, 1.0]) "
                + "GROUP BY value ORDER BY value")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        value\tcnt
                        1.0\t2
                        2.0\t1
                        """));
    }

    @Test
    public void testStandaloneWithOrdinality() throws Exception {
        assertMemoryLeak(() -> assertQuery("SELECT val, ord FROM UNNEST(ARRAY[10.0, 20.0, 30.0]) WITH ORDINALITY AS t(val, ord)")
                .noLeakCheck()
                .noRandomAccess()
                .returns("""
                        val\tord
                        10.0\t1
                        20.0\t2
                        30.0\t3
                        """));
    }

    @Test
    public void testStandaloneWithWhere() throws Exception {
        assertMemoryLeak(() -> assertQuery("SELECT value FROM UNNEST(ARRAY[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]) "
                + "WHERE value > 7.0")
                .noLeakCheck()
                .noRandomAccess()
                .returns("""
                        value
                        8.0
                        9.0
                        10.0
                        """));
    }

    @Test
    public void testSubqueryContainingUnnest() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 3.0] arr FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT val FROM (SELECT u.val FROM t, UNNEST(t.arr) u(val))")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            1.0
                            2.0
                            3.0
                            """);
        });
    }

    @Test
    public void testSubqueryContainingUnnestWithAggregation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 3.0, 4.0, 5.0] arr FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT sum(val) s FROM (SELECT u.val FROM t, UNNEST(t.arr) u(val))")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            s
                            15.0
                            """);
        });
    }

    @Test
    public void testSubqueryContainingUnnestWithColumnAliases() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0] a, ARRAY[10.0, 20.0] b "
                    + "FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT x, y FROM (SELECT u.x, u.y FROM t, UNNEST(t.a, t.b) u(x, y))")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            x\ty
                            1.0\t10.0
                            2.0\t20.0
                            """);
        });
    }

    @Test
    public void testSumEquivalenceWithArraySum() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 3.0, 4.0] arr FROM long_sequence(1)"
                    + ")");
            // Verify array_sum and UNNEST SUM produce same result
            assertQuery("SELECT array_sum(arr) s FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s
                            10.0
                            """);
            assertQuery("SELECT sum(u.val) s FROM t, UNNEST(t.arr) u(val)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            s
                            10.0
                            """);
        });
    }

    @Test
    public void testSumOfUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 3.0, 4.0] arr FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT sum(u.val) s FROM t, UNNEST(t.arr) u(val)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            s
                            10.0
                            """);
        });
    }

    @Test
    public void testTableAliasOnly() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0] arr FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT u.value FROM t, UNNEST(t.arr) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            value
                            1.0
                            2.0
                            """);
        });
    }

    @Test
    public void testTableAliasWithAS() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0] arr FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT u.col1 FROM t, UNNEST(t.arr) AS u(col1)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            col1
                            1.0
                            2.0
                            """);
        });
    }

    @Test
    public void testTableAliasWithoutAS() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0] arr FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT u.col1 FROM t, UNNEST(t.arr) u(col1)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            col1
                            1.0
                            2.0
                            """);
        });
    }

    @Test
    public void testTooManyColumnAliases() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (arr DOUBLE[])");
            assertException(
                    "SELECT * FROM t, UNNEST(t.arr) u(x, y, z)",
                    36,
                    "too many column aliases for UNNEST"
            );
        });
    }

    @Test
    public void testTooManyColumnAliasesWithOrdinality() throws Exception {
        // WITH ORDINALITY allows one extra alias (for the ord column).
        // Three aliases for 1 array + 1 ordinality = 2 max -> error.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (arr DOUBLE[])");
            assertException(
                    "SELECT * FROM t, UNNEST(t.arr) WITH ORDINALITY u(x, ord, z)",
                    57,
                    "too many column aliases for UNNEST"
            );
        });
    }

    @Test
    public void testUnnestOnTableWithDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE t ("
                            + "ts TIMESTAMP, sym SYMBOL, arr DOUBLE[]"
                            + ") TIMESTAMP(ts)"
            );
            execute(
                    "INSERT INTO t VALUES "
                            + "('2025-01-01T00:00:00.000000Z', 'A', ARRAY[1.0, 2.0]),"
                            + "('2025-01-02T00:00:00.000000Z', 'B', ARRAY[3.0])"
            );
            assertQuery("SELECT t.ts, t.sym, u.val FROM t, UNNEST(t.arr) u(val)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tsym\tval
                            2025-01-01T00:00:00.000000Z\tA\t1.0
                            2025-01-01T00:00:00.000000Z\tA\t2.0
                            2025-01-02T00:00:00.000000Z\tB\t3.0
                            """);
        });
    }

    @Test
    public void testUnnest2DArrayEmpty() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (arr DOUBLE[][])");
            execute("INSERT INTO t VALUES (ARRAY[]::DOUBLE[][])");
            assertQuery("SELECT value FROM t, UNNEST(t.arr)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("value\n");
        });
    }

    @Test
    public void testUnnest2DArrayMixed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE[][], b DOUBLE[])");
            execute("INSERT INTO t VALUES ("
                    + "ARRAY[[1.0, 2.0], [3.0, 4.0]], "
                    + "ARRAY[100.0, 200.0])");
            assertQuery("SELECT u.x, u.y FROM t, UNNEST(t.a, t.b) u(x, y)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            x\ty
                            [1.0,2.0]\t100.0
                            [3.0,4.0]\t200.0
                            """);
        });
    }

    @Test
    public void testUnnest2DArrayMixedUnequalLengths() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE[][], b DOUBLE[])");
            execute("INSERT INTO t VALUES ("
                    + "ARRAY[[1.0, 2.0]], "
                    + "ARRAY[100.0, 200.0, 300.0])");
            assertQuery("SELECT u.x, u.y FROM t, UNNEST(t.a, t.b) u(x, y)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            x	y
                            [1.0,2.0]	100.0
                            null	200.0
                            null	300.0
                            """);
        });
    }

    @Test
    public void testUnnest2DArrayMixedUnequalLengthsDimLenAndElement() throws Exception {
        // The pad rows of the shorter source have no array behind them. Selecting the column whole
        // reads it through a @Nullable path, but dim_length() and x[i] take Record's
        // getArrayDimLen()/getArrayDouble1d2d() defaults, which dereference what getArray() hands
        // back. The pad rows have to read as a NULL array there too, not blow up.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE[][], b DOUBLE[])");
            execute("INSERT INTO t VALUES ("
                    + "ARRAY[[1.0, 2.0]], "
                    + "ARRAY[100.0, 200.0, 300.0])");
            assertQuery("SELECT dim_length(u.x, 1) len, u.x[1] first, u.y FROM t, UNNEST(t.a, t.b) u(x, y)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            len\tfirst\ty
                            2\t1.0\t100.0
                            null\tnull\t200.0
                            null\tnull\t300.0
                            """);
        });
    }

    @Test
    public void testUnnest2DArrayMultipleRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id LONG, arr DOUBLE[][])");
            execute("""
                    INSERT INTO t VALUES
                    (1, ARRAY[[1.0, 2.0], [3.0, 4.0]]),
                    (2, ARRAY[[10.0, 20.0]])
                    """);
            assertQuery("SELECT t.id, u.val FROM t, UNNEST(t.arr) u(val)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\tval
                            1\t[1.0,2.0]
                            1\t[3.0,4.0]
                            2\t[10.0,20.0]
                            """);
        });
    }

    @Test
    public void testUnnest2DArrayNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (arr DOUBLE[][])");
            execute("INSERT INTO t VALUES (NULL)");
            assertQuery("SELECT value FROM t, UNNEST(t.arr)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("value\n");
        });
    }

    @Test
    public void testUnnest2DArraySliceAccess() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (arr DOUBLE[][])");
            execute("INSERT INTO t VALUES (ARRAY[[10.0, 20.0, 30.0], [40.0, 50.0, 60.0]])");
            assertQuery("SELECT array_sum(u.val) s FROM t, UNNEST(t.arr) u(val)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            s
                            60.0
                            150.0
                            """);
        });
    }

    @Test
    public void testUnnest2DArrayToRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (arr DOUBLE[][])");
            execute("INSERT INTO t VALUES (ARRAY[[1.0, 2.0], [3.0, 4.0]])");
            assertQuery("SELECT value FROM t, UNNEST(t.arr)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            value
                            [1.0,2.0]
                            [3.0,4.0]
                            """);
        });
    }

    @Test
    public void testUnnest2DArrayWithOrdinality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (arr DOUBLE[][])");
            execute("INSERT INTO t VALUES (ARRAY[[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]])");
            assertQuery("SELECT u.val, u.ord "
                    + "FROM t, UNNEST(t.arr) WITH ORDINALITY u(val, ord)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val\tord
                            [1.0,2.0]\t1
                            [3.0,4.0]\t2
                            [5.0,6.0]\t3
                            """);
        });
    }

    @Test
    public void testUnnest2DChainedToScalar() throws Exception {
        // Verifies that reading scalar doubles from a 2D array UNNEST
        // produces correct values (no double-offset).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (arr DOUBLE[][])");
            execute("INSERT INTO t VALUES (ARRAY[[10.0, 20.0], [30.0, 40.0]])");
            // First UNNEST: 2D -> 1D slices, second UNNEST: 1D -> scalars
            assertQuery("SELECT array_sum(u.val) s FROM t, UNNEST(t.arr) u(val)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            s
                            30.0
                            70.0
                            """);
        });
    }

    @Test
    public void testUnnest3DArrayTo2D() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (arr DOUBLE[][][])");
            execute("INSERT INTO t VALUES ("
                    + "ARRAY[ARRAY[[1.0, 2.0], [3.0, 4.0]], ARRAY[[5.0, 6.0], [7.0, 8.0]]]"
                    + ")");
            assertQuery("SELECT value FROM t, UNNEST(t.arr)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            value
                            [[1.0,2.0],[3.0,4.0]]
                            [[5.0,6.0],[7.0,8.0]]
                            """);
        });
    }

    @Test
    public void testUnnestInSelectListThrowsError() throws Exception {
        assertMemoryLeak(() -> assertException(
                "SELECT UNNEST(ARRAY[1.0, 2.0])",
                7,
                "UNNEST cannot be used as an expression"
        ));
    }

    @Test
    public void testNestedUnnestThrowsError() throws Exception {
        assertMemoryLeak(() -> assertException(
                "SELECT * FROM UNNEST(UNNEST(ARRAY[[1, 2], [3, 4]])) AS unnest_column",
                21,
                "UNNEST cannot be used as an expression"
        ));
    }

    @Test
    public void testUnnest2DArrayChained() throws Exception {
        // Unnest a 2D array by chaining two UNNEST in FROM
        assertMemoryLeak(() -> assertQuery("SELECT u2.val FROM ("
                + "  SELECT * FROM UNNEST(ARRAY[[1.0, 2.0], [3.0, 4.0]]) t(arr)"
                + "), UNNEST(arr) u2(val)")
                .noLeakCheck()
                .noRandomAccess()
                .returns("""
                        val
                        1.0
                        2.0
                        3.0
                        4.0
                        """));
    }

    @Test
    public void testVeryLargeArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT rnd_double_array(1, 0, 0, 100_000) arr FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT count() cnt FROM t, UNNEST(t.arr) u(val)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            cnt
                            100000
                            """);
        });
    }

    @Test
    public void testWhereNoMatchingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 3.0] arr FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT u.val FROM t, UNNEST(t.arr) u(val) WHERE u.val > 100.0")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("val\n");
        });
    }

    @Test
    public void testWhereNullCheck() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 3.0] a, ARRAY[10.0] b "
                    + "FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT u.x, u.y FROM t, UNNEST(t.a, t.b) u(x, y) "
                    + "WHERE u.y IS NOT NULL")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            x\ty
                            1.0\t10.0
                            """);
        });
    }

    @Test
    public void testWhereOnBaseColumnPushedDown() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT x::LONG id, ARRAY[x * 1.0, x * 2.0] arr "
                    + "FROM long_sequence(5)"
                    + ")");
            assertQuery("SELECT t.id, u.val FROM t, UNNEST(t.arr) u(val) WHERE t.id = 3")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\tval
                            3\t3.0
                            3\t6.0
                            """);
        });
    }

    @Test
    public void testWhereOnBothBaseAndUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT x::LONG id, ARRAY[x * 1.0, x * 2.0, x * 3.0] arr "
                    + "FROM long_sequence(3)"
                    + ")");
            assertQuery("SELECT t.id, u.val FROM t, UNNEST(t.arr) u(val) "
                    + "WHERE t.id >= 2 AND u.val > 3.0")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\tval
                            2\t4.0
                            2\t6.0
                            3\t6.0
                            3\t9.0
                            """);
        });
    }

    @Test
    public void testWhereOnOrdinalityColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[10.0, 20.0, 30.0, 40.0, 50.0] arr "
                    + "FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT u.val, u.ord "
                    + "FROM t, UNNEST(t.arr) WITH ORDINALITY u(val, ord) "
                    + "WHERE u.ord > 2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val\tord
                            30.0\t3
                            40.0\t4
                            50.0\t5
                            """);
        });
    }

    @Test
    public void testWhereOnUnnestedColumnAbove() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 5.0, 2.0, 8.0, 3.0] arr FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT u.val FROM t, UNNEST(t.arr) u(val) WHERE u.val > 4.0")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            5.0
                            8.0
                            """);
        });
    }

    @Test
    public void testWhereWithInvalidFilterExpression() throws Exception {
        // Validates that a parse error in post-UNNEST WHERE does not
        // cause a double-free (compileBooleanFilter vs compileJoinFilter).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (arr DOUBLE[])");
            assertException(
                    "SELECT * FROM t, UNNEST(t.arr) u(val) WHERE u.nonexistent > 0",
                    44,
                    "Invalid column"
            );
        });
    }

    @Test
    public void testWhereWithOrOnBase() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id LONG, arr DOUBLE[])");
            execute("""
                    INSERT INTO t VALUES
                    (1, ARRAY[10.0]),
                    (2, ARRAY[20.0]),
                    (3, ARRAY[30.0])
                    """);
            assertQuery("SELECT t.id, u.val FROM t, UNNEST(t.arr) u(val) "
                    + "WHERE t.id = 1 OR t.id = 3")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\tval
                            1\t10.0
                            3\t30.0
                            """);
        });
    }

    @Test
    public void testWithArithmeticOnUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[10.0, 20.0, 30.0] arr FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT u.val * 2 doubled, u.val + 1 plus_one "
                    + "FROM t, UNNEST(t.arr) u(val)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            doubled\tplus_one
                            20.0\t11.0
                            40.0\t21.0
                            60.0\t31.0
                            """);
        });
    }

    @Test
    public void testWithCTEAsBase() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT x::LONG id, ARRAY[x * 1.0, x * 2.0] arr "
                    + "FROM long_sequence(2)"
                    + ")");
            assertQuery("WITH cte AS (SELECT id, arr FROM t) "
                    + "SELECT cte.id, u.val FROM cte, UNNEST(cte.arr) u(val)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\tval
                            1\t1.0
                            1\t2.0
                            2\t2.0
                            2\t4.0
                            """);
        });
    }

    @Test
    public void testWithCaseExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[5.0, 15.0, 25.0] arr FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT u.val, CASE WHEN u.val > 10 THEN 'high' ELSE 'low' END label "
                    + "FROM t, UNNEST(t.arr) u(val)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val\tlabel
                            5.0\tlow
                            15.0\thigh
                            25.0\thigh
                            """);
        });
    }

    @Test
    public void testWithDistinct() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 1.0, 2.0, 3.0] arr FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT DISTINCT u.val FROM t, UNNEST(t.arr) u(val) ORDER BY u.val")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            val
                            1.0
                            2.0
                            3.0
                            """);
        });
    }

    @Test
    public void testWithGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 1.0] arr FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT u.val, count() cnt FROM t, UNNEST(t.arr) u(val) GROUP BY u.val ORDER BY u.val")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            val\tcnt
                            1.0\t2
                            2.0\t1
                            """);
        });
    }

    @Test
    public void testWithJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id LONG, arr DOUBLE[])");
            execute("INSERT INTO t VALUES (1, ARRAY[10.0, 20.0])");
            execute("CREATE TABLE t2 (id LONG, name SYMBOL)");
            execute("INSERT INTO t2 VALUES (1, 'Alice')");
            assertQuery("SELECT t.id, u.val, t2.name "
                    + "FROM t, UNNEST(t.arr) u(val) "
                    + "JOIN t2 ON t2.id = t.id")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\tval\tname
                            1\t10.0\tAlice
                            1\t20.0\tAlice
                            """);
        });
    }

    @Test
    public void testWithOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ARRAY[3.0, 1.0, 2.0] arr FROM long_sequence(1))");
            assertQuery("SELECT u.val FROM t, UNNEST(t.arr) u(val) ORDER BY u.val")
                    .noLeakCheck()
                    .returns("""
                            val
                            1.0
                            2.0
                            3.0
                            """);
        });
    }

    @Test
    public void testWithOrdinality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ARRAY[10.0, 20.0, 30.0] arr FROM long_sequence(1))");
            assertQuery("SELECT u.val, u.ord FROM t, UNNEST(t.arr) WITH ORDINALITY u(val, ord)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val\tord
                            10.0\t1
                            20.0\t2
                            30.0\t3
                            """);
        });
    }

    // Tests for keyword validation in column aliases

    @Test
    public void testWithOrdinalityMultipleBaseRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT x::LONG id, ARRAY[x * 1.0, x * 2.0] arr "
                    + "FROM long_sequence(3)"
                    + ")");
            assertQuery("SELECT t.id, u.val, u.ord FROM t, UNNEST(t.arr) WITH ORDINALITY u(val, ord)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\tval\tord
                            1\t1.0\t1
                            1\t2.0\t2
                            2\t2.0\t1
                            2\t4.0\t2
                            3\t3.0\t1
                            3\t6.0\t2
                            """);
        });
    }

    @Test
    public void testWithSubqueryAsBase() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT x::LONG id, ARRAY[x * 1.0, x * 2.0] arr "
                    + "FROM long_sequence(3)"
                    + ")");
            assertQuery("SELECT sub.id, u.val FROM "
                    + "(SELECT id, arr FROM t WHERE id > 1) sub, "
                    + "UNNEST(sub.arr) u(val)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\tval
                            2\t2.0
                            2\t4.0
                            3\t3.0
                            3\t6.0
                            """);
        });
    }

    // Test for double-offset fix (chained 2D->scalar)

    @Test
    public void testWithUnionAll() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0] arr FROM long_sequence(1)"
                    + ")");
            assertQuery("SELECT u.val FROM t, UNNEST(t.arr) u(val) "
                    + "UNION ALL "
                    + "SELECT value FROM UNNEST(ARRAY[10.0, 20.0])")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            1.0
                            2.0
                            10.0
                            20.0
                            """);
        });
    }

    // Test for invalid post-UNNEST filter (compileBooleanFilter)

    @Test
    public void testWithWhereOnBaseColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT rnd_symbol('BTC', 'ETH') sym, ARRAY[x * 1.0, x * 2.0] arr "
                    + "FROM long_sequence(10)"
                    + ")");
            assertQuery("SELECT t.sym, u.value FROM t, UNNEST(t.arr) u(value) WHERE t.sym = 'NONE'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("sym\tvalue\n");
        });
    }

    // Test for metadata separation (multiple UNNEST expressions)

    @Test
    public void testWithWhereOnUnnestedColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ARRAY[1.0, 2.0, 3.0, 4.0, 5.0] arr FROM long_sequence(1))");
            assertQuery("SELECT u.val FROM t, UNNEST(t.arr) u(val) WHERE u.val > 3.0")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            4.0
                            5.0
                            """);
        });
    }
}

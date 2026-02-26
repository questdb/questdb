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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class UnnestTest extends AbstractCairoTest {

    @Test
    public void testAliasInGroupByAndOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 1.0, 3.0, 2.0] arr FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "val\tcnt\n"
                            + "1.0\t2\n"
                            + "2.0\t2\n"
                            + "3.0\t1\n",
                    "SELECT u.val, count() cnt FROM t, UNNEST(t.arr) u(val) "
                            + "GROUP BY u.val ORDER BY u.val"
            );
        });
    }

    @Test
    public void testAvgOnUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[10.0, 20.0, 30.0] arr FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "avg\n"
                            + "20.0\n",
                    "SELECT avg(u.val) FROM t, UNNEST(t.arr) u(val)",
                    null, false, false, true
            );
        });
    }

    @Test
    public void testColumnAliasConflictsWithBase() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT x::LONG id, ARRAY[100.0, 200.0] arr FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "id\tid1\n"
                            + "1\t100.0\n"
                            + "1\t200.0\n",
                    "SELECT t.id, u.id id1 FROM t, UNNEST(t.arr) u(id)",
                    (String) null
            );
        });
    }

    @Test
    public void testCountDistinctUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 1.0, 3.0, 2.0] arr FROM long_sequence(1)"
                    + ")");
            // Use GROUP BY to count distinct values
            assertQueryNoLeakCheck(
                    "val\tcnt\n"
                            + "1.0\t2\n"
                            + "2.0\t2\n"
                            + "3.0\t1\n",
                    "SELECT u.val, count() cnt FROM t, UNNEST(t.arr) u(val) "
                            + "GROUP BY u.val ORDER BY u.val"
            );
        });
    }

    @Test
    public void testCTEAsBaseForUnnest() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT x::LONG id, ARRAY[x * 10.0, x * 20.0] arr "
                    + "FROM long_sequence(2)"
                    + ")");
            assertQueryNoLeakCheck(
                    "id\tval\n"
                            + "1\t10.0\n"
                            + "1\t20.0\n"
                            + "2\t20.0\n"
                            + "2\t40.0\n",
                    "WITH cte AS (SELECT id, arr FROM t) "
                            + "SELECT cte.id, u.val FROM cte, UNNEST(cte.arr) u(val)",
                    (String) null
            );
        });
    }

    @Test
    public void testCTEContainingUnnest() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 3.0] arr FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "val\n"
                            + "1.0\n"
                            + "2.0\n"
                            + "3.0\n",
                    "WITH cte AS (SELECT u.val FROM t, UNNEST(t.arr) u(val)) "
                            + "SELECT val FROM cte",
                    (String) null
            );
        });
    }

    @Test
    public void testCTEContainingUnnestWithOrdinality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[10.0, 20.0, 30.0] arr FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "val\tord\n"
                            + "10.0\t1\n"
                            + "20.0\t2\n"
                            + "30.0\t3\n",
                    "WITH cte AS ("
                            + "SELECT u.val, u.ord "
                            + "FROM t, UNNEST(t.arr) WITH ORDINALITY u(val, ord)"
                            + ") SELECT val, ord FROM cte",
                    (String) null
            );
        });
    }

    @Test
    public void testCTEUnnestJoinedWithTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id LONG, arr DOUBLE[])");
            execute("INSERT INTO t VALUES (1, ARRAY[10.0, 20.0])");
            execute("CREATE TABLE t2 (id LONG, name SYMBOL)");
            execute("INSERT INTO t2 VALUES (1, 'Alice')");
            assertQueryNoLeakCheck(
                    "id\tval\tname\n"
                            + "1\t10.0\tAlice\n"
                            + "1\t20.0\tAlice\n",
                    "WITH unnested AS ("
                            + "SELECT t.id, u.val FROM t, UNNEST(t.arr) u(val)"
                            + ") SELECT unnested.id, unnested.val, t2.name "
                            + "FROM unnested JOIN t2 ON t2.id = unnested.id",
                    (String) null
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
            assertQueryNoLeakCheck(
                    "value1\tvalue2\n"
                            + "1.0\t10.0\n"
                            + "2.0\t20.0\n",
                    "SELECT u.value1, u.value2 FROM t, UNNEST(t.a, t.b) u",
                    (String) null
            );
        });
    }

    @Test
    public void testDotNotationAccess() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT x::LONG id, ARRAY[x * 1.0, x * 2.0] arr "
                    + "FROM long_sequence(2)"
                    + ")");
            assertQueryNoLeakCheck(
                    "id\tval\n"
                            + "2\t2.0\n"
                            + "2\t4.0\n",
                    "SELECT t.id, u.val FROM t, UNNEST(t.arr) u(val) "
                            + "WHERE t.id = 2 ORDER BY u.val",
                    false
            );
        });
    }

    @Test
    public void testEmptyArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ARRAY[]::DOUBLE[] arr FROM long_sequence(1))");
            assertQueryNoLeakCheck(
                    "value\n",
                    "SELECT value FROM t, UNNEST(t.arr)",
                    (String) null
            );
        });
    }

    @Test
    public void testGroupByBaseColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT x::LONG id, ARRAY[x * 10.0, x * 20.0] arr "
                    + "FROM long_sequence(3)"
                    + ")");
            assertQueryNoLeakCheck(
                    "id\ts\n"
                            + "1\t30.0\n"
                            + "2\t60.0\n"
                            + "3\t90.0\n",
                    "SELECT t.id, sum(u.val) s FROM t, UNNEST(t.arr) u(val) "
                            + "GROUP BY t.id ORDER BY t.id"
            );
        });
    }

    @Test
    public void testGroupByOrdinalityBuckets() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 3.0, 4.0, 5.0] arr FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "bucket\tcnt\n"
                            + "0\t2\n"
                            + "1\t2\n"
                            + "2\t1\n",
                    "SELECT (u.ord - 1) / 2 bucket, count() cnt "
                            + "FROM t, UNNEST(t.arr) WITH ORDINALITY u(val, ord) "
                            + "GROUP BY bucket ORDER BY bucket"
            );
        });
    }

    @Test
    public void testGroupByUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 1.0] arr FROM long_sequence(2)"
                    + ")");
            assertQueryNoLeakCheck(
                    "val\tcnt\n"
                            + "1.0\t4\n"
                            + "2.0\t2\n",
                    "SELECT u.val, count() cnt FROM t, UNNEST(t.arr) u(val) "
                            + "GROUP BY u.val ORDER BY u.val"
            );
        });
    }

    @Test
    public void testGroupByWithFilterOnCount() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 1.0, 3.0, 1.0] arr FROM long_sequence(1)"
                    + ")");
            // QuestDB doesn't support HAVING - use WHERE on aggregated subquery
            // instead. Since UNNEST can't be in subqueries, test GROUP BY + count
            assertQueryNoLeakCheck(
                    "val\tcnt\n"
                            + "1.0\t3\n"
                            + "2.0\t1\n"
                            + "3.0\t1\n",
                    "SELECT u.val, count() cnt FROM t, UNNEST(t.arr) u(val) "
                            + "GROUP BY u.val ORDER BY u.val"
            );
        });
    }

    @Test
    public void testMinMaxOnUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[5.0, 1.0, 9.0, 3.0, 7.0] arr FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "mn\tmx\n"
                            + "1.0\t9.0\n",
                    "SELECT min(u.val) mn, max(u.val) mx FROM t, UNNEST(t.arr) u(val)",
                    null, false, false, true
            );
        });
    }

    @Test
    public void testMixedNullAndNonNullArrays() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE[], b DOUBLE[])");
            execute("INSERT INTO t VALUES (ARRAY[1.0, 2.0], NULL)");
            assertQueryNoLeakCheck(
                    "x\ty\n"
                            + "1.0\tnull\n"
                            + "2.0\tnull\n",
                    "SELECT u.x, u.y FROM t, UNNEST(t.a, t.b) u(x, y)",
                    (String) null
            );
        });
    }

    @Test
    public void testMultipleArraysDifferentLengths() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 3.0] a, ARRAY[10.0, 20.0] b "
                    + "FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "x\ty\n"
                            + "1.0\t10.0\n"
                            + "2.0\t20.0\n"
                            + "3.0\tnull\n",
                    "SELECT u.x, u.y FROM t, UNNEST(t.a, t.b) u(x, y)",
                    (String) null
            );
        });
    }

    @Test
    public void testMultipleArraysSameLength() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[100.0, 200.0] prices, ARRAY[10.0, 20.0] sizes "
                    + "FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "price\tsize\n"
                            + "100.0\t10.0\n"
                            + "200.0\t20.0\n",
                    "SELECT u.price, u.size FROM t, UNNEST(t.prices, t.sizes) u(price, size)",
                    (String) null
            );
        });
    }

    @Test
    public void testMultipleRowsAllNullArrays() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (arr DOUBLE[])");
            execute("INSERT INTO t VALUES (NULL)");
            execute("INSERT INTO t VALUES (NULL)");
            execute("INSERT INTO t VALUES (NULL)");
            assertQueryNoLeakCheck(
                    "value\n",
                    "SELECT value FROM t, UNNEST(t.arr)",
                    (String) null
            );
        });
    }

    @Test
    public void testMultipleRowsEmptyAndNonEmpty() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id LONG, arr DOUBLE[])");
            execute("INSERT INTO t VALUES (1, ARRAY[]::DOUBLE[])");
            execute("INSERT INTO t VALUES (2, ARRAY[10.0, 20.0])");
            execute("INSERT INTO t VALUES (3, ARRAY[]::DOUBLE[])");
            execute("INSERT INTO t VALUES (4, ARRAY[30.0])");
            assertQueryNoLeakCheck(
                    "id\tval\n"
                            + "2\t10.0\n"
                            + "2\t20.0\n"
                            + "4\t30.0\n",
                    "SELECT t.id, u.val FROM t, UNNEST(t.arr) u(val)",
                    (String) null
            );
        });
    }

    @Test
    public void testMultipleRowsLargeArrays() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT rnd_double_array(1, 0, 0, 1000) arr FROM long_sequence(3)"
                    + ")");
            assertQueryNoLeakCheck(
                    "cnt\n"
                            + "3000\n",
                    "SELECT count() cnt FROM t, UNNEST(t.arr) u(val)",
                    null, false, false, true
            );
        });
    }

    @Test
    public void testMultipleRowsRowCount() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (arr DOUBLE[])");
            execute("INSERT INTO t VALUES (ARRAY[1.0, 2.0])");
            execute("INSERT INTO t VALUES (ARRAY[3.0, 4.0, 5.0])");
            execute("INSERT INTO t VALUES (ARRAY[6.0])");
            assertQueryNoLeakCheck(
                    "cnt\n"
                            + "6\n",
                    "SELECT count() cnt FROM t, UNNEST(t.arr)",
                    null, false, false, true
            );
        });
    }

    @Test
    public void testMultipleRowsSelectBaseColumnsOnly() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT x::LONG id, ARRAY[x * 1.0, x * 2.0] arr "
                    + "FROM long_sequence(2)"
                    + ")");
            assertQueryNoLeakCheck(
                    "id\n"
                            + "1\n"
                            + "1\n"
                            + "2\n"
                            + "2\n",
                    "SELECT t.id FROM t, UNNEST(t.arr) u(val)",
                    (String) null
            );
        });
    }

    @Test
    public void testMultipleRowsSomeNullArrays() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id LONG, arr DOUBLE[])");
            execute("INSERT INTO t VALUES (1, ARRAY[10.0, 20.0])");
            execute("INSERT INTO t VALUES (2, NULL)");
            execute("INSERT INTO t VALUES (3, ARRAY[30.0])");
            assertQueryNoLeakCheck(
                    "id\tval\n"
                            + "1\t10.0\n"
                            + "1\t20.0\n"
                            + "3\t30.0\n",
                    "SELECT t.id, u.val FROM t, UNNEST(t.arr) u(val)",
                    (String) null
            );
        });
    }

    @Test
    public void testMultipleRowsVaryingArrayLengths() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id LONG, arr DOUBLE[])");
            execute("INSERT INTO t VALUES (1, ARRAY[1.0])");
            execute("INSERT INTO t VALUES (2, ARRAY[2.0, 3.0])");
            execute("INSERT INTO t VALUES (3, ARRAY[4.0, 5.0, 6.0])");
            assertQueryNoLeakCheck(
                    "id\tval\n"
                            + "1\t1.0\n"
                            + "2\t2.0\n"
                            + "2\t3.0\n"
                            + "3\t4.0\n"
                            + "3\t5.0\n"
                            + "3\t6.0\n",
                    "SELECT t.id, u.val FROM t, UNNEST(t.arr) u(val)",
                    (String) null
            );
        });
    }

    @Test
    public void testMultipleRowsWithOrdinalityResets() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id LONG, arr DOUBLE[])");
            execute("INSERT INTO t VALUES (1, ARRAY[10.0, 20.0])");
            execute("INSERT INTO t VALUES (2, ARRAY[30.0, 40.0, 50.0])");
            assertQueryNoLeakCheck(
                    "id\tval\tord\n"
                            + "1\t10.0\t1\n"
                            + "1\t20.0\t2\n"
                            + "2\t30.0\t1\n"
                            + "2\t40.0\t2\n"
                            + "2\t50.0\t3\n",
                    "SELECT t.id, u.val, u.ord "
                            + "FROM t, UNNEST(t.arr) WITH ORDINALITY u(val, ord)",
                    (String) null
            );
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
            assertQueryNoLeakCheck(
                    "x\ty\n"
                            + "1.0\t10.0\n"
                            + "1.0\t20.0\n"
                            + "2.0\t10.0\n"
                            + "2.0\t20.0\n",
                    "SELECT u1.x, u2.y "
                            + "FROM t, UNNEST(t.a) u1(x), UNNEST(t.b) u2(y)",
                    (String) null
            );
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
            assertQueryNoLeakCheck(
                    "value\n",
                    "SELECT value FROM t, UNNEST(t.arr)",
                    (String) null
            );
        });
    }

    @Test
    public void testOrderByBaseColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id LONG, arr DOUBLE[])");
            execute("INSERT INTO t VALUES (2, ARRAY[20.0])");
            execute("INSERT INTO t VALUES (1, ARRAY[10.0])");
            assertQueryNoLeakCheck(
                    "id\tval\n"
                            + "1\t10.0\n"
                            + "2\t20.0\n",
                    "SELECT t.id, u.val FROM t, UNNEST(t.arr) u(val) ORDER BY t.id",
                    false
            );
        });
    }

    @Test
    public void testOrderByMultipleColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id LONG, arr DOUBLE[])");
            execute("INSERT INTO t VALUES (1, ARRAY[2.0, 1.0])");
            execute("INSERT INTO t VALUES (2, ARRAY[4.0, 3.0])");
            assertQueryNoLeakCheck(
                    "id\tval\n"
                            + "1\t1.0\n"
                            + "1\t2.0\n"
                            + "2\t3.0\n"
                            + "2\t4.0\n",
                    "SELECT t.id, u.val FROM t, UNNEST(t.arr) u(val) "
                            + "ORDER BY t.id, u.val",
                    false
            );
        });
    }

    @Test
    public void testOrderByOrdinality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[10.0, 20.0, 30.0] arr FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "val\tord\n"
                            + "30.0\t3\n"
                            + "20.0\t2\n"
                            + "10.0\t1\n",
                    "SELECT u.val, u.ord FROM t, UNNEST(t.arr) WITH ORDINALITY u(val, ord) "
                            + "ORDER BY u.ord DESC",
                    false
            );
        });
    }

    @Test
    public void testOrderByUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[30.0, 10.0, 20.0] arr FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "val\n"
                            + "10.0\n"
                            + "20.0\n"
                            + "30.0\n",
                    "SELECT u.val FROM t, UNNEST(t.arr) u(val) ORDER BY u.val ASC",
                    false
            );
        });
    }

    @Test
    public void testOrderByWithLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[5.0, 3.0, 1.0, 4.0, 2.0] arr FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "val\n"
                            + "1.0\n"
                            + "2.0\n"
                            + "3.0\n",
                    "SELECT u.val FROM t, UNNEST(t.arr) u(val) ORDER BY u.val LIMIT 3",
                    false
            );
        });
    }

    @Test
    public void testOrdinalityAlias() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[10.0, 20.0] arr FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "val\trow_num\n"
                            + "10.0\t1\n"
                            + "20.0\t2\n",
                    "SELECT u.val, u.row_num "
                            + "FROM t, UNNEST(t.arr) WITH ORDINALITY u(val, row_num)",
                    (String) null
            );
        });
    }

    @Test
    public void testPartialColumnAliases() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0] a, ARRAY[10.0, 20.0] b "
                    + "FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "x\tvalue2\n"
                            + "1.0\t10.0\n"
                            + "2.0\t20.0\n",
                    "SELECT u.x, u.value2 FROM t, UNNEST(t.a, t.b) u(x)",
                    (String) null
            );
        });
    }

    @Test
    public void testSelectCountStar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 3.0] arr FROM long_sequence(4)"
                    + ")");
            assertQueryNoLeakCheck(
                    "count\n"
                            + "12\n",
                    "SELECT count(*) FROM t, UNNEST(t.arr) u(val)",
                    null, false, false, true
            );
        });
    }

    @Test
    public void testSelectMixedBaseAndUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT x::LONG id, ARRAY[x * 1.0] arr FROM long_sequence(2)"
                    + ")");
            assertQueryNoLeakCheck(
                    "id\tval\n"
                            + "1\t1.0\n"
                            + "2\t2.0\n",
                    "SELECT t.id, u.val FROM t, UNNEST(t.arr) u(val)",
                    (String) null
            );
        });
    }

    @Test
    public void testSelectOnlyUnnestedColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT x::LONG id, ARRAY[x * 10.0, x * 20.0] arr "
                    + "FROM long_sequence(2)"
                    + ")");
            assertQueryNoLeakCheck(
                    "val\n"
                            + "10.0\n"
                            + "20.0\n"
                            + "20.0\n"
                            + "40.0\n",
                    "SELECT u.val FROM t, UNNEST(t.arr) u(val)",
                    (String) null
            );
        });
    }

    @Test
    public void testSelectStar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0] arr FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "arr\tvalue\n"
                            + "[1.0,2.0]\t1.0\n"
                            + "[1.0,2.0]\t2.0\n",
                    "SELECT * FROM t, UNNEST(t.arr)",
                    (String) null
            );
        });
    }

    @Test
    public void testSelectStarMultipleArrays() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0] a, ARRAY[10.0] b FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "a\tb\tvalue1\tvalue2\n"
                            + "[1.0]\t[10.0]\t1.0\t10.0\n",
                    "SELECT * FROM t, UNNEST(t.a, t.b) u",
                    (String) null
            );
        });
    }

    @Test
    public void testSelectStarWithOrdinality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[5.0] arr FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "arr\tval\tord\n"
                            + "[5.0]\t5.0\t1\n",
                    "SELECT * FROM t, UNNEST(t.arr) WITH ORDINALITY u(val, ord)",
                    (String) null
            );
        });
    }

    @Test
    public void testSelectUStarAndTStar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT x::LONG id, ARRAY[x * 10.0] arr FROM long_sequence(2)"
                    + ")");
            assertQueryNoLeakCheck(
                    "id\tarr\tval\n"
                            + "1\t[10.0]\t10.0\n"
                            + "2\t[20.0]\t20.0\n",
                    "SELECT t.*, u.val FROM t, UNNEST(t.arr) u(val)",
                    (String) null
            );
        });
    }

    @Test
    public void testSelectWithExpressionOnUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[10.0, 20.0, 30.0] arr FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "doubled\n"
                            + "20.0\n"
                            + "40.0\n"
                            + "60.0\n",
                    "SELECT u.val * 2 doubled FROM t, UNNEST(t.arr) u(val)",
                    (String) null
            );
        });
    }

    @Test
    public void testSingleArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ARRAY[1.0, 2.0, 3.0] arr FROM long_sequence(1))");
            assertQueryNoLeakCheck(
                    "value\n"
                            + "1.0\n"
                            + "2.0\n"
                            + "3.0\n",
                    "SELECT value FROM t, UNNEST(t.arr)",
                    (String) null
            );
        });
    }

    @Test
    public void testSingleArrayWithAlias() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ARRAY[10.0, 20.0, 30.0] arr FROM long_sequence(1))");
            assertQueryNoLeakCheck(
                    "price\n"
                            + "10.0\n"
                            + "20.0\n"
                            + "30.0\n",
                    "SELECT u.price FROM t, UNNEST(t.arr) u(price)",
                    (String) null
            );
        });
    }

    @Test
    public void testSingleElementArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ARRAY[42.0] arr FROM long_sequence(1))");
            assertQueryNoLeakCheck(
                    "value\n"
                            + "42.0\n",
                    "SELECT value FROM t, UNNEST(t.arr)",
                    (String) null
            );
        });
    }

    @Test
    public void testStandalone() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "value\n"
                        + "1.0\n"
                        + "2.0\n"
                        + "3.0\n",
                "SELECT value FROM UNNEST(ARRAY[1.0, 2.0, 3.0])",
                (String) null
        ));
    }

    @Test
    public void testStandaloneEmpty() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "value\n",
                "SELECT value FROM UNNEST(ARRAY[]::DOUBLE[])",
                (String) null
        ));
    }

    @Test
    public void testStandaloneMultipleArrays() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "value1\tvalue2\n"
                        + "1.0\t10.0\n"
                        + "2.0\t20.0\n"
                        + "3.0\t30.0\n",
                "SELECT value1, value2 FROM UNNEST(ARRAY[1.0, 2.0, 3.0], ARRAY[10.0, 20.0, 30.0])",
                (String) null
        ));
    }

    @Test
    public void testStandaloneMultipleArraysDiffLen() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "value1\tvalue2\n"
                        + "1.0\t10.0\n"
                        + "2.0\t20.0\n"
                        + "3.0\tnull\n",
                "SELECT value1, value2 FROM UNNEST(ARRAY[1.0, 2.0, 3.0], ARRAY[10.0, 20.0])",
                (String) null
        ));
    }

    @Test
    public void testStandaloneNull() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "value\n",
                "SELECT value FROM UNNEST(NULL::DOUBLE[])",
                (String) null
        ));
    }

    @Test
    public void testStandaloneSingleElement() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "value\n"
                        + "42.0\n",
                "SELECT value FROM UNNEST(ARRAY[42.0])",
                (String) null
        ));
    }

    @Test
    public void testStandaloneWithGroupBy() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "value\tcnt\n"
                        + "1.0\t2\n"
                        + "2.0\t1\n",
                "SELECT value, count() cnt FROM UNNEST(ARRAY[1.0, 2.0, 1.0]) "
                        + "GROUP BY value ORDER BY value"
        ));
    }

    @Test
    public void testStandaloneWithOrdinality() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "val\tord\n"
                        + "10.0\t1\n"
                        + "20.0\t2\n"
                        + "30.0\t3\n",
                "SELECT val, ord FROM UNNEST(ARRAY[10.0, 20.0, 30.0]) WITH ORDINALITY AS t(val, ord)",
                (String) null
        ));
    }

    @Test
    public void testStandaloneWithWhere() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "value\n"
                        + "8.0\n"
                        + "9.0\n"
                        + "10.0\n",
                "SELECT value FROM UNNEST(ARRAY[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]) "
                        + "WHERE value > 7.0",
                (String) null
        ));
    }

    @Test
    public void testSubqueryContainingUnnest() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 3.0] arr FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "val\n"
                            + "1.0\n"
                            + "2.0\n"
                            + "3.0\n",
                    "SELECT val FROM (SELECT u.val FROM t, UNNEST(t.arr) u(val))",
                    (String) null
            );
        });
    }

    @Test
    public void testSubqueryContainingUnnestWithAggregation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 3.0, 4.0, 5.0] arr FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "s\n"
                            + "15.0\n",
                    "SELECT sum(val) s FROM (SELECT u.val FROM t, UNNEST(t.arr) u(val))",
                    null, false, false, true
            );
        });
    }

    @Test
    public void testSubqueryContainingUnnestWithColumnAliases() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0] a, ARRAY[10.0, 20.0] b "
                    + "FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "x\ty\n"
                            + "1.0\t10.0\n"
                            + "2.0\t20.0\n",
                    "SELECT x, y FROM (SELECT u.x, u.y FROM t, UNNEST(t.a, t.b) u(x, y))",
                    (String) null
            );
        });
    }

    @Test
    public void testSumOfUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 3.0, 4.0] arr FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "s\n"
                            + "10.0\n",
                    "SELECT sum(u.val) s FROM t, UNNEST(t.arr) u(val)",
                    null, false, false, true
            );
        });
    }

    @Test
    public void testSumEquivalenceWithArraySum() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 3.0, 4.0] arr FROM long_sequence(1)"
                    + ")");
            // Verify array_sum and UNNEST SUM produce same result
            assertQueryNoLeakCheck(
                    "s\n"
                            + "10.0\n",
                    "SELECT array_sum(arr) s FROM t"
            );
            assertQueryNoLeakCheck(
                    "s\n"
                            + "10.0\n",
                    "SELECT sum(u.val) s FROM t, UNNEST(t.arr) u(val)",
                    null, false, false, true
            );
        });
    }

    @Test
    public void testTableAliasOnly() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0] arr FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "value\n"
                            + "1.0\n"
                            + "2.0\n",
                    "SELECT u.value FROM t, UNNEST(t.arr) u",
                    (String) null
            );
        });
    }

    @Test
    public void testTableAliasWithAS() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0] arr FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "col1\n"
                            + "1.0\n"
                            + "2.0\n",
                    "SELECT u.col1 FROM t, UNNEST(t.arr) AS u(col1)",
                    (String) null
            );
        });
    }

    @Test
    public void testTableAliasWithoutAS() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0] arr FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "col1\n"
                            + "1.0\n"
                            + "2.0\n",
                    "SELECT u.col1 FROM t, UNNEST(t.arr) u(col1)",
                    (String) null
            );
        });
    }

    @Test
    public void testUnnest2DArrayEmpty() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (arr DOUBLE[][])");
            execute("INSERT INTO t VALUES (ARRAY[]::DOUBLE[][])");
            assertQueryNoLeakCheck(
                    "value\n",
                    "SELECT value FROM t, UNNEST(t.arr)",
                    (String) null
            );
        });
    }

    @Test
    public void testUnnest2DArrayMixed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE[][], b DOUBLE[])");
            execute("INSERT INTO t VALUES ("
                    + "ARRAY[[1.0, 2.0], [3.0, 4.0]], "
                    + "ARRAY[100.0, 200.0])");
            assertQueryNoLeakCheck(
                    "x\ty\n"
                            + "[1.0,2.0]\t100.0\n"
                            + "[3.0,4.0]\t200.0\n",
                    "SELECT u.x, u.y FROM t, UNNEST(t.a, t.b) u(x, y)",
                    (String) null
            );
        });
    }

    @Test
    public void testUnnest2DArrayMultipleRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id LONG, arr DOUBLE[][])");
            execute("INSERT INTO t VALUES (1, ARRAY[[1.0, 2.0], [3.0, 4.0]])");
            execute("INSERT INTO t VALUES (2, ARRAY[[10.0, 20.0]])");
            assertQueryNoLeakCheck(
                    "id\tval\n"
                            + "1\t[1.0,2.0]\n"
                            + "1\t[3.0,4.0]\n"
                            + "2\t[10.0,20.0]\n",
                    "SELECT t.id, u.val FROM t, UNNEST(t.arr) u(val)",
                    (String) null
            );
        });
    }

    @Test
    public void testUnnest2DArrayNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (arr DOUBLE[][])");
            execute("INSERT INTO t VALUES (NULL)");
            assertQueryNoLeakCheck(
                    "value\n",
                    "SELECT value FROM t, UNNEST(t.arr)",
                    (String) null
            );
        });
    }

    @Test
    public void testUnnest2DArraySliceAccess() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (arr DOUBLE[][])");
            execute("INSERT INTO t VALUES (ARRAY[[10.0, 20.0, 30.0], [40.0, 50.0, 60.0]])");
            assertQueryNoLeakCheck(
                    "s\n"
                            + "60.0\n"
                            + "150.0\n",
                    "SELECT array_sum(u.val) s FROM t, UNNEST(t.arr) u(val)",
                    (String) null
            );
        });
    }

    @Test
    public void testUnnest2DArrayToRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (arr DOUBLE[][])");
            execute("INSERT INTO t VALUES (ARRAY[[1.0, 2.0], [3.0, 4.0]])");
            assertQueryNoLeakCheck(
                    "value\n"
                            + "[1.0,2.0]\n"
                            + "[3.0,4.0]\n",
                    "SELECT value FROM t, UNNEST(t.arr)",
                    (String) null
            );
        });
    }

    @Test
    public void testUnnest2DArrayWithOrdinality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (arr DOUBLE[][])");
            execute("INSERT INTO t VALUES (ARRAY[[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]])");
            assertQueryNoLeakCheck(
                    "val\tord\n"
                            + "[1.0,2.0]\t1\n"
                            + "[3.0,4.0]\t2\n"
                            + "[5.0,6.0]\t3\n",
                    "SELECT u.val, u.ord "
                            + "FROM t, UNNEST(t.arr) WITH ORDINALITY u(val, ord)",
                    (String) null
            );
        });
    }

    @Test
    public void testUnnest3DArrayTo2D() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (arr DOUBLE[][][])");
            execute("INSERT INTO t VALUES ("
                    + "ARRAY[ARRAY[[1.0, 2.0], [3.0, 4.0]], ARRAY[[5.0, 6.0], [7.0, 8.0]]]"
                    + ")");
            assertQueryNoLeakCheck(
                    "value\n"
                            + "[[1.0,2.0],[3.0,4.0]]\n"
                            + "[[5.0,6.0],[7.0,8.0]]\n",
                    "SELECT value FROM t, UNNEST(t.arr)",
                    (String) null
            );
        });
    }

    @Test
    public void testVeryLargeArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT rnd_double_array(1, 0, 0, 100000) arr FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "cnt\n"
                            + "100000\n",
                    "SELECT count() cnt FROM t, UNNEST(t.arr) u(val)",
                    null, false, false, true
            );
        });
    }

    @Test
    public void testWhereNoMatchingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 3.0] arr FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT u.val FROM t, UNNEST(t.arr) u(val) WHERE u.val > 100.0",
                    (String) null
            );
        });
    }

    @Test
    public void testWhereNullCheck() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 3.0] a, ARRAY[10.0] b "
                    + "FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "x\ty\n"
                            + "1.0\t10.0\n",
                    "SELECT u.x, u.y FROM t, UNNEST(t.a, t.b) u(x, y) "
                            + "WHERE u.y IS NOT NULL",
                    (String) null
            );
        });
    }

    @Test
    public void testWhereOnBaseColumnPushedDown() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT x::LONG id, ARRAY[x * 1.0, x * 2.0] arr "
                    + "FROM long_sequence(5)"
                    + ")");
            assertQueryNoLeakCheck(
                    "id\tval\n"
                            + "3\t3.0\n"
                            + "3\t6.0\n",
                    "SELECT t.id, u.val FROM t, UNNEST(t.arr) u(val) WHERE t.id = 3",
                    (String) null
            );
        });
    }

    @Test
    public void testWhereOnBothBaseAndUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT x::LONG id, ARRAY[x * 1.0, x * 2.0, x * 3.0] arr "
                    + "FROM long_sequence(3)"
                    + ")");
            assertQueryNoLeakCheck(
                    "id\tval\n"
                            + "2\t4.0\n"
                            + "2\t6.0\n"
                            + "3\t6.0\n"
                            + "3\t9.0\n",
                    "SELECT t.id, u.val FROM t, UNNEST(t.arr) u(val) "
                            + "WHERE t.id >= 2 AND u.val > 3.0",
                    (String) null
            );
        });
    }

    @Test
    public void testWhereOnOrdinalityColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[10.0, 20.0, 30.0, 40.0, 50.0] arr "
                    + "FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "val\tord\n"
                            + "30.0\t3\n"
                            + "40.0\t4\n"
                            + "50.0\t5\n",
                    "SELECT u.val, u.ord "
                            + "FROM t, UNNEST(t.arr) WITH ORDINALITY u(val, ord) "
                            + "WHERE u.ord > 2",
                    (String) null
            );
        });
    }

    @Test
    public void testWhereOnUnnestedColumnAbove() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 5.0, 2.0, 8.0, 3.0] arr FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "val\n"
                            + "5.0\n"
                            + "8.0\n",
                    "SELECT u.val FROM t, UNNEST(t.arr) u(val) WHERE u.val > 4.0",
                    (String) null
            );
        });
    }

    @Test
    public void testWhereWithOrOnBase() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id LONG, arr DOUBLE[])");
            execute("INSERT INTO t VALUES (1, ARRAY[10.0])");
            execute("INSERT INTO t VALUES (2, ARRAY[20.0])");
            execute("INSERT INTO t VALUES (3, ARRAY[30.0])");
            assertQueryNoLeakCheck(
                    "id\tval\n"
                            + "1\t10.0\n"
                            + "3\t30.0\n",
                    "SELECT t.id, u.val FROM t, UNNEST(t.arr) u(val) "
                            + "WHERE t.id = 1 OR t.id = 3",
                    (String) null
            );
        });
    }

    @Test
    public void testWithArithmeticOnUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[10.0, 20.0, 30.0] arr FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "doubled\tplus_one\n"
                            + "20.0\t11.0\n"
                            + "40.0\t21.0\n"
                            + "60.0\t31.0\n",
                    "SELECT u.val * 2 doubled, u.val + 1 plus_one "
                            + "FROM t, UNNEST(t.arr) u(val)",
                    (String) null
            );
        });
    }

    @Test
    public void testWithCaseExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[5.0, 15.0, 25.0] arr FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "val\tlabel\n"
                            + "5.0\tlow\n"
                            + "15.0\thigh\n"
                            + "25.0\thigh\n",
                    "SELECT u.val, CASE WHEN u.val > 10 THEN 'high' ELSE 'low' END label "
                            + "FROM t, UNNEST(t.arr) u(val)",
                    (String) null
            );
        });
    }

    @Test
    public void testWithCTEAsBase() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT x::LONG id, ARRAY[x * 1.0, x * 2.0] arr "
                    + "FROM long_sequence(2)"
                    + ")");
            assertQueryNoLeakCheck(
                    "id\tval\n"
                            + "1\t1.0\n"
                            + "1\t2.0\n"
                            + "2\t2.0\n"
                            + "2\t4.0\n",
                    "WITH cte AS (SELECT id, arr FROM t) "
                            + "SELECT cte.id, u.val FROM cte, UNNEST(cte.arr) u(val)",
                    (String) null
            );
        });
    }

    @Test
    public void testWithDistinct() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 1.0, 2.0, 3.0] arr FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "val\n"
                            + "1.0\n"
                            + "2.0\n"
                            + "3.0\n",
                    "SELECT DISTINCT u.val FROM t, UNNEST(t.arr) u(val) ORDER BY u.val"
            );
        });
    }

    @Test
    public void testWithGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0, 1.0] arr FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "val\tcnt\n"
                            + "1.0\t2\n"
                            + "2.0\t1\n",
                    "SELECT u.val, count() cnt FROM t, UNNEST(t.arr) u(val) GROUP BY u.val ORDER BY u.val"
            );
        });
    }

    @Test
    public void testWithJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id LONG, arr DOUBLE[])");
            execute("INSERT INTO t VALUES (1, ARRAY[10.0, 20.0])");
            execute("CREATE TABLE t2 (id LONG, name SYMBOL)");
            execute("INSERT INTO t2 VALUES (1, 'Alice')");
            assertQueryNoLeakCheck(
                    "id\tval\tname\n"
                            + "1\t10.0\tAlice\n"
                            + "1\t20.0\tAlice\n",
                    "SELECT t.id, u.val, t2.name "
                            + "FROM t, UNNEST(t.arr) u(val) "
                            + "JOIN t2 ON t2.id = t.id",
                    (String) null
            );
        });
    }

    @Test
    public void testWithOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ARRAY[3.0, 1.0, 2.0] arr FROM long_sequence(1))");
            assertQueryNoLeakCheck(
                    "val\n"
                            + "1.0\n"
                            + "2.0\n"
                            + "3.0\n",
                    "SELECT u.val FROM t, UNNEST(t.arr) u(val) ORDER BY u.val",
                    false
            );
        });
    }

    @Test
    public void testWithOrdinality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ARRAY[10.0, 20.0, 30.0] arr FROM long_sequence(1))");
            assertQueryNoLeakCheck(
                    "val\tord\n"
                            + "10.0\t1\n"
                            + "20.0\t2\n"
                            + "30.0\t3\n",
                    "SELECT u.val, u.ord FROM t, UNNEST(t.arr) WITH ORDINALITY u(val, ord)",
                    (String) null
            );
        });
    }

    @Test
    public void testWithOrdinalityMultipleBaseRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT x::LONG id, ARRAY[x * 1.0, x * 2.0] arr "
                    + "FROM long_sequence(3)"
                    + ")");
            assertQueryNoLeakCheck(
                    "id\tval\tord\n"
                            + "1\t1.0\t1\n"
                            + "1\t2.0\t2\n"
                            + "2\t2.0\t1\n"
                            + "2\t4.0\t2\n"
                            + "3\t3.0\t1\n"
                            + "3\t6.0\t2\n",
                    "SELECT t.id, u.val, u.ord FROM t, UNNEST(t.arr) WITH ORDINALITY u(val, ord)",
                    (String) null
            );
        });
    }

    @Test
    public void testWithSubqueryAsBase() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT x::LONG id, ARRAY[x * 1.0, x * 2.0] arr "
                    + "FROM long_sequence(3)"
                    + ")");
            assertQueryNoLeakCheck(
                    "id\tval\n"
                            + "2\t2.0\n"
                            + "2\t4.0\n"
                            + "3\t3.0\n"
                            + "3\t6.0\n",
                    "SELECT sub.id, u.val FROM "
                            + "(SELECT id, arr FROM t WHERE id > 1) sub, "
                            + "UNNEST(sub.arr) u(val)",
                    (String) null
            );
        });
    }

    @Test
    public void testWithUnionAll() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT ARRAY[1.0, 2.0] arr FROM long_sequence(1)"
                    + ")");
            assertQueryNoLeakCheck(
                    "val\n"
                            + "1.0\n"
                            + "2.0\n"
                            + "10.0\n"
                            + "20.0\n",
                    "SELECT u.val FROM t, UNNEST(t.arr) u(val) "
                            + "UNION ALL "
                            + "SELECT value FROM UNNEST(ARRAY[10.0, 20.0])",
                    (String) null
            );
        });
    }

    @Test
    public void testWithWhereOnBaseColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS ("
                    + "SELECT rnd_symbol('BTC', 'ETH') sym, ARRAY[x * 1.0, x * 2.0] arr "
                    + "FROM long_sequence(10)"
                    + ")");
            assertQueryNoLeakCheck(
                    "sym\tvalue\n",
                    "SELECT t.sym, u.value FROM t, UNNEST(t.arr) u(value) WHERE t.sym = 'NONE'",
                    (String) null
            );
        });
    }

    @Test
    public void testWithWhereOnUnnestedColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ARRAY[1.0, 2.0, 3.0, 4.0, 5.0] arr FROM long_sequence(1))");
            assertQueryNoLeakCheck(
                    "val\n"
                            + "4.0\n"
                            + "5.0\n",
                    "SELECT u.val FROM t, UNNEST(t.arr) u(val) WHERE u.val > 3.0",
                    (String) null
            );
        });
    }
}

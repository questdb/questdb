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

/**
 * Brutal SQL parser testing for array type handling edge cases.
 */
public class SqlParserArrayBrutalTest extends AbstractCairoTest {

    @Test
    public void testAlterTableArrayTypeErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (id int)");

            // Alter table add column with space error
            assertException("ALTER TABLE test ADD COLUMN data double []", 40, "array type requires no whitespace");
        });
    }

    @Test
    public void testArrayConstructorVsCastErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (id int)");

            // Valid array constructors (spaces allowed)
            assertSql("ARRAY\n", "SELECT ARRAY[1, 2, 3] FROM test");
            assertSql("ARRAY\n", "SELECT ARRAY[ 1 , 2 , 3 ] FROM test");

            // Mixed valid constructor with invalid cast
            assertException("SELECT CAST(ARRAY[1, 2, 3] AS int []) FROM test", 34, "array type requires no whitespace");
        });
    }

    @Test
    public void testCTEArrayTypeErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (id int)");

            // CTE with array type errors
            assertException(
                    "WITH cte AS (SELECT CAST(null AS double []) AS arr FROM base) SELECT * FROM cte",
                    40,
                    "array type requires no whitespace"
            );

            // Nested CTEs
            assertException(
                    "WITH cte1 AS (SELECT id FROM base), cte2 AS (SELECT CAST(null AS int []) AS x FROM cte1) SELECT * FROM cte2",
                    69,
                    "array type requires no whitespace"
            );
        });
    }

    @Test
    public void testComplexExpressionArrayTypeErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (a int, b int, c double)");

            // Arithmetic with array type error
            assertException("SELECT CAST(a + b AS int []) FROM test", 25, "array type requires no whitespace");

            // Function calls with array type error
            assertException("SELECT CAST(ABS(a) AS int []) FROM test", 26, "array type requires no whitespace");

            // CASE expressions with array type error
            assertException("SELECT CASE WHEN a > b THEN CAST(c AS double []) ELSE null END FROM test", 45, "array type requires no whitespace");

            // Nested function calls
            assertException("SELECT CAST(COALESCE(a, b) AS int []) FROM test", 34, "array type requires no whitespace");
        });
    }

    @Test
    public void testInsertArrayValueErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (data double[])");

            // Valid inserts should work
            execute("INSERT INTO test VALUES (ARRAY[1.0, 2.0, 3.0])");

            // But cast errors should still fail
            assertException("INSERT INTO test VALUES (CAST(null AS double []))", 45, "array type requires no whitespace");
        });
    }

    @Test
    public void testJoinArrayTypeErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (id int)");
            execute("CREATE TABLE t2 (id int)");

            // JOIN with array type error in SELECT
            assertException("SELECT CAST(t1.id AS int []) FROM t1 JOIN t2 ON t1.id = t2.id", 25, "array type requires no whitespace");

            // JOIN with array type error in condition
            assertException("SELECT * FROM t1 JOIN t2 ON CAST(t1.id AS int []) = t2.id", 46, "array type requires no whitespace");
        });
    }

    @Test
    public void testLimitOffsetArrayTypeErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (id int, num int)");

            // LIMIT with array cast error
            assertException("SELECT * FROM test LIMIT CAST(num AS int [])", 41, "array type requires no whitespace");
        });
    }

    @Test
    public void testOrderByArrayTypeErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (id int)");

            // ORDER BY with array cast error
            assertException("SELECT * FROM test ORDER BY CAST(id AS int [])", 43, "array type requires no whitespace");

            // GROUP BY with array cast error
            assertException("SELECT COUNT(*) FROM test GROUP BY CAST(id AS int [])", 50, "array type requires no whitespace");
        });
    }

    @Test
    public void testSelectWithArrayCastErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (id int, data double[])");

            // Invalid casts with spaces
            assertException("SELECT CAST(null AS double []) FROM test", 27, "array type requires no whitespace");
            assertException("SELECT CAST(id AS int []) FROM test", 22, "array type requires no whitespace");

            // Complex expressions with cast errors
            assertException("SELECT CASE WHEN id > 0 THEN CAST(null AS double []) ELSE null END FROM test", 49, "array type requires no whitespace");
        });
    }

    @Test
    public void testSubqueryArrayTypeErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (id int)");

            // Subquery with array type error
            assertException("SELECT (SELECT CAST(null AS double []) FROM test LIMIT 1)", 35, "array type requires no whitespace");

            // EXISTS with array type error
            assertException("SELECT * FROM test WHERE EXISTS (SELECT CAST(null AS int []) FROM test WHERE id = 1)", 57, "array type requires no whitespace");

            // IN with array type error
            assertException("SELECT * FROM test WHERE id IN (SELECT CAST(id AS int []) FROM test)", 54, "array type requires no whitespace");
        });
    }

    @Test
    public void testUpdateArrayTypeErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (id int, data double[])");
            execute("INSERT INTO test VALUES (1, ARRAY[1.0, 2.0])");

            // Valid update
            execute("UPDATE test SET data = ARRAY[3.0, 4.0] WHERE id = 1");

            // Invalid update with cast error
            assertException("UPDATE test SET data = CAST(null AS double []) WHERE id = 1", 43, "array type requires no whitespace");
        });
    }

    @Test
    public void testWindowFunctionArrayTypeErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (id int, value double)");

            // Aggregate function with array type error
            assertException("SELECT CAST(SUM(value) AS double []) FROM test", 33, "array type requires no whitespace");
        });
    }
}
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

package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Brutal SQL parser testing for array type handling edge cases.
 */
public class SqlParserArrayBrutalTest extends AbstractCairoTest {

    @Test
    public void testAlterTableArrayTypeErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (id int)");

            // Alter table add column with space error
            assertException("ALTER TABLE test ADD COLUMN data double []", 37, "Array type requires no whitespace");
            assertException("ALTER TABLE test ADD COLUMN arr int []", 34, "Array type requires no whitespace");

            // Multiple columns in alter
            assertException("ALTER TABLE test ADD COLUMN (a double [], b int)", 40, "Array type requires no whitespace");
        });
    }

    @Test
    public void testArrayConstructorVsCastErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (id int)");

            // Valid array constructors (spaces allowed)
            execute("SELECT [1, 2, 3] FROM test");
            execute("SELECT [ 1 , 2 , 3 ] FROM test");
            execute("SELECT ARRAY[1, 2, 3] FROM test");
            execute("SELECT ARRAY[ 1 , 2 , 3 ] FROM test");

            // Invalid cast (no spaces allowed)
            assertException("SELECT CAST([1, 2, 3] AS int []) FROM test", 34, "Array type requires no whitespace");

            // Mixed valid constructor with invalid cast
            assertException("SELECT CAST(ARRAY[1, 2, 3] AS int []) FROM test", 38, "Array type requires no whitespace");
        });
    }

    @Test
    public void testArrayTypeInConstraints() throws Exception {
        // Test if constraints can reference array types with spaces
        assertException("CREATE TABLE test (id int, data double [], CHECK (CAST(id AS int []) > 0))", 59, "Array type requires no whitespace");

        // Default values with array cast errors
        assertException("CREATE TABLE test (id int DEFAULT CAST(1 AS int []))", 44, "Array type requires no whitespace");
    }

    @Test
    public void testArrayTypeInFunctionDefinitions() throws Exception {
        // Test if function parameters can have array types with spaces (should fail)
        assertException("CREATE FUNCTION test_func(param double []) RETURNS int LANGUAGE SQL AS 'SELECT 1'", 39, "Array type requires no whitespace");

        // Test function return type with space (should fail)
        assertException("CREATE FUNCTION test_func(param int) RETURNS double [] LANGUAGE SQL AS 'SELECT ARRAY[1.0]'", 49, "Array type requires no whitespace");
    }

    @Test
    public void testCTEArrayTypeErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (id int)");

            // CTE with array type errors
            assertException("WITH cte AS (SELECT CAST(null AS double []) AS arr FROM base) SELECT * FROM cte", 40, "Array type requires no whitespace");

            // Nested CTEs
            assertException("WITH cte1 AS (SELECT id FROM base), cte2 AS (SELECT CAST(null AS int []) AS x FROM cte1) SELECT * FROM cte2", 73, "Array type requires no whitespace");
        });
    }

    @Test
    public void testComplexExpressionArrayTypeErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (a int, b int, c double)");

            // Arithmetic with array type error
            assertException("SELECT CAST(a + b AS int []) FROM test", 25, "Array type requires no whitespace");

            // Function calls with array type error
            assertException("SELECT CAST(ABS(a) AS int []) FROM test", 26, "Array type requires no whitespace");

            // CASE expressions with array type error
            assertException("SELECT CASE WHEN a > b THEN CAST(c AS double []) ELSE null END FROM test", 45, "Array type requires no whitespace");

            // Nested function calls
            assertException("SELECT CAST(COALESCE(a, b) AS int []) FROM test", 34, "Array type requires no whitespace");
        });
    }

    @Test
    public void testCreateTableArrayTypeErrors() throws Exception {
        // Basic whitespace errors
        assertException("CREATE TABLE t (x double [])", 23, "Array type requires no whitespace");
        assertException("CREATE TABLE t (x int [])", 21, "Array type requires no whitespace");

        // Complex multi-column scenarios
        assertException("CREATE TABLE t (a int, b double [], c varchar)", 32, "Array type requires no whitespace");
        assertException("CREATE TABLE t (a double [], b int [], c string)", 37, "Array type requires no whitespace");

        // Mixed valid and invalid columns
        assertException("CREATE TABLE t (valid_col int[], invalid_col double [], another_valid varchar[])", 56, "Array type requires no whitespace");
    }

    @Test
    public void testIndexCreationArrayTypeErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (id int, data double[])");

            // Array columns typically can't be indexed, but test the parser doesn't crash
            // This might fail for different reasons, but shouldn't crash on array syntax
            try {
                execute("CREATE INDEX idx ON test (CAST(id AS int []))");
                fail("Should have failed");
            } catch (Exception e) {
                // Should fail due to array syntax error, not crash
                TestUtils.assertContains(e.getMessage(), "Array type requires no whitespace");
            }
        });
    }

    @Test
    public void testInsertArrayValueErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (data double[])");

            // Valid inserts should work
            execute("INSERT INTO test VALUES (ARRAY[1.0, 2.0, 3.0])");
            execute("INSERT INTO test VALUES ([1.0, 2.0, 3.0])");

            // But cast errors should still fail
            assertException("INSERT INTO test VALUES (CAST(null AS double []))", 46, "Array type requires no whitespace");
        });
    }

    @Test
    public void testJoinArrayTypeErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (id int)");
            execute("CREATE TABLE t2 (id int)");

            // JOIN with array type error in SELECT
            assertException("SELECT CAST(t1.id AS int []) FROM t1 JOIN t2 ON t1.id = t2.id", 25, "Array type requires no whitespace");

            // JOIN with array type error in condition
            assertException("SELECT * FROM t1 JOIN t2 ON CAST(t1.id AS int []) = t2.id", 47, "Array type requires no whitespace");
        });
    }

    @Test
    public void testLimitOffsetArrayTypeErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (id int, num int)");

            // LIMIT with array cast error
            assertException("SELECT * FROM test LIMIT CAST(num AS int [])", 37, "Array type requires no whitespace");

            // OFFSET with array cast error
            assertException("SELECT * FROM test OFFSET CAST(num AS int [])", 38, "Array type requires no whitespace");
        });
    }

    @Test
    public void testOrderByArrayTypeErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (id int)");

            // ORDER BY with array cast error
            assertException("SELECT * FROM test ORDER BY CAST(id AS int [])", 43, "Array type requires no whitespace");

            // GROUP BY with array cast error
            assertException("SELECT COUNT(*) FROM test GROUP BY CAST(id AS int [])", 47, "Array type requires no whitespace");

            // HAVING with array cast error
            assertException("SELECT COUNT(*) FROM test GROUP BY id HAVING CAST(COUNT(*) AS int []) > 0", 61, "Array type requires no whitespace");
        });
    }

    @Test
    public void testPartitionByArrayTypeErrors() throws Exception {
        // Test partition by with array cast errors
        assertException("CREATE TABLE test (id int, ts timestamp) PARTITION BY CAST(id AS int [])", 66, "Array type requires no whitespace");
    }

    @Test
    public void testSelectWithArrayCastErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (id int, data double[])");

            // Valid selects
            execute("SELECT data FROM test");
            execute("SELECT CAST(null AS double[]) FROM test");

            // Invalid casts with spaces
            assertException("SELECT CAST(null AS double []) FROM test", 27, "Array type requires no whitespace");
            assertException("SELECT CAST(id AS int []) FROM test", 22, "Array type requires no whitespace");

            // Complex expressions with cast errors
            assertException("SELECT CASE WHEN id > 0 THEN CAST(null AS double []) ELSE null END FROM test", 50, "Array type requires no whitespace");
        });
    }

    @Test
    public void testSubqueryArrayTypeErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (id int)");

            // Subquery with array type error
            assertException("SELECT (SELECT CAST(null AS double []) FROM test LIMIT 1)", 35, "Array type requires no whitespace");

            // EXISTS with array type error
            assertException("SELECT * FROM test WHERE EXISTS (SELECT CAST(null AS int []) FROM test WHERE id = 1)", 64, "Array type requires no whitespace");

            // IN with array type error
            assertException("SELECT * FROM test WHERE id IN (SELECT CAST(id AS int []) FROM test)", 55, "Array type requires no whitespace");
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
            assertException("UPDATE test SET data = CAST(null AS double []) WHERE id = 1", 44, "Array type requires no whitespace");
        });
    }

    @Test
    public void testWindowFunctionArrayTypeErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (id int, value double)");

            // Window function with array type error
            assertException("SELECT CAST(ROW_NUMBER() OVER (ORDER BY id) AS int []) FROM test", 51, "Array type requires no whitespace");

            // Aggregate function with array type error
            assertException("SELECT CAST(SUM(value) AS double []) FROM test", 33, "Array type requires no whitespace");
        });
    }
}
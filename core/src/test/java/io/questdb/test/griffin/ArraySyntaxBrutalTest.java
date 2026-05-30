/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __\__ \ |_| |_| | |_) |
 *    \___\\__,_|\___||___/\__|____/|____/
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
 * Brutal edge case testing for array syntax validation.
 * Tests every conceivable way users might mess up array syntax.
 */
public class ArraySyntaxBrutalTest extends AbstractCairoTest {

    @Test
    public void testAlterTableArrayTypeErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (id int)");

            // Alter table add column with space error
            assertQuery("ALTER TABLE test ADD COLUMN data double []")
                    .fails(40, "array type requires no whitespace");
        });
    }

    @Test
    public void testBasicWhitespaceInArrayTypes() throws Exception {
        // Single space - the original problem
        assertQuery("CREATE TABLE t (x double [])")
                .fails(25, "array type requires no whitespace");

        // Tab character
        assertQuery("CREATE TABLE t (x double\t[])")
                .fails(25, "array type requires no whitespace");
    }

    @Test
    public void testCastExpressionWhitespaceErrors() throws Exception {
        // Basic cast with space
        assertQuery("SELECT CAST(null AS double [])")
                .fails(27, "array type requires no whitespace");
    }

    @Test
    public void testColumnCastWithSpaces() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (x int, y int)");

            // These should all fail with space errors
            assertQuery("SELECT x::double [] FROM base")
                    .fails(17, "array type requires no whitespace");
        });
    }

    @Test
    public void testCreateTableInvalidSyntax() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("create table test (arr double[1])")
                    .fails(30, "arrays do not have a fixed size, remove the number");
            assertQuery("create table test (arr double[][][3])")
                    .fails(34, "arrays do not have a fixed size, remove the number");
            assertQuery("create table test (arr double [1])")
                    .fails(30, "array type requires no whitespace between type and brackets");
            assertQuery("create table test (arr array[double])")
                    .fails(23, "the system supports type-safe arrays, e.g. `type[]`. Supported types are: DOUBLE");
            assertQuery("create table test (arr array[])")
                    .fails(23, "the system supports type-safe arrays, e.g. `type[]`. Supported types are: DOUBLE");
            assertQuery("create table test (arr array)")
                    .fails(23, "the system supports type-safe arrays, e.g. `type[]`. Supported types are: DOUBLE");
            assertQuery("create table test (arr double[][col][col2])")
                    .fails(31, "syntax error at column type definition, expected array type: 'DOUBLE[][]...");
        });
    }

    @Test
    public void testErrorRecoveryAfterArraySyntaxError() throws Exception {
        // Verify parser can recover after array syntax errors
        assertQuery("CREATE TABLE t (x double [], y int)")
                .fails(25, "array type requires no whitespace");

        // Should be able to create valid table after error
        assertMemoryLeak(() -> {
            execute("CREATE TABLE recovery_test (x double[], y int)");
            execute("DROP TABLE recovery_test");
        });
    }

    @Test
    public void testExtremeWhitespaceScenarios() throws Exception {
        // Extreme amounts of whitespace
        assertQuery("CREATE TABLE t (x double      [])")
                .fails(30, "array type requires no whitespace");

        // Mixed with line breaks
        String sqlWithLineBreak = "CREATE TABLE t (x double\n[])";
        assertQuery(sqlWithLineBreak)
                .fails(25, "array type requires no whitespace");
    }

    @Test
    public void testMultipleDimensionsWithSpaces() throws Exception {
        // Multi-dimensional arrays with spaces in different positions
        assertQuery("CREATE TABLE t (x double [] [])")
                .fails(25, "array type requires no whitespace");
        assertQuery("CREATE TABLE t (x double[][] [])")
                .fails(29, "array type requires no whitespace");
    }

    @Test
    public void testSelectWithArrayCastErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (id int, data double[])");

            // Valid selects
            assertSql(
                    "data\n",
                    "SELECT data FROM test"
            );
            assertSql(
                    "cast\n",
                    "SELECT CAST(null AS double[]) FROM test"
            );

            // Invalid casts with spaces
            assertQuery("SELECT CAST(null AS double []) FROM test")
                    .fails(27, "array type requires no whitespace");
        });
    }

    @Test
    public void testValidArraySyntaxStillWorks() throws Exception {
        assertMemoryLeak(() -> {
            // Valid syntax should continue to work
            execute("CREATE TABLE test_valid (id int, data double[])");
            execute("DROP TABLE test_valid");

            // Valid casts should work
            assertSql("cast\nnull\n", "SELECT CAST(null AS double[])");
        });
    }
}

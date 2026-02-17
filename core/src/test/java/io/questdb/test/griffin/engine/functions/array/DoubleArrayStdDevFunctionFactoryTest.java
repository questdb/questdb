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

package io.questdb.test.griffin.engine.functions.array;

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class DoubleArrayStdDevFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testEmptyArray() throws SqlException {
        assertSqlWithTypes(
                "array_stddev\nnull:DOUBLE\n",
                "SELECT array_stddev(ARRAY[]::double[])");
        assertSqlWithTypes(
                "array_stddev_samp\nnull:DOUBLE\n",
                "SELECT array_stddev_samp(ARRAY[]::double[])");
    }

    @Test
    public void testMultiDimensionalPop() throws SqlException {
        // Mean = 2.5, population stddev = sqrt(((1-2.5)^2 + (2-2.5)^2 + (3-2.5)^2 + (4-2.5)^2) / 4)
        // = sqrt((2.25 + 0.25 + 0.25 + 2.25) / 4) = sqrt(5/4) = sqrt(1.25) ≈ 1.118033988749895
        assertSqlWithTypes(
                "array_stddev_pop\n1.118033988749895:DOUBLE\n",
                "SELECT array_stddev_pop(ARRAY[[1.0, 2.0], [3.0, 4.0]])");
    }

    @Test
    public void testMultiDimensionalPopGroupBy() throws SqlException {
        execute("CREATE TABLE multi_dim_pop_test (grp INT, val DOUBLE)");
        execute("INSERT INTO multi_dim_pop_test VALUES (1, 1.0), (1, 2.0), (1, 3.0), (1, 4.0)");

        // Mean = 2.5, population stddev = sqrt(((1-2.5)^2 + (2-2.5)^2 + (3-2.5)^2 + (4-2.5)^2) / 4) ≈ 1.118033988749895
        assertSqlWithTypes(
                "stddev_pop\n1.118033988749895:DOUBLE\n",
                "SELECT stddev_pop(val) FROM multi_dim_pop_test GROUP BY grp");
    }

    @Test
    public void testMultiDimensionalSamp() throws SqlException {
        // Mean = 2.5, stddev = sqrt(((1-2.5)^2 + (2-2.5)^2 + (3-2.5)^2 + (4-2.5)^2) / 3)
        // = sqrt((2.25 + 0.25 + 0.25 + 2.25) / 3) = sqrt(5/3) ≈ 1.2909944487358056
        assertSqlWithTypes(
                "array_stddev\n1.2909944487358056:DOUBLE\n",
                "SELECT array_stddev(ARRAY[[1.0, 2.0], [3.0, 4.0]])");
        assertSqlWithTypes(
                "array_stddev_samp\n1.2909944487358056:DOUBLE\n",
                "SELECT array_stddev_samp(ARRAY[[1.0, 2.0], [3.0, 4.0]])");
    }

    @Test
    public void testMultiDimensionalSampGroupBy() throws SqlException {
        execute("CREATE TABLE multi_dim_samp_test (grp INT, val DOUBLE)");
        execute("INSERT INTO multi_dim_samp_test VALUES (1, 1.0), (1, 2.0), (1, 3.0), (1, 4.0)");

        // Mean = 2.5, sample stddev = sqrt(((1-2.5)^2 + (2-2.5)^2 + (3-2.5)^2 + (4-2.5)^2) / 3) ≈ 1.2909944487358056
        assertSqlWithTypes(
                "stddev_samp\n1.2909944487358056:DOUBLE\n",
                "SELECT stddev_samp(val) FROM multi_dim_samp_test GROUP BY grp");
    }

    @Test
    public void testNullArray() throws SqlException {
        assertSqlWithTypes(
                "array_stddev\nnull:DOUBLE\n",
                "SELECT array_stddev(null::double[])");
        assertSqlWithTypes(
                "array_stddev_samp\nnull:DOUBLE\n",
                "SELECT array_stddev_samp(null::double[])");
    }

    @Test
    public void testSimplePop() throws SqlException {
        // Mean = 3, population stddev = sqrt(((1-3)^2 + (2-3)^2 + (3-3)^2 + (4-3)^2 + (5-3)^2) / 5)
        // = sqrt((4 + 1 + 0 + 1 + 4) / 5) = sqrt(10/5) = sqrt(2) ≈ 1.4142135623730951
        assertSqlWithTypes(
                "array_stddev_pop\n1.4142135623730951:DOUBLE\n",
                "SELECT array_stddev_pop(ARRAY[1.0, 2.0, 3.0, 4.0, 5.0])");
    }

    @Test
    public void testSimplePopGroupBy() throws SqlException {
        execute("CREATE TABLE simple_pop_test (grp INT, val DOUBLE)");
        execute("INSERT INTO simple_pop_test VALUES (1, 1.0), (1, 2.0), (1, 3.0), (1, 4.0), (1, 5.0)");

        // Mean = 3, population stddev = sqrt(10/5) = sqrt(2) ≈ 1.4142135623730951
        assertSqlWithTypes(
                "stddev_pop\n1.4142135623730951:DOUBLE\n",
                "SELECT stddev_pop(val) FROM simple_pop_test GROUP BY grp");
    }

    @Test
    public void testSimpleSamp() throws SqlException {
        // Mean = 3, stddev = sqrt(((1-3)^2 + (2-3)^2 + (3-3)^2 + (4-3)^2 + (5-3)^2) / 4)
        // = sqrt((4 + 1 + 0 + 1 + 4) / 4) = sqrt(10/4) = sqrt(2.5) ≈ 1.5811388300841898
        assertSqlWithTypes(
                "array_stddev\n1.5811388300841898:DOUBLE\n",
                "SELECT array_stddev(ARRAY[1.0, 2.0, 3.0, 4.0, 5.0])");
        assertSqlWithTypes(
                "array_stddev_samp\n1.5811388300841898:DOUBLE\n",
                "SELECT array_stddev_samp(ARRAY[1.0, 2.0, 3.0, 4.0, 5.0])");
    }

    @Test
    public void testSimpleSampGroupBy() throws SqlException {
        execute("CREATE TABLE simple_samp_test (grp INT, val DOUBLE)");
        execute("INSERT INTO simple_samp_test VALUES (1, 1.0), (1, 2.0), (1, 3.0), (1, 4.0), (1, 5.0)");

        // Mean = 3, sample stddev = sqrt(10/4) = sqrt(2.5) ≈ 1.5811388300841898
        assertSqlWithTypes(
                "stddev_samp\n1.5811388300841898:DOUBLE\n",
                "SELECT stddev_samp(val) FROM simple_samp_test GROUP BY grp");
    }

    @Test
    public void testSingleElementPop() throws SqlException {
        assertSqlWithTypes(
                "array_stddev_pop\n0.0:DOUBLE\n",
                "SELECT array_stddev_pop(ARRAY[5.0])");
    }

    @Test
    public void testSingleElementPopGroupBy() throws SqlException {
        execute("CREATE TABLE single_pop_test (grp INT, val DOUBLE)");
        execute("INSERT INTO single_pop_test VALUES (1, 5.0)");

        assertSqlWithTypes(
                "stddev_pop\n0.0:DOUBLE\n",
                "SELECT stddev_pop(val) FROM single_pop_test GROUP BY grp");
    }

    @Test
    public void testSingleElementSamp() throws SqlException {
        assertSqlWithTypes(
                "array_stddev\nnull:DOUBLE\n",
                "SELECT array_stddev(ARRAY[5.0])");
        assertSqlWithTypes(
                "array_stddev_samp\nnull:DOUBLE\n",
                "SELECT array_stddev_samp(ARRAY[5.0])");
    }

    @Test
    public void testSingleElementSampGroupBy() throws SqlException {
        execute("CREATE TABLE single_samp_test (grp INT, val DOUBLE)");
        execute("INSERT INTO single_samp_test VALUES (1, 5.0)");

        assertSqlWithTypes(
                "stddev_samp\nnull:DOUBLE\n",
                "SELECT stddev_samp(val) FROM single_samp_test GROUP BY grp");
    }

    @Test
    public void testThreeElementsPop() throws SqlException {
        // Array: [2.0, 4.0, 6.0]
        // Mean = 4, population stddev = sqrt( ( (2-4)^2 + (4-4)^2 + (6-4)^2 ) / 3)
        // = sqrt((4 + 0 + 4) / 3) = sqrt(8/3) ≈ 1.632993161855452
        assertSqlWithTypes(
                "array_stddev_pop\n1.632993161855452:DOUBLE\n",
                "SELECT array_stddev_pop(ARRAY[2.0, 4.0, 6.0])");
    }

    @Test
    public void testThreeElementsPopGroupBy() throws SqlException {
        execute("CREATE TABLE three_pop_test (grp INT, val DOUBLE)");
        execute("INSERT INTO three_pop_test VALUES (1, 2.0), (1, 4.0), (1, 6.0)");

        // Mean = 4, population stddev = sqrt(8/3) ≈ 1.632993161855452
        assertSqlWithTypes(
                "stddev_pop\n1.632993161855452:DOUBLE\n",
                "SELECT stddev_pop(val) FROM three_pop_test GROUP BY grp");
    }

    @Test
    public void testThreeElementsPopNonVanilla() throws SqlException {
        // Slice: [ [2], [4], [6] ]
        // Mean = 4, population stddev = sqrt( ( (2-4)^2 + (4-4)^2 + (6-4)^2 ) / 3)
        // = sqrt((4 + 0 + 4) / 3) = sqrt(8/3) ≈ 1.632993161855452
        assertSqlWithTypes(
                "array_stddev_pop\n1.632993161855452:DOUBLE\n",
                "SELECT array_stddev_pop(ARRAY[ [1.0, 2.0], [3.0, 4.0], [5.0, 6.0] ][1:, 2:])");
    }

    @Test
    public void testThreeElementsSamp() throws SqlException {
        // Array: [2.0, 4.0, 6.0]
        // Mean = 4, sample stddev = sqrt( ( (2-4)^2 + (4-4)^2 + (6-4)^2 ) / 2)
        // = sqrt((4 + 0 + 4) / 2) = sqrt(4) = 2.0
        assertSqlWithTypes(
                "array_stddev\n2.0:DOUBLE\n",
                "SELECT array_stddev(ARRAY[2.0, 4.0, 6.0])");
        assertSqlWithTypes(
                "array_stddev_samp\n2.0:DOUBLE\n",
                "SELECT array_stddev_samp(ARRAY[2.0, 4.0, 6.0])");
    }

    @Test
    public void testThreeElementsSampGroupBy() throws SqlException {
        execute("CREATE TABLE three_samp_test (grp INT, val DOUBLE)");
        execute("INSERT INTO three_samp_test VALUES (1, 2.0), (1, 4.0), (1, 6.0)");

        // Mean = 4, sample stddev = sqrt(8/2) = 2.0
        assertSqlWithTypes(
                "stddev_samp\n2.0:DOUBLE\n",
                "SELECT stddev_samp(val) FROM three_samp_test GROUP BY grp");
    }

    @Test
    public void testThreeElementsSampNonVanilla() throws SqlException {
        // Slice: [ [2], [4], [6] ]
        // Mean = 4, stddev = sqrt( ( (2-4)^2 + (4-4)^2 + (6-4)^2 ) / 2)
        // = sqrt((4 + 0 + 4) / 2) = sqrt(4) = 2.0
        assertSqlWithTypes(
                "array_stddev\n2.0:DOUBLE\n",
                "SELECT array_stddev(ARRAY[ [1.0, 2.0], [3.0, 4.0], [5.0, 6.0] ][1:, 2:])");
        assertSqlWithTypes(
                "array_stddev_samp\n2.0:DOUBLE\n",
                "SELECT array_stddev_samp(ARRAY[ [1.0, 2.0], [3.0, 4.0], [5.0, 6.0] ][1:, 2:])");
    }

    @Test
    public void testTwoElementsPop() throws SqlException {
        // Mean = 2, population stddev = sqrt(((1-2)^2 + (3-2)^2) / 2) = sqrt((1 + 1) / 2) = sqrt(1) = 1.0
        assertSqlWithTypes(
                "array_stddev_pop\n1.0:DOUBLE\n",
                "SELECT array_stddev_pop(ARRAY[1.0, 3.0])");
    }

    @Test
    public void testTwoElementsPopGroupBy() throws SqlException {
        execute("CREATE TABLE two_pop_test (grp INT, val DOUBLE)");
        execute("INSERT INTO two_pop_test VALUES (1, 1.0), (1, 3.0)");

        // Mean = 2, population stddev = sqrt(2/2) = 1.0
        assertSqlWithTypes(
                "stddev_pop\n1.0:DOUBLE\n",
                "SELECT stddev_pop(val) FROM two_pop_test GROUP BY grp");
    }

    @Test
    public void testTwoElementsSamp() throws SqlException {
        // Mean = 2, stddev = sqrt(((1-2)^2 + (3-2)^2) / 1) = sqrt((1 + 1) / 1) = sqrt(2) ≈ 1.4142135623730951
        assertSqlWithTypes(
                "array_stddev\n1.4142135623730951:DOUBLE\n",
                "SELECT array_stddev(ARRAY[1.0, 3.0])");
        assertSqlWithTypes(
                "array_stddev_samp\n1.4142135623730951:DOUBLE\n",
                "SELECT array_stddev_samp(ARRAY[1.0, 3.0])");
    }

    @Test
    public void testTwoElementsSampGroupBy() throws SqlException {
        execute("CREATE TABLE two_samp_test (grp INT, val DOUBLE)");
        execute("INSERT INTO two_samp_test VALUES (1, 1.0), (1, 3.0)");

        // Mean = 2, sample stddev = sqrt(2/1) = sqrt(2) ≈ 1.4142135623730951
        assertSqlWithTypes(
                "stddev_samp\n1.4142135623730951:DOUBLE\n",
                "SELECT stddev_samp(val) FROM two_samp_test GROUP BY grp");
    }

    @Test
    public void testWithInfinityPop() throws SqlException {
        // Infinity values should be ignored
        // Mean = 2, population stddev = sqrt(((1-2)^2 + (3-2)^2) / 2) = sqrt(2/2) = 1.0
        assertSqlWithTypes(
                "array_stddev_pop\n1.0:DOUBLE\n",
                "SELECT array_stddev_pop(ARRAY[1.0, 3.0, 'Infinity'::double])");
    }

    @Test
    public void testWithInfinityPopGroupBy() throws SqlException {
        execute("CREATE TABLE infinity_pop_test (grp INT, val DOUBLE)");
        execute("INSERT INTO infinity_pop_test VALUES (1, 1.0), (1, 3.0), (1, 'Infinity'::double)");

        // Infinity values should be ignored, leaving [1.0, 3.0]
        // Mean = 2, population stddev = sqrt(2/2) = 1.0
        assertSqlWithTypes(
                "stddev_pop\n1.0:DOUBLE\n",
                "SELECT stddev_pop(val) FROM infinity_pop_test GROUP BY grp");
    }

    @Test
    public void testWithInfinitySamp() throws SqlException {
        // Infinity values should be ignored
        assertSqlWithTypes(
                "array_stddev\n1.4142135623730951:DOUBLE\n",
                "SELECT array_stddev(ARRAY[1.0, 3.0, 'Infinity'::double])");
        assertSqlWithTypes(
                "array_stddev_samp\n1.4142135623730951:DOUBLE\n",
                "SELECT array_stddev_samp(ARRAY[1.0, 3.0, 'Infinity'::double])");
    }

    @Test
    public void testWithInfinitySampGroupBy() throws SqlException {
        execute("CREATE TABLE infinity_samp_test (grp INT, val DOUBLE)");
        execute("INSERT INTO infinity_samp_test VALUES (1, 1.0), (1, 3.0), (1, 'Infinity'::double)");

        // Infinity values should be ignored, leaving [1.0, 3.0]
        // Mean = 2, sample stddev = sqrt(2/1) = sqrt(2) ≈ 1.4142135623730951
        assertSqlWithTypes(
                "stddev_samp\n1.4142135623730951:DOUBLE\n",
                "SELECT stddev_samp(val) FROM infinity_samp_test GROUP BY grp");
    }

    @Test
    public void testWithNaNPop() throws SqlException {
        // NaN values should be ignored
        // Mean = 2, population stddev = sqrt(((1-2)^2 + (3-2)^2) / 2) = sqrt(2/2) = 1.0
        assertSqlWithTypes(
                "array_stddev_pop\n1.0:DOUBLE\n",
                "SELECT array_stddev_pop(ARRAY[1.0, 3.0, NaN])");
    }

    @Test
    public void testWithNaNPopGroupBy() throws SqlException {
        execute("CREATE TABLE nan_pop_test (grp INT, val DOUBLE)");
        execute("INSERT INTO nan_pop_test VALUES (1, 1.0), (1, 3.0), (1, NaN)");

        // NaN values should be ignored, leaving [1.0, 3.0]
        // Mean = 2, population stddev = sqrt(2/2) = 1.0
        assertSqlWithTypes(
                "stddev_pop\n1.0:DOUBLE\n",
                "SELECT stddev_pop(val) FROM nan_pop_test GROUP BY grp");
    }

    @Test
    public void testWithNaNSamp() throws SqlException {
        // NaN values should be ignored
        assertSqlWithTypes(
                "array_stddev\n1.4142135623730951:DOUBLE\n",
                "SELECT array_stddev(ARRAY[1.0, 3.0, NaN])");
        assertSqlWithTypes(
                "array_stddev_samp\n1.4142135623730951:DOUBLE\n",
                "SELECT array_stddev_samp(ARRAY[1.0, 3.0, NaN])");
    }

    @Test
    public void testWithNaNSampGroupBy() throws SqlException {
        execute("CREATE TABLE nan_samp_test (grp INT, val DOUBLE)");
        execute("INSERT INTO nan_samp_test VALUES (1, 1.0), (1, 3.0), (1, NaN)");

        // NaN values should be ignored, leaving [1.0, 3.0]
        // Mean = 2, sample stddev = sqrt(2/1) = sqrt(2) ≈ 1.4142135623730951
        assertSqlWithTypes(
                "stddev_samp\n1.4142135623730951:DOUBLE\n",
                "SELECT stddev_samp(val) FROM nan_samp_test GROUP BY grp");
    }

    @Test
    public void testWithNegativeValuesPop() throws SqlException {
        // Test array [-2, -1, 0, 1, 2]
        // Mean = 0, population stddev = sqrt(((−2−0)^2 + (−1−0)^2 + (0−0)^2 + (1−0)^2 + (2−0)^2) / 5)
        // = sqrt((4 + 1 + 0 + 1 + 4) / 5) = sqrt(10/5) = sqrt(2) ≈ 1.4142135623730951
        assertSqlWithTypes(
                "array_stddev_pop\n1.4142135623730951:DOUBLE\n",
                "select array_stddev_pop(ARRAY[-2.0, -1.0, 0.0, 1.0, 2.0])");
    }

    @Test
    public void testWithNegativeValuesPopGroupBy() throws SqlException {
        execute("CREATE TABLE negative_pop_test (grp INT, val DOUBLE)");
        execute("INSERT INTO negative_pop_test VALUES (1, -2.0), (1, -1.0), (1, 0.0), (1, 1.0), (1, 2.0)");

        // Mean = 0, population stddev = sqrt(10/5) = sqrt(2) ≈ 1.4142135623730951
        assertSqlWithTypes(
                "stddev_pop\n1.4142135623730951:DOUBLE\n",
                "SELECT stddev_pop(val) FROM negative_pop_test GROUP BY grp");
    }

    @Test
    public void testWithNegativeValuesSamp() throws SqlException {
        // Test array [-2, -1, 0, 1, 2]
        // Mean = 0, stddev = sqrt(((−2−0)^2 + (−1−0)^2 + (0−0)^2 + (1−0)^2 + (2−0)^2) / 4)
        // = sqrt((4 + 1 + 0 + 1 + 4) / 4) = sqrt(10/4) = sqrt(2.5) ≈ 1.5811388300841898
        assertSqlWithTypes(
                "array_stddev\n1.5811388300841898:DOUBLE\n",
                "select array_stddev(ARRAY[-2.0, -1.0, 0.0, 1.0, 2.0])");
        assertSqlWithTypes(
                "array_stddev_samp\n1.5811388300841898:DOUBLE\n",
                "select array_stddev_samp(ARRAY[-2.0, -1.0, 0.0, 1.0, 2.0])");
    }

    @Test
    public void testWithNegativeValuesSampGroupBy() throws SqlException {
        execute("CREATE TABLE negative_samp_test (grp INT, val DOUBLE)");
        execute("INSERT INTO negative_samp_test VALUES (1, -2.0), (1, -1.0), (1, 0.0), (1, 1.0), (1, 2.0)");

        // Mean = 0, sample stddev = sqrt(10/4) = sqrt(2.5) ≈ 1.5811388300841898
        assertSqlWithTypes(
                "stddev_samp\n1.5811388300841898:DOUBLE\n",
                "SELECT stddev_samp(val) FROM negative_samp_test GROUP BY grp");
    }

    @Test
    public void testWithOnlyInfinityAndNaN() throws SqlException {
        // If all values are infinite or NaN, result should be NaN
        assertSqlWithTypes(
                "array_stddev\nnull:DOUBLE\n",
                "select array_stddev(ARRAY[NaN, 'Infinity'::double, '-Infinity'::double])");
        assertSqlWithTypes(
                "array_stddev_samp\nnull:DOUBLE\n",
                "select array_stddev_samp(ARRAY[NaN, 'Infinity'::double, '-Infinity'::double])");
    }

    @Test
    public void testWithOnlyInfinityAndNaNGroupBy() throws SqlException {
        execute("CREATE TABLE only_inf_nan_test (grp INT, val DOUBLE)");
        execute("INSERT INTO only_inf_nan_test VALUES (1, NaN), (1, 'Infinity'::double), (1, '-Infinity'::double)");

        // If all values are infinite or NaN, result should be null
        assertSqlWithTypes(
                "stddev_samp\nnull:DOUBLE\n",
                "SELECT stddev_samp(val) FROM only_inf_nan_test GROUP BY grp");
        assertSqlWithTypes(
                "stddev_pop\nnull:DOUBLE\n",
                "SELECT stddev_pop(val) FROM only_inf_nan_test GROUP BY grp");
    }

    @Test
    public void testWithTablePop() throws SqlException {
        execute(
                "CREATE TABLE tango_pop as (SELECT ARRAY[" +
                        "rnd_double(0)*100, rnd_double(0)*100, rnd_double(0)*100" +
                        "] arr FROM long_sequence(5))");

        // Just verify that the function works with table data
        assertSql(
                "count\n5\n",
                "SELECT count(*) FROM (SELECT array_stddev_pop(arr) FROM tango_pop WHERE array_stddev_pop(arr) IS NOT NULL)");
    }

    @Test
    public void testWithTableSamp() throws SqlException {
        execute(
                "CREATE TABLE tango as (SELECT ARRAY[" +
                        "rnd_double(0)*100, rnd_double(0)*100, rnd_double(0)*100" +
                        "] arr FROM long_sequence(5))");

        // Just verify that the function works with table data
        assertSql(
                "count\n5\n",
                "SELECT count(*) FROM (SELECT array_stddev(arr) FROM tango WHERE array_stddev(arr) IS NOT NULL)");
        assertSql(
                "count\n5\n",
                "SELECT count(*) FROM (SELECT array_stddev_samp(arr) FROM tango WHERE array_stddev_samp(arr) IS NOT NULL)");
    }

    @Test
    public void testZeroVariance() throws SqlException {
        // All elements are the same, so standard deviation should be 0
        assertSqlWithTypes(
                "array_stddev\n0.0:DOUBLE\n",
                "select array_stddev(ARRAY[5.0, 5.0, 5.0, 5.0])");
        assertSqlWithTypes(
                "array_stddev_samp\n0.0:DOUBLE\n",
                "select array_stddev_samp(ARRAY[5.0, 5.0, 5.0, 5.0])");
    }

    @Test
    public void testZeroVarianceGroupBy() throws SqlException {
        execute("CREATE TABLE zero_var_test (grp INT, val DOUBLE)");
        execute("INSERT INTO zero_var_test VALUES (1, 5.0), (1, 5.0), (1, 5.0), (1, 5.0)");

        // All elements are the same, so standard deviation should be 0
        assertSqlWithTypes(
                "stddev_samp\n0.0:DOUBLE\n",
                "SELECT stddev_samp(val) FROM zero_var_test GROUP BY grp");
        assertSqlWithTypes(
                "stddev_pop\n0.0:DOUBLE\n",
                "SELECT stddev_pop(val) FROM zero_var_test GROUP BY grp");
    }
}

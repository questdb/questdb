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
    }

    @Test
    public void testMultiDimNonVanilla() throws SqlException {
        // Slice: [ [2], [4], [6] ]
        // Mean = 4, stddev = sqrt( ( (2-4)^2 + (4-4)^2 + (6-4)^2 ) / 2)
        // = sqrt((4 + 0 + 4) / 2) = sqrt(4) = 2
        assertSqlWithTypes(
                "array_stddev\n2.0:DOUBLE\n",
                "SELECT array_stddev(ARRAY[ [1.0, 2.0], [3.0, 4.0], [5.0, 6.0] ][1:, 2:])");
    }

    @Test
    public void testMultiDimensional() throws SqlException {
        // Mean = 2.5, stddev = sqrt(((1-2.5)^2 + (2-2.5)^2 + (3-2.5)^2 + (4-2.5)^2) / 3)
        // = sqrt((2.25 + 0.25 + 0.25 + 2.25) / 3) = sqrt(5/3) ≈ 1.2909944487358056
        assertSqlWithTypes(
                "array_stddev\n1.2909944487358056:DOUBLE\n",
                "SELECT array_stddev(ARRAY[[1.0, 2.0], [3.0, 4.0]])");
    }

    @Test
    public void testNullArray() throws SqlException {
        assertSqlWithTypes(
                "array_stddev\nnull:DOUBLE\n",
                "SELECT array_stddev(null::double[])");
    }

    @Test
    public void testSimple() throws SqlException {
        // Mean = 3, stddev = sqrt(((1-3)^2 + (2-3)^2 + (3-3)^2 + (4-3)^2 + (5-3)^2) / 4)
        // = sqrt((4 + 1 + 0 + 1 + 4) / 4) = sqrt(10/4) = sqrt(2.5) ≈ 1.5811388300841898
        assertSqlWithTypes(
                "array_stddev\n1.5811388300841898:DOUBLE\n",
                "SELECT array_stddev(ARRAY[1.0, 2.0, 3.0, 4.0, 5.0])");
    }

    @Test
    public void testSingleElement() throws SqlException {
        assertSqlWithTypes(
                "array_stddev\n0.0:DOUBLE\n",
                "SELECT array_stddev(ARRAY[5.0])");
    }

    @Test
    public void testTwoElements() throws SqlException {
        // Mean = 2, stddev = sqrt(((1-2)^2 + (3-2)^2) / 1) = sqrt((1 + 1) / 1) = sqrt(2) ≈ 1.4142135623730951
        assertSqlWithTypes(
                "array_stddev\n1.4142135623730951:DOUBLE\n",
                "SELECT array_stddev(ARRAY[1.0, 3.0])");
    }

    @Test
    public void testWithInfinity() throws SqlException {
        // Infinity values should be ignored
        assertSqlWithTypes(
                "array_stddev\n1.4142135623730951:DOUBLE\n",
                "SELECT array_stddev(ARRAY[1.0, 3.0, 'Infinity'::double])");
    }

    @Test
    public void testWithNaN() throws SqlException {
        // NaN values should be ignored
        assertSqlWithTypes(
                "array_stddev\n1.4142135623730951:DOUBLE\n",
                "SELECT array_stddev(ARRAY[1.0, 3.0, NaN])");
    }

    @Test
    public void testWithNegativeValues() throws SqlException {
        // Test array [-2, -1, 0, 1, 2]
        // Mean = 0, stddev = sqrt(((−2−0)^2 + (−1−0)^2 + (0−0)^2 + (1−0)^2 + (2−0)^2) / 4)
        // = sqrt((4 + 1 + 0 + 1 + 4) / 4) = sqrt(10/4) = sqrt(2.5) ≈ 1.5811388300841898
        assertSqlWithTypes(
                "array_stddev\n1.5811388300841898:DOUBLE\n",
                "select array_stddev(ARRAY[-2.0, -1.0, 0.0, 1.0, 2.0])");
    }

    @Test
    public void testWithOnlyInfinityAndNaN() throws SqlException {
        // If all values are infinite or NaN, result should be NaN
        assertSqlWithTypes(
                "array_stddev\nnull:DOUBLE\n",
                "select array_stddev(ARRAY[NaN, 'Infinity'::double, '-Infinity'::double])");
    }

    @Test
    public void testWithTable() throws SqlException {
        execute(
                "CREATE TABLE tango as (SELECT ARRAY[" +
                        "rnd_double(0)*100, rnd_double(0)*100, rnd_double(0)*100" +
                        "] arr FROM long_sequence(5))");

        // Just verify that the function works with table data
        assertSql(
                "count\n5\n",
                "SELECT count(*) FROM (SELECT array_stddev(arr) FROM tango WHERE array_stddev(arr) IS NOT NULL)");
    }

    @Test
    public void testZeroVariance() throws SqlException {
        // All elements are the same, so standard deviation should be 0
        assertSqlWithTypes(
                "array_stddev\n0.0:DOUBLE\n",
                "select array_stddev(ARRAY[5.0, 5.0, 5.0, 5.0])");
    }
}

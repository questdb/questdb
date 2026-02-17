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

public class DoubleArrayMinFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testEmptyArray() throws SqlException {
        assertSqlWithTypes(
                "array_min\nnull:DOUBLE\n",
                "SELECT array_min(ARRAY[]::double[])");
    }

    @Test
    public void testMultiDimNonVanilla() throws SqlException {
        assertSqlWithTypes(
                "array_min\n2.0:DOUBLE\n",
                "SELECT array_min(ARRAY[ [1.0, 2.0], [3.0, 4.0], [5.0, 6.0] ][1:, 2:])");
    }

    @Test
    public void testMultiDimensional() throws SqlException {
        assertSqlWithTypes(
                "array_min\n1.0:DOUBLE\n",
                "SELECT array_min(ARRAY[[3.0, 2.0], [1.0, 4.0]])");
    }

    @Test
    public void testNullArray() throws SqlException {
        assertSqlWithTypes(
                "array_min\nnull:DOUBLE\n",
                "SELECT array_min(null::double[])");
    }

    @Test
    public void testSimple() throws SqlException {
        assertSqlWithTypes(
                "array_min\n1.0:DOUBLE\n",
                "SELECT array_min(ARRAY[4.0, 2.0, 3.0, 1.0, 5.0])");
    }

    @Test
    public void testSingleElement() throws SqlException {
        assertSqlWithTypes(
                "array_min\n5.0:DOUBLE\n",
                "SELECT array_min(ARRAY[5.0])");
    }

    @Test
    public void testTwoElements() throws SqlException {
        assertSqlWithTypes(
                "array_min\n1.0:DOUBLE\n",
                "SELECT array_min(ARRAY[1.0, 3.0])");
    }

    @Test
    public void testWithInfinity() throws SqlException {
        // Infinity values should be ignored
        assertSqlWithTypes(
                "array_min\n1.0:DOUBLE\n",
                "SELECT array_min(ARRAY[1.0, 3.0, 'Infinity'::double])");
    }

    @Test
    public void testWithInfinityNegative() throws SqlException {
        // Infinity values should be ignored
        assertSqlWithTypes(
                "array_min\n1.0:DOUBLE\n",
                "SELECT array_min(ARRAY[1.0, 3.0, '-Infinity'::double])");
    }

    @Test
    public void testWithNaN() throws SqlException {
        // NaN values should be ignored
        assertSqlWithTypes(
                "array_min\n1.0:DOUBLE\n",
                "SELECT array_min(ARRAY[1.0, 3.0, NaN])");
    }

    @Test
    public void testWithNegativeValues() throws SqlException {
        assertSqlWithTypes(
                "array_min\n-2.0:DOUBLE\n",
                "select array_min(ARRAY[-2.0, -1.0, 0.0, 1.0, 2.0])");
    }

    @Test
    public void testWithOnlyInfinityAndNaN() throws SqlException {
        // If all values are infinite or NaN, result should be NaN
        assertSqlWithTypes(
                "array_min\nnull:DOUBLE\n",
                "select array_min(ARRAY[NaN, 'Infinity'::double, '-Infinity'::double])");
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
                "SELECT count(*) FROM (SELECT array_min(arr) FROM tango WHERE array_min(arr) IS NOT NULL)");
    }
}

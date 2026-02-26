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
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class DoubleArrayReverseFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testDoubleReverse() throws SqlException {
        // reversing twice yields the original
        assertSqlWithTypes(
                "array_reverse\n[1.0,2.0,3.0]:DOUBLE[]\n",
                "SELECT array_reverse(array_reverse(ARRAY[1.0, 2.0, 3.0]))");
    }

    @Test
    public void testEmptyArray() throws SqlException {
        assertSqlWithTypes(
                "array_reverse\n[]:DOUBLE[]\n",
                "SELECT array_reverse(ARRAY[]::double[])");
    }

    @Test
    public void testMultiDimNonVanilla() throws SqlException {
        // sliced 3x3 array [1:, 2:] gives 3x2 non-vanilla with reversed inner arrays
        assertSqlWithTypes(
                "array_reverse\n[[1.0,3.0],[2.0,4.0],[6.0,7.0]]:DOUBLE[][]\n",
                "SELECT array_reverse(ARRAY[ [5.0, 3.0, 1.0], [6.0, 4.0, 2.0], [9.0, 7.0, 6.0] ][1:, 2:])");
    }

    @Test
    public void testMultiDimensional() throws SqlException {
        assertSqlWithTypes(
                "array_reverse\n[[2.0,1.0],[4.0,3.0]]:DOUBLE[][]\n",
                "SELECT array_reverse(ARRAY[ [1.0, 2.0], [3.0, 4.0] ])");
    }

    @Test
    public void testNullArray() throws SqlException {
        assertSqlWithTypes(
                "array_reverse\nnull:DOUBLE[]\n",
                "SELECT array_reverse(null::double[])");
    }

    @Test
    public void testParallel() throws Exception {
        execute("CREATE TABLE tmp AS (SELECT rnd_symbol('a','b','v') sym, rnd_double_array(1, 0) book FROM long_sequence(10000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        // double-reverse preserves all values, so sum must be identical
                        String original = "SELECT sym, round(sum(array_sum(book)), 2) s FROM tmp GROUP BY sym ORDER BY 1";
                        TestUtils.printSql(engine, sqlExecutionContext, original, sink);
                        String expected = sink.toString();

                        String reversed = "SELECT sym, round(sum(array_sum(array_reverse(array_reverse(book)))), 2) s FROM tmp GROUP BY sym ORDER BY 1";
                        TestUtils.assertSql(engine, sqlExecutionContext, reversed, sink, expected);
                    },
                    configuration,
                    LOG
            );
        }
    }

    @Test
    public void testReverse() throws SqlException {
        assertSqlWithTypes(
                "array_reverse\n[3.0,2.0,1.0]:DOUBLE[]\n",
                "SELECT array_reverse(ARRAY[1.0, 2.0, 3.0])");
    }

    @Test
    public void testReverseOddLength() throws SqlException {
        assertSqlWithTypes(
                "array_reverse\n[5.0,4.0,3.0,2.0,1.0]:DOUBLE[]\n",
                "SELECT array_reverse(ARRAY[1.0, 2.0, 3.0, 4.0, 5.0])");
    }

    @Test
    public void testSingleElement() throws SqlException {
        assertSqlWithTypes(
                "array_reverse\n[5.0]:DOUBLE[]\n",
                "SELECT array_reverse(ARRAY[5.0])");
    }

    @Test
    public void testTwoElements() throws SqlException {
        assertSqlWithTypes(
                "array_reverse\n[2.0,1.0]:DOUBLE[]\n",
                "SELECT array_reverse(ARRAY[1.0, 2.0])");
    }

    @Test
    public void testWithNaN() throws SqlException {
        assertSqlWithTypes(
                "array_reverse\n[3.0,null,2.0,null,1.0]:DOUBLE[]\n",
                "SELECT array_reverse(ARRAY[1.0, NaN, 2.0, NaN, 3.0])");
    }

    @Test
    public void testWithNegativeAndInfinity() throws SqlException {
        // QuestDB arrays store infinities as NaN, so they appear as null
        assertSqlWithTypes(
                "array_reverse\n[null,-1.0,null]:DOUBLE[]\n",
                "SELECT array_reverse(ARRAY['-Infinity'::double, -1.0, 'Infinity'::double])");
    }

    @Test
    public void testWithTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT ARRAY[rnd_double(0) * 100, rnd_double(0) * 100, rnd_double(0) * 100] arr FROM long_sequence(5))");

            assertSql(
                    "count\n5\n",
                    "SELECT count(*) FROM (SELECT array_reverse(arr) FROM tango)");
        });
    }
}

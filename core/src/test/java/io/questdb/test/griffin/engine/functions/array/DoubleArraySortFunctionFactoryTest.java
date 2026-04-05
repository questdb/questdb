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

package io.questdb.test.griffin.engine.functions.array;

import io.questdb.griffin.SqlException;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class DoubleArraySortFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAlreadySorted() throws SqlException {
        assertSqlWithTypes(
                "array_sort\n[1.0,2.0,3.0]:DOUBLE[]\n",
                "SELECT array_sort(ARRAY[1.0, 2.0, 3.0])");
    }

    @Test
    public void testAscendingDefault() throws SqlException {
        assertSqlWithTypes(
                "array_sort\n[1.0,2.0,3.0]:DOUBLE[]\n",
                "SELECT array_sort(ARRAY[3.0, 1.0, 2.0])");
    }

    @Test
    public void testCompositionWithReverse() throws SqlException {
        assertSqlWithTypes(
                "array_reverse\n[3.0,2.0,1.0]:DOUBLE[]\n",
                "SELECT array_reverse(array_sort(ARRAY[3.0, 1.0, 2.0]))");
    }

    @Test
    public void testDescending() throws SqlException {
        assertSqlWithTypes(
                "array_sort\n[3.0,2.0,1.0]:DOUBLE[]\n",
                "SELECT array_sort(ARRAY[3.0, 1.0, 2.0], true)");
    }

    @Test
    public void testDescendingExplicitFalse() throws SqlException {
        assertSqlWithTypes(
                "array_sort\n[1.0,2.0,3.0]:DOUBLE[]\n",
                "SELECT array_sort(ARRAY[3.0, 1.0, 2.0], false)");
    }

    @Test
    public void testDuplicateValues() throws SqlException {
        assertSqlWithTypes(
                "array_sort\n[1.0,2.0,2.0,3.0]:DOUBLE[]\n",
                "SELECT array_sort(ARRAY[2.0, 3.0, 1.0, 2.0])");
    }

    @Test
    public void testEmptyArray() throws SqlException {
        assertSqlWithTypes(
                "array_sort\n[]:DOUBLE[]\n",
                "SELECT array_sort(ARRAY[]::double[])");
    }

    @Test
    public void testMultiDimNonVanilla() throws SqlException {
        // sliced 3x3 array [1:, 2:] gives 3x2 non-vanilla with unsorted inner arrays
        assertSqlWithTypes(
                "array_sort\n[[1.0,3.0],[2.0,4.0],[6.0,7.0]]:DOUBLE[][]\n",
                "SELECT array_sort(ARRAY[ [5.0, 3.0, 1.0], [6.0, 4.0, 2.0], [9.0, 7.0, 6.0] ][1:, 2:])");
    }

    @Test
    public void testMultiDimensional() throws SqlException {
        assertSqlWithTypes(
                "array_sort\n[[1.0,3.0],[1.0,4.0]]:DOUBLE[][]\n",
                "SELECT array_sort(ARRAY[ [3.0, 1.0], [4.0, 1.0] ])");
    }

    @Test
    public void testNullArray() throws SqlException {
        assertSqlWithTypes(
                "array_sort\nnull:DOUBLE[]\n",
                "SELECT array_sort(null::double[])");
    }

    @Test
    public void testNullsDefaultAscending() throws SqlException {
        // ascending: NaN last
        assertSqlWithTypes(
                "array_sort\n[1.0,2.0,null]:DOUBLE[]\n",
                "SELECT array_sort(ARRAY[1.0, NaN, 2.0])");
    }

    @Test
    public void testNullsDefaultDescending() throws SqlException {
        // descending: NaN first (default nullsFirst=true when descending=true)
        assertSqlWithTypes(
                "array_sort\n[null,2.0,1.0]:DOUBLE[]\n",
                "SELECT array_sort(ARRAY[1.0, NaN, 2.0], true)");
    }

    @Test
    public void testNullsFirstAscending() throws SqlException {
        assertSqlWithTypes(
                "array_sort\n[null,1.0,2.0]:DOUBLE[]\n",
                "SELECT array_sort(ARRAY[1.0, NaN, 2.0], false, true)");
    }

    @Test
    public void testNullsLastDescending() throws SqlException {
        assertSqlWithTypes(
                "array_sort\n[2.0,1.0,null]:DOUBLE[]\n",
                "SELECT array_sort(ARRAY[1.0, NaN, 2.0], true, false)");
    }

    @Test
    public void testParallel() throws Exception {
        execute("CREATE TABLE tmp AS (SELECT rnd_symbol('a','b','v') sym, rnd_double_array(1, 0) book FROM long_sequence(10000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        // sorting preserves all values, so sum must be identical to unsorted
                        String unsorted = "SELECT sym, round(sum(array_sum(book)), 2) s FROM tmp GROUP BY sym ORDER BY 1";
                        TestUtils.printSql(engine, sqlExecutionContext, unsorted, sink);
                        String expected = sink.toString();

                        String sorted = "SELECT sym, round(sum(array_sum(array_sort(book))), 2) s FROM tmp GROUP BY sym ORDER BY 1";
                        TestUtils.assertSql(engine, sqlExecutionContext, sorted, sink, expected);
                    },
                    configuration,
                    LOG
            );
        }
    }

    @Test
    public void testSingleElement() throws SqlException {
        assertSqlWithTypes(
                "array_sort\n[5.0]:DOUBLE[]\n",
                "SELECT array_sort(ARRAY[5.0])");
    }

    @Test
    public void testTwoElements() throws SqlException {
        assertSqlWithTypes(
                "array_sort\n[1.0,2.0]:DOUBLE[]\n",
                "SELECT array_sort(ARRAY[2.0, 1.0])");
    }

    @Test
    public void testWithAllNaN() throws SqlException {
        assertSqlWithTypes(
                "array_sort\n[null,null,null]:DOUBLE[]\n",
                "SELECT array_sort(ARRAY[NaN, NaN, NaN])");
    }

    @Test
    public void testWithInfinity() throws SqlException {
        // QuestDB arrays store infinity as NaN, so both become null
        assertSqlWithTypes(
                "array_sort\n[1.0,2.0,null,null]:DOUBLE[]\n",
                "SELECT array_sort(ARRAY[NaN, 2.0, 'Infinity'::double, 1.0])");
    }

    @Test
    public void testWithMixedNaN() throws SqlException {
        assertSqlWithTypes(
                "array_sort\n[1.0,2.0,3.0,null,null]:DOUBLE[]\n",
                "SELECT array_sort(ARRAY[NaN, 2.0, NaN, 3.0, 1.0])");
    }

    @Test
    public void testWithNegativeInfinity() throws SqlException {
        // -Infinity is preserved as a distinct value and sorts before all finite values
        assertSqlWithTypes(
                "array_sort\n[null,1.0,2.0]:DOUBLE[]\n",
                "SELECT array_sort(ARRAY[2.0, '-Infinity'::double, 1.0])");
    }

    @Test
    public void testWithNegativeValues() throws SqlException {
        assertSqlWithTypes(
                "array_sort\n[-3.0,-2.0,-1.0,0.0,1.0]:DOUBLE[]\n",
                "SELECT array_sort(ARRAY[1.0, -2.0, 0.0, -3.0, -1.0])");
    }

    @Test
    public void testWithTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT ARRAY[rnd_double(0) * 100, rnd_double(0) * 100, rnd_double(0) * 100] arr FROM long_sequence(5))");

            assertSql(
                    "count\n5\n",
                    "SELECT count(*) FROM (SELECT array_sort(arr) FROM tango)");
        });
    }

    @Test
    public void testWithTableGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE tango AS (
                        SELECT rnd_symbol('a','b') sym,
                               ARRAY[rnd_double(0) * 100, rnd_double(0) * 100] arr
                        FROM long_sequence(10)
                    )""");

            // verify array_sort works inside aggregate query
            assertSql(
                    "count\n2\n",
                    "SELECT count(*) FROM (SELECT sym, count() FROM tango WHERE array_sum(array_sort(arr)) > -1 GROUP BY sym)");
        });
    }
}

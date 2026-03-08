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

public class DoubleArrayRoundFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testLargeNegScale() throws SqlException {
        assertSqlWithTypes("""
                        round
                        [null]:DOUBLE[]
                        """,
                "select round(array[14.7778], -18)");
    }

    @Test
    public void testLargePosScale() throws SqlException {
        assertSqlWithTypes("""
                        round
                        [null]:DOUBLE[]
                        """,
                "select round(array[14.7778], 17)");
    }

    @Test
    public void testLeftNan() throws SqlException {
        assertSqlWithTypes("""
                        round
                        [null]:DOUBLE[]
                        """,
                "select round(array[NaN], 5)");
    }

    @Test
    public void testMultiDimensional() throws SqlException {
        assertSqlWithTypes("""
                        round
                        [[1.2000000000000002,4.6000000000000005],[7.9,10.100000000000001]]:DOUBLE[][]
                        """,
                "select round(array[ [ 1.23, 4.56 ], [ 7.89, 10.1112 ] ], 1)");
    }

    @Test
    public void testNegScaleHigherThanNumber() throws SqlException {
        assertSqlWithTypes("""
                        round
                        [0.0]:DOUBLE[]
                        """,
                "select round(array[14.7778], -5)");
    }

    @Test
    public void testNegScaleNegValue() throws SqlException {
        assertSqlWithTypes("""
                        round
                        [-100.0]:DOUBLE[]
                        """,
                "select round(array[-104.9], -1)");
    }

    @Test
    public void testNegScaleNegValue2() throws SqlException {
        assertSqlWithTypes("""
                        round
                        [-110.0]:DOUBLE[]
                        """,
                "select round(array[-106.1], -1)");
    }

    @Test
    public void testNegScaleNull() throws SqlException {
        assertSqlWithTypes("""
                        round
                        null:DOUBLE[]
                        """,
                "select round(null::double[], -3)");
    }

    @Test
    public void testNegScalePosValue() throws SqlException {
        assertSqlWithTypes("""
                        round
                        [100.0]:DOUBLE[]
                        """,
                "select round(array[104.9], -1)");
    }

    @Test
    public void testNegScalePosValue2() throws SqlException {
        assertSqlWithTypes("""
                        round
                        [110.0]:DOUBLE[]
                        """,
                "select round(array[106.1], -1)");
    }

    @Test
    public void testOKNegScale() throws SqlException {
        assertSqlWithTypes("""
                        round
                        [0.0]:DOUBLE[]
                        """,
                "select round(array[14.7778], -13)");
    }

    @Test
    public void testOKPosScale() throws SqlException {
        assertSqlWithTypes("""
                        round
                        [14.7778]:DOUBLE[]
                        """,
                "select round(array[14.7778], 11)");
    }

    @Test
    public void testPosScaleHigherThanNumber() throws SqlException {
        assertSqlWithTypes("""
                        round
                        [14.7778]:DOUBLE[]
                        """,
                "select round(array[14.7778], 7)");
    }

    @Test
    public void testPosScaleNegValue() throws SqlException {
        assertSqlWithTypes("""
                        round
                        [-100.5]:DOUBLE[]
                        """,
                "select round(array[-100.54], 1)");
    }

    @Test
    public void testPosScaleNegValue2() throws SqlException {
        assertSqlWithTypes("""
                        round
                        [-100.60000000000001]:DOUBLE[]
                        """,
                "select round(array[-100.56], 1)");
    }

    @Test
    public void testPosScaleNull() throws SqlException {
        assertSqlWithTypes("""
                        round
                        null:DOUBLE[]
                        """,
                "select round(null::double[], 1)");
    }

    @Test
    public void testPosScalePosValue() throws SqlException {
        assertSqlWithTypes("""
                        round
                        [100.4]:DOUBLE[]
                        """,
                "select round(array[100.44], 1)");
    }

    @Test
    public void testPosScalePosValue2() throws SqlException {
        assertSqlWithTypes("""
                        round
                        [100.5]:DOUBLE[]
                        """,
                "select round(array[100.45], 1)");
    }

    @Test
    public void testPosScalePosValueParallel() throws Exception {
        execute("create table tmp as (select rnd_symbol('a','b','v') sym, rnd_double_array(1,0) book from long_sequence(10000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        String sql = "select sym, round(sum(array_sum(round(book,2))),2) from tmp group by sym order by 1";
                        TestUtils.assertSql(
                                engine,
                                sqlExecutionContext,
                                sql,
                                sink,
                                """
                                        sym\tround
                                        a\t9688.69
                                        b\t9938.03
                                        v\t9898.59
                                        """
                        );

                        TestUtils.assertSql(
                                engine,
                                sqlExecutionContext,
                                "explain " + sql,
                                sink,
                                """
                                        QUERY PLAN
                                        Sort light
                                          keys: [sym]
                                            VirtualRecord
                                              functions: [sym,round(sum,2)]
                                                Async Group By workers: 4
                                                  keys: [sym]
                                                  values: [sum(array_sum(roundbook))]
                                                  filter: null
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: tmp
                                        """
                        );
                    },
                    configuration,
                    LOG
            );
        }
    }

    @Test
    public void testRightNan() throws SqlException {
        assertSqlWithTypes("""
                        round
                        [null]:DOUBLE[]
                        """,
                "select round(array[123.65], null)");
    }

    @Test
    public void testSimple() throws SqlException {
        assertSqlWithTypes("""
                        round
                        [14.778]:DOUBLE[]
                        """,
                "select round(array[14.7778], 3)");
    }

    @Test
    public void testSimpleZeroScale() throws SqlException {
        assertSqlWithTypes("""
                        round
                        [15.0]:DOUBLE[]
                        """,
                "select round(array[14.7778],0)");
    }
}

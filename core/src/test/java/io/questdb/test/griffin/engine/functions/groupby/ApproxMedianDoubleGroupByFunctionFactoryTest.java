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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.cairo.CairoException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class ApproxMedianDoubleGroupByFunctionFactoryTest extends AbstractCairoTest {


    @Test
    public void testApproxMedianAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x long)");
            execute("insert into test values (null), (null), (null)");
            assertSql(
                    "approx_percentile\tapprox_median\n" +
                            "null\tnull\n",
                    "select approx_percentile(x, 0.5), approx_median(x) from test"
            );
        });
    }

    @Test
    public void testApproxMedianAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select 5.0 x from long_sequence(100))");
            assertSql(
                    "approx_percentile\tapprox_median\n" +
                            "5.0\t5.0\n",
                    "select approx_percentile(x, 0.5), approx_median(x) from test"
            );
        });
    }

    @Test
    public void testApproxMedianDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "approx_percentile\tapprox_median\n" +
                            "51.9375\t51.9375\n",
                    "select approx_percentile(x, 0.5), approx_median(x) from test"
            );
        });
    }

    @Test
    public void testApproxMedianEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            assertSql(
                    "approx_percentile\tapprox_median\n" +
                            "null\tnull\n",
                    "select approx_percentile(x, 0.5), approx_median(x) from test"
            );
        });
    }

    @Test
    public void testApproxMedianFloatValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as float) x from long_sequence(100))");
            assertSql(
                    "approx_percentile\tapprox_median\n" +
                            "51.9375\t51.9375\n",
                    "select approx_percentile(x, 0.5), approx_median(x) from test"
            );
        });
    }

    @Test
    public void testApproxMedianPackedAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x long)");
            execute("insert into test values (null), (null), (null)");
            assertSql(
                    "approx_percentile\tapprox_median\n" +
                            "null\tnull\n",
                    "select approx_percentile(x, 0.5, 5), approx_median(x, 5) from test"
            );
        });
    }

    @Test
    public void testApproxMedianPackedEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            assertSql(
                    "approx_percentile\tapprox_median\n" +
                            "null\tnull\n",
                    "select approx_percentile(x, 0.5, 5), approx_median(x, 5) from test"
            );
        });
    }

    @Test
    public void testApproxMedianSomeNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            execute("insert into test values (1.0), (null), (null), (null)");
            assertSql(
                    "approx_percentile\tapprox_median\n" +
                            "1.0\t1.0\n",
                    "select approx_percentile(x, 0.5), approx_median(x) from test"
            );
        });
    }

    @Test
    public void testApproxMedianWithPrecision1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "approx_percentile\tapprox_median\n" +
                            "51.9375\t51.9375\n",
                    "select approx_percentile(x, 0.5, 1), approx_median(x, 1) from test"
            );
        });
    }

    @Test
    public void testApproxMedianWithPrecision2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "approx_percentile\tapprox_median\n" +
                            "50.2421875\t50.2421875\n",
                    "select approx_percentile(x, 0.5, 2), approx_median(x, 2) from test"
            );
        });
    }

    @Test
    public void testApproxMedianWithPrecision3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "approx_percentile\tapprox_median\n" +
                            "50.0302734375\t50.0302734375\n",
                    "select approx_percentile(x, 0.5, 3), approx_median(x, 3) from test"
            );
        });
    }

    @Test
    public void testApproxMedianWithPrecision4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "approx_percentile\tapprox_median\n" +
                            "50.00189208984375\t50.00189208984375\n",
                    "select approx_percentile(x, 0.5, 4), approx_median(x, 4) from test"
            );
        });
    }

    @Test
    public void testApproxMedianWithPrecision5() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "approx_percentile\tapprox_median\n" +
                            "50.00023651123047\t50.00023651123047\n",
                    "select approx_percentile(x, 0.5, 5), approx_median(x, 5) from test"
            );
        });
    }

    @Test
    public void testInvalidPrecision1() throws Exception {
        assertException(
                "select approx_median(x::double, 6) from long_sequence(1)",
                32,
                "precision must be between 0 and 5"
        );
    }

    @Test
    public void testInvalidPrecision2() throws Exception {
        assertException(
                "select approx_median(x::double, -1) from long_sequence(1)",
                32,
                "precision must be between 0 and 5"
        );
    }

    @Test
    public void testThrowsOnNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            execute("insert into test values (1.0), (-1.0)");
            try {
                assertSql(
                        "approx_median\n" +
                                "1.0\n",
                        "select approx_median(x) from test"
                );
                Assert.fail();
            } catch (CairoException ignore) {
            }
        });
    }

    @Test
    public void testThrowsOnNegativeValuesPacked() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            execute("insert into test values (1.0), (-1.0)");
            try {
                assertSql(
                        "approx_median\n" +
                                "1.0\n",
                        "select approx_median(x, 5) from test"
                );
                Assert.fail();
            } catch (CairoException ignore) {
            }
        });
    }
}

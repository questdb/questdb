/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class ApproxPercentileGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testInvalidPercentile1() throws Exception {
        assertException(
                "select approx_percentile(x, 1.1) from long_sequence(1)",
                7,
                "percentile must be between 0 and 1"
        );
    }

    @Test
    public void testInvalidPercentile2() throws Exception {
        assertException(
                "select approx_percentile(x, -1) from long_sequence(1)",
                7,
                "percentile must be between 0 and 1"
        );
    }

    @Test
    public void testInvalidPercentile3() throws Exception {
        assertException(
                "select approx_percentile(x, x) from long_sequence(1)",
                28,
                "percentile must be a constant"
        );
    }

    @Test
    public void testInvalidPrecision1() throws Exception {
        assertException(
                "select approx_percentile(x, 0.5, 6) from long_sequence(1)",
                7,
                "precision must be between 0 and 5"
        );
    }

    @Test
    public void testInvalidPrecision2() throws Exception {
        assertException(
                "select approx_percentile(x, 0.5, -1) from long_sequence(1)",
                7,
                "precision must be between 0 and 5"
        );
    }

    @Test
    public void testApprox0thPercentileDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n1.0\n", "select approx_percentile(x, 0) from test"
            );
        });
    }

    @Test
    public void testApprox50thPercentileDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n50.0302734375\n", "select approx_percentile(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testApprox50thPercentileDoubleValuesWith5() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n50.0302734375\n", "select approx_percentile(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testApprox100thPercentileDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n100.0615234375\n", "select approx_percentile(x, 1.0) from test"
            );
        });
    }

    @Test
    public void testApprox50thPercentileLongValues() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test as (select x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n50.0302734375\n", "select approx_percentile(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testApprox50thPercentileIntValues() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test as (select cast(x as int) x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n50.0302734375\n", "select approx_percentile(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testApprox50thPercentileFloatValues() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test as (select cast(x as float) x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n50.0302734375\n", "select approx_percentile(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testApproxPercentileEmptyTable() throws Exception {
        compile("create table test (x long)");
        assertMemoryLeak(() -> assertSql("approx_percentile\n" +
                "NaN\n", "select approx_percentile(x, 0.5) from test")
        );
    }

    @Test
    public void testApproxPercentileAllNulls() throws Exception {
        compile("create table test (x long)");
        insert("insert into test values (null), (null), (null)");
        assertMemoryLeak(() -> assertSql("approx_percentile\n" +
                "NaN\n", "select approx_percentile(x, 0.5) from test")
        );
    }

    @Test
    public void testApproxPercentileSomeNulls() throws Exception {
        compile("create table test (x long)");
        insert("insert into test values (1.0), (null), (null), (null)");
        assertMemoryLeak(() -> assertSql("approx_percentile\n" +
                "1.0\n", "select approx_percentile(x, 0.5) from test")
        );
    }

    @Test
    public void testApproxPercentileAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test as (select 5.0 x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n5.0\n", "select approx_percentile(x, 0.5) from test"
            );
        });
    }

    // test increasing level of precision
    @Test
    public void testApprox50thPercentileWithPrecision1() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test as (select cast(x as double) x from long_sequence(1000))");
            assertSql(
                    "approx_percentile\n511.9375\n", "select approx_percentile(x, 0.5, 1) from test"
            );
        });
    }

    @Test
    public void testApprox50thPercentileWithPrecision2() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test as (select cast(x as double) x from long_sequence(1000))");
            assertSql(
                    "approx_percentile\n501.9921875\n", "select approx_percentile(x, 0.5, 2) from test"
            );
        });
    }

    @Test
    public void testApprox50thPercentileWithPrecision3() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test as (select cast(x as double) x from long_sequence(1000))");
            assertSql(
                    "approx_percentile\n500.2490234375\n", "select approx_percentile(x, 0.5, 3) from test"
            );
        });
    }

    @Test
    public void testApprox50thPercentileWithPrecision4() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test as (select cast(x as double) x from long_sequence(1000))");
            assertSql(
                    "approx_percentile\n500.01556396484375\n", "select approx_percentile(x, 0.5, 4) from test"
            );
        });
    }

    @Test
    public void testApprox50thPercentileWithPrecision5() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test as (select cast(x as double) x from long_sequence(1000))");
            assertSql(
                    "approx_percentile\n500.00194549560547\n", "select approx_percentile(x, 0.5, 5) from test"
            );
        });
    }

    @Test
    public void testApproxPercentileWithPercentileBindVariable() throws Exception {
        bindVariableService.setDouble(0, 0.5);
        assertMemoryLeak(() -> {
            ddl("create table test as (select 5.0 x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n5.0\n", "select approx_percentile(x, $1) from test"
            );
        });
    }
}

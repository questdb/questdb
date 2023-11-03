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
                "select approx_percentile(1.1, x) from long_sequence(1)",
                25,
                "percentile must be between 0 and 1"
        );
    }

    @Test
    public void testInvalidPercentile2() throws Exception {
        assertException(
                "select approx_percentile(x, x) from long_sequence(1)",
                25,
                "percentile must be a constant"
        );
    }

    @Test
    public void testDouble0thPercentile() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n1.0\n", "select approx_percentile(0, x) from test"
            );
        });
    }

    @Test
    public void testDouble50thPercentile() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n50.0302734375\n", "select approx_percentile(0.5, x) from test"
            );
        });
    }

    @Test
    public void testDouble100thPercentile() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n100.0615234375\n", "select approx_percentile(1.0, x) from test"
            );
        });
    }

    @Test
    public void testDoubleEmpty() throws Exception {
        compile("create table test (x long)");
        assertMemoryLeak(() -> assertSql("approx_percentile\n" +
                "NaN\n", "select approx_percentile(0.5, x) from test")
        );
    }

    @Test
    public void testDoubleAllNull() throws Exception {
        compile("create table test (x long)");
        insert("insert into test values (null), (null), (null)");
        assertMemoryLeak(() -> assertSql("approx_percentile\n" +
                "NaN\n", "select approx_percentile(0.5, x) from test")
        );
    }

    @Test
    public void testDoubleSomeNull() throws Exception {
        compile("create table test (x long)");
        insert("insert into test values (1.0), (null), (null), (null)");
        assertMemoryLeak(() -> assertSql("approx_percentile\n" +
                "1.0\n", "select approx_percentile(0.5, x) from test")
        );
    }

    @Test
    public void testDoubleAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test as (select 5.0 x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n5.0\n", "select approx_percentile(0.5, x) from test"
            );
        });
    }

    @Test
    public void testLong0thPercentile() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test as (select x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n1.0\n", "select approx_percentile(0, x) from test"
            );
        });
    }

    @Test
    public void testLong25thPercentile() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test as (select x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n25.0\n", "select approx_percentile(0.25, x) from test"
            );
        });
    }

    @Test
    public void testLong50thPercentile() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test as (select x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n50.0\n", "select approx_percentile(0.5, x) from test"
            );
        });
    }

    @Test
    public void testLong75thPercentile() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test as (select x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n75.0\n", "select approx_percentile(0.75, x) from test"
            );
        });
    }

    @Test
    public void testLong100thPercentile() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test as (select x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n100.0\n", "select approx_percentile(1.0, x) from test"
            );
        });
    }
}

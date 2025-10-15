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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class PercentileDiscLongGroupByFunctionFactoryTest extends AbstractCairoTest {

    private static final String txDdl = """
            CREATE TABLE tx_traffic (timestamp TIMESTAMP, value LONG);
            """;

    private static final String txDml = """
            INSERT INTO tx_traffic (timestamp, value) VALUES
            ('2022-05-02T14:51:28Z', '10'),
            ('2022-05-02T14:51:58Z', '85'),
            ('2022-05-02T14:52:28Z', '148'),
            ('2022-05-02T14:52:58Z', '135'),
            ('2022-05-02T14:53:28Z', '47'),
            ('2022-05-02T14:53:58Z', '61'),
            ('2022-05-02T14:54:28Z', '9'),
            ('2022-05-02T14:54:58Z', '63'),
            ('2022-05-02T14:55:28Z', '8'),
            ('2022-05-02T14:55:58Z', '61'),
            ('2022-05-02T14:56:28Z', '8'),
            ('2022-05-02T14:56:58Z', '177'),
            ('2022-05-02T14:57:28Z', '12'),
            ('2022-05-02T14:57:58Z', '62'),
            ('2022-05-02T14:58:28Z', '161'),
            ('2022-05-02T14:58:58Z', '63'),
            ('2022-05-02T14:59:28Z', '10'),
            ('2022-05-02T14:59:58Z', '60'),
            ('2022-05-02T15:00:28Z', '14'),
            ('2022-05-02T15:00:58Z', '200'),
            ('2022-05-02T15:01:28Z', '406'),
            ('2022-05-02T15:01:58Z', '849'),
            ('2022-05-02T15:02:28Z', '14'),
            ('2022-05-02T15:02:58Z', '126'),
            ('2022-05-02T15:03:28Z', '217'),
            ('2022-05-02T15:03:58Z', '66'),
            ('2022-05-02T15:04:28Z', '39'),
            ('2022-05-02T15:04:58Z', '289'),
            ('2022-05-02T15:05:28Z', '182'),
            ('2022-05-02T15:05:58Z', '219'),
            ('2022-05-02T15:06:28Z', '125'),
            ('2022-05-02T15:06:58Z', '87'),
            ('2022-05-02T15:07:28Z', '191'),
            ('2022-05-02T15:07:58Z', '242'),
            ('2022-05-02T15:08:28Z', '224'),
            ('2022-05-02T15:08:58Z', '240'),
            ('2022-05-02T15:09:28Z', '70'),
            ('2022-05-02T15:09:58Z', '177'),
            ('2022-05-02T15:10:28Z', '184'),
            ('2022-05-02T15:10:58Z', '118'),
            ('2022-05-02T15:11:28Z', '8'),
            ('2022-05-02T15:11:58Z', '63'),
            ('2022-05-02T15:12:28Z', '13'),
            ('2022-05-02T15:12:58Z', '60'),
            ('2022-05-02T15:13:28Z', '47'),
            ('2022-05-02T15:13:58Z', '62'),
            ('2022-05-02T15:14:28Z', '11'),
            ('2022-05-02T15:14:58Z', '59'),
            ('2022-05-02T15:15:28Z', '8'),
            ('2022-05-02T15:15:58Z', '65');
            """;

    @Test
    public void test0thPercentileLongValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select x from long_sequence(100))");
            assertSql(
                    "percentile_disc\n1\n",
                    "select percentile_disc(x, 0) from test"
            );
        });
    }

    @Test
    public void test100thPercentileLongValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select x from long_sequence(100))");
            assertSql(
                    "percentile_disc\n100\n",
                    "select percentile_disc(x, 1.0) from test"
            );
        });
    }

    @Test
    public void test50thPercentileIntValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as int) x from long_sequence(100))");
            assertSql(
                    "percentile_disc\n50\n",
                    "select percentile_disc(x, 0.5) from test"
            );
        });
    }

    @Test
    public void test50thPercentileLongValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select x from long_sequence(100))");
            assertSql(
                    "percentile_disc\n50\n",
                    "select percentile_disc(x, 0.5) from test"
            );
        });
    }

    @Test
    public void test50thPercentileLongValuesWith5() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select x from long_sequence(100))");
            assertSql(
                    "percentile_disc\n50\n",
                    "select percentile_disc(x, 0.5) from test"
            );
        });
    }

    // test increasing level of precision
    @Test
    public void test50thPercentileWithPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select x from long_sequence(1000))");
            assertSql(
                    "percentile_disc\n500\n",
                    "select percentile_disc(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testInvalidPercentile1() throws Exception {
        assertException(
                "select percentile_disc(x, 1.1) from long_sequence(1)",
                26,
                "invalid percentile"
        );
    }

    @Test
    public void testInvalidPercentile2() throws Exception {
        assertException(
                "select percentile_disc(x, -1.1) from long_sequence(1)",
                26,
                "invalid percentile"
        );
    }

    @Test
    public void testInvalidPercentilePacked1() throws Exception {
        assertException(
                "select percentile_disc(x, 1.1) from long_sequence(1)",
                26,
                "invalid percentile"
        );
    }

    @Test
    public void testInvalidPercentilePacked2() throws Exception {
        assertException(
                "select percentile_disc(x, -1.1) from long_sequence(1)",
                26,
                "invalid percentile"
        );
    }

    @Test
    public void testNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x long)");
            execute("insert into test values (1), (-1)");
            assertSql(
                    "percentile_disc\n" +
                            "1\n",
                    "select percentile_disc(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testPercentileAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x long)");
            execute("insert into test values (null), (null), (null)");
            assertSql(
                    "percentile_disc\n" +
                            "null\n",
                    "select percentile_disc(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testPercentileAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select 5L x from long_sequence(100))");
            assertSql(
                    "percentile_disc\n5\n",
                    "select percentile_disc(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testPercentileEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x long)");
            assertSql(
                    "percentile_disc\n" +
                            "null\n",
                    "select percentile_disc(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testPercentilePackedAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x long)");
            execute("insert into test values (null), (null), (null)");
            assertSql(
                    "percentile_disc\n" +
                            "null\n",
                    "select percentile_disc(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testPercentilePackedEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x long)");
            assertSql(
                    "percentile_disc\n" +
                            "null\n",
                    "select percentile_disc(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testPercentilePackedWithPercentileBindVariable() throws Exception {
        bindVariableService.setDouble(0, 0.5);
        assertMemoryLeak(() -> {
            execute("create table test as (select 5L x from long_sequence(100))");
            assertSql(
                    "percentile_disc\n5\n",
                    "select percentile_disc(x, $1) from test"
            );
        });
    }

    @Test
    public void testPercentileSomeNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x long)");
            execute("insert into test values (1), (null), (null), (null)");
            assertSql(
                    "percentile_disc\n" +
                            "1\n",
                    "select percentile_disc(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testPercentileWithPercentileBindVariable() throws Exception {
        bindVariableService.setDouble(0, 0.5);
        assertMemoryLeak(() -> {
            execute("create table test as (select 5L x from long_sequence(100))");
            assertSql(
                    "percentile_disc\n5\n",
                    "select percentile_disc(x, $1) from test"
            );
        });
    }

    @Test
    public void testThrowsOnNegativeValuesPacked() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x long)");
            execute("insert into test values (1), (-1)");
            assertSql(
                    "percentile_disc\n" +
                            "1\n",
                    "select percentile_disc(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testWithKnownData() throws Exception {
        assertMemoryLeak(() -> {
            execute(txDdl);
            execute(txDml);
            assertSql("percentile_disc\n" +
                            "289\n",
                    "select percentile_disc(value, 0.95) from tx_traffic");
        });
    }

    @Test
    public void testNegativePercentile() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select x from long_sequence(100))");
            // -0.95 should be equivalent to 1 - 0.95 = 0.05
            assertSql(
                    "percentile_disc\n6\n",
                    "select percentile_disc(x, -0.95) from test"
            );
        });
    }

    @Test
    public void testNegativePercentileEqualsPositive() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select x from long_sequence(100))");
            // -0.5 should be equivalent to 1 - 0.5 = 0.5
            assertSql(
                    "percentile_disc\n50\n",
                    "select percentile_disc(x, -0.5) from test"
            );
        });
    }

    @Test
    public void testNegativeOnePercentile() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select x from long_sequence(100))");
            // -1.0 should be equivalent to 1 - 1.0 = 0.0 (0th percentile)
            assertSql(
                    "percentile_disc\n1\n",
                    "select percentile_disc(x, -1.0) from test"
            );
        });
    }
}

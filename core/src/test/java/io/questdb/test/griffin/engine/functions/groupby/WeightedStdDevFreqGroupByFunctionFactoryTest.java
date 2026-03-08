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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class WeightedStdDevFreqGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testWeightedStddevFreqAgainstNonWeighted() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE freq_weights (value DOUBLE, freq DOUBLE)");
            execute("""
                    INSERT into freq_weights VALUES
                        (13, 1),
                        (42, 2),
                        (27, 3)
                    """);
            execute("CREATE TABLE samples (value DOUBLE)");
            execute("""
                    INSERT INTO samples VALUES
                        (13),
                        (42),
                        (42),
                        (27),
                        (27),
                        (27)
                    """);
            assertSql(
                    "weighted_stddev\n10.9848\n",
                    "SELECT round(weighted_stddev_freq(value, freq), 4) weighted_stddev FROM freq_weights"
            );
            assertSql(
                    "stddev\n10.9848\n",
                    "SELECT round(stddev_samp(value), 4) stddev FROM samples"
            );
        });
    }

    @Test
    public void testWeightedStddevFreqAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango as (SELECT 17.2151921 value, rnd_double() weight FROM long_sequence(100))");
            assertSql(
                    "weighted_stddev_freq\n0.0\n", "SELECT weighted_stddev_freq(value, weight) FROM tango"
            );
        });
    }

    @Test
    public void testWeightedStddevFreqDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango as (SELECT x::DOUBLE x FROM long_sequence(100))");
            assertSql(
                    "weighted_stddev_freq\n23.687784005919827\n",
                    "SELECT weighted_stddev_freq(x, 101.0 - x) FROM tango"
            );
        });
    }

    @Test
    public void testWeightedStddevFreqFirstNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(x DOUBLE)");
            execute("INSERT INTO 'tango' VALUES (null)");
            execute("INSERT INTO 'tango' SELECT x FROM long_sequence(100)");
            assertSql(
                    "weighted_stddev_freq\n23.687784005919827\n",
                    "SELECT weighted_stddev_freq(x, 101.0 - x) FROM tango"
            );
        });
    }

    @Test
    public void testWeightedStddevFreqFloatValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango as (SELECT x::FLOAT value, (101.0 - x)::FLOAT weight FROM long_sequence(100))");
            assertSql(
                    "weighted_stddev_freq\n23.687784005919827\n",
                    "SELECT weighted_stddev_freq(value, weight) FROM tango"
            );
        });
    }

    @Test
    public void testWeightedStddevFreqHugeValuesAndWeights() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango as (SELECT 100_000_000 * x x FROM long_sequence(1_000_000))");
            assertSql(
                    "weighted_stddev_freq\n2.357023782494708E13\n", "SELECT weighted_stddev_freq(x, x) FROM tango"
            );
        });
    }

    @Test
    public void testWeightedStddevFreqIntValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango as (SELECT x::INT value, (101 - x) weight FROM long_sequence(100))");
            assertSql(
                    "weighted_stddev_freq\n23.687784005919827\n",
                    "SELECT weighted_stddev_freq(value, weight) FROM tango"
            );
        });
    }

    @Test
    public void testWeightedStddevFreqNoValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(x int)");
            assertSql(
                    "weighted_stddev_freq\nnull\n", "SELECT weighted_stddev_freq(x, x) FROM tango"
            );
        });
    }

    @Test
    public void testWeightedStddevFreqNullValues() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "weighted_stddev_freq\nnull\n",
                "SELECT weighted_stddev_freq(x, 1.0) FROM (SELECT null::double x FROM long_sequence(100))"
        ));
    }

    @Test
    public void testWeightedStddevFreqNullValuesAndWeights() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "weighted_stddev_freq\nnull\n",
                "SELECT weighted_stddev_freq(x, x) FROM (SELECT null::double x FROM long_sequence(100))"
        ));
    }

    @Test
    public void testWeightedStddevFreqNullWeights() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "weighted_stddev_freq\nnull\n",
                "SELECT weighted_stddev_freq(1.0, x) FROM (SELECT null::double x FROM long_sequence(100))"
        ));
    }

    @Test
    public void testWeightedStddevFreqOneRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(x double)");
            execute("INSERT INTO 'tango' VALUES (17.2151920)");
            assertSql(
                    "weighted_stddev_freq\n0.0\n", "SELECT weighted_stddev_freq(x, x) FROM tango"
            );
        });
    }

    @Test
    public void testWeightedStddevFreqSampleByFill() throws Exception {
        assertMemoryLeak(() -> {
            // Create table with gaps: data at hour 0 and hour 2, gap at hour 1
            execute("CREATE TABLE test_fill (value DOUBLE, weight DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            // Hour 0: values 1,2,3 with weight 1 -> stddev ≈ 1.0
            execute("""
                    INSERT INTO test_fill VALUES
                        (1.0, 1.0, '1970-01-01T00:00'),
                        (2.0, 1.0, '1970-01-01T00:30'),
                        (3.0, 1.0, '1970-01-01T00:45')
                    """);
            // Hour 1: no data (gap to be filled)
            // Hour 2: values 1,2,3,4,5 with weight 1 -> stddev ≈ 1.58
            execute("""
                    INSERT INTO test_fill VALUES
                        (1.0, 1.0, '1970-01-01T02:00'),
                        (2.0, 1.0, '1970-01-01T02:15'),
                        (3.0, 1.0, '1970-01-01T02:30'),
                        (4.0, 1.0, '1970-01-01T02:45'),
                        (5.0, 1.0, '1970-01-01T02:50')
                    """);

            // Get the actual stddev values for hour 0 and hour 2
            String hour0Stddev = queryResult("SELECT round(weighted_stddev_freq(value, weight), 8) FROM test_fill WHERE ts < '1970-01-01T01'");
            String hour2Stddev = queryResult("SELECT round(weighted_stddev_freq(value, weight), 8) FROM test_fill WHERE ts >= '1970-01-01T02'");

            double stddev0 = Double.parseDouble(hour0Stddev.trim().split("\n")[1]);
            double stddev2 = Double.parseDouble(hour2Stddev.trim().split("\n")[1]);

            assertSql(
                    "ts\tround\n" +
                            "1970-01-01T00:00:00.000000Z\t" + stddev0 + "\n" +
                            "1970-01-01T01:00:00.000000Z\t" + stddev0 + "\n" +
                            "1970-01-01T02:00:00.000000Z\t" + stddev2 + "\n",
                    "SELECT ts, round(weighted_stddev_freq(value, weight), 8) FROM test_fill SAMPLE BY 1h FILL(PREV)"
            );
            assertSql(
                    "ts\tround\n" +
                            "1970-01-01T00:00:00.000000Z\t" + stddev0 + "\n" +
                            "1970-01-01T01:00:00.000000Z\tnull\n" +
                            "1970-01-01T02:00:00.000000Z\t" + stddev2 + "\n",
                    "SELECT ts, round(weighted_stddev_freq(value, weight), 8) FROM test_fill SAMPLE BY 1h FILL(NULL)"
            );
        });
    }

    @Test
    public void testWeightedStddevFreqSomeNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango as (SELECT x::DOUBLE x FROM long_sequence(100))");
            execute("INSERT INTO 'tango' VALUES (null)");
            assertSql(
                    "weighted_stddev_freq\n23.687784005919827\n",
                    "SELECT weighted_stddev_freq(x, 101.0 - x) FROM tango"
            );
        });
    }

    @Test
    public void testWeightedStddevFreqZeroAndNegativeWeight() throws Exception {
        assertMemoryLeak(() -> {
            assertSql(
                    """
                            weighted_stddev_freq
                            null
                            """,
                    "SELECT weighted_stddev_freq(x, 0) FROM long_sequence(10)"
            );
            assertSql(
                    """
                            weighted_stddev_freq
                            0.7071067811865476
                            """,
                    """
                            SELECT weighted_stddev_freq(
                                x,
                                CASE WHEN x < 3 THEN 1 ELSE 0 END
                            ) FROM long_sequence(10)
                            """
            );
            // Weights sum to zero
            assertSql(
                    """
                            weighted_stddev_freq
                            null
                            """,
                    "SELECT weighted_stddev_freq(x, x - 6) FROM long_sequence(11)"
            );
            // Weights all negative
            assertSql(
                    """
                            weighted_stddev_freq
                            null
                            """,
                    "SELECT weighted_stddev_freq(x, x - 12) FROM long_sequence(11)"
            );
        });
    }

    private String queryResult(String query) throws Exception {
        StringSink sink = new StringSink();
        TestUtils.printSql(engine, sqlExecutionContext, query, sink);
        return sink.toString();
    }
}

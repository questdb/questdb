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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class WeightedStdDevRelGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testWeightedStddevRelAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango as (SELECT 17.2151921 value, rnd_double() weight FROM long_sequence(100))");
            assertQuery("SELECT weighted_stddev_rel(value, weight) FROM tango")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("weighted_stddev_rel\n0.0\n");
            assertQuery("SELECT weighted_stddev(value, weight) FROM tango")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("weighted_stddev\n0.0\n");
        });
    }

    @Test
    public void testWeightedStddevRelDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango as (SELECT x::DOUBLE x FROM long_sequence(100))");
            assertQuery("SELECT weighted_stddev_rel(x, 101.0 - x) FROM tango")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("weighted_stddev_rel\n23.84414071334121\n");
            assertQuery("SELECT weighted_stddev(x, 101.0 - x) FROM tango")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("weighted_stddev\n23.84414071334121\n");
        });
    }

    @Test
    public void testWeightedStddevRelFirstNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(x DOUBLE)");
            execute("INSERT INTO 'tango' VALUES (null)");
            execute("INSERT INTO 'tango' SELECT x FROM long_sequence(100)");
            assertQuery("SELECT weighted_stddev_rel(x, 101.0 - x) FROM tango")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("weighted_stddev_rel\n23.84414071334121\n");
            assertQuery("SELECT weighted_stddev(x, 101.0 - x) FROM tango")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("weighted_stddev\n23.84414071334121\n");
        });
    }

    @Test
    public void testWeightedStddevRelFloatValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango as (SELECT x::FLOAT value, (101.0 - x)::FLOAT weight FROM long_sequence(100))");
            assertQuery("SELECT weighted_stddev_rel(value, weight) FROM tango")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("weighted_stddev_rel\n23.84414071334121\n");
            assertQuery("SELECT weighted_stddev(value, weight) FROM tango")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("weighted_stddev\n23.84414071334121\n");
        });
    }

    @Test
    public void testWeightedStddevRelHugeValuesAndWeights() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango as (SELECT 100_000_000 * x x FROM long_sequence(1_000_000))");
            assertQuery("SELECT weighted_stddev_rel(x, x) FROM tango")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("weighted_stddev_rel\n2.3570253538446816E13\n");
            assertQuery("SELECT weighted_stddev(x, x) FROM tango")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("weighted_stddev\n2.3570253538446816E13\n");
        });
    }

    @Test
    public void testWeightedStddevRelIntValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango as (SELECT x::INT value, (101 - x) weight FROM long_sequence(100))");
            assertQuery("SELECT weighted_stddev_rel(value, weight) FROM tango")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("weighted_stddev_rel\n23.84414071334121\n");
            assertQuery("SELECT weighted_stddev(value, weight) FROM tango")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("weighted_stddev\n23.84414071334121\n");
        });
    }

    @Test
    public void testWeightedStddevRelNoValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(x int)");
            assertQuery("SELECT weighted_stddev_rel(x, x) FROM tango")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("weighted_stddev_rel\nnull\n");
            assertQuery("SELECT weighted_stddev(x, x) FROM tango")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("weighted_stddev\nnull\n");
        });
    }

    @Test
    public void testWeightedStddevRelNullValues() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("SELECT weighted_stddev_rel(x, 1.0) FROM (SELECT null::double x FROM long_sequence(100))")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("weighted_stddev_rel\nnull\n");
            assertQuery("SELECT weighted_stddev(x, 1.0) FROM (SELECT null::double x FROM long_sequence(100))")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("weighted_stddev\nnull\n");
        });
    }

    @Test
    public void testWeightedStddevRelNullValuesAndWeights() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("SELECT weighted_stddev_rel(x, x) FROM (SELECT null::double x FROM long_sequence(100))")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("weighted_stddev_rel\nnull\n");
            assertQuery("SELECT weighted_stddev(x, x) FROM (SELECT null::double x FROM long_sequence(100))")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("weighted_stddev\nnull\n");
        });
    }

    @Test
    public void testWeightedStddevRelNullWeights() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("SELECT weighted_stddev_rel(1.0, x) FROM (SELECT null::double x FROM long_sequence(100))")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("weighted_stddev_rel\nnull\n");
            assertQuery("SELECT weighted_stddev(1.0, x) FROM (SELECT null::double x FROM long_sequence(100))")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("weighted_stddev\nnull\n");
        });
    }

    @Test
    public void testWeightedStddevRelOneRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(x double)");
            execute("INSERT INTO 'tango' VALUES (17.2151920)");
            assertQuery("SELECT weighted_stddev_rel(x, x) FROM tango")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("weighted_stddev_rel\nnull\n");
            assertQuery("SELECT weighted_stddev(x, x) FROM tango")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("weighted_stddev\nnull\n");
        });
    }

    @Test
    public void testWeightedStddevRelSampleByFill() throws Exception {
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
            String hour0Stddev = queryResult("SELECT round(weighted_stddev_rel(value, weight), 8) FROM test_fill WHERE ts < '1970-01-01T01'");
            String hour2Stddev = queryResult("SELECT round(weighted_stddev_rel(value, weight), 8) FROM test_fill WHERE ts >= '1970-01-01T02'");

            double stddev0 = Double.parseDouble(hour0Stddev.trim().split("\n")[1]);
            double stddev2 = Double.parseDouble(hour2Stddev.trim().split("\n")[1]);

            assertQuery("SELECT ts, round(weighted_stddev_rel(value, weight), 8) FROM test_fill SAMPLE BY 1h FILL(PREV)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("ts\tround\n" +
                            "1970-01-01T00:00:00.000000Z\t" + stddev0 + "\n" +
                            "1970-01-01T01:00:00.000000Z\t" + stddev0 + "\n" +
                            "1970-01-01T02:00:00.000000Z\t" + stddev2 + "\n");
            assertQuery("SELECT ts, round(weighted_stddev_rel(value, weight), 8) FROM test_fill SAMPLE BY 1h FILL(NULL)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("ts\tround\n" +
                            "1970-01-01T00:00:00.000000Z\t" + stddev0 + "\n" +
                            "1970-01-01T01:00:00.000000Z\tnull\n" +
                            "1970-01-01T02:00:00.000000Z\t" + stddev2 + "\n");
        });
    }

    @Test
    public void testWeightedStddevRelSomeNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango as (SELECT x::DOUBLE x FROM long_sequence(100))");
            execute("INSERT INTO 'tango' VALUES (null)");
            assertQuery("SELECT weighted_stddev_rel(x, 101.0 - x) FROM tango")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("weighted_stddev_rel\n23.84414071334121\n");
            assertQuery("SELECT weighted_stddev(x, 101.0 - x) FROM tango")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("weighted_stddev\n23.84414071334121\n");
        });
    }

    @Test
    public void testWeightedStddevRelZeroAndNegativeWeight() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("SELECT weighted_stddev_rel(x, 0) FROM long_sequence(10)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            weighted_stddev_rel
                            null
                            """);
            assertQuery("SELECT weighted_stddev(x, 0) FROM long_sequence(10)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            weighted_stddev
                            null
                            """);
            assertQuery("""
                    SELECT weighted_stddev_rel(
                        x,
                        CASE WHEN x < 3 THEN 1 ELSE 0 END
                    ) FROM long_sequence(10)
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            weighted_stddev_rel
                            0.7071067811865476
                            """);
            assertQuery("""
                    SELECT weighted_stddev(
                        x,
                        CASE WHEN x < 3 THEN 1 ELSE 0 END
                    ) FROM long_sequence(10)
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            weighted_stddev
                            0.7071067811865476
                            """);
            // Weights sum to zero
            assertQuery("SELECT weighted_stddev_rel(x, x - 6) FROM long_sequence(11)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            weighted_stddev_rel
                            null
                            """);
            assertQuery("SELECT weighted_stddev(x, x - 6) FROM long_sequence(11)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            weighted_stddev
                            null
                            """);
            // Weights all negative
            assertQuery("SELECT weighted_stddev_rel(x, x - 12) FROM long_sequence(11)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            weighted_stddev_rel
                            null
                            """);
            assertQuery("SELECT weighted_stddev(x, x - 12) FROM long_sequence(11)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            weighted_stddev
                            null
                            """);
        });
    }

    private String queryResult(String query) throws Exception {
        StringSink sink = new StringSink();
        TestUtils.printSql(engine, sqlExecutionContext, query, sink);
        return sink.toString();
    }
}

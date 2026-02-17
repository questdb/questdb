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

package io.questdb.test.griffin.engine.functions.date;

import io.questdb.std.datetime.nanotime.StationaryNanosClock;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.StationaryMicrosClock;
import org.junit.BeforeClass;
import org.junit.Test;

public class TimestampSequenceFunctionFactoryTest extends AbstractCairoTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        testMicrosClock = StationaryMicrosClock.INSTANCE;
        testNanoClock = StationaryNanosClock.INSTANCE;
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testInitCall() throws Exception {
        final String expected = """
                ts
                2021-04-25T00:00:00.000000Z
                2021-04-25T00:00:00.300000Z
                2021-04-25T00:00:00.600000Z
                2021-04-25T00:00:01.100000Z
                2021-04-25T00:00:01.800000Z
                2021-04-25T00:00:02.000000Z
                2021-04-25T00:00:02.600000Z
                2021-04-25T00:00:02.900000Z
                2021-04-25T00:00:03.300000Z
                2021-04-25T00:00:03.800000Z
                """;

        assertSql(
                expected,
                """
                        SELECT timestamp_sequence(
                                 to_timestamp('2021-04-25T00:00:00', 'yyyy-MM-ddTHH:mm:ss'),
                                 rnd_long(1,10,0) * 100000L
                        ) ts from long_sequence(10, 900, 800)"""
        );
    }

    @Test
    public void testInitCallNano() throws Exception {
        final String expected = """
                ts
                2021-04-25T00:00:00.000000000Z
                2021-04-25T00:00:00.300000000Z
                2021-04-25T00:00:00.600000000Z
                2021-04-25T00:00:01.100000000Z
                2021-04-25T00:00:01.800000000Z
                2021-04-25T00:00:02.000000000Z
                2021-04-25T00:00:02.600000000Z
                2021-04-25T00:00:02.900000000Z
                2021-04-25T00:00:03.300000000Z
                2021-04-25T00:00:03.800000000Z
                """;

        assertSql(
                expected,
                """
                        SELECT timestamp_sequence_ns(
                                 to_timestamp_ns('2021-04-25T00:00:00', 'yyyy-MM-ddTHH:mm:ss'),
                                 rnd_long(1,10,0) * 100000000L
                        ) ts from long_sequence(10, 900, 800)"""
        );
    }

    @Test
    public void testNanoTimestampSequenceWithZeroStartValue() throws Exception {
        final String expected = """
                ac\tts
                1\t1970-01-01T00:00:00.000000000Z
                2\t1970-01-01T00:00:00.000001000Z
                3\t1970-01-01T00:00:00.000002000Z
                4\t1970-01-01T00:00:00.000003000Z
                5\t1970-01-01T00:00:00.000004000Z
                6\t1970-01-01T00:00:00.000005000Z
                7\t1970-01-01T00:00:00.000006000Z
                8\t1970-01-01T00:00:00.000007000Z
                9\t1970-01-01T00:00:00.000008000Z
                10\t1970-01-01T00:00:00.000009000Z
                """;

        assertSql(
                expected,
                "select x ac, timestamp_sequence_ns(0, 1000) ts from long_sequence(10)"
        );
    }

    @Test
    public void testStableProjection() throws Exception {
        // test that output of timestamp_sequence() does not change
        // within the same row.
        // in other words: timestamps on the same row must differ exactly by 1 hour due to the dateadd()

        allowFunctionMemoization();

        String expected = """
                ts\tdateadd
                2021-04-25T00:00:00.000000Z\t2021-04-25T01:00:00.000000Z
                2021-04-25T00:05:00.000000Z\t2021-04-25T01:05:00.000000Z
                2021-04-25T00:10:00.000000Z\t2021-04-25T01:10:00.000000Z
                2021-04-25T00:15:00.000000Z\t2021-04-25T01:15:00.000000Z
                2021-04-25T00:20:00.000000Z\t2021-04-25T01:20:00.000000Z
                2021-04-25T00:25:00.000000Z\t2021-04-25T01:25:00.000000Z
                2021-04-25T00:30:00.000000Z\t2021-04-25T01:30:00.000000Z
                2021-04-25T00:35:00.000000Z\t2021-04-25T01:35:00.000000Z
                2021-04-25T00:40:00.000000Z\t2021-04-25T01:40:00.000000Z
                2021-04-25T00:45:00.000000Z\t2021-04-25T01:45:00.000000Z
                """;

        assertSql(
                expected,
                """
                        SELECT timestamp_sequence(
                                 to_timestamp('2021-04-25T00:00:00', 'yyyy-MM-ddTHH:mm:ss'),
                                 300_000_000L
                        ) ts, dateadd('h', 1, ts) from long_sequence(10)"""
        );

        expected = """
                ts\tdateadd
                2021-04-25T00:00:00.000000000Z\t2021-04-25T01:00:00.000000000Z
                2021-04-25T00:05:00.000000000Z\t2021-04-25T01:05:00.000000000Z
                2021-04-25T00:10:00.000000000Z\t2021-04-25T01:10:00.000000000Z
                2021-04-25T00:15:00.000000000Z\t2021-04-25T01:15:00.000000000Z
                2021-04-25T00:20:00.000000000Z\t2021-04-25T01:20:00.000000000Z
                2021-04-25T00:25:00.000000000Z\t2021-04-25T01:25:00.000000000Z
                2021-04-25T00:30:00.000000000Z\t2021-04-25T01:30:00.000000000Z
                2021-04-25T00:35:00.000000000Z\t2021-04-25T01:35:00.000000000Z
                2021-04-25T00:40:00.000000000Z\t2021-04-25T01:40:00.000000000Z
                2021-04-25T00:45:00.000000000Z\t2021-04-25T01:45:00.000000000Z
                """;

        assertSql(
                expected,
                """
                        SELECT timestamp_sequence_ns(
                                 to_timestamp_ns('2021-04-25T00:00:00', 'yyyy-MM-ddTHH:mm:ss'),
                                 300_000_000_000L
                        ) ts, dateadd('h', 1, ts) from long_sequence(10)"""
        );
    }

    @Test
    public void testTimestampSequenceWithSystimestampCall() throws Exception {
        String expected = """
                ac\tts
                1\t1970-01-01T00:00:00.000000Z
                2\t1970-01-01T00:00:00.001000Z
                3\t1970-01-01T00:00:00.002000Z
                4\t1970-01-01T00:00:00.003000Z
                5\t1970-01-01T00:00:00.004000Z
                6\t1970-01-01T00:00:00.005000Z
                7\t1970-01-01T00:00:00.006000Z
                8\t1970-01-01T00:00:00.007000Z
                9\t1970-01-01T00:00:00.008000Z
                10\t1970-01-01T00:00:00.009000Z
                """;

        assertSql(
                expected,
                "select x ac, timestamp_sequence(systimestamp(), 1000) ts from long_sequence(10)"
        );

        expected = """
                ac\tts
                1\t1970-01-01T00:00:00.000000000Z
                2\t1970-01-01T00:00:00.000001000Z
                3\t1970-01-01T00:00:00.000002000Z
                4\t1970-01-01T00:00:00.000003000Z
                5\t1970-01-01T00:00:00.000004000Z
                6\t1970-01-01T00:00:00.000005000Z
                7\t1970-01-01T00:00:00.000006000Z
                8\t1970-01-01T00:00:00.000007000Z
                9\t1970-01-01T00:00:00.000008000Z
                10\t1970-01-01T00:00:00.000009000Z
                """;

        assertSql(
                expected,
                "select x ac, timestamp_sequence_ns(systimestamp_ns(), 1000) ts from long_sequence(10)"
        );
    }

    @Test
    public void testTimestampSequenceWithZeroStartValue() throws Exception {
        final String expected = """
                ac\tts
                1\t1970-01-01T00:00:00.000000Z
                2\t1970-01-01T00:00:00.001000Z
                3\t1970-01-01T00:00:00.002000Z
                4\t1970-01-01T00:00:00.003000Z
                5\t1970-01-01T00:00:00.004000Z
                6\t1970-01-01T00:00:00.005000Z
                7\t1970-01-01T00:00:00.006000Z
                8\t1970-01-01T00:00:00.007000Z
                9\t1970-01-01T00:00:00.008000Z
                10\t1970-01-01T00:00:00.009000Z
                """;

        assertSql(
                expected,
                "select x ac, timestamp_sequence(0, 1000) ts from long_sequence(10)"
        );
    }
}
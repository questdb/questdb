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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class MonthTimestampSamplerTest {
    private final TestTimestampType timestampType;

    public MonthTimestampSamplerTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testNextTimestamp() throws Exception {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final TimestampSampler sampler = timestampDriver.getTimestampSampler(1, 'M', 0);

        final String[] src = new String[]{
                "2013-12-31T00:00:00.000000Z",
                "2014-01-01T00:00:00.000000Z",
                "2020-01-01T12:12:12.123456Z",
        };
        final String[] next = new String[]{
                "2014-01-01T00:00:00.000000Z",
                "2014-02-01T00:00:00.000000Z",
                "2020-02-01T00:00:00.000000Z",
        };
        Assert.assertEquals(src.length, next.length);

        for (int i = 0; i < src.length; i++) {
            long ts = timestampDriver.parseFloorLiteral(src[i]);
            long nextTs = sampler.nextTimestamp(ts);
            Assert.assertEquals(timestampDriver.parseFloorLiteral(next[i]), nextTs);
        }
    }

    @Test
    public void testNextTimestampWithStep() throws Exception {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final TimestampSampler sampler = timestampDriver.getTimestampSampler(1, 'M', 0);

        final String[] src = new String[]{
                "2013-12-31T00:00:00.000000Z",
                "2014-01-01T00:00:00.000000Z",
                "2020-01-01T12:12:12.123456Z",
        };
        final String[] next = new String[]{
                "2014-03-01T00:00:00.000000Z",
                "2014-04-01T00:00:00.000000Z",
                "2020-04-01T00:00:00.000000Z",
        };
        Assert.assertEquals(src.length, next.length);

        for (int i = 0; i < src.length; i++) {
            long ts = timestampDriver.parseFloorLiteral(src[i]);
            long nextTs = sampler.nextTimestamp(ts, 3);
            Assert.assertEquals(timestampDriver.parseFloorLiteral(next[i]), nextTs);
        }
    }

    @Test
    public void testPreviousTimestamp() throws Exception {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final TimestampSampler sampler = timestampDriver.getTimestampSampler(1, 'M', 0);

        final String[] src = new String[]{
                "2013-12-31T00:00:00.000000Z",
                "2014-01-01T00:00:00.000000Z",
                "2020-02-01T12:12:12.123456Z",
        };
        final String[] prev = new String[]{
                "2013-11-01T00:00:00.000000Z",
                "2013-12-01T00:00:00.000000Z",
                "2020-01-01T00:00:00.000000Z",
        };
        Assert.assertEquals(src.length, prev.length);

        for (int i = 0; i < src.length; i++) {
            long ts = timestampDriver.parseFloorLiteral(src[i]);
            long prevTs = sampler.previousTimestamp(ts);
            Assert.assertEquals(timestampDriver.parseFloorLiteral(prev[i]), prevTs);
        }
    }

    @Test
    public void testRound() throws Exception {
        final String[] src = new String[]{
                "2013-12-31T00:00:00.000000Z",
                "2014-01-01T00:00:00.000000Z",
                "2014-02-12T12:12:12.123456Z",
                "2022-02-23T17:08:58.000000Z",
                "2014-02-12T12:12:12.123456Z",
                "2024-11-12T12:12:12.123456Z",
        };
        final String[] rounded = new String[]{
                "2013-12-01T00:00:00.000000Z",
                "2014-01-01T00:00:00.000000Z",
                "2014-02-01T00:00:00.000000Z",
                "2022-02-01T00:00:00.000000Z",
                "2014-01-01T00:00:00.000000Z",
                "2024-11-01T00:00:00.000000Z",
        };
        final int[] strides = new int[]{
                1,
                1,
                1,
                1,
                3,
                10,
        };
        Assert.assertEquals(src.length, rounded.length);
        Assert.assertEquals(src.length, strides.length);

        final TimestampDriver timestampDriver = timestampType.getDriver();
        for (int i = 0; i < src.length; i++) {
            final TimestampSampler sampler = timestampDriver.getTimestampSampler(strides[i], 'M', 0);
            final long ts = timestampDriver.parseFloorLiteral(src[i]);
            final long roundedTs = sampler.round(ts);
            Assert.assertEquals(timestampDriver.parseFloorLiteral(rounded[i]), roundedTs);
        }
    }

    @Test
    public void testRoundMatchesFloor() throws Exception {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final String[] src = new String[]{
                "1967-12-31T01:11:42.123456789Z",
                "1970-01-01T00:00:00.000000000Z",
                "1970-01-05T00:00:00.000000000Z",
                "2013-12-31T00:00:00.000000000Z",
                "2014-01-01T01:12:12.000000001Z",
                "2014-02-12T12:12:12.123456789Z",
                "2025-09-04T10:45:01.987654321Z",
        };

        final TimestampSampler sampler = timestampDriver.getTimestampSampler(1, 'M', 0);
        sampler.setStart(0);

        final TimestampDriver.TimestampFloorMethod floorMethod = timestampDriver.getTimestampFloorMethod("month");
        final TimestampDriver.TimestampFloorWithStrideMethod floorWithStrideMethod = timestampDriver.getTimestampFloorWithStrideMethod("month");
        final TimestampDriver.TimestampFloorWithOffsetMethod floorWithOffsetMethod = timestampDriver.getTimestampFloorWithOffsetMethod('M');
        for (int i = 0; i < src.length; i++) {
            final long ts = timestampDriver.parseFloorLiteral(src[i]);
            final long roundedTs = sampler.round(ts);
            Assert.assertEquals(floorMethod.floor(ts), roundedTs);
            Assert.assertEquals(floorWithStrideMethod.floor(ts, 1), roundedTs);
            Assert.assertEquals(floorWithOffsetMethod.floor(ts, 1, 0), roundedTs);
        }
    }

    @Test
    public void testSimple() throws Exception {
        final StringSink sink = new StringSink();
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final TimestampSampler sampler = timestampDriver.getTimestampSampler(6, 'M', 0);

        long timestamp = timestampDriver.parseFloorLiteral("2018-11-16T15:00:00.000000Z");
        sampler.setStart(timestamp);

        for (int i = 0; i < 20; i++) {
            long ts = sampler.nextTimestamp(timestamp);
            sink.putISODate(timestampDriver, ts).put('\n');
            Assert.assertEquals(timestamp, sampler.previousTimestamp(ts));
            timestamp = ts;
        }

        TestUtils.assertEquals(
                AbstractCairoTest.replaceTimestampSuffix(
                        "2019-05-16T15:00:00.000000Z\n" +
                                "2019-11-16T15:00:00.000000Z\n" +
                                "2020-05-16T15:00:00.000000Z\n" +
                                "2020-11-16T15:00:00.000000Z\n" +
                                "2021-05-16T15:00:00.000000Z\n" +
                                "2021-11-16T15:00:00.000000Z\n" +
                                "2022-05-16T15:00:00.000000Z\n" +
                                "2022-11-16T15:00:00.000000Z\n" +
                                "2023-05-16T15:00:00.000000Z\n" +
                                "2023-11-16T15:00:00.000000Z\n" +
                                "2024-05-16T15:00:00.000000Z\n" +
                                "2024-11-16T15:00:00.000000Z\n" +
                                "2025-05-16T15:00:00.000000Z\n" +
                                "2025-11-16T15:00:00.000000Z\n" +
                                "2026-05-16T15:00:00.000000Z\n" +
                                "2026-11-16T15:00:00.000000Z\n" +
                                "2027-05-16T15:00:00.000000Z\n" +
                                "2027-11-16T15:00:00.000000Z\n" +
                                "2028-05-16T15:00:00.000000Z\n" +
                                "2028-11-16T15:00:00.000000Z\n",
                        timestampType.getTypeName()
                ),
                sink
        );
    }
}

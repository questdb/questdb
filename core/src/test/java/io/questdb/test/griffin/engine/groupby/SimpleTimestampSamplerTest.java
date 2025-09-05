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
import io.questdb.griffin.engine.groupby.SimpleTimestampSampler;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.nanotime.Nanos;
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
public class SimpleTimestampSamplerTest {
    private final TestTimestampType timestampType;

    public SimpleTimestampSamplerTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testNextTimestamp() throws NumericException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final SimpleTimestampSampler sampler = new SimpleTimestampSampler(timestampDriver.fromMinutes(1), timestampType.getTimestampType());
        sampler.setStart(0);

        final String[] src = new String[]{
                "2013-12-31T00:00:00.000000000Z",
                "2014-01-01T00:00:00.000000000Z",
                "2020-01-01T12:12:12.123456789Z",
        };
        final String[] next = new String[]{
                "2013-12-31T00:01:00.000000000Z",
                "2014-01-01T00:01:00.000000000Z",
                "2020-01-01T12:13:12.123456789Z",
        };
        Assert.assertEquals(src.length, next.length);

        for (int i = 0; i < src.length; i++) {
            long ts = timestampDriver.parseFloorLiteral(src[i]);
            long nextTs = sampler.nextTimestamp(ts);
            Assert.assertEquals(timestampDriver.parseFloorLiteral(next[i]), nextTs);
        }
    }

    @Test
    public void testNextTimestampWithStep() throws NumericException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final SimpleTimestampSampler sampler = new SimpleTimestampSampler(timestampDriver.fromSeconds(1), timestampType.getTimestampType());
        sampler.setStart(0);

        final String[] src = new String[]{
                "2013-12-31T00:00:00.000000000Z",
                "2014-01-01T00:00:00.000000000Z",
                "2020-01-01T12:12:59.123456789Z",
        };
        final String[] next = new String[]{
                "2013-12-31T00:00:03.000000000Z",
                "2014-01-01T00:00:03.000000000Z",
                "2020-01-01T12:13:02.123456789Z",
        };
        Assert.assertEquals(src.length, next.length);

        for (int i = 0; i < src.length; i++) {
            long ts = timestampDriver.parseFloorLiteral(src[i]);
            long nextTs = sampler.nextTimestamp(ts, 3);
            Assert.assertEquals(timestampDriver.parseFloorLiteral(next[i]), nextTs);
        }
    }

    @Test
    public void testPreviousTimestamp() throws NumericException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final SimpleTimestampSampler sampler = new SimpleTimestampSampler(timestampDriver.fromHours(1), timestampType.getTimestampType());
        sampler.setStart(0);

        final String[] src = new String[]{
                "2013-12-31T00:00:00.000000000Z",
                "2014-01-01T00:00:00.000000000Z",
                "2020-02-01T12:12:12.123456789Z",
        };
        final String[] prev = new String[]{
                "2013-12-30T23:00:00.000000000Z",
                "2013-12-31T23:00:00.000000000Z",
                "2020-02-01T11:12:12.123456789Z",
        };
        Assert.assertEquals(src.length, prev.length);

        for (int i = 0; i < src.length; i++) {
            long ts = timestampDriver.parseFloorLiteral(src[i]);
            long prevTs = sampler.previousTimestamp(ts);
            Assert.assertEquals(timestampDriver.parseFloorLiteral(prev[i]), prevTs);
        }
    }

    @Test
    public void testRound() throws NumericException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final String[] src = new String[]{
                "1967-12-31T01:11:42.123456789Z",
                "1970-01-01T00:00:00.000000000Z",
                "1970-01-05T00:00:00.000000000Z",
                "2013-12-31T00:00:00.000000000Z",
                "2014-01-01T01:12:12.000000001Z",
                "2014-02-12T12:12:12.123456789Z",
                "2014-02-12T12:12:12.123456789Z",
                "2014-02-12T12:12:12.123456789Z",
                "2014-02-12T12:12:12.123456789Z",
        };
        final String[] rounded = new String[]{
                "1967-12-31T01:00:00.000000000Z",
                "1970-01-01T00:00:00.000000000Z",
                "1970-01-04T00:00:00.000000000Z",
                "2013-12-31T00:00:00.000000000Z",
                "2014-01-01T01:00:00.000000000Z",
                "2014-02-12T12:00:00.000000000Z",
                "2014-02-12T12:12:00.000000000Z",
                "2014-02-12T00:00:00.000000000Z",
                "2014-02-09T00:00:00.000000000Z",
        };
        final long[] strides = new long[]{
                timestampDriver.fromHours(1),
                timestampDriver.fromDays(1),
                timestampDriver.fromDays(3),
                timestampDriver.fromHours(1),
                timestampDriver.fromHours(1),
                timestampDriver.fromHours(1),
                timestampDriver.fromMinutes(1),
                timestampDriver.fromDays(1),
                timestampDriver.fromDays(10),
        };
        Assert.assertEquals(src.length, rounded.length);
        Assert.assertEquals(src.length, strides.length);

        for (int i = 0; i < src.length; i++) {
            final SimpleTimestampSampler sampler = new SimpleTimestampSampler(strides[i], timestampType.getTimestampType());
            sampler.setStart(0);
            final long ts = timestampDriver.parseFloorLiteral(src[i]);
            final long roundedTs = sampler.round(ts);
            Assert.assertEquals(
                    "expected " + rounded[i] + ", got " + Nanos.toString(timestampDriver.toNanos(roundedTs)),
                    timestampDriver.parseFloorLiteral(rounded[i]),
                    roundedTs
            );
        }
    }

    @Test
    public void testRoundMatchesFloor() throws NumericException {
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

        final SimpleTimestampSampler sampler = new SimpleTimestampSampler(timestampDriver.fromDays(1), timestampType.getTimestampType());
        sampler.setStart(0);

        final TimestampDriver.TimestampFloorMethod floorMethod = timestampDriver.getTimestampFloorMethod("day");
        final TimestampDriver.TimestampFloorWithStrideMethod floorWithStrideMethod = timestampDriver.getTimestampFloorWithStrideMethod("day");
        final TimestampDriver.TimestampFloorWithOffsetMethod floorWithOffsetMethod = timestampDriver.getTimestampFloorWithOffsetMethod('d');
        for (int i = 0; i < src.length; i++) {
            final long ts = timestampDriver.parseFloorLiteral(src[i]);
            final long roundedTs = sampler.round(ts);
            Assert.assertEquals(floorMethod.floor(ts), roundedTs);
            Assert.assertEquals(floorWithStrideMethod.floor(ts, 1), roundedTs);
            Assert.assertEquals(floorWithOffsetMethod.floor(ts, 1, 0), roundedTs);
        }
    }

    @Test
    public void testSimple() throws NumericException {
        final StringSink sink = new StringSink();
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final SimpleTimestampSampler sampler = new SimpleTimestampSampler(timestampDriver.fromHours(1), timestampType.getTimestampType());

        long timestamp = timestampDriver.parseFloorLiteral("2018-11-16T15:00:00.000000Z");
        sampler.setStart(timestamp);

        for (int i = 0; i < 10; i++) {
            long ts = sampler.nextTimestamp(timestamp);
            sink.putISODate(timestampDriver, ts).put('\n');
            Assert.assertEquals(timestamp, sampler.previousTimestamp(ts));
            timestamp = ts;
        }

        TestUtils.assertEquals(
                AbstractCairoTest.replaceTimestampSuffix(
                        "2018-11-16T16:00:00.000000Z\n" +
                                "2018-11-16T17:00:00.000000Z\n" +
                                "2018-11-16T18:00:00.000000Z\n" +
                                "2018-11-16T19:00:00.000000Z\n" +
                                "2018-11-16T20:00:00.000000Z\n" +
                                "2018-11-16T21:00:00.000000Z\n" +
                                "2018-11-16T22:00:00.000000Z\n" +
                                "2018-11-16T23:00:00.000000Z\n" +
                                "2018-11-17T00:00:00.000000Z\n" +
                                "2018-11-17T01:00:00.000000Z\n",
                        timestampType.getTypeName()
                ),
                sink
        );
    }
}

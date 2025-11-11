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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.WeekTimestampMicrosSampler;
import io.questdb.griffin.engine.groupby.WeekTimestampNanosSampler;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.datetime.nanotime.Nanos;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class WeekTimestampSamplerTest {
    private static final String FIXED_PART = "T15:31:57.000000Z\n";

    @Test
    public void testRound() throws NumericException {
        testRound(1, "2025-08-24T23:59:59.999999Z", "2025-08-18T00:00:00.000000Z");
        testRound(1, "2025-08-25T00:00:00.000000Z", "2025-08-25T00:00:00.000000Z");
        testRound(1, "2026-01-01T00:00:00.000000Z", "2025-12-29T00:00:00.000000Z");

        testRound(50, "1970-01-01T00:00:00.000000Z", "1969-12-29T00:00:00.000000Z");
        testRound(50, "2051-01-01T00:00:00.000000Z", "2050-06-27T00:00:00.000000Z");
        testRound(100, "2024-01-01T00:00:00.000000Z", "2023-08-28T00:00:00.000000Z");
        testRound(1000, "2025-12-31T23:59:59.999999Z", "2008-04-28T00:00:00.000000Z");
    }

    @Test
    public void testRoundMatchesFloorMicros() throws Exception {
        testRoundMatchesFloor(MicrosTimestampDriver.INSTANCE);
    }

    @Test
    public void testRoundMatchesFloorNanos() throws Exception {
        testRoundMatchesFloor(NanosTimestampDriver.INSTANCE);
    }

    @Test
    public void testSingleStep() throws NumericException {
        testSampler(
                1,
                "2018-12-07" + FIXED_PART +
                        "2018-12-28" + FIXED_PART +
                        "2019-01-18" + FIXED_PART +
                        "2019-02-08" + FIXED_PART +
                        "2019-03-01" + FIXED_PART +
                        "2019-03-22" + FIXED_PART +
                        "2019-04-12" + FIXED_PART +
                        "2019-05-03" + FIXED_PART +
                        "2019-05-24" + FIXED_PART +
                        "2019-06-14" + FIXED_PART +
                        "2019-07-05" + FIXED_PART +
                        "2019-07-26" + FIXED_PART +
                        "2019-08-16" + FIXED_PART +
                        "2019-09-06" + FIXED_PART +
                        "2019-09-27" + FIXED_PART +
                        "2019-10-18" + FIXED_PART +
                        "2019-11-08" + FIXED_PART +
                        "2019-11-29" + FIXED_PART +
                        "2019-12-20" + FIXED_PART +
                        "2020-01-10" + FIXED_PART
        );
    }

    @Test
    public void testTripleStep() throws NumericException {
        testSampler(
                3,
                "2019-01-18" + FIXED_PART +
                        "2019-03-22" + FIXED_PART +
                        "2019-05-24" + FIXED_PART +
                        "2019-07-26" + FIXED_PART +
                        "2019-09-27" + FIXED_PART +
                        "2019-11-29" + FIXED_PART +
                        "2020-01-31" + FIXED_PART +
                        "2020-04-03" + FIXED_PART +
                        "2020-06-05" + FIXED_PART +
                        "2020-08-07" + FIXED_PART +
                        "2020-10-09" + FIXED_PART +
                        "2020-12-11" + FIXED_PART +
                        "2021-02-12" + FIXED_PART +
                        "2021-04-16" + FIXED_PART +
                        "2021-06-18" + FIXED_PART +
                        "2021-08-20" + FIXED_PART +
                        "2021-10-22" + FIXED_PART +
                        "2021-12-24" + FIXED_PART +
                        "2022-02-25" + FIXED_PART +
                        "2022-04-29" + FIXED_PART
        );
    }

    private void testRound(int stepWeeks, String timestamp, String expectedRounded) throws NumericException {
        final WeekTimestampMicrosSampler samplerUs = new WeekTimestampMicrosSampler(stepWeeks);
        samplerUs.setStart(0);
        final long tsUs = MicrosFormatUtils.parseUTCTimestamp(timestamp);
        final long expectedUs = MicrosFormatUtils.parseUTCTimestamp(expectedRounded);
        Assert.assertEquals(expectedUs, samplerUs.round(tsUs));

        final WeekTimestampNanosSampler samplerNs = new WeekTimestampNanosSampler(stepWeeks);
        samplerNs.setStart(0);
        final long tsNs = tsUs * Nanos.MICRO_NANOS;
        Assert.assertEquals(expectedUs * Nanos.MICRO_NANOS, samplerNs.round(tsNs));
    }

    private void testRoundMatchesFloor(TimestampDriver timestampDriver) throws NumericException, SqlException {
        final String[] src = new String[]{
                "1967-12-31T01:11:42.123456789Z",
                "1970-01-01T00:00:00.000000000Z",
                "1970-01-05T00:00:00.000000000Z",
                "2013-12-31T00:00:00.000000000Z",
                "2014-01-01T01:12:12.000000001Z",
                "2014-02-12T12:12:12.123456789Z",
                "2025-09-04T10:45:01.987654321Z",
        };

        final TimestampSampler sampler = timestampDriver.getTimestampSampler(1, 'w', 0);
        sampler.setStart(0);

        final TimestampDriver.TimestampFloorMethod floorMethod = timestampDriver.getTimestampFloorMethod("week");
        final TimestampDriver.TimestampFloorWithStrideMethod floorWithStrideMethod = timestampDriver.getTimestampFloorWithStrideMethod("week");
        final TimestampDriver.TimestampFloorWithOffsetMethod floorWithOffsetMethod = timestampDriver.getTimestampFloorWithOffsetMethod('w');
        for (int i = 0; i < src.length; i++) {
            final long ts = timestampDriver.parseFloorLiteral(src[i]);
            final long roundedTs = sampler.round(ts);
            Assert.assertEquals(floorMethod.floor(ts), roundedTs);
            Assert.assertEquals(floorWithStrideMethod.floor(ts, 1), roundedTs);
            Assert.assertEquals(floorWithOffsetMethod.floor(ts, 1, 0), roundedTs);
        }
    }

    private void testSampler(int stepWeeks, String expected) throws NumericException {
        testSampler(stepWeeks, expected, ColumnType.TIMESTAMP_MICRO);
        testSampler(stepWeeks, expected, ColumnType.TIMESTAMP_NANO);
    }

    private void testSampler(int stepWeeks, String expected, int timestampType) throws NumericException {
        StringSink sink = new StringSink();
        TimestampSampler sampler = ColumnType.isTimestampMicro(timestampType)
                ? new WeekTimestampMicrosSampler(3)
                : new WeekTimestampNanosSampler(3);
        TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        long timestamp = driver.parseFloorLiteral("2018-11-16T15:31:57.000000Z");
        sampler.setStart(timestamp);
        for (int i = 0; i < 20; i++) {
            long ts = sampler.nextTimestamp(timestamp, stepWeeks);
            sink.putISODate(driver, ts).put('\n');
            if (stepWeeks == 1) {
                Assert.assertEquals(timestamp, sampler.previousTimestamp(ts));
            }
            timestamp = ts;
        }
        TestUtils.assertEquals(AbstractCairoTest.replaceTimestampSuffix(expected, ColumnType.nameOf(timestampType)), sink);
    }
}

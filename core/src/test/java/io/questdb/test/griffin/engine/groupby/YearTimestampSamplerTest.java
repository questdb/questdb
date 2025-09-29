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
import io.questdb.griffin.engine.groupby.YearTimestampMicrosSampler;
import io.questdb.griffin.engine.groupby.YearTimestampNanosSampler;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.datetime.nanotime.Nanos;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class YearTimestampSamplerTest {
    private static final String FIXED_PART = "-11-16T15:00:00.000000Z\n";

    @Test
    public void testRound() throws NumericException {
        testRound(1, "2023-01-01T00:00:00.000000Z", "2023-01-01T00:00:00.000000Z");
        testRound(1, "2023-01-01T00:00:00.000001Z", "2023-01-01T00:00:00.000000Z");
        testRound(1, "2024-08-08T12:57:07.388314Z", "2024-01-01T00:00:00.000000Z");

        testRound(2, "2024-01-01T00:00:00.000000Z", "2024-01-01T00:00:00.000000Z");
        testRound(2, "2025-08-08T12:57:07.388314Z", "2024-01-01T00:00:00.000000Z");
        testRound(2, "2025-12-31T23:59:59.999999Z", "2024-01-01T00:00:00.000000Z");

        testRound(10, "2020-01-01T00:00:00.000000Z", "2020-01-01T00:00:00.000000Z");
        testRound(10, "2024-01-01T00:00:00.000000Z", "2020-01-01T00:00:00.000000Z");
        testRound(10, "2025-12-31T23:59:59.999999Z", "2020-01-01T00:00:00.000000Z");

        testRound(50, "1970-01-01T00:00:00.000000Z", "1970-01-01T00:00:00.000000Z");
        testRound(50, "2051-01-01T00:00:00.000000Z", "2020-01-01T00:00:00.000000Z");
        testRound(100, "2024-01-01T00:00:00.000000Z", "1970-01-01T00:00:00.000000Z");
        testRound(1000, "2025-12-31T23:59:59.999999Z", "1970-01-01T00:00:00.000000Z");
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
                "2022" + FIXED_PART +
                        "2026" + FIXED_PART +
                        "2030" + FIXED_PART +
                        "2034" + FIXED_PART +
                        "2038" + FIXED_PART +
                        "2042" + FIXED_PART +
                        "2046" + FIXED_PART +
                        "2050" + FIXED_PART +
                        "2054" + FIXED_PART +
                        "2058" + FIXED_PART +
                        "2062" + FIXED_PART +
                        "2066" + FIXED_PART +
                        "2070" + FIXED_PART +
                        "2074" + FIXED_PART +
                        "2078" + FIXED_PART +
                        "2082" + FIXED_PART +
                        "2086" + FIXED_PART +
                        "2090" + FIXED_PART +
                        "2094" + FIXED_PART +
                        "2098" + FIXED_PART
        );
    }

    @Test
    public void testTripleStep() throws NumericException {
        testSampler(
                3,
                "2030" + FIXED_PART +
                        "2042" + FIXED_PART +
                        "2054" + FIXED_PART +
                        "2066" + FIXED_PART +
                        "2078" + FIXED_PART +
                        "2090" + FIXED_PART +
                        "2102" + FIXED_PART +
                        "2114" + FIXED_PART +
                        "2126" + FIXED_PART +
                        "2138" + FIXED_PART +
                        "2150" + FIXED_PART +
                        "2162" + FIXED_PART +
                        "2174" + FIXED_PART +
                        "2186" + FIXED_PART +
                        "2198" + FIXED_PART +
                        "2210" + FIXED_PART +
                        "2222" + FIXED_PART +
                        "2234" + FIXED_PART +
                        "2246" + FIXED_PART +
                        "2258" + FIXED_PART
        );
    }

    private void testRound(int stepYears, String timestamp, String expectedRounded) throws NumericException {
        final YearTimestampMicrosSampler samplerUs = new YearTimestampMicrosSampler(stepYears);
        samplerUs.setStart(0);
        final long tsUs = MicrosFormatUtils.parseUTCTimestamp(timestamp);
        final long expectedUs = MicrosFormatUtils.parseUTCTimestamp(expectedRounded);
        Assert.assertEquals(expectedUs, samplerUs.round(tsUs));

        final YearTimestampNanosSampler samplerNs = new YearTimestampNanosSampler(stepYears);
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

        final TimestampSampler sampler = timestampDriver.getTimestampSampler(1, 'y', 0);
        sampler.setStart(0);

        final TimestampDriver.TimestampFloorMethod floorMethod = timestampDriver.getTimestampFloorMethod("year");
        final TimestampDriver.TimestampFloorWithStrideMethod floorWithStrideMethod = timestampDriver.getTimestampFloorWithStrideMethod("year");
        final TimestampDriver.TimestampFloorWithOffsetMethod floorWithOffsetMethod = timestampDriver.getTimestampFloorWithOffsetMethod('y');
        for (int i = 0; i < src.length; i++) {
            final long ts = timestampDriver.parseFloorLiteral(src[i]);
            final long roundedTs = sampler.round(ts);
            Assert.assertEquals(floorMethod.floor(ts), roundedTs);
            Assert.assertEquals(floorWithStrideMethod.floor(ts, 1), roundedTs);
            Assert.assertEquals(floorWithOffsetMethod.floor(ts, 1, 0), roundedTs);
        }
    }

    private void testSampler(int stepYears, String expected) throws NumericException {
        testSampler(stepYears, expected, ColumnType.TIMESTAMP_MICRO);
        testSampler(stepYears, expected, ColumnType.TIMESTAMP_NANO);
    }

    private void testSampler(int stepYears, String expected, int timestampType) throws NumericException {
        StringSink sink = new StringSink();
        TimestampSampler sampler = ColumnType.isTimestampMicro(timestampType)
                ? new YearTimestampMicrosSampler(4)
                : new YearTimestampNanosSampler(4);
        TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        long timestamp = driver.parseFloorLiteral("2018-11-16T15:00:00.000000Z");
        sampler.setStart(timestamp);
        for (int i = 0; i < 20; i++) {
            long ts = sampler.nextTimestamp(timestamp, stepYears);
            sink.putISODate(driver, ts).put('\n');
            if (stepYears == 1) {
                Assert.assertEquals(timestamp, sampler.previousTimestamp(ts));
            }
            timestamp = ts;
        }
        TestUtils.assertEquals(AbstractCairoTest.replaceTimestampSuffix(expected, ColumnType.nameOf(timestampType)), sink);
    }
}

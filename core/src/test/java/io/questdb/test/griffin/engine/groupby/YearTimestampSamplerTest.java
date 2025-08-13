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

import io.questdb.griffin.engine.groupby.YearTimestampSampler;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.StringSink;
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
        final YearTimestampSampler sampler = new YearTimestampSampler(stepYears);
        sampler.setStart(0);
        final long ts = TimestampFormatUtils.parseUTCTimestamp(timestamp);
        Assert.assertEquals(TimestampFormatUtils.parseUTCTimestamp(expectedRounded), sampler.round(ts));
    }

    private void testSampler(int stepSize, String expected) throws NumericException {
        StringSink sink = new StringSink();
        YearTimestampSampler sampler = new YearTimestampSampler(4);
        long timestamp = TimestampFormatUtils.parseUTCTimestamp("2018-11-16T15:00:00.000000Z");
        sampler.setStart(timestamp);
        for (int i = 0; i < 20; i++) {
            long ts = sampler.nextTimestamp(timestamp, stepSize);
            sink.putISODate(ts).put('\n');
            if (stepSize == 1) {
                Assert.assertEquals(timestamp, sampler.previousTimestamp(ts));
            }
            timestamp = ts;
        }
        TestUtils.assertEquals(expected, sink);
    }
}

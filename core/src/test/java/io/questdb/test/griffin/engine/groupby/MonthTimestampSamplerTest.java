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

import io.questdb.griffin.engine.groupby.MonthTimestampSampler;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class MonthTimestampSamplerTest {

    @Test
    public void testNextTimestamp() throws NumericException {
        MonthTimestampSampler sampler = new MonthTimestampSampler(1);

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
            long ts = TimestampFormatUtils.parseUTCTimestamp(src[i]);
            long nextTs = sampler.nextTimestamp(ts);
            Assert.assertEquals(TimestampFormatUtils.parseUTCTimestamp(next[i]), nextTs);
        }
    }

    @Test
    public void testPreviousTimestamp() throws NumericException {
        MonthTimestampSampler sampler = new MonthTimestampSampler(1);

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
            long ts = TimestampFormatUtils.parseUTCTimestamp(src[i]);
            long prevTs = sampler.previousTimestamp(ts);
            Assert.assertEquals(TimestampFormatUtils.parseUTCTimestamp(prev[i]), prevTs);
        }
    }

    @Test
    public void testRound() throws NumericException {
        MonthTimestampSampler sampler = new MonthTimestampSampler(1);

        final String[] src = new String[]{
                "2013-12-31T00:00:00.000000Z",
                "2014-01-01T00:00:00.000000Z",
                "2014-02-12T12:12:12.123456Z",
        };
        final String[] rounded = new String[]{
                "2013-12-01T00:00:00.000000Z",
                "2014-01-01T00:00:00.000000Z",
                "2014-02-01T00:00:00.000000Z",
        };
        Assert.assertEquals(src.length, rounded.length);

        for (int i = 0; i < src.length; i++) {
            long ts = TimestampFormatUtils.parseUTCTimestamp(src[i]);
            long roundedTs = sampler.round(ts);
            Assert.assertEquals(TimestampFormatUtils.parseUTCTimestamp(rounded[i]), roundedTs);
        }
    }

    @Test
    public void testSimple() throws NumericException {
        StringSink sink = new StringSink();
        MonthTimestampSampler sampler = new MonthTimestampSampler(6);

        long timestamp = TimestampFormatUtils.parseUTCTimestamp("2018-11-16T15:00:00.000000Z");
        sampler.setStart(timestamp);

        for (int i = 0; i < 20; i++) {
            long ts = sampler.nextTimestamp(timestamp);
            sink.putISODate(ts).put('\n');
            Assert.assertEquals(timestamp, sampler.previousTimestamp(ts));
            timestamp = ts;
        }

        TestUtils.assertEquals(
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
                sink
        );
    }
}

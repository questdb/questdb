/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.groupby;

import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class YearTimestampSamplerTest {

    @Test
    public void testSimple() throws NumericException {
        StringSink sink = new StringSink();
        YearTimestampSampler sampler = new YearTimestampSampler(4);

        long timestamp = TimestampFormatUtils.parseUTCTimestamp("2018-11-16T15:00:00.000000Z");

        sampler.setStart(timestamp);

        for (int i = 0; i < 20; i++) {
            long ts = sampler.nextTimestamp(timestamp);
            sink.putISODate(ts).put('\n');
            Assert.assertEquals(timestamp, sampler.previousTimestamp(ts));
            timestamp = ts;
        }

        TestUtils.assertEquals(
                "2022-11-16T15:00:00.000000Z\n" +
                        "2026-11-16T15:00:00.000000Z\n" +
                        "2030-11-16T15:00:00.000000Z\n" +
                        "2034-11-16T15:00:00.000000Z\n" +
                        "2038-11-16T15:00:00.000000Z\n" +
                        "2042-11-16T15:00:00.000000Z\n" +
                        "2046-11-16T15:00:00.000000Z\n" +
                        "2050-11-16T15:00:00.000000Z\n" +
                        "2054-11-16T15:00:00.000000Z\n" +
                        "2058-11-16T15:00:00.000000Z\n" +
                        "2062-11-16T15:00:00.000000Z\n" +
                        "2066-11-16T15:00:00.000000Z\n" +
                        "2070-11-16T15:00:00.000000Z\n" +
                        "2074-11-16T15:00:00.000000Z\n" +
                        "2078-11-16T15:00:00.000000Z\n" +
                        "2082-11-16T15:00:00.000000Z\n" +
                        "2086-11-16T15:00:00.000000Z\n" +
                        "2090-11-16T15:00:00.000000Z\n" +
                        "2094-11-16T15:00:00.000000Z\n" +
                        "2098-11-16T15:00:00.000000Z\n",
                sink
        );
    }
}
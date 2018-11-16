/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.groupby;

import com.questdb.std.NumericException;
import com.questdb.std.microtime.DateFormatUtils;
import com.questdb.std.str.StringSink;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class YearTimestampSamplerTest {

    @Test
    public void testSimple() throws NumericException {
        StringSink sink = new StringSink();
        YearTimestampSampler sampler = new YearTimestampSampler(4);

        long timestamp = DateFormatUtils.parseTimestamp("2018-11-16T15:00:00.000000Z");

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
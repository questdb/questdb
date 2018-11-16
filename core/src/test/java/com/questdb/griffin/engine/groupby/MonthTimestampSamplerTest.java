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

public class MonthTimestampSamplerTest {
    @Test
    public void testSimple() throws NumericException {
        StringSink sink = new StringSink();
        MonthTimestampSampler sampler = new MonthTimestampSampler(6);

        long timestamp = DateFormatUtils.parseTimestamp("2018-11-16T15:00:00.000000Z");

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
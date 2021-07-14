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

import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TzSamplerTest {
    @BeforeClass
    public static void setup() {
        Timestamps.toString(0);
    }

    @Test
    public void testSimple() throws NumericException, SqlException {
        String[][] testCases = new String[][] {
                new String[] {"2021-03-28 00:01", "2021-03-28T00:00:00.000Z"},
                new String[] {"2021-03-28 00:59", "2021-03-28T00:00:00.000Z"},
                new String[] {"2021-03-28 01:00", "2021-03-28T01:00:00.000Z"},
                new String[] {"2021-03-28 01:01", "2021-03-28T01:00:00.000Z"},
                new String[] {"2021-03-28 01:59", "2021-03-28T01:00:00.000Z"},
                new String[] {"2021-03-28 02:00", "2021-03-28T03:00:00.000Z"}, // 2AM is time when clock moves forward
                new String[] {"2021-03-28 02:01", "2021-03-28T03:00:00.000Z"}, // 2AM is time when clock moves forward
                new String[] {"2021-03-28 02:59", "2021-03-28T03:00:00.000Z"}, // 2AM is time when clock moves forward
                new String[] {"2021-03-28 03:01", "2021-03-28T03:00:00.000Z"},
                new String[] {"2021-03-28 04:01", "2021-03-28T04:00:00.000Z"}
        };
        for(int i = 0; i < testCases.length; i++) {
            assertStartFrom("Europe/Berlin", testCases[i][0], testCases[i][1], "00:00", Timestamps.HOUR_MICROS);
        }
    }

    @Test
    public void testResumesCorrectlyAfterPassingDST() throws NumericException, SqlException {
        final String timezone = "Europe/Kiev";
        TzSampler sampler = createSampler(timezone, "00:00", Timestamps.HOUR_MICROS);
        assertStartFrom(timezone, "2021-03-28 00:01", "2021-03-28T00:00:00.000Z", sampler);
        TestUtils.assertEquals("2021-03-28T01:00:00.000Z", getNext(timezone, sampler));
        TestUtils.assertEquals("2021-03-28T02:00:00.000Z", getNext(timezone, sampler));
        TestUtils.assertEquals("2021-03-28T04:00:00.000Z", getNext(timezone, sampler));
        TestUtils.assertEquals("2021-03-28T05:00:00.000Z", getNext(timezone, sampler));
        TestUtils.assertEquals("2021-03-28T06:00:00.000Z", getNext(timezone, sampler));


        assertStartFrom(timezone, "2021-03-28 00:01", "2021-03-28T00:00:00.000Z", sampler);
        TestUtils.assertEquals("2021-03-28T01:00:00.000Z", getNext(timezone, sampler));
        TestUtils.assertEquals("2021-03-28T02:00:00.000Z", getNext(timezone, sampler));
        TestUtils.assertEquals("2021-03-28T04:00:00.000Z", getNext(timezone, sampler));
        TestUtils.assertEquals("2021-03-28T05:00:00.000Z", getNext(timezone, sampler));
        TestUtils.assertEquals("2021-03-28T06:00:00.000Z", getNext(timezone, sampler));
    }

    @Test
    public void testOnTimeMovesForward() throws NumericException, SqlException {
        final String timezone = "Europe/Kiev";
        TzSampler sampler = createSampler(timezone, "00:00", Timestamps.HOUR_MICROS);
        assertStartFrom(timezone, "2021-10-31 00:01", "2021-10-31T00:00:00.000Z", sampler);
        TestUtils.assertEquals("2021-10-31T01:00:00.000Z", getNext(timezone, sampler));
        TestUtils.assertEquals("2021-10-31T02:00:00.000Z", getNext(timezone, sampler));
        TestUtils.assertEquals("2021-10-31T03:00:00.000Z", getNext(timezone, sampler));
        TestUtils.assertEquals("2021-10-31T04:00:00.000Z", getNext(timezone, sampler));
        TestUtils.assertEquals("2021-10-31T05:00:00.000Z", getNext(timezone, sampler));
    }

    private String getNext(String timezone, TzSampler sampler) throws NumericException {
        long nextTimestamp = sampler.getNextTimestamp();
        return Timestamps.toString(
                Timestamps.toTimezone(nextTimestamp, TimestampFormatUtils.enLocale, timezone));
    }

    private void assertStartFrom(String tz, String startFromLoc, String startFromRoundedLocExpected, String offset, long sampleByMicro) throws NumericException, SqlException {
        TzSampler sampler = createSampler(tz, offset, sampleByMicro);
        assertStartFrom(tz, startFromLoc, startFromRoundedLocExpected, sampler);
    }

    private void assertStartFrom(String tz, String tsLoc, String tsLocExpected, TzSampler sampler) throws NumericException {
        long timestampLocal = IntervalUtils.parseFloorPartialDate(tsLoc);
        long utcTimezone = Timestamps.toUTC(timestampLocal, TimestampFormatUtils.enLocale, tz);
        long startFromUtc = sampler.startFrom(utcTimezone);
        long startFromRoundedLoc = Timestamps.toTimezone(startFromUtc, TimestampFormatUtils.enLocale, tz);

        String actual = Timestamps.toString(startFromRoundedLoc);
        Assert.assertEquals(tsLocExpected, actual);
    }

    private TzSampler createSampler(String timezone, String offset, long sampleByMicro) throws SqlException {
        TimestampSampler sampler = new MicroTimestampSampler(sampleByMicro);
        TzSampler res = new TzSampler(sampler, StrConstant.newInstance(timezone), StrConstant.newInstance(offset), 13, 17);
        res.init(null, null);
        return res;
    }
}

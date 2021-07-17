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

package io.questdb.std.datetime.microtime;

import io.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

public class OffsetParserTest {

    private static void assertError(String offset) {
        Assert.assertEquals(Long.MIN_VALUE, Timestamps.parseOffset(offset));
    }

    private static void assertThat(int expected, String offset) {
        long r = Timestamps.parseOffset(offset);
        Assert.assertEquals(expected, Numbers.decodeLowInt(r));
    }

    @Test
    public void testBadDelim() {
        assertError("UTC+01x30");
    }

    @Test
    public void testBadGMT() {
        assertError("GMX+03:30");
    }

    @Test
    public void testBadHour() {
        assertError("UTC+0x:30");
    }

    @Test
    public void testBadMinute() {
        assertError("UTC+01:3x");
    }

    @Test
    public void testBadSign() {
        assertError("GMT*08:00");
    }

    @Test
    public void testBadStart() {
        assertError("*08");
    }

    @Test
    public void testBadUtc() {
        assertError("UTX+09:00");
    }

    @Test
    public void testGMTCamelCasePositive() {
        assertThat(2 * 60 + 15, "gMt+02:15");
    }

    @Test
    public void testGMTNegative() {
        assertThat(-2 * 60 + -15, "gMt-02:15");
    }

    @Test
    public void testGMTPositive() {
        assertThat(3 * 60 + 30, "GMT+03:30");
    }

    @Test
    public void testHourMinNoDelim() {
        assertThat(8 * 60 + 15, "0815");
    }

    @Test
    public void testHourMinNoDelimNegative() {
        assertThat(-3 * 60 - 30, "-0330");
    }

    @Test
    public void testHourMinNoDelimPositive() {
        assertThat(8 * 60 + 15, "+0815");
    }

    @Test
    public void testHoursOnly() {
        assertThat(8 * 60, "08");
    }

    @Test
    public void testLargeHour() {
        assertError("UTC+24:00");
    }

    @Test
    public void testLargeMinute() {
        assertError("UTC+12:60");
    }

    @Test
    public void testMissingHour() {
        assertError("UTC+");
    }

    @Test
    public void testMissingMinute() {
        assertError("UTC+01:");
    }

    @Test
    public void testNegativeHoursOnly() {
        assertThat(-4 * 60, "-04");
    }

    @Test
    public void testPositiveHoursOnly() {
        assertThat(8 * 60, "+08");
    }

    @Test
    public void testShortHour() {
        assertError("UTC+1");
    }

    @Test
    public void testShortMinute() {
        assertError("UTC+01:3");
    }

    @Test
    public void testUTCCamelCasePositive() {
        assertThat(9 * 60, "uTc+09:00");
    }

    @Test
    public void testUTCNegative() {
        assertThat(-4 * 60 - 15, "UTC-04:15");
    }

    @Test
    public void testUTCPositive() {
        assertThat(9 * 60, "UTC+09:00");
    }
}
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

package io.questdb.griffin.model;

import io.questdb.griffin.SqlException;
import io.questdb.std.LongList;
import io.questdb.std.NumericException;
import io.questdb.std.microtime.TimestampFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.griffin.GriffinParserTestUtils.intervalToString;

public class IntrinsicModelTest {

    private static final StringSink sink = new StringSink();
    private final LongList a = new LongList();
    private final LongList b = new LongList();
    private final LongList out = new LongList();

    @Before
    public void setUp() {
        a.clear();
        b.clear();
        out.clear();
    }

    @Test
    public void testDateCeilYYYY() throws NumericException {
        assertDateCeil("2016-01-01T00:00:00.000000Z", "2015");
    }

    @Test
    public void testDateCeilYYYYMM() throws NumericException {
        assertDateCeil("2015-03-01T00:00:00.000000Z", "2015-02");
    }

    @Test
    public void testDateCeilYYYYMMDD() throws NumericException {
        assertDateCeil("2016-02-29T00:00:00.000000Z", "2016-02-28");
    }

    @Test
    public void testDateCeilYYYYMMDDH() throws NumericException {
        assertDateCeil("2015-02-28T08:00:00.000000Z", "2015-02-28T07");
    }

    @Test
    public void testDateCeilYYYYMMDDHm() throws NumericException {
        assertDateCeil("2015-02-28T07:22:00.000000Z", "2015-02-28T07:21");
    }

    @Test
    public void testDateCeilYYYYMMDDHms() throws NumericException {
        assertDateCeil("2015-02-28T07:21:45.000000Z", "2015-02-28T07:21:44");
    }

    @Test
    public void testDateCeilYYYYMMDDHmsS() throws NumericException {
        assertDateCeil("2015-02-28T07:21:44.557000Z", "2015-02-28T07:21:44.556");
    }

    @Test
    public void testDateCeilYYYYMMDDHmsSU() throws NumericException {
        assertDateCeil("2015-02-28T07:21:44.556012Z", "2015-02-28T07:21:44.556011");
    }

    //////////////////////////////

    @Test
    public void testDateCeilYYYYMMDDNonLeap() throws NumericException {
        assertDateCeil("2017-03-01T00:00:00.000000Z", "2017-02-28");
    }

    @Test
    public void testDateCeilYYYYMMOverflow() throws NumericException {
        assertDateCeil("2016-01-01T00:00:00.000000Z", "2015-12");
    }

    @Test
    public void testDateFloorYYYY() throws NumericException {
        assertDateFloor("2015-01-01T00:00:00.000000Z", "2015");
    }

    @Test
    public void testDateFloorYYYYMM() throws NumericException {
        assertDateFloor("2015-02-01T00:00:00.000000Z", "2015-02");
    }

    @Test
    public void testDateFloorYYYYMMDD() throws NumericException {
        assertDateFloor("2015-02-28T00:00:00.000000Z", "2015-02-28");
    }

    @Test
    public void testDateFloorYYYYMMDDH() throws NumericException {
        assertDateFloor("2015-02-28T07:00:00.000000Z", "2015-02-28T07");
    }

    @Test
    public void testDateFloorYYYYMMDDHm() throws NumericException {
        assertDateFloor("2015-02-28T07:21:00.000000Z", "2015-02-28T07:21");
    }

    @Test
    public void testDateFloorYYYYMMDDHms() throws NumericException {
        assertDateFloor("2015-02-28T07:21:44.000000Z", "2015-02-28T07:21:44");
    }

    @Test
    public void testDateFloorYYYYMMDDHmsS() throws NumericException {
        assertDateFloor("2015-02-28T07:21:44.556000Z", "2015-02-28T07:21:44.556");
    }

    @Test
    public void testDateFloorYYYYMMDDHmsSU() throws NumericException {
        assertDateFloor("2015-02-28T07:21:44.556011Z", "2015-02-28T07:21:44.556011");
    }

    @Test
    public void testIntersectContain2() throws Exception {
        a.add(TimestampFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"));
        a.add(TimestampFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z"));

        b.add(TimestampFormatUtils.parseDateTime("2016-03-10T09:00:00.000Z"));
        b.add(TimestampFormatUtils.parseDateTime("2016-03-10T13:30:00.000Z"));

        assertIntersect("[{lo=2016-03-10T10:00:00.000000Z, hi=2016-03-10T12:00:00.000000Z}]");
    }

    @Test
    public void testIntersectMergeOverlap() throws Exception {
        a.add(TimestampFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"));
        a.add(TimestampFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z"));

        b.add(TimestampFormatUtils.parseDateTime("2016-03-10T11:00:00.000Z"));
        b.add(TimestampFormatUtils.parseDateTime("2016-03-10T14:00:00.000Z"));

        assertIntersect("[{lo=2016-03-10T11:00:00.000000Z, hi=2016-03-10T12:00:00.000000Z}]");
    }

    @Test
    public void testIntersectMergeOverlap2() throws Exception {
        a.add(TimestampFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"));
        a.add(TimestampFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z"));

        b.add(TimestampFormatUtils.parseDateTime("2016-03-10T11:00:00.000Z"));
        b.add(TimestampFormatUtils.parseDateTime("2016-03-10T14:00:00.000Z"));

        assertIntersect("[{lo=2016-03-10T11:00:00.000000Z, hi=2016-03-10T12:00:00.000000Z}]");
    }

    @Test
    public void testIntersectNoOverlap() throws Exception {
        a.add(TimestampFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"));
        a.add(TimestampFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z"));
        a.add(TimestampFormatUtils.parseDateTime("2016-03-10T14:00:00.000Z"));
        a.add(TimestampFormatUtils.parseDateTime("2016-03-10T16:00:00.000Z"));

        b.add(TimestampFormatUtils.parseDateTime("2016-03-10T13:00:00.000Z"));
        b.add(TimestampFormatUtils.parseDateTime("2016-03-10T13:30:00.000Z"));

        assertIntersect("[]");
    }

    @Test
    public void testIntersectSame() throws Exception {
        a.add(TimestampFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"));
        a.add(TimestampFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z"));

        b.add(TimestampFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"));
        b.add(TimestampFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z"));

        assertIntersect("[{lo=2016-03-10T10:00:00.000000Z, hi=2016-03-10T12:00:00.000000Z}]");
    }

    @Test
    public void testIntersectTwoOverlapOne2() throws Exception {
        a.add(TimestampFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"));
        a.add(TimestampFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z"));


        a.add(TimestampFormatUtils.parseDateTime("2016-03-10T14:00:00.000Z"));
        a.add(TimestampFormatUtils.parseDateTime("2016-03-10T16:00:00.000Z"));


        b.add(TimestampFormatUtils.parseDateTime("2016-03-10T11:00:00.000Z"));
        b.add(TimestampFormatUtils.parseDateTime("2016-03-10T15:00:00.000Z"));

        assertIntersect("[{lo=2016-03-10T11:00:00.000000Z, hi=2016-03-10T12:00:00.000000Z},{lo=2016-03-10T14:00:00.000000Z, hi=2016-03-10T15:00:00.000000Z}]");
    }

    @Test
    public void testInvert() throws SqlException {
        final String intervalStr = "2018-01-10T10:30:00.000Z;30m;2d;2";
        LongList out = new LongList();
        IntrinsicModel.parseIntervalEx(intervalStr, 0, intervalStr.length(), 0, out);
        IntrinsicModel.invert(out);
        TestUtils.assertEquals("[{lo=, hi=2018-01-10T10:29:59.999999Z},{lo=2018-01-10T11:00:00.000001Z, hi=2018-01-12T10:29:59.999999Z},{lo=2018-01-12T11:00:00.000001Z, hi=294247-01-10T04:00:54.775807Z}]",
                intervalToString(out));
    }

    @Test
    public void testParseLongInterval22() throws Exception {
        assertShortInterval("[{lo=2015-03-12T10:00:00.000000Z, hi=2015-03-12T10:05:00.999999Z},{lo=2015-03-12T10:30:00.000000Z, hi=2015-03-12T10:35:00.999999Z},{lo=2015-03-12T11:00:00.000000Z, hi=2015-03-12T11:05:00.999999Z},{lo=2015-03-12T11:30:00.000000Z, hi=2015-03-12T11:35:00.999999Z},{lo=2015-03-12T12:00:00.000000Z, hi=2015-03-12T12:05:00.999999Z},{lo=2015-03-12T12:30:00.000000Z, hi=2015-03-12T12:35:00.999999Z},{lo=2015-03-12T13:00:00.000000Z, hi=2015-03-12T13:05:00.999999Z},{lo=2015-03-12T13:30:00.000000Z, hi=2015-03-12T13:35:00.999999Z},{lo=2015-03-12T14:00:00.000000Z, hi=2015-03-12T14:05:00.999999Z},{lo=2015-03-12T14:30:00.000000Z, hi=2015-03-12T14:35:00.999999Z}]",
                "2015-03-12T10:00:00;5m;30m;10");
    }

    @Test
    public void testParseLongInterval32() throws Exception {
        assertShortInterval("[{lo=2016-03-21T00:00:00.000000Z, hi=2021-03-21T23:59:59.999999Z}]", "2016-03-21;3y;6M;5");
    }

    @Test
    public void testParseShortDayErr() {
        assertIntervalError("2016-02-30");
    }

    @Test
    public void testParseShortDayErr2() {
        assertIntervalError("2016-02-3");
    }

    @Test
    public void testParseShortHourErr1() {
        assertIntervalError("2016-02-15T1");
    }

    @Test
    public void testParseShortHourErr2() {
        assertIntervalError("2016-02-15T31");
    }

    @Test
    public void testParseShortHourErr3() {
        assertIntervalError("2016-02-15X1");
    }

    @Test
    public void testParseShortInterval1() throws Exception {
        assertShortInterval("[{lo=2016-01-01T00:00:00.000000Z, hi=2016-12-31T23:59:59.999999Z}]", "2016");
    }

    @Test
    public void testParseShortInterval2() throws Exception {
        assertShortInterval("[{lo=2016-03-01T00:00:00.000000Z, hi=2016-03-31T23:59:59.999999Z}]", "2016-03");
    }

    @Test
    public void testParseShortInterval3() throws Exception {
        assertShortInterval("[{lo=2016-03-21T00:00:00.000000Z, hi=2016-03-21T23:59:59.999999Z}]", "2016-03-21");
    }

    @Test
    public void testParseShortInterval32() throws Exception {
        assertShortInterval("[{lo=2016-03-21T00:00:00.000000Z, hi=2016-03-21T23:59:59.999999Z}]", "2016-03-21");
    }

    @Test
    public void testParseShortInterval4() throws Exception {
        assertShortInterval("[{lo=2016-03-21T10:00:00.000000Z, hi=2016-03-21T10:59:59.999999Z}]", "2016-03-21T10");
    }

    @Test
    public void testParseShortInterval5() throws Exception {
        assertShortInterval("[{lo=2016-03-21T10:30:00.000000Z, hi=2016-03-21T10:30:59.999999Z}]", "2016-03-21T10:30");
    }

    @Test
    public void testParseShortInterval6() throws Exception {
        assertShortInterval("[{lo=2016-03-21T10:30:40.000000Z, hi=2016-03-21T10:30:40.999999Z}]", "2016-03-21T10:30:40");
    }

    @Test
    public void testParseShortInterval7() throws Exception {
        assertShortInterval("[{lo=2016-03-21T10:30:40.100000Z, hi=2016-03-21T10:30:40.100000Z}]", "2016-03-21T10:30:40.100Z");
    }

    @Test
    public void testParseShortMilliErr() {
        assertIntervalError("2016-03-21T10:31:61.23");
    }

    @Test
    public void testParseShortMinErr() {
        assertIntervalError("2016-03-21T10:3");
    }

    @Test
    public void testParseShortMinErr2() {
        assertIntervalError("2016-03-21T10:69");
    }

    @Test
    public void testParseShortMonthErr() {
        assertIntervalError("2016-1");
    }

    @Test
    public void testParseShortMonthErr2() {
        assertIntervalError("2016x11");
    }

    @Test
    public void testParseShortMonthRange() {
        assertIntervalError("2016-66");
    }

    @Test
    public void testParseShortSecErr() {
        assertIntervalError("2016-03-21T10:31:61");
    }

    @Test
    public void testParseShortSecErr1() {
        assertIntervalError("2016-03-21T10:31:1");
    }

    @Test
    public void testParseShortYearErr() {
        assertIntervalError("201");
    }

    private static void assertShortInterval(String expected, String interval) throws SqlException {
        LongList out = new LongList();
        IntrinsicModel.parseIntervalEx(interval, 0, interval.length(), 0, out);
        TestUtils.assertEquals(expected, intervalToString(out));
    }

    private void assertDateCeil(String expected, String value) throws NumericException {
        sink.clear();
        long t = IntrinsicModel.parseCCPartialDate(value);
        TimestampFormatUtils.appendDateTimeUSec(sink, t);
        TestUtils.assertEquals(expected, sink);
    }

    private void assertDateFloor(String expected, String value) throws NumericException {
        sink.clear();
        long t = IntrinsicModel.parseFloorPartialDate(value);
        TimestampFormatUtils.appendDateTimeUSec(sink, t);
        TestUtils.assertEquals(expected, sink);
    }

    private void assertIntersect(String expected) {
        IntrinsicModel.intersect(a, b, out);
        TestUtils.assertEquals(expected, intervalToString(out));
    }

    private void assertIntervalError(String interval) {
        try {
            IntrinsicModel.parseIntervalEx(interval, 0, interval.length(), 0, out);
            Assert.fail();
        } catch (SqlException ignore) {
        }
    }
}
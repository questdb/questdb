/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.std.datetime.millitime;

import io.questdb.std.NumericException;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.datetime.millitime.Dates;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DatesTest {

    private final StringSink sink = new StringSink();

    @Before
    public void setUp() {
        sink.clear();
    }

    @Test
    public void testAddDaysPrevEpoch() {
        long millis = DateFormatUtils.parseUTCDate("1888-05-12T23:45:51.045Z");
        DateFormatUtils.appendDateTime(sink, Dates.addDays(millis, 24));
        TestUtils.assertEquals("1888-06-05T23:45:51.045Z", sink);
    }

    @Test
    public void testAddHours() throws NumericException {
        long millis = DateFormatUtils.parseUTCDate("2008-05-12T23:45:51.045Z");
        DateFormatUtils.appendDateTime(sink, Dates.addHours(millis, 5));
        TestUtils.assertEquals("2008-05-13T04:45:51.045Z", sink);
    }

    @Test
    public void testAddMicros() throws NumericException {
        long millis = DateFormatUtils.parseUTCDate("2008-05-12T23:45:51.045Z");
        DateFormatUtils.appendDateTime(sink, Dates.addMicros(millis, 500000));
        TestUtils.assertEquals("2008-05-12T23:45:51.545Z", sink);
    }

    @Test
    public void testAddMillis() throws NumericException {
        long millis = DateFormatUtils.parseUTCDate("2008-05-12T23:45:51.045Z");
        DateFormatUtils.appendDateTime(sink, Dates.addMillis(millis, 500));
        TestUtils.assertEquals("2008-05-12T23:45:51.545Z", sink);
    }

    @Test
    public void testAddMinutes() throws NumericException {
        long millis = DateFormatUtils.parseUTCDate("2008-05-12T23:45:51.045Z");
        DateFormatUtils.appendDateTime(sink, Dates.addMinutes(millis, 15));
        TestUtils.assertEquals("2008-05-13T00:00:51.045Z", sink);
    }

    @Test
    public void testAddMonths() {
        long millis = DateFormatUtils.parseUTCDate("2008-05-12T23:45:51.045Z");
        DateFormatUtils.appendDateTime(sink, Dates.addMonths(millis, -10));
        TestUtils.assertEquals("2007-07-12T23:45:51.045Z", sink);
    }

    @Test
    public void testAddMonthsPrevEpoch() {
        long millis = DateFormatUtils.parseUTCDate("1888-05-12T23:45:51.045Z");
        DateFormatUtils.appendDateTime(sink, Dates.addMonths(millis, -10));
        TestUtils.assertEquals("1887-07-12T23:45:51.045Z", sink);
    }

    @Test
    public void testAddNanos() throws NumericException {
        long millis = DateFormatUtils.parseUTCDate("2008-05-12T23:45:51.045Z");
        DateFormatUtils.appendDateTime(sink, Dates.addNanos(millis, 500000000));
        TestUtils.assertEquals("2008-05-12T23:45:51.545Z", sink);
    }

    @Test
    public void testAddSeconds() throws NumericException {
        long millis = DateFormatUtils.parseUTCDate("2008-05-12T23:45:51.045Z");
        DateFormatUtils.appendDateTime(sink, Dates.addSeconds(millis, 30));
        TestUtils.assertEquals("2008-05-12T23:46:21.045Z", sink);
    }

    @Test
    public void testAddWeeks() throws NumericException {
        long millis = DateFormatUtils.parseUTCDate("2008-05-12T23:45:51.045Z");
        DateFormatUtils.appendDateTime(sink, Dates.addWeeks(millis, 2));
        TestUtils.assertEquals("2008-05-26T23:45:51.045Z", sink);
    }

    @Test
    public void testAddYears() {
        long millis = DateFormatUtils.parseUTCDate("1988-05-12T23:45:51.045Z");
        DateFormatUtils.appendDateTime(sink, Dates.addYears(millis, 10));
        TestUtils.assertEquals("1998-05-12T23:45:51.045Z", sink);
    }

    @Test
    public void testAddYears3() {
        long millis = DateFormatUtils.parseUTCDate("2014-01-01T00:00:00.000Z");
        DateFormatUtils.appendDateTime(sink, Dates.addYears(millis, 1));
        TestUtils.assertEquals("2015-01-01T00:00:00.000Z", sink);
    }

    @Test
    public void testAddYearsNonLeapToLeap() {
        long millis = DateFormatUtils.parseUTCDate("2015-01-01T00:00:00.000Z");
        DateFormatUtils.appendDateTime(sink, Dates.addYears(millis, 1));
        TestUtils.assertEquals("2016-01-01T00:00:00.000Z", sink);
    }

    @Test
    public void testAddYearsPrevEpoch() {
        long millis = DateFormatUtils.parseUTCDate("1888-05-12T23:45:51.045Z");
        DateFormatUtils.appendDateTime(sink, Dates.addYears(millis, 10));
        TestUtils.assertEquals("1898-05-12T23:45:51.045Z", sink);
    }

    @Test
    public void testCeilDD() {
        long millis = DateFormatUtils.parseUTCDate("2008-05-12T23:45:51.045Z");
        DateFormatUtils.appendDateTime(sink, Dates.ceilDD(millis));
        TestUtils.assertEquals("2008-05-12T23:59:59.999Z", sink);
    }

    @Test
    public void testCeilDDPrevEpoch() {
        long millis = DateFormatUtils.parseUTCDate("1888-05-12T23:45:51.045Z");
        DateFormatUtils.appendDateTime(sink, Dates.ceilDD(millis));
        TestUtils.assertEquals("1888-05-12T23:59:59.999Z", sink);
    }

    @Test
    public void testCeilMM() {
        long millis = DateFormatUtils.parseUTCDate("2008-05-12T23:45:51.045Z");
        DateFormatUtils.appendDateTime(sink, Dates.ceilMM(millis));
        TestUtils.assertEquals("2008-05-31T23:59:59.999Z", sink);
    }

    @Test
    public void testCeilYYYY() {
        long millis = DateFormatUtils.parseUTCDate("2008-05-12T23:45:51.045Z");
        DateFormatUtils.appendDateTime(sink, Dates.ceilYYYY(millis));
        TestUtils.assertEquals("2008-12-31T23:59:59.999Z", sink);
    }

    @Test
    public void testDayOfWeek() {
        long millis = DateFormatUtils.parseUTCDate("1893-03-19T17:16:30.192Z");
        Assert.assertEquals(7, Dates.getDayOfWeek(millis));
        Assert.assertEquals(1, Dates.getDayOfWeekSundayFirst(millis));
        millis = DateFormatUtils.parseUTCDate("2017-04-09T17:16:30.192Z");
        Assert.assertEquals(7, Dates.getDayOfWeek(millis));
        Assert.assertEquals(1, Dates.getDayOfWeekSundayFirst(millis));
    }

    @Test
    public void testDayOfYear() {
        long millis = DateFormatUtils.parseUTCDate("2020-01-01T17:16:30.192Z");
        Assert.assertEquals(1, Dates.getDayOfYear(millis));
        millis = DateFormatUtils.parseUTCDate("2019-03-10T07:16:30.192Z");
        Assert.assertEquals(69, Dates.getDayOfYear(millis));
        millis = DateFormatUtils.parseUTCDate("2020-03-10T07:16:30.192Z");
        Assert.assertEquals(70, Dates.getDayOfYear(millis));
        millis = DateFormatUtils.parseUTCDate("1893-03-19T17:16:30.192Z");
        Assert.assertEquals(78, Dates.getDayOfYear(millis));
        millis = DateFormatUtils.parseUTCDate("2020-12-31T12:00:00.000Z");
        Assert.assertEquals(366, Dates.getDayOfYear(millis));
        millis = DateFormatUtils.parseUTCDate("2021-12-31T12:00:00.000Z");
        Assert.assertEquals(365, Dates.getDayOfYear(millis));

        millis = DateFormatUtils.parseUTCDate("-2021-12-31T12:00:00.000Z");
        Assert.assertEquals(365, Dates.getDayOfYear(millis));
        millis = DateFormatUtils.parseUTCDate("-2020-12-31T12:00:00.000Z");
        Assert.assertEquals(366, Dates.getDayOfYear(millis));
    }

    @Test
    public void testDaysBetween() {
        Assert.assertEquals(41168,
                Dates.getDaysBetween(
                        DateFormatUtils.parseUTCDate("1904-11-05T23:45:41.045Z"),
                        DateFormatUtils.parseUTCDate("2017-07-24T23:45:31.045Z")
                )
        );
        Assert.assertEquals(41169,
                Dates.getDaysBetween(
                        DateFormatUtils.parseUTCDate("1904-11-05T23:45:41.045Z"),
                        DateFormatUtils.parseUTCDate("2017-07-24T23:45:51.045Z")
                )
        );
    }

    @Test
    public void testFloorDD() {
        long millis = DateFormatUtils.parseUTCDate("2008-05-12T23:45:51.045Z");
        DateFormatUtils.appendDateTime(sink, Dates.floorDD(millis));
        TestUtils.assertEquals("2008-05-12T00:00:00.000Z", sink);
    }

    @Test
    public void testFloorHH() {
        long millis = DateFormatUtils.parseUTCDate("2008-05-12T23:45:51.045Z");
        DateFormatUtils.appendDateTime(sink, Dates.floorHH(millis));
        TestUtils.assertEquals("2008-05-12T23:00:00.000Z", sink);
    }

    @Test
    public void testFloorMM() {
        long millis = DateFormatUtils.parseUTCDate("2008-05-12T23:45:51.045Z");
        DateFormatUtils.appendDateTime(sink, Dates.floorMM(millis));
        TestUtils.assertEquals("2008-05-01T00:00:00.000Z", sink);
    }

    @Test
    public void testFloorYYYY() {
        long millis = DateFormatUtils.parseUTCDate("2008-05-12T23:45:51.045Z");
        DateFormatUtils.appendDateTime(sink, Dates.floorYYYY(millis));
        TestUtils.assertEquals("2008-01-01T00:00:00.000Z", sink);
    }

    @Test
    public void testFormatCalDate1() {
        DateFormatUtils.formatDashYYYYMMDD(sink, DateFormatUtils.parseUTCDate("2008-05-10T12:31:02.008Z"));
        TestUtils.assertEquals("2008-05-10", sink);
    }

    @Test
    public void testFormatCalDate2() {
        DateFormatUtils.formatYYYYMM(sink, DateFormatUtils.parseUTCDate("2008-05-10T12:31:02.008Z"));
        TestUtils.assertEquals("2008-05", sink);
    }

    @Test
    public void testFormatDateTime() {
        assertTrue("2014-11-30T12:34:55.332Z");
        assertTrue("2008-03-15T11:22:30.500Z");
        assertTrue("1917-10-01T11:22:30.500Z");
        assertTrue("0900-01-01T01:02:00.005Z");
    }

    @Test
    public void testFormatHTTP() {
        DateFormatUtils.formatHTTP(sink, DateFormatUtils.parseUTCDate("2015-12-05T12:34:55.332Z"));
        TestUtils.assertEquals("Sat, 5 Dec 2015 12:34:55 GMT", sink);
    }

    @Test
    public void testFormatHTTP2() {
        DateFormatUtils.formatHTTP(sink, DateFormatUtils.parseUTCDate("2015-12-05T12:04:55.332Z"));
        TestUtils.assertEquals("Sat, 5 Dec 2015 12:04:55 GMT", sink);
    }

    @Test
    public void testGetCentury() throws NumericException {
        Assert.assertEquals(21, Dates.getCentury(DateFormatUtils.parseUTCDate("2008-05-12T23:45:51.045Z")));
        Assert.assertEquals(20, Dates.getCentury(DateFormatUtils.parseUTCDate("1999-12-31T23:59:59.999Z")));
        Assert.assertEquals(19, Dates.getCentury(DateFormatUtils.parseUTCDate("1899-12-31T23:59:59.999Z")));
        Assert.assertEquals(1, Dates.getCentury(DateFormatUtils.parseUTCDate("0001-01-01T00:00:00.000Z")));
    }

    @Test
    public void testGetDecade() throws NumericException {
        Assert.assertEquals(200, Dates.getDecade(DateFormatUtils.parseUTCDate("2008-05-12T23:45:51.045Z")));
        Assert.assertEquals(199, Dates.getDecade(DateFormatUtils.parseUTCDate("1999-12-31T23:59:59.999Z")));
        Assert.assertEquals(190, Dates.getDecade(DateFormatUtils.parseUTCDate("1900-01-01T00:00:00.000Z")));
    }

    @Test
    public void testGetDow() throws NumericException {
        Assert.assertEquals(1, Dates.getDow(DateFormatUtils.parseUTCDate("2008-05-12T23:45:51.045Z")));
        Assert.assertEquals(0, Dates.getDow(DateFormatUtils.parseUTCDate("2008-05-11T00:00:00.000Z")));
        Assert.assertEquals(6, Dates.getDow(DateFormatUtils.parseUTCDate("2008-05-17T00:00:00.000Z")));
    }

    @Test
    public void testGetDoy() throws NumericException {
        Assert.assertEquals(133, Dates.getDoy(DateFormatUtils.parseUTCDate("2008-05-12T23:45:51.045Z")));
        Assert.assertEquals(1, Dates.getDoy(DateFormatUtils.parseUTCDate("2008-01-01T00:00:00.000Z")));
        Assert.assertEquals(366, Dates.getDoy(DateFormatUtils.parseUTCDate("2008-12-31T23:59:59.999Z")));
        Assert.assertEquals(365, Dates.getDoy(DateFormatUtils.parseUTCDate("2009-12-31T23:59:59.999Z")));
    }

    @Test
    public void testGetIsoYear() throws NumericException {
        Assert.assertEquals(2008, Dates.getIsoYear(DateFormatUtils.parseUTCDate("2008-05-12T23:45:51.045Z")));
        Assert.assertEquals(2009, Dates.getIsoYear(DateFormatUtils.parseUTCDate("2008-12-29T00:00:00.000Z")));
        Assert.assertEquals(2009, Dates.getIsoYear(DateFormatUtils.parseUTCDate("2009-01-01T00:00:00.000Z")));
    }

    @Test
    public void testGetMillennium() throws NumericException {
        Assert.assertEquals(3, Dates.getMillennium(DateFormatUtils.parseUTCDate("2008-05-12T23:45:51.045Z")));
        Assert.assertEquals(2, Dates.getMillennium(DateFormatUtils.parseUTCDate("1999-12-31T23:59:59.999Z")));
        Assert.assertEquals(2, Dates.getMillennium(DateFormatUtils.parseUTCDate("1001-01-01T00:00:00.000Z")));
        Assert.assertEquals(1, Dates.getMillennium(DateFormatUtils.parseUTCDate("1000-12-31T23:59:59.999Z")));
    }

    @Test
    public void testMonthsBetween() {
        // a < b, same year
        Assert.assertEquals(2,
                Dates.getMonthsBetween(
                        DateFormatUtils.parseUTCDate("2014-05-12T23:45:51.045Z"),
                        DateFormatUtils.parseUTCDate("2014-07-15T23:45:51.045Z")
                )
        );

        // a > b, same year
        Assert.assertEquals(2,
                Dates.getMonthsBetween(
                        DateFormatUtils.parseUTCDate("2014-07-15T23:45:51.045Z"),
                        DateFormatUtils.parseUTCDate("2014-05-12T23:45:51.045Z")
                )
        );

        // a < b, different year
        Assert.assertEquals(26,
                Dates.getMonthsBetween(
                        DateFormatUtils.parseUTCDate("2014-05-12T23:45:51.045Z"),
                        DateFormatUtils.parseUTCDate("2016-07-15T23:45:51.045Z")
                )
        );

        // a < b, same year, a has higher residuals
        Assert.assertEquals(1,
                Dates.getMonthsBetween(DateFormatUtils.parseUTCDate("2014-05-12T23:45:51.045Z"),
                        DateFormatUtils.parseUTCDate("2014-07-03T23:45:51.045Z"))
        );

        // a < b, a before epoch, a has higher residuals
        Assert.assertEquals(109 * 12 + 1,
                Dates.getMonthsBetween(DateFormatUtils.parseUTCDate("1905-05-12T23:45:51.045Z"),
                        DateFormatUtils.parseUTCDate("2014-07-03T23:45:51.045Z"))
        );
    }

    @Test
    public void testNExtOrSameDow3() {
        // thursday
        long millis = DateFormatUtils.parseUTCDate("2017-04-06T00:00:00.000Z");
        DateFormatUtils.appendDateTime(sink, Dates.nextOrSameDayOfWeek(millis, 4));
        TestUtils.assertEquals("2017-04-06T00:00:00.000Z", sink);
    }

    @Test
    public void testNextOrSameDow1() {
        // thursday
        long millis = DateFormatUtils.parseUTCDate("2017-04-06T00:00:00.000Z");
        DateFormatUtils.appendDateTime(sink, Dates.nextOrSameDayOfWeek(millis, 3));
        TestUtils.assertEquals("2017-04-12T00:00:00.000Z", sink);
    }

    @Test
    public void testNextOrSameDow2() {
        // thursday
        long millis = DateFormatUtils.parseUTCDate("2017-04-06T00:00:00.000Z");
        DateFormatUtils.appendDateTime(sink, Dates.nextOrSameDayOfWeek(millis, 6));
        TestUtils.assertEquals("2017-04-08T00:00:00.000Z", sink);
    }

    @Test
    public void testOverflowDate() {
        Assert.assertEquals("6477-07-27T03:15:50.400Z", Dates.toString(142245170150400L));
    }

    @Test
    public void testParseBadISODate() {
        expectExceptionDateTime("201");
        expectExceptionDateTime("2014");
        expectExceptionDateTime("2014-");
        expectExceptionDateTime("2014-0");
        expectExceptionDateTime("2014-03");
        expectExceptionDateTime("2014-03-");
        expectExceptionDateTime("2014-03-1");
        expectExceptionDateTime("2014-03-10");
        expectExceptionDateTime("2014-03-10T0");
        expectExceptionDateTime("2014-03-10T01");
        expectExceptionDateTime("2014-03-10T01-");
        expectExceptionDateTime("2014-03-10T01:1");
        expectExceptionDateTime("2014-03-10T01:19");
        expectExceptionDateTime("2014-03-10T01:19:");
        expectExceptionDateTime("2014-03-10T01:19:28.");
        expectExceptionDateTime("2014-03-10T01:19:28.2");
        expectExceptionDateTime("2014-03-10T01:19:28.255K");
    }

    @Test
    public void testParseDateTime() {
        String date = "2008-02-29T10:54:01.010Z";
        DateFormatUtils.appendDateTime(sink, DateFormatUtils.parseUTCDate(date));
        TestUtils.assertEquals(date, sink);
    }

    @Test
    public void testParseDateTimePrevEpoch() {
        String date = "1812-02-29T10:54:01.010Z";
        DateFormatUtils.appendDateTime(sink, DateFormatUtils.parseUTCDate(date));
        TestUtils.assertEquals(date, sink);
    }

    @Test(expected = NumericException.class)
    public void testParseWrongDay() {
        DateFormatUtils.parseUTCDate("2013-09-31T00:00:00.000Z");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongHour() {
        DateFormatUtils.parseUTCDate("2013-09-30T25:00:00.000Z");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongMillis() {
        DateFormatUtils.parseUTCDate("2013-09-30T22:04:34.1024Z");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongMinute() {
        DateFormatUtils.parseUTCDate("2013-09-30T22:61:00.000Z");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongMonth() {
        DateFormatUtils.parseUTCDate("2013-00-12T00:00:00.000Z");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongSecond() {
        DateFormatUtils.parseUTCDate("2013-09-30T22:04:60.000Z");
    }

    @Test
    public void testPreviousOrSameDow1() {
        // thursday
        long millis = DateFormatUtils.parseUTCDate("2017-04-06T00:00:00.000Z");
        DateFormatUtils.appendDateTime(sink, Dates.previousOrSameDayOfWeek(millis, 3));
        TestUtils.assertEquals("2017-04-05T00:00:00.000Z", sink);
    }

    @Test
    public void testPreviousOrSameDow2() {
        // thursday
        long millis = DateFormatUtils.parseUTCDate("2017-04-06T00:00:00.000Z");
        DateFormatUtils.appendDateTime(sink, Dates.previousOrSameDayOfWeek(millis, 6));
        TestUtils.assertEquals("2017-04-01T00:00:00.000Z", sink);
    }

    @Test
    public void testPreviousOrSameDow3() {
        // thursday
        long millis = DateFormatUtils.parseUTCDate("2017-04-06T00:00:00.000Z");
        DateFormatUtils.appendDateTime(sink, Dates.previousOrSameDayOfWeek(millis, 4));
        TestUtils.assertEquals("2017-04-06T00:00:00.000Z", sink);
    }

    @Test
    public void testToUSecString() throws NumericException {
        String result = Dates.toUSecString(DateFormatUtils.parseUTCDate("2008-05-12T23:45:51.045Z"));
        Assert.assertEquals("2008-05-12T23:45:51.045Z", result);
    }

    @Test
    public void testWeekOfMonth() {
        long millis = DateFormatUtils.parseUTCDate("2020-01-01T17:16:30.192Z");
        Assert.assertEquals(1, Dates.getWeekOfMonth(millis));
        millis = DateFormatUtils.parseUTCDate("2019-03-10T07:16:30.192Z");
        Assert.assertEquals(2, Dates.getWeekOfMonth(millis));
        millis = DateFormatUtils.parseUTCDate("2020-12-31T12:00:00.000Z");
        Assert.assertEquals(5, Dates.getWeekOfMonth(millis));
        millis = DateFormatUtils.parseUTCDate("2021-12-31T12:00:00.000Z");
        Assert.assertEquals(5, Dates.getWeekOfMonth(millis));
        millis = DateFormatUtils.parseUTCDate("-2019-03-10T07:16:30.192Z");
        Assert.assertEquals(2, Dates.getWeekOfMonth(millis));
        millis = DateFormatUtils.parseUTCDate("-2020-12-31T12:00:00.000Z");
        Assert.assertEquals(5, Dates.getWeekOfMonth(millis));
    }

    @Test
    public void testWeekOfYear() {
        long millis = DateFormatUtils.parseUTCDate("2020-01-01T17:16:30.192Z");
        Assert.assertEquals(1, Dates.getWeekOfYear(millis));
        millis = DateFormatUtils.parseUTCDate("2019-03-10T07:16:30.192Z");
        Assert.assertEquals(10, Dates.getWeekOfYear(millis));
        millis = DateFormatUtils.parseUTCDate("2020-03-10T07:16:30.192Z");
        Assert.assertEquals(11, Dates.getWeekOfYear(millis));
        millis = DateFormatUtils.parseUTCDate("1893-03-19T17:16:30.192Z");
        Assert.assertEquals(12, Dates.getWeekOfYear(millis));
        millis = DateFormatUtils.parseUTCDate("2020-12-31T12:00:00.000Z");
        Assert.assertEquals(53, Dates.getWeekOfYear(millis));
        millis = DateFormatUtils.parseUTCDate("2021-12-31T12:00:00.000Z");
        Assert.assertEquals(53, Dates.getWeekOfYear(millis));
        millis = DateFormatUtils.parseUTCDate("-2020-12-31T12:00:00.000Z");
        Assert.assertEquals(53, Dates.getWeekOfYear(millis));
        millis = DateFormatUtils.parseUTCDate("-2021-12-31T12:00:00.000Z");
        Assert.assertEquals(53, Dates.getWeekOfYear(millis));
    }

    @Test
    public void testYearsBetween() {
        Assert.assertEquals(112,
                Dates.getYearsBetween(
                        DateFormatUtils.parseUTCDate("1904-11-05T23:45:41.045Z"),
                        DateFormatUtils.parseUTCDate("2017-07-24T23:45:31.045Z")
                )
        );

        Assert.assertEquals(113,
                Dates.getYearsBetween(
                        DateFormatUtils.parseUTCDate("1904-11-05T23:45:41.045Z"),
                        DateFormatUtils.parseUTCDate("2017-12-24T23:45:51.045Z")
                )
        );
    }

    @Test
    public void testYearsBetween2() throws NumericException {
        long millis1 = DateFormatUtils.parseUTCDate("2020-04-24T01:49:12.005Z");
        long millis2 = DateFormatUtils.parseUTCDate("2025-04-24T01:49:12.005Z");
        Assert.assertEquals(5, Dates.getYearsBetween(millis1, millis2));
        Assert.assertEquals(5, Dates.getYearsBetween(millis2, millis1));
    }

    @Test
    public void testYearsBetween3() throws NumericException {
        long millis1 = DateFormatUtils.parseUTCDate("2020-04-24T01:49:12.005Z");
        long millis2 = DateFormatUtils.parseUTCDate("2024-04-24T01:49:12.005Z");
        Assert.assertEquals(4, Dates.getYearsBetween(millis1, millis2));
        Assert.assertEquals(4, Dates.getYearsBetween(millis2, millis1));
    }

    private void assertTrue(String date) throws NumericException {
        DateFormatUtils.appendDateTime(sink, DateFormatUtils.parseUTCDate(date));
        TestUtils.assertEquals(date, sink);
        sink.clear();
    }

    private void expectExceptionDateTime(String s) {
        try {
            DateFormatUtils.parseUTCDate(s);
            Assert.fail("Expected exception");
        } catch (NumericException ignore) {
        }
    }
}

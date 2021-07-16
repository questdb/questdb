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
import io.questdb.std.NumericException;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.std.datetime.TimeZoneRuleFactory.RESOLUTION_MICROS;

public class TimestampsTest {

    private final StringSink sink = new StringSink();

    @Before
    public void setUp() {
        sink.clear();
    }

    @Test
    public void testAddDaysPrevEpoch() throws Exception {
        long micros = TimestampFormatUtils.parseTimestamp("1888-05-12T23:45:51.045Z");
        TimestampFormatUtils.appendDateTime(sink, Timestamps.addDays(micros, 24));
        TestUtils.assertEquals("1888-06-05T23:45:51.045Z", sink);
    }

    @Test
    public void testAddMonths() throws Exception {
        long micros = TimestampFormatUtils.parseTimestamp("2008-05-12T23:45:51.045Z");
        TimestampFormatUtils.appendDateTime(sink, Timestamps.addMonths(micros, -10));
        TestUtils.assertEquals("2007-07-12T23:45:51.045Z", sink);
    }

    @Test
    public void testAddMonthsPrevEpoch() throws Exception {
        long micros = TimestampFormatUtils.parseTimestamp("1888-05-12T23:45:51.045Z");
        TimestampFormatUtils.appendDateTime(sink, Timestamps.addMonths(micros, -10));
        TestUtils.assertEquals("1887-07-12T23:45:51.045Z", sink);
    }

    @Test
    public void testAddYears() throws Exception {
        long micros = TimestampFormatUtils.parseTimestamp("1988-05-12T23:45:51.045Z");
        TimestampFormatUtils.appendDateTime(sink, Timestamps.addYear(micros, 10));
        TestUtils.assertEquals("1998-05-12T23:45:51.045Z", sink);
    }

    @Test
    public void testAddYears3() throws Exception {
        long micros = TimestampFormatUtils.parseTimestamp("2014-01-01T00:00:00.000Z");
        TimestampFormatUtils.appendDateTime(sink, Timestamps.addYear(micros, 1));
        TestUtils.assertEquals("2015-01-01T00:00:00.000Z", sink);
    }

    @Test
    public void testAddYearsNonLeapToLeap() throws Exception {
        long micros = TimestampFormatUtils.parseTimestamp("2015-01-01T00:00:00.000Z");
        TimestampFormatUtils.appendDateTime(sink, Timestamps.addYear(micros, 1));
        TestUtils.assertEquals("2016-01-01T00:00:00.000Z", sink);
    }

    @Test
    public void testAddYearsPrevEpoch() throws Exception {
        long micros = TimestampFormatUtils.parseTimestamp("1888-05-12T23:45:51.045Z");
        TimestampFormatUtils.appendDateTime(sink, Timestamps.addYear(micros, 10));
        TestUtils.assertEquals("1898-05-12T23:45:51.045Z", sink);
    }

    @Test
    public void testCeilDD() throws Exception {
        long micros = TimestampFormatUtils.parseTimestamp("2008-05-12T23:45:51.045Z");
        TimestampFormatUtils.appendDateTime(sink, Timestamps.ceilDD(micros));
        TestUtils.assertEquals("2008-05-12T23:59:59.999Z", sink);
    }

    @Test
    public void testCeilDDPrevEpoch() throws Exception {
        long micros = TimestampFormatUtils.parseTimestamp("1888-05-12T23:45:51.045Z");
        TimestampFormatUtils.appendDateTime(sink, Timestamps.ceilDD(micros));
        TestUtils.assertEquals("1888-05-12T23:59:59.999Z", sink);
    }

    @Test
    public void testCeilMM() throws Exception {
        long micros = TimestampFormatUtils.parseTimestamp("2008-05-12T23:45:51.045Z");
        TimestampFormatUtils.appendDateTime(sink, Timestamps.ceilMM(micros));
        TestUtils.assertEquals("2008-05-31T23:59:59.999Z", sink);
    }

    @Test
    public void testCeilYYYY() throws Exception {
        long micros = TimestampFormatUtils.parseTimestamp("2008-05-12T23:45:51.045Z");
        TimestampFormatUtils.appendDateTime(sink, Timestamps.ceilYYYY(micros));
        TestUtils.assertEquals("2008-12-31T23:59:59.999Z", sink);
    }

    @Test
    public void testDayOfWeek() throws Exception {
        long micros = TimestampFormatUtils.parseTimestamp("1893-03-19T17:16:30.192Z");
        Assert.assertEquals(7, Timestamps.getDayOfWeek(micros));
        Assert.assertEquals(1, Timestamps.getDayOfWeekSundayFirst(micros));
        micros = TimestampFormatUtils.parseTimestamp("2017-04-09T17:16:30.192Z");
        Assert.assertEquals(7, Timestamps.getDayOfWeek(micros));
        Assert.assertEquals(1, Timestamps.getDayOfWeekSundayFirst(micros));
    }

    @Test
    public void testDaysBetween() throws Exception {
        Assert.assertEquals(41168,
                Timestamps.getDaysBetween(
                        TimestampFormatUtils.parseTimestamp("1904-11-05T23:45:41.045Z"),
                        TimestampFormatUtils.parseTimestamp("2017-07-24T23:45:31.045Z")
                )
        );
        Assert.assertEquals(41169,
                Timestamps.getDaysBetween(
                        TimestampFormatUtils.parseTimestamp("1904-11-05T23:45:41.045Z"),
                        TimestampFormatUtils.parseTimestamp("2017-07-24T23:45:51.045Z")
                )
        );
    }

    @Test
    public void testFloorDD() throws Exception {
        long micros = TimestampFormatUtils.parseTimestamp("2008-05-12T23:45:51.045Z");
        TimestampFormatUtils.appendDateTime(sink, Timestamps.floorDD(micros));
        TestUtils.assertEquals("2008-05-12T00:00:00.000Z", sink);
    }

    @Test
    public void testFloorHH() throws Exception {
        long micros = TimestampFormatUtils.parseTimestamp("2008-05-12T23:45:51.045Z");
        TimestampFormatUtils.appendDateTime(sink, Timestamps.floorHH(micros));
        TestUtils.assertEquals("2008-05-12T23:00:00.000Z", sink);
    }

    @Test
    public void testFloorMM() throws Exception {
        long micros = TimestampFormatUtils.parseTimestamp("2008-05-12T23:45:51.045Z");
        TimestampFormatUtils.appendDateTime(sink, Timestamps.floorMM(micros));
        TestUtils.assertEquals("2008-05-01T00:00:00.000Z", sink);
    }

    @Test
    public void testFloorYYYY() throws Exception {
        long micros = TimestampFormatUtils.parseTimestamp("2008-05-12T23:45:51.045Z");
        TimestampFormatUtils.appendDateTime(sink, Timestamps.floorYYYY(micros));
        TestUtils.assertEquals("2008-01-01T00:00:00.000Z", sink);
    }

    @Test
    public void testFormatCalDate1() throws Exception {
        TimestampFormatUtils.formatDashYYYYMMDD(sink, TimestampFormatUtils.parseTimestamp("2008-05-10T12:31:02.008Z"));
        TestUtils.assertEquals("2008-05-10", sink);
    }

    @Test
    public void testFormatCalDate2() throws Exception {
        TimestampFormatUtils.formatYYYYMM(sink, TimestampFormatUtils.parseTimestamp("2008-05-10T12:31:02.008Z"));
        TestUtils.assertEquals("2008-05", sink);
    }

    @Test
    public void testFormatCalDate3() throws Exception {
        TimestampFormatUtils.formatYYYYMMDD(sink, TimestampFormatUtils.parseTimestamp("2008-05-10T12:31:02.008Z"));
        TestUtils.assertEquals("20080510", sink);
    }

    @Test
    public void testFormatDateTime() throws Exception {
        assertTrue("2014-11-30T12:34:55.332Z");
        assertTrue("2008-03-15T11:22:30.500Z");
        assertTrue("1917-10-01T11:22:30.500Z");
        assertTrue("0900-01-01T01:02:00.005Z");
    }

    @Test
    public void testFormatHTTP() throws Exception {
        TimestampFormatUtils.formatHTTP(sink, TimestampFormatUtils.parseTimestamp("2015-12-05T12:34:55.332Z"));
        TestUtils.assertEquals("Sat, 5 Dec 2015 12:34:55 GMT", sink);
    }

    @Test
    public void testFormatHTTP2() throws Exception {
        TimestampFormatUtils.formatHTTP(sink, TimestampFormatUtils.parseTimestamp("2015-12-05T12:04:55.332Z"));
        TestUtils.assertEquals("Sat, 5 Dec 2015 12:04:55 GMT", sink);
    }

    @Test
    public void testFormatNanosTz() throws Exception {
        final long micros = TimestampFormatUtils.parseDateTime("2008-05-10T12:31:02.008998991+01:00");
        TimestampFormatUtils.USEC_UTC_FORMAT.format(micros, DateLocaleFactory.INSTANCE.getLocale("en"), null, sink);
        TestUtils.assertEquals("2008-05-10T11:31:02.008998", sink);
    }

    @Test
    public void testFormatNanosZ() throws Exception {
        final long micros = TimestampFormatUtils.parseDateTime("2008-05-10T12:31:02.008998991Z");
        TimestampFormatUtils.USEC_UTC_FORMAT.format(micros, DateLocaleFactory.INSTANCE.getLocale("en"), null, sink);
        TestUtils.assertEquals("2008-05-10T12:31:02.008998", sink);
    }

    @Test
    public void testMonthsBetween() throws Exception {
        // a < b, same year
        Assert.assertEquals(2,
                Timestamps.getMonthsBetween(
                        TimestampFormatUtils.parseTimestamp("2014-05-12T23:45:51.045Z"),
                        TimestampFormatUtils.parseTimestamp("2014-07-15T23:45:51.045Z")
                )
        );

        // a > b, same year
        Assert.assertEquals(2,
                Timestamps.getMonthsBetween(
                        TimestampFormatUtils.parseTimestamp("2014-07-15T23:45:51.045Z"),
                        TimestampFormatUtils.parseTimestamp("2014-05-12T23:45:51.045Z")
                )
        );

        // a < b, different year
        Assert.assertEquals(26,
                Timestamps.getMonthsBetween(
                        TimestampFormatUtils.parseTimestamp("2014-05-12T23:45:51.045Z"),
                        TimestampFormatUtils.parseTimestamp("2016-07-15T23:45:51.045Z")
                )
        );

        // a < b, same year, a has higher residuals
        Assert.assertEquals(1,
                Timestamps.getMonthsBetween(TimestampFormatUtils.parseTimestamp("2014-05-12T23:45:51.045Z"),
                        TimestampFormatUtils.parseTimestamp("2014-07-03T23:45:51.045Z"))
        );

        // a < b, a before epoch, a has higher residuals
        Assert.assertEquals(109 * 12 + 1,
                Timestamps.getMonthsBetween(TimestampFormatUtils.parseTimestamp("1905-05-12T23:45:51.045Z"),
                        TimestampFormatUtils.parseTimestamp("2014-07-03T23:45:51.045Z"))
        );
    }

    @Test
    public void testNExtOrSameDow3() throws Exception {
        // thursday
        long micros = TimestampFormatUtils.parseTimestamp("2017-04-06T00:00:00.000Z");
        TimestampFormatUtils.appendDateTime(sink, Timestamps.nextOrSameDayOfWeek(micros, 4));
        TestUtils.assertEquals("2017-04-06T00:00:00.000Z", sink);
    }

    @Test
    public void testNextDSTRulesAfterLast() throws NumericException {
        String tz = "Europe/Berlin";
        TimeZoneRules rules = TimestampFormatUtils.enLocale.getZoneRules(
                Numbers.decodeLowInt(TimestampFormatUtils.enLocale.matchZone(tz, 0, tz.length())),
                RESOLUTION_MICROS
        );

        final long ts = rules.getNextDST(TimestampFormatUtils.parseUTCTimestamp("2021-10-31T01:00:00.000000Z"));
        Assert.assertEquals("2022-03-27T01:00:00.000Z", Timestamps.toString(ts));
    }

    @Test
    public void testNextDSTRulesAfterFirst() throws NumericException {
        String tz = "Europe/Berlin";
        TimeZoneRules rules = TimestampFormatUtils.enLocale.getZoneRules(
                Numbers.decodeLowInt(TimestampFormatUtils.enLocale.matchZone(tz, 0, tz.length())),
                RESOLUTION_MICROS
        );

        final long ts = rules.getNextDST(TimestampFormatUtils.parseUTCTimestamp("2021-07-03T01:00:00.000000Z"));
        Assert.assertEquals("2021-10-31T01:00:00.000Z", Timestamps.toString(ts));
    }

    @Test
    public void testNextDSTHistory() throws NumericException {
        String tz = "Europe/Berlin";
        TimeZoneRules rules = TimestampFormatUtils.enLocale.getZoneRules(
                Numbers.decodeLowInt(TimestampFormatUtils.enLocale.matchZone(tz, 0, tz.length())),
                RESOLUTION_MICROS
        );

        final long ts = rules.getNextDST(TimestampFormatUtils.parseUTCTimestamp("1991-09-20T01:00:00.000000Z"));
        Assert.assertEquals("1991-09-29T01:00:00.000Z", Timestamps.toString(ts));
    }

    @Test
    public void testNextDSTHistoryLast() throws NumericException {
        String tz = "Europe/Berlin";
        TimeZoneRules rules = TimestampFormatUtils.enLocale.getZoneRules(
                Numbers.decodeLowInt(TimestampFormatUtils.enLocale.matchZone(tz, 0, tz.length())),
                RESOLUTION_MICROS
        );

        final long ts = rules.getNextDST(TimestampFormatUtils.parseUTCTimestamp("1997-10-26T01:00:00.000000Z"));
        Assert.assertEquals("1998-03-29T01:00:00.000Z", Timestamps.toString(ts));
    }

    @Test
    public void testNextDSTRulesBeforeFirst() throws NumericException {
        String tz = "Europe/Berlin";
        TimeZoneRules rules = TimestampFormatUtils.enLocale.getZoneRules(
                Numbers.decodeLowInt(TimestampFormatUtils.enLocale.matchZone(tz, 0, tz.length())),
                RESOLUTION_MICROS
        );

        final long ts = rules.getNextDST(TimestampFormatUtils.parseUTCTimestamp("2021-02-10T01:00:00.000000Z"));
        Assert.assertEquals("2021-03-28T01:00:00.000Z", Timestamps.toString(ts));
    }

    @Test
    public void testNextDSTFixed() throws NumericException {
        String tz = "GMT";
        TimeZoneRules rules = TimestampFormatUtils.enLocale.getZoneRules(
                Numbers.decodeLowInt(TimestampFormatUtils.enLocale.matchZone(tz, 0, tz.length())),
                RESOLUTION_MICROS
        );

        final long ts = rules.getNextDST(TimestampFormatUtils.parseUTCTimestamp("2021-02-10T01:00:00.000000Z"));
        Assert.assertEquals(Long.MAX_VALUE, ts);
    }

    @Test
    public void testNextOrSameDow1() throws Exception {
        // thursday
        long micros = TimestampFormatUtils.parseTimestamp("2017-04-06T00:00:00.000Z");
        TimestampFormatUtils.appendDateTime(sink, Timestamps.nextOrSameDayOfWeek(micros, 3));
        TestUtils.assertEquals("2017-04-12T00:00:00.000Z", sink);
    }

    @Test
    public void testNextOrSameDow2() throws Exception {
        // thursday
        long micros = TimestampFormatUtils.parseTimestamp("2017-04-06T00:00:00.000Z");
        TimestampFormatUtils.appendDateTime(sink, Timestamps.nextOrSameDayOfWeek(micros, 6));
        TestUtils.assertEquals("2017-04-08T00:00:00.000Z", sink);
    }

    @Test
    public void testOverflowDate() {
        Assert.assertEquals("6477-07-27T03:15:50.400Z", Timestamps.toString(142245170150400000L));
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
    public void testParseDateTime() throws Exception {
        String date = "2008-02-29T10:54:01.010Z";
        TimestampFormatUtils.appendDateTime(sink, TimestampFormatUtils.parseTimestamp(date));
        TestUtils.assertEquals(date, sink);
    }

    @Test
    public void testParseDateTimePrevEpoch() throws Exception {
        String date = "1812-02-29T10:54:01.010Z";
        TimestampFormatUtils.appendDateTime(sink, TimestampFormatUtils.parseTimestamp(date));
        TestUtils.assertEquals(date, sink);
    }

    @Test
    public void testParseTimestampNotNullLocale() {
        try {
            // we deliberately mangle timezone so that function begins to rely on locale to resole text
            TimestampFormatUtils.parseUTCTimestamp("2020-01-10T15:00:01.000143Zz");
            Assert.fail();
        } catch (NumericException ignored) {
        }
    }

    @Test(expected = NumericException.class)
    public void testParseWrongDay() throws Exception {
        TimestampFormatUtils.parseTimestamp("2013-09-31T00:00:00.000Z");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongHour() throws Exception {
        TimestampFormatUtils.parseTimestamp("2013-09-30T24:00:00.000Z");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongMicros() throws Exception {
        TimestampFormatUtils.parseTimestamp("2013-09-30T22:04:34.1024091Z");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongMinute() throws Exception {
        TimestampFormatUtils.parseTimestamp("2013-09-30T22:61:00.000Z");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongMonth() throws Exception {
        TimestampFormatUtils.parseTimestamp("2013-00-12T00:00:00.000Z");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongSecond() throws Exception {
        TimestampFormatUtils.parseTimestamp("2013-09-30T22:04:60.000Z");
    }

    @Test
    public void testPreviousOrSameDow1() throws Exception {
        // thursday
        long micros = TimestampFormatUtils.parseTimestamp("2017-04-06T00:00:00.000Z");
        TimestampFormatUtils.appendDateTime(sink, Timestamps.previousOrSameDayOfWeek(micros, 3));
        TestUtils.assertEquals("2017-04-05T00:00:00.000Z", sink);
    }

    @Test
    public void testPreviousOrSameDow2() throws Exception {
        // thursday
        long micros = TimestampFormatUtils.parseTimestamp("2017-04-06T00:00:00.000Z");
        TimestampFormatUtils.appendDateTime(sink, Timestamps.previousOrSameDayOfWeek(micros, 6));
        TestUtils.assertEquals("2017-04-01T00:00:00.000Z", sink);
    }

    @Test
    public void testPreviousOrSameDow3() throws Exception {
        // thursday
        long micros = TimestampFormatUtils.parseTimestamp("2017-04-06T00:00:00.000Z");
        TimestampFormatUtils.appendDateTime(sink, Timestamps.previousOrSameDayOfWeek(micros, 4));
        TestUtils.assertEquals("2017-04-06T00:00:00.000Z", sink);
    }

    @Test(expected = NumericException.class)
    public void testToTimezoneInvalidTimezoneName() throws NumericException {
        DateLocale locale = DateLocaleFactory.INSTANCE.getLocale("en");
        long micros = TimestampFormatUtils.parseTimestamp("2019-12-10T10:00:00.000000Z");
        Timestamps.toTimezone(micros, locale, "Somewhere");
    }

    @Test
    public void testToTimezoneWithHours() throws NumericException {
        DateLocale locale = DateLocaleFactory.INSTANCE.getLocale("en");
        long micros = TimestampFormatUtils.parseTimestamp("2019-12-10T10:00:00.000000Z");
        long offsetMicros = Timestamps.toTimezone(micros, locale, "+03:45");
        TestUtils.assertEquals("2019-12-10T13:45:00.000Z", Timestamps.toString(offsetMicros));
    }

    @Test
    public void testToTimezoneWithHoursInString() throws NumericException {
        DateLocale locale = DateLocaleFactory.INSTANCE.getLocale("en");
        long micros = TimestampFormatUtils.parseTimestamp("2019-12-10T10:00:00.000000Z");
        long offsetMicros = Timestamps.toTimezone(micros, locale, "hello +03:45 there", 6, 12);
        TestUtils.assertEquals("2019-12-10T13:45:00.000Z", Timestamps.toString(offsetMicros));
    }

    @Test
    public void testToTimezoneWithTimezoneName() throws NumericException {
        DateLocale locale = DateLocaleFactory.INSTANCE.getLocale("en");
        long micros = TimestampFormatUtils.parseTimestamp("2019-12-10T10:00:00.000000Z");
        long offsetMicros = Timestamps.toTimezone(micros, locale, "Europe/Prague");
        TestUtils.assertEquals("2019-12-10T11:00:00.000Z", Timestamps.toString(offsetMicros));
    }

    @Test(expected = NumericException.class)
    public void testToUTCInvalidTimezoneName() throws NumericException {
        DateLocale locale = DateLocaleFactory.INSTANCE.getLocale("en");
        long micros = TimestampFormatUtils.parseTimestamp("2019-12-10T10:00:00.000000Z");
        Timestamps.toUTC(micros, locale, "Somewhere");
    }

    @Test
    public void testToUTCWithHours() throws NumericException {
        DateLocale locale = DateLocaleFactory.INSTANCE.getLocale("en");
        long micros = TimestampFormatUtils.parseTimestamp("2019-12-10T10:00:00.000000Z");
        long offsetMicros = Timestamps.toUTC(micros, locale, "+03:45");
        TestUtils.assertEquals("2019-12-10T06:15:00.000Z", Timestamps.toString(offsetMicros));
    }

    @Test
    public void testToUTCWithHoursInString() throws NumericException {
        DateLocale locale = DateLocaleFactory.INSTANCE.getLocale("en");
        long micros = TimestampFormatUtils.parseTimestamp("2019-12-10T10:00:00.000000Z");
        long offsetMicros = Timestamps.toUTC(micros, locale, "hello +03:45 there", 6, 12);
        TestUtils.assertEquals("2019-12-10T06:15:00.000Z", Timestamps.toString(offsetMicros));
    }

    @Test
    public void testToUTCWithTimezoneName() throws NumericException {
        DateLocale locale = DateLocaleFactory.INSTANCE.getLocale("en");
        long micros = TimestampFormatUtils.parseTimestamp("2019-12-10T10:00:00.000000Z");
        long offsetMicros = Timestamps.toUTC(micros, locale, "Europe/Prague");
        TestUtils.assertEquals("2019-12-10T09:00:00.000Z", Timestamps.toString(offsetMicros));
    }

    @Test
    public void testYearsBetween() throws Exception {
        Assert.assertEquals(112,
                Timestamps.getYearsBetween(
                        TimestampFormatUtils.parseTimestamp("1904-11-05T23:45:41.045Z"),
                        TimestampFormatUtils.parseTimestamp("2017-07-24T23:45:31.045Z")
                )
        );

        Assert.assertEquals(113,
                Timestamps.getYearsBetween(
                        TimestampFormatUtils.parseTimestamp("1904-11-05T23:45:41.045Z"),
                        TimestampFormatUtils.parseTimestamp("2017-12-24T23:45:51.045Z")
                )
        );
    }

    @Test
    public void testYearsBetween2() throws NumericException {
        long micros1 = TimestampFormatUtils.parseTimestamp("2020-04-24T01:49:12.005Z");
        long micros2 = TimestampFormatUtils.parseTimestamp("2025-04-24T01:49:12.005Z");
        Assert.assertEquals(5, Timestamps.getYearsBetween(micros1, micros2));
        Assert.assertEquals(5, Timestamps.getYearsBetween(micros2, micros1));
    }

    @Test
    public void testYearsBetween3() throws NumericException {
        long micros1 = TimestampFormatUtils.parseTimestamp("2020-04-24T01:49:12.005Z");
        long micros2 = TimestampFormatUtils.parseTimestamp("2024-04-24T01:49:12.005Z");
        Assert.assertEquals(4, Timestamps.getYearsBetween(micros1, micros2));
        Assert.assertEquals(4, Timestamps.getYearsBetween(micros2, micros1));
    }

    private void assertTrue(String date) throws NumericException {
        TimestampFormatUtils.appendDateTime(sink, TimestampFormatUtils.parseTimestamp(date));
        TestUtils.assertEquals(date, sink);
        sink.clear();
    }

    private void expectExceptionDateTime(String s) {
        try {
            TimestampFormatUtils.parseTimestamp(s);
            Assert.fail("Expected exception");
        } catch (NumericException ignore) {
        }
    }
}

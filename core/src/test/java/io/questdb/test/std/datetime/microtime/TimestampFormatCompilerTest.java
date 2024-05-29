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

package io.questdb.test.std.datetime.microtime;

import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.IntHashSet;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.microtime.TimestampFormatCompiler;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.ex.BytecodeException;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static io.questdb.std.datetime.TimeZoneRuleFactory.RESOLUTION_MICROS;

public class TimestampFormatCompilerTest {

    private static final TimestampFormatCompiler compiler = new TimestampFormatCompiler();
    private final static DateFormat REFERENCE = compiler.compile("yyyy-MM-ddTHH:mm:ss.SSSUUUz");
    private static final DateLocale defaultLocale = DateLocaleFactory.INSTANCE.getLocale("en-GB");
    private final static StringSink sink = new StringSink();

    @BeforeClass
    public static void setUp() {
        TimestampFormatUtils.updateReferenceYear(Timestamps.toMicros(1997, 1, 1, 0, 0));
    }

    @Test
    public void test12HourSystemsOneBase() throws Exception {
        testAgainstJavaReferenceImpl("hh:mm a");
    }

    @Test
    public void test12HourSystemsZeroBase() throws Exception {
        testAgainstJavaReferenceImpl("KK:mm a");
    }

    @Test
    public void test24HourSystemsOneBase() throws Exception {
        testAgainstJavaReferenceImpl("kk:mm");
    }

    @Test
    public void test24HourSystemsZeroBase() throws Exception {
        testAgainstJavaReferenceImpl("HH:mm");
    }

    @Test(expected = NumericException.class)
    public void testBadAmPm() throws Exception {
        assertThat("KaMMy", "", "11 0910 am");
    }

    @Test(expected = NumericException.class)
    public void testBadAmPm2() throws Exception {
        assertThat("KaMMy", "", "11az0910");
    }

    @Test(expected = NumericException.class)
    public void testBadDelimiter() throws Exception {
        assertThat("y.MM", "0001-03-01T00:00:00.000Z", "1-03");
    }

    @Test(expected = NumericException.class)
    public void testBadMonth() throws Exception {
        assertThat("yMM", "0001-03-01T00:00:00.000Z", "133");
    }

    @Test
    public void testBasicParserCompiler() throws Exception {
        DateFormat fmt = compiler.compile("E, dd MMM yyyy a KK:m:s.S Z");
        String utcPattern = "yyyy-MM-ddTHH:mm:ss.SSSz";
        DateFormat utc = compiler.compile(utcPattern);
        long millis = fmt.parse("Mon, 08 Apr 2017 PM 11:11:10.123 UTC", defaultLocale);
        sink.clear();
        utc.format(millis, defaultLocale, "Z", sink);
        TestUtils.assertEquals("2017-04-08T23:11:10.123Z", sink);
    }

    @Test
    public void testDayGreedy() throws Exception {
        assertThat("d, MM-yy", "2011-10-03T00:00:00.000Z", "3, 10-11");
        assertThat("d, MM-yy", "2011-10-03T00:00:00.000Z", "03, 10-11");
        assertThat("d, MM-yy", "2011-10-25T00:00:00.000Z", "25, 10-11");
    }

    @Test
    public void testDayMonthYear() throws Exception {
        assertThat("dd-MM-yyyy", "2010-03-10T00:00:00.000Z", "10-03-2010");
    }

    @Test
    public void testDayMonthYearNoDelim() throws Exception {
        assertThat("yyyyddMM", "2010-03-10T00:00:00.000Z", "20101003");
    }

    @Test
    public void testDayOfYear() throws Exception {
        assertThat("D, MM-yyyy", "2010-11-01T00:00:00.000Z", "25, 11-2010");
    }

    @Test
    public void testDayOneDigit() throws Exception {
        assertThat("dyyyy", "2014-01-03T00:00:00.000Z", "32014");
    }

    @Test
    public void testEra() throws Exception {
        assertThat("E, dd-MM-yyyy G", "2014-04-03T00:00:00.000Z", "Tuesday, 03-04-2014 AD");
        assertThat("E, dd-MM-yyyy G", "-2013-04-03T00:00:00.000Z", "Tuesday, 03-04-2014 BC");
    }

    @Test
    public void testFirstHourIn12HourClock() throws Exception {
        assertThat("MM/dd/yyyy hh:mm:ss a", "2017-04-09T00:01:00.000Z", "04/09/2017 12:01:00 am");
    }

    @Test
    public void testFormatAMPM() throws Exception {
        assertFormat("pm, 31", "a, dd", "2017-03-31T14:00:00.000Z");
        assertFormat("pm, 31", "a, dd", "2017-03-31T12:00:00.000Z");
        assertFormat("am, 31", "a, dd", "2017-03-31T00:00:00.000Z");
        assertFormat("am, 31", "a, dd", "2017-03-31T11:59:59.999Z");
    }

    @Test
    public void testFormatBSTtoMSK() throws Exception {
        DateFormat fmt = get("dd-MM-yyyy HH:mm:ss Z");
        String targetTimezoneName = "MSK";

        long millis = fmt.parse("06-04-2017 01:09:30 BST", defaultLocale);
        millis += defaultLocale.getRules(targetTimezoneName, RESOLUTION_MICROS).getOffset(millis);
        sink.clear();
        fmt.format(millis, defaultLocale, targetTimezoneName, sink);
        TestUtils.assertEquals("06-04-2017 03:09:30 MSK", sink);
    }

    @Test
    public void testFormatDay() throws Exception {
        assertFormat("03", "dd", "2014-04-03T00:00:00.000Z");
    }

    @Test
    public void testFormatDayOfYear() throws Exception {
        assertFormat("1", "D", "2010-01-01T00:00:00.000Z");
        assertFormat("69", "D", "2010-03-10T00:00:00.000Z");
        assertFormat("70", "D", "2020-03-10T00:00:00.000Z");
    }

    @Test
    public void testFormatEra() throws Exception {
        assertFormat("AD", "G", "2017-04-09T00:00:00.000Z");
        assertFormat("BC", "G", "-1024-04-09T00:00:00.000Z");
    }

    @Test
    public void testFormatFirstHourIn12HourClock() throws Exception {
        assertFormat("04/09/2017 00:01:00 am", "MM/dd/yyyy KK:mm:ss a", "2017-04-09T00:01:00.000Z");
        assertFormat("04/09/2017 00:59:59 am", "MM/dd/yyyy KK:mm:ss a", "2017-04-09T00:59:59.000Z");
    }

    @Test
    public void testFormatHour23() throws Exception {
        assertFormat("pm, 14", "a, HH", "2017-03-31T14:00:00.000Z");
        assertFormat("pm, 12", "a, HH", "2017-03-31T12:00:00.000Z");
        assertFormat("am, 03", "a, HH", "2017-03-31T03:00:00.000Z");
        assertFormat("am, 11", "a, HH", "2017-03-31T11:59:59.999Z");

        assertFormat("14", "HH", "2017-03-31T14:00:00.000Z");
        assertFormat("12", "HH", "2017-03-31T12:00:00.000Z");
        assertFormat("03", "HH", "2017-03-31T03:00:00.000Z");
        assertFormat("11", "HH", "2017-03-31T11:59:59.999Z");
    }

    @Test
    public void testFormatHour23OneDigit() throws Exception {
        // H = hour, 0-23
        assertFormat("pm, 14", "a, H", "2017-03-31T14:00:00.000Z");
        assertFormat("pm, 12", "a, H", "2017-03-31T12:00:00.000Z");
        assertFormat("am, 3", "a, H", "2017-03-31T03:00:00.000Z");
        assertFormat("am, 11", "a, H", "2017-03-31T11:59:59.999Z");

        assertFormat("14", "H", "2017-03-31T14:00:00.000Z");
        assertFormat("12", "H", "2017-03-31T12:00:00.000Z");
        assertFormat("3", "H", "2017-03-31T03:00:00.000Z");
        assertFormat("11", "H", "2017-03-31T11:59:59.999Z");
        assertFormat("0", "H", "2017-03-31T00:00:00.000Z");
    }

    @Test
    public void testFormatHour24() throws Exception {
        // k = hour, 1-24
        assertFormat("pm, 14", "a, kk", "2017-03-31T14:00:00.000Z");
        assertFormat("pm, 12", "a, kk", "2017-03-31T12:00:00.000Z");
        assertFormat("am, 03", "a, kk", "2017-03-31T03:00:00.000Z");
        assertFormat("am, 11", "a, kk", "2017-03-31T11:59:59.999Z");

        assertFormat("14", "kk", "2017-03-31T14:00:00.000Z");
        assertFormat("12", "kk", "2017-03-31T12:00:00.000Z");
        assertFormat("03", "kk", "2017-03-31T03:00:00.000Z");
        assertFormat("11", "kk", "2017-03-31T11:59:59.999Z");
        assertFormat("24", "kk", "2017-03-31T00:00:00.000Z");
    }

    @Test
    public void testFormatHour24OneDigit() throws Exception {
        // k = hour, 1-24
        assertFormat("pm, 14", "a, k", "2017-03-31T14:00:00.000Z");
        assertFormat("pm, 12", "a, k", "2017-03-31T12:00:00.000Z");
        assertFormat("am, 3", "a, k", "2017-03-31T03:00:00.000Z");
        assertFormat("am, 11", "a, k", "2017-03-31T11:59:59.999Z");

        assertFormat("14", "k", "2017-03-31T14:00:00.000Z");
        assertFormat("12", "k", "2017-03-31T12:00:00.000Z");
        assertFormat("3", "k", "2017-03-31T03:00:00.000Z");
        assertFormat("11", "k", "2017-03-31T11:59:59.999Z");
    }

    @Test
    public void testFormatHourTwelve() throws Exception {
        // h = hour, 1-12
        assertFormat("pm, 02", "a, hh", "2017-03-31T14:00:00.000Z");
        assertFormat("pm, 12", "a, hh", "2017-03-31T12:00:00.000Z");
        assertFormat("am, 03", "a, hh", "2017-03-31T03:00:00.000Z");
        assertFormat("am, 11", "a, hh", "2017-03-31T11:59:59.999Z");

        assertFormat("02", "hh", "2017-03-31T14:00:00.000Z");
        assertFormat("12", "hh", "2017-03-31T12:00:00.000Z");
        assertFormat("03", "hh", "2017-03-31T03:00:00.000Z");
        assertFormat("11", "hh", "2017-03-31T11:59:59.999Z");
    }

    @Test
    public void testFormatHourTwelveOneDigit() throws Exception {
        // h = hour, 1-12
        assertFormat("pm, 2", "a, h", "2017-03-31T14:00:00.000Z");
        assertFormat("pm, 12", "a, h", "2017-03-31T12:00:00.000Z");
        assertFormat("am, 3", "a, h", "2017-03-31T03:00:00.000Z");
        assertFormat("am, 11", "a, h", "2017-03-31T11:59:59.999Z");
        assertFormat("am, 12", "a, h", "2017-03-31T00:00:00.000Z");
        assertFormat("pm, 12", "a, h", "2017-03-31T12:00:00.000Z");

        assertFormat("2", "h", "2017-03-31T14:00:00.000Z");
        assertFormat("12", "h", "2017-03-31T12:00:00.000Z");
        assertFormat("3", "h", "2017-03-31T03:00:00.000Z");
        assertFormat("11", "h", "2017-03-31T11:59:59.999Z");
        assertFormat("12", "h", "2017-03-31T00:00:00.000Z");
    }

    @Test
    public void testFormatHourZeroEleven() throws Exception {
        // K = hour, 0-11
        assertFormat("pm, 02", "a, KK", "2017-03-31T14:00:00.000Z");
        assertFormat("pm, 00", "a, KK", "2017-03-31T12:00:00.000Z");
        assertFormat("am, 03", "a, KK", "2017-03-31T03:00:00.000Z");
        assertFormat("am, 11", "a, KK", "2017-03-31T11:59:59.999Z");
        assertFormat("am, 00", "a, KK", "2017-03-31T00:00:00.000Z");
        assertFormat("pm, 00", "a, KK", "2017-03-31T12:00:00.000Z");

        assertFormat("02", "KK", "2017-03-31T14:00:00.000Z");
        assertFormat("00", "KK", "2017-03-31T12:00:00.000Z");
        assertFormat("00", "KK", "2017-03-31T00:00:00.000Z");
        assertFormat("03", "KK", "2017-03-31T03:00:00.000Z");
        assertFormat("11", "KK", "2017-03-31T11:59:59.999Z");
    }

    @Test
    public void testFormatHourZeroElevenOneDigit() throws Exception {
        assertFormat("pm, 2", "a, K", "2017-03-31T14:00:00.000Z");
        assertFormat("pm, 0", "a, K", "2017-03-31T12:00:00.000Z");
        assertFormat("am, 3", "a, K", "2017-03-31T03:00:00.000Z");
        assertFormat("am, 11", "a, K", "2017-03-31T11:59:59.999Z");
        assertFormat("am, 0", "a, K", "2017-03-31T00:00:00.000Z");
        assertFormat("pm, 0", "a, K", "2017-03-31T12:00:00.000Z");

        assertFormat("2", "K", "2017-03-31T14:00:00.000Z");
        assertFormat("0", "K", "2017-03-31T12:00:00.000Z");
        assertFormat("3", "K", "2017-03-31T03:00:00.000Z");
        assertFormat("11", "K", "2017-03-31T11:59:59.999Z");
    }

    @Test
    public void testFormatISOWeek() throws NumericException {
        assertFormat("05", "ww", "2022-01-31T02:02:02.000012Z");
        assertMicros("yyyy-ww HH:mm:ss.SSS U", "2022-01-31T02:02:02.001002Z", "2022-05 02:02:02.001 002");
    }

    @Test
    public void testFormatIsoWeekOfYear() throws Exception {
        assertFormat("53", "ww", "2010-01-01T00:00:00.000Z");
        assertFormat("10", "ww", "2010-03-10T00:00:00.000Z");
        assertFormat("11", "ww", "2020-03-10T00:00:00.000Z");
    }

    @Test
    public void testFormatMicros() throws Exception {
        assertFormat("678-15", "S-U", "1978-03-19T21:20:45.678Z", 15);
        assertFormat("678.025", "S.UUU", "1978-03-19T21:20:45.678Z", 25);
        assertFormat("1978, .025", "yyyy, .UUU", "1978-03-19T21:20:45.678Z", 25);
        assertFormat("1978, .25 678", "yyyy, .U SSS", "1978-03-19T21:20:45.678Z", 25);
    }

    @Test
    public void testFormatMicros3Micros() throws NumericException {
        assertFormat("2022-02-02 02:02:02.000 2", "y-MM-dd HH:mm:ss.SSS U", "2022-02-02T02:02:02.000002Z");
    }

    @Test
    public void testFormatMicros6Five() throws NumericException {
        assertFormat("2022-02-02 02:02:02.074812", "y-MM-dd HH:mm:ss.U+", "2022-02-02T02:02:02.074812Z");
    }

    @Test
    public void testFormatMicros6Four() throws NumericException {
        assertFormat("2022-02-02 02:02:02.004812", "y-MM-dd HH:mm:ss.U+", "2022-02-02T02:02:02.004812Z");
    }

    @Test
    public void testFormatMicros6Milli() throws NumericException {
        assertFormat("2022-02-02 02:02:02.123000", "y-MM-dd HH:mm:ss.U+", "2022-02-02T02:02:02.123000Z");
    }

    @Test
    public void testFormatMicros6One() throws NumericException {
        assertFormat("2022-02-02 02:02:02.000002", "y-MM-dd HH:mm:ss.U+", "2022-02-02T02:02:02.000002Z");
    }

    @Test
    public void testFormatMicros6Six() throws NumericException {
        assertFormat("2022-02-02 02:02:02.374812", "y-MM-dd HH:mm:ss.U+", "2022-02-02T02:02:02.374812Z");
    }

    @Test
    public void testFormatMicros6Three() throws NumericException {
        assertFormat("2022-02-02 02:02:02.000812", "y-MM-dd HH:mm:ss.U+", "2022-02-02T02:02:02.000812Z");
    }

    @Test
    public void testFormatMicros6Two() throws NumericException {
        assertFormat("2022-02-02 02:02:02.000012", "y-MM-dd HH:mm:ss.U+", "2022-02-02T02:02:02.000012Z");
    }

    @Test
    public void testFormatMillis() throws Exception {
        assertFormat("033", "SSS", "2017-03-31T14:00:05.033Z");
        assertFormat("579", "SSS", "2017-03-31T12:00:59.579Z");
    }

    @Test
    public void testFormatMillisOneDigit() throws Exception {
        assertFormat("15", "S", "2017-03-31T14:05:03.015Z");
        assertFormat("459", "S", "2017-03-31T12:59:45.459Z");
    }

    @Test
    public void testFormatMinute() throws Exception {
        assertFormat("05", "mm", "2017-03-31T14:05:00.000Z");
        assertFormat("59", "mm", "2017-03-31T12:59:00.000Z");
    }

    @Test
    public void testFormatMinuteOneDigit() throws Exception {
        assertFormat("5", "m", "2017-03-31T14:05:00.000Z");
        assertFormat("59", "m", "2017-03-31T12:59:00.000Z");
    }

    @Test
    public void testFormatMonthName() throws Exception {
        assertFormat("09, April", "dd, MMMM", "2017-04-09T00:00:00.000Z");
        assertFormat("09, December", "dd, MMMM", "2017-12-09T00:00:00.000Z");
        assertFormat("09, January", "dd, MMMM", "2017-01-09T00:00:00.000Z");

        assertFormat("April", "MMMM", "2017-04-09T00:00:00.000Z");
        assertFormat("December", "MMMM", "2017-12-09T00:00:00.000Z");
        assertFormat("January", "MMMM", "2017-01-09T00:00:00.000Z");
    }

    @Test
    public void testFormatMonthOneDigit() throws Exception {
        assertFormat("09, 4", "dd, M", "2017-04-09T00:00:00.000Z");
        assertFormat("09, 12", "dd, M", "2017-12-09T00:00:00.000Z");
        assertFormat("09, 1", "dd, M", "2017-01-09T00:00:00.000Z");

        assertFormat("4", "M", "2017-04-09T00:00:00.000Z");
        assertFormat("12", "M", "2017-12-09T00:00:00.000Z");
        assertFormat("1", "M", "2017-01-09T00:00:00.000Z");
    }

    @Test
    public void testFormatMonthTwoDigits() throws Exception {
        assertFormat("09, 04", "dd, MM", "2017-04-09T00:00:00.000Z");
        assertFormat("09, 12", "dd, MM", "2017-12-09T00:00:00.000Z");
        assertFormat("09, 01", "dd, MM", "2017-01-09T00:00:00.000Z");

        assertFormat("04", "MM", "2017-04-09T00:00:00.000Z");
        assertFormat("12", "MM", "2017-12-09T00:00:00.000Z");
        assertFormat("01", "MM", "2017-01-09T00:00:00.000Z");
    }

    @Test
    public void testFormatNano9Five() throws NumericException {
        assertFormat("2022-02-02 02:02:02.074812", "y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.074812Z");
    }

    @Test
    public void testFormatNano9Four() throws NumericException {
        assertFormat("2022-02-02 02:02:02.004812", "y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.004812Z");
    }

    @Test
    public void testFormatNano9One() throws NumericException {
        assertFormat("2022-02-02 02:02:02.000002", "y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.000002Z");
    }

    @Test
    public void testFormatNano9Six() throws NumericException {
        assertFormat("2022-02-02 02:02:02.374812", "y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.374812Z");
    }

    @Test
    public void testFormatNano9Three() throws NumericException {
        assertFormat("2022-02-02 02:02:02.000812", "y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.000812Z");
    }

    @Test
    public void testFormatNano9Two() throws NumericException {
        assertFormat("2022-02-02 02:02:02.000012", "y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.000012Z");
    }

    @Test
    public void testFormatNanoOneDigits() throws Exception {
        // we do not store nanos
        assertFormat("09, 017 0", "dd, yyy N", "2017-04-09T00:00:00.333123Z");
        // in this format N - nanos should not be greedy
        assertMicros("yyyy-MM-dd HH:mm:ss.NSSS", "2014-04-03T04:32:49.010000Z", "2014-04-03 04:32:49.1010");
    }

    @Test
    public void testFormatNanoThreeDigits() throws Exception {
        // we do not store nanos
        assertFormat("09, 017 000", "dd, yyy NNN", "2017-04-09T00:00:00.333123Z");
        // in this format N - nanos should not be greedy
        assertMicros("yyyy-MM-dd HH:mm:ss.SSSNNN", "2014-04-03T04:32:49.010000Z", "2014-04-03 04:32:49.010123");
    }

    @Test
    public void testFormatSecond() throws Exception {
        assertFormat("05", "ss", "2017-03-31T14:00:05.000Z");
        assertFormat("59", "ss", "2017-03-31T12:00:59.000Z");
    }

    @Test
    public void testFormatSecondOneDigit() throws Exception {
        assertFormat("3", "s", "2017-03-31T14:05:03.000Z");
        assertFormat("45", "s", "2017-03-31T12:59:45.000Z");
    }

    @Test
    public void testFormatShortDay() throws Exception {
        assertFormat("3", "d", "2014-04-03T00:00:00.000Z");
    }

    @Test
    public void testFormatShortMonthName() throws Exception {
        assertFormat("09, Apr", "dd, MMM", "2017-04-09T00:00:00.000Z");
        assertFormat("09, Dec", "dd, MMM", "2017-12-09T00:00:00.000Z");
        assertFormat("09, Jan", "dd, MMM", "2017-01-09T00:00:00.000Z");

        assertFormat("Apr", "MMM", "2017-04-09T00:00:00.000Z");
        assertFormat("Dec", "MMM", "2017-12-09T00:00:00.000Z");
        assertFormat("Jan", "MMM", "2017-01-09T00:00:00.000Z");
    }

    @Test
    public void testFormatShortWeekday() throws Exception {
        assertFormat("09, Sun", "dd, E", "2017-04-09T00:00:00.000Z");
        assertFormat("10, Mon", "dd, E", "2017-04-10T00:00:00.000Z");
        assertFormat("11, Tue", "dd, E", "2017-04-11T00:00:00.000Z");
        assertFormat("12, Wed", "dd, E", "2017-04-12T00:00:00.000Z");
        assertFormat("13, Thu", "dd, E", "2017-04-13T00:00:00.000Z");
        assertFormat("14, Fri", "dd, E", "2017-04-14T00:00:00.000Z");
        assertFormat("15, Sat", "dd, E", "2017-04-15T00:00:00.000Z");
    }

    @Test
    public void testFormatTimezone() throws Exception {
        assertFormat("GMT", "z", "2014-04-03T00:00:00.000Z");
    }

    @Test
    public void testFormatWeekOfYear() throws Exception {
        assertFormat("1", "w", "2010-01-01T00:00:00.000Z");
        assertFormat("10", "w", "2010-03-10T00:00:00.000Z");
        assertFormat("11", "w", "2020-03-10T00:00:00.000Z");
    }

    @Test
    public void testFormatWeekday() throws Exception {
        assertFormat("09, Sunday", "dd, EE", "2017-04-09T00:00:00.000Z");
        assertFormat("10, Monday", "dd, EE", "2017-04-10T00:00:00.000Z");
        assertFormat("11, Tuesday", "dd, EE", "2017-04-11T00:00:00.000Z");
        assertFormat("12, Wednesday", "dd, EE", "2017-04-12T00:00:00.000Z");
        assertFormat("13, Thursday", "dd, EE", "2017-04-13T00:00:00.000Z");
        assertFormat("14, Friday", "dd, EE", "2017-04-14T00:00:00.000Z");
        assertFormat("15, Saturday", "dd, EE", "2017-04-15T00:00:00.000Z");
    }

    @Test
    public void testFormatWeekdayDigit() throws Exception {
        assertFormat("09, 1", "dd, u", "2017-04-09T00:00:00.000Z");
        assertFormat("10, 2", "dd, u", "2017-04-10T00:00:00.000Z");
        assertFormat("11, 3", "dd, u", "2017-04-11T00:00:00.000Z");
        assertFormat("12, 4", "dd, u", "2017-04-12T00:00:00.000Z");
        assertFormat("13, 5", "dd, u", "2017-04-13T00:00:00.000Z");
        assertFormat("14, 6", "dd, u", "2017-04-14T00:00:00.000Z");
        assertFormat("15, 7", "dd, u", "2017-04-15T00:00:00.000Z");
    }

    @Test
    public void testFormatYearFourDigits() throws Exception {
        assertFormat("09, 2017", "dd, yyyy", "2017-04-09T00:00:00.000Z");
        assertFormat("2017", "yyyy", "2017-04-09T00:00:00.000Z");

        assertFormat("09, 0007", "dd, yyyy", "0007-04-09T00:00:00.000Z");
        assertFormat("0007", "yyyy", "0007-04-09T00:00:00.000Z");
    }

    @Test
    public void testFormatYearIsoFourDigits() throws Exception {
        assertFormat("53, 2020", "ww, YYYY", "2021-01-02T00:00:00.000Z");
        assertFormat("2020", "YYYY", "2021-01-02T00:00:00.000Z");

        assertFormat("01, 1970", "ww, YYYY", "1970-01-01T00:00:00.000Z");
        assertFormat("1970", "YYYY", "1970-01-01T00:00:00.000Z");
    }

    @Test
    public void testFormatYearOneDigit() throws Exception {
        assertFormat("09, 2017", "dd, y", "2017-04-09T00:00:00.000Z");
        assertFormat("2017", "y", "2017-04-09T00:00:00.000Z");

        assertFormat("09, 7", "dd, y", "0007-04-09T00:00:00.000Z");
        assertFormat("7", "y", "0007-04-09T00:00:00.000Z");
    }

    @Test
    public void testFormatYearThreeDigits() throws Exception {
        assertFormat("09, 017", "dd, yyy", "2017-04-09T00:00:00.000Z");
        assertFormat("017", "yyy", "2017-04-09T00:00:00.000Z");

        assertFormat("09, 777", "dd, yyy", "0777-04-09T00:00:00.000Z");
        assertFormat("099", "yyy", "0099-04-09T00:00:00.000Z");
    }

    @Test
    public void testFormatYearTwoDigits() throws Exception {
        assertFormat("09, 17", "dd, yy", "2017-04-09T00:00:00.000Z");
        assertFormat("17", "yy", "2017-04-09T00:00:00.000Z");

        assertFormat("09, 07", "dd, yy", "0007-04-09T00:00:00.000Z");
        assertFormat("07", "yy", "0007-04-09T00:00:00.000Z");
    }

    @Test
    public void testFormatZeroYear() throws Exception {
        assertFormat("09, 1", "dd, y", "0000-04-09T00:00:00.000Z");
        assertFormat("09, 01", "dd, yy", "0000-04-09T00:00:00.000Z");
        assertFormat("09, 001", "dd, yyy", "0000-04-09T00:00:00.000Z");
        assertFormat("09, 0001", "dd, yyyy", "0000-04-09T00:00:00.000Z");
    }

    @Test
    public void testGreedyMillis() throws NumericException {
        assertThat("y-MM-dd HH:mm:ss.Sz", "2014-04-03T04:32:49.010Z", "2014-04-03 04:32:49.01Z");
    }

    @Test
    public void testGreedyYear() throws Exception {
        assertThat("y-MM", "1564-03-01T00:00:00.000Z", "1564-03");
        assertThat("y-MM", "2036-03-01T00:00:00.000Z", "36-03");
        assertThat("y-MM", "2015-03-01T00:00:00.000Z", "15-03");
        assertThat("y-MM", "0137-03-01T00:00:00.000Z", "137-03");
    }

    @Test
    public void testGreedyYear2() throws Exception {
        long referenceYear = TimestampFormatUtils.getReferenceYear();
        try {
            TimestampFormatUtils.updateReferenceYear(Timestamps.toMicros(2015, 1, 20, 0, 0));
            assertThat("y-MM", "1564-03-01T00:00:00.000Z", "1564-03");
            assertThat("y-MM", "2006-03-01T00:00:00.000Z", "06-03");
            assertThat("y-MM", "2055-03-01T00:00:00.000Z", "55-03");
            assertThat("y-MM", "0137-03-01T00:00:00.000Z", "137-03");
        } finally {
            TimestampFormatUtils.updateReferenceYear(referenceYear);
        }
    }

    @Test(expected = NumericException.class)
    public void testHour12BadAM() throws Exception {
        assertThat("K MMy a", "2010-09-01T04:00:00.000Z", "13 0910 am");
    }

    @Test(expected = NumericException.class)
    public void testHour12BadPM() throws Exception {
        assertThat("K MMy a", "2010-09-01T04:00:00.000Z", "13 0910 pm");
    }

    @Test
    public void testHour12Greedy() throws Exception {
        assertThat("K MMy a", "2010-09-01T23:00:00.000Z", "11 0910 pm");
        assertThat("KaMMy", "2010-09-01T23:00:00.000Z", "11pm0910");
    }

    @Test
    public void testHour12GreedyOneBased() throws Exception {
        assertThat("h MMy a", "2010-09-01T23:00:00.000Z", "11 0910 pm");
        assertThat("haMMy", "2010-09-01T23:00:00.000Z", "11pm0910");
    }

    @Test
    public void testHour12OneDigit() throws Exception {
        assertThat("KMMy a", "2010-09-01T04:00:00.000Z", "40910 am");
        assertThat("KMMy a", "2010-09-01T16:00:00.000Z", "40910 pm");
    }

    @Test
    public void testHour12OneDigitDefaultAM() throws Exception {
        assertThat("KMMy", "2010-09-01T04:00:00.000Z", "40910");
    }

    @Test
    public void testHour12OneDigitOneBased() throws Exception {
        assertThat("hMMy a", "2010-09-01T04:00:00.000Z", "40910 am");
        assertThat("hMMy a", "2010-09-01T16:00:00.000Z", "40910 pm");
    }

    @Test
    public void testHour12TwoDigits() throws Exception {
        assertThat("KKMMy a", "2010-09-01T04:00:00.000Z", "040910 am");
        assertThat("KKMMy a", "2010-09-01T23:00:00.000Z", "110910 pm");
    }

    @Test
    public void testHour12TwoDigitsOneBased() throws Exception {
        assertThat("hhMMy a", "2010-09-01T03:00:00.000Z", "030910 am");
        assertThat("hhMMy a", "2010-09-01T23:00:00.000Z", "110910 pm");
    }

    @Test
    public void testHour24Greedy() throws Exception {
        assertThat("H, dd-MM", "1970-11-04T03:00:00.000Z", "3, 04-11");
        assertThat("H, dd-MM", "1970-11-04T19:00:00.000Z", "19, 04-11");

        assertThat("k, dd-MM", "1970-11-04T03:00:00.000Z", "3, 04-11");
        assertThat("k, dd-MM", "1970-11-04T19:00:00.000Z", "19, 04-11");
        assertThat("H, dd-MM-yyyy", "2012-11-04T19:00:00.000Z", "19, 04-11-2012");
    }

    @Test
    public void testHour24OneDigit() throws Exception {
        assertThat("HMMy", "2010-09-01T04:00:00.000Z", "40910");
        assertThat("kMMy", "2010-09-01T04:00:00.000Z", "40910");
        assertThat("Hmm MM-yyyy", "2010-09-01T04:09:00.000Z", "409 09-2010");
    }

    @Test
    public void testHour24TwoDigits() throws Exception {
        assertThat("HHMMy", "2010-09-01T04:00:00.000Z", "040910");
        assertThat("HHMMy", "2010-09-01T23:00:00.000Z", "230910");

        assertThat("kkMMy", "2010-09-01T04:00:00.000Z", "040910");
        assertThat("kkMMy", "2010-09-01T23:00:00.000Z", "230910");
    }

    @Test
    public void testHttpFormat() throws Exception {
        assertThat("E, dd MMM yyyy HH:mm:ss", "2017-04-05T14:55:10.000Z", "Mon, 05 Apr 2017 14:55:10");
    }

    @Test
    public void testIgnoredNanos() throws Exception {
        assertThat("E, dd-MM-yyyy N", "2014-04-03T00:00:00.000Z", "Fri, 03-04-2014 234");
        assertThat("EE, dd-MM-yyyy N", "2014-04-03T00:00:00.000Z", "Fri, 03-04-2014 234");
    }

    @Test
    public void testIsoWeekOfYear() throws Exception {
        assertThat("ww, YYYY", "2010-02-08T00:00:00.000Z", "06, 2010");
    }

    @Test
    public void testIsoYear() throws Exception {
        assertThat("YYYY", "2010-01-01T00:00:00.000Z", "2010");
    }

    @Test
    public void testLeapYear() throws Exception {
        assertThat("dd-MM-yyyy", "2016-02-29T00:00:00.000Z", "29-02-2016");
    }

    @Test(expected = NumericException.class)
    public void testLeapYearFailure() throws Exception {
        assertThat("dd-MM-yyyy", "", "29-02-2015");
    }

    @Test(expected = BytecodeException.class)
    public void testLongPattern() {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            b.append("KK").append(' ').append('Z').append(',');
        }
        compiler.compile(b);
    }

    @Test
    public void testMicrosGreedy() throws Exception {
        assertMicros("y-MM-dd HH:mm:ss.SSSUz", "2014-04-03T04:32:49.010330Z", "2014-04-03 04:32:49.01033Z");
        assertMicros("yyyy U", "2017-01-01T00:00:00.000550Z", "2017 55");
        assertMicros("U dd-MM-yyyy", "2014-10-03T00:00:00.000314Z", "314 03-10-2014");
    }

    @Test
    public void testMicrosOneDigit() throws Exception {
        assertMicros("mmUHH MMy", "2010-09-01T13:55:00.000002Z", "55213 0910");
        assertMicros("UHH dd-MM-yyyy", "2014-10-03T14:00:00.000003Z", "314 03-10-2014");
    }

    @Test
    public void testMicrosThreeDigit() throws Exception {
        assertMicros("mmUUUHH MMy", "2010-09-01T13:55:00.000015Z", "5501513 0910");
    }

    @Test
    public void testMillisGreedy() throws Exception {
        assertThat("ddMMy HH:mm:ss.S", "2078-03-19T21:20:45.678Z", "190378 21:20:45.678");
    }

    @Test(expected = NumericException.class)
    public void testMillisGreedyShort() throws Exception {
        assertThat("ddMMy HH:mm:ss.SSS", "1978-03-19T21:20:45.678Z", "190378 21:20:45.");
    }

    @Test
    public void testMillisOneDigit() throws Exception {
        assertThat("mmsSHH MMy", "2010-09-01T13:55:03.002Z", "553213 0910");
        assertMicros("SHH dd-MM-yyyy", "2014-10-03T14:00:00.003000Z", "314 03-10-2014");
    }

    @Test
    public void testMillisThreeDigits() throws Exception {
        assertThat("ddMMy HH:mm:ss.SSS", "2078-03-19T21:20:45.678Z", "190378 21:20:45.678");
    }

    @Test
    public void testMinuteGreedy() throws Exception {
        assertThat("dd-MM-yy HH:m", "2010-09-03T14:54:00.000Z", "03-09-10 14:54");
    }

    @Test
    public void testMinuteOneDigit() throws Exception {
        assertThat("mHH MMy", "2010-09-01T13:05:00.000Z", "513 0910");
    }

    @Test
    public void testMinuteTwoDigits() throws Exception {
        assertThat("mm:HH MMy", "2010-09-01T13:45:00.000Z", "45:13 0910");
    }

    @Test
    public void testMonthGreedy() throws Exception {
        assertThat("M-y", "2012-11-01T00:00:00.000Z", "11-12");
        assertThat("M-y", "2012-02-01T00:00:00.000Z", "2-12");
    }

    @Test
    public void testMonthNameAndThreeDigitYear() throws Exception {
        assertThat("dd-MMM-y", "2012-11-15T00:00:00.000Z", "15-NOV-12");
        assertThat("dd MMMM yyy", "0213-09-18T00:00:00.000Z", "18 September 213");
    }

    @Test
    public void testMonthOneDigit() throws Exception {
        assertThat("My", "2010-04-01T00:00:00.000Z", "410");
    }

    @Test
    public void testNegativeYear() throws Exception {
        assertThat("yyyy MMM dd", "-2010-08-01T00:00:00.000Z", "-2010 Aug 01");

        DateFormat fmt1 = compiler.compile("G yyyy MMM", true);
        DateFormat fmt2 = compiler.compile("yyyy MMM dd", true);

        long millis = fmt2.parse("-2010 Aug 01", defaultLocale);
        sink.clear();
        fmt1.format(millis, defaultLocale, "Z", sink);
        TestUtils.assertEquals("BC -2010 Aug", sink);
    }

    @Test
    public void testOperationUniqueness() {

        Assert.assertTrue(TimestampFormatCompiler.getOpCount() > 0);

        IntHashSet codeSet = new IntHashSet();
        CharSequenceHashSet nameSet = new CharSequenceHashSet();
        for (int i = 0, n = TimestampFormatCompiler.getOpCount(); i < n; i++) {
            String name = TimestampFormatCompiler.getOpName(i);
            int code = TimestampFormatCompiler.getOpCode(name);
            Assert.assertTrue(codeSet.add(code));
            Assert.assertTrue(nameSet.add(name));
        }
    }

    @Test
    public void testParseMicros6Mid() throws Exception {
        assertMicros("y-MM-dd HH:mm:ss.U+", "2022-02-02T02:02:02.123000Z", "2022-02-02 02:02:02.123");
    }

    @Test
    public void testParseMicros6Mid2() throws Exception {
        assertMicros("y-MM-dd HH:mm:ss.U+", "2022-02-02T02:02:02.123700Z", "2022-02-02 02:02:02.1237");
    }

    @Test
    public void testParseMicros6Small() throws Exception {
        assertMicros("y-MM-dd HH:mm:ss.U+", "2022-02-02T02:02:02.100000Z", "2022-02-02 02:02:02.1");
    }

    @Test
    public void testParseNanos9Eight() throws Exception {
        assertMicros("y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.123456Z", "2022-02-02 02:02:02.12345678");
    }

    @Test
    public void testParseNanos9Five() throws Exception {
        assertMicros("y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.123450Z", "2022-02-02 02:02:02.12345");
    }

    @Test
    public void testParseNanos9Four() throws Exception {
        assertMicros("y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.123400Z", "2022-02-02 02:02:02.1234");
    }

    @Test
    public void testParseNanos9Nine() throws Exception {
        assertMicros("y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.123456Z", "2022-02-02 02:02:02.123456789");
    }

    @Test
    public void testParseNanos9One() throws Exception {
        assertMicros("y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.100000Z", "2022-02-02 02:02:02.1");
    }

    @Test
    public void testParseNanos9Seven() throws Exception {
        assertMicros("y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.123456Z", "2022-02-02 02:02:02.1234567");
    }

    @Test
    public void testParseNanos9Six() throws Exception {
        assertMicros("y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.123456Z", "2022-02-02 02:02:02.123456");
    }

    @Test(expected = NumericException.class)
    public void testParseNanos9Ten() throws Exception {
        assertMicros("y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.123456Z", "2022-02-02 02:02:02.1234567891");
    }

    @Test
    public void testParseNanos9Three() throws Exception {
        assertMicros("y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.123000Z", "2022-02-02 02:02:02.123");
    }

    @Test
    public void testParseNanos9Two() throws Exception {
        assertMicros("y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.120000Z", "2022-02-02 02:02:02.12");
    }

    @Test
    public void testParseUtc() throws Exception {
        assertThat(TimestampFormatUtils.UTC_PATTERN, "2011-10-03T00:00:00.000Z", "2011-10-03T00:00:00.000Z");
    }

    @Test
    public void testQuote() throws Exception {
        assertThat("yyyy'y'ddMM", "2010-03-10T00:00:00.000Z", "2010y1003");
    }

    @Test(expected = NumericException.class)
    public void testRandomFormat() throws Exception {
        assertThat("Ketchup", "", "2021-11-19T14:00:00.000Z");
    }

    @Test
    public void testSecondGreedy() throws Exception {
        assertThat("ddMMy HH:mm:s", "2078-03-19T21:20:45.000Z", "190378 21:20:45");
    }

    @Test
    public void testSecondOneDigit() throws Exception {
        assertThat("mmsHH MMy", "2010-09-01T13:55:03.000Z", "55313 0910");
    }

    @Test
    public void testSecondTwoDigits() throws Exception {
        assertThat("ddMMy HH:mm:ss", "2078-03-19T21:20:45.000Z", "190378 21:20:45");
    }

    @Test
    public void testSingleDigitYear() throws Exception {
        assertThat("yMM", "0001-03-01T00:00:00.000Z", "103");
    }

    @Test
    public void testThreeDigitYear() throws Exception {
        assertThat("u, dd-MM-yyy", "0999-04-03T00:00:00.000Z", "5, 03-04-999");
    }

    @Test
    public void testTimeZone1() throws Exception {
        assertThat("dd-MM-yy HH:m z", "2010-09-03T11:54:00.000Z", "03-09-10 14:54 EAT");
    }

    @Test
    public void testTimeZone2() throws Exception {
        assertThat("dd-MM-yyyy HH:m z", "2015-09-03T18:50:00.000Z", "03-09-2015 21:50 EET");
    }

    @Test
    public void testTimeZone3() throws Exception {
        assertThat("dd-MM-yy HH:m z", "2010-09-03T20:50:00.000Z", "03-09-10 21:50 BST");
    }

    @Test
    public void testTimeZone4() throws Exception {
        assertThat("dd-MM-yy HH:m z", "2010-09-03T21:01:00.000Z", "03-09-10 23:01 Hora de verano de Sudáfrica", "es-PA");
    }

    @Test
    public void testTimeZone5() throws Exception {
        assertThat("dd-MM-yy HH:m [z]", "2010-09-03T21:01:00.000Z", "03-09-10 23:01 [Hora de verano de Sudáfrica]", "es-PA");
    }

    @Test
    public void testTimeZone6() throws Exception {
        assertThat("dd-MM-yy HH:m z", "2010-09-03T17:35:00.000Z", "03-09-10 21:50 +04:15");
    }

    @Test
    public void testTimeZone7() throws Exception {
        assertThat("dd-MM-yy HH:m z", "2010-09-04T05:50:00.000Z", "03-09-10 21:50 UTC-08:00");
    }

    @Test
    public void testTimeZone8() throws Exception {
        assertThat("dd-MM-yy HH:m z", "2010-09-04T07:50:00.000Z", "03-09-10 21:50 -10");
    }

    @Test(expected = NumericException.class)
    public void testTooLongInput() throws Exception {
        assertThat("E, dd-MM-yyyy G", "2014-04-03T00:00:00.000Z", "Tuesday, 03-04-2014 ADD");
    }

    @Test
    public void testTwoDigitYear() throws Exception {
        assertThat("MMyy", "2010-11-01T00:00:00.000Z", "1110");
        assertThat("MM, yy", "2010-11-01T00:00:00.000Z", "11, 10");
    }

    @Test
    public void testWeekOfYear() throws Exception {
        assertThat("w, MM-yyyy", "2010-11-01T00:00:00.000Z", "6, 11-2010");
    }

    @Test
    public void testWeekdayDigit() throws Exception {
        assertThat("u, dd-MM-yyyy", "2014-04-03T00:00:00.000Z", "5, 03-04-2014");
    }

    @Test(expected = NumericException.class)
    public void testWeekdayIncomplete() throws Exception {
        assertThat("E, dd-MM-yyyy", "2014-04-03T00:00:00.000Z", "Tu, 03-04-2014");
    }

    @Test(expected = NumericException.class)
    public void testWeekdayIncomplete2() throws Exception {
        assertThat("dd-MM-yyyy, E", "2014-04-03T00:00:00.000Z", "03-04-2014, Fr");
    }

    @Test
    public void testWeekdayLong() throws Exception {
        assertThat("E, dd-MM-yyyy", "2014-04-03T00:00:00.000Z", "Tuesday, 03-04-2014");
        assertThat("EE, dd-MM-yyyy", "2014-04-03T00:00:00.000Z", "Tuesday, 03-04-2014");
    }

    @Test
    public void testWeekdayShort() throws Exception {
        assertThat("E, dd-MM-yyyy", "2014-04-03T00:00:00.000Z", "Fri, 03-04-2014");
        assertThat("EE, dd-MM-yyyy", "2014-04-03T00:00:00.000Z", "Fri, 03-04-2014");
    }

    private static DateFormat get(CharSequence pattern) {
        return compiler.compile(pattern, true);
    }

    private void assertFormat(String expected, String pattern, String date) throws NumericException {
        assertFormat(expected, pattern, date, 0);
    }

    private void assertFormat(String expected, String pattern, String date, int mic) throws NumericException {
        sink.clear();
        long micros = TimestampFormatUtils.parseTimestamp(date) + mic;
        get(pattern).format(micros, defaultLocale, "GMT", sink);
        TestUtils.assertEqualsIgnoreCase(expected, sink);
        sink.clear();
        compiler.compile(pattern, false).format(micros, defaultLocale, "GMT", sink);
        TestUtils.assertEqualsIgnoreCase(expected, sink);
    }

    private void assertMicros(String pattern, String expected, String input) throws NumericException {
        sink.clear();
        REFERENCE.format(get(pattern).parse(input, defaultLocale), defaultLocale, "Z", sink);
        TestUtils.assertEquals(expected, sink);

        sink.clear();
        REFERENCE.format(compiler.compile(pattern).parse(input, defaultLocale), defaultLocale, "Z", sink);
    }

    private void assertThat(String pattern, String expected, String input, CharSequence localeId) throws NumericException {
        assertThat(pattern, expected, input, DateLocaleFactory.INSTANCE.getLocale(localeId));
    }

    private void assertThat(String pattern, String expected, String input) throws NumericException {
        assertThat(pattern, expected, input, defaultLocale);
    }

    private void assertThat(String pattern, String expected, String input, DateLocale locale) throws NumericException {
        DateFormat format = get(pattern);
        TestUtils.assertEquals(expected, Timestamps.toString(format.parse(input, locale)));

        DateFormat compiled = compiler.compile(pattern);
        TestUtils.assertEquals(expected, Timestamps.toString(compiled.parse(input, locale)));
    }

    private void testAgainstJavaReferenceImpl(String pattern) throws Exception {
        SimpleDateFormat javaFmt = new SimpleDateFormat(pattern, Locale.UK);
        javaFmt.setTimeZone(java.util.TimeZone.getTimeZone("UTC"));
        DateFormat genericQuestFmt = get(pattern);
        DateFormat compiledQuestFmt = compiler.compile(pattern);

        long step = TimeUnit.MINUTES.toMicros(15);
        for (long tsMicros = 0, n = TimeUnit.DAYS.toMicros(1); tsMicros < n; tsMicros += step) {
            sink.clear();
            long tsMillis = tsMicros / 1000;
            String javaFormatted = javaFmt.format(new Date(tsMillis));

            genericQuestFmt.format(tsMicros, defaultLocale, "UTC", sink);
            Assert.assertEquals(javaFormatted, sink.toString());

            sink.clear();
            compiledQuestFmt.format(tsMicros, defaultLocale, "UTC", sink);
            Assert.assertEquals(javaFormatted, sink.toString());

            // now we know both Java and QuestDB format the same way.
            // let's try to parse it back.
            Assert.assertEquals(tsMicros, genericQuestFmt.parse(sink, defaultLocale));
            Assert.assertEquals(tsMicros, compiledQuestFmt.parse(sink, defaultLocale));

            // sanity check
            Assert.assertEquals(tsMillis, javaFmt.parse(sink.toString()).getTime());
        }
    }
}
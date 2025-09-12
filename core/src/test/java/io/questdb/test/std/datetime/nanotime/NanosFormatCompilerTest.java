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

package io.questdb.test.std.datetime.nanotime;

import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.IntHashSet;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.microtime.MicrosFormatCompiler;
import io.questdb.std.datetime.nanotime.Nanos;
import io.questdb.std.datetime.nanotime.NanosFormatCompiler;
import io.questdb.std.datetime.nanotime.NanosFormatUtils;
import io.questdb.std.ex.BytecodeException;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static io.questdb.std.datetime.TimeZoneRuleFactory.RESOLUTION_NANOS;

public class NanosFormatCompilerTest {

    private final static DateFormat REFERENCE = NanosFormatUtils.NSEC_UTC_FORMAT;
    private static final NanosFormatCompiler compiler = new NanosFormatCompiler();
    private final static DateLocale defaultLocale = DateLocaleFactory.INSTANCE.getLocale("en-GB");
    private final static StringSink sink = new StringSink();

    @BeforeClass
    public static void setUp() {
        NanosFormatUtils.updateReferenceYear(Nanos.toNanos(1997, 1, 1, 0, 0));
    }

    @Test
    public void test12HourSystemsOneBase() throws ParseException {
        testAgainstJavaReferenceImpl("hh:mm a");
    }

    @Test
    public void test12HourSystemsZeroBase() throws ParseException {
        testAgainstJavaReferenceImpl("KK:mm a");
    }

    @Test
    public void test24HourSystemsOneBase() throws ParseException {
        testAgainstJavaReferenceImpl("kk:mm");
    }

    @Test
    public void test24HourSystemsZeroBase() throws ParseException {
        testAgainstJavaReferenceImpl("HH:mm");
    }

    @Test(expected = NumericException.class)
    public void testBadAmPm() {
        assertThat("KaMMy", "", "11 0910 am");
    }

    @Test(expected = NumericException.class)
    public void testBadAmPm2() {
        assertThat("KaMMy", "", "11az0910");
    }

    @Test(expected = NumericException.class)
    public void testBadDelimiter() {
        assertThat("y.MM", "0001-03-01T00:00:00.000Z", "1-03");
    }

    @Test(expected = NumericException.class)
    public void testBadMonth() {
        assertThat("yMM", "0001-03-01T00:00:00.000Z", "133");
    }

    @Test
    public void testBasicParserCompiler() {
        DateFormat fmt = compiler.compile("E, dd MMM yyyy a KK:m:s.S Z");
        String utcPattern = "yyyy-MM-ddTHH:mm:ss.SSSz";
        DateFormat utc = compiler.compile(utcPattern);
        long millis = fmt.parse("Mon, 08 Apr 2017 PM 11:11:10.123 UTC", DateLocaleFactory.EN_LOCALE);
        sink.clear();
        utc.format(millis, DateLocaleFactory.EN_LOCALE, "Z", sink);
        TestUtils.assertEquals("2017-04-08T23:11:10.123Z", sink);
    }

    @Test
    public void testDayGreedy() {
        assertThat("d, MM-yy", "2011-10-03T00:00:00.000000000Z", "3, 10-11");
        assertThat("d, MM-yy", "2011-10-03T00:00:00.000000000Z", "03, 10-11");
        assertThat("d, MM-yy", "2011-10-25T00:00:00.000000000Z", "25, 10-11");
    }

    @Test
    public void testDayMonthYear() {
        assertThat("dd-MM-yyyy", "2010-03-10T00:00:00.000000000Z", "10-03-2010");
    }

    @Test
    public void testDayMonthYearNoDelim() {
        assertThat("yyyyddMM", "2010-03-10T00:00:00.000000000Z", "20101003");
    }

    @Test
    public void testDayOfYear() {
        assertThat("D, MM-yyyy", "2010-11-01T00:00:00.000000000Z", "25, 11-2010");
    }

    @Test
    public void testDayOneDigit() {
        assertThat("dyyyy", "2014-01-03T00:00:00.000000000Z", "32014");
    }

    @Test
    public void testEra() {
        assertThat("E, dd-MM-yyyy G", "2014-04-03T00:00:00.000000000Z", "Tuesday, 03-04-2014 AD");
    }

    @Test
    public void testEraBCUnsupported() {
        assertException("E, dd-MM-yyyy G", "Tuesday, 03-04-2014 BC");
    }

    @Test
    public void testFirstHourIn12HourClock() {
        assertThat("MM/dd/yyyy hh:mm:ss a", "2017-04-09T00:01:00.000000000Z", "04/09/2017 12:01:00 am");
    }

    @Test
    public void testFormatAMPM() {
        assertFormat("pm, 31", "a, dd", "2017-03-31T14:00:00.000000000Z");
        assertFormat("pm, 31", "a, dd", "2017-03-31T12:00:00.000000000Z");
        assertFormat("am, 31", "a, dd", "2017-03-31T00:00:00.000000000Z");
        assertFormat("am, 31", "a, dd", "2017-03-31T11:59:59.999999999Z");
    }

    @Test
    public void testFormatBSTtoMSK() {
        DateFormat fmt = get("dd-MM-yyyy HH:mm:ss Z");
        String targetTimezoneName = "MSK";
        long nanos = fmt.parse("06-04-2017 01:09:30 BST", defaultLocale);
        nanos += defaultLocale.getRules(targetTimezoneName, RESOLUTION_NANOS).getOffset(nanos);
        sink.clear();
        fmt.format(nanos, defaultLocale, targetTimezoneName, sink);
        TestUtils.assertEquals("06-04-2017 03:09:30 MSK", sink);
    }

    @Test
    public void testFormatDay() {
        assertFormat("03", "dd", "2014-04-03T00:00:00.000000000Z");
    }

    @Test
    public void testFormatDayOfYear() {
        assertFormat("1", "D", "2010-01-01T00:00:00.000000000Z");
        assertFormat("69", "D", "2010-03-10T00:00:00.000000000Z");
        assertFormat("70", "D", "2020-03-10T00:00:00.000000000Z");
    }

    @Test
    public void testFormatEra() {
        assertFormat("AD", "G", "2017-04-09T00:00:00.000000000Z");
    }

    @Test
    public void testFormatFirstHourIn12HourClock() {
        assertFormat("04/09/2017 00:01:00 am", "MM/dd/yyyy KK:mm:ss a", "2017-04-09T00:01:00.000000000Z");
        assertFormat("04/09/2017 00:59:59 am", "MM/dd/yyyy KK:mm:ss a", "2017-04-09T00:59:59.000000000Z");
    }

    @Test
    public void testFormatHour23() {
        assertFormat("pm, 14", "a, HH", "2017-03-31T14:00:00.000000000Z");
        assertFormat("pm, 12", "a, HH", "2017-03-31T12:00:00.000000000Z");
        assertFormat("am, 03", "a, HH", "2017-03-31T03:00:00.000000000Z");
        assertFormat("am, 11", "a, HH", "2017-03-31T11:59:59.999999999Z");

        assertFormat("14", "HH", "2017-03-31T14:00:00.000000000Z");
        assertFormat("12", "HH", "2017-03-31T12:00:00.000000000Z");
        assertFormat("03", "HH", "2017-03-31T03:00:00.000000000Z");
        assertFormat("11", "HH", "2017-03-31T11:59:59.999999999Z");
    }

    @Test
    public void testFormatHour23OneDigit() {
        // H = hour, 0-23
        assertFormat("pm, 14", "a, H", "2017-03-31T14:00:00.123456789Z");
        assertFormat("pm, 12", "a, H", "2017-03-31T12:00:00.987654321Z");
        assertFormat("am, 3", "a, H", "2017-03-31T03:00:00.456789123Z");
        assertFormat("am, 11", "a, H", "2017-03-31T11:59:59.999123456Z");

        assertFormat("14", "H", "2017-03-31T14:00:00.789456123Z");
        assertFormat("12", "H", "2017-03-31T12:00:00.123987456Z");
        assertFormat("3", "H", "2017-03-31T03:00:00.654321987Z");
        assertFormat("11", "H", "2017-03-31T11:59:59.999654321Z");
        assertFormat("0", "H", "2017-03-31T00:00:00.246813579Z");
    }

    @Test
    public void testFormatHour24() {
        // k = hour, 1-24
        assertFormat("pm, 14", "a, kk", "2017-03-31T14:00:00.123456789Z");
        assertFormat("pm, 12", "a, kk", "2017-03-31T12:00:00.987654321Z");
        assertFormat("am, 03", "a, kk", "2017-03-31T03:00:00.456789123Z");
        assertFormat("am, 11", "a, kk", "2017-03-31T11:59:59.999123456Z");

        assertFormat("14", "kk", "2017-03-31T14:00:00.789456123Z");
        assertFormat("12", "kk", "2017-03-31T12:00:00.123987456Z");
        assertFormat("03", "kk", "2017-03-31T03:00:00.654321987Z");
        assertFormat("11", "kk", "2017-03-31T11:59:59.999654321Z");
        assertFormat("24", "kk", "2017-03-31T00:00:00.246813579Z");
    }

    @Test
    public void testFormatHour24OneDigit() {
        // k = hour, 1-24
        assertFormat("pm, 14", "a, k", "2017-03-31T14:00:00.135792468Z");
        assertFormat("pm, 12", "a, k", "2017-03-31T12:00:00.975318642Z");
        assertFormat("am, 3", "a, k", "2017-03-31T03:00:00.864213579Z");
        assertFormat("am, 11", "a, k", "2017-03-31T11:59:59.999753159Z");

        assertFormat("14", "k", "2017-03-31T14:00:00.159357246Z");
        assertFormat("12", "k", "2017-03-31T12:00:00.357951246Z");
        assertFormat("3", "k", "2017-03-31T03:00:00.258741369Z");
        assertFormat("11", "k", "2017-03-31T11:59:59.999321654Z");
    }

    @Test
    public void testFormatHourTwelve() {
        // h = hour, 1-12
        assertFormat("pm, 02", "a, hh", "2017-03-31T14:00:00.741258963Z");
        assertFormat("pm, 12", "a, hh", "2017-03-31T12:00:00.852963147Z");
        assertFormat("am, 03", "a, hh", "2017-03-31T03:00:00.369258147Z");
        assertFormat("am, 11", "a, hh", "2017-03-31T11:59:59.999147258Z");

        assertFormat("02", "hh", "2017-03-31T14:00:00.963258741Z");
        assertFormat("12", "hh", "2017-03-31T12:00:00.258963147Z");
        assertFormat("03", "hh", "2017-03-31T03:00:00.741369258Z");
        assertFormat("11", "hh", "2017-03-31T11:59:59.999963852Z");
    }

    @Test
    public void testFormatHourTwelveOneDigit() {
        // h = hour, 1-12
        assertFormat("pm, 2", "a, h", "2017-03-31T14:00:00.147258369Z");
        assertFormat("pm, 12", "a, h", "2017-03-31T12:00:00.369147258Z");
        assertFormat("am, 3", "a, h", "2017-03-31T03:00:00.852741369Z");
        assertFormat("am, 11", "a, h", "2017-03-31T11:59:59.999852147Z");
        assertFormat("am, 12", "a, h", "2017-03-31T00:00:00.357852741Z");
        assertFormat("pm, 12", "a, h", "2017-03-31T12:00:00.654987321Z");

        assertFormat("2", "h", "2017-03-31T14:00:00.987456123Z");
        assertFormat("12", "h", "2017-03-31T12:00:00.123456987Z");
        assertFormat("3", "h", "2017-03-31T03:00:00.456123789Z");
        assertFormat("11", "h", "2017-03-31T11:59:59.999789456Z");
        assertFormat("12", "h", "2017-03-31T00:00:00.654321789Z");
    }

    @Test
    public void testFormatHourZeroEleven() {
        // K = hour, 0-11
        assertFormat("pm, 02", "a, KK", "2017-03-31T14:00:00.753159852Z");
        assertFormat("pm, 00", "a, KK", "2017-03-31T12:00:00.159753852Z");
        assertFormat("am, 03", "a, KK", "2017-03-31T03:00:00.357951246Z");
        assertFormat("am, 11", "a, KK", "2017-03-31T11:59:59.999456123Z");
        assertFormat("am, 00", "a, KK", "2017-03-31T00:00:00.258741963Z");
        assertFormat("pm, 00", "a, KK", "2017-03-31T12:00:00.147258963Z");

        assertFormat("02", "KK", "2017-03-31T14:00:00.963147258Z");
        assertFormat("00", "KK", "2017-03-31T12:00:00.369852741Z");
        assertFormat("00", "KK", "2017-03-31T00:00:00.741258369Z");
        assertFormat("03", "KK", "2017-03-31T03:00:00.852963741Z");
        assertFormat("11", "KK", "2017-03-31T11:59:59.999741963Z");
    }

    @Test
    public void testFormatHourZeroElevenOneDigit() {
        assertFormat("pm, 2", "a, K", "2017-03-31T14:00:00.147963258Z");
        assertFormat("pm, 0", "a, K", "2017-03-31T12:00:00.753852147Z");
        assertFormat("am, 3", "a, K", "2017-03-31T03:00:00.258369147Z");
        assertFormat("am, 11", "a, K", "2017-03-31T11:59:59.999369852Z");
        assertFormat("am, 0", "a, K", "2017-03-31T00:00:00.741963258Z");
        assertFormat("pm, 0", "a, K", "2017-03-31T12:00:00.852147369Z");

        assertFormat("2", "K", "2017-03-31T14:00:00.963741258Z");
        assertFormat("0", "K", "2017-03-31T12:00:00.147852963Z");
        assertFormat("3", "K", "2017-03-31T03:00:00.258147963Z");
        assertFormat("11", "K", "2017-03-31T11:59:59.999852369Z");
    }

    @Test
    public void testFormatISOWeek() throws NumericException {
        assertFormat("05", "ww", "2022-01-31T02:02:02.000012234Z");
        assertNanos("yyyy-ww HH:mm:ss.SSS U", "2022-01-31T02:02:02.001002000Z", "2022-05 02:02:02.001 002");
    }

    @Test
    public void testFormatIsoWeekOfYear() {
        assertFormat("53", "ww", "2010-01-01T00:00:00.123456789Z");
        assertFormat("10", "ww", "2010-03-10T00:00:00.987654321Z");
        assertFormat("11", "ww", "2020-03-10T00:00:00.456789123Z");
    }

    @Test
    public void testFormatMicros() {
        assertFormat("678-15", "S-U", "1978-03-19T21:20:45.678015023Z");
        assertFormat("678.702", "S.UUU", "1978-03-19T21:20:45.678702514Z");
        assertFormat("1978, .025", "yyyy, .UUU", "1978-03-19T21:20:45.678025901Z");
        assertFormat("1978, .25 678", "yyyy, .U SSS", "1978-03-19T21:20:45.678025091Z");
    }

    @Test
    public void testFormatMicros3Micros() throws NumericException {
        assertFormat("2022-02-02 02:02:02.000 2", "y-MM-dd HH:mm:ss.SSS U", "2022-02-02T02:02:02.000002123Z");
    }

    @Test
    public void testFormatMicros3Nanos() throws NumericException {
        assertFormat("2022-02-02 02:02:02.000 123", "y-MM-dd HH:mm:ss.SSS N", "2022-02-02T02:02:02.000002123Z");
    }

    @Test
    public void testFormatMillis() {
        assertFormat("033", "SSS", "2017-03-31T14:00:05.033123456Z");
        assertFormat("579", "SSS", "2017-03-31T12:00:59.579789123Z");
    }

    @Test
    public void testFormatMillisOneDigit() {
        assertFormat("15", "S", "2017-03-31T14:05:03.015456789Z");
        assertFormat("459", "S", "2017-03-31T12:59:45.459123456Z");
    }

    @Test
    public void testFormatMinute() {
        assertFormat("05", "mm", "2017-03-31T14:05:00.789456123Z");
        assertFormat("59", "mm", "2017-03-31T12:59:00.654321987Z");
    }

    @Test
    public void testFormatMinuteOneDigit() {
        assertFormat("5", "m", "2017-03-31T14:05:00.123987456Z");
        assertFormat("59", "m", "2017-03-31T12:59:00.246813579Z");
    }

    @Test
    public void testFormatMonthName() {
        assertFormat("09, April", "dd, MMMM", "2017-04-09T00:00:00.135792468Z");
        assertFormat("09, December", "dd, MMMM", "2017-12-09T00:00:00.975318642Z");
        assertFormat("09, January", "dd, MMMM", "2017-01-09T00:00:00.864213579Z");

        assertFormat("April", "MMMM", "2017-04-09T00:00:00.159357246Z");
        assertFormat("December", "MMMM", "2017-12-09T00:00:00.357951246Z");
        assertFormat("January", "MMMM", "2017-01-09T00:00:00.258741369Z");
    }

    @Test
    public void testFormatMonthOneDigit() {
        assertFormat("09, 4", "dd, M", "2017-04-09T00:00:00.741258963Z");
        assertFormat("09, 12", "dd, M", "2017-12-09T00:00:00.852963147Z");
        assertFormat("09, 1", "dd, M", "2017-01-09T00:00:00.369258147Z");

        assertFormat("4", "M", "2017-04-09T00:00:00.963258741Z");
        assertFormat("12", "M", "2017-12-09T00:00:00.258963147Z");
        assertFormat("1", "M", "2017-01-09T00:00:00.741369258Z");
    }

    @Test
    public void testFormatMonthTwoDigits() {
        assertFormat("09, 04", "dd, MM", "2017-04-09T00:00:00.147258369Z");
        assertFormat("09, 12", "dd, MM", "2017-12-09T00:00:00.369147258Z");
        assertFormat("09, 01", "dd, MM", "2017-01-09T00:00:00.852741369Z");

        assertFormat("04", "MM", "2017-04-09T00:00:00.987456123Z");
        assertFormat("12", "MM", "2017-12-09T00:00:00.123456987Z");
        assertFormat("01", "MM", "2017-01-09T00:00:00.456123789Z");
    }

    @Test
    public void testFormatNano9Five() throws NumericException {
        assertFormat("2022-02-02 02:02:02.074812123", "y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.074812123Z");
    }

    @Test
    public void testFormatNano9Four() throws NumericException {
        assertFormat("2022-02-02 02:02:02.004812000", "y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.004812000Z");
    }

    @Test
    public void testFormatNano9One() throws NumericException {
        assertFormat("2022-02-02 02:02:02.000002000", "y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.000002000Z");
    }

    @Test
    public void testFormatNano9Six() throws NumericException {
        assertFormat("2022-02-02 02:02:02.374812000", "y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.374812000Z");
    }

    @Test
    public void testFormatNano9Three() throws NumericException {
        assertFormat("2022-02-02 02:02:02.000812000", "y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.000812000Z");
    }

    @Test
    public void testFormatNano9Two() throws NumericException {
        assertFormat("2022-02-02 02:02:02.000012000", "y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.000012000Z");
    }

    @Test
    public void testFormatNanoOneDigits() {
        // we do not store nanos
        assertFormat("09, 017 556", "dd, yyy N", "2017-04-09T00:00:00.333123556Z");
        // in this format N - nanos should not be greedy
        assertNanos("yyyy-MM-dd HH:mm:ss.NSSS", "2014-04-03T04:32:49.010000001Z", "2014-04-03 04:32:49.1010");
    }

    @Test
    public void testFormatNanoThreeDigits() {
        // we do not store nanos
        assertFormat("09, 017 537", "dd, yyy NNN", "2017-04-09T00:00:00.333123537Z");
        // in this format N - nanos should not be greedy
        assertNanos("yyyy-MM-dd HH:mm:ss.SSSNNN", "2014-04-03T04:32:49.010000123Z", "2014-04-03 04:32:49.010123");
    }

    @Test
    public void testFormatSecond() {
        assertFormat("05", "ss", "2017-03-31T14:00:05.000000000Z");
        assertFormat("59", "ss", "2017-03-31T12:00:59.000000000Z");
    }

    @Test
    public void testFormatSecondOneDigit() {
        assertFormat("3", "s", "2017-03-31T14:05:03.789456123Z");
        assertFormat("45", "s", "2017-03-31T12:59:45.654321987Z");
    }

    @Test
    public void testFormatShortDay() {
        assertFormat("3", "d", "2014-04-03T00:00:00.123987456Z");
    }

    @Test
    public void testFormatShortMonthName() {
        assertFormat("09, Apr", "dd, MMM", "2017-04-09T00:00:00.246813579Z");
        assertFormat("09, Dec", "dd, MMM", "2017-12-09T00:00:00.135792468Z");
        assertFormat("09, Jan", "dd, MMM", "2017-01-09T00:00:00.975318642Z");

        assertFormat("Apr", "MMM", "2017-04-09T00:00:00.864213579Z");
        assertFormat("Dec", "MMM", "2017-12-09T00:00:00.159357246Z");
        assertFormat("Jan", "MMM", "2017-01-09T00:00:00.357951246Z");
    }

    @Test
    public void testFormatShortWeekday() {
        assertFormat("09, Sun", "dd, E", "2017-04-09T00:00:00.258741369Z");
        assertFormat("10, Mon", "dd, E", "2017-04-10T00:00:00.741258963Z");
        assertFormat("11, Tue", "dd, E", "2017-04-11T00:00:00.852963147Z");
        assertFormat("12, Wed", "dd, E", "2017-04-12T00:00:00.369258147Z");
        assertFormat("13, Thu", "dd, E", "2017-04-13T00:00:00.963258741Z");
        assertFormat("14, Fri", "dd, E", "2017-04-14T00:00:00.258963147Z");
        assertFormat("15, Sat", "dd, E", "2017-04-15T00:00:00.741369258Z");
    }

    @Test
    public void testFormatTimezone() {
        assertFormat("GMT", "z", "2014-04-03T00:00:00.147258369Z");
    }

    @Test
    public void testFormatWeekOfYear() {
        assertFormat("1", "w", "2010-01-01T00:00:00.369147258Z");
        assertFormat("10", "w", "2010-03-10T00:00:00.852741369Z");
        assertFormat("11", "w", "2020-03-10T00:00:00.987456123Z");
    }

    @Test
    public void testFormatWeekday() {
        assertFormat("09, Sunday", "dd, EE", "2017-04-09T00:00:00.123456987Z");
        assertFormat("10, Monday", "dd, EE", "2017-04-10T00:00:00.456123789Z");
        assertFormat("11, Tuesday", "dd, EE", "2017-04-11T00:00:00.789456123Z");
        assertFormat("12, Wednesday", "dd, EE", "2017-04-12T00:00:00.654321789Z");
        assertFormat("13, Thursday", "dd, EE", "2017-04-13T00:00:00.147852963Z");
        assertFormat("14, Friday", "dd, EE", "2017-04-14T00:00:00.258147963Z");
        assertFormat("15, Saturday", "dd, EE", "2017-04-15T00:00:00.999852369Z");
    }

    @Test
    public void testFormatWeekdayDigit() {
        assertFormat("09, 1", "dd, u", "2017-04-09T00:00:00.753159852Z");
        assertFormat("10, 2", "dd, u", "2017-04-10T00:00:00.159753852Z");
        assertFormat("11, 3", "dd, u", "2017-04-11T00:00:00.357951246Z");
        assertFormat("12, 4", "dd, u", "2017-04-12T00:00:00.999456123Z");
        assertFormat("13, 5", "dd, u", "2017-04-13T00:00:00.258741963Z");
        assertFormat("14, 6", "dd, u", "2017-04-14T00:00:00.147258963Z");
        assertFormat("15, 7", "dd, u", "2017-04-15T00:00:00.963147258Z");
    }

    @Test
    public void testFormatYearFourDigits() {
        assertFormat("09, 2017", "dd, yyyy", "2017-04-09T00:00:00.369852741Z");
        assertFormat("2017", "yyyy", "2017-04-09T00:00:00.741258369Z");

        assertFormat("09, 1985", "dd, yyyy", "1985-04-09T00:00:00.852963741Z");
        assertFormat("1999", "yyyy", "1999-04-09T00:00:00.999741963Z");
    }

    @Test
    public void testFormatYearIsoFourDigits() {
        assertFormat("53, 2020", "ww, YYYY", "2021-01-02T00:00:00.147963258Z");
        assertFormat("2020", "YYYY", "2021-01-02T00:00:00.753852147Z");

        assertFormat("01, 1970", "ww, YYYY", "1970-01-01T00:00:00.258369147Z");
        assertFormat("1970", "YYYY", "1970-01-01T00:00:00.999369852Z");
    }

    @Test
    public void testFormatYearOneDigit() {
        assertFormat("09, 2017", "dd, y", "2017-04-09T00:00:00.741963258Z");
        assertFormat("2017", "y", "2017-04-09T00:00:00.852147369Z");

        assertFormat("09, 1981", "dd, y", "1981-04-09T00:00:00.963741258Z");
        assertFormat("2001", "y", "2001-04-09T00:00:00.147852963Z");
    }

    @Test
    public void testFormatYearThreeDigits() {
        assertFormat("09, 017", "dd, yyy", "2017-04-09T00:00:00.258147963Z");
        assertFormat("017", "yyy", "2017-04-09T00:00:00.999852369Z");

        assertFormat("09, 915", "dd, yyy", "1915-04-09T00:00:00.147963258Z");
        assertFormat("781", "yyy", "1781-04-09T00:00:00.753852147Z");
    }

    @Test
    public void testFormatYearTwoDigits() {
        assertFormat("09, 17", "dd, yy", "2017-04-09T00:00:00.258369147Z");
        assertFormat("17", "yy", "2017-04-09T00:00:00.999369852Z");

        assertFormat("09, 81", "dd, yy", "1981-04-09T00:00:00.741963258Z");
        assertFormat("91", "yy", "1991-04-09T00:00:00.852147369Z");
    }

    @Test
    public void testGreedyMillis() throws NumericException {
        assertThat("y-MM-dd HH:mm:ss.Sz", "2014-04-03T04:32:49.010000000Z", "2014-04-03 04:32:49.01Z");
    }

    @Test
    public void testGreedyYear() {
        assertThat("y-MM", "2004-03-01T00:00:00.000000000Z", "2004-03");
        assertThat("y-MM", "2036-03-01T00:00:00.000000000Z", "36-03");
        assertThat("y-MM", "2015-03-01T00:00:00.000000000Z", "15-03");
        assertThat("y-MM", "1937-03-01T00:00:00.000000000Z", "1937-03");
    }

    @Test
    public void testGreedyYear2() {
        long referenceYear = NanosFormatUtils.getReferenceYear();
        try {
            NanosFormatUtils.updateReferenceYear(Nanos.toNanos(2015, 1, 20, 0, 0));
            assertThat("y-MM", "1960-03-01T00:00:00.000000000Z", "1960-03");
            assertThat("y-MM", "2006-03-01T00:00:00.000000000Z", "06-03");
            assertThat("y-MM", "2055-03-01T00:00:00.000000000Z", "55-03");
            assertThat("y-MM", "1678-03-01T00:00:00.000000000Z", "1678-03");
        } finally {
            NanosFormatUtils.updateReferenceYear(referenceYear);
        }
    }

    @Test
    public void testHour12BadAM() {
        assertException("K MMy a", "13 0910 am");
    }

    @Test
    public void testHour12BadPM() {
        assertException("K MMy a", "13 0910 pm");
    }

    @Test
    public void testHour12Greedy() {
        assertThat("K MMy a", "2010-09-01T23:00:00.000000000Z", "11 0910 pm");
        assertThat("KaMMy", "2010-09-01T23:00:00.000000000Z", "11pm0910");
    }

    @Test
    public void testHour12GreedyOneBased() {
        assertThat("h MMy a", "2010-09-01T23:00:00.000000000Z", "11 0910 pm");
        assertThat("haMMy", "2010-09-01T23:00:00.000000000Z", "11pm0910");
    }

    @Test
    public void testHour12OneDigit() {
        assertThat("KMMy a", "2010-09-01T04:00:00.000000000Z", "40910 am");
        assertThat("KMMy a", "2010-09-01T16:00:00.000000000Z", "40910 pm");
    }

    @Test
    public void testHour12OneDigitDefaultAM() {
        assertThat("KMMy", "2010-09-01T04:00:00.000000000Z", "40910");
    }

    @Test
    public void testHour12OneDigitOneBased() {
        assertThat("hMMy a", "2010-09-01T04:00:00.000000000Z", "40910 am");
        assertThat("hMMy a", "2010-09-01T16:00:00.000000000Z", "40910 pm");
    }

    @Test
    public void testHour12TwoDigits() {
        assertThat("KKMMy a", "2010-09-01T04:00:00.000000000Z", "040910 am");
        assertThat("KKMMy a", "2010-09-01T23:00:00.000000000Z", "110910 pm");
    }

    @Test
    public void testHour12TwoDigitsOneBased() {
        assertThat("hhMMy a", "2010-09-01T03:00:00.000000000Z", "030910 am");
        assertThat("hhMMy a", "2010-09-01T23:00:00.000000000Z", "110910 pm");
    }

    @Test
    public void testHour24Greedy() {
        assertThat("H, dd-MM", "1970-11-04T03:00:00.000000000Z", "3, 04-11");
        assertThat("H, dd-MM", "1970-11-04T19:00:00.000000000Z", "19, 04-11");

        assertThat("k, dd-MM", "1970-11-04T03:00:00.000000000Z", "3, 04-11");
        assertThat("k, dd-MM", "1970-11-04T19:00:00.000000000Z", "19, 04-11");
        assertThat("H, dd-MM-yyyy", "2012-11-04T19:00:00.000000000Z", "19, 04-11-2012");
    }

    @Test
    public void testHour24OneDigit() {
        assertThat("HMMy", "2010-09-01T04:00:00.000000000Z", "40910");
        assertThat("kMMy", "2010-09-01T04:00:00.000000000Z", "40910");
        assertThat("Hmm MM-yyyy", "2010-09-01T04:09:00.000000000Z", "409 09-2010");
    }

    @Test
    public void testHour24TwoDigits() {
        assertThat("HHMMy", "2010-09-01T04:00:00.000000000Z", "040910");
        assertThat("HHMMy", "2010-09-01T23:00:00.000000000Z", "230910");

        assertThat("kkMMy", "2010-09-01T04:00:00.000000000Z", "040910");
        assertThat("kkMMy", "2010-09-01T23:00:00.000000000Z", "230910");
    }

    @Test
    public void testHttpFormat() {
        assertThat("E, dd MMM yyyy HH:mm:ss", "2017-04-05T14:55:10.000000000Z", "Mon, 05 Apr 2017 14:55:10");
    }

    @Test
    public void testIgnoredNanos() {
        assertThat("E, dd-MM-yyyy N", "2014-04-03T00:00:00.000000234Z", "Fri, 03-04-2014 234");
        assertThat("EE, dd-MM-yyyy N", "2014-04-03T00:00:00.000000234Z", "Fri, 03-04-2014 234");
    }

    @Test
    public void testIsoWeekOfYear() {
        assertThat("ww, YYYY", "2010-02-08T00:00:00.000000000Z", "06, 2010");
    }

    @Test
    public void testIsoYear() {
        assertThat("YYYY", "2010-01-01T00:00:00.000000000Z", "2010");
    }

    @Test
    public void testLeapYear() {
        assertThat("dd-MM-yyyy", "2016-02-29T00:00:00.000000000Z", "29-02-2016");
    }

    @Test(expected = NumericException.class)
    public void testLeapYearFailure() {
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
    public void testMicrosGreedy() {
        assertNanos("y-MM-dd HH:mm:ss.SSSUz", "2014-04-03T04:32:49.010330000Z", "2014-04-03 04:32:49.01033Z");
        assertNanos("yyyy U", "2017-01-01T00:00:00.000550000Z", "2017 55");
        assertNanos("U dd-MM-yyyy", "2014-10-03T00:00:00.000314000Z", "314 03-10-2014");
    }

    @Test
    public void testMicrosOneDigit() {
        assertNanos("mmUHH MMy", "2010-09-01T13:55:00.000002000Z", "55213 0910");
        assertNanos("UHH dd-MM-yyyy", "2014-10-03T14:00:00.000003000Z", "314 03-10-2014");
    }

    @Test
    public void testMicrosThreeDigit() {
        assertNanos("mmUUUHH MMy", "2010-09-01T13:55:00.000015000Z", "5501513 0910");
    }

    @Test
    public void testMillisGreedy() {
        assertThat("ddMMy HH:mm:ss.S", "2078-03-19T21:20:45.678000000Z", "190378 21:20:45.678");
    }

    @Test
    public void testMillisGreedyShort() {
        assertException("ddMMy HH:mm:ss.SSS", "190378 21:20:45.");
    }

    @Test
    public void testMillisOneDigit() {
        assertThat("mmsSHH MMy", "2010-09-01T13:55:03.002000000Z", "553213 0910");
        assertNanos("SHH dd-MM-yyyy", "2014-10-03T14:00:00.003000000Z", "314 03-10-2014");
    }

    @Test
    public void testMillisThreeDigits() {
        assertThat("ddMMy HH:mm:ss.SSS", "2078-03-19T21:20:45.678000000Z", "190378 21:20:45.678");
    }

    @Test
    public void testMinuteGreedy() {
        assertThat("dd-MM-yy HH:m", "2010-09-03T14:54:00.000000000Z", "03-09-10 14:54");
    }

    @Test
    public void testMinuteOneDigit() {
        assertThat("mHH MMy", "2010-09-01T13:05:00.000000000Z", "513 0910");
    }

    @Test
    public void testMinuteTwoDigits() {
        assertThat("mm:HH MMy", "2010-09-01T13:45:00.000000000Z", "45:13 0910");
    }

    @Test
    public void testMonthGreedy() {
        assertThat("M-y", "2012-11-01T00:00:00.000000000Z", "11-12");
        assertThat("M-y", "2012-02-01T00:00:00.000000000Z", "2-12");
    }

    @Test
    public void testMonthNameAndThreeDigitYear() {
        assertThat("dd-MMM-y", "2012-11-15T00:00:00.000000000Z", "15-NOV-12");
    }

    @Test
    public void testMonthOneDigit() {
        assertThat("My", "2010-04-01T00:00:00.000000000Z", "410");
    }

    @Test
    public void testOperationUniqueness() {

        Assert.assertTrue(MicrosFormatCompiler.getOpCount() > 0);

        IntHashSet codeSet = new IntHashSet();
        CharSequenceHashSet nameSet = new CharSequenceHashSet();
        for (int i = 0, n = MicrosFormatCompiler.getOpCount(); i < n; i++) {
            String name = MicrosFormatCompiler.getOpName(i);
            int code = MicrosFormatCompiler.getOpCode(name);
            Assert.assertTrue(codeSet.add(code));
            Assert.assertTrue(nameSet.add(name));
        }
    }

    @Test
    public void testParseNanos9Eight() {
        assertNanos("y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.123456780Z", "2022-02-02 02:02:02.12345678");
    }

    @Test
    public void testParseNanos9Five() {
        assertNanos("y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.123450000Z", "2022-02-02 02:02:02.12345");
    }

    @Test
    public void testParseNanos9Four() {
        assertNanos("y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.123400000Z", "2022-02-02 02:02:02.1234");
    }

    @Test
    public void testParseNanos9Nine() {
        assertNanos("y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.123456789Z", "2022-02-02 02:02:02.123456789");
    }

    @Test
    public void testParseNanos9One() {
        assertNanos("y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.100000000Z", "2022-02-02 02:02:02.1");
    }

    @Test
    public void testParseNanos9Seven() {
        assertNanos("y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.123456700Z", "2022-02-02 02:02:02.1234567");
    }

    @Test
    public void testParseNanos9Six() {
        assertNanos("y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.123456000Z", "2022-02-02 02:02:02.123456");
    }

    @Test(expected = NumericException.class)
    public void testParseNanos9Ten() {
        assertNanos("y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.123456789Z", "2022-02-02 02:02:02.1234567891");
    }

    @Test
    public void testParseNanos9Three() {
        assertNanos("y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.123000000Z", "2022-02-02 02:02:02.123");
    }

    @Test
    public void testParseNanos9Two() {
        assertNanos("y-MM-dd HH:mm:ss.N+", "2022-02-02T02:02:02.120000000Z", "2022-02-02 02:02:02.12");
    }

    @Test
    public void testParseUtc() {
        assertThat(CommonUtils.UTC_PATTERN, "2011-10-03T00:00:00.000000000Z", "2011-10-03T00:00:00.000Z");
    }

    @Test
    public void testQuote() {
        assertThat("yyyy'y'ddMM", "2010-03-10T00:00:00.000000000Z", "2010y1003");
    }

    @Test(expected = NumericException.class)
    public void testRandomFormat() {
        assertThat("Ketchup", "", "2021-11-19T14:00:00.000Z");
    }

    @Test
    public void testSecondGreedy() {
        assertThat("ddMMy HH:mm:s", "2078-03-19T21:20:45.000000000Z", "190378 21:20:45");
    }

    @Test
    public void testSecondOneDigit() {
        assertThat("mmsHH MMy", "2010-09-01T13:55:03.000000000Z", "55313 0910");
    }

    @Test
    public void testSecondTwoDigits() {
        assertThat("ddMMy HH:mm:ss", "2078-03-19T21:20:45.000000000Z", "190378 21:20:45");
    }

    @Test
    public void testTimeZone1() {
        assertThat("dd-MM-yy HH:m z", "2010-09-03T11:54:00.000000000Z", "03-09-10 14:54 EAT");
    }

    @Test
    public void testTimeZone2() {
        assertThat("dd-MM-yyyy HH:m z", "2015-09-03T18:50:00.000000000Z", "03-09-2015 21:50 EET");
    }

    @Test
    public void testTimeZone3() {
        assertThat("dd-MM-yy HH:m z", "2010-09-03T20:50:00.000000000Z", "03-09-10 21:50 BST");
    }

    @Test
    public void testTimeZone4() {
        assertThat("dd-MM-yy HH:m z", "2003-10-23T04:01:00.000000000Z", "23-10-03 06:01 Hora de verano de Sudáfrica", "es-PA");
    }

    @Test
    public void testTimeZone5() {
        assertThat("dd-MM-yy HH:m [z]", "2010-09-03T21:01:00.000000000Z", "03-09-10 23:01 [Hora de verano de Sudáfrica]", "es-PA");
    }

    @Test
    public void testTimeZone6() {
        assertThat("dd-MM-yy HH:m z", "2010-09-03T17:35:00.000000000Z", "03-09-10 21:50 +04:15");
    }

    @Test
    public void testTimeZone7() {
        assertThat("dd-MM-yy HH:m z", "2010-09-04T05:50:00.000000000Z", "03-09-10 21:50 UTC-08:00");
    }

    @Test
    public void testTimeZone8() {
        assertThat("dd-MM-yy HH:m z", "2010-09-04T07:50:00.000000000Z", "03-09-10 21:50 -10");
    }

    @Test(expected = NumericException.class)
    public void testTooLongInput() {
        assertThat("E, dd-MM-yyyy G", "2014-04-03T00:00:00.000Z", "Tuesday, 03-04-2014 ADD");
    }

    @Test
    public void testTwoDigitYear() {
        assertThat("MMyy", "2010-11-01T00:00:00.000000000Z", "1110");
        assertThat("MM, yy", "2010-11-01T00:00:00.000000000Z", "11, 10");
    }

    @Test
    public void testWeekOfYear() {
        assertThat("w, MM-yyyy", "2010-11-01T00:00:00.000000000Z", "6, 11-2010");
    }

    @Test
    public void testWeekdayDigit() {
        assertThat("u, dd-MM-yyyy", "2014-04-03T00:00:00.000000000Z", "5, 03-04-2014");
    }

    @Test
    public void testWeekdayIncomplete() {
        assertException("E, dd-MM-yyyy", "Tu, 03-04-2014");
    }

    @Test
    public void testWeekdayIncomplete2() {
        assertException("dd-MM-yyyy, E", "03-04-2014, Fr");
    }

    @Test
    public void testWeekdayLong() {
        assertThat("E, dd-MM-yyyy", "2014-04-03T00:00:00.000000000Z", "Tuesday, 03-04-2014");
        assertThat("EE, dd-MM-yyyy", "2014-04-03T00:00:00.000000000Z", "Tuesday, 03-04-2014");
    }

    @Test
    public void testWeekdayShort() {
        assertThat("E, dd-MM-yyyy", "2014-04-03T00:00:00.000000000Z", "Fri, 03-04-2014");
        assertThat("EE, dd-MM-yyyy", "2014-04-03T00:00:00.000000000Z", "Fri, 03-04-2014");
    }

    @Test
    public void testYearOverflow() {
        assertException("G", "-1024-04-09T00:00:00.000000000Z");
    }

    private static void assertException(String pattern, String input) {
        DateFormat format = get(pattern);
        try {
            format.parse(pattern, DateLocaleFactory.EN_LOCALE);
            Assert.fail();
        } catch (NumericException ignored) {
        }

        DateFormat compiled = compiler.compile(pattern);
        try {
            compiled.parse(input, DateLocaleFactory.EN_LOCALE);
            Assert.fail();
        } catch (NumericException ignored) {
        }
    }

    private static DateFormat get(CharSequence pattern) {
        return compiler.compile(pattern, true);
    }

    private void assertFormat(String expected, String pattern, String utcInput) throws NumericException {
        long nanos = NanosFormatUtils.parseNSecUTC(utcInput);

        sink.clear();
        get(pattern).format(nanos, DateLocaleFactory.EN_LOCALE, "GMT", sink);
        TestUtils.assertEqualsIgnoreCase(expected, sink);

        sink.clear();
        compiler.compile(pattern, false).format(nanos, DateLocaleFactory.EN_LOCALE, "GMT", sink);
        TestUtils.assertEqualsIgnoreCase(expected, sink);
    }

    private void assertNanos(String pattern, String expected, String input) throws NumericException {
        sink.clear();
        REFERENCE.format(get(pattern).parse(input, DateLocaleFactory.EN_LOCALE), DateLocaleFactory.EN_LOCALE, "Z", sink);
        TestUtils.assertEquals(expected, sink);

        sink.clear();
        REFERENCE.format(compiler.compile(pattern).parse(input, DateLocaleFactory.EN_LOCALE), DateLocaleFactory.EN_LOCALE, "Z", sink);
    }

    private void assertThat(String pattern, String expected, String input, CharSequence localeId) throws NumericException {
        assertThat(pattern, expected, input, DateLocaleFactory.INSTANCE.getLocale(localeId));
    }

    private void assertThat(String pattern, String expected, String input) throws NumericException {
        assertThat(pattern, expected, input, defaultLocale);
    }

    private void assertThat(String pattern, String expected, String input, DateLocale locale) throws NumericException {
        DateFormat format = get(pattern);
        sink.clear();
        NanosFormatUtils.appendDateTimeNSec(sink, format.parse(input, locale));
        TestUtils.assertEquals(expected, sink);

        sink.clear();
        DateFormat compiled = compiler.compile(pattern);
        NanosFormatUtils.appendDateTimeNSec(sink, compiled.parse(input, locale));
        TestUtils.assertEquals(expected, sink);
    }

    private void testAgainstJavaReferenceImpl(String pattern) throws ParseException {
        SimpleDateFormat javaFmt = new SimpleDateFormat(pattern, Locale.UK);
        javaFmt.setTimeZone(java.util.TimeZone.getTimeZone("UTC"));
        DateFormat genericQuestFmt = get(pattern);
        DateFormat compiledQuestFmt = compiler.compile(pattern);

        long step = TimeUnit.MINUTES.toNanos(15);
        for (long nanos = 0, n = TimeUnit.DAYS.toNanos(1); nanos < n; nanos += step) {
            sink.clear();
            long tsMillis = nanos / Nanos.MILLI_NANOS;
            String javaFormatted = javaFmt.format(new Date(tsMillis));

            genericQuestFmt.format(nanos, DateLocaleFactory.EN_LOCALE, "UTC", sink);
            TestUtils.assertEqualsIgnoreCase(javaFormatted, sink);

            sink.clear();
            compiledQuestFmt.format(nanos, DateLocaleFactory.EN_LOCALE, "UTC", sink);
            TestUtils.assertEqualsIgnoreCase(javaFormatted, sink);

            // now we know both Java and QuestDB format the same way.
            // let's try to parse it back.
            Assert.assertEquals(nanos, genericQuestFmt.parse(sink, DateLocaleFactory.EN_LOCALE));
            Assert.assertEquals(nanos, compiledQuestFmt.parse(sink, DateLocaleFactory.EN_LOCALE));

            // sanity check
            Assert.assertEquals(tsMillis, javaFmt.parse(sink.toString()).getTime());
        }
    }
}

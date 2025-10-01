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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TimestampDriver;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.nanotime.Nanos;
import io.questdb.std.datetime.nanotime.NanosFormatCompiler;
import io.questdb.std.datetime.nanotime.NanosFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.temporal.ChronoUnit;

import static io.questdb.std.datetime.DateLocaleFactory.EN_LOCALE;
import static io.questdb.std.datetime.TimeZoneRuleFactory.RESOLUTION_NANOS;
import static io.questdb.std.datetime.nanotime.NanosFormatUtils.parseNSecUTC;

public class NanosTest {
    private final StringSink sink = new StringSink();

    @Before
    public void setUp() {
        sink.clear();
    }

    @Test
    public void testAddDaysPrevEpoch() {
        NanosFormatCompiler compiler = new NanosFormatCompiler();
        DateFormat fmt = compiler.compile(CommonUtils.NSEC_UTC_PATTERN, true);
        long nanos = fmt.parse("2021-09-09T22:44:56.108872787Z", EN_LOCALE);
        System.out.println(nanos);
        nanos = Nanos.addDays(nanos, 24);
        fmt.format(nanos, EN_LOCALE, "Z", sink);
        System.out.println(sink);

        assertNanos(
                "1888-06-05T23:45:51.045111344Z",
                Nanos.addDays(parseNSecUTC("1888-05-12T23:45:51.045111344Z"), 24)
        );
    }

    @Test
    public void testAddMonths() {
        assertNanos(
                "2007-07-12T23:45:51.045509761Z",
                Nanos.addMonths(parseNSecUTC("2008-05-12T23:45:51.045509761Z"), -10)
        );
    }

    @Test
    public void testAddMonthsPrevEpoch() {
        assertNanos(
                "1887-07-12T23:45:51.045887332Z",
                Nanos.addMonths(parseNSecUTC("1888-05-12T23:45:51.045887332Z"), -10)
        );
    }

    @Test
    public void testAddYears() {
        assertNanos(
                "1998-05-12T23:45:51.045456091Z",
                Nanos.addYears(parseNSecUTC("1988-05-12T23:45:51.045456091Z"), 10)
        );
    }

    @Test
    public void testAddYears3() {
        assertNanos(
                "2015-01-01T00:00:00.988765341Z",
                Nanos.addYears(parseNSecUTC("2014-01-01T00:00:00.988765341Z"), 1)
        );
    }

    @Test
    public void testAddYearsNonLeapToLeap() {
        assertNanos(
                "2016-01-01T00:00:00.878901304Z",
                Nanos.addYears(parseNSecUTC("2015-01-01T00:00:00.878901304Z"), 1)
        );
    }

    @Test
    public void testAddYearsPrevEpoch() {
        assertNanos(
                "1898-05-12T23:45:51.045340901Z",
                Nanos.addYears(parseNSecUTC("1888-05-12T23:45:51.045340901Z"), 10)
        );
        assertNanos(
                "1893-05-12T00:00:00.000000000Z",
                Nanos.addYears(parseNSecUTC("1888-05-12T00:00:00.000000000Z"), 5)
        );
    }

    @Test
    public void testCeilDD() {
        assertNanos(
                "2008-05-13T00:00:00.000000000Z",
                Nanos.ceilDD(parseNSecUTC("2008-05-12T23:45:51.045900304Z"))
        );
    }

    @Test
    public void testCeilDDPrevEpoch() {
        assertNanos(
                "1888-05-13T00:00:00.000000000Z",
                Nanos.ceilDD(parseNSecUTC("1888-05-12T23:45:51.045807102Z"))
        );
    }

    @Test
    public void testCeilHH() {
        assertNanos(
                "2021-09-09T23:00:00.000000000Z",
                Nanos.ceilHH(parseNSecUTC("2021-09-09T22:44:56.789131908Z"))
        );
    }

    @Test
    public void testCeilMI() {
        assertNanos(
                "2021-09-09T22:45:00.000000000Z",
                Nanos.ceilMI(parseNSecUTC("2021-09-09T22:44:56.108872787Z"))
        );
    }

    @Test
    public void testCeilMM() {
        assertNanos(
                "2008-06-01T00:00:00.000000000Z",
                Nanos.ceilMM(parseNSecUTC("2008-05-12T23:45:51.045901781Z"))
        );
    }

    @Test
    public void testCeilMS() {
        assertNanos(
                "2021-09-09T22:44:56.109000000Z",
                Nanos.ceilMS(parseNSecUTC("2021-09-09T22:44:56.108872209Z"))
        );
    }

    @Test
    public void testCeilSS() {
        assertNanos(
                "2021-09-09T22:44:57.000000000Z",
                Nanos.ceilSS(parseNSecUTC("2021-09-09T22:44:56.789761309Z"))
        );
    }

    @Test
    public void testCeilWW() {
        assertNanos(
                "2024-01-08T00:00:00.000000000Z",
                Nanos.ceilWW(parseNSecUTC("2024-01-02T23:59:59.999821912Z"))
        );
    }

    @Test
    public void testCeilYYYY() {
        assertNanos(
                "2009-01-01T00:00:00.000000000Z",
                Nanos.ceilYYYY(parseNSecUTC("2008-05-12T23:45:51.045998123Z"))
        );
    }

    @Test
    public void testDayOfWeek() {
        long nanos = parseNSecUTC("1893-03-19T17:16:30.192901502Z");
        Assert.assertEquals(7, Nanos.getDayOfWeek(nanos));
        Assert.assertEquals(1, Nanos.getDayOfWeekSundayFirst(nanos));
        nanos = parseNSecUTC("2017-04-09T17:16:30.192576221Z");
        Assert.assertEquals(7, Nanos.getDayOfWeek(nanos));
        Assert.assertEquals(1, Nanos.getDayOfWeekSundayFirst(nanos));
    }

    @Test
    public void testDayOfYear() {
        long nanos = parseNSecUTC("2020-01-01T17:16:30.192901003Z");
        Assert.assertEquals(1, Nanos.getDayOfYear(nanos));
        nanos = parseNSecUTC("2019-03-10T07:16:30.192009167Z");
        Assert.assertEquals(69, Nanos.getDayOfYear(nanos));
        nanos = parseNSecUTC("2020-03-10T07:16:30.192890121Z");
        Assert.assertEquals(70, Nanos.getDayOfYear(nanos));
        nanos = parseNSecUTC("1893-03-19T17:16:30.192908212Z");
        Assert.assertEquals(78, Nanos.getDayOfYear(nanos));
        nanos = parseNSecUTC("2020-12-31T12:00:00.886901233Z");
        Assert.assertEquals(366, Nanos.getDayOfYear(nanos));
        nanos = parseNSecUTC("2021-12-31T12:00:00.899029812Z");
        Assert.assertEquals(365, Nanos.getDayOfYear(nanos));
    }

    @Test
    public void testDaysBetween() {
        Assert.assertEquals(
                41168,
                Nanos.getDaysBetween(
                        NanosFormatUtils.parseNanos("1904-11-05T23:45:41.045Z"),
                        NanosFormatUtils.parseNanos("2017-07-24T23:45:31.045Z")
                )
        );
        Assert.assertEquals(
                41169,
                Nanos.getDaysBetween(
                        NanosFormatUtils.parseNanos("1904-11-05T23:45:41.045Z"),
                        NanosFormatUtils.parseNanos("2017-07-24T23:45:51.045Z")
                )
        );
    }

    @Test
    public void testFloorDD() {
        testFloorDD("1969-12-31T23:59:59.999999999Z", "1969-12-31T00:00:00.000000000Z");
        testFloorDD("1969-01-01T00:00:00.000000000Z", "1969-01-01T00:00:00.000000000Z");
        testFloorDD("1969-01-01T12:13:14.567891234Z", "1969-01-01T00:00:00.000000000Z");
        testFloorDD("1970-01-01T00:00:00.000000000Z", "1970-01-01T00:00:00.000000000Z");
        testFloorDD("2008-05-12T23:45:51.045990123Z", "2008-05-12T00:00:00.000000000Z");
        testFloorDD("2025-09-03T23:59:59.999999999Z", "2025-09-03T00:00:00.000000000Z");
    }

    @Test
    public void testFloorDDWithStride() {
        assertNanos(
                "2021-09-06T00:00:00.000000000Z",
                Nanos.floorDD(parseNSecUTC("2021-09-09T22:44:56.784123789Z"), 12)
        );
    }

    @Test
    public void testFloorHH() {
        testFloorHH("1969-12-31T23:59:59.999999999Z", "1969-12-31T23:00:00.000000000Z");
        testFloorHH("1969-01-01T00:00:00.000000000Z", "1969-01-01T00:00:00.000000000Z");
        testFloorHH("1969-01-01T12:13:14.567567567Z", "1969-01-01T12:00:00.000000000Z");
        testFloorHH("1970-01-01T00:00:00.000000000Z", "1970-01-01T00:00:00.000000000Z");
        testFloorHH("2008-05-12T23:45:51.901781502Z", "2008-05-12T23:00:00.000000000Z");
        testFloorHH("2025-09-03T23:59:59.999999999Z", "2025-09-03T23:00:00.000000000Z");
    }

    @Test
    public void testFloorHHWithStride() {
        assertNanos(
                "2021-09-09T19:00:00.000000000Z",
                Nanos.floorHH(parseNSecUTC("2021-09-09T22:44:56.784123321Z"), 5)
        );
    }

    @Test
    public void testFloorMC() {
        testFloorMC("1969-12-31T23:59:59.999999999Z", "1969-12-31T23:59:59.999999000Z");
        testFloorMC("1969-01-01T00:00:00.000000000Z", "1969-01-01T00:00:00.000000000Z");
        testFloorMC("1969-01-01T12:13:14.567567567Z", "1969-01-01T12:13:14.567567000Z");
        testFloorMC("1970-01-01T00:00:00.000000000Z", "1970-01-01T00:00:00.000000000Z");
        testFloorMC("2008-05-12T23:45:51.045045045Z", "2008-05-12T23:45:51.045045000Z");
        testFloorMC("2025-09-03T23:59:59.999999999Z", "2025-09-03T23:59:59.999999000Z");
    }

    @Test
    public void testFloorMI() {
        testFloorMI("1969-12-31T23:59:59.999999999Z", "1969-12-31T23:59:00.000000000Z");
        testFloorMI("1969-01-01T00:00:00.000000000Z", "1969-01-01T00:00:00.000000000Z");
        testFloorMI("1969-01-01T12:13:14.567567567Z", "1969-01-01T12:13:00.000000000Z");
        testFloorMI("1970-01-01T00:00:00.000000000Z", "1970-01-01T00:00:00.000000000Z");
        testFloorMI("2008-05-12T23:45:51.045045045Z", "2008-05-12T23:45:00.000000000Z");
        testFloorMI("2025-09-03T23:59:59.999999999Z", "2025-09-03T23:59:00.000000000Z");
    }

    @Test
    public void testFloorMIWithStride() {
        assertNanos(
                "2021-09-09T22:18:00.000000000Z",
                Nanos.floorMI(parseNSecUTC("2021-09-09T22:44:56.784123987Z"), 73)
        );
    }

    @Test
    public void testFloorMM() {
        testFloorMM("1961-01-12T23:45:51.123123123Z", "1961-01-01T00:00:00.000000000Z");
        testFloorMM("1969-01-12T23:45:51.123123123Z", "1969-01-01T00:00:00.000000000Z");
        testFloorMM("1969-06-15T01:01:00.345345345Z", "1969-06-01T00:00:00.000000000Z");
        testFloorMM("1970-01-12T23:45:51.045000234Z", "1970-01-01T00:00:00.000000000Z");
        testFloorMM("1970-02-12T23:45:51.283000000Z", "1970-02-01T00:00:00.000000000Z");
        testFloorMM("1970-11-12T23:45:51.045901405Z", "1970-11-01T00:00:00.000000000Z");
        testFloorMM("2008-05-12T23:45:51.283000000Z", "2008-05-01T00:00:00.000000000Z");
        testFloorMM("2022-02-22T20:18:30.000000000Z", "2022-02-01T00:00:00.000000000Z");
    }

    @Test
    public void testFloorMMEpoch() {
        assertNanos(
                "1969-10-01T00:00:00.000000000Z",
                Nanos.floorMM(parseNSecUTC("1969-10-12T23:45:51.045045045Z"), 3)
        );
        assertNanos(
                "1970-10-01T00:00:00.000000000Z",
                Nanos.floorMM(parseNSecUTC("1970-10-12T23:45:51.045901781Z"), 3)
        );
        assertNanos(
                "1970-11-01T00:00:00.000000000Z",
                Nanos.floorMM(parseNSecUTC("1970-11-12T23:45:51.045981401Z"), 2)
        );
        assertNanos(
                "1970-01-01T00:00:00.000000000Z",
                Nanos.floorMM(parseNSecUTC("1970-02-12T23:45:51.045981401Z"), 3)
        );
        assertNanos(
                "1969-08-01T00:00:00.000000000Z",
                Nanos.floorMM(parseNSecUTC("1969-11-12T23:45:51.045201112Z"), 5)
        );
        assertNanos(
                "1969-01-01T00:00:00.000000000Z",
                Nanos.floorMM(parseNSecUTC("1969-05-12T23:45:51.045511341Z"), 6)
        );
    }

    @Test
    public void testFloorMS() {
        testFloorMS("1969-12-31T23:59:59.999999999Z", "1969-12-31T23:59:59.999000000Z");
        testFloorMS("1969-01-01T00:00:00.000000000Z", "1969-01-01T00:00:00.000000000Z");
        testFloorMS("1969-01-01T12:13:14.567567567Z", "1969-01-01T12:13:14.567000000Z");
        testFloorMS("1970-01-01T00:00:00.000000000Z", "1970-01-01T00:00:00.000000000Z");
        testFloorMS("2008-05-12T23:45:51.045045045Z", "2008-05-12T23:45:51.045000000Z");
        testFloorMS("2025-09-03T23:59:59.999999999Z", "2025-09-03T23:59:59.999000000Z");
    }

    @Test
    public void testFloorMSWithStride() {
        assertNanos(
                "2021-09-09T22:44:56.625000000Z",
                Nanos.floorMS(parseNSecUTC("2021-09-09T22:44:56.784123654Z"), 225)
        );
    }

    @Test
    public void testFloorNS() {
        testFloorNS("1969-12-31T23:59:59.999999999Z", "1969-12-31T23:59:59.999999999Z");
        testFloorNS("1969-01-01T00:00:00.000000000Z", "1969-01-01T00:00:00.000000000Z");
        testFloorNS("1969-01-01T12:13:14.567567567Z", "1969-01-01T12:13:14.567567567Z");
        testFloorNS("1970-01-01T00:00:00.000000000Z", "1970-01-01T00:00:00.000000000Z");
        testFloorNS("2008-05-12T23:45:51.045045045Z", "2008-05-12T23:45:51.045045045Z");
        testFloorNS("2025-09-03T23:59:59.999999999Z", "2025-09-03T23:59:59.999999999Z");
    }

    @Test
    public void testFloorNSEpoch() {
        assertNanos(
                "1969-12-31T23:59:59.999999997Z",
                Nanos.floorNS(parseNSecUTC("1969-12-31T23:59:59.999999999Z"), 3)
        );
        assertNanos(
                "2001-12-31T23:59:59.999999990Z",
                Nanos.floorNS(parseNSecUTC("2001-12-31T23:59:59.999999999Z"), 10)
        );
    }

    @Test
    public void testFloorSS() {
        testFloorSS("1969-12-31T23:59:59.999999999Z", "1969-12-31T23:59:59.000000000Z");
        testFloorSS("1969-01-01T00:00:00.000000000Z", "1969-01-01T00:00:00.000000000Z");
        testFloorSS("1969-01-01T12:13:14.567567567Z", "1969-01-01T12:13:14.000000000Z");
        testFloorSS("1970-01-01T00:00:00.000000000Z", "1970-01-01T00:00:00.000000000Z");
        testFloorSS("2008-05-12T23:45:51.045045045Z", "2008-05-12T23:45:51.000000000Z");
        testFloorSS("2025-09-03T23:59:59.999999999Z", "2025-09-03T23:59:59.000000000Z");
    }

    @Test
    public void testFloorSSWithStride() {
        assertNanos(
                "2021-09-09T22:44:40.000000000Z",
                Nanos.floorSS(parseNSecUTC("2021-09-09T22:44:56.789876543Z"), 40)
        );
    }

    @Test
    public void testFloorWW() {
        testFloorWW("1969-12-29T00:00:00.000000000Z", "1969-12-29T00:00:00.000000000Z");
        testFloorWW("1970-01-01T00:00:00.000000000Z", "1969-12-29T00:00:00.000000000Z");
        testFloorWW("2025-01-02T23:59:59.999876543Z", "2024-12-30T00:00:00.000000000Z");
        testFloorWW("2025-09-01T00:00:00.000000000Z", "2025-09-01T00:00:00.000000000Z");
        testFloorWW("2025-09-02T13:59:59.000111222Z", "2025-09-01T00:00:00.000000000Z");
    }

    @Test
    public void testFloorWWWithStride() {
        assertNanos(
                "2021-08-30T00:00:00.000000000Z",
                Nanos.floorWW(parseNSecUTC("2021-09-09T22:44:56.784123456Z"), 2)
        );
    }

    @Test
    public void testFloorYYYY() {
        testFloorYYYY("1969-01-01T00:00:00.000000000Z", "1969-01-01T00:00:00.000000000Z");
        testFloorYYYY("1970-01-01T00:00:00.000000000Z", "1970-01-01T00:00:00.000000000Z");
        testFloorYYYY("2008-05-12T23:45:51.045123456Z", "2008-01-01T00:00:00.000000000Z");
        testFloorYYYY("2025-12-31T23:59:59.999999999Z", "2025-01-01T00:00:00.000000000Z");
    }

    @Test
    public void testFloorYYYYEpoch() {
        assertNanos(
                "1967-01-01T00:00:00.000000000Z",
                Nanos.floorYYYY(parseNSecUTC("1968-05-12T23:45:51.045045045Z"), 3)
        );
        assertNanos(
                "2000-01-01T00:00:00.000000000Z",
                Nanos.floorYYYY(parseNSecUTC("2000-10-12T23:45:51.045901781Z"), 3)
        );
        assertNanos(
                "2000-01-01T00:00:00.000000000Z",
                Nanos.floorYYYY(parseNSecUTC("2002-10-12T23:45:51.045901781Z"), 3)
        );
        assertNanos(
                "2005-01-01T00:00:00.000000000Z",
                Nanos.floorYYYY(parseNSecUTC("2006-10-12T23:45:51.045901781Z"), 5)
        );
        assertNanos(
                "1970-01-01T00:00:00.000000000Z",
                Nanos.floorYYYY(parseNSecUTC("1970-05-12T23:45:51.045654321Z"), 3)
        );
        assertNanos(
                "1967-01-01T00:00:00.000000000Z",
                Nanos.floorYYYY(parseNSecUTC("1967-05-12T23:45:51.045246813Z"), 3)
        );
        assertNanos(
                "1973-01-01T00:00:00.000000000Z",
                Nanos.floorYYYY(parseNSecUTC("1975-05-12T23:45:51.045789123Z"), 3)
        );
    }

    @Test
    public void testFormatDateTime() {
        assertTrue("2014-11-30T12:34:55.332981291Z");
        assertTrue("2008-03-15T11:22:30.500800301Z");
        assertTrue("1917-10-01T11:22:30.500504109Z");
        assertTrue("2045-01-01T01:02:00.005093022Z");
    }

    @Test
    public void testGetWeek() {
        Assert.assertEquals(52, Nanos.getWeek(parseNSecUTC("2017-12-31T13:32:12.531123456Z")));
        Assert.assertEquals(1, Nanos.getWeek(parseNSecUTC("2018-01-01T03:32:12.531789123Z")));
        Assert.assertEquals(51, Nanos.getWeek(parseNSecUTC("2020-12-20T13:32:12.531456789Z")));
        Assert.assertEquals(52, Nanos.getWeek(parseNSecUTC("2020-12-23T13:32:12.531654321Z")));
        Assert.assertEquals(53, Nanos.getWeek(parseNSecUTC("2021-01-01T13:32:12.531987654Z")));
        Assert.assertEquals(1, Nanos.getWeek(parseNSecUTC("2021-01-04T13:32:12.531246813Z")));
        Assert.assertEquals(1, Nanos.getWeek(parseNSecUTC("2022-01-09T13:32:12.531135792Z")));
        Assert.assertEquals(2, Nanos.getWeek(parseNSecUTC("2022-01-10T13:32:12.531864209Z")));
    }

    @Test
    public void testMonthsBetween() {
        // a < b, same year
        Assert.assertEquals(
                2,
                Nanos.getMonthsBetween(
                        parseNSecUTC("2014-05-12T23:45:51.045123456Z"),
                        parseNSecUTC("2014-07-15T23:45:51.045789123Z")
                )
        );

        // a > b, same year
        Assert.assertEquals(
                2,
                Nanos.getMonthsBetween(
                        parseNSecUTC("2014-07-15T23:45:51.045246813Z"),
                        parseNSecUTC("2014-05-12T23:45:51.045987654Z")
                )
        );

        // a < b, different year
        Assert.assertEquals(
                26,
                Nanos.getMonthsBetween(
                        parseNSecUTC("2014-05-12T23:45:51.045654321Z"),
                        parseNSecUTC("2016-07-15T23:45:51.045135792Z")
                )
        );

        // a < b, same year, a has higher residuals
        Assert.assertEquals(
                1,
                Nanos.getMonthsBetween(
                        parseNSecUTC("2014-05-12T23:45:51.045864209Z"),
                        parseNSecUTC("2014-07-03T23:45:51.045567890Z")
                )
        );

        // a < b, a before epoch, a has higher residuals
        Assert.assertEquals(
                109 * 12 + 1,
                Nanos.getMonthsBetween(
                        parseNSecUTC("1905-05-12T23:45:51.045112233Z"),
                        parseNSecUTC("2014-07-03T23:45:51.045445566Z")
                )
        );
    }

    @Test
    public void testNExtOrSameDow3() {
        assertNanos(
                "2017-04-06T00:00:00.000123456Z",
                Nanos.nextOrSameDayOfWeek(parseNSecUTC("2017-04-06T00:00:00.000123456Z"), 4)
        );
    }

    @Test
    public void testNextDSTFixed() throws NumericException {
        String tz = "GMT";
        TimeZoneRules rules = getRules(tz);
        final long ts = rules.getNextDST(parseNSecUTC("2021-02-10T01:00:00.231654879Z"));
        Assert.assertEquals(Long.MAX_VALUE, ts);
    }

    @Test
    public void testNextDSTHistory() throws NumericException {
        TimeZoneRules rules = getRules("Europe/Berlin");

        assertNanos(
                "1991-09-29T01:00:00.000000000Z",
                rules.getNextDST(parseNSecUTC("1991-09-20T01:00:00.123456789Z"))
        );
    }

    @Test
    public void testNextDSTHistoryLast() throws NumericException {
        TimeZoneRules rules = getRules("Europe/Berlin");

        assertNanos(
                "1998-03-29T01:00:00.000000000Z",
                rules.getNextDST(parseNSecUTC("1997-10-26T01:00:00.987654321Z"))
        );
    }

    @Test
    public void testNextDSTRulesAfterFirst() throws NumericException {
        TimeZoneRules rules = getRules("Europe/Berlin");
        assertNanos(
                "2021-10-31T01:00:00.000000000Z",
                rules.getNextDST(parseNSecUTC("2021-07-03T01:00:00.333222111Z"))
        );
    }

    @Test
    public void testNextDSTRulesAfterLast() throws NumericException {
        TimeZoneRules rules = getRules("Europe/Berlin");
        assertNanos(
                "2022-03-27T01:00:00.000000000Z",
                rules.getNextDST(parseNSecUTC("2021-10-31T01:00:00.111222333Z"))
        );
    }

    @Test
    public void testNextDSTRulesBeforeFirst() throws NumericException {
        String tz = "Europe/Berlin";
        TimeZoneRules rules = getRules(tz);
        assertNanos(
                "2021-03-28T01:00:00.000000000Z",
                rules.getNextDST(parseNSecUTC("2021-02-10T01:00:00.765100304Z"))
        );
    }

    @Test
    public void testNextOrSameDow1() {
        assertNanos(
                "2017-04-12T00:00:00.123456789Z",
                Nanos.nextOrSameDayOfWeek(parseNSecUTC("2017-04-06T00:00:00.123456789Z"), 3)
        );
    }

    @Test
    public void testNextOrSameDow2() {
        assertNanos(
                "2017-04-08T00:00:00.876543210Z",
                Nanos.nextOrSameDayOfWeek(parseNSecUTC("2017-04-06T00:00:00.876543210Z"), 6)
        );
    }

    @Test
    public void testParseBadISODate() {
        expectExceptionDateTime("201");
        expectExceptionDateTime("2014-");
        expectExceptionDateTime("2014-0");
        expectExceptionDateTime("2014-03-");
        expectExceptionDateTime("2014-03-1");
        expectExceptionDateTime("2014-03-10T0");
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
        assertTrue("2008-02-29T10:54:01.010123902Z");
    }

    @Test
    public void testParseDateTimePrevEpoch() {
        assertTrue("1812-02-29T10:54:01.010901450Z");
    }

    @Test
    public void testParseTimestampNotNullLocale() {
        try {
            // we deliberately mangle timezone so that function begins to rely on locale to resolve text
            NanosFormatUtils.parseNanos("2020-01-10T15:00:01.000143Zz");
            Assert.fail();
        } catch (NumericException ignored) {
        }
    }

    @Test
    public void testParseWW() throws NumericException {
        TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP_NANO);
        DateFormat byWeek = driver.getPartitionDirFormatMethod(PartitionBy.WEEK);
        try {
            byWeek.parse("2020-W00", EN_LOCALE);
            Assert.fail("ISO Week 00 is invalid");
        } catch (NumericException ignore) {
        }

        try {
            byWeek.parse("2020-W54", EN_LOCALE);
            Assert.fail();
        } catch (NumericException ignore) {
        }

        Assert.assertEquals("2019-12-30T00:00:00.000Z", Nanos.toString(byWeek.parse("2020-W01", EN_LOCALE)));
        Assert.assertEquals("2020-12-28T00:00:00.000Z", Nanos.toString(byWeek.parse("2020-W53", EN_LOCALE)));
        Assert.assertEquals("2021-01-04T00:00:00.000Z", Nanos.toString(byWeek.parse("2021-W01", EN_LOCALE)));

        try {
            byWeek.parse("2019-W53", EN_LOCALE);
            Assert.fail("2019 has 52 ISO weeks");
        } catch (NumericException ignore) {
        }

        Assert.assertEquals("2019-12-30T00:00:00.000Z", Nanos.toString(byWeek.parse("2020-W01", EN_LOCALE)));
        Assert.assertEquals("2014-12-22T00:00:00.000Z", Nanos.toString(byWeek.parse("2014-W52", EN_LOCALE)));
        Assert.assertEquals("2015-12-28T00:00:00.000Z", Nanos.toString(byWeek.parse("2015-W53", EN_LOCALE)));
    }

    @Test(expected = NumericException.class)
    public void testParseWrongDay() {
        parseNSecUTC("2013-09-31T00:00:00.000Z");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongHour() {
        parseNSecUTC("2013-09-30T25:00:00.000Z");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongMicros() {
        parseNSecUTC("2013-09-30T22:04:34.1024091Z");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongMinute() {
        parseNSecUTC("2013-09-30T22:61:00.000Z");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongMonth() {
        parseNSecUTC("2013-00-12T00:00:00.000Z");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongSecond() {
        parseNSecUTC("2013-09-30T22:04:60.000Z");
    }

    @Test
    public void testPreviousOrSameDow1() {
        assertNanos(
                "2017-04-05T00:00:00.111222333Z",
                Nanos.previousOrSameDayOfWeek(parseNSecUTC("2017-04-06T00:00:00.111222333Z"), 3)
        );
    }

    @Test
    public void testPreviousOrSameDow2() {
        assertNanos(
                "2017-04-01T00:00:00.444555666Z",
                Nanos.previousOrSameDayOfWeek(parseNSecUTC("2017-04-06T00:00:00.444555666Z"), 6)
        );
    }

    @Test
    public void testPreviousOrSameDow3() {
        assertNanos(
                "2017-04-06T00:00:00.777888999Z",
                Nanos.previousOrSameDayOfWeek(parseNSecUTC("2017-04-06T00:00:00.777888999Z"), 4)
        );
    }

    @Test
    public void testToNanos() {
        Assert.assertEquals(1, Nanos.toNanos(1, ChronoUnit.NANOS));
        Assert.assertEquals(1000, Nanos.toNanos(1, ChronoUnit.MICROS));
        Assert.assertEquals(1_000_000, Nanos.toNanos(1, ChronoUnit.MILLIS));
        Assert.assertEquals(1_000_000_000, Nanos.toNanos(1, ChronoUnit.SECONDS));

        Assert.assertEquals(60_000_000_000L, Nanos.toNanos(1, ChronoUnit.MINUTES));
        Assert.assertEquals(Long.MAX_VALUE, Nanos.toNanos(Long.MAX_VALUE, ChronoUnit.NANOS));

        Assert.assertEquals(Nanos.toNanos(1, ChronoUnit.HOURS), Nanos.toNanos(60, ChronoUnit.MINUTES));
        Assert.assertEquals(0, Nanos.toNanos(0, ChronoUnit.NANOS));
        Assert.assertEquals(0, Nanos.toNanos(0, ChronoUnit.MICROS));
        Assert.assertEquals(0, Nanos.toNanos(0, ChronoUnit.MILLIS));
        Assert.assertEquals(0, Nanos.toNanos(0, ChronoUnit.SECONDS));
        Assert.assertEquals(0, Nanos.toNanos(0, ChronoUnit.MINUTES));

        // micros values remain unchanged
        Assert.assertEquals(123456789_000L, Nanos.toNanos(123456789L, ChronoUnit.MICROS));
        Assert.assertEquals(123456789_001L, Nanos.toNanos(123456789001L, ChronoUnit.NANOS));
    }

    @Test(expected = NumericException.class)
    public void testToTimezoneInvalidTimezoneName() throws NumericException {
        DateLocale locale = DateLocaleFactory.INSTANCE.getLocale("en");
        long nanos = parseNSecUTC("2019-12-10T10:00:00.123456789Z");
        Nanos.toTimezone(nanos, locale, "Somewhere");
    }

    @Test
    public void testToTimezoneWithHours() throws NumericException {
        DateLocale locale = DateLocaleFactory.INSTANCE.getLocale("en");
        long nanos = parseNSecUTC("2019-12-10T10:00:00.123456789Z");
        long offsetNanos = Nanos.toTimezone(nanos, locale, "+03:45");
        assertNanos("2019-12-10T13:45:00.123456789Z", offsetNanos);
    }

    @Test
    public void testToTimezoneWithHoursInString() throws NumericException {
        DateLocale locale = DateLocaleFactory.INSTANCE.getLocale("en");
        long nanos = parseNSecUTC("2019-12-10T10:00:00.987654321Z");
        long offsetNanos = Nanos.toTimezone(nanos, locale, "hello +03:45 there", 6, 12);
        assertNanos("2019-12-10T13:45:00.987654321Z", offsetNanos);
    }

    @Test
    public void testToTimezoneWithTimezoneName() throws NumericException {
        DateLocale locale = DateLocaleFactory.INSTANCE.getLocale("en");
        long nanos = parseNSecUTC("2019-12-10T10:00:00.123456789Z");
        long offsetNanos = Nanos.toTimezone(nanos, locale, "Europe/Prague");
        assertNanos("2019-12-10T11:00:00.123456789Z", offsetNanos);
    }

    @Test(expected = NumericException.class)
    public void testToUTCInvalidTimezoneName() throws NumericException {
        DateLocale locale = DateLocaleFactory.INSTANCE.getLocale("en");
        long nanos = parseNSecUTC("2019-12-10T10:00:00.987654321Z");
        Nanos.toUTC(nanos, locale, "Somewhere");
    }

    @Test
    public void testToUTCWithHours() throws NumericException {
        DateLocale locale = DateLocaleFactory.INSTANCE.getLocale("en");
        long nanos = parseNSecUTC("2019-12-10T10:00:00.456789123Z");
        long offsetNanos = Nanos.toUTC(nanos, locale, "+03:45");
        assertNanos("2019-12-10T06:15:00.456789123Z", offsetNanos);
    }

    @Test
    public void testToUTCWithHoursInString() throws NumericException {
        DateLocale locale = DateLocaleFactory.INSTANCE.getLocale("en");
        long nanos = parseNSecUTC("2019-12-10T10:00:00.135792468Z");
        long offsetNanos = Nanos.toUTC(nanos, locale, "hello +03:45 there", 6, 12);
        assertNanos("2019-12-10T06:15:00.135792468Z", offsetNanos);
    }

    @Test
    public void testToUTCWithTimezoneName() throws NumericException {
        DateLocale locale = DateLocaleFactory.INSTANCE.getLocale("en");
        long nanos = parseNSecUTC("2019-12-10T10:00:00.246813579Z");
        long offsetNanos = Nanos.toUTC(nanos, locale, "Europe/Prague");
        assertNanos("2019-12-10T09:00:00.246813579Z", offsetNanos);
    }

    @Test
    public void testWallHour() {
        Assert.assertEquals(19, Nanos.getWallHours(1592078287051004114L));
        Assert.assertEquals(15, Nanos.getWallHours(1592063943181693345L));
        Assert.assertEquals(8, Nanos.getWallHours(-1592063943181693876L));
    }

    @Test
    public void testWeekOfMonth() {
        Assert.assertEquals(1, Nanos.getWeekOfMonth(parseNSecUTC("2020-01-01T17:16:30.192345678Z")));
        Assert.assertEquals(2, Nanos.getWeekOfMonth(parseNSecUTC("2019-03-10T07:16:30.192837465Z")));
        Assert.assertEquals(5, Nanos.getWeekOfMonth(parseNSecUTC("2020-12-31T12:00:00.000111222Z")));
        Assert.assertEquals(5, Nanos.getWeekOfMonth(parseNSecUTC("2021-12-31T12:00:00.000333444Z")));
    }

    @Test
    public void testWeekOfYear() {
        Assert.assertEquals(1, Nanos.getWeekOfYear(parseNSecUTC("2020-01-01T17:16:30.192555666Z")));
        Assert.assertEquals(10, Nanos.getWeekOfYear(parseNSecUTC("2019-03-10T07:16:30.192777888Z")));
        Assert.assertEquals(11, Nanos.getWeekOfYear(parseNSecUTC("2020-03-10T07:16:30.192999000Z")));
        Assert.assertEquals(12, Nanos.getWeekOfYear(parseNSecUTC("1893-03-19T17:16:30.192111222Z")));
        Assert.assertEquals(53, Nanos.getWeekOfYear(parseNSecUTC("2020-12-31T12:00:00.000444555Z")));
        Assert.assertEquals(53, Nanos.getWeekOfYear(parseNSecUTC("2021-12-31T12:00:00.000666777Z")));
    }

    @Test
    public void testYearsBetween() {
        Assert.assertEquals(
                112,
                Nanos.getYearsBetween(
                        parseNSecUTC("1904-11-05T23:45:41.045123456Z"),
                        parseNSecUTC("2017-07-24T23:45:31.045789123Z")
                )
        );

        Assert.assertEquals(
                113,
                Nanos.getYearsBetween(
                        parseNSecUTC("1904-11-05T23:45:41.045246813Z"),
                        parseNSecUTC("2017-12-24T23:45:51.045987654Z")
                )
        );
    }

    @Test
    public void testYearsBetween2() throws NumericException {
        long nanos1 = parseNSecUTC("2020-04-24T01:49:12.005123456Z");
        long nanos2 = parseNSecUTC("2025-04-24T01:49:12.005789123Z");
        Assert.assertEquals(5, Nanos.getYearsBetween(nanos1, nanos2));
        Assert.assertEquals(5, Nanos.getYearsBetween(nanos2, nanos1));
    }

    @Test
    public void testYearsBetween3() throws NumericException {
        long nanos1 = parseNSecUTC("2020-04-24T01:49:12.005654321Z");
        long nanos2 = parseNSecUTC("2024-04-24T01:49:12.005987654Z");
        Assert.assertEquals(4, Nanos.getYearsBetween(nanos1, nanos2));
        Assert.assertEquals(4, Nanos.getYearsBetween(nanos2, nanos1));
    }

    private static TimeZoneRules getRules(String tz) throws NumericException {
        return EN_LOCALE.getZoneRules(
                Numbers.decodeLowInt(EN_LOCALE.matchZone(tz, 0, tz.length())),
                RESOLUTION_NANOS
        );
    }

    private void assertNanos(CharSequence expected, long nanosActual) {
        sink.clear();
        NanosFormatUtils.appendDateTimeNSec(sink, nanosActual);
        TestUtils.assertEquals(expected, sink);
    }

    private void assertTrue(String date) throws NumericException {
        sink.clear();
        NanosFormatUtils.appendDateTimeNSec(sink, parseNSecUTC(date));
        TestUtils.assertEquals(date, sink);
    }

    private void expectExceptionDateTime(String s) {
        try {
            NanosFormatUtils.parseNanos(s);
            Assert.fail("Expected exception");
        } catch (NumericException ignore) {
        }
    }

    private void testFloorDD(String timestamp, String expected) {
        assertNanos(
                expected,
                Nanos.floorDD(parseNSecUTC(timestamp))
        );
        assertNanos(
                expected,
                Nanos.floorDD(parseNSecUTC(timestamp), 1)
        );
        assertNanos(
                expected,
                Nanos.floorDD(parseNSecUTC(timestamp), 1, 0)
        );
    }

    private void testFloorHH(String timestamp, String expected) {
        assertNanos(
                expected,
                Nanos.floorHH(parseNSecUTC(timestamp))
        );
        assertNanos(
                expected,
                Nanos.floorHH(parseNSecUTC(timestamp), 1)
        );
        assertNanos(
                expected,
                Nanos.floorHH(parseNSecUTC(timestamp), 1, 0)
        );
    }

    private void testFloorMC(String timestamp, String expected) {
        assertNanos(
                expected,
                Nanos.floorMC(parseNSecUTC(timestamp))
        );
        assertNanos(
                expected,
                Nanos.floorMC(parseNSecUTC(timestamp), 1)
        );
        assertNanos(
                expected,
                Nanos.floorMC(parseNSecUTC(timestamp), 1, 0)
        );
    }

    private void testFloorMI(String timestamp, String expected) {
        assertNanos(
                expected,
                Nanos.floorMI(parseNSecUTC(timestamp))
        );
        assertNanos(
                expected,
                Nanos.floorMI(parseNSecUTC(timestamp), 1)
        );
        assertNanos(
                expected,
                Nanos.floorMI(parseNSecUTC(timestamp), 1, 0)
        );
    }

    private void testFloorMM(String timestamp, String expected) {
        assertNanos(
                expected,
                Nanos.floorMM(parseNSecUTC(timestamp))
        );
        assertNanos(
                expected,
                Nanos.floorMM(parseNSecUTC(timestamp), 1)
        );
        assertNanos(
                expected,
                Nanos.floorMM(parseNSecUTC(timestamp), 1, 0)
        );
    }

    private void testFloorMS(String timestamp, String expected) {
        assertNanos(
                expected,
                Nanos.floorMS(parseNSecUTC(timestamp))
        );
        assertNanos(
                expected,
                Nanos.floorMS(parseNSecUTC(timestamp), 1)
        );
        assertNanos(
                expected,
                Nanos.floorMS(parseNSecUTC(timestamp), 1, 0)
        );
    }

    private void testFloorNS(String timestamp, String expected) {
        assertNanos(
                expected,
                Nanos.floorNS(parseNSecUTC(timestamp))
        );
        assertNanos(
                expected,
                Nanos.floorNS(parseNSecUTC(timestamp), 1)
        );
        assertNanos(
                expected,
                Nanos.floorNS(parseNSecUTC(timestamp), 1, 0)
        );
    }

    private void testFloorSS(String timestamp, String expected) {
        assertNanos(
                expected,
                Nanos.floorSS(parseNSecUTC(timestamp))
        );
        assertNanos(
                expected,
                Nanos.floorSS(parseNSecUTC(timestamp), 1)
        );
        assertNanos(
                expected,
                Nanos.floorSS(parseNSecUTC(timestamp), 1, 0)
        );
    }

    private void testFloorWW(String timestamp, String expected) {
        assertNanos(
                expected,
                Nanos.floorWW(parseNSecUTC(timestamp))
        );
        assertNanos(
                expected,
                Nanos.floorWW(parseNSecUTC(timestamp), 1)
        );
        assertNanos(
                expected,
                Nanos.floorWW(parseNSecUTC(timestamp), 1, 0)
        );
    }

    private void testFloorYYYY(String timestamp, String expected) {
        assertNanos(
                expected,
                Nanos.floorYYYY(parseNSecUTC(timestamp))
        );
        assertNanos(
                expected,
                Nanos.floorYYYY(parseNSecUTC(timestamp), 1)
        );
        assertNanos(
                expected,
                Nanos.floorYYYY(parseNSecUTC(timestamp), 1, 0)
        );
    }
}

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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.cairo.PartitionBy.getPartitionDirFormatMethod;
import static io.questdb.std.datetime.DateLocaleFactory.EN_LOCALE;
import static io.questdb.std.datetime.TimeZoneRuleFactory.RESOLUTION_MICROS;
import static io.questdb.std.datetime.microtime.MicrosFormatUtils.parseHTTP;

public class MicrosTest {
    private final StringSink sink = new StringSink();

    @Before
    public void setUp() {
        sink.clear();
    }

    @Test
    public void testAddDaysPrevEpoch() {
        long micros = MicrosFormatUtils.parseTimestamp("1888-05-12T23:45:51.045045Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.addDays(micros, 24));
        TestUtils.assertEquals("1888-06-05T23:45:51.045045Z", sink);
    }

    @Test
    public void testAddMonths() {
        long micros = MicrosFormatUtils.parseTimestamp("2008-05-12T23:45:51.045045Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.addMonths(micros, -10));
        TestUtils.assertEquals("2007-07-12T23:45:51.045045Z", sink);
    }

    @Test
    public void testAddMonthsPrevEpoch() {
        long micros = MicrosFormatUtils.parseTimestamp("1888-05-12T23:45:51.045045Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.addMonths(micros, -10));
        TestUtils.assertEquals("1887-07-12T23:45:51.045045Z", sink);
    }

    @Test
    public void testAddYears() {
        long micros = MicrosFormatUtils.parseTimestamp("1988-05-12T23:45:51.045045Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.addYears(micros, 10));
        TestUtils.assertEquals("1998-05-12T23:45:51.045045Z", sink);
    }

    @Test
    public void testAddYears3() {
        long micros = MicrosFormatUtils.parseTimestamp("2014-01-01T00:00:00.000000Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.addYears(micros, 1));
        TestUtils.assertEquals("2015-01-01T00:00:00.000000Z", sink);
    }

    @Test
    public void testAddYearsNonLeapToLeap() {
        long micros = MicrosFormatUtils.parseTimestamp("2015-01-01T00:00:00.000000Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.addYears(micros, 1));
        TestUtils.assertEquals("2016-01-01T00:00:00.000000Z", sink);
    }

    @Test
    public void testAddYearsPrevEpoch() {
        long micros = MicrosFormatUtils.parseTimestamp("1888-05-12T23:45:51.045045Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.addYears(micros, 10));
        TestUtils.assertEquals("1898-05-12T23:45:51.045045Z", sink);
        sink.clear();
        micros = MicrosFormatUtils.parseTimestamp("1888-05-12T00:00:00.000000Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.addYears(micros, 5));
        TestUtils.assertEquals("1893-05-12T00:00:00.000000Z", sink);
    }

    @Test
    public void testCeilDD() {
        long micros = MicrosFormatUtils.parseTimestamp("2008-05-12T23:45:51.045045Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.ceilDD(micros));
        TestUtils.assertEquals("2008-05-13T00:00:00.000000Z", sink);
    }

    @Test
    public void testCeilDDPrevEpoch() {
        long micros = MicrosFormatUtils.parseTimestamp("1888-05-12T23:45:51.045045Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.ceilDD(micros));
        TestUtils.assertEquals("1888-05-13T00:00:00.000000Z", sink);
    }

    @Test
    public void testCeilHH() {
        final long micros = MicrosFormatUtils.parseTimestamp("2021-09-09T22:44:56.789789Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.ceilHH(micros));
        TestUtils.assertEquals("2021-09-09T23:00:00.000000Z", sink);
    }

    @Test
    public void testCeilMI() {
        final long micros = MicrosFormatUtils.parseTimestamp("2021-09-09T22:44:56.108872Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.ceilMI(micros));
        TestUtils.assertEquals("2021-09-09T22:45:00.000000Z", sink);
    }

    @Test
    public void testCeilMM() {
        long micros = MicrosFormatUtils.parseTimestamp("2008-05-12T23:45:51.045045Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.ceilMM(micros));
        TestUtils.assertEquals("2008-06-01T00:00:00.000000Z", sink);
    }

    @Test
    public void testCeilMS() {
        final long micros = MicrosFormatUtils.parseTimestamp("2021-09-09T22:44:56.108872Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.ceilMS(micros));
        TestUtils.assertEquals("2021-09-09T22:44:56.109000Z", sink);
    }

    @Test
    public void testCeilSS() {
        final long micros = MicrosFormatUtils.parseTimestamp("2021-09-09T22:44:56.789789Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.ceilSS(micros));
        TestUtils.assertEquals("2021-09-09T22:44:57.000000Z", sink);
    }

    @Test
    public void testCeilWW() {
        long micros = MicrosFormatUtils.parseTimestamp("2024-01-02T23:59:59.999999Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.ceilWW(micros));
        TestUtils.assertEquals("2024-01-08T00:00:00.000000Z", sink);
    }

    @Test
    public void testCeilYYYY() {
        long micros = MicrosFormatUtils.parseTimestamp("2008-05-12T23:45:51.045045Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.ceilYYYY(micros));
        TestUtils.assertEquals("2009-01-01T00:00:00.000000Z", sink);
    }

    @Test
    public void testDayOfWeek() {
        long micros = MicrosFormatUtils.parseTimestamp("1893-03-19T17:16:30.192Z");
        Assert.assertEquals(7, Micros.getDayOfWeek(micros));
        Assert.assertEquals(1, Micros.getDayOfWeekSundayFirst(micros));
        micros = MicrosFormatUtils.parseTimestamp("2017-04-09T17:16:30.192Z");
        Assert.assertEquals(7, Micros.getDayOfWeek(micros));
        Assert.assertEquals(1, Micros.getDayOfWeekSundayFirst(micros));
    }

    @Test
    public void testDayOfYear() {
        long micros = MicrosFormatUtils.parseTimestamp("2020-01-01T17:16:30.192Z");
        Assert.assertEquals(1, Micros.getDayOfYear(micros));
        micros = MicrosFormatUtils.parseTimestamp("2019-03-10T07:16:30.192Z");
        Assert.assertEquals(69, Micros.getDayOfYear(micros));
        micros = MicrosFormatUtils.parseTimestamp("2020-03-10T07:16:30.192Z");
        Assert.assertEquals(70, Micros.getDayOfYear(micros));
        micros = MicrosFormatUtils.parseTimestamp("1893-03-19T17:16:30.192Z");
        Assert.assertEquals(78, Micros.getDayOfYear(micros));
        micros = MicrosFormatUtils.parseTimestamp("2020-12-31T12:00:00.000Z");
        Assert.assertEquals(366, Micros.getDayOfYear(micros));
        micros = MicrosFormatUtils.parseTimestamp("2021-12-31T12:00:00.000Z");
        Assert.assertEquals(365, Micros.getDayOfYear(micros));
    }

    @Test
    public void testDaysBetween() {
        Assert.assertEquals(
                41168,
                Micros.getDaysBetween(
                        MicrosFormatUtils.parseTimestamp("1904-11-05T23:45:41.045Z"),
                        MicrosFormatUtils.parseTimestamp("2017-07-24T23:45:31.045Z")
                )
        );
        Assert.assertEquals(
                41169,
                Micros.getDaysBetween(
                        MicrosFormatUtils.parseTimestamp("1904-11-05T23:45:41.045Z"),
                        MicrosFormatUtils.parseTimestamp("2017-07-24T23:45:51.045Z")
                )
        );
    }

    @Test
    public void testFloorDD() {
        testFloorDD("1969-12-31T23:59:59.999999Z", "1969-12-31T00:00:00.000000Z");
        testFloorDD("1969-01-01T00:00:00.000000Z", "1969-01-01T00:00:00.000000Z");
        testFloorDD("1969-01-01T12:13:14.567567Z", "1969-01-01T00:00:00.000000Z");
        testFloorDD("1970-01-01T00:00:00.000000Z", "1970-01-01T00:00:00.000000Z");
        testFloorDD("2008-05-12T23:45:51.045045Z", "2008-05-12T00:00:00.000000Z");
        testFloorDD("2025-09-03T23:59:59.999999Z", "2025-09-03T00:00:00.000000Z");
    }

    @Test
    public void testFloorHH() {
        testFloorHH("1969-12-31T23:59:59.999999Z", "1969-12-31T23:00:00.000000Z");
        testFloorHH("1969-01-01T00:00:00.000000Z", "1969-01-01T00:00:00.000000Z");
        testFloorHH("1969-01-01T12:13:14.567567Z", "1969-01-01T12:00:00.000000Z");
        testFloorHH("1970-01-01T00:00:00.000000Z", "1970-01-01T00:00:00.000000Z");
        testFloorHH("2008-05-12T23:45:51.045045Z", "2008-05-12T23:00:00.000000Z");
        testFloorHH("2025-09-03T23:59:59.999999Z", "2025-09-03T23:00:00.000000Z");
    }

    @Test
    public void testFloorMC() {
        testFloorMC("1969-12-31T23:59:59.999999Z", "1969-12-31T23:59:59.999999Z");
        testFloorMC("1969-01-01T00:00:00.000000Z", "1969-01-01T00:00:00.000000Z");
        testFloorMC("1969-01-01T12:13:14.567567Z", "1969-01-01T12:13:14.567567Z");
        testFloorMC("1970-01-01T00:00:00.000000Z", "1970-01-01T00:00:00.000000Z");
        testFloorMC("2008-05-12T23:45:51.045045Z", "2008-05-12T23:45:51.045045Z");
        testFloorMC("2025-09-03T23:59:59.999999Z", "2025-09-03T23:59:59.999999Z");
    }

    @Test
    public void testFloorMCEpoch() {
        long micros = MicrosFormatUtils.parseTimestamp("1969-12-31T23:59:59.999999Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorMC(micros, 3));
        TestUtils.assertEquals("1969-12-31T23:59:59.999997Z", sink);
        sink.clear();
        micros = MicrosFormatUtils.parseTimestamp("2001-12-31T23:59:59.999999Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorMC(micros, 10));
        TestUtils.assertEquals("2001-12-31T23:59:59.999990Z", sink);
    }

    @Test
    public void testFloorMI() {
        testFloorMI("1969-12-31T23:59:59.999999Z", "1969-12-31T23:59:00.000000Z");
        testFloorMI("1969-01-01T00:00:00.000000Z", "1969-01-01T00:00:00.000000Z");
        testFloorMI("1969-01-01T12:13:14.567567Z", "1969-01-01T12:13:00.000000Z");
        testFloorMI("1970-01-01T00:00:00.000000Z", "1970-01-01T00:00:00.000000Z");
        testFloorMI("2008-05-12T23:45:51.045045Z", "2008-05-12T23:45:00.000000Z");
        testFloorMI("2025-09-03T23:59:59.999999Z", "2025-09-03T23:59:00.000000Z");
    }

    @Test
    public void testFloorMM() {
        testFloorMM("1961-01-12T23:45:51.123123Z", "1961-01-01T00:00:00.000000Z");
        testFloorMM("1969-01-12T23:45:51.123123Z", "1969-01-01T00:00:00.000000Z");
        testFloorMM("1969-06-15T01:01:00.345345Z", "1969-06-01T00:00:00.000000Z");
        testFloorMM("1970-01-12T23:45:51.123123Z", "1970-01-01T00:00:00.000000Z");
        testFloorMM("1970-02-12T23:45:51.045045Z", "1970-02-01T00:00:00.000000Z");
        testFloorMM("1970-11-12T23:45:51.123123Z", "1970-11-01T00:00:00.000000Z");
        testFloorMM("2008-05-12T23:45:51.045045Z", "2008-05-01T00:00:00.000000Z");
        testFloorMM("2022-02-22T20:18:30.283283Z", "2022-02-01T00:00:00.000000Z");
    }

    @Test
    public void testFloorMMEpoch() {
        long micros = MicrosFormatUtils.parseTimestamp("1969-10-12T23:45:51.045045Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorMM(micros, 2));
        TestUtils.assertEquals("1969-09-01T00:00:00.000000Z", sink);
        sink.clear();
        micros = MicrosFormatUtils.parseTimestamp("1969-11-12T23:45:51.045045Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorMM(micros, 3));
        TestUtils.assertEquals("1969-10-01T00:00:00.000000Z", sink);
        sink.clear();
        micros = MicrosFormatUtils.parseTimestamp("1969-05-12T23:45:51.045045Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorMM(micros, 5));
        TestUtils.assertEquals("1969-03-01T00:00:00.000000Z", sink);
        sink.clear();
        micros = MicrosFormatUtils.parseTimestamp("1970-10-12T23:45:51.045045Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorMM(micros, 3));
        TestUtils.assertEquals("1970-10-01T00:00:00.000000Z", sink);
        sink.clear();
        micros = MicrosFormatUtils.parseTimestamp("1970-11-12T23:45:51.045045Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorMM(micros, 2));
        TestUtils.assertEquals("1970-11-01T00:00:00.000000Z", sink);
        sink.clear();
        micros = MicrosFormatUtils.parseTimestamp("1970-02-12T23:45:51.045045Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorMM(micros, 3));
        TestUtils.assertEquals("1970-01-01T00:00:00.000000Z", sink);
    }

    @Test
    public void testFloorMS() {
        testFloorMS("1969-12-31T23:59:59.999999Z", "1969-12-31T23:59:59.999000Z");
        testFloorMS("1969-01-01T00:00:00.000000Z", "1969-01-01T00:00:00.000000Z");
        testFloorMS("1969-01-01T12:13:14.567567Z", "1969-01-01T12:13:14.567000Z");
        testFloorMS("1970-01-01T00:00:00.000000Z", "1970-01-01T00:00:00.000000Z");
        testFloorMS("2008-05-12T23:45:51.045045Z", "2008-05-12T23:45:51.045000Z");
        testFloorMS("2025-09-03T23:59:59.999999Z", "2025-09-03T23:59:59.999000Z");
    }

    @Test
    public void testFloorNS() {
        testFloorNS("1969-12-31T23:59:59.999999Z", "1969-12-31T23:59:59.999999Z");
        testFloorNS("1969-01-01T00:00:00.000000Z", "1969-01-01T00:00:00.000000Z");
        testFloorNS("1969-01-01T12:13:14.567567Z", "1969-01-01T12:13:14.567567Z");
        testFloorNS("1970-01-01T00:00:00.000000Z", "1970-01-01T00:00:00.000000Z");
        testFloorNS("2008-05-12T23:45:51.045045Z", "2008-05-12T23:45:51.045045Z");
        testFloorNS("2025-09-03T23:59:59.999999Z", "2025-09-03T23:59:59.999999Z");
    }

    @Test
    public void testFloorNSEpoch() {
        long micros = MicrosFormatUtils.parseTimestamp("1969-12-31T23:59:59.999999Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorNS(micros, 3000));
        TestUtils.assertEquals("1969-12-31T23:59:59.999997Z", sink);
        sink.clear();
        micros = MicrosFormatUtils.parseTimestamp("2001-12-31T23:59:59.999999Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorNS(micros, 10000));
        TestUtils.assertEquals("2001-12-31T23:59:59.999990Z", sink);
    }

    @Test
    public void testFloorSS() {
        testFloorSS("1969-12-31T23:59:59.999999Z", "1969-12-31T23:59:59.000000Z");
        testFloorSS("1969-01-01T00:00:00.000000Z", "1969-01-01T00:00:00.000000Z");
        testFloorSS("1969-01-01T12:13:14.567567Z", "1969-01-01T12:13:14.000000Z");
        testFloorSS("1970-01-01T00:00:00.000000Z", "1970-01-01T00:00:00.000000Z");
        testFloorSS("2008-05-12T23:45:51.045045Z", "2008-05-12T23:45:51.000000Z");
        testFloorSS("2025-09-03T23:59:59.999999Z", "2025-09-03T23:59:59.000000Z");
    }

    @Test
    public void testFloorWW() {
        testFloorWW("1969-01-01T00:00:00.000000Z", "1968-12-30T00:00:00.000000Z");
        testFloorWW("1970-01-01T00:00:00.000000Z", "1969-12-29T00:00:00.000000Z");
        testFloorWW("2025-01-02T23:59:59.999999Z", "2024-12-30T00:00:00.000000Z");
        testFloorWW("2025-09-01T00:00:00.000000Z", "2025-09-01T00:00:00.000000Z");
        testFloorWW("2025-09-02T13:59:59.999999Z", "2025-09-01T00:00:00.000000Z");
    }

    @Test
    public void testFloorWithStride() {
        //postgresql: SELECT date_bin('2 weeks', '2021-09-09 22:44:56.784123Z', '1970-01-01 00:00:00.000000');
        sink.clear();
        long micros = MicrosFormatUtils.parseTimestamp("2021-09-09T22:44:56.784123Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorWW(micros, 2));
        TestUtils.assertEquals("2021-08-30T00:00:00.000000Z", sink); // -3 days adjustment
        sink.clear();
        micros = MicrosFormatUtils.parseTimestamp("2021-09-09T22:44:56.784123Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorDD(micros, 12));
        TestUtils.assertEquals("2021-09-06T00:00:00.000000Z", sink);
        sink.clear();
        micros = MicrosFormatUtils.parseTimestamp("2021-09-09T22:44:56.784123Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorHH(micros, 5));
        TestUtils.assertEquals("2021-09-09T19:00:00.000000Z", sink);
        sink.clear();
        micros = MicrosFormatUtils.parseTimestamp("2021-09-09T22:44:56.784123Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorMI(micros, 73));
        TestUtils.assertEquals("2021-09-09T22:18:00.000000Z", sink);
        sink.clear();
        micros = MicrosFormatUtils.parseTimestamp("2021-09-09T22:44:56.789Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorSS(micros, 40));
        TestUtils.assertEquals("2021-09-09T22:44:40.000000Z", sink);
        sink.clear();
        micros = MicrosFormatUtils.parseTimestamp("2021-09-09T22:44:56.784123Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorMS(micros, 225));
        TestUtils.assertEquals("2021-09-09T22:44:56.625000Z", sink);
    }

    @Test
    public void testFloorYYYY() {
        testFloorYYYY("1969-01-01T00:00:00.000000Z", "1969-01-01T00:00:00.000000Z");
        testFloorYYYY("1970-01-01T00:00:00.000000Z", "1970-01-01T00:00:00.000000Z");
        testFloorYYYY("2008-05-12T23:45:51.045045Z", "2008-01-01T00:00:00.000000Z");
        testFloorYYYY("2025-12-31T23:59:59.999999Z", "2025-01-01T00:00:00.000000Z");
    }

    @Test
    public void testFloorYYYYEpoch() {
        long micros;
        micros = MicrosFormatUtils.parseTimestamp("1968-05-12T23:45:51.045045Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorYYYY(micros, 3));
        TestUtils.assertEquals("1967-01-01T00:00:00.000000Z", sink);
        sink.clear();
        micros = MicrosFormatUtils.parseTimestamp("1975-05-12T23:45:51.045045Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorYYYY(micros, 3));
        TestUtils.assertEquals("1973-01-01T00:00:00.000000Z", sink);
        sink.clear();
        micros = MicrosFormatUtils.parseTimestamp("1975-05-12T23:45:51.045045Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorYYYY(micros, 3));
        TestUtils.assertEquals("1973-01-01T00:00:00.000000Z", sink);
        sink.clear();
        micros = MicrosFormatUtils.parseTimestamp("1970-05-12T23:45:51.045045Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorYYYY(micros, 3));
        TestUtils.assertEquals("1970-01-01T00:00:00.000000Z", sink);
        sink.clear();
        micros = MicrosFormatUtils.parseTimestamp("1967-05-12T23:45:51.045045Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorYYYY(micros, 3));
        TestUtils.assertEquals("1967-01-01T00:00:00.000000Z", sink);
    }

    @Test
    public void testFormatDateTime() {
        assertTrue("2014-11-30T12:34:55.332Z");
        assertTrue("2008-03-15T11:22:30.500Z");
        assertTrue("1917-10-01T11:22:30.500Z");
        assertTrue("0900-01-01T01:02:00.005Z");
    }

    @Test
    public void testFormatNanosTz() {
        final long micros = MicrosFormatUtils.parseDateTime("2008-05-10T12:31:02.008998991+01:00");
        MicrosFormatUtils.USEC_UTC_FORMAT.format(micros, DateLocaleFactory.INSTANCE.getLocale("en"), null, sink);
        TestUtils.assertEquals("2008-05-10T11:31:02.008998", sink);
    }

    @Test
    public void testFormatNanosZ() {
        final long micros = MicrosFormatUtils.parseDateTime("2008-05-10T12:31:02.008998991Z");
        MicrosFormatUtils.USEC_UTC_FORMAT.format(micros, DateLocaleFactory.INSTANCE.getLocale("en"), null, sink);
        TestUtils.assertEquals("2008-05-10T12:31:02.008998", sink);
    }

    @Test
    public void testGetDayOfTheWeekOfEndOfYear() {
        Assert.assertEquals(0, Micros.getDayOfTheWeekOfEndOfYear(2017));
        Assert.assertEquals(1, Micros.getDayOfTheWeekOfEndOfYear(1984));
        Assert.assertEquals(2, Micros.getDayOfTheWeekOfEndOfYear(2019));
        Assert.assertEquals(3, Micros.getDayOfTheWeekOfEndOfYear(2014));
        Assert.assertEquals(4, Micros.getDayOfTheWeekOfEndOfYear(2020));
        Assert.assertEquals(5, Micros.getDayOfTheWeekOfEndOfYear(2021));
        Assert.assertEquals(6, Micros.getDayOfTheWeekOfEndOfYear(1994));
    }

    @Test
    public void testGetIsoYearDayOffset() {
        Assert.assertEquals(-3, CommonUtils.getIsoYearDayOffset(2015));
        Assert.assertEquals(3, CommonUtils.getIsoYearDayOffset(2016));
        Assert.assertEquals(1, CommonUtils.getIsoYearDayOffset(2017));
        Assert.assertEquals(0, CommonUtils.getIsoYearDayOffset(2018));
        Assert.assertEquals(-1, CommonUtils.getIsoYearDayOffset(2019));
        Assert.assertEquals(-2, CommonUtils.getIsoYearDayOffset(2020));
        Assert.assertEquals(3, CommonUtils.getIsoYearDayOffset(2021));
        Assert.assertEquals(2, CommonUtils.getIsoYearDayOffset(2022));
    }

    @Test
    public void testGetWeek() {
        long micros = MicrosFormatUtils.parseTimestamp("2017-12-31T13:32:12.531Z");
        Assert.assertEquals(52, Micros.getWeek(micros));
        micros = MicrosFormatUtils.parseTimestamp("2018-01-01T03:32:12.531Z");
        Assert.assertEquals(1, Micros.getWeek(micros));
        micros = MicrosFormatUtils.parseTimestamp("2020-12-20T13:32:12.531Z");
        Assert.assertEquals(51, Micros.getWeek(micros));
        micros = MicrosFormatUtils.parseTimestamp("2020-12-23T13:32:12.531Z");
        Assert.assertEquals(52, Micros.getWeek(micros));
        micros = MicrosFormatUtils.parseTimestamp("2021-01-01T13:32:12.531Z");
        Assert.assertEquals(53, Micros.getWeek(micros));
        micros = MicrosFormatUtils.parseTimestamp("2021-01-04T13:32:12.531Z");
        Assert.assertEquals(1, Micros.getWeek(micros));
        micros = MicrosFormatUtils.parseTimestamp("2022-01-09T13:32:12.531Z");
        Assert.assertEquals(1, Micros.getWeek(micros));
        micros = MicrosFormatUtils.parseTimestamp("2022-01-10T13:32:12.531Z");
        Assert.assertEquals(2, Micros.getWeek(micros));
    }

    @Test
    public void testGetWeeks() {
        Assert.assertEquals(52, CommonUtils.getWeeks(2017));
        Assert.assertEquals(52, CommonUtils.getWeeks(2021));
        Assert.assertEquals(53, CommonUtils.getWeeks(2020));
    }

    @Test
    public void testMonthsBetween() {
        // a < b, same year
        Assert.assertEquals(
                2,
                Micros.getMonthsBetween(
                        MicrosFormatUtils.parseTimestamp("2014-05-12T23:45:51.045Z"),
                        MicrosFormatUtils.parseTimestamp("2014-07-15T23:45:51.045Z")
                )
        );

        // a > b, same year
        Assert.assertEquals(
                2,
                Micros.getMonthsBetween(
                        MicrosFormatUtils.parseTimestamp("2014-07-15T23:45:51.045Z"),
                        MicrosFormatUtils.parseTimestamp("2014-05-12T23:45:51.045Z")
                )
        );

        // a < b, different year
        Assert.assertEquals(
                26,
                Micros.getMonthsBetween(
                        MicrosFormatUtils.parseTimestamp("2014-05-12T23:45:51.045Z"),
                        MicrosFormatUtils.parseTimestamp("2016-07-15T23:45:51.045Z")
                )
        );

        // a < b, same year, a has higher residuals
        Assert.assertEquals(
                1,
                Micros.getMonthsBetween(
                        MicrosFormatUtils.parseTimestamp("2014-05-12T23:45:51.045Z"),
                        MicrosFormatUtils.parseTimestamp("2014-07-03T23:45:51.045Z")
                )
        );

        // a < b, a before epoch, a has higher residuals
        Assert.assertEquals(
                109 * 12 + 1,
                Micros.getMonthsBetween(
                        MicrosFormatUtils.parseTimestamp("1905-05-12T23:45:51.045Z"),
                        MicrosFormatUtils.parseTimestamp("2014-07-03T23:45:51.045Z")
                )
        );
    }

    @Test
    public void testNExtOrSameDow3() {
        // thursday
        long micros = MicrosFormatUtils.parseTimestamp("2017-04-06T00:00:00.000000Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.nextOrSameDayOfWeek(micros, 4));
        TestUtils.assertEquals("2017-04-06T00:00:00.000000Z", sink);
    }

    @Test
    public void testNextDSTFixed() throws NumericException {
        String tz = "GMT";
        TimeZoneRules rules = EN_LOCALE.getZoneRules(
                Numbers.decodeLowInt(EN_LOCALE.matchZone(tz, 0, tz.length())),
                RESOLUTION_MICROS
        );

        final long ts = rules.getNextDST(MicrosFormatUtils.parseTimestamp("2021-02-10T01:00:00.000000Z"));
        Assert.assertEquals(Long.MAX_VALUE, ts);
    }

    @Test
    public void testNextDSTHistory() throws NumericException {
        String tz = "Europe/Berlin";
        TimeZoneRules rules = EN_LOCALE.getZoneRules(
                Numbers.decodeLowInt(EN_LOCALE.matchZone(tz, 0, tz.length())),
                RESOLUTION_MICROS
        );

        final long ts = rules.getNextDST(MicrosFormatUtils.parseTimestamp("1991-09-20T01:00:00.000000Z"));
        Assert.assertEquals("1991-09-29T01:00:00.000Z", Micros.toString(ts));
    }

    @Test
    public void testNextDSTHistoryLast() throws NumericException {
        String tz = "Europe/Berlin";
        TimeZoneRules rules = EN_LOCALE.getZoneRules(
                Numbers.decodeLowInt(EN_LOCALE.matchZone(tz, 0, tz.length())),
                RESOLUTION_MICROS
        );

        final long ts = rules.getNextDST(MicrosFormatUtils.parseTimestamp("1997-10-26T01:00:00.000000Z"));
        Assert.assertEquals("1998-03-29T01:00:00.000Z", Micros.toString(ts));
    }

    @Test
    public void testNextDSTRulesAfterFirst() throws NumericException {
        String tz = "Europe/Berlin";
        TimeZoneRules rules = EN_LOCALE.getZoneRules(
                Numbers.decodeLowInt(EN_LOCALE.matchZone(tz, 0, tz.length())),
                RESOLUTION_MICROS
        );

        final long ts = rules.getNextDST(MicrosFormatUtils.parseTimestamp("2021-07-03T01:00:00.000000Z"));
        Assert.assertEquals("2021-10-31T01:00:00.000Z", Micros.toString(ts));
    }

    @Test
    public void testNextDSTRulesAfterLast() throws NumericException {
        String tz = "Europe/Berlin";
        TimeZoneRules rules = EN_LOCALE.getZoneRules(
                Numbers.decodeLowInt(EN_LOCALE.matchZone(tz, 0, tz.length())),
                RESOLUTION_MICROS
        );

        final long ts = rules.getNextDST(MicrosFormatUtils.parseTimestamp("2021-10-31T01:00:00.000000Z"));
        Assert.assertEquals("2022-03-27T01:00:00.000Z", Micros.toString(ts));
    }

    @Test
    public void testNextDSTRulesBeforeFirst() throws NumericException {
        String tz = "Europe/Berlin";
        TimeZoneRules rules = EN_LOCALE.getZoneRules(
                Numbers.decodeLowInt(EN_LOCALE.matchZone(tz, 0, tz.length())),
                RESOLUTION_MICROS
        );

        final long ts = rules.getNextDST(MicrosFormatUtils.parseTimestamp("2021-02-10T01:00:00.000000Z"));
        Assert.assertEquals("2021-03-28T01:00:00.000Z", Micros.toString(ts));
    }

    @Test
    public void testNextOrSameDow1() {
        // thursday
        long micros = MicrosFormatUtils.parseTimestamp("2017-04-06T00:00:00.000000Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.nextOrSameDayOfWeek(micros, 3));
        TestUtils.assertEquals("2017-04-12T00:00:00.000000Z", sink);
    }

    @Test
    public void testNextOrSameDow2() {
        // thursday
        long micros = MicrosFormatUtils.parseTimestamp("2017-04-06T00:00:00.000000Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.nextOrSameDayOfWeek(micros, 6));
        TestUtils.assertEquals("2017-04-08T00:00:00.000000Z", sink);
    }

    @Test
    public void testOverflowDate() {
        Assert.assertEquals("6477-07-27T03:15:50.400Z", Micros.toString(142245170150400000L));
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
        String date = "2008-02-29T10:54:01.010Z";
        MicrosFormatUtils.appendDateTime(sink, MicrosFormatUtils.parseTimestamp(date));
        TestUtils.assertEquals(date, sink);
    }

    @Test
    public void testParseDateTimePrevEpoch() {
        String date = "1812-02-29T10:54:01.010Z";
        MicrosFormatUtils.appendDateTime(sink, MicrosFormatUtils.parseTimestamp(date));
        TestUtils.assertEquals(date, sink);
    }

    @Test
    public void testParseHttp() throws NumericException {
        Assert.assertEquals(1744545248000000L, parseHTTP("Sun, 13 Apr 2025 11:54:08 GMT"));
        Assert.assertEquals(1744545248000000L, parseHTTP("Sun, 13-Apr-2025 11:54:08 GMT"));
        Assert.assertEquals(1741375399000000L, parseHTTP("Fri, 07 Mar 2025 19:23:19 GMT"));
        Assert.assertEquals(1741375399000000L, parseHTTP("Fri, 07-Mar-2025 19:23:19 GMT"));
        Assert.assertEquals(1741375399000000L, parseHTTP("Fri, 7 Mar 2025 19:23:19 GMT"));
        Assert.assertEquals(1741375399000000L, parseHTTP("Fri, 7-Mar-2025 19:23:19 GMT"));
    }

    @Test
    public void testParseTimestampNotNullLocale() {
        try {
            // we deliberately mangle timezone so that function begins to rely on locale to resolve text
            MicrosFormatUtils.parseTimestamp("2020-01-10T15:00:01.000143Zz");
            Assert.fail();
        } catch (NumericException ignored) {
        }
    }

    @Test
    public void testParseWW() throws NumericException {
        DateFormat byWeek = getPartitionDirFormatMethod(ColumnType.TIMESTAMP, PartitionBy.WEEK);
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

        Assert.assertEquals("2019-12-30T00:00:00.000Z", Micros.toString(byWeek.parse("2020-W01", EN_LOCALE)));
        Assert.assertEquals("2020-12-28T00:00:00.000Z", Micros.toString(byWeek.parse("2020-W53", EN_LOCALE)));
        Assert.assertEquals("2021-01-04T00:00:00.000Z", Micros.toString(byWeek.parse("2021-W01", EN_LOCALE)));

        try {
            byWeek.parse("2019-W53", EN_LOCALE);
            Assert.fail("2019 has 52 ISO weeks");
        } catch (NumericException ignore) {
        }

        Assert.assertEquals("2019-12-30T00:00:00.000Z", Micros.toString(byWeek.parse("2020-W01", EN_LOCALE)));
        Assert.assertEquals("2014-12-22T00:00:00.000Z", Micros.toString(byWeek.parse("2014-W52", EN_LOCALE)));
        Assert.assertEquals("2015-12-28T00:00:00.000Z", Micros.toString(byWeek.parse("2015-W53", EN_LOCALE)));
    }

    @Test(expected = NumericException.class)
    public void testParseWrongDay() {
        MicrosFormatUtils.parseTimestamp("2013-09-31T00:00:00.000Z");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongHour() {
        MicrosFormatUtils.parseTimestamp("2013-09-30T25:00:00.000Z");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongMicros() {
        MicrosFormatUtils.parseTimestamp("2013-09-30T22:04:34.1024091Z");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongMinute() {
        MicrosFormatUtils.parseTimestamp("2013-09-30T22:61:00.000Z");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongMonth() {
        MicrosFormatUtils.parseTimestamp("2013-00-12T00:00:00.000Z");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongSecond() {
        MicrosFormatUtils.parseTimestamp("2013-09-30T22:04:60.000Z");
    }

    @Test
    public void testPreviousOrSameDow1() {
        // thursday
        long micros = MicrosFormatUtils.parseTimestamp("2017-04-06T00:00:00.000000Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.previousOrSameDayOfWeek(micros, 3));
        TestUtils.assertEquals("2017-04-05T00:00:00.000000Z", sink);
    }

    @Test
    public void testPreviousOrSameDow2() {
        // thursday
        long micros = MicrosFormatUtils.parseTimestamp("2017-04-06T00:00:00.000000Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.previousOrSameDayOfWeek(micros, 6));
        TestUtils.assertEquals("2017-04-01T00:00:00.000000Z", sink);
    }

    @Test
    public void testPreviousOrSameDow3() {
        // thursday
        long micros = MicrosFormatUtils.parseTimestamp("2017-04-06T00:00:00.000000Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.previousOrSameDayOfWeek(micros, 4));
        TestUtils.assertEquals("2017-04-06T00:00:00.000000Z", sink);
    }

    @Test(expected = NumericException.class)
    public void testToTimezoneInvalidTimezoneName() throws NumericException {
        DateLocale locale = DateLocaleFactory.INSTANCE.getLocale("en");
        long micros = MicrosFormatUtils.parseTimestamp("2019-12-10T10:00:00.000000Z");
        Micros.toTimezone(micros, locale, "Somewhere");
    }

    @Test
    public void testToTimezoneWithHours() throws NumericException {
        DateLocale locale = DateLocaleFactory.INSTANCE.getLocale("en");
        long micros = MicrosFormatUtils.parseTimestamp("2019-12-10T10:00:00.000000Z");
        long offsetMicros = Micros.toTimezone(micros, locale, "+03:45");
        TestUtils.assertEquals("2019-12-10T13:45:00.000Z", Micros.toString(offsetMicros));
    }

    @Test
    public void testToTimezoneWithHoursInString() throws NumericException {
        DateLocale locale = DateLocaleFactory.INSTANCE.getLocale("en");
        long micros = MicrosFormatUtils.parseTimestamp("2019-12-10T10:00:00.000000Z");
        long offsetMicros = Micros.toTimezone(micros, locale, "hello +03:45 there", 6, 12);
        TestUtils.assertEquals("2019-12-10T13:45:00.000Z", Micros.toString(offsetMicros));
    }

    @Test
    public void testToTimezoneWithTimezoneName() throws NumericException {
        DateLocale locale = DateLocaleFactory.INSTANCE.getLocale("en");
        long micros = MicrosFormatUtils.parseTimestamp("2019-12-10T10:00:00.000000Z");
        long offsetMicros = Micros.toTimezone(micros, locale, "Europe/Prague");
        TestUtils.assertEquals("2019-12-10T11:00:00.000Z", Micros.toString(offsetMicros));
    }

    @Test(expected = NumericException.class)
    public void testToUTCInvalidTimezoneName() throws NumericException {
        DateLocale locale = DateLocaleFactory.INSTANCE.getLocale("en");
        long micros = MicrosFormatUtils.parseTimestamp("2019-12-10T10:00:00.000000Z");
        Micros.toUTC(micros, locale, "Somewhere");
    }

    @Test
    public void testToUTCWithHours() throws NumericException {
        DateLocale locale = DateLocaleFactory.INSTANCE.getLocale("en");
        long micros = MicrosFormatUtils.parseTimestamp("2019-12-10T10:00:00.000000Z");
        long offsetMicros = Micros.toUTC(micros, locale, "+03:45");
        TestUtils.assertEquals("2019-12-10T06:15:00.000Z", Micros.toString(offsetMicros));
    }

    @Test
    public void testToUTCWithHoursInString() throws NumericException {
        DateLocale locale = DateLocaleFactory.INSTANCE.getLocale("en");
        long micros = MicrosFormatUtils.parseTimestamp("2019-12-10T10:00:00.000000Z");
        long offsetMicros = Micros.toUTC(micros, locale, "hello +03:45 there", 6, 12);
        TestUtils.assertEquals("2019-12-10T06:15:00.000Z", Micros.toString(offsetMicros));
    }

    @Test
    public void testToUTCWithTimezoneName() throws NumericException {
        DateLocale locale = DateLocaleFactory.INSTANCE.getLocale("en");
        long micros = MicrosFormatUtils.parseTimestamp("2019-12-10T10:00:00.000000Z");
        long offsetMicros = Micros.toUTC(micros, locale, "Europe/Prague");
        TestUtils.assertEquals("2019-12-10T09:00:00.000Z", Micros.toString(offsetMicros));
    }

    @Test
    public void testWeekOfMonth() {
        long micros = MicrosFormatUtils.parseTimestamp("2020-01-01T17:16:30.192Z");
        Assert.assertEquals(1, Micros.getWeekOfMonth(micros));
        micros = MicrosFormatUtils.parseTimestamp("2019-03-10T07:16:30.192Z");
        Assert.assertEquals(2, Micros.getWeekOfMonth(micros));
        micros = MicrosFormatUtils.parseTimestamp("2020-12-31T12:00:00.000Z");
        Assert.assertEquals(5, Micros.getWeekOfMonth(micros));
        micros = MicrosFormatUtils.parseTimestamp("2021-12-31T12:00:00.000Z");
        Assert.assertEquals(5, Micros.getWeekOfMonth(micros));
    }

    @Test
    public void testWeekOfYear() {
        long micros = MicrosFormatUtils.parseTimestamp("2020-01-01T17:16:30.192Z");
        Assert.assertEquals(1, Micros.getWeekOfYear(micros));
        micros = MicrosFormatUtils.parseTimestamp("2019-03-10T07:16:30.192Z");
        Assert.assertEquals(10, Micros.getWeekOfYear(micros));
        micros = MicrosFormatUtils.parseTimestamp("2020-03-10T07:16:30.192Z");
        Assert.assertEquals(11, Micros.getWeekOfYear(micros));
        micros = MicrosFormatUtils.parseTimestamp("1893-03-19T17:16:30.192Z");
        Assert.assertEquals(12, Micros.getWeekOfYear(micros));
        micros = MicrosFormatUtils.parseTimestamp("2020-12-31T12:00:00.000Z");
        Assert.assertEquals(53, Micros.getWeekOfYear(micros));
        micros = MicrosFormatUtils.parseTimestamp("2021-12-31T12:00:00.000Z");
        Assert.assertEquals(53, Micros.getWeekOfYear(micros));
    }

    @Test
    public void testYearsBetween() {
        Assert.assertEquals(
                112,
                Micros.getYearsBetween(
                        MicrosFormatUtils.parseTimestamp("1904-11-05T23:45:41.045Z"),
                        MicrosFormatUtils.parseTimestamp("2017-07-24T23:45:31.045Z")
                )
        );

        Assert.assertEquals(
                113,
                Micros.getYearsBetween(
                        MicrosFormatUtils.parseTimestamp("1904-11-05T23:45:41.045Z"),
                        MicrosFormatUtils.parseTimestamp("2017-12-24T23:45:51.045Z")
                )
        );
    }

    @Test
    public void testYearsBetween2() throws NumericException {
        long micros1 = MicrosFormatUtils.parseTimestamp("2020-04-24T01:49:12.005Z");
        long micros2 = MicrosFormatUtils.parseTimestamp("2025-04-24T01:49:12.005Z");
        Assert.assertEquals(5, Micros.getYearsBetween(micros1, micros2));
        Assert.assertEquals(5, Micros.getYearsBetween(micros2, micros1));
    }

    @Test
    public void testYearsBetween3() throws NumericException {
        long micros1 = MicrosFormatUtils.parseTimestamp("2020-04-24T01:49:12.005Z");
        long micros2 = MicrosFormatUtils.parseTimestamp("2024-04-24T01:49:12.005Z");
        Assert.assertEquals(4, Micros.getYearsBetween(micros1, micros2));
        Assert.assertEquals(4, Micros.getYearsBetween(micros2, micros1));
    }

    private void assertTrue(String date) throws NumericException {
        MicrosFormatUtils.appendDateTime(sink, MicrosFormatUtils.parseTimestamp(date));
        TestUtils.assertEquals(date, sink);
        sink.clear();
    }

    private void expectExceptionDateTime(String s) {
        try {
            MicrosFormatUtils.parseTimestamp(s);
            Assert.fail("Expected exception");
        } catch (NumericException ignore) {
        }
    }

    private void testFloorDD(String timestamp, String expected) {
        sink.clear();
        final long micros = MicrosFormatUtils.parseTimestamp(timestamp);
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorDD(micros));
        TestUtils.assertEquals(expected, sink);
        sink.clear();
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorDD(micros, 1));
        TestUtils.assertEquals(expected, sink);
        sink.clear();
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorDD(micros, 1, 0));
        TestUtils.assertEquals(expected, sink);
    }

    private void testFloorHH(String timestamp, String expected) {
        sink.clear();
        final long micros = MicrosFormatUtils.parseTimestamp(timestamp);
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorHH(micros));
        TestUtils.assertEquals(expected, sink);
        sink.clear();
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorHH(micros, 1));
        TestUtils.assertEquals(expected, sink);
        sink.clear();
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorHH(micros, 1, 0));
        TestUtils.assertEquals(expected, sink);
    }

    private void testFloorMC(String timestamp, String expected) {
        sink.clear();
        final long micros = MicrosFormatUtils.parseTimestamp(timestamp);
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorMC(micros));
        TestUtils.assertEquals(expected, sink);
        sink.clear();
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorMC(micros, 1));
        TestUtils.assertEquals(expected, sink);
        sink.clear();
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorMC(micros, 1, 0));
        TestUtils.assertEquals(expected, sink);
    }

    private void testFloorMI(String timestamp, String expected) {
        sink.clear();
        final long micros = MicrosFormatUtils.parseTimestamp(timestamp);
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorMI(micros));
        TestUtils.assertEquals(expected, sink);
        sink.clear();
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorMI(micros, 1));
        TestUtils.assertEquals(expected, sink);
        sink.clear();
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorMI(micros, 1, 0));
        TestUtils.assertEquals(expected, sink);
    }

    private void testFloorMM(String timestamp, String expected) {
        sink.clear();
        final long micros = MicrosFormatUtils.parseTimestamp(timestamp);
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorMM(micros));
        TestUtils.assertEquals(expected, sink);
        sink.clear();
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorMM(micros, 1));
        TestUtils.assertEquals(expected, sink);
        sink.clear();
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorMM(micros, 1, 0));
        TestUtils.assertEquals(expected, sink);
    }

    private void testFloorMS(String timestamp, String expected) {
        sink.clear();
        final long micros = MicrosFormatUtils.parseTimestamp(timestamp);
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorMS(micros));
        TestUtils.assertEquals(expected, sink);
        sink.clear();
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorMS(micros, 1));
        TestUtils.assertEquals(expected, sink);
        sink.clear();
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorMS(micros, 1, 0));
        TestUtils.assertEquals(expected, sink);
    }

    private void testFloorNS(String timestamp, String expected) {
        sink.clear();
        final long micros = MicrosFormatUtils.parseTimestamp(timestamp);
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorNS(micros));
        TestUtils.assertEquals(expected, sink);
        sink.clear();
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorNS(micros, 1));
        TestUtils.assertEquals(expected, sink);
        sink.clear();
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorNS(micros, 1, 0));
        TestUtils.assertEquals(expected, sink);
    }

    private void testFloorSS(String timestamp, String expected) {
        sink.clear();
        final long micros = MicrosFormatUtils.parseTimestamp(timestamp);
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorSS(micros));
        TestUtils.assertEquals(expected, sink);
        sink.clear();
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorSS(micros, 1));
        TestUtils.assertEquals(expected, sink);
        sink.clear();
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorSS(micros, 1, 0));
        TestUtils.assertEquals(expected, sink);
    }

    private void testFloorWW(String timestamp, String expected) {
        sink.clear();
        final long micros = MicrosFormatUtils.parseTimestamp(timestamp);
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorWW(micros));
        TestUtils.assertEquals(expected, sink);
        sink.clear();
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorWW(micros, 1));
        TestUtils.assertEquals(expected, sink);
        sink.clear();
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorWW(micros, 1, 0));
        TestUtils.assertEquals(expected, sink);
    }

    private void testFloorYYYY(String timestamp, String expected) {
        sink.clear();
        final long micros = MicrosFormatUtils.parseTimestamp(timestamp);
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorYYYY(micros));
        TestUtils.assertEquals(expected, sink);
        sink.clear();
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorYYYY(micros, 1));
        TestUtils.assertEquals(expected, sink);
        sink.clear();
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorYYYY(micros, 1, 0));
        TestUtils.assertEquals(expected, sink);
    }
}

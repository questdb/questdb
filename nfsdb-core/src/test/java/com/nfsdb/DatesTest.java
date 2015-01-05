/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb;

import com.nfsdb.export.StringSink;
import com.nfsdb.utils.Dates;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DatesTest {

    private final StringSink sink = new StringSink();

    @Before
    public void setUp() throws Exception {
        sink.clear();
    }

    @Test
    public void testFormatDateTime() throws Exception {
        assertTrue("2014-11-30T12:34:55.332Z");
        assertTrue("2008-03-15T11:22:30.500Z");
        assertTrue("1917-10-01T11:22:30.500Z");
        assertTrue("0900-01-01T01:02:00.005Z");
    }

    @Test
    public void testFormatCalDate1() throws Exception {
        Dates.formatDashYYYYMMDD(sink, Dates.parseDateTime("2008-05-10T12:31:02.008Z"));
        Assert.assertEquals("2008-05-10", sink.toString());
    }

    @Test
    public void testFormatCalDate2() throws Exception {
        Dates.formatYYYYMM(sink, Dates.parseDateTime("2008-05-10T12:31:02.008Z"));
        Assert.assertEquals("2008-05", sink.toString());
    }

    @Test
    public void testFormatCalDate3() throws Exception {
        Dates.formatYYYYMMDD(sink, Dates.parseDateTime("2008-05-10T12:31:02.008Z"));
        Assert.assertEquals("20080510", sink.toString());
    }

    private void assertTrue(String date) {
        Dates.appendDateTime(sink, Dates.parseDateTime(date));
        Assert.assertEquals(date, sink.toString());
        sink.clear();
    }

    @Test
    public void testParseDateTime() throws Exception {
        String date = "2008-02-29T10:54:01.010Z";
        Dates.appendDateTime(sink, Dates.parseDateTime(date));
        Assert.assertEquals(date, sink.toString());
    }

    @Test
    public void testParseDateTimePrevEpoch() throws Exception {
        String date = "1812-02-29T10:54:01.010Z";
        Dates.appendDateTime(sink, Dates.parseDateTime(date));
        Assert.assertEquals(date, sink.toString());
    }

    @Test(expected = NumberFormatException.class)
    public void testParseWrongMonth() throws Exception {
        Dates.parseDateTime("2013-00-12T00:00:00.000Z");
    }

    @Test(expected = NumberFormatException.class)
    public void testParseWrongDay() throws Exception {
        Dates.parseDateTime("2013-09-31T00:00:00.000Z");
    }

    @Test(expected = NumberFormatException.class)
    public void testParseWrongHour() throws Exception {
        Dates.parseDateTime("2013-09-30T24:00:00.000Z");
    }

    @Test(expected = NumberFormatException.class)
    public void testParseWrongMinute() throws Exception {
        Dates.parseDateTime("2013-09-30T22:61:00.000Z");
    }

    @Test(expected = NumberFormatException.class)
    public void testParseWrongSecond() throws Exception {
        Dates.parseDateTime("2013-09-30T22:04:60.000Z");
    }

    @Test(expected = NumberFormatException.class)
    public void testParseWrongMillis() throws Exception {
        Dates.parseDateTime("2013-09-30T22:04:34.1024Z");
    }

    @Test
    public void testFloorYYYY() throws Exception {
        long millis = Dates.parseDateTime("2008-05-12T23:45:51.045Z");
        Dates.appendDateTime(sink, Dates.floorYYYY(millis));
        Assert.assertEquals("2008-01-01T00:00:00.000Z", sink.toString());
    }

    @Test
    public void testFloorMM() throws Exception {
        long millis = Dates.parseDateTime("2008-05-12T23:45:51.045Z");
        Dates.appendDateTime(sink, Dates.floorMM(millis));
        Assert.assertEquals("2008-05-01T00:00:00.000Z", sink.toString());
    }

    @Test
    public void testFloorDD() throws Exception {
        long millis = Dates.parseDateTime("2008-05-12T23:45:51.045Z");
        Dates.appendDateTime(sink, Dates.floorDD(millis));
        Assert.assertEquals("2008-05-12T00:00:00.000Z", sink.toString());
    }

    @Test
    public void testFloorHH() throws Exception {
        long millis = Dates.parseDateTime("2008-05-12T23:45:51.045Z");
        Dates.appendDateTime(sink, Dates.floorHH(millis));
        Assert.assertEquals("2008-05-12T23:00:00.000Z", sink.toString());
    }

    @Test
    public void testCeilYYYY() throws Exception {
        long millis = Dates.parseDateTime("2008-05-12T23:45:51.045Z");
        Dates.appendDateTime(sink, Dates.ceilYYYY(millis));
        Assert.assertEquals("2008-12-31T23:59:59.999Z", sink.toString());
    }

    @Test
    public void testCeilMM() throws Exception {
        long millis = Dates.parseDateTime("2008-05-12T23:45:51.045Z");
        Dates.appendDateTime(sink, Dates.ceilMM(millis));
        Assert.assertEquals("2008-05-31T23:59:59.999Z", sink.toString());
    }

    @Test
    public void testCeilDD() throws Exception {
        long millis = Dates.parseDateTime("2008-05-12T23:45:51.045Z");
        Dates.appendDateTime(sink, Dates.ceilDD(millis));
        Assert.assertEquals("2008-05-12T23:59:59.999Z", sink.toString());
    }

    @Test
    public void testCeilDDPrevEpoch() throws Exception {
        long millis = Dates.parseDateTime("1888-05-12T23:45:51.045Z");
        Dates.appendDateTime(sink, Dates.ceilDD(millis));
        Assert.assertEquals("1888-05-12T23:59:59.999Z", sink.toString());
    }

    @Test
    public void testAddMonths() throws Exception {
        long millis = Dates.parseDateTime("2008-05-12T23:45:51.045Z");
        Dates.appendDateTime(sink, Dates.addMonths(millis, -10));
        Assert.assertEquals("2007-07-12T23:45:51.045Z", sink.toString());
    }

    @Test
    public void testAddMonthsPrevEpoch() throws Exception {
        long millis = Dates.parseDateTime("1888-05-12T23:45:51.045Z");
        Dates.appendDateTime(sink, Dates.addMonths(millis, -10));
        Assert.assertEquals("1887-07-12T23:45:51.045Z", sink.toString());
    }

    @Test
    public void testAddYears() throws Exception {
        long millis = Dates.parseDateTime("1988-05-12T23:45:51.045Z");
        Dates.appendDateTime(sink, Dates.addYear(millis, 10));
        Assert.assertEquals("1998-05-12T23:45:51.045Z", sink.toString());
    }

    @Test
    public void testAddYearsPrevEpoch() throws Exception {
        long millis = Dates.parseDateTime("1888-05-12T23:45:51.045Z");
        Dates.appendDateTime(sink, Dates.addYear(millis, 10));
        Assert.assertEquals("1898-05-12T23:45:51.045Z", sink.toString());
    }


    @Test
    public void testAddDaysPrevEpoch() throws Exception {
        long millis = Dates.parseDateTime("1888-05-12T23:45:51.045Z");
        Dates.appendDateTime(sink, Dates.addDays(millis, 24));
        Assert.assertEquals("1888-06-05T23:45:51.045Z", sink.toString());
    }
}

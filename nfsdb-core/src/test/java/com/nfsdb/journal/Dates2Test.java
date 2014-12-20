/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.journal;

import com.nfsdb.journal.test.tools.TestCharSink;
import com.nfsdb.journal.utils.Dates;
import com.nfsdb.journal.utils.Dates2;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class Dates2Test {

    private final TestCharSink sink = new TestCharSink();

    @Before
    public void setUp() throws Exception {
        sink.clear();
    }

    @Test
    public void testFormatDateTime() throws Exception {
        assertTrue("2014-11-30T12:34:55.332Z");
        assertTrue("2008-03-15T11:22:30.500Z");
        assertTrue("1917-10-01T11:22:30.500Z");
        assertTrue("900-01-01T01:02:00.005Z");
    }

    @Test
    public void testFormatCalDate1() throws Exception {
        Dates2.appendCalDate1(sink, Dates.toMillis("2008-05-10T12:31:02.008Z"));
        Assert.assertEquals("2008-05-10", sink.toString());
    }

    @Test
    public void testFormatCalDate2() throws Exception {
        Dates2.appendCalDate2(sink, Dates.toMillis("2008-05-10T12:31:02.008Z"));
        Assert.assertEquals("2008-05", sink.toString());
    }

    @Test
    public void testFormatCalDate3() throws Exception {
        Dates2.appendCalDate3(sink, Dates.toMillis("2008-05-10T12:31:02.008Z"));
        Assert.assertEquals("20080510", sink.toString());
    }

    private void assertTrue(String date) {
        Dates2.appendDateTime(sink, Dates.toMillis(date));
        Assert.assertEquals(date, sink.toString());
        sink.clear();
    }

    @Test
    public void testParseDateTime() throws Exception {
        String date = "2008-02-29T10:54:01.010Z";
        Dates2.appendDateTime(sink, Dates2.parseDateTime(date));
        Assert.assertEquals(date, sink.toString());
    }

    @Test
    public void testParseDateTimePrevEpoch() throws Exception {
        String date = "1812-02-29T10:54:01.010Z";
        Dates2.appendDateTime(sink, Dates2.parseDateTime(date));
        Assert.assertEquals(date, sink.toString());
    }

    @Test(expected = NumberFormatException.class)
    public void testParseWrongMonth() throws Exception {
        Dates2.parseDateTime("2013-00-12T00:00:00.000Z");
    }

    @Test(expected = NumberFormatException.class)
    public void testParseWrongDay() throws Exception {
        Dates2.parseDateTime("2013-09-31T00:00:00.000Z");
    }

    @Test(expected = NumberFormatException.class)
    public void testParseWrongHour() throws Exception {
        Dates2.parseDateTime("2013-09-30T24:00:00.000Z");
    }

    @Test(expected = NumberFormatException.class)
    public void testParseWrongMinute() throws Exception {
        Dates2.parseDateTime("2013-09-30T22:61:00.000Z");
    }

    @Test(expected = NumberFormatException.class)
    public void testParseWrongSecond() throws Exception {
        Dates2.parseDateTime("2013-09-30T22:04:60.000Z");
    }

    @Test(expected = NumberFormatException.class)
    public void testParseWrongMillis() throws Exception {
        Dates2.parseDateTime("2013-09-30T22:04:34.1024Z");
    }

    @Test
    public void testFloorYYYY() throws Exception {
        long millis = Dates2.parseDateTime("2008-05-12T23:45:51.045Z");
        Dates2.appendDateTime(sink, Dates2.floorYYYY(millis));
        Assert.assertEquals("2008-01-01T00:00:00.000Z", sink.toString());
    }

    @Test
    public void testFloorMM() throws Exception {
        long millis = Dates2.parseDateTime("2008-05-12T23:45:51.045Z");
        Dates2.appendDateTime(sink, Dates2.floorMM(millis));
        Assert.assertEquals("2008-05-01T00:00:00.000Z", sink.toString());
    }

    @Test
    public void testFloorDD() throws Exception {
        long millis = Dates2.parseDateTime("2008-05-12T23:45:51.045Z");
        Dates2.appendDateTime(sink, Dates2.floorDD(millis));
        Assert.assertEquals("2008-05-12T00:00:00.000Z", sink.toString());
    }

    @Test
    public void testFloorHH() throws Exception {
        long millis = Dates2.parseDateTime("2008-05-12T23:45:51.045Z");
        Dates2.appendDateTime(sink, Dates2.floorHH(millis));
        Assert.assertEquals("2008-05-12T23:00:00.000Z", sink.toString());
    }

    @Test
    public void testCeilYYYY() throws Exception {
        long millis = Dates2.parseDateTime("2008-05-12T23:45:51.045Z");
        Dates2.appendDateTime(sink, Dates2.ceilYYYY(millis));
        Assert.assertEquals("2008-12-31T23:59:59.999Z", sink.toString());
    }

    @Test
    public void testCeilMM() throws Exception {
        long millis = Dates2.parseDateTime("2008-05-12T23:45:51.045Z");
        Dates2.appendDateTime(sink, Dates2.ceilMM(millis));
        Assert.assertEquals("2008-05-31T23:59:59.999Z", sink.toString());
    }

    @Test
    public void testCeilDD() throws Exception {
        long millis = Dates2.parseDateTime("2008-05-12T23:45:51.045Z");
        Dates2.appendDateTime(sink, Dates2.ceilDD(millis));
        Assert.assertEquals("2008-05-12T23:59:59.999Z", sink.toString());
    }

    @Test
    public void testCeilDDPrevEpoch() throws Exception {
        long millis = Dates2.parseDateTime("1888-05-12T23:45:51.045Z");
        Dates2.appendDateTime(sink, Dates2.ceilDD(millis));
        Assert.assertEquals("1888-05-12T23:59:59.999Z", sink.toString());
    }

    @Test
    public void testAddMonths() throws Exception {
        long millis = Dates2.parseDateTime("2008-05-12T23:45:51.045Z");
        Dates2.appendDateTime(sink, Dates2.addMonths(millis, -10));
        Assert.assertEquals("2007-07-12T23:45:51.045Z", sink.toString());
    }

    @Test
    public void testAddMonthsPrevEpoch() throws Exception {
        long millis = Dates2.parseDateTime("1888-05-12T23:45:51.045Z");
        Dates2.appendDateTime(sink, Dates2.addMonths(millis, -10));
        Assert.assertEquals("1887-07-12T23:45:51.045Z", sink.toString());
    }

    @Test
    public void testAddYears() throws Exception {
        long millis = Dates2.parseDateTime("1988-05-12T23:45:51.045Z");
        Dates2.appendDateTime(sink, Dates2.addYear(millis, 10));
        Assert.assertEquals("1998-05-12T23:45:51.045Z", sink.toString());
    }

    @Test
    public void testAddYearsPrevEpoch() throws Exception {
        long millis = Dates2.parseDateTime("1888-05-12T23:45:51.045Z");
        Dates2.appendDateTime(sink, Dates2.addYear(millis, 10));
        Assert.assertEquals("1898-05-12T23:45:51.045Z", sink.toString());
    }


    @Test
    public void testAddDaysPrevEpoch() throws Exception {
        long millis = Dates2.parseDateTime("1888-05-12T23:45:51.045Z");
        Dates2.appendDateTime(sink, Dates2.addDays(millis, 24));
        Assert.assertEquals("1888-06-05T23:45:51.045Z", sink.toString());
    }
}

/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.lexer;

import com.questdb.std.LongList;
import com.questdb.std.microtime.DateFormatUtils;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.questdb.griffin.lexer.GriffinParserTestUtils.intervalToString;

public class IntervalCompilerTest {

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
    public void testIntersectContain2() throws Exception {
        a.add(DateFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"));
        a.add(DateFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z"));

        b.add(DateFormatUtils.parseDateTime("2016-03-10T09:00:00.000Z"));
        b.add(DateFormatUtils.parseDateTime("2016-03-10T13:30:00.000Z"));

        assertIntersect("[{lo=2016-03-10T10:00:00.000000Z, hi=2016-03-10T12:00:00.000000Z}]");
    }

    @Test
    public void testIntersectMergeOverlap() throws Exception {
        a.add(DateFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"));
        a.add(DateFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z"));

        b.add(DateFormatUtils.parseDateTime("2016-03-10T11:00:00.000Z"));
        b.add(DateFormatUtils.parseDateTime("2016-03-10T14:00:00.000Z"));

        assertIntersect("[{lo=2016-03-10T11:00:00.000000Z, hi=2016-03-10T12:00:00.000000Z}]");
    }

    @Test
    public void testIntersectMergeOverlap2() throws Exception {
        a.add(DateFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"));
        a.add(DateFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z"));

        b.add(DateFormatUtils.parseDateTime("2016-03-10T11:00:00.000Z"));
        b.add(DateFormatUtils.parseDateTime("2016-03-10T14:00:00.000Z"));

        assertIntersect("[{lo=2016-03-10T11:00:00.000000Z, hi=2016-03-10T12:00:00.000000Z}]");
    }

    @Test
    public void testIntersectNoOverlap() throws Exception {
        a.add(DateFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"));
        a.add(DateFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z"));
        a.add(DateFormatUtils.parseDateTime("2016-03-10T14:00:00.000Z"));
        a.add(DateFormatUtils.parseDateTime("2016-03-10T16:00:00.000Z"));

        b.add(DateFormatUtils.parseDateTime("2016-03-10T13:00:00.000Z"));
        b.add(DateFormatUtils.parseDateTime("2016-03-10T13:30:00.000Z"));

        assertIntersect("[]");
    }

    @Test
    public void testIntersectSame() throws Exception {
        a.add(DateFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"));
        a.add(DateFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z"));

        b.add(DateFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"));
        b.add(DateFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z"));

        assertIntersect("[{lo=2016-03-10T10:00:00.000000Z, hi=2016-03-10T12:00:00.000000Z}]");
    }

    @Test
    public void testIntersectTwoOverlapOne2() throws Exception {
        a.add(DateFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"));
        a.add(DateFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z"));


        a.add(DateFormatUtils.parseDateTime("2016-03-10T14:00:00.000Z"));
        a.add(DateFormatUtils.parseDateTime("2016-03-10T16:00:00.000Z"));


        b.add(DateFormatUtils.parseDateTime("2016-03-10T11:00:00.000Z"));
        b.add(DateFormatUtils.parseDateTime("2016-03-10T15:00:00.000Z"));

        assertIntersect("[{lo=2016-03-10T11:00:00.000000Z, hi=2016-03-10T12:00:00.000000Z},{lo=2016-03-10T14:00:00.000000Z, hi=2016-03-10T15:00:00.000000Z}]");
    }

    @Test
    public void testInvert() throws ParserException {
        final String intervalStr = "2018-01-10T10:30:00.000Z;30m;2d;2";
        LongList out = new LongList();
        IntervalCompiler.parseIntervalEx(intervalStr, 0, intervalStr.length(), 0, out);
        IntervalCompiler.invert(out);
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

    @Test
    public void testSubtractClipNone() throws Exception {
        a.add(DateFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"));
        a.add(DateFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z"));

        a.add(DateFormatUtils.parseDateTime("2016-03-10T14:00:00.000Z"));
        a.add(DateFormatUtils.parseDateTime("2016-03-10T15:00:00.000Z"));

        b.add(DateFormatUtils.parseDateTime("2016-03-10T13:00:00.000Z"));
        b.add(DateFormatUtils.parseDateTime("2016-03-10T13:30:00.000Z"));

        IntervalCompiler.subtract(a, b, out);
        TestUtils.assertEquals("[{lo=2016-03-10T10:00:00.000000Z, hi=2016-03-10T12:00:00.000000Z},{lo=2016-03-10T14:00:00.000000Z, hi=2016-03-10T15:00:00.000000Z}]",
                intervalToString(out));
    }

    @Test
    public void testSubtractClipTwo() throws Exception {
        a.add(DateFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"));
        a.add(DateFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z"));

        a.add(DateFormatUtils.parseDateTime("2016-03-10T14:00:00.000Z"));
        a.add(DateFormatUtils.parseDateTime("2016-03-10T15:00:00.000Z"));

        b.add(DateFormatUtils.parseDateTime("2016-03-10T11:00:00.000Z"));
        b.add(DateFormatUtils.parseDateTime("2016-03-10T14:30:00.000Z"));

        IntervalCompiler.subtract(a, b, out);
        TestUtils.assertEquals("[{lo=2016-03-10T10:00:00.000000Z, hi=2016-03-10T10:59:59.999999Z},{lo=2016-03-10T14:30:00.000001Z, hi=2016-03-10T15:00:00.000000Z}]", intervalToString(out));
    }

    @Test
    public void testSubtractConsume() throws Exception {
        a.add(DateFormatUtils.parseDateTime("2016-03-10T11:00:00.000Z"));
        a.add(DateFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z"));

        b.add(DateFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"));
        b.add(DateFormatUtils.parseDateTime("2016-03-10T14:00:00.000Z"));

        IntervalCompiler.subtract(a, b, out);
        TestUtils.assertEquals("[]", intervalToString(out));
    }

    @Test
    public void testSubtractMakeHole() throws Exception {
        a.add(DateFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"));
        a.add(DateFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z"));
        b.add(DateFormatUtils.parseDateTime("2016-03-10T10:30:00.000Z"));
        b.add(DateFormatUtils.parseDateTime("2016-03-10T11:30:00.000Z"));

        IntervalCompiler.subtract(a, b, out);
        TestUtils.assertEquals("[{lo=2016-03-10T10:00:00.000000Z, hi=2016-03-10T10:29:59.999999Z},{lo=2016-03-10T11:30:00.000001Z, hi=2016-03-10T12:00:00.000000Z}]", intervalToString(out));
    }

    @Test
    public void testSubtractSame() throws Exception {
        a.add(DateFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"));
        a.add(DateFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z"));
        b.add(DateFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"));
        b.add(DateFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z"));

        IntervalCompiler.subtract(a, b, out);
        TestUtils.assertEquals("[]", intervalToString(out));
    }

    private static void assertShortInterval(String expected, String interval) throws ParserException {
        LongList out = new LongList();
        IntervalCompiler.parseIntervalEx(interval, 0, interval.length(), 0, out);
        TestUtils.assertEquals(expected, intervalToString(out));
    }

    private void assertIntersect(String expected) {
        IntervalCompiler.intersect(a, b, out);
        TestUtils.assertEquals(expected, intervalToString(out));
    }

    private void assertIntervalError(String interval) {
        try {
            IntervalCompiler.parseIntervalEx(interval, 0, interval.length(), 0, out);
            Assert.fail();
        } catch (ParserException ignore) {
        }
    }

}
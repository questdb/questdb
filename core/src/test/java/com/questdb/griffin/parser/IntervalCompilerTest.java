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

package com.questdb.griffin.parser;

import com.questdb.std.LongList;
import com.questdb.std.ObjList;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.std.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.questdb.griffin.parser.GriffinParserTestUtils.intervalToString;

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

        assertIntersect("[Interval{lo=2016-03-10T10:00:00.000Z, hi=2016-03-10T12:00:00.000Z}]");
    }

    @Test
    public void testIntersectMergeOverlap() throws Exception {
        a.add(DateFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"));
        a.add(DateFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z"));

        b.add(DateFormatUtils.parseDateTime("2016-03-10T11:00:00.000Z"));
        b.add(DateFormatUtils.parseDateTime("2016-03-10T14:00:00.000Z"));

        assertIntersect("[Interval{lo=2016-03-10T11:00:00.000Z, hi=2016-03-10T12:00:00.000Z}]");
    }

    @Test
    public void testIntersectMergeOverlap2() throws Exception {
        a.add(DateFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"));
        a.add(DateFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z"));

        b.add(DateFormatUtils.parseDateTime("2016-03-10T11:00:00.000Z"));
        b.add(DateFormatUtils.parseDateTime("2016-03-10T14:00:00.000Z"));

        assertIntersect("[Interval{lo=2016-03-10T11:00:00.000Z, hi=2016-03-10T12:00:00.000Z}]");
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

        assertIntersect("[Interval{lo=2016-03-10T10:00:00.000Z, hi=2016-03-10T12:00:00.000Z}]");
    }

    @Test
    public void testIntersectTwoOverlapOne2() throws Exception {
        a.add(DateFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"));
        a.add(DateFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z"));


        a.add(DateFormatUtils.parseDateTime("2016-03-10T14:00:00.000Z"));
        a.add(DateFormatUtils.parseDateTime("2016-03-10T16:00:00.000Z"));


        b.add(DateFormatUtils.parseDateTime("2016-03-10T11:00:00.000Z"));
        b.add(DateFormatUtils.parseDateTime("2016-03-10T15:00:00.000Z"));

        assertIntersect("[Interval{lo=2016-03-10T11:00:00.000Z, hi=2016-03-10T12:00:00.000Z},Interval{lo=2016-03-10T14:00:00.000Z, hi=2016-03-10T15:00:00.000Z}]");
    }

    @Test
    public void testInvert() throws ParserException {
        final String intervalStr = "2018-01-10T10:30:00.000Z;30m;2d;2";
        LongList out = new LongList();
        IntervalCompiler.parseIntervalEx(intervalStr, 0, intervalStr.length(), 0, out);
        IntervalCompiler.invert(out);
        Assert.assertEquals("[Interval{lo=, hi=2018-01-10T10:29:59.999Z},Interval{lo=2018-01-10T11:00:00.001Z, hi=2018-01-12T10:29:59.999Z},Interval{lo=2018-01-12T11:00:00.001Z, hi=292278994-08-17T07:12:55.807Z}]",
                intervalToString(out));
    }

    @Test
    public void testMergeIntervals() throws Exception {
        ObjList<Interval> a = new ObjList<>();
        ObjList<Interval> b = new ObjList<>();
        a.add(new Interval(DateFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"), DateFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z")));
        a.add(new Interval(DateFormatUtils.parseDateTime("2016-03-10T14:00:00.000Z"), DateFormatUtils.parseDateTime("2016-03-10T16:00:00.000Z")));
        b.add(new Interval(DateFormatUtils.parseDateTime("2016-03-10T11:00:00.000Z"), DateFormatUtils.parseDateTime("2016-03-10T15:00:00.000Z")));
        Assert.assertEquals("[Interval{lo=2016-03-10T10:00:00.000Z, hi=2016-03-10T16:00:00.000Z}]", IntervalCompiler.union(a, b).toString());
    }

    @Test
    public void testParseLongInterval22() throws Exception {
        assertShortInterval("[Interval{lo=2015-03-12T10:00:00.000Z, hi=2015-03-12T10:05:00.999Z},Interval{lo=2015-03-12T10:30:00.000Z, hi=2015-03-12T10:35:00.999Z},Interval{lo=2015-03-12T11:00:00.000Z, hi=2015-03-12T11:05:00.999Z},Interval{lo=2015-03-12T11:30:00.000Z, hi=2015-03-12T11:35:00.999Z},Interval{lo=2015-03-12T12:00:00.000Z, hi=2015-03-12T12:05:00.999Z},Interval{lo=2015-03-12T12:30:00.000Z, hi=2015-03-12T12:35:00.999Z},Interval{lo=2015-03-12T13:00:00.000Z, hi=2015-03-12T13:05:00.999Z},Interval{lo=2015-03-12T13:30:00.000Z, hi=2015-03-12T13:35:00.999Z},Interval{lo=2015-03-12T14:00:00.000Z, hi=2015-03-12T14:05:00.999Z},Interval{lo=2015-03-12T14:30:00.000Z, hi=2015-03-12T14:35:00.999Z}]",
                "2015-03-12T10:00:00;5m;30m;10");
    }

    @Test
    public void testParseLongInterval32() throws Exception {
        assertShortInterval("[Interval{lo=2016-03-21T00:00:00.000Z, hi=2019-03-21T23:59:59.999Z},Interval{lo=2016-09-21T00:00:00.000Z, hi=2019-09-21T23:59:59.999Z},Interval{lo=2017-03-21T00:00:00.000Z, hi=2020-03-21T23:59:59.999Z},Interval{lo=2017-09-21T00:00:00.000Z, hi=2020-09-21T23:59:59.999Z},Interval{lo=2018-03-21T00:00:00.000Z, hi=2021-03-21T23:59:59.999Z}]", "2016-03-21;3y;6M;5");
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
        assertShortInterval("[Interval{lo=2016-01-01T00:00:00.000Z, hi=2016-12-31T23:59:59.999Z}]", "2016");
    }

    @Test
    public void testParseShortInterval2() throws Exception {
        assertShortInterval("[Interval{lo=2016-03-01T00:00:00.000Z, hi=2016-03-31T23:59:59.999Z}]", "2016-03");
    }

    @Test
    public void testParseShortInterval3() throws Exception {
        assertShortInterval("[Interval{lo=2016-03-21T00:00:00.000Z, hi=2016-03-21T23:59:59.999Z}]", "2016-03-21");
    }

    @Test
    public void testParseShortInterval32() throws Exception {
        assertShortInterval("[Interval{lo=2016-03-21T00:00:00.000Z, hi=2016-03-21T23:59:59.999Z}]", "2016-03-21");
    }

    @Test
    public void testParseShortInterval4() throws Exception {
        assertShortInterval("[Interval{lo=2016-03-21T10:00:00.000Z, hi=2016-03-21T10:59:59.999Z}]", "2016-03-21T10");
    }

    @Test
    public void testParseShortInterval5() throws Exception {
        assertShortInterval("[Interval{lo=2016-03-21T10:30:00.000Z, hi=2016-03-21T10:30:59.999Z}]", "2016-03-21T10:30");
    }

    @Test
    public void testParseShortInterval6() throws Exception {
        assertShortInterval("[Interval{lo=2016-03-21T10:30:40.000Z, hi=2016-03-21T10:30:40.999Z}]", "2016-03-21T10:30:40");
    }

    @Test
    public void testParseShortInterval7() throws Exception {
        assertShortInterval("[Interval{lo=2016-03-21T10:30:40.100Z, hi=2016-03-21T10:30:40.100Z}]", "2016-03-21T10:30:40.100Z");
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
        Assert.assertEquals("[Interval{lo=2016-03-10T10:00:00.000Z, hi=2016-03-10T12:00:00.000Z},Interval{lo=2016-03-10T14:00:00.000Z, hi=2016-03-10T15:00:00.000Z}]",
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
        Assert.assertEquals("[Interval{lo=2016-03-10T10:00:00.000Z, hi=2016-03-10T10:59:59.999Z},Interval{lo=2016-03-10T14:30:00.001Z, hi=2016-03-10T15:00:00.000Z}]", intervalToString(out));
    }

    @Test
    public void testSubtractConsume() throws Exception {
        a.add(DateFormatUtils.parseDateTime("2016-03-10T11:00:00.000Z"));
        a.add(DateFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z"));

        b.add(DateFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"));
        b.add(DateFormatUtils.parseDateTime("2016-03-10T14:00:00.000Z"));

        IntervalCompiler.subtract(a, b, out);
        Assert.assertEquals("[]", intervalToString(out));
    }

    @Test
    public void testSubtractMakeHole() throws Exception {
        a.add(DateFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"));
        a.add(DateFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z"));
        b.add(DateFormatUtils.parseDateTime("2016-03-10T10:30:00.000Z"));
        b.add(DateFormatUtils.parseDateTime("2016-03-10T11:30:00.000Z"));

        IntervalCompiler.subtract(a, b, out);
        Assert.assertEquals("[Interval{lo=2016-03-10T10:00:00.000Z, hi=2016-03-10T10:29:59.999Z},Interval{lo=2016-03-10T11:30:00.001Z, hi=2016-03-10T12:00:00.000Z}]", intervalToString(out));
    }

    @Test
    public void testSubtractSame() throws Exception {
        a.add(DateFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"));
        a.add(DateFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z"));
        b.add(DateFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"));
        b.add(DateFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z"));

        IntervalCompiler.subtract(a, b, out);
        Assert.assertEquals("[]", intervalToString(out));
    }

    @Test
    public void testUnionContain() throws Exception {
        ObjList<Interval> a = new ObjList<>();
        ObjList<Interval> b = new ObjList<>();
        a.add(new Interval(DateFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"), DateFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z")));
        b.add(new Interval(DateFormatUtils.parseDateTime("2016-03-10T09:00:00.000Z"), DateFormatUtils.parseDateTime("2016-03-10T13:30:00.000Z")));
        Assert.assertEquals("[Interval{lo=2016-03-10T09:00:00.000Z, hi=2016-03-10T13:30:00.000Z}]", IntervalCompiler.union(a, b).toString());
    }

    @Test
    public void testUnionContiguous() throws Exception {
        ObjList<Interval> a = new ObjList<>();
        ObjList<Interval> b = new ObjList<>();
        a.add(new Interval(DateFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"), DateFormatUtils.parseDateTime("2016-03-10T12:59:59.999Z")));
        b.add(new Interval(DateFormatUtils.parseDateTime("2016-03-10T13:00:00.000Z"), DateFormatUtils.parseDateTime("2016-03-10T14:00:00.000Z")));
        Assert.assertEquals("[Interval{lo=2016-03-10T10:00:00.000Z, hi=2016-03-10T14:00:00.000Z}]", IntervalCompiler.union(a, b).toString());
    }

    @Test
    public void testUnionNonOverlapping() throws Exception {
        ObjList<Interval> a = new ObjList<>();
        ObjList<Interval> b = new ObjList<>();
        a.add(new Interval(DateFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"), DateFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z")));
        b.add(new Interval(DateFormatUtils.parseDateTime("2016-03-10T13:00:00.000Z"), DateFormatUtils.parseDateTime("2016-03-10T15:00:00.000Z")));
        Assert.assertEquals("[Interval{lo=2016-03-10T10:00:00.000Z, hi=2016-03-10T12:00:00.000Z},Interval{lo=2016-03-10T13:00:00.000Z, hi=2016-03-10T15:00:00.000Z}]", IntervalCompiler.union(a, b).toString());
        Assert.assertEquals("[Interval{lo=2016-03-10T10:00:00.000Z, hi=2016-03-10T12:00:00.000Z},Interval{lo=2016-03-10T13:00:00.000Z, hi=2016-03-10T15:00:00.000Z}]", IntervalCompiler.union(b, a).toString());
    }

    @Test
    public void testUnionSame() throws Exception {
        ObjList<Interval> a = new ObjList<>();
        ObjList<Interval> b = new ObjList<>();
        a.add(new Interval(DateFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"), DateFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z")));
        b.add(new Interval(DateFormatUtils.parseDateTime("2016-03-10T10:00:00.000Z"), DateFormatUtils.parseDateTime("2016-03-10T12:00:00.000Z")));
        Assert.assertEquals("[Interval{lo=2016-03-10T10:00:00.000Z, hi=2016-03-10T12:00:00.000Z}]", IntervalCompiler.union(a, b).toString());
    }

    private static void assertShortInterval(String expected, String interval) throws ParserException {
        LongList out = new LongList();
        IntervalCompiler.parseIntervalEx(interval, 0, interval.length(), 0, out);
        Assert.assertEquals(expected, intervalToString(out));
    }

    private void assertIntersect(String expected) {
        IntervalCompiler.intersect(a, b, out);
        Assert.assertEquals(expected, intervalToString(out));
    }

    private void assertIntervalError(String interval) {
        try {
            IntervalCompiler.parseIntervalEx(interval, 0, interval.length(), 0, out);
            Assert.fail();
        } catch (ParserException ignore) {
        }
    }

}
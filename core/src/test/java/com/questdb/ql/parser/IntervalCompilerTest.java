/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.ql.parser;

import com.questdb.ex.ParserException;
import com.questdb.misc.Dates;
import com.questdb.misc.Interval;
import com.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

public class IntervalCompilerTest {

    @Test
    public void testIntersectContain() throws Exception {
        ObjList<Interval> a = new ObjList<>();
        ObjList<Interval> b = new ObjList<>();
        a.add(new Interval(Dates.parseDateTime("2016-03-10T10:00:00.000Z"), Dates.parseDateTime("2016-03-10T12:00:00.000Z")));
        b.add(new Interval(Dates.parseDateTime("2016-03-10T09:00:00.000Z"), Dates.parseDateTime("2016-03-10T13:30:00.000Z")));
        Assert.assertEquals("[Interval{lo=2016-03-10T10:00:00.000Z, hi=2016-03-10T12:00:00.000Z}]", IntervalCompiler.intersect(a, b).toString());
    }

    @Test
    public void testIntersectMergeOverlap() throws Exception {
        ObjList<Interval> a = new ObjList<>();
        ObjList<Interval> b = new ObjList<>();
        a.add(new Interval(Dates.parseDateTime("2016-03-10T10:00:00.000Z"), Dates.parseDateTime("2016-03-10T12:00:00.000Z")));
        b.add(new Interval(Dates.parseDateTime("2016-03-10T11:00:00.000Z"), Dates.parseDateTime("2016-03-10T14:00:00.000Z")));
        Assert.assertEquals("[Interval{lo=2016-03-10T11:00:00.000Z, hi=2016-03-10T12:00:00.000Z}]", IntervalCompiler.intersect(a, b).toString());
    }

    @Test
    public void testIntersectNoOverlap() throws Exception {
        ObjList<Interval> a = new ObjList<>();
        ObjList<Interval> b = new ObjList<>();
        a.add(new Interval(Dates.parseDateTime("2016-03-10T10:00:00.000Z"), Dates.parseDateTime("2016-03-10T12:00:00.000Z")));
        a.add(new Interval(Dates.parseDateTime("2016-03-10T14:00:00.000Z"), Dates.parseDateTime("2016-03-10T16:00:00.000Z")));
        b.add(new Interval(Dates.parseDateTime("2016-03-10T13:00:00.000Z"), Dates.parseDateTime("2016-03-10T13:30:00.000Z")));
        Assert.assertEquals("[]", IntervalCompiler.intersect(a, b).toString());
    }

    @Test
    public void testIntersectSame() throws Exception {
        ObjList<Interval> a = new ObjList<>();
        ObjList<Interval> b = new ObjList<>();
        a.add(new Interval(Dates.parseDateTime("2016-03-10T10:00:00.000Z"), Dates.parseDateTime("2016-03-10T12:00:00.000Z")));
        b.add(new Interval(Dates.parseDateTime("2016-03-10T10:00:00.000Z"), Dates.parseDateTime("2016-03-10T12:00:00.000Z")));
        Assert.assertEquals("[Interval{lo=2016-03-10T10:00:00.000Z, hi=2016-03-10T12:00:00.000Z}]", IntervalCompiler.intersect(a, b).toString());
    }

    @Test
    public void testIntersectTwoOverlapOne() throws Exception {
        ObjList<Interval> a = new ObjList<>();
        ObjList<Interval> b = new ObjList<>();
        a.add(new Interval(Dates.parseDateTime("2016-03-10T10:00:00.000Z"), Dates.parseDateTime("2016-03-10T12:00:00.000Z")));
        a.add(new Interval(Dates.parseDateTime("2016-03-10T14:00:00.000Z"), Dates.parseDateTime("2016-03-10T16:00:00.000Z")));
        b.add(new Interval(Dates.parseDateTime("2016-03-10T11:00:00.000Z"), Dates.parseDateTime("2016-03-10T15:00:00.000Z")));
        Assert.assertEquals("[Interval{lo=2016-03-10T11:00:00.000Z, hi=2016-03-10T12:00:00.000Z},Interval{lo=2016-03-10T14:00:00.000Z, hi=2016-03-10T15:00:00.000Z}]", IntervalCompiler.intersect(a, b).toString());
    }

    @Test
    public void testMergeIntervals() throws Exception {
        ObjList<Interval> a = new ObjList<>();
        ObjList<Interval> b = new ObjList<>();
        a.add(new Interval(Dates.parseDateTime("2016-03-10T10:00:00.000Z"), Dates.parseDateTime("2016-03-10T12:00:00.000Z")));
        a.add(new Interval(Dates.parseDateTime("2016-03-10T14:00:00.000Z"), Dates.parseDateTime("2016-03-10T16:00:00.000Z")));
        b.add(new Interval(Dates.parseDateTime("2016-03-10T11:00:00.000Z"), Dates.parseDateTime("2016-03-10T15:00:00.000Z")));
        Assert.assertEquals("[Interval{lo=2016-03-10T10:00:00.000Z, hi=2016-03-10T16:00:00.000Z}]", IntervalCompiler.union(a, b).toString());
    }

    @Test(expected = ParserException.class)
    public void testParseShortDayErr() throws Exception {
        IntervalCompiler.parseIntervalEx("2016-02-30", 0, 10, 0);
    }

    @Test(expected = ParserException.class)
    public void testParseShortDayErr2() throws Exception {
        IntervalCompiler.parseIntervalEx("2016-02-3", 0, 9, 0);
    }

    @Test(expected = ParserException.class)
    public void testParseShortHourErr1() throws Exception {
        IntervalCompiler.parseIntervalEx("2016-02-15T1", 0, 12, 0);
    }

    @Test(expected = ParserException.class)
    public void testParseShortHourErr2() throws Exception {
        IntervalCompiler.parseIntervalEx("2016-02-15T31", 0, 13, 0);
    }

    @Test(expected = ParserException.class)
    public void testParseShortHourErr3() throws Exception {
        IntervalCompiler.parseIntervalEx("2016-02-15X1", 0, 12, 0);
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

    @Test(expected = ParserException.class)
    public void testParseShortMilliErr() throws Exception {
        IntervalCompiler.parseIntervalEx("2016-03-21T10:31:61.23", 0, 22, 0);
    }

    @Test(expected = ParserException.class)
    public void testParseShortMinErr() throws Exception {
        IntervalCompiler.parseIntervalEx("2016-03-21T10:3", 0, 15, 0);
    }

    @Test(expected = ParserException.class)
    public void testParseShortMinErr2() throws Exception {
        IntervalCompiler.parseIntervalEx("2016-03-21T10:69", 0, 15, 0);
    }

    @Test(expected = ParserException.class)
    public void testParseShortMonthErr() throws Exception {
        IntervalCompiler.parseIntervalEx("2016-1", 0, 6, 0);
    }

    @Test(expected = ParserException.class)
    public void testParseShortMonthErr2() throws Exception {
        IntervalCompiler.parseIntervalEx("2016x11", 0, 7, 0);
    }

    @Test(expected = ParserException.class)
    public void testParseShortMonthRange() throws Exception {
        IntervalCompiler.parseIntervalEx("2016-66", 0, 7, 0);
    }

    @Test(expected = ParserException.class)
    public void testParseShortSecErr() throws Exception {
        IntervalCompiler.parseIntervalEx("2016-03-21T10:31:61", 0, 19, 0);
    }

    @Test(expected = ParserException.class)
    public void testParseShortSecErr1() throws Exception {
        IntervalCompiler.parseIntervalEx("2016-03-21T10:31:1", 0, 18, 0);
    }

    @Test(expected = ParserException.class)
    public void testParseShortYearErr() throws Exception {
        IntervalCompiler.parseIntervalEx("201", 0, 3, 0);
    }

    @Test
    public void testSubtractClipNone() throws Exception {
        ObjList<Interval> a = new ObjList<>();
        ObjList<Interval> b = new ObjList<>();
        a.add(new Interval(Dates.parseDateTime("2016-03-10T10:00:00.000Z"), Dates.parseDateTime("2016-03-10T12:00:00.000Z")));
        a.add(new Interval(Dates.parseDateTime("2016-03-10T14:00:00.000Z"), Dates.parseDateTime("2016-03-10T15:00:00.000Z")));
        b.add(new Interval(Dates.parseDateTime("2016-03-10T13:00:00.000Z"), Dates.parseDateTime("2016-03-10T13:30:00.000Z")));
        Assert.assertEquals("[Interval{lo=2016-03-10T10:00:00.000Z, hi=2016-03-10T12:00:00.000Z},Interval{lo=2016-03-10T14:00:00.000Z, hi=2016-03-10T15:00:00.000Z}]", IntervalCompiler.subtract(a, b).toString());
    }

    @Test
    public void testSubtractClipTwo() throws Exception {
        ObjList<Interval> a = new ObjList<>();
        ObjList<Interval> b = new ObjList<>();
        a.add(new Interval(Dates.parseDateTime("2016-03-10T10:00:00.000Z"), Dates.parseDateTime("2016-03-10T12:00:00.000Z")));
        a.add(new Interval(Dates.parseDateTime("2016-03-10T14:00:00.000Z"), Dates.parseDateTime("2016-03-10T15:00:00.000Z")));
        b.add(new Interval(Dates.parseDateTime("2016-03-10T11:00:00.000Z"), Dates.parseDateTime("2016-03-10T14:30:00.000Z")));
        Assert.assertEquals("[Interval{lo=2016-03-10T10:00:00.000Z, hi=2016-03-10T11:00:00.000Z},Interval{lo=2016-03-10T14:30:00.000Z, hi=2016-03-10T15:00:00.000Z}]", IntervalCompiler.subtract(a, b).toString());
    }

    @Test
    public void testSubtractConsume() throws Exception {
        ObjList<Interval> a = new ObjList<>();
        ObjList<Interval> b = new ObjList<>();
        a.add(new Interval(Dates.parseDateTime("2016-03-10T11:00:00.000Z"), Dates.parseDateTime("2016-03-10T12:00:00.000Z")));
        b.add(new Interval(Dates.parseDateTime("2016-03-10T10:00:00.000Z"), Dates.parseDateTime("2016-03-10T14:00:00.000Z")));
        Assert.assertEquals("[]", IntervalCompiler.subtract(a, b).toString());
    }

    @Test
    public void testSubtractMakeHole() throws Exception {
        ObjList<Interval> a = new ObjList<>();
        ObjList<Interval> b = new ObjList<>();
        a.add(new Interval(Dates.parseDateTime("2016-03-10T10:00:00.000Z"), Dates.parseDateTime("2016-03-10T12:00:00.000Z")));
        b.add(new Interval(Dates.parseDateTime("2016-03-10T10:30:00.000Z"), Dates.parseDateTime("2016-03-10T11:30:00.000Z")));
        Assert.assertEquals("[Interval{lo=2016-03-10T10:00:00.000Z, hi=2016-03-10T10:30:00.000Z},Interval{lo=2016-03-10T11:30:00.000Z, hi=2016-03-10T12:00:00.000Z}]", IntervalCompiler.subtract(a, b).toString());
    }

    @Test
    public void testSubtractSame() throws Exception {
        ObjList<Interval> a = new ObjList<>();
        ObjList<Interval> b = new ObjList<>();
        a.add(new Interval(Dates.parseDateTime("2016-03-10T10:00:00.000Z"), Dates.parseDateTime("2016-03-10T12:00:00.000Z")));
        b.add(new Interval(Dates.parseDateTime("2016-03-10T10:00:00.000Z"), Dates.parseDateTime("2016-03-10T12:00:00.000Z")));
        Assert.assertEquals("[]", IntervalCompiler.subtract(a, b).toString());
    }

    @Test
    public void testUnionContain() throws Exception {
        ObjList<Interval> a = new ObjList<>();
        ObjList<Interval> b = new ObjList<>();
        a.add(new Interval(Dates.parseDateTime("2016-03-10T10:00:00.000Z"), Dates.parseDateTime("2016-03-10T12:00:00.000Z")));
        b.add(new Interval(Dates.parseDateTime("2016-03-10T09:00:00.000Z"), Dates.parseDateTime("2016-03-10T13:30:00.000Z")));
        Assert.assertEquals("[Interval{lo=2016-03-10T09:00:00.000Z, hi=2016-03-10T13:30:00.000Z}]", IntervalCompiler.union(a, b).toString());
    }

    @Test
    public void testUnionContiguous() throws Exception {
        ObjList<Interval> a = new ObjList<>();
        ObjList<Interval> b = new ObjList<>();
        a.add(new Interval(Dates.parseDateTime("2016-03-10T10:00:00.000Z"), Dates.parseDateTime("2016-03-10T12:59:59.999Z")));
        b.add(new Interval(Dates.parseDateTime("2016-03-10T13:00:00.000Z"), Dates.parseDateTime("2016-03-10T14:00:00.000Z")));
        Assert.assertEquals("[Interval{lo=2016-03-10T10:00:00.000Z, hi=2016-03-10T14:00:00.000Z}]", IntervalCompiler.union(a, b).toString());
    }

    @Test
    public void testUnionNonOverlapping() throws Exception {
        ObjList<Interval> a = new ObjList<>();
        ObjList<Interval> b = new ObjList<>();
        a.add(new Interval(Dates.parseDateTime("2016-03-10T10:00:00.000Z"), Dates.parseDateTime("2016-03-10T12:00:00.000Z")));
        b.add(new Interval(Dates.parseDateTime("2016-03-10T13:00:00.000Z"), Dates.parseDateTime("2016-03-10T15:00:00.000Z")));
        Assert.assertEquals("[Interval{lo=2016-03-10T10:00:00.000Z, hi=2016-03-10T12:00:00.000Z},Interval{lo=2016-03-10T13:00:00.000Z, hi=2016-03-10T15:00:00.000Z}]", IntervalCompiler.union(a, b).toString());
        Assert.assertEquals("[Interval{lo=2016-03-10T10:00:00.000Z, hi=2016-03-10T12:00:00.000Z},Interval{lo=2016-03-10T13:00:00.000Z, hi=2016-03-10T15:00:00.000Z}]", IntervalCompiler.union(b, a).toString());
    }

    @Test
    public void testUnionSame() throws Exception {
        ObjList<Interval> a = new ObjList<>();
        ObjList<Interval> b = new ObjList<>();
        a.add(new Interval(Dates.parseDateTime("2016-03-10T10:00:00.000Z"), Dates.parseDateTime("2016-03-10T12:00:00.000Z")));
        b.add(new Interval(Dates.parseDateTime("2016-03-10T10:00:00.000Z"), Dates.parseDateTime("2016-03-10T12:00:00.000Z")));
        Assert.assertEquals("[Interval{lo=2016-03-10T10:00:00.000Z, hi=2016-03-10T12:00:00.000Z}]", IntervalCompiler.union(a, b).toString());
    }

    private static void assertShortInterval(String expected, String interval) throws ParserException {
        Assert.assertEquals(expected, IntervalCompiler.parseIntervalEx(interval, 0, interval.length(), 0).toString());
    }
}
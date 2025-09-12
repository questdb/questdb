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

package io.questdb.test.griffin.model;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.IntervalOperation;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.LongList;
import io.questdb.std.NumericException;
import io.questdb.std.str.StringSink;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static io.questdb.test.griffin.GriffinParserTestUtils.intervalToString;

@RunWith(Parameterized.class)
public class IntrinsicModelTest {
    private static final StringSink sink = new StringSink();
    private final LongList a = new LongList();
    private final LongList b = new LongList();
    private final LongList out = new LongList();
    private final TestTimestampType timestampType;

    public IntrinsicModelTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Before
    public void setUp() {
        a.clear();
        b.clear();
        out.clear();
    }

    @Test
    public void testDateCeilMicroWithDiffFraction() throws NumericException {
        assertDateFloor("2015-02-28T08:22:44.556012Z", "2015-02-28T08:22:44.556012");
        assertDateFloor("2015-02-28T08:22:44.556010Z", "2015-02-28T08:22:44.55601");
        assertDateFloor("2015-02-28T08:22:44.556000Z", "2015-02-28T08:22:44.5560");
        assertDateFloor("2015-02-28T08:22:44.556000Z", "2015-02-28T08:22:44.556");
        assertDateFloor("2015-02-28T08:22:44.550000Z", "2015-02-28T08:22:44.55");
        assertDateFloor("2015-02-28T08:22:44.500000Z", "2015-02-28T08:22:44.5");
    }

    @Test(expected = NumericException.class)
    public void testDateFloorFails() throws NumericException {
        assertDateFloor("", "2015-01-01T00:00:00.000000-1");
    }

    @Test(expected = NumericException.class)
    public void testDateFloorFailsOnTzSign() throws NumericException {
        assertDateFloor("", "2015-01-01T00:00:00.000000≠10");
    }

    @Test
    public void testDateFloorMicroWithTzHrs() throws NumericException {
        assertDateFloor("2015-02-28T09:22:44.556011Z", "2015-02-28T08:22:44.556011-01");
        assertDateFloor("2015-02-28T09:22:44.556011Z", "2015-02-28 08:22:44.556011-01");
    }

    @Test
    public void testDateFloorMicroWithTzHrsMins() throws NumericException {
        assertDateFloor("2015-02-28T04:38:44.556011Z", "2015-02-28T06:00:44.556011+01:22");
        assertDateFloor("2015-02-28T04:38:44.556011Z", "2015-02-28T06:00:44.556011+0122");
        assertDateFloor("2015-02-28T04:38:44.556011Z", "2015-02-28 06:00:44.556011+0122");
    }

    @Test
    public void testDateFloorMillsWithTzHrsMins() throws NumericException {
        assertDateFloor("2015-02-28T06:30:44.555000Z", "2015-02-28T06:00:44.555-00:30");
        assertDateFloor("2015-02-28T06:30:44.555000Z", "2015-02-28T06:00:44.555-0030");
    }

    @Test
    public void testDateFloorSecsWithTzHrsMins() throws NumericException {
        assertDateFloor("2015-02-28T05:00:44.000000Z", "2015-02-28T06:00:44+01:00");
        assertDateFloor("2015-02-28T05:00:44.000000Z", "2015-02-28T06:00:44+0100");
        assertDateFloor("2015-02-28T05:00:44.000000Z", "2015-02-28 06:00:44+0100");
    }

    @Test
    public void testDateFloorYYYY() throws NumericException {
        assertDateFloor("2015-01-01T00:00:00.000000Z", "2015");
    }

    @Test
    public void testDateFloorYYYYMM() throws NumericException {
        assertDateFloor("2015-02-01T00:00:00.000000Z", "2015-02");
    }

    @Test
    public void testDateFloorYYYYMMDD() throws NumericException {
        assertDateFloor("2015-02-28T00:00:00.000000Z", "2015-02-28");
    }

    @Test
    public void testDateFloorYYYYMMDDH() throws NumericException {
        assertDateFloor("2015-02-28T07:00:00.000000Z", "2015-02-28T07");
        assertDateFloor("2015-02-28T07:00:00.000000Z", "2015-02-28 07");
    }

    @Test
    public void testDateFloorYYYYMMDDHm() throws NumericException {
        assertDateFloor("2015-02-28T07:21:00.000000Z", "2015-02-28T07:21");
        assertDateFloor("2015-02-28T07:21:00.000000Z", "2015-02-28 07:21");
    }

    @Test
    public void testDateFloorYYYYMMDDHms() throws NumericException {
        assertDateFloor("2015-02-28T07:21:44.000000Z", "2015-02-28T07:21:44");
        assertDateFloor("2015-02-28T07:21:44.000000Z", "2015-02-28 07:21:44");
    }

    @Test
    public void testDateFloorYYYYMMDDHmsS() throws NumericException {
        assertDateFloor("2015-02-28T07:21:44.556000Z", "2015-02-28T07:21:44.556");
        assertDateFloor("2015-02-28T07:21:44.556000Z", "2015-02-28 07:21:44.556");
    }

    @Test
    public void testDateFloorYYYYMMDDHmsSU() throws NumericException {
        assertDateFloor("2015-02-28T07:21:44.556011Z", "2015-02-28T07:21:44.556011");
        assertDateFloor("2015-02-28T07:21:44.556011Z", "2015-02-28 07:21:44.556011");
    }

    @Test
    public void testIntersectContain2() {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T10:00:00.000Z"));
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T12:00:00.000Z"));

        b.add(timestampDriver.parseFloorLiteral("2016-03-10T09:00:00.000Z"));
        b.add(timestampDriver.parseFloorLiteral("2016-03-10T13:30:00.000Z"));

        assertIntersect("[{lo=2016-03-10T10:00:00.000000Z, hi=2016-03-10T12:00:00.000000Z}]");
    }

    @Test
    public void testIntersectMergeOverlap() {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T10:00:00.000Z"));
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T12:00:00.000Z"));

        b.add(timestampDriver.parseFloorLiteral("2016-03-10T11:00:00.000Z"));
        b.add(timestampDriver.parseFloorLiteral("2016-03-10T14:00:00.000Z"));

        assertIntersect("[{lo=2016-03-10T11:00:00.000000Z, hi=2016-03-10T12:00:00.000000Z}]");
    }

    @Test
    public void testIntersectMergeOverlap2() {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T10:00:00.000Z"));
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T12:00:00.000Z"));

        b.add(timestampDriver.parseFloorLiteral("2016-03-10T11:00:00.000Z"));
        b.add(timestampDriver.parseFloorLiteral("2016-03-10T14:00:00.000Z"));

        assertIntersect("[{lo=2016-03-10T11:00:00.000000Z, hi=2016-03-10T12:00:00.000000Z}]");
    }

    @Test
    public void testIntersectNoOverlap() {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T10:00:00.000Z"));
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T12:00:00.000Z"));
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T14:00:00.000Z"));
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T16:00:00.000Z"));

        b.add(timestampDriver.parseFloorLiteral("2016-03-10T13:00:00.000Z"));
        b.add(timestampDriver.parseFloorLiteral("2016-03-10T13:30:00.000Z"));

        assertIntersect("[]");
    }

    @Test
    public void testIntersectSame() {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T10:00:00.000Z"));
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T12:00:00.000Z"));

        b.add(timestampDriver.parseFloorLiteral("2016-03-10T10:00:00.000Z"));
        b.add(timestampDriver.parseFloorLiteral("2016-03-10T12:00:00.000Z"));

        assertIntersect("[{lo=2016-03-10T10:00:00.000000Z, hi=2016-03-10T12:00:00.000000Z}]");
    }

    @Test
    public void testIntersectTwoOverlapOne2() {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T10:00:00.000Z"));
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T12:00:00.000Z"));

        a.add(timestampDriver.parseFloorLiteral("2016-03-10T14:00:00.000Z"));
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T16:00:00.000Z"));

        b.add(timestampDriver.parseFloorLiteral("2016-03-10T11:00:00.000Z"));
        b.add(timestampDriver.parseFloorLiteral("2016-03-10T15:00:00.000Z"));

        assertIntersect("[{lo=2016-03-10T11:00:00.000000Z, hi=2016-03-10T12:00:00.000000Z},{lo=2016-03-10T14:00:00.000000Z, hi=2016-03-10T15:00:00.000000Z}]");
    }

    @Test
    public void testInvert() throws SqlException {
        final String intervalStr = "2018-01-10T10:30:00.000Z;30m;2d;2";
        LongList out = new LongList();
        TimestampDriver timestampDriver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP);
        IntervalUtils.parseInterval(timestampDriver, intervalStr, 0, intervalStr.length(), 0, out, IntervalOperation.INTERSECT);
        IntervalUtils.applyLastEncodedInterval(timestampDriver, out);
        IntervalUtils.invert(out);
        TestUtils.assertEquals(
                "[{lo=, hi=2018-01-10T10:29:59.999999Z},{lo=2018-01-10T11:00:00.000001Z, hi=2018-01-12T10:29:59.999999Z},{lo=2018-01-12T11:00:00.000001Z, hi=294247-01-10T04:00:54.775807Z}]",
                intervalToString(timestampDriver, out)
        );
    }

    @Test
    public void testParseLongInterval22() throws Exception {
        assertShortInterval(
                "[{lo=2015-03-12T10:00:00.000000Z, hi=2015-03-12T10:05:00.999999Z},{lo=2015-03-12T10:30:00.000000Z, hi=2015-03-12T10:35:00.999999Z},{lo=2015-03-12T11:00:00.000000Z, hi=2015-03-12T11:05:00.999999Z},{lo=2015-03-12T11:30:00.000000Z, hi=2015-03-12T11:35:00.999999Z},{lo=2015-03-12T12:00:00.000000Z, hi=2015-03-12T12:05:00.999999Z},{lo=2015-03-12T12:30:00.000000Z, hi=2015-03-12T12:35:00.999999Z},{lo=2015-03-12T13:00:00.000000Z, hi=2015-03-12T13:05:00.999999Z},{lo=2015-03-12T13:30:00.000000Z, hi=2015-03-12T13:35:00.999999Z},{lo=2015-03-12T14:00:00.000000Z, hi=2015-03-12T14:05:00.999999Z},{lo=2015-03-12T14:30:00.000000Z, hi=2015-03-12T14:35:00.999999Z}]",
                "2015-03-12T10:00:00;5m;30m;10"
        );
    }

    @Test
    public void testParseLongInterval32() throws Exception {
        assertShortInterval("[{lo=2016-03-21T00:00:00.000000Z, hi=2021-03-21T23:59:59.999999Z}]", "2016-03-21;3y;6M;5");
    }

    @Test
    public void testParseLongMinusInterval() throws Exception {
        assertShortInterval(
                "[{lo=2015-03-12T10:00:00.000000Z, hi=2015-03-12T10:05:00.999999Z},{lo=2015-03-12T10:30:00.000000Z, hi=2015-03-12T10:35:00.999999Z},{lo=2015-03-12T11:00:00.000000Z, hi=2015-03-12T11:05:00.999999Z}]",
                "2015-03-12T11:00:00;5m;-30m;3"
        );
        assertShortInterval(
                "[{lo=2014-11-12T11:00:00.000000Z, hi=2014-11-12T11:05:00.999999Z},{lo=2015-01-12T11:00:00.000000Z, hi=2015-01-12T11:05:00.999999Z},{lo=2015-03-12T11:00:00.000000Z, hi=2015-03-12T11:05:00.999999Z}]",
                "2015-03-12T11:00:00;5m;-2M;3"
        );
        assertShortInterval(
                "[{lo=2013-03-12T11:00:00.000000Z, hi=2013-03-12T11:05:00.999999Z},{lo=2014-03-12T11:00:00.000000Z, hi=2014-03-12T11:05:00.999999Z},{lo=2015-03-12T11:00:00.000000Z, hi=2015-03-12T11:05:00.999999Z}]",
                "2015-03-12T11:00:00;5m;-1y;3"
        );
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
    public void testParseShortInterval10() throws Exception {
        Assume.assumeTrue(timestampType == TestTimestampType.NANO);
        assertShortInterval("[{lo=2016-03-21T10:30:40.123456780Z, hi=2016-03-21T10:30:40.123456789Z}]", "2016-03-21T10:30:40.12345678");
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
    public void testParseShortInterval8() throws Exception {
        assertShortInterval("[{lo=2016-03-21T10:30:40.100000Z, hi=2016-03-21T10:30:40.100999Z}]", "2016-03-21T10:30:40.100");
        assertShortInterval("[{lo=2016-03-21T10:30:40.280000Z, hi=2016-03-21T10:30:40.289999Z}]", "2016-03-21T10:30:40.28");
    }

    @Test
    public void testParseShortInterval9() throws Exception {
        Assume.assumeTrue(timestampType == TestTimestampType.MICRO);
        assertShortInterval("[{lo=2016-03-21T10:30:40.123456Z, hi=2016-03-21T10:30:40.123456Z}]", "2016-03-21T10:30:40.12345678");
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
        assertIntervalError("201-");
    }

    @Test
    public void testParseShortYearErr1() {
        assertIntervalError("20-");
    }

    private void assertDateFloor(String expected, String value) throws NumericException {
        sink.clear();
        final TimestampDriver timestampDriver = timestampType.getDriver();
        long t = timestampDriver.parseFloorLiteral(value);
        timestampDriver.append(sink, t);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? expected.replaceAll("Z", "000Z").replaceAll("999999Z", "999999999Z")
                        : expected,
                sink
        );
    }

    private void assertIntersect(String expected) {
        out.add(a);
        out.add(b);
        IntervalUtils.intersectInPlace(out, a.size());
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType()) ?
                        expected.replaceAll("000000Z", "000000000Z").replaceAll("999999Z", "999999999Z")
                        : expected,
                intervalToString(timestampType.getDriver(), out)
        );
    }

    private void assertIntervalError(String interval) {
        try {
            final TimestampDriver timestampDriver = timestampType.getDriver();
            IntervalUtils.parseInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
            IntervalUtils.applyLastEncodedInterval(timestampDriver, out);
            Assert.fail();
        } catch (SqlException ignore) {
        }
    }

    private void assertShortInterval(String expected, String interval) throws SqlException {
        LongList out = new LongList();
        final TimestampDriver timestampDriver = timestampType.getDriver();
        IntervalUtils.parseInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        IntervalUtils.applyLastEncodedInterval(timestampDriver, out);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? expected.replaceAll("00Z", "00000Z").replaceAll("99Z", "99999Z")
                        : expected,
                intervalToString(timestampDriver, out)
        );
    }
}

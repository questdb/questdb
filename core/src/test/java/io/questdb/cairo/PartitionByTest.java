/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cairo;

import io.questdb.std.Chars;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PartitionByTest {
    private static final StringSink sink = new StringSink();

    @Before
    public void setUp() {
        sink.clear();
    }

    @Test
    public void testAddCeilFloorDay() throws NumericException {
        testAddCeilFloor(
                "2023-03-02T00:00:00.000000Z",
                PartitionBy.DAY,
                "2023-03-01T00:00:00.000000Z",
                "2023-03-01T11:22:00.000000Z"
        );
    }

    @Test
    public void testAddCeilFloorDayEdge() throws NumericException {
        testAddCeilFloor(
                "2023-03-02T00:00:00.000000Z",
                PartitionBy.DAY,
                "2023-03-01T00:00:00.000000Z",
                "2023-03-01T00:00:00.000000Z"
        );
    }

    @Test
    public void testAddCeilFloorHour() throws NumericException {
        testAddCeilFloor(
                "2021-02-12T17:00:00.000000Z",
                PartitionBy.HOUR,
                "2021-02-12T16:00:00.000000Z",
                "2021-02-12T16:38:00.000000Z"
        );
    }

    @Test
    public void testAddCeilFloorHourEdge() throws NumericException {
        testAddCeilFloor(
                "2021-02-12T17:00:00.000000Z",
                PartitionBy.HOUR,
                "2021-02-12T16:00:00.000000Z",
                "2021-02-12T16:00:00.000000Z"
        );
    }

    @Test
    public void testAddCeilFloorMonth() throws NumericException {
        testAddCeilFloor(
                "2023-04-01T00:00:00.000000Z",
                PartitionBy.MONTH,
                "2023-03-01T00:00:00.000000Z",
                "2023-03-16T11:22:00.000000Z"
        );
    }

    @Test
    public void testAddCeilFloorMonthEdge() throws NumericException {
        testAddCeilFloor(
                "2023-04-01T00:00:00.000000Z",
                PartitionBy.MONTH,
                "2023-03-01T00:00:00.000000Z",
                "2023-03-01T00:00:00.000000Z"
        );
    }

    @Test
    public void testAddCeilFloorNone() {
        PartitionBy.PartitionAddMethod addMethod = PartitionBy.getPartitionAddMethod(PartitionBy.NONE);
        Assert.assertNull(addMethod);
        PartitionBy.PartitionFloorMethod floorMethod = PartitionBy.getPartitionFloorMethod(PartitionBy.NONE);
        Assert.assertNull(floorMethod);
        PartitionBy.PartitionCeilMethod ceilMethod = PartitionBy.getPartitionCeilMethod(PartitionBy.NONE);
        Assert.assertNull(ceilMethod);
    }

    @Test
    public void testAddCeilFloorYear() throws NumericException {
        testAddCeilFloor(
                "2024-01-01T00:00:00.000000Z",
                PartitionBy.YEAR,
                "2023-01-01T00:00:00.000000Z",
                "2023-03-17T11:22:00.000000Z"
        );
    }

    @Test
    public void testAddCeilFloorYearEdge() throws NumericException {
        testAddCeilFloor(
                "2024-01-01T00:00:00.000000Z",
                PartitionBy.YEAR,
                "2023-01-01T00:00:00.000000Z",
                "2023-01-01T00:00:00.000000Z"
        );
    }

    @Test
    public void testDirectoryFormattingDay() throws NumericException {
        assertFormatAndParse("2013-03-31", "2013-03-31T00:00:00.000000Z", PartitionBy.DAY);
    }

    @Test
    public void testDirectoryFormattingHour() throws NumericException {
        assertFormatAndParse("2014-09-03T21", "2014-09-03T21:00:00.000000Z", PartitionBy.HOUR);
    }

    @Test
    public void testDirectoryFormattingMonth() throws NumericException {
        assertFormatAndParse("2013-03", "2013-03-01T00:00:00.000000Z", PartitionBy.MONTH);
    }

    @Test
    public void testDirectoryFormattingNone() throws NumericException {
        assertFormatAndParse("default", "1970-01-01T00:00:00.000000Z", PartitionBy.NONE);
    }

    @Test
    public void testDirectoryFormattingYear() throws NumericException {
        assertFormatAndParse("2014", "2014-01-01T00:00:00.000000Z", PartitionBy.YEAR);
    }

    @Test
    public void testDirectoryParseFailureByDay() {
        assertParseFailure("'YYYY-MM-DD' expected", "2013-03", PartitionBy.DAY);
    }

    @Test
    public void testDirectoryParseFailureByHour() {
        assertParseFailure("'YYYY-MM-DDTHH' expected", "2013-03-12", PartitionBy.HOUR);
    }

    @Test
    public void testDirectoryParseFailureByMonth() {
        assertParseFailure("'YYYY-MM' expected", "2013-03-02", PartitionBy.MONTH);
    }

    @Test
    public void testDirectoryParseFailureByYear() {
        assertParseFailure("'YYYY' expected", "2013-03-12", PartitionBy.YEAR);
    }

    @Test
    public void testIsPartitioned() {
        Assert.assertTrue(PartitionBy.isPartitioned(PartitionBy.DAY));
        Assert.assertTrue(PartitionBy.isPartitioned(PartitionBy.MONTH));
        Assert.assertTrue(PartitionBy.isPartitioned(PartitionBy.YEAR));
        Assert.assertTrue(PartitionBy.isPartitioned(PartitionBy.HOUR));
        Assert.assertFalse(PartitionBy.isPartitioned(PartitionBy.NONE));
    }

    @Test
    public void testPartitionByNameDay() {
        testPartitionByName("DAY", PartitionBy.DAY);
    }

    @Test
    public void testPartitionByNameHour() {
        testPartitionByName("HOUR", PartitionBy.HOUR);
    }

    @Test
    public void testPartitionByNameMonth() {
        testPartitionByName("MONTH", PartitionBy.MONTH);
    }

    @Test
    public void testPartitionByNameNone() {
        testPartitionByName("NONE", PartitionBy.NONE);
    }

    @Test
    public void testPartitionByNameYear() {
        testPartitionByName("YEAR", PartitionBy.YEAR);
    }

    @Test
    public void testSetPathByDay() throws NumericException {
        setSetPath(
                "2018-10-12T23:59:59.999999Z",
                "a/b/2018-10-12",
                "2018-10-12T00:00:00.000000Z",
                PartitionBy.DAY
        );
    }

    @Test
    public void testSetPathByHour() throws NumericException {
        setSetPath(
                "2021-04-01T18:59:59.999999Z",
                "a/b/2021-04-01T18",
                "2021-04-01T18:00:00.000000Z",
                PartitionBy.HOUR
        );
    }

    @Test
    public void testSetPathByMonth() throws NumericException {
        setSetPath(
                "2021-04-30T23:59:59.999999Z",
                "a/b/2021-04",
                "2021-04-01T00:00:00.000000Z",
                PartitionBy.MONTH
        );
    }

    @Test
    public void testSetPathByNone() throws NumericException {
        sink.put("a/b/");
        final long expectedCeilTimestamp = Long.MAX_VALUE;
        Assert.assertEquals(
                expectedCeilTimestamp,
                PartitionBy.setSinkForPartition(
                        sink,
                        PartitionBy.NONE,
                        TimestampFormatUtils.parseTimestamp("2021-01-01T00:00:00.000000Z"),
                        true
                )
        );
        TestUtils.assertEquals("a/b/default", sink);
    }

    @Test
    public void testSetPathByYear() throws NumericException {
        setSetPath(
                "2021-12-31T23:59:59.999999Z",
                "a/b/2021",
                "2021-01-01T00:00:00.000000Z",
                PartitionBy.YEAR
        );
    }

    @Test
    public void testSetPathNoCalcByDay() throws NumericException {
        setSetPathNoCalc(
                "a/b/2018-10-12",
                "2018-10-12T00:00:00.000000Z",
                PartitionBy.DAY
        );
    }

    @Test
    public void testSetPathNoCalcByHour() throws NumericException {
        setSetPathNoCalc(
                "a/b/2021-04-01T18",
                "2021-04-01T18:00:00.000000Z",
                PartitionBy.HOUR
        );
    }

    @Test
    public void testSetPathNoCalcByMonth() throws NumericException {
        setSetPathNoCalc(
                "a/b/2021-04",
                "2021-04-01T00:00:00.000000Z",
                PartitionBy.MONTH
        );
    }

    @Test
    public void testSetPathNoCalcByYear() throws NumericException {
        setSetPathNoCalc(
                "a/b/2021",
                "2021-01-01T00:00:00.000000Z",
                PartitionBy.YEAR
        );
    }

    @Test
    public void testUnknowns() {
        try {
            //noinspection ResultOfMethodCallIgnored
            PartitionBy.getPartitionDirFormatMethod(-1);
            Assert.fail();
        } catch (Exception ignored) {
        }

        TestUtils.assertEquals("UNKNOWN", PartitionBy.toString(-1));
    }

    private void assertFormatAndParse(CharSequence expectedDirName, CharSequence timestampString, int partitionBy) throws NumericException {
        final long expected = TimestampFormatUtils.parseTimestamp(timestampString);
        DateFormat dirFormatMethod = PartitionBy.getPartitionDirFormatMethod(partitionBy);
        dirFormatMethod.format(expected, TimestampFormatUtils.enLocale, null, sink);
        TestUtils.assertEquals(expectedDirName, sink);
        Assert.assertEquals(expected, PartitionBy.parsePartitionDirName(sink, partitionBy));
    }

    private void assertParseFailure(CharSequence expected, CharSequence dirName, int partitionBy) {
        try {
            PartitionBy.parsePartitionDirName(dirName, partitionBy);
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertEquals(expected, e.getFlyweightMessage());
        }
    }

    private void setSetPath(
            CharSequence expectedCeilTimestamp2,
            CharSequence expectedDirName,
            CharSequence timestamp,
            int partitionBy
    ) throws NumericException {
        sink.put("a/b/");
        final long expectedCeilTimestamp = TimestampFormatUtils.parseTimestamp(expectedCeilTimestamp2);
        Assert.assertEquals(
                expectedCeilTimestamp,
                PartitionBy.setSinkForPartition(
                        sink,
                        partitionBy,
                        TimestampFormatUtils.parseTimestamp(timestamp),
                        true
                )
        );
        TestUtils.assertEquals(expectedDirName, sink);
    }

    private void setSetPathNoCalc(
            CharSequence expectedDirName,
            CharSequence timestamp,
            int partitionBy
    ) throws NumericException {
        sink.put("a/b/");
        Assert.assertEquals(
                0,
                PartitionBy.setSinkForPartition(
                        sink,
                        partitionBy,
                        TimestampFormatUtils.parseTimestamp(timestamp),
                        false
                )
        );
        TestUtils.assertEquals(expectedDirName, sink);
    }

    private void testAddCeilFloor(
            CharSequence expectedNext,
            int partitionBy,
            CharSequence partitionTimestampStr,
            CharSequence midPartitionTimestampStr
    ) throws NumericException {
        final long expectedNextPartitionTimestamp = TimestampFormatUtils.parseTimestamp(expectedNext);
        final long partitionTimestamp = TimestampFormatUtils.parseTimestamp(partitionTimestampStr);
        final long midPartitionTimestamp = TimestampFormatUtils.parseTimestamp(midPartitionTimestampStr);

        PartitionBy.PartitionAddMethod addMethod = PartitionBy.getPartitionAddMethod(partitionBy);
        Assert.assertNotNull(addMethod);

        PartitionBy.PartitionFloorMethod floorMethod = PartitionBy.getPartitionFloorMethod(partitionBy);
        Assert.assertNotNull(floorMethod);

        PartitionBy.PartitionCeilMethod ceilMethod = PartitionBy.getPartitionCeilMethod(partitionBy);
        Assert.assertNotNull(ceilMethod);

        Assert.assertEquals(expectedNextPartitionTimestamp, addMethod.calculate(partitionTimestamp, 1));
        Assert.assertEquals(partitionTimestamp, floorMethod.floor(midPartitionTimestamp));
        Assert.assertEquals(expectedNextPartitionTimestamp, ceilMethod.ceil(midPartitionTimestamp));
    }

    private void testPartitionByName(CharSequence expectedPartitionName, int partitionBy) {
        CharSequence partitionName = PartitionBy.toString(partitionBy);
        TestUtils.assertEquals(expectedPartitionName, partitionName);
        Assert.assertEquals(partitionBy, PartitionBy.fromString(partitionName));
        Assert.assertEquals(partitionBy, PartitionBy.fromString(Chars.toString(partitionName).toUpperCase()));
        Assert.assertEquals(partitionBy, PartitionBy.fromString(Chars.toString(partitionName).toLowerCase()));
    }
}
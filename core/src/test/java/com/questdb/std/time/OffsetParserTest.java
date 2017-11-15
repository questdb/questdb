package com.questdb.std.time;

import com.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

public class OffsetParserTest {

    @Test
    public void testBadDelim() throws Exception {
        assertError("UTC+01x30");
    }

    @Test
    public void testBadGMT() throws Exception {
        assertError("GMX+03:30");
    }

    @Test
    public void testBadHour() throws Exception {
        assertError("UTC+0x:30");
    }

    @Test
    public void testBadMinute() throws Exception {
        assertError("UTC+01:3x");
    }

    @Test
    public void testBadSign() throws Exception {
        assertError("GMT*08:00");
    }

    @Test
    public void testBadStart() throws Exception {
        assertError("*08");
    }

    @Test
    public void testBadUtc() throws Exception {
        assertError("UTX+09:00");
    }

    @Test
    public void testGMTCamelCasePositive() throws Exception {
        assertThat(2 * 60 + 15, "gMt+02:15");
    }

    @Test
    public void testGMTNegative() throws Exception {
        assertThat(-2 * 60 + -15, "gMt-02:15");
    }

    @Test
    public void testGMTPositive() throws Exception {
        assertThat(3 * 60 + 30, "GMT+03:30");
    }

    @Test
    public void testHourMinNoDelim() throws Exception {
        assertThat(8 * 60 + 15, "0815");
    }

    @Test
    public void testHourMinNoDelimNegative() throws Exception {
        assertThat(-3 * 60 - 30, "-0330");
    }

    @Test
    public void testHourMinNoDelimPositive() throws Exception {
        assertThat(8 * 60 + 15, "+0815");
    }

    @Test
    public void testHoursOnly() throws Exception {
        assertThat(8 * 60, "08");
    }

    @Test
    public void testLargeHour() throws Exception {
        assertError("UTC+24:00");
    }

    @Test
    public void testLargeMinute() throws Exception {
        assertError("UTC+12:60");
    }

    @Test
    public void testMissingHour() throws Exception {
        assertError("UTC+");
    }

    @Test
    public void testMissingMinute() throws Exception {
        assertError("UTC+01:");
    }

    @Test
    public void testNegativeHoursOnly() throws Exception {
        assertThat(-4 * 60, "-04");
    }

    @Test
    public void testPositiveHoursOnly() throws Exception {
        assertThat(8 * 60, "+08");
    }

    @Test
    public void testShortHour() throws Exception {
        assertError("UTC+1");
    }

    @Test
    public void testShortMinute() throws Exception {
        assertError("UTC+01:3");
    }

    @Test
    public void testUTCCamelCasePositive() throws Exception {
        assertThat(9 * 60, "uTc+09:00");
    }

    @Test
    public void testUTCNegative() throws Exception {
        assertThat(-4 * 60 - 15, "UTC-04:15");
    }

    @Test
    public void testUTCPositive() throws Exception {
        assertThat(9 * 60, "UTC+09:00");
    }

    private static void assertError(String offset) {
        Assert.assertEquals(Long.MIN_VALUE, Dates.parseOffset(offset, 0, offset.length()));
    }

    private static void assertThat(int expected, String offset) {
        long r = Dates.parseOffset(offset, 0, offset.length());
        Assert.assertEquals(expected, Numbers.decodeInt(r));
    }
}
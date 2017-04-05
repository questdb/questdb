package com.questdb.std.time;

import com.questdb.std.time.Dates;
import org.junit.Assert;
import org.junit.Test;

public class OffsetParserTest {

    @Test
    public void testBadDelim() throws Exception {
        assertThat(Long.MIN_VALUE, "UTC+01x30");
    }

    @Test
    public void testBadGMT() throws Exception {
        assertThat(Long.MIN_VALUE, "GMX+03:30");
    }

    @Test
    public void testBadHour() throws Exception {
        assertThat(Long.MIN_VALUE, "UTC+0x:30");
    }

    @Test
    public void testBadMinute() throws Exception {
        assertThat(Long.MIN_VALUE, "UTC+01:3x");
    }

    @Test
    public void testBadSign() throws Exception {
        assertThat(Long.MIN_VALUE, "GMT*08:00");
    }

    @Test
    public void testBadStart() throws Exception {
        assertThat(Long.MIN_VALUE, "*08");
    }

    @Test
    public void testBadUtc() throws Exception {
        assertThat(Long.MIN_VALUE, "UTX+09:00");
    }

    @Test
    public void testGMTCamelCasePositive() throws Exception {
        assertThat(2 * Dates.HOUR_MILLIS + 15 * Dates.MINUTE_MILLIS, "gMt+02:15");
    }

    @Test
    public void testGMTNegative() throws Exception {
        assertThat(-2 * Dates.HOUR_MILLIS + -15 * Dates.MINUTE_MILLIS, "gMt-02:15");
    }

    @Test
    public void testGMTPositive() throws Exception {
        assertThat(3 * Dates.HOUR_MILLIS + 30 * Dates.MINUTE_MILLIS, "GMT+03:30");
    }

    @Test
    public void testHourMinNoDelim() throws Exception {
        assertThat(8 * Dates.HOUR_MILLIS + 15 * Dates.MINUTE_MILLIS, "0815");
    }

    @Test
    public void testHourMinNoDelimNegative() throws Exception {
        assertThat(-3 * Dates.HOUR_MILLIS - 30 * Dates.MINUTE_MILLIS, "-0330");
    }

    @Test
    public void testHourMinNoDelimPositive() throws Exception {
        assertThat(8 * Dates.HOUR_MILLIS + 15 * Dates.MINUTE_MILLIS, "+0815");
    }

    @Test
    public void testHoursOnly() throws Exception {
        assertThat(8 * Dates.HOUR_MILLIS, "08");
    }

    @Test
    public void testMissingHour() throws Exception {
        assertThat(Long.MIN_VALUE, "UTC+");
    }

    @Test
    public void testMissingMinute() throws Exception {
        assertThat(Long.MIN_VALUE, "UTC+01:");
    }

    @Test
    public void testNegativeHoursOnly() throws Exception {
        assertThat(-4 * Dates.HOUR_MILLIS, "-04");
    }

    @Test
    public void testPositiveHoursOnly() throws Exception {
        assertThat(8 * Dates.HOUR_MILLIS, "+08");
    }

    @Test
    public void testShortHour() throws Exception {
        assertThat(Long.MIN_VALUE, "UTC+1");
    }

    @Test
    public void testShortMinute() throws Exception {
        assertThat(Long.MIN_VALUE, "UTC+01:3");
    }

    @Test
    public void testUTCCamelCasePositive() throws Exception {
        assertThat(32400000L, "uTc+09:00");
    }

    @Test
    public void testUTCNegative() throws Exception {
        assertThat(-4 * Dates.HOUR_MILLIS - 15 * Dates.MINUTE_MILLIS, "UTC-04:15");
    }

    @Test
    public void testUTCPositive() throws Exception {
        assertThat(32400000L, "UTC+09:00");
    }

    private static void assertThat(long expected, String offset) {
        Assert.assertEquals(expected, Dates.parseOffset(offset, 0, offset.length()));
    }
}
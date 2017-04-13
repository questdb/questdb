package com.questdb.std.time;

import com.questdb.ex.NumericException;
import com.questdb.misc.Chars;
import com.questdb.misc.Numbers;

public class DateFormatUtils {
    public static void assertChar(char c, CharSequence in, int pos, int hi) throws NumericException {
        assertRemaining(pos, hi);
        if (in.charAt(pos) != c) {
            throw NumericException.INSTANCE;
        }
    }

    static int assertString(CharSequence delimiter, int len, CharSequence in, int pos, int hi) throws NumericException {
        assertRemaining(pos + len - 1, hi);
        if (!Chars.equals(delimiter, in, pos, pos + len)) {
            throw NumericException.INSTANCE;
        }
        return pos + len;
    }

    public static void assertRemaining(int pos, int hi) throws NumericException {
        if (pos < hi) {
            return;
        }
        throw NumericException.INSTANCE;
    }

    public static long computeMillis(int year, int month, int day, int hour, int minute, int second, int millis, int timezone, long offset, DateLocale locale, int pos, int hi) throws NumericException {
        // extra input
        if (pos < hi) {
            throw NumericException.INSTANCE;
        }

        boolean leap = Dates.isLeapYear(year);

        // wrong month
        if (month < 1 || month > 12) {
            throw NumericException.INSTANCE;
        }

        if (hour < 0 || hour > 23) {
            throw NumericException.INSTANCE;
        }

        // wrong day of month
        if (day < 1 || day > Dates.getDaysPerMonth(month, leap)) {
            throw NumericException.INSTANCE;
        }

        long datetime = Dates.yearMillis(year, leap)
                + Dates.monthOfYearMillis(month, leap)
                + (day - 1) * Dates.DAY_MILLIS
                + hour * Dates.HOUR_MILLIS
                + minute * Dates.MINUTE_MILLIS
                + second * Dates.SECOND_MILLIS
                + millis;

        if (timezone > -1) {
            datetime -= locale.getZoneRules(timezone).getOffset(datetime, year, leap);
        } else if (offset > Long.MIN_VALUE) {
            datetime -= offset;
        }

        return datetime;
    }
}

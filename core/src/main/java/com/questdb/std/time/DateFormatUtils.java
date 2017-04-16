package com.questdb.std.time;

import com.questdb.ex.NumericException;
import com.questdb.misc.Chars;
import com.questdb.misc.Numbers;

public class DateFormatUtils {
    public static final int HOUR_24 = 2;
    public static final int HOUR_PM = 1;
    public static final int HOUR_AM = 0;
    static long referenceYear;
    static int thisCenturyLimit;
    static int thisCenturyLow;
    static int prevCenturyLow;
    private static long newYear;

    public static long getReferenceYear() {
        return referenceYear;
    }

    public static void init() {
        // do nothing method to make sure class is loaded and its static code has run
    }

    public static void updateReferenceYear(long millis) {
        referenceYear = millis;

        int referenceYear = Dates.getYear(millis);
        int centuryOffset = referenceYear % 100;
        thisCenturyLimit = centuryOffset + 20;
        if (thisCenturyLimit > 100) {
            thisCenturyLimit = thisCenturyLimit % 100;
            thisCenturyLow = referenceYear - centuryOffset + 100;
        } else {
            thisCenturyLow = referenceYear - centuryOffset;
        }
        prevCenturyLow = thisCenturyLow - 100;
        newYear = Dates.endOfYear(referenceYear);
    }

    static void assertChar(char c, CharSequence in, int pos, int hi) throws NumericException {
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

    static void assertRemaining(int pos, int hi) throws NumericException {
        if (pos < hi) {
            return;
        }
        throw NumericException.INSTANCE;
    }

    static void assertNoTail(int pos, int hi) throws NumericException {
        if (pos < hi) {
            throw NumericException.INSTANCE;
        }
    }

    static long compute(
            DateLocale locale,
            int era,
            int year,
            int month,
            int day,
            int hour,
            int minute,
            int second,
            int millis,
            int timezone,
            long offset,
            int hourType) throws NumericException {

        if (era == 0) {
            year = -(year - 1);
        }

        boolean leap = Dates.isLeapYear(year);

        // wrong month
        if (month < 1 || month > 12) {
            throw NumericException.INSTANCE;
        }

        switch (hourType) {
            case HOUR_PM:
                hour += 12;
            case HOUR_24:
                // wrong hour
                if (hour < 0 || hour > 23) {
                    throw NumericException.INSTANCE;
                }
                break;
            default:
                // wrong 12-hour clock hour
                if (hour < 0 || hour > 11) {
                    throw NumericException.INSTANCE;
                }
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

    static long parseYearGreedy(CharSequence in, int pos, int hi) throws NumericException {
        long l = Numbers.parseIntSafely(in, pos, hi);
        int len = Numbers.decodeLen(l);
        int year;
        if (len == 2) {
            year = adjustYear(Numbers.decodeInt(l));
        } else {
            year = Numbers.decodeInt(l);
        }

        return Numbers.encodeIntAndLen(year, len);
    }

    static int adjustYear(int year) {
        return (year < thisCenturyLimit ? thisCenturyLow : prevCenturyLow) + year;
    }

    static {
        updateReferenceYear(System.currentTimeMillis());
    }
}

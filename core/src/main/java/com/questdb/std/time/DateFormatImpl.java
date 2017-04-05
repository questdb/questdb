package com.questdb.std.time;

import com.questdb.ex.NumericException;
import com.questdb.misc.Chars;
import com.questdb.std.IntList;
import com.questdb.std.ObjList;
import com.questdb.misc.Numbers;

public class DateFormatImpl implements DateFormat {
    public static final int HOUR_24 = 1;
    public static final int HOUR_AM = 2;
    public static final int HOUR_PM = 3;
    private static long referenceYear;
    private static int thisCenturyLimit;
    private static int thisCenturyLow;
    private static int prevCenturyLow;
    private static long newYear;
    private final IntList compiledOps;
    private final ObjList<String> delimiters;

    public DateFormatImpl(IntList compiledOps, ObjList<String> delimiters) {
        this.compiledOps = compiledOps;
        this.delimiters = delimiters;
    }

    public static long getReferenceYear() {
        return referenceYear;
    }

    public static void updateReferenceYear(long millis) {
        DateFormatImpl.referenceYear = millis;

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

    @Override
    public long parse(CharSequence in, DateLocale locale) throws NumericException {
        return parse(in, 0, in.length(), locale);
    }

    @Override
    public long parse(CharSequence in, int lo, int hi, DateLocale locale) throws NumericException {
        int day = 1;
        int month = 1;
        int year = 1970;
        int hour = 0;
        int minute = 0;
        int second = 0;
        int millis = 0;
        int era = 1;
        int timezone = -1;

        int hourType = HOUR_24;

        int pos = lo;
        long l;
        int len;

        for (int i = 0, n = compiledOps.size(); i < n; i++) {

            int op = compiledOps.getQuick(i);
            switch (op) {

                // AM/PM
                case DateFormatCompiler.OP_AM_PM:
                    l = locale.matchAMPM(in, pos, hi);
                    hourType = Numbers.decodeInt(l) == 0 ? HOUR_AM : HOUR_PM;
                    pos += Numbers.decodeLen(l);
                    break;

                // MILLIS
                case DateFormatCompiler.OP_MILLIS_ONE_DIGIT:
                    assertRemaining(pos, hi);
                    millis = Numbers.parseInt(in, pos, ++pos);
                    break;

                case DateFormatCompiler.OP_MILLIS_THREE_DIGITS:
                    assertRemaining(pos + 2, hi);
                    millis = Numbers.parseInt(in, pos, pos += 3);
                    break;

                case DateFormatCompiler.OP_MILLIS_GREEDY:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    millis = Numbers.decodeInt(l);
                    pos += Numbers.decodeLen(l);
                    break;

                // SECOND
                case DateFormatCompiler.OP_SECOND_ONE_DIGIT:
                    assertRemaining(pos, hi);
                    second = Numbers.parseInt(in, pos, ++pos);
                    break;

                case DateFormatCompiler.OP_SECOND_TWO_DIGITS:
                    assertRemaining(pos + 1, hi);
                    second = Numbers.parseInt(in, pos, pos += 2);
                    break;

                case DateFormatCompiler.OP_SECOND_GREEDY:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    second = Numbers.decodeInt(l);
                    pos += Numbers.decodeLen(l);
                    break;

                // MINUTE
                case DateFormatCompiler.OP_MINUTE_ONE_DIGIT:
                    assertRemaining(pos, hi);
                    minute = Numbers.parseInt(in, pos, ++pos);
                    break;

                case DateFormatCompiler.OP_MINUTE_TWO_DIGITS:
                    assertRemaining(pos + 1, hi);
                    minute = Numbers.parseInt(in, pos, pos += 2);
                    break;

                case DateFormatCompiler.OP_MINUTE_GREEDY:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    minute = Numbers.decodeInt(l);
                    pos += Numbers.decodeLen(l);
                    break;

                // HOUR (0-11)
                case DateFormatCompiler.OP_HOUR_12_ONE_DIGIT:
                    assertRemaining(pos, hi);
                    hour = Numbers.parseInt(in, pos, ++pos);
                    if (hourType == HOUR_24) {
                        hourType = HOUR_AM;
                    }
                    break;

                case DateFormatCompiler.OP_HOUR_12_TWO_DIGITS:
                    assertRemaining(pos + 1, hi);
                    hour = Numbers.parseInt(in, pos, pos += 2);
                    if (hourType == HOUR_24) {
                        hourType = HOUR_AM;
                    }
                    break;

                case DateFormatCompiler.OP_HOUR_12_GREEDY:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    hour = Numbers.decodeInt(l);
                    pos += Numbers.decodeLen(l);
                    if (hourType == HOUR_24) {
                        hourType = HOUR_AM;
                    }
                    break;

                // HOUR (1-12)
                case DateFormatCompiler.OP_HOUR_12_ONE_DIGIT_ONE_BASED:
                    assertRemaining(pos, hi);
                    hour = Numbers.parseInt(in, pos, ++pos) - 1;
                    if (hourType == HOUR_24) {
                        hourType = HOUR_AM;
                    }
                    break;

                case DateFormatCompiler.OP_HOUR_12_TWO_DIGITS_ONE_BASED:
                    assertRemaining(pos + 1, hi);
                    hour = Numbers.parseInt(in, pos, pos += 2) - 1;
                    if (hourType == HOUR_24) {
                        hourType = HOUR_AM;
                    }
                    break;

                case DateFormatCompiler.OP_HOUR_12_GREEDY_ONE_BASED:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    hour = Numbers.decodeInt(l) - 1;
                    pos += Numbers.decodeLen(l);
                    if (hourType == HOUR_24) {
                        hourType = HOUR_AM;
                    }
                    break;

                // HOUR (0-23)
                case DateFormatCompiler.OP_HOUR_24_ONE_DIGIT:
                    assertRemaining(pos, hi);
                    hour = Numbers.parseInt(in, pos, ++pos);
                    break;

                case DateFormatCompiler.OP_HOUR_24_TWO_DIGITS:
                    assertRemaining(pos + 1, hi);
                    hour = Numbers.parseInt(in, pos, pos += 2);
                    break;

                case DateFormatCompiler.OP_HOUR_24_GREEDY:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    hour = Numbers.decodeInt(l);
                    pos += Numbers.decodeLen(l);
                    break;

                // HOUR (1 - 24)
                case DateFormatCompiler.OP_HOUR_24_ONE_DIGIT_ONE_BASED:
                    assertRemaining(pos, hi);
                    hour = Numbers.parseInt(in, pos, ++pos) - 1;
                    break;

                case DateFormatCompiler.OP_HOUR_24_TWO_DIGITS_ONE_BASED:
                    assertRemaining(pos + 1, hi);
                    hour = Numbers.parseInt(in, pos, pos += 2) - 1;
                    break;

                case DateFormatCompiler.OP_HOUR_24_GREEDY_ONE_BASED:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    hour = Numbers.decodeInt(l) - 1;
                    pos += Numbers.decodeLen(l);
                    break;

                // DAY
                case DateFormatCompiler.OP_DAY_ONE_DIGIT:
                    assertRemaining(pos, hi);
                    day = Numbers.parseInt(in, pos, ++pos);
                    break;
                case DateFormatCompiler.OP_DAY_TWO_DIGITS:
                    assertRemaining(pos + 1, hi);
                    day = Numbers.parseInt(in, pos, pos += 2);
                    break;
                case DateFormatCompiler.OP_DAY_GREEDY:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    day = Numbers.decodeInt(l);
                    pos += Numbers.decodeLen(l);
                    break;

                case DateFormatCompiler.OP_DAY_NAME_LONG:
                case DateFormatCompiler.OP_DAY_NAME_SHORT:
                    l = locale.matchWeekday(in, pos, hi);
                    // ignore weekday
                    pos += Numbers.decodeLen(l);
                    break;

                case DateFormatCompiler.OP_DAY_OF_WEEK:
                    assertRemaining(pos + 1, hi);
                    // ignore weekday
                    Numbers.parseInt(in, pos, ++pos);
                    break;

                // MONTH

                case DateFormatCompiler.OP_MONTH_ONE_DIGIT:
                    assertRemaining(pos, hi);
                    month = Numbers.parseInt(in, pos, ++pos);
                    break;
                case DateFormatCompiler.OP_MONTH_TWO_DIGITS:
                    assertRemaining(pos + 1, hi);
                    month = Numbers.parseInt(in, pos, pos += 2);
                    break;
                case DateFormatCompiler.OP_MONTH_GREEDY:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    month = Numbers.decodeInt(l);
                    pos += Numbers.decodeLen(l);
                    break;

                case DateFormatCompiler.OP_MONTH_SHORT_NAME:
                case DateFormatCompiler.OP_MONTH_LONG_NAME:
                    l = locale.matchMonth(in, pos, hi);
                    month = Numbers.decodeInt(l) + 1;
                    pos += Numbers.decodeLen(l);
                    break;

                // YEAR

                case DateFormatCompiler.OP_YEAR_ONE_DIGIT:
                    assertRemaining(pos, hi);
                    year = Numbers.parseInt(in, pos, ++pos);
                    break;
                case DateFormatCompiler.OP_YEAR_TWO_DIGITS:
                    assertRemaining(pos + 1, hi);
                    year = adjustYear(Numbers.parseInt(in, pos, pos += 2));
                    break;
                case DateFormatCompiler.OP_YEAR_FOUR_DIGITS:
                    assertRemaining(pos + 3, hi);
                    year = Numbers.parseInt(in, pos, pos += 4);
                    break;
                case DateFormatCompiler.OP_YEAR_GREEDY:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    len = Numbers.decodeLen(l);
                    if (len == 2) {
                        year = adjustYear(Numbers.decodeInt(l));
                    } else {
                        year = Numbers.decodeInt(l);
                    }
                    pos += len;
                    break;

                // ERA
                case DateFormatCompiler.OP_ERA:
                    l = locale.matchEra(in, pos, hi);
                    era = Numbers.decodeInt(l);
                    pos += Numbers.decodeLen(l);
                    break;

                // TIMEZONE
                case DateFormatCompiler.OP_TIME_ZONE_SHORT:
                    l = locale.matchZone(in, pos, hi);
                    timezone = Numbers.decodeInt(l);
                    pos += Numbers.decodeLen(l);
                    break;

                // SEPARATORS
                default:
                    String delimiter = delimiters.getQuick(-op - 1);
                    len = delimiter.length();
                    assertRemaining(pos + len - 1, hi);
                    // unexpected separator character
                    if (!Chars.equals(delimiter, in, pos, pos + len)) {
                        throw NumericException.INSTANCE;
                    }
                    pos += len;
                    break;
            }
        }

        // extra input
        if (pos < hi) {
            throw NumericException.INSTANCE;
        }

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
            datetime -= locale.getZoneRules(timezone).getOffset(datetime);
        }

        return datetime;
    }

    private static void assertRemaining(int pos, int hi) throws NumericException {
        if (pos < hi) {
            return;
        }
        throw NumericException.INSTANCE;
    }

    private int adjustYear(int year) {
        return (year < thisCenturyLimit ? thisCenturyLow : prevCenturyLow) + year;
    }

    static {
        updateReferenceYear(System.currentTimeMillis());
    }
}

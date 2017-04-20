package com.questdb.std.time;

import com.questdb.ex.NumericException;
import com.questdb.misc.Chars;
import com.questdb.misc.Numbers;
import com.questdb.std.str.CharSink;

public class DateFormatUtils {
    public static final int HOUR_24 = 2;
    public static final int HOUR_PM = 1;
    public static final int HOUR_AM = 0;
    public static final DateFormat UTC_FORMAT;
    public static final DateFormat FMT1;
    public static final DateFormat FMT2;
    public static final DateFormat FMT3;
    static final String UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.SSSz";
    private static final DateFormat FMT4;
    private static final DateFormat HTTP_FORMAT;
    private static final DateFormat TIME24;
    private static final DateLocale defaultLocale = DateLocaleFactory.INSTANCE.getDateLocale("en-GB");
    static long referenceYear;
    static int thisCenturyLimit;
    static int thisCenturyLow;
    static int prevCenturyLow;
    private static long newYear;

    // YYYY-MM-DDThh:mm:ss.mmmmZ
    public static void appendDateTime(CharSink sink, long millis) {
        if (millis == Long.MIN_VALUE) {
            return;
        }
        UTC_FORMAT.format(millis, defaultLocale, "Z", sink);
    }

    // YYYY-MM-DD
    public static void formatDashYYYYMMDD(CharSink sink, long millis) {
        int y = Dates.getYear(millis);
        boolean l = Dates.isLeapYear(y);
        int m = Dates.getMonthOfYear(millis, y, l);
        Numbers.append(sink, y);
        Dates.append0(sink.put('-'), m);
        Dates.append0(sink.put('-'), Dates.getDayOfMonth(millis, y, m, l));
    }

    public static void formatHTTP(CharSink sink, long millis) {
        HTTP_FORMAT.format(millis, defaultLocale, "GMT", sink);
    }

    public static void formatMMMDYYYY(CharSink sink, long millis) {
        FMT4.format(millis, defaultLocale, "Z", sink);
    }

    // YYYY
    public static void formatYYYY(CharSink sink, long millis) {
        Numbers.append(sink, Dates.getYear(millis));
    }

    // YYYY-MM
    public static void formatYYYYMM(CharSink sink, long millis) {
        int y = Dates.getYear(millis);
        int m = Dates.getMonthOfYear(millis, y, Dates.isLeapYear(y));
        Numbers.append(sink, y);
        Dates.append0(sink.put('-'), m);
    }

    // YYYYMMDD
    public static void formatYYYYMMDD(CharSink sink, long millis) {
        int y = Dates.getYear(millis);
        boolean l = Dates.isLeapYear(y);
        int m = Dates.getMonthOfYear(millis, y, l);
        Numbers.append(sink, y);
        Dates.append0(sink, m);
        Dates.append0(sink, Dates.getDayOfMonth(millis, y, m, l));
    }

    public static long getReferenceYear() {
        return referenceYear;
    }

    public static void init() {
        // do nothing method to make sure class is loaded and its static code has run
    }

    public static void main(String[] args) throws NumericException {
        String s = "2017-04-01T00:00:00.000Z";
        DateFormat f = new DateFormatCompiler().compile(UTC_PATTERN, true);
        System.out.println(f.parse(s, defaultLocale));
    }

    // YYYY-MM-DDThh:mm:ss.mmm
    public static long parseDateTime(CharSequence seq) throws NumericException {
        return parseDateTime(seq, 0, seq.length());
    }

    public static long parseDateTimeFmt1(CharSequence seq) throws NumericException {
        return parseDateTimeFmt1(seq, 0, seq.length());
    }

    public static long parseDateTimeFmt2(CharSequence seq) throws NumericException {
        return parseDateTimeFmt2(seq, 0, seq.length());
    }

    public static long parseDateTimeFmt3(CharSequence seq) throws NumericException {
        return FMT3.parse(seq, defaultLocale);
    }

    // YYYY-MM-DDThh:mm:ss.mmm
    public static long parseDateTimeQuiet(CharSequence seq) {
        try {
            return parseDateTime(seq, 0, seq.length());
        } catch (NumericException e) {
            return Long.MIN_VALUE;
        }
    }

    public static long parseTime24(CharSequence seq) throws NumericException {
        return TIME24.parse(seq, defaultLocale);
    }

    public static long tryParse(CharSequence s) throws NumericException {
        return tryParse(s, 0, s.length());
    }

    public static long tryParse(CharSequence s, int lo, int lim) throws NumericException {
        try {
            return parseDateTime(s, lo, lim);
        } catch (NumericException ignore) {
        }

        try {
            return parseDateTimeFmt1(s, lo, lim);
        } catch (NumericException ignore) {
        }

        return parseDateTimeFmt2(s, lo, lim);
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

    static void appendAmPm(CharSink sink, int hour, DateLocale locale) {
        if (hour < 12) {
            sink.put(locale.getAMPM(0));
        } else {
            sink.put(locale.getAMPM(1));
        }
    }

    static void assertChar(char c, CharSequence in, int pos, int hi) throws NumericException {
        assertRemaining(pos, hi);
        if (in.charAt(pos) != c) {
            throw NumericException.INSTANCE;
        }
    }

    static int assertString(CharSequence delimiter, int len, CharSequence in, int pos, int hi) throws NumericException {
        if (delimiter.charAt(0) == '\'' && delimiter.charAt(len - 1) == '\'') {
            assertRemaining(pos + len - 3, hi);
            if (!Chars.equals(delimiter, 1, len - 1, in, pos, pos + len - 2)) {
                throw NumericException.INSTANCE;
            }
            return pos + len - 2;
        } else {
            assertRemaining(pos + len - 1, hi);
            if (!Chars.equals(delimiter, in, pos, pos + len)) {
                throw NumericException.INSTANCE;
            }
            return pos + len;
        }
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

        if (hour < 0 || hour > 23) {
            throw NumericException.INSTANCE;
        }

        if (minute < 0 || minute > 59) {
            throw NumericException.INSTANCE;
        }

        if (second < 0 || second > 59) {
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

    static long parseYearFourDigits(CharSequence in, int pos, int hi) throws NumericException {
        return Numbers.parseIntSafely(in, pos, hi);
    }

    static int adjustYear(int year) {
        return (year < thisCenturyLimit ? thisCenturyLow : prevCenturyLow) + year;
    }

    static void appendHour12(CharSink sink, int hour) {
        if (hour < 12) {
            sink.put(hour);
        } else {
            sink.put(hour - 12);
        }
    }

    static void appendHour12Padded(CharSink sink, int hour) {
        if (hour < 12) {
            Dates.append0(sink, hour);
        } else {
            Dates.append0(sink, hour - 12);
        }
    }

    static void appendHour121Padded(CharSink sink, int hour) {
        if (hour < 12) {
            Dates.append0(sink, hour + 1);
        } else {
            Dates.append0(sink, hour - 11);
        }
    }

    static void appendHour121(CharSink sink, int hour) {
        if (hour < 12) {
            sink.put(hour + 1);
        } else {
            sink.put(hour - 11);
        }
    }

    static void appendEra(CharSink sink, int year, DateLocale locale) {
        if (year < 0) {
            sink.put(locale.getEra(0));
        } else {
            sink.put(locale.getEra(1));
        }
    }

    private static long parseDateTime(CharSequence seq, int lo, int lim) throws NumericException {
        return UTC_FORMAT.parse(seq, lo, lim, defaultLocale);
    }

    // YYYY-MM-DD hh:mm:ss
    static long parseDateTimeFmt1(CharSequence seq, int lo, int lim) throws NumericException {
        return FMT1.parse(seq, lo, lim, defaultLocale);
    }

    // MM/DD/YYYY
    static long parseDateTimeFmt2(CharSequence seq, int lo, int lim) throws NumericException {
        return FMT2.parse(seq, lo, lim, defaultLocale);
    }

    static {
        updateReferenceYear(System.currentTimeMillis());
        DateFormatCompiler compiler = new DateFormatCompiler();
        UTC_FORMAT = compiler.compile(UTC_PATTERN, false);
        HTTP_FORMAT = compiler.compile("E, d MMM yyyy HH:mm:ss Z", false);
        FMT1 = compiler.compile("yyyy-MM-dd HH:mm:ss", false);
        FMT3 = compiler.compile("dd/MM/y", false);
        FMT2 = compiler.compile("MM/dd/y", false);
        FMT4 = compiler.compile("MMM d yyyy", false);
        TIME24 = compiler.compile("H:m", false);
    }
}

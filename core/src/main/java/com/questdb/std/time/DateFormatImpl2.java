package com.questdb.std.time;

import com.questdb.ex.NumericException;
import com.questdb.misc.Numbers;
import com.questdb.std.str.CharSink;

public class DateFormatImpl2 extends AbstractDateFormat {
    private static long referenceYear;
    private static int thisCenturyLimit;
    private static int thisCenturyLow;
    private static int prevCenturyLow;
    private static long newYear;

    public static long getReferenceYear() {
        return referenceYear;
    }

    public static void main(String[] args) throws NumericException {
        DateFormatImpl2 fmt = new DateFormatImpl2();
        fmt.parse("Mon, 07 Jun 2017 15:37:40 GMT", DateLocaleFactory.INSTANCE.getDateLocale("en-GB"));
    }

    public static void updateReferenceYear(long millis) {
        DateFormatImpl2.referenceYear = millis;

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
    public void append(long datetime, DateLocale locale, CharSequence timeZoneName, CharSink sink) throws NumericException {
    }

    @Override
    public long parse(CharSequence in, int lo, int hi, DateLocale locale) throws NumericException {
        // "E, dd MMM yyyy HH:mm:ss z"
        int day = 1;
        int month = 1;
        int year = 1970;
        int hour = 0;
        int minute = 0;
        int second = 0;
        int millis = 0;
        int pos = lo;
        long l;
        int timezone = -1;
        long offset = 0;
        // E
        l = locale.matchWeekday(in, pos, hi);
        pos += Numbers.decodeLen(l);

/*
        // random chars
        pos = DateFormatUtils.assertString(", ", 2, in, pos, hi);

        // dd
        DateFormatUtils.assertRemaining(pos + 1, hi);
        day = Numbers.parseInt(in, pos, pos += 2);

        DateFormatUtils.assertChar(' ', in, pos++, hi);

*/

        // MMM
        l = locale.matchMonth(in, pos, hi);
        month = Numbers.decodeInt(l) + 1;
        pos += Numbers.decodeLen(l);

        DateFormatUtils.assertChar(' ', in, pos++, hi);

        // yyyy
        DateFormatUtils.assertRemaining(pos + 3, hi);
        year = Numbers.parseInt(in, pos, pos += 4);

        DateFormatUtils.assertChar(' ', in, pos++, hi);
/*
        // HH
        DateFormatUtils.assertRemaining(pos + 1, hi);
        hour = Numbers.parseInt(in, pos, pos += 2);

        DateFormatUtils.assertChar(':', in, pos++, hi);

        // mm
        DateFormatUtils.assertRemaining(pos + 1, hi);
        minute = Numbers.parseInt(in, pos, pos += 2);

        DateFormatUtils.assertChar(':', in, pos++, hi);

        // ss
        DateFormatUtils.assertRemaining(pos + 1, hi);
        second = Numbers.parseInt(in, pos, pos += 2);

//        DateFormatUtils.assertChar(' ', in, pos, hi);
//        pos++;
//
*/

        // z
        l = Dates.parseOffset(in, pos, hi);
        if (l == Long.MIN_VALUE) {
            l = locale.matchZone(in, pos, hi);
            timezone = Numbers.decodeInt(l);
        } else {
            offset = Numbers.decodeInt(l) * Dates.MINUTE_MILLIS;
        }
        pos += Numbers.decodeLen(l);

        return 0;
//        return DateFormatUtils.computeMillis(
//                year,
//                month,
//                day,
//                hour,
//                minute,
//                second,
//                millis,
//                timezone,
//                offset,
//                locale,
//                pos,
//                hi
//        );
    }

    @Override
    public long parse(CharSequence in, DateLocale locale) throws NumericException {
        return parse(in, 0, in.length(), locale);
    }

    static {
        updateReferenceYear(System.currentTimeMillis());
    }
}

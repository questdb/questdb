package com.questdb.std.time;

import com.questdb.ex.NumericException;
import com.questdb.misc.Numbers;
import com.questdb.std.str.CharSink;

import static com.questdb.std.time.DateFormatUtils.HOUR_24;

public class DateFormatImpl2 extends AbstractDateFormat {

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
        int hourType = HOUR_24;
        int era = 1;

        // E
        l = locale.matchWeekday(in, pos, hi);
        pos += Numbers.decodeLen(l);

//        l = locale.matchAMPM(in, pos, hi);
//        hourType = Numbers.decodeInt(l) == 0 ? DateFormatUtils.HOUR_AM : DateFormatUtils.HOUR_PM;
//        pos += Numbers.decodeLen(l);
//

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

//
*/
        DateFormatUtils.assertChar(' ', in, pos, hi);
        pos++;

        // z
        l = Dates.parseOffset(in, pos, hi);
        if (l == Long.MIN_VALUE) {
            l = locale.matchZone(in, pos, hi);
            timezone = Numbers.decodeInt(l);
        } else {
            offset = Numbers.decodeInt(l) * Dates.MINUTE_MILLIS;
        }
        pos += Numbers.decodeLen(l);

        DateFormatUtils.assertNoTail(pos, hi);

        return DateFormatUtils.compute(
                locale,
                era,
                year,
                month,
                day,
                hour,
                minute,
                second,
                millis,
                timezone,
                offset,
                hourType
        );
    }
}

package com.questdb.std.time;

import com.questdb.ex.NumericException;
import com.questdb.misc.Numbers;
import com.questdb.std.str.CharSink;

import static com.questdb.std.time.DateFormatUtils.*;

public class DateFormatImpl2 extends AbstractDateFormat {

    @Override
    public void append(long datetime, DateLocale locale, CharSequence timeZoneName, CharSink sink) throws NumericException {
    }

    @Override
    public long parse(CharSequence in, int lo, int hi, DateLocale locale) throws NumericException {
        // "E, dd MMM yyyy HH:mm:ss z"
        int day = 1;
        int month;
        int year;
        int hour;
        int minute = 0;
        int second = 0;
        int millis = 0;
        int pos = lo;

        long l = 0;
        int timezone = -1;
        long offset = Long.MIN_VALUE;
        int hourType = HOUR_24;
        int era = 1;

        l = DateFormatUtils.parseYearGreedy(in, pos, hi);
        year = Numbers.decodeInt(l);
        pos += Numbers.decodeLen(l);

        // KK
        assertRemaining(pos + 1, hi);
        hour = Numbers.parseInt(in, pos, pos += 2);
        if (hourType == HOUR_24) {
            hourType = HOUR_AM;
        }

        assertRemaining(pos + 1, hi);
        month = Numbers.parseInt(in, pos, pos += 2);


        l = locale.matchAMPM(in, pos, hi);
        hourType = Numbers.decodeInt(l);
        pos += Numbers.decodeLen(l);

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

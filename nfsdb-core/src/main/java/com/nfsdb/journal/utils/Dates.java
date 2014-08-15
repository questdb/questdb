/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.journal.utils;

import com.nfsdb.journal.PartitionType;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.exceptions.JournalUnsupportedTypeException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

public final class Dates {

    public static DateTime utc(int year, int month, int day, int hour, int minute) {
        return new DateTime(year, month, day, hour, minute, DateTimeZone.UTC);
    }

    public static String toString(long millis) {
        return utc(millis).toString();
    }

    public static DateTime utc(long millis) {
        return new DateTime(millis, DateTimeZone.UTC);
    }

    public static Interval interval(String start, String end) {
        return interval(toMillis(start), toMillis(end));
    }

    public static Interval interval(long start, long end) {
        if (end < start) {
            return new Interval(end, start, DateTimeZone.UTC);
        } else {
            return new Interval(start, end, DateTimeZone.UTC);
        }
    }

    public static long toMillis(String date) {
        return new DateTime(date, DateTimeZone.UTC).getMillis();
    }

    public static Interval interval(DateTime start, DateTime end) {
        return interval(start.getMillis(), end.getMillis());
    }

    public static Interval lastMonths(int duration) {
        return lastMonths(utc(), duration);
    }

    public static DateTime utc() {
        return DateTime.now(DateTimeZone.UTC);
    }

    public static Interval intervalForDirName(String name, PartitionType partitionType) {
        switch (partitionType) {
            case YEAR:
                return intervalForDate(Dates.utc(name + "-01-01T00:00:00.000Z").getMillis(), partitionType);
            case MONTH:
                return intervalForDate(Dates.utc(name + "-01T00:00:00.000Z").getMillis(), partitionType);
            case DAY:
                return intervalForDate(Dates.utc(name + "T00:00:00.000Z").getMillis(), partitionType);
            case NONE:
                if ("default".equals(name)) {
                    return new Interval(0, Long.MAX_VALUE, DateTimeZone.UTC);
                }
            default:
                throw new JournalUnsupportedTypeException(partitionType);
        }
    }

    public static Interval intervalForDate(long timestamp, PartitionType partitionType) {
        switch (partitionType) {
            case NONE:
                return new Interval(0, Long.MAX_VALUE, DateTimeZone.UTC);
            default:
                long lo = intervalStart(timestamp, partitionType);
                long hi = intervalEnd(lo, partitionType);
                return new Interval(lo, hi, DateTimeZone.UTC);
        }
    }

    public static DateTime utc(String date) {
        return new DateTime(date, DateTimeZone.UTC);
    }

    public static String dirNameForIntervalStart(Interval interval, PartitionType partitionType) {
        switch (partitionType) {
            case YEAR:
                return interval.getStart().toString("YYYY");
            case MONTH:
                return interval.getStart().toString("YYYY-MM");
            case DAY:
                return interval.getStart().toString("YYYY-MM-dd");
            case NONE:
                return "default";
        }
        return "";
    }

    private Dates() {
    } // Prevent construction.

    private static Interval lastMonths(DateTime endDateTime, int duration) {
        if (duration < 1) {
            throw new JournalRuntimeException("Duration should be >= 1: %d", duration);
        }
        DateTime start = endDateTime.minusMonths(duration);
        return new Interval(start, endDateTime);
    }

    private static long intervalStart(long timestamp, PartitionType partitionType) {
        switch (partitionType) {
            case YEAR:
                return Dates.utc(timestamp).withMonthOfYear(1).withDayOfMonth(1).withTimeAtStartOfDay().getMillis();
            case MONTH:
                return Dates.utc(timestamp).withDayOfMonth(1).withTimeAtStartOfDay().getMillis();
            case DAY:
                return Dates.utc(timestamp).withTimeAtStartOfDay().getMillis();
        }
        return 0;
    }

    private static long intervalEnd(long start, PartitionType partitionType) {
        switch (partitionType) {
            case YEAR:
                return Dates.utc(start).plusYears(1).getMillis() - 1;
            case MONTH:
                return Dates.utc(start).plusMonths(1).getMillis() - 1;
            case DAY:
                return Dates.utc(start).plusDays(1).getMillis() - 1;
        }
        return 0;
    }
}

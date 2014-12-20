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
import com.nfsdb.journal.exceptions.JournalUnsupportedTypeException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

public final class Dates {

    private Dates() {
    } // Prevent construction.

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

    public static Interval lastMonth() {
        long millis = System.currentTimeMillis();
        return new Interval(millis - 30 * Dates2.DAY_MILLIS, millis);
    }

    public static DateTime utc() {
        return DateTime.now(DateTimeZone.UTC);
    }

    public static Interval intervalForDirName(String name, PartitionType partitionType) {
        switch (partitionType) {
            case YEAR:
                return intervalForDate(Dates2.parseDateTime(name + "-01-01T00:00:00.000Z"), partitionType);
            case MONTH:
                return intervalForDate(Dates2.parseDateTime(name + "-01T00:00:00.000Z"), partitionType);
            case DAY:
                return intervalForDate(Dates2.parseDateTime(name + "T00:00:00.000Z"), partitionType);
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

    private static long intervalStart(long timestamp, PartitionType partitionType) {
        switch (partitionType) {
            case YEAR:
                return Dates2.floorYYYY(timestamp);
            case MONTH:
                return Dates2.floorMM(timestamp);
            case DAY:
                return Dates2.floorDD(timestamp);
        }
        return 0;
    }

    private static long intervalEnd(long start, PartitionType partitionType) {
        switch (partitionType) {
            case YEAR:
                return Dates2.ceilYYYY(start);
            case MONTH:
                return Dates2.ceilMM(start);
            case DAY:
                return Dates2.ceilDD(start);
        }
        return 0;
    }
}

/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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
import com.nfsdb.journal.export.StringSink;

public final class Dates {

    private Dates() {
    } // Prevent construction.

    public static String toString(long millis) {
        StringSink sink = new StringSink();
        Dates2.appendDateTime(sink, millis);
        return sink.toString();
    }

    public static Interval interval(String start, String end) {
        return interval(toMillis(start), toMillis(end));
    }

    public static Interval interval(long start, long end) {
        if (end < start) {
            return new Interval(end, start);
        } else {
            return new Interval(start, end);
        }
    }

    public static long toMillis(String date) {
        return Dates2.parseDateTime(date);
    }

    public static Interval lastMonth() {
        long millis = System.currentTimeMillis();
        return new Interval(millis - 30 * Dates2.DAY_MILLIS, millis);
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
                    return new Interval(0, Long.MAX_VALUE);
                }
            default:
                throw new JournalUnsupportedTypeException(partitionType);
        }
    }

    public static Interval intervalForDate(long timestamp, PartitionType partitionType) {
        switch (partitionType) {
            case NONE:
                return new Interval(0, Long.MAX_VALUE);
            default:
                long lo = intervalStart(timestamp, partitionType);
                long hi = intervalEnd(lo, partitionType);
                return new Interval(lo, hi);
        }
    }

    public static String dirNameForIntervalStart(Interval interval, PartitionType partitionType) {
        StringSink sink = new StringSink();
        switch (partitionType) {
            case YEAR:
                Dates2.appendYear(sink, interval.getLo());
                break;
            case MONTH:
                Dates2.appendCalDate2(sink, interval.getLo());
                break;
            case DAY:
                Dates2.appendCalDate1(sink, interval.getLo());
                break;
            case NONE:
                return "default";
        }
        return sink.toString();
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

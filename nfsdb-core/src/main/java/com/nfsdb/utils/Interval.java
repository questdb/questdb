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

package com.nfsdb.utils;

import com.nfsdb.PartitionType;
import com.nfsdb.exceptions.JournalUnsupportedTypeException;
import com.nfsdb.io.sink.StringSink;

public class Interval {
    private final long lo;
    private final long hi;

    public Interval(long lo, long hi) {
        if (hi < lo) {
            this.lo = hi;
            this.hi = lo;
        } else {
            this.lo = lo;
            this.hi = hi;
        }
    }

    public Interval(CharSequence lo, CharSequence hi) {
        this(Dates.parseDateTime(lo), Dates.parseDateTime(hi));
    }

    public Interval(long millis, PartitionType t) {
        switch (t) {
            case YEAR:
                this.lo = Dates.floorYYYY(millis);
                this.hi = Dates.ceilYYYY(millis);
                break;
            case MONTH:
                this.lo = Dates.floorMM(millis);
                this.hi = Dates.ceilMM(millis);
                break;
            case DAY:
                this.lo = Dates.floorDD(millis);
                this.hi = Dates.ceilMM(millis);
                break;
            default:
                this.lo = 0;
                this.hi = Long.MAX_VALUE;
        }
    }

    public Interval(String dir, PartitionType t) {
        long millis;
        switch (t) {
            case YEAR:
                millis = Dates.parseDateTime(dir + "-01-01T00:00:00.000Z");
                this.lo = Dates.floorYYYY(millis);
                this.hi = Dates.ceilYYYY(millis);
                break;
            case MONTH:
                millis = Dates.parseDateTime(dir + "-01T00:00:00.000Z");
                this.lo = Dates.floorMM(millis);
                this.hi = Dates.ceilMM(millis);
                break;
            case DAY:
                millis = Dates.parseDateTime(dir + "T00:00:00.000Z");
                this.lo = Dates.floorDD(millis);
                this.hi = Dates.ceilDD(millis);
                break;
            default:
                if (!"default".equals(dir)) {
                    throw new JournalUnsupportedTypeException(t);
                }
                this.lo = 0;
                this.hi = Long.MAX_VALUE;
        }
    }

    public boolean contains(long x) {
        return (x >= lo && x < hi);
    }

    public String getDirName(PartitionType t) {
        StringSink sink = new StringSink();
        switch (t) {
            case YEAR:
                Dates.formatYYYY(sink, lo);
                break;
            case MONTH:
                Dates.formatYYYYMM(sink, lo);
                break;
            case DAY:
                Dates.formatDashYYYYMMDD(sink, lo);
                break;
            default:
                return "default";
        }
        return sink.toString();
    }

    public long getHi() {
        return hi;
    }

    public long getLo() {
        return lo;
    }

    @Override
    public int hashCode() {
        int result = (int) (lo ^ (lo >>> 32));
        return 31 * result + (int) (hi ^ (hi >>> 32));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Interval interval = (Interval) o;
        return hi == interval.hi && lo == interval.lo;
    }

    public boolean isAfter(long x) {
        return (lo > x);
    }

    public boolean isBefore(long x) {
        return hi <= x;
    }

    public boolean overlaps(Interval other) {
        return lo < other.hi && other.lo < hi;
    }
}


/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.misc;

import com.nfsdb.PartitionType;
import com.nfsdb.ex.JournalUnsupportedTypeException;
import com.nfsdb.ex.NumericException;
import com.nfsdb.io.sink.StringSink;

public class Interval {
    private long lo;
    private long hi;

    public Interval(long lo, long hi) {
        if (hi < lo) {
            this.lo = hi;
            this.hi = lo;
        } else {
            this.lo = lo;
            this.hi = hi;
        }
    }

    public Interval(CharSequence lo, CharSequence hi) throws NumericException {
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
                break;
        }
    }

    public Interval(String dir, PartitionType t) throws NumericException {
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

    @Override
    public String toString() {
        return "Interval{" +
                "lo=" + Dates.toString(lo) +
                ", hi=" + Dates.toString(hi) +
                '}';
    }

    public boolean isAfter(long x) {
        return (lo > x);
    }

    public boolean isBefore(long x) {
        return hi <= x;
    }

    public void update(long lo, long hi) {
        this.lo = lo;
        this.hi = hi;
    }
}


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

public class Interval {
    private final long lo;
    private final long hi;

    public Interval(long lo, long hi) {
        this.lo = lo;
        this.hi = hi;
    }

    public long getLo() {
        return lo;
    }

    public long getHi() {
        return hi;
    }

    public boolean contains(long x) {
        return (x >= lo && x < hi);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Interval interval = (Interval) o;
        return hi == interval.hi && lo == interval.lo;
    }

    @Override
    public int hashCode() {
        int result = (int) (lo ^ (lo >>> 32));
        result = 31 * result + (int) (hi ^ (hi >>> 32));
        return result;
    }
}


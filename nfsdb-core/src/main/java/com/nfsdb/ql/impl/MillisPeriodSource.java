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

package com.nfsdb.ql.impl;

import com.nfsdb.utils.Interval;

import java.util.Iterator;

public class MillisPeriodSource implements IntervalSource {
    private final Interval start;
    private final Interval next;
    private final long m;
    private final int count;
    private int pos;

    public MillisPeriodSource(Interval start, long m, int count) {
        this.start = start;
        this.m = m;
        this.count = count;
        this.next = new Interval(start.getLo(), start.getHi());
    }

    @Override
    public boolean hasNext() {
        return pos < count;
    }

    @Override
    public Interval next() {
        if (pos++ == 0) {
            return start;
        } else {
            next.update(next.getLo() + m, next.getHi() + m);
            return next;
        }
    }

    @Override
    public void remove() {

    }

    @Override
    public Iterator<Interval> iterator() {
        return this;
    }
}
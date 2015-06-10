/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 ******************************************************************************/
package com.nfsdb.ql.impl;

import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.utils.Dates;
import com.nfsdb.utils.Interval;

public class MonthsIntervalSource extends AbstractImmutableIterator<Interval> implements IntervalSource {
    private final Interval start;
    private final Interval next;
    private final int period;
    private final int count;
    private int pos;

    public MonthsIntervalSource(Interval start, int period, int count) {
        this.start = start;
        this.period = period;
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
            next.update(Dates.addMonths(next.getLo(), period), Dates.addMonths(next.getHi(), period));
            return next;
        }
    }

    @Override
    public void reset() {
        pos = 0;
        next.update(start.getLo(), start.getHi());
    }
}
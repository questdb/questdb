/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
 *
 ******************************************************************************/

package io.questdb.griffin.model;

import io.questdb.griffin.SqlException;
import io.questdb.std.LongList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjectFactory;

import java.io.Closeable;


public class StaticIntervalsModel implements Mutable, IntervalModel {
    public static final ObjectFactory<StaticIntervalsModel> FACTORY = StaticIntervalsModel::new;
    public static final int TRUE = 1;
    public static final int FALSE = 2;
    public static final int UNDEFINED = 0;
    private static final LongList INFINITE_INTERVAL;
    private final LongList reversableAInstance = new LongList();

    // This one can be overriden in the sequence of operations and then reversed back.
    private LongList intervalsA = reversableAInstance;
    private final LongList intervalsB = new LongList();
    private final LongList intervalsC = new LongList();
    public LongList intervals;
    public int intrinsicValue = UNDEFINED;
    private Interval tempInterval = new Interval();

    private Closeable reverseAToDefaultInstance = () -> intervalsA = reversableAInstance;

    public void applyIntersect(long lo, long hi, int period, char periodType, int count) {
        LongList temp = shuffleTemp(intervals, null);
        tempInterval.of(lo, hi, period, periodType, count);
        IntervalUtils.apply(temp, tempInterval);
        intersectIntervals(temp);
    }

    public void applySubtract(long lo, long hi, int period, char periodType, int count) {
        LongList temp = shuffleTemp(intervals, null);
        tempInterval.of(lo, hi, period, periodType, count);
        IntervalUtils.apply(temp, tempInterval);
        subtractIntervals(temp);
    }

    @Override
    public void clear() {
        intervalsA = reversableAInstance;
        intervals = null;
    }

    @Override
    public boolean hasIntervals() {
        return intervals != null;
    }

    @Override
    public void intersectEmpty() {
        intervals = intervalsA;
        intervals.clear();
    }

    @Override
    public void intersectIntervals(long lo, long hi) {
        LongList temp = shuffleTemp(intervals, null);
        temp.add(lo);
        temp.add(hi);
        intersectIntervals(temp);
    }

    @Override
    public void intersectIntervals(CharSequence seq, int lo, int lim, int position) throws SqlException {
        LongList temp = shuffleTemp(intervals, null);
        IntervalUtils.parseIntervalEx(seq, lo, lim, position, tempInterval);
        IntervalUtils.apply(temp, tempInterval);
        intersectIntervals(temp);
    }

    public Closeable overrideWith(LongList result) {
        intervalsA = result;
        intervals = result.size() > 0 ? result : null;
        return reverseAToDefaultInstance;
    }

    @Override
    public void subtractIntervals(long lo, long hi) {
        LongList temp = shuffleTemp(intervals, null);
        temp.add(lo);
        temp.add(hi);
        subtractIntervals(temp);
    }

    @Override
    public void subtractIntervals(CharSequence seq, int lo, int lim, int position) throws SqlException {
        LongList temp = shuffleTemp(intervals, null);
        IntervalUtils.parseIntervalEx(seq, lo, lim, position, tempInterval);
        IntervalUtils.apply(temp, tempInterval);
        subtractIntervals(temp);
    }

    @Override
    public boolean isEmptySet() {
        return intervals != null && intervals.size() == 0;
    }

    public void of(LongList intervals) {
        if (intervals != null){
            intervalsA.clear();
            this.intervals = intervalsA;
            this.intervals.add(intervals);
        } else {
            this.intervals = null;
        }
    }

    @Override
    public String toString() {
        return "StaticIntervalsModel{" +
                "intervalCount=" + (intervals != null ? intervals.size() : 0) +
                '}';
    }

    private void intersectIntervals(LongList intervals) {
        if (this.intervals == null) {
            this.intervals = intervals;
        } else {
            final LongList dest = shuffleTemp(intervals, this.intervals);
            IntervalUtils.intersect(intervals, this.intervals, dest);
            this.intervals = dest;
        }

        if (this.intervals.size() == 0) {
            intrinsicValue = FALSE;
        }
    }

    private LongList shuffleTemp(LongList src1, LongList src2) {
        LongList result = shuffleTemp0(src1, src2);
        result.clear();
        return result;
    }

    private LongList shuffleTemp0(LongList src1, LongList src2) {
        if (src2 != null) {
            if ((src1 == intervalsA && src2 == intervalsB) || (src1 == intervalsB && src2 == intervalsA)) {
                return intervalsC;
            }
            // this is the only possibility because we never return 'intervalsA' for two args
            return intervalsB;
        }

        if (src1 == intervalsA) {
            return intervalsB;
        }
        return intervalsA;
    }

    private void subtractIntervals(LongList temp) {
        IntervalUtils.invert(temp);
        if (this.intervals == null) {
            intervals = temp;
        } else {
            final LongList dest = shuffleTemp(temp, this.intervals);
            IntervalUtils.intersect(temp, this.intervals, dest);
            this.intervals = dest;
        }
        if (this.intervals.size() == 0) {
            intrinsicValue = FALSE;
        }
    }

    static {
        INFINITE_INTERVAL = new LongList();
        INFINITE_INTERVAL.add(Long.MIN_VALUE);
        INFINITE_INTERVAL.add(Long.MAX_VALUE);
    }

}

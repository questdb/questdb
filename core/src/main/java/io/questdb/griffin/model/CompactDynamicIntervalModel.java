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

import io.questdb.cairo.sql.Function;
import io.questdb.griffin.SqlException;
import io.questdb.std.LongList;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class CompactDynamicIntervalModel implements Mutable {
    private static final int INTERSECT_EQ_NO_PERIOD = Numbers.encodeLowHighShorts(IntervalOperation.INTERSECT_EQUALS, (short)0);
    private static final int INTERSECT_NO_PERIOD = Numbers.encodeLowHighShorts(IntervalOperation.INTERSECT, (short)0);
    private static final int SUBTRACT_NO_PERIOD = Numbers.encodeLowHighShorts(IntervalOperation.SUBTRACT, (short)0);
    private static final long EMPTY = Numbers.INT_NaN + 2;

    private final int STATIC_LONGS_PER_DYNAMIC_INTERVAL = 4;
    private final int LO_INDEX = 0;
    private final int HI_INDEX = 1;
    private final int OPERATION_PERIOD_TYPE_ADJUSTMENT_INDEX = 2;
    private final int PERIOD_COUNT_INDEX = 3;

    // All data needed to re-evaluate intervals
    // is stored in 2 lists - ListLong and List of functions
    // ListLongs has STATIC_LONGS_PER_DYNAMIC_INTERVAL entries per 1 dynamic interval
    // and pairs of static intervals in the end
    private final LongList staticPeriods = new LongList();
    private final ObjList<Function> dynamicRangeList = new ObjList<>();
    private final RuntimePeriodCursor cursor = new RuntimePeriodCursor();

    public void extractStaticIntervalsTo(LongList intervals) {
        int staticStart = STATIC_LONGS_PER_DYNAMIC_INTERVAL * dynamicRangeList.size();
        intervals.add(staticPeriods, staticStart, staticPeriods.size());
    }

    public RuntimePeriod getDynamicPeriod(int i) {
        assert i >= 0 && i < dynamicRangeList.size();
        return cursor.of(i);
    }

    @Override
    public void clear() {
        staticPeriods.clear();
        dynamicRangeList.clear();
    }

    public void intersect(long lo, Function hi, int adjustment) {
        staticPeriods.add(lo);
        staticPeriods.add(EMPTY);
        staticPeriods.add(Numbers.encodeLowHighInts(INTERSECT_NO_PERIOD, adjustment));
        staticPeriods.add(0L);

        dynamicRangeList.add(hi);
    }

    public void intersect(Function lo, long hi, int adjustment) {
        staticPeriods.add(EMPTY);
        staticPeriods.add(hi);
        staticPeriods.add(Numbers.encodeLowHighInts(INTERSECT_NO_PERIOD, adjustment));
        staticPeriods.add(0L);

        dynamicRangeList.add(lo);
    }

    public void intersectEquals(Function function) {
        staticPeriods.add(EMPTY);
        staticPeriods.add(EMPTY);
        staticPeriods.add(Numbers.encodeLowHighInts(INTERSECT_EQ_NO_PERIOD,0));
        staticPeriods.add(0L);

        dynamicRangeList.add(function);
    }


    public void intersect(long lo, long hi) {
        staticPeriods.add(lo);
        staticPeriods.add(hi);
        staticPeriods.add(Numbers.encodeLowHighInts(INTERSECT_NO_PERIOD,0));
        staticPeriods.add(0L);

        dynamicRangeList.add(null);
    }

    public void subtractInterval(long lo, long hi) {
        staticPeriods.add(lo);
        staticPeriods.add(hi);
        staticPeriods.add(Numbers.encodeLowHighInts(SUBTRACT_NO_PERIOD,0));
        staticPeriods.add(0L);

        dynamicRangeList.add(null);
    }

    public void intersectIntervals(CharSequence seq, int lo, int lim, int position, Interval tempInterval) throws SqlException {
        IntervalUtils.parseIntervalEx(seq, lo, lim, position, tempInterval);
        saveInterval(tempInterval, IntervalOperation.INTERSECT);
    }

    public int size() {
        return dynamicRangeList.size();
    }

    public void subtractIntervals(CharSequence seq, int lo, int lim, int position, Interval tempInterval) throws SqlException {
        IntervalUtils.parseIntervalEx(seq, lo, lim, position, tempInterval);
        saveInterval(tempInterval, IntervalOperation.SUBTRACT);
    }

    private void saveInterval(Interval tempInterval, short operation) {
        staticPeriods.add(tempInterval.lo);
        staticPeriods.add(tempInterval.hi);
        // Should be ASCII to safely cast to / from short
        assert (int)tempInterval.periodType < 0xFF;
        staticPeriods.add(Numbers.encodeLowHighInts(
                Numbers.encodeLowHighShorts(operation, (short) tempInterval.periodType),
                tempInterval.count));
        staticPeriods.add(Numbers.encodeLowHighInts(tempInterval.period, tempInterval.count));

        dynamicRangeList.add(null);
    }

    public void saveStaticIntervals(LongList staticIntervals) {
        if (staticIntervals != null) {
            staticPeriods.add(staticIntervals);
        }
    }

    private class RuntimePeriodCursor implements RuntimePeriod {
        private int index;

        public RuntimePeriod of(int i) {
            this.index = i;
            return this;
        }

        @Override
        public int getCount() {
            long periodcount = staticPeriods.getQuick(index * STATIC_LONGS_PER_DYNAMIC_INTERVAL + PERIOD_COUNT_INDEX);
            return Numbers.decodeHighInt(periodcount);
        }

        @Override
        public Function getDynamicHi() {
            long hi = getStaticHi();
            if (hi == EMPTY) {
                return dynamicRangeList.getQuick(index);
            }
            return null;
        }

        @Override
        public int getDynamicIncrement() {
            long operationpdadj = staticPeriods.getQuick(index * STATIC_LONGS_PER_DYNAMIC_INTERVAL + OPERATION_PERIOD_TYPE_ADJUSTMENT_INDEX);
            return Numbers.decodeHighInt(operationpdadj);
        }

        @Override
        public Function getDynamicLo() {
            long lo = getStaticLo();
            if (lo == EMPTY) {
                return dynamicRangeList.getQuick(index);
            }
            return null;
        }

        @Override
        public short getOperation() {
            long operationpdadj = staticPeriods.getQuick(index * STATIC_LONGS_PER_DYNAMIC_INTERVAL + OPERATION_PERIOD_TYPE_ADJUSTMENT_INDEX);
            return Numbers.decodeLowShort(Numbers.decodeLowInt(operationpdadj));
        }

        @Override
        public int getPeriod() {
            long periodcount = staticPeriods.getQuick(index * STATIC_LONGS_PER_DYNAMIC_INTERVAL + PERIOD_COUNT_INDEX);
            return Numbers.decodeLowInt(periodcount);
        }

        @Override
        public char getPeriodType() {
            long operationpdadj = staticPeriods.getQuick(index * STATIC_LONGS_PER_DYNAMIC_INTERVAL + OPERATION_PERIOD_TYPE_ADJUSTMENT_INDEX);
            return (char) Numbers.decodeHighShort(Numbers.decodeLowInt(operationpdadj));
        }

        @Override
        public long getStaticHi() {
            return staticPeriods.getQuick(index * STATIC_LONGS_PER_DYNAMIC_INTERVAL + HI_INDEX);
        }

        @Override
        public long getStaticLo() {
            return staticPeriods.getQuick(index * STATIC_LONGS_PER_DYNAMIC_INTERVAL + LO_INDEX);
        }
    }
}

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
import io.questdb.std.ObjList;

/**
 * Collects interval during query parsing
 * and records them as 2 types:
 * - static list of intervals as 2 long points [lo, hi] in staticIntervals list
 * - dynamic list of functions.
 * <p>
 * When first interval involving function is added all data starts to be encoded in 4 longs in staticPeriods
 * 0: lo (long)
 * 1: hi (long)
 * 2: operation (short), period type (short), adjustment (short), dynamicIndicator (short)
 * 3: period (int), count (int)
 * <p>
 * and the index when it happens stored in pureStaticCount
 */
public class RuntimeIntervalModelBuilder implements Mutable {
    // All data needed to re-evaluate intervals
    // is stored in 2 lists - ListLong and List of functions
    // ListLongs has STATIC_LONGS_PER_DYNAMIC_INTERVAL entries per 1 dynamic interval
    // and pairs of static intervals in the end
    private final LongList staticIntervals = new LongList();
    private final ObjList<Function> dynamicRangeList = new ObjList<>();
    private boolean intervalApplied = false;

    public RuntimeIntrinsicIntervalModel build() {
        return new RuntimeIntervalModel(new LongList(staticIntervals), new ObjList<>(dynamicRangeList));
    }

    @Override
    public void clear() {
        staticIntervals.clear();
        dynamicRangeList.clear();
        intervalApplied = false;
    }

    public boolean hasIntervalFilters() {
        return intervalApplied;
    }

    public void intersect(long lo, Function hi, short adjustment) {
        if (isEmptySet()) return;

        IntervalUtils.addHiLoInterval(lo, 0, adjustment, IntervalDynamicIndicator.IS_HI_DYNAMIC, IntervalOperation.INTERSECT, staticIntervals);
        dynamicRangeList.add(hi);
        intervalApplied = true;
    }

    public void intersect(Function lo, long hi, short adjustment) {
        if (isEmptySet()) return;

        IntervalUtils.addHiLoInterval(0, hi, adjustment, IntervalDynamicIndicator.IS_LO_DYNAMIC, IntervalOperation.INTERSECT, staticIntervals);
        dynamicRangeList.add(lo);
        intervalApplied = true;
    }

    public void intersect(long lo, long hi) {
        if (isEmptySet()) return;
        if (dynamicRangeList.size() == 0) {
            staticIntervals.add(lo);
            staticIntervals.add(hi);
            if (intervalApplied) {
                IntervalUtils.intersectInplace(staticIntervals, staticIntervals.size() - 2);
            }
        } else {
            IntervalUtils.addHiLoInterval(lo, hi, IntervalOperation.INTERSECT, staticIntervals);
            dynamicRangeList.add(null);
        }
        intervalApplied = true;
    }

    public void intersectEmpty() {
        clear();
        intervalApplied = true;
    }

    public void intersectEquals(Function function) {
        if (isEmptySet()) return;

        IntervalUtils.addHiLoInterval(0, 0, (short) 0, IntervalDynamicIndicator.IS_LO_HI_DYNAMIC, IntervalOperation.INTERSECT, staticIntervals);
        dynamicRangeList.add(function);
        intervalApplied = true;
    }

    public void intersectIntervals(CharSequence seq, int lo, int lim, int position) throws SqlException {
        if (isEmptySet()) return;
        int size = staticIntervals.size();
        IntervalUtils.parseIntervalEx(seq, lo, lim, position, staticIntervals, IntervalOperation.INTERSECT);
        if (dynamicRangeList.size() == 0) {
            IntervalUtils.applyLastEncodedIntervalEx(staticIntervals);
            if (intervalApplied) {
                IntervalUtils.intersectInplace(staticIntervals, size);
            }
        } else {
            // else - nothing to do, interval already encoded in staticPeriods as 4 longs
            dynamicRangeList.add(null);
        }
        intervalApplied = true;
    }

    public boolean isEmptySet() {
        return intervalApplied && staticIntervals.size() == 0;
    }

    public void subtractInterval(long lo, long hi) {
        if (isEmptySet()) return;
        if (dynamicRangeList.size() == 0) {
            int size = staticIntervals.size();
            staticIntervals.add(lo);
            staticIntervals.add(hi);
            IntervalUtils.invert(staticIntervals, size);
            if (intervalApplied) {
                IntervalUtils.intersectInplace(staticIntervals, size);
            }
        } else {
            IntervalUtils.addHiLoInterval(lo, hi, IntervalOperation.SUBTRACT, staticIntervals);
            dynamicRangeList.add(null);
        }
        intervalApplied = true;
    }

    public void subtractIntervals(CharSequence seq, int lo, int lim, int position) throws SqlException {
        if (isEmptySet()) return;
        int size = staticIntervals.size();
        IntervalUtils.parseIntervalEx(seq, lo, lim, position, staticIntervals, IntervalOperation.SUBTRACT);
        if (dynamicRangeList.size() == 0) {
            IntervalUtils.applyLastEncodedIntervalEx(staticIntervals);
            IntervalUtils.invert(staticIntervals, size);
            if (intervalApplied) {
                IntervalUtils.intersectInplace(staticIntervals, size);
            }
        } else {
            // else - nothing to do, interval already encoded in staticPeriods as 4 longs
            dynamicRangeList.add(null);
        }
        intervalApplied = true;
    }
}

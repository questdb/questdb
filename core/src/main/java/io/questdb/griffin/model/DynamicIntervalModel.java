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
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.LongList;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.Timestamps;

import java.io.Closeable;
import java.io.IOException;

public class DynamicIntervalModel implements IntervalModel, Mutable {
    private final Interval tempInterval = new Interval();
    private final StaticIntervalsModel staticIntervalsModel = new StaticIntervalsModel();
    private CompactDynamicIntervalModel dynamicModel = new CompactDynamicIntervalModel();

    @Override
    public void clear() {
        staticIntervalsModel.clear();
        if (dynamicModel == null) {
            // This will then be passed to a factory, create new copy
            dynamicModel = new CompactDynamicIntervalModel();
        } else {
            dynamicModel.clear();
        }
    }

    @Override
    public boolean hasIntervals() {
        return isDynamic() || staticIntervalsModel.intervals != null;
    }

    @Override
    public void intersectEmpty() {
        dynamicModel.clear();
        staticIntervalsModel.intersectEmpty();
    }

    @Override
    public void intersectIntervals(long lo, long hi) {
        if (isDynamic()) {
            dynamicModel.intersect(lo, hi);
        } else {
            staticIntervalsModel.intersectIntervals(lo, hi);
        }
    }

    @Override
    public void intersectIntervals(CharSequence seq, int lo, int lim, int position) throws SqlException {
        if (isDynamic()) {
            dynamicModel.intersectIntervals(seq, lo, lim, position, tempInterval);
        } else {
            staticIntervalsModel.intersectIntervals(seq, lo, lim, position);
        }
    }

    @Override
    public void subtractIntervals(long lo, long hi) {
        if (isDynamic()) {
            dynamicModel.subtractInterval(lo, hi);
        } else {
            staticIntervalsModel.subtractIntervals(lo, hi);
        }
    }

    @Override
    public void subtractIntervals(CharSequence seq, int lo, int lim, int position) throws SqlException {
        if (isDynamic()) {
            dynamicModel.subtractIntervals(seq, lo, lim, position, tempInterval);
        } else {
            staticIntervalsModel.subtractIntervals(seq, lo, lim, position);
        }
    }

    public boolean isEmptySet() {
        return !isDynamic() && staticIntervalsModel.isEmptySet();
    }

    public RuntimeIntrinsicIntervalModel getIntervalModel() {
        if (!isDynamic()) {
            LongList intervalCopy = staticIntervalsModel.intervals != null ? new LongList(staticIntervalsModel.intervals) : null;
            return new StaticRuntimeIntrinsicIntervalModel(intervalCopy);
        } else {
            dynamicModel.saveStaticIntervals(staticIntervalsModel.intervals);
            RuntimeIntrinsicIntervalModel result = new DynamicRuntimeIntrinsicIntervalModel(dynamicModel, staticIntervalsModel);
            dynamicModel = null;
            return result;
        }
    }

    public void intersectIntervals(long low, Function function, int funcAdjust) {
        // Intersect nothing with anything is still nothing.
        if (!isDynamic() && staticIntervalsModel.isEmptySet()) return;
        dynamicModel.intersect(low, function, funcAdjust);
    }

    public void intersectIntervals(Function function, long hi, int funcAdjust) {
        // Intersect nothing with anything is still nothing.
        if (!isDynamic() && staticIntervalsModel.isEmptySet()) return;
        dynamicModel.intersect(function, hi, funcAdjust);
    }

    public void intersectEquals(Function lo) {
        // Intersect nothing with anything is still nothing.
        if (!isDynamic() && staticIntervalsModel.isEmptySet()) return;
        dynamicModel.intersectEquals(lo);
    }

    private boolean isDynamic() {
        return dynamicModel.size() > 0;
    }

    private static class DynamicRuntimeIntrinsicIntervalModel implements RuntimeIntrinsicIntervalModel {
        private static final LongList EMPTY_INTERVALS = new LongList();
        private final CompactDynamicIntervalModel intervals;
        private StaticIntervalsModel reusableTempModel;
        private final LongList result = new LongList();

        private DynamicRuntimeIntrinsicIntervalModel(CompactDynamicIntervalModel intervals, StaticIntervalsModel reusableTempModel) {
            this.intervals = intervals;
            this.reusableTempModel = reusableTempModel;
        }

        @Override
        public LongList calculateIntervals(SqlExecutionContext sqlContext) {
            result.clear();
            intervals.extractStaticIntervalsTo(result);
            try (Closeable ignored = reusableTempModel.overrideWith(result)) {
                for (int i = 0; i < this.intervals.size(); i++) {
                    RuntimePeriod toApply = intervals.getDynamicPeriod(i);

                    long lo, hi = 0;
                    if (toApply.getDynamicLo() != null) {
                        toApply.getDynamicLo().init(null, sqlContext);
                        lo = toApply.getDynamicLo().getTimestamp(null);
                        // Numbers.LONG_NaN == Long.MIN_VALUE
                        // there is no way to understand if the function evaluated to min value or
                        // NULL. Assume it's null and it's period starting with undefined boundary.
                        if (lo == Numbers.LONG_NaN) {
                            return EMPTY_INTERVALS;
                        }
                        lo += toApply.getDynamicIncrement();
                    } else {
                        lo = toApply.getStaticLo();
                    }

                    if (toApply.getOperation() != IntervalOperation.INTERSECT_EQUALS) {
                        if (toApply.getDynamicHi() != null) {
                            toApply.getDynamicHi().init(null, sqlContext);
                            hi = toApply.getDynamicHi().getTimestamp(null);
                            if (hi == Numbers.LONG_NaN) {
                                return EMPTY_INTERVALS;
                            }
                            hi += toApply.getDynamicIncrement();
                        } else {
                            hi = toApply.getStaticHi();
                        }
                    }

                    switch (toApply.getOperation()) {
                        case IntervalOperation.SUBTRACT:
                            reusableTempModel.applySubtract(
                                    lo,
                                    hi,
                                    toApply.getPeriod(),
                                    toApply.getPeriodType(),
                                    toApply.getCount());
                            break;

                        case IntervalOperation.INTERSECT:
                            reusableTempModel.applyIntersect(
                                    lo,
                                    hi,
                                    toApply.getPeriod(),
                                    toApply.getPeriodType(),
                                    toApply.getCount());
                            break;

                        case IntervalOperation.INTERSECT_EQUALS:
                            // Single value stored in lo
                            reusableTempModel.applyIntersect(
                                    lo,
                                    lo,
                                    toApply.getPeriod(),
                                    toApply.getPeriodType(),
                                    toApply.getCount());
                            break;

                        default:
                    }
                }

                // reusableTempModel.intervals round robin between 3 lists
                // if the final result is not in the list instance we want
                // copy it to it before restoring reusableTempModel
                if (reusableTempModel.intervals != result) {
                    result.clear();
                    result.add(reusableTempModel.intervals);
                }
                return result;
            } catch (IOException e) {
                // Should never be the case.
                assert false;
                return null;
            }
        }

        @Override
        public boolean isFocused(Timestamps.TimestampFloorMethod floorDd) {
            return false;
        }
    }
}

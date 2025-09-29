/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.std.ObjectFactory;

/**
 * Part of implicit rule optimizer.
 * Extracts important timestamp and indexed symbol parts from the query filter.
 */
public class IntrinsicModel implements Mutable {
    public static final ObjectFactory<IntrinsicModel> FACTORY = IntrinsicModel::new;
    public static final int FALSE = 2;
    public static final int TRUE = 1;
    public static final int UNDEFINED = 0;
    private static final LongList INFINITE_INTERVAL;
    public final ObjList<Function> keyExcludedValueFuncs = new ObjList<>();
    public final ObjList<Function> keyValueFuncs = new ObjList<>();
    private final RuntimeIntervalModelBuilder runtimeIntervalBuilder = new RuntimeIntervalModelBuilder();
    public ExpressionNode filter;
    public int intrinsicValue = UNDEFINED;
    // Indexed symbol column used as the initial "efficient" filter for the query.
    public CharSequence keyColumn;
    public ObjList<ExpressionNode> keyExcludedNodes = new ObjList<>();
    public QueryModel keySubQuery;

    public RuntimeIntrinsicIntervalModel buildIntervalModel() {
        return runtimeIntervalBuilder.build();
    }

    public void of(int timestampType, int partitionBy) {
        this.runtimeIntervalBuilder.of(timestampType, partitionBy);
    }

    @Override
    public void clear() {
        keyColumn = null;
        keyValueFuncs.clear();
        keyExcludedValueFuncs.clear();
        runtimeIntervalBuilder.clear();
        filter = null;
        intrinsicValue = UNDEFINED;
        keySubQuery = null;
        keyExcludedNodes.clear();
    }

    public void clearBetweenTempParsing() {
        runtimeIntervalBuilder.clearBetweenParsing();
    }

    public boolean hasIntervalFilters() {
        return runtimeIntervalBuilder.hasIntervalFilters();
    }

    public void intersectEmpty() {
        runtimeIntervalBuilder.intersectEmpty();
        intrinsicValue = FALSE;
    }

    public void intersectIntervals(long lo, long hi) {
        runtimeIntervalBuilder.intersect(lo, hi);
    }

    public void intersectIntervals(CharSequence seq, int lo, int lim, int position) throws SqlException {
        runtimeIntervalBuilder.intersectIntervals(seq, lo, lim, position);
        if (runtimeIntervalBuilder.isEmptySet()) {
            intrinsicValue = FALSE;
        }
    }

    public void intersectIntervals(long lo, Function function, short funcAdjust) {
        runtimeIntervalBuilder.intersect(lo, function, funcAdjust);
        if (runtimeIntervalBuilder.isEmptySet()) {
            intrinsicValue = FALSE;
        }
    }

    public void intersectIntervals(Function function, long hi, short funcAdjust) {
        runtimeIntervalBuilder.intersect(function, hi, funcAdjust);
        if (runtimeIntervalBuilder.isEmptySet()) {
            intrinsicValue = FALSE;
        }
    }

    public void intersectRuntimeIntervals(Function intervalFunction) {
        runtimeIntervalBuilder.intersectRuntimeIntervals(intervalFunction);
        if (runtimeIntervalBuilder.isEmptySet()) {
            intrinsicValue = FALSE;
        }
    }

    public void intersectRuntimeTimestamp(Function function) {
        runtimeIntervalBuilder.intersectRuntimeTimestamp(function);
        if (runtimeIntervalBuilder.isEmptySet()) {
            intrinsicValue = FALSE;
        }
    }

    public void intersectTimestamp(CharSequence seq, int lo, int lim, int position) throws SqlException {
        runtimeIntervalBuilder.intersectTimestamp(seq, lo, lim, position);
        if (runtimeIntervalBuilder.isEmptySet()) {
            intrinsicValue = FALSE;
        }
    }

    public void setBetweenBoundary(long timestamp) {
        runtimeIntervalBuilder.setBetweenBoundary(timestamp);
    }

    public void setBetweenBoundary(Function timestamp) {
        runtimeIntervalBuilder.setBetweenBoundary(timestamp);
    }

    public void setBetweenNegated(boolean isNegated) {
        runtimeIntervalBuilder.setBetweenNegated(isNegated);
    }

    public void subtractEquals(Function function) {
        runtimeIntervalBuilder.subtractEquals(function);
        if (runtimeIntervalBuilder.isEmptySet()) {
            intrinsicValue = FALSE;
        }
    }

    public void subtractIntervals(long lo, long hi) {
        runtimeIntervalBuilder.subtractInterval(lo, hi);
        if (runtimeIntervalBuilder.isEmptySet()) {
            intrinsicValue = FALSE;
        }
    }

    public void subtractIntervals(CharSequence seq, int lo, int lim, int position) throws SqlException {
        runtimeIntervalBuilder.subtractIntervals(seq, lo, lim, position);
        if (runtimeIntervalBuilder.isEmptySet()) {
            intrinsicValue = FALSE;
        }
    }

    public void subtractRuntimeIntervals(Function intervalFunction) {
        runtimeIntervalBuilder.subtractRuntimeIntervals(intervalFunction);
        if (runtimeIntervalBuilder.isEmptySet()) {
            intrinsicValue = FALSE;
        }
    }

    @Override
    public String toString() {
        return "IntrinsicModel{" +
                "keyValueFuncs=" + keyValueFuncs +
                ", keyColumn='" + keyColumn + '\'' +
                ", filter=" + filter +
                '}';
    }

    public void unionIntervals(long lo, long hi) {
        runtimeIntervalBuilder.union(lo, hi);
    }

    static {
        INFINITE_INTERVAL = new LongList();
        INFINITE_INTERVAL.add(Long.MIN_VALUE, Long.MAX_VALUE);
    }
}

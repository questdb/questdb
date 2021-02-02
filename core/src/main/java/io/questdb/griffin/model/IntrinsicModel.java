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
import io.questdb.std.*;

import static io.questdb.griffin.SqlKeywords.isNullKeyword;

public class IntrinsicModel implements Mutable {
    public static final ObjectFactory<IntrinsicModel> FACTORY = IntrinsicModel::new;
    public static final int TRUE = 1;
    public static final int FALSE = 2;
    public static final int UNDEFINED = 0;
    private static final LongList INFINITE_INTERVAL;
    public final CharSequenceHashSet keyValues = new CharSequenceHashSet();
    public final CharSequenceHashSet keyExcludedValues = new CharSequenceHashSet();
    public final IntList keyValuePositions = new IntList();
    public final IntList keyExcludedValuePositions = new IntList();
    public CharSequence keyColumn;
    public ExpressionNode filter;
    public int intrinsicValue = UNDEFINED;
    public QueryModel keySubQuery;
    private final RuntimeIntervalModelBuilder runtimeIntervalBuilder = new RuntimeIntervalModelBuilder();

    @Override
    public void clear() {
        keyColumn = null;
        keyValues.clear();
        keyExcludedValues.clear();
        keyValuePositions.clear();
        keyExcludedValuePositions.clear();
        runtimeIntervalBuilder.clear();
        filter = null;
        intrinsicValue = UNDEFINED;
        keySubQuery = null;
    }

    public void excludeValue(ExpressionNode val) {

        final int index;
        if (isNullKeyword(val.token)) {
            index = keyValues.removeNull();
            if (index > -1) {
                keyValuePositions.removeIndex(index);
            }
        } else {
            int keyIndex = Chars.isQuoted(val.token) ? keyValues.keyIndex(val.token, 1, val.token.length() - 1) : keyValues.keyIndex(val.token);
            if (keyIndex < 0) {
                index = keyValues.getListIndexAt(keyIndex);
                keyValues.removeAt(keyIndex);
            } else {
                index = -1;
            }
        }

        if (index > -1) {
            keyValuePositions.removeIndex(index);
        }

        if (keyValues.size() == 0) {
            intrinsicValue = FALSE;
        }
    }

    public RuntimeIntrinsicIntervalModel buildIntervalModel() {
        return runtimeIntervalBuilder.build();
    }

    public boolean hasIntervalFilters() {
        return runtimeIntervalBuilder.hasIntervalFilters();
    }

    public void intersectEmpty() {
        runtimeIntervalBuilder.intersectEmpty();
        intrinsicValue = FALSE;
    }

    public void intersectEquals(Function function) {
        runtimeIntervalBuilder.intersectEquals(function);
        if (runtimeIntervalBuilder.isEmptySet()) intrinsicValue = FALSE;
    }

    public void intersectIntervals(long lo, long hi) {
        runtimeIntervalBuilder.intersect(lo, hi);
    }

    public void intersectIntervals(CharSequence seq, int lo, int lim, int position) throws SqlException {
        runtimeIntervalBuilder.intersectIntervals(seq, lo, lim, position);
        if (runtimeIntervalBuilder.isEmptySet()) intrinsicValue = FALSE;
    }

    public void intersectIntervals(long lo, Function function, short funcAdjust) {
        runtimeIntervalBuilder.intersect(lo, function, funcAdjust);
        if (runtimeIntervalBuilder.isEmptySet()) intrinsicValue = FALSE;
    }

    public void intersectIntervals(Function function, long hi, short funcAdjust) {
        runtimeIntervalBuilder.intersect(function, hi, funcAdjust);
        if (runtimeIntervalBuilder.isEmptySet()) intrinsicValue = FALSE;
    }

    public void subtractIntervals(long lo, long hi) {
        runtimeIntervalBuilder.subtractInterval(lo, hi);
        if (runtimeIntervalBuilder.isEmptySet()) intrinsicValue = FALSE;
    }

    public void subtractIntervals(CharSequence seq, int lo, int lim, int position) throws SqlException {
        runtimeIntervalBuilder.subtractIntervals(seq, lo, lim, position);
        if (runtimeIntervalBuilder.isEmptySet()) intrinsicValue = FALSE;
    }

    @Override
    public String toString() {
        return "IntrinsicModel{" +
                "keyValues=" + keyValues +
                ", keyColumn='" + keyColumn + '\'' +
                ", filter=" + filter +
                '}';
    }

    static {
        INFINITE_INTERVAL = new LongList();
        INFINITE_INTERVAL.add(Long.MIN_VALUE);
        INFINITE_INTERVAL.add(Long.MAX_VALUE);
    }
}

/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.std.DoubleList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.Nullable;

class TextChartGroupByFunction extends VarcharFunction implements UnaryFunction, GroupByFunction {
    // ▁▂▃▄▅▆▇█ - 8 levels of vertical resolution
    static final char[] SPARK_CHARS = {'\u2581', '\u2582', '\u2583', '\u2584', '\u2585', '\u2586', '\u2587', '\u2588'};
    private static final int INITIAL_SINK_CAPACITY = 512;
    private static final int LIST_CLEAR_THRESHOLD = 64;
    private final Function arg;
    private final char[] chars;
    private final int functionPosition;
    private final int maxValues;
    private final @Nullable Function maxFunc;
    private final @Nullable Function minFunc;
    private final String name;
    private final ObjList<DirectUtf8Sink> sinks = new ObjList<>();
    private final ObjList<DoubleList> valueLists = new ObjList<>();
    private final @Nullable Function widthFunc;
    private int poolIndex = 0;
    private int valueIndex;

    TextChartGroupByFunction(
            String name,
            char[] chars,
            Function arg,
            @Nullable Function minFunc,
            @Nullable Function maxFunc,
            @Nullable Function widthFunc,
            int functionPosition,
            int maxBufferLength
    ) {
        this.name = name;
        this.chars = chars;
        this.arg = arg;
        this.minFunc = minFunc;
        this.maxFunc = maxFunc;
        this.widthFunc = widthFunc;
        this.functionPosition = functionPosition;
        // Each value produces one 3-byte UTF-8 character in the output
        this.maxValues = maxBufferLength / 3;
    }

    @Override
    public void clear() {
        if (sinks.size() > LIST_CLEAR_THRESHOLD) {
            for (int i = sinks.size() - 1; i > LIST_CLEAR_THRESHOLD - 1; i--) {
                Misc.free(sinks.getQuick(i));
                sinks.remove(i);
                valueLists.remove(i);
            }
        }
        for (int i = 0, n = sinks.size(); i < n; i++) {
            DirectUtf8Sink sink = sinks.getQuick(i);
            if (sink != null) {
                sink.clear();
            }
            DoubleList list = valueLists.getQuick(i);
            if (list != null) {
                list.clear();
            }
        }
        poolIndex = 0;
    }

    @Override
    public void close() {
        Misc.freeObjListAndClear(sinks);
        valueLists.clear();
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final DirectUtf8Sink sink;
        final DoubleList list;
        if (sinks.size() <= poolIndex) {
            sinks.extendAndSet(poolIndex, sink = new DirectUtf8Sink(INITIAL_SINK_CAPACITY));
            valueLists.extendAndSet(poolIndex, list = new DoubleList());
        } else {
            sink = sinks.getQuick(poolIndex);
            sink.clear();
            list = valueLists.getQuick(poolIndex);
            list.clear();
        }

        final double value = arg.getDouble(record);
        if (Double.isNaN(value)) {
            mapValue.putBool(valueIndex + 1, true);
        } else {
            list.add(value);
            mapValue.putBool(valueIndex + 1, false);
        }
        mapValue.putInt(valueIndex, poolIndex++);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final double value = arg.getDouble(record);
        if (!Double.isNaN(value)) {
            final DoubleList list = valueLists.getQuick(mapValue.getInt(valueIndex));
            if (list.size() >= maxValues) {
                throw CairoException.nonCritical().position(functionPosition)
                        .put(name).put("() result exceeds max size of ")
                        .put(maxValues * 3).put(" bytes");
            }
            list.add(value);
            mapValue.putBool(valueIndex + 1, false);
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public int getValueIndex() {
        return valueIndex;
    }

    @Override
    public @Nullable Utf8Sequence getVarcharA(Record rec) {
        if (rec.getBool(valueIndex + 1)) {
            return null;
        }

        final int idx = rec.getInt(valueIndex);
        final DirectUtf8Sink sink = sinks.getQuick(idx);
        if (sink.size() > 0) {
            return sink;
        }

        final DoubleList list = valueLists.getQuick(idx);
        final int size = list.size();
        if (size == 0) {
            return null;
        }

        double min = effectiveMin(list, size);
        double max = effectiveMax(list, size);

        final int width = effectiveWidth(size);
        if (width >= size) {
            renderValues(sink, list, 0, size, min, max);
        } else {
            renderSubsampled(sink, list, size, width, min, max);
        }

        return sink;
    }

    // Returns the same sink as getVarcharA because the render is cached (sink.size() > 0 check).
    // This follows the same pattern as StringAggVarcharGroupByFunction.
    @Override
    public @Nullable Utf8Sequence getVarcharB(Record rec) {
        return getVarcharA(rec);
    }

    @Override
    public void initValueIndex(int valueIndex) {
        this.valueIndex = valueIndex;
    }

    @Override
    public void initValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.INT);
        columnTypes.add(ColumnType.BOOLEAN);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isScalar() {
        return false;
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putBool(valueIndex + 1, true);
    }

    @Override
    public boolean supportsParallelism() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(name).val('(').val(arg);
        if (minFunc != null) {
            sink.val(',').val(minFunc);
            sink.val(',').val(maxFunc);
            if (widthFunc != null) {
                sink.val(',').val(widthFunc);
            }
        }
        sink.val(')');
    }

    @Override
    public void toTop() {
        UnaryFunction.super.toTop();
        poolIndex = 0;
    }

    private char charForValue(double value, double min, double range) {
        if (range == 0.0) {
            return chars[chars.length - 1];
        }
        double clamped = Math.max(min, Math.min(min + range, value));
        int idx = (int) ((clamped - min) / range * (chars.length - 1));
        return chars[Math.min(idx, chars.length - 1)];
    }

    // minFunc/maxFunc are constant functions; passing null as Record is the
    // standard convention for reading constant function values outside a cursor.
    private double effectiveMax(DoubleList list, int size) {
        if (maxFunc != null) {
            double v = maxFunc.getDouble(null);
            if (!Double.isNaN(v)) {
                return v;
            }
        }
        double max = list.getQuick(0);
        for (int i = 1; i < size; i++) {
            double v = list.getQuick(i);
            if (v > max) {
                max = v;
            }
        }
        return max;
    }

    private double effectiveMin(DoubleList list, int size) {
        if (minFunc != null) {
            double v = minFunc.getDouble(null);
            if (!Double.isNaN(v)) {
                return v;
            }
        }
        double min = list.getQuick(0);
        for (int i = 1; i < size; i++) {
            double v = list.getQuick(i);
            if (v < min) {
                min = v;
            }
        }
        return min;
    }

    private int effectiveWidth(int valueCount) {
        if (widthFunc != null) {
            int w = widthFunc.getInt(null);
            return Math.max(1, w);
        }
        return valueCount;
    }

    private void renderSubsampled(DirectUtf8Sink sink, DoubleList list, int size, int width, double min, double max) {
        double range = max - min;
        double bucketSize = (double) size / width;
        for (int i = 0; i < width; i++) {
            int from = (int) (i * bucketSize);
            int to = (int) ((i + 1) * bucketSize);
            if (to > size) {
                to = size;
            }
            // Guard against empty buckets from floating-point rounding
            if (from >= to) {
                from = to - 1;
            }
            double sum = 0;
            for (int j = from; j < to; j++) {
                sum += list.getQuick(j);
            }
            double avg = sum / (to - from);
            sink.put(charForValue(avg, min, range));
        }
    }

    private void renderValues(DirectUtf8Sink sink, DoubleList list, int from, int to, double min, double max) {
        double range = max - min;
        for (int i = from; i < to; i++) {
            sink.put(charForValue(list.getQuick(i), min, range));
        }
    }
}

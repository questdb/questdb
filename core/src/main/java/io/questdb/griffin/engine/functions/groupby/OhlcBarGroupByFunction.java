/*******************************************************************************
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
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import org.jetbrains.annotations.Nullable;

/**
 * Aggregate that computes OHLC (Open, High, Low, Close) from a numeric column
 * and renders a horizontal candlestick bar using Unicode characters.
 * <p>
 * Open is the first non-NULL value (smallest rowId), Close is the last
 * (largest rowId), High is the maximum, Low is the minimum.
 * <p>
 * Requires explicit min/max bounds for scaling. The bounds can come from
 * literal constants, bind variables, or lateral join columns. This ensures
 * deterministic rendering regardless of execution path (serial or parallel).
 * <p>
 * <b>MapValue layout</b> (8 slots):
 * <pre>
 *   +0  LONG    firstRowId   (open row id; LONG_NULL = no observations)
 *   +1  DOUBLE  firstValue   (open price)
 *   +2  LONG    lastRowId    (close row id)
 *   +3  DOUBLE  lastValue    (close price)
 *   +4  DOUBLE  minValue     (low price)
 *   +5  DOUBLE  maxValue     (high price)
 *   +6  DOUBLE  scaleMin     (user-supplied lower bound, from lateral join or constant)
 *   +7  DOUBLE  scaleMax     (user-supplied upper bound, from lateral join or constant)
 * </pre>
 * <b>Rendering characters</b> (all 3-byte BMP):
 * <pre>
 *   U+2800  Braille Blank               (padding beyond wick)
 *   U+2500  Box Drawings Light Horiz     (wick: low-to-body, body-to-high)
 *   U+2588  Full Block                   (bullish body: close >= open)
 *   U+2591  Light Shade                  (bearish body: close < open)
 *   U+2502  Box Drawings Light Vertical  (doji: close == open)
 * </pre>
 */
public class OhlcBarGroupByFunction extends VarcharFunction implements UnaryFunction, GroupByFunction {
    static final int DEFAULT_WIDTH = 40;
    private static final int LABEL_RESERVE = 120;
    private static final char BLANK = '\u2800';
    private static final char BODY_BEAR = '\u2591';
    private static final char BODY_BULL = '\u2588';
    private static final char DOJI = '\u2502';
    private static final char WICK = '\u2500';
    private final Function arg;
    private final int functionPosition;
    private final @Nullable Utf8StringSink labelSink;
    private final Function maxFunc;
    private final int maxBufferLength;
    private final int maxWidth;
    private final Function minFunc;
    private final int minPosition;
    private final String name;
    private final DirectUtf8String viewA = new DirectUtf8String();
    private final DirectUtf8String viewB = new DirectUtf8String();
    private final @Nullable Function widthFunc;
    private final int widthPosition;
    private GroupByAllocator allocator;
    private long cachedKeyA1;
    private long cachedKeyA2;
    private long cachedKeyB1;
    private long cachedKeyB2;
    private long cachedRenderLenA;
    private long cachedRenderLenB;
    private long cachedRenderPtrA;
    private long cachedRenderPtrB;
    private long lastRenderPtr;
    private int valueIndex;

    public OhlcBarGroupByFunction(
            String name,
            Function arg,
            Function minFunc,
            Function maxFunc,
            @Nullable Function widthFunc,
            boolean showLabels,
            int functionPosition,
            int minPosition,
            int widthPosition,
            int maxBufferLength
    ) {
        this.name = name;
        this.arg = arg;
        this.minFunc = minFunc;
        this.maxFunc = maxFunc;
        this.widthFunc = widthFunc;
        this.functionPosition = functionPosition;
        this.minPosition = minPosition;
        this.widthPosition = widthPosition;
        this.maxBufferLength = maxBufferLength;
        this.labelSink = showLabels ? new Utf8StringSink() : null;
        if (showLabels) {
            this.maxWidth = Math.max(1, (maxBufferLength - LABEL_RESERVE) / 3);
        } else {
            this.maxWidth = maxBufferLength / 3;
        }
    }

    @Override
    public void clear() {
        cachedKeyA1 = 0;
        cachedKeyA2 = 0;
        cachedKeyB1 = 0;
        cachedKeyB2 = 0;
        cachedRenderLenA = 0;
        cachedRenderLenB = 0;
        cachedRenderPtrA = 0;
        cachedRenderPtrB = 0;
        lastRenderPtr = 0;
    }

    @Override
    public void close() {
        Misc.free(arg);
        Misc.free(minFunc);
        Misc.free(maxFunc);
        Misc.free(widthFunc);
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        cachedKeyA1 = 0;
        cachedKeyA2 = 0;
        final double value = arg.getDouble(record);
        if (Double.isNaN(value)) {
            mapValue.putLong(valueIndex, Numbers.LONG_NULL);
            mapValue.putDouble(valueIndex + 1, Double.NaN);
            mapValue.putLong(valueIndex + 2, Numbers.LONG_NULL);
            mapValue.putDouble(valueIndex + 3, Double.NaN);
            mapValue.putDouble(valueIndex + 4, Double.NaN);
            mapValue.putDouble(valueIndex + 5, Double.NaN);
            mapValue.putDouble(valueIndex + 6, minFunc.getDouble(record));
            mapValue.putDouble(valueIndex + 7, maxFunc.getDouble(record));
            return;
        }
        mapValue.putLong(valueIndex, rowId);
        mapValue.putDouble(valueIndex + 1, value);
        mapValue.putLong(valueIndex + 2, rowId);
        mapValue.putDouble(valueIndex + 3, value);
        mapValue.putDouble(valueIndex + 4, value);
        mapValue.putDouble(valueIndex + 5, value);
        mapValue.putDouble(valueIndex + 6, minFunc.getDouble(record));
        mapValue.putDouble(valueIndex + 7, maxFunc.getDouble(record));
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final double value = arg.getDouble(record);
        if (Double.isNaN(value)) {
            return;
        }
        long firstRowId = mapValue.getLong(valueIndex);
        if (firstRowId == Numbers.LONG_NULL) {
            computeFirst(mapValue, record, rowId);
            return;
        }
        if (rowId < firstRowId) {
            mapValue.putLong(valueIndex, rowId);
            mapValue.putDouble(valueIndex + 1, value);
        }
        if (rowId > mapValue.getLong(valueIndex + 2)) {
            mapValue.putLong(valueIndex + 2, rowId);
            mapValue.putDouble(valueIndex + 3, value);
        }
        double currentMin = mapValue.getDouble(valueIndex + 4);
        if (value < currentMin) {
            mapValue.putDouble(valueIndex + 4, value);
        }
        double currentMax = mapValue.getDouble(valueIndex + 5);
        if (value > currentMax) {
            mapValue.putDouble(valueIndex + 5, value);
        }
        // Reconcile bounds: take widest range across all rows in group
        double newMin = minFunc.getDouble(record);
        double storedMin = mapValue.getDouble(valueIndex + 6);
        if (!Double.isNaN(newMin) && (Double.isNaN(storedMin) || newMin < storedMin)) {
            mapValue.putDouble(valueIndex + 6, newMin);
        }
        double newMax = maxFunc.getDouble(record);
        double storedMax = mapValue.getDouble(valueIndex + 7);
        if (!Double.isNaN(newMax) && (Double.isNaN(storedMax) || newMax > storedMax)) {
            mapValue.putDouble(valueIndex + 7, newMax);
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public int getSampleByFlags() {
        return SAMPLE_BY_FILL_NONE | SAMPLE_BY_FILL_NULL | SAMPLE_BY_FILL_PREVIOUS;
    }

    @Override
    public int getValueIndex() {
        return valueIndex;
    }

    @Override
    public @Nullable Utf8Sequence getVarcharA(Record rec) {
        long firstRowId = rec.getLong(valueIndex);
        if (firstRowId == Numbers.LONG_NULL) {
            return null;
        }
        long lastRowId = rec.getLong(valueIndex + 2);
        if (firstRowId == cachedKeyA1 && lastRowId == cachedKeyA2 && cachedRenderPtrA != 0) {
            return viewA.of(cachedRenderPtrA, cachedRenderPtrA + cachedRenderLenA);
        }
        final long outBytes = render(rec);
        if (outBytes < 0) {
            return null;
        }
        final long out = lastRenderPtr;
        cachedKeyA1 = firstRowId;
        cachedKeyA2 = lastRowId;
        cachedRenderPtrA = out;
        cachedRenderLenA = outBytes;
        return viewA.of(out, out + outBytes);
    }

    @Override
    public @Nullable Utf8Sequence getVarcharB(Record rec) {
        long firstRowId = rec.getLong(valueIndex);
        if (firstRowId == Numbers.LONG_NULL) {
            return null;
        }
        long lastRowId = rec.getLong(valueIndex + 2);
        if (firstRowId == cachedKeyB1 && lastRowId == cachedKeyB2 && cachedRenderPtrB != 0) {
            return viewB.of(cachedRenderPtrB, cachedRenderPtrB + cachedRenderLenB);
        }
        if (firstRowId == cachedKeyA1 && lastRowId == cachedKeyA2 && cachedRenderPtrA != 0) {
            cachedKeyB1 = firstRowId;
            cachedKeyB2 = lastRowId;
            cachedRenderPtrB = cachedRenderPtrA;
            cachedRenderLenB = cachedRenderLenA;
            return viewB.of(cachedRenderPtrA, cachedRenderPtrA + cachedRenderLenA);
        }
        final long outBytes = render(rec);
        if (outBytes < 0) {
            return null;
        }
        final long out = lastRenderPtr;
        cachedKeyB1 = firstRowId;
        cachedKeyB2 = lastRowId;
        cachedRenderPtrB = out;
        cachedRenderLenB = outBytes;
        return viewB.of(out, out + outBytes);
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        arg.init(symbolTableSource, executionContext);
        minFunc.init(symbolTableSource, executionContext);
        maxFunc.init(symbolTableSource, executionContext);
        if (widthFunc != null) {
            widthFunc.init(symbolTableSource, executionContext);
        }
    }

    @Override
    public void initValueIndex(int valueIndex) {
        this.valueIndex = valueIndex;
    }

    @Override
    public void initValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.LONG);   // +0 firstRowId
        columnTypes.add(ColumnType.DOUBLE); // +1 firstValue (open)
        columnTypes.add(ColumnType.LONG);   // +2 lastRowId
        columnTypes.add(ColumnType.DOUBLE); // +3 lastValue (close)
        columnTypes.add(ColumnType.DOUBLE); // +4 minValue (low)
        columnTypes.add(ColumnType.DOUBLE); // +5 maxValue (high)
        columnTypes.add(ColumnType.DOUBLE); // +6 scaleMin (user-supplied lower bound)
        columnTypes.add(ColumnType.DOUBLE); // +7 scaleMax (user-supplied upper bound)
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
    public boolean isThreadSafe() {
        return false;
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        long srcFirstRowId = srcValue.getLong(valueIndex);
        if (srcFirstRowId == Numbers.LONG_NULL) {
            return;
        }
        double srcMin = srcValue.getDouble(valueIndex + 4);
        double srcMax = srcValue.getDouble(valueIndex + 5);
        long destFirstRowId = destValue.getLong(valueIndex);
        if (destFirstRowId == Numbers.LONG_NULL) {
            destValue.putLong(valueIndex, srcFirstRowId);
            destValue.putDouble(valueIndex + 1, srcValue.getDouble(valueIndex + 1));
            destValue.putLong(valueIndex + 2, srcValue.getLong(valueIndex + 2));
            destValue.putDouble(valueIndex + 3, srcValue.getDouble(valueIndex + 3));
            destValue.putDouble(valueIndex + 4, srcMin);
            destValue.putDouble(valueIndex + 5, srcMax);
            destValue.putDouble(valueIndex + 6, srcValue.getDouble(valueIndex + 6));
            destValue.putDouble(valueIndex + 7, srcValue.getDouble(valueIndex + 7));
            return;
        }
        if (srcFirstRowId < destFirstRowId) {
            destValue.putLong(valueIndex, srcFirstRowId);
            destValue.putDouble(valueIndex + 1, srcValue.getDouble(valueIndex + 1));
        }
        long srcLastRowId = srcValue.getLong(valueIndex + 2);
        long destLastRowId = destValue.getLong(valueIndex + 2);
        if (srcLastRowId > destLastRowId) {
            destValue.putLong(valueIndex + 2, srcLastRowId);
            destValue.putDouble(valueIndex + 3, srcValue.getDouble(valueIndex + 3));
        }
        double destMin = destValue.getDouble(valueIndex + 4);
        if (srcMin < destMin) {
            destValue.putDouble(valueIndex + 4, srcMin);
        }
        double destMax = destValue.getDouble(valueIndex + 5);
        if (srcMax > destMax) {
            destValue.putDouble(valueIndex + 5, srcMax);
        }
        // Reconcile user-supplied bounds (slots +6/+7): widen to the
        // widest range seen across shards, same logic as computeNext.
        double srcScaleMin = srcValue.getDouble(valueIndex + 6);
        double destScaleMin = destValue.getDouble(valueIndex + 6);
        if (!Double.isNaN(srcScaleMin) && (Double.isNaN(destScaleMin) || srcScaleMin < destScaleMin)) {
            destValue.putDouble(valueIndex + 6, srcScaleMin);
        }
        double srcScaleMax = srcValue.getDouble(valueIndex + 7);
        double destScaleMax = destValue.getDouble(valueIndex + 7);
        if (!Double.isNaN(srcScaleMax) && (Double.isNaN(destScaleMax) || srcScaleMax > destScaleMax)) {
            destValue.putDouble(valueIndex + 7, srcScaleMax);
        }
    }

    @Override
    public void setAllocator(GroupByAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, Numbers.LONG_NULL);
        mapValue.putDouble(valueIndex + 1, Double.NaN);
        mapValue.putLong(valueIndex + 2, Numbers.LONG_NULL);
        mapValue.putDouble(valueIndex + 3, Double.NaN);
        mapValue.putDouble(valueIndex + 4, Double.NaN);
        mapValue.putDouble(valueIndex + 5, Double.NaN);
        mapValue.putDouble(valueIndex + 6, Double.NaN);
        mapValue.putDouble(valueIndex + 7, Double.NaN);
    }

    @Override
    public boolean supportsParallelism() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(name).val('(').val(arg);
        sink.val(',').val(minFunc);
        sink.val(',').val(maxFunc);
        if (widthFunc != null) {
            sink.val(',').val(widthFunc);
        }
        sink.val(')');
    }

    @Override
    public void toTop() {
        arg.toTop();
        minFunc.toTop();
        maxFunc.toTop();
        if (widthFunc != null) {
            widthFunc.toTop();
        }
    }

    private int effectiveWidth() {
        int w;
        if (widthFunc != null) {
            w = widthFunc.getInt(null);
            if (w < 1) {
                throw CairoException.nonCritical().position(widthPosition)
                        .put("width must be a positive integer");
            }
        } else {
            w = DEFAULT_WIDTH;
        }
        if (w > maxWidth) {
            throw CairoException.nonCritical().position(widthPosition)
                    .put("breached memory limit set for ").put(name)
                    .put(" [maxWidth=").put(maxWidth)
                    .put(", requestedWidth=").put(w).put(']');
        }
        return w;
    }

    private int mapPosition(double value, double low, double range, int width) {
        if (range == 0.0) {
            return width / 2;
        }
        double proportion = (value - low) / range;
        int pos = (int) Math.round(proportion * (width - 1));
        return Math.max(0, Math.min(width - 1, pos));
    }

    private void putBmpChar(long out, int pos, char c) {
        int packed = (0xE0 | ((c >> 12) & 0x0F))
                | ((0x80 | ((c >> 6) & 0x3F)) << 8)
                | ((0x80 | (c & 0x3F)) << 16);
        Unsafe.getUnsafe().putInt(out + pos * 3L, packed);
    }

    private long render(Record rec) {
        double open = rec.getDouble(valueIndex + 1);
        double close = rec.getDouble(valueIndex + 3);
        double low = rec.getDouble(valueIndex + 4);
        double high = rec.getDouble(valueIndex + 5);

        int width = effectiveWidth();

        double scaleMin = rec.getDouble(valueIndex + 6);
        double scaleMax = rec.getDouble(valueIndex + 7);

        if (Double.isNaN(scaleMin) || Double.isNaN(scaleMax)) {
            return -1;
        }
        if (scaleMin > scaleMax) {
            throw CairoException.nonCritical().position(minPosition)
                    .put(name).put("() min must not exceed max [min=")
                    .put(scaleMin).put(", max=").put(scaleMax).put(']');
        }

        double scaleRange = scaleMax - scaleMin;

        int lowPos = mapPosition(low, scaleMin, scaleRange, width);
        int highPos = mapPosition(high, scaleMin, scaleRange, width);
        int openPos = mapPosition(open, scaleMin, scaleRange, width);
        int closePos = mapPosition(close, scaleMin, scaleRange, width);

        int bodyStart = Math.min(openPos, closePos);
        int bodyEnd = Math.max(openPos, closePos);
        // True doji only when open actually equals close, not just when
        // they map to the same character position due to rounding.
        boolean isDoji = open == close;
        boolean isBullish = close >= open;

        long barBytes = (long) width * 3;
        long labelBytes = 0;
        if (labelSink != null) {
            labelSink.clear();
            labelSink.putAscii(" O:");
            Numbers.append(labelSink, open);
            labelSink.putAscii(" H:");
            Numbers.append(labelSink, high);
            labelSink.putAscii(" L:");
            Numbers.append(labelSink, low);
            labelSink.putAscii(" C:");
            Numbers.append(labelSink, close);
            labelBytes = labelSink.size();
        }

        long totalBytes = barBytes + labelBytes;
        if (totalBytes > maxBufferLength) {
            throw CairoException.nonCritical().position(functionPosition)
                    .put("breached memory limit set for ").put(name)
                    .put(" [maxBytes=").put(maxBufferLength)
                    .put(", actualBytes=").put(totalBytes).put(']');
        }

        // +1 byte for putBmpChar's 4-byte write safety on the last char
        long out = allocator.malloc(totalBytes + 1);

        for (int i = 0; i < width; i++) {
            char c;
            if (i < lowPos || i > highPos) {
                c = BLANK;
            } else if (isDoji && i == bodyStart) {
                c = DOJI;
            } else if (i >= bodyStart && i <= bodyEnd) {
                c = isBullish ? BODY_BULL : BODY_BEAR;
            } else {
                c = WICK;
            }
            putBmpChar(out, i, c);
        }

        if (labelSink != null && labelBytes > 0) {
            for (int i = 0; i < labelBytes; i++) {
                Unsafe.getUnsafe().putByte(out + barBytes + i, labelSink.byteAt(i));
            }
        }

        lastRenderPtr = out;
        return barBytes + labelBytes;
    }
}

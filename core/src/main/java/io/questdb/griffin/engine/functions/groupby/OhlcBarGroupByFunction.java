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
 * Each candle is scaled against its own High/Low range: the wick spans
 * the full width and the body position within the wick shows where
 * Open/Close sit relative to the High/Low range. For cross-group
 * comparable scaling, use the scalar variant
 * {@code ohlc_bar(open, high, low, close, min, max, width)} with
 * window functions to provide explicit bounds.
 * <p>
 * <b>MapValue layout</b> (6 slots):
 * <pre>
 *   +0  LONG    firstRowId   (open row id; LONG_NULL = no observations)
 *   +1  DOUBLE  firstValue   (open price)
 *   +2  LONG    lastRowId    (close row id)
 *   +3  DOUBLE  lastValue    (close price)
 *   +4  DOUBLE  minValue     (low price)
 *   +5  DOUBLE  maxValue     (high price)
 * </pre>
 *
 * <b>Rendering characters</b> (all 3-byte BMP):
 * <pre>
 *   U+2800  ⠀  Braille Blank               (padding beyond wick)
 *   U+2500  ─  Box Drawings Light Horiz     (wick: low-to-body, body-to-high)
 *   U+2588  █  Full Block                   (bullish body: close >= open)
 *   U+2591  ░  Light Shade                  (bearish body: close < open)
 *   U+2502  │  Box Drawings Light Vertical  (doji: close == open)
 * </pre>
 */
public class OhlcBarGroupByFunction extends VarcharFunction implements UnaryFunction, GroupByFunction {
    static final int DEFAULT_WIDTH = 40;
    // Reserve bytes for label text: " O:<num> H:<num> L:<num> C:<num>"
    // Each number can be up to ~24 chars. 4 labels + separators ~ 120 bytes max.
    private static final int LABEL_RESERVE = 120;
    private static final char BLANK = '\u2800';
    private static final char BODY_BEAR = '\u2591';
    private static final char BODY_BULL = '\u2588';
    private static final char DOJI = '\u2502';
    private static final char WICK = '\u2500';
    private final Function arg;
    private final int functionPosition;
    private final int maxWidth;
    private final String name;
    private final boolean showLabels;
    // Scratch buffer for label formatting. Allocated once, reused on every
    // render call. The internal byte[] grows on first use (~100 bytes for
    // 4 doubles) and stays sized - no per-row heap allocation after warmup.
    // Same pattern as BarFunctionFactory's Utf8StringSink sinkA/sinkB.
    private final @Nullable Utf8StringSink labelSink;
    private final DirectUtf8String viewA = new DirectUtf8String();
    private final DirectUtf8String viewB = new DirectUtf8String();
    private final @Nullable Function widthFunc;
    private final int widthPosition;
    private GroupByAllocator allocator;
    // Render caches - one per flyweight side, keyed by the
    // (firstRowId, lastRowId) pair that identifies a group's state.
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
            @Nullable Function widthFunc,
            boolean showLabels,
            int functionPosition,
            int widthPosition,
            int maxBufferLength
    ) {
        this.name = name;
        this.arg = arg;
        this.widthFunc = widthFunc;
        this.showLabels = showLabels;
        this.functionPosition = functionPosition;
        this.widthPosition = widthPosition;
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
            return;
        }
        mapValue.putLong(valueIndex, rowId);
        mapValue.putDouble(valueIndex + 1, value);
        mapValue.putLong(valueIndex + 2, rowId);
        mapValue.putDouble(valueIndex + 3, value);
        mapValue.putDouble(valueIndex + 4, value);
        mapValue.putDouble(valueIndex + 5, value);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final double value = arg.getDouble(record);
        if (Double.isNaN(value)) {
            return;
        }
        long firstRowId = mapValue.getLong(valueIndex);
        if (firstRowId == Numbers.LONG_NULL) {
            // All previous values were NaN; treat as first observation.
            computeFirst(mapValue, record, rowId);
            return;
        }
        // Update first (open) if this rowId is earlier
        if (rowId < firstRowId) {
            mapValue.putLong(valueIndex, rowId);
            mapValue.putDouble(valueIndex + 1, value);
        }
        // Update last (close) if this rowId is later
        if (rowId > mapValue.getLong(valueIndex + 2)) {
            mapValue.putLong(valueIndex + 2, rowId);
            mapValue.putDouble(valueIndex + 3, value);
        }
        // Update min (low)
        double currentMin = mapValue.getDouble(valueIndex + 4);
        if (value < currentMin) {
            mapValue.putDouble(valueIndex + 4, value);
        }
        // Update max (high)
        double currentMax = mapValue.getDouble(valueIndex + 5);
        if (value > currentMax) {
            mapValue.putDouble(valueIndex + 5, value);
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
        // If side A already rendered this group, share its buffer.
        if (firstRowId == cachedKeyA1 && lastRowId == cachedKeyA2 && cachedRenderPtrA != 0) {
            cachedKeyB1 = firstRowId;
            cachedKeyB2 = lastRowId;
            cachedRenderPtrB = cachedRenderPtrA;
            cachedRenderLenB = cachedRenderLenA;
            return viewB.of(cachedRenderPtrA, cachedRenderPtrA + cachedRenderLenA);
        }
        final long outBytes = render(rec);
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
            // Dest is empty, adopt src's state.
            destValue.putLong(valueIndex, srcFirstRowId);
            destValue.putDouble(valueIndex + 1, srcValue.getDouble(valueIndex + 1));
            destValue.putLong(valueIndex + 2, srcValue.getLong(valueIndex + 2));
            destValue.putDouble(valueIndex + 3, srcValue.getDouble(valueIndex + 3));
            destValue.putDouble(valueIndex + 4, srcMin);
            destValue.putDouble(valueIndex + 5, srcMax);
            return;
        }
        // Merge first (open): smallest rowId wins
        if (srcFirstRowId < destFirstRowId) {
            destValue.putLong(valueIndex, srcFirstRowId);
            destValue.putDouble(valueIndex + 1, srcValue.getDouble(valueIndex + 1));
        }
        // Merge last (close): largest rowId wins
        long srcLastRowId = srcValue.getLong(valueIndex + 2);
        long destLastRowId = destValue.getLong(valueIndex + 2);
        if (srcLastRowId > destLastRowId) {
            destValue.putLong(valueIndex + 2, srcLastRowId);
            destValue.putDouble(valueIndex + 3, srcValue.getDouble(valueIndex + 3));
        }
        // Merge min (low)
        double destMin = destValue.getDouble(valueIndex + 4);
        if (srcMin < destMin) {
            destValue.putDouble(valueIndex + 4, srcMin);
        }
        // Merge max (high)
        double destMax = destValue.getDouble(valueIndex + 5);
        if (srcMax > destMax) {
            destValue.putDouble(valueIndex + 5, srcMax);
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
    }

    @Override
    public boolean supportsParallelism() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(name).val('(').val(arg);
        if (widthFunc != null) {
            sink.val(',').val(widthFunc);
        }
        sink.val(')');
    }

    @Override
    public void toTop() {
        arg.toTop();
        if (widthFunc != null) {
            widthFunc.toTop();
        }
    }

    private int effectiveWidth() {
        if (widthFunc != null) {
            int w = widthFunc.getInt(null);
            if (w < 1) {
                throw CairoException.nonCritical().position(widthPosition)
                        .put("width must be a positive integer");
            }
            if (w > maxWidth) {
                throw CairoException.nonCritical().position(widthPosition)
                        .put("breached memory limit set for ").put(name)
                        .put(" [maxWidth=").put(maxWidth)
                        .put(", requestedWidth=").put(w).put(']');
            }
            return w;
        }
        return DEFAULT_WIDTH;
    }

    private int mapPosition(double value, double low, double range, int width) {
        if (range == 0.0) {
            return width / 2;
        }
        double proportion = (value - low) / range;
        int pos = (int) (proportion * (width - 1));
        return Math.max(0, Math.min(width - 1, pos));
    }

    // All chars used are in the BMP and encode as three UTF-8 bytes:
    // 1110xxxx 10yyyyyy 10zzzzzz. Pack into a single little-endian int.
    private void putChar(long out, int pos, char c) {
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

        // Scale each candle against its own low/high range.
        // For cross-group comparable scaling, use the scalar variant
        // ohlc_bar(o,h,l,c,min,max,width) with window functions.
        double scaleMin = low;
        double scaleMax = high;
        double scaleRange = scaleMax - scaleMin;

        int lowPos = mapPosition(low, scaleMin, scaleRange, width);
        int highPos = mapPosition(high, scaleMin, scaleRange, width);
        int openPos = mapPosition(open, scaleMin, scaleRange, width);
        int closePos = mapPosition(close, scaleMin, scaleRange, width);

        int bodyStart = Math.min(openPos, closePos);
        int bodyEnd = Math.max(openPos, closePos);
        boolean isDoji = openPos == closePos;
        boolean isBullish = close >= open;

        // Calculate output size
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

        // +1 byte for putChar's 4-byte write safety on the last BMP char
        long out = allocator.malloc(barBytes + labelBytes + 1);

        // Render: body only, blank everywhere else. Per-group scaling
        // means low=0 and high=width-1, so wicks would span the full
        // width and carry no information. The body position within
        // blank space shows where O/C sit relative to the H/L range.
        for (int i = 0; i < width; i++) {
            char c;
            if (isDoji && i == bodyStart) {
                c = DOJI;
            } else if (i >= bodyStart && i <= bodyEnd) {
                c = isBullish ? BODY_BULL : BODY_BEAR;
            } else {
                c = BLANK;
            }
            putChar(out, i, c);
        }

        // Append label bytes
        if (labelSink != null && labelBytes > 0) {
            for (int i = 0; i < labelBytes; i++) {
                Unsafe.getUnsafe().putByte(out + barBytes + i, labelSink.byteAt(i));
            }
        }

        lastRenderPtr = out;
        return barBytes + labelBytes;
    }
}

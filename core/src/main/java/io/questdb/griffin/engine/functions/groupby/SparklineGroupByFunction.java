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
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.Nullable;

/**
 * Aggregate that renders a per-group numeric sequence as a short text chart
 * (sparkline). Values are captured with their rowId so rendering is
 * deterministic regardless of scan order, and the aggregate can run under the
 * parallel GROUP BY path.
 * <p>
 * <b>Parallelism strategy.</b> Same pattern as {@link TwapGroupByFunction}.
 * Each worker accumulates its observations into a native buffer via its own
 * {@link GroupByAllocator}. Within a single worker page frames arrive in
 * rowId order, so per-worker buffers are sorted runs. During the merge phase
 * two sorted runs are combined with a merge-sort merge step into a fresh
 * buffer in the destination's allocator - the owner's allocator on the
 * non-sharded merge path, or a per-worker allocator on the sharded merge
 * path. Either way, the destination buffer lives in an allocator that
 * outlives the merge until cursor close.
 * <p>
 * <b>MapValue layout</b> (3 slots):
 * <pre>
 *   +0  LONG   pair buffer pointer (0 = no observations)
 *   +1  LONG   observation count
 *   +2  LONG   buffer capacity in entries
 * </pre>
 * <b>Pair entry layout</b> (16 bytes each):
 * <pre>
 *   [0..7]   rowId (long)
 *   [8..15]  value (double)
 * </pre>
 */
public class SparklineGroupByFunction extends VarcharFunction implements UnaryFunction, GroupByFunction {
    static final char[] SPARK_CHARS = {'▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'};
    private static final long ENTRY_SIZE = 16;
    private static final long INITIAL_CAPACITY = 16;
    private final Function arg;
    private final char[] chars;
    private final int functionPosition;
    private final @Nullable Function maxFunc;
    private final int maxValues;
    private final @Nullable Function minFunc;
    private final String name;
    // A and B need independent flyweights because sort's comparator
    // fetches both sides from this same Function and expects neither to
    // clobber the other.
    private final DirectUtf8String viewA = new DirectUtf8String();
    private final DirectUtf8String viewB = new DirectUtf8String();
    private final @Nullable Function widthFunc;
    private final int widthPosition;
    private GroupByAllocator allocator;
    private long cachedPairPtrA;
    private long cachedPairPtrB;
    private long cachedRenderLenA;
    private long cachedRenderLenB;
    private long cachedRenderPtrA;
    private long cachedRenderPtrB;
    private long lastRenderPtr;
    private int valueIndex;

    public SparklineGroupByFunction(
            String name,
            char[] chars,
            Function arg,
            @Nullable Function minFunc,
            @Nullable Function maxFunc,
            @Nullable Function widthFunc,
            int functionPosition,
            int widthPosition,
            int maxBufferLength
    ) {
        this.name = name;
        this.chars = chars;
        this.arg = arg;
        this.minFunc = minFunc;
        this.maxFunc = maxFunc;
        this.widthFunc = widthFunc;
        this.functionPosition = functionPosition;
        this.widthPosition = widthPosition;
        // Each rendered char occupies 3 bytes of UTF-8.
        this.maxValues = maxBufferLength / 3;
    }

    @Override
    public void clear() {
        // Render caches hold native pointers into the GroupByAllocator. When
        // the enclosing factory is reused across cursor runs the allocator
        // is reset and the same addresses may be handed out again - stale
        // cache entries would then point at freed memory. computeFirst also
        // clears these on every new group, but the owner's instance on the
        // sharded merge path sees groups only via merge() and would miss
        // that reset.
        cachedPairPtrA = 0;
        cachedPairPtrB = 0;
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
        // A fresh group is about to overwrite slot 0 - invalidate render
        // caches so viewA/viewB don't return a stale pointer for a
        // recycled group.
        cachedPairPtrA = 0;
        cachedPairPtrB = 0;
        final double value = arg.getDouble(record);
        if (Double.isNaN(value)) {
            mapValue.putLong(valueIndex, 0);
            mapValue.putLong(valueIndex + 1, 0);
            mapValue.putLong(valueIndex + 2, 0);
            return;
        }
        long ptr = allocator.malloc(INITIAL_CAPACITY * ENTRY_SIZE);
        Unsafe.putLong(ptr, rowId);
        Unsafe.putDouble(ptr + 8, value);
        mapValue.putLong(valueIndex, ptr);
        mapValue.putLong(valueIndex + 1, 1);
        mapValue.putLong(valueIndex + 2, INITIAL_CAPACITY);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final double value = arg.getDouble(record);
        if (Double.isNaN(value)) {
            return;
        }
        long count = mapValue.getLong(valueIndex + 1);
        if (count <= 0) {
            long ptr = allocator.malloc(INITIAL_CAPACITY * ENTRY_SIZE);
            Unsafe.putLong(ptr, rowId);
            Unsafe.putDouble(ptr + 8, value);
            mapValue.putLong(valueIndex, ptr);
            mapValue.putLong(valueIndex + 1, 1);
            mapValue.putLong(valueIndex + 2, INITIAL_CAPACITY);
            return;
        }
        if (count >= maxValues) {
            throw CairoException.nonCritical().position(functionPosition)
                    .put(name).put("() result exceeds max size of ")
                    .put((long) maxValues * 3).put(" bytes");
        }
        long capacity = mapValue.getLong(valueIndex + 2);
        long ptr = mapValue.getLong(valueIndex);
        if (count >= capacity) {
            long newCapacity = capacity * 2;
            ptr = allocator.realloc(ptr, capacity * ENTRY_SIZE, newCapacity * ENTRY_SIZE);
            mapValue.putLong(valueIndex, ptr);
            mapValue.putLong(valueIndex + 2, newCapacity);
        }
        long offset = count * ENTRY_SIZE;
        Unsafe.putLong(ptr + offset, rowId);
        Unsafe.putDouble(ptr + offset + 8, value);
        mapValue.putLong(valueIndex + 1, count + 1);
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
        long count = rec.getLong(valueIndex + 1);
        if (count <= 0) {
            return null;
        }
        long ptr = rec.getLong(valueIndex);
        if (ptr == cachedPairPtrA) {
            return viewA.of(cachedRenderPtrA, cachedRenderPtrA + cachedRenderLenA);
        }
        final long outBytes = renderForPtr(ptr, (int) count);
        final long out = lastRenderPtr;
        cachedPairPtrA = ptr;
        cachedRenderPtrA = out;
        cachedRenderLenA = outBytes;
        return viewA.of(out, out + outBytes);
    }

    @Override
    public @Nullable Utf8Sequence getVarcharB(Record rec) {
        long count = rec.getLong(valueIndex + 1);
        if (count <= 0) {
            return null;
        }
        long ptr = rec.getLong(valueIndex);
        if (ptr == cachedPairPtrB) {
            return viewB.of(cachedRenderPtrB, cachedRenderPtrB + cachedRenderLenB);
        }
        // If side A already rendered this ptr we can share its buffer
        // rather than allocating again - the bytes in allocator memory
        // are stable for the query's lifetime.
        if (ptr == cachedPairPtrA) {
            cachedPairPtrB = ptr;
            cachedRenderPtrB = cachedRenderPtrA;
            cachedRenderLenB = cachedRenderLenA;
            return viewB.of(cachedRenderPtrA, cachedRenderPtrA + cachedRenderLenA);
        }
        final long outBytes = renderForPtr(ptr, (int) count);
        final long out = lastRenderPtr;
        cachedPairPtrB = ptr;
        cachedRenderPtrB = out;
        cachedRenderLenB = outBytes;
        return viewB.of(out, out + outBytes);
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        arg.init(symbolTableSource, executionContext);
        if (minFunc != null) {
            minFunc.init(symbolTableSource, executionContext);
        }
        if (maxFunc != null) {
            maxFunc.init(symbolTableSource, executionContext);
        }
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
        columnTypes.add(ColumnType.LONG); // +0 pair buffer pointer
        columnTypes.add(ColumnType.LONG); // +1 count
        columnTypes.add(ColumnType.LONG); // +2 capacity (entries)
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isScalar() {
        return false;
    }

    /**
     * Must return false because this function stores a per-worker
     * {@link GroupByAllocator} reference. Returning true would cause the
     * parallel GROUP BY engine to share a single function instance across
     * workers, leading to concurrent access to the non-thread-safe allocator.
     */
    @Override
    public boolean isThreadSafe() {
        return false;
    }

    /**
     * Merges the source worker's observation buffer into the destination
     * buffer. Both are sorted by rowId within their respective workers.
     * The merge produces a single sorted buffer via a merge-sort merge
     * step, allocated in the destination's allocator - either the owner's
     * allocator on the non-sharded merge path or a per-worker allocator
     * on the sharded merge path.
     * <p>
     * When dest is empty, the source buffer is copied into a fresh
     * allocation in the destination's allocator rather than aliasing the
     * raw pointer, because the source buffer lives in a separate allocator
     * that will be reclaimed independently.
     */
    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        long srcCount = srcValue.getLong(valueIndex + 1);
        if (srcCount <= 0) {
            return;
        }
        long srcPtr = srcValue.getLong(valueIndex);
        long destCount = destValue.getLong(valueIndex + 1);
        if (destCount <= 0) {
            long newCapacity = Math.max(INITIAL_CAPACITY, srcCount);
            long newPtr = allocator.malloc(newCapacity * ENTRY_SIZE);
            Vect.memcpy(newPtr, srcPtr, srcCount * ENTRY_SIZE);
            destValue.putLong(valueIndex, newPtr);
            destValue.putLong(valueIndex + 1, srcCount);
            destValue.putLong(valueIndex + 2, newCapacity);
            return;
        }
        long destPtr = destValue.getLong(valueIndex);
        long mergedCount = destCount + srcCount;
        long mergedPtr = allocator.malloc(mergedCount * ENTRY_SIZE);
        long di = 0, si = 0, mi = 0;
        while (di < destCount && si < srcCount) {
            long destAddr = destPtr + di * ENTRY_SIZE;
            long srcAddr = srcPtr + si * ENTRY_SIZE;
            long destRowId = Unsafe.getLong(destAddr);
            long srcRowId = Unsafe.getLong(srcAddr);
            long mergedAddr = mergedPtr + mi * ENTRY_SIZE;
            // Inline the 16-byte copy as two longs - a JNI Vect.memcpy
            // call dominates over the payload cost for entries this small.
            if (destRowId <= srcRowId) {
                Unsafe.putLong(mergedAddr, destRowId);
                Unsafe.putLong(mergedAddr + 8, Unsafe.getLong(destAddr + 8));
                di++;
            } else {
                Unsafe.putLong(mergedAddr, srcRowId);
                Unsafe.putLong(mergedAddr + 8, Unsafe.getLong(srcAddr + 8));
                si++;
            }
            mi++;
        }
        if (di < destCount) {
            Vect.memcpy(mergedPtr + mi * ENTRY_SIZE, destPtr + di * ENTRY_SIZE, (destCount - di) * ENTRY_SIZE);
        }
        if (si < srcCount) {
            Vect.memcpy(mergedPtr + mi * ENTRY_SIZE, srcPtr + si * ENTRY_SIZE, (srcCount - si) * ENTRY_SIZE);
        }
        destValue.putLong(valueIndex, mergedPtr);
        destValue.putLong(valueIndex + 1, mergedCount);
        destValue.putLong(valueIndex + 2, mergedCount);
    }

    @Override
    public void setAllocator(GroupByAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, 0);
        mapValue.putLong(valueIndex + 1, 0);
        mapValue.putLong(valueIndex + 2, 0);
    }

    @Override
    public boolean supportsParallelism() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        // Either all three of (minFunc, maxFunc, widthFunc) are non-null
        // (sparkline(D, d, d, i) factory) or all are null (sparkline(D)
        // factory). There is no intermediate case.
        sink.val(name).val('(').val(arg);
        if (minFunc != null) {
            sink.val(',').val(minFunc);
            sink.val(',').val(maxFunc);
            sink.val(',').val(widthFunc);
        }
        sink.val(')');
    }

    @Override
    public void toTop() {
        arg.toTop();
        if (minFunc != null) {
            minFunc.toTop();
        }
        if (maxFunc != null) {
            maxFunc.toTop();
        }
        if (widthFunc != null) {
            widthFunc.toTop();
        }
    }

    private char charForValue(double value, double min, double range) {
        if (range == 0.0) {
            return chars[chars.length - 1];
        }
        double clamped = Math.max(min, Math.min(min + range, value));
        int idx = (int) ((clamped - min) / range * (chars.length - 1));
        return chars[Math.min(idx, chars.length - 1)];
    }

    private int effectiveWidth(int valueCount) {
        if (widthFunc != null) {
            int w = widthFunc.getInt(null);
            if (w < 1) {
                throw CairoException.nonCritical().position(widthPosition)
                        .put("width must be a positive integer");
            }
            return w;
        }
        return valueCount;
    }

    // Renders the pair buffer at `ptr` into a fresh allocator-backed
    // output buffer. Writes the output pointer into {@link #lastRenderPtr}
    // and returns the byte length.
    private long renderForPtr(long ptr, int size) {
        // Single-pass min+max when either is auto. Halves the pre-render
        // scan cost vs two separate full walks.
        double userMin = minFunc != null ? minFunc.getDouble(null) : Double.NaN;
        double userMax = maxFunc != null ? maxFunc.getDouble(null) : Double.NaN;
        // When both bounds are user-supplied, reject an inverted range up
        // front. Leaving it through produces a negative range that clamps
        // every value to min and renders as a flat all-bottom line, which
        // hides the bug from the user.
        if (!Double.isNaN(userMin) && !Double.isNaN(userMax) && userMin > userMax) {
            throw CairoException.nonCritical().position(functionPosition)
                    .put(name).put("() min must not exceed max [min=")
                    .put(userMin).put(", max=").put(userMax).put(']');
        }
        double min, max;
        if (Double.isNaN(userMin) || Double.isNaN(userMax)) {
            double first = Unsafe.getDouble(ptr + 8);
            min = max = first;
            for (int i = 1; i < size; i++) {
                double v = Unsafe.getDouble(ptr + i * ENTRY_SIZE + 8);
                if (v < min) min = v;
                if (v > max) max = v;
            }
            if (!Double.isNaN(userMin)) min = userMin;
            if (!Double.isNaN(userMax)) max = userMax;
        } else {
            min = userMin;
            max = userMax;
        }

        final int width = effectiveWidth(size);
        final int outChars = Math.min(width, size);
        // Per-worker computeNext caps size at maxValues, but two workers'
        // partials can merge to a larger count. Enforce the cap on the
        // rendered char count so parallel and serial paths throw at the
        // same boundary.
        if (outChars > maxValues) {
            throw CairoException.nonCritical().position(functionPosition)
                    .put(name).put("() result exceeds max size of ")
                    .put((long) maxValues * 3).put(" bytes");
        }
        final long outBytes = (long) outChars * 3;
        // Allocate one extra byte so putChar can emit a 4-byte little-endian
        // word for every character without worrying about overrunning the
        // last slot. The trailing byte is garbage and excluded from the
        // returned view length.
        long out = allocator.malloc(outBytes + 1);
        if (width >= size) {
            renderValues(out, ptr, size, min, max);
        } else {
            renderSubsampled(out, ptr, size, width, min, max);
        }
        lastRenderPtr = out;
        return outBytes;
    }

    // All chars used by sparkline/bar live in the BMP and encode as three
    // UTF-8 bytes: 1110xxxx 10yyyyyy 10zzzzzz. Pack the trio into a single
    // little-endian int and emit with one putInt; the caller overallocates
    // one trailing byte so the last character's sloppy 4th byte is safe.
    // Assumes little-endian host (x86, ARM64 default) - matches the rest
    // of QuestDB's native storage.
    private void putChar(long out, int pos, char c) {
        int packed = (0xE0 | ((c >> 12) & 0x0F))
                | ((0x80 | ((c >> 6) & 0x3F)) << 8)
                | ((0x80 | (c & 0x3F)) << 16);
        Unsafe.putInt(out + pos * 3L, packed);
    }

    private void renderSubsampled(long out, long ptr, int size, int width, double min, double max) {
        // Caller guarantees 0 < width < size, so bucketSize > 1 and every
        // bucket contains at least one value.
        double range = max - min;
        double bucketSize = (double) size / width;
        for (int i = 0; i < width; i++) {
            int from = (int) (i * bucketSize);
            int to = (int) ((i + 1) * bucketSize);
            double sum = 0;
            for (int j = from; j < to; j++) {
                sum += Unsafe.getDouble(ptr + j * ENTRY_SIZE + 8);
            }
            double avg = sum / (to - from);
            putChar(out, i, charForValue(avg, min, range));
        }
    }

    private void renderValues(long out, long ptr, int size, double min, double max) {
        double range = max - min;
        for (int i = 0; i < size; i++) {
            double v = Unsafe.getDouble(ptr + i * ENTRY_SIZE + 8);
            putChar(out, i, charForValue(v, min, range));
        }
    }
}

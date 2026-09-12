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
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByCharSink;
import io.questdb.griffin.engine.groupby.GroupByLongList;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Rows;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectUtf16Sink;

/**
 * Deterministic, parallel-capable {@code string_agg(str, delimiter)}.
 * <p>
 * Each group's map value holds two pointers:
 * <pre>
 * | valueIndex: char sink ptr | valueIndex+1: run-list ptr |
 * +----------------------------+------------------------------+
 * |          8 bytes           |            8 bytes           |
 * +----------------------------+------------------------------+
 * </pre>
 * The char sink ({@link GroupByCharSink}) holds the raw characters of every accepted value,
 * concatenated without gaps. The run list ({@link GroupByLongList}) is a flat sequence of
 * (rowId, sinkLocation) pairs, one pair per run of values that arrived from the same page frame:
 * <pre>
 * | rowId (run start) | sinkLocation | rowId (run start) | sinkLocation | ...
 * +---------------------+----------------+---------------------+----------------+
 * |       8 bytes       |    8 bytes     |       8 bytes       |    8 bytes     |
 * +---------------------+----------------+---------------------+----------------+
 * </pre>
 * {@code sinkLocation} locates a run inside the char sink:
 * <pre>
 * | offset into char sink | run length (chars) |
 * +-------------------------+----------------------+
 * |         32 bits         |       32 bits        |
 * +-------------------------+----------------------+
 * </pre>
 * Worker threads append runs in an arbitrary order relative to each other, though values within
 * a single run are always in scan order (one page frame is always scanned by one thread).
 * Determinism is restored at read time: {@link #materialize} sorts runs by their starting rowId
 * and concatenates them, reproducing the same output as a single-threaded scan regardless of how
 * work was distributed across threads.
 */
class StringAggGroupByFunction extends StrFunction implements UnaryFunction, GroupByFunction {
    private final Function arg;
    private final char delimiter;
    private final int functionPosition;
    private final GroupByLongList listA = new GroupByLongList(16);
    private final GroupByLongList listB = new GroupByLongList(16);
    private final int maxBytes;
    private final DirectUtf16Sink resultSinkA = new DirectUtf16Sink(16);
    private final DirectUtf16Sink resultSinkB = new DirectUtf16Sink(16);
    private final GroupByCharSink sinkA = new GroupByCharSink();
    private final GroupByCharSink sinkB = new GroupByCharSink();
    private final DirectLongList sortCpy = new DirectLongList(32, MemoryTag.NATIVE_GROUP_BY_FUNCTION);
    private final DirectLongList sortData = new DirectLongList(32, MemoryTag.NATIVE_GROUP_BY_FUNCTION);
    private int totalMemoryUsed;
    private int valueIndex;

    public StringAggGroupByFunction(Function arg, int functionPosition, char delimiter, int maxBytes) {
        this.arg = arg;
        this.delimiter = delimiter;
        this.functionPosition = functionPosition;
        this.maxBytes = maxBytes;
    }

    @Override
    public void clear() {
        sinkA.of(0);
        sinkB.of(0);
        listA.resetPtr();
        listB.resetPtr();
        resultSinkA.clear();
        resultSinkB.clear();
        totalMemoryUsed = 0;
    }

    @Override
    public void close() {
        Misc.free(resultSinkA);
        Misc.free(resultSinkB);
        Misc.free(sortData);
        Misc.free(sortCpy);
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final CharSequence str = arg.getStrA(record);
        if (str == null) {
            mapValue.putLong(valueIndex, 0);
            mapValue.putLong(valueIndex + 1, 0);
            return;
        }
        sinkA.of(0);
        listA.of(0);
        append(rowId, str);
        mapValue.putLong(valueIndex, sinkA.ptr());
        mapValue.putLong(valueIndex + 1, listA.ptr());
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final CharSequence str = arg.getStrA(record);
        if (str == null) {
            return;
        }
        sinkA.of(mapValue.getLong(valueIndex));
        listA.of(mapValue.getLong(valueIndex + 1));
        append(rowId, str);
        mapValue.putLong(valueIndex, sinkA.ptr());
        mapValue.putLong(valueIndex + 1, listA.ptr());
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public CharSequence getStrA(Record rec) {
        return materialize(rec, resultSinkA);
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return materialize(rec, resultSinkB);
    }

    @Override
    public int getValueIndex() {
        return valueIndex;
    }

    @Override
    public void initValueIndex(int valueIndex) {
        this.valueIndex = valueIndex;
    }

    @Override
    public void initValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.LONG);
        columnTypes.add(ColumnType.LONG);
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
        final long srcListPtr = srcValue.getLong(valueIndex + 1);
        if (srcListPtr == 0) {
            return;
        }
        final long destListPtr = destValue.getLong(valueIndex + 1);
        if (destListPtr == 0) {
            destValue.putLong(valueIndex, srcValue.getLong(valueIndex));
            destValue.putLong(valueIndex + 1, srcListPtr);
            return;
        }

        sinkA.of(destValue.getLong(valueIndex));
        sinkB.of(srcValue.getLong(valueIndex));
        listA.of(destListPtr);
        listB.of(srcListPtr);

        final int destCharOffset = sinkA.length();
        final int srcSize = listB.size();
        for (int i = 0; i < srcSize; i += 2) {
            final long srcRowId = listB.get(i);
            final long sinkLocation = listB.get(i + 1);
            final int off = unpackOffset(sinkLocation);
            final int len = unpackLen(sinkLocation);
            listA.add(srcRowId);
            listA.add(pack(off + destCharOffset, len));
            totalMemoryUsed += len * 2 + 2;
        }
        assertSizeCompliance();

        sinkA.put(sinkB);

        destValue.putLong(valueIndex, sinkA.ptr());
        destValue.putLong(valueIndex + 1, listA.ptr());
    }

    @Override
    public void setAllocator(GroupByAllocator allocator) {
        sinkA.setAllocator(allocator);
        sinkB.setAllocator(allocator);
        listA.setAllocator(allocator);
        listB.setAllocator(allocator);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, 0);
        mapValue.putLong(valueIndex + 1, 0);
    }

    @Override
    public boolean supportsParallelism() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("string_agg(").val(arg).val(',').val(delimiter).val(')');
    }

    @Override
    public void toTop() {
        UnaryFunction.super.toTop();
        totalMemoryUsed = 0;
    }

    private static long pack(int offset, int len) {
        return ((long) offset << 32) | (len & 0xffffffffL);
    }

    private static int unpackLen(long sinkLocation) {
        return (int) sinkLocation;
    }

    private static int unpackOffset(long sinkLocation) {
        return (int) (sinkLocation >>> 32);
    }

    private void append(long rowId, CharSequence str) {
        final int len = str.length();
        final int size = listA.size();
        final boolean sameRun = size > 0 && Rows.toPartitionIndex(rowId) == Rows.toPartitionIndex(listA.get(size - 2));
        if (sameRun) {
            final int startOffset = unpackOffset(listA.get(size - 1));
            sinkA.putAscii(delimiter);
            sinkA.put(str);
            listA.set(size - 1, pack(startOffset, sinkA.length() - startOffset));
        } else {
            final int offset = sinkA.length();
            sinkA.put(str);
            listA.add(rowId);
            listA.add(pack(offset, len));
        }
        totalMemoryUsed += len * 2 + (size > 0 ? 2 : 0);
        assertSizeCompliance();
    }

    private void assertSizeCompliance() {
        if (totalMemoryUsed > maxBytes) {
            throw CairoException.nonCritical()
                    .position(functionPosition)
                    .put("string_agg() result exceeds max size of ")
                    .put(maxBytes)
                    .put(" bytes");
        }
    }

    private CharSequence materialize(Record rec, DirectUtf16Sink resultSink) {
        final long listPtr = rec.getLong(valueIndex + 1);
        if (listPtr == 0) {
            return null;
        }
        listA.of(listPtr);
        final int size = listA.size();
        final int count = size / 2;
        sortData.clear();
        sortData.ensureCapacity(size);
        Vect.memcpy(sortData.getAddress(), listA.dataPtr(), (long) size * Long.BYTES);
        if (count > 1) {
            sortCpy.clear();
            sortCpy.ensureCapacity(size);
            Vect.radixSortLongIndexAscInPlace(sortData.getAddress(), count, sortCpy.getAddress());
        }
        sinkA.of(rec.getLong(valueIndex));
        resultSink.clear();
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                resultSink.put(delimiter);
            }
            final long sinkLocation = sortData.get(2L * i + 1);
            final int off = unpackOffset(sinkLocation);
            final int len = unpackLen(sinkLocation);
            for (int j = 0; j < len; j++) {
                resultSink.put(sinkA.charAt(off + j));
            }
        }
        return resultSink;
    }
}

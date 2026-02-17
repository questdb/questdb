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

package io.questdb.cutlass.pgwire;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.FlatArrayView;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.BoolList;
import io.questdb.std.LongList;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8Sequence;

/**
 * A read-only view into a binary encoded VARCHAR array received from PGWire clients.
 * The view is backed directly by the memory arena that holds data received along with BIND messages.
 * <p>
 * Uses a LongList to store element offsets for O(1) random access to variable-length string elements.
 */
public final class PGNonNullVarcharArrayView extends ArrayView implements FlatArrayView, Mutable {
    private final LongList elementOffsets = new LongList();
    private final BoolList isAsciis = new BoolList();
    private final DirectUtf8String view = new DirectUtf8String();

    public PGNonNullVarcharArrayView() {
        this.isVanilla = false;
        this.flatViewLength = 1;
        this.flatView = this;
    }

    @Override
    public void appendToMemFlat(MemoryA mem, int offset, int length) {
        for (int i = 0; i < length; i++) {
            mem.putVarchar(getVarcharAt(i));
        }
    }

    @Override
    public void clear() {
        elementOffsets.clear();
        isAsciis.clear();
        shape.clear();
        strides.clear();
        flatViewLength = 1;
        flatViewOffset = 0;
        type = ColumnType.UNDEFINED;
    }

    @Override
    public double getDoubleAtAbsIndex(int elemIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLongAtAbsIndex(int elemIndex) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the UTF-8 string at the specified index.
     *
     * @param index the element index (0-based)
     * @return the UTF-8 string at the specified index
     */
    public Utf8Sequence getVarcharAt(int index) {
        if (index < 0 || index >= flatViewLength) {
            throw CairoException.nonCritical().put("array index out of bounds [index=").put(index)
                    .put(", length=").put(flatViewLength).put(']');
        }

        long elementAddr = elementOffsets.getQuick(index);
        if (elementAddr == -1) {
            return null;
        }
        int elementLen = Numbers.bswap(Unsafe.getUnsafe().getInt(elementAddr));
        return view.of(elementAddr + Integer.BYTES, elementAddr + Integer.BYTES + elementLen, isAsciis.get(index));
    }

    @Override
    public int length() {
        return flatViewLength;
    }

    private static boolean isAscii(long addr, int len) {
        for (int i = 0; i < len; i++) {
            if (Unsafe.getUnsafe().getByte(addr + i) < 0) {
                return false;
            }
        }
        return true;
    }

    private void defaultStrides() {
        final int nDims = shape.size();
        strides.clear();
        for (int i = 0; i < nDims; i++) {
            strides.add(0);
        }

        int stride = 1;
        for (int i = nDims - 1; i >= 0; i--) {
            strides.set(i, stride);
            stride *= shape.get(i);
        }
    }

    void addDimLen(int dimLen) {
        shape.add(dimLen);
        flatViewLength *= dimLen;
    }

    /**
     * Sets memory pointers and builds element offset index for O(1) random access.
     * Must be called after shape was set up via {@link #addDimLen(int)}.
     */
    void setPtrAndBuildOffsetIndex(long lo, long hi, PGPipelineEntry pipelineEntry)
            throws PGMessageProcessingException {
        assert shape.size() > 0;

        final int elementCount = flatViewLength;
        elementOffsets.clear();
        elementOffsets.setPos(elementCount);
        isAsciis.clear();
        isAsciis.setPos(elementCount);
        long ptr = lo;
        for (int i = 0; i < elementCount; i++) {
            int elementLenBE = Unsafe.getUnsafe().getInt(ptr);
            // -1 indicates NULL element
            if (elementLenBE == -1) {
                elementOffsets.setQuick(i, -1);
                isAsciis.setQuick(i, true);
                ptr += Integer.BYTES;
            } else {
                int elementLen = Numbers.bswap(elementLenBE);
                elementOffsets.setQuick(i, ptr);
                isAsciis.setQuick(i, isAscii(ptr + Integer.BYTES, elementLen));
                ptr += Integer.BYTES + elementLen;
            }
        }

        if (ptr != hi) {
            throw PGMessageProcessingException.instance(pipelineEntry)
                    .put("unexpected array size [expected=").put(hi - lo)
                    .put(", actual=").put(ptr - lo).put(']');
        }
        this.type = ColumnType.encodeArrayType(ColumnType.VARCHAR, shape.size(), false);
        defaultStrides();
    }
}

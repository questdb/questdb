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
import io.questdb.cairo.arr.FlatArrayView;
import io.questdb.cairo.arr.MutableArray;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;

/**
 * A view into a binary encoded array received from PGWire clients. The view is backed directly by the memory arena that
 * holds data received along with BIND messages.
 */
final class PGNonNullBinaryArrayView extends MutableArray implements FlatArrayView, Mutable {
    private long hi;
    private long lo;

    public PGNonNullBinaryArrayView() {
        this.isVanilla = false;
        this.flatViewLength = 1;
        this.flatView = this;
    }

    @Override
    public void appendToMemFlat(MemoryA mem, int offset, int length) {
        switch (ColumnType.decodeArrayElementType(type)) {
            case ColumnType.LONG:
            case ColumnType.DOUBLE:
                for (int i = 0; i < length; i++) {
                    mem.putLong(getLong(i));
                }
                break;
            default:
                throw new UnsupportedOperationException("not implemented yet");
        }
    }

    @Override
    public void clear() {
        shape.clear();
        strides.clear();
        flatViewLength = 1;
        lo = 0;
        hi = 0;
        type = ColumnType.UNDEFINED;
    }

    @Override
    public double getDoubleAtAbsIndex(int flatIndex) {
        final long addr = lo + Integer.BYTES + ((long) flatIndex * (Double.BYTES + Integer.BYTES));
        assert addr < hi;
        long networkOrderVal = Unsafe.getUnsafe().getLong(addr);
        return Double.longBitsToDouble(Numbers.bswap(networkOrderVal));
    }

    @Override
    public long getLongAtAbsIndex(int flatIndex) {
        final long addr = lo + Integer.BYTES + ((long) flatIndex * (Long.BYTES + Integer.BYTES));
        assert addr < hi;
        long networkOrderVal = Unsafe.getUnsafe().getLong(addr);
        return Numbers.bswap(networkOrderVal);
    }

    @Override
    public int length() {
        return flatViewLength;
    }

    void addDimLen(int dimLen) {
        shape.add(dimLen);
        flatViewLength *= dimLen;
    }

    /**
     * Sets memory pointers to the PGWire binary array data and configures array structure.
     *
     * <p>This method initializes the view by:</p>
     * <ol>
     *   <li>Determining the element type based on PostgreSQL OID</li>
     *   <li>Validating that the array contains no NULL elements</li>
     *   <li>Setting memory bounds (lo and hi pointers)</li>
     *   <li>Configuring array type information</li>
     *   <li>Setting up array strides for multi-dimensional access</li>
     *   <li>Validating the total memory size matches expected size based on dimensions</li>
     * </ol>
     * <p>After this method is called, the view is ready to be used to access array elements.</p>
     * <strong>Important:</strong> This method may be called only after shape was set up by the caller via {@link #addDimLen(int)}.
     *
     * @param lo            Start address of the binary array data in memory
     * @param hi            End address of the binary array data in memory (exclusive)
     * @param pgOidType     PostgreSQL OID type identifier
     * @param pipelineEntry The pipeline entry context for error reporting
     * @throws PGMessageProcessingException If array structure is invalid or contains unsupported elements
     * @throws CairoException               If array contains NULL elements or has unsupported element type
     */
    void setPtrAndCalculateStrides(long lo, long hi, int pgOidType, PGPipelineEntry pipelineEntry) throws PGMessageProcessingException {
        assert shape.size() > 0;

        short componentNativeType;
        int expectedElementSize;
        switch (pgOidType) {
            case PGOids.PG_INT8:
                componentNativeType = ColumnType.LONG;
                expectedElementSize = Long.BYTES;
                break;
            case PGOids.PG_FLOAT8:
                componentNativeType = ColumnType.DOUBLE;
                expectedElementSize = Double.BYTES;
                break;
            default:
                throw CairoException.nonCritical().put("unsupported array type, only arrays of int8 and float8 are supported [pgOid=").put(pgOidType).put(']');
        }

        // validate that there are no nulls in the array since we don't support them
        int increment = Integer.BYTES + expectedElementSize;
        int expectedElementSizeBE = Numbers.bswap(expectedElementSize);
        for (long p = lo; p < hi; p += increment) {

            // element size as reported by the client
            int actualElementSizeBE = Unsafe.getUnsafe().getInt(p);

            // -1 is a special value that indicates a NULL element
            // no need to swap bytes since -1 is always -1, regardless of endianness, it's all 1s
            if (actualElementSizeBE == -1) {
                throw CairoException.nonCritical().put("null elements are not supported in arrays");
            }

            if (actualElementSizeBE != expectedElementSizeBE) {
                int actualElementSize = Numbers.bswap(actualElementSizeBE);
                throw PGMessageProcessingException.instance(pipelineEntry).put("unexpected array element size [expected=").put(expectedElementSize).put(", actual=").put(actualElementSize).put(']');
            }
        }

        // Check client is not misbehaving. Buggy clients can send arrays with wrong size. see: https://github.com/pgjdbc/pgjdbc/issues/3567
        // Important: We have to validate that the array size is as expected only after we have checked for null elements.
        // Since a presence of nulls also affects the size of the array, and we want to report null elements to user
        // since that's more likely than a buggy client.
        long totalExpectedSizeBytes = (long) (expectedElementSize + Integer.BYTES) * flatViewLength;
        if (hi - lo != totalExpectedSizeBytes) {
            throw PGMessageProcessingException.instance(pipelineEntry).put("unexpected array size [expected=").put(totalExpectedSizeBytes).put(", actual=").put(hi - lo).put(']');
        }

        // ok, all good, looks we were given a valid array
        this.lo = lo;
        this.hi = hi;
        this.type = ColumnType.encodeArrayType(componentNativeType, shape.size());
        resetToDefaultStrides();
    }
}

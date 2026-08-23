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

package io.questdb.griffin.engine.functions.window;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

public abstract class AbstractWindowFunctionFactory implements FunctionFactory {

    @Override
    public boolean isWindow() {
        return true;
    }

    // Snapshots the partition key types. The code generator hands out a reusable buffer it clears and
    // rebuilds for each window column's PARTITION BY, so a partitioned function that reads the types
    // back later (e.g. when it builds its map lazily in initRecordComparator) must copy them up front.
    static ArrayColumnTypes copyKeyTypes(ColumnTypes keyTypes) {
        final ArrayColumnTypes copy = new ArrayColumnTypes();
        for (int i = 0, n = keyTypes.getColumnCount(); i < n; i++) {
            copy.add(keyTypes.getColumnType(i));
        }
        return copy;
    }

    /**
     * Copies one partition's whole ring slab from {@code srcArena} into {@code dstArena} and
     * writes the slab's new start offset into {@code dstValue}. Used by the live-view frontier
     * sweep to re-home the partitions that survive so the arena can be compacted down to them;
     * see {@code BasePartitionedWindowFunction.copyRingSlab}.
     * <p>
     * The whole slab moves, not just the live records: {@code firstIdx} and {@code size} address
     * positions WITHIN the capacity, so preserving the slab's internal geometry is what lets the
     * caller leave every other slot of the copied value alone. Compacting the ring itself would
     * mean rewriting those two as well, and the sweep has no reason to - the arena's waste is
     * the dead partitions, not the slack inside a live one's ring.
     * <p>
     * {@code appendAddressFor} can reallocate {@code dstArena}, so the destination address is
     * taken from its return value and the new offset derived from the page address only after
     * that; reading either earlier can name a buffer that has already moved.
     */
    static void copyRingSlab(
            MapValue srcValue,
            MapValue dstValue,
            MemoryARW srcArena,
            MemoryARW dstArena,
            int startOffsetValueIndex,
            int capacityValueIndex,
            int recordSize
    ) {
        final long capacity = srcValue.getLong(capacityValueIndex);
        final long startOffset = srcValue.getLong(startOffsetValueIndex);
        final long bytes = capacity * recordSize;
        // Range-check the pair before trusting it, because the failure this guards against is
        // otherwise SILENT. The two indices are declared per concrete class against a layout that
        // differs between the RANGE and ROWS shapes of the same aggregate and shifts again in
        // subclasses that add a leading slot; naming the wrong pair reads some neighbouring
        // counter as a geometry, copies the wrong bytes (or, when the misread capacity is zero,
        // copies none and leaves the entry naming the arena as it was before the truncate) and
        // corrupts results only later and elsewhere. An existing entry always has a slab - the
        // first row of a partition allocates one, and resetPartition zeroes the accumulator slots
        // without touching the geometry - so a non-positive capacity or an out-of-range extent
        // means the caller named the wrong slots, and that is worth failing the refresh over.
        if (capacity <= 0 || startOffset < 0 || startOffset + bytes > srcArena.getAppendOffset()) {
            throw CairoException.critical(0)
                    .put("window ring slab out of range, check the (startOffset, capacity) value indices [startOffset=")
                    .put(startOffset)
                    .put(", capacity=").put(capacity)
                    .put(", recordSize=").put(recordSize)
                    .put(", arenaUsed=").put(srcArena.getAppendOffset())
                    .put(']');
        }
        final long dstAddress = dstArena.appendAddressFor(bytes);
        Vect.memcpy(dstAddress, srcArena.getPageAddress(0) + startOffset, bytes);
        dstValue.putLong(startOffsetValueIndex, dstAddress - dstArena.getPageAddress(0));
    }

    // Mirrors SqlCodeGenerator's private coerceRuntimeConstantType and preserves the former SUBSAMPLE
    // target/stride validation contract: resolve a still-UNDEFINED bind-variable arg to `type`, otherwise
    // require the arg to already be a constant/runtime-constant whose type is convertible to `type`
    // (message/pos on mismatch). Shared by the keep-flag window factories
    // (uniform/cadence/m4/minmax/lttb/lttb-gap).
    static void coerceRuntimeConstantType(Function func, int type, SqlExecutionContext context, CharSequence message, int pos) throws SqlException {
        if (ColumnType.isUndefined(func.getType())) {
            func.assignType(type, context.getBindVariableService());
        } else if ((!func.isConstant() && !func.isRuntimeConstant()) || !ColumnType.isConvertibleFrom(func.getType(), type)) {
            throw SqlException.$(pos, message);
        }
    }

    static long validateStride(long stride, int position) throws SqlException {
        if (stride == Numbers.LONG_NULL) {
            throw SqlException.$(position, "stride must be set");
        }
        if (stride < 1) {
            throw SqlException.$(position, "stride must be at least 1");
        }
        if (stride > Integer.MAX_VALUE) {
            throw SqlException.$(position, "stride exceeds maximum of ").put(Integer.MAX_VALUE);
        }
        return stride;
    }

    static long validateTarget(long target, int position) throws SqlException {
        if (target == Numbers.LONG_NULL) {
            throw SqlException.$(position, "target point count must be set");
        }
        if (target < 2) {
            throw SqlException.$(position, "target points must be at least 2");
        }
        if (target > Integer.MAX_VALUE) {
            throw SqlException.$(position, "target points exceeds maximum of ").put(Integer.MAX_VALUE);
        }
        return target;
    }

    static void expandRingBuffer(MemoryARW memory, RingBufferDesc desc, int recordSize) {
        desc.capacity <<= 1;
        long oldAddress = memory.getPageAddress(0) + desc.startOffset;
        long newAddress = -1;

        // try to find matching block in free list
        for (int i = 0, n = desc.freeList.size(); i < n; i += 2) {
            if (desc.freeList.getQuick(i) == desc.capacity) {
                newAddress = memory.getPageAddress(0) + desc.freeList.getQuick(i + 1);
                // replace block info with ours
                desc.freeList.setQuick(i, desc.size);
                desc.freeList.setQuick(i + 1, desc.startOffset);
                break;
            }
        }

        if (newAddress == -1) {
            newAddress = memory.appendAddressFor(desc.capacity * recordSize);
            // call above can end up resizing and thus changing memory start address
            oldAddress = memory.getPageAddress(0) + desc.startOffset;
            desc.freeList.add(desc.size, desc.startOffset);
        }

        if (desc.firstIdx == 0) {
            Vect.memcpy(newAddress, oldAddress, desc.size * recordSize);
        } else {
            desc.firstIdx %= desc.size;
            //we can't simply copy because that'd leave a gap in the middle
            long firstPieceSize = (desc.size - desc.firstIdx) * recordSize;
            Vect.memcpy(newAddress, oldAddress + desc.firstIdx * recordSize, firstPieceSize);
            Vect.memcpy(newAddress + firstPieceSize, oldAddress, desc.firstIdx * recordSize);
            desc.firstIdx = 0;
        }

        desc.startOffset = newAddress - memory.getPageAddress(0);
    }

    protected boolean supportNullsDesc() {
        return false;
    }

    static abstract class BaseNullFunction extends BaseWindowFunction {
        private final boolean isRange;
        private final String name;
        private final VirtualRecord partitionByRecord;
        private final long rowHi;
        private final long rowLo;

        BaseNullFunction(Function arg, String name, long rowLo, long rowHi, boolean isRange, VirtualRecord partitionByRecord) {
            super(arg);
            this.name = name;
            this.rowLo = rowLo;
            this.rowHi = rowHi;
            this.isRange = isRange;
            this.partitionByRecord = partitionByRecord;
        }

        @Override
        public void close() {
            super.close();
            if (partitionByRecord != null) {
                Misc.freeObjList(partitionByRecord.getFunctions());
            }
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public int getPassCount() {
            return ZERO_PASS;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            if (arg != null) {
                sink.val('(').val(arg).val(')');
            } else {
                sink.val("(*)");
            }

            sink.val(" over (");
            if (partitionByRecord != null) {
                sink.val("partition by ");
                sink.val(partitionByRecord.getFunctions());
            }
            if (isRange) {
                sink.val(" range between ");
            } else {
                sink.val(" rows between ");
            }

            if (rowLo != Long.MIN_VALUE) {
                sink.val(Math.abs(rowLo));
            } else {
                sink.val("unbounded");
            }
            sink.val(" preceding and ");
            if (rowHi == 0) {
                sink.val("current row");
            } else {
                sink.val(Math.abs(rowHi)).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
        }
    }

    static class Decimal128NullFunction extends BaseNullFunction {
        private final int type;

        Decimal128NullFunction(Function arg, String name, long rowLo, long rowHi, boolean isRange, VirtualRecord partitionByRecord, int type) {
            super(arg, name, rowLo, rowHi, isRange, partitionByRecord);
            this.type = type;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.ofRawNull();
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, Decimals.DECIMAL128_HI_NULL);
            Unsafe.putLong(addr + Long.BYTES, Decimals.DECIMAL128_LO_NULL);
        }
    }

    static class Decimal16NullFunction extends BaseNullFunction {
        private final int type;

        Decimal16NullFunction(Function arg, String name, long rowLo, long rowHi, boolean isRange, VirtualRecord partitionByRecord, int type) {
            super(arg, name, rowLo, rowHi, isRange, partitionByRecord);
            this.type = type;
        }

        @Override
        public short getDecimal16(Record rec) {
            return Decimals.DECIMAL16_NULL;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), Decimals.DECIMAL16_NULL);
        }
    }

    static class Decimal256NullFunction extends BaseNullFunction {
        private final int type;

        Decimal256NullFunction(Function arg, String name, long rowLo, long rowHi, boolean isRange, VirtualRecord partitionByRecord, int type) {
            super(arg, name, rowLo, rowHi, isRange, partitionByRecord);
            this.type = type;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.ofRawNull();
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, Decimals.DECIMAL256_HH_NULL);
            Unsafe.putLong(addr + Long.BYTES, Decimals.DECIMAL256_HL_NULL);
            Unsafe.putLong(addr + 2 * Long.BYTES, Decimals.DECIMAL256_LH_NULL);
            Unsafe.putLong(addr + 3 * Long.BYTES, Decimals.DECIMAL256_LL_NULL);
        }
    }

    static class Decimal32NullFunction extends BaseNullFunction {
        private final int type;

        Decimal32NullFunction(Function arg, String name, long rowLo, long rowHi, boolean isRange, VirtualRecord partitionByRecord, int type) {
            super(arg, name, rowLo, rowHi, isRange, partitionByRecord);
            this.type = type;
        }

        @Override
        public int getDecimal32(Record rec) {
            return Decimals.DECIMAL32_NULL;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), Decimals.DECIMAL32_NULL);
        }
    }

    static class Decimal64NullFunction extends BaseNullFunction {
        private final int type;

        Decimal64NullFunction(Function arg, String name, long rowLo, long rowHi, boolean isRange, VirtualRecord partitionByRecord, int type) {
            super(arg, name, rowLo, rowHi, isRange, partitionByRecord);
            this.type = type;
        }

        @Override
        public long getDecimal64(Record rec) {
            return Decimals.DECIMAL64_NULL;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), Decimals.DECIMAL64_NULL);
        }
    }

    static class Decimal8NullFunction extends BaseNullFunction {
        private final int type;

        Decimal8NullFunction(Function arg, String name, long rowLo, long rowHi, boolean isRange, VirtualRecord partitionByRecord, int type) {
            super(arg, name, rowLo, rowHi, isRange, partitionByRecord);
            this.type = type;
        }

        @Override
        public byte getDecimal8(Record rec) {
            return Decimals.DECIMAL8_NULL;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), Decimals.DECIMAL8_NULL);
        }
    }

    static class DoubleNullFunction extends BaseNullFunction implements WindowDoubleFunction {

        DoubleNullFunction(Function arg, String name, long rowLo, long rowHi, boolean isRange, VirtualRecord partitionByRecord) {
            super(arg, name, rowLo, rowHi, isRange, partitionByRecord);
        }

        @Override
        public double getDouble(Record rec) {
            return Double.NaN;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), Double.NaN);
        }
    }

    static class LongNullFunction extends BaseNullFunction implements WindowLongFunction {
        private final long zeroValue;

        LongNullFunction(Function arg, String name, long rowLo, long rowHi, boolean isRange, VirtualRecord partitionByRecord, long zeroValue) {
            super(arg, name, rowLo, rowHi, isRange, partitionByRecord);
            this.zeroValue = zeroValue;
        }

        @Override
        public long getLong(Record rec) {
            return zeroValue;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), zeroValue);
        }
    }

    protected static class RingBufferDesc {
        long capacity;
        long firstIdx;
        LongList freeList;
        long size;
        long startOffset;

        void reset(long capacity, long startOffset, long size, long firstIdx, LongList freeList) {
            this.capacity = capacity;
            this.startOffset = startOffset;
            this.size = size;
            this.firstIdx = firstIdx;
            this.freeList = freeList;
        }
    }

    static class TimestampNullFunction extends BaseNullFunction implements WindowTimestampFunction {
        private final long zeroValue;

        TimestampNullFunction(Function arg, String name, long rowLo, long rowHi, boolean isRange, VirtualRecord partitionByRecord, long zeroValue) {
            super(arg, name, rowLo, rowHi, isRange, partitionByRecord);
            this.zeroValue = zeroValue;
        }

        @Override
        public long getDate(Record rec) {
            // zeroValue is always LONG_NULL, which is the NULL sentinel for both DATE and TIMESTAMP results.
            return zeroValue;
        }

        @Override
        public long getTimestamp(Record rec) {
            return zeroValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), zeroValue);
        }
    }
}

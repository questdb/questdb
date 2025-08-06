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

package io.questdb.griffin.engine.functions.window;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.model.WindowColumn;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

public abstract class AbstractWindowFunctionFactory implements FunctionFactory {

    @Override
    public boolean isWindow() {
        return true;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), Double.NaN);
        }
    }

    static class LongNullFunction extends BaseNullFunction implements WindowLongFunction {
        private final Long zeroValue;

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
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), zeroValue);
        }
    }

    static class RingBufferDesc {
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
}

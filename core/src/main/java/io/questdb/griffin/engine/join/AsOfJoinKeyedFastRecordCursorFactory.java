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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.*;
import io.questdb.std.str.Utf8Sequence;

public final class AsOfJoinKeyedFastRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final AsOfJoinKeyedFastRecordCursor cursor;
    private final RecordSink masterKeySink;
    private final OffheapSink masterSinkTarget = new OffheapSink();
    private final RecordSink slaveKeySink;
    private final OffheapSink slaveSinkTarget = new OffheapSink();

    public AsOfJoinKeyedFastRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordSink masterKeySink,
            RecordCursorFactory slaveFactory,
            RecordSink slaveKeySink,
            int columnSplit,
            JoinContext joinContext) {
        super(metadata, joinContext, masterFactory, slaveFactory);
        assert slaveFactory.supportsTimeFrameCursor();
        this.masterKeySink = masterKeySink;
        this.slaveKeySink = slaveKeySink;
        this.cursor = new AsOfJoinKeyedFastRecordCursor(
                columnSplit,
                NullRecordFactory.getInstance(slaveFactory.getMetadata()),
                masterFactory.getMetadata().getTimestampIndex(),
                slaveFactory.getMetadata().getTimestampIndex(),
                configuration.getSqlAsOfJoinLookAhead()
        );
    }

    @Override
    public boolean followedOrderByAdvice() {
        return masterFactory.followedOrderByAdvice();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor masterCursor = masterFactory.getCursor(executionContext);
        TimeFrameRecordCursor slaveCursor = null;
        try {
            slaveCursor = slaveFactory.getTimeFrameCursor(executionContext);
            cursor.of(masterCursor, slaveCursor);
            return cursor;
        } catch (Throwable e) {
            Misc.free(slaveCursor);
            Misc.free(masterCursor);
            throw e;
        }
    }

    @Override
    public int getScanDirection() {
        return masterFactory.getScanDirection();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("AsOf Join Fast Scan");
        sink.attr("condition").val(joinContext);
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        Misc.freeIfCloseable(getMetadata());
        Misc.free(masterFactory);
        Misc.free(slaveFactory);
        Misc.free(masterSinkTarget);
        Misc.free(slaveSinkTarget);
    }

    // todo: generalize this to support all types
    private static class OffheapSink implements RecordSinkSPI, QuietCloseable {
        private static final int INITIAL_CAPACITY_BYTES = 8;
        private static final long MAX_HEAP_SIZE = 4096;
        private long appendAddress;
        private long heapLimit;
        private long heapStart;


        private OffheapSink() {
            this.appendAddress = Unsafe.malloc(INITIAL_CAPACITY_BYTES, MemoryTag.NATIVE_JOIN_MAP); // todo: is this the right tag?
            this.heapStart = appendAddress;
            this.heapLimit = appendAddress + INITIAL_CAPACITY_BYTES;
        }

        @Override
        public void close() {
            if (appendAddress != 0) {
                Unsafe.free(heapStart, heapLimit - heapStart, MemoryTag.NATIVE_JOIN_MAP);
                appendAddress = 0;
                heapStart = 0;
            }
        }

        @Override
        public void putBin(BinarySequence value) {
            if (value == null) {
                putVarSizeNull();
            } else {
                long len = value.length() + 4L;
                if (len > Integer.MAX_VALUE) {
                    throw CairoException.nonCritical().put("binary column is too large");
                }

                checkCapacity((int) len);
                int l = (int) (len - Integer.BYTES);
                Unsafe.getUnsafe().putInt(appendAddress, l);
                value.copyTo(appendAddress + Integer.BYTES, 0, l);
                appendAddress += len;
            }
        }

        @Override
        public void putBool(boolean value) {
            checkCapacity(1);
            Unsafe.getUnsafe().putBoolean(null, appendAddress, value);
            appendAddress += 1;
        }

        @Override
        public void putByte(byte value) {
            checkCapacity(1);
            Unsafe.getUnsafe().putByte(appendAddress, value);
            appendAddress += 1;
        }

        @Override
        public void putChar(char value) {
            checkCapacity(2);
            Unsafe.getUnsafe().putChar(appendAddress, value);
            appendAddress += 2;
        }

        @Override
        public void putDate(long value) {
            checkCapacity(8);
            Unsafe.getUnsafe().putLong(appendAddress, value);
            appendAddress += 8;
        }

        @Override
        public void putDouble(double value) {
            checkCapacity(8);
            Unsafe.getUnsafe().putDouble(appendAddress, value);
            appendAddress += 8;
        }

        @Override
        public void putFloat(float value) {
            checkCapacity(4);
            Unsafe.getUnsafe().putFloat(appendAddress, value);
            appendAddress += 4;
        }

        @Override
        public void putIPv4(int value) {
            checkCapacity(4);
            Unsafe.getUnsafe().putInt(appendAddress, value);
            appendAddress += 4;
        }

        @Override
        public void putInt(int value) {
            checkCapacity(4);
            Unsafe.getUnsafe().putInt(appendAddress, value);
            appendAddress += 4;
        }

        @Override
        public void putLong(long value) {
            checkCapacity(8);
            Unsafe.getUnsafe().putLong(appendAddress, value);
            appendAddress += 8;
        }

        @Override
        public void putLong128(long lo, long hi) {
            checkCapacity(16);
            Unsafe.getUnsafe().putLong(appendAddress, lo);
            Unsafe.getUnsafe().putLong(appendAddress + 8, hi);
            appendAddress += 16;
        }

        @Override
        public void putLong256(Long256 value) {
            checkCapacity(32);
            Unsafe.getUnsafe().putLong(appendAddress, value.getLong0());
            Unsafe.getUnsafe().putLong(appendAddress + 8, value.getLong1());
            Unsafe.getUnsafe().putLong(appendAddress + 16, value.getLong2());
            Unsafe.getUnsafe().putLong(appendAddress + 24, value.getLong3());
            appendAddress += 32;
        }

        @Override
        public void putLong256(long l0, long l1, long l2, long l3) {
            checkCapacity(32);
            Unsafe.getUnsafe().putLong(appendAddress, l0);
            Unsafe.getUnsafe().putLong(appendAddress + 8, l1);
            Unsafe.getUnsafe().putLong(appendAddress + 16, l2);
            Unsafe.getUnsafe().putLong(appendAddress + 24, l3);
            appendAddress += 32;
        }

        @Override
        public void putRecord(Record value) {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public void putShort(short value) {
            checkCapacity(2);
            Unsafe.getUnsafe().putShort(appendAddress, value);
            appendAddress += 2;
        }

        @Override
        public void putStr(CharSequence value) {
            if (value == null) {
                putVarSizeNull();
                return;
            }

            int len = value.length();
            checkCapacity(((long) len << 1) + 4L);
            Unsafe.getUnsafe().putInt(appendAddress, len);
            appendAddress += 4L;
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putChar(appendAddress + ((long) i << 1), value.charAt(i));
            }
            appendAddress += (long) len << 1;
        }

        @Override
        public void putStr(CharSequence value, int lo, int hi) {
            int len = hi - lo;
            checkCapacity(((long) len << 1) + 4L);
            Unsafe.getUnsafe().putInt(appendAddress, len);
            appendAddress += 4L;
            for (int i = lo; i < hi; i++) {
                Unsafe.getUnsafe().putChar(appendAddress + ((long) (i - lo) << 1), value.charAt(i));
            }
            appendAddress += (long) len << 1;
        }

        @Override
        public void putTimestamp(long value) {
            checkCapacity(8);
            Unsafe.getUnsafe().putLong(appendAddress, value);
            appendAddress += 8;
        }

        @Override
        public void putVarchar(Utf8Sequence value) {
            int byteCount = VarcharTypeDriver.getSingleMemValueByteCount(value);
            checkCapacity(byteCount);
            VarcharTypeDriver.appendPlainValue(appendAddress, value, false);
            appendAddress += byteCount;
        }

        public void reset() {
            appendAddress = heapStart;
        }

        @Override
        public void skip(int bytes) {
            checkCapacity(bytes);
            appendAddress += bytes;
        }

        public boolean storesSameDataAs(OffheapSink other) {
            long thisSize = appendAddress - heapStart;
            long otherSize = other.appendAddress - other.heapStart;
            if (thisSize != otherSize) {
                return false;
            }
            return Vect.memeq(heapStart, other.heapStart, thisSize);
        }

        private void putVarSizeNull() {
            checkCapacity(4L);
            Unsafe.getUnsafe().putInt(appendAddress, TableUtils.NULL_LEN);
            appendAddress += 4L;
        }

        // Returns delta between new and old heapStart addresses.
        private long resize(long entrySize, long appendAddress) {
            assert appendAddress >= heapStart;
            long currentCapacity = heapLimit - heapStart;
            long newCapacity = currentCapacity << 1;
            long target = appendAddress + entrySize - heapStart;
            if (newCapacity < target) {
                newCapacity = Numbers.ceilPow2(target);
            }
            if (newCapacity > MAX_HEAP_SIZE) {
                throw LimitOverflowException.instance().put("limit of ").put(MAX_HEAP_SIZE).put(" memory exceeded in ASOF join");
            }

            long kAddress = Unsafe.realloc(heapStart, currentCapacity, newCapacity, MemoryTag.NATIVE_JOIN_MAP);

            long delta = kAddress - heapStart;
            this.heapStart = kAddress;
            this.heapLimit = kAddress + newCapacity;

            return delta;
        }

        protected void checkCapacity(long requiredSize) {
            if (appendAddress + requiredSize > heapLimit) {
                long delta = resize(requiredSize, appendAddress);
                heapStart += delta;
                appendAddress += delta;
                assert heapStart > 0;
                assert appendAddress > 0;
            }
        }
    }

    private class AsOfJoinKeyedFastRecordCursor extends AbstractAsOfJoinFastRecordCursor {
        private int origFrameIndex = -1;
        private long origRowId = -1;

        public AsOfJoinKeyedFastRecordCursor(
                int columnSplit,
                Record nullRecord,
                int masterTimestampIndex,
                int slaveTimestampIndex,
                int lookahead
        ) {
            super(columnSplit, nullRecord, masterTimestampIndex, slaveTimestampIndex, lookahead);
        }

        @Override
        public boolean hasNext() {
            if (isMasterHasNextPending) {
                masterHasNext = masterCursor.hasNext();
                isMasterHasNextPending = false;
            }
            if (!masterHasNext) {
                return false;
            }

            if (origRowId != -1) {
                slaveCursor.recordAt(slaveRecB, Rows.toRowID(origFrameIndex, origRowId));
            }
            final long masterTimestamp = masterRecord.getTimestamp(masterTimestampIndex);
            if (masterTimestamp >= lookaheadTimestamp) {
                nextSlave(masterTimestamp);
            }
            isMasterHasNextPending = true;
            boolean hasSlave = record.hasSlave();
            if (!hasSlave) {
                return true;
            }

            // ok, the non-keyed matcher found a record with matching timestamps.
            // we have to make sure the JOIN keys match as well.
            masterSinkTarget.reset();
            masterKeySink.copy(masterRecord, masterSinkTarget);
            TimeFrame timeFrame = slaveCursor.getTimeFrame();


            int slaveRecordIndex = ((PageFrameMemoryRecord) slaveRecB).getFrameIndex();
            origFrameIndex = slaveRecordIndex;
            int cursorPrevCounter = 0;
            if (timeFrame.getIndex() != slaveRecordIndex) {
                while (timeFrame.getIndex() < slaveRecordIndex) {
                    slaveCursor.next();
                    cursorPrevCounter--;
                }
                while (timeFrame.getIndex() > slaveRecordIndex) {
                    slaveCursor.prev();
                    cursorPrevCounter++;
                }
                slaveCursor.open();
            }

            slaveCursor.open();
            assert timeFrame.isOpen();
            long rowHi = timeFrame.getRowHi();
            long rowLo = timeFrame.getRowLo();

//            assert slaveFrameRow >= rowLo && slaveFrameRow < rowHi;

            long keyedRowId = ((PageFrameMemoryRecord) slaveRecB).getRowIndex();
            origRowId = keyedRowId;
            int keyedFrameIndex = timeFrame.getIndex();
            for (; ; ) {
                slaveSinkTarget.reset();
                slaveKeySink.copy(slaveRecB, slaveSinkTarget);
                if (masterSinkTarget.storesSameDataAs(slaveSinkTarget)) {
                    // we have a match, that's awesome, no need to traverse the slave cursor!
                    break;
                }

                // let's try to move backwards in the slave cursor until we have a match
                keyedRowId--;
                if (keyedRowId < rowLo) {
                    // ops, we exhausted this frame, let's try the previous one
                    cursorPrevCounter++;
                    if (!slaveCursor.prev()) {
                        // there is no previous frame, we are done, no match :(
                        // if we are here, chances are we are also pretty slow because we are scanning the entire slave cursor!
                        record.hasSlave(false);
                        break;
                    }
                    slaveCursor.open();

                    keyedFrameIndex = timeFrame.getIndex();
                    keyedRowId = timeFrame.getRowHi() - 1; // should it be -1? I never know. inclusive, exclusive, it's all a blur. I assume this one is exclusive. to be checked.
                    rowLo = timeFrame.getRowLo();
                }
                slaveCursor.recordAt(slaveRecB, Rows.toRowID(keyedFrameIndex, keyedRowId));
            }

            // rewind the slave cursor to the original position
            if (cursorPrevCounter > 0) {
                for (int i = 0; i < cursorPrevCounter; i++) {
                    slaveCursor.next();
                }
            } else if (cursorPrevCounter < 0) {
                for (int i = 0; i < -cursorPrevCounter; i++) {
                    slaveCursor.prev();
                }
            }
            assert slaveFrameIndex == timeFrame.getIndex();
            slaveCursor.open();
            return true;
        }

        @Override
        public void toTop() {
            super.toTop();
            origFrameIndex = -1;
            origRowId = -1;
        }
    }
}

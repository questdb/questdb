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

package io.questdb.griffin.engine.functions.window;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

public class CountFunctionFactoryHelper {
    public static final ArrayColumnTypes COUNT_COLUMN_TYPES;
    public static final ArrayColumnTypes COUNT_OVER_PARTITION_RANGE_COLUMN_TYPES;
    public static final ArrayColumnTypes COUNT_OVER_PARTITION_ROWS_COLUMN_TYPES;
    static final String COUNT_NAME = "count";

    static Function newCountWindowFunction(AbstractWindowFunctionFactory factory,
                                           int position,
                                           ObjList<Function> args,
                                           CairoConfiguration configuration,
                                           SqlExecutionContext sqlExecutionContext,
                                           IsRecordNotNull isRecordNotNull) throws SqlException {
        WindowContext windowContext = sqlExecutionContext.getWindowContext();
        windowContext.validate(position, factory.supportNullsDesc());
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        if (rowsHi < rowsLo) {
            return new AbstractWindowFunctionFactory.LongNullFunction(args.get(0),
                    CountFunctionFactoryHelper.COUNT_NAME,
                    rowsLo,
                    rowsHi,
                    framingMode == WindowExpression.FRAMING_RANGE,
                    partitionByRecord,
                    0);
        }

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            CountFunctionFactoryHelper.COUNT_COLUMN_TYPES
                    );

                    return new CountOverPartitionFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            isRecordNotNull
                    );
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            CountFunctionFactoryHelper.COUNT_COLUMN_TYPES
                    );

                    // same as for rows because calculation stops at current rows even if there are 'equal' following rows
                    return new CountOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            isRecordNotNull
                    );
                } // range between [unbounded | x] preceding and [x preceding | current row], except unbounded preceding to current row
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }

                    int timestampIndex = windowContext.getTimestampIndex();

                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                CountFunctionFactoryHelper.COUNT_OVER_PARTITION_RANGE_COLUMN_TYPES
                        );
                        mem = Vm.getCARWInstance(
                                configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(),
                                MemoryTag.NATIVE_CIRCULAR_BUFFER
                        );

                        // moving count over range between timestamp - rowsLo and timestamp + rowsHi (inclusive)
                        return new CountOverPartitionRangeFrameFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                rowsLo,
                                rowsHi,
                                mem,
                                configuration.getSqlWindowInitialRangeBufferSize(),
                                timestampIndex,
                                args.get(0),
                                isRecordNotNull
                        );
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                // between unbounded preceding and current row
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            CountFunctionFactoryHelper.COUNT_COLUMN_TYPES
                    );

                    return new CountOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            isRecordNotNull
                    );
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new CountOverCurrentRowFunction(args.get(0), isRecordNotNull);
                } // whole partition
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            CountFunctionFactoryHelper.COUNT_COLUMN_TYPES
                    );

                    return new CountOverPartitionFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            isRecordNotNull
                    );
                }
                //between [unbounded | x] preceding and [x preceding | current row]
                else {
                    Map map = null;
                    try {
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                CountFunctionFactoryHelper.COUNT_OVER_PARTITION_ROWS_COLUMN_TYPES
                        );
                        MemoryARW mem = Vm.getCARWInstance(
                                configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(),
                                MemoryTag.NATIVE_CIRCULAR_BUFFER
                        );

                        return new CountOverPartitionRowsFrameFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                rowsLo,
                                rowsHi,
                                args.get(0),
                                mem,
                                isRecordNotNull
                        );
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                }
            }
        } else { // no partition key
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                // if there's no order by then all elements are equal in range mode, thus calculation is done on whole result set
                if (!windowContext.isOrdered() && windowContext.isDefaultFrame()) {
                    return new CountOverWholeResultSetFunction(args.get(0), isRecordNotNull);
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    // same as for rows because calculation stops at current rows even if there are 'equal' following rows
                    return new CountOverUnboundedRowsFrameFunction(args.get(0), isRecordNotNull);
                } // range between [unbounded | x] preceding and [x preceding | current row]
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }

                    int timestampIndex = windowContext.getTimestampIndex();

                    // moving count over range between timestamp - rowsLo and timestamp + rowsHi (inclusive)
                    return new CountOverRangeFrameFunction(
                            rowsLo,
                            rowsHi,
                            configuration,
                            timestampIndex,
                            args.get(0),
                            isRecordNotNull
                    );
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                // between unbounded preceding and current row
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new CountOverUnboundedRowsFrameFunction(args.get(0), isRecordNotNull);
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new CountOverCurrentRowFunction(args.get(0), isRecordNotNull);
                } // whole result set
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new CountOverWholeResultSetFunction(args.get(0), isRecordNotNull);
                } // between [unbounded | x] preceding and [x preceding | current row]
                else {
                    MemoryARW mem = Vm.getCARWInstance(
                            configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(),
                            MemoryTag.NATIVE_CIRCULAR_BUFFER
                    );
                    return new CountOverRowsFrameFunction(
                            args.get(0),
                            rowsLo,
                            rowsHi,
                            mem,
                            isRecordNotNull
                    );
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    @FunctionalInterface
    public interface IsRecordNotNull {
        boolean isNotNull(Function arg, Record record);
    }

    static class CountOverCurrentRowFunction extends BaseWindowFunction implements WindowLongFunction {

        private static final long VALUE_ONE = 1L;
        private static final long VALUE_ZERO = 0L;
        private final IsRecordNotNull isNotNullFunc;

        CountOverCurrentRowFunction(Function arg, IsRecordNotNull isNotNullFunc) {
            super(arg);
            this.isNotNullFunc = isNotNullFunc;
        }

        @Override
        public long getLong(Record rec) {
            return isNotNullFunc.isNotNull(arg, rec) ? VALUE_ONE : VALUE_ZERO;
        }

        @Override
        public String getName() {
            return CountFunctionFactoryHelper.COUNT_NAME;
        }

        @Override
        public int getPassCount() {
            return ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), isNotNullFunc.isNotNull(arg, record) ? VALUE_ONE : VALUE_ZERO);
        }
    }

    // handles count(arg) over (partition by x)
    // order by is absent so default frame mode includes all rows in partition
    static class CountOverPartitionFunction extends BasePartitionedWindowFunction implements WindowLongFunction {

        private final IsRecordNotNull isNotNullFunc;

        public CountOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, IsRecordNotNull isNotNullFunc) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.isNotNullFunc = isNotNullFunc;
        }

        @Override
        public String getName() {
            return CountFunctionFactoryHelper.COUNT_NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }


        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            long count = 0;
            if (!value.isNew()) {
                count = value.getLong(0);
            }

            if (isNotNullFunc.isNotNull(arg, record)) {
                count++;
            }

            value.putLong(0, count);
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            long val = value != null ? value.getLong(0) : 0;
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), val);
        }
    }

    // Handles count(arg) over (partition by x order by ts range between [unbounded | y] preceding and [z preceding | current row])
    public static class CountOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction implements WindowLongFunction {

        private static final int RECORD_SIZE = Long.BYTES;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final LongList freeList = new LongList();
        private final int initialBufferSize;
        private final IsRecordNotNull isNotNullFunc;
        private final long maxDiff;
        private final MemoryARW memory;
        private final AbstractWindowFunctionFactory.RingBufferDesc memoryDesc = new AbstractWindowFunctionFactory.RingBufferDesc();
        private final long minDiff;
        private final int timestampIndex;
        private long count;

        public CountOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                Function arg,
                IsRecordNotNull isNotNullFunc
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            frameIncludesCurrentValue = rangeHi == 0;
            this.isNotNullFunc = isNotNullFunc;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
            freeList.clear();
        }

        @Override
        public void computeNext(Record record) {
            // map stores
            // 0 - current counter number
            // 1 - native array start offset (relative to memory address)
            // 2 - size of ring buffer (number of ts stored in it; not all of them need to belong to frame)
            // 3 - capacity of ring buffer
            // 4 - index of first (the oldest) valid buffer element
            // actual frame data [ts] stored in mem at [ offset + first_idx*8, offset + last_idx*8]
            // if frameLoBounded == false ring buffer store only suffix of the window with ts > current_ts + rangeHi (rangeHi is negative),
            // because all ts on remained prefix will always be accumulated, and we don't need to store them in the buffer
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();
            long frameSize;
            long startOffset;
            long size;
            long capacity;
            long firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;

                if (isNotNullFunc.isNotNull(arg, record)) {
                    memory.putLong(startOffset, timestamp);
                    if (frameIncludesCurrentValue) {
                        frameSize = 1;
                        size = frameLoBounded ? 1 : 0;
                    } else {
                        frameSize = 0;
                        size = 1;
                    }
                } else {
                    size = 0;
                    frameSize = 0;
                }
            } else {
                frameSize = mapValue.getLong(0);
                startOffset = mapValue.getLong(1);
                size = mapValue.getLong(2);
                capacity = mapValue.getLong(3);
                firstIdx = mapValue.getLong(4);
                long newFirstIdx = firstIdx;

                if (frameLoBounded) {
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
                            // if rangeHi < 0, some elements from the window can be not in the frame
                            if (frameSize > 0) {
                                frameSize--;
                            }
                            newFirstIdx = (idx + 1) % capacity;
                            size--;
                        } else {
                            break;
                        }
                    }
                }
                firstIdx = newFirstIdx;

                if (isNotNullFunc.isNotNull(arg, record)) {
                    if (size == capacity) { // buffer full
                        memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                        AbstractWindowFunctionFactory.expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                        capacity = memoryDesc.capacity;
                        startOffset = memoryDesc.startOffset;
                        firstIdx = memoryDesc.firstIdx;
                    }

                    // add ts element to buffer
                    memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                    size++;
                }

                // find new top border of range frame and add new elements
                if (frameLoBounded) {
                    for (long i = frameSize; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        long diff = Math.abs(ts - timestamp);

                        if (diff <= maxDiff && diff >= minDiff) {
                            frameSize++;
                        } else {
                            break;
                        }
                    }
                } else {
                    newFirstIdx = firstIdx;
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            frameSize++;
                            newFirstIdx = (idx + 1) % capacity;
                            size--;
                        } else {
                            break;
                        }
                    }

                    firstIdx = newFirstIdx;
                }
            }

            this.count = frameSize;
            mapValue.putLong(0, frameSize);
            mapValue.putLong(1, startOffset);
            mapValue.putLong(2, size);
            mapValue.putLong(3, capacity);
            mapValue.putLong(4, firstIdx);
        }

        @Override
        public long getLong(Record rec) {
            return count;
        }

        @Override
        public String getName() {
            return CountFunctionFactoryHelper.COUNT_NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }


        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), count);
        }

        @Override
        public void reopen() {
            super.reopen();
            // memory will allocate on first use
            this.count = 0;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
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
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" range between ");
            if (frameLoBounded) {
                sink.val(maxDiff);
            } else {
                sink.val("unbounded");
            }
            sink.val(" preceding and ");
            if (minDiff == 0) {
                sink.val("current row");
            } else {
                sink.val(minDiff).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            freeList.clear();
        }
    }

    // handles count(arg) over (partition by x [order by o] rows between y and z)
    public static class CountOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowLongFunction {

        //number of values we need to keep to compute over frame
        // (can be bigger than frame because we've to buffer values between rowsHi and current row )
        private final int bufferSize;

        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final IsRecordNotNull isRecordNotNull;
        // holds fixed-size ring buffers of boolean values
        private final MemoryARW memory;
        protected long count;

        public CountOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                IsRecordNotNull isRecordNotNull
        ) {
            super(map, partitionByRecord, partitionBySink, arg);

            if (rowsLo > Long.MIN_VALUE) {
                frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
                bufferSize = (int) Math.abs(rowsLo);
                frameLoBounded = true;
            } else {
                frameSize = 1;
                bufferSize = (int) Math.abs(rowsHi);
                frameLoBounded = false;
            }
            frameIncludesCurrentValue = rowsHi == 0;

            this.memory = memory;
            this.isRecordNotNull = isRecordNotNull;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            // map stores:
            // 0 - count, current number of non-null rows in frame
            // 1 - (0-based) index of oldest value [0, bufferSize]
            // 2 - native array start offset (relative to memory address)
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            long count = 0;
            long loIdx;//current index of lo frame value ('oldest')
            long startOffset;
            boolean isNotNull = isRecordNotNull.isNotNull(arg, record);

            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor(bufferSize) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && isNotNull) {
                    count = 1;
                }

                for (int i = 0; i < bufferSize; i++) {
                    memory.putBool(startOffset + i, false);
                }
                this.count = count;
            } else {
                count = value.getLong(0);
                loIdx = value.getLong(1);
                startOffset = value.getLong(2);
                if (frameIncludesCurrentValue ? isNotNull : memory.getBool(startOffset + ((loIdx + frameSize - 1) % bufferSize))) {
                    count++;
                }
                this.count = count;

                if (frameLoBounded) {
                    //remove the oldest element
                    if (memory.getBool(startOffset + loIdx)) {
                        count--;
                    }
                }
            }

            value.putLong(0, count);
            value.putLong(1, (loIdx + 1) % bufferSize);
            value.putLong(2, startOffset);//not necessary because it doesn't change
            memory.putBool(startOffset + loIdx, isNotNull);
        }

        @Override
        public long getLong(Record rec) {
            return count;
        }

        @Override
        public String getName() {
            return CountFunctionFactoryHelper.COUNT_NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }


        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), count);
        }

        @Override
        public void reopen() {
            super.reopen();
            // memory will allocate on first use
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            if (arg != null) {
                sink.val('(').val(arg).val(')');
            } else {
                sink.val("*");
            }
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());

            sink.val(" rows between ");
            if (frameLoBounded) {
                sink.val(bufferSize);
            } else {
                sink.val("unbounded");
            }
            sink.val(" preceding and ");
            if (frameIncludesCurrentValue) {
                sink.val("current row");
            } else {
                sink.val(bufferSize - frameSize).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
        }
    }

    // Handles count(arg) over ([order by ts] range between [unbounded | x] preceding and [ x preceding | current row ] ); no partition by key
    public static class CountOverRangeFrameFunction extends BaseWindowFunction implements Reopenable, WindowLongFunction {
        private static final int RECORD_SIZE = Long.BYTES;
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final IsRecordNotNull isRecordNotNull;
        private final long maxDiff;
        // holds resizable ring buffers
        // actual frame data - [timestamp] - is stored in mem at [ offset + first_idx*8, offset + last_idx*8]
        private final MemoryARW memory;
        private final long minDiff;
        private final int timestampIndex;
        private long capacity;
        private long count;
        private long firstIdx;
        private long size;
        private long startOffset;

        public CountOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                CairoConfiguration configuration,
                int timestampIdx,
                Function arg,
                IsRecordNotNull isRecordNotNull
        ) {
            super(arg);
            this.initialCapacity = configuration.getSqlWindowStorePageSize() / RECORD_SIZE;
            this.memory = Vm.getCARWInstance(
                    configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowStoreMaxPages(),
                    MemoryTag.NATIVE_CIRCULAR_BUFFER
            );
            this.isRecordNotNull = isRecordNotNull;
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE; // maxDiff must be used only if frameLoBounded
            minDiff = Math.abs(rangeHi);
            timestampIndex = timestampIdx;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            count = 0;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            long timestamp = record.getTimestamp(timestampIndex);
            boolean isNotNull = isRecordNotNull.isNotNull(arg, record);
            long newFirstIdx = firstIdx;
            if (frameLoBounded) {
                // find new bottom border of range frame and remove unneeded elements
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        // if rangeHi < 0, some elements from the window can be not in the frame
                        if (count > 0) {
                            count--;
                        }
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        break;
                    }
                }
            }
            firstIdx = newFirstIdx;

            if (isNotNull) {
                if (size == capacity) { //buffer full
                    long newAddress = memory.appendAddressFor((capacity << 1) * RECORD_SIZE);
                    // call above can end up resizing and thus changing memory start address
                    long oldAddress = memory.getPageAddress(0) + startOffset;

                    if (firstIdx == 0) {
                        Vect.memcpy(newAddress, oldAddress, size * RECORD_SIZE);
                    } else {
                        firstIdx %= size;
                        //we can't simply copy because that'd leave a gap in the middle
                        long firstPieceSize = (size - firstIdx) * RECORD_SIZE;
                        Vect.memcpy(newAddress, oldAddress + firstIdx * RECORD_SIZE, firstPieceSize);
                        Vect.memcpy(newAddress + firstPieceSize, oldAddress, firstIdx * RECORD_SIZE);
                        firstIdx = 0;
                    }

                    startOffset = newAddress - memory.getPageAddress(0);
                    capacity <<= 1;
                }

                // add element to buffer
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                size++;
            }

            // find new top border of range frame and add new elements
            if (frameLoBounded) {
                for (long i = count, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);

                    if (diff <= maxDiff && diff >= minDiff) {
                        count++;
                    } else {
                        break;
                    }
                }
            } else {
                newFirstIdx = firstIdx;
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        count++;
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        break;
                    }
                }
                firstIdx = newFirstIdx;
            }
        }

        @Override
        public long getLong(Record rec) {
            return count;
        }

        @Override
        public String getName() {
            return CountFunctionFactoryHelper.COUNT_NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }


        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), count);
        }

        @Override
        public void reopen() {
            count = 0;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            size = 0;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(')');
            sink.val(" over (");
            sink.val("range between ");
            if (frameLoBounded) {
                sink.val(maxDiff);
            } else {
                sink.val("unbounded");
            }
            sink.val(" preceding and ");
            if (minDiff == 0) {
                sink.val("current row");
            } else {
                sink.val(minDiff).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            count = 0;
            size = 0;
        }
    }

    // Handles count(arg) over ([order by o] rows between y and z); there's no partition by.
    public static class CountOverRowsFrameFunction extends BaseWindowFunction implements Reopenable, WindowLongFunction {
        private final MemoryARW buffer;
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final IsRecordNotNull isRecordNotNull;
        private long count = 0;
        private long lastcount = 0;
        private int loIdx = 0;

        public CountOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, IsRecordNotNull isRecordNotNull) {
            super(arg);
            assert rowsLo != Long.MIN_VALUE || rowsHi != 0; // use CountOverUnboundedRowsFrameFunction in case of (Long.MIN_VALUE, 0) range
            if (rowsLo > Long.MIN_VALUE) {
                frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
                bufferSize = (int) Math.abs(rowsLo);//number of values we need to keep to compute over frame
                frameLoBounded = true;
            } else {
                frameSize = (int) Math.abs(rowsHi);
                bufferSize = frameSize;
                frameLoBounded = false;
            }

            frameIncludesCurrentValue = rowsHi == 0;
            this.buffer = memory;
            this.isRecordNotNull = isRecordNotNull;
            try {
                initBuffer();
            } catch (Throwable t) {
                close();
                throw t;
            }

        }

        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        @Override
        public void computeNext(Record record) {
            boolean originIsNotNull = isRecordNotNull.isNotNull(arg, record);
            boolean isNotNull = originIsNotNull;

            if (frameLoBounded && !frameIncludesCurrentValue) {
                isNotNull = buffer.getBool((loIdx + frameSize - 1) % bufferSize);
            } else if (!frameLoBounded && !frameIncludesCurrentValue) {
                isNotNull = buffer.getBool(loIdx % bufferSize);
            }
            if (isNotNull) {
                lastcount++;
            }
            this.count = lastcount;

            if (frameLoBounded) {
                //remove the oldest element with newest
                if (buffer.getBool(loIdx % bufferSize)) {
                    lastcount--;
                }
            }

            //overwrite oldest element
            buffer.putBool(loIdx, originIsNotNull);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public long getLong(Record rec) {
            return count;
        }

        @Override
        public String getName() {
            return CountFunctionFactoryHelper.COUNT_NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }


        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), count);
        }

        @Override
        public void reopen() {
            count = 0;
            lastcount = 0;
            loIdx = 0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            count = 0;
            lastcount = 0;
            loIdx = 0;
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
            sink.val(" rows between ");
            if (frameLoBounded) {
                sink.val(bufferSize);
            } else {
                sink.val("unbounded");
            }
            sink.val(" preceding and ");
            if (frameIncludesCurrentValue) {
                sink.val("current row");
            } else {
                sink.val(bufferSize - frameSize).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            count = 0;
            loIdx = 0;
            lastcount = 0;
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putBool(i, false);
            }
        }
    }

    // count(arg) over (partition by x order by ts range between unbounded preceding and current row)
// Doesn't require ts buffering.
    static class CountOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowLongFunction {
        private final IsRecordNotNull isRecordNotNull;
        private long count;

        public CountOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, IsRecordNotNull isRecordNotNull) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.isRecordNotNull = isRecordNotNull;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            long count = 0;
            if (!value.isNew()) {
                count = value.getLong(0);
            }
            if (isRecordNotNull.isNotNull(arg, record)) {
                count++;
            }
            this.count = count;
            value.putLong(0, count);
        }

        @Override
        public long getLong(Record rec) {
            return count;
        }

        @Override
        public String getName() {
            return CountFunctionFactoryHelper.COUNT_NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }


        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), count);
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
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row)");
        }
    }

    // Handles count(arg) over (rows between unbounded preceding and current row); there's no partition by.
    static public class CountOverUnboundedRowsFrameFunction extends BaseWindowFunction implements WindowLongFunction {

        private final IsRecordNotNull isRecordNotNull;
        private long count = 0;

        public CountOverUnboundedRowsFrameFunction(Function arg, IsRecordNotNull isRecordNotNull) {
            super(arg);
            this.isRecordNotNull = isRecordNotNull;
        }

        @Override
        public void computeNext(Record record) {
            if (isRecordNotNull.isNotNull(arg, record)) {
                count++;
            }
        }

        @Override
        public long getLong(Record rec) {
            return count;
        }

        @Override
        public String getName() {
            return CountFunctionFactoryHelper.COUNT_NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }


        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), count);
        }

        @Override
        public void reset() {
            super.reset();
            count = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            if (arg != null) {
                sink.val('(').val(arg).val(')');
            } else {
                sink.val("(*)");
            }
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            count = 0;
        }
    }

    // count(arg) over () - empty clause, no partition by no order by, no frame == default frame
    static class CountOverWholeResultSetFunction extends BaseWindowFunction implements WindowLongFunction {
        private final IsRecordNotNull isRecordNotNull;
        private long count;

        public CountOverWholeResultSetFunction(Function arg, IsRecordNotNull isRecordNotNull) {
            super(arg);
            this.isRecordNotNull = isRecordNotNull;
        }

        @Override
        public String getName() {
            return CountFunctionFactoryHelper.COUNT_NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }


        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (isRecordNotNull.isNotNull(arg, record)) {
                count++;
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), count);
        }

        @Override
        public void reset() {
            super.reset();
            count = 0;
        }

        @Override
        public void toTop() {
            super.toTop();
            count = 0;
        }
    }

    static {
        COUNT_COLUMN_TYPES = new ArrayColumnTypes();
        COUNT_COLUMN_TYPES.add(ColumnType.LONG);

        COUNT_OVER_PARTITION_RANGE_COLUMN_TYPES = new ArrayColumnTypes();
        COUNT_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG);  // current frame count
        COUNT_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG);  // native array start offset, requires updating on resize
        COUNT_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG);  // native buffer size
        COUNT_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG);  // native buffer capacity
        COUNT_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG);  // index of first buffered element

        COUNT_OVER_PARTITION_ROWS_COLUMN_TYPES = new ArrayColumnTypes();
        COUNT_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG);  // count
        COUNT_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG);  // position of current oldest element
        COUNT_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG);  // start offset of native array
    }
}

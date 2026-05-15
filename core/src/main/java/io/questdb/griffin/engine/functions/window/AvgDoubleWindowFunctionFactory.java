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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.lv.LiveViewSnapshotKeyCodec;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

public class AvgDoubleWindowFunctionFactory extends AbstractWindowFunctionFactory {

    public static final ArrayColumnTypes AVG_COLUMN_TYPES;
    public static final ArrayColumnTypes AVG_OVER_PARTITION_RANGE_COLUMN_TYPES;
    public static final ArrayColumnTypes AVG_OVER_PARTITION_ROWS_COLUMN_TYPES;
    public static final ArrayColumnTypes AVG_OVER_PARTITION_ROWS_COLUMN_TYPES_LV;

    private static final String NAME = "avg";
    private static final String SIGNATURE = NAME + "(D)";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        WindowContext windowContext = sqlExecutionContext.getWindowContext();
        windowContext.validate(position, supportNullsDesc());
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();

        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        if (rowsHi < rowsLo) {
            return new DoubleNullFunction(args.get(0),
                    NAME,
                    rowsLo,
                    rowsHi,
                    framingMode == WindowExpression.FRAMING_RANGE,
                    partitionByRecord);
        }

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                // moving average over whole partition (no order by, default frame) or (order by, unbounded preceding to unbounded following)
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            AVG_COLUMN_TYPES
                    );

                    return new AvgOverPartitionFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0)
                    );
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            AVG_COLUMN_TYPES
                    );

                    // same as for rows because calculation stops at current rows even if there are 'equal' following rows
                    return new AvgOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            partitionByKeyTypes
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
                                AVG_OVER_PARTITION_RANGE_COLUMN_TYPES
                        );
                        mem = Vm.getCARWInstance(
                                configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(),
                                MemoryTag.NATIVE_CIRCULAR_BUFFER
                        );

                        // moving average over range between timestamp - rowsLo and timestamp + rowsHi (inclusive)
                        return new AvgOverPartitionRangeFrameFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                rowsLo,
                                rowsHi,
                                args.get(0),
                                mem,
                                configuration.getSqlWindowInitialRangeBufferSize(),
                                timestampIndex
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
                            AVG_COLUMN_TYPES
                    );

                    return new AvgOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            partitionByKeyTypes
                    );
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new AvgOverCurrentRowFunction(args.get(0));
                } // whole partition
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            AVG_COLUMN_TYPES
                    );

                    return new AvgOverPartitionFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0)
                    );
                }
                //between [unbounded | x] preceding and [x preceding | current row]
                else {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                liveView ? AVG_OVER_PARTITION_ROWS_COLUMN_TYPES_LV
                                        : AVG_OVER_PARTITION_ROWS_COLUMN_TYPES
                        );
                        mem = Vm.getCARWInstance(
                                configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(),
                                MemoryTag.NATIVE_CIRCULAR_BUFFER
                        );

                        // moving average over preceding N rows
                        return new AvgOverPartitionRowsFrameFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                rowsLo,
                                rowsHi,
                                args.get(0),
                                mem,
                                partitionByKeyTypes,
                                liveView,
                                configuration
                        );
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            }
        } else { // no partition key
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                // if there's no order by then all elements are equal in range mode, thus calculation is done on whole result set
                if (!windowContext.isOrdered() && windowContext.isDefaultFrame()) {
                    return new AvgOverWholeResultSetFunction(args.get(0));
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    // same as for rows because calculation stops at current rows even if there are 'equal' following rows
                    return new AvgOverUnboundedRowsFrameFunction(args.get(0));
                } // range between [unbounded | x] preceding and [x preceding | current row]
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }

                    int timestampIndex = windowContext.getTimestampIndex();

                    // moving average over range between timestamp - rowsLo and timestamp + rowsHi (inclusive)
                    return new AvgOverRangeFrameFunction(
                            rowsLo,
                            rowsHi,
                            args.get(0),
                            configuration,
                            timestampIndex
                    );
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                // between unbounded preceding and current row
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new AvgOverUnboundedRowsFrameFunction(args.get(0));
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new AvgOverCurrentRowFunction(args.get(0));
                } // whole result set
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new AvgOverWholeResultSetFunction(args.get(0));
                } // between [unbounded | x] preceding and [x preceding | current row]
                else {
                    MemoryARW mem = Vm.getCARWInstance(
                            configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(),
                            MemoryTag.NATIVE_CIRCULAR_BUFFER
                    );
                    return new AvgOverRowsFrameFunction(
                            args.get(0),
                            rowsLo,
                            rowsHi,
                            mem
                    );
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    // (rows between current row and current row) processes 1-element-big set, so simply it returns expression value
    static class AvgOverCurrentRowFunction extends BaseWindowFunction implements WindowDoubleFunction {

        private double value;

        AvgOverCurrentRowFunction(Function arg) {
            super(arg);
        }

        @Override
        public void computeNext(Record record) {
            value = arg.getDouble(record);
        }

        @Override
        public double getDouble(Record rec) {
            return value;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    // handles avg() over (partition by x)
    // order by is absent so default frame mode includes all rows in partition
    static class AvgOverPartitionFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        public AvgOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }


        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            double d = arg.getDouble(record);
            if (Numbers.isFinite(d)) {
                partitionByRecord.of(record);
                MapKey key = map.withKey();
                key.put(partitionByRecord, partitionBySink);
                MapValue value = key.createValue();

                long count;
                double sum;

                if (value.isNew()) {
                    count = 1;
                    sum = d;
                } else {
                    count = value.getLong(1) + 1;
                    sum = value.getDouble(0) + d;
                }
                value.putDouble(0, sum);
                value.putLong(1, count);
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();

            double val = value != null ? value.getDouble(0) : Double.NaN;

            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), val);
        }

        @Override
        public void preparePass2() {
            RecordCursor cursor = map.getCursor();
            MapRecord record = map.getRecord();
            while (cursor.hasNext()) {
                MapValue value = record.getValue();
                long count = value.getLong(1);
                if (count > 0) {
                    double sum = value.getDouble(0);
                    value.putDouble(0, sum / count);
                }
            }
        }

        @Override
        public void resetPartition(Record record) {
            // ANCHOR-driven reset (RFC 123). The Map value's [sum, count] slots
            // return to identity so the next row in the new bucket starts fresh.
            // findValue() returns null when the partition has no recorded state
            // yet — nothing to reset.
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            if (value != null) {
                value.putDouble(0, 0.0);
                value.putLong(1, 0L);
            }
        }
    }

    // Handles avg() over (partition by x order by ts range between [unbounded | y] preceding and [z preceding | current row])
    // Removable cumulative aggregation with timestamp & value stored in resizable ring buffers
    // When lower bound is unbounded we add but immediately discard any values that enter the frame so buffer should only contain values
    // between upper bound and current row's value.
    public static class AvgOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        private static final int RECORD_SIZE = Long.BYTES + Double.BYTES;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        // list of [size, startOffset] pairs marking free space within mem
        private final LongList freeList = new LongList();
        private final int initialBufferSize;
        private final long maxDiff;
        // holds resizable ring buffers
        private final MemoryARW memory;
        private final RingBufferDesc memoryDesc = new RingBufferDesc();
        private final long minDiff;
        private final int timestampIndex;
        protected double sum;
        private double avg;

        public AvgOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE; // maxDiff must be used only if frameLoBounded
            minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;

            frameIncludesCurrentValue = rangeHi == 0;
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
            // 0 - sum, never a NaN
            // 1 - current number of non-null rows in in-memory frame (equals to frame size because we don't store nulls)
            // 2 - native array start offset (relative to memory address)
            // 3 - size of ring buffer (number of elements stored in it; not all of them need to belong to frame)
            // 4 - capacity of ring buffer
            // 5 - index of first (the oldest) valid buffer element
            // actual frame data - [timestamp, value] pairs - is stored in mem at [ offset + first_idx*16, offset + last_idx*16]
            // note: we ignore nulls to reduce memory usage
            // if frameLoBounded == false ring buffer store only suffix of the window with ts > current_ts + rangeHi (rangeHi is negative),
            // because all values on remained prefix will always be accumulated, and we don't need to store them in the buffer

            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            double sum;
            long frameSize;
            long startOffset;
            long size;
            long capacity;
            long firstIdx;

            long timestamp = record.getTimestamp(timestampIndex);
            double d = arg.getDouble(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;

                if (Numbers.isFinite(d)) {
                    memory.putLong(startOffset, timestamp);
                    memory.putDouble(startOffset + Long.BYTES, d);

                    if (frameIncludesCurrentValue) {
                        sum = d;
                        avg = d;
                        this.sum = d;
                        frameSize = 1;
                        size = frameLoBounded ? 1 : 0;
                    } else {
                        sum = 0.0;
                        avg = Double.NaN;
                        this.sum = Double.NaN;
                        frameSize = 0;
                        size = 1;
                    }
                } else {
                    size = 0;
                    sum = 0.0;
                    avg = Double.NaN;
                    this.sum = Double.NaN;
                    frameSize = 0;
                }
            } else {
                sum = mapValue.getDouble(0);
                frameSize = mapValue.getLong(1);
                startOffset = mapValue.getLong(2);
                size = mapValue.getLong(3);
                capacity = mapValue.getLong(4);
                firstIdx = mapValue.getLong(5);

                long newFirstIdx = firstIdx;

                if (frameLoBounded) {
                    // find new bottom border of range frame and remove unneeded elements
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
                            // if rangeHi < 0, some elements from the window can be not in the frame
                            if (frameSize > 0) {
                                double val = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                                sum -= val;
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

                // add new element if not null
                if (Numbers.isFinite(d)) {
                    if (size == capacity) { //buffer full
                        memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                        expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                        capacity = memoryDesc.capacity;
                        startOffset = memoryDesc.startOffset;
                        firstIdx = memoryDesc.firstIdx;
                    }

                    // add element to buffer
                    memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                    memory.putDouble(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
                    size++;
                }

                // find new top border of range frame and add new elements
                if (frameLoBounded) {
                    for (long i = frameSize; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        long diff = Math.abs(ts - timestamp);

                        if (diff <= maxDiff && diff >= minDiff) {
                            double value = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            sum += value;
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
                            double val = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            sum += val;
                            frameSize++;
                            newFirstIdx = (idx + 1) % capacity;
                            size--;
                        } else {
                            break;
                        }
                    }

                    firstIdx = newFirstIdx;
                }

                if (frameSize != 0) {
                    avg = sum / frameSize;
                    this.sum = sum;
                } else {
                    avg = Double.NaN;
                    this.sum = Double.NaN;
                }

            }

            mapValue.putDouble(0, sum);
            mapValue.putLong(1, frameSize);
            mapValue.putLong(2, startOffset);
            mapValue.putLong(3, size);
            mapValue.putLong(4, capacity);
            mapValue.putLong(5, firstIdx);
        }

        @Override
        public double getDouble(Record rec) {
            return avg;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }


        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), avg);
        }

        @Override
        public void reopen() {
            super.reopen();
            // memory will allocate on first use
            avg = Double.NaN;
            sum = Double.NaN;
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
            sink.val('(').val(arg).val(')');
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

    // handles avg() over (partition by x [order by o] rows between y and z)
    // removable cumulative aggregation
    static class AvgOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        //number of values we need to keep to compute over frame
        // (can be bigger than frame because we've to buffer values between rowsHi and current row )
        private final int bufferSize;

        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        // holds fixed-size ring buffers of double values
        private final MemoryARW memory;
        private final CairoConfiguration configuration;
        // Deep copy of the partition-by key column types. The factory's
        // partitionByKeyTypes buffer is reused across compiles; keeping a copy
        // here outlives that lifetime for compactPartitionMap and the snapshot
        // codec.
        private final ArrayColumnTypes keyColumnTypes;
        private final boolean liveView;
        // Full value-layout (including tombstone slot) for the compactPartitionMap
        // scratch Map. Null for non-live-view compiles.
        private final ArrayColumnTypes mapValueTypes;
        // Value-slot index of the per-partition tombstone byte; -1 outside LV.
        private final int tombstoneValueIndex;
        protected double sum;
        private double avg;
        // Single-writer (refresh worker), not volatile.
        private long tombstoneCount;

        public AvgOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                ColumnTypes partitionByKeyTypes,
                boolean liveView,
                CairoConfiguration configuration
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
            this.liveView = liveView;
            this.configuration = configuration;
            if (liveView) {
                ArrayColumnTypes keyTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = partitionByKeyTypes.getColumnCount(); i < n; i++) {
                    keyTypesCopy.add(partitionByKeyTypes.getColumnType(i));
                }
                this.keyColumnTypes = keyTypesCopy;
                ArrayColumnTypes valueTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = AVG_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.getColumnCount(); i < n; i++) {
                    valueTypesCopy.add(AVG_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.getColumnType(i));
                }
                this.mapValueTypes = valueTypesCopy;
                this.tombstoneValueIndex = 4;
            } else {
                this.keyColumnTypes = null;
                this.mapValueTypes = null;
                this.tombstoneValueIndex = -1;
            }
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void compactPartitionMap() {
            if (tombstoneValueIndex < 0 || tombstoneCount == 0) {
                return;
            }
            Map scratch = MapFactory.createUnorderedMap(configuration, keyColumnTypes, mapValueTypes);
            try {
                MapRecordCursor cursor = map.getCursor();
                MapRecord record = map.getRecord();
                while (cursor.hasNext()) {
                    MapValue srcValue = record.getValue();
                    if (srcValue.getByte(tombstoneValueIndex) == 1) {
                        continue;
                    }
                    long srcKeyHash = record.keyHashCode();
                    MapKey dstKey = scratch.withKey();
                    record.copyToKey(dstKey);
                    MapValue dstValue = dstKey.createValue(srcKeyHash);
                    record.copyValue(dstValue);
                }
                Misc.free(map);
                map = scratch;
                scratch = null;
                tombstoneCount = 0;
            } finally {
                Misc.free(scratch);
            }
        }

        @Override
        public void computeNext(Record record) {
            // map stores:
            // 0 - sum, never store NaN in it
            // 1 - current number of non-null rows in frame
            // 2 - (0-based) index of oldest value [0, bufferSize]
            // 3 - native array start offset (relative to memory address)
            // we keep nulls in window and reject them when computing avg

            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            long count;
            double sum;
            long loIdx;//current index of lo frame value ('oldest')
            long startOffset;
            double d = arg.getDouble(record);

            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Double.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && Numbers.isFinite(d)) {
                    sum = d;
                    count = 1;
                    avg = d;
                    this.sum = d;
                } else {
                    sum = 0.0;
                    avg = Double.NaN;
                    this.sum = Double.NaN;
                    count = 0;
                }

                for (int i = 0; i < bufferSize; i++) {
                    memory.putDouble(startOffset + (long) i * Double.BYTES, Double.NaN);
                }
            } else {
                sum = value.getDouble(0);
                count = value.getLong(1);
                loIdx = value.getLong(2);
                startOffset = value.getLong(3);

                //compute value using top frame element (that could be current or previous row)
                double hiValue = frameIncludesCurrentValue ? d : memory.getDouble(startOffset + ((loIdx + frameSize - 1) % bufferSize) * Double.BYTES);
                if (Numbers.isFinite(hiValue)) {
                    count++;
                    sum += hiValue;
                }

                //here sum is correct for current row
                if (count != 0) {
                    avg = sum / count;
                    this.sum = sum;
                } else {
                    avg = Double.NaN;
                    this.sum = Double.NaN;
                }


                if (frameLoBounded) {
                    //remove the oldest element
                    double loValue = memory.getDouble(startOffset + loIdx * Double.BYTES);
                    if (Numbers.isFinite(loValue)) {
                        sum -= loValue;
                        count--;
                    }
                }
            }

            value.putDouble(0, sum);
            value.putLong(1, count);
            value.putLong(2, (loIdx + 1) % bufferSize);
            value.putLong(3, startOffset);//not necessary because it doesn't change
            memory.putDouble(startOffset + loIdx * Double.BYTES, d);
            if (tombstoneValueIndex >= 0 && value.getByte(tombstoneValueIndex) != 0) {
                value.putByte(tombstoneValueIndex, (byte) 0);
                tombstoneCount--;
            }
        }

        @Override
        public double getDouble(Record rec) {
            return avg;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public Map getPartitionMap() {
            return map;
        }


        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), avg);
        }

        @Override
        public void reopen() {
            super.reopen();
            tombstoneCount = 0;
            // memory will allocate on first use
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            tombstoneCount = 0;
        }

        @Override
        public void resetPartition(Record record) {
            // ANCHOR-driven reset. Drop the partition's bounded-ROWS frame to
            // empty: sum=0, count=0, loIdx=0, ring slots back to NaN so the
            // next row in the new anchor bucket re-anchors cleanly. The ring's
            // startOffset (slot 3) stays allocated and the next row's write
            // reuses it from index 0.
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            if (value != null) {
                final long startOffset = value.getLong(3);
                value.putDouble(0, 0.0);
                value.putLong(1, 0L);
                value.putLong(2, 0L);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putDouble(startOffset + (long) i * Double.BYTES, Double.NaN);
                }
                if (tombstoneValueIndex >= 0 && value.getByte(tombstoneValueIndex) == 0) {
                    value.putByte(tombstoneValueIndex, (byte) 1);
                    tombstoneCount++;
                }
            }
        }

        @Override
        public void restore(MemoryR source, int formatVersion) {
            map.clear();
            memory.truncate();
            tombstoneCount = 0;
            long srcOffset = 0;
            final long partitionCount = source.getLong(srcOffset);
            srcOffset += Long.BYTES;
            final long ringBytes = (long) bufferSize * Double.BYTES;
            for (long p = 0; p < partitionCount; p++) {
                MapKey key = map.withKey();
                srcOffset = LiveViewSnapshotKeyCodec.readKey(key, source, srcOffset, keyColumnTypes);
                MapValue value = key.createValue();
                final double partitionSum = source.getDouble(srcOffset);
                srcOffset += Double.BYTES;
                final long partitionCountVal = source.getLong(srcOffset);
                srcOffset += Long.BYTES;
                final long loIdx = source.getLong(srcOffset);
                srcOffset += Long.BYTES;
                final long newStartOffset = memory.appendAddressFor(ringBytes) - memory.getPageAddress(0);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putDouble(newStartOffset + (long) i * Double.BYTES, source.getDouble(srcOffset));
                    srcOffset += Double.BYTES;
                }
                value.putDouble(0, partitionSum);
                value.putLong(1, partitionCountVal);
                value.putLong(2, loIdx);
                value.putLong(3, newStartOffset);
                if (tombstoneValueIndex >= 0) {
                    value.putByte(tombstoneValueIndex, (byte) 0);
                }
            }
        }

        @Override
        public void snapshot(MemoryA sink) {
            // Two-pass walk so the partition count written first matches the
            // entries that follow even if tombstoneCount drifts between cycles.
            // Tombstoned entries are skipped; the restored Map starts at the
            // post-compaction shape.
            MapRecordCursor cursor = map.getCursor();
            MapRecord record = map.getRecord();
            long liveCount = 0;
            while (cursor.hasNext()) {
                if (tombstoneValueIndex < 0 || record.getValue().getByte(tombstoneValueIndex) == 0) {
                    liveCount++;
                }
            }
            sink.putLong(liveCount);

            cursor.toTop();
            final int keyStartIndex = mapValueTypes != null
                    ? mapValueTypes.getColumnCount()
                    : AVG_OVER_PARTITION_ROWS_COLUMN_TYPES.getColumnCount();
            while (cursor.hasNext()) {
                final MapValue value = record.getValue();
                if (tombstoneValueIndex >= 0 && value.getByte(tombstoneValueIndex) != 0) {
                    continue;
                }
                LiveViewSnapshotKeyCodec.writeKey(sink, record, keyColumnTypes, keyStartIndex);
                sink.putDouble(value.getDouble(0));
                sink.putLong(value.getLong(1));
                sink.putLong(value.getLong(2));
                final long startOffset = value.getLong(3);
                for (int i = 0; i < bufferSize; i++) {
                    sink.putDouble(memory.getDouble(startOffset + (long) i * Double.BYTES));
                }
            }
        }

        @Override
        public int snapshotFormatVersion() {
            return 1;
        }

        @Override
        public int snapshotMinSupportedVersion() {
            return 1;
        }

        @Override
        public boolean supportsSnapshot() {
            return liveView
                    && keyColumnTypes != null
                    && LiveViewSnapshotKeyCodec.isAllTypesSupported(keyColumnTypes);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(')');
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
            tombstoneCount = 0;
        }
    }

    // Handles avg() over ([order by ts] range between [unbounded | x] preceding and [ x preceding | current row ] ); no partition by key
    // When lower bound is unbounded we add but immediately discard any values that enter the frame so buffer should only contain values
    // between upper bound and current row's value .
    static class AvgOverRangeFrameFunction extends BaseWindowFunction implements Reopenable, WindowDoubleFunction {
        private static final int RECORD_SIZE = Long.BYTES + Double.BYTES;
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final long maxDiff;
        // holds resizable ring buffers
        // actual frame data - [timestamp, value] pairs - is stored in mem at [ offset + first_idx*16, offset + last_idx*16]
        // note: we ignore nulls to reduce memory usage
        private final MemoryARW memory;
        private final long minDiff;
        private final int timestampIndex;
        protected double externalSum;
        private double avg;
        private long capacity;
        private long firstIdx;
        private long frameSize;
        private long size;
        private long startOffset;
        private double sum;

        public AvgOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx
        ) {
            this(
                    rangeLo,
                    rangeHi,
                    arg,
                    configuration.getSqlWindowStorePageSize() / RECORD_SIZE,
                    Vm.getCARWInstance(
                            configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(),
                            MemoryTag.NATIVE_CIRCULAR_BUFFER
                    ),
                    timestampIdx
            );
        }

        public AvgOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                long initialCapacity,
                MemoryARW memory,
                int timestampIdx
        ) {
            super(arg);
            this.initialCapacity = initialCapacity;
            this.memory = memory;
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE; // maxDiff must be used only if frameLoBounded
            minDiff = Math.abs(rangeHi);
            timestampIndex = timestampIdx;

            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            sum = 0.0;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            long timestamp = record.getTimestamp(timestampIndex);
            double d = arg.getDouble(record);

            long newFirstIdx = firstIdx;

            if (frameLoBounded) {
                // find new bottom border of range frame and remove unneeded elements
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        // if rangeHi < 0, some elements from the window can be not in the frame
                        if (frameSize > 0) {
                            double val = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            sum -= val;
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

            // add new element if not null
            if (Numbers.isFinite(d)) {
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
                memory.putDouble(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
                size++;
            }

            // find new top border of range frame and add new elements
            if (frameLoBounded) {
                for (long i = frameSize, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);

                    if (diff <= maxDiff && diff >= minDiff) {
                        double value = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        sum += value;
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
                        double val = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        sum += val;
                        frameSize++;
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        break;
                    }
                }
                firstIdx = newFirstIdx;
            }
            if (frameSize != 0) {
                avg = sum / frameSize;
                externalSum = sum;

            } else {
                avg = Double.NaN;
                externalSum = Double.NaN;
            }
        }

        @Override
        public double getDouble(Record rec) {
            return avg;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }


        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), avg);
        }

        @Override
        public void reopen() {
            avg = Double.NaN;
            externalSum = Double.NaN;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            sum = 0.0;
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
            super.toTop();
            avg = Double.NaN;
            externalSum = Double.NaN;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            sum = 0.0;
        }
    }

    // Handles avg() over ([order by o] rows between y and z); there's no partition by.
    // Removable cumulative aggregation.
    static class AvgOverRowsFrameFunction extends BaseWindowFunction implements Reopenable, WindowDoubleFunction {
        private final MemoryARW buffer;
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        protected double externalSum = 0;
        private double avg = 0;
        private long count = 0;
        private int loIdx = 0;
        private double sum = 0.0;

        public AvgOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory) {
            super(arg);

            assert rowsLo != Long.MIN_VALUE || rowsHi != 0; // use AvgOverUnboundedRowsFrameFunction in case of (Long.MIN_VALUE, 0) range
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
            double d = arg.getDouble(record);

            //compute value using top frame element (that could be current or previous row)
            double hiValue = d;
            if (frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getDouble((long) ((loIdx + frameSize - 1) % bufferSize) * Double.BYTES);
            } else if (!frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getDouble((long) (loIdx % bufferSize) * Double.BYTES);
            }
            if (Numbers.isFinite(hiValue)) {
                sum += hiValue;
                count++;
            }
            if (count != 0) {
                avg = sum / count;
                externalSum = sum;
            } else {
                avg = Double.NaN;
                externalSum = Double.NaN;
            }


            if (frameLoBounded) {
                //remove the oldest element with newest
                double loValue = buffer.getDouble((long) loIdx * Double.BYTES);
                if (Numbers.isFinite(loValue)) {
                    sum -= loValue;
                    count--;
                }
            }

            //overwrite oldest element
            buffer.putDouble((long) loIdx * Double.BYTES, d);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public double getDouble(Record rec) {
            return avg;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }


        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), avg);
        }

        @Override
        public void reopen() {
            avg = 0;
            count = 0;
            loIdx = 0;
            sum = 0.0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            avg = 0;
            count = 0;
            loIdx = 0;
            sum = 0.0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(')');
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
            avg = 0;
            count = 0;
            loIdx = 0;
            sum = 0.0;
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putDouble((long) i * Double.BYTES, Double.NaN);
            }
        }
    }

    // Handles:
    // - avg(a) over (partition by x rows between unbounded preceding and current row)
    // - avg(a) over (partition by x order by ts range between unbounded preceding and current row)
    // Doesn't require value buffering.
    static class AvgOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {
        // Stable snapshot of the partition-by key column types, taken at
        // construction. The factory passes the live WindowContext buffer
        // which it reuses across window-function compiles; the function's
        // own copy keeps the types available after compilation has moved
        // on (live-view snapshot, Phase 5 eviction).
        private final ArrayColumnTypes keyColumnTypes;
        private double avg;

        public AvgOverUnboundedPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                Function arg,
                ColumnTypes partitionByKeyTypes
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.keyColumnTypes = new ArrayColumnTypes();
            for (int i = 0, n = partitionByKeyTypes.getColumnCount(); i < n; i++) {
                this.keyColumnTypes.add(partitionByKeyTypes.getColumnType(i));
            }
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            double sum;
            long count;

            if (value.isNew()) {
                sum = 0;
                count = 0;
            } else {
                sum = value.getDouble(0);
                count = value.getLong(1);
            }

            double d = arg.getDouble(record);
            if (Numbers.isFinite(d)) {
                sum += d;
                count++;
            }

            value.putDouble(0, sum);
            value.putLong(1, count);
            avg = count != 0 ? sum / count : Double.NaN;
        }

        @Override
        public double getDouble(Record rec) {
            return avg;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public Map getPartitionMap() {
            return map;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), avg);
        }

        @Override
        public void resetPartition(Record record) {
            // ANCHOR-driven reset (RFC 123). Zero the [sum, count] slots; next
            // computeNext reads sum=0, count=0 and re-anchors on the post-reset row.
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            if (value != null) {
                value.putDouble(0, 0.0);
                value.putLong(1, 0L);
            }
        }

        @Override
        public void restore(MemoryR source, int formatVersion) {
            map.clear();
            long offset = 0;
            final long partitionCount = source.getLong(offset);
            offset += Long.BYTES;
            for (long p = 0; p < partitionCount; p++) {
                MapKey key = map.withKey();
                offset = LiveViewSnapshotKeyCodec.readKey(key, source, offset, keyColumnTypes);
                MapValue value = key.createValue();
                value.putDouble(0, source.getDouble(offset));
                offset += Double.BYTES;
                value.putLong(1, source.getLong(offset));
                offset += Long.BYTES;
            }
        }

        @Override
        public void snapshot(MemoryA sink) {
            sink.putLong(map.size());
            MapRecordCursor cursor = map.getCursor();
            MapRecord record = map.getRecord();
            // AVG_COLUMN_TYPES = [DOUBLE sum, LONG count], so the key column
            // sits at record index 2 in the Map's [values, key] record layout.
            final int keyStartIndex = AVG_COLUMN_TYPES.getColumnCount();
            while (cursor.hasNext()) {
                LiveViewSnapshotKeyCodec.writeKey(sink, record, keyColumnTypes, keyStartIndex);
                final MapValue value = record.getValue();
                sink.putDouble(value.getDouble(0));
                sink.putLong(value.getLong(1));
            }
        }

        @Override
        public int snapshotFormatVersion() {
            return 1;
        }

        @Override
        public int snapshotMinSupportedVersion() {
            return 1;
        }

        @Override
        public boolean supportsSnapshot() {
            return LiveViewSnapshotKeyCodec.isAllTypesSupported(keyColumnTypes);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(arg).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row)");
        }
    }

    // Handles avg() over (rows between unbounded preceding and current row); there's no partition by.
    static class AvgOverUnboundedRowsFrameFunction extends BaseWindowFunction implements WindowDoubleFunction {

        private double avg;
        private long count = 0;
        private double sum = 0.0;

        public AvgOverUnboundedRowsFrameFunction(Function arg) {
            super(arg);
        }

        @Override
        public void computeNext(Record record) {
            double d = arg.getDouble(record);
            if (Numbers.isFinite(d)) {
                sum += d;
                count++;
            }

            avg = count != 0 ? sum / count : Double.NaN;
        }

        @Override
        public double getDouble(Record rec) {
            return avg;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }


        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), avg);
        }

        @Override
        public void reset() {
            super.reset();
            avg = Double.NaN;
            count = 0;
            sum = 0.0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(arg).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();

            avg = Double.NaN;
            count = 0;
            sum = 0.0;
        }
    }

    // avg() over () - empty clause, no partition by no order by, no frame == default frame
    static class AvgOverWholeResultSetFunction extends BaseWindowFunction implements WindowDoubleFunction {
        private double avg;
        private long count;
        private double sum;

        public AvgOverWholeResultSetFunction(Function arg) {
            super(arg);
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }


        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            double d = arg.getDouble(record);
            if (Numbers.isFinite(d)) {
                sum += d;
                count++;
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), avg);
        }

        @Override
        public void preparePass2() {
            avg = count > 0 ? sum / count : Double.NaN;
        }

        @Override
        public void reset() {
            super.reset();
            avg = Double.NaN;
            count = 0;
            sum = 0.0;
        }

        @Override
        public void toTop() {
            super.toTop();

            avg = Double.NaN;
            count = 0;
            sum = 0.0;
        }
    }

    static {
        AVG_COLUMN_TYPES = new ArrayColumnTypes();
        AVG_COLUMN_TYPES.add(ColumnType.DOUBLE);
        AVG_COLUMN_TYPES.add(ColumnType.LONG);

        AVG_OVER_PARTITION_RANGE_COLUMN_TYPES = new ArrayColumnTypes();
        AVG_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.DOUBLE);// current frame sum
        AVG_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG);  // number of (non-null) values in current frame
        AVG_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG);  // native array start offset, requires updating on resize
        AVG_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG);   // native buffer size
        AVG_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG);   // native buffer capacity
        AVG_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG);   // index of first buffered element

        AVG_OVER_PARTITION_ROWS_COLUMN_TYPES = new ArrayColumnTypes();
        AVG_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.DOUBLE);// sum
        AVG_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG);// current frame size
        AVG_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG);// position of current oldest element
        AVG_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG);// start offset of native array

        AVG_OVER_PARTITION_ROWS_COLUMN_TYPES_LV = new ArrayColumnTypes();
        AVG_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.DOUBLE);// sum
        AVG_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.LONG);// current frame size
        AVG_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.LONG);// position of current oldest element
        AVG_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.LONG);// start offset of native array
        AVG_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.BYTE);// tombstone (anchor-driven compaction)
    }
}

/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.cairo.map.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.orderby.RecordComparatorCompiler;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.model.WindowColumn;
import io.questdb.std.*;

public class AvgDoubleWindowFunctionFactory implements FunctionFactory {

    private static final ArrayColumnTypes AVG_COLUMN_TYPES;

    private static final String NAME = "avg";
    private static final String SIGNATURE = NAME + "(D)";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public boolean isWindow() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final WindowContext windowContext = sqlExecutionContext.getWindowContext();
        if (windowContext.isEmpty()) {
            throw SqlException.emptyWindowContext(position);
        }

        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();

        if (!windowContext.isDefaultFrame()) {
            if (rowsLo > 0) {
                throw SqlException.$(windowContext.getRowsLoKindPos(), "frame start supports UNBOUNDED PRECEDING, _number_ PRECEDING and CURRENT ROW only");
            }
            if (rowsHi > 0) {
                if (rowsHi != Long.MAX_VALUE) {
                    throw SqlException.$(windowContext.getRowsHiKindPos(), "frame end supports _number_ PRECEDING and CURRENT ROW only");
                } else if (rowsLo != Long.MIN_VALUE) {
                    throw SqlException.$(windowContext.getRowsHiKindPos(), "frame end supports UNBOUNDED FOLLOWING only when frame start is UNBOUNDED PRECEDING");
                }
            }
        }

        int exclusionKind = windowContext.getExclusionKind();
        int exclusionKindPos = windowContext.getExclusionKindPos();
        if (exclusionKind != WindowColumn.EXCLUDE_NO_OTHERS
                && exclusionKind != WindowColumn.EXCLUDE_CURRENT_ROW) {
            throw SqlException.$(exclusionKindPos, "only EXCLUDE NO OTHERS and EXCLUDE CURRENT ROW exclusion modes are supported");
        }

        if (exclusionKind == WindowColumn.EXCLUDE_CURRENT_ROW) {
            // assumes frame doesn't use 'following'
            if (rowsHi == Long.MAX_VALUE) {
                throw SqlException.$(exclusionKindPos, "EXCLUDE CURRENT ROW not supported with UNBOUNDED FOLLOWING frame boundary");
            }

            if (rowsHi == 0) {
                rowsHi = -1;
            }
            if (rowsHi < rowsLo) {
                throw SqlException.$(exclusionKindPos, "end of window is higher than start of window due to exclusion mode");
            }
        }

        int framingMode = windowContext.getFramingMode();
        if (framingMode == WindowColumn.FRAMING_GROUPS) {
            throw SqlException.$(position, "function not implemented for given window parameters");
        }

        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();

        if (partitionByRecord != null) {
            if (framingMode == WindowColumn.FRAMING_RANGE) {
                // moving average over whole partition (no order by, default frame) or (order by, unbounded preceding to unbounded following)
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createMap(
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
                    Map map = MapFactory.createMap(
                            configuration,
                            partitionByKeyTypes,
                            AVG_COLUMN_TYPES
                    );

                    //same as for rows because calculation stops at current rows even if there are 'equal' following rows
                    return new AvgOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0)
                    );
                } // range between [unbounded | x] preceding and [x preceding | current row]
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }

                    int timestampIndex = windowContext.getTimestampIndex();

                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.DOUBLE);// current frame sum
                    columnTypes.add(ColumnType.LONG);  // number of (non-null) values in current frame
                    columnTypes.add(ColumnType.LONG);  // native array start offset, requires updating on resize
                    columnTypes.add(ColumnType.LONG);   // native buffer size
                    columnTypes.add(ColumnType.LONG);   // native buffer capacity
                    columnTypes.add(ColumnType.LONG);   // index of first buffered element

                    Map map = MapFactory.createMap(
                            configuration,
                            partitionByKeyTypes,
                            columnTypes
                    );

                    final int initialBufferSize = configuration.getSqlWindowInitialRangeBufferSize();
                    MemoryARW mem = Vm.getARWInstance(configuration.getSqlWindowStorePageSize(), configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);

                    // moving average over range between timestamp - rowsLo and timestamp + rowsHi (inclusive)
                    return new AvgOverPartitionRangeFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            rowsLo,
                            rowsHi,
                            args.get(0),
                            mem,
                            initialBufferSize,
                            timestampIndex
                    );
                }
            } else if (framingMode == WindowColumn.FRAMING_ROWS) {
                //between unbounded preceding and current row
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createMap(
                            configuration,
                            partitionByKeyTypes,
                            AVG_COLUMN_TYPES
                    );

                    return new AvgOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0)
                    );
                } // between current row and current row
                else if (rowsLo == 0 && rowsLo == rowsHi) {
                    return new AvgOverCurrentRowFunction(args.get(0));
                } // whole partition
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createMap(
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
                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.DOUBLE);// sum
                    columnTypes.add(ColumnType.LONG);// current frame size
                    columnTypes.add(ColumnType.INT);// position of current oldest element
                    columnTypes.add(ColumnType.LONG);// start offset of native array

                    Map map = MapFactory.createMap(
                            configuration,
                            partitionByKeyTypes,
                            columnTypes
                    );

                    MemoryARW mem = Vm.getARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);

                    // moving average over preceding N rows
                    return new AvgOverPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            rowsLo,
                            rowsHi,
                            args.get(0),
                            mem
                    );
                }
            }
        } else { // no partition key
            if (framingMode == WindowColumn.FRAMING_RANGE) {
                // if there's no order by then all elements are equal in range mode, thus calculation is done on whole result set
                if (!windowContext.isOrdered() && windowContext.isDefaultFrame()) {
                    return new AvgOverWholeResultSetFunction(args.get(0));
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    //same as for rows because calculation stops at current rows even if there are 'equal' following rows
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

            } else if (framingMode == WindowColumn.FRAMING_ROWS) {
                //between unbounded preceding and current row
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new AvgOverUnboundedRowsFrameFunction(args.get(0));
                } // between current row and current row
                else if (rowsLo == 0 && rowsLo == rowsHi) {
                    return new AvgOverCurrentRowFunction(args.get(0));
                } // whole result set
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new AvgOverWholeResultSetFunction(args.get(0));
                } //between [unbounded | x] preceding and [x preceding | current row]
                else {
                    MemoryARW mem = Vm.getARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(),
                            MemoryTag.NATIVE_CIRCULAR_BUFFER);

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
    static class AvgOverCurrentRowFunction extends BaseAvgFunction {

        private double avg;

        AvgOverCurrentRowFunction(Function arg) {
            super(arg);
        }

        @Override
        public void computeNext(Record record) {
            avg = arg.getDouble(record);
        }

        @Override
        public double getDouble(Record rec) {
            return avg;
        }

        @Override
        public int getPassCount() {
            return ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), avg);
        }
    }

    // handles avg() over (partition by x)
    // order by is absent so default frame mode includes all rows in partition
    static class AvgOverPartitionFunction extends BasePartitionedAvgFunction {

        public AvgOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
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

            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), val);
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
    }

    // Handles avg() over (partition by x order by ts range between [undobuned | y] preceding and [z preceding | current row])
    // Removable cumulative aggregation with timestamp & value stored in resizable ring buffers
    // When lower bound is unbounded we add but immediately discard any values that enter the frame so buffer should only contain values
    // between upper bound and current row's value.
    static class AvgOverPartitionRangeFrameFunction extends BasePartitionedAvgFunction {

        private static final int RECORD_SIZE = Long.BYTES + Double.BYTES;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        // list of [size, startOffset] pairs marking free space within mem
        private final LongList freeList = new LongList();
        private final int initialBufferSize;
        private final long maxDiff;
        // holds resizable ring buffers
        private final MemoryARW memory;
        private final long minDiff;
        private final int timestampIndex;
        private double avg;

        public AvgOverPartitionRangeFrameFunction(Map map,
                                                  VirtualRecord partitionByRecord,
                                                  RecordSink partitionBySink,
                                                  long rangeLo,
                                                  long rangeHi,
                                                  Function arg,
                                                  MemoryARW memory,
                                                  int initialBufferSize,
                                                  int timestampIdx) {
            super(map, partitionByRecord, partitionBySink, arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
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
                    if (frameLoBounded || !frameIncludesCurrentValue) {
                        memory.putLong(startOffset, timestamp);
                        memory.putDouble(startOffset + Long.BYTES, d);
                        size = 1;
                    } else {
                        size = 0;
                    }

                    if (frameIncludesCurrentValue) {
                        sum = d;
                        avg = d;
                        frameSize = 1;
                    } else {
                        sum = 0.0;
                        avg = Double.NaN;
                        frameSize = 0;
                    }
                } else {
                    size = 0;
                    sum = 0.0;
                    avg = Double.NaN;
                    frameSize = 0;
                }
            } else {
                sum = mapValue.getDouble(0);
                frameSize = mapValue.getLong(1);
                startOffset = mapValue.getLong(2);
                size = mapValue.getInt(3);
                capacity = mapValue.getInt(4);
                firstIdx = mapValue.getInt(5);

                long newFirstIdx = firstIdx;

                if (frameLoBounded) {
                    // find new bottom border of range frame and remove unneeded elements
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
                            double val = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            sum -= val;
                            frameSize--;
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
                        capacity <<= 1;

                        long oldAddress = memory.getPageAddress(0) + startOffset;
                        long newAddress = -1;

                        // try to find matching block in free list
                        for (int i = 0, n = freeList.size(); i < n; i += 2) {
                            if (freeList.getQuick(i) == capacity) {
                                newAddress = memory.getPageAddress(0) + freeList.getQuick(i + 1);
                                // replace block info with ours
                                freeList.setQuick(i, size);
                                freeList.setQuick(i + 1, startOffset);
                                break;
                            }
                        }

                        if (newAddress == -1) {
                            newAddress = memory.appendAddressFor(capacity * RECORD_SIZE);
                            // call above can end up resizing and thus changing memory start address
                            oldAddress = memory.getPageAddress(0) + startOffset;
                            freeList.add(size, startOffset);
                        }

                        if (firstIdx == 0) {
                            Vect.memcpy(newAddress, oldAddress, size * RECORD_SIZE);
                        } else {
                            //we can't simply copy because that'd leave a gap in the middle
                            long firstPieceSize = (size - firstIdx) * RECORD_SIZE;
                            Vect.memcpy(newAddress, oldAddress + firstIdx * RECORD_SIZE, firstPieceSize);
                            Vect.memcpy(newAddress + firstPieceSize, oldAddress, ((firstIdx + size) % size) * RECORD_SIZE);
                            firstIdx = 0;
                        }

                        startOffset = newAddress - memory.getPageAddress(0);
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

                avg = (frameSize != 0 ? sum / frameSize : Double.NaN);
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
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reopen() {
            super.reopen();
            // memory will allocate on first use
            avg = Double.NaN;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
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
    static class AvgOverPartitionRowsFrameFunction extends BasePartitionedAvgFunction {

        //number of values we need to keep to compute over frame
        // (can be bigger than frame because we've to buffer values between rowsHi and current row )
        private final int bufferSize;

        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        // holds fixed-size ring buffers of double values
        private final MemoryARW memory;
        private double avg;

        public AvgOverPartitionRowsFrameFunction(Map map,
                                                 VirtualRecord partitionByRecord,
                                                 RecordSink partitionBySink,
                                                 long rowsLo,
                                                 long rowsHi,
                                                 Function arg,
                                                 MemoryARW memory) {
            super(map, partitionByRecord, partitionBySink, arg);

            if (rowsLo > Long.MIN_VALUE) {
                frameSize = (int) (rowsHi - rowsLo);
                bufferSize = (int) Math.abs(rowsLo);
                frameLoBounded = true;
            } else {
                frameSize = (int) Math.abs(rowsHi);
                bufferSize = frameSize;
                frameLoBounded = false;
            }
            frameIncludesCurrentValue = rowsHi == 0;

            this.memory = memory;
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
            int loIdx;//current index of lo frame value ('oldest')
            long startOffset;
            double d = arg.getDouble(record);

            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor(bufferSize * Double.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && Numbers.isFinite(d)) {
                    sum = d;
                    count = 1;
                    avg = d;
                } else {
                    sum = 0.0;
                    avg = Double.NaN;
                    count = 0;
                }

                for (int i = 0; i < bufferSize; i++) {
                    memory.putDouble(startOffset + i * Double.BYTES, Double.NaN);
                }
            } else {
                sum = value.getDouble(0);
                count = value.getLong(1);
                loIdx = value.getInt(2);
                startOffset = value.getInt(3);

                //compute value using top frame element (that could be current or previous row)
                double hiValue = frameIncludesCurrentValue ? d : memory.getDouble(startOffset + ((loIdx + frameSize) % bufferSize) * Double.BYTES);
                if (Numbers.isFinite(hiValue)) {
                    count++;
                    sum += hiValue;
                }

                //here sum is correct for current row
                avg = count != 0 ? sum / count : Double.NaN;

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
        }

        @Override
        public double getDouble(Record rec) {
            return avg;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), avg);
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
            sink.val(NAME);
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
            if (bufferSize - frameSize == 0) {
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

    // Handles avg() over ([order by ts] range between [unbounded | x] preceding and [ x preceding | current row ] ); no partition by key
    // When lower bound is unbounded we add but immediately discard any values that enter the frame so buffer should only contain values
    // between upper bound and current row's value .
    static class AvgOverRangeFrameFunction extends BaseAvgFunction implements Reopenable {
        private final int RECORD_SIZE = Long.BYTES + Double.BYTES;
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final long maxDiff;
        // holds resizable ring buffers
        // actual frame data - [timestamp, value] pairs - is stored in mem at [ offset + first_idx*16, offset + last_idx*16]
        // note: we ignore nulls to reduce memory usage
        private final MemoryARW memory;
        private final long minDiff;
        private final int timestampIndex;
        private double avg;
        private long capacity;
        private long firstIdx;
        private long frameSize;
        private long size;
        private long startOffset;
        private double sum;

        public AvgOverRangeFrameFunction(long rangeLo,
                                         long rangeHi,
                                         Function arg,
                                         CairoConfiguration configuration,
                                         int timestampIdx) {
            super(arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            minDiff = Math.abs(rangeHi);
            timestampIndex = timestampIdx;
            initialCapacity = configuration.getSqlWindowStorePageSize() / RECORD_SIZE;

            capacity = initialCapacity;
            memory = Vm.getARWInstance(configuration.getSqlWindowStorePageSize(), configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
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
                        double val = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        sum -= val;
                        frameSize--;
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
                    long newAddress = memory.appendAddressFor(capacity * RECORD_SIZE);
                    // call above can end up resizing and thus changing memory start address
                    long oldAddress = memory.getPageAddress(0) + startOffset;

                    if (firstIdx == 0) {
                        Vect.memcpy(newAddress, oldAddress, size * RECORD_SIZE);
                    } else {
                        //we can't simply copy because that'd leave a gap in the middle
                        long firstPieceSize = (size - firstIdx) * RECORD_SIZE;
                        Vect.memcpy(newAddress, oldAddress + firstIdx * RECORD_SIZE, firstPieceSize);
                        Vect.memcpy(newAddress + firstPieceSize, oldAddress, ((firstIdx + size) % size) * RECORD_SIZE);
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

            avg = frameSize != 0 ? sum / frameSize : Double.NaN;
        }

        @Override
        public double getDouble(Record rec) {
            return avg;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reopen() {
            avg = Double.NaN;
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
            sink.val(NAME);
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
    static class AvgOverRowsFrameFunction extends BaseAvgFunction implements Reopenable {
        private final MemoryARW buffer;
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private double avg = 0;
        private long count = 0;
        private int loIdx = 0;
        private double sum = 0.0;

        public AvgOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory) {
            super(arg);

            if (rowsLo > Long.MIN_VALUE) {
                frameSize = (int) (rowsHi - rowsLo);
                bufferSize = (int) Math.abs(rowsLo);//number of values we need to keep to compute over frame
                frameLoBounded = true;
            } else {
                frameSize = (int) Math.abs(rowsHi);
                bufferSize = frameSize;
                frameLoBounded = false;
            }

            frameIncludesCurrentValue = rowsHi == 0;
            this.buffer = memory;
            initBuffer();
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
            double hiValue = frameIncludesCurrentValue ? d : buffer.getDouble(((loIdx + frameSize) % bufferSize) * Double.BYTES);
            if (Numbers.isFinite(hiValue)) {
                sum += hiValue;
                count++;
            }

            avg = count != 0 ? sum / count : Double.NaN;

            if (frameLoBounded) {
                //remove the oldest element with newest
                double loValue = buffer.getDouble(loIdx * Double.BYTES);
                if (Numbers.isFinite(loValue)) {
                    sum -= loValue;
                    count--;
                }
            }

            //overwrite oldest element
            buffer.putDouble(loIdx * Double.BYTES, d);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public double getDouble(Record rec) {
            return avg;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), avg);
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
            sink.val(NAME);
            sink.val('(').val(arg).val(')');
            sink.val(" over (");
            sink.val(" rows between ");
            if (frameLoBounded) {
                sink.val(bufferSize);
            } else {
                sink.val("unbounded");
            }
            sink.val(" preceding and ");
            if (bufferSize - frameSize == 0) {
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
                buffer.putDouble(i * Double.BYTES, Double.NaN);
            }
        }
    }

    // Handles:
    // - avg(a) over (partition by x rows between unbounded preceding and current row)
    // - avg(a) over (partition by x order by ts groups between unbounded preceding and current row)
    // - avg(a) over (partition by x order by ts range between unbounded preceding and current row)
    // Doesn't require value buffering.
    static class AvgOverUnboundedPartitionRowsFrameFunction extends BasePartitionedAvgFunction {

        private double avg;

        public AvgOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
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

                value.putDouble(0, sum);
                value.putLong(1, count);
            }

            avg = count != 0 ? sum / count : Double.NaN;
        }

        @Override
        public double getDouble(Record rec) {
            return avg;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), avg);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(arg).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row )");
        }
    }

    // Handles avg() over (rows between unbounded preceding and current row); there's no partititon by.
    static class AvgOverUnboundedRowsFrameFunction extends BaseAvgFunction {

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
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);

            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), avg);
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
    static class AvgOverWholeResultSetFunction extends BaseAvgFunction {
        private double avg;
        private long count;
        private double sum;

        public AvgOverWholeResultSetFunction(Function arg) {
            super(arg);
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), avg);
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

    private static abstract class BaseAvgFunction extends DoubleFunction implements WindowFunction, ScalarFunction {
        protected final Function arg;
        protected int columnIndex;

        public BaseAvgFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public void close() {
            arg.close();
        }

        @Override
        public double getDouble(Record rec) {
            //unused
            throw new UnsupportedOperationException();
        }

        @Override
        public void initRecordComparator(RecordComparatorCompiler recordComparatorCompiler, ArrayColumnTypes chainTypes, IntList order) {
        }

        @Override
        public void reset() {

        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(arg).val(')');
            sink.val(" over ()");
        }

        @Override
        public void toTop() {
            arg.toTop();
        }
    }

    private static abstract class BasePartitionedAvgFunction extends BaseAvgFunction implements Reopenable {
        protected final Map map;
        protected final VirtualRecord partitionByRecord;
        protected final RecordSink partitionBySink;

        public BasePartitionedAvgFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(arg);
            this.map = map;
            this.partitionByRecord = partitionByRecord;
            this.partitionBySink = partitionBySink;
        }

        @Override
        public void close() {
            super.close();
            map.close();
            Misc.freeObjList(partitionByRecord.getFunctions());
        }

        @Override
        public void reopen() {
            map.reopen();
        }

        @Override
        public void reset() {
            map.close();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(arg).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            map.clear();
        }
    }

    static {
        AVG_COLUMN_TYPES = new ArrayColumnTypes();
        AVG_COLUMN_TYPES.add(ColumnType.DOUBLE);
        AVG_COLUMN_TYPES.add(ColumnType.LONG);
    }
}

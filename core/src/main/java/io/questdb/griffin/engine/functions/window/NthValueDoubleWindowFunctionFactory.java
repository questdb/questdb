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
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

// Returns the value evaluated at the n-th row of the window frame (1-based).
public class NthValueDoubleWindowFunctionFactory extends AbstractWindowFunctionFactory {

    public static final String NAME = "nth_value";
    private static final ArrayColumnTypes NTH_VALUE_COLUMN_TYPES;
    private static final String SIGNATURE = NAME + "(DI)";

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

        Function nFunc = args.get(1);
        if (!nFunc.isConstant()) {
            throw SqlException.$(argPositions.getQuick(1), "nth_value n must be a constant");
        }
        int n = nFunc.getInt(null);
        if (n <= 0) {
            throw SqlException.$(argPositions.getQuick(1), "nth_value n must be a positive integer");
        }

        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        if (rowsHi < rowsLo) {
            return new DoubleNullFunction(
                    args.get(0),
                    NAME,
                    rowsLo,
                    rowsHi,
                    windowContext.getFramingMode() == WindowExpression.FRAMING_RANGE,
                    windowContext.getPartitionByRecord()
            );
        }

        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                // whole partition (no order by, default frame) or (order by, unbounded preceding to unbounded following)
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            NTH_VALUE_COLUMN_TYPES
                    );

                    return new NthValueOverPartitionFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            n
                    );
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            NTH_VALUE_COLUMN_TYPES
                    );

                    return new NthValueOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            n
                    );
                } // range between [unbounded | x] preceding and [x preceding | current row]
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }

                    int timestampIndex = windowContext.getTimestampIndex();

                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.LONG);  // number of values in current frame
                    columnTypes.add(ColumnType.LONG);  // native array start offset, requires updating on resize
                    columnTypes.add(ColumnType.LONG);  // native buffer size
                    columnTypes.add(ColumnType.LONG);  // native buffer capacity
                    columnTypes.add(ColumnType.LONG);  // index of first buffered element

                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            columnTypes
                    );

                    final int initialBufferSize = configuration.getSqlWindowInitialRangeBufferSize();
                    MemoryARW mem = Vm.getCARWInstance(
                            configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(),
                            MemoryTag.NATIVE_CIRCULAR_BUFFER
                    );

                    return new NthValueOverPartitionRangeFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            rowsLo,
                            rowsHi,
                            args.get(0),
                            mem,
                            initialBufferSize,
                            timestampIndex,
                            n
                    );
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                // between unbounded preceding and current row
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            NTH_VALUE_COLUMN_TYPES
                    );

                    return new NthValueOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            n
                    );
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new NthValueOverCurrentRowFunction(args.get(0), n);
                } // whole partition
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            NTH_VALUE_COLUMN_TYPES
                    );

                    return new NthValueOverPartitionFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            n
                    );
                }
                // between [unbounded | x] preceding and [x preceding | current row] (but not unbounded preceding to current row)
                else {
                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.LONG); // position of current oldest element
                    columnTypes.add(ColumnType.LONG); // start offset of native array
                    columnTypes.add(ColumnType.LONG); // count of values in buffer

                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            columnTypes
                    );

                    MemoryARW mem = Vm.getCARWInstance(
                            configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(),
                            MemoryTag.NATIVE_CIRCULAR_BUFFER
                    );

                    return new NthValueOverPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            rowsLo,
                            rowsHi,
                            args.get(0),
                            mem,
                            n
                    );
                }
            }
        } else { // no partition key
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                // if there's no order by then all elements are equal in range mode, thus calculation is done on whole result set
                if (!windowContext.isOrdered() && windowContext.isDefaultFrame()) {
                    return new NthValueOverWholeResultSetFunction(args.get(0), n);
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new NthValueOverUnboundedRowsFrameFunction(args.get(0), n);
                } // range between [unbounded | x] preceding and [y preceding | current row]
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }

                    int timestampIndex = windowContext.getTimestampIndex();

                    return new NthValueOverRangeFrameFunction(
                            rowsLo,
                            rowsHi,
                            args.get(0),
                            configuration,
                            timestampIndex,
                            n
                    );
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                // between unbounded preceding and [current row | unbounded following]
                if (rowsLo == Long.MIN_VALUE && (rowsHi == 0 || rowsHi == Long.MAX_VALUE)) {
                    return new NthValueOverUnboundedRowsFrameFunction(args.get(0), n);
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new NthValueOverCurrentRowFunction(args.get(0), n);
                } // between [unbounded | x] preceding and [y preceding | current row]
                else {
                    MemoryARW mem = Vm.getCARWInstance(
                            configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(),
                            MemoryTag.NATIVE_CIRCULAR_BUFFER
                    );

                    return new NthValueOverRowsFrameFunction(
                            args.get(0),
                            rowsLo,
                            rowsHi,
                            mem,
                            n
                    );
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    // (rows between current row and current row) processes 1-element-big set
    // n=1 returns the current value, n>1 returns NaN
    static class NthValueOverCurrentRowFunction extends BaseWindowFunction implements WindowDoubleFunction {

        private final int n;
        private double value;

        NthValueOverCurrentRowFunction(Function arg, int n) {
            super(arg);
            this.n = n;
        }

        @Override
        public void computeNext(Record record) {
            value = n == 1 ? arg.getDouble(record) : Double.NaN;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (rows between current row and current row)");
        }
    }

    // handles nth_value() over (partition by x)
    // order by is absent so default frame mode includes all rows in the partition
    // TWO_PASS: pass1 counts rows per partition, stores nth value when found; pass2 emits
    static class NthValueOverPartitionFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        private final int n;
        private double nthValue;

        public NthValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int n) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.n = n;
        }

        @Override
        public void computeNext(Record record) {
            // not used, two-pass function
        }

        @Override
        public double getDouble(Record rec) {
            return nthValue;
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
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            if (value.isNew()) {
                // first row in partition: count = 1
                if (n == 1) {
                    value.putDouble(0, arg.getDouble(record));
                } else {
                    value.putDouble(0, Double.NaN);
                }
                value.putLong(1, 1);
            } else {
                long count = value.getLong(1) + 1;
                value.putLong(1, count);
                if (count == n) {
                    value.putDouble(0, arg.getDouble(record));
                }
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
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(')');
        }
    }

    // Handles nth_value() over (partition by x order by ts range between y preceding and [z preceding | current row])
    // Removable cumulative aggregation with timestamp & value stored in resizable ring buffers
    public static class NthValueOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        protected static final int RECORD_SIZE = Long.BYTES + Double.BYTES;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final LongList freeList = new LongList();
        protected final int initialBufferSize;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final RingBufferDesc memoryDesc = new RingBufferDesc();
        protected final long minDiff;
        protected final int n;
        protected final int timestampIndex;
        protected double nthValue;

        public NthValueOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int n
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            this.n = n;
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
            double d = arg.getDouble(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;

                memory.putLong(startOffset, timestamp);
                memory.putDouble(startOffset + Long.BYTES, d);
                size = 1;

                if (frameIncludesCurrentValue) {
                    frameSize = 1;
                    nthValue = n == 1 ? d : Double.NaN;
                } else {
                    frameSize = 0;
                    nthValue = Double.NaN;
                }
            } else {
                frameSize = mapValue.getLong(0);
                startOffset = mapValue.getLong(1);
                size = mapValue.getLong(2);
                capacity = mapValue.getLong(3);
                firstIdx = mapValue.getLong(4);

                if (!frameLoBounded && frameSize >= n) {
                    // nth value already found and won't change
                    long nthIdx = (firstIdx + n - 1) % capacity;
                    nthValue = memory.getDouble(startOffset + nthIdx * RECORD_SIZE + Long.BYTES);
                    return;
                }

                long newFirstIdx = firstIdx;

                if (frameLoBounded) {
                    // find new bottom border of range frame and remove unneeded elements
                    for (long i = 0, nn = size; i < nn; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
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

                // add new element
                if (size == capacity) { // buffer full
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
                    if (size > 0) {
                        long idx = firstIdx % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            frameSize++;
                            newFirstIdx = idx;
                        }
                    }
                    firstIdx = newFirstIdx;
                }

                if (frameSize >= n) {
                    long nthIdx = (firstIdx + n - 1) % capacity;
                    nthValue = memory.getDouble(startOffset + nthIdx * RECORD_SIZE + Long.BYTES);
                } else {
                    nthValue = Double.NaN;
                }
            }

            mapValue.putLong(0, frameSize);
            mapValue.putLong(1, startOffset);
            mapValue.putLong(2, size);
            mapValue.putLong(3, capacity);
            mapValue.putLong(4, firstIdx);
        }

        @Override
        public double getDouble(Record rec) {
            return nthValue;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void reopen() {
            super.reopen();
            nthValue = Double.NaN;
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
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" range between ");
            sink.val(maxDiff);
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

    // handles nth_value() over (partition by x [order by o] rows between y and z)
    // removable cumulative aggregation
    public static class NthValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        protected final int bufferSize;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final int frameSize;
        protected final MemoryARW memory;
        protected final int n;
        protected double nthValue;

        public NthValueOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int n
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
            this.frameIncludesCurrentValue = rowsHi == 0;
            this.memory = memory;
            this.n = n;
        }

        @Override
        public void computeNext(Record record) {
            // map stores:
            // 0 - (0-based) index of oldest value [0, bufferSize]
            // 1 - native array start offset (relative to memory address)
            // 2 - count of values in buffer

            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            long loIdx;
            long startOffset;
            long count;
            double d = arg.getDouble(record);

            if (value.isNew()) {
                loIdx = 0;
                count = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Double.BYTES) - memory.getPageAddress(0);
                value.putLong(1, startOffset);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putDouble(startOffset + (long) i * Double.BYTES, Double.NaN);
                }
            } else {
                loIdx = value.getLong(0);
                startOffset = value.getLong(1);
                count = value.getLong(2);

                if (!frameLoBounded && count >= bufferSize + n - 1) {
                    // frame starts from the very beginning and nth value is already locked in
                    long nthIdx = (loIdx + bufferSize - count) % bufferSize;
                    nthIdx = (nthIdx + n - 1) % bufferSize;
                    nthValue = memory.getDouble(startOffset + nthIdx * Double.BYTES);
                    return;
                }
            }

            // compute current frame size: how many elements are in the frame
            long effectiveCount = Math.min(count, bufferSize);
            long currentFrameSize;
            if (frameLoBounded) {
                currentFrameSize = Math.min(effectiveCount, frameSize);
                if (count == 0 && frameIncludesCurrentValue) {
                    // current row enters the frame
                    currentFrameSize = 1;
                }
            } else {
                // unbounded preceding: frame grows with each row
                currentFrameSize = effectiveCount;
                if (frameIncludesCurrentValue) {
                    currentFrameSize = count + 1;
                }
            }

            if (frameLoBounded) {
                if (count >= (bufferSize - frameSize) && count > 0) {
                    // we have elements in the frame; the first frame element is at
                    long frameStartIdx = (loIdx + bufferSize - effectiveCount) % bufferSize;
                    currentFrameSize = Math.min(effectiveCount, frameSize);
                    if (frameIncludesCurrentValue) {
                        // d is about to be written at loIdx, but hasn't been written yet
                        // frame includes current row
                        if (n <= currentFrameSize) {
                            long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                            nthValue = memory.getDouble(startOffset + nthIdx * Double.BYTES);
                        } else if (n == currentFrameSize + 1) {
                            nthValue = d;
                        } else {
                            nthValue = Double.NaN;
                        }
                    } else {
                        if (n <= currentFrameSize) {
                            long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                            nthValue = memory.getDouble(startOffset + nthIdx * Double.BYTES);
                        } else {
                            nthValue = Double.NaN;
                        }
                    }
                } else if (count == 0 && frameIncludesCurrentValue) {
                    nthValue = n == 1 ? d : Double.NaN;
                } else {
                    nthValue = Double.NaN;
                }
            } else {
                // unbounded preceding
                if (count == 0 && frameIncludesCurrentValue) {
                    nthValue = n == 1 ? d : Double.NaN;
                } else if (count > (bufferSize - frameSize)) {
                    long frameStartIdx = (loIdx + bufferSize - effectiveCount) % bufferSize;
                    if (n <= effectiveCount) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        nthValue = memory.getDouble(startOffset + nthIdx * Double.BYTES);
                    } else if (frameIncludesCurrentValue && n == effectiveCount + 1) {
                        nthValue = d;
                    } else {
                        nthValue = Double.NaN;
                    }
                } else {
                    nthValue = Double.NaN;
                }
            }

            count = Math.min(count + 1, bufferSize);
            value.putLong(0, (loIdx + 1) % bufferSize);
            value.putLong(2, count);

            memory.putDouble(startOffset + loIdx * Double.BYTES, d);
        }

        @Override
        public double getDouble(Record rec) {
            return nthValue;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), nthValue);
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
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between ");
            sink.val(bufferSize);
            sink.val(" preceding and ");
            if (frameIncludesCurrentValue) {
                sink.val("current row");
            } else {
                sink.val(bufferSize + 1 - frameSize).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
        }
    }

    // Handles nth_value() over ([order by ts] range between x preceding and [ y preceding | current row ] ); no partition by key
    public static class NthValueOverRangeFrameFunction extends BaseWindowFunction implements Reopenable, WindowDoubleFunction {
        protected final int RECORD_SIZE = Long.BYTES + Double.BYTES;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final long initialCapacity;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final long minDiff;
        protected final int n;
        protected final int timestampIndex;
        protected long capacity;
        protected long firstIdx;
        protected long frameSize;
        protected double nthValue;
        protected long size;
        protected long startOffset;

        public NthValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int n
        ) {
            super(arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            minDiff = Math.abs(rangeHi);
            timestampIndex = timestampIdx;
            initialCapacity = configuration.getSqlWindowStorePageSize() / RECORD_SIZE;

            capacity = initialCapacity;
            memory = Vm.getCARWInstance(
                    configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowStoreMaxPages(),
                    MemoryTag.NATIVE_CIRCULAR_BUFFER
            );
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            frameIncludesCurrentValue = rangeHi == 0;
            this.n = n;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            if (!frameLoBounded && frameSize >= n) {
                long nthIdx = (firstIdx + n - 1) % capacity;
                nthValue = memory.getDouble(startOffset + nthIdx * RECORD_SIZE + Long.BYTES);
                return;
            }

            long timestamp = record.getTimestamp(timestampIndex);
            double d = arg.getDouble(record);

            long newFirstIdx = firstIdx;

            if (frameLoBounded) {
                // find new bottom border of range frame and remove unneeded elements
                for (long i = 0, nn = size; i < nn; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
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

            // add new element
            if (size == capacity) { // buffer full
                long newAddress = memory.appendAddressFor(capacity * RECORD_SIZE);
                // call above can end up resizing and thus changing memory start address
                long oldAddress = memory.getPageAddress(0) + startOffset;

                if (firstIdx == 0) {
                    Vect.memcpy(newAddress, oldAddress, size * RECORD_SIZE);
                } else {
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

            // find new top border of range frame and add new elements
            if (frameLoBounded) {
                for (long i = frameSize, nn = size; i < nn; i++) {
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
                if (size > 0) {
                    long idx = firstIdx % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        frameSize++;
                        newFirstIdx = idx;
                    }
                }
                firstIdx = newFirstIdx;
            }

            if (frameSize >= n) {
                long nthIdx = (firstIdx + n - 1) % capacity;
                nthValue = memory.getDouble(startOffset + nthIdx * RECORD_SIZE + Long.BYTES);
            } else {
                nthValue = Double.NaN;
            }
        }

        @Override
        public double getDouble(Record rec) {
            return nthValue;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void reopen() {
            nthValue = Double.NaN;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
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
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("range between ");
            sink.val(maxDiff);
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
            nthValue = Double.NaN;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
        }
    }

    // Handles nth_value() over ([order by o] rows between y and z); there's no partition by.
    // Removable cumulative aggregation.
    public static class NthValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable, WindowDoubleFunction {
        protected final MemoryARW buffer;
        protected final int bufferSize;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final int frameSize;
        protected final int n;
        protected long count = 0;
        protected int loIdx = 0;
        protected double nthValue;

        public NthValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int n) {
            super(arg);

            assert rowsLo != Long.MIN_VALUE || rowsHi != 0; // use NthValueOverUnboundedRowsFrameFunction for (Long.MIN_VALUE, 0)

            if (rowsLo > Long.MIN_VALUE) {
                frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
                bufferSize = (int) Math.abs(rowsLo);
                frameLoBounded = true;
            } else {
                frameSize = (int) Math.abs(rowsHi);
                bufferSize = frameSize;
                frameLoBounded = false;
            }

            frameIncludesCurrentValue = rowsHi == 0;
            this.buffer = memory;
            this.n = n;
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
            long effectiveCount = Math.min(count, bufferSize);

            if (!frameLoBounded && count >= bufferSize + n - 1) {
                // frame start is locked in
                long frameStartIdx = (loIdx + bufferSize - effectiveCount) % bufferSize;
                long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                nthValue = buffer.getDouble((long) nthIdx * Double.BYTES);
            } else if (frameLoBounded) {
                long currentFrameElements = Math.min(effectiveCount, frameSize);
                if (count >= (bufferSize - frameSize) && count > 0) {
                    long frameStartIdx = (loIdx + bufferSize - effectiveCount) % bufferSize;
                    if (frameIncludesCurrentValue) {
                        if (n <= currentFrameElements) {
                            long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                            nthValue = buffer.getDouble((long) nthIdx * Double.BYTES);
                        } else if (n == currentFrameElements + 1) {
                            nthValue = d;
                        } else {
                            nthValue = Double.NaN;
                        }
                    } else {
                        if (n <= currentFrameElements) {
                            long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                            nthValue = buffer.getDouble((long) nthIdx * Double.BYTES);
                        } else {
                            nthValue = Double.NaN;
                        }
                    }
                } else if (count == 0 && frameIncludesCurrentValue) {
                    nthValue = n == 1 ? d : Double.NaN;
                } else {
                    nthValue = Double.NaN;
                }
            } else {
                // not frameLoBounded, not enough elements yet
                if (count == 0 && frameIncludesCurrentValue) {
                    nthValue = n == 1 ? d : Double.NaN;
                } else if (count > (bufferSize - frameSize)) {
                    long frameStartIdx = (loIdx + bufferSize - effectiveCount) % bufferSize;
                    if (n <= effectiveCount) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        nthValue = buffer.getDouble((long) nthIdx * Double.BYTES);
                    } else if (frameIncludesCurrentValue && n == effectiveCount + 1) {
                        nthValue = d;
                    } else {
                        nthValue = Double.NaN;
                    }
                } else {
                    nthValue = Double.NaN;
                }
            }

            count = Math.min(count + 1, bufferSize);

            // overwrite oldest element
            buffer.putDouble((long) loIdx * Double.BYTES, d);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public double getDouble(Record rec) {
            return nthValue;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void reopen() {
            nthValue = Double.NaN;
            loIdx = 0;
            initBuffer();
            count = 0;
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            nthValue = Double.NaN;
            loIdx = 0;
            count = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val(" rows between ");
            sink.val(bufferSize);
            sink.val(" preceding and ");
            if (frameIncludesCurrentValue) {
                sink.val("current row");
            } else {
                sink.val(bufferSize + 1 - frameSize).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            nthValue = Double.NaN;
            loIdx = 0;
            initBuffer();
            count = 0;
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putDouble((long) i * Double.BYTES, Double.NaN);
            }
        }
    }

    // Handles:
    // - nth_value(a, n) over (partition by x rows between unbounded preceding and [current row | x preceding])
    // - nth_value(a, n) over (partition by x order by ts range between unbounded preceding and [current row | x preceding])
    static class NthValueOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        protected final int n;
        protected double value;

        public NthValueOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int n) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.n = n;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            if (mapValue.isNew()) {
                // first row: count = 1
                if (n == 1) {
                    double d = arg.getDouble(record);
                    mapValue.putDouble(0, d);
                    value = d;
                } else {
                    mapValue.putDouble(0, Double.NaN);
                    value = Double.NaN;
                }
                mapValue.putLong(1, 1);
            } else {
                long count = mapValue.getLong(1);
                if (count >= n) {
                    // nth value already found
                    value = mapValue.getDouble(0);
                } else {
                    count++;
                    mapValue.putLong(1, count);
                    if (count == n) {
                        double d = arg.getDouble(record);
                        mapValue.putDouble(0, d);
                        value = d;
                    } else {
                        value = Double.NaN;
                    }
                }
            }
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
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row)");
        }
    }

    // handles nth_value() over () or nth_value() over (rows between unbounded preceding and current row); no partition by
    // Counts rows; when count reaches n, stores the value. Emits stored value on all subsequent rows.
    public static class NthValueOverUnboundedRowsFrameFunction extends BaseWindowFunction implements WindowDoubleFunction {
        protected final int n;
        protected long count;
        protected boolean found;
        protected double value = Double.NaN;

        public NthValueOverUnboundedRowsFrameFunction(Function arg, int n) {
            super(arg);
            this.n = n;
        }

        @Override
        public void computeNext(Record record) {
            if (!found) {
                count++;
                if (count == n) {
                    value = arg.getDouble(record);
                    found = true;
                }
            }
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
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            count = 0;
            found = false;
            value = Double.NaN;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            count = 0;
            found = false;
            value = Double.NaN;
        }
    }

    // handles nth_value() over () with default frame and no order by
    // all rows are in one partition; TWO_PASS: pass1 finds nth value, pass2 writes it
    public static class NthValueOverWholeResultSetFunction extends BaseWindowFunction implements WindowDoubleFunction {
        protected final int n;
        protected long count;
        protected boolean found;
        protected double value = Double.NaN;

        public NthValueOverWholeResultSetFunction(Function arg, int n) {
            super(arg);
            this.n = n;
        }

        @Override
        public void computeNext(Record record) {
            if (!found) {
                count++;
                if (count == n) {
                    value = arg.getDouble(record);
                    found = true;
                }
            }
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
            return WindowFunction.TWO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!found) {
                count++;
                if (count == n) {
                    value = arg.getDouble(record);
                    found = true;
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            count = 0;
            found = false;
            value = Double.NaN;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over ()");
        }

        @Override
        public void toTop() {
            super.toTop();
            count = 0;
            found = false;
            value = Double.NaN;
        }
    }

    static {
        NTH_VALUE_COLUMN_TYPES = new ArrayColumnTypes();
        NTH_VALUE_COLUMN_TYPES.add(ColumnType.DOUBLE);
        NTH_VALUE_COLUMN_TYPES.add(ColumnType.LONG); // row count within partition
    }
}

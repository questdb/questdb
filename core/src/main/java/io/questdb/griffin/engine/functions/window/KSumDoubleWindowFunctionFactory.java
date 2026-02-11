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
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

/**
 * Kahan summation window function.
 * Uses the Kahan summation algorithm for improved floating-point precision.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Kahan_summation_algorithm">Kahan summation algorithm</a>
 */
public class KSumDoubleWindowFunctionFactory extends AbstractWindowFunctionFactory {

    // Column types for partition-based functions: sum, compensation, count
    private static final ArrayColumnTypes KSUM_COLUMN_TYPES;
    // Column types for partition range frame: sum, compensation, frameSize, startOffset, size, capacity, firstIdx
    private static final ArrayColumnTypes KSUM_OVER_PARTITION_RANGE_COLUMN_TYPES;
    // Column types for partition rows frame: sum, compensation, count, loIdx, startOffset
    private static final ArrayColumnTypes KSUM_OVER_PARTITION_ROWS_COLUMN_TYPES;
    private static final String NAME = "ksum";
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
                // ksum over whole partition (no order by, default frame) or (order by, unbounded preceding to unbounded following)
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            KSUM_COLUMN_TYPES
                    );

                    return new KSumOverPartitionFunction(
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
                            KSUM_COLUMN_TYPES
                    );

                    return new KSumOverUnboundedPartitionRowsFrameFunction(
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

                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                KSUM_OVER_PARTITION_RANGE_COLUMN_TYPES
                        );
                        mem = Vm.getCARWInstance(
                                configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(),
                                MemoryTag.NATIVE_CIRCULAR_BUFFER
                        );

                        return new KSumOverPartitionRangeFrameFunction(
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
                            KSUM_COLUMN_TYPES
                    );

                    return new KSumOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0)
                    );
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new KSumOverCurrentRowFunction(args.get(0));
                } // whole partition
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            KSUM_COLUMN_TYPES
                    );

                    return new KSumOverPartitionFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0)
                    );
                }
                // between [unbounded | x] preceding and [x preceding | current row]
                else {
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                KSUM_OVER_PARTITION_ROWS_COLUMN_TYPES
                        );
                        mem = Vm.getCARWInstance(
                                configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(),
                                MemoryTag.NATIVE_CIRCULAR_BUFFER
                        );

                        return new KSumOverPartitionRowsFrameFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                rowsLo,
                                rowsHi,
                                args.get(0),
                                mem
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
                    return new KSumOverWholeResultSetFunction(args.get(0));
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new KSumOverUnboundedRowsFrameFunction(args.get(0));
                } // range between [unbounded | x] preceding and [x preceding | current row]
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }

                    int timestampIndex = windowContext.getTimestampIndex();

                    return new KSumOverRangeFrameFunction(
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
                    return new KSumOverUnboundedRowsFrameFunction(args.get(0));
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new KSumOverCurrentRowFunction(args.get(0));
                } // whole result set
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new KSumOverWholeResultSetFunction(args.get(0));
                } // between [unbounded | x] preceding and [x preceding | current row]
                else {
                    MemoryARW mem = Vm.getCARWInstance(
                            configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(),
                            MemoryTag.NATIVE_CIRCULAR_BUFFER
                    );
                    try {
                        return new KSumOverRowsFrameFunction(
                                args.get(0),
                                rowsLo,
                                rowsHi,
                                mem
                        );
                    } catch (Throwable th) {
                        Misc.free(mem);
                        throw th;
                    }
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    // (rows between current row and current row) processes 1-element-big set, so simply returns expression value
    static class KSumOverCurrentRowFunction extends BaseWindowFunction implements WindowDoubleFunction {
        private double value;

        KSumOverCurrentRowFunction(Function arg) {
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    // handles ksum() over (partition by x)
    // order by is absent so default frame mode includes all rows in partition
    static class KSumOverPartitionFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        public KSumOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
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
                double c;

                if (value.isNew()) {
                    count = 1;
                    sum = d;
                    c = 0.0;
                } else {
                    sum = value.getDouble(0);
                    c = value.getDouble(1);
                    count = value.getLong(2) + 1;
                    // Kahan addition
                    double y = d - c;
                    double t = sum + y;
                    c = (t - sum) - y;
                    sum = t;
                }
                value.putDouble(0, sum);
                value.putDouble(1, c);
                value.putLong(2, count);
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
            // No-op: map entries are only created when there's at least one finite value,
            // so no fixup is needed. Partitions with all NULLs have no map entry and
            // pass2() handles that by returning NaN.
        }
    }

    // Handles ksum() over (partition by x order by ts range between [unbounded | y] preceding and [z preceding | current row])
    static class KSumOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        private static final int RECORD_SIZE = Long.BYTES + Double.BYTES;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final LongList freeList = new LongList();
        private final int initialBufferSize;
        private final long maxDiff;
        private final MemoryARW memory;
        private final RingBufferDesc memoryDesc = new RingBufferDesc();
        private final long minDiff;
        private final int timestampIndex;
        private double sum;

        public KSumOverPartitionRangeFrameFunction(
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
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
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
            // map stores:
            // 0 - sum
            // 1 - compensation (c)
            // 2 - current number of non-null rows in frame
            // 3 - native array start offset
            // 4 - size of ring buffer
            // 5 - capacity of ring buffer
            // 6 - index of first valid buffer element

            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            double sum;
            double c;
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
                        c = 0.0;
                        this.sum = d;
                        frameSize = 1;
                        size = frameLoBounded ? 1 : 0;
                    } else {
                        sum = 0.0;
                        c = 0.0;
                        this.sum = Double.NaN;
                        frameSize = 0;
                        size = 1;
                    }
                } else {
                    size = 0;
                    sum = 0.0;
                    c = 0.0;
                    this.sum = Double.NaN;
                    frameSize = 0;
                }
            } else {
                sum = mapValue.getDouble(0);
                c = mapValue.getDouble(1);
                frameSize = mapValue.getLong(2);
                startOffset = mapValue.getLong(3);
                size = mapValue.getLong(4);
                capacity = mapValue.getLong(5);
                firstIdx = mapValue.getLong(6);

                long newFirstIdx = firstIdx;

                if (frameLoBounded) {
                    // find new bottom border of range frame and remove unneeded elements
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
                            if (frameSize > 0) {
                                double val = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                                // Kahan subtraction
                                double y = -val - c;
                                double t = sum + y;
                                c = (t - sum) - y;
                                sum = t;
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
                    if (size == capacity) {
                        memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                        expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                        capacity = memoryDesc.capacity;
                        startOffset = memoryDesc.startOffset;
                        firstIdx = memoryDesc.firstIdx;
                    }

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
                            double val = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            // Kahan addition
                            double y = val - c;
                            double t = sum + y;
                            c = (t - sum) - y;
                            sum = t;
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
                            // Kahan addition
                            double y = val - c;
                            double t = sum + y;
                            c = (t - sum) - y;
                            sum = t;
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
                    this.sum = sum;
                } else {
                    this.sum = Double.NaN;
                }
            }

            mapValue.putDouble(0, sum);
            mapValue.putDouble(1, c);
            mapValue.putLong(2, frameSize);
            mapValue.putLong(3, startOffset);
            mapValue.putLong(4, size);
            mapValue.putLong(5, capacity);
            mapValue.putLong(6, firstIdx);
        }

        @Override
        public double getDouble(Record rec) {
            return sum;
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
            // pass1 is never called when getPassCount() returns ZERO_PASS
            throw new UnsupportedOperationException();
        }

        @Override
        public void reopen() {
            super.reopen();
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

    // handles ksum() over (partition by x [order by o] rows between y and z)
    static class KSumOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final MemoryARW memory;
        private double sum;

        public KSumOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory
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
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            // map stores:
            // 0 - sum
            // 1 - compensation (c)
            // 2 - current number of non-null rows in frame
            // 3 - (0-based) index of oldest value [0, bufferSize]
            // 4 - native array start offset

            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            long count;
            double sum;
            double c;
            long loIdx;
            long startOffset;
            double d = arg.getDouble(record);

            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Double.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && Numbers.isFinite(d)) {
                    sum = d;
                    c = 0.0;
                    count = 1;
                    this.sum = d;
                } else {
                    sum = 0.0;
                    c = 0.0;
                    this.sum = Double.NaN;
                    count = 0;
                }

                for (int i = 0; i < bufferSize; i++) {
                    memory.putDouble(startOffset + (long) i * Double.BYTES, Double.NaN);
                }
            } else {
                sum = value.getDouble(0);
                c = value.getDouble(1);
                count = value.getLong(2);
                loIdx = value.getLong(3);
                startOffset = value.getLong(4);

                // compute value using top frame element
                double hiValue = frameIncludesCurrentValue ? d : memory.getDouble(startOffset + ((loIdx + frameSize - 1) % bufferSize) * Double.BYTES);
                if (Numbers.isFinite(hiValue)) {
                    count++;
                    // Kahan addition
                    double y = hiValue - c;
                    double t = sum + y;
                    c = (t - sum) - y;
                    sum = t;
                }

                if (count != 0) {
                    this.sum = sum;
                } else {
                    this.sum = Double.NaN;
                }

                if (frameLoBounded) {
                    // remove the oldest element
                    double loValue = memory.getDouble(startOffset + loIdx * Double.BYTES);
                    if (Numbers.isFinite(loValue)) {
                        // Kahan subtraction
                        double y = -loValue - c;
                        double t = sum + y;
                        c = (t - sum) - y;
                        sum = t;
                        count--;
                    }
                }
            }

            value.putDouble(0, sum);
            value.putDouble(1, c);
            value.putLong(2, count);
            value.putLong(3, (loIdx + 1) % bufferSize);
            value.putLong(4, startOffset);
            memory.putDouble(startOffset + loIdx * Double.BYTES, d);
        }

        @Override
        public double getDouble(Record rec) {
            return sum;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), sum);
        }

        @Override
        public void reopen() {
            super.reopen();
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

    // Handles ksum() over ([order by ts] range between ...)
    static class KSumOverRangeFrameFunction extends BaseWindowFunction implements Reopenable, WindowDoubleFunction {
        private static final int RECORD_SIZE = Long.BYTES + Double.BYTES;
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final long maxDiff;
        private final MemoryARW memory;
        private final long minDiff;
        private final int timestampIndex;
        private double c; // Kahan compensation
        private long capacity;
        private double externalSum;
        private long firstIdx;
        private long frameSize;
        private long size;
        private long startOffset;
        private double sum;

        public KSumOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx
        ) {
            super(arg);
            this.initialCapacity = configuration.getSqlWindowStorePageSize() / RECORD_SIZE;
            this.memory = Vm.getCARWInstance(
                    configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowStoreMaxPages(),
                    MemoryTag.NATIVE_CIRCULAR_BUFFER
            );
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            minDiff = Math.abs(rangeHi);
            timestampIndex = timestampIdx;

            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            sum = 0.0;
            c = 0.0;
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
                        if (frameSize > 0) {
                            double val = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            // Kahan subtraction
                            double y = -val - c;
                            double t = sum + y;
                            c = (t - sum) - y;
                            sum = t;
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
                // Buffer should never fill up in non-partitioned range frame because
                // removals keep size bounded by window size, which is <= initial capacity
                assert size < capacity : "buffer overflow in KSumOverRangeFrameFunction";

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
                        double val = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        // Kahan addition
                        double y = val - c;
                        double t = sum + y;
                        c = (t - sum) - y;
                        sum = t;
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
                        // Kahan addition
                        double y = val - c;
                        double t = sum + y;
                        c = (t - sum) - y;
                        sum = t;
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
                externalSum = sum;
            } else {
                externalSum = Double.NaN;
            }
        }

        @Override
        public double getDouble(Record rec) {
            return externalSum;
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
            // pass1 is never called when getPassCount() returns ZERO_PASS
            throw new UnsupportedOperationException();
        }

        @Override
        public void reopen() {
            externalSum = Double.NaN;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            sum = 0.0;
            c = 0.0;
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
            externalSum = Double.NaN;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            sum = 0.0;
            c = 0.0;
        }
    }

    // Handles ksum() over ([order by o] rows between y and z); no partition by
    static class KSumOverRowsFrameFunction extends BaseWindowFunction implements Reopenable, WindowDoubleFunction {
        private final MemoryARW buffer;
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private double c = 0.0; // Kahan compensation
        private long count = 0;
        private double externalSum = 0;
        private int loIdx = 0;
        private double sum = 0.0;

        public KSumOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory) {
            super(arg);

            assert rowsLo != Long.MIN_VALUE || rowsHi != 0;
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

            // compute value using top frame element
            double hiValue = d;
            if (frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getDouble((long) ((loIdx + frameSize - 1) % bufferSize) * Double.BYTES);
            } else if (!frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getDouble((long) (loIdx % bufferSize) * Double.BYTES);
            }
            if (Numbers.isFinite(hiValue)) {
                // Kahan addition
                double y = hiValue - c;
                double t = sum + y;
                c = (t - sum) - y;
                sum = t;
                count++;
            }
            if (count != 0) {
                externalSum = sum;
            } else {
                externalSum = Double.NaN;
            }

            if (frameLoBounded) {
                // remove the oldest element
                double loValue = buffer.getDouble((long) loIdx * Double.BYTES);
                if (Numbers.isFinite(loValue)) {
                    // Kahan subtraction
                    double y = -loValue - c;
                    double t = sum + y;
                    c = (t - sum) - y;
                    sum = t;
                    count--;
                }
            }

            // overwrite oldest element
            buffer.putDouble((long) loIdx * Double.BYTES, d);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public double getDouble(Record rec) {
            return externalSum;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), externalSum);
        }

        @Override
        public void reopen() {
            externalSum = 0;
            count = 0;
            loIdx = 0;
            sum = 0.0;
            c = 0.0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            externalSum = 0;
            count = 0;
            loIdx = 0;
            sum = 0.0;
            c = 0.0;
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
            externalSum = 0;
            count = 0;
            loIdx = 0;
            sum = 0.0;
            c = 0.0;
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putDouble((long) i * Double.BYTES, Double.NaN);
            }
        }
    }

    // Handles ksum() over (partition by x rows between unbounded preceding and current row)
    static class KSumOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        private double sum;

        public KSumOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            double sum;
            double c;
            long count;

            if (value.isNew()) {
                sum = 0;
                c = 0;
                count = 0;
            } else {
                sum = value.getDouble(0);
                c = value.getDouble(1);
                count = value.getLong(2);
            }

            double d = arg.getDouble(record);
            if (Numbers.isFinite(d)) {
                // Kahan addition
                double y = d - c;
                double t = sum + y;
                c = (t - sum) - y;
                sum = t;
                count++;
            }

            value.putDouble(0, sum);
            value.putDouble(1, c);
            value.putLong(2, count);
            this.sum = count != 0 ? sum : Double.NaN;
        }

        @Override
        public double getDouble(Record rec) {
            return sum;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), sum);
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

    // Handles ksum() over (rows between unbounded preceding and current row); no partition by
    static class KSumOverUnboundedRowsFrameFunction extends BaseWindowFunction implements WindowDoubleFunction {

        private double c = 0.0; // Kahan compensation
        private long count = 0;
        private double externalSum;
        private double sum = 0.0;

        public KSumOverUnboundedRowsFrameFunction(Function arg) {
            super(arg);
        }

        @Override
        public void computeNext(Record record) {
            double d = arg.getDouble(record);
            if (Numbers.isFinite(d)) {
                // Kahan addition
                double y = d - c;
                double t = sum + y;
                c = (t - sum) - y;
                sum = t;
                count++;
            }

            externalSum = count != 0 ? sum : Double.NaN;
        }

        @Override
        public double getDouble(Record rec) {
            return externalSum;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), externalSum);
        }

        @Override
        public void reset() {
            super.reset();
            externalSum = Double.NaN;
            count = 0;
            sum = 0.0;
            c = 0.0;
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
            externalSum = Double.NaN;
            count = 0;
            sum = 0.0;
            c = 0.0;
        }
    }

    // ksum() over () - empty clause, no partition by, no order by, no frame == default frame
    static class KSumOverWholeResultSetFunction extends BaseWindowFunction implements WindowDoubleFunction {
        private double c; // Kahan compensation
        private long count;
        private double externalSum;
        private double sum;

        public KSumOverWholeResultSetFunction(Function arg) {
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
                // Kahan addition
                double y = d - c;
                double t = sum + y;
                c = (t - sum) - y;
                sum = t;
                count++;
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), externalSum);
        }

        @Override
        public void preparePass2() {
            externalSum = count > 0 ? sum : Double.NaN;
        }

        @Override
        public void reset() {
            super.reset();
            externalSum = Double.NaN;
            count = 0;
            sum = 0.0;
            c = 0.0;
        }

        @Override
        public void toTop() {
            super.toTop();
            externalSum = Double.NaN;
            count = 0;
            sum = 0.0;
            c = 0.0;
        }
    }

    static {
        KSUM_COLUMN_TYPES = new ArrayColumnTypes();
        KSUM_COLUMN_TYPES.add(ColumnType.DOUBLE); // sum
        KSUM_COLUMN_TYPES.add(ColumnType.DOUBLE); // compensation (c)
        KSUM_COLUMN_TYPES.add(ColumnType.LONG);   // count

        KSUM_OVER_PARTITION_ROWS_COLUMN_TYPES = new ArrayColumnTypes();
        KSUM_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.DOUBLE); // sum
        KSUM_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.DOUBLE); // compensation (c)
        KSUM_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG);   // count
        KSUM_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG);   // loIdx
        KSUM_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG);   // startOffset

        KSUM_OVER_PARTITION_RANGE_COLUMN_TYPES = new ArrayColumnTypes();
        KSUM_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.DOUBLE); // sum
        KSUM_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.DOUBLE); // compensation (c)
        KSUM_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG);   // frameSize
        KSUM_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG);   // startOffset
        KSUM_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG);   // size
        KSUM_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG);   // capacity
        KSUM_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG);   // firstIdx
    }
}

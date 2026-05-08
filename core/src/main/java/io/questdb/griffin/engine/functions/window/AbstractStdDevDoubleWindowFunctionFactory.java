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
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
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
import io.questdb.std.Vect;

public abstract class AbstractStdDevDoubleWindowFunctionFactory extends AbstractWindowFunctionFactory {

    private static final ArrayColumnTypes STDDEV_COLUMN_TYPES;
    private static final ArrayColumnTypes STDDEV_OVER_PARTITION_RANGE_COLUMN_TYPES;
    private static final ArrayColumnTypes STDDEV_OVER_PARTITION_ROWS_COLUMN_TYPES;

    // Naive sum-of-squares formula, used by sliding-window (removable) frames.
    static double computeResult(double sum, double sumSq, long count, boolean isSample, boolean isSqrt) {
        long denom = isSample ? count - 1 : count;
        if (denom <= 0) {
            return Double.NaN;
        }
        double variance = (sumSq - (sum * sum) / count) / denom;
        if (variance < 0) {
            variance = 0.0;
        }
        return isSqrt ? Math.sqrt(variance) : variance;
    }

    // Welford's online algorithm result, used by non-removable (running/whole) frames.
    // m2 is the running sum of squared deviations from the running mean.
    static double computeResultWelford(double m2, long count, boolean isSample, boolean isSqrt) {
        long denom = isSample ? count - 1 : count;
        if (denom <= 0) {
            return Double.NaN;
        }
        double variance = m2 / denom;
        if (variance < 0) {
            variance = 0.0;
        }
        return isSqrt ? Math.sqrt(variance) : variance;
    }

    protected abstract boolean isSample();

    protected abstract boolean isSqrt();

    protected abstract String name();

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

        boolean isSample = isSample();
        boolean isSqrt = isSqrt();
        String name = name();

        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        if (rowsHi < rowsLo) {
            return new DoubleNullFunction(
                    args.get(0),
                    name,
                    rowsLo,
                    rowsHi,
                    framingMode == WindowExpression.FRAMING_RANGE,
                    partitionByRecord
            );
        }

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                // whole partition (no order by, default frame) or (order by, unbounded preceding to unbounded following)
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            STDDEV_COLUMN_TYPES
                    );

                    return new StdDevOverPartitionFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            isSample,
                            isSqrt,
                            name
                    );
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            STDDEV_COLUMN_TYPES
                    );

                    return new StdDevOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            isSample,
                            isSqrt,
                            name
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
                                STDDEV_OVER_PARTITION_RANGE_COLUMN_TYPES
                        );
                        mem = Vm.getCARWInstance(
                                configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(),
                                MemoryTag.NATIVE_CIRCULAR_BUFFER
                        );

                        return new StdDevOverPartitionRangeFrameFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                rowsLo,
                                rowsHi,
                                args.get(0),
                                mem,
                                configuration.getSqlWindowInitialRangeBufferSize(),
                                timestampIndex,
                                isSample,
                                isSqrt,
                                name
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
                            STDDEV_COLUMN_TYPES
                    );

                    return new StdDevOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            isSample,
                            isSqrt,
                            name
                    );
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new StdDevOverCurrentRowFunction(args.get(0), isSample, name);
                } // whole partition
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            STDDEV_COLUMN_TYPES
                    );

                    return new StdDevOverPartitionFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            isSample,
                            isSqrt,
                            name
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
                                STDDEV_OVER_PARTITION_ROWS_COLUMN_TYPES
                        );
                        mem = Vm.getCARWInstance(
                                configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(),
                                MemoryTag.NATIVE_CIRCULAR_BUFFER
                        );

                        return new StdDevOverPartitionRowsFrameFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                rowsLo,
                                rowsHi,
                                args.get(0),
                                mem,
                                isSample,
                                isSqrt,
                                name
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
                    return new StdDevOverWholeResultSetFunction(args.get(0), isSample, isSqrt, name);
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new StdDevOverUnboundedRowsFrameFunction(args.get(0), isSample, isSqrt, name);
                } // range between [unbounded | x] preceding and [x preceding | current row]
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }

                    int timestampIndex = windowContext.getTimestampIndex();

                    return new StdDevOverRangeFrameFunction(
                            rowsLo,
                            rowsHi,
                            args.get(0),
                            configuration,
                            timestampIndex,
                            isSample,
                            isSqrt,
                            name
                    );
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                // between unbounded preceding and current row
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new StdDevOverUnboundedRowsFrameFunction(args.get(0), isSample, isSqrt, name);
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new StdDevOverCurrentRowFunction(args.get(0), isSample, name);
                } // whole result set
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new StdDevOverWholeResultSetFunction(args.get(0), isSample, isSqrt, name);
                } // between [unbounded | x] preceding and [x preceding | current row]
                else {
                    MemoryARW mem = Vm.getCARWInstance(
                            configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(),
                            MemoryTag.NATIVE_CIRCULAR_BUFFER
                    );

                    return new StdDevOverRowsFrameFunction(
                            args.get(0),
                            rowsLo,
                            rowsHi,
                            mem,
                            isSample,
                            isSqrt,
                            name
                    );
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    // (rows between current row and current row) processes a 1-element set.
    // Population stddev of a single value is 0; sample stddev is undefined (NaN).
    static class StdDevOverCurrentRowFunction extends BaseWindowFunction implements WindowDoubleFunction {
        private final boolean isSample;
        private final String name;
        private double value;

        StdDevOverCurrentRowFunction(Function arg, boolean isSample, String name) {
            super(arg);
            this.isSample = isSample;
            this.name = name;
        }

        @Override
        public void computeNext(Record record) {
            final double d = arg.getDouble(record);
            value = Numbers.isFinite(d) ? (isSample ? Double.NaN : 0.0) : Double.NaN;
        }

        @Override
        public double getDouble(Record rec) {
            return value;
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name);
            sink.val('(').val(arg).val(')');
            sink.val(" over (rows between current row and current row)");
        }
    }

    // Handles stddev() over (partition by x), order by absent or whole-partition frame.
    static class StdDevOverPartitionFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {
        private final boolean isSample;
        private final boolean isSqrt;
        private final String name;

        StdDevOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, boolean isSample, boolean isSqrt, String name) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.isSample = isSample;
            this.isSqrt = isSqrt;
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            // Welford's online algorithm: map stores [0]=mean, [1]=m2, [2]=count
            double d = arg.getDouble(record);
            if (Numbers.isFinite(d)) {
                partitionByRecord.of(record);
                MapKey key = map.withKey();
                key.put(partitionByRecord, partitionBySink);
                MapValue value = key.createValue();

                if (value.isNew()) {
                    value.putDouble(0, d);
                    value.putDouble(1, 0.0);
                    value.putLong(2, 1);
                } else {
                    long count = value.getLong(2) + 1;
                    double oldMean = value.getDouble(0);
                    double newMean = oldMean + (d - oldMean) / count;
                    double m2 = value.getDouble(1) + (d - newMean) * (d - oldMean);
                    value.putDouble(0, newMean);
                    value.putDouble(1, m2);
                    value.putLong(2, count);
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();

            final double stdDev = value != null ? value.getDouble(0) : Double.NaN;
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), stdDev);
        }

        @Override
        public void preparePass2() {
            RecordCursor cursor = map.getCursor();
            MapRecord record = map.getRecord();
            while (cursor.hasNext()) {
                MapValue value = record.getValue();
                double m2 = value.getDouble(1);
                long count = value.getLong(2);
                value.putDouble(0, computeResultWelford(m2, count, isSample, isSqrt));
            }
        }
    }

    // Handles stddev() over (partition by x order by ts range between [unbounded | y] preceding and [z preceding | current row]).
    // Removable cumulative aggregation with timestamp & value stored in resizable ring buffers.
    static class StdDevOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        private static final int RECORD_SIZE = Long.BYTES + Double.BYTES;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        // list of [size, startOffset] pairs marking free space within mem
        private final LongList freeList = new LongList();
        private final int initialBufferSize;
        private final boolean isSample;
        private final boolean isSqrt;
        private final long maxDiff;
        private final MemoryARW memory;
        private final RingBufferDesc memoryDesc = new RingBufferDesc();
        private final long minDiff;
        private final String name;
        private final int timestampIndex;
        private double stddev;

        StdDevOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                boolean isSample,
                boolean isSqrt,
                String name
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            frameIncludesCurrentValue = rangeHi == 0;
            this.isSample = isSample;
            this.isSqrt = isSqrt;
            this.name = name;
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
            // 0 - sum, never NaN
            // 1 - sumSq, never NaN
            // 2 - current number of finite values in frame
            // 3 - native array start offset (relative to memory address)
            // 4 - ring buffer size (buffered elements, not all necessarily in frame)
            // 5 - ring buffer capacity
            // 6 - index of first (oldest) valid buffer element
            // ring data: [timestamp, value] pairs; only finite values are stored

            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            double sum;
            double sumSq;
            long count;
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
                        sumSq = d * d;
                        count = 1;
                        stddev = computeResult(sum, sumSq, count, isSample, isSqrt);
                        size = frameLoBounded ? 1 : 0;
                    } else {
                        sum = 0.0;
                        sumSq = 0.0;
                        count = 0;
                        stddev = Double.NaN;
                        size = 1;
                    }
                } else {
                    size = 0;
                    sum = 0.0;
                    sumSq = 0.0;
                    count = 0;
                    stddev = Double.NaN;
                }
            } else {
                sum = mapValue.getDouble(0);
                sumSq = mapValue.getDouble(1);
                count = mapValue.getLong(2);
                startOffset = mapValue.getLong(3);
                size = mapValue.getLong(4);
                capacity = mapValue.getLong(5);
                firstIdx = mapValue.getLong(6);

                long newFirstIdx = firstIdx;

                if (frameLoBounded) {
                    // find new lower bound and evict outdated values
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
                            if (count > 0) {
                                double val = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                                sum -= val;
                                sumSq -= val * val;
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

                // append current finite value to buffer
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

                // include newly entered upper-bound values
                if (frameLoBounded) {
                    for (long i = count; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        long diff = Math.abs(ts - timestamp);

                        if (diff <= maxDiff && diff >= minDiff) {
                            double value = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            sum += value;
                            sumSq += value * value;
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
                            double val = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            sum += val;
                            sumSq += val * val;
                            count++;
                            newFirstIdx = (idx + 1) % capacity;
                            size--;
                        } else {
                            break;
                        }
                    }

                    firstIdx = newFirstIdx;
                }

                stddev = computeResult(sum, sumSq, count, isSample, isSqrt);
            }

            mapValue.putDouble(0, sum);
            mapValue.putDouble(1, sumSq);
            mapValue.putLong(2, count);
            mapValue.putLong(3, startOffset);
            mapValue.putLong(4, size);
            mapValue.putLong(5, capacity);
            mapValue.putLong(6, firstIdx);
        }

        @Override
        public double getDouble(Record rec) {
            return stddev;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), stddev);
        }

        @Override
        public void reopen() {
            super.reopen();
            stddev = Double.NaN;
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

    // Handles stddev() over (partition by x [order by o] rows between y and z).
    // Removable cumulative aggregation.
    static class StdDevOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final boolean isSample;
        private final boolean isSqrt;
        private final MemoryARW memory;
        private final String name;
        private double stddev;

        StdDevOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                boolean isSample,
                boolean isSqrt,
                String name
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
            this.isSample = isSample;
            this.isSqrt = isSqrt;
            this.name = name;
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
            // 1 - sumSq
            // 2 - finite count in frame
            // 3 - (0-based) index of oldest value
            // 4 - native array start offset
            // ring buffer stores raw values including NaN placeholders

            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            double sum;
            double sumSq;
            long count;
            long loIdx;
            long startOffset;
            double d = arg.getDouble(record);

            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Double.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && Numbers.isFinite(d)) {
                    sum = d;
                    sumSq = d * d;
                    count = 1;
                    stddev = computeResult(sum, sumSq, count, isSample, isSqrt);
                } else {
                    sum = 0.0;
                    sumSq = 0.0;
                    count = 0;
                    stddev = Double.NaN;
                }

                for (int i = 0; i < bufferSize; i++) {
                    memory.putDouble(startOffset + (long) i * Double.BYTES, Double.NaN);
                }
            } else {
                sum = value.getDouble(0);
                sumSq = value.getDouble(1);
                count = value.getLong(2);
                loIdx = value.getLong(3);
                startOffset = value.getLong(4);

                double hiValue = frameIncludesCurrentValue ? d : memory.getDouble(startOffset + ((loIdx + frameSize - 1) % bufferSize) * Double.BYTES);
                if (Numbers.isFinite(hiValue)) {
                    count++;
                    sum += hiValue;
                    sumSq += hiValue * hiValue;
                }

                stddev = computeResult(sum, sumSq, count, isSample, isSqrt);

                if (frameLoBounded) {
                    double loValue = memory.getDouble(startOffset + loIdx * Double.BYTES);
                    if (Numbers.isFinite(loValue)) {
                        sum -= loValue;
                        sumSq -= loValue * loValue;
                        count--;
                    }
                }
            }

            value.putDouble(0, sum);
            value.putDouble(1, sumSq);
            value.putLong(2, count);
            value.putLong(3, (loIdx + 1) % bufferSize);
            value.putLong(4, startOffset);
            memory.putDouble(startOffset + loIdx * Double.BYTES, d);
        }

        @Override
        public double getDouble(Record rec) {
            return stddev;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), stddev);
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

    // Handles stddev() over ([order by ts] range between [unbounded | x] preceding and [x preceding | current row]); no partition key.
    static class StdDevOverRangeFrameFunction extends BaseWindowFunction implements Reopenable, WindowDoubleFunction {
        private static final int RECORD_SIZE = Long.BYTES + Double.BYTES;
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final boolean isSample;
        private final boolean isSqrt;
        private final long maxDiff;
        private final MemoryARW memory;
        private final long minDiff;
        private final String name;
        private final int timestampIndex;
        private long capacity;
        private long count;
        private long firstIdx;
        private long size;
        private long startOffset;
        private double stddev;
        private double sum;
        private double sumSq;

        StdDevOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                boolean isSample,
                boolean isSqrt,
                String name
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
                    timestampIdx,
                    isSample,
                    isSqrt,
                    name
            );
        }

        StdDevOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                long initialCapacity,
                MemoryARW memory,
                int timestampIdx,
                boolean isSample,
                boolean isSqrt,
                String name
        ) {
            super(arg);
            this.initialCapacity = initialCapacity;
            this.memory = memory;
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            minDiff = Math.abs(rangeHi);
            timestampIndex = timestampIdx;
            this.isSample = isSample;
            this.isSqrt = isSqrt;
            this.name = name;

            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            count = 0;
            size = 0;
            sum = 0.0;
            sumSq = 0.0;
            stddev = Double.NaN;
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
                // find new lower bound and evict outdated values
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        if (count > 0) {
                            double val = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            sum -= val;
                            sumSq -= val * val;
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

            if (Numbers.isFinite(d)) {
                if (size == capacity) {
                    long newAddress = memory.appendAddressFor((capacity << 1) * RECORD_SIZE);
                    long oldAddress = memory.getPageAddress(0) + startOffset;

                    if (firstIdx == 0) {
                        Vect.memcpy(newAddress, oldAddress, size * RECORD_SIZE);
                    } else {
                        firstIdx %= size;
                        long firstPieceSize = (size - firstIdx) * RECORD_SIZE;
                        Vect.memcpy(newAddress, oldAddress + firstIdx * RECORD_SIZE, firstPieceSize);
                        Vect.memcpy(newAddress + firstPieceSize, oldAddress, firstIdx * RECORD_SIZE);
                        firstIdx = 0;
                    }

                    startOffset = newAddress - memory.getPageAddress(0);
                    capacity <<= 1;
                }

                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                memory.putDouble(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
                size++;
            }

            // include newly entered upper-bound values
            if (frameLoBounded) {
                for (long i = count, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);

                    if (diff <= maxDiff && diff >= minDiff) {
                        double value = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        sum += value;
                        sumSq += value * value;
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
                        double val = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        sum += val;
                        sumSq += val * val;
                        count++;
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        break;
                    }
                }
                firstIdx = newFirstIdx;
            }

            stddev = computeResult(sum, sumSq, count, isSample, isSqrt);
        }

        @Override
        public double getDouble(Record rec) {
            return stddev;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), stddev);
        }

        @Override
        public void reopen() {
            stddev = Double.NaN;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            count = 0;
            size = 0;
            sum = 0.0;
            sumSq = 0.0;
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
            stddev = Double.NaN;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            count = 0;
            size = 0;
            sum = 0.0;
            sumSq = 0.0;
        }
    }

    // Handles stddev() over ([order by o] rows between y and z); no partition key.
    // Removable cumulative aggregation.
    static class StdDevOverRowsFrameFunction extends BaseWindowFunction implements Reopenable, WindowDoubleFunction {
        private final MemoryARW buffer;
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final boolean isSample;
        private final boolean isSqrt;
        private final String name;
        private long count = 0;
        private int loIdx = 0;
        private double stddev = Double.NaN;
        private double sum = 0.0;
        private double sumSq = 0.0;

        StdDevOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, boolean isSample, boolean isSqrt, String name) {
            super(arg);
            this.isSample = isSample;
            this.isSqrt = isSqrt;
            this.name = name;

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

            double hiValue = d;
            if (frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getDouble((long) ((loIdx + frameSize - 1) % bufferSize) * Double.BYTES);
            } else if (!frameLoBounded && !frameIncludesCurrentValue) {
                hiValue = buffer.getDouble((long) (loIdx % bufferSize) * Double.BYTES);
            }

            if (Numbers.isFinite(hiValue)) {
                sum += hiValue;
                sumSq += hiValue * hiValue;
                count++;
            }

            stddev = computeResult(sum, sumSq, count, isSample, isSqrt);

            if (frameLoBounded) {
                double loValue = buffer.getDouble((long) loIdx * Double.BYTES);
                if (Numbers.isFinite(loValue)) {
                    sum -= loValue;
                    sumSq -= loValue * loValue;
                    count--;
                }
            }

            buffer.putDouble((long) loIdx * Double.BYTES, d);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public double getDouble(Record rec) {
            return stddev;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), stddev);
        }

        @Override
        public void reopen() {
            stddev = Double.NaN;
            count = 0;
            loIdx = 0;
            sum = 0.0;
            sumSq = 0.0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            stddev = Double.NaN;
            count = 0;
            loIdx = 0;
            sum = 0.0;
            sumSq = 0.0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(')');
            sink.val(" over (");
            sink.val("rows between ");
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
            stddev = Double.NaN;
            count = 0;
            loIdx = 0;
            sum = 0.0;
            sumSq = 0.0;
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putDouble((long) i * Double.BYTES, Double.NaN);
            }
        }
    }

    // Handles:
    // - stddev(a) over (partition by x rows between unbounded preceding and current row)
    // - stddev(a) over (partition by x order by ts range between unbounded preceding and current row)
    // Doesn't require value buffering.
    static class StdDevOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {
        private final boolean isSample;
        private final boolean isSqrt;
        private final String name;
        private double stddev = Double.NaN;

        StdDevOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, boolean isSample, boolean isSqrt, String name) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.isSample = isSample;
            this.isSqrt = isSqrt;
            this.name = name;
        }

        @Override
        public void computeNext(Record record) {
            // Welford's online algorithm: map stores [0]=mean, [1]=m2, [2]=count
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            double mean;
            double m2;
            long count;

            if (value.isNew()) {
                mean = 0.0;
                m2 = 0.0;
                count = 0;
            } else {
                mean = value.getDouble(0);
                m2 = value.getDouble(1);
                count = value.getLong(2);
            }

            double d = arg.getDouble(record);
            if (Numbers.isFinite(d)) {
                count++;
                double oldMean = mean;
                mean += (d - mean) / count;
                m2 += (d - mean) * (d - oldMean);
            }

            value.putDouble(0, mean);
            value.putDouble(1, m2);
            value.putLong(2, count);
            stddev = computeResultWelford(m2, count, isSample, isSqrt);
        }

        @Override
        public double getDouble(Record rec) {
            return stddev;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), stddev);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name);
            sink.val('(').val(arg).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row)");
        }
    }

    // Handles stddev() over (rows between unbounded preceding and current row); no partition key.
    // Uses Welford's online algorithm for numerical stability.
    static class StdDevOverUnboundedRowsFrameFunction extends BaseWindowFunction implements WindowDoubleFunction {
        private final boolean isSample;
        private final boolean isSqrt;
        private final String name;
        private long count = 0;
        private double m2 = 0.0;
        private double mean = 0.0;
        private double stddev = Double.NaN;

        StdDevOverUnboundedRowsFrameFunction(Function arg, boolean isSample, boolean isSqrt, String name) {
            super(arg);
            this.isSample = isSample;
            this.isSqrt = isSqrt;
            this.name = name;
        }

        @Override
        public void computeNext(Record record) {
            double d = arg.getDouble(record);
            if (Numbers.isFinite(d)) {
                count++;
                double oldMean = mean;
                mean += (d - mean) / count;
                m2 += (d - mean) * (d - oldMean);
            }

            stddev = computeResultWelford(m2, count, isSample, isSqrt);
        }

        @Override
        public double getDouble(Record rec) {
            return stddev;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), stddev);
        }

        @Override
        public void reset() {
            super.reset();
            stddev = Double.NaN;
            count = 0;
            mean = 0.0;
            m2 = 0.0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name);
            sink.val('(').val(arg).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            stddev = Double.NaN;
            count = 0;
            mean = 0.0;
            m2 = 0.0;
        }
    }

    // stddev() over () - empty clause, no partition by, no order by, default frame.
    // Uses Welford's online algorithm for numerical stability.
    static class StdDevOverWholeResultSetFunction extends BaseWindowFunction implements WindowDoubleFunction {
        private final boolean isSample;
        private final boolean isSqrt;
        private final String name;
        private long count;
        private double m2;
        private double mean;
        private double stddev;

        StdDevOverWholeResultSetFunction(Function arg, boolean isSample, boolean isSqrt, String name) {
            super(arg);
            this.isSample = isSample;
            this.isSqrt = isSqrt;
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            double d = arg.getDouble(record);
            if (Numbers.isFinite(d)) {
                count++;
                double oldMean = mean;
                mean += (d - mean) / count;
                m2 += (d - mean) * (d - oldMean);
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), stddev);
        }

        @Override
        public void preparePass2() {
            stddev = computeResultWelford(m2, count, isSample, isSqrt);
        }

        @Override
        public void reset() {
            super.reset();
            count = 0;
            stddev = Double.NaN;
            mean = 0.0;
            m2 = 0.0;
        }

        @Override
        public void toTop() {
            super.toTop();
            count = 0;
            stddev = Double.NaN;
            mean = 0.0;
            m2 = 0.0;
        }
    }

    static {
        // Used by Welford classes (OverPartition, OverUnboundedPartitionRows):
        //   [0] = mean (pass1) / stddev result (pass2), [1] = m2, [2] = count
        // Used by naive classes (sliding-frame variants):
        //   [0] = sum, [1] = sumSq, [2] = count
        STDDEV_COLUMN_TYPES = new ArrayColumnTypes();
        STDDEV_COLUMN_TYPES.add(ColumnType.DOUBLE);
        STDDEV_COLUMN_TYPES.add(ColumnType.DOUBLE);
        STDDEV_COLUMN_TYPES.add(ColumnType.LONG);

        STDDEV_OVER_PARTITION_ROWS_COLUMN_TYPES = new ArrayColumnTypes();
        STDDEV_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.DOUBLE); // sum
        STDDEV_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.DOUBLE); // sumSq
        STDDEV_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG);   // finite count in frame
        STDDEV_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG);   // lo index
        STDDEV_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG);   // start offset

        STDDEV_OVER_PARTITION_RANGE_COLUMN_TYPES = new ArrayColumnTypes();
        STDDEV_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.DOUBLE); // sum
        STDDEV_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.DOUBLE); // sumSq
        STDDEV_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG);   // finite count in frame
        STDDEV_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG);   // start offset
        STDDEV_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG);   // ring size
        STDDEV_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG);   // ring capacity
        STDDEV_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG);   // first index
    }
}

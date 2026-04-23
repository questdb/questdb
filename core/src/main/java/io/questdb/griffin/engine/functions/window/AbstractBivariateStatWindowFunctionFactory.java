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

public abstract class AbstractBivariateStatWindowFunctionFactory extends AbstractWindowFunctionFactory {

    private static final ArrayColumnTypes BIVAR_COLUMN_TYPES;
    private static final ArrayColumnTypes BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES;
    private static final ArrayColumnTypes BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES;

    // Naive sum-of-products formula, used by sliding-window (removable) frames.
    static double computeCorr(double sumXY, double sumXX, double sumYY, double sumX, double sumY, long count) {
        if (count <= 1) {
            return Double.NaN;
        }
        double cXY = sumXY - sumX * sumY / count;
        double cXX = sumXX - sumX * sumX / count;
        double cYY = sumYY - sumY * sumY / count;
        if (cXX < 0) {
            cXX = 0;
        }
        if (cYY < 0) {
            cYY = 0;
        }
        double denom = Math.sqrt(cXX * cYY);
        return denom == 0.0 ? Double.NaN : cXY / denom;
    }

    // Welford's online algorithm result for correlation.
    static double computeCorrWelford(double sumXY, double sumXX, double sumYY, long count) {
        if (count <= 1) {
            return Double.NaN;
        }
        if (sumXX < 0) {
            sumXX = 0;
        }
        if (sumYY < 0) {
            sumYY = 0;
        }
        double denom = Math.sqrt(sumXX * sumYY);
        return denom == 0.0 ? Double.NaN : sumXY / denom;
    }

    // Naive sum-of-products formula for covariance, used by sliding-window (removable) frames.
    static double computeCovar(double sumXY, double sumX, double sumY, long count, boolean isSample) {
        long denom = isSample ? count - 1 : count;
        if (denom <= 0) {
            return Double.NaN;
        }
        return (sumXY - sumX * sumY / count) / denom;
    }

    // Welford's online algorithm result for covariance.
    static double computeCovarWelford(double sumXY, long count, boolean isSample) {
        long denom = isSample ? count - 1 : count;
        if (denom <= 0) {
            return Double.NaN;
        }
        return sumXY / denom;
    }

    private static double computeResultNaive(double sumXY, double sumXX, double sumYY, double sumX, double sumY, long count, boolean isCorrelation, boolean isSample) {
        if (isCorrelation) {
            return computeCorr(sumXY, sumXX, sumYY, sumX, sumY, count);
        } else {
            return computeCovar(sumXY, sumX, sumY, count, isSample);
        }
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

        boolean isCorrelation = isCorrelation();
        boolean isSample = isSample();
        String name = name();

        Function argY = args.get(0);
        Function argX = args.get(1);

        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        if (rowsHi < rowsLo) {
            Misc.free(argX);
            return new DoubleNullFunction(
                    argY,
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
                            BIVAR_COLUMN_TYPES
                    );

                    return new BivarStatOverPartitionFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            argY,
                            argX,
                            isCorrelation,
                            isSample,
                            name
                    );
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            BIVAR_COLUMN_TYPES
                    );

                    return new BivarStatOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            argY,
                            argX,
                            isCorrelation,
                            isSample,
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
                                BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES
                        );
                        mem = Vm.getCARWInstance(
                                configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(),
                                MemoryTag.NATIVE_CIRCULAR_BUFFER
                        );

                        return new BivarStatOverPartitionRangeFrameFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                rowsLo,
                                rowsHi,
                                argY,
                                argX,
                                mem,
                                configuration.getSqlWindowInitialRangeBufferSize(),
                                timestampIndex,
                                isCorrelation,
                                isSample,
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
                            BIVAR_COLUMN_TYPES
                    );

                    return new BivarStatOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            argY,
                            argX,
                            isCorrelation,
                            isSample,
                            name
                    );
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new BivarStatOverCurrentRowFunction(argY, argX, isCorrelation, isSample, name);
                } // whole partition
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            BIVAR_COLUMN_TYPES
                    );

                    return new BivarStatOverPartitionFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            argY,
                            argX,
                            isCorrelation,
                            isSample,
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
                                BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES
                        );
                        mem = Vm.getCARWInstance(
                                configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(),
                                MemoryTag.NATIVE_CIRCULAR_BUFFER
                        );

                        return new BivarStatOverPartitionRowsFrameFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                rowsLo,
                                rowsHi,
                                argY,
                                argX,
                                mem,
                                isCorrelation,
                                isSample,
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
                    return new BivarStatOverWholeResultSetFunction(argY, argX, isCorrelation, isSample, name);
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new BivarStatOverUnboundedRowsFrameFunction(argY, argX, isCorrelation, isSample, name);
                } // range between [unbounded | x] preceding and [x preceding | current row]
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }

                    int timestampIndex = windowContext.getTimestampIndex();

                    return new BivarStatOverRangeFrameFunction(
                            rowsLo,
                            rowsHi,
                            argY,
                            argX,
                            configuration,
                            timestampIndex,
                            isCorrelation,
                            isSample,
                            name
                    );
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                // between unbounded preceding and current row
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new BivarStatOverUnboundedRowsFrameFunction(argY, argX, isCorrelation, isSample, name);
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new BivarStatOverCurrentRowFunction(argY, argX, isCorrelation, isSample, name);
                } // whole result set
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new BivarStatOverWholeResultSetFunction(argY, argX, isCorrelation, isSample, name);
                } // between [unbounded | x] preceding and [x preceding | current row]
                else {
                    MemoryARW mem = Vm.getCARWInstance(
                            configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(),
                            MemoryTag.NATIVE_CIRCULAR_BUFFER
                    );

                    return new BivarStatOverRowsFrameFunction(
                            argY,
                            argX,
                            rowsLo,
                            rowsHi,
                            mem,
                            isCorrelation,
                            isSample,
                            name
                    );
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    protected abstract boolean isCorrelation();

    protected abstract boolean isSample();

    protected abstract String name();

    // (rows between current row and current row) processes a 1-element set.
    // covar_pop returns 0.0 (both finite), covar_samp returns NaN, corr always returns NaN.
    static class BivarStatOverCurrentRowFunction extends BaseBivariateWindowFunction implements WindowDoubleFunction {
        private final Function argY;
        private final boolean isCorrelation;
        private final boolean isSample;
        private final String name;
        private double value;

        BivarStatOverCurrentRowFunction(Function argY, Function argX, boolean isCorrelation, boolean isSample, String name) {
            super(argY, argX);
            this.argY = argY;
            this.isCorrelation = isCorrelation;
            this.isSample = isSample;
            this.name = name;
        }

        @Override
        public void computeNext(Record record) {
            final double y = argY.getDouble(record);
            final double x = argX.getDouble(record);
            if (Numbers.isFinite(y) && Numbers.isFinite(x)) {
                if (isCorrelation) {
                    value = Double.NaN;
                } else {
                    value = isSample ? Double.NaN : 0.0;
                }
            } else {
                value = Double.NaN;
            }
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name);
            sink.val('(').val(argY).val(',').val(argX).val(')');
            sink.val(" over (rows between current row and current row)");
        }
    }

    // Handles bivariate stat over (partition by x), order by absent or whole-partition frame.
    static class BivarStatOverPartitionFunction extends BasePartitionedBivariateWindowFunction implements WindowDoubleFunction {
        private final Function argY;
        private final boolean isCorrelation;
        private final boolean isSample;
        private final String name;

        BivarStatOverPartitionFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                Function argY,
                Function argX,
                boolean isCorrelation,
                boolean isSample,
                String name
        ) {
            super(map, partitionByRecord, partitionBySink, argY, argX);
            this.argY = argY;
            this.isCorrelation = isCorrelation;
            this.isSample = isSample;
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
            // Welford's online algorithm: map stores [0]=meanX, [1]=sumXX, [2]=meanY, [3]=sumYY, [4]=sumXY, [5]=count
            double y = argY.getDouble(record);
            double x = argX.getDouble(record);
            if (Numbers.isFinite(y) && Numbers.isFinite(x)) {
                partitionByRecord.of(record);
                MapKey key = map.withKey();
                key.put(partitionByRecord, partitionBySink);
                MapValue value = key.createValue();

                if (value.isNew()) {
                    value.putDouble(0, x);    // meanX
                    value.putDouble(1, 0.0);  // sumXX
                    value.putDouble(2, y);    // meanY
                    value.putDouble(3, 0.0);  // sumYY
                    value.putDouble(4, 0.0);  // sumXY
                    value.putLong(5, 1);      // count
                } else {
                    long count = value.getLong(5) + 1;
                    double oldMeanX = value.getDouble(0);
                    double newMeanX = oldMeanX + (x - oldMeanX) / count;
                    double oldMeanY = value.getDouble(2);
                    double newMeanY = oldMeanY + (y - oldMeanY) / count;
                    double sumXX = value.getDouble(1) + (x - newMeanX) * (x - oldMeanX);
                    double sumYY = value.getDouble(3) + (y - newMeanY) * (y - oldMeanY);
                    double sumXY = value.getDouble(4) + (x - newMeanX) * (y - oldMeanY);
                    value.putDouble(0, newMeanX);
                    value.putDouble(1, sumXX);
                    value.putDouble(2, newMeanY);
                    value.putDouble(3, sumYY);
                    value.putDouble(4, sumXY);
                    value.putLong(5, count);
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();

            final double result = value != null ? value.getDouble(0) : Double.NaN;
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), result);
        }

        @Override
        public void preparePass2() {
            RecordCursor cursor = map.getCursor();
            MapRecord record = map.getRecord();
            while (cursor.hasNext()) {
                MapValue value = record.getValue();
                double sumXX = value.getDouble(1);
                double sumYY = value.getDouble(3);
                double sumXY = value.getDouble(4);
                long count = value.getLong(5);
                double result;
                if (isCorrelation) {
                    result = computeCorrWelford(sumXY, sumXX, sumYY, count);
                } else {
                    result = computeCovarWelford(sumXY, count, isSample);
                }
                value.putDouble(0, result);
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name);
            sink.val('(').val(argY).val(',').val(argX).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(')');
        }
    }

    // Handles bivariate stat over (partition by x order by ts range between [unbounded | y] preceding and [z preceding | current row]).
    // Removable cumulative aggregation with timestamp, x and y stored in resizable ring buffers.
    static class BivarStatOverPartitionRangeFrameFunction extends BasePartitionedBivariateWindowFunction implements WindowDoubleFunction {

        private static final int RECORD_SIZE = Long.BYTES + 2 * Double.BYTES;
        private final Function argY;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        // list of [size, startOffset] pairs marking free space within mem
        private final LongList freeList = new LongList();
        private final int initialBufferSize;
        private final boolean isCorrelation;
        private final boolean isSample;
        private final long maxDiff;
        private final MemoryARW memory;
        private final RingBufferDesc memoryDesc = new RingBufferDesc();
        private final long minDiff;
        private final String name;
        private final int timestampIndex;
        private double result;

        BivarStatOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function argY,
                Function argX,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                boolean isCorrelation,
                boolean isSample,
                String name
        ) {
            super(map, partitionByRecord, partitionBySink, argY, argX);
            this.argY = argY;
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            frameIncludesCurrentValue = rangeHi == 0;
            this.isCorrelation = isCorrelation;
            this.isSample = isSample;
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
            // 0 - sumX, never NaN
            // 1 - sumXX, never NaN
            // 2 - sumY, never NaN
            // 3 - sumYY, never NaN
            // 4 - sumXY, never NaN
            // 5 - current number of finite pairs in frame
            // 6 - native array start offset (relative to memory address)
            // 7 - ring buffer size (buffered elements, not all necessarily in frame)
            // 8 - ring buffer capacity
            // 9 - index of first (oldest) valid buffer element
            // ring data: [timestamp, x, y] triples; only finite pairs are stored

            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            double sumX;
            double sumXX;
            double sumY;
            double sumYY;
            double sumXY;
            long count;
            long startOffset;
            long size;
            long capacity;
            long firstIdx;

            long timestamp = record.getTimestamp(timestampIndex);
            double y = argY.getDouble(record);
            double x = argX.getDouble(record);
            boolean isFinitePair = Numbers.isFinite(y) && Numbers.isFinite(x);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;

                if (isFinitePair) {
                    memory.putLong(startOffset, timestamp);
                    memory.putDouble(startOffset + Long.BYTES, x);
                    memory.putDouble(startOffset + Long.BYTES + Double.BYTES, y);

                    if (frameIncludesCurrentValue) {
                        sumX = x;
                        sumXX = x * x;
                        sumY = y;
                        sumYY = y * y;
                        sumXY = x * y;
                        count = 1;
                        result = computeResultNaive(sumXY, sumXX, sumYY, sumX, sumY, count, isCorrelation, isSample);
                        size = frameLoBounded ? 1 : 0;
                    } else {
                        sumX = 0.0;
                        sumXX = 0.0;
                        sumY = 0.0;
                        sumYY = 0.0;
                        sumXY = 0.0;
                        count = 0;
                        result = Double.NaN;
                        size = 1;
                    }
                } else {
                    size = 0;
                    sumX = 0.0;
                    sumXX = 0.0;
                    sumY = 0.0;
                    sumYY = 0.0;
                    sumXY = 0.0;
                    count = 0;
                    result = Double.NaN;
                }
            } else {
                sumX = mapValue.getDouble(0);
                sumXX = mapValue.getDouble(1);
                sumY = mapValue.getDouble(2);
                sumYY = mapValue.getDouble(3);
                sumXY = mapValue.getDouble(4);
                count = mapValue.getLong(5);
                startOffset = mapValue.getLong(6);
                size = mapValue.getLong(7);
                capacity = mapValue.getLong(8);
                firstIdx = mapValue.getLong(9);

                long newFirstIdx = firstIdx;

                if (frameLoBounded) {
                    // find new lower bound and evict outdated values
                    for (long i = 0, n = size; i < n; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
                            if (count > 0) {
                                double valX = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                                double valY = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES + Double.BYTES);
                                sumX -= valX;
                                sumXX -= valX * valX;
                                sumY -= valY;
                                sumYY -= valY * valY;
                                sumXY -= valX * valY;
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

                // append current finite pair to buffer
                if (isFinitePair) {
                    if (size == capacity) {
                        memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                        expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                        capacity = memoryDesc.capacity;
                        startOffset = memoryDesc.startOffset;
                        firstIdx = memoryDesc.firstIdx;
                    }

                    memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                    memory.putDouble(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, x);
                    memory.putDouble(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES + Double.BYTES, y);
                    size++;
                }

                // include newly entered upper-bound values
                if (frameLoBounded) {
                    for (long i = count; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        long diff = Math.abs(ts - timestamp);

                        if (diff <= maxDiff && diff >= minDiff) {
                            double valX = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            double valY = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES + Double.BYTES);
                            sumX += valX;
                            sumXX += valX * valX;
                            sumY += valY;
                            sumYY += valY * valY;
                            sumXY += valX * valY;
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
                            double valX = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            double valY = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES + Double.BYTES);
                            sumX += valX;
                            sumXX += valX * valX;
                            sumY += valY;
                            sumYY += valY * valY;
                            sumXY += valX * valY;
                            count++;
                            newFirstIdx = (idx + 1) % capacity;
                            size--;
                        } else {
                            break;
                        }
                    }

                    firstIdx = newFirstIdx;
                }

                result = computeResultNaive(sumXY, sumXX, sumYY, sumX, sumY, count, isCorrelation, isSample);
            }

            mapValue.putDouble(0, sumX);
            mapValue.putDouble(1, sumXX);
            mapValue.putDouble(2, sumY);
            mapValue.putDouble(3, sumYY);
            mapValue.putDouble(4, sumXY);
            mapValue.putLong(5, count);
            mapValue.putLong(6, startOffset);
            mapValue.putLong(7, size);
            mapValue.putLong(8, capacity);
            mapValue.putLong(9, firstIdx);
        }

        @Override
        public double getDouble(Record rec) {
            return result;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), result);
        }

        @Override
        public void reopen() {
            super.reopen();
            result = Double.NaN;
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
            sink.val('(').val(argY).val(',').val(argX).val(')');
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

    // Handles bivariate stat over (partition by x [order by o] rows between y and z).
    // Removable cumulative aggregation.
    static class BivarStatOverPartitionRowsFrameFunction extends BasePartitionedBivariateWindowFunction implements WindowDoubleFunction {

        private static final int SLOT_SIZE = 2 * Double.BYTES;
        private final Function argY;
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final boolean isCorrelation;
        private final boolean isSample;
        private final MemoryARW memory;
        private final String name;
        private double result;

        BivarStatOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function argY,
                Function argX,
                MemoryARW memory,
                boolean isCorrelation,
                boolean isSample,
                String name
        ) {
            super(map, partitionByRecord, partitionBySink, argY, argX);
            this.argY = argY;

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
            this.isCorrelation = isCorrelation;
            this.isSample = isSample;
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
            // 0 - sumX
            // 1 - sumXX
            // 2 - sumY
            // 3 - sumYY
            // 4 - sumXY
            // 5 - finite count in frame
            // 6 - (0-based) index of oldest value
            // 7 - native array start offset
            // ring buffer stores [x, y] pairs including NaN placeholders

            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            double sumX;
            double sumXX;
            double sumY;
            double sumYY;
            double sumXY;
            long count;
            long loIdx;
            long startOffset;
            double y = argY.getDouble(record);
            double x = argX.getDouble(record);

            if (value.isNew()) {
                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * SLOT_SIZE) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && Numbers.isFinite(y) && Numbers.isFinite(x)) {
                    sumX = x;
                    sumXX = x * x;
                    sumY = y;
                    sumYY = y * y;
                    sumXY = x * y;
                    count = 1;
                    result = computeResultNaive(sumXY, sumXX, sumYY, sumX, sumY, count, isCorrelation, isSample);
                } else {
                    sumX = 0.0;
                    sumXX = 0.0;
                    sumY = 0.0;
                    sumYY = 0.0;
                    sumXY = 0.0;
                    count = 0;
                    result = Double.NaN;
                }

                for (int i = 0; i < bufferSize; i++) {
                    memory.putDouble(startOffset + (long) i * SLOT_SIZE, Double.NaN);
                    memory.putDouble(startOffset + (long) i * SLOT_SIZE + Double.BYTES, Double.NaN);
                }
            } else {
                sumX = value.getDouble(0);
                sumXX = value.getDouble(1);
                sumY = value.getDouble(2);
                sumYY = value.getDouble(3);
                sumXY = value.getDouble(4);
                count = value.getLong(5);
                loIdx = value.getLong(6);
                startOffset = value.getLong(7);

                double hiX;
                double hiY;
                if (frameIncludesCurrentValue) {
                    hiX = x;
                    hiY = y;
                } else {
                    long hiSlot = (loIdx + frameSize - 1) % bufferSize;
                    hiX = memory.getDouble(startOffset + hiSlot * SLOT_SIZE);
                    hiY = memory.getDouble(startOffset + hiSlot * SLOT_SIZE + Double.BYTES);
                }
                if (Numbers.isFinite(hiX) && Numbers.isFinite(hiY)) {
                    count++;
                    sumX += hiX;
                    sumXX += hiX * hiX;
                    sumY += hiY;
                    sumYY += hiY * hiY;
                    sumXY += hiX * hiY;
                }

                result = computeResultNaive(sumXY, sumXX, sumYY, sumX, sumY, count, isCorrelation, isSample);

                if (frameLoBounded) {
                    double loX = memory.getDouble(startOffset + loIdx * SLOT_SIZE);
                    double loY = memory.getDouble(startOffset + loIdx * SLOT_SIZE + Double.BYTES);
                    if (Numbers.isFinite(loX) && Numbers.isFinite(loY)) {
                        sumX -= loX;
                        sumXX -= loX * loX;
                        sumY -= loY;
                        sumYY -= loY * loY;
                        sumXY -= loX * loY;
                        count--;
                    }
                }
            }

            value.putDouble(0, sumX);
            value.putDouble(1, sumXX);
            value.putDouble(2, sumY);
            value.putDouble(3, sumYY);
            value.putDouble(4, sumXY);
            value.putLong(5, count);
            value.putLong(6, (loIdx + 1) % bufferSize);
            value.putLong(7, startOffset);
            memory.putDouble(startOffset + loIdx * SLOT_SIZE, x);
            memory.putDouble(startOffset + loIdx * SLOT_SIZE + Double.BYTES, y);
        }

        @Override
        public double getDouble(Record rec) {
            return result;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), result);
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
            sink.val('(').val(argY).val(',').val(argX).val(')');
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

    // Handles bivariate stat over ([order by ts] range between [unbounded | x] preceding and [x preceding | current row]); no partition key.
    static class BivarStatOverRangeFrameFunction extends BaseBivariateWindowFunction implements Reopenable, WindowDoubleFunction {
        private static final int RECORD_SIZE = Long.BYTES + 2 * Double.BYTES;
        private final Function argY;
        private final boolean frameLoBounded;
        private final long initialCapacity;
        private final boolean isCorrelation;
        private final boolean isSample;
        private final long maxDiff;
        private final MemoryARW memory;
        private final long minDiff;
        private final String name;
        private final int timestampIndex;
        private long capacity;
        private long count;
        private long firstIdx;
        private double result;
        private long size;
        private long startOffset;
        private double sumX;
        private double sumXX;
        private double sumXY;
        private double sumY;
        private double sumYY;

        BivarStatOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function argY,
                Function argX,
                CairoConfiguration configuration,
                int timestampIdx,
                boolean isCorrelation,
                boolean isSample,
                String name
        ) {
            this(
                    rangeLo,
                    rangeHi,
                    argY,
                    argX,
                    configuration.getSqlWindowStorePageSize() / RECORD_SIZE,
                    Vm.getCARWInstance(
                            configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(),
                            MemoryTag.NATIVE_CIRCULAR_BUFFER
                    ),
                    timestampIdx,
                    isCorrelation,
                    isSample,
                    name
            );
        }

        BivarStatOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function argY,
                Function argX,
                long initialCapacity,
                MemoryARW memory,
                int timestampIdx,
                boolean isCorrelation,
                boolean isSample,
                String name
        ) {
            super(argY, argX);
            this.argY = argY;
            this.initialCapacity = initialCapacity;
            this.memory = memory;
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Long.MAX_VALUE;
            minDiff = Math.abs(rangeHi);
            timestampIndex = timestampIdx;
            this.isCorrelation = isCorrelation;
            this.isSample = isSample;
            this.name = name;

            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            count = 0;
            size = 0;
            sumX = 0.0;
            sumXX = 0.0;
            sumY = 0.0;
            sumYY = 0.0;
            sumXY = 0.0;
            result = Double.NaN;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            long timestamp = record.getTimestamp(timestampIndex);
            double y = argY.getDouble(record);
            double x = argX.getDouble(record);
            boolean isFinitePair = Numbers.isFinite(y) && Numbers.isFinite(x);

            long newFirstIdx = firstIdx;

            if (frameLoBounded) {
                // find new lower bound and evict outdated values
                for (long i = 0, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        if (count > 0) {
                            double valX = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                            double valY = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES + Double.BYTES);
                            sumX -= valX;
                            sumXX -= valX * valX;
                            sumY -= valY;
                            sumYY -= valY * valY;
                            sumXY -= valX * valY;
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

            if (isFinitePair) {
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
                memory.putDouble(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, x);
                memory.putDouble(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES + Double.BYTES, y);
                size++;
            }

            // include newly entered upper-bound values
            if (frameLoBounded) {
                for (long i = count, n = size; i < n; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);

                    if (diff <= maxDiff && diff >= minDiff) {
                        double valX = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        double valY = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES + Double.BYTES);
                        sumX += valX;
                        sumXX += valX * valX;
                        sumY += valY;
                        sumYY += valY * valY;
                        sumXY += valX * valY;
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
                        double valX = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES);
                        double valY = memory.getDouble(startOffset + idx * RECORD_SIZE + Long.BYTES + Double.BYTES);
                        sumX += valX;
                        sumXX += valX * valX;
                        sumY += valY;
                        sumYY += valY * valY;
                        sumXY += valX * valY;
                        count++;
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        break;
                    }
                }
                firstIdx = newFirstIdx;
            }

            result = computeResultNaive(sumXY, sumXX, sumYY, sumX, sumY, count, isCorrelation, isSample);
        }

        @Override
        public double getDouble(Record rec) {
            return result;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), result);
        }

        @Override
        public void reopen() {
            result = Double.NaN;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            count = 0;
            size = 0;
            sumX = 0.0;
            sumXX = 0.0;
            sumY = 0.0;
            sumYY = 0.0;
            sumXY = 0.0;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(argY).val(',').val(argX).val(')');
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
            result = Double.NaN;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            count = 0;
            size = 0;
            sumX = 0.0;
            sumXX = 0.0;
            sumY = 0.0;
            sumYY = 0.0;
            sumXY = 0.0;
        }
    }

    // Handles bivariate stat over ([order by o] rows between y and z); no partition key.
    // Removable cumulative aggregation.
    static class BivarStatOverRowsFrameFunction extends BaseBivariateWindowFunction implements Reopenable, WindowDoubleFunction {
        private static final int SLOT_SIZE = 2 * Double.BYTES;
        private final Function argY;
        private final MemoryARW buffer;
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        private final boolean isCorrelation;
        private final boolean isSample;
        private final String name;
        private long count = 0;
        private int loIdx = 0;
        private double result = Double.NaN;
        private double sumX = 0.0;
        private double sumXX = 0.0;
        private double sumXY = 0.0;
        private double sumY = 0.0;
        private double sumYY = 0.0;

        BivarStatOverRowsFrameFunction(
                Function argY,
                Function argX,
                long rowsLo,
                long rowsHi,
                MemoryARW memory,
                boolean isCorrelation,
                boolean isSample,
                String name
        ) {
            super(argY, argX);
            this.argY = argY;
            this.isCorrelation = isCorrelation;
            this.isSample = isSample;
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
            double y = argY.getDouble(record);
            double x = argX.getDouble(record);

            double hiX;
            double hiY;
            if (frameLoBounded && !frameIncludesCurrentValue) {
                long hiSlot = (loIdx + frameSize - 1) % bufferSize;
                hiX = buffer.getDouble(hiSlot * SLOT_SIZE);
                hiY = buffer.getDouble(hiSlot * SLOT_SIZE + Double.BYTES);
            } else if (!frameLoBounded && !frameIncludesCurrentValue) {
                hiX = buffer.getDouble((long) (loIdx % bufferSize) * SLOT_SIZE);
                hiY = buffer.getDouble((long) (loIdx % bufferSize) * SLOT_SIZE + Double.BYTES);
            } else {
                hiX = x;
                hiY = y;
            }

            if (Numbers.isFinite(hiX) && Numbers.isFinite(hiY)) {
                sumX += hiX;
                sumXX += hiX * hiX;
                sumY += hiY;
                sumYY += hiY * hiY;
                sumXY += hiX * hiY;
                count++;
            }

            result = computeResultNaive(sumXY, sumXX, sumYY, sumX, sumY, count, isCorrelation, isSample);

            if (frameLoBounded) {
                double loX = buffer.getDouble((long) loIdx * SLOT_SIZE);
                double loY = buffer.getDouble((long) loIdx * SLOT_SIZE + Double.BYTES);
                if (Numbers.isFinite(loX) && Numbers.isFinite(loY)) {
                    sumX -= loX;
                    sumXX -= loX * loX;
                    sumY -= loY;
                    sumYY -= loY * loY;
                    sumXY -= loX * loY;
                    count--;
                }
            }

            buffer.putDouble((long) loIdx * SLOT_SIZE, x);
            buffer.putDouble((long) loIdx * SLOT_SIZE + Double.BYTES, y);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public double getDouble(Record rec) {
            return result;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), result);
        }

        @Override
        public void reopen() {
            result = Double.NaN;
            count = 0;
            loIdx = 0;
            sumX = 0.0;
            sumXX = 0.0;
            sumY = 0.0;
            sumYY = 0.0;
            sumXY = 0.0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            result = Double.NaN;
            count = 0;
            loIdx = 0;
            sumX = 0.0;
            sumXX = 0.0;
            sumY = 0.0;
            sumYY = 0.0;
            sumXY = 0.0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(argY).val(',').val(argX).val(')');
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
            result = Double.NaN;
            count = 0;
            loIdx = 0;
            sumX = 0.0;
            sumXX = 0.0;
            sumY = 0.0;
            sumYY = 0.0;
            sumXY = 0.0;
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putDouble((long) i * SLOT_SIZE, Double.NaN);
                buffer.putDouble((long) i * SLOT_SIZE + Double.BYTES, Double.NaN);
            }
        }
    }

    // Handles:
    // - bivariate stat over (partition by x rows between unbounded preceding and current row)
    // - bivariate stat over (partition by x order by ts range between unbounded preceding and current row)
    // Doesn't require value buffering.
    static class BivarStatOverUnboundedPartitionRowsFrameFunction extends BasePartitionedBivariateWindowFunction implements WindowDoubleFunction {
        private final Function argY;
        private final boolean isCorrelation;
        private final boolean isSample;
        private final String name;
        private double result = Double.NaN;

        BivarStatOverUnboundedPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                Function argY,
                Function argX,
                boolean isCorrelation,
                boolean isSample,
                String name
        ) {
            super(map, partitionByRecord, partitionBySink, argY, argX);
            this.argY = argY;
            this.isCorrelation = isCorrelation;
            this.isSample = isSample;
            this.name = name;
        }

        @Override
        public void computeNext(Record record) {
            // Welford's online algorithm: map stores [0]=meanX, [1]=sumXX, [2]=meanY, [3]=sumYY, [4]=sumXY, [5]=count
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            double meanX;
            double sumXX;
            double meanY;
            double sumYY;
            double sumXY;
            long count;

            if (value.isNew()) {
                meanX = 0.0;
                sumXX = 0.0;
                meanY = 0.0;
                sumYY = 0.0;
                sumXY = 0.0;
                count = 0;
            } else {
                meanX = value.getDouble(0);
                sumXX = value.getDouble(1);
                meanY = value.getDouble(2);
                sumYY = value.getDouble(3);
                sumXY = value.getDouble(4);
                count = value.getLong(5);
            }

            double y = argY.getDouble(record);
            double x = argX.getDouble(record);
            if (Numbers.isFinite(y) && Numbers.isFinite(x)) {
                count++;
                double oldMeanX = meanX;
                meanX += (x - meanX) / count;
                double oldMeanY = meanY;
                meanY += (y - meanY) / count;
                sumXX += (x - meanX) * (x - oldMeanX);
                sumYY += (y - meanY) * (y - oldMeanY);
                sumXY += (x - meanX) * (y - oldMeanY);
            }

            value.putDouble(0, meanX);
            value.putDouble(1, sumXX);
            value.putDouble(2, meanY);
            value.putDouble(3, sumYY);
            value.putDouble(4, sumXY);
            value.putLong(5, count);

            if (isCorrelation) {
                result = computeCorrWelford(sumXY, sumXX, sumYY, count);
            } else {
                result = computeCovarWelford(sumXY, count, isSample);
            }
        }

        @Override
        public double getDouble(Record rec) {
            return result;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), result);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name);
            sink.val('(').val(argY).val(',').val(argX).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row)");
        }
    }

    // Handles bivariate stat over (rows between unbounded preceding and current row); no partition key.
    // Uses Welford's online algorithm for numerical stability.
    static class BivarStatOverUnboundedRowsFrameFunction extends BaseBivariateWindowFunction implements WindowDoubleFunction {
        private final Function argY;
        private final boolean isCorrelation;
        private final boolean isSample;
        private final String name;
        private long count = 0;
        private double meanX = 0.0;
        private double meanY = 0.0;
        private double result = Double.NaN;
        private double sumXX = 0.0;
        private double sumXY = 0.0;
        private double sumYY = 0.0;

        BivarStatOverUnboundedRowsFrameFunction(Function argY, Function argX, boolean isCorrelation, boolean isSample, String name) {
            super(argY, argX);
            this.argY = argY;
            this.isCorrelation = isCorrelation;
            this.isSample = isSample;
            this.name = name;
        }

        @Override
        public void computeNext(Record record) {
            double y = argY.getDouble(record);
            double x = argX.getDouble(record);
            if (Numbers.isFinite(y) && Numbers.isFinite(x)) {
                count++;
                double oldMeanX = meanX;
                meanX += (x - meanX) / count;
                double oldMeanY = meanY;
                meanY += (y - meanY) / count;
                sumXX += (x - meanX) * (x - oldMeanX);
                sumYY += (y - meanY) * (y - oldMeanY);
                sumXY += (x - meanX) * (y - oldMeanY);
            }

            if (isCorrelation) {
                result = computeCorrWelford(sumXY, sumXX, sumYY, count);
            } else {
                result = computeCovarWelford(sumXY, count, isSample);
            }
        }

        @Override
        public double getDouble(Record rec) {
            return result;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), result);
        }

        @Override
        public void reset() {
            super.reset();
            result = Double.NaN;
            count = 0;
            meanX = 0.0;
            meanY = 0.0;
            sumXX = 0.0;
            sumYY = 0.0;
            sumXY = 0.0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name);
            sink.val('(').val(argY).val(',').val(argX).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            result = Double.NaN;
            count = 0;
            meanX = 0.0;
            meanY = 0.0;
            sumXX = 0.0;
            sumYY = 0.0;
            sumXY = 0.0;
        }
    }

    // Bivariate stat over () - empty clause, no partition by, no order by, default frame.
    // Uses Welford's online algorithm for numerical stability.
    static class BivarStatOverWholeResultSetFunction extends BaseBivariateWindowFunction implements WindowDoubleFunction {
        private final Function argY;
        private final boolean isCorrelation;
        private final boolean isSample;
        private final String name;
        private long count;
        private double meanX;
        private double meanY;
        private double result;
        private double sumXX;
        private double sumXY;
        private double sumYY;

        BivarStatOverWholeResultSetFunction(Function argY, Function argX, boolean isCorrelation, boolean isSample, String name) {
            super(argY, argX);
            this.argY = argY;
            this.isCorrelation = isCorrelation;
            this.isSample = isSample;
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
            double y = argY.getDouble(record);
            double x = argX.getDouble(record);
            if (Numbers.isFinite(y) && Numbers.isFinite(x)) {
                count++;
                double oldMeanX = meanX;
                meanX += (x - meanX) / count;
                double oldMeanY = meanY;
                meanY += (y - meanY) / count;
                sumXX += (x - meanX) * (x - oldMeanX);
                sumYY += (y - meanY) * (y - oldMeanY);
                sumXY += (x - meanX) * (y - oldMeanY);
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), result);
        }

        @Override
        public void preparePass2() {
            if (isCorrelation) {
                result = computeCorrWelford(sumXY, sumXX, sumYY, count);
            } else {
                result = computeCovarWelford(sumXY, count, isSample);
            }
        }

        @Override
        public void reset() {
            super.reset();
            count = 0;
            result = Double.NaN;
            meanX = 0.0;
            meanY = 0.0;
            sumXX = 0.0;
            sumYY = 0.0;
            sumXY = 0.0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name);
            sink.val('(').val(argY).val(',').val(argX).val(')');
            sink.val(" over ()");
        }

        @Override
        public void toTop() {
            super.toTop();
            count = 0;
            result = Double.NaN;
            meanX = 0.0;
            meanY = 0.0;
            sumXX = 0.0;
            sumYY = 0.0;
            sumXY = 0.0;
        }
    }

    static {
        // Used by Welford classes (OverPartition, OverUnboundedPartitionRows):
        //   [0] = meanX (pass1) / result (pass2), [1] = sumXX, [2] = meanY, [3] = sumYY, [4] = sumXY, [5] = count
        // Used by naive classes (sliding-frame variants):
        //   [0] = sumX, [1] = sumXX, [2] = sumY, [3] = sumYY, [4] = sumXY, [5] = count
        BIVAR_COLUMN_TYPES = new ArrayColumnTypes();
        BIVAR_COLUMN_TYPES.add(ColumnType.DOUBLE);  // sumX / meanX
        BIVAR_COLUMN_TYPES.add(ColumnType.DOUBLE);  // sumXX
        BIVAR_COLUMN_TYPES.add(ColumnType.DOUBLE);  // sumY / meanY
        BIVAR_COLUMN_TYPES.add(ColumnType.DOUBLE);  // sumYY
        BIVAR_COLUMN_TYPES.add(ColumnType.DOUBLE);  // sumXY
        BIVAR_COLUMN_TYPES.add(ColumnType.LONG);    // count

        BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES = new ArrayColumnTypes();
        BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.DOUBLE);  // sumX
        BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.DOUBLE);  // sumXX
        BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.DOUBLE);  // sumY
        BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.DOUBLE);  // sumYY
        BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.DOUBLE);  // sumXY
        BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG);    // finite count in frame
        BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG);    // lo index
        BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG);    // start offset

        BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES = new ArrayColumnTypes();
        BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.DOUBLE); // sumX
        BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.DOUBLE); // sumXX
        BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.DOUBLE); // sumY
        BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.DOUBLE); // sumYY
        BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.DOUBLE); // sumXY
        BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG);   // finite count in frame
        BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG);   // start offset
        BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG);   // ring size
        BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG);   // ring capacity
        BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES.add(ColumnType.LONG);   // first index
    }
}

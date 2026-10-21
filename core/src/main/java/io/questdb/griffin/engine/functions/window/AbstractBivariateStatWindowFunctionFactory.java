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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.lv.LiveViewCheckpointRangeRingStateReader;
import io.questdb.cairo.lv.LiveViewCheckpointRingStateSink;
import io.questdb.cairo.lv.LiveViewCheckpointRingStateSource;
import io.questdb.cairo.lv.LiveViewSnapshotKeyCodec;
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
import io.questdb.cairo.lv.LiveViewStatePageWriter;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.lv.LiveViewStatePageReader;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.Nullable;

public abstract class AbstractBivariateStatWindowFunctionFactory extends AbstractWindowFunctionFactory {

    static final ArrayColumnTypes BIVAR_COLUMN_TYPES;
    static final ArrayColumnTypes BIVAR_COLUMN_TYPES_LV;
    private static final ArrayColumnTypes BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES;
    // Live-view variant of BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES with the BYTE
    // tombstone slot appended.
    private static final ArrayColumnTypes BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES_LV;
    private static final ArrayColumnTypes BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES;
    // Live-view variant of BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES with the BYTE
    // tombstone slot appended.
    private static final ArrayColumnTypes BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES_LV;

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
        return Numbers.corrFromSums(cXY, cXX, cYY);
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
        return Numbers.corrFromSums(sumXY, sumXX, sumYY);
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
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            liveView ? BIVAR_COLUMN_TYPES_LV : BIVAR_COLUMN_TYPES
                    );

                    return new BivarStatOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            argY,
                            argX,
                            isCorrelation,
                            isSample,
                            name,
                            partitionByKeyTypes,
                            liveView,
                            configuration
                    );
                } // range between [unbounded | x] preceding and [x preceding | current row]
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }

                    int timestampIndex = windowContext.getTimestampIndex();

                    final boolean liveView = windowContext.isLiveView();
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                liveView ? BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES_LV
                                        : BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES
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
                                name,
                                partitionByKeyTypes,
                                liveView
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
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            liveView ? BIVAR_COLUMN_TYPES_LV : BIVAR_COLUMN_TYPES
                    );

                    return new BivarStatOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            argY,
                            argX,
                            isCorrelation,
                            isSample,
                            name,
                            partitionByKeyTypes,
                            liveView,
                            configuration
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
                    final boolean liveView = windowContext.isLiveView();
                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                liveView ? BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES_LV
                                        : BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES
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
                                name,
                                partitionByKeyTypes,
                                liveView
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
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), value);
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
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), result);
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
        // Deep copy of the partition-by key column types for the snapshot codec. The
        // factory reuses its partitionByKeyTypes buffer across compiles. Null outside LV.
        private final ArrayColumnTypes keyColumnTypes;
        private final boolean liveView;
        // Full value layout, tombstone slot included. Null outside LV.
        private final ArrayColumnTypes mapValueTypes;
        private final long maxDiff;
        private final MemoryARW memory;
        private final RingBufferDesc memoryDesc = new RingBufferDesc();
        private final long minDiff;
        private final String name;
        private final RingRestoreSink ringRestore = new RingRestoreSink();
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
                String name,
                ColumnTypes partitionByKeyTypes,
                boolean liveView
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
            this.liveView = liveView;
            if (liveView) {
                ArrayColumnTypes keyTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = partitionByKeyTypes.getColumnCount(); i < n; i++) {
                    keyTypesCopy.add(partitionByKeyTypes.getColumnType(i));
                }
                this.keyColumnTypes = keyTypesCopy;
                ArrayColumnTypes valueTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES_LV.getColumnCount(); i < n; i++) {
                    valueTypesCopy.add(BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES_LV.getColumnType(i));
                }
                this.mapValueTypes = valueTypesCopy;
                this.tombstoneValueIndex = 10;
            } else {
                this.keyColumnTypes = null;
                this.mapValueTypes = null;
                this.tombstoneValueIndex = -1;
            }
        }

        /**
         * Each ring row carries an {@code (x, y)} pair, two raw IEEE-754 words, so the ring
         * stores it under the two-word value kind. That kind's pages hold raw 64-bit words (raw
         * or FoR-packed), so both doubles round-trip by their exact bits; it is the only
         * non-deque two-word kind the ring codec has.
         */
        @Override
        public int checkpointRingValueKind() {
            return LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DECIMAL128;
        }

        @Override
        public int checkpointStateFormatVersion() {
            return 1;
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
            // 10 - tombstone (live view only)
            // ring data: [timestamp, x, y] triples; only finite pairs are stored. With a bounded
            // frame start the first `count` buffered triples are the frame and the rest are
            // triples a trailing frame end has not reached yet.

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
                if (tombstoneValueIndex >= 0) {
                    mapValue.putByte(tombstoneValueIndex, (byte) 0);
                }
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
                        if (Numbers.saturatedAbsDiff(timestamp, ts) > maxDiff) {
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
                        long diff = Numbers.saturatedAbsDiff(ts, timestamp);

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
                        if (Numbers.saturatedAbsDiff(timestamp, ts) >= minDiff) {
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

        /**
         * Streams the partition's buffered {@code (x, y)} pairs oldest first, behind the in-frame
         * count as the ring's frame size. The five running sums do not fit the ring's widest
         * (four-word) scalar, so the one scalar word the format requires carries nothing and
         * restore re-accumulates the sums from the in-frame prefix of the ring instead.
         */
        @Override
        public void freezeCheckpointRingState(LiveViewCheckpointRingStateSink sink, MapValue value) {
            sink.putScalarState(0L, value.getLong(5));
            final long startOffset = value.getLong(6);
            final long size = value.getLong(7);
            final long capacity = value.getLong(8);
            final long firstIdx = value.getLong(9);
            for (long i = 0; i < size; i++) {
                final long rec = startOffset + ((firstIdx + i) % capacity) * RECORD_SIZE;
                sink.putRow(
                        memory.getLong(rec),
                        Double.doubleToRawLongBits(memory.getDouble(rec + Long.BYTES)),
                        Double.doubleToRawLongBits(memory.getDouble(rec + Long.BYTES + Double.BYTES))
                );
            }
        }

        /**
         * The in-RAM whole-state form, used by
         * {@link io.questdb.cairo.lv.LiveViewFunctionSnapshot} to clone a partition into an
         * out-of-order repair's scratch overlay: {@code (count, size)}, then the {@code size}
         * buffered {@code (timestamp, x, y)} triples, oldest first. The durable checkpoint uses
         * the chunked ring form
         * {@link #freezeCheckpointRingState(LiveViewCheckpointRingStateSink, MapValue)} instead,
         * which shares pages across roots. Neither carries an accumulator - both restores
         * re-accumulate the sums from the ring - so a state-layout change must move both
         * together.
         */
        @Override
        public void freezeCheckpointState(LiveViewStatePageWriter sink, MapValue value) {
            sink.putLong(value.getLong(5));
            final long startOffset = value.getLong(6);
            final long size = value.getLong(7);
            final long capacity = value.getLong(8);
            final long firstIdx = value.getLong(9);
            sink.putLong(size);
            for (long i = 0; i < size; i++) {
                final long rec = startOffset + ((firstIdx + i) % capacity) * RECORD_SIZE;
                sink.putLong(memory.getLong(rec));
                sink.putDouble(memory.getDouble(rec + Long.BYTES));
                sink.putDouble(memory.getDouble(rec + Long.BYTES + Double.BYTES));
            }
        }

        @Override
        public ColumnTypes getCheckpointKeyColumnTypes() {
            return keyColumnTypes;
        }

        @Override
        public int getCheckpointKeyStartIndex() {
            return mapValueTypes != null
                    ? mapValueTypes.getColumnCount()
                    : BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES.getColumnCount();
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
        public Map getPartitionMap() {
            return map;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        /**
         * The arena holding every partition's ring slab. Only the live-view frontier sweep of a
         * {@code BasePartitionedWindowFunction} reads it, so it is inert here: this class
         * descends from {@link BasePartitionedBivariateWindowFunction}, whose sweep enrols no
         * ring, and {@link #supportsCheckpointState()} declines the one unbounded-start RANGE
         * shape that keeps its ANCHOR, so no sweep reaches this class anyway.
         */
        @Override
        public MemoryARW getRingArena() {
            return memory;
        }

        @Override
        public boolean hasFrameLocalCheckpointState() {
            // With a bounded frame start the ring holds the frame's own pairs, plus those a
            // trailing frame end has not reached yet, and the five sums and the count accumulate
            // over exactly the in-frame prefix. Replaying from the frame's lower edge rebuilds
            // all of it; the replayed sums skip the rounding the expired pairs left behind, which
            // the contract allows: the values emitted are the same, up to the documented
            // floating drift.
            return true;
        }

        @Override
        public void onCheckpointRestoreBegin() {
            super.onCheckpointRestoreBegin();
            memory.jumpTo(0);
            freeList.clear();
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), result);
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
        public void resetPartition(Record record) {
            // ANCHOR-driven reset, ported for parity with avg's RANGE frame; no anchored window
            // reaches this class today (see getRingArena). Drop the partition's frame to empty:
            // the five sums, count, size and firstIdx go to zero; the ring slab stays allocated
            // and the next row writes from index 0.
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            if (value != null) {
                for (int i = 0; i < 5; i++) {
                    value.putDouble(i, 0.0);
                }
                value.putLong(5, 0L);
                // slot 6 (startOffset) stays - the ring slab is reused.
                value.putLong(7, 0L);
                // slot 8 (capacity) stays.
                value.putLong(9, 0L);
                if (!value.isNew() && tombstoneValueIndex >= 0 && value.getByte(tombstoneValueIndex) != 1) {
                    value.putByte(tombstoneValueIndex, (byte) 1);
                    tombstoneCount++;
                }
            }
        }

        @Override
        public void restoreCheckpointRingState(LiveViewCheckpointRingStateSource source, MapValue value) {
            final long size = source.getRowCount();
            final long count = source.getFrameSize();
            validateRestoredFrame(count, size);
            final long capacity = WindowFunction.restoredRingCapacity(size, initialBufferSize);
            final long newStartOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            ringRestore.of(newStartOffset, capacity);
            source.forEachRow(ringRestore);
            if (ringRestore.rows != size) {
                throw CairoException.critical(0)
                        .put("live view checkpoint bivariate RANGE ring row count mismatch [expected=").put(size)
                        .put(", actual=").put(ringRestore.rows).put(']');
            }
            putRestoredState(value, newStartOffset, count, size, capacity);
        }

        /**
         * Reads the in-RAM image
         * {@link #freezeCheckpointState(LiveViewStatePageWriter, MapValue)} wrote, onto the same
         * convention as {@link #restoreCheckpointRingState(LiveViewCheckpointRingStateSource, MapValue)}:
         * the count verbatim, the triples into a fresh slab rebased to index 0, and the five
         * sums re-accumulated from the in-frame prefix.
         */
        @Override
        public long restoreCheckpointState(LiveViewStatePageReader source, long offset, MapValue value) {
            final long count = source.getLong(offset);
            offset += Long.BYTES;
            final long size = source.getLong(offset);
            offset += Long.BYTES;
            validateRestoredFrame(count, size);
            final long capacity = WindowFunction.restoredRingCapacity(size, initialBufferSize);
            final long newStartOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            for (long i = 0; i < size; i++) {
                final long rec = newStartOffset + i * RECORD_SIZE;
                memory.putLong(rec, source.getLong(offset));
                offset += Long.BYTES;
                memory.putDouble(rec + Long.BYTES, source.getDouble(offset));
                offset += Double.BYTES;
                memory.putDouble(rec + Long.BYTES + Double.BYTES, source.getDouble(offset));
                offset += Double.BYTES;
            }
            putRestoredState(value, newStartOffset, count, size, capacity);
            return offset;
        }

        @Override
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            super.setMemoryTracker(tracker);
            memory.setMemoryTracker(tracker);
        }

        @Override
        public boolean supportsCheckpointRingState() {
            return supportsCheckpointState();
        }

        @Override
        public boolean supportsCheckpointState() {
            // A bounded frame start only. There the frame is a prefix of the ring, which is what
            // restore re-accumulates the five sums from. An unbounded start folds expired pairs
            // into the sums and drops them from the ring, so the ring no longer holds what the
            // sums cover. SqlParser's finite-influence gate rejects that shape at CREATE unless
            // an ANCHOR resets the window, and RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            // EXCLUDE CURRENT ROW keeps its ANCHOR and still lands in this class, so the class
            // declines it itself.
            return liveView
                    && frameLoBounded
                    && keyColumnTypes != null
                    && LiveViewSnapshotKeyCodec.isAllTypesSupported(keyColumnTypes);
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

        /**
         * Publishes a restored partition: the ring sits in a fresh slab at
         * {@code startOffset}, rebased to index 0, and the five sums are re-accumulated over its
         * first {@code count} triples - the in-frame prefix - with the same {@code +=} arithmetic
         * {@link #computeNext(Record)} applies when a pair enters the frame. The result is a
         * fresh summation of the frame, so it can differ from the incrementally maintained sums
         * in the last ULPs, which the frame-local contract allows.
         */
        private void putRestoredState(MapValue value, long startOffset, long count, long size, long capacity) {
            double sumX = 0.0;
            double sumXX = 0.0;
            double sumY = 0.0;
            double sumYY = 0.0;
            double sumXY = 0.0;
            for (long i = 0; i < count; i++) {
                final long rec = startOffset + i * RECORD_SIZE;
                final double x = memory.getDouble(rec + Long.BYTES);
                final double y = memory.getDouble(rec + Long.BYTES + Double.BYTES);
                sumX += x;
                sumXX += x * x;
                sumY += y;
                sumYY += y * y;
                sumXY += x * y;
            }
            value.putDouble(0, sumX);
            value.putDouble(1, sumXX);
            value.putDouble(2, sumY);
            value.putDouble(3, sumYY);
            value.putDouble(4, sumXY);
            value.putLong(5, count);
            value.putLong(6, startOffset);
            value.putLong(7, size);
            value.putLong(8, capacity);
            value.putLong(9, 0L);
            if (tombstoneValueIndex >= 0) {
                value.putByte(tombstoneValueIndex, (byte) 0);
            }
        }

        // The shared reader checks only the frame size's sign. The bounded frame start
        // supportsCheckpointState() admits keeps the frame a prefix of the ring, so a count past
        // the buffered pairs is a corrupt root, not a state to continue from - and the
        // re-accumulation would read past the ring.
        private void validateRestoredFrame(long count, long size) {
            if (count < 0 || count > size) {
                throw CairoException.critical(0)
                        .put("live view checkpoint bivariate RANGE frame size out of range [frameSize=").put(count)
                        .put(", rowCount=").put(size).put(']');
            }
        }

        /**
         * Writes restored ring triples straight into the partition's freshly sized slab. Reused
         * across partitions so a restore that walks thousands of them allocates nothing per
         * partition.
         */
        private class RingRestoreSink implements LiveViewCheckpointRingStateSource.Decimal128RowConsumer {
            private long capacity;
            private long rows;
            private long startOffset;

            @Override
            public void accept(long timestamp, long xBits, long yBits) {
                final double x = Double.longBitsToDouble(xBits);
                final double y = Double.longBitsToDouble(yBits);
                // computeNext buffers finite pairs only, so a non-finite half here is a corrupt
                // value page; the sums re-accumulated from it would never recover.
                if (!Numbers.isFinite(x) || !Numbers.isFinite(y)) {
                    throw CairoException.critical(0)
                            .put("live view checkpoint bivariate RANGE ring pair is not finite");
                }
                // The slab is sized from the reader's row count; a replay running past it would
                // write over whatever the arena appends next.
                if (rows >= capacity) {
                    throw CairoException.critical(0)
                            .put("live view checkpoint bivariate RANGE ring overflows its slab [capacity=")
                            .put(capacity).put(']');
                }
                final long rec = startOffset + rows * RECORD_SIZE;
                memory.putLong(rec, timestamp);
                memory.putDouble(rec + Long.BYTES, x);
                memory.putDouble(rec + Long.BYTES + Double.BYTES, y);
                rows++;
            }

            private void of(long startOffset, long capacity) {
                this.startOffset = startOffset;
                this.capacity = capacity;
                this.rows = 0;
            }
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
        // Deep copy of the partition-by key column types for the snapshot codec. The
        // factory reuses its partitionByKeyTypes buffer across compiles. Null outside LV.
        private final ArrayColumnTypes keyColumnTypes;
        private final boolean liveView;
        // Full value layout, tombstone slot included. Null outside LV.
        private final ArrayColumnTypes mapValueTypes;
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
                String name,
                ColumnTypes partitionByKeyTypes,
                boolean liveView
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
            this.liveView = liveView;
            if (liveView) {
                ArrayColumnTypes keyTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = partitionByKeyTypes.getColumnCount(); i < n; i++) {
                    keyTypesCopy.add(partitionByKeyTypes.getColumnType(i));
                }
                this.keyColumnTypes = keyTypesCopy;
                ArrayColumnTypes valueTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.getColumnCount(); i < n; i++) {
                    valueTypesCopy.add(BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.getColumnType(i));
                }
                this.mapValueTypes = valueTypesCopy;
                this.tombstoneValueIndex = 8;
            } else {
                this.keyColumnTypes = null;
                this.mapValueTypes = null;
                this.tombstoneValueIndex = -1;
            }
        }

        @Override
        public int checkpointStateFormatVersion() {
            return 1;
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
            // 8 - tombstone (live view only)
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
                if (tombstoneValueIndex >= 0) {
                    value.putByte(tombstoneValueIndex, (byte) 0);
                }
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

        /**
         * Writes {@code (sumX, sumXX, sumY, sumYY, sumXY, count, loIdx)} followed by the
         * partition's whole ring of {@code (x, y)} pairs, NaN placeholders included. The ring
         * is always {@code bufferSize} pairs long, so the image width depends on the declared
         * frame rather than on the partition.
         */
        @Override
        public void freezeCheckpointState(LiveViewStatePageWriter sink, MapValue value) {
            for (int i = 0; i < 5; i++) {
                sink.putDouble(value.getDouble(i));
            }
            sink.putLong(value.getLong(5));
            sink.putLong(value.getLong(6));
            final long startOffset = value.getLong(7);
            for (int i = 0; i < bufferSize; i++) {
                final long slotOffset = startOffset + (long) i * SLOT_SIZE;
                sink.putDouble(memory.getDouble(slotOffset));
                sink.putDouble(memory.getDouble(slotOffset + Double.BYTES));
            }
        }

        @Override
        public ColumnTypes getCheckpointKeyColumnTypes() {
            return keyColumnTypes;
        }

        @Override
        public int getCheckpointKeyStartIndex() {
            return mapValueTypes != null
                    ? mapValueTypes.getColumnCount()
                    : BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES.getColumnCount();
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
        public Map getPartitionMap() {
            return map;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public boolean hasFrameLocalCheckpointState() {
            // The ring holds the frame's own N (x, y) pairs and the five sums plus the count
            // accumulate over exactly those, so a warm-up of N predecessors reconstructs the
            // state. The replayed ring starts at a different rotation and the replayed sums
            // skip the rounding the dropped rows left behind, which the contract allows: the
            // values emitted are the same, up to the documented floating drift.
            return true;
        }

        @Override
        public void onCheckpointRestoreBegin() {
            super.onCheckpointRestoreBegin();
            memory.jumpTo(0);
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), result);
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
        public void resetPartition(Record record) {
            // ANCHOR-driven reset. Drop the partition's frame to empty: the five sums, the
            // count and loIdx go to zero and every ring pair back to NaN, so the next row
            // re-anchors cleanly. The ring's startOffset (slot 7) stays allocated and the
            // next row's write reuses it from index 0.
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            if (value != null) {
                final long startOffset = value.getLong(7);
                for (int i = 0; i < 5; i++) {
                    value.putDouble(i, 0.0);
                }
                value.putLong(5, 0L);
                value.putLong(6, 0L);
                for (int i = 0; i < bufferSize; i++) {
                    final long slotOffset = startOffset + (long) i * SLOT_SIZE;
                    memory.putDouble(slotOffset, Double.NaN);
                    memory.putDouble(slotOffset + Double.BYTES, Double.NaN);
                }
                if (!value.isNew() && tombstoneValueIndex >= 0 && value.getByte(tombstoneValueIndex) != 1) {
                    value.putByte(tombstoneValueIndex, (byte) 1);
                    tombstoneCount++;
                }
            }
        }

        @Override
        public long restoreCheckpointState(LiveViewStatePageReader source, long offset, MapValue value) {
            final long ringBytes = (long) bufferSize * SLOT_SIZE;
            for (int i = 0; i < 5; i++) {
                value.putDouble(i, source.getDouble(offset));
                offset += Double.BYTES;
            }
            final long partitionCount = source.getLong(offset);
            offset += Long.BYTES;
            final long loIdx = source.getLong(offset);
            offset += Long.BYTES;
            final long newStartOffset = memory.appendAddressFor(ringBytes) - memory.getPageAddress(0);
            for (int i = 0; i < bufferSize; i++) {
                final long slotOffset = newStartOffset + (long) i * SLOT_SIZE;
                memory.putDouble(slotOffset, source.getDouble(offset));
                offset += Double.BYTES;
                memory.putDouble(slotOffset + Double.BYTES, source.getDouble(offset));
                offset += Double.BYTES;
            }
            value.putLong(5, partitionCount);
            value.putLong(6, loIdx);
            value.putLong(7, newStartOffset);
            if (tombstoneValueIndex >= 0) {
                value.putByte(tombstoneValueIndex, (byte) 0);
            }
            return offset;
        }

        @Override
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            // Bind the tracker on BOTH native structures: super handles the unbounded
            // per-partition map (and the compaction scratch), this handles the ring
            // buffer. Omitting super left the map untracked, so a high-cardinality
            // PARTITION BY grew it past the per-query limit unaccounted.
            super.setMemoryTracker(tracker);
            memory.setMemoryTracker(tracker);
        }

        @Override
        public boolean supportsCheckpointState() {
            return liveView
                    && keyColumnTypes != null
                    && LiveViewSnapshotKeyCodec.isAllTypesSupported(keyColumnTypes);
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
            // memory allocates lazily on reopen(), under the tracker bound by the cursor
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
                    if (Numbers.saturatedAbsDiff(timestamp, ts) > maxDiff) {
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
                    long diff = Numbers.saturatedAbsDiff(ts, timestamp);

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
                    if (Numbers.saturatedAbsDiff(timestamp, ts) >= minDiff) {
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
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), result);
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
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            memory.setMemoryTracker(tracker);
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
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), result);
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
        private final CairoConfiguration configuration;
        private final boolean isCorrelation;
        private final boolean isSample;
        private final ArrayColumnTypes keyColumnTypes;
        private final boolean liveView;
        private final ArrayColumnTypes mapValueTypes;
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
                String name,
                ColumnTypes partitionByKeyTypes,
                boolean liveView,
                CairoConfiguration configuration
        ) {
            super(map, partitionByRecord, partitionBySink, argY, argX);
            this.argY = argY;
            this.isCorrelation = isCorrelation;
            this.isSample = isSample;
            this.name = name;
            this.liveView = liveView;
            this.configuration = configuration;
            if (liveView) {
                ArrayColumnTypes keyTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = partitionByKeyTypes.getColumnCount(); i < n; i++) {
                    keyTypesCopy.add(partitionByKeyTypes.getColumnType(i));
                }
                this.keyColumnTypes = keyTypesCopy;
                ArrayColumnTypes valueTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = BIVAR_COLUMN_TYPES_LV.getColumnCount(); i < n; i++) {
                    valueTypesCopy.add(BIVAR_COLUMN_TYPES_LV.getColumnType(i));
                }
                this.mapValueTypes = valueTypesCopy;
                this.tombstoneValueIndex = 6;
            } else {
                this.keyColumnTypes = null;
                this.mapValueTypes = null;
                this.tombstoneValueIndex = -1;
            }
        }

        @Override
        protected Map newCompactionScratch() {
            return MapFactory.createUnorderedMap(configuration, keyColumnTypes, mapValueTypes);
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
                if (tombstoneValueIndex >= 0) {
                    value.putByte(tombstoneValueIndex, (byte) 0);
                }
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
        public Map getPartitionMap() {
            return map;
        }

        @Override
        public ColumnTypes getCheckpointKeyColumnTypes() {
            return keyColumnTypes;
        }

        @Override
        public int getCheckpointKeyStartIndex() {
            return mapValueTypes != null
                    ? mapValueTypes.getColumnCount()
                    : BIVAR_COLUMN_TYPES.getColumnCount();
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), result);
        }

        @Override
        public void reopen() {
            super.reopen();
            tombstoneCount = 0;
        }

        @Override
        public void reset() {
            super.reset();
            tombstoneCount = 0;
        }

        @Override
        public void resetPartition(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            if (value != null) {
                value.putDouble(0, 0.0);
                value.putDouble(1, 0.0);
                value.putDouble(2, 0.0);
                value.putDouble(3, 0.0);
                value.putDouble(4, 0.0);
                value.putLong(5, 0L);
                if (!value.isNew() && tombstoneValueIndex >= 0 && value.getByte(tombstoneValueIndex) != 1) {
                    value.putByte(tombstoneValueIndex, (byte) 1);
                    tombstoneCount++;
                }
            }
        }

        @Override
        public long restoreCheckpointState(LiveViewStatePageReader source, long offset, MapValue value) {
            for (int i = 0; i < 5; i++) {
                value.putDouble(i, source.getDouble(offset));
                offset += Double.BYTES;
            }
            value.putLong(5, source.getLong(offset));
            offset += Long.BYTES;
            if (tombstoneValueIndex >= 0) {
                value.putByte(tombstoneValueIndex, (byte) 0);
            }
            return offset;
        }

        @Override
        public int checkpointStateFormatVersion() {
            return 1;
        }

        @Override
        public void freezeCheckpointState(LiveViewStatePageWriter sink, MapValue value) {
            for (int i = 0; i < 5; i++) {
                sink.putDouble(value.getDouble(i));
            }
            sink.putLong(value.getLong(5));
        }

        @Override
        public boolean supportsCheckpointState() {
            return liveView
                    && keyColumnTypes != null
                    && LiveViewSnapshotKeyCodec.isAllTypesSupported(keyColumnTypes);
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

        @Override
        public void toTop() {
            super.toTop();
            tombstoneCount = 0;
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
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), result);
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
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), result);
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

        BIVAR_COLUMN_TYPES_LV = new ArrayColumnTypes();
        BIVAR_COLUMN_TYPES_LV.add(ColumnType.DOUBLE);  // sumX / meanX
        BIVAR_COLUMN_TYPES_LV.add(ColumnType.DOUBLE);  // sumXX
        BIVAR_COLUMN_TYPES_LV.add(ColumnType.DOUBLE);  // sumY / meanY
        BIVAR_COLUMN_TYPES_LV.add(ColumnType.DOUBLE);  // sumYY
        BIVAR_COLUMN_TYPES_LV.add(ColumnType.DOUBLE);  // sumXY
        BIVAR_COLUMN_TYPES_LV.add(ColumnType.LONG);    // count
        BIVAR_COLUMN_TYPES_LV.add(ColumnType.BYTE);    // tombstone (anchor-driven compaction)

        BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES = new ArrayColumnTypes();
        BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.DOUBLE);  // sumX
        BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.DOUBLE);  // sumXX
        BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.DOUBLE);  // sumY
        BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.DOUBLE);  // sumYY
        BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.DOUBLE);  // sumXY
        BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG);    // finite count in frame
        BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG);    // lo index
        BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG);    // start offset

        BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES_LV = new ArrayColumnTypes();
        BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.DOUBLE);  // sumX
        BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.DOUBLE);  // sumXX
        BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.DOUBLE);  // sumY
        BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.DOUBLE);  // sumYY
        BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.DOUBLE);  // sumXY
        BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.LONG);    // finite count in frame
        BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.LONG);    // lo index
        BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.LONG);    // start offset
        BIVAR_OVER_PARTITION_ROWS_COLUMN_TYPES_LV.add(ColumnType.BYTE);    // tombstone

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

        BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES_LV = new ArrayColumnTypes();
        BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.DOUBLE); // sumX
        BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.DOUBLE); // sumXX
        BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.DOUBLE); // sumY
        BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.DOUBLE); // sumYY
        BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.DOUBLE); // sumXY
        BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.LONG);   // finite count in frame
        BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.LONG);   // start offset
        BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.LONG);   // ring size
        BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.LONG);   // ring capacity
        BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.LONG);   // first index
        BIVAR_OVER_PARTITION_RANGE_COLUMN_TYPES_LV.add(ColumnType.BYTE);   // tombstone
    }
}

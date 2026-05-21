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
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

public class SumDoubleWindowFunctionFactory extends AbstractWindowFunctionFactory {

    private static final String NAME = "sum";
    private static final String SIGNATURE = NAME + "(D)";
    private static final ArrayColumnTypes SUM_COLUMN_TYPES;
    private static final ArrayColumnTypes SUM_COLUMN_TYPES_LV;

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
                // moving sum over whole partition (no order by, default frame) or (order by, unbounded preceding to unbounded following)
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            SUM_COLUMN_TYPES
                    );

                    return new SumOverPartitionFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0)
                    );
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    final boolean liveView = windowContext.isLiveView();
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            liveView ? SUM_COLUMN_TYPES_LV : SUM_COLUMN_TYPES
                    );

                    // same as for rows because calculation stops at current rows even if there are 'equal' following rows
                    return new SumOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
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
                                liveView
                                        ? AvgDoubleWindowFunctionFactory.AVG_OVER_PARTITION_RANGE_COLUMN_TYPES_LV
                                        : AvgDoubleWindowFunctionFactory.AVG_OVER_PARTITION_RANGE_COLUMN_TYPES
                        );
                        mem = Vm.getCARWInstance(
                                configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(),
                                MemoryTag.NATIVE_CIRCULAR_BUFFER
                        );

                        // moving sum over range between timestamp - rowsLo and timestamp + rowsHi (inclusive)
                        return new SumOverPartitionRangeFrameFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                rowsLo,
                                rowsHi,
                                args.get(0),
                                mem,
                                configuration.getSqlWindowInitialRangeBufferSize(),
                                timestampIndex,
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
                            liveView ? SUM_COLUMN_TYPES_LV : SUM_COLUMN_TYPES
                    );

                    return new SumOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            partitionByKeyTypes,
                            liveView,
                            configuration
                    );
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new SumOverCurrentRowFunction(args.get(0));
                } // whole partition
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            SUM_COLUMN_TYPES
                    );

                    return new SumOverPartitionFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0)
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
                                liveView
                                        ? AvgDoubleWindowFunctionFactory.AVG_OVER_PARTITION_ROWS_COLUMN_TYPES_LV
                                        : AvgDoubleWindowFunctionFactory.AVG_OVER_PARTITION_ROWS_COLUMN_TYPES
                        );
                        mem = Vm.getCARWInstance(
                                configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(),
                                MemoryTag.NATIVE_CIRCULAR_BUFFER
                        );

                        // moving sum over preceding N rows
                        return new SumOverPartitionRowsFrameFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                rowsLo,
                                rowsHi,
                                args.get(0),
                                mem,
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
                    return new SumOverWholeResultSetFunction(args.get(0));
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    // same as for rows because calculation stops at current rows even if there are 'equal' following rows
                    return new SumOverUnboundedRowsFrameFunction(args.get(0));
                } // range between [unbounded | x] preceding and [x preceding | current row]
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }

                    int timestampIndex = windowContext.getTimestampIndex();

                    // moving sum over range between timestamp - rowsLo and timestamp + rowsHi (inclusive)
                    return new SumOverRangeFrameFunction(
                            rowsLo,
                            rowsHi,
                            args.get(0),
                            configuration,
                            timestampIndex
                    );
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                //between unbounded preceding and current row
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new SumOverUnboundedRowsFrameFunction(args.get(0));
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new SumOverCurrentRowFunction(args.get(0));
                } // whole result set
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new SumOverWholeResultSetFunction(args.get(0));
                } // between [unbounded | x] preceding and [x preceding | current row]
                else {
                    MemoryARW mem = Vm.getCARWInstance(
                            configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(),
                            MemoryTag.NATIVE_CIRCULAR_BUFFER
                    );

                    return new SumOverRowsFrameFunction(
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
    static class SumOverCurrentRowFunction extends AvgDoubleWindowFunctionFactory.AvgOverCurrentRowFunction {
        SumOverCurrentRowFunction(Function arg) {
            super(arg);
        }

        @Override
        public String getName() {
            return NAME;
        }
    }

    // handles sum() over (partition by x)
    // order by is absent so default frame mode includes all rows in partition
    static class SumOverPartitionFunction extends AvgDoubleWindowFunctionFactory.AvgOverPartitionFunction {

        public SumOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        @Override
        public String getName() {
            return NAME;
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
                    value.putDouble(0, sum);
                }
            }
        }
    }

    // Handles sum() over (partition by x order by ts range between [unbounded | y] preceding and [z preceding | current row])
    // Removable cumulative aggregation with timestamp & value stored in resizable ring buffers
    // When lower bound is unbounded we add but immediately discard any values that enter the frame so buffer should only contain values
    // between upper bound and current row's value.
    public static class SumOverPartitionRangeFrameFunction extends AvgDoubleWindowFunctionFactory.AvgOverPartitionRangeFrameFunction {

        public SumOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                ColumnTypes partitionByKeyTypes,
                boolean liveView
        ) {
            super(map, partitionByRecord, partitionBySink, rangeLo, rangeHi, arg, memory, initialBufferSize, timestampIdx,
                    partitionByKeyTypes, liveView);
        }

        @Override
        public double getDouble(Record rec) {
            return sum;
        }

        @Override
        public String getName() {
            return NAME;
        }
    }

    // handles sum() over (partition by x [order by o] rows between y and z)
    // removable cumulative aggregation
    public static class SumOverPartitionRowsFrameFunction extends AvgDoubleWindowFunctionFactory.AvgOverPartitionRowsFrameFunction {
        public SumOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                ColumnTypes partitionByKeyTypes,
                boolean liveView
        ) {
            super(map, partitionByRecord, partitionBySink, rowsLo, rowsHi, arg, memory,
                    partitionByKeyTypes, liveView);
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), sum);
        }
    }

    // Handles sum() over ([order by ts] range between [unbounded | x] preceding and [ x preceding | current row ] ); no partition by key
    // When lower bound is unbounded we add but immediately discard any values that enter the frame so buffer should only contain values
    // between upper bound and current row's value .
    public static class SumOverRangeFrameFunction extends AvgDoubleWindowFunctionFactory.AvgOverRangeFrameFunction {
        public SumOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx
        ) {
            super(rangeLo, rangeHi, arg, configuration, timestampIdx);
        }

        public SumOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                long initialCapacity,
                MemoryARW memory,
                int timestampIdx
        ) {
            super(rangeLo, rangeHi, arg, initialCapacity, memory, timestampIdx);
        }


        @Override
        public double getDouble(Record rec) {
            return externalSum;
        }

        @Override
        public String getName() {
            return NAME;
        }
    }

    // Handles sum() over ([order by o] rows between y and z); there's no partition by.
    // Removable cumulative aggregation.
    public static class SumOverRowsFrameFunction extends AvgDoubleWindowFunctionFactory.AvgOverRowsFrameFunction {

        public SumOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory) {
            super(arg, rowsLo, rowsHi, memory);
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), externalSum);
        }

        @Override
        public void toTop() {
            super.toTop();
            externalSum = 0;
        }
    }

    // Handles:
    // - sum(a) over (partition by x rows between unbounded preceding and current row)
    // - sum(a) over (partition by x order by ts range between unbounded preceding and current row)
    // Doesn't require value buffering.
    static class SumOverUnboundedPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        private final CairoConfiguration configuration;
        private final ArrayColumnTypes keyColumnTypes;
        private final boolean liveView;
        // Full value layout (including tombstone slot) for the
        // newCompactionScratch() scratch Map used by the frontier sweep. Null
        // outside live-view mode.
        private final ArrayColumnTypes mapValueTypes;
        // Value-slot index of the per-partition tombstone byte; -1 outside LV.
        private double sum;
        // Single-writer (refresh worker), not volatile.

        public SumOverUnboundedPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                Function arg,
                ColumnTypes partitionByKeyTypes,
                boolean liveView,
                CairoConfiguration configuration
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.liveView = liveView;
            this.configuration = configuration;
            this.keyColumnTypes = new ArrayColumnTypes();
            for (int i = 0, n = partitionByKeyTypes.getColumnCount(); i < n; i++) {
                this.keyColumnTypes.add(partitionByKeyTypes.getColumnType(i));
            }
            if (liveView) {
                ArrayColumnTypes valueTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = SUM_COLUMN_TYPES_LV.getColumnCount(); i < n; i++) {
                    valueTypesCopy.add(SUM_COLUMN_TYPES_LV.getColumnType(i));
                }
                this.mapValueTypes = valueTypesCopy;
                this.tombstoneValueIndex = 2;
            } else {
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
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            double sum;
            long count;

            if (value.isNew()) {
                if (tombstoneValueIndex >= 0) {
                    value.putByte(tombstoneValueIndex, (byte) 0);
                }
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
        public Map getPartitionMap() {
            return map;
        }

        @Override
        public ColumnTypes getSnapshotKeyColumnTypes() {
            return keyColumnTypes;
        }

        @Override
        public int getSnapshotKeyStartIndex() {
            return mapValueTypes != null
                    ? mapValueTypes.getColumnCount()
                    : SUM_COLUMN_TYPES.getColumnCount();
        }

        @Override
        public void markPartitionAlive(Record record) {
            if (tombstoneValueIndex < 0 || tombstoneCount == 0) {
                return;
            }
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            if (value != null && value.getByte(tombstoneValueIndex) == 1) {
                value.putByte(tombstoneValueIndex, (byte) 0);
                tombstoneCount--;
            }
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), sum);
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
            // ANCHOR-driven reset. Zero the [sum, count] slots; next
            // computeNext re-anchors on the post-reset row.
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            if (value != null) {
                value.putDouble(0, 0.0);
                value.putLong(1, 0L);
                if (!value.isNew() && tombstoneValueIndex >= 0 && value.getByte(tombstoneValueIndex) != 1) {
                    value.putByte(tombstoneValueIndex, (byte) 1);
                    tombstoneCount++;
                }
            }
        }

        @Override
        public void restore(MemoryR source, int formatVersion) {
            map.clear();
            tombstoneCount = 0;
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
                if (tombstoneValueIndex >= 0) {
                    value.putByte(tombstoneValueIndex, (byte) 0);
                }
            }
        }

        @Override
        public long restorePartitionState(MemoryR source, long offset, MapValue value, int formatVersion) {
            value.putDouble(0, source.getDouble(offset));
            offset += Double.BYTES;
            value.putLong(1, source.getLong(offset));
            offset += Long.BYTES;
            if (tombstoneValueIndex >= 0) {
                value.putByte(tombstoneValueIndex, (byte) 0);
            }
            return offset;
        }

        @Override
        public void snapshot(MemoryA sink) {
            // Two-pass walk: count non-tombstoned partitions, then emit each.
            MapRecordCursor cursor = map.getCursor();
            MapRecord record = map.getRecord();
            final long liveCount;
            if (tombstoneValueIndex < 0 || tombstoneCount == 0) {
                liveCount = map.size();
            } else {
                long count = 0;
                while (cursor.hasNext()) {
                    if (record.getValue().getByte(tombstoneValueIndex) != 1) {
                        count++;
                    }
                }
                liveCount = count;
            }
            sink.putLong(liveCount);

            cursor.toTop();
            final int keyStartIndex = mapValueTypes != null
                    ? mapValueTypes.getColumnCount()
                    : SUM_COLUMN_TYPES.getColumnCount();
            while (cursor.hasNext()) {
                final MapValue value = record.getValue();
                if (tombstoneValueIndex >= 0 && value.getByte(tombstoneValueIndex) == 1) {
                    continue;
                }
                LiveViewSnapshotKeyCodec.writeKey(sink, record, keyColumnTypes, keyStartIndex);
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
        public void snapshotPartitionState(MemoryA sink, MapValue value) {
            sink.putDouble(value.getDouble(0));
            sink.putLong(value.getLong(1));
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

        @Override
        public void toTop() {
            super.toTop();
            tombstoneCount = 0;
        }
    }

    // Handles sum() over (rows between unbounded preceding and current row); there's no partition by.
    public static class SumOverUnboundedRowsFrameFunction extends BaseWindowFunction implements WindowDoubleFunction {

        private long count = 0;
        private double externalSum;
        private double sum = 0.0;

        public SumOverUnboundedRowsFrameFunction(Function arg) {
            super(arg);
        }

        @Override
        public void computeNext(Record record) {
            double d = arg.getDouble(record);
            if (Numbers.isFinite(d)) {
                sum += d;
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
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), externalSum);
        }

        @Override
        public void reset() {
            super.reset();
            externalSum = Double.NaN;
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

            externalSum = Double.NaN;
            count = 0;
            sum = 0.0;
        }
    }

    // sum() over () - empty clause, no partition by no order by, no frame == default frame
    static class SumOverWholeResultSetFunction extends BaseWindowFunction implements WindowDoubleFunction {
        private long count;
        private double externalSum;
        private double sum;

        public SumOverWholeResultSetFunction(Function arg) {
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
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), externalSum);
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
        }

        @Override
        public void toTop() {
            super.toTop();

            externalSum = Double.NaN;
            count = 0;
            sum = 0.0;
        }
    }

    static {
        SUM_COLUMN_TYPES = new ArrayColumnTypes();
        SUM_COLUMN_TYPES.add(ColumnType.DOUBLE);
        SUM_COLUMN_TYPES.add(ColumnType.LONG);

        SUM_COLUMN_TYPES_LV = new ArrayColumnTypes();
        SUM_COLUMN_TYPES_LV.add(ColumnType.DOUBLE); // sum
        SUM_COLUMN_TYPES_LV.add(ColumnType.LONG);   // count
        SUM_COLUMN_TYPES_LV.add(ColumnType.BYTE);   // tombstone (anchor-driven compaction)
    }
}

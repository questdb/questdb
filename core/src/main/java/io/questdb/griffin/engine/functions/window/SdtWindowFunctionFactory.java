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
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

/**
 * Swinging Door Trending (SDT) window function.
 * <p>
 * {@code sdt(ts, value, compdev) over (order by ts)} marks each row {@code true} (keep) or
 * {@code false} (drop) according to the Swinging Door Trending line-simplification algorithm: a
 * point is kept only when it cannot be represented, within {@code compdev}, by a straight line
 * drawn between the last two kept points.
 * <p>
 * ORDER BY is required and custom framing is not allowed. PARTITION BY is supported via a
 * map-backed per-partition {@link SwingingDoor} state (see {@link SdtOverPartitionFunction}).
 */
public class SdtWindowFunctionFactory extends AbstractWindowFunctionFactory {

    public static final String NAME = "sdt";
    private static final String SIGNATURE = NAME + "(NDd)";
    private static final int RECORD_SIZE = Byte.BYTES;

    // Map value layout for per-partition SwingingDoor state.
    private static final ArrayColumnTypes SDT_STATE_TYPES;
    // slot indices
    private static final int ST_FLAGS = 0;        // LONG bitfield: bit0 hasAnchor, bit1 hasInterval, bit2 hasPending
    private static final int ST_ANCHOR_INDEX = 1; // LONG
    private static final int ST_ANCHOR_TS = 2;    // LONG
    private static final int ST_ANCHOR_VAL = 3;   // DOUBLE
    private static final int ST_SLOPE_HI = 4;     // DOUBLE
    private static final int ST_SLOPE_LO = 5;     // DOUBLE
    private static final int ST_PEND_INDEX = 6;   // LONG
    private static final int ST_PEND_TS = 7;      // LONG
    private static final int ST_PEND_VAL = 8;     // DOUBLE

    static {
        SDT_STATE_TYPES = new ArrayColumnTypes();
        SDT_STATE_TYPES.add(ColumnType.LONG);   // flags
        SDT_STATE_TYPES.add(ColumnType.LONG);   // anchorIndex
        SDT_STATE_TYPES.add(ColumnType.LONG);   // anchorTs
        SDT_STATE_TYPES.add(ColumnType.DOUBLE); // anchorValue
        SDT_STATE_TYPES.add(ColumnType.DOUBLE); // slopeHi
        SDT_STATE_TYPES.add(ColumnType.DOUBLE); // slopeLo
        SDT_STATE_TYPES.add(ColumnType.LONG);   // pendingIndex
        SDT_STATE_TYPES.add(ColumnType.LONG);   // pendingTs
        SDT_STATE_TYPES.add(ColumnType.DOUBLE); // pendingValue
    }

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    protected boolean supportNullsDesc() {
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
        WindowContext windowContext = sqlExecutionContext.getWindowContext();
        windowContext.validate(position, supportNullsDesc());

        if (!windowContext.isOrdered()) {
            throw SqlException.$(position, "sdt() requires ORDER BY");
        }
        if (!windowContext.isDefaultFrame()) {
            throw SqlException.$(position, "sdt() does not support framing; remove ROWS/RANGE clause");
        }

        Function tsArg = args.getQuick(0);
        Function valueArg = args.getQuick(1);
        Function compdevArg = args.getQuick(2);
        if (!compdevArg.isConstant()) {
            throw SqlException.$(argPositions.getQuick(2), "constant expected");
        }
        double compdev = compdevArg.getDouble(null);
        if (!(compdev >= 0) || !Numbers.isFinite(compdev)) { // rejects NaN and negatives
            throw SqlException.$(argPositions.getQuick(2), "compdev must be a non-negative finite constant");
        }

        boolean ignoreNulls = windowContext.isIgnoreNulls();
        MemoryARW mem = Vm.getCARWInstance(
                configuration.getSqlWindowStorePageSize(),
                configuration.getSqlWindowStoreMaxPages(),
                MemoryTag.NATIVE_CIRCULAR_BUFFER
        );

        if (windowContext.getPartitionByRecord() != null) {
            Map map = MapFactory.createUnorderedMap(
                    configuration,
                    windowContext.getPartitionByKeyTypes(),
                    SDT_STATE_TYPES
            );
            return new SdtOverPartitionFunction(
                    map,
                    windowContext.getPartitionByRecord(),
                    windowContext.getPartitionBySink(),
                    tsArg, valueArg, compdev, ignoreNulls, mem
            );
        }
        return new SdtOverWholeResultSetFunction(tsArg, valueArg, compdev, ignoreNulls, mem);
    }

    // ---- non-partitioned, two-pass ----
    static class SdtOverWholeResultSetFunction extends BaseWindowFunction implements SwingingDoor.Sink, Reopenable {
        private final double compdev;
        private final boolean ignoreNulls;
        private final MemoryARW mem;
        private final Function tsArg;
        private final SwingingDoor sd = new SwingingDoor();
        private long appendOffset; // pass1 write cursor (bytes)
        private ObjList<ExpressionNode> orderBy;
        private long readOffset;   // pass2 read cursor (bytes)

        SdtOverWholeResultSetFunction(Function tsArg, Function valueArg, double compdev,
                                       boolean ignoreNulls, MemoryARW mem) {
            super(valueArg); // BaseWindowFunction stores the "value" arg as `arg`
            this.tsArg = tsArg;
            this.compdev = compdev;
            this.ignoreNulls = ignoreNulls;
            this.mem = mem;
            sd.configure(compdev);
        }

        @Override
        public void close() {
            super.close();
            mem.close();
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
        public int getType() {
            return ColumnType.BOOLEAN;
        }

        @Override
        public boolean getBool(Record rec) {
            // Unused on the cached (pass1/pass2) execution path; present only to satisfy
            // the Function contract for a BOOLEAN-typed result.
            return false;
        }

        @Override
        public void getSelectedRows(DirectLongList dest) {
            // Row-selecting fusion (SUBSAMPLE sdt): enumerate the kept ABSOLUTE rows directly from the
            // finalized keep-byte buffer instead of materializing a BOOLEAN column + downstream Filter.
            // pass1 writes exactly one keep-byte per row at its own monotonic offset (appendOffset,
            // 0,1,2,... == the base chain row index on the fused OVER (ORDER BY ts) path), and the
            // SwingingDoor's eager-tentative-marking + back-patch has converged by end of input: every
            // row still tentatively kept (the last pending point, and any point before a RESPECT-NULLS
            // gap) keeps its keep=1 byte, so the buffer is final by the time this runs (after
            // preparePass2, which only rewinds readOffset). Walking 0..rowCount ascending and emitting
            // each keep==1 index is therefore byte-identical to the rows pass2 would have flagged true.
            dest.clear();
            final long rowCount = appendOffset / RECORD_SIZE;
            for (long absRow = 0; absRow < rowCount; absRow++) {
                if (mem.getByte(absRow * RECORD_SIZE) != 0) {
                    dest.add(absRow);
                }
            }
        }

        @Override
        public void initRecordComparator(
                SqlCodeGenerator sqlGenerator,
                RecordMetadata metadata,
                ArrayColumnTypes chainTypes,
                IntList orderIndices,
                ObjList<ExpressionNode> orderBy,
                IntList orderByDirection
        ) {
            this.orderBy = orderBy;
        }

        @Override
        public boolean isIgnoreNulls() {
            return ignoreNulls;
        }

        @Override
        public boolean isRowSelecting() {
            // Sole-window-function keep flag: after preparePass2 the keep-byte buffer is finalized, so
            // getSelectedRows() enumerates the exact kept absolute rows and the codegen keep-flag filter
            // can be fused into the cursor (SUBSAMPLE sdt only ever produces OVER (ORDER BY ts)).
            return true;
        }

        // Sink: write keep-byte at absolute buffer offset `index`.
        @Override
        public void mark(long index, boolean keep) {
            mem.putByte(index, keep ? (byte) 1 : (byte) 0);
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            long ts = tsArg.getTimestamp(record);
            double value = arg.getDouble(record);
            boolean isNull = Numbers.isNull(value); // Double NaN sentinel
            sd.accept(appendOffset, ts, value, isNull, ignoreNulls, this);
            appendOffset += RECORD_SIZE;
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            byte keep = mem.getByte(readOffset);
            readOffset += RECORD_SIZE;
            Unsafe.getUnsafe().putByte(spi.getAddress(recordOffset, columnIndex), keep);
        }

        @Override
        public void preparePass2() {
            readOffset = 0;
        }

        @Override
        public void reopen() {
            appendOffset = 0;
            readOffset = 0;
            sd.reset();
        }

        @Override
        public void reset() {
            super.reset();
            mem.close();
            appendOffset = 0;
            readOffset = 0;
        }

        @Override
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            mem.setMemoryTracker(tracker);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME).val('(').val(tsArg).val(", ").val(arg).val(", ").val(compdev).val(')');
            if (isIgnoreNulls()) {
                sink.val(" ignore nulls");
            }
            sink.val(" over (");
            if (orderBy != null) {
                sink.val("order by ");
                sink.val(orderBy);
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            appendOffset = 0;
            readOffset = 0;
            sd.reset();
        }
    }

    // ---- partitioned, two-pass; per-partition SwingingDoor state lives in the Map ----
    // Intentionally NOT row-selecting: keep bytes are written in ORDER BY order but the buffer would
    // interleave rows across partitions, so a single ascending absolute-row keep-set can't be exposed
    // cheaply. This is moot for SUBSAMPLE, which only desugars to OVER (ORDER BY ts) (no PARTITION BY),
    // and the fuse gate excludes PARTITION BY anyway, so this function stays on the unfused pass2 path.
    static class SdtOverPartitionFunction extends BasePartitionedWindowFunction implements SwingingDoor.Sink {
        private final double compdev;
        private final boolean ignoreNulls;
        private final MemoryARW mem;
        private final SwingingDoor scratch = new SwingingDoor();
        private final Function tsArg;
        private long appendOffset; // pass1 write cursor (bytes), monotonic across ALL partitions
        private ObjList<ExpressionNode> orderBy;
        private long readOffset;   // pass2 read cursor (bytes), monotonic across ALL partitions

        SdtOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink,
                                  Function tsArg, Function valueArg, double compdev, boolean ignoreNulls,
                                  MemoryARW mem) {
            super(map, partitionByRecord, partitionBySink, valueArg);
            this.tsArg = tsArg;
            this.compdev = compdev;
            this.ignoreNulls = ignoreNulls;
            this.mem = mem;
            scratch.configure(compdev);
        }

        @Override
        public void close() {
            super.close();
            mem.close();
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
        public int getType() {
            return ColumnType.BOOLEAN;
        }

        @Override
        public boolean getBool(Record rec) {
            // Unused on the cached (pass1/pass2) execution path; present only to satisfy
            // the Function contract for a BOOLEAN-typed result.
            return false;
        }

        @Override
        public void initRecordComparator(
                SqlCodeGenerator sqlGenerator,
                RecordMetadata metadata,
                ArrayColumnTypes chainTypes,
                IntList orderIndices,
                ObjList<ExpressionNode> orderBy,
                IntList orderByDirection
        ) {
            this.orderBy = orderBy;
        }

        @Override
        public boolean isIgnoreNulls() {
            return ignoreNulls;
        }

        // Sink: write keep-byte at absolute buffer offset `index`.
        @Override
        public void mark(long index, boolean keep) {
            mem.putByte(index, keep ? (byte) 1 : (byte) 0);
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            if (value.isNew()) {
                scratch.reset();
            } else {
                scratch.load(
                        value.getLong(ST_FLAGS),
                        value.getLong(ST_ANCHOR_INDEX),
                        value.getLong(ST_ANCHOR_TS),
                        value.getDouble(ST_ANCHOR_VAL),
                        value.getDouble(ST_SLOPE_HI),
                        value.getDouble(ST_SLOPE_LO),
                        value.getLong(ST_PEND_INDEX),
                        value.getLong(ST_PEND_TS),
                        value.getDouble(ST_PEND_VAL)
                );
            }

            long ts = tsArg.getTimestamp(record);
            double v = arg.getDouble(record);
            boolean isNull = Numbers.isNull(v); // Double NaN sentinel
            scratch.accept(appendOffset, ts, v, isNull, ignoreNulls, this);
            appendOffset += RECORD_SIZE;

            // store the mutated state back into this partition's map slot
            value.putLong(ST_FLAGS, scratch.packFlags());
            value.putLong(ST_ANCHOR_INDEX, scratch.anchorIndex());
            value.putLong(ST_ANCHOR_TS, scratch.anchorTs());
            value.putDouble(ST_ANCHOR_VAL, scratch.anchorValue());
            value.putDouble(ST_SLOPE_HI, scratch.slopeHi());
            value.putDouble(ST_SLOPE_LO, scratch.slopeLo());
            value.putLong(ST_PEND_INDEX, scratch.pendingIndex());
            value.putLong(ST_PEND_TS, scratch.pendingTs());
            value.putDouble(ST_PEND_VAL, scratch.pendingValue());
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            byte keep = mem.getByte(readOffset);
            readOffset += RECORD_SIZE;
            Unsafe.getUnsafe().putByte(spi.getAddress(recordOffset, columnIndex), keep);
        }

        @Override
        public void preparePass2() {
            readOffset = 0;
        }

        @Override
        public void reopen() {
            super.reopen();
            // mem allocates lazily on first use, matching SdtOverWholeResultSetFunction
            appendOffset = 0;
            readOffset = 0;
        }

        @Override
        public void reset() {
            super.reset();
            mem.close();
            appendOffset = 0;
            readOffset = 0;
        }

        @Override
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            super.setMemoryTracker(tracker);
            mem.setMemoryTracker(tracker);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME).val('(').val(tsArg).val(", ").val(arg).val(", ").val(compdev).val(')');
            if (isIgnoreNulls()) {
                sink.val(" ignore nulls");
            }
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            if (orderBy != null) {
                sink.val(" order by ");
                sink.val(orderBy);
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            appendOffset = 0;
            readOffset = 0;
        }
    }
}

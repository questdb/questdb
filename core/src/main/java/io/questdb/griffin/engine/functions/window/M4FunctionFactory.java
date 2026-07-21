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
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.M4Algorithm;
import io.questdb.griffin.engine.table.SubsampleAlgorithm;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

/**
 * m4(ts, value, target) window function.
 * <p>
 * Boolean "keep this row?" flag that marks up to 4 representative points (first, min, max, last) per
 * time bucket, using the same selection rule as SUBSAMPLE's M4 algorithm ({@link M4Algorithm#select}),
 * re-homed here over a per-partition native buffer of {@code (ordinal, ts, value)} entries built during
 * pass1 rather than SUBSAMPLE's whole-cursor buffer. Unlike the position-only {@code uniform}/{@code
 * cadence} window functions, m4 must inspect the value column to compute per-bucket min/max, so it
 * materializes every row before {@link #preparePass2()} runs the bucketing selection.
 * <p>
 * {@link BucketSelectWindowFunction} is intentionally algorithm-agnostic (it is handed a {@link
 * SubsampleAlgorithm} instance to drive the actual bucket selection): the minmax window function is
 * expected to reuse this same class with {@code MinMaxAlgorithm.INSTANCE} instead of duplicating the
 * buffering/pass1/pass2 plumbing.
 */
public class M4FunctionFactory extends AbstractWindowFunctionFactory {

    public static final String NAME = "m4";
    // Uppercase 'L' (not the constant-only lowercase 'l') so a non-constant target reaches newInstance
    // and gets the friendly "target must be a constant" message below, matching the
    // UniformFunctionFactory/CadenceFunctionFactory precedent - a lowercase-const-flagged signature
    // char would instead make the overload resolution silently not match, surfacing a generic
    // "there is no matching function" error from FunctionParser instead.
    private static final String SIGNATURE = NAME + "(NDL)";

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
        final WindowContext windowContext = sqlExecutionContext.getWindowContext();
        windowContext.validate(position, supportNullsDesc());

        if (!windowContext.isOrdered()) {
            throw SqlException.$(position, "m4() requires ORDER BY");
        }

        if (!windowContext.isDefaultFrame()) {
            throw SqlException.$(position, "m4() does not support framing; remove ROWS/RANGE clause");
        }

        if (windowContext.getPartitionByRecord() != null) {
            throw SqlException.$(position, "m4() does not support PARTITION BY");
        }

        final Function tsArg = args.getQuick(0);
        final Function valueArg = args.getQuick(1);
        final Function targetArg = args.getQuick(2);

        // Reproduce SqlCodeGenerator.generateSubsample's numeric-column check (same message) so
        // SUBSAMPLE m4(...) and this window function reject the same columns identically.
        final short valueTag = ColumnType.tagOf(valueArg.getType());
        if (valueTag != ColumnType.DOUBLE && valueTag != ColumnType.FLOAT
                && valueTag != ColumnType.INT && valueTag != ColumnType.LONG
                && valueTag != ColumnType.SHORT && valueTag != ColumnType.BYTE) {
            throw SqlException.$(argPositions.getQuick(1), "numeric column expected, got: ")
                    .put(ColumnType.nameOf(valueArg.getType()));
        }

        if (!targetArg.isConstant()) {
            throw SqlException.$(argPositions.getQuick(2), "target must be a constant");
        }
        long target = targetArg.getLong(null);
        if (target == Numbers.LONG_NULL || target < 2) {
            throw SqlException.$(argPositions.getQuick(2), "target points must be at least 2");
        }
        if (target > Integer.MAX_VALUE) {
            throw SqlException.$(argPositions.getQuick(2), "target points exceeds maximum of ").put(Integer.MAX_VALUE);
        }

        return new BucketSelectWindowFunction(tsArg, valueArg, target, M4Algorithm.INSTANCE, NAME);
    }

    // m4(ts, value, target) over (order by xxx) - no partition by, no framing.
    //
    // Shared base for value-inspecting bucket-selection window functions (m4 now, minmax later):
    // materializes (ordinal, ts, value) into a growable native buffer during pass1, then hands the
    // buffer to the supplied SubsampleAlgorithm in preparePass2. The returned selected buffer
    // positions are ascending (guaranteed by every SubsampleAlgorithm implementation, which walks the
    // buffer in ts order and only ever advances forward), so pass2 can mark keeps with the same
    // monotonic-pointer walk UniformFunctionFactory/CadenceFunctionFactory use for their own
    // (position-only) `selected` lists.
    static class BucketSelectWindowFunction extends BaseWindowFunction implements Reopenable {

        private static final long INITIAL_CAPACITY = 64;
        private final SubsampleAlgorithm algorithm;
        private final String name;
        private final DirectLongList selected = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT);
        private final long target;
        private final Function tsArg;
        private final Function valueArg;
        private long buffer;
        private long bufferCapacity; // in entries
        private SqlExecutionCircuitBreaker circuitBreaker;
        private long count;          // running row counter during pass1; becomes bufferSize
        private boolean lastKeep;    // last keep-flag computed in pass2; see getBool() below
        private ObjList<ExpressionNode> orderBy;
        // pass1 (count) and pass2 (pass2Ordinal/selIdx) are two separate traversals of the same
        // partition. CachedWindowRecordCursorFactory must replay the SAME WindowSortBuffer order
        // for both passes, or these counters (and the buffer positions stashed in `selected`) desync
        // and the wrong rows get marked kept. A future change to the cached-cursor traversal order
        // must preserve this pass1/pass2 ordering invariant.
        private long pass2Ordinal;   // running row counter during pass2 (same traversal order as pass1)
        private long selIdx;         // monotonic cursor into `selected` during pass2

        BucketSelectWindowFunction(Function tsArg, Function valueArg, long target, SubsampleAlgorithm algorithm, String name) {
            super(null);
            this.tsArg = tsArg;
            this.valueArg = valueArg;
            this.target = target;
            this.algorithm = algorithm;
            this.name = name;
        }

        @Override
        public void close() {
            super.close();
            Misc.free(tsArg);
            Misc.free(valueArg);
            selected.close();
            freeBuffer();
        }

        @Override
        public boolean getBool(Record rec) {
            // Not reached in normal operation: the keep flag is materialized directly into the
            // chain slot in pass2 (see below) and read back from there, never via getBool(). This
            // override is purely defensive against a future caller that reads the function itself.
            return lastKeep;
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
        public int getType() {
            return ColumnType.BOOLEAN;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            tsArg.init(symbolTableSource, executionContext);
            valueArg.init(symbolTableSource, executionContext);
            this.circuitBreaker = executionContext.getCircuitBreaker();
        }

        @Override
        public void initRecordComparator(
                SqlCodeGenerator sqlGenerator,
                RecordMetadata metadata,
                ArrayColumnTypes chainTypes,
                IntList orderIndices,
                ObjList<ExpressionNode> orderBy,
                IntList orderByDirection
        ) throws SqlException {
            this.orderBy = orderBy;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            final long ts = tsArg.getTimestamp(record);
            final double value = valueArg.getDouble(record);
            ensureCapacity();
            final long offset = count * SubsampleAlgorithm.ENTRY_SIZE;
            Unsafe.getUnsafe().putLong(buffer + offset, count);
            Unsafe.getUnsafe().putLong(buffer + offset + 8, ts);
            Unsafe.getUnsafe().putDouble(buffer + offset + 16, value);
            count++;
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            final boolean keep = selIdx < selected.size() && selected.get(selIdx) == pass2Ordinal;
            if (keep) {
                selIdx++;
            }
            pass2Ordinal++;
            lastKeep = keep;
            // BOOLEAN is a 1-byte chain column (see ColumnType.TYPE_SIZE[BOOLEAN]); write a byte,
            // not a long, or we'd corrupt the next column's storage.
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), (byte) (keep ? 1 : 0));
        }

        @Override
        public void preparePass2() {
            selIdx = 0;
            pass2Ordinal = 0;
            algorithm.select(buffer, (int) count, (int) target, selected, circuitBreaker);
        }

        @Override
        public void reopen() {
            count = 0;
            pass2Ordinal = 0;
            selIdx = 0;
            selected.reopen();
            selected.clear();
        }

        @Override
        public void reset() {
            super.reset();
            count = 0;
            pass2Ordinal = 0;
            selIdx = 0;
            selected.close();
            freeBuffer();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name);
            sink.val('(').val(tsArg).val(',').val(valueArg).val(',').val(target).val(')');
            if (orderBy != null) {
                sink.val(" over (");
                sink.val("order by ");
                sink.val(orderBy);
                sink.val(')');
            } else {
                sink.val(" over ()");
            }
        }

        @Override
        public void toTop() {
            super.toTop();
            count = 0;
            pass2Ordinal = 0;
            selIdx = 0;
            selected.clear();
        }

        private void ensureCapacity() {
            if (buffer == 0) {
                bufferCapacity = INITIAL_CAPACITY;
                buffer = Unsafe.malloc(bufferCapacity * SubsampleAlgorithm.ENTRY_SIZE, MemoryTag.NATIVE_FUNC_RSS);
            } else if (count >= bufferCapacity) {
                final long newCapacity = bufferCapacity << 1;
                buffer = Unsafe.realloc(
                        buffer,
                        bufferCapacity * SubsampleAlgorithm.ENTRY_SIZE,
                        newCapacity * SubsampleAlgorithm.ENTRY_SIZE,
                        MemoryTag.NATIVE_FUNC_RSS
                );
                bufferCapacity = newCapacity;
            }
        }

        private void freeBuffer() {
            if (buffer != 0) {
                Unsafe.free(buffer, bufferCapacity * SubsampleAlgorithm.ENTRY_SIZE, MemoryTag.NATIVE_FUNC_RSS);
                buffer = 0;
                bufferCapacity = 0;
            }
        }
    }
}

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
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.WindowSPI;
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
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

/**
 * uniform(n) window function.
 * Boolean "keep this row?" flag that marks an evenly-spaced subset of {@code n} rows out of the
 * ordered partition, using the same evenly-spaced-index formula as SUBSAMPLE's uniform algorithm:
 * for target N over n rows (n &gt; N), divisor = N-1, range = n-1, half = divisor/2, and the i-th
 * (0-based) kept ordinal is {@code (i*range + half) / divisor}, de-duplicated. When n &lt;= N every
 * row is kept.
 */
public class UniformFunctionFactory extends AbstractWindowFunctionFactory {

    public static final String NAME = "uniform";
    // LONG signature so both INT literals (auto-widened) and LONG literals resolve; the value is
    // validated to fit a positive long target below.
    private static final String SIGNATURE = NAME + "(L)";

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
            throw SqlException.$(position, "uniform() requires ORDER BY");
        }

        if (!windowContext.isDefaultFrame()) {
            throw SqlException.$(position, "uniform() does not support framing; remove ROWS/RANGE clause");
        }

        if (windowContext.getPartitionByRecord() != null) {
            throw SqlException.$(position, "uniform() does not support PARTITION BY");
        }

        // A bind-variable target that is unset at compile - and may be re-bound between executions -
        // is read PER-EXECUTION (see UniformFunction.init) rather than frozen here. A constant target
        // is range-validated right below (compile time, matching the pre-bind-var-support factory and
        // the legacy SUBSAMPLE cursor's own constant handling); a constant otherwise reads to the same
        // value at every open, so constant behavior is unchanged.
        Function targetArg = args.getQuick(0);
        int targetPosition = argPositions.getQuick(0);
        if (!targetArg.isConstant() && !targetArg.isRuntimeConstant()) {
            throw SqlException.$(targetPosition, "target must be a constant or bind variable");
        }
        // Preserve SUBSAMPLE target handling: resolve an UNDEFINED bind variable to LONG and reject
        // anything not convertible to LONG (e.g. a bind variable already bound to a non-numeric type).
        coerceRuntimeConstantType(targetArg, ColumnType.LONG, sqlExecutionContext, "target point count must be an integer", targetPosition);
        final short targetTypeTag = ColumnType.tagOf(targetArg.getType());
        if (targetTypeTag != ColumnType.INT && targetTypeTag != ColumnType.LONG
                && targetTypeTag != ColumnType.SHORT && targetTypeTag != ColumnType.BYTE) {
            throw SqlException.$(targetPosition, "integer expected for target point count");
        }

        // Constants and runtime constants share one range contract; runtime constants are re-read
        // in init() so rebinding between cursor opens remains supported.
        long resolvedTarget = 0;
        if (targetArg.isConstant()) {
            resolvedTarget = validateTarget(targetArg.getLong(null), targetPosition);
        }

        return new UniformFunction(
                targetArg,
                targetPosition,
                resolvedTarget,
                configuration.getSubsampleMaxRows(),
                position
        );
    }

    // uniform(n) over (order by xxx) - no partition by, no framing.
    static class UniformFunction extends BaseWindowFunction implements Reopenable {

        private final DirectLongList selected = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT, true);
        private final int functionPosition;
        // May be a bind variable / runtime constant, so its value is resolved every execution in
        // init() (before pass1/preparePass2 need it) rather than frozen at newInstance.
        private final long maxRows;
        private final Function targetArg;
        private final int targetPosition;
        private long count;          // running row counter during pass1; becomes totalRows
        private boolean keepAll;
        private boolean lastKeep;    // last keep-flag computed in pass2; see getBool() below
        private ObjList<ExpressionNode> orderBy;
        private long target;         // resolved in init() from targetArg for the current execution
        // pass1 (count) and pass2 (pass2Ordinal/selIdx) are two separate traversals of the same
        // partition. CachedWindowRecordCursorFactory must replay the SAME WindowSortBuffer order
        // for both passes, or these counters (and the ordinals stashed in `selected`) desync and
        // the wrong rows get marked kept. A future change to the cached-cursor traversal order
        // must preserve this pass1/pass2 ordering invariant.
        private long pass2Ordinal;   // running row counter during pass2 (same traversal order as pass1)
        private long selIdx;         // monotonic cursor into `selected` during pass2

        UniformFunction(Function targetArg, int targetPosition, long resolvedTarget, long maxRows, int functionPosition) {
            super(null);
            this.targetArg = targetArg;
            this.targetPosition = targetPosition;
            this.maxRows = maxRows;
            this.functionPosition = functionPosition;
            // For a constant target, already range-validated at newInstance (compile time); for a
            // bind-variable target this is an unused placeholder, overwritten every execution in
            // init() below.
            this.target = resolvedTarget;
        }

        @Override
        public void close() {
            super.close();
            Misc.free(targetArg);
            selected.close();
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            targetArg.init(symbolTableSource, executionContext);
            if (!targetArg.isConstant()) {
                // Resolve target for THIS execution: a bind-variable target is re-read (and
                // range-checked) every run, so re-binding between executions takes effect.
                target = validateTarget(targetArg.getLong(null), targetPosition);
            }
            // A constant target was already resolved and range-validated at newInstance (compile
            // time); it reads the same value every execution, so there is nothing to redo here.
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
        public void getSelectedRows(DirectLongList dest) {
            // Position-only: `selected` already holds ascending pass1 traversal ordinals (no null rows
            // are dropped), and keepAll means every traversal row 0..count-1 is kept. Byte-identical to the
            // rows pass2 would have flagged keep=true.
            dest.clear();
            if (keepAll) {
                for (long i = 0; i < count; i++) {
                    dest.add(i);
                }
            } else {
                for (long i = 0, n = selected.size(); i < n; i++) {
                    dest.add(selected.get(i));
                }
            }
        }

        @Override
        public boolean isRowSelecting() {
            return true;
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
            if (isSubsampleKeepFlag() && count >= maxRows) {
                throw CairoException.nonCritical().position(functionPosition)
                        .put("SUBSAMPLE input exceeds maximum of ").put(maxRows).put(" rows");
            }
            count++;
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            boolean keep;
            if (keepAll) {
                keep = true;
            } else {
                keep = selIdx < selected.size() && selected.get(selIdx) == pass2Ordinal;
                if (keep) {
                    selIdx++;
                }
            }
            pass2Ordinal++;
            lastKeep = keep;
            // BOOLEAN is a 1-byte chain column (see ColumnType.TYPE_SIZE[BOOLEAN]); write a byte,
            // not a long, or we'd corrupt the next column's storage.
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), (byte) (keep ? 1 : 0));
        }

        @Override
        public void preparePass2() {
            long totalRows = count;
            selected.clear();
            selIdx = 0;
            pass2Ordinal = 0;
            if (totalRows <= target) {
                keepAll = true;
                return;
            }
            keepAll = false;
            long divisor = target - 1;
            long range = totalRows - 1;
            long half = divisor / 2;
            long prev = -1;
            for (long i = 0; i < target; i++) {
                long pos = (i * range + half) / divisor;
                if (pos != prev) { // positions are non-decreasing; dedup consecutive repeats
                    selected.add(pos);
                    prev = pos;
                }
            }
        }

        @Override
        public void reopen() {
            count = 0;
            keepAll = false;
            pass2Ordinal = 0;
            selIdx = 0;
            selected.reopen();
            selected.clear();
        }

        @Override
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            selected.setMemoryTracker(tracker);
        }

        @Override
        public void reset() {
            super.reset();
            count = 0;
            keepAll = false;
            pass2Ordinal = 0;
            selIdx = 0;
            selected.close();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            // Render the constant value (byte-identical to the pre-bind-var plan); for a bind-variable
            // target `target` is not resolved until init(), so render the argument's own plan instead.
            sink.val('(');
            if (targetArg.isConstant()) {
                sink.val(targetArg.getLong(null));
            } else {
                sink.val(targetArg);
            }
            sink.val(')');
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
            keepAll = false;
            pass2Ordinal = 0;
            selIdx = 0;
            selected.clear();
        }
    }
}

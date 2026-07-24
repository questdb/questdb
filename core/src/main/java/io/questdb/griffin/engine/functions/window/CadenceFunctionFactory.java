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
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

/**
 * cadence(stride) window function.
 * <p>
 * Boolean "keep this row?" flag that marks a stride-spaced subset of the ordered partition, using
 * SUBSAMPLE's cadence selection rule over row ordinals 0..totalRows-1: ordinal 0 is always
 * kept; when {@code stride == 1} every row is kept; when {@code stride > totalRows} only ordinal 0
 * is kept (no last-row pin); otherwise ordinals {@code stride+offset, 2*stride+offset, ...} are kept
 * and the last ordinal is pinned unless already selected.
 * <p>
 * The optional second argument (see {@link CadenceSeedFunctionFactory}, signature {@code cadence(LL)})
 * supplies the stride offset's seed: no second argument -&gt; offset 0; a literal {@code NULL} second
 * argument -&gt; a fresh random
 * offset drawn from the execution's {@link Rnd} on every execution; any other constant (or bind
 * variable / runtime constant) -&gt; a deterministic offset derived from the seed value via a
 * splitmix64 mix, computed fresh every execution (so a bind-variable seed can change between runs).
 */
public class CadenceFunctionFactory extends AbstractWindowFunctionFactory {

    public static final String NAME = "cadence";
    static final int SEED_MODE_DETERMINISTIC = 1;
    static final int SEED_MODE_NONE = 0;
    static final int SEED_MODE_RANDOM = 2;
    // LONG signature so both INT literals (auto-widened) and LONG literals resolve; the value is
    // validated to fit the positive INT stride range below.
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
        return newInstance0(position, args, argPositions, configuration, sqlExecutionContext, false, supportNullsDesc());
    }

    // Shared by CadenceFunctionFactory (cadence(L)) and CadenceSeedFunctionFactory (cadence(LL)).
    static Function newInstance0(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext,
            boolean hasSeed,
            boolean supportNullsDesc
    ) throws SqlException {
        final WindowContext windowContext = sqlExecutionContext.getWindowContext();
        windowContext.validate(position, supportNullsDesc);

        if (!windowContext.isOrdered()) {
            throw SqlException.$(position, "cadence() requires ORDER BY");
        }

        if (!windowContext.isDefaultFrame()) {
            throw SqlException.$(position, "cadence() does not support framing; remove ROWS/RANGE clause");
        }

        if (windowContext.getPartitionByRecord() != null) {
            throw SqlException.$(position, "cadence() does not support PARTITION BY");
        }

        // A bind-variable stride that is unset at compile - and may be re-bound between executions -
        // is read PER-EXECUTION (see CadenceFunction.init) rather than frozen here. A constant stride
        // is range-validated right below (compile time, matching the pre-bind-var-support factory and
        // the legacy SUBSAMPLE cursor's own constant handling); a constant otherwise reads to the same
        // value at every open, so constant behavior is unchanged.
        Function strideArg = args.getQuick(0);
        int stridePosition = argPositions.getQuick(0);
        if (!strideArg.isConstant() && !strideArg.isRuntimeConstant()) {
            throw SqlException.$(stridePosition, "stride must be a constant or bind variable");
        }
        // Preserve SUBSAMPLE stride handling: resolve an UNDEFINED bind variable to LONG and reject
        // anything not convertible to LONG (e.g. a bind variable already bound to a non-numeric type).
        coerceRuntimeConstantType(strideArg, ColumnType.LONG, sqlExecutionContext, "stride must be an integer", stridePosition);
        final short strideTypeTag = ColumnType.tagOf(strideArg.getType());
        if (strideTypeTag != ColumnType.INT && strideTypeTag != ColumnType.LONG
                && strideTypeTag != ColumnType.SHORT && strideTypeTag != ColumnType.BYTE) {
            throw SqlException.$(stridePosition, "integer expected for stride");
        }

        // Constants and runtime constants share one range contract; runtime constants are re-read
        // in init() so rebinding between cursor opens remains supported.
        long resolvedStride = 0;
        if (strideArg.isConstant()) {
            resolvedStride = validateStride(strideArg.getLong(null), stridePosition);
        }

        Function seedFunc = null;
        int seedMode = SEED_MODE_NONE;
        int seedPosition = 0;
        if (hasSeed) {
            Function seedArg = args.getQuick(1);
            seedPosition = argPositions.getQuick(1);
            if (seedArg.isNullConstant()) {
                // Literal NULL -> random mode. seedArg is not needed at runtime.
                seedMode = SEED_MODE_RANDOM;
            } else {
                if (!seedArg.isConstant() && !seedArg.isRuntimeConstant()) {
                    throw SqlException.$(seedPosition, "seed must be a constant, bind variable, or NULL");
                }
                coerceRuntimeConstantType(
                        seedArg,
                        ColumnType.LONG,
                        sqlExecutionContext,
                        "integer or NULL expected for seed",
                        seedPosition
                );
                final short seedTypeTag = ColumnType.tagOf(seedArg.getType());
                if (seedTypeTag != ColumnType.INT && seedTypeTag != ColumnType.LONG
                        && seedTypeTag != ColumnType.SHORT && seedTypeTag != ColumnType.BYTE) {
                    throw SqlException.$(seedPosition, "integer or NULL expected for seed");
                }
                seedFunc = seedArg;
                seedMode = SEED_MODE_DETERMINISTIC;
            }
        }

        return new CadenceFunction(
                strideArg,
                stridePosition,
                resolvedStride,
                seedFunc,
                seedMode,
                seedPosition,
                configuration.getSubsampleMaxRows(),
                position
        );
    }

    // cadence(stride[, seed]) over (order by xxx) - no partition by, no framing.
    static class CadenceFunction extends BaseWindowFunction implements Reopenable {

        private final DirectLongList selected = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT, true);
        private final int functionPosition;
        private final long maxRows;
        private final int seedMode;
        private final int seedPosition;
        // Non-null only in SEED_MODE_DETERMINISTIC; may be a bind variable / runtime constant, so its
        // value is re-read every execution (in preparePass2, after init() refreshes it for this run).
        private final Function seedFunc;
        // May be a bind variable / runtime constant, so its value is resolved every execution in
        // init() (before pass1/preparePass2 need it) rather than frozen at newInstance.
        private final Function strideFunc;
        private final int stridePosition;
        private long count;          // running row counter during pass1; becomes totalRows
        private long stride;         // resolved in init() from strideFunc for the current execution
        // Resolved once per execution in init(), before the base cursor is scanned. Random mode uses
        // the legacy nextInt primitive exactly once whenever stride > 1, even for empty/short inputs.
        private long offset;
        private boolean keepAll;
        private boolean lastKeep;    // last keep-flag computed in pass2; see getBool() below
        private ObjList<ExpressionNode> orderBy;
        // pass1 (count) and pass2 (pass2Ordinal/selIdx) are two separate traversals of the same
        // partition. CachedWindowRecordCursorFactory must replay the SAME WindowSortBuffer order
        // for both passes, or these counters (and the ordinals stashed in `selected`) desync and
        // the wrong rows get marked kept. A future change to the cached-cursor traversal order
        // must preserve this pass1/pass2 ordering invariant.
        private long pass2Ordinal;   // running row counter during pass2 (same traversal order as pass1)
        private long selIdx;         // monotonic cursor into `selected` during pass2

        CadenceFunction(
                Function strideFunc,
                int stridePosition,
                long resolvedStride,
                Function seedFunc,
                int seedMode,
                int seedPosition,
                long maxRows,
                int functionPosition
        ) {
            super(null);
            this.strideFunc = strideFunc;
            this.stridePosition = stridePosition;
            this.maxRows = maxRows;
            this.functionPosition = functionPosition;
            // For a constant stride, already range-validated at newInstance (compile time); for a
            // bind-variable stride this is an unused placeholder, overwritten every execution in
            // init() below.
            this.stride = resolvedStride;
            this.seedFunc = seedFunc;
            this.seedMode = seedMode;
            this.seedPosition = seedPosition;
        }

        @Override
        public void close() {
            super.close();
            Misc.free(strideFunc);
            Misc.free(seedFunc);
            selected.close();
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
            // are dropped), and keepAll (stride==1) means every traversal row 0..count-1 is kept.
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
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            strideFunc.init(symbolTableSource, executionContext);
            if (!strideFunc.isConstant()) {
                // Resolve stride for THIS execution: a bind-variable stride is re-read (and range-checked)
                // every run, so re-binding between executions takes effect.
                stride = validateStride(strideFunc.getLong(null), stridePosition);
            }
            // A constant stride was already resolved and range-validated at newInstance (compile
            // time); it reads the same value every execution, so there is nothing to redo here.
            offset = 0;
            if (seedFunc != null) {
                seedFunc.init(symbolTableSource, executionContext);
                // Preserve cadence(1)'s no-op behavior: a correctly typed but unset seed is not read.
                if (stride > 1) {
                    final long seed = seedFunc.getLong(null);
                    if (seed == Numbers.LONG_NULL) {
                        throw SqlException.$(seedPosition, "seed must be set");
                    }
                    offset = deterministicOffset(seed, stride);
                }
            } else if (seedMode == SEED_MODE_RANDOM && stride > 1) {
                offset = executionContext.getRandom().nextInt((int) stride);
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
        ) throws SqlException {
            this.orderBy = orderBy;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            // Preserve the legacy cadence(1) no-op: it bypassed buffering and the SUBSAMPLE cap.
            if (isSubsampleKeepFlag() && stride > 1 && count >= maxRows) {
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
            if (stride == 1) {
                keepAll = true;
                return;
            }
            keepAll = false;
            if (totalRows <= 0) {
                return;
            }
            // Always keep the first row.
            selected.add(0);
            if (stride > totalRows) {
                // Only the first row, no last-row pin.
                return;
            }
            // long running index: stride + offset can exceed Integer.MAX_VALUE, which is why this
            // and totalRows/pos are long rather than int.
            for (long pos = stride + offset; pos < totalRows; pos += stride) {
                selected.add(pos);
            }
            long lastOrdinal = totalRows - 1;
            if (selected.size() == 0 || selected.get(selected.size() - 1) != lastOrdinal) {
                selected.add(lastOrdinal);
            }
        }

        @Override
        public void reopen() {
            count = 0;
            keepAll = false;
            pass2Ordinal = 0;
            selIdx = 0;
            selected.reopen();
            offset = 0;
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
            offset = 0;
            selected.close();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            // Render the constant value (byte-identical to the pre-bind-var plan); for a bind-variable
            // stride `stride` is not resolved until init(), so render the argument's own plan instead.
            sink.val('(');
            if (strideFunc.isConstant()) {
                sink.val(strideFunc.getLong(null));
            } else {
                sink.val(strideFunc);
            }
            if (seedMode == SEED_MODE_RANDOM) {
                sink.val(", null");
            } else if (seedMode == SEED_MODE_DETERMINISTIC) {
                sink.val(", ").val(seedFunc);
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

        // Splitmix64 finalizer preserves deterministic seed compatibility without mutating the
        // execution context's shared random generator.
        private static long deterministicOffset(long seed, long stride) {
            long h = seed;
            h = (h ^ (h >>> 30)) * 0xbf58476d1ce4e5b9L;
            h = (h ^ (h >>> 27)) * 0x94d049bb133111ebL;
            h = h ^ (h >>> 31);
            // floorMod (not Math.abs(h) % stride): Math.abs(Long.MIN_VALUE) stays negative, which
            // would return a negative offset for the one seed that hashes to it.
            return Math.floorMod(h, stride);
        }
    }
}

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

import io.questdb.PropertyKey;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoException;
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
import io.questdb.griffin.engine.table.SubsampleAlgorithm;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

/**
 * Shared implementation behind the value-inspecting bucket-selection window functions
 * {@code m4}, {@code minmax} and {@code lttb} - that is, {@code fn(ts, value, target)
 * over (order by ...)} with no PARTITION BY and no framing.
 * <p>
 * Materializes {@code (ts, value)} into a growable native buffer during pass1, then hands the
 * buffer to the injected {@link SubsampleAlgorithm} in {@link #preparePass2()}. The returned
 * selected buffer positions are ascending (guaranteed by every {@code SubsampleAlgorithm}
 * implementation, which walks the buffer in ts order and only ever advances forward), so pass2
 * can mark keeps with the same monotonic-pointer walk {@link UniformFunctionFactory}/
 * {@link CadenceFunctionFactory} use for their own (position-only) selections.
 * <p>
 * The class is algorithm-agnostic so the three factories share one copy of the buffering and
 * pass1/pass2 plumbing rather than duplicating it. It also owns the <b>single</b> NULL screening
 * site for all three functions: see {@link #pass1} and {@link SubsampleAlgorithm}'s NULL contract,
 * which the algorithms downstream rely on.
 */
class BucketSelectWindowFunction extends BaseWindowFunction implements Reopenable {

    private static final long INITIAL_CAPACITY = 64;
    private final SubsampleAlgorithm algorithm;
    private final int functionPosition;
    private final long maxRows;
    private final String name;
    // Per-traversal-row null bitset built in pass1 (1 bit/row, appended in traversal order).
    // pass2 consults it instead of re-deriving isNullRow(record) from a random-access
    // base re-read; see pass2NeedsBaseRecord(). Same native-memory lifecycle as `selected`
    // (allocate on reopen, clear on toTop, close on reset/close) - a prior real native leak on
    // the lttb gap scratch is the discipline mirrored here.
    private final DirectLongList nullBits = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT, true);
    private final DirectLongList selected = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT, true);
    // May be a bind variable / runtime constant, so its value is resolved every execution in
    // init() (before pass1/preparePass2 need it) rather than frozen at newInstance.
    private final Function targetArg;
    private final int targetPosition;
    private final Function tsArg;
    private final Function valueArg;
    private long buffer;
    private long bufferCapacity; // in entries
    private SqlExecutionCircuitBreaker circuitBreaker;
    private long count;          // running non-null row counter during pass1; becomes bufferSize.
    // Rows with a NULL ts or a NULL value are dropped from the buffer entirely (never
    // appended, never counted in `count`). This is the single screening site for m4/minmax/lttb;
    // see SubsampleAlgorithm's NULL contract, which the algorithms rely on to compare with plain
    // < / > and carry no non-finite guards of their own.
    // They still count toward the SUBSAMPLE row cap, which pass1 checks against rowCount.
    private boolean lastKeep;    // last keep-flag computed in pass2; see getBool() below
    // Previous buffered (non-null) timestamp seen by pass1; enforces the algorithms'
    // ascending-input precondition. Reads are gated on count > 0, so the field needs no
    // reset plumbing of its own - count's resets (reopen/toTop/reset) cover it.
    private long lastTs;
    @Nullable
    private MemoryTracker memoryTracker;
    private ObjList<ExpressionNode> orderBy;
    // pass1 (count) and pass2 (pass2Ordinal/selIdx) are two separate traversals of the same
    // partition. CachedWindowRecordCursorFactory must replay the SAME WindowSortBuffer order
    // for both passes, or these counters (and the buffer positions stashed in `selected`) desync
    // and the wrong rows get marked kept. A future change to the cached-cursor traversal order
    // must preserve this pass1/pass2 ordering invariant.
    // Because null rows are dropped from the buffer, buffer position is NOT the row ordinal -
    // pass2Ordinal instead counts only the non-null rows pass2 has visited so far (recomputing
    // isNullRow keeps it aligned with pass1's bufferCount, since both passes see rows in the
    // same order).
    private long pass2Ordinal;   // running non-null row counter during pass2 (same traversal order as pass1)
    private long pass2Row;       // running ALL-row counter during pass2 (index into nullBits, same order as pass1)
    private long rowCount;       // running ALL-row counter during pass1 (null + non-null); number of bits in nullBits; drives the SUBSAMPLE row cap
    private long selIdx;         // monotonic cursor into `selected` during pass2
    private long target;         // resolved in init() from targetArg for the current execution
    // Resolved once at construction (valueArg's type never changes across rows), used by
    // the lane readers below for the per-type value read and null-sentinel mapping.
    private final short valueTag;
    // Loop-invariant lane flag: integral value columns (INT/LONG/SHORT/BYTE) buffer the raw
    // long - exact over the full 64-bit range, where narrowing to double collapses values
    // beyond 2^53 - while floating-point columns (FLOAT/DOUBLE) buffer the double.
    // preparePass2 hands the flag to algorithm.select so comparisons run in the matching
    // domain; see SubsampleAlgorithm for the dual-lane entry layout.
    private final boolean hasIntegralValues;

    BucketSelectWindowFunction(
            Function tsArg,
            Function valueArg,
            Function targetArg,
            int targetPosition,
            long resolvedTarget,
            SubsampleAlgorithm algorithm,
            String name,
            long maxRows,
            int functionPosition
    ) {
        super(null);
        this.tsArg = tsArg;
        this.valueArg = valueArg;
        this.valueTag = ColumnType.tagOf(valueArg.getType());
        this.hasIntegralValues = valueTag == ColumnType.INT || valueTag == ColumnType.LONG
                || valueTag == ColumnType.SHORT || valueTag == ColumnType.BYTE;
        this.targetArg = targetArg;
        this.targetPosition = targetPosition;
        // For a constant target, already range-validated at newInstance (compile time); for a
        // bind-variable target this is an unused placeholder, overwritten every execution in
        // init() below.
        this.target = resolvedTarget;
        this.algorithm = algorithm;
        this.functionPosition = functionPosition;
        this.maxRows = maxRows;
        this.name = name;
    }

    // Shared by M4FunctionFactory/MinMaxFunctionFactory/LttbFunctionFactory's newInstance(0).
    // Preserves the SUBSAMPLE target-point contract: coerce an UNDEFINED bind variable to LONG,
    // reject anything not convertible to LONG or with a non-integer tag, and validate a CONSTANT
    // target at compile time. A bind-variable target is range-validated per execution in init();
    // see there. Returns the resolved constant value, or 0 (unused placeholder) for a bind variable.
    static long coerceAndValidateConstantTarget(Function targetArg, int targetPosition, SqlExecutionContext sqlExecutionContext) throws SqlException {
        AbstractWindowFunctionFactory.coerceRuntimeConstantType(targetArg, ColumnType.LONG, sqlExecutionContext, "target point count must be an integer", targetPosition);
        final short targetTypeTag = ColumnType.tagOf(targetArg.getType());
        if (targetTypeTag != ColumnType.INT && targetTypeTag != ColumnType.LONG
                && targetTypeTag != ColumnType.SHORT && targetTypeTag != ColumnType.BYTE) {
            throw SqlException.$(targetPosition, "integer expected for target point count");
        }
        if (!targetArg.isConstant()) {
            return 0;
        }
        return AbstractWindowFunctionFactory.validateTarget(targetArg.getLong(null), targetPosition);
    }

    @Override
    public void close() {
        super.close();
        Misc.free(tsArg);
        Misc.free(valueArg);
        Misc.free(targetArg);
        selected.close();
        nullBits.close();
        freeBuffer();
    }

    @Override
    public void cursorClosed() {
        super.cursorClosed();
        tsArg.cursorClosed();
        valueArg.cursorClosed();
        targetArg.cursorClosed();
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
    public boolean isRowSelecting() {
        // Sole-window-function keep flag: after preparePass2, getSelectedRows() enumerates the
        // exact kept traversal ordinals, so the filter can be fused into the cursor.
        return true;
    }

    @Override
    public boolean pass2NeedsBaseRecord() {
        // pass2 drives entirely off pass1's cached (ts,value) buffer, `selected`, and the
        // per-row null bitset; it never reads the base Record. Lets the cached executor skip
        // the per-row random-access base re-read in its pass2 loop.
        return false;
    }

    @Override
    public int getType() {
        return ColumnType.BOOLEAN;
    }

    @Override
    public void getSelectedRows(DirectLongList dest) {
        // Map `selected` (ascending non-null BUFFER ordinals chosen by preparePass2) back to
        // ascending pass1 traversal ordinals using pass1's null bitset. The o-th non-null row
        // in pass1 traversal order corresponds to buffer ordinal o; a single forward walk
        // over the null bitset advances both cursors monotonically, so this is byte-identical to
        // the rows pass2 would have flagged keep=true.
        dest.clear();
        long selIdx = 0;
        long nonNullOrdinal = 0;
        final long selSize = selected.size();
        for (long absRow = 0; absRow < rowCount && selIdx < selSize; absRow++) {
            if (!nullFlag(absRow)) {
                if (selected.get(selIdx) == nonNullOrdinal) {
                    dest.add(absRow);
                    selIdx++;
                }
                nonNullOrdinal++;
            }
        }
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        super.init(symbolTableSource, executionContext);
        bindArgs(symbolTableSource, executionContext);
    }

    // Live-view incremental refresh skips init() from the second cycle on and calls this
    // instead; args cache cursor-scoped bindings, so rebind them every cycle. See
    // BaseWindowFunction.initPartitionBy.
    @Override
    public void initPartitionBy(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        super.initPartitionBy(symbolTableSource, executionContext);
        bindArgs(symbolTableSource, executionContext);
    }

    private void bindArgs(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        tsArg.init(symbolTableSource, executionContext);
        valueArg.init(symbolTableSource, executionContext);
        targetArg.init(symbolTableSource, executionContext);
        if (!targetArg.isConstant()) {
            // Resolve target for THIS execution: a bind-variable target is re-read (and
            // range-checked) every run, so re-binding between executions takes effect.
            target = AbstractWindowFunctionFactory.validateTarget(targetArg.getLong(null), targetPosition);
        }
        // A constant target was already resolved and range-validated at newInstance (compile
        // time); it reads the same value every execution, so there is nothing to redo here.
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
        // Compile-time half of the ascending-order contract: reject a window ORDER BY that
        // is descending as written. pass1's monotonicity guard below covers what this
        // cannot see - an ascending order key that is not the timestamp argument.
        AbstractWindowFunctionFactory.validateAscendingOrder(orderByDirection, functionPosition, name);
        this.orderBy = orderBy;
    }

    @Override
    public void pass1(Record record, long recordOffset, WindowSPI spi) {
        // The cap counts physical input rows. A NULL row never reaches the buffer, but it still
        // costs a base row id, a sort entry and a null bit in the window cursor, so it counts too,
        // exactly as every row does for uniform/cadence (which have no value column to be NULL).
        if (isSubsampleKeepFlag() && rowCount >= maxRows) {
            throw CairoException.nonCritical().position(functionPosition)
                    .put("SUBSAMPLE input exceeds maximum of ").put(maxRows).put(" rows (raise ")
                    .put(PropertyKey.CAIRO_SQL_SUBSAMPLE_MAX_ROWS.getPropertyPath()).put(')');
        }
        final long ts = tsArg.getTimestamp(record);
        if (ts == Numbers.LONG_NULL) {
            // Dropped: not appended to the buffer, not counted in `count` (the row cap above has
            // already counted it) - mirrors bufferInput().
            // Record the drop in the per-row null bitset so pass2 need not re-read the record.
            appendNullFlag(true);
            return;
        }
        final long longValue;
        final double doubleValue;
        if (hasIntegralValues) {
            longValue = readLongValue(record);
            doubleValue = 0;
            if (isIntegralNull(longValue)) {
                // Dropped: the tag-specific null sentinel must never enter selection as a
                // huge magnitude (LONG_NULL is Long.MIN_VALUE).
                appendNullFlag(true);
                return;
            }
        } else {
            longValue = 0;
            doubleValue = readDoubleValue(record);
            if (Numbers.isNull(doubleValue)) {
                // Dropped: a NULL value must never seed (or otherwise poison) a bucket's
                // min/max. QuestDB defines a NULL double as NON-FINITE, so this screens NaN
                // AND both infinities - matching Numbers.isNull/isFinite everywhere else in
                // this package, and SUBSAMPLE's documented "m4, minmax and lttb ignore rows
                // whose value is NULL". Double.isNaN would let a projected overflow (e.g.
                // p * 1e308 -> +Inf) win a bucket's max and render to the client as a null
                // data point, and leave lttb computing Inf - Inf = NaN triangle areas.
                appendNullFlag(true);
                return;
            }
        }
        // The algorithms bucket a buffer they assume is ascending (see SubsampleAlgorithm):
        // descending ORDER BY is rejected at compile time, but an ascending order key that is
        // not the timestamp argument (including expression arguments) can still deliver
        // backward steps; refuse them rather than silently mis-bucket. Equal timestamps stay
        // legal - the algorithms handle them via their single-bucket degenerate path.
        if (count > 0 && ts < lastTs) {
            throw CairoException.nonCritical().position(functionPosition)
                    .put(name).put("() requires the timestamp argument in ascending ORDER BY order");
        }
        lastTs = ts;
        if (count >= Integer.MAX_VALUE) {
            throw CairoException.nonCritical()
                    .put(name).put(" input exceeds maximum of ").put(Integer.MAX_VALUE).put(" rows");
        }
        appendNullFlag(false);
        ensureCapacity();
        final long offset = count * SubsampleAlgorithm.ENTRY_SIZE;
        // No ordinal is stored: an entry's ordinal is `count`, which is exactly the buffer
        // index the algorithms hand back in `selected`, so it is derived rather than kept.
        // See SubsampleAlgorithm for the entry layout.
        Unsafe.getUnsafe().putLong(buffer + offset, ts);
        if (hasIntegralValues) {
            Unsafe.getUnsafe().putLong(buffer + offset + 8, longValue);
        } else {
            Unsafe.getUnsafe().putDouble(buffer + offset + 8, doubleValue);
        }
        count++;
    }

    @Override
    public void pass2(Record record, long recordOffset, WindowSPI spi) {
        final boolean keep;
        // Consult pass1's cached null bitset in the same traversal order pass1 wrote it (both
        // passes visit rows in the same order), so this stays byte-identical to the old
        // isNullRow(record) path while needing no base-record re-read - see pass2NeedsBaseRecord().
        if (nullFlag(pass2Row++)) {
            // Same row this was in pass1, so this stays aligned with the bufferCount pass1
            // assigned to non-null rows.
            keep = false;
        } else {
            final long bufferPos = pass2Ordinal++;
            while (selIdx < selected.size() && selected.get(selIdx) < bufferPos) {
                selIdx++;
            }
            keep = selIdx < selected.size() && selected.get(selIdx) == bufferPos;
            if (keep) {
                selIdx++;
            }
        }
        lastKeep = keep;
        // BOOLEAN is a 1-byte chain column (see ColumnType.TYPE_SIZE[BOOLEAN]); write a byte,
        // not a long, or we'd corrupt the next column's storage.
        Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), (byte) (keep ? 1 : 0));
    }

    @Override
    public void preparePass2() {
        selIdx = 0;
        pass2Ordinal = 0;
        pass2Row = 0;
        if (count > Integer.MAX_VALUE || target > Integer.MAX_VALUE) {
            throw CairoException.nonCritical().put(name).put(" input exceeds maximum of ").put(Integer.MAX_VALUE).put(" rows");
        }
        if (count <= target) {
            // When the buffered row count already fits the target, keep every buffered row rather
            // than bucketing. Running algorithm.select here would dedup first/min/max/last and can drop
            // rows (e.g. a monotonic run collapses to just {first,last}). Null rows stay dropped
            // because they were never appended to the buffer.
            selected.clear();
            for (long i = 0; i < count; i++) {
                selected.add(i);
            }
        } else {
            algorithm.select(buffer, (int) count, (int) target, hasIntegralValues, selected, circuitBreaker);
        }
    }

    @Override
    public void reopen() {
        count = 0;
        rowCount = 0;
        pass2Ordinal = 0;
        pass2Row = 0;
        selIdx = 0;
        selected.reopen();
        selected.clear();
        nullBits.reopen();
        nullBits.clear();
    }

    @Override
    public void setMemoryTracker(@Nullable MemoryTracker tracker) {
        this.memoryTracker = tracker;
        selected.setMemoryTracker(tracker);
        nullBits.setMemoryTracker(tracker);
    }

    @Override
    public void reset() {
        super.reset();
        count = 0;
        rowCount = 0;
        pass2Ordinal = 0;
        pass2Row = 0;
        selIdx = 0;
        selected.close();
        nullBits.close();
        freeBuffer();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(name);
        // Render the constant value (byte-identical to the pre-bind-var plan); for a bind-variable
        // target `target` is not resolved until init(), so render the argument's own plan instead.
        sink.val('(').val(tsArg).val(',').val(valueArg).val(',');
        if (targetArg.isConstant()) {
            sink.val(targetArg.getLong(null));
        } else {
            sink.val(targetArg);
        }
        toPlanAdditionalArgs(sink);
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
        tsArg.toTop();
        valueArg.toTop();
        targetArg.toTop();
        count = 0;
        rowCount = 0;
        pass2Ordinal = 0;
        pass2Row = 0;
        selIdx = 0;
        selected.clear();
        nullBits.clear();
    }

    protected void toPlanAdditionalArgs(PlanSink sink) {
    }

    private void ensureCapacity() {
        if (buffer == 0) {
            bufferCapacity = INITIAL_CAPACITY;
            buffer = Unsafe.malloc(
                    bufferCapacity * SubsampleAlgorithm.ENTRY_SIZE,
                    MemoryTag.NATIVE_FUNC_RSS,
                    memoryTracker
            );
        } else if (count >= bufferCapacity) {
            final long newCapacity = bufferCapacity << 1;
            buffer = Unsafe.realloc(
                    buffer,
                    bufferCapacity * SubsampleAlgorithm.ENTRY_SIZE,
                    newCapacity * SubsampleAlgorithm.ENTRY_SIZE,
                    MemoryTag.NATIVE_FUNC_RSS,
                    memoryTracker
            );
            bufferCapacity = newCapacity;
        }
    }

    private void freeBuffer() {
        if (buffer != 0) {
            Unsafe.free(
                    buffer,
                    bufferCapacity * SubsampleAlgorithm.ENTRY_SIZE,
                    MemoryTag.NATIVE_FUNC_RSS,
                    memoryTracker
            );
            buffer = 0;
            bufferCapacity = 0;
        }
    }

    /**
     * Appends one bit for the current pass1 row to {@code nullBits} in traversal order:
     * {@code true} for a dropped (NULL ts / NULL value) row, {@code false} for a
     * buffered row. pass1 is called once per row in order, so this is O(1) amortised. pass2
     * reads the same bits back via {@link #nullFlag(long)} instead of re-deriving null-ness from
     * a random-access base re-read: a NULL timestamp or a NULL value is dropped.
     */
    private void appendNullFlag(boolean isNull) {
        final long wordIndex = rowCount >>> 6;
        // rowCount grows by 1 per call, so at most one new 64-bit word is needed, and only when
        // this row opens a fresh word (rowCount % 64 == 0).
        if (wordIndex >= nullBits.size()) {
            nullBits.add(0L);
        }
        if (isNull) {
            nullBits.set(wordIndex, nullBits.get(wordIndex) | (1L << (rowCount & 63)));
        }
        rowCount++;
    }

    /**
     * Reads the null bit for traversal ordinal {@code row} recorded during pass1.
     */
    private boolean nullFlag(long row) {
        return (nullBits.get(row >>> 6) & (1L << (row & 63))) != 0;
    }

    /**
     * Reads a floating-point value column (FLOAT/DOUBLE) as a double; NULL reads as NaN.
     * Callers screen the result with {@link Numbers#isNull(double)}, which also rejects the
     * infinities a projected expression can produce.
     */
    private double readDoubleValue(Record record) {
        if (valueTag == ColumnType.FLOAT) {
            // Float.NaN widens to Double.NaN, so no explicit mapping is needed here.
            return valueArg.getFloat(record);
        }
        return valueArg.getDouble(record);
    }

    /**
     * Reads an integral value column (INT/LONG/SHORT/BYTE) as a raw long, exact over the
     * full 64-bit range. Null sentinels pass through unchanged; callers screen them with
     * {@link #isIntegralNull(long)}.
     */
    private long readLongValue(Record record) {
        return switch (valueTag) {
            case ColumnType.INT -> valueArg.getInt(record);
            case ColumnType.LONG -> valueArg.getLong(record);
            case ColumnType.SHORT -> valueArg.getShort(record);
            default -> valueArg.getByte(record); // BYTE - the only remaining integral tag
        };
    }

    /**
     * Tag-specific null-sentinel test for the integral lane: LONG_NULL is null only for a
     * LONG column and INT_NULL only for an INT column (a LONG column holding
     * Integer.MIN_VALUE is a real value). SHORT/BYTE have no null sentinel, so no
     * SHORT/BYTE row is ever dropped.
     */
    private boolean isIntegralNull(long value) {
        return (valueTag == ColumnType.LONG && value == Numbers.LONG_NULL)
                || (valueTag == ColumnType.INT && value == Numbers.INT_NULL);
    }
}

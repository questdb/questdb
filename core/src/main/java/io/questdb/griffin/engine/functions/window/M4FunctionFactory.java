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
        // Per-absolute-row null bitset built in pass1 (1 bit/row, appended in absolute traversal
        // order). pass2 consults it instead of re-deriving isNullRow(record) from a random-access
        // base re-read; see pass2NeedsBaseRecord(). Same native-memory lifecycle as `selected`
        // (allocate on reopen, clear on toTop, close on reset/close) - a prior real native leak on
        // the lttb gap scratch is the discipline mirrored here.
        private final DirectLongList nullBits = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT);
        private final DirectLongList selected = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT);
        private final long target;
        private final Function tsArg;
        private final Function valueArg;
        private long buffer;
        private long bufferCapacity; // in entries
        private SqlExecutionCircuitBreaker circuitBreaker;
        private long count;          // running non-null row counter during pass1; becomes bufferSize.
        // Rows with a NULL ts or a null/NaN value are dropped from the buffer entirely (never
        // appended, never counted), mirroring SubsampleRecordCursorFactory.bufferInput()/
        // getValueAsDouble() - a null seeding a bucket's min/max would otherwise poison it forever
        // (NaN comparisons are always false, so minVal/maxVal, once NaN, never update again).
        private boolean lastKeep;    // last keep-flag computed in pass2; see getBool() below
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
        private long rowCount;       // running ALL-row counter during pass1 (null + non-null); number of bits in nullBits
        private long selIdx;         // monotonic cursor into `selected` during pass2
        // Resolved once at construction (valueArg's type never changes across rows), used by
        // readValue() to replicate SubsampleRecordCursorFactory.getValueAsDouble()'s per-type
        // null -> NaN mapping.
        private final short valueTag;

        BucketSelectWindowFunction(Function tsArg, Function valueArg, long target, SubsampleAlgorithm algorithm, String name) {
            super(null);
            this.tsArg = tsArg;
            this.valueArg = valueArg;
            this.valueTag = ColumnType.tagOf(valueArg.getType());
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
            nullBits.close();
            freeBuffer();
        }

        @Override
        public void cursorClosed() {
            super.cursorClosed();
            tsArg.cursorClosed();
            valueArg.cursorClosed();
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
            // exact kept absolute rows, so the filter can be fused into the cursor.
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
            // ascending ABSOLUTE base-row indices using pass1's null bitset. The o-th non-null row
            // in absolute traversal order corresponds to buffer ordinal o; a single forward walk
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
            if (ts == Numbers.LONG_NULL) {
                // Dropped: not appended to the buffer, not counted - mirrors bufferInput().
                // Record the drop in the per-row null bitset so pass2 need not re-read the record.
                appendNullFlag(true);
                return;
            }
            final double value = readValue(record);
            if (Double.isNaN(value)) {
                // Dropped: a null/NaN value must never seed (or otherwise poison) a bucket's
                // min/max - mirrors bufferInput()'s "if (Double.isNaN(value)) continue;".
                appendNullFlag(true);
                return;
            }
            appendNullFlag(false);
            ensureCapacity();
            final long offset = count * SubsampleAlgorithm.ENTRY_SIZE;
            Unsafe.getUnsafe().putLong(buffer + offset, count);
            Unsafe.getUnsafe().putLong(buffer + offset + 8, ts);
            Unsafe.getUnsafe().putDouble(buffer + offset + 16, value);
            count++;
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            final boolean keep;
            // Consult pass1's cached null bitset in the same absolute traversal order pass1 wrote it
            // (both passes visit rows in the same order), so this stays byte-identical to the old
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
            if (count <= target) {
                // Mirror SubsampleRecordCursorFactory.bufferAndSelect's
                // `bufferSize <= targetPoints -> selectAll()` short-circuit: when the buffered
                // (non-null) row count already fits the target, keep every buffered row rather than
                // bucketing. Running algorithm.select here would dedup first/min/max/last and can drop
                // rows (e.g. a monotonic run collapses to just {first,last}), diverging from the old
                // SUBSAMPLE cursor which returns ALL rows in this case. Null rows stay dropped - they
                // were never appended to the buffer, so keeping all buffered rows keeps only non-nulls,
                // exactly as selectAll() over bufferInput()'s null-filtered buffer does.
                selected.clear();
                for (long i = 0; i < count; i++) {
                    selected.add(i);
                }
            } else {
                algorithm.select(buffer, (int) count, (int) target, selected, circuitBreaker);
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
            rowCount = 0;
            pass2Ordinal = 0;
            pass2Row = 0;
            selIdx = 0;
            selected.clear();
            nullBits.clear();
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

        /**
         * Appends one bit for the current pass1 row to {@code nullBits} in absolute traversal
         * order: {@code true} for a dropped (NULL ts / null-or-NaN value) row, {@code false} for a
         * buffered row. pass1 is called once per row in order, so this is O(1) amortised. pass2
         * reads the same bits back via {@link #nullFlag(long)} instead of re-deriving null-ness from
         * a random-access base re-read (mirrors the old {@code isNullRow(record)}: a NULL timestamp,
         * or a null/NaN value, per SubsampleRecordCursorFactory.bufferInput()).
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
         * Reads the null bit for absolute row {@code row} recorded during pass1.
         */
        private boolean nullFlag(long row) {
            return (nullBits.get(row >>> 6) & (1L << (row & 63))) != 0;
        }

        /**
         * Reads the value column as a double, mapping each type's NULL sentinel to NaN - mirrors
         * SubsampleRecordCursorFactory.getValueAsDouble() (SHORT/BYTE have no null sentinel).
         */
        private double readValue(Record record) {
            switch (valueTag) {
                case ColumnType.DOUBLE:
                    return valueArg.getDouble(record);
                case ColumnType.FLOAT:
                    // Float.NaN widens to Double.NaN, so no explicit mapping is needed here.
                    return valueArg.getFloat(record);
                case ColumnType.INT: {
                    final int v = valueArg.getInt(record);
                    return v != Numbers.INT_NULL ? v : Double.NaN;
                }
                case ColumnType.LONG: {
                    final long v = valueArg.getLong(record);
                    return v != Numbers.LONG_NULL ? v : Double.NaN;
                }
                case ColumnType.SHORT:
                    return valueArg.getShort(record);
                case ColumnType.BYTE:
                    return valueArg.getByte(record);
                default:
                    return valueArg.getDouble(record);
            }
        }
    }
}

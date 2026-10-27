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

package io.questdb.griffin.engine.window;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.RecordChain;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.ColumnwiseBooleanOp;
import io.questdb.griffin.engine.functions.ColumnwiseComparison;
import io.questdb.griffin.engine.functions.ColumnwiseFunction;
import io.questdb.griffin.engine.functions.FloatFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
import io.questdb.griffin.engine.functions.columns.DoubleColumn;
import io.questdb.griffin.engine.functions.columns.FloatColumn;
import io.questdb.griffin.engine.functions.columns.IntColumn;
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.functions.columns.SymbolColumn;
import io.questdb.griffin.engine.functions.columns.TimestampColumn;
import io.questdb.griffin.engine.functions.conditional.CaseBranches;
import io.questdb.griffin.engine.functions.conditional.CaseFunction;
import io.questdb.griffin.engine.functions.groupby.CountLongConstGroupByFunction;
import io.questdb.griffin.engine.functions.memoization.MemoizerFunction;
import io.questdb.griffin.engine.functions.window.BaseWindowFunction;
import io.questdb.griffin.engine.functions.window.LagDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.window.LagLongFunctionFactory;
import io.questdb.griffin.engine.functions.window.SumDoubleWindowFunctionFactory;
import io.questdb.griffin.engine.table.KeyMajorPageFrameRecordCursor;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * Computes the row-by-row tasks of a parallel window column-wise, one batch of rows of a key
 * run at a time, for the chains whose every step it can evaluate exactly: {@code lag} over a
 * column, then projections of column references, constants, {@code +} and {@code -}, and filters
 * that compare such values (e.g.
 * {@code seq < lag(seq) OVER (PARTITION BY sym ORDER BY time)}, or {@code seq - lag(seq) < 0}), and
 * the stand-ins of folded or replayed functions ({@link AsyncWindowFoldEcho}), which output their argument
 * for the query's thread to compute the function over (e.g. a running sum of one symbol's sizes).
 * <p>
 * The row path ({@code AsyncWindowAtom.Slot.compute}) moves a record to each row and calls the
 * window functions, every step and the record sink through their virtual getters, which on a
 * server that runs many queries are megamorphic call sites. This kernel instead loads each column
 * the chain reads for a batch of rows into a typed buffer, in one tight loop per column, and
 * evaluates each operation over the whole batch, then writes only the rows every filter keeps.
 * <p>
 * <b>Exactness.</b> It evaluates only functions whose arithmetic it reproduces bit for bit:
 * <ul>
 *     <li>a column is read as {@link PageFrameMemoryRecord} reads it (a frame without the
 *     column's data reads NULL), and converted to the type another function reads it as with the
 *     standard base class's getter ({@code IntFunction.getLong()} and so on); a frame whose
 *     columns are read through a type conversion makes the task take the row path;</li>
 *     <li>{@code lag(x, k)} without a default and without IGNORE NULLS, of
 *     {@link LagLongFunctionFactory.LagFunction} or {@link LagDoubleFunctionFactory.LagFunction}
 *     exactly: NULL for the first k rows since the function last started afresh, then the value k
 *     rows back, as the function's ring holds it;</li>
 *     <li>{@code +} and {@code -} through {@link ColumnwiseFunction}, comparisons through
 *     {@link ColumnwiseComparison}, each only where the class that declares the claim declares
 *     the getter too;</li>
 *     <li>constants are read once, through their own getter.</li>
 * </ul>
 * The window functions start afresh where the row path starts them: at the task's first row, and
 * at each key the task's rows start when the slot resets its functions at key starts. Anything it
 * cannot evaluate keeps the row path, decided once per plan, at its first execution, when every
 * step is known.
 * <p>
 * One instance serves one slot and is used by the thread that holds the slot only.
 */
public final class AsyncWindowRowKernel {
    /**
     * What {@link #compute} returns when a frame makes the task take the row path.
     */
    public static final long UNSUPPORTED = -2;
    private static final int BATCH_ROWS = 256;
    // batches between two checks of the circuit breaker and of the round's cancellation
    private static final int CHECK_BATCHES = 16;
    private static final int K_ADD = 5;
    private static final int K_BOOL_EQ = 10;
    private static final int K_CASE = 12;
    private static final int K_COLUMN = 0;
    private static final int K_CONST = 2;
    private static final int K_CONVERT = 3;
    private static final int K_EQ = 8;
    private static final int K_IS_NULL = 9;
    private static final int K_KEY = 1;
    private static final int K_LAG = 4;
    private static final int K_LT = 7;
    private static final int K_OR = 11;
    // a window step's running sum, see SumDoubleWindowFunctionFactory.SumOverUnboundedRowsFrameFunction
    private static final int K_RUN_SUM = 13;
    private static final int K_SUB = 6;
    // the most rows back a lag may read; a larger offset keeps the row path
    private static final int MAX_LAG_OFFSET = 64;
    // value representations of a node's buffer
    private static final int V_BOOL = 4;
    private static final int V_DOUBLE = 2;
    private static final int V_FLOAT = 3;
    private static final int V_INT = 0;
    private static final int V_LONG = 1;
    private final long[] batchRows = new long[BATCH_ROWS];
    // the columns the batch loads, by node
    private final ObjList<Node> columns = new ObjList<>();
    private final int keyColumnIndex;
    private final ObjList<Node> lags = new ObjList<>();
    // the record a last GROUP BY step reads, over the chain's columns before it, or null
    private GroupInputRecord groupInput;
    private final ObjList<Node> runSums = new ObjList<>();
    // every node, children before their parents
    private final ObjList<Node> nodes = new ObjList<>();
    private final long[] outputOffsets;
    // the chain's columns, by output column
    private final Node[] outputs;
    private final ObjList<Node> predicates = new ObjList<>();
    private final int[] selected = new int[BATCH_ROWS];
    // rows emit() appended since computeSlice() began
    private long sliceRowCount;
    // tasks computed since the last resetCounts(), for tests
    private long taskCount;

    private AsyncWindowRowKernel(int keyColumnIndex, int outputCount) {
        this.keyColumnIndex = keyColumnIndex;
        this.outputs = new Node[outputCount];
        this.outputOffsets = new long[outputCount];
    }

    /**
     * Compiles the kernel of a slot, or returns null when a step of its chain is beyond it.
     *
     * @param functions      the window's functions, one per output column of the window
     * @param mapStateCount  the window Map groups over the functions; the kernel needs none
     * @param stages         the steps after the window
     * @param outputTypes    the types of the columns the chain holds, the last step's output
     * @param crossIndex     the scan's columns the functions read as theirs, or null
     * @param keyColumnIndex the scan's key column, which holds one value per key run, or -1
     */
    public static @Nullable AsyncWindowRowKernel compile(
            ObjList<Function> functions,
            int mapStateCount,
            ObjList<AsyncWindowStage> stages,
            IntList outputTypes,
            @Nullable IntList crossIndex,
            int keyColumnIndex
    ) {
        if (mapStateCount > 0) {
            return null;
        }
        final AsyncWindowGroupByStage groupStage = stages.size() > 0 && stages.getLast() instanceof AsyncWindowGroupByStage g ? g : null;
        final AsyncWindowRowKernel kernel = new AsyncWindowRowKernel(keyColumnIndex, groupStage == null ? outputTypes.size() : 0);
        final Compiler compiler = new Compiler(kernel, crossIndex);
        // the window's own record, then each step's
        Resolver resolver = new WindowResolver(compiler, functions);
        // the functions of the last record's columns
        ObjList<Function> recordFunctions = functions;
        for (int i = 0, n = stages.size(); i < n; i++) {
            final AsyncWindowStage stage = stages.getQuick(i);
            switch (stage.getKind()) {
                case AsyncWindowStage.KIND_VIRTUAL -> {
                    resolver = new ProjectionResolver(compiler, stage.getFunctions(), stage.getReservedSlots(), resolver);
                    recordFunctions = stage.getFunctions();
                }
                case AsyncWindowStage.KIND_WINDOW -> {
                    // a window step after a filter sees only the rows it keeps; the kernel computes
                    // every row
                    if (stage.getMapStatesCount() > 0 || kernel.predicates.size() > 0) {
                        return null;
                    }
                    resolver = new WindowStageResolver(compiler, stage.getFunctions(), resolver, i);
                    recordFunctions = stage.getFunctions();
                }
                case AsyncWindowStage.KIND_FILTER -> {
                    final Node predicate = compiler.compileBool(stage.getFunctions().getQuick(0), resolver);
                    if (predicate == null) {
                        return null;
                    }
                    kernel.predicates.add(predicate);
                }
                case AsyncWindowStage.KIND_GROUP_BY -> {
                    // the last step: its input record's columns, which it reads with their own types'
                    // getters only
                    if (stage != groupStage || !kernel.compileGroupInput(recordFunctions, resolver, groupStage)) {
                        return null;
                    }
                    kernel.allocate();
                    return kernel;
                }
                default -> {
                    return null;
                }
            }
        }
        for (int c = 0, n = outputTypes.size(); c < n; c++) {
            final int rep;
            switch (ColumnType.tagOf(outputTypes.getQuick(c))) {
                case ColumnType.INT, ColumnType.SYMBOL -> rep = V_INT;
                case ColumnType.LONG, ColumnType.TIMESTAMP -> rep = V_LONG;
                case ColumnType.DOUBLE -> rep = V_DOUBLE;
                case ColumnType.FLOAT -> rep = V_FLOAT;
                default -> {
                    return null;
                }
            }
            final Node node = resolver.resolve(c, rep);
            if (node == null) {
                return null;
            }
            kernel.outputs[c] = node;
        }
        kernel.allocate();
        return kernel;
    }

    /**
     * Computes a task's rows as {@code AsyncWindowAtom.Slot.compute} does, see the class
     * documentation, and appends the rows from {@code emitFrom} on that every filter keeps to the
     * chain. Returns the offset of the last record appended, -1 when none was, or
     * {@link #UNSUPPORTED} when a frame's columns are read through a type conversion: the chain
     * is then empty again, and the task is the row path's.
     */
    public long compute(
            DirectLongList rows,
            LongList keyStarts,
            long emitFrom,
            boolean resetAtKeyStarts,
            RecordChain chain,
            PageFrameMemoryPool pool,
            PageFrameMemoryRecord record,
            SqlExecutionCircuitBreaker circuitBreaker,
            UnorderedPageFrameSequence<?> sequence
    ) {
        final long rowCount = rows.size();
        chain.rewind(rowCount - emitFrom);
        if (rowCount == 0) {
            taskCount++;
            return -1;
        }
        final long stride = chain.getFixedRecordStride();
        for (int c = 0, n = outputOffsets.length; c < n; c++) {
            outputOffsets[c] = chain.getOffsetOfColumn(0, c);
        }
        final long[] batch = batchRows;
        final int keyStartCount = keyStarts.size();
        // the task's first row starts the functions afresh, whatever key it continues
        startLags();
        int keyStartIndex = 0;
        long prevOffset = -1;
        int frameIndex = -1;
        int key = 0;
        boolean keyPending = true;
        int batches = 0;
        long lo = 0;
        while (lo < rowCount) {
            // the run of the key the rows from lo belong to ends at the next key start
            while (keyStartIndex < keyStartCount && keyStarts.getQuick(keyStartIndex) <= lo) {
                if (keyStarts.getQuick(keyStartIndex) == lo) {
                    // a key starts here: read its value off its first row, and start the functions
                    // afresh when the slot does
                    keyPending = true;
                    if (resetAtKeyStarts && lo > 0) {
                        startLags();
                    }
                }
                keyStartIndex++;
            }
            final long runHi = keyStartIndex < keyStartCount ? Math.min(rowCount, keyStarts.getQuick(keyStartIndex)) : rowCount;
            if (++batches == CHECK_BATCHES) {
                batches = 0;
                circuitBreaker.statefulThrowExceptionIfTripped();
                if (!sequence.isActive()) {
                    // the round was cancelled: its output will never be read
                    return prevOffset;
                }
            }
            final int batchFrameIndex = KeyMajorPageFrameRecordCursor.toFrameIndex(rows.get(lo));
            if (batchFrameIndex != frameIndex) {
                frameIndex = batchFrameIndex;
                final PageFrameMemory frameMemory = pool.navigateTo(frameIndex);
                if (frameMemory.hasColumnTypeCasts()) {
                    chain.rewind(0);
                    return UNSUPPORTED;
                }
                record.init(frameMemory);
                for (int i = 0, n = columns.size(); i < n; i++) {
                    final Node column = columns.getQuick(i);
                    column.address = frameMemory.getPageAddress(column.column);
                }
            }
            // the rows of one frame and one key run, at most a batch of them
            final long hi = Math.min(runHi, lo + BATCH_ROWS);
            int n = 0;
            for (long i = lo; i < hi; i++) {
                final long rowId = rows.get(i);
                if (KeyMajorPageFrameRecordCursor.toFrameIndex(rowId) != frameIndex) {
                    break;
                }
                batch[n++] = KeyMajorPageFrameRecordCursor.toFrameRowIndex(rowId);
            }
            if (keyPending) {
                keyPending = false;
                if (keyColumnIndex > -1) {
                    // every row of the run has the key the walk collected it for
                    record.setRowIndex(batch[0]);
                    key = record.getInt(keyColumnIndex);
                }
            }
            evaluate(batch, n, key);
            // the rows from emitFrom on that every filter keeps
            prevOffset = emit((int) Math.min(n, Math.max(0, emitFrom - lo)), n, chain, stride, prevOffset);
            lo += n;
        }
        taskCount++;
        return prevOffset;
    }

    /**
     * Computes a slice's rows as {@code AsyncWindowAtom.Slot.computeCompiledSlice} does, over the rows
     * the compiled prefilter passed, frame rows {@code rowLo + sliceRows[i]} for i below
     * {@code count}, from the functions' clean state; appends the rows every filter keeps to the
     * chain after {@code prevOffset}. Returns the offset of the last record appended,
     * {@code prevOffset} when none was ({@link #getSliceRowCount()} tells how many), or
     * {@link #UNSUPPORTED} when the frame's columns are read through a type conversion. Only for a
     * kernel without a key column, see {@link #isSliceCapable()}.
     */
    public long computeSlice(
            PageFrameMemory frameMemory,
            long rowLo,
            DirectLongList sliceRows,
            long count,
            RecordChain chain,
            long prevOffset,
            SqlExecutionCircuitBreaker circuitBreaker,
            UnorderedPageFrameSequence<?> sequence
    ) {
        assert isSliceCapable();
        sliceRowCount = 0;
        if (frameMemory.hasColumnTypeCasts()) {
            return UNSUPPORTED;
        }
        final long stride = chain.getFixedRecordStride();
        for (int c = 0, n = outputOffsets.length; c < n; c++) {
            outputOffsets[c] = chain.getOffsetOfColumn(0, c);
        }
        for (int i = 0, n = columns.size(); i < n; i++) {
            final Node column = columns.getQuick(i);
            column.address = frameMemory.getPageAddress(column.column);
        }
        startLags();
        final long[] batch = batchRows;
        final long rowsAddr = sliceRows.getAddress();
        int batches = 0;
        for (long lo = 0; lo < count; ) {
            if (++batches == CHECK_BATCHES) {
                batches = 0;
                circuitBreaker.statefulThrowExceptionIfTripped();
                if (!sequence.isActive()) {
                    // the round was cancelled: its output will never be read
                    return prevOffset;
                }
            }
            final int n = (int) Math.min(BATCH_ROWS, count - lo);
            for (int j = 0; j < n; j++) {
                batch[j] = rowLo + Unsafe.getLong(rowsAddr + ((lo + j) << 3));
            }
            evaluate(batch, n, 0);
            prevOffset = emit(0, n, chain, stride, prevOffset);
            lo += n;
        }
        taskCount++;
        return prevOffset;
    }

    /**
     * Rows a {@link #computeSlice} appended.
     */
    public long getSliceRowCount() {
        return sliceRowCount;
    }

    /**
     * Whether {@link #computeSlice} may compute a slice: the chain reads no key column.
     */
    public boolean isSliceCapable() {
        return keyColumnIndex < 0;
    }

    // Appends the batch's rows from emitLo to n that every filter keeps to the chain.
    private long emit(int emitLo, int n, RecordChain chain, long stride, long prevOffset) {
        int m = 0;
        final ObjList<Node> predicates = this.predicates;
        final int predicateCount = predicates.size();
        for (int j = emitLo; j < n; j++) {
            boolean keep = true;
            for (int p = 0; p < predicateCount; p++) {
                if (!predicates.getQuick(p).bb[j]) {
                    keep = false;
                    break;
                }
            }
            if (keep) {
                selected[m++] = j;
            }
        }
        if (m > 0) {
            final long first = chain.appendFixedRecords(prevOffset, m);
            prevOffset = first + (m - 1) * stride;
            long address = chain.addressOf(first);
            for (int s = 0; s < m; s++) {
                writeRow(address, selected[s]);
                address += stride;
            }
            sliceRowCount += m;
        }
        return prevOffset;
    }

    /**
     * Tasks this kernel computed since the slot opened its cursor.
     */
    public long getTaskCount() {
        return taskCount;
    }

    // a task computed with the kernel, see getTaskCount()
    void countTask() {
        taskCount++;
    }

    void resetCounts() {
        taskCount = 0;
    }

    /**
     * The scan's key column, which holds one value per key run, or -1.
     */
    public int getKeyColumnIndex() {
        return keyColumnIndex;
    }

    /**
     * Whether the chain ends in a GROUP BY step, which the slot computes row by row over
     * {@link #getGroupInput()}: the kernel computes the steps before it, a batch at a time, see
     * {@link #evaluateGroupBatch}.
     */
    public boolean hasGroupInput() {
        return groupInput != null;
    }

    /**
     * The record the GROUP BY step reads in place of the step before it: the kernel's values of that
     * step's columns at the row {@link GroupInputRecord#setRow} sets.
     */
    public GroupInputRecord getGroupInput() {
        return groupInput;
    }

    /**
     * Computes the steps before the GROUP BY for the batch's frame rows, of the frame
     * {@link #ofGroupFrame} set and one key run, after the rows before them in the task. Returns
     * whether every filter keeps the batch's row j, see {@link #isKept}.
     */
    public void evaluateGroupBatch(long[] batch, int n, int key) {
        evaluate(batch, n, key);
    }

    /**
     * Whether every filter step keeps the last batch's row j.
     */
    public boolean isKept(int j) {
        final ObjList<Node> predicates = this.predicates;
        for (int p = 0, k = predicates.size(); p < k; p++) {
            if (!predicates.getQuick(p).bb[j]) {
                return false;
            }
        }
        return true;
    }

    /**
     * The batches from now on are of this frame, a native one.
     */
    public void ofGroupFrame(PageFrameMemory frameMemory) {
        for (int i = 0, n = columns.size(); i < n; i++) {
            final Node column = columns.getQuick(i);
            column.address = frameMemory.getPageAddress(column.column);
        }
    }

    /**
     * The window functions and every window step start afresh: the task's first row, a key's.
     */
    public void startFunctions() {
        startLags();
    }

    /**
     * The window steps from the given one on start afresh, as the slot's toTop() of them.
     */
    public void startStages(int fromStage) {
        startRunSums(fromStage);
    }

    // The record a last GROUP BY step reads: the columns of the record before it, by its functions,
    // each an INT, SYMBOL, LONG, TIMESTAMP, FLOAT or DOUBLE that every column reference of the step's
    // aggregates reads with its own type's getter, as the step's keys and its head capture do.
    private boolean compileGroupInput(ObjList<Function> recordFunctions, Resolver resolver, AsyncWindowGroupByStage stage) {
        final int n = recordFunctions.size();
        final Node[] nodes = new Node[n];
        final int[] types = new int[n];
        for (int c = 0; c < n; c++) {
            final int type = ColumnType.tagOf(recordFunctions.getQuick(c).getType());
            final int rep = switch (type) {
                // a SYMBOL as its key, which the step's keys and head capture read with getInt()
                case ColumnType.INT, ColumnType.SYMBOL -> V_INT;
                case ColumnType.LONG, ColumnType.TIMESTAMP -> V_LONG;
                case ColumnType.FLOAT -> V_FLOAT;
                case ColumnType.DOUBLE -> V_DOUBLE;
                default -> -1;
            };
            if (rep < 0) {
                return false;
            }
            nodes[c] = resolver.resolve(c, rep);
            if (nodes[c] == null) {
                return false;
            }
            types[c] = type;
        }
        final ObjList<GroupByFunction> functions = stage.getGroupByFunctions();
        for (int i = 0, k = functions.size(); i < k; i++) {
            if (!readsOwnTypes(functions.getQuick(i), types)) {
                return false;
            }
        }
        groupInput = new GroupInputRecord(nodes);
        return true;
    }

    // Whether every column reference of the function reads its column with its own type's getter,
    // through a plain column function, and nothing else of the function reads the record.
    private static boolean readsOwnTypes(Function function, int[] types) {
        if (function instanceof ColumnFunction cf) {
            final int column = cf.getColumnIndex();
            if (column < 0 || column >= types.length) {
                return false;
            }
            final Class<?> expected = switch (types[column]) {
                case ColumnType.INT -> IntColumn.class;
                case ColumnType.LONG -> LongColumn.class;
                case ColumnType.TIMESTAMP -> TimestampColumn.class;
                case ColumnType.FLOAT -> FloatColumn.class;
                case ColumnType.DOUBLE -> DoubleColumn.class;
                default -> null;
            };
            return function.getClass() == expected;
        }
        if (function.isConstant() || function.getClass() == CountLongConstGroupByFunction.class) {
            return true;
        }
        if (function instanceof UnaryFunction f) {
            return readsOwnTypes(f.getArg(), types);
        }
        if (function instanceof BinaryFunction f) {
            return readsOwnTypes(f.getLeft(), types) && readsOwnTypes(f.getRight(), types);
        }
        if (function instanceof TernaryFunction f) {
            return readsOwnTypes(f.getLeft(), types) && readsOwnTypes(f.getCenter(), types) && readsOwnTypes(f.getRight(), types);
        }
        if (function instanceof MultiArgFunction f) {
            final ObjList<Function> args = f.args();
            for (int i = 0, n = args.size(); i < n; i++) {
                if (!readsOwnTypes(args.getQuick(i), types)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    private static double convertToDouble(int fromRep, Node a, int i) {
        return switch (fromRep) {
            // IntFunction.getDouble()
            case V_INT -> Numbers.intToDouble(a.ib[i]);
            // LongFunction.getDouble()
            case V_LONG -> a.lb[i] != Numbers.LONG_NULL ? a.lb[i] : Double.NaN;
            // FloatFunction.getDouble()
            case V_FLOAT -> a.fb[i];
            default -> a.db[i];
        };
    }

    private void allocate() {
        for (int i = 0, n = nodes.size(); i < n; i++) {
            final Node node = nodes.getQuick(i);
            switch (node.rep) {
                case V_INT -> node.ib = new int[BATCH_ROWS];
                case V_LONG -> node.lb = new long[BATCH_ROWS];
                case V_DOUBLE -> node.db = new double[BATCH_ROWS];
                case V_FLOAT -> node.fb = new float[BATCH_ROWS];
                default -> node.bb = new boolean[BATCH_ROWS];
            }
            if (node.kind == K_LAG) {
                if (node.rep == V_LONG) {
                    node.longRing = new long[node.lagOffset];
                } else {
                    node.doubleRing = new double[node.lagOffset];
                }
            }
        }
    }

    // Every node over the batch's rows, children first.
    private void evaluate(long[] batch, int n, int key) {
        final ObjList<Node> nodes = this.nodes;
        for (int k = 0, nodeCount = nodes.size(); k < nodeCount; k++) {
            final Node node = nodes.getQuick(k);
            switch (node.kind) {
                case K_COLUMN -> loadColumn(node, batch, n);
                case K_KEY -> Arrays.fill(node.ib, 0, n, key);
                case K_CONST -> fillConstant(node, n);
                case K_CONVERT -> convert(node, n);
                case K_LAG -> lag(node, n);
                case K_ADD, K_SUB -> arithmetic(node, n);
                case K_LT, K_EQ -> compare(node, n);
                case K_IS_NULL -> isNull(node, n);
                case K_BOOL_EQ -> {
                    // EqBooleanFunctionFactory.Func
                    final boolean[] x = node.a.bb, y = node.b.bb, out = node.bb;
                    final boolean negated = node.negated;
                    for (int i = 0; i < n; i++) {
                        out[i] = negated != (x[i] == y[i]);
                    }
                }
                case K_OR -> {
                    // OrFunctionFactory: both sides are pure, so evaluating the right one always is the same
                    final boolean[] x = node.a.bb, y = node.b.bb, out = node.bb;
                    for (int i = 0; i < n; i++) {
                        out[i] = x[i] || y[i];
                    }
                }
                case K_CASE -> caseOf(node, n);
                case K_RUN_SUM -> runningSum(node, n);
                default -> throw new AssertionError("unknown node kind " + node.kind);
            }
        }
    }

    private static void arithmetic(Node node, int n) {
        final Node a = node.a;
        final Node b = node.b;
        final boolean add = node.kind == K_ADD;
        switch (node.rep) {
            case V_INT -> {
                // AddIntFunctionFactory, SubIntFunctionFactory: NULL on either side, else wraps
                final int[] x = a.ib, y = b.ib, out = node.ib;
                for (int i = 0; i < n; i++) {
                    final int l = x[i], r = y[i];
                    out[i] = l == Numbers.INT_NULL || r == Numbers.INT_NULL ? Numbers.INT_NULL : (add ? l + r : l - r);
                }
            }
            case V_LONG -> {
                // AddLongFunctionFactory, SubLongFunctionFactory
                final long[] x = a.lb, y = b.lb, out = node.lb;
                for (int i = 0; i < n; i++) {
                    final long l = x[i], r = y[i];
                    out[i] = l == Numbers.LONG_NULL || r == Numbers.LONG_NULL ? Numbers.LONG_NULL : (add ? l + r : l - r);
                }
            }
            default -> {
                // AddDoubleFunctionFactory, SubDoubleFunctionFactory: IEEE
                final double[] x = a.db, y = b.db, out = node.db;
                for (int i = 0; i < n; i++) {
                    out[i] = add ? x[i] + y[i] : x[i] - y[i];
                }
            }
        }
    }

    // A searched CASE's value: the first condition that holds picks its THEN, else the ELSE, as
    // CaseFunctionFactory's picker does; each value read with the CASE's own type.
    private static void caseOf(Node node, int n) {
        final Node[] conds = node.conds;
        final Node[] vals = node.vals;
        final Node otherwise = node.otherwise;
        final int k = conds.length;
        for (int i = 0; i < n; i++) {
            Node picked = otherwise;
            for (int c = 0; c < k; c++) {
                if (conds[c].bb[i]) {
                    picked = vals[c];
                    break;
                }
            }
            switch (node.rep) {
                case V_INT -> node.ib[i] = picked.ib[i];
                case V_LONG -> node.lb[i] = picked.lb[i];
                case V_FLOAT -> node.fb[i] = picked.fb[i];
                default -> node.db[i] = picked.db[i];
            }
        }
    }

    // EqDoubleFunctionFactory's IS NULL functions: Numbers.isNull() of a FLOAT or DOUBLE, the
    // sentinel of an INT or LONG
    private static void isNull(Node node, int n) {
        final boolean negated = node.negated;
        final boolean[] out = node.bb;
        final Node a = node.a;
        switch (a.rep) {
            case V_FLOAT -> {
                final float[] x = a.fb;
                for (int i = 0; i < n; i++) {
                    out[i] = negated != Numbers.isNull(x[i]);
                }
            }
            case V_DOUBLE -> {
                final double[] x = a.db;
                for (int i = 0; i < n; i++) {
                    out[i] = negated != Numbers.isNull(x[i]);
                }
            }
            case V_INT -> {
                final int[] x = a.ib;
                for (int i = 0; i < n; i++) {
                    out[i] = negated != (x[i] == Numbers.INT_NULL);
                }
            }
            default -> {
                final long[] x = a.lb;
                for (int i = 0; i < n; i++) {
                    out[i] = negated != (x[i] == Numbers.LONG_NULL);
                }
            }
        }
    }

    // SumOverUnboundedRowsFrameFunction.computeNext() over the batch: finite values add up, NULL
    // until the first
    private static void runningSum(Node node, int n) {
        final double[] x = node.a.db, out = node.db;
        double sum = node.runSum;
        long count = node.runCount;
        for (int i = 0; i < n; i++) {
            final double d = x[i];
            if (Numbers.isFinite(d)) {
                sum += d;
                count++;
            }
            out[i] = count != 0 ? sum : Double.NaN;
        }
        node.runSum = sum;
        node.runCount = count;
    }

    private static void compare(Node node, int n) {
        final boolean negated = node.negated;
        final boolean[] out = node.bb;
        final Node a = node.a;
        final Node b = node.b;
        switch (node.operandRep) {
            case V_INT -> {
                final int[] x = a.ib, y = b.ib;
                if (node.kind == K_LT) {
                    for (int i = 0; i < n; i++) {
                        out[i] = Numbers.lessThan(x[i], y[i], negated);
                    }
                } else {
                    for (int i = 0; i < n; i++) {
                        out[i] = negated != (x[i] == y[i]);
                    }
                }
            }
            case V_LONG -> {
                final long[] x = a.lb, y = b.lb;
                if (node.kind == K_LT) {
                    for (int i = 0; i < n; i++) {
                        out[i] = Numbers.lessThan(x[i], y[i], negated);
                    }
                } else {
                    for (int i = 0; i < n; i++) {
                        out[i] = negated != (x[i] == y[i]);
                    }
                }
            }
            default -> {
                final double[] x = a.db, y = b.db;
                if (node.kind == K_LT) {
                    for (int i = 0; i < n; i++) {
                        final double l = x[i], r = y[i];
                        final boolean eq = Numbers.equals(l, r);
                        out[i] = negated ? (eq || l > r) : (!eq && l < r);
                    }
                } else {
                    for (int i = 0; i < n; i++) {
                        out[i] = negated != Numbers.equals(x[i], y[i]);
                    }
                }
            }
        }
    }

    private static void convert(Node node, int n) {
        final Node a = node.a;
        switch (node.rep) {
            case V_LONG -> {
                // IntFunction.getLong()
                final int[] x = a.ib;
                final long[] out = node.lb;
                for (int i = 0; i < n; i++) {
                    out[i] = Numbers.intToLong(x[i]);
                }
            }
            case V_DOUBLE -> {
                final double[] out = node.db;
                for (int i = 0; i < n; i++) {
                    out[i] = convertToDouble(a.rep, a, i);
                }
            }
            default -> throw new AssertionError("unsupported conversion to " + node.rep);
        }
    }

    private static void fillConstant(Node node, int n) {
        switch (node.rep) {
            case V_INT -> Arrays.fill(node.ib, 0, n, (int) node.longConstant);
            case V_LONG -> Arrays.fill(node.lb, 0, n, node.longConstant);
            case V_FLOAT -> Arrays.fill(node.fb, 0, n, (float) node.doubleConstant);
            default -> Arrays.fill(node.db, 0, n, node.doubleConstant);
        }
    }

    // The ring of LagLongFunctionFactory.LagFunction.computeNext0(), and its count, over the
    // batch: the value k rows back, NULL for the first k rows since the function started afresh.
    private static void lag(Node node, int n) {
        final int k = node.lagOffset;
        int pos = node.lagPosition;
        long count = node.lagCount;
        if (node.rep == V_LONG) {
            final long[] src = node.a.lb, out = node.lb, ring = node.longRing;
            for (int i = 0; i < n; i++) {
                out[i] = count < k ? Numbers.LONG_NULL : ring[pos];
                ring[pos] = src[i];
                if (++pos == k) {
                    pos = 0;
                }
                count++;
            }
        } else {
            final double[] src = node.a.db, out = node.db, ring = node.doubleRing;
            for (int i = 0; i < n; i++) {
                out[i] = count < k ? Double.NaN : ring[pos];
                ring[pos] = src[i];
                if (++pos == k) {
                    pos = 0;
                }
                count++;
            }
        }
        node.lagPosition = pos;
        node.lagCount = count;
    }

    // A column of the scan, as PageFrameMemoryRecord reads it: NULL where the frame has no data.
    private static void loadColumn(Node node, long[] batch, int n) {
        final long address = node.address;
        switch (node.rep) {
            case V_INT -> {
                final int[] out = node.ib;
                if (address == 0) {
                    Arrays.fill(out, 0, n, Numbers.INT_NULL);
                } else {
                    for (int i = 0; i < n; i++) {
                        out[i] = Unsafe.getInt(address + (batch[i] << 2));
                    }
                }
            }
            case V_LONG -> {
                final long[] out = node.lb;
                if (address == 0) {
                    Arrays.fill(out, 0, n, Numbers.LONG_NULL);
                } else {
                    for (int i = 0; i < n; i++) {
                        out[i] = Unsafe.getLong(address + (batch[i] << 3));
                    }
                }
            }
            case V_FLOAT -> {
                final float[] out = node.fb;
                if (address == 0) {
                    Arrays.fill(out, 0, n, Float.NaN);
                } else {
                    for (int i = 0; i < n; i++) {
                        out[i] = Unsafe.getFloat(address + (batch[i] << 2));
                    }
                }
            }
            default -> {
                final double[] out = node.db;
                if (address == 0) {
                    Arrays.fill(out, 0, n, Double.NaN);
                } else {
                    for (int i = 0; i < n; i++) {
                        out[i] = Unsafe.getDouble(address + (batch[i] << 3));
                    }
                }
            }
        }
    }

    // The window functions and the window steps' running sums start afresh, as the slot's
    // resetWindows() and toTop() start them.
    private void startLags() {
        for (int i = 0, n = lags.size(); i < n; i++) {
            final Node lag = lags.getQuick(i);
            lag.lagCount = 0;
            lag.lagPosition = 0;
        }
        startRunSums(0);
    }

    // The running sums of the window steps from the given one on start afresh, as their toTop().
    private void startRunSums(int fromStage) {
        for (int i = 0, n = runSums.size(); i < n; i++) {
            final Node sum = runSums.getQuick(i);
            if (sum.stageIndex >= fromStage) {
                sum.runSum = 0.0;
                sum.runCount = 0;
            }
        }
    }

    // Writes row j's output columns into the record at address, as the chain's record sink would
    // copy them from the last step's record.
    private void writeRow(long address, int j) {
        final Node[] outputs = this.outputs;
        final long[] offsets = outputOffsets;
        for (int c = 0, n = outputs.length; c < n; c++) {
            final Node node = outputs[c];
            final long a = address + offsets[c];
            switch (node.rep) {
                case V_INT -> Unsafe.putInt(a, node.ib[j]);
                case V_LONG -> Unsafe.putLong(a, node.lb[j]);
                case V_FLOAT -> Unsafe.putFloat(a, node.fb[j]);
                default -> Unsafe.putDouble(a, node.db[j]);
            }
        }
    }

    /**
     * The values of the step before a last GROUP BY at a row of the last batch: each column read with
     * its own type's getter, as the step's record reads it, and with the standard base classes'
     * conversions (IntFunction's getLong() and getDouble(), LongFunction's and FloatFunction's
     * getDouble()) for a reader that asks for them.
     */
    public static final class GroupInputRecord implements Record {
        private final Node[] columns;
        private int row;

        private GroupInputRecord(Node[] columns) {
            this.columns = columns;
        }

        @Override
        public double getDouble(int col) {
            final Node node = columns[col];
            return switch (node.rep) {
                case V_DOUBLE -> node.db[row];
                default -> convertToDouble(node.rep, node, row);
            };
        }

        @Override
        public float getFloat(int col) {
            final Node node = columns[col];
            if (node.rep != V_FLOAT) {
                throw new UnsupportedOperationException();
            }
            return node.fb[row];
        }

        @Override
        public int getInt(int col) {
            final Node node = columns[col];
            if (node.rep != V_INT) {
                throw new UnsupportedOperationException();
            }
            return node.ib[row];
        }

        @Override
        public long getLong(int col) {
            final Node node = columns[col];
            return switch (node.rep) {
                case V_LONG -> node.lb[row];
                // IntFunction.getLong()
                case V_INT -> Numbers.intToLong(node.ib[row]);
                default -> throw new UnsupportedOperationException();
            };
        }

        @Override
        public long getTimestamp(int col) {
            final Node node = columns[col];
            if (node.rep != V_LONG) {
                throw new UnsupportedOperationException();
            }
            return node.lb[row];
        }

        /**
         * Stands the record on the last batch's row j.
         */
        public void setRow(int j) {
            this.row = j;
        }
    }

    // Resolves the column of a record, read through a getter, to a node.
    private interface Resolver {
        @Nullable
        Node resolve(int column, int rep);
    }

    private static final class Compiler {
        private final IntList crossIndex;
        private final AsyncWindowRowKernel kernel;
        // the nodes made so far, to share a column load or a conversion between its readers
        private final ObjList<Node> made = new ObjList<>();

        Compiler(AsyncWindowRowKernel kernel, @Nullable IntList crossIndex) {
            this.kernel = kernel;
            this.crossIndex = crossIndex;
        }

        // The representation a type's own getter returns, -1 for one the kernel does not hold.
        static int repOf(int type) {
            return switch (ColumnType.tagOf(type)) {
                case ColumnType.INT, ColumnType.SYMBOL -> V_INT;
                case ColumnType.LONG, ColumnType.TIMESTAMP -> V_LONG;
                case ColumnType.DOUBLE -> V_DOUBLE;
                case ColumnType.FLOAT -> V_FLOAT;
                default -> -1;
            };
        }

        // Whether a function of the given representation, read through the getter of another,
        // converts as the standard base class's getter does, and the kernel has that conversion.
        static boolean canConvert(int from, int to) {
            if (from == to) {
                return true;
            }
            return switch (to) {
                case V_LONG -> from == V_INT;
                case V_DOUBLE -> from == V_INT || from == V_LONG || from == V_FLOAT;
                default -> false;
            };
        }

        @Nullable
        Node compileBool(Function function, Resolver resolver) {
            function = unwrap(function);
            if (function instanceof ColumnwiseBooleanOp op && declaresGetter(function, "getColumnwiseBooleanOp", "getBool")) {
                return compileBooleanOp(op, resolver);
            }
            if (!(function instanceof ColumnwiseComparison comparison) || !declaresGetter(function, "getColumnwiseComparison", "getBool")) {
                return null;
            }
            final int operandRep;
            switch (ColumnType.tagOf(comparison.getColumnwiseOperandType())) {
                case ColumnType.INT -> operandRep = V_INT;
                case ColumnType.LONG -> operandRep = V_LONG;
                case ColumnType.DOUBLE -> operandRep = V_DOUBLE;
                default -> {
                    return null;
                }
            }
            final Node a = compile(comparison.getLeft(), operandRep, resolver);
            final Node b = a != null ? compile(comparison.getRight(), operandRep, resolver) : null;
            if (b == null) {
                return null;
            }
            final int kind = switch (comparison.getColumnwiseComparison()) {
                case ColumnwiseComparison.CMP_LT -> K_LT;
                case ColumnwiseComparison.CMP_EQ -> K_EQ;
                default -> -1;
            };
            if (kind < 0) {
                return null;
            }
            final Node node = new Node(kind, V_BOOL);
            node.a = a;
            node.b = b;
            node.operandRep = operandRep;
            node.negated = comparison.isNegated();
            return add(node);
        }

        @Nullable
        private Node compileBooleanOp(ColumnwiseBooleanOp op, Resolver resolver) {
            final Node node;
            switch (op.getColumnwiseBooleanOp()) {
                case ColumnwiseBooleanOp.OP_IS_NULL -> {
                    final int operandRep;
                    switch (ColumnType.tagOf(op.getColumnwiseOperandType())) {
                        case ColumnType.FLOAT -> operandRep = V_FLOAT;
                        case ColumnType.DOUBLE -> operandRep = V_DOUBLE;
                        case ColumnType.INT -> operandRep = V_INT;
                        case ColumnType.LONG -> operandRep = V_LONG;
                        default -> {
                            return null;
                        }
                    }
                    final Node a = compile(op.getColumnwiseArg(0), operandRep, resolver);
                    if (a == null) {
                        return null;
                    }
                    node = new Node(K_IS_NULL, V_BOOL);
                    node.a = a;
                }
                case ColumnwiseBooleanOp.OP_EQ, ColumnwiseBooleanOp.OP_OR -> {
                    final Node a = compileBool(op.getColumnwiseArg(0), resolver);
                    final Node b = a != null ? compileBool(op.getColumnwiseArg(1), resolver) : null;
                    if (b == null) {
                        return null;
                    }
                    node = new Node(op.getColumnwiseBooleanOp() == ColumnwiseBooleanOp.OP_EQ ? K_BOOL_EQ : K_OR, V_BOOL);
                    node.a = a;
                    node.b = b;
                }
                default -> {
                    return null;
                }
            }
            node.negated = op.isColumnwiseNegated();
            return add(node);
        }

        // A searched CASE of the representation rep: its conditions, its values and its ELSE
        @Nullable
        private Node compileCase(CaseFunction function, int rep, Resolver resolver) {
            final CaseBranches.Values branches = function.branches();
            final ObjList<Function> conditions = branches.conditions();
            final ObjList<Function> values = branches.thenValues();
            if (conditions == null || conditions.size() != values.size() || rep == V_BOOL) {
                return null;
            }
            final int k = conditions.size();
            final Node[] conds = new Node[k];
            final Node[] vals = new Node[k];
            for (int c = 0; c < k; c++) {
                conds[c] = compileBool(conditions.getQuick(c), resolver);
                vals[c] = conds[c] != null ? compile(values.getQuick(c), rep, resolver) : null;
                if (vals[c] == null) {
                    return null;
                }
            }
            final Node otherwise = compile(branches.elseValue(), rep, resolver);
            if (otherwise == null) {
                return null;
            }
            final Node node = new Node(K_CASE, rep);
            node.conds = conds;
            node.vals = vals;
            node.otherwise = otherwise;
            return add(node);
        }

        // A window step's running sum, SumOverUnboundedRowsFrameFunction exactly: its argument read
        // with getDouble()
        @Nullable
        Node compileRunningSum(Function function, int rep, Resolver input, int stageIndex) {
            for (int i = 0, n = made.size(); i < n; i++) {
                final Node m = made.getQuick(i);
                if (m.kind == K_RUN_SUM && m.function == function) {
                    return convert(m, rep);
                }
            }
            final Node arg = compile(((BaseWindowFunction) function).getWindowArgument(), V_DOUBLE, input);
            if (arg == null) {
                return null;
            }
            final Node node = new Node(K_RUN_SUM, V_DOUBLE);
            node.a = arg;
            node.function = function;
            node.stageIndex = stageIndex;
            kernel.runSums.add(add(node));
            return convert(node, rep);
        }

        // A function of the window or a step, read through the getter of rep, against the
        // record its column references read.
        @Nullable
        Node compile(Function function, int rep, Resolver resolver) {
            function = unwrap(function);
            if (function.isConstant()) {
                return constant(function, rep);
            }
            final int ownRep = repOf(function.getType());
            if (ownRep < 0 || !convertsAsBaseClass(function, ownRep, rep)) {
                return null;
            }
            final Node own;
            if (function instanceof WindowFunction) {
                return null;
            } else if (function instanceof ColumnFunction cf) {
                if (!isPlainColumn(function)) {
                    return null;
                }
                own = resolver.resolve(cf.getColumnIndex(), ownRep);
            } else if (function instanceof ColumnwiseFunction cf && declaresGetter(function, "getColumnwiseOp", getterName(ownRep))) {
                final int op = cf.getColumnwiseOp();
                if ((op != ColumnwiseFunction.OP_ADD && op != ColumnwiseFunction.OP_SUB)
                        || (ownRep != V_INT && ownRep != V_LONG && ownRep != V_DOUBLE)
                        || !(function instanceof io.questdb.griffin.engine.functions.BinaryFunction bf)) {
                    return null;
                }
                final Node a = compile(bf.getLeft(), ownRep, resolver);
                final Node b = a != null ? compile(bf.getRight(), ownRep, resolver) : null;
                if (b == null) {
                    return null;
                }
                final Node node = new Node(op == ColumnwiseFunction.OP_ADD ? K_ADD : K_SUB, ownRep);
                node.a = a;
                node.b = b;
                own = add(node);
            } else if (function instanceof CaseFunction cf && declaresGetter(function, "branches", getterName(ownRep))) {
                own = compileCase(cf, ownRep, resolver);
            } else {
                return null;
            }
            return own != null ? convert(own, rep) : null;
        }

        // The window's function of a column: a column of the scan, a lag of one, or the stand-in
        // of a folded or replayed function, which outputs its argument.
        @Nullable
        Node compileWindowFunction(Function function, int rep) {
            function = unwrap(function);
            if (function.getClass() == AsyncWindowFoldEcho.class) {
                // AsyncWindowFoldEcho.computeNext() reads its argument's getDouble(), and getDouble()
                // returns it
                return rep == V_DOUBLE ? compile(((AsyncWindowFoldEcho) function).getKeyRunArgument(), V_DOUBLE, scanResolver()) : null;
            }
            if (function instanceof WindowFunction) {
                final int lagRep;
                final long offset;
                if (function.getClass() == LagLongFunctionFactory.LagFunction.class) {
                    final LagLongFunctionFactory.LagFunction lag = (LagLongFunctionFactory.LagFunction) function;
                    if (lag.hasLagDefault() || lag.isIgnoreNulls()) {
                        return null;
                    }
                    lagRep = V_LONG;
                    offset = lag.getLagOffset();
                } else if (function.getClass() == LagDoubleFunctionFactory.LagFunction.class) {
                    final LagDoubleFunctionFactory.LagFunction lag = (LagDoubleFunctionFactory.LagFunction) function;
                    if (lag.hasLagDefault() || lag.isIgnoreNulls()) {
                        return null;
                    }
                    lagRep = V_DOUBLE;
                    offset = lag.getLagOffset();
                } else {
                    return null;
                }
                // read through its own type's getter only
                if (offset < 1 || offset > MAX_LAG_OFFSET || lagRep != rep) {
                    return null;
                }
                // LagFunction.computeNext0() reads its argument with its own type's getter
                final Node arg = compile(((io.questdb.griffin.engine.functions.window.BaseWindowFunction) function).getWindowArgument(), lagRep, scanResolver());
                if (arg == null) {
                    return null;
                }
                Node node = null;
                for (int i = 0, n = made.size(); i < n; i++) {
                    final Node m = made.getQuick(i);
                    if (m.kind == K_LAG && m.function == function) {
                        node = m;
                        break;
                    }
                }
                if (node == null) {
                    node = new Node(K_LAG, lagRep);
                    node.a = arg;
                    node.lagOffset = (int) offset;
                    node.function = function;
                    node = add(node);
                    kernel.lags.add(node);
                }
                return convert(node, rep);
            }
            return compile(function, rep, scanResolver());
        }

        // Whether the function, of representation from, read through the getter of to, returns
        // what the standard base class of its type returns: its own getter, or IntFunction's
        // getLong() and getDouble(), LongFunction's getDouble(), FloatFunction's getDouble(), as
        // the class inherits them. The kernel converts as those do.
        private static boolean convertsAsBaseClass(Function function, int from, int to) {
            if (from == to) {
                return true;
            }
            if (!canConvert(from, to)) {
                return false;
            }
            final Class<?> base = switch (from) {
                case V_INT -> IntFunction.class;
                case V_LONG -> LongFunction.class;
                case V_FLOAT -> FloatFunction.class;
                default -> null;
            };
            return base != null && base.isInstance(function) && declaringClass(function.getClass(), getterName(to), Record.class) == base;
        }

        private static boolean declaresGetter(Function function, String claim, String getter) {
            final Class<?> claimClass = declaringClass(function.getClass(), claim);
            final Class<?> getterClass = getter != null ? declaringClass(function.getClass(), getter, Record.class) : null;
            return claimClass != null && claimClass == getterClass;
        }

        private static @Nullable Class<?> declaringClass(Class<?> clazz, String name, Class<?>... parameterTypes) {
            for (Class<?> c = clazz; c != null; c = c.getSuperclass()) {
                try {
                    c.getDeclaredMethod(name, parameterTypes);
                    return c;
                } catch (NoSuchMethodException ignore) {
                    // look further up
                }
            }
            return null;
        }

        private static @Nullable String getterName(int rep) {
            return switch (rep) {
                case V_INT -> "getInt";
                case V_LONG -> "getLong";
                case V_DOUBLE -> "getDouble";
                case V_FLOAT -> "getFloat";
                default -> null;
            };
        }

        // A column function the kernel reads as the record does, with the standard base class's
        // conversions: no other column function class qualifies.
        private static boolean isPlainColumn(Function function) {
            final Class<?> c = function.getClass();
            return c == IntColumn.class || c == LongColumn.class || c == DoubleColumn.class
                    || c == FloatColumn.class || c == TimestampColumn.class || c == SymbolColumn.class;
        }

        private static Function unwrap(Function function) {
            // a memoizer returns its argument's value
            while (function instanceof MemoizerFunction mf) {
                function = mf.getArg();
            }
            return function;
        }

        private Node add(Node node) {
            made.add(node);
            kernel.nodes.add(node);
            return node;
        }

        // A scan column read raw: shared between its readers.
        @Nullable
        Node column(int baseColumn, int rep) {
            final int scanColumn = crossIndex != null ? (baseColumn < crossIndex.size() ? crossIndex.getQuick(baseColumn) : -1) : baseColumn;
            if (scanColumn < 0) {
                return null;
            }
            if (scanColumn == kernel.keyColumnIndex && rep == V_INT) {
                for (int i = 0, n = made.size(); i < n; i++) {
                    if (made.getQuick(i).kind == K_KEY) {
                        return made.getQuick(i);
                    }
                }
                return add(new Node(K_KEY, V_INT));
            }
            for (int i = 0, n = made.size(); i < n; i++) {
                final Node m = made.getQuick(i);
                if (m.kind == K_COLUMN && m.column == scanColumn) {
                    return m.rep == rep ? m : null;
                }
            }
            final Node node = new Node(K_COLUMN, rep);
            node.column = scanColumn;
            kernel.columns.add(node);
            return add(node);
        }

        @Nullable
        private Node constant(Function function, int rep) {
            final Node node = new Node(K_CONST, rep);
            switch (rep) {
                case V_INT -> node.longConstant = function.getInt(null);
                case V_LONG -> node.longConstant = function.getLong(null);
                case V_DOUBLE -> node.doubleConstant = function.getDouble(null);
                case V_FLOAT -> node.doubleConstant = function.getFloat(null);
                default -> {
                    return null;
                }
            }
            return add(node);
        }

        @Nullable
        private Node convert(Node node, int rep) {
            if (node.rep == rep) {
                return node;
            }
            if (!canConvert(node.rep, rep)) {
                return null;
            }
            for (int i = 0, n = made.size(); i < n; i++) {
                final Node m = made.getQuick(i);
                if (m.kind == K_CONVERT && m.a == node && m.rep == rep) {
                    return m;
                }
            }
            final Node c = new Node(K_CONVERT, rep);
            c.a = node;
            return add(c);
        }

        private Resolver scanResolver() {
            return (column, rep) -> column(column, rep);
        }
    }

    private static final class Node {
        final int kind;
        final int rep;
        Node a;
        // K_COLUMN: the column's address in the current frame, 0 for none
        long address;
        Node b;
        boolean[] bb;
        int column;
        // K_CASE: the conditions, their values and the ELSE
        Node[] conds;
        double[] db;
        double doubleConstant;
        double[] doubleRing;
        float[] fb;
        // K_LAG: the window function it stands for
        Function function;
        int[] ib;
        long lagCount;
        int lagOffset;
        int lagPosition;
        long[] lb;
        long longConstant;
        long[] longRing;
        boolean negated;
        int operandRep;
        Node otherwise;
        // K_RUN_SUM: the sum and the count of the finite values so far, and the window step's index
        long runCount;
        double runSum;
        int stageIndex;
        Node[] vals;

        Node(int kind, int rep) {
            this.kind = kind;
            this.rep = rep;
        }
    }

    // A step's projection, by its output columns. Its functions read their column references
    // through the projection's own record: its own columns below the reserved slots, the input's
    // from there on, as VirtualFunctionRecord's join record serves them.
    private static final class ProjectionResolver implements Resolver {
        private final Compiler compiler;
        private final ObjList<Function> functions;
        private final Resolver input;
        private final Resolver own = this::resolveOwn;
        private final int reservedSlots;

        ProjectionResolver(Compiler compiler, ObjList<Function> functions, int reservedSlots, Resolver input) {
            this.compiler = compiler;
            this.functions = functions;
            this.reservedSlots = reservedSlots;
            this.input = input;
        }

        @Override
        public @Nullable Node resolve(int column, int rep) {
            return column < functions.size() ? compiler.compile(functions.getQuick(column), rep, own) : null;
        }

        private @Nullable Node resolveOwn(int column, int rep) {
            if (column >= reservedSlots) {
                return input.resolve(column - reservedSlots, rep);
            }
            return column < functions.size() ? compiler.compile(functions.getQuick(column), rep, own) : null;
        }
    }

    // A window step's record (VirtualRecord): each column is one of its functions over the input,
    // a running sum or a function of the input's columns.
    private record WindowStageResolver(Compiler compiler, ObjList<Function> functions, Resolver input, int stageIndex) implements Resolver {
        @Override
        public @Nullable Node resolve(int column, int rep) {
            if (column >= functions.size()) {
                return null;
            }
            final Function function = Compiler.unwrap(functions.getQuick(column));
            if (function.getClass() == SumDoubleWindowFunctionFactory.SumOverUnboundedRowsFrameFunction.class) {
                return compiler.compileRunningSum(function, rep, input, stageIndex);
            }
            return function instanceof WindowFunction ? null : compiler.compile(function, rep, input);
        }
    }

    // The window's own record: each column is one of its functions over the scan's record.
    private record WindowResolver(Compiler compiler, ObjList<Function> functions) implements Resolver {
        @Override
        public @Nullable Node resolve(int column, int rep) {
            return compiler.compileWindowFunction(functions.getQuick(column), rep);
        }
    }
}

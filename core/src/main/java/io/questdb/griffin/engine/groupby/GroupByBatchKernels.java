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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.ByteFunction;
import io.questdb.griffin.engine.functions.ColumnwiseFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.FloatFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.ShortFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.columns.ByteColumn;
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
import io.questdb.griffin.engine.functions.columns.DoubleColumn;
import io.questdb.griffin.engine.functions.columns.FloatColumn;
import io.questdb.griffin.engine.functions.columns.IntColumn;
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.functions.columns.ShortColumn;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * Column-wise batch kernels for the parallel GROUP BY.
 * <p>
 * The row path calls {@code computeNext()} once per row per aggregate, and each call walks the
 * argument's function tree with a virtual getter per node. On a hot JVM those call sites are
 * shared by every query the JVM has run, so they turn megamorphic and slow down with the query
 * mix. This class instead evaluates the arguments of the aggregates that support it once per
 * batch of rows, one operation at a time over typed buffers (column loads, {@code + - * /} and
 * casts), and hands the buffers to the aggregate's batch kernel, a tight loop that updates the
 * group values. No virtual call is made per row.
 * <p>
 * <b>Exactness.</b> Every loop here is written to return, for each row, the same value as the
 * function tree's getter would, bit for bit:
 * <ul>
 *     <li>a column load reads the frame memory exactly as {@link PageFrameMemoryRecord} does;
 *     a frame where the column has no directly readable buffer (a column top, or a column read
 *     through a type conversion) makes the aggregate take the row path for that batch;</li>
 *     <li>arithmetic is only evaluated for classes that declare their operation through
 *     {@link ColumnwiseFunction}, and implicit conversions between types only when the getter is
 *     the one the standard base class ({@link IntFunction}, {@link LongFunction}, ...) defines;</li>
 *     <li>constants and runtime constants (bind variables) are read once per batch through their
 *     own getter;</li>
 *     <li>the kernels apply the same arithmetic as {@code computeFirst()}/{@code computeNext()},
 *     in the same row order, so the group values, and the results, are bit-identical to the row
 *     path for the same frame-to-worker assignment. {@code merge()} is unchanged.</li>
 * </ul>
 * Anything else, a function class or a type pair without a loop here, keeps the row path.
 * <p>
 * <b>Compilation.</b> {@link #compile} builds a {@link Program} once per factory from the owner's
 * aggregates: the node graph, with arguments shared by several aggregates, such as {@code ask - bid}
 * in {@code max(ask - bid)} and {@code stddev(ask - bid)}, interned into one node so that they are
 * evaluated once per batch. {@link Program#newInstance} then makes the per-slot evaluator (the
 * owner and each worker) without compiling again: it only checks that the slot's aggregates have
 * the program's shape and binds the slot's own constant functions.
 * <p>
 * <b>Memory.</b> The argument buffers of a slot are one native block of
 * {@link Program#getScratchBytes()}, allocated on the slot's first batch under the query's
 * {@link MemoryTracker} and freed by {@link #clear()} (cursor close) and {@link #close()}. A program
 * whose buffers would exceed {@link #getMaxScratchBytes()} over all slots is not built, so the
 * query takes the row path.
 * <p>
 * One instance serves one slot and is used by that slot's thread only.
 */
public final class GroupByBatchKernels implements QuietCloseable, Mutable {
    /**
     * The default cap on the argument buffers of one factory, over all its slots.
     */
    public static final long DEFAULT_MAX_SCRATCH_BYTES = 64L << 20;
    /**
     * Rows are the packed entries of a keyed batch, see {@link Map#decodeBatchRowIndex(long)}.
     */
    public static final int MODE_PACKED = 0;
    /**
     * Rows are the contiguous frame rows {@code [lo, lo + rowCount)}.
     */
    public static final int MODE_RANGE = 1;
    /**
     * Rows are frame row indexes, stored as longs.
     */
    public static final int MODE_ROWS = 2;
    private static final int KIND_BINARY = 4;
    private static final int KIND_CAST = 3;
    private static final int KIND_COLUMN = 0;
    private static final int KIND_CONSTANT = 1;
    private static final int KIND_CONVERT = 2;
    // The methods whose declaring class the compiler checks, see DECLARING.
    private static final int M_COLUMNWISE_OP = 6;
    private static final int M_COLUMNWISE_OPERAND_TYPE = 7;
    private static final int M_COUNT = 8;
    private static final int M_GET_BYTE = 5;
    private static final int M_GET_DOUBLE = 3;
    private static final int M_GET_FLOAT = 2;
    private static final int M_GET_INT = 0;
    private static final int M_GET_LONG = 1;
    private static final int M_GET_SHORT = 4;
    // Per class, the class declaring each M_* method (null when the class has none), looked up
    // once per class rather than once per compile.
    private static final ClassValue<Class<?>[]> DECLARING = new ClassValue<>() {
        @Override
        protected Class<?>[] computeValue(Class<?> type) {
            final Class<?>[] declaring = new Class<?>[M_COUNT];
            declaring[M_GET_INT] = declaringClass(type, "getInt", Record.class);
            declaring[M_GET_LONG] = declaringClass(type, "getLong", Record.class);
            declaring[M_GET_FLOAT] = declaringClass(type, "getFloat", Record.class);
            declaring[M_GET_DOUBLE] = declaringClass(type, "getDouble", Record.class);
            declaring[M_GET_SHORT] = declaringClass(type, "getShort", Record.class);
            declaring[M_GET_BYTE] = declaringClass(type, "getByte", Record.class);
            declaring[M_COLUMNWISE_OP] = declaringClass(type, "getColumnwiseOp");
            declaring[M_COLUMNWISE_OPERAND_TYPE] = declaringClass(type, "getColumnwiseOperandType");
            return declaring;
        }
    };
    private static final ClassValue<Boolean> KERNEL_DECLARED = new ClassValue<>() {
        @Override
        protected Boolean computeValue(Class<?> type) {
            return isKernelDeclaredWithRowMethods(type);
        }
    };
    private static long maxScratchBytes = DEFAULT_MAX_SCRATCH_BYTES;
    private final ObjList<Args> args;
    private final Function[] constants;
    private final long[] nodeEpochs;
    private final boolean[] nodeOk;
    private final Program program;
    private long epoch;
    // Batches an aggregate took with its kernel, batches it had to take the row path for, frames
    // the reducer kept wholly on the row path, and rows passed to of(). Plain counters: one
    // slot's thread updates them.
    private long kernelBatchCount;
    private long kernelRowCount;
    private long rowPathFrameCount;
    private long lo;
    private MemoryTracker memoryTracker;
    private int mode;
    private PageFrameMemoryRecord record;
    private long rowPathBatchCount;
    private int rowCount;
    // the frame row indexes of the batch, or 0 for contiguous rows from lo
    private long rows;
    private boolean rowsDecoded;
    private long rowsAddr;
    private long scratchAddr;
    private MemoryTracker scratchTracker;

    private GroupByBatchKernels(Program program, Function[] constants) {
        this.program = program;
        this.constants = constants;
        final int nodeCount = program.nodes.length;
        this.nodeEpochs = new long[nodeCount];
        this.nodeOk = new boolean[nodeCount];
        this.args = new ObjList<>(program.functionCount);
        for (int i = 0, n = program.functionCount; i < n; i++) {
            final Node[] nodes = program.argNodes[i];
            this.args.extendAndSet(i, nodes != null ? new Args(nodes) : null);
        }
    }

    /**
     * Compiles the batch kernels of a factory's aggregates, once for all its slots.
     *
     * @param functions the owner's aggregates
     * @param capacity  the most rows a batch passed to {@link #of} holds
     * @param slotCount the number of slots (the owner and the workers) that may each allocate the
     *                  argument buffers
     * @return the program, or null when no aggregate has a kernel or the argument buffers of all
     * slots would exceed {@link #getMaxScratchBytes()}
     */
    public static @Nullable Program compile(ObjList<GroupByFunction> functions, int capacity, int slotCount) {
        final Compiler compiler = new Compiler();
        final int functionCount = functions.size();
        final Node[][] argNodes = new Node[functionCount][];
        int kernelCount = 0;
        for (int i = 0; i < functionCount; i++) {
            argNodes[i] = compiler.compileKernel(functions.getQuick(i));
            if (argNodes[i] != null) {
                kernelCount++;
            }
        }
        if (kernelCount == 0) {
            return null;
        }
        final Program program = compiler.finish(functions, argNodes, kernelCount, capacity);
        if (program.scratchBytes > maxScratchBytes / Math.max(1, slotCount)) {
            return null;
        }
        return program;
    }

    /**
     * Sums the counters of a factory's slots: [batches the aggregates took with their kernels,
     * batches they took the row path for, frames the reducer kept wholly on the row path (see
     * {@link #countRowPathFrame()}), rows passed to {@link #of}].
     */
    @TestOnly
    public static long[] getCounts(@Nullable GroupByBatchKernels owner, @Nullable PerWorker perWorker) {
        final long[] counts = new long[4];
        addCounts(owner, counts);
        if (perWorker != null) {
            for (int i = 0, n = perWorker.kernels.size(); i < n; i++) {
                addCounts(perWorker.kernels.getQuick(i), counts);
            }
        }
        return counts;
    }

    /**
     * The cap on the argument buffers of one factory, over all its slots.
     */
    public static long getMaxScratchBytes() {
        return maxScratchBytes;
    }

    @TestOnly
    public static void setMaxScratchBytes(long bytes) {
        maxScratchBytes = bytes;
    }

    /**
     * Whether the aggregate can use a batch kernel at all, regardless of its arguments.
     */
    public static boolean supportsKernel(GroupByFunction function) {
        return function.getBatchKernelArgCount() > 0
                && !function.isOrderSensitive()
                && KERNEL_DECLARED.get(function.getClass());
    }

    /**
     * Frees the argument buffers and unbinds the memory tracker. The next batch allocates the
     * buffers again.
     */
    @Override
    public void clear() {
        freeScratch();
        memoryTracker = null;
    }

    @Override
    public void close() {
        freeScratch();
    }

    /**
     * Counts a frame the reducer kept wholly on the row path without consulting the kernels: a
     * late-materialized Parquet frame, or a frame with column tops or type casts in the vectorized
     * non-keyed reduce.
     */
    public void countRowPathFrame() {
        rowPathFrameCount++;
    }

    @TestOnly
    public long getAllocatedScratchBytes() {
        return scratchAddr != 0 ? program.scratchBytes : 0;
    }

    @TestOnly
    public Function getConstant(int index) {
        return constants[index];
    }

    @TestOnly
    public int getConstantCount() {
        return constants.length;
    }

    public int getCapacity() {
        return program.capacity;
    }

    public int getKernelCount() {
        return program.kernelCount;
    }

    public Program getProgram() {
        return program;
    }

    public boolean isKernel(int functionIndex) {
        return program.isKernel(functionIndex);
    }

    /**
     * Starts a batch. Must be called before {@link #prepare(int)} for each batch.
     *
     * @param record   the frame's record; only its page addresses are read, its position is not
     *                 changed
     * @param mode     one of {@link #MODE_PACKED}, {@link #MODE_RANGE} or {@link #MODE_ROWS}
     * @param rowsAddr the packed entries or row indexes, unused in {@link #MODE_RANGE}
     * @param lo       the first row in {@link #MODE_RANGE}, unused otherwise
     * @param rowCount the number of rows, at most {@link #getCapacity()}
     */
    public void of(PageFrameMemoryRecord record, int mode, long rowsAddr, long lo, int rowCount) {
        assert rowCount <= program.capacity;
        if (scratchAddr == 0) {
            allocateScratch();
        }
        this.epoch++;
        this.record = record;
        this.mode = mode;
        this.rowsAddr = rowsAddr;
        this.lo = lo;
        this.rowCount = rowCount;
        this.rows = mode == MODE_ROWS ? rowsAddr : 0;
        // packed entries are decoded into frame row indexes once per batch, on the first load
        this.rowsDecoded = mode != MODE_PACKED;
        this.kernelRowCount += rowCount;
    }

    /**
     * Evaluates the arguments of an aggregate for the current batch.
     *
     * @return the evaluated arguments, or null when the aggregate has no kernel or must take the
     * row path for this batch (a column with no readable buffer in this frame)
     */
    public @Nullable Args prepare(int functionIndex) {
        final Args a = args.getQuiet(functionIndex);
        if (a == null) {
            return null;
        }
        for (int i = 0, n = a.nodes.length; i < n; i++) {
            if (!evaluate(a.nodes[i])) {
                rowPathBatchCount++;
                return null;
            }
        }
        kernelBatchCount++;
        return a;
    }

    /**
     * Binds the query's memory tracker, which the next allocation of the argument buffers is
     * charged to.
     */
    public void setMemoryTracker(@Nullable MemoryTracker memoryTracker) {
        this.memoryTracker = memoryTracker;
    }

    private static void addCounts(@Nullable GroupByBatchKernels kernels, long[] counts) {
        if (kernels != null) {
            counts[0] += kernels.kernelBatchCount;
            counts[1] += kernels.rowPathBatchCount;
            counts[2] += kernels.rowPathFrameCount;
            counts[3] += kernels.kernelRowCount;
        }
    }

    private static Class<?> baseClassOf(int type) {
        switch (type) {
            case ColumnType.INT:
                return IntFunction.class;
            case ColumnType.LONG:
                return LongFunction.class;
            case ColumnType.FLOAT:
                return FloatFunction.class;
            case ColumnType.DOUBLE:
                return DoubleFunction.class;
            case ColumnType.SHORT:
                return ShortFunction.class;
            default:
                return ByteFunction.class;
        }
    }

    private static boolean castSupported(int from, int to) {
        // The explicit cast rules of CastXToYFunctionFactory, keyed by the getter the cast reads.
        // Casts to FLOAT are left out: the optimiser strips ::float from an aggregate's argument,
        // so none reaches the evaluator and no test could cover one. FLOAT to FLOAT and DOUBLE to
        // DOUBLE are the casts that read the getter of their own result type (long::float,
        // long::double): the value passes through.
        switch (from) {
            case ColumnType.INT:
            case ColumnType.SHORT:
            case ColumnType.BYTE:
                return to == ColumnType.INT || to == ColumnType.LONG || to == ColumnType.DOUBLE;
            case ColumnType.LONG:
                return to == ColumnType.INT;
            case ColumnType.FLOAT:
                return to == ColumnType.INT || to == ColumnType.LONG || to == ColumnType.FLOAT || to == ColumnType.DOUBLE;
            case ColumnType.DOUBLE:
                return to == ColumnType.INT || to == ColumnType.LONG || to == ColumnType.DOUBLE;
            default:
                return false;
        }
    }

    private static boolean columnConvertSupported(int from, int to) {
        // The getters IntFunction, LongFunction, FloatFunction, DoubleFunction, ShortFunction and
        // ByteFunction define for types other than their own, applied by the column load itself.
        switch (from) {
            case ColumnType.INT:
                return to == ColumnType.LONG || to == ColumnType.FLOAT || to == ColumnType.DOUBLE;
            case ColumnType.LONG:
            case ColumnType.FLOAT:
                // no FLOAT consumer reads a LONG or DOUBLE: QuestDB widens such mixes to DOUBLE
                return to == ColumnType.DOUBLE;
            case ColumnType.SHORT:
            case ColumnType.BYTE:
                return to == ColumnType.INT || to == ColumnType.LONG || to == ColumnType.FLOAT || to == ColumnType.DOUBLE;
            default:
                return false;
        }
    }

    private static boolean convertSupported(int from, int to) {
        // The same getters for a computed value. SHORT and BYTE values only come from columns, and
        // no FLOAT consumer reads a LONG or DOUBLE, so those pairs are not needed here.
        switch (from) {
            case ColumnType.INT:
                return to == ColumnType.LONG || to == ColumnType.FLOAT || to == ColumnType.DOUBLE;
            case ColumnType.LONG:
            case ColumnType.FLOAT:
                return to == ColumnType.DOUBLE;
            default:
                return false;
        }
    }

    private static boolean declaredBy(Class<?> clazz, int method, Class<?> declaring) {
        return DECLARING.get(clazz)[method] == declaring;
    }

    private static @Nullable Class<?> declaringClass(Class<?> clazz, String name, Class<?>... parameterTypes) {
        try {
            return clazz.getMethod(name, parameterTypes).getDeclaringClass();
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    // bytes per value in the buffers: INT, SHORT and BYTE values are held widened to int
    private static int elementSize(int type) {
        return type == ColumnType.LONG || type == ColumnType.DOUBLE ? Long.BYTES : Integer.BYTES;
    }

    private static int getterOf(int type) {
        switch (type) {
            case ColumnType.INT:
                return M_GET_INT;
            case ColumnType.LONG:
                return M_GET_LONG;
            case ColumnType.FLOAT:
                return M_GET_FLOAT;
            case ColumnType.DOUBLE:
                return M_GET_DOUBLE;
            case ColumnType.SHORT:
                return M_GET_SHORT;
            default:
                return M_GET_BYTE;
        }
    }

    private static boolean isKernelDeclaredWithRowMethods(Class<?> clazz) {
        // A kernel is only trusted where it sits next to the row methods it replicates. A subclass
        // that overrides computeFirst(), computeNext(), the keyed row path computeKeyedBatch(), or
        // a row-path helper the kernels inline, such as the protected aggregate() of stddev,
        // variance, covar and corr, without its own kernel keeps the row path.
        try {
            final Class<?> first = clazz.getMethod("computeFirst", MapValue.class, Record.class, long.class).getDeclaringClass();
            final Class<?> next = clazz.getMethod("computeNext", MapValue.class, Record.class, long.class).getDeclaringClass();
            final Class<?> keyed = clazz.getMethod(
                    "computeKeyedBatchKernel",
                    FlyweightPackedMapValue.class,
                    long.class,
                    long.class,
                    int.class,
                    Args.class
            ).getDeclaringClass();
            final Class<?> notKeyed = clazz.getMethod("computeBatchKernel", MapValue.class, int.class, Args.class).getDeclaringClass();
            if (first != next || next != keyed || keyed != notKeyed) {
                return false;
            }
            // the keyed kernel mirrors computeKeyedBatch(): the interface default, which runs
            // computeFirst()/computeNext() per row, or the kernel class's own
            final Class<?> keyedRowPath = clazz.getMethod(
                    "computeKeyedBatch",
                    PageFrameMemoryRecord.class,
                    FlyweightPackedMapValue.class,
                    long.class,
                    long.class,
                    long.class,
                    long.class
            ).getDeclaringClass();
            if (keyedRowPath != keyed && keyedRowPath != GroupByFunction.class) {
                return false;
            }
            for (Class<?> c = clazz; c != keyed; c = c.getSuperclass()) {
                for (Method m : c.getDeclaredMethods()) {
                    switch (m.getName()) {
                        case "aggregate":
                        case "computeFirst":
                        case "computeNext":
                        case "computeKeyedBatch":
                            return false;
                        default:
                            break;
                    }
                }
            }
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    private static boolean isStorable(int type) {
        return type == ColumnType.INT || type == ColumnType.LONG || type == ColumnType.FLOAT
                || type == ColumnType.DOUBLE || type == ColumnType.SHORT || type == ColumnType.BYTE;
    }

    private void allocateScratch() {
        final long size = program.scratchBytes;
        scratchAddr = Unsafe.malloc(size, MemoryTag.NATIVE_GROUP_BY_FUNCTION, memoryTracker);
        scratchTracker = memoryTracker;
        for (int i = 0, n = args.size(); i < n; i++) {
            final Args a = args.getQuick(i);
            if (a != null) {
                for (int k = 0, m = a.nodes.length; k < m; k++) {
                    a.addresses[k] = scratchAddr + a.nodes[k].offset;
                }
            }
        }
        // the buffers of the previous allocation are gone: nothing is memoised
        epoch++;
    }

    private void binary(Node node, long out) {
        final int n = rowCount;
        final long l = scratchAddr + node.a.offset;
        final long r = scratchAddr + node.b.offset;
        switch (node.type) {
            case ColumnType.INT:
                switch (node.op) {
                    case ColumnwiseFunction.OP_ADD:
                        for (int i = 0; i < n; i++) {
                            final long p = (long) i << 2;
                            final int x = Unsafe.getInt(l + p);
                            final int y = Unsafe.getInt(r + p);
                            Unsafe.putInt(out + p, x == Numbers.INT_NULL || y == Numbers.INT_NULL ? Numbers.INT_NULL : x + y);
                        }
                        break;
                    case ColumnwiseFunction.OP_SUB:
                        for (int i = 0; i < n; i++) {
                            final long p = (long) i << 2;
                            final int x = Unsafe.getInt(l + p);
                            final int y = Unsafe.getInt(r + p);
                            Unsafe.putInt(out + p, x == Numbers.INT_NULL || y == Numbers.INT_NULL ? Numbers.INT_NULL : x - y);
                        }
                        break;
                    case ColumnwiseFunction.OP_MUL:
                        for (int i = 0; i < n; i++) {
                            final long p = (long) i << 2;
                            final int x = Unsafe.getInt(l + p);
                            final int y = Unsafe.getInt(r + p);
                            Unsafe.putInt(out + p, x == Numbers.INT_NULL || y == Numbers.INT_NULL ? Numbers.INT_NULL : x * y);
                        }
                        break;
                    default:
                        for (int i = 0; i < n; i++) {
                            final long p = (long) i << 2;
                            final int x = Unsafe.getInt(l + p);
                            final int y = Unsafe.getInt(r + p);
                            Unsafe.putInt(out + p, x == Numbers.INT_NULL || y == Numbers.INT_NULL || y == 0 ? Numbers.INT_NULL : x / y);
                        }
                        break;
                }
                break;
            case ColumnType.LONG:
                switch (node.op) {
                    case ColumnwiseFunction.OP_ADD:
                        for (int i = 0; i < n; i++) {
                            final long p = (long) i << 3;
                            final long x = Unsafe.getLong(l + p);
                            final long y = Unsafe.getLong(r + p);
                            Unsafe.putLong(out + p, x == Numbers.LONG_NULL || y == Numbers.LONG_NULL ? Numbers.LONG_NULL : x + y);
                        }
                        break;
                    case ColumnwiseFunction.OP_SUB:
                        for (int i = 0; i < n; i++) {
                            final long p = (long) i << 3;
                            final long x = Unsafe.getLong(l + p);
                            final long y = Unsafe.getLong(r + p);
                            Unsafe.putLong(out + p, x == Numbers.LONG_NULL || y == Numbers.LONG_NULL ? Numbers.LONG_NULL : x - y);
                        }
                        break;
                    case ColumnwiseFunction.OP_MUL:
                        for (int i = 0; i < n; i++) {
                            final long p = (long) i << 3;
                            final long x = Unsafe.getLong(l + p);
                            final long y = Unsafe.getLong(r + p);
                            Unsafe.putLong(out + p, x == Numbers.LONG_NULL || y == Numbers.LONG_NULL ? Numbers.LONG_NULL : x * y);
                        }
                        break;
                    default:
                        for (int i = 0; i < n; i++) {
                            final long p = (long) i << 3;
                            final long x = Unsafe.getLong(l + p);
                            final long y = Unsafe.getLong(r + p);
                            Unsafe.putLong(out + p, x == Numbers.LONG_NULL || y == Numbers.LONG_NULL || y == 0 ? Numbers.LONG_NULL : x / y);
                        }
                        break;
                }
                break;
            case ColumnType.FLOAT:
                switch (node.op) {
                    case ColumnwiseFunction.OP_ADD:
                        for (int i = 0; i < n; i++) {
                            final long p = (long) i << 2;
                            Unsafe.putFloat(out + p, Unsafe.getFloat(l + p) + Unsafe.getFloat(r + p));
                        }
                        break;
                    case ColumnwiseFunction.OP_SUB:
                        for (int i = 0; i < n; i++) {
                            final long p = (long) i << 2;
                            Unsafe.putFloat(out + p, Unsafe.getFloat(l + p) - Unsafe.getFloat(r + p));
                        }
                        break;
                    case ColumnwiseFunction.OP_MUL:
                        for (int i = 0; i < n; i++) {
                            final long p = (long) i << 2;
                            Unsafe.putFloat(out + p, Unsafe.getFloat(l + p) * Unsafe.getFloat(r + p));
                        }
                        break;
                    default:
                        for (int i = 0; i < n; i++) {
                            final long p = (long) i << 2;
                            final float f = Unsafe.getFloat(l + p) / Unsafe.getFloat(r + p);
                            Unsafe.putFloat(out + p, Numbers.isFinite(f) ? f : Float.NaN);
                        }
                        break;
                }
                break;
            default:
                switch (node.op) {
                    case ColumnwiseFunction.OP_ADD:
                        for (int i = 0; i < n; i++) {
                            final long p = (long) i << 3;
                            Unsafe.putDouble(out + p, Unsafe.getDouble(l + p) + Unsafe.getDouble(r + p));
                        }
                        break;
                    case ColumnwiseFunction.OP_SUB:
                        for (int i = 0; i < n; i++) {
                            final long p = (long) i << 3;
                            Unsafe.putDouble(out + p, Unsafe.getDouble(l + p) - Unsafe.getDouble(r + p));
                        }
                        break;
                    case ColumnwiseFunction.OP_MUL:
                        for (int i = 0; i < n; i++) {
                            final long p = (long) i << 3;
                            Unsafe.putDouble(out + p, Unsafe.getDouble(l + p) * Unsafe.getDouble(r + p));
                        }
                        break;
                    default:
                        for (int i = 0; i < n; i++) {
                            final long p = (long) i << 3;
                            final double d = Unsafe.getDouble(l + p) / Unsafe.getDouble(r + p);
                            Unsafe.putDouble(out + p, Numbers.isFinite(d) ? d : Double.NaN);
                        }
                        break;
                }
                break;
        }
    }

    private void cast(Node node, long out) {
        // The getters of CastXToYFunctionFactory, operand type X in node.op, result type Y.
        final int n = rowCount;
        final long in = scratchAddr + node.a.offset;
        final int from = node.op;
        switch (node.type) {
            case ColumnType.INT:
                if (from == ColumnType.LONG) {
                    for (int i = 0; i < n; i++) {
                        final long value = Unsafe.getLong(in + ((long) i << 3));
                        Unsafe.putInt(out + ((long) i << 2), value == Numbers.LONG_NULL ? Numbers.INT_NULL : (int) value);
                    }
                } else if (from == ColumnType.FLOAT) {
                    for (int i = 0; i < n; i++) {
                        final long p = (long) i << 2;
                        final float value = Unsafe.getFloat(in + p);
                        Unsafe.putInt(out + p, Numbers.isNull(value) || value > Integer.MAX_VALUE || value < Integer.MIN_VALUE ? Numbers.INT_NULL : (int) value);
                    }
                } else if (from == ColumnType.DOUBLE) {
                    for (int i = 0; i < n; i++) {
                        final double value = Unsafe.getDouble(in + ((long) i << 3));
                        Unsafe.putInt(out + ((long) i << 2), Numbers.isNull(value) || value > Integer.MAX_VALUE || value < Integer.MIN_VALUE ? Numbers.INT_NULL : (int) value);
                    }
                } else {
                    // SHORT and BYTE are held widened to int
                    Unsafe.copyMemory(in, out, (long) n << 2);
                }
                break;
            case ColumnType.LONG:
                if (from == ColumnType.INT) {
                    for (int i = 0; i < n; i++) {
                        Unsafe.putLong(out + ((long) i << 3), Numbers.intToLong(Unsafe.getInt(in + ((long) i << 2))));
                    }
                } else if (from == ColumnType.FLOAT) {
                    for (int i = 0; i < n; i++) {
                        final float value = Unsafe.getFloat(in + ((long) i << 2));
                        Unsafe.putLong(out + ((long) i << 3), Numbers.isNull(value) || value > Long.MAX_VALUE || value < Long.MIN_VALUE ? Numbers.LONG_NULL : (long) value);
                    }
                } else if (from == ColumnType.DOUBLE) {
                    for (int i = 0; i < n; i++) {
                        final long p = (long) i << 3;
                        final double value = Unsafe.getDouble(in + p);
                        Unsafe.putLong(out + p, Numbers.isNull(value) || value > Long.MAX_VALUE || value < Long.MIN_VALUE ? Numbers.LONG_NULL : (long) value);
                    }
                } else {
                    for (int i = 0; i < n; i++) {
                        Unsafe.putLong(out + ((long) i << 3), Unsafe.getInt(in + ((long) i << 2)));
                    }
                }
                break;
            default:
                if (from == ColumnType.INT) {
                    for (int i = 0; i < n; i++) {
                        final int value = Unsafe.getInt(in + ((long) i << 2));
                        Unsafe.putDouble(out + ((long) i << 3), value != Numbers.INT_NULL ? value : Double.NaN);
                    }
                } else if (from == ColumnType.FLOAT) {
                    for (int i = 0; i < n; i++) {
                        Unsafe.putDouble(out + ((long) i << 3), Unsafe.getFloat(in + ((long) i << 2)));
                    }
                } else {
                    for (int i = 0; i < n; i++) {
                        Unsafe.putDouble(out + ((long) i << 3), Unsafe.getInt(in + ((long) i << 2)));
                    }
                }
                break;
        }
    }

    private boolean column(Node node, long out) {
        final long addr = record.getPageAddress(node.columnIndex);
        if (addr == 0) {
            // A column top or a converted column: PageFrameMemoryRecord reads these through its
            // typed getters, so the aggregate takes the row path for this batch.
            return false;
        }
        // The load and, when the parent reads the column with another type's getter, the base
        // class's conversion, in one pass, with the row access chosen once per load.
        if (mode == MODE_RANGE) {
            loadRange(node.type, node.op, addr, lo, out, rowCount);
        } else {
            if (!rowsDecoded) {
                decodeRows();
            }
            loadRows(node.type, node.op, addr, rows, out, rowCount);
        }
        return true;
    }

    private void constant(Node node, long out) {
        final int n = rowCount;
        final Function f = constants[node.constIndex];
        switch (node.type) {
            case ColumnType.INT:
                fillInt(out, n, f.getInt(record));
                break;
            case ColumnType.LONG: {
                final long value = f.getLong(record);
                for (int i = 0; i < n; i++) {
                    Unsafe.putLong(out + ((long) i << 3), value);
                }
                break;
            }
            case ColumnType.FLOAT: {
                final float value = f.getFloat(record);
                for (int i = 0; i < n; i++) {
                    Unsafe.putFloat(out + ((long) i << 2), value);
                }
                break;
            }
            case ColumnType.DOUBLE: {
                final double value = f.getDouble(record);
                for (int i = 0; i < n; i++) {
                    Unsafe.putDouble(out + ((long) i << 3), value);
                }
                break;
            }
            case ColumnType.SHORT:
                fillInt(out, n, f.getShort(record));
                break;
            default:
                fillInt(out, n, f.getByte(record));
                break;
        }
    }

    private void convert(Node node, long out) {
        // The getters IntFunction, LongFunction and FloatFunction define for other types, operand
        // type in node.op, result type node.type (see convertSupported())
        final int n = rowCount;
        final long in = scratchAddr + node.a.offset;
        final int from = node.op;
        switch (node.type) {
            case ColumnType.LONG:
                for (int i = 0; i < n; i++) {
                    Unsafe.putLong(out + ((long) i << 3), Numbers.intToLong(Unsafe.getInt(in + ((long) i << 2))));
                }
                break;
            case ColumnType.FLOAT:
                for (int i = 0; i < n; i++) {
                    final long p = (long) i << 2;
                    Unsafe.putFloat(out + p, Numbers.intToFloat(Unsafe.getInt(in + p)));
                }
                break;
            default:
                if (from == ColumnType.INT) {
                    for (int i = 0; i < n; i++) {
                        Unsafe.putDouble(out + ((long) i << 3), Numbers.intToDouble(Unsafe.getInt(in + ((long) i << 2))));
                    }
                } else if (from == ColumnType.LONG) {
                    for (int i = 0; i < n; i++) {
                        final long p = (long) i << 3;
                        final long value = Unsafe.getLong(in + p);
                        Unsafe.putDouble(out + p, value != Numbers.LONG_NULL ? value : Double.NaN);
                    }
                } else {
                    for (int i = 0; i < n; i++) {
                        Unsafe.putDouble(out + ((long) i << 3), Unsafe.getFloat(in + ((long) i << 2)));
                    }
                }
                break;
        }
    }

    // The frame row indexes of the packed keyed-batch entries, into the head of the scratch block.
    private void decodeRows() {
        final long out = scratchAddr;
        final long in = rowsAddr;
        for (int i = 0, n = rowCount; i < n; i++) {
            final long p = (long) i << 3;
            Unsafe.putLong(out + p, Map.decodeBatchRowIndex(Unsafe.getLong(in + p)));
        }
        rows = out;
        rowsDecoded = true;
    }

    private boolean evaluate(Node node) {
        final int index = node.index;
        if (nodeEpochs[index] == epoch) {
            return nodeOk[index];
        }
        nodeEpochs[index] = epoch;
        nodeOk[index] = false;
        final long out = scratchAddr + node.offset;
        switch (node.kind) {
            case KIND_COLUMN:
                if (!column(node, out)) {
                    return false;
                }
                break;
            case KIND_CONSTANT:
                constant(node, out);
                break;
            case KIND_CONVERT:
                if (!evaluate(node.a)) {
                    return false;
                }
                convert(node, out);
                break;
            case KIND_CAST:
                if (!evaluate(node.a)) {
                    return false;
                }
                cast(node, out);
                break;
            default:
                if (!evaluate(node.a) || !evaluate(node.b)) {
                    return false;
                }
                binary(node, out);
                break;
        }
        nodeOk[index] = true;
        return true;
    }

    private static void fillInt(long out, int n, int value) {
        for (int i = 0; i < n; i++) {
            Unsafe.putInt(out + ((long) i << 2), value);
        }
    }

    private void freeScratch() {
        if (scratchAddr != 0) {
            Unsafe.free(scratchAddr, program.scratchBytes, MemoryTag.NATIVE_GROUP_BY_FUNCTION, scratchTracker);
            scratchAddr = 0;
            scratchTracker = null;
            for (int i = 0, n = args.size(); i < n; i++) {
                final Args a = args.getQuick(i);
                if (a != null) {
                    Arrays.fill(a.addresses, 0);
                }
            }
        }
    }

    // Contiguous frame rows [lo, lo + n) of a column of type `from`, read as type `to`.
    private static void loadRange(int to, int from, long addr, long lo, long out, int n) {
        switch (to) {
            case ColumnType.DOUBLE:
                switch (from) {
                    case ColumnType.DOUBLE:
                        Unsafe.copyMemory(addr + (lo << 3), out, (long) n << 3);
                        break;
                    case ColumnType.FLOAT: {
                        // FloatFunction.getDouble()
                        final long src = addr + (lo << 2);
                        for (int i = 0; i < n; i++) {
                            Unsafe.putDouble(out + ((long) i << 3), Unsafe.getFloat(src + ((long) i << 2)));
                        }
                        break;
                    }
                    case ColumnType.INT: {
                        // IntFunction.getDouble()
                        final long src = addr + (lo << 2);
                        for (int i = 0; i < n; i++) {
                            Unsafe.putDouble(out + ((long) i << 3), Numbers.intToDouble(Unsafe.getInt(src + ((long) i << 2))));
                        }
                        break;
                    }
                    case ColumnType.LONG: {
                        // LongFunction.getDouble()
                        final long src = addr + (lo << 3);
                        for (int i = 0; i < n; i++) {
                            final long p = (long) i << 3;
                            final long value = Unsafe.getLong(src + p);
                            Unsafe.putDouble(out + p, value != Numbers.LONG_NULL ? value : Double.NaN);
                        }
                        break;
                    }
                    case ColumnType.SHORT: {
                        final long src = addr + (lo << 1);
                        for (int i = 0; i < n; i++) {
                            Unsafe.putDouble(out + ((long) i << 3), Unsafe.getShort(src + ((long) i << 1)));
                        }
                        break;
                    }
                    default: {
                        final long src = addr + lo;
                        for (int i = 0; i < n; i++) {
                            Unsafe.putDouble(out + ((long) i << 3), Unsafe.getByte(src + i));
                        }
                        break;
                    }
                }
                break;
            case ColumnType.FLOAT:
                switch (from) {
                    case ColumnType.FLOAT:
                        Unsafe.copyMemory(addr + (lo << 2), out, (long) n << 2);
                        break;
                    case ColumnType.INT: {
                        // IntFunction.getFloat()
                        final long src = addr + (lo << 2);
                        for (int i = 0; i < n; i++) {
                            final long p = (long) i << 2;
                            Unsafe.putFloat(out + p, Numbers.intToFloat(Unsafe.getInt(src + p)));
                        }
                        break;
                    }
                    case ColumnType.SHORT: {
                        final long src = addr + (lo << 1);
                        for (int i = 0; i < n; i++) {
                            Unsafe.putFloat(out + ((long) i << 2), Unsafe.getShort(src + ((long) i << 1)));
                        }
                        break;
                    }
                    default: {
                        final long src = addr + lo;
                        for (int i = 0; i < n; i++) {
                            Unsafe.putFloat(out + ((long) i << 2), Unsafe.getByte(src + i));
                        }
                        break;
                    }
                }
                break;
            case ColumnType.LONG:
                switch (from) {
                    case ColumnType.LONG:
                        Unsafe.copyMemory(addr + (lo << 3), out, (long) n << 3);
                        break;
                    case ColumnType.INT: {
                        // IntFunction.getLong()
                        final long src = addr + (lo << 2);
                        for (int i = 0; i < n; i++) {
                            Unsafe.putLong(out + ((long) i << 3), Numbers.intToLong(Unsafe.getInt(src + ((long) i << 2))));
                        }
                        break;
                    }
                    case ColumnType.SHORT: {
                        final long src = addr + (lo << 1);
                        for (int i = 0; i < n; i++) {
                            Unsafe.putLong(out + ((long) i << 3), Unsafe.getShort(src + ((long) i << 1)));
                        }
                        break;
                    }
                    default: {
                        final long src = addr + lo;
                        for (int i = 0; i < n; i++) {
                            Unsafe.putLong(out + ((long) i << 3), Unsafe.getByte(src + i));
                        }
                        break;
                    }
                }
                break;
            default:
                // INT, SHORT and BYTE values, held widened to int
                switch (from) {
                    case ColumnType.INT:
                        Unsafe.copyMemory(addr + (lo << 2), out, (long) n << 2);
                        break;
                    case ColumnType.SHORT: {
                        final long src = addr + (lo << 1);
                        for (int i = 0; i < n; i++) {
                            Unsafe.putInt(out + ((long) i << 2), Unsafe.getShort(src + ((long) i << 1)));
                        }
                        break;
                    }
                    default: {
                        final long src = addr + lo;
                        for (int i = 0; i < n; i++) {
                            Unsafe.putInt(out + ((long) i << 2), Unsafe.getByte(src + i));
                        }
                        break;
                    }
                }
                break;
        }
    }

    // Frame rows at the row indexes stored as longs at `rows`, of a column of type `from`, read as
    // type `to`.
    private static void loadRows(int to, int from, long addr, long rows, long out, int n) {
        switch (to) {
            case ColumnType.DOUBLE:
                switch (from) {
                    case ColumnType.DOUBLE:
                        for (int i = 0; i < n; i++) {
                            final long p = (long) i << 3;
                            Unsafe.putDouble(out + p, Unsafe.getDouble(addr + (Unsafe.getLong(rows + p) << 3)));
                        }
                        break;
                    case ColumnType.FLOAT:
                        // FloatFunction.getDouble()
                        for (int i = 0; i < n; i++) {
                            final long p = (long) i << 3;
                            Unsafe.putDouble(out + p, Unsafe.getFloat(addr + (Unsafe.getLong(rows + p) << 2)));
                        }
                        break;
                    case ColumnType.INT:
                        // IntFunction.getDouble()
                        for (int i = 0; i < n; i++) {
                            final long p = (long) i << 3;
                            Unsafe.putDouble(out + p, Numbers.intToDouble(Unsafe.getInt(addr + (Unsafe.getLong(rows + p) << 2))));
                        }
                        break;
                    case ColumnType.LONG:
                        // LongFunction.getDouble()
                        for (int i = 0; i < n; i++) {
                            final long p = (long) i << 3;
                            final long value = Unsafe.getLong(addr + (Unsafe.getLong(rows + p) << 3));
                            Unsafe.putDouble(out + p, value != Numbers.LONG_NULL ? value : Double.NaN);
                        }
                        break;
                    case ColumnType.SHORT:
                        for (int i = 0; i < n; i++) {
                            final long p = (long) i << 3;
                            Unsafe.putDouble(out + p, Unsafe.getShort(addr + (Unsafe.getLong(rows + p) << 1)));
                        }
                        break;
                    default:
                        for (int i = 0; i < n; i++) {
                            final long p = (long) i << 3;
                            Unsafe.putDouble(out + p, Unsafe.getByte(addr + Unsafe.getLong(rows + p)));
                        }
                        break;
                }
                break;
            case ColumnType.FLOAT:
                switch (from) {
                    case ColumnType.FLOAT:
                        for (int i = 0; i < n; i++) {
                            Unsafe.putFloat(out + ((long) i << 2), Unsafe.getFloat(addr + (Unsafe.getLong(rows + ((long) i << 3)) << 2)));
                        }
                        break;
                    case ColumnType.INT:
                        // IntFunction.getFloat()
                        for (int i = 0; i < n; i++) {
                            Unsafe.putFloat(out + ((long) i << 2), Numbers.intToFloat(Unsafe.getInt(addr + (Unsafe.getLong(rows + ((long) i << 3)) << 2))));
                        }
                        break;
                    case ColumnType.SHORT:
                        for (int i = 0; i < n; i++) {
                            Unsafe.putFloat(out + ((long) i << 2), Unsafe.getShort(addr + (Unsafe.getLong(rows + ((long) i << 3)) << 1)));
                        }
                        break;
                    default:
                        for (int i = 0; i < n; i++) {
                            Unsafe.putFloat(out + ((long) i << 2), Unsafe.getByte(addr + Unsafe.getLong(rows + ((long) i << 3))));
                        }
                        break;
                }
                break;
            case ColumnType.LONG:
                switch (from) {
                    case ColumnType.LONG:
                        for (int i = 0; i < n; i++) {
                            final long p = (long) i << 3;
                            Unsafe.putLong(out + p, Unsafe.getLong(addr + (Unsafe.getLong(rows + p) << 3)));
                        }
                        break;
                    case ColumnType.INT:
                        // IntFunction.getLong()
                        for (int i = 0; i < n; i++) {
                            final long p = (long) i << 3;
                            Unsafe.putLong(out + p, Numbers.intToLong(Unsafe.getInt(addr + (Unsafe.getLong(rows + p) << 2))));
                        }
                        break;
                    case ColumnType.SHORT:
                        for (int i = 0; i < n; i++) {
                            final long p = (long) i << 3;
                            Unsafe.putLong(out + p, Unsafe.getShort(addr + (Unsafe.getLong(rows + p) << 1)));
                        }
                        break;
                    default:
                        for (int i = 0; i < n; i++) {
                            final long p = (long) i << 3;
                            Unsafe.putLong(out + p, Unsafe.getByte(addr + Unsafe.getLong(rows + p)));
                        }
                        break;
                }
                break;
            default:
                // INT, SHORT and BYTE values, held widened to int
                switch (from) {
                    case ColumnType.INT:
                        for (int i = 0; i < n; i++) {
                            Unsafe.putInt(out + ((long) i << 2), Unsafe.getInt(addr + (Unsafe.getLong(rows + ((long) i << 3)) << 2)));
                        }
                        break;
                    case ColumnType.SHORT:
                        for (int i = 0; i < n; i++) {
                            Unsafe.putInt(out + ((long) i << 2), Unsafe.getShort(addr + (Unsafe.getLong(rows + ((long) i << 3)) << 1)));
                        }
                        break;
                    default:
                        for (int i = 0; i < n; i++) {
                            Unsafe.putInt(out + ((long) i << 2), Unsafe.getByte(addr + Unsafe.getLong(rows + ((long) i << 3))));
                        }
                        break;
                }
                break;
        }
    }

    /**
     * The evaluated arguments of one aggregate for the current batch: value {@code i} of argument
     * {@code k} sits at {@code address(k) + i * size}, where size is 8 bytes for a LONG or DOUBLE
     * getter type and 4 bytes otherwise (INT, SHORT and BYTE values are widened to int).
     */
    public static final class Args {
        private final long[] addresses;
        private final Node[] nodes;

        private Args(Node[] nodes) {
            this.nodes = nodes;
            this.addresses = new long[nodes.length];
        }

        public long address(int argIndex) {
            return addresses[argIndex];
        }
    }

    /**
     * The batch kernels of a factory's aggregates, compiled once and shared, read-only, by the
     * evaluators of all its slots.
     */
    public static final class Program {
        private final Class<?>[] aggregateClasses;
        private final Node[][] argNodes;
        private final int capacity;
        private final int constantCount;
        private final int functionCount;
        private final int kernelCount;
        private final Node[] nodes;
        private final long scratchBytes;
        // The function tree walk of the compile, one entry per function visited, in visit order:
        // the function's class, its constant index (-1 when not a constant), its column index
        // (-1 when not a column), and the number of its arguments the walk descended into.
        private final int[] visitArity;
        private final Class<?>[] visitClasses;
        private final int[] visitColumns;
        private final int[] visitConstants;

        private Program(
                Node[] nodes,
                Node[][] argNodes,
                Class<?>[] aggregateClasses,
                int kernelCount,
                int constantCount,
                int capacity,
                long scratchBytes,
                Class<?>[] visitClasses,
                int[] visitConstants,
                int[] visitColumns,
                int[] visitArity
        ) {
            this.nodes = nodes;
            this.argNodes = argNodes;
            this.aggregateClasses = aggregateClasses;
            this.functionCount = argNodes.length;
            this.kernelCount = kernelCount;
            this.constantCount = constantCount;
            this.capacity = capacity;
            this.scratchBytes = scratchBytes;
            this.visitClasses = visitClasses;
            this.visitConstants = visitConstants;
            this.visitColumns = visitColumns;
            this.visitArity = visitArity;
        }

        public int getKernelCount() {
            return kernelCount;
        }

        /**
         * The size of one slot's argument buffers.
         */
        public long getScratchBytes() {
            return scratchBytes;
        }

        public boolean isKernel(int functionIndex) {
            return functionIndex < functionCount && argNodes[functionIndex] != null;
        }

        /**
         * Makes the evaluator of one slot. The slot's aggregates must be the program's, or copies
         * of them of the same shape (per-worker copies): every function the compile visited must
         * have the same class, column and constant position. Only the constant functions are
         * taken from the slot.
         *
         * @return the evaluator, or null when the slot's aggregates do not have the program's shape
         */
        public @Nullable GroupByBatchKernels newInstance(ObjList<GroupByFunction> functions) {
            if (functions.size() != functionCount) {
                return null;
            }
            final Function[] constants = new Function[constantCount];
            int visit = 0;
            for (int i = 0; i < functionCount; i++) {
                final Node[] nodes = argNodes[i];
                if (nodes == null) {
                    continue;
                }
                final GroupByFunction function = functions.getQuick(i);
                if (function.getClass() != aggregateClasses[i] || function.getBatchKernelArgCount() != nodes.length) {
                    return null;
                }
                for (int k = 0; k < nodes.length; k++) {
                    visit = bind(function.getBatchKernelArg(k), visit, constants);
                    if (visit < 0) {
                        return null;
                    }
                }
            }
            if (visit != visitClasses.length) {
                return null;
            }
            return new GroupByBatchKernels(this, constants);
        }

        // Walks the slot's function tree as the compile walked the owner's; returns the next visit
        // index, or -1 on a mismatch.
        private int bind(Function f, int visit, Function[] constants) {
            if (visit >= visitClasses.length || f.getClass() != visitClasses[visit]) {
                return -1;
            }
            final boolean constant = f.isConstant() || f.isRuntimeConstant();
            final int constantIndex = visitConstants[visit];
            if (constantIndex >= 0) {
                if (!constant) {
                    return -1;
                }
                constants[constantIndex] = f;
                return visit + 1;
            }
            if (constant) {
                return -1;
            }
            final int columnIndex = visitColumns[visit];
            if (columnIndex >= 0) {
                return ((ColumnFunction) f).getColumnIndex() == columnIndex ? visit + 1 : -1;
            }
            switch (visitArity[visit]) {
                case 1:
                    return bind(((UnaryFunction) f).getArg(), visit + 1, constants);
                case 2: {
                    final BinaryFunction bf = (BinaryFunction) f;
                    final int next = bind(bf.getLeft(), visit + 1, constants);
                    return next < 0 ? -1 : bind(bf.getRight(), next, constants);
                }
                default:
                    return -1;
            }
        }
    }

    /**
     * The evaluators of a factory's worker slots, each made from the program on the slot's first
     * use, by the slot's thread, so that compiling a factory does not pay for slots its queries
     * never run on.
     */
    public static final class PerWorker implements QuietCloseable, Mutable {
        private final ObjList<GroupByBatchKernels> kernels;
        private final ObjList<GroupByFunction> ownerFunctions;
        private final ObjList<ObjList<GroupByFunction>> perWorkerFunctions;
        private final Program program;
        private final boolean[] resolved;
        private MemoryTracker memoryTracker;

        /**
         * @param perWorkerFunctions the workers' own aggregates, or null when they share the owner's
         */
        public PerWorker(
                Program program,
                ObjList<GroupByFunction> ownerFunctions,
                @Nullable ObjList<ObjList<GroupByFunction>> perWorkerFunctions,
                int workerCount
        ) {
            this.program = program;
            this.ownerFunctions = ownerFunctions;
            this.perWorkerFunctions = perWorkerFunctions;
            this.kernels = new ObjList<>(workerCount);
            this.kernels.setPos(workerCount);
            this.resolved = new boolean[workerCount];
        }

        /**
         * Frees the slots' argument buffers and unbinds the memory tracker.
         */
        @Override
        public void clear() {
            Misc.clearObjList(kernels);
            memoryTracker = null;
        }

        @Override
        public void close() {
            for (int i = 0, n = kernels.size(); i < n; i++) {
                Misc.free(kernels.getQuick(i));
            }
        }

        /**
         * Returns the slot's evaluator, making it on first use. Only the slot's holder may call it.
         *
         * @return the evaluator, or null when the worker's aggregates do not have the program's
         * shape (they then take the row path)
         */
        public @Nullable GroupByBatchKernels get(int slotId) {
            final GroupByBatchKernels slotKernels = kernels.getQuick(slotId);
            if (slotKernels != null || resolved[slotId]) {
                return slotKernels;
            }
            return newSlotKernels(slotId);
        }

        /**
         * Returns the slot's evaluator if it has been made, without making it.
         */
        @TestOnly
        public @Nullable GroupByBatchKernels getIfMade(int slotId) {
            return kernels.getQuick(slotId);
        }

        public void setMemoryTracker(@Nullable MemoryTracker memoryTracker) {
            this.memoryTracker = memoryTracker;
            for (int i = 0, n = kernels.size(); i < n; i++) {
                final GroupByBatchKernels slotKernels = kernels.getQuick(i);
                if (slotKernels != null) {
                    slotKernels.setMemoryTracker(memoryTracker);
                }
            }
        }

        public int size() {
            return kernels.size();
        }

        private @Nullable GroupByBatchKernels newSlotKernels(int slotId) {
            resolved[slotId] = true;
            final GroupByBatchKernels slotKernels = program.newInstance(
                    perWorkerFunctions != null ? perWorkerFunctions.getQuick(slotId) : ownerFunctions
            );
            if (slotKernels != null) {
                slotKernels.setMemoryTracker(memoryTracker);
                kernels.setQuick(slotId, slotKernels);
            }
            return slotKernels;
        }
    }

    // Builds a Program: compiles the aggregates' argument trees into an interned node graph and
    // records the walk.
    private static final class Compiler {
        private final ObjList<Node> nodes = new ObjList<>();
        private final IntList visitArity = new IntList();
        private final ObjList<Class<?>> visitClasses = new ObjList<>();
        private final IntList visitColumns = new IntList();
        private final IntList visitConstants = new IntList();
        private int constantCount;

        private @Nullable Node compile(Function f, int getterType) {
            final int visit = visitClasses.size();
            visitClasses.add(f.getClass());
            visitConstants.add(-1);
            visitColumns.add(-1);
            visitArity.add(0);
            if (f.isConstant() || f.isRuntimeConstant()) {
                // A constant is read through its own getter once per batch, so any getter is exact.
                // Constants are never interned: each is the slot's own function.
                if (!isStorable(getterType)) {
                    return null;
                }
                visitConstants.setQuick(visit, constantCount);
                final Node node = new Node(KIND_CONSTANT, getterType, 0, -1, constantCount++, null, null);
                nodes.add(node);
                return node;
            }
            final int nativeType = ColumnType.tagOf(f.getType());
            if (!isStorable(nativeType)) {
                return null;
            }
            final Class<?> clazz = f.getClass();
            final Node node;
            if (clazz == IntColumn.class || clazz == LongColumn.class || clazz == FloatColumn.class
                    || clazz == DoubleColumn.class || clazz == ShortColumn.class || clazz == ByteColumn.class) {
                final int columnIndex = ((ColumnFunction) f).getColumnIndex();
                visitColumns.setQuick(visit, columnIndex);
                if (getterType != nativeType
                        && columnConvertSupported(nativeType, getterType)
                        && getterType != ColumnType.SHORT
                        && declaredBy(clazz, getterOf(getterType), baseClassOf(nativeType))) {
                    // the load converts as the base class's getter does: no separate pass
                    return intern(KIND_COLUMN, getterType, nativeType, columnIndex, null, null);
                }
                node = intern(KIND_COLUMN, nativeType, nativeType, columnIndex, null, null);
            } else if (f instanceof ColumnwiseFunction cf
                    && declaredBy(clazz, M_COLUMNWISE_OP, clazz)
                    && declaredBy(clazz, getterOf(nativeType), clazz)) {
                final int op = cf.getColumnwiseOp();
                if (op == ColumnwiseFunction.OP_CAST) {
                    if (!(f instanceof UnaryFunction uf) || !declaredBy(clazz, M_COLUMNWISE_OPERAND_TYPE, clazz)) {
                        return null;
                    }
                    final int operandType = cf.getColumnwiseOperandType();
                    if (!castSupported(operandType, nativeType)) {
                        return null;
                    }
                    visitArity.setQuick(visit, 1);
                    final Node a = compile(uf.getArg(), operandType);
                    if (a == null) {
                        return null;
                    }
                    // e.g. long::double reads the argument's getDouble(): the value passes through
                    node = operandType == nativeType ? a : intern(KIND_CAST, nativeType, operandType, -1, a, null);
                } else if (op >= ColumnwiseFunction.OP_ADD && op <= ColumnwiseFunction.OP_DIV) {
                    if (!(f instanceof BinaryFunction bf)) {
                        return null;
                    }
                    if (nativeType != ColumnType.INT && nativeType != ColumnType.LONG
                            && nativeType != ColumnType.FLOAT && nativeType != ColumnType.DOUBLE) {
                        return null;
                    }
                    visitArity.setQuick(visit, 2);
                    final Node a = compile(bf.getLeft(), nativeType);
                    if (a == null) {
                        return null;
                    }
                    final Node b = compile(bf.getRight(), nativeType);
                    if (b == null) {
                        return null;
                    }
                    node = intern(KIND_BINARY, nativeType, op, -1, a, b);
                } else {
                    return null;
                }
            } else {
                return null;
            }

            if (getterType == nativeType) {
                return node;
            }
            // The parent reads this function with another type's getter. That is exact only when the
            // getter is the standard base class's conversion, which convertSupported() mirrors.
            if (!convertSupported(nativeType, getterType) || !declaredBy(clazz, getterOf(getterType), baseClassOf(nativeType))) {
                return null;
            }
            return intern(KIND_CONVERT, getterType, nativeType, -1, node, null);
        }

        private Node @Nullable [] compileKernel(GroupByFunction function) {
            if (!supportsKernel(function)) {
                return null;
            }
            final int nodeCount = nodes.size();
            final int visitCount = visitClasses.size();
            final int savedConstantCount = constantCount;
            final int argCount = function.getBatchKernelArgCount();
            final Node[] argNodes = new Node[argCount];
            for (int i = 0; i < argCount; i++) {
                final Node node = compile(function.getBatchKernelArg(i), function.getBatchKernelArgType(i));
                if (node == null) {
                    // drop what this aggregate added: only its own later nodes refer to its nodes
                    nodes.setPos(nodeCount);
                    visitClasses.setPos(visitCount);
                    visitConstants.setPos(visitCount);
                    visitColumns.setPos(visitCount);
                    visitArity.setPos(visitCount);
                    constantCount = savedConstantCount;
                    return null;
                }
                argNodes[i] = node;
            }
            return argNodes;
        }

        private Program finish(ObjList<GroupByFunction> functions, Node[][] argNodes, int kernelCount, int capacity) {
            final Node[] nodeArray = new Node[nodes.size()];
            // the head of the block holds the decoded row indexes of a keyed batch
            long offset = (long) capacity << 3;
            for (int i = 0, n = nodes.size(); i < n; i++) {
                final Node node = nodes.getQuick(i);
                node.index = i;
                node.offset = offset;
                offset += (((long) capacity * elementSize(node.type)) + 7) & ~7L;
                nodeArray[i] = node;
            }
            final Class<?>[] aggregateClasses = new Class<?>[argNodes.length];
            for (int i = 0; i < argNodes.length; i++) {
                if (argNodes[i] != null) {
                    aggregateClasses[i] = functions.getQuick(i).getClass();
                }
            }
            final int visitCount = visitClasses.size();
            final Class<?>[] classes = new Class<?>[visitCount];
            final int[] constants = new int[visitCount];
            final int[] columns = new int[visitCount];
            final int[] arity = new int[visitCount];
            for (int i = 0; i < visitCount; i++) {
                classes[i] = visitClasses.getQuick(i);
                constants[i] = visitConstants.getQuick(i);
                columns[i] = visitColumns.getQuick(i);
                arity[i] = visitArity.getQuick(i);
            }
            return new Program(
                    nodeArray,
                    argNodes,
                    aggregateClasses,
                    kernelCount,
                    constantCount,
                    capacity,
                    offset,
                    classes,
                    constants,
                    columns,
                    arity
            );
        }

        // Returns the node with this structure, adding it if there is none. Exact: two nodes are
        // the same only when their kind, type, operation, column and children (by identity) are,
        // and constants, which are never interned, make every node above them distinct.
        private Node intern(int kind, int type, int op, int columnIndex, @Nullable Node a, @Nullable Node b) {
            for (int i = 0, n = nodes.size(); i < n; i++) {
                final Node node = nodes.getQuick(i);
                if (node.kind == kind && node.type == type && node.op == op && node.columnIndex == columnIndex
                        && node.a == a && node.b == b && kind != KIND_CONSTANT) {
                    return node;
                }
            }
            final Node node = new Node(kind, type, op, columnIndex, -1, a, b);
            nodes.add(node);
            return node;
        }
    }

    private static final class Node {
        private final Node a;
        private final Node b;
        private final int columnIndex;
        private final int constIndex;
        private final int kind;
        private final int op;
        private final int type;
        // set when the program is finished
        private int index;
        private long offset;

        private Node(int kind, int type, int op, int columnIndex, int constIndex, Node a, Node b) {
            this.kind = kind;
            this.type = type;
            this.op = op;
            this.columnIndex = columnIndex;
            this.constIndex = constIndex;
            this.a = a;
            this.b = b;
        }
    }
}

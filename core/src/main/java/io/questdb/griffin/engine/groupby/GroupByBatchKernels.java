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
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.HashMap;

/**
 * Column-wise batch kernels for the parallel GROUP BY.
 * <p>
 * The row path calls {@code computeNext()} once per row per aggregate, and each call walks the
 * argument's function tree with a virtual getter per node. On a hot JVM those call sites are
 * shared by every query the JVM has run, so they turn megamorphic and slow down with the query
 * mix. This class instead evaluates the arguments of the aggregates that support it once per
 * batch of rows, one operation at a time over typed arrays (column loads, {@code + - * /} and
 * casts), and hands the arrays to the aggregate's batch kernel, a tight loop that updates the
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
 * One instance serves one slot (the owner or a worker) and is used by that slot's thread only.
 * Arguments shared by several aggregates, such as {@code ask - bid} in {@code max(ask - bid)} and
 * {@code stddev(ask - bid)}, are evaluated once per batch.
 */
public final class GroupByBatchKernels {
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
    private static final ClassValue<Boolean> KERNEL_DECLARED = new ClassValue<>() {
        @Override
        protected Boolean computeValue(Class<?> type) {
            return isKernelDeclaredWithRowMethods(type);
        }
    };
    private final ObjList<Args> args;
    private final int capacity;
    private final int kernelCount;
    private long epoch;
    private long lo;
    private int mode;
    private PageFrameMemoryRecord record;
    // Batches an aggregate took with its kernel, and batches it had to take the row path for.
    // Plain counters: one slot's thread updates them.
    private long kernelBatchCount;
    private int rowCount;
    private long rowPathBatchCount;
    private long rowsAddr;

    private GroupByBatchKernels(ObjList<Args> args, int kernelCount, int capacity) {
        this.args = args;
        this.kernelCount = kernelCount;
        this.capacity = capacity;
    }

    /**
     * Compiles the batch kernels for a slot's aggregates.
     *
     * @param functions the slot's aggregates
     * @param capacity  the most rows a batch passed to {@link #of} holds
     * @return the kernels, or null when no aggregate has one
     */
    public static @Nullable GroupByBatchKernels newInstance(ObjList<GroupByFunction> functions, int capacity) {
        final ObjList<Args> args = new ObjList<>(functions.size());
        final HashMap<String, Node> interned = new HashMap<>();
        int kernelCount = 0;
        for (int i = 0, n = functions.size(); i < n; i++) {
            final Args a = compileKernel(functions.getQuick(i), interned);
            args.extendAndSet(i, a);
            if (a != null) {
                kernelCount++;
            }
        }
        return kernelCount > 0 ? new GroupByBatchKernels(args, kernelCount, capacity) : null;
    }

    /**
     * Whether the aggregate can use a batch kernel at all, regardless of its arguments.
     */
    public static boolean supportsKernel(GroupByFunction function) {
        return function.getBatchKernelArgCount() > 0
                && !function.isOrderSensitive()
                && KERNEL_DECLARED.get(function.getClass());
    }

    public int getCapacity() {
        return capacity;
    }

    @TestOnly
    public long getKernelBatchCount() {
        return kernelBatchCount;
    }

    public int getKernelCount() {
        return kernelCount;
    }

    @TestOnly
    public long getRowPathBatchCount() {
        return rowPathBatchCount;
    }

    public boolean isKernel(int functionIndex) {
        return args.getQuiet(functionIndex) != null;
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
        assert rowCount <= capacity;
        this.epoch++;
        this.record = record;
        this.mode = mode;
        this.rowsAddr = rowsAddr;
        this.lo = lo;
        this.rowCount = rowCount;
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

    private static boolean castSupported(int from, int to) {
        // The explicit cast rules of CastXToYFunctionFactory, keyed by the getter the cast reads.
        switch (from) {
            case ColumnType.INT:
                return to == ColumnType.LONG || to == ColumnType.FLOAT || to == ColumnType.DOUBLE;
            case ColumnType.LONG:
                return to == ColumnType.INT;
            case ColumnType.FLOAT:
                return to == ColumnType.INT || to == ColumnType.LONG || to == ColumnType.FLOAT || to == ColumnType.DOUBLE;
            case ColumnType.DOUBLE:
                return to == ColumnType.INT || to == ColumnType.LONG || to == ColumnType.FLOAT || to == ColumnType.DOUBLE;
            case ColumnType.SHORT:
            case ColumnType.BYTE:
                return to == ColumnType.INT || to == ColumnType.LONG || to == ColumnType.FLOAT || to == ColumnType.DOUBLE;
            default:
                return false;
        }
    }

    private static @Nullable Node compile(Function f, int getterType, HashMap<String, Node> interned) {
        if (f.isConstant() || f.isRuntimeConstant()) {
            // A constant is read through its own getter once per batch, so any getter is exact.
            if (!isStorable(getterType)) {
                return null;
            }
            return new Node(KIND_CONSTANT, getterType, 0, -1, f, null, null);
        }
        final int nativeType = ColumnType.tagOf(f.getType());
        if (!isStorable(nativeType)) {
            return null;
        }
        final Class<?> clazz = f.getClass();
        Node node;
        String key;
        if (clazz == IntColumn.class || clazz == LongColumn.class || clazz == FloatColumn.class
                || clazz == DoubleColumn.class || clazz == ShortColumn.class || clazz == ByteColumn.class) {
            final int columnIndex = ((ColumnFunction) f).getColumnIndex();
            if (getterType != nativeType
                    && convertSupported(nativeType, getterType)
                    && getterType != ColumnType.SHORT
                    && declaredBy(clazz, getterName(getterType), baseClassOf(nativeType))) {
                // the load converts as the base class's getter does: no separate pass
                key = "c" + nativeType + '>' + getterType + ':' + columnIndex;
                node = interned.get(key);
                if (node == null) {
                    node = new Node(KIND_COLUMN, getterType, nativeType, columnIndex, null, null, null);
                    node.key = key;
                    interned.put(key, node);
                }
                return node;
            }
            key = "c" + nativeType + ':' + columnIndex;
            node = interned.get(key);
            if (node == null) {
                node = new Node(KIND_COLUMN, nativeType, nativeType, columnIndex, null, null, null);
                interned.put(key, node);
            }
        } else if (f instanceof ColumnwiseFunction cf
                && declaredBy(clazz, "getColumnwiseOp", clazz)
                && declaredBy(clazz, getterName(nativeType), clazz)) {
            final int op = cf.getColumnwiseOp();
            if (op == ColumnwiseFunction.OP_CAST) {
                if (!(f instanceof UnaryFunction uf) || !declaredBy(clazz, "getColumnwiseOperandType", clazz)) {
                    return null;
                }
                final int operandType = cf.getColumnwiseOperandType();
                if (!castSupported(operandType, nativeType)) {
                    return null;
                }
                final Node a = compile(uf.getArg(), operandType, interned);
                if (a == null) {
                    return null;
                }
                if (operandType == nativeType) {
                    // e.g. long::double reads the argument's getDouble(): the value passes through
                    node = a;
                    key = null;
                } else {
                    key = "x" + operandType + '>' + nativeType + '(' + a.key + ')';
                    node = interned.get(key);
                    if (node == null) {
                        node = new Node(KIND_CAST, nativeType, operandType, -1, null, a, null);
                    }
                }
            } else if (op >= ColumnwiseFunction.OP_ADD && op <= ColumnwiseFunction.OP_DIV) {
                if (!(f instanceof BinaryFunction bf)) {
                    return null;
                }
                if (nativeType != ColumnType.INT && nativeType != ColumnType.LONG
                        && nativeType != ColumnType.FLOAT && nativeType != ColumnType.DOUBLE) {
                    return null;
                }
                final Node a = compile(bf.getLeft(), nativeType, interned);
                if (a == null) {
                    return null;
                }
                final Node b = compile(bf.getRight(), nativeType, interned);
                if (b == null) {
                    return null;
                }
                key = "b" + op + ':' + nativeType + '(' + a.key + ',' + b.key + ')';
                node = interned.get(key);
                if (node == null) {
                    node = new Node(KIND_BINARY, nativeType, op, -1, null, a, b);
                }
            } else {
                return null;
            }
            if (key != null && node.key == null) {
                node.key = key;
                interned.put(key, node);
            }
        } else {
            return null;
        }
        if (node.key == null) {
            node.key = key;
        }

        if (getterType == nativeType) {
            return node;
        }
        // The parent reads this function with another type's getter. That is exact only when the
        // getter is the standard base class's conversion, which convertSupported() mirrors.
        if (!convertSupported(nativeType, getterType) || !declaredBy(clazz, getterName(getterType), baseClassOf(nativeType))) {
            return null;
        }
        final String convertKey = "v" + nativeType + '>' + getterType + '(' + node.key + ')';
        Node convert = interned.get(convertKey);
        if (convert == null) {
            convert = new Node(KIND_CONVERT, getterType, nativeType, -1, null, node, null);
            convert.key = convertKey;
            interned.put(convertKey, convert);
        }
        return convert;
    }

    private static @Nullable Args compileKernel(GroupByFunction function, HashMap<String, Node> interned) {
        if (!supportsKernel(function)) {
            return null;
        }
        final int argCount = function.getBatchKernelArgCount();
        final Node[] nodes = new Node[argCount];
        for (int i = 0; i < argCount; i++) {
            final Node node = compile(function.getBatchKernelArg(i), function.getBatchKernelArgType(i), interned);
            if (node == null) {
                return null;
            }
            nodes[i] = node;
        }
        return new Args(nodes);
    }

    private static boolean convertSupported(int from, int to) {
        // The getters IntFunction, LongFunction, FloatFunction, DoubleFunction, ShortFunction and
        // ByteFunction define for types other than their own.
        switch (from) {
            case ColumnType.INT:
                return to == ColumnType.LONG || to == ColumnType.FLOAT || to == ColumnType.DOUBLE;
            case ColumnType.LONG:
                return to == ColumnType.FLOAT || to == ColumnType.DOUBLE;
            case ColumnType.FLOAT:
                return to == ColumnType.DOUBLE;
            case ColumnType.DOUBLE:
                return to == ColumnType.FLOAT;
            case ColumnType.SHORT:
                return to == ColumnType.INT || to == ColumnType.LONG || to == ColumnType.FLOAT || to == ColumnType.DOUBLE;
            case ColumnType.BYTE:
                return to == ColumnType.SHORT || to == ColumnType.INT || to == ColumnType.LONG
                        || to == ColumnType.FLOAT || to == ColumnType.DOUBLE;
            default:
                return false;
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

    private static boolean declaredBy(Class<?> clazz, String methodName, Class<?> declaring) {
        try {
            if (methodName.startsWith("getColumnwise")) {
                return clazz.getMethod(methodName).getDeclaringClass() == declaring;
            }
            return clazz.getMethod(methodName, Record.class).getDeclaringClass() == declaring;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    private static String getterName(int type) {
        switch (type) {
            case ColumnType.INT:
                return "getInt";
            case ColumnType.LONG:
                return "getLong";
            case ColumnType.FLOAT:
                return "getFloat";
            case ColumnType.DOUBLE:
                return "getDouble";
            case ColumnType.SHORT:
                return "getShort";
            default:
                return "getByte";
        }
    }

    private static boolean isKernelDeclaredWithRowMethods(Class<?> clazz) {
        // A kernel is only trusted where it sits next to the row methods it replicates: a subclass
        // that overrides computeFirst() or computeNext() without its own kernel keeps the row path.
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
            return first == next && next == keyed && keyed == notKeyed;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    private static boolean isStorable(int type) {
        return type == ColumnType.INT || type == ColumnType.LONG || type == ColumnType.FLOAT
                || type == ColumnType.DOUBLE || type == ColumnType.SHORT || type == ColumnType.BYTE;
    }

    private void binary(Node node) {
        final int n = rowCount;
        final Node a = node.a;
        final Node b = node.b;
        switch (node.type) {
            case ColumnType.INT: {
                final int[] l = a.ints;
                final int[] r = b.ints;
                final int[] out = node.ints;
                switch (node.op) {
                    case ColumnwiseFunction.OP_ADD:
                        for (int i = 0; i < n; i++) {
                            final int x = l[i];
                            final int y = r[i];
                            out[i] = x == Numbers.INT_NULL || y == Numbers.INT_NULL ? Numbers.INT_NULL : x + y;
                        }
                        break;
                    case ColumnwiseFunction.OP_SUB:
                        for (int i = 0; i < n; i++) {
                            final int x = l[i];
                            final int y = r[i];
                            out[i] = x == Numbers.INT_NULL || y == Numbers.INT_NULL ? Numbers.INT_NULL : x - y;
                        }
                        break;
                    case ColumnwiseFunction.OP_MUL:
                        for (int i = 0; i < n; i++) {
                            final int x = l[i];
                            final int y = r[i];
                            out[i] = x == Numbers.INT_NULL || y == Numbers.INT_NULL ? Numbers.INT_NULL : x * y;
                        }
                        break;
                    default:
                        for (int i = 0; i < n; i++) {
                            final int x = l[i];
                            final int y = r[i];
                            out[i] = x == Numbers.INT_NULL || y == Numbers.INT_NULL || y == 0 ? Numbers.INT_NULL : x / y;
                        }
                        break;
                }
                break;
            }
            case ColumnType.LONG: {
                final long[] l = a.longs;
                final long[] r = b.longs;
                final long[] out = node.longs;
                switch (node.op) {
                    case ColumnwiseFunction.OP_ADD:
                        for (int i = 0; i < n; i++) {
                            final long x = l[i];
                            final long y = r[i];
                            out[i] = x == Numbers.LONG_NULL || y == Numbers.LONG_NULL ? Numbers.LONG_NULL : x + y;
                        }
                        break;
                    case ColumnwiseFunction.OP_SUB:
                        for (int i = 0; i < n; i++) {
                            final long x = l[i];
                            final long y = r[i];
                            out[i] = x == Numbers.LONG_NULL || y == Numbers.LONG_NULL ? Numbers.LONG_NULL : x - y;
                        }
                        break;
                    case ColumnwiseFunction.OP_MUL:
                        for (int i = 0; i < n; i++) {
                            final long x = l[i];
                            final long y = r[i];
                            out[i] = x == Numbers.LONG_NULL || y == Numbers.LONG_NULL ? Numbers.LONG_NULL : x * y;
                        }
                        break;
                    default:
                        for (int i = 0; i < n; i++) {
                            final long x = l[i];
                            final long y = r[i];
                            out[i] = x == Numbers.LONG_NULL || y == Numbers.LONG_NULL || y == 0 ? Numbers.LONG_NULL : x / y;
                        }
                        break;
                }
                break;
            }
            case ColumnType.FLOAT: {
                final float[] l = a.floats;
                final float[] r = b.floats;
                final float[] out = node.floats;
                switch (node.op) {
                    case ColumnwiseFunction.OP_ADD:
                        for (int i = 0; i < n; i++) {
                            out[i] = l[i] + r[i];
                        }
                        break;
                    case ColumnwiseFunction.OP_SUB:
                        for (int i = 0; i < n; i++) {
                            out[i] = l[i] - r[i];
                        }
                        break;
                    case ColumnwiseFunction.OP_MUL:
                        for (int i = 0; i < n; i++) {
                            out[i] = l[i] * r[i];
                        }
                        break;
                    default:
                        for (int i = 0; i < n; i++) {
                            final float f = l[i] / r[i];
                            out[i] = Numbers.isFinite(f) ? f : Float.NaN;
                        }
                        break;
                }
                break;
            }
            default: {
                final double[] l = a.doubles;
                final double[] r = b.doubles;
                final double[] out = node.doubles;
                switch (node.op) {
                    case ColumnwiseFunction.OP_ADD:
                        for (int i = 0; i < n; i++) {
                            out[i] = l[i] + r[i];
                        }
                        break;
                    case ColumnwiseFunction.OP_SUB:
                        for (int i = 0; i < n; i++) {
                            out[i] = l[i] - r[i];
                        }
                        break;
                    case ColumnwiseFunction.OP_MUL:
                        for (int i = 0; i < n; i++) {
                            out[i] = l[i] * r[i];
                        }
                        break;
                    default:
                        for (int i = 0; i < n; i++) {
                            final double d = l[i] / r[i];
                            out[i] = Numbers.isFinite(d) ? d : Double.NaN;
                        }
                        break;
                }
                break;
            }
        }
    }

    private void cast(Node node) {
        // The getters of CastXToYFunctionFactory, operand type X in node.op, result type Y.
        final int n = rowCount;
        final Node a = node.a;
        final int from = node.op;
        switch (node.type) {
            case ColumnType.INT: {
                final int[] out = node.ints;
                if (from == ColumnType.LONG) {
                    final long[] in = a.longs;
                    for (int i = 0; i < n; i++) {
                        final long value = in[i];
                        out[i] = value == Numbers.LONG_NULL ? Numbers.INT_NULL : (int) value;
                    }
                } else if (from == ColumnType.FLOAT) {
                    final float[] in = a.floats;
                    for (int i = 0; i < n; i++) {
                        final float value = in[i];
                        out[i] = Numbers.isNull(value) || value > Integer.MAX_VALUE || value < Integer.MIN_VALUE ? Numbers.INT_NULL : (int) value;
                    }
                } else if (from == ColumnType.DOUBLE) {
                    final double[] in = a.doubles;
                    for (int i = 0; i < n; i++) {
                        final double value = in[i];
                        out[i] = Numbers.isNull(value) || value > Integer.MAX_VALUE || value < Integer.MIN_VALUE ? Numbers.INT_NULL : (int) value;
                    }
                } else {
                    // SHORT and BYTE are held widened to int
                    System.arraycopy(a.ints, 0, out, 0, n);
                }
                break;
            }
            case ColumnType.LONG: {
                final long[] out = node.longs;
                if (from == ColumnType.INT) {
                    final int[] in = a.ints;
                    for (int i = 0; i < n; i++) {
                        out[i] = Numbers.intToLong(in[i]);
                    }
                } else if (from == ColumnType.FLOAT) {
                    final float[] in = a.floats;
                    for (int i = 0; i < n; i++) {
                        final float value = in[i];
                        out[i] = Numbers.isNull(value) || value > Long.MAX_VALUE || value < Long.MIN_VALUE ? Numbers.LONG_NULL : (long) value;
                    }
                } else if (from == ColumnType.DOUBLE) {
                    final double[] in = a.doubles;
                    for (int i = 0; i < n; i++) {
                        final double value = in[i];
                        out[i] = Numbers.isNull(value) || value > Long.MAX_VALUE || value < Long.MIN_VALUE ? Numbers.LONG_NULL : (long) value;
                    }
                } else {
                    final int[] in = a.ints;
                    for (int i = 0; i < n; i++) {
                        out[i] = in[i];
                    }
                }
                break;
            }
            case ColumnType.FLOAT: {
                final float[] out = node.floats;
                if (from == ColumnType.INT) {
                    final int[] in = a.ints;
                    for (int i = 0; i < n; i++) {
                        final int value = in[i];
                        out[i] = value != Numbers.INT_NULL ? value : Float.NaN;
                    }
                } else if (from == ColumnType.DOUBLE) {
                    final double[] in = a.doubles;
                    for (int i = 0; i < n; i++) {
                        final double value = in[i];
                        out[i] = Numbers.isNull(value) || value > Float.MAX_VALUE || value < -Float.MAX_VALUE ? Float.NaN : (float) value;
                    }
                } else {
                    final int[] in = a.ints;
                    for (int i = 0; i < n; i++) {
                        out[i] = in[i];
                    }
                }
                break;
            }
            default: {
                final double[] out = node.doubles;
                if (from == ColumnType.INT) {
                    final int[] in = a.ints;
                    for (int i = 0; i < n; i++) {
                        final int value = in[i];
                        out[i] = value != Numbers.INT_NULL ? value : Double.NaN;
                    }
                } else if (from == ColumnType.FLOAT) {
                    final float[] in = a.floats;
                    for (int i = 0; i < n; i++) {
                        out[i] = in[i];
                    }
                } else {
                    final int[] in = a.ints;
                    for (int i = 0; i < n; i++) {
                        out[i] = in[i];
                    }
                }
                break;
            }
        }
    }

    private boolean column(Node node) {
        final long addr = record.getPageAddress(node.columnIndex);
        if (addr == 0) {
            // A column top or a converted column: PageFrameMemoryRecord reads these through its
            // typed getters, so the aggregate takes the row path for this batch.
            return false;
        }
        // The load and, when the parent reads the column with another type's getter, the base
        // class's conversion, in one pass. Row i of the batch is rowIndex(...) in every mode.
        final int n = rowCount;
        final int mode = this.mode;
        final long lo = this.lo;
        final long rowsAddr = this.rowsAddr;
        switch (node.type) {
            case ColumnType.DOUBLE: {
                final double[] out = node.doubles;
                switch (node.op) {
                    case ColumnType.DOUBLE:
                        for (int i = 0; i < n; i++) {
                            out[i] = Unsafe.getDouble(addr + (rowIndex(mode, lo, rowsAddr, i) << 3));
                        }
                        break;
                    case ColumnType.FLOAT:
                        // FloatFunction.getDouble()
                        for (int i = 0; i < n; i++) {
                            out[i] = Unsafe.getFloat(addr + (rowIndex(mode, lo, rowsAddr, i) << 2));
                        }
                        break;
                    case ColumnType.INT:
                        // IntFunction.getDouble()
                        for (int i = 0; i < n; i++) {
                            out[i] = Numbers.intToDouble(Unsafe.getInt(addr + (rowIndex(mode, lo, rowsAddr, i) << 2)));
                        }
                        break;
                    case ColumnType.LONG:
                        // LongFunction.getDouble()
                        for (int i = 0; i < n; i++) {
                            final long value = Unsafe.getLong(addr + (rowIndex(mode, lo, rowsAddr, i) << 3));
                            out[i] = value != Numbers.LONG_NULL ? value : Double.NaN;
                        }
                        break;
                    case ColumnType.SHORT:
                        for (int i = 0; i < n; i++) {
                            out[i] = Unsafe.getShort(addr + (rowIndex(mode, lo, rowsAddr, i) << 1));
                        }
                        break;
                    default:
                        for (int i = 0; i < n; i++) {
                            out[i] = Unsafe.getByte(addr + rowIndex(mode, lo, rowsAddr, i));
                        }
                        break;
                }
                break;
            }
            case ColumnType.FLOAT: {
                final float[] out = node.floats;
                switch (node.op) {
                    case ColumnType.FLOAT:
                        for (int i = 0; i < n; i++) {
                            out[i] = Unsafe.getFloat(addr + (rowIndex(mode, lo, rowsAddr, i) << 2));
                        }
                        break;
                    case ColumnType.INT:
                        // IntFunction.getFloat()
                        for (int i = 0; i < n; i++) {
                            out[i] = Numbers.intToFloat(Unsafe.getInt(addr + (rowIndex(mode, lo, rowsAddr, i) << 2)));
                        }
                        break;
                    case ColumnType.LONG:
                        // LongFunction.getFloat()
                        for (int i = 0; i < n; i++) {
                            out[i] = Numbers.longToFloat(Unsafe.getLong(addr + (rowIndex(mode, lo, rowsAddr, i) << 3)));
                        }
                        break;
                    case ColumnType.DOUBLE:
                        // DoubleFunction.getFloat()
                        for (int i = 0; i < n; i++) {
                            out[i] = (float) Unsafe.getDouble(addr + (rowIndex(mode, lo, rowsAddr, i) << 3));
                        }
                        break;
                    case ColumnType.SHORT:
                        for (int i = 0; i < n; i++) {
                            out[i] = Unsafe.getShort(addr + (rowIndex(mode, lo, rowsAddr, i) << 1));
                        }
                        break;
                    default:
                        for (int i = 0; i < n; i++) {
                            out[i] = Unsafe.getByte(addr + rowIndex(mode, lo, rowsAddr, i));
                        }
                        break;
                }
                break;
            }
            case ColumnType.LONG: {
                final long[] out = node.longs;
                switch (node.op) {
                    case ColumnType.LONG:
                        for (int i = 0; i < n; i++) {
                            out[i] = Unsafe.getLong(addr + (rowIndex(mode, lo, rowsAddr, i) << 3));
                        }
                        break;
                    case ColumnType.INT:
                        // IntFunction.getLong()
                        for (int i = 0; i < n; i++) {
                            out[i] = Numbers.intToLong(Unsafe.getInt(addr + (rowIndex(mode, lo, rowsAddr, i) << 2)));
                        }
                        break;
                    case ColumnType.SHORT:
                        for (int i = 0; i < n; i++) {
                            out[i] = Unsafe.getShort(addr + (rowIndex(mode, lo, rowsAddr, i) << 1));
                        }
                        break;
                    default:
                        for (int i = 0; i < n; i++) {
                            out[i] = Unsafe.getByte(addr + rowIndex(mode, lo, rowsAddr, i));
                        }
                        break;
                }
                break;
            }
            default: {
                // INT, SHORT and BYTE values, held widened to int
                final int[] out = node.ints;
                switch (node.op) {
                    case ColumnType.INT:
                        for (int i = 0; i < n; i++) {
                            out[i] = Unsafe.getInt(addr + (rowIndex(mode, lo, rowsAddr, i) << 2));
                        }
                        break;
                    case ColumnType.SHORT:
                        for (int i = 0; i < n; i++) {
                            out[i] = Unsafe.getShort(addr + (rowIndex(mode, lo, rowsAddr, i) << 1));
                        }
                        break;
                    default:
                        for (int i = 0; i < n; i++) {
                            out[i] = Unsafe.getByte(addr + rowIndex(mode, lo, rowsAddr, i));
                        }
                        break;
                }
                break;
            }
        }
        return true;
    }

    private void constant(Node node) {
        final int n = rowCount;
        final Function f = node.constant;
        switch (node.type) {
            case ColumnType.INT: {
                final int value = f.getInt(record);
                final int[] out = node.ints;
                for (int i = 0; i < n; i++) {
                    out[i] = value;
                }
                break;
            }
            case ColumnType.LONG: {
                final long value = f.getLong(record);
                final long[] out = node.longs;
                for (int i = 0; i < n; i++) {
                    out[i] = value;
                }
                break;
            }
            case ColumnType.FLOAT: {
                final float value = f.getFloat(record);
                final float[] out = node.floats;
                for (int i = 0; i < n; i++) {
                    out[i] = value;
                }
                break;
            }
            case ColumnType.DOUBLE: {
                final double value = f.getDouble(record);
                final double[] out = node.doubles;
                for (int i = 0; i < n; i++) {
                    out[i] = value;
                }
                break;
            }
            case ColumnType.SHORT: {
                final int value = f.getShort(record);
                final int[] out = node.ints;
                for (int i = 0; i < n; i++) {
                    out[i] = value;
                }
                break;
            }
            default: {
                final int value = f.getByte(record);
                final int[] out = node.ints;
                for (int i = 0; i < n; i++) {
                    out[i] = value;
                }
                break;
            }
        }
    }

    private void convert(Node node) {
        // The getters the standard base classes define for types other than their own, operand
        // type in node.op, result type node.type.
        final int n = rowCount;
        final Node a = node.a;
        final int from = node.op;
        switch (node.type) {
            case ColumnType.LONG: {
                final long[] out = node.longs;
                final int[] in = a.ints;
                if (from == ColumnType.INT) {
                    for (int i = 0; i < n; i++) {
                        out[i] = Numbers.intToLong(in[i]);
                    }
                } else {
                    for (int i = 0; i < n; i++) {
                        out[i] = in[i];
                    }
                }
                break;
            }
            case ColumnType.FLOAT: {
                final float[] out = node.floats;
                if (from == ColumnType.INT) {
                    final int[] in = a.ints;
                    for (int i = 0; i < n; i++) {
                        out[i] = Numbers.intToFloat(in[i]);
                    }
                } else if (from == ColumnType.LONG) {
                    final long[] in = a.longs;
                    for (int i = 0; i < n; i++) {
                        out[i] = Numbers.longToFloat(in[i]);
                    }
                } else if (from == ColumnType.DOUBLE) {
                    final double[] in = a.doubles;
                    for (int i = 0; i < n; i++) {
                        out[i] = (float) in[i];
                    }
                } else {
                    final int[] in = a.ints;
                    for (int i = 0; i < n; i++) {
                        out[i] = in[i];
                    }
                }
                break;
            }
            case ColumnType.DOUBLE: {
                final double[] out = node.doubles;
                if (from == ColumnType.INT) {
                    final int[] in = a.ints;
                    for (int i = 0; i < n; i++) {
                        out[i] = Numbers.intToDouble(in[i]);
                    }
                } else if (from == ColumnType.LONG) {
                    final long[] in = a.longs;
                    for (int i = 0; i < n; i++) {
                        final long value = in[i];
                        out[i] = value != Numbers.LONG_NULL ? value : Double.NaN;
                    }
                } else if (from == ColumnType.FLOAT) {
                    final float[] in = a.floats;
                    for (int i = 0; i < n; i++) {
                        out[i] = in[i];
                    }
                } else {
                    final int[] in = a.ints;
                    for (int i = 0; i < n; i++) {
                        out[i] = in[i];
                    }
                }
                break;
            }
            default: {
                // INT or SHORT from SHORT or BYTE: held widened to int already
                System.arraycopy(a.ints, 0, node.ints, 0, n);
                break;
            }
        }
    }

    private boolean evaluate(Node node) {
        if (node.epoch == epoch) {
            return node.ok;
        }
        node.epoch = epoch;
        node.ok = false;
        node.ensureCapacity(capacity);
        switch (node.kind) {
            case KIND_COLUMN:
                if (!column(node)) {
                    return false;
                }
                break;
            case KIND_CONSTANT:
                constant(node);
                break;
            case KIND_CONVERT:
                if (!evaluate(node.a)) {
                    return false;
                }
                convert(node);
                break;
            case KIND_CAST:
                if (!evaluate(node.a)) {
                    return false;
                }
                cast(node);
                break;
            default:
                if (!evaluate(node.a) || !evaluate(node.b)) {
                    return false;
                }
                binary(node);
                break;
        }
        node.ok = true;
        return true;
    }

    // Frame row index of batch row i: contiguous rows, packed keyed-batch entries, or row indexes.
    private static long rowIndex(int mode, long lo, long rowsAddr, int i) {
        if (mode == MODE_RANGE) {
            return lo + i;
        }
        final long value = Unsafe.getLong(rowsAddr + ((long) i << 3));
        return mode == MODE_PACKED ? Map.decodeBatchRowIndex(value) : value;
    }

    /**
     * The evaluated arguments of one aggregate for the current batch: value {@code i} of argument
     * {@code k} is element {@code i} of the array for the argument's getter type.
     */
    public static final class Args {
        private final Node[] nodes;

        private Args(Node[] nodes) {
            this.nodes = nodes;
        }

        public double[] doubles(int argIndex) {
            return nodes[argIndex].doubles;
        }

        public float[] floats(int argIndex) {
            return nodes[argIndex].floats;
        }

        public int[] ints(int argIndex) {
            return nodes[argIndex].ints;
        }

        public long[] longs(int argIndex) {
            return nodes[argIndex].longs;
        }
    }

    private static final class Node {
        private final Node a;
        private final Node b;
        private final int columnIndex;
        private final Function constant;
        private final int kind;
        private final int op;
        private final int type;
        private double[] doubles;
        private long epoch = -1;
        private float[] floats;
        private int[] ints;
        private String key;
        private long[] longs;
        private boolean ok;

        private Node(int kind, int type, int op, int columnIndex, Function constant, Node a, Node b) {
            this.kind = kind;
            this.type = type;
            this.op = op;
            this.columnIndex = columnIndex;
            this.constant = constant;
            this.a = a;
            this.b = b;
            if (kind == KIND_CONSTANT) {
                // constants are not shared between aggregates
                this.key = "k" + System.identityHashCode(this);
            }
        }

        private void ensureCapacity(int capacity) {
            switch (type) {
                case ColumnType.LONG:
                    if (longs == null) {
                        longs = new long[capacity];
                    }
                    break;
                case ColumnType.FLOAT:
                    if (floats == null) {
                        floats = new float[capacity];
                    }
                    break;
                case ColumnType.DOUBLE:
                    if (doubles == null) {
                        doubles = new double[capacity];
                    }
                    break;
                default:
                    if (ints == null) {
                        ints = new int[capacity];
                    }
                    break;
            }
        }
    }
}

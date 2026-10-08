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
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;

/**
 * How {@link AsyncWindowRecordCursor} may split one key of the scan into several tasks, decided
 * at plan time from the window's functions. Without a split, a key is computed whole by one task,
 * or by the query's own thread when it is larger than a task may be.
 * <ul>
 *     <li>{@link #MODE_WARMUP}: every function's value depends only on the rows of a bounded ROWS
 *     frame that ends at or before the current row, or on the last k rows ({@code lag}). A task
 *     that continues a key starts {@link #getWarmupRows()} rows early, which rebuilds the frame,
 *     and returns only its own rows.</li>
 *     <li>{@link #MODE_WARMUP} with prefix columns, for a chain of windows (see
 *     {@link AsyncWindowChainSplit}): warm-up rows rebuild the stages before a last window of
 *     running aggregates, which a task computes from scratch over its own rows and the query's
 *     thread combines as in {@link #MODE_PREFIX}.</li>
 *     <li>{@link #MODE_PREFIX}: every function is a running aggregate from UNBOUNDED PRECEDING to
 *     the current row (sum, count, first_value; min and max of integers only) or
 *     {@code row_number}. A task that
 *     continues a key computes it from scratch, and the query's thread combines each of its rows
 *     with the value the key had at the end of the previous task before returning them.</li>
 * </ul>
 * Results equal the serial window's bit for bit, with one exception: a running DOUBLE sum under
 * PARTITION BY ({@link #OP_ADD}) whose key a split spreads over tasks may differ in its last bits,
 * since the carry adds the same values in a different order, as parallel GROUP BY does. A sum of
 * whole numbers is exact either way, and the planner proves which are (see
 * {@code SqlCodeGenerator.isNonNegativeValue}). Everything else is exact by construction:
 * <ul>
 *     <li>integer, count, min, max, first, last, lag and row_number results, which do not depend
 *     on the order of additions;</li>
 *     <li>a running DOUBLE sum of a single key, which the query's thread folds in order
 *     ({@link #OP_FOLD});</li>
 *     <li>a bounded frame's DOUBLE {@code avg} or {@code sum} of a single key, which the query's
 *     thread replays with the serial function itself ({@link #OP_REPLAY}): the serial function's
 *     running sum carries the rounding of the key's whole history, which warm-up rows cannot
 *     rebuild. Over several keys such a frame keeps its keys whole, unless its argument is a
 *     whole number.</li>
 * </ul>
 */
public class AsyncWindowSplitPlan implements Plannable {
    public static final int MODE_NONE = 0;
    public static final int MODE_PREFIX = 2;
    public static final int MODE_WARMUP = 1;
    public static final AsyncWindowSplitPlan NONE = new AsyncWindowSplitPlan(MODE_NONE, 0, new IntList(), new IntList(), new IntList());
    public static final int OP_ADD = 0;
    public static final int OP_FIRST = 3;
    /**
     * A running DOUBLE sum of a single key whose workers output each row's argument (see
     * {@link AsyncWindowFoldEcho}) and whose query thread folds the sum over them, in order: exact.
     */
    public static final int OP_FOLD = 4;
    public static final int OP_MAX = 2;
    public static final int OP_MIN = 1;
    /**
     * A bounded frame's DOUBLE avg or sum of a single key (see
     * {@link io.questdb.griffin.engine.functions.window.ReplayableWindowFunction}) whose workers
     * output each row's argument (see {@link AsyncWindowFoldEcho}) and whose query thread computes
     * the frame over them, in order, with its own copy of the function, which also computed the
     * rows before them: exact. Needs no warm-up rows.
     */
    public static final int OP_REPLAY = 5;
    private final int mode;
    // MODE_PREFIX: the output columns to combine, how, and their column types
    private final IntList prefixColumns;
    private final IntList prefixOps;
    private final IntList prefixTypes;
    private final long warmupRows;
    // with a GROUP BY step: the carried group key's index among the step's keys, and its column in
    // the step's input; -1 without a carried group key
    private int groupCarryInputColumn = -1;
    private int groupCarryKeyIndex = -1;

    public AsyncWindowSplitPlan(int mode, long warmupRows, IntList prefixColumns, IntList prefixOps, IntList prefixTypes) {
        this.mode = mode;
        this.warmupRows = warmupRows;
        this.prefixColumns = prefixColumns;
        this.prefixOps = prefixOps;
        this.prefixTypes = prefixTypes;
    }

    /**
     * Combines the value a running aggregate had at the end of the previous part of its key with
     * a value computed from scratch over the next part, as raw bits: a DOUBLE as its long bits,
     * an INT widened. A NULL on either side yields the other.
     */
    public static long combine(int op, int columnType, long carry, long local) {
        assert op != OP_FOLD && op != OP_REPLAY : "a fold is not combined";
        if (op == OP_FIRST) {
            return carry;
        }
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.DOUBLE: {
                final double c = Double.longBitsToDouble(carry);
                final double l = Double.longBitsToDouble(local);
                // NULL is NaN alone: a sum that overflowed to an infinity is a value
                if (Double.isNaN(c)) {
                    return local;
                }
                if (Double.isNaN(l)) {
                    return carry;
                }
                // a DOUBLE is only ever added: its min and max are not split, see the planner
                assert op == OP_ADD;
                // A running sum skips infinite arguments, so one that overflowed stays where it
                // went: serially, adding the next part's finite values leaves it there.
                if (Double.isInfinite(c)) {
                    return carry;
                }
                final double result = c + l;
                return Double.doubleToRawLongBits(result);
            }
            case ColumnType.LONG: {
                if (carry == Numbers.LONG_NULL) {
                    return local;
                }
                if (local == Numbers.LONG_NULL) {
                    return carry;
                }
                return switch (op) {
                    case OP_ADD -> carry + local;
                    case OP_MIN -> Math.min(carry, local);
                    default -> Math.max(carry, local);
                };
            }
            default: {
                // INT, for min and max only
                final int c = (int) carry;
                final int l = (int) local;
                if (c == Numbers.INT_NULL) {
                    return local;
                }
                if (l == Numbers.INT_NULL) {
                    return carry;
                }
                return op == OP_MIN ? Math.min(c, l) : Math.max(c, l);
            }
        }
    }

    /**
     * Whether an op is computed by the query's thread over the workers' stand-ins, see
     * {@link AsyncWindowFoldEcho}, rather than combined with a carry.
     */
    public static boolean isFold(int op) {
        return op == OP_FOLD || op == OP_REPLAY;
    }

    /**
     * Whether a column is folded or replayed, see {@link #OP_FOLD} and {@link #OP_REPLAY}: every
     * task's rows need the query thread's pass, also of a key the task starts, and the workers'
     * copies of the column output its stand-in, which no step may read.
     */
    public boolean hasFold() {
        for (int i = 0, n = prefixOps.size(); i < n; i++) {
            if (isFold(prefixOps.getQuick(i))) {
                return true;
            }
        }
        return false;
    }

    private boolean hasOp(int op) {
        for (int i = 0, n = prefixOps.size(); i < n; i++) {
            if (prefixOps.getQuick(i) == op) {
                return true;
            }
        }
        return false;
    }

    // whether a column combines a running value, folded or carried
    private boolean hasRunningCarry() {
        for (int i = 0, n = prefixOps.size(); i < n; i++) {
            if (prefixOps.getQuick(i) != OP_REPLAY) {
                return true;
            }
        }
        return false;
    }

    public int getGroupCarryInputColumn() {
        return groupCarryInputColumn;
    }

    public int getGroupCarryKeyIndex() {
        return groupCarryKeyIndex;
    }

    public int getMode() {
        return mode;
    }

    /**
     * With a GROUP BY step whose group key is the carried running value: its index among the
     * step's keys, and its column in the step's input, whose captured rows take the carry too.
     */
    public void setGroupCarry(int keyIndex, int inputColumn) {
        this.groupCarryKeyIndex = keyIndex;
        this.groupCarryInputColumn = inputColumn;
    }

    public int getPrefixColumn(int i) {
        return prefixColumns.getQuick(i);
    }

    public int getPrefixCount() {
        return prefixColumns.size();
    }

    public int getPrefixOp(int i) {
        return prefixOps.getQuick(i);
    }

    public int getPrefixType(int i) {
        return prefixTypes.getQuick(i);
    }

    public long getWarmupRows() {
        return warmupRows;
    }

    @Override
    public void toPlan(PlanSink sink) {
        switch (mode) {
            case MODE_WARMUP -> {
                sink.val("warmup ").val(warmupRows).val(" rows");
                if (hasRunningCarry()) {
                    sink.val(hasOp(OP_FOLD) ? ", running carry, folded" : ", running carry");
                }
                if (hasOp(OP_REPLAY)) {
                    sink.val(", frame replayed");
                }
            }
            case MODE_PREFIX -> {
                if (hasRunningCarry()) {
                    sink.val(hasOp(OP_FOLD) ? "running carry, folded" : "running carry");
                    if (hasOp(OP_REPLAY)) {
                        sink.val(", frame replayed");
                    }
                } else {
                    sink.val("frame replayed");
                }
            }
            default -> sink.val("none");
        }
    }
}

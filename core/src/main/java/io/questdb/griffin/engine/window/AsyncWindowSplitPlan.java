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
import org.jetbrains.annotations.TestOnly;

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
 * Results equal the serial window's bit for bit, by construction:
 * <ul>
 *     <li>integer, count, min, max, first, last, lag and row_number results, which do not depend
 *     on the order of additions: a LONG carry adds in two's complement, which wraps as the serial
 *     sum does;</li>
 *     <li>a running DOUBLE sum, which the query's thread folds in walk order, key by key
 *     ({@link #OP_FOLD}), unless its values are whole numbers of a known bound: a carry adds
 *     those exactly ({@link #OP_ADD}) while no sum passes {@link #EXACT_DOUBLE_MAGNITUDE}, which
 *     the cursor checks against the walk's rows before it splits a key, see
 *     {@link #getExactRowLimit()};</li>
 *     <li>a bounded frame's DOUBLE {@code avg} or {@code sum}, which the query's thread replays
 *     with the serial function's own arithmetic ({@link #OP_REPLAY}): the serial function's
 *     running sum carries the rounding of the key's whole history, which warm-up rows cannot
 *     rebuild. A frame of whole numbers whose sums stay within {@link #EXACT_DOUBLE_MAGNITUDE},
 *     by their bound and the frame's rows, is rebuilt from warm-up rows, exactly.</li>
 * </ul>
 */
public class AsyncWindowSplitPlan implements Plannable {
    /**
     * The magnitude up to which a DOUBLE holds every integer: a sum of whole numbers is exact, in
     * any order, while every partial sum stays within it.
     */
    public static final long EXACT_DOUBLE_MAGNITUDE = 1L << 53;
    /**
     * The largest magnitude of the whole numbers a running DOUBLE sum may carry ({@link #OP_ADD})
     * rather than fold: a walk of up to 2^32 rows keeps such a sum within
     * {@link #EXACT_DOUBLE_MAGNITUDE}. A sum of larger values is folded, whose order is the
     * serial one.
     */
    public static final long MAX_CARRIED_WHOLE_BOUND = 1L << 21;
    public static final int MODE_NONE = 0;
    public static final int MODE_PREFIX = 2;
    public static final int MODE_WARMUP = 1;
    public static final AsyncWindowSplitPlan NONE = new AsyncWindowSplitPlan(MODE_NONE, 0, new IntList(), new IntList(), new IntList());
    public static final int OP_ADD = 0;
    public static final int OP_FIRST = 3;
    /**
     * A running DOUBLE sum whose workers output each row's argument (see
     * {@link AsyncWindowFoldEcho}) and whose query thread folds the sum over them, in walk order,
     * from the key's sum before the task for a key the task continues and from scratch at each
     * key the task starts: the serial additions in the serial order, exact, and an overflow
     * where the serial sum overflows and nowhere else.
     */
    public static final int OP_FOLD = 4;
    public static final int OP_MAX = 2;
    public static final int OP_MIN = 1;
    /**
     * A bounded frame's DOUBLE avg or sum (see
     * {@link io.questdb.griffin.engine.functions.window.ReplayableWindowFunction}) whose workers
     * output each row's argument (see {@link AsyncWindowFoldEcho}) and whose query thread computes
     * the frame over every row, in walk order, with its own copy of the function, which also took
     * the rows it computed itself before the tasks: the serial operations in the serial order,
     * exact. Starts afresh at each key a task starts; needs no warm-up rows.
     */
    public static final int OP_REPLAY = 5;
    private static long exactMagnitude = EXACT_DOUBLE_MAGNITUDE;
    private final int mode;
    // MODE_PREFIX: the output columns to combine, how, and their column types
    private final IntList prefixColumns;
    private final IntList prefixOps;
    private final IntList prefixTypes;
    private final long warmupRows;
    // OP_ADD of whole DOUBLE values: the most rows a walk may have for every sum to stay exact
    private long exactRowLimit = Long.MAX_VALUE;
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
                // A DOUBLE is only ever added, and only a sum of whole numbers that stays within
                // EXACT_DOUBLE_MAGNITUDE, see getExactRowLimit(): its min and max are not split,
                // and other sums are folded, see the planner.
                assert op == OP_ADD;
                // never reached by such a sum; kept as the serial sum behaves: it skips infinite
                // arguments, so one that overflowed stays where it went
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
     * The most rows a walk may have for a carried running sum of whole numbers of magnitude up to
     * {@code bound} to stay exact: {@link #EXACT_DOUBLE_MAGNITUDE} over the bound.
     */
    public static long exactRowLimit(long bound) {
        assert bound > -1;
        return bound == 0 ? Long.MAX_VALUE : exactMagnitude / bound;
    }

    /**
     * Whether an op is computed by the query's thread over the workers' stand-ins, see
     * {@link AsyncWindowFoldEcho}, rather than combined with a carry.
     */
    public static boolean isFold(int op) {
        return op == OP_FOLD || op == OP_REPLAY;
    }

    /**
     * Whether a key that continues in a task may be rebuilt from {@code warmupRows} rows before
     * the task's first own row: they come from the previous task, which holds about
     * {@code taskRows} rows, and must stay under half of them. Overflow-safe for any count.
     */
    public static boolean isWarmupWithinTask(long warmupRows, long taskRows) {
        // warmupRows * 2 < taskRows, for taskRows >= 1
        return warmupRows <= (taskRows - 1) / 2;
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

    /**
     * Whether the column is folded or replayed, see {@link #isFold}.
     */
    public boolean isFolded(int column) {
        for (int i = 0, n = prefixOps.size(); i < n; i++) {
            if (prefixColumns.getQuick(i) == column && isFold(prefixOps.getQuick(i))) {
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

    /**
     * Lowers the magnitude {@link #exactRowLimit(long)} divides, so that a test reaches the limit
     * with a few thousand rows; {@link #EXACT_DOUBLE_MAGNITUDE} restores it.
     */
    @TestOnly
    public static void setExactMagnitude(long magnitude) {
        exactMagnitude = magnitude;
    }

    /**
     * The most rows the walk may have for this plan's carried DOUBLE sums ({@link #OP_ADD} of
     * whole numbers) to be exact, {@code Long.MAX_VALUE} without such a sum. The cursor runs a
     * walk over more rows serially, see {@code AsyncWindowRecordCursor}.
     */
    public long getExactRowLimit() {
        return exactRowLimit;
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
    public void setExactRowLimit(long exactRowLimit) {
        this.exactRowLimit = exactRowLimit;
    }

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

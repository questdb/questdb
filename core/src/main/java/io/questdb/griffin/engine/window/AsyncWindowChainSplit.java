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

import io.questdb.std.IntList;

/**
 * Whether, and how, an Async Window with steps after its window functions (see
 * {@link AsyncWindowStage}) may still split a key over several tasks, folded stage by stage as the
 * planner appends them. Each window stage on its own allows warm-up rows, a running carry, or no
 * split, as {@link AsyncWindowSplitPlan} describes for one window. Along the chain:
 * <ul>
 *     <li>warm-up rows add up: a window over k rows of a window over j rows needs the j + k rows
 *     before the task's first one;</li>
 *     <li>a carry must come last, so that its running values reach the output as they are and the
 *     cursor can combine them there; warm-up rows before it rebuild the stages before it, and its
 *     own window starts afresh at the task's first own row;</li>
 *     <li>a projection keeps whatever the stages before it allow, but nothing may follow a carry;
 *     a filter keeps warm-up rows, but stops a carry, which would be taken from the last row
 *     output instead of the last row computed.</li>
 * </ul>
 */
public class AsyncWindowChainSplit {
    private final IntList carryColumns = new IntList();
    private final IntList carryOps = new IntList();
    private final IntList carryTypes = new IntList();
    // the stage, by index among the steps after the window, whose windows carry; -1 when the
    // window's own functions carry, -2 when nothing does
    private int carryStage = -2;
    private boolean unsplittable;
    // rows before a continued key's first own row that rebuild the stages, -1 when none are needed
    private long warmupRows = -1;

    public static AsyncWindowChainSplit of(AsyncWindowSplitPlan plan) {
        final AsyncWindowChainSplit split = new AsyncWindowChainSplit();
        switch (plan.getMode()) {
            case AsyncWindowSplitPlan.MODE_WARMUP -> split.warmupRows = plan.getWarmupRows();
            case AsyncWindowSplitPlan.MODE_PREFIX -> {
                split.carryStage = -1;
                for (int i = 0, n = plan.getPrefixCount(); i < n; i++) {
                    split.carryColumns.add(plan.getPrefixColumn(i));
                    split.carryOps.add(plan.getPrefixOp(i));
                    split.carryTypes.add(plan.getPrefixType(i));
                }
            }
            default -> split.unsplittable = true;
        }
        return split;
    }

    public int getCarryStage() {
        return carryStage >= 0 ? carryStage : -1;
    }

    /**
     * The split after a filter stage. Warm-up rows still rebuild the stages, and a filter only
     * decides which rows are output; a carry, which the cursor takes from the last row output,
     * no longer splits.
     */
    public AsyncWindowChainSplit thenFilter() {
        final AsyncWindowChainSplit next = copy();
        if (carryStage > -2) {
            next.unsplittable = true;
        }
        return next;
    }

    /**
     * The split after a GROUP BY step: none. A key split over tasks would split a group too, and
     * its aggregates are not combined.
     */
    public AsyncWindowChainSplit thenGroupBy() {
        final AsyncWindowChainSplit next = copy();
        next.unsplittable = true;
        return next;
    }

    /**
     * The split after a projection.
     */
    public AsyncWindowChainSplit thenProjection() {
        final AsyncWindowChainSplit next = copy();
        if (carryStage > -2) {
            // the carry is combined in the output, which must be the carrying window's
            next.unsplittable = true;
        }
        return next;
    }

    /**
     * The split after a window stage, whose own functions allow {@code plan} on their own.
     *
     * @param stage the index of the stage among the steps after the window
     */
    public AsyncWindowChainSplit thenWindow(AsyncWindowSplitPlan plan, int stage) {
        final AsyncWindowChainSplit next = copy();
        if (carryStage > -2) {
            next.unsplittable = true;
            return next;
        }
        switch (plan.getMode()) {
            case AsyncWindowSplitPlan.MODE_WARMUP -> next.warmupRows = Math.max(0, warmupRows) + plan.getWarmupRows();
            case AsyncWindowSplitPlan.MODE_PREFIX -> {
                next.carryStage = stage;
                for (int i = 0, n = plan.getPrefixCount(); i < n; i++) {
                    next.carryColumns.add(plan.getPrefixColumn(i));
                    next.carryOps.add(plan.getPrefixOp(i));
                    next.carryTypes.add(plan.getPrefixType(i));
                }
            }
            default -> next.unsplittable = true;
        }
        return next;
    }

    /**
     * The plan the cursor follows, with {@code taskRows} rows a task: as for one window, warm-up
     * rows must stay under half a task.
     */
    public AsyncWindowSplitPlan toPlan(long taskRows) {
        if (unsplittable) {
            return AsyncWindowSplitPlan.NONE;
        }
        if (warmupRows > -1) {
            if (warmupRows * 2 >= taskRows) {
                return AsyncWindowSplitPlan.NONE;
            }
            // a carry of the window's own functions has no warm-up rows before it
            assert carryStage != -1;
            return new AsyncWindowSplitPlan(AsyncWindowSplitPlan.MODE_WARMUP, warmupRows, copyOf(carryColumns), copyOf(carryOps), copyOf(carryTypes));
        }
        if (carryStage > -2) {
            return new AsyncWindowSplitPlan(AsyncWindowSplitPlan.MODE_PREFIX, 0, copyOf(carryColumns), copyOf(carryOps), copyOf(carryTypes));
        }
        return AsyncWindowSplitPlan.NONE;
    }

    private static IntList copyOf(IntList list) {
        final IntList copy = new IntList(list.size());
        copy.addAll(list);
        return copy;
    }

    private AsyncWindowChainSplit copy() {
        final AsyncWindowChainSplit copy = new AsyncWindowChainSplit();
        copy.carryColumns.addAll(carryColumns);
        copy.carryOps.addAll(carryOps);
        copy.carryTypes.addAll(carryTypes);
        copy.carryStage = carryStage;
        copy.unsplittable = unsplittable;
        copy.warmupRows = warmupRows;
        return copy;
    }
}

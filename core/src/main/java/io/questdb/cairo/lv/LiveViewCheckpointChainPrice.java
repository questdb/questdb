/*******************************************************************************
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

package io.questdb.cairo.lv;

import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.BoolList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * What a chained checkpoint repair pays to keep its chain, checked boundary by boundary as
 * the replay reaches each one, before it is frozen. Two repairs chain: the out-of-order
 * resume, whose chain stands on the root it restored, and the end-of-table head miss, whose
 * chain stands on nothing.
 * <p>
 * A chained boundary freezes incrementally only the state a seal keeps dirty sets for: the
 * ANCHOR window's root, its durable projections and the functions it dispatches to that track
 * their keys. Every other stateful function - W - images all its live keys at every boundary:
 * one outside the ANCHOR window, and anchored lag, rank, dense_rank, corr/covar*, EXCLUDE
 * CURRENT ROW calls and row_number past the window's leaf budget. What W images at boundary i
 * is the live maps there, K_i, which the late commit and any apply-ahead commit can grow past
 * the frontier's K_front wherever their keys land, so the price is read where the freeze
 * cursor is about to freeze boundary i: every row at or below it folded, none above it.
 * <p>
 * Boundary i of R is refused when {@code max(K_front, K_i) x (R - i) > B - S_i}: S_i is what
 * the boundaries admitted so far imaged, and B the budget, the larger of the base rows the
 * repair's own replay is estimated to read and
 * {@code cairo.live.view.checkpoint.repair.scan.max.keys}. Admitting boundary i needs
 * {@code S_i + (R - i) x M_i <= B} and adds {@code K_i <= M_i}, so S never passes B: a kept
 * chain images at most B keys whole, whatever keys the change brings and wherever they land,
 * and a refused one has imaged at most B before it stops.
 * <p>
 * The two repairs differ in what they pass:
 * <ul>
 *     <li>the resume passes the rows its plan estimated above the anchor, and binds before
 *     its restore clears the maps, so K_front is the frontier's W keys - a floor for every
 *     boundary of a replay that restored the anchor, which holds every key live there;</li>
 *     <li>the head miss passes the rows it estimates itself over {@code [L, +inf)} of its
 *     pinned snapshot, and binds after its wipe, so K_front is 0: its replay rebuilds from
 *     L, and boundary i holds the keys seen in {@code [L, B_i]} and nothing else.</li>
 * </ul>
 * <p>
 * The resume uses its worker's instance and never parks; a head miss may park across turns
 * while its worker prices other repairs, so its instance lives in its repair session. Nothing
 * allocates after the first use.
 */
public final class LiveViewCheckpointChainPrice implements BoundaryFreezingCursor.FreezeBudget {
    // Per W member: whether its walk reads the anchor map (a fused runtime-only member, whose
    // own map the window owns and keeps closed) rather than its own.
    private final BoolList anchorMapWalks = new BoolList();
    private final ObjList<WindowFunction> wholeImagedFunctions = new ObjList<>();
    private LiveViewWindow anchorWindow;
    // K_i of the boundary the last isAffordable() call priced.
    private long boundaryKeys;
    private long budgetKeys;
    private long frontierKeys;
    // S: the keys the boundaries admitted so far image whole.
    private long imagedKeys;
    // The base rows the repair's replay is estimated to read, B's first term.
    private long replayScanRows;

    /**
     * Whether a seal - a cadence seal or a chained boundary - walks the whole state of
     * {@code function}, a function with {@code supportsCheckpointState()}, in the steady state
     * where the predecessor holds its root and the runtime adopted that root as its baseline.
     * The same three branches as {@code LiveViewCheckpointTimelineStoreWriter.freezeBoundary},
     * in its order and with its gates: a durable projection rides the window root, a
     * runtime-only member of a fused group walks the group's map, and everything else is walked
     * by {@code freezeFunction}.
     *
     * @param anchorWindow the view's ANCHOR window, or null
     * @param anchorable   the functions the ANCHOR window dispatches to, or null
     */
    public static boolean isCheckpointImagedWhole(
            @Nullable LiveViewWindow anchorWindow,
            @Nullable ObjList<WindowFunction> anchorable,
            WindowFunction function
    ) {
        if (anchorWindow != null) {
            final LiveViewWindowStatePlan plan = anchorWindow.getCheckpointStoragePlan();
            // Every anchored window has one, whatever fusion says: freezeBoundary refuses to seal
            // a window without it, and CREATE refuses such a view first.
            assert plan != null : "anchored live view window has no checkpoint storage plan";
            final int projectionIndex = plan.indexOfProjectionFunction(function);
            if (projectionIndex >= 0) {
                if (plan.isDurableProjection(projectionIndex)) {
                    // The window root holds its state.
                    return false;
                }
                if (anchorWindow.isWindowStateFused()) {
                    // The member walk needs the function's own baseline beside the window's.
                    return !function.isCheckpointDirtyTracked();
                }
            }
        }
        // A ring is never frozen incrementally, and only a dispatched function has its dirty
        // set marked at all.
        return function.supportsCheckpointRingState()
                || !isDispatched(anchorable, function)
                || !function.isCheckpointDirtyTracked();
    }

    /**
     * Decides one boundary: it is refused when {@code max(K_front, K_i) x (R - i) > B - S}.
     * Written as a division, which is exact for non-negative integers and cannot overflow.
     *
     * @param imagedKeys          S, the keys the boundaries admitted so far image
     * @param boundaryKeys        K_i, the keys W holds at this boundary
     * @param frontierKeys        K_front, the keys W held at the frontier
     * @param remainingBoundaries R - i, this boundary and every one above it, at least 1
     * @param budgetKeys          B, at least S
     * @return whether this boundary may be frozen
     */
    public static boolean isWithinBudget(
            long imagedKeys,
            long boundaryKeys,
            long frontierKeys,
            int remainingBoundaries,
            long budgetKeys
    ) {
        return Math.max(frontierKeys, boundaryKeys) <= (budgetKeys - imagedKeys) / remainingBoundaries;
    }

    /**
     * Drops the window and the functions the last repair priced.
     */
    public void clear() {
        anchorWindow = null;
        wholeImagedFunctions.clear();
        anchorMapWalks.clear();
        boundaryKeys = 0;
        budgetKeys = 0;
        frontierKeys = 0;
        imagedKeys = 0;
        replayScanRows = 0;
    }

    /**
     * @return K_i of the boundary the last {@link #isAffordable} priced
     */
    public long getBoundaryKeys() {
        return boundaryKeys;
    }

    public long getBudgetKeys() {
        return budgetKeys;
    }

    public long getFrontierKeys() {
        return frontierKeys;
    }

    /**
     * @return S, the keys the boundaries admitted so far image whole
     */
    public long getImagedKeys() {
        return imagedKeys;
    }

    /**
     * @return the base rows the repair's replay is estimated to read, which {@link #of} took
     */
    public long getReplayScanRows() {
        return replayScanRows;
    }

    /**
     * @return whether the last {@link #of} found something the chain images whole, which is
     * what a repair binding this price to its freeze cursor asks
     */
    public boolean hasWholeImagedFunctions() {
        return wholeImagedFunctions.size() > 0;
    }

    @Override
    public boolean isAffordable(int boundaryIndex, int boundaryCount) {
        boundaryKeys = liveKeys();
        if (!isWithinBudget(imagedKeys, boundaryKeys, frontierKeys, boundaryCount - boundaryIndex, budgetKeys)) {
            return false;
        }
        imagedKeys += boundaryKeys;
        return true;
    }

    /**
     * Collects W out of {@code windowFactory}'s functions and reads K_front off their maps,
     * so the caller binds it where the maps hold what K_front means for its repair: the
     * resume before anything clears them for the anchor restore, the head miss after its
     * wipe.
     *
     * @param anchorWindow   the ANCHOR window of the runtime the chain freezes, or null
     * @param windowFactory  the runtime the chain freezes
     * @param replayScanRows the base rows the repair's replay is estimated to read, a count:
     *                       never {@code LONG_NULL}, which a plan field that priced nothing
     *                       holds
     * @param scanMaxKeys    {@code cairo.live.view.checkpoint.repair.scan.max.keys}, at or
     *                       below zero for no floor
     * @return false when nothing the chain freezes images keys whole, so there is nothing to
     * price: a scalar root images no key, and every other function freezes incrementally
     */
    public boolean of(
            @Nullable LiveViewWindow anchorWindow,
            WindowRecordCursorFactory windowFactory,
            long replayScanRows,
            long scanMaxKeys
    ) {
        assert replayScanRows >= 0 : "a chain priced against no row estimate";
        clear();
        final ObjList<WindowFunction> functions = windowFactory.getWindowFunctions();
        final ObjList<WindowFunction> anchorable = windowFactory.getAnchorableWindowFunctions();
        // A member of a fused group is walked out of the window's anchor map, whose storage
        // plan every anchored window has (see isCheckpointImagedWhole).
        final LiveViewWindowStatePlan fusedPlan = anchorWindow != null && anchorWindow.isWindowStateFused()
                ? anchorWindow.getCheckpointStoragePlan()
                : null;
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (!function.supportsCheckpointState() || !isCheckpointImagedWhole(anchorWindow, anchorable, function)) {
                continue;
            }
            // freezeBoundary hands a member of a fused group to the group's walk, which reads
            // the anchor map; the member's own map is closed.
            final boolean isAnchorMapWalked = fusedPlan != null && fusedPlan.indexOfProjectionFunction(function) >= 0;
            if (!isAnchorMapWalked && function.getPartitionMap() == null) {
                continue;
            }
            wholeImagedFunctions.add(function);
            anchorMapWalks.add(isAnchorMapWalked);
        }
        if (wholeImagedFunctions.size() == 0) {
            return false;
        }
        this.anchorWindow = anchorWindow;
        this.replayScanRows = replayScanRows;
        // The decided rule, in one place: a kept chain images at most what its own replay
        // reads, or the configured floor for a small replay.
        budgetKeys = Math.max(replayScanRows, LiveViewCheckpointRowsBounds.budgetOf(scanMaxKeys));
        frontierKeys = liveKeys();
        return true;
    }

    private static boolean isDispatched(@Nullable ObjList<WindowFunction> anchorable, WindowFunction function) {
        if (anchorable == null) {
            return false;
        }
        for (int i = 0, n = anchorable.size(); i < n; i++) {
            if (anchorable.getQuick(i) == function) {
                return true;
            }
        }
        return false;
    }

    // The keys a complete walk of every W member visits right now.
    private long liveKeys() {
        long keys = 0;
        for (int i = 0, n = wholeImagedFunctions.size(); i < n; i++) {
            keys += anchorMapWalks.get(i)
                    ? anchorWindow.getAnchorMapSize()
                    : wholeImagedFunctions.getQuick(i).getPartitionMap().size();
        }
        return keys;
    }
}

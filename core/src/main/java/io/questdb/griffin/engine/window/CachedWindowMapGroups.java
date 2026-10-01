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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnTypes;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * The window Map groups of a cached window factory, arranged by the traversal that drives
 * them: one list per ORDER BY sort group, and one per pass-1 scan direction for the
 * functions that need no sort of their own.
 * <p>
 * The group itself is {@link WindowMapState} and is the streaming cursor's as well - one map,
 * one lookup a row, every contributor then every projection. What is different here is only
 * where the row comes from and what happens to the answer. A cached factory hands its
 * functions a chain record and a row offset, and each function's {@code pass1} writes its
 * scalar output to that row's address; a bound function's {@code pass1} is
 * {@code computeNext} (a no-op once bound) followed by that write, so driving the group
 * immediately before the sort group's {@code pass1} loop leaves every member writing the
 * value the group's projection loop has just materialized.
 *
 * <h2>Two-pass groups</h2>
 * A group whose functions read the whole partition - {@code sum(x) + avg(x) + count(x)} over
 * {@code (partition by k)} - is driven from two traversals rather than one, and appears in two
 * of the lists below: the pass-1 list of its bucket, where {@code computeNext} absorbs the row
 * and projects nothing, and the pass-2 list, where {@link WindowMapState#projectPass2} writes
 * every output from the accumulator pass 1 left final. A bound member's {@code pass1} and
 * {@code preparePass2} are then no-ops and its {@code pass2} is the write, which is how the
 * destructive finalization those functions perform on their own maps - {@code avg} replacing
 * the sum slot a {@code sum} projection still needs - stops being needed at all.
 * <p>
 * A group is never mixed: {@link WindowMapSpec} carries the pass count, so a whole-partition
 * function and a cumulative one over the same key and frame are already two groups.
 *
 * <h2>Why a group belongs to exactly one traversal</h2>
 * A cached factory splits its functions three ways - one bucket per distinct ORDER BY index
 * list, plus the natural-order functions, themselves split by pass-1 scan direction - and each
 * bucket is traversed separately. A {@link WindowMapSpec} carries the order columns, their
 * directions, whether that order was dismissed against the base cursor, and the function's own
 * pass-1 direction, which is exactly what decides the bucket. So two functions that share a
 * spec share a bucket, and the groups are compiled from one bucket's members at a time: a sort
 * group's Map subgroups, which is the containment the design asks for, rather than a property
 * checked afterwards.
 * <p>
 * Sharing a sort is not sharing a map, and the reverse split is what makes that so: one bucket
 * routinely holds several specs - different partition keys, different frames - and each of them
 * is its own group with its own key domain.
 *
 * <h2>Ownership</h2>
 * This object owns the group maps and nothing else: the functions, their arguments and their
 * now-dormant private partition maps stay the factory's exactly as they were. The owning
 * factory frees it once, and the per-cursor {@link #reopen} / {@link #reset} pair moves the
 * native backing under whichever per-query {@link MemoryTracker} the open bound.
 * <p>
 * That pair is the whole of the lifecycle, and deliberately the same one the bound functions
 * have: allocated on the open that finds the cursor closed, handed back at close, and left
 * alone by {@code toTop} - which on a cached cursor rewinds a chain the traversal has already
 * filled and re-runs no pass. There is no clear of the key domain anywhere, because a group
 * has to answer what the private maps it replaced would have answered, and those are cleared
 * by exactly the same two events.
 */
public final class CachedWindowMapGroups implements QuietCloseable {
    private final ObjList<WindowMapState> allStates;
    private final ObjList<WindowMapState> backwardUnorderedStates;
    private final ObjList<WindowMapState> forwardUnorderedStates;
    private final ObjList<ObjList<WindowMapState>> orderedPass2States;
    private final ObjList<ObjList<WindowMapState>> orderedStates;
    private final ObjList<WindowAccumulatorPlan> plans;
    private final ObjList<WindowMapState> unorderedPass2States;

    private CachedWindowMapGroups(
            @NotNull ObjList<WindowAccumulatorPlan> plans,
            @NotNull ObjList<WindowMapState> allStates,
            @Nullable ObjList<ObjList<WindowMapState>> orderedStates,
            @Nullable ObjList<WindowMapState> forwardUnorderedStates,
            @Nullable ObjList<WindowMapState> backwardUnorderedStates,
            @Nullable ObjList<ObjList<WindowMapState>> orderedPass2States,
            @Nullable ObjList<WindowMapState> unorderedPass2States
    ) {
        this.plans = plans;
        this.allStates = allStates;
        this.orderedStates = orderedStates;
        this.forwardUnorderedStates = forwardUnorderedStates;
        this.backwardUnorderedStates = backwardUnorderedStates;
        this.orderedPass2States = orderedPass2States;
        this.unorderedPass2States = unorderedPass2States;
    }

    /**
     * Compiles one plan per Map group the given buckets form and binds the ones this build
     * gives a runtime, or returns null when the factory's functions form no group at all.
     * <p>
     * A non-null answer holding no state is the ordinary shape with
     * {@code cairo.sql.window.map.fusion.enabled} off: the plans are still worked out - a plan
     * no runtime reads costs a query nothing - and every function keeps the private map it
     * has always had.
     *
     * @param orderedFunctions   one list per ORDER BY sort group, in the order the factory
     *                           traverses them, so {@link #getOrderedStates(int)} answers by
     *                           the same index
     * @param unorderedFunctions the functions that need no sort of their own, or null
     * @param specFunctions      every window function of the SELECT list, in output order
     * @param specs              the window spec of the {@code specFunctions} entry beside it,
     *                           or null for a shape the group compiler does not admit
     * @param chainTypes         the record chain's own type list, by index. It is the layout
     *                           of the record these functions read and the group's key
     *                           projection writes from, and it is dense where the chain
     *                           metadata is not - that metadata leaves a hole at every window
     *                           output's index
     */
    public static @Nullable CachedWindowMapGroups of(
            @NotNull CairoConfiguration configuration,
            @NotNull BytecodeAssembler asm,
            @NotNull ObjList<ObjList<WindowFunction>> orderedFunctions,
            @Nullable ObjList<WindowFunction> unorderedFunctions,
            @NotNull ObjList<WindowFunction> specFunctions,
            @NotNull ObjList<WindowMapSpec> specs,
            @NotNull ColumnTypes chainTypes
    ) {
        final ObjList<WindowMapState> allStates = new ObjList<>();
        ObjList<WindowAccumulatorPlan> plans = null;
        ObjList<ObjList<WindowMapState>> orderedStates = null;
        ObjList<ObjList<WindowMapState>> orderedPass2States = null;
        ObjList<WindowMapState> forwardUnorderedStates = null;
        ObjList<WindowMapState> backwardUnorderedStates = null;
        ObjList<WindowMapState> unorderedPass2States = null;
        try {
            for (int i = 0, n = orderedFunctions.size(); i < n; i++) {
                final ObjList<WindowFunction> bucket = orderedFunctions.getQuick(i);
                final ObjList<WindowAccumulatorPlan> bucketPlans =
                        compileBucket(bucket, specFunctions, specs, chainTypes);
                if (bucketPlans == null) {
                    continue;
                }
                plans = collect(plans, bucketPlans);
                final ObjList<WindowMapState> states =
                        WindowMapState.createGroups(configuration, asm, bucketPlans, chainTypes);
                if (states == null) {
                    continue;
                }
                allStates.addAll(states);
                if (orderedStates == null) {
                    orderedStates = new ObjList<>(n);
                }
                orderedStates.extendAndSet(i, states);
                // A bucket is one sort, not one frame: a whole-partition group and a
                // cumulative one over the same ORDER BY share the traversal that fills
                // their maps and part company at the end of it.
                ObjList<WindowMapState> bucketPass2States = null;
                for (int j = 0, m = states.size(); j < m; j++) {
                    final WindowMapState state = states.getQuick(j);
                    if (state.isTwoPass()) {
                        if (bucketPass2States == null) {
                            bucketPass2States = new ObjList<>();
                        }
                        bucketPass2States.add(state);
                    }
                }
                if (bucketPass2States != null) {
                    if (orderedPass2States == null) {
                        orderedPass2States = new ObjList<>(n);
                    }
                    orderedPass2States.extendAndSet(i, bucketPass2States);
                }
            }
            if (unorderedFunctions != null) {
                final ObjList<WindowAccumulatorPlan> bucketPlans =
                        compileBucket(unorderedFunctions, specFunctions, specs, chainTypes);
                if (bucketPlans != null) {
                    plans = collect(plans, bucketPlans);
                    final ObjList<WindowMapState> states =
                            WindowMapState.createGroups(configuration, asm, bucketPlans, chainTypes);
                    if (states != null) {
                        allStates.addAll(states);
                        for (int i = 0, n = states.size(); i < n; i++) {
                            // Every member of a group agrees with its spec on the pass-1
                            // direction, so the group runs in whichever of the two loops its
                            // members' pass1 does.
                            final WindowMapState state = states.getQuick(i);
                            if (isForward(state)) {
                                if (forwardUnorderedStates == null) {
                                    forwardUnorderedStates = new ObjList<>();
                                }
                                forwardUnorderedStates.add(state);
                            } else {
                                if (backwardUnorderedStates == null) {
                                    backwardUnorderedStates = new ObjList<>();
                                }
                                backwardUnorderedStates.add(state);
                            }
                            if (state.isTwoPass()) {
                                // One list whichever way pass 1 ran: the natural-order pass-2
                                // traversal is a single forward walk of the whole chain, which
                                // is what the functions' own pass2 loop already is.
                                if (unorderedPass2States == null) {
                                    unorderedPass2States = new ObjList<>();
                                }
                                unorderedPass2States.add(state);
                            }
                        }
                    }
                }
            }
        } catch (Throwable th) {
            Misc.freeObjList(allStates);
            throw th;
        }
        if (plans == null) {
            // No plan means no state either - a state exists only where a plan does - so
            // there is nothing to free on the way out.
            return null;
        }
        return new CachedWindowMapGroups(
                plans,
                allStates,
                orderedStates,
                forwardUnorderedStates,
                backwardUnorderedStates,
                orderedPass2States,
                unorderedPass2States
        );
    }

    @Override
    public void close() {
        Misc.freeObjList(allStates);
        allStates.clear();
    }

    /**
     * The groups driven by the backward natural-order traversal, or null when there are none.
     */
    public @Nullable ObjList<WindowMapState> getBackwardUnorderedStates() {
        return backwardUnorderedStates;
    }

    /**
     * The groups driven by the forward natural-order traversal - the base scan that fills the
     * record chain - or null when there are none.
     */
    public @Nullable ObjList<WindowMapState> getForwardUnorderedStates() {
        return forwardUnorderedStates;
    }

    /**
     * The two-pass groups of sort group {@code index}, or null when it holds none. They are a
     * subset of {@link #getOrderedStates(int)} - pass 1 fills every group of the bucket
     * alike - and what this list adds is that these ones still have their outputs to write
     * when the factory's own pass-2 traversal of the same sort group runs.
     */
    public @Nullable ObjList<WindowMapState> getOrderedPass2States(int index) {
        return orderedPass2States != null ? orderedPass2States.getQuiet(index) : null;
    }

    /**
     * The groups driven by sort group {@code index}'s traversal, or null when that sort group
     * forms none.
     */
    public @Nullable ObjList<WindowMapState> getOrderedStates(int index) {
        return orderedStates != null ? orderedStates.getQuiet(index) : null;
    }

    /**
     * Every group this factory's functions form, whether a runtime binds it or not. A plan
     * without a state is one {@code cairo.sql.window.map.fusion.enabled} turned away, which is
     * a decision worth being able to assert rather than an absence.
     */
    @TestOnly
    public ObjList<WindowAccumulatorPlan> getPlans() {
        return plans;
    }

    /**
     * Every bound group, in no particular traversal order. It is the lifecycle list - what
     * {@link #reopen} allocates and {@link #reset} hands back - and what a test reads the
     * structural counters off.
     */
    @TestOnly
    public ObjList<WindowMapState> getStates() {
        return allStates;
    }

    /**
     * The two-pass groups of the natural-order bucket, or null when it holds none. One list
     * whichever way their pass 1 ran, because the pass-2 traversal is a single forward walk of
     * every row.
     */
    public @Nullable ObjList<WindowMapState> getUnorderedPass2States() {
        return unorderedPass2States;
    }

    /**
     * Binds the per-query tracker on every group's map and allocates its backing, in that
     * order, so the malloc and the free {@link #reset()} performs land on one counter.
     */
    public void reopen(@Nullable MemoryTracker memoryTracker) {
        for (int i = 0, n = allStates.size(); i < n; i++) {
            final WindowMapState state = allStates.getQuick(i);
            state.setMemoryTracker(memoryTracker);
            state.reopen();
        }
    }

    /**
     * Hands every group's map backing back at cursor close, leaving the groups reusable.
     */
    public void reset() {
        for (int i = 0, n = allStates.size(); i < n; i++) {
            allStates.getQuick(i).reset();
        }
    }

    private static @Nullable ObjList<WindowAccumulatorPlan> compileBucket(
            @NotNull ObjList<WindowFunction> bucket,
            @NotNull ObjList<WindowFunction> specFunctions,
            @NotNull ObjList<WindowMapSpec> specs,
            @NotNull ColumnTypes chainTypes
    ) {
        // The bucket's own spec list, in the bucket's own order. Output positions are
        // therefore the bucket's rather than the SELECT list's, which the plan reads for one
        // thing only - the lowest position among equal candidates wins the contributor role -
        // and a bucket collects its members in SELECT order, so the two agree on which that is.
        final ObjList<WindowMapSpec> bucketSpecs = new ObjList<>(bucket.size());
        for (int i = 0, n = bucket.size(); i < n; i++) {
            bucketSpecs.add(specOf(bucket.getQuick(i), specFunctions, specs));
        }
        return WindowAccumulatorPlanBuilder.compileGroups(bucket, bucketSpecs, chainTypes);
    }

    private static ObjList<WindowAccumulatorPlan> collect(
            @Nullable ObjList<WindowAccumulatorPlan> plans,
            @NotNull ObjList<WindowAccumulatorPlan> bucketPlans
    ) {
        if (plans == null) {
            plans = new ObjList<>();
        }
        plans.addAll(bucketPlans);
        return plans;
    }

    private static boolean isForward(@NotNull WindowMapState state) {
        final WindowMapSpec spec = state.getPlan().getSpec();
        assert spec != null;
        return spec.getPass1ScanDirection() == WindowFunction.Pass1ScanDirection.FORWARD;
    }

    private static @Nullable WindowMapSpec specOf(
            @NotNull WindowFunction function,
            @NotNull ObjList<WindowFunction> specFunctions,
            @NotNull ObjList<WindowMapSpec> specs
    ) {
        for (int i = 0, n = specFunctions.size(); i < n; i++) {
            if (specFunctions.getQuick(i) == function) {
                return specs.getQuick(i);
            }
        }
        return null;
    }
}

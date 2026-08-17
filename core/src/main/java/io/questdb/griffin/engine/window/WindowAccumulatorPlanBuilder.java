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

import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.sql.Function;
import io.questdb.std.Hash;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * Collects the window functions of one group and, on {@link #build(int)}, merges identical
 * components, folds a component into a wider one that provably contains it, orders what is
 * left canonically and assigns the slot bases a {@link WindowAccumulatorPlan} is read
 * through.
 * <p>
 * A query buckets its functions by {@link WindowMapSpec} and reads the plan straight.
 * Nothing durable is decided here: no manifest, no byte offsets, no codec version and no leaf
 * budget to truncate against. A group's layout is a promise to nothing beyond its own cursor,
 * which is what lets it be re-decided on every compile.
 * <p>
 * Every rejection is an ordinary answer rather than an error. A function the group does not
 * carry keeps the private map and the per-row update it has today, and a group that carries
 * only one function is not worth binding at all - see
 * {@link WindowAccumulatorPlan#getStructuralReduction()}.
 */
public final class WindowAccumulatorPlanBuilder {
    /**
     * The slot table's floor, and the size it is first allocated at. Growth is driven by the
     * half-full test below rather than by this constant, so a table of this size grows at the
     * ninth component, not the seventeenth.
     */
    private static final int MIN_COMPONENT_SLOTS = 16;
    /**
     * The activation rule: an owner binds a group only where it removes at least this much
     * of the unfused runtime. Two is what a plain two-function group scores - both private
     * maps go dormant behind one group map - and one is what a single-function group scores,
     * which is a map moved rather than a map removed.
     */
    private static final int MIN_STRUCTURAL_REDUCTION = 2;
    /**
     * Per component, whether the projection currently chosen as its contributor is a
     * {@link WindowAccumulatorProjection#isPartitionKeyGuarded() guarded} one. A guarded
     * projection keeps a different counter from the component it reads, so it yields the
     * role to any candidate that does not, and {@link #build(int)} refuses a component left
     * with no other.
     */
    private final IntList componentContributorIsGuarded = new IntList();
    private final IntList componentContributorOutputPositions = new IntList();
    private final IntList componentContributors = new IntList();
    /**
     * Open-addressed slots over {@link #components}, holding {@code componentIndex + 1} with
     * zero for an empty slot, so a projection finds the component it names by hash rather
     * than by comparing itself against every component added so far. A group's components
     * are as many as its distinct arguments, so the scan it replaces grew with the SELECT
     * list on both sides.
     * <p>
     * Hashed on exactly the four fields {@link WindowAccumulatorDescriptor#isSameIdentity}
     * compares and resolved through that same method, so the table cannot separate two
     * components the builder would have merged.
     */
    private final IntList componentSlots = new IntList();
    private final ObjList<WindowAccumulatorDescriptor> components = new ObjList<>();
    private final FoldPolicy foldPolicy;
    private final WindowAccumulatorCandidate.PartitionKeyGuard partitionKeyGuard;
    private final IntList projectionComponents = new IntList();
    /**
     * Per projection, the component its own function would keep when it stands alone. Kept
     * beside {@link #projectionComponents}, which the fold moves onto a host: the difference
     * between the two is the slice a runtime handing the state back would read.
     */
    private final ObjList<WindowAccumulatorDescriptor> projectionFunctionComponents = new ObjList<>();
    private final ObjList<WindowFunction> projectionFunctions = new ObjList<>();
    private final IntList projectionKinds = new IntList();
    private final IntList projectionOutputPositions = new IntList();
    private final WindowMapSpec spec;
    private int componentSlotMask;

    public WindowAccumulatorPlanBuilder(@NotNull WindowMapSpec spec) {
        this(spec, null);
    }

    /**
     * @param spec       the group identity every function of this builder shares, or null when
     *                   the owner proves that identity its own way - a live view holds an
     *                   encoded window identity and a key schema instead, and compiles no
     *                   {@link WindowMapSpec} at all
     * @param foldPolicy an owner's veto on the containment fold, or null to fold wherever
     *                   {@link WindowAccumulatorDescriptor#derivedSlotOffset} proves it
     */
    public WindowAccumulatorPlanBuilder(@Nullable WindowMapSpec spec, @Nullable FoldPolicy foldPolicy) {
        this.spec = spec;
        this.foldPolicy = foldPolicy;
        // The whole of a group's key is its spec's, so one guard serves every function this
        // builder is offered. A live view has no spec and offers no guarded projection through
        // this path: its own compiler resolves the candidate and passes a guard of its own.
        //
        // The key term has to be a column before it can be the argument's: an expression term
        // answers -1, which is a column no argument names, and the explicit test is what keeps
        // that from resting on the caller having resolved its argument first.
        this.partitionKeyGuard = (function, argumentColumnIndex, rowCountHost) -> spec != null
                && spec.getPartitionColumnCount() == 1
                && spec.getKeyColumnCount() == 1
                && spec.getPartitionColumnIndex(0) >= 0
                && spec.getPartitionColumnIndex(0) == argumentColumnIndex;
    }

    /**
     * Compiles one plan per window Map group the factory's functions form, or null when
     * none forms.
     * <p>
     * Groups are found by scanning the SELECT list once and bucketing every function whose
     * {@link WindowMapSpec} is equal to a bucket already open, so two windows join when
     * their normalized specifications match however they were written or named. Within a
     * bucket the walk is the compiler's: resolve each function's accumulator identity, offer
     * it, and let the builder decide whether it merges, folds or stands alone.
     * <p>
     * A bucket is dropped unless the plan it produces removes something real - see
     * {@link WindowAccumulatorPlan#getStructuralReduction()} - so a window carrying one
     * fusible function produces no group, and neither does one carrying none.
     *
     * @param functions   the functions to bucket, in output order - every SELECT-list
     *                    function for the streaming cursor, and one sort group's own
     *                    members for a cached one, which is why the list is only required
     *                    to hold functions
     * @param specs       one entry per {@code functions} index: the window spec that index
     *                    was compiled under, or null for a non-window column and for a
     *                    window shape this build does not group
     * @param recordTypes the types of the record the window functions and their arguments
     *                    read, by index - see
     *                    {@link WindowAccumulatorDescriptor#directColumnIndex}
     */
    public static @Nullable ObjList<WindowAccumulatorPlan> compileGroups(
            @NotNull ObjList<? extends Function> functions,
            @NotNull ObjList<WindowMapSpec> specs,
            @NotNull ColumnTypes recordTypes
    ) {
        ObjList<WindowAccumulatorPlanBuilder> builders = null;
        // Per builder, the SELECT-list indexes bucketed onto it. Filling these on the one
        // pass below is what keeps the second phase from re-scanning the whole SELECT list
        // once per group: the membership question is answered where it is already being
        // asked, and the answer is kept rather than asked again.
        ObjList<IntList> builderMembers = null;
        // Buckets the open builders by their spec's hash, so a function finds the group it
        // belongs to without comparing its spec against every group already open. The hash
        // narrows and isSameSpec still decides, so a collision costs a comparison the scan
        // would have made anyway and can never join two groups that differ.
        final IntList builderSlots = new IntList();
        int builderSlotMask = 0;
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowMapSpec spec = i < specs.size() ? specs.getQuick(i) : null;
            if (spec == null || !(functions.getQuick(i) instanceof WindowFunction)) {
                continue;
            }
            if (builders == null) {
                builders = new ObjList<>();
                builderMembers = new ObjList<>();
                builderSlots.setAll(MIN_COMPONENT_SLOTS, 0);
                builderSlotMask = MIN_COMPONENT_SLOTS - 1;
            }
            int builderIndex = -1;
            int slot = spec.getSpecHash() & builderSlotMask;
            for (; ; ) {
                final int entry = builderSlots.getQuick(slot);
                if (entry == 0) {
                    break;
                }
                if (builders.getQuick(entry - 1).spec.isSameSpec(spec)) {
                    builderIndex = entry - 1;
                    break;
                }
                slot = (slot + 1) & builderSlotMask;
            }
            if (builderIndex < 0) {
                builderIndex = builders.size();
                builders.add(new WindowAccumulatorPlanBuilder(spec));
                builderMembers.add(new IntList());
                // Grow before the table can pass half full, so the probe above stays short
                // and always finds an empty slot to stop on.
                if (builderSlots.size() < (builders.size() << 1)) {
                    final int slotCount = Math.max(
                            MIN_COMPONENT_SLOTS,
                            Numbers.ceilPow2(builders.size() << 2)
                    );
                    builderSlots.setAll(slotCount, 0);
                    builderSlotMask = slotCount - 1;
                    for (int j = 0, m = builders.size(); j < m; j++) {
                        int s = builders.getQuick(j).spec.getSpecHash() & builderSlotMask;
                        while (builderSlots.getQuick(s) != 0) {
                            s = (s + 1) & builderSlotMask;
                        }
                        builderSlots.setQuick(s, j + 1);
                    }
                } else {
                    builderSlots.setQuick(slot, builderIndex + 1);
                }
            }
            builderMembers.getQuick(builderIndex).add(i);
        }
        if (builders == null) {
            return null;
        }
        ObjList<WindowAccumulatorPlan> plans = null;
        for (int b = 0, bn = builders.size(); b < bn; b++) {
            final WindowAccumulatorPlanBuilder builder = builders.getQuick(b);
            // Read before a single projection is added, because whether a count over the
            // partition key may join a row count depends on the group holding an unguarded
            // reading of that same row count - and the count may precede it in the SELECT
            // list. Everything else about a projection follows from the function alone.
            final WindowFunction rowCountHost = rowCountHost(functions, specs, builder.spec);
            final IntList members = builderMembers.getQuick(b);
            for (int m = 0, mn = members.size(); m < mn; m++) {
                final int i = members.getQuick(m);
                addAccumulatorProjection(
                        builder,
                        (WindowFunction) functions.getQuick(i),
                        i,
                        recordTypes,
                        rowCountHost
                );
            }
            final WindowAccumulatorPlan plan = builder.build(0);
            if (plan == null || plan.getStructuralReduction() < MIN_STRUCTURAL_REDUCTION) {
                continue;
            }
            if (plans == null) {
                plans = new ObjList<>();
            }
            plans.add(plan);
        }
        return plans;
    }

    /**
     * Binds {@code function} to {@code component}, creating the component when no
     * previously added projection already names an identical one. Declines when the family
     * cannot produce {@code projectionKind}.
     * <p>
     * The window identity is not re-checked here as the live-view builder re-checks it: a
     * builder is created per {@link WindowMapSpec} bucket, so belonging to this builder is
     * what having that identity means.
     *
     * @return true when the projection joined the group
     */
    public boolean addProjection(
            @NotNull WindowFunction function,
            @NotNull WindowAccumulatorDescriptor component,
            int projectionKind,
            int outputPosition
    ) {
        if (!WindowAccumulatorProjection.isCompatible(component.getFamily(), projectionKind)) {
            return false;
        }
        final boolean isGuarded = projectionKind == WindowAccumulatorProjection.PROJECTION_COUNT_PARTITION_KEY;
        int componentIndex = indexOfSameIdentity(component);
        if (componentIndex < 0) {
            componentIndex = components.size();
            components.add(component);
            indexComponent(componentIndex);
            componentContributors.add(projectionFunctions.size());
            componentContributorOutputPositions.add(outputPosition);
            componentContributorIsGuarded.add(isGuarded ? 1 : 0);
        } else if (isBetterContributor(componentIndex, outputPosition, isGuarded)) {
            // Deterministic contributor choice: an unguarded projection outranks a guarded
            // one whatever their positions, and among equals the lowest output position
            // wins - so the answer follows the compiled query rather than traversal order.
            componentContributors.setQuick(componentIndex, projectionFunctions.size());
            componentContributorOutputPositions.setQuick(componentIndex, outputPosition);
            componentContributorIsGuarded.setQuick(componentIndex, isGuarded ? 1 : 0);
        }
        projectionFunctions.add(function);
        projectionComponents.add(componentIndex);
        projectionFunctionComponents.add(component);
        projectionKinds.add(projectionKind);
        projectionOutputPositions.add(outputPosition);
        return true;
    }

    /**
     * Assembles the plan, or returns null when nothing joined the group.
     *
     * @param slotPrefix how many map value slots the group's owner reserved ahead of the
     *                   components - zero for an ordinary query, the anchor slots for a
     *                   live view
     */
    public @Nullable WindowAccumulatorPlan build(int slotPrefix) {
        if (components.size() == 0) {
            return null;
        }
        foldDerivedComponents();
        sortComponentsByIdentity();
        // Both renumbered the components the slots point at, so the table is rebuilt rather
        // than left describing the order they replaced. Nothing offers this builder a
        // projection after build() today; rebuilding is what keeps that from being a
        // silent requirement.
        rebuildComponentSlots();
        final int componentCount = components.size();
        for (int i = 0; i < componentCount; i++) {
            if (componentContributorIsGuarded.getQuick(i) != 0) {
                // Belt and braces: the compiler only offers a guarded projection where the
                // same group already holds an unguarded reading of the same component, so
                // this is unreachable. Declining the whole plan rather than shipping a
                // component nothing maintains correctly is the fail-safe direction - every
                // function goes back to the private map it owns outside a group.
                return null;
            }
        }
        final IntList componentSlotBases = new IntList(componentCount);
        int slot = slotPrefix;
        for (int i = 0; i < componentCount; i++) {
            componentSlotBases.add(slot);
            slot += components.getQuick(i).getSlotCount();
        }
        final ObjList<WindowAccumulatorProjection> projections = new ObjList<>(projectionKinds.size());
        for (int i = 0, n = projectionKinds.size(); i < n; i++) {
            final int componentIndex = projectionComponents.getQuick(i);
            projections.add(new WindowAccumulatorProjection(
                    projectionKinds.getQuick(i),
                    projectionOutputPositions.getQuick(i),
                    componentIndex,
                    componentSlotBases.getQuick(componentIndex),
                    components.getQuick(componentIndex),
                    projectionFunctionComponents.getQuick(i)
            ));
        }
        return new WindowAccumulatorPlan(
                spec,
                components,
                componentSlotBases,
                componentContributors,
                projections,
                projectionFunctions,
                slotPrefix,
                slot
        );
    }

    /**
     * Offers {@code function} to the group, and reports whether it joined. Every rejection
     * below is an ordinary answer: the function keeps the private map and the per-row
     * update it has outside a group.
     */
    private static boolean addAccumulatorProjection(
            WindowAccumulatorPlanBuilder builder,
            WindowFunction function,
            int outputPosition,
            ColumnTypes recordTypes,
            @Nullable WindowFunction rowCountHost
    ) {
        if (!isFusibleAccumulator(function)) {
            return false;
        }
        final WindowAccumulatorCandidate candidate = WindowAccumulatorCandidate.of(
                function,
                recordTypes,
                rowCountHost,
                builder.partitionKeyGuard
        );
        return candidate != null && builder.addProjection(
                function,
                candidate.getComponent(),
                candidate.getProjectionKind(),
                outputPosition
        );
    }

    /**
     * Hashes the three fields {@link WindowAccumulatorDescriptor#derivedSlotOffset} requires
     * a pair to agree on before it will look at their families at all. Deliberately the
     * method's precondition rather than its conclusion: a family pair added to its table
     * still lands in the chain this builds, where a hash over the pairs themselves would
     * have to be updated alongside it.
     */
    private static int foldChainHash(WindowAccumulatorDescriptor component) {
        int h = component.getContributionKind();
        h = h * 31 + component.getArgumentColumnIndex();
        h = h * 31 + component.getArgumentColumnType();
        return Hash.spread(h);
    }

    /**
     * Hashes exactly the four fields {@link WindowAccumulatorDescriptor#isSameIdentity}
     * compares, so two components the builder would merge cannot land in different slots.
     */
    private static int identityHash(WindowAccumulatorDescriptor component) {
        int h = component.getFamily();
        h = h * 31 + component.getContributionKind();
        h = h * 31 + component.getArgumentColumnIndex();
        h = h * 31 + component.getArgumentColumnType();
        return Hash.spread(h);
    }

    /**
     * The gates a function passes before any part of its accumulator identity is read.
     * Shared by the projection walk and by the row-count pre-pass, so the pre-pass cannot
     * name a host the walk would have declined.
     * <p>
     * There is one, and it is the map. A group exists to replace several function-owned
     * partition maps with one, so a function that owns none - a cumulative function over an
     * unpartitioned window keeps its state in scalar fields - has nothing to contribute to
     * that trade and would only gain a probe.
     */
    private static boolean isFusibleAccumulator(WindowFunction function) {
        return function.getPartitionMap() != null;
    }

    /**
     * The first function of {@code functions} in this group that would join it as an
     * unguarded {@link WindowAccumulatorDescriptor#FAMILY_ROW_COUNT row count} - a
     * {@code count(*)} or a partitioned {@code row_number()} - or null when the group holds
     * none.
     * <p>
     * It is the host a {@code count} over the window's own partition key may join, and the
     * reason the search runs before any projection is added: such a count may precede its
     * host in the SELECT list, and whether it fuses must not depend on that.
     * <p>
     * The gates are the same ones {@link #addAccumulatorProjection} applies, so this cannot
     * name a host that walk would decline, and the family half of the test is
     * {@link WindowAccumulatorCandidate#isRowCountHost}'s - the same one the live-view
     * compiler's own pre-pass reads.
     */
    private static @Nullable WindowFunction rowCountHost(
            ObjList<? extends Function> functions,
            ObjList<WindowMapSpec> specs,
            WindowMapSpec groupSpec
    ) {
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowMapSpec spec = i < specs.size() ? specs.getQuick(i) : null;
            if (spec != null
                    && groupSpec.isSameSpec(spec)
                    && functions.getQuick(i) instanceof WindowFunction windowFunction
                    && isFusibleAccumulator(windowFunction)
                    && WindowAccumulatorCandidate.isRowCountHost(windowFunction)) {
                return windowFunction;
            }
        }
        return null;
    }

    /**
     * Drops every component whose whole state already sits inside another's, moving its
     * projections onto that host. This is what takes {@code sum(x) + avg(x) + count(x)} from
     * two components and three slots to one component and two: the count reads the counter
     * the sum already keeps rather than maintaining a second copy of it.
     * <p>
     * The fold is a function of the component <b>set</b> and not of the order the
     * projections were added in, so reordering two outputs of one query cannot move a slot
     * base. Where more than one host could serve, the smallest identity wins, which is the
     * order the layout is assigned in anyway.
     * <p>
     * A host must be strictly wider than its guest, so the relation is a strict partial
     * order and cannot cycle. Guests are still resolved widest-first and a host that is
     * itself folded is skipped, so a chain - which no pair in
     * {@link WindowAccumulatorDescriptor#derivedSlotOffset}'s table forms today - would
     * collapse one link at a time rather than leave a projection pointing at a component
     * that is no longer there.
     */
    private void foldDerivedComponents() {
        final int n = components.size();
        if (n < 2) {
            return;
        }
        final int[] byDescendingWidth = orderByDescendingWidth();
        final int[] hostOf = new int[n];
        Arrays.fill(hostOf, -1);
        // Chain the components by the triple derivedSlotOffset demands before it will
        // consider a pair at all, so a guest looks at the components that could host it
        // rather than at every one. A hash collision only widens a chain - the predicate
        // below still decides every pair - so the answer is the one the full scan gave.
        final int slotCount = Math.max(MIN_COMPONENT_SLOTS, Numbers.ceilPow2(n << 2));
        final int mask = slotCount - 1;
        final int[] chainHeads = new int[slotCount];
        Arrays.fill(chainHeads, -1);
        final int[] chainNext = new int[n];
        for (int i = 0; i < n; i++) {
            final int slot = foldChainHash(components.getQuick(i)) & mask;
            chainNext[i] = chainHeads[slot];
            chainHeads[slot] = i;
        }
        for (int i = 0; i < n; i++) {
            final int guest = byDescendingWidth[i];
            final WindowAccumulatorDescriptor guestComponent = components.getQuick(guest);
            int host = -1;
            // The winner is the smallest by identity, and two components in this list never
            // share one - they would have merged - so which order the chain visits them in
            // does not change the answer.
            for (int candidate = chainHeads[foldChainHash(guestComponent) & mask];
                 candidate >= 0;
                 candidate = chainNext[candidate]) {
                final WindowAccumulatorDescriptor candidateComponent = components.getQuick(candidate);
                if (candidate == guest
                        || hostOf[candidate] != -1
                        || candidateComponent.getSlotCount() <= guestComponent.getSlotCount()
                        || candidateComponent.derivedSlotOffset(guestComponent) < 0
                        || (foldPolicy != null && !foldPolicy.canFold(candidateComponent, guestComponent))) {
                    continue;
                }
                if (host < 0 || candidateComponent.compareIdentity(components.getQuick(host)) < 0) {
                    host = candidate;
                }
            }
            hostOf[guest] = host;
        }
        final int[] newIndexOfOld = new int[n];
        final ObjList<WindowAccumulatorDescriptor> kept = new ObjList<>(n);
        final IntList keptContributors = new IntList(n);
        final IntList keptContributorGuards = new IntList(n);
        final IntList keptContributorPositions = new IntList(n);
        for (int i = 0; i < n; i++) {
            if (hostOf[i] != -1) {
                // A folded component's contributor goes with it: the host has its own, and
                // it is the only one whose state is the whole component.
                newIndexOfOld[i] = -1;
                continue;
            }
            newIndexOfOld[i] = kept.size();
            kept.add(components.getQuick(i));
            keptContributors.add(componentContributors.getQuick(i));
            keptContributorPositions.add(componentContributorOutputPositions.getQuick(i));
            keptContributorGuards.add(componentContributorIsGuarded.getQuick(i));
        }
        if (kept.size() == n) {
            return;
        }
        for (int i = 0, m = projectionComponents.size(); i < m; i++) {
            final int old = projectionComponents.getQuick(i);
            projectionComponents.setQuick(i, newIndexOfOld[hostOf[old] != -1 ? hostOf[old] : old]);
        }
        components.clear();
        components.addAll(kept);
        componentContributors.clear();
        componentContributors.addAll(keptContributors);
        componentContributorOutputPositions.clear();
        componentContributorOutputPositions.addAll(keptContributorPositions);
        componentContributorIsGuarded.clear();
        componentContributorIsGuarded.addAll(keptContributorGuards);
    }

    /**
     * Adds the last component to the slot table, growing it when it would pass half full so
     * the linear probe stays short.
     */
    private void indexComponent(int componentIndex) {
        if (componentSlots.size() < (components.size() << 1)) {
            rebuildComponentSlots();
            return;
        }
        insertComponentSlot(componentIndex);
    }

    /**
     * The index of the component {@code component} would merge into, or {@code -1} when the
     * builder holds none.
     */
    private int indexOfSameIdentity(WindowAccumulatorDescriptor component) {
        if (componentSlots.size() == 0) {
            return -1;
        }
        int slot = identityHash(component) & componentSlotMask;
        for (; ; ) {
            final int entry = componentSlots.getQuick(slot);
            if (entry == 0) {
                return -1;
            }
            if (components.getQuick(entry - 1).isSameIdentity(component)) {
                return entry - 1;
            }
            slot = (slot + 1) & componentSlotMask;
        }
    }

    private void insertComponentSlot(int componentIndex) {
        int slot = identityHash(components.getQuick(componentIndex)) & componentSlotMask;
        while (componentSlots.getQuick(slot) != 0) {
            slot = (slot + 1) & componentSlotMask;
        }
        componentSlots.setQuick(slot, componentIndex + 1);
    }

    /**
     * Whether a projection just offered to component {@code componentIndex} should take the
     * contributor role from the one that holds it.
     * <p>
     * An unguarded projection always outranks a guarded one, whatever their output
     * positions: a guarded {@code count(k)} keeps the partition's {@code count(k)} where the
     * component keeps its row count, so it cannot be the one that maintains the state. Among
     * two of the same rank the lowest output position wins, which is what makes the choice a
     * function of the compiled query rather than of the order the compiler walked it in.
     */
    private boolean isBetterContributor(int componentIndex, int outputPosition, boolean isGuarded) {
        final boolean isIncumbentGuarded = componentContributorIsGuarded.getQuick(componentIndex) != 0;
        if (isGuarded != isIncumbentGuarded) {
            return isIncumbentGuarded;
        }
        return outputPosition < componentContributorOutputPositions.getQuick(componentIndex);
    }

    /**
     * Whether {@code left} sorts ahead of {@code right} in the order {@code isByWidth}
     * selects - the "less" the max-heap in {@link #sortComponentOrder} is built on.
     */
    private boolean isComponentLess(int left, int right, boolean isByWidth) {
        return isByWidth
                ? isWiderThan(left, right)
                : components.getQuick(left).compareIdentity(components.getQuick(right)) < 0;
    }

    private boolean isWiderThan(int left, int right) {
        final WindowAccumulatorDescriptor a = components.getQuick(left);
        final WindowAccumulatorDescriptor b = components.getQuick(right);
        return a.getSlotCount() != b.getSlotCount()
                ? a.getSlotCount() > b.getSlotCount()
                : a.compareIdentity(b) < 0;
    }

    /**
     * Orders the component indexes by descending slot count, ties broken by identity so the
     * answer does not depend on insertion order.
     */
    private int[] orderByDescendingWidth() {
        final int n = components.size();
        final int[] order = new int[n];
        for (int i = 0; i < n; i++) {
            order[i] = i;
        }
        sortComponentOrder(order, true);
        return order;
    }

    /**
     * Re-slots every component. Also the way the table is invalidated: the fold and the
     * identity sort renumber {@link #components}, and the slots hold those numbers.
     */
    private void rebuildComponentSlots() {
        final int slotCount = Math.max(MIN_COMPONENT_SLOTS, Numbers.ceilPow2(components.size() << 2));
        componentSlots.setAll(slotCount, 0);
        componentSlotMask = slotCount - 1;
        for (int i = 0, n = components.size(); i < n; i++) {
            insertComponentSlot(i);
        }
    }

    private void siftComponentOrder(int[] order, int root, int size, boolean isByWidth) {
        for (; ; ) {
            int largest = root;
            final int left = (root << 1) + 1;
            if (left < size && isComponentLess(order[largest], order[left], isByWidth)) {
                largest = left;
            }
            final int right = left + 1;
            if (right < size && isComponentLess(order[largest], order[right], isByWidth)) {
                largest = right;
            }
            if (largest == root) {
                return;
            }
            final int top = order[root];
            order[root] = order[largest];
            order[largest] = top;
            root = largest;
        }
    }

    /**
     * Heap-sorts {@code order} in place - descending by width when {@code isByWidth},
     * ascending by identity otherwise.
     * <p>
     * Heap rather than insertion because a group holds as many components as the SELECT
     * list has distinct arguments, which is not the handful the two call sites used to
     * assume: a hundred-column projection over one window sorts a hundred of them, twice.
     * In place and comparator-free, so the O(C log C) costs no allocation. Neither order
     * needs stability to be deterministic - two components in this list never share an
     * identity, because they would have been merged into one.
     */
    private void sortComponentOrder(int[] order, boolean isByWidth) {
        final int n = order.length;
        for (int root = (n >> 1) - 1; root >= 0; root--) {
            siftComponentOrder(order, root, n, isByWidth);
        }
        for (int end = n - 1; end > 0; end--) {
            final int top = order[0];
            order[0] = order[end];
            order[end] = top;
            siftComponentOrder(order, 0, end, isByWidth);
        }
    }

    /**
     * Sorts the components by identity, carrying the per-component bookkeeping and every
     * projection's component index with them. The index rewrite stays in one place, which
     * is what the ordering is for: the plan's component numbering follows the identities
     * rather than the SELECT list.
     */
    private void sortComponentsByIdentity() {
        final int n = components.size();
        final int[] order = new int[n];
        for (int i = 0; i < n; i++) {
            order[i] = i;
        }
        sortComponentOrder(order, false);
        final int[] newIndexOfOld = new int[n];
        final ObjList<WindowAccumulatorDescriptor> sorted = new ObjList<>(n);
        final IntList sortedContributors = new IntList(n);
        final IntList sortedContributorGuards = new IntList(n);
        final IntList sortedContributorPositions = new IntList(n);
        for (int i = 0; i < n; i++) {
            final int old = order[i];
            newIndexOfOld[old] = i;
            sorted.add(components.getQuick(old));
            sortedContributors.add(componentContributors.getQuick(old));
            sortedContributorPositions.add(componentContributorOutputPositions.getQuick(old));
            sortedContributorGuards.add(componentContributorIsGuarded.getQuick(old));
        }
        components.clear();
        components.addAll(sorted);
        componentContributors.clear();
        componentContributors.addAll(sortedContributors);
        componentContributorOutputPositions.clear();
        componentContributorOutputPositions.addAll(sortedContributorPositions);
        componentContributorIsGuarded.clear();
        componentContributorIsGuarded.addAll(sortedContributorGuards);
        for (int i = 0, m = projectionComponents.size(); i < m; i++) {
            projectionComponents.setQuick(i, newIndexOfOld[projectionComponents.getQuick(i)]);
        }
    }

    /**
     * An owner's veto on the containment fold.
     * <p>
     * Which family pairs contain which is
     * {@link WindowAccumulatorDescriptor#derivedSlotOffset}'s answer and is a fact about the
     * two runtime states. Whether a particular owner will <b>carry</b> that fold can be a
     * further question: a live view persists the host's image and hands the guest's decoder a
     * run inside it, so its fold is a claim about two byte codecs as well, and it is withheld
     * unless both are at the version the claim was proved at. An owner with nothing to add
     * passes null and folds wherever the runtime table proves it.
     */
    @FunctionalInterface
    public interface FoldPolicy {
        /**
         * @param host  the wider component the guest would be read out of
         * @param guest the component whose whole state the host provably contains
         * @return true when this owner will carry the fold
         */
        boolean canFold(@NotNull WindowAccumulatorDescriptor host, @NotNull WindowAccumulatorDescriptor guest);
    }
}

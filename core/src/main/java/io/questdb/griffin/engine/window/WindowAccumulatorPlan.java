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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The compiled layout of one window Map group: the accumulator components its map value is
 * made of, where each sits in that value, and which SELECT-list outputs read them.
 * <p>
 * Everything here is runtime-only. There is no manifest, no byte offset and no codec
 * version, because an ordinary query persists nothing: a component's whole address is its
 * slot base in a map value that lives and dies with the cursor. That is deliberate rather
 * than incidental - it is what lets the same merge, fold, canonical order and contributor
 * rule serve a durable owner later without any of them being a promise to a stored root.
 *
 * <h2>Value layout</h2>
 * <pre>
 *   slots 0 .. prefix-1:  the owner's own bookkeeping, if it has any
 *   then:                 components in canonical identity order
 * </pre>
 * The prefix is {@link #getSlotPrefix()} and is zero for an ordinary query, which owns
 * nothing beyond the accumulators and is the only owner this build compiles for. Components
 * are ordered by {@link WindowAccumulatorDescriptor#compareIdentity} and never by SELECT-list
 * order, so reordering the outputs of one query cannot move a component's slot base.
 *
 * <h2>Nothing here is bound</h2>
 * A plan is compiled in shadow: it names the components and the bindings but allocates no
 * map, installs no slots on any function, and changes no plan text. What it does carry is
 * {@link #getStructuralReduction()}, the count of function-owned maps and per-row updates a
 * runtime owner would remove by adopting it, which is what an owner decides on.
 */
public final class WindowAccumulatorPlan {
    private final ObjList<WindowAccumulatorDescriptor> components;
    private final IntList componentSlotBases;
    private final IntList contributorIndexes;
    private final ObjList<WindowFunction> projectionFunctions;
    private final ObjList<WindowAccumulatorProjection> projections;
    private final int slotCount;
    private final int slotPrefix;
    private final WindowMapSpec spec;

    WindowAccumulatorPlan(
            WindowMapSpec spec,
            ObjList<WindowAccumulatorDescriptor> components,
            IntList componentSlotBases,
            IntList contributorIndexes,
            ObjList<WindowAccumulatorProjection> projections,
            ObjList<WindowFunction> projectionFunctions,
            int slotPrefix,
            int slotCount
    ) {
        this.spec = spec;
        this.components = components;
        this.componentSlotBases = componentSlotBases;
        this.contributorIndexes = contributorIndexes;
        this.projections = projections;
        this.projectionFunctions = projectionFunctions;
        this.slotPrefix = slotPrefix;
        this.slotCount = slotCount;
    }

    /**
     * Appends every component's slot types, in canonical order, to {@code types}. The
     * caller has already added its own prefix slots, so what comes back is the group's
     * whole map value and the component slot bases index straight into it.
     */
    public void buildMapValueTypes(@NotNull ArrayColumnTypes types) {
        for (int i = 0, n = components.size(); i < n; i++) {
            final WindowAccumulatorDescriptor component = components.getQuick(i);
            for (int slot = 0, slots = component.getSlotCount(); slot < slots; slot++) {
                types.add(component.getSlotColumnType(slot));
            }
        }
    }

    public WindowAccumulatorDescriptor getComponent(int index) {
        return components.getQuick(index);
    }

    public int getComponentCount() {
        return components.size();
    }

    /**
     * Returns component {@code index}'s first slot in the group's map value, the owner's
     * prefix already counted in.
     */
    public int getComponentSlotBase(int index) {
        return componentSlotBases.getQuick(index);
    }

    /**
     * Returns the function that updates component {@code index}. Every other projection on
     * that component reads the state this one maintains and writes nothing, which is what
     * stops two outputs over one accumulator counting a row twice.
     * <p>
     * A contributor is never a {@link WindowAccumulatorProjection#isDerived() derived}
     * projection - its own function maintains a narrower state than the component, so it
     * cannot maintain the whole of it - nor a
     * {@link WindowAccumulatorProjection#isPartitionKeyGuarded() guarded} one, whose
     * counter differs from its component's on the NULL-key partition.
     */
    public WindowFunction getContributor(int index) {
        return projectionFunctions.getQuick(contributorIndexes.getQuick(index));
    }

    /**
     * Returns the index into the projection list of component {@code index}'s contributor.
     */
    public int getContributorIndex(int index) {
        return contributorIndexes.getQuick(index);
    }

    public WindowAccumulatorProjection getProjection(int index) {
        return projections.getQuick(index);
    }

    public int getProjectionCount() {
        return projections.size();
    }

    /**
     * Returns the SELECT-list function projection {@code index} belongs to. Held as a
     * non-owning reference: the compiled factory owns every window function, and the plan
     * lives and dies with it.
     */
    public WindowFunction getProjectionFunction(int index) {
        return projectionFunctions.getQuick(index);
    }

    /**
     * The number of value slots the group's components occupy, the owner's prefix already
     * counted in - so it is the whole map value's slot count.
     */
    public int getSlotCount() {
        return slotCount;
    }

    /**
     * The number of slots the group's owner reserved ahead of the components. An ordinary
     * query reserves none, so this is zero for every plan the compiler builds.
     */
    public int getSlotPrefix() {
        return slotPrefix;
    }

    /**
     * The group identity every function of this plan shares. Every plan
     * {@link WindowAccumulatorPlanBuilder#compileGroups} produces carries one: the walk skips
     * a function whose {@link WindowMapSpec} is null and opens a builder per spec over the
     * rest. A {@link WindowMapState} is only ever built over a plan that carries one, since
     * the spec is where its map key comes from.
     * <p>
     * No caller can observe null today. The nullability is the two-argument
     * {@link WindowAccumulatorPlanBuilder} constructor's - it is what admits a spec-less
     * builder - and {@code compileGroups} is the only site in the repository that opens a
     * builder at all, through the one-argument constructor that requires a spec. Both readers
     * say so rather than branch: {@link WindowMapState}'s constructor and
     * {@code CachedWindowMapGroups.isForward} assert the result is non-null.
     */
    public @Nullable WindowMapSpec getSpec() {
        return spec;
    }

    /**
     * How much of the unfused runtime binding this plan removes: one per function-owned map
     * that goes dormant, plus one per per-row accumulator update that two outputs stop
     * making separately.
     * <p>
     * It is what an owner's activation rule reads. A one-function plan scores one - it
     * moves a map rather than removing one - and is worth compiling for a test, but not
     * worth binding a runtime through: the abstraction would cost a query something and buy
     * it nothing.
     */
    public int getStructuralReduction() {
        return projectionFunctions.size() + (projectionFunctions.size() - components.size());
    }
}

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

package io.questdb.cairo.lv;

import io.questdb.griffin.engine.window.WindowAccumulatorDescriptor;
import io.questdb.griffin.engine.window.WindowAccumulatorProjection;
import org.jetbrains.annotations.NotNull;

/**
 * The <b>durable</b> half of one output's binding onto an accumulator component: where the
 * fields the {@link WindowAccumulatorProjection runtime projection} reads as map value
 * slots sit as byte offsets in a fused leaf's scalar payload.
 * <p>
 * Everything about the binding itself - which component this output reads, which slots its
 * fields occupy, whether it is derived or guarded - is the runtime projection's and is read
 * off it rather than restated here. The four fields this class adds are the ones an
 * ordinary query has no use for: the component's offset and length in the payload, and the
 * narrower slice the projecting function's own decoder consumes out of it.
 *
 * <h2>Derived projections</h2>
 * A projection's component need not be the one its own function would have persisted
 * alone. A {@code count(x)} beside a {@code sum(x)} reads the counter the sum already
 * keeps, which the plan admits only where
 * {@link LiveViewAccumulatorDescriptor#derivedStateOffset} proves the count's whole image
 * sits verbatim inside the sum's. {@link #getFunctionStateOffset()} and
 * {@link #getFunctionStateLength()} are that narrower slice, and are what a restore hands
 * the function rather than the component's own bounds.
 *
 * <h2>Not checkpoint-stateless</h2>
 * A derived {@code avg} or {@code count} still depends on every row its component has
 * absorbed, so it keeps its real {@link LiveViewCheckpointDependency} for repair planning
 * even when it owns no checkpoint root of its own. Reporting it as stateless would hand a
 * localized repair a zero-width replay floor and restore wrong state.
 */
public final class LiveViewAccumulatorProjection {
    private final LiveViewAccumulatorDescriptor component;
    private final int componentStateLength;
    private final int componentStateOffset;
    private final LiveViewAccumulatorDescriptor functionComponent;
    private final int functionStateLength;
    private final int functionStateOffset;
    private final int nonNullCountFieldOffset;
    private final WindowAccumulatorProjection runtime;
    private final int sumFieldOffset;

    /**
     * @param kind                 one of the {@link WindowAccumulatorProjection}
     *                             {@code PROJECTION_*} constants
     * @param outputPosition       the SELECT-list index of the projecting function.
     *                             Recorded for diagnostics and for binding the runtime
     *                             output; deliberately <b>not</b> persisted, since a
     *                             recompile may move it without changing the state
     * @param componentIndex       index into the plan's ordered component list
     * @param componentStateOffset the component's offset in the fused scalar payload
     * @param componentSlotBase    the component's first slot in the window's fused
     *                             runtime map value
     * @param component            the component this projection reads
     * @param functionComponent    the component the projecting function persists on its
     *                             own, which is {@code component} unless the plan folded
     *                             it into a wider host
     */
    public LiveViewAccumulatorProjection(
            int kind,
            int outputPosition,
            int componentIndex,
            int componentStateOffset,
            int componentSlotBase,
            @NotNull LiveViewAccumulatorDescriptor component,
            @NotNull LiveViewAccumulatorDescriptor functionComponent
    ) {
        this.runtime = new WindowAccumulatorProjection(
                kind,
                outputPosition,
                componentIndex,
                componentSlotBase,
                component.getRuntime(),
                functionComponent.getRuntime()
        );
        final int derivedOffset = component.derivedStateOffset(functionComponent);
        if (derivedOffset < 0) {
            // Unreachable from the plan, which folds only where the containment holds -
            // and the runtime constructor above has already refused the same fold in
            // slots. Stated here anyway for the one thing it adds: the codec pinning, and
            // therefore the offsets a restore feeds a decoder. A wrong one reads a
            // neighbour's bytes at a length that looks right.
            throw new IllegalArgumentException("live view accumulator projection does not fit its component state");
        }
        this.component = component;
        this.functionComponent = functionComponent;
        this.componentStateOffset = componentStateOffset;
        this.componentStateLength = component.getStateLength();
        this.functionStateOffset = componentStateOffset + derivedOffset;
        this.functionStateLength = functionComponent.getStateLength();
        this.sumFieldOffset = absoluteFieldOffset(
                component,
                componentStateOffset,
                WindowAccumulatorDescriptor.FIELD_SUM
        );
        this.nonNullCountFieldOffset = absoluteFieldOffset(
                component,
                componentStateOffset,
                WindowAccumulatorDescriptor.FIELD_NON_NULL_COUNT
        );
    }

    /**
     * The component this projection reads. Several projections may return the same one.
     */
    public LiveViewAccumulatorDescriptor getComponent() {
        return component;
    }

    public int getComponentIndex() {
        return runtime.getComponentIndex();
    }

    /**
     * The component's first slot in the window's fused runtime map value.
     */
    public int getComponentSlotBase() {
        return runtime.getComponentSlotBase();
    }

    public int getComponentStateLength() {
        return componentStateLength;
    }

    public int getComponentStateOffset() {
        return componentStateOffset;
    }

    /**
     * Returns {@code field}'s offset in the fused scalar payload, or {@code -1} when the
     * component this projection reads carries no such field. The absolute counterpart of
     * {@link LiveViewAccumulatorDescriptor#getFieldOffset(int)}.
     */
    public int getFieldOffset(int field) {
        return absoluteFieldOffset(component, componentStateOffset, field);
    }

    /**
     * Returns {@code field}'s slot in the window's fused runtime map value, or {@code -1}
     * when the component this projection reads carries no such field. This is what a
     * bound function reads its state through.
     */
    public int getFieldSlot(int field) {
        return runtime.getFieldSlot(field);
    }

    /**
     * The component the projecting function would persist on its own, which is
     * {@link #getComponent()} unless the plan folded it into a wider host.
     */
    public LiveViewAccumulatorDescriptor getFunctionComponent() {
        return functionComponent;
    }

    /**
     * Where the projecting function's own state begins in the fused runtime map value.
     * The slot counterpart of {@link #getFunctionStateOffset()}, and it is what a
     * runtime handing the state back to the function's own map reads from: a derived
     * {@code count} takes the host's counter slot and nothing else.
     */
    public int getFunctionSlotBase() {
        return runtime.getFunctionSlotBase();
    }

    /**
     * The width of the projecting function's own whole-state image, which equals
     * {@link #getComponentStateLength()} unless this projection is derived.
     */
    public int getFunctionStateLength() {
        return functionStateLength;
    }

    /**
     * Where the projecting function's own whole-state image begins in the fused scalar
     * payload. A restore hands the function this rather than the component's offset, and
     * requires its decoder to stop exactly {@link #getFunctionStateLength()} bytes later:
     * a derived {@code count} reads the host's counter and nothing else, so the slice it
     * consumes is not the slice the component occupies.
     */
    public int getFunctionStateOffset() {
        return functionStateOffset;
    }

    public int getKind() {
        return runtime.getKind();
    }

    /**
     * Returns the contributing-row counter's offset in the fused scalar payload.
     * Present for every family this class binds.
     */
    public int getNonNullCountFieldOffset() {
        return nonNullCountFieldOffset;
    }

    /**
     * Returns the contributing-row counter's slot in the window's fused runtime map
     * value. Present for every family this class binds, which is why a bound function
     * uses it as its "am I fused" answer.
     */
    public int getNonNullCountSlot() {
        return runtime.getNonNullCountSlot();
    }

    public int getOutputPosition() {
        return runtime.getOutputPosition();
    }

    /**
     * The runtime binding this one persists. Which component the output reads and which
     * slots it reads it through are that projection's answers, not this one's.
     */
    public @NotNull WindowAccumulatorProjection getRuntime() {
        return runtime;
    }

    /**
     * Returns the running sum's offset in the fused scalar payload, or {@code -1} when
     * the component carries none.
     */
    public int getSumFieldOffset() {
        return sumFieldOffset;
    }

    /**
     * Returns the running sum's slot in the window's fused runtime map value, or
     * {@code -1} when the component carries none.
     */
    public int getSumSlot() {
        return runtime.getSumSlot();
    }

    /**
     * Whether this projection reads a component wider than the one its own function
     * persists alone - a {@code count(x)} bound onto the counter inside a
     * {@code sum(x)}. A derived projection is never a component's contributor: its
     * function freezes an image narrower than the component, so it cannot write one.
     */
    public boolean isDerived() {
        return runtime.isDerived();
    }

    /**
     * Whether this output corrects the component's counter with a per-row test on the
     * partition key rather than reading it straight - a {@code count(k)} over the very
     * column the window partitions by.
     */
    public boolean isPartitionKeyGuarded() {
        return runtime.isPartitionKeyGuarded();
    }

    private static int absoluteFieldOffset(LiveViewAccumulatorDescriptor component, int componentStateOffset, int field) {
        final int relative = component.getFieldOffset(field);
        return relative < 0 ? -1 : componentStateOffset + relative;
    }
}

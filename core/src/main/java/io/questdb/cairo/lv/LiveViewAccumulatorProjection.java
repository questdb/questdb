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

import org.jetbrains.annotations.NotNull;

/**
 * Immutable binding of one SELECT-list window function onto the
 * {@link LiveViewAccumulatorDescriptor accumulator component} whose state it reads.
 * <p>
 * A projection owns no state. It names a component, the absolute offsets of the
 * fields it reads inside the fused scalar payload, and the arithmetic that turns
 * those fields into the value the output column emits. Several projections may name
 * one component - that sharing is the whole point of the fused layout - and the
 * component's own identity, not any projection, is what the manifest persists.
 *
 * <h2>Derived projections</h2>
 * A projection's component need not be the one its own function would have persisted
 * alone. A {@code count(x)} beside a {@code sum(x)} reads the counter the sum already
 * keeps, which the plan admits only where
 * {@link LiveViewAccumulatorDescriptor#derivedStateOffset} proves the count's whole
 * image sits verbatim inside the sum's. Such a projection is <b>derived</b>: it
 * contributes nothing, and the slice its function's own decoder reads is narrower than
 * the component - {@link #getFunctionStateOffset()} and
 * {@link #getFunctionStateLength()} are that slice, and are what a restore hands the
 * function rather than the component's own bounds.
 * <p>
 * <b>A projection is not checkpoint-stateless.</b> A derived {@code avg} or
 * {@code count} still depends on every row its component has absorbed, so it keeps
 * its real {@link LiveViewCheckpointDependency} for repair planning even when it owns
 * no checkpoint root of its own. Reporting it as stateless would hand a localized
 * repair a zero-width replay floor and restore wrong state.
 */
public final class LiveViewAccumulatorProjection {
    /**
     * {@code sum / nonNullCount}, or SQL NULL for an empty component.
     */
    public static final int PROJECTION_AVG = 1;
    /**
     * {@code nonNullCount}, which is exact and never NULL.
     */
    public static final int PROJECTION_COUNT = 2;
    /**
     * The default: this output reads no shared accumulator.
     */
    public static final int PROJECTION_NONE = 0;
    /**
     * {@code sum}, or SQL NULL for an empty component.
     */
    public static final int PROJECTION_SUM = 3;
    private final LiveViewAccumulatorDescriptor component;
    private final int componentIndex;
    private final int componentSlotBase;
    private final int componentStateLength;
    private final int componentStateOffset;
    private final LiveViewAccumulatorDescriptor functionComponent;
    private final int functionSlotBase;
    private final int functionStateLength;
    private final int functionStateOffset;
    private final boolean isDerived;
    private final int kind;
    private final int nonNullCountFieldOffset;
    private final int nonNullCountSlot;
    private final int outputPosition;
    private final int sumFieldOffset;
    private final int sumSlot;

    /**
     * @param kind                 one of the {@code PROJECTION_*} constants
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
        if (!isCompatible(component.getFamily(), kind)) {
            throw new IllegalArgumentException("live view accumulator projection does not fit its component family");
        }
        final int derivedOffset = component.derivedStateOffset(functionComponent);
        if (derivedOffset < 0) {
            // Unreachable from the plan, which folds only where the containment holds.
            // Stated here anyway: this constructor is what turns the claim into the
            // offsets a restore feeds a decoder, and a wrong one reads a neighbour's
            // bytes at a length that looks right.
            throw new IllegalArgumentException("live view accumulator projection does not fit its component state");
        }
        this.kind = kind;
        this.outputPosition = outputPosition;
        this.component = component;
        this.functionComponent = functionComponent;
        this.componentIndex = componentIndex;
        this.componentStateOffset = componentStateOffset;
        this.componentSlotBase = componentSlotBase;
        this.componentStateLength = component.getStateLength();
        this.isDerived = derivedOffset != 0 || component.getStateLength() != functionComponent.getStateLength();
        this.functionStateOffset = componentStateOffset + derivedOffset;
        this.functionStateLength = functionComponent.getStateLength();
        this.functionSlotBase = componentSlotBase + component.derivedSlotOffset(functionComponent);
        this.sumFieldOffset = absoluteFieldOffset(component, componentStateOffset, LiveViewAccumulatorDescriptor.FIELD_SUM);
        this.nonNullCountFieldOffset = absoluteFieldOffset(
                component,
                componentStateOffset,
                LiveViewAccumulatorDescriptor.FIELD_NON_NULL_COUNT
        );
        this.sumSlot = absoluteFieldSlot(component, componentSlotBase, LiveViewAccumulatorDescriptor.FIELD_SUM);
        this.nonNullCountSlot = absoluteFieldSlot(
                component,
                componentSlotBase,
                LiveViewAccumulatorDescriptor.FIELD_NON_NULL_COUNT
        );
    }

    /**
     * Whether {@code kind} can be computed from a {@code family} component's fields.
     * <p>
     * {@code PROJECTION_COUNT} fits both families on the arithmetic alone - each
     * carries a {@code nonNullCount} - but that is not on its own a licence to bind a
     * {@code count} onto a {@code sum}'s counter: the two must also share a
     * contribution predicate and an argument, which the component identity is what
     * proves. The plan checks the identity first and reaches this only for a binding
     * that has already passed it.
     */
    public static boolean isCompatible(int family, int kind) {
        switch (kind) {
            case PROJECTION_SUM:
            case PROJECTION_AVG:
                return family == LiveViewAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT;
            case PROJECTION_COUNT:
                return family == LiveViewAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT
                        || family == LiveViewAccumulatorDescriptor.FAMILY_NON_NULL_COUNT;
            default:
                return false;
        }
    }

    /**
     * The component this projection reads. Several projections may return the same one.
     */
    public LiveViewAccumulatorDescriptor getComponent() {
        return component;
    }

    public int getComponentIndex() {
        return componentIndex;
    }

    /**
     * The component's first slot in the window's fused runtime map value.
     */
    public int getComponentSlotBase() {
        return componentSlotBase;
    }

    public int getComponentStateLength() {
        return componentStateLength;
    }

    public int getComponentStateOffset() {
        return componentStateOffset;
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
        return functionSlotBase;
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
        return kind;
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
        return nonNullCountSlot;
    }

    public int getOutputPosition() {
        return outputPosition;
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
        return sumSlot;
    }

    /**
     * Whether this projection reads a component wider than the one its own function
     * persists alone - a {@code count(x)} bound onto the counter inside a
     * {@code sum(x)}. A derived projection is never a component's contributor: its
     * function freezes an image narrower than the component, so it cannot write one.
     */
    public boolean isDerived() {
        return isDerived;
    }

    private static int absoluteFieldOffset(LiveViewAccumulatorDescriptor component, int componentStateOffset, int field) {
        final int relative = component.getFieldOffset(field);
        return relative < 0 ? -1 : componentStateOffset + relative;
    }

    private static int absoluteFieldSlot(LiveViewAccumulatorDescriptor component, int componentSlotBase, int field) {
        final int relative = component.getFieldSlot(field);
        return relative < 0 ? -1 : componentSlotBase + relative;
    }
}

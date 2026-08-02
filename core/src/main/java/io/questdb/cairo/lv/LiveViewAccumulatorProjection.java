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
    private final int componentIndex;
    private final int componentStateLength;
    private final int componentStateOffset;
    private final int kind;
    private final int nonNullCountFieldOffset;
    private final int outputPosition;
    private final int sumFieldOffset;

    /**
     * @param kind                 one of the {@code PROJECTION_*} constants
     * @param outputPosition       the SELECT-list index of the projecting function.
     *                             Recorded for diagnostics and for binding the runtime
     *                             output; deliberately <b>not</b> persisted, since a
     *                             recompile may move it without changing the state
     * @param componentIndex       index into the plan's ordered component list
     * @param componentStateOffset the component's offset in the fused scalar payload
     * @param component            the component this projection reads
     */
    public LiveViewAccumulatorProjection(
            int kind,
            int outputPosition,
            int componentIndex,
            int componentStateOffset,
            @NotNull LiveViewAccumulatorDescriptor component
    ) {
        if (!isCompatible(component.getFamily(), kind)) {
            throw new IllegalArgumentException("live view accumulator projection does not fit its component family");
        }
        this.kind = kind;
        this.outputPosition = outputPosition;
        this.componentIndex = componentIndex;
        this.componentStateOffset = componentStateOffset;
        this.componentStateLength = component.getStateLength();
        this.sumFieldOffset = absoluteFieldOffset(component, componentStateOffset, LiveViewAccumulatorDescriptor.FIELD_SUM);
        this.nonNullCountFieldOffset = absoluteFieldOffset(
                component,
                componentStateOffset,
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

    public int getComponentIndex() {
        return componentIndex;
    }

    public int getComponentStateLength() {
        return componentStateLength;
    }

    public int getComponentStateOffset() {
        return componentStateOffset;
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

    private static int absoluteFieldOffset(LiveViewAccumulatorDescriptor component, int componentStateOffset, int field) {
        final int relative = component.getFieldOffset(field);
        return relative < 0 ? -1 : componentStateOffset + relative;
    }
}

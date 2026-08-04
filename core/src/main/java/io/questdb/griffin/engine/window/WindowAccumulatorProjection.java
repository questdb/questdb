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

import org.jetbrains.annotations.NotNull;

/**
 * Immutable binding of one SELECT-list window function onto the
 * {@link WindowAccumulatorDescriptor accumulator component} whose state it reads.
 * <p>
 * A projection owns no state. It names a component, the map value slots of the fields it
 * reads, and the arithmetic that turns those fields into the value the output column
 * emits. Several projections may name one component - that sharing is the whole point of
 * the fused layout - and the component's own identity, not any projection, is what a
 * group is laid out by.
 * <p>
 * Slot indexes rather than byte offsets are what a runtime binding carries. The durable
 * counterpart - where a component's image starts in a persisted payload, and how long the
 * projecting function's own slice is - lives in {@code LiveViewAccumulatorProjection},
 * which wraps one of these.
 *
 * <h2>Derived projections</h2>
 * A projection's component need not be the one its own function would have kept alone. A
 * {@code count(x)} beside a {@code sum(x)} reads the counter the sum already keeps, which
 * a plan admits only where {@link WindowAccumulatorDescriptor#derivedSlotOffset} proves
 * the count's whole state sits verbatim inside the sum's. Such a projection is
 * <b>derived</b>: it contributes nothing, and the slice its own function would own is
 * narrower than the component - {@link #getFunctionSlotBase()} is where that slice starts,
 * and is what a runtime handing the state back reads from.
 *
 * <h2>Guarded projections</h2>
 * One projection does not read its component's fields at all but corrects them with a
 * per-row test: a {@code count(k)} over the column its own window partitions by emits
 * {@code partition-key-is-null ? 0 : rowCount}, so it shares the row-count component
 * {@code count(*)} keeps instead of keeping a counter of its own.
 * {@link #isPartitionKeyGuarded()} is what marks it, and it is the one projection kind
 * whose value is not a function of the state alone.
 * <p>
 * <b>A projection is not stateless.</b> A derived {@code avg} or {@code count} still
 * depends on every row its component has absorbed; what it does not have is state of its
 * own to maintain.
 */
public final class WindowAccumulatorProjection {
    /**
     * {@code sum / nonNullCount}, or SQL NULL for an empty component.
     */
    public static final int PROJECTION_AVG = 1;
    /**
     * The contributing-row counter, which is exact and never NULL. It is what a
     * {@code count} emits, and equally what a partitioned {@code row_number()} emits off
     * a row-count component: after {@code n} rows the running number and the running
     * count are the same number.
     */
    public static final int PROJECTION_COUNT = 2;
    /**
     * {@code partition-key-is-null ? 0 : rowCount} - what a {@code count(k)} emits when
     * {@code k} is the window's own partition key.
     * <p>
     * Every row of a partition carries the same {@code k}, so the counter such a call
     * keeps is the partition's row count wherever {@code k} is present and zero where it
     * is not. That makes it a reading of a {@link WindowAccumulatorDescriptor#FAMILY_ROW_COUNT}
     * component rather than a component of its own - the same component {@code count(*)}
     * and {@code row_number()} maintain - with the guard supplied per row from the
     * argument rather than from the state.
     * <p>
     * The guard is what keeps it out of {@link #PROJECTION_COUNT}: the NULL-key partition
     * exists and its {@code count(k)} is zero there while its row count is not, so an
     * unguarded reading of the same slot would be wrong for exactly one partition.
     */
    public static final int PROJECTION_COUNT_PARTITION_KEY = 8;
    /**
     * The component's one slot, read straight - what a {@code max} or a {@code min} emits.
     * <p>
     * One kind for both directions, unlike the families it reads: which way the extremum
     * points is decided when the state is <b>maintained</b>, so by the time an output reads
     * the slot there is nothing left to choose. That is also why it needs no empty-state
     * test the way {@link #PROJECTION_SUM} does - an extremum's identity is already the NULL
     * its own type emits.
     */
    public static final int PROJECTION_EXTREMUM = 9;
    /**
     * The default: this output reads no shared accumulator.
     */
    public static final int PROJECTION_NONE = 0;
    /**
     * {@code sqrt(m2 / n)}, the population standard deviation.
     */
    public static final int PROJECTION_STDDEV_POP = 4;
    /**
     * {@code sqrt(m2 / (n - 1))}, the sample standard deviation.
     */
    public static final int PROJECTION_STDDEV_SAMP = 5;
    /**
     * {@code sum}, or SQL NULL for an empty component.
     */
    public static final int PROJECTION_SUM = 3;
    /**
     * {@code m2 / n}, the population variance.
     */
    public static final int PROJECTION_VAR_POP = 6;
    /**
     * {@code m2 / (n - 1)}, the sample variance.
     */
    public static final int PROJECTION_VAR_SAMP = 7;
    private final WindowAccumulatorDescriptor component;
    private final int componentIndex;
    private final int componentSlotBase;
    private final WindowAccumulatorDescriptor functionComponent;
    private final int functionSlotBase;
    private final boolean isDerived;
    private final int kind;
    private final int nonNullCountSlot;
    private final int outputPosition;
    private final int sumSlot;

    /**
     * @param kind              one of the {@code PROJECTION_*} constants
     * @param outputPosition    the SELECT-list index of the projecting function.
     *                          Recorded for diagnostics and for binding the runtime
     *                          output
     * @param componentIndex    index into the plan's ordered component list
     * @param componentSlotBase the component's first slot in the group's fused map value
     * @param component         the component this projection reads
     * @param functionComponent the component the projecting function would keep on its
     *                          own, which is {@code component} unless the plan folded it
     *                          into a wider host
     */
    public WindowAccumulatorProjection(
            int kind,
            int outputPosition,
            int componentIndex,
            int componentSlotBase,
            @NotNull WindowAccumulatorDescriptor component,
            @NotNull WindowAccumulatorDescriptor functionComponent
    ) {
        if (!isCompatible(component.getFamily(), kind)) {
            throw new IllegalArgumentException("window accumulator projection does not fit its component family");
        }
        final int derivedSlotOffset = component.derivedSlotOffset(functionComponent);
        if (derivedSlotOffset < 0) {
            // Unreachable from a plan, which folds only where the containment holds.
            // Stated here anyway: this constructor is what turns the claim into the slots
            // a runtime reads, and a wrong one reads a neighbour's field at a width that
            // looks right.
            throw new IllegalArgumentException("window accumulator projection does not fit its component state");
        }
        this.kind = kind;
        this.outputPosition = outputPosition;
        this.component = component;
        this.functionComponent = functionComponent;
        this.componentIndex = componentIndex;
        this.componentSlotBase = componentSlotBase;
        this.isDerived = derivedSlotOffset != 0 || component.getSlotCount() != functionComponent.getSlotCount();
        this.functionSlotBase = componentSlotBase + derivedSlotOffset;
        this.sumSlot = absoluteFieldSlot(component, componentSlotBase, WindowAccumulatorDescriptor.FIELD_SUM);
        this.nonNullCountSlot = absoluteFieldSlot(
                component,
                componentSlotBase,
                WindowAccumulatorDescriptor.FIELD_NON_NULL_COUNT
        );
    }

    /**
     * Whether {@code kind} can be computed from a {@code family} component's fields.
     * <p>
     * {@code PROJECTION_COUNT} fits several families on the arithmetic alone - each
     * carries a {@code nonNullCount} - but that is not on its own a licence to bind a
     * {@code count} onto a {@code sum}'s counter: the two must also share a
     * contribution predicate and an argument, which the component identity is what
     * proves. A plan checks the identity first and reaches this only for a binding
     * that has already passed it.
     */
    public static boolean isCompatible(int family, int kind) {
        switch (kind) {
            case PROJECTION_SUM:
            case PROJECTION_AVG:
                return family == WindowAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT;
            case PROJECTION_COUNT:
                // Every family carries a counter, and the Welford one is here because a
                // count(x) folded onto a stddev(x) reads that stddev's counter. Which
                // counter a given call may read is still the component identity's answer,
                // not this one's.
                return family == WindowAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT
                        || family == WindowAccumulatorDescriptor.FAMILY_DOUBLE_WELFORD
                        || family == WindowAccumulatorDescriptor.FAMILY_NON_NULL_COUNT
                        || family == WindowAccumulatorDescriptor.FAMILY_ROW_COUNT;
            case PROJECTION_EXTREMUM:
                // The four extremum families and nothing else. Every one of them carries the
                // single slot this kind reads, and no other family does.
                return family == WindowAccumulatorDescriptor.FAMILY_DOUBLE_MAX
                        || family == WindowAccumulatorDescriptor.FAMILY_DOUBLE_MIN
                        || family == WindowAccumulatorDescriptor.FAMILY_LONG_MAX
                        || family == WindowAccumulatorDescriptor.FAMILY_LONG_MIN;
            case PROJECTION_COUNT_PARTITION_KEY:
                // The row count alone. A guarded reading of a non-null count would be
                // either a tautology or a contradiction - that counter already applies the
                // argument's own predicate - so the kind names the one family whose counter
                // it corrects.
                return family == WindowAccumulatorDescriptor.FAMILY_ROW_COUNT;
            case PROJECTION_STDDEV_POP:
            case PROJECTION_STDDEV_SAMP:
            case PROJECTION_VAR_POP:
            case PROJECTION_VAR_SAMP:
                return family == WindowAccumulatorDescriptor.FAMILY_DOUBLE_WELFORD;
            default:
                return false;
        }
    }

    /**
     * The component this projection reads. Several projections may return the same one.
     */
    public WindowAccumulatorDescriptor getComponent() {
        return component;
    }

    public int getComponentIndex() {
        return componentIndex;
    }

    /**
     * The component's first slot in the group's fused map value.
     */
    public int getComponentSlotBase() {
        return componentSlotBase;
    }

    /**
     * Returns {@code field}'s slot in the group's fused map value, or {@code -1} when the
     * component this projection reads carries no such field. This is what a bound
     * function reads its state through, and it is generic in the field because a family
     * wider than {@code (sum, count)} - Welford's, say - has fields no named getter here
     * would cover.
     */
    public int getFieldSlot(int field) {
        return absoluteFieldSlot(component, componentSlotBase, field);
    }

    /**
     * The component the projecting function would keep on its own, which is
     * {@link #getComponent()} unless the plan folded it into a wider host.
     */
    public WindowAccumulatorDescriptor getFunctionComponent() {
        return functionComponent;
    }

    /**
     * Where the projecting function's own state begins in the group's fused map value.
     * A runtime handing the state back to the function's own map reads from here: a
     * derived {@code count} takes the host's counter slot and nothing else.
     */
    public int getFunctionSlotBase() {
        return functionSlotBase;
    }

    public int getKind() {
        return kind;
    }

    /**
     * Returns the contributing-row counter's slot in the group's fused map value, or
     * {@code -1} when the component carries none - which the extremum families do not.
     * <p>
     * It was once every family's, and so served as a bound function's "am I fused" answer.
     * That is now {@link #getComponentSlotBase()}, which every binding has by construction.
     */
    public int getNonNullCountSlot() {
        return nonNullCountSlot;
    }

    public int getOutputPosition() {
        return outputPosition;
    }

    /**
     * Returns the running sum's slot in the group's fused map value, or {@code -1} when
     * the component carries none.
     */
    public int getSumSlot() {
        return sumSlot;
    }

    /**
     * Whether this projection reads a component wider than the one its own function
     * would keep alone - a {@code count(x)} bound onto the counter inside a
     * {@code sum(x)}. A derived projection is never a component's contributor: its
     * function maintains a narrower state than the component, so it cannot maintain the
     * whole of it.
     */
    public boolean isDerived() {
        return isDerived;
    }

    /**
     * Whether this output corrects the component's counter with a per-row test on the
     * partition key rather than reading it straight - a {@code count(k)} over the very
     * column the window partitions by.
     * <p>
     * Such a projection is the one shape whose emitted value is not a function of the
     * state alone, and two things follow from that. It must never be chosen as its
     * component's <b>contributor</b>: the counter it would keep on its own is the
     * partition's {@code count(k)} and the component's is the partition's row count,
     * and those differ on the NULL-key partition. And a runtime handing the state back
     * to the function's private map must apply the guard rather than copy the slot,
     * for the same reason.
     */
    public boolean isPartitionKeyGuarded() {
        return kind == PROJECTION_COUNT_PARTITION_KEY;
    }

    private static int absoluteFieldSlot(WindowAccumulatorDescriptor component, int componentSlotBase, int field) {
        final int relative = component.getFieldSlot(field);
        return relative < 0 ? -1 : componentSlotBase + relative;
    }
}

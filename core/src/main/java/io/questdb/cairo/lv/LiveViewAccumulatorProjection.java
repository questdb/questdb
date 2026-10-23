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
 * Durable byte offsets for a {@link WindowAccumulatorProjection}'s runtime binding.
 * The runtime projection owns component, slot and derivation decisions; this class
 * adds the fused payload offset and the slice consumed by the function's decoder.
 * <p>
 * Derived slices require {@link LiveViewAccumulatorDescriptor#derivedStateOffset} to
 * prove codec containment. They retain their real repair dependency even without a
 * separate root: derived state is not checkpoint-stateless.
 * <p>
 * Runtime-only components persist through their own function roots and carry
 * {@link #NOT_PERSISTED} for fused byte offsets. Callers must check {@link #isDurable()}
 * before reading those offsets.
 */
public final class LiveViewAccumulatorProjection {
    /**
     * The offset of a projection whose state the fused payload does not carry. Every byte
     * offset on a runtime-only projection is this: a group's map value holds its slots, and
     * the manifest does not name them.
     */
    public static final int NOT_PERSISTED = -1;
    private final LiveViewAccumulatorDescriptor component;
    private final int componentStateOffset;
    private final LiveViewAccumulatorDescriptor functionComponent;
    private final int functionStateLength;
    private final int functionStateOffset;
    private final int nonNullCountFieldOffset;
    private final WindowAccumulatorProjection runtime;

    /**
     * Wraps the runtime binding a compiled plan already made in the byte facts a live view
     * persists it with. The binding itself - the kind, the output position, the component
     * index and every slot - is {@code runtime}'s and is not re-decided here.
     *
     * @param runtime              the runtime projection this one persists
     * @param componentStateOffset the component's offset in the fused scalar payload, or
     *                             {@link #NOT_PERSISTED} when the leaf budget left the
     *                             component out of the manifest and the projecting function
     *                             keeps a root of its own
     * @param component            the durable half of the component {@code runtime} reads
     * @param functionComponent    the durable half of the component the projecting function
     *                             persists on its own, which is {@code component} unless the
     *                             plan folded it into a wider host
     */
    public LiveViewAccumulatorProjection(
            @NotNull WindowAccumulatorProjection runtime,
            int componentStateOffset,
            @NotNull LiveViewAccumulatorDescriptor component,
            @NotNull LiveViewAccumulatorDescriptor functionComponent
    ) {
        if (!component.getRuntime().isSameIdentity(runtime.getComponent())
                || !functionComponent.getRuntime().isSameIdentity(runtime.getFunctionComponent())) {
            // The two halves have to describe one binding: the byte offsets below are this
            // component's and the slots are that one's, and a payload sliced by one while the
            // map value is read through the other is a silent misread rather than a failure.
            throw new IllegalArgumentException("live view accumulator projection does not describe its runtime binding");
        }
        this.runtime = runtime;
        final int derivedOffset = component.derivedStateOffset(functionComponent);
        if (derivedOffset < 0) {
            // Unreachable from the plan, which folds only where the containment holds - and
            // the runtime binding was refused the same fold in slots when it was built.
            // Stated here anyway for the one thing it adds: the codec pinning, and therefore
            // the offsets a restore feeds a decoder. A wrong one reads a neighbour's bytes at
            // a length that looks right.
            throw new IllegalArgumentException("live view accumulator projection does not fit its component state");
        }
        this.component = component;
        this.functionComponent = functionComponent;
        this.componentStateOffset = componentStateOffset;
        this.functionStateOffset = componentStateOffset == NOT_PERSISTED
                ? NOT_PERSISTED
                : componentStateOffset + derivedOffset;
        this.functionStateLength = functionComponent.getStateLength();
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
        return runtime.getFunctionSlotBase();
    }

    /**
     * The width of the projecting function's own whole-state image, which equals the
     * component's state length unless this projection is derived.
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

    /**
     * The runtime binding this one persists. Which component the output reads and which
     * slots it reads it through are that projection's answers, not this one's.
     */
    public @NotNull WindowAccumulatorProjection getRuntime() {
        return runtime;
    }

    /**
     * Whether the fused payload carries this projection's component, and so whether any of
     * the byte offsets above mean anything. False for a runtime-only member: its slots are
     * in the group's map value and its bytes are on its own function root.
     */
    public boolean isDurable() {
        return componentStateOffset != NOT_PERSISTED;
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
        if (componentStateOffset == NOT_PERSISTED) {
            return NOT_PERSISTED;
        }
        final int relative = component.getFieldOffset(field);
        return relative < 0 ? -1 : componentStateOffset + relative;
    }
}

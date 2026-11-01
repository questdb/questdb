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

import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.map.Map;
import io.questdb.griffin.engine.window.WindowAccumulatorDescriptor;
import io.questdb.griffin.engine.window.WindowAccumulatorPlan;
import io.questdb.griffin.engine.window.WindowAccumulatorPlanBuilder;
import io.questdb.griffin.engine.window.WindowAccumulatorProjection;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * Durable layout of a live view's fused window state, rendered from its
 * {@link WindowAccumulatorPlan}. Component sharing and containment follow the runtime
 * plan; persistence adds codec compatibility, canonical order and an inline budget.
 * A partition-key count projection never contributes to the shared row-count state.
 * <p>
 * The scalar payload starts with an eight-byte anchor, followed by components in
 * encoded-identity order, with no state page references. This order survives projection
 * reordering. The prefix fitting {@link LiveViewCheckpointContracts#MAX_INLINE_LEAF_STATE_BYTES}
 * is durable in the fused root; the remaining components share the runtime map but
 * persist through their functions' own roots. Residual functions keep their own map
 * and root. Together these categories cover the entire factory.
 * <p>
 * The manifest, window identity and key schema must match a predecessor before its
 * leaves can be reused. Runtime slot bases and durable offsets derive from the same
 * component order.
 */
public final class LiveViewWindowStatePlan {
    /**
     * The anchor value's width in the fused scalar payload. It is a LONG for every
     * anchor type the runtime admits - {@code LiveViewWindow} widens an INT anchor
     * into the same slot - so the payload's shape does not vary with it.
     */
    public static final int ANCHOR_STATE_BYTES = Long.BYTES;
    /**
     * The anchor value leads the payload. Fixed rather than derived so a decoder can
     * read it before it has looked at a single component.
     */
    public static final int ANCHOR_STATE_OFFSET = 0;
    /**
     * The window's own value slots - anchor value, initialized flag, tombstone and the
     * cadence a key was last marked dirty in - which lead the fused runtime map value
     * exactly as the anchor value leads the fused scalar payload. {@code LiveViewWindow}
     * defines those slots and reads this constant back for its value layout, so the two
     * cannot disagree about where the components start.
     * <p>
     * Runtime only. The durable payload is laid out from {@link #ANCHOR_STATE_OFFSET} and
     * {@link #ANCHOR_STATE_BYTES} and carries no slot index, so a slot added here changes
     * no encoding and no checkpoint written by another build.
     */
    public static final int WINDOW_VALUE_SLOT_COUNT = 4;
    private static final int WINDOW_IDENTITY_FORMAT_VERSION = 1;
    private static final int WINDOW_IDENTITY_MAGIC = 0x4c565749; // LVWI
    private final ObjList<LiveViewAccumulatorDescriptor> components;
    private final int durableComponentCount;
    private final ColumnTypes keyColumnTypes;
    private final LiveViewWindowStateManifest manifest;
    private final ObjList<LiveViewAccumulatorProjection> projections;
    private final ObjList<WindowFunction> residualFunctions;
    /**
     * The runtime plan this one renders, index-aligned with it in components and
     * projections; its slot bases, contributors and projection functions are this plan's.
     * Null for an anchor-only plan, which has no component or projection to index it by.
     */
    private final WindowAccumulatorPlan runtimePlan;
    private final int totalInlineStateBytes;
    private final int totalRuntimeStateBytes;
    private final byte[] windowIdentity;

    private LiveViewWindowStatePlan(
            byte[] windowIdentity,
            ColumnTypes keyColumnTypes,
            ObjList<LiveViewAccumulatorDescriptor> components,
            int durableComponentCount,
            WindowAccumulatorPlan runtimePlan,
            ObjList<LiveViewAccumulatorProjection> projections,
            ObjList<WindowFunction> residualFunctions,
            LiveViewWindowStateManifest manifest,
            int totalInlineStateBytes,
            int totalRuntimeStateBytes
    ) {
        this.windowIdentity = windowIdentity;
        this.keyColumnTypes = keyColumnTypes;
        this.components = components;
        this.durableComponentCount = durableComponentCount;
        this.runtimePlan = runtimePlan;
        this.projections = projections;
        this.residualFunctions = residualFunctions;
        this.manifest = manifest;
        this.totalInlineStateBytes = totalInlineStateBytes;
        this.totalRuntimeStateBytes = totalRuntimeStateBytes;
    }

    /**
     * Encodes the canonical identity of the window group a fused root belongs to.
     * Length-prefixed throughout, so a delimiter inside a SQL identifier or an
     * expression rendering cannot alias another window's identity.
     */
    public static byte[] encodeWindowIdentity(
            @NotNull CharSequence canonicalWindowName,
            @NotNull CharSequence partitionSignature,
            @NotNull CharSequence orderSignature
    ) {
        final long size = 5L * Integer.BYTES
                + LiveViewCheckpointMetadata.utf8Bytes(canonicalWindowName)
                + LiveViewCheckpointMetadata.utf8Bytes(partitionSignature)
                + LiveViewCheckpointMetadata.utf8Bytes(orderSignature);
        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("live view window identity is too large");
        }
        final byte[] encoded = new byte[(int) size];
        int offset = LiveViewCheckpointMetadata.putInt(encoded, 0, WINDOW_IDENTITY_MAGIC);
        offset = LiveViewCheckpointMetadata.putInt(encoded, offset, WINDOW_IDENTITY_FORMAT_VERSION);
        offset = putIdentityField(encoded, offset, canonicalWindowName);
        offset = putIdentityField(encoded, offset, partitionSignature);
        putIdentityField(encoded, offset, orderSignature);
        return encoded;
    }

    private static int putIdentityField(byte[] sink, int offset, CharSequence value) {
        offset = LiveViewCheckpointMetadata.putInt(sink, offset, LiveViewCheckpointMetadata.utf8Bytes(value));
        return LiveViewCheckpointMetadata.putUtf8(sink, offset, value);
    }

    /**
     * Installs each projection's fused slots on the function that emits it, which is
     * what turns the group's hot-path state methods into no-ops: from here the window's
     * one map value carries the accumulators and the functions only read it.
     * <p>
     * A function is handed the {@link LiveViewAccumulatorProjection#getRuntime() runtime}
     * half of the binding, which is the slots alone: where the same component's image sits
     * in a persisted payload is this plan's business and the seal's, never the row loop's.
     */
    public void bindProjectionFunctions() {
        for (int i = 0, n = projections.size(); i < n; i++) {
            getProjectionFunction(i).bindWindowStateSlots(projections.getQuick(i).getRuntime());
        }
    }

    public LiveViewAccumulatorDescriptor getComponent(int index) {
        return components.getQuick(index);
    }

    /**
     * The whole group's component count - the durable prefix and the runtime-only tail
     * together. It is the map value's shape, so every runtime walk takes it: the value
     * layout, the reset, the contributor loop and both directions of adoption.
     */
    public int getComponentCount() {
        return components.size();
    }

    /**
     * Returns component {@code index}'s first slot in the window's fused runtime map
     * value. The slot counterpart of the manifest's state offset.
     */
    public int getComponentSlotBase(int index) {
        return runtimePlan.getComponentSlotBase(index);
    }

    /**
     * Returns the function that updates component {@code index}. Every other
     * projection on that component is a read-only reader of the same state. The runtime
     * plan picks it deterministically - the lowest output position among the compatible
     * contributors - so no owner output position ever reaches disk.
     * <p>
     * A contributor is always a projection whose own function persists that exact
     * component, never a {@link WindowAccumulatorProjection#isDerived() derived} one:
     * a {@code count} folded onto a sum's counter freezes eight bytes and the component
     * is sixteen, so it could not write the image even though it can read it. Nor a
     * {@link LiveViewAccumulatorProjection#isPartitionKeyGuarded() guarded} one, whose
     * image is the same eight bytes as its component's and still a different number.
     */
    public WindowFunction getContributor(int index) {
        return runtimePlan.getContributor(index);
    }

    /**
     * How many of {@link #getComponentCount()}'s components the fused leaf carries, which
     * is the prefix of the canonical order that fits the leaf budget. Every walk that
     * reads or writes a persisted payload takes this rather than the whole count; the
     * components past it are the group's at runtime and their own functions' on disk.
     */
    public int getDurableComponentCount() {
        return durableComponentCount;
    }

    public LiveViewWindowStateManifest getManifest() {
        return manifest;
    }

    public LiveViewAccumulatorProjection getProjection(int index) {
        return projections.getQuick(index);
    }

    public int getProjectionCount() {
        return projections.size();
    }

    /**
     * Returns the SELECT-list function projection {@code index} belongs to. Held as a
     * non-owning reference: the compiled factory owns every window function, and the
     * plan lives and dies with it.
     */
    public WindowFunction getProjectionFunction(int index) {
        return runtimePlan.getProjectionFunction(index);
    }

    /**
     * Returns the window functions this plan does not group at all. Each keeps both its own
     * map and its own legacy function root - a ring-backed RANGE function, a bounded ROWS
     * accumulator, an expression argument, a DECIMAL {@code sum}. "One B-tree per window"
     * therefore means one tree for the durable components plus independent roots for these
     * and for every runtime-only member.
     * <p>
     * A component the leaf budget left out is <b>not</b> here: its projection is a
     * runtime-only member of the group, which is a different thing from a function the
     * group never took - see {@link #isDurableProjection(int)}. In SELECT-list order,
     * because that is the order they arrived in.
     */
    public ObjList<WindowFunction> getResidualFunctions() {
        return residualFunctions;
    }

    /**
     * The whole fused scalar payload's width, anchor value included. The durable prefix's
     * width: a runtime-only member contributes nothing to a leaf.
     */
    public int getTotalInlineStateBytes() {
        return totalInlineStateBytes;
    }

    /**
     * The width of one entry's whole component image - every component, durable or not, in
     * the plan's canonical order. It is what the in-RAM repair overlay carries the group's
     * state across in, which has to be all of it: a runtime-only member's accumulator lives
     * in the same map value and no function root is consulted during a repair.
     */
    public int getTotalRuntimeStateBytes() {
        return totalRuntimeStateBytes;
    }

    /**
     * Returns an owned copy of the canonical window group identity.
     */
    public byte[] getWindowIdentity() {
        return Arrays.copyOf(windowIdentity, windowIdentity.length);
    }

    /**
     * Borrows the immutable compiled window identity. The compiled factory owns this
     * plan and array; package-internal callers must not mutate or outlive that factory.
     */
    byte[] borrowWindowIdentity() {
        return windowIdentity;
    }

    /**
     * Returns the projection {@code function} emits, or {@code -1} when the group does not
     * carry it. Identity comparison: the plan holds non-owning references to the very
     * functions the factory compiled, so two distinct functions are two distinct outputs
     * however alike their identities read.
     */
    public int indexOfProjectionFunction(WindowFunction function) {
        for (int i = 0, n = projections.size(); i < n; i++) {
            if (getProjectionFunction(i) == function) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Whether projection {@code index} is the function that maintains its own component,
     * and so the one whose root holds that component's whole image. It is what a restore
     * of a runtime-only member's root asks before writing anything into the group's slots:
     * a derived or guarded member's root holds a narrower or corrected number that is not
     * the component's state, and the contributor's root holds the state itself.
     */
    public boolean isContributor(int index) {
        final int componentIndex = projections.getQuick(index).getComponentIndex();
        return runtimePlan.getContributorIndex(componentIndex) == index;
    }

    /**
     * Whether projection {@code index}'s state is in the fused payload rather than on the
     * function root its own function keeps. False for a runtime-only member, which is
     * grouped in the map and separate on disk.
     */
    public boolean isDurableProjection(int index) {
        return projections.getQuick(index).isDurable();
    }

    /**
     * Whether {@code other} is the same partition-key layout the grouped functions
     * key their state by. {@code LiveViewWindow} calls this before adopting the plan:
     * the fused entry's key comes from the anchor map, so a plan whose components are
     * keyed differently describes state that map cannot address.
     */
    public boolean isKeyLayoutCompatible(@Nullable ColumnTypes other) {
        return isSameLayout(keyColumnTypes, other);
    }

    /**
     * Whether {@code other} carries the same encoded window group identity.
     */
    public boolean isSameWindowIdentity(byte @Nullable [] other) {
        return Arrays.equals(windowIdentity, other);
    }

    /**
     * Closes every projection function's private partition map, which is the whole of
     * what those maps hold once the window owns the group's state.
     * <p>
     * Closing rather than clearing: a cleared map keeps its backing, and that backing is
     * charged to the per-view tracker for the view's life against state nothing writes.
     * The maps were allocated under the tracker and are freed under it, so the two stay
     * symmetric.
     */
    public void releaseProjectionMaps() {
        for (int i = 0, n = projections.size(); i < n; i++) {
            final Map map = getProjectionFunction(i).getPartitionMap();
            if (map != null && map.isOpen()) {
                map.close();
            }
        }
    }

    /**
     * Opens and empties every projection function's private partition map, so state can
     * be put back into it - the legacy-checkpoint adapter reads a per-function root into
     * one before hoisting it into the fused value, and declining the plan hands the
     * group's state back the same way.
     */
    public void reopenProjectionMaps() {
        for (int i = 0, n = projections.size(); i < n; i++) {
            final Map map = getProjectionFunction(i).getPartitionMap();
            if (map != null) {
                map.reopen();
                map.clear();
            }
        }
    }

    /**
     * Ends every projection function's own incremental checkpoint baseline, because the
     * group taking their state over or handing it back has changed which dirty set the
     * keys they touch are recorded in.
     * <p>
     * A {@link #isDurableProjection(int) durable} member is covered twice over - it holds
     * no root at all while it is one, and the seal refuses to build on a predecessor that
     * has none - but a <b>runtime-only</b> member keeps its root across the move while its
     * keys go into the window's dirty set, and its own set stands empty for as long as the
     * group owns it. Freezing that set after the move would publish a root missing every
     * key the group touched. The two cases are not distinguished here: an eligibility
     * change is rare and one complete freeze per function is what it costs.
     */
    public void requireProjectionCheckpointFullScan() {
        for (int i = 0, n = projections.size(); i < n; i++) {
            getProjectionFunction(i).requireCheckpointFullScan();
        }
    }

    /**
     * Takes the fused slots off every projection's function, putting each one back on
     * the private map and the per-row accumulator update it owns outside a fused group.
     * The state itself is the caller's to move; this only changes who is asked for it.
     */
    public void unbindProjectionFunctions() {
        for (int i = 0, n = projections.size(); i < n; i++) {
            getProjectionFunction(i).bindWindowStateSlots(null);
        }
    }

    private static boolean isSameLayout(@Nullable ColumnTypes a, @Nullable ColumnTypes b) {
        if (a == null || b == null) {
            return a == b;
        }
        final int n = a.getColumnCount();
        if (n != b.getColumnCount()) {
            return false;
        }
        for (int i = 0; i < n; i++) {
            if (a.getColumnType(i) != b.getColumnType(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Renders the runtime plan's sharing decisions into durable offsets, a manifest and
     * an inline-budget prefix. Rejected candidates remain residual functions with their
     * own state and roots.
     */
    public static final class Builder {
        /**
         * A durable owner carries a fold only where both sides' component codecs are at the
         * version the containment was proved byte for byte at. Which pairs contain which is
         * the runtime table's answer; this is the further question a persisted layout has to
         * ask, since the host writes the image and the guest's own decoder reads a run inside
         * it.
         */
        private static final WindowAccumulatorPlanBuilder.FoldPolicy DURABLE_FOLD_POLICY =
                (host, guest) -> LiveViewAccumulatorDescriptor.isContainmentProofCodec(host.getFamily())
                        && LiveViewAccumulatorDescriptor.isContainmentProofCodec(guest.getFamily());
        /**
         * The durable half of every component offered to the group, one entry per distinct
         * identity. The runtime plan comes back naming runtime components, and these are what
         * they are persisted through - kept rather than rebuilt so that one durable descriptor
         * serves the manifest, the layout and every projection that reads it.
         */
        private final ObjList<LiveViewAccumulatorDescriptor> offeredComponents = new ObjList<>();
        private final ObjList<WindowFunction> residualFunctions = new ObjList<>();
        private final WindowAccumulatorPlanBuilder runtimeBuilder =
                new WindowAccumulatorPlanBuilder(null, DURABLE_FOLD_POLICY);
        private ColumnTypes keyColumnTypes;
        private byte[] windowIdentity;

        /**
         * Offers {@code function}'s {@code component} to the group, and reports whether it
         * joined.
         * <p>
         * Three durable gates run here and the rest of the answer is the runtime builder's.
         * A projection is declined when its key layout is empty; when the contributing
         * implementation's declared fixed width does not equal the family's state length -
         * the manifest would then name a slice the runtime image does not fill, and the leaf
         * carries no length of its own to catch it; and when it disagrees with the group's
         * window identity or key layout, which {@link #ofWindow} fixed from the anchored
         * window itself. A projection disagreeing with either belongs to a different window
         * group.
         *
         * @return true when the projection joined the group
         */
        public boolean addProjection(
                @NotNull WindowFunction function,
                @NotNull LiveViewAccumulatorDescriptor component,
                int projectionKind,
                int outputPosition,
                byte @NotNull [] candidateWindowIdentity,
                @Nullable ColumnTypes candidateKeyColumnTypes
        ) {
            if (candidateKeyColumnTypes == null || candidateKeyColumnTypes.getColumnCount() == 0) {
                return false;
            }
            if (component.getStateLength() != function.checkpointStateFixedLength()) {
                return false;
            }
            if (windowIdentity == null
                    || !Arrays.equals(windowIdentity, candidateWindowIdentity)
                    || !isSameLayout(keyColumnTypes, candidateKeyColumnTypes)) {
                return false;
            }
            if (!runtimeBuilder.addProjection(function, component.getRuntime(), projectionKind, outputPosition)) {
                return false;
            }
            rememberDurableComponent(component);
            return true;
        }

        /**
         * Records a window function the group does not carry, so the plan can report
         * the whole factory rather than only the part it fused.
         */
        public void addResidual(@NotNull WindowFunction function) {
            residualFunctions.add(function);
        }

        /**
         * Names the anchored window this plan describes, before any projection is offered.
         * <p>
         * The identity and key schema are the anchored window's own rather than any
         * projection's, so a window whose every function is a residual still has them - an
         * anchored DECIMAL {@code sum} is the standing example. A plan is the storage
         * decision for the window, and a window that cannot say which one it is could not
         * be compared against a predecessor root at all.
         * <p>
         * A projection offered later still has to agree with what is latched here: a
         * function carrying another window's identity or another key layout belongs to a
         * different group and is declined into the residual list, exactly as it was when
         * the first projection to join was what latched the pair.
         */
        public void ofWindow(byte @NotNull [] windowIdentity, @NotNull ColumnTypes keyColumnTypes) {
            this.windowIdentity = windowIdentity;
            this.keyColumnTypes = keyColumnTypes;
        }

        /**
         * Assembles the plan, or returns null when no anchored window named it through
         * {@link #ofWindow}.
         * <p>
         * An anchored window whose functions all stayed residual still gets a plan: one
         * with an empty component list, a manifest declaring zero of them and an eight-byte
         * payload holding the anchor value alone. Storage layout is a fact about the
         * window, not about how many of its functions happened to be fusible, and a window
         * without a plan is a window whose checkpoints would have no single root shape.
         */
        public @Nullable LiveViewWindowStatePlan build() {
            if (windowIdentity == null || keyColumnTypes == null) {
                return null;
            }
            final WindowAccumulatorPlan runtimePlan = runtimeBuilder.build(WINDOW_VALUE_SLOT_COUNT);
            if (runtimePlan == null) {
                return anchorOnly(null);
            }
            final int durableComponentCount = componentsWithinTheLeafBudget(runtimePlan);
            if (durableComponentCount == 0) {
                // Unreachable through the compiler, which admits a function only while its
                // own declared image fits MAX_INLINE_COMPONENT_STATE_BYTES and so leaves the
                // first component inside the leaf budget by a wide margin. The fail-safe
                // answer keeps the window's plan and hands every function back to the root
                // it has outside a group, which is an anchor-only shape.
                return anchorOnly(runtimePlan);
            }
            final int componentCount = runtimePlan.getComponentCount();
            final ObjList<LiveViewAccumulatorDescriptor> components = new ObjList<>(componentCount);
            // Every component's offset in the fused payload, or NOT_PERSISTED past the
            // budget. The runtime slot layout and the durable one are the same components in
            // the same order, so a component's slot base is read straight off the plan that
            // assigned it and only the byte offsets are added here.
            final IntList componentOffsets = new IntList(componentCount);
            final ObjList<LiveViewAccumulatorDescriptor> durableComponents = new ObjList<>(durableComponentCount);
            final IntList durableComponentOffsets = new IntList(durableComponentCount);
            int offset = ANCHOR_STATE_OFFSET + ANCHOR_STATE_BYTES;
            int runtimeStateBytes = 0;
            for (int i = 0; i < componentCount; i++) {
                final LiveViewAccumulatorDescriptor component = durableComponent(runtimePlan.getComponent(i));
                components.add(component);
                runtimeStateBytes += component.getStateLength();
                if (i < durableComponentCount) {
                    componentOffsets.add(offset);
                    durableComponents.add(component);
                    durableComponentOffsets.add(offset);
                    offset += component.getStateLength();
                } else {
                    componentOffsets.add(LiveViewAccumulatorProjection.NOT_PERSISTED);
                }
            }
            // The budget walk above and this loop add the same widths from the same start, so
            // they cannot disagree - and if they ever did, the manifest and the leaf would
            // both be sized by a total nothing checked.
            assert offset <= LiveViewCheckpointContracts.MAX_INLINE_LEAF_STATE_BYTES;
            final int projectionCount = runtimePlan.getProjectionCount();
            final ObjList<LiveViewAccumulatorProjection> projections = new ObjList<>(projectionCount);
            for (int i = 0; i < projectionCount; i++) {
                final WindowAccumulatorProjection projection = runtimePlan.getProjection(i);
                projections.add(new LiveViewAccumulatorProjection(
                        projection,
                        componentOffsets.getQuick(projection.getComponentIndex()),
                        components.getQuick(projection.getComponentIndex()),
                        durableComponent(projection.getFunctionComponent())
                ));
            }
            return new LiveViewWindowStatePlan(
                    windowIdentity,
                    keyColumnTypes,
                    components,
                    durableComponentCount,
                    runtimePlan,
                    projections,
                    residualFunctions,
                    new LiveViewWindowStateManifest(
                            durableComponents,
                            durableComponentOffsets,
                            ANCHOR_STATE_OFFSET,
                            ANCHOR_STATE_BYTES,
                            offset
                    ),
                    offset,
                    runtimeStateBytes
            );
        }

        /**
         * Assembles the plan of an anchored window that carries no durable component: the
         * window's identity and key schema, a manifest declaring zero components, and the
         * eight-byte payload the anchor value alone fills.
         * <p>
         * Every function the factory compiled is a residual here, so the shape is the one a
         * view has always had on disk - one root for the window's keys and anchor values,
         * one root per function for its state - carried by a window root whose manifest
         * declares no components rather than by a state root of a second kind.
         *
         * @param runtimePlan the group the runtime builder made, whose projections join the
         *                    residual list because no component of theirs fits the leaf, or
         *                    null when there was no group at all
         */
        private LiveViewWindowStatePlan anchorOnly(@Nullable WindowAccumulatorPlan runtimePlan) {
            final ObjList<WindowFunction> residuals = new ObjList<>(residualFunctions.size());
            residuals.addAll(residualFunctions);
            if (runtimePlan != null) {
                // Appended rather than merged back into SELECT-list order: this arm is the
                // unreachable fail-safe above, and the list's order is what the residual
                // walk reports rather than anything a root is keyed by.
                for (int i = 0, n = runtimePlan.getProjectionCount(); i < n; i++) {
                    residuals.add(runtimePlan.getProjectionFunction(i));
                }
            }
            final int payloadBytes = ANCHOR_STATE_OFFSET + ANCHOR_STATE_BYTES;
            return new LiveViewWindowStatePlan(
                    windowIdentity,
                    keyColumnTypes,
                    new ObjList<>(0),
                    0,
                    null,
                    new ObjList<>(0),
                    residuals,
                    new LiveViewWindowStateManifest(
                            new ObjList<>(0),
                            new IntList(0),
                            ANCHOR_STATE_OFFSET,
                            ANCHOR_STATE_BYTES,
                            payloadBytes
                    ),
                    payloadBytes,
                    0
            );
        }

        /**
         * Returns how many of the plan's components the leaf carries: the longest prefix of
         * its canonical order whose layout fits
         * {@link LiveViewCheckpointContracts#MAX_INLINE_LEAF_STATE_BYTES}. Every projection
         * reading a component past it becomes a runtime-only member - grouped in the map,
         * separate on disk.
         * <p>
         * A group that overflows therefore degrades rather than falling off a cliff: declining
         * whole would put every function back on a root of its own, which costs a slower seal
         * and twice the metadata segments per seal. Keeping the part that fits keeps most of
         * the win, and the components left out are exactly the ones that would have needed an
         * overflow page beside the leaf, which a fused entry must not reference.
         * <p>
         * The prefix is taken in canonical identity order, which is the order the layout is
         * assigned in, so the answer does not depend on the SELECT list. It is a prefix rather
         * than a greedy pack because a prefix is what makes the persisted manifest readable as
         * "the components the leaf carries, in order" - a packed subset would describe the
         * same layout while leaving nothing in the ordering to say why one component is in and
         * a narrower later one is out.
         * <p>
         * A projection whose component was folded onto a host that then falls past the budget
         * follows that host out of the manifest, and persists its own narrower image on its
         * own root - read out of the host's slots, which the group still keeps. Nothing tries
         * to un-fold it back into the leaf: the fold is what decided the layout the budget was
         * measured against, and re-deciding it here would make the manifest depend on the
         * order the two passes ran in.
         */
        private int componentsWithinTheLeafBudget(WindowAccumulatorPlan runtimePlan) {
            int kept = 0;
            int offset = ANCHOR_STATE_OFFSET + ANCHOR_STATE_BYTES;
            for (int i = 0, n = runtimePlan.getComponentCount(); i < n; i++) {
                offset += durableComponent(runtimePlan.getComponent(i)).getStateLength();
                if (offset > LiveViewCheckpointContracts.MAX_INLINE_LEAF_STATE_BYTES) {
                    break;
                }
                kept++;
            }
            return kept;
        }

        /**
         * Returns the durable half of a component the runtime plan named. It is one of the
         * components a projection was offered with - the plan's components and the standalone
         * component of every projection both are - so the lookup always hits.
         */
        private LiveViewAccumulatorDescriptor durableComponent(WindowAccumulatorDescriptor runtime) {
            for (int i = 0, n = offeredComponents.size(); i < n; i++) {
                final LiveViewAccumulatorDescriptor component = offeredComponents.getQuick(i);
                if (component.getRuntime().isSameIdentity(runtime)) {
                    return component;
                }
            }
            throw new IllegalStateException("live view window state plan names a component nothing offered");
        }

        private void rememberDurableComponent(LiveViewAccumulatorDescriptor component) {
            for (int i = 0, n = offeredComponents.size(); i < n; i++) {
                if (offeredComponents.getQuick(i).isSameIdentity(component)) {
                    return;
                }
            }
            offeredComponents.add(component);
        }
    }
}

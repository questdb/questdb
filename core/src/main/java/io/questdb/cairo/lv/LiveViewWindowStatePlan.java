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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * The compiled description of one live view's fused window state: which accumulator
 * components its durable state is made of, where each sits in the leaf's scalar
 * payload, and which SELECT-list outputs read them.
 * <p>
 * The plan is the single owner of that answer for a live view. Sharing is decided once, from
 * proven component identities - never inferred ad hoc in the checkpoint writer, and
 * never from SELECT-list order, which a recompile may change without changing a byte
 * of state. A function the plan does not bind is a <b>residual</b> and keeps the
 * legacy per-function root it has today; the two lists together are always the whole
 * factory.
 * <p>
 * Sharing itself is not a durable question, so this plan does not decide it: its
 * {@link Builder} composes a {@link WindowAccumulatorPlan} and renders that plan's
 * components into a persisted layout. Sharing comes in two strengths
 * there, applied in that order. Two projections whose components are <b>identical</b> merge
 * outright - {@code sum(x)} beside {@code avg(x)}. A projection whose component's whole image
 * sits <b>inside</b> another's is folded onto that host and reads its own bytes out of the
 * wider slice - {@code count(x)} beside either of them, which
 * {@link LiveViewAccumulatorDescriptor#derivedStateOffset} is what proves. The second
 * is what takes {@code sum(x) + avg(x) + count(x)} to one component and a 24-byte
 * fused entry. Neither ever applies across arguments or contribution predicates, so
 * the target shape's {@code sum(amt)} and {@code count(acct)} remain two components.
 * <p>
 * One projection joins on neither strength but on an argument the window itself pins:
 * a {@code count(k)} over the very column the window partitions by reads the row-count
 * component {@code count(*)} maintains and corrects it per row - see
 * {@link WindowAccumulatorProjection#PROJECTION_COUNT_PARTITION_KEY}. Such a
 * projection never becomes its component's contributor, because the counter it would
 * keep alone is not the one the component holds.
 *
 * <h2>Scalar layout</h2>
 * <pre>
 *   offset 0: anchor value, 8 bytes
 *   then:     components in canonical identity order
 *   refs:     empty
 * </pre>
 * Components are ordered by their encoded identity, so two nodes compiling the same
 * view - and one node recompiling it after the projections were reordered - lay the
 * state out identically. The payload must fit
 * {@link LiveViewCheckpointContracts#MAX_INLINE_LEAF_STATE_BYTES}: the B-tree splits
 * on entry count rather than encoded size, so an unbounded "fixed width means inline"
 * rule would build very large 64-entry leaves and make every CRC and decode along the
 * path more expensive. A group that overflows keeps the prefix of that order which
 * fits, and every component past it stays in the group's map value while its projecting
 * function keeps the function root it has today - see below.
 *
 * <h2>Durable and runtime-only members</h2>
 * The budget is a fact about a persisted leaf and not about a map, so the two questions it
 * used to answer at once are separate:
 * <ul>
 *     <li>a <b>durable member</b> is a component in {@link #getManifest() the manifest}.
 *     Its bytes are in every fused leaf and its function has no root of its own;</li>
 *     <li>a <b>runtime-only member</b> is a component past the budget. Its slots are in the
 *     window's one map value exactly like a durable member's - one map, one probe, one
 *     accumulator update - and its bytes go to the function root its projection keeps, read
 *     out of the group's value at {@link LiveViewAccumulatorProjection#getFunctionSlotBase()}
 *     through the same component codec the manifest would have used.</li>
 * </ul>
 * The durable members are a prefix of the canonical order and the runtime-only ones the
 * tail, so {@link #getDurableComponentCount()} splits one list rather than naming a subset.
 * A <b>residual</b> is neither: a function the compiler never offered, or one the runtime
 * builder turned away, which keeps both its own map and its own root.
 *
 * <h2>What reads it</h2>
 * The plan is durable, not advisory. {@code LiveViewCheckpointWindowRoot} persists
 * {@link #getManifest()} as the layout every fused leaf is sliced by, and compares it
 * byte-for-byte against a predecessor root's before a seal may build on that
 * predecessor's leaves; {@link #getWindowIdentity()} and the key schema are the other
 * two halves of that compatibility test. {@code LiveViewWindow} builds its fused map
 * value from the same component order, so a component's runtime slot base and its
 * durable state offset are two readings of one decision.
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
     * The window's own value slots - anchor value, initialized flag, tombstone - which
     * lead the fused runtime map value exactly as the anchor value leads the fused
     * scalar payload. {@code LiveViewWindow} defines those slots and reads this constant
     * back for its value layout, so the two cannot disagree about where the components
     * start.
     */
    public static final int WINDOW_VALUE_SLOT_COUNT = 3;
    private static final int WINDOW_IDENTITY_FORMAT_VERSION = 1;
    private static final int WINDOW_IDENTITY_MAGIC = 0x4c565749; // LVWI
    private final IntList componentSlotBases;
    private final ObjList<LiveViewAccumulatorDescriptor> components;
    /**
     * Per component, the index into {@link #projectionFunctions} of the function that
     * updates it. The plan chooses it deterministically - the lowest output position
     * among the compatible contributors - so a view recompiled without the projection
     * that happened to own the accumulator last time still updates it, and no "owner
     * output position" ever reaches disk.
     */
    private final IntList contributorIndexes;
    private final int durableComponentCount;
    private final ColumnTypes keyColumnTypes;
    private final LiveViewWindowStateManifest manifest;
    private final ObjList<WindowFunction> projectionFunctions;
    private final ObjList<LiveViewAccumulatorProjection> projections;
    private final ObjList<WindowFunction> residualFunctions;
    private final int totalInlineStateBytes;
    private final int totalRuntimeStateBytes;
    private final byte[] windowIdentity;

    private LiveViewWindowStatePlan(
            byte[] windowIdentity,
            ColumnTypes keyColumnTypes,
            ObjList<LiveViewAccumulatorDescriptor> components,
            int durableComponentCount,
            IntList componentSlotBases,
            IntList contributorIndexes,
            ObjList<LiveViewAccumulatorProjection> projections,
            ObjList<WindowFunction> projectionFunctions,
            ObjList<WindowFunction> residualFunctions,
            LiveViewWindowStateManifest manifest,
            int totalInlineStateBytes,
            int totalRuntimeStateBytes
    ) {
        this.windowIdentity = windowIdentity;
        this.keyColumnTypes = keyColumnTypes;
        this.components = components;
        this.durableComponentCount = durableComponentCount;
        this.componentSlotBases = componentSlotBases;
        this.contributorIndexes = contributorIndexes;
        this.projections = projections;
        this.projectionFunctions = projectionFunctions;
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
        final byte[][] fields = {
                canonicalWindowName.toString().getBytes(StandardCharsets.UTF_8),
                partitionSignature.toString().getBytes(StandardCharsets.UTF_8),
                orderSignature.toString().getBytes(StandardCharsets.UTF_8)
        };
        int size = 2 * Integer.BYTES + fields.length * Integer.BYTES;
        for (int i = 0; i < fields.length; i++) {
            size += fields[i].length;
        }
        final ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putInt(WINDOW_IDENTITY_MAGIC);
        buffer.putInt(WINDOW_IDENTITY_FORMAT_VERSION);
        for (int i = 0; i < fields.length; i++) {
            buffer.putInt(fields[i].length);
            buffer.put(fields[i]);
        }
        return buffer.array();
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
            projectionFunctions.getQuick(i).bindWindowStateSlots(projections.getQuick(i).getRuntime());
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
        return componentSlotBases.getQuick(index);
    }

    /**
     * Returns the function that updates component {@code index}. Every other
     * projection on that component is a read-only reader of the same state.
     * <p>
     * A contributor is always a projection whose own function persists that exact
     * component, never a {@link LiveViewAccumulatorProjection#isDerived() derived} one:
     * a {@code count} folded onto a sum's counter freezes eight bytes and the component
     * is sixteen, so it could not write the image even though it can read it. Nor a
     * {@link LiveViewAccumulatorProjection#isPartitionKeyGuarded() guarded} one, whose
     * image is the same eight bytes as its component's and still a different number.
     */
    public WindowFunction getContributor(int index) {
        return projectionFunctions.getQuick(contributorIndexes.getQuick(index));
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

    /**
     * Returns the partition-key layout the grouped functions share. A window-state
     * root is compatible with a predecessor only when this matches too, so it is
     * carried on the plan rather than re-derived per seal.
     */
    public ColumnTypes getKeyColumnTypes() {
        return keyColumnTypes;
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
        return projectionFunctions.getQuick(index);
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
     * Returns the projection {@code function} emits, or {@code -1} when the group does not
     * carry it. Identity comparison: the plan holds non-owning references to the very
     * functions the factory compiled, so two distinct functions are two distinct outputs
     * however alike their identities read.
     */
    public int indexOfProjectionFunction(WindowFunction function) {
        for (int i = 0, n = projectionFunctions.size(); i < n; i++) {
            if (projectionFunctions.getQuick(i) == function) {
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
        return contributorIndexes.getQuick(componentIndex) == index;
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
        for (int i = 0, n = projectionFunctions.size(); i < n; i++) {
            final Map map = projectionFunctions.getQuick(i).getPartitionMap();
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
        for (int i = 0, n = projectionFunctions.size(); i < n; i++) {
            final Map map = projectionFunctions.getQuick(i).getPartitionMap();
            if (map != null) {
                map.reopen();
                map.clear();
            }
        }
    }

    /**
     * Takes the fused slots off every projection's function, putting each one back on
     * the private map and the per-row accumulator update it owns outside a fused group.
     * The state itself is the caller's to move; this only changes who is asked for it.
     */
    public void unbindProjectionFunctions() {
        for (int i = 0, n = projectionFunctions.size(); i < n; i++) {
            projectionFunctions.getQuick(i).bindWindowStateSlots(null);
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
     * Collects the compiler's candidate projections and, on {@link #build()}, renders the
     * layout a {@link WindowAccumulatorPlanBuilder} decides into the durable one: the fused
     * byte offsets, the manifest, and the leaf budget that may hand the tail of the group
     * back as residuals.
     * <p>
     * Which components a group has, which of them merge, which fold onto a wider host, what
     * order they sit in and which projection maintains each is <b>not</b> decided here. All
     * of that is a fact about the accumulators rather than about persisting them, so it is
     * the runtime builder's, and this class composes one rather than repeating it - the two
     * used to be the same three hundred lines twice, which is how a layout and the manifest
     * describing it come to disagree. What is added on top is exactly what only exists
     * because the state is written down.
     * <p>
     * Every rejection is an ordinary answer rather than an error: the caller adds the
     * function to the residual list and it keeps the legacy root it has today.
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
         * window identity or key layout, which the first projection to join fixes. A later
         * one disagreeing with either belongs to a different window group.
         * <p>
         * The identity is latched only once a projection has actually joined, so a function
         * the runtime builder turns away cannot fix the group it was not admitted to.
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
            if (windowIdentity != null
                    && (!Arrays.equals(windowIdentity, candidateWindowIdentity)
                    || !isSameLayout(keyColumnTypes, candidateKeyColumnTypes))) {
                return false;
            }
            if (!runtimeBuilder.addProjection(function, component.getRuntime(), projectionKind, outputPosition)) {
                return false;
            }
            if (windowIdentity == null) {
                windowIdentity = candidateWindowIdentity;
                keyColumnTypes = candidateKeyColumnTypes;
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
         * Assembles the plan, or returns null when the group is empty, when the runtime
         * builder declined it, or when not even its first component fits the leaf budget.
         */
        public @Nullable LiveViewWindowStatePlan build() {
            final WindowAccumulatorPlan runtimePlan = runtimeBuilder.build(WINDOW_VALUE_SLOT_COUNT);
            if (runtimePlan == null) {
                return null;
            }
            final int durableComponentCount = componentsWithinTheLeafBudget(runtimePlan);
            if (durableComponentCount == 0) {
                // Unreachable through the compiler, which admits a function only while its
                // own declared image fits MAX_INLINE_COMPONENT_STATE_BYTES and so leaves the
                // first component inside the leaf budget by a wide margin. Declining whole is
                // the fail-safe answer: every function goes back to the legacy root it has
                // outside a group.
                return null;
            }
            final int componentCount = runtimePlan.getComponentCount();
            final ObjList<LiveViewAccumulatorDescriptor> components = new ObjList<>(componentCount);
            // Every component's offset in the fused payload, or NOT_PERSISTED past the
            // budget. The runtime slot layout and the durable one are the same components in
            // the same order, so a component's slot base is read straight off the plan that
            // assigned it and only the byte offsets are added here.
            final IntList componentOffsets = new IntList(componentCount);
            final IntList componentSlotBases = new IntList(componentCount);
            final ObjList<LiveViewAccumulatorDescriptor> durableComponents = new ObjList<>(durableComponentCount);
            final IntList durableComponentOffsets = new IntList(durableComponentCount);
            int offset = ANCHOR_STATE_OFFSET + ANCHOR_STATE_BYTES;
            int runtimeStateBytes = 0;
            for (int i = 0; i < componentCount; i++) {
                final LiveViewAccumulatorDescriptor component = durableComponent(runtimePlan.getComponent(i));
                components.add(component);
                componentSlotBases.add(runtimePlan.getComponentSlotBase(i));
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
            final ObjList<WindowFunction> projectionFunctions = new ObjList<>(projectionCount);
            for (int i = 0; i < projectionCount; i++) {
                final WindowAccumulatorProjection projection = runtimePlan.getProjection(i);
                projections.add(new LiveViewAccumulatorProjection(
                        projection,
                        componentOffsets.getQuick(projection.getComponentIndex()),
                        components.getQuick(projection.getComponentIndex()),
                        durableComponent(projection.getFunctionComponent())
                ));
                projectionFunctions.add(runtimePlan.getProjectionFunction(i));
            }
            // Every projection the runtime plan made is a member, so the contributor indexes
            // are the runtime plan's own. The budget renumbered them while it handed the tail
            // back as residuals; it no longer drops anything, so there is nothing to remap.
            final IntList contributorIndexes = new IntList(componentCount);
            for (int i = 0; i < componentCount; i++) {
                contributorIndexes.add(runtimePlan.getContributorIndex(i));
            }
            return new LiveViewWindowStatePlan(
                    windowIdentity,
                    keyColumnTypes,
                    components,
                    durableComponentCount,
                    componentSlotBases,
                    contributorIndexes,
                    projections,
                    projectionFunctions,
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
         * Returns how many of the plan's components the leaf carries: the longest prefix of
         * its canonical order whose layout fits
         * {@link LiveViewCheckpointContracts#MAX_INLINE_LEAF_STATE_BYTES}. Every projection
         * reading a component past it becomes a runtime-only member - grouped in the map,
         * separate on disk.
         * <p>
         * A group that overflows therefore degrades rather than falling off a cliff. It used
         * to decline whole, and what that cost is measurable: over 1M retained keys a
         * four-component group declining seals in 359 ms and publishes 8 metadata segments per
         * seal, against 79 ms and 4 for the same group fused. Keeping the part that fits keeps
         * most of that, and the components left out are exactly the ones that would have
         * needed a storage kind the leaf does not have.
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

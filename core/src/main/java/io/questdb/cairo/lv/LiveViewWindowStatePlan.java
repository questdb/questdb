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
 * The plan is the single owner of that answer. Sharing is decided here, once, from
 * proven component identities - never inferred ad hoc in the checkpoint writer, and
 * never from SELECT-list order, which a recompile may change without changing a byte
 * of state. A function the plan does not bind is a <b>residual</b> and keeps the
 * legacy per-function root it has today; the two lists together are always the whole
 * factory.
 * <p>
 * Sharing comes in two strengths, and the plan applies them in that order. Two
 * projections whose components are <b>identical</b> merge outright - {@code sum(x)}
 * beside {@code avg(x)}. A projection whose component's whole image sits <b>inside</b>
 * another's is folded onto that host and reads its own bytes out of the wider slice -
 * {@code count(x)} beside either of them, which
 * {@link LiveViewAccumulatorDescriptor#derivedStateOffset} is what proves. The second
 * is what takes {@code sum(x) + avg(x) + count(x)} to one component and a 24-byte
 * fused entry. Neither ever applies across arguments or contribution predicates, so
 * the target shape's {@code sum(amt)} and {@code count(acct)} remain two components.
 *
 * <h2>Scalar layout</h2>
 * <pre>
 *   offset 0: anchor value, 8 bytes
 *   then:     components in canonical identity order
 *   refs:     empty
 * </pre>
 * Components are ordered by their encoded identity, so two nodes compiling the same
 * view - and one node recompiling it after the projections were reordered - lay the
 * state out identically. The complete payload must fit
 * {@link LiveViewCheckpointContracts#MAX_INLINE_LEAF_STATE_BYTES}: the B-tree splits
 * on entry count rather than encoded size, so an unbounded "fixed width means inline"
 * rule would build very large 64-entry leaves and make every CRC and decode along the
 * path more expensive. A group that does not fit gets no plan at all and every one of
 * its functions stays on its legacy root.
 *
 * <h2>Status</h2>
 * The plan compiles and validates today but nothing persists it yet: the checkpoint
 * seal still writes one legacy root per function. Its first durable consumer is the
 * window-state root, which reads {@link #getManifest()} for the layout and
 * {@link #getWindowIdentity()} plus the key schema for predecessor compatibility.
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
    private final ColumnTypes keyColumnTypes;
    private final LiveViewWindowStateManifest manifest;
    private final ObjList<WindowFunction> projectionFunctions;
    private final ObjList<LiveViewAccumulatorProjection> projections;
    private final ObjList<WindowFunction> residualFunctions;
    private final int totalInlineStateBytes;
    private final byte[] windowIdentity;

    private LiveViewWindowStatePlan(
            byte[] windowIdentity,
            ColumnTypes keyColumnTypes,
            ObjList<LiveViewAccumulatorDescriptor> components,
            IntList componentSlotBases,
            IntList contributorIndexes,
            ObjList<LiveViewAccumulatorProjection> projections,
            ObjList<WindowFunction> projectionFunctions,
            ObjList<WindowFunction> residualFunctions,
            LiveViewWindowStateManifest manifest,
            int totalInlineStateBytes
    ) {
        this.windowIdentity = windowIdentity;
        this.keyColumnTypes = keyColumnTypes;
        this.components = components;
        this.componentSlotBases = componentSlotBases;
        this.contributorIndexes = contributorIndexes;
        this.projections = projections;
        this.projectionFunctions = projectionFunctions;
        this.residualFunctions = residualFunctions;
        this.manifest = manifest;
        this.totalInlineStateBytes = totalInlineStateBytes;
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
     */
    public void bindProjectionFunctions() {
        for (int i = 0, n = projections.size(); i < n; i++) {
            projectionFunctions.getQuick(i).bindWindowStateSlots(projections.getQuick(i));
        }
    }

    public LiveViewAccumulatorDescriptor getComponent(int index) {
        return components.getQuick(index);
    }

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
     * is sixteen, so it could not write the image even though it can read it.
     */
    public WindowFunction getContributor(int index) {
        return projectionFunctions.getQuick(contributorIndexes.getQuick(index));
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
     * Returns the window functions this plan does not group, in SELECT-list order.
     * Each keeps its own legacy function root - a ring-backed RANGE function, a
     * bounded ROWS accumulator, {@code count(*)}, an expression argument. "One B-tree
     * per window" therefore means one tree for the grouped components plus independent
     * roots for these.
     */
    public ObjList<WindowFunction> getResidualFunctions() {
        return residualFunctions;
    }

    /**
     * The whole fused scalar payload's width, anchor value included.
     */
    public int getTotalInlineStateBytes() {
        return totalInlineStateBytes;
    }

    /**
     * Returns an owned copy of the canonical window group identity.
     */
    public byte[] getWindowIdentity() {
        return Arrays.copyOf(windowIdentity, windowIdentity.length);
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
     * Collects the compiler's candidate projections and, on {@link #build()}, merges
     * identical components, orders them canonically, assigns the fused offsets and
     * freezes the manifest.
     * <p>
     * Every rejection is an ordinary answer rather than an error: the caller adds the
     * function to the residual list and it keeps the legacy root it has today.
     */
    public static final class Builder {
        private final IntList componentContributorOutputPositions = new IntList();
        private final IntList componentContributors = new IntList();
        private final ObjList<LiveViewAccumulatorDescriptor> components = new ObjList<>();
        private final IntList projectionComponents = new IntList();
        /**
         * Per projection, the component its own function persists when it stands alone.
         * Kept beside {@link #projectionComponents}, which the fold moves onto a host:
         * the difference between the two is the slice the function's decoder reads, and
         * a restore needs both.
         */
        private final ObjList<LiveViewAccumulatorDescriptor> projectionFunctionComponents = new ObjList<>();
        private final ObjList<WindowFunction> projectionFunctions = new ObjList<>();
        private final IntList projectionKinds = new IntList();
        private final IntList projectionOutputPositions = new IntList();
        private final ObjList<WindowFunction> residualFunctions = new ObjList<>();
        private ColumnTypes keyColumnTypes;
        private byte[] windowIdentity;

        /**
         * Binds {@code function} to {@code component}, creating the component when no
         * previously added projection already names an identical one.
         * <p>
         * The first accepted projection fixes the group's window identity and key
         * layout; a later one disagreeing with either belongs to a different window
         * group and is declined. A projection is also declined when its family cannot
         * produce {@code projectionKind}, or when the contributing implementation's
         * declared fixed width does not equal the family's state length - the manifest
         * would then name a slice the runtime image does not fill, and the leaf carries
         * no length of its own to catch it.
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
            if (!LiveViewAccumulatorProjection.isCompatible(component.getFamily(), projectionKind)) {
                return false;
            }
            if (component.getStateLength() != function.checkpointStateFixedLength()) {
                return false;
            }
            if (windowIdentity == null) {
                windowIdentity = candidateWindowIdentity;
                keyColumnTypes = candidateKeyColumnTypes;
            } else if (!Arrays.equals(windowIdentity, candidateWindowIdentity)
                    || !isSameLayout(keyColumnTypes, candidateKeyColumnTypes)) {
                return false;
            }
            int componentIndex = -1;
            for (int i = 0, n = components.size(); i < n; i++) {
                if (components.getQuick(i).isSameIdentity(component)) {
                    componentIndex = i;
                    break;
                }
            }
            if (componentIndex < 0) {
                componentIndex = components.size();
                components.add(component);
                componentContributors.add(projectionFunctions.size());
                componentContributorOutputPositions.add(outputPosition);
            } else if (outputPosition < componentContributorOutputPositions.getQuick(componentIndex)) {
                // Deterministic contributor choice: the lowest output position wins, so
                // the answer follows the compiled view rather than traversal order.
                componentContributors.setQuick(componentIndex, projectionFunctions.size());
                componentContributorOutputPositions.setQuick(componentIndex, outputPosition);
            }
            projectionFunctions.add(function);
            projectionComponents.add(componentIndex);
            projectionFunctionComponents.add(component);
            projectionKinds.add(projectionKind);
            projectionOutputPositions.add(outputPosition);
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
         * Assembles the plan, or returns null when the group is empty or its complete
         * scalar layout does not fit the leaf budget.
         */
        public @Nullable LiveViewWindowStatePlan build() {
            if (components.size() == 0) {
                return null;
            }
            foldDerivedComponents();
            final int componentCount = components.size();
            sortComponentsByIdentity();
            final IntList componentOffsets = new IntList(componentCount);
            // The runtime slot layout follows the durable one exactly: same components,
            // same order, so a component's slot base and its state offset are two
            // readings of one decision and neither can drift from the other.
            final IntList componentSlotBases = new IntList(componentCount);
            int offset = ANCHOR_STATE_OFFSET + ANCHOR_STATE_BYTES;
            int slot = WINDOW_VALUE_SLOT_COUNT;
            for (int i = 0; i < componentCount; i++) {
                componentOffsets.add(offset);
                componentSlotBases.add(slot);
                offset += components.getQuick(i).getStateLength();
                slot += components.getQuick(i).getSlotCount();
            }
            if (offset > LiveViewCheckpointContracts.MAX_INLINE_LEAF_STATE_BYTES) {
                // The whole group falls back rather than part of it: a window root is
                // complete or absent, and a half-fused group would need the combined
                // overflow page the format does not have yet.
                return null;
            }
            final ObjList<LiveViewAccumulatorProjection> projections = new ObjList<>(projectionKinds.size());
            for (int i = 0, n = projectionKinds.size(); i < n; i++) {
                final int componentIndex = projectionComponents.getQuick(i);
                projections.add(new LiveViewAccumulatorProjection(
                        projectionKinds.getQuick(i),
                        projectionOutputPositions.getQuick(i),
                        componentIndex,
                        componentOffsets.getQuick(componentIndex),
                        componentSlotBases.getQuick(componentIndex),
                        components.getQuick(componentIndex),
                        projectionFunctionComponents.getQuick(i)
                ));
            }
            return new LiveViewWindowStatePlan(
                    windowIdentity,
                    keyColumnTypes,
                    components,
                    componentSlotBases,
                    componentContributors,
                    projections,
                    projectionFunctions,
                    residualFunctions,
                    new LiveViewWindowStateManifest(
                            components,
                            componentOffsets,
                            ANCHOR_STATE_OFFSET,
                            ANCHOR_STATE_BYTES,
                            offset
                    ),
                    offset
            );
        }

        /**
         * Drops every component whose whole image already sits inside another's, moving
         * its projections onto that host. This is what takes
         * {@code sum(x) + avg(x) + count(x)} from two components and 32 inline bytes to
         * one component and 24: the count reads the counter the sum already keeps rather
         * than persisting a second copy of it.
         * <p>
         * The fold is a function of the component <b>set</b> and not of the order the
         * projections were added in, which it has to be - the manifest it feeds is
         * compared byte-for-byte against a predecessor's, so a fold that followed
         * SELECT-list order would make reordering two outputs force a conversion seal.
         * Where more than one host could serve, the smallest encoded identity wins, which
         * is the order the layout is assigned in anyway.
         * <p>
         * A host must be strictly wider than its guest, so the relation is a strict
         * partial order and cannot cycle. Guests are still resolved widest-first and a
         * host that is itself folded is skipped, so a chain - which no pair in
         * {@link LiveViewAccumulatorDescriptor#derivedStateOffset}'s table forms today -
         * would collapse one link at a time rather than leave a projection pointing at a
         * component that is no longer there.
         */
        private void foldDerivedComponents() {
            final int n = components.size();
            if (n < 2) {
                return;
            }
            final int[] byDescendingWidth = orderByDescendingWidth();
            final int[] hostOf = new int[n];
            Arrays.fill(hostOf, -1);
            for (int i = 0; i < n; i++) {
                final int guest = byDescendingWidth[i];
                final LiveViewAccumulatorDescriptor guestComponent = components.getQuick(guest);
                int host = -1;
                for (int candidate = 0; candidate < n; candidate++) {
                    final LiveViewAccumulatorDescriptor candidateComponent = components.getQuick(candidate);
                    if (candidate == guest
                            || hostOf[candidate] != -1
                            || candidateComponent.getStateLength() <= guestComponent.getStateLength()
                            || candidateComponent.derivedStateOffset(guestComponent) < 0) {
                        continue;
                    }
                    if (host < 0 || candidateComponent.compareIdentity(components.getQuick(host)) < 0) {
                        host = candidate;
                    }
                }
                hostOf[guest] = host;
            }
            final int[] newIndexOfOld = new int[n];
            final ObjList<LiveViewAccumulatorDescriptor> kept = new ObjList<>(n);
            final IntList keptContributors = new IntList(n);
            final IntList keptContributorPositions = new IntList(n);
            for (int i = 0; i < n; i++) {
                if (hostOf[i] != -1) {
                    // A folded component's contributor goes with it: the host has its own,
                    // and it is the only one whose image is the whole component.
                    newIndexOfOld[i] = -1;
                    continue;
                }
                newIndexOfOld[i] = kept.size();
                kept.add(components.getQuick(i));
                keptContributors.add(componentContributors.getQuick(i));
                keptContributorPositions.add(componentContributorOutputPositions.getQuick(i));
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
        }

        /**
         * Orders the component indexes by descending state length, ties broken by encoded
         * identity so the answer does not depend on insertion order.
         */
        private int[] orderByDescendingWidth() {
            final int n = components.size();
            final int[] order = new int[n];
            for (int i = 0; i < n; i++) {
                order[i] = i;
            }
            for (int i = 1; i < n; i++) {
                final int candidate = order[i];
                int j = i - 1;
                while (j >= 0 && isWiderThan(candidate, order[j])) {
                    order[j + 1] = order[j];
                    j--;
                }
                order[j + 1] = candidate;
            }
            return order;
        }

        private boolean isWiderThan(int left, int right) {
            final LiveViewAccumulatorDescriptor a = components.getQuick(left);
            final LiveViewAccumulatorDescriptor b = components.getQuick(right);
            return a.getStateLength() != b.getStateLength()
                    ? a.getStateLength() > b.getStateLength()
                    : a.compareIdentity(b) < 0;
        }

        /**
         * Insertion-sorts the components by encoded identity, carrying the per-component
         * bookkeeping and every projection's component index with them. Insertion sort
         * because a fused group holds a handful of components, and because it keeps the
         * index rewrite in one place.
         */
        private void sortComponentsByIdentity() {
            final int n = components.size();
            final int[] order = new int[n];
            for (int i = 0; i < n; i++) {
                order[i] = i;
            }
            for (int i = 1; i < n; i++) {
                final int candidate = order[i];
                int j = i - 1;
                while (j >= 0 && components.getQuick(order[j]).compareIdentity(components.getQuick(candidate)) > 0) {
                    order[j + 1] = order[j];
                    j--;
                }
                order[j + 1] = candidate;
            }
            final int[] newIndexOfOld = new int[n];
            final ObjList<LiveViewAccumulatorDescriptor> sorted = new ObjList<>(n);
            final IntList sortedContributors = new IntList(n);
            final IntList sortedContributorPositions = new IntList(n);
            for (int i = 0; i < n; i++) {
                final int old = order[i];
                newIndexOfOld[old] = i;
                sorted.add(components.getQuick(old));
                sortedContributors.add(componentContributors.getQuick(old));
                sortedContributorPositions.add(componentContributorOutputPositions.getQuick(old));
            }
            components.clear();
            components.addAll(sorted);
            componentContributors.clear();
            componentContributors.addAll(sortedContributors);
            componentContributorOutputPositions.clear();
            componentContributorOutputPositions.addAll(sortedContributorPositions);
            for (int i = 0, m = projectionComponents.size(); i < m; i++) {
                projectionComponents.setQuick(i, newIndexOfOld[projectionComponents.getQuick(i)]);
            }
        }
    }
}

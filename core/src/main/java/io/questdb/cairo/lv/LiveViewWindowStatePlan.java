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
    private static final int WINDOW_IDENTITY_FORMAT_VERSION = 1;
    private static final int WINDOW_IDENTITY_MAGIC = 0x4c565749; // LVWI
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

    public LiveViewAccumulatorDescriptor getComponent(int index) {
        return components.getQuick(index);
    }

    public int getComponentCount() {
        return components.size();
    }

    /**
     * Returns the function that updates component {@code index}. Every other
     * projection on that component is a read-only reader of the same state.
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
            final int componentCount = components.size();
            if (componentCount == 0) {
                return null;
            }
            sortComponentsByIdentity();
            final IntList componentOffsets = new IntList(componentCount);
            int offset = ANCHOR_STATE_OFFSET + ANCHOR_STATE_BYTES;
            for (int i = 0; i < componentCount; i++) {
                componentOffsets.add(offset);
                offset += components.getQuick(i).getStateLength();
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
                        components.getQuick(componentIndex)
                ));
            }
            return new LiveViewWindowStatePlan(
                    windowIdentity,
                    keyColumnTypes,
                    components,
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

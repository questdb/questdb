/*******************************************************************************
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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.lv.LiveViewCheckpointGenerationTracker;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;

/**
 * Test-only in-memory stand-in for the not-yet-built on-disk checkpoint
 * state/data store. It models a versioned checkpoint timeline as a mapping from
 * timeline generation to the {@code CheckpointRoot}s current in that
 * generation, each root referencing immutable data segments by id.
 * <p>
 * It exists so the generation-pin mechanism
 * ({@link LiveViewCheckpointGenerationTracker}) can be exercised end to end
 * without the real page store: a reader pins a generation, resolves that
 * generation's payload roots, and {@link #purge} honors the pin - "files
 * referenced by a published superblock slot or a pinned reader are not
 * deleted". A generation is retained when it is one of the two live superblock
 * slots (current or fallback) or still pinned; a data segment exists on the
 * simulated filesystem exactly while some retained generation references it, so
 * a segment shared with a retained generation survives while a dropped
 * generation's exclusive segments are collected.
 */
public class LiveViewCheckpointInMemoryPayloadStore {

    private final ObjList<Generation> generations = new ObjList<>();

    /**
     * @return the number of generations still present after the last {@link #purge}
     */
    public int generationCount() {
        return generations.size();
    }

    /**
     * @return true when {@code generation}'s payload roots are still present
     */
    public boolean hasGeneration(long generation) {
        return indexOf(generation) > -1;
    }

    /**
     * Records {@code roots} as the current roots of {@code generation}. The roots'
     * segments become present on the simulated filesystem.
     */
    public void publishGeneration(long generation, ObjList<PayloadRoot> roots) {
        assert indexOf(generation) < 0 : "generation already published: " + generation;
        final Generation g = new Generation(generation);
        for (int i = 0, n = roots.size(); i < n; i++) {
            g.roots.add(roots.getQuick(i));
        }
        generations.add(g);
    }

    /**
     * Drops every generation that is neither a live superblock slot ({@code current}
     * or {@code fallback}) nor pinned by a live reader, then leaves each data segment
     * present exactly while some surviving generation references it.
     *
     * @param currentGeneration  the current superblock slot's generation
     * @param fallbackGeneration the previous superblock slot's generation, or
     *                           {@link LiveViewCheckpointGenerationTracker#NO_GENERATION}
     *                           when there is no fallback slot
     * @param tracker            the pin authority; {@code isGenerationPinned} decides
     *                           whether an otherwise-obsolete generation is retained
     */
    public void purge(long currentGeneration, long fallbackGeneration, LiveViewCheckpointGenerationTracker tracker) {
        for (int i = generations.size() - 1; i >= 0; i--) {
            final long g = generations.getQuick(i).generation;
            final boolean retained = g == currentGeneration
                    || g == fallbackGeneration
                    || tracker.isGenerationPinned(g);
            if (!retained) {
                generations.remove(i);
            }
        }
    }

    /**
     * @return true when data segment {@code segmentId} is still referenced by some
     * present generation (i.e. still exists on the simulated filesystem)
     */
    public boolean segmentExists(long segmentId) {
        for (int i = 0, n = generations.size(); i < n; i++) {
            final ObjList<PayloadRoot> roots = generations.getQuick(i).roots;
            for (int j = 0, m = roots.size(); j < m; j++) {
                if (roots.getQuick(j).segmentIds.indexOf(segmentId) > -1) {
                    return true;
                }
            }
        }
        return false;
    }

    private int indexOf(long generation) {
        for (int i = 0, n = generations.size(); i < n; i++) {
            if (generations.getQuick(i).generation == generation) {
                return i;
            }
        }
        return -1;
    }

    /**
     * One checkpoint root held entirely in memory: the boundary identity plus
     * the ids of the immutable data segments it references.
     */
    public static final class PayloadRoot {
        public final long checkpointId;
        public final long definitionTxn;
        public final long maxTimestamp;
        public final LongList segmentIds = new LongList();

        public PayloadRoot(long checkpointId, long maxTimestamp, long definitionTxn, long... segmentIds) {
            this.checkpointId = checkpointId;
            this.maxTimestamp = maxTimestamp;
            this.definitionTxn = definitionTxn;
            for (long segmentId : segmentIds) {
                this.segmentIds.add(segmentId);
            }
        }
    }

    private static final class Generation {
        final long generation;
        final ObjList<PayloadRoot> roots = new ObjList<>();

        Generation(long generation) {
            this.generation = generation;
        }
    }
}

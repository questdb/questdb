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

import io.questdb.cairo.CairoException;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;

/**
 * In-memory owner of live {@link LiveViewCheckpointGenerationPin}s and the current
 * published timeline generation (design section 5 invariants 4-5, section 16.2).
 * <p>
 * A publication records the new current generation and its three superblock root
 * references with {@link #setCurrentGeneration}; a reader then takes a pin on the
 * current generation with {@link #pin()}. The tracker is the garbage-collection
 * authority the future purge job (design section 16.2) consults: it reports
 * {@link #minPinnedGeneration()} - the oldest generation any reader still pins - so
 * a physical object retired at generation {@code r} is deletable only when
 * {@code minPinnedGeneration() > r} (in addition to being unreachable from both
 * live superblock slots). {@link #isGenerationPinned(long)} answers the exact
 * "is any reader still on this specific generation" question the after-reader-
 * release lifecycle steps (design section 16.4) need.
 * <p>
 * The current-generation snapshot advances under publication while old pins keep
 * their own captured snapshot, so a reader that pinned {@code G} is unaffected by a
 * later {@code G + 1} publication (invariant 4). Generations advance monotonically;
 * {@link #setCurrentGeneration} rejects a backwards move.
 * <p>
 * All mutating and reading methods synchronize on this instance, so pins, releases,
 * publications, and purge-floor reads from different threads (a refresh reader, a
 * publishing writer, a purge job) are serialized. Pins are pooled and reused to stay
 * allocation-free on the read path.
 */
public class LiveViewCheckpointGenerationTracker implements QuietCloseable {

    /**
     * Sentinel current generation before any {@link #setCurrentGeneration}: nothing
     * is published yet, so {@link #pin()} has nothing to pin.
     */
    public static final long NO_GENERATION = -1;

    /**
     * {@link #minPinnedGeneration()} value when no reader holds a pin. It does not
     * constrain purge: {@code minPinnedGeneration() > retireGeneration} holds for
     * every real retire generation, so with no readers only the two live superblock
     * slots protect physical objects.
     */
    public static final long NO_PINS = Long.MAX_VALUE;

    private final LiveViewCheckpointPageRef currentRowPositionDeltaRootRef = new LiveViewCheckpointPageRef();
    private final LiveViewCheckpointPageRef currentSegmentDirectoryRootRef = new LiveViewCheckpointPageRef();
    private final LiveViewCheckpointPageRef currentTimelineRootRef = new LiveViewCheckpointPageRef();
    // One entry per live pin; holds that pin's captured generation. Duplicates are
    // expected (many readers on one generation) and each release removes exactly one.
    private final LongList pinnedGenerations = new LongList();
    private final ObjList<LiveViewCheckpointGenerationPin> pool = new ObjList<>();
    private long currentGeneration = NO_GENERATION;

    /**
     * Releases pooled pins and clears the current-generation snapshot. Asserts no pin
     * is still outstanding: an outstanding pin at close is a reader that never
     * released its generation.
     */
    @Override
    public synchronized void close() {
        assert pinnedGenerations.size() == 0
                : "live view checkpoint generation pins leaked: " + pinnedGenerations.size();
        pinnedGenerations.clear();
        pool.clear();
        currentGeneration = NO_GENERATION;
        currentTimelineRootRef.clear();
        currentRowPositionDeltaRootRef.clear();
        currentSegmentDirectoryRootRef.clear();
    }

    /**
     * @return the number of pins currently held across all generations
     */
    public synchronized int getActivePinCount() {
        return pinnedGenerations.size();
    }

    /**
     * @return the current published generation, or {@link #NO_GENERATION} before the
     * first {@link #setCurrentGeneration}
     */
    public synchronized long getCurrentGeneration() {
        return currentGeneration;
    }

    /**
     * @return true when at least one live pin holds exactly {@code generation}
     */
    public synchronized boolean isGenerationPinned(long generation) {
        final int n = pinnedGenerations.size();
        for (int i = 0; i < n; i++) {
            if (pinnedGenerations.getQuick(i) == generation) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return the oldest generation any reader still pins, or {@link #NO_PINS} when
     * no pin is held. This is the reader floor the purge job (design section 16.2)
     * combines with the two live superblock slots.
     */
    public synchronized long minPinnedGeneration() {
        final int n = pinnedGenerations.size();
        long min = NO_PINS;
        for (int i = 0; i < n; i++) {
            final long g = pinnedGenerations.getQuick(i);
            if (g < min) {
                min = g;
            }
        }
        return min;
    }

    /**
     * Takes a pin on the current published generation, snapshotting its root
     * references into the returned pin. Throws when no generation has been published.
     * The caller must {@link LiveViewCheckpointGenerationPin#close()} the pin.
     */
    public synchronized LiveViewCheckpointGenerationPin pin() {
        if (currentGeneration == NO_GENERATION) {
            throw CairoException.critical(0)
                    .put("live view checkpoint has no published generation to pin");
        }
        final LiveViewCheckpointGenerationPin pin = acquire();
        pin.arm(
                this,
                currentGeneration,
                currentTimelineRootRef,
                currentRowPositionDeltaRootRef,
                currentSegmentDirectoryRootRef
        );
        pinnedGenerations.add(currentGeneration);
        return pin;
    }

    /**
     * @return the number of live pins holding exactly {@code generation}
     */
    public synchronized int pinCount(long generation) {
        final int n = pinnedGenerations.size();
        int count = 0;
        for (int i = 0; i < n; i++) {
            if (pinnedGenerations.getQuick(i) == generation) {
                count++;
            }
        }
        return count;
    }

    /**
     * Records {@code generation} as the current published generation and snapshots
     * its three superblock root references. Generations advance monotonically; a
     * backwards move is rejected. Re-recording the same generation (e.g. a recovery
     * re-open of the selected slot) is allowed and refreshes the root references.
     */
    public synchronized void setCurrentGeneration(
            long generation,
            @NotNull LiveViewCheckpointPageRef timelineRootRef,
            @NotNull LiveViewCheckpointPageRef rowPositionDeltaRootRef,
            @NotNull LiveViewCheckpointPageRef segmentDirectoryRootRef
    ) {
        if (generation < 0) {
            throw CairoException.critical(0)
                    .put("live view checkpoint generation must be non-negative [generation=").put(generation).put(']');
        }
        if (currentGeneration != NO_GENERATION && generation < currentGeneration) {
            throw CairoException.critical(0)
                    .put("live view checkpoint generation must not move backwards [current=").put(currentGeneration)
                    .put(", next=").put(generation).put(']');
        }
        currentGeneration = generation;
        copyRef(currentTimelineRootRef, timelineRootRef);
        copyRef(currentRowPositionDeltaRootRef, rowPositionDeltaRootRef);
        copyRef(currentSegmentDirectoryRootRef, segmentDirectoryRootRef);
    }

    /**
     * Returns {@code pin} to the pool and drops its generation from the live set.
     * Called by {@link LiveViewCheckpointGenerationPin#close()}.
     */
    synchronized void release(long generation, @NotNull LiveViewCheckpointGenerationPin pin) {
        // Removes exactly one occurrence of generation (the first), which is correct
        // for a multiset of equal generations.
        pinnedGenerations.remove(generation);
        pin.disarm();
        pool.add(pin);
    }

    private static void copyRef(LiveViewCheckpointPageRef dst, LiveViewCheckpointPageRef src) {
        dst.of(src.getSegmentId(), src.getOffset(), src.getLength());
    }

    private LiveViewCheckpointGenerationPin acquire() {
        final int n = pool.size();
        if (n > 0) {
            final LiveViewCheckpointGenerationPin pin = pool.getQuick(n - 1);
            pool.setPos(n - 1);
            return pin;
        }
        return new LiveViewCheckpointGenerationPin();
    }
}

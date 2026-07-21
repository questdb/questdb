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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * Owns the durable Phase-1 logical-checkpoint catalogue: its A/B superblock,
 * bounded startup validation, and in-memory generation-pin authority.
 * <p>
 * Startup validates both slots independently at the superblock layer, then
 * validates only the selected generation's root metadata pages. It deliberately
 * does not walk either tree: corrupt deep paths remain a lazy access failure. If
 * a selected root page is missing, corrupt, or has the wrong node kind, startup
 * tries the other valid slot before exposing any generation. Only after bounded
 * validation succeeds is the generation installed in the tracker and available
 * to readers through {@link #pin()}.
 * <p>
 * Publication follows the opposite order. Callers first write and rename every
 * immutable metadata segment, fill the candidate fields exposed by
 * {@link #getSuperblock()}, and call {@link #publish()}. The candidate root pages
 * are validated before the inactive slot is touched; the tracker advances only
 * after the superblock commit point succeeds. Thus an orphan segment before slot
 * publication is harmless, and a reader pin can never name an unpublished
 * generation.
 */
public class LiveViewCheckpointMetaStore implements Closeable {

    private final Path checkpointsDir = new Path();
    private final LiveViewCheckpointGenerationTracker generationTracker = new LiveViewCheckpointGenerationTracker();
    private final LiveViewCheckpointRowPositionDeltaReader rowPositionDeltaReader;
    private final LiveViewCheckpointSegmentDirectory segmentDirectory;
    private final LiveViewCheckpointSuperblock superblock;
    private final LiveViewCheckpointTimelineReader timelineReader;
    private boolean isOpen;

    public LiveViewCheckpointMetaStore(@NotNull CairoConfiguration configuration) {
        superblock = new LiveViewCheckpointSuperblock(configuration);
        segmentDirectory = new LiveViewCheckpointSegmentDirectory(configuration);
        timelineReader = new LiveViewCheckpointTimelineReader(configuration);
        rowPositionDeltaReader = new LiveViewCheckpointRowPositionDeltaReader(configuration);
    }

    @Override
    public void close() {
        generationTracker.close();
        Misc.free(rowPositionDeltaReader);
        Misc.free(segmentDirectory);
        Misc.free(timelineReader);
        Misc.free(superblock);
        Misc.free(checkpointsDir);
        isOpen = false;
    }

    public LiveViewCheckpointSuperblock getSuperblock() {
        ensureOpen();
        return superblock;
    }

    public boolean isValid() {
        ensureOpen();
        return superblock.isValid();
    }

    /**
     * Returns the base-WAL retention floor required by both durable A/B slots
     * and every live generation pin owned by this store.
     */
    public long getWalPurgeFloor() {
        ensureOpen();
        if (!superblock.isValid()) {
            return -1;
        }
        final long slotFloor = superblock.getWalPurgeFloor();
        final long pinnedFloor = generationTracker.minPinnedNormalizedBaseSeqTxn();
        return pinnedFloor < 0 ? slotFloor : Math.min(slotFloor, pinnedFloor);
    }

    /**
     * Opens the catalogue and exposes the newest generation that passes bounded
     * validation, falling back to the other valid slot before returning when
     * necessary. A fresh or doubly unusable catalogue remains validly open but has
     * no generation to pin.
     */
    public void of(@Transient @NotNull Path checkpointsDir) {
        if (isOpen) {
            throw CairoException.critical(0).put("live view checkpoint metadata store already open");
        }
        this.checkpointsDir.of(checkpointsDir);
        superblock.of(checkpointsDir);
        timelineReader.of(checkpointsDir);
        rowPositionDeltaReader.of(checkpointsDir);
        isOpen = true;

        boolean triedFallback = false;
        while (superblock.isValid()) {
            try {
                validateSelectedRoots();
                installSelectedGeneration();
                return;
            } catch (CairoException e) {
                if (e.getErrno() != CairoException.LV_CHECKPOINT_TIMELINE_INVALID) {
                    throw e;
                }
                if (triedFallback || !superblock.selectFallbackSlot()) {
                    superblock.clearSelection();
                    return;
                }
                triedFallback = true;
            }
        }
    }

    public LiveViewCheckpointGenerationPin pin() {
        ensureOpen();
        return generationTracker.pin();
    }

    long getMinPinnedGeneration() {
        ensureOpen();
        return generationTracker.minPinnedGeneration();
    }

    long getOldestValidSuperblockGeneration() {
        ensureOpen();
        return superblock.getOldestValidGeneration();
    }

    /**
     * Validates the candidate root pages, commits the inactive superblock slot,
     * and only then makes the new generation pinnable.
     */
    public void publish() {
        ensureOpen();
        validateSelectedRoots();
        superblock.publish();
        installSelectedGeneration();
    }

    private void ensureOpen() {
        if (!isOpen) {
            throw CairoException.critical(0).put("live view checkpoint metadata store is not open");
        }
    }

    private void installSelectedGeneration() {
        generationTracker.setCurrentGeneration(
                superblock.generation,
                superblock.normalizedBaseSeqTxn,
                superblock.coveredLvSeqTxn,
                superblock.timelineRootRef,
                superblock.rowPositionDeltaRootRef,
                superblock.segmentDirectoryRootRef
        );
    }

    private void validateSelectedRoots() {
        // Decoding the root verifies both the page CRC/framing and the tree node's
        // kind/count bounds, but intentionally does not follow child references.
        if (!superblock.timelineRootRef.isNull()) {
            timelineReader.rootChildCount(superblock.timelineRootRef);
        }
        if (!superblock.rowPositionDeltaRootRef.isNull()) {
            rowPositionDeltaReader.rootChildCount(superblock.rowPositionDeltaRootRef);
        }
        // Decode the bounded catalogue root, but do not open or scan data files.
        segmentDirectory.of(checkpointsDir, superblock.segmentDirectoryRootRef);
    }
}

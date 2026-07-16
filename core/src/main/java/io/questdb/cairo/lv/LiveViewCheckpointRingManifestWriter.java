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
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * Publishes the {@code _checkpoints/_ring} manifest.
 * <p>
 * Holds one {@link BlockFileWriter} and one {@link Path} for the life of the
 * refresh worker: a publication happens once per refresh cycle that advances
 * the view, so the per-publish cost is the manifest rewrite plus one
 * mmap/munmap, and nothing beyond that should be allocated.
 * <p>
 * Durability follows {@code cairo.commit.mode}, matching what the {@code .cp}
 * files this manifest lists already do - {@link BlockFileWriter#commit()} syncs
 * only when the mode is not {@code NOSYNC}, and {@code NOSYNC} is the default.
 * Under {@code NOSYNC} the publication ordering the protocol relies on holds
 * across a process crash (the page cache stays coherent) but not across power
 * loss. That matches, rather than weakens, the status quo: today's correctness
 * rests on a best-effort {@code removeQuiet} unlink becoming durable, with no
 * directory fsync anywhere in the checkpoint path, whereas an allow-list
 * reduces it to one file's write ordering. Making the sync unconditional is a
 * separate decision with a measured cost - this sits on the refresh latency
 * path.
 * <p>
 * A failed publication is safe by construction and must never block a replay:
 * {@code coveredBaseSeqTxn} advances only on a successful publication, so the
 * on-disk manifest always holds a {@code (membership, covered)} pair that was
 * valid when written, and a restart either finds {@code covered} equal to the
 * reconciled floor (trust it) or not (fall back). Callers therefore log and
 * carry on rather than abandoning the cycle.
 */
public class LiveViewCheckpointRingManifestWriter implements Closeable {
    private final Path path = new Path();
    private final BlockFileWriter writer;

    public LiveViewCheckpointRingManifestWriter(@NotNull CairoConfiguration configuration) {
        this.writer = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode());
    }

    @Override
    public void close() {
        Misc.free(writer);
        Misc.free(path);
    }

    /**
     * Rewrites {@code <liveViewDir>/_checkpoints/_ring} with {@code entries} as
     * the set of checkpoints sealed at {@code coveredBaseSeqTxn}.
     * <p>
     * Publishes in place - {@link BlockFileWriter} alternates regions and flips
     * the version, so there is no temporary file for a sweep to clean up, and a
     * crash mid-commit leaves either the prior region or a checksum error, both
     * of which the reader turns into a conservative fallback.
     *
     * @param liveViewDir       absolute path to the LV directory, without the
     *                          {@code _checkpoints/} suffix
     * @param generation        publication counter; the caller increments it on
     *                          each successful publication
     * @param coveredBaseSeqTxn base seqTxn at which every listed entry is
     *                          proven sealed
     * @param entries           packed ring snapshot, taken under the refresh
     *                          latch, {@link LiveViewCheckpointRingManifest#ENTRY_SIZE}
     *                          longs per record, oldest first
     * @throws io.questdb.cairo.CairoException if the manifest cannot be written
     */
    public void publish(
            @Transient @NotNull Path liveViewDir,
            long generation,
            long coveredBaseSeqTxn,
            @NotNull LongList entries
    ) {
        LiveViewCheckpointRingManifest.ringManifestPath(path, liveViewDir);
        writer.of(path.$());
        LiveViewCheckpointRingManifest.append(generation, coveredBaseSeqTxn, entries, writer);
    }
}

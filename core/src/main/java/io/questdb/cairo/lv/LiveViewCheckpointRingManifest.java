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

import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.std.LongList;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

/**
 * Durable allow-list of the retained-checkpoint ring, persisted in
 * {@code <liveViewDir>/_checkpoints/_ring}.
 * <p>
 * Mirrors {@link LiveViewState}'s file shape: BlockFile with typed blocks,
 * rewritten in full on every publication. The name deliberately carries no
 * {@code .cp} extension - {@link LiveViewRecovery}'s startup sweep leaves
 * non-{@code .cp} names alone, so the manifest needs no sweep changes to
 * survive.
 * <p>
 * The block payload is:
 * <pre>
 *   int   formatVersion
 *   long  generation
 *   long  coveredBaseSeqTxn
 *   int   entryCount
 *   entryCount x { long lvSeqTxn, long maxTs, long baseSeqTxn, long lvRowsTotal, long stateBytes }
 * </pre>
 * Entry field order matches {@link LiveViewInstance}'s in-memory ring record,
 * so a publication is a straight {@link LongList} copy in either direction.
 * <p>
 * {@code coveredBaseSeqTxn} is the base seqTxn at which every listed entry is
 * proven sealed: each entry incorporates every base row with timestamp at or
 * below its own {@code maxTs} from every base commit through
 * {@code coveredBaseSeqTxn}. It is a claim about the <em>sealedness</em> of the
 * listed entries, not a claim that the live view consumed that seqTxn, which is
 * why a publication can precede the commit it names.
 * <p>
 * The manifest is an allow-list, not an inventory: a {@code .cp} the current
 * manifest does not list is never a resume anchor, whatever its filename or
 * contents. That is what makes a failed retirement unlink harmless - a stale
 * file absent from {@code _ring} is garbage, never trusted.
 * <p>
 * Ring state is derived, so a missing, corrupt or version-skewed manifest costs
 * a boundary rebuild and never invalidates the view.
 */
public class LiveViewCheckpointRingManifest {

    /**
     * Index of {@code baseSeqTxn} within an entry record - the last fully
     * processed base seqTxn at the checkpoint.
     */
    public static final int ENTRY_BASE_SEQ_TXN = 2;
    /**
     * Index of {@code lvRowsTotal} within an entry record - total live-view
     * rows produced through the checkpoint.
     */
    public static final int ENTRY_LV_ROWS_TOTAL = 3;
    /**
     * Index of {@code lvSeqTxn} within an entry record - the {@code .cp}
     * filename key, which sits in base-seqTxn space despite its name.
     */
    public static final int ENTRY_LV_SEQ_TXN = 0;
    /**
     * Index of {@code maxTs} within an entry record - max base-table timestamp
     * the checkpoint's window state reflects. The anchor lookup key.
     */
    public static final int ENTRY_MAX_TS = 1;
    /**
     * Longs per entry record. Matches {@link LiveViewInstance}'s ring record
     * size and field order.
     */
    public static final int ENTRY_SIZE = 5;
    /**
     * Index of {@code stateBytes} within an entry record - the {@code .cp}'s
     * on-disk size, the quantity the byte-budget prune caps.
     */
    public static final int ENTRY_STATE_BYTES = 4;
    public static final int RING_MANIFEST_BLOCK_TYPE = 0;
    public static final String RING_MANIFEST_FILE_NAME = "_ring";
    /**
     * Format version stamped as the first field of the manifest block. A reader
     * that finds a higher value discards the manifest and falls back to
     * highest-{@code .cp}-only recovery; it never invalidates the view, because
     * the ring is derived state. Bump when the payload layout changes
     * incompatibly; prefer a new block type for compatible additions.
     */
    public static final int RING_MANIFEST_FORMAT_VERSION = 1;
    /**
     * Payload bytes ahead of the first entry record: {@code formatVersion} INT,
     * {@code generation} LONG, {@code coveredBaseSeqTxn} LONG,
     * {@code entryCount} INT.
     */
    public static final int RING_MANIFEST_HEADER_SIZE = 2 * Integer.BYTES + 2 * Long.BYTES;

    /**
     * Appends the manifest block and commits the writer, publishing
     * {@code entries} as the set of checkpoints sealed at
     * {@code coveredBaseSeqTxn}.
     * <p>
     * {@code entries} is a packed ring snapshot - {@link #ENTRY_SIZE} longs per
     * record, oldest first - as held by {@link LiveViewInstance}. The caller
     * must take the snapshot under the refresh latch.
     * <p>
     * {@code generation} increases on every successful publication. It is
     * diagnostic: nothing selects on it, and the block checksum already catches
     * the corruption a stale generation would signal.
     */
    public static void append(
            long generation,
            long coveredBaseSeqTxn,
            @NotNull LongList entries,
            @NotNull BlockFileWriter writer
    ) {
        final int size = entries.size();
        assert isPackedRingValid(entries, coveredBaseSeqTxn) : "ring snapshot violates the manifest entry invariants";
        final AppendableBlock block = writer.append();
        block.putInt(RING_MANIFEST_FORMAT_VERSION);
        block.putLong(generation);
        block.putLong(coveredBaseSeqTxn);
        block.putInt(size / ENTRY_SIZE);
        for (int i = 0; i < size; i++) {
            block.putLong(entries.getQuick(i));
        }
        block.commit(RING_MANIFEST_BLOCK_TYPE);
        writer.commit();
    }

    /**
     * Points {@code path} at {@code <liveViewDir>/_checkpoints/_ring}. Both the
     * publication path and the startup sweep address the manifest through this
     * helper, so the two can never drift apart.
     */
    public static Path ringManifestPath(@NotNull Path path, @NotNull Path liveViewDir) {
        return path.of(liveViewDir)
                .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME)
                .concat(RING_MANIFEST_FILE_NAME);
    }

    /**
     * Mirrors the reader's structural validation so a wiring bug surfaces at
     * the publication that introduces it rather than at the restart that
     * discards the ring. Assert-only: an invalid manifest on disk is safe (the
     * reader rejects it and recovery falls back), a wrong one in memory is not.
     */
    private static boolean isPackedRingValid(@NotNull LongList entries, long coveredBaseSeqTxn) {
        final int size = entries.size();
        if (size % ENTRY_SIZE != 0) {
            return false;
        }
        long priorBaseSeqTxn = Long.MIN_VALUE;
        long priorLvSeqTxn = Long.MIN_VALUE;
        long priorMaxTs = Long.MIN_VALUE;
        for (int i = 0; i < size; i += ENTRY_SIZE) {
            final long lvSeqTxn = entries.getQuick(i + ENTRY_LV_SEQ_TXN);
            final long maxTs = entries.getQuick(i + ENTRY_MAX_TS);
            final long baseSeqTxn = entries.getQuick(i + ENTRY_BASE_SEQ_TXN);
            if (i > 0 && (lvSeqTxn <= priorLvSeqTxn || maxTs <= priorMaxTs || baseSeqTxn < priorBaseSeqTxn)) {
                return false;
            }
            if (lvSeqTxn > coveredBaseSeqTxn || baseSeqTxn > coveredBaseSeqTxn) {
                return false;
            }
            if (entries.getQuick(i + ENTRY_STATE_BYTES) < 0) {
                return false;
            }
            priorBaseSeqTxn = baseSeqTxn;
            priorLvSeqTxn = lvSeqTxn;
            priorMaxTs = maxTs;
        }
        return true;
    }
}

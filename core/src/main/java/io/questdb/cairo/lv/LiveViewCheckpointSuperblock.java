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
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import io.questdb.std.Zip;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * The fixed A/B superblock {@code _checkpoints/_timeline} that names the current
 * generation of the versioned checkpoint timeline (design section 8.2).
 * <p>
 * The file holds two independently-checksummed, fixed-size slots. A publication
 * writes the <em>inactive</em> slot (the one not currently selected) and its
 * higher generation number makes it the new current slot; the other slot remains
 * the recovery fallback. On open, the highest-generation slot that passes its own
 * checksum and format checks is selected; a torn or corrupt newest slot falls
 * back to the previous slot without any wider scan (design sections 8.2, 14.1).
 * This is the sole commit point for a timeline generation: a reader observes
 * either the complete old generation or the complete new one, never a partial
 * splice.
 * <p>
 * Each slot carries only authoritative metadata - generation and history/definition
 * identity, the two seqTxn coordinates ({@code normalizedBaseSeqTxn} in base-txn
 * space, {@code coveredLvSeqTxn} in live-view-writer space), the three root
 * references into metadata segments, the next-id counters, and physical byte
 * totals - plus a trailing CRC32. Publication durability follows
 * {@code cairo.commit.mode}: under {@code NOSYNC} the write ordering holds across
 * a process crash but not power loss, matching the rest of the checkpoint path.
 * <p>
 * This instance doubles as the value holder: after {@link #of} or
 * {@link #publish()} the public fields reflect the currently selected slot, and
 * they are the source the next {@link #publish()} writes. Callers must advance
 * {@link #generation} strictly on each publication.
 */
public class LiveViewCheckpointSuperblock implements Closeable {

    /**
     * Result of {@link #getSelectedSlot()} when neither slot is valid (a fresh or
     * doubly-corrupt timeline).
     */
    public static final int NO_SLOT = -1;
    public static final int SLOT_COVERED_LV_SEQTXN_OFFSET = 48;
    /**
     * Byte offset of the trailing CRC32 within a slot. The checksum covers
     * {@link #SLOT_CRC_COVERAGE} bytes from the slot base.
     */
    public static final int SLOT_CRC_OFFSET = 148;
    /**
     * Bytes of a slot the CRC32 covers: everything from the magic through the
     * last root reference, excluding the CRC field itself.
     */
    public static final int SLOT_CRC_COVERAGE = SLOT_CRC_OFFSET;
    public static final int SLOT_DATA_BYTES_OFFSET = 80;
    public static final int SLOT_DEFINITION_TXN_OFFSET = 24;
    public static final int SLOT_FORMAT_VERSION = 1;
    public static final int SLOT_FORMAT_VERSION_OFFSET = 8;
    public static final int SLOT_GENERATION_OFFSET = 16;
    public static final int SLOT_HISTORY_EPOCH_OFFSET = 32;
    /**
     * Magic marking a superblock slot: ASCII {@code "LVTMLN"} with a trailing
     * version nibble. A distinctive 8-byte value so a foreign or zeroed slot is
     * rejected before the checksum runs.
     */
    public static final long SLOT_MAGIC = 0x4C56_544D_4C4E_0001L;
    public static final int SLOT_MAGIC_OFFSET = 0;
    public static final int SLOT_METADATA_BYTES_OFFSET = 72;
    public static final int SLOT_NEXT_CHECKPOINT_ID_OFFSET = 56;
    public static final int SLOT_NEXT_SEGMENT_ID_OFFSET = 64;
    public static final int SLOT_NORMALIZED_BASE_SEQTXN_OFFSET = 40;
    public static final int SLOT_ROW_POSITION_DELTA_ROOT_REF_OFFSET = 108;
    public static final int SLOT_SEGMENT_DIRECTORY_ROOT_REF_OFFSET = 128;
    /**
     * Fixed size of one slot. The file is exactly {@link #FILE_SIZE} = two slots.
     */
    public static final int SLOT_SIZE = 152;
    public static final int SLOT_TIMELINE_ROOT_REF_OFFSET = 88;
    /**
     * Total size of the {@code _timeline} file: two slots back to back.
     */
    public static final long FILE_SIZE = 2L * SLOT_SIZE;
    private static final int SLOT_RESERVED_OFFSET = 12;
    // Value fields reflecting the currently selected slot (design section 8.2).
    public long coveredLvSeqTxn;
    public long dataBytes;
    public long definitionTxn;
    public long generation;
    public long historyEpoch;
    public long metadataBytes;
    public long nextCheckpointId;
    public long nextSegmentId;
    public long normalizedBaseSeqTxn;
    public final LiveViewCheckpointPageRef rowPositionDeltaRootRef = new LiveViewCheckpointPageRef();
    public final LiveViewCheckpointPageRef segmentDirectoryRootRef = new LiveViewCheckpointPageRef();
    public final LiveViewCheckpointPageRef timelineRootRef = new LiveViewCheckpointPageRef();
    private final int commitMode;
    private final FilesFacade ff;
    private final MemoryMARW mem;
    private final Path path = new Path();
    private long generationFloor = Long.MIN_VALUE;
    private boolean isOpen;
    private long oldestValidGeneration = Long.MAX_VALUE;
    private int selectedSlot = NO_SLOT;

    public LiveViewCheckpointSuperblock(@NotNull CairoConfiguration configuration) {
        this.ff = configuration.getFilesFacade();
        this.commitMode = configuration.getCommitMode();
        this.mem = Vm.getCMARWInstance();
    }

    @Override
    public void close() {
        // Non-truncating: storeSlot() writes at absolute offsets and never
        // advances the append cursor, so a truncating close would shrink the
        // file to zero and destroy both slots.
        mem.close(false);
        Misc.free(path);
        generationFloor = Long.MIN_VALUE;
        oldestValidGeneration = Long.MAX_VALUE;
        isOpen = false;
        selectedSlot = NO_SLOT;
        resetFields();
    }

    /**
     * @return the currently selected slot (0 or 1), or {@link #NO_SLOT} when
     * neither slot is valid
     */
    public int getSelectedSlot() {
        ensureOpen();
        return selectedSlot;
    }

    /**
     * Returns the oldest independently checksum-valid A/B slot generation. This
     * is the fallback floor used by generation-safe physical purge. A corrupt
     * slot does not protect files; a structurally unusable but checksum-valid
     * slot is retained conservatively until it is overwritten.
     */
    public long getOldestValidGeneration() {
        ensureOpen();
        return oldestValidGeneration;
    }

    /**
     * @return true when at least one slot passed validation on the last selection
     */
    public boolean isValid() {
        ensureOpen();
        return selectedSlot != NO_SLOT;
    }

    /**
     * Opens (creating if absent) {@code <checkpointsDir>/_timeline}, then selects
     * the current generation into this instance's fields. A fresh file selects
     * {@link #NO_SLOT} and leaves the fields at their defaults.
     */
    public void of(@Transient @NotNull Path checkpointsDir) {
        if (isOpen) {
            mem.close(false);
            isOpen = false;
        }
        LiveViewCheckpointLayout.timelinePath(path, checkpointsDir);
        // size < 1 allocates and maps exactly FILE_SIZE, zero-filling a fresh
        // file and preserving an existing one.
        mem.of(
                ff,
                path.$(),
                FILE_SIZE,
                -1,
                MemoryTag.MMAP_DEFAULT,
                CairoConfiguration.O_NONE,
                -1
        );
        isOpen = true;
        selectedSlot = select();
    }

    /**
     * Writes the current field values into the inactive slot and syncs per
     * {@code cairo.commit.mode}, then re-selects so the fields reflect the newly
     * current generation. The caller must have advanced {@link #generation} past
     * the value the other slot holds.
     */
    public void publish() {
        ensureOpen();
        if (generation < 0 || generation <= generationFloor) {
            throw CairoException.critical(0)
                    .put("live view checkpoint generation must advance [current=")
                    .put(generationFloor == Long.MIN_VALUE ? -1 : generationFloor)
                    .put(", next=").put(generation).put(']');
        }
        final int target = selectedSlot == 0 ? 1 : 0;
        storeSlot((long) target * SLOT_SIZE);
        if (commitMode != CommitMode.NOSYNC) {
            mem.sync(commitMode == CommitMode.ASYNC);
        }
        selectedSlot = select();
    }

    /**
     * Selects the other independently valid slot after bounded validation of the
     * current slot's referenced root pages failed. This is package-private because
     * only {@link LiveViewCheckpointMetaStore} may fall back: once a generation has
     * been exposed through a pin, late corruption invalidates a root version rather
     * than switching the whole live view to an older generation.
     *
     * @return true when the other slot is structurally valid and was selected
     */
    boolean selectFallbackSlot() {
        ensureOpen();
        final int fallback = selectedSlot == 0 ? 1 : 0;
        if (fallback >= 0 && isSlotValid((long) fallback * SLOT_SIZE)) {
            selectedSlot = fallback;
            loadSlot((long) fallback * SLOT_SIZE);
            return true;
        }
        selectedSlot = NO_SLOT;
        resetFields();
        return false;
    }

    void clearSelection() {
        ensureOpen();
        selectedSlot = NO_SLOT;
        resetFields();
    }

    private void ensureOpen() {
        if (!isOpen) {
            throw CairoException.critical(0)
                    .put("live view checkpoint superblock is not open");
        }
    }

    private boolean isSlotValid(long base) {
        final long magic = mem.getLong(base + SLOT_MAGIC_OFFSET);
        if (magic != SLOT_MAGIC) {
            return false;
        }
        final int computedCrc = Zip.crc32(0, mem.addressOf(base), SLOT_CRC_COVERAGE);
        final int storedCrc = mem.getInt(base + SLOT_CRC_OFFSET);
        if (computedCrc != storedCrc) {
            return false;
        }
        final int formatVersion = mem.getInt(base + SLOT_FORMAT_VERSION_OFFSET);
        return formatVersion == SLOT_FORMAT_VERSION;
    }

    private void loadSlot(long base) {
        generation = mem.getLong(base + SLOT_GENERATION_OFFSET);
        definitionTxn = mem.getLong(base + SLOT_DEFINITION_TXN_OFFSET);
        historyEpoch = mem.getLong(base + SLOT_HISTORY_EPOCH_OFFSET);
        normalizedBaseSeqTxn = mem.getLong(base + SLOT_NORMALIZED_BASE_SEQTXN_OFFSET);
        coveredLvSeqTxn = mem.getLong(base + SLOT_COVERED_LV_SEQTXN_OFFSET);
        nextCheckpointId = mem.getLong(base + SLOT_NEXT_CHECKPOINT_ID_OFFSET);
        nextSegmentId = mem.getLong(base + SLOT_NEXT_SEGMENT_ID_OFFSET);
        metadataBytes = mem.getLong(base + SLOT_METADATA_BYTES_OFFSET);
        dataBytes = mem.getLong(base + SLOT_DATA_BYTES_OFFSET);
        timelineRootRef.readFrom(mem, base + SLOT_TIMELINE_ROOT_REF_OFFSET);
        rowPositionDeltaRootRef.readFrom(mem, base + SLOT_ROW_POSITION_DELTA_ROOT_REF_OFFSET);
        segmentDirectoryRootRef.readFrom(mem, base + SLOT_SEGMENT_DIRECTORY_ROOT_REF_OFFSET);
    }

    private void resetFields() {
        generation = 0;
        definitionTxn = 0;
        historyEpoch = 0;
        normalizedBaseSeqTxn = 0;
        coveredLvSeqTxn = 0;
        nextCheckpointId = 0;
        nextSegmentId = 0;
        metadataBytes = 0;
        dataBytes = 0;
        timelineRootRef.clear();
        rowPositionDeltaRootRef.clear();
        segmentDirectoryRootRef.clear();
    }

    private int select() {
        int best = NO_SLOT;
        long bestGeneration = Long.MIN_VALUE;
        generationFloor = Long.MIN_VALUE;
        oldestValidGeneration = Long.MAX_VALUE;
        for (int slot = 0; slot < 2; slot++) {
            final long base = (long) slot * SLOT_SIZE;
            if (isSlotValid(base)) {
                final long gen = mem.getLong(base + SLOT_GENERATION_OFFSET);
                if (best == NO_SLOT || gen > bestGeneration) {
                    best = slot;
                    bestGeneration = gen;
                }
                generationFloor = Math.max(generationFloor, gen);
                oldestValidGeneration = Math.min(oldestValidGeneration, gen);
            }
        }
        if (best == NO_SLOT) {
            resetFields();
        } else {
            loadSlot((long) best * SLOT_SIZE);
        }
        return best;
    }

    private void storeSlot(long base) {
        mem.putLong(base + SLOT_MAGIC_OFFSET, SLOT_MAGIC);
        mem.putInt(base + SLOT_FORMAT_VERSION_OFFSET, SLOT_FORMAT_VERSION);
        mem.putInt(base + SLOT_RESERVED_OFFSET, 0);
        mem.putLong(base + SLOT_GENERATION_OFFSET, generation);
        mem.putLong(base + SLOT_DEFINITION_TXN_OFFSET, definitionTxn);
        mem.putLong(base + SLOT_HISTORY_EPOCH_OFFSET, historyEpoch);
        mem.putLong(base + SLOT_NORMALIZED_BASE_SEQTXN_OFFSET, normalizedBaseSeqTxn);
        mem.putLong(base + SLOT_COVERED_LV_SEQTXN_OFFSET, coveredLvSeqTxn);
        mem.putLong(base + SLOT_NEXT_CHECKPOINT_ID_OFFSET, nextCheckpointId);
        mem.putLong(base + SLOT_NEXT_SEGMENT_ID_OFFSET, nextSegmentId);
        mem.putLong(base + SLOT_METADATA_BYTES_OFFSET, metadataBytes);
        mem.putLong(base + SLOT_DATA_BYTES_OFFSET, dataBytes);
        timelineRootRef.writeTo(mem, base + SLOT_TIMELINE_ROOT_REF_OFFSET);
        rowPositionDeltaRootRef.writeTo(mem, base + SLOT_ROW_POSITION_DELTA_ROOT_REF_OFFSET);
        segmentDirectoryRootRef.writeTo(mem, base + SLOT_SEGMENT_DIRECTORY_ROOT_REF_OFFSET);
        // CRC last, covering everything before it.
        final int crc = Zip.crc32(0, mem.addressOf(base), SLOT_CRC_COVERAGE);
        mem.putInt(base + SLOT_CRC_OFFSET, crc);
    }
}

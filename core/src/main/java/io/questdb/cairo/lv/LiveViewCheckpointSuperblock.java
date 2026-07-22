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
import io.questdb.std.Numbers;
import io.questdb.std.Transient;
import io.questdb.std.Zip;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * The fixed A/B superblock {@code _checkpoints/_timeline} that names the
 * current generation of the versioned checkpoint timeline.
 * <p>
 * The file holds two independently-checksummed, fixed-size slots. A publication
 * writes the <em>inactive</em> slot (the one not currently selected) and its
 * higher generation number makes it the new current slot; the other slot
 * remains the recovery fallback. On open, the highest-generation slot that
 * passes its own checksum and format checks is selected; a torn or corrupt
 * newest slot falls back to the previous slot without any wider scan. This is
 * the sole commit point for a timeline generation: a reader observes either the
 * complete old generation or the complete new one, never a partial splice.
 * <p>
 * Each slot carries only authoritative metadata - generation and history/definition
 * identity, the two seqTxn coordinates ({@code normalizedBaseSeqTxn} in base-txn
 * space, {@code coveredLvSeqTxn} in live-view-writer space), the three root
 * references into metadata segments, the next-id counters, and the logical and
 * physical byte totals - plus a trailing CRC32. Publication durability follows
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
    public static final int SLOT_CRC_OFFSET = 172;
    /**
     * Bytes of a slot the CRC32 covers: everything from the magic through the
     * last accounting total, excluding the CRC field itself.
     */
    public static final int SLOT_CRC_COVERAGE = SLOT_CRC_OFFSET;
    public static final int SLOT_DATA_BYTES_OFFSET = 80;
    public static final int SLOT_DEFINITION_TXN_OFFSET = 24;
    public static final int SLOT_FORMAT_VERSION = 2;
    public static final int SLOT_FORMAT_VERSION_OFFSET = 8;
    public static final int SLOT_GENERATION_OFFSET = 16;
    public static final int SLOT_HISTORY_EPOCH_OFFSET = 32;
    public static final int SLOT_LOGICAL_STATE_BYTES_OFFSET = 156;
    /**
     * Magic marking a superblock slot: ASCII {@code "LVTMLN"} with a trailing
     * version nibble. A distinctive 8-byte value so a foreign or zeroed slot is
     * rejected before the checksum runs.
     */
    public static final long SLOT_MAGIC = 0x4C56_544D_4C4E_0002L;
    /**
     * The magic without its version nibble. A slot matching this under
     * {@link #SLOT_MAGIC_FAMILY_MASK} was written as a timeline superblock by
     * some build; whether this build can read it is then decided by the version
     * nibble and {@link #SLOT_FORMAT_VERSION_OFFSET}.
     */
    public static final long SLOT_MAGIC_FAMILY = 0x4C56_544D_4C4E_0000L;
    public static final long SLOT_MAGIC_FAMILY_MASK = 0xFFFF_FFFF_FFFF_0000L;
    public static final int SLOT_MAGIC_OFFSET = 0;
    public static final int SLOT_METADATA_BYTES_OFFSET = 72;
    public static final int SLOT_NEXT_CHECKPOINT_ID_OFFSET = 56;
    public static final int SLOT_NEXT_SEGMENT_ID_OFFSET = 64;
    public static final int SLOT_NORMALIZED_BASE_SEQTXN_OFFSET = 40;
    public static final int SLOT_ROW_POSITION_DELTA_BYTES_OFFSET = 164;
    public static final int SLOT_ROW_POSITION_DELTA_ROOT_REF_OFFSET = 108;
    /**
     * Byte offset of the seed sweep's resume cursor. It holds the base-cursor row
     * offset the sweep had consumed when this generation's newest root was sealed,
     * or {@link Numbers#LONG_NULL} for a generation a steady seal or a repair
     * published. See {@link #seedCursorOffset}.
     */
    public static final int SLOT_SEED_CURSOR_OFFSET_OFFSET = 148;
    public static final int SLOT_SEGMENT_DIRECTORY_ROOT_REF_OFFSET = 128;
    /**
     * Fixed size of one slot. The file is exactly {@link #FILE_SIZE} = two slots.
     */
    public static final int SLOT_SIZE = 176;
    public static final int SLOT_TIMELINE_ROOT_REF_OFFSET = 88;
    /**
     * Total size of the {@code _timeline} file: two slots back to back.
     */
    public static final long FILE_SIZE = 2L * SLOT_SIZE;
    private static final int SLOT_RESERVED_OFFSET = 12;
    // Value fields reflecting the currently selected slot.
    public long coveredLvSeqTxn;
    public long dataBytes;
    public long definitionTxn;
    public long generation;
    public long historyEpoch;
    /**
     * Running sum of {@code logicalStateBytes} over every current logical root:
     * what this generation's state would cost if no root shared a page with the
     * root beside it. Paired with {@link #metadataBytes} + {@link #dataBytes},
     * which is what the timeline actually wrote, it is the sharing the persistent
     * chunk layer buys. A cadence seal adds the appended root's value; a repair
     * adds the signed difference of every root it re-versions.
     */
    public long logicalStateBytes;
    public long metadataBytes;
    public long nextCheckpointId;
    public long nextSegmentId;
    public long normalizedBaseSeqTxn;
    /**
     * The share of {@link #metadataBytes} the persistent row-position difference
     * index wrote. Only a repair with a non-zero suffix delta adds to it, so it
     * prices what keeping an unchanged suffix's cumulative recovery coordinate
     * exact costs against the rest of the metadata.
     */
    public long rowPositionDeltaBytes;
    public final LiveViewCheckpointPageRef rowPositionDeltaRootRef = new LiveViewCheckpointPageRef();
    /**
     * The seed sweep's resume cursor for this generation: the row offset the
     * sweep's base cursor had consumed when the generation's newest root was
     * sealed. {@link Numbers#LONG_NULL} when a steady cadence seal or a repair
     * published the generation, which is what tells a restart that the newest
     * root is not a mid-sweep resume point.
     */
    public long seedCursorOffset = Numbers.LONG_NULL;
    public final LiveViewCheckpointPageRef segmentDirectoryRootRef = new LiveViewCheckpointPageRef();
    public final LiveViewCheckpointPageRef timelineRootRef = new LiveViewCheckpointPageRef();
    private final int commitMode;
    private final FilesFacade ff;
    private final MemoryMARW mem;
    private final Path path = new Path();
    private long coveredLvSeqTxnCeiling = -1;
    private long generationFloor = Long.MIN_VALUE;
    private long normalizedBaseSeqTxnCeiling = -1;
    private boolean isOpen;
    private long oldestValidGeneration = Long.MAX_VALUE;
    private int selectedSlot = NO_SLOT;
    private long walPurgeFloor = -1;

    public LiveViewCheckpointSuperblock(@NotNull CairoConfiguration configuration) {
        this.ff = configuration.getFilesFacade();
        this.commitMode = configuration.getCommitMode();
        this.mem = Vm.getCMARWInstance();
    }

    /**
     * Classifies {@code _timeline} as written by a build whose slot layout this
     * one cannot read. The probe reads two fields at offsets that stay put
     * across layout versions - the magic and the format version - so each build
     * can recognize the other's file without agreeing on anything else.
     * <p>
     * It deliberately validates no checksum. {@link #storeSlot} writes the magic
     * and the version ahead of the CRC, so a slot torn by a crash still carries
     * this build's pair and reads as native; ordinary A/B selection then rejects
     * it on the checksum and falls back. A slot outside the magic family -
     * zeroed, unwritten, short, or unrelated - is not classified either way.
     * Bit rot inside the version field reads as foreign, which costs a rebuild
     * of derived state and nothing else.
     *
     * @return true when either slot carries the timeline magic family with a
     * magic or format version this build does not write
     */
    public static boolean isForeignFormat(@NotNull FilesFacade ff, @NotNull LPSZ timelinePath) {
        final long fd = ff.openRO(timelinePath);
        if (fd < 0) {
            return false;
        }
        try {
            for (int slot = 0; slot < 2; slot++) {
                final long base = (long) slot * SLOT_SIZE;
                // A short or failed read returns -1, whose masked form cannot
                // match the family, so a truncated file is left unclassified.
                final long magic = ff.readNonNegativeLong(fd, base + SLOT_MAGIC_OFFSET);
                if ((magic & SLOT_MAGIC_FAMILY_MASK) != SLOT_MAGIC_FAMILY) {
                    continue;
                }
                if (magic != SLOT_MAGIC
                        || ff.readNonNegativeInt(fd, base + SLOT_FORMAT_VERSION_OFFSET) != SLOT_FORMAT_VERSION) {
                    return true;
                }
            }
            return false;
        } finally {
            ff.close(fd);
        }
    }

    @Override
    public void close() {
        // Non-truncating: storeSlot() writes at absolute offsets and never
        // advances the append cursor, so a truncating close would shrink the
        // file to zero and destroy both slots.
        mem.close(false);
        Misc.free(path);
        generationFloor = Long.MIN_VALUE;
        normalizedBaseSeqTxnCeiling = -1;
        coveredLvSeqTxnCeiling = -1;
        oldestValidGeneration = Long.MAX_VALUE;
        walPurgeFloor = -1;
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
     * Returns the greatest next-segment id advertised by either independently
     * checksum-valid slot. Every immutable file referenced by a valid slot has
     * an id below this ceiling, so startup orphan cleanup may only remove final
     * segment names at or above it. Taking the maximum (rather than the selected
     * slot's value) protects a newer checksum-valid slot whose bounded root
     * validation fell back to the older generation.
     */
    public long getNextSegmentIdCeiling() {
        ensureOpen();
        long ceiling = 0;
        for (int slot = 0; slot < 2; slot++) {
            final long base = (long) slot * SLOT_SIZE;
            if (isSlotValid(base)) {
                ceiling = Math.max(ceiling, mem.getLong(base + SLOT_NEXT_SEGMENT_ID_OFFSET));
            }
        }
        return ceiling;
    }

    /**
     * Returns the oldest base-table transaction still required by either
     * independently valid A/B slot. The value is a WAL retention floor, not the
     * selected generation's recovery coordinate: the fallback slot remains a
     * possible recovery source until a later publication overwrites it.
     *
     * @return the minimum {@code normalizedBaseSeqTxn} across valid slots, or
     * {@code -1} when neither slot is valid
     */
    public long getWalPurgeFloor() {
        ensureOpen();
        return walPurgeFloor;
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
        if (normalizedBaseSeqTxn < 0
                || coveredLvSeqTxn < 0
                || normalizedBaseSeqTxn < normalizedBaseSeqTxnCeiling
                || coveredLvSeqTxn < coveredLvSeqTxnCeiling) {
            throw CairoException.critical(0)
                    .put("live view checkpoint generation watermarks must not move backwards")
                    .put(" [storedBase=").put(normalizedBaseSeqTxnCeiling)
                    .put(", nextBase=").put(normalizedBaseSeqTxn)
                    .put(", storedLv=").put(coveredLvSeqTxnCeiling)
                    .put(", nextLv=").put(coveredLvSeqTxn).put(']');
        }
        if (seedCursorOffset < 0 && seedCursorOffset != Numbers.LONG_NULL) {
            throw CairoException.critical(0)
                    .put("live view checkpoint seed cursor offset must be non-negative, was ")
                    .put(seedCursorOffset);
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
        if (formatVersion != SLOT_FORMAT_VERSION) {
            return false;
        }
        // The seed cursor is either a real row offset or the "not a mid-sweep
        // generation" sentinel. Any other negative value would make a resume
        // skip backwards through the base cursor.
        final long seedCursorOffset = mem.getLong(base + SLOT_SEED_CURSOR_OFFSET_OFFSET);
        if (seedCursorOffset < 0 && seedCursorOffset != Numbers.LONG_NULL) {
            return false;
        }
        // These are authoritative publication coordinates. Reject impossible
        // values during bounded slot validation rather than allowing a
        // valid-CRC corrupt slot to release WAL by looking like "no floor".
        return mem.getLong(base + SLOT_GENERATION_OFFSET) >= 0
                && mem.getLong(base + SLOT_NORMALIZED_BASE_SEQTXN_OFFSET) >= 0
                && mem.getLong(base + SLOT_COVERED_LV_SEQTXN_OFFSET) >= 0;
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
        logicalStateBytes = mem.getLong(base + SLOT_LOGICAL_STATE_BYTES_OFFSET);
        rowPositionDeltaBytes = mem.getLong(base + SLOT_ROW_POSITION_DELTA_BYTES_OFFSET);
        seedCursorOffset = mem.getLong(base + SLOT_SEED_CURSOR_OFFSET_OFFSET);
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
        logicalStateBytes = 0;
        rowPositionDeltaBytes = 0;
        seedCursorOffset = Numbers.LONG_NULL;
        timelineRootRef.clear();
        rowPositionDeltaRootRef.clear();
        segmentDirectoryRootRef.clear();
    }

    private int select() {
        int best = NO_SLOT;
        long bestGeneration = Long.MIN_VALUE;
        generationFloor = Long.MIN_VALUE;
        normalizedBaseSeqTxnCeiling = -1;
        coveredLvSeqTxnCeiling = -1;
        oldestValidGeneration = Long.MAX_VALUE;
        walPurgeFloor = -1;
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
                final long normalizedBaseSeqTxn = mem.getLong(base + SLOT_NORMALIZED_BASE_SEQTXN_OFFSET);
                final long coveredLvSeqTxn = mem.getLong(base + SLOT_COVERED_LV_SEQTXN_OFFSET);
                normalizedBaseSeqTxnCeiling = Math.max(normalizedBaseSeqTxnCeiling, normalizedBaseSeqTxn);
                coveredLvSeqTxnCeiling = Math.max(coveredLvSeqTxnCeiling, coveredLvSeqTxn);
                walPurgeFloor = walPurgeFloor < 0
                        ? normalizedBaseSeqTxn
                        : Math.min(walPurgeFloor, normalizedBaseSeqTxn);
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
        mem.putLong(base + SLOT_LOGICAL_STATE_BYTES_OFFSET, logicalStateBytes);
        mem.putLong(base + SLOT_ROW_POSITION_DELTA_BYTES_OFFSET, rowPositionDeltaBytes);
        mem.putLong(base + SLOT_SEED_CURSOR_OFFSET_OFFSET, seedCursorOffset);
        timelineRootRef.writeTo(mem, base + SLOT_TIMELINE_ROOT_REF_OFFSET);
        rowPositionDeltaRootRef.writeTo(mem, base + SLOT_ROW_POSITION_DELTA_ROOT_REF_OFFSET);
        segmentDirectoryRootRef.writeTo(mem, base + SLOT_SEGMENT_DIRECTORY_ROOT_REF_OFFSET);
        // CRC last, covering everything before it.
        final int crc = Zip.crc32(0, mem.addressOf(base), SLOT_CRC_COVERAGE);
        mem.putInt(base + SLOT_CRC_OFFSET, crc);
    }
}

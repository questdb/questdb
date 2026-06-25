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

package io.questdb.cairo;

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

import java.io.Closeable;

/**
 * Per-table on-disk epoch marker file {@code _snapshot} recording the last durably committed epoch.
 *
 * <h3>Format</h3>
 * The file uses an A/B-versioned slot layout mirroring {@code _cv}/{@code _txn}:
 * <pre>
 * [0,  8)  = version (long): bumped on each write; (version &amp; 1) == 1 => slot B is live, else slot A
 * [8,  52) = slot A (SLOT_SIZE = 44 bytes)
 * [52, 96) = slot B (SLOT_SIZE = 44 bytes)
 * </pre>
 * Each slot is:
 * <pre>
 * [+0,  +8)  = epochSeqTxn (long)
 * [+8,  +16) = epochTxn    (long)
 * [+16, +24) = ts          (long)
 * [+24, +28) = formatVersion (int) = FORMAT_VERSION
 * [+28, +32) = padding (4 bytes, zeroed)
 * [+32, +40) = MAGIC  = SNAPSHOT_CHECKSUM_MAGIC (long)
 * [+40, +44) ... wait, this is 12 bytes short — see constants below for actual layout
 * </pre>
 * <p>
 * SLOT body = 28 bytes ({@link #SLOT_BODY_SIZE}):
 * epochSeqTxn + epochTxn + ts + formatVersion + padding.
 * <p>
 * Checksum trailer = 16 bytes ({@link #SLOT_TRAILER_SIZE}):
 * {@link TableUtils#SNAPSHOT_CHECKSUM_MAGIC} + 8-byte xxh3 checksum over the whole body.
 * <p>
 * Total slot = {@link #SLOT_SIZE} = 44 bytes.
 * Total file = {@link #FILE_SIZE} = 8 + 2 * 44 = 96 bytes.
 *
 * <h3>Write protocol (mirrors _cv doCommit)</h3>
 * <ol>
 *   <li>Write new epoch values into the INACTIVE slot (body fields + MAGIC + checksum).</li>
 *   <li>{@code Unsafe.storeFence()} — ensure body bytes are globally visible before the version bump.</li>
 *   <li>Bump the version word to point at the newly written slot.</li>
 *   <li>msync the whole file (MS_SYNC) to ensure the mmap bytes are in the page cache and all extent
 *       metadata is journaled.</li>
 *   <li>fsync the fd — the durability anchor: guarantees both data and inode metadata survive a crash.
 *       This is what makes the epoch marker a hard crash boundary.</li>
 * </ol>
 *
 * <h3>Read protocol (mirrors TxReader A/B fallback)</h3>
 * <ol>
 *   <li>Read version; select live slot by (version &amp; 1).</li>
 *   <li>Verify MAGIC + checksum of the live slot body.</li>
 *   <li>On mismatch, fall back to the other (prior) slot.</li>
 *   <li>If both fail (or the file is absent/empty) → return false (epoch 0 / full WAL replay).</li>
 * </ol>
 */
public class SnapshotMarker implements Closeable {
    // Format version written into each slot body.
    public static final int FORMAT_VERSION = 1;

    // Slot body layout offsets (relative to slot base).
    public static final int SLOT_OFFSET_EPOCH_SEQ_TXN = 0;
    public static final int SLOT_OFFSET_EPOCH_TXN = 8;
    public static final int SLOT_OFFSET_TS = 16;
    public static final int SLOT_OFFSET_FORMAT_VERSION = 24;
    // 4 bytes padding at +28 (zeroed, reserved for future use).

    // Slot trailer layout (immediately after the body).
    public static final int SLOT_BODY_SIZE = 32; // epochSeqTxn(8) + epochTxn(8) + ts(8) + formatVersion(4) + padding(4)
    public static final int SLOT_TRAILER_SIZE = 16; // MAGIC(8) + checksum(8)
    public static final int SLOT_SIZE = SLOT_BODY_SIZE + SLOT_TRAILER_SIZE; // 48 bytes

    // File header.
    public static final int HEADER_SIZE = 8; // version word only

    // Fixed file offsets.
    public static final int OFFSET_VERSION = 0;
    public static final int OFFSET_SLOT_A = HEADER_SIZE;                 // = 8
    public static final int OFFSET_SLOT_B = HEADER_SIZE + SLOT_SIZE;     // = 56

    // Total on-disk file size.
    public static final int FILE_SIZE = HEADER_SIZE + 2 * SLOT_SIZE; // = 8 + 2*48 = 104 bytes

    // Trailer sub-offsets within the trailer (relative to slot base + SLOT_BODY_SIZE).
    private static final int TRAILER_OFFSET_MAGIC = 0;
    private static final int TRAILER_OFFSET_CHECKSUM = 8;

    private final CairoConfiguration configuration;
    private final FilesFacade ff;
    private MemoryCMARW mem;
    private long epochSeqTxn;
    private long epochTxn;
    private long epochTs;
    private long version; // version word from the file

    public SnapshotMarker(CairoConfiguration configuration) {
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
    }

    /**
     * Open the marker file at {@code path} (which should already point at the table dir or an
     * absolute path including the filename). The file is created / grown as needed.
     */
    public SnapshotMarker of(LPSZ path) {
        if (mem != null) {
            mem.close(false);
        }
        this.mem = Vm.getSmallCMARWInstance(ff, path, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
        // Ensure the file is at least FILE_SIZE bytes so both slots are always addressable.
        if (mem.size() < FILE_SIZE) {
            mem.jumpTo(FILE_SIZE);
        }
        this.version = mem.getLong(OFFSET_VERSION);
        return this;
    }

    /**
     * Convenience: open using a pre-positioned Path (caller sets up the path, we just use it).
     * Equivalent to {@link #of(LPSZ)} but accepts a {@link Path} directly for positional clarity.
     */
    public SnapshotMarker of(Path path) {
        return of(path.$());
    }

    /**
     * Write epoch {@code {epochSeqTxn, epochTxn, ts}} into the INACTIVE slot, then:
     * <ol>
     *   <li>Store checksum trailer (MAGIC + xxh3).</li>
     *   <li>{@code Unsafe.storeFence()} to publish body before the version flip.</li>
     *   <li>Bump the version word (flip to the newly written slot).</li>
     *   <li>msync the whole file (MS_SYNC = range-fsync: journal commit + data-at-device + device flush).</li>
     *   <li>fsync the fd — the hard durability anchor ensuring both data and inode metadata survive crash.</li>
     * </ol>
     */
    public void write(long epochSeqTxn, long epochTxn, long ts) {
        // The ACTIVE slot is selected by (version & 1): 0 => A, 1 => B.
        // We write into the INACTIVE slot (the opposite one).
        boolean currentIsA = (version & 1L) == 0L;
        long writeSlotOffset = currentIsA ? OFFSET_SLOT_B : OFFSET_SLOT_A;

        // Ensure mapping covers at least FILE_SIZE.
        mem.jumpTo(FILE_SIZE);

        // Write the slot body.
        mem.putLong(writeSlotOffset + SLOT_OFFSET_EPOCH_SEQ_TXN, epochSeqTxn);
        mem.putLong(writeSlotOffset + SLOT_OFFSET_EPOCH_TXN, epochTxn);
        mem.putLong(writeSlotOffset + SLOT_OFFSET_TS, ts);
        mem.putInt(writeSlotOffset + SLOT_OFFSET_FORMAT_VERSION, FORMAT_VERSION);
        mem.putInt(writeSlotOffset + 28, 0); // padding

        // Write checksum trailer: MAGIC then xxh3 over the body.
        long bodyAddr = mem.addressOf(writeSlotOffset);
        long checksum = TableUtils.calculateCvAreaChecksum(bodyAddr, SLOT_BODY_SIZE);
        mem.putLong(writeSlotOffset + SLOT_BODY_SIZE + TRAILER_OFFSET_MAGIC, TableUtils.SNAPSHOT_CHECKSUM_MAGIC);
        mem.putLong(writeSlotOffset + SLOT_BODY_SIZE + TRAILER_OFFSET_CHECKSUM, checksum);

        // Store fence: all body + trailer bytes must be globally visible before the version bump.
        Unsafe.storeFence();

        // Bump the version to select the newly written slot.
        ++version;
        mem.putLong(OFFSET_VERSION, version);

        // msync the full file: MS_SYNC = data-at-device + journal commit + device-cache flush.
        // On Linux this is a range-fdatasync that journals both extent metadata and data.
        mem.sync(false /* sync = MS_SYNC, not MS_ASYNC */);

        // fsync the fd: the durability anchor. Guarantees inode metadata (mtime, i_size) AND data
        // are stable after a crash, independently of the shared-journal policy. This is the call
        // that makes the epoch marker a hard crash boundary.
        ff.fsync(mem.getFd());
    }

    /**
     * Attempt to load the epoch from the marker file.
     * <p>
     * Reads the version-selected (live) slot, verifies its MAGIC + CRC; on failure falls back to
     * the other slot. If both slots fail (or the file is absent/empty) returns {@code false} and
     * the caller should treat the epoch as 0 (back-compat: full WAL replay).
     *
     * @return {@code true} if a valid epoch was loaded, {@code false} if absent / both slots torn
     */
    public boolean tryLoad() {
        if (mem == null || mem.size() < FILE_SIZE) {
            epochSeqTxn = 0;
            epochTxn = 0;
            epochTs = 0;
            return false;
        }

        long ver = mem.getLong(OFFSET_VERSION);
        // Live slot: (ver & 1) == 1 => B, else => A.
        boolean liveIsA = (ver & 1L) == 0L;
        long liveSlotOffset = liveIsA ? OFFSET_SLOT_A : OFFSET_SLOT_B;
        long otherSlotOffset = liveIsA ? OFFSET_SLOT_B : OFFSET_SLOT_A;

        if (tryLoadSlot(liveSlotOffset)) {
            return true;
        }

        // Live slot CRC mismatch or missing MAGIC: fall back to the other (prior-commit) slot.
        if (tryLoadSlot(otherSlotOffset)) {
            return true;
        }

        // Both slots torn/invalid: report epoch 0.
        epochSeqTxn = 0;
        epochTxn = 0;
        epochTs = 0;
        return false;
    }

    /** Returns the epochSeqTxn loaded by the last successful {@link #tryLoad()}. */
    public long getEpochSeqTxn() {
        return epochSeqTxn;
    }

    /** Returns the epochTxn loaded by the last successful {@link #tryLoad()}. */
    public long getEpochTxn() {
        return epochTxn;
    }

    /** Returns the epoch timestamp loaded by the last successful {@link #tryLoad()}. */
    public long getEpochTs() {
        return epochTs;
    }

    @Override
    public void close() {
        if (mem != null) {
            mem.close(false); // no truncation: leave the file at FILE_SIZE
            mem = null;
        }
    }

    // ---- private helpers ----

    /**
     * Try to load epoch values from the slot at {@code slotOffset}.
     * Returns true if the MAGIC is present and the body checksum is valid.
     */
    private boolean tryLoadSlot(long slotOffset) {
        // Check MAGIC presence first (gates the checksum verify).
        long magic = mem.getLong(slotOffset + SLOT_BODY_SIZE + TRAILER_OFFSET_MAGIC);
        if (magic != TableUtils.SNAPSHOT_CHECKSUM_MAGIC) {
            return false;
        }

        // Verify body checksum.
        long storedChecksum = mem.getLong(slotOffset + SLOT_BODY_SIZE + TRAILER_OFFSET_CHECKSUM);
        long bodyAddr = mem.addressOf(slotOffset);
        long computedChecksum = TableUtils.calculateCvAreaChecksum(bodyAddr, SLOT_BODY_SIZE);
        if (storedChecksum != computedChecksum) {
            return false;
        }

        // Read fields.
        epochSeqTxn = mem.getLong(slotOffset + SLOT_OFFSET_EPOCH_SEQ_TXN);
        epochTxn = mem.getLong(slotOffset + SLOT_OFFSET_EPOCH_TXN);
        epochTs = mem.getLong(slotOffset + SLOT_OFFSET_TS);
        return true;
    }
}

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
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

/**
 * Per-partition vector of block hashes over the native files in one partition directory, used to
 * detect bit rot and torn writes in user data.
 * <p>
 * The file lives INSIDE the partition directory, which is deliberate: partition directories are
 * versioned ({@code 2022-02-25.8}), so any mutation that produces a new directory version starts
 * with no sidecar at all. That is "coverage dropped" for free, and it is why only genuinely
 * in-place mutations need an explicit {@link #invalidate()}.
 * <p>
 * On-disk layout:
 * <pre>
 * Header (write-once, 64 bytes)
 *   [0,8)    MAGIC          long, {@link #MAGIC}
 *   [8,12)   fileVersion    int,  {@link #FILE_VERSION}
 *   [12,16)  blockSize      int,  bytes, power of two
 *   [16,20)  slotSize       int,  bytes per slot, INCLUDING its trailer
 *   [20,64)  reserved, zero
 *
 * Slot A body at HEADER_SIZE, slot B body at HEADER_SIZE + slotSize
 *
 * Slot body
 *   [0,8)    generation     long, monotone; 0 means "never written"
 *   [8,16)   bodyLen        long, slot start to trailer, INCLUDING this 24-byte header
 *   [16,20)  entryCount     int
 *   [20,24)  pad, zero
 *   [24,...) entries
 *   [bodyLen, bodyLen+16)   trailer: [SLOT_TRAILER_MAGIC:8][checksum:8] over [slotStart, bodyLen)
 *
 * Entry (8-byte aligned)
 *   [0,4)    nameLen        int
 *   [4,8)    blockCount     int
 *   [8,16)   fileLength     long
 *   [16,16+pad8(nameLen))   name bytes, zero-padded to a multiple of 8
 *   then blockCount * 8 bytes of block hashes
 * </pre>
 * <p>
 * <b>A slot is validated as a whole.</b> That is the difference from
 * {@code TxnLogCrcSidecar}, where each entry is consulted independently and therefore needs a
 * seqlock stamp to prove it landed. Here a partially written slot fails its trailer and the entire
 * slot is discarded in favour of the other one. If you ever make an entry independently valid, you
 * must add a per-entry stamp with it.
 * <p>
 * <b>There is no selector word.</b> Slot choice comes from the generation INSIDE each checksummed
 * body: the reader takes the highest generation whose trailer verifies. {@code SnapshotMarker}
 * documents that its selector is unchecksummed and can revert independently under power loss; here
 * the failure mode is removed rather than mitigated.
 * <p>
 * <b>A torn sidecar is never evidence about the data.</b> Both slots failing yields
 * {@link ChecksumTrailer#ABSENT} -- "unverified" -- and never {@link ChecksumTrailer#MISMATCH}. The
 * sidecar carries no durability claim and is fully re-derivable from the data it describes, so every
 * failure path here must cost DETECTION, never data or ingestion.
 */
public class PartitionChecksumSidecar implements QuietCloseable {
    /**
     * Name of the sidecar within a partition directory.
     */
    public static final String FILE_NAME = "_chk";
    /**
     * Spells " PCHKSM " on disk (little-endian).
     */
    public static final long MAGIC = 0x204D534B48435020L;
    /**
     * Spells " PCHKSLT" on disk (little-endian). Distinct from {@link #MAGIC} so a header read as a
     * trailer, or vice versa, cannot classify as present.
     */
    public static final long SLOT_TRAILER_MAGIC = 0x544C534B48435020L;
    public static final int FILE_VERSION = 1;
    public static final int HEADER_SIZE = 64;
    public static final int SLOT_HEADER_SIZE = 24;
    /**
     * Smallest slot worth writing: enough for a handful of single-block files.
     */
    public static final int MIN_SLOT_CAPACITY = 256;

    private static final Log LOG = LogFactory.getLog(PartitionChecksumSidecar.class);
    private static final int OFFSET_BLOCK_SIZE = 12;
    private static final int OFFSET_FILE_VERSION = 8;
    private static final int OFFSET_SLOT_SIZE = 16;
    private static final int SLOT_OFFSET_BODY_LEN = 8;
    private static final int SLOT_OFFSET_ENTRY_COUNT = 16;
    private static final int SLOT_OFFSET_GENERATION = 0;

    private final LongList entryOffsets = new LongList();
    private final ObjList<String> entryNames = new ObjList<>();
    private int blockSize;
    private long generation;
    private int lastMismatchBlock = -1;
    private MemoryCMARW mem;
    private int pendingEntryCount;
    private MemoryCARW scratch;
    private int slotSize;
    private int winningSlot = -1;

    public static int blockCountFor(long fileLength, int blockSize) {
        if (fileLength <= 0) {
            return 0;
        }
        return (int) ((fileLength + blockSize - 1) / blockSize);
    }

    /**
     * The first block whose hash may have changed after the file grew from {@code previousLength}.
     * <p>
     * This is {@code previousLength / blockSize}, NOT the block after it: the block CONTAINING the
     * old end was partial, and appending fills it, so its hash changes. Skipping it would leave a
     * stale hash and report corruption on correctly written data.
     */
    public static int firstDirtyBlock(long previousLength, int blockSize) {
        if (previousLength <= 0) {
            return 0;
        }
        return (int) (previousLength / blockSize);
    }

    /**
     * Hashes one block of a mapped file. The last block is normally partial, so the range is clamped
     * to {@code fileLength}.
     */
    public static long hashBlock(long mappedAddr, long fileLength, int blockIndex, int blockSize) {
        final long lo = (long) blockIndex * blockSize;
        final long hi = Math.min(lo + blockSize, fileLength);
        return TableUtils.calculateCvAreaChecksum(mappedAddr + lo, hi - lo);
    }

    public int blockCount(int entryIndex) {
        return mem.getInt(entryOffsets.getQuick(entryIndex) + 4);
    }

    /**
     * The file's RECORDED block size. Never reinterpret an existing vector at a different size.
     */
    public int blockSize() {
        return blockSize;
    }

    public long blockHash(int entryIndex, int blockIndex) {
        final long entry = entryOffsets.getQuick(entryIndex);
        final int nameLen = mem.getInt(entry);
        return mem.getLong(entry + 16 + pad8(nameLen) + (long) blockIndex * Long.BYTES);
    }

    @Override
    public void close() {
        mem = Misc.free(mem);
        scratch = Misc.free(scratch);
        entryOffsets.clear();
        entryNames.clear();
        winningSlot = -1;
        generation = 0;
        blockSize = 0;
        slotSize = 0;
    }

    /**
     * {@link ChecksumTrailer#PRESENT_OK} when a slot verifies, {@link ChecksumTrailer#ABSENT}
     * otherwise. Never {@link ChecksumTrailer#MISMATCH}: a sidecar that fails to verify says
     * something about ITSELF, not about the data it describes.
     */
    public int coverage() {
        return winningSlot >= 0 ? ChecksumTrailer.PRESENT_OK : ChecksumTrailer.ABSENT;
    }

    public int fileCount() {
        return entryOffsets.size();
    }

    public long fileLength(int entryIndex) {
        return mem.getLong(entryOffsets.getQuick(entryIndex) + 8);
    }

    public CharSequence fileName(int entryIndex) {
        return entryNames.getQuick(entryIndex);
    }

    public long generation() {
        return generation;
    }

    public int indexOf(CharSequence name) {
        for (int i = 0, n = entryNames.size(); i < n; i++) {
            if (entryNames.getQuick(i).contentEquals(name)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Drops all coverage. Call this BEFORE an in-place mutation, never after: invalidating after the
     * rewrite leaves a window in which the sidecar describes bytes that no longer exist, and a crash
     * there yields stale coverage -- strictly worse than none.
     */
    public void invalidate() {
        if (mem == null) {
            return;
        }
        for (int s = 0; s < 2; s++) {
            final long base = HEADER_SIZE + (long) s * slotSize;
            mem.putLong(base + SLOT_OFFSET_GENERATION, 0);
            // A bodyLen of 0 fails the bounds check on read, so the slot can never be selected.
            mem.putLong(base + SLOT_OFFSET_BODY_LEN, 0);
        }
        winningSlot = -1;
        generation = 0;
        entryOffsets.clear();
        entryNames.clear();
    }

    public boolean isOpen() {
        return mem != null;
    }

    /**
     * Block index of the first mismatch found by {@link #verifyFile}, or -1.
     */
    public int lastMismatchBlock() {
        return lastMismatchBlock;
    }

    /**
     * Opens for READING, or creates a minimal file. Writers must use the 4-arg form with a capacity
     * sized to the partition -- see {@link #slotCapacityFor}.
     */
    public void of(FilesFacade ff, Path path, int blockSize) {
        of(ff, path, blockSize, MIN_SLOT_CAPACITY);
    }

    /**
     * Slot size for a body of {@code neededBytes}, plus headroom for the partition growing before it
     * is finally sealed.
     * <p>
     * This is sized per partition, NOT fixed. A fixed 256 KiB floor made a 150 KiB partition carry a
     * 512 KiB sidecar -- more checksum than data -- which showed up as an exact-diskSize assertion
     * failing and as a blown cursor RSS budget. At 8 bytes per 1 MiB block the vector is ~8 KiB per
     * GiB of data, so the honest size is tiny and proportional.
     */
    public static int slotCapacityFor(long neededBytes) {
        final long withHeadroom = neededBytes + (neededBytes >> 2) + ChecksumTrailer.TRAILER_SIZE;
        return (int) Math.max(MIN_SLOT_CAPACITY, Math.min(Integer.MAX_VALUE >> 1, withHeadroom));
    }

    /**
     * Opens, creating if absent. Never throws: an unopenable sidecar degrades to "not open", which
     * classifies as {@link ChecksumTrailer#ABSENT}.
     */
    public void of(FilesFacade ff, Path path, int blockSize, int slotCapacity) {
        try {
            of0(ff, path, blockSize, slotCapacity);
        } catch (Throwable th) {
            // ENOSPC, EROFS, EMFILE, permissions. This file carries no durability claim and is fully
            // re-derivable, so failing to open it must cost DETECTION, never ingestion.
            LOG.error().$("could not open partition checksum sidecar, coverage disabled [path=").$(path)
                    .$(", error=").$(th.getMessage()).I$();
            close();
        }
    }

    public void beginGeneration() {
        pendingEntryCount = 0;
        if (scratch == null) {
            // One page the size of a slot, so addressOf(0) is contiguous across everything that can
            // legally fit. An overflow spills into a second page but is rejected by the bounds check
            // in commitGeneration before anything is copied out of page 0.
            scratch = Vm.getCARWInstance(Math.max(slotSize, HEADER_SIZE), 2, MemoryTag.NATIVE_DEFAULT);
        }
        scratch.jumpTo(0);
    }

    public void putFile(CharSequence name, long length, int blockCount) {
        final int nameLen = name.length();
        scratch.putInt(nameLen);
        scratch.putInt(blockCount);
        scratch.putLong(length);
        for (int i = 0; i < nameLen; i++) {
            scratch.putByte((byte) name.charAt(i));
        }
        for (int i = nameLen, n = pad8(nameLen); i < n; i++) {
            scratch.putByte((byte) 0);
        }
        pendingEntryCount++;
    }

    public void putBlockHash(long hash) {
        scratch.putLong(hash);
    }

    /**
     * Publishes the buffered generation into the slot that did NOT win, body first and trailer last.
     * Returns false, having written nothing, when the sidecar is closed or the body would not fit --
     * a half-written slot would be stale coverage, which is worse than absent coverage.
     * <p>
     * Does not sync. Callers order that against the data the hashes describe.
     */
    public boolean commitGeneration() {
        if (mem == null || scratch == null) {
            return false;
        }
        final long pendingLen = scratch.getAppendOffset();
        final long bodyLen = SLOT_HEADER_SIZE + pendingLen;
        if (bodyLen + ChecksumTrailer.TRAILER_SIZE > slotSize) {
            LOG.error().$("partition checksum body too large for slot, coverage dropped [need=").$(bodyLen)
                    .$(", slotSize=").$(slotSize).I$();
            return false;
        }
        final int target = winningSlot == 0 ? 1 : 0;
        final long base = HEADER_SIZE + (long) target * slotSize;
        mem.putLong(base + SLOT_OFFSET_GENERATION, generation + 1);
        mem.putLong(base + SLOT_OFFSET_BODY_LEN, bodyLen);
        mem.putInt(base + SLOT_OFFSET_ENTRY_COUNT, pendingEntryCount);
        mem.putInt(base + SLOT_OFFSET_ENTRY_COUNT + 4, 0);
        if (pendingLen > 0) {
            Unsafe.getUnsafe().copyMemory(scratch.addressOf(0), mem.addressOf(base + SLOT_HEADER_SIZE), pendingLen);
        }
        // Trailer last: it is what makes the slot selectable, and it covers the generation too.
        mem.putLong(base + bodyLen, SLOT_TRAILER_MAGIC);
        mem.putLong(base + bodyLen + Long.BYTES, TableUtils.calculateCvAreaChecksum(mem.addressOf(base), bodyLen));
        selectSlot();
        return true;
    }

    public void fdatasync() {
        if (mem != null) {
            mem.getFilesFacade().fdatasync(mem.getFd());
        }
    }

    public void sync(boolean async) {
        if (mem != null) {
            mem.sync(async);
        }
    }

    /**
     * Verifies every covered block of one file against the stored vector.
     * <p>
     * A file SHORTER than the recorded length is truncation, which is real corruption. A file LONGER
     * is the normal "appended since the last generation" state -- the uncovered tail is exactly the
     * "newest blocks unverified" outcome the ordering rules produce on purpose.
     */
    public int verifyFile(FilesFacade ff, LPSZ filePath, int entryIndex) {
        lastMismatchBlock = -1;
        if (entryIndex < 0 || coverage() != ChecksumTrailer.PRESENT_OK) {
            return ChecksumTrailer.ABSENT;
        }
        final long storedLength = fileLength(entryIndex);
        final int blocks = blockCount(entryIndex);
        if (storedLength <= 0 || blocks <= 0) {
            return ChecksumTrailer.ABSENT;
        }
        final long actualLength = ff.length(filePath);
        if (actualLength < storedLength) {
            LOG.critical().$("covered file is shorter than recorded [path=").$(filePath)
                    .$(", recorded=").$(storedLength).$(", actual=").$(actualLength).I$();
            return ChecksumTrailer.MISMATCH;
        }
        final long fd = ff.openRO(filePath);
        if (fd < 0) {
            // Cannot read it => no verdict. A purge racing the scrub must not read as corruption.
            return ChecksumTrailer.ABSENT;
        }
        long addr = 0;
        try {
            addr = ff.mmap(fd, storedLength, 0, Files.MAP_RO, MemoryTag.MMAP_DEFAULT);
            if (addr == FilesFacade.MAP_FAILED) {
                addr = 0;
                return ChecksumTrailer.ABSENT;
            }
            for (int b = 0; b < blocks; b++) {
                if (hashBlock(addr, storedLength, b, blockSize) != blockHash(entryIndex, b)) {
                    lastMismatchBlock = b;
                    return ChecksumTrailer.MISMATCH;
                }
            }
            return ChecksumTrailer.PRESENT_OK;
        } finally {
            if (addr != 0) {
                ff.munmap(addr, storedLength, MemoryTag.MMAP_DEFAULT);
            }
            ff.close(fd);
        }
    }

    private static int pad8(int len) {
        return (len + 7) & ~7;
    }

    private void of0(FilesFacade ff, Path path, int blockSize, int slotCapacity) {
        close();
        mem = Vm.getCMARWInstance();
        mem.smallFile(ff, path.$(), MemoryTag.MMAP_TABLE_WRITER);
        if (mem.size() >= HEADER_SIZE && mem.getLong(0) == MAGIC) {
            final int fileVersion = mem.getInt(OFFSET_FILE_VERSION);
            if (fileVersion != FILE_VERSION) {
                LOG.error().$("unsupported partition checksum sidecar, coverage disabled [path=").$(path)
                        .$(", version=").$(fileVersion).I$();
                close();
                return;
            }
            // The file's own values govern. Reinterpreting an existing vector at a different block
            // size compares every block against the wrong expected hash.
            this.blockSize = mem.getInt(OFFSET_BLOCK_SIZE);
            this.slotSize = mem.getInt(OFFSET_SLOT_SIZE);
        } else {
            this.blockSize = blockSize;
            this.slotSize = slotCapacity;
            mem.putLong(0, MAGIC);
            mem.putInt(OFFSET_FILE_VERSION, FILE_VERSION);
            mem.putInt(OFFSET_BLOCK_SIZE, blockSize);
            mem.putInt(OFFSET_SLOT_SIZE, slotCapacity);
        }
        final long fileSize = HEADER_SIZE + 2L * slotSize;
        mem.extend(fileSize);
        // Park the append offset at the full size. Everything here is written at absolute offsets, so
        // without this the append offset stays at 0 and close() truncates the file to nothing -- the
        // next open then finds no MAGIC and silently re-initialises, losing every generation.
        mem.jumpTo(fileSize);
        selectSlot();
    }

    /**
     * Highest generation whose trailer verifies. Bounds are checked before any address arithmetic.
     */
    private void selectSlot() {
        winningSlot = -1;
        generation = 0;
        entryOffsets.clear();
        entryNames.clear();
        if (mem == null || slotSize <= SLOT_HEADER_SIZE + ChecksumTrailer.TRAILER_SIZE) {
            return;
        }
        for (int s = 0; s < 2; s++) {
            final long base = HEADER_SIZE + (long) s * slotSize;
            if (base + SLOT_HEADER_SIZE > mem.size()) {
                continue;
            }
            final long bodyLen = mem.getLong(base + SLOT_OFFSET_BODY_LEN);
            // Reject an impossible length BEFORE it reaches address arithmetic.
            if (bodyLen < SLOT_HEADER_SIZE || bodyLen > slotSize - ChecksumTrailer.TRAILER_SIZE) {
                continue;
            }
            if (base + bodyLen + ChecksumTrailer.TRAILER_SIZE > mem.size()) {
                continue;
            }
            final int classification = ChecksumTrailer.classify(
                    mem.getLong(base + bodyLen),
                    mem.getLong(base + bodyLen + Long.BYTES),
                    mem.addressOf(base),
                    bodyLen,
                    SLOT_TRAILER_MAGIC
            );
            if (classification != ChecksumTrailer.PRESENT_OK) {
                continue;
            }
            final long g = mem.getLong(base + SLOT_OFFSET_GENERATION);
            if (g > generation) {
                generation = g;
                winningSlot = s;
            }
        }
        if (winningSlot >= 0 && !walkEntries()) {
            // A verified body whose entry walk does not add up is not something to guess about.
            winningSlot = -1;
            generation = 0;
            entryOffsets.clear();
            entryNames.clear();
        }
    }

    private boolean walkEntries() {
        final long base = HEADER_SIZE + (long) winningSlot * slotSize;
        final long bodyLen = mem.getLong(base + SLOT_OFFSET_BODY_LEN);
        final int entryCount = mem.getInt(base + SLOT_OFFSET_ENTRY_COUNT);
        if (entryCount < 0) {
            return false;
        }
        long offset = base + SLOT_HEADER_SIZE;
        final long end = base + bodyLen;
        final StringBuilder sink = new StringBuilder();
        for (int i = 0; i < entryCount; i++) {
            if (offset + 16 > end) {
                return false;
            }
            final int nameLen = mem.getInt(offset);
            final int blocks = mem.getInt(offset + 4);
            if (nameLen < 0 || blocks < 0) {
                return false;
            }
            final long next = offset + 16 + pad8(nameLen) + (long) blocks * Long.BYTES;
            if (next > end) {
                return false;
            }
            sink.setLength(0);
            for (int c = 0; c < nameLen; c++) {
                sink.append((char) (mem.getByte(offset + 16 + c) & 0xFF));
            }
            entryOffsets.add(offset);
            entryNames.add(sink.toString());
            offset = next;
        }
        return true;
    }
}

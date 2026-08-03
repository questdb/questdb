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

package io.questdb.cairo.idx;

import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.cairo.vm.api.MemoryW;
import io.questdb.std.LongList;
import io.questdb.std.Unsafe;

/**
 * Reads and writes individual seal entries in the v2 .pk chain.
 * <p>
 * An entry is immutable once appended. The only mutation that ever happens
 * to an entry's bytes is when the writer reclaims the entry's region during
 * GC — at which point the entry is no longer reachable from the chain head
 * and no reader can be pinned on it.
 * <p>
 * Entry layout (header is V2_ENTRY_HEADER_SIZE = 56 bytes):
 * <pre>
 *   [0..7]                                              LEN
 *   [8..15]                                             SEAL_TXN
 *   [16..23]                                            VALUE_MEM_SIZE
 *   [24..31]                                            MAX_VALUE
 *   [32..35]                                            KEY_COUNT
 *   [36..39]                                            GEN_COUNT
 *   [40..43]                                            BLOCK_CAPACITY
 *   [44..47]                                            COVERING_FORMAT (reserved)
 *   [48..55]                                            PREV_ENTRY_OFFSET
 *   [56 ..)                                             GEN_DIR (GEN_COUNT * GEN_DIR_ENTRY_SIZE)
 *   [56 + GEN_COUNT*GEN_DIR_ENTRY_SIZE ..)              COVER_END_OFFSETS (COVER_COUNT * 8 bytes)
 * </pre>
 * COVER_COUNT is constant for the .pk file's posting column instance and is
 * published in the .pci sidecar header, so every entry in this .pk shares the
 * same cover footer length.
 * <p>
 * The entry-level "table _txn this entry takes effect at" (txnAtSeal) is NOT a
 * header field: it lives in slot[0]'s TXN_AT_SEAL within the gen-dir region.
 * Entries written with {@code genCount == 0} therefore have no on-disk
 * txnAtSeal and {@link #read} returns 0 for that case; production writers
 * always pass {@code genCount >= 1}.
 */
public final class PostingIndexChainEntry {

    private PostingIndexChainEntry() {
    }

    /**
     * Convenience overload: entry size for a no-cover-columns layout.
     * Equivalent to {@code entrySize(genCount, 0)}.
     */
    public static int entrySize(int genCount) {
        return entrySize(genCount, 0);
    }

    /**
     * Total entry size in bytes: header + gen dir + cover end-offset footer,
     * padded to an 8-byte boundary. {@code coverCount} is the .pci-published
     * fixed cover-column count for this posting column instance and is the
     * same for every entry in a given .pk file.
     */
    public static int entrySize(int genCount, int coverCount) {
        long bytes = (long) PostingIndexUtils.V2_ENTRY_HEADER_SIZE
                + (long) genCount * (long) PostingIndexUtils.GEN_DIR_ENTRY_SIZE
                + (long) coverCount * (long) PostingIndexUtils.COVER_END_OFFSET_ENTRY_SIZE;
        // Round up to 8 to keep subsequent entries 8-byte aligned.
        bytes = (bytes + 7L) & ~7L;
        return (int) bytes;
    }

    /**
     * Convenience overload that skips the cover end-offset footer. Useful for
     * callers that only care about header fields (chain-writer recovery,
     * test fixtures).
     */
    public static long read(MemoryR keyMem, long entryOffset, Snapshot into) {
        return read(keyMem, entryOffset, 0, into);
    }

    /**
     * Cover-column count from an entry's size: LEN = round8(56 + genCount*44 +
     * coverCount*8), pad &lt; 8, so floor-division is exact in either format.
     * Only safe on an entry nobody is extending. On the head it returns 5 or 6
     * too many, because extendHead writes LEN before GEN_COUNT and a crash
     * between them leaves a size describing one more gen than the count admits.
     * Resolving a head's count goes through resolveEntryCoverCount.
     */
    public static int coverCountFromLen(int genCount, long len) {
        long cover = len - PostingIndexUtils.V2_ENTRY_HEADER_SIZE - (long) genCount * PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
        return cover > 0 ? (int) (cover / PostingIndexUtils.COVER_END_OFFSET_ENTRY_SIZE) : 0;
    }

    /**
     * The stored COVERING_FORMAT int packs the format discriminator in its low
     * byte and, for format 1, the entry's own coverCount in the upper bits. This
     * makes a format-1 entry's gen-dir position self-contained (an atomic int
     * read), so a reader whose live .pci coverCount is transiently stale (e.g.
     * mid covering-config transition) still resolves the gen-dir correctly.
     * Legacy format-0 entries stored a plain 0, which decodes to format 0 /
     * coverCount 0 — unchanged.
     */
    public static int packCoveringFormat(int coveringFormat, int coverCount) {
        return coveringFormat == PostingIndexUtils.COVERING_FORMAT_DEALIASED
                ? (PostingIndexUtils.COVERING_FORMAT_DEALIASED | (coverCount << 8))
                : coveringFormat;
    }

    /**
     * Read an entry header at {@code entryOffset} into {@code into}. Returns
     * the offset just past this entry's bytes (useful for forward scans).
     * <p>
     * {@code coverCount} must match the .pci value for this posting column
     * instance — readers source it from {@code openSidecarFilesIfPresent}
     * before calling this method.
     */
    public static long read(MemoryR keyMem, long entryOffset, int coverCount, Snapshot into) {
        into.offset = entryOffset;
        // GEN_COUNT must be read FIRST. Pairs with the storeFence-guarded
        // GEN_COUNT store in PostingIndexChainWriter.extendHead: the writer
        // updates KEY_COUNT, LEN, VALUE_MEM_SIZE, MAX_VALUE and the cover
        // end-offset footer in place, fences, then bumps GEN_COUNT last. By
        // reading GEN_COUNT first and fencing after, this reader sees an
        // old GEN_COUNT (with matching old fields) or a new GEN_COUNT (with
        // matching new fields), but never new GEN_COUNT with old
        // VALUE_MEM_SIZE / cover sizes.
        into.genCount = keyMem.getInt(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_GEN_COUNT);
        Unsafe.loadFence();
        into.len = keyMem.getLong(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_LEN);
        into.sealTxn = keyMem.getLong(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_SEAL_TXN);
        into.valueMemSize = keyMem.getLong(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_VALUE_MEM_SIZE);
        into.maxValue = keyMem.getLong(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_MAX_VALUE);
        into.keyCount = keyMem.getInt(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_KEY_COUNT);
        into.blockCapacity = keyMem.getInt(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_BLOCK_CAPACITY);
        int rawCoveringFormat = keyMem.getInt(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_COVERING_FORMAT);
        into.coveringFormat = unpackCoveringFormat(rawCoveringFormat);
        into.prevEntryOffset = keyMem.getLong(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_PREV_ENTRY_OFFSET);
        // Format 1 (de-aliased) shifts the gen-dir past the fixed cover footer by
        // the ENTRY's OWN coverCount (packed atomically in the format int), so the
        // gen-dir/footer positions never depend on the reader's (possibly stale)
        // .pci coverCount. Format 0 puts the gen-dir directly after the header
        // (no reserve) and trails the footer, so its coverCount is irrelevant to
        // the gen-dir and its footer span is bounded by LEN below.
        // The size limit is what keeps the reads in range: one corrupted bit in
        // the 24-bit packed count reads as about 16.7M covers, and the TXN_AT_SEAL
        // read below would then reach 134 MB past the entry -- SIGSEGV in a -da
        // build. The picker checks LEN against the mapping, but that check runs
        // after read() returns, so it cannot prevent this.
        int entryCoverCount = limitCoverCountToEntrySize(unpackCoverCount(rawCoveringFormat), into.genCount, into.len);
        into.coverCount = into.coveringFormat == PostingIndexUtils.COVERING_FORMAT_DEALIASED ? entryCoverCount : coverCount;
        into.genDirOffset = resolveGenDirOffset(entryOffset, 0, into.coveringFormat, entryCoverCount);
        // Entry-level txnAtSeal sources from slot[0] (single source of truth).
        into.txnAtSeal = into.genCount > 0
                ? keyMem.getLong(into.genDirOffset + PostingIndexUtils.GEN_DIR_OFFSET_TXN_AT_SEAL)
                : 0L;
        into.coverFileEndOffsets.clear();
        // Use the ENTRY's authoritative cover count (into.coverCount): for format 1
        // that is the entry's own packed coverCount, so a format-1 entry's footer
        // is read even when the reader's live .pci coverCount is transiently stale
        // (== 0 mid covering-config) — covered reads stay robust rather than
        // returning NULL. For format 0 it is the caller's coverCount, so legacy
        // behaviour is unchanged.
        final int effectiveCoverCount = into.coverCount;
        if (effectiveCoverCount > 0) {
            long footerOffset = resolveCoverFooterOffset(entryOffset, into.genCount, into.coveringFormat, entryCoverCount);
            // How many footer slots the entry actually carries. Format 1: its own
            // packed coverCount. Format 0: derived from the trailing byte span
            // (self-bound against LEN — a legacy entry sealed with fewer covers
            // than expected reads only what is present). Bound by this so we never
            // dereference past the entry.
            int entryFooterSlots = into.coveringFormat == PostingIndexUtils.COVERING_FORMAT_DEALIASED
                    ? entryCoverCount
                    : (int) Math.max(0L, (entryOffset + into.len - footerOffset) / PostingIndexUtils.COVER_END_OFFSET_ENTRY_SIZE);
            int writtenCovers = Math.max(0, Math.min(effectiveCoverCount, entryFooterSlots));
            into.coverFileEndOffsets.setPos(effectiveCoverCount);
            for (int c = 0; c < writtenCovers; c++) {
                into.coverFileEndOffsets.setQuick(
                        c,
                        keyMem.getLong(footerOffset + (long) c * PostingIndexUtils.COVER_END_OFFSET_ENTRY_SIZE)
                );
            }
            for (int c = writtenCovers; c < effectiveCoverCount; c++) {
                into.coverFileEndOffsets.setQuick(c, 0L);
            }
        }
        return entryOffset + into.len;
    }

    /**
     * Overload without a limit, for callers that only read header or txn fields.
     * It can return too many covers for a format-0 entry whose size was left
     * wrong back when it was the head. That does no damage there, because format 0
     * puts its gen-dir at entry+56 whatever the count says. Turn the result into
     * an offset and it does.
     */
    public static int resolveEntryCoverCount(MemoryR keyMem, long entryOffset) {
        return resolveEntryCoverCount(keyMem, entryOffset, Long.MAX_VALUE);
    }

    /**
     * The entry's own coverCount, for format-1 entries only (from the packed
     * COVERING_FORMAT int). Returns 0 for format 0 — legacy entries never carry a
     * packed count and their gen-dir has no cover reserve anyway.
     */
    public static int unpackCoverCount(int rawStored) {
        return (rawStored & 0xFF) == PostingIndexUtils.COVERING_FORMAT_DEALIASED ? (rawStored >>> 8) : 0;
    }

    /**
     * Bytes reserved between the 56-byte header and the gen-dir for the FIXED
     * cover-end footer. Format 1 (de-aliased) reserves {@code coverCount*8} here
     * so the gen-dir starts after the footer; format 0 (legacy, trailing footer)
     * reserves 0. Non-covering ({@code coverCount==0}) is 0 in both formats, so
     * format 0 and format 1 entries are byte-identical there.
     */
    public static long coverFooterReserve(int coveringFormat, int coverCount) {
        return coveringFormat == PostingIndexUtils.COVERING_FORMAT_DEALIASED
                ? (long) coverCount * PostingIndexUtils.COVER_END_OFFSET_ENTRY_SIZE
                : 0L;
    }

    /**
     * An existing entry's authoritative cover-column count. Use this for anything
     * that becomes an offset, a length or a loop bound.
     * <p>
     * Format 1 reads its own packed count, which extendHead never rewrites, and
     * reduces it to what the entry can hold: the field is 24 bits, so a single
     * corrupted bit reads as about 16.7M covers, and publishToChain would write
     * its gen-dir 134 MB past the entry. Format 0 has no packed count and must
     * work one out from the size, so it needs entryEndLimit to cover the case
     * where a crash left the head with a size that describes one more gen than it
     * has. Without it, a head with no covers at all works out to 5 and gets
     * migrated as if it were an old covering one.
     *
     * @param entryEndLimit the entry's known end; pass the chain header's published
     *                      regionLimit, which the interrupted extendHead never
     *                      advanced and so still holds the last correct length.
     *                      Must not reach past a superseded entry -- migrate and
     *                      head-trim leave those behind, and a limit beyond one
     *                      restricts nothing. Long.MAX_VALUE when the caller
     *                      derives no offset from the result.
     */
    public static int resolveEntryCoverCount(MemoryR keyMem, long entryOffset, long entryEndLimit) {
        int rawStored = keyMem.getInt(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_COVERING_FORMAT);
        int genCount = keyMem.getInt(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_GEN_COUNT);
        long len = Math.min(
                keyMem.getLong(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_LEN),
                entryEndLimit - entryOffset
        );
        if (unpackCoveringFormat(rawStored) == PostingIndexUtils.COVERING_FORMAT_DEALIASED) {
            return limitCoverCountToEntrySize(unpackCoverCount(rawStored), genCount, len);
        }
        return coverCountFromLen(genCount, len);
    }

    public static int unpackCoveringFormat(int rawStored) {
        return rawStored & 0xFF;
    }

    /**
     * Legacy (format-0) cover footer offset: right after the gen-dir. Valid for
     * {@code coverCount==0} regardless of format. Prefer the 4-arg overload for
     * covering entries.
     */
    public static long resolveCoverFooterOffset(long entryOffset, int genCount) {
        return resolveCoverFooterOffset(entryOffset, genCount, PostingIndexUtils.COVERING_FORMAT_LEGACY, 0);
    }

    /**
     * Format-aware cover-end footer offset. Format 1: fixed at {@code entry+56}
     * (independent of GEN_COUNT — the de-alias). Format 0: trailing, at
     * {@code entry+56 + genCount*GEN_DIR_ENTRY_SIZE} (aliases gen-dir slot
     * genCount).
     */
    public static long resolveCoverFooterOffset(long entryOffset, int genCount, int coveringFormat, int coverCount) {
        if (coveringFormat == PostingIndexUtils.COVERING_FORMAT_DEALIASED) {
            return entryOffset + PostingIndexUtils.V2_ENTRY_HEADER_SIZE;
        }
        return entryOffset
                + PostingIndexUtils.V2_ENTRY_HEADER_SIZE
                + (long) genCount * PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
    }

    /**
     * Legacy (format-0 / no cover reserve) gen-dir offset. Correct for
     * {@code coverCount==0} and for format-0 covering entries. Prefer the 4-arg
     * overload for covering entries.
     */
    public static long resolveGenDirOffset(long entryOffset, int genIndex) {
        return resolveGenDirOffset(entryOffset, genIndex, PostingIndexUtils.COVERING_FORMAT_LEGACY, 0);
    }

    /**
     * Format-aware gen-dir offset. Format 1 shifts the gen-dir past the fixed
     * footer reserve ({@code coverCount*8}); format 0 puts it directly after the
     * header. Stride is {@code GEN_DIR_ENTRY_SIZE} in both.
     */
    public static long resolveGenDirOffset(long entryOffset, int genIndex, int coveringFormat, int coverCount) {
        return entryOffset
                + PostingIndexUtils.V2_ENTRY_HEADER_SIZE
                + coverFooterReserve(coveringFormat, coverCount)
                + (long) genIndex * PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
    }

    /**
     * Update the c-th cover end-offset footer slot in place. Used by
     * {@link PostingIndexChainWriter#extendHead} so a same-sealTxn gen flush
     * republishes the new sidecar extent atomically with VALUE_MEM_SIZE.
     * Format-aware: for format 1 the footer is at the fixed {@code entry+56}
     * offset, so appending a gen never overwrites it.
     */
    public static void writeCoverEndOffset(MemoryARW keyMem, long entryOffset, int genCount, int coverIndex, long endOffset, int coveringFormat, int coverCount) {
        long footerOffset = resolveCoverFooterOffset(entryOffset, genCount, coveringFormat, coverCount);
        keyMem.putLong(footerOffset + (long) coverIndex * PostingIndexUtils.COVER_END_OFFSET_ENTRY_SIZE, endOffset);
    }

    /**
     * Convenience overload that writes no cover end-offset footer. For
     * call sites that don't need covering (chain-writer tests, the empty
     * sentinel writes during truncate / initialiseEmpty).
     */
    public static void writeHeader(
            MemoryW keyMem,
            long entryOffset,
            long sealTxn,
            long txnAtSeal,
            long valueMemSize,
            long maxValue,
            int keyCount,
            int genCount,
            int blockCapacity,
            int coveringFormat,
            long prevEntryOffset
    ) {
        writeHeader(keyMem, entryOffset, sealTxn, txnAtSeal, valueMemSize, maxValue, keyCount,
                genCount, blockCapacity, coveringFormat, prevEntryOffset, null);
    }

    /**
     * Write a complete entry header at {@code entryOffset}. {@code txnAtSeal}
     * lands in slot[0]'s {@code TXN_AT_SEAL} field (single on-disk source of
     * truth for entry-level visibility) when {@code genCount > 0}. Callers
     * must write the rest of the gen dir payload separately. When
     * {@code coverEndOffsets} is non-null, also fills the cover footer.
     * <p>
     * The entry must be fully written and durable before
     * {@link PostingIndexChainHeader#publish} advances the chain head.
     */
    public static void writeHeader(
            MemoryW keyMem,
            long entryOffset,
            long sealTxn,
            long txnAtSeal,
            long valueMemSize,
            long maxValue,
            int keyCount,
            int genCount,
            int blockCapacity,
            int coveringFormat,
            long prevEntryOffset,
            LongList coverEndOffsets
    ) {
        int coverCount = coverEndOffsets != null ? coverEndOffsets.size() : 0;
        long len = entrySize(genCount, coverCount);
        keyMem.putLong(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_LEN, len);
        keyMem.putLong(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_SEAL_TXN, sealTxn);
        keyMem.putLong(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_VALUE_MEM_SIZE, valueMemSize);
        keyMem.putLong(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_MAX_VALUE, maxValue);
        keyMem.putInt(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_KEY_COUNT, keyCount);
        keyMem.putInt(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_GEN_COUNT, genCount);
        keyMem.putInt(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_BLOCK_CAPACITY, blockCapacity);
        keyMem.putInt(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_COVERING_FORMAT, packCoveringFormat(coveringFormat, coverCount));
        keyMem.putLong(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_PREV_ENTRY_OFFSET, prevEntryOffset);
        if (genCount > 0) {
            long slot0Offset = resolveGenDirOffset(entryOffset, 0, coveringFormat, coverCount);
            keyMem.putLong(slot0Offset + PostingIndexUtils.GEN_DIR_OFFSET_TXN_AT_SEAL, txnAtSeal);
        }
        if (coverCount > 0) {
            long footerOffset = resolveCoverFooterOffset(entryOffset, genCount, coveringFormat, coverCount);
            for (int c = 0; c < coverCount; c++) {
                keyMem.putLong(
                        footerOffset + (long) c * PostingIndexUtils.COVER_END_OFFSET_ENTRY_SIZE,
                        coverEndOffsets.getQuick(c)
                );
            }
        }
    }

    /**
     * Reduce a packed cover count to what the entry's bytes could hold. A correct
     * entry satisfies 56 + genCount*44 + coverCount*8 &lt;= len, so the space left
     * over never rejects a valid count; a wrong genCount only makes the limit
     * smaller, and a negative result gives 0 -- gen-dir at entry+56, empty footer,
     * nothing read past the entry.
     */
    private static int limitCoverCountToEntrySize(int packedCoverCount, int genCount, long len) {
        long maxCoverSlots = (len
                - PostingIndexUtils.V2_ENTRY_HEADER_SIZE
                - (long) genCount * PostingIndexUtils.GEN_DIR_ENTRY_SIZE)
                / PostingIndexUtils.COVER_END_OFFSET_ENTRY_SIZE;
        return (int) Math.min(packedCoverCount, Math.max(0L, maxCoverSlots));
    }

    /**
     * Reusable read snapshot. Fields populated by {@link #read}.
     */
    public static final class Snapshot {
        public final LongList coverFileEndOffsets = new LongList();
        public int blockCapacity;
        public int coverCount;
        public int coveringFormat;
        public int genCount;
        public long genDirOffset;
        public int keyCount;
        public long len;
        public long maxValue;
        public long offset;
        public long prevEntryOffset;
        public long sealTxn;
        public long txnAtSeal;
        public long valueMemSize;

        public void reset() {
            this.offset = 0;
            this.len = 0;
            this.sealTxn = 0;
            this.txnAtSeal = 0;
            this.valueMemSize = 0;
            this.maxValue = 0;
            this.keyCount = 0;
            this.genCount = 0;
            this.blockCapacity = 0;
            this.coverCount = 0;
            this.coveringFormat = 0;
            this.prevEntryOffset = PostingIndexUtils.V2_NO_HEAD;
            this.genDirOffset = 0;
            this.coverFileEndOffsets.clear();
        }
    }
}

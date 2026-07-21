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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.cairo.vm.api.MemoryW;
import io.questdb.std.LongList;
import io.questdb.std.Unsafe;

/**
 * Writer-side stateful helper that mirrors the v2 .pk chain header in
 * memory and provides the publish, extend and recovery primitives the
 * {@link PostingIndexWriter} needs to manage the chain.
 * <p>
 * Single-threaded: the design keeps a single writer per .pk file. There
 * is no internal synchronisation. The chain header's two-page seqlock,
 * applied via {@link PostingIndexChainHeader#publish}, is the only thing
 * that lets concurrent readers observe a consistent snapshot while this
 * class is mid-update.
 * <p>
 * Lifecycle:
 * <ol>
 *   <li>{@link #initialiseEmpty} — for a fresh .pk: writes the empty
 *       header pages and resets the in-memory state.</li>
 *   <li>{@link #openExisting} — for an existing .pk: reads the header
 *       and the head entry into in-memory state; rejects non-V2
 *       formats.</li>
 *   <li>{@link #recoveryDropAbandoned} — if the writer was reopened
 *       after a crash, drops chain entries that were published but never
 *       committed (txnAtSeal &gt; current table _txn).</li>
 *   <li>{@link #appendNewEntry} / {@link #extendHead} — used during
 *       the writer's seal and flush flows.</li>
 *   <li>{@link #resetState} — for close().</li>
 * </ol>
 * <p>
 * This class does <b>not</b> own the {@code keyMem} mapping — callers
 * pass it in on every call. The mapping must be wide enough to cover
 * the entire chain region (header pages + entry region high-water
 * mark).
 */
public final class PostingIndexChainWriter {

    private final PostingIndexChainEntry.Snapshot entryScratch = new PostingIndexChainEntry.Snapshot();
    private final PostingIndexChainHeader.Snapshot headerScratch = new PostingIndexChainHeader.Snapshot();
    private long activePageOffset;
    private long currentTxnAtSeal;
    private long entryCount;
    private long genCounter;
    private long headEntryOffset;
    // Cached sealTxn of the current head entry. Distinct from genCounter:
    // recoveryDropAbandoned can rewind headEntryOffset to a surviving
    // entry whose sealTxn is below the high-water genCounter (it cannot
    // safely rewind genCounter itself because dropped sealTxn .pv files
    // are still on disk awaiting purge). publishToChain consults this
    // instead of genCounter so post-recovery writes extend the survivor
    // rather than tripping the appendNewEntry monotonicity assertion.
    private long headSealTxn;
    private boolean isHeadTrimmedOnLastRecovery;
    private long regionBase;
    private long regionLimit;

    public PostingIndexChainWriter() {
        resetState();
    }

    /**
     * Convenience overload for callers without covering (e.g. chain-writer
     * tests). Equivalent to passing {@code coverEndOffsets = null}.
     */
    public long appendNewEntry(
            MemoryARW keyMem,
            long sealTxn,
            long txnAtSeal,
            long valueMemSize,
            long maxValue,
            int keyCount,
            int genCount,
            int blockCapacity,
            int coveringFormat
    ) {
        return appendNewEntry(keyMem, sealTxn, txnAtSeal, valueMemSize, maxValue, keyCount,
                genCount, blockCapacity, coveringFormat, null);
    }

    /**
     * Append a new immutable chain entry at the current {@code regionLimit}
     * and publish it as the new head. Caller must have already written the
     * gen-dir bytes at {@code (returnedEntryOffset + V2_ENTRY_HEADER_SIZE)};
     * otherwise readers will see uninitialised gen-dir bytes.
     * <p>
     * On return the {@code keyMem} is durable as far as user-space stores —
     * the caller is still responsible for syncing the file if durability
     * across power loss is required.
     *
     * @param keyMem          the .pk memory mapping (writable + readable)
     * @param sealTxn         the suffix for {@code .pv.{sealTxn}} and
     *                        {@code .pc{i}.{sealTxn}} files. Must be greater than
     *                        the current {@link #getGenCounter()} so monotonicity
     *                        is preserved across the chain.
     * @param txnAtSeal       the table {@code _txn} value this entry takes
     *                        effect at. Readers pin via the scoreboard before
     *                        querying and pick the entry where
     *                        {@code txnAtSeal <= pinned _txn}.
     * @param valueMemSize    bytes in {@code .pv.{sealTxn}}.
     * @param maxValue        highest row id covered by this entry.
     * @param keyCount        distinct keys at this seal.
     * @param genCount        number of gen-dir entries the caller has written
     *                        starting at the entry's gen-dir region.
     * @param blockCapacity   block capacity for this entry.
     * @param coveringFormat  reserved (currently 0). Lets sidecar formats
     *                        evolve per-seal in future.
     * @param coverEndOffsets per-cover-column authoritative valid-byte extent
     *                        in {@code .pc{i}.{sealTxn}}, written into the
     *                        entry's footer; {@code null} or empty means no
     *                        covering (footer omitted).
     * @return the byte offset where the new entry starts.
     */
    public long appendNewEntry(
            MemoryARW keyMem,
            long sealTxn,
            long txnAtSeal,
            long valueMemSize,
            long maxValue,
            int keyCount,
            int genCount,
            int blockCapacity,
            int coveringFormat,
            LongList coverEndOffsets
    ) {
        if (sealTxn <= genCounter) {
            throw CairoException.critical(0)
                    .put("posting index sealTxn must advance [current=").put(genCounter)
                    .put(", attempted=").put(sealTxn).put(']');
        }
        int coverCount = coverEndOffsets != null ? coverEndOffsets.size() : 0;
        long entryOffset = regionLimit;
        long prevHead = headEntryOffset;
        PostingIndexChainEntry.writeHeader(
                keyMem,
                entryOffset,
                sealTxn,
                txnAtSeal,
                valueMemSize,
                maxValue,
                keyCount,
                genCount,
                blockCapacity,
                coveringFormat,
                prevHead,
                coverEndOffsets
        );
        Unsafe.storeFence();
        // Update mirrors before publishing so accessors see the new state
        // by the time the publish becomes visible to readers.
        long newRegionLimit = entryOffset + PostingIndexChainEntry.entrySize(genCount, coverCount);
        headEntryOffset = entryOffset;
        regionLimit = newRegionLimit;
        entryCount++;
        genCounter = sealTxn;
        headSealTxn = sealTxn;
        currentTxnAtSeal = txnAtSeal;
        activePageOffset = PostingIndexChainHeader.publish(
                keyMem,
                activePageOffset,
                headEntryOffset,
                entryCount,
                regionBase,
                regionLimit,
                genCounter
        );
        return entryOffset;
    }

    /**
     * Convenience overload for callers without covering. Equivalent to
     * passing {@code newCoverEndOffsets = null}.
     */
    public void extendHead(
            MemoryARW keyMem,
            int newGenCount,
            int newKeyCount,
            long newValueMemSize,
            long newMaxValue
    ) {
        extendHead(keyMem, newGenCount, newKeyCount, newValueMemSize, newMaxValue, null);
    }

    /**
     * Extend the current head entry with a freshly-appended gen-dir entry.
     * Caller must have already written the new gen-dir bytes at offset
     * {@code (head + V2_ENTRY_HEADER_SIZE + currentGenCount * GEN_DIR_ENTRY_SIZE)}.
     * <p>
     * This bumps the head entry's GEN_COUNT, KEY_COUNT, LEN, VALUE_MEM_SIZE
     * and MAX_VALUE in place, then republishes the header so readers see the
     * new {@code regionLimit}. The fields are 4- or 8-byte aligned so single
     * stores are atomic on x86 / aarch64; combined with the storeFence after
     * the gen-dir bytes, this is the sparse-gen sub-protocol described in
     * section 4.5 of POSTING_INDEX_CHAIN_DESIGN.md.
     *
     * @param newCoverEndOffsets per-cover-column updated valid-byte extent in
     *                           {@code .pc{i}.{sealTxn}}, written into the
     *                           head entry's footer; {@code null} or empty
     *                           leaves the existing footer untouched.
     * @throws IllegalStateException if the chain is empty.
     */
    public void extendHead(
            MemoryARW keyMem,
            int newGenCount,
            int newKeyCount,
            long newValueMemSize,
            long newMaxValue,
            LongList newCoverEndOffsets
    ) {
        if (headEntryOffset == PostingIndexUtils.V2_NO_HEAD) {
            throw new IllegalStateException("posting index chain is empty; no head entry to extend");
        }
        int coverCount = newCoverEndOffsets != null ? newCoverEndOffsets.size() : 0;
        long newLen = PostingIndexChainEntry.entrySize(newGenCount, coverCount);
        // GEN_COUNT must be written LAST. It is the field readers latch on
        // to (via PostingIndexChainEntry.read which reads it first under a
        // loadFence) to gate visibility of all the other in-place updates,
        // including VALUE_MEM_SIZE which sizes the readers' valueMem
        // mapping and COVER_END_OFFSETS which size the readers' sidecar
        // mappings. Storing GEN_COUNT before those — even with a single
        // trailing storeFence — would let a reader observe a new GEN_COUNT
        // with old sizes and dereference past the mapping. The chain
        // header's outer seqlock does NOT cover these in-place stores:
        // extendHead mutates the entry before the publish() call that
        // bumps the seqlock.
        keyMem.putInt(headEntryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_KEY_COUNT, newKeyCount);
        keyMem.putLong(headEntryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_LEN, newLen);
        keyMem.putLong(headEntryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_VALUE_MEM_SIZE, newValueMemSize);
        keyMem.putLong(headEntryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_MAX_VALUE, newMaxValue);
        // Cover end-offset footer slots are at offset
        // (header + newGenCount * GEN_DIR_ENTRY_SIZE). The footer's location
        // depends on newGenCount, so we must write the cover sizes at the
        // location matching the GEN_COUNT we are about to publish.
        if (coverCount > 0) {
            for (int c = 0; c < coverCount; c++) {
                PostingIndexChainEntry.writeCoverEndOffset(
                        keyMem,
                        headEntryOffset,
                        newGenCount,
                        c,
                        newCoverEndOffsets.getQuick(c)
                );
            }
        }
        // Fence pairs with the loadFence after GEN_COUNT in
        // PostingIndexChainEntry.read: if a reader observes the new
        // GEN_COUNT, all stores above (and the gen-dir bytes the caller
        // wrote before this call) are also visible.
        Unsafe.storeFence();
        keyMem.putInt(headEntryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_GEN_COUNT, newGenCount);
        Unsafe.storeFence();
        regionLimit = headEntryOffset + newLen;
        activePageOffset = PostingIndexChainHeader.publish(
                keyMem,
                activePageOffset,
                headEntryOffset,
                entryCount,
                regionBase,
                regionLimit,
                genCounter
        );
    }

    public long getActivePageOffset() {
        return activePageOffset;
    }

    public long getCurrentTxnAtSeal() {
        return currentTxnAtSeal;
    }

    public long getEntryCount() {
        return entryCount;
    }

    public long getGenCounter() {
        return genCounter;
    }

    public long getHeadEntryOffset() {
        return headEntryOffset;
    }

    /**
     * Returns the sealTxn of the current head entry, or -1 if the chain
     * is empty. This may be lower than {@link #getGenCounter()} after
     * {@link #recoveryDropAbandoned} rewinds the head past abandoned
     * entries; the high-water genCounter is preserved so dropped
     * sealTxns are not reused while their .pv files await purge.
     */
    public long getHeadSealTxn() {
        return headSealTxn;
    }

    public long getRegionBase() {
        return regionBase;
    }

    public long getRegionLimit() {
        return regionLimit;
    }

    /**
     * Read the head entry's predecessor and return its {@code txnAtSeal}.
     * Used by the seal-purge accounting to compute the lower bound of the
     * visibility window for a file the just-published entry supersedes.
     * <p>
     * Returns {@code -1} when the chain has fewer than two entries (the
     * head was the very first seal — no superseded predecessor exists, so
     * callers should fall back to the column-instance creation txn). The
     * read goes through {@code keyMem} on every call: the helper does not
     * cache predecessors because the cost is one entry-size read and the
     * call rate is one-per-publish.
     */
    public long getSecondEntryTxnAtSeal(MemoryR keyMem) {
        if (headEntryOffset == PostingIndexUtils.V2_NO_HEAD || entryCount < 2) {
            return -1L;
        }
        PostingIndexChainEntry.read(keyMem, headEntryOffset, 0, entryScratch);
        long prevOffset = entryScratch.prevEntryOffset;
        if (prevOffset == PostingIndexUtils.V2_NO_HEAD) {
            return -1L;
        }
        PostingIndexChainEntry.read(keyMem, prevOffset, 0, entryScratch);
        return entryScratch.txnAtSeal;
    }

    public boolean hasHead() {
        return headEntryOffset != PostingIndexUtils.V2_NO_HEAD;
    }

    /**
     * Initialise both header pages on a fresh .pk file. Resets in-memory
     * state to the empty-chain defaults: the first append will use
     * {@code sealTxn = 0}.
     */
    public void initialiseEmpty(MemoryW keyMem) {
        initialiseEmpty(keyMem, 0L);
    }

    /**
     * Initialise both header pages on a fresh .pk file with a caller-supplied
     * starting sealTxn. The first {@link #appendNewEntry} call must use
     * exactly {@code startSealTxn}; subsequent appends must use strictly
     * larger values.
     */
    public void initialiseEmpty(MemoryW keyMem, long startSealTxn) {
        PostingIndexChainHeader.initialiseEmpty(keyMem, startSealTxn);
        resetState();
        genCounter = startSealTxn - 1L;
    }

    public boolean isHeadTrimmedOnLastRecovery() {
        return isHeadTrimmedOnLastRecovery;
    }

    /**
     * Read the current head entry's fields into {@code into}. Cheap: the
     * helper already cached the head pointer, so this is a single entry
     * read at the known offset. Throws {@link IllegalStateException} if
     * the chain is empty.
     */
    public void loadHeadEntry(MemoryR keyMem, PostingIndexChainEntry.Snapshot into) {
        if (headEntryOffset == PostingIndexUtils.V2_NO_HEAD) {
            throw new IllegalStateException("posting index chain is empty");
        }
        // Writer state-restore from the head entry uses only header fields
        // (sealTxn, valueMemSize, etc.); cover end-offsets in the entry are
        // re-established from the writer's own sidecarMems on reopen, so we
        // skip the cover footer read here.
        PostingIndexChainEntry.read(keyMem, headEntryOffset, 0, into);
    }

    /**
     * Open an existing .pk file: read the chain header, populate the in-memory
     * state from the header and the head entry. Rejects {@code FORMAT_VERSION}
     * values other than {@link PostingIndexUtils#V2_FORMAT_VERSION} with a
     * {@link CairoException}.
     */
    public void openExisting(MemoryR keyMem) {
        if (!PostingIndexChainHeader.readUnderSeqlock(keyMem, headerScratch)) {
            throw CairoException.critical(0).put("posting index header unreadable");
        }
        if (headerScratch.formatVersion != PostingIndexUtils.V2_FORMAT_VERSION) {
            throw CairoException.critical(0)
                    .put("Unsupported Posting index version [expected=")
                    .put(PostingIndexUtils.V2_FORMAT_VERSION)
                    .put(", actual=").put(headerScratch.formatVersion).put(']');
        }
        activePageOffset = headerScratch.pageOffset;
        headEntryOffset = headerScratch.headEntryOffset;
        regionBase = headerScratch.regionBase;
        regionLimit = headerScratch.regionLimit;
        entryCount = headerScratch.entryCount;
        genCounter = headerScratch.generationCounter;
        if (headEntryOffset != PostingIndexUtils.V2_NO_HEAD) {
            PostingIndexChainEntry.read(keyMem, headEntryOffset, 0, entryScratch);
            currentTxnAtSeal = entryScratch.txnAtSeal;
            headSealTxn = entryScratch.sealTxn;
        } else {
            currentTxnAtSeal = -1L;
            headSealTxn = -1L;
        }
    }

    /**
     * Returns the next sealTxn that {@link #appendNewEntry} will assign to
     * a new entry, without advancing any state. Use this to compute the
     * filenames for {@code .pv.{sealTxn}} and {@code .pc{i}.{sealTxn}} on
     * disk before staging the entry payload.
     */
    public long peekNextSealTxn() {
        return genCounter + 1L;
    }

    /**
     * Read only the chain header and return its {@code regionLimit} -- the
     * live-region high-water offset. Lets a caller size the {@code keyMem}
     * mapping from the header (which is self-consistent with the head entry
     * it references) instead of from {@code ff.length()}, which can lag the
     * writer that extended the .pk during parallel apply and leave the head
     * entry outside a short mapping. Needs only the header pages mapped.
     * <p>
     * Validates the header the same way {@link #openExisting} does (seqlock read
     * plus format version) and additionally range-checks the region descriptors
     * before the value is trusted to size a mapping: it rejects an inconsistent
     * region (a {@code regionBase} below the reserved window, or a
     * {@code regionLimit} that precedes {@code regionBase}) and an out-of-region
     * {@code headEntryOffset}. The head-pointer check is the important one for
     * the reopen path: the caller feeds the result to
     * {@code keyMem.jumpTo(regionLimit)} and then {@code openExisting} reads the
     * head entry at {@code headEntryOffset}, so a corrupt-but-version-consistent
     * header whose head points outside {@code [regionBase, regionLimit)} would be
     * read past the mapping (SIGSEGV in a {@code -da} build). These are the same
     * checks the positional-pread reader
     * {@code PostingIndexUtils#readSealTxnFromKeyFdTriState} enforces;
     * {@code regionLimit == regionBase} is the legitimate empty-region shape and
     * stays valid. This does not upper-bound {@code regionLimit} (neither does the
     * pread reader): a consistent-but-oversized value is caught downstream by the
     * allocation failing in {@code jumpTo}, whose error path releases the mapping
     * without truncating the .pk.
     */
    public long peekRegionLimit(MemoryR keyMem) {
        if (!PostingIndexChainHeader.readUnderSeqlock(keyMem, headerScratch)) {
            throw CairoException.critical(0).put("posting index header unreadable");
        }
        if (headerScratch.formatVersion != PostingIndexUtils.V2_FORMAT_VERSION) {
            throw CairoException.critical(0)
                    .put("Unsupported Posting index version [expected=")
                    .put(PostingIndexUtils.V2_FORMAT_VERSION)
                    .put(", actual=").put(headerScratch.formatVersion).put(']');
        }
        if (headerScratch.regionBase < PostingIndexUtils.KEY_FILE_RESERVED
                || headerScratch.regionLimit < headerScratch.regionBase) {
            throw CairoException.critical(0)
                    .put("posting index header has invalid region [regionBase=")
                    .put(headerScratch.regionBase)
                    .put(", regionLimit=").put(headerScratch.regionLimit).put(']');
        }
        if (headerScratch.headEntryOffset != PostingIndexUtils.V2_NO_HEAD
                && (headerScratch.headEntryOffset < headerScratch.regionBase
                || headerScratch.headEntryOffset >= headerScratch.regionLimit)) {
            throw CairoException.critical(0)
                    .put("posting index header has out-of-region head [headEntryOffset=")
                    .put(headerScratch.headEntryOffset)
                    .put(", regionBase=").put(headerScratch.regionBase)
                    .put(", regionLimit=").put(headerScratch.regionLimit).put(']');
        }
        return headerScratch.regionLimit;
    }

    /**
     * Read the current head entry's cover end-offset footer into {@code out}
     * (cleared first). Lets a caller preserve the footer across a republish it
     * cannot recompute -- e.g. a same-sealTxn gen flush whose writer-side
     * coverCount field is transiently 0 (between clearCovering and the next
     * configureCovering), or a freshly reopened per-partition writer that has
     * not yet captured the live sidecar extent. The head entry on disk carries
     * the authoritative footer from the seal that wrote the .pc, so reusing it
     * keeps the chain entry pointing at the existing covered data instead of
     * publishing an empty footer (which makes readers see the partition as
     * having no covered values). Leaves {@code out} empty when there is no head
     * or no cover footer.
     */
    public void readHeadCoverEndOffsets(MemoryARW keyMem, LongList out) {
        out.clear();
        if (!hasHead()) {
            return;
        }
        long headOffset = headEntryOffset;
        int genCount = keyMem.getInt(headOffset + PostingIndexUtils.V2_ENTRY_OFFSET_GEN_COUNT);
        long len = keyMem.getLong(headOffset + PostingIndexUtils.V2_ENTRY_OFFSET_LEN);
        long footerOffset = PostingIndexChainEntry.resolveCoverFooterOffset(headOffset, genCount);
        long footerBytesAvailable = Math.max(0L, headOffset + len - footerOffset);
        int coverCount = (int) (footerBytesAvailable / PostingIndexUtils.COVER_END_OFFSET_ENTRY_SIZE);
        for (int c = 0; c < coverCount; c++) {
            out.add(keyMem.getLong(footerOffset + (long) c * PostingIndexUtils.COVER_END_OFFSET_ENTRY_SIZE));
        }
    }

    /**
     * Walk the chain backwards from head and drop every entry whose
     * {@code txnAtSeal > currentTableTxn}. These are abandoned publishes
     * from a previous writer that crashed before {@code _txn} was
     * committed; they reference {@code .pv.{sealTxn}} and {@code .pc*.{sealTxn}}
     * files on disk that no reader can ever pin (since the {@code _txn}
     * never actually landed).
     * <p>
     * If at least one entry is dropped, the orphan {@code sealTxn} values
     * are appended to {@code orphanSealTxns} (in head-to-tail order, i.e.
     * newest first) so the caller can schedule the corresponding sidecar
     * files for deletion. The header is republished after the walk
     * completes so readers see the corrected chain.
     * <p>
     * The {@link #getGenCounter()} mirror does not regress, even when the
     * dropped entries had higher sealTxns than any retained entry. Reusing
     * those sealTxn values is unsafe: the on-disk {@code .pv.{sealTxn}}
     * and {@code .pc*.{sealTxn}} files may still exist (until the orphan
     * scan deletes them), so a future seal must use a strictly larger
     * sealTxn.
     *
     * @return the number of entries that were dropped.
     */
    public int recoveryDropAbandoned(MemoryARW keyMem, long currentTableTxn, LongList orphanSealTxns) {
        isHeadTrimmedOnLastRecovery = false;
        if (headEntryOffset == PostingIndexUtils.V2_NO_HEAD) {
            return 0;
        }
        int dropped = 0;
        long offset = headEntryOffset;
        long newHead = headEntryOffset;
        long newEntryCount = entryCount;
        long newRegionLimit = regionLimit;
        boolean isHeadTrimmed = false;
        // Walk strictly bounded by entryCount: a corrupted prev pointer that
        // loops back on itself would otherwise drop more entries than the
        // chain contains and, for an unbounded cycle, never terminate. The
        // picker uses the same defense for the same reason.
        long visited = 0;
        while (offset != PostingIndexUtils.V2_NO_HEAD) {
            if (visited++ >= entryCount) {
                throw CairoException.critical(0)
                        .put("posting index chain recovery exceeded entryCount; corrupted prev pointer? [entryCount=")
                        .put(entryCount).put(']');
            }
            // Each entry must lie inside the live region; an offset outside
            // that band is also a corruption signal.
            if (offset < regionBase || offset >= newRegionLimit) {
                throw CairoException.critical(0)
                        .put("posting index chain entry offset out of range [offset=")
                        .put(offset).put(", regionBase=").put(regionBase)
                        .put(", regionLimit=").put(newRegionLimit).put(']');
            }
            PostingIndexChainEntry.read(keyMem, offset, 0, entryScratch);
            if (entryScratch.txnAtSeal <= currentTableTxn) {
                // entry-level visible, but fast-path may have stuffed
                // in-flight gens in this entry's tail via extendHead. Trim
                // them off via per-slot TXN_AT_SEAL.
                int trimmedTo = trimInFlightTailGens(keyMem, offset, entryScratch.genCount, currentTableTxn);
                if (trimmedTo == 0) {
                    orphanSealTxns.add(entryScratch.sealTxn);
                    newHead = entryScratch.prevEntryOffset;
                    newEntryCount--;
                    newRegionLimit = offset;
                    dropped++;
                } else if (trimmedTo < entryScratch.genCount) {
                    isHeadTrimmed = true;
                    // Copy to virgin space past the ORIGINAL regionLimit, not
                    // newRegionLimit. When entries were dropped ahead of this
                    // one, newRegionLimit has been rewound onto the bytes of
                    // those just-dropped entries -- and those entries were
                    // previously published heads that a concurrent reader may
                    // still have cached. Writing the trimmed copy there would
                    // overwrite their bytes before the header republish below,
                    // letting a straddling reader pass its stillStable re-check
                    // on torn bytes. regionLimit is always virgin space past
                    // the head, so the source and every dropped entry stay
                    // byte-intact as unreachable gaps until GC.
                    long copyAt = regionLimit;
                    long copyLen = applyHeadTrim(keyMem, offset, trimmedTo, entryScratch.len, copyAt);
                    newHead = copyAt;
                    newRegionLimit = copyAt + copyLen;
                }
                break;
            }
            orphanSealTxns.add(entryScratch.sealTxn);
            newHead = entryScratch.prevEntryOffset;
            newEntryCount--;
            // The entry being dropped started at `offset`. After dropping,
            // the new high-water is exactly its starting offset.
            newRegionLimit = offset;
            offset = entryScratch.prevEntryOffset;
            dropped++;
        }
        if (dropped == 0 && !isHeadTrimmed) {
            return 0;
        }
        isHeadTrimmedOnLastRecovery = isHeadTrimmed;
        headEntryOffset = newHead;
        entryCount = newEntryCount;
        regionLimit = newRegionLimit;
        if (newHead != PostingIndexUtils.V2_NO_HEAD) {
            PostingIndexChainEntry.read(keyMem, newHead, 0, entryScratch);
            currentTxnAtSeal = entryScratch.txnAtSeal;
            headSealTxn = entryScratch.sealTxn;
        } else {
            currentTxnAtSeal = -1L;
            headSealTxn = -1L;
        }
        Unsafe.storeFence();
        activePageOffset = PostingIndexChainHeader.publish(
                keyMem,
                activePageOffset,
                headEntryOffset,
                entryCount,
                regionBase,
                regionLimit,
                genCounter
        );
        return dropped;
    }

    /**
     * Reset the in-memory state to the empty-chain defaults. Does <b>not</b>
     * touch any backing memory. Call this on close() so a subsequent
     * {@link #initialiseEmpty} or {@link #openExisting} starts from clean
     * state.
     */
    public void resetState() {
        activePageOffset = PostingIndexUtils.PAGE_A_OFFSET;
        headEntryOffset = PostingIndexUtils.V2_NO_HEAD;
        regionBase = PostingIndexUtils.V2_ENTRY_REGION_BASE;
        regionLimit = PostingIndexUtils.V2_ENTRY_REGION_BASE;
        entryCount = 0;
        // Default starting genCounter is -1 so the very first appendNewEntry
        // can use sealTxn=0, matching the historical .pv.0 filename
        // convention. Callers wanting a different starting sealTxn must use
        // initialiseEmpty(MemoryW, long) which overrides this.
        genCounter = -1L;
        headSealTxn = -1L;
        currentTxnAtSeal = -1L;
    }

    /**
     * Update only the head entry's MAX_VALUE field, leaving GEN_COUNT and
     * LEN unchanged. Used by the writer's setMaxValue path between gen
     * flushes when row ids advance but no new gen has been emitted yet.
     * <p>
     * Republishes the header so the seqlock advances and a concurrent
     * reader will pick up the new max-value on its next read. No-op when
     * the chain is empty (the first flush will create a head entry with
     * the right max-value).
     */
    public void updateHeadMaxValue(MemoryARW keyMem, long maxValue) {
        if (headEntryOffset == PostingIndexUtils.V2_NO_HEAD) {
            return;
        }
        keyMem.putLong(headEntryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_MAX_VALUE, maxValue);
        Unsafe.storeFence();
        activePageOffset = PostingIndexChainHeader.publish(
                keyMem,
                activePageOffset,
                headEntryOffset,
                entryCount,
                regionBase,
                regionLimit,
                genCounter
        );
    }

    /**
     * Write a trimmed copy of the source entry at {@code destOffset}
     * (retaining {@code keepGenCount} gens) and return its length.
     * Copy-on-write avoids in-place mutation; the chain header publish
     * at the end of {@link #recoveryDropAbandoned} atomically flips the
     * head pointer, so concurrent readers see either the untouched
     * source or the fully-written destination, never a half-state.
     * <p>
     * The destination reuses the source's {@code sealTxn} -- both
     * reference the same {@code .pv.{sealTxn}} / {@code .pc{i}.{sealTxn}}
     * files; the destination's gen-dir indexes a prefix of the same
     * value-file ranges. The source's bytes remain in the region as an
     * unreachable gap (no entry's {@code prevEntryOffset} points at it).
     */
    private long applyHeadTrim(MemoryARW keyMem, long sourceOffset, int keepGenCount, long sourceLen, long destOffset) {
        assert keepGenCount >= 1;
        long lastSlot = sourceOffset + PostingIndexUtils.V2_ENTRY_HEADER_SIZE
                + (long) (keepGenCount - 1) * PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
        long lastFileOffset = keyMem.getLong(lastSlot + PostingIndexUtils.GEN_DIR_OFFSET_FILE_OFFSET);
        long lastSize = keyMem.getLong(lastSlot + PostingIndexUtils.GEN_DIR_OFFSET_SIZE);
        long lastMaxValue = keyMem.getLong(lastSlot + PostingIndexUtils.GEN_DIR_OFFSET_MAX_VALUE);
        int newKeyCount = 0;
        for (int g = 0; g < keepGenCount; g++) {
            long slot = sourceOffset + PostingIndexUtils.V2_ENTRY_HEADER_SIZE
                    + (long) g * PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
            int slotMaxKey = keyMem.getInt(slot + PostingIndexUtils.GEN_DIR_OFFSET_MAX_KEY);
            if (slotMaxKey + 1 > newKeyCount) {
                newKeyCount = slotMaxKey + 1;
            }
        }
        // coverPlusPadding's up-to-7-byte trailing pad rounds down on / 8.
        int oldGenCount = keyMem.getInt(sourceOffset + PostingIndexUtils.V2_ENTRY_OFFSET_GEN_COUNT);
        long coverPlusPadding = sourceLen
                - PostingIndexUtils.V2_ENTRY_HEADER_SIZE
                - (long) oldGenCount * PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
        int coverCount = coverPlusPadding > 0
                ? (int) (coverPlusPadding / PostingIndexUtils.COVER_END_OFFSET_ENTRY_SIZE)
                : 0;
        long newLen = PostingIndexChainEntry.entrySize(keepGenCount, coverCount);

        long sourceSealTxn = keyMem.getLong(sourceOffset + PostingIndexUtils.V2_ENTRY_OFFSET_SEAL_TXN);
        int blockCapacity = keyMem.getInt(sourceOffset + PostingIndexUtils.V2_ENTRY_OFFSET_BLOCK_CAPACITY);
        int coveringFormat = keyMem.getInt(sourceOffset + PostingIndexUtils.V2_ENTRY_OFFSET_COVERING_FORMAT);
        long sourcePrevEntryOffset = keyMem.getLong(sourceOffset + PostingIndexUtils.V2_ENTRY_OFFSET_PREV_ENTRY_OFFSET);

        keyMem.putLong(destOffset + PostingIndexUtils.V2_ENTRY_OFFSET_LEN, newLen);
        keyMem.putLong(destOffset + PostingIndexUtils.V2_ENTRY_OFFSET_SEAL_TXN, sourceSealTxn);
        keyMem.putLong(destOffset + PostingIndexUtils.V2_ENTRY_OFFSET_VALUE_MEM_SIZE, lastFileOffset + lastSize);
        keyMem.putLong(destOffset + PostingIndexUtils.V2_ENTRY_OFFSET_MAX_VALUE, lastMaxValue);
        keyMem.putInt(destOffset + PostingIndexUtils.V2_ENTRY_OFFSET_KEY_COUNT, newKeyCount);
        keyMem.putInt(destOffset + PostingIndexUtils.V2_ENTRY_OFFSET_GEN_COUNT, keepGenCount);
        keyMem.putInt(destOffset + PostingIndexUtils.V2_ENTRY_OFFSET_BLOCK_CAPACITY, blockCapacity);
        keyMem.putInt(destOffset + PostingIndexUtils.V2_ENTRY_OFFSET_COVERING_FORMAT, coveringFormat);
        keyMem.putLong(destOffset + PostingIndexUtils.V2_ENTRY_OFFSET_PREV_ENTRY_OFFSET, sourcePrevEntryOffset);

        // Slot layout mixes long and int fields; copy field-by-field.
        long sourceGenDir = sourceOffset + PostingIndexUtils.V2_ENTRY_HEADER_SIZE;
        long destGenDir = destOffset + PostingIndexUtils.V2_ENTRY_HEADER_SIZE;
        for (int g = 0; g < keepGenCount; g++) {
            long sSlot = sourceGenDir + (long) g * PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
            long dSlot = destGenDir + (long) g * PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
            keyMem.putLong(dSlot + PostingIndexUtils.GEN_DIR_OFFSET_FILE_OFFSET,
                    keyMem.getLong(sSlot + PostingIndexUtils.GEN_DIR_OFFSET_FILE_OFFSET));
            keyMem.putLong(dSlot + PostingIndexUtils.GEN_DIR_OFFSET_SIZE,
                    keyMem.getLong(sSlot + PostingIndexUtils.GEN_DIR_OFFSET_SIZE));
            keyMem.putInt(dSlot + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT,
                    keyMem.getInt(sSlot + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT));
            keyMem.putInt(dSlot + PostingIndexUtils.GEN_DIR_OFFSET_MIN_KEY,
                    keyMem.getInt(sSlot + PostingIndexUtils.GEN_DIR_OFFSET_MIN_KEY));
            keyMem.putInt(dSlot + PostingIndexUtils.GEN_DIR_OFFSET_MAX_KEY,
                    keyMem.getInt(sSlot + PostingIndexUtils.GEN_DIR_OFFSET_MAX_KEY));
            keyMem.putLong(dSlot + PostingIndexUtils.GEN_DIR_OFFSET_TXN_AT_SEAL,
                    keyMem.getLong(sSlot + PostingIndexUtils.GEN_DIR_OFFSET_TXN_AT_SEAL));
            keyMem.putLong(dSlot + PostingIndexUtils.GEN_DIR_OFFSET_MAX_VALUE,
                    keyMem.getLong(sSlot + PostingIndexUtils.GEN_DIR_OFFSET_MAX_VALUE));
        }

        if (coverCount > 0) {
            long sourceFooterOffset = sourceOffset + PostingIndexUtils.V2_ENTRY_HEADER_SIZE
                    + (long) oldGenCount * PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
            long destFooterOffset = destOffset + PostingIndexUtils.V2_ENTRY_HEADER_SIZE
                    + (long) keepGenCount * PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
            for (int c = 0; c < coverCount; c++) {
                keyMem.putLong(destFooterOffset + (long) c * PostingIndexUtils.COVER_END_OFFSET_ENTRY_SIZE,
                        keyMem.getLong(sourceFooterOffset + (long) c * PostingIndexUtils.COVER_END_OFFSET_ENTRY_SIZE));
            }
        }

        Unsafe.storeFence();
        return newLen;
    }

    /**
     * Returns the number of head-entry gen-dir slots to keep, trimming the
     * tail of slots whose TXN_AT_SEAL exceeds {@code currentTableTxn}.
     */
    private int trimInFlightTailGens(MemoryARW keyMem, long entryOffset, int genCount, long currentTableTxn) {
        Unsafe.loadFence();
        int keep = genCount;
        while (keep > 0) {
            long slotOffset = entryOffset + PostingIndexUtils.V2_ENTRY_HEADER_SIZE
                    + (long) (keep - 1) * PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
            long slotTxnAtSeal = keyMem.getLong(slotOffset + PostingIndexUtils.GEN_DIR_OFFSET_TXN_AT_SEAL);
            if (slotTxnAtSeal <= currentTableTxn) {
                break;
            }
            keep--;
        }
        return keep;
    }
}

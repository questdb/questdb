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
    private long regionBase;
    private long regionLimit;

    public PostingIndexChainWriter() {
        resetState();
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
     * @param keyMem         the .pk memory mapping (writable + readable)
     * @param sealTxn        the suffix for {@code .pv.{sealTxn}} and
     *                       {@code .pc{i}.{sealTxn}} files. Must be greater than
     *                       the current {@link #getGenCounter()} so monotonicity
     *                       is preserved across the chain.
     * @param txnAtSeal      the table {@code _txn} value this entry takes
     *                       effect at. Readers pin via the scoreboard before
     *                       querying and pick the entry where
     *                       {@code txnAtSeal <= pinned _txn}.
     * @param valueMemSize   bytes in {@code .pv.{sealTxn}}.
     * @param maxValue       highest row id covered by this entry.
     * @param keyCount       distinct keys at this seal.
     * @param genCount       number of gen-dir entries the caller has written
     *                       starting at the entry's gen-dir region.
     * @param blockCapacity  block capacity for this entry.
     * @param coveringFormat reserved (currently 0). Lets sidecar formats
     *                       evolve per-seal in future.
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
            int coveringFormat
    ) {
        if (sealTxn <= genCounter) {
            throw CairoException.critical(0)
                    .put("posting index sealTxn must advance [current=").put(genCounter)
                    .put(", attempted=").put(sealTxn).put(']');
        }
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
                prevHead
        );
        Unsafe.storeFence();
        // Update mirrors before publishing so accessors see the new state
        // by the time the publish becomes visible to readers.
        long newRegionLimit = entryOffset + PostingIndexChainEntry.entrySize(genCount);
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
     * @throws IllegalStateException if the chain is empty.
     */
    public void extendHead(
            MemoryARW keyMem,
            int newGenCount,
            int newKeyCount,
            long newValueMemSize,
            long newMaxValue
    ) {
        if (headEntryOffset == PostingIndexUtils.V2_NO_HEAD) {
            throw new IllegalStateException("posting index chain is empty; no head entry to extend");
        }
        long newLen = PostingIndexChainEntry.entrySize(newGenCount);
        // GEN_COUNT must be written LAST. It is the field readers latch on
        // to (via PostingIndexChainEntry.read which reads it first under a
        // loadFence) to gate visibility of all the other in-place updates,
        // including VALUE_MEM_SIZE which sizes the readers' valueMem
        // mapping. Storing GEN_COUNT before VALUE_MEM_SIZE — even with a
        // single trailing storeFence — would let a reader observe a new
        // GEN_COUNT with an old VALUE_MEM_SIZE and dereference past the
        // mapping. The chain header's outer seqlock does NOT cover these
        // in-place stores: extendHead mutates the entry before the
        // publish() call that bumps the seqlock.
        keyMem.putInt(headEntryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_KEY_COUNT, newKeyCount);
        keyMem.putLong(headEntryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_LEN, newLen);
        keyMem.putLong(headEntryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_VALUE_MEM_SIZE, newValueMemSize);
        keyMem.putLong(headEntryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_MAX_VALUE, newMaxValue);
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
        PostingIndexChainEntry.read(keyMem, headEntryOffset, entryScratch);
        long prevOffset = entryScratch.prevEntryOffset;
        if (prevOffset == PostingIndexUtils.V2_NO_HEAD) {
            return -1L;
        }
        PostingIndexChainEntry.read(keyMem, prevOffset, entryScratch);
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
        PostingIndexChainEntry.read(keyMem, headEntryOffset, into);
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
            PostingIndexChainEntry.read(keyMem, headEntryOffset, entryScratch);
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
        if (headEntryOffset == PostingIndexUtils.V2_NO_HEAD) {
            return 0;
        }
        int dropped = 0;
        long offset = headEntryOffset;
        long newHead = headEntryOffset;
        long newEntryCount = entryCount;
        long newRegionLimit = regionLimit;
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
            PostingIndexChainEntry.read(keyMem, offset, entryScratch);
            if (entryScratch.txnAtSeal <= currentTableTxn) {
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
        if (dropped == 0) {
            return 0;
        }
        headEntryOffset = newHead;
        entryCount = newEntryCount;
        regionLimit = newRegionLimit;
        if (newHead != PostingIndexUtils.V2_NO_HEAD) {
            PostingIndexChainEntry.read(keyMem, newHead, entryScratch);
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
}

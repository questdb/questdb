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
import io.questdb.std.Unsafe;

/**
 * Reads and writes the v2 .pk header page. The header is the only mutable
 * region in the v2 layout; it advertises the chain head, entry count, and
 * the high-water mark of the entry region.
 * <p>
 * The header lives in two seqlock-protected pages (A at offset 0, B at
 * offset 4096). Writers always update the inactive page and then flip via
 * the seqlock. Readers always go through {@link #readUnderSeqlock} which
 * picks the consistent page and retries on a torn read.
 * <p>
 * This class is a stateless helper. It does not own any memory; callers
 * pass in the .pk memory mapping.
 */
public final class PostingIndexChainHeader {

    /**
     * Snapshot of the active header fields, populated by
     * {@link #readUnderSeqlock(MemoryR, Snapshot)}. Reusable: callers can
     * keep one instance and pass it in repeatedly to avoid allocation on
     * the hot path.
     */
    public static final class Snapshot {
        public long entryCount;
        public long formatVersion;
        public long generationCounter;
        public long headEntryOffset;
        public long pageOffset;
        public long regionBase;
        public long regionLimit;

        public boolean isEmpty() {
            return headEntryOffset == PostingIndexUtils.V2_NO_HEAD;
        }

        public void reset() {
            this.formatVersion = 0;
            this.headEntryOffset = PostingIndexUtils.V2_NO_HEAD;
            this.entryCount = 0;
            this.regionBase = 0;
            this.regionLimit = 0;
            this.generationCounter = 0;
            this.pageOffset = -1;
        }
    }

    private PostingIndexChainHeader() {
    }

    /**
     * Initialise both header pages on a freshly-created .pk file. Page A is
     * left in the active state with an empty chain; Page B is zeroed and
     * stays inactive until the first publish.
     */
    public static void initialiseEmpty(MemoryW keyMem) {
        // Page A: active, empty chain.
        writePageRaw(
                keyMem,
                PostingIndexUtils.PAGE_A_OFFSET,
                /* sequence */ 2L,
                PostingIndexUtils.V2_FORMAT_VERSION,
                PostingIndexUtils.V2_NO_HEAD,
                /* entryCount */ 0L,
                /* regionBase */ PostingIndexUtils.V2_ENTRY_REGION_BASE,
                /* regionLimit */ PostingIndexUtils.V2_ENTRY_REGION_BASE,
                /* genCounter */ 0L
        );
        // Page B: inactive (sequence=0). Will be picked up on first publish.
        writePageRaw(
                keyMem,
                PostingIndexUtils.PAGE_B_OFFSET,
                /* sequence */ 0L,
                /* formatVersion */ 0L,
                PostingIndexUtils.V2_NO_HEAD,
                0L, 0L, 0L, 0L
        );
    }

    /**
     * Publish a new header state to the inactive page and seqlock-flip it
     * to active. The previously-active page becomes inactive; readers still
     * mid-flight on it complete their seqlock retry and pick up the new
     * page on the next iteration.
     *
     * @param keyMem            the .pk memory mapping (writable)
     * @param activePageOffset  current active page offset (PAGE_A_OFFSET or PAGE_B_OFFSET)
     * @param newHeadEntryOffset byte offset of the latest entry (V2_NO_HEAD if empty)
     * @param newEntryCount      number of live entries in the chain
     * @param newRegionBase      byte offset of the oldest live entry's start
     * @param newRegionLimit     high-water byte offset past the last entry
     * @param newGenCounter      monotonic generation counter (last-assigned sealTxn)
     * @return the new active page offset (the previously-inactive one)
     */
    public static long publish(
            MemoryARW keyMem,
            long activePageOffset,
            long newHeadEntryOffset,
            long newEntryCount,
            long newRegionBase,
            long newRegionLimit,
            long newGenCounter
    ) {
        long inactiveOffset = activePageOffset == PostingIndexUtils.PAGE_A_OFFSET
                ? PostingIndexUtils.PAGE_B_OFFSET
                : PostingIndexUtils.PAGE_A_OFFSET;

        long currentActiveSeq = keyMem.getLong(activePageOffset + PostingIndexUtils.V2_HEADER_OFFSET_SEQUENCE_START);
        // New sequence is at least active+2 and even. The seqlock convention
        // is: even=stable, odd=in-progress. Bump active by +2 (skip an odd
        // value) so readers can distinguish "stale inactive" from "fresh
        // inactive" by sequence ordering.
        long newSeq = (currentActiveSeq | 1L) + 1L;

        // Phase 1: stage with sequence == newSeq | 1 (odd, in-progress).
        long stagingSeq = newSeq | 1L;
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(inactiveOffset + PostingIndexUtils.V2_HEADER_OFFSET_SEQUENCE_START, stagingSeq);
        keyMem.putLong(inactiveOffset + PostingIndexUtils.V2_HEADER_OFFSET_SEQUENCE_END, stagingSeq);

        // Phase 2: write payload.
        keyMem.putLong(inactiveOffset + PostingIndexUtils.V2_HEADER_OFFSET_FORMAT_VERSION, PostingIndexUtils.V2_FORMAT_VERSION);
        keyMem.putLong(inactiveOffset + PostingIndexUtils.V2_HEADER_OFFSET_HEAD_ENTRY_OFFSET, newHeadEntryOffset);
        keyMem.putLong(inactiveOffset + PostingIndexUtils.V2_HEADER_OFFSET_ENTRY_COUNT, newEntryCount);
        keyMem.putLong(inactiveOffset + PostingIndexUtils.V2_HEADER_OFFSET_REGION_BASE, newRegionBase);
        keyMem.putLong(inactiveOffset + PostingIndexUtils.V2_HEADER_OFFSET_REGION_LIMIT, newRegionLimit);
        keyMem.putLong(inactiveOffset + PostingIndexUtils.V2_HEADER_OFFSET_GEN_COUNTER, newGenCounter);

        // Phase 3: finalise with even sequence == newSeq, releasing the page.
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(inactiveOffset + PostingIndexUtils.V2_HEADER_OFFSET_SEQUENCE_END, newSeq);
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(inactiveOffset + PostingIndexUtils.V2_HEADER_OFFSET_SEQUENCE_START, newSeq);

        return inactiveOffset;
    }

    /**
     * Read the active header into {@code into}. Performs a seqlock-protected
     * read with bounded retries. Returns true if a consistent snapshot was
     * obtained, false if all retries failed (caller should treat as "header
     * unreadable" and propagate as an error).
     * <p>
     * Picks whichever page (A or B) has a higher even sequence number.
     * Both pages may transiently be in an in-progress (odd) state during a
     * publish; the loop retries until one settles.
     */
    public static boolean readUnderSeqlock(MemoryR keyMem, Snapshot into) {
        for (int attempt = 0; attempt < 16; attempt++) {
            long seqStartA = keyMem.getLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.V2_HEADER_OFFSET_SEQUENCE_START);
            long seqEndA = keyMem.getLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.V2_HEADER_OFFSET_SEQUENCE_END);
            long seqStartB = keyMem.getLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.V2_HEADER_OFFSET_SEQUENCE_START);
            long seqEndB = keyMem.getLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.V2_HEADER_OFFSET_SEQUENCE_END);
            Unsafe.getUnsafe().loadFence();

            boolean aStable = seqStartA == seqEndA && (seqStartA & 1L) == 0L && seqStartA != 0L;
            boolean bStable = seqStartB == seqEndB && (seqStartB & 1L) == 0L && seqStartB != 0L;
            long pageOffset;
            long expectedSeq;
            if (aStable && bStable) {
                pageOffset = seqStartA >= seqStartB ? PostingIndexUtils.PAGE_A_OFFSET : PostingIndexUtils.PAGE_B_OFFSET;
                expectedSeq = pageOffset == PostingIndexUtils.PAGE_A_OFFSET ? seqStartA : seqStartB;
            } else if (aStable) {
                pageOffset = PostingIndexUtils.PAGE_A_OFFSET;
                expectedSeq = seqStartA;
            } else if (bStable) {
                pageOffset = PostingIndexUtils.PAGE_B_OFFSET;
                expectedSeq = seqStartB;
            } else {
                // Both pages mid-write. Spin briefly and retry.
                continue;
            }

            long formatVersion = keyMem.getLong(pageOffset + PostingIndexUtils.V2_HEADER_OFFSET_FORMAT_VERSION);
            long headEntryOffset = keyMem.getLong(pageOffset + PostingIndexUtils.V2_HEADER_OFFSET_HEAD_ENTRY_OFFSET);
            long entryCount = keyMem.getLong(pageOffset + PostingIndexUtils.V2_HEADER_OFFSET_ENTRY_COUNT);
            long regionBase = keyMem.getLong(pageOffset + PostingIndexUtils.V2_HEADER_OFFSET_REGION_BASE);
            long regionLimit = keyMem.getLong(pageOffset + PostingIndexUtils.V2_HEADER_OFFSET_REGION_LIMIT);
            long genCounter = keyMem.getLong(pageOffset + PostingIndexUtils.V2_HEADER_OFFSET_GEN_COUNTER);
            Unsafe.getUnsafe().loadFence();

            // Re-read the seqlock pair on the picked page; if either changed,
            // retry. This closes the window where a writer flipped to this
            // page between our pick and our payload reads.
            long postSeqStart = keyMem.getLong(pageOffset + PostingIndexUtils.V2_HEADER_OFFSET_SEQUENCE_START);
            long postSeqEnd = keyMem.getLong(pageOffset + PostingIndexUtils.V2_HEADER_OFFSET_SEQUENCE_END);
            if (postSeqStart != expectedSeq || postSeqEnd != expectedSeq) {
                continue;
            }

            into.formatVersion = formatVersion;
            into.headEntryOffset = headEntryOffset;
            into.entryCount = entryCount;
            into.regionBase = regionBase;
            into.regionLimit = regionLimit;
            into.generationCounter = genCounter;
            into.pageOffset = pageOffset;
            return true;
        }
        return false;
    }

    private static void writePageRaw(
            MemoryW keyMem,
            long pageOffset,
            long sequence,
            long formatVersion,
            long headEntryOffset,
            long entryCount,
            long regionBase,
            long regionLimit,
            long genCounter
    ) {
        keyMem.putLong(pageOffset + PostingIndexUtils.V2_HEADER_OFFSET_SEQUENCE_START, sequence);
        keyMem.putLong(pageOffset + PostingIndexUtils.V2_HEADER_OFFSET_FORMAT_VERSION, formatVersion);
        keyMem.putLong(pageOffset + PostingIndexUtils.V2_HEADER_OFFSET_HEAD_ENTRY_OFFSET, headEntryOffset);
        keyMem.putLong(pageOffset + PostingIndexUtils.V2_HEADER_OFFSET_ENTRY_COUNT, entryCount);
        keyMem.putLong(pageOffset + PostingIndexUtils.V2_HEADER_OFFSET_REGION_BASE, regionBase);
        keyMem.putLong(pageOffset + PostingIndexUtils.V2_HEADER_OFFSET_REGION_LIMIT, regionLimit);
        keyMem.putLong(pageOffset + PostingIndexUtils.V2_HEADER_OFFSET_GEN_COUNTER, genCounter);
        keyMem.putLong(pageOffset + PostingIndexUtils.V2_HEADER_OFFSET_SEQUENCE_END, sequence);
    }
}

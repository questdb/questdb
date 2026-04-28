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

import io.questdb.cairo.vm.api.MemoryR;

/**
 * Picks the chain entry that a reader pinned at {@code pinnedTableTxn}
 * should use. The rule is: select the entry with the largest
 * {@code txnAtSeal} that is {@code <= pinnedTableTxn}.
 * <p>
 * Entries with {@code txnAtSeal > pinnedTableTxn} are either:
 * <ul>
 *   <li>uncommitted publishes whose corresponding {@code txWriter.commit()}
 *       has not yet landed (transient state during a healthy commit), or
 *   <li>abandoned publishes from a writer that distressed before commit
 *       (will be reclaimed by the next writer-open recovery pass).
 * </ul>
 * In both cases the reader skips them. The strict-pin invariant guarantees
 * no reader will ever be pinned at a {@code _txn} value that hasn't
 * actually committed, so an entry with {@code txnAtSeal > pinnedTableTxn}
 * cannot be the right one for this reader.
 */
public final class PostingIndexChainPicker {

    public static final int RESULT_EMPTY_CHAIN = 1;
    public static final int RESULT_HEADER_UNREADABLE = 3;
    public static final int RESULT_NO_VISIBLE_ENTRY = 2;
    public static final int RESULT_OK = 0;

    private PostingIndexChainPicker() {
    }

    /**
     * Walk the chain backwards from head and pick the first entry where
     * {@code txnAtSeal <= pinnedTableTxn}. Populates {@code into} with the
     * picked entry on success.
     *
     * @return one of the RESULT_* codes:
     *   <ul>
     *     <li>{@code RESULT_OK} — entry was found and {@code into} is populated;</li>
     *     <li>{@code RESULT_EMPTY_CHAIN} — chain has no entries at all;</li>
     *     <li>{@code RESULT_NO_VISIBLE_ENTRY} — chain is non-empty but every
     *         entry has {@code txnAtSeal > pinnedTableTxn} (only happens
     *         immediately after a snapshot restore or pre-first-seal);</li>
     *     <li>{@code RESULT_HEADER_UNREADABLE} — header seqlock retries
     *         exhausted; caller should treat as I/O error.</li>
     *   </ul>
     */
    public static int pick(
            MemoryR keyMem,
            long pinnedTableTxn,
            PostingIndexChainHeader.Snapshot headerScratch,
            PostingIndexChainEntry.Snapshot into
    ) {
        if (!PostingIndexChainHeader.readUnderSeqlock(keyMem, headerScratch)) {
            return RESULT_HEADER_UNREADABLE;
        }
        if (headerScratch.isEmpty()) {
            return RESULT_EMPTY_CHAIN;
        }

        long entryOffset = headerScratch.headEntryOffset;
        long regionBase = headerScratch.regionBase;
        long regionLimit = headerScratch.regionLimit;
        long entryCount = headerScratch.entryCount;
        long visited = 0;

        while (entryOffset != PostingIndexUtils.V2_NO_HEAD) {
            // Sanity-check: the offset must lie inside the live region.
            // A corrupted PREV_ENTRY_OFFSET would otherwise drive the walk
            // off into garbage memory.
            if (entryOffset < regionBase || entryOffset >= regionLimit) {
                return RESULT_HEADER_UNREADABLE;
            }
            // Hard cap on iterations to defend against a corrupted prev
            // pointer that loops back on itself.
            if (visited++ > entryCount) {
                return RESULT_HEADER_UNREADABLE;
            }

            PostingIndexChainEntry.read(keyMem, entryOffset, into);
            if (into.txnAtSeal <= pinnedTableTxn) {
                return RESULT_OK;
            }
            entryOffset = into.prevEntryOffset;
        }
        // Walked off the chain — every entry's txnAtSeal exceeded pinnedTableTxn.
        return RESULT_NO_VISIBLE_ENTRY;
    }
}

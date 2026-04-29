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
import io.questdb.cairo.vm.api.MemoryW;
import io.questdb.std.Unsafe;

/**
 * Reads and writes individual seal entries in the v2 .pk chain.
 * <p>
 * An entry is immutable once appended. The only mutation that ever happens
 * to an entry's bytes is when the writer reclaims the entry's region during
 * GC — at which point the entry is no longer reachable from the chain head
 * and no reader can be pinned on it.
 * <p>
 * Entry layout (header is V2_ENTRY_HEADER_SIZE = 64 bytes):
 * <pre>
 *   [0..7]    LEN
 *   [8..15]   SEAL_TXN
 *   [16..23]  TXN_AT_SEAL
 *   [24..31]  VALUE_MEM_SIZE
 *   [32..39]  MAX_VALUE
 *   [40..43]  KEY_COUNT
 *   [44..47]  GEN_COUNT
 *   [48..51]  BLOCK_CAPACITY
 *   [52..55]  COVERING_FORMAT (reserved)
 *   [56..63]  PREV_ENTRY_OFFSET
 *   [64..]    GEN_DIR  (GEN_COUNT * GEN_DIR_ENTRY_SIZE bytes)
 * </pre>
 */
public final class PostingIndexChainEntry {

    /**
     * Reusable read snapshot. Fields populated by {@link #read}.
     */
    public static final class Snapshot {
        public int blockCapacity;
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
            this.coveringFormat = 0;
            this.prevEntryOffset = PostingIndexUtils.V2_NO_HEAD;
            this.genDirOffset = 0;
        }
    }

    private PostingIndexChainEntry() {
    }

    /**
     * Total entry size in bytes for a given gen count: header + gen dir
     * payload, padded to an 8-byte boundary.
     */
    public static int entrySize(int genCount) {
        long bytes = (long) PostingIndexUtils.V2_ENTRY_HEADER_SIZE
                + (long) genCount * (long) PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
        // Round up to 8 to keep subsequent entries 8-byte aligned.
        bytes = (bytes + 7L) & ~7L;
        return (int) bytes;
    }

    /**
     * Read an entry header at {@code entryOffset} into {@code into}. Returns
     * the offset just past this entry's bytes (useful for forward scans).
     */
    public static long read(MemoryR keyMem, long entryOffset, Snapshot into) {
        into.offset = entryOffset;
        // GEN_COUNT must be read FIRST. Pairs with the storeFence-guarded
        // GEN_COUNT store in PostingIndexChainWriter.extendHead: the writer
        // updates KEY_COUNT, LEN, VALUE_MEM_SIZE and MAX_VALUE in place,
        // fences, then bumps GEN_COUNT last. By reading GEN_COUNT first
        // and fencing after, this reader sees an old GEN_COUNT (with
        // matching old fields) or a new GEN_COUNT (with matching new
        // fields), but never new GEN_COUNT with old VALUE_MEM_SIZE.
        into.genCount = keyMem.getInt(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_GEN_COUNT);
        Unsafe.loadFence();
        into.len = keyMem.getLong(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_LEN);
        into.sealTxn = keyMem.getLong(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_SEAL_TXN);
        into.txnAtSeal = keyMem.getLong(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_TXN_AT_SEAL);
        into.valueMemSize = keyMem.getLong(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_VALUE_MEM_SIZE);
        into.maxValue = keyMem.getLong(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_MAX_VALUE);
        into.keyCount = keyMem.getInt(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_KEY_COUNT);
        into.blockCapacity = keyMem.getInt(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_BLOCK_CAPACITY);
        into.coveringFormat = keyMem.getInt(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_COVERING_FORMAT);
        into.prevEntryOffset = keyMem.getLong(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_PREV_ENTRY_OFFSET);
        into.genDirOffset = entryOffset + PostingIndexUtils.V2_ENTRY_HEADER_SIZE;
        return entryOffset + into.len;
    }

    /**
     * Compute the byte offset of a single gen-dir entry within an entry.
     */
    public static long resolveGenDirOffset(long entryOffset, int genIndex) {
        return entryOffset
                + PostingIndexUtils.V2_ENTRY_HEADER_SIZE
                + (long) genIndex * PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
    }

    /**
     * Write a complete entry header at {@code entryOffset}. Caller must
     * write the gen dir payload itself afterward (or before the entry is
     * made visible by publishing the chain head).
     * <p>
     * The entry must be fully written and durable on disk before
     * {@link PostingIndexChainHeader#publish} advances the chain head;
     * otherwise a reader could observe a stale entry.
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
        long len = entrySize(genCount);
        keyMem.putLong(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_LEN, len);
        keyMem.putLong(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_SEAL_TXN, sealTxn);
        keyMem.putLong(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_TXN_AT_SEAL, txnAtSeal);
        keyMem.putLong(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_VALUE_MEM_SIZE, valueMemSize);
        keyMem.putLong(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_MAX_VALUE, maxValue);
        keyMem.putInt(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_KEY_COUNT, keyCount);
        keyMem.putInt(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_GEN_COUNT, genCount);
        keyMem.putInt(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_BLOCK_CAPACITY, blockCapacity);
        keyMem.putInt(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_COVERING_FORMAT, coveringFormat);
        keyMem.putLong(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_PREV_ENTRY_OFFSET, prevEntryOffset);
    }
}

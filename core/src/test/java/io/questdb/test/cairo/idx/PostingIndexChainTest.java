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

package io.questdb.test.cairo.idx;

import io.questdb.cairo.idx.PostingIndexChainEntry;
import io.questdb.cairo.idx.PostingIndexChainHeader;
import io.questdb.cairo.idx.PostingIndexChainPicker;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for the v2 .pk chain layout. Uses an in-memory MemoryCARW
 * backed by a single contiguous direct allocation — does not require disk,
 * table writers, or the broader posting index integration.
 */
public class PostingIndexChainTest {

    private MemoryCARWImpl mem;

    @Before
    public void setUp() {
        mem = new MemoryCARWImpl(64 * 1024, 4, MemoryTag.NATIVE_DEFAULT);
        // Pre-extend so getLong/putLong don't trigger growth races during
        // concurrent reads.
        mem.jumpTo(64 * 1024);
        // Zero the buffer so unused fields read as 0.
        for (long o = 0; o < 64 * 1024; o += 8) {
            mem.putLong(o, 0L);
        }
    }

    @After
    public void tearDown() {
        mem = Misc.free(mem);
    }

    @Test
    public void testInitialiseEmpty() {
        PostingIndexChainHeader.initialiseEmpty(mem);

        PostingIndexChainHeader.Snapshot snapshot = new PostingIndexChainHeader.Snapshot();
        boolean ok = PostingIndexChainHeader.readUnderSeqlock(mem, snapshot);
        Assert.assertTrue(ok);
        Assert.assertEquals(PostingIndexUtils.V2_FORMAT_VERSION, snapshot.formatVersion);
        Assert.assertEquals(PostingIndexUtils.V2_NO_HEAD, snapshot.headEntryOffset);
        Assert.assertEquals(0, snapshot.entryCount);
        Assert.assertEquals(PostingIndexUtils.V2_ENTRY_REGION_BASE, snapshot.regionBase);
        Assert.assertEquals(PostingIndexUtils.V2_ENTRY_REGION_BASE, snapshot.regionLimit);
        // Default initialiseEmpty leaves genCounter at -1 so the first
        // appendNewEntry can use sealTxn=0 (matches .pv.0 naming).
        Assert.assertEquals(-1L, snapshot.generationCounter);
        Assert.assertTrue(snapshot.isEmpty());
    }

    @Test
    public void testInitialiseEmptyWithExplicitStartSealTxn() {
        PostingIndexChainHeader.initialiseEmpty(mem, /* startSealTxn */ 7L);

        PostingIndexChainHeader.Snapshot snapshot = new PostingIndexChainHeader.Snapshot();
        Assert.assertTrue(PostingIndexChainHeader.readUnderSeqlock(mem, snapshot));
        Assert.assertEquals(6L, snapshot.generationCounter);
    }

    @Test
    public void testPickerEmptyChain() {
        PostingIndexChainHeader.initialiseEmpty(mem);

        PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
        PostingIndexChainEntry.Snapshot entry = new PostingIndexChainEntry.Snapshot();
        int result = PostingIndexChainPicker.pick(mem, /* pinnedTxn */ 100L, header, entry);

        Assert.assertEquals(PostingIndexChainPicker.RESULT_EMPTY_CHAIN, result);
    }

    @Test
    public void testAbandonedEntrySkipped() {
        // Healthy entries:  (sealTxn=1, txnAtSeal=10), (sealTxn=2, txnAtSeal=20)
        // Abandoned entry:  (sealTxn=3, txnAtSeal=99) — publish landed but
        //   commit never did, so any pinned reader has _txn < 99.
        PostingIndexChainHeader.initialiseEmpty(mem);
        long off1 = appendEntry(1, 10, PostingIndexUtils.V2_NO_HEAD);
        long off2 = appendEntry(2, 20, off1);
        long off3 = appendEntry(3, 99, off2);
        publishHead(off3, /* count */ 3, /* genCounter */ 3);

        PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
        PostingIndexChainEntry.Snapshot entry = new PostingIndexChainEntry.Snapshot();

        // Reader pinned at 25 walks head (sealTxn=3, txnAtSeal=99 — skip)
        // then sealTxn=2, txnAtSeal=20 — visible.
        Assert.assertEquals(PostingIndexChainPicker.RESULT_OK,
                PostingIndexChainPicker.pick(mem, 25L, header, entry));
        Assert.assertEquals(2, entry.sealTxn);

        Assert.assertEquals(PostingIndexChainPicker.RESULT_OK,
                PostingIndexChainPicker.pick(mem, 50L, header, entry));
        Assert.assertEquals(2, entry.sealTxn);

        // Pin at 99 sees the (now-no-longer-abandoned) head; this case
        // exists only as a deterministic check of the picker, not a real
        // production state.
        Assert.assertEquals(PostingIndexChainPicker.RESULT_OK,
                PostingIndexChainPicker.pick(mem, 99L, header, entry));
        Assert.assertEquals(3, entry.sealTxn);
    }

    /**
     * NOTE: an in-memory concurrency stress test was attempted here but
     * MemoryCARWImpl is not designed for concurrent access (its
     * appendAddress and growth are not thread-safe). Real concurrency
     * testing happens at integration in Phase 2 when this code is wired
     * into PostingIndexWriter against a mmap-backed MemoryCMARW.
     */
    @org.junit.Ignore("see comment; concurrency tested at integration")
    @Test
    public void testConcurrentReaderObservesConsistentSnapshot() throws Exception {
        PostingIndexChainHeader.initialiseEmpty(mem);
        long off1 = PostingIndexUtils.V2_ENTRY_REGION_BASE;
        PostingIndexChainEntry.writeHeader(mem, off1, 1, 10, 0, 0, 0, 0, 64, 0, PostingIndexUtils.V2_NO_HEAD);
        long limit = off1 + PostingIndexChainEntry.entrySize(0);
        publishHead(off1, 1, 1, limit);

        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicInteger inconsistencies = new AtomicInteger(0);
        AtomicInteger readerOps = new AtomicInteger(0);
        CountDownLatch readyToStart = new CountDownLatch(4);
        CountDownLatch done = new CountDownLatch(4);

        Thread writer = new Thread(() -> {
            try {
                readyToStart.countDown();
                long activePage = PostingIndexUtils.PAGE_A_OFFSET;
                long prevOffset = off1;
                long nextOffset = limit;
                long sealTxn = 1;
                long entryCount = 1;
                long bufferEnd = 64 * 1024 - PostingIndexChainEntry.entrySize(0);
                while (!stop.get() && nextOffset < bufferEnd) {
                    sealTxn++;
                    long txnAtSeal = sealTxn * 10;
                    PostingIndexChainEntry.writeHeader(
                            mem, nextOffset, sealTxn, txnAtSeal, 0, 0, 0, 0, 64, 0, prevOffset
                    );
                    long thisOffset = nextOffset;
                    nextOffset += PostingIndexChainEntry.entrySize(0);
                    entryCount++;
                    activePage = PostingIndexChainHeader.publish(
                            mem, activePage, thisOffset, entryCount,
                            PostingIndexUtils.V2_ENTRY_REGION_BASE, nextOffset, sealTxn
                    );
                    prevOffset = thisOffset;
                }
            } finally {
                done.countDown();
            }
        }, "chain-writer");

        Runnable readerTask = () -> {
            PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
            PostingIndexChainEntry.Snapshot entry = new PostingIndexChainEntry.Snapshot();
            try {
                readyToStart.countDown();
                while (!stop.get()) {
                    if (!PostingIndexChainHeader.readUnderSeqlock(mem, header)) {
                        inconsistencies.incrementAndGet();
                        continue;
                    }
                    if (header.formatVersion != PostingIndexUtils.V2_FORMAT_VERSION) {
                        inconsistencies.incrementAndGet();
                        continue;
                    }
                    if (header.headEntryOffset != PostingIndexUtils.V2_NO_HEAD) {
                        if (header.headEntryOffset < header.regionBase
                                || header.headEntryOffset >= header.regionLimit) {
                            inconsistencies.incrementAndGet();
                            continue;
                        }
                        // Picker re-reads the header under its own seqlock,
                        // so its returned entry is consistent with whatever
                        // header it observed (possibly newer than ours). We
                        // only verify the entry it picked is internally
                        // consistent: offset within region, txnAtSeal <= pin,
                        // sealTxn > 0.
                        int rc = PostingIndexChainPicker.pick(mem, Long.MAX_VALUE, header, entry);
                        if (rc != PostingIndexChainPicker.RESULT_OK) {
                            inconsistencies.incrementAndGet();
                            continue;
                        }
                        if (entry.offset < header.regionBase || entry.sealTxn <= 0 || entry.txnAtSeal < 0) {
                            inconsistencies.incrementAndGet();
                            continue;
                        }
                    }
                    readerOps.incrementAndGet();
                }
            } finally {
                done.countDown();
            }
        };

        Thread r1 = new Thread(readerTask, "chain-reader-1");
        Thread r2 = new Thread(readerTask, "chain-reader-2");
        Thread r3 = new Thread(readerTask, "chain-reader-3");

        writer.start();
        r1.start();
        r2.start();
        r3.start();
        readyToStart.await();

        Thread.sleep(200);
        stop.set(true);
        done.await();

        Assert.assertTrue("readers should complete at least one cycle",
                readerOps.get() > 0);
        Assert.assertEquals("no torn reads should be observed",
                0, inconsistencies.get());
    }

    @Test
    public void testMultiEntryPicker() {
        PostingIndexChainHeader.initialiseEmpty(mem);
        long off1 = appendEntry(1, 10, PostingIndexUtils.V2_NO_HEAD);
        long off2 = appendEntry(2, 20, off1);
        long off3 = appendEntry(3, 30, off2);
        publishHead(off3, /* count */ 3, /* genCounter */ 3);

        PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
        PostingIndexChainEntry.Snapshot entry = new PostingIndexChainEntry.Snapshot();

        Assert.assertEquals(PostingIndexChainPicker.RESULT_OK,
                PostingIndexChainPicker.pick(mem, 30L, header, entry));
        Assert.assertEquals(3, entry.sealTxn);

        Assert.assertEquals(PostingIndexChainPicker.RESULT_OK,
                PostingIndexChainPicker.pick(mem, 100L, header, entry));
        Assert.assertEquals(3, entry.sealTxn);

        // Pin at 25 → picks middle (sealTxn=2 with txnAtSeal=20).
        Assert.assertEquals(PostingIndexChainPicker.RESULT_OK,
                PostingIndexChainPicker.pick(mem, 25L, header, entry));
        Assert.assertEquals(2, entry.sealTxn);
        Assert.assertEquals(20, entry.txnAtSeal);

        // Pin at exactly 20 → still picks middle.
        Assert.assertEquals(PostingIndexChainPicker.RESULT_OK,
                PostingIndexChainPicker.pick(mem, 20L, header, entry));
        Assert.assertEquals(2, entry.sealTxn);

        // Pin at 19 → picks oldest.
        Assert.assertEquals(PostingIndexChainPicker.RESULT_OK,
                PostingIndexChainPicker.pick(mem, 19L, header, entry));
        Assert.assertEquals(1, entry.sealTxn);
        Assert.assertEquals(10, entry.txnAtSeal);

        // Pin below all entries.
        Assert.assertEquals(PostingIndexChainPicker.RESULT_NO_VISIBLE_ENTRY,
                PostingIndexChainPicker.pick(mem, 5L, header, entry));
    }

    @Test
    public void testEntryRoundTripWithGenDir() {
        PostingIndexChainHeader.initialiseEmpty(mem);
        long entryOffset = PostingIndexUtils.V2_ENTRY_REGION_BASE;
        PostingIndexChainEntry.writeHeader(
                mem,
                entryOffset,
                /* sealTxn */ 7,
                /* txnAtSeal */ 42,
                /* valueMemSize */ 4096,
                /* maxValue */ 12345,
                /* keyCount */ 100,
                /* genCount */ 3,
                /* blockCapacity */ 64,
                /* coveringFormat */ 0,
                /* prevEntryOffset */ PostingIndexUtils.V2_NO_HEAD
        );
        // Synthetic gen-dir entries.
        for (int i = 0; i < 3; i++) {
            long base = PostingIndexChainEntry.resolveGenDirOffset(entryOffset, i);
            mem.putLong(base + PostingIndexUtils.GEN_DIR_OFFSET_FILE_OFFSET, 100L * (i + 1));
            mem.putLong(base + PostingIndexUtils.GEN_DIR_OFFSET_SIZE, 200L * (i + 1));
            mem.putInt(base + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT, 10 * (i + 1));
            mem.putInt(base + PostingIndexUtils.GEN_DIR_OFFSET_MIN_KEY, i);
            mem.putInt(base + PostingIndexUtils.GEN_DIR_OFFSET_MAX_KEY, 50 + i);
        }
        long entryLen = PostingIndexChainEntry.entrySize(3);
        publishHead(entryOffset, 1, 7, entryOffset + entryLen);

        PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
        PostingIndexChainEntry.Snapshot entry = new PostingIndexChainEntry.Snapshot();

        Assert.assertEquals(PostingIndexChainPicker.RESULT_OK,
                PostingIndexChainPicker.pick(mem, 100L, header, entry));
        Assert.assertEquals(7, entry.sealTxn);
        Assert.assertEquals(42, entry.txnAtSeal);
        Assert.assertEquals(4096, entry.valueMemSize);
        Assert.assertEquals(12345, entry.maxValue);
        Assert.assertEquals(100, entry.keyCount);
        Assert.assertEquals(3, entry.genCount);
        Assert.assertEquals(64, entry.blockCapacity);
        Assert.assertEquals(0, entry.coveringFormat);
        Assert.assertEquals(PostingIndexUtils.V2_NO_HEAD, entry.prevEntryOffset);
        Assert.assertEquals(entryOffset + PostingIndexUtils.V2_ENTRY_HEADER_SIZE, entry.genDirOffset);

        for (int i = 0; i < 3; i++) {
            long base = PostingIndexChainEntry.resolveGenDirOffset(entry.offset, i);
            Assert.assertEquals(100L * (i + 1), mem.getLong(base + PostingIndexUtils.GEN_DIR_OFFSET_FILE_OFFSET));
            Assert.assertEquals(200L * (i + 1), mem.getLong(base + PostingIndexUtils.GEN_DIR_OFFSET_SIZE));
            Assert.assertEquals(10 * (i + 1), mem.getInt(base + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT));
            Assert.assertEquals(i, mem.getInt(base + PostingIndexUtils.GEN_DIR_OFFSET_MIN_KEY));
            Assert.assertEquals(50 + i, mem.getInt(base + PostingIndexUtils.GEN_DIR_OFFSET_MAX_KEY));
        }
    }

    @Test
    public void testEntrySizeIsAlignedToEightBytes() {
        Assert.assertEquals(64, PostingIndexChainEntry.entrySize(0));
        Assert.assertEquals(96, PostingIndexChainEntry.entrySize(1)); // 64+28=92 → 96
        Assert.assertEquals(120, PostingIndexChainEntry.entrySize(2)); // 64+56=120
        Assert.assertEquals(152, PostingIndexChainEntry.entrySize(3)); // 64+84=148 → 152
        for (int n = 0; n < 50; n++) {
            int sz = PostingIndexChainEntry.entrySize(n);
            Assert.assertEquals("entry size for genCount=" + n + " must be 8-aligned",
                    0, sz & 7);
            Assert.assertTrue(sz >= 64 + n * 28);
            Assert.assertTrue(sz < 64 + n * 28 + 8);
        }
    }

    @Test
    public void testCorruptedPrevPointerDetected() {
        PostingIndexChainHeader.initialiseEmpty(mem);
        long entryOffset = PostingIndexUtils.V2_ENTRY_REGION_BASE;
        PostingIndexChainEntry.writeHeader(
                mem, entryOffset,
                1, 10, 0, 0, 0, 0, 64, 0,
                /* prevEntryOffset */ 999_999L
        );
        publishHead(entryOffset, 1, 1, entryOffset + PostingIndexChainEntry.entrySize(0));

        PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
        PostingIndexChainEntry.Snapshot entry = new PostingIndexChainEntry.Snapshot();

        Assert.assertEquals(PostingIndexChainPicker.RESULT_HEADER_UNREADABLE,
                PostingIndexChainPicker.pick(mem, 5L, header, entry));
    }

    @Test
    public void testCircularChainDetected() {
        PostingIndexChainHeader.initialiseEmpty(mem);
        long off1 = PostingIndexUtils.V2_ENTRY_REGION_BASE;
        long off2 = off1 + PostingIndexChainEntry.entrySize(0);

        PostingIndexChainEntry.writeHeader(mem, off1, 1, 10, 0, 0, 0, 0, 64, 0, off2);
        PostingIndexChainEntry.writeHeader(mem, off2, 2, 20, 0, 0, 0, 0, 64, 0, off1);
        publishHead(off2, /* count */ 2, 2, off2 + PostingIndexChainEntry.entrySize(0));

        PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
        PostingIndexChainEntry.Snapshot entry = new PostingIndexChainEntry.Snapshot();

        Assert.assertEquals(PostingIndexChainPicker.RESULT_HEADER_UNREADABLE,
                PostingIndexChainPicker.pick(mem, 5L, header, entry));
    }

    @Test
    public void testRepeatedPublishesPickHighestSeq() {
        // Many publishes back-to-back. After each, the picker pinned at
        // Long.MAX_VALUE must return the latest entry.
        PostingIndexChainHeader.initialiseEmpty(mem);
        long offset = PostingIndexUtils.V2_ENTRY_REGION_BASE;
        long prevOffset = PostingIndexUtils.V2_NO_HEAD;
        long activePage = PostingIndexUtils.PAGE_A_OFFSET;
        long entryCount = 0;
        PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
        PostingIndexChainEntry.Snapshot entry = new PostingIndexChainEntry.Snapshot();

        for (int i = 1; i <= 64; i++) {
            PostingIndexChainEntry.writeHeader(
                    mem, offset, /* sealTxn */ i, /* txnAtSeal */ i * 7L,
                    0, 0, 0, 0, 64, 0, prevOffset
            );
            long entryLen = PostingIndexChainEntry.entrySize(0);
            entryCount++;
            activePage = PostingIndexChainHeader.publish(
                    mem, activePage, offset, entryCount,
                    PostingIndexUtils.V2_ENTRY_REGION_BASE, offset + entryLen, i
            );

            int rc = PostingIndexChainPicker.pick(mem, Long.MAX_VALUE, header, entry);
            Assert.assertEquals(PostingIndexChainPicker.RESULT_OK, rc);
            Assert.assertEquals("after publish " + i, i, entry.sealTxn);
            Assert.assertEquals(i * 7L, entry.txnAtSeal);

            prevOffset = offset;
            offset += entryLen;
        }
    }

    @Test
    public void testSingleEntryPicker() {
        PostingIndexChainHeader.initialiseEmpty(mem);
        long offset = appendEntry(
                /* sealTxn */ 1,
                /* txnAtSeal */ 5,
                /* prevOffset */ PostingIndexUtils.V2_NO_HEAD
                /* genCount */
        );
        publishHead(offset, /* count */ 1, /* genCounter */ 1);

        PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
        PostingIndexChainEntry.Snapshot entry = new PostingIndexChainEntry.Snapshot();

        // Pin at exactly the seal txn.
        Assert.assertEquals(PostingIndexChainPicker.RESULT_OK,
                PostingIndexChainPicker.pick(mem, 5L, header, entry));
        Assert.assertEquals(1, entry.sealTxn);
        Assert.assertEquals(5, entry.txnAtSeal);

        // Pin above the seal txn.
        Assert.assertEquals(PostingIndexChainPicker.RESULT_OK,
                PostingIndexChainPicker.pick(mem, 100L, header, entry));
        Assert.assertEquals(1, entry.sealTxn);

        // Pin below the seal txn — entry not visible to this reader.
        Assert.assertEquals(PostingIndexChainPicker.RESULT_NO_VISIBLE_ENTRY,
                PostingIndexChainPicker.pick(mem, 4L, header, entry));
    }

    private long appendEntry(long sealTxn, long txnAtSeal, long prevOffset) {
        long offset;
        if (prevOffset == PostingIndexUtils.V2_NO_HEAD) {
            offset = PostingIndexUtils.V2_ENTRY_REGION_BASE;
        } else {
            offset = prevOffset + PostingIndexChainEntry.entrySize(readGenCount(prevOffset));
        }
        PostingIndexChainEntry.writeHeader(
                mem, offset, sealTxn, txnAtSeal, /* valueMemSize */ 0,
                /* maxValue */ 0, /* keyCount */ 0, 0,
                /* blockCapacity */ 64, /* coveringFormat */ 0, prevOffset
        );
        return offset;
    }

    private void publishHead(long headOffset, long entryCount, long genCounter) {
        publishHead(
                headOffset, entryCount, genCounter,
                headOffset + PostingIndexChainEntry.entrySize(readGenCount(headOffset))
        );
    }

    private void publishHead(long headOffset, long entryCount, long genCounter, long regionLimit) {
        PostingIndexChainHeader.Snapshot snap = new PostingIndexChainHeader.Snapshot();
        boolean ok = PostingIndexChainHeader.readUnderSeqlock(mem, snap);
        Assert.assertTrue("read header before publishing", ok);
        PostingIndexChainHeader.publish(
                mem,
                snap.pageOffset,
                headOffset,
                entryCount,
                PostingIndexUtils.V2_ENTRY_REGION_BASE,
                regionLimit,
                genCounter
        );
    }

    private int readGenCount(long entryOffset) {
        return mem.getInt(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_GEN_COUNT);
    }
}

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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.idx.PostingIndexChainEntry;
import io.questdb.cairo.idx.PostingIndexChainHeader;
import io.questdb.cairo.idx.PostingIndexChainPicker;
import io.questdb.cairo.idx.PostingIndexChainWriter;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link PostingIndexChainWriter}, the writer-side stateful
 * helper. Uses an in-memory {@link MemoryCARWImpl} backed by a single
 * direct allocation so the tests do not require disk or the broader
 * posting index integration.
 */
public class PostingIndexChainWriterTest {

    private MemoryCARWImpl mem;

    @Before
    public void setUp() {
        mem = new MemoryCARWImpl(64 * 1024, 4, MemoryTag.NATIVE_DEFAULT);
        mem.jumpTo(64 * 1024);
        for (long o = 0; o < 64 * 1024; o += 8) {
            mem.putLong(o, 0L);
        }
    }

    @After
    public void tearDown() {
        mem = Misc.free(mem);
    }

    @Test
    public void testAppendChainsEntriesViaPrevPointer() {
        PostingIndexChainWriter w = new PostingIndexChainWriter();
        w.initialiseEmpty(mem);

        long off1 = w.appendNewEntry(mem, 1, 10, 0, 0, 0, 1, 64, 0);
        long off2 = w.appendNewEntry(mem, 2, 20, 0, 0, 0, 1, 64, 0);
        long off3 = w.appendNewEntry(mem, 3, 30, 0, 0, 0, 1, 64, 0);

        PostingIndexChainEntry.Snapshot snap = new PostingIndexChainEntry.Snapshot();
        PostingIndexChainEntry.read(mem, off3, snap);
        Assert.assertEquals(off2, snap.prevEntryOffset);
        PostingIndexChainEntry.read(mem, off2, snap);
        Assert.assertEquals(off1, snap.prevEntryOffset);
        PostingIndexChainEntry.read(mem, off1, snap);
        Assert.assertEquals(PostingIndexUtils.V2_NO_HEAD, snap.prevEntryOffset);

        Assert.assertEquals(3, w.getEntryCount());
        Assert.assertEquals(off3, w.getHeadEntryOffset());
        Assert.assertEquals(3, w.getGenCounter());
        Assert.assertEquals(30, w.getCurrentTxnAtSeal());
    }

    @Test
    public void testAppendNewEntryRejectsNonMonotonicSealTxn() {
        PostingIndexChainWriter w = new PostingIndexChainWriter();
        w.initialiseEmpty(mem);
        w.appendNewEntry(mem, 5, 10, 0, 0, 0, 1, 64, 0);

        try {
            w.appendNewEntry(mem, 5, 11, 0, 0, 0, 1, 64, 0);
            Assert.fail("expected CairoException for non-monotonic sealTxn");
        } catch (CairoException expected) {
            Assert.assertTrue(expected.getMessage().contains("sealTxn must advance"));
        }

        try {
            w.appendNewEntry(mem, 4, 12, 0, 0, 0, 1, 64, 0);
            Assert.fail("expected CairoException for backward sealTxn");
        } catch (CairoException expected) {
            Assert.assertTrue(expected.getMessage().contains("sealTxn must advance"));
        }
    }

    @Test
    public void testAppendUpdatesPickerView() {
        PostingIndexChainWriter w = new PostingIndexChainWriter();
        w.initialiseEmpty(mem);

        w.appendNewEntry(mem, 1, 10, 4096, 999, 12, 1, 64, 0);

        PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
        PostingIndexChainEntry.Snapshot picked = new PostingIndexChainEntry.Snapshot();
        Assert.assertEquals(PostingIndexChainPicker.RESULT_OK,
                PostingIndexChainPicker.pick(mem, 100L, header, picked));
        Assert.assertEquals(1, picked.sealTxn);
        Assert.assertEquals(10, picked.txnAtSeal);
        Assert.assertEquals(4096, picked.valueMemSize);
        Assert.assertEquals(999, picked.maxValue);
        Assert.assertEquals(12, picked.keyCount);
        Assert.assertEquals(1, picked.genCount);

        // Reader pinned below this entry's txnAtSeal cannot see it.
        Assert.assertEquals(PostingIndexChainPicker.RESULT_NO_VISIBLE_ENTRY,
                PostingIndexChainPicker.pick(mem, 5L, header, picked));
    }

    @Test
    public void testCircularChainTerminatesOnRecovery() {
        // Recovery must terminate even if the prev pointer forms a cycle.
        // Synthesise off1 -> off2 -> off1 with both entries marked
        // abandoned (txnAtSeal beyond currentTableTxn). The walk should
        // throw rather than spin forever or grow the orphan list without
        // bound.
        PostingIndexChainWriter w = new PostingIndexChainWriter();
        w.initialiseEmpty(mem);

        long off1 = PostingIndexUtils.V2_ENTRY_REGION_BASE;
        long off2 = off1 + PostingIndexChainEntry.entrySize(1);

        PostingIndexChainEntry.writeHeader(mem, off1, 1, 100, 0, 0, 0, 1, 64, 0, off2);
        PostingIndexChainEntry.writeHeader(mem, off2, 2, 200, 0, 0, 0, 1, 64, 0, off1);
        long limit = off2 + PostingIndexChainEntry.entrySize(1);
        PostingIndexChainHeader.publish(
                mem, PostingIndexUtils.PAGE_A_OFFSET, off2, /* entryCount */ 2,
                PostingIndexUtils.V2_ENTRY_REGION_BASE, limit, 2L
        );
        w.openExisting(mem);
        Assert.assertEquals(off2, w.getHeadEntryOffset());

        LongList orphans = new LongList();
        try {
            w.recoveryDropAbandoned(mem, /* currentTableTxn */ 50L, orphans);
            Assert.fail("expected CairoException for corrupted chain");
        } catch (CairoException expected) {
            String msg = expected.getMessage();
            Assert.assertTrue(
                    "message should describe the corruption: " + msg,
                    msg.contains("exceeded entryCount") || msg.contains("offset out of range")
            );
        }
    }

    @Test
    public void testExtendHeadGrowsGenCountAndLength() {
        PostingIndexChainWriter w = new PostingIndexChainWriter();
        w.initialiseEmpty(mem);

        long off = w.appendNewEntry(mem, 1, 10, 100, 50, 5, 1, 64, 0);
        long lenBefore = mem.getLong(off + PostingIndexUtils.V2_ENTRY_OFFSET_LEN);

        // Caller writes the new gen-dir entry's bytes at the right offset.
        long newGenDirOffset = PostingIndexChainEntry.resolveGenDirOffset(off, 1);
        mem.putLong(newGenDirOffset + PostingIndexUtils.GEN_DIR_OFFSET_FILE_OFFSET, 7777L);
        mem.putLong(newGenDirOffset + PostingIndexUtils.GEN_DIR_OFFSET_SIZE, 8888L);
        mem.putInt(newGenDirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT, -3); // sparse marker
        mem.putInt(newGenDirOffset + PostingIndexUtils.GEN_DIR_OFFSET_MIN_KEY, 1);
        mem.putInt(newGenDirOffset + PostingIndexUtils.GEN_DIR_OFFSET_MAX_KEY, 9);

        w.extendHead(mem, /* newGenCount */ 2, /* newKeyCount */ 7, /* newValueMemSize */ 200, /* newMaxValue */ 75);

        // Verify the head entry's mutable fields advanced.
        Assert.assertEquals(2, mem.getInt(off + PostingIndexUtils.V2_ENTRY_OFFSET_GEN_COUNT));
        Assert.assertEquals(7, mem.getInt(off + PostingIndexUtils.V2_ENTRY_OFFSET_KEY_COUNT));
        Assert.assertEquals(200, mem.getLong(off + PostingIndexUtils.V2_ENTRY_OFFSET_VALUE_MEM_SIZE));
        Assert.assertEquals(75, mem.getLong(off + PostingIndexUtils.V2_ENTRY_OFFSET_MAX_VALUE));
        long lenAfter = mem.getLong(off + PostingIndexUtils.V2_ENTRY_OFFSET_LEN);
        Assert.assertTrue("LEN must grow by at least one gen-dir entry", lenAfter > lenBefore);
        Assert.assertEquals(PostingIndexChainEntry.entrySize(2), lenAfter);
        // Region limit advances accordingly.
        Assert.assertEquals(off + lenAfter, w.getRegionLimit());

        // The picker must observe the new state.
        PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
        PostingIndexChainEntry.Snapshot picked = new PostingIndexChainEntry.Snapshot();
        Assert.assertEquals(PostingIndexChainPicker.RESULT_OK,
                PostingIndexChainPicker.pick(mem, 50L, header, picked));
        Assert.assertEquals(2, picked.genCount);
        Assert.assertEquals(75, picked.maxValue);
        Assert.assertEquals(200, picked.valueMemSize);
        // The previously-stored gen-dir entry stays intact.
        long gen1Offset = PostingIndexChainEntry.resolveGenDirOffset(picked.offset, 1);
        Assert.assertEquals(7777L, mem.getLong(gen1Offset + PostingIndexUtils.GEN_DIR_OFFSET_FILE_OFFSET));
        Assert.assertEquals(8888L, mem.getLong(gen1Offset + PostingIndexUtils.GEN_DIR_OFFSET_SIZE));
        Assert.assertEquals(-3, mem.getInt(gen1Offset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT));
    }

    @Test
    public void testExtendHeadOnEmptyChainThrows() {
        PostingIndexChainWriter w = new PostingIndexChainWriter();
        w.initialiseEmpty(mem);
        try {
            w.extendHead(mem, 1, 1, 100, 50);
            Assert.fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
            Assert.assertTrue(expected.getMessage().contains("empty"));
        }
    }

    @Test
    public void testInitialiseEmptyResetsState() {
        PostingIndexChainWriter w = new PostingIndexChainWriter();
        w.initialiseEmpty(mem);

        Assert.assertEquals(PostingIndexUtils.PAGE_A_OFFSET, w.getActivePageOffset());
        Assert.assertEquals(PostingIndexUtils.V2_NO_HEAD, w.getHeadEntryOffset());
        Assert.assertEquals(PostingIndexUtils.V2_ENTRY_REGION_BASE, w.getRegionBase());
        Assert.assertEquals(PostingIndexUtils.V2_ENTRY_REGION_BASE, w.getRegionLimit());
        Assert.assertEquals(0, w.getEntryCount());
        // Default initialiseEmpty leaves genCounter at -1 so peekNextSealTxn=0,
        // matching the historical .pv.0 filename convention.
        Assert.assertEquals(-1L, w.getGenCounter());
        Assert.assertEquals(-1L, w.getCurrentTxnAtSeal());
        Assert.assertFalse(w.hasHead());
        Assert.assertEquals(0L, w.peekNextSealTxn());

        // Verify the on-disk header is consistent with the in-memory mirror.
        PostingIndexChainHeader.Snapshot snap = new PostingIndexChainHeader.Snapshot();
        Assert.assertTrue(PostingIndexChainHeader.readUnderSeqlock(mem, snap));
        Assert.assertTrue(snap.isEmpty());
        Assert.assertEquals(PostingIndexUtils.V2_FORMAT_VERSION, snap.formatVersion);
    }

    @Test
    public void testLoadHeadEntry() {
        PostingIndexChainWriter w = new PostingIndexChainWriter();
        w.initialiseEmpty(mem);
        w.appendNewEntry(mem, 1, 10, 4096, 99, 7, 1, 64, 0);

        PostingIndexChainEntry.Snapshot snap = new PostingIndexChainEntry.Snapshot();
        w.loadHeadEntry(mem, snap);
        Assert.assertEquals(1, snap.sealTxn);
        Assert.assertEquals(10, snap.txnAtSeal);
        Assert.assertEquals(4096, snap.valueMemSize);
        Assert.assertEquals(99, snap.maxValue);
        Assert.assertEquals(7, snap.keyCount);
        Assert.assertEquals(1, snap.genCount);
    }

    @Test
    public void testLoadHeadEntryOnEmptyChainThrows() {
        PostingIndexChainWriter w = new PostingIndexChainWriter();
        w.initialiseEmpty(mem);
        PostingIndexChainEntry.Snapshot snap = new PostingIndexChainEntry.Snapshot();
        try {
            w.loadHeadEntry(mem, snap);
            Assert.fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
            Assert.assertTrue(expected.getMessage().contains("empty"));
        }
    }

    @Test
    public void testOpenExistingPopulatesStateFromHead() {
        PostingIndexChainWriter writer = new PostingIndexChainWriter();
        writer.initialiseEmpty(mem);
        writer.appendNewEntry(mem, 1, 10, 0, 0, 0, 1, 64, 0);
        writer.appendNewEntry(mem, 2, 20, 0, 0, 0, 1, 64, 0);

        // Simulate writer reopen: a fresh instance reads from the same memory.
        PostingIndexChainWriter reopened = new PostingIndexChainWriter();
        reopened.openExisting(mem);
        Assert.assertEquals(writer.getHeadEntryOffset(), reopened.getHeadEntryOffset());
        Assert.assertEquals(writer.getEntryCount(), reopened.getEntryCount());
        Assert.assertEquals(writer.getGenCounter(), reopened.getGenCounter());
        Assert.assertEquals(writer.getRegionLimit(), reopened.getRegionLimit());
        Assert.assertEquals(writer.getCurrentTxnAtSeal(), reopened.getCurrentTxnAtSeal());
    }

    @Test
    public void testOpenExistingRejectsBadFormatVersion() {
        PostingIndexChainHeader.initialiseEmpty(mem);
        // Corrupt the format version on page A.
        mem.putLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.V2_HEADER_OFFSET_FORMAT_VERSION,
                /* bogus */ 99L);

        PostingIndexChainWriter w = new PostingIndexChainWriter();
        try {
            w.openExisting(mem);
            Assert.fail("expected CairoException for unsupported format version");
        } catch (CairoException expected) {
            Assert.assertTrue(expected.getMessage().contains("Unsupported Posting index version"));
        }
    }

    @Test
    public void testPeekRegionLimitRejectsBadFormatVersion() {
        PostingIndexChainHeader.initialiseEmpty(mem);
        // Corrupt the active page's format version. The seqlock stays intact, so
        // readUnderSeqlock returns the bogus value and peekRegionLimit rejects it.
        mem.putLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.V2_HEADER_OFFSET_FORMAT_VERSION, 99L);

        PostingIndexChainWriter w = new PostingIndexChainWriter();
        try {
            w.peekRegionLimit(mem);
            Assert.fail("expected CairoException for unsupported format version");
        } catch (CairoException expected) {
            Assert.assertTrue(expected.getMessage().contains("Unsupported Posting index version"));
        }
    }

    @Test
    public void testPeekRegionLimitRejectsInvertedRegion() {
        PostingIndexChainHeader.initialiseEmpty(mem);
        // regionBase past the reserved window but regionLimit below it: an inverted
        // (regionLimit < regionBase) region descriptor. headEntryOffset stays V2_NO_HEAD
        // so only the region check can trip.
        mem.putLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.V2_HEADER_OFFSET_REGION_BASE, 16384L);
        mem.putLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.V2_HEADER_OFFSET_REGION_LIMIT, 12288L);

        PostingIndexChainWriter w = new PostingIndexChainWriter();
        try {
            w.peekRegionLimit(mem);
            Assert.fail("expected CairoException for inverted region");
        } catch (CairoException expected) {
            Assert.assertTrue(expected.getMessage().contains("posting index header has invalid region"));
        }
    }

    @Test
    public void testPeekRegionLimitRejectsOutOfRegionHead() {
        PostingIndexChainHeader.initialiseEmpty(mem);
        // Valid region [8192, 16384); a head pointer at regionLimit is out of the
        // half-open range and must be rejected (the SIGSEGV guard). Pins the >= bound.
        mem.putLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.V2_HEADER_OFFSET_REGION_LIMIT, 16384L);
        mem.putLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.V2_HEADER_OFFSET_HEAD_ENTRY_OFFSET, 16384L);

        PostingIndexChainWriter w = new PostingIndexChainWriter();
        try {
            w.peekRegionLimit(mem);
            Assert.fail("expected CairoException for out-of-region head");
        } catch (CairoException expected) {
            Assert.assertTrue(expected.getMessage().contains("posting index header has out-of-region head"));
        }
    }

    @Test
    public void testPeekRegionLimitRejectsRegionBaseBelowReserved() {
        PostingIndexChainHeader.initialiseEmpty(mem);
        // regionBase inside the reserved header window is inconsistent: the entry region
        // must start at or past KEY_FILE_RESERVED.
        mem.putLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.V2_HEADER_OFFSET_REGION_BASE,
                PostingIndexUtils.KEY_FILE_RESERVED - 8L);

        PostingIndexChainWriter w = new PostingIndexChainWriter();
        try {
            w.peekRegionLimit(mem);
            Assert.fail("expected CairoException for region base below reserved window");
        } catch (CairoException expected) {
            Assert.assertTrue(expected.getMessage().contains("posting index header has invalid region"));
        }
    }

    @Test
    public void testPeekRegionLimitRejectsUnreadableHeader() {
        PostingIndexChainHeader.initialiseEmpty(mem);
        // Break the active page's seqlock (start != end) and leave the inactive page at
        // its zero sequence, so no page ever reads stable and readUnderSeqlock gives up.
        mem.putLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.V2_HEADER_OFFSET_SEQUENCE_START, 3L);

        PostingIndexChainWriter w = new PostingIndexChainWriter();
        try {
            w.peekRegionLimit(mem);
            Assert.fail("expected CairoException for unreadable header");
        } catch (CairoException expected) {
            Assert.assertTrue(expected.getMessage().contains("posting index header unreadable"));
        }
    }

    @Test
    public void testPeekRegionLimitReturnsRegionLimitForEmptyChain() {
        // The legitimate empty-region shape (regionBase == regionLimit == entry region base,
        // head == V2_NO_HEAD) stays valid and returns the region limit.
        PostingIndexChainHeader.initialiseEmpty(mem);
        PostingIndexChainWriter w = new PostingIndexChainWriter();
        Assert.assertEquals(PostingIndexUtils.V2_ENTRY_REGION_BASE, w.peekRegionLimit(mem));
    }

    @Test
    public void testPeekRegionLimitReturnsRegionLimitForNonEmptyChain() {
        // A published non-empty chain: peekRegionLimit must return the same high-water the
        // writer recorded, so a caller can size the mapping to cover the head entry.
        PostingIndexChainWriter writer = new PostingIndexChainWriter();
        writer.initialiseEmpty(mem);
        writer.appendNewEntry(mem, 1, 10, 0, 0, 0, 1, 64, 0);
        writer.appendNewEntry(mem, 2, 20, 0, 0, 0, 1, 64, 0);

        PostingIndexChainWriter reader = new PostingIndexChainWriter();
        Assert.assertEquals(writer.getRegionLimit(), reader.peekRegionLimit(mem));
        Assert.assertTrue("a non-empty chain's region must extend past the header window",
                reader.peekRegionLimit(mem) > PostingIndexUtils.KEY_FILE_RESERVED);
    }

    @Test
    public void testRecoveryDropAbandonedKeepsOnlyCommittedEntries() {
        PostingIndexChainWriter w = new PostingIndexChainWriter();
        w.initialiseEmpty(mem);

        // Healthy entries — txnAtSeal at 10, 20, 30.
        w.appendNewEntry(mem, 1, 10, 0, 0, 0, 1, 64, 0);
        w.appendNewEntry(mem, 2, 20, 0, 0, 0, 1, 64, 0);
        w.appendNewEntry(mem, 3, 30, 0, 0, 0, 1, 64, 0);
        // Two abandoned entries — txnAtSeal beyond what _txn ever committed.
        long expectedRegionLimitAfter = w.appendNewEntry(mem, 4, 99, 0, 0, 0, 1, 64, 0);
        w.appendNewEntry(mem, 5, 100, 0, 0, 0, 1, 64, 0);

        // entry #4's start

        LongList orphans = new LongList();
        int dropped = w.recoveryDropAbandoned(mem, /* currentTableTxn */ 30L, orphans);

        Assert.assertEquals(2, dropped);
        Assert.assertEquals(2, orphans.size());
        // Orphans are recorded head-to-tail (newest first).
        Assert.assertEquals(5, orphans.get(0));
        Assert.assertEquals(4, orphans.get(1));
        Assert.assertEquals(3, w.getEntryCount());
        Assert.assertEquals(30, w.getCurrentTxnAtSeal());
        Assert.assertEquals(expectedRegionLimitAfter, w.getRegionLimit());

        // The chain is now visible only up to txnAtSeal=30.
        PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
        PostingIndexChainEntry.Snapshot entry = new PostingIndexChainEntry.Snapshot();
        Assert.assertEquals(PostingIndexChainPicker.RESULT_OK,
                PostingIndexChainPicker.pick(mem, Long.MAX_VALUE, header, entry));
        Assert.assertEquals(3, entry.sealTxn);
        Assert.assertEquals(30, entry.txnAtSeal);

        // genCounter does not regress: it remembers the highest sealTxn ever
        // assigned (5), even though the entries that used 4 and 5 were
        // dropped. This guarantees future seals never reuse those values.
        Assert.assertEquals(5, w.getGenCounter());
        Assert.assertEquals(6, w.peekNextSealTxn());
    }

    @Test
    public void testRecoveryDropAbandonedNoOpOnEmptyChain() {
        PostingIndexChainWriter w = new PostingIndexChainWriter();
        w.initialiseEmpty(mem);
        LongList orphans = new LongList();
        Assert.assertEquals(0, w.recoveryDropAbandoned(mem, 100L, orphans));
        Assert.assertEquals(0, orphans.size());
    }

    @Test
    public void testRecoveryDropAbandonedNoOpWhenAllEntriesCommitted() {
        PostingIndexChainWriter w = new PostingIndexChainWriter();
        w.initialiseEmpty(mem);
        w.appendNewEntry(mem, 1, 10, 0, 0, 0, 1, 64, 0);
        w.appendNewEntry(mem, 2, 20, 0, 0, 0, 1, 64, 0);
        long headBefore = w.getHeadEntryOffset();
        long limitBefore = w.getRegionLimit();
        long counterBefore = w.getGenCounter();

        LongList orphans = new LongList();
        Assert.assertEquals(0, w.recoveryDropAbandoned(mem, 100L, orphans));
        Assert.assertEquals(0, orphans.size());
        Assert.assertEquals(headBefore, w.getHeadEntryOffset());
        Assert.assertEquals(limitBefore, w.getRegionLimit());
        Assert.assertEquals(counterBefore, w.getGenCounter());
    }

    @Test
    public void testRecoveryHeadTrimRelocatesCoverFooter() {
        PostingIndexChainWriter w = new PostingIndexChainWriter();
        w.initialiseEmpty(mem);

        // Build a single covered entry with genCount=3 and coverCount=2.
        // appendNewEntry only stamps slot[0].TXN_AT_SEAL; the remaining
        // slots are written by hand so we can simulate an extendHead-style
        // fast-path tail with in-flight txnAtSeals.
        LongList coverEnds = new LongList();
        coverEnds.add(7777L);
        coverEnds.add(8888L);
        long entryOffset = w.appendNewEntry(
                mem,
                /* sealTxn */ 1, /* txnAtSeal */ 10,
                /* valueMemSize */ 0, /* maxValue */ -1,
                /* keyCount */ 0, /* genCount */ 3,
                /* blockCapacity */ 64, /* coveringFormat */ 0,
                coverEnds
        );
        // Stamp the three slots so trimInFlightTailGens has data to work
        // with: slot[0] visible, slot[1..2] in-flight.
        long slot0 = entryOffset + PostingIndexUtils.V2_ENTRY_HEADER_SIZE;
        long slot1 = slot0 + PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
        long slot2 = slot1 + PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
        // slot[0] already has TXN_AT_SEAL=10 via appendNewEntry; assert
        // the precondition rather than re-write it.
        Assert.assertEquals(10L, mem.getLong(slot0 + PostingIndexUtils.GEN_DIR_OFFSET_TXN_AT_SEAL));
        mem.putLong(slot1 + PostingIndexUtils.GEN_DIR_OFFSET_TXN_AT_SEAL, 11L);
        mem.putLong(slot2 + PostingIndexUtils.GEN_DIR_OFFSET_TXN_AT_SEAL, 12L);
        // Per-slot MAX_VALUE: the trim restores entry.MAX_VALUE from the
        // last surviving slot's value.
        mem.putLong(slot0 + PostingIndexUtils.GEN_DIR_OFFSET_MAX_VALUE, 100L);
        mem.putLong(slot1 + PostingIndexUtils.GEN_DIR_OFFSET_MAX_VALUE, 200L);
        mem.putLong(slot2 + PostingIndexUtils.GEN_DIR_OFFSET_MAX_VALUE, 300L);
        // Per-slot file extent: the trim caps entry.VALUE_MEM_SIZE at
        // slot[keep-1].FILE_OFFSET + slot[keep-1].SIZE.
        mem.putLong(slot0 + PostingIndexUtils.GEN_DIR_OFFSET_FILE_OFFSET, 0L);
        mem.putLong(slot0 + PostingIndexUtils.GEN_DIR_OFFSET_SIZE, 50L);
        mem.putLong(slot1 + PostingIndexUtils.GEN_DIR_OFFSET_FILE_OFFSET, 50L);
        mem.putLong(slot1 + PostingIndexUtils.GEN_DIR_OFFSET_SIZE, 50L);
        mem.putLong(slot2 + PostingIndexUtils.GEN_DIR_OFFSET_FILE_OFFSET, 100L);
        mem.putLong(slot2 + PostingIndexUtils.GEN_DIR_OFFSET_SIZE, 50L);

        long preRecoveryRegionLimit = w.getRegionLimit();

        LongList orphans = new LongList();
        int dropped = w.recoveryDropAbandoned(mem, /* currentTableTxn */ 10L, orphans);

        Assert.assertEquals("entry-level visible: nothing dropped", 0, dropped);
        Assert.assertEquals(0, orphans.size());
        Assert.assertTrue("head must be flagged as trimmed", w.isHeadTrimmedOnLastRecovery());
        Assert.assertEquals(1L, w.getEntryCount());

        // Copy-on-write: the source entry's bytes are untouched; a new
        // trimmed entry sits at the post-recovery region tail.
        Assert.assertEquals(3, mem.getInt(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_GEN_COUNT));

        long newHead = w.getHeadEntryOffset();
        Assert.assertEquals(preRecoveryRegionLimit, newHead);
        Assert.assertNotEquals(entryOffset, newHead);

        long expectedLen = PostingIndexChainEntry.entrySize(1, 2);
        Assert.assertEquals(1, mem.getInt(newHead + PostingIndexUtils.V2_ENTRY_OFFSET_GEN_COUNT));
        Assert.assertEquals(expectedLen, mem.getLong(newHead + PostingIndexUtils.V2_ENTRY_OFFSET_LEN));
        Assert.assertEquals(newHead + expectedLen, w.getRegionLimit());
        Assert.assertEquals(100L, mem.getLong(newHead + PostingIndexUtils.V2_ENTRY_OFFSET_MAX_VALUE));
        Assert.assertEquals(50L, mem.getLong(newHead + PostingIndexUtils.V2_ENTRY_OFFSET_VALUE_MEM_SIZE));

        long newFooterOffset = newHead + PostingIndexUtils.V2_ENTRY_HEADER_SIZE
                + (long) PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
        Assert.assertEquals(7777L, mem.getLong(newFooterOffset));
        Assert.assertEquals(8888L, mem.getLong(newFooterOffset + PostingIndexUtils.COVER_END_OFFSET_ENTRY_SIZE));

        PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
        PostingIndexChainEntry.Snapshot picked = new PostingIndexChainEntry.Snapshot();
        Assert.assertEquals(
                PostingIndexChainPicker.RESULT_OK,
                PostingIndexChainPicker.pick(mem, Long.MAX_VALUE, /* coverCount */ 2, header, picked)
        );
        Assert.assertEquals(newHead, picked.offset);
        Assert.assertEquals(1, picked.genCount);
        Assert.assertEquals(2, picked.coverFileEndOffsets.size());
        Assert.assertEquals(7777L, picked.coverFileEndOffsets.getQuick(0));
        Assert.assertEquals(8888L, picked.coverFileEndOffsets.getQuick(1));
    }

    @Test
    public void testRecoveryHeadTrimSingleTailGenNoCover() {
        PostingIndexChainWriter w = new PostingIndexChainWriter();
        w.initialiseEmpty(mem);

        long entryOffset = w.appendNewEntry(
                mem,
                /* sealTxn */ 1, /* txnAtSeal */ 10,
                /* valueMemSize */ 0, /* maxValue */ -1,
                /* keyCount */ 0, /* genCount */ 2,
                /* blockCapacity */ 64, /* coveringFormat */ 0
        );
        long slot0 = entryOffset + PostingIndexUtils.V2_ENTRY_HEADER_SIZE;
        long slot1 = slot0 + PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
        mem.putLong(slot1 + PostingIndexUtils.GEN_DIR_OFFSET_TXN_AT_SEAL, 11L);
        mem.putLong(slot0 + PostingIndexUtils.GEN_DIR_OFFSET_FILE_OFFSET, 0L);
        mem.putLong(slot0 + PostingIndexUtils.GEN_DIR_OFFSET_SIZE, 64L);
        mem.putLong(slot0 + PostingIndexUtils.GEN_DIR_OFFSET_MAX_VALUE, 42L);
        mem.putInt(slot0 + PostingIndexUtils.GEN_DIR_OFFSET_MAX_KEY, 4);

        long preRecoveryRegionLimit = w.getRegionLimit();
        int dropped = w.recoveryDropAbandoned(mem, /* currentTableTxn */ 10L, new LongList());

        Assert.assertEquals(0, dropped);
        Assert.assertTrue(w.isHeadTrimmedOnLastRecovery());
        // Source untouched, new head at region tail.
        Assert.assertEquals(2, mem.getInt(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_GEN_COUNT));

        long newHead = w.getHeadEntryOffset();
        Assert.assertEquals(preRecoveryRegionLimit, newHead);
        Assert.assertEquals(1, mem.getInt(newHead + PostingIndexUtils.V2_ENTRY_OFFSET_GEN_COUNT));
        Assert.assertEquals(
                PostingIndexChainEntry.entrySize(1, 0),
                mem.getLong(newHead + PostingIndexUtils.V2_ENTRY_OFFSET_LEN)
        );
        Assert.assertEquals(42L, mem.getLong(newHead + PostingIndexUtils.V2_ENTRY_OFFSET_MAX_VALUE));
        Assert.assertEquals(64L, mem.getLong(newHead + PostingIndexUtils.V2_ENTRY_OFFSET_VALUE_MEM_SIZE));
        Assert.assertEquals(5, mem.getInt(newHead + PostingIndexUtils.V2_ENTRY_OFFSET_KEY_COUNT));
    }

    @Test
    public void testRecoveryDropsPartialSealEntryAtGetTxnPlusOne() {
        // Models the SEAL branch in TableWriter.sealPostingIndexForPartition:
        // a chain entry is published with txnAtSeal = txWriter.getTxn() + 1,
        // then the surrounding O3 / parquet-rewrite path fails before
        // txWriter.commit lands. The next writer-open runs recovery with
        // currentTableTxn still at the pre-fail committed _txn; the
        // (T+1 > T) predicate must drop the partial SEAL entry.
        PostingIndexChainWriter w = new PostingIndexChainWriter();
        w.initialiseEmpty(mem);

        final long committedTxn = 42L;
        long survivor = w.appendNewEntry(mem, 1, committedTxn, 0, 0, 0, 1, 64, 0);
        w.appendNewEntry(mem, 2, committedTxn + 1L, 0, 0, 0, 1, 64, 0);

        LongList orphans = new LongList();
        int dropped = w.recoveryDropAbandoned(mem, committedTxn, orphans);

        Assert.assertEquals(1, dropped);
        Assert.assertEquals(1, orphans.size());
        Assert.assertEquals(2L, orphans.getQuick(0));
        Assert.assertEquals(1L, w.getEntryCount());
        Assert.assertEquals(survivor, w.getHeadEntryOffset());
        Assert.assertFalse(w.isHeadTrimmedOnLastRecovery());

        PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
        PostingIndexChainEntry.Snapshot picked = new PostingIndexChainEntry.Snapshot();
        Assert.assertEquals(PostingIndexChainPicker.RESULT_OK,
                PostingIndexChainPicker.pick(mem, Long.MAX_VALUE, header, picked));
        Assert.assertEquals(1L, picked.sealTxn);
        Assert.assertEquals(committedTxn, picked.txnAtSeal);
    }

    @Test
    public void testRecoveryHeadTrimDropsAllTailGensKeepingSlot0Only() {
        PostingIndexChainWriter w = new PostingIndexChainWriter();
        w.initialiseEmpty(mem);

        long entryOffset = w.appendNewEntry(
                mem,
                /* sealTxn */ 1, /* txnAtSeal */ 5,
                /* valueMemSize */ 0, /* maxValue */ -1,
                /* keyCount */ 0, /* genCount */ 5,
                /* blockCapacity */ 64, /* coveringFormat */ 0
        );
        long slot0 = entryOffset + PostingIndexUtils.V2_ENTRY_HEADER_SIZE;
        mem.putLong(slot0 + PostingIndexUtils.GEN_DIR_OFFSET_FILE_OFFSET, 0L);
        mem.putLong(slot0 + PostingIndexUtils.GEN_DIR_OFFSET_SIZE, 80L);
        mem.putLong(slot0 + PostingIndexUtils.GEN_DIR_OFFSET_MAX_VALUE, 7L);
        mem.putInt(slot0 + PostingIndexUtils.GEN_DIR_OFFSET_MAX_KEY, 2);
        for (int g = 1; g < 5; g++) {
            long slot = entryOffset + PostingIndexUtils.V2_ENTRY_HEADER_SIZE
                    + (long) g * PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
            mem.putLong(slot + PostingIndexUtils.GEN_DIR_OFFSET_TXN_AT_SEAL, 5L + g);
        }

        long preRecoveryRegionLimit = w.getRegionLimit();
        int dropped = w.recoveryDropAbandoned(mem, /* currentTableTxn */ 5L, new LongList());

        Assert.assertEquals(0, dropped);
        Assert.assertTrue(w.isHeadTrimmedOnLastRecovery());
        Assert.assertEquals(5, mem.getInt(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_GEN_COUNT));

        long newHead = w.getHeadEntryOffset();
        Assert.assertEquals(preRecoveryRegionLimit, newHead);
        Assert.assertEquals(1, mem.getInt(newHead + PostingIndexUtils.V2_ENTRY_OFFSET_GEN_COUNT));
        Assert.assertEquals(
                PostingIndexChainEntry.entrySize(1, 0),
                mem.getLong(newHead + PostingIndexUtils.V2_ENTRY_OFFSET_LEN)
        );
        Assert.assertEquals(7L, mem.getLong(newHead + PostingIndexUtils.V2_ENTRY_OFFSET_MAX_VALUE));
        Assert.assertEquals(80L, mem.getLong(newHead + PostingIndexUtils.V2_ENTRY_OFFSET_VALUE_MEM_SIZE));
        Assert.assertEquals(3, mem.getInt(newHead + PostingIndexUtils.V2_ENTRY_OFFSET_KEY_COUNT));
        Assert.assertEquals(newHead + PostingIndexChainEntry.entrySize(1, 0), w.getRegionLimit());
    }

    @Test
    public void testRecoveryHeadTrimKeepsMultipleGensWithCover() {
        PostingIndexChainWriter w = new PostingIndexChainWriter();
        w.initialiseEmpty(mem);

        LongList coverEnds = new LongList();
        coverEnds.add(1111L);
        coverEnds.add(2222L);
        long entryOffset = w.appendNewEntry(
                mem,
                /* sealTxn */ 1, /* txnAtSeal */ 10,
                /* valueMemSize */ 0, /* maxValue */ -1,
                /* keyCount */ 0, /* genCount */ 5,
                /* blockCapacity */ 64, /* coveringFormat */ 0,
                coverEnds
        );
        for (int g = 0; g < 5; g++) {
            long slot = entryOffset + PostingIndexUtils.V2_ENTRY_HEADER_SIZE
                    + (long) g * PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
            mem.putLong(slot + PostingIndexUtils.GEN_DIR_OFFSET_FILE_OFFSET, (long) g * 100);
            mem.putLong(slot + PostingIndexUtils.GEN_DIR_OFFSET_SIZE, 100L);
            mem.putLong(slot + PostingIndexUtils.GEN_DIR_OFFSET_MAX_VALUE, (long) (g + 1) * 50);
            mem.putInt(slot + PostingIndexUtils.GEN_DIR_OFFSET_MAX_KEY, g);
        }
        // slot[0] keeps TXN_AT_SEAL=10 from appendNewEntry; the rest mix
        // visible (slot[1..2]) and in-flight (slot[3..4]).
        long slot1 = entryOffset + PostingIndexUtils.V2_ENTRY_HEADER_SIZE
                + (long) PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
        long slot2 = slot1 + PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
        long slot3 = slot2 + PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
        long slot4 = slot3 + PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
        mem.putLong(slot1 + PostingIndexUtils.GEN_DIR_OFFSET_TXN_AT_SEAL, 10L);
        mem.putLong(slot2 + PostingIndexUtils.GEN_DIR_OFFSET_TXN_AT_SEAL, 10L);
        mem.putLong(slot3 + PostingIndexUtils.GEN_DIR_OFFSET_TXN_AT_SEAL, 11L);
        mem.putLong(slot4 + PostingIndexUtils.GEN_DIR_OFFSET_TXN_AT_SEAL, 12L);

        long preRecoveryRegionLimit = w.getRegionLimit();
        int dropped = w.recoveryDropAbandoned(mem, /* currentTableTxn */ 10L, new LongList());

        Assert.assertEquals(0, dropped);
        Assert.assertTrue(w.isHeadTrimmedOnLastRecovery());
        Assert.assertEquals(5, mem.getInt(entryOffset + PostingIndexUtils.V2_ENTRY_OFFSET_GEN_COUNT));

        long newHead = w.getHeadEntryOffset();
        Assert.assertEquals(preRecoveryRegionLimit, newHead);
        Assert.assertEquals(3, mem.getInt(newHead + PostingIndexUtils.V2_ENTRY_OFFSET_GEN_COUNT));
        Assert.assertEquals(
                PostingIndexChainEntry.entrySize(3, 2),
                mem.getLong(newHead + PostingIndexUtils.V2_ENTRY_OFFSET_LEN)
        );
        Assert.assertEquals(150L, mem.getLong(newHead + PostingIndexUtils.V2_ENTRY_OFFSET_MAX_VALUE));
        Assert.assertEquals(300L, mem.getLong(newHead + PostingIndexUtils.V2_ENTRY_OFFSET_VALUE_MEM_SIZE));
        Assert.assertEquals(3, mem.getInt(newHead + PostingIndexUtils.V2_ENTRY_OFFSET_KEY_COUNT));

        long newFooterOffset = newHead + PostingIndexUtils.V2_ENTRY_HEADER_SIZE
                + 3L * PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
        Assert.assertEquals(1111L, mem.getLong(newFooterOffset));
        Assert.assertEquals(2222L, mem.getLong(newFooterOffset + PostingIndexUtils.COVER_END_OFFSET_ENTRY_SIZE));

        PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
        PostingIndexChainEntry.Snapshot picked = new PostingIndexChainEntry.Snapshot();
        Assert.assertEquals(PostingIndexChainPicker.RESULT_OK,
                PostingIndexChainPicker.pick(mem, Long.MAX_VALUE, /* coverCount */ 2, header, picked));
        Assert.assertEquals(newHead, picked.offset);
        Assert.assertEquals(3, picked.genCount);
        Assert.assertEquals(2, picked.coverFileEndOffsets.size());
        Assert.assertEquals(1111L, picked.coverFileEndOffsets.getQuick(0));
        Assert.assertEquals(2222L, picked.coverFileEndOffsets.getQuick(1));
    }

    @Test
    public void testRecoveryHeadTrimLeavesSourceEntryBytesIntact() {
        PostingIndexChainWriter w = new PostingIndexChainWriter();
        w.initialiseEmpty(mem);

        LongList coverEnds = new LongList();
        coverEnds.add(5555L);
        long sourceOffset = w.appendNewEntry(
                mem, /* sealTxn */ 1, /* txnAtSeal */ 10,
                /* valueMemSize */ 0, /* maxValue */ -1,
                /* keyCount */ 0, /* genCount */ 3,
                /* blockCapacity */ 64, /* coveringFormat */ 0,
                coverEnds
        );
        long slot1 = sourceOffset + PostingIndexUtils.V2_ENTRY_HEADER_SIZE
                + (long) PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
        long slot2 = slot1 + PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
        mem.putLong(slot1 + PostingIndexUtils.GEN_DIR_OFFSET_TXN_AT_SEAL, 11L);
        mem.putLong(slot2 + PostingIndexUtils.GEN_DIR_OFFSET_TXN_AT_SEAL, 12L);

        long sourceEntryLen = PostingIndexChainEntry.entrySize(3, 1);
        long[] sourceBytes = new long[(int) (sourceEntryLen / Long.BYTES)];
        for (int i = 0; i < sourceBytes.length; i++) {
            sourceBytes[i] = mem.getLong(sourceOffset + (long) i * Long.BYTES);
        }

        int dropped = w.recoveryDropAbandoned(mem, /* currentTableTxn */ 10L, new LongList());

        Assert.assertEquals(0, dropped);
        Assert.assertTrue(w.isHeadTrimmedOnLastRecovery());
        Assert.assertNotEquals(sourceOffset, w.getHeadEntryOffset());

        // Source entry bytes must be byte-for-byte unchanged: a concurrent
        // reader holding a cached head pointer = sourceOffset must observe
        // the original consistent snapshot, not a half-mutated state.
        for (int i = 0; i < sourceBytes.length; i++) {
            Assert.assertEquals(
                    "source byte at index " + i + " was mutated by applyHeadTrim",
                    sourceBytes[i], mem.getLong(sourceOffset + (long) i * Long.BYTES)
            );
        }
    }

    @Test
    public void testRecoveryHeadTrimPreservesPrevPointerChain() {
        PostingIndexChainWriter w = new PostingIndexChainWriter();
        w.initialiseEmpty(mem);

        long olderOffset = w.appendNewEntry(mem, 1, 5, 0, 0, 0, 1, 64, 0);
        long headOffset = w.appendNewEntry(mem, 2, 10, 0, 0, 0, 2, 64, 0);
        long headSlot1 = headOffset + PostingIndexUtils.V2_ENTRY_HEADER_SIZE
                + (long) PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
        mem.putLong(headSlot1 + PostingIndexUtils.GEN_DIR_OFFSET_TXN_AT_SEAL, 11L);

        int dropped = w.recoveryDropAbandoned(mem, /* currentTableTxn */ 10L, new LongList());

        Assert.assertEquals(0, dropped);
        Assert.assertTrue(w.isHeadTrimmedOnLastRecovery());
        Assert.assertEquals(2L, w.getEntryCount());

        long newHead = w.getHeadEntryOffset();
        Assert.assertNotEquals(headOffset, newHead);
        Assert.assertEquals(
                "new head's prev must skip the source entry and point at the older entry",
                olderOffset, mem.getLong(newHead + PostingIndexUtils.V2_ENTRY_OFFSET_PREV_ENTRY_OFFSET)
        );
    }

    @Test
    public void testRecoveryHeadTrimAfterDropsCopiesToVirginSpace() {
        // Drop-then-trim: the head entry is abandoned (dropped), and the
        // entry below it is visible but carries an in-flight tail gen
        // (trimmed). The trimmed copy must land in virgin space past the
        // ORIGINAL regionLimit, never on the just-dropped entry's bytes --
        // that entry was a previously-published head and a concurrent
        // reader may still have it cached; overwriting it before the
        // header republish would expose torn bytes to a straddling reader.
        PostingIndexChainWriter w = new PostingIndexChainWriter();
        w.initialiseEmpty(mem);

        // E1: visible (txnAtSeal=10), genCount=2 with an in-flight slot[1].
        long trimmedOffset = w.appendNewEntry(mem, 1, 10, 0, 0, 0, 2, 64, 0);
        long e1Slot0 = trimmedOffset + PostingIndexUtils.V2_ENTRY_HEADER_SIZE;
        long e1Slot1 = e1Slot0 + PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
        mem.putLong(e1Slot0 + PostingIndexUtils.GEN_DIR_OFFSET_FILE_OFFSET, 0L);
        mem.putLong(e1Slot0 + PostingIndexUtils.GEN_DIR_OFFSET_SIZE, 64L);
        mem.putLong(e1Slot0 + PostingIndexUtils.GEN_DIR_OFFSET_MAX_VALUE, 42L);
        mem.putInt(e1Slot0 + PostingIndexUtils.GEN_DIR_OFFSET_MAX_KEY, 4);
        mem.putLong(e1Slot1 + PostingIndexUtils.GEN_DIR_OFFSET_TXN_AT_SEAL, 11L);

        // E2: abandoned head (txnAtSeal=20 > committedTxn 10), gets dropped.
        long droppedOffset = w.appendNewEntry(mem, 2, 20, 0, 0, 0, 1, 64, 0);
        long preRecoveryRegionLimit = w.getRegionLimit();

        // Snapshot E2's bytes: recovery must leave them byte-for-byte intact.
        long droppedEntryLen = PostingIndexChainEntry.entrySize(1, 0);
        long[] droppedBytes = new long[(int) (droppedEntryLen / Long.BYTES)];
        for (int i = 0; i < droppedBytes.length; i++) {
            droppedBytes[i] = mem.getLong(droppedOffset + (long) i * Long.BYTES);
        }

        LongList orphans = new LongList();
        int dropped = w.recoveryDropAbandoned(mem, /* currentTableTxn */ 10L, orphans);

        Assert.assertEquals(1, dropped);
        Assert.assertEquals(1, orphans.size());
        Assert.assertEquals(2L, orphans.getQuick(0));
        Assert.assertTrue(w.isHeadTrimmedOnLastRecovery());
        Assert.assertEquals(1L, w.getEntryCount());

        // The trimmed copy lands past the original regionLimit, not on the
        // dropped entry's bytes.
        long newHead = w.getHeadEntryOffset();
        Assert.assertEquals(preRecoveryRegionLimit, newHead);
        Assert.assertNotEquals(droppedOffset, newHead);
        Assert.assertEquals(1, mem.getInt(newHead + PostingIndexUtils.V2_ENTRY_OFFSET_GEN_COUNT));
        Assert.assertEquals(
                PostingIndexChainEntry.entrySize(1, 0),
                mem.getLong(newHead + PostingIndexUtils.V2_ENTRY_OFFSET_LEN)
        );
        Assert.assertEquals(42L, mem.getLong(newHead + PostingIndexUtils.V2_ENTRY_OFFSET_MAX_VALUE));
        Assert.assertEquals(64L, mem.getLong(newHead + PostingIndexUtils.V2_ENTRY_OFFSET_VALUE_MEM_SIZE));
        Assert.assertEquals(5, mem.getInt(newHead + PostingIndexUtils.V2_ENTRY_OFFSET_KEY_COUNT));

        // Regression guard: the dropped entry's bytes must be untouched. A
        // concurrent reader with headEntryOffset == droppedOffset cached
        // must keep observing a consistent snapshot until it re-reads the
        // republished header.
        for (int i = 0; i < droppedBytes.length; i++) {
            Assert.assertEquals(
                    "dropped entry byte at index " + i + " was overwritten by applyHeadTrim",
                    droppedBytes[i], mem.getLong(droppedOffset + (long) i * Long.BYTES)
            );
        }
    }

    @Test
    public void testRecoveryWholeEntryDroppedWhenSlot0InFlight() {
        PostingIndexChainWriter w = new PostingIndexChainWriter();
        w.initialiseEmpty(mem);

        w.appendNewEntry(mem, 1, 10, 0, 0, 0, 1, 64, 0);
        long inFlightOffset = w.appendNewEntry(mem, 2, 20, 0, 0, 0, 1, 64, 0);
        long inFlightSlot0 = inFlightOffset + PostingIndexUtils.V2_ENTRY_HEADER_SIZE;
        mem.putLong(inFlightSlot0 + PostingIndexUtils.GEN_DIR_OFFSET_TXN_AT_SEAL, 20L);

        LongList orphans = new LongList();
        int dropped = w.recoveryDropAbandoned(mem, /* currentTableTxn */ 10L, orphans);

        Assert.assertEquals(1, dropped);
        Assert.assertFalse(w.isHeadTrimmedOnLastRecovery());
        Assert.assertEquals(1, orphans.size());
        Assert.assertEquals(2L, orphans.getQuick(0));
        Assert.assertEquals(1L, w.getEntryCount());
    }

    @Test
    public void testResetStateClearsInMemoryMirror() {
        PostingIndexChainWriter w = new PostingIndexChainWriter();
        w.initialiseEmpty(mem);
        w.appendNewEntry(mem, 1, 10, 0, 0, 0, 1, 64, 0);

        w.resetState();

        Assert.assertEquals(PostingIndexUtils.PAGE_A_OFFSET, w.getActivePageOffset());
        Assert.assertEquals(PostingIndexUtils.V2_NO_HEAD, w.getHeadEntryOffset());
        Assert.assertEquals(0, w.getEntryCount());
        Assert.assertEquals(-1L, w.getGenCounter());
        Assert.assertEquals(-1L, w.getCurrentTxnAtSeal());
        Assert.assertEquals(PostingIndexUtils.V2_ENTRY_REGION_BASE, w.getRegionLimit());
    }

    @Test
    public void testUpdateHeadMaxValueDoesNotChangeStructure() {
        PostingIndexChainWriter w = new PostingIndexChainWriter();
        w.initialiseEmpty(mem);

        long off = w.appendNewEntry(mem, 1, 10, 100, 50, 5, 1, 64, 0);
        long lenBefore = mem.getLong(off + PostingIndexUtils.V2_ENTRY_OFFSET_LEN);
        int genCountBefore = mem.getInt(off + PostingIndexUtils.V2_ENTRY_OFFSET_GEN_COUNT);
        long limitBefore = w.getRegionLimit();

        w.updateHeadMaxValue(mem, /* maxValue */ 7777);

        Assert.assertEquals(7777, mem.getLong(off + PostingIndexUtils.V2_ENTRY_OFFSET_MAX_VALUE));
        Assert.assertEquals(lenBefore, mem.getLong(off + PostingIndexUtils.V2_ENTRY_OFFSET_LEN));
        Assert.assertEquals(genCountBefore, mem.getInt(off + PostingIndexUtils.V2_ENTRY_OFFSET_GEN_COUNT));
        Assert.assertEquals(limitBefore, w.getRegionLimit());

        // The header seqlock advanced, so the picker observes the new max-value.
        PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
        PostingIndexChainEntry.Snapshot picked = new PostingIndexChainEntry.Snapshot();
        Assert.assertEquals(PostingIndexChainPicker.RESULT_OK,
                PostingIndexChainPicker.pick(mem, 50L, header, picked));
        Assert.assertEquals(7777, picked.maxValue);
    }

    @Test
    public void testUpdateHeadMaxValueOnEmptyChainIsNoOp() {
        PostingIndexChainWriter w = new PostingIndexChainWriter();
        w.initialiseEmpty(mem);
        // Should not throw.
        w.updateHeadMaxValue(mem, 100L);
        Assert.assertFalse(w.hasHead());
    }
}

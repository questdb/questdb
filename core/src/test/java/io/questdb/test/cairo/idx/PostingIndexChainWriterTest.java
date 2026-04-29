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

        long off1 = w.appendNewEntry(mem, 1, 10, 0, 0, 0, 0, 64, 0);
        long off2 = w.appendNewEntry(mem, 2, 20, 0, 0, 0, 0, 64, 0);
        long off3 = w.appendNewEntry(mem, 3, 30, 0, 0, 0, 0, 64, 0);

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
        w.appendNewEntry(mem, 5, 10, 0, 0, 0, 0, 64, 0);

        try {
            w.appendNewEntry(mem, 5, 11, 0, 0, 0, 0, 64, 0);
            Assert.fail("expected CairoException for non-monotonic sealTxn");
        } catch (CairoException expected) {
            Assert.assertTrue(expected.getMessage().contains("sealTxn must advance"));
        }

        try {
            w.appendNewEntry(mem, 4, 12, 0, 0, 0, 0, 64, 0);
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
        long off2 = off1 + PostingIndexChainEntry.entrySize(0);

        PostingIndexChainEntry.writeHeader(mem, off1, 1, 100, 0, 0, 0, 0, 64, 0, off2);
        PostingIndexChainEntry.writeHeader(mem, off2, 2, 200, 0, 0, 0, 0, 64, 0, off1);
        long limit = off2 + PostingIndexChainEntry.entrySize(0);
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
        writer.appendNewEntry(mem, 1, 10, 0, 0, 0, 0, 64, 0);
        writer.appendNewEntry(mem, 2, 20, 0, 0, 0, 0, 64, 0);

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
    public void testRecoveryDropAbandonedKeepsOnlyCommittedEntries() {
        PostingIndexChainWriter w = new PostingIndexChainWriter();
        w.initialiseEmpty(mem);

        // Healthy entries — txnAtSeal at 10, 20, 30.
        w.appendNewEntry(mem, 1, 10, 0, 0, 0, 0, 64, 0);
        w.appendNewEntry(mem, 2, 20, 0, 0, 0, 0, 64, 0);
        w.appendNewEntry(mem, 3, 30, 0, 0, 0, 0, 64, 0);
        // Two abandoned entries — txnAtSeal beyond what _txn ever committed.
        long expectedRegionLimitAfter = w.appendNewEntry(mem, 4, 99, 0, 0, 0, 0, 64, 0);
        w.appendNewEntry(mem, 5, 100, 0, 0, 0, 0, 64, 0);

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
        w.appendNewEntry(mem, 1, 10, 0, 0, 0, 0, 64, 0);
        w.appendNewEntry(mem, 2, 20, 0, 0, 0, 0, 64, 0);
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
    public void testResetStateClearsInMemoryMirror() {
        PostingIndexChainWriter w = new PostingIndexChainWriter();
        w.initialiseEmpty(mem);
        w.appendNewEntry(mem, 1, 10, 0, 0, 0, 0, 64, 0);

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

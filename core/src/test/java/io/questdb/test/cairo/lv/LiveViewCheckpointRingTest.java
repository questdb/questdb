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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewCheckpointRingManifest;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewLifecycleState;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit coverage for the retained-checkpoint ring on {@link LiveViewInstance}
 * (add / accessors / prune). The ring backs the O3 resume-from-nearest-checkpoint
 * path: instead of one head checkpoint, the flush cycle retains a bounded window of
 * recent checkpoints so a cross-commit / apply-ahead O3 can resume from the newest
 * sealed checkpoint below the late row rather than rebuilding the whole view.
 * <p>
 * These tests lock the in-memory bookkeeping in isolation (no disk, no refresh worker):
 * records stay ordered by {@code maxTs}, prune evicts oldest-first under each bound and
 * always keeps at least one, and eviction reports the {@code lvSeqTxn}s the caller must
 * unlink.
 */
public class LiveViewCheckpointRingTest {

    @Test
    public void testAddAndAccessors() {
        LiveViewInstance instance = newStubInstance();
        // (lvSeqTxn, maxTs, baseSeqTxn, lvRowsTotal, stateBytes)
        instance.addRetainedCheckpoint(1, 10, 100, 1000, 50);
        instance.addRetainedCheckpoint(2, 20, 200, 2000, 60);
        instance.addRetainedCheckpoint(3, 30, 300, 3000, 70);

        Assert.assertEquals(3, instance.getRetainedCheckpointCount());

        // Oldest (position 0).
        Assert.assertEquals(1, instance.getRetainedCheckpointLvSeqTxn(0));
        Assert.assertEquals(10, instance.getRetainedCheckpointMaxTs(0));
        Assert.assertEquals(100, instance.getRetainedCheckpointBaseSeqTxn(0));
        Assert.assertEquals(1000, instance.getRetainedCheckpointLvRowsTotal(0));
        Assert.assertEquals(50, instance.getRetainedCheckpointStateBytes(0));

        // Newest (position count - 1).
        Assert.assertEquals(3, instance.getRetainedCheckpointLvSeqTxn(2));
        Assert.assertEquals(30, instance.getRetainedCheckpointMaxTs(2));
        Assert.assertEquals(300, instance.getRetainedCheckpointBaseSeqTxn(2));
        Assert.assertEquals(3000, instance.getRetainedCheckpointLvRowsTotal(2));
        Assert.assertEquals(70, instance.getRetainedCheckpointStateBytes(2));

        Assert.assertEquals(50 + 60 + 70, instance.getRetainedCheckpointsTotalStateBytes());
    }

    @Test
    public void testCopyRetainedCheckpointsToAppendsPackedRecords() {
        LiveViewInstance instance = newStubInstance();
        instance.addRetainedCheckpoint(1, 10, 100, 1000, 50);
        instance.addRetainedCheckpoint(2, 20, 200, 2000, 60);

        // The manifest publishes off this copy, so the packed layout it produces
        // is the manifest's entry layout: ENTRY_SIZE longs per record, oldest
        // first, fields in (lvSeqTxn, maxTs, baseSeqTxn, lvRowsTotal, stateBytes)
        // order. A field reordered on either side turns anchors into garbage
        // silently, which is what the assert in copyRetainedCheckpointsTo guards.
        LongList snapshot = new LongList();
        instance.copyRetainedCheckpointsTo(snapshot);

        Assert.assertEquals(2 * LiveViewCheckpointRingManifest.ENTRY_SIZE, snapshot.size());
        assertEntry(snapshot, 0, 1, 10, 100, 1000, 50);
        assertEntry(snapshot, 1, 2, 20, 200, 2000, 60);
    }

    @Test
    public void testCopyRetainedCheckpointsToEmptyRing() {
        LiveViewInstance instance = newStubInstance();

        // A boundary rebuild retires the whole ring and publishes an empty
        // manifest, so an empty copy is a normal outcome, not an error.
        LongList snapshot = new LongList();
        instance.copyRetainedCheckpointsTo(snapshot);

        Assert.assertEquals(0, snapshot.size());
    }

    @Test
    public void testCopyRetainedCheckpointsToTracksRingMutation() {
        LiveViewInstance instance = newStubInstance();
        instance.addRetainedCheckpoint(1, 10, 100, 1000, 50);
        instance.addRetainedCheckpoint(2, 20, 200, 2000, 60);

        LongList snapshot = new LongList();
        instance.copyRetainedCheckpointsTo(snapshot);

        // The copy must be a snapshot, not a view: a later retire cannot reach
        // back into a publication that already read it.
        instance.invalidateRetainedCheckpointsFrom(20, null);
        Assert.assertEquals(1, instance.getRetainedCheckpointCount());
        Assert.assertEquals(2 * LiveViewCheckpointRingManifest.ENTRY_SIZE, snapshot.size());

        // Re-taking it after the retire sees the survivors only - the membership
        // the next publication lists.
        snapshot.clear();
        instance.copyRetainedCheckpointsTo(snapshot);
        Assert.assertEquals(LiveViewCheckpointRingManifest.ENTRY_SIZE, snapshot.size());
        assertEntry(snapshot, 0, 1, 10, 100, 1000, 50);
    }

    @Test
    public void testPruneByBytes() {
        LiveViewInstance instance = newStubInstance();
        for (int i = 1; i <= 5; i++) {
            instance.addRetainedCheckpoint(i, i * 10L, i * 100L, i * 1000L, 100);
        }

        LongList evicted = new LongList();
        // count / horizon disabled; keep total <= 250 bytes (100 bytes each -> keep 2).
        instance.pruneRetainedCheckpoints(-1, 250, Numbers.LONG_NULL, evicted);

        Assert.assertEquals(2, instance.getRetainedCheckpointCount());
        Assert.assertEquals(200, instance.getRetainedCheckpointsTotalStateBytes());
        // The 3 oldest were dropped; the 2 newest survive.
        Assert.assertEquals(3, evicted.size());
        Assert.assertEquals(1, evicted.getQuick(0));
        Assert.assertEquals(2, evicted.getQuick(1));
        Assert.assertEquals(3, evicted.getQuick(2));
        Assert.assertEquals(4, instance.getRetainedCheckpointLvSeqTxn(0));
        Assert.assertEquals(5, instance.getRetainedCheckpointLvSeqTxn(1));
    }

    @Test
    public void testPruneByCount() {
        LiveViewInstance instance = newStubInstance();
        for (int i = 1; i <= 5; i++) {
            instance.addRetainedCheckpoint(i, i * 10L, i * 100L, i * 1000L, 100);
        }

        LongList evicted = new LongList();
        // bytes / horizon disabled; keep the 2 newest.
        instance.pruneRetainedCheckpoints(2, -1, Numbers.LONG_NULL, evicted);

        Assert.assertEquals(2, instance.getRetainedCheckpointCount());
        Assert.assertEquals(3, evicted.size());
        Assert.assertEquals(1, evicted.getQuick(0));
        Assert.assertEquals(2, evicted.getQuick(1));
        Assert.assertEquals(3, evicted.getQuick(2));
        // Survivors keep the highest maxTs.
        Assert.assertEquals(40, instance.getRetainedCheckpointMaxTs(0));
        Assert.assertEquals(50, instance.getRetainedCheckpointMaxTs(1));
    }

    @Test
    public void testPruneByHorizon() {
        LiveViewInstance instance = newStubInstance();
        // maxTs = 10, 20, 30, 40, 50
        for (int i = 1; i <= 5; i++) {
            instance.addRetainedCheckpoint(i, i * 10L, i * 100L, i * 1000L, 100);
        }

        LongList evicted = new LongList();
        // Drop everything with maxTs < 35 (i.e. 10, 20, 30); count / bytes disabled.
        instance.pruneRetainedCheckpoints(-1, -1, 35, evicted);

        Assert.assertEquals(2, instance.getRetainedCheckpointCount());
        Assert.assertEquals(3, evicted.size());
        Assert.assertEquals(40, instance.getRetainedCheckpointMaxTs(0));
        Assert.assertEquals(50, instance.getRetainedCheckpointMaxTs(1));
    }

    @Test
    public void testPruneKeepsAtLeastOne() {
        LiveViewInstance instance = newStubInstance();
        instance.addRetainedCheckpoint(1, 10, 100, 1000, 100);
        instance.addRetainedCheckpoint(2, 20, 200, 2000, 100);
        instance.addRetainedCheckpoint(3, 30, 300, 3000, 100);

        LongList evicted = new LongList();
        // Every bound demands eviction, but the newest entry must survive for restart.
        instance.pruneRetainedCheckpoints(0, 1, Numbers.LONG_NULL, evicted);

        Assert.assertEquals(1, instance.getRetainedCheckpointCount());
        Assert.assertEquals(3, instance.getRetainedCheckpointLvSeqTxn(0));
        Assert.assertEquals(30, instance.getRetainedCheckpointMaxTs(0));
        Assert.assertEquals(2, evicted.size());
    }

    @Test
    public void testPruneNoOpWhenWithinBounds() {
        LiveViewInstance instance = newStubInstance();
        instance.addRetainedCheckpoint(1, 10, 100, 1000, 100);
        instance.addRetainedCheckpoint(2, 20, 200, 2000, 100);

        LongList evicted = new LongList();
        instance.pruneRetainedCheckpoints(8, 1_000_000, Numbers.LONG_NULL, evicted);

        Assert.assertEquals(2, instance.getRetainedCheckpointCount());
        Assert.assertEquals(0, evicted.size());
    }

    @Test
    public void testPruneToleratesNullEvictedList() {
        LiveViewInstance instance = newStubInstance();
        instance.addRetainedCheckpoint(1, 10, 100, 1000, 100);
        instance.addRetainedCheckpoint(2, 20, 200, 2000, 100);
        instance.addRetainedCheckpoint(3, 30, 300, 3000, 100);

        instance.pruneRetainedCheckpoints(1, -1, Numbers.LONG_NULL, null);

        Assert.assertEquals(1, instance.getRetainedCheckpointCount());
        Assert.assertEquals(3, instance.getRetainedCheckpointLvSeqTxn(0));
    }

    @Test
    public void testRemoveRetainedCheckpointAbsentIsNoOp() {
        LiveViewInstance instance = newStubInstance();
        instance.addRetainedCheckpoint(1, 10, 100, 1000, 100);
        instance.addRetainedCheckpoint(2, 20, 200, 2000, 100);

        // An lvSeqTxn not in the ring (restart / seed restore run with an empty
        // ring, and the head is not always a ring entry) leaves the ring intact.
        Assert.assertFalse(instance.removeRetainedCheckpoint(99));

        Assert.assertEquals(2, instance.getRetainedCheckpointCount());
        Assert.assertEquals(1, instance.getRetainedCheckpointLvSeqTxn(0));
        Assert.assertEquals(2, instance.getRetainedCheckpointLvSeqTxn(1));
    }

    @Test
    public void testRemoveRetainedCheckpointMiddle() {
        LiveViewInstance instance = newStubInstance();
        instance.addRetainedCheckpoint(1, 10, 100, 1000, 50);
        instance.addRetainedCheckpoint(2, 20, 200, 2000, 60);
        instance.addRetainedCheckpoint(3, 30, 300, 3000, 70);

        // Evict a mid-ring anchor (a corrupt .cp found unusable): the newer and
        // older entries around it survive, maxTs order stays intact, and the
        // byte total drops by exactly the evicted entry's state bytes.
        Assert.assertTrue(instance.removeRetainedCheckpoint(2));

        Assert.assertEquals(2, instance.getRetainedCheckpointCount());
        Assert.assertEquals(1, instance.getRetainedCheckpointLvSeqTxn(0));
        Assert.assertEquals(10, instance.getRetainedCheckpointMaxTs(0));
        Assert.assertEquals(3, instance.getRetainedCheckpointLvSeqTxn(1));
        Assert.assertEquals(30, instance.getRetainedCheckpointMaxTs(1));
        Assert.assertEquals(50 + 70, instance.getRetainedCheckpointsTotalStateBytes());
    }

    @Test
    public void testRecordCheckpointRingPublication() {
        LiveViewInstance instance = newStubInstance();

        // Nothing published yet: generation 0 (a real manifest always carries
        // >= 1) and no covered seqTxn to compare a reconciled floor against.
        Assert.assertEquals(0, instance.getLastPublishedRingGeneration());
        Assert.assertEquals(Numbers.LONG_NULL, instance.getLastPublishedRingCoveredBaseSeqTxn());
        // No manifest, so nothing to resume from: WalPurgeJob's base WAL floor
        // stays where the head arm alone puts it.
        Assert.assertEquals(Numbers.LONG_NULL, instance.getLastPublishedRingNewestBaseSeqTxn());
        Assert.assertFalse(instance.isCheckpointRingDirty());

        instance.recordCheckpointRingPublication(1, 100, 90);
        Assert.assertEquals(1, instance.getLastPublishedRingGeneration());
        Assert.assertEquals(100, instance.getLastPublishedRingCoveredBaseSeqTxn());
        Assert.assertEquals(90, instance.getLastPublishedRingNewestBaseSeqTxn());
        Assert.assertFalse(instance.isCheckpointRingDirty());

        instance.recordCheckpointRingPublication(2, 140, 130);
        Assert.assertEquals(2, instance.getLastPublishedRingGeneration());
        Assert.assertEquals(140, instance.getLastPublishedRingCoveredBaseSeqTxn());
        Assert.assertEquals(130, instance.getLastPublishedRingNewestBaseSeqTxn());

        // An O3 retire publishes survivors, so the newest listed entry - and the
        // floor with it - moves DOWN while covered moves up. The floor has to
        // follow it down: that survivor is what a trusting restart resumes from.
        instance.recordCheckpointRingPublication(3, 160, 90);
        Assert.assertEquals(160, instance.getLastPublishedRingCoveredBaseSeqTxn());
        Assert.assertEquals(90, instance.getLastPublishedRingNewestBaseSeqTxn());

        // A retire that empties the ring unlinks every .cp with it, so there is
        // nothing left to resume from and the floor is released.
        instance.recordCheckpointRingPublication(4, 200, Numbers.LONG_NULL);
        Assert.assertEquals(200, instance.getLastPublishedRingCoveredBaseSeqTxn());
        Assert.assertEquals(Numbers.LONG_NULL, instance.getLastPublishedRingNewestBaseSeqTxn());
    }

    @Test
    public void testRecordCheckpointRingPublicationFailureHoldsCovered() {
        LiveViewInstance instance = newStubInstance();
        instance.recordCheckpointRingPublication(1, 100, 90);

        // covered describes what is durable, so a failed publication must not
        // advance it: that is exactly what makes the failure safe. A restart here
        // finds covered (100) unequal to a reconciled floor that moved past it,
        // and falls back instead of trusting stale membership.
        instance.recordCheckpointRingPublicationFailure();
        Assert.assertTrue(instance.isCheckpointRingDirty());
        Assert.assertEquals(1, instance.getLastPublishedRingGeneration());
        Assert.assertEquals(100, instance.getLastPublishedRingCoveredBaseSeqTxn());
        // The floor holds with it, for the same reason and a sharper one: the
        // in-memory ring may already carry an entry the failed publication never
        // listed, and letting the floor reach it would release the base WAL the
        // durable manifest's older newest entry still resumes from.
        Assert.assertEquals(90, instance.getLastPublishedRingNewestBaseSeqTxn());

        // The next success claims the generation the failed attempt did not, and
        // clears the flag.
        instance.recordCheckpointRingPublication(2, 180, 170);
        Assert.assertFalse(instance.isCheckpointRingDirty());
        Assert.assertEquals(2, instance.getLastPublishedRingGeneration());
        Assert.assertEquals(180, instance.getLastPublishedRingCoveredBaseSeqTxn());
        Assert.assertEquals(170, instance.getLastPublishedRingNewestBaseSeqTxn());
    }

    @Test
    public void testRemoveRetainedCheckpointNewest() {
        LiveViewInstance instance = newStubInstance();
        instance.addRetainedCheckpoint(1, 10, 100, 1000, 50);
        instance.addRetainedCheckpoint(2, 20, 200, 2000, 60);
        instance.addRetainedCheckpoint(3, 30, 300, 3000, 70);

        // Evict the newest entry (the head is the newest ring entry): the older
        // sealed anchors remain available as resume points.
        Assert.assertTrue(instance.removeRetainedCheckpoint(3));

        Assert.assertEquals(2, instance.getRetainedCheckpointCount());
        Assert.assertEquals(1, instance.getRetainedCheckpointLvSeqTxn(0));
        Assert.assertEquals(2, instance.getRetainedCheckpointLvSeqTxn(1));
        Assert.assertEquals(20, instance.getRetainedCheckpointMaxTs(1));
    }

    private static void assertEntry(
            LongList snapshot,
            int index,
            long lvSeqTxn,
            long maxTs,
            long baseSeqTxn,
            long lvRowsTotal,
            long stateBytes
    ) {
        final int base = index * LiveViewCheckpointRingManifest.ENTRY_SIZE;
        Assert.assertEquals(lvSeqTxn, snapshot.getQuick(base + LiveViewCheckpointRingManifest.ENTRY_LV_SEQ_TXN));
        Assert.assertEquals(maxTs, snapshot.getQuick(base + LiveViewCheckpointRingManifest.ENTRY_MAX_TS));
        Assert.assertEquals(baseSeqTxn, snapshot.getQuick(base + LiveViewCheckpointRingManifest.ENTRY_BASE_SEQ_TXN));
        Assert.assertEquals(lvRowsTotal, snapshot.getQuick(base + LiveViewCheckpointRingManifest.ENTRY_LV_ROWS_TOTAL));
        Assert.assertEquals(stateBytes, snapshot.getQuick(base + LiveViewCheckpointRingManifest.ENTRY_STATE_BYTES));
    }

    private static LiveViewInstance newStubInstance() {
        // A definition-less stub is enough: the ring helpers only touch the LongList,
        // never the definition or any disk-backed state.
        TableToken token = new TableToken("core_price_lv", "core_price_lv~1", null, 1, true, false, false);
        return new LiveViewInstance(token, LiveViewLifecycleState.STATE_UNREADABLE);
    }
}

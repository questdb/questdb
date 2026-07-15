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

    private static LiveViewInstance newStubInstance() {
        // A definition-less stub is enough: the ring helpers only touch the LongList,
        // never the definition or any disk-backed state.
        TableToken token = new TableToken("core_price_lv", "core_price_lv~1", null, 1, true, false, false);
        return new LiveViewInstance(token, LiveViewLifecycleState.STATE_UNREADABLE);
    }
}

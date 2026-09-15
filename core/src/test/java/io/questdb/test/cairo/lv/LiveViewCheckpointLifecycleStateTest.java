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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.lv.LiveViewCheckpointLifecycleState;
import io.questdb.std.LongList;
import org.junit.Assert;
import org.junit.Test;

public class LiveViewCheckpointLifecycleStateTest {

    @Test
    public void testDistinctLifecycleIdentityDoesNotAliasAndResetReusesRetirementShell() {
        final LiveViewCheckpointLifecycleState state = new LiveViewCheckpointLifecycleState();
        final LongList retired = new LongList();
        retired.add(11);
        retired.add(12);

        state.markReconciled(7);
        state.replacePendingRetirements(7, retired);
        state.markOrphanScanCompleted(7, false);
        state.incrementSweepsSinceOrphanScan(7);
        final int oldShell = state.getPendingRetirementIdentityForTest(7);

        Assert.assertFalse("new identity must not inherit reconciliation", state.isReconciled(8));
        Assert.assertFalse("new identity must not inherit completed scan", state.isOrphanScanCompleted(8));
        Assert.assertFalse("new identity must not inherit orphan risk", state.isOrphanScanNeeded(8));
        Assert.assertEquals("new identity must not inherit cadence", 0, state.getSweepCountForTest(8));
        Assert.assertNull("new identity must not inherit retirements", state.getPendingRetirements(8));
        state.reset(7);
        Assert.assertEquals(1, state.getRetirementPoolSizeForTest());

        retired.clear();
        retired.add(21);
        state.replacePendingRetirements(8, retired);
        Assert.assertEquals("identity rollover must reuse the dropped shell", oldShell,
                state.getPendingRetirementIdentityForTest(8));
        Assert.assertEquals(21, state.getPendingRetirements(8).getQuick(0));
    }

    @Test
    public void testDropResetPrunesGenerationAndPoolsRetirementShell() {
        final LiveViewCheckpointLifecycleState state = new LiveViewCheckpointLifecycleState();
        final LongList retired = new LongList();
        retired.add(31);
        state.markReconciled(9);
        state.replacePendingRetirements(9, retired);
        final int oldShell = state.getPendingRetirementIdentityForTest(9);

        state.reset(9);
        Assert.assertEquals("drop must prune the live generation entry", 0, state.getActiveGenerationCountForTest());
        Assert.assertEquals(1, state.getRetirementPoolSizeForTest());

        retired.setQuick(0, 41);
        state.replacePendingRetirements(10, retired);
        Assert.assertEquals(1, state.getActiveGenerationCountForTest());
        Assert.assertFalse("recreated view must not inherit reconciliation", state.isReconciled(10));
        Assert.assertEquals("recreate must refill the dropped generation's shell", oldShell,
                state.getPendingRetirementIdentityForTest(10));
    }

    @Test
    public void testSuccessfulPublicationReturnsPendingShellAndCloseClearsEverything() {
        final LiveViewCheckpointLifecycleState state = new LiveViewCheckpointLifecycleState();
        final LongList retired = new LongList();
        retired.add(51);
        state.replacePendingRetirements(23, retired);
        state.clearPendingRetirements(23);
        Assert.assertEquals(1, state.getRetirementPoolSizeForTest());

        state.clear();
        Assert.assertEquals(0, state.getActiveGenerationCountForTest());
        Assert.assertEquals(0, state.getRetirementPoolSizeForTest());
    }
}

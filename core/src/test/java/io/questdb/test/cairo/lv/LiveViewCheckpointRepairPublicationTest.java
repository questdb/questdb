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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.lv.LiveViewCheckpointContracts.RepairPublicationStage;
import io.questdb.cairo.lv.LiveViewCheckpointRepairPublication;
import io.questdb.cairo.lv.LiveViewCheckpointRepairPublication.RuntimeDisposition;
import io.questdb.std.Numbers;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The repair publication ordering as a machine rather than a comment. These are
 * narrow unit tests over {@link LiveViewCheckpointRepairPublication}: no native
 * memory, no engine. The end-to-end consequences - a stalled apply deferring the
 * repair, the runtime handed back exactly once - are in
 * {@link LiveViewCheckpointTimelineRepairTest}.
 */
public class LiveViewCheckpointRepairPublicationTest {

    @Test
    public void testAStageMayNotRepeatOrGoBackwards() {
        final LiveViewCheckpointRepairPublication publication = new LiveViewCheckpointRepairPublication();
        publication.plan();
        publication.candidateReady(RuntimeDisposition.PROMOTE_REPLAY);
        try {
            publication.candidateReady(RuntimeDisposition.KEEP_PRIMARY);
            Assert.fail("expected the repeated stage to be refused");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "is not moving forward");
        }
        // The refusal must not have taken the disposition with it.
        Assert.assertSame(RuntimeDisposition.PROMOTE_REPLAY, publication.getRuntimeDisposition());

        publication.replacementCommitted(9);
        publication.replacementApplied();
        try {
            publication.timelinePublished();
            publication.timelinePublished();
            Assert.fail("expected the repeated publication to be refused");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "is not moving forward");
        }
    }

    @Test
    public void testAnEmptyRepairSkipsEveryStageItHasNoWorkFor() {
        // A rebuild that emitted nothing, replaced nothing and localized nothing still
        // walks the ordering: it just never commits a replacement or publishes a
        // generation, so it reaches the watermark straight from the runtime exchange.
        final LiveViewCheckpointRepairPublication publication = new LiveViewCheckpointRepairPublication();
        publication.plan();
        publication.candidateReady(RuntimeDisposition.PROMOTE_REPLAY);
        Assert.assertFalse(publication.hasCommittedReplacement());
        Assert.assertEquals(Numbers.LONG_NULL, publication.getCommittedLvSeqTxn());
        Assert.assertTrue(
                "nothing committed is trivially reconciled",
                publication.isReplacementReconciled()
        );

        publication.runtimePromoted();
        publication.watermarkAdvanced();
        Assert.assertEquals(
                RepairPublicationStage.CONSUMED_WATERMARK_AND_PURGE_FLOOR_ADVANCED,
                publication.getStage()
        );
    }

    @Test
    public void testAnUnappliedReplacementBlocksThePublicationAndTheWatermark() {
        final LiveViewCheckpointRepairPublication publication = new LiveViewCheckpointRepairPublication();
        publication.plan();
        publication.candidateReady(RuntimeDisposition.KEEP_PRIMARY);
        publication.replacementCommitted(41);

        Assert.assertTrue(publication.hasCommittedReplacement());
        Assert.assertEquals(41, publication.getCommittedLvSeqTxn());
        Assert.assertFalse(publication.isReplacementReconciled());

        try {
            publication.timelinePublished();
            Assert.fail("expected the generation to be refused over an unapplied replacement");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "unapplied replacement");
        }
        try {
            publication.watermarkAdvanced();
            Assert.fail("expected the watermark to be refused over an unapplied replacement");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "past unmaterialized output");
        }

        // The runtime is the one thing that still has to settle: the disposition was
        // fixed before the commit and the compiled factory must not be left mixed. It
        // does not claim the stage, so nothing downstream of it opens up.
        publication.runtimePromoted();
        Assert.assertTrue(publication.isRuntimeSettled());
        Assert.assertEquals(RepairPublicationStage.LV_WAL_REPLACEMENT_COMMITTED, publication.getStage());
        try {
            publication.watermarkAdvanced();
            Assert.fail("expected the watermark to stay refused");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "past unmaterialized output");
        }
    }

    @Test
    public void testClearReturnsThePublicationToItsUnopenedState() {
        final LiveViewCheckpointRepairPublication publication = new LiveViewCheckpointRepairPublication();
        publication.plan();
        publication.candidateReady(RuntimeDisposition.KEEP_PRIMARY);
        publication.replacementCommitted(3);
        publication.replacementApplied();
        publication.runtimePromoted();

        publication.clear();
        Assert.assertNull(publication.getStage());
        Assert.assertNull(publication.getRuntimeDisposition());
        Assert.assertFalse(publication.hasCommittedReplacement());
        Assert.assertFalse(publication.isRuntimeSettled());
        Assert.assertFalse(publication.isKeepPrimaryRuntime());
        // Reusable across repairs: the next one opens at PLAN like the first.
        publication.plan();
        Assert.assertEquals(RepairPublicationStage.PLAN, publication.getStage());
    }

    @Test
    public void testEveryStageRequiresTheOneItDependsOn() {
        final LiveViewCheckpointRepairPublication publication = new LiveViewCheckpointRepairPublication();
        try {
            publication.candidateReady(RuntimeDisposition.KEEP_PRIMARY);
            Assert.fail("expected a repair that never planned to be refused");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "instead of PLAN");
        }

        publication.plan();
        try {
            publication.replacementCommitted(1);
            Assert.fail("expected a commit before the candidate roots to be refused");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "candidate roots are not ready");
        }

        publication.candidateReady(RuntimeDisposition.KEEP_PRIMARY);
        publication.replacementCommitted(1);
        publication.replacementApplied();
        try {
            publication.watermarkAdvanced();
            Assert.fail("expected the watermark before the runtime exchange to be refused");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "before settling its runtime");
        }
    }

    @Test
    public void testTheFullOrderingWalksOnceThroughEveryStage() {
        final LiveViewCheckpointRepairPublication publication = new LiveViewCheckpointRepairPublication();
        Assert.assertNull(publication.getStage());
        Assert.assertFalse(publication.isAtOrAfter(RepairPublicationStage.PLAN));

        publication.plan();
        Assert.assertEquals(RepairPublicationStage.PLAN, publication.getStage());

        publication.candidateReady(RuntimeDisposition.KEEP_PRIMARY);
        Assert.assertTrue(publication.isKeepPrimaryRuntime());
        Assert.assertFalse(publication.isRuntimeSettled());

        publication.replacementCommitted(17);
        Assert.assertEquals(17, publication.getCommittedLvSeqTxn());
        Assert.assertFalse(publication.isReplacementReconciled());

        publication.replacementApplied();
        Assert.assertTrue(publication.isReplacementReconciled());

        publication.timelinePublished();
        publication.runtimePromoted();
        Assert.assertTrue(publication.isRuntimeSettled());

        publication.watermarkAdvanced();
        Assert.assertEquals(
                RepairPublicationStage.CONSUMED_WATERMARK_AND_PURGE_FLOOR_ADVANCED,
                publication.getStage()
        );
        Assert.assertTrue(publication.isAtOrAfter(RepairPublicationStage.PLAN));
    }
}

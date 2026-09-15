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
import io.questdb.cairo.lv.LiveViewCheckpointRecoveryPhase;
import io.questdb.cairo.lv.LiveViewDefinition;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewLifecycleState;
import io.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit coverage for the live-view lifecycle state machine that feeds
 * {@code live_views().view_status}.
 * <p>
 * The {@code creating} and {@code dropping} labels are not observable through the
 * {@code live_views()} catalogue in any deterministic way, so they are locked here at the enum level
 * instead of via a SQL query:
 * <ul>
 *   <li><b>creating</b> - the registry entry is locked but not yet committed, so the view is not in
 *   the registry the catalogue enumerates ({@code LiveViewsFunctionFactory} reads
 *   {@code getLiveViewRegistry().getViews(...)}). {@link LiveViewLifecycleState#derive} cannot even
 *   produce {@code CREATING}, because its caller already holds a committed instance.</li>
 *   <li><b>dropping</b> - {@code CairoEngine.dropLiveView} calls {@code liveViewRegistry.removeView}
 *   before it flips the instance's dropped flag, so the instance leaves the enumerated registry
 *   before it would ever report {@code dropping}.</li>
 * </ul>
 * These tests therefore lock the state-derivation truth table and the exact catalogue label strings
 * (a rename would silently break operator dashboards and tooling that match on them).
 */
public class LiveViewLifecycleStateTest {

    @Test
    public void testARebuildDeferralIsNotABlock() {
        final LiveViewInstance instance = new LiveViewInstance((LiveViewDefinition) null, (TableToken) null, 1, false, -1);
        instance.markCheckpointRebuildDeferred("waits");
        // A view whose rebuild waits for its base's apply has not stopped: it reports active, and
        // nothing that reads a block - the refresh gate, the WAL purge floor - takes it for one.
        Assert.assertEquals(LiveViewCheckpointRecoveryPhase.REBUILD_DEFERRED, instance.getCheckpointRecoveryPhase());
        Assert.assertTrue(instance.isCheckpointRebuildDeferred());
        Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
        Assert.assertEquals(LiveViewLifecycleState.ACTIVE, instance.getLifecycleState());
        Assert.assertEquals("waits", instance.getCheckpointRecoveryReason());

        // A cycle that succeeded settled the debt the deferral was waiting to pay.
        instance.recordRefreshSuccess();
        Assert.assertEquals(LiveViewCheckpointRecoveryPhase.NONE, instance.getCheckpointRecoveryPhase());
        Assert.assertNull(instance.getCheckpointRecoveryReason());

        // A rebuild that ran after the wait and was refused replaces it with a block.
        instance.markCheckpointRebuildDeferred("waits");
        instance.markCheckpointRebuildBlocked("refused");
        Assert.assertEquals(LiveViewCheckpointRecoveryPhase.REBUILD_BLOCKED, instance.getCheckpointRecoveryPhase());
        Assert.assertEquals(LiveViewLifecycleState.INVALID, instance.getLifecycleState());
        // Nothing that ends a deferral lifts a block, and no deferral displaces one.
        instance.clearCheckpointRebuildDeferred();
        instance.recordRefreshSuccess();
        instance.markCheckpointRebuildDeferred("waits");
        Assert.assertEquals(LiveViewCheckpointRecoveryPhase.REBUILD_BLOCKED, instance.getCheckpointRecoveryPhase());
        Assert.assertEquals("refused", instance.getCheckpointRecoveryReason());
    }

    @Test
    public void testAnInvalidatedOrDroppedViewWaitsForNothing() {
        // The invalidation takes the view out of refresh, and the runtime-state free that follows
        // it - under the refresh latch, after any cycle that could still have deferred - ends the
        // wait with it. An invalid view reporting a rebuild that resumes on its own would be wrong
        // twice over.
        final LiveViewInstance invalidated = new LiveViewInstance((LiveViewDefinition) null, (TableToken) null, 1, false, -1);
        invalidated.markCheckpointRebuildDeferred("waits");
        invalidated.markInvalid("boom", 42);
        invalidated.tryFreeRuntimeStateIfInvalid();
        Assert.assertEquals(LiveViewLifecycleState.INVALID, invalidated.getLifecycleState());
        Assert.assertEquals(LiveViewCheckpointRecoveryPhase.NONE, invalidated.getCheckpointRecoveryPhase());
        Assert.assertNull(invalidated.getCheckpointRecoveryReason());

        // The same for a drop, whose close runs under the same latch.
        final LiveViewInstance dropped = new LiveViewInstance((LiveViewDefinition) null, (TableToken) null, 1, false, -1);
        dropped.markCheckpointRebuildDeferred("waits");
        dropped.markAsDropped();
        dropped.tryCloseIfDropped();
        Assert.assertEquals(LiveViewCheckpointRecoveryPhase.NONE, dropped.getCheckpointRecoveryPhase());
        Assert.assertNull(dropped.getCheckpointRecoveryReason());
    }

    @Test
    public void testCatalogueNamesAreStableLowerCase() {
        // The exact strings surfaced by live_views().view_status. Locks all six, including the two
        // transient/internal states that no SQL query can observe.
        Assert.assertEquals("creating", LiveViewLifecycleState.CREATING.catalogueName());
        Assert.assertEquals("active", LiveViewLifecycleState.ACTIVE.catalogueName());
        Assert.assertEquals("seeding", LiveViewLifecycleState.SEEDING.catalogueName());
        Assert.assertEquals("invalid", LiveViewLifecycleState.INVALID.catalogueName());
        Assert.assertEquals("dropping", LiveViewLifecycleState.DROPPING.catalogueName());
        Assert.assertEquals("version_unsupported", LiveViewLifecycleState.VERSION_UNSUPPORTED.catalogueName());
    }

    @Test
    public void testCheckpointRecoveryPhaseNamesAreStable() {
        // The exact strings surfaced by live_views().checkpoint_recovery_phase, and which of them
        // stop the view.
        Assert.assertNull(LiveViewCheckpointRecoveryPhase.name(LiveViewCheckpointRecoveryPhase.NONE));
        Assert.assertEquals("blocked", LiveViewCheckpointRecoveryPhase.name(LiveViewCheckpointRecoveryPhase.BLOCKED));
        Assert.assertEquals("rebuild_blocked", LiveViewCheckpointRecoveryPhase.name(LiveViewCheckpointRecoveryPhase.REBUILD_BLOCKED));
        Assert.assertEquals("rebuild_deferred", LiveViewCheckpointRecoveryPhase.name(LiveViewCheckpointRecoveryPhase.REBUILD_DEFERRED));
        Assert.assertFalse(LiveViewCheckpointRecoveryPhase.isBlocked(LiveViewCheckpointRecoveryPhase.NONE));
        Assert.assertTrue(LiveViewCheckpointRecoveryPhase.isBlocked(LiveViewCheckpointRecoveryPhase.BLOCKED));
        Assert.assertTrue(LiveViewCheckpointRecoveryPhase.isBlocked(LiveViewCheckpointRecoveryPhase.REBUILD_BLOCKED));
        Assert.assertFalse(LiveViewCheckpointRecoveryPhase.isBlocked(LiveViewCheckpointRecoveryPhase.REBUILD_DEFERRED));
    }

    @Test
    public void testDeriveActiveAndSeeding() {
        // Registry-visible, valid, not blocked: the seed signal alone chooses SEEDING vs ACTIVE.
        Assert.assertEquals(LiveViewLifecycleState.ACTIVE, LiveViewLifecycleState.derive(true, false, false, false));
        Assert.assertEquals(LiveViewLifecycleState.SEEDING, LiveViewLifecycleState.derive(true, false, false, true));
    }

    @Test
    public void testDeriveDroppingWhenNotRegistryVisible() {
        // A not-registry-visible (marked-dropped) instance is DROPPING regardless of the other signals.
        // This is the sole producer of DROPPING, hence the authoritative check for the dropping label.
        Assert.assertEquals(LiveViewLifecycleState.DROPPING, LiveViewLifecycleState.derive(false, false, false, false));
        Assert.assertEquals(LiveViewLifecycleState.DROPPING, LiveViewLifecycleState.derive(false, true, false, false));
        Assert.assertEquals(LiveViewLifecycleState.DROPPING, LiveViewLifecycleState.derive(false, false, false, true));
        Assert.assertEquals(LiveViewLifecycleState.DROPPING, LiveViewLifecycleState.derive(false, true, false, true));
        // A blocked view is stopped, not gone: DROPPING still wins over it.
        Assert.assertEquals(LiveViewLifecycleState.DROPPING, LiveViewLifecycleState.derive(false, false, true, false));
        Assert.assertEquals(LiveViewLifecycleState.DROPPING, LiveViewLifecycleState.derive(false, true, true, true));
    }

    @Test
    public void testDeriveInvalidTakesPrecedenceOverSeeding() {
        // A registry-visible, invalid instance is INVALID even if the seed signal is still set.
        Assert.assertEquals(LiveViewLifecycleState.INVALID, LiveViewLifecycleState.derive(true, true, false, false));
        Assert.assertEquals(LiveViewLifecycleState.INVALID, LiveViewLifecycleState.derive(true, true, false, true));
    }

    @Test
    public void testDeriveReportsACheckpointFormatBlockAsInvalid() {
        // The block is not _lv.s.invalid - it is re-derived from the superblock every start - but it
        // stops refresh just the same, so it reports under the status an operator already searches
        // for. checkpoint_recovery_phase is what tells the two apart.
        Assert.assertEquals(LiveViewLifecycleState.INVALID, LiveViewLifecycleState.derive(true, false, true, false));
        // And it outranks the seed signal, exactly as a durable invalidation does.
        Assert.assertEquals(LiveViewLifecycleState.INVALID, LiveViewLifecycleState.derive(true, false, true, true));
    }

    @Test
    public void testInvalidationPayloadIsPublishedBeforeInvalidState() throws Exception {
        final LiveViewInstance instance = new LiveViewInstance((LiveViewDefinition) null, (TableToken) null, 1, false, -1);
        final CountDownLatch reasonCopyStarted = new CountDownLatch(1);
        final CountDownLatch releaseReasonCopy = new CountDownLatch(1);
        final AtomicReference<Throwable> writerError = new AtomicReference<>();
        final CharSequence reason = new CharSequence() {
            @Override
            public char charAt(int index) {
                return "boom".charAt(index);
            }

            @Override
            public int length() {
                reasonCopyStarted.countDown();
                try {
                    if (!releaseReasonCopy.await(10, TimeUnit.SECONDS)) {
                        throw new AssertionError("timed out waiting to release invalidation reason copy");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                return 4;
            }

            @Override
            public CharSequence subSequence(int start, int end) {
                return "boom".subSequence(start, end);
            }

            @Override
            public String toString() {
                return "boom";
            }
        };
        final Thread writer = new Thread(() -> {
            try {
                instance.markInvalid(reason, 42);
            } catch (Throwable th) {
                writerError.set(th);
            }
        });

        writer.start();
        try {
            Assert.assertTrue("writer did not start copying the reason", reasonCopyStarted.await(10, TimeUnit.SECONDS));
            Assert.assertEquals(LiveViewLifecycleState.ACTIVE, instance.getLifecycleState());
            Assert.assertNull(instance.getInvalidationReason());
            Assert.assertEquals(Numbers.LONG_NULL, instance.getStateReader().getInvalidationTimestampUs());
        } finally {
            releaseReasonCopy.countDown();
            writer.join(10_000);
        }

        Assert.assertFalse("writer did not stop", writer.isAlive());
        Assert.assertNull(writerError.get());
        Assert.assertEquals(LiveViewLifecycleState.INVALID, instance.getLifecycleState());
        Assert.assertEquals("boom", instance.getInvalidationReason());
        Assert.assertEquals(42, instance.getStateReader().getInvalidationTimestampUs());
    }
}

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

package io.questdb.test.cutlass.qwp;

import io.questdb.cairo.TableToken;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.qwp.server.egress.DefaultQwpEgressExtension;
import io.questdb.cutlass.qwp.server.egress.QwpEgressExtension;
import io.questdb.cutlass.qwp.server.egress.QwpEgressProcessorState;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.test.AbstractCairoTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Contract tests for the {@link QwpEgressExtension} SPI. Verifies that the
 * OSS default ({@link DefaultQwpEgressExtension}) is a strict no-op and
 * that the engine's set/get pair round-trips a custom extension.
 * <p>
 * The dispatch path through {@code QwpEgressUpgradeProcessor} is covered
 * by the existing QWP integration tests. This class is the unit-level
 * net that fixes the contract of the SPI itself so a future OSS-side
 * change cannot quietly regress, for example, the default
 * {@code hasPendingOutboundWork} return value (which would silently
 * break the lost-WRITE recovery path) or the
 * {@code computeIngestCredits} ceiling (which would silently throttle
 * OSS ingest).
 */
public class QwpEgressExtensionDispatchTest extends AbstractCairoTest {

    @After
    public void restoreDefaultExtension() {
        if (engine != null) {
            engine.setQwpEgressExtension(DefaultQwpEgressExtension.INSTANCE);
        }
    }

    @Test
    public void testDefaultComputeIngestCreditsReturnsMaxValue() {
        // OSS-only ingest must run at full speed when no Enterprise hub is
        // installed: the credits hint trailer is the ceiling.
        Assert.assertEquals(Integer.MAX_VALUE,
                DefaultQwpEgressExtension.INSTANCE.computeIngestCredits(null));
    }

    @Test
    public void testDefaultHandleMessageReturnsFalse() {
        // The contract: returning false tells the OSS processor to log
        // the unknown-kind error and disconnect. Anything else (e.g.
        // accidentally returning true) would silently swallow malformed
        // frames in OSS-only builds.
        Assert.assertFalse(DefaultQwpEgressExtension.INSTANCE.handleMessage(
                null, null, (byte) 0x42, 0L, 0));
    }

    @Test
    public void testDefaultHasPendingOutboundWorkReturnsFalse() {
        // The PISR-vs-PSW pivot point for the lost-WRITE recovery race:
        // OSS-only builds must surface false here so the recv-zero path
        // re-arms READ as it always did, instead of converting into a
        // WRITE arm and starving inbound progress.
        Assert.assertFalse(DefaultQwpEgressExtension.INSTANCE.hasPendingOutboundWork(null));
    }

    @Test
    public void testDefaultOnCacheResetIsNoOp() {
        // No throw for any reset mask.
        DefaultQwpEgressExtension.INSTANCE.onCacheReset(null, null, (byte) 0);
        DefaultQwpEgressExtension.INSTANCE.onCacheReset(null, null, (byte) 0xFF);
        DefaultQwpEgressExtension.INSTANCE.onCacheReset(null, null, (byte) 0x01);
    }

    @Test
    public void testDefaultOnConnectionClosedIsNoOp() {
        DefaultQwpEgressExtension.INSTANCE.onConnectionClosed(null);
    }

    @Test
    public void testDefaultResumeSendIsNoOp() throws Exception {
        DefaultQwpEgressExtension.INSTANCE.resumeSend(null, null);
    }

    @Test
    public void testEngineGetterReturnsDefaultWhenNothingInstalled() {
        Assert.assertSame("engine must report DefaultQwpEgressExtension.INSTANCE when no Enterprise install has happened",
                DefaultQwpEgressExtension.INSTANCE, engine.getQwpEgressExtension());
    }

    @Test
    public void testEngineSetterRoundTripsCustomExtension() {
        RecordingExtension recorder = new RecordingExtension();
        engine.setQwpEgressExtension(recorder);
        Assert.assertSame(recorder, engine.getQwpEgressExtension());

        // Restore and confirm.
        engine.setQwpEgressExtension(DefaultQwpEgressExtension.INSTANCE);
        Assert.assertSame(DefaultQwpEgressExtension.INSTANCE, engine.getQwpEgressExtension());
    }

    @Test
    public void testRecordingExtensionObservesEveryCallback() throws Exception {
        // A recording extension installed on the engine should observe
        // every callback the OSS processor would route to it via the
        // SPI. Calling the callbacks directly on the engine-returned
        // reference proves the round-trip is the same instance the
        // processor would dispatch into.
        RecordingExtension recorder = new RecordingExtension();
        engine.setQwpEgressExtension(recorder);

        QwpEgressExtension installed = engine.getQwpEgressExtension();
        installed.handleMessage(null, null, (byte) 0x10, 0L, 0);
        installed.hasPendingOutboundWork(null);
        installed.onCacheReset(null, null, (byte) 0);
        installed.onConnectionClosed(null);
        installed.resumeSend(null, null);
        installed.computeIngestCredits(null);

        Assert.assertEquals(1, recorder.handleMessageCount.get());
        Assert.assertEquals(1, recorder.hasPendingOutboundWorkCount.get());
        Assert.assertEquals(1, recorder.onCacheResetCount.get());
        Assert.assertEquals(1, recorder.onConnectionClosedCount.get());
        Assert.assertEquals(1, recorder.resumeSendCount.get());
        Assert.assertEquals(1, recorder.computeIngestCreditsCount.get());
    }

    private static final class RecordingExtension implements QwpEgressExtension {
        final AtomicInteger computeIngestCreditsCount = new AtomicInteger();
        final AtomicInteger handleMessageCount = new AtomicInteger();
        final AtomicInteger hasPendingOutboundWorkCount = new AtomicInteger();
        final AtomicInteger onCacheResetCount = new AtomicInteger();
        final AtomicInteger onConnectionClosedCount = new AtomicInteger();
        final AtomicInteger resumeSendCount = new AtomicInteger();

        @Override
        public int computeIngestCredits(TableToken table) {
            computeIngestCreditsCount.incrementAndGet();
            return 42;
        }

        @Override
        public boolean handleMessage(
                HttpConnectionContext context,
                QwpEgressProcessorState state,
                byte msgKind,
                long payload,
                int length
        ) throws PeerIsSlowToReadException, PeerDisconnectedException, ServerDisconnectException {
            handleMessageCount.incrementAndGet();
            return true;
        }

        @Override
        public boolean hasPendingOutboundWork(HttpConnectionContext context) {
            hasPendingOutboundWorkCount.incrementAndGet();
            return false;
        }

        @Override
        public void onCacheReset(HttpConnectionContext context, QwpEgressProcessorState state, byte mask) {
            onCacheResetCount.incrementAndGet();
        }

        @Override
        public void onConnectionClosed(HttpConnectionContext context) {
            onConnectionClosedCount.incrementAndGet();
        }

        @Override
        public void resumeSend(
                HttpConnectionContext context,
                QwpEgressProcessorState state
        ) throws PeerIsSlowToReadException, PeerDisconnectedException, ServerDisconnectException {
            resumeSendCount.incrementAndGet();
        }
    }
}

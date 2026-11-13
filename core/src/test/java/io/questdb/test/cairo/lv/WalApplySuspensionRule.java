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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.test.tools.LogCapture;
import org.jetbrains.annotations.Nullable;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Fails a test during which a WAL apply suspended a table. A suspension does not stay set: an
 * operator's RESUME, a DROP, or the next inline apply of a live view's table clears it, often before
 * the test ends. So the rule reads the CRITICAL line ApplyWal2TableJob logs before it suspends a
 * table, through one {@link LogCapture} that spans the test's {@code @Before} and {@code @After}
 * methods.
 * <p>
 * The console writer delivers records in the order they were logged, after a delay nothing bounds:
 * a stalled stdout holds them back for seconds. So the rule bounds its window with markers it logs
 * itself. A line an earlier test logged arrives ahead of the start marker, and the rule skips it.
 * Every line the test logs arrives ahead of the end marker, which the rule waits for as
 * {@link LogCapture#stop()} does. A wait that runs out only leaves the rule less to read: a slow
 * logging worker never fails a test.
 * <p>
 * The console writer holds one interceptor, so a test under the rule creates, starts and stops no
 * LogCapture of its own: it reads its log lines through {@link #getCapture()}. The rule fails a test
 * that displaced its capture, or restarted it, which empties it.
 */
class WalApplySuspensionRule implements TestRule {
    private static final Log LOG = LogFactory.getLog(WalApplySuspensionRule.class);
    private static final String LOST_INTERCEPTOR_MESSAGE = "WalApplySuspensionRule lost the console log interceptor: "
            + "the test created, started or stopped a LogCapture; check log lines through WalApplySuspensionRule.getCapture()";
    private static final String LOST_WINDOW_START_MESSAGE = "WalApplySuspensionRule lost the start of its window: "
            + "the test restarted getCapture(), which clears it";
    // ApplyWal2TableJob.handleWalApplyFailure logs it before it suspends the table.
    private static final String SUSPENSION_LINE = "job failed, table suspended";
    private static final AtomicLong WINDOW_SEQ = new AtomicLong();
    private final long endWaitMs;
    private final long startWaitMs;
    private volatile LogCapture capture;

    public WalApplySuspensionRule() {
        this(LogCapture.STOP_DRAIN_TIMEOUT_MS, LogCapture.STOP_DRAIN_TIMEOUT_MS);
    }

    // WalApplySuspensionRuleTest's: a short start wait lets a case hold the logging worker across the
    // rule's start, and a short end wait lets one hold it past the rule's end.
    WalApplySuspensionRule(long startWaitMs, long endWaitMs) {
        this.startWaitMs = startWaitMs;
        this.endWaitMs = endWaitMs;
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                final LogCapture capture = new LogCapture();
                capture.start();
                final long window = WINDOW_SEQ.incrementAndGet();
                final String startMarker = "WalApplySuspensionRule window " + window + " opens";
                final String endMarker = "WalApplySuspensionRule window " + window + " closes";
                LOG.advisory().$(startMarker).$();
                // The window starts at the marker's position, whenever it arrives; the wait keeps an
                // earlier test's backlog out of the end wait's budget.
                capture.drain(startWaitMs);
                WalApplySuspensionRule.this.capture = capture;
                Throwable failure = null;
                try {
                    base.evaluate();
                } catch (Throwable th) {
                    failure = th;
                } finally {
                    WalApplySuspensionRule.this.capture = null;
                }
                if (!capture.isInstalled()) {
                    // No record can reach this capture any more, and its stop() would take the
                    // interceptor from the capture that displaced it.
                    throw withSuppressed(new AssertionError(LOST_INTERCEPTOR_MESSAGE), failure);
                }
                LOG.advisory().$(endMarker).$();
                onEndMarkerLogged();
                capture.stop(endWaitMs);
                if (capture.countOccurrences(startMarker) == 0 && capture.countOccurrences(endMarker) > 0) {
                    // The start marker was logged before the test ran, and the capture held the
                    // interceptor throughout: only start() on it can have cleared the marker.
                    throw withSuppressed(new AssertionError(LOST_WINDOW_START_MESSAGE), failure);
                }
                try {
                    capture.assertNotLoggedAfter(startMarker, SUSPENSION_LINE);
                } catch (AssertionError e) {
                    throw withSuppressed(e, failure);
                }
                if (failure != null) {
                    throw failure;
                }
            }
        };
    }

    /**
     * The capture of the test the rule runs, null outside it. The test reads its log lines through
     * it and must not start or stop it.
     */
    LogCapture getCapture() {
        return capture;
    }

    // A test hook: the rule calls it right after it logs its end marker, ahead of the end wait.
    // WalApplySuspensionRuleTest releases a logging worker it parked here, so the worker delivers the
    // test's lines inside the end wait, with no timer deciding when.
    void onEndMarkerLogged() {
    }

    private static AssertionError withSuppressed(AssertionError error, @Nullable Throwable failure) {
        if (failure != null) {
            error.addSuppressed(failure);
        }
        return error;
    }
}

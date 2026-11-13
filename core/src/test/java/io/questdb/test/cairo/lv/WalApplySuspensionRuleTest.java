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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.log.Log;
import io.questdb.log.LogConsoleWriter;
import io.questdb.log.LogFactory;
import io.questdb.log.LogWriter;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjHashSet;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import org.junit.Assert;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * {@link WalApplySuspensionRule}: what it reports, what it passes, and how it reads a log the logging
 * worker delivers late. Each case runs a statement under a rule of its own. The stall cases park the
 * logging worker inside the console writer's interceptor, as io.questdb.test.log.LogCaptureTest
 * does, so every line logged from then on waits in the ring until the case lets the worker go.
 * <p>
 * Also runAnchoredRemovalFuzz's check of the live view lines of a run
 * ({@link LiveViewFuzzTest#assertNoLiveViewCriticalLineAfter}), which reads the rule's capture.
 */
public class WalApplySuspensionRuleTest extends AbstractLiveViewTest {
    // A suspension line an earlier test logged, and one the test under the rule logged, in the form
    // ApplyWal2TableJob.handleWalApplyFailure gives it.
    private static final String EARLIER_SUSPENSION_LINE = "job failed, table suspended [table=earlier~1, seqTxn=7, error=a line of an earlier test]";
    private static final String EARLIER_SUSPENSION_TABLE = "table=earlier~1";
    private static final String LATE_SUSPENSION_LINE = "job failed, table suspended [table=late~1, seqTxn=3, error=a line of the test]";
    private static final String LIVE_VIEW_CRITICAL_LINE = "a CRITICAL line live view code logged during the run";
    // A logger of io.questdb.cairo.lv: runAnchoredRemovalFuzz's check reads its CRITICAL lines.
    private static final Log LIVE_VIEW_LOG = LogFactory.getLog(LiveViewRefreshJob.class);
    private static final Log LOG = LogFactory.getLog(WalApplySuspensionRuleTest.class);
    private static final String LOST_INTERCEPTOR_MESSAGE = "WalApplySuspensionRule lost the console log interceptor: "
            + "the test created, started or stopped a LogCapture; check log lines through WalApplySuspensionRule.getCapture()";
    private static final String LOST_WINDOW_START_MESSAGE = "WalApplySuspensionRule lost the start of its window: "
            + "the test restarted getCapture(), which clears it";
    private static final String RUN_MARKER = "WalApplySuspensionRuleTest anchored removal run starts";
    // The rule's waits in a stall case: each one runs out against a parked worker, so a short one
    // keeps the case fast.
    private static final long STALLED_WAIT_MS = 200;
    private static final String SUSPENSION_LINE = "job failed, table suspended";

    @Test
    public void testAnchoredRemovalCheckReportsALineAfterTheArmRunMarker() throws Exception {
        // runAnchoredRemovalFuzz reads a run's live view lines from the marker
        // logAnchoredRemovalRunMarker() logs on. The check passes a capture that lacks its marker, so
        // that marker must reach the capture, or the check reads nothing.
        assertMemoryLeak(() -> {
            final LogCapture capture = new LogCapture();
            capture.start();
            try {
                final String runMarker = LiveViewFuzzTest.logAnchoredRemovalRunMarker();
                LIVE_VIEW_LOG.critical().$(LIVE_VIEW_CRITICAL_LINE).$();
                final AssertionError error = Assert.assertThrows(
                        AssertionError.class,
                        () -> LiveViewFuzzTest.assertNoLiveViewCriticalLineAfter(capture, runMarker)
                );
                assertReportsLine(error, " C i.q.c.l.", " C i.q.c.l.LiveViewRefreshJob " + LIVE_VIEW_CRITICAL_LINE);
            } finally {
                capture.stop();
            }
        });
    }

    @Test
    public void testAnchoredRemovalCheckSkipsLinesAheadOfItsRunMarker() throws Exception {
        // The rule's capture spans the whole test: the live view lines of setUp and of earlier runs
        // sit ahead of a run's marker, and are not the run's.
        assertMemoryLeak(() -> {
            final LogCapture capture = new LogCapture();
            capture.start();
            try {
                LIVE_VIEW_LOG.critical().$(LIVE_VIEW_CRITICAL_LINE).$();
                LOG.advisory().$(RUN_MARKER).$();
                LiveViewFuzzTest.assertNoLiveViewCriticalLineAfter(capture, RUN_MARKER);
                Assert.assertEquals("the line ahead of the marker reached the capture", 1, capture.countOccurrences(LIVE_VIEW_CRITICAL_LINE));
            } finally {
                capture.stop();
            }
        });
    }

    @Test
    public void testAnchoredRemovalCheckWaitsOutAStalledLogWorker() throws Exception {
        // The check waits for the run's lines as LogCapture.stop() does. The worker stays parked until
        // the wait has outlived drain()'s bound, and only then delivers a CRITICAL live view line the
        // run logged: the check must still see it. The capture's fake clock jumps past that bound, so
        // no real time decides when the worker goes.
        assertMemoryLeak(() -> {
            final CountDownLatch release = parkTheLoggingWorker();
            try {
                final LogCapture capture = new LogCapture();
                LOG.advisory().$(RUN_MARKER).$();
                LIVE_VIEW_LOG.critical().$(LIVE_VIEW_CRITICAL_LINE).$();
                final long[] nowMs = {0};
                capture.setClockForTest(() -> nowMs[0]);
                capture.setSleeperForTest(ms -> {
                    if (nowMs[0] < LogCapture.DRAIN_TIMEOUT_MS) {
                        // drain()'s bound passes with the worker still parked
                        nowMs[0] = LogCapture.DRAIN_TIMEOUT_MS;
                    } else {
                        release.countDown();
                        Os.sleep(ms);
                        nowMs[0] += ms;
                    }
                });
                final AssertionError error = Assert.assertThrows(
                        AssertionError.class,
                        () -> LiveViewFuzzTest.assertNoLiveViewCriticalLineAfter(capture, RUN_MARKER)
                );
                assertReportsLine(error, " C i.q.c.l.", " C i.q.c.l.LiveViewRefreshJob " + LIVE_VIEW_CRITICAL_LINE);
            } finally {
                release.countDown();
                consoleWriter().setInterceptor(null);
            }
        });
    }

    @Test
    public void testChecksNothingWhenTheWorkerMissesBothWaits() throws Throwable {
        // The worker stays parked from before the rule's start to after its end: neither marker
        // arrives, and the rule checks nothing rather than fail the test for a slow worker. A
        // suspension line still undelivered when the end wait runs out is the documented bound, as
        // for LogCapture.stop().
        final CountDownLatch release = parkTheLoggingWorker();
        try {
            final WalApplySuspensionRule rule = new WalApplySuspensionRule(STALLED_WAIT_MS, STALLED_WAIT_MS);
            final LogCapture[] testCapture = new LogCapture[1];
            rule.apply(statementOf(() -> assertMemoryLeak(() -> {
                LOG.critical().$(LATE_SUSPENSION_LINE).$();
                testCapture[0] = rule.getCapture();
            })), Description.EMPTY).evaluate();
            release.countDown();
            Assert.assertEquals(0, testCapture[0].countOccurrences("WalApplySuspensionRule window"));
            Assert.assertFalse("the end wait removed the interceptor", testCapture[0].isInstalled());
        } finally {
            release.countDown();
            consoleWriter().setInterceptor(null);
        }
    }

    @Test
    public void testFailsATestThatLeavesALogCaptureOfItsOwnRunning() throws Throwable {
        // The test's own capture holds the console interceptor when the test ends. The rule reports
        // the test and leaves that capture alone: a stop() of the rule's capture would wait out its
        // bound for a sentinel only the test's capture receives, then take the interceptor away.
        final LogCapture[] testCapture = new LogCapture[1];
        try {
            final AssertionError error = assertRuleThrows(new WalApplySuspensionRule(), () -> assertMemoryLeak(() -> {
                testCapture[0] = new LogCapture();
                testCapture[0].start();
            }));
            Assert.assertEquals(LOST_INTERCEPTOR_MESSAGE, error.getMessage());
            Assert.assertEquals(0, error.getSuppressed().length);
            Assert.assertTrue("the rule must leave the test's capture installed", testCapture[0].isInstalled());
        } finally {
            if (testCapture[0] != null) {
                testCapture[0].stop();
            }
        }
    }

    @Test
    public void testFailsATestThatRestartsTheRuleCapture() throws Throwable {
        // start() clears the capture, the start marker with it: the rule cannot tell where the
        // test's lines begin.
        final WalApplySuspensionRule rule = new WalApplySuspensionRule();
        final AssertionError error = assertRuleThrows(rule, () -> assertMemoryLeak(() -> rule.getCapture().start()));
        Assert.assertEquals(LOST_WINDOW_START_MESSAGE, error.getMessage());
        Assert.assertEquals(0, error.getSuppressed().length);
    }

    @Test
    public void testFailsATestThatRestartsTheRuleCaptureAheadOfItsFailure() throws Throwable {
        final WalApplySuspensionRule rule = new WalApplySuspensionRule();
        final AssertionError symptom = new AssertionError("symptom");
        final AssertionError error = assertRuleThrows(rule, () -> assertMemoryLeak(() -> {
            rule.getCapture().start();
            throw symptom;
        }));
        Assert.assertEquals(LOST_WINDOW_START_MESSAGE, error.getMessage());
        assertSuppressedOnly(symptom, error);
    }

    @Test
    public void testFailsATestThatRunsALogCaptureOfItsOwn() throws Throwable {
        // The console writer holds one interceptor: a test's own LogCapture takes it from the rule's,
        // whose capture then misses the suspension that follows. The rule reads which capture holds
        // the interceptor, so no wait decides it.
        final String[] baseDir = new String[1];
        final AtomicBoolean isFaultArmed = new AtomicBoolean();
        final AssertionError error = assertRuleThrows(new WalApplySuspensionRule(), () -> assertMemoryLeak(dayTwoColumnOpenFault(baseDir, isFaultArmed), () -> {
            final LogCapture testCapture = new LogCapture();
            testCapture.start();
            testCapture.stop();
            suspendBaseOnItsDayTwoApply(baseDir, isFaultArmed);
            resumeBaseAndAssertItCaughtUp();
        }));
        Assert.assertEquals(LOST_INTERCEPTOR_MESSAGE, error.getMessage());
        Assert.assertEquals(0, error.getSuppressed().length);
    }

    @Test
    public void testFailsATestThatRunsALogCaptureOfItsOwnAheadOfItsFailure() throws Throwable {
        final AssertionError symptom = new AssertionError("symptom");
        final AssertionError error = assertRuleThrows(new WalApplySuspensionRule(), () -> assertMemoryLeak(() -> {
            final LogCapture testCapture = new LogCapture();
            testCapture.start();
            testCapture.stop();
            throw symptom;
        }));
        Assert.assertEquals(LOST_INTERCEPTOR_MESSAGE, error.getMessage());
        assertSuppressedOnly(symptom, error);
    }

    @Test
    public void testFailsATestThatStopsTheRuleCapture() throws Throwable {
        final String[] baseDir = new String[1];
        final AtomicBoolean isFaultArmed = new AtomicBoolean();
        final WalApplySuspensionRule rule = new WalApplySuspensionRule();
        final AssertionError error = assertRuleThrows(rule, () -> assertMemoryLeak(dayTwoColumnOpenFault(baseDir, isFaultArmed), () -> {
            rule.getCapture().stop();
            suspendBaseOnItsDayTwoApply(baseDir, isFaultArmed);
            resumeBaseAndAssertItCaughtUp();
        }));
        Assert.assertEquals(LOST_INTERCEPTOR_MESSAGE, error.getMessage());
        Assert.assertEquals(0, error.getSuppressed().length);
    }

    @Test
    public void testPassesACleanTest() throws Throwable {
        // The rule publishes its capture only while the test runs.
        final WalApplySuspensionRule rule = new WalApplySuspensionRule();
        Assert.assertNull(rule.getCapture());
        rule.apply(statementOf(() -> assertMemoryLeak(() -> {
            Assert.assertNotNull(rule.getCapture());
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('2026-04-01T00:00:00.000000Z', 1), ('2026-04-02T00:00:00.000000Z', 2)");
            drainWalQueue();
            assertQuery("SELECT count() FROM base").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");
        })), Description.EMPTY).evaluate();
        Assert.assertNull(rule.getCapture());
    }

    @Test
    public void testPassesOperatorSuspensions() throws Throwable {
        // An operator's SUSPEND WAL, of a table or of a live view, logs no apply failure, nor does
        // a DROP of the suspended table or view, or a drive after it.
        new WalApplySuspensionRule().apply(statementOf(() -> assertMemoryLeak(() -> {
            setCurrentMicros(0);
            execute("CREATE TABLE t (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES ('2026-04-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            createCountingView();
            final TableToken tToken = engine.verifyTableName("t");
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES ('2026-04-01T00:00:00.000000Z', 1)");
                drainWalQueue();
                driveRefreshToQuiescence(job);

                execute("ALTER TABLE t SUSPEND WAL");
                Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tToken));
                execute("DROP TABLE t");
                drainWalQueue();
                driveRefreshToQuiescence(job);

                execute("ALTER LIVE VIEW lv SUSPEND WAL WITH 'DISK FULL', 'manual halt'");
                Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(lvToken));
                execute("INSERT INTO base VALUES ('2026-04-01T01:00:00.000000Z', 2)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                execute("DROP LIVE VIEW lv");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }
        })), Description.EMPTY).evaluate();
    }

    @Test
    public void testPublicRuleReportsASuspensionTheWorkerDeliversPastTheDrainBound() throws Throwable {
        // The fuzz classes' rule, from the public constructor, waits for the test's lines as
        // LogCapture.stop() does, well past drain()'s bound. The worker stays parked from before the
        // rule's start until the rule's end wait has outlived drain()'s bound, and only then delivers
        // the test's suspension line: the rule must still report it. The capture's fake clock runs the
        // rule's waits, so no real time decides when the worker goes, and the 30 s start wait runs out
        // at once.
        final CountDownLatch release = parkTheLoggingWorker();
        try {
            final long[] nowMs = {0};
            final long[] releaseAfterMs = {Long.MAX_VALUE};
            final WalApplySuspensionRule rule = new WalApplySuspensionRule() {
                @Override
                void onCaptureCreated(LogCapture capture) {
                    capture.setClockForTest(() -> nowMs[0]);
                    capture.setSleeperForTest(ms -> {
                        if (nowMs[0] > releaseAfterMs[0]) {
                            release.countDown();
                            Os.sleep(ms);
                        }
                        nowMs[0] += ms;
                    });
                }

                @Override
                void onEndMarkerLogged() {
                    // the end wait starts now: the worker goes once it has outlived drain()'s bound
                    releaseAfterMs[0] = nowMs[0] + LogCapture.DRAIN_TIMEOUT_MS;
                }
            };
            final AssertionError error = assertRuleThrows(rule, () -> assertMemoryLeak(() -> LOG.critical().$(LATE_SUSPENSION_LINE).$()));
            assertReportsLine(error, SUSPENSION_LINE, " C i.q.t.c.l.WalApplySuspensionRuleTest " + LATE_SUSPENSION_LINE);
            Assert.assertEquals(0, error.getSuppressed().length);
        } finally {
            release.countDown();
            consoleWriter().setInterceptor(null);
        }
    }

    @Test
    public void testReportsABaseSuspensionAResumeHealed() throws Throwable {
        final String[] baseDir = new String[1];
        final AtomicBoolean isFaultArmed = new AtomicBoolean();
        final AssertionError error = assertRuleThrows(new WalApplySuspensionRule(), () -> assertMemoryLeak(dayTwoColumnOpenFault(baseDir, isFaultArmed), () -> {
            suspendBaseOnItsDayTwoApply(baseDir, isFaultArmed);
            resumeBaseAndAssertItCaughtUp();
        }));
        assertReportsDayTwoSuspension(error, baseDir[0]);
        Assert.assertEquals(0, error.getSuppressed().length);
    }

    @Test
    public void testReportsALiveViewSuspensionTheSameDriveHealed() throws Throwable {
        // A live view's table heals on its next inline apply, and here that apply comes in the same
        // drive: a late row's repair lands the block whose forward flush failed. No poll of the
        // suspended flag after the drive sees the suspension; its log line stays.
        final String[] lvDir = new String[1];
        final AtomicBoolean isFaultArmed = new AtomicBoolean();
        final AssertionError error = assertRuleThrows(new WalApplySuspensionRule(), () -> assertMemoryLeak(dayTwoColumnOpenFault(lvDir, isFaultArmed), () -> {
            setCurrentMicros(0);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            createCountingView();
            final TableToken lvToken = engine.verifyTableName("lv");
            lvDir[0] = lvToken.getDirName();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES ('2026-04-01T00:00:00.000000Z', 1), ('2026-04-01T02:00:00.000000Z', 3)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base VALUES ('2026-04-02T00:00:00.000000Z', 2)");
                execute("INSERT INTO base VALUES ('2026-04-01T01:00:00.000000Z', 4)");
                drainWalQueue();
                isFaultArmed.set(true);
                driveRefreshToQuiescence(job);
                Assert.assertFalse("the fault must have fired", isFaultArmed.get());
                Assert.assertFalse("the drive's repair lands the block the failed flush left", engine.getTableSequencerAPI().isSuspended(lvToken));
            }
            drainWalQueue();
            assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize()
                    .returns("""
                            ts\tx\trn
                            2026-04-01T00:00:00.000000Z\t1\t1
                            2026-04-01T01:00:00.000000Z\t4\t2
                            2026-04-01T02:00:00.000000Z\t3\t3
                            2026-04-02T00:00:00.000000Z\t2\t4
                            """);
        }));
        assertReportsDayTwoSuspension(error, lvDir[0]);
        Assert.assertEquals(0, error.getSuppressed().length);
    }

    @Test
    public void testReportsASuspensionAheadOfTheTestFailure() throws Throwable {
        // The suspension is what made the test fail: the rule reports it, with the failure
        // suppressed under it.
        final String[] baseDir = new String[1];
        final AtomicBoolean isFaultArmed = new AtomicBoolean();
        final AssertionError symptom = new AssertionError("symptom");
        final AssertionError error = assertRuleThrows(new WalApplySuspensionRule(), () -> assertMemoryLeak(dayTwoColumnOpenFault(baseDir, isFaultArmed), () -> {
            suspendBaseOnItsDayTwoApply(baseDir, isFaultArmed);
            resumeBaseAndAssertItCaughtUp();
            throw symptom;
        }));
        assertReportsDayTwoSuspension(error, baseDir[0]);
        assertSuppressedOnly(symptom, error);
    }

    @Test
    public void testReportsASuspensionOfATableTheTestDropped() throws Throwable {
        final String[] baseDir = new String[1];
        final AtomicBoolean isFaultArmed = new AtomicBoolean();
        final AssertionError error = assertRuleThrows(new WalApplySuspensionRule(), () -> assertMemoryLeak(dayTwoColumnOpenFault(baseDir, isFaultArmed), () -> {
            suspendBaseOnItsDayTwoApply(baseDir, isFaultArmed);
            execute("DROP TABLE base");
            drainWalQueue();
        }));
        assertReportsDayTwoSuspension(error, baseDir[0]);
        Assert.assertEquals(0, error.getSuppressed().length);
    }

    @Test
    public void testReportsASuspensionTheWorkerDeliversInTheEndWait() throws Throwable {
        // The worker stays parked from before the rule's start until the rule has logged its end
        // marker, with an earlier test's suspension line queued ahead of everything. The end wait
        // receives the whole backlog, and the rule reports the test's line, not the earlier one.
        final CountDownLatch release = parkTheLoggingWorker();
        try {
            LOG.critical().$(EARLIER_SUSPENSION_LINE).$();
            final WalApplySuspensionRule rule = new WalApplySuspensionRule(STALLED_WAIT_MS, LogCapture.STOP_DRAIN_TIMEOUT_MS) {
                @Override
                void onEndMarkerLogged() {
                    release.countDown();
                }
            };
            final AssertionError error = assertRuleThrows(rule, () -> assertMemoryLeak(() -> LOG.critical().$(LATE_SUSPENSION_LINE).$()));
            assertReportsLine(error, SUSPENSION_LINE, " C i.q.t.c.l.WalApplySuspensionRuleTest " + LATE_SUSPENSION_LINE);
            Assert.assertEquals(0, error.getSuppressed().length);
        } finally {
            release.countDown();
            consoleWriter().setInterceptor(null);
        }
    }

    @Test
    public void testRethrowsTheTestFailureWithoutASuspension() throws Throwable {
        final AssertionError symptom = new AssertionError("symptom");
        final AssertionError error = assertRuleThrows(new WalApplySuspensionRule(), () -> assertMemoryLeak(() -> {
            throw symptom;
        }));
        Assert.assertSame(symptom, error);
        Assert.assertEquals(0, error.getSuppressed().length);
    }

    @Test
    public void testSkipsAnEarlierTestLineThatArrivesAfterStartClears() throws Throwable {
        // The worker stays parked across the rule's start, past start()'s drain, so a line an
        // earlier test logged lands in the capture after start() cleared it. It lands ahead of the
        // start marker, and the rule skips it.
        final CountDownLatch release = parkTheLoggingWorker();
        try {
            LOG.critical().$(EARLIER_SUSPENSION_LINE).$();
            final WalApplySuspensionRule rule = new WalApplySuspensionRule(STALLED_WAIT_MS, LogCapture.STOP_DRAIN_TIMEOUT_MS);
            rule.apply(statementOf(() -> assertMemoryLeak(() -> {
                release.countDown();
                final LogCapture capture = rule.getCapture();
                capture.drain(LogCapture.STOP_DRAIN_TIMEOUT_MS);
                Assert.assertEquals("the earlier line reached the capture", 1, capture.countOccurrences(EARLIER_SUSPENSION_TABLE));
            })), Description.EMPTY).evaluate();
        } finally {
            release.countDown();
            consoleWriter().setInterceptor(null);
        }
    }

    @Test
    public void testSkipsAnEarlierTestLineTheWorkerDeliversInTheEndWait() throws Throwable {
        // testReportsASuspensionTheWorkerDeliversInTheEndWait without the test's line: the rule
        // passes, and its capture holds the earlier line, which its position ahead of the start
        // marker leaves out of the window.
        final CountDownLatch release = parkTheLoggingWorker();
        try {
            LOG.critical().$(EARLIER_SUSPENSION_LINE).$();
            final WalApplySuspensionRule rule = new WalApplySuspensionRule(STALLED_WAIT_MS, LogCapture.STOP_DRAIN_TIMEOUT_MS) {
                @Override
                void onEndMarkerLogged() {
                    release.countDown();
                }
            };
            final LogCapture[] testCapture = new LogCapture[1];
            rule.apply(statementOf(() -> assertMemoryLeak(() -> testCapture[0] = rule.getCapture())), Description.EMPTY).evaluate();
            Assert.assertEquals(1, testCapture[0].countOccurrences(EARLIER_SUSPENSION_TABLE));
        } finally {
            release.countDown();
            consoleWriter().setInterceptor(null);
        }
    }

    // The line ApplyWal2TableJob logs as it suspends a table whose day-two apply failed on
    // dayTwoColumnOpenFault, at seqTxn 2: the table's first commit applied, its second failed.
    private static void assertReportsDayTwoSuspension(AssertionError error, String tableDir) {
        assertReportsLine(
                error,
                SUSPENSION_LINE,
                " C i.q.c.w.ApplyWal2TableJob job failed, table suspended [table=" + tableDir
                        + ", seqTxn=2, error=io.questdb.cairo.CairoException: [0] bulk update failed and will be rolled back"
        );
    }

    // The checks quote the whole line of a match: the log timestamp, which no test can predict, then
    // the level, the logger and the text, up to the '\n' of the record's "\r\n" (Misc.EOL).
    private static void assertReportsLine(AssertionError error, String message, String lineAfterTimestamp) {
        final String prefix = "Message '" + message + "' was logged: ";
        Assert.assertTrue(
                "expected: " + prefix + "<timestamp>" + lineAfterTimestamp + "\\r, actual: " + error.getMessage(),
                Pattern.matches(Pattern.quote(prefix) + "\\S+" + Pattern.quote(lineAfterTimestamp + "\r"), error.getMessage())
        );
    }

    private static AssertionError assertRuleThrows(WalApplySuspensionRule rule, ThrowingRunnable test) {
        return Assert.assertThrows(AssertionError.class, () -> rule.apply(statementOf(test), Description.EMPTY).evaluate());
    }

    private static void assertSuppressedOnly(AssertionError suppressed, AssertionError error) {
        Assert.assertEquals(1, error.getSuppressed().length);
        Assert.assertSame(suppressed, error.getSuppressed()[0]);
    }

    private static LogConsoleWriter consoleWriter() {
        final ObjHashSet<LogWriter> jobs = LogFactory.getInstance().getJobs();
        for (int i = 0, n = jobs.size(); i < n; i++) {
            if (jobs.get(i) instanceof LogConsoleWriter consoleWriter) {
                return consoleWriter;
            }
        }
        throw new AssertionError("no LogConsoleWriter configured");
    }

    private static void createCountingView() throws Exception {
        execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS SELECT ts, x, "
                + "count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE x > 0");
    }

    // LiveViewTest.testSuspendedLiveViewCanBeResumed's fault: the open of x.d in the table's
    // 2026-04-02 partition fails once. The writer opens it on an O3 open-column task, which logs the
    // error and fails the apply with "bulk update failed and will be rolled back", errno 0.
    private static FilesFacade dayTwoColumnOpenFault(String[] tableDir, AtomicBoolean isArmed) {
        return new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (isArmed.get()
                        && tableDir[0] != null
                        && Utf8s.endsWithAscii(name, "x.d")
                        && Utf8s.containsAscii(name, tableDir[0])
                        && Utf8s.containsAscii(name, "2026-04-02")
                        && isArmed.compareAndSet(true, false)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
    }

    // Parks the logging worker inside the console writer's interceptor: every record logged from
    // now on waits in the ring until the returned latch opens. A capture created or started
    // meanwhile installs its own interceptor, which the worker hands every later record to.
    private static CountDownLatch parkTheLoggingWorker() throws InterruptedException {
        final CountDownLatch parked = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        consoleWriter().setInterceptor(_ -> {
            parked.countDown();
            try {
                release.await();
            } catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }
        });
        LOG.advisory().$("WalApplySuspensionRuleTest parks the logging worker").$();
        Assert.assertTrue(
                "test setup: the logging worker must be parked inside the interceptor",
                parked.await(30, TimeUnit.SECONDS)
        );
        return release;
    }

    private static Statement statementOf(ThrowingRunnable test) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                test.run();
            }
        };
    }

    // Creates base, applies its first commit and fails the apply of its second, on day two:
    // ApplyWal2TableJob suspends base at seqTxn 2.
    private static void suspendBaseOnItsDayTwoApply(String[] baseDir, AtomicBoolean isFaultArmed) throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        final TableToken baseToken = engine.verifyTableName("base");
        baseDir[0] = baseToken.getDirName();
        execute("INSERT INTO base VALUES ('2026-04-01T00:00:00.000000Z', 1)");
        drainWalQueue();
        isFaultArmed.set(true);
        execute("INSERT INTO base VALUES ('2026-04-02T00:00:00.000000Z', 2)");
        drainWalQueue();
        Assert.assertFalse("the fault must have fired", isFaultArmed.get());
        Assert.assertTrue("the failed apply suspends base", engine.getTableSequencerAPI().isSuspended(baseToken));
    }

    private void resumeBaseAndAssertItCaughtUp() throws Exception {
        execute("ALTER TABLE base RESUME WAL");
        drainWalQueue();
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("base")));
        assertQuery("SELECT count() FROM base").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");
    }
}

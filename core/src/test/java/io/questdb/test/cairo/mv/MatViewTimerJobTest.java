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

package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.cairo.mv.MatViewTimerTask;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.ConcurrentQueue;
import io.questdb.mp.Queue;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@RunWith(Parameterized.class)
public class MatViewTimerJobTest extends AbstractCairoTest {
    private final TestTimestampType timestampType;

    public MatViewTimerJobTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestTimestampType> testParams() {
        return List.of(TestTimestampType.MICRO, TestTimestampType.NANO);
    }

    @Test
    public void testAddAfterDropRemoveBeforeGraphRemoval() throws Exception {
        testRegistrationAfterDrop(false, false);
    }

    @Test
    public void testDroppedStateDoesNotRegisterTimers() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable(engine, sqlExecutionContext);
            execute("CREATE MATERIALIZED VIEW mv REFRESH EVERY 1m DEFERRED START '1970-01-02' AS SELECT ts, last(price) price FROM base SAMPLE BY 1h");
            final TableToken token = engine.verifyTableName("mv");
            final MatViewState state = engine.getMatViewStateStore().getViewState(token);
            state.markAsDropped();
            final MatViewTimerJob timer = new MatViewTimerJob(engine);
            timer.run();
            Assert.assertEquals(0, timer.getTimerCount());
            engine.getMatViewTimerQueue().enqueue(new MatViewTimerTask().ofUpdate(token));
            timer.run();
            Assert.assertEquals(0, timer.getTimerCount());
            Assert.assertEquals(0, state.getRegisteredTimerCount());
        });
    }

    @Test
    public void testExpiryDiscardsDroppedStateTimers() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable(engine, sqlExecutionContext);
            execute("CREATE MATERIALIZED VIEW mv REFRESH MANUAL AS SELECT ts, last(price) price FROM base SAMPLE BY 1h");
            final MatViewTimerJob timer = new MatViewTimerJob(engine);
            timer.run();
            final MatViewState state = engine.getMatViewStateStore().getViewState(engine.verifyTableName("mv"));
            Assert.assertEquals(1, state.getRegisteredTimerCount());
            state.markAsDropped();
            currentMicros += Micros.DAY_MICROS;
            timer.run();
            Assert.assertEquals(0, timer.getTimerCount());
            Assert.assertEquals(0, state.getRegisteredTimerCount());
        });
    }

    @Test
    public void testExpiryDiscardsMissingStateBeforeRemovePublication() throws Exception {
        assertMemoryLeak(() -> {
            final GatedQueue queue = new GatedQueue();
            try (CairoEngine testEngine = createEngine(queue);
                 SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(testEngine, 1)) {
                testEngine.load();
                createBaseTable(testEngine, context);
                testEngine.execute("CREATE MATERIALIZED VIEW mv REFRESH EVERY 1m DEFERRED PERIOD (LENGTH 1h) AS SELECT ts, last(price) price FROM base SAMPLE BY 1h", context);
                final TableToken token = testEngine.verifyTableName("mv");
                final MatViewTimerJob timer = new MatViewTimerJob(testEngine);
                timer.run();
                Assert.assertEquals(3, timer.getTimerCount());
                final Gates gates = new Gates(true);
                queue.gates = gates;
                final AtomicReference<Throwable> error = new AtomicReference<>();
                final Thread drop = dropThread(testEngine, error);
                try {
                    drop.start();
                    await(gates.removeEntered);
                    Assert.assertNull(testEngine.getMatViewStateStore().getViewState(token));
                    Assert.assertNotNull(testEngine.getDependentViewGraph().getViewDefinition(token));
                    currentMicros += Micros.DAY_MICROS;
                    timer.run();
                    Assert.assertEquals("missing-state expiry must retire all timer types", 0, timer.getTimerCount());
                } finally {
                    gates.allowRemove.countDown();
                    TestUtils.joinThreads(drop);
                    assertNoThreadError(error);
                }
                timer.run();
                currentMicros += Micros.DAY_MICROS;
                Assert.assertFalse(timer.run());
            }
        });
    }

    @Test
    public void testInvalidStateRetainsTimersThroughRecovery() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable(engine, sqlExecutionContext);
            execute("CREATE MATERIALIZED VIEW mv REFRESH EVERY 1m DEFERRED PERIOD (LENGTH 1h) AS SELECT ts, last(price) price FROM base SAMPLE BY 1h");
            final TableToken token = engine.verifyTableName("mv");
            final MatViewState state = engine.getMatViewStateStore().getViewState(token);
            final MatViewTimerJob timer = new MatViewTimerJob(engine);

            // Pending invalidation suppresses dispatch, not registration or the next deadline.
            state.markAsPendingInvalidation("test invalidation");
            timer.run();
            Assert.assertEquals(3, timer.getTimerCount());
            currentMicros += Micros.DAY_MICROS;
            timer.run();
            Assert.assertEquals(3, timer.getTimerCount());
            Assert.assertEquals(3, state.getRegisteredTimerCount());

            Assert.assertTrue(state.tryLock());
            try {
                state.clearPendingInvalidationForTesting();
                state.markAsInvalid("test invalidation");
            } finally {
                state.unlock();
            }
            engine.getMatViewTimerQueue().enqueue(new MatViewTimerTask().ofUpdate(token));
            timer.run();
            Assert.assertEquals(3, timer.getTimerCount());
            currentMicros += Micros.DAY_MICROS;
            timer.run();
            Assert.assertEquals(3, timer.getTimerCount());
            Assert.assertEquals(3, state.getRegisteredTimerCount());

            execute("INSERT INTO base VALUES ('1970-01-01T01:00:00', 42)");
            drainWalAndMatViewQueues();
            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainWalAndMatViewQueues();
            Assert.assertFalse(state.isInvalid());
            assertQuery("SELECT price FROM mv").expectSize().noLeakCheck().returns("price\n42.0\n");

            // The retained schedule must pick up later data without another ADD or UPDATE.
            execute("INSERT INTO base VALUES ('1970-01-01T02:00:00', 43)");
            drainWalAndMatViewQueues();
            currentMicros += Micros.DAY_MICROS;
            timer.run();
            drainWalAndMatViewQueues();
            Assert.assertEquals(3, timer.getTimerCount());
            Assert.assertEquals(3, state.getRegisteredTimerCount());
            assertQuery("SELECT price FROM mv ORDER BY ts").expectSize().noLeakCheck().returns("price\n42.0\n43.0\n");
        });
    }

    @Test
    public void testTimerEvery5MonthsCountsFromStart() throws Exception {
        // START falls in April, so the 5-month cadence runs April, September, February, July. A grid
        // counted from January of each year would fire in June and November, and one counted from
        // January 1970 would fire in August, January and June. The timer job registers the timer on
        // 2024-05-10, inside the first period, so the first deadline comes from the grid.
        assertMemoryLeak(() -> {
            createBaseTable(engine, sqlExecutionContext);
            currentMicros = parseFloorPartialTimestamp("2024-05-10T00:00:00.000000Z");
            execute("CREATE MATERIALIZED VIEW mv REFRESH EVERY 5M DEFERRED START '2024-04-15T08:00:00.000000Z' AS SELECT ts, last(price) price FROM base SAMPLE BY 1h");
            final MatViewTimerJob timer = new MatViewTimerJob(engine);
            tickTimer(timer, "2024-05-10T00:00:00.000000Z");
            assertLastRefreshStart("mv", "");
            tickTimer(timer, "2024-06-15T08:00:00.000000Z");
            assertLastRefreshStart("mv", "");
            tickTimer(timer, "2024-08-15T08:00:00.000000Z");
            assertLastRefreshStart("mv", "");
            tickTimer(timer, "2024-09-15T07:59:59.999999Z");
            assertLastRefreshStart("mv", "");
            tickTimer(timer, "2024-09-15T08:00:00.000000Z");
            assertLastRefreshStart("mv", "2024-09-15T08:00:00.000000Z");
            tickTimer(timer, "2024-11-15T08:00:00.000000Z");
            assertLastRefreshStart("mv", "2024-09-15T08:00:00.000000Z");
            tickTimer(timer, "2025-01-15T08:00:00.000000Z");
            assertLastRefreshStart("mv", "2024-09-15T08:00:00.000000Z");
            tickTimer(timer, "2025-02-15T08:00:00.000000Z");
            assertLastRefreshStart("mv", "2025-02-15T08:00:00.000000Z");
            tickTimer(timer, "2025-06-15T08:00:00.000000Z");
            assertLastRefreshStart("mv", "2025-02-15T08:00:00.000000Z");
            tickTimer(timer, "2025-07-15T08:00:00.000000Z");
            assertLastRefreshStart("mv", "2025-07-15T08:00:00.000000Z");
        });
    }

    @Test
    public void testTimerEveryDayInTimeZoneFiresAtLocalTimesAroundDstEndAheadOfUtc() throws Exception {
        // DST ends in Europe/Berlin on 2022-10-30 at 01:00Z (03:00 CEST -> 02:00 CET), and the clock passes
        // 02:00-03:00 local twice.
        testTimerFiresAtLocalTimes("Europe/Berlin", ChronoUnit.DAYS, 3, halfPastHoursOf("2022-10-29"));
    }

    @Test
    public void testTimerEveryDayInTimeZoneFiresAtLocalTimesAroundDstEndBehindUtc() throws Exception {
        // DST ends in America/New_York on 2022-11-06 at 06:00Z (02:00 EDT -> 01:00 EST), and the clock
        // passes 01:00-02:00 local twice.
        testTimerFiresAtLocalTimes("America/New_York", ChronoUnit.DAYS, 3, halfPastHoursOf("2022-11-05"));
    }

    @Test
    public void testTimerEveryDayInTimeZoneFiresAtLocalTimesAroundDstEndSouthOfEquator() throws Exception {
        // DST ends in Australia/Sydney on 2022-04-03 at 16:00Z the day before (03:00 AEDT -> 02:00 AEST),
        // and the clock passes 02:00-03:00 local twice.
        testTimerFiresAtLocalTimes("Australia/Sydney", ChronoUnit.DAYS, 3, halfPastHoursOf("2022-04-02"));
    }

    @Test
    public void testTimerEveryDayInTimeZoneFiresAtLocalTimesAroundDstStartAheadOfUtc() throws Exception {
        // DST starts in Europe/Berlin on 2022-03-27 at 01:00Z (02:00 CET -> 03:00 CEST), and the clock
        // skips 02:00-03:00 local.
        testTimerFiresAtLocalTimes("Europe/Berlin", ChronoUnit.DAYS, 3, halfPastHoursOf("2022-03-26"));
    }

    @Test
    public void testTimerEveryDayInTimeZoneFiresAtLocalTimesAroundDstStartBehindUtc() throws Exception {
        // DST starts in America/New_York on 2022-03-13 at 07:00Z (02:00 EST -> 03:00 EDT), and the clock
        // skips 02:00-03:00 local.
        testTimerFiresAtLocalTimes("America/New_York", ChronoUnit.DAYS, 3, halfPastHoursOf("2022-03-12"));
    }

    @Test
    public void testTimerEveryDayInTimeZoneFiresAtLocalTimesAroundDstStartSouthOfEquator() throws Exception {
        // DST starts in Australia/Sydney on 2022-10-02 at 16:00Z the day before (02:00 AEST -> 03:00 AEDT),
        // and the clock skips 02:00-03:00 local.
        testTimerFiresAtLocalTimes("Australia/Sydney", ChronoUnit.DAYS, 3, halfPastHoursOf("2022-10-01"));
    }

    @Test
    public void testTimerEveryDayInTimeZoneKeepsLocalTimeAcrossDstEnd() throws Exception {
        // DST ends in America/New_York on 2022-11-06 at 06:00Z (02:00 EDT -> 01:00 EST), so 03:00 local is
        // 07:00Z the day before and 08:00Z from that day on. The timer job registers the timer on
        // 2022-11-04, after START.
        assertMemoryLeak(() -> {
            createBaseTable(engine, sqlExecutionContext);
            currentMicros = parseFloorPartialTimestamp("2022-11-04T12:00:00.000000Z");
            execute("CREATE MATERIALIZED VIEW mv REFRESH EVERY 1d DEFERRED START '2022-03-01T03:00:00.000000Z' TIME ZONE 'America/New_York' AS SELECT ts, last(price) price FROM base SAMPLE BY 1h");
            final MatViewTimerJob timer = new MatViewTimerJob(engine);
            tickTimer(timer, "2022-11-04T12:00:00.000000Z");
            assertLastRefreshStart("mv", "");
            tickTimer(timer, "2022-11-05T06:59:59.999999Z");
            assertLastRefreshStart("mv", "");
            tickTimer(timer, "2022-11-05T07:00:00.000000Z");
            assertLastRefreshStart("mv", "2022-11-05T07:00:00.000000Z");
            tickTimer(timer, "2022-11-06T07:59:59.999999Z");
            assertLastRefreshStart("mv", "2022-11-05T07:00:00.000000Z");
            tickTimer(timer, "2022-11-06T08:00:00.000000Z");
            assertLastRefreshStart("mv", "2022-11-06T08:00:00.000000Z");
            tickTimer(timer, "2022-11-07T07:59:59.999999Z");
            assertLastRefreshStart("mv", "2022-11-06T08:00:00.000000Z");
            tickTimer(timer, "2022-11-07T08:00:00.000000Z");
            assertLastRefreshStart("mv", "2022-11-07T08:00:00.000000Z");
        });
    }

    @Test
    public void testTimerEveryDayInTimeZoneKeepsLocalTimeAcrossDstStart() throws Exception {
        // DST starts in America/New_York on 2022-03-13 at 07:00Z (02:00 EST -> 03:00 EDT), so 03:00 local
        // is 08:00Z the day before and 07:00Z from that day on. The timer job registers the timer on
        // 2022-03-11, after START.
        assertMemoryLeak(() -> {
            createBaseTable(engine, sqlExecutionContext);
            currentMicros = parseFloorPartialTimestamp("2022-03-11T12:00:00.000000Z");
            execute("CREATE MATERIALIZED VIEW mv REFRESH EVERY 1d DEFERRED START '2022-03-01T03:00:00.000000Z' TIME ZONE 'America/New_York' AS SELECT ts, last(price) price FROM base SAMPLE BY 1h");
            final MatViewTimerJob timer = new MatViewTimerJob(engine);
            tickTimer(timer, "2022-03-11T12:00:00.000000Z");
            assertLastRefreshStart("mv", "");
            tickTimer(timer, "2022-03-12T07:59:59.999999Z");
            assertLastRefreshStart("mv", "");
            tickTimer(timer, "2022-03-12T08:00:00.000000Z");
            assertLastRefreshStart("mv", "2022-03-12T08:00:00.000000Z");
            tickTimer(timer, "2022-03-13T06:59:59.999999Z");
            assertLastRefreshStart("mv", "2022-03-12T08:00:00.000000Z");
            tickTimer(timer, "2022-03-13T07:00:00.000000Z");
            assertLastRefreshStart("mv", "2022-03-13T07:00:00.000000Z");
            tickTimer(timer, "2022-03-14T06:59:59.999999Z");
            assertLastRefreshStart("mv", "2022-03-13T07:00:00.000000Z");
            tickTimer(timer, "2022-03-14T07:00:00.000000Z");
            assertLastRefreshStart("mv", "2022-03-14T07:00:00.000000Z");
        });
    }

    @Test
    public void testTimerEveryHourInTimeZoneFiresAtLocalTimesAroundDstEnd() throws Exception {
        // DST ends in America/New_York on 2022-11-06 at 06:00Z (02:00 EDT -> 01:00 EST). 01:00 local takes
        // its first pass, at 05:00Z, and 02:00 local is 07:00Z, so the timer does not fire at 06:00Z.
        testTimerFiresAtLocalTimes("America/New_York", ChronoUnit.HOURS, 6, "2022-11-06T00:00");
    }

    @Test
    public void testTimerEveryHourInTimeZoneFiresAtLocalTimesAroundDstStart() throws Exception {
        // DST starts in America/New_York on 2022-03-13 at 07:00Z (02:00 EST -> 03:00 EDT). 02:00 local
        // moves later, to 03:00 EDT, so both 02:00 and 03:00 local are 07:00Z.
        testTimerFiresAtLocalTimes("America/New_York", ChronoUnit.HOURS, 6, "2022-03-13T00:00");
    }

    @Test
    public void testTimerEveryMonthClampsStartDayToMonthEnd() throws Exception {
        // START falls on January 31st. The timer job registers the timer on 2024-02-10, so the first
        // deadline falls on the last day of February; later firings return to the 31st or clamp to
        // the last day of shorter months.
        assertMemoryLeak(() -> {
            createBaseTable(engine, sqlExecutionContext);
            currentMicros = parseFloorPartialTimestamp("2024-02-10T00:00:00.000000Z");
            execute("CREATE MATERIALIZED VIEW mv REFRESH EVERY 1M DEFERRED START '2024-01-31T08:00:00.000000Z' AS SELECT ts, last(price) price FROM base SAMPLE BY 1h");
            final MatViewTimerJob timer = new MatViewTimerJob(engine);
            tickTimer(timer, "2024-02-10T00:00:00.000000Z");
            assertLastRefreshStart("mv", "");
            tickTimer(timer, "2024-02-29T07:59:59.999999Z");
            assertLastRefreshStart("mv", "");
            tickTimer(timer, "2024-02-29T08:00:00.000000Z");
            assertLastRefreshStart("mv", "2024-02-29T08:00:00.000000Z");
            tickTimer(timer, "2024-03-31T07:59:59.999999Z");
            assertLastRefreshStart("mv", "2024-02-29T08:00:00.000000Z");
            tickTimer(timer, "2024-03-31T08:00:00.000000Z");
            assertLastRefreshStart("mv", "2024-03-31T08:00:00.000000Z");
            tickTimer(timer, "2024-04-30T08:00:00.000000Z");
            assertLastRefreshStart("mv", "2024-04-30T08:00:00.000000Z");
        });
    }

    @Test
    public void testTimerEveryMonthCreatedMidPeriod() throws Exception {
        // The timer job registers the timer on 2024-02-10, after START, so the first deadline comes
        // from the sampler grid rather than from START. It must be the February firing.
        assertMemoryLeak(() -> {
            createBaseTable(engine, sqlExecutionContext);
            currentMicros = parseFloorPartialTimestamp("2024-02-10T00:00:00.000000Z");
            execute("CREATE MATERIALIZED VIEW mv REFRESH EVERY 1M DEFERRED START '2024-01-15T08:00:00.000000Z' AS SELECT ts, last(price) price FROM base SAMPLE BY 1h");
            final MatViewTimerJob timer = new MatViewTimerJob(engine);
            tickTimer(timer, "2024-02-10T00:00:00.000000Z");
            assertLastRefreshStart("mv", "");
            tickTimer(timer, "2024-02-15T07:59:59.999999Z");
            assertLastRefreshStart("mv", "");
            tickTimer(timer, "2024-02-15T08:00:00.000000Z");
            assertLastRefreshStart("mv", "2024-02-15T08:00:00.000000Z");
            tickTimer(timer, "2024-03-15T07:59:59.999999Z");
            assertLastRefreshStart("mv", "2024-02-15T08:00:00.000000Z");
            tickTimer(timer, "2024-03-15T08:00:00.000000Z");
            assertLastRefreshStart("mv", "2024-03-15T08:00:00.000000Z");
        });
    }

    @Test
    public void testTimerEveryMonthRecreatedMidPeriod() throws Exception {
        // ALTER re-creates the timer on 2024-02-10 with a START of 2024-01-15T08:00, partway through
        // that schedule's January period. The re-created timer must keep the February firing. Since
        // the old schedule fires on the 20th, a firing on the 15th can only come from the new timer.
        assertMemoryLeak(() -> {
            createBaseTable(engine, sqlExecutionContext);
            currentMicros = parseFloorPartialTimestamp("2024-01-01T00:00:00.000000Z");
            execute("CREATE MATERIALIZED VIEW mv REFRESH EVERY 1M DEFERRED START '2024-01-20T08:00:00.000000Z' AS SELECT ts, last(price) price FROM base SAMPLE BY 1h");
            final MatViewTimerJob timer = new MatViewTimerJob(engine);
            tickTimer(timer, "2024-01-01T00:00:00.000000Z");
            assertLastRefreshStart("mv", "");
            tickTimer(timer, "2024-01-20T08:00:00.000000Z");
            assertLastRefreshStart("mv", "2024-01-20T08:00:00.000000Z");

            currentMicros = parseFloorPartialTimestamp("2024-02-10T00:00:00.000000Z");
            execute("ALTER MATERIALIZED VIEW mv SET REFRESH EVERY 1M START '2024-01-15T08:00:00.000000Z';");
            drainWalAndMatViewQueues();
            tickTimer(timer, "2024-02-10T00:00:00.000000Z");
            assertLastRefreshStart("mv", "2024-01-20T08:00:00.000000Z");
            tickTimer(timer, "2024-02-15T07:59:59.999999Z");
            assertLastRefreshStart("mv", "2024-01-20T08:00:00.000000Z");
            tickTimer(timer, "2024-02-15T08:00:00.000000Z");
            assertLastRefreshStart("mv", "2024-02-15T08:00:00.000000Z");
            tickTimer(timer, "2024-02-20T08:00:00.000000Z");
            assertLastRefreshStart("mv", "2024-02-15T08:00:00.000000Z");
        });
    }

    @Test
    public void testTimerEveryYearCreatedMidPeriod() throws Exception {
        // The timer job registers both timers on 2024-03-01, after START and before the June
        // anniversary. The 1-year timer must keep the 2024 firing, and the 2-year one must count
        // its strides from 2021 rather than from 1970.
        assertMemoryLeak(() -> {
            createBaseTable(engine, sqlExecutionContext);
            currentMicros = parseFloorPartialTimestamp("2024-03-01T00:00:00.000000Z");
            execute("CREATE MATERIALIZED VIEW mv_1y REFRESH EVERY 1y DEFERRED START '2023-06-15T08:00:00.000000Z' AS SELECT ts, last(price) price FROM base SAMPLE BY 1h");
            execute("CREATE MATERIALIZED VIEW mv_2y REFRESH EVERY 2y DEFERRED START '2021-06-15T08:00:00.000000Z' AS SELECT ts, last(price) price FROM base SAMPLE BY 1h");
            final MatViewTimerJob timer = new MatViewTimerJob(engine);
            tickTimer(timer, "2024-03-01T00:00:00.000000Z");
            assertLastRefreshStart("mv_1y", "");
            assertLastRefreshStart("mv_2y", "");
            tickTimer(timer, "2024-06-15T07:59:59.999999Z");
            assertLastRefreshStart("mv_1y", "");
            assertLastRefreshStart("mv_2y", "");
            tickTimer(timer, "2024-06-15T08:00:00.000000Z");
            assertLastRefreshStart("mv_1y", "2024-06-15T08:00:00.000000Z");
            assertLastRefreshStart("mv_2y", "");
            tickTimer(timer, "2025-06-15T08:00:00.000000Z");
            assertLastRefreshStart("mv_1y", "2025-06-15T08:00:00.000000Z");
            assertLastRefreshStart("mv_2y", "2025-06-15T08:00:00.000000Z");
        });
    }

    @Test
    public void testUpdateDroppedStateDoesNotRegisterTimers() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable(engine, sqlExecutionContext);
            execute("CREATE MATERIALIZED VIEW mv REFRESH EVERY 1m DEFERRED START '1970-01-02' AS SELECT ts, last(price) price FROM base SAMPLE BY 1h");
            final TableToken token = engine.verifyTableName("mv");
            final MatViewState state = engine.getMatViewStateStore().getViewState(token);
            final Queue<MatViewTimerTask> queue = engine.getMatViewTimerQueue();
            final MatViewTimerTask delayedAdd = new MatViewTimerTask();
            Assert.assertTrue(queue.tryDequeue(delayedAdd));
            Assert.assertEquals(MatViewTimerTask.ADD, delayedAdd.getOperation());
            Assert.assertEquals(token, delayedAdd.getMatViewToken());
            final MatViewTimerJob timer = new MatViewTimerJob(engine);
            Assert.assertEquals(0, timer.getTimerCount());
            Assert.assertEquals(0, state.getRegisteredTimerCount());

            // With no timers to remove, UPDATE makes addTimers the first getter of dropped state.
            state.markAsDropped();
            queue.enqueue(new MatViewTimerTask().ofUpdate(token));
            timer.run();
            Assert.assertEquals("UPDATE must reject non-null dropped state", 0, timer.getTimerCount());
            Assert.assertEquals(0, state.getRegisteredTimerCount());
        });
    }

    @Test
    public void testUpdateAfterCompletedDrop() throws Exception {
        testRegistrationAfterDrop(true, true);
    }

    @Test
    public void testUpdateAfterDropRemoveBeforeGraphRemoval() throws Exception {
        testRegistrationAfterDrop(true, false);
    }

    private static void assertNoThreadError(AtomicReference<Throwable> error) {
        if (error.get() != null) {
            throw new AssertionError("lifecycle thread failed", error.get());
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            Assert.assertTrue("timer lifecycle coordination timeout", latch.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static Thread dropThread(CairoEngine testEngine, AtomicReference<Throwable> error) {
        return new Thread(() -> {
            try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(testEngine, 1)) {
                testEngine.execute("DROP MATERIALIZED VIEW mv", context);
            } catch (Throwable th) {
                error.compareAndSet(null, th);
            } finally {
                Path.clearThreadLocals();
            }
        }, "mat-view-drop");
    }

    // Half past each hour from midnight to 04:00 local time on the given day: in and around the hour that a
    // DST change skips or repeats in the time zones of the tests.
    private static String[] halfPastHoursOf(String day) {
        return new String[]{day + "T00:30", day + "T01:30", day + "T02:30", day + "T03:30"};
    }

    private void assertLastRefreshStart(String viewName, String expectedTimestamp) throws Exception {
        assertQuery("SELECT last_refresh_start_timestamp FROM materialized_views WHERE view_name = '" + viewName + "'")
                .noRandomAccess()
                .noLeakCheck()
                .returns("last_refresh_start_timestamp\n" + expectedTimestamp + "\n");
    }

    // Checks that every view "mv<i>" last refreshed at the latest of its refresh times, firings[i], at or
    // before nowMicros, or has never refreshed.
    private void assertLastRefreshStarts(long[][] firings, long nowMicros) throws Exception {
        final StringSink expected = new StringSink();
        expected.put("view_name\tlast_refresh_start_timestamp\n");
        for (int i = 0; i < firings.length; i++) {
            expected.put("mv").put(i).put('\t');
            long lastFiring = Long.MIN_VALUE;
            for (long firing : firings[i]) {
                if (firing <= nowMicros) {
                    lastFiring = Math.max(lastFiring, firing);
                }
            }
            if (lastFiring != Long.MIN_VALUE) {
                MicrosFormatUtils.appendDateTimeUSec(expected, lastFiring);
            }
            expected.put('\n');
        }
        assertQuery("SELECT view_name, last_refresh_start_timestamp FROM materialized_views ORDER BY view_name")
                .noLeakCheck()
                .returns(expected);
    }

    private void createBaseTable(CairoEngine testEngine, SqlExecutionContext context) throws Exception {
        currentMicros = Micros.MINUTE_MICROS;
        testEngine.execute("CREATE TABLE base (ts " + timestampType.getTypeName() + ", price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL", context);
    }

    private CairoEngine createEngine(GatedQueue queue) throws Exception {
        return new CairoEngine(new DefaultTestCairoConfiguration(temp.newFolder().getAbsolutePath()) {
            @Override
            public MicrosecondClock getMicrosecondClock() {
                return () -> currentMicros;
            }
        }) {
            @Override
            protected Queue<MatViewTimerTask> createMatViewTimerQueue() {
                return queue;
            }
        };
    }

    private void testRegistrationAfterDrop(boolean isUpdate, boolean isDropCompletedBeforeTick) throws Exception {
        assertMemoryLeak(() -> {
            final GatedQueue queue = new GatedQueue();
            try (CairoEngine testEngine = createEngine(queue);
                 SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(testEngine, 1)) {
                testEngine.load();
                createBaseTable(testEngine, context);
                final MatViewTimerJob timer = new MatViewTimerJob(testEngine);
                TableToken previousToken = null;
                for (int episode = 0; episode < 3; episode++) {
                    testEngine.execute("CREATE MATERIALIZED VIEW mv REFRESH " + (isUpdate ? "IMMEDIATE" : "EVERY 1m DEFERRED START '1970-01-02'") + " AS SELECT ts, last(price) price FROM base SAMPLE BY 1h", context);
                    final TableToken token = testEngine.verifyTableName("mv");
                    if (previousToken != null) {
                        Assert.assertNotEquals(previousToken.getDirName(), token.getDirName());
                    }
                    final MatViewTimerTask delayedAdd = new MatViewTimerTask();
                    if (!isUpdate) {
                        // Hold CREATE's real ADD, just as a publisher paused after installing state would.
                        Assert.assertTrue(queue.tryDequeue(delayedAdd));
                        Assert.assertEquals(MatViewTimerTask.ADD, delayedAdd.getOperation());
                    }
                    drainWalAndMatViewQueues(testEngine);
                    timer.run();
                    Assert.assertEquals(0, timer.getTimerCount());
                    final Gates gates = new Gates(false);
                    queue.gates = gates;
                    final AtomicReference<Throwable> error = new AtomicReference<>();
                    final Thread apply = new Thread(() -> {
                        try {
                            drainWalQueue(testEngine);
                        } catch (Throwable th) {
                            error.compareAndSet(null, th);
                        } finally {
                            Path.clearThreadLocals();
                        }
                    }, "mat-view-alter-apply");
                    final Thread drop = dropThread(testEngine, error);
                    try {
                        if (isUpdate) {
                            testEngine.execute("ALTER MATERIALIZED VIEW mv SET REFRESH EVERY 1m START '1970-01-02';", context);
                            apply.start();
                            await(gates.updateEntered);
                        }
                        drop.start();
                        await(gates.removePublished);
                        Assert.assertNull(testEngine.getMatViewStateStore().getViewState(token));
                        Assert.assertNotNull(testEngine.getDependentViewGraph().getViewDefinition(token));
                        // Consume REMOVE before the stale registration reaches the consumer.
                        timer.run();
                        if (isUpdate) {
                            gates.allowUpdate.countDown();
                            await(gates.updatePublished);
                        } else {
                            queue.enqueue(delayedAdd);
                        }
                        if (isDropCompletedBeforeTick) {
                            gates.allowRemove.countDown();
                            TestUtils.joinThreads(drop);
                            assertNoThreadError(error);
                            Assert.assertNull(testEngine.getDependentViewGraph().getViewDefinition(token));
                        }
                        timer.run();
                        Assert.assertEquals("stale registration must not retain timers", 0, timer.getTimerCount());
                    } finally {
                        gates.allowUpdate.countDown();
                        gates.allowRemove.countDown();
                        try {
                            TestUtils.joinThreads(apply, drop);
                            assertNoThreadError(error);
                        } finally {
                            queue.gates = null;
                        }
                    }
                    for (int tick = 0; tick < 3; tick++) {
                        currentMicros += Micros.MINUTE_MICROS;
                        Assert.assertFalse(timer.run());
                        Assert.assertEquals(0, timer.getTimerCount());
                    }
                    previousToken = token;
                }
            }
        });
    }

    // Creates a view "mv<i>" for each START, a local time in the given time zone, that refreshes on a timer
    // every unit from that START, and runs the timer job just before and at each of the first firingCount
    // refresh times of every view. Each view must refresh at the local times that its START and EVERY give,
    // as ZonedDateTime.of() puts them in the time zone: a local time that a DST start skips moves later by
    // the length of the gap, and one that a DST end repeats takes its first pass. The STARTs lie within one
    // unit of each other, so that no view refreshes again before the last of these refresh times.
    private void testTimerFiresAtLocalTimes(String timeZone, ChronoUnit unit, int firingCount, String... starts) throws Exception {
        // The clock moves on by up to a day at every tick, so every WAL apply would take a durable epoch,
        // which syncs the table. The epochs play no part in the timers.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        assertMemoryLeak(() -> {
            final ZoneId zone = ZoneId.of(timeZone);
            final long[][] firings = new long[starts.length][firingCount];
            final TreeSet<Long> firingTimes = new TreeSet<>();
            createBaseTable(engine, sqlExecutionContext);
            for (int i = 0; i < starts.length; i++) {
                execute(
                        "CREATE MATERIALIZED VIEW mv" + i + " REFRESH EVERY 1" + (unit == ChronoUnit.HOURS ? 'h' : 'd') + " DEFERRED" +
                                " START '" + starts[i] + ":00.000000Z' TIME ZONE '" + timeZone + "'" +
                                " AS SELECT ts, last(price) price FROM base SAMPLE BY 1h"
                );
                final LocalDateTime start = LocalDateTime.parse(starts[i]);
                for (int k = 0; k < firingCount; k++) {
                    firings[i][k] = ZonedDateTime.of(start.plus(k, unit), zone).toEpochSecond() * Micros.SECOND_MICROS;
                    firingTimes.add(firings[i][k]);
                }
            }

            // The timer job registers the timers a day before the first refresh, so each timer fires first
            // at its START.
            final MatViewTimerJob timer = new MatViewTimerJob(engine);
            tickTimer(timer, firingTimes.first() - Micros.DAY_MICROS);
            assertLastRefreshStarts(firings, currentMicros);
            for (long firingTime : firingTimes) {
                tickTimer(timer, firingTime - 1);
                assertLastRefreshStarts(firings, currentMicros);
                tickTimer(timer, firingTime);
                assertLastRefreshStarts(firings, currentMicros);
            }
        });
    }

    // Commits a fresh base table row before every tick: a refresh that finds no new base table
    // transactions restores last_refresh_start_timestamp, which would hide the firing.
    private void tickTimer(MatViewTimerJob timer, String now) throws Exception {
        tickTimer(timer, parseFloorPartialTimestamp(now));
    }

    private void tickTimer(MatViewTimerJob timer, long nowMicros) throws Exception {
        currentMicros = nowMicros;
        execute("INSERT INTO base VALUES ('2024-01-01T00:00:00.000000Z', 42)");
        drainWalAndMatViewQueues();
        drainMatViewTimerQueue(timer);
        drainWalAndMatViewQueues();
    }

    private static class GatedQueue implements Queue<MatViewTimerTask> {
        private final Queue<MatViewTimerTask> delegate = ConcurrentQueue.createConcurrentQueue(MatViewTimerTask.ITEM_FACTORY);
        private volatile Gates gates;

        @Override
        public void clear() {
            delegate.clear();
        }

        @Override
        public void enqueue(MatViewTimerTask task) {
            final Gates active = gates;
            if (active != null && task.getOperation() == MatViewTimerTask.UPDATE) {
                active.updateEntered.countDown();
                await(active.allowUpdate);
                delegate.enqueue(task);
                active.updatePublished.countDown();
            } else if (active != null && task.getOperation() == MatViewTimerTask.REMOVE) {
                active.removeEntered.countDown();
                if (active.isRemoveBlockedBeforePublication) {
                    await(active.allowRemove);
                }
                delegate.enqueue(task);
                active.removePublished.countDown();
                if (!active.isRemoveBlockedBeforePublication) {
                    await(active.allowRemove);
                }
            } else {
                delegate.enqueue(task);
            }
        }

        @Override
        public boolean tryDequeue(MatViewTimerTask target) {
            return delegate.tryDequeue(target);
        }
    }

    private static class Gates {
        private final CountDownLatch allowRemove = new CountDownLatch(1);
        private final CountDownLatch allowUpdate = new CountDownLatch(1);
        private final boolean isRemoveBlockedBeforePublication;
        private final CountDownLatch removeEntered = new CountDownLatch(1);
        private final CountDownLatch removePublished = new CountDownLatch(1);
        private final CountDownLatch updateEntered = new CountDownLatch(1);
        private final CountDownLatch updatePublished = new CountDownLatch(1);

        private Gates(boolean isRemoveBlockedBeforePublication) {
            this.isRemoveBlockedBeforePublication = isRemoveBlockedBeforePublication;
        }
    }
}

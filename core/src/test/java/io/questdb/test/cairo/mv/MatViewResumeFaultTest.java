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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.griffin.SqlException;
import io.questdb.std.Files;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * RESUME persists its decision with a view state event in the view's WAL, written when the statement executes, after
 * the backlog update it runs without SKIP BACKLOG. When the state event's write fails, the statement fails with the
 * write's error and changes nothing: the view stays invalid with its reason and backlog, its latch is free, and its
 * WAL holds no new transaction. The same statement run again resumes the view. When the backlog update's write fails,
 * the backlog is lost, as for any failed backlog update, and RESUME refuses until SKIP BACKLOG.
 */
public class MatViewResumeFaultTest extends AbstractCairoTest {
    private static final String CAUGHT_UP_ROWS = """
            ts\tsym\tprice
            2024-01-01T00:00:00.000000Z\ta\t1.0
            2024-01-01T01:00:00.000000Z\tb\t2.0
            """;
    private static final String RESUME_SKIP_BACKLOG_SQL = "REFRESH MATERIALIZED VIEW price_1h RESUME SKIP BACKLOG";
    private static final String RESUME_SQL = "REFRESH MATERIALIZED VIEW price_1h RESUME";
    // the rows of a view that skipped b@01:10
    private static final String SKIPPED_ROWS = """
            ts\tsym\tprice
            2024-01-01T00:00:00.000000Z\ta\t1.0
            """;
    private final AtomicBoolean isFailArmed = new AtomicBoolean();
    private final StringSink failingPart = new StringSink();

    @Test
    public void testResumeFailsWhenStateEventWriteFails() throws Exception {
        // The backlog is current, so RESUME writes only its own state event, which fails. The backlog stays tracked,
        // and RESUME run again catches the view up on it.
        assertMemoryLeak(newFilesFacade(), () -> {
            final TableToken viewToken = createInvalidViewWithBacklog();
            engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(viewToken);
            drainWalAndMatViewQueues();
            assertStatementFailsOnWrite(viewToken, RESUME_SQL, "could not open");
            execute(RESUME_SQL);
            drainWalAndMatViewQueues();
            assertViewValidAndRefreshing(CAUGHT_UP_ROWS);
        });
    }

    @Test
    public void testResumeRefusesWhenBacklogUpdateWriteFails() throws Exception {
        // The backlog misses a base transaction, so RESUME first updates it, and that write fails. A failed update
        // loses the backlog (design section 6, "Lost backlog"): RESUME refuses with the cause, and keeps refusing,
        // until RESUME SKIP BACKLOG drops the backlog; RANGE then fills the gap.
        assertMemoryLeak(newFilesFacade(), () -> {
            final TableToken viewToken = createInvalidViewWithBacklog();
            final String lost = "materialized view backlog is lost [reason=backlog update failed]; RESUME cannot catch up on base "
                    + "changes made while the view was invalid. Run REFRESH MATERIALIZED VIEW price_1h RESUME SKIP BACKLOG, "
                    + "then REFRESH ... RANGE over the gap";
            assertStatementFailsOnWrite(viewToken, RESUME_SQL, lost);
            assertExceptionNoLeakCheck(RESUME_SQL, RESUME_SQL.indexOf("price_1h"), lost);
            assertViewStatus("invalid", "test invalidation");
            execute(RESUME_SKIP_BACKLOG_SQL);
            drainWalAndMatViewQueues();
            assertViewValidAndRefreshing(SKIPPED_ROWS);
            execute("REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2024-01-01T01:00:00.000000Z' TO '2024-01-01T02:00:00.000000Z'");
            drainWalAndMatViewQueues();
            assertQuery("price_1h ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns(CAUGHT_UP_ROWS
                    + "2024-01-01T02:00:00.000000Z\tc\t3.0\n");
        });
    }

    @Test
    public void testResumeSkipBacklogFailsWhenStateEventWriteFails() throws Exception {
        // SKIP BACKLOG updates no backlog and writes only its own state event, which fails; run again, it jumps over the
        // row the base received while the view was invalid.
        assertMemoryLeak(newFilesFacade(), () -> {
            final TableToken viewToken = createInvalidViewWithBacklog();
            assertStatementFailsOnWrite(viewToken, RESUME_SKIP_BACKLOG_SQL, "could not open");
            execute(RESUME_SKIP_BACKLOG_SQL);
            drainWalAndMatViewQueues();
            assertViewValidAndRefreshing(SKIPPED_ROWS);
        });
    }

    // Runs the statement with the next write to the view's transaction log failing. The statement fails with the
    // message and changes nothing: the view stays invalid with its reason and rows, its latch is free, its WAL holds no
    // new transaction and is not suspended.
    private void assertStatementFailsOnWrite(TableToken viewToken, String sql, String message) throws Exception {
        final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
        final long viewSeqTxn = viewSeqTxn(viewToken);
        failingPart.clear();
        failingPart.put(viewToken.getDirName()).put(Files.SEPARATOR).put(WalUtils.SEQ_DIR).put(Files.SEPARATOR)
                .put(WalUtils.TXNLOG_PARTS_DIR).put(Files.SEPARATOR).put(viewSeqTxn);
        isFailArmed.set(true);
        try {
            execute(sql);
            Assert.fail("the statement must fail when its view WAL write fails: " + sql);
        } catch (SqlException | CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), message);
        } finally {
            Assert.assertFalse("the write did not fail", isFailArmed.getAndSet(false));
        }
        Assert.assertFalse(state.isLocked());
        Assert.assertEquals(viewSeqTxn, viewSeqTxn(viewToken));
        drainWalAndMatViewQueues();
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(viewToken));
        assertViewStatus("invalid", "test invalidation");
        assertQuery("price_1h ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns(SKIPPED_ROWS);
    }

    // The view is valid, and a new base row reaches it.
    private void assertViewValidAndRefreshing(String expectedRows) throws Exception {
        assertViewStatus("valid", "");
        assertQuery("price_1h ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns(expectedRows);
        execute("INSERT INTO base VALUES ('c', 3.0, '2024-01-01T02:10:00.000000Z')");
        drainWalAndMatViewQueues();
        assertViewStatus("valid", "");
        assertQuery("price_1h ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns(expectedRows
                + "2024-01-01T02:00:00.000000Z\tc\t3.0\n");
    }

    // An IMMEDIATE view refreshed from a@00:10, made invalid, and a base row b@01:10 it misses while invalid.
    private TableToken createInvalidViewWithBacklog() throws Exception {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO base VALUES ('a', 1.0, '2024-01-01T00:10:00.000000Z')");
        drainWalQueue();
        execute("CREATE MATERIALIZED VIEW price_1h AS (SELECT ts, sym, max(price) AS price FROM base SAMPLE BY 1h) PARTITION BY DAY");
        drainWalAndMatViewQueues();
        final TableToken viewToken = engine.verifyTableName("price_1h");
        engine.getMatViewStateStore().enqueueInvalidate(viewToken, "test invalidation");
        drainWalAndMatViewQueues();
        execute("INSERT INTO base VALUES ('b', 2.0, '2024-01-01T01:10:00.000000Z')");
        drainWalAndMatViewQueues();
        assertViewStatus("invalid", "test invalidation");
        return viewToken;
    }

    // One transaction per part of a transaction log, so the next view transaction opens a part of its own, whose
    // open fails once armed.
    private TestFilesFacadeImpl newFilesFacade() {
        setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 1);
        return new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (isFailArmed.get() && Utf8s.endsWithAscii(name, failingPart)) {
                    isFailArmed.set(false);
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
    }

    private void assertViewStatus(String status, String reason) throws Exception {
        assertQuery("SELECT view_status, invalidation_reason FROM materialized_views()")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tinvalidation_reason\n" + status + '\t' + reason + '\n');
    }

    private static long viewSeqTxn(TableToken viewToken) {
        return engine.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn();
    }
}

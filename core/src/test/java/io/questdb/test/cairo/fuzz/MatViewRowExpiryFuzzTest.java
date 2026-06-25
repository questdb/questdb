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

package io.questdb.test.cairo.fuzz;

import io.questdb.PropertyKey;
import io.questdb.cairo.O3PartitionJob;
import io.questdb.cairo.RowExpiryCleanupJob;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Concurrency/stress fuzz for EXPIRE ROWS on passthrough materialized views, built on the standard parallel
 * WAL fuzz harness ({@link AbstractFuzzTest} / {@link FuzzRunner}). The base table is hammered by the usual
 * multi-threaded fuzz transactions (inserts, O3, REPLACE inserts, cancels, rollbacks, nulls) while, in the
 * background, mat-view refresh jobs, WAL apply, an EXPIRE cleanup job (optionally) and read queries all run
 * against the SAME policied view at once.
 * <ul>
 *     <li>{@link #testConcurrentRefreshAndReads()} runs ingest + refresh + apply + concurrent reads of the
 *     view; cleanup runs only once the system quiesces, so the read filter must then show EXACTLY the keep-set
 *     of the final base data, and the view must answer a battery of query shapes identically to that keep-set
 *     expressed independently as {@code LATEST ON}.</li>
 *     <li>{@link #testConcurrentCleanup()} additionally runs {@link RowExpiryCleanupJob} CONCURRENTLY with the
 *     fuzz (the worst case for the cleanup concurrency gate). It asserts robustness only -- no suspended table,
 *     no worker error, no leak, the view stays queryable and keeps the keep-latest "one row per key" shape --
 *     not an exact read-set, because a cleanup commit racing an O3 back-fill is a documented best-effort window
 *     that can drop a row (recoverable by a full refresh), which a strict equality check would flag flakily.</li>
 * </ul>
 * keep-latest (one row per key) is used so the post-fuzz comparison has a deterministic row order; the
 * single-threaded {@code RowExpiryFuzzTest} covers the other modes against an independent in-Java oracle.
 */
public class MatViewRowExpiryFuzzTest extends AbstractFuzzTest {

    // Bridge: AbstractCairoTest.assertSql(expected, sql) was removed in favor of the QueryAssertion
    // builder (OSS #7195). Drive the builder via returnsOnce() so the suite's calls keep working.
    private void assertSql(CharSequence expected, CharSequence sql) throws Exception {
        assertQuery(sql).noLeakCheck().returnsOnce(expected);
    }

    @Test
    public void testConcurrentCleanup() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            runExpiryFuzz(rnd, "expire rows keep latest partition by c2", true);
        });
    }

    @Test
    public void testConcurrentRefreshAndReads() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            runExpiryFuzz(rnd, "expire rows keep latest partition by c2", false);
        });
    }

    @Test
    public void testDeterministicBackfillBetweenScanAndCommitSurvives() throws Exception {
        // M3 DETERMINISTIC data-loss guard (non-fuzz). The core defense against cleanup deleting a row a
        // concurrent writer back-filled into a NON-ACTIVE partition since the survivor scan is the SEQUENCER-TXN
        // GATE: the job attempts reclamation only when the view is FULLY APPLIED at sweep start
        // (racyOpsAllowed = writerTxn == seqTxn). There is no in-job hook to pause exactly between the survivor
        // scan and the destructive commit, so instead of racing threads we reproduce the gate's trigger state
        // DETERMINISTICALLY: we leave the view COMMITTED-BUT-NOT-APPLIED (refresh advances the view's sequencer
        // txn, but we skip the WAL apply, so writerTxn < seqTxn). A back-filled KEPT row is thus invisible to a
        // reader-based survivor recount -- exactly the window the gate must cover. Cleanup MUST defer (reclaim
        // nothing); after we apply the view, the back-filled kept row must SURVIVE and be visible.
        //
        // What this CAN force deterministically: the writerTxn<seqTxn precondition that makes the survivor scan
        // miss a committed back-fill, and the requirement that cleanup not reclaim while in that state.
        // What it CANNOT force without a hook: the exact instruction-level interleave of scan vs commit on two
        // threads (the concurrent fuzz test exercises that probabilistically; the M3 no-loss assertion there is
        // the statistical counterpart).
        assertMemoryLeak(() -> {
            final String base = "m3_base";
            final String view = "m3_mv";
            execute("create table " + base + " (c2 symbol, c3 double, ts timestamp) timestamp(ts) partition by day wal");
            // Superseded rows in non-active partitions -> genuinely reclaimable once caught up.
            execute("insert into " + base + " values " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +   // superseded by A@01-03
                    "('B', 2.0, '2024-01-02T00:00:00.000000Z')," +   // B latest (non-active)
                    "('A', 3.0, '2024-01-03T00:00:00.000000Z')");    // A latest (active partition)
            drainWalAndMatViewQueues();
            execute("create materialized view " + view + " as (select * from " + base + ") expire rows keep latest partition by c2");
            drainWalAndMatViewQueues();

            final TableToken viewToken = engine.verifyTableName(view);
            final String predicate;
            try (TableMetadata m = engine.getTableMetadata(viewToken)) {
                predicate = m.getExpiryPredicate();
            }

            // Sanity: fully applied, and the keep-set (latest per key) is visible.
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(viewToken);
            Assert.assertEquals("precondition: view fully applied", tracker.getSeqTxn(), tracker.getWriterTxn());
            assertSql("c2\tc3\nA\t3.0\nB\t2.0\n", "select c2, c3 from " + view + " order by c2");

            // Back-fill a row that becomes a KEPT (latest) row for a NEW key C in a NON-ACTIVE partition
            // (01-02), then commit it to the VIEW's WAL via refresh but DO NOT apply -> writerTxn < seqTxn.
            execute("insert into " + base + " values ('C', 9.0, '2024-01-02T12:00:00.000000Z')");
            drainWalQueue();                 // apply the base insert (base caught up)
            drainMatViewQueue(engine);       // refresh: commits the back-fill into the VIEW's WAL sequencer...
            // ...but intentionally NO drainWalQueue() for the view -> the view is committed-but-not-applied.

            Assert.assertTrue(
                    "back-fill must be committed-but-not-applied on the view (writerTxn < seqTxn)",
                    tracker.getWriterTxn() < tracker.getSeqTxn()
            );

            // Run cleanup in this state. The gate must DEFER (racyOpsAllowed=false): no reclamation, so the
            // committed-but-unapplied kept C row cannot be physically deleted.
            final boolean reclaimed;
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                reclaimed = job.cleanupTable(viewToken, predicate);
            }
            Assert.assertFalse("cleanup must defer while the view is not fully applied (M3 gate)", reclaimed);

            // Now apply the view fully and assert the back-filled KEPT row SURVIVED (it was never reclaimed).
            drainWalAndMatViewQueues();
            Assert.assertEquals("view now fully applied", tracker.getSeqTxn(), tracker.getWriterTxn());
            assertSql(
                    "c2\tc3\nA\t3.0\nB\t2.0\nC\t9.0\n",
                    "select c2, c3 from " + view + " order by c2"
            );

            // A subsequent caught-up sweep may now reclaim the genuinely superseded A@01-01 -- but C must remain.
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                job.cleanupTable(viewToken, predicate);
            }
            drainWalAndMatViewQueues();
            assertSql(
                    "c2\tc3\nA\t3.0\nB\t2.0\nC\t9.0\n",
                    "select c2, c3 from " + view + " order by c2"
            );
        });
    }

    @Override
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        // Heavy concurrent apply/refresh/cleanup can briefly hold writer locks under load.
        setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, "60000");
        spinLockTimeout = 60_000;
    }

    private void assertQueryPatterns(SqlCompiler compiler, String base, String view) throws Exception {
        // The keep-set of a keep-latest passthrough view, expressed independently of the read filter.
        final String keepSet = "(select * from " + base + " latest on ts partition by c2)";
        final String[] templates = {
                "select count() c from TBL",
                "select c2, count() c, max(c3) mx, min(c1) mn from TBL group by c2 order by c2",
                "select c1, c2, c3, ts from TBL where c2 = 'AB' order by c2, ts, c1",
                "select c2, c1, ts from TBL where c3 > 2 order by c2, ts, c1",
                "select c2, c3 from TBL latest on ts partition by c2 order by c2",
                "select c2 from TBL order by c2 limit 3",
                "select c1, c2 from TBL where c2 in (select c2 from TBL where c3 > 3) order by c2, c1",
        };
        for (String tmpl : templates) {
            try {
                TestUtils.assertSqlCursors(
                        compiler,
                        sqlExecutionContext,
                        tmpl.replace("TBL", keepSet),
                        tmpl.replace("TBL", view),
                        LOG
                );
            } catch (AssertionError e) {
                throw new AssertionError("query-pattern mismatch [q=" + tmpl + "]: " + e.getMessage(), e);
            }
        }
    }

    private void runExpiryFuzz(Rnd rnd, String expireClause, boolean concurrentCleanup) throws Exception {
        final String base = getTestName() + "_0";
        final String view = base + "_mv";

        fuzzer.createInitialTableWal(base, "timestamp");
        // createInitialTable appends a few "column top" columns via async WAL ALTERs; apply them BEFORE
        // creating the passthrough view so "select *" captures the full base schema (otherwise the view
        // freezes at the pre-ALTER column set and refresh/compare break on the column-count mismatch).
        drainWalQueue();
        execute("create materialized view " + view + " as (select * from " + base + ") " + expireClause);
        drainWalAndMatViewQueues();

        final TableToken viewToken = engine.verifyTableName(view);
        final String predicate;
        try (TableMetadata m = engine.getTableMetadata(viewToken)) {
            predicate = m.getExpiryPredicate();
        }

        // Inserts / O3 / REPLACE / cancel / rollback / nulls only -- NO structural ops, truncate or partition
        // drop, which would invalidate a passthrough "select *" view and break the keep-set comparison.
        fuzzer.setFuzzCounts(
                rnd.nextBoolean(),
                rnd.nextInt(600),
                rnd.nextInt(1000),
                rnd.nextInt(3),
                rnd.nextInt(5),
                rnd.nextInt(1000),
                rnd.nextInt(15000),
                3 + rnd.nextInt(4)
        );
        fuzzer.setFuzzProbabilities(
                0.05,  // cancelRows
                0.05,  // notSet
                0.10,  // nullSet
                0.05,  // rollback
                0.0,   // colAdd
                0.0,   // colRemove
                0.0,   // colRename
                0.0,   // colTypeChange
                1.0,   // dataAdd
                0.0,   // equalTsRows (keep keep-latest unambiguous)
                0.0,   // partitionDrop
                0.0,   // truncate
                0.0,   // tableDrop
                0.0,   // setTtl
                0.10,  // replaceInsert
                0.10   // symbolAccessValidation
        );
        setFuzzProperties(rnd);

        final ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(base, rnd);

        final AtomicBoolean stop = new AtomicBoolean();
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final ObjList<Thread> jobs = new ObjList<>();
        final int refreshJobCount = 1 + rnd.nextInt(3);
        for (int i = 0; i < refreshJobCount; i++) {
            jobs.add(startRefreshJob(i, stop, rnd));
        }
        jobs.add(startViewQueryJob(view, stop, errors, rnd));
        if (concurrentCleanup) {
            jobs.add(startCleanupJob(viewToken, predicate, stop, errors, rnd));
        }

        engine.releaseInactive();
        final ObjList<ObjList<FuzzTransaction>> all = new ObjList<>();
        all.add(transactions);
        fuzzer.applyManyWalParallel(all, rnd, getTestName(), true, true);

        stop.set(true);
        for (int i = 0, n = jobs.size(); i < n; i++) {
            final Thread th = jobs.getQuick(i);
            TestUtils.unchecked(() -> th.join());
        }
        rethrow(errors);

        drainWalQueue();
        fuzzer.checkNoSuspendedTables();
        drainWalAndMatViewQueues();
        fuzzer.checkNoSuspendedTables();

        // Final cleanup on quiescent data (no ingestion racing the commit), then settle.
        try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
            job.cleanupTable(viewToken, predicate);
        }
        drainWalAndMatViewQueues();

        if (concurrentCleanup) {
            // Fast targeted read-filter probe before the exact comparison below: keep-latest must show ONLY
            // the latest (max-ts) row per key — no visible row is older than its key's max. Null-key safe.
            assertSql(
                    "stale\n0\n",
                    "select count() stale from (select ts, max(ts) over (partition by c2) mx from " + view + ") where ts < mx"
            );
        }
        // Full correctness for BOTH paths: the read-filtered view == the keep-set (latest per c2) of the final
        // base, and the view answers a battery of query shapes identically. The CONCURRENT path holds to the
        // SAME exact equality (no best-effort relaxation): cleanup and the refresh job are mutually exclusive
        // per view (both take MatViewState#tryLock(), see RowExpiryCleanupJob#cleanupTable), so a back-fill can
        // never be dropped between cleanup's survivor scan and its REPLACE_RANGE commit — the otherwise
        // best-effort commit-window race is closed. The deterministic counterpart is
        // testDeterministicBackfillBetweenScanAndCommitSurvives (cleanup DEFERS while a write is in flight).
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            TestUtils.assertSqlCursors(
                    compiler,
                    sqlExecutionContext,
                    "select * from " + base + " latest on ts partition by c2 order by c2",
                    "select * from " + view + " order by c2",
                    LOG
            );
            assertQueryPatterns(compiler, base, view);
        }
    }

    private Thread startCleanupJob(TableToken viewToken, String predicate, AtomicBoolean stop, ConcurrentLinkedQueue<Throwable> errors, Rnd outsideRnd) {
        final Rnd rnd = new Rnd(outsideRnd.nextLong(), outsideRnd.nextLong());
        final Thread th = new Thread(() -> {
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                while (!stop.get() && errors.isEmpty()) {
                    job.cleanupTable(viewToken, predicate);
                    Os.sleep(rnd.nextInt(15));
                }
            } catch (Throwable th2) {
                errors.add(th2);
            } finally {
                Path.clearThreadLocals();
            }
        }, "row-expiry-cleanup");
        th.start();
        return th;
    }

    private Thread startRefreshJob(int workerId, AtomicBoolean stop, Rnd outsideRnd) {
        final Rnd rnd = new Rnd(outsideRnd.nextLong(), outsideRnd.nextLong());
        final Thread th = new Thread(() -> {
            try (MatViewRefreshJob refreshJob = new MatViewRefreshJob(workerId, engine, 0)) {
                while (!stop.get()) {
                    refreshJob.run(workerId);
                    Os.sleep(rnd.nextInt(50));
                }
                // Drain the remainder, interleaving WAL apply so the refresh has base data to consume.
                try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                    do {
                        drainWalQueue(walApplyJob, engine);
                    } while (refreshJob.run(workerId));
                }
            } catch (Throwable throwable) {
                LOG.error().$("refresh job failed: ").$(throwable).$();
            } finally {
                Path.clearThreadLocals();
                Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
            }
        }, "refresh-job" + workerId);
        th.start();
        return th;
    }

    private Thread startViewQueryJob(String view, AtomicBoolean stop, ConcurrentLinkedQueue<Throwable> errors, Rnd outsideRnd) {
        final Rnd rnd = new Rnd(outsideRnd.nextLong(), outsideRnd.nextLong());
        final String[] queries = {
                "select count() from " + view,
                "select c2, count(), max(c3) from " + view + " order by c2",
                "select * from " + view + " where c2 = 'BC' limit 20",
                "select * from " + view + " latest on ts partition by c2",
                "select c1, ts from " + view + " order by ts desc limit 25",
        };
        final Thread th = new Thread(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine)) {
                while (!stop.get() && errors.isEmpty()) {
                    final String sql = queries[rnd.nextInt(queries.length)];
                    try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory();
                         RecordCursor cursor = factory.getCursor(ctx)) {
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) ;
                    } catch (Throwable th2) {
                        if (!isTolerable(th2)) {
                            errors.add(th2);
                            break;
                        }
                    }
                    Os.sleep(rnd.nextInt(5));
                }
            } catch (Throwable th2) {
                errors.add(th2);
            } finally {
                Path.clearThreadLocals();
            }
        }, "view-query");
        th.start();
        return th;
    }

    private static boolean isTolerable(Throwable th) {
        if (th instanceof TableReferenceOutOfDateException) {
            return true;
        }
        final String m = th.getMessage();
        return m != null && (m.contains("cached query") || m.contains("does not exist") || m.contains("table is dropped"));
    }

    private static void rethrow(ConcurrentLinkedQueue<Throwable> errors) {
        final Throwable th = errors.peek();
        if (th != null) {
            throw new AssertionError("concurrent worker failed: " + th.getMessage(), th);
        }
    }
}

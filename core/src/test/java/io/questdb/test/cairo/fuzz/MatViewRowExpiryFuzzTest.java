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
import io.questdb.griffin.SqlException;
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
 * background, mat-view refresh jobs, WAL apply, an EXPIRE cleanup job (optionally), a SET/DROP EXPIRE policy
 * churn job (optionally) and read queries all run against the SAME policied view at once.
 * <ul>
 *     <li>{@link #testConcurrentRefreshAndReads()} runs ingest + refresh + apply + concurrent reads of a
 *     keep-latest view; cleanup runs only once the system quiesces, so the read filter must then show EXACTLY
 *     the keep-set of the final base data, and the view must answer a battery of query shapes identically to
 *     that keep-set expressed independently as {@code LATEST ON}.</li>
 *     <li>{@link #testConcurrentCleanup()} additionally runs {@link RowExpiryCleanupJob} CONCURRENTLY with the
 *     fuzz. A keep-latest policy preserves physical history ({@code cleanupTable0} returns before any
 *     destructive work), so this variant exercises the per-view {@code MatViewState#tryLock()} mutual
 *     exclusion, discovery, and read-filter correctness under load - not reclamation.</li>
 *     <li>{@link #testConcurrentCleanupScalarWhen()} is the destructive-path stress: a scalar value policy
 *     ({@code when c3 < 0}) is monotonic, so the concurrent cleanup job runs real survivor scans and
 *     sequencer-fenced REPLACE_RANGE wipes/compactions WHILE ingest, refresh, reads and a SET/DROP EXPIRE
 *     policy churn thread race it. Every churned predicate expires a subset of the final canonical policy,
 *     so the end-of-run keep-set comparison stays exact even though physical reclamation is irreversible.</li>
 * </ul>
 * keep-latest (one row per key) gives the post-fuzz comparison a deterministic row order via its key; the
 * scalar variant relies on distinct timestamps (the equal-ts row probability is zero) and orders by the
 * designated timestamp. The single-threaded {@code RowExpiryFuzzTest} covers the remaining modes against an
 * independent in-Java oracle.
 */
public class MatViewRowExpiryFuzzTest extends AbstractFuzzTest {

    private static final String SCALAR_PREDICATE = "c3 < 0";

    @Override
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        // Heavy concurrent apply/refresh/cleanup can briefly hold writer locks under load.
        setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, "60000");
        spinLockTimeout = 60_000;
    }

    @Test
    public void testConcurrentCleanup() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            runExpiryFuzz(rnd, "expire rows keep latest partition by c2", true, false);
        });
    }

    @Test
    public void testConcurrentCleanupScalarWhen() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            runExpiryFuzz(rnd, "expire rows when " + SCALAR_PREDICATE, true, true);
        });
    }

    @Test
    public void testConcurrentRefreshAndReads() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            runExpiryFuzz(rnd, "expire rows keep latest partition by c2", false, false);
        });
    }

    @Test
    public void testDeterministicBackfillBetweenScanAndCommitSurvives() throws Exception {
        // DETERMINISTIC data-loss guard (non-fuzz) for the SCALAR destructive path. Cleanup must never
        // reclaim while the view has committed-but-unapplied WAL txns: the survivor scan reads only APPLIED
        // state, so a refresh back-fill committed to the view's sequencer but not yet applied is invisible
        // to it. There is no in-job hook to pause exactly between the survivor scan and the destructive
        // commit, so the trigger state is reproduced deterministically: refresh commits the back-fill into
        // the view's WAL (seqTxn advances) and the view's WAL apply is skipped (writerTxn < seqTxn). The
        // sweep baselines expectedSeqTxn on its reader's applied txn, so the per-commit sequencer fence
        // (commitWithParamsIfSeqTxn) rejects the fully-expired partition's wipe -- cleanup defers, reclaiming
        // nothing. After the view applies, the back-filled kept row must be visible, and a caught-up sweep
        // must reclaim the fully-expired partition while that row SURVIVES. The instruction-level
        // scan-vs-commit interleave on live threads is exercised probabilistically by
        // testConcurrentCleanupScalarWhen.
        assertMemoryLeak(() -> {
            final String base = "m3_base";
            final String view = "m3_mv";
            execute("create table " + base + " (c2 symbol, c3 double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into " + base + " values " +
                    "('A', -1.0, '2024-01-01T00:00:00.000000Z')," +  // expired (c3 < 0); day 01-01 fully expired -> reclaimable
                    "('B', -2.0, '2024-01-02T00:00:00.000000Z')," +  // expired; 01-02's APPLIED state is fully expired too
                    "('A', 3.0, '2024-01-03T00:00:00.000000Z')");    // kept (active partition)
            drainWalAndMatViewQueues();
            execute("create materialized view " + view + " as (select * from " + base + ") expire rows when " + SCALAR_PREDICATE);
            drainWalAndMatViewQueues();

            final TableToken viewToken = engine.verifyTableName(view);
            final String predicate;
            try (TableMetadata m = engine.getTableMetadata(viewToken)) {
                predicate = m.getExpiryPredicate();
            }

            // Sanity: fully applied, the read filter hides the expired rows, all three partitions are on disk.
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(viewToken);
            Assert.assertEquals("precondition: view fully applied", tracker.getSeqTxn(), tracker.getWriterTxn());
            assertQuery("select c2, c3 from " + view + " order by c2").noLeakCheck().returns("c2\tc3\nA\t3.0\n");
            assertQuery("select count() p from table_partitions('" + view + "')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");

            // Back-fill a KEPT row into the NON-ACTIVE 01-02 partition (whose applied state is fully
            // expired), then commit it to the VIEW's WAL via refresh but DO NOT apply -> writerTxn < seqTxn.
            execute("insert into " + base + " values ('C', 9.0, '2024-01-02T12:00:00.000000Z')");
            drainWalQueue();                 // apply the base insert (base caught up)
            drainMatViewQueue(engine);       // refresh: commits the back-fill into the VIEW's WAL sequencer...
            // ...but intentionally NO drainWalQueue() for the view -> the view is committed-but-not-applied.

            Assert.assertTrue(
                    "back-fill must be committed-but-not-applied on the view (writerTxn < seqTxn)",
                    tracker.getWriterTxn() < tracker.getSeqTxn()
            );

            // The sweep classifies BOTH 01-01 and 01-02 as fully expired and attempts their wipes, but the
            // refresh txn already advanced the sequencer past the sweep's reader baseline, so the fence
            // rejects both commits and nothing is reclaimed. This is the data-loss window the fence closes:
            // an unfenced empty REPLACE_RANGE over 01-02 would sequence AFTER the pending refresh txn and
            // physically delete the committed-but-unapplied kept C row when applied.
            final boolean reclaimed;
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                reclaimed = job.cleanupTable(viewToken, predicate);
            }
            Assert.assertFalse("cleanup must defer while the view is not fully applied", reclaimed);
            assertQuery("select count() p from table_partitions('" + view + "')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");

            // Now apply the view fully and assert the back-filled KEPT row is visible.
            drainWalAndMatViewQueues();
            Assert.assertEquals("view now fully applied", tracker.getSeqTxn(), tracker.getWriterTxn());
            assertQuery("select c2, c3 from " + view + " order by c2").noLeakCheck().returns("c2\tc3\nA\t3.0\nC\t9.0\n");

            // A caught-up sweep reclaims: 01-01 is wiped outright and 01-02 compacts to its surviving kept
            // row. The back-filled row SURVIVES physical reclamation.
            final boolean reclaimedAfterApply;
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                reclaimedAfterApply = job.cleanupTable(viewToken, predicate);
            }
            Assert.assertTrue("caught-up sweep must reclaim the fully-expired partition", reclaimedAfterApply);
            drainWalAndMatViewQueues();
            assertQuery("select count() p from table_partitions('" + view + "')").noRandomAccess().expectSize().noLeakCheck().returns("p\n2\n");
            assertQuery("select c2, c3 from " + view + " order by c2").noLeakCheck().returns("c2\tc3\nA\t3.0\nC\t9.0\n");
        });
    }

    private void assertQueryPatterns(SqlCompiler compiler, String keepSet, String view) throws Exception {
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

    private long getPartitionCount(String view) throws SqlException {
        try (RecordCursorFactory factory = engine.select("select count() from table_partitions('" + view + "')", sqlExecutionContext)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                return cursor.getRecord().getLong(0);
            }
        }
    }

    private void runExpiryFuzz(Rnd rnd, String expireClause, boolean concurrentCleanup, boolean policyChurn) throws Exception {
        final String base = getTestName() + "_0";
        final String view = base + "_mv";
        final boolean isScalar = expireClause.contains(" when ");
        // The keep-set of the policied passthrough view, expressed independently of the read filter. The
        // scalar oracle uses the same CASE keep-filter shape the read filter compiles to, evaluated over the
        // BASE table, so physical cleanup on the view cannot mask a read-filter defect. Row order: keep-latest
        // is total on the key; the scalar comparison orders by the designated timestamp with c1 as a
        // tiebreak (the generator makes duplicate timestamps rare but does not guarantee uniqueness).
        final String keepSetSql = isScalar
                ? "(select * from " + base + " where case when (" + SCALAR_PREDICATE + ") then false else true end)"
                : "(select * from " + base + " latest on ts partition by c2)";
        final String keepSetOrderBy = isScalar ? "ts, c1" : "c2";

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
                // setTtl: the fuzzer's TTL lands on the BASE table, whose partitions it then evicts, while the
                // passthrough view keeps its own copy -- the end-of-run comparison against the base would then
                // measure the base's TTL, not the policy. TTL on the VIEW composes with the policy and
                // MatViewExpireRowsTest.testTtl* covers it deterministically.
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
            jobs.add(startRefreshJob(i, stop, errors, rnd));
        }
        jobs.add(startViewQueryJob(view, stop, errors, rnd));
        if (concurrentCleanup) {
            jobs.add(startCleanupJob(viewToken, predicate, stop, errors, rnd));
        }
        if (policyChurn) {
            jobs.add(startPolicyChurnJob(view, stop, errors, rnd));
        }

        engine.releaseInactive();
        final ObjList<ObjList<FuzzTransaction>> all = new ObjList<>();
        all.add(transactions);
        Throwable primaryFailure = null;
        try {
            fuzzer.applyManyWalParallel(all, rnd, getTestName(), true, true);
        } catch (Throwable th) {
            primaryFailure = th;
        } finally {
            stop.set(true);
            for (int i = 0, n = jobs.size(); i < n; i++) {
                final Thread th = jobs.getQuick(i);
                try {
                    th.join();
                } catch (Throwable joinFailure) {
                    errors.add(joinFailure);
                }
            }
        }
        if (primaryFailure != null) {
            Throwable workerFailure;
            while ((workerFailure = errors.poll()) != null) {
                primaryFailure.addSuppressed(workerFailure);
            }
            if (primaryFailure instanceof Exception exception) {
                throw exception;
            }
            throw (Error) primaryFailure;
        }
        rethrow(errors);

        drainWalQueue();
        fuzzer.checkNoSuspendedTables();
        drainWalAndMatViewQueues();
        fuzzer.checkNoSuspendedTables();

        if (policyChurn) {
            // Deterministic end state after churn: restore the canonical policy. Every churned predicate
            // expires a SUBSET of it, so any row physically reclaimed under a churned policy is also expired
            // by the canonical one and the keep-set oracle below stays exact.
            execute("alter materialized view " + view + " set expire rows when " + SCALAR_PREDICATE);
            drainWalAndMatViewQueues();
        }
        // Re-read the authoritative predicate: under churn the discovery-time snapshot is stale (cleanup's
        // own authoritative re-read defers on such a mismatch, so the final sweep must use the current one).
        final String finalPredicate;
        try (TableMetadata m = engine.getTableMetadata(viewToken)) {
            finalPredicate = m.getExpiryPredicate();
        }
        Assert.assertNotNull(finalPredicate);
        if (policyChurn) {
            TestUtils.assertContains(finalPredicate, SCALAR_PREDICATE);
        }

        // Final cleanup on quiescent data (no ingestion racing the commit), then settle.
        try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
            job.cleanupTable(viewToken, finalPredicate);
            if (isScalar && getPartitionCount(view) > 1) {
                // Non-vacuity: a scalar value policy has no bounds threshold, so a quiescent sweep over a
                // multi-partition view must classify via survivor scans (fresh job => empty generation cache).
                Assert.assertTrue(
                        "final scalar sweep must classify partitions via survivor scans",
                        job.getScalarPartitionScanCount() > 0
                );
            }
        }
        drainWalAndMatViewQueues();

        if (concurrentCleanup) {
            // Fast targeted read-filter probe before the exact comparison below. Keep-latest: no visible row
            // is older than its key's max (null-key safe). Scalar: no visible row satisfies the expiry
            // predicate.
            if (isScalar) {
                assertQuery("select count() stale from " + view + " where " + SCALAR_PREDICATE).noRandomAccess().expectSize().noLeakCheck().returns("stale\n0\n");
            } else {
                assertQuery("select count() stale from (select ts, max(ts) over (partition by c2) mx from " + view + ") where ts < mx").noRandomAccess().expectSize().noLeakCheck().returns("stale\n0\n");
            }
        }
        // Full correctness for BOTH paths: the read-filtered view == the keep-set of the final base, and the
        // view answers a battery of query shapes identically. The CONCURRENT path holds to the SAME exact
        // equality (no best-effort relaxation): cleanup and the refresh job are mutually exclusive per view
        // (both take MatViewState#tryLock(), see RowExpiryCleanupJob#cleanupTable), and every destructive
        // commit is fenced on the sequencer txn, so a back-fill can never be dropped between cleanup's
        // survivor scan and its REPLACE_RANGE commit. The deterministic counterpart is
        // testDeterministicBackfillBetweenScanAndCommitSurvives (cleanup DEFERS while a write is in flight).
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            TestUtils.assertSqlCursors(
                    compiler,
                    sqlExecutionContext,
                    "select * from " + keepSetSql + " order by " + keepSetOrderBy,
                    "select * from " + view + " order by " + keepSetOrderBy,
                    LOG
            );
            assertQueryPatterns(compiler, keepSetSql, view);
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

    private Thread startPolicyChurnJob(String view, AtomicBoolean stop, ConcurrentLinkedQueue<Throwable> errors, Rnd outsideRnd) {
        final Rnd rnd = new Rnd(outsideRnd.nextLong(), outsideRnd.nextLong());
        // Every predicate here expires a SUBSET of the canonical policy (SCALAR_PREDICATE) the run is settled
        // to at the end: a row physically reclaimed under a churned policy is also expired by the canonical
        // one, so churn cannot desync the end-of-run keep-set oracle even though reclamation is irreversible.
        final String[] alters = {
                "alter materialized view " + view + " set expire rows when c3 < -100",
                "alter materialized view " + view + " set expire rows when c3 < -10",
                "alter materialized view " + view + " drop expire",
                "alter materialized view " + view + " set expire rows when " + SCALAR_PREDICATE,
        };
        final Thread th = new Thread(() -> {
            try (SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine)) {
                while (!stop.get() && errors.isEmpty()) {
                    try {
                        execute(alters[rnd.nextInt(alters.length)], ctx);
                    } catch (Throwable th2) {
                        if (!isTolerable(th2)) {
                            errors.add(th2);
                            break;
                        }
                    }
                    Os.sleep(rnd.nextInt(40));
                }
            } catch (Throwable th2) {
                errors.add(th2);
            } finally {
                Path.clearThreadLocals();
            }
        }, "policy-churn");
        th.start();
        return th;
    }

    private Thread startRefreshJob(int workerId, AtomicBoolean stop, ConcurrentLinkedQueue<Throwable> errors, Rnd outsideRnd) {
        final Rnd rnd = new Rnd(outsideRnd.nextLong(), outsideRnd.nextLong());
        final Thread th = new Thread(() -> {
            try (MatViewRefreshJob refreshJob = new MatViewRefreshJob(workerId, engine, 0)) {
                while (!stop.get()) {
                    refreshJob.run();
                    Os.sleep(rnd.nextInt(50));
                }
                // Drain the remainder, interleaving WAL apply so the refresh has base data to consume.
                try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                    do {
                        drainWalQueue(walApplyJob, engine);
                    } while (refreshJob.run());
                }
            } catch (Throwable throwable) {
                // Surface a refresh failure (e.g. an -ea assertion under concurrency) to the main thread
                // instead of only logging it, otherwise a masked failure passes the run.
                errors.add(throwable);
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
        return m != null && (m.contains("cached query")
                || m.contains("does not exist")
                || m.contains("table is dropped")
                // Sustained SET/DROP EXPIRE churn can exhaust a compile's policy-epoch retry budget; that is
                // an expected outcome of the adversarial churn, not a correctness failure.
                || m.contains("too many row-expiry policy changes"));
    }

    private static void rethrow(ConcurrentLinkedQueue<Throwable> errors) {
        final Throwable th = errors.peek();
        if (th != null) {
            throw new AssertionError("concurrent worker failed: " + th.getMessage(), th);
        }
    }
}

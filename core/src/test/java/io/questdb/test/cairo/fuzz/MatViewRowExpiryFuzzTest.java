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
            job.cleanupTable(viewToken, predicate, 0);
        }
        drainWalAndMatViewQueues();

        if (concurrentCleanup) {
            // Robustness only (a concurrent cleanup/back-fill race may have dropped a row, recoverable by a
            // full refresh): keep-latest must still show ONLY the latest (max-ts) row per key of whatever
            // physically survived -- no visible row is older than its key's max. This is M1-tolerant (it does
            // not assume an exact set) and null-key safe, and a read-filter regression would surface stale
            // rows here. Also confirms the view is queryable post-fuzz.
            assertSql(
                    "stale\n0\n",
                    "select count() stale from (select ts, max(ts) over (partition by c2) mx from " + view + ") where ts < mx"
            );
            return;
        }
        // Full correctness: the read-filtered view == the keep-set (latest per c2) of the final base, and the
        // view answers a battery of query shapes identically to that keep-set.
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
                    job.cleanupTable(viewToken, predicate, 0);
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

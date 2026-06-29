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

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.lv.LiveViewRecordCursor;
import io.questdb.griffin.engine.lv.LiveViewRecordCursorFactory;
import io.questdb.mp.Job;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Concurrency tests for live views, covering the ingestion and lifecycle shapes the
 * single-writer {@link LiveViewFuzzTest} cannot reach. The production-shaped scenarios:
 * <ul>
 *   <li><b>Multi-WalWriter base interleaving.</b> Several threads each open their own
 *   {@link WalWriter} on the same base table and commit concurrently, so the sequencer
 *   weaves their transactions into an interleaved log. Because a later-committed
 *   transaction can carry earlier timestamps than an already-materialized one, the
 *   live view's incremental refresh exercises O3 head-miss replay over a transaction
 *   stream no single-writer test produces.</li>
 *   <li><b>Concurrent refresh during ingestion.</b> A refresh-driver thread applies
 *   the base WAL and runs the refresh job while the writer threads are still
 *   ingesting - the steady-state production timing where base writes and live-view
 *   maintenance overlap.</li>
 *   <li><b>Concurrent DROP during refresh.</b> A {@code DROP LIVE VIEW} races a
 *   refresh-driver thread that keeps pumping the refresh job. The refresh job swallows
 *   per-view failures (so a torn-down view never throws into the worker); the test
 *   asserts the drop tears the view down cleanly - registry empty, base table intact,
 *   no leak, no crash - whatever the interleaving.</li>
 *   <li><b>Concurrent CREATE during ingestion.</b> A {@code CREATE LIVE VIEW ...
 *   BACKFILL} races concurrent base writes. The earliest rows are committed before
 *   CREATE so the backfill floor sits at the global-min timestamp and no concurrently
 *   ingested row falls below it; the backfill sweep and forward refresh between them
 *   cover every row exactly once, so the final state still equals a from-scratch
 *   recompute.</li>
 *   <li><b>Reader-churn soak.</b> Many reader threads repeatedly open and drain a
 *   cursor over an {@code IN MEMORY} live view while a refresh-driver appends to the
 *   in-memory tier via the fast-path CAS and writers ingest - the lock-free
 *   read/publish hand-off the RFC risk callout flags. Readers must never see a torn
 *   read or crash, and the quiesced final state still matches the recompute. The
 *   {@code InMem} variant uses a SYMBOL-free {@code row_number()} view so the reads
 *   route through Mode B (seam routing over the pinned slot), and each read asserts
 *   the per-snapshot invariant - rows ts-ascending, rn a gapless 1..N sequence - so
 *   a stale-restamped pre-O3 row or a seam duplicate/gap fails the read. The
 *   cross-writer O3 drives the in-mem rebuild against the live Mode B readers (the
 *   both-slots-pinned skip path).</li>
 * </ul>
 * <p>
 * <b>Why the oracle stays deterministic despite the concurrency.</b> The premise of
 * an incremental-maintenance engine is that the incrementally materialized state
 * equals a from-scratch recompute over the base table. The threads race only during
 * ingestion (and, for the second scenario, refresh); the test then joins every
 * worker, quiesces the refresh single-threaded, and only then compares the live view
 * against the same window query recomputed over the base table. Whatever order the
 * transactions interleaved in, the final state is a deterministic function of the row
 * set, so the comparison is sound. As in the fuzz test, every generated row has a
 * strictly-unique, strictly-increasing timestamp, so {@code OVER (ORDER BY ts ...)}
 * and the natural ts scan order used by {@code OVER ()} are total orders that both
 * the incremental and the batch path agree on. Out-of-order ingestion comes purely
 * from the cross-writer commit interleaving, never from colliding timestamps.
 * <p>
 * The test clock is pinned a full year below the data: a non-backfill view's lower
 * bound is the wall-clock CREATE moment and O3 head-miss replay only re-emits base
 * rows at or above that floor, so the data must sit above the clock. The refresh
 * driver advances the clock to clear FLUSH EVERY gating, but never by enough to cross
 * the one-year gap, so every row stays above the floor.
 */
public class LiveViewConcurrencyTest extends AbstractCairoTest {

    private static final long CLOCK_ADVANCE_MICROS = 250_000; // > FLUSH EVERY 100ms
    // The clock sits at 2026-01-01 (the CREATE moment / view lower bound); the data
    // starts a year later so the refresh driver's clock advances can never lift the
    // floor above a data row, even if it spins many times during ingestion.
    private static final String CLOCK_START = "2026-01-01T00:00:00.000000Z";
    private static final String DATA_START = "2027-01-01T00:00:00.000000Z";
    private static final String[] SYMBOLS = {"AA", "BB", "CC", "DD"};

    @Test
    public void testConcurrentCreateDuringIngestion() throws Exception {
        // CREATE LIVE VIEW ... BACKFILL races concurrent base ingestion.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> runCreateDuringIngestion(rnd, 4, 700));
    }

    @Test
    public void testConcurrentDropDuringRefresh() throws Exception {
        // DROP LIVE VIEW races a refresh-driver thread pumping the refresh job.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> runDropDuringRefresh(rnd, 4, 700));
    }

    @Test
    public void testConcurrentRefreshDuringIngestion() throws Exception {
        // A refresh-driver thread maintains the view while four writers ingest.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> runConcurrent(rnd, 0, 4, 800, false, true));
    }

    @Test
    public void testMultiWalWriterInterleaving() throws Exception {
        // Four concurrent writers, then a single-threaded refresh to quiescence.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> runConcurrent(rnd, 0, 4, 600, false, false));
    }

    @Test
    public void testMultiWalWriterInterleavingInMemory() throws Exception {
        // Same, with the in-memory tier enabled (fixed-width output, tier-eligible).
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> runConcurrent(rnd, 0, 4, 600, true, false));
    }

    @Test
    public void testMultiWalWriterInterleavingRowNumber() throws Exception {
        // Ranking re-sequencing under interleaved multi-writer O3 (the Finding 1/2b
        // surface, now from genuinely concurrent commits rather than shuffled inserts).
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> runConcurrent(rnd, 1, 6, 600, false, false));
    }

    @Test
    public void testReaderChurnSoak() throws Exception {
        // Reader threads churn cursors over an IN MEMORY view while a refresh driver
        // appends via the fast-path CAS and writers ingest (the RFC risk callout).
        // The sum() view carries a SYMBOL passthrough, so the reads route disk-only;
        // this soak stresses the read/publish hand-off without Mode B in the loop.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> runReaderChurnSoak(rnd, 4, 4, 800, false, false));
    }

    @Test
    public void testReaderChurnSoakInMem() throws Exception {
        // Same soak with a SYMBOL-free row_number() view so the reads genuinely
        // route through Mode B (seam routing over the pinned in-mem slot). The
        // readers assert a mid-flight invariant on every snapshot - ts strictly
        // ascending and rn a gapless 1..N sequence - so a torn slot, a seam
        // duplicate/gap, or a stale-restamped pre-O3 row surfaces as a value
        // mismatch, not merely a crash. The cross-writer O3 drives the in-mem
        // rebuild against the live Mode B readers (the both-slots-pinned race).
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> runReaderChurnSoak(rnd, 4, 4, 800, true, false));
    }

    @Test
    public void testReaderChurnSoakModeALead() throws Exception {
        // Mode A variant of the row_number() soak: the refresh driver advances the
        // clock only a fraction of FLUSH EVERY per tick, so most refreshes publish
        // an un-flushed lead into the in-mem tier (the tier leads disk) and flushes
        // land underneath the readers only every few ticks. The readers churn
        // cursors over the live lead and assert the same per-snapshot invariant - ts
        // strictly ascending, rn a gapless 1..N sequence - which must hold whether a
        // snapshot is served from the lead, the overlap, or disk-only after a fence
        // miss. So a torn lead publish, a seam duplicate/gap at the overlap/lead
        // boundary, or a stale-restamped slot surfaces as a value mismatch. After
        // the run quiesces the test rebuilds a known lead and asserts Mode A is
        // engaged (the cursor serves the lead and equals the recompute).
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> runReaderChurnSoak(rnd, 4, 4, 800, true, true));
    }

    private static void appendRow(WalWriter walWriter, long ts, int symIdx, long iv, double xv) {
        TableWriter.Row row = walWriter.newRow(ts);
        if (symIdx < 0) {
            row.putSym(1, (CharSequence) null);
        } else {
            row.putSym(1, SYMBOLS[symIdx]);
        }
        row.putLong(2, iv); // LONG_NULL stores as NULL
        row.putDouble(3, xv);
        row.append();
    }

    private static boolean drainJob(Job job) {
        boolean any = false;
        for (int i = 0; i < 64 && job.run(); i++) {
            any = true;
        }
        return any;
    }

    // Generates the logical dataset: strictly-unique, strictly-increasing timestamps
    // (so OVER (ORDER BY ts) and the natural ts scan order used by OVER () are total
    // orders both the incremental and the batch path agree on), random symbols and
    // values with occasional NULLs. The data starts a year above the test clock so
    // every row sits above a non-backfill view's CREATE-moment lower bound.
    private static void generateDataset(Rnd rnd, int rowCount, long[] tsv, int[] symIdx, long[] iv, double[] xv) {
        long ts = MicrosTimestampDriver.floor(DATA_START);
        for (int k = 0; k < rowCount; k++) {
            ts += 1 + rnd.nextInt(5_000_000); // 1us .. 5s, keeps ts strictly increasing
            if (rnd.nextInt(20) == 0) {
                ts += 86_400_000_000L; // occasional full-day jump to span more partitions
            }
            tsv[k] = ts;
            symIdx[k] = rnd.nextInt(20) == 0 ? -1 : rnd.nextInt(SYMBOLS.length); // -1 => NULL symbol
            iv[k] = rnd.nextInt(20) == 0 ? Numbers.LONG_NULL : (rnd.nextInt(2001) - 1000);
            xv[k] = rnd.nextDouble() * 1000.0;
        }
    }

    // The two grammar-legal, deterministic window shapes the fuzz test also uses:
    // a partitioned bounded-frame aggregate and ranking OVER (). Both carry the
    // incremental-snapshot contract and are total deterministic functions of a
    // unique-ts row set, so the recompute oracle holds under any ingestion order.
    private static String projection(int variant, int n) {
        final String frame = "PARTITION BY sym ORDER BY ts ROWS BETWEEN " + n + " PRECEDING AND CURRENT ROW";
        return switch (variant) {
            case 0 -> "ts, sym, i, sum(i) OVER (" + frame + ") AS v";
            case 1 -> "ts, sym, row_number() OVER () AS rn";
            default -> throw new IllegalArgumentException("variant=" + variant);
        };
    }

    // Drives the named view's backfill sweep to completion, re-fetching the instance
    // each pass, then applies the LV WAL. Mirrors the fuzz harness.
    private void driveBackfillToCompletion(LiveViewRefreshJob job, String viewName) {
        for (int i = 0; i < 1000; i++) {
            LiveViewInstance inst = engine.getLiveViewRegistry().getViewInstance(viewName);
            if (inst == null
                    || inst.getStateReader().getBackfillState() != LiveViewState.BACKFILL_STATE_BACKFILLING) {
                break;
            }
            drainJob(job);
        }
        drainWalQueue();
    }

    // Pumps the refresh job until no further LV WAL work is produced, advancing the
    // clock each pass so deferred flushes land, and applying the LV's own WAL after
    // each burst. Mirrors the fuzz harness.
    private void driveRefreshToQuiescence(LiveViewRefreshJob job) {
        for (int i = 0; i < 512; i++) {
            setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
            drainWalQueue();
            boolean progressed = drainJob(job);
            drainWalQueue();
            if (!progressed) {
                break;
            }
        }
    }

    // Builds a writer thread that owns its own WalWriter and ingests a round-robin
    // slice of [fromIndex, rowCount): writer w gets fromIndex+w, fromIndex+w+numWriters,
    // ... The slices are disjoint and globally ts-ordered, so timestamps stay unique;
    // the cross-writer commit interleaving is what produces O3. The thread awaits the
    // barrier before its first write and clears thread-locals on exit for the leak check.
    private Thread newWriterThread(
            int writerId,
            int numWriters,
            int fromIndex,
            int rowCount,
            int batch,
            long[] tsv,
            int[] symIdx,
            long[] iv,
            double[] xv,
            TableToken baseToken,
            CyclicBarrier barrier,
            ConcurrentLinkedQueue<Throwable> errors
    ) {
        return new Thread(() -> {
            try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                barrier.await();
                int sinceCommit = 0;
                for (int k = fromIndex + writerId; k < rowCount; k += numWriters) {
                    appendRow(walWriter, tsv[k], symIdx[k], iv[k], xv[k]);
                    if (++sinceCommit >= batch) {
                        walWriter.commit();
                        sinceCommit = 0;
                    }
                }
                walWriter.commit();
            } catch (Throwable th) {
                errors.add(th);
            } finally {
                Path.clearThreadLocals();
            }
        }, "lv-writer-" + writerId);
    }

    // Opens a fresh cursor over the live view and fully drains it, touching the
    // fixed-width columns so the read path actually runs over the row buffers. Called
    // in a tight loop by the reader threads while the view refreshes concurrently; it
    // asserts nothing about the row set (the view is mid-flight) - a torn read or a
    // corrupt tier slot surfaces as an exception or a JVM crash, not a value mismatch.
    // The final single-threaded oracle validates contents after quiescence.
    private void readViewOnce() throws Exception {
        try (
                SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine);
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile("SELECT * FROM lv", ctx).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(ctx)
        ) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                record.getLong(0); // ts
                record.getLong(2); // i
                record.getLong(3); // v (sum aggregate)
            }
        }
    }

    // Opens a fresh cursor over the SYMBOL-free row_number() view (columns
    // ts, i, rn) and drains it, asserting the per-snapshot invariant that holds
    // for every consistent LV-table version: rows come back ts-ascending (the
    // designated-timestamp total order) and rn is a gapless 1..N sequence in that
    // order. row_number() OVER () numbers rows in ts-ascending scan order and the
    // O3 replay re-sequences the whole table, so any committed snapshot - whether
    // served disk-only or through Mode B - must satisfy it. A torn read, a seam
    // duplicate/gap, or a stale pre-O3 row re-stamped into the slot breaks it.
    // The view is mid-flight, so the row count itself is not asserted here; the
    // final single-threaded oracle validates the full contents after quiescence.
    private void readRowNumberViewOnce() throws Exception {
        try (
                SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine);
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile("SELECT * FROM lv", ctx).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(ctx)
        ) {
            final Record record = cursor.getRecord();
            long prevTs = Long.MIN_VALUE;
            long expectedRn = 1;
            while (cursor.hasNext()) {
                long ts = record.getLong(0);
                long rn = record.getLong(2);
                if (ts <= prevTs) {
                    throw new AssertionError("ts not strictly ascending: prevTs=" + prevTs + ", ts=" + ts);
                }
                if (rn != expectedRn) {
                    throw new AssertionError("rn not a gapless 1..N sequence: expected=" + expectedRn
                            + ", actual=" + rn + ", ts=" + ts);
                }
                prevTs = ts;
                expectedRn++;
            }
        }
    }

    private void runConcurrent(
            Rnd rnd,
            int variant,
            int numWriters,
            int rowCount,
            boolean inMemory,
            boolean concurrentRefresh
    ) throws Exception {
        // Reset the clock per run to the fixed CREATE moment so the one-year gap to
        // the data is restored even after a prior run advanced it.
        setCurrentMicros(MicrosTimestampDriver.floor(CLOCK_START));

        final int n = 1 + rnd.nextInt(8);
        final String viewSql = "SELECT " + projection(variant, n) + " FROM base";
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + (inMemory ? "IN MEMORY 60s " : "")
                + "AS " + viewSql;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        LOG.info().$("LV concurrency: variant=").$(variant).$(", writers=").$(numWriters)
                .$(", rows=").$(rowCount).$(", n=").$(n).$(", inMem=").$(inMemory)
                .$(", concurrentRefresh=").$(concurrentRefresh).$(", sql=").$(viewSql).$();

        // Generate the logical dataset: strictly-unique, strictly-increasing
        // timestamps; random symbols and values with occasional NULLs.
        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        generateDataset(rnd, rowCount, tsv, symIdx, iv, xv);

        execute(createSql);

        final TableToken baseToken = engine.verifyTableName("base");
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
        try {
            final int driverCount = concurrentRefresh ? 1 : 0;
            final CyclicBarrier barrier = new CyclicBarrier(numWriters + driverCount);
            final AtomicBoolean ingesting = new AtomicBoolean(true);

            // Each writer owns its own WalWriter and writes a round-robin slice of
            // the rows (writer w gets w, w+numWriters, ...). The slices are disjoint
            // and globally ts-ordered, so timestamps stay unique; the cross-writer
            // commit interleaving is what produces O3.
            final Thread[] writers = new Thread[numWriters];
            for (int w = 0; w < numWriters; w++) {
                final int batch = 5 + rnd.nextInt(20);
                writers[w] = newWriterThread(w, numWriters, 0, rowCount, batch, tsv, symIdx, iv, xv, baseToken, barrier, errors);
            }

            // Optional refresh-driver thread: applies the base WAL and runs the
            // refresh job while ingestion is in flight (steady-state production
            // timing). Only this thread touches the clock during the concurrent
            // phase; the final quiescence drive runs after it has joined.
            final LiveViewRefreshJob driverJob = job;
            final Thread driver = concurrentRefresh ? new Thread(() -> {
                try {
                    barrier.await();
                    while (ingesting.get()) {
                        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                        drainWalQueue();
                        drainJob(driverJob);
                    }
                } catch (Throwable th) {
                    errors.add(th);
                } finally {
                    Path.clearThreadLocals();
                }
            }, "lv-refresh-driver") : null;

            for (Thread t : writers) {
                t.start();
            }
            if (driver != null) {
                driver.start();
            }
            for (Thread t : writers) {
                t.join();
            }
            ingesting.set(false);
            if (driver != null) {
                driver.join();
            }

            if (!errors.isEmpty()) {
                throw new RuntimeException("worker thread failed", errors.peek());
            }

            // Quiesce single-threaded, then assert the differential oracle below.
            drainWalQueue();
            driveRefreshToQuiescence(job);
        } finally {
            Misc.free(job);
        }

        // The oracle: the live view must equal the window query recomputed over the
        // base table. ORDER BY 1 (the unique ts) gives both sides a total order;
        // genericStringMatch tolerates SYMBOL-vs-STRING on passthrough.
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + viewSql + ") ORDER BY 1",
                "(lv) ORDER BY 1",
                LOG,
                true
        );

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
    }

    // CREATE LIVE VIEW ... BACKFILL races concurrent base ingestion: the writers and
    // the CREATE start together off the barrier, so the view comes into being while
    // the suffix is still being written.
    private void runCreateDuringIngestion(Rnd rnd, int numWriters, int rowCount) throws Exception {
        setCurrentMicros(MicrosTimestampDriver.floor(CLOCK_START));

        final int n = 1 + rnd.nextInt(8);
        final String viewSql = "SELECT " + projection(0, n) + " FROM base";
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms BACKFILL AS " + viewSql;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        generateDataset(rnd, rowCount, tsv, symIdx, iv, xv);

        // Pre-commit the earliest rows [0, preCount) single-threaded, BEFORE CREATE, so
        // the BACKFILL floor sits at the global-min timestamp (tsv[0]). Every row the
        // writers ingest concurrently with CREATE is then above the floor, so even an O3
        // commit is never rejected as sub-floor (Finding 3) - the backfill sweep and
        // forward refresh between them cover the full row set exactly once, so the view
        // still equals the recompute. The suffix [preCount, rowCount) races CREATE.
        final int preCount = 1 + rnd.nextInt(8);
        final TableToken baseToken = engine.verifyTableName("base");

        LOG.info().$("LV concurrency CREATE-during-ingestion: writers=").$(numWriters)
                .$(", rows=").$(rowCount).$(", n=").$(n).$(", preCount=").$(preCount)
                .$(", sql=").$(viewSql).$();

        try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
            for (int k = 0; k < preCount; k++) {
                appendRow(walWriter, tsv[k], symIdx[k], iv[k], xv[k]);
            }
            walWriter.commit();
        }
        drainWalQueue();

        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        LiveViewRefreshJob job = null;
        try {
            // numWriters writers + this (main) thread, released together, so CREATE and
            // the suffix ingestion start at the same instant.
            final CyclicBarrier barrier = new CyclicBarrier(numWriters + 1);
            final Thread[] writers = new Thread[numWriters];
            for (int w = 0; w < numWriters; w++) {
                final int batch = 5 + rnd.nextInt(20);
                writers[w] = newWriterThread(w, numWriters, preCount, rowCount, batch, tsv, symIdx, iv, xv, baseToken, barrier, errors);
            }
            for (Thread t : writers) {
                t.start();
            }
            barrier.await();
            execute(createSql); // races the concurrent suffix ingestion
            for (Thread t : writers) {
                t.join();
            }
            if (!errors.isEmpty()) {
                throw new RuntimeException("worker thread failed", errors.peek());
            }

            // Quiesce single-threaded: finish the backfill sweep, then drain forward.
            job = new LiveViewRefreshJob(0, engine, 1);
            drainWalQueue();
            driveBackfillToCompletion(job, "lv");
            driveRefreshToQuiescence(job);
        } finally {
            Misc.free(job);
        }

        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + viewSql + ") ORDER BY 1",
                "(lv) ORDER BY 1",
                LOG,
                true
        );

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
    }

    // DROP LIVE VIEW races a refresh-driver thread that keeps pumping the refresh job
    // while writers ingest. The refresh job swallows per-view failures
    // (handleRefreshFailure), so a torn-down view never throws into the driver - the
    // contract under test is a clean teardown, not a thrown error.
    private void runDropDuringRefresh(Rnd rnd, int numWriters, int rowCount) throws Exception {
        setCurrentMicros(MicrosTimestampDriver.floor(CLOCK_START));

        final int n = 1 + rnd.nextInt(8);
        final String viewSql = "SELECT " + projection(0, n) + " FROM base";
        // IN MEMORY so the drop tears down the in-mem tier (slot buffers, double
        // buffer) under the race, not just the on-disk path.
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 60s AS " + viewSql;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        generateDataset(rnd, rowCount, tsv, symIdx, iv, xv);

        execute(createSql);

        LOG.info().$("LV concurrency DROP-during-refresh: writers=").$(numWriters)
                .$(", rows=").$(rowCount).$(", n=").$(n).$(", sql=").$(viewSql).$();

        final TableToken baseToken = engine.verifyTableName("base");
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
        final AtomicBoolean refreshing = new AtomicBoolean(true);
        try {
            // numWriters writers + the refresh driver + this (main) thread, released
            // together, so by the time the main thread fires the DROP the writers are
            // ingesting and the driver is turning the refresh job - the drop lands
            // mid-flight.
            final CyclicBarrier barrier = new CyclicBarrier(numWriters + 2);
            final Thread[] writers = new Thread[numWriters];
            for (int w = 0; w < numWriters; w++) {
                final int batch = 5 + rnd.nextInt(20);
                writers[w] = newWriterThread(w, numWriters, 0, rowCount, batch, tsv, symIdx, iv, xv, baseToken, barrier, errors);
            }
            final Thread driver = new Thread(() -> {
                try {
                    barrier.await();
                    while (refreshing.get()) {
                        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                        drainWalQueue();
                        drainJob(job);
                    }
                } catch (Throwable th) {
                    errors.add(th);
                } finally {
                    Path.clearThreadLocals();
                }
            }, "lv-refresh-driver");

            for (Thread t : writers) {
                t.start();
            }
            driver.start();
            barrier.await();
            execute("DROP LIVE VIEW lv"); // races the in-flight refresh

            for (Thread t : writers) {
                t.join();
            }
            refreshing.set(false);
            driver.join();

            if (!errors.isEmpty()) {
                throw new RuntimeException("worker thread failed", errors.peek());
            }
        } finally {
            Misc.free(job);
        }

        // Clean teardown: the view is gone from the registry and the base table
        // survived intact (the view drop leaves its row set untouched). No leak -
        // assertMemoryLeak wraps the whole run.
        Assert.assertFalse(engine.getLiveViewRegistry().hasView("lv"));
        drainWalQueue();
        assertQuery("SELECT count(*) FROM base").noRandomAccess().expectSize().returns("count\n" + rowCount + "\n");

        execute("DROP TABLE base");
    }

    // Reader threads churn cursors over an IN MEMORY view while the refresh driver
    // appends via the fast-path CAS and writers ingest (the RFC reader-churn risk
    // callout). The readers detect torn reads / tier-slot corruption by crashing or
    // throwing; the quiesced final state still matches the recompute.
    // <p>
    // leadMode: the driver advances the clock only a fraction of FLUSH EVERY per
    // tick, so most refreshes publish an un-flushed lead (the tier leads disk) and
    // flushes land only every few ticks - the readers churn against a live lead
    // (Mode A) with flushes underneath, instead of a strict disk subset (Mode B).
    private void runReaderChurnSoak(Rnd rnd, int numWriters, int numReaders, int rowCount, boolean modeB, boolean leadMode) throws Exception {
        setCurrentMicros(MicrosTimestampDriver.floor(CLOCK_START));

        final int n = 1 + rnd.nextInt(8);
        // modeB: a SYMBOL-free row_number() view, so the read path routes through
        // the in-mem tier (Mode B seam routing) and the readers can assert the
        // gapless-rn invariant per snapshot. Otherwise: a sum() view with a SYMBOL
        // passthrough, which routes disk-only (the tier holds segment-local symbol
        // ids the reader cannot resolve) but still exercises the publish hand-off.
        final String viewSql = modeB
                ? "SELECT ts, i, row_number() OVER () AS rn FROM base"
                : "SELECT " + projection(0, n) + " FROM base";
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 60s AS " + viewSql;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        generateDataset(rnd, rowCount, tsv, symIdx, iv, xv);

        execute(createSql);

        LOG.info().$("LV concurrency reader-churn soak: writers=").$(numWriters)
                .$(", readers=").$(numReaders).$(", rows=").$(rowCount).$(", n=").$(n)
                .$(", modeB=").$(modeB).$(", leadMode=").$(leadMode).$(", sql=").$(viewSql).$();

        final TableToken baseToken = engine.verifyTableName("base");
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
        final AtomicBoolean running = new AtomicBoolean(true);
        try {
            // numWriters writers + the refresh driver, released together. Readers spin
            // independently (no synchronized start needed) until running clears.
            final CyclicBarrier barrier = new CyclicBarrier(numWriters + 1);
            final Thread[] writers = new Thread[numWriters];
            for (int w = 0; w < numWriters; w++) {
                final int batch = 5 + rnd.nextInt(20);
                writers[w] = newWriterThread(w, numWriters, 0, rowCount, batch, tsv, symIdx, iv, xv, baseToken, barrier, errors);
            }
            // In lead mode advance the clock by a fraction of FLUSH EVERY per tick so
            // a flush comes due only every few ticks; the refreshes in between
            // publish the un-flushed lead. Otherwise advance past FLUSH EVERY every
            // tick so every refresh also flushes (the tier stays a disk subset).
            final long clockStepMicros = leadMode ? CLOCK_ADVANCE_MICROS / 6 : CLOCK_ADVANCE_MICROS;
            final Thread driver = new Thread(() -> {
                try {
                    barrier.await();
                    while (running.get()) {
                        setCurrentMicros(currentMicros + clockStepMicros);
                        drainWalQueue();
                        drainJob(job);
                    }
                } catch (Throwable th) {
                    errors.add(th);
                } finally {
                    Path.clearThreadLocals();
                }
            }, "lv-refresh-driver");

            final Thread[] readers = new Thread[numReaders];
            for (int r = 0; r < numReaders; r++) {
                readers[r] = new Thread(() -> {
                    try {
                        while (running.get()) {
                            if (modeB) {
                                readRowNumberViewOnce();
                            } else {
                                readViewOnce();
                            }
                        }
                    } catch (Throwable th) {
                        errors.add(th);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }, "lv-reader-" + r);
            }

            for (Thread t : writers) {
                t.start();
            }
            driver.start();
            for (Thread t : readers) {
                t.start();
            }
            for (Thread t : writers) {
                t.join();
            }
            running.set(false);
            driver.join();
            for (Thread t : readers) {
                t.join();
            }

            if (!errors.isEmpty()) {
                throw new RuntimeException("worker thread failed", errors.peek());
            }

            // Quiesce single-threaded, then assert the differential oracle below.
            drainWalQueue();
            driveRefreshToQuiescence(job);
        } finally {
            Misc.free(job);
        }

        // The driveRefreshToQuiescence above flushed every lead, so the view is a
        // strict disk subset now - the standard ORDER BY 1 oracle is safe (no lead
        // to drop on a fence miss).
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + viewSql + ") ORDER BY 1",
                "(lv) ORDER BY 1",
                LOG,
                true
        );

        if (modeB && !leadMode) {
            // Guard against the soak silently passing on disk-only reads: confirm
            // the quiesced production read path actually routes through Mode B.
            assertModeBEngaged();
        }

        if (leadMode) {
            // Confirm Mode A is reachable for this view shape: rebuild a known
            // un-flushed lead and assert the cursor serves it and equals the
            // recompute. (The soak above already exercised live leads under the
            // readers; this pins it deterministically post-quiescence.)
            assertModeALeadEngaged(viewSql, tsv[rowCount - 1]);
        }

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
    }

    // Rebuilds a deterministic un-flushed lead on top of the quiesced (fully
    // flushed) state and asserts Mode A serves it: pins the flush clock to now and
    // refreshes a forward batch above the global max ts so it publishes into the
    // tier without crossing FLUSH EVERY, then opens the inner cursor and asserts it
    // routes through the tier, serves exactly the lead, and equals the recompute.
    private void assertModeALeadEngaged(String viewSql, long maxTs) throws Exception {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        instance.setLastFlushTimeUs(currentMicros);
        execute("INSERT INTO base (ts, sym, i, x) VALUES ("
                + (maxTs + 1) + "::timestamp, 'AA', 1, 1.0), ("
                + (maxTs + 2) + "::timestamp, 'AA', 2, 2.0)");
        drainWalQueue();
        try (LiveViewRefreshJob leadJob = new LiveViewRefreshJob(0, engine, 1)) {
            drainJob(leadJob); // refresh only -> lead in RAM (clock not advanced past FLUSH EVERY)
        }

        final long leadRows = instance.getLeadRowCount();
        Assert.assertTrue("a non-empty lead must be resident", leadRows > 0);

        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile("SELECT * FROM lv", sqlExecutionContext).getRecordCursorFactory()
        ) {
            RecordCursorFactory f = factory;
            while (f != null && !(f instanceof LiveViewRecordCursorFactory)) {
                f = f.getBaseFactory();
            }
            Assert.assertNotNull("expected a LiveViewRecordCursorFactory in the plan", f);
            try (LiveViewRecordCursor cursor = (LiveViewRecordCursor) f.getCursor(sqlExecutionContext)) {
                StringSink sink = new StringSink();
                println(f.getMetadata(), cursor, sink);
                Assert.assertTrue("Mode A lead read must route through the tier", cursor.isRoutingEligible());
                Assert.assertEquals("the cursor must serve exactly the lead", leadRows, cursor.leadRowsServed());
            }
        }

        // Direct SELECT * FROM lv (native ts order) equals the recompute, including
        // the lead - not the ORDER BY 1 wrapper, whose routing is not guaranteed
        // Mode A.
        StringSink lvOut = new StringSink();
        printSql("SELECT * FROM lv", lvOut);
        StringSink recompute = new StringSink();
        printSql(viewSql, recompute);
        Assert.assertEquals("Mode A lead read must equal the recompute", recompute.toString(), lvOut.toString());
    }

    // Confirms the SYMBOL-free row_number() view engages Mode B on the production
    // read path. Run single-threaded after quiescence, so the published slot is
    // stable and stamped with the latest applied seqTxn the fresh reader reports -
    // the fence holds and the cursor routes through the in-mem slot rather than
    // disk-only. Without this the reader-churn soak could pass even if every read
    // had fallen back to disk-only, leaving Mode B untested.
    private void assertModeBEngaged() throws Exception {
        try (
                SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine);
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile("SELECT * FROM lv", ctx).getRecordCursorFactory()
        ) {
            RecordCursorFactory f = factory;
            while (f != null && !(f instanceof LiveViewRecordCursorFactory)) {
                f = f.getBaseFactory();
            }
            Assert.assertNotNull("expected a LiveViewRecordCursorFactory in the plan", f);
            try (LiveViewRecordCursor cursor = (LiveViewRecordCursor) f.getCursor(ctx)) {
                Assert.assertTrue("quiesced row_number() read must route through Mode B", cursor.isRoutingEligible());
            }
        }
    }
}

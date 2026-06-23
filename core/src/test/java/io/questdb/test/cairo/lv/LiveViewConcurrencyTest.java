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
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.mp.Job;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Concurrency tests for live views, covering the ingestion shapes the single-writer
 * {@link LiveViewFuzzTest} cannot reach. Two production-shaped scenarios:
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

    private static boolean drainJob(Job job) {
        boolean any = false;
        for (int i = 0; i < 64 && job.run(); i++) {
            any = true;
        }
        return any;
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

        // Per-writer commit batch sizes, drawn from the seed so the run is
        // reproducible from the logged seed.
        final int[] batchSizes = new int[numWriters];
        for (int w = 0; w < numWriters; w++) {
            batchSizes[w] = 5 + rnd.nextInt(20);
        }

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
                final int writerId = w;
                final int batch = batchSizes[w];
                writers[w] = new Thread(() -> {
                    try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                        barrier.await();
                        int sinceCommit = 0;
                        for (int k = writerId; k < rowCount; k += numWriters) {
                            TableWriter.Row row = walWriter.newRow(tsv[k]);
                            if (symIdx[k] < 0) {
                                row.putSym(1, (CharSequence) null);
                            } else {
                                row.putSym(1, SYMBOLS[symIdx[k]]);
                            }
                            row.putLong(2, iv[k]); // LONG_NULL stores as NULL
                            row.putDouble(3, xv[k]);
                            row.append();
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
                }, "lv-writer-" + w);
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
}

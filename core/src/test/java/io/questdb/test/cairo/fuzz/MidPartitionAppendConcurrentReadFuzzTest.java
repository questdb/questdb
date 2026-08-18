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

package io.questdb.test.cairo.fuzz;

import io.questdb.PropertyKey;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Rnd;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Concurrent-reader fuzz for the mid-partition covered append path. The append
 * path extends a LIVE .pc in place (a fresh gen slot at the partition's existing
 * seal txn) rather than rotating the file the way the reseal does, so a reader
 * holding an older mapping of that .pc is exactly the hazard to prove safe -
 * the same failure mode that the block-apply fast path had to close on the last
 * partition.
 * <p>
 * The writer thread only ever appends into an EARLY day while a later day
 * already holds rows, so every commit is an append into a non-last partition and
 * takes the path under test (asserted via COVERING_SEAL_APPEND_COUNT). N
 * reader threads concurrently run covered scans and check, per observation:
 * <ul>
 *   <li>no crash or exception;</li>
 *   <li>covered == base at the reader's OWN snapshot, via a single-statement
 *       {@code (covered) EXCEPT (no_covering)} diff - any torn, stale or
 *       out-of-bounds covered fragment shows up as rows;</li>
 *   <li>snapshot-safe bounds: covered {@code max(value)} never exceeds the id
 *       ceiling published before the drain, {@code min(value) >= 1}, and the
 *       total row count never goes backwards.</li>
 * </ul>
 * The legacy variant seeds a format-0 (aliased-footer) covering head first: the
 * append path must refuse it (that head cannot be extended in place) and leave
 * it to the reseal, which migrates it to format 1 - after which appends resume.
 */
public class MidPartitionAppendConcurrentReadFuzzTest extends AbstractFuzzTest {

    private static final long DAY_MICROS = 24L * 60 * 60 * 1_000_000L;
    // Anchored to a day boundary so "target day" arithmetic is exact.
    private static final long T0 = 1_700_000_000_000_000L / DAY_MICROS * DAY_MICROS;
    // Writes go into day 1; day 3 exists throughout, so day 1 is never last.
    private static final long TARGET_DAY = T0 + DAY_MICROS;

    @Before
    public void enableCoveringCounters() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = true;
        resetCoveringCounters();
    }

    @After
    public void disableCoveringCounters() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = false;
        PostingIndexWriter.COVERING_SEAL_APPEND_DISABLED = false;
        PostingIndexWriter.FORCE_LEGACY_COVERING_FORMAT = false;
    }

    @Test
    public void testMidPartitionAppendConcurrentReadFuzz() throws Exception {
        runConcurrentReadFuzz(generateRandom(LOG), false);
    }

    /**
     * The same concurrent-reader load against the LAST partition. DEDUP
     * disqualifies the WAL fast-lag gate, so these appends take the O3 route into
     * the last partition - where the covered append path now extends a live .pc
     * in place on the partition every reader is scanning.
     */
    @Test
    public void testLastPartitionAppendConcurrentReadFuzzDedup() throws Exception {
        runConcurrentReadFuzz(generateRandom(LOG), false, true);
    }

    @Test
    public void testLastPartitionAppendConcurrentReadFuzzDedupRegression() throws Exception {
        runConcurrentReadFuzz(generateRandom(LOG, 0x63b1e04a7c25d9L, 0x0af52c9138be74L), false, true);
    }

    @Test
    public void testMidPartitionAppendConcurrentReadFuzzLegacyFormat0() throws Exception {
        runConcurrentReadFuzz(generateRandom(LOG), true);
    }

    @Test
    public void testMidPartitionAppendConcurrentReadFuzzLegacyFormat0Regression() throws Exception {
        runConcurrentReadFuzz(generateRandom(LOG, 0x2b91f4c7e0a63dL, 0x74d0a3e19c5b82L), true);
    }

    @Test
    public void testMidPartitionAppendConcurrentReadFuzzRegression() throws Exception {
        runConcurrentReadFuzz(generateRandom(LOG, 0x6f2a09d5b3c184L, 0x0e57c1b93da462L), false);
    }

    private void resetCoveringCounters() {
        PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.set(0);
        PostingIndexWriter.COVERING_AUTOSEAL_COUNT.set(0);
        PostingIndexWriter.COVERING_COW_MIGRATE_COUNT.set(0);
        PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.set(0);
    }

    private void runConcurrentReadFuzz(Rnd rnd, boolean legacyInitial) throws Exception {
        runConcurrentReadFuzz(rnd, legacyInitial, false);
    }

    private void runConcurrentReadFuzz(Rnd rnd, boolean legacyInitial, boolean dedup) throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 10_000_000);
        setProperty(PropertyKey.CAIRO_WAL_APPLY_LOOK_AHEAD_TXN_COUNT, 2000);
        setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 600_000);

        final int symbolCardinality = 3 + rnd.nextInt(6);
        final int readerCount = 3 + rnd.nextInt(3);
        // Enough commits to cross the gen threshold several times, so the
        // deferred compaction (seal) also runs while readers are active.
        final int rounds = 120 + rnd.nextInt(80);
        final int nullMod = 7 + rnd.nextInt(20);
        // {id, ts within the target day}
        final long[] writeCursor = {1L, TARGET_DAY + 1};

        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (value), value DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY WAL"
                    + (dedup ? " DEDUP UPSERT KEYS(ts, sym)" : ""));
            if (!dedup) {
                // Day 3 anchors the table's max timestamp: every later write into day
                // 1 is therefore an append into a partition that is NOT the last one.
                // Its single row carries value 0, below the id floor of 1 the readers
                // assert, so it is written with a NULL value instead.
                execute("INSERT INTO t SELECT (" + (T0 + 3 * DAY_MICROS) + ")::TIMESTAMP, 'S0', cast(NULL AS DOUBLE)"
                        + " FROM long_sequence(1)");
            }
            // With dedup there is deliberately NO later anchor: the target day is
            // the LAST partition, and dedup routes its appends through O3.
            drainWalQueue();

            if (legacyInitial) {
                PostingIndexWriter.FORCE_LEGACY_COVERING_FORMAT = true;
                try {
                    for (int b = 0; b < 6; b++) {
                        writeBatch(writeCursor, 30 + rnd.nextInt(60), 1 + rnd.nextInt(1000), symbolCardinality, nullMod);
                        drainWalQueue();
                    }
                } finally {
                    PostingIndexWriter.FORCE_LEGACY_COVERING_FORMAT = false;
                }
            } else {
                // Seed the target partition so the measured commits append to an
                // existing (not newly created) partition.
                writeBatch(writeCursor, 500, 100, symbolCardinality, nullMod);
                drainWalQueue();
            }
            resetCoveringCounters();

            final AtomicReference<Throwable> bgError = new AtomicReference<>();
            final AtomicBoolean stop = new AtomicBoolean();
            final AtomicLong rowFloor = new AtomicLong(0);
            final AtomicLong idCeiling = new AtomicLong(0);

            final Thread[] readers = new Thread[readerCount];
            for (int r = 0; r < readerCount; r++) {
                readers[r] = new Thread(() -> {
                    final Rnd rrnd = new Rnd();
                    try (
                            SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                                    .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                                            null, null, -1, null);
                            SqlCompiler compiler = engine.getSqlCompiler()
                    ) {
                        long prevTotal = 0;
                        while (!stop.get() && bgError.get() == null) {
                            final String sym = "S" + rrnd.nextInt(symbolCardinality);
                            final long floor = rowFloor.get();

                            // Covered == base at ONE snapshot. SYMMETRIC: a
                            // one-way EXCEPT only catches rows the covered scan
                            // invents, not rows it is MISSING - which is exactly
                            // what an unindexed tail looks like.
                            final long diff = scalar(compiler, ctx,
                                    "SELECT count(*) FROM ("
                                            + "((SELECT ts, value FROM t WHERE sym = '" + sym + "')"
                                            + " EXCEPT "
                                            + "(SELECT /*+ no_covering */ ts, value FROM t WHERE sym = '" + sym + "'))"
                                            + " UNION ALL "
                                            + "((SELECT /*+ no_covering */ ts, value FROM t WHERE sym = '" + sym + "')"
                                            + " EXCEPT "
                                            + "(SELECT ts, value FROM t WHERE sym = '" + sym + "')))");
                            if (diff != 0) {
                                throw new AssertionError("covered read disagrees with base column at snapshot for "
                                        + sym + ": EXCEPT returned " + diff + " rows");
                            }

                            final long total;
                            final long covMax;
                            final long covMin;
                            try (RecordCursorFactory f = compiler.compile(
                                    "SELECT count(*), max(value), min(value) FROM t WHERE sym = '" + sym + "'",
                                    ctx).getRecordCursorFactory();
                                 RecordCursor cur = f.getCursor(ctx)) {
                                if (cur.hasNext()) {
                                    total = cur.getRecord().getLong(0);
                                    final double mx = cur.getRecord().getDouble(1);
                                    final double mn = cur.getRecord().getDouble(2);
                                    covMax = Double.isNaN(mx) ? -1 : (long) mx;
                                    covMin = Double.isNaN(mn) ? Long.MAX_VALUE : (long) mn;
                                } else {
                                    total = 0;
                                    covMax = -1;
                                    covMin = Long.MAX_VALUE;
                                }
                            }
                            // Ceiling read AFTER the covered read: ids only grow and the
                            // ceiling is published before each drain, so this is an upper
                            // bound on anything that snapshot can hold.
                            final long ceiling = idCeiling.get();
                            if (ceiling > 0 && covMax > ceiling) {
                                throw new AssertionError("covered max(value)=" + covMax + " for " + sym
                                        + " exceeds the id ceiling " + ceiling
                                        + " (rows past published coverage / garbage)");
                            }
                            if (total > 0 && covMin != Long.MAX_VALUE && covMin < 1) {
                                throw new AssertionError("covered min(value)=" + covMin + " for " + sym
                                        + " is below 1 (stale/garbage covered fragment)");
                            }

                            final long grandTotal = scalar(compiler, ctx, "SELECT count(*) FROM t");
                            if (grandTotal < floor) {
                                throw new AssertionError("total count " + grandTotal + " fell below committed floor " + floor);
                            }
                            if (grandTotal < prevTotal) {
                                throw new AssertionError("total count went backwards: " + prevTotal + " -> " + grandTotal);
                            }
                            prevTotal = grandTotal;

                            // Drive the covered record cursor itself (crash surface).
                            try (RecordCursorFactory f = compiler.compile(
                                    "SELECT ts, sym, value FROM t WHERE sym = '" + sym + "' ORDER BY value LIMIT 32",
                                    ctx).getRecordCursorFactory();
                                 RecordCursor cur = f.getCursor(ctx)) {
                                //noinspection StatementWithEmptyBody
                                while (cur.hasNext()) {
                                    cur.getRecord().getDouble(2);
                                }
                            }
                        }
                    } catch (Throwable e) {
                        bgError.set(e);
                    }
                }, "midpart-append-reader-" + r);
                readers[r].setDaemon(true);
                readers[r].start();
            }

            try {
                for (int round = 0; round < rounds && bgError.get() == null; round++) {
                    final int txns = 1 + rnd.nextInt(4);
                    for (int t = 0; t < txns; t++) {
                        writeBatch(writeCursor, 10 + rnd.nextInt(200), 1 + rnd.nextInt(1000), symbolCardinality, nullMod);
                    }
                    // Ceiling BEFORE the drain (ids only grow), floor AFTER.
                    idCeiling.set(writeCursor[0] - 1);
                    drainWalQueue();
                    rowFloor.set(1);
                }
            } finally {
                stop.set(true);
                for (Thread th : readers) {
                    th.join(60_000);
                    Assert.assertFalse("reader thread did not terminate", th.isAlive());
                }
            }

            Assert.assertNull("concurrent covered reader failed: " + bgError.get(), bgError.get());
            Assert.assertFalse("table suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
            Assert.assertTrue("the mid-partition append path must fire (appends="
                            + PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.get() + ')',
                    PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.get() > 0);
            if (legacyInitial) {
                // A format-0 head must never be extended in place (the readers'
                // clean scans above are the proof it was not); it migrates to
                // format 1 through the reseal or the publishToChain COW.
                Assert.assertTrue("legacy format-0 head must migrate (fullReseals="
                                + PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get()
                                + ", cowMigrates=" + PostingIndexWriter.COVERING_COW_MIGRATE_COUNT.get() + ')',
                        PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get() > 0
                                || PostingIndexWriter.COVERING_COW_MIGRATE_COUNT.get() > 0);
            }
        });
    }

    private long scalar(SqlCompiler compiler, SqlExecutionContextImpl ctx, String sql) throws Exception {
        try (RecordCursorFactory f = compiler.compile(sql, ctx).getRecordCursorFactory();
             RecordCursor cur = f.getCursor(ctx)) {
            return cur.hasNext() ? cur.getRecord().getLong(0) : 0;
        }
    }

    /**
     * Appends one ascending batch into the target (mid) partition, advancing the
     * shared {id, ts} cursor. Values are the globally ascending id, so the
     * readers' monotonic and ceiling checks hold.
     */
    private void writeBatch(long[] cursor, int rows, long step, int symbolCardinality, int nullMod) throws Exception {
        final long v0 = cursor[0] - 1;
        final long startTs = cursor[1];
        execute("INSERT INTO t SELECT (" + startTs + " + x * " + step + ")::TIMESTAMP AS ts,"
                + " 'S' || ((" + v0 + " + x) % " + symbolCardinality + ") AS sym,"
                + " CASE WHEN ((" + v0 + " + x) % " + nullMod + ") = 0 THEN cast(NULL AS DOUBLE)"
                + " ELSE (" + v0 + " + x)::DOUBLE END AS value FROM long_sequence(" + rows + ")");
        cursor[0] = v0 + rows + 1;
        cursor[1] = startTs + (long) rows * step;
        // The batch must stay inside the target day, else it would create a new
        // last partition and stop exercising the mid-partition path.
        Assert.assertTrue("stream left the target partition", cursor[1] < TARGET_DAY + DAY_MICROS);
    }
}

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
 * Test D — concurrent-reader fuzz during covering block-apply fast-path ingest.
 * Directly targets the B1 failure mode (a reader holding an older mapped
 * {@code .pc} extent while a gen slot is written). The fast path writes covered
 * fragments only at fresh TXN_AT_SEAL-gated slots (never mutating visible data),
 * so readers must be safe; this proves it under concurrency + fuzz.
 * <p>
 * A single writer thread (the test thread) applies an ascending-biased random
 * stream as multi-txn backlogs drained one block at a time (so the block-apply
 * fast path fires, {@code fastLag > 0}) into ONE growing partition, accumulating
 * deferred gens until the MAX_GEN_COUNT auto-seal fires mid-run WHILE readers are
 * active. {@code value} is a globally unique ascending id (>= 1, with occasional
 * NULLs), so covered aggregates have snapshot-safe monotonic bounds.
 * <p>
 * N reader threads run covered scans concurrently. Per observation:
 * <ul>
 *   <li>NO crash / exception (a thrown reader is captured and fails the test);</li>
 *   <li>covered == base at the reader's OWN snapshot: a single statement
 *       {@code (covered scan) EXCEPT (no_covering scan)} of the same table must
 *       return zero rows — both branches read one reader txn, so any torn /
 *       garbage / stale covered fragment (or a covered value differing from the
 *       base column) surfaces as a non-empty diff;</li>
 *   <li>snapshot-safe bounds: total {@code count(*) >= } the committed floor and
 *       never goes backwards; covered {@code max(value) <=} the global id ceiling
 *       and {@code min(value) >= 1} (no rows past sealed coverage, no garbage).</li>
 * </ul>
 * A crash, a non-empty covered-vs-base diff, or an out-of-bounds covered value is
 * a real concurrency/crash bug in the fast path. Reproduce with the
 * {@code random seeds:} log line.
 */
public class CoveringIndexFastPathConcurrentReadFuzzTest extends AbstractFuzzTest {

    @Before
    public void enableCoveringCounters() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = true;
        resetCoveringCounters();
    }

    @After
    public void disableCoveringCounters() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = false;
        PostingIndexWriter.COVERING_FASTPATH_DISABLED = false;
    }

    @Test
    public void testFastPathConcurrentReadFuzz() throws Exception {
        runConcurrentReadFuzz(generateRandom(LOG), false);
    }

    @Test
    public void testFastPathConcurrentReadFuzzRegression() throws Exception {
        runConcurrentReadFuzz(generateRandom(LOG, 0x4d9a2e7f1c063bL, 0x81b5c39e7a204fL), false);
    }

    // Legacy 9.4.x on-disk head: seed a format-0 (aliased-footer) covering head,
    // then run the SAME concurrent covered-read + fast-lag block-apply load. The
    // O3-fallback guard must (a) never trip the OOB (the format-0 head is sent to
    // O3, not extended in place), (b) migrate the head to format 1 on the first
    // block-apply (the O3 reseal), (c) then fast-path (fastLag>0) race-free.
    @Test
    public void testFastPathConcurrentReadFuzzLegacyFormat0Regression() throws Exception {
        runConcurrentReadFuzz(generateRandom(LOG, 0x1f3c7a90e5d24bL, 0x77b1e6c0a4f293L), true);
    }

    @Test
    public void testFastPathConcurrentReadFuzzLegacyFormat0() throws Exception {
        runConcurrentReadFuzz(generateRandom(LOG), true);
    }

    private void resetCoveringCounters() {
        PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.set(0);
        PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.set(0);
        PostingIndexWriter.COVERING_AUTOSEAL_COUNT.set(0);
        PostingIndexWriter.COVERING_MAX_GENCOUNT_OBSERVED.set(0);
        PostingIndexWriter.COVERING_MAX_SEGCOUNT_OBSERVED.set(0);
    }

    private void runConcurrentReadFuzz(Rnd rnd, boolean legacyInitial) throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 10_000_000);
        setProperty(PropertyKey.CAIRO_WAL_APPLY_LOOK_AHEAD_TXN_COUNT, 2000);
        setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 600_000);

        final int symbolCardinality = 3 + rnd.nextInt(6);   // 3..8
        final int readerCount = 3 + rnd.nextInt(3);          // 3..5
        // Enough drained blocks to cross MAX_GEN_COUNT (128) so the auto-seal
        // fires mid-run while readers are active.
        final int rounds = 150 + rnd.nextInt(80);            // 150..229
        final int nullMod = 7 + rnd.nextInt(20);
        final long baseTs = 1_700_000_000_000_000L;
        // A cursor shared between the (optional) legacy format-0 seed and the
        // concurrent writer loop so ids/timestamps stay globally ascending.
        final long[] writeCursor = {0L, baseTs}; // {id, ts}

        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (value), value DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            if (legacyInitial) {
                // Establish a LEGACY (format-0) covering head on disk BEFORE the
                // concurrent load: several small ascending commits accumulate real
                // covering gens under the aliased-footer layout.
                PostingIndexWriter.FORCE_LEGACY_COVERING_FORMAT = true;
                try {
                    for (int b = 0; b < 6; b++) {
                        final int rows = 30 + rnd.nextInt(60);
                        final long step = 1 + rnd.nextInt(1000);
                        final long v0 = writeCursor[0];
                        final long startTs = writeCursor[1];
                        execute("INSERT INTO t SELECT (" + startTs + " + x * " + step + ")::TIMESTAMP AS ts,"
                                + " 'S' || ((" + v0 + " + x) % " + symbolCardinality + ") AS sym,"
                                + " CASE WHEN ((" + v0 + " + x) % " + nullMod + ") = 0 THEN cast(NULL AS DOUBLE)"
                                + " ELSE (" + v0 + " + x)::DOUBLE END AS value FROM long_sequence(" + rows + ")");
                        drainWalQueue();
                        writeCursor[0] = v0 + rows;
                        writeCursor[1] = startTs + (long) rows * step;
                    }
                } finally {
                    PostingIndexWriter.FORCE_LEGACY_COVERING_FORMAT = false;
                }
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

                            // (1) Covered == base at ONE snapshot: single statement, both
                            // branches share the reader txn -> any covered fragment that
                            // disagrees with the base column produces a non-empty diff.
                            final long diff = scalar(compiler, ctx,
                                    "SELECT count(*) FROM ("
                                            + "(SELECT ts, value FROM t WHERE sym = '" + sym + "')"
                                            + " EXCEPT "
                                            + "(SELECT /*+ no_covering */ ts, value FROM t WHERE sym = '" + sym + "'))");
                            if (diff != 0) {
                                throw new AssertionError("covered read disagrees with base column at snapshot for "
                                        + sym + ": EXCEPT returned " + diff + " rows");
                            }

                            // (2) Snapshot-safe bounds via the covering index.
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
                            // Read the ceiling AFTER the covered read: ids only grow and the
                            // ceiling is published before each drain, so a ceiling read after the
                            // snapshot is an upper bound on any value that snapshot can contain.
                            final long ceiling = idCeiling.get();
                            if (covMax > ceiling && ceiling > 0) {
                                throw new AssertionError("covered max(value)=" + covMax + " for " + sym
                                        + " exceeds the global id ceiling " + ceiling + " (rows past sealed coverage / garbage)");
                            }
                            if (covMin < 1 && total > 0 && covMin != Long.MAX_VALUE) {
                                throw new AssertionError("covered min(value)=" + covMin + " for " + sym
                                        + " is below 1 (stale/garbage covered fragment)");
                            }

                            // (3) Total row count is monotonic and never below the floor.
                            final long grandTotal = scalar(compiler, ctx, "SELECT count(*) FROM t");
                            if (grandTotal < floor) {
                                throw new AssertionError("total count " + grandTotal + " fell below committed floor " + floor);
                            }
                            if (grandTotal < prevTotal) {
                                throw new AssertionError("total count went backwards: " + prevTotal + " -> " + grandTotal);
                            }
                            prevTotal = grandTotal;

                            // (4) Exercise the covered record cursor (crash surface).
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
                }, "covering-fastpath-reader-" + r);
                readers[r].setDaemon(true);
                readers[r].start();
            }

            long id = writeCursor[0];
            long ts = writeCursor[1];
            try {
                for (int round = 0; round < rounds && bgError.get() == null; round++) {
                    final int txns = 2 + rnd.nextInt(5); // 2..6 -> multi-txn backlog -> block apply
                    for (int t = 0; t < txns; t++) {
                        final int rows = 10 + rnd.nextInt(200);
                        final long step = 1 + rnd.nextInt(1000);
                        final long v0 = id;
                        final long startTs = ts;
                        execute("INSERT INTO t SELECT (" + startTs + " + x * " + step + ")::TIMESTAMP AS ts,"
                                + " 'S' || ((" + v0 + " + x) % " + symbolCardinality + ") AS sym,"
                                + " CASE WHEN ((" + v0 + " + x) % " + nullMod + ") = 0 THEN cast(NULL AS DOUBLE)"
                                + " ELSE (" + v0 + " + x)::DOUBLE END AS value"
                                + " FROM long_sequence(" + rows + ")");
                        id += rows;
                        ts = startTs + (long) rows * step;
                    }
                    // Publish the ceiling BEFORE draining (ids only grow), the floor AFTER.
                    idCeiling.set(id);
                    drainWalQueue();
                    rowFloor.set(id);
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
            Assert.assertTrue("fast path must fire (fastLag=" + PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get() + ")",
                    PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get() > 0);
            if (legacyInitial) {
                // The O3-fallback guard must have sent the format-0 head through
                // O3 at least once (its reseal migrates the head to format 1),
                // after which block-applies fast-path (asserted above).
                Assert.assertTrue("legacy format-0 head must migrate via an O3 reseal (fullReseals="
                                + PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get() + ")",
                        PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get() > 0);
            }
        });
    }

    private long scalar(SqlCompiler compiler, SqlExecutionContextImpl ctx, String sql) throws Exception {
        try (RecordCursorFactory f = compiler.compile(sql, ctx).getRecordCursorFactory();
             RecordCursor cur = f.getCursor(ctx)) {
            return cur.hasNext() ? cur.getRecord().getLong(0) : 0;
        }
    }
}

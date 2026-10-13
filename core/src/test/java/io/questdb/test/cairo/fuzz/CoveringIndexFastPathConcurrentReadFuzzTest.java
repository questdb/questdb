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
        PostingIndexWriter.COVERING_SEAL_APPEND_DISABLED = false;
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

    // The gap the block-apply guard alone missed: a LEGACY format-0 covering
    // table driven PURELY by the SINGLE-TXN fast-lag path (one txn per drain, NOT
    // block-apply) under concurrent covered reads. Without the single-txn gate
    // guard this OOBs (the single-txn fast-lag extends the format-0 head in
    // place); with it, the format-0 head is sent to the full commit, migrates to
    // format 1 (fullReseals>0), and subsequent commits fast-path (fastLag>0)
    // race-free.
    @Test
    public void testSingleTxnConcurrentReadFuzzLegacyFormat0Regression() throws Exception {
        runConcurrentReadFuzz(generateRandom(LOG, 0x3ae7195c02f4d8L, 0x62c8b0e73915afL), true, true);
    }

    @Test
    public void testSingleTxnConcurrentReadFuzzLegacyFormat0() throws Exception {
        runConcurrentReadFuzz(generateRandom(LOG), true, true);
    }

    // The path the other tests miss: pre-existing WAL lag flushed BEFORE an O3
    // commit — the DIRECT applyFromWalLagToLastPartition(:10232) call that bypasses
    // the Possible-predicate gate. Interleaves out-of-order (O3) batches with a
    // non-zero WAL lag on a LEGACY format-0 covering head under concurrent covered
    // reads. Without the method-level guard the pre-existing-lag apply extends the
    // format-0 head in place -> OOB; with it, that call bails to O3 which migrates
    // the head to format 1.
    @Test
    public void testO3PreLagConcurrentReadFuzzLegacyFormat0Regression() throws Exception {
        // Pinned to the pre-deferral path: this variant's job is to prove the
        // publishToChain COW fires when a LEGACY format-0 head is extended in
        // place by the O3-merge index commit. The covered append path stops that
        // route from extending the head at all (the legacy head migrates via the
        // reseal instead), so leaving it on would quietly retire the COW coverage
        // rather than test it. The COW remains reachable from other extend sites.
        PostingIndexWriter.COVERING_SEAL_APPEND_DISABLED = true;
        runConcurrentReadFuzz(generateRandom(LOG, 0x5c1e83b7096d2fL, 0x4a90e2f1b7c358L), true, false, true);
    }

    @Test
    public void testO3PreLagConcurrentReadFuzzLegacyFormat0() throws Exception {
        // Pinned to the pre-deferral path: this variant's job is to prove the
        // publishToChain COW fires when a LEGACY format-0 head is extended in
        // place by the O3-merge index commit. The covered append path stops that
        // route from extending the head at all (the legacy head migrates via the
        // reseal instead), so leaving it on would quietly retire the COW coverage
        // rather than test it. The COW remains reachable from other extend sites.
        PostingIndexWriter.COVERING_SEAL_APPEND_DISABLED = true;
        runConcurrentReadFuzz(generateRandom(LOG), true, false, true);
    }

    private void resetCoveringCounters() {
        PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.set(0);
        PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.set(0);
        PostingIndexWriter.COVERING_AUTOSEAL_COUNT.set(0);
        PostingIndexWriter.COVERING_MAX_GENCOUNT_OBSERVED.set(0);
        PostingIndexWriter.COVERING_MAX_SEGCOUNT_OBSERVED.set(0);
        PostingIndexWriter.COVERING_COW_MIGRATE_COUNT.set(0);
    }

    private void runConcurrentReadFuzz(Rnd rnd, boolean legacyInitial) throws Exception {
        runConcurrentReadFuzz(rnd, legacyInitial, false, false);
    }

    private void runConcurrentReadFuzz(Rnd rnd, boolean legacyInitial, boolean singleTxn) throws Exception {
        runConcurrentReadFuzz(rnd, legacyInitial, singleTxn, false);
    }

    private void runConcurrentReadFuzz(Rnd rnd, boolean legacyInitial, boolean singleTxn, boolean o3Mode) throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 10_000_000);
        setProperty(PropertyKey.CAIRO_WAL_APPLY_LOOK_AHEAD_TXN_COUNT, 2000);
        setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 600_000);
        if (o3Mode) {
            // Keep rows in WAL lag (not fully committed) so an out-of-order batch
            // hits the O3 commit path with PRE-EXISTING lag -> the direct
            // applyFromWalLagToLastPartition(:10232) call.
            setProperty(PropertyKey.CAIRO_WAL_MAX_LAG_SIZE, 5 * 1024 * 1024);
            setProperty(PropertyKey.CAIRO_WAL_MAX_LAG_TXN_COUNT, 50);
        }

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
                            // The base arm is no_INDEX, not no_covering, and the diff is
                            // SYMMETRIC. no_covering only disables the covered read and
                            // leaves the index scan in place, so a posting that is
                            // MISSING drops the row from both arms and a one-way diff
                            // stays 0 - which is precisely what an unindexed tail looks
                            // like. Proven by mutation: breaking the seal's rebuild
                            // fallback left the old oracle green and fails this one.
                            final long diff = scalar(compiler, ctx,
                                    "SELECT count(*) FROM ("
                                            + "((SELECT ts, value FROM t WHERE sym = '" + sym + "')"
                                            + " EXCEPT "
                                            + "(SELECT /*+ no_index */ ts, value FROM t WHERE sym = '" + sym + "'))"
                                            + " UNION ALL "
                                            + "((SELECT /*+ no_index */ ts, value FROM t WHERE sym = '" + sym + "')"
                                            + " EXCEPT "
                                            + "(SELECT ts, value FROM t WHERE sym = '" + sym + "')))");
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
                    // singleTxn: exactly one txn per drain -> the single-txn
                    // fast-lag commit path (NOT block-apply).
                    final int txns = singleTxn ? 1 : (2 + rnd.nextInt(5)); // 2..6 -> multi-txn backlog -> block apply
                    for (int t = 0; t < txns; t++) {
                        final int rows = 10 + rnd.nextInt(200);
                        final long step = 1 + rnd.nextInt(1000);
                        final long v0 = id;
                        // o3Mode: ~1 in 4 batches back-dates below the current max
                        // (out-of-order), so the drain takes the O3 commit path with
                        // the ascending rows still sitting in WAL lag -> the
                        // pre-existing-lag-before-O3 apply (:10232). value stays == id
                        // (ascending), so the reader's monotonic/ceiling checks hold.
                        final boolean o3 = o3Mode && rnd.nextInt(4) == 0 && ts > baseTs + 5_000_000L;
                        final long startTs = o3 ? (ts - (long) (1 + rnd.nextInt(4)) * 1_000_000L) : ts;
                        execute("INSERT INTO t SELECT (" + startTs + " + x * " + step + ")::TIMESTAMP AS ts,"
                                + " 'S' || ((" + v0 + " + x) % " + symbolCardinality + ") AS sym,"
                                + " CASE WHEN ((" + v0 + " + x) % " + nullMod + ") = 0 THEN cast(NULL AS DOUBLE)"
                                + " ELSE (" + v0 + " + x)::DOUBLE END AS value"
                                + " FROM long_sequence(" + rows + ")");
                        id += rows;
                        // Keep the ascending cursor monotonic even after an o3 dip,
                        // so the bulk of the stream stays in-order (builds lag).
                        ts = Math.max(ts, startTs + (long) rows * step);
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
                // The legacy format-0 head must migrate to format 1 -- via an O3
                // reseal (the guard paths) and/or the publishToChain COW (the
                // unguardable O3-merge / syncColumns extend paths). Either way no
                // format-0 head is ever extended in place (asserted by the readers'
                // clean covered scans above).
                Assert.assertTrue("legacy format-0 head must migrate (fullReseals="
                                + PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get()
                                + ", cowMigrates=" + PostingIndexWriter.COVERING_COW_MIGRATE_COUNT.get() + ")",
                        PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get() > 0
                                || PostingIndexWriter.COVERING_COW_MIGRATE_COUNT.get() > 0);
                if (o3Mode) {
                    // The O3 partition-merge index commit (o3ConsumePartitionUpdates
                    // -> o3CopySafe -> commit) extends the legacy head in place with
                    // no call-site guard able to intercept it; the publishToChain COW
                    // migrates it there. Prove the COW actually fires on that path.
                    Assert.assertTrue("O3-merge path must COW-migrate the legacy head (cowMigrates="
                                    + PostingIndexWriter.COVERING_COW_MIGRATE_COUNT.get() + ")",
                            PostingIndexWriter.COVERING_COW_MIGRATE_COUNT.get() > 0);
                }
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

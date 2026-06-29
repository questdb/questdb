/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.PropertyKey;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.lv.LiveViewRecordCursor;
import io.questdb.griffin.engine.lv.LiveViewRecordCursorFactory;
import io.questdb.mp.Job;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Differential fuzz test for live views.
 * <p>
 * The premise of an incremental-maintenance engine is that the incrementally
 * materialized state equals a from-scratch recompute over the base table. This
 * test verifies exactly that invariant: it drives randomized inserts (in-order
 * and out-of-order), simulated restarts, and optional backfill at the base
 * table, then cross-checks the live view's contents against the same window
 * query recomputed directly over the base table.
 * <p>
 * <b>Why the oracle is sound.</b> Window functions are order-dependent, so a
 * row-level comparison is only meaningful when both the incremental and the
 * batch path agree on a total ordering of the input rows. Two design choices
 * guarantee that:
 * <ul>
 *   <li><b>Strictly-unique timestamps.</b> Every generated row has a distinct,
 *   strictly-increasing timestamp. The base table's designated timestamp is
 *   therefore a total order, and {@code OVER (ORDER BY ts ...)} - as well as the
 *   natural ts scan order used by {@code OVER ()} - is unambiguous. Duplicate
 *   timestamps would let the two paths break ties differently, which is not a
 *   correctness bug but would still fail a row-level diff. So out-of-order
 *   ingestion is produced by <i>shuffling insertion order across commits</i>,
 *   never by colliding timestamps.</li>
 *   <li><b>Grammar-legal, deterministic window shapes.</b> Only bounded
 *   {@code ROWS BETWEEN N PRECEDING AND CURRENT ROW} frames and ranking
 *   {@code OVER ()} are used. Live views reject unbounded aggregate frames
 *   without an ANCHOR clause, and bounded frames over a unique-ts ordering are
 *   deterministic functions of the row set.</li>
 * </ul>
 * The comparison normalizes row order with {@code ORDER BY 1} (the unique
 * timestamp) and uses {@code genericStringMatch} so a SYMBOL passthrough that
 * the materializer stores as STRING still compares by value.
 */
public class LiveViewFuzzTest extends AbstractCairoTest {

    // Variants 0..4 and 6 are ORDER BY ts bounded-frame aggregates over
    // LONG/DOUBLE columns (sum/max/first_value/count/avg/min); the decimal variant
    // (DECIMAL_VARIANT) is the same bounded-frame shape over a DECIMAL column (a
    // random width + aggregate per run). Their output is a total deterministic
    // function of the (unique-ts) row set, so the recompute oracle holds under any
    // ingestion order, including O3 and restart.
    // Variant 5 is ranking row_number() OVER () with no ORDER BY. Its numbering
    // follows scan order; the incremental engine always (re)scans the base in
    // ts-ascending order - forward-append in ts order, head-miss replay from the
    // lower bound, head-hit replay continuing from the checkpoint's ts-ordered
    // count - so the numbering matches a batch recompute (which also scans the
    // designated timestamp ascending). All variants are fuzzed under O3, restart,
    // and BACKFILL in any combination.
    // FLUSH EVERY rate-limits LV commits by wall clock: a refresh within
    // flushEveryMicros of the previous commit is deferred. Tests drive a
    // controllable clock (currentMicros) and advance it past this interval
    // before each refresh so flushes are deterministic, not wall-clock racy.
    private static final long CLOCK_ADVANCE_MICROS = 250_000; // > FLUSH EVERY 100ms
    // The decimal variant (the last variant) exercises the migrated DECIMAL
    // aggregate window family over a bounded ROWS frame. Each run picks a random
    // storage width (one of the six DECIMAL precisions below, which select the
    // six Decimal8/16/32/64/128/256 widths) and a random aggregate. The recompute
    // oracle holds exactly as it does for the LONG/DOUBLE aggregates: a bounded
    // frame over a unique-ts total order is a deterministic function of the row
    // set, so the incremental view must equal the from-scratch recompute.
    private static final int DECIMAL_FUNC_COUNT = 6; // sum, max, min, first_value, avg, avg(d, scale)
    private static final int[] DECIMAL_PRECISION = {2, 4, 9, 18, 38, 60};
    private static final int[] DECIMAL_SCALE = {0, 0, 3, 2, 6, 0};
    private static final int DECIMAL_VARIANT = 7;
    private static final int MAX_FRAME = 20;
    private static final String[] SYMBOLS = {
            "AA", "BB", "CC", "DD", "EE", "FF", "GG", "HH",
            "II", "JJ", "KK", "LL", "MM", "NN", "OO", "PP"
    };

    @Test
    public void testFuzzBackfill() throws Exception {
        // BACKFILL + O3: the head-miss REPLACE_RANGE [replayMinTs, +inf) re-merges into the
        // multi-partition backfilled data. This used to corrupt the view through a storage-engine
        // replace-mode bug (a replace appending partitions above the last partition left the
        // writer's active columns stale, and the next replace reused them); fixed in TableWriter,
        // regression: WalWriterReplaceRangeTest.testReplaceRangeAddsPartitionsAboveLastThenRebuilds.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            for (int v = 0; v < variantCount(); v++) {
                runFuzz(rnd, v, 140, true, rnd.nextBoolean(), true, rnd.nextBoolean());
            }
        });
    }

    @Test
    public void testFuzzInMemReadBack() throws Exception {
        // Mode B read-back: a row_number() view (so SELECT * FROM lv routes through
        // the in-mem tier), with or without a SYMBOL passthrough (chosen per run),
        // fuzzed under O3 + optional restart + optional backfill. After quiescence
        // the read-back is cross-checked three ways: it equals the from-scratch
        // recompute (the standard oracle), Mode B is confirmed actually engaged,
        // and the Mode B result is byte-identical to the forced disk-only path. The
        // SYMBOL passthrough form exercises the LV-space symbol-id translation
        // (segment-local churn across commits) through Mode B.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(() -> {
            final boolean restart = rnd.nextBoolean();
            final boolean backfill = rnd.nextBoolean();
            runFuzz(rnd, 0, 120 + rnd.nextInt(280), true, restart, backfill, true, true);
        });
    }

    @Test
    public void testFuzzInOrder() throws Exception {
        // In-order ingestion: the happy incremental path, no head replay.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            for (int v = 0; v < variantCount(); v++) {
                runFuzz(rnd, v, 160, false, false, false, rnd.nextBoolean());
            }
        });
    }

    @Test
    public void testFuzzLeadReadBack() throws Exception {
        // Mode A read-back: after the randomized O3 + optional backfill churn the
        // harness builds a deterministic un-flushed lead on top of the applied
        // state (a forward batch refreshed into the in-mem tier but held below the
        // FLUSH EVERY cadence, so it never reaches disk). The final read then
        // routes through the lead and is cross-checked three ways: the tier-on read
        // serves exactly the lead and equals the from-scratch recompute, while the
        // forced disk-only fallback serves only the applied prefix (the recompute
        // with the lead trimmed off). A SYMBOL passthrough is added on half the
        // runs, exercising the eager-interned lead's id resolution through Mode A.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(() -> {
            final boolean backfill = rnd.nextBoolean();
            runFuzz(rnd, 0, 120 + rnd.nextInt(280), true, false, backfill, true, false, true);
        });
    }

    @Test
    public void testFuzzLeadReadBackCrashRecovery() throws Exception {
        // Mode A read-back with a crash-before-flush twist: the harness builds the
        // un-flushed lead, verifies the Mode A cross-checks, then simulates a crash
        // (registry clear + rebuild from disk, which drops the RAM-only lead) and a
        // restart that recovers the lead by draining the retained base WAL forward
        // (lvConsumedSeqTxn == applied keeps the lead's base rows). The same Mode A
        // cross-checks must hold on the recovered lead. restart=true drives the
        // post-build crash; the per-commit restarts inside runFuzz add further churn.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            final boolean backfill = rnd.nextBoolean();
            runFuzz(rnd, 0, 120 + rnd.nextInt(220), true, true, backfill, true, false, true);
        });
    }

    @Test
    public void testFuzzO3() throws Exception {
        // Out-of-order ingestion across commits, refreshing between each commit
        // so late rows force head replay against already-materialized state.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            for (int v = 0; v < variantCount(); v++) {
                runFuzz(rnd, v, 160, true, false, false, rnd.nextBoolean());
            }
        });
    }

    @Test
    public void testFuzzO3WithRestart() throws Exception {
        // O3 plus simulated restarts (registry clear + rebuild from disk) at
        // quiescent points, with checkpoints written every refresh.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            for (int v = 0; v < variantCount(); v++) {
                runFuzz(rnd, v, 140, true, true, false, rnd.nextBoolean());
            }
        });
    }

    @Test
    public void testFuzzRandomized() throws Exception {
        // A single fully-random configuration per CI run; seed is logged for
        // reproduction. Explores combinations the pinned tests do not.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(8));
        assertMemoryLeak(() -> {
            final boolean o3 = rnd.nextBoolean();
            // BACKFILL now combines with O3 (the merge bug forcing them apart is fixed).
            final boolean backfill = rnd.nextBoolean();
            final int variant = rnd.nextInt(variantCount());
            final boolean restart = o3 && rnd.nextBoolean();
            runFuzz(rnd, variant, 80 + rnd.nextInt(520), o3, restart, backfill, rnd.nextBoolean());
        });
    }

    @Test
    public void testFuzzWidened() throws Exception {
        // Concentrated heavy corner: larger datasets with O3 + restart + backfill +
        // in-mem all on together, across every variant. Per-run symbol cardinality
        // and partition spread (chosen inside runFuzz) still vary, so a batch of
        // runs samples the high-cardinality / many-partition corners the pinned
        // tests rarely hit all at once. Seed is logged for reproduction.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(() -> {
            for (int v = 0; v < variantCount(); v++) {
                runFuzz(rnd, v, 300 + rnd.nextInt(400), true, true, true, true);
            }
        });
    }

    // Mode A read-back cross-check: with a known un-flushed lead resident, the
    // tier-on read must serve exactly the lead and equal the from-scratch
    // recompute, while the forced disk-only fallback serves only the applied
    // prefix (the recompute with the trailing lead rows trimmed). All three sides
    // share the native ts-ascending order, so the comparison is byte-for-byte.
    // Run single-threaded after the worker is freed and the lead is built.
    private static void assertLeadReadBack(String viewSql) throws SqlException {
        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        final long leadRows = instance.getLeadRowCount();
        Assert.assertTrue("Mode A read-back needs a non-empty lead", leadRows > 0);

        // The tier routes Mode A and serves exactly the un-flushed lead.
        Assert.assertEquals("the cursor must serve exactly the instance's lead",
                leadRows, leadRowsServedFor("SELECT * FROM lv"));

        // Tier-on content equals the recompute (both native ts-ascending order).
        StringSink lvOut = new StringSink();
        printSql("SELECT * FROM lv", lvOut);
        StringSink recompute = new StringSink();
        printSql(viewSql, recompute);
        Assert.assertEquals("Mode A read must equal the recompute", recompute.toString(), lvOut.toString());

        // Disk-only fallback content equals the applied prefix (recompute minus the lead).
        StringSink diskOnly = new StringSink();
        printDiskOnly("SELECT * FROM lv", diskOnly);
        Assert.assertEquals("disk-only read must equal the applied prefix",
                dropTrailingDataRows(recompute.toString(), leadRows), diskOnly.toString());
    }

    // Confirms SELECT * FROM lv actually routes through Mode B (the in-mem tier),
    // not disk-only. Opens the inner LiveViewRecordCursor directly (unwrapping any
    // QueryProgress wrapper), drains it, and asserts the fence engaged and the tier
    // served rows. Run single-threaded after quiescence; the top-up cycle guarantees
    // the slot is populated.
    private static void assertModeBEngaged() throws SqlException {
        try (RecordCursorFactory factory = select("SELECT * FROM lv")) {
            RecordCursorFactory f = factory;
            while (f != null && !(f instanceof LiveViewRecordCursorFactory)) {
                f = f.getBaseFactory();
            }
            Assert.assertNotNull("expected a LiveViewRecordCursorFactory in the plan", f);
            try (LiveViewRecordCursor cursor = (LiveViewRecordCursor) f.getCursor(sqlExecutionContext)) {
                StringSink sink = new StringSink();
                println(f.getMetadata(), cursor, sink);
                Assert.assertTrue("read-back must route through Mode B", cursor.isRoutingEligible());
                Assert.assertTrue("Mode B must serve in-mem rows", cursor.inMemRowsServed() > 0);
            }
        }
    }

    // Runs the SELECT with the tier on (Mode B) and then with the fence forced off
    // (disk-only, achieved by mismatching both slots' stamps) and asserts the two
    // outputs are byte-identical. Restores the stamps afterwards. Mirrors the
    // differential oracle in LiveViewInMemReadTest; safe single-threaded only.
    private static void assertModeBMatchesDiskOnly(String sql) throws SqlException {
        StringSink modeB = new StringSink();
        printSql(sql, modeB);

        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        LiveViewInMemoryTier tier = instance.getInMemoryTier();
        Assert.assertNotNull(tier);
        long s0 = tier.getSlot(0).lvSeqTxn();
        long s1 = tier.getSlot(1).lvSeqTxn();
        tier.getSlot(0).setLvSeqTxn(mismatch(s0));
        tier.getSlot(1).setLvSeqTxn(mismatch(s1));
        StringSink diskOnly = new StringSink();
        try {
            printSql(sql, diskOnly);
        } finally {
            tier.getSlot(0).setLvSeqTxn(s0);
            tier.getSlot(1).setLvSeqTxn(s1);
        }
        Assert.assertEquals("Mode B vs disk-only mismatch for: " + sql, diskOnly.toString(), modeB.toString());
    }

    // Boundaries [0, ..., len] splitting a segment of length len into 1..~10
    // contiguous commits.
    private static int[] commitBounds(Rnd rnd, int len) {
        final int commits = Math.max(1, Math.min(len, 2 + rnd.nextInt(9)));
        final int[] b = new int[commits + 1];
        for (int c = 0; c <= commits; c++) {
            b[c] = (int) ((long) c * len / commits);
        }
        return b;
    }

    // Renders one INSERT literal for a DECIMAL(precision, scale) value: an
    // occasional NULL, else a signed value with exactly `scale` fractional digits
    // and the mandatory 'm' suffix. The unscaled magnitude is capped at 10^15 - 1
    // (well within a long) and, for narrow precisions, at the column's own range,
    // so the value always fits the column. sum() widens its result type, so a
    // bounded frame of up to MAX_FRAME+1 such values never overflows.
    private static String decimalLiteral(Rnd rnd, int precision, int scale) {
        if (rnd.nextInt(20) == 0) {
            return "null";
        }
        long max = 1L;
        for (int i = 0, lim = Math.min(precision, 15); i < lim; i++) {
            max *= 10L;
        }
        max -= 1;
        final long mag = (long) (rnd.nextDouble() * (max + 1)); // [0, max]
        final StringBuilder sb = new StringBuilder();
        if (mag != 0 && rnd.nextBoolean()) {
            sb.append('-');
        }
        if (scale == 0) {
            sb.append(mag);
        } else {
            String digits = Long.toString(mag);
            while (digits.length() <= scale) {
                digits = '0' + digits;
            }
            final int split = digits.length() - scale;
            sb.append(digits, 0, split).append('.').append(digits, split, digits.length());
        }
        return sb.append('m').toString();
    }

    // Builds the projection for the decimal variant: a passthrough of the DECIMAL
    // column d plus one migrated aggregate over the same bounded ROWS frame the
    // LONG/DOUBLE variants use. last_value and nth_value are omitted: a bounded
    // frame ending at CURRENT ROW routes last_value to the un-migrated
    // IncludeCurrent shape (rejected at CREATE), and nth_value needs a distinct
    // frame; both are covered byte-exact by the smoke test instead.
    private static String decimalProjection(int func, int n, int targetScale) {
        final String frame = "PARTITION BY sym ORDER BY ts ROWS BETWEEN " + n + " PRECEDING AND CURRENT ROW";
        final String agg = switch (func) {
            case 0 -> "sum(d)";
            case 1 -> "max(d)";
            case 2 -> "min(d)";
            case 3 -> "first_value(d)";
            case 4 -> "avg(d)";
            case 5 -> "avg(d, " + targetScale + ")";
            default -> throw new IllegalArgumentException("decimalFunc=" + func);
        };
        return "ts, sym, d, " + agg + " OVER (" + frame + ") AS v";
    }

    private static boolean drainJob(Job job) {
        boolean any = false;
        for (int i = 0; i < 64 && job.run(); i++) {
            any = true;
        }
        return any;
    }

    // Drops the last `count` data rows from a printSql output (a header line plus
    // one '\n'-terminated line per row), keeping the header. Used to turn the full
    // recompute into the applied prefix (recompute minus the un-flushed lead) the
    // disk-only fallback must match.
    private static String dropTrailingDataRows(String printed, long count) {
        if (count <= 0) {
            return printed;
        }
        // split(-1) keeps the trailing empty token after the final '\n', so for a
        // header + N data rows the array is [header, r1, ..., rN, ""].
        final String[] lines = printed.split("\n", -1);
        final int dataRows = lines.length - 2;
        final int keep = (int) (dataRows - count);
        Assert.assertTrue("cannot drop more rows than present", keep >= 0);
        final StringSink sb = new StringSink();
        sb.put(lines[0]).put('\n');
        for (int i = 1; i <= keep; i++) {
            sb.put(lines[i]).put('\n');
        }
        return sb.toString();
    }

    // Opens the inner LiveViewRecordCursor for the SELECT (unwrapping any
    // QueryProgress wrapper), drains it, asserts the read routed through the tier
    // (Mode A), and returns the number of un-flushed lead rows it served.
    private static long leadRowsServedFor(String sql) throws SqlException {
        try (RecordCursorFactory factory = select(sql)) {
            RecordCursorFactory f = factory;
            while (f != null && !(f instanceof LiveViewRecordCursorFactory)) {
                f = f.getBaseFactory();
            }
            Assert.assertNotNull("expected a LiveViewRecordCursorFactory in the plan", f);
            try (LiveViewRecordCursor cursor = (LiveViewRecordCursor) f.getCursor(sqlExecutionContext)) {
                StringSink sink = new StringSink();
                println(f.getMetadata(), cursor, sink);
                Assert.assertTrue("Mode A read-back must route through the tier", cursor.isRoutingEligible());
                return cursor.leadRowsServed();
            }
        }
    }

    // Maps a slot stamp to a value the disk reader can never report, forcing the
    // fence off so the read serves disk-only. LONG_NULL slots map to 1.
    private static long mismatch(long seqTxn) {
        return seqTxn == Numbers.LONG_NULL ? 1 : seqTxn + 1_000_000;
    }

    // Prints the SELECT with the seqTxn fence forced off (both slot stamps
    // mismatched), so the cursor falls back to the disk-only path and serves only
    // the applied prefix. Restores the stamps afterwards.
    private static void printDiskOnly(String sql, StringSink sink) throws SqlException {
        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        LiveViewInMemoryTier tier = instance.getInMemoryTier();
        Assert.assertNotNull(tier);
        long s0 = tier.getSlot(0).lvSeqTxn();
        long s1 = tier.getSlot(1).lvSeqTxn();
        tier.getSlot(0).setLvSeqTxn(mismatch(s0));
        tier.getSlot(1).setLvSeqTxn(mismatch(s1));
        try {
            printSql(sql, sink);
        } finally {
            tier.getSlot(0).setLvSeqTxn(s0);
            tier.getSlot(1).setLvSeqTxn(s1);
        }
    }

    // Returns the projection (SELECT list) for the given non-decimal window-query
    // variant (the decimal variant routes to decimalProjection instead). Every
    // shape is grammar-legal in a live view and deterministic under a
    // unique-timestamp total order. The fuzzed set is exactly the window shapes
    // that carry the incremental-snapshot contract: PARTITION BY rows-frame
    // sum/max/min/first_value/count/avg, plus ranking OVER (). Un-partitioned
    // aggregate windows and last_value over a CURRENT ROW frame are rejected at
    // CREATE (no snapshot support), so they are not fuzzed here. min reuses Max's
    // migrated MaxMinOver* classes, so it carries the same snapshot contract. N is
    // the bounded-frame radius.
    private static String projection(int variant, int n) {
        final String frame = "PARTITION BY sym ORDER BY ts ROWS BETWEEN " + n + " PRECEDING AND CURRENT ROW";
        return switch (variant) {
            case 0 -> "ts, sym, i, sum(i) OVER (" + frame + ") AS v";
            case 1 -> "ts, sym, i, max(i) OVER (" + frame + ") AS v";
            case 2 -> "ts, sym, i, first_value(i) OVER (" + frame + ") AS v";
            case 3 -> "ts, sym, count() OVER (" + frame + ") AS v";
            case 4 -> "ts, sym, x, avg(x) OVER (" + frame + ") AS v";
            case 5 -> "ts, sym, row_number() OVER () AS rn";
            case 6 -> "ts, sym, i, min(i) OVER (" + frame + ") AS v";
            default -> throw new IllegalArgumentException("variant=" + variant);
        };
    }

    // Row indices [lo, lo+1, ..., hi-1], shuffled in place when o3 is set so
    // insertion order diverges from ts order (the source of out-of-order writes).
    private static int[] segmentOrder(Rnd rnd, int lo, int hi, boolean o3) {
        final int[] a = new int[hi - lo];
        for (int k = 0; k < a.length; k++) {
            a[k] = lo + k;
        }
        if (o3) {
            for (int k = a.length - 1; k > 0; k--) {
                int j = rnd.nextInt(k + 1);
                int tmp = a[k];
                a[k] = a[j];
                a[j] = tmp;
            }
        }
        return a;
    }

    private static int variantCount() {
        return DECIMAL_VARIANT + 1;
    }

    // Builds a deterministic un-flushed lead on top of the already-applied state:
    // pins the flush clock to the current (un-advanced) test clock so the next
    // refresh publishes the inserted rows into the in-mem tier as the lead without
    // crossing FLUSH EVERY, then refreshes a forward batch above the global max ts.
    // Disk keeps only the applied prefix; the tier leads it by these two rows. The
    // clock is never advanced, so the lead stays un-flushed.
    private void buildLeadForReadBack(LiveViewRefreshJob job, long maxTs) throws Exception {
        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        instance.setLastFlushTimeUs(currentMicros);
        execute("INSERT INTO base (ts, sym, i, x) VALUES ("
                + (maxTs + 1) + "::timestamp, 'AA', 1, 1.0), ("
                + (maxTs + 2) + "::timestamp, 'AA', 2, 2.0)");
        drainWalQueue();
        drainJob(job); // refresh only -> lead in RAM (clock not advanced past FLUSH EVERY)
    }

    // Drives the named view's backfill sweep to completion on the caller's job,
    // re-fetching the instance each pass so it survives a restart, then applies
    // the LV WAL. Mirrors the smoke test helper.
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

    // Pumps the refresh job until no further LV WAL work is produced, advancing
    // the clock each pass so deferred flushes land, and applying the LV's own
    // WAL after each burst.
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

    private void insertCommit(
            StringSink sink,
            int[] order,
            int from,
            int to,
            long[] tsv,
            int[] symIdx,
            long[] iv,
            double[] xv,
            boolean[] xNull,
            String[] dLit
    ) throws Exception {
        if (from >= to) {
            return;
        }
        sink.clear();
        sink.put("INSERT INTO base (ts, sym, i, x");
        if (dLit != null) {
            sink.put(", d");
        }
        sink.put(") VALUES ");
        for (int r = from; r < to; r++) {
            int k = order[r];
            if (r > from) {
                sink.put(',');
            }
            sink.put('(').put(tsv[k]).put("::timestamp,");
            if (symIdx[k] < 0) {
                sink.put("null,");
            } else {
                sink.put('\'').put(SYMBOLS[symIdx[k]]).put("',");
            }
            if (iv[k] == Numbers.LONG_NULL) {
                sink.put("null,");
            } else {
                sink.put(iv[k]).put(',');
            }
            if (xNull[k]) {
                sink.put("null");
            } else {
                sink.put(xv[k]);
            }
            if (dLit != null) {
                sink.put(',').put(dLit[k]);
            }
            sink.put(')');
        }
        execute(sink);
    }

    // One refresh cycle past the FLUSH EVERY rate-limit: advances the clock so
    // the commit is not deferred, runs the job, and applies the LV WAL.
    private void refreshCycle(LiveViewRefreshJob job) {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        drainJob(job);
        drainWalQueue();
    }

    // Simulates a crash-before-flush: drops the in-memory registry (losing the
    // RAM-only lead) and rebuilds from on-disk state, then a restart that recovers
    // the lead by draining the retained base WAL forward. The restored instance's
    // flush clock is pinned so drain-forward rebuilds the lead without re-flushing
    // it (lvConsumedSeqTxn == applied retained the lead's base rows). One drain pass
    // restores the head .cp, replays to the applied point, and rebuilds the lead.
    private void restartAndRecoverLead() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
        LiveViewInstance restored = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(restored);
        restored.setLastFlushTimeUs(currentMicros);
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            drainJob(job);
        }
        drainWalQueue();
    }

    private void runFuzz(
            Rnd rnd,
            int variant,
            int rowCount,
            boolean o3,
            boolean restart,
            boolean backfill,
            boolean inMemory
    ) throws Exception {
        runFuzz(rnd, variant, rowCount, o3, restart, backfill, inMemory, false, false);
    }

    private void runFuzz(
            Rnd rnd,
            int variant,
            int rowCount,
            boolean o3,
            boolean restart,
            boolean backfill,
            boolean inMemory,
            boolean inMemReadBack
    ) throws Exception {
        runFuzz(rnd, variant, rowCount, o3, restart, backfill, inMemory, inMemReadBack, false);
    }

    private void runFuzz(
            Rnd rnd,
            int variant,
            int rowCount,
            boolean o3,
            boolean restart,
            boolean backfill,
            boolean inMemory,
            boolean inMemReadBack,
            boolean leadReadBack
    ) throws Exception {
        // Drive a controllable clock so FLUSH EVERY flush gating is deterministic.
        // Pin "now" a day BEFORE the data start (2026-01-01). A non-backfill
        // view's lower bound is the wall-clock CREATE moment, and O3 head-miss
        // replay only re-emits base rows at or above that floor - so the clock
        // must sit below every data timestamp or the replay would drop rows the
        // recompute keeps. The per-cycle clock advance (250ms) stays far under
        // the one-day gap across a whole test run.
        if (currentMicros < 0) {
            setCurrentMicros(MicrosTimestampDriver.floor("2025-12-31T00:00:00.000000Z"));
        }

        final int n = 1 + rnd.nextInt(MAX_FRAME);
        // Per-run partition cardinality: 1..16 distinct symbols (plus an occasional
        // NULL symbol partition). High cardinality means many window partitions,
        // each with few rows, stressing the partition-map snapshot/restore path.
        final int symCount = 1 + rnd.nextInt(SYMBOLS.length);
        // Per-run partition spread along the time axis (see the generation loop):
        // tight (sub-5s steps, rare day jumps) .. wide (sub-15min steps, frequent
        // day jumps), so a run's data spans from one tightly-packed partition to a
        // few dozen. The wide regime stresses O3 / REPLACE_RANGE across many
        // partition boundaries (the Finding 2 territory).
        final int stepMode = rnd.nextInt(3);
        final int baseStepMax = stepMode == 0 ? 5_000_000 : stepMode == 1 ? 60_000_000 : 900_000_000;
        final int dayJumpEvery = stepMode == 0 ? 20 : 12;
        final boolean withWhere = rnd.nextInt(3) == 0;
        // inMemReadBack forces a row_number() output so SELECT * FROM lv routes
        // through the in-mem tier (Mode B). Half the read-back runs add a SYMBOL
        // passthrough: the refresh worker stores LV-table-space symbol ids, so
        // the random per-commit symbol churn (segment-local ids diverge from
        // LV-space ids) is exercised through Mode B under O3 / restart / backfill.
        // The decimal family always carries a SYMBOL passthrough, so it never
        // combines with the read-back path.
        final boolean symbolReadBack = (inMemReadBack || leadReadBack) && rnd.nextBoolean();
        final boolean isDecimal = !inMemReadBack && !leadReadBack && variant == DECIMAL_VARIANT;
        final boolean inMem = inMemory || inMemReadBack || leadReadBack;
        final int decimalWidth = isDecimal ? rnd.nextInt(DECIMAL_PRECISION.length) : -1;
        final int decimalPrecision = isDecimal ? DECIMAL_PRECISION[decimalWidth] : 0;
        final int decimalScale = isDecimal ? DECIMAL_SCALE[decimalWidth] : 0;
        final String decimalType = isDecimal ? "DECIMAL(" + decimalPrecision + ", " + decimalScale + ")" : null;
        final int decimalFunc = isDecimal ? rnd.nextInt(DECIMAL_FUNC_COUNT) : -1;
        // Target scale for the rescale form avg(d, ts); >= input scale keeps the
        // rescaled precision (= precision - scale + targetScale) within bounds.
        final int rescaleTargetScale = isDecimal ? decimalScale + rnd.nextInt(4) : 0;
        final String projection;
        if (inMemReadBack || leadReadBack) {
            // Fixed-width identity output: SELECT * FROM lv is then a full-schema
            // projection the in-mem tier can serve, so the read routes through
            // the tier (Mode B subset, or Mode A with an un-flushed lead) instead
            // of disk-only. The optional SYMBOL passthrough is resolved against the
            // disk reader's symbol table via the LV-space ids the refresh worker
            // stored, plus the per-tier symbol cache for any lead-only value.
            projection = symbolReadBack
                    ? "ts, sym, i, row_number() OVER () AS rn"
                    : "ts, i, row_number() OVER () AS rn";
        } else if (isDecimal) {
            projection = decimalProjection(decimalFunc, n, rescaleTargetScale);
        } else {
            projection = projection(variant, n);
        }
        final String viewSql = "SELECT " + projection + " FROM base" + (withWhere ? " WHERE i > 0" : "");
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + (inMem ? "IN MEMORY 60s " : "")
                + (backfill ? "BACKFILL " : "")
                + "AS " + viewSql;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE"
                + (isDecimal ? ", d " + decimalType : "")
                + ") TIMESTAMP(ts) PARTITION BY DAY WAL");

        LOG.info().$("LV fuzz: variant=").$(variant).$(", rows=").$(rowCount)
                .$(", n=").$(n).$(", symCount=").$(symCount).$(", stepMode=").$(stepMode)
                .$(", o3=").$(o3).$(", restart=").$(restart)
                .$(", backfill=").$(backfill).$(", inMem=").$(inMem)
                .$(", inMemReadBack=").$(inMemReadBack).$(", leadReadBack=").$(leadReadBack)
                .$(", symbolReadBack=").$(symbolReadBack)
                .$(", where=").$(withWhere).$(", decimalType=").$(decimalType)
                .$(", sql=").$(viewSql).$();

        // Generate the logical dataset: strictly-unique, strictly-increasing
        // timestamps; random symbols and values with occasional NULLs.
        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        final boolean[] xNull = new boolean[rowCount];
        final String[] dLit = isDecimal ? new String[rowCount] : null;
        final int maxDayJumps = 30; // cap partition spread so a wide-step run stays fast
        int dayJumps = 0;
        long ts = MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z");
        for (int k = 0; k < rowCount; k++) {
            ts += 1 + rnd.nextInt(baseStepMax); // keeps ts strictly increasing
            if (dayJumps < maxDayJumps && rnd.nextInt(dayJumpEvery) == 0) {
                ts += 86_400_000_000L; // full-day jump to span more partitions
                dayJumps++;
            }
            tsv[k] = ts;
            symIdx[k] = rnd.nextInt(20) == 0 ? -1 : rnd.nextInt(symCount); // -1 => NULL symbol
            iv[k] = rnd.nextInt(20) == 0 ? Numbers.LONG_NULL : (rnd.nextInt(2001) - 1000);
            xNull[k] = rnd.nextInt(20) == 0;
            xv[k] = rnd.nextDouble() * 1000.0;
            if (isDecimal) {
                dLit[k] = decimalLiteral(rnd, decimalPrecision, decimalScale);
            }
        }

        // Backfill captures pre-CREATE history. Put the EARLIEST rows (by ts)
        // before CREATE so the backfill floor sits at the global min ts and no
        // post-CREATE O3 row falls below it - such a row would be rejected and
        // diverge from the recompute. Non-backfill: everything lands post-CREATE.
        final int preCount = backfill ? rnd.nextInt(rowCount + 1) : 0;

        final StringSink sink = new StringSink();
        LiveViewRefreshJob job = null;
        try {
            // Pre-CREATE history: earliest segment [0, preCount), inserted in
            // random commit order for O3.
            if (preCount > 0) {
                final int[] preOrder = segmentOrder(rnd, 0, preCount, o3);
                final int[] cb = commitBounds(rnd, preOrder.length);
                for (int c = 0; c + 1 < cb.length; c++) {
                    insertCommit(sink, preOrder, cb[c], cb[c + 1], tsv, symIdx, iv, xv, xNull, dLit);
                    drainWalQueue();
                }
            }

            execute(createSql);
            job = new LiveViewRefreshJob(0, engine, 1);

            if (backfill) {
                driveBackfillToCompletion(job, "lv");
            }

            // Post-CREATE: segment [preCount, rowCount), refreshed per commit so a
            // later (older-ts) commit is genuinely O3 vs the materialized state.
            if (preCount < rowCount) {
                final int[] postOrder = segmentOrder(rnd, preCount, rowCount, o3);
                final int[] cb = commitBounds(rnd, postOrder.length);
                for (int c = 0; c + 1 < cb.length; c++) {
                    insertCommit(sink, postOrder, cb[c], cb[c + 1], tsv, symIdx, iv, xv, xNull, dLit);
                    drainWalQueue();
                    refreshCycle(job);

                    // Simulate a restart at a quiescent point (ACTIVE state only).
                    if (restart && rnd.nextInt(3) == 0) {
                        LiveViewInstance inst = engine.getLiveViewRegistry().getViewInstance("lv");
                        if (inst != null
                                && inst.getStateReader().getBackfillState() == LiveViewState.BACKFILL_STATE_ACTIVE) {
                            job = Misc.free(job);
                            engine.getLiveViewRegistry().clear();
                            engine.buildViewGraphs();
                            job = new LiveViewRefreshJob(0, engine, 1);
                        }
                    }
                }
            }

            driveRefreshToQuiescence(job);

            if (inMemReadBack) {
                // Top up with one clean forward row above the global max ts so the
                // in-mem tier is guaranteed populated at the final read. A run that
                // ended on a restart would otherwise leave the freshly-rebuilt tier
                // empty (no post-restart ingestion to publish), routing the
                // read-back disk-only and leaving Mode B unexercised. i>0 keeps the
                // row past the optional WHERE; the recompute oracle below naturally
                // includes it.
                execute("INSERT INTO base (ts, sym, i, x) VALUES ("
                        + (tsv[rowCount - 1] + 1) + "::timestamp, 'AA', 1, 1.0)");
                drainWalQueue();
                refreshCycle(job);
                driveRefreshToQuiescence(job);
            } else if (leadReadBack) {
                // Build a deterministic un-flushed lead on top of the applied
                // state: pin the flush clock to now and refresh a forward batch
                // above the global max ts so it publishes into the tier as the lead
                // without crossing FLUSH EVERY (no flush, so disk keeps only the
                // applied prefix). The clock is not advanced, so the lead stays.
                buildLeadForReadBack(job, tsv[rowCount - 1]);
            }
        } finally {
            Misc.free(job);
        }

        if (leadReadBack) {
            // Mode A read-back cross-checks, single-threaded now the worker is freed
            // and a known lead is resident: the tier-on read serves exactly the
            // un-flushed lead and equals the recompute, while the forced disk-only
            // fallback serves only the applied prefix (the recompute minus the
            // lead). Uses a direct SELECT * FROM lv (native ts order) rather than
            // the ORDER BY 1 wrapper, whose routing is not guaranteed to be Mode A.
            assertLeadReadBack(viewSql);

            if (restart) {
                // Crash-before-flush: drop the in-memory registry (losing the RAM
                // lead) and rebuild from disk, then a restart that recovers the lead
                // by draining the retained base WAL forward. The same cross-checks
                // must hold on the recovered lead.
                restartAndRecoverLead();
                assertLeadReadBack(viewSql);
            }
        } else {
            // The oracle: the live view must equal the window query recomputed over
            // the base table. ORDER BY 1 (the unique ts) gives both sides a total
            // order; genericStringMatch tolerates SYMBOL-vs-STRING on passthrough.
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "(" + viewSql + ") ORDER BY 1",
                    "(lv) ORDER BY 1",
                    LOG,
                    true
            );

            if (inMemReadBack) {
                // Mode B read-back cross-checks, single-threaded now that the worker
                // is freed and the view is quiesced: the tier actually serves the
                // read, and the Mode B result is byte-identical to the forced
                // disk-only path under whatever O3 / restart / backfill pattern this
                // run produced.
                assertModeBEngaged();
                assertModeBMatchesDiskOnly("SELECT * FROM lv");
            }
        }

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
    }
}

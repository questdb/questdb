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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.lv.LiveViewRecordCursor;
import io.questdb.griffin.engine.lv.LiveViewRecordCursorFactory;
import io.questdb.mp.Job;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LongHashSet;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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
    // Extended variants past the all-variant loop. variantCount() stays at the
    // base set, so the for-v loops never reach these; their dedicated @Test
    // methods drive them directly. RANGE_* exercise the bounded-RANGE
    // monotonic-deque maintenance path (distinct from the ROWS ring buffer);
    // LAG_* exercise the lag ZERO_PASS window; TIE_PEER_* exercise the
    // peer/frame-position window shapes (last_value, nth_value, IGNORE NULLS).
    // Values are contiguous past DECIMAL_VARIANT so projection() can switch on them.
    private static final int RANGE_SUM_VARIANT = 8;
    private static final int RANGE_AVG_VARIANT = 9;
    private static final int RANGE_FIRST_VALUE_VARIANT = 10;
    private static final int LAG_VARIANT = 11;
    private static final int LAG_OFFSET_VARIANT = 12;
    // Tie/peer-order-sensitive bounded-frame shapes (see testFuzzTiePeerShapes).
    // Each reads a value at a specific frame position rather than aggregating,
    // so it is sensitive to how the incremental ring buffer tracks frame ends and
    // NULL skips; all remain a deterministic function of a unique-ts total order,
    // so the standard recompute oracle holds.
    private static final int LAST_VALUE_VARIANT = 13;
    private static final int LAST_VALUE_IGNORE_NULLS_VARIANT = 14;
    private static final int NTH_VALUE_VARIANT = 15;
    private static final int FIRST_VALUE_IGNORE_NULLS_VARIANT = 16;
    // Window shapes driven over concurrent multi-WalWriter base ingestion by
    // runConcurrentWriterFuzz. Every shape here reads the (ts, sym, i, x) base the
    // writer threads populate (so the DECIMAL variant, which needs a d column, is
    // excluded) and is a deterministic function of the unique-ts row set, so the
    // recompute oracle holds regardless of how the writers interleave. The
    // un-partitioned row_number() OVER () (variant 5) is kept in - its whole-table
    // re-sequencing under apply-ahead is the Finding 4 surface this arm targets.
    private static final int[] CONCURRENT_WRITER_VARIANTS = {
            0, 1, 2, 3, 4, 5, 6,
            RANGE_SUM_VARIANT, RANGE_AVG_VARIANT, RANGE_FIRST_VALUE_VARIANT,
            LAG_VARIANT, LAG_OFFSET_VARIANT
    };
    // Partitioned (PARTITION BY sym ORDER BY ts) variants driven over a DEDUP base
    // by runDedupFuzz. Their output is a deterministic function of the deduped base
    // (every (ts, sym) is unique after apply, so ts is a total order within each sym
    // partition), which keeps the recompute oracle sound under duplicate timestamps.
    // The un-partitioned row_number OVER () (variant 5) is excluded: its scan-order
    // tie-break is ambiguous once timestamps collide. The decimal variant is excluded
    // too - its storage path is orthogonal to the coupled dedup refresh and already
    // fuzzed by the non-dedup arms.
    private static final int[] DEDUP_VARIANTS = {
            0, 1, 2, 3, 4, 6,
            RANGE_SUM_VARIANT, RANGE_AVG_VARIANT, RANGE_FIRST_VALUE_VARIANT,
            LAG_VARIANT, LAG_OFFSET_VARIANT
    };
    // The canonical fixed-width (ts, sym, i, x) window-shape set, fuzzed by the
    // REPLACE_RANGE arm (runReplaceRangeFuzz) and the removal freeze-and-continue
    // arm (runRemovalFreezeContinueFuzz); the concurrent-writer arm drives the
    // same set under its own name. The decimal variant needs a d column and is
    // excluded. Every shape is a deterministic function of a unique-ts row set,
    // so both arms' oracles hold: the replace arm recomputes over the final
    // applied base (well-defined - a later replace wins, and replace rows draw
    // timestamps from a never-reused pool), the removal arm over the logical
    // shadow dataset.
    private static final int[] FIXED_WIDTH_VARIANTS = {
            0, 1, 2, 3, 4, 5, 6,
            RANGE_SUM_VARIANT, RANGE_AVG_VARIANT, RANGE_FIRST_VALUE_VARIANT,
            LAG_VARIANT, LAG_OFFSET_VARIANT
    };
    // Window shapes driven over a TIMESTAMP_NS (nanosecond-precision) base by
    // runNanosBaseFuzz: the ROWS-frame aggregates, ranking OVER (), and
    // lag()/lag(,k) - every shape whose result is a function of the ROW SET,
    // independent of the timestamp UNIT. What is under test is the
    // timestamp-driver-aware refresh path (ns partition arithmetic, IN MEMORY
    // micros-to-ns scaling), not a unit-sensitive frame; the bounded-RANGE
    // '<n> MINUTE' variants remain the micros arms' job and are excluded here.
    private static final int[] NANOS_BASE_VARIANTS = {
            0, 1, 2, 3, 4, 5, 6, LAG_VARIANT, LAG_OFFSET_VARIANT
    };
    // Tie/peer-order-sensitive bounded-frame shapes driven by runTiePeerShapes:
    // last_value / last_value IGNORE NULLS / nth_value / first_value IGNORE NULLS
    // over a PARTITION BY sym ORDER BY ts ROWS frame. Each is a deterministic
    // function of the unique-ts row set, so the standard recompute oracle holds;
    // they exercise frame-end tracking and NULL-skip paths the aggregate arms do not.
    private static final int[] TIE_PEER_VARIANTS = {
            LAST_VALUE_VARIANT, LAST_VALUE_IGNORE_NULLS_VARIANT,
            NTH_VALUE_VARIANT, FIRST_VALUE_IGNORE_NULLS_VARIANT
    };
    // Anchored-window fuzz variants (driven via runAnchoredFuzz): sum, avg,
    // count, max, row_number, plus the tie/peer-order ranking shapes rank and
    // dense_rank (F11) - all over a named WINDOW carrying ANCHOR EXPRESSION.
    // rank/dense_rank need the unbounded, ordered frame a ranking function
    // implies, so they ride the anchored harness rather than the bounded-ROWS
    // one; over a unique-ts total order they collapse to a per-bucket sequence,
    // which the (sym, bucket)-partitioned oracle recomputes exactly.
    private static final int ANCHORED_VARIANT_COUNT = 7;
    private static final int MAX_FRAME = 20;
    // Sentinel i for phantom (rolled-back) rows: large and positive, so a leaked phantom
    // both survives a WHERE i>0 filter and stands out against the [-1000, 1000] real data.
    private static final long PHANTOM_SENTINEL = 999_999;
    private static final String[] SYMBOLS = {
            "AA", "BB", "CC", "DD", "EE", "FF", "GG", "HH",
            "II", "JJ", "KK", "LL", "MM", "NN", "OO", "PP"
    };

    @Test
    public void testFuzzAnchored() throws Exception {
        // Differential fuzz for anchored (ANCHOR EXPRESSION) windows. The anchor
        // resets the cumulative aggregate whenever the anchor expression changes
        // value in ts order. The fuzz uses a MONOTONIC anchor (timestamp_floor),
        // so "reset on change" is identical to "partition by the bucket value" -
        // the oracle is therefore the equivalent (sym, bucket)-partitioned regular
        // window recomputed over the base. Driven under O3 plus optional restart
        // and optional backfill, so the anchor map rebuild on head-miss / head-hit
        // replay and across a restart is cross-checked against the recompute. The
        // final two variants are the F11 ranking shapes rank() and dense_rank():
        // they need the unbounded ordered frame a ranking function implies, so
        // they ride the anchor here rather than a bounded-ROWS frame.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(() -> {
            for (int v = 0; v < ANCHORED_VARIANT_COUNT; v++) {
                runAnchoredFuzz(rnd, v, 120 + rnd.nextInt(180), rnd.nextBoolean(), rnd.nextBoolean());
            }
        });
    }

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
    public void testFuzzBaseDdlInvalidation() throws Exception {
        // Invalidation transitions from base DDL: every operation that must
        // flip the view to INVALID - referenced-column DROP / RENAME / retype,
        // base-table RENAME, base-table DROP - fires once per run against a
        // randomized dataset and window shape. Each transition asserts the
        // specific invalidation reason, that the view's materialized output
        // stays queryable and byte-identical to the pre-invalidation snapshot,
        // and (where the base still exists) that further base ingestion never
        // reaches the invalid view.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(() -> {
            for (int op = 0; op < 5; op++) {
                runBaseDdlInvalidationFuzz(rnd, op, 40 + rnd.nextInt(80));
            }
        });
    }

    @Test
    public void testFuzzBaseDdlTransparent() throws Exception {
        // Base-DDL churn that must be TRANSPARENT to the view: between
        // ingestion commits the base receives ADD / DROP / RENAME / retype DDL
        // on columns the view never references. The view must stay ACTIVE
        // across every change (the invalidation gate checks referenced columns
        // only), and the run-end recompute oracle must hold - the per-cycle
        // column-mapping re-resolution keeps reading (ts, sym, i, x) correctly
        // as unreferenced columns come, go, get renamed, and change type
        // around them. Runs under O3 with optional restart / BACKFILL /
        // IN MEMORY, so the DDL churn also interleaves with head replays and
        // registry rebuilds.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(() -> {
            for (int i = 0; i < FIXED_WIDTH_VARIANTS.length; i++) {
                runBaseDdlFuzz(rnd, FIXED_WIDTH_VARIANTS[i], 120 + rnd.nextInt(160),
                        rnd.nextBoolean(), rnd.nextBoolean(), rnd.nextBoolean());
            }
        });
    }

    @Test
    public void testFuzzBaseTtl() throws Exception {
        // Base-table TTL retention is transparent to the view: an insert that
        // advances the base max timestamp past the TTL window evicts older
        // partitions at apply time, exactly like DROP PARTITION - a non-DATA
        // operation the refresh worker walks past. The view stays ACTIVE, its
        // already-emitted rows below the evicted range are frozen (not
        // retracted), and forward ingestion continues the window accumulation
        // as if the evicted rows still existed. The run-end oracle therefore
        // recomputes over a shadow table holding the LOGICAL dataset (every row
        // ever inserted). Parametrized over the partition unit (DAY / HOUR),
        // with the TTL granularity matched to it, plus restart and IN MEMORY.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(() -> {
            for (int i = 0; i < FIXED_WIDTH_VARIANTS.length; i++) {
                runBaseTtlFuzz(rnd, FIXED_WIDTH_VARIANTS[i], 120 + rnd.nextInt(160),
                        rnd.nextBoolean(), rnd.nextBoolean(), rnd.nextBoolean());
            }
        });
    }

    @Test
    public void testFuzzConcurrentWriters() throws Exception {
        // Differential fuzz with the base ingested by MULTIPLE concurrent WAL
        // writers rather than the single-writer SQL INSERT path every other arm
        // uses. Each variant's post-CREATE rows are split into disjoint,
        // globally-ts-ordered round-robin slices, one per writer thread; the
        // writers own their own WalWriter and commit concurrently, so the base
        // sequencer interleaves their transactions and the apply job batches
        // several seqTxns per cycle. That interleave / apply-ahead is the Finding 4
        // territory (a trailing in-order global-max commit was once forward-re-
        // appended as a permanent duplicate), and the row_number() variant's whole-
        // table re-sequencing is its sharpest surface, so that variant always runs a
        // concurrent refresh driver; the others randomize it. The recompute oracle
        // stays exact because the disjoint slices reassemble to the same unique-ts
        // base no matter how the writers interleave, so the quiesced view must equal
        // the from-scratch recompute. Optional BACKFILL captures pre-CREATE history
        // (single-writer, so the backfill floor pins the global-min ts) before the
        // concurrent suffix.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(() -> {
            for (int i = 0; i < CONCURRENT_WRITER_VARIANTS.length; i++) {
                final int variant = CONCURRENT_WRITER_VARIANTS[i];
                // row_number() OVER () (variant 5) always drives a concurrent refresh
                // so the apply-ahead re-sequencing path is exercised every run.
                final boolean concurrentRefresh = variant == 5 || rnd.nextBoolean();
                runConcurrentWriterFuzz(rnd, variant, 200 + rnd.nextInt(300),
                        2 + rnd.nextInt(3), concurrentRefresh, rnd.nextBoolean());
            }
        });
    }

    @Test
    public void testFuzzDedup() throws Exception {
        // Differential fuzz over a DEDUP UPSERT KEYS(ts, sym) base - the coupled,
        // applied-reader refresh path. Unlike every other arm, timestamps are NOT
        // unique: they are drawn from a small pool so many rows share one ts across
        // different keys (additive same-ts, the Phase 2a clean raw-WAL fast path),
        // and a forced fraction re-emit an existing (ts, sym) with a new value (real
        // dedup replacement, routed through the applied-reader replay). The recompute
        // oracle stays sound because (ts, sym) is the dedup key: after apply every
        // (ts, sym) is unique, so within each sym partition ts is a total order and a
        // partitioned window is a deterministic function of the final deduped base.
        // Each variant runs under O3 with random restart and backfill; DROP PARTITION
        // of an unprocessed future band (removals=true) exercises the divergence gate
        // - a removed row the LV never emitted must not leak onto the raw-WAL path.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(() -> {
            for (int i = 0; i < DEDUP_VARIANTS.length; i++) {
                runDedupFuzz(rnd, DEDUP_VARIANTS[i], 120 + rnd.nextInt(160),
                        true, rnd.nextBoolean(), rnd.nextBoolean(), true);
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
    public void testFuzzLag() throws Exception {
        // lag() and lag(, k) are ZERO_PASS partitioned windows. The recompute
        // oracle holds because lag over a unique-ts total order is a deterministic
        // function of the row set. Fuzzed under O3 plus optional restart and
        // optional backfill, so the per-partition lookback state survives the
        // head replay and checkpoint restore paths.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(() -> {
            for (int v = LAG_VARIANT; v <= LAG_OFFSET_VARIANT; v++) {
                runFuzz(rnd, v, 120 + rnd.nextInt(160), true, rnd.nextBoolean(), rnd.nextBoolean(), rnd.nextBoolean());
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
    public void testFuzzMultipleLiveViews() throws Exception {
        // Several live views over ONE base table, all maintained by a single
        // refresh worker (production runs one LiveViewRefreshJob per worker, and
        // each scans every view). Every view carries a DISTINCT window shape and
        // is cross-checked against its own from-scratch recompute at quiescence,
        // so the one worker must maintain K unrelated views correctly off the
        // shared base WAL stream. The views also share ONE base WAL retention
        // floor: WalPurgeJob pins the base WAL to the minimum lvConsumedSeqTxn
        // across all non-dropped dependents. The run applies a final batch of
        // base rows WITHOUT refreshing (so the base head is ahead of every view's
        // floor), drains a real WalPurgeJob - only the shared LV floor can retain
        // that batch's WAL - and then refreshes: all views must converge, which
        // they cannot if the purge ignored the floor and dropped the segments.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(() -> {
            runMultiViewFuzz(rnd, 200 + rnd.nextInt(200), false);
            runMultiViewFuzz(rnd, 200 + rnd.nextInt(200), true);
        });
    }

    @Test
    public void testFuzzNanosBase() throws Exception {
        // Differential fuzz over a TIMESTAMP_NS (nanosecond-precision) base
        // partitioned BY HOUR - a non-DAY partition unit and the ns timestamp
        // driver at once. The refresh path is timestamp-driver-aware (ns
        // partition arithmetic, IN MEMORY micros-to-ns scaling), and the
        // recompute oracle holds identically to the micros arms: a unit-agnostic
        // window shape over a unique-ts total order is a deterministic function
        // of the row set. Fuzzed under O3 plus optional restart / BACKFILL /
        // IN MEMORY.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(() -> {
            for (int i = 0; i < NANOS_BASE_VARIANTS.length; i++) {
                runNanosBaseFuzz(rnd, NANOS_BASE_VARIANTS[i], 120 + rnd.nextInt(160),
                        true, rnd.nextBoolean(), rnd.nextBoolean(), rnd.nextBoolean());
            }
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
    public void testFuzzParquetBase() throws Exception {
        // Differential fuzz over a base whose settled partitions are converted to
        // PARQUET while an incremental live view maintains itself off it. The
        // refresh consumes the base WAL stream, not base partitions, so converting
        // an already-consumed partition is physically transparent; in-order runs
        // convert mid-stream and keep refreshing over the partially-parquet base,
        // and every run converts once more at the end. The recompute oracle is
        // unchanged - the from-scratch recompute reads the same parquet/native
        // base. (A BACKFILL view over a parquet base is deliberately excluded: it
        // double-counts rows, a separate parquet-read defect - see runParquetBaseFuzz.)
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(() -> {
            for (int i = 0; i < FIXED_WIDTH_VARIANTS.length; i++) {
                runParquetBaseFuzz(rnd, FIXED_WIDTH_VARIANTS[i], 120 + rnd.nextInt(160),
                        rnd.nextBoolean(), rnd.nextBoolean());
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
    public void testFuzzRangeFrame() throws Exception {
        // Bounded RANGE frames exercise the monotonic-deque maintenance path,
        // distinct from the ROWS ring buffer the other aggregate variants use. The
        // recompute oracle holds identically: a bounded RANGE frame over a
        // unique-ts total order is a deterministic function of the row set. Fuzzed
        // under O3 plus optional restart and optional backfill.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(() -> {
            for (int v = RANGE_SUM_VARIANT; v <= RANGE_FIRST_VALUE_VARIANT; v++) {
                runFuzz(rnd, v, 120 + rnd.nextInt(160), true, rnd.nextBoolean(), rnd.nextBoolean(), rnd.nextBoolean());
            }
        });
    }

    @Test
    public void testFuzzReaderVsRefresh() throws Exception {
        // Reader-vs-refresh fuzz: a single refresh driver races reader threads
        // that continuously open cursors over the live view - one through the
        // native SELECT * FROM lv (tier-routed when IN MEMORY), one through the
        // (lv) ORDER BY 1 wrapper - while the main thread ingests. Every reader
        // snapshot must be prefix-consistent: per-reader monotonic row count,
        // strictly-ascending ts, gapless row_number 1..N, and the two
        // var-length passthroughs decoding back to their ts-derived values (a
        // torn read surfaces as a mismatch or a crash). This is a deliberately
        // WEAKER oracle than exact equality - it catches structural violations
        // mid-flight, not wrong aggregate values - so the run still ends on the
        // quiescent recompute cross-check; it complements the multi-worker soak
        // (LiveViewConcurrencyTest#testMultiRefreshWorkerConvergence) rather
        // than replacing it. The second run ingests out of order, racing the
        // readers against O3 REPLACE_RANGE rewrites instead of plain appends.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(() -> {
            runReaderVsRefreshFuzz(rnd, 300 + rnd.nextInt(400), false, rnd.nextBoolean());
            runReaderVsRefreshFuzz(rnd, 300 + rnd.nextInt(400), true, rnd.nextBoolean());
        });
    }

    @Test
    public void testFuzzRemovalFreezeAndContinue() throws Exception {
        // TRUNCATE / DROP PARTITION of rows the view has ALREADY emitted: the
        // freeze-and-continue contract. Both are non-DATA operations the
        // refresh worker walks past - the view stays ACTIVE, its emitted rows
        // are preserved byte-for-byte, and subsequent forward ingestion
        // continues the window accumulation as if the removed rows were still
        // in the base. The run-end oracle therefore recomputes over a shadow
        // table holding the LOGICAL dataset (every row ever inserted). Each
        // variant randomizes TRUNCATE vs DROP PARTITION (bottom / middle / top
        // days of the emitted range), restart, BACKFILL, and IN MEMORY.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(() -> {
            for (int i = 0; i < FIXED_WIDTH_VARIANTS.length; i++) {
                runRemovalFreezeContinueFuzz(rnd, FIXED_WIDTH_VARIANTS[i], 120 + rnd.nextInt(160),
                        rnd.nextBoolean(), rnd.nextBoolean(), rnd.nextBoolean(), rnd.nextBoolean());
            }
        });
    }

    @Test
    public void testFuzzReplaceRange() throws Exception {
        // Differential fuzz with REPLACE_RANGE data commits on the base,
        // interleaved with plain O3 inserts. Each replace atomically deletes a
        // random band [lo, hi) - possibly covering rows the view has already
        // emitted, or rows still pending in the base WAL - and inserts 0..3
        // fresh rows inside it (0 = a pure delete). The deletion side of a
        // replace commit is visible to the refresh drain only through the
        // commit's range metadata (its inserted rows may all sit above the
        // frontier, or be absent), so this arm exercises the range-aware O3
        // trigger end to end, under optional restart / BACKFILL / IN MEMORY:
        // any ghost row a converging replay failed to erase diverges from the
        // recompute oracle at quiescence.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(() -> {
            for (int i = 0; i < FIXED_WIDTH_VARIANTS.length; i++) {
                runReplaceRangeFuzz(rnd, FIXED_WIDTH_VARIANTS[i], 120 + rnd.nextInt(160),
                        rnd.nextBoolean(), rnd.nextBoolean(), rnd.nextBoolean());
            }
        });
    }

    @Test
    public void testFuzzRolledBackCommits() throws Exception {
        // Rolled-back base transactions must be INVISIBLE to the live view: a WAL
        // transaction that appends rows then rolls back never advances the base
        // sequencer, so its rows never reach the seqTxn-driven LV drain (the same
        // commit-boundary that makes a cancelled row invisible). Between the real
        // committed batches this arm injects doomed transactions - phantom rows carrying
        // a large sentinel i and a timestamp drawn just below a committed row, then
        // WalWriter.rollback(). A leaked phantom would both survive the optional WHERE
        // i>0 and, being below the frontier, trip an O3 replay - a visible divergence
        // from the recompute over the committed base at quiescence. Every fixed-width
        // variant runs under in-order / O3 and optional BACKFILL.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(() -> {
            for (int i = 0; i < FIXED_WIDTH_VARIANTS.length; i++) {
                runRolledBackFuzz(rnd, FIXED_WIDTH_VARIANTS[i], 120 + rnd.nextInt(160),
                        rnd.nextBoolean(), rnd.nextBoolean());
            }
        });
    }

    @Test
    public void testFuzzStorageTimingProps() throws Exception {
        // F9: differential fuzz under randomized storage / WAL-apply-timing config.
        // None of these properties change the query result - the recompute oracle
        // is unchanged - but they reshape HOW base commits batch into apply cycles:
        // the apply look-ahead bounds how many base seqTxns one apply cycle folds
        // into a base-table commit, the apply time quota and commit-to-table lag
        // (size / txn count) cap how much buffers before a base-table commit, and
        // the O3 partition-split knobs reshape the physical partition layout the
        // refresh reads back. A single random config is pinned per run and every
        // fixed-width variant is driven under O3 with optional restart / backfill /
        // IN MEMORY; the quiescent view must still equal the from-scratch recompute
        // regardless of how the base commits were batched underneath it.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        setProperty(PropertyKey.CAIRO_WAL_APPLY_LOOK_AHEAD_TXN_COUNT, 1 + rnd.nextInt(64));
        setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 1 + rnd.nextInt(250));
        setProperty(PropertyKey.CAIRO_WAL_MAX_LAG_TXN_COUNT, 1 + rnd.nextInt(20));
        setProperty(PropertyKey.CAIRO_WAL_MAX_LAG_SIZE, rnd.nextLong(10L * 1024 * 1024));
        setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 200 + rnd.nextInt(19_800));
        setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 1 + rnd.nextInt(2));
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, 1L << (16 + rnd.nextInt(6)));
        assertMemoryLeak(() -> {
            for (int v = 0; v < variantCount(); v++) {
                runFuzz(rnd, v, 120 + rnd.nextInt(160), true, rnd.nextBoolean(), rnd.nextBoolean(), rnd.nextBoolean());
            }
        });
    }

    @Test
    public void testFuzzTiePeerShapes() throws Exception {
        // F11: tie/peer-order-sensitive window shapes folded into the differential
        // recompute oracle. last_value (RESPECT and IGNORE NULLS), nth_value(k),
        // and first_value IGNORE NULLS each read a value at a specific frame
        // position rather than aggregating the frame, so they stress the
        // incremental ring buffer's frame-end tracking and NULL-skip bookkeeping
        // that the sum / avg / min / max arms never touch. Every shape is a
        // deterministic function of the unique-ts row set, so the standard
        // recompute oracle (the identical window SQL over the base) holds; each
        // variant runs under O3 with optional restart / backfill / IN MEMORY. The
        // ranking peers rank() and dense_rank() need an unbounded ordered frame and
        // are covered by the anchored arm instead (see testFuzzAnchored).
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(() -> {
            for (int i = 0; i < TIE_PEER_VARIANTS.length; i++) {
                runFuzz(rnd, TIE_PEER_VARIANTS[i], 120 + rnd.nextInt(160),
                        true, rnd.nextBoolean(), rnd.nextBoolean(), rnd.nextBoolean());
            }
        });
    }

    @Test
    public void testFuzzVarSize() throws Exception {
        // Var-length passthrough tier coverage: an LV projecting STRING / VARCHAR /
        // BINARY / DOUBLE[] columns straight through alongside row_number() OVER (),
        // so SELECT * FROM lv routes through the in-mem tier (Mode B) and the tier
        // must store and read back every var-length value. Three configs per run -
        // in-order, O3, and O3 + restart - each with random backfill and a fresh
        // random dataset, so the var-length (data, aux) write/read paths, the flush
        // flyweight, and the O3 disk-stager rebuild are all exercised. After
        // quiescence each run cross-checks three ways: the read-back equals the
        // from-scratch recompute, Mode B is confirmed engaged, and the Mode B
        // result is byte-identical to the forced disk-only path.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(() -> {
            final boolean backfill = rnd.nextBoolean();
            runVarSizeFuzz(rnd, 120 + rnd.nextInt(160), false, false, backfill);
            runVarSizeFuzz(rnd, 120 + rnd.nextInt(160), true, false, backfill);
            runVarSizeFuzz(rnd, 120 + rnd.nextInt(160), true, true, backfill);
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

    // Oracle (plain query over the base) for an anchored fuzz variant: the anchor
    // reset is replicated by adding the monotonic bucket to a regular window's
    // PARTITION BY, with the window's natural default frame (UNBOUNDED PRECEDING TO
    // CURRENT ROW). For a monotonic anchor this is semantically identical to the
    // anchored live-view query over a unique-ts total order.
    private static String anchoredOracleProjection(int variant, String bucket) {
        final String part = "PARTITION BY sym, " + bucket + " ORDER BY ts";
        return switch (variant) {
            case 0 -> "ts, sym, i, sum(i) OVER (" + part + ") AS v";
            case 1 -> "ts, sym, x, avg(x) OVER (" + part + ") AS v";
            case 2 -> "ts, sym, count() OVER (" + part + ") AS v";
            case 3 -> "ts, sym, i, max(i) OVER (" + part + ") AS v";
            case 4 -> "ts, sym, row_number() OVER (" + part + ") AS v";
            case 5 -> "ts, sym, rank() OVER (" + part + ") AS v";
            case 6 -> "ts, sym, dense_rank() OVER (" + part + ") AS v";
            default -> throw new IllegalArgumentException("anchored variant=" + variant);
        };
    }

    // The live-view projection for an anchored fuzz variant: an unbounded
    // cumulative aggregate (or ranking) over a named WINDOW that carries the
    // ANCHOR EXPRESSION (declared by the caller). Inline ANCHOR is rejected at
    // CREATE, so the anchor must live on a named WINDOW.
    private static String anchoredViewProjection(int variant) {
        return switch (variant) {
            case 0 -> "ts, sym, i, sum(i) OVER w AS v";
            case 1 -> "ts, sym, x, avg(x) OVER w AS v";
            case 2 -> "ts, sym, count() OVER w AS v";
            case 3 -> "ts, sym, i, max(i) OVER w AS v";
            case 4 -> "ts, sym, row_number() OVER w AS v";
            case 5 -> "ts, sym, rank() OVER w AS v";
            case 6 -> "ts, sym, dense_rank() OVER w AS v";
            default -> throw new IllegalArgumentException("anchored variant=" + variant);
        };
    }

    // Appends one (ts, sym, i, x) row through a WalWriter. A negative symIdx writes
    // a NULL symbol; LONG_NULL in iv stores as a NULL LONG. Used by the concurrent-
    // writer fuzz threads, which each own their own WalWriter.
    private static void appendRow(WalWriter walWriter, long ts, int symIdx, long iv, double xv) {
        TableWriter.Row row = walWriter.newRow(ts);
        if (symIdx < 0) {
            row.putSym(1, (CharSequence) null);
        } else {
            row.putSym(1, SYMBOLS[symIdx]);
        }
        row.putLong(2, iv);
        row.putDouble(3, xv);
        row.append();
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
    // sum/max/min/first_value/count/avg, plus ranking OVER (), the bounded-RANGE
    // counterparts (the monotonic-deque path), and lag()/lag(,k). Un-partitioned
    // aggregate windows and last_value over a CURRENT ROW frame are rejected at
    // CREATE (no snapshot support), so they are not fuzzed here. min reuses Max's
    // migrated MaxMinOver* classes, so it carries the same snapshot contract. N is
    // the bounded-frame radius (rows for ROWS frames; minutes for RANGE frames;
    // the lookback offset for lag).
    private static String projection(int variant, int n) {
        final String frame = "PARTITION BY sym ORDER BY ts ROWS BETWEEN " + n + " PRECEDING AND CURRENT ROW";
        // Bounded RANGE frame: the radius is a time interval (n minutes), so the
        // captured row count varies with the per-run ts step. The frame is over
        // the designated timestamp and deterministic under a unique-ts total order.
        final String rangeFrame = "PARTITION BY sym ORDER BY ts RANGE BETWEEN '" + n + "' MINUTE PRECEDING AND CURRENT ROW";
        return switch (variant) {
            case 0 -> "ts, sym, i, sum(i) OVER (" + frame + ") AS v";
            case 1 -> "ts, sym, i, max(i) OVER (" + frame + ") AS v";
            case 2 -> "ts, sym, i, first_value(i) OVER (" + frame + ") AS v";
            case 3 -> "ts, sym, count() OVER (" + frame + ") AS v";
            case 4 -> "ts, sym, x, avg(x) OVER (" + frame + ") AS v";
            case 5 -> "ts, sym, row_number() OVER () AS rn";
            case 6 -> "ts, sym, i, min(i) OVER (" + frame + ") AS v";
            case RANGE_SUM_VARIANT -> "ts, sym, i, sum(i) OVER (" + rangeFrame + ") AS v";
            case RANGE_AVG_VARIANT -> "ts, sym, x, avg(x) OVER (" + rangeFrame + ") AS v";
            case RANGE_FIRST_VALUE_VARIANT -> "ts, sym, i, first_value(i) OVER (" + rangeFrame + ") AS v";
            // lag is frame-independent, but a live view rejects a bare unbounded
            // window (no ANCHOR), so it carries a bounded ROWS frame here. Both the
            // live view and the oracle use this identical SQL, so the frame (which
            // lag ignores) never changes the cross-check.
            case LAG_VARIANT -> "ts, sym, i, lag(i) OVER (" + frame + ") AS v";
            case LAG_OFFSET_VARIANT -> "ts, sym, i, lag(i, " + (1 + (n & 3)) + ") OVER (" + frame + ") AS v";
            // Peer/frame-position shapes. last_value / nth_value snapshot support is
            // gated on the VALUE type (LONG is not migrated; TIMESTAMP is), so these
            // read the designated ts - last_value tracks the frame's trailing row (a
            // frame ending one row back, so it is the prior row's ts, NULL near the
            // partition start), nth_value(k) picks the kth frame row. first_value on
            // LONG is migrated, so its IGNORE NULLS form reads i and genuinely skips
            // the NULL i values. All deterministic over a unique-ts total order.
            case LAST_VALUE_VARIANT ->
                    "ts, sym, i, last_value(ts) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN " + (n + 1) + " PRECEDING AND 1 PRECEDING) AS v";
            case LAST_VALUE_IGNORE_NULLS_VARIANT -> "ts, sym, i, last_value(ts) IGNORE NULLS OVER (" + frame + ") AS v";
            case NTH_VALUE_VARIANT -> "ts, sym, i, nth_value(ts, " + (1 + (n & 3)) + ") OVER (" + frame + ") AS v";
            case FIRST_VALUE_IGNORE_NULLS_VARIANT -> "ts, sym, i, first_value(i) IGNORE NULLS OVER (" + frame + ") AS v";
            default -> throw new IllegalArgumentException("variant=" + variant);
        };
    }

    // A random ASCII string of length [minLen, maxLen], drawn from [a-zA-Z0-9]
    // (no quote chars, so it embeds straight into a single-quoted SQL literal).
    // Used to build the STRING / VARCHAR / BINARY var-length passthrough values
    // for the var-size fuzz; maxLen up to 24 spans both the fully-inlined VARCHAR
    // header and the split data-region path.
    private static String randomAscii(Rnd rnd, int minLen, int maxLen) {
        final int len = minLen + rnd.nextInt(maxLen - minLen + 1);
        final StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            final int c = rnd.nextInt(62);
            if (c < 26) {
                sb.append((char) ('a' + c));
            } else if (c < 52) {
                sb.append((char) ('A' + c - 26));
            } else {
                sb.append((char) ('0' + c - 52));
            }
        }
        return sb.toString();
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

    // Renders one INSERT value tuple for the four var-length passthrough columns
    // (STRING, VARCHAR, BINARY, DOUBLE[]) - each an occasional NULL, else a random
    // value. STRING/VARCHAR carry an empty string on some rows (a real value
    // distinct from NULL) and range up to 24 chars so the run exercises both the
    // inlined and the split VARCHAR path; BINARY rides a non-empty 'string'::binary
    // cast; the array carries 1..5 doubles. The value bytes need not be
    // reconstructible by the recompute - they are materialized once into the base
    // table, and both the live view and the from-scratch recompute read them back
    // from there.
    private static String varSizeTuple(Rnd rnd) {
        final StringBuilder sb = new StringBuilder();
        if (rnd.nextInt(20) == 0) {
            sb.append("null");
        } else {
            sb.append('\'').append(randomAscii(rnd, 0, 24)).append('\'');
        }
        sb.append(", ");
        if (rnd.nextInt(20) == 0) {
            sb.append("null");
        } else {
            sb.append('\'').append(randomAscii(rnd, 0, 24)).append('\'');
        }
        sb.append(", ");
        if (rnd.nextInt(20) == 0) {
            sb.append("null");
        } else {
            sb.append('\'').append(randomAscii(rnd, 1, 16)).append("'::binary");
        }
        sb.append(", ");
        if (rnd.nextInt(20) == 0) {
            sb.append("null");
        } else {
            sb.append("ARRAY[");
            final int len = 1 + rnd.nextInt(5);
            for (int j = 0; j < len; j++) {
                if (j > 0) {
                    sb.append(',');
                }
                sb.append(rnd.nextInt(1000)).append(".0");
            }
            sb.append(']');
        }
        return sb.toString();
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

    // One randomized REPLACE_RANGE commit against the base: picks a band whose
    // boundaries anchor on dataset row timestamps - the low bound lands exactly
    // ON a row's ts half the time, exercising the non-strict frontier edge -
    // and writes 0..3 fresh rows inside the band (0 = a pure delete). Fresh row
    // timestamps come from the never-reused usedTs pool, so global ts
    // uniqueness survives any sequence of replaces and the recompute oracle
    // stays sound. Rows are appended in random order half the time, so a
    // replace commit can also be intra-commit out-of-order. The band's low
    // bound stays strictly above the global-min ts (tsv[0]), keeping every
    // replace row above a backfill run's floor.
    private void commitReplaceRange(Rnd rnd, TableToken baseToken, long[] tsv, LongHashSet usedTs, int symCount) {
        final int rowCount = tsv.length;
        final int a = 1 + rnd.nextInt(rowCount - 1);
        final int b = a + rnd.nextInt(rowCount - a);
        final long lo = rnd.nextBoolean() ? tsv[a] : tsv[a] + 1 + rnd.nextInt(1000);
        final long hi = Math.max(lo + 1, rnd.nextBoolean() ? tsv[b] + 1 : tsv[b] + 2 + rnd.nextInt(1000));

        final long[] rowTs = new long[3];
        int written = 0;
        for (int r = 0, newRows = rnd.nextInt(4); r < newRows; r++) {
            // Draw an unused in-band ts; a saturated band (all in-band values
            // already used - possible only for a 1-microsecond band) skips the row.
            for (int attempt = 0; attempt < 16; attempt++) {
                final long t = lo + rnd.nextLong(hi - lo);
                if (usedTs.add(t)) {
                    rowTs[written++] = t;
                    break;
                }
            }
        }
        if (written > 1 && rnd.nextBoolean()) {
            Arrays.sort(rowTs, 0, written);
        }
        try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
            for (int r = 0; r < written; r++) {
                appendRow(
                        walWriter,
                        rowTs[r],
                        rnd.nextInt(20) == 0 ? -1 : rnd.nextInt(symCount),
                        rnd.nextInt(20) == 0 ? Numbers.LONG_NULL : (rnd.nextInt(2001) - 1000),
                        rnd.nextDouble() * 1000.0
                );
            }
            walWriter.commitWithParams(lo, hi, WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE);
        }
    }

    // Converts settled base partitions in [loTs, hiTs) to parquet through the
    // WAL apply path. Tolerates the benign outcomes the parquet-conversion fuzz
    // op also tolerates (an empty range matching no partition, a partition that
    // could not be converted, or one already parquet) so an unlucky dataset
    // shape does not fail the run.
    private void convertPartitionsToParquet(long loTs, long hiTs) throws SqlException {
        if (hiTs <= loTs) {
            return;
        }
        try {
            execute("ALTER TABLE base CONVERT PARTITION TO PARQUET WHERE ts >= " + loTs + " AND ts < " + hiTs);
            drainWalQueue();
        } catch (CairoException e) {
            final CharSequence msg = e.getFlyweightMessage();
            if (Chars.contains(msg, "no partitions matched WHERE clause")
                    || Chars.contains(msg, "could not convert partition")
                    || Chars.contains(msg, "already a parquet partition")) {
                return;
            }
            throw e;
        }
    }

    // Physical row count of a table (post-eviction base row count for the TTL
    // arm, so the fuzz can assert TTL actually evicted partitions).
    private long countRows(String table) throws SqlException {
        try (
                RecordCursorFactory factory = select("SELECT count() FROM " + table);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(cursor.hasNext());
            return cursor.getRecord().getLong(0);
        }
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

    // Removal traffic for the dedup arm that keeps the recompute oracle sound.
    // Inserts a small batch into a far-future partition (strictly above all real
    // data and the current frontier) and drops it BEFORE the LV refreshes, so the
    // LV never emits these rows. TRUNCATE / DROP PARTITION of already-emitted rows
    // would freeze the derived prefix and make the LV path-dependent (LV keeps the
    // rows, the recompute over the shrunken base does not) - only removals confined
    // to the unprocessed window stay oracle-checkable. On the next cycle the applied
    // base no longer holds the doomed rows; the DROP PARTITION seqTxn advances the
    // dedup divergence watermark, so the coupled dispatch must route through the
    // applied reader, not the raw-WAL fast path - which would wrongly append the
    // doomed (above-frontier) rows and be caught by the differential oracle.
    private void dropDoomedFuturePartition(StringSink sink, LiveViewRefreshJob job, Rnd rnd) throws Exception {
        final long doomed = MicrosTimestampDriver.floor("2030-01-01T00:00:00.000000Z");
        final int rows = 1 + rnd.nextInt(3);
        sink.clear();
        sink.put("INSERT INTO base (ts, sym, i, x) VALUES ");
        for (int r = 0; r < rows; r++) {
            if (r > 0) {
                sink.put(',');
            }
            sink.put('(').put(doomed + r).put("::timestamp,'")
                    .put(SYMBOLS[rnd.nextInt(SYMBOLS.length)]).put("',")
                    .put(rnd.nextInt(100)).put(',').put(rnd.nextDouble() * 100.0).put(')');
        }
        execute(sink);
        drainWalQueue();
        execute("ALTER TABLE base DROP PARTITION LIST '2030-01-01'");
        drainWalQueue();
        refreshCycle(job);
    }

    // One random transparent base-DDL op: ADD a fresh unreferenced column, or
    // DROP / RENAME / retype (INT<->LONG) one of the previously-added extras.
    // Never touches (ts, sym, i, x), so the view must stay ACTIVE throughout.
    // names/types track the live extras; seq[0] keeps generated names unique
    // across drops.
    private void emitTransparentBaseDdl(Rnd rnd, ObjList<String> names, IntList types, int[] seq) throws Exception {
        final int op = names.size() == 0 ? 0 : rnd.nextInt(4);
        switch (op) {
            case 0 -> {
                final String name = "extra" + seq[0]++;
                final int type = rnd.nextInt(2);
                execute("ALTER TABLE base ADD COLUMN " + name + (type == 0 ? " INT" : " LONG"));
                names.add(name);
                types.add(type);
            }
            case 1 -> {
                final int idx = rnd.nextInt(names.size());
                execute("ALTER TABLE base DROP COLUMN " + names.getQuick(idx));
                names.remove(idx);
                types.removeIndex(idx);
            }
            case 2 -> {
                final int idx = rnd.nextInt(names.size());
                final String renamed = "extra" + seq[0]++;
                execute("ALTER TABLE base RENAME COLUMN " + names.getQuick(idx) + " TO " + renamed);
                names.setQuick(idx, renamed);
            }
            case 3 -> {
                final int idx = rnd.nextInt(names.size());
                final int newType = 1 - types.getQuick(idx);
                execute("ALTER TABLE base ALTER COLUMN " + names.getQuick(idx)
                        + " TYPE " + (newType == 0 ? "INT" : "LONG"));
                types.setQuick(idx, newType);
            }
            default -> throw new IllegalStateException("op=" + op);
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

    // Inserts the full logical dataset into the named table in ts order, in
    // chunks of 200 rows per INSERT. Builds the shadow "logical base" the
    // removal freeze-and-continue fuzz recomputes its oracle over.
    private void insertLogicalDataset(
            String table,
            long[] tsv,
            int[] symIdx,
            long[] iv,
            double[] xv,
            boolean[] xNull
    ) throws Exception {
        final StringSink sink = new StringSink();
        for (int from = 0, rowCount = tsv.length; from < rowCount; from += 200) {
            final int to = Math.min(rowCount, from + 200);
            sink.clear();
            sink.put("INSERT INTO ").put(table).put(" (ts, sym, i, x) VALUES ");
            for (int k = from; k < to; k++) {
                if (k > from) {
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
                sink.put(')');
            }
            execute(sink);
        }
        drainWalQueue();
    }

    // Inserts rows [from, to) of order (the shuffled segment) into the ns base in
    // one commit. Mirrors insertCommit but casts each timestamp to TIMESTAMP_NS so
    // the numeric literal is read as nanoseconds since epoch, matching the base's
    // ns designated timestamp.
    private void insertNsCommit(
            StringSink sink,
            int[] order,
            int from,
            int to,
            long[] tsv,
            int[] symIdx,
            long[] iv,
            double[] xv,
            boolean[] xNull
    ) throws Exception {
        if (from >= to) {
            return;
        }
        sink.clear();
        sink.put("INSERT INTO base (ts, sym, i, x) VALUES ");
        for (int r = from; r < to; r++) {
            final int k = order[r];
            if (r > from) {
                sink.put(',');
            }
            sink.put('(').put(tsv[k]).put("::timestamp_ns,");
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
            sink.put(')');
        }
        execute(sink);
    }

    // Inserts the var-length rows [from, to) of order (the shuffled segment) into
    // the var-size base table in one commit. Mirrors insertCommit, but the base
    // schema is (ts, vs, vv, vb, va) and each row's pre-rendered value tuple lives
    // in tuple[k]. The commit-order shuffle is what produces O3.
    private void insertVarSizeCommit(StringSink sink, int[] order, int from, int to, long[] tsv, String[] tuple) throws Exception {
        if (from >= to) {
            return;
        }
        sink.clear();
        sink.put("INSERT INTO base (ts, vs, vv, vb, va) VALUES ");
        for (int r = from; r < to; r++) {
            final int k = order[r];
            if (r > from) {
                sink.put(',');
            }
            sink.put('(').put(tsv[k]).put("::timestamp,").put(tuple[k]).put(')');
        }
        execute(sink);
    }

    // A writer thread that owns its own WalWriter and ingests the round-robin slice
    // [fromIndex+writerId, rowCount) with stride numWriters, committing every batch
    // rows. The slices are disjoint and globally ts-ordered, so timestamps stay
    // unique across writers; the cross-writer commit interleaving is what produces
    // O3 and apply-ahead batching. The thread awaits the barrier before its first
    // write and clears thread-locals on exit for the leak check.
    private Thread newConcurrentWriterThread(
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
        }, "lv-cw-writer-" + writerId);
    }

    // A reader thread that loops the given query over the live view until
    // stopped, asserting the prefix-consistency invariant on every snapshot:
    // per-reader monotonic row count, strictly-ascending ts, gapless
    // row_number 1..N, and ts-derived var-length passthroughs (vs decodes to
    // the decimal ts, vv to 'v' + the decimal ts). Columns are
    // (ts, vs, vv, i, rn). Any violation - including a torn var-length read -
    // lands in the errors queue and fails the run.
    private Thread newPrefixInvariantReader(
            String sql,
            AtomicBoolean running,
            ConcurrentLinkedQueue<Throwable> errors,
            AtomicLong rowsValidated
    ) {
        return new Thread(() -> {
            long lastCount = 0;
            try (SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine)) {
                while (running.get()) {
                    try (
                            SqlCompiler compiler = engine.getSqlCompiler();
                            RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory();
                            RecordCursor cursor = factory.getCursor(ctx)
                    ) {
                        final Record record = cursor.getRecord();
                        long prevTs = Long.MIN_VALUE;
                        long expectedRn = 1;
                        long count = 0;
                        while (cursor.hasNext()) {
                            final long rowTs = record.getTimestamp(0);
                            final CharSequence vs = record.getStrA(1);
                            final Utf8Sequence vv = record.getVarcharA(2);
                            final long rn = record.getLong(4);
                            if (rowTs <= prevTs) {
                                throw new AssertionError("ts not strictly ascending [sql=" + sql
                                        + ", prevTs=" + prevTs + ", ts=" + rowTs + ']');
                            }
                            if (rn != expectedRn) {
                                throw new AssertionError("rn not a gapless 1..N sequence [sql=" + sql
                                        + ", expected=" + expectedRn + ", actual=" + rn + ", ts=" + rowTs + ']');
                            }
                            if (vs == null || Numbers.parseLong(vs) != rowTs) {
                                throw new AssertionError("vs STRING passthrough torn [sql=" + sql
                                        + ", ts=" + rowTs + ", vs=" + vs + ']');
                            }
                            if (vv == null || vv.size() < 2 || vv.byteAt(0) != 'v'
                                    || Numbers.parseLong(vv, 1, vv.size()) != rowTs) {
                                throw new AssertionError("vv VARCHAR passthrough torn [sql=" + sql
                                        + ", ts=" + rowTs + ", vv=" + vv + ']');
                            }
                            prevTs = rowTs;
                            expectedRn++;
                            count++;
                        }
                        if (count < lastCount) {
                            throw new AssertionError("row count not monotonic [sql=" + sql
                                    + ", last=" + lastCount + ", now=" + count + ']');
                        }
                        lastCount = count;
                        rowsValidated.addAndGet(count);
                    }
                }
            } catch (Throwable th) {
                errors.add(th);
            } finally {
                Path.clearThreadLocals();
            }
        }, "lv-rvr-reader");
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

    // Differential fuzz for an anchored window variant. Mirrors runFuzz's
    // ingestion shape (pre-CREATE backfill history, then per-commit O3 refresh
    // with optional quiescent restarts) but cross-checks against a DISTINCT oracle
    // SQL: the anchored live view vs. the equivalent (sym, bucket)-partitioned
    // regular window over the base. O3 is always on (segmentOrder shuffles), so
    // the anchor map is rebuilt on head-miss / head-hit replay and is verified to
    // agree with the from-scratch recompute after quiescence.
    private void runAnchoredFuzz(
            Rnd rnd,
            int variant,
            int rowCount,
            boolean restart,
            boolean backfill
    ) throws Exception {
        // Pin the clock a day below the data, like runFuzz: a non-backfill view's
        // lower bound is the CREATE moment, and O3 head-miss replay only re-emits
        // rows at or above it, so the clock must sit below every data timestamp.
        if (currentMicros < 0) {
            setCurrentMicros(MicrosTimestampDriver.floor("2025-12-31T00:00:00.000000Z"));
        }

        // A monotonic anchor: timestamp_floor is non-decreasing in ts, so the
        // anchor value changes exactly at bucket boundaries and never repeats.
        // "Reset on change" is then identical to "partition by the bucket value",
        // which is what the oracle does. '1h' yields frequent resets, '1d' coarse
        // ones; both are exercised across a run batch.
        final String bucket = rnd.nextBoolean() ? "timestamp_floor('1h', ts)" : "timestamp_floor('1d', ts)";
        final int symCount = 1 + rnd.nextInt(SYMBOLS.length);
        final int stepMode = rnd.nextInt(3);
        final int baseStepMax = stepMode == 0 ? 5_000_000 : stepMode == 1 ? 60_000_000 : 900_000_000;
        final int dayJumpEvery = stepMode == 0 ? 20 : 12;

        final String viewSql = "SELECT " + anchoredViewProjection(variant) + " FROM base "
                + "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION " + bucket + ")";
        final String oracleSql = "SELECT " + anchoredOracleProjection(variant, bucket) + " FROM base";
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + (backfill ? "BACKFILL " : "")
                + "AS " + viewSql;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        LOG.info().$("LV anchored fuzz: variant=").$(variant).$(", rows=").$(rowCount)
                .$(", symCount=").$(symCount).$(", stepMode=").$(stepMode)
                .$(", restart=").$(restart).$(", backfill=").$(backfill)
                .$(", bucket=").$(bucket).$(", sql=").$(viewSql).$();

        // Strictly-unique, strictly-increasing timestamps so ts is a total order;
        // random symbols and values with occasional NULLs.
        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        final boolean[] xNull = new boolean[rowCount];
        final int maxDayJumps = 30;
        int dayJumps = 0;
        long ts = MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z");
        for (int k = 0; k < rowCount; k++) {
            ts += 1 + rnd.nextInt(baseStepMax);
            if (dayJumps < maxDayJumps && rnd.nextInt(dayJumpEvery) == 0) {
                ts += 86_400_000_000L;
                dayJumps++;
            }
            tsv[k] = ts;
            symIdx[k] = rnd.nextInt(20) == 0 ? -1 : rnd.nextInt(symCount);
            iv[k] = rnd.nextInt(20) == 0 ? Numbers.LONG_NULL : (rnd.nextInt(2001) - 1000);
            xNull[k] = rnd.nextInt(20) == 0;
            xv[k] = rnd.nextDouble() * 1000.0;
        }

        // Backfill captures pre-CREATE history: put the earliest rows before CREATE
        // so the backfill floor sits at the global-min ts and no post-CREATE O3 row
        // falls below it. Non-backfill: everything lands post-CREATE.
        final int preCount = backfill ? rnd.nextInt(rowCount + 1) : 0;

        final StringSink sink = new StringSink();
        LiveViewRefreshJob job = null;
        try {
            if (preCount > 0) {
                final int[] preOrder = segmentOrder(rnd, 0, preCount, true);
                final int[] cb = commitBounds(rnd, preOrder.length);
                for (int c = 0; c + 1 < cb.length; c++) {
                    insertCommit(sink, preOrder, cb[c], cb[c + 1], tsv, symIdx, iv, xv, xNull, null);
                    drainWalQueue();
                }
            }

            execute(createSql);
            job = new LiveViewRefreshJob(0, engine, 1);

            if (backfill) {
                driveBackfillToCompletion(job, "lv");
            }

            if (preCount < rowCount) {
                final int[] postOrder = segmentOrder(rnd, preCount, rowCount, true);
                final int[] cb = commitBounds(rnd, postOrder.length);
                for (int c = 0; c + 1 < cb.length; c++) {
                    insertCommit(sink, postOrder, cb[c], cb[c + 1], tsv, symIdx, iv, xv, xNull, null);
                    drainWalQueue();
                    refreshCycle(job);

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
        } finally {
            Misc.free(job);
        }

        // The oracle: the anchored live view must equal the equivalent
        // (sym, bucket)-partitioned window recomputed over the base table.
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + oracleSql + ") ORDER BY 1",
                "(lv) ORDER BY 1",
                LOG,
                true
        );

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
    }

    // Transparent-DDL fuzz (see testFuzzBaseDdlTransparent). Mirrors runFuzz's
    // ingestion shape (pre-CREATE backfill history, per-commit O3 refresh,
    // optional quiescent restarts), but between post-CREATE commits the base
    // receives random ADD / DROP / RENAME / retype DDL on columns the view
    // never references. Each DDL is followed by a stays-ACTIVE assertion and a
    // walk-past refresh cycle; the run ends on the standard recompute oracle.
    private void runBaseDdlFuzz(
            Rnd rnd,
            int variant,
            int rowCount,
            boolean restart,
            boolean backfill,
            boolean inMemory
    ) throws Exception {
        // Pin the clock a day below the data, like runFuzz: a non-backfill view's
        // lower bound is the CREATE moment, and O3 head-miss replay only re-emits
        // rows at or above it, so the clock must sit below every data timestamp.
        if (currentMicros < 0) {
            setCurrentMicros(MicrosTimestampDriver.floor("2025-12-31T00:00:00.000000Z"));
        }

        final int n = 1 + rnd.nextInt(MAX_FRAME);
        final int symCount = 1 + rnd.nextInt(SYMBOLS.length);
        final int stepMode = rnd.nextInt(3);
        final int baseStepMax = stepMode == 0 ? 5_000_000 : stepMode == 1 ? 60_000_000 : 900_000_000;
        final int dayJumpEvery = stepMode == 0 ? 20 : 12;
        final boolean withWhere = rnd.nextInt(3) == 0;

        final String viewSql = "SELECT " + projection(variant, n) + " FROM base" + (withWhere ? " WHERE i > 0" : "");
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + (inMemory ? "IN MEMORY 60s " : "")
                + (backfill ? "BACKFILL " : "")
                + "AS " + viewSql;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        LOG.info().$("LV base-DDL fuzz: variant=").$(variant).$(", rows=").$(rowCount)
                .$(", n=").$(n).$(", symCount=").$(symCount).$(", stepMode=").$(stepMode)
                .$(", restart=").$(restart).$(", backfill=").$(backfill).$(", inMem=").$(inMemory)
                .$(", where=").$(withWhere).$(", sql=").$(viewSql).$();

        // Strictly-unique, strictly-increasing timestamps so ts is a total order;
        // random symbols and values with occasional NULLs.
        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        final boolean[] xNull = new boolean[rowCount];
        final int maxDayJumps = 30;
        int dayJumps = 0;
        long ts = MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z");
        for (int k = 0; k < rowCount; k++) {
            ts += 1 + rnd.nextInt(baseStepMax);
            if (dayJumps < maxDayJumps && rnd.nextInt(dayJumpEvery) == 0) {
                ts += 86_400_000_000L;
                dayJumps++;
            }
            tsv[k] = ts;
            symIdx[k] = rnd.nextInt(20) == 0 ? -1 : rnd.nextInt(symCount);
            iv[k] = rnd.nextInt(20) == 0 ? Numbers.LONG_NULL : (rnd.nextInt(2001) - 1000);
            xNull[k] = rnd.nextInt(20) == 0;
            xv[k] = rnd.nextDouble() * 1000.0;
        }

        // Backfill captures pre-CREATE history: the earliest rows go before CREATE
        // so the backfill floor sits at the global-min ts and no post-CREATE O3 row
        // falls below it. Non-backfill: everything lands post-CREATE.
        final int preCount = backfill ? rnd.nextInt(rowCount + 1) : 0;

        // Live unreferenced extras: names, their current type (0=INT, 1=LONG),
        // and a monotonic name counter surviving drops.
        final ObjList<String> extraNames = new ObjList<>();
        final IntList extraTypes = new IntList();
        final int[] extraSeq = {0};

        final StringSink sink = new StringSink();
        LiveViewRefreshJob job = null;
        try {
            if (preCount > 0) {
                final int[] preOrder = segmentOrder(rnd, 0, preCount, true);
                final int[] cb = commitBounds(rnd, preOrder.length);
                for (int c = 0; c + 1 < cb.length; c++) {
                    insertCommit(sink, preOrder, cb[c], cb[c + 1], tsv, symIdx, iv, xv, xNull, null);
                    drainWalQueue();
                }
            }

            execute(createSql);
            job = new LiveViewRefreshJob(0, engine, 1);

            if (backfill) {
                driveBackfillToCompletion(job, "lv");
            }

            if (preCount < rowCount) {
                final int[] postOrder = segmentOrder(rnd, preCount, rowCount, true);
                final int[] cb = commitBounds(rnd, postOrder.length);
                for (int c = 0; c + 1 < cb.length; c++) {
                    insertCommit(sink, postOrder, cb[c], cb[c + 1], tsv, symIdx, iv, xv, xNull, null);
                    drainWalQueue();
                    refreshCycle(job);

                    if (rnd.nextInt(3) == 0) {
                        emitTransparentBaseDdl(rnd, extraNames, extraTypes, extraSeq);
                        drainWalQueue();
                        LiveViewInstance inst = engine.getLiveViewRegistry().getViewInstance("lv");
                        Assert.assertNotNull(inst);
                        Assert.assertFalse("unreferenced base DDL must not invalidate the LV", inst.isInvalid());
                        refreshCycle(job); // walk past the structural seqTxn
                    }

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
        } finally {
            Misc.free(job);
        }

        // The oracle: the live view must equal the window query recomputed over
        // the base table (whose unreferenced columns may have churned freely).
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

    // Invalidation-transition fuzz (see testFuzzBaseDdlInvalidation). Ingests a
    // randomized dataset, quiesces, snapshots the view, then fires ONE
    // invalidating base-DDL op and asserts the transition end to end: the view
    // flips INVALID with the operation-specific reason, its materialized output
    // stays queryable and byte-identical to the snapshot, and further base
    // ingestion (where the base still exists) never reaches it.
    // op: 0 = DROP referenced column, 1 = RENAME referenced column,
    // 2 = retype referenced column, 3 = RENAME TABLE base, 4 = DROP TABLE base.
    private void runBaseDdlInvalidationFuzz(Rnd rnd, int op, int rowCount) throws Exception {
        // Pin the clock a day below the data, like runFuzz.
        if (currentMicros < 0) {
            setCurrentMicros(MicrosTimestampDriver.floor("2025-12-31T00:00:00.000000Z"));
        }

        final int n = 1 + rnd.nextInt(MAX_FRAME);
        final int symCount = 1 + rnd.nextInt(SYMBOLS.length);
        final boolean inMemory = rnd.nextBoolean();
        // Column ops target the value column the projection reads: variant 0
        // reads i, variant 4 reads x. Table-level ops draw any fixed-width shape.
        final int variant = op <= 2
                ? (rnd.nextBoolean() ? 0 : 4)
                : FIXED_WIDTH_VARIANTS[rnd.nextInt(FIXED_WIDTH_VARIANTS.length)];
        final String column = variant == 4 ? "x" : "i";
        final String otherColumn = variant == 4 ? "i" : "x";

        final String viewSql = "SELECT " + projection(variant, n) + " FROM base";
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + (inMemory ? "IN MEMORY 60s " : "")
                + "AS " + viewSql;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("DROP TABLE IF EXISTS base2");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        LOG.info().$("LV base-DDL invalidation fuzz: op=").$(op).$(", variant=").$(variant)
                .$(", rows=").$(rowCount).$(", n=").$(n).$(", symCount=").$(symCount)
                .$(", inMem=").$(inMemory).$(", sql=").$(viewSql).$();

        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        final boolean[] xNull = new boolean[rowCount];
        long ts = MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z");
        for (int k = 0; k < rowCount; k++) {
            ts += 1 + rnd.nextInt(60_000_000);
            tsv[k] = ts;
            symIdx[k] = rnd.nextInt(20) == 0 ? -1 : rnd.nextInt(symCount);
            iv[k] = rnd.nextInt(20) == 0 ? Numbers.LONG_NULL : (rnd.nextInt(2001) - 1000);
            xNull[k] = rnd.nextInt(20) == 0;
            xv[k] = rnd.nextDouble() * 1000.0;
        }

        final StringSink sink = new StringSink();
        final StringSink snapshot = new StringSink();
        LiveViewRefreshJob job = null;
        try {
            execute(createSql);
            job = new LiveViewRefreshJob(0, engine, 1);

            final int[] order = segmentOrder(rnd, 0, rowCount, true);
            final int[] cb = commitBounds(rnd, order.length);
            for (int c = 0; c + 1 < cb.length; c++) {
                insertCommit(sink, order, cb[c], cb[c + 1], tsv, symIdx, iv, xv, xNull, null);
                drainWalQueue();
                refreshCycle(job);
            }
            driveRefreshToQuiescence(job);
            printSql("(lv) ORDER BY 1", snapshot);

            final String expectedReason;
            switch (op) {
                case 0 -> {
                    execute("ALTER TABLE base DROP COLUMN " + column);
                    expectedReason = "drop column operation";
                }
                case 1 -> {
                    execute("ALTER TABLE base RENAME COLUMN " + column + " TO " + column + "_r");
                    expectedReason = "rename column operation";
                }
                case 2 -> {
                    execute("ALTER TABLE base ALTER COLUMN " + column + " TYPE VARCHAR");
                    expectedReason = "change column type operation";
                }
                case 3 -> {
                    execute("RENAME TABLE base TO base2");
                    expectedReason = "base table rename";
                }
                case 4 -> {
                    execute("DROP TABLE base");
                    expectedReason = "base table drop";
                }
                default -> throw new IllegalStateException("op=" + op);
            }
            drainWalQueue();
            refreshCycle(job);

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertTrue("op " + op + " must invalidate the LV", instance.isInvalid());
            Assert.assertTrue(
                    "wrong invalidation reason [op=" + op + ", reason=" + instance.getInvalidationReason() + ']',
                    Chars.contains(instance.getInvalidationReason(), expectedReason)
            );

            // The invalid view keeps serving its materialized rows unchanged.
            sink.clear();
            printSql("(lv) ORDER BY 1", sink);
            TestUtils.assertEquals("invalidation must not touch materialized output", snapshot, sink);

            // Where the base still exists (column ops), further ingestion must
            // never reach the invalid view.
            if (op <= 2) {
                execute("INSERT INTO base (ts, sym, " + otherColumn + ") VALUES ("
                        + (tsv[rowCount - 1] + 1) + "::timestamp, 'AA', 1), ("
                        + (tsv[rowCount - 1] + 2) + "::timestamp, 'BB', 2)");
                drainWalQueue();
                driveRefreshToQuiescence(job);

                Assert.assertTrue("LV must stay invalid", instance.isInvalid());
                sink.clear();
                printSql("(lv) ORDER BY 1", sink);
                TestUtils.assertEquals("post-invalidation ingestion must not reach the view", snapshot, sink);
            }
        } finally {
            Misc.free(job);
        }

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE IF EXISTS base");
        execute("DROP TABLE IF EXISTS base2");
    }

    // Freeze-and-continue fuzz where base-table TTL is the removal trigger (see
    // testFuzzBaseTtl). Phase 1 ingests and quiesces a multi-partition dataset
    // over an intact base; a single far-ahead trigger row then advances the base
    // max past the TTL window, evicting the phase-1 partitions at apply time -
    // the automatic-retention counterpart to an explicit DROP PARTITION. The view
    // must walk past the eviction (stay ACTIVE, freeze its emitted rows), so
    // phase 2 continues strictly in order on top of the frozen state (an O3
    // replay after a removal is path-dependent - see runRemovalFreezeContinueFuzz).
    // The run-end oracle recomputes over a shadow table holding the LOGICAL
    // dataset (every row ever inserted): the walk-past keeps the window
    // accumulators, so post-eviction rows continue as if the evicted rows still
    // existed. Parametrized over the partition unit (DAY / HOUR) with a matched
    // TTL granularity, plus restart and IN MEMORY.
    private void runBaseTtlFuzz(
            Rnd rnd,
            int variant,
            int rowCount,
            boolean hourPartition,
            boolean restart,
            boolean inMemory
    ) throws Exception {
        // Re-pin the clock a day below the data start on EVERY call (not just the
        // first): this arm later advances the wall clock ABOVE the data to arm TTL
        // (which evicts relative to min(maxTimestamp, wallClock)), so the shared
        // per-test clock would otherwise sit above the next variant's data and
        // push its view lower bound past the early rows. The lower bound is fixed
        // at CREATE, below every row, so the forward-append path still emits all.
        setCurrentMicros(MicrosTimestampDriver.floor("2025-12-31T00:00:00.000000Z"));

        final int n = 1 + rnd.nextInt(MAX_FRAME);
        final int symCount = 1 + rnd.nextInt(SYMBOLS.length);
        final boolean withWhere = rnd.nextInt(3) == 0;

        // Partition unit + a matched TTL granularity (an hour TTL needs sub-day
        // partitioning; a day TTL pairs with day partitioning). A one-unit window
        // evicts every partition older than one partition width below the max.
        final String partUnit = hourPartition ? "HOUR" : "DAY";
        final long partWidth = hourPartition ? 3_600_000_000L : 86_400_000_000L;
        final String ttlClause = "TTL 1 " + partUnit;

        final String projection = projection(variant, n);
        final String where = withWhere ? " WHERE i > 0" : "";
        final String viewSql = "SELECT " + projection + " FROM base" + where;
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + (inMemory ? "IN MEMORY 60s " : "")
                + "AS " + viewSql;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("DROP TABLE IF EXISTS shadow");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY "
                + partUnit + " " + ttlClause + " WAL");

        LOG.info().$("LV base-TTL fuzz: variant=").$(variant).$(", rows=").$(rowCount)
                .$(", n=").$(n).$(", symCount=").$(symCount).$(", partUnit=").$(partUnit)
                .$(", ttlClause=").$(ttlClause).$(", restart=").$(restart).$(", inMem=").$(inMemory)
                .$(", where=").$(withWhere).$(", sql=").$(viewSql).$();

        // Strictly-unique, strictly-increasing timestamps so ts is a total order.
        // Phase-1 rows [0, splitPoint) span several partitions (a partition-width
        // step every few rows). At splitPoint a big forward jump - well past the
        // TTL window from phase-1's tail - so applying the trigger row there
        // evicts every phase-1 partition. Phase-2 rows continue just above it.
        final int splitPoint = rowCount / 3 + rnd.nextInt(rowCount / 3);
        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        final boolean[] xNull = new boolean[rowCount];
        long ts = MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z");
        for (int k = 0; k < rowCount; k++) {
            if (k == splitPoint) {
                // Jump the trigger row three partition widths past phase-1's tail
                // so applying it advances the max well past the one-unit TTL
                // window and evicts every phase-1 partition.
                ts += 3 * partWidth;
            } else {
                ts += 1 + rnd.nextInt(1_000_000);
                if (rnd.nextInt(6) == 0) {
                    ts += partWidth; // cross a partition boundary
                }
            }
            tsv[k] = ts;
            symIdx[k] = rnd.nextInt(20) == 0 ? -1 : rnd.nextInt(symCount);
            iv[k] = rnd.nextInt(20) == 0 ? Numbers.LONG_NULL : (rnd.nextInt(2001) - 1000);
            xNull[k] = rnd.nextInt(20) == 0;
            xv[k] = rnd.nextDouble() * 1000.0;
        }

        final StringSink sink = new StringSink();
        final StringSink preRemoval = new StringSink();
        LiveViewRefreshJob job = null;
        try {
            execute(createSql);
            job = new LiveViewRefreshJob(0, engine, 1);

            // Phase 1: O3 churn over an intact base, per-commit refresh.
            final int[] phase1Order = segmentOrder(rnd, 0, splitPoint, true);
            final int[] cb = commitBounds(rnd, phase1Order.length);
            for (int c = 0; c + 1 < cb.length; c++) {
                insertCommit(sink, phase1Order, cb[c], cb[c + 1], tsv, symIdx, iv, xv, xNull, null);
                drainWalQueue();
                refreshCycle(job);

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
            driveRefreshToQuiescence(job);

            // Intact-base checkpoint of the oracle: logical == physical here.
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "(" + viewSql + ") ORDER BY 1",
                    "(lv) ORDER BY 1",
                    LOG,
                    true
            );
            printSql("(lv) ORDER BY 1", preRemoval);

            final long baseRowsBefore = countRows("base");

            // The TTL trigger: advance the wall clock above the trigger row so
            // TTL keys off the data max, then insert the single far-ahead row.
            // Its apply advances the base max past the window and evicts every
            // phase-1 partition.
            setCurrentMicros(tsv[splitPoint] + partWidth);
            final int[] triggerOrder = {splitPoint};
            insertCommit(sink, triggerOrder, 0, 1, tsv, symIdx, iv, xv, xNull, null);
            drainWalQueue();
            refreshCycle(job); // walk past the eviction seqTxn

            final long baseRowsAfter = countRows("base");
            Assert.assertTrue(
                    "base TTL must have evicted phase-1 partitions [before=" + baseRowsBefore
                            + ", after=" + baseRowsAfter + ']',
                    baseRowsAfter < baseRowsBefore
            );

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertFalse("LV must stay valid after base TTL eviction", instance.isInvalid());
            // The eviction must not retract emitted rows. The trigger row's ts is
            // the highest so far, so under ORDER BY 1 it sorts last (or is
            // filtered out by the optional WHERE) - either way the pre-removal
            // output stays an exact prefix of the current output.
            sink.clear();
            printSql("(lv) ORDER BY 1", sink);
            Assert.assertTrue(
                    "TTL eviction must not retract emitted LV rows [pre=\n" + preRemoval + "\npost=\n" + sink + ']',
                    sink.toString().startsWith(preRemoval.toString())
            );

            // Phase 2: strictly in-order continuation on top of the frozen state.
            final int[] phase2Order = segmentOrder(rnd, splitPoint + 1, rowCount, false);
            final int[] cb2 = commitBounds(rnd, phase2Order.length);
            for (int c = 0; c + 1 < cb2.length; c++) {
                insertCommit(sink, phase2Order, cb2[c], cb2[c + 1], tsv, symIdx, iv, xv, xNull, null);
                drainWalQueue();
                refreshCycle(job);

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
            driveRefreshToQuiescence(job);
        } finally {
            Misc.free(job);
        }

        // The oracle: the view must equal the window query recomputed over the
        // LOGICAL dataset - every generated row, as if nothing was evicted -
        // materialized into the shadow table in ts order.
        execute("CREATE TABLE shadow (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        insertLogicalDataset("shadow", tsv, symIdx, iv, xv, xNull);
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(SELECT " + projection + " FROM shadow" + where + ") ORDER BY 1",
                "(lv) ORDER BY 1",
                LOG,
                true
        );

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
        execute("DROP TABLE shadow");
    }

    // Differential fuzz where the base is ingested by several concurrent WalWriters
    // (see testFuzzConcurrentWriters). Mirrors runFuzz's dataset shape - unique,
    // strictly-increasing timestamps; optional pre-CREATE BACKFILL history - but the
    // post-CREATE suffix is split into disjoint round-robin slices, one per writer
    // thread, that commit in parallel so the sequencer interleaves their
    // transactions. With concurrentRefresh a single refresh driver runs alongside
    // ingestion (steady-state timing, so the base apply job races ahead of the LV
    // trigger - the apply-ahead path); otherwise the refresh runs single-threaded
    // after the writers join. Either way the view is quiesced before the exact-
    // equality recompute oracle.
    private void runConcurrentWriterFuzz(
            Rnd rnd,
            int variant,
            int rowCount,
            int numWriters,
            boolean concurrentRefresh,
            boolean backfill
    ) throws Exception {
        // A concurrent refresh driver advances the flush clock in an unbounded loop
        // while ingestion is in flight, so - unlike the single-threaded fuzz arms,
        // which sit a day below the data - the clock is pinned a full YEAR below the
        // data start. That keeps it under a non-backfill view's CREATE-moment lower
        // bound for the whole run (a 250ms/advance loop never climbs a year), so
        // head-miss replay never drops a row the recompute keeps.
        setCurrentMicros(MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z"));
        final long dataStart = MicrosTimestampDriver.floor("2027-01-01T00:00:00.000000Z");

        final int n = 1 + rnd.nextInt(MAX_FRAME);
        final int symCount = 1 + rnd.nextInt(SYMBOLS.length);
        final int stepMode = rnd.nextInt(3);
        final int baseStepMax = stepMode == 0 ? 5_000_000 : stepMode == 1 ? 60_000_000 : 900_000_000;
        final int dayJumpEvery = stepMode == 0 ? 20 : 12;
        final boolean withWhere = rnd.nextInt(3) == 0;

        final String viewSql = "SELECT " + projection(variant, n) + " FROM base" + (withWhere ? " WHERE i > 0" : "");
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + (backfill ? "BACKFILL " : "")
                + "AS " + viewSql;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        LOG.info().$("LV concurrent-writer fuzz: variant=").$(variant).$(", rows=").$(rowCount)
                .$(", n=").$(n).$(", writers=").$(numWriters).$(", symCount=").$(symCount)
                .$(", stepMode=").$(stepMode).$(", concurrentRefresh=").$(concurrentRefresh)
                .$(", backfill=").$(backfill).$(", where=").$(withWhere).$(", sql=").$(viewSql).$();

        // Strictly-unique, strictly-increasing timestamps so ts is a total order;
        // random symbols and values with occasional NULLs.
        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        final int maxDayJumps = 30;
        int dayJumps = 0;
        long ts = dataStart;
        for (int k = 0; k < rowCount; k++) {
            ts += 1 + rnd.nextInt(baseStepMax);
            if (dayJumps < maxDayJumps && rnd.nextInt(dayJumpEvery) == 0) {
                ts += 86_400_000_000L;
                dayJumps++;
            }
            tsv[k] = ts;
            symIdx[k] = rnd.nextInt(20) == 0 ? -1 : rnd.nextInt(symCount);
            iv[k] = rnd.nextInt(20) == 0 ? Numbers.LONG_NULL : (rnd.nextInt(2001) - 1000);
            xv[k] = rnd.nextDouble() * 1000.0;
        }

        // Backfill captures pre-CREATE history: the earliest rows [0, preCount) go
        // before CREATE so the backfill floor sits at the global-min ts and no
        // concurrent post-CREATE row falls below it. Non-backfill: everything lands
        // post-CREATE via the concurrent writers.
        final int preCount = backfill ? rnd.nextInt(rowCount + 1) : 0;

        final TableToken baseToken = engine.verifyTableName("base");
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        LiveViewRefreshJob job = null;
        try {
            // Pre-CREATE history (single-writer, in ts order): keeps the backfill
            // floor at the global-min ts.
            if (preCount > 0) {
                try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                    for (int k = 0; k < preCount; k++) {
                        appendRow(walWriter, tsv[k], symIdx[k], iv[k], xv[k]);
                    }
                    walWriter.commit();
                }
                drainWalQueue();
            }

            execute(createSql);
            job = new LiveViewRefreshJob(0, engine, 1);

            if (backfill) {
                driveBackfillToCompletion(job, "lv");
            }

            // Concurrent suffix [preCount, rowCount): numWriters threads each own a
            // WalWriter and commit a disjoint round-robin slice in parallel, with an
            // optional refresh driver racing them.
            if (preCount < rowCount) {
                final int driverCount = concurrentRefresh ? 1 : 0;
                final CyclicBarrier barrier = new CyclicBarrier(numWriters + driverCount);
                final AtomicBoolean ingesting = new AtomicBoolean(true);

                final Thread[] writers = new Thread[numWriters];
                for (int w = 0; w < numWriters; w++) {
                    final int batch = 5 + rnd.nextInt(20);
                    writers[w] = newConcurrentWriterThread(
                            w, numWriters, preCount, rowCount, batch, tsv, symIdx, iv, xv, baseToken, barrier, errors);
                }

                // Only the driver touches the clock during the concurrent phase; the
                // final quiescence drive runs after it has joined.
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
                }, "lv-cw-refresh-driver") : null;

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

    // Differential fuzz for a DEDUP UPSERT KEYS(ts, sym) base. Mirrors runFuzz's
    // ingestion shape (pre-CREATE backfill history, then per-commit O3 refresh with
    // optional quiescent restarts and removal events), but with a data model built
    // for dedup: timestamps are drawn from a pool small enough to force same-ts /
    // same-(ts, sym) collisions, and a fifth of the emissions re-point onto an
    // earlier (ts, sym) so a real below-frontier replacement is guaranteed. The
    // oracle recomputes the window over the applied (post-dedup) base and orders
    // both sides by (ts, sym::string) - a total order because (ts, sym) is the dedup
    // key; genericStringMatch tolerates SYMBOL-vs-STRING on the sym passthrough.
    private void runDedupFuzz(
            Rnd rnd,
            int variant,
            int rowCount,
            boolean o3,
            boolean restart,
            boolean backfill,
            boolean removals
    ) throws Exception {
        // Pin the clock a day below the data (see runFuzz): a non-backfill view's
        // lower bound is the CREATE moment and forward-append drops rows below it.
        if (currentMicros < 0) {
            setCurrentMicros(MicrosTimestampDriver.floor("2025-12-31T00:00:00.000000Z"));
        }

        final int n = 1 + rnd.nextInt(MAX_FRAME);
        final int symCount = 1 + rnd.nextInt(SYMBOLS.length);
        final int stepMode = rnd.nextInt(3);
        final int baseStepMax = stepMode == 0 ? 5_000_000 : stepMode == 1 ? 60_000_000 : 900_000_000;
        final int dayJumpEvery = stepMode == 0 ? 20 : 12;
        final boolean withWhere = rnd.nextInt(3) == 0;

        final String projection = projection(variant, n);
        final String viewSql = "SELECT " + projection + " FROM base" + (withWhere ? " WHERE i > 0" : "");
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + (backfill ? "BACKFILL " : "")
                + "AS " + viewSql;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) "
                + "TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");

        LOG.info().$("LV dedup fuzz: variant=").$(variant).$(", rows=").$(rowCount)
                .$(", n=").$(n).$(", symCount=").$(symCount).$(", stepMode=").$(stepMode)
                .$(", o3=").$(o3).$(", restart=").$(restart).$(", backfill=").$(backfill)
                .$(", removals=").$(removals).$(", where=").$(withWhere).$(", sql=").$(viewSql).$();

        // Distinct, strictly-increasing timestamp pool - fewer distinct values than
        // rows, so many emissions collide on one ts (additive across keys) and the
        // forced re-emissions collide on one (ts, sym) (real dedup). Partition spread
        // mirrors runFuzz (per-run step size plus occasional day jumps).
        final int poolSize = Math.max(2, rowCount / 4);
        final long[] pool = new long[poolSize];
        final int maxDayJumps = 30;
        int dayJumps = 0;
        long ts = MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z");
        for (int k = 0; k < poolSize; k++) {
            ts += 1 + rnd.nextInt(baseStepMax);
            if (dayJumps < maxDayJumps && rnd.nextInt(dayJumpEvery) == 0) {
                ts += 86_400_000_000L;
                dayJumps++;
            }
            pool[k] = ts;
        }

        // Each emission draws a ts from the pool and a non-null sym (a dedup key);
        // (ts, sym) may repeat across emissions. i/x carry occasional NULLs. The
        // recompute oracle reads the final deduped base, so the keep-last winner
        // never has to be predicted here.
        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        final boolean[] xNull = new boolean[rowCount];
        for (int k = 0; k < rowCount; k++) {
            tsv[k] = pool[rnd.nextInt(poolSize)];
            symIdx[k] = rnd.nextInt(symCount); // never -1: sym is a dedup key, kept non-null
            iv[k] = rnd.nextInt(20) == 0 ? Numbers.LONG_NULL : (rnd.nextInt(2001) - 1000);
            xNull[k] = rnd.nextInt(20) == 0;
            xv[k] = rnd.nextDouble() * 1000.0;
        }
        // Backfill floor guard: the backfill lower bound is the base min ts at CREATE.
        // ts is not monotonic in the emission index here, so pin emission 0 to the
        // global-min ts (pool[0]) and force it pre-CREATE (preCount >= 1) - then no
        // post-CREATE row falls below the floor and gets dropped, diverging the oracle.
        tsv[0] = pool[0];
        // Force a replacement fraction: re-point some emissions onto an earlier
        // (ts, sym) so a real below-frontier dedup is guaranteed, exercising the
        // applied-reader replay path, not only the additive fast path.
        for (int r = 0, forced = rowCount / 5; r < forced; r++) {
            final int dst = 1 + rnd.nextInt(rowCount - 1);
            final int src = rnd.nextInt(dst);
            tsv[dst] = tsv[src];
            symIdx[dst] = symIdx[src];
        }

        // Backfill captures pre-CREATE history: preCount >= 1 keeps the global-min ts
        // (emission 0) before CREATE. Non-backfill: everything lands post-CREATE.
        final int preCount = backfill ? 1 + rnd.nextInt(rowCount) : 0;

        final StringSink sink = new StringSink();
        LiveViewRefreshJob job = null;
        try {
            if (preCount > 0) {
                final int[] preOrder = segmentOrder(rnd, 0, preCount, o3);
                final int[] cb = commitBounds(rnd, preOrder.length);
                for (int c = 0; c + 1 < cb.length; c++) {
                    insertCommit(sink, preOrder, cb[c], cb[c + 1], tsv, symIdx, iv, xv, xNull, null);
                    drainWalQueue();
                }
            }

            execute(createSql);
            job = new LiveViewRefreshJob(0, engine, 1);

            if (backfill) {
                driveBackfillToCompletion(job, "lv");
            }

            if (preCount < rowCount) {
                final int[] postOrder = segmentOrder(rnd, preCount, rowCount, o3);
                final int[] cb = commitBounds(rnd, postOrder.length);
                for (int c = 0; c + 1 < cb.length; c++) {
                    insertCommit(sink, postOrder, cb[c], cb[c + 1], tsv, symIdx, iv, xv, xNull, null);
                    drainWalQueue();
                    refreshCycle(job);

                    // Remove rows the LV has not yet emitted (see dropDoomedFuturePartition).
                    if (removals && rnd.nextInt(4) == 0) {
                        dropDoomedFuturePartition(sink, job, rnd);
                    }

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
        } finally {
            Misc.free(job);
        }

        // The oracle: the live view equals the window recomputed over the applied
        // (deduped) base. (ts, sym::string) is a total order because (ts, sym) is the
        // dedup key; genericStringMatch tolerates SYMBOL-vs-STRING on the passthrough.
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + viewSql + ") ORDER BY ts, sym::string",
                "(lv) ORDER BY ts, sym::string",
                LOG,
                true
        );

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
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

    // Multiple live views over one base (see testFuzzMultipleLiveViews). K views
    // with distinct window shapes (a random mix of IN MEMORY) are maintained by a
    // single refresh worker and each is cross-checked against its own recompute.
    // The shared WAL retention floor is exercised concretely: after the front of
    // the dataset quiesces, a final batch of base rows is committed and applied to
    // the base table but the views are NOT refreshed, so the base's own applied
    // seqTxn is at the head while every view's lvConsumedSeqTxn lags below it. A
    // real WalPurgeJob is then drained - the ONLY thing pinning the final batch's
    // WAL segments is the minimum lvConsumedSeqTxn across the K dependents
    // (WalPurgeJob.getSafeToPurgeUpToTxn). The views are then refreshed and must
    // converge; a purge that ignored the shared LV floor would have deleted the
    // segments they still need, leaving them short of the final batch.
    private void runMultiViewFuzz(Rnd rnd, int rowCount, boolean o3) throws Exception {
        if (currentMicros < 0) {
            setCurrentMicros(MicrosTimestampDriver.floor("2025-12-31T00:00:00.000000Z"));
        }

        final int viewCount = 2 + rnd.nextInt(3); // 2..4 views
        final int symCount = 1 + rnd.nextInt(SYMBOLS.length);
        final int stepMode = rnd.nextInt(3);
        final int baseStepMax = stepMode == 0 ? 5_000_000 : stepMode == 1 ? 60_000_000 : 900_000_000;
        final int dayJumpEvery = stepMode == 0 ? 20 : 12;

        // Distinct window shapes, one per view: shuffle the fixed-width set and
        // take the first viewCount. Each view gets its own frame radius.
        final int[] variants = Arrays.copyOf(FIXED_WIDTH_VARIANTS, FIXED_WIDTH_VARIANTS.length);
        for (int k = variants.length - 1; k > 0; k--) {
            final int j = rnd.nextInt(k + 1);
            final int tmp = variants[k];
            variants[k] = variants[j];
            variants[j] = tmp;
        }
        final String[] viewNames = new String[viewCount];
        final String[] viewSql = new String[viewCount];
        for (int v = 0; v < viewCount; v++) {
            viewNames[v] = "lv" + v;
            viewSql[v] = "SELECT " + projection(variants[v], 1 + rnd.nextInt(MAX_FRAME)) + " FROM base";
        }

        for (int v = 0; v < viewCount; v++) {
            execute("DROP LIVE VIEW IF EXISTS " + viewNames[v]);
        }
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        LOG.info().$("LV multi-view fuzz: views=").$(viewCount).$(", rows=").$(rowCount)
                .$(", symCount=").$(symCount).$(", stepMode=").$(stepMode).$(", o3=").$(o3).$();

        // Strictly-unique, strictly-increasing timestamps; no pre-CREATE history,
        // so every view sees every row and each shares one oracle shape (a
        // recompute over the full base).
        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        final boolean[] xNull = new boolean[rowCount];
        final int maxDayJumps = 30;
        int dayJumps = 0;
        long ts = MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z");
        for (int k = 0; k < rowCount; k++) {
            ts += 1 + rnd.nextInt(baseStepMax);
            if (dayJumps < maxDayJumps && rnd.nextInt(dayJumpEvery) == 0) {
                ts += 86_400_000_000L;
                dayJumps++;
            }
            tsv[k] = ts;
            symIdx[k] = rnd.nextInt(20) == 0 ? -1 : rnd.nextInt(symCount);
            iv[k] = rnd.nextInt(20) == 0 ? Numbers.LONG_NULL : (rnd.nextInt(2001) - 1000);
            xNull[k] = rnd.nextInt(20) == 0;
            xv[k] = rnd.nextDouble() * 1000.0;
        }

        // Front [0, splitPoint) is ingested and quiesced; the final batch
        // [splitPoint, rowCount) - the highest timestamps, so a pure forward
        // append - is applied to the base but left UN-consumed by the views for
        // the purge test. Both segments are non-empty.
        final int splitPoint = rowCount - (1 + rnd.nextInt(Math.max(1, rowCount / 4)));

        final StringSink sink = new StringSink();
        LiveViewRefreshJob job = null;
        try {
            for (int v = 0; v < viewCount; v++) {
                final String inMem = rnd.nextBoolean() ? "IN MEMORY 60s " : "";
                execute("CREATE LIVE VIEW " + viewNames[v] + " FLUSH EVERY 100ms " + inMem + "AS " + viewSql[v]);
            }
            job = new LiveViewRefreshJob(0, engine, 1);

            final int[] frontOrder = segmentOrder(rnd, 0, splitPoint, o3);
            final int[] cb = commitBounds(rnd, frontOrder.length);
            for (int c = 0; c + 1 < cb.length; c++) {
                insertCommit(sink, frontOrder, cb[c], cb[c + 1], tsv, symIdx, iv, xv, xNull, null);
                drainWalQueue();
                refreshCycle(job); // one job refreshes ALL views
            }
            driveRefreshToQuiescence(job);

            // The final batch: apply it to the base but do NOT refresh, so every
            // view's consumed floor stays below the base head.
            final int[] finalOrder = segmentOrder(rnd, splitPoint, rowCount, false);
            final int[] cbFinal = commitBounds(rnd, finalOrder.length);
            for (int c = 0; c + 1 < cbFinal.length; c++) {
                insertCommit(sink, finalOrder, cbFinal[c], cbFinal[c + 1], tsv, symIdx, iv, xv, xNull, null);
                drainWalQueue();
            }

            // A real purge with several dependents. The base table has already
            // applied the final batch, so only the minimum lvConsumedSeqTxn across
            // the K views can retain its WAL segments - the shared-floor path.
            try (WalPurgeJob purgeJob = new WalPurgeJob(engine)) {
                purgeJob.drain(0);
            }

            // Now refresh: every view must consume the retained final batch and
            // converge. A purge that dropped the segments would leave them short.
            driveRefreshToQuiescence(job);
        } finally {
            Misc.free(job);
        }

        // Every view equals its own from-scratch recompute over the base.
        for (int v = 0; v < viewCount; v++) {
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "(" + viewSql[v] + ") ORDER BY 1",
                    "(" + viewNames[v] + ") ORDER BY 1",
                    LOG,
                    true
            );
        }

        for (int v = 0; v < viewCount; v++) {
            execute("DROP LIVE VIEW " + viewNames[v]);
        }
        execute("DROP TABLE base");
    }

    // Differential fuzz over a TIMESTAMP_NS base partitioned BY HOUR (see
    // testFuzzNanosBase). Mirrors runFuzz's ingestion shape but with nanosecond
    // timestamps and an hour partition unit, exercising the driver-aware refresh
    // path. The recompute oracle is unchanged: the live view must equal the
    // window query recomputed over the base.
    private void runNanosBaseFuzz(
            Rnd rnd,
            int variant,
            int rowCount,
            boolean o3,
            boolean restart,
            boolean backfill,
            boolean inMemory
    ) throws Exception {
        // Pin the wall clock (micros) a day below the ns data start. The view's
        // lower bound is the CREATE moment converted to base (ns) units, so a
        // wall clock below the data keeps it under every ns row timestamp.
        if (currentMicros < 0) {
            setCurrentMicros(MicrosTimestampDriver.floor("2025-12-31T00:00:00.000000Z"));
        }

        final int n = 1 + rnd.nextInt(MAX_FRAME);
        final int symCount = 1 + rnd.nextInt(SYMBOLS.length);
        final int stepMode = rnd.nextInt(3);
        // Nanosecond step ranges (micros ranges scaled by 1000) plus occasional
        // hour jumps, so the data spans several HOUR partitions.
        final long baseStepMaxNs = stepMode == 0 ? 5_000_000_000L : stepMode == 1 ? 60_000_000_000L : 900_000_000_000L;
        final int hourJumpEvery = stepMode == 0 ? 20 : 12;
        final long hourNs = 3_600_000_000_000L;
        final boolean withWhere = rnd.nextInt(3) == 0;

        final String viewSql = "SELECT " + projection(variant, n) + " FROM base" + (withWhere ? " WHERE i > 0" : "");
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + (inMemory ? "IN MEMORY 60s " : "")
                + (backfill ? "BACKFILL " : "")
                + "AS " + viewSql;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP_NS, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR WAL");

        LOG.info().$("LV nanos-base fuzz: variant=").$(variant).$(", rows=").$(rowCount)
                .$(", n=").$(n).$(", symCount=").$(symCount).$(", stepMode=").$(stepMode)
                .$(", o3=").$(o3).$(", restart=").$(restart).$(", backfill=").$(backfill)
                .$(", inMem=").$(inMemory).$(", where=").$(withWhere).$(", sql=").$(viewSql).$();

        // Strictly-unique, strictly-increasing nanosecond timestamps.
        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        final boolean[] xNull = new boolean[rowCount];
        final int maxHourJumps = 30;
        int hourJumps = 0;
        long ts = NanosTimestampDriver.floor("2026-01-01T00:00:00.000000Z");
        for (int k = 0; k < rowCount; k++) {
            ts += 1 + rnd.nextLong(baseStepMaxNs);
            if (hourJumps < maxHourJumps && rnd.nextInt(hourJumpEvery) == 0) {
                ts += hourNs;
                hourJumps++;
            }
            tsv[k] = ts;
            symIdx[k] = rnd.nextInt(20) == 0 ? -1 : rnd.nextInt(symCount);
            iv[k] = rnd.nextInt(20) == 0 ? Numbers.LONG_NULL : (rnd.nextInt(2001) - 1000);
            xNull[k] = rnd.nextInt(20) == 0;
            xv[k] = rnd.nextDouble() * 1000.0;
        }

        final int preCount = backfill ? rnd.nextInt(rowCount + 1) : 0;

        final StringSink sink = new StringSink();
        LiveViewRefreshJob job = null;
        try {
            if (preCount > 0) {
                final int[] preOrder = segmentOrder(rnd, 0, preCount, o3);
                final int[] cb = commitBounds(rnd, preOrder.length);
                for (int c = 0; c + 1 < cb.length; c++) {
                    insertNsCommit(sink, preOrder, cb[c], cb[c + 1], tsv, symIdx, iv, xv, xNull);
                    drainWalQueue();
                }
            }

            execute(createSql);
            job = new LiveViewRefreshJob(0, engine, 1);

            if (backfill) {
                driveBackfillToCompletion(job, "lv");
            }

            if (preCount < rowCount) {
                final int[] postOrder = segmentOrder(rnd, preCount, rowCount, o3);
                final int[] cb = commitBounds(rnd, postOrder.length);
                for (int c = 0; c + 1 < cb.length; c++) {
                    insertNsCommit(sink, postOrder, cb[c], cb[c + 1], tsv, symIdx, iv, xv, xNull);
                    drainWalQueue();
                    refreshCycle(job);

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
        } finally {
            Misc.free(job);
        }

        // The oracle: the live view must equal the window query recomputed over
        // the ns base.
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

    // Differential fuzz over a base whose settled partitions are converted to
    // PARQUET while a live view maintains itself off it (see testFuzzParquetBase).
    // The live view is incremental (NOT backfill): its refresh consumes the base
    // WAL stream, not base partitions, so converting an already-consumed partition
    // to parquet is physically transparent. Under in-order ingestion the run also
    // converts settled partitions MID-STREAM (strictly below the current commit's
    // day, so no later row ever writes into a parquet partition), then continues
    // refreshing over the partially-parquet base; every run also converts once
    // more at the end. The recompute oracle is unchanged - the from-scratch
    // recompute reads the same base, parquet partitions and all.
    //
    // NOTE: this arm deliberately does NOT use BACKFILL. BACKFILL reads base
    // partitions through a page-frame cursor, and a BACKFILL live view over a
    // base with parquet partitions currently double-counts rows (reproduced
    // deterministically while writing this arm - the backfill over parquet
    // partitions over-reads). That is a separate defect in the parquet
    // partition read path, out of scope for this test-only change; this arm
    // stays on the incremental (WAL-consuming) path, which is transparent.
    private void runParquetBaseFuzz(Rnd rnd, int variant, int rowCount, boolean o3, boolean inMemory) throws Exception {
        if (currentMicros < 0) {
            setCurrentMicros(MicrosTimestampDriver.floor("2025-12-31T00:00:00.000000Z"));
        }

        final int n = 1 + rnd.nextInt(MAX_FRAME);
        final int symCount = 1 + rnd.nextInt(SYMBOLS.length);
        final boolean withWhere = rnd.nextInt(3) == 0;

        final String viewSql = "SELECT " + projection(variant, n) + " FROM base" + (withWhere ? " WHERE i > 0" : "");
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + (inMemory ? "IN MEMORY 60s " : "")
                + "AS " + viewSql;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        LOG.info().$("LV parquet-base fuzz: variant=").$(variant).$(", rows=").$(rowCount)
                .$(", n=").$(n).$(", symCount=").$(symCount).$(", o3=").$(o3)
                .$(", inMem=").$(inMemory).$(", where=").$(withWhere).$(", sql=").$(viewSql).$();

        // Strictly-unique, strictly-increasing timestamps with a partition
        // boundary crossed every few rows, so the run has several settled
        // partitions to convert to parquet.
        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        final boolean[] xNull = new boolean[rowCount];
        long ts = MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z");
        for (int k = 0; k < rowCount; k++) {
            ts += 1 + rnd.nextInt(1_000_000);
            if (rnd.nextInt(8) == 0) {
                ts += 86_400_000_000L; // cross a day partition boundary
            }
            tsv[k] = ts;
            symIdx[k] = rnd.nextInt(20) == 0 ? -1 : rnd.nextInt(symCount);
            iv[k] = rnd.nextInt(20) == 0 ? Numbers.LONG_NULL : (rnd.nextInt(2001) - 1000);
            xNull[k] = rnd.nextInt(20) == 0;
            xv[k] = rnd.nextDouble() * 1000.0;
        }

        final StringSink sink = new StringSink();
        LiveViewRefreshJob job = null;
        try {
            execute(createSql);
            job = new LiveViewRefreshJob(0, engine, 1);

            final int[] order = segmentOrder(rnd, 0, rowCount, o3);
            final int[] cb = commitBounds(rnd, order.length);
            long convertedUpToDay = 0;
            for (int c = 0; c + 1 < cb.length; c++) {
                insertCommit(sink, order, cb[c], cb[c + 1], tsv, symIdx, iv, xv, xNull, null);
                drainWalQueue();
                refreshCycle(job);

                // Mid-stream conversion, in-order runs only: under in-order
                // ingestion order[] is the identity, so every row already
                // inserted has ts <= tsv[cb[c+1]-1] and every future row is
                // strictly higher. Converting partitions strictly below that
                // row's day is therefore safe (no later insert lands in a
                // parquet partition), and the view keeps refreshing afterwards
                // over a partially-parquet base.
                if (!o3 && rnd.nextInt(3) == 0) {
                    final long settledDay = tsv[cb[c + 1] - 1] - tsv[cb[c + 1] - 1] % 86_400_000_000L;
                    convertPartitionsToParquet(convertedUpToDay, settledDay);
                    convertedUpToDay = settledDay;
                }
            }

            // A final conversion over every settled partition below the max day,
            // so the fully-consumed base is (mostly) parquet before the last read.
            final long maxDay = tsv[rowCount - 1] - tsv[rowCount - 1] % 86_400_000_000L;
            convertPartitionsToParquet(convertedUpToDay, maxDay);
            driveRefreshToQuiescence(job);
        } finally {
            Misc.free(job);
        }

        // The oracle: the live view must equal the window query recomputed over
        // the base (whose partitions are now a parquet/native mix).
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

    // Reader-vs-refresh fuzz (see testFuzzReaderVsRefresh). One refresh driver
    // thread owns the clock and both apply jobs; two reader threads loop the
    // native and the ORDER BY 1 wrapper query, asserting the prefix-consistency
    // invariant on every snapshot; the main thread single-writer-ingests the
    // dataset in small commits (shuffled across commits when o3 is set, racing
    // the readers against O3 REPLACE_RANGE rewrites). After ingestion the race
    // continues until the view has consumed the full base - so the tail flushes
    // are read-validated too - then the run quiesces single-threaded and ends
    // on the exact recompute oracle.
    private void runReaderVsRefreshFuzz(Rnd rnd, int rowCount, boolean o3, boolean inMemory) throws Exception {
        // The refresh driver advances the flush clock in an unbounded loop, so -
        // like the concurrent-writer arm - the clock is pinned a full YEAR below
        // the data start, keeping it under the non-backfill view's CREATE-moment
        // lower bound for the whole run.
        setCurrentMicros(MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z"));
        final long dataStart = MicrosTimestampDriver.floor("2027-01-01T00:00:00.000000Z");

        final String viewSql = "SELECT ts, vs, vv, i, row_number() OVER () AS rn FROM base";
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + (inMemory ? "IN MEMORY 60s " : "")
                + "AS " + viewSql;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, vs STRING, vv VARCHAR, i LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");

        LOG.info().$("LV reader-vs-refresh fuzz: rows=").$(rowCount)
                .$(", o3=").$(o3).$(", inMem=").$(inMemory).$(", sql=").$(viewSql).$();

        // Strictly-unique, strictly-increasing timestamps; the var-length values
        // derive from the ts, so a reader can validate any row in isolation.
        final long[] tsv = new long[rowCount];
        long ts = dataStart;
        for (int k = 0; k < rowCount; k++) {
            ts += 1 + rnd.nextInt(60_000_000);
            if (rnd.nextInt(16) == 0) {
                ts += 86_400_000_000L;
            }
            tsv[k] = ts;
        }

        execute(createSql);

        final TableToken baseToken = engine.verifyTableName("base");
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final AtomicBoolean running = new AtomicBoolean(true);
        LiveViewRefreshJob job = null;
        try {
            job = new LiveViewRefreshJob(0, engine, 1);
            final LiveViewRefreshJob driverJob = job;

            // The single refresh driver owns the clock and both apply jobs for
            // the whole concurrent phase; the main thread only commits inserts.
            final Thread driver = new Thread(() -> {
                try {
                    while (running.get()) {
                        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                        drainWalQueue();
                        drainJob(driverJob);
                    }
                } catch (Throwable th) {
                    errors.add(th);
                } finally {
                    Path.clearThreadLocals();
                }
            }, "lv-rvr-refresh-driver");
            final AtomicLong nativeRowsValidated = new AtomicLong();
            final AtomicLong wrapperRowsValidated = new AtomicLong();
            final Thread nativeReader = newPrefixInvariantReader("SELECT * FROM lv", running, errors, nativeRowsValidated);
            final Thread wrapperReader = newPrefixInvariantReader("(lv) ORDER BY 1", running, errors, wrapperRowsValidated);
            driver.start();
            nativeReader.start();
            wrapperReader.start();

            // Single-writer ingestion in small commits so the readers see many
            // intermediate versions; the o3 shuffle makes later commits dip
            // below the frontier, racing the readers against replay rewrites.
            final int[] order = segmentOrder(rnd, 0, rowCount, o3);
            final StringSink sink = new StringSink();
            for (int from = 0; from < rowCount; ) {
                final int to = Math.min(rowCount, from + 3 + rnd.nextInt(12));
                sink.clear();
                sink.put("INSERT INTO base (ts, vs, vv, i) VALUES ");
                for (int r = from; r < to; r++) {
                    final int k = order[r];
                    if (r > from) {
                        sink.put(',');
                    }
                    sink.put('(').put(tsv[k]).put("::timestamp,'").put(tsv[k])
                            .put("','v").put(tsv[k]).put("',").put(rnd.nextInt(1000)).put(')');
                }
                execute(sink);
                from = to;
            }

            // Keep the race going until the view has consumed the full base;
            // hard-bounded so a stalled refresh fails instead of hanging.
            final long lastBaseTxn = engine.getTableSequencerAPI().lastTxn(baseToken);
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            for (int i = 0; i < 3000 && instance.getLastProcessedSeqTxn() < lastBaseTxn && errors.isEmpty(); i++) {
                Os.sleep(10);
            }

            running.set(false);
            driver.join();
            nativeReader.join();
            wrapperReader.join();
            if (!errors.isEmpty()) {
                throw new RuntimeException("worker thread failed", errors.peek());
            }
            Assert.assertTrue(
                    "view did not consume the base in time [lastProcessed=" + instance.getLastProcessedSeqTxn()
                            + ", lastBaseTxn=" + lastBaseTxn + ']',
                    instance.getLastProcessedSeqTxn() >= lastBaseTxn
            );
            // Vacuity guard: both readers must have validated real rows, not
            // spun over empty snapshots the whole run.
            Assert.assertTrue("native reader validated no rows", nativeRowsValidated.get() > 0);
            Assert.assertTrue("wrapper reader validated no rows", wrapperRowsValidated.get() > 0);

            driveRefreshToQuiescence(job);
        } finally {
            Misc.free(job);
        }

        // The oracle: the live view must equal the window query recomputed over
        // the base table. ORDER BY 1 (the unique ts) gives both sides a total order.
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

    // Freeze-and-continue fuzz for TRUNCATE / DROP PARTITION of already-emitted
    // base rows (see testFuzzRemovalFreezeAndContinue). Phase 1 mirrors runFuzz
    // (optional pre-CREATE backfill history, per-commit O3 refresh, optional
    // quiescent restarts) over the first split of the dataset, quiesces, and
    // cross-checks the standard recompute oracle while the base is still
    // intact. The removal event then TRUNCATEs the base or drops one or two
    // whole days of emitted rows; the walk-past refresh must keep the view
    // ACTIVE and its output byte-identical. Phase 2 ingests the remaining rows
    // STRICTLY IN ORDER - an O3 replay after the removal re-reads the physical
    // base and picks head-hit (logical, carried .cp state) or head-miss (frozen
    // prefix + physical recompute) depending on checkpoint cadence, which is
    // deliberately path-dependent behaviour an exact-equality oracle cannot
    // pin; the head-miss half has deterministic coverage in LiveViewSmokeTest's
    // frozen-prefix replay tests. The forward-only continuation is the logical
    // stream, so the run-end oracle recomputes over a shadow table holding
    // every generated row. Restarts stay sound because the restore path
    // (replayToApplied) re-feeds the base's RAW WAL, where removed rows'
    // commits still exist; the base stays non-dedup - the dedup arm restores
    // from the applied (physical) base instead.
    private void runRemovalFreezeContinueFuzz(
            Rnd rnd,
            int variant,
            int rowCount,
            boolean truncate,
            boolean restart,
            boolean backfill,
            boolean inMemory
    ) throws Exception {
        // Pin the clock a day below the data, like runFuzz: a non-backfill view's
        // lower bound is the CREATE moment, and O3 head-miss replay only re-emits
        // rows at or above it, so the clock must sit below every data timestamp.
        if (currentMicros < 0) {
            setCurrentMicros(MicrosTimestampDriver.floor("2025-12-31T00:00:00.000000Z"));
        }

        final int n = 1 + rnd.nextInt(MAX_FRAME);
        final int symCount = 1 + rnd.nextInt(SYMBOLS.length);
        final int stepMode = rnd.nextInt(3);
        final int baseStepMax = stepMode == 0 ? 5_000_000 : stepMode == 1 ? 60_000_000 : 900_000_000;
        final int dayJumpEvery = stepMode == 0 ? 20 : 12;
        final boolean withWhere = rnd.nextInt(3) == 0;

        final String projection = projection(variant, n);
        final String where = withWhere ? " WHERE i > 0" : "";
        final String viewSql = "SELECT " + projection + " FROM base" + where;
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + (inMemory ? "IN MEMORY 60s " : "")
                + (backfill ? "BACKFILL " : "")
                + "AS " + viewSql;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("DROP TABLE IF EXISTS shadow");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        LOG.info().$("LV removal fuzz: variant=").$(variant).$(", rows=").$(rowCount)
                .$(", n=").$(n).$(", symCount=").$(symCount).$(", stepMode=").$(stepMode)
                .$(", truncate=").$(truncate).$(", restart=").$(restart)
                .$(", backfill=").$(backfill).$(", inMem=").$(inMemory)
                .$(", where=").$(withWhere).$(", sql=").$(viewSql).$();

        // Strictly-unique, strictly-increasing timestamps so ts is a total order;
        // random symbols and values with occasional NULLs.
        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        final boolean[] xNull = new boolean[rowCount];
        final int maxDayJumps = 30;
        int dayJumps = 0;
        long ts = MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z");
        for (int k = 0; k < rowCount; k++) {
            ts += 1 + rnd.nextInt(baseStepMax);
            if (dayJumps < maxDayJumps && rnd.nextInt(dayJumpEvery) == 0) {
                ts += 86_400_000_000L;
                dayJumps++;
            }
            tsv[k] = ts;
            symIdx[k] = rnd.nextInt(20) == 0 ? -1 : rnd.nextInt(symCount);
            iv[k] = rnd.nextInt(20) == 0 ? Numbers.LONG_NULL : (rnd.nextInt(2001) - 1000);
            xNull[k] = rnd.nextInt(20) == 0;
            xv[k] = rnd.nextDouble() * 1000.0;
        }

        // Phase 1 rows [0, splitPoint) are ingested, refreshed, and quiesced
        // before the removal; phase 2 rows [splitPoint, rowCount) continue after
        // it. Both phases are non-empty so the removal always targets emitted
        // rows and the continuation is always exercised.
        final int splitPoint = rowCount / 3 + rnd.nextInt(rowCount / 3);
        final int preCount = backfill ? 1 + rnd.nextInt(splitPoint) : 0;

        final StringSink sink = new StringSink();
        final StringSink preRemoval = new StringSink();
        LiveViewRefreshJob job = null;
        try {
            if (preCount > 0) {
                final int[] preOrder = segmentOrder(rnd, 0, preCount, true);
                final int[] cb = commitBounds(rnd, preOrder.length);
                for (int c = 0; c + 1 < cb.length; c++) {
                    insertCommit(sink, preOrder, cb[c], cb[c + 1], tsv, symIdx, iv, xv, xNull, null);
                    drainWalQueue();
                }
            }

            execute(createSql);
            job = new LiveViewRefreshJob(0, engine, 1);

            if (backfill) {
                driveBackfillToCompletion(job, "lv");
            }

            // Phase 1: O3 churn over an intact base, per-commit refresh.
            if (preCount < splitPoint) {
                final int[] phase1Order = segmentOrder(rnd, preCount, splitPoint, true);
                final int[] cb = commitBounds(rnd, phase1Order.length);
                for (int c = 0; c + 1 < cb.length; c++) {
                    insertCommit(sink, phase1Order, cb[c], cb[c + 1], tsv, symIdx, iv, xv, xNull, null);
                    drainWalQueue();
                    refreshCycle(job);

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

            // Intact-base checkpoint of the oracle: at this point logical ==
            // physical, so the standard recompute must already hold.
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "(" + viewSql + ") ORDER BY 1",
                    "(lv) ORDER BY 1",
                    LOG,
                    true
            );
            printSql("(lv) ORDER BY 1", preRemoval);

            // The removal event: every targeted row is emitted (phase 1 has
            // quiesced), so this removes derived history, not pending input.
            if (truncate) {
                execute("TRUNCATE TABLE base");
            } else {
                // One or two whole days of phase-1 rows - the day list spans the
                // emitted range, so the pick lands on its bottom, middle, or top.
                final LongHashSet seenDays = new LongHashSet();
                final ObjList<String> days = new ObjList<>();
                for (int k = 0; k < splitPoint; k++) {
                    final long dayFloor = tsv[k] - tsv[k] % 86_400_000_000L;
                    if (seenDays.add(dayFloor)) {
                        days.add(Micros.toString(dayFloor).substring(0, 10));
                    }
                }
                final int dropIdx = rnd.nextInt(days.size());
                sink.clear();
                sink.put("ALTER TABLE base DROP PARTITION LIST '").put(days.getQuick(dropIdx)).put('\'');
                if (rnd.nextBoolean() && dropIdx + 1 < days.size()) {
                    sink.put(", '").put(days.getQuick(dropIdx + 1)).put('\'');
                }
                execute(sink);
            }
            drainWalQueue();
            refreshCycle(job); // walk past the removal seqTxn

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertFalse("LV must stay valid after base row removal", instance.isInvalid());
            sink.clear();
            printSql("(lv) ORDER BY 1", sink);
            TestUtils.assertEquals("removal must not retract emitted LV rows", preRemoval, sink);

            // Phase 2: strictly in-order continuation on top of the frozen state.
            final int[] phase2Order = segmentOrder(rnd, splitPoint, rowCount, false);
            final int[] cb = commitBounds(rnd, phase2Order.length);
            for (int c = 0; c + 1 < cb.length; c++) {
                insertCommit(sink, phase2Order, cb[c], cb[c + 1], tsv, symIdx, iv, xv, xNull, null);
                drainWalQueue();
                refreshCycle(job);

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

            driveRefreshToQuiescence(job);
        } finally {
            Misc.free(job);
        }

        // The oracle: the view must equal the window query recomputed over the
        // LOGICAL dataset - every generated row, as if nothing was removed -
        // materialized into the shadow table in ts order.
        execute("CREATE TABLE shadow (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        insertLogicalDataset("shadow", tsv, symIdx, iv, xv, xNull);
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(SELECT " + projection + " FROM shadow" + where + ") ORDER BY 1",
                "(lv) ORDER BY 1",
                LOG,
                true
        );

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
        execute("DROP TABLE shadow");
    }

    // Differential fuzz where the base receives REPLACE_RANGE data commits
    // (WalWriter.commitWithParams) interleaved with plain insert commits. A
    // replace commit atomically deletes every base row in [lo, hi) and inserts
    // its own rows - possibly none - so unlike every other arm the logical row
    // set shrinks as well as grows. The refresh worker must converge the view
    // onto the applied (post-replace) base: the drain sees the deletion only
    // through the commit's range metadata and routes any band reaching at or
    // below the frontier to the O3 replay, even when every inserted row sits
    // above the frontier. Ingestion mirrors runFuzz (pre-CREATE backfill
    // history, per-commit O3 refresh, optional quiescent restarts); replace
    // events fire between insert commits and once more after the last insert,
    // so at least one band lands over fully-settled data every run.
    private void runReplaceRangeFuzz(
            Rnd rnd,
            int variant,
            int rowCount,
            boolean restart,
            boolean backfill,
            boolean inMemory
    ) throws Exception {
        // Pin the clock a day below the data, like runFuzz: a non-backfill view's
        // lower bound is the CREATE moment, and O3 head-miss replay only re-emits
        // rows at or above it, so the clock must sit below every data timestamp.
        if (currentMicros < 0) {
            setCurrentMicros(MicrosTimestampDriver.floor("2025-12-31T00:00:00.000000Z"));
        }

        final int n = 1 + rnd.nextInt(MAX_FRAME);
        final int symCount = 1 + rnd.nextInt(SYMBOLS.length);
        final int stepMode = rnd.nextInt(3);
        final int baseStepMax = stepMode == 0 ? 5_000_000 : stepMode == 1 ? 60_000_000 : 900_000_000;
        final int dayJumpEvery = stepMode == 0 ? 20 : 12;
        final boolean withWhere = rnd.nextInt(3) == 0;

        final String viewSql = "SELECT " + projection(variant, n) + " FROM base" + (withWhere ? " WHERE i > 0" : "");
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + (inMemory ? "IN MEMORY 60s " : "")
                + (backfill ? "BACKFILL " : "")
                + "AS " + viewSql;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        LOG.info().$("LV replace-range fuzz: variant=").$(variant).$(", rows=").$(rowCount)
                .$(", n=").$(n).$(", symCount=").$(symCount).$(", stepMode=").$(stepMode)
                .$(", restart=").$(restart).$(", backfill=").$(backfill).$(", inMem=").$(inMemory)
                .$(", where=").$(withWhere).$(", sql=").$(viewSql).$();

        // Strictly-unique, strictly-increasing timestamps so ts is a total order;
        // random symbols and values with occasional NULLs. Every dataset ts goes
        // into usedTs up front so replace rows can never collide with a dataset
        // row that is still to be inserted.
        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        final boolean[] xNull = new boolean[rowCount];
        final LongHashSet usedTs = new LongHashSet();
        final int maxDayJumps = 30;
        int dayJumps = 0;
        long ts = MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z");
        for (int k = 0; k < rowCount; k++) {
            ts += 1 + rnd.nextInt(baseStepMax);
            if (dayJumps < maxDayJumps && rnd.nextInt(dayJumpEvery) == 0) {
                ts += 86_400_000_000L;
                dayJumps++;
            }
            tsv[k] = ts;
            usedTs.add(ts);
            symIdx[k] = rnd.nextInt(20) == 0 ? -1 : rnd.nextInt(symCount);
            iv[k] = rnd.nextInt(20) == 0 ? Numbers.LONG_NULL : (rnd.nextInt(2001) - 1000);
            xNull[k] = rnd.nextInt(20) == 0;
            xv[k] = rnd.nextDouble() * 1000.0;
        }

        // Backfill captures pre-CREATE history. preCount >= 1 keeps the global-min
        // ts (and thus the backfill floor) pre-CREATE, so no replace row - all
        // anchored strictly above tsv[0] - can land below the floor and get
        // dropped, diverging the oracle. Non-backfill: everything lands post-CREATE.
        final int preCount = backfill ? 1 + rnd.nextInt(rowCount) : 0;

        final TableToken baseToken = engine.verifyTableName("base");
        final StringSink sink = new StringSink();
        LiveViewRefreshJob job = null;
        try {
            if (preCount > 0) {
                final int[] preOrder = segmentOrder(rnd, 0, preCount, true);
                final int[] cb = commitBounds(rnd, preOrder.length);
                for (int c = 0; c + 1 < cb.length; c++) {
                    insertCommit(sink, preOrder, cb[c], cb[c + 1], tsv, symIdx, iv, xv, xNull, null);
                    drainWalQueue();
                }
            }

            execute(createSql);
            job = new LiveViewRefreshJob(0, engine, 1);

            if (backfill) {
                driveBackfillToCompletion(job, "lv");
            }

            if (preCount < rowCount) {
                final int[] postOrder = segmentOrder(rnd, preCount, rowCount, true);
                final int[] cb = commitBounds(rnd, postOrder.length);
                for (int c = 0; c + 1 < cb.length; c++) {
                    insertCommit(sink, postOrder, cb[c], cb[c + 1], tsv, symIdx, iv, xv, xNull, null);
                    drainWalQueue();
                    refreshCycle(job);

                    if (rnd.nextInt(3) == 0) {
                        // A replace band over whatever is currently in the base -
                        // emitted rows, rows still pending in the base WAL, or
                        // nothing at all when the band covers not-yet-inserted ts.
                        commitReplaceRange(rnd, baseToken, tsv, usedTs, symCount);
                        drainWalQueue();
                        refreshCycle(job);
                    }

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

            // A final replace event on top of the fully-ingested base, so every
            // run ends with at least one band over settled, fully-emitted data.
            commitReplaceRange(rnd, baseToken, tsv, usedTs, symCount);
            drainWalQueue();

            driveRefreshToQuiescence(job);
        } finally {
            Misc.free(job);
        }

        // The oracle: the live view must equal the window query recomputed over
        // the applied (post-replace) base. ORDER BY 1 (the unique ts) gives both
        // sides a total order; genericStringMatch tolerates SYMBOL-vs-STRING on
        // passthrough.
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

    // Differential fuzz proving rolled-back base transactions never reach the view
    // (see testFuzzRolledBackCommits). Committed rows follow runFuzz's shape - unique,
    // strictly-increasing timestamps, optional pre-CREATE BACKFILL history - but every
    // commit is written through a direct WalWriter, so between real batches a doomed
    // transaction can append phantom rows and then rollback() without ever advancing the
    // sequencer. Phantoms carry i = PHANTOM_SENTINEL and a timestamp just below a
    // committed row, so a leak is loud: it survives WHERE i>0 and lands below the
    // frontier. The recompute over the committed base is the oracle - it can never
    // contain a phantom, so equality at quiescence proves none leaked.
    private void runRolledBackFuzz(Rnd rnd, int variant, int rowCount, boolean o3, boolean backfill) throws Exception {
        setCurrentMicros(MicrosTimestampDriver.floor("2025-12-31T00:00:00.000000Z"));

        final int n = 1 + rnd.nextInt(MAX_FRAME);
        final int symCount = 1 + rnd.nextInt(SYMBOLS.length);
        final int stepMode = rnd.nextInt(3);
        final int baseStepMax = stepMode == 0 ? 5_000_000 : stepMode == 1 ? 60_000_000 : 900_000_000;
        final int dayJumpEvery = stepMode == 0 ? 20 : 12;
        final boolean withWhere = rnd.nextInt(3) == 0;

        final String viewSql = "SELECT " + projection(variant, n) + " FROM base" + (withWhere ? " WHERE i > 0" : "");
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + (backfill ? "BACKFILL " : "")
                + "AS " + viewSql;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        LOG.info().$("LV rolled-back fuzz: variant=").$(variant).$(", rows=").$(rowCount)
                .$(", n=").$(n).$(", symCount=").$(symCount).$(", stepMode=").$(stepMode)
                .$(", o3=").$(o3).$(", backfill=").$(backfill).$(", where=").$(withWhere).$(", sql=").$(viewSql).$();

        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        final int maxDayJumps = 30;
        int dayJumps = 0;
        long ts = MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z");
        for (int k = 0; k < rowCount; k++) {
            ts += 1 + rnd.nextInt(baseStepMax);
            if (dayJumps < maxDayJumps && rnd.nextInt(dayJumpEvery) == 0) {
                ts += 86_400_000_000L;
                dayJumps++;
            }
            tsv[k] = ts;
            symIdx[k] = rnd.nextInt(20) == 0 ? -1 : rnd.nextInt(symCount);
            iv[k] = rnd.nextInt(20) == 0 ? Numbers.LONG_NULL : (rnd.nextInt(2001) - 1000);
            xv[k] = rnd.nextDouble() * 1000.0;
        }

        final int preCount = backfill ? rnd.nextInt(rowCount + 1) : 0;
        final TableToken baseToken = engine.verifyTableName("base");
        LiveViewRefreshJob job = null;
        try {
            // Pre-CREATE backfill history (single committed transaction, in ts order).
            if (preCount > 0) {
                try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                    for (int k = 0; k < preCount; k++) {
                        appendRow(walWriter, tsv[k], symIdx[k], iv[k], xv[k]);
                    }
                    walWriter.commit();
                }
                drainWalQueue();
            }

            execute(createSql);
            job = new LiveViewRefreshJob(0, engine, 1);
            if (backfill) {
                driveBackfillToCompletion(job, "lv");
            }

            if (preCount < rowCount) {
                final int[] postOrder = segmentOrder(rnd, preCount, rowCount, o3);
                final int[] cb = commitBounds(rnd, postOrder.length);
                for (int c = 0; c + 1 < cb.length; c++) {
                    // A doomed transaction before the real commit: append phantom rows,
                    // then roll the whole thing back. It never advances the sequencer, so
                    // no seqTxn is produced and the LV drain never sees these rows.
                    if (rnd.nextInt(3) == 0) {
                        try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                            for (int g = 0, ghosts = 1 + rnd.nextInt(3); g < ghosts; g++) {
                                appendRow(walWriter, phantomTs(rnd, tsv), rnd.nextInt(symCount),
                                        PHANTOM_SENTINEL, rnd.nextDouble() * 1000.0);
                            }
                            walWriter.rollback();
                        }
                    }

                    // The real committed batch.
                    try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                        for (int r = cb[c]; r < cb[c + 1]; r++) {
                            final int idx = postOrder[r];
                            appendRow(walWriter, tsv[idx], symIdx[idx], iv[idx], xv[idx]);
                        }
                        walWriter.commit();
                    }
                    drainWalQueue();
                    refreshCycle(job);
                }
            }
            driveRefreshToQuiescence(job);
        } finally {
            Misc.free(job);
        }

        // The oracle: the live view must equal the window query recomputed over the
        // committed base. ORDER BY 1 (the unique ts) gives both sides a total order;
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

    // A phantom timestamp just below a randomly-chosen committed row - below the running
    // frontier once that region is emitted, so a leaked phantom would trip an O3 replay
    // rather than sit harmlessly above the data.
    private static long phantomTs(Rnd rnd, long[] tsv) {
        return tsv[rnd.nextInt(tsv.length)] - 1 - rnd.nextInt(3);
    }

    // Differential fuzz for the var-length tier storage. The LV projects every
    // var-length type the tier learned to store (STRING / VARCHAR / BINARY /
    // DOUBLE[]) straight through, plus row_number() OVER () so SELECT * FROM lv
    // routes through the in-mem tier (Mode B) - the var-length passthroughs are
    // the subject under test, the window fn only makes the query a valid LV.
    // Ingestion mirrors runFuzz (pre-CREATE backfill history, then per-commit O3
    // refresh with optional quiescent restarts); after quiescence a top-up
    // forward row guarantees the tier is populated, then the read-back is
    // cross-checked against the recompute, Mode B is confirmed engaged, and the
    // Mode B result is compared byte-for-byte against the forced disk-only path.
    private void runVarSizeFuzz(Rnd rnd, int rowCount, boolean o3, boolean restart, boolean backfill) throws Exception {
        // Pin the clock a day below the data, like runFuzz: a non-backfill view's
        // lower bound is the CREATE moment, and O3 head-miss replay only re-emits
        // rows at or above it, so the clock must sit below every data timestamp.
        if (currentMicros < 0) {
            setCurrentMicros(MicrosTimestampDriver.floor("2025-12-31T00:00:00.000000Z"));
        }

        final int stepMode = rnd.nextInt(3);
        final int baseStepMax = stepMode == 0 ? 5_000_000 : stepMode == 1 ? 60_000_000 : 900_000_000;
        final int dayJumpEvery = stepMode == 0 ? 20 : 12;

        final String viewSql = "SELECT ts, vs, vv, vb, va, row_number() OVER () AS rn FROM base";
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 60s "
                + (backfill ? "BACKFILL " : "")
                + "AS " + viewSql;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, vs STRING, vv VARCHAR, vb BINARY, va DOUBLE[]) "
                + "TIMESTAMP(ts) PARTITION BY DAY WAL");

        LOG.info().$("LV var-size fuzz: rows=").$(rowCount).$(", stepMode=").$(stepMode)
                .$(", o3=").$(o3).$(", restart=").$(restart).$(", backfill=").$(backfill)
                .$(", sql=").$(viewSql).$();

        // Strictly-unique, strictly-increasing timestamps so ts is a total order;
        // each row's four var-length values are pre-rendered into tuple[k].
        final long[] tsv = new long[rowCount];
        final String[] tuple = new String[rowCount];
        final int maxDayJumps = 30;
        int dayJumps = 0;
        long ts = MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z");
        for (int k = 0; k < rowCount; k++) {
            ts += 1 + rnd.nextInt(baseStepMax);
            if (dayJumps < maxDayJumps && rnd.nextInt(dayJumpEvery) == 0) {
                ts += 86_400_000_000L;
                dayJumps++;
            }
            tsv[k] = ts;
            tuple[k] = varSizeTuple(rnd);
        }

        // Backfill captures pre-CREATE history: the earliest rows go before CREATE
        // so the backfill floor sits at the global-min ts and no post-CREATE O3 row
        // falls below it. Non-backfill: everything lands post-CREATE.
        final int preCount = backfill ? rnd.nextInt(rowCount + 1) : 0;

        final StringSink sink = new StringSink();
        LiveViewRefreshJob job = null;
        try {
            if (preCount > 0) {
                final int[] preOrder = segmentOrder(rnd, 0, preCount, o3);
                final int[] cb = commitBounds(rnd, preOrder.length);
                for (int c = 0; c + 1 < cb.length; c++) {
                    insertVarSizeCommit(sink, preOrder, cb[c], cb[c + 1], tsv, tuple);
                    drainWalQueue();
                }
            }

            execute(createSql);
            job = new LiveViewRefreshJob(0, engine, 1);

            if (backfill) {
                driveBackfillToCompletion(job, "lv");
            }

            if (preCount < rowCount) {
                final int[] postOrder = segmentOrder(rnd, preCount, rowCount, o3);
                final int[] cb = commitBounds(rnd, postOrder.length);
                for (int c = 0; c + 1 < cb.length; c++) {
                    insertVarSizeCommit(sink, postOrder, cb[c], cb[c + 1], tsv, tuple);
                    drainWalQueue();
                    refreshCycle(job);

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

            // Top up with one clean forward row above the global max ts so the
            // in-mem tier is guaranteed populated at the final read - a run that
            // ended on a restart would otherwise leave the freshly-rebuilt tier
            // empty, routing the read-back disk-only and leaving Mode B unexercised.
            execute("INSERT INTO base (ts, vs, vv, vb, va) VALUES ("
                    + (tsv[rowCount - 1] + 1) + "::timestamp, 'zz', 'yy', 'xx'::binary, ARRAY[1.0, 2.0])");
            drainWalQueue();
            refreshCycle(job);
            driveRefreshToQuiescence(job);
        } finally {
            Misc.free(job);
        }

        // The oracle: the live view must equal the window query recomputed over the
        // base table. ORDER BY 1 (the unique ts) gives both sides a total order and
        // compares the var-length passthroughs cell by cell.
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + viewSql + ") ORDER BY 1",
                "(lv) ORDER BY 1",
                LOG,
                true
        );

        // Var-length read-back cross-checks, single-threaded now the worker is
        // freed and the view is quiesced: SELECT * FROM lv actually routes through
        // Mode B (the tier serves the var-length values from RAM), and the Mode B
        // result is byte-identical to the forced disk-only path - so a tier that
        // stored a var-length value wrong would diverge from the disk oracle here.
        assertModeBEngaged();
        assertModeBMatchesDiskOnly("SELECT * FROM lv");

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
    }
}

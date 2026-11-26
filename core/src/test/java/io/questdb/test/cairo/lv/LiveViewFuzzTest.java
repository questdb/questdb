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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ErrorTag;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.lv.LiveViewCheckpointAnchorPlan;
import io.questdb.cairo.lv.LiveViewCheckpointCaptureLedger;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointRecoveryPhase;
import io.questdb.cairo.lv.LiveViewCheckpointRepairMarker;
import io.questdb.cairo.lv.LiveViewCheckpointRepairPlan;
import io.questdb.cairo.lv.LiveViewCheckpointRepairRoute;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewCompiledPlan;
import io.questdb.cairo.lv.LiveViewInMemoryBuffer;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewRefreshStats;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.lv.LiveViewRecordCursor;
import io.questdb.griffin.engine.lv.LiveViewRecordCursorFactory;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.LongHashSet;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.datetime.nanotime.NanosFormatUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import io.questdb.test.cairo.fuzz.AbstractFuzzTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Differential fuzz test for live views.
 * <p>
 * The premise of an incremental-maintenance engine is that the incrementally
 * materialized state equals a from-scratch recompute over the base table. This
 * test verifies exactly that invariant: it drives randomized inserts (in-order
 * and out-of-order), simulated restarts, and optional seed at the base
 * table, then cross-checks the live view's contents against the same window
 * query recomputed directly over the base table.
 * <p>
 * <b>Why the oracle is sound.</b> Window functions are order-dependent, so a
 * row-level comparison is only meaningful when both the incremental and the
 * batch path agree on a total ordering of the input rows. Two design choices
 * guarantee that:
 * <ul>
 *   <li><b>Unique (timestamp, key) pairs.</b> No two generated rows share a
 *   timestamp and a {@code sym} key, so within every {@code PARTITION BY sym}
 *   partition the designated timestamp is a total order and
 *   {@code OVER (... ORDER BY ts ...)} is unambiguous. Rows of different keys
 *   may tie on the timestamp (see {@link LiveViewFuzzRows}), which is what the
 *   repair paths have to keep apart. A shape that is not partitioned by the key -
 *   {@code PARTITION BY 0}, ranking in scan order - would let the two paths break
 *   such ties differently, so its runs keep every timestamp unique. Out-of-order
 *   ingestion is produced by <i>shuffling insertion order across commits</i>,
 *   never by colliding (timestamp, key) pairs.</li>
 *   <li><b>Grammar-legal, deterministic window shapes.</b> Only bounded
 *   {@code ROWS BETWEEN N PRECEDING AND CURRENT ROW} frames and ranking
 *   {@code OVER ()} are used. Live views reject unbounded aggregate frames
 *   without an ANCHOR clause, and bounded frames over a unique-ts ordering are
 *   deterministic functions of the row set.</li>
 * </ul>
 * The comparison normalizes row order with {@code ORDER BY 1, 2} (the unique
 * timestamp and key pair) and uses {@code genericStringMatch} so a SYMBOL
 * passthrough that the materializer stores as STRING still compares by value.
 */
public class LiveViewFuzzTest extends AbstractLiveViewTest {

    @Before
    public void pinClockBelowFuzzData() {
        // Pin the clock a day below the generated data (which starts at 2026-01-01) before
        // every test. currentMicros is a static that survives across test classes, and the
        // per-helper pins below only fire when it is still unset - so a class that ran earlier
        // in the same JVM and left a clock ABOVE the data would silently push every
        // START FROM NOW boundary above it, emptying views the recompute oracle expects to be
        // full. Pinning here unconditionally is what makes this suite independent of the class
        // that happens to precede it.
        setCurrentMicros(MicrosTimestampDriver.floor("2025-12-31T00:00:00.000000Z"));
    }

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
    // and SEED in any combination.
    // FLUSH EVERY rate-limits LV commits by wall clock: a refresh within
    // flushEveryMicros of the previous commit is deferred. Tests drive a
    // controllable clock (currentMicros) and advance it past this interval
    // before each refresh so flushes are deterministic, not wall-clock racy.
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
    // The anchor shapes testFuzzConcurrentWritersAnchored drives over runConcurrentWriterFuzz,
    // one run each: zone-less and zoned daily anchors, twice over. Each draws a wall time its
    // zone's fall-back does not repeat, so the bucket oracle stays exact.
    private static final int[] CONCURRENT_WRITER_ANCHOR_MODES = {
            LiveViewFuzzAnchor.DAILY_PLAIN, LiveViewFuzzAnchor.DAILY_ZONED,
            LiveViewFuzzAnchor.DAILY_PLAIN, LiveViewFuzzAnchor.DAILY_ZONED
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
    // Anchored shapes past ANCHORED_VARIANT_COUNT, which only testFuzzResumeChainPrice drives, so
    // every other anchored arm draws what it drew before. Each one's seals image every key whole,
    // as rank's and dense_rank's do: lag keeps no dirty set, covar_pop no baseline, and an
    // EXCLUDE CURRENT ROW avg or first_value is a ring - AvgDouble's own class, and a
    // first_value that inherits BasePartitionedWindowFunction's markPartitionAlive.
    private static final int ANCHORED_LAG_VARIANT = 7;
    private static final int ANCHORED_COVAR_POP_VARIANT = 8;
    private static final int ANCHORED_EXCLUDE_CURRENT_ROW_AVG_VARIANT = 9;
    private static final int ANCHORED_EXCLUDE_CURRENT_ROW_FIRST_VALUE_VARIANT = 10;
    // testFuzzResumeChainPrice's shapes, every one a function a chained seal images whole: the
    // unanchored ROWS and RANGE sums and avgs and lag, and the anchored rank, dense_rank, lag,
    // covar_pop and EXCLUDE CURRENT ROW avg and first_value. The first_value comes last, so
    // every shape before it draws what it drew before it was added.
    private static final int[] CHAIN_PRICE_UNANCHORED_VARIANTS = {
            0, 4, RANGE_SUM_VARIANT, RANGE_AVG_VARIANT, LAG_VARIANT
    };
    private static final int[] CHAIN_PRICE_ANCHORED_VARIANTS = {
            5, 6, ANCHORED_LAG_VARIANT, ANCHORED_COVAR_POP_VARIANT, ANCHORED_EXCLUDE_CURRENT_ROW_AVG_VARIANT,
            ANCHORED_EXCLUDE_CURRENT_ROW_FIRST_VALUE_VARIANT
    };
    // Salt the chain price arm's draws apart from the run's sequence, so its runs draw their data
    // as every other arm's do.
    private static final long CHAIN_PRICE_SALT0 = 0xC2B2AE3D27D4EB4FL;
    private static final long CHAIN_PRICE_SALT1 = 0x165667B19E3779F9L;
    // The keys a new-key episode's correction brings: more than three times the 16 a run draws,
    // and more than the 17 other rows the episode's partition holds above its anchor. See
    // commitNewKeyEpisode.
    private static final int NEW_KEY_EPISODE_KEYS = 50;
    // runLeadOrderingFuzz's cycle group: flush, in-order, in-order, late-arrival.
    private static final int CYCLES_PER_GROUP = 4;
    private static final int LATE_COMMIT_PHASE = 3;
    private static final int MAX_FRAME = 20;
    // START FROM modes a run draws from. The mode decides only HOW the view resolves its
    // event-time boundary at CREATE - every refresh path reads the resolved boundary and none
    // of them reads the kind - so what actually varies across the three is the boundary VALUE
    // they can produce:
    //   BEGINNING  no floor at all (LONG_NULL), so every scan runs unbounded;
    //   NOW        the CREATE-moment clock, which the harness keeps pinned below every
    //              generated row: a FINITE floor that still admits the whole dataset, so the
    //              seed sweep, the forward drain and both replays open their cursors AT a real
    //              timestamp (partition culling, binary search) rather than from LONG_NULL;
    //   TIMESTAMP  a literal cutting the dataset at a random row - the only mode that puts base
    //              rows BELOW the bound, which is what exercises the seed's partial sweep, the
    //              forward path's below-bound rejects, and the floor on every replay.
    // The recompute oracle folds the same resolved boundary into its own WHERE, so a run's
    // cross-check stays exact whichever mode it drew.
    private static final int START_FROM_BEGINNING = 0;
    private static final int START_FROM_MODES = 3;
    private static final int START_FROM_NOW = 1;
    private static final int START_FROM_TIMESTAMP = 2;
    // Sentinel i for phantom (rolled-back) rows: large and positive, so a leaked phantom
    // both survives a WHERE i>0 filter and stands out against the [-1000, 1000] real data.
    private static final long PHANTOM_SENTINEL = 999_999;
    // The base removals runAnchoredRemovalFuzz drives.
    private static final int REMOVAL_DROP_PARTITION = 1;
    private static final int REMOVAL_TRUNCATE = 0;
    private static final int REMOVAL_TTL = 2;
    // Numbers the marker each run of runAnchoredRemovalFuzz logs, so its check of the test's
    // capture starts at its own run.
    private static final AtomicLong ANCHORED_REMOVAL_RUN_SEQ = new AtomicLong();
    // A line live view code logged that holds a date with a year of five digits or more: see
    // assertNoLiveViewLineReadsNanosAsMicrosAfter.
    private static final String LIVE_VIEW_LINE_WITH_A_FIVE_DIGIT_YEAR = "live view[^\\n]*\\d{5}-\\d\\d-\\d\\dT";
    // Numbers the marker each run over a nanosecond base logs, so its check of the test's capture
    // starts at its own run.
    private static final AtomicLong NANOS_BASE_RUN_SEQ = new AtomicLong();
    // What the restatement guard logs when a restore that failed its count stands it down: see
    // failTheSecondRepairsCommit.
    private static final String RESTATEMENT_GUARD_STAND_DOWN = "live view rebuild from the applied base runs without the restatement guard";
    // Numbers the marker each episode of failTheSecondRepairsCommit logs, so the check of the
    // test's capture after it starts at its own episode.
    private static final AtomicLong SECOND_REPAIR_EPISODE_SEQ = new AtomicLong();
    // What the restart after failTheOwedSealOfALateRow meets: the repair marker still live over
    // the splice, a root at the frontier that reads it as stale, no timeline, which a streak of
    // failed seals retired, or the timeline and its live marker, which that retire kept under a
    // lock on _timeline.
    private static final int OWED_SEAL_MARKER_LIVE = 0;
    private static final int OWED_SEAL_RESTORABLE = 1;
    private static final int OWED_SEAL_RETIRED = 2;
    private static final int OWED_SEAL_RETIRED_TIMELINE_KEPT = 3;
    // Salt the owed-seal episode's random sequence apart from the run's, which drawing it leaves
    // where it was, so a pinned-seed batch draws what it drew before the episode existed.
    private static final long OWED_SEAL_EPISODE_SALT0 = 0x5DEECE66DL;
    private static final long OWED_SEAL_EPISODE_SALT1 = 0x2545F4914F6CDD1DL;
    // Salt the draw of a lock on _timeline for a retire apart from the episode's sequence, for the
    // same reason.
    private static final long KEPT_TIMELINE_RETIRE_SALT0 = 0x3243F6A8885A308DL;
    private static final long KEPT_TIMELINE_RETIRE_SALT1 = 0x13198A2E03707344L;
    // What a seed sweep's seal logs when it fails.
    private static final String SEED_SEAL_FAILED = "could not write live view seed checkpoint [view=lv";
    // What a restart logs when a live repair marker sends it to the rebuild from the applied base.
    private static final String MARKER_REBUILD = "live view restart rebuilding from applied base [view=lv, "
            + "cause=prefix preservation repair marker present";
    // Salt the draw of a crash at a departure's frontier truncate apart from the run's sequence,
    // for the same reason.
    private static final long FRONTIER_TRUNCATE_CRASH_SALT0 = 0x94D049BB133111EBL;
    private static final long FRONTIER_TRUNCATE_CRASH_SALT1 = 0x6A09E667F3BCC909L;
    // Salt the draws of the kept-timeline episode a departure's frontier truncate can open apart
    // from the run's sequence, for the same reason: see runKeptTimelineEpisode.
    private static final long KEPT_TIMELINE_EPISODE_SALT0 = 0x1B873593CC9E2D51L;
    private static final long KEPT_TIMELINE_EPISODE_SALT1 = 0x85EBCA6BC2B2AE35L;
    // Salt the draw of a fault on the kept-timeline episode's check of _timeline after the retire's
    // unlink apart from the run's sequence, for the same reason: see runKeptTimelineEpisode.
    private static final long KEPT_TIMELINE_CHECK_SALT0 = 0x9E3779B97F4A7C15L;
    private static final long KEPT_TIMELINE_CHECK_SALT1 = 0xD1B54A32D192ED03L;
    // Salt the draw of a lock on the repair marker the kept-timeline episode leaves without its
    // _timeline apart from the episode's sequence, so the episode draws what it drew before: see
    // runKeptTimelineEpisode.
    private static final long KEPT_TIMELINE_MARKER_SALT0 = 0x452821E638D01377L;
    private static final long KEPT_TIMELINE_MARKER_SALT1 = 0xBE5466CF34E90C6CL;
    // Salt the draw of a leftover repair marker beside a seed sweep apart from the run's sequence,
    // for the same reason: see driveSeed.
    private static final long SEED_MARKER_SALT0 = 0xC0AC29B7C97C50DDL;
    private static final long SEED_MARKER_SALT1 = 0x3F84D5B5B5470917L;
    // Salt the draws of the sparse departure whose frontier seal fails apart from the run's
    // sequence, for the same reason.
    private static final long FRONTIER_SEAL_FAILURE_SALT0 = 0x2C1B3C6D7E9F0A1BL;
    private static final long FRONTIER_SEAL_FAILURE_SALT1 = 0x5851F42D4C957F2DL;
    // Salt the draw of a seal pause ahead of a sparse departure apart from the run's sequence, for
    // the same reason.
    private static final long SPARSE_DEPARTURE_PAUSE_SALT0 = 0x9E3779B97F4A7C15L;
    private static final long SPARSE_DEPARTURE_PAUSE_SALT1 = 0xBF58476D1CE4E5B9L;
    // Salt the draw of which frontier-sealing departure of a run restores a root inside its new
    // frontier's frame apart from the run's sequence, for the same reason.
    private static final long FRONTIER_STATE_ROUTE_SALT0 = 0x8CB92BA72F3D8DD7L;
    private static final long FRONTIER_STATE_ROUTE_SALT1 = 0x3C6EF372FE94F82BL;
    // Salt the draws of a departure above a rowless anchor apart from the run's sequence, for the
    // same reason.
    private static final long ROWLESS_ANCHOR_EPISODE_SALT0 = 0xD6E8FEB86659FD93L;
    private static final long ROWLESS_ANCHOR_EPISODE_SALT1 = 0xA0761D6478BD642FL;
    // Salt the draw of a shutdown that cancels a drift recovery ahead of a restart apart from the run's
    // sequence, for the same reason.
    private static final long DRIFT_RECOVERY_CANCELLATION_SALT0 = 0x8BB84B93962EACC9L;
    private static final long DRIFT_RECOVERY_CANCELLATION_SALT1 = 0x4B33A62ED433D4A3L;
    // The job.run() passes a burst of the closing window runs after a shutdown cancelled a turn: see
    // cancelADriftRecoveryByShutdown and shutDownMidDrain.
    private static final int CLOSING_BURST_RUNS = 8;
    // The seed pair of testFuzzShutdownMidDrainShape.
    private static final long SHUTDOWN_MID_DRAIN_SHAPE_SEED0 = 6601564164335161269L;
    private static final long SHUTDOWN_MID_DRAIN_SHAPE_SEED1 = -3445544022848412854L;
    // The seed pairs of testFuzzDepartedOwedRootLandsOnTheFirstStretch and
    // testFuzzDepartedOwedRootLandsAfterThePause.
    private static final long DEPARTED_OWED_ROOT_SEED0 = 877946048693873L;
    private static final long DEPARTED_OWED_ROOT_SEED1 = 1795437718880L;
    private static final long DEPARTED_OWED_ROOT_PAUSE_SEED0 = 877957421481583L;
    private static final long DEPARTED_OWED_ROOT_PAUSE_SEED1 = 1795437730252L;
    // The seed pair of testFuzzClearedHeadLandsOnBothRoutes.
    private static final long CLEARED_HEAD_SEED0 = 896886180891510L;
    private static final long CLEARED_HEAD_SEED1 = 1795456659012L;
    // Salt the draws of a shutdown that cancels a lead drain ahead of a restart of runFuzz apart from the
    // run's sequence, for the same reason.
    private static final long SHUTDOWN_MID_DRAIN_SALT0 = 0x94D049BB133111EBL;
    private static final long SHUTDOWN_MID_DRAIN_SALT1 = 0x2545F4914F6CDD1DL;
    // Salt the draws of a two-commit departure episode apart from the run's sequence, so the run
    // draws what it drew before the episode existed.
    private static final long TWO_COMMIT_DEPARTURE_SALT0 = 0xA4093822299F31D0L;
    private static final long TWO_COMMIT_DEPARTURE_SALT1 = 0x082EFA98EC4E6C89L;
    // Salt the draw of the late row a departure keeps for the owed-root arm apart from the run's
    // sequence, for the same reason: see departNewestRows.
    private static final long DEPARTED_OWED_ROOT_SALT0 = 0x6C62272E07BB0142L;
    private static final long DEPARTED_OWED_ROOT_SALT1 = 0x62B821756295C58DL;
    // Salt the draws of a lagging TTL episode apart from the run's sequence, for the same reason:
    // see runLaggingTtlEpisode.
    private static final long LAGGING_TTL_EPISODE_SALT0 = 0xFF51AFD7ED558CCDL;
    private static final long LAGGING_TTL_EPISODE_SALT1 = 0xC4CEB9FE1A85EC53L;
    // What handleRefreshFailure logs when a recovery a turn ran met the refresh breaker: see
    // cancelADriftRecoveryByShutdown.
    private static final String RECOVERY_CANCELLED_LINE = "live view refresh cancelled during recovery [view=lv, dropped=false, invalid=false]";
    // The outcomes of the head-miss rebuild's end-of-table splice departNewestRowsOverSparseRoots
    // sets up, one per shape: the splice kept roots above the new frontier and the rebuild
    // truncates there, the rebuild emits no row over a root at the frontier and keeps the
    // timeline, or it emits none with no root there and seals the state it derived at the
    // frontier.
    private static final int SPARSE_DEPARTURE_FRONTIER_SEAL = 2;
    private static final int SPARSE_DEPARTURE_KEEP = 1;
    private static final int SPARSE_DEPARTURE_SHAPES = 3;
    private static final int SPARSE_DEPARTURE_TRUNCATE = 0;
    private static final String[] SYMBOLS = {
            "AA", "BB", "CC", "DD", "EE", "FF", "GG", "HH",
            "II", "JJ", "KK", "LL", "MM", "NN", "OO", "PP"
    };
    // The shapes runOperatorSuspensionFuzz records, which testFuzzOperatorWalSuspensionShapes pins.
    private static final int SUSPENSION_SHAPE_ADD_COLUMN = 0;
    private static final int SUSPENSION_SHAPE_LATE_ROW = 1;
    private static final int SUSPENSION_SHAPE_LIST_ACROSS_RESTART = 2;
    private static final String[] SUSPENSION_SHAPE_NAMES = {
            "a base ADD COLUMN inside an episode",
            "a late row inside an episode",
            "a listed view restarted inside an episode",
            "a repair parked across an episode",
            "a refusal at a writer acquire",
            "a refusal at a commit",
            "a refused restart rebuild",
            "a refused seed commit"
    };
    private static final int SUSPENSION_SHAPE_PARKED_REPAIR = 3;
    private static final int SUSPENSION_SHAPE_REFUSED_ACQUIRE = 4;
    private static final int SUSPENSION_SHAPE_REFUSED_COMMIT = 5;
    private static final int SUSPENSION_SHAPE_REFUSED_RESTART_REBUILD = 6;
    private static final int SUSPENSION_SHAPE_REFUSED_SEED = 7;
    // Seed pairs for testFuzzOperatorWalSuspensionShapes, each with whether its run is anchored, and the shapes it runs.
    private static final long[][] SUSPENSION_SHAPE_SEEDS = {
            // a refused seed commit, a base ADD COLUMN and a late row inside an episode
            {625199969504902L, 6314394583738L, 0},
            // a refusal at a writer acquire and a refused restart rebuild
            {858644142239532L, 6841997108474L, 0},
            // a listed view restarted inside an episode
            {700697597008148L, 8795306794114L, 0},
            // a refusal at a commit
            {319271087038110L, 1090664651966L, 0},
            // a repair parked across an episode
            {767450532419752L, 3528567112905L, 1}
    };
    private static final String SUSPENSION_PAUSE_LINE = "live view refresh paused, its WAL is suspended [view=lv";
    // Fails a test during which a WAL apply suspended a table, which would leave the oracles comparing
    // a frozen base or view table. runAnchoredRemovalFuzz reads its live view lines through the
    // rule's capture.
    @Rule
    public final WalApplySuspensionRule walApplySuspensionRule = new WalApplySuspensionRule();
    // The refresh jobs runDedupFuzz, runReplaceRangeFuzz and runAnchoredFuzz ran, summed, so a batch
    // can assert which repairs its departures or its late commits reached.
    private final LiveViewRouteCoverage coverage = new LiveViewRouteCoverage();
    // Copies the view's checkpoint directory to a crash image when a departure that drew a crash
    // reaches its frontier truncate's marker: see runUnderTheFrontierTruncateCrash. Locks the
    // view's _timeline for a retire an episode drew the lock for: see failTheOwedSealOfALateRow and
    // departOverSparseRootsFailingTheFrontierSeal. The tests whose runs end on departures install it.
    private final CheckpointFaults checkpointFaults = new CheckpointFaults();
    // The second late row failTheSecondRepairsCommit committed to the base alone, which base_shadow
    // takes once the restart after the episode has been checked against it; empty outside such an
    // episode.
    private final StringSink secondRepairShadowValues = new StringSink();
    // Heads a departure left below the newest timestamp the view has seen whose base coordinate
    // the stretch of cycles without rows after it moved, past the commits of any late rows ahead
    // of them, over a batch: see commitRowlessStretch.
    private int departureHeadAdvances;
    // Of those, the heads a sparse departure whose rebuild emitted no row re-stamped on the root
    // at the new frontier: see departNewestRowsOverSparseRoots.
    private int restampedHeadAdvances;
    // Of departureHeadAdvances, the heads of filtered views whose in-order rows came straight
    // after the departure, with no late row ahead of them: see commitRowlessStretch.
    private int inOrderOnlyHeadAdvances;
    // Late rows a batch's filters dropped below the newest timestamp a view had seen after a
    // departure, each of whose resumes moved the head to its commit: see commitLateRowsTheFilterDrops.
    private int lateRowlessHeadAdvances;
    // Filtered stretches of a batch that could commit late rows ahead of their in-order rows.
    // Every other one, from the first, does, and the rest commit their in-order rows straight
    // after the departure: see commitRowlessStretch. Counted rather than drawn, so a pinned-seed
    // batch draws what it drew before.
    private int lateRowStretchCandidates;
    // Departure episodes a batch ran. The episodes draw their stretch length and their unsealed
    // shape from it rather than from the run's random sequence, so a pinned-seed batch draws what
    // it drew before they existed.
    private int departureEpisodes;
    // Departures of a batch that could decline the checkpoint chain: every third one, from the
    // first, does. See departNewestRows.
    private int chainDeclineCandidates;
    // Departures above a rowless anchor a batch ran, every other one, from the first, with the
    // kept batch's root at the view's new frontier. Then, by how each ended with the chain
    // declined: a watermark that published the root the frontier truncate left at the frontier,
    // and a seal that landed at a frontier the truncate left no root at. See
    // departNewestRowsAboveARowlessAnchor.
    private int rowlessAnchorEpisodes;
    private int rowlessAnchorWatermarks;
    private int rowlessAnchorFrontierSeals;
    // Of rowlessAnchorEpisodes, the ones whose chained correction the chain price refused, which
    // leaves no rowless anchor to depart above: see departNewestRowsAboveARowlessAnchor.
    private int rowlessAnchorEpisodesPricedOut;
    // Set while testFuzzResumeChainPrice or testFuzzHeadMissChainPrice runs: runDedupFuzz then
    // draws its knobs for the chain price, checks the oracle after every restart, and runs a
    // new-key episode in one run of two, or a head-miss new-key episode in every run.
    private boolean isChainPriceArm;
    // Set while testFuzzDedupSkip runs: runDedupFuzz creates its base DEDUP SKIP, under which a
    // re-emitted (ts, sym) leaves the row the base holds, and runs no departure, since a SKIP base
    // drops the upserts that move a filtered view's rows out.
    private boolean isDedupSkipArm;
    // Set while testFuzzHeadMissChainPrice runs, beside isChainPriceArm: every run commits a
    // head-miss new-key episode instead of the resume's. See commitHeadMissNewKeyEpisode.
    private boolean isHeadMissChainPriceArm;
    // Runs of the chain price arm that could commit a new-key episode: every other one, from the
    // first, does. See commitNewKeyEpisode.
    private int newKeyEpisodeCandidates;
    // New-key episodes the batch ran, and the ones the price refused after a boundary froze.
    private int newKeyEpisodes;
    private int newKeyEpisodeLateDeclines;
    // Head-miss new-key episodes the batch ran, and the chained head-miss splices they published
    // ahead of the correction the price refused. See commitHeadMissNewKeyEpisode.
    private int headMissNewKeyEpisodes;
    private int headMissChainedSplices;
    // Set while testFuzzDedupAnchored or testFuzzTwoCommitDeparturesOfARangeFrame runs: an anchored
    // or RANGE-frame run of runDedupFuzz follows its main loop with a period whose newest rows depart
    // in two commits. See departAPeriodInTwoCommits, which runAnchoredRemovalFuzz runs as well.
    private boolean isTwoCommitDepartureArm;
    // Two-commit departure episodes the batch ran, the ones whose first departure took the head
    // miss, and the ones whose second departure derived the state at the view's new frontier. Of
    // those, the ones whose second departure, under a declined chain, sealed or kept the root at
    // that frontier after its commit.
    private int twoCommitDepartureEpisodes;
    private int twoCommitDepartureHeadMisses;
    private int twoCommitDepartureDerivations;
    private int twoCommitDepartureDeclinedChainDerivations;
    // Set while testFuzzDepartedOwedRootEpisodes or a pinned-seed test of it runs: every run
    // filters its view, and a departure that leaves a row above the root its repair anchors on,
    // with no root above that row, fails the repair's closing seal, which owes the row a root: the
    // resume's, when departNewestRows keeps a late row in the commit of a departure of a batch no
    // root sealed, which every such departure is, and the head-miss rebuild's, when
    // departNewestRowsOverSparseRoots keeps the departed batch's first row over its keeping shape.
    // On each route every other such departure, from the second, comes after two failed seals in a
    // row, so its closing seal is the third and pauses the seal. The stretch without rows after it
    // must land the owed root at the view's newest row, below the newest timestamp the view has
    // seen: see commitRowlessStretch.
    private boolean isDepartedOwedRootArm;
    private int departedOwedRootResumeEpisodes;
    private int departedOwedRootHeadMissEpisodes;
    // The root the last such departure owes, for the stretch after it to land, or LONG_NULL: with
    // the generation its repair marker names, the departure's base commit, whether its closing
    // seal paused the seal, and whether the head-miss rebuild repaired it.
    private long departedOwedRootTs = Numbers.LONG_NULL;
    private long departedOwedRootMarkerGeneration;
    private long departedOwedRootSeqTxn;
    private boolean isDepartedOwedRootPaused;
    private boolean isDepartedOwedRootHeadMiss;
    // The owed roots the stretches landed and the ones of them after a pause, then, of each, the
    // ones the head-miss rebuild owed.
    private int departedOwedRootLandings;
    private int departedOwedRootLandingsAfterThePause;
    private int departedOwedRootHeadMissLandings;
    private int departedOwedRootHeadMissLandingsAfterThePause;
    // Set while testFuzzClearedHeadEpisodes or its pinned-seed test runs: a filtered view's
    // departure whose repair keeps a root at the view's new frontier and only re-stamps the head on
    // it fails the read of the view's table that re-stamp makes, which declines it and leaves the
    // head clear: the resume's, when departNewestRows' frontier truncate keeps the kept batch's
    // root, and the head-miss rebuild's, when departNewestRowsOverSparseRoots emits no row over
    // that root. The stretch without rows after it must put the head back on that root at the
    // commit of its first row: see awaitTheClearedHead.
    private boolean isClearedHeadArm;
    // The root the last such departure left the head clear over, for the stretch after it to
    // publish, or LONG_NULL, with the publications of a cleared head the job counted by then and
    // whether the head-miss rebuild repaired it.
    private long clearedHeadRootTs = Numbers.LONG_NULL;
    private long clearedHeadPublicationsBefore;
    private boolean isClearedHeadHeadMiss;
    // The departures whose re-stamp the arm failed and the stretches that published their root,
    // then, of each, the ones the head-miss rebuild repaired.
    private int clearedHeadEpisodes;
    private int clearedHeadHeadMissEpisodes;
    private int clearedHeadLandings;
    private int clearedHeadHeadMissLandings;
    // Departure episodes of a batch whose departed rows no root sealed, so the resume repairing
    // their departure anchored on the head's own root and emitted nothing: see departNewestRows.
    private int unsealedDepartures;
    // Of those, the episodes whose kept batch landed below the newest timestamp the view had seen,
    // which a replace band that deleted the base's newest rows leaves above every row, so their
    // departed batch started above it: see departNewestRows.
    private int unsealedDeparturesBelowLatestSeenTs;
    // Late rows of a batch whose repair failed its closing seal over a splice with no root at the
    // view's frontier, which leaves the repair marker live: see failTheOwedSealOfALateRow. Of
    // those, the ones the head-miss rebuild repaired through its end-of-table splice. Then, by the
    // restart after each: refused over the day the run dropped, restored from the root a later
    // seal landed at the frontier, and met after the failure streak retired the timeline, which
    // the retire removed or a lock on _timeline kept. Of the refused ones, those whose retire
    // removed the timeline and its marker, so the restatement guard refused a rebuild over an
    // absent timeline.
    private int owedSealEpisodes;
    private int headMissOwedSealEpisodes;
    private int blockedOwedSealEpisodes;
    private int restoredOwedSealEpisodes;
    private int retiredOwedSealEpisodes;
    private int blockedTimelineAbsentOwedSealEpisodes;
    // Owed-seal episodes whose failures paused the seal, and of those, the ones whose clean commit
    // came inside the pause with the marker live, and the ones whose clean commit came after it
    // and landed, on either route and on the resume route: see failTheOwedSealOfALateRow.
    private int pausedOwedSealEpisodes;
    private int markerLiveInsideThePauseOwedSealEpisodes;
    private int restoredAfterThePauseOwedSealEpisodes;
    private int restoredAfterThePauseResumedOwedSealEpisodes;
    // Runs of testFuzzBaseDdlInvalidation whose invalid view a second invalidating operation reached:
    // see invalidateBaseAgain.
    private int secondBaseInvalidations;
    // Lagging TTL episodes a batch ran, the ones the open anchor segment's residual repaired, and of
    // those the ones that fell back to the boundary rebuild over the rows a TTL eviction froze: see
    // runLaggingTtlEpisode.
    private int laggingTtlEpisodes;
    private int laggingTtlResidualRepairs;
    private int laggingTtlResidualBoundaryRebuilds;
    // Set while testFuzzDedup or testFuzzReplaceRange runs: every other owed-seal episode of its
    // batch, from the first, counted rather than drawn, so a pinned-seed batch of another test
    // draws what it drew before, ends on a second late row whose replacement commit fails inside
    // the window the failed closing seal left open: see failTheSecondRepairsCommit. Then those
    // episodes, the ones of them that failed the commit in the process, the ones that stopped the
    // process at it, and the ones whose in-place rebuild the restatement guard refused over the
    // day the run dropped.
    private boolean isSecondRepairCommitFaultArm;
    private int secondRepairCommitFaultCandidates;
    private int secondRepairCommitFaultEpisodes;
    private int secondRepairCommitFaultsInPlace;
    private int secondRepairCommitStops;
    private int secondRepairRebuildsRefusedInPlace;
    // The marker failTheSecondRepairsCommit logged ahead of its fault, which the check after the
    // restart that follows it reads the capture from; null outside such an episode.
    private String secondRepairEpisodeMarker;
    // Sparse departures whose rebuild re-stamped the head on the root at the new frontier inside a
    // pause the seal's failures armed ahead of them: see departNewestRowsOverSparseRoots.
    private int restampsInsideThePause;
    // Departures of a batch whose frontier truncate a crash interrupted between its marker and its
    // publication: the resume's, which departNewestRows reaches, and the head-miss rebuild's, which
    // the truncating sparse departure reaches. Then, by the restart from the crash image: a rebuild
    // from the applied base, and one the restatement guard refused over the day the run dropped.
    // No such restart may restore the splice's generation the crash left.
    private int resumeFrontierTruncateCrashes;
    private int headMissFrontierTruncateCrashes;
    private int rebuiltFrontierTruncateCrashes;
    private int blockedFrontierTruncateCrashes;
    // Retires that met a lock on _timeline and kept the timeline with its live repair marker: the
    // one at the fourth failure in a row of an owed-seal episode, and the one that answers the
    // failed frontier seal of a filtered sparse departure. See failTheOwedSealOfALateRow and
    // departOverSparseRootsFailingTheFrontierSeal. The restart after each must rebuild over the
    // marker, and restartRebuildsOverTheMarkerBefore holds the count of such rebuilds the test's
    // capture showed ahead of it.
    private int owedSealKeptTimelineRetires;
    private int sparseFrontierSealKeptTimelineRetires;
    private int restartRebuildsOverTheMarkerBefore;
    // Set while testFuzzDedup, testFuzzReplaceRange or testFuzzKeptTimelineEpisodes runs: a
    // departure of a filtered view whose resume reaches its frontier truncate may fail that
    // truncate's publication under a lock on _timeline, which keeps the splice's generation the
    // view then refuses. Then the episodes that kept the timeline, the seals the view refused
    // while the lock held, the late rows it repaired with no anchor meanwhile, and the fresh
    // histories the episodes opened once the lock went: see runKeptTimelineEpisode.
    private boolean isKeptTimelineEpisodeArm;
    // Kept-timeline episodes whose retire could not check _timeline after the unlink the lock
    // refused: see runKeptTimelineEpisode.
    private int keptTimelineCheckFaults;
    private int keptTimelineEpisodes;
    private int keptTimelineFreshHistories;
    private int keptTimelineLateRowRepairs;
    // Kept-timeline episodes whose repair marker a lock kept once _timeline went, the seed sweeps
    // that sealed a seed root beside a marker a lock kept, and the seals refused because they would
    // open a runtime history beside such a marker: see runKeptTimelineEpisode and driveSeed. Then
    // the kept-timeline episodes whose marker no lock kept, where the first seal once the lock on
    // _timeline went retried the retire, which removed _timeline and the marker with it, and
    // opened the fresh history.
    private int keptTimelineMarkerLeftovers;
    private int keptTimelineRefusedSeals;
    private int keptTimelineRetiresWithTheMarker;
    private int refusedHistoryOpenings;
    private int seedRootsBesideAMarker;
    // Retires that answered the failed frontier seal of a sparse departure with no lock on
    // _timeline, which removed the timeline and its marker, and whose restart the restatement
    // guard refused over the day the run dropped, as a rebuild over an absent timeline: see
    // departOverSparseRootsFailingTheFrontierSeal.
    private int blockedTimelineAbsentSparseFrontierSeals;
    // Turns runDedupFuzz ran ahead of a restart under a shutdown that trips the refresh breaker on the
    // drift of a base ADD COLUMN, and of those, the turns whose drift recovery the breaker ended rather
    // than one that completed ahead of any breaker check: see cancelADriftRecoveryByShutdown.
    private int driftRecoveryCancellationTurns;
    private int cancelledDriftRecoveries;
    // Of those turns, the ones after which the fallback scan would still drive the view, so the burst of the
    // closing window that followed met a view a worker would have refreshed: see runClosingBurst.
    private int drivenClosingBursts;
    // Shutdowns runFuzz ran ahead of a restart that cancelled a lead drain after it fed rows past the lead, so the
    // burst of the closing window that followed met accumulators ahead of the lead: see shutDownMidDrain.
    private int leftoverClosingBursts;

    @Test
    public void testFuzzAnchored() throws Exception {
        // Differential fuzz for anchored (ANCHOR EXPRESSION) windows. The anchor
        // resets the cumulative aggregate whenever the anchor expression changes
        // value in ts order. The fuzz uses a MONOTONIC anchor (timestamp_floor),
        // so "reset on change" is identical to "partition by the bucket value" -
        // the oracle is therefore the equivalent (sym, bucket)-partitioned regular
        // window recomputed over the base. Driven under O3 plus optional restart
        // and optional seed, so the anchor map rebuild on head-miss / head-hit
        // replay and across a restart is cross-checked against the recompute. The
        // final two variants are the F11 ranking shapes rank() and dense_rank():
        // they need the unbounded ordered frame a ranking function implies, so
        // they ride the anchor here rather than a bounded-ROWS frame.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        if (rnd.nextBoolean()) {
            // A replay budget of a few rows parks repairs across turns, so a restart can land
            // on a parked repair as well as on an un-flushed lead.
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1 + rnd.nextInt(8));
        }
        assertMemoryLeak(() -> {
            for (int v = 0; v < ANCHORED_VARIANT_COUNT; v++) {
                runAnchoredFuzz(rnd, v, 120 + rnd.nextInt(180), rnd.nextBoolean(), rnd.nextBoolean(), false, false);
            }
        });
    }

    @Test
    public void testFuzzAnchoredNanosBase() throws Exception {
        // testFuzzAnchored's arm over a TIMESTAMP_NS base. Only an anchored view repairs one
        // segment at a time, so this is the arm whose segment repairs log their timestamps over a
        // nanosecond base, and each run checks that every live view line printed them in
        // nanoseconds. The recompute oracle holds as over a microsecond base. Every other variant
        // indexes sym, the odd or the even ones by the run's draw, so its segment repairs weigh a
        // keyed read, and a one-row index open price lets a segment as small as a fuzz run's price
        // one. The batch asserts it repaired segments and weighed a keyed read of at least one. It
        // does not assert a priced one: the walk counts a segment it cannot price as unpriced, and
        // in some draws the indexed variants repair only such segments.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        if (rnd.nextBoolean()) {
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1 + rnd.nextInt(8));
        }
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        final int indexedVariantParity = rnd.nextInt(2);
        assertMemoryLeak(() -> {
            for (int v = 0; v < ANCHORED_VARIANT_COUNT; v++) {
                runAnchoredFuzz(rnd, v, 120 + rnd.nextInt(180), rnd.nextBoolean(), rnd.nextBoolean(), true, v % 2 == indexedVariantParity);
            }
        });
        coverage.log(LOG, "anchored nanos");
        coverage.assertReachedStat(LiveViewRefreshStats.SEGMENT_REPAIR_COUNT);
        Assert.assertTrue(
                "no indexed variant weighed a keyed read of a segment\n" + coverage.table(),
                coverage.get(LiveViewRefreshStats.KEYED_SCAN_PRICED_COUNT) + coverage.get(LiveViewRefreshStats.KEYED_SCAN_UNPRICED_COUNT) > 0
        );
    }

    @Test
    public void testFuzzSeed() throws Exception {
        // SEED + O3: the head-miss REPLACE_RANGE [replayMinTs, +inf) re-merges into the
        // multi-partition seeded data. This used to corrupt the view through a storage-engine
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
        // reaches the invalid view. A second invalidating operation then
        // reaches the invalid view, which keeps the first one's reason, in
        // memory and across a restart.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(() -> {
            for (int op = 0; op < 5; op++) {
                runBaseDdlInvalidationFuzz(rnd, op, 40 + rnd.nextInt(80));
            }
        });
        Assert.assertEquals("every run must invalidate its view a second time", 5, secondBaseInvalidations);
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
        // around them. Runs under O3 with optional restart / SEED /
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
    public void testFuzzBaseTtlAnchored() throws Exception {
        // testFuzzBaseTtl over anchored views, whose repairs follow the anchor's segments and,
        // over an indexed key, the correction's keys, with the repair knobs drawn per run (see
        // LiveViewFuzzKnobs). Phase two stays in timestamp order, as there, because TTL keeps
        // evicting behind it, so the oracle is the whole logical dataset bucketed by the anchor.
        // The first run, and every later one with even odds, ends on a lagging repair over an
        // hour the TTL evicted from the view's open anchor segment: see runLaggingTtlEpisode.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            for (int i = 0; i < 3; i++) {
                runAnchoredRemovalFuzz(rnd, rnd.nextInt(ANCHORED_VARIANT_COUNT), 120 + rnd.nextInt(160),
                        REMOVAL_TTL, rnd.nextBoolean(), false, rnd.nextBoolean(), i == 0 || rnd.nextBoolean());
            }
        });
        assertReachedLaggingTtlResidualBoundaryRebuilds();
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
        // the from-scratch recompute. Optional SEED captures pre-CREATE history
        // (single-writer, so the seed floor pins the global-min ts) before the
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
                        2 + rnd.nextInt(3), concurrentRefresh, rnd.nextBoolean(), null);
            }
        });
    }

    @Test
    public void testFuzzConcurrentWritersAnchored() throws Exception {
        // testFuzzConcurrentWriters over anchored windows, zone-less and zoned, each run with a
        // concurrent refresh driver and a backfilling writer. The streaming writers commit the
        // rows in timestamp order; the backfilling writer holds back a share of them and commits
        // each one only once the stream has moved past it. Every writer commits once per refresh
        // tick, and half the late commits land behind the tick's streaming ones, so a drain
        // meets in-order rows and a late row into a closed anchor segment in one pass. That
        // interleave is where a per-segment loop over a runtime still holding the drain's
        // in-order rows folded them twice, and inflated count and sum: the deterministic cases
        // are the testInOrderRowsCoalescedWithALateCommit* family in
        // LiveViewCheckpointSegmentRepairTest. A later repair of the same segment can heal such
        // a row before the run ends, so the refresh driver also compares the view with the
        // recompute between ticks, whenever the view has caught up with the base. The repair
        // knobs are drawn per run with the per-segment loop on (see LiveViewFuzzKnobs).
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            long segmentRepairs = 0;
            for (int i = 0; i < CONCURRENT_WRITER_ANCHOR_MODES.length; i++) {
                segmentRepairs += runConcurrentWriterFuzz(rnd, rnd.nextInt(ANCHORED_VARIANT_COUNT), 300 + rnd.nextInt(300),
                        2 + rnd.nextInt(2), true, rnd.nextBoolean(),
                        LiveViewFuzzAnchor.draw(rnd, CONCURRENT_WRITER_ANCHOR_MODES[i]));
            }
            // A batch whose late rows never reached a closed segment raced nothing this test is about.
            Assert.assertTrue("the late rows must have repaired closed anchor segments", segmentRepairs > 0);
        });
    }

    @Test
    public void testFuzzDedup() throws Exception {
        // Differential fuzz over a DEDUP UPSERT KEYS(ts, sym) base - the coupled,
        // applied-reader refresh path. Unlike every other arm, timestamps are NOT
        // unique: they are drawn from a small pool so many rows share one ts across
        // different keys (additive same-ts, the Phase 2a clean raw-WAL fast path),
        // and a forced fraction re-emit an existing (ts, sym) with a new value (real
        // dedup replacement, routed through the applied-reader replay; testFuzzDedupSkip
        // runs a SKIP base, which drops the re-emitted row instead). The recompute
        // oracle stays sound because (ts, sym) is the dedup key: after apply every
        // (ts, sym) is unique, so within each sym partition ts is a total order and a
        // partitioned window is a deterministic function of the final deduped base.
        // Each variant runs under O3 with random restart and seed; DROP PARTITION
        // of an unprocessed future band (removals=true) exercises the divergence gate
        // - a removed row the LV never emitted must not leak onto the raw-WAL path.
        isSecondRepairCommitFaultArm = true;
        isKeptTimelineEpisodeArm = true;
        runDedupFuzzBatch(TestUtils.generateRandom(LOG));
        coverage.log(LOG, "dedup");
        // Each run ends moving its newest rows out of the view: see departNewestRows, and
        // departNewestRowsOverSparseRoots for the RANGE-frame runs, which take one shape each.
        coverage.assertReachedStat(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT);
        coverage.assertReachedStat(LiveViewRefreshStats.SPLICE_ZERO_ROW_KEEP_COUNT);
        coverage.assertReachedStat(LiveViewRefreshStats.TRUNCATE_ZERO_ROW_KEEP_COUNT);
        assertReachedEveryRowlessAnchorOutcome();
        assertReachedEveryHeadMissSpliceOutcome();
        assertReachedEveryRowlessStretchOutcome();
        assertReachedAnOwedSealEpisode();
        assertReachedASecondRepairCommitFault();
        assertReachedAResumeFrontierTruncateCrash();
        logKeptTimelineRetires();
        logKeptTimelineEpisodes();
    }

    @Test
    public void testFuzzUnderTheProductionCadence() throws Exception {
        // Unanchored runs under the production rows cadence, so a stream of late commits resumes
        // over the repair roots its closing seals leave, and the seals remove them: the drawn
        // unanchored arms pin a cadence of a few rows, under which nearly every root is a cadence
        // root. Each run checks the view against the recompute, and that every repair root the
        // view tracks is one of the two newest roots.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            for (int i = 0; i < 4; i++) {
                runFuzz(rnd, rnd.nextInt(variantCount()), 120 + rnd.nextInt(160), true, rnd.nextBoolean(), rnd.nextBoolean(), false);
            }
        });
    }

    @Test
    public void testFuzzKeptTimelineEpisodes() throws Exception {
        // A pinned seed pair whose testFuzzDedup batch fails the frontier truncate of a filtered
        // view's departure under a lock on _timeline twice, each time keeping the splice's
        // generation: the view refuses its seals while the lock holds, repairs a late row with no
        // anchor in the kept generation, and opens a fresh history once the lock goes. In the first
        // episode the first seal after the lock retries the retire, which removes _timeline and
        // the marker with it, and opens the history. In the second a lock keeps the marker once
        // _timeline goes, and the writer refuses that seal, which would open a runtime history
        // beside it. The batch's seed sweeps seal seed roots beside a marker a lock keeps too. See
        // runKeptTimelineEpisode and driveSeed. A generator change that stops producing any of
        // that fails here.
        isSecondRepairCommitFaultArm = true;
        isKeptTimelineEpisodeArm = true;
        runDedupFuzzBatch(TestUtils.generateRandom(LOG, 289968849569755L, 1794849741680L));
        logKeptTimelineEpisodes();
        Assert.assertTrue("no departure kept its timeline under the lock", keptTimelineEpisodes > 0);
        Assert.assertTrue("no retire under the lock failed its check of the timeline", keptTimelineCheckFaults > 0);
        Assert.assertTrue("no seal was refused over a kept timeline", keptTimelineRefusedSeals > 0);
        Assert.assertTrue("no late row was repaired over a kept timeline", keptTimelineLateRowRepairs > 0);
        Assert.assertTrue("no seal opened a fresh history once the lock went", keptTimelineFreshHistories > 0);
        Assert.assertTrue("no seal's retry of the retire removed the kept timeline with its marker", keptTimelineRetiresWithTheMarker > 0);
        Assert.assertTrue("no lock kept the marker once the kept timeline went", keptTimelineMarkerLeftovers > 0);
        Assert.assertTrue("no seed root landed beside a marker a lock kept", seedRootsBesideAMarker > 0);
        Assert.assertTrue("no seal was refused a runtime history beside such a marker", refusedHistoryOpenings > 0);
    }

    @Test
    public void testFuzzDedupSkip() throws Exception {
        // testFuzzDedup and testFuzzDedupAnchored over a DEDUP SKIP KEYS(ts, sym) base. The forced
        // re-emissions of an earlier (ts, sym) leave the row the base already holds, in the same
        // commit as across commits, so a repair that read the newest write as the surviving row
        // diverges from the recompute over the applied base. No run ends with a departure: see
        // isDedupSkipArm.
        isDedupSkipArm = true;
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(() -> {
            for (int i = 0; i < DEDUP_VARIANTS.length; i++) {
                runDedupFuzz(rnd, DEDUP_VARIANTS[i], 120 + rnd.nextInt(160),
                        true, rnd.nextBoolean(), rnd.nextBoolean(), true, null, -1);
            }
            for (int i = 0; i < 2; i++) {
                runDedupFuzz(rnd, rnd.nextInt(ANCHORED_VARIANT_COUNT), 120 + rnd.nextInt(160),
                        true, true, rnd.nextBoolean(), true,
                        LiveViewFuzzAnchor.draw(rnd, rnd.nextInt(LiveViewFuzzAnchor.DAILY_EPOCH)), -1);
            }
        });
    }

    @Test
    public void testFuzzDedupAnchored() throws Exception {
        // testFuzzDedup over anchored views on a DEDUP UPSERT KEYS(ts, sym) base whose key is
        // indexed in most runs, with the repair knobs drawn per run. Restarts land within a
        // checkpoint cadence or two of a collapse, behind removals of rows the view never
        // emitted: the shape that once stopped a restarting view over a deduplicating base.
        // Each run's main loop is followed by a period whose newest rows depart in two commits,
        // the second of which resumes from a root above the view's new frontier: see
        // departAPeriodInTwoCommits.
        isTwoCommitDepartureArm = true;
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            for (int i = 0; i < 3; i++) {
                runDedupFuzz(rnd, rnd.nextInt(ANCHORED_VARIANT_COUNT), 120 + rnd.nextInt(160),
                        true, true, rnd.nextBoolean(), true,
                        LiveViewFuzzAnchor.draw(rnd, rnd.nextInt(LiveViewFuzzAnchor.DAILY_EPOCH)), -1);
            }
        });
        coverage.log(LOG, "dedup anchored");
        assertReachedTwoCommitDepartures();
        assertReachedDeclinedChainTwoCommitDepartures();
        coverage.assertReachedStat(LiveViewRefreshStats.RESUME_FRONTIER_STATE_RESTORE_COUNT);
    }

    @Test
    public void testFuzzTwoCommitDeparturesOfARangeFrame() throws Exception {
        // testFuzzDedup's RANGE-frame runs, each followed by a period whose newest rows depart in
        // two commits: the first takes the head miss, which warms from the frame below it, and the
        // second resumes from the root that head miss re-versioned, whose state misses the rows of
        // the view's new frontier's frame below that floor. See departAPeriodInTwoCommits.
        isTwoCommitDepartureArm = true;
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(checkpointFaults, () -> {
            for (int i = 0; i < 4; i++) {
                runDedupFuzz(rnd, i % 2 == 0 ? RANGE_SUM_VARIANT : RANGE_AVG_VARIANT, 120 + rnd.nextInt(160),
                        true, rnd.nextBoolean(), rnd.nextBoolean(), true, null, -1);
            }
        });
        coverage.log(LOG, "two-commit departures of a range frame");
        assertReachedTwoCommitDepartures();
        assertReachedDeclinedChainTwoCommitDepartures();
        coverage.assertReachedStat(LiveViewRefreshStats.RESUME_FRONTIER_STATE_WARM_UP_COUNT);
    }

    @Test
    public void testFuzzDedupDriftRecoveryCancellationShape() throws Exception {
        // A pinned seed pair whose testFuzzDedup run ahead of a restart has a shutdown end the drift recovery a
        // base ADD COLUMN asks for (see cancelADriftRecoveryByShutdown), rather than see the recovery complete
        // ahead of any breaker check. A generator change that stops producing it fails here.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 2);
        final Rnd rnd = new Rnd(3400517558656649421L, 9097098649117042840L);
        assertMemoryLeak(checkpointFaults, () -> runDedupFuzz(rnd, 0, 200, true, true, false, true, null, -1));
        Assert.assertTrue("the pinned seeds must cancel a drift recovery", cancelledDriftRecoveries > 0);
        Assert.assertTrue("the pinned seeds must run a closing burst over a view the scan would drive", drivenClosingBursts > 0);
    }

    @Test
    public void testFuzzShutdownMidDrainShape() throws Exception {
        // A pinned seed pair whose runFuzz run has a shutdown cancel a lead drain after it fed rows past the lead,
        // ahead of a restart (see shutDownMidDrain): the burst of the closing window that follows must start no
        // turn, and the view the restart recovers must equal the recompute. A generator change that stops
        // producing it fails here. The running count of variant 5 shifts every later row by any row fed twice, and
        // a root a commit seals keeps the cadence's seal of the flush due.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        final Rnd rnd = new Rnd(SHUTDOWN_MID_DRAIN_SHAPE_SEED0, SHUTDOWN_MID_DRAIN_SHAPE_SEED1);
        assertMemoryLeak(checkpointFaults, () -> runFuzz(rnd, 5, 200, true, true, false, false));
        Assert.assertTrue("the pinned seeds must cancel a lead drain that fed rows past the lead", leftoverClosingBursts > 0);
    }

    @Test
    public void testFuzzInMemReadBack() throws Exception {
        // Mode B read-back: a row_number() view (so SELECT * FROM lv routes through
        // the in-mem tier), with or without a SYMBOL passthrough (chosen per run),
        // fuzzed under O3 + optional restart + optional seed. After quiescence
        // the read-back is cross-checked three ways: it equals the from-scratch
        // recompute (the standard oracle), Mode B is confirmed actually engaged,
        // and the Mode B result is byte-identical to the forced disk-only path. The
        // SYMBOL passthrough form exercises the LV-space symbol-id translation
        // (segment-local churn across commits) through Mode B.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(() -> {
            final boolean restart = rnd.nextBoolean();
            final boolean seed = rnd.nextBoolean();
            runFuzz(rnd, 0, 120 + rnd.nextInt(280), true, restart, seed, true, true);
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
        // optional seed, so the per-partition lookback state survives the
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
    public void testFuzzLeadOrderingUnderO3() throws Exception {
        // Pins down the lead-ordering invariant: min(lead ts) >= max(disk ts)
        // whenever the seqTxn fence holds. Non-strict; see assertLeadAboveDisk for
        // why the tie is legitimate.
        //
        // The invariant is what would let a reader serve the lead as a band sitting
        // on top of the disk scan - notably in reverse, for ORDER BY ts DESC - so a
        // regression here is silently mis-ordered output rather than a crash.
        // Nothing in production asserts it: it emerges from the refresh job's O3
        // diversion, which sends a below-frontier commit to replay/rebuild instead
        // of appending it into the lead.
        //
        // The existing arms cannot cover it. Their refreshCycle advances the clock
        // past FLUSH EVERY, so every drain refreshes AND flushes and the lead is
        // always empty at the observation point. This arm holds the flush off on
        // most cycles so an un-flushed lead is genuinely resident while late rows
        // land on it - the only shape that could drive a below-disk row into a lead.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(() -> runLeadOrderingFuzz(rnd));
    }

    @Test
    public void testFuzzLeadReadBack() throws Exception {
        // Mode A read-back: after the randomized O3 + optional seed churn the
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
            final boolean seed = rnd.nextBoolean();
            runFuzz(rnd, 0, 120 + rnd.nextInt(280), true, false, seed, true, false, true);
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
            final boolean seed = rnd.nextBoolean();
            runFuzz(rnd, 0, 120 + rnd.nextInt(220), true, true, seed, true, false, true);
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
        // of the row set. Fuzzed under O3 plus optional restart / SEED /
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
    public void testFuzzOperatorWalSuspension() throws Exception {
        // SUSPEND WAL and cairo.wal.apply.suspended.tables pause a live view over out-of-order ingestion: one to
        // three episodes a run, each over random commits and refresh passes, with write denial drawn per run.
        // An episode may start inside a turn - ahead of its latch, at its commit, or while it handles a fault -
        // and may hold a base ADD COLUMN, a DROP PARTITION, a restart of a listed view, and a mid-drain fault
        // armed for the first drain after it. While an episode lasts the view consumes nothing, commits nothing,
        // stays valid and is charged nothing, and wal_tables() reports its source; after each, and at the end,
        // the view equals its SELECT over the base, or over a shadow of every insert after a removal.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault("ts", "x", "sym");
        final IntHashSet shapes = new IntHashSet();
        assertMemoryLeak(fault.facade(), () -> {
            for (int run = 0; run < 3; run++) {
                runOperatorSuspensionFuzz(rnd, fault, false, 60 + rnd.nextInt(140), shapes);
            }
        });
    }

    @Test
    public void testFuzzOperatorWalSuspensionAnchored() throws Exception {
        // testFuzzOperatorWalSuspension over anchored views, whose repairs follow the anchor's segments, with a
        // replay budget of a few rows in one run of two, so an episode can start over a parked repair.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        if (rnd.nextBoolean()) {
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1 + rnd.nextInt(4));
        }
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault("ts", "x", "sym");
        final IntHashSet shapes = new IntHashSet();
        assertMemoryLeak(fault.facade(), () -> {
            for (int run = 0; run < 3; run++) {
                runOperatorSuspensionFuzz(rnd, fault, true, 60 + rnd.nextInt(140), shapes);
            }
        });
    }

    @Test
    public void testFuzzOperatorWalSuspensionShapes() throws Exception {
        // Pinned seeds whose runs, between them, start or hold an episode in every shape the generator draws: a
        // refusal at a writer acquire, one at a commit, a refused restart rebuild, a refused seed commit, a
        // repair parked across an episode, a late row inside an episode, a base ADD COLUMN inside an episode, and
        // a listed view restarted inside an episode. A generator change that stops producing a shape fails here.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 2);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 2);
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault("ts", "x", "sym");
        final IntHashSet shapes = new IntHashSet();
        assertMemoryLeak(fault.facade(), () -> {
            for (int s = 0; s < SUSPENSION_SHAPE_SEEDS.length; s++) {
                final Rnd rnd = new Rnd(SUSPENSION_SHAPE_SEEDS[s][0], SUSPENSION_SHAPE_SEEDS[s][1]);
                runOperatorSuspensionFuzz(rnd, fault, SUSPENSION_SHAPE_SEEDS[s][2] == 1, 120, shapes);
            }
        });
        for (int shape = 0; shape < SUSPENSION_SHAPE_NAMES.length; shape++) {
            Assert.assertTrue("the pinned seeds must run the shape: " + SUSPENSION_SHAPE_NAMES[shape], shapes.contains(shape));
        }
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
        // base. SEED over a parquet base has its own arm (testFuzzSeedParquetBase),
        // since it reads base partitions rather than the WAL stream.
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
    public void testFuzzSeedParquetBase() throws Exception {
        // Differential fuzz over a SEED view whose pre-CREATE history lives in
        // parquet partitions. The seed sweep reads base partitions through a
        // page-frame cursor and resumes across turns with skipRows(); when the view
        // carries a WHERE, the filter is pushed down to the parquet row-group level,
        // so a fully non-matching partition is pruned before the scan yields its
        // rows. The skip must land on the same row the pruned scan next yields.
        // A tiny per-turn budget maximises the number of skipRows() resumes.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(() -> {
            for (int i = 0; i < FIXED_WIDTH_VARIANTS.length; i++) {
                runSeedParquetFuzz(rnd, FIXED_WIDTH_VARIANTS[i], 120 + rnd.nextInt(160),
                        rnd.nextBoolean());
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
            // SEED now combines with O3 (the merge bug forcing them apart is fixed).
            final boolean seed = rnd.nextBoolean();
            final int variant = rnd.nextInt(variantCount());
            final boolean restart = o3 && rnd.nextBoolean();
            runFuzz(rnd, variant, 80 + rnd.nextInt(520), o3, restart, seed, rnd.nextBoolean());
        });
    }

    @Test
    public void testFuzzRangeFrame() throws Exception {
        // Bounded RANGE frames exercise the monotonic-deque maintenance path,
        // distinct from the ROWS ring buffer the other aggregate variants use. The
        // recompute oracle holds identically: a bounded RANGE frame over a
        // unique-ts total order is a deterministic function of the row set. Fuzzed
        // under O3 plus optional restart and optional seed.
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
        // days of the emitted range), restart, SEED, and IN MEMORY.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(() -> {
            for (int i = 0; i < FIXED_WIDTH_VARIANTS.length; i++) {
                runRemovalFreezeContinueFuzz(rnd, FIXED_WIDTH_VARIANTS[i], 120 + rnd.nextInt(160),
                        rnd.nextBoolean(), rnd.nextBoolean(), rnd.nextBoolean(), rnd.nextBoolean(), false);
            }
        });
    }

    @Test
    public void testFuzzRemovalFreezeAndContinueLagging() throws Exception {
        // The freeze-and-continue contract while the refresh lags WAL apply. An
        // out-of-order commit (the trigger), a late row below an emitted view row and
        // a DROP / DETACH PARTITION of every day below the trigger's day apply before a
        // single refresh, the late row before or after the removal. The repair of the
        // trigger then runs against a base that already holds the later commits, and the
        // view must keep every row it emitted below the trigger's day byte-for-byte,
        // while the rows from that day up follow the base that remains. Half of the
        // variants run per invocation, picked by a drawn parity.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        final int laggingParity = rnd.nextInt(2);
        assertMemoryLeak(() -> {
            for (int i = 0; i < FIXED_WIDTH_VARIANTS.length; i++) {
                if ((i & 1) == laggingParity) {
                    runRemovalFreezeContinueFuzz(rnd, FIXED_WIDTH_VARIANTS[i], 120 + rnd.nextInt(160),
                            false, rnd.nextBoolean(), rnd.nextBoolean(), rnd.nextBoolean(), true);
                }
            }
        });
    }

    @Test
    public void testFuzzRemovalFreezeAndContinueAnchored() throws Exception {
        // testFuzzRemovalFreezeAndContinue over anchored views, whose repairs follow the anchor's
        // segments and, over an indexed key, the correction's keys, with the repair knobs drawn
        // per run. Phase two arrives out of order, so repairs run with removed rows below them.
        // See runAnchoredRemovalFuzz for what the oracle can and cannot claim then. Each run ends
        // on a period above the base's removed rows whose newest rows two pure-delete REPLACE_RANGE
        // bands remove: see departAPeriodInTwoCommits.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            for (int i = 0; i < 4; i++) {
                runAnchoredRemovalFuzz(rnd, rnd.nextInt(ANCHORED_VARIANT_COUNT), 120 + rnd.nextInt(160),
                        rnd.nextBoolean() ? REMOVAL_TRUNCATE : REMOVAL_DROP_PARTITION,
                        rnd.nextBoolean(), rnd.nextBoolean(), rnd.nextBoolean(), false);
            }
        });
        assertReachedTwoCommitDepartures();
        Assert.assertTrue("no second band derived the state at the view's new frontier", twoCommitDepartureDerivations > 0);
    }

    @Test
    public void testFuzzResumeChainPrice() throws Exception {
        // The resume's chain price under a key budget tight enough to refuse boundaries: a
        // whole-range resume keeps its chain only while the keys its boundaries image whole fit
        // max(the estimated replay rows, scan.max.keys), checked at each boundary as the replay
        // reaches it. Every shape here images its keys whole at each chained root, over a DEDUP
        // base. scan.max.keys is 1 or 2-5 and max.chained.boundaries 256, so the price, not the
        // count guard, decides. One run in two commits a new-key episode first, a correction the
        // price admits at its first boundary and refuses at its second (see commitNewKeyEpisode).
        // Each run ends on a departure and a restart. The view equals the recompute at every
        // oracle point and after every restart; a departure checks what stands on a kept chain only
        // where the price kept it.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        isChainPriceArm = true;
        try {
            assertMemoryLeak(checkpointFaults, () -> {
                for (int variant : CHAIN_PRICE_UNANCHORED_VARIANTS) {
                    runDedupFuzz(rnd, variant, 120 + rnd.nextInt(160), true, true, rnd.nextBoolean(), true, null, -1);
                }
                for (int variant : CHAIN_PRICE_ANCHORED_VARIANTS) {
                    runDedupFuzz(rnd, variant, 120 + rnd.nextInt(160), true, true, rnd.nextBoolean(), true,
                            LiveViewFuzzAnchor.draw(rnd, rnd.nextInt(LiveViewFuzzAnchor.DAILY_EPOCH)), -1);
                }
            });
        } finally {
            isChainPriceArm = false;
        }
        coverage.log(LOG, "chain price");
        LOG.info().$("LV fuzz chain price [newKeyEpisodes=").$(newKeyEpisodes)
                .$(", newKeyEpisodeLateDeclines=").$(newKeyEpisodeLateDeclines)
                .$(", rowlessAnchorEpisodes=").$(rowlessAnchorEpisodes)
                .$(", rowlessAnchorEpisodesPricedOut=").$(rowlessAnchorEpisodesPricedOut).I$();
        coverage.assertReachedStat(LiveViewRefreshStats.RESUME_CHAIN_KEY_BUDGET_DECLINE_COUNT);
        coverage.assertReachedStat(LiveViewRefreshStats.RESUME_CHAIN_DISCARDED_ROOT_COUNT);
        coverage.assertReachedStat(LiveViewRefreshStats.TRUNCATE_ZERO_ROW_KEEP_COUNT);
    }

    @Test
    public void testFuzzHeadMissChainPrice() throws Exception {
        // The end-of-table head miss's chain price under the chain price arm's key budget: a head
        // miss whose chain stands on nothing keeps it only while the keys its boundaries image
        // whole fit max(the replay's estimated rows, scan.max.keys), checked at each boundary as
        // the replay reaches it. testFuzzResumeChainPrice's anchored shapes, each imaging one
        // function whole, over a DEDUP base and an anchor whose segments start at UTC midnight.
        // Every run commits a head-miss new-key episode (see commitHeadMissNewKeyEpisode): a late
        // row whose chained splice publishes, then a correction the price admits at its first
        // boundary and refuses at its second, so both decline counters move in every run. The
        // view equals the recompute after each of the two and at every other oracle point.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        isChainPriceArm = true;
        isHeadMissChainPriceArm = true;
        try {
            assertMemoryLeak(checkpointFaults, () -> {
                for (int variant : CHAIN_PRICE_ANCHORED_VARIANTS) {
                    runDedupFuzz(rnd, variant, 120 + rnd.nextInt(160), true, true, rnd.nextBoolean(), true,
                            LiveViewFuzzAnchor.drawUtcMidnight(rnd), -1);
                }
            });
        } finally {
            isChainPriceArm = false;
            isHeadMissChainPriceArm = false;
        }
        coverage.log(LOG, "head-miss chain price");
        LOG.info().$("LV fuzz head-miss chain price [episodes=").$(headMissNewKeyEpisodes)
                .$(", chainedSplices=").$(headMissChainedSplices).I$();
        Assert.assertEquals("every run publishes a chained head-miss splice", CHAIN_PRICE_ANCHORED_VARIANTS.length, headMissChainedSplices);
        Assert.assertEquals("every run's price refuses its correction", CHAIN_PRICE_ANCHORED_VARIANTS.length, headMissNewKeyEpisodes);
        coverage.assertReachedStat(LiveViewRefreshStats.HEAD_MISS_SPLICE_KEY_BUDGET_DECLINE_COUNT);
        coverage.assertReachedStat(LiveViewRefreshStats.HEAD_MISS_SPLICE_DISCARDED_ROOT_COUNT);
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
        // trigger end to end, under optional restart / SEED / IN MEMORY:
        // any ghost row a converging replay failed to erase diverges from the
        // recompute oracle at quiescence.
        isSecondRepairCommitFaultArm = true;
        isKeptTimelineEpisodeArm = true;
        runReplaceRangeFuzzVariants(TestUtils.generateRandom(LOG), true);
        // Each run ends moving its newest rows out of the view: see departNewestRows, and
        // departNewestRowsOverSparseRoots for the RANGE-frame runs, which take one shape each.
        coverage.assertReachedStat(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT);
        coverage.assertReachedStat(LiveViewRefreshStats.SPLICE_ZERO_ROW_KEEP_COUNT);
        coverage.assertReachedStat(LiveViewRefreshStats.TRUNCATE_ZERO_ROW_KEEP_COUNT);
        assertReachedEveryRowlessAnchorOutcome();
        assertReachedEveryHeadMissSpliceOutcome();
        assertReachedEveryRowlessStretchOutcome();
        assertReachedAnOwedSealEpisode();
        assertReachedASecondRepairCommitFault();
        assertReachedAResumeFrontierTruncateCrash();
        logKeptTimelineRetires();
        logKeptTimelineEpisodes();
    }

    @Test
    public void testFuzzOwedSealEpisodeThroughThePause() throws Exception {
        // Batches of the RANGE-frame replace-range runs whose owed-seal episodes, between them,
        // pause the seal three ways: a clean commit after the pause lands its seal on the ladder
        // the pause kept, on each route, a clean commit inside the pause leaves the marker live,
        // and a fault that outlasts the pause retires the timeline at the fourth failure. Their
        // sparse departures re-stamp the head on the root at the new frontier inside a pause the
        // seal's failures armed ahead of them, and the stretch after it moves the head's base.
        // Each batch's keeping departure leaves its head below the newest timestamp its filtered
        // view has seen, so the batches run at least three stretches that may commit late rows,
        // and every other one does: at least one commits late rows ahead of its in-order rows,
        // and at least one commits its in-order rows straight after the departure.
        // A truncating one crashes between its frontier truncate's marker and the truncate's
        // publication, and the restart from what the crash left never restores. The first retire
        // at the fourth failure, and the one after a filtered sparse departure's failed frontier
        // seal, meet a lock on _timeline: each keeps the timeline and its live marker, and the
        // restart rebuilds over the marker. The fourth batch draws a lock for its first retire at
        // the fourth failure and none for its second, and the fifth none for the retire after its
        // filtered sparse departure's failed frontier seal: each unlocked retire removes the
        // timeline and its marker, and the restatement guard refuses the restart's rebuild over
        // the day the run dropped, as a rebuild over an absent timeline.
        final long[][] seeds = {
                {2277450534470274333L, 984054158561696L},
                {8920866532787660373L, 984089867478109L},
                {1611528458713692810L, 984110371927461L},
                {99928471246783L, 1794659701302L},
                {101175290116121L, 1794660948121L}
        };
        for (long[] seed : seeds) {
            setCurrentMicros(-1);
            runRangeFrameReplaceRangeFuzz(TestUtils.generateRandom(LOG, seed[0], seed[1]));
        }
        assertReachedAnOwedSealEpisode();
        Assert.assertTrue("no owed-seal episode paused the seal", pausedOwedSealEpisodes > 0);
        Assert.assertTrue("no fault outlasted the pause", retiredOwedSealEpisodes > 0);
        Assert.assertTrue("no clean commit came inside the pause", markerLiveInsideThePauseOwedSealEpisodes > 0);
        Assert.assertTrue("no resumed episode landed its seal after the pause", restoredAfterThePauseResumedOwedSealEpisodes > 0);
        Assert.assertTrue(
                "no head-miss episode landed its seal after the pause",
                restoredAfterThePauseOwedSealEpisodes > restoredAfterThePauseResumedOwedSealEpisodes
        );
        Assert.assertTrue("no sparse departure re-stamped its head inside a pause", restampsInsideThePause > 0);
        Assert.assertTrue("no late row the filter dropped below a departure moved the head", lateRowlessHeadAdvances > 0);
        Assert.assertTrue("no departure's in-order rows moved its head with no late row ahead of them", inOrderOnlyHeadAdvances > 0);
        logFrontierTruncateCrashes();
        Assert.assertTrue("no crash interrupted a sparse departure's frontier truncate", headMissFrontierTruncateCrashes > 0);
        assertReachedAKeptTimelineRetire();
        Assert.assertTrue(
                "no retire after the pause removed the timeline ahead of a rebuild the guard refused",
                blockedTimelineAbsentOwedSealEpisodes > 0
        );
        Assert.assertTrue(
                "no retire after a failed frontier seal removed the timeline ahead of a rebuild the guard refused",
                blockedTimelineAbsentSparseFrontierSeals > 0
        );
    }

    @Test
    public void testFuzzDepartedOwedRootEpisodes() throws Exception {
        // testFuzzDedup's and testFuzzReplaceRange's batches with the owed-root arm on: their
        // filtered views' departures that leave a row above the root their repair anchors on fail
        // the repair's closing seal, on the resume and on the head-miss rebuild, once or as the third
        // failure in a row, and the stretch without rows after each must land the owed root at the
        // view's newest row, which the restart after it restores. See isDepartedOwedRootArm.
        isDepartedOwedRootArm = true;
        final Rnd rnd = TestUtils.generateRandom(LOG);
        runDedupFuzzBatch(rnd);
        runReplaceRangeFuzzVariants(rnd, true);
        assertReachedEveryDepartedOwedRootLanding();
    }

    @Test
    public void testFuzzDepartedOwedRootLandsOnTheFirstStretch() throws Exception {
        // A pinned seed pair whose testFuzzDepartedOwedRootEpisodes dedup batch fails a departure's
        // closing seal once on each route, and lands each owed root on the stretch without rows
        // after it. A generator change that stops producing it fails here.
        isDepartedOwedRootArm = true;
        runDedupFuzzBatch(TestUtils.generateRandom(LOG, DEPARTED_OWED_ROOT_SEED0, DEPARTED_OWED_ROOT_SEED1));
        assertReachedADepartedOwedRootLandingOnTheFirstStretch();
    }

    @Test
    public void testFuzzClearedHeadEpisodes() throws Exception {
        // testFuzzDedup's and testFuzzReplaceRange's batches with the cleared-head arm on: their
        // filtered views' departures whose repair only re-stamps the head on the root it kept at
        // the view's new frontier fail the re-stamp's read of the view's table, on the resume and
        // on the head-miss rebuild, and the stretch without rows after each must put the head back
        // on that root, which the restart after it restores. See isClearedHeadArm.
        isClearedHeadArm = true;
        final Rnd rnd = TestUtils.generateRandom(LOG);
        runDedupFuzzBatch(rnd);
        runReplaceRangeFuzzVariants(rnd, true);
        assertReachedAClearedHeadLanding();
    }

    @Test
    public void testFuzzClearedHeadLandsOnBothRoutes() throws Exception {
        // A pinned seed pair whose testFuzzClearedHeadEpisodes batches leave the head clear over a
        // root at the frontier on each route and put it back on the stretch after it: the head-miss
        // rebuild's keeping shapes reach it in every batch, and the resume's frontier truncate only
        // in a departure that draws no late row, crash or declined chain. A generator change that
        // stops producing it fails here.
        isClearedHeadArm = true;
        final Rnd rnd = TestUtils.generateRandom(LOG, CLEARED_HEAD_SEED0, CLEARED_HEAD_SEED1);
        runDedupFuzzBatch(rnd);
        runReplaceRangeFuzzVariants(rnd, true);
        assertReachedAClearedHeadLanding();
        Assert.assertTrue("no resume's cleared head landed", clearedHeadLandings > clearedHeadHeadMissLandings);
        Assert.assertTrue("no head-miss rebuild's cleared head landed", clearedHeadHeadMissLandings > 0);
    }

    @Test
    public void testFuzzDepartedOwedRootLandsAfterThePause() throws Exception {
        // A pinned seed pair whose testFuzzDepartedOwedRootEpisodes batches fail a departure's
        // closing seal as the third failure in a row, which pauses the seal, and land the owed root
        // on the stretch without rows after the pause. A generator change that stops producing it
        // fails here.
        isDepartedOwedRootArm = true;
        final Rnd rnd = TestUtils.generateRandom(LOG, DEPARTED_OWED_ROOT_PAUSE_SEED0, DEPARTED_OWED_ROOT_PAUSE_SEED1);
        runDedupFuzzBatch(rnd);
        runReplaceRangeFuzzVariants(rnd, true);
        assertReachedADepartedOwedRootLandingAfterThePause();
    }

    @Test
    public void testFuzzReplaceRangeAfterAZeroRowHeadMissSplice() throws Exception {
        // A run whose variant 9, a RANGE '17' MINUTE IN MEMORY view, deletes rows at the end of
        // the base table through a head-miss rebuild that emits no row, with no root a restart
        // could restore at the view's new frontier. The splice that rebuild published once kept a
        // generation no restart could restore, and the restart after the run's departure episode
        // failed its row count and stopped the view over the day the run dropped. The rebuild
        // then retired the timeline; it now derives the state at the frontier and seals it there,
        // and the restart after the departure restores.
        runReplaceRangeFuzzVariants(TestUtils.generateRandom(LOG, 437310277036417L, 1793666395061L), false);
        coverage.assertReachedStat(LiveViewRefreshStats.HEAD_MISS_ZERO_ROW_FRONTIER_SEAL_COUNT);
    }

    @Test
    public void testFuzzReplaceRangeTruncatingAHeadMissSpliceAtItsFrontier() throws Exception {
        // A run whose replace bands reach the head-miss rebuild's end-of-table splice with output
        // ending below a root the splice kept, which the rebuild truncates at the view's new
        // frontier, and with no row emitted and no root at the frontier, which once retired the
        // timeline and now seals the state the rebuild derived at the frontier. A random run
        // reaches either only now and then.
        runReplaceRangeFuzzVariants(TestUtils.generateRandom(LOG, 449697238060352L, 1793678782022L), false);
        coverage.assertReachedStat(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT);
        coverage.assertReachedStat(LiveViewRefreshStats.HEAD_MISS_ZERO_ROW_FRONTIER_SEAL_COUNT);
    }

    @Test
    public void testFuzzReplaceRangeUnsealedDepartureAfterDeletingTheNewestRows() throws Exception {
        // A run whose variant 4 deletes the base's newest rows through a replace band, which
        // leaves the newest timestamp the view has seen hours above every base row, ahead of a
        // departure episode that leaves its departed rows unsealed. Its kept batch lands below
        // that timestamp, and the out-of-order repair that takes it seals it. The departed batch
        // once landed below it too, so a repair sealed that batch as well, and the episode failed.
        runReplaceRangeFuzzVariants(TestUtils.generateRandom(LOG, 483053825472492L, 1793712138610L), true);
        Assert.assertTrue(
                "no unsealed departure had its kept batch below the newest timestamp the view had seen",
                unsealedDeparturesBelowLatestSeenTs > 0
        );
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
        // variant runs under in-order / O3 and optional SEED.
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
        // the apply block size and look-ahead bound how many base seqTxns one apply cycle folds
        // into a base-table commit, the apply time quota and commit-to-table lag
        // (size / txn count) cap how much buffers before a base-table commit, and
        // the O3 partition-split knobs reshape the physical partition layout the
        // refresh reads back. A single random config is pinned per run and every
        // fixed-width variant is driven under O3 with optional restart / seed /
        // IN MEMORY; the quiescent view must still equal the from-scratch recompute
        // regardless of how the base commits were batched underneath it.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        setProperty(PropertyKey.CAIRO_WAL_APPLY_LOOK_AHEAD_TXN_COUNT, 1 + rnd.nextInt(64));
        setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 1 + rnd.nextInt(250));
        setProperty(PropertyKey.CAIRO_WAL_MAX_LAG_TXN_COUNT, 1 + rnd.nextInt(20));
        setProperty(PropertyKey.CAIRO_WAL_MAX_LAG_SIZE, rnd.nextLong(10L * 1024 * 1024));
        setProperty(PropertyKey.CAIRO_WAL_APPLY_BLOCK_SIZE, AbstractFuzzTest.getRndWalApplyBlockSize(rnd));
        setProperty(PropertyKey.CAIRO_WAL_APPLY_BLOCK_MAX_ROWS, AbstractFuzzTest.getRndWalApplyBlockMaxRows(rnd));
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
        // variant runs under O3 with optional restart / seed / IN MEMORY. The
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
        // in-order, O3, and O3 + restart - each with random seed and a fresh
        // random dataset, so the var-length (data, aux) write/read paths, the flush
        // flyweight, and the O3 disk-stager rebuild are all exercised. After
        // quiescence each run cross-checks three ways: the read-back equals the
        // from-scratch recompute, Mode B is confirmed engaged, and the Mode B
        // result is byte-identical to the forced disk-only path.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(() -> {
            final boolean seed = rnd.nextBoolean();
            runVarSizeFuzz(rnd, 120 + rnd.nextInt(160), false, false, seed);
            runVarSizeFuzz(rnd, 120 + rnd.nextInt(160), true, false, seed);
            runVarSizeFuzz(rnd, 120 + rnd.nextInt(160), true, true, seed);
        });
    }

    @Test
    public void testFuzzWidened() throws Exception {
        // Concentrated heavy corner: larger datasets with O3 + restart + seed +
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

    @Test
    public void testRunsUnderTheWalApplySuspensionRule() {
        final LogCapture capture = walApplySuspensionRule.getCapture();
        Assert.assertNotNull("the rule must wrap every test of the class", capture);
        Assert.assertTrue("the rule's capture must hold the console log interceptor", capture.isInstalled());
    }

    // runAnchoredRemovalFuzz's check that no live view code logged a CRITICAL line during a run.
    // The logging worker can lag a stalled stdout by seconds, so it waits for the run's lines up to
    // the bound LogCapture.stop() waits, then reads from the run's marker on: the capture spans the
    // whole test, and the lines of setUp and of earlier runs sit ahead of the marker.
    static void assertNoLiveViewCriticalLineAfter(LogCapture capture, String runMarker) {
        capture.drain(LogCapture.STOP_DRAIN_TIMEOUT_MS);
        capture.assertNotLoggedAfter(runMarker, " C i.q.c.l.");
    }

    // The check of a run over a nanosecond base that every live view line printed its timestamps
    // in nanoseconds. Such a run's dates fall in 2025 to 2026, and the largest nanosecond timestamp
    // in 2262, so a date with a year of five digits or more is a nanosecond value printed as
    // microseconds, some 56,000 years out. Waits for the run's lines and reads from its marker on,
    // as assertNoLiveViewCriticalLineAfter does.
    static void assertNoLiveViewLineReadsNanosAsMicrosAfter(LogCapture capture, String runMarker) {
        capture.drain(LogCapture.STOP_DRAIN_TIMEOUT_MS);
        capture.assertNotLoggedREAfter(runMarker, LIVE_VIEW_LINE_WITH_A_FIVE_DIGIT_YEAR);
    }

    // Logs the marker a run of runAnchoredRemovalFuzz opens with, and returns it for
    // assertNoLiveViewCriticalLineAfter, which reads the run's lines from it on.
    static String logAnchoredRemovalRunMarker() {
        final String runMarker = "anchored removal run " + ANCHORED_REMOVAL_RUN_SEQ.incrementAndGet() + " starts";
        LOG.advisory().$(runMarker).$();
        return runMarker;
    }

    // Logs the marker a run over a nanosecond base opens with, and returns it for
    // assertNoLiveViewLineReadsNanosAsMicrosAfter, which reads the run's lines from it on.
    static String logNanosBaseRunMarker() {
        final String runMarker = "nanosecond base run " + NANOS_BASE_RUN_SEQ.incrementAndGet() + " starts";
        LOG.advisory().$(runMarker).$();
        return runMarker;
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
            case ANCHORED_LAG_VARIANT -> "ts, sym, i, lag(i) OVER (" + part + ") AS v";
            case ANCHORED_COVAR_POP_VARIANT -> "ts, sym, i, x, covar_pop(x, i) OVER (" + part + ") AS v";
            case ANCHORED_EXCLUDE_CURRENT_ROW_AVG_VARIANT ->
                    "ts, sym, x, avg(x) OVER (" + part + " " + anchoredWindowFrame(variant) + ") AS v";
            case ANCHORED_EXCLUDE_CURRENT_ROW_FIRST_VALUE_VARIANT ->
                    "ts, sym, x, first_value(x) OVER (" + part + " " + anchoredWindowFrame(variant) + ") AS v";
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
            case ANCHORED_LAG_VARIANT -> "ts, sym, i, lag(i) OVER w AS v";
            case ANCHORED_COVAR_POP_VARIANT -> "ts, sym, i, x, covar_pop(x, i) OVER w AS v";
            case ANCHORED_EXCLUDE_CURRENT_ROW_AVG_VARIANT -> "ts, sym, x, avg(x) OVER w AS v";
            case ANCHORED_EXCLUDE_CURRENT_ROW_FIRST_VALUE_VARIANT -> "ts, sym, x, first_value(x) OVER w AS v";
            default -> throw new IllegalArgumentException("anchored variant=" + variant);
        };
    }

    // The frame an anchored fuzz variant's named window carries ahead of its ANCHOR clause, with
    // a trailing space, or nothing for the default frame.
    private static String anchoredWindowFrame(int variant) {
        return variant == ANCHORED_EXCLUDE_CURRENT_ROW_AVG_VARIANT || variant == ANCHORED_EXCLUDE_CURRENT_ROW_FIRST_VALUE_VARIANT
                ? "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW "
                : "";
    }

    // Appends one (ts, sym, i, x) row to an INSERT's VALUES list.
    private static void appendEpisodeRow(StringSink values, long ts, int symIdx, long iv, double xv) {
        if (values.length() > 0) {
            values.put(", ");
        }
        values.put('(').put(ts).put("::timestamp, '").put(SYMBOLS[symIdx]).put("', ").put(iv).put(", ").put(xv).put(')');
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

    // Arrives at the current phase of a backfilling run's ticks and waits for every other
    // party to arrive. runConcurrentWriterFuzz runs each tick as two phases: in the first,
    // every writer still running makes its commit for the tick; in the second, the refresh
    // driver drains them while the writers wait. A party that leaves deregisters, and one that
    // fails terminates the phaser, so no party waits on it. Bounded, so a stalled party fails
    // the run instead of hanging it.
    private static void arriveAndAwaitTick(Phaser ticks) throws InterruptedException, TimeoutException {
        ticks.awaitAdvanceInterruptibly(ticks.arrive(), REFRESH_TICK_WAIT_NANOS, TimeUnit.NANOSECONDS);
    }

    // Asserts the view has a checkpoint timeline on disk for a restart to restore from.
    // Without one, LiveViewRefreshJob.tryRestoreFromTimeline rebuilds from the applied
    // base instead, so a crash-recovery arm that needs the restore path checks this
    // precondition up front rather than failing later on a symptom.
    private static void assertCheckpointTimelineExists() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        try (Path checkpointsDir = new Path(); Path timelinePath = new Path()) {
            checkpointsDir.of(configuration.getDbRoot())
                    .concat(instance.getLiveViewToken())
                    .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
            LiveViewCheckpointLayout.timelinePath(timelinePath, checkpointsDir);
            Assert.assertTrue("the checkpoint timeline must exist on disk: " + timelinePath,
                    configuration.getFilesFacade().exists(timelinePath.$()));
        }
    }

    private static void assertFirstReasonStands(int op, String firstReason, String secondReason, CharSequence actual) {
        Assert.assertTrue(
                "the first invalidation's reason must stand [op=" + op + ", second=" + secondReason + ", actual=" + actual + ']',
                Chars.contains(actual, firstReason) && !Chars.contains(actual, secondReason)
        );
    }

    // Asserts the lead-ordering invariant on a view's published slot: no un-flushed
    // lead row may carry a timestamp BELOW the on-disk maximum, i.e.
    // min(lead ts) >= max(disk ts). Returns true if the compare actually ran, so a
    // caller can prove the check is not vacuous.
    //
    // The bound is NON-strict - equality is reachable and by design. An additive
    // commit whose min ts equals the frontier is not diverted to O3 (the
    // cross-commit compare in LiveViewRefreshJob's drain is a strict
    // txnMinTs < latestSeen), so its rows append into the lead at exactly the
    // on-disk max ts. Asserting a strict > here would fail on that legitimate tie;
    // LiveViewRecordCursor.hasNext's leadStart == 0 branch exists for the same
    // reason.
    //
    // Only meaningful while the seqTxn fence holds: leadStart partitions the slot
    // against the slot's OWN lvSeqTxn, so a disk snapshot at a different version
    // says nothing about where this slot's lead begins. Mirrors the production
    // fence in LiveViewRecordCursor.diskReaderSeqTxn (reader.getSeqTxn() vs
    // slot.lvSeqTxn()).
    private static boolean assertLeadAboveDisk(LiveViewInstance instance) {
        final LiveViewInMemoryTier tier = instance.getInMemoryTier();
        if (tier == null) {
            return false;
        }
        final int slotIdx = tier.acquireRead();
        if (slotIdx < 0) {
            return false; // tier closed (DROP / invalidate); nothing published to check
        }
        try {
            final LiveViewInMemoryBuffer slot = tier.getSlot(slotIdx);
            final long leadRowCount = slot.leadRowCount();
            final long slotSeqTxn = slot.lvSeqTxn();
            final int tsCol = slot.getTimestampColumnIndex();
            if (leadRowCount == 0 || slotSeqTxn == Numbers.LONG_NULL || tsCol < 0) {
                // No lead resident (an O3 cycle rebuilds the tier off disk and
                // zeroes it; a flush cycle empties it), an unstamped slot the fence
                // can never engage on, or a ts-less output schema.
                return false;
            }
            try (TableReader reader = engine.getReader(instance.getLiveViewToken())) {
                if (reader.getSeqTxn() != slotSeqTxn || reader.size() == 0) {
                    // Fence off (the slot and this snapshot are different LV
                    // versions), or disk is empty so the lead sits above nothing.
                    return false;
                }
                final long diskMaxTs = reader.getMaxTimestamp();
                final long leadMinTs = slot.getLong(slot.rowCount() - leadRowCount, tsCol);
                if (leadMinTs < diskMaxTs) {
                    Assert.fail("lead ts below disk max for view " + instance.getLiveViewToken().getTableName()
                            + ": leadMinTs=" + leadMinTs + ", diskMaxTs=" + diskMaxTs
                            + ", lvSeqTxn=" + slotSeqTxn + ", rowCount=" + slot.rowCount()
                            + ", leadRowCount=" + leadRowCount);
                }
                return true;
            }
        } finally {
            tier.releaseRead(slotIdx);
        }
    }

    // Mode A read-back cross-check: with a known un-flushed lead resident, the
    // tier-on read must serve exactly the lead and equal the from-scratch
    // recompute, while the forced disk-only fallback serves only the applied
    // prefix (the recompute with the trailing lead rows trimmed). All three sides
    // share the native ts-ascending order, so the comparison is byte-for-byte.
    // Run single-threaded after the worker is freed and the lead is built.
    private static void assertLeadReadBack(String oracleSql) throws SqlException {
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
        printSql(oracleSql, recompute);
        Assert.assertEquals("Mode A read must equal the recompute", recompute.toString(), lvOut.toString());

        // Disk-only fallback content equals the applied prefix (recompute minus the lead).
        StringSink diskOnly = new StringSink();
        printDiskOnly("lv", "SELECT * FROM lv", diskOnly);
        Assert.assertEquals("disk-only read must equal the applied prefix",
                dropTrailingDataRows(recompute.toString(), leadRows), diskOnly.toString());
    }

    // The symbol checks a resident lead must pass: each key under one label through the view's
    // own symbol table, and a keyed WINDOW JOIN from the view that matches the same join from
    // a plain copy of it. The view projects (ts, sym, i, rn).
    private static void assertLeadSymbols(int symCount) throws SqlException {
        final ObjList<String> keys = new ObjList<>();
        for (int k = 0; k < symCount; k++) {
            keys.add(SYMBOLS[k]);
        }
        assertSymbolLabelsConsistent("lv", "sym", keys);
        assertViewWindowJoinMatchesCopy("lv", "base", "sym", "i", "t.rn > 0");
    }

    // Waits until every streaming writer still running has committed in the current tick of
    // runConcurrentWriterFuzz. Each commits once per tick, so the wait ends inside the tick;
    // the bound fails a stalled stream instead of hanging the run.
    private static void awaitTickStreamCommits(
            AtomicInteger tickStreamCommits,
            AtomicInteger streamingWritersDone,
            int numStreamingWriters,
            ConcurrentLinkedQueue<Throwable> errors
    ) {
        final long deadlineNanos = System.nanoTime() + REFRESH_TICK_WAIT_NANOS;
        while (tickStreamCommits.get() < numStreamingWriters - streamingWritersDone.get() && errors.isEmpty()) {
            if (System.nanoTime() >= deadlineNanos) {
                throw new AssertionError("the streaming writers did not commit within "
                        + (REFRESH_TICK_WAIT_NANOS / 1_000_000_000L) + 's');
            }
            Os.pause();
        }
    }

    /**
     * Leaves the checkpoint cadence to the clock for departNewestRowsOverSparseRoots: no row
     * count, and no learned duration shorter than the configured one, seals a root, so one
     * seals only once the clock moves past the default duration, as the server's default row
     * count leaves the roots of a view with few rows. Returns the row cadence it replaced, for
     * {@link #endSparseCheckpointCadence}.
     */
    private static long beginSparseCheckpointCadence() {
        final long rowsCadence = engine.getConfiguration().getLiveViewCheckpointRows();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ADAPTIVE_CADENCE_ENABLED, "false");
        return rowsCadence;
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

    // Draws four rows a millisecond apart from fromTs, on random keys and with values the run's
    // filter keeps, onto an INSERT's VALUES list. Fills tsOut, and symOut when not null, with
    // what it drew.
    private static void drawEpisodeBatch(
            Rnd rnd,
            StringSink values,
            long fromTs,
            @Nullable LongHashSet usedTs,
            int symCount,
            long[] tsOut,
            @Nullable int[] symOut
    ) {
        long t = fromTs;
        for (int k = 0; k < tsOut.length; k++, t += 1_000) {
            while (usedTs != null && !usedTs.add(t)) {
                t++;
            }
            final int sym = rnd.nextInt(symCount);
            tsOut[k] = t;
            if (symOut != null) {
                symOut[k] = sym;
            }
            final long i = 1 + rnd.nextInt(1000);
            appendEpisodeRow(values, t, sym, i, rnd.nextDouble() * 1000.0);
        }
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

    // Puts back the cadence beginSparseCheckpointCadence replaced.
    private static void endSparseCheckpointCadence(long rowsCadence) {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, rowsCadence);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ADAPTIVE_CADENCE_ENABLED, (String) null);
    }

    // The owed-seal episode's own random sequence, drawn from the run's without advancing it.
    private static Rnd owedSealEpisodeRnd(Rnd rnd) {
        return new Rnd(rnd.getSeed0() ^ OWED_SEAL_EPISODE_SALT0, rnd.getSeed1() ^ OWED_SEAL_EPISODE_SALT1);
    }

    // The sequence an episode draws a lock on _timeline for a retire from, drawn from the episode's
    // without advancing it.
    private static Rnd keptTimelineRetireRnd(Rnd rnd) {
        return new Rnd(rnd.getSeed0() ^ KEPT_TIMELINE_RETIRE_SALT0, rnd.getSeed1() ^ KEPT_TIMELINE_RETIRE_SALT1);
    }

    // The sequence a departure draws the fault on its kept-timeline episode's check of _timeline
    // from, drawn from the run's without advancing it.
    private static Rnd keptTimelineCheckRnd(Rnd rnd) {
        return new Rnd(rnd.getSeed0() ^ KEPT_TIMELINE_CHECK_SALT0, rnd.getSeed1() ^ KEPT_TIMELINE_CHECK_SALT1);
    }

    // The sequence a departure draws its kept-timeline episode from, drawn from the run's without
    // advancing it.
    private static Rnd keptTimelineEpisodeRnd(Rnd rnd) {
        return new Rnd(rnd.getSeed0() ^ KEPT_TIMELINE_EPISODE_SALT0, rnd.getSeed1() ^ KEPT_TIMELINE_EPISODE_SALT1);
    }

    // The sequence a kept-timeline episode draws its lock on the repair marker from, drawn from
    // the episode's without advancing it.
    private static Rnd keptTimelineMarkerRnd(Rnd episodeRnd) {
        return new Rnd(episodeRnd.getSeed0() ^ KEPT_TIMELINE_MARKER_SALT0, episodeRnd.getSeed1() ^ KEPT_TIMELINE_MARKER_SALT1);
    }

    // The sequence a run draws a leftover repair marker beside its seed from, drawn from the run's
    // without advancing it.
    private static Rnd seedMarkerRnd(Rnd rnd) {
        return new Rnd(rnd.getSeed0() ^ SEED_MARKER_SALT0, rnd.getSeed1() ^ SEED_MARKER_SALT1);
    }

    // The sequence a departure draws a crash at its frontier truncate from, drawn from the run's
    // without advancing it.
    private static Rnd frontierTruncateCrashRnd(Rnd rnd) {
        return new Rnd(rnd.getSeed0() ^ FRONTIER_TRUNCATE_CRASH_SALT0, rnd.getSeed1() ^ FRONTIER_TRUNCATE_CRASH_SALT1);
    }

    private static Path frontierTruncateCrashImageDir() {
        return new Path().of(configuration.getDbRoot()).concat("frontier_truncate_crash_image").slash();
    }

    // The sequence a restart of runDedupFuzz draws a shutdown that cancels a drift recovery ahead of it
    // from, drawn from the run's without advancing it.
    private static Rnd driftRecoveryCancellationRnd(Rnd rnd) {
        return new Rnd(rnd.getSeed0() ^ DRIFT_RECOVERY_CANCELLATION_SALT0, rnd.getSeed1() ^ DRIFT_RECOVERY_CANCELLATION_SALT1);
    }

    // The sequence a restart of runFuzz draws a shutdown that cancels a lead drain ahead of it, and the
    // episode's rows, from, drawn from the run's without advancing it.
    private static Rnd shutdownMidDrainRnd(Rnd rnd) {
        return new Rnd(rnd.getSeed0() ^ SHUTDOWN_MID_DRAIN_SALT0, rnd.getSeed1() ^ SHUTDOWN_MID_DRAIN_SALT1);
    }

    // The sequence a run draws which of its two frontier-sealing departures restores a root inside
    // the new frontier's frame from, drawn from the run's without advancing it.
    private static Rnd frontierStateRouteRnd(Rnd rnd) {
        return new Rnd(rnd.getSeed0() ^ FRONTIER_STATE_ROUTE_SALT0, rnd.getSeed1() ^ FRONTIER_STATE_ROUTE_SALT1);
    }

    // The sequence a sparse departure draws its seal pause from, drawn from the run's without
    // advancing it.
    private static Rnd sparseDeparturePauseRnd(Rnd rnd) {
        return new Rnd(rnd.getSeed0() ^ SPARSE_DEPARTURE_PAUSE_SALT0, rnd.getSeed1() ^ SPARSE_DEPARTURE_PAUSE_SALT1);
    }

    // The sequence the chain price arm draws a run's knobs and its new-key episode from, drawn
    // from the run's without advancing it.
    private static Rnd chainPriceRnd(Rnd rnd) {
        return new Rnd(rnd.getSeed0() ^ CHAIN_PRICE_SALT0, rnd.getSeed1() ^ CHAIN_PRICE_SALT1);
    }

    // The sequence a departure above a rowless anchor draws its rows from, drawn from the run's
    // without advancing it.
    private static Rnd rowlessAnchorEpisodeRnd(Rnd rnd) {
        return new Rnd(rnd.getSeed0() ^ ROWLESS_ANCHOR_EPISODE_SALT0, rnd.getSeed1() ^ ROWLESS_ANCHOR_EPISODE_SALT1);
    }

    // Whether the variant's window has a RANGE frame, which a localized rebuild can run to the end
    // of the base table behind: a ROWS frame needs a finite convergence bound.
    private static boolean isRangeFrameVariant(int variant) {
        return variant == RANGE_SUM_VARIANT || variant == RANGE_AVG_VARIANT || variant == RANGE_FIRST_VALUE_VARIANT;
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

    // The first row at or above from that isHeldBack marks, or rowCount when none is left.
    private static int nextHeldBackRow(boolean[] isHeldBack, int from, int rowCount) {
        int k = from;
        while (k < rowCount && !isHeldBack[k]) {
            k++;
        }
        return k;
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
            case 5 ->
                    "ts, sym, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn";
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
            case FIRST_VALUE_IGNORE_NULLS_VARIANT ->
                    "ts, sym, i, first_value(i) IGNORE NULLS OVER (" + frame + ") AS v";
            default -> throw new IllegalArgumentException("variant=" + variant);
        };
    }

    // Builds the emission order for runLeadOrderingFuzz and fills boundsOut with the
    // commits+1 offsets splitting it into commits. Each commit's rows are ascending
    // except on the LATE_COMMIT_PHASE cycles, which also carry rows held back from
    // earlier chunks - genuine below-frontier arrivals.
    //
    // The phase assignment is deliberate, not drawn. segmentOrder(o3=true) is the
    // wrong generator here: a full shuffle puts nearly every commit below the
    // frontier, so nearly every cycle diverts to O3 replay, which rebuilds the tier
    // off disk and zeroes the lead - leaving nothing to compare on any cycle. Row-
    // level randomness has the same failure mode more subtly: on an unlucky seed
    // every pinned commit draws a late row and the arm silently goes vacuous. So the
    // phase within each 4-cycle group is fixed (see runLeadOrderingFuzz): the
    // in-order pinned cycles guarantee a resident lead to check, and the late commit
    // lands ON that lead.
    private static int[] lateRowEmissionOrder(Rnd rnd, int rowCount, int commits, IntList boundsOut) {
        final int[] order = new int[rowCount];
        final IntList deferred = new IntList();
        int emitted = 0;
        boundsOut.clear();
        boundsOut.add(0);
        for (int c = 0; c < commits; c++) {
            final int from = (int) ((long) c * rowCount / commits);
            final int to = (int) ((long) (c + 1) * rowCount / commits);
            for (int k = from; k < to; k++) {
                // Hold back a minority of rows, but never from the last group - they
                // would have no later commit to arrive on.
                if (c < commits - 1 && rnd.nextInt(8) == 0) {
                    deferred.add(k);
                    continue;
                }
                order[emitted++] = k;
            }
            if (c % CYCLES_PER_GROUP == LATE_COMMIT_PHASE) {
                // Release every row held back so far: each sits below the frontier
                // the preceding in-order commits advanced, so this commit is O3 both
                // across commits and (since they trail this chunk's own ascending
                // rows) within itself.
                for (int i = 0, n = deferred.size(); i < n; i++) {
                    order[emitted++] = deferred.getQuick(i);
                }
                deferred.clear();
            }
            boundsOut.add(emitted);
        }
        // Anything still held back (the tail group defers nothing, but a short run
        // can leave the last release empty) trails the final commit.
        for (int i = 0, n = deferred.size(); i < n; i++) {
            order[emitted++] = deferred.getQuick(i);
        }
        boundsOut.setQuick(commits, emitted);
        Assert.assertEquals(rowCount, emitted);
        return order;
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
    // The oldest day of the base's rows when every row up to the end of that day is committed and no row still
    // to come falls on it, so a DROP PARTITION of it leaves nothing for a later commit to repair below;
    // Numbers.LONG_NULL otherwise.
    private static long removableDay(long[] tsv, boolean[] committed) {
        int prefix = 0;
        while (prefix < tsv.length && committed[prefix]) {
            prefix++;
        }
        if (prefix == 0) {
            return Numbers.LONG_NULL;
        }
        final long day = tsv[0] - tsv[0] % Micros.DAY_MICROS;
        if (prefix < tsv.length && tsv[prefix] - tsv[prefix] % Micros.DAY_MICROS <= day) {
            return Numbers.LONG_NULL;
        }
        return day;
    }

    private static void removeTimelineIfAny(LiveViewInstance instance) {
        try (
                Path dir = new Path().of(configuration.getDbRoot())
                        .concat(instance.getLiveViewToken())
                        .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
                Path timelinePath = new Path()
        ) {
            LiveViewCheckpointLayout.timelinePath(timelinePath, dir);
            configuration.getFilesFacade().removeQuiet(timelinePath.$());
        }
    }

    // The INSERT into base that sink holds, made an INSERT into shadow.
    private static String shadowInsert(StringSink sink) {
        return Chars.toString(sink).replace("INSERT INTO base (", "INSERT INTO shadow (");
    }

    // What ALTER LIVE VIEW lv SUSPEND WAL runs, once, right before the next commit to a live view's WAL.
    private static AtomicBoolean suspendAtNextLiveViewCommit() {
        final TableToken token = engine.verifyTableName("lv");
        final AtomicBoolean isArmed = new AtomicBoolean(true);
        CairoEngine.setRoleSwitchMintObserver(() -> {
            if (isArmed.compareAndSet(true, false)) {
                engine.addWalApplySuspended(token);
                engine.getTableSequencerAPI().suspendTable(token, ErrorTag.DISK_FULL, "fuzz");
            }
        });
        return isArmed;
    }

    private static int countPauses(LogCapture capture) {
        capture.drain();
        return capture.countOccurrences(SUSPENSION_PAUSE_LINE);
    }

    // ALTER LIVE VIEW lv SUSPEND WAL, with or without an operator tag and message.
    private static void suspendLiveView(Rnd rnd) {
        try {
            execute(rnd.nextBoolean() ? "ALTER LIVE VIEW lv SUSPEND WAL" : "ALTER LIVE VIEW lv SUSPEND WAL WITH 'DISK FULL', 'fuzz'");
        } catch (SqlException e) {
            throw new AssertionError(e);
        }
    }

    private static int[] segmentOrder(Rnd rnd, int lo, int hi, boolean o3) {
        return segmentOrder(rnd, lo, hi, o3, 0);
    }

    // As above, but maxLateness bounds how far a row may travel from its ts
    // position. The default (0) shuffles the whole segment, which spreads every
    // commit across the entire time range: a commit's minTs then sits at the
    // bottom of the data, so its retire unseals every retained checkpoint and
    // the ring never holds more than the entry the cycle itself writes. A
    // positive maxLateness displaces a row by at most that many positions, so a
    // commit's minTs stays near the ingestion frontier and the retire unseals
    // only the newest few entries - which is what lets the ring accumulate while
    // late rows still arrive to resume from an older entry of it.
    private static int[] segmentOrder(Rnd rnd, int lo, int hi, boolean o3, int maxLateness) {
        final int[] a = new int[hi - lo];
        for (int k = 0; k < a.length; k++) {
            a[k] = lo + k;
        }
        if (o3 && maxLateness <= 0) {
            for (int k = a.length - 1; k > 0; k--) {
                int j = rnd.nextInt(k + 1);
                int tmp = a[k];
                a[k] = a[j];
                a[j] = tmp;
            }
        } else if (o3) {
            for (int k = 0; k < a.length - 1; k++) {
                if (rnd.nextInt(4) == 0) {
                    final int j = k + 1 + rnd.nextInt(Math.min(maxLateness, a.length - k - 1));
                    final int tmp = a[k];
                    a[k] = a[j];
                    a[j] = tmp;
                }
            }
        }
        return a;
    }

    // The event-time boundary the run's start mode resolves to at CREATE, in the base's
    // timestamp units. This is the value the view persists as its whole membership rule, and
    // the value the recompute oracle folds into its WHERE.
    //
    // NOW reads the test clock, which every arm pins a day below the generated data and only
    // ever advances by refresh cycles, so nothing between here and the CREATE moves it - the
    // clock read here IS the boundary CREATE resolves (asserted by assertPersistedStartBound).
    // An explicit literal cuts at a random row, endpoints included: cut 0 lands the boundary
    // exactly ON the first row (which is inclusive, so the whole dataset still qualifies) and
    // cut == rowCount lands it one tick above the last, so NO base row qualifies and the view
    // seeds and drains empty.
    private static long startBoundary(Rnd rnd, int startMode, long[] tsv, boolean nanosBase) {
        return startBoundary(rnd, startMode, tsv, nanosBase, tsv.length);
    }

    // As above, but maxCut caps how many of the leading rows an explicit boundary may exclude.
    // An arm whose assertion needs the view to be NON-EMPTY - one that snapshots the view's
    // output and compares it back, or walks a gapless row_number() - passes a limit below
    // rowCount, so a cut can thin the view but never empty it and leave the assertion vacuous.
    private static long startBoundary(Rnd rnd, int startMode, long[] tsv, boolean nanosBase, int maxCut) {
        return switch (startMode) {
            case START_FROM_BEGINNING -> Numbers.LONG_NULL;
            case START_FROM_NOW -> nanosBase ? currentMicros * 1000 : currentMicros;
            case START_FROM_TIMESTAMP -> {
                final int cut = rnd.nextInt(maxCut + 1);
                yield cut < tsv.length ? tsv[cut] : tsv[tsv.length - 1] + 1;
            }
            default -> throw new IllegalArgumentException("startMode=" + startMode);
        };
    }

    // The START FROM clause the run's view declares. BEGINNING and NOW are syntax; an explicit
    // boundary renders as a quoted literal in the base's own precision, which is the precision
    // CREATE parses it back with - so a NANO base keeps its sub-microsecond digits.
    private static String startFromClause(int startMode, long boundary, boolean nanosBase) {
        final StringSink sink = new StringSink();
        sink.put("START FROM ");
        switch (startMode) {
            case START_FROM_BEGINNING -> sink.put("BEGINNING");
            case START_FROM_NOW -> sink.put("NOW");
            case START_FROM_TIMESTAMP -> {
                sink.put('\'');
                if (nanosBase) {
                    NanosFormatUtils.appendDateTimeNSec(sink, boundary);
                } else {
                    MicrosFormatUtils.appendDateTimeUSec(sink, boundary);
                }
                sink.put('\'');
            }
            default -> throw new IllegalArgumentException("startMode=" + startMode);
        }
        return sink.put(' ').toString();
    }

    // A run's share of rows that tie the previous row's timestamp under another key: none in
    // one run of three, so a batch still runs on unique timestamps too.
    private static int tiePct(Rnd rnd) {
        return rnd.nextInt(3) == 0 ? 0 : 10 + rnd.nextInt(40);
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

    // The WHERE tail shared by a run's view SELECT and its recompute oracle. The two differ in
    // exactly one term: the boundary.
    //
    // The VIEW passes LONG_NULL, because it must derive its lower bound from the START FROM
    // clause rather than restate it in the SELECT - restating it would test the WHERE, not the
    // membership rule. The ORACLE passes the resolved boundary, because a live view holds
    // exactly the rows of its SELECT whose designated timestamp also clears that bound, and its
    // window functions accumulate over that subset alone: a below-bound row must not enter the
    // frame, must not consume a row_number(), and must not survive a replay. A BEGINNING run
    // resolves to LONG_NULL, so its oracle is the view's SELECT unchanged.
    private static String whereTail(boolean withWhere, long boundary) {
        if (boundary == Numbers.LONG_NULL) {
            return withWhere ? " WHERE i > 0" : "";
        }
        return " WHERE ts >= " + boundary + (withWhere ? " AND i > 0" : "");
    }

    // The oracle of a run that ended on departNewestRows: the recompute over the base, or, when
    // dropTheOldestDayTheViewHolds dropped a day, the view's own rows from before the drop.
    private void assertDepartureOracle(
            String oracleOrderedSql,
            String viewOrderedSql,
            @Nullable String rowsBeforeTheDrop
    ) throws Exception {
        if (rowsBeforeTheDrop == null) {
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracleOrderedSql, viewOrderedSql, LOG, true);
        } else {
            final StringSink rows = new StringSink();
            printSql(viewOrderedSql, rows);
            TestUtils.assertEquals("the view must keep the rows of the day its base dropped", rowsBeforeTheDrop, rows);
        }
    }

    // The one call an anchored EXCLUDE CURRENT ROW run's ANCHOR window dispatches to is a ring,
    // which keeps no checkpoint dirty set and claims none: a seal walks a ring's whole map
    // whatever a set names. A view with no compiled plan yet has dispatched no row, so holds none.
    private void assertDispatchedRingKeepsNoCheckpointDirtySet() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        final LiveViewCompiledPlan plan = instance.getCompiledPlan();
        if (plan == null) {
            return;
        }
        final ObjList<WindowFunction> dispatched = plan.getWindowFactory().getAnchorableWindowFunctions();
        Assert.assertNotNull("the anchored window must dispatch to its call", dispatched);
        Assert.assertEquals(1, dispatched.size());
        final WindowFunction ring = dispatched.getQuick(0);
        Assert.assertTrue(ring.getName() + " over the EXCLUDE CURRENT ROW frame must be a ring", ring.supportsCheckpointRingState());
        Assert.assertNull(ring.getName() + " is a ring, which keeps no checkpoint dirty set", ring.getCheckpointDirtyPartitionMap());
        Assert.assertFalse(ring.getName() + " is a ring, which claims no checkpoint dirty set", ring.isCheckpointDirtyTracked());
    }

    // Every outcome of the head-miss rebuild's end-of-table splice, which a batch's RANGE-frame runs
    // reach one each through departNewestRowsOverSparseRoots, and both routes to the state at the
    // new frontier, which its frontier-sealing run's two departures take one each.
    private void assertReachedEveryHeadMissSpliceOutcome() {
        coverage.assertReachedStat(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT);
        coverage.assertReachedStat(LiveViewRefreshStats.HEAD_MISS_SPLICE_ZERO_ROW_KEEP_COUNT);
        coverage.assertReachedStat(LiveViewRefreshStats.HEAD_MISS_ZERO_ROW_FRONTIER_SEAL_COUNT);
        coverage.assertReachedStat(LiveViewRefreshStats.HEAD_MISS_FRONTIER_STATE_RESTORE_COUNT);
        coverage.assertReachedStat(LiveViewRefreshStats.HEAD_MISS_FRONTIER_STATE_WARM_UP_COUNT);
    }

    // A departure that leaves a head below the newest timestamp the view has seen, which every
    // batch's departNewestRows runs and its truncating and keeping sparse departures leave, then
    // the stretch of cycles without rows after it moved that head's base coordinate. One of them
    // re-stamped the head on the root at the new frontier because its rebuild emitted no row,
    // which every batch's keeping sparse departure does. One of them left its departed rows
    // unsealed above the head, which every batch's departNewestRows does one episode in three.
    // The batch's frontier-sealing sparse departure is followed by one whose closing seal fails,
    // which retires the timeline and leaves no head, and the seal declined the first root the
    // stretch after it asked for above the view's newest row: see departOverSparseRootsFailingTheFrontierSeal.
    private void assertReachedEveryRowlessStretchOutcome() {
        Assert.assertTrue("no stretch without rows moved the head a departure left", departureHeadAdvances > 0);
        Assert.assertTrue("no stretch without rows moved the head a departure re-stamped without rows", restampedHeadAdvances > 0);
        Assert.assertTrue("no late row the filter dropped below a departure moved the head", lateRowlessHeadAdvances > 0);
        Assert.assertTrue("no departure left its departed rows unsealed above the head", unsealedDepartures > 0);
        coverage.assertReachedStat(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT);
    }

    // An owed root that a departure's failed closing seal left, which the stretch after it landed,
    // on each route, once with the seal's first failure and once with its third, which paused the
    // seal: see isDepartedOwedRootArm.
    // A head that a departure's declined re-stamp left clear over the root at the view's new
    // frontier, which the stretch after it put back, and no such head the stretch after it did not
    // check: see isClearedHeadArm. Which route the departures that reach it take turns on the draws,
    // so the routes are logged.
    private void assertReachedAClearedHeadLanding() {
        LOG.info().$("LV fuzz cleared heads [episodes=").$(clearedHeadEpisodes)
                .$(", headMissEpisodes=").$(clearedHeadHeadMissEpisodes)
                .$(", landings=").$(clearedHeadLandings)
                .$(", headMissLandings=").$(clearedHeadHeadMissLandings).I$();
        Assert.assertTrue("no departure left the head clear", clearedHeadEpisodes > 0);
        Assert.assertEquals("every cleared head lands on the stretch after it", clearedHeadEpisodes, clearedHeadLandings);
        Assert.assertEquals(clearedHeadHeadMissEpisodes, clearedHeadHeadMissLandings);
    }

    private void assertReachedEveryDepartedOwedRootLanding() {
        assertReachedADepartedOwedRootLandingOnTheFirstStretch();
        assertReachedADepartedOwedRootLandingAfterThePause();
    }

    private void assertReachedADepartedOwedRootLandingAfterThePause() {
        logDepartedOwedRootLandings();
        Assert.assertTrue(
                "no owed root of a resume landed after the pause",
                departedOwedRootLandingsAfterThePause > departedOwedRootHeadMissLandingsAfterThePause
        );
        Assert.assertTrue("no owed root of a head-miss rebuild landed after the pause", departedOwedRootHeadMissLandingsAfterThePause > 0);
    }

    private void assertReachedADepartedOwedRootLandingOnTheFirstStretch() {
        logDepartedOwedRootLandings();
        final int headMissFirstStretchLandings = departedOwedRootHeadMissLandings - departedOwedRootHeadMissLandingsAfterThePause;
        Assert.assertTrue(
                "no owed root of a resume landed on the first stretch",
                departedOwedRootLandings - departedOwedRootLandingsAfterThePause > headMissFirstStretchLandings
        );
        Assert.assertTrue("no owed root of a head-miss rebuild landed on the first stretch", headMissFirstStretchLandings > 0);
    }

    private void logDepartedOwedRootLandings() {
        LOG.info().$("LV fuzz departed owed roots [resumeEpisodes=").$(departedOwedRootResumeEpisodes)
                .$(", headMissEpisodes=").$(departedOwedRootHeadMissEpisodes)
                .$(", landings=").$(departedOwedRootLandings)
                .$(", landingsAfterThePause=").$(departedOwedRootLandingsAfterThePause)
                .$(", headMissLandings=").$(departedOwedRootHeadMissLandings)
                .$(", headMissLandingsAfterThePause=").$(departedOwedRootHeadMissLandingsAfterThePause).I$();
    }

    // The route of the restart after departNewestRowsOverSparseRoots: a restore whichever way the
    // departure kept, truncated or sealed the timeline, over any day the base has lost since. A
    // truncating departure whose crash left the image the restart came back from, isCrashed, never
    // restores: see assertTheRestartFromTheFrontierTruncateCrashImage.
    private void assertSparseDepartureRestartRoute(boolean isCrashed, boolean isDayDropped) {
        if (isCrashed) {
            assertTheRestartFromTheFrontierTruncateCrashImage(isDayDropped);
        } else {
            assertRestoredFromTimeline("lv");
        }
    }

    // Every batch crashes the frontier truncate of one of departNewestRows' departures at least: see
    // departNewestRows. The head-miss rebuild's, which the truncating sparse departure reaches one
    // batch in two, and the restarts' outcomes are logged.
    private void assertReachedAResumeFrontierTruncateCrash() {
        logFrontierTruncateCrashes();
        Assert.assertTrue("no crash interrupted a departure's frontier truncate", resumeFrontierTruncateCrashes > 0);
    }

    // One batch of testFuzzDedup: every DEDUP_VARIANTS run over the deduplicating base under O3 and
    // removals, with a random restart and seed, and the RANGE-frame runs' sparse departures taking
    // one shape each, from the first one the batch draws.
    private void runDedupFuzzBatch(Rnd rnd) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        final int firstSparseDepartureShape = rnd.nextInt(SPARSE_DEPARTURE_SHAPES);
        assertMemoryLeak(checkpointFaults, () -> {
            for (int i = 0, rangeFrameRuns = 0; i < DEDUP_VARIANTS.length; i++) {
                final int variant = DEDUP_VARIANTS[i];
                final int sparseDepartureShape = isRangeFrameVariant(variant)
                        ? (firstSparseDepartureShape + rangeFrameRuns++) % SPARSE_DEPARTURE_SHAPES
                        : -1;
                runDedupFuzz(rnd, variant, 120 + rnd.nextInt(160),
                        true, rnd.nextBoolean(), rnd.nextBoolean(), true, null, sparseDepartureShape);
            }
        });
    }

    // The kept-timeline episodes a test ran, and what they reached: see runKeptTimelineEpisode.
    private void logKeptTimelineEpisodes() {
        LOG.info().$("LV fuzz kept-timeline episodes [episodes=").$(keptTimelineEpisodes)
                .$(", checkFaults=").$(keptTimelineCheckFaults)
                .$(", refusedSeals=").$(keptTimelineRefusedSeals)
                .$(", lateRowRepairs=").$(keptTimelineLateRowRepairs)
                .$(", freshHistories=").$(keptTimelineFreshHistories)
                .$(", retiresWithTheMarker=").$(keptTimelineRetiresWithTheMarker)
                .$(", markerLeftovers=").$(keptTimelineMarkerLeftovers)
                .$(", seedRootsBesideAMarker=").$(seedRootsBesideAMarker)
                .$(", refusedHistoryOpenings=").$(refusedHistoryOpenings).I$();
    }

    // A retire that met a lock on _timeline on each arm that draws one: the test's first owed-seal
    // episode whose fault outlasts the pause, and its first filtered sparse departure whose frontier
    // seal fails, each lock theirs. See failTheOwedSealOfALateRow and
    // departOverSparseRootsFailingTheFrontierSeal.
    private void assertReachedAKeptTimelineRetire() {
        logKeptTimelineRetires();
        Assert.assertTrue("no retire after the pause met a lock on _timeline", owedSealKeptTimelineRetires > 0);
        Assert.assertTrue("no retire after a failed frontier seal met a lock on _timeline", sparseFrontierSealKeptTimelineRetires > 0);
    }

    // Both ends of a departure above a rowless anchor, which every batch's departNewestRows runs
    // after every departure no crash struck, in turn: see departNewestRowsAboveARowlessAnchor.
    private void assertReachedEveryRowlessAnchorOutcome() {
        LOG.info().$("LV fuzz departures above a rowless anchor [episodes=").$(rowlessAnchorEpisodes)
                .$(", watermarks=").$(rowlessAnchorWatermarks)
                .$(", frontierSeals=").$(rowlessAnchorFrontierSeals).I$();
        Assert.assertTrue("no departure above a rowless anchor published the root at the frontier", rowlessAnchorWatermarks > 0);
        Assert.assertTrue("no departure above a rowless anchor sealed the frontier", rowlessAnchorFrontierSeals > 0);
    }

    private void logKeptTimelineRetires() {
        LOG.info().$("LV fuzz retires that kept a locked timeline [owedSeal=").$(owedSealKeptTimelineRetires)
                .$(", sparseFrontierSeal=").$(sparseFrontierSealKeptTimelineRetires).I$();
    }

    private void logFrontierTruncateCrashes() {
        LOG.info().$("LV fuzz frontier truncate crashes [resume=").$(resumeFrontierTruncateCrashes)
                .$(", headMiss=").$(headMissFrontierTruncateCrashes)
                .$(", rebuilt=").$(rebuiltFrontierTruncateCrashes)
                .$(", blocked=").$(blockedFrontierTruncateCrashes).I$();
    }

    /**
     * Asserts the restart from the image of a crash between a frontier truncate's marker and its
     * publication. The truncate ran after its repair's replacement commit, so its marker records
     * no seqTxn and reads as live over the splice's generation the crash left: the restart
     * rebuilds from the applied base, and over the day the run dropped, when {@code isDayDropped},
     * the restatement guard refuses the rebuild and the view stops with every row it held. A
     * restore of that generation would stand on roots above the view's frontier.
     */
    private void assertTheRestartFromTheFrontierTruncateCrashImage(boolean isDayDropped) {
        if (!isDayDropped) {
            assertRebuiltFromAppliedBase("lv");
            rebuiltFrontierTruncateCrashes++;
            return;
        }
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        Assert.assertEquals("rebuild_blocked", LiveViewCheckpointRecoveryPhase.routeName(instance.getCheckpointRestoreRoute()));
        Assert.assertTrue(instance.isCheckpointRecoveryBlocked());
        TestUtils.assertContains(
                instance.getCheckpointRecoveryReason(),
                "would drop rows it retains [cause=prefix preservation repair marker present]: "
        );
        blockedFrontierTruncateCrashes++;
    }

    // CREATE must have resolved and persisted exactly the boundary the run's oracle folds into
    // its WHERE. Without this the oracle would silently drift from the view under test: a NOW
    // that read a clock the harness did not pin, or a literal that lost precision on the way
    // through the parser, would leave the recompute checking a different row set - and a view
    // that agreed with the drifted boundary would still pass.
    private void assertPersistedStartBound(String viewName, long boundary) {
        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(viewName);
        Assert.assertNotNull("live view '" + viewName + "' is not registered", instance);
        Assert.assertEquals(
                "the view's persisted START FROM boundary must be the one the oracle recomputes with",
                boundary,
                instance.getDefinition().getViewLowerBoundTimestamp()
        );
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

    /**
     * Commits two batches of four rows above every base row, each refreshed on its own, so each
     * seals a root at any row cadence the batch draws, then moves the newer batch out of the
     * view: the shape after which a restart once could not restore from the timeline, because
     * the splice that repairs the departure kept the departed rows' root above the view's new
     * frontier. The rows leave through a REPLACE_RANGE pure delete, or, over a filtered view
     * whose base has dedup keys, through an upsert of each of them to a value the filter rejects.
     * Either way the root below them is the anchor, and resuming from it reads no more rows than
     * a localized rebuild would. One run in three adds a late row the filter keeps between the
     * two batches in the same commit, and one in three in the next commit. One episode in three,
     * when {@code isUnsealedDepartureAllowed}, commits the departed batch inside the cadence and
     * above the newest timestamp the view has seen instead, so the resume anchors on the head's
     * own root and emits nothing, and adds no late row. {@code usedTs} is the run's pool of taken
     * timestamps, or null on a base whose dedup keys keep every row unique. When
     * {@code isFrontierTruncateCrashAllowed}, the departure crashes between the resume's frontier
     * truncate's marker and the truncate's publication, until one departure of the batch has, and
     * one departure in three after that: see runUnderTheFrontierTruncateCrash. When
     * {@code isRowlessAnchorEpisodeAllowed}, a departure no crash struck is followed by
     * {@link #departNewestRowsAboveARowlessAnchor}. While {@link #isKeptTimelineEpisodeArm}
     * holds, a departure of a filtered view that draws neither the crash nor a declined chain may
     * fail its frontier truncate's publication under a lock on {@code _timeline} instead: see
     * {@link #runKeptTimelineEpisode}, which the oracle the caller runs over the view,
     * {@code oracleOrderedSql} against {@code viewOrderedSql}, checks after each step.
     *
     * @return true when rows departed, and the caller restarts
     */
    private boolean departNewestRows(
            Rnd rnd,
            LiveViewRefreshJob job,
            TableToken baseToken,
            @Nullable LongHashSet usedTs,
            int symCount,
            boolean withWhere,
            boolean isUnsealedDepartureAllowed,
            boolean isFrontierTruncateCrashAllowed,
            boolean isRowlessAnchorEpisodeAllowed,
            String oracleOrderedSql,
            String viewOrderedSql
    ) throws Exception {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        final long baseMaxTs = queryTimestamp("SELECT max(ts) FROM base");
        if (instance == null
                || instance.getStateReader().getSeedState() != LiveViewState.SEED_STATE_ACTIVE
                || baseMaxTs == Numbers.LONG_NULL) {
            return false;
        }
        final long[] keptTs = new long[4];
        // Above the view's START FROM boundary too, which the run's replace bands can leave above
        // every base row: rows below it never reach the view.
        final long fromTs = Math.max(baseMaxTs, instance.getDefinition().getViewLowerBoundTimestamp());
        commitEpisodeBatch(rnd, fromTs + 1 + rnd.nextInt(1_000_000), usedTs, symCount, keptTs, null);
        drainWalQueue();
        driveRefreshToQuiescence(job);
        // One episode in three whose kept batch sealed the head, under a duration cadence the
        // episode's passes cannot outlast, commits the departed batch with no row cadence: no root
        // seals it, so the resume repairing its departure anchors on the head's own root and
        // emits nothing. While the owed-root arm is on, every such episode of a filtered view does.
        final boolean isUnsealedTurn = isUnsealedDepartureAllowed && departureEpisodes++ % 3 == 1;
        final boolean isDepartedBatchUnsealed = (isUnsealedTurn || (isDepartedOwedRootArm && withWhere && isUnsealedDepartureAllowed))
                && instance.getHeadCheckpointMaxTs() == keptTs[3]
                && engine.getConfiguration().getLiveViewCheckpointMaxDurationMicros() >= Micros.MINUTE_MICROS;
        final long rowsCadence = isDepartedBatchUnsealed ? beginSparseCheckpointCadence() : Numbers.LONG_NULL;
        boolean isOwedRootDeparture = false;
        try {
            final long[] departedTs = new long[4];
            final int[] departedSym = new int[4];
            // A replace band that deleted the base's newest rows leaves the newest timestamp the
            // view has seen above every base row, so the kept batch can land below it. A batch
            // below it drains out of order, and the repair that takes it seals it whatever the
            // cadence, so an unsealed departed batch starts above it.
            final long latestSeenTs = instance.getLatestSeenTs();
            final boolean isKeptBatchBelowLatestSeenTs = latestSeenTs > keptTs[3];
            final long departedFromTs = isDepartedBatchUnsealed ? Math.max(keptTs[3], latestSeenTs) : keptTs[3];
            commitEpisodeBatch(rnd, departedFromTs + 2 + rnd.nextInt(1_000_000), usedTs, symCount, departedTs, departedSym);
            drainWalQueue();
            driveRefreshToQuiescence(job);
            if (isDepartedBatchUnsealed) {
                Assert.assertEquals("no root seals the departed batch", keptTs[3], instance.getHeadCheckpointMaxTs());
            }

            int lateMode = rnd.nextInt(3);
            long lateTs = Numbers.LONG_NULL;
            for (int attempt = 0; lateMode > 0 && attempt < 16; attempt++) {
                final long t = keptTs[3] + 1 + rnd.nextLong(departedTs[0] - keptTs[3] - 1);
                if (usedTs == null || usedTs.add(t)) {
                    lateTs = t;
                    break;
                }
            }
            if (lateTs == Numbers.LONG_NULL || isDepartedBatchUnsealed) {
                // A late row the filter keeps would give the resume a row to emit.
                lateMode = 0;
            }
            final int lateSym = rnd.nextInt(symCount);
            final long lateI = 1 + rnd.nextInt(1000);
            final double lateX = rnd.nextDouble() * 1000.0;
            // While the arm is on, the test's first departure of a filtered view whose departed batch
            // a root sealed, and one in three after that, fails its frontier truncate's publication
            // under a lock on _timeline: see runKeptTimelineEpisode. It needs the splice, so it takes
            // the departure from the crash and from a declined chain, which the next departure can
            // draw. Drawn from a sequence salted apart from the run's, and only while the arm is on,
            // so a pinned-seed batch of a test without the arm draws what it drew before.
            final boolean isKeptTimelineDrawn = isKeptTimelineEpisodeArm
                    && withWhere
                    && isFrontierTruncateCrashAllowed
                    && !isDepartedBatchUnsealed
                    && (keptTimelineEpisodes == 0 || keptTimelineEpisodeRnd(rnd).nextInt(3) == 0);
            final boolean isCrashDrawn = !isKeptTimelineDrawn
                    && isFrontierTruncateCrashAllowed
                    && (resumeFrontierTruncateCrashes == 0 || frontierTruncateCrashRnd(rnd).nextInt(3) == 0);
            final long truncatesBefore = job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT);
            final long resumesBefore = job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR);
            // The first departure of a batch that can reach it, and one in three after it, declines
            // the checkpoint chain: its resume truncates at the kept batch's root ahead of its
            // commit and replays no row, so that root, the view's new frontier, is published at the
            // commit, and the restart restores from it. A late row in the departure's own commit
            // would give the replay a row to emit, a crash drawn at the splice's frontier truncate
            // needs the splice, and the batch's first unsealed departure keeps the splice whose
            // zero-row re-stamp it exists for. Counted rather than drawn, so a pinned-seed batch
            // draws what it drew before.
            final boolean isChainDeclined = !isCrashDrawn
                    && !isKeptTimelineDrawn
                    && lateMode != 1
                    && (!isDepartedBatchUnsealed || unsealedDepartures > 0)
                    && chainDeclineCandidates++ % 3 == 0;
            final int maxChainedBoundaries = engine.getConfiguration().getLiveViewCheckpointRepairMaxChainedBoundaries();
            if (isChainDeclined) {
                setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
            }
            // While the owed-root arm is on, a filtered view's departure of a batch no root sealed
            // keeps a late row between the two batches in its commit, which the resume from the
            // kept batch's root emits: the view's new frontier, which the splice's newest root, the
            // kept batch's, sits below. The resume's closing seal, which owes that row a root,
            // fails. Every other such departure, from the second, comes after two failed seals in
            // a row, so its closing seal is the third and pauses the seal. Drawn from a sequence
            // salted apart from the run's, and only while the arm is on, so a pinned-seed batch of
            // a test without the arm draws what it drew before.
            long owedRootTs = Numbers.LONG_NULL;
            if (isDepartedOwedRootArm && withWhere && isDepartedBatchUnsealed && !isChainDeclined) {
                final Rnd owedRootRnd = departedOwedRootRnd(rnd);
                for (int attempt = 0; attempt < 16 && owedRootTs == Numbers.LONG_NULL; attempt++) {
                    final long t = keptTs[3] + 1 + owedRootRnd.nextLong(departedTs[0] - keptTs[3] - 1);
                    if (usedTs == null || usedTs.add(t)) {
                        owedRootTs = t;
                    }
                }
            }
            isOwedRootDeparture = owedRootTs != Numbers.LONG_NULL;
            final boolean isOwedRootPaused = isOwedRootDeparture && departedOwedRootResumeEpisodes++ % 2 == 1;
            if (isOwedRootDeparture) {
                lateMode = 1;
                lateTs = owedRootTs;
                if (isOwedRootPaused) {
                    failRowlessSealsAheadOfADeparture(job, usedTs, instance, 2);
                }
            }
            final long owedRootSealFailuresBefore = instance.getCheckpointSealFailures();
            final long owedRootMarkerGeneration = publishedCheckpointGeneration(instance);
            if (isOwedRootDeparture) {
                hooks(job).setSimulateResumeHeadSealStart(
                        () -> job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH)
                );
            }
            // While the cleared-head arm is on, a filtered view's departure of a sealed batch that
            // no late row or fault in its commit drew fails the read of the view's table the
            // resume's closing seal makes, when that seal only re-stamps the head on a root the
            // frontier truncate kept at the view's new frontier: see awaitTheClearedHead. Counted
            // rather than drawn, so the run's random sequence draws what it drew before.
            final boolean isClearedHeadDeparture = isClearedHeadArm
                    && withWhere
                    && !isDepartedBatchUnsealed
                    && !isCrashDrawn
                    && !isKeptTimelineDrawn
                    && !isChainDeclined
                    && lateMode == 0;
            if (isClearedHeadDeparture) {
                // Only behind the frontier truncate of a splice, whose closing seal re-stamps the
                // head when the truncate kept a root at the frontier.
                hooks(job).setSimulateResumeHeadSealStart(() -> {
                    if (job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT) > truncatesBefore) {
                        failTheReadOfARestamp(instance);
                    }
                });
            }
            final long sealFailuresBefore = instance.getCheckpointSealFailures();
            // The test's first kept-timeline episode, and one in two after it, also fails the check
            // of _timeline after the unlink the lock refuses: the retire cannot tell whether the
            // file went, which it answers as it answers the file the lock kept. Drawn from a
            // sequence salted apart from the run's, so a pinned-seed batch draws what it drew before.
            final boolean isTimelineCheckFailing = isKeptTimelineDrawn
                    && (keptTimelineCheckFaults == 0 || keptTimelineCheckRnd(rnd).nextBoolean());
            if (isKeptTimelineDrawn) {
                checkpointFaults.armTheKeptTimeline(() -> {
                    job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
                    checkpointFaults.lockTheTimeline();
                    if (isTimelineCheckFailing) {
                        checkpointFaults.failTheTimelineCheckAfterItsNextUnlink();
                    }
                });
            }
            try {
                departNewestBatch(job, baseToken, usedTs, withWhere, departedTs, departedSym, keptTs[3], lateMode, lateTs, lateSym, lateI, lateX, isCrashDrawn);
            } finally {
                if (isChainDeclined) {
                    setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, maxChainedBoundaries);
                }
                if (isKeptTimelineDrawn) {
                    checkpointFaults.disarmTheKeptTimeline();
                    job.setCheckpointTimelineTestFailureStage(0);
                }
                if (isOwedRootDeparture) {
                    hooks(job).setSimulateResumeHeadSealStart(null);
                    job.setCheckpointTimelineTestFailureStage(0);
                }
                if (isClearedHeadDeparture) {
                    hooks(job).setSimulateResumeHeadSealStart(null);
                    checkpointFaults.disarmTheViewTableRead();
                }
            }
            if (isClearedHeadDeparture) {
                awaitTheClearedHead(job, instance, false);
            }
            if (isKeptTimelineDrawn && checkpointFaults.consumeKeptTimeline()) {
                Assert.assertEquals(
                        "the retire's check of _timeline fails when the departure draws it",
                        isTimelineCheckFailing ? 1 : 0,
                        checkpointFaults.consumeFailedTimelineChecks()
                );
                if (isTimelineCheckFailing) {
                    keptTimelineCheckFaults++;
                }
                runKeptTimelineEpisode(
                        keptTimelineEpisodeRnd(rnd),
                        job,
                        usedTs,
                        symCount,
                        keptTs,
                        sealFailuresBefore,
                        oracleOrderedSql,
                        viewOrderedSql
                );
            }
            if (checkpointFaults.hasCrashed()) {
                Assert.assertEquals(
                        "the crash interrupts the resume's frontier truncate",
                        truncatesBefore + 1,
                        job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT)
                );
                resumeFrontierTruncateCrashes++;
            }
            if (isOwedRootDeparture) {
                Assert.assertEquals("the resume repairs the departure", 1, job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR) - resumesBefore);
                Assert.assertEquals("the late row is the view's new frontier", owedRootTs, queryTimestamp("SELECT max(ts) FROM lv"));
                Assert.assertEquals("the failed closing seal leaves the head on the kept batch's root", keptTs[3], instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("the replay fed the late row", owedRootTs, instance.getMinSeenTsSinceCheckpoint());
                Assert.assertEquals(departedTs[3], instance.getLatestSeenTs());
                awaitTheDepartedOwedRoot(instance, owedRootTs, owedRootMarkerGeneration, owedRootSealFailuresBefore, isOwedRootPaused, false);
            } else if (isDepartedBatchUnsealed) {
                Assert.assertEquals("the departure keeps the head on the kept batch's root", keptTs[3], instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("with nothing fed since it", Long.MAX_VALUE, instance.getMinSeenTsSinceCheckpoint());
                Assert.assertEquals(departedTs[3], instance.getLatestSeenTs());
                unsealedDepartures++;
                if (isKeptBatchBelowLatestSeenTs) {
                    unsealedDeparturesBelowLatestSeenTs++;
                }
            }
        } finally {
            if (isDepartedBatchUnsealed) {
                endSparseCheckpointCadence(rowsCadence);
            }
        }
        // The restart after a crash comes back from the crash image, which no later root reaches.
        // An owed root, or a cleared head's root, has to land on the stretch without rows after the
        // departure, which a departure above a rowless anchor would land with its batches' seals.
        if (isRowlessAnchorEpisodeAllowed
                && !checkpointFaults.hasCrashed()
                && !isOwedRootDeparture
                && clearedHeadRootTs == Numbers.LONG_NULL) {
            departNewestRowsAboveARowlessAnchor(rowlessAnchorEpisodeRnd(rnd), job, baseToken, usedTs, symCount, withWhere);
        }
        return true;
    }

    /**
     * A departure above a rowless anchor: three batches of four rows above every row the view has
     * seen, kept, middle and newest, each committed and refreshed on its own, so each seals a root
     * at its newest row. A chained correction moves the middle batch out of the view, or, every
     * other episode, every row of it but its first: the splice re-versions the middle batch's root
     * with the rows below it, so that root holds no row of its own, and keeps the newest batch's
     * root above it. With the chain declined, the newest batch then departs: its resume anchors on
     * the rowless root and replays no row, so the view's new frontier sits below the anchor. A
     * frontier truncate drops the anchor after the replacement commit, and then a watermark
     * publishes the kept batch's root, which sits at the frontier, or, where the middle batch's
     * first row is the frontier and no root sits there, a seal lands there. Either way the head
     * sits on a root at the frontier at the repair commit, the timeline keeps its prefix, and the
     * caller's restart restores from it. The rows leave as they do in departNewestRows. Draws
     * from {@code episodeRnd} alone, so the run's random sequence draws what it drew before.
     */
    private void departNewestRowsAboveARowlessAnchor(
            Rnd episodeRnd,
            LiveViewRefreshJob job,
            TableToken baseToken,
            @Nullable LongHashSet usedTs,
            int symCount,
            boolean withWhere
    ) throws Exception {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        final boolean isRootKeptAtTheFrontier = rowlessAnchorEpisodes++ % 2 == 0;
        final long fromTs = Math.max(
                Math.max(queryTimestamp("SELECT max(ts) FROM base"), instance.getLatestSeenTs()),
                instance.getDefinition().getViewLowerBoundTimestamp()
        );
        final long[] keptTs = new long[4];
        final long[] middleTs = new long[4];
        final int[] middleSym = new int[4];
        final long[] newestTs = new long[4];
        final int[] newestSym = new int[4];
        commitEpisodeBatch(episodeRnd, fromTs + 1 + episodeRnd.nextInt(1_000_000), usedTs, symCount, keptTs, null);
        drainWalQueue();
        driveRefreshToQuiescence(job);
        commitEpisodeBatch(episodeRnd, keptTs[3] + 2 + episodeRnd.nextInt(1_000_000), usedTs, symCount, middleTs, middleSym);
        drainWalQueue();
        driveRefreshToQuiescence(job);
        commitEpisodeBatch(episodeRnd, middleTs[3] + 2 + episodeRnd.nextInt(1_000_000), usedTs, symCount, newestTs, newestSym);
        drainWalQueue();
        driveRefreshToQuiescence(job);
        Assert.assertEquals("the newest batch seals its root", newestTs[3], instance.getHeadCheckpointMaxTs());

        // The chain stays on: the splice keeps the middle batch's root above the rows it keeps.
        final long declinesBefore = job.getStatsForTest().get(LiveViewRefreshStats.RESUME_CHAIN_KEY_BUDGET_DECLINE_COUNT);
        final long frontierTs;
        if (isRootKeptAtTheFrontier) {
            frontierTs = keptTs[3];
            departNewestBatch(job, baseToken, usedTs, withWhere, middleTs, middleSym, frontierTs, 0, Numbers.LONG_NULL, 0, 0, 0.0, false);
        } else {
            frontierTs = middleTs[0];
            departNewestBatch(
                    job,
                    baseToken,
                    usedTs,
                    withWhere,
                    Arrays.copyOfRange(middleTs, 1, middleTs.length),
                    Arrays.copyOfRange(middleSym, 1, middleSym.length),
                    frontierTs,
                    0,
                    Numbers.LONG_NULL,
                    0,
                    0,
                    0.0,
                    false
            );
        }
        if (job.getStatsForTest().get(LiveViewRefreshStats.RESUME_CHAIN_KEY_BUDGET_DECLINE_COUNT) != declinesBefore) {
            // A tight key budget priced the chain out: the truncate dropped the middle batch's
            // root with the newest batch's, so no rowless anchor is left to depart above.
            rowlessAnchorEpisodesPricedOut++;
            return;
        }
        final LongList ladder = snapshotCheckpointLadder(instance);
        final int top = ladder.size();
        Assert.assertTrue("the kept, middle and newest batches' roots", top >= 6);
        Assert.assertEquals(newestTs[3], ladder.getQuick(top - 2));
        Assert.assertEquals(middleTs[3], ladder.getQuick(top - 4));
        Assert.assertEquals(keptTs[3], ladder.getQuick(top - 6));
        Assert.assertEquals(
                "the middle batch's root stands on the rows the splice kept below it, none of its own",
                ladder.getQuick(top - 5) + (isRootKeptAtTheFrontier ? 0 : 1),
                ladder.getQuick(top - 3)
        );

        final long keepsBefore = job.getStatsForTest().get(LiveViewRefreshStats.TRUNCATE_ZERO_ROW_KEEP_COUNT);
        final long resetsBefore = instance.getCheckpointTimelineResets();
        final long generationBefore = publishedCheckpointGeneration(instance);
        final long nextCheckpointIdBefore = publishedNextCheckpointId(instance);
        final int maxChainedBoundaries = engine.getConfiguration().getLiveViewCheckpointRepairMaxChainedBoundaries();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        try {
            departNewestBatch(job, baseToken, usedTs, withWhere, newestTs, newestSym, frontierTs, 0, Numbers.LONG_NULL, 0, 0, 0.0, false);
        } finally {
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, maxChainedBoundaries);
        }
        Assert.assertEquals("the zero-row resume keeps its timeline", resetsBefore, instance.getCheckpointTimelineResets());
        Assert.assertEquals(keepsBefore + 1, job.getStatsForTest().get(LiveViewRefreshStats.TRUNCATE_ZERO_ROW_KEEP_COUNT));
        final LongList kept = snapshotCheckpointLadder(instance);
        Assert.assertEquals("the frontier truncate drops the rowless anchor", frontierTs, kept.getQuick(kept.size() - 2));
        Assert.assertEquals(frontierTs, instance.getHeadCheckpointMaxTs());
        Assert.assertEquals(
                "at the repair commit",
                instance.getStateReader().getLvConsumedSeqTxn(),
                instance.getHeadCheckpointBaseSeqTxn()
        );
        Assert.assertEquals(
                "the truncate, the frontier truncate and the closing publication",
                generationBefore + 3,
                publishedCheckpointGeneration(instance)
        );
        try (Path dir = checkpointsDirOf(instance)) {
            Assert.assertFalse("no repair marker stays", LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
        }
        if (isRootKeptAtTheFrontier) {
            Assert.assertEquals("the watermark mints no checkpoint id", nextCheckpointIdBefore, publishedNextCheckpointId(instance));
            rowlessAnchorWatermarks++;
        } else {
            Assert.assertEquals("the seal mints one checkpoint id", nextCheckpointIdBefore + 1, publishedNextCheckpointId(instance));
            rowlessAnchorFrontierSeals++;
        }
    }

    /**
     * A correction the chain price admits at its first boundary and refuses at its second. Four
     * batches of four rows on keys the run uses, a second apart from the floor of the day above
     * every row the base and the view hold, each committed and refreshed on its own, so each seals
     * a root at its newest row. Then one late commit: a row on a key the run uses just above the
     * first batch's root, which the resume anchors on, and {@link #NEW_KEY_EPISODE_KEYS} rows on
     * keys the run never used just above the second batch's root. The estimate interpolates the
     * day's rows from its floor to its newest row, so with the first root 3 ms above the floor and
     * the newest row 3 s above it, it counts nearly every row above the anchor: at least the 50
     * new keys' rows, at most 17 + 50. The replay reaches the second batch's root with the
     * frontier's keys, at most 16, and 3 x 16 stays within the estimate, so that root freezes; it
     * reaches the third with the new keys too, and K_1 + 2 x (K_front + 50) passes the estimate:
     * the price refuses it and discards the frozen root. Draws from {@code episodeRnd} alone.
     *
     * @return false when the view is not active, so nothing was committed
     */
    private boolean commitNewKeyEpisode(Rnd episodeRnd, LiveViewRefreshJob job, int symCount) throws Exception {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        if (instance == null || instance.getStateReader().getSeedState() != LiveViewState.SEED_STATE_ACTIVE) {
            return false;
        }
        final long floorTs = Math.max(
                Math.max(queryTimestamp("SELECT max(ts) FROM base"), instance.getLatestSeenTs()),
                instance.getDefinition().getViewLowerBoundTimestamp()
        );
        final long dayTs = floorTs - floorTs % Micros.DAY_MICROS + Micros.DAY_MICROS;
        final long[][] batchTs = new long[4][4];
        for (int b = 0; b < batchTs.length; b++) {
            commitEpisodeBatch(episodeRnd, dayTs + 1 + b * Micros.SECOND_MICROS, null, symCount, batchTs[b], null);
            drainWalQueue();
            driveRefreshToQuiescence(job);
            Assert.assertEquals("each batch seals its root", batchTs[b][3], instance.getHeadCheckpointMaxTs());
        }
        final StringSink values = new StringSink();
        appendEpisodeRow(values, batchTs[0][3] + 1, episodeRnd.nextInt(symCount), 1 + episodeRnd.nextInt(1000), episodeRnd.nextDouble() * 1000.0);
        for (int k = 0; k < NEW_KEY_EPISODE_KEYS; k++) {
            values.put(", (").put(batchTs[1][3] + 1).put("::timestamp, 'NK").put(k).put("', ")
                    .put(1 + episodeRnd.nextInt(1000)).put(", ").put(episodeRnd.nextDouble() * 1000.0).put(')');
        }
        final long declinesBefore = job.getStatsForTest().get(LiveViewRefreshStats.RESUME_CHAIN_KEY_BUDGET_DECLINE_COUNT);
        final long discardedBefore = job.getStatsForTest().get(LiveViewRefreshStats.RESUME_CHAIN_DISCARDED_ROOT_COUNT);
        execute("INSERT INTO base (ts, sym, i, x) VALUES " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
        final long declines = job.getStatsForTest().get(LiveViewRefreshStats.RESUME_CHAIN_KEY_BUDGET_DECLINE_COUNT) - declinesBefore;
        final long discarded = job.getStatsForTest().get(LiveViewRefreshStats.RESUME_CHAIN_DISCARDED_ROOT_COUNT) - discardedBefore;
        LOG.info().$("LV fuzz new-key episode [anchorTs=").$ts(batchTs[0][3])
                .$(", newKeysTs=").$ts(batchTs[1][3] + 1)
                .$(", declines=").$(declines)
                .$(", discarded=").$(discarded).I$();
        newKeyEpisodes++;
        if (discarded > 0) {
            Assert.assertEquals("a late decline discards the root it froze below the new keys", 1, discarded);
            newKeyEpisodeLateDeclines++;
        }
        return true;
    }

    /**
     * A correction the head miss's chain price admits at its first boundary and refuses at its
     * second, after a late row whose chained splice it publishes. One row alone in the first UTC
     * day above every row the base and the view hold, then four batches of four rows on keys the
     * run uses in the day after it, a second apart from the day's floor. The lone row commits
     * with the first batch, so that batch's seal lands above it and no root sits between it and
     * the run's newest root; each batch's refresh seals a root at its newest row. The view's
     * anchor starts its segments at UTC midnight, the floor of the base's DAY partitions, so a
     * late row below the day's first root rebuilds from the day's floor and reads the day's
     * partition whole, which the estimate counts exactly. A resume from the run's newest root
     * reads that partition too, and the lone row's, which the estimate also counts whole, and
     * the part of its own above the root: at least one row more. The rebuild is cheaper, so
     * each late commit takes the head miss, whose chain stands on nothing, over the day's four
     * roots.
     * <p>
     * The first late commit, one row just above the day's first row, runs with scan.max.keys at
     * 0, which lifts the budget, so its chain publishes: the four roots re-versioned, the three
     * above the first frozen incrementally. The second runs under the arm's budget: a row at the
     * day's floor and {@link #NEW_KEY_EPISODE_KEYS} rows on keys the run never used just above
     * the first batch's root, so B = max(16 + 1 + 1 + 50 = 68 rows, scan.max.keys of at most 5)
     * = 68. Each shape images one function whole, holding the keys the replay has read: boundary
     * 0 at most the two late rows' and the first batch's, 6, and 6 x 4 = 24 <= 68 admits it;
     * boundary 1 at least the 50 new keys, and 50 x 3 = 150 > 68 refuses it, discarding the root
     * boundary 0 froze. Every row the episode writes passes the run's filter (i > 0) and sits
     * above its START FROM boundary. Draws from {@code episodeRnd} alone.
     *
     * @return false when the view is not active, so nothing was committed
     */
    private boolean commitHeadMissNewKeyEpisode(
            Rnd episodeRnd,
            LiveViewRefreshJob job,
            int symCount,
            String oracleOrderedSql,
            String viewOrderedSql
    ) throws Exception {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        if (instance == null || instance.getStateReader().getSeedState() != LiveViewState.SEED_STATE_ACTIVE) {
            return false;
        }
        final long floorTs = Math.max(
                Math.max(queryTimestamp("SELECT max(ts) FROM base"), instance.getLatestSeenTs()),
                instance.getDefinition().getViewLowerBoundTimestamp()
        );
        final long loneDayTs = floorTs - floorTs % Micros.DAY_MICROS + Micros.DAY_MICROS;
        final long loneTs = loneDayTs + episodeRnd.nextLong(Micros.DAY_MICROS);
        final long dayTs = loneDayTs + Micros.DAY_MICROS;
        final long[][] batchTs = new long[4][4];
        final StringSink values = new StringSink();
        appendEpisodeRow(values, loneTs, episodeRnd.nextInt(symCount), 1 + episodeRnd.nextInt(1000), episodeRnd.nextDouble() * 1000.0);
        drawEpisodeBatch(episodeRnd, values, dayTs + 1, null, symCount, batchTs[0], null);
        execute("INSERT INTO base (ts, sym, i, x) VALUES " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
        Assert.assertEquals("each batch seals its root", batchTs[0][3], instance.getHeadCheckpointMaxTs());
        for (int b = 1; b < batchTs.length; b++) {
            commitEpisodeBatch(episodeRnd, dayTs + 1 + b * Micros.SECOND_MICROS, null, symCount, batchTs[b], null);
            drainWalQueue();
            driveRefreshToQuiescence(job);
            Assert.assertEquals("each batch seals its root", batchTs[b][3], instance.getHeadCheckpointMaxTs());
        }
        assertHeadMissEpisodeRoots(instance, batchTs, loneTs);

        final LiveViewRefreshStats stats = job.getStatsForTest();
        final LiveViewCheckpointCaptureLedger ledger = instance.getCheckpointCaptureLedgerForTest();
        final long incrementalBefore = ledger.getWindowIncrementalCaptures();
        long declinesBefore = stats.get(LiveViewRefreshStats.HEAD_MISS_SPLICE_KEY_BUDGET_DECLINE_COUNT);
        long rangeRoutesBefore = stats.getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RANGE_REBUILD);
        long versionedBefore = instance.getCheckpointRepairRootsVersioned();
        values.clear();
        appendEpisodeRow(values, batchTs[0][0] + 1, episodeRnd.nextInt(symCount), 1 + episodeRnd.nextInt(1000), episodeRnd.nextDouble() * 1000.0);
        final long scanMaxKeys = engine.getConfiguration().getLiveViewCheckpointRepairScanMaxKeys();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 0);
        try {
            execute("INSERT INTO base (ts, sym, i, x) VALUES " + values);
            drainWalQueue();
            driveRefreshToQuiescence(job);
        } finally {
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, scanMaxKeys);
        }
        Assert.assertEquals("the lifted budget keeps the chain", declinesBefore, stats.get(LiveViewRefreshStats.HEAD_MISS_SPLICE_KEY_BUDGET_DECLINE_COUNT));
        Assert.assertEquals("the late row takes the head miss", rangeRoutesBefore + 1, stats.getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RANGE_REBUILD));
        Assert.assertEquals("the splice re-versions the day's four roots", versionedBefore + 4, instance.getCheckpointRepairRootsVersioned());
        Assert.assertEquals("the chain freezes the window incrementally above its first root", incrementalBefore + 3, ledger.getWindowIncrementalCaptures());
        headMissChainedSplices++;
        TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracleOrderedSql, viewOrderedSql, LOG, true);
        assertHeadMissEpisodeRoots(instance, batchTs, loneTs);

        values.clear();
        appendEpisodeRow(values, dayTs, episodeRnd.nextInt(symCount), 1 + episodeRnd.nextInt(1000), episodeRnd.nextDouble() * 1000.0);
        for (int k = 0; k < NEW_KEY_EPISODE_KEYS; k++) {
            values.put(", (").put(batchTs[0][3] + 1).put("::timestamp, 'NK").put(k).put("', ")
                    .put(1 + episodeRnd.nextInt(1000)).put(", ").put(episodeRnd.nextDouble() * 1000.0).put(')');
        }
        declinesBefore = stats.get(LiveViewRefreshStats.HEAD_MISS_SPLICE_KEY_BUDGET_DECLINE_COUNT);
        final long discardedBefore = stats.get(LiveViewRefreshStats.HEAD_MISS_SPLICE_DISCARDED_ROOT_COUNT);
        rangeRoutesBefore = stats.getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RANGE_REBUILD);
        versionedBefore = instance.getCheckpointRepairRootsVersioned();
        execute("INSERT INTO base (ts, sym, i, x) VALUES " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
        final long declines = stats.get(LiveViewRefreshStats.HEAD_MISS_SPLICE_KEY_BUDGET_DECLINE_COUNT) - declinesBefore;
        final long discarded = stats.get(LiveViewRefreshStats.HEAD_MISS_SPLICE_DISCARDED_ROOT_COUNT) - discardedBefore;
        LOG.info().$("LV fuzz head-miss new-key episode [loneTs=").$ts(loneTs)
                .$(", dayTs=").$ts(dayTs)
                .$(", newKeysTs=").$ts(batchTs[0][3] + 1)
                .$(", declines=").$(declines)
                .$(", discarded=").$(discarded).I$();
        Assert.assertEquals("the price refuses the second boundary", 1, declines);
        Assert.assertEquals("and discards the root the first one froze", 1, discarded);
        Assert.assertEquals("the correction takes the head miss", rangeRoutesBefore + 1, stats.getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RANGE_REBUILD));
        Assert.assertEquals("the declined chain re-versions no root", versionedBefore, instance.getCheckpointRepairRootsVersioned());
        headMissNewKeyEpisodes++;
        return true;
    }

    /**
     * Commits a period of rows above every row of the run, a root at the end of each commit, and
     * departs its newest rows in two commits: an upsert out of the filter when
     * {@code isUpsertDeparture}, a pure-delete REPLACE_RANGE band otherwise. Of two keys, K1 and K2:
     * <ul>
     *     <li>for an anchored view ({@code rangeFrameMicros} is {@link Numbers#LONG_NULL}), around
     *     the first anchor boundary B above the root row, a second into the day after the run's
     *     rows, with closed periods on either side and the root row inside the one below: then rows
     *     of both keys spread up to the frontier F = B - 30s, with one of K1 at C1 = B + 1s above
     *     them; then K1 at C2 = B + 2s. A resume from the root below B reads the rows of the
     *     crossing batch, so the departure of C1 prices the head miss from B, whose splice
     *     re-versions the roots at C1 and C2 from a runtime warmed from B. The departure of C2
     *     resumes from the root at C1 and replays nothing, which leaves the view's frontier at F,
     *     below the anchor: the resume derives the state at F by restoring the root row's root,
     *     inside F's period;</li>
     *     <li>for a RANGE frame of width W: a root row of K1 at t0; then a kept row of K1 at
     *     t0 + 1.15W, rows of K2 above it below C1 - W, and rows of K1 up to F = t0 + 2.1W, with C1 =
     *     t0 + 2.3W above them; then C2 = t0 + 2.4W. The departure of C1 prices the head miss from
     *     C1 - W, and the resume that departs C2 leaves the frontier at F, whose frame reaches back to
     *     F - W, above the root at t0: it folds the frame's rows into an identity runtime.</li>
     * </ul>
     * Then late rows below the boundary, or below C1, of a departed key and an untouched one. Under
     * a row cadence of one and a chain no count or key budget declines - but for the second
     * departure of every other episode the departure arm ({@link #isTwoCommitDepartureArm}) runs,
     * from the first, whose resume then holds no capture and truncates and seals at F after its
     * commit - the view must equal {@code oracleOrderedSql} after each commit, unless it is null,
     * and after a restart, which ends the episode when {@code isRestartedAtTheEnd}. The rows that
     * stay go to {@code survivingRows}, when not null, as an INSERT's VALUES list. The episode's
     * rows start on the day partition after the run's newest row, which holds no other row, so the
     * planner's estimate of the rows each repair reads tells the two dispositions apart.
     *
     * @return the job the run goes on with
     */
    private LiveViewRefreshJob departAPeriodInTwoCommits(
            Rnd episodeRnd,
            LiveViewRefreshJob job,
            boolean isUpsertDeparture,
            long rangeFrameMicros,
            @Nullable String oracleOrderedSql,
            String viewOrderedSql,
            boolean isRestartedAtTheEnd,
            @Nullable StringSink survivingRows
    ) throws Exception {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        if (instance == null
                || instance.isInvalid()
                || instance.isCheckpointRecoveryBlocked()
                || instance.getStateReader().getSeedState() != LiveViewState.SEED_STATE_ACTIVE) {
            return job;
        }
        final long floorTs = Math.max(
                Math.max(queryTimestamp("SELECT max(ts) FROM base"), instance.getLatestSeenTs()),
                instance.getDefinition().getViewLowerBoundTimestamp()
        );
        // The episode starts on a day partition of its own: the planner prices a repair by
        // spreading a partition's rows evenly over its span, and the run's rows on the same day
        // would price a resume over the episode's few rows as no rows at all.
        final long freshDayTs = floorTs - floorTs % Micros.DAY_MICROS + Micros.DAY_MICROS;
        // K1's root row, the crossing batch with its key per row, the two departed rows and the
        // two late rows, of K1 and K2.
        final long rootTs;
        final long[] batchTs;
        final int[] batchKeys;
        final long departedTs1;
        final long departedTs2;
        final long lateTs1;
        final long lateTs2;
        if (rangeFrameMicros == Numbers.LONG_NULL) {
            final LiveViewCheckpointAnchorPlan anchorPlan = instance.getAnchorWindow() != null
                    ? instance.getAnchorWindow().getCheckpointAnchorPlan()
                    : null;
            if (anchorPlan == null) {
                return job;
            }
            // The first boundary above the root row, at the fresh day's start, whose period starts
            // at it and is closed above, with the previous period holding the root row.
            final long rootCandidateTs = freshDayTs + Micros.SECOND_MICROS;
            final long boundaryTs = anchorPlan.getSegmentEndExclusive(rootCandidateTs);
            if (boundaryTs == Numbers.LONG_NULL || boundaryTs - 2 * Micros.MINUTE_MICROS <= rootCandidateTs) {
                return job;
            }
            final long previousStartTs = anchorPlan.getSegmentStart(boundaryTs - 30 * Micros.SECOND_MICROS);
            if (previousStartTs == Long.MIN_VALUE
                    || previousStartTs > rootCandidateTs
                    || anchorPlan.getSegmentStart(boundaryTs + Micros.SECOND_MICROS) != boundaryTs
                    || anchorPlan.getSegmentEndExclusive(boundaryTs + Micros.SECOND_MICROS) == Numbers.LONG_NULL) {
                return job;
            }
            rootTs = rootCandidateTs;
            // The crossing batch's rows below the boundary spread over the previous period from
            // the root row up to the frontier B - 30s, so the planner's even spread over the day
            // partition prices the resume from the root at every one of them.
            final int belowCount = 10;
            batchTs = new long[belowCount];
            batchKeys = new int[belowCount];
            final long frontierTs = boundaryTs - 30 * Micros.SECOND_MICROS;
            for (int r = 0; r < belowCount; r++) {
                batchTs[r] = rootTs + (frontierTs - rootTs) * (r + 1) / belowCount;
                batchKeys[r] = r % 2;
            }
            departedTs1 = boundaryTs + Micros.SECOND_MICROS;
            departedTs2 = boundaryTs + 2 * Micros.SECOND_MICROS;
            lateTs1 = boundaryTs - 20 * Micros.SECOND_MICROS;
            lateTs2 = boundaryTs - 15 * Micros.SECOND_MICROS;
        } else {
            final long w = rangeFrameMicros;
            final long t0 = freshDayTs + Micros.SECOND_MICROS;
            rootTs = t0;
            batchTs = new long[]{t0 + w * 115 / 100, t0 + w * 120 / 100, t0 + w * 121 / 100, t0 + w * 122 / 100, t0 + w * 2, t0 + w * 210 / 100};
            batchKeys = new int[]{0, 1, 1, 1, 0, 0};
            departedTs1 = t0 + w * 230 / 100;
            departedTs2 = t0 + w * 240 / 100;
            lateTs1 = t0 + w * 215 / 100;
            lateTs2 = t0 + w * 216 / 100;
        }
        final String key1 = SYMBOLS[0];
        final String key2 = SYMBOLS[1];
        final TableToken baseToken = engine.verifyTableName("base");
        final LiveViewRefreshStats stats = job.getStatsForTest();
        // A root per commit, and a chain for the first departure's splice whatever the run drew:
        // a splice the count guard or the key budget declines truncates the root at C1 away, and
        // the second departure then takes the head miss from the period's start.
        final long rowsCadence = engine.getConfiguration().getLiveViewCheckpointRows();
        final long maxChainedBoundaries = engine.getConfiguration().getLiveViewCheckpointRepairMaxChainedBoundaries();
        final long scanMaxKeys = engine.getConfiguration().getLiveViewCheckpointRepairScanMaxKeys();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 256);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1_000_000);
        try {
            final StringSink kept = new StringSink();
            appendTwoCommitDepartureRow(episodeRnd, kept, rootTs, key1);
            commitTwoCommitDepartureRows(job, kept, oracleOrderedSql, viewOrderedSql);
            final StringSink batch = new StringSink();
            for (int r = 0; r < batchTs.length; r++) {
                appendTwoCommitDepartureRow(episodeRnd, batch, batchTs[r], batchKeys[r] == 0 ? key1 : key2);
            }
            if (survivingRows != null) {
                survivingRows.put(survivingRows.length() > 0 ? ", " : "").put(kept).put(", ").put(batch);
            }
            appendTwoCommitDepartureRow(episodeRnd, batch, departedTs1, key1);
            commitTwoCommitDepartureRows(job, batch, oracleOrderedSql, viewOrderedSql);
            final StringSink newest = new StringSink();
            appendTwoCommitDepartureRow(episodeRnd, newest, departedTs2, key1);
            commitTwoCommitDepartureRows(job, newest, oracleOrderedSql, viewOrderedSql);
            // The departure arm declines the chain for the second departure of every other episode,
            // from the first: counted rather than drawn, so every batch runs its share.
            final boolean isSecondChainDeclined = isTwoCommitDepartureArm && twoCommitDepartureEpisodes % 2 == 0;
            LOG.info().$("LV fuzz two-commit departure [rootTs=").$ts(rootTs)
                    .$(", frontierTs=").$ts(batchTs[batchTs.length - 1])
                    .$(", departedTs1=").$ts(departedTs1)
                    .$(", departedTs2=").$ts(departedTs2)
                    .$(", rangeFrameMicros=").$(rangeFrameMicros)
                    .$(", upsert=").$(isUpsertDeparture)
                    .$(", secondChainDeclined=").$(isSecondChainDeclined).I$();
            final long headMissRepairsBefore = countHeadMissRepairs(job);
            departTwoCommitDepartureRow(baseToken, departedTs1, key1, isUpsertDeparture);
            driveRefreshToQuiescence(job);
            assertTwoCommitDepartureOracle(oracleOrderedSql, viewOrderedSql);
            if (countHeadMissRepairs(job) > headMissRepairsBefore) {
                twoCommitDepartureHeadMisses++;
            }
            final long derivationsBefore = stats.get(LiveViewRefreshStats.RESUME_FRONTIER_STATE_RESTORE_COUNT)
                    + stats.get(LiveViewRefreshStats.RESUME_FRONTIER_STATE_WARM_UP_COUNT);
            final long zeroRowKeepsBefore = stats.get(LiveViewRefreshStats.TRUNCATE_ZERO_ROW_KEEP_COUNT);
            if (isSecondChainDeclined) {
                setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
            }
            departTwoCommitDepartureRow(baseToken, departedTs2, key1, isUpsertDeparture);
            driveRefreshToQuiescence(job);
            assertTwoCommitDepartureOracle(oracleOrderedSql, viewOrderedSql);
            if (isSecondChainDeclined) {
                setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 256);
            }
            if (stats.get(LiveViewRefreshStats.RESUME_FRONTIER_STATE_RESTORE_COUNT)
                    + stats.get(LiveViewRefreshStats.RESUME_FRONTIER_STATE_WARM_UP_COUNT) > derivationsBefore) {
                twoCommitDepartureDerivations++;
                if (stats.get(LiveViewRefreshStats.TRUNCATE_ZERO_ROW_KEEP_COUNT) > zeroRowKeepsBefore) {
                    twoCommitDepartureDeclinedChainDerivations++;
                }
            }
            final StringSink late = new StringSink();
            appendTwoCommitDepartureRow(episodeRnd, late, lateTs1, key1);
            appendTwoCommitDepartureRow(episodeRnd, late, lateTs2, key2);
            if (survivingRows != null) {
                survivingRows.put(", ").put(late);
            }
            commitTwoCommitDepartureRows(job, late, oracleOrderedSql, viewOrderedSql);
            twoCommitDepartureEpisodes++;
        } finally {
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, rowsCadence);
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, maxChainedBoundaries);
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, scanMaxKeys);
        }
        if (!isRestartedAtTheEnd) {
            return job;
        }
        coverage.absorb(job);
        Misc.free(job);
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
        final LiveViewRefreshJob restarted = new LiveViewRefreshJob(0, engine, 1);
        driveRefreshToQuiescence(restarted);
        assertTwoCommitDepartureOracle(oracleOrderedSql, viewOrderedSql);
        return restarted;
    }

    // Appends one row of a two-commit departure episode to values: x > 0 passes every filter the
    // harnesses draw, and i > 0 the dedup harness's.
    private static void appendTwoCommitDepartureRow(Rnd episodeRnd, StringSink values, long ts, String key) {
        values.put(values.length() > 0 ? ", (" : "(").put(ts).put("::timestamp, '").put(key).put("', ")
                .put(1 + episodeRnd.nextInt(1000)).put(", ").put(1 + episodeRnd.nextInt(1000) / 8.0).put(')');
    }

    private void assertTwoCommitDepartureOracle(@Nullable String oracleOrderedSql, String viewOrderedSql) throws SqlException {
        if (oracleOrderedSql != null) {
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracleOrderedSql, viewOrderedSql, LOG, true);
        }
    }

    // The batches each test of the two-commit departure arm ran: see departAPeriodInTwoCommits.
    private void assertReachedTwoCommitDepartures() {
        LOG.info().$("LV fuzz two-commit departures [episodes=").$(twoCommitDepartureEpisodes)
                .$(", headMisses=").$(twoCommitDepartureHeadMisses)
                .$(", derivations=").$(twoCommitDepartureDerivations)
                .$(", declinedChainDerivations=").$(twoCommitDepartureDeclinedChainDerivations).I$();
        Assert.assertTrue("no run departed a period in two commits", twoCommitDepartureEpisodes > 0);
        Assert.assertTrue("no first departure took the head miss", twoCommitDepartureHeadMisses > 0);
    }

    // The lagging TTL episodes of testFuzzBaseTtlAnchored's batch: see runLaggingTtlEpisode.
    private void assertReachedLaggingTtlResidualBoundaryRebuilds() {
        LOG.info().$("LV fuzz lagging TTL episodes [episodes=").$(laggingTtlEpisodes)
                .$(", residualRepairs=").$(laggingTtlResidualRepairs)
                .$(", residualBoundaryRebuilds=").$(laggingTtlResidualBoundaryRebuilds).I$();
        Assert.assertTrue(
                "no lagging TTL episode's open segment residual fell back to the boundary rebuild",
                laggingTtlResidualBoundaryRebuilds > 0
        );
    }

    // The declined-chain second departures of the departure arm's batch: see departAPeriodInTwoCommits.
    private void assertReachedDeclinedChainTwoCommitDepartures() {
        Assert.assertTrue(
                "no second departure under a declined chain derived the state at the view's new frontier and put its root there",
                twoCommitDepartureDeclinedChainDerivations > 0
        );
    }

    private void commitTwoCommitDepartureRows(
            LiveViewRefreshJob job,
            CharSequence values,
            @Nullable String oracleOrderedSql,
            String viewOrderedSql
    ) throws Exception {
        execute("INSERT INTO base (ts, sym, i, x) VALUES " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
        assertTwoCommitDepartureOracle(oracleOrderedSql, viewOrderedSql);
    }

    // Moves the row at ts of key out of the view: an upsert below every filter the dedup harness
    // draws, or a pure-delete REPLACE_RANGE band over ts alone, which the episode's timestamps keep
    // to that row.
    private void departTwoCommitDepartureRow(TableToken baseToken, long ts, String key, boolean isUpsert) throws SqlException {
        if (isUpsert) {
            execute("INSERT INTO base (ts, sym, i, x) VALUES (" + ts + "::timestamp, '" + key + "', -1, 0.0)");
        } else {
            try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                walWriter.commitWithParams(ts, ts + 1, WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE);
            }
        }
        drainWalQueue();
    }

    // The sequence the owed-root arm draws the late row a departure keeps from, drawn from the
    // run's without advancing it.
    private static Rnd departedOwedRootRnd(Rnd rnd) {
        return new Rnd(rnd.getSeed0() ^ DEPARTED_OWED_ROOT_SALT0, rnd.getSeed1() ^ DEPARTED_OWED_ROOT_SALT1);
    }

    // The sequence a lagging TTL episode draws from, drawn from the run's without advancing it.
    private static Rnd laggingTtlEpisodeRnd(Rnd rnd) {
        return new Rnd(rnd.getSeed0() ^ LAGGING_TTL_EPISODE_SALT0, rnd.getSeed1() ^ LAGGING_TTL_EPISODE_SALT1);
    }

    private static Rnd twoCommitDepartureRnd(Rnd rnd) {
        return new Rnd(rnd.getSeed0() ^ TWO_COMMIT_DEPARTURE_SALT0, rnd.getSeed1() ^ TWO_COMMIT_DEPARTURE_SALT1);
    }

    // The four roots of a head-miss new-key episode's day, at its batches' newest rows, and below
    // them no root at or above the lone row, so a resume from the newest root below the day reads
    // that row: see commitHeadMissNewKeyEpisode.
    private void assertHeadMissEpisodeRoots(LiveViewInstance instance, long[][] batchTs, long loneTs) {
        final LongList ladder = snapshotCheckpointLadder(instance);
        final int top = ladder.size();
        final int dayEntries = 2 * batchTs.length;
        Assert.assertTrue("the day's four roots", top >= dayEntries);
        for (int b = 0; b < batchTs.length; b++) {
            Assert.assertEquals("the root of batch " + b, batchTs[b][3], ladder.getQuick(top - dayEntries + 2 * b));
        }
        if (top > dayEntries) {
            Assert.assertTrue("no root between the lone row and the day", ladder.getQuick(top - dayEntries - 2) < loneTs);
        }
    }

    // The departure departNewestRows commits: the departed batch upserted out of the filter or
    // deleted by a REPLACE_RANGE, with the late row lateMode names. The departure's own turn
    // crashes at its frontier truncate when isCrashDrawn.
    private void departNewestBatch(
            LiveViewRefreshJob job,
            TableToken baseToken,
            @Nullable LongHashSet usedTs,
            boolean withWhere,
            long[] departedTs,
            int[] departedSym,
            long keptHighTs,
            int lateMode,
            long lateTs,
            int lateSym,
            long lateI,
            double lateX,
            boolean isCrashDrawn
    ) throws Exception {
        final boolean isUpsert = usedTs == null && withWhere;
        final long departedHighTs = departedTs[departedTs.length - 1];
        LOG.info().$("LV fuzz departure: keptHighTs=").$ts(keptHighTs).$(", departedLowTs=").$ts(departedTs[0])
                .$(", departedHighTs=").$ts(departedHighTs).$(", upsert=").$(isUpsert)
                .$(", lateMode=").$(lateMode).$(", lateTs=").$ts(lateTs)
                .$(", crashDrawn=").$(isCrashDrawn).$();
        runUnderTheFrontierTruncateCrash(isCrashDrawn, () -> {
            if (isUpsert) {
                final StringSink sink = new StringSink();
                sink.put("INSERT INTO base (ts, sym, i, x) VALUES ");
                for (int k = 0; k < departedTs.length; k++) {
                    sink.put(k > 0 ? ", (" : "(").put(departedTs[k]).put("::timestamp, '").put(SYMBOLS[departedSym[k]]).put("', -1, 0.0)");
                }
                if (lateMode == 1) {
                    sink.put(", (").put(lateTs).put("::timestamp, '").put(SYMBOLS[lateSym]).put("', ").put(lateI).put(", ").put(lateX).put(')');
                }
                execute(sink);
            } else {
                try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                    if (lateMode == 1) {
                        appendRow(walWriter, lateTs, lateSym, lateI, lateX);
                    }
                    walWriter.commitWithParams(lateMode == 1 ? lateTs : departedTs[0], departedHighTs + 1, WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE);
                }
            }
            drainWalQueue();
            driveRefreshToQuiescence(job);
        });
        if (lateMode == 2) {
            execute("INSERT INTO base (ts, sym, i, x) VALUES (" + lateTs + "::timestamp, '" + SYMBOLS[lateSym] + "', " + lateI + ", " + lateX + ")");
            drainWalQueue();
            driveRefreshToQuiescence(job);
        }
    }

    // A retire kept _timeline under the lock: the view refuses the generation the superblock
    // names, keptGeneration, which nothing publishes over, its head stays clear, and the frontier
    // truncate's marker beside the file names that generation and records no seqTxn, so it reads
    // as live.
    private void assertKeptTimelineRefused(LiveViewInstance instance, long keptGeneration) {
        Assert.assertTrue("the view refuses the generation the retire kept", instance.isCheckpointTimelineRetirePending());
        Assert.assertEquals("no head survives the retire", Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
        Assert.assertEquals("nothing publishes over the kept generation", keptGeneration, publishedCheckpointGeneration(instance));
        try (Path dir = checkpointsDirOf(instance)) {
            Assert.assertEquals("the marker names the kept generation", keptGeneration, LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir));
            Assert.assertEquals("and records no seqTxn", Numbers.LONG_NULL, LiveViewCheckpointRepairMarker.readLvSeqTxn(configuration, dir));
        }
    }

    /**
     * The departure's frontier truncate failed to publish, and the retire that answered it met a
     * lock on {@code _timeline}: the splice's generation, whose root of the departed batch sits
     * above the view's new frontier, stays on disk under the truncate's marker, and the view
     * refuses it. The test's first episode, and one in two after it, also failed the retire's check
     * of {@code _timeline} after the unlink, which the retire answers as it answers the file the
     * lock kept, so nothing below differs. The lock holds for a drawn 0 to 2 later steps: one or two rows the filter drops,
     * whose cycles have no head to stand on and decline a first root above the view's newest row,
     * then a late row the filter keeps, inside the kept batch, whose repair finds no anchor and
     * whose closing seal retries the retire and is refused. While the lock holds nothing
     * publishes over the kept generation, the head stays clear and the marker live, and the view
     * matches its oracle after each step. Once the lock goes, past any pause the refusals armed,
     * the first seal, a forward row's, retries the retire, which removes the kept file and the
     * marker with it, and opens a fresh history, which the caller's restart restores. The test's
     * second episode, and one in two after it, finds the marker locked once the lock on
     * {@code _timeline} goes instead, which refuses that first seal: see
     * {@link #refuseTheHistoryBesideALockedMarker}. Draws from {@code episodeRnd} alone.
     *
     * @param keptTs             the kept batch's timestamps, below the departed one's
     * @param sealFailuresBefore the seal failures the view counted ahead of the departure
     */
    private void runKeptTimelineEpisode(
            Rnd episodeRnd,
            LiveViewRefreshJob job,
            @Nullable LongHashSet usedTs,
            int symCount,
            long[] keptTs,
            long sealFailuresBefore,
            String oracleOrderedSql,
            String viewOrderedSql
    ) throws Exception {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        keptTimelineEpisodes++;
        // The truncate never published, so the superblock still names the splice's generation,
        // which its marker names too.
        final long keptGeneration = publishedCheckpointGeneration(instance);
        assertKeptTimelineRefused(instance, keptGeneration);
        TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracleOrderedSql, viewOrderedSql, LOG, true);
        final int lockedSteps = episodeRnd.nextInt(3);
        LOG.info().$("LV fuzz kept timeline: keptGeneration=").$(keptGeneration).$(", lockedSteps=").$(lockedSteps).$();
        if (lockedSteps > 0) {
            long ts = Math.max(queryTimestamp("SELECT max(ts) FROM base"), instance.getLatestSeenTs());
            for (int c = 0, n = 1 + episodeRnd.nextInt(2); c < n; c++) {
                ts = commitRowTheFilterDrops(job, usedTs, ts);
            }
            assertKeptTimelineRefused(instance, keptGeneration);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracleOrderedSql, viewOrderedSql, LOG, true);
        }
        if (lockedSteps > 1 && keptTs[3] - keptTs[0] > 1) {
            long lateTs = Numbers.LONG_NULL;
            for (int attempt = 0; attempt < 16 && lateTs == Numbers.LONG_NULL; attempt++) {
                final long t = keptTs[0] + 1 + episodeRnd.nextLong(keptTs[3] - keptTs[0] - 1);
                if (usedTs == null || usedTs.add(t)) {
                    lateTs = t;
                }
            }
            if (lateTs != Numbers.LONG_NULL) {
                final LiveViewRefreshStats stats = job.getStatsForTest();
                final long resumesBefore = stats.getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR)
                        + stats.getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_KEYED_RESUME_FROM_ANCHOR);
                final StringSink values = new StringSink();
                appendEpisodeRow(values, lateTs, episodeRnd.nextInt(symCount), 1 + episodeRnd.nextInt(1000), episodeRnd.nextDouble() * 1000.0);
                execute("INSERT INTO base (ts, sym, i, x) VALUES " + values);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals(
                        "no resume anchors on the kept generation",
                        resumesBefore,
                        stats.getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR)
                                + stats.getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_KEYED_RESUME_FROM_ANCHOR)
                );
                assertKeptTimelineRefused(instance, keptGeneration);
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracleOrderedSql, viewOrderedSql, LOG, true);
                keptTimelineLateRowRepairs++;
            }
        }
        keptTimelineRefusedSeals += (int) (instance.getCheckpointSealFailures() - sealFailuresBefore);
        checkpointFaults.unlockTheTimeline();
        // The test's first episode finds the repair marker unlocked once the lock on _timeline
        // goes, and its next one, and one in two after that, finds it locked. Drawn from a
        // sequence salted apart from the episode's, so the episode draws what it drew before.
        final Rnd markerRnd = keptTimelineMarkerRnd(episodeRnd);
        final boolean isMarkerLocked = keptTimelineRetiresWithTheMarker > 0
                && (keptTimelineMarkerLeftovers == 0 || markerRnd.nextInt(2) == 0);
        if (isMarkerLocked) {
            refuseTheHistoryBesideALockedMarker(markerRnd, job, usedTs, symCount, oracleOrderedSql, viewOrderedSql);
        } else {
            // The forward row's seal below is the first since the lock went: the kept file still
            // stands, and the marker beside it.
            assertKeptTimelineRefused(instance, keptGeneration);
        }
        // Past any pause the refusals armed: each is shorter than the duration cadence's wait.
        setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
        Assert.assertFalse(instance.isSealOnCooldown(currentMicros));
        long forwardTs = Math.max(queryTimestamp("SELECT max(ts) FROM base"), instance.getLatestSeenTs()) + 1;
        while (usedTs != null && !usedTs.add(forwardTs)) {
            forwardTs++;
        }
        final StringSink values = new StringSink();
        appendEpisodeRow(values, forwardTs, episodeRnd.nextInt(symCount), 1 + episodeRnd.nextInt(1000), episodeRnd.nextDouble() * 1000.0);
        execute("INSERT INTO base (ts, sym, i, x) VALUES " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
        Assert.assertFalse("the seal's retry of the retire removed the kept file", instance.isCheckpointTimelineRetirePending());
        Assert.assertEquals("and the seal opened a fresh history", 1, publishedCheckpointGeneration(instance));
        Assert.assertEquals("at the forward row", forwardTs, instance.getHeadCheckpointMaxTs());
        try (Path dir = checkpointsDirOf(instance)) {
            Assert.assertFalse("the marker went with the kept file", LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
        }
        keptTimelineFreshHistories++;
        if (!isMarkerLocked) {
            keptTimelineRetiresWithTheMarker++;
        }
        TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracleOrderedSql, viewOrderedSql, LOG, true);
    }

    /**
     * The kept-timeline episode's lock on {@code _timeline} is gone, and a lock now keeps the
     * repair marker beside it. A forward row's seal, past any pause the episode's refusals armed,
     * retries the retire, which removes {@code _timeline} and cannot remove the marker, and would
     * then open a runtime history beside the marker, which every restart would read against it:
     * the seal is refused, and no history opens. The marker's lock goes on the way out, and the
     * episode's own forward row then opens the history. Draws from {@code markerRnd} alone.
     */
    private void refuseTheHistoryBesideALockedMarker(
            Rnd markerRnd,
            LiveViewRefreshJob job,
            @Nullable LongHashSet usedTs,
            int symCount,
            String oracleOrderedSql,
            String viewOrderedSql
    ) throws Exception {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        keptTimelineMarkerLeftovers++;
        checkpointFaults.lockTheMarker();
        try {
            setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
            Assert.assertFalse(instance.isSealOnCooldown(currentMicros));
            final long sealFailuresBefore = instance.getCheckpointSealFailures();
            long forwardTs = Math.max(queryTimestamp("SELECT max(ts) FROM base"), instance.getLatestSeenTs()) + 1;
            while (usedTs != null && !usedTs.add(forwardTs)) {
                forwardTs++;
            }
            final StringSink values = new StringSink();
            appendEpisodeRow(values, forwardTs, markerRnd.nextInt(symCount), 1 + markerRnd.nextInt(1000), markerRnd.nextDouble() * 1000.0);
            execute("INSERT INTO base (ts, sym, i, x) VALUES " + values);
            drainWalQueue();
            driveRefreshToQuiescence(job);
            Assert.assertFalse("the seal's retry of the retire removed the kept file", instance.isCheckpointTimelineRetirePending());
            Assert.assertEquals("the seal is refused while the marker stays", sealFailuresBefore + 1, instance.getCheckpointSealFailures());
            Assert.assertEquals("no history opens beside it", 0, publishedCheckpointGeneration(instance));
            try (Path dir = checkpointsDirOf(instance)) {
                Assert.assertTrue("the lock keeps the marker", LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
            }
            Assert.assertTrue("the retry of the retire and the seal each meet the lock", checkpointFaults.getRefusedMarkerUnlinks() >= 2);
            refusedHistoryOpenings++;
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracleOrderedSql, viewOrderedSql, LOG, true);
        } finally {
            checkpointFaults.unlockTheMarker();
        }
    }

    /**
     * Drives the view's seed to completion. While the kept-timeline arm is on, the test's runs
     * draw, from a sequence salted apart from the run's, a seed beside a leftover repair marker,
     * until one seed root has landed beside one and one run in three after that: the marker naming
     * no base generation that a retire keeping its timeline writes, stamped ahead of the first seed
     * turn, under a lock that refuses every unlink of it until the seed completes. No reader of a
     * seed history reads the marker, so no seed seal fails on it, and the sweep keeps its resume
     * points. The completion's seal opens the runtime history, which a restart would read the
     * marker against, so it opens none while the lock holds. Then the lock goes, and the view's
     * next seal removes the marker and opens the history.
     */
    private void driveSeed(Rnd rnd, LiveViewRefreshJob job) {
        if (!isKeptTimelineEpisodeArm || !(seedRootsBesideAMarker == 0 || seedMarkerRnd(rnd).nextInt(3) == 0)) {
            driveSeedToCompletion(job, "lv");
            return;
        }
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        try (Path dir = checkpointsDirOf(instance)) {
            LiveViewCheckpointRepairMarker.write(
                    configuration,
                    dir,
                    instance.getLiveViewToken().getTableId(),
                    0,
                    Numbers.LONG_NULL,
                    Numbers.LONG_NULL,
                    Numbers.LONG_NULL
            );
        }
        final LogCapture capture = walApplySuspensionRule.getCapture();
        capture.drain(LogCapture.STOP_DRAIN_TIMEOUT_MS);
        final int seedSealFailuresBefore = capture.countOccurrences(SEED_SEAL_FAILED);
        final long sealFailuresBefore = instance.getCheckpointSealFailures();
        boolean isSeedRootBesideTheMarker = false;
        checkpointFaults.lockTheMarker();
        try {
            for (int pass = 0; pass < SEED_COMPLETION_PASSES && instance.getStateReader().getSeedState() == LiveViewState.SEED_STATE_SEEDING; pass++) {
                if (!job.run()) {
                    // A turn that faulted backs off: the sweep resumes at the view's deadline.
                    final long retryUs = nextRefreshRetryMicros();
                    if (retryUs != Numbers.LONG_NULL) {
                        setCurrentMicros(retryUs);
                    }
                }
                drainWalQueue();
                if (instance.getStateReader().getSeedState() == LiveViewState.SEED_STATE_SEEDING) {
                    Assert.assertEquals("no seal fails while the view seeds", sealFailuresBefore, instance.getCheckpointSealFailures());
                    isSeedRootBesideTheMarker |= instance.getSeedCheckpointDataOffset() != Numbers.LONG_NULL;
                }
            }
            Assert.assertEquals("the seed completes", LiveViewState.SEED_STATE_ACTIVE, instance.getStateReader().getSeedState());
            capture.drain(LogCapture.STOP_DRAIN_TIMEOUT_MS);
            Assert.assertEquals("no seed seal fails on the marker", seedSealFailuresBefore, capture.countOccurrences(SEED_SEAL_FAILED));
            final long completionSealFailures = instance.getCheckpointSealFailures() - sealFailuresBefore;
            Assert.assertTrue("the completion seals once at most", completionSealFailures <= 1);
            Assert.assertEquals("no runtime history opens beside the marker", 0, publishedCheckpointGeneration(instance));
            try (Path dir = checkpointsDirOf(instance)) {
                Assert.assertTrue("the lock keeps the marker", LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
            }
            if (isSeedRootBesideTheMarker) {
                seedRootsBesideAMarker++;
            }
            refusedHistoryOpenings += (int) completionSealFailures;
        } finally {
            checkpointFaults.unlockTheMarker();
        }
        drainWalQueue();
    }

    /**
     * A second {@link #SPARSE_DEPARTURE_FRONTIER_SEAL} departure after the first one's episode,
     * whose closing seal fails once: the rebuild derives the frontier's state, the seal of it
     * fails, and with no root a restart could restore at the frontier the rebuild retires the
     * timeline. Over a filtered view that retire may meet a lock on {@code _timeline}, which keeps
     * the timeline and the live marker beside it. The stretch without rows after it has no head to
     * stand on, so its first cycle asks for a root at the newest timestamp the view has seen,
     * above its newest row, which the seal declines. The restart after it finds no timeline, or
     * the live marker, and rebuilds from the applied base, which the restatement guard refuses
     * while the base lacks {@code droppedDayTs}'s day, the one the run dropped, until that day goes
     * back. Its draws come from a sequence salted apart from the run's. Returns the job the view
     * refreshes on from there.
     *
     * @param isKeptPrefixSealed whether the departure derives the frontier's state by a restore of
     *                           a root inside the frontier's frame: see departNewestRowsOverSparseRoots
     */
    private LiveViewRefreshJob departOverSparseRootsFailingTheFrontierSeal(
            Rnd rnd,
            LiveViewRefreshJob job,
            @Nullable LongHashSet usedTs,
            int symCount,
            boolean withWhere,
            int frameMinutes,
            boolean isKeptPrefixSealed,
            long droppedDayTs,
            String oracleOrderedSql,
            String viewOrderedSql
    ) throws Exception {
        final Rnd episodeRnd = new Rnd(rnd.getSeed0() ^ FRONTIER_SEAL_FAILURE_SALT0, rnd.getSeed1() ^ FRONTIER_SEAL_FAILURE_SALT1);
        // Over a filtered view, whose stretch below commits rows the filter drops and so reads no
        // timeline, the retire that answers the failed seal meets a lock on _timeline: the test's
        // first such retire does, and one in two after it, drawn from a sequence salted apart from
        // the episode's.
        final boolean isTimelineLockedForTheRetire = withWhere
                && (sparseFrontierSealKeptTimelineRetires == 0 || keptTimelineRetireRnd(episodeRnd).nextBoolean());
        Assert.assertFalse(
                departNewestRowsOverSparseRoots(
                        episodeRnd,
                        job,
                        engine.verifyTableName("base"),
                        usedTs,
                        symCount,
                        withWhere,
                        frameMinutes,
                        SPARSE_DEPARTURE_FRONTIER_SEAL,
                        isKeptPrefixSealed,
                        true,
                        isTimelineLockedForTheRetire
                )
        );
        if (isTimelineLockedForTheRetire) {
            restartRebuildsOverTheMarkerBefore = countRestartRebuildsOverTheMarker();
        }
        final long declinesBefore = job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT);
        commitRowlessStretch(job, usedTs, withWhere, false, false);
        Assert.assertTrue(
                "the seal declines a root above the newest row of a view the departure left with no head",
                job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT) > declinesBefore
        );
        assertNoRefreshFaults("lv");
        coverage.absorb(job);
        Misc.free(job);
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
        final LiveViewRefreshJob restartedJob = new LiveViewRefreshJob(0, engine, 1);
        driveRefreshToQuiescence(restartedJob);
        if (isTimelineLockedForTheRetire) {
            Assert.assertEquals(
                    "the restart rebuilds over the marker the retire kept",
                    restartRebuildsOverTheMarkerBefore + 1,
                    countRestartRebuildsOverTheMarker()
            );
        }
        if (droppedDayTs != Numbers.LONG_NULL
                && queryTimestamp("SELECT max(ts) FROM base WHERE ts >= " + droppedDayTs + " AND ts < " + (droppedDayTs + Micros.DAY_MICROS)) == Numbers.LONG_NULL) {
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertEquals("rebuild_blocked", LiveViewCheckpointRecoveryPhase.routeName(instance.getCheckpointRestoreRoute()));
            TestUtils.assertContains(
                    instance.getCheckpointRecoveryReason(),
                    "would drop rows it retains [cause="
                            + (isTimelineLockedForTheRetire ? "prefix preservation repair marker present" : "timeline is absent")
                            + "]: "
            );
            if (!isTimelineLockedForTheRetire) {
                blockedTimelineAbsentSparseFrontierSeals++;
            }
            return restoreTheDroppedDayAndRestart(restartedJob, droppedDayTs, oracleOrderedSql, viewOrderedSql);
        }
        assertRebuiltFromAppliedBase("lv");
        TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracleOrderedSql, viewOrderedSql, LOG, true);
        return restartedJob;
    }

    /**
     * The sparse-root counterpart of {@link #departNewestRows}, for a RANGE-frame view. Commits a
     * kept and a departed batch of four rows on a day of their own above every row the base and
     * the view hold, under {@link #beginSparseCheckpointCadence}, so a root seals only where the
     * clock has moved past the duration cadence, then moves the departed batch out of the view. The departed batch sits
     * more than twice the frame's width above the kept one, so a rebuild warming its state from one
     * frame below the departed rows reads none of the rows a resume from a root under them reads,
     * and the plan prices the rebuild below the resume: the head-miss rebuild repairs the departure
     * through its end-of-table splice. Asserts the splice ends the way {@code shape} names:
     * <ul>
     *     <li>{@link #SPARSE_DEPARTURE_FRONTIER_SEAL}: the two batches commit together, so no
     *     root sits at the kept batch's newest row, whether the cadence seals the departed batch or
     *     not, and the departure emits no row: the rebuild derives the state at the new frontier,
     *     truncates a root the departed batch sealed, and seals the frontier. With
     *     {@code isKeptPrefixSealed} the kept batch's first three rows commit alone past the
     *     cadence, so a root sits at the third, inside the frame of the fourth, half the frame's
     *     width above it, and the rebuild restores it and folds the fourth row onto it; otherwise
     *     no root sits inside that frame, and the rebuild warms the frame from its floor. With
     *     {@code isFrontierSealFailed} the seal fails instead, once, and the rebuild retires the
     *     timeline;</li>
     *     <li>{@link #SPARSE_DEPARTURE_TRUNCATE}: a root sits at the departed batch's newest row,
     *     and the departure either keeps the departed batch's first row, which the rebuild emits
     *     below that root, or, over a filtered view, emits no row above a root at the kept batch's
     *     newest row: the rebuild truncates the timeline at the new frontier;</li>
     *     <li>{@link #SPARSE_DEPARTURE_KEEP}: a root sits at the kept batch's newest row and none
     *     above it, and the departure emits no row: the rebuild keeps the timeline.</li>
     * </ul>
     * A resume from a root at the kept batch's newest row reads more than the rebuild only over
     * rows the filter drops above that root, so only a filtered view takes those shapes. The rows
     * leave as they do in departNewestRows. A rebuild that emits no row over the root at the new
     * frontier puts the head back on that root, by a re-stamp that names no root.
     *
     * @param frameMinutes                 the width of the view's RANGE frame, in minutes
     * @param isKeptPrefixSealed           whether a root seals the kept batch's first three rows alone, for
     *                                     {@link #SPARSE_DEPARTURE_FRONTIER_SEAL}
     * @param isFrontierSealFailed         whether the frontier seal of {@link #SPARSE_DEPARTURE_FRONTIER_SEAL}
     *                                     fails
     * @param isTimelineLockedForTheRetire whether the retire that answers the failed frontier seal
     *                                     meets a lock on {@code _timeline}, which keeps the
     *                                     timeline and its live marker
     * @return true when the rebuild emitted no row and re-stamped the head on the root at the new
     * frontier: {@link #SPARSE_DEPARTURE_KEEP}, and {@link #SPARSE_DEPARTURE_TRUNCATE} keeping no
     * departed row
     */
    private boolean departNewestRowsOverSparseRoots(
            Rnd rnd,
            LiveViewRefreshJob job,
            TableToken baseToken,
            @Nullable LongHashSet usedTs,
            int symCount,
            boolean withWhere,
            int frameMinutes,
            int shape,
            boolean isKeptPrefixSealed,
            boolean isFrontierSealFailed,
            boolean isTimelineLockedForTheRetire
    ) throws Exception {
        assert !isFrontierSealFailed || shape == SPARSE_DEPARTURE_FRONTIER_SEAL;
        assert !isTimelineLockedForTheRetire || isFrontierSealFailed;
        assert !isKeptPrefixSealed || shape == SPARSE_DEPARTURE_FRONTIER_SEAL;
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        Assert.assertEquals(LiveViewState.SEED_STATE_ACTIVE, instance.getStateReader().getSeedState());
        final long baseMaxTs = queryTimestamp("SELECT max(ts) FROM base");
        Assert.assertNotEquals(Numbers.LONG_NULL, baseMaxTs);
        final boolean isZeroRowTruncate = shape == SPARSE_DEPARTURE_TRUNCATE && withWhere && rnd.nextBoolean();
        // One truncating departure in two crashes between its frontier truncate's marker and the
        // truncate's publication, drawn apart from the run's sequence.
        final boolean isCrashDrawn = shape == SPARSE_DEPARTURE_TRUNCATE && frontierTruncateCrashRnd(rnd).nextBoolean();
        final boolean isKeptBatchSealed = shape == SPARSE_DEPARTURE_KEEP || isZeroRowTruncate;
        final boolean isDepartedBatchSealed = shape == SPARSE_DEPARTURE_TRUNCATE
                || (shape == SPARSE_DEPARTURE_FRONTIER_SEAL && rnd.nextBoolean());
        // While the owed-root arm is on, the keeping shape over a filtered view keeps the departed
        // batch's first row, which the rebuild emits above the kept batch's root, the splice's
        // newest: the view's new frontier, a root at which the rebuild's closing seal owes and
        // fails to write. Every other such departure, from the second, comes after two failed
        // seals in a row, so its closing seal is the third and pauses the seal. Counted rather
        // than drawn, so the run's random sequence draws what it drew before.
        final boolean isOwedRootDeparture = isDepartedOwedRootArm && shape == SPARSE_DEPARTURE_KEEP && withWhere;
        final boolean isOwedRootPaused = isOwedRootDeparture && departedOwedRootHeadMissEpisodes++ % 2 == 1;
        final boolean isRowKept = (shape == SPARSE_DEPARTURE_TRUNCATE && !isZeroRowTruncate) || isOwedRootDeparture;
        // The rows start the day above the base table's newest row, so that day's partition holds
        // them alone: the plan prices a scan of a partition as its share of the rows spread from
        // the partition's floor to its newest row. That day is also above the view's runtime
        // frontier, which a correction that moved the view's newest rows out leaves above them,
        // and above its START FROM boundary, which the run's replace bands can leave above every
        // base row.
        final long floorTs = Math.max(
                Math.max(baseMaxTs, instance.getLatestSeenTs()),
                instance.getDefinition().getViewLowerBoundTimestamp()
        );
        long keptFromTs = floorTs - floorTs % Micros.DAY_MICROS + Micros.DAY_MICROS + 1 + rnd.nextLong(Micros.SECOND_MICROS);
        final long frameMicros = frameMinutes * Micros.MINUTE_MICROS;
        if (shape == SPARSE_DEPARTURE_FRONTIER_SEAL && !isKeptPrefixSealed && hasCheckpointTimeline(instance)) {
            // The rebuild warms the frame of the kept batch's newest row from its floor only while
            // no root sits inside it, and the frame reaches the day below by up to its width: a
            // root the run sealed there moves the batch up by that width.
            final LongList ladder = snapshotCheckpointLadder(instance);
            if (ladder.size() > 0 && ladder.getQuick(ladder.size() - 2) >= keptFromTs - frameMicros) {
                keptFromTs += frameMicros;
            }
        }
        final long[] keptTs = new long[4];
        final StringSink values = new StringSink();
        if (isKeptPrefixSealed) {
            // The draws of one four-row batch, in their order. The first three rows commit alone
            // past the cadence, so a root seals the third, and the fourth sits half the frame's
            // width above it, inside its frame. The plan prices a scan of the day's partition as
            // its share of the rows spread from the day's start, so the gap is what makes the
            // resume from that root read a row more than the rebuild of the departed rows.
            final long[] keptPrefixTs = new long[3];
            drawEpisodeBatch(rnd, values, keptFromTs, usedTs, symCount, keptPrefixTs, null);
            setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
            execute("INSERT INTO base (ts, sym, i, x) VALUES " + values);
            drainWalQueue();
            driveRefreshToQuiescence(job);
            Assert.assertEquals("a root seals the kept batch's third row", keptPrefixTs[2], instance.getHeadCheckpointMaxTs());
            values.clear();
            final long[] keptLastTs = new long[1];
            drawEpisodeBatch(rnd, values, keptPrefixTs[2] + frameMicros / 2, usedTs, symCount, keptLastTs, null);
            System.arraycopy(keptPrefixTs, 0, keptTs, 0, keptPrefixTs.length);
            keptTs[3] = keptLastTs[0];
        } else {
            drawEpisodeBatch(rnd, values, keptFromTs, usedTs, symCount, keptTs, null);
        }
        if (isKeptBatchSealed) {
            setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
            execute("INSERT INTO base (ts, sym, i, x) VALUES " + values);
            drainWalQueue();
            driveRefreshToQuiescence(job);
            Assert.assertEquals("a root seals the kept batch", keptTs[3], instance.getHeadCheckpointMaxTs());
            values.clear();
        }
        final long departedFromTs = keptTs[3] + (2L * frameMinutes + 1) * Micros.MINUTE_MICROS + 1 + rnd.nextLong(Micros.SECOND_MICROS);
        if (isKeptBatchSealed) {
            // Rows the filter drops above the kept batch's root, just below the frame under the
            // departed batch: a resume from that root reads them, and the rebuild does not. Ahead
            // of the departed rows in the commit, which the drain then takes in timestamp order.
            for (int k = 2; k >= 1; k--) {
                long t = departedFromTs - frameMinutes * Micros.MINUTE_MICROS - k * 1_000L;
                while (usedTs != null && !usedTs.add(t)) {
                    t--;
                }
                appendEpisodeRow(values, t, rnd.nextInt(symCount), -1, rnd.nextDouble() * 1000.0);
            }
        }
        final long[] departedTs = new long[4];
        final int[] departedSym = new int[4];
        drawEpisodeBatch(rnd, values, departedFromTs, usedTs, symCount, departedTs, departedSym);
        if (isDepartedBatchSealed) {
            setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
        }
        execute("INSERT INTO base (ts, sym, i, x) VALUES " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
        if (isDepartedBatchSealed) {
            Assert.assertEquals("a root seals the departed batch", departedTs[3], instance.getHeadCheckpointMaxTs());
        } else if (isKeptBatchSealed) {
            Assert.assertEquals("no root seals the departed batch", keptTs[3], instance.getHeadCheckpointMaxTs());
        } else if (isKeptPrefixSealed) {
            Assert.assertEquals("no root seals the kept batch's newest row or the departed batch", keptTs[2], instance.getHeadCheckpointMaxTs());
        }
        // A frontier-sealing departure that drew no root over its departed batch may still meet
        // one there: the duration cadence can have run out before the batches committed. A root
        // there is the one root above the kept batch, which the frontier truncate drops.
        final boolean isDepartedRootSealed = instance.getHeadCheckpointMaxTs() == departedTs[3];

        final boolean isUpsert = usedTs == null && withWhere;
        final long keptI = 1 + rnd.nextInt(1000);
        final double keptX = rnd.nextDouble() * 1000.0;
        LOG.info().$("LV fuzz sparse departure: shape=").$(shape).$(", keptHighTs=").$ts(keptTs[3])
                .$(", departedLowTs=").$ts(departedTs[0]).$(", departedHighTs=").$ts(departedTs[3])
                .$(", keptSealed=").$(isKeptBatchSealed).$(", departedSealed=").$(isDepartedBatchSealed)
                .$(", rowKept=").$(isRowKept).$(", upsert=").$(isUpsert).$();
        // Over a filtered view, a shape whose rebuild emits no row over a root at the new frontier
        // may meet a seal its failures paused: three cycles without rows past the cadence fail
        // their seals, which the run draws apart from its own sequence.
        final boolean isPausedAhead = withWhere
                && (shape == SPARSE_DEPARTURE_KEEP || isZeroRowTruncate)
                && sparseDeparturePauseRnd(rnd).nextBoolean()
                && !isOwedRootDeparture;
        final long sealFailuresBeforeThePause = instance.getCheckpointSealFailures();
        if (isPausedAhead) {
            pauseTheSealAheadOfADeparture(job, usedTs, instance);
        } else if (isOwedRootPaused) {
            failRowlessSealsAheadOfADeparture(job, usedTs, instance, 2);
        }
        final long owedRootMarkerGeneration = publishedCheckpointGeneration(instance);
        final long truncatesBefore = job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT);
        final long keepsBefore = job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_ZERO_ROW_KEEP_COUNT);
        final long retiresBefore = job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_ZERO_ROW_RETIRE_COUNT);
        final long sealsBefore = job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_ZERO_ROW_FRONTIER_SEAL_COUNT);
        final long restoresBefore = job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_FRONTIER_STATE_RESTORE_COUNT);
        final long warmUpsBefore = job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_FRONTIER_STATE_WARM_UP_COUNT);
        final long headMissRepairsBefore = countHeadMissRepairs(job);
        final long resetsBefore = instance.getCheckpointTimelineResets();
        final long sealFailuresBefore = instance.getCheckpointSealFailures();
        // While the cleared-head arm is on, a filtered view's departure that emits no row over the
        // root at the kept batch's newest row fails the read of the view's table the rebuild's
        // closing seal makes, when that seal only re-stamps the head on the root at the new
        // frontier: see awaitTheClearedHead. Counted rather than drawn, so the run's random
        // sequence draws what it drew before.
        final boolean isClearedHeadDeparture = isClearedHeadArm
                && withWhere
                && !isRowKept
                && !isCrashDrawn
                && (shape == SPARSE_DEPARTURE_KEEP || isZeroRowTruncate);
        if (isClearedHeadDeparture) {
            hooks(job).setSimulateHeadMissHeadSealStart(() -> failTheReadOfARestamp(instance));
        }
        if (isFrontierSealFailed || isOwedRootDeparture) {
            // After the frontier truncate or the splice, ahead of the seal: the seal's publication
            // fails, and the lock, when drawn, holds _timeline for the retire that answers it.
            hooks(job).setSimulateHeadMissHeadSealStart(() -> {
                job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
                if (isTimelineLockedForTheRetire) {
                    checkpointFaults.lockTheTimeline();
                }
            });
        }
        runUnderTheFrontierTruncateCrash(isCrashDrawn, () -> {
            if (isUpsert) {
                values.clear();
                for (int k = 0; k < departedTs.length; k++) {
                    if (k == 0 && isRowKept) {
                        appendEpisodeRow(values, departedTs[k], departedSym[k], keptI, keptX);
                    } else {
                        appendEpisodeRow(values, departedTs[k], departedSym[k], -1, 0.0);
                    }
                }
                execute("INSERT INTO base (ts, sym, i, x) VALUES " + values);
            } else {
                try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                    if (isRowKept) {
                        appendRow(walWriter, departedTs[0], departedSym[0], keptI, keptX);
                    }
                    walWriter.commitWithParams(departedTs[0], departedTs[3] + 1, WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE);
                }
            }
            drainWalQueue();
            driveRefreshToQuiescence(job);
        });
        if (isCrashDrawn) {
            Assert.assertTrue("the crash must interrupt the frontier truncate", checkpointFaults.hasCrashed());
            headMissFrontierTruncateCrashes++;
        }
        Assert.assertEquals("the head-miss rebuild repairs the departure", headMissRepairsBefore + 1, countHeadMissRepairs(job));
        if (isClearedHeadDeparture) {
            hooks(job).setSimulateHeadMissHeadSealStart(null);
            checkpointFaults.disarmTheViewTableRead();
        }
        if (isOwedRootDeparture) {
            hooks(job).setSimulateHeadMissHeadSealStart(null);
            job.setCheckpointTimelineTestFailureStage(0);
        }
        if (isFrontierSealFailed) {
            hooks(job).setSimulateHeadMissHeadSealStart(null);
            job.setCheckpointTimelineTestFailureStage(0);
            Assert.assertEquals(
                    "the retire that answers the failed seal meets the lock once",
                    isTimelineLockedForTheRetire ? 1 : 0,
                    checkpointFaults.unlockTheTimeline()
            );
        }
        Assert.assertEquals(
                "the splice truncates at the frontier",
                shape == SPARSE_DEPARTURE_TRUNCATE || (shape == SPARSE_DEPARTURE_FRONTIER_SEAL && isDepartedRootSealed) ? 1 : 0,
                job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT) - truncatesBefore
        );
        Assert.assertEquals(
                "the splice keeps the timeline over a rebuild that emitted no row",
                shape == SPARSE_DEPARTURE_KEEP && !isOwedRootDeparture ? 1 : 0,
                job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_ZERO_ROW_KEEP_COUNT) - keepsBefore
        );
        Assert.assertEquals(
                "no departure retires the timeline for want of the frontier's state",
                retiresBefore,
                job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_ZERO_ROW_RETIRE_COUNT)
        );
        Assert.assertEquals(
                "the rebuild derives the frontier's state by a restore of the root inside its frame",
                isKeptPrefixSealed ? 1 : 0,
                job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_FRONTIER_STATE_RESTORE_COUNT) - restoresBefore
        );
        Assert.assertEquals(
                "the rebuild derives the frontier's state by a warm-up of its frame",
                shape == SPARSE_DEPARTURE_FRONTIER_SEAL && !isKeptPrefixSealed ? 1 : 0,
                job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_FRONTIER_STATE_WARM_UP_COUNT) - warmUpsBefore
        );
        Assert.assertEquals(
                "and seals it at the frontier",
                shape == SPARSE_DEPARTURE_FRONTIER_SEAL && !isFrontierSealFailed ? 1 : 0,
                job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_ZERO_ROW_FRONTIER_SEAL_COUNT) - sealsBefore
        );
        final long frontierTs = isRowKept ? departedTs[0] : keptTs[3];
        Assert.assertEquals("the view's new frontier", frontierTs, queryTimestamp("SELECT max(ts) FROM lv"));
        if (isFrontierSealFailed) {
            // The seal's failure counts, and the frontier holds no root a restart could restore, so
            // the rebuild retires the timeline, which takes its repair marker with it unless a lock
            // keeps _timeline.
            Assert.assertEquals("the seal fails", sealFailuresBefore + 1, instance.getCheckpointSealFailures());
            Assert.assertEquals("the timeline retires", resetsBefore + 1, instance.getCheckpointTimelineResets());
            if (isTimelineLockedForTheRetire) {
                // The lock keeps the generation, the splice's or the truncate's, and the retire the
                // marker beside it, which names the generation one below it: a live marker.
                Assert.assertTrue("the locked timeline stays", hasCheckpointTimeline(instance));
                assertRepairMarker(instance, publishedCheckpointGeneration(instance) - 1, false);
                sparseFrontierSealKeptTimelineRetires++;
            } else {
                Assert.assertFalse("the retired timeline is gone", hasCheckpointTimeline(instance));
                try (Path dir = checkpointsDirOf(instance)) {
                    Assert.assertFalse("the retire takes the marker with it", LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
                }
            }
            Assert.assertEquals("no head stands", Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
        } else {
            // A truncate that keeps no root retires the timeline too, and the seal at the frontier
            // opens a fresh one: a view with no root below its new frontier. Either way, what a
            // restart restores from is the newest root, at the frontier, counting every row of the
            // view.
            if (shape == SPARSE_DEPARTURE_KEEP) {
                Assert.assertEquals("the timeline stays", resetsBefore, instance.getCheckpointTimelineResets());
            }
            final LongList ladder = snapshotCheckpointLadder(instance);
            if (isOwedRootDeparture) {
                Assert.assertEquals("the splice's newest root is the kept batch's", keptTs[3], ladder.getQuick(ladder.size() - 2));
                Assert.assertEquals("the rebuild cleared the head", Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(departedTs[3], instance.getLatestSeenTs());
                awaitTheDepartedOwedRoot(instance, frontierTs, owedRootMarkerGeneration, sealFailuresBefore, isOwedRootPaused, true);
                return false;
            }
            Assert.assertEquals("the newest root sits at the new frontier", frontierTs, ladder.getQuick(ladder.size() - 2));
            Assert.assertEquals("the newest root counts every row of the view", countRows("lv"), ladder.getQuick(ladder.size() - 1));
            if (shape == SPARSE_DEPARTURE_FRONTIER_SEAL) {
                // A seal of the state the rebuild derived at the frontier, which names its root.
                Assert.assertEquals("the head sits on the root sealed at the new frontier", frontierTs, instance.getHeadCheckpointMaxTs());
                Assert.assertNotEquals("a seal names its root", Numbers.LONG_NULL, instance.getHeadCheckpointRootId());
            } else if (!isRowKept) {
                if (isClearedHeadDeparture && awaitTheClearedHead(job, instance, true)) {
                    // The re-stamp writes nothing, so a pause the departure met let it through, and
                    // the failed read it declined on records no failure.
                    if (isPausedAhead) {
                        Assert.assertEquals(sealFailuresBeforeThePause + 3, instance.getCheckpointSealFailures());
                        Assert.assertTrue("the pause stands", instance.isSealOnCooldown(currentMicros));
                    }
                    return false;
                }
                Assert.assertEquals("the head goes back on the root at the new frontier", frontierTs, instance.getHeadCheckpointMaxTs());
                Assert.assertEquals("by a re-stamp, which names no root", Numbers.LONG_NULL, instance.getHeadCheckpointRootId());
                if (isPausedAhead) {
                    // The re-stamp writes nothing, so the pause lets it through, and it records
                    // neither a failure nor the success that would end the streak.
                    Assert.assertEquals(sealFailuresBeforeThePause + 3, instance.getCheckpointSealFailures());
                    Assert.assertEquals(3, instance.getSealFailureStreak());
                    Assert.assertTrue("the pause stands", instance.isSealOnCooldown(currentMicros));
                    restampsInsideThePause++;
                }
                return true;
            }
        }
        return false;
    }

    /**
     * Three commits whose rows the view's filter drops, each past any duration cadence a run
     * draws, with the writer failing every publication: each cycle seals or republishes the head
     * and fails, and the third failure in a row pauses the seal over the intact timeline, which
     * the writer then stops failing.
     */
    private void pauseTheSealAheadOfADeparture(
            LiveViewRefreshJob job,
            @Nullable LongHashSet usedTs,
            LiveViewInstance instance
    ) throws Exception {
        final long sealFailures = instance.getCheckpointSealFailures();
        final long resets = instance.getCheckpointTimelineResets();
        final long headMaxTs = instance.getHeadCheckpointMaxTs();
        job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
        try {
            long ts = Math.max(queryTimestamp("SELECT max(ts) FROM base"), instance.getLatestSeenTs());
            for (int c = 1; c <= 3; c++) {
                ts = commitRowTheFilterDrops(job, usedTs, ts);
                Assert.assertEquals("each cycle fails its seal", sealFailures + c, instance.getCheckpointSealFailures());
            }
        } finally {
            job.setCheckpointTimelineTestFailureStage(0);
        }
        Assert.assertEquals("the third failure pauses", resets, instance.getCheckpointTimelineResets());
        Assert.assertEquals(headMaxTs, instance.getHeadCheckpointMaxTs());
        Assert.assertTrue(instance.isSealOnCooldown(currentMicros));
    }

    /**
     * {@code failures} commits whose rows the view's filter drops, each past any duration cadence a
     * run draws, with the writer failing every publication: each cycle seals the rows above the head
     * and fails, and the writer then stops failing. Fewer than three failures in a row retire nothing
     * and arm no pause.
     */
    private void failRowlessSealsAheadOfADeparture(
            LiveViewRefreshJob job,
            @Nullable LongHashSet usedTs,
            LiveViewInstance instance,
            int failures
    ) throws Exception {
        assert failures < 3;
        final long sealFailures = instance.getCheckpointSealFailures();
        final int streak = instance.getSealFailureStreak();
        final long resets = instance.getCheckpointTimelineResets();
        final long headMaxTs = instance.getHeadCheckpointMaxTs();
        job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
        try {
            long ts = Math.max(queryTimestamp("SELECT max(ts) FROM base"), instance.getLatestSeenTs());
            for (int c = 1; c <= failures; c++) {
                ts = commitRowTheFilterDrops(job, usedTs, ts);
                Assert.assertEquals("each cycle fails its seal", sealFailures + c, instance.getCheckpointSealFailures());
            }
        } finally {
            job.setCheckpointTimelineTestFailureStage(0);
        }
        Assert.assertEquals(streak + failures, instance.getSealFailureStreak());
        Assert.assertEquals("no failure retires", resets, instance.getCheckpointTimelineResets());
        Assert.assertEquals(headMaxTs, instance.getHeadCheckpointMaxTs());
        Assert.assertFalse(instance.isSealOnCooldown(currentMicros));
    }

    /**
     * Asserts what a departure the cleared-head arm drew left, once the arm failed the read of its
     * closing re-stamp: the head clear with no write time and nothing fed since, over a timeline
     * whose newest root sits at the view's newest row and counts every row of it, with the repair
     * marker resolved. Leaves that root for the stretch without rows after the departure to put the
     * head back on: see {@link #assertTheStretchPublishedTheClearedHead}.
     *
     * @return false when the departure's closing seal re-stamped nothing, so the arm failed no read
     */
    private boolean awaitTheClearedHead(LiveViewRefreshJob job, LiveViewInstance instance, boolean isHeadMiss) throws Exception {
        final int failedReads = checkpointFaults.consumeFailedViewTableReads();
        Assert.assertTrue("the arm fails one read at most", failedReads <= 1);
        if (failedReads == 0) {
            return false;
        }
        final long frontierTs = queryTimestamp("SELECT max(ts) FROM lv");
        Assert.assertEquals("the declined re-stamp leaves the head clear", Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
        Assert.assertEquals(Numbers.LONG_NULL, instance.getLastCheckpointWrittenUs());
        Assert.assertEquals("nothing fed since", Long.MAX_VALUE, instance.getMinSeenTsSinceCheckpoint());
        Assert.assertTrue("the newest timestamp the view has seen stands above its newest row", instance.getLatestSeenTs() > frontierTs);
        Assert.assertTrue("the timeline stays", hasCheckpointTimeline(instance));
        final LongList ladder = snapshotCheckpointLadder(instance);
        Assert.assertEquals("its newest root sits at the view's newest row", frontierTs, ladder.getQuick(ladder.size() - 2));
        Assert.assertEquals("and counts every row of the view", countRows("lv"), ladder.getQuick(ladder.size() - 1));
        try (Path dir = checkpointsDirOf(instance)) {
            Assert.assertFalse("the repair resolved its marker", LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
        }
        LOG.info().$("LV fuzz departure left the head clear [rootTs=").$ts(frontierTs).$(", headMiss=").$(isHeadMiss).I$();
        clearedHeadRootTs = frontierTs;
        clearedHeadPublicationsBefore = job.getStatsForTest().get(LiveViewRefreshStats.CLEARED_HEAD_PUBLICATION_COUNT);
        isClearedHeadHeadMiss = isHeadMiss;
        clearedHeadEpisodes++;
        if (isHeadMiss) {
            clearedHeadHeadMissEpisodes++;
        }
        return true;
    }

    /**
     * Asserts the stretch of rows the view's filter drops put the head the departure before it left
     * clear back on the root at the view's newest row: one publication of the cleared head, on the
     * stretch's first row, which is due at once, and the head's own publications on the rows after
     * it, each past any duration cadence a run draws, so the head sits at the commit of the last.
     */
    private void assertTheStretchPublishedTheClearedHead(LiveViewRefreshJob job, LiveViewInstance instance) throws Exception {
        Assert.assertEquals("the stretch puts the head back on the root at the view's newest row", clearedHeadRootTs, instance.getHeadCheckpointMaxTs());
        Assert.assertEquals(clearedHeadRootTs, queryTimestamp("SELECT max(ts) FROM lv"));
        Assert.assertEquals(
                "at the commit of the stretch's last row",
                instance.getStateReader().getLvConsumedSeqTxn(),
                instance.getHeadCheckpointBaseSeqTxn()
        );
        Assert.assertEquals(
                "by one publication of the cleared head",
                clearedHeadPublicationsBefore + 1,
                job.getStatsForTest().get(LiveViewRefreshStats.CLEARED_HEAD_PUBLICATION_COUNT)
        );
        clearedHeadLandings++;
        if (isClearedHeadHeadMiss) {
            clearedHeadHeadMissLandings++;
        }
        clearedHeadRootTs = Numbers.LONG_NULL;
    }

    /**
     * Fails the next read of the view's table, from a repair's closing seal, when that seal only
     * re-stamps the head: the newest root of the generation the repair published sits at the newest
     * row the view's table holds and counts every row of it.
     */
    private void failTheReadOfARestamp(LiveViewInstance instance) {
        final long rows;
        final long maxTs;
        try (TableReader reader = engine.getReader(instance.getLiveViewToken())) {
            rows = reader.size();
            maxTs = reader.getMaxTimestamp();
        }
        if (rows > 0 && hasCheckpointTimeline(instance)) {
            final LongList ladder = snapshotCheckpointLadder(instance);
            if (ladder.size() > 0
                    && ladder.getQuick(ladder.size() - 2) == maxTs
                    && ladder.getQuick(ladder.size() - 1) == rows) {
                checkpointFaults.failTheNextViewTableRead(instance);
            }
        }
    }

    /**
     * Asserts what a departure the owed-root arm drew left: its closing seal, which owes
     * {@code owedRootTs}, the view's newest row, a root, failed once, the third failure in a row
     * when {@code isPaused}, which pauses the seal, and the repair kept the live marker naming
     * {@code markerGeneration}, with the newest timestamp the view has seen above that row. Leaves
     * the owed root for the stretch without rows after the departure to land: see
     * {@link #assertTheStretchLandedTheDepartedOwedRoot}.
     */
    private void awaitTheDepartedOwedRoot(
            LiveViewInstance instance,
            long owedRootTs,
            long markerGeneration,
            long sealFailuresBefore,
            boolean isPaused,
            boolean isHeadMiss
    ) throws Exception {
        Assert.assertEquals("the view's new frontier", owedRootTs, queryTimestamp("SELECT max(ts) FROM lv"));
        Assert.assertTrue("the newest timestamp the view has seen stands above it", instance.getLatestSeenTs() > owedRootTs);
        Assert.assertEquals("the closing seal fails", sealFailuresBefore + 1, instance.getCheckpointSealFailures());
        Assert.assertEquals(isPaused ? 3 : 1, instance.getSealFailureStreak());
        Assert.assertEquals("a third failure in a row pauses the seal", isPaused, instance.isSealOnCooldown(currentMicros));
        assertRepairMarker(instance, markerGeneration, false);
        assertNoCheckpointRootAboveTheNewestRow(instance);
        LOG.info().$("LV fuzz departure owes a root [owedRootTs=").$ts(owedRootTs).$(", headMiss=").$(isHeadMiss)
                .$(", paused=").$(isPaused).I$();
        departedOwedRootTs = owedRootTs;
        departedOwedRootMarkerGeneration = markerGeneration;
        departedOwedRootSeqTxn = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("base"));
        isDepartedOwedRootPaused = isPaused;
        isDepartedOwedRootHeadMiss = isHeadMiss;
    }

    /**
     * Asserts the stretch of {@code inOrderRows} in-order rows the view's filter drops, each past
     * any duration cadence a run draws and so past any pause, landed the root the departure before
     * it owed the view's newest row: the head sits there at the commit of the stretch's last row,
     * the marker reads as stale, the failure streak ended, and the older superblock slot holds the
     * splice, at the departure's commit, after one row, and the publication of the row before the
     * last after more.
     */
    private void assertTheStretchLandedTheDepartedOwedRoot(LiveViewInstance instance, int inOrderRows) throws Exception {
        final long consumedSeqTxn = instance.getStateReader().getLvConsumedSeqTxn();
        Assert.assertEquals("the stretch lands the owed root at the view's newest row", departedOwedRootTs, instance.getHeadCheckpointMaxTs());
        Assert.assertEquals(departedOwedRootTs, queryTimestamp("SELECT max(ts) FROM lv"));
        Assert.assertEquals("at the commit of the stretch's last row", consumedSeqTxn, instance.getHeadCheckpointBaseSeqTxn());
        Assert.assertEquals("the stretch's rows follow the departure", departedOwedRootSeqTxn + inOrderRows, consumedSeqTxn);
        Assert.assertEquals(
                "the WAL floor moves past the departure's commit",
                inOrderRows == 1 ? departedOwedRootSeqTxn : consumedSeqTxn - 1,
                instance.getCheckpointTimelineWalPurgeFloor()
        );
        Assert.assertEquals("the root that lands ends the failure streak", 0, instance.getSealFailureStreak());
        assertRepairMarker(instance, departedOwedRootMarkerGeneration, true);
        departedOwedRootLandings++;
        if (isDepartedOwedRootPaused) {
            departedOwedRootLandingsAfterThePause++;
        }
        if (isDepartedOwedRootHeadMiss) {
            departedOwedRootHeadMissLandings++;
            if (isDepartedOwedRootPaused) {
                departedOwedRootHeadMissLandingsAfterThePause++;
            }
        }
        departedOwedRootTs = Numbers.LONG_NULL;
    }

    /**
     * Three commits whose rows the view's filter drops, each past any duration cadence a run
     * draws. A restart put the head and the newest timestamp the view has seen at the newest
     * row it holds, so none of them may fail a publication or retire the timeline.
     */
    private void commitRowlessCyclesAfterARestart(LiveViewRefreshJob job, @Nullable LongHashSet usedTs) throws Exception {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        final long sealFailures = instance.getCheckpointSealFailures();
        final long resets = instance.getCheckpointTimelineResets();
        long ts = Math.max(queryTimestamp("SELECT max(ts) FROM base"), instance.getLatestSeenTs());
        for (int c = 0; c < 3; c++) {
            ts = commitRowTheFilterDrops(job, usedTs, ts);
        }
        Assert.assertEquals("a cycle without rows failed a publication", sealFailures, instance.getCheckpointSealFailures());
        Assert.assertEquals("a cycle without rows retired the timeline", resets, instance.getCheckpointTimelineResets());
        assertNoCheckpointRootAboveTheNewestRow(instance);
    }

    /**
     * Two or three rows the view's filter drops, one a commit, each past any duration cadence a run
     * draws, at the first free timestamps above the root a departure left the head on and below the
     * newest timestamp the view has seen. Each one's resume anchors on that root and emits nothing,
     * and its splice publishes at the commit: the head must name the commit, no root may sit above
     * the newest row the view holds, and every splice must carry out of the catalogue the entries
     * the sweep before it proposed. The count derives from departureEpisodes and the timestamps
     * from the head root, so the run's random sequence draws what it drew before.
     */
    private void commitLateRowsTheFilterDrops(
            LiveViewRefreshJob job,
            @Nullable LongHashSet usedTs,
            LiveViewInstance instance,
            long headMaxTs
    ) throws Exception {
        final long latestSeenTs = instance.getLatestSeenTs();
        Set<Long> deadAfterTheFirst = null;
        long ts = headMaxTs + 1;
        for (int c = 0, n = 2 + departureEpisodes % 2; c < n; c++, ts++) {
            while (usedTs != null && !usedTs.add(ts)) {
                ts++;
            }
            if (ts >= latestSeenTs) {
                break;
            }
            setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
            execute("INSERT INTO base (ts, sym, i, x) VALUES (" + ts + "::timestamp, '" + SYMBOLS[0] + "', -1, 0.0)");
            drainWalQueue();
            driveRefreshToQuiescence(job);
            Assert.assertEquals(
                    "a late row the filter drops must move the head to its commit",
                    instance.getStateReader().getLvConsumedSeqTxn(),
                    instance.getHeadCheckpointBaseSeqTxn()
            );
            assertNoCheckpointRootAboveTheNewestRow(instance);
            if (deadAfterTheFirst == null) {
                deadAfterTheFirst = deadCatalogueEntries(instance);
            }
            lateRowlessHeadAdvances++;
        }
        if (deadAfterTheFirst != null) {
            final Set<Long> catalogue = catalogue(instance);
            for (long segmentId : deadAfterTheFirst) {
                Assert.assertFalse(
                        "a later splice must carry out the entries the first late row's sweep proposed, segmentId=" + segmentId,
                        catalogue.contains(segmentId)
                );
            }
        }
    }

    // One row the view's filter drops, above afterTs and every timestamp usedTs holds, committed
    // past any duration cadence a run draws and refreshed. Returns its timestamp.
    private long commitRowTheFilterDrops(LiveViewRefreshJob job, @Nullable LongHashSet usedTs, long afterTs) throws Exception {
        long ts = afterTs + 1;
        while (usedTs != null && !usedTs.add(ts)) {
            ts++;
        }
        setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
        execute("INSERT INTO base (ts, sym, i, x) VALUES (" + ts + "::timestamp, '" + SYMBOLS[0] + "', -1, 0.0)");
        drainWalQueue();
        driveRefreshToQuiescence(job);
        return ts;
    }

    /**
     * Commits that put no row through the view after a departure, each past any duration cadence
     * a run draws: one to three in-order rows the view's filter drops, above the newest timestamp
     * the view has seen; or, over a view with no filter, one column added to the base, which the
     * view takes by restoring its runtime from the timeline in place, or, once the departure
     * retired the timeline, a REPLACE_RANGE above every row that deletes nothing. When
     * {@code isLateRowStretchAllowed} and the departure left the head below the newest timestamp
     * the view has seen, every other such filtered stretch, counted by lateRowStretchCandidates,
     * commits the late rows of {@link #commitLateRowsTheFilterDrops} ahead of its in-order rows,
     * and the rest commit their in-order rows straight after the departure. A departure leaves the
     * newest timestamp the view has seen above the newest row it holds, where a cycle without rows
     * once sealed a root every later restart failed against. Asserts no root sits above that row,
     * and that a head the departure left below that timestamp, which {@code isHeadRestamped} says
     * a departure that emitted no row re-stamped, moved its base coordinate past the commit of any
     * late row. Over a filtered view nothing reached the window since that head, or since the last
     * late row re-stamped it, so the in-order rows must move it to the commit of the last of them,
     * on the root the departure left.
     */
    private void commitRowlessStretch(
            LiveViewRefreshJob job,
            @Nullable LongHashSet usedTs,
            boolean withWhere,
            boolean isHeadRestamped,
            boolean isLateRowStretchAllowed
    ) throws Exception {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        final long headMaxTs = instance.getHeadCheckpointMaxTs();
        final boolean isDepartureHead = headMaxTs != Numbers.LONG_NULL && headMaxTs < instance.getLatestSeenTs();
        // A root the departure owes the view's newest row, which its closing seal failed to land:
        // the stretch's in-order rows land it, and late rows would land it through their own
        // repairs' closing seals instead.
        final long owedRootTs = departedOwedRootTs;
        final boolean isLateRowStretch = withWhere && isDepartureHead && isLateRowStretchAllowed
                && owedRootTs == Numbers.LONG_NULL
                && lateRowStretchCandidates++ % 2 == 0;
        // The base coordinate the stretch must move the head past: the one the departure left, or
        // the commit of the last late row, which re-stamped the head there.
        long headBaseSeqTxn = instance.getHeadCheckpointBaseSeqTxn();
        LOG.info().$("LV fuzz rowless stretch: head=").$ts(headMaxTs).$(", latestSeen=").$ts(instance.getLatestSeenTs())
                .$(", filtered=").$(withWhere).$(", lateRows=").$(isLateRowStretch).$(", owedRoot=").$ts(owedRootTs).$();
        Assert.assertTrue("only a filtered view's departure owes a root", withWhere || owedRootTs == Numbers.LONG_NULL);
        if (withWhere) {
            if (isLateRowStretch) {
                commitLateRowsTheFilterDrops(job, usedTs, instance, headMaxTs);
                headBaseSeqTxn = instance.getHeadCheckpointBaseSeqTxn();
            }
            long ts = Math.max(queryTimestamp("SELECT max(ts) FROM base"), instance.getLatestSeenTs());
            final int inOrderRows = 1 + departureEpisodes % 3;
            for (int c = 0; c < inOrderRows; c++) {
                ts = commitRowTheFilterDrops(job, usedTs, ts);
            }
            if (owedRootTs != Numbers.LONG_NULL) {
                assertTheStretchLandedTheDepartedOwedRoot(instance, inOrderRows);
            }
            if (clearedHeadRootTs != Numbers.LONG_NULL) {
                assertTheStretchPublishedTheClearedHead(job, instance);
            }
        } else if (hasCheckpointTimeline(instance)) {
            setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
            execute("ALTER TABLE base ADD COLUMN rowless_stretch INT");
            drainWalQueue();
            driveRefreshToQuiescence(job);
        } else {
            // With no timeline to restore from, the column would have the view rebuilt from the
            // base. A REPLACE_RANGE above every row deletes nothing and puts no row through the
            // window instead: the departure that retired the timeline left no head, so the cycle
            // asks for a first root at the newest timestamp the view has seen, above its newest
            // row, and the seal declines it.
            setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
            final long lo = Math.max(queryTimestamp("SELECT max(ts) FROM base"), instance.getLatestSeenTs()) + 1;
            replaceBaseRange(lo, lo + 1, Double.NaN);
            drainWalQueue();
            driveRefreshToQuiescence(job);
        }
        assertNoCheckpointRootAboveTheNewestRow(instance);
        if (isDepartureHead) {
            Assert.assertTrue(
                    "the stretch must move the base coordinate of the head the departure left past any late row's commit",
                    instance.getHeadCheckpointBaseSeqTxn() > headBaseSeqTxn
            );
            if (withWhere) {
                Assert.assertEquals(
                        "the in-order rows the filter drops must move the head to their commit",
                        instance.getStateReader().getLvConsumedSeqTxn(),
                        instance.getHeadCheckpointBaseSeqTxn()
                );
                Assert.assertEquals(
                        "on the root the departure left, or the one it owed the view's newest row",
                        owedRootTs != Numbers.LONG_NULL ? owedRootTs : headMaxTs,
                        instance.getHeadCheckpointMaxTs()
                );
                if (!isLateRowStretch) {
                    inOrderOnlyHeadAdvances++;
                }
            }
            departureHeadAdvances++;
            if (isHeadRestamped) {
                restampedHeadAdvances++;
            }
        } else {
            Assert.assertFalse("a re-stamped head sits below the newest timestamp the view has seen", isHeadRestamped);
        }
    }

    // Every sparse departure that keeps its timeline ends on failTheOwedSealOfALateRow, so every
    // batch fails a closing seal over a root its repair owes. The episode after the departure that
    // keeps the timeline, which every batch runs, is shaped for the head-miss rebuild, so every
    // batch fails the closing seal of an end-of-table splice too. The restart after each turns on
    // the route its late row takes, on the failures it draws and on whether its run dropped a day,
    // and the two such episodes of a batch cannot reach every outcome, so the outcomes are logged.
    private void assertReachedAnOwedSealEpisode() {
        LOG.info().$("LV fuzz owed-seal episodes [episodes=").$(owedSealEpisodes)
                .$(", headMiss=").$(headMissOwedSealEpisodes)
                .$(", blocked=").$(blockedOwedSealEpisodes)
                .$(", restored=").$(restoredOwedSealEpisodes)
                .$(", retired=").$(retiredOwedSealEpisodes)
                .$(", blockedTimelineAbsent=").$(blockedTimelineAbsentOwedSealEpisodes)
                .$(", paused=").$(pausedOwedSealEpisodes)
                .$(", markerLiveInsideThePause=").$(markerLiveInsideThePauseOwedSealEpisodes)
                .$(", restoredAfterThePause=").$(restoredAfterThePauseOwedSealEpisodes)
                .$(", restoredAfterThePauseResumed=").$(restoredAfterThePauseResumedOwedSealEpisodes)
                .$(", restampsInsideThePause=").$(restampsInsideThePause).I$();
        Assert.assertTrue("no late row's repair left its marker live over a failed closing seal", owedSealEpisodes > 0);
        Assert.assertTrue(
                "no late row's head-miss rebuild left its marker live over a failed closing seal",
                headMissOwedSealEpisodes > 0
        );
    }

    // Every other owed-seal episode of the arm, from the first, ends on failTheSecondRepairsCommit,
    // so every batch of it, whose three RANGE-frame runs end on an owed-seal episode each, fails
    // the replacement commit of a second late row inside the window twice, counted rather than
    // drawn: once in the process and once by stopping the process. Whether over a day the run
    // dropped, and on which route, turns on the draws, so those are logged.
    private void assertReachedASecondRepairCommitFault() {
        LOG.info().$("LV fuzz second repair commit faults [episodes=").$(secondRepairCommitFaultEpisodes)
                .$(", inPlace=").$(secondRepairCommitFaultsInPlace)
                .$(", stops=").$(secondRepairCommitStops)
                .$(", refusedInPlace=").$(secondRepairRebuildsRefusedInPlace).I$();
        Assert.assertTrue(
                "no second late row inside the owed window failed its replacement commit in the process",
                secondRepairCommitFaultsInPlace > 0
        );
        Assert.assertTrue(
                "no second late row inside the owed window stopped the process at its replacement commit",
                secondRepairCommitStops > 0
        );
    }

    // The repair marker on disk names markerBaseGeneration, under the splice's generation one past
    // it, which a restore reads as live, or, when isStale, under a later one, which it reads as
    // stale.
    private void assertRepairMarker(LiveViewInstance instance, long markerBaseGeneration, boolean isStale) {
        try (Path dir = checkpointsDirOf(instance)) {
            Assert.assertTrue("the repair marker must stay on disk", LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
            Assert.assertEquals(markerBaseGeneration, LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir));
        }
        final long generation = publishedCheckpointGeneration(instance);
        if (isStale) {
            Assert.assertTrue("a root has landed past the splice", generation > markerBaseGeneration + 1);
        } else {
            Assert.assertEquals("no root has landed past the splice", markerBaseGeneration + 1, generation);
        }
    }

    /**
     * Asserts what the restart after failTheOwedSealOfALateRow met, on {@code job}, and returns the
     * job the view refreshes on from there. With a root a later seal landed at the frontier, a
     * restore. With the marker still live or the timeline retired, the rebuild from the applied
     * base: over the day the run dropped, which {@code droppedDayTs} names, the restatement guard
     * refuses it and the view stops with every row {@code base_shadow} keeps, until the day goes
     * back into the base and a restart rebuilds the view past the guard. After an episode that
     * ended on {@link #failTheSecondRepairsCommit}, nothing stood the guard down, and
     * {@code base_shadow} takes the second late row once the view is checked against it.
     */
    private LiveViewRefreshJob assertTheRestartAfterTheOwedSealEpisode(
            LiveViewRefreshJob job,
            int owedSealOutcome,
            long droppedDayTs,
            String oracleOrderedSql,
            String shadowOracleOrderedSql,
            String viewOrderedSql
    ) throws Exception {
        if (owedSealOutcome == OWED_SEAL_RESTORABLE) {
            assertRestoredFromTimeline("lv");
            restoredOwedSealEpisodes++;
            settleTheSecondRepairsCommitFault();
            return job;
        }
        if (owedSealOutcome == OWED_SEAL_RETIRED || owedSealOutcome == OWED_SEAL_RETIRED_TIMELINE_KEPT) {
            retiredOwedSealEpisodes++;
        }
        if (owedSealOutcome == OWED_SEAL_RETIRED_TIMELINE_KEPT) {
            Assert.assertEquals(
                    "the restart rebuilds over the marker the retire kept",
                    restartRebuildsOverTheMarkerBefore + 1,
                    countRestartRebuildsOverTheMarker()
            );
        }
        if (droppedDayTs == Numbers.LONG_NULL) {
            assertRebuiltFromAppliedBase("lv");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracleOrderedSql, viewOrderedSql, LOG, true);
            settleTheSecondRepairsCommitFault();
            return job;
        }
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        Assert.assertEquals("rebuild_blocked", LiveViewCheckpointRecoveryPhase.routeName(instance.getCheckpointRestoreRoute()));
        Assert.assertTrue(instance.isCheckpointRecoveryBlocked());
        TestUtils.assertContains(
                instance.getCheckpointRecoveryReason(),
                "would drop rows it retains [cause="
                        + (owedSealOutcome == OWED_SEAL_RETIRED ? "timeline is absent" : "prefix preservation repair marker present")
                        + "]: "
        );
        TestUtils.assertSqlCursors(engine, sqlExecutionContext, shadowOracleOrderedSql, viewOrderedSql, LOG, true);
        blockedOwedSealEpisodes++;
        if (owedSealOutcome == OWED_SEAL_RETIRED) {
            blockedTimelineAbsentOwedSealEpisodes++;
        }
        settleTheSecondRepairsCommitFault();
        return restoreTheDroppedDayAndRestart(job, droppedDayTs, oracleOrderedSql, viewOrderedSql);
    }

    /**
     * After an episode that ended on {@link #failTheSecondRepairsCommit}, once the restart after
     * it has been checked: asserts that nothing stood the restatement guard down from the
     * episode's marker on, and gives {@code base_shadow} the second late row the episode committed
     * to the base alone, if the view had not consumed it in the episode. A no-op after any other
     * episode.
     */
    private void settleTheSecondRepairsCommitFault() throws Exception {
        if (secondRepairEpisodeMarker == null) {
            return;
        }
        final LogCapture capture = walApplySuspensionRule.getCapture();
        capture.drain(LogCapture.STOP_DRAIN_TIMEOUT_MS);
        Assert.assertTrue("the capture must hold the episode's marker", capture.countOccurrences(secondRepairEpisodeMarker) > 0);
        capture.assertNotLoggedAfter(secondRepairEpisodeMarker, RESTATEMENT_GUARD_STAND_DOWN);
        secondRepairEpisodeMarker = null;
        if (secondRepairShadowValues.length() > 0) {
            execute("INSERT INTO base_shadow (ts, sym, i, x) VALUES " + secondRepairShadowValues);
            drainWalQueue();
            secondRepairShadowValues.clear();
        }
    }

    // The restarts the test's capture shows rebuilding over a live repair marker, read once the
    // logging worker has delivered what it holds.
    private int countRestartRebuildsOverTheMarker() {
        final LogCapture capture = walApplySuspensionRule.getCapture();
        capture.drain(LogCapture.STOP_DRAIN_TIMEOUT_MS);
        return capture.countOccurrences(MARKER_REBUILD);
    }

    private Path checkpointsDirOf(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    // The checkpoint id the next root of the view's timeline takes, which the superblock names.
    private long publishedNextCheckpointId(LiveViewInstance instance) {
        try (Path dir = checkpointsDirOf(instance); LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration)) {
            store.of(dir);
            return store.getSuperblock().nextCheckpointId;
        }
    }

    /**
     * Stops the process a departure's frontier truncate crashed in, after the stretch and the drop
     * that followed the departure, and puts the crash image back as the view's checkpoint
     * directory: the restart the caller runs next comes back from what the crash left, the
     * splice's generation under the truncate's marker. Everything outside that directory keeps
     * what the turns after the departure wrote, which the rebuild from the applied base the marker
     * sends the restart to replaces.
     */
    private void putTheFrontierTruncateCrashImageBack() {
        final TableToken viewToken = engine.verifyTableName("lv");
        engine.getLiveViewRegistry().clear();
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        engine.releaseInactive();
        try (
                Path checkpoints = new Path().of(configuration.getDbRoot()).concat(viewToken).concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME).slash();
                Path crashImage = frontierTruncateCrashImageDir()
        ) {
            final FilesFacade ff = configuration.getFilesFacade();
            Assert.assertTrue(ff.rmdir(checkpoints));
            TestUtils.copyDirectory(crashImage, checkpoints, configuration.getMkDirMode());
            Assert.assertTrue(ff.rmdir(crashImage));
        }
    }

    /**
     * Puts the day the run dropped, which {@code droppedDayTs} names, back into the base from
     * {@code base_shadow}, and restarts: the restatement guard lets the restart's rebuild of the
     * view through, and the rebuild, which retires the timeline with any marker on it, gives the
     * recompute. Returns the job the view refreshes on from there.
     */
    private LiveViewRefreshJob restoreTheDroppedDayAndRestart(
            LiveViewRefreshJob job,
            long droppedDayTs,
            String oracleOrderedSql,
            String viewOrderedSql
    ) throws Exception {
        execute("INSERT INTO base (ts, sym, i, x) SELECT ts, sym, i, x FROM base_shadow WHERE ts >= "
                + droppedDayTs + " AND ts < " + (droppedDayTs + Micros.DAY_MICROS));
        drainWalQueue();
        coverage.absorb(job);
        Misc.free(job);
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
        final LiveViewRefreshJob rebuiltJob = new LiveViewRefreshJob(0, engine, 1);
        driveRefreshToQuiescence(rebuiltJob);
        assertRebuiltFromAppliedBase("lv");
        Assert.assertFalse(engine.getLiveViewRegistry().getViewInstance("lv").isCheckpointRecoveryBlocked());
        TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracleOrderedSql, viewOrderedSql, LOG, true);
        return rebuiltJob;
    }

    /**
     * Runs {@code commit}, and when {@code isCrashDrawn}, with {@link #checkpointFaults}
     * armed: the second repair marker rename the commit's turn makes, the marker a frontier
     * truncate writes after its splice's, copies the view's checkpoint directory to the crash
     * image, as a crash ahead of the truncate's publication would leave it. The turn then runs on
     * to its end, and the caller restarts from the image after what follows the departure.
     */
    private void runUnderTheFrontierTruncateCrash(boolean isCrashDrawn, TestUtils.LeakProneCode commit) throws Exception {
        if (!isCrashDrawn) {
            commit.run();
            return;
        }
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        try (Path checkpoints = checkpointsDirOf(instance).slash(); Path crashImage = frontierTruncateCrashImageDir()) {
            checkpointFaults.armTheCrash(() -> TestUtils.copyDirectory(checkpoints, crashImage, configuration.getMkDirMode()));
            try {
                commit.run();
            } finally {
                checkpointFaults.disarmTheCrash();
            }
        }
    }

    // One row the run's filter keeps, at the first timestamp above afterTs the run has not used,
    // committed to the base and to base_shadow, and refreshed. Returns its timestamp.
    private long commitAKeptRowToTheBaseAndItsShadow(
            Rnd episodeRnd,
            LiveViewRefreshJob job,
            @Nullable LongHashSet usedTs,
            int symCount,
            long afterTs
    ) throws Exception {
        long ts = afterTs + 1;
        while (usedTs != null && !usedTs.add(ts)) {
            ts++;
        }
        final StringSink values = new StringSink();
        appendEpisodeRow(values, ts, episodeRnd.nextInt(symCount), 1 + episodeRnd.nextInt(1000), episodeRnd.nextDouble() * 1000.0);
        commitToTheBaseAndItsShadow(values, job);
        return ts;
    }

    private void commitToTheBaseAndItsShadow(CharSequence values, LiveViewRefreshJob job) throws Exception {
        execute("INSERT INTO base (ts, sym, i, x) VALUES " + values);
        execute("INSERT INTO base_shadow (ts, sym, i, x) VALUES " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    // base_shadow: a copy of the base under the same partitioning and dedup keys, which keeps every
    // row the base loses from here on. The run's oracle after an owed-seal episode.
    private void copyTheBaseToItsShadow(boolean isDedup) throws Exception {
        execute("DROP TABLE IF EXISTS base_shadow");
        execute("CREATE TABLE base_shadow (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL"
                + (isDedup ? " DEDUP UPSERT KEYS(ts, sym)" : ""));
        execute("INSERT INTO base_shadow SELECT ts, sym, i, x FROM base");
        drainWalQueue();
    }

    /**
     * Fails the closing seal of a late row's repair over a splice whose newest root sits below
     * the view's frontier, which leaves the repair marker live, under the sparse checkpoint
     * cadence. Batches A and B of four rows, above every row and each committed past the duration
     * cadence, seal a root each, and batch C commits above them inside it. A late row between A
     * and B is repaired by the resume from A's root or by the head-miss rebuild, and either
     * re-versions B's root and keeps it as the splice's newest, below C. Both seal hooks set the
     * writer stage that fails a publication ahead of its superblock, so the closing seal fails. The
     * stage stays for {@code k}, drawn in [0, 3], more commits, each of which fails a seal: on the
     * head-miss route a row above C, whose first checkpoint fails, and on the resume route another
     * late row between A and B, whose closing seal fails. The third failure in a row pauses the
     * seal, and the clock passes the pause ahead of a fourth, which retires the timeline. Then one
     * clean commit of a row above C follows, inside the cadence, and for {@code k} of two, past the
     * pause or inside it as the episode draws. Every row goes into {@code base_shadow} too.
     * <p>
     * {@code isHeadMissShaped} picks the route, as departNewestRowsOverSparseRoots does. Without
     * it, B and the late row sit within a second above A, so a rebuild warming its state from one
     * frame below the late row reads A's rows and more, and the plan takes the resume. With it,
     * the batches start the day above every row, which their partition then holds alone, B sits
     * more than twice the frame's width above A, and the late row just below B: that rebuild reads
     * none of the rows between A's root and the frame under the late row a resume reads, and the
     * plan takes the head-miss rebuild.
     *
     * <p>
     * Under {@link #isSecondRepairCommitFaultArm}, every other episode ends right after the late
     * row's failed closing seal, on {@link #failTheSecondRepairsCommit}, with the day the run
     * dropped, if any, named by {@code droppedDayTs} and the oracles the episode checks against.
     *
     * @param frameMinutes the width of the view's RANGE frame, in minutes
     * @return what the restart after the episode meets: {@link #OWED_SEAL_RETIRED} when the k
     * commits made four failures in a row, which retired the timeline and armed a cooldown the
     * clean commit's seal waits on, and {@link #OWED_SEAL_RETIRED_TIMELINE_KEPT} when that retire
     * met a lock on {@code _timeline}, which the episode draws, and kept the timeline with the live
     * marker beside it; {@link #OWED_SEAL_RESTORABLE} when the clean commit's seal
     * landed, which on the resume route, whose head parked on B's root, only the end of the pause
     * makes due inside the cadence, and on the head-miss route, which cleared the head, every
     * commit outside the pause does; and {@link #OWED_SEAL_MARKER_LIVE} otherwise
     */
    private int failTheOwedSealOfALateRow(
            Rnd episodeRnd,
            LiveViewRefreshJob job,
            @Nullable LongHashSet usedTs,
            int symCount,
            int frameMinutes,
            boolean isHeadMissShaped,
            long droppedDayTs,
            String oracleOrderedSql,
            String shadowOracleOrderedSql,
            String viewOrderedSql
    ) throws Exception {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        // Whether the retire at the fourth failure in a row, when k makes one, meets a lock on
        // _timeline: the test's first such retire does, and one in two after it, drawn from a
        // sequence salted apart from the episode's.
        final boolean isTimelineLockedForTheRetire = owedSealKeptTimelineRetires == 0
                || keptTimelineRetireRnd(episodeRnd).nextBoolean();
        // Drawn ahead of everything else the episode draws, so it does not follow from k.
        final boolean isPausePassedWhenPaused = episodeRnd.nextBoolean();
        final long floorTs = Math.max(
                Math.max(queryTimestamp("SELECT max(ts) FROM base"), instance.getLatestSeenTs()),
                instance.getDefinition().getViewLowerBoundTimestamp()
        );
        final StringSink values = new StringSink();
        final long[] batchA = new long[4];
        setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
        final long batchAFromTs = isHeadMissShaped
                ? floorTs - floorTs % Micros.DAY_MICROS + Micros.DAY_MICROS + 1 + episodeRnd.nextLong(Micros.SECOND_MICROS)
                : floorTs + 1 + episodeRnd.nextInt(1_000_000);
        drawEpisodeBatch(episodeRnd, values, batchAFromTs, usedTs, symCount, batchA, null);
        commitToTheBaseAndItsShadow(values, job);
        Assert.assertEquals("a root seals batch A", batchA[3], instance.getHeadCheckpointMaxTs());
        final long[] batchB = new long[4];
        values.clear();
        setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
        final long batchBFromTs = isHeadMissShaped
                ? batchA[3] + (2L * frameMinutes + 1) * Micros.MINUTE_MICROS + 1 + episodeRnd.nextLong(Micros.SECOND_MICROS)
                : batchA[3] + 2 + episodeRnd.nextInt(1_000_000);
        drawEpisodeBatch(episodeRnd, values, batchBFromTs, usedTs, symCount, batchB, null);
        commitToTheBaseAndItsShadow(values, job);
        Assert.assertEquals("a root seals batch B", batchB[3], instance.getHeadCheckpointMaxTs());
        final long[] batchC = new long[4];
        values.clear();
        drawEpisodeBatch(episodeRnd, values, batchB[3] + 1 + episodeRnd.nextInt(1_000_000), usedTs, symCount, batchC, null);
        commitToTheBaseAndItsShadow(values, job);
        Assert.assertEquals("no root seals batch C", batchB[3], instance.getHeadCheckpointMaxTs());

        final long resumesBefore = job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR);
        final long headMissRepairsBefore = countHeadMissRepairs(job);
        final long sealFailuresBefore = instance.getCheckpointSealFailures();
        final long resetsBefore = instance.getCheckpointTimelineResets();
        final long faultsBefore = instance.getRefreshFaultCount();
        final long markerBaseGeneration = publishedCheckpointGeneration(instance);
        long lateTs;
        if (isHeadMissShaped) {
            lateTs = batchB[0] - 1 - episodeRnd.nextLong(Micros.SECOND_MICROS);
            while (usedTs != null && !usedTs.add(lateTs)) {
                lateTs--;
            }
            Assert.assertTrue(
                    "the frame under the late row sits above A's root",
                    lateTs - frameMinutes * Micros.MINUTE_MICROS > batchA[3]
            );
        } else {
            lateTs = batchA[3] + 1 + episodeRnd.nextLong(batchB[0] - batchA[3] - 1);
            while (usedTs != null && !usedTs.add(lateTs)) {
                lateTs++;
            }
        }
        Assert.assertTrue("the late row sits between batches A and B", lateTs > batchA[3] && lateTs < batchB[0]);
        values.clear();
        appendEpisodeRow(values, lateTs, episodeRnd.nextInt(symCount), 1 + episodeRnd.nextInt(1000), episodeRnd.nextDouble() * 1000.0);
        final Runnable failTheSeal = () -> job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
        final int k = episodeRnd.nextInt(4);
        // Two more failures make three in a row, which pause the seal: whether the clock then
        // passes the pause before the clean commit. Three more make the fourth after the pause.
        final boolean isPausePassed = k == 2 && isPausePassedWhenPaused;
        final boolean isResumed;
        long rowTs = batchC[3];
        long lastMarkerBaseGeneration = markerBaseGeneration;
        try {
            hooks(job).setSimulateResumeHeadSealStart(failTheSeal);
            hooks(job).setSimulateHeadMissHeadSealStart(failTheSeal);
            try {
                commitToTheBaseAndItsShadow(values, job);
            } finally {
                hooks(job).setSimulateResumeHeadSealStart(null);
                hooks(job).setSimulateHeadMissHeadSealStart(null);
            }
            isResumed = job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR) > resumesBefore;
            LOG.info().$("LV fuzz owed-seal episode [lateTs=").$ts(lateTs).$(", headMissShaped=").$(isHeadMissShaped)
                    .$(", resumed=").$(isResumed).$(", k=").$(k).$(", pausePassed=").$(isPausePassed).I$();
            // The restart ahead of the episode compiled the view over every column the rowless
            // stretch added to the base, so the repair meets no drift.
            Assert.assertEquals("the late row's turn does not fault", faultsBefore, instance.getRefreshFaultCount());
            Assert.assertEquals(
                    "a resume from A's root or the head-miss rebuild repairs the late row",
                    1,
                    (isResumed ? 1 : 0) + countHeadMissRepairs(job) - headMissRepairsBefore
            );
            Assert.assertEquals("the episode's shape picks the route", isHeadMissShaped, !isResumed);
            Assert.assertEquals(
                    "the resume parks the head on B's root, and the head-miss rebuild clears it",
                    isResumed ? batchB[3] : Numbers.LONG_NULL,
                    instance.getHeadCheckpointMaxTs()
            );
            Assert.assertEquals("the closing seal fails", sealFailuresBefore + 1, instance.getCheckpointSealFailures());
            assertRepairMarker(instance, markerBaseGeneration, false);
            owedSealEpisodes++;
            if (isHeadMissShaped) {
                headMissOwedSealEpisodes++;
            }
            if (isSecondRepairCommitFaultArm && secondRepairCommitFaultCandidates++ % 2 == 0) {
                return failTheSecondRepairsCommit(
                        episodeRnd,
                        job,
                        instance,
                        usedTs,
                        symCount,
                        batchA,
                        batchB,
                        isHeadMissShaped,
                        droppedDayTs,
                        oracleOrderedSql,
                        shadowOracleOrderedSql,
                        viewOrderedSql
                );
            }
            for (int i = 0; i < k; i++) {
                final boolean isRetireLocked = i == 2 && isTimelineLockedForTheRetire;
                if (i == 2) {
                    // Past the pause, so the fourth failure in a row is attempted.
                    setCurrentMicros(currentMicros + Micros.MINUTE_MICROS + Micros.SECOND_MICROS);
                }
                if (isRetireLocked) {
                    checkpointFaults.lockTheTimeline();
                }
                if (isResumed) {
                    // On the resume route, whose head parked on B's root seals nothing inside the
                    // cadence, each failure is the closing seal of another late row between A and
                    // B, which the resume from A's root repairs over the live marker.
                    lastMarkerBaseGeneration = publishedCheckpointGeneration(instance);
                    final long resumes = job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR);
                    long nextLateTs = batchA[3] + 1 + episodeRnd.nextLong(batchB[0] - batchA[3] - 1);
                    while (usedTs != null && !usedTs.add(nextLateTs)) {
                        nextLateTs++;
                    }
                    values.clear();
                    appendEpisodeRow(values, nextLateTs, episodeRnd.nextInt(symCount), 1 + episodeRnd.nextInt(1000), episodeRnd.nextDouble() * 1000.0);
                    // The splice publishes, and only the closing seal fails.
                    job.setCheckpointTimelineTestFailureStage(0);
                    hooks(job).setSimulateResumeHeadSealStart(failTheSeal);
                    try {
                        commitToTheBaseAndItsShadow(values, job);
                    } finally {
                        hooks(job).setSimulateResumeHeadSealStart(null);
                    }
                    Assert.assertEquals(
                            "the resume from A's root repairs the next late row",
                            resumes + 1,
                            job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR)
                    );
                } else {
                    rowTs = commitAKeptRowToTheBaseAndItsShadow(episodeRnd, job, usedTs, symCount, rowTs);
                }
                if (isRetireLocked) {
                    Assert.assertEquals("the retire at the fourth failure meets the lock once", 1, checkpointFaults.unlockTheTimeline());
                }
                Assert.assertEquals("each commit fails its seal", sealFailuresBefore + 2 + i, instance.getCheckpointSealFailures());
                if (i < 2) {
                    Assert.assertEquals("three failures in a row retire nothing", resetsBefore, instance.getCheckpointTimelineResets());
                }
            }
        } finally {
            job.setCheckpointTimelineTestFailureStage(0);
            checkpointFaults.unlockTheTimeline();
        }
        if (k >= 2) {
            pausedOwedSealEpisodes++;
            if (isPausePassed) {
                setCurrentMicros(currentMicros + Micros.MINUTE_MICROS + Micros.SECOND_MICROS);
            }
        }
        final long cleanTs = commitAKeptRowToTheBaseAndItsShadow(episodeRnd, job, usedTs, symCount, rowTs);
        Assert.assertEquals("no commit after the late row faults", faultsBefore, instance.getRefreshFaultCount());
        Assert.assertEquals("the clean commit fails nothing", sealFailuresBefore + 1 + k, instance.getCheckpointSealFailures());
        if (k == 3) {
            Assert.assertEquals("the fourth failure in a row retires the timeline", resetsBefore + 1, instance.getCheckpointTimelineResets());
            Assert.assertEquals("and its cooldown holds the clean commit's seal", Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
            if (isTimelineLockedForTheRetire) {
                // The retire keeps the generation the lock held, and the live marker beside it,
                // which no seal has read as stale since: the restart rebuilds over the marker.
                Assert.assertTrue("the locked timeline stays", hasCheckpointTimeline(instance));
                assertRepairMarker(instance, lastMarkerBaseGeneration, false);
                owedSealKeptTimelineRetires++;
                restartRebuildsOverTheMarkerBefore = countRestartRebuildsOverTheMarker();
                return OWED_SEAL_RETIRED_TIMELINE_KEPT;
            }
            Assert.assertFalse(hasCheckpointTimeline(instance));
            try (Path dir = checkpointsDirOf(instance)) {
                Assert.assertFalse("the retire takes the marker", LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
            }
            return OWED_SEAL_RETIRED;
        }
        Assert.assertEquals("no retire before the pause ends", resetsBefore, instance.getCheckpointTimelineResets());
        if (isPausePassed) {
            // The first commit after the pause seals whatever the cadence says, on the ladder the
            // pause kept, which reads the marker as stale.
            Assert.assertEquals("the clean commit's seal lands", cleanTs, instance.getHeadCheckpointMaxTs());
            assertRepairMarker(instance, lastMarkerBaseGeneration, true);
            restoredAfterThePauseOwedSealEpisodes++;
            if (isResumed) {
                restoredAfterThePauseResumedOwedSealEpisodes++;
            }
            return OWED_SEAL_RESTORABLE;
        }
        if (isResumed || k == 2) {
            // The resume parked the head on B's root, and nothing inside the cadence seals; the
            // pause holds whatever the head-miss rebuild's cleared head would have sealed.
            Assert.assertEquals(isResumed ? batchB[3] : Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
            assertRepairMarker(instance, lastMarkerBaseGeneration, false);
            if (k == 2) {
                markerLiveInsideThePauseOwedSealEpisodes++;
            }
            return OWED_SEAL_MARKER_LIVE;
        }
        // The head-miss rebuild cleared the head, so every commit after the late row seals at once.
        Assert.assertEquals("the clean commit's seal lands", cleanTs, instance.getHeadCheckpointMaxTs());
        assertRepairMarker(instance, markerBaseGeneration, true);
        return OWED_SEAL_RESTORABLE;
    }

    /**
     * Run by failTheOwedSealOfALateRow on every other episode of {@link #isSecondRepairCommitFaultArm},
     * right after the late row's closing seal failed and left the repair marker live over the
     * splice's generation. A second late row inside that window, placed for the route the first
     * took, goes to the base alone, and its repair writes its marker over the live one, so the
     * marker records no seqTxn. Its replacement commit fails from the role-switch mint seam: in the
     * process with a critical error on the batch's first such episode and every other one from it,
     * and on the others by stopping the process there, which the caller's restart then comes back
     * from. In the process, the fault meets the in-place recovery, unless a head-miss rebuild that
     * converged below the frontier kept the primary runtime and retries. A recovery that meets the
     * marker rebuilds from the applied base, which the restatement guard refuses over the day the
     * run dropped, {@code droppedDayTs}: the view stops with the rows {@code base_shadow} keeps, the
     * second late row unconsumed. Otherwise the view consumes the row and matches the recompute,
     * over {@code base_shadow} when the base lost a day. Nothing may stand the guard down: see
     * {@link #assertTheRestartAfterTheOwedSealEpisode}, which also gives {@code base_shadow} the
     * row once the view holds it.
     *
     * @return what the restart after the episode meets: {@link #OWED_SEAL_MARKER_LIVE} after the
     * stop or a rebuild refused in place, and {@link #OWED_SEAL_RESTORABLE} once the view holds the
     * row, whose rebuild or retry left no live marker
     */
    private int failTheSecondRepairsCommit(
            Rnd episodeRnd,
            LiveViewRefreshJob job,
            LiveViewInstance instance,
            @Nullable LongHashSet usedTs,
            int symCount,
            long[] batchA,
            long[] batchB,
            boolean isHeadMissShaped,
            long droppedDayTs,
            String oracleOrderedSql,
            String shadowOracleOrderedSql,
            String viewOrderedSql
    ) throws Exception {
        // Counted rather than drawn: see assertReachedASecondRepairCommitFault.
        final boolean isInPlace = secondRepairCommitFaultEpisodes % 2 == 0;
        long lateTs;
        if (isHeadMissShaped) {
            lateTs = batchB[0] - 1 - episodeRnd.nextLong(Micros.SECOND_MICROS);
            while (usedTs != null && !usedTs.add(lateTs)) {
                lateTs--;
            }
        } else {
            lateTs = batchA[3] + 1 + episodeRnd.nextLong(batchB[0] - batchA[3] - 1);
            while (usedTs != null && !usedTs.add(lateTs)) {
                lateTs++;
            }
        }
        Assert.assertTrue("the second late row sits between batches A and B", lateTs > batchA[3] && lateTs < batchB[0]);
        secondRepairShadowValues.clear();
        appendEpisodeRow(secondRepairShadowValues, lateTs, episodeRnd.nextInt(symCount), 1 + episodeRnd.nextInt(1000), episodeRnd.nextDouble() * 1000.0);
        secondRepairEpisodeMarker = "second repair commit fault episode " + SECOND_REPAIR_EPISODE_SEQ.incrementAndGet() + " starts";
        LOG.advisory().$(secondRepairEpisodeMarker).$();
        LOG.info().$("LV fuzz second repair commit fault [lateTs=").$ts(lateTs).$(", headMissShaped=").$(isHeadMissShaped)
                .$(", inPlace=").$(isInPlace).$(", dayDropped=").$(droppedDayTs != Numbers.LONG_NULL).I$();
        // The second repair's publications land: its replacement commit is the step that fails.
        job.setCheckpointTimelineTestFailureStage(0);
        final long lvSeqTxnBefore = engine.getTableSequencerAPI().lastTxn(instance.getLiveViewToken());
        final LiveViewReplacementCommitWitness witness = new LiveViewReplacementCommitWitness(
                engine,
                instance,
                publishedCheckpointGeneration(instance),
                isInPlace ? LiveViewReplacementCommitWitness.FAIL_CRITICAL : LiveViewReplacementCommitWitness.FAIL_CANCELLATION
        );
        execute("INSERT INTO base (ts, sym, i, x) VALUES " + secondRepairShadowValues);
        drainWalQueue();
        CairoEngine.setRoleSwitchMintObserver(witness::onMint);
        try {
            if (isInPlace) {
                driveRefreshToQuiescence(job);
            } else {
                for (int pass = 0; pass < REFRESH_QUIESCENCE_PASSES && !witness.hasFailed(); pass++) {
                    setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                    drainWalQueue();
                    job.processNotificationsForTest();
                }
            }
        } finally {
            CairoEngine.setRoleSwitchMintObserver(null);
        }
        Assert.assertTrue("the second late row's repair must reach its replacement commit", witness.hasFailed());
        for (int i = 0, n = witness.size(); i < n; i++) {
            Assert.assertEquals("a marker written over a live one records no seqTxn", Numbers.LONG_NULL, witness.getRecordedLvSeqTxn(i));
        }
        secondRepairCommitFaultEpisodes++;
        if (!isInPlace) {
            Assert.assertEquals("the replacement never commits", lvSeqTxnBefore, engine.getTableSequencerAPI().lastTxn(instance.getLiveViewToken()));
            secondRepairCommitStops++;
            return OWED_SEAL_MARKER_LIVE;
        }
        secondRepairCommitFaultsInPlace++;
        if (instance.isCheckpointRecoveryBlocked()) {
            Assert.assertNotEquals("only a day the base lost refuses the rebuild", Numbers.LONG_NULL, droppedDayTs);
            TestUtils.assertContains(instance.getCheckpointRecoveryReason(), "would drop rows it retains [cause=mid-drain refresh failure]: ");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, shadowOracleOrderedSql, viewOrderedSql, LOG, true);
            secondRepairRebuildsRefusedInPlace++;
            return OWED_SEAL_MARKER_LIVE;
        }
        // The view consumed the row, so base_shadow takes it now.
        execute("INSERT INTO base_shadow (ts, sym, i, x) VALUES " + secondRepairShadowValues);
        drainWalQueue();
        secondRepairShadowValues.clear();
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                droppedDayTs != Numbers.LONG_NULL ? shadowOracleOrderedSql : oracleOrderedSql,
                viewOrderedSql,
                LOG,
                true
        );
        return OWED_SEAL_RESTORABLE;
    }

    private boolean hasCheckpointTimeline(LiveViewInstance instance) {
        try (
                Path checkpointsDir = new Path().of(configuration.getDbRoot())
                        .concat(instance.getLiveViewToken())
                        .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
                Path timelinePath = new Path()
        ) {
            LiveViewCheckpointLayout.timelinePath(timelinePath, checkpointsDir);
            return configuration.getFilesFacade().exists(timelinePath.$());
        }
    }

    // Four rows a millisecond apart from fromTs, on random keys and with values the run's filter
    // keeps, in one commit. Fills tsOut, and symOut when not null, with what it wrote.
    private void commitEpisodeBatch(
            Rnd rnd,
            long fromTs,
            @Nullable LongHashSet usedTs,
            int symCount,
            long[] tsOut,
            @Nullable int[] symOut
    ) throws SqlException {
        final StringSink values = new StringSink();
        drawEpisodeBatch(rnd, values, fromTs, usedTs, symCount, tsOut, symOut);
        execute("INSERT INTO base (ts, sym, i, x) VALUES " + values);
    }

    // One randomized REPLACE_RANGE commit against the base: picks a band whose
    // boundaries anchor on dataset row timestamps - the low bound lands exactly
    // ON a row's ts half the time, exercising the non-strict frontier edge -
    // and writes 0..3 fresh rows inside the band (0 = a pure delete). Fresh row
    // timestamps come from the never-reused usedTs pool, so global ts
    // uniqueness survives any sequence of replaces and the recompute oracle
    // stays sound. Rows are appended in random order half the time, so a
    // replace commit can also be intra-commit out-of-order. The band's low
    // bound anchors at or above tsv[1], but nothing keeps it above the view's START FROM
    // boundary: a run that drew an explicit bound routinely produces bands sitting entirely
    // below it, or straddling it, which is exactly what the clamped-delete path must survive.
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

    // The first column of the first row of a query returning a TIMESTAMP, or LONG_NULL.
    private long queryTimestamp(String sql) throws SqlException {
        try (
                RecordCursorFactory factory = select(sql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            return cursor.hasNext() ? cursor.getRecord().getTimestamp(0) : Numbers.LONG_NULL;
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

    /**
     * One run in two drops the oldest day the view holds rows of, as TTL or DROP PARTITION
     * would, after {@link #departNewestRows} and ahead of the restart that follows it. The view
     * keeps those rows and the base no longer holds them, so the restart brings them back only
     * from the timeline: a rebuild from the base would lose them, or be refused. The view is
     * checked against the recompute first, while the base still holds them, and its rows from
     * then on are the oracle. A view whose rows all sit on one day drops nothing: its oldest
     * day holds the departure's rows too.
     *
     * @return the view's rows ahead of the drop, ordered by {@code viewOrderedSql}, or null when
     * the run dropped nothing
     */
    private @Nullable String dropTheOldestDayTheViewHolds(
            Rnd rnd,
            LiveViewRefreshJob job,
            String oracleOrderedSql,
            String viewOrderedSql
    ) throws Exception {
        if (rnd.nextBoolean()) {
            return null;
        }
        final long viewMinTs = queryTimestamp("SELECT min(ts) FROM lv");
        final long viewMaxTs = queryTimestamp("SELECT max(ts) FROM lv");
        if (viewMinTs == Numbers.LONG_NULL || viewMinTs / Micros.DAY_MICROS == viewMaxTs / Micros.DAY_MICROS) {
            return null;
        }
        TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracleOrderedSql, viewOrderedSql, LOG, true);
        final StringSink rows = new StringSink();
        printSql(viewOrderedSql, rows);
        final StringSink day = new StringSink();
        MicrosFormatUtils.appendDateTimeUSec(day, viewMinTs - viewMinTs % Micros.DAY_MICROS);
        day.clear("yyyy-MM-dd".length());
        LOG.info().$("LV fuzz departure: dropping the oldest day the view holds [day=").$(day).I$();
        execute("ALTER TABLE base DROP PARTITION LIST '" + day + "'");
        drainWalQueue();
        driveRefreshToQuiescence(job);
        return rows.toString();
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

    // Inserts one (ts, sym, i, x) row into the base in its own commit, rendered as
    // insertCommit renders its rows: a negative symIdx, a LONG_NULL i and xNull
    // insert NULLs.
    private void insertRow(StringSink sink, long ts, int symIdx, long i, double x, boolean xNull) throws Exception {
        sink.clear();
        sink.put("INSERT INTO base (ts, sym, i, x) VALUES (").put(ts).put("::timestamp,");
        if (symIdx < 0) {
            sink.put("null,");
        } else {
            sink.put('\'').put(SYMBOLS[symIdx]).put("',");
        }
        if (i == Numbers.LONG_NULL) {
            sink.put("null,");
        } else {
            sink.put(i).put(',');
        }
        if (xNull) {
            sink.put("null");
        } else {
            sink.put(x);
        }
        sink.put(')');
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

    // Joins a worker thread, tolerating a null reference (a thread never started
    // because the try body threw before assigning it) and an interrupt. Used by the
    // cleanup finally so it can join every worker before freeing shared native state.
    private static void joinSilently(Thread thread) {
        if (thread == null) {
            return;
        }
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // A writer thread that commits the rows the streaming writers hold back, each one late, on
    // the ticks of runConcurrentWriterFuzz (see arriveAndAwaitTick). Every tick it commits the
    // next batch of its rows the stream has moved lagRows past, if there is one, and then
    // waits the tick out. The held-back rows go in ascending ts order, so each lands below the
    // stream's head, and a lag past a day jump puts it in a closed anchor segment. Once the
    // stream has finished it commits a batch per tick until its rows run out.
    // <p>
    // Half of its commits wait for every streaming commit of the tick first. A refresh task
    // names the commit that queued it, so the first commit of a tick drains alone and the task
    // re-queued behind it carries the rest together: a late commit behind the stream's meets a
    // drain that has already fed in-order rows. The other half race the stream. Like
    // newConcurrentWriterThread, it awaits the barrier before its first write and clears
    // thread-locals on exit for the leak check.
    private Thread newBackfillWriterThread(
            boolean[] isHeldBack,
            int fromIndex,
            int rowCount,
            int lagRows,
            int batch,
            long orderSeed,
            long[] tsv,
            int[] symIdx,
            long[] iv,
            double[] xv,
            TableToken baseToken,
            CyclicBarrier barrier,
            Phaser ticks,
            AtomicInteger streamedTo,
            AtomicInteger tickStreamCommits,
            AtomicInteger streamingWritersDone,
            int numStreamingWriters,
            AtomicLong backfilledRows,
            ConcurrentLinkedQueue<Throwable> errors
    ) {
        return new Thread(() -> {
            final Rnd orderRnd = new Rnd(orderSeed, orderSeed >>> 1);
            try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                barrier.await();
                int k = nextHeldBackRow(isHeldBack, fromIndex, rowCount);
                while (k < rowCount && errors.isEmpty()) {
                    final boolean isStreamDone = streamingWritersDone.get() == numStreamingWriters;
                    if (isStreamDone || streamedTo.get() >= k + lagRows) {
                        if (!isStreamDone && orderRnd.nextBoolean()) {
                            awaitTickStreamCommits(tickStreamCommits, streamingWritersDone, numStreamingWriters, errors);
                        }
                        int rows = 0;
                        while (rows < batch && k < rowCount && (isStreamDone || streamedTo.get() >= k + lagRows)) {
                            appendRow(walWriter, tsv[k], symIdx[k], iv[k], xv[k]);
                            rows++;
                            k = nextHeldBackRow(isHeldBack, k + 1, rowCount);
                        }
                        walWriter.commit();
                        backfilledRows.addAndGet(rows);
                    }
                    // The commit phase, then the drain phase.
                    arriveAndAwaitTick(ticks);
                    arriveAndAwaitTick(ticks);
                }
            } catch (Throwable th) {
                errors.add(th);
                barrier.reset();
                ticks.forceTermination();
            } finally {
                ticks.arriveAndDeregister();
                clearWorkerThreadLocals();
            }
        }, "lv-cw-backfill-writer");
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
                clearWorkerThreadLocals();
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
                clearWorkerThreadLocals();
            }
        }, "lv-rvr-reader");
    }

    // A streaming writer of a backfilling run (see runConcurrentWriterFuzz). It owns its own
    // WalWriter, and once per tick (see arriveAndAwaitTick) claims the next batch of rows the
    // backfilling writer does not hold back, appends and commits them under streamLock, raises
    // streamedTo to the batch's last row and counts the commit into tickStreamCommits. The lock
    // keeps the stream in timestamp order across writers, so every streaming commit is an
    // in-order one and the late rows are the backfilling writer's alone. It counts itself into
    // streamingWritersDone on exit, failed or not, so the backfilling writer never waits on a
    // stream that has stopped. It awaits the barrier before its first write and clears
    // thread-locals on exit for the leak check.
    private Thread newStreamingWriterThread(
            int writerId,
            int rowCount,
            int batch,
            long[] tsv,
            int[] symIdx,
            long[] iv,
            double[] xv,
            boolean[] isHeldBack,
            AtomicInteger nextStreamRow,
            Object streamLock,
            Phaser ticks,
            AtomicInteger streamedTo,
            AtomicInteger tickStreamCommits,
            AtomicInteger streamingWritersDone,
            TableToken baseToken,
            CyclicBarrier barrier,
            ConcurrentLinkedQueue<Throwable> errors
    ) {
        return new Thread(() -> {
            try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                barrier.await();
                boolean isStreamLeft = true;
                while (isStreamLeft && errors.isEmpty()) {
                    synchronized (streamLock) {
                        int k = nextStreamRow.get();
                        int rows = 0;
                        int lastRow = -1;
                        while (rows < batch && k < rowCount) {
                            if (!isHeldBack[k]) {
                                appendRow(walWriter, tsv[k], symIdx[k], iv[k], xv[k]);
                                lastRow = k;
                                rows++;
                            }
                            k++;
                        }
                        nextStreamRow.set(k);
                        walWriter.commit();
                        if (lastRow > -1) {
                            streamedTo.set(lastRow);
                        }
                        isStreamLeft = k < rowCount;
                    }
                    tickStreamCommits.incrementAndGet();
                    // The commit phase, then the drain phase.
                    arriveAndAwaitTick(ticks);
                    arriveAndAwaitTick(ticks);
                }
            } catch (Throwable th) {
                errors.add(th);
                ticks.forceTermination();
            } finally {
                streamingWritersDone.incrementAndGet();
                ticks.arriveAndDeregister();
                clearWorkerThreadLocals();
            }
        }, "lv-cw-stream-writer-" + writerId);
    }

    // One refresh cycle with the flush held off, then the lead-ordering check;
    // returns true if the check actually compared (see assertLeadAboveDisk).
    // Pinning lastFlushTimeUs to now makes flushDue false for this drain (the same
    // lever buildLeadForReadBack uses), so the cycle refreshes into the tier
    // without writing through to disk and its rows stay resident as an un-flushed
    // lead. The pin only delays the flush - a later unpinned cycle, or the closing
    // driveRefreshToQuiescence, still writes those rows through.
    private boolean pinnedRefreshCycle(LiveViewRefreshJob job) {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        instance.setLastFlushTimeUs(currentMicros);
        drainJob(job);
        drainWalQueue();
        return assertLeadAboveDisk(instance);
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
    // restores the head checkpoint, replays to the applied point, and rebuilds the lead.
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
    // ingestion shape (pre-CREATE seed history, then per-commit O3 refresh
    // with optional restarts, over an un-flushed lead or a parked repair as
    // well) but cross-checks against a DISTINCT oracle
    // SQL: the anchored live view vs. the equivalent (sym, bucket)-partitioned
    // regular window over the base. O3 is always on (segmentOrder shuffles), so
    // the anchor map is rebuilt on head-miss / head-hit replay and is verified to
    // agree with the from-scratch recompute after quiescence. Over a TIMESTAMP_NS
    // base when isNanos: the generated timestamps go in as nanoseconds, the START
    // FROM boundary too, and every live view line the run logged must print its
    // timestamps in nanoseconds, the segment repairs' included. The base indexes sym
    // when isSymIndexed, which is what gives a segment repair a keyed read to price.
    // The run's refresh jobs add their counters to coverage.
    private void runAnchoredFuzz(
            Rnd rnd,
            int variant,
            int rowCount,
            boolean restart,
            boolean seed,
            boolean isNanos,
            boolean isSymIndexed
    ) throws Exception {
        // Pin the clock a day below the data, like runFuzz: START FROM NOW resolves the
        // CREATE-moment clock into the view's lower bound, and this harness wants that bound
        // to admit the whole dataset. A run that wants a boundary CUTTING the data draws the
        // explicit-timestamp mode instead, which cuts at a known row.
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

        final String runMarker = isNanos ? logNanosBaseRunMarker() : null;
        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts " + (isNanos ? "TIMESTAMP_NS" : "TIMESTAMP")
                + ", sym SYMBOL" + (isSymIndexed ? " INDEX" : "") + ", i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        // Ascending timestamps with ties across keys: every anchored variant partitions by sym,
        // and (ts, sym) stays unique, so ts is a total order within each partition. Random
        // symbols and values with occasional NULLs.
        final int tiePct = tiePct(rnd);
        final LiveViewFuzzRows rows = LiveViewFuzzRows.generate(
                rnd, rowCount, symCount, MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z"),
                baseStepMax, dayJumpEvery, tiePct
        );
        final long[] tsv = rows.tsv;
        if (isNanos) {
            // The same instants in the base's unit.
            for (int k = 0; k < tsv.length; k++) {
                tsv[k] *= 1000;
            }
        }
        final int[] symIdx = rows.symIdx;
        final long[] iv = rows.iv;
        final double[] xv = rows.xv;
        final boolean[] xNull = rows.xNull;

        // Pre-CREATE history (the earliest rows by ts): a base with committed history at CREATE
        // seeds under every start mode, and the boundary decides how much of it the sweep
        // emits. The anchor map must come out of the seed agreeing with the recompute over the
        // same bounded row set - a sub-boundary row leaking in would open its bucket early.
        final int preCount = seed ? rnd.nextInt(rowCount + 1) : 0;
        final int startMode = rnd.nextInt(START_FROM_MODES);
        final long boundary = startBoundary(rnd, startMode, tsv, isNanos);
        final String viewSql = "SELECT " + anchoredViewProjection(variant) + " FROM base "
                + "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION " + bucket + ")";
        final String oracleSql = "SELECT " + anchoredOracleProjection(variant, bucket) + " FROM base"
                + whereTail(false, boundary);
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + startFromClause(startMode, boundary, isNanos)
                + "AS " + viewSql;

        LOG.info().$("LV anchored fuzz: variant=").$(variant).$(", rows=").$(rowCount)
                .$(", symCount=").$(symCount).$(", stepMode=").$(stepMode)
                .$(", tiePct=").$(tiePct).$(", ties=").$(rows.getTieCount())
                .$(", restart=").$(restart).$(", preCount=").$(preCount)
                .$(", startMode=").$(startMode).$(", boundary=").$(boundary)
                .$(", nanos=").$(isNanos).$(", symIndexed=").$(isSymIndexed)
                .$(", bucket=").$(bucket).$(", sql=").$(viewSql).$();

        final StringSink sink = new StringSink();
        LiveViewRefreshJob job = null;
        try {
            if (preCount > 0) {
                final int[] preOrder = segmentOrder(rnd, 0, preCount, true);
                final int[] cb = commitBounds(rnd, preOrder.length);
                for (int c = 0; c + 1 < cb.length; c++) {
                    insertAnchoredCommit(sink, preOrder, cb[c], cb[c + 1], tsv, symIdx, iv, xv, xNull, isNanos);
                    drainWalQueue();
                }
            }

            execute(createSql);
            assertPersistedStartBound("lv", boundary);
            job = new LiveViewRefreshJob(0, engine, 1);

            if (preCount > 0) {
                driveSeedToCompletion(job, "lv");
            }

            if (preCount < rowCount) {
                final int[] postOrder = segmentOrder(rnd, preCount, rowCount, true);
                final int[] cb = commitBounds(rnd, postOrder.length);
                for (int c = 0; c + 1 < cb.length; c++) {
                    insertAnchoredCommit(sink, postOrder, cb[c], cb[c + 1], tsv, symIdx, iv, xv, xNull, isNanos);
                    drainWalQueue();
                    if (restart && rnd.nextInt(4) == 0) {
                        // Holds the flush off, so the restart below can land on an un-flushed
                        // lead; a small replay budget (see testFuzzAnchored) can leave a repair
                        // parked under it too.
                        pinnedRefreshCycle(job);
                    } else {
                        refreshCycle(job);
                    }

                    if (restart && rnd.nextInt(3) == 0) {
                        LiveViewInstance inst = engine.getLiveViewRegistry().getViewInstance("lv");
                        if (inst != null
                                && inst.getStateReader().getSeedState() == LiveViewState.SEED_STATE_ACTIVE) {
                            coverage.absorb(job);
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
            coverage.absorb(job);
            Misc.free(job);
        }

        // The oracle: the anchored live view must equal the equivalent
        // (sym, bucket)-partitioned window recomputed over the base table. ORDER BY 1, 2 -
        // (ts, sym), unique over ties - gives both sides a total order.
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + oracleSql + ") ORDER BY 1, 2",
                "(lv) ORDER BY 1, 2",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
        if (isNanos) {
            assertNoLiveViewLineReadsNanosAsMicrosAfter(walApplySuspensionRule.getCapture(), runMarker);
        }

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
    }

    // One commit of runAnchoredFuzz's rows, whose timestamps are nanoseconds when isNanos.
    private void insertAnchoredCommit(
            StringSink sink,
            int[] order,
            int from,
            int to,
            long[] tsv,
            int[] symIdx,
            long[] iv,
            double[] xv,
            boolean[] xNull,
            boolean isNanos
    ) throws Exception {
        if (isNanos) {
            insertNsCommit(sink, order, from, to, tsv, symIdx, iv, xv, xNull);
        } else {
            insertCommit(sink, order, from, to, tsv, symIdx, iv, xv, xNull, null);
        }
    }

    // Transparent-DDL fuzz (see testFuzzBaseDdlTransparent). Mirrors runFuzz's
    // ingestion shape (pre-CREATE seed history, per-commit O3 refresh,
    // optional quiescent restarts), but between post-CREATE commits the base
    // receives random ADD / DROP / RENAME / retype DDL on columns the view
    // never references. Each DDL is followed by a stays-ACTIVE assertion and a
    // walk-past refresh cycle; the run ends on the standard recompute oracle.
    private void runBaseDdlFuzz(
            Rnd rnd,
            int variant,
            int rowCount,
            boolean restart,
            boolean seed,
            boolean inMemory
    ) throws Exception {
        // Pin the clock a day below the data, like runFuzz: START FROM NOW resolves the
        // CREATE-moment clock into the view's lower bound, and this harness wants that bound
        // to admit the whole dataset. A run that wants a boundary CUTTING the data draws the
        // explicit-timestamp mode instead, which cuts at a known row.
        if (currentMicros < 0) {
            setCurrentMicros(MicrosTimestampDriver.floor("2025-12-31T00:00:00.000000Z"));
        }

        final int n = 1 + rnd.nextInt(MAX_FRAME);
        final int symCount = 1 + rnd.nextInt(SYMBOLS.length);
        final int stepMode = rnd.nextInt(3);
        final int baseStepMax = stepMode == 0 ? 5_000_000 : stepMode == 1 ? 60_000_000 : 900_000_000;
        final int dayJumpEvery = stepMode == 0 ? 20 : 12;
        final boolean withWhere = rnd.nextInt(3) == 0;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

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

        // Pre-CREATE history (the earliest rows by ts): committed history at CREATE puts the
        // view in SEEDING under every start mode, and the boundary decides how much of it the
        // sweep emits.
        final int preCount = seed ? rnd.nextInt(rowCount + 1) : 0;
        final int startMode = rnd.nextInt(START_FROM_MODES);
        final long boundary = startBoundary(rnd, startMode, tsv, false);
        final String viewSql = "SELECT " + projection(variant, n) + " FROM base" + whereTail(withWhere, Numbers.LONG_NULL);
        final String oracleSql = "SELECT " + projection(variant, n) + " FROM base" + whereTail(withWhere, boundary);
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + (inMemory ? "IN MEMORY 60s " : "")
                + startFromClause(startMode, boundary, false)
                + "AS " + viewSql;

        LOG.info().$("LV base-DDL fuzz: variant=").$(variant).$(", rows=").$(rowCount)
                .$(", n=").$(n).$(", symCount=").$(symCount).$(", stepMode=").$(stepMode)
                .$(", restart=").$(restart).$(", preCount=").$(preCount)
                .$(", startMode=").$(startMode).$(", boundary=").$(boundary).$(", inMem=").$(inMemory)
                .$(", where=").$(withWhere).$(", sql=").$(viewSql).$();

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
            assertPersistedStartBound("lv", boundary);
            job = new LiveViewRefreshJob(0, engine, 1);

            if (preCount > 0) {
                driveSeedToCompletion(job, "lv");
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
                                && inst.getStateReader().getSeedState() == LiveViewState.SEED_STATE_ACTIVE) {
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

        // The oracle: the live view must equal its own window query recomputed over the base
        // table (whose unreferenced columns may have churned freely) under the run's START FROM
        // boundary.
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

    // Invalidation-transition fuzz (see testFuzzBaseDdlInvalidation). Ingests a
    // randomized dataset, quiesces, snapshots the view, then fires ONE
    // invalidating base-DDL op and asserts the transition end to end: the view
    // flips INVALID with the operation-specific reason, its materialized output
    // stays queryable and byte-identical to the snapshot, and further base
    // ingestion (where the base still exists) never reaches it. A second
    // invalidating op (invalidateBaseAgain) then reaches the invalid view, which
    // keeps the first op's reason, in memory and across a restart.
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

        // The one arm that stays on a single start mode. Its oracle is the view's own
        // pre-invalidation output, snapshotted and compared back, so what it measures is that an
        // invalidating DDL leaves the materialized rows untouched - a property that has nothing to
        // do with WHICH rows the view holds. Drawing a boundary would only risk emptying the view
        // and turning that comparison into two empty strings.
        final String viewSql = "SELECT " + projection(variant, n) + " FROM base";
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + (inMemory ? "IN MEMORY 60s " : "")
                + "START FROM NOW AS " + viewSql;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("DROP TABLE IF EXISTS base2");
        execute("DROP TABLE IF EXISTS base3");
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
        final String expectedReason;
        final String secondReason;
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

            secondReason = invalidateBaseAgain(rnd, op);
            drainWalQueue();
            Assert.assertTrue(instance.isInvalid());
            assertFirstReasonStands(op, expectedReason, secondReason, instance.getInvalidationReason());
            secondBaseInvalidations++;
        } finally {
            Misc.free(job);
        }

        // A restart loads the first reason from the view's state file.
        engine.getLiveViewRegistry().clear();
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        engine.releaseInactive();
        engine.buildViewGraphs();
        sink.clear();
        printSql("SELECT view_status, invalidation_reason FROM live_views() WHERE view_name = 'lv'", sink);
        TestUtils.assertContains(sink, "invalid\t");
        assertFirstReasonStands(op, expectedReason, secondReason, sink);

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE IF EXISTS base");
        execute("DROP TABLE IF EXISTS base2");
        execute("DROP TABLE IF EXISTS base3");
    }

    // A second invalidating operation, which reaches the view runBaseDdlInvalidationFuzz invalidated through
    // the view's base name: on the base itself after a column op, and after op 3, which renamed the base, or
    // op 4, which dropped it, on a namesake table created under that name. Returns the operation's reason.
    private String invalidateBaseAgain(Rnd rnd, int op) throws SqlException {
        if (op <= 2) {
            if (rnd.nextBoolean()) {
                execute("RENAME TABLE base TO base2");
                return "base table rename";
            }
            execute("DROP TABLE base");
            return "base table drop";
        }
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        if (op == 3) {
            execute("DROP TABLE base");
            return "base table drop";
        }
        return switch (rnd.nextInt(3)) {
            case 0 -> {
                execute("RENAME TABLE base TO base3");
                yield "base table rename";
            }
            case 1 -> {
                execute("INSERT INTO base (ts, sym, i) VALUES ('2026-01-01T00:00:00.000000Z', 'AA', 1)");
                drainWalQueue();
                execute("UPDATE base SET i = 2");
                yield "update operation";
            }
            // Every projection reads sym.
            default -> {
                execute("ALTER TABLE base DROP COLUMN sym");
                yield "drop column operation [column=sym]";
            }
        };
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
        // Re-pin the clock a day below the data start on EVERY call (not just the first): this
        // arm later advances the wall clock ABOVE the data to arm TTL (which evicts relative to
        // min(maxTimestamp, wallClock)), so the shared per-test clock would otherwise sit above
        // the next variant's data and push a START FROM NOW view's boundary past its early rows.
        // Pinned here, the NOW boundary lands below every row and the run's own cut - if it drew
        // an explicit boundary - is the only thing that excludes any.
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

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("DROP TABLE IF EXISTS shadow");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY "
                + partUnit + " " + ttlClause + " WAL");

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

        // The cut stays inside phase 1's first half, so the view always emits phase-1 rows
        // for the eviction to freeze - a boundary above the whole phase would leave the
        // freeze-and-continue assertion comparing one empty output against another.
        final int startMode = rnd.nextInt(START_FROM_MODES);
        final long boundary = startBoundary(rnd, startMode, tsv, false, splitPoint / 2);
        final String where = whereTail(withWhere, Numbers.LONG_NULL);
        final String oracleWhere = whereTail(withWhere, boundary);
        final String viewSql = "SELECT " + projection + " FROM base" + where;
        final String oracleSql = "SELECT " + projection + " FROM base" + oracleWhere;
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + (inMemory ? "IN MEMORY 60s " : "")
                + startFromClause(startMode, boundary, false)
                + "AS " + viewSql;

        LOG.info().$("LV base-TTL fuzz: variant=").$(variant).$(", rows=").$(rowCount)
                .$(", n=").$(n).$(", symCount=").$(symCount).$(", partUnit=").$(partUnit)
                .$(", ttlClause=").$(ttlClause).$(", restart=").$(restart).$(", inMem=").$(inMemory)
                .$(", startMode=").$(startMode).$(", boundary=").$(boundary)
                .$(", where=").$(withWhere).$(", sql=").$(viewSql).$();

        final StringSink sink = new StringSink();
        final StringSink preRemoval = new StringSink();
        LiveViewRefreshJob job = null;
        try {
            execute(createSql);
            assertPersistedStartBound("lv", boundary);
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
                            && inst.getStateReader().getSeedState() == LiveViewState.SEED_STATE_ACTIVE) {
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
                    "(" + oracleSql + ") ORDER BY 1",
                    "(lv) ORDER BY 1",
                    LOG,
                    true
            );
            assertNoRefreshFaults("lv");
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
                            && inst.getStateReader().getSeedState() == LiveViewState.SEED_STATE_ACTIVE) {
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

        // The oracle: the view must equal the window query recomputed over the LOGICAL dataset -
        // every generated row, as if nothing was evicted - materialized into the shadow table in
        // ts order, and bounded by the run's START FROM boundary, which the accumulators applied
        // to the rows they walked.
        execute("CREATE TABLE shadow (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        insertLogicalDataset("shadow", tsv, symIdx, iv, xv, xNull);
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(SELECT " + projection + " FROM shadow" + oracleWhere + ") ORDER BY 1",
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
    // strictly-increasing timestamps; optional pre-CREATE SEED history - but the
    // post-CREATE suffix is split into disjoint round-robin slices, one per writer
    // thread, that commit in parallel so the sequencer interleaves their
    // transactions. With concurrentRefresh a single refresh driver runs alongside
    // ingestion (steady-state timing, so the base apply job races ahead of the LV
    // trigger - the apply-ahead path); otherwise the refresh runs single-threaded
    // after the writers join. Either way the view is quiesced before the exact-
    // equality recompute oracle.
    // <p>
    // With an anchor (see testFuzzConcurrentWritersAnchored) the run is an
    // ANCHORED_VARIANT_COUNT shape over the anchor's named window, over a key indexed in most
    // runs, under the repair knobs drawn for it. Its streaming writers commit an in-order
    // stream (see newStreamingWriterThread) and a backfilling writer commits a share of the
    // suffix late, behind them (see newBackfillWriterThread), all paced to the refresh
    // driver's ticks. Between ticks the driver also checks a view caught up with the base
    // against the recompute, and the run ends by asserting that the backfill and those
    // checks happened.
    //
    // Returns the closed anchor segments the run's refresh job repaired over their own range.
    private long runConcurrentWriterFuzz(
            Rnd rnd,
            int variant,
            int rowCount,
            int numWriters,
            boolean concurrentRefresh,
            boolean seed,
            @Nullable LiveViewFuzzAnchor anchor
    ) throws Exception {
        Assert.assertTrue("an anchored run paces its writers to a concurrent refresh driver", anchor == null || concurrentRefresh);
        // A concurrent refresh driver advances the flush clock in an unbounded loop while
        // ingestion is in flight, so - unlike the single-threaded fuzz arms, which sit a day
        // below the data - the clock is pinned a full YEAR below the data start. A START FROM
        // NOW view resolves its boundary from that clock, and the year of headroom keeps the
        // boundary below every row for the whole run (a 250ms-per-advance loop never climbs a
        // year), so only an explicit-boundary run excludes any row. An anchored run's data
        // starts where its shape needs it, a day above the clock the anchor pins; the boundary
        // resolves at CREATE, before the driver moves the clock at all.
        final long dataStart;
        final StringSink knobLog = new StringSink();
        if (anchor != null) {
            setCurrentMicros(anchor.clockMicros);
            dataStart = anchor.dataStartMicros;
            // The per-segment loop stays on: it is the route that repairs a closed anchor
            // segment's late row on its own, and the late rows are what the backfilling
            // writer brings. A turn drains every commit a tick lands at the default budget,
            // so it can feed a streaming commit before it meets the late one; a budget of one
            // commit never does. Half the runs truncate at every segment repair rather than
            // splice: a truncate drops the root at the frontier, and the seal behind the
            // segment then freezes whatever runtime the loop kept, which is where a runtime
            // still holding the drain's in-order rows got folded twice.
            final LiveViewFuzzKnobs knobs = LiveViewFuzzKnobs.randomize(rnd);
            knobs.isPerSegment = true;
            knobs.turnMaxCommits = LiveViewFuzzKnobs.DEFAULT;
            if (rnd.nextBoolean()) {
                knobs.maxChainedBoundaries = 0;
            }
            knobs.apply(AbstractLiveViewTest::setProperty);
            knobs.toSink(knobLog);
        } else {
            setCurrentMicros(MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z"));
            dataStart = MicrosTimestampDriver.floor("2027-01-01T00:00:00.000000Z");
        }

        final int n = 1 + rnd.nextInt(MAX_FRAME);
        final int symCount = 1 + rnd.nextInt(SYMBOLS.length);
        final int stepMode = rnd.nextInt(3);
        final int baseStepMax = stepMode == 0 ? 5_000_000 : stepMode == 1 ? 60_000_000 : 900_000_000;
        final int dayJumpEvery = stepMode == 0 ? 20 : 12;
        final boolean withWhere = rnd.nextInt(3) == 0;
        final boolean isIndexed = anchor != null && rnd.nextInt(4) != 0;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL" + (isIndexed ? " INDEX" : "")
                + ", i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        // Ascending timestamps, with ties across keys for every variant partitioned by sym
        // (all but 5, and every anchored shape): (ts, sym) stays unique, so ts is a total
        // order within each partition. The round-robin slices put the rows of one tie group on
        // different writers. Random symbols and values with occasional NULLs.
        final int tiePct = anchor == null && variant == 5 ? 0 : tiePct(rnd);
        final LiveViewFuzzRows rows = LiveViewFuzzRows.generate(rnd, rowCount, symCount, dataStart, baseStepMax, dayJumpEvery, tiePct);
        final long[] tsv = rows.tsv;
        final int[] symIdx = rows.symIdx;
        final long[] iv = rows.iv;
        final double[] xv = rows.xv;

        // Pre-CREATE history: the earliest rows [0, preCount), committed by a single writer in
        // ts order. Committed history at CREATE puts the view in SEEDING under every start
        // mode; the boundary decides how much of it the sweep emits. An anchored run keeps at
        // least half the rows for its concurrent phase, which is what the run is about.
        final int preCount = seed ? rnd.nextInt(anchor != null ? rowCount / 2 : rowCount + 1) : 0;
        final int startMode = rnd.nextInt(START_FROM_MODES);
        final long boundary = startBoundary(rnd, startMode, tsv, false);
        final String projection = anchor != null ? anchoredViewProjection(variant) : projection(variant, n);
        final String oracleProjection = anchor != null ? anchoredOracleProjection(variant, anchor.bucket) : projection;
        final String window = anchor != null ? " WINDOW w AS (PARTITION BY sym ORDER BY ts " + anchor.clause + ")" : "";
        final String viewSql = "SELECT " + projection + " FROM base" + whereTail(withWhere, Numbers.LONG_NULL) + window;
        final String oracleSql = "SELECT " + oracleProjection + " FROM base" + whereTail(withWhere, boundary);
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + startFromClause(startMode, boundary, false)
                + "AS " + viewSql;

        // The backfilling writer's share of the concurrent suffix, and how far behind the
        // stream it commits each of its rows. With a day jump every dayJumpEvery rows on
        // average, most of those rows land in a closed anchor segment.
        final boolean[] isHeldBack = anchor != null ? new boolean[rowCount] : null;
        final int backfillPct = 10 + rnd.nextInt(21);
        final int backfillLagRows = 2 * dayJumpEvery + rnd.nextInt(4 * dayJumpEvery);
        if (isHeldBack != null) {
            for (int k = preCount; k < rowCount; k++) {
                isHeldBack[k] = rnd.nextInt(100) < backfillPct;
            }
        }

        LOG.info().$("LV concurrent-writer fuzz: variant=").$(variant).$(", rows=").$(rowCount)
                .$(", tiePct=").$(tiePct).$(", ties=").$(rows.getTieCount())
                .$(", n=").$(n).$(", writers=").$(numWriters).$(", symCount=").$(symCount)
                .$(", stepMode=").$(stepMode).$(", concurrentRefresh=").$(concurrentRefresh)
                .$(", preCount=").$(preCount).$(", startMode=").$(startMode).$(", boundary=").$(boundary)
                .$(", where=").$(withWhere).$(", indexed=").$(isIndexed)
                .$(", anchor=").$(anchor != null ? anchor.clause : "none")
                .$(", backfillPct=").$(isHeldBack != null ? backfillPct : 0)
                .$(", backfillLagRows=").$(backfillLagRows)
                .$(", knobs=[").$(knobLog).$("], sql=").$(viewSql).$();

        final TableToken baseToken = engine.verifyTableName("base");
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final AtomicLong backfilledRows = new AtomicLong();
        final AtomicLong midRunRecomputeChecks = new AtomicLong();
        final long segmentRepairs;
        LiveViewRefreshJob job = null;
        try {
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
            assertPersistedStartBound("lv", boundary);
            job = new LiveViewRefreshJob(0, engine, 1);

            if (preCount > 0) {
                driveSeedToCompletion(job, "lv");
            }

            // Concurrent suffix [preCount, rowCount): numWriters threads each own a
            // WalWriter and commit a disjoint round-robin slice in parallel, with an
            // optional refresh driver racing them; or, in an anchored run, stream it in
            // order beside the backfilling writer.
            if (preCount < rowCount) {
                final int driverCount = concurrentRefresh ? 1 : 0;
                final int backfillWriterCount = isHeldBack != null ? 1 : 0;
                final CyclicBarrier barrier = new CyclicBarrier(numWriters + driverCount + backfillWriterCount);
                final AtomicBoolean ingesting = new AtomicBoolean(true);
                final AtomicInteger streamedTo = new AtomicInteger(-1);
                final AtomicInteger streamingWritersDone = new AtomicInteger();
                // A backfilling run's ticks, one commit per writer each, so every drain reads
                // the commits of one tick together (see arriveAndAwaitTick). The driver is a
                // party, and so is every writer until it leaves. Unpaced without a backfill.
                final Phaser ticks = isHeldBack != null ? new Phaser(numWriters + 2) : null;
                // The streaming commits of the current tick; the driver resets it between the
                // tick's two phases, while every writer waits.
                final AtomicInteger tickStreamCommits = new AtomicInteger();

                // Without a backfill, round-robin slices; with one, an in-order stream the
                // writers claim batch by batch, so the late rows are the backfilling writer's.
                final AtomicInteger nextStreamRow = new AtomicInteger(preCount);
                final Object streamLock = new Object();
                final Thread[] writers = new Thread[numWriters];
                for (int w = 0; w < numWriters; w++) {
                    final int batch = 5 + rnd.nextInt(20);
                    writers[w] = isHeldBack != null
                            ? newStreamingWriterThread(
                            w, rowCount, batch, tsv, symIdx, iv, xv, isHeldBack, nextStreamRow, streamLock,
                            ticks, streamedTo, tickStreamCommits, streamingWritersDone, baseToken, barrier, errors)
                            : newConcurrentWriterThread(
                            w, numWriters, preCount, rowCount, batch, tsv, symIdx, iv, xv, baseToken, barrier, errors);
                }
                final Thread backfillWriter = isHeldBack != null ? newBackfillWriterThread(
                        isHeldBack, preCount, rowCount, backfillLagRows, 1 + rnd.nextInt(3), rnd.nextLong(), tsv, symIdx, iv, xv,
                        baseToken, barrier, ticks, streamedTo, tickStreamCommits, streamingWritersDone, numWriters,
                        backfilledRows, errors
                ) : null;

                // Only the driver touches the clock during the concurrent phase; the
                // final quiescence drive runs after it has joined.
                final LiveViewRefreshJob driverJob = job;
                final Thread driver = concurrentRefresh ? new Thread(() -> {
                    try {
                        barrier.await();
                        while (ingesting.get()) {
                            if (ticks != null) {
                                // The commit phase: every writer still running has committed.
                                arriveAndAwaitTick(ticks);
                                tickStreamCommits.set(0);
                            }
                            setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                            drainWalQueue();
                            drainJob(driverJob);
                            if (ticks != null) {
                                // The writers wait, so the base holds still: a view caught up with
                                // it must already equal the recompute. A later repair of the same
                                // segment can heal a wrong row before the run ends, so the
                                // run-end oracle alone misses what this sees.
                                if (assertTierReadConsistent("lv", "base", oracleSql, "1, 2") == TIER_READ_RECOMPUTE) {
                                    midRunRecomputeChecks.incrementAndGet();
                                }
                                // The drain phase: the writers may commit again.
                                arriveAndAwaitTick(ticks);
                            }
                        }
                    } catch (Throwable th) {
                        errors.add(th);
                        if (ticks != null) {
                            ticks.forceTermination();
                        }
                    } finally {
                        clearWorkerThreadLocals();
                    }
                }, "lv-cw-refresh-driver") : null;

                for (Thread t : writers) {
                    t.start();
                }
                if (backfillWriter != null) {
                    backfillWriter.start();
                }
                if (driver != null) {
                    driver.start();
                }
                for (Thread t : writers) {
                    t.join();
                }
                if (backfillWriter != null) {
                    backfillWriter.join();
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
            segmentRepairs = job.getStatsForTest().get(LiveViewRefreshStats.SEGMENT_REPAIR_COUNT);
            if (anchor != null) {
                LOG.info().$("LV concurrent-writer fuzz done: backfilledRows=").$(backfilledRows.get())
                        .$(", midRunRecomputeChecks=").$(midRunRecomputeChecks.get())
                        .$(", segmentRepairs=").$(segmentRepairs)
                        .$(", segmentYields=").$(job.getStatsForTest().get(LiveViewRefreshStats.SEGMENT_YIELD_COUNT))
                        .$(", keyedSegments=").$(job.getStatsForTest().get(LiveViewRefreshStats.KEYED_REPLAY_SEGMENT_COUNT)).$();
                // The run's own evidence: without it a run whose backfill never landed, or whose
                // view never caught up with the base between ticks, passes having raced nothing.
                Assert.assertTrue("the backfilling writer must have committed late rows", backfilledRows.get() > 0);
                Assert.assertTrue(
                        "the driver must have compared a caught-up view with the recompute mid-run",
                        midRunRecomputeChecks.get() > 0
                );
            }
        } finally {
            Misc.free(job);
        }

        // The oracle: the live view must equal its own window query recomputed over the base
        // table under the run's START FROM boundary. ORDER BY 1, 2 - (ts, sym), unique over
        // ties - gives both sides a total order; genericStringMatch tolerates SYMBOL-vs-STRING
        // on passthrough.
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + oracleSql + ") ORDER BY 1, 2",
                "(lv) ORDER BY 1, 2",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
        return segmentRepairs;
    }

    // One refresh turn ahead of a restart of runDedupFuzz, after a base ADD COLUMN no view references,
    // with the engine's closing flag - what a shutdown raises, and what the refresh breaker reads -
    // raised as the turn's drift reaches the refresh failure handler and lowered after the turn. The
    // drift recovery completes ahead of any breaker check, or the breaker ends it; either way nothing
    // is charged and the view stays valid, and the restart that follows recovers it.
    private void cancelADriftRecoveryByShutdown(LiveViewRefreshJob job, LiveViewInstance instance) throws Exception {
        execute("ALTER TABLE base ADD COLUMN cancelled_drift" + driftRecoveryCancellationTurns++ + " INT");
        drainWalQueue();
        final int retryCount = instance.getFlushRetryCount();
        final long retryStartUs = instance.getFlushRetryStartUs();
        final LogCapture capture = walApplySuspensionRule.getCapture();
        capture.drain(LogCapture.STOP_DRAIN_TIMEOUT_MS);
        final int cancelledBefore = capture.countOccurrences(RECOVERY_CANCELLED_LINE);
        final AtomicBoolean isTripped = new AtomicBoolean();
        hooks(job).setRefreshFaultObserver(t -> {
            if (t instanceof TableReferenceOutOfDateException && isTripped.compareAndSet(false, true)) {
                engine.setClosing(true);
            }
        });
        // Past FLUSH EVERY, so the turn drains the column's commit and meets the drift.
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        try {
            job.run();
            if (engine.isClosing() && runClosingBurst(job, instance)) {
                drivenClosingBursts++;
            }
        } finally {
            engine.setClosing(false);
            hooks(job).setRefreshFaultObserver(null);
        }
        Assert.assertFalse("a shutdown must not invalidate the view [reason=" + instance.getInvalidationReason() + ']', instance.isInvalid());
        Assert.assertEquals("a shutdown must not count toward the flush-retry budget", retryCount, instance.getFlushRetryCount());
        Assert.assertEquals("a shutdown must not start a flush-retry streak", retryStartUs, instance.getFlushRetryStartUs());
        capture.drain(LogCapture.STOP_DRAIN_TIMEOUT_MS);
        if (capture.countOccurrences(RECOVERY_CANCELLED_LINE) > cancelledBefore) {
            cancelledDriftRecoveries++;
        }
    }

    // The passes the refresh workers run after a shutdown cancelled a turn, with the engine's closing flag still up
    // until the pools halt. None may start a turn, so the job's refresh latch, the view's refresh faults and its
    // processed watermark stay where the cancelled turn left them. Returns whether the fallback scan would have
    // driven the view at the burst's start - the base's applied head past what the view processed - which is when
    // a burst that started turns would show it.
    private boolean runClosingBurst(LiveViewRefreshJob job, LiveViewInstance instance) {
        final LiveViewTestRefreshHooks hooks = hooks(job);
        final long appliedHead = engine.getTableSequencerAPI()
                .getTxnTracker(instance.getDefinition().getBaseTableToken())
                .getWriterTxn();
        final long processed = instance.getLastProcessedSeqTxn();
        final boolean isDrivable = appliedHead > processed;
        final int latchAttempts = hooks.getRefreshLatchAttempts();
        final long faults = instance.getRefreshFaultCount();
        for (int i = 0; i < CLOSING_BURST_RUNS; i++) {
            job.run();
        }
        Assert.assertEquals("a closing engine must start no refresh turn", latchAttempts, hooks.getRefreshLatchAttempts());
        Assert.assertEquals("a closing engine must cancel no refresh turn", faults, instance.getRefreshFaultCount());
        Assert.assertEquals("a closing engine must flush no lead", processed, instance.getLastProcessedSeqTxn());
        return isDrivable;
    }

    // Differential fuzz for a DEDUP UPSERT|SKIP KEYS(ts, sym) base. Mirrors runFuzz's
    // ingestion shape (pre-CREATE seed history, then per-commit O3 refresh with
    // optional quiescent restarts and removal events), but with a data model built
    // for dedup: timestamps are drawn from a pool small enough to force same-ts /
    // same-(ts, sym) collisions, and a fifth of the emissions re-point onto an
    // earlier (ts, sym) so a real below-frontier dedup is guaranteed. The
    // oracle recomputes the window over the applied (post-dedup) base and orders
    // both sides by (ts, sym::string) - a total order because (ts, sym) is the dedup
    // key; genericStringMatch tolerates SYMBOL-vs-STRING on the sym passthrough.
    // sparseDepartureShape is the SPARSE_DEPARTURE_* shape an unanchored run ends on, through
    // departNewestRowsOverSparseRoots, or -1 for departNewestRows.
    private void runDedupFuzz(
            Rnd rnd,
            int variant,
            int rowCount,
            boolean o3,
            boolean restart,
            boolean seed,
            boolean removals,
            @Nullable LiveViewFuzzAnchor anchor,
            int sparseDepartureShape
    ) throws Exception {
        // Pin the clock a day below the data (see runFuzz): START FROM NOW resolves the
        // CREATE-moment clock into the view's lower bound, and this harness wants that bound to
        // admit the whole dataset. Only an explicit-boundary run excludes any row.
        if (anchor != null) {
            setCurrentMicros(anchor.clockMicros);
        } else if (currentMicros < 0) {
            setCurrentMicros(MicrosTimestampDriver.floor("2025-12-31T00:00:00.000000Z"));
        }
        // An anchored run is an ANCHORED_VARIANT_COUNT shape over the anchor's named window,
        // over a key indexed in most runs, under the repair knobs drawn for it.
        final StringSink knobLog = new StringSink();
        final boolean isIndexed = anchor != null && rnd.nextInt(4) != 0;
        // The chain price arm draws a seal per pass or few and a key budget tight enough to
        // refuse a chain, under the count guard's default, from a sequence of its own.
        final Rnd priceRnd = isChainPriceArm ? chainPriceRnd(rnd) : null;
        final long priceCheckpointRows = priceRnd != null ? 1 + priceRnd.nextInt(4) : LiveViewFuzzKnobs.DEFAULT;
        final long priceScanMaxKeys = priceRnd != null ? (priceRnd.nextBoolean() ? 1 : 2 + priceRnd.nextInt(4)) : LiveViewFuzzKnobs.DEFAULT;
        if (anchor != null) {
            final LiveViewFuzzKnobs knobs = LiveViewFuzzKnobs.randomize(rnd);
            if (priceRnd != null) {
                knobs.checkpointRows = priceCheckpointRows;
                knobs.scanMaxKeys = priceScanMaxKeys;
                knobs.maxChainedBoundaries = 256;
            }
            knobs.apply(AbstractLiveViewTest::setProperty);
            knobs.toSink(knobLog);
        } else if (priceRnd != null) {
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, priceCheckpointRows);
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, priceScanMaxKeys);
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 256);
            knobLog.put("ckptRows=").put(priceCheckpointRows).put(", chained=256, scanMaxKeys=").put(priceScanMaxKeys);
        }

        final int n = 1 + rnd.nextInt(MAX_FRAME);
        final int symCount = 1 + rnd.nextInt(SYMBOLS.length);
        final int stepMode = rnd.nextInt(3);
        final int baseStepMax = stepMode == 0 ? 5_000_000 : stepMode == 1 ? 60_000_000 : 900_000_000;
        final int dayJumpEvery = stepMode == 0 ? 20 : 12;
        // The sparse departure that keeps the timeline needs rows the filter drops.
        // The owed-root arm takes filtered views' departures alone, so it filters every run.
        final boolean withWhere = rnd.nextInt(3) == 0 || sparseDepartureShape == SPARSE_DEPARTURE_KEEP || isDepartedOwedRootArm;

        final String projection = anchor != null ? anchoredViewProjection(variant) : projection(variant, n);
        final String oracleProjection = anchor != null ? anchoredOracleProjection(variant, anchor.bucket) : projection;
        final String window = anchor != null
                ? " WINDOW w AS (PARTITION BY sym ORDER BY ts " + anchoredWindowFrame(variant) + anchor.clause + ")"
                : "";
        // The anchored EXCLUDE CURRENT ROW shapes, whose one call the ANCHOR window dispatches to a ring.
        final boolean isDispatchedRing = anchor != null && !anchoredWindowFrame(variant).isEmpty();
        // UPSERT keeps the last write of a (ts, sym), SKIP keeps the first one. Only testFuzzDedupSkip
        // runs SKIP: a decision of the arm, not a draw, so the pinned-seed shapes keep their sequences.
        final String dedupMode = isDedupSkipArm ? "SKIP" : "UPSERT";

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL" + (isIndexed ? " INDEX" : "") + ", i LONG, x DOUBLE) "
                + "TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP " + dedupMode + " KEYS(ts, sym)");

        // Distinct, strictly-increasing timestamp pool - fewer distinct values than
        // rows, so many emissions collide on one ts (additive across keys) and the
        // forced re-emissions collide on one (ts, sym) (real dedup). Partition spread
        // mirrors runFuzz (per-run step size plus occasional day jumps).
        final int poolSize = Math.max(2, rowCount / 4);
        final long[] pool = new long[poolSize];
        final int maxDayJumps = 30;
        int dayJumps = 0;
        long ts = anchor != null ? anchor.dataStartMicros : MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z");
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
        // recompute oracle reads the final deduped base, so the surviving row (the
        // last write under UPSERT, the first under SKIP) never has to be predicted here.
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
        // Force a dedup fraction: re-point some emissions onto an earlier (ts, sym)
        // so a real below-frontier dedup is guaranteed, exercising the applied-reader
        // replay path, not only the additive fast path. Of the two writes of a
        // (ts, sym), UPSERT keeps the later one and SKIP keeps the earlier one.
        for (int r = 0, forced = rowCount / 5; r < forced; r++) {
            final int dst = 1 + rnd.nextInt(rowCount - 1);
            final int src = rnd.nextInt(dst);
            tsv[dst] = tsv[src];
            symIdx[dst] = symIdx[src];
        }

        // Pre-CREATE history: emissions [0, preCount). Unlike the other arms, ts is NOT
        // monotonic in the emission index here (every row draws from the pool), so the
        // pre-CREATE segment is an arbitrary slice of the timestamp range rather than its
        // prefix - a post-CREATE emission below every seeded row is normal, and legal: the
        // boundary is the only floor, and BEGINNING has none. An explicit boundary therefore
        // cuts the POOL (which is sorted), not the emission array.
        final int preCount = seed ? rnd.nextInt(rowCount + 1) : 0;
        final int startMode = rnd.nextInt(START_FROM_MODES);
        final long boundary = startBoundary(rnd, startMode, pool, false);
        final String viewSql = "SELECT " + projection + " FROM base" + whereTail(withWhere, Numbers.LONG_NULL) + window;
        final String oracleSql = "SELECT " + oracleProjection + " FROM base" + whereTail(withWhere, boundary);
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + startFromClause(startMode, boundary, false)
                + "AS " + viewSql;

        LOG.info().$("LV dedup fuzz: variant=").$(variant).$(", dedupMode=").$(dedupMode).$(", rows=").$(rowCount)
                .$(", n=").$(n).$(", symCount=").$(symCount).$(", stepMode=").$(stepMode)
                .$(", o3=").$(o3).$(", restart=").$(restart).$(", preCount=").$(preCount)
                .$(", startMode=").$(startMode).$(", boundary=").$(boundary)
                .$(", removals=").$(removals).$(", where=").$(withWhere).$(", indexed=").$(isIndexed)
                .$(", sparseDeparture=").$(sparseDepartureShape)
                .$(", knobs=[").$(knobLog).$("], sql=").$(viewSql).$();

        final String oracleOrderedSql = "(" + oracleSql + ") ORDER BY ts, sym::string";
        final String viewOrderedSql = "(lv) ORDER BY ts, sym::string";
        // The run's oracle once failTheOwedSealOfALateRow ran: base_shadow keeps the day the run
        // drops ahead of it.
        final String shadowOracleOrderedSql = "(SELECT " + oracleProjection + " FROM base_shadow"
                + whereTail(withWhere, boundary) + ") ORDER BY ts, sym::string";
        String departureOracleOrderedSql = oracleOrderedSql;
        final StringSink sink = new StringSink();
        String rowsBeforeTheDrop = null;
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
            assertPersistedStartBound("lv", boundary);
            job = new LiveViewRefreshJob(0, engine, 1);

            if (preCount > 0) {
                driveSeed(rnd, job);
            }

            if (preCount < rowCount) {
                final int[] postOrder = segmentOrder(rnd, preCount, rowCount, o3);
                final int[] cb = commitBounds(rnd, postOrder.length);
                for (int c = 0; c + 1 < cb.length; c++) {
                    insertCommit(sink, postOrder, cb[c], cb[c + 1], tsv, symIdx, iv, xv, xNull, null);
                    drainWalQueue();
                    refreshCycle(job);
                    if (isDispatchedRing) {
                        assertDispatchedRingKeepsNoCheckpointDirtySet();
                    }

                    // Remove rows the LV has not yet emitted (see dropDoomedFuturePartition).
                    if (removals && rnd.nextInt(4) == 0) {
                        dropDoomedFuturePartition(sink, job, rnd);
                    }

                    if (restart && rnd.nextInt(3) == 0) {
                        LiveViewInstance inst = engine.getLiveViewRegistry().getViewInstance("lv");
                        if (inst != null
                                && inst.getStateReader().getSeedState() == LiveViewState.SEED_STATE_ACTIVE) {
                            if (driftRecoveryCancellationRnd(rnd).nextInt(3) == 0) {
                                cancelADriftRecoveryByShutdown(job, inst);
                            }
                            coverage.absorb(job);
                            job = Misc.free(job);
                            engine.getLiveViewRegistry().clear();
                            engine.buildViewGraphs();
                            job = new LiveViewRefreshJob(0, engine, 1);
                            if (isChainPriceArm) {
                                driveRefreshToQuiescence(job);
                                TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracleOrderedSql, viewOrderedSql, LOG, true);
                                if (isDispatchedRing) {
                                    assertDispatchedRingKeepsNoCheckpointDirtySet();
                                }
                            }
                        }
                    }
                }
            }

            driveRefreshToQuiescence(job);
            // The oracle over the main loop. The restart that ends departNewestRows can rebuild the
            // view from the base, which erases a ghost row or restores a missing row the main loop
            // left, so the check after that restart does not stand in for this one.
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracleOrderedSql, viewOrderedSql, LOG, true);
            assertRepairRootsAmongTheTwoNewest(engine.getLiveViewRegistry().getViewInstance("lv"));
            if (isDispatchedRing) {
                assertDispatchedRingKeepsNoCheckpointDirtySet();
            }
            if (isTwoCommitDepartureArm && (anchor != null || variant == RANGE_SUM_VARIANT || variant == RANGE_AVG_VARIANT)) {
                job = departAPeriodInTwoCommits(
                        twoCommitDepartureRnd(rnd),
                        job,
                        withWhere,
                        anchor != null ? Numbers.LONG_NULL : n * Micros.MINUTE_MICROS,
                        oracleOrderedSql,
                        viewOrderedSql,
                        true,
                        null
                );
            }
            if (isHeadMissChainPriceArm) {
                // Every run, so each one pins the head miss's decline.
                if (commitHeadMissNewKeyEpisode(priceRnd, job, symCount, oracleOrderedSql, viewOrderedSql)) {
                    TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracleOrderedSql, viewOrderedSql, LOG, true);
                }
            } else if (priceRnd != null && newKeyEpisodeCandidates++ % 2 == 0 && commitNewKeyEpisode(priceRnd, job, symCount)) {
                // One run in two, counted rather than drawn, so every batch runs its share.
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracleOrderedSql, viewOrderedSql, LOG, true);
            }
            if (sparseDepartureShape >= 0) {
                final long rowsCadence = beginSparseCheckpointCadence();
                try {
                    // Which of the run's two frontier-sealing departures restores a root inside the
                    // new frontier's frame; the other warms the frame.
                    final boolean isFrontierRootRestored = frontierStateRouteRnd(rnd).nextBoolean();
                    final boolean isHeadRestamped = departNewestRowsOverSparseRoots(
                            rnd,
                            job,
                            engine.verifyTableName("base"),
                            null,
                            symCount,
                            withWhere,
                            n,
                            sparseDepartureShape,
                            sparseDepartureShape == SPARSE_DEPARTURE_FRONTIER_SEAL && isFrontierRootRestored,
                            false,
                            false
                    );
                    // A crash the truncating departure drew left the image the restart below
                    // comes back from.
                    final boolean isCrashed = checkpointFaults.consumeCrash();
                    commitRowlessStretch(job, null, withWhere, isHeadRestamped, anchor == null);
                    copyTheBaseToItsShadow(true);
                    final long viewMinTs = queryTimestamp("SELECT min(ts) FROM lv");
                    final String rowsBeforeTheSparseDrop = dropTheOldestDayTheViewHolds(rnd, job, oracleOrderedSql, viewOrderedSql);
                    long droppedDayTs = rowsBeforeTheSparseDrop != null ? viewMinTs - viewMinTs % Micros.DAY_MICROS : Numbers.LONG_NULL;
                    coverage.absorb(job);
                    job = Misc.free(job);
                    if (isCrashed) {
                        putTheFrontierTruncateCrashImageBack();
                    }
                    engine.getLiveViewRegistry().clear();
                    engine.buildViewGraphs();
                    job = new LiveViewRefreshJob(0, engine, 1);
                    driveRefreshToQuiescence(job);
                    // The restart after the departure and the stretch without rows: the departure's
                    // own route, over any day the base has lost since, which the view keeps.
                    assertSparseDepartureRestartRoute(isCrashed, rowsBeforeTheSparseDrop != null);
                    assertDepartureOracle(oracleOrderedSql, viewOrderedSql, rowsBeforeTheSparseDrop);
                    if (isCrashed && droppedDayTs != Numbers.LONG_NULL) {
                        // The restart from the crash image stopped the view over the day the run
                        // dropped. The day goes back, which lets the view rebuild, and the
                        // owed-seal episode runs over a base that lost nothing.
                        job = restoreTheDroppedDayAndRestart(job, droppedDayTs, oracleOrderedSql, viewOrderedSql);
                        droppedDayTs = Numbers.LONG_NULL;
                    }
                    final int owedSealOutcome = failTheOwedSealOfALateRow(
                            owedSealEpisodeRnd(rnd),
                            job,
                            null,
                            symCount,
                            n,
                            sparseDepartureShape == SPARSE_DEPARTURE_KEEP,
                            droppedDayTs,
                            oracleOrderedSql,
                            shadowOracleOrderedSql,
                            viewOrderedSql
                    );
                    departureOracleOrderedSql = shadowOracleOrderedSql;
                    coverage.absorb(job);
                    job = Misc.free(job);
                    engine.getLiveViewRegistry().clear();
                    engine.buildViewGraphs();
                    job = new LiveViewRefreshJob(0, engine, 1);
                    driveRefreshToQuiescence(job);
                    job = assertTheRestartAfterTheOwedSealEpisode(
                            job,
                            owedSealOutcome,
                            droppedDayTs,
                            oracleOrderedSql,
                            shadowOracleOrderedSql,
                            viewOrderedSql
                    );
                    if (withWhere) {
                        commitRowlessCyclesAfterARestart(job, null);
                        coverage.absorb(job);
                        job = Misc.free(job);
                        engine.getLiveViewRegistry().clear();
                        engine.buildViewGraphs();
                        job = new LiveViewRefreshJob(0, engine, 1);
                        driveRefreshToQuiescence(job);
                        assertRestoredFromTimeline("lv");
                    }
                    if (sparseDepartureShape == SPARSE_DEPARTURE_FRONTIER_SEAL) {
                        job = departOverSparseRootsFailingTheFrontierSeal(
                                rnd,
                                job,
                                null,
                                symCount,
                                withWhere,
                                n,
                                !isFrontierRootRestored,
                                droppedDayTs,
                                oracleOrderedSql,
                                viewOrderedSql
                        );
                        // The restart after it rebuilt the view from the base, with any day the
                        // run dropped back in it.
                        departureOracleOrderedSql = oracleOrderedSql;
                    }
                } finally {
                    endSparseCheckpointCadence(rowsCadence);
                }
            } else if (!isDedupSkipArm && departNewestRows(rnd, job, engine.verifyTableName("base"), null, symCount, withWhere, anchor == null, anchor == null, anchor == null, oracleOrderedSql, viewOrderedSql)) {
                // Not over a SKIP base, which drops the upserts that move a filtered view's rows out.
                // An anchored run draws repair knobs that can take the departure off the chained
                // resume, and every route there keeps the timeline, a declined chain whose replay
                // emits no row included. The default knobs keep the chain, and only they crash the
                // departure.
                // A crash the departure drew left the image the restart below comes back from.
                final boolean isCrashed = checkpointFaults.consumeCrash();
                commitRowlessStretch(job, null, withWhere, false, anchor == null);
                long droppedDayTs = Numbers.LONG_NULL;
                if (isCrashed) {
                    copyTheBaseToItsShadow(true);
                }
                final long viewMinTs = queryTimestamp("SELECT min(ts) FROM lv");
                rowsBeforeTheDrop = dropTheOldestDayTheViewHolds(rnd, job, oracleOrderedSql, viewOrderedSql);
                if (rowsBeforeTheDrop != null) {
                    droppedDayTs = viewMinTs - viewMinTs % Micros.DAY_MICROS;
                }
                coverage.absorb(job);
                job = Misc.free(job);
                if (isCrashed) {
                    putTheFrontierTruncateCrashImageBack();
                }
                engine.getLiveViewRegistry().clear();
                engine.buildViewGraphs();
                job = new LiveViewRefreshJob(0, engine, 1);
                driveRefreshToQuiescence(job);
                if (isCrashed) {
                    assertTheRestartFromTheFrontierTruncateCrashImage(droppedDayTs != Numbers.LONG_NULL);
                    assertDepartureOracle(oracleOrderedSql, viewOrderedSql, rowsBeforeTheDrop);
                    if (droppedDayTs != Numbers.LONG_NULL) {
                        job = restoreTheDroppedDayAndRestart(job, droppedDayTs, oracleOrderedSql, viewOrderedSql);
                        // The base holds the day again, so the recompute is the run's oracle.
                        rowsBeforeTheDrop = null;
                    }
                } else {
                    // No fault struck the run, so the departure left a root at the view's frontier.
                    assertRestoredFromTimeline("lv");
                    if (isDispatchedRing) {
                        assertDispatchedRingKeepsNoCheckpointDirtySet();
                    }
                }
                if (withWhere) {
                    commitRowlessCyclesAfterARestart(job, null);
                    coverage.absorb(job);
                    job = Misc.free(job);
                    engine.getLiveViewRegistry().clear();
                    engine.buildViewGraphs();
                    job = new LiveViewRefreshJob(0, engine, 1);
                    driveRefreshToQuiescence(job);
                    assertRestoredFromTimeline("lv");
                }
            }
        } finally {
            coverage.absorb(job);
            Misc.free(job);
        }

        // The oracle: the live view equals its own window recomputed over the applied (deduped)
        // base under the run's START FROM boundary. (ts, sym::string) is a total order because
        // (ts, sym) is the dedup key; genericStringMatch tolerates SYMBOL-vs-STRING on the
        // passthrough. A run that dropped a day the view holds compares against the view's own
        // rows from before the drop instead, or against base_shadow, which keeps that day, when an
        // owed-seal episode followed the drop.
        assertDepartureOracle(departureOracleOrderedSql, viewOrderedSql, rowsBeforeTheDrop);

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
        execute("DROP TABLE IF EXISTS base_shadow");
    }

    // Commits rowCount rows of one batch to the base, the first one second above fromTs and each one second above the
    // one before, with i > 0 to pass the optional WHERE, and returns the timestamp of the last.
    private long commitShutdownMidDrainBatch(Rnd episodeRnd, int symCount, long fromTs, int rowCount) throws SqlException {
        final StringSink sink = new StringSink();
        sink.put("INSERT INTO base (ts, sym, i, x) VALUES ");
        long ts = fromTs;
        for (int r = 0; r < rowCount; r++) {
            ts += Micros.SECOND_MICROS;
            sink.put(r == 0 ? "" : ", ").put('(').put(ts).put("::timestamp, '")
                    .put(SYMBOLS[episodeRnd.nextInt(symCount)]).put("', ")
                    .put(1 + episodeRnd.nextInt(1000)).put(", ")
                    .put(episodeRnd.nextInt(1000) / 8.0).put(')');
        }
        execute(sink);
        return ts;
    }

    // A shutdown that cancels a lead drain after it fed rows, ahead of a restart of runFuzz, and the passes of the
    // closing window after it: see testFuzzShutdownMidDrainShape. With the view's flush held off, three in-order
    // batches land above every row of the run: the first committed and applied, which queues its own task, then
    // the second, and the third of at least six rows, both committed and not applied. One run drains the first
    // into the lead, and the task the refreshed notification re-enqueues has the next turn drain the second and
    // the third, with the engine's closing flag raised as the turn reads the third, so the breaker cancels the
    // turn within the third's rows. FLUSH EVERY then falls due, and the burst of the closing window must start no
    // turn: a pass of the fallback scan would drive the view at the base's applied head of the first batch, flush
    // the lead and seal the accumulators holding the second as the root of the first, which the restart would
    // replay the second over. Returns whether the flag rose; it is down again on return.
    private boolean shutDownMidDrain(
            Rnd episodeRnd,
            LiveViewRefreshJob job,
            LiveViewInstance instance,
            int symCount,
            long datasetMaxTs
    ) throws Exception {
        // The run's rows not committed yet sit at or below the dataset's newest one, and an earlier episode's above it.
        final long fromTs = Math.max(datasetMaxTs, queryTimestamp("SELECT max(ts) FROM base"));
        final TableToken baseToken = engine.verifyTableName("base");
        // Installs the hooks ahead of the turns whose latch attempts the burst compares with.
        hooks(job);
        // Holds the flush off: the drains land in the lead until the clock moves past FLUSH EVERY.
        instance.setLastFlushTimeUs(currentMicros);
        final long firstBatchMaxTs = commitShutdownMidDrainBatch(episodeRnd, symCount, fromTs, 1 + episodeRnd.nextInt(3));
        drainWalQueue();
        final long firstBatchSeqTxn = engine.getTableSequencerAPI().lastTxn(baseToken);
        final long secondBatchMaxTs = commitShutdownMidDrainBatch(episodeRnd, symCount, firstBatchMaxTs, 1 + episodeRnd.nextInt(4));
        commitShutdownMidDrainBatch(episodeRnd, symCount, secondBatchMaxTs, 6 + episodeRnd.nextInt(4));
        checkpointFaults.armClosingAtBaseWalRead(baseToken.getDirName(), 2);
        try {
            job.run();
            if (!engine.isClosing()) {
                return false;
            }
            // The cancelled turn fed rows past the lead: the view saw a row above the first batch, while its refresh
            // cursor stayed at the first batch's commit.
            final boolean isLeftover = instance.getLatestSeenTs() > firstBatchMaxTs
                    && instance.getRefreshedUpToSeqTxn() == firstBatchSeqTxn;
            setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
            runClosingBurst(job, instance);
            if (isLeftover) {
                leftoverClosingBursts++;
            }
            return true;
        } finally {
            checkpointFaults.disarmClosingAtBaseWalRead();
            engine.setClosing(false);
        }
    }

    private void runFuzz(
            Rnd rnd,
            int variant,
            int rowCount,
            boolean o3,
            boolean restart,
            boolean seed,
            boolean inMemory
    ) throws Exception {
        runFuzz(rnd, variant, rowCount, o3, restart, seed, inMemory, false, false);
    }

    private void runFuzz(
            Rnd rnd,
            int variant,
            int rowCount,
            boolean o3,
            boolean restart,
            boolean seed,
            boolean inMemory,
            boolean inMemReadBack
    ) throws Exception {
        runFuzz(rnd, variant, rowCount, o3, restart, seed, inMemory, inMemReadBack, false);
    }

    private void runFuzz(
            Rnd rnd,
            int variant,
            int rowCount,
            boolean o3,
            boolean restart,
            boolean seed,
            boolean inMemory,
            boolean inMemReadBack,
            boolean leadReadBack
    ) throws Exception {
        runFuzz(rnd, variant, rowCount, o3, restart, seed, inMemory, inMemReadBack, leadReadBack, 0);
    }

    private void runFuzz(
            Rnd rnd,
            int variant,
            int rowCount,
            boolean o3,
            boolean restart,
            boolean seed,
            boolean inMemory,
            boolean inMemReadBack,
            boolean leadReadBack,
            int maxLateness
    ) throws Exception {
        // Drive a controllable clock so FLUSH EVERY flush gating is deterministic.
        // Pin "now" a day BEFORE the data start (2026-01-01). A non-seed
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
        // LV-space ids) is exercised through Mode B under O3 / restart / seed.
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
                    ? "ts, sym, i, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn"
                    : "ts, i, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn";
        } else if (isDecimal) {
            projection = decimalProjection(decimalFunc, n, rescaleTargetScale);
        } else {
            projection = projection(variant, n);
        }
        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE"
                + (isDecimal ? ", d " + decimalType : "")
                + ") TIMESTAMP(ts) PARTITION BY DAY WAL");

        // Generate the logical dataset: ascending timestamps, random symbols and values with
        // occasional NULLs. A window partitioned by sym also draws timestamp ties across keys,
        // which keep (ts, sym) unique and so every sym partition a total order (see
        // LiveViewFuzzRows). Variant 5 and the read-back outputs are not partitioned by sym, so
        // their timestamps stay unique.
        final int tiePct = inMemReadBack || leadReadBack || variant == 5 ? 0 : tiePct(rnd);
        final LiveViewFuzzRows rows = LiveViewFuzzRows.generate(
                rnd, rowCount, symCount, MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z"),
                baseStepMax, dayJumpEvery, tiePct
        );
        final long[] tsv = rows.tsv;
        final int[] symIdx = rows.symIdx;
        final long[] iv = rows.iv;
        final double[] xv = rows.xv;
        final boolean[] xNull = rows.xNull;
        final String[] dLit = isDecimal ? new String[rowCount] : null;
        if (isDecimal) {
            for (int k = 0; k < rowCount; k++) {
                dLit[k] = decimalLiteral(rnd, decimalPrecision, decimalScale);
            }
        }

        // Pre-CREATE history: the earliest preCount rows by ts. A base with any committed
        // transaction at CREATE puts the view in SEEDING and runs the initial bounded sweep,
        // whatever start mode it drew - so this is what decides whether a run seeds, and the
        // boundary alone decides which of those rows the seed emits. (The old rule that the
        // pre-CREATE segment had to be the earliest rows so no later row fell below the floor
        // is gone with the floor it protected: BEGINNING now has no lower bound at all.)
        final int preCount = seed ? rnd.nextInt(rowCount + 1) : 0;
        final int startMode = rnd.nextInt(START_FROM_MODES);
        // The lead read-back arms take the full cut range too, empty view included: the rows they
        // insert above the global max ts carry i > 0 and clear every boundary the draw can produce,
        // and the crash arm seals its own checkpoint root before building the lead (see below).
        final long boundary = startBoundary(rnd, startMode, tsv, false);
        final String viewSql = "SELECT " + projection + " FROM base" + whereTail(withWhere, Numbers.LONG_NULL);
        final String oracleSql = "SELECT " + projection + " FROM base" + whereTail(withWhere, boundary);
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + (inMem ? "IN MEMORY 60s " : "")
                + startFromClause(startMode, boundary, false)
                + "AS " + viewSql;

        LOG.info().$("LV fuzz: variant=").$(variant).$(", rows=").$(rowCount)
                .$(", n=").$(n).$(", symCount=").$(symCount).$(", stepMode=").$(stepMode)
                .$(", tiePct=").$(tiePct).$(", ties=").$(rows.getTieCount())
                .$(", o3=").$(o3).$(", restart=").$(restart)
                .$(", preCount=").$(preCount).$(", startMode=").$(startMode)
                .$(", boundary=").$(boundary).$(", inMem=").$(inMem)
                .$(", inMemReadBack=").$(inMemReadBack).$(", leadReadBack=").$(leadReadBack)
                .$(", symbolReadBack=").$(symbolReadBack)
                .$(", where=").$(withWhere).$(", decimalType=").$(decimalType)
                .$(", sql=").$(viewSql).$();

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
            assertPersistedStartBound("lv", boundary);
            job = new LiveViewRefreshJob(0, engine, 1);

            // A base holding committed history at CREATE seeds under every start mode - the
            // boundary decides how much of it the sweep emits, which for a literal cutting
            // above the pre-CREATE segment is nothing at all (the empty-seed corner).
            if (preCount > 0) {
                driveSeedToCompletion(job, "lv");
            }

            // Post-CREATE: segment [preCount, rowCount), refreshed per commit so a
            // later (older-ts) commit is genuinely O3 vs the materialized state.
            if (preCount < rowCount) {
                final int[] postOrder = segmentOrder(rnd, preCount, rowCount, o3, maxLateness);
                final int[] cb = commitBounds(rnd, postOrder.length);
                for (int c = 0; c + 1 < cb.length; c++) {
                    insertCommit(sink, postOrder, cb[c], cb[c + 1], tsv, symIdx, iv, xv, xNull, dLit);
                    drainWalQueue();
                    if (restart && inMem && rnd.nextInt(4) == 0) {
                        // Holds the flush off, so the restart below can land on an un-flushed
                        // lead rather than only at a point where everything is on disk.
                        pinnedRefreshCycle(job);
                    } else {
                        refreshCycle(job);
                    }

                    // Simulate a restart (ACTIVE state only), a resident lead included.
                    if (restart && rnd.nextInt(3) == 0) {
                        LiveViewInstance inst = engine.getLiveViewRegistry().getViewInstance("lv");
                        if (inst != null
                                && inst.getStateReader().getSeedState() == LiveViewState.SEED_STATE_ACTIVE) {
                            // In a test that installs CheckpointFaults, a shutdown can cancel a lead drain ahead of
                            // the restart.
                            final Rnd shutdownRnd = shutdownMidDrainRnd(rnd);
                            final boolean isShutDownMidDrain = ff == checkpointFaults
                                    && shutdownRnd.nextInt(2) == 0
                                    && shutDownMidDrain(shutdownRnd, job, inst, symCount, tsv[rowCount - 1]);
                            job = Misc.free(job);
                            engine.getLiveViewRegistry().clear();
                            engine.buildViewGraphs();
                            job = new LiveViewRefreshJob(0, engine, 1);
                            if (isShutDownMidDrain) {
                                // The restart restores the newest root the closing window left. One sealed from the
                                // accumulators the cancelled drain fed would count those rows twice here.
                                driveRefreshToQuiescence(job);
                                TestUtils.assertSqlCursors(
                                        engine,
                                        sqlExecutionContext,
                                        "(" + oracleSql + ") ORDER BY 1, 2",
                                        "(lv) ORDER BY 1, 2",
                                        LOG,
                                        true
                                );
                            }
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
                long leadFloorTs = tsv[rowCount - 1];
                if (restart) {
                    // The crash below recovers the lead by restoring a sealed checkpoint root,
                    // but the run can end with no timeline on disk: a boundary above every row
                    // keeps the view empty, so nothing ever seals, and an O3 resume replay that
                    // emits no row (every row above its anchor fails the WHERE) over a view it
                    // leaves empty has no frontier to keep a root at, so it retires the
                    // timeline by design. The un-flushed lead seals nothing either, so the
                    // restart would rebuild from the applied base, which publishes straight to
                    // LV disk and leaves no lead for assertLeadReadBack to find. Flush one
                    // forward row first: its flush seals a fresh root the crash restores from.
                    // i > 0 keeps the row past the optional WHERE, and the recompute oracle
                    // naturally includes it.
                    leadFloorTs++;
                    execute("INSERT INTO base (ts, sym, i, x) VALUES (" + leadFloorTs + "::timestamp, 'AA', 1, 1.0)");
                    drainWalQueue();
                    refreshCycle(job);
                    driveRefreshToQuiescence(job);
                    assertCheckpointTimelineExists();
                }
                // Build a deterministic un-flushed lead on top of the applied
                // state: pin the flush clock to now and refresh a forward batch
                // above the global max ts so it publishes into the tier as the lead
                // without crossing FLUSH EVERY (no flush, so disk keeps only the
                // applied prefix). The clock is not advanced, so the lead stays.
                buildLeadForReadBack(job, leadFloorTs);
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
            assertLeadReadBack(oracleSql);
            if (symbolReadBack) {
                assertLeadSymbols(symCount);
            }

            if (restart) {
                // Crash-before-flush: drop the in-memory registry (losing the RAM
                // lead) and rebuild from disk, then a restart that recovers the lead
                // by draining the retained base WAL forward. The same cross-checks
                // must hold on the recovered lead.
                restartAndRecoverLead();
                assertLeadReadBack(oracleSql);
                if (symbolReadBack) {
                    assertLeadSymbols(symCount);
                }
            }
        } else {
            // The oracle: the live view must equal its own window query recomputed over the
            // base table under the run's START FROM boundary. ORDER BY 1, 2 - (ts, sym), unique
            // even over timestamp ties - gives both sides a total order; genericStringMatch
            // tolerates SYMBOL-vs-STRING on passthrough.
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "(" + oracleSql + ") ORDER BY 1, 2",
                    "(lv) ORDER BY 1, 2",
                    LOG,
                    true
            );
            assertNoRefreshFaults("lv");
            assertRepairRootsAmongTheTwoNewest(engine.getLiveViewRegistry().getViewInstance("lv"));

            if (inMemReadBack) {
                // Mode B read-back cross-checks, single-threaded now that the worker
                // is freed and the view is quiesced: the tier actually serves the
                // read, and the Mode B result is byte-identical to the forced
                // disk-only path under whatever O3 / restart / seed pattern this
                // run produced.
                assertModeBEngaged("lv");
                assertModeBMatchesDiskOnly("lv", "SELECT * FROM lv");
                // The same differential over the shapes that route LEAD-ONLY rather than
                // seam-split. The view is quiesced, so the lead is flushed and the slot is
                // pure overlap - which is what makes routed and disk-only comparable, and
                // what makes these bite: lead-only must then add NOTHING to the disk scan,
                // so a walk that starts its band at 0 rather than leadStart re-serves the
                // whole overlap on top of disk and diverges here as duplicates, under every
                // O3 / restart / seed pattern the run produced.
                //
                // Each arm reaches a different mechanism, which is why one will not do:
                // the projection prunes ts and ITERATES, so it walks the forward lead-only
                // band (count() below cannot - it is answered from size() without ever
                // calling hasNext); the backward scan walks the descending band, which
                // serves its rows in the opposite order; count() covers size()'s
                // mode-independent diskSize + leadRowCount sum; and sum(i) leaves the
                // record cursor entirely for the page-frame path.
                assertModeBMatchesDiskOnly("lv", "SELECT i, rn FROM lv");
                assertModeBMatchesDiskOnly("lv", "SELECT * FROM lv ORDER BY ts DESC");
                assertModeBMatchesDiskOnly("lv", "SELECT count() FROM lv");
                assertModeBMatchesDiskOnly("lv", "SELECT sum(i) FROM lv");
            }
        }

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
    }

    // Drives late-arriving commits against a live view while an un-flushed lead is
    // held resident, asserting min(lead ts) >= max(disk ts) after every such cycle.
    //
    // Most cycles are pinned - the flush is held off, so the cycle's rows stay in
    // the tier as lead; the rest flush, moving disk forward so the next pinned cycle
    // builds a fresh lead on top of a longer disk prefix. The emission order is
    // mostly ascending with rows released late (see lateRowEmissionOrder), so a
    // below-frontier commit routinely lands with a lead already resident.
    private void runLeadOrderingFuzz(Rnd rnd) throws Exception {
        if (currentMicros < 0) {
            setCurrentMicros(MicrosTimestampDriver.floor("2025-12-31T00:00:00.000000Z"));
        }
        final int rowCount = 120 + rnd.nextInt(160);
        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        final boolean[] xNull = new boolean[rowCount];
        final int symCount = 1 + rnd.nextInt(SYMBOLS.length);
        // Keep the whole run's ts span far inside the IN MEMORY window below: an
        // un-flushed lead only stays resident if the tier can actually hold it. A
        // window narrower than the lead's own ts span stalls the publish and the
        // worker emergency-flushes the lead to disk, which zeroes leadRowCount and
        // makes the compare vacuous. ~280 rows * 1s max step is under 5 minutes of
        // data against the 1h window.
        final int baseStepMax = 1_000_000;
        long ts = MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z");
        for (int k = 0; k < rowCount; k++) {
            ts += 1 + rnd.nextInt(baseStepMax); // keeps ts strictly increasing
            tsv[k] = ts;
            symIdx[k] = rnd.nextInt(20) == 0 ? -1 : rnd.nextInt(symCount); // -1 => NULL symbol
            iv[k] = rnd.nextInt(20) == 0 ? Numbers.LONG_NULL : rnd.nextInt(2000) - 1000;
            xNull[k] = rnd.nextInt(20) == 0;
            xv[k] = xNull[k] ? Double.NaN : rnd.nextDouble() * 1000;
        }

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        // A fixed-width identity output, so the tier can store every column and the
        // lead is genuinely resident. IN MEMORY 1h dwarfs the run's span, so the
        // tier holds the whole output and eviction never touches the lead band.
        final String viewSql = "SELECT ts, sym, i, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base";
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 1h START FROM BEGINNING AS " + viewSql);

        LOG.info().$("LV lead-ordering fuzz: rows=").$(rowCount).$(", symCount=").$(symCount).$();

        final StringSink sink = new StringSink();
        final IntList bounds = new IntList();
        final ObjList<String> keys = new ObjList<>();
        for (int k = 0; k < symCount; k++) {
            keys.add(SYMBOLS[k]);
        }
        LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
        long leadChecks = 0;
        long tierReadChecks = 0;
        try {
            // A fixed 16-way split rather than commitBounds' random 2..10: the check
            // only runs on a pinned cycle carrying a resident lead, and a 2-commit
            // run leaves too few of those to be worth the setup.
            final int commits = 4 * CYCLES_PER_GROUP;
            final int[] order = lateRowEmissionOrder(rnd, rowCount, commits, bounds);
            for (int c = 0; c < commits; c++) {
                insertCommit(sink, order, bounds.getQuick(c), bounds.getQuick(c + 1),
                        tsv, symIdx, iv, xv, xNull, null);
                drainWalQueue();
                // Each 4-cycle group runs: flush, in-order, in-order, late. The
                // flush leaves disk non-empty and the lead at 0; the two in-order
                // pinned cycles rebuild a lead and are where the compare actually
                // runs; the late commit then lands on that resident lead (and, being
                // O3, usually rebuilds the tier and empties it again - hence the
                // in-order cycles carrying the check).
                if (c % CYCLES_PER_GROUP == 0) {
                    refreshCycle(job);
                } else {
                    if (pinnedRefreshCycle(job)) {
                        leadChecks++;
                    }
                    // With the lead resident: a read through the tier must already equal the
                    // recompute once the view has refreshed every applied commit, and the
                    // lead's symbols must read back under one label each.
                    if (assertTierReadConsistent("lv", "base", viewSql, "1, 2, 3, 4") == TIER_READ_RECOMPUTE) {
                        tierReadChecks++;
                    }
                    assertSymbolLabelsConsistent("lv", "sym", keys);
                }
            }
            driveRefreshToQuiescence(job);
        } finally {
            Misc.free(job);
        }

        // Prove the run was not vacuous. Every early return in assertLeadAboveDisk
        // (no lead resident, fence off, empty disk) is a legitimate skip, so a
        // mis-tuned harness can quietly reduce this arm to a no-op that still
        // reports green - an IN MEMORY window narrower than the lead's ts span, for
        // one, makes the worker emergency-flush and leadRowCount is then always 0.
        // Fail loudly instead.
        Assert.assertTrue("the lead-ordering compare never ran - this arm is vacuous", leadChecks > 0);
        Assert.assertTrue("no read through the tier ran over a resident lead - this arm is vacuous", tierReadChecks > 0);

        // The lead must not merely be well-ordered - it must also be right. The
        // usual oracle: the view equals its own recompute over the base.
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + viewSql + ") ORDER BY 1",
                "(lv) ORDER BY 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
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
        // so the views differ only in their window shape and their START FROM
        // boundary, each recomputing against its own.
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

        // Each view draws its OWN start mode and boundary, so one refresh worker maintains K
        // views that disagree about which base rows belong to them while sharing one base WAL
        // stream and one retention floor. The boundary is a membership rule, not a progress one:
        // a view whose boundary excludes the final batch entirely must still consume its seqTxns
        // and advance the floor with the others, or the purge stalls behind it.
        final int[] startModes = new int[viewCount];
        final long[] boundaries = new long[viewCount];
        final String[] oracleSql = new String[viewCount];
        for (int v = 0; v < viewCount; v++) {
            startModes[v] = rnd.nextInt(START_FROM_MODES);
            boundaries[v] = startBoundary(rnd, startModes[v], tsv, false);
            oracleSql[v] = viewSql[v] + whereTail(false, boundaries[v]);
        }

        final StringSink sink = new StringSink();
        LiveViewRefreshJob job = null;
        try {
            for (int v = 0; v < viewCount; v++) {
                final String inMem = rnd.nextBoolean() ? "IN MEMORY 60s " : "";
                execute("CREATE LIVE VIEW " + viewNames[v] + " FLUSH EVERY 100ms " + inMem
                        + startFromClause(startModes[v], boundaries[v], false) + "AS " + viewSql[v]);
                assertPersistedStartBound(viewNames[v], boundaries[v]);
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

        // Every view equals its own from-scratch recompute over the base, under its own boundary.
        for (int v = 0; v < viewCount; v++) {
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "(" + oracleSql[v] + ") ORDER BY 1",
                    "(" + viewNames[v] + ") ORDER BY 1",
                    LOG,
                    true
            );
            assertNoRefreshFaults(viewNames[v]);
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
    // window query recomputed over the base, and every live view line the run
    // logged must print its timestamps in nanoseconds.
    private void runNanosBaseFuzz(
            Rnd rnd,
            int variant,
            int rowCount,
            boolean o3,
            boolean restart,
            boolean seed,
            boolean inMemory
    ) throws Exception {
        // Pin the wall clock (micros) a day below the ns data start. A START FROM NOW view
        // converts that CREATE moment into base (ns) units, so a wall clock below the data
        // keeps its boundary under every ns row timestamp.
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

        final String runMarker = logNanosBaseRunMarker();
        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP_NS, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR WAL");

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

        // Every boundary here lives in NANOSECOND units: NOW scales the micros clock up by the
        // base's driver, and an explicit literal keeps all nine fractional digits through the
        // parser - so a run that cuts the dataset cuts it at an exact ns row timestamp, and a
        // boundary silently truncated to micros would move the cut and fail the oracle.
        final int preCount = seed ? rnd.nextInt(rowCount + 1) : 0;
        final int startMode = rnd.nextInt(START_FROM_MODES);
        final long boundary = startBoundary(rnd, startMode, tsv, true);
        final String viewSql = "SELECT " + projection(variant, n) + " FROM base" + whereTail(withWhere, Numbers.LONG_NULL);
        final String oracleSql = "SELECT " + projection(variant, n) + " FROM base" + whereTail(withWhere, boundary);
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + (inMemory ? "IN MEMORY 60s " : "")
                + startFromClause(startMode, boundary, true)
                + "AS " + viewSql;

        LOG.info().$("LV nanos-base fuzz: variant=").$(variant).$(", rows=").$(rowCount)
                .$(", n=").$(n).$(", symCount=").$(symCount).$(", stepMode=").$(stepMode)
                .$(", o3=").$(o3).$(", restart=").$(restart).$(", preCount=").$(preCount)
                .$(", startMode=").$(startMode).$(", boundary=").$(boundary)
                .$(", inMem=").$(inMemory).$(", where=").$(withWhere).$(", sql=").$(viewSql).$();

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
            assertPersistedStartBound("lv", boundary);
            job = new LiveViewRefreshJob(0, engine, 1);

            if (preCount > 0) {
                driveSeedToCompletion(job, "lv");
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
                                && inst.getStateReader().getSeedState() == LiveViewState.SEED_STATE_ACTIVE) {
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

        // The oracle: the live view must equal its own window query recomputed over the ns base
        // under the run's START FROM boundary.
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + oracleSql + ") ORDER BY 1",
                "(lv) ORDER BY 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
        assertNoLiveViewLineReadsNanosAsMicrosAfter(walApplySuspensionRule.getCapture(), runMarker);

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
    }

    // Differential fuzz over a base whose settled partitions are converted to
    // PARQUET while a live view maintains itself off it (see testFuzzParquetBase).
    // The live view is incremental (NOT seed): its refresh consumes the base
    // WAL stream, not base partitions, so converting an already-consumed partition
    // to parquet is physically transparent. Under in-order ingestion the run also
    // converts settled partitions MID-STREAM (strictly below the current commit's
    // day, so no later row ever writes into a parquet partition), then continues
    // refreshing over the partially-parquet base; every run also converts once
    // more at the end. The recompute oracle is unchanged - the from-scratch
    // recompute reads the same base, parquet partitions and all.
    //
    // NOTE: this arm stays on the incremental (WAL-consuming) path, which is
    // transparent to parquet conversion. SEED over a parquet base is covered
    // separately by runSeedParquetFuzz: it reads base partitions, so it
    // exercises the parquet page-frame skip path the incremental path does not.
    // Operator WAL suspension fuzz (see testFuzzOperatorWalSuspension). Rows in out-of-order commits, part of
    // them committed before CREATE so the view seeds, and one to three suspension episodes over the commits.
    // Every shape an episode ran goes into shapes.
    private void runOperatorSuspensionFuzz(
            Rnd rnd,
            LiveViewMidDrainFault fault,
            boolean isAnchored,
            int rowCount,
            IntHashSet shapes
    ) throws Exception {
        setCurrentMicros(MicrosTimestampDriver.floor("2025-12-31T00:00:00.000000Z"));
        final int symCount = 1 + rnd.nextInt(SYMBOLS.length);
        final int variant = isAnchored ? rnd.nextInt(ANCHORED_VARIANT_COUNT) : FIXED_WIDTH_VARIANTS[rnd.nextInt(FIXED_WIDTH_VARIANTS.length)];
        final int n = 1 + rnd.nextInt(MAX_FRAME);
        final String bucket = rnd.nextBoolean() ? "timestamp_floor('1h', ts)" : "timestamp_floor('1d', ts)";
        final LiveViewFuzzRows rows = LiveViewFuzzRows.generate(
                rnd, rowCount, symCount, MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z"),
                60_000_000, 12, isAnchored || variant != 5 ? tiePct(rnd) : 0
        );
        final long[] tsv = rows.tsv;
        // Drawn after the rows: a seed's first draws are its weakest.
        final boolean isWriteDenied = rnd.nextBoolean();
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, String.valueOf(isWriteDenied));
        final int preCount = rnd.nextInt(3) == 0 ? 1 + rnd.nextInt(rowCount / 2) : 0;
        final String viewSql = isAnchored
                ? "SELECT " + anchoredViewProjection(variant) + " FROM base WINDOW w AS (PARTITION BY sym ORDER BY ts "
                  + anchoredWindowFrame(variant) + "ANCHOR EXPRESSION " + bucket + ")"
                : "SELECT " + projection(variant, n) + " FROM base";
        final String oracleFrom = isAnchored ? "SELECT " + anchoredOracleProjection(variant, bucket) + " FROM " : "SELECT " + projection(variant, n) + " FROM ";

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("DROP TABLE IF EXISTS shadow");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE TABLE shadow (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        final StringSink sink = new StringSink();
        if (preCount > 0) {
            final int[] preOrder = segmentOrder(rnd, 0, preCount, true);
            insertCommit(sink, preOrder, 0, preOrder.length, tsv, rows.symIdx, rows.iv, rows.xv, rows.xNull, null);
            execute(shadowInsert(sink));
            drainWalQueue();
        }
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + viewSql);
        fault.of(engine.verifyTableName("base").getDirName());

        // The rows CREATE did not see, in commits of a few rows, late by a few positions at most.
        final int[] order = segmentOrder(rnd, preCount, rowCount, true, 1 + rnd.nextInt(6));
        final int commitCount = Math.max(1, Math.min(order.length, 10 + rnd.nextInt(20)));
        final int[] bounds = new int[commitCount + 1];
        for (int c = 0; c <= commitCount; c++) {
            bounds[c] = (int) ((long) c * order.length / commitCount);
        }
        // Episodes [start, end) in commit indices: they do not overlap, and each ends by the last commit.
        final int episodeCount = 1 + rnd.nextInt(3);
        final int[] episodeStart = new int[episodeCount];
        final int[] episodeEnd = new int[episodeCount];
        // A seeding view's first episode starts at the first commit in one run of two, so it can catch the sweep.
        int next = preCount > 0 && rnd.nextBoolean() ? 0 : rnd.nextInt(Math.max(1, commitCount / 3));
        int episodes = 0;
        for (int e = 0; e < episodeCount && next < commitCount; e++) {
            episodeStart[episodes] = next;
            episodeEnd[episodes] = Math.min(commitCount, next + 1 + rnd.nextInt(Math.max(1, commitCount / 4)));
            next = episodeEnd[episodes] + 1 + rnd.nextInt(3);
            episodes++;
        }
        LOG.info().$("LV operator suspension fuzz [anchored=").$(isAnchored).$(", variant=").$(variant)
                .$(", rows=").$(rowCount).$(", preCount=").$(preCount).$(", commits=").$(commitCount)
                .$(", episodes=").$(episodes).$(", writeDenied=").$(isWriteDenied).$(", sql=").$(viewSql).I$();

        final LogCapture capture = walApplySuspensionRule.getCapture();
        final boolean[] committed = new boolean[rowCount];
        for (int k = 0; k < preCount; k++) {
            committed[k] = true;
        }
        long maxCommittedTs = preCount > 0 ? tsv[preCount - 1] : Long.MIN_VALUE;
        boolean isRemoved = false;
        boolean isColumnAdded = false;
        LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
        try {
            // A seeding view sweeps part of its history before the first commit, so an episode that starts at
            // the first commit can catch the sweep.
            for (int i = 0; i < 2; i++) {
                job.run();
                drainWalQueue();
            }
            int episode = 0;
            boolean isStatement = false;
            long pausedProcessed = 0;
            long pausedConsumed = 0;
            long pausedViewSeqTxn = 0;
            int pausedRetries = 0;
            boolean isFaultDue = false;
            boolean isLateSincePause = false;
            // A restart rebuild the episode's suspension refused, which runs again after RESUME.
            boolean isRestartRebuildPending = false;
            final boolean[] committedBeforePause = new boolean[rowCount];
            for (int c = 0; c < commitCount; c++) {
                final boolean isStarting = episode < episodes && c == episodeStart[episode];
                final boolean isInside = episode < episodes && c > episodeStart[episode] && c < episodeEnd[episode];
                final boolean isEnding = episode < episodes && c == episodeEnd[episode];
                if (isEnding) {
                    if (isStatement) {
                        execute("ALTER LIVE VIEW lv RESUME WAL");
                    } else {
                        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, null);
                    }
                    isFaultDue = rnd.nextInt(4) == 0;
                    isRestartRebuildPending = false;
                    episode++;
                }
                // A removal of rows the view emitted before the pause, with no late row pending since: the
                // drain after RESUME reads every pending row in order from the WAL, and the rest of the run
                // commits in order, so no repair reads the base below the removal (freeze-and-continue).
                // Nor while a restart rebuild is pending: it runs over the base as RESUME finds it, and the
                // restatement guard stops a rebuild that would drop rows the view retains. Checked after the
                // draw, so the draws of every other episode stay as they were.
                if (isInside && !isAnchored && !isRemoved && !isLateSincePause && rnd.nextInt(5) == 0
                        && !isRestartRebuildPending) {
                    final long day = removableDay(tsv, committedBeforePause);
                    if (day != Numbers.LONG_NULL) {
                        execute("ALTER TABLE base DROP PARTITION LIST '" + Micros.toString(day).substring(0, 10) + "'");
                        drainWalQueue();
                        Arrays.sort(order, bounds[c], order.length);
                        isRemoved = true;
                    }
                }
                if (isInside && !isColumnAdded && rnd.nextInt(4) == 0) {
                    execute("ALTER TABLE base ADD COLUMN note INT");
                    execute("ALTER TABLE shadow ADD COLUMN note INT");
                    drainWalQueue();
                    isColumnAdded = true;
                    shapes.add(SUSPENSION_SHAPE_ADD_COLUMN);
                }
                if (isInside && !isStatement && rnd.nextInt(3) == 0) {
                    job = Misc.free(job);
                    engine.getLiveViewRegistry().clear();
                    engine.buildViewGraphs();
                    job = new LiveViewRefreshJob(0, engine, 1);
                    shapes.add(SUSPENSION_SHAPE_LIST_ACROSS_RESTART);
                }

                if (isStarting) {
                    System.arraycopy(committed, 0, committedBeforePause, 0, rowCount);
                    isLateSincePause = false;
                }
                boolean isLate = false;
                for (int r = bounds[c]; r < bounds[c + 1]; r++) {
                    final int k = order[r];
                    isLate |= tsv[k] < maxCommittedTs;
                    maxCommittedTs = Math.max(maxCommittedTs, tsv[k]);
                    committed[k] = true;
                }
                if (isLate && isInside) {
                    shapes.add(SUSPENSION_SHAPE_LATE_ROW);
                }
                isLateSincePause |= isLate;
                insertCommit(sink, order, bounds[c], bounds[c + 1], tsv, rows.symIdx, rows.iv, rows.xv, rows.xNull, null);
                execute(shadowInsert(sink));
                drainWalQueue();
                final boolean isFaultArmed = isEnding && isFaultDue;
                if (isFaultArmed) {
                    // A one-shot fault for the first drain after the episode, armed once every WAL apply of the
                    // commit is past, and withdrawn after that drain so no later apply meets it.
                    fault.arm(rnd.nextInt(2));
                    isFaultDue = false;
                }

                if (isStarting) {
                    isStatement = rnd.nextInt(3) != 0;
                    final LiveViewInstance before = engine.getLiveViewRegistry().getViewInstance("lv");
                    Assert.assertNotNull(before);
                    if (before.getSuspendedRepair() != null) {
                        shapes.add(SUSPENSION_SHAPE_PARKED_REPAIR);
                    }
                    final boolean isSeeding = before.getStateReader().getSeedState() == LiveViewState.SEED_STATE_SEEDING;
                    capture.drain();
                    final int pausesBefore = capture.countOccurrences(SUSPENSION_PAUSE_LINE);
                    final int restartRebuildsBefore = capture.countOccurrences("live view restart rebuild hit a retryable fault, retrying [view=lv");
                    // A sweep commits through the same fence the mint observer sits in, so a seeding view takes
                    // that start in one episode of two.
                    final int startMode = !isStatement ? 0 : isSeeding && rnd.nextBoolean() ? 2 : rnd.nextInt(5);
                    switch (startMode) {
                        case 1 -> {
                            // Ahead of the latch of the turn that drains this commit.
                            final AtomicBoolean isArmed = new AtomicBoolean(true);
                            hooks(job).setSimulatePeerTurnBeforeRefreshLatch(() -> {
                                if (isArmed.compareAndSet(true, false)) {
                                    suspendLiveView(rnd);
                                }
                            });
                            refreshCycle(job);
                            hooks(job).setSimulatePeerTurnBeforeRefreshLatch(null);
                            if (isArmed.get()) {
                                suspendLiveView(rnd);
                            } else if (isWriteDenied && countPauses(capture) > pausesBefore) {
                                shapes.add(SUSPENSION_SHAPE_REFUSED_ACQUIRE);
                            }
                        }
                        case 2 -> {
                            // At the next commit to the view's WAL.
                            final AtomicBoolean isArmed = suspendAtNextLiveViewCommit();
                            refreshCycle(job);
                            CairoEngine.setRoleSwitchMintObserver(null);
                            if (isArmed.get()) {
                                suspendLiveView(rnd);
                            } else if (isWriteDenied && countPauses(capture) > pausesBefore) {
                                shapes.add(isSeeding ? SUSPENSION_SHAPE_REFUSED_SEED : SUSPENSION_SHAPE_REFUSED_COMMIT);
                            }
                        }
                        case 3 -> {
                            // While the turn handles a fault of its drain, which the commit's rows are past the
                            // apply of by now.
                            final AtomicBoolean isArmed = new AtomicBoolean(true);
                            hooks(job).setRefreshFaultObserver(t -> {
                                if (isArmed.compareAndSet(true, false)) {
                                    suspendLiveView(rnd);
                                }
                            });
                            fault.arm(0);
                            refreshCycle(job);
                            fault.disarm();
                            hooks(job).setRefreshFaultObserver(null);
                            if (isArmed.get()) {
                                suspendLiveView(rnd);
                            }
                        }
                        case 4 -> {
                            // A restart that finds no timeline, whose rebuild commits once SUSPEND has landed.
                            if (!isRemoved && !isSeeding && before.getSuspendedRepair() == null) {
                                job = Misc.free(job);
                                removeTimelineIfAny(before);
                                engine.getLiveViewRegistry().clear();
                                engine.buildViewGraphs();
                                job = new LiveViewRefreshJob(0, engine, 1);
                                final AtomicBoolean isArmed = suspendAtNextLiveViewCommit();
                                refreshCycle(job);
                                CairoEngine.setRoleSwitchMintObserver(null);
                                if (isArmed.get()) {
                                    suspendLiveView(rnd);
                                } else {
                                    capture.drain();
                                    isRestartRebuildPending = capture.countOccurrences(
                                            "live view restart rebuild hit a retryable fault, retrying [view=lv"
                                    ) > restartRebuildsBefore;
                                    if (isWriteDenied && countPauses(capture) > pausesBefore && isRestartRebuildPending) {
                                        shapes.add(SUSPENSION_SHAPE_REFUSED_RESTART_REBUILD);
                                    }
                                }
                            } else {
                                suspendLiveView(rnd);
                                refreshCycle(job);
                            }
                        }
                        default -> {
                            if (isStatement) {
                                suspendLiveView(rnd);
                            } else {
                                setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, engine.verifyTableName("lv").getDirName());
                            }
                            refreshCycle(job);
                        }
                    }
                    // What the turn the suspension caught left is what the pause must keep.
                    final LiveViewInstance paused = engine.getLiveViewRegistry().getViewInstance("lv");
                    Assert.assertNotNull(paused);
                    pausedProcessed = paused.getLastProcessedSeqTxn();
                    pausedConsumed = paused.getStateReader().getLvConsumedSeqTxn();
                    pausedViewSeqTxn = engine.getTableSequencerAPI().getTxnTracker(engine.verifyTableName("lv")).getSeqTxn();
                    pausedRetries = paused.getFlushRetryCount();
                } else if (rnd.nextInt(3) == 0) {
                    // One pass, which can leave a repair parked on its turn budget for the next episode.
                    setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                    job.run();
                    drainWalQueue();
                    if (isFaultArmed) {
                        fault.disarm();
                    }
                } else {
                    refreshCycle(job);
                    if (isFaultArmed) {
                        fault.disarm();
                    }
                }

                final boolean isPaused = episode < episodes && c >= episodeStart[episode] && c < episodeEnd[episode];
                if (isPaused) {
                    for (int pass = 0; pass < 2; pass++) {
                        setCurrentMicros(currentMicros + 61_000_000L);
                        drainWalQueue();
                        drainJob(job);
                    }
                    final LiveViewInstance paused = engine.getLiveViewRegistry().getViewInstance("lv");
                    Assert.assertNotNull(paused);
                    Assert.assertFalse("a paused view must stay valid [reason=" + paused.getInvalidationReason() + ']', paused.isInvalid());
                    Assert.assertEquals("a paused view consumes nothing", pausedProcessed, paused.getLastProcessedSeqTxn());
                    Assert.assertEquals("a paused view applies nothing", pausedConsumed, paused.getStateReader().getLvConsumedSeqTxn());
                    Assert.assertEquals("a paused view commits nothing", pausedViewSeqTxn,
                            engine.getTableSequencerAPI().getTxnTracker(engine.verifyTableName("lv")).getSeqTxn());
                    Assert.assertTrue("a pause is not charged", paused.getFlushRetryCount() <= pausedRetries);
                    sink.clear();
                    printSql("SELECT suspended FROM wal_tables() WHERE name = 'lv'", sink);
                    TestUtils.assertEquals("suspended\n" + isStatement + '\n', sink);
                }
            }
            if (episode < episodes) {
                if (isStatement) {
                    execute("ALTER LIVE VIEW lv RESUME WAL");
                } else {
                    setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, null);
                }
            }
            for (int pass = 0; pass < 4; pass++) {
                setCurrentMicros(currentMicros + 61_000_000L);
                driveRefreshToQuiescence(job);
            }
            fault.disarmAll();
        } finally {
            fault.disarmAll();
            CairoEngine.setRoleSwitchMintObserver(null);
            setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, null);
            Misc.free(job);
        }

        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        Assert.assertFalse("[reason=" + instance.getInvalidationReason() + ']', instance.isInvalid());
        Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
        sink.clear();
        printSql("SELECT suspended FROM wal_tables() WHERE name = 'lv'", sink);
        TestUtils.assertEquals("suspended\nfalse\n", sink);
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + oracleFrom + (isRemoved ? "shadow" : "base") + ") ORDER BY 1, 2",
                "(lv) ORDER BY 1, 2",
                LOG,
                true
        );
        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
        execute("DROP TABLE shadow");
    }

    private void runParquetBaseFuzz(Rnd rnd, int variant, int rowCount, boolean o3, boolean inMemory) throws Exception {
        if (currentMicros < 0) {
            setCurrentMicros(MicrosTimestampDriver.floor("2025-12-31T00:00:00.000000Z"));
        }

        final int n = 1 + rnd.nextInt(MAX_FRAME);
        final int symCount = 1 + rnd.nextInt(SYMBOLS.length);
        final boolean withWhere = rnd.nextInt(3) == 0;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

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

        final int startMode = rnd.nextInt(START_FROM_MODES);
        final long boundary = startBoundary(rnd, startMode, tsv, false);
        final String viewSql = "SELECT " + projection(variant, n) + " FROM base" + whereTail(withWhere, Numbers.LONG_NULL);
        final String oracleSql = "SELECT " + projection(variant, n) + " FROM base" + whereTail(withWhere, boundary);
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + (inMemory ? "IN MEMORY 60s " : "")
                + startFromClause(startMode, boundary, false)
                + "AS " + viewSql;

        LOG.info().$("LV parquet-base fuzz: variant=").$(variant).$(", rows=").$(rowCount)
                .$(", n=").$(n).$(", symCount=").$(symCount).$(", o3=").$(o3)
                .$(", startMode=").$(startMode).$(", boundary=").$(boundary)
                .$(", inMem=").$(inMemory).$(", where=").$(withWhere).$(", sql=").$(viewSql).$();

        final StringSink sink = new StringSink();
        LiveViewRefreshJob job = null;
        try {
            execute(createSql);
            assertPersistedStartBound("lv", boundary);
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

        // The oracle: the live view must equal its own window query recomputed over the base
        // (whose partitions are now a parquet/native mix) under the run's START FROM boundary.
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + oracleSql + ") ORDER BY 1",
                "(lv) ORDER BY 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
    }

    // Seed-over-parquet fuzz (see testFuzzSeedParquetBase). The whole
    // dataset is committed and (a random prefix of its settled partitions)
    // converted to parquet BEFORE the view is created with SEED, so the sweep
    // reads parquet base partitions. A WHERE clause (present most runs) pushes the
    // filter down to the parquet row-group level; with a tiny per-turn budget the
    // sweep resumes with skipRows() many times, each of which must land past the
    // pruned row groups. The oracle is the from-scratch recompute over the
    // (parquet/native) base.
    private void runSeedParquetFuzz(Rnd rnd, int variant, int rowCount, boolean inMemory) throws Exception {
        if (currentMicros < 0) {
            setCurrentMicros(MicrosTimestampDriver.floor("2025-12-31T00:00:00.000000Z"));
        }

        final int n = 1 + rnd.nextInt(MAX_FRAME);
        final int symCount = 1 + rnd.nextInt(SYMBOLS.length);
        // WHERE present on most runs so the parquet row-group pushdown path is the
        // common case; the no-WHERE runs cover the plain (non-pruned) skip.
        final boolean withWhere = rnd.nextInt(4) != 0;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        // Strictly-unique, strictly-increasing timestamps crossing a partition
        // boundary every few rows, so the run has several settled partitions.
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

        // A finite boundary makes the sweep open the parquet cursor AT the bound rather than at
        // the base's first row: whole partitions below it are culled and the first surviving one
        // is binary-searched into. The per-turn skipRows() resume then has to land on the same
        // row that culled-and-pruned scan next yields, which is the pairing this arm exists to
        // check - so it is worth drawing all three modes here, not just BEGINNING.
        final int startMode = rnd.nextInt(START_FROM_MODES);
        final long boundary = startBoundary(rnd, startMode, tsv, false);
        final String viewSql = "SELECT " + projection(variant, n) + " FROM base" + whereTail(withWhere, Numbers.LONG_NULL);
        final String oracleSql = "SELECT " + projection(variant, n) + " FROM base" + whereTail(withWhere, boundary);
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + (inMemory ? "IN MEMORY 60s " : "")
                + startFromClause(startMode, boundary, false)
                + "AS " + viewSql;

        LOG.info().$("LV seed-parquet fuzz: variant=").$(variant).$(", rows=").$(rowCount)
                .$(", n=").$(n).$(", symCount=").$(symCount).$(", inMem=").$(inMemory)
                .$(", startMode=").$(startMode).$(", boundary=").$(boundary)
                .$(", where=").$(withWhere).$(", sql=").$(viewSql).$();

        final StringSink sink = new StringSink();
        LiveViewRefreshJob job = null;
        try {
            // Commit the whole dataset in order (the seed captures pre-CREATE
            // history), then convert a random prefix of settled partitions to
            // parquet before the view exists.
            final int[] order = segmentOrder(rnd, 0, rowCount, false);
            final int[] cb = commitBounds(rnd, order.length);
            for (int c = 0; c + 1 < cb.length; c++) {
                insertCommit(sink, order, cb[c], cb[c + 1], tsv, symIdx, iv, xv, xNull, null);
                drainWalQueue();
            }
            final long firstDay = MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z");
            final long maxDay = tsv[rowCount - 1] - tsv[rowCount - 1] % 86_400_000_000L;
            // Convert [firstDay, cutoff) to parquet; cutoff lands on a random day
            // boundary so the base is a parquet/native mix at seed time.
            long cutoff = firstDay + (1 + rnd.nextInt(8)) * 86_400_000_000L;
            if (cutoff > maxDay) {
                cutoff = maxDay;
            }
            convertPartitionsToParquet(firstDay, cutoff);

            execute(createSql);
            assertPersistedStartBound("lv", boundary);
            job = new LiveViewRefreshJob(0, engine, 1);
            driveSeedToCompletion(job, "lv");
            driveRefreshToQuiescence(job);
        } finally {
            Misc.free(job);
        }

        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + oracleSql + ") ORDER BY 1",
                "(lv) ORDER BY 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");

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
        // The refresh driver advances the flush clock in an unbounded loop, so - like the
        // concurrent-writer arm - the clock is pinned a full YEAR below the data start, keeping
        // a START FROM NOW view's CREATE-moment boundary below every row for the whole run.
        setCurrentMicros(MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z"));
        final long dataStart = MicrosTimestampDriver.floor("2027-01-01T00:00:00.000000Z");

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, vs STRING, vv VARCHAR, i LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");

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

        // The cut stays in the first half: the readers' prefix invariant (a gapless
        // row_number() 1..N over a monotonic row count) is only worth asserting against a view
        // that holds rows, and the run's vacuity guard fails outright on an empty one. A
        // below-bound row that leaked into the view WOULD break the invariant, since it would
        // renumber the rows a reader saw a moment earlier.
        final int startMode = rnd.nextInt(START_FROM_MODES);
        final long boundary = startBoundary(rnd, startMode, tsv, false, rowCount / 2);
        final String viewSql = "SELECT ts, vs, vv, i, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base";
        final String oracleSql = viewSql + whereTail(false, boundary);
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + (inMemory ? "IN MEMORY 60s " : "")
                + startFromClause(startMode, boundary, false)
                + "AS " + viewSql;

        LOG.info().$("LV reader-vs-refresh fuzz: rows=").$(rowCount)
                .$(", o3=").$(o3).$(", inMem=").$(inMemory)
                .$(", startMode=").$(startMode).$(", boundary=").$(boundary).$(", sql=").$(viewSql).$();

        execute(createSql);
        assertPersistedStartBound("lv", boundary);

        final TableToken baseToken = engine.verifyTableName("base");
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final AtomicBoolean running = new AtomicBoolean(true);
        LiveViewRefreshJob job = null;
        Thread driver = null;
        Thread nativeReader = null;
        Thread wrapperReader = null;
        try {
            job = new LiveViewRefreshJob(0, engine, 1);
            final LiveViewRefreshJob driverJob = job;

            // The single refresh driver owns the clock and both apply jobs for
            // the whole concurrent phase; the main thread only commits inserts.
            driver = new Thread(() -> {
                try {
                    while (running.get()) {
                        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                        drainWalQueue();
                        drainJob(driverJob);
                    }
                } catch (Throwable th) {
                    errors.add(th);
                } finally {
                    clearWorkerThreadLocals();
                }
            }, "lv-rvr-refresh-driver");
            final AtomicLong nativeRowsValidated = new AtomicLong();
            final AtomicLong wrapperRowsValidated = new AtomicLong();
            nativeReader = newPrefixInvariantReader("SELECT * FROM lv", running, errors, nativeRowsValidated);
            wrapperReader = newPrefixInvariantReader("(lv) ORDER BY 1", running, errors, wrapperRowsValidated);
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
            // hard-bounded so a stalled refresh fails instead of hanging. The loop
            // exits as soon as the view catches up, so a generous bound costs
            // nothing on the happy path: it only widens the margin before a loaded
            // CI box (where the refresh worker competes with the reader threads for
            // cores) is misreported as a stalled refresh. 120s, was 30s.
            final long lastBaseTxn = engine.getTableSequencerAPI().lastTxn(baseToken);
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            for (int i = 0; i < 12_000 && instance.getLastProcessedSeqTxn() < lastBaseTxn && errors.isEmpty(); i++) {
                Os.sleep(10);
            }

            // Hold the readers open until each has seen the populated view. The o3 arm can publish
            // the whole window in a single terminal REPLACE_RANGE commit - an O3 replay recomputes
            // the view and rewrites the tier in one go - so the view is empty right up to the moment
            // the catch-up loop above returns, and the commit that ends that loop is also the first
            // one a reader could observe. Winding the readers down here therefore hands the vacuity
            // guard below a run in which they only ever saw empty snapshots, and it fails a healthy
            // run. Bounded, and it exits the moment both readers have their row, so it costs nothing
            // on the arm that accumulates incrementally.
            for (int i = 0; i < 1_000
                    && (nativeRowsValidated.get() == 0 || wrapperRowsValidated.get() == 0)
                    && errors.isEmpty(); i++) {
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
            // Stop and join the workers BEFORE freeing the job. An assertion failure or an
            // ingestion error in the try jumps straight here while the driver thread may
            // still be inside drainJob(job); freeing first would be a use-after-free of the
            // job's native state, and skipping the joins would strand non-daemon threads.
            running.set(false);
            joinSilently(driver);
            joinSilently(nativeReader);
            joinSilently(wrapperReader);
            Misc.free(job);
        }

        // The oracle: the live view must equal its own window query recomputed over the base
        // table under the run's START FROM boundary. ORDER BY 1 (the unique ts) gives both sides
        // a total order.
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + oracleSql + ") ORDER BY 1",
                "(lv) ORDER BY 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
    }

    // Freeze-and-continue fuzz for TRUNCATE / DROP PARTITION of already-emitted
    // base rows (see testFuzzRemovalFreezeAndContinue). Phase 1 mirrors runFuzz
    // (optional pre-CREATE seed history, per-commit O3 refresh, optional
    // quiescent restarts) over the first split of the dataset, quiesces, and
    // cross-checks the standard recompute oracle while the base is still
    // intact. The removal event then TRUNCATEs the base or drops one or two
    // whole days of emitted rows; the walk-past refresh must keep the view
    // ACTIVE and its output byte-identical. Phase 2 ingests the remaining rows
    // STRICTLY IN ORDER - an O3 replay after the removal re-reads the physical
    // base and picks head-hit (logical, carried checkpoint state) or head-miss (frozen
    // prefix + physical recompute) depending on checkpoint cadence, which is
    // deliberately path-dependent behaviour an exact-equality oracle cannot
    // pin; the head-miss half has deterministic coverage in LiveViewSmokeTest's
    // frozen-prefix replay tests. The forward-only continuation is the logical
    // stream, so the run-end oracle recomputes over a shadow table holding
    // every generated row. Restarts stay sound because the restore path
    // (replayToApplied) re-feeds the base's RAW WAL, where removed rows'
    // commits still exist; the base stays non-dedup - the dedup arm restores
    // from the applied (physical) base instead.
    //
    // The lagging run (see testFuzzRemovalFreezeAndContinueLagging) replaces the
    // removal event. After the intact-base check it applies, before a single
    // refresh, an out-of-order trigger r_o3 = tsv[kj] + 1, a late row r_ins below
    // the emitted row kj - 1, and a DROP / DETACH PARTITION of every phase-1 day
    // below X, the day of row kj - r_ins either before the removal (and removed with
    // it) or after it (and out of the view: filtered, or below START FROM). The
    // removal sits inside the apply-ahead range, so the repair rebuilds the view
    // from the base it pins, which holds no row below X the view keeps: the view's
    // rows below X must stay byte-for-byte as emitted, and its rows from X up equal
    // the recompute over the remaining base. Phase 2 continues on that state, so
    // the run-end oracle recomputes over the physical base instead of the shadow.
    // The run never truncates and drops only the days below X; it also keeps the
    // removal out of an earlier refresh. In each of those shapes the repair's
    // replacement range reaches rows the removal froze - from the trigger's own
    // timestamp (LiveViewSmokeTest#testO3ApplyAheadTruncateFallsBackToBoundary pins
    // that for TRUNCATE), across a dropped middle day, or from a localized output
    // floor - so the frozen-row check does not hold there.
    private void runRemovalFreezeContinueFuzz(
            Rnd rnd,
            int variant,
            int rowCount,
            boolean truncate,
            boolean restart,
            boolean seed,
            boolean inMemory,
            boolean lagging
    ) throws Exception {
        // Pin the clock a day below the data, like runFuzz: START FROM NOW resolves the
        // CREATE-moment clock into the view's lower bound, and this harness wants that bound
        // to admit the whole dataset. A run that wants a boundary CUTTING the data draws the
        // explicit-timestamp mode instead, which cuts at a known row.
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

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("DROP TABLE IF EXISTS shadow");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        // Ascending timestamps, with ties across keys for every variant partitioned by sym
        // (all but 5): (ts, sym) stays unique, so ts is a total order within each partition.
        // Random symbols and values with occasional NULLs.
        final int tiePct = variant == 5 ? 0 : tiePct(rnd);
        final LiveViewFuzzRows rows = LiveViewFuzzRows.generate(
                rnd, rowCount, symCount, MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z"),
                baseStepMax, dayJumpEvery, tiePct
        );
        final long[] tsv = rows.tsv;
        final int[] symIdx = rows.symIdx;
        final long[] iv = rows.iv;
        final double[] xv = rows.xv;
        final boolean[] xNull = rows.xNull;

        // Phase 1 rows [0, splitPoint) are ingested, refreshed, and quiesced
        // before the removal; phase 2 rows [splitPoint, rowCount) continue after
        // it. Both phases are non-empty so the removal always targets emitted
        // rows and the continuation is always exercised.
        final int splitPoint = rowCount / 3 + rnd.nextInt(rowCount / 3);
        // A lagging run shapes the rows around kj, drawn from (splitPoint / 2 + 1,
        // splitPoint - 2): rows kj - 1 .. kj + 1 lie in phase 1, at or above any START
        // FROM cut, which stays at or below row splitPoint / 2.
        final int kj;
        final long dayX;
        if (lagging) {
            kj = splitPoint / 2 + 2 + rnd.nextInt(splitPoint - splitPoint / 2 - 4);
            // A day jump at kj, so every row below kj lies in a day below X, the day of row kj.
            for (int k = kj; k < rowCount; k++) {
                tsv[k] += 86_400_000_000L;
            }
            // Steps of at least 2 micros below row kj - 1 and above row kj leave r_ins and
            // r_o3 timestamps no row has. A tie makes a step 0, so the shift tops it up to 2.
            final long belowStep = tsv[kj - 1] - tsv[kj - 2];
            if (belowStep < 2) {
                for (int k = kj - 1; k < rowCount; k++) {
                    tsv[k] += 2 - belowStep;
                }
            }
            final long aboveStep = tsv[kj + 1] - tsv[kj];
            if (aboveStep < 2) {
                for (int k = kj + 1; k < rowCount; k++) {
                    tsv[k] += 2 - aboveStep;
                }
            }
            // Rows kj - 1 .. kj + 1 pass the view's filter. Row kj - 1 is the emitted row
            // r_ins sits below, and row kj is the lowest row the repair re-derives. Row
            // kj + 1 lifts the view's frontier, which only rows the view keeps advance,
            // above r_o3, so r_o3 is out of order and triggers the repair. Were r_o3 in
            // order, r_ins would trigger it, and a trigger's own timestamp floors the
            // repair's replacement range below rows the removal froze.
            for (int k = kj - 1; k <= kj + 1; k++) {
                if (iv[k] == Numbers.LONG_NULL || iv[k] <= 0) {
                    iv[k] = 1 + rnd.nextInt(1000);
                }
            }
            dayX = tsv[kj] - tsv[kj] % 86_400_000_000L;
        } else {
            kj = -1;
            dayX = Numbers.LONG_NULL;
        }
        final int preCount = seed ? 1 + rnd.nextInt(splitPoint) : 0;
        // The cut stays inside phase 1's first half, so the removal always has emitted view rows
        // to freeze - a boundary above the whole phase would leave the freeze assertion
        // comparing one empty output against another.
        final int startMode = rnd.nextInt(START_FROM_MODES);
        final long boundary = startBoundary(rnd, startMode, tsv, false, splitPoint / 2);
        final String where = whereTail(withWhere, Numbers.LONG_NULL);
        final String oracleWhere = whereTail(withWhere, boundary);
        final String viewSql = "SELECT " + projection + " FROM base" + where;
        final String oracleSql = "SELECT " + projection + " FROM base" + oracleWhere;
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + (inMemory ? "IN MEMORY 60s " : "")
                + startFromClause(startMode, boundary, false)
                + "AS " + viewSql;

        LOG.info().$("LV removal fuzz: variant=").$(variant).$(", rows=").$(rowCount)
                .$(", tiePct=").$(tiePct).$(", ties=").$(rows.getTieCount())
                .$(", n=").$(n).$(", symCount=").$(symCount).$(", stepMode=").$(stepMode)
                .$(", truncate=").$(truncate).$(", restart=").$(restart)
                .$(", preCount=").$(preCount).$(", startMode=").$(startMode).$(", boundary=").$(boundary)
                .$(", inMem=").$(inMemory).$(", lagging=").$(lagging).$(", kj=").$(kj)
                .$(", where=").$(withWhere).$(", sql=").$(viewSql).$();

        final StringSink sink = new StringSink();
        final StringSink preRemoval = new StringSink();
        final StringSink frozenBelowX = new StringSink();
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
            assertPersistedStartBound("lv", boundary);
            job = new LiveViewRefreshJob(0, engine, 1);

            if (preCount > 0) {
                driveSeedToCompletion(job, "lv");
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
                                && inst.getStateReader().getSeedState() == LiveViewState.SEED_STATE_ACTIVE) {
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
            // physical, so the standard recompute must already hold. ORDER BY 1, 2 -
            // (ts, sym), unique over ties - gives both sides a total order.
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "(" + oracleSql + ") ORDER BY 1, 2",
                    "(lv) ORDER BY 1, 2",
                    LOG,
                    true
            );
            assertNoRefreshFaults("lv");
            assertRepairRootsAmongTheTwoNewest(engine.getLiveViewRegistry().getViewInstance("lv"));
            printSql("(lv) ORDER BY 1, 2", preRemoval);

            if (lagging) {
                // Every phase-1 day below X, which the removal takes.
                final LongHashSet doomedDays = new LongHashSet();
                final ObjList<String> removedDays = new ObjList<>();
                for (int k = 0; k < kj; k++) {
                    final long dayFloor = tsv[k] - tsv[k] % 86_400_000_000L;
                    if (doomedDays.add(dayFloor)) {
                        removedDays.add(Micros.toString(dayFloor).substring(0, 10));
                    }
                }
                // r_ins after the removal must stay out of the view: a row the filter drops,
                // or, without a filter, a row below a START FROM that lies inside a doomed day.
                long belowStartTs = Numbers.LONG_NULL;
                if (!withWhere && boundary != Numbers.LONG_NULL) {
                    final long startDay = boundary - boundary % 86_400_000_000L;
                    if (doomedDays.contains(startDay)) {
                        long candidate = boundary - 1;
                        while (candidate >= startDay && Arrays.binarySearch(tsv, candidate) >= 0) {
                            candidate--;
                        }
                        if (candidate >= startDay) {
                            belowStartTs = candidate;
                        }
                    }
                }
                final boolean isRemovalFirst = (withWhere || belowStartTs != Numbers.LONG_NULL) && rnd.nextBoolean();
                final long rIns = isRemovalFirst && !withWhere ? belowStartTs : tsv[kj - 1] - 1;
                final int rInsSym = rnd.nextInt(20) == 0 ? -1 : rnd.nextInt(symCount);
                final long rInsI;
                if (isRemovalFirst && withWhere) {
                    rInsI = rnd.nextBoolean() ? Numbers.LONG_NULL : -rnd.nextInt(1001);
                } else {
                    rInsI = rnd.nextInt(20) == 0 ? Numbers.LONG_NULL : (rnd.nextInt(2001) - 1000);
                }
                final boolean rInsXNull = rnd.nextInt(20) == 0;
                final double rInsX = rnd.nextDouble() * 1000.0;
                if (!isRemovalFirst) {
                    // r_ins lands in the last doomed day unless row kj - 1 sits exactly on a
                    // day floor; the removal takes r_ins's day either way.
                    final long rInsDay = rIns - rIns % 86_400_000_000L;
                    if (doomedDays.add(rInsDay)) {
                        removedDays.add(Micros.toString(rInsDay).substring(0, 10));
                    }
                }
                final boolean isDetach = rnd.nextBoolean();
                final StringSink removalSql = new StringSink();
                removalSql.put("ALTER TABLE base ").put(isDetach ? "DETACH" : "DROP").put(" PARTITION LIST ");
                for (int d = 0, m = removedDays.size(); d < m; d++) {
                    if (d > 0) {
                        removalSql.put(", ");
                    }
                    removalSql.put('\'').put(removedDays.getQuick(d)).put('\'');
                }
                final long rO3 = tsv[kj] + 1;
                LOG.info().$("LV removal fuzz lagging: dayX=").$ts(dayX).$(", rO3=").$ts(rO3)
                        .$(", rIns=").$ts(rIns).$(", removalFirst=").$(isRemovalFirst)
                        .$(", removal=").$(removalSql).$();

                // Not vacuous: the view holds an emitted row between r_ins and X (row
                // kj - 1), which a repair floor taken from r_ins would reach, and r_o3 sits
                // below the view's frontier, so it triggers the repair.
                try (
                        RecordCursorFactory factory = select("SELECT count() FROM lv WHERE ts > " + rIns + " AND ts < " + dayX);
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertTrue("the view must hold an emitted row between r_ins and X", cursor.getRecord().getLong(0) > 0);
                }
                final LiveViewInstance lagged = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lagged);
                Assert.assertTrue("r_o3 must sit below the view's frontier", rO3 < lagged.getLatestSeenTs());
                printSql("(SELECT * FROM lv WHERE ts < " + dayX + ") ORDER BY 1, 2", frozenBelowX);

                // The trigger and the commits after it all apply before a single refresh.
                insertRow(sink, rO3, rnd.nextInt(20) == 0 ? -1 : rnd.nextInt(symCount), 1 + rnd.nextInt(1000),
                        rnd.nextDouble() * 1000.0, rnd.nextInt(20) == 0);
                if (isRemovalFirst) {
                    execute(removalSql);
                    insertRow(sink, rIns, rInsSym, rInsI, rInsX, rInsXNull);
                } else {
                    insertRow(sink, rIns, rInsSym, rInsI, rInsX, rInsXNull);
                    execute(removalSql);
                }
                drainWalQueue();
                driveRefreshToQuiescence(job);

                final LiveViewInstance repaired = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(repaired);
                Assert.assertFalse("LV must stay valid after the lagging repair", repaired.isInvalid());
                sink.clear();
                printSql("(SELECT * FROM lv WHERE ts < " + dayX + ") ORDER BY 1, 2", sink);
                TestUtils.assertEquals("the lagging repair must keep the rows the removal froze", frozenBelowX, sink);
                assertNoRefreshFaults("lv");
            } else {
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
                printSql("(lv) ORDER BY 1, 2", sink);
                TestUtils.assertEquals("removal must not retract emitted LV rows", preRemoval, sink);
            }

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
                            && inst.getStateReader().getSeedState() == LiveViewState.SEED_STATE_ACTIVE) {
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

        if (lagging) {
            // The rows below X stay as emitted, and the repair left the window state of a
            // recompute over the base it pinned, so the rows from X up equal the recompute
            // over the base that remains: the removal left no row below X the view keeps.
            sink.clear();
            printSql("(SELECT * FROM lv WHERE ts < " + dayX + ") ORDER BY 1, 2", sink);
            TestUtils.assertEquals("the continuation must keep the rows the removal froze", frozenBelowX, sink);
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "(" + oracleSql + ") ORDER BY 1, 2",
                    "(SELECT * FROM lv WHERE ts >= " + dayX + ") ORDER BY 1, 2",
                    LOG,
                    true
            );
            assertNoRefreshFaults("lv");
            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
            return;
        }

        // The oracle: the view must equal the window query recomputed over the LOGICAL dataset -
        // every generated row, as if nothing was removed - materialized into the shadow table in
        // ts order, and bounded by the run's START FROM boundary, which the accumulators applied
        // to the rows they walked.
        execute("CREATE TABLE shadow (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        insertLogicalDataset("shadow", tsv, symIdx, iv, xv, xNull);
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(SELECT " + projection + " FROM shadow" + oracleWhere + ") ORDER BY 1, 2",
                "(lv) ORDER BY 1, 2",
                LOG,
                true
        );

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
        execute("DROP TABLE shadow");
    }

    // Base removal under an anchored view (see testFuzzRemovalFreezeAndContinueAnchored and
    // testFuzzBaseTtlAnchored). The shape of runRemovalFreezeContinueFuzz and runBaseTtlFuzz:
    // phase one is ingested out of order and quiesced, the removal drops rows the view has
    // already emitted, and phase two continues above them. What differs is the window - an
    // ANCHOR clause from LiveViewFuzzAnchor over an indexed key in most runs, with the repair
    // knobs drawn per run - and that phase two may arrive out of order too.
    //
    // A repair after the removal reads the base, which no longer holds the removed rows. Rows
    // below its output floor stay as the view emitted them, and rows above it are derived
    // again from the base as it stands, and a resume can take its floor from a root anywhere
    // below the correction, removed rows included. What a segment holds once it lies above a
    // floor that reached below the removal therefore depends on the route the repair took. An
    // anchored segment's rows are a function of that segment's base rows alone, though, so
    // every segment above every removed row equals the recompute over the logical dataset
    // (every row ever inserted, held in a shadow table), whatever repaired it. Below that, the
    // log and the view's state speak for the run.
    // TTL keeps evicting behind phase two, so a TTL run keeps phase two in timestamp order and
    // its whole output equals the recompute over the logical dataset. A TTL run with
    // isLaggingTtlEpisode partitions its base by the hour under a daily anchor, and ends on
    // runLaggingTtlEpisode once that check passed.
    //
    // Every run must end ACTIVE, except that a restart after the removal may stop the view as
    // rebuild_blocked when it finds no checkpoint to restore: the rebuild would drop rows the
    // view retains. No run may log a CRITICAL line from the live view package; a stale row a
    // repair leaves behind in a segment it rewrote fails its row-count proof at that level.
    private void runAnchoredRemovalFuzz(
            Rnd rnd,
            int variant,
            int rowCount,
            int removal,
            boolean restart,
            boolean seed,
            boolean inMemory,
            boolean isLaggingTtlEpisode
    ) throws Exception {
        Assert.assertTrue("only a TTL run ends on a lagging TTL episode", !isLaggingTtlEpisode || removal == REMOVAL_TTL);
        // Drawn apart from the run's sequence, which drawing it leaves where it was.
        final Rnd laggingTtlEpisodeRnd = laggingTtlEpisodeRnd(rnd);
        final int ttlHours = isLaggingTtlEpisode ? 1 + laggingTtlEpisodeRnd.nextInt(3) : 0;
        final LiveViewFuzzKnobs knobs = LiveViewFuzzKnobs.randomize(rnd);
        knobs.apply(AbstractLiveViewTest::setProperty);
        final StringSink knobLog = new StringSink();
        knobs.toSink(knobLog);
        // The 1970 shape is left out: a TTL run moves the clock above the data, which for 1970
        // data is a clock the next run cannot pin below its own. A lagging TTL episode needs a
        // daily anchor, whose segment spans several hour partitions.
        final LiveViewFuzzAnchor anchor = LiveViewFuzzAnchor.draw(
                rnd,
                isLaggingTtlEpisode
                        ? LiveViewFuzzAnchor.DAILY_PLAIN + rnd.nextInt(2)
                        : rnd.nextInt(LiveViewFuzzAnchor.DAILY_EPOCH)
        );
        setCurrentMicros(anchor.clockMicros);
        final boolean isIndexed = rnd.nextInt(4) != 0;
        final int symCount = 1 + rnd.nextInt(SYMBOLS.length);
        final int stepMode = rnd.nextInt(3);
        final int baseStepMax = stepMode == 0 ? 5_000_000 : stepMode == 1 ? 60_000_000 : 900_000_000;
        final int dayJumpEvery = stepMode == 0 ? 20 : 12;
        final int tiePct = tiePct(rnd);
        final LiveViewFuzzRows rows = LiveViewFuzzRows.generate(
                rnd, rowCount, symCount, anchor.dataStartMicros, baseStepMax, dayJumpEvery, tiePct);
        final long[] tsv = rows.tsv;
        final LongHashSet usedTs = new LongHashSet();
        for (int k = 0; k < rowCount; k++) {
            usedTs.add(tsv[k]);
        }
        final int splitPoint = rowCount / 3 + rnd.nextInt(rowCount / 3);
        if (removal == REMOVAL_TTL) {
            // Three days above phase one, so applying the first phase-two row moves the base's
            // maximum well past the one-day TTL window and evicts every phase-one partition.
            // Every row moves alike, so the ties stay.
            for (int k = splitPoint; k < rowCount; k++) {
                tsv[k] += 3 * 86_400_000_000L;
            }
        }
        final boolean isPhaseTwoO3 = removal != REMOVAL_TTL && rnd.nextBoolean();
        final int maxLateness = rnd.nextBoolean() ? 0 : 1 + rnd.nextInt(20);
        final int preCount = seed ? 1 + rnd.nextInt(splitPoint) : 0;
        final int pinPct = inMemory ? rnd.nextInt(40) : 0;

        final String viewSql = "SELECT " + anchoredViewProjection(variant) + " FROM base"
                + " WINDOW w AS (PARTITION BY sym ORDER BY ts " + anchor.clause + ")";
        final String oracleProjection = anchoredOracleProjection(variant, anchor.bucket);
        LOG.info().$("LV anchored removal fuzz: variant=").$(variant).$(", rows=").$(rowCount)
                .$(", removal=").$(removal).$(", symCount=").$(symCount).$(", indexed=").$(isIndexed)
                .$(", anchor=").$(anchor.clause).$(", stepMode=").$(stepMode).$(", tiePct=").$(tiePct)
                .$(", ties=").$(rows.getTieCount()).$(", splitPoint=").$(splitPoint)
                .$(", phaseTwoO3=").$(isPhaseTwoO3).$(", maxLateness=").$(maxLateness)
                .$(", restart=").$(restart).$(", preCount=").$(preCount).$(", inMem=").$(inMemory)
                .$(", pinPct=").$(pinPct).$(", laggingTtlEpisode=").$(isLaggingTtlEpisode)
                .$(", ttlHours=").$(ttlHours).$(", knobs=[").$(knobLog).$("]").$();

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("DROP TABLE IF EXISTS shadow");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL" + (isIndexed ? " INDEX" : "")
                + ", i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY "
                + (isLaggingTtlEpisode ? "HOUR TTL " + ttlHours + " HOURS" : "DAY" + (removal == REMOVAL_TTL ? " TTL 1 DAY" : ""))
                + " WAL");

        final StringSink sink = new StringSink();
        long maxRemovedTs = Numbers.LONG_NULL;
        long dropLo = Numbers.LONG_NULL;
        long dropHi = Numbers.LONG_NULL;
        // Late rows phase two sends into the dropped days, kept for the shadow table.
        final LongList lateIntoRemoved = new LongList();
        final IntList lateIntoRemovedKeys = new IntList();
        // The rows of the two-commit departure episode the bands left, kept for the shadow table.
        final StringSink twoCommitSurvivingRows = new StringSink();
        int phaseTwoFrom = splitPoint;
        boolean isRestartedAfterRemoval = false;
        LiveViewRefreshJob job = null;
        final String runMarker = logAnchoredRemovalRunMarker();
        try {
            if (preCount > 0) {
                final int[] preOrder = segmentOrder(rnd, 0, preCount, true);
                final int[] cb = commitBounds(rnd, preOrder.length);
                for (int c = 0; c + 1 < cb.length; c++) {
                    insertCommit(sink, preOrder, cb[c], cb[c + 1], tsv, rows.symIdx, rows.iv, rows.xv, rows.xNull, null);
                    drainWalQueue();
                }
            }
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms " + (inMemory ? "IN MEMORY 60s " : "")
                    + "START FROM BEGINNING AS " + viewSql);
            job = new LiveViewRefreshJob(0, engine, 1);
            if (preCount > 0) {
                driveSeedToCompletion(job, "lv");
            }

            // Phase 1: out of order over an intact base.
            if (preCount < splitPoint) {
                final int[] phase1Order = segmentOrder(rnd, preCount, splitPoint, true, maxLateness);
                final int[] cb = commitBounds(rnd, phase1Order.length);
                for (int c = 0; c + 1 < cb.length; c++) {
                    insertCommit(sink, phase1Order, cb[c], cb[c + 1], tsv, rows.symIdx, rows.iv, rows.xv, rows.xNull, null);
                    drainWalQueue();
                    job = anchoredRemovalPass(rnd, job, pinPct, restart);
                }
            }
            driveRefreshToQuiescence(job);
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "(SELECT " + oracleProjection + " FROM base) ORDER BY 1, 2",
                    "(lv) ORDER BY 1, 2",
                    LOG,
                    true
            );
            final StringSink preRemoval = new StringSink();
            printSql("(lv) ORDER BY 1, 2", preRemoval);

            // The removal: every removed row is one the view has already emitted.
            switch (removal) {
                case REMOVAL_TRUNCATE -> {
                    execute("TRUNCATE TABLE base");
                    maxRemovedTs = tsv[splitPoint - 1];
                }
                case REMOVAL_DROP_PARTITION -> {
                    final LongHashSet seenDays = new LongHashSet();
                    final ObjList<String> days = new ObjList<>();
                    for (int k = 0; k < splitPoint; k++) {
                        final long dayFloor = tsv[k] - tsv[k] % 86_400_000_000L;
                        if (seenDays.add(dayFloor)) {
                            days.add(Micros.toString(dayFloor).substring(0, 10));
                        }
                    }
                    final int dropIdx = rnd.nextInt(days.size());
                    final int dropCount = rnd.nextBoolean() && dropIdx + 1 < days.size() ? 2 : 1;
                    sink.clear();
                    sink.put("ALTER TABLE base DROP PARTITION LIST '").put(days.getQuick(dropIdx)).put('\'');
                    if (dropCount == 2) {
                        sink.put(", '").put(days.getQuick(dropIdx + 1)).put('\'');
                    }
                    execute(sink);
                    dropLo = MicrosTimestampDriver.floor(days.getQuick(dropIdx) + "T00:00:00.000000Z");
                    dropHi = dropLo + dropCount * 86_400_000_000L;
                    for (int k = 0; k < splitPoint; k++) {
                        if (tsv[k] >= dropLo && tsv[k] < dropHi) {
                            maxRemovedTs = Math.max(maxRemovedTs, tsv[k]);
                        }
                    }
                }
                case REMOVAL_TTL -> {
                    // TTL evicts relative to min(maxTimestamp, wall clock), so the clock moves
                    // above the trigger row before it lands.
                    setCurrentMicros(tsv[splitPoint] + 86_400_000_000L);
                    insertCommit(sink, new int[]{splitPoint}, 0, 1, tsv, rows.symIdx, rows.iv, rows.xv, rows.xNull, null);
                    phaseTwoFrom = splitPoint + 1;
                }
                default -> throw new IllegalArgumentException("removal=" + removal);
            }
            drainWalQueue();
            refreshCycle(job);
            final LiveViewInstance afterRemoval = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(afterRemoval);
            Assert.assertFalse("the view must stay valid after the base lost rows it emitted", afterRemoval.isInvalid());
            sink.clear();
            printSql("(lv) ORDER BY 1, 2", sink);
            if (removal == REMOVAL_TTL) {
                // The trigger row sorts last, so the pre-removal output stays a prefix.
                Assert.assertTrue("TTL eviction must not retract emitted rows", sink.toString().startsWith(preRemoval.toString()));
            } else {
                TestUtils.assertEquals("the removal must not retract emitted rows", preRemoval, sink);
            }

            // Phase 2, above the removal; out of order unless TTL keeps evicting behind it.
            if (phaseTwoFrom < rowCount) {
                final int[] phase2Order = segmentOrder(rnd, phaseTwoFrom, rowCount, isPhaseTwoO3, maxLateness);
                final int[] cb = commitBounds(rnd, phase2Order.length);
                for (int c = 0; c + 1 < cb.length; c++) {
                    insertCommit(sink, phase2Order, cb[c], cb[c + 1], tsv, rows.symIdx, rows.iv, rows.xv, rows.xNull, null);
                    if (isPhaseTwoO3 && dropLo != Numbers.LONG_NULL && rnd.nextInt(3) == 0) {
                        // A late row into a dropped day, on the key of a row that day held: a
                        // repair of its segment meets the stale rows the view keeps for that key.
                        final int k = rnd.nextInt(splitPoint);
                        final long lateTs = tsv[k] + 1;
                        if (tsv[k] >= dropLo && tsv[k] < dropHi && !usedTs.contains(lateTs)) {
                            usedTs.add(lateTs);
                            lateIntoRemoved.add(lateTs);
                            lateIntoRemovedKeys.add(rows.symIdx[k]);
                            insertCommit(sink, new int[]{0}, 0, 1, new long[]{lateTs}, new int[]{rows.symIdx[k]},
                                    new long[]{1}, new double[]{1.0}, new boolean[]{false}, null);
                        }
                    }
                    drainWalQueue();
                    final LiveViewRefreshJob before = job;
                    job = anchoredRemovalPass(rnd, job, pinPct, restart);
                    isRestartedAfterRemoval |= job != before;
                }
            }
            driveRefreshToQuiescence(job);
            if (removal != REMOVAL_TTL) {
                // A period above every removed row whose newest rows two pure-delete bands remove,
                // the second resuming from a root above the view's new frontier. The rows that stay
                // join the logical dataset the oracle below recomputes.
                job = departAPeriodInTwoCommits(
                        twoCommitDepartureRnd(rnd),
                        job,
                        false,
                        Numbers.LONG_NULL,
                        null,
                        "(lv) ORDER BY 1, 2",
                        false,
                        twoCommitSurvivingRows
                );
            }
        } finally {
            Misc.free(job);
        }

        assertNoLiveViewCriticalLineAfter(walApplySuspensionRule.getCapture(), runMarker);
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        if (instance.isCheckpointRecoveryBlocked()) {
            Assert.assertTrue(
                    "the view stopped in recovery phase " + LiveViewCheckpointRecoveryPhase.routeName(instance.getCheckpointRestoreRoute())
                            + " with no restart after the removal",
                    isRestartedAfterRemoval
            );
            Assert.assertEquals(
                    "a restart after a removal may stop the view only on a refused rebuild",
                    LiveViewCheckpointRecoveryPhase.ROUTE_REBUILD_BLOCKED,
                    instance.getCheckpointRestoreRoute()
            );
            LOG.info().$("LV anchored removal fuzz: the view stopped as rebuild_blocked after a restart").$();
        } else {
            Assert.assertFalse("view invalid: " + instance.getInvalidationReason(), instance.isInvalid());
            execute("CREATE TABLE shadow (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            insertLogicalDataset("shadow", tsv, rows.symIdx, rows.iv, rows.xv, rows.xNull);
            for (int r = 0, n = lateIntoRemoved.size(); r < n; r++) {
                final int key = lateIntoRemovedKeys.getQuick(r);
                execute("INSERT INTO shadow (ts, sym, i, x) VALUES (" + lateIntoRemoved.getQuick(r) + "::timestamp, "
                        + (key < 0 ? "null" : "'" + SYMBOLS[key] + "'") + ", 1, 1.0)");
            }
            if (twoCommitSurvivingRows.length() > 0) {
                execute("INSERT INTO shadow (ts, sym, i, x) VALUES " + twoCommitSurvivingRows);
            }
            drainWalQueue();
            final String shadowOracle = "SELECT " + oracleProjection + " FROM shadow";
            if (maxRemovedTs == Numbers.LONG_NULL || !isPhaseTwoO3) {
                // Nothing removed (a TTL run checks its eviction separately), or phase two in
                // order: nothing repaired a segment that held removed rows.
                TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                        "(" + shadowOracle + ") ORDER BY 1, 2", "(lv) ORDER BY 1, 2", LOG, true);
            } else {
                // The segments above every removed row equal the recompute.
                final long maxRemovedBucket = queryTimestamp("SELECT max(" + anchor.bucket + ") FROM shadow WHERE ts <= " + maxRemovedTs);
                final long cleanFromTs = queryTimestamp("SELECT min(ts) FROM shadow WHERE " + anchor.bucket + " > " + maxRemovedBucket + "::timestamp");
                if (cleanFromTs != Numbers.LONG_NULL) {
                    TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                            "(" + shadowOracle + ") WHERE ts >= " + cleanFromTs + " ORDER BY 1, 2",
                            "(SELECT * FROM lv WHERE ts >= " + cleanFromTs + ") ORDER BY 1, 2", LOG, true);
                }
            }
            execute("DROP TABLE shadow");
            if (isLaggingTtlEpisode) {
                runLaggingTtlEpisode(laggingTtlEpisodeRnd, oracleProjection, symCount, ttlHours);
                assertNoLiveViewCriticalLineAfter(walApplySuspensionRule.getCapture(), runMarker);
            }
        }

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
    }

    // The episode a TTL run of runAnchoredRemovalFuzz with isLaggingTtlEpisode ends on, over its
    // base partitioned by the hour with a TTL of ttlHours: a lagging out-of-order repair whose open
    // anchor segment holds an hour the TTL evicted and hours that survive it. In one daily segment
    // above every row of the run: rows in the segment's first whole hour H0, which the view emits;
    // rows three hours up, in H3; and a row at F, by whose apply the TTL has evicted H0 while the
    // view holds its rows. Then the base applies an out-of-order row in H3, below F (the trigger), a
    // late row into H0, which the apply evicts again, and an in-order row, before one refresh
    // drains them.
    //
    // Whatever repaired the pass, the view rows below the base's minimum keep their emitted values,
    // and the rows from there up pair one to one with the base's rows. A boundary rebuild replaces
    // from the lower of the trigger and the lowest row its replay emits, the base's minimum, so its
    // rows from there up equal the anchored recompute over the base as it stands. Any other route
    // may resume from a root that counts the evicted rows. Every other episode of a batch, from the
    // first, has the open anchor segment's residual repair the pass whatever the run drew, and
    // lands its late row below the newest row of H0, so the residual's floor sits below a row the
    // view holds and the repair falls back to the boundary rebuild: see
    // assertReachedLaggingTtlResidualBoundaryRebuilds. The other episodes land it anywhere in H0.
    private void runLaggingTtlEpisode(Rnd episodeRnd, String oracleProjection, int symCount, int ttlHours) throws Exception {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        if (instance == null
                || instance.isInvalid()
                || instance.isCheckpointRecoveryBlocked()
                || instance.getStateReader().getSeedState() != LiveViewState.SEED_STATE_ACTIVE
                || instance.getAnchorWindow() == null
                || instance.getAnchorWindow().getCheckpointAnchorPlan() == null) {
            return;
        }
        final LiveViewCheckpointAnchorPlan anchorPlan = instance.getAnchorWindow().getCheckpointAnchorPlan();
        final long floorTs = Math.max(queryTimestamp("SELECT max(ts) FROM base"), instance.getLatestSeenTs());
        final long segmentStartTs = anchorPlan.getSegmentStart(floorTs - floorTs % Micros.DAY_MICROS + 2 * Micros.DAY_MICROS);
        if (segmentStartTs == Long.MIN_VALUE) {
            return;
        }
        final long hour0Ts = segmentStartTs - segmentStartTs % Micros.HOUR_MICROS + Micros.HOUR_MICROS;
        final long hour3Ts = hour0Ts + 3 * Micros.HOUR_MICROS;
        // TTL measures a partition's age from its ceiling, against the lower of the newest row and
        // the clock, which the episode moves with its rows. So F evicts H0, whose partition ends
        // ttlHours and two hours below it, and the in-order row keeps H3, whose partition ends 35
        // minutes short of ttlHours below it.
        final long frontierTs = hour3Ts + ttlHours * Micros.HOUR_MICROS + 5 * Micros.MINUTE_MICROS;
        final long inOrderTs = frontierTs + 20 * Micros.MINUTE_MICROS;
        if (anchorPlan.getSegmentStart(inOrderTs) != segmentStartTs) {
            return;
        }
        // H0's and H3's rows sit on whole seconds, and the trigger and the late row off them, so no
        // two rows of the episode tie.
        final StringSink hour0Rows = new StringSink();
        final int hour0RowCount = 2 + episodeRnd.nextInt(3);
        long hour0MaxTs = Numbers.LONG_NULL;
        for (int r = 0; r < hour0RowCount; r++) {
            hour0MaxTs = hour0Ts + (5 + 12L * r) * Micros.MINUTE_MICROS + episodeRnd.nextInt(60) * Micros.SECOND_MICROS;
            appendLaggingTtlEpisodeRow(episodeRnd, hour0Rows, hour0MaxTs, symCount);
        }
        final StringSink hour3Rows = new StringSink();
        final int hour3RowCount = 1 + episodeRnd.nextInt(3);
        long hour3MinTs = Numbers.LONG_NULL;
        for (int r = 0; r < hour3RowCount; r++) {
            final long ts = hour3Ts + (10 + 15L * r) * Micros.MINUTE_MICROS + episodeRnd.nextInt(60) * Micros.SECOND_MICROS;
            if (r == 0) {
                hour3MinTs = ts;
            }
            appendLaggingTtlEpisodeRow(episodeRnd, hour3Rows, ts, symCount);
        }
        final StringSink frontierRow = new StringSink();
        appendLaggingTtlEpisodeRow(episodeRnd, frontierRow, frontierTs, symCount);
        final long triggerTs = hour3Ts + (1 + episodeRnd.nextInt(55)) * Micros.MINUTE_MICROS + 1 + episodeRnd.nextInt(999_999);
        final StringSink triggerRow = new StringSink();
        appendLaggingTtlEpisodeRow(episodeRnd, triggerRow, triggerTs, symCount);
        final boolean isResidualForced = laggingTtlEpisodes % 2 == 0;
        final long lateCeilingTs = isResidualForced ? hour0MaxTs : hour0Ts + Micros.HOUR_MICROS;
        final long lateTs = hour0Ts + episodeRnd.nextLong((lateCeilingTs - hour0Ts) / Micros.SECOND_MICROS) * Micros.SECOND_MICROS
                + 1 + episodeRnd.nextInt(999_998);
        final StringSink lateRow = new StringSink();
        appendLaggingTtlEpisodeRow(episodeRnd, lateRow, lateTs, symCount);
        final StringSink inOrderRow = new StringSink();
        appendLaggingTtlEpisodeRow(episodeRnd, inOrderRow, inOrderTs, symCount);
        final long baseMinTs = Math.min(hour3MinTs, triggerTs);

        LOG.info().$("LV fuzz lagging TTL episode [hour0Ts=").$ts(hour0Ts)
                .$(", hour0MaxTs=").$ts(hour0MaxTs)
                .$(", lateTs=").$ts(lateTs)
                .$(", triggerTs=").$ts(triggerTs)
                .$(", frontierTs=").$ts(frontierTs)
                .$(", ttlHours=").$(ttlHours)
                .$(", residualForced=").$(isResidualForced).I$();
        final long faultsBefore = instance.getRefreshFaultCount();
        final boolean isPerSegmentEnabled = engine.getConfiguration().isLiveViewCheckpointRepairPerSegmentEnabled();
        final boolean isOpenSegmentKeyedEnabled = engine.getConfiguration().isLiveViewCheckpointRepairOpenSegmentKeyedReplayEnabled();
        if (isResidualForced) {
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_PER_SEGMENT_ENABLED, "true");
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_OPEN_SEGMENT_KEYED_REPLAY_ENABLED, "true");
        }
        final StringSink frozenRows = new StringSink();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            setCurrentMicros(hour0Ts + Micros.HOUR_MICROS);
            execute("INSERT INTO base (ts, sym, i, x) VALUES " + hour0Rows);
            drainWalQueue();
            driveRefreshToQuiescence(job);
            setCurrentMicros(hour3Ts + Micros.HOUR_MICROS);
            execute("INSERT INTO base (ts, sym, i, x) VALUES " + hour3Rows);
            drainWalQueue();
            driveRefreshToQuiescence(job);
            setCurrentMicros(frontierTs);
            execute("INSERT INTO base (ts, sym, i, x) VALUES " + frontierRow);
            drainWalQueue();
            driveRefreshToQuiescence(job);
            Assert.assertEquals("the TTL must have evicted H0", hour3MinTs, queryTimestamp("SELECT min(ts) FROM base"));
            printSql("(SELECT * FROM lv WHERE ts < " + baseMinTs + ") ORDER BY 1, 2", frozenRows);

            setCurrentMicros(inOrderTs);
            execute("INSERT INTO base (ts, sym, i, x) VALUES " + triggerRow);
            execute("INSERT INTO base (ts, sym, i, x) VALUES " + lateRow);
            execute("INSERT INTO base (ts, sym, i, x) VALUES " + inOrderRow);
            drainWalQueue();
            driveRefreshToQuiescence(job);
        } finally {
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_PER_SEGMENT_ENABLED, String.valueOf(isPerSegmentEnabled));
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_OPEN_SEGMENT_KEYED_REPLAY_ENABLED, String.valueOf(isOpenSegmentKeyedEnabled));
        }

        Assert.assertFalse("view invalid: " + instance.getInvalidationReason(), instance.isInvalid());
        Assert.assertEquals("the lagging repair must not fault", faultsBefore, instance.getRefreshFaultCount());
        Assert.assertEquals("the apply must have evicted the late row", baseMinTs, queryTimestamp("SELECT min(ts) FROM base"));
        final StringSink viewRows = new StringSink();
        printSql("(SELECT * FROM lv WHERE ts < " + baseMinTs + ") ORDER BY 1, 2", viewRows);
        TestUtils.assertEquals("a lagging repair must keep the view rows a TTL eviction froze", frozenRows, viewRows);
        final int denialReason = instance.getCheckpointRepairLastDenialReason();
        final boolean isBoundaryRebuild = instance.getCheckpointRepairLastDisposition() == LiveViewCheckpointRepairPlan.DISPOSITION_BOUNDARY_REBUILD
                && denialReason != LiveViewCheckpointRepairPlan.DENIAL_NONE;
        if (isBoundaryRebuild) {
            TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                    "(SELECT " + oracleProjection + " FROM base) ORDER BY 1, 2",
                    "(SELECT * FROM lv WHERE ts >= " + baseMinTs + ") ORDER BY 1, 2", LOG, true);
        } else {
            TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                    "(SELECT ts, sym FROM base) ORDER BY 1, 2",
                    "(SELECT ts, sym FROM lv WHERE ts >= " + baseMinTs + ") ORDER BY 1, 2", LOG, true);
        }
        // The open segment's residual plans from its own floor, the late row, where the change set's
        // union range plans from the trigger: see LiveViewRefreshJob.repairChangeSetResidual.
        final LogCapture capture = walApplySuspensionRule.getCapture();
        capture.drain(LogCapture.STOP_DRAIN_TIMEOUT_MS);
        final boolean isResidualRepair = capture.countOccurrences("live view O3 replay [view=lv, lateRowTs=" + lateTs + ",") > 0;
        laggingTtlEpisodes++;
        if (isResidualRepair) {
            laggingTtlResidualRepairs++;
            if (isBoundaryRebuild
                    && (denialReason == LiveViewCheckpointRepairPlan.DENIAL_REMOVED_OUTPUT_FLOOR
                    || denialReason == LiveViewCheckpointRepairPlan.DENIAL_UNFLUSHED_OUTPUT)) {
                laggingTtlResidualBoundaryRebuilds++;
            }
        }
        LOG.info().$("LV fuzz lagging TTL episode repaired [residual=").$(isResidualRepair)
                .$(", disposition=").$(LiveViewCheckpointRepairPlan.dispositionName(instance.getCheckpointRepairLastDisposition(), denialReason))
                .$(", denial=").$(LiveViewCheckpointRepairPlan.denialReasonName(denialReason)).I$();
    }

    // Appends one (ts, sym, i, x) row of a lagging TTL episode, over one of the run's keys.
    private static void appendLaggingTtlEpisodeRow(Rnd episodeRnd, StringSink values, long ts, int symCount) {
        appendEpisodeRow(values, ts, episodeRnd.nextInt(symCount), 1 + episodeRnd.nextInt(1000), (1 + episodeRnd.nextInt(1000)) / 8.0);
    }

    // One refresh pass of runAnchoredRemovalFuzz: a pinned flush now and then, so a lead is
    // resident when the next pass lands, and a restart now and then, whatever the view holds.
    // Returns the job the next pass runs on.
    private LiveViewRefreshJob anchoredRemovalPass(Rnd rnd, LiveViewRefreshJob job, int pinPct, boolean restart) {
        if (rnd.nextInt(100) < pinPct) {
            pinnedRefreshCycle(job);
        } else {
            refreshCycle(job);
        }
        if (restart && rnd.nextInt(3) == 0) {
            final LiveViewInstance inst = engine.getLiveViewRegistry().getViewInstance("lv");
            if (inst != null && inst.getStateReader().getSeedState() == LiveViewState.SEED_STATE_ACTIVE) {
                Misc.free(job);
                engine.getLiveViewRegistry().clear();
                engine.buildViewGraphs();
                return new LiveViewRefreshJob(0, engine, 1);
            }
        }
        return job;
    }

    // Every fixed-width variant through runReplaceRangeFuzz, each with its own random knobs, under
    // a row cadence drawn once for the batch. With isSparseDepartureDrawn, the RANGE-frame runs
    // end on departNewestRowsOverSparseRoots, taking its shapes in turn from one the batch draws.
    private void runReplaceRangeFuzzVariants(Rnd rnd, boolean isSparseDepartureDrawn) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        final int firstSparseDepartureShape = isSparseDepartureDrawn ? rnd.nextInt(SPARSE_DEPARTURE_SHAPES) : -1;
        assertMemoryLeak(checkpointFaults, () -> {
            for (int i = 0, rangeFrameRuns = 0; i < FIXED_WIDTH_VARIANTS.length; i++) {
                final int variant = FIXED_WIDTH_VARIANTS[i];
                final int sparseDepartureShape = isSparseDepartureDrawn && isRangeFrameVariant(variant)
                        ? (firstSparseDepartureShape + rangeFrameRuns++) % SPARSE_DEPARTURE_SHAPES
                        : -1;
                runReplaceRangeFuzz(rnd, variant, 120 + rnd.nextInt(160),
                        rnd.nextBoolean(), rnd.nextBoolean(), rnd.nextBoolean(), sparseDepartureShape);
            }
        });
        coverage.log(LOG, "replace range");
    }

    // The RANGE-frame runs of runReplaceRangeFuzzVariants alone, each ending on the sparse
    // departure shape it takes in turn, and each followed by its owed-seal episode.
    private void runRangeFrameReplaceRangeFuzz(Rnd rnd) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        final int firstSparseDepartureShape = rnd.nextInt(SPARSE_DEPARTURE_SHAPES);
        assertMemoryLeak(checkpointFaults, () -> {
            for (int i = 0, rangeFrameRuns = 0; i < FIXED_WIDTH_VARIANTS.length; i++) {
                final int variant = FIXED_WIDTH_VARIANTS[i];
                if (isRangeFrameVariant(variant)) {
                    runReplaceRangeFuzz(rnd, variant, 40 + rnd.nextInt(40), rnd.nextBoolean(), rnd.nextBoolean(),
                            rnd.nextBoolean(), (firstSparseDepartureShape + rangeFrameRuns++) % SPARSE_DEPARTURE_SHAPES);
                }
            }
        });
    }

    // Differential fuzz where the base receives REPLACE_RANGE data commits
    // (WalWriter.commitWithParams) interleaved with plain insert commits. A
    // replace commit atomically deletes every base row in [lo, hi) and inserts
    // its own rows - possibly none - so unlike every other arm the logical row
    // set shrinks as well as grows. The refresh worker must converge the view
    // onto the applied (post-replace) base: the drain sees the deletion only
    // through the commit's range metadata and routes any band reaching at or
    // below the frontier to the O3 replay, even when every inserted row sits
    // above the frontier. Ingestion mirrors runFuzz (pre-CREATE seed
    // history, per-commit O3 refresh, optional quiescent restarts); replace
    // events fire between insert commits and once more after the last insert,
    // so at least one band lands over fully-settled data every run.
    // sparseDepartureShape is the SPARSE_DEPARTURE_* shape the run ends on, through
    // departNewestRowsOverSparseRoots, or -1 for departNewestRows.
    private void runReplaceRangeFuzz(
            Rnd rnd,
            int variant,
            int rowCount,
            boolean restart,
            boolean seed,
            boolean inMemory,
            int sparseDepartureShape
    ) throws Exception {
        // Pin the clock a day below the data, like runFuzz: START FROM NOW resolves the
        // CREATE-moment clock into the view's lower bound, and this harness wants that bound
        // to admit the whole dataset. A run that wants a boundary CUTTING the data draws the
        // explicit-timestamp mode instead, which cuts at a known row.
        if (currentMicros < 0) {
            setCurrentMicros(MicrosTimestampDriver.floor("2025-12-31T00:00:00.000000Z"));
        }

        final int n = 1 + rnd.nextInt(MAX_FRAME);
        final int symCount = 1 + rnd.nextInt(SYMBOLS.length);
        final int stepMode = rnd.nextInt(3);
        final int baseStepMax = stepMode == 0 ? 5_000_000 : stepMode == 1 ? 60_000_000 : 900_000_000;
        final int dayJumpEvery = stepMode == 0 ? 20 : 12;
        // The sparse departure that keeps the timeline needs rows the filter drops.
        // The owed-root arm takes filtered views' departures alone, so it filters every run.
        final boolean withWhere = rnd.nextInt(3) == 0 || sparseDepartureShape == SPARSE_DEPARTURE_KEEP || isDepartedOwedRootArm;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        // Ascending timestamps, with ties across keys for every variant partitioned by sym
        // (all but 5): (ts, sym) stays unique, so ts is a total order within each partition.
        // Random symbols and values with occasional NULLs. Every dataset ts goes into usedTs
        // up front so replace rows can never collide with a dataset row that is still to be
        // inserted.
        final int tiePct = variant == 5 ? 0 : tiePct(rnd);
        final LiveViewFuzzRows rows = LiveViewFuzzRows.generate(
                rnd, rowCount, symCount, MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z"),
                baseStepMax, dayJumpEvery, tiePct
        );
        final long[] tsv = rows.tsv;
        final int[] symIdx = rows.symIdx;
        final long[] iv = rows.iv;
        final double[] xv = rows.xv;
        final boolean[] xNull = rows.xNull;
        final LongHashSet usedTs = new LongHashSet();
        for (int k = 0; k < rowCount; k++) {
            usedTs.add(tsv[k]);
        }

        // Pre-CREATE history (the earliest rows by ts), plus the run's start mode. A cutting
        // boundary is worth drawing here specifically: a replace band can then land entirely
        // BELOW the bound (the view must not react to a deletion of rows it never held) or
        // STRADDLE it (the view must delete only the part of the band it did hold, clamped up to
        // the bound). Both are the shape the two clamp bugs took - one on each node - so the
        // oracle recomputing over the final applied base under the same bound is the check that
        // would have caught them.
        final int preCount = seed ? rnd.nextInt(rowCount + 1) : 0;
        final int startMode = rnd.nextInt(START_FROM_MODES);
        final long boundary = startBoundary(rnd, startMode, tsv, false);
        final String viewSql = "SELECT " + projection(variant, n) + " FROM base" + whereTail(withWhere, Numbers.LONG_NULL);
        final String oracleSql = "SELECT " + projection(variant, n) + " FROM base" + whereTail(withWhere, boundary);
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + (inMemory ? "IN MEMORY 60s " : "")
                + startFromClause(startMode, boundary, false)
                + "AS " + viewSql;

        LOG.info().$("LV replace-range fuzz: variant=").$(variant).$(", rows=").$(rowCount)
                .$(", tiePct=").$(tiePct).$(", ties=").$(rows.getTieCount())
                .$(", n=").$(n).$(", symCount=").$(symCount).$(", stepMode=").$(stepMode)
                .$(", restart=").$(restart).$(", preCount=").$(preCount)
                .$(", startMode=").$(startMode).$(", boundary=").$(boundary).$(", inMem=").$(inMemory)
                .$(", where=").$(withWhere).$(", sparseDeparture=").$(sparseDepartureShape).$(", sql=").$(viewSql).$();

        final TableToken baseToken = engine.verifyTableName("base");
        final String oracleOrderedSql = "(" + oracleSql + ") ORDER BY 1, 2";
        final String viewOrderedSql = "(lv) ORDER BY 1, 2";
        // The run's oracle once failTheOwedSealOfALateRow ran: base_shadow keeps the day the run
        // drops ahead of it.
        final String shadowOracleOrderedSql = "(SELECT " + projection(variant, n) + " FROM base_shadow"
                + whereTail(withWhere, boundary) + ") ORDER BY 1, 2";
        String departureOracleOrderedSql = oracleOrderedSql;
        final StringSink sink = new StringSink();
        String rowsBeforeTheDrop = null;
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
            assertPersistedStartBound("lv", boundary);
            job = new LiveViewRefreshJob(0, engine, 1);

            if (preCount > 0) {
                driveSeed(rnd, job);
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
                                && inst.getStateReader().getSeedState() == LiveViewState.SEED_STATE_ACTIVE) {
                            coverage.absorb(job);
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
            // The oracle over the main loop. The restart that ends departNewestRows can rebuild the
            // view from the base, which erases a ghost row or restores a missing row the main loop
            // left, so the check after that restart does not stand in for this one.
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracleOrderedSql, viewOrderedSql, LOG, true);
            assertRepairRootsAmongTheTwoNewest(engine.getLiveViewRegistry().getViewInstance("lv"));
            if (sparseDepartureShape >= 0) {
                final long rowsCadence = beginSparseCheckpointCadence();
                try {
                    // Which of the run's two frontier-sealing departures restores a root inside the
                    // new frontier's frame; the other warms the frame.
                    final boolean isFrontierRootRestored = frontierStateRouteRnd(rnd).nextBoolean();
                    final boolean isHeadRestamped = departNewestRowsOverSparseRoots(
                            rnd,
                            job,
                            baseToken,
                            usedTs,
                            symCount,
                            withWhere,
                            n,
                            sparseDepartureShape,
                            sparseDepartureShape == SPARSE_DEPARTURE_FRONTIER_SEAL && isFrontierRootRestored,
                            false,
                            false
                    );
                    // A crash the truncating departure drew left the image the restart below
                    // comes back from.
                    final boolean isCrashed = checkpointFaults.consumeCrash();
                    commitRowlessStretch(job, usedTs, withWhere, isHeadRestamped, true);
                    copyTheBaseToItsShadow(false);
                    final long viewMinTs = queryTimestamp("SELECT min(ts) FROM lv");
                    final String rowsBeforeTheSparseDrop = dropTheOldestDayTheViewHolds(rnd, job, oracleOrderedSql, viewOrderedSql);
                    long droppedDayTs = rowsBeforeTheSparseDrop != null ? viewMinTs - viewMinTs % Micros.DAY_MICROS : Numbers.LONG_NULL;
                    // The faults of the departure, the rowless stretch and the drop. The owed-seal
                    // episode accounts for its own.
                    assertNoRefreshFaults("lv");
                    coverage.absorb(job);
                    job = Misc.free(job);
                    if (isCrashed) {
                        putTheFrontierTruncateCrashImageBack();
                    }
                    engine.getLiveViewRegistry().clear();
                    engine.buildViewGraphs();
                    job = new LiveViewRefreshJob(0, engine, 1);
                    driveRefreshToQuiescence(job);
                    // The restart after the departure and the stretch without rows: the departure's
                    // own route, over any day the base has lost since, which the view keeps.
                    assertSparseDepartureRestartRoute(isCrashed, rowsBeforeTheSparseDrop != null);
                    assertDepartureOracle(oracleOrderedSql, viewOrderedSql, rowsBeforeTheSparseDrop);
                    if (isCrashed && droppedDayTs != Numbers.LONG_NULL) {
                        // The restart from the crash image stopped the view over the day the run
                        // dropped. The day goes back, which lets the view rebuild, and the
                        // owed-seal episode runs over a base that lost nothing.
                        job = restoreTheDroppedDayAndRestart(job, droppedDayTs, oracleOrderedSql, viewOrderedSql);
                        droppedDayTs = Numbers.LONG_NULL;
                    }
                    final int owedSealOutcome = failTheOwedSealOfALateRow(
                            owedSealEpisodeRnd(rnd),
                            job,
                            usedTs,
                            symCount,
                            n,
                            sparseDepartureShape == SPARSE_DEPARTURE_KEEP,
                            droppedDayTs,
                            oracleOrderedSql,
                            shadowOracleOrderedSql,
                            viewOrderedSql
                    );
                    departureOracleOrderedSql = shadowOracleOrderedSql;
                    coverage.absorb(job);
                    job = Misc.free(job);
                    engine.getLiveViewRegistry().clear();
                    engine.buildViewGraphs();
                    job = new LiveViewRefreshJob(0, engine, 1);
                    driveRefreshToQuiescence(job);
                    job = assertTheRestartAfterTheOwedSealEpisode(
                            job,
                            owedSealOutcome,
                            droppedDayTs,
                            oracleOrderedSql,
                            shadowOracleOrderedSql,
                            viewOrderedSql
                    );
                    if (withWhere) {
                        commitRowlessCyclesAfterARestart(job, usedTs);
                        coverage.absorb(job);
                        assertNoRefreshFaults("lv");
                        job = Misc.free(job);
                        engine.getLiveViewRegistry().clear();
                        engine.buildViewGraphs();
                        job = new LiveViewRefreshJob(0, engine, 1);
                        driveRefreshToQuiescence(job);
                        assertRestoredFromTimeline("lv");
                    }
                    if (sparseDepartureShape == SPARSE_DEPARTURE_FRONTIER_SEAL) {
                        job = departOverSparseRootsFailingTheFrontierSeal(
                                rnd,
                                job,
                                usedTs,
                                symCount,
                                withWhere,
                                n,
                                !isFrontierRootRestored,
                                droppedDayTs,
                                oracleOrderedSql,
                                viewOrderedSql
                        );
                        // The restart after it rebuilt the view from the base, with any day the
                        // run dropped back in it.
                        departureOracleOrderedSql = oracleOrderedSql;
                    }
                } finally {
                    endSparseCheckpointCadence(rowsCadence);
                }
            } else if (departNewestRows(rnd, job, baseToken, usedTs, symCount, withWhere, true, true, true, oracleOrderedSql, viewOrderedSql)) {
                // A crash the departure drew left the image the restart below comes back from.
                final boolean isCrashed = checkpointFaults.consumeCrash();
                commitRowlessStretch(job, usedTs, withWhere, false, true);
                if (isCrashed) {
                    copyTheBaseToItsShadow(false);
                }
                final long viewMinTs = queryTimestamp("SELECT min(ts) FROM lv");
                rowsBeforeTheDrop = dropTheOldestDayTheViewHolds(rnd, job, oracleOrderedSql, viewOrderedSql);
                final long droppedDayTs = rowsBeforeTheDrop != null ? viewMinTs - viewMinTs % Micros.DAY_MICROS : Numbers.LONG_NULL;
                coverage.absorb(job);
                // The restart below registers a fresh instance whose fault count starts at zero,
                // so only this check sees the faults this instance healed, the departure
                // episode's included.
                assertNoRefreshFaults("lv");
                job = Misc.free(job);
                if (isCrashed) {
                    putTheFrontierTruncateCrashImageBack();
                }
                engine.getLiveViewRegistry().clear();
                engine.buildViewGraphs();
                job = new LiveViewRefreshJob(0, engine, 1);
                driveRefreshToQuiescence(job);
                if (isCrashed) {
                    assertTheRestartFromTheFrontierTruncateCrashImage(droppedDayTs != Numbers.LONG_NULL);
                    assertDepartureOracle(oracleOrderedSql, viewOrderedSql, rowsBeforeTheDrop);
                    if (droppedDayTs != Numbers.LONG_NULL) {
                        job = restoreTheDroppedDayAndRestart(job, droppedDayTs, oracleOrderedSql, viewOrderedSql);
                        // The base holds the day again, so the recompute is the run's oracle.
                        rowsBeforeTheDrop = null;
                    }
                } else {
                    // No fault struck the run, so the departure left a root at the view's frontier.
                    assertRestoredFromTimeline("lv");
                }
                if (withWhere) {
                    commitRowlessCyclesAfterARestart(job, usedTs);
                    coverage.absorb(job);
                    assertNoRefreshFaults("lv");
                    job = Misc.free(job);
                    engine.getLiveViewRegistry().clear();
                    engine.buildViewGraphs();
                    job = new LiveViewRefreshJob(0, engine, 1);
                    driveRefreshToQuiescence(job);
                    assertRestoredFromTimeline("lv");
                }
            }
        } finally {
            coverage.absorb(job);
            Misc.free(job);
        }

        // The oracle: the live view must equal its own window query recomputed over the applied
        // (post-replace) base under the run's START FROM boundary. ORDER BY 1, 2 - (ts, sym),
        // unique over ties - gives both sides a total order; genericStringMatch tolerates
        // SYMBOL-vs-STRING on passthrough. A run that dropped a day the view holds compares
        // against the view's own rows from before the drop instead, or against base_shadow,
        // which keeps that day, when an owed-seal episode followed the drop.
        assertDepartureOracle(departureOracleOrderedSql, viewOrderedSql, rowsBeforeTheDrop);
        assertNoRefreshFaults("lv");

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
        execute("DROP TABLE IF EXISTS base_shadow");
    }

    // Differential fuzz proving rolled-back base transactions never reach the view
    // (see testFuzzRolledBackCommits). Committed rows follow runFuzz's shape - unique,
    // strictly-increasing timestamps, optional pre-CREATE SEED history - but every
    // commit is written through a direct WalWriter, so between real batches a doomed
    // transaction can append phantom rows and then rollback() without ever advancing the
    // sequencer. Phantoms carry i = PHANTOM_SENTINEL and a timestamp just below a
    // committed row, so a leak is loud: it survives WHERE i>0 and lands below the
    // frontier. The recompute over the committed base is the oracle - it can never
    // contain a phantom, so equality at quiescence proves none leaked.
    private void runRolledBackFuzz(Rnd rnd, int variant, int rowCount, boolean o3, boolean seed) throws Exception {
        setCurrentMicros(MicrosTimestampDriver.floor("2025-12-31T00:00:00.000000Z"));

        final int n = 1 + rnd.nextInt(MAX_FRAME);
        final int symCount = 1 + rnd.nextInt(SYMBOLS.length);
        final int stepMode = rnd.nextInt(3);
        final int baseStepMax = stepMode == 0 ? 5_000_000 : stepMode == 1 ? 60_000_000 : 900_000_000;
        final int dayJumpEvery = stepMode == 0 ? 20 : 12;
        final boolean withWhere = rnd.nextInt(3) == 0;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

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

        final int preCount = seed ? rnd.nextInt(rowCount + 1) : 0;
        final int startMode = rnd.nextInt(START_FROM_MODES);
        final long boundary = startBoundary(rnd, startMode, tsv, false);
        final String viewSql = "SELECT " + projection(variant, n) + " FROM base" + whereTail(withWhere, Numbers.LONG_NULL);
        final String oracleSql = "SELECT " + projection(variant, n) + " FROM base" + whereTail(withWhere, boundary);
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + startFromClause(startMode, boundary, false)
                + "AS " + viewSql;

        LOG.info().$("LV rolled-back fuzz: variant=").$(variant).$(", rows=").$(rowCount)
                .$(", n=").$(n).$(", symCount=").$(symCount).$(", stepMode=").$(stepMode)
                .$(", o3=").$(o3).$(", preCount=").$(preCount).$(", startMode=").$(startMode)
                .$(", boundary=").$(boundary).$(", where=").$(withWhere).$(", sql=").$(viewSql).$();

        final TableToken baseToken = engine.verifyTableName("base");
        LiveViewRefreshJob job = null;
        try {
            // Pre-CREATE seed history (single committed transaction, in ts order).
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
            assertPersistedStartBound("lv", boundary);
            job = new LiveViewRefreshJob(0, engine, 1);
            if (preCount > 0) {
                driveSeedToCompletion(job, "lv");
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

        // The oracle: the live view must equal its own window query recomputed over the
        // committed base under the run's START FROM boundary. ORDER BY 1 (the unique ts) gives
        // both sides a total order; genericStringMatch tolerates SYMBOL-vs-STRING on
        // passthrough.
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + oracleSql + ") ORDER BY 1",
                "(lv) ORDER BY 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");

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
    // Ingestion mirrors runFuzz (pre-CREATE seed history, then per-commit O3
    // refresh with optional quiescent restarts); after quiescence a top-up
    // forward row guarantees the tier is populated, then the read-back is
    // cross-checked against the recompute, Mode B is confirmed engaged, and the
    // Mode B result is compared byte-for-byte against the forced disk-only path.
    private void runVarSizeFuzz(Rnd rnd, int rowCount, boolean o3, boolean restart, boolean seed) throws Exception {
        // Pin the clock a day below the data, like runFuzz: START FROM NOW resolves the
        // CREATE-moment clock into the view's lower bound, and this harness wants that bound
        // to admit the whole dataset. A run that wants a boundary CUTTING the data draws the
        // explicit-timestamp mode instead, which cuts at a known row.
        if (currentMicros < 0) {
            setCurrentMicros(MicrosTimestampDriver.floor("2025-12-31T00:00:00.000000Z"));
        }

        final int stepMode = rnd.nextInt(3);
        final int baseStepMax = stepMode == 0 ? 5_000_000 : stepMode == 1 ? 60_000_000 : 900_000_000;
        final int dayJumpEvery = stepMode == 0 ? 20 : 12;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, vs STRING, vv VARCHAR, vb BINARY, va DOUBLE[]) "
                + "TIMESTAMP(ts) PARTITION BY DAY WAL");

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

        // Pre-CREATE history (the earliest rows by ts) plus the run's start mode: a var-length
        // seed has to write its passthrough columns through the tier's (data, aux) staging path
        // just as the forward drain does, and a bounded seed exercises that with the cursor
        // opened at the boundary rather than at the first row.
        final int preCount = seed ? rnd.nextInt(rowCount + 1) : 0;
        final int startMode = rnd.nextInt(START_FROM_MODES);
        final long boundary = startBoundary(rnd, startMode, tsv, false);
        final String viewSql = "SELECT ts, vs, vv, vb, va, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base";
        final String oracleSql = viewSql + whereTail(false, boundary);
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 60s "
                + startFromClause(startMode, boundary, false)
                + "AS " + viewSql;

        LOG.info().$("LV var-size fuzz: rows=").$(rowCount).$(", stepMode=").$(stepMode)
                .$(", o3=").$(o3).$(", restart=").$(restart).$(", preCount=").$(preCount)
                .$(", startMode=").$(startMode).$(", boundary=").$(boundary)
                .$(", sql=").$(viewSql).$();

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
            assertPersistedStartBound("lv", boundary);
            job = new LiveViewRefreshJob(0, engine, 1);

            if (preCount > 0) {
                driveSeedToCompletion(job, "lv");
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
                                && inst.getStateReader().getSeedState() == LiveViewState.SEED_STATE_ACTIVE) {
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
                "(" + oracleSql + ") ORDER BY 1",
                "(lv) ORDER BY 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");

        // Var-length read-back cross-checks, single-threaded now the worker is
        // freed and the view is quiesced: SELECT * FROM lv actually routes through
        // Mode B (the tier serves the var-length values from RAM), and the Mode B
        // result is byte-identical to the forced disk-only path - so a tier that
        // stored a var-length value wrong would diverge from the disk oracle here.
        assertModeBEngaged("lv");
        assertModeBMatchesDiskOnly("lv", "SELECT * FROM lv");

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
    }

    /**
     * The checkpoint faults the runs that end on departures draw. Once the crash is armed, runs an
     * action right after the second repair marker rename lands: the marker a frontier truncate
     * writes after its splice's, ahead of its publication. A crash taken there stays recorded until
     * the caller consumes it. While the timeline is locked, refuses the unlink of {@code _timeline}
     * and every directory removal under the checkpoint directory, as a lock on the file refuses
     * them, so a retire keeps the generation, and the repair marker the retire keeps beside it.
     * While the marker is locked, refuses every unlink of the repair marker and of its staged
     * sibling, so the marker outlives a {@code _timeline} a retire removes. Once armed, fails the
     * check of whether {@code _timeline} exists that follows its next unlink, refused or not.
     */
    private static final class CheckpointFaults extends TestFilesFacadeImpl {
        // -1 disarmed; otherwise the base WAL timestamp column opens still to let through before the one that raises
        // the engine's closing flag.
        private final AtomicInteger closingCountdown = new AtomicInteger(-1);
        private final AtomicInteger failedTimelineChecks = new AtomicInteger();
        private final AtomicInteger markerRenames = new AtomicInteger();
        private final AtomicInteger refusedMarkerUnlinks = new AtomicInteger();
        private final AtomicInteger refusedTimelineUnlinks = new AtomicInteger();
        private final AtomicInteger failedViewTableReads = new AtomicInteger();
        // The directory of the view whose table's next _txn open fails, or null.
        private final AtomicReference<String> failedViewTableDir = new AtomicReference<>();
        // The directory of the base whose WAL reads armClosingAtBaseWalRead counts.
        private volatile String closingBaseDir;
        private volatile boolean hasCrashed;
        private volatile boolean hasKeptTheTimeline;
        private volatile boolean isFailingTheCheckAfterTheNextTimelineUnlink;
        private volatile boolean isFailingTheNextTimelineCheck;
        private volatile boolean isMarkerLocked;
        private volatile boolean isTimelineLocked;
        private volatile Runnable onCrash;
        private volatile Runnable onTheKeptTimeline;

        private static boolean isTimelineFile(LPSZ name) {
            return Utf8s.containsAscii(name, LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME)
                    && Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.TIMELINE_FILE_NAME);
        }

        @Override
        public boolean exists(LPSZ path) {
            if (isFailingTheNextTimelineCheck && isTimelineFile(path)) {
                isFailingTheNextTimelineCheck = false;
                failedTimelineChecks.incrementAndGet();
                throw CairoException.critical(0).put("injected fault on the check of _timeline");
            }
            return super.exists(path);
        }

        @Override
        public long openRO(LPSZ name) {
            final String viewDir = failedViewTableDir.get();
            if (viewDir != null
                    && Utf8s.containsAscii(name, Files.SEPARATOR + viewDir + Files.SEPARATOR)
                    && Utf8s.endsWithAscii(name, Files.SEPARATOR + TableUtils.TXN_FILE_NAME)
                    && failedViewTableDir.compareAndSet(viewDir, null)) {
                failedViewTableReads.incrementAndGet();
                return -1;
            }
            final String dir = closingBaseDir;
            if (dir != null
                    && closingCountdown.get() >= 0
                    && Utf8s.containsAscii(name, dir)
                    && Utf8s.containsAscii(name, "wal")
                    && Utf8s.endsWithAscii(name, Files.SEPARATOR + "ts.d")
                    && closingCountdown.getAndDecrement() == 0) {
                engine.setClosing(true);
            }
            return super.openRO(name);
        }

        @Override
        public boolean removeQuiet(LPSZ name) {
            if (isMarkerLocked
                    && Utf8s.containsAscii(name, LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME)
                    && (Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME)
                    || Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME + LiveViewCheckpointLayout.TMP_SUFFIX))) {
                refusedMarkerUnlinks.incrementAndGet();
                return false;
            }
            if (isTimelineFile(name)) {
                if (isFailingTheCheckAfterTheNextTimelineUnlink) {
                    isFailingTheCheckAfterTheNextTimelineUnlink = false;
                    isFailingTheNextTimelineCheck = true;
                }
                if (isTimelineLocked) {
                    refusedTimelineUnlinks.incrementAndGet();
                    return false;
                }
            }
            return super.removeQuiet(name);
        }

        @Override
        public boolean rmdir(Path name, boolean haltOnError) {
            if (isTimelineLocked && Utf8s.containsAscii(name, LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME)) {
                return false;
            }
            return super.rmdir(name, haltOnError);
        }

        @Override
        public int rename(LPSZ from, LPSZ to) {
            final int result = super.rename(from, to);
            final Runnable crash = onCrash;
            final Runnable keep = onTheKeptTimeline;
            if ((crash != null || keep != null)
                    && result == Files.FILES_RENAME_OK
                    && Utf8s.endsWithAscii(to, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME)
                    && markerRenames.incrementAndGet() == 2) {
                if (crash != null) {
                    onCrash = null;
                    crash.run();
                    hasCrashed = true;
                }
                if (keep != null) {
                    onTheKeptTimeline = null;
                    keep.run();
                    hasKeptTheTimeline = true;
                }
            }
            return result;
        }

        // Raises the engine's closing flag, what a shutdown raises, at the open of the timestamp column of the WAL
        // of the base in baseDir that comes after skip of them, and lets the open go ahead. A drain makes one as it
        // starts reading each commit, so the flag rises inside the drain, after it fed the commits ahead of that
        // one: see shutDownMidDrain.
        void armClosingAtBaseWalRead(String baseDir, int skip) {
            closingCountdown.set(-1);
            closingBaseDir = baseDir;
            closingCountdown.set(skip);
        }

        void armTheCrash(Runnable onCrash) {
            markerRenames.set(0);
            hasCrashed = false;
            this.onCrash = onCrash;
        }

        // Runs action at the second repair marker rename the next commit's turn makes, the marker a
        // frontier truncate writes after its splice's, as armTheCrash does, for the kept-timeline
        // episode: see runKeptTimelineEpisode.
        void armTheKeptTimeline(Runnable action) {
            markerRenames.set(0);
            hasKeptTheTimeline = false;
            onTheKeptTimeline = action;
        }

        // Whether the kept-timeline action ran since the last call, which forgets it.
        boolean consumeKeptTimeline() {
            final boolean wasKept = hasKeptTheTimeline;
            hasKeptTheTimeline = false;
            return wasKept;
        }

        // Disarms the kept-timeline action, and the fault on the check of _timeline, armed or
        // pending, which only that action arms.
        void disarmTheKeptTimeline() {
            onTheKeptTimeline = null;
            isFailingTheCheckAfterTheNextTimelineUnlink = false;
            isFailingTheNextTimelineCheck = false;
        }

        // The checks of _timeline the fault failed since the last call, which forgets them.
        int consumeFailedTimelineChecks() {
            return failedTimelineChecks.getAndSet(0);
        }

        // Fails, once, the check of whether _timeline exists that follows its next unlink.
        void failTheTimelineCheckAfterItsNextUnlink() {
            isFailingTheNextTimelineCheck = false;
            isFailingTheCheckAfterTheNextTimelineUnlink = true;
        }

        // The reads of a view's table failTheNextViewTableRead failed since the last call, which
        // forgets them.
        int consumeFailedViewTableReads() {
            return failedViewTableReads.getAndSet(0);
        }

        // Disarms the fault failTheNextViewTableRead armed, if it has not fired.
        void disarmTheViewTableRead() {
            failedViewTableDir.set(null);
        }

        // Fails the next read of the view's table: evicts its pooled readers, which would serve the
        // read without opening a file, and fails the next open of the table's _txn. The caller holds
        // no reader of the view's table.
        void failTheNextViewTableRead(LiveViewInstance instance) {
            Assert.assertTrue("no reader of the view's table may be in use", engine.lockReadersByTableToken(instance.getLiveViewToken()));
            engine.unlockReaders(instance.getLiveViewToken());
            failedViewTableDir.set(instance.getLiveViewToken().getDirName());
        }

        // Whether the crash was taken since the last call, which forgets it.
        boolean consumeCrash() {
            final boolean wasCrashed = hasCrashed;
            hasCrashed = false;
            return wasCrashed;
        }

        void disarmClosingAtBaseWalRead() {
            closingCountdown.set(-1);
            closingBaseDir = null;
        }

        void disarmTheCrash() {
            onCrash = null;
        }

        boolean hasCrashed() {
            return hasCrashed;
        }

        // Refuses every unlink of the repair marker, _repairing, and of its staged sibling.
        void lockTheMarker() {
            refusedMarkerUnlinks.set(0);
            isMarkerLocked = true;
        }

        void lockTheTimeline() {
            refusedTimelineUnlinks.set(0);
            isTimelineLocked = true;
        }

        // The unlinks of the repair marker the lock refused since it was set.
        int getRefusedMarkerUnlinks() {
            return refusedMarkerUnlinks.get();
        }

        // Lifts the lock on the repair marker and returns the unlinks it refused since it was set.
        int unlockTheMarker() {
            isMarkerLocked = false;
            return refusedMarkerUnlinks.getAndSet(0);
        }

        // Lifts the lock and returns the unlinks of _timeline it refused since it was set.
        int unlockTheTimeline() {
            isTimelineLocked = false;
            return refusedTimelineUnlinks.getAndSet(0);
        }
    }
}

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

package org.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointRepairPlan;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewWindow;
import io.questdb.cairo.lv.LiveViewWindowStatePlan;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRuleFactory;
import io.questdb.std.datetime.microtime.Micros;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Locale;

/**
 * Steady-state companion to {@link LiveViewHighCardinalityBenchmark}, which measures one
 * incremental batch immediately after the seed and so compares two builds at a fixed state
 * size. This one seeds once and then runs many incremental batches, so per-batch refresh
 * cost can be watched as retained window state grows - the shape of the reported
 * failure, where the view kept up early and fell progressively behind.
 * <p>
 * Same customer schema and live-view DDL, a brand-new {@code account_id} per row by
 * default, and a forced checkpoint per batch so the seal is measured rather than skipped.
 * <p>
 * With the default {@code ANCHOR DAILY} every row of a run lands in one anchor bucket -
 * 3.1M rows 444 us apart span 23 minutes - so no partition ever falls behind the frontier
 * and the sweep never fires. {@code --anchor-period} shortens the bucket so a run spans
 * many of them, and {@code --account-window} bounds how many accounts are alive in one
 * bucket, so older accounts age out and {@code LiveViewWindow.compact()} runs repeatedly.
 * The per-batch {@code sweeps} / {@code evicted} / {@code sweep_ms} columns and the
 * closing {@code # sweeps} line report what that costs, including the seal that follows
 * a sweep against one that does not - the seal stays incremental across a sweep, but has
 * to carry one removal per evicted key on top of the keys the batch touched.
 * <p>
 * <b>The zoned anchor.</b> {@code --anchor-period=daily:<zone>} hangs an IANA zone off the
 * DAILY sugar, as in {@code --anchor-period=daily:Europe/Berlin}. A zoned anchor desugars
 * to {@code timestamp_floor_utc} rather than {@code timestamp_floor}, whose civil days are
 * 23 and 25 hours wide across a DST transition, and the checkpoint anchor planner reads
 * that shape through a branch of its own. It is the shape whose repair used to report
 * {@code boundary rebuild/incomplete dependency} in the {@code repair} column on every
 * correction and re-read the view from its own start; a run that names a zone is what
 * prices the localized path against that. Read {@code repair} and {@code o3_scan_rows}
 * against a run of the same parameters without the zone. The account window still slides
 * on a fixed 24-hour grid, shifted by the zone's offset at the run's first timestamp, so
 * it tracks the zone's civil day for as long as the run stays inside one DST regime and
 * drifts by the transition's own amount past one.
 * <p>
 * {@code --compact-threshold} and {@code --compact-stale-percent} move the two arms of the
 * trigger. The threshold is an absolute count and stops binding once the map is large; the
 * stale percent scales with the map and is what decides at that point. Lowering it sweeps
 * more often and evicts less each time.
 * <p>
 * <b>The write side.</b> A refresh does not only read base rows: it writes corrected output
 * into live-view partitions that already exist, and a repair whose replacement range reaches
 * a partition on disk rewrites the whole of that partition rather than appending to it. The
 * {@code lv_apply_ms}, {@code lv_rows}, {@code lv_phys_rows}, {@code lv_write_amp} and
 * {@code lv_parts} columns report that half, and the closing {@code # lv_writes} line turns
 * it into bytes. None of it is visible in a run whose live view holds one partition, so
 * {@code --ts-step-us} widens the timestamp axis: at the default 444 us a run of a few
 * million rows spans minutes and one partition, and at 3600 us it spans hours and ten of
 * them. {@code spanHours} in the header line is what the run covers.
 * <p>
 * <b>The out-of-order shape.</b> {@code --o3-spread} decides how many distinct anchor
 * segments the late rows of one commit reach - consecutive late rows step down by one
 * further anchor period - which is what the production logs show and what a per-segment
 * repair is priced against. {@code --hot-key-percent} folds a share of all rows onto one
 * key, so one posting list holds that share of the base; a keyed scan is priced from
 * posting counts and loses on exactly that distribution. {@code --equal-ts-percent} makes a
 * share of the rows repeat the row below them in both timestamp and key, which is the
 * {@code (timestamp, key)} group a physical row identity would have to number.
 * {@code --base-dedup} puts dedup keys on the base as the stand-in for what such an
 * identity would put on the live view's own table, and {@code --lv-dedup=true} puts them
 * where they actually belong: on the view's own table, as a CREATE-time property. With
 * {@code --repair-keyed-replay=true} beside it a keyed segment repair publishes on them -
 * only the rows it recomputed, upserted onto the pair, with every other stored row left
 * where it stands - which the {@code # sparse} line reports and which
 * {@code lv_phys_rows} and {@code lv_write_amp} are the write-side columns for. Alone, the
 * identity costs the path that does not use it: every live-view commit then carries a
 * non-default dedup mode, which forces a full commit instead of a lag-retaining or
 * block-coalesced one. The columns are {@code lv_apply_ms} and {@code base_apply_ms}. {@code --repair-per-segment=false}
 * restores the union range a repair took before the change set was decomposed into the
 * anchor segments it touches, which is the control column that half of the write side is
 * measured against. {@code --repair-isolated-runtime=false} puts a converging repair's replay
 * back through the primary runtime, which means copying the whole window state aside before it and after
 * it. That copy is as large as the state itself, so the control column it restores is the one
 * that shows what a repair paid for the view's key domain rather than for its own range - and
 * it grows with {@code --account-window}, not with the correction.
 * {@code --repair-segment-yield=false} keeps one segment's replay inside the turn that
 * started it, which is what both segment loops did before the yield existed. The yield does
 * not change what a repair reads or writes - it changes how much of it one worker pass
 * holds - so the column it moves is {@code refresh_max_pass_ms} rather than
 * {@code refresh_ms}, and the two budgets that cut it are
 * {@code cairo.live.view.checkpoint.repair.replay.max.rows} and
 * {@code cairo.live.view.refresh.turn.max.duration.micros}.
 * {@code --repair-keyed-replay=true} lets a corrected closed segment follow the keys its
 * correction touched through the base's posting index, publishing the rest of the segment
 * from the view's own stored output. It is off by default because a copied-forward row is
 * no longer recomputed from the base. It changes the READ, not the write: {@code o3_scan_rows}
 * falls and {@code lv_phys_rows} does not move, because the block carries the segment's
 * whole row set either way. The base column MUST be indexed ({@code --index=true}) or no
 * segment is priced and none takes the route.
 * <p>
 * Build and run:
 * <pre>
 * mvn -pl benchmarks -am package -o -DskipTests -Dmaven.test.skip=true
 *
 * java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED -Xmx12g \
 *     -cp benchmarks/target/benchmarks.jar \
 *     org.questdb.LiveViewSteadyStateBenchmark \
 *     --seed=2400000 --batch=135000 --batches=3 --checkpoint-rows=135000
 *
 * # sweep mode: 1M accounts alive per 8-minute bucket, 10 buckets over the run
 * java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED -Xmx12g \
 *     -cp benchmarks/target/benchmarks.jar \
 *     org.questdb.LiveViewSteadyStateBenchmark \
 *     --seed=1000000 --batch=1000000 --batches=10 --checkpoint-rows=1000000 \
 *     --anchor-period=8m --account-window=1000000
 *
 * # a time-zone-aware daily anchor, which is what takes the planner's zoned branch
 * java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED -Xmx12g \
 *     -cp benchmarks/target/benchmarks.jar \
 *     org.questdb.LiveViewSteadyStateBenchmark \
 *     --seed=200000 --batch=5500 --batches=200 --checkpoint-rows=1000000 \
 *     --anchor-period=daily:Europe/Berlin --account-window=20000000 \
 *     --ts-step-us=909 --o3-percent=2 --o3-commit-percent=50 --o3-from-batch=50
 * </pre>
 */
public class LiveViewSteadyStateBenchmark {

    // The two must agree: DAILY_ANCHOR_TIME goes into the DDL, and the account window
    // slides on bucket boundaries computed from the offset.
    private static final long DAILY_ANCHOR_OFFSET_MICROS = 12L * Micros.HOUR_MICROS;
    private static final String DAILY_ANCHOR_PERIOD = "daily";
    private static final String DAILY_ANCHOR_TIME = "12:00";
    private static final int MAX_SUM_COLUMNS = 24;
    private static final int RESTART_PROBE_ROWS = 1_000;
    private static final long START_TS = 1_785_496_035_000_000L;
    // The default spacing of the generated stream. --ts-step-us widens it, which is what
    // lets a run of a few million rows span many base partitions and many anchor segments
    // - and so lets a repair land in a closed partition the apply has to rewrite whole,
    // rather than in the head partition it can append to.
    private static final long DEFAULT_TS_STEP_MICROS = 444L;
    private static final String VIEW_NAME = "payments_view";

    public static void main(String[] args) throws Exception {
        long seedRows = 1_000_000L;
        int batchRows = 135_000;
        int batches = 15;
        long checkpointRows = 135_000L;
        long checkpointDurationMicros = 24L * Micros.HOUR_MICROS;
        long checkpointPurgeInterval = -1;
        long checkpointCompactionInterval = -1;
        boolean isIndexed = true;
        boolean isRestartMeasured = false;
        boolean isSymbolPreSized = true;
        int recycleAccounts = 0; // 0 = every row a brand new account
        long accountWindow = 0; // 0 = no rolling window, so nothing ages out
        String anchorPeriodArg = DAILY_ANCHOR_PERIOD;
        int compactStalePercent = -1; // -1 = leave the configuration default alone
        int compactThreshold = -1; // -1 = leave the configuration default alone
        String shape = Shape.TARGET.name;
        String keyType = "symbol";
        int nullPercent = 0;
        int sumColumns = 0; // extra one-component sum(qN) projections, for the width sweep
        int commitsPerBatch = 1;
        double o3Percent = 0; // 0 = strictly forward, every row above the last
        String o3Lag = "1m";
        int o3FromBatch = 0; // batches below this one stay strictly forward
        String o3Spread = null; // unset = every late row equally late, so one commit touches one segment
        // A weighted ladder of correction depths, "<dur>:<weight>,..." - the shape a real
        // stream has, where a minority of commits is a second late, a smaller minority an
        // hour and a handful a day. Unset leaves the single --o3-lag depth in place.
        String o3Depths = null;
        // The share of COMMITS carrying late rows. The refresh classifies out-of-order per
        // commit, so a stream where half the commits are clean and half carry a few late
        // rows is a different workload from one where every commit carries them, at the same
        // late-row share. --o3-percent is then the share within an eligible commit.
        int o3CommitPercent = 100;
        double hotKeyPercent = 0; // 0 = no key is hotter than any other
        double equalTsPercent = 0; // 0 = no two rows share a (timestamp, key) pair
        long tsStepMicros = DEFAULT_TS_STEP_MICROS;
        // Dedup keys on the base, as the stand-in for what fix 3C would put on the live
        // view's own table. A live-view table cannot carry them yet, and the fast paths
        // the question is about - the WAL-lag append into the last partition, the
        // all-in-order single-segment block, and the SYMBOL dedup-key remap on apply -
        // are TableWriter's rather than the live view's, so the same schema on the base
        // exercises the same code. Rows are unique in (created_at, account_id) by
        // construction unless --equal-ts-percent says otherwise, so nothing is deduped
        // away and the reading is the dedup path's own cost.
        boolean isBaseDeduped = false;
        // Dedup keys on the live view's OWN table - (designated timestamp, projected
        // partition key), the identity a sparse repair publication would upsert on. What it
        // measures is the price of carrying that identity on the path that does not use it:
        // every live-view commit then stamps a non-default dedup mode, which forces a full
        // commit rather than a lag-retaining or block-coalesced one. Read lv_apply_ms and
        // base_apply_ms against a run without it. --base-dedup is the older stand-in for the
        // same question, put on the base because a live-view table could not carry the keys.
        boolean isLvDeduped = false;
        // -1 = leave the configuration default alone. 0 declines the chain outright, which
        // is how a run reproduces what a repair cost before it kept its ladder.
        int repairMaxChainedBoundaries = -1;
        boolean isRepairIsolatedRuntime = true;
        boolean isRepairSegmentYield = true;
        boolean isRepairKeyedReplay = false;
        long keyedScanIndexOpenRows = -1; // -1 = leave the production cost model unchanged
        // The open anchor segment's own keyed resume - the route the reported workload's
        // corrections actually take, since a daily anchor leaves 99% of them inside the
        // segment the runtime still stands in. Left at the configuration default, so a run
        // that names nothing measures what ships.
        boolean isOpenSegmentKeyedReplay = true;
        boolean forceOpenSegmentKeyedReplay = false;
        boolean isRepairPerSegment = true;
        for (String arg : args) {
            if (arg.startsWith("--restart=")) {
                isRestartMeasured = Boolean.parseBoolean(arg.substring(10));
                continue;
            }
            if (arg.startsWith("--seed=")) {
                seedRows = Long.parseLong(arg.substring(7));
            } else if (arg.startsWith("--batch=")) {
                batchRows = Integer.parseInt(arg.substring(8));
            } else if (arg.startsWith("--batches=")) {
                batches = Integer.parseInt(arg.substring(10));
            } else if (arg.startsWith("--checkpoint-rows=")) {
                checkpointRows = Long.parseLong(arg.substring(18));
            } else if (arg.startsWith("--checkpoint-duration-us=")) {
                checkpointDurationMicros = Long.parseLong(arg.substring(25));
            } else if (arg.startsWith("--checkpoint-purge-interval=")) {
                checkpointPurgeInterval = Long.parseLong(arg.substring("--checkpoint-purge-interval=".length()));
            } else if (arg.startsWith("--checkpoint-compaction-interval=")) {
                checkpointCompactionInterval = Long.parseLong(
                        arg.substring("--checkpoint-compaction-interval=".length())
                );
            } else if (arg.startsWith("--presize-symbol=")) {
                isSymbolPreSized = Boolean.parseBoolean(arg.substring(17));
            } else if (arg.startsWith("--index=")) {
                isIndexed = Boolean.parseBoolean(arg.substring(8));
            } else if (arg.startsWith("--recycle-accounts=")) {
                recycleAccounts = Integer.parseInt(arg.substring(19));
            } else if (arg.startsWith("--account-window=")) {
                accountWindow = Long.parseLong(arg.substring(17));
            } else if (arg.startsWith("--anchor-period=")) {
                anchorPeriodArg = arg.substring(16);
            } else if (arg.startsWith("--compact-threshold=")) {
                compactThreshold = Integer.parseInt(arg.substring(20));
            } else if (arg.startsWith("--compact-stale-percent=")) {
                compactStalePercent = Integer.parseInt(arg.substring(24));
            } else if (arg.startsWith("--shape=")) {
                shape = arg.substring(8);
            } else if (arg.startsWith("--key-type=")) {
                keyType = arg.substring(11);
            } else if (arg.startsWith("--null-percent=")) {
                nullPercent = Integer.parseInt(arg.substring(15));
            } else if (arg.startsWith("--sum-columns=")) {
                sumColumns = Integer.parseInt(arg.substring(14));
            } else if (arg.startsWith("--commits-per-batch=")) {
                commitsPerBatch = Integer.parseInt(arg.substring(20));
            } else if (arg.startsWith("--o3-percent=")) {
                o3Percent = Double.parseDouble(arg.substring(13));
            } else if (arg.startsWith("--o3-lag=")) {
                o3Lag = arg.substring(9);
            } else if (arg.startsWith("--o3-from-batch=")) {
                o3FromBatch = Integer.parseInt(arg.substring(16));
            } else if (arg.startsWith("--o3-spread=")) {
                o3Spread = arg.substring(12);
            } else if (arg.startsWith("--o3-depths=")) {
                o3Depths = arg.substring(12);
            } else if (arg.startsWith("--o3-commit-percent=")) {
                o3CommitPercent = Integer.parseInt(arg.substring(20));
            } else if (arg.startsWith("--hot-key-percent=")) {
                hotKeyPercent = Double.parseDouble(arg.substring(18));
            } else if (arg.startsWith("--equal-ts-percent=")) {
                equalTsPercent = Double.parseDouble(arg.substring(19));
            } else if (arg.startsWith("--base-dedup=")) {
                isBaseDeduped = Boolean.parseBoolean(arg.substring(13));
            } else if (arg.startsWith("--lv-dedup=")) {
                isLvDeduped = Boolean.parseBoolean(arg.substring(11));
            } else if (arg.startsWith("--ts-step-us=")) {
                tsStepMicros = Long.parseLong(arg.substring(13));
            } else if (arg.startsWith("--repair-max-chained-boundaries=")) {
                repairMaxChainedBoundaries = Integer.parseInt(arg.substring(32));
            } else if (arg.startsWith("--repair-per-segment=")) {
                isRepairPerSegment = Boolean.parseBoolean(arg.substring(21));
            } else if (arg.startsWith("--repair-isolated-runtime=")) {
                isRepairIsolatedRuntime = Boolean.parseBoolean(arg.substring(26));
            } else if (arg.startsWith("--repair-segment-yield=")) {
                isRepairSegmentYield = Boolean.parseBoolean(arg.substring(23));
            } else if (arg.startsWith("--repair-keyed-replay=")) {
                isRepairKeyedReplay = Boolean.parseBoolean(arg.substring(22));
            } else if (arg.startsWith("--open-segment-keyed-replay=")) {
                isOpenSegmentKeyedReplay = Boolean.parseBoolean(arg.substring(28));
            } else if (arg.startsWith("--force-open-segment-keyed-replay=")) {
                forceOpenSegmentKeyedReplay = Boolean.parseBoolean(
                        arg.substring("--force-open-segment-keyed-replay=".length()));
            } else if (arg.startsWith("--keyed-scan-index-open-rows=")) {
                keyedScanIndexOpenRows = Long.parseLong(arg.substring("--keyed-scan-index-open-rows=".length()));
            } else {
                throw new IllegalArgumentException("unknown argument: " + arg);
            }
        }
        if (accountWindow > 0 && recycleAccounts > 0) {
            throw new IllegalArgumentException("--account-window and --recycle-accounts both pick the account, use one");
        }
        final Shape selectShape = Shape.of(shape);
        final KeyType partitionKeyType = KeyType.of(keyType);
        if (nullPercent < 0 || nullPercent > 100) {
            throw new IllegalArgumentException("--null-percent must be within [0, 100]: " + nullPercent);
        }
        if (sumColumns < 0 || sumColumns > MAX_SUM_COLUMNS) {
            throw new IllegalArgumentException("--sum-columns must be within [0, " + MAX_SUM_COLUMNS + "]: " + sumColumns);
        }
        if (commitsPerBatch < 1 || commitsPerBatch > batchRows) {
            throw new IllegalArgumentException("--commits-per-batch must be within [1, " + batchRows + "]: " + commitsPerBatch);
        }
        if (o3Percent < 0 || o3Percent > 50) {
            throw new IllegalArgumentException("--o3-percent must be within [0, 50]: " + o3Percent);
        }
        if (o3FromBatch < 0) {
            throw new IllegalArgumentException("--o3-from-batch must not be negative: " + o3FromBatch);
        }
        if (checkpointPurgeInterval < -1 || checkpointCompactionInterval < -1) {
            throw new IllegalArgumentException("checkpoint maintenance intervals must be -1 or non-negative");
        }
        if (o3CommitPercent < 1 || o3CommitPercent > 100) {
            throw new IllegalArgumentException("--o3-commit-percent must be within [1, 100]: " + o3CommitPercent);
        }
        if (hotKeyPercent < 0 || hotKeyPercent > 100) {
            throw new IllegalArgumentException("--hot-key-percent must be within [0, 100]: " + hotKeyPercent);
        }
        if (equalTsPercent < 0 || equalTsPercent > 50) {
            throw new IllegalArgumentException("--equal-ts-percent must be within [0, 50]: " + equalTsPercent);
        }
        if (tsStepMicros < 1) {
            throw new IllegalArgumentException("--ts-step-us must be positive: " + tsStepMicros);
        }
        // Only a SYMBOL key has a dictionary to pre-size or an index to build, so both
        // knobs describe nothing on an INT or LONG key. They are forced off and the
        // header line reports what the run actually used.
        if (partitionKeyType != KeyType.SYMBOL) {
            isIndexed = false;
            isSymbolPreSized = false;
        }

        // --anchor-period=daily:<zone> names an IANA zone for the DAILY anchor. That form
        // desugars to timestamp_floor_utc rather than timestamp_floor, and the zoned shape is
        // the one the checkpoint anchor planner recognizes through its own branch, so it is
        // what a run has to name to price a repair against a zone's civil day rather than
        // against a fixed 24-hour grid. Only DAILY carries a zone: the planner requires
        // ANCHOR DAILY for the zoned shape, so a zone on an ANCHOR EXPRESSION period would
        // move the view's buckets and still leave the run measuring an unplanned anchor.
        final int anchorZoneAt = anchorPeriodArg.indexOf(':');
        final String anchorPeriod = anchorZoneAt < 0 ? anchorPeriodArg : anchorPeriodArg.substring(0, anchorZoneAt);
        final String anchorZone = anchorZoneAt < 0 ? null : anchorPeriodArg.substring(anchorZoneAt + 1);
        if (anchorZone != null && !DAILY_ANCHOR_PERIOD.equals(anchorPeriod)) {
            throw new IllegalArgumentException(
                    "--anchor-period carries a time zone only on 'daily': " + anchorPeriodArg
            );
        }
        final long anchorPeriodMicros = anchorPeriodMicros(anchorPeriod);
        // The account window slides on a fixed 24-hour grid, so the offset it counts from has
        // to name the same instants the DDL's anchor fires on. A zoned anchor fires at
        // DAILY_ANCHOR_TIME local, which is the zone's offset earlier in UTC. Taking that
        // offset at START_TS makes the two grids coincide for as long as the run stays inside
        // one DST regime and drift by the transition's own amount past one - an approximation
        // in where accounts age out, not in what the view computes or what a repair plans.
        final long anchorOffsetMicros = DAILY_ANCHOR_PERIOD.equals(anchorPeriod)
                ? DAILY_ANCHOR_OFFSET_MICROS - anchorZoneOffsetMicros(anchorZone)
                : 0;
        final long totalRows = seedRows + (long) batchRows * batches;
        // What decides whether a sweep can fire at all: an account falls behind the
        // frontier only once the anchor has advanced two buckets past its last row.
        final long rowsPerBucket = anchorPeriodMicros / tsStepMicros;
        // Every o3EveryN-th row of a batch carries a timestamp o3LagMicros below the one
        // its position would give it, which is what the reported out-of-order workload
        // looks like on the wire: a forward stream with a minority of late arrivals
        // sprinkled through it. 0 leaves the generator strictly forward.
        final long o3EveryN = o3Percent > 0 ? Math.max(2, Math.round(100 / o3Percent)) : 0;
        final long o3LagMicros = o3EveryN > 0 ? anchorPeriodMicros(o3Lag) : 0;
        // How many distinct anchor segments the late rows of one commit reach. The
        // production logs price the deep tail per anchor segment a correction touches,
        // so a run that lands every late row in the same segment measures the cheapest
        // shape rather than the reported one. Consecutive late rows step down by one
        // whole anchor period, which is what puts them in distinct segments; a spread
        // narrower than the anchor period therefore describes nothing and leaves one
        // step, which the header line reports.
        final long o3SpreadMicros = o3EveryN > 0 && o3Spread != null ? anchorPeriodMicros(o3Spread) : 0;
        final int o3SpreadSteps = (int) Math.max(1, o3SpreadMicros / anchorPeriodMicros + (o3SpreadMicros > 0 ? 1 : 0));
        // The deepest a late row goes, which is what the generator has to keep above
        // START_TS: a row shifted below the view's own start produces no output at all
        // and would be counted as a rejection rather than as a correction.
        final long o3MaxLagMicros = o3LagMicros + (long) (o3SpreadSteps - 1) * anchorPeriodMicros;
        // The depth ladder, when one is given. It replaces --o3-lag and --o3-spread outright:
        // a late row picks its depth from the ladder by its own ordinal, and a depth that
        // would put the row below START_TS leaves that row where it was rather than
        // suppressing every late row in the run, which is what the single-depth guard does.
        final O3Depths o3DepthLadder = o3EveryN > 0 && o3Depths != null ? O3Depths.of(o3Depths) : null;
        // Every hotKeyEveryN-th row carries the same key, so one posting list holds
        // hotKeyPercent of the base. Fix 3B prices a keyed scan from posting counts
        // precisely because that distribution decides it, and a run where every key is
        // equally cold cannot show the case it loses on.
        final long hotKeyEveryN = hotKeyPercent > 0 ? Math.max(2, Math.round(100 / hotKeyPercent)) : 0;
        // Every equalTsEveryN-th row repeats the row below it - both its timestamp and
        // its key - so the (timestamp, key) group holds two qualifying rows. That is the
        // shape 3C's ordinal exists for, and the one measurement 4 of Stage 0 counts in
        // the real base.
        final long equalTsEveryN = equalTsPercent > 0 ? Math.max(2, Math.round(100 / equalTsPercent)) : 0;
        final int commitRows = batchRows / commitsPerBatch;
        final RowShape rowShape = new RowShape(recycleAccounts, accountWindow, anchorPeriodMicros, anchorOffsetMicros,
                partitionKeyType, nullPercent, sumColumns, tsStepMicros, hotKeyEveryN, equalTsEveryN);

        final Path dbRoot = Files.createTempDirectory("lv-steady-");
        CairoEngine engine = null;
        final long finalCheckpointRows = checkpointRows;
        final long finalCheckpointDuration = checkpointDurationMicros;
        final long finalCheckpointPurgeInterval = checkpointPurgeInterval;
        final long finalCheckpointCompactionInterval = checkpointCompactionInterval;
        final int finalCompactThreshold = compactThreshold;
        final int finalCompactStalePercent = compactStalePercent;
        final int finalRepairMaxChainedBoundaries = repairMaxChainedBoundaries;
        final boolean finalRepairIsolatedRuntime = isRepairIsolatedRuntime;
        final boolean finalRepairPerSegment = isRepairPerSegment;
        final boolean finalRepairSegmentYield = isRepairSegmentYield;
        final boolean finalRepairKeyedReplay = isRepairKeyedReplay;
        final boolean finalOpenSegmentKeyedReplay = isOpenSegmentKeyedReplay;
        final long finalKeyedScanIndexOpenRows = keyedScanIndexOpenRows;
        final boolean finalLvDedup = isLvDeduped;
        try {
            final CairoConfiguration configuration = new DefaultCairoConfiguration(dbRoot.toString()) {
                @Override
                public long getLiveViewCheckpointMaxDurationMicros() {
                    return finalCheckpointDuration;
                }

                @Override
                public long getLiveViewCheckpointCompactionInterval() {
                    return finalCheckpointCompactionInterval >= 0
                            ? finalCheckpointCompactionInterval
                            : super.getLiveViewCheckpointCompactionInterval();
                }

                @Override
                public long getLiveViewCheckpointPurgeInterval() {
                    return finalCheckpointPurgeInterval >= 0
                            ? finalCheckpointPurgeInterval
                            : super.getLiveViewCheckpointPurgeInterval();
                }

                @Override
                public int getLiveViewCheckpointRepairMaxChainedBoundaries() {
                    return finalRepairMaxChainedBoundaries >= 0
                            ? finalRepairMaxChainedBoundaries
                            : super.getLiveViewCheckpointRepairMaxChainedBoundaries();
                }

                @Override
                public long getLiveViewCheckpointRows() {
                    return finalCheckpointRows;
                }

                @Override
                public int getLiveViewPartitionCompactStalePercent() {
                    return finalCompactStalePercent >= 0
                            ? finalCompactStalePercent
                            : super.getLiveViewPartitionCompactStalePercent();
                }

                @Override
                public int getLiveViewPartitionCompactThreshold() {
                    return finalCompactThreshold > 0
                            ? finalCompactThreshold
                            : super.getLiveViewPartitionCompactThreshold();
                }

                @Override
                public boolean isDevModeEnabled() {
                    return true;
                }

                @Override
                public boolean isLiveViewCheckpointRepairIsolatedRuntimeEnabled() {
                    return finalRepairIsolatedRuntime;
                }

                @Override
                public boolean isLiveViewCheckpointRepairPerSegmentEnabled() {
                    return finalRepairPerSegment;
                }

                @Override
                public boolean isLiveViewCheckpointRepairSegmentYieldEnabled() {
                    return finalRepairSegmentYield;
                }

                @Override
                public boolean isLiveViewCheckpointRepairKeyedReplayEnabled() {
                    return finalRepairKeyedReplay;
                }

                @Override
                public long getLiveViewCheckpointRepairKeyedScanIndexOpenRows() {
                    return finalKeyedScanIndexOpenRows >= 0
                            ? finalKeyedScanIndexOpenRows
                            : super.getLiveViewCheckpointRepairKeyedScanIndexOpenRows();
                }

                @Override
                public boolean isLiveViewCheckpointRepairOpenSegmentKeyedReplayEnabled() {
                    return finalOpenSegmentKeyedReplay;
                }

                @Override
                public boolean isLiveViewCheckpointRepairSparsePublicationEnabled() {
                    return finalLvDedup;
                }
            };
            System.out.printf(
                    Locale.ROOT,
                    "# seed=%d batch=%d batches=%d checkpointRows=%d checkpointPurgeInterval=%d "
                            + "checkpointCompactionInterval=%d preSizeSymbol=%s index=%s recycleAccounts=%d "
                            + "anchorPeriod=%s anchorZone=%s accountWindow=%d rowsPerBucket=%d buckets=%d compactThreshold=%d "
                            + "compactStalePercent=%d shape=%s keyType=%s nullPercent=%d sumColumns=%d "
                            + "commitsPerBatch=%d commitRows=%d o3EveryN=%d o3Lag=%s o3LagRows=%d o3FromBatch=%d "
                            + "o3SpreadSteps=%d o3MaxLagRows=%d o3Depths=%s o3CommitPercent=%d "
                            + "hotKeyEveryN=%d equalTsEveryN=%d tsStepUs=%d "
                            + "spanHours=%.2f baseDedup=%s lvDedup=%s repairMaxChainedBoundaries=%d repairPerSegment=%s "
                            + "repairIsolatedRuntime=%s repairSegmentYield=%s repairKeyedReplay=%s "
                            + "openSegmentKeyedReplay=%s keyedScanIndexOpenRows=%d%n",
                    seedRows, batchRows, batches, checkpointRows,
                    configuration.getLiveViewCheckpointPurgeInterval(),
                    configuration.getLiveViewCheckpointCompactionInterval(),
                    isSymbolPreSized, isIndexed, recycleAccounts,
                    anchorPeriod, anchorZone == null ? "none" : anchorZone,
                    accountWindow, rowsPerBucket, totalRows / rowsPerBucket,
                    configuration.getLiveViewPartitionCompactThreshold(),
                    configuration.getLiveViewPartitionCompactStalePercent(),
                    selectShape.name, partitionKeyType.name, nullPercent, sumColumns,
                    commitsPerBatch, commitRows, o3EveryN, o3EveryN > 0 ? o3Lag : "none", o3LagMicros / tsStepMicros,
                    o3FromBatch, o3SpreadSteps, o3MaxLagMicros / tsStepMicros,
                    o3DepthLadder == null ? "none" : o3DepthLadder.describe(), o3CommitPercent,
                    hotKeyEveryN, equalTsEveryN, tsStepMicros, (double) totalRows * tsStepMicros / Micros.HOUR_MICROS, isBaseDeduped,
                    configuration.isLiveViewCheckpointRepairSparsePublicationEnabled(),
                    configuration.getLiveViewCheckpointRepairMaxChainedBoundaries(),
                    configuration.isLiveViewCheckpointRepairPerSegmentEnabled(),
                    configuration.isLiveViewCheckpointRepairIsolatedRuntimeEnabled(),
                    configuration.isLiveViewCheckpointRepairSegmentYieldEnabled(),
                    configuration.isLiveViewCheckpointRepairKeyedReplayEnabled(),
                    configuration.isLiveViewCheckpointRepairOpenSegmentKeyedReplayEnabled(),
                    configuration.getLiveViewCheckpointRepairKeyedScanIndexOpenRows()
            );

            engine = new CairoEngine(configuration);
            engine.load();
            final SqlExecutionContext sqlCtx = new SqlExecutionContextImpl(engine, 1).with(
                    configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                    null, null, -1, null
            );

            // Distinct accounts, not rows: a rolling window revisits its accounts, so
            // sizing the symbol map on the row count would over-allocate it by orders
            // of magnitude and distort the footprint line.
            final long distinctAccounts = distinctAccounts(totalRows, rowsPerBucket, recycleAccounts, accountWindow);
            final String capacity = isSymbolPreSized ? " capacity " + symbolCapacity(distinctAccounts) : "";
            final String indexClause = isIndexed ? " index capacity 4" : "";
            engine.execute(
                    "create table payments ("
                            + "created_at timestamp, "
                            + "account_id " + partitionKeyType.columnDdl(capacity, indexClause) + ", "
                            + "amount double"
                            + sumColumnDdl(sumColumns)
                            + ") timestamp(created_at) partition by hour wal"
                            + (isBaseDeduped ? " dedup upsert keys(created_at, account_id)" : ""),
                    sqlCtx
            );
            engine.execute(insertSql(rowShape, 1, seedRows, 0, 0, 1, false), sqlCtx);
            drainWal(engine);

            engine.execute(
                    "create live view " + VIEW_NAME + " "
                            + "flush every 5s start from beginning as "
                            + "select created_at, account_id, "
                            + selectShape.projections(sumColumns)
                            + " from payments "
                            + "window w as (partition by account_id order by created_at "
                            + anchorClause(anchorPeriod, anchorZone) + ")"
                            + selectShape.extraWindows(),
                    sqlCtx
            );
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(VIEW_NAME);
            final TableToken lvToken = engine.getTableTokenIfExists(VIEW_NAME);
            final CheckpointSegments segments = new CheckpointSegments(dbRoot.resolve(lvToken.getDirName()));

            try (
                    LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
                    // What a refresh allocates and hands back within the batch. The window
                    // state it retains shows up in the resting level rather than here, so the
                    // reading isolates the transient: the WAL reader's symbol maps, the
                    // per-txn diff overlays and the checkpoint's encode scratch.
                    NativeTagPeakSampler sampler = new NativeTagPeakSampler(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM)
            ) {
                job.setForceOpenSegmentKeyedReplayForTest(forceOpenSegmentKeyedReplay);
                final long seedStart = System.nanoTime();
                drainLiveView(engine, instance, job);
                System.out.printf(Locale.ROOT, "# seed_ms=%.3f seed_checkpoint_ms=%.3f%n",
                        (System.nanoTime() - seedStart) / 1e6, instance.getHeadCheckpointWriteMicros() / 1e3);

                segments.sample();
                System.out.println("batch\tstate_rows\tbase_apply_ms\trefresh_ms\trefresh_max_pass_ms\tcheckpoint_ms\trefresh_ex_cp_ms\trows_per_sec\tstate_bytes\tlag_seqtxn\tfaults\tmap_rows\tsweeps\tevicted\tsweep_ms\trefresh_peak_mb\tmeta_segs\tdata_segs\tmeta_bytes\tdata_bytes\to3_scan_rows\to3_resume_rows\to3_boundary_rows\ttl_gen\ttl_entries\thead_root\thead_lag_rows\tlv_apply_ms\tlv_rows\tlv_phys_rows\tlv_write_amp\tlv_parts\trepair");
                long firstRow = seedRows + 1;
                // A seal after a sweep is the one this measurement is about: compact()
                // demotes the next seal to a full scan of the whole live state, while a
                // seal in a sweep-free batch freezes only the keys the batch touched.
                double sweptSealMs = 0.0;
                int sweptSealCount = 0;
                double unsweptSealMs = 0.0;
                int unsweptSealCount = 0;
                long compactionCountBefore = compactionCount(instance);
                long compactedPartitionsBefore = compactedPartitionCount(instance);
                long compactionMicrosBefore = compactionMicros(instance);
                // The repair counters are cumulative over the view's life, so a batch's own
                // replay volume is their delta. The seed's own restore work is below the
                // first reading and stays out of every batch.
                long o3ScanRowsBefore = instance.getO3ReplayScanRows();
                long o3ResumeRowsBefore = instance.getO3ResumeReplayRows();
                long o3BoundaryRowsBefore = instance.getO3BoundaryReplayRows();
                final long o3ResumeRowsAtStart = o3ResumeRowsBefore;
                final long o3BoundaryRowsAtStart = o3BoundaryRowsBefore;
                long o3ScanRowsTotal = 0;
                // How many commits have carried late rows, which the closing line reports
                // and a ladder walks its rungs by. Counted on every eligible commit rather
                // than only on a ladder's, so the figure means the same thing on a
                // single-depth run; it spans batches, so a rung's share is exact over the
                // run rather than restarted every batch.
                long o3CommitOrdinal = 0;
                // The write side of a refresh, accumulated over the run: what the batches
                // emitted, what the apply physically wrote for it, and how long that took.
                long lvRowsTotal = 0;
                long lvPhysRowsTotal = 0;
                long lvApplyMicrosTotal = 0;
                for (int b = 0; b < batches; b++) {
                    // One INSERT is one WAL commit, and O3 is classified per commit, so
                    // the split decides how many repairs a batch triggers as much as the
                    // late-row share does.
                    // --o3-from-batch keeps the leading batches strictly forward, so the
                    // view builds the ladder of checkpoint boundaries a real one would hold
                    // before the first late row arrives. What a repair can resume from
                    // depends entirely on that ladder.
                    final long batchO3EveryN = b >= o3FromBatch ? o3EveryN : 0;
                    for (int c = 0; c < commitsPerBatch; c++) {
                        final long commitFirstRow = firstRow + (long) c * commitRows;
                        final int rows = c == commitsPerBatch - 1 ? batchRows - c * commitRows : commitRows;
                        // Out-of-order is classified per commit, so which commits are
                        // eligible is a knob of its own: a run where every commit carries a
                        // late row triggers a repair on every commit, whatever the late-row
                        // share is. The eligible ones are spread evenly over each hundred.
                        final long commitIndex = (long) b * commitsPerBatch + c;
                        final boolean isCommitO3 = batchO3EveryN > 0 && commitIndex % 100 < o3CommitPercent;
                        // A commit's correction depth is one rung of the ladder, walked by
                        // the eligible commit's own ordinal rather than by the raw commit
                        // index - which would correlate the rung with the eligibility test
                        // and leave the ladder's upper rungs unreachable. All of a commit's
                        // late rows then share one depth, so a commit corrects one segment,
                        // which is what the production logs show 162 of 193 deep commits do.
                        if (isCommitO3) {
                            o3CommitOrdinal++;
                        }
                        final long commitLagMicros = isCommitO3 && o3DepthLadder != null
                                ? o3DepthLadder.depthMicrosFor(o3CommitOrdinal - 1)
                                : o3LagMicros;
                        engine.execute(
                                insertSql(rowShape, commitFirstRow, rows, isCommitO3 ? batchO3EveryN : 0,
                                        commitLagMicros, o3DepthLadder != null ? 1 : o3SpreadSteps,
                                        o3DepthLadder != null),
                                sqlCtx
                        );
                    }
                    final long baseStart = System.nanoTime();
                    drainWal(engine);
                    final long baseNanos = System.nanoTime() - baseStart;

                    // A seal is detected by its write clock, not by the head root id: an O3
                    // repair retires the timeline and re-opens it, so the fresh head can carry
                    // the same root id the retired one did, and an id comparison then reports
                    // no checkpoint for the most expensive seal the view performs.
                    final long checkpointWrittenUsBefore = instance.getLastCheckpointWrittenUs();
                    sampler.reset();
                    // What the batch's corrected output costs to land. Committed rows are
                    // the net row count the view gained; physically written rows are what
                    // the apply actually wrote for it, which is the whole of every partition
                    // it had to merge rather than append to. The two are equal for a strictly
                    // forward run and diverge by the partition rewrite fix 3 is about - a
                    // repair's replacement commit re-emits its whole range, so the view gains
                    // one batch and the apply writes the range. Both counters are engine-wide,
                    // and the base is drained above, so the delta across the refresh is the
                    // live view's own.
                    final long lvRowsBefore = engine.getMetrics().tableWriterMetrics().getCommittedRows();
                    final long lvPhysRowsBefore = engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
                    final long lvApplyUsBefore = job.getLiveViewApplyMicros();
                    final long refreshStart = System.nanoTime();
                    final long maxPassNanos = drainLiveView(engine, instance, job);
                    final long refreshNanos = System.nanoTime() - refreshStart;
                    final long lvRows = engine.getMetrics().tableWriterMetrics().getCommittedRows() - lvRowsBefore;
                    final long lvPhysRows =
                            engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - lvPhysRowsBefore;
                    final long lvApplyUs = job.getLiveViewApplyMicros() - lvApplyUsBefore;
                    lvRowsTotal += lvRows;
                    lvPhysRowsTotal += lvPhysRows;
                    lvApplyMicrosTotal += lvApplyUs;
                    final double refreshPeakMb = sampler.peakAboveBaseBytes() / (1024.0 * 1024.0);
                    final boolean isCheckpointWritten = instance.getLastCheckpointWrittenUs() != checkpointWrittenUsBefore;
                    final double checkpointMs = isCheckpointWritten ? instance.getHeadCheckpointWriteMicros() / 1e3 : 0.0;

                    final long expected = firstRow - 1 + batchRows;
                    final long lvRowsNow = instance.getLvRowsTotal();
                    if (lvRowsNow != expected) {
                        throw new IllegalStateException("row mismatch: expected " + expected
                                + ", got " + lvRowsNow);
                    }
                    // A recompiled window starts its counters at zero, so a delta below the
                    // previous reading is that reading itself, not a negative sweep count.
                    final long compactionCountAfter = compactionCount(instance);
                    final long compactedPartitionsAfter = compactedPartitionCount(instance);
                    final long compactionMicrosAfter = compactionMicros(instance);
                    final long sweeps = Math.max(0, compactionCountAfter - compactionCountBefore);
                    final long evicted = Math.max(0, compactedPartitionsAfter - compactedPartitionsBefore);
                    final double sweepMs = Math.max(0, compactionMicrosAfter - compactionMicrosBefore) / 1e3;
                    compactionCountBefore = compactionCountAfter;
                    compactedPartitionsBefore = compactedPartitionsAfter;
                    compactionMicrosBefore = compactionMicrosAfter;
                    if (isCheckpointWritten) {
                        if (sweeps > 0) {
                            sweptSealMs += checkpointMs;
                            sweptSealCount++;
                        } else {
                            unsweptSealMs += checkpointMs;
                            unsweptSealCount++;
                        }
                    }

                    final long o3ScanRowsAfter = instance.getO3ReplayScanRows();
                    final long o3ResumeRowsAfter = instance.getO3ResumeReplayRows();
                    final long o3BoundaryRowsAfter = instance.getO3BoundaryReplayRows();
                    final long o3ScanRows = o3ScanRowsAfter - o3ScanRowsBefore;
                    final long o3ResumeRows = o3ResumeRowsAfter - o3ResumeRowsBefore;
                    final long o3BoundaryRows = o3BoundaryRowsAfter - o3BoundaryRowsBefore;
                    o3ScanRowsBefore = o3ScanRowsAfter;
                    o3ResumeRowsBefore = o3ResumeRowsAfter;
                    o3BoundaryRowsBefore = o3BoundaryRowsAfter;
                    o3ScanRowsTotal += o3ScanRows;
                    // What the last repair of the batch decided, and why it read more than a
                    // localized rebuild would have. Every repair in one batch takes the same
                    // route in this workload, so the last is representative.
                    final String repair = repairName(instance);

                    final long baseSeqTxn = engine.getTableSequencerAPI()
                            .getTxnTracker(engine.getTableTokenIfExists("payments"))
                            .getWriterTxn();
                    segments.sample();
                    System.out.printf(
                            Locale.ROOT,
                            "%d\t%d\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.0f\t%d\t%d\t%d\t%d\t%d\t%d\t%.3f\t%.1f\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%.3f\t%d\t%d\t%.1f\t%d\t%s%n",
                            b,
                            expected,
                            baseNanos / 1e6,
                            refreshNanos / 1e6,
                            maxPassNanos / 1e6,
                            checkpointMs,
                            refreshNanos / 1e6 - checkpointMs,
                            batchRows / (refreshNanos / 1e9),
                            instance.getHeadCheckpointStateBytes(),
                            baseSeqTxn - instance.getLastProcessedSeqTxn(),
                            instance.getRefreshFaultCount(),
                            anchorMapSize(instance),
                            sweeps,
                            evicted,
                            sweepMs,
                            refreshPeakMb,
                            segments.getAddedMetaSegments(),
                            segments.getAddedDataSegments(),
                            segments.getAddedMetaBytes(),
                            segments.getAddedDataBytes(),
                            o3ScanRows,
                            o3ResumeRows,
                            o3BoundaryRows,
                            timelineGeneration(instance),
                            timelineEntries(instance),
                            instance.getHeadCheckpointRootId(),
                            headLagRows(instance, tsStepMicros),
                            lvApplyUs / 1e3,
                            lvRows,
                            lvPhysRows,
                            lvRows > 0 ? (double) lvPhysRows / lvRows : 0.0,
                            partitionCount(engine, lvToken),
                            repair
                    );
                    firstRow += batchRows;
                }
                if (o3EveryN > 0) {
                    // Read amplification is the whole story of an out-of-order run: how many
                    // base rows the repairs re-read for every row the batches ingested. A
                    // strictly forward run reads each row once, so anything above 1 is what
                    // lateness costs.
                    final long ingested = (long) batchRows * Math.max(1, batches - o3FromBatch);
                    System.out.printf(
                            Locale.ROOT,
                            "# o3 ingested=%d late_commits=%d scan_rows=%d amplification=%.1fx resume_rows=%d "
                                    + "boundary_rows=%d "
                                    + "rejected=%d repair=%s isolated_replays=%d segment_yields=%d%n",
                            ingested,
                            o3CommitOrdinal,
                            o3ScanRowsTotal,
                            (double) o3ScanRowsTotal / ingested,
                            instance.getO3ResumeReplayRows() - o3ResumeRowsAtStart,
                            instance.getO3BoundaryReplayRows() - o3BoundaryRowsAtStart,
                            instance.getO3RejectedCount(),
                            repairName(instance),
                            job.isolatedReplayTurnCountForTest(),
                            job.segmentYieldCountForTest()
                    );
                    // What a keyed replay of each corrected closed segment would have read,
                    // beside what the segment replays this run actually performed did.
                    // cheaper counts the segments where the keyed side wins once the merge
                    // and the per-key-per-frame setup are priced in; unpriced counts the
                    // ones with no usable key domain, which read whole either way.
                    //
                    // With --repair-keyed-replay=false - the default, and the control
                    // column - nothing takes the route, so posting_rows is a
                    // counterfactual and whole_rows is the measurement it is
                    // counterfactual to. With it on, keyed_segments counts the segments
                    // that took it and merged_rows the rows they copied forward from the
                    // view's own output instead of recomputing; the block still carries
                    // the segment's whole row set, so merged_rows is a read saving rather
                    // than a write one.
                    System.out.printf(
                            Locale.ROOT,
                            "# keyed priced=%d cheaper=%d posting_rows=%d whole_rows=%d unpriced=%d "
                                    + "keyed_segments=%d merged_rows=%d%n",
                            job.keyedScanPricedCountForTest(),
                            job.keyedScanCheaperCountForTest(),
                            job.keyedScanPostingRowsForTest(),
                            job.keyedScanWholeRangeRowsForTest(),
                            job.keyedScanUnpricedCountForTest(),
                            job.keyedReplaySegmentCountForTest(),
                            job.keyedReplayMergedRowsForTest()
                    );
                    // What a sparse keyed publication would cost to attempt: whether each
                    // repaired segment's own output carries every (timestamp, projected
                    // key) pair once. It runs dark - every repair publishes its whole
                    // replaced range with REPLACE_RANGE either way - so checked minus
                    // unique is the fallback rate such a publication would run at, and
                    // duplicate_rows is what it would silently collapse if it did not
                    // fall back. max_group is the widest equal-timestamp group any repair
                    // emitted, which is both what the detector's scratch is worth and how
                    // close the shape sits to a duplicate; unchecked counts repairs whose
                    // output carries no key the pair could be named through.
                    System.out.printf(
                            Locale.ROOT,
                            "# unique checked=%d unique=%d rows=%d duplicate_rows=%d "
                                    + "max_group=%d unchecked=%d%n",
                            job.outputUniquenessCheckedRepairsForTest(),
                            job.outputUniquenessUniqueRepairsForTest(),
                            job.outputUniquenessCheckedRowsForTest(),
                            job.outputUniquenessDuplicateRowsForTest(),
                            job.outputUniquenessMaxGroupRowsForTest(),
                            job.outputUniquenessUncheckedRepairsForTest()
                    );
                    // What acting on that verdict actually did. published counts the
                    // segment repairs that committed only the rows they recomputed,
                    // upserted onto the view's own dedup keys; rows_kept is the stored
                    // rows those publications left exactly where they stood and a
                    // replacement would have had to rewrite - so it is the write saving,
                    // where merged_rows above is the read one. fallback counts the
                    // attempts abandoned before commit, which published their whole range
                    // with REPLACE_RANGE instead. All three are zero unless the view was
                    // CREATEd with --lv-dedup=true and --repair-keyed-replay=true is on:
                    // the identity is a schema property and only a keyed repair has a
                    // smaller row set to publish.
                    System.out.printf(
                            Locale.ROOT,
                            "# open_keyed priced=%d cheaper=%d posting_rows=%d whole_rows=%d unpriced=%d "
                                    + "resumes=%d arithmetic_positions=%d sparse_resumes=%d%n",
                            job.openSegmentKeyedPricedCountForTest(),
                            job.openSegmentKeyedCheaperCountForTest(),
                            job.openSegmentKeyedPostingRowsForTest(),
                            job.openSegmentKeyedWholeRangeRowsForTest(),
                            job.openSegmentKeyedUnpricedCountForTest(),
                            job.openSegmentKeyedResumeCountForTest(),
                            job.openSegmentArithmeticRowPositionCountForTest(),
                            job.openSegmentSparseResumeCountForTest()
                    );
                    System.out.printf(
                            Locale.ROOT,
                            "# sparse published=%d fallback=%d rows_kept=%d open_arithmetic_positions=%d%n",
                            job.sparsePublicationCountForTest(),
                            job.sparsePublicationFallbackCountForTest(),
                            job.sparsePublicationRowsKeptForTest(),
                            job.openSegmentArithmeticRowPositionCountForTest()
                    );
                }
                // The write side of the run, which is the term fix 3 turns on. A strictly
                // forward run appends, so amplification is 1 and the apply is a rounding
                // error beside the refresh. A repair whose replacement range reaches a
                // closed partition merges it, and every row of that partition is rewritten
                // - so amplification is the ratio of live-view rows the apply wrote to
                // live-view rows the refresh emitted, and bytes_per_row turns it into the
                // figure the design is priced in.
                reportLiveViewWrites(engine, lvToken, lvRowsTotal, lvPhysRowsTotal, lvApplyMicrosTotal);
                reportSweeps(instance, sweptSealMs, sweptSealCount, unsweptSealMs, unsweptSealCount);
                segments.report();

                if (isRestartMeasured) {
                    // Drop the in-memory instances and read the definitions back from
                    // disk, so the next refresh has to rebuild every window map from
                    // the checkpoint rather than from memory. This is the restart the
                    // live view tests drive, and it isolates the checkpoint restore
                    // from the rest of engine startup. One small batch is what makes
                    // the view resume at all - with nothing to process it would not
                    // restore - and its own refresh cost is negligible beside a
                    // multi-million key restore.
                    final long stateRows = firstRow - 1;
                    engine.getLiveViewRegistry().clear();
                    engine.buildViewGraphs();
                    engine.execute(insertSql(rowShape, firstRow, RESTART_PROBE_ROWS, 0, 0, 1, false), sqlCtx);
                    drainWal(engine);
                    final LiveViewInstance restarted = engine.getLiveViewRegistry().getViewInstance(VIEW_NAME);
                    try (LiveViewRefreshJob restartJob = new LiveViewRefreshJob(0, engine, 1)) {
                        final long checkpointWrittenUsBefore = restarted.getLastCheckpointWrittenUs();
                        sampler.reset();
                        final long restoreStart = System.nanoTime();
                        drainLiveView(engine, restarted, restartJob);
                        final long restoreNanos = System.nanoTime() - restoreStart;
                        final double restorePeakMb = sampler.peakAboveBaseBytes() / (1024.0 * 1024.0);
                        // The seal the resumed view performs in this window is reported
                        // separately: it is a distinct cost from reading the checkpoint
                        // back, and whether it full-scans the restored state or freezes
                        // only the probe's keys is what dominates a restart. state_bytes
                        // is the head root's logical size after that seal - it must still
                        // account for every restored key, which is what tells an
                        // incremental reseal apart from one that dropped state.
                        final boolean isCheckpointWritten = restarted.getLastCheckpointWrittenUs() != checkpointWrittenUsBefore;
                        final double checkpointMs = isCheckpointWritten ? restarted.getHeadCheckpointWriteMicros() / 1e3 : 0.0;
                        final long expected = stateRows + RESTART_PROBE_ROWS;
                        if (restarted.getLvRowsTotal() != expected) {
                            throw new IllegalStateException(
                                    "restore row mismatch: expected " + expected + ", got " + restarted.getLvRowsTotal());
                        }
                        System.out.printf(
                                Locale.ROOT,
                                "# restore state_rows=%d window_ms=%.3f reseal_ms=%.3f read_back_ms=%.3f probe_rows=%d "
                                        + "state_bytes=%d lookup_depth=%d faults=%d peak_mb=%.1f%n",
                                stateRows,
                                restoreNanos / 1e6,
                                checkpointMs,
                                restoreNanos / 1e6 - checkpointMs,
                                RESTART_PROBE_ROWS,
                                restarted.getHeadCheckpointStateBytes(),
                                restarted.getCheckpointLastLookupDepth(),
                                restarted.getRefreshFaultCount(),
                                restorePeakMb
                        );
                    }
                }

                reportFootprint(engine, dbRoot, partitionKeyType == KeyType.SYMBOL);
            }
        } finally {
            engine = Misc.free(engine);
            deleteRecursively(dbRoot);
        }
    }

    private static long anchorMapSize(LiveViewInstance instance) {
        final LiveViewWindow window = instance.getAnchorWindow();
        return window == null ? 0 : window.getAnchorMapSize();
    }

    /**
     * ANCHOR DAILY is the sugar the reported workload uses. Every other period goes
     * through {@code ANCHOR EXPRESSION timestamp_floor(...)}, the two-argument form the
     * live view compiler can still prove monotone in the designated timestamp - which
     * is what leaves the frontier sweep enabled at all.
     * <p>
     * A named zone rides on the DAILY sugar as its second literal, which is the only
     * spelling that reaches {@code timestamp_floor_utc} and so the only one that reaches
     * the anchor planner's zoned branch.
     */
    private static String anchorClause(String anchorPeriod, String anchorZone) {
        if (!DAILY_ANCHOR_PERIOD.equals(anchorPeriod)) {
            return "anchor expression timestamp_floor('" + anchorPeriod + "', created_at)";
        }
        return anchorZone == null
                ? "anchor daily '" + DAILY_ANCHOR_TIME + "'"
                : "anchor daily '" + DAILY_ANCHOR_TIME + "' '" + anchorZone + "'";
    }

    /**
     * The anchor bucket width in micros, used to place a row's account in the rolling
     * window. Only fixed-duration units are accepted: a calendar unit does not divide
     * the timestamp axis evenly, so the account window could not slide in step with it.
     */
    private static long anchorPeriodMicros(String anchorPeriod) {
        if (DAILY_ANCHOR_PERIOD.equals(anchorPeriod)) {
            return Micros.DAY_MICROS;
        }
        final int length = anchorPeriod.length();
        if (length < 2) {
            throw new IllegalArgumentException("--anchor-period must be 'daily' or <count><unit>, e.g. 5m");
        }
        final long unitMicros = switch (anchorPeriod.charAt(length - 1)) {
            case 's' -> Micros.SECOND_MICROS;
            case 'm' -> Micros.MINUTE_MICROS;
            case 'h' -> Micros.HOUR_MICROS;
            case 'd' -> Micros.DAY_MICROS;
            default ->
                    throw new IllegalArgumentException("--anchor-period unit must be one of s, m, h, d: " + anchorPeriod);
        };
        final long count = Long.parseLong(anchorPeriod.substring(0, length - 1));
        if (count < 1) {
            throw new IllegalArgumentException("--anchor-period count must be positive: " + anchorPeriod);
        }
        return count * unitMicros;
    }

    /**
     * The zone's UTC offset at the run's own {@code START_TS}, which is what turns the local
     * {@code DAILY_ANCHOR_TIME} the DDL names into the UTC instants the account window's
     * fixed grid counts from. Resolving the zone here also fails a misspelled one on the
     * command line rather than deep inside CREATE LIVE VIEW.
     */
    private static long anchorZoneOffsetMicros(String anchorZone) {
        if (anchorZone == null) {
            return 0;
        }
        try {
            return DateLocaleFactory.EN_LOCALE
                    .getRules(anchorZone, TimeZoneRuleFactory.RESOLUTION_MICROS)
                    .getOffset(START_TS);
        } catch (NumericException e) {
            throw new IllegalArgumentException("--anchor-period names an unknown time zone: " + anchorZone);
        }
    }

    private static long compactedPartitionCount(LiveViewInstance instance) {
        final LiveViewWindow window = instance.getAnchorWindow();
        return window == null ? 0 : window.getCompactedPartitionCount();
    }

    private static long compactionCount(LiveViewInstance instance) {
        final LiveViewWindow window = instance.getAnchorWindow();
        return window == null ? 0 : window.getCompactionCount();
    }

    private static long compactionMicros(LiveViewInstance instance) {
        final LiveViewWindow window = instance.getAnchorWindow();
        return window == null ? 0 : window.getCompactionMicros();
    }

    /**
     * How many accounts the run creates, which is what the output SYMBOL column has to
     * be sized for. A rolling window creates its own width once and then one slide per
     * anchor bucket the run spans.
     */
    private static long distinctAccounts(long totalRows, long rowsPerBucket, int recycleAccounts, long accountWindow) {
        if (accountWindow > 0) {
            return accountWindow + (totalRows / rowsPerBucket + 1) * Math.max(1, accountWindow / 2);
        }
        return recycleAccounts > 0 ? recycleAccounts : totalRows;
    }

    /**
     * How far the head checkpoint's boundary sits below the rows the view has seen,
     * counted in rows of the generated stream. A resume replays from the boundary
     * forward, so this is what one would cost; a head that stops advancing while the
     * view keeps ingesting is a resume growing without bound.
     */
    private static long headLagRows(LiveViewInstance instance, long tsStepMicros) {
        final long headMaxTs = instance.getHeadCheckpointMaxTs();
        final long latestSeenTs = instance.getLatestSeenTs();
        return headMaxTs == Numbers.LONG_NULL || latestSeenTs == Numbers.LONG_NULL
                ? -1
                : (latestSeenTs - headMaxTs) / tsStepMicros;
    }

    /**
     * How many partitions the live view holds. What one repair rewrites is bounded by
     * this: a replacement range spanning the whole anchor segment covers every live-view
     * partition inside it, and each of those the apply merges rather than appends to is
     * rewritten whole. A run whose live view holds one partition cannot show that cost at
     * all, which is why {@code --ts-step-us} exists.
     */
    private static long partitionCount(CairoEngine engine, TableToken token) {
        try (TableReader reader = engine.getReader(token)) {
            return reader.getPartitionCount();
        }
    }

    /**
     * How many logical boundaries the published checkpoint timeline holds. This is what
     * an out-of-order repair searches for an anchor below the change, so a timeline that
     * has been retired to nothing is a repair that can only rebuild from the view's own
     * start - the {@code checkpoint_timeline_entries} column of {@code live_views()}.
     */
    private static long timelineEntries(LiveViewInstance instance) {
        final long[] timeline = instance.getCheckpointTimeline();
        return timeline.length > 1 ? timeline[1] : 0;
    }

    /**
     * The published timeline generation, {@code checkpoint_timeline_generation}. It
     * advances once per publication, so a batch that sealed nothing leaves it standing.
     */
    private static long timelineGeneration(LiveViewInstance instance) {
        final long[] timeline = instance.getCheckpointTimeline();
        return timeline.length > 0 ? timeline[0] : 0;
    }

    /**
     * What the view's last out-of-order repair decided, rendered as
     * {@code disposition/denial} - the two fields {@code live_views()} reports as
     * {@code checkpoint_repair_last_disposition} and {@code checkpoint_repair_last_denial}.
     * A repair that read exactly its localized interval carries no denial and prints the
     * disposition alone; {@code none} is a batch whose commits were all in order.
     */
    private static String repairName(LiveViewInstance instance) {
        final String disposition = LiveViewCheckpointRepairPlan.dispositionName(
                instance.getCheckpointRepairLastDisposition(),
                instance.getCheckpointRepairLastDenialReason()
        );
        if (disposition == null) {
            return "none";
        }
        final String denial = LiveViewCheckpointRepairPlan.denialReasonName(instance.getCheckpointRepairLastDenialReason());
        return denial == null ? disposition : disposition + "/" + denial;
    }

    /**
     * What the run wrote into live-view partitions, and how much of that was rewrite
     * rather than append.
     * <p>
     * {@code rows} is the net row count the view gained and {@code phys_rows} what the
     * apply physically wrote for it, so {@code amplification} is live-view rows written per
     * live-view row ingested. The two agree while every commit appends to the head
     * partition. They diverge as soon as a repair publishes a replacement range that
     * reaches a partition already on disk: {@code O3PartitionJob} splits a partition only
     * when the untouched prefix is both absolutely and relatively large, and corrected
     * output for a repair's key domain is spread across the whole range by construction,
     * so the merge covers the partition and every row of it is written again.
     * <p>
     * {@code row_bytes} is the view's fixed column width read off its metadata, so
     * {@code rewritten_mb} is what those physical rows actually cost in column data. It is
     * deliberately not the view's directory size: that also holds the checkpoint store and
     * every partition copy an O3 merge left behind for a purge job this benchmark does not
     * run, and dividing it by the row count would inflate the width several times over. A
     * view carrying a variable-width column reports {@code row_bytes=0} and no bytes, since
     * no single width describes it.
     */
    private static void reportLiveViewWrites(
            CairoEngine engine,
            TableToken lvToken,
            long rows,
            long physRows,
            long applyMicros
    ) {
        final long lvRows;
        try (TableReader reader = engine.getReader(lvToken)) {
            lvRows = reader.size();
        }
        long rowBytes = 0;
        try (TableMetadata lvMeta = engine.getTableMetadata(lvToken)) {
            for (int i = 0, n = lvMeta.getColumnCount(); i < n; i++) {
                final int type = lvMeta.getColumnType(i);
                if (ColumnType.isVarSize(type)) {
                    rowBytes = 0;
                    break;
                }
                rowBytes += ColumnType.sizeOf(type);
            }
        }
        System.out.printf(
                Locale.ROOT,
                "# lv_writes rows=%d phys_rows=%d amplification=%.2fx apply_ms=%.3f apply_rows_per_sec=%.0f "
                        + "lv_rows=%d row_bytes=%d rewritten_mb=%.1f%n",
                rows,
                physRows,
                rows > 0 ? (double) physRows / rows : 0.0,
                applyMicros / 1e3,
                applyMicros > 0 ? physRows / (applyMicros / 1e6) : 0.0,
                lvRows,
                rowBytes,
                physRows * (double) rowBytes / (1024.0 * 1024.0)
        );
    }

    /**
     * What the frontier sweep cost the run. {@code seal_after_sweep_ms} is the mean seal
     * in a batch that swept and {@code seal_no_sweep_ms} the mean seal in one that did
     * not - the two numbers the removal-recording change has to move, since a sweep
     * currently demotes the next seal to a full scan of the entire live state. Each
     * carries its sample count, because a run that swept once compares a single seal
     * against many.
     */
    private static void reportSweeps(
            LiveViewInstance instance,
            double sweptSealMs,
            int sweptSealCount,
            double unsweptSealMs,
            int unsweptSealCount
    ) {
        final LiveViewWindow window = instance.getAnchorWindow();
        if (window == null) {
            System.out.println("# sweeps none - the view carries no anchored window");
            return;
        }
        System.out.printf(
                Locale.ROOT,
                "# sweeps count=%d evicted=%d sweep_ms=%.3f last_sweep_map_rows=%d map_rows=%d "
                        + "seal_after_sweep_ms=%.3f/%d seal_no_sweep_ms=%.3f/%d%n",
                window.getCompactionCount(),
                window.getCompactedPartitionCount(),
                window.getCompactionMicros() / 1e3,
                window.getLastCompactionMapSize(),
                window.getAnchorMapSize(),
                sweptSealCount > 0 ? sweptSealMs / sweptSealCount : 0.0,
                sweptSealCount,
                unsweptSealCount > 0 ? unsweptSealMs / unsweptSealCount : 0.0,
                unsweptSealCount
        );
        reportWindowState(window);
    }

    /**
     * The fused group's shape and the {@link io.questdb.cairo.map.Map} implementation it
     * landed on.
     * <p>
     * The implementation is the line to watch when the partition key is an INT: fusing the
     * accumulators into the window's value takes {@code keySize + valueSize} past
     * {@code cairo.sql.unordered.map.max.entry.size} and moves the map from
     * {@code Unordered4Map} to {@code OrderedMap}, trading a probe on the fastest shape
     * for one on a slower one - against four fewer probes per row. A LONG key was already
     * past the limit before fusing and a SYMBOL or STRING key was never eligible, so this
     * only ever moves for the INT-keyed control.
     */
    private static void reportWindowState(LiveViewWindow window) {
        final LiveViewWindowStatePlan plan = window.getCheckpointWindowStatePlan();
        // The state-root kind and the root counts are the per-seal fixed cost the fusion is
        // about: a window root replaces the anchor root and every durable projection's root
        // at once, so a view whose whole SELECT list fuses publishes two metadata files per
        // seal - window root plus checkpoint root - where it used to publish one per
        // function on top of those two. The meta_segs column is where that shows up as a
        // number.
        //
        // Two kinds of function still publish a root of their own and they cost the same
        // per seal, so both are reported: a residual, which owns a map and a probe per row
        // as well, and a runtime-only member, which is in the group's one map and only its
        // bytes are separate.
        System.out.printf(
                Locale.ROOT,
                "# window_state map=%s state_root=%s components=%d durable_components=%d projections=%d "
                        + "entry_state_bytes=%d runtime_only_members=%s residual_functions=%s%n",
                window.getAnchorMapImplementation(),
                plan == null ? "anchor" : "window",
                plan == null ? 0 : plan.getComponentCount(),
                plan == null ? 0 : plan.getDurableComponentCount(),
                plan == null ? 0 : plan.getProjectionCount(),
                plan == null ? Long.BYTES : plan.getTotalInlineStateBytes(),
                plan == null ? "n/a" : Integer.toString(runtimeOnlyMembers(plan)),
                plan == null ? "n/a" : Integer.toString(plan.getResidualFunctions().size())
        );
    }

    /**
     * How many of the plan's projections are grouped in the window's map but persist on a
     * root of their own - the components the leaf budget left out of the manifest.
     */
    private static int runtimeOnlyMembers(LiveViewWindowStatePlan plan) {
        int count = 0;
        for (int i = 0, n = plan.getProjectionCount(); i < n; i++) {
            if (!plan.isDurableProjection(i)) {
                count++;
            }
        }
        return count;
    }

    /**
     * What the view costs to keep, once it has stopped growing. A CACHE output
     * SYMBOL uses native memory for the writer's value-to-key map and heap only
     * for values that readers resolve and retain. The off-heap total also
     * includes the window state and WAL symbol maps; the on-disk numbers separate
     * the view's own symbol dictionary from its column data.
     * <p>
     * Heap is read after a best-effort collection, which is advisory rather than
     * exact. Run with a fixed -Xmx and compare two builds on the same host.
     */
    private static void reportFootprint(CairoEngine engine, Path dbRoot, boolean isSymbolKey) throws IOException {
        for (int i = 0; i < 4; i++) {
            System.gc();
            try {
                Thread.sleep(150);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        final Runtime runtime = Runtime.getRuntime();
        final long heapBytes = runtime.totalMemory() - runtime.freeMemory();
        final TableToken viewToken = engine.getTableTokenIfExists(VIEW_NAME);
        final Path viewPath = dbRoot.resolve(viewToken.getDirName());
        int symbolCapacity = -1;
        boolean isSymbolCached = false;
        try (TableMetadata viewMeta = engine.getTableMetadata(viewToken)) {
            for (int i = 0, n = viewMeta.getColumnCount(); i < n; i++) {
                if (ColumnType.isSymbol(viewMeta.getColumnType(i))) {
                    symbolCapacity = viewMeta.getColumnMetadata(i).getSymbolCapacity();
                    isSymbolCached = viewMeta.getColumnMetadata(i).isSymbolCacheFlag();
                    break;
                }
            }
        }
        System.out.printf(
                Locale.ROOT,
                "# footprint heap_mb=%.1f offheap_mb=%.1f view_symbol_mb=%.1f view_total_mb=%.1f "
                        + "view_symbol_capacity=%d view_symbol_cached=%s%n",
                heapBytes / (1024.0 * 1024.0),
                Unsafe.getMemUsed() / (1024.0 * 1024.0),
                isSymbolKey ? dirBytes(viewPath, "account_id.") / (1024.0 * 1024.0) : 0.0,
                dirBytes(viewPath, null) / (1024.0 * 1024.0),
                symbolCapacity,
                isSymbolCached
        );
    }

    /**
     * Sums the size of every file under {@code dir}, optionally restricted to file
     * names starting with {@code namePrefix}. The symbol dictionary of a column
     * lives in {@code <column>.o/.c/.k/.v} at the table root, so the prefix picks
     * exactly it out of the table's own directory - partition subdirectories hold
     * the column data and are counted only by the unrestricted call.
     */
    private static long dirBytes(Path dir, String namePrefix) throws IOException {
        if (!Files.exists(dir)) {
            return 0;
        }
        final long[] total = new long[1];
        Files.walkFileTree(dir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (namePrefix == null || file.getFileName().toString().startsWith(namePrefix)) {
                    total[0] += attrs.size();
                }
                return FileVisitResult.CONTINUE;
            }
        });
        return total[0];
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        Files.walkFileTree(dir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult postVisitDirectory(Path d, IOException exc) throws IOException {
                Files.delete(d);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /**
     * Runs the refresh to quiescence.
     * <p>
     * The {@code drainWal} inside the loop is the base table's, not the view's:
     * {@code ApplyWal2TableJob.doRun} skips live-view tokens, so the view's own apply runs
     * inline inside {@code job.run()} and only {@link LiveViewRefreshJob#getLiveViewApplyMicros()}
     * separates it from the replay it shares a thread with. That apply is the half of a
     * refresh fix 3 turns on: it is where corrected output meets live-view partitions that
     * already exist, and where a repair reaching a closed partition rewrites the whole of it.
     */
    /**
     * Drives the view to quiescence and reports the longest single worker pass it took.
     * <p>
     * The total is what {@code refresh_ms} carries; this is the other half of the same
     * question. A repair that may not park holds the worker for its whole replay, so the
     * longest pass <b>is</b> the repair; one that parks on the turn budget spreads the same
     * replay over passes, so the longest pass is bounded by that budget while the total
     * barely moves. One pass is one {@code Job.run()}, which on this single-view benchmark
     * is one refresh turn plus the drive of whatever it parked.
     */
    private static long drainLiveView(CairoEngine engine, LiveViewInstance instance, LiveViewRefreshJob job) {
        instance.setLastFlushTimeUs(Numbers.LONG_NULL);
        long maxPassNanos = 0;
        for (int i = 0; i < 4_096; i++) {
            boolean isProgressed = false;
            for (; ; ) {
                final long passStart = System.nanoTime();
                final boolean isRun = job.run();
                if (!isRun) {
                    break;
                }
                maxPassNanos = Math.max(maxPassNanos, System.nanoTime() - passStart);
                isProgressed = true;
            }
            drainWal(engine);
            if (!isProgressed) {
                return maxPassNanos;
            }
            instance.setLastFlushTimeUs(Numbers.LONG_NULL);
        }
        throw new IllegalStateException("live view did not quiesce");
    }

    private static void drainWal(CairoEngine engine) {
        try (ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 0)) {
            while (walApplyJob.run()) {
                // drain
            }
            if (new CheckWalTransactionsJob(engine).run()) {
                while (walApplyJob.run()) {
                    // drain
                }
            }
        }
    }

    /**
     * One commit's worth of rows, as the {@code shape} describes them. {@code o3EveryN}
     * sprinkles late arrivals through the otherwise forward stream: every
     * {@code o3EveryN}-th row is stamped {@code o3LagMicros} below its own position, so
     * the commit's own minimum sits below the previous commit's maximum and the refresh
     * classifies it as out of order. The rest of the commit is where it would have been,
     * which is what an ingestion pipeline emitting mostly fresh rows beside a minority of
     * late ones looks like. A row is only moved when its position is far enough above
     * {@link #START_TS} for the deepest shift to stay inside the run, so nothing lands
     * below the view's own lower bound.
     * <p>
     * {@code o3SpreadSteps} decides how many distinct anchor segments those late rows
     * reach: consecutive late rows step down by one further whole anchor period, cycling
     * through the steps, so one commit corrects several segments at once. That is the
     * shape the production logs show and the one a per-segment repair is priced against;
     * a single step leaves every late row in the same segment.
     * <p>
     * A {@code ladder} replaces both: a late row then takes its depth from the weighted
     * set of depths {@code --o3-depths} names, so one run carries the seconds-late
     * majority, the minutes-late minority and the day-deep tail a real stream mixes,
     * rather than one depth at a time.
     */
    private static String insertSql(
            RowShape shape,
            long firstRow,
            long rows,
            long o3EveryN,
            long o3LagMicros,
            int o3SpreadSteps,
            boolean isLadderDepth
    ) {
        // The row's ordinal in the whole generated stream. Every other expression is a
        // function of it, so a row that repeats an earlier one repeats it here and comes
        // out identical in both timestamp and key.
        final String rowIndex = "(x + " + (firstRow - 1) + ")";
        // An equal-timestamp row is the row below it, generated again: same position, same
        // account. Both have to move together, since a (timestamp, key) group is what an
        // ordinal would have to number - two rows sharing only the timestamp are separate
        // groups and need no ordinal at all.
        final String twinIndex = shape.equalTsEveryN > 0
                ? "(case when " + rowIndex + " % " + shape.equalTsEveryN + " = 0 and " + rowIndex + " > 0 then "
                  + rowIndex + " - 1 else " + rowIndex + " end)"
                : rowIndex;
        final String acct = shape.accountExpression(twinIndex);
        final String amountExpr = "(" + rowIndex + " % 2001 - 1000) * 0.01";
        // A NULL amount is what separates the two counters a fused group might otherwise
        // look equivalent on: sum(amount) skips the row and count(account_id) does not.
        final String nullableAmountExpr = shape.nullPercent > 0
                ? "case when " + rowIndex + " % 100 < " + shape.nullPercent + " then null::double else " + amountExpr + " end"
                : amountExpr;
        final String position = "(" + START_TS + " + " + twinIndex + " * " + shape.tsStepMicros + ")";
        final long maxLagMicros = o3LagMicros + (long) (o3SpreadSteps - 1) * shape.anchorPeriodMicros;
        final String lag;
        if (o3SpreadSteps > 1) {
            lag = "(" + o3LagMicros + " + (" + rowIndex + " / " + o3EveryN + ") % " + o3SpreadSteps
                    + " * " + shape.anchorPeriodMicros + ")";
        } else {
            lag = Long.toString(o3LagMicros);
        }
        // The single-depth form guards the whole run by its one depth: no row moves until
        // the stream is deeper than that. A ladder cannot, because its deepest rung would
        // then hold back the shallow corrections that make up most of the workload, so it
        // guards per row instead and leaves a row that would land below START_TS where it
        // was. Either way nothing lands below the view's own lower bound.
        final String lateGate = isLadderDepth
                ? position + " - " + lag + " >= " + START_TS
                : twinIndex + " > " + maxLagMicros / shape.tsStepMicros;
        final String timestamp = o3EveryN > 0
                ? "case when " + rowIndex + " % " + o3EveryN + " = 0 and " + lateGate
                  + " then " + position + " - " + lag + " else " + position + " end"
                : position;
        final StringBuilder sql = new StringBuilder("insert into payments ")
                .append("select (").append(timestamp).append(")::timestamp, ")
                .append(acct).append(", ")
                .append(nullableAmountExpr);
        for (int i = 1; i <= shape.sumColumns; i++) {
            sql.append(", (").append(rowIndex).append(" % ").append(2000 + i).append(") * 0.01");
        }
        return sql.append(" from long_sequence(").append(rows).append(')').toString();
    }

    /**
     * The extra {@code q1..qN} DOUBLE columns the width sweep sums over. Each one is a
     * distinct argument and therefore a distinct 16-byte component, so {@code N} moves
     * the fused entry's width by 16 bytes a step with nothing else about the view
     * changing - which is what makes the leaf-budget question measurable rather than
     * arguable.
     */
    private static String sumColumnDdl(int sumColumns) {
        final StringBuilder ddl = new StringBuilder();
        for (int i = 1; i <= sumColumns; i++) {
            ddl.append(", q").append(i).append(" double");
        }
        return ddl.toString();
    }

    private static int symbolCapacity(long rows) {
        long capacity = 16;
        while (capacity < rows && capacity < (1L << 30)) {
            capacity <<= 1;
        }
        return (int) Math.min(capacity, 1L << 30);
    }

    /**
     * The published checkpoint segments, sampled from the view's own
     * {@code _checkpoints} directory between batches.
     * <p>
     * The two counts are the per-seal fixed cost that does not scale with the dirty set,
     * and they are what removing a root actually removes: a seal publishes one metadata
     * file per state root plus one for the checkpoint root, each preceded by a probe and
     * followed by an mmap, a sync and a rename. On a small incremental dirty set that
     * cost dominates the per-entry bytes entirely.
     * <p>
     * A segment id only ever increases, so "published since the last sample" is counted
     * as the files whose id is above the highest id seen then - exact even when the same
     * batch also retired older segments, which a plain file-count delta would net out.
     * The retained figures are the whole directory as it stands.
     */
    private static final class CheckpointSegments {
        private final Path dataDir;
        private final Path metaDir;
        private long addedDataBytes;
        private long addedDataSegments;
        private long addedMetaBytes;
        private long addedMetaSegments;
        private long maxDataSegmentId = -1;
        private long maxMetaSegmentId = -1;
        private long retainedDataBytes;
        private long retainedDataSegments;
        private long retainedMetaBytes;
        private long retainedMetaSegments;

        CheckpointSegments(Path viewDir) {
            final Path checkpoints = viewDir.resolve(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
            metaDir = checkpoints.resolve(LiveViewCheckpointLayout.META_DIR_NAME);
            dataDir = checkpoints.resolve(LiveViewCheckpointLayout.DATA_DIR_NAME);
        }

        long getAddedDataBytes() {
            return addedDataBytes;
        }

        long getAddedDataSegments() {
            return addedDataSegments;
        }

        long getAddedMetaBytes() {
            return addedMetaBytes;
        }

        long getAddedMetaSegments() {
            return addedMetaSegments;
        }

        void report() {
            System.out.printf(
                    Locale.ROOT,
                    "# segments retained_meta=%d retained_meta_bytes=%d retained_data=%d retained_data_bytes=%d%n",
                    retainedMetaSegments,
                    retainedMetaBytes,
                    retainedDataSegments,
                    retainedDataBytes
            );
        }

        void sample() throws IOException {
            final long[] meta = scan(metaDir, maxMetaSegmentId);
            addedMetaSegments = meta[0];
            addedMetaBytes = meta[1];
            retainedMetaSegments = meta[2];
            retainedMetaBytes = meta[3];
            maxMetaSegmentId = meta[4];
            final long[] data = scan(dataDir, maxDataSegmentId);
            addedDataSegments = data[0];
            addedDataBytes = data[1];
            retainedDataSegments = data[2];
            retainedDataBytes = data[3];
            maxDataSegmentId = data[4];
        }

        /**
         * Returns {@code {addedCount, addedBytes, retainedCount, retainedBytes, maxId}}
         * for one segment directory. A {@code .tmp} name is a staged file the publish has
         * not renamed yet and is not a segment.
         */
        private static long[] scan(Path dir, long previousMaxId) throws IOException {
            final long[] out = {0, 0, 0, 0, previousMaxId};
            if (!Files.exists(dir)) {
                return out;
            }
            Files.walkFileTree(dir, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    final String name = file.getFileName().toString();
                    final int dot = name.indexOf('.');
                    if (dot < 0 || name.endsWith(".tmp")) {
                        return FileVisitResult.CONTINUE;
                    }
                    final long id;
                    try {
                        id = Long.parseLong(name.substring(dot + 1));
                    } catch (NumberFormatException e) {
                        return FileVisitResult.CONTINUE;
                    }
                    out[2]++;
                    out[3] += attrs.size();
                    if (id > previousMaxId) {
                        out[0]++;
                        out[1] += attrs.size();
                    }
                    out[4] = Math.max(out[4], id);
                    return FileVisitResult.CONTINUE;
                }
            });
            return out;
        }
    }

    /**
     * A weighted ladder of out-of-order correction depths, as
     * {@code --o3-depths=<duration>:<weight>,...} names them - for instance
     * {@code 1s:40,5s:47,3m:5,3h:1,36h:1}. A late row picks its rung from its own ordinal
     * in the stream, so the mix is exact and reproducible rather than sampled.
     * <p>
     * It exists because a real out-of-order stream is not one depth. The reported
     * workload is roughly half in-order commits, half within ten seconds, and a tail of a
     * few per cent reaching minutes, hours and days back - and those three cost entirely
     * different things: a seconds-deep correction resumes from the anchor inside the open
     * segment, an hours-deep one may cross into a closed segment, and a day-deep one
     * repairs a segment the view has long since sealed. A run carrying one depth at a time
     * prices one of those and says nothing about how they compose.
     * <p>
     * Weights are relative and need not sum to a hundred.
     */
    private static final class O3Depths {
        private final int[] cumulativeWeights;
        private final long[] depthMicros;
        private final String spec;
        private final int totalWeight;

        private O3Depths(String spec, long[] depthMicros, int[] cumulativeWeights, int totalWeight) {
            this.spec = spec;
            this.depthMicros = depthMicros;
            this.cumulativeWeights = cumulativeWeights;
            this.totalWeight = totalWeight;
        }

        /**
         * What the header line reports the run used.
         */
        String describe() {
            return spec;
        }

        /**
         * The depth one eligible commit's late rows carry, picked by that commit's ordinal
         * among the eligible ones. Walking the rungs in weight order rather than sampling
         * them makes the mix exact over any run long enough to cover the ladder once.
         */
        long depthMicrosFor(long commitOrdinal) {
            final long rung = Math.floorMod(commitOrdinal, totalWeight);
            for (int i = 0, n = depthMicros.length; i < n; i++) {
                if (rung < cumulativeWeights[i]) {
                    return depthMicros[i];
                }
            }
            return depthMicros[depthMicros.length - 1];
        }

        static O3Depths of(String spec) {
            final String[] rungs = spec.split(",");
            final long[] depths = new long[rungs.length];
            final int[] cumulative = new int[rungs.length];
            int total = 0;
            for (int i = 0; i < rungs.length; i++) {
                final int colon = rungs[i].indexOf(':');
                if (colon < 1) {
                    throw new IllegalArgumentException("--o3-depths rung must be <duration>:<weight>: " + rungs[i]);
                }
                final int weight = Integer.parseInt(rungs[i].substring(colon + 1).trim());
                if (weight < 1) {
                    throw new IllegalArgumentException("--o3-depths weight must be positive: " + rungs[i]);
                }
                depths[i] = anchorPeriodMicros(rungs[i].substring(0, colon).trim());
                total += weight;
                cumulative[i] = total;
            }
            return new O3Depths(spec, depths, cumulative, total);
        }
    }

    /**
     * Everything about a generated row that does not change between commits: how the
     * timestamp axis is spaced, how a row picks its account, and the two distribution
     * knobs Stage 0 of fix 3 needs. One instance serves the seed, every batch and the
     * restart probe, so the three can never drift apart.
     */
    private static final class RowShape {
        private final long accountWindow;
        private final long anchorOffsetMicros;
        private final long anchorPeriodMicros;
        private final long equalTsEveryN;
        private final long hotKeyEveryN;
        private final KeyType keyType;
        private final int nullPercent;
        private final int recycleAccounts;
        private final int sumColumns;
        private final long tsStepMicros;

        private RowShape(
                int recycleAccounts,
                long accountWindow,
                long anchorPeriodMicros,
                long anchorOffsetMicros,
                KeyType keyType,
                int nullPercent,
                int sumColumns,
                long tsStepMicros,
                long hotKeyEveryN,
                long equalTsEveryN
        ) {
            this.recycleAccounts = recycleAccounts;
            this.accountWindow = accountWindow;
            this.anchorPeriodMicros = anchorPeriodMicros;
            this.anchorOffsetMicros = anchorOffsetMicros;
            this.keyType = keyType;
            this.nullPercent = nullPercent;
            this.sumColumns = sumColumns;
            this.tsStepMicros = tsStepMicros;
            this.hotKeyEveryN = hotKeyEveryN;
            this.equalTsEveryN = equalTsEveryN;
        }

        /**
         * The SQL that picks a row's account, given the SQL expression for its ordinal in
         * the stream. Three shapes:
         * <ul>
         *     <li>default - a brand-new account per row, so nothing recurs;</li>
         *     <li>{@code --recycle-accounts=N} - N accounts round-robin, so every account
         *     recurs forever and none ever falls behind the frontier;</li>
         *     <li>{@code --account-window=N} - N accounts alive per anchor bucket, the
         *     window sliding forward by half its width each bucket. Half a bucket's
         *     accounts therefore recur from the previous bucket and half are new, and the
         *     half left behind ages out two buckets later, which is what makes the sweep
         *     fire. When a bucket holds fewer rows than the window, a bucket touches only
         *     as many accounts as it has rows - the {@code rowsPerBucket} header field
         *     is what tells the two apart.</li>
         * </ul>
         * {@code --hot-key-percent} then folds a share of the rows onto account zero
         * whichever shape picked them, so one posting list holds that share of the base.
         */
        private String accountExpression(String rowIndex) {
            final String accountId;
            if (accountWindow > 0) {
                final long slide = Math.max(1, accountWindow / 2);
                final String bucket = "((" + START_TS + " + " + rowIndex + " * " + tsStepMicros
                        + " - " + anchorOffsetMicros + ") / " + anchorPeriodMicros + ")";
                accountId = "(" + bucket + " * " + slide + " + " + rowIndex + " % " + accountWindow + ")";
            } else if (recycleAccounts > 0) {
                accountId = "(" + rowIndex + " % " + recycleAccounts + ")";
            } else {
                accountId = rowIndex;
            }
            final String hotOrNot = hotKeyEveryN > 0
                    ? "(case when " + rowIndex + " % " + hotKeyEveryN + " = 0 then 0 else " + accountId + " end)"
                    : accountId;
            return keyType.accountExpression(hotOrNot);
        }
    }

    /**
     * The partition key's column type, and with it the runtime map shape the fused
     * window value lands on.
     * <p>
     * {@code MapFactory.createUnorderedMap} selects {@code Unordered4Map} or
     * {@code Unordered8Map} only while {@code keySize + valueSize} fits
     * {@code cairo.sql.unordered.map.max.entry.size}, default 16. An INT key with the
     * anchor-only 10-byte value sits at 14 and moves to {@code OrderedMap} at 26 once a
     * 16-byte accumulator joins it; a LONG key was already past the limit at 18 before
     * fusing, and a SYMBOL key was never eligible. The INT control is therefore the only
     * run that can show the transition, and it is why it exists.
     */
    private enum KeyType {
        INT("int"),
        LONG("long"),
        SYMBOL("symbol");

        private final String name;

        KeyType(String name) {
            this.name = name;
        }

        static KeyType of(String name) {
            for (KeyType keyType : values()) {
                if (keyType.name.equals(name)) {
                    return keyType;
                }
            }
            throw new IllegalArgumentException("--key-type must be one of symbol, int, long: " + name);
        }

        String accountExpression(String accountId) {
            return switch (this) {
                case SYMBOL -> "'acct-' || " + accountId + "::string";
                case INT -> accountId + "::int";
                case LONG -> accountId + "::long";
            };
        }

        String columnDdl(String capacity, String indexClause) {
            return this == SYMBOL ? "symbol" + capacity + " nocache" + indexClause : name;
        }
    }

    /**
     * The SELECT list a run measures. Each shape is one row of the acceptance plan: what
     * the fused group is made of decides how many components the entry carries, how many
     * of them several projections share, and which functions stay on a legacy root.
     */
    private enum Shape {
        /**
         * Four dispersion calls plus a {@code count} that folds onto their counter - one
         * 24-byte Welford component serving five outputs.
         */
        DISPERSION("dispersion"),
        /**
         * A fused group beside a bounded ROWS call, which keeps a ring-backed root of its
         * own. The mixed shape the acceptance plan asks for.
         */
        MIXED("mixed"),
        /**
         * {@code count(*)} and {@code row_number()} over one row-count component.
         */
        ROW_COUNT("row-count"),
        /**
         * The single-{@code count} control.
         */
        SINGLE_COUNT("count"),
        /**
         * The single-{@code sum} control, and the one to read beside the INT key type.
         */
        SINGLE_SUM("sum"),
        /**
         * The same single {@code sum} over an expression rather than a column reference,
         * which the plan declines for want of an argument key. It is the unfused control
         * to read {@link #SINGLE_SUM} against: same arithmetic, same rows, one function,
         * and the only difference is whether the window owns the state or the function
         * does.
         */
        SINGLE_SUM_UNFUSED("sum-unfused"),
        /**
         * Three projections onto one {@code (sum, nonNullCount)} component: the shape the
         * cross-family derivation exists for.
         */
        SUM_AVG_COUNT("sum-avg-count"),
        /**
         * The reported workload: a {@code sum} over the amount beside a {@code count} over
         * the key. Their arguments differ, so the two counters never merge and the entry
         * carries two components.
         */
        TARGET("target");

        private final String name;

        Shape(String name) {
            this.name = name;
        }

        static Shape of(String name) {
            for (Shape shape : values()) {
                if (shape.name.equals(name)) {
                    return shape;
                }
            }
            throw new IllegalArgumentException("unknown --shape: " + name);
        }

        String extraWindows() {
            return this == MIXED
                    ? ", r as (partition by account_id order by created_at rows between 63 preceding and current row)"
                    : "";
        }

        String projections(int sumColumns) {
            final StringBuilder select = new StringBuilder(switch (this) {
                case DISPERSION -> "stddev_samp(amount) over w as ss, stddev_pop(amount) over w as sp, "
                        + "var_samp(amount) over w as vs, var_pop(amount) over w as vp, "
                        + "count(amount) over w as c";
                case MIXED -> "sum(amount) over w as cumulative_sum, sum(amount) over r as bounded_sum";
                case ROW_COUNT -> "count(*) over w as n, row_number() over w as rn";
                case SINGLE_COUNT -> "count(account_id) over w as cumulative_count";
                case SINGLE_SUM -> "sum(amount) over w as cumulative_sum";
                case SINGLE_SUM_UNFUSED -> "sum(amount + 0.0) over w as cumulative_sum";
                case SUM_AVG_COUNT -> "sum(amount) over w as s, avg(amount) over w as a, "
                        + "count(amount) over w as c";
                case TARGET -> "sum(amount) over w as cumulative_sum, count(account_id) over w as cumulative_count";
            });
            for (int i = 1; i <= sumColumns; i++) {
                select.append(", sum(q").append(i).append(") over w as qs").append(i);
            }
            return select.toString();
        }
    }

    /**
     * Samples one memory tag's native usage on a side thread and keeps the highest reading
     * since the last {@link #reset()}. A refresh allocates and frees within {@code job.run()}
     * - the WAL reader's symbol maps go back at the end of every drain - so nothing a caller
     * reads before or after the call can see that peak, and a sampler is the only way to put
     * a number on it. Sampling every millisecond costs nothing next to a refresh measured in
     * hundreds, and the peaks this exists to catch last far longer than one interval.
     */
    private static final class NativeTagPeakSampler extends Thread implements QuietCloseable {
        private final int memoryTag;
        private volatile long base;
        private volatile long peak;
        private volatile boolean running = true;

        NativeTagPeakSampler(int memoryTag) {
            super("lv-native-peak-sampler");
            this.memoryTag = memoryTag;
            setDaemon(true);
            reset();
            start();
        }

        @Override
        public void close() {
            running = false;
        }

        /**
         * The highest reading since the last reset, less the resting level that reset
         * recorded - so retained state does not count towards a batch's transient peak.
         */
        public long peakAboveBaseBytes() {
            return Math.max(0, peak - base);
        }

        public void reset() {
            final long used = Unsafe.getMemUsedByTag(memoryTag);
            base = used;
            peak = used;
        }

        @Override
        public void run() {
            while (running) {
                final long used = Unsafe.getMemUsedByTag(memoryTag);
                if (used > peak) {
                    peak = used;
                }
                Os.sleep(1);
            }
        }
    }
}

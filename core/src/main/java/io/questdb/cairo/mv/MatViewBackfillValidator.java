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

package io.questdb.cairo.mv;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.wal.WalPreCommitValidator;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.Nullable;

/**
 * Enforces the bucket-whole rule on direct user writes into a materialized view.
 * <p>
 * A backfill txn is accepted only if the bucket containing its max-row timestamp
 * ends at-or-before the effective frozen-zone boundary's bucket floor. The
 * effective boundary is the minimum of: (1) what this commit's own snapshot of
 * {@code min(max(base_ts), wallClock) - LIMIT} produces, and (2) the snapshot
 * the most recent refresh tick published via
 * {@link MatViewState#setLastRefreshFrozenBoundaryAnchor(long)}. Taking the
 * minimum keeps the accepted ceiling strictly below the lo of any in-flight
 * refresh's REPLACE_RANGE coverage, so refresh never wipes a row this validator
 * just accepted.
 * <p>
 * Refresh-job writes carry {@link WalTxnType#MAT_VIEW_DATA} or
 * {@link WalTxnType#DATA} with {@link WalUtils#WAL_DEDUP_MODE_REPLACE_RANGE} and
 * are passed through untouched; only generic {@link WalTxnType#DATA} with the
 * default dedup mode -- user INSERT/COPY/ILP/QWP -- is checked.
 * <p>
 * When {@code REFRESH LIMIT == 0} the validator rejects user DATA writes
 * outright. The entry-point gate ({@code engine.isBackfillableMatView}) already
 * does the same for callers that consult it; rejecting here closes the gap for
 * cached ILP/QWP {@code TableUpdateDetails} that outlive an
 * {@code ALTER MATERIALIZED VIEW ... SET REFRESH LIMIT 0}.
 */
public final class MatViewBackfillValidator implements WalPreCommitValidator {
    private static final Log LOG = LogFactory.getLog(MatViewBackfillValidator.class);
    private final CairoEngine engine;
    private final TableToken matViewToken;
    // Sampler is lazily allocated on first validated commit and reused across
    // subsequent commits on this writer. Sample interval and unit are fixed at
    // mat-view creation (changing either requires DROP + recreate), so the
    // cached sampler stays valid for the writer's lifetime.
    private TimestampSampler cachedSampler;
    // Per-writer cache to skip the base reader open on commits where the base
    // table writer txn has not advanced since the last validated commit. Reset
    // when the base table is recreated (token instance differs) or when the
    // writer txn moves.
    private long cachedBaseTxn = -1;
    private long cachedMaxBaseTs = Long.MIN_VALUE;
    private TableToken cachedBaseToken;

    public MatViewBackfillValidator(CairoEngine engine, TableToken matViewToken) {
        assert matViewToken.isMatView();
        this.engine = engine;
        this.matViewToken = matViewToken;
    }

    /**
     * Compute the current frozen-zone cutoff for {@code def} as the boundary's
     * bucket floor (in the base table's timestamp driver units). Returns
     * {@link Numbers#LONG_NULL} when no cutoff is meaningful: the view has no
     * {@code REFRESH LIMIT} set (no frozen zone exists), the wall-clock
     * escape-hatch config is on (the whole frozen-zone feature is off so the
     * entry-point gate would reject backfill anyway), or the sampler cannot be
     * reconstructed.
     * <p>
     * When a cutoff is returned, it matches the validator's commit-time check:
     * {@code min(own anchor, published anchor) - LIMIT}, snapped to the
     * sampler's bucket floor. {@code state} carries the published anchor when
     * the engine has the view in memory; when {@code state} is null (transient
     * race with create/drop) the helper falls back to its own snapshot.
     * <p>
     * Allocates a sampler per call and opens the base reader once per call.
     * Callers iterating many views (e.g. the {@code materialized_views()}
     * cursor) should use {@link #createSampler(MatViewDefinition)} once and
     * pass the reused sampler in via the four-arg overload.
     */
    public static long computeFrozenBoundaryBucketFloor(
            CairoEngine engine,
            MatViewDefinition def,
            @Nullable MatViewState state
    ) {
        final TimestampSampler sampler = createSampler(def);
        if (sampler == null) {
            return Numbers.LONG_NULL;
        }
        return computeFrozenBoundaryBucketFloor(engine, def, state, sampler);
    }

    /**
     * Overload that reuses a caller-managed sampler. Caller is responsible for
     * matching the sampler to {@code def}'s sampling interval and unit (one
     * sampler per unique (driver, interval, unit) tuple) and may pass the same
     * sampler across many calls.
     */
    public static long computeFrozenBoundaryBucketFloor(
            CairoEngine engine,
            MatViewDefinition def,
            @Nullable MatViewState state,
            TimestampSampler sampler
    ) {
        if (def.getRefreshLimitHoursOrMonths() == 0) {
            return Numbers.LONG_NULL;
        }
        if (engine.getConfiguration().isMatViewRefreshLimitWallClockEnabled()) {
            // Escape-hatch on: entire frozen-zone feature is meant to be off.
            // Surface no cutoff so the gate and the metadata stay consistent.
            return Numbers.LONG_NULL;
        }
        sampler.setOffset(def.getFixedOffset());
        final long ownMaxBaseTs = readBaseMaxTimestamp(engine, def);
        return computeBoundaryBucketFloor(engine, def, sampler, state, ownMaxBaseTs);
    }

    /**
     * Build a {@link TimestampSampler} matching {@code def}'s sampling
     * interval and unit. Returns {@code null} if the stored sampler params
     * cannot be reconstructed (corruption).
     */
    public static @Nullable TimestampSampler createSampler(MatViewDefinition def) {
        try {
            return TimestampSamplerFactory.getInstance(
                    def.getBaseTableTimestampDriver(),
                    def.getSamplingInterval(),
                    def.getSamplingIntervalUnit(),
                    0
            );
        } catch (SqlException e) {
            // Stored sampler params are validated at create-time, so a failure
            // here is corruption.
            return null;
        }
    }

    @Override
    public void validate(byte txnType, byte dedupMode, long txnMinTs, long txnMaxTs) {
        // Refresh-job writes carry MAT_VIEW_DATA OR DATA+REPLACE_RANGE and are
        // exempt from the bucket-whole rule by definition (the refresh job owns
        // the managed zone). Only the user-INSERT flavour -- DATA + default dedup
        // -- gets validated here.
        if (txnType != WalTxnType.DATA || dedupMode != WalUtils.WAL_DEDUP_MODE_DEFAULT) {
            return;
        }
        // Empty txn -- nothing to validate.
        if (txnMaxTs < 0) {
            return;
        }

        final MatViewState state = engine.getMatViewStateStore().getViewState(matViewToken);
        if (state == null) {
            // View is being dropped or never finished initialising. The
            // entry-point gate is the authoritative contract; let the txn
            // through and rely on the gate to have caught real backfills.
            return;
        }
        final MatViewDefinition def = state.getViewDefinition();
        if (def == null) {
            return;
        }
        // No REFRESH LIMIT means no frozen zone exists, so a user could not
        // have reached the WAL writer via the entry-point gate
        // (isBackfillableMatView). Low-level WAL-state tests and any future
        // internal write path can still legitimately commit DATA txns through
        // this writer, so let them through -- the entry-point gate remains the
        // contract. Stale ILP/QWP TableUpdateDetails caches that hold a TUD
        // for a mat view whose LIMIT has just been ALTERed to 0 can slip rows
        // through until the cache TTL expires, but the next FULL refresh's
        // truncateSoft will reset the view anyway.
        if (def.getRefreshLimitHoursOrMonths() == 0) {
            return;
        }

        // Sampler is cached on the instance and reused across commits; sample
        // interval/unit cannot change without DROP + recreate. setOffset is
        // idempotent so it is safe to call every time in case the offset ever
        // becomes mutable.
        if (cachedSampler == null) {
            try {
                cachedSampler = TimestampSamplerFactory.getInstance(
                        def.getBaseTableTimestampDriver(),
                        def.getSamplingInterval(),
                        def.getSamplingIntervalUnit(),
                        0
                );
            } catch (SqlException e) {
                // Stored sampler params are validated at create-time; treat a
                // failure here as corruption and let the txn through (refresh
                // would skip it on the same grounds).
                return;
            }
        }
        cachedSampler.setOffset(def.getFixedOffset());

        final long ownMaxBaseTs = readBaseMaxTimestampCached(def);
        final long boundaryBucketFloor = computeBoundaryBucketFloor(engine, def, cachedSampler, state, ownMaxBaseTs);
        if (boundaryBucketFloor == Numbers.LONG_NULL) {
            return;
        }

        final TimestampDriver driver = def.getBaseTableTimestampDriver();
        final long maxRowBucketFloor = cachedSampler.round(txnMaxTs);
        final long maxRowBucketEnd = cachedSampler.nextTimestamp(maxRowBucketFloor);

        if (maxRowBucketEnd > boundaryBucketFloor) {
            throw CairoException.nonCritical()
                    .put("backfill row falls in or past the managed zone of materialized view [view=")
                    .put(matViewToken.getTableName())
                    .put(", maxRowTs=").ts(driver, txnMaxTs)
                    .put(", rowBucket=[").ts(driver, maxRowBucketFloor)
                    .put(", ").ts(driver, maxRowBucketEnd)
                    .put("), boundaryBucketFloor=").ts(driver, boundaryBucketFloor)
                    .put("]; backfill bucket end must be at-or-before the boundary bucket floor");
        }
    }

    /**
     * Snap the effective boundary -- the smaller of the validator's own snapshot
     * anchor and the anchor the last refresh tick published -- through the
     * sampler. Coordinating with refresh's published anchor is what makes the
     * validator safe under an in-flight refresh: refresh's REPLACE_RANGE always
     * runs with the anchor it published, so accepted buckets sitting strictly
     * below that anchor's snapped boundary are never overlapped.
     */
    private static long computeBoundaryBucketFloor(
            CairoEngine engine,
            MatViewDefinition def,
            TimestampSampler sampler,
            @Nullable MatViewState state,
            long ownMaxBaseTs
    ) {
        final int limitHoursOrMonths = def.getRefreshLimitHoursOrMonths();
        if (limitHoursOrMonths == 0) {
            return Numbers.LONG_NULL;
        }
        if (engine.getConfiguration().isMatViewRefreshLimitWallClockEnabled()) {
            return Numbers.LONG_NULL;
        }
        final TimestampDriver driver = def.getBaseTableTimestampDriver();
        final long now = driver.getTicks();
        long boundaryAnchor = ownMaxBaseTs == Long.MIN_VALUE ? now : Math.min(ownMaxBaseTs, now);
        if (state != null) {
            final long publishedAnchor = state.getLastRefreshFrozenBoundaryAnchor();
            if (publishedAnchor != Numbers.LONG_NULL) {
                boundaryAnchor = Math.min(boundaryAnchor, publishedAnchor);
            }
        }
        final long rawBoundary = limitHoursOrMonths > 0
                ? boundaryAnchor - driver.fromHours(limitHoursOrMonths)
                : driver.addMonths(boundaryAnchor, limitHoursOrMonths);
        return sampler.round(rawBoundary);
    }

    /**
     * Open the base reader and read max(base_ts). Returns {@link Long#MIN_VALUE}
     * when the base table is gone or the reader cannot be acquired; the caller
     * then falls back to wall clock. Reader pool failures are logged at advisory
     * level so operators can spot persistent fallbacks.
     */
    private static long readBaseMaxTimestamp(CairoEngine engine, MatViewDefinition def) {
        final TableToken baseTableToken = engine.getTableTokenIfExists(def.getBaseTableName());
        if (baseTableToken == null) {
            return Long.MIN_VALUE;
        }
        try (TableReader reader = engine.getReader(baseTableToken)) {
            return reader.getMaxTimestamp();
        } catch (CairoException | TableReferenceOutOfDateException e) {
            LOG.advisory().$("mat view backfill boundary falling back to wall clock; base reader unavailable [view=")
                    .$(def.getMatViewToken())
                    .$(", base=").$(baseTableToken)
                    .$(", reason=").$safe(e.getMessage())
                    .I$();
            return Long.MIN_VALUE;
        }
    }

    /**
     * Validator-instance variant of {@link #readBaseMaxTimestamp} that skips the
     * reader open when the base table's writer txn has not advanced since the
     * last validated commit on this writer. Cache resets when the base token
     * differs (drop + recreate) or when the writer txn moves.
     */
    private long readBaseMaxTimestampCached(MatViewDefinition def) {
        final TableToken baseTableToken = engine.getTableTokenIfExists(def.getBaseTableName());
        if (baseTableToken == null) {
            cachedBaseToken = null;
            cachedBaseTxn = -1;
            cachedMaxBaseTs = Long.MIN_VALUE;
            return Long.MIN_VALUE;
        }
        final long currentBaseTxn = engine.getTableSequencerAPI().getTxnTracker(baseTableToken).getWriterTxn();
        if (baseTableToken == cachedBaseToken && currentBaseTxn == cachedBaseTxn && cachedMaxBaseTs != Long.MIN_VALUE) {
            return cachedMaxBaseTs;
        }
        try (TableReader reader = engine.getReader(baseTableToken)) {
            final long maxBaseTs = reader.getMaxTimestamp();
            cachedBaseToken = baseTableToken;
            cachedBaseTxn = currentBaseTxn;
            cachedMaxBaseTs = maxBaseTs;
            return maxBaseTs;
        } catch (CairoException | TableReferenceOutOfDateException e) {
            LOG.advisory().$("mat view backfill boundary falling back to wall clock; base reader unavailable [view=")
                    .$(matViewToken)
                    .$(", base=").$(baseTableToken)
                    .$(", reason=").$safe(e.getMessage())
                    .I$();
            cachedBaseToken = null;
            cachedBaseTxn = -1;
            cachedMaxBaseTs = Long.MIN_VALUE;
            return Long.MIN_VALUE;
        }
    }
}

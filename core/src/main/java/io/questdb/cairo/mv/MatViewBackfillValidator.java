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
import io.questdb.cairo.wal.WalCommitPreValidator;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.std.Numbers;

/**
 * Enforces the bucket-whole rule on direct user writes into a materialized view.
 * <p>
 * A backfill txn is accepted only if every row's containing sample-by bucket
 * ends strictly at-or-before the boundary's bucket floor. Since the txn carries
 * its own min/max timestamps, the per-row check collapses to a per-txn check:
 * the bucket containing the txn's max row timestamp must end at-or-before the
 * boundary's bucket floor; if it extends past, the whole txn is rejected.
 * <p>
 * The boundary is computed at commit time using {@code min(max(base_ts), wallClock) - LIMIT},
 * matching {@link MatViewRefreshJob}'s formula. The compile-time boundary cannot
 * be trusted because it keeps moving as base data lands; checking at commit time
 * minimises the race.
 * <p>
 * Refresh-job writes use {@link WalTxnType#MAT_VIEW_DATA} and are passed through
 * untouched; only generic {@link WalTxnType#DATA} writes (user INSERT/COPY/ILP/QWP)
 * are checked.
 */
public final class MatViewBackfillValidator implements WalCommitPreValidator {
    private final CairoEngine engine;
    private final TableToken matViewToken;

    public MatViewBackfillValidator(CairoEngine engine, TableToken matViewToken) {
        assert matViewToken.isMatView();
        this.engine = engine;
        this.matViewToken = matViewToken;
    }

    /**
     * Compute the current frozen-zone cutoff for {@code def} as the boundary's
     * bucket floor (in the base table's timestamp driver units). Returns
     * {@link Numbers#LONG_NULL} when the view has no {@code REFRESH LIMIT} set
     * (no frozen zone exists) or when the sampler cannot be reconstructed.
     * <p>
     * The boundary is the same one {@link MatViewRefreshJob} computes:
     * {@code min(max(base_ts), wallClock) - LIMIT}, or {@code wallClock - LIMIT}
     * when the escape-hatch config is on. A freshly-configured sampler is used
     * so we never race with the definition's shared sampler that the refresh
     * job mutates on every iteration. Opening the base reader is bounded by
     * the configured pool TTL; callers querying this from
     * {@code materialized_views()} pay one reader open per view.
     */
    public static long computeFrozenBoundaryBucketFloor(CairoEngine engine, MatViewDefinition def) {
        final int limitHoursOrMonths = def.getRefreshLimitHoursOrMonths();
        if (limitHoursOrMonths == 0) {
            return Numbers.LONG_NULL;
        }
        final TimestampDriver driver = def.getBaseTableTimestampDriver();
        final long now = driver.getTicks();
        final long boundaryAnchor;
        if (engine.getConfiguration().isMatViewRefreshLimitWallClockEnabled()) {
            boundaryAnchor = now;
        } else {
            final TableToken baseTableToken = engine.getTableTokenIfExists(def.getBaseTableName());
            long maxBaseTs = Long.MIN_VALUE;
            if (baseTableToken != null) {
                try (TableReader reader = engine.getReader(baseTableToken)) {
                    maxBaseTs = reader.getMaxTimestamp();
                } catch (CairoException ignored) {
                    // base reader is unavailable -- fall back to wall clock.
                }
            }
            boundaryAnchor = maxBaseTs == Long.MIN_VALUE ? now : Math.min(maxBaseTs, now);
        }
        final long rawBoundary = limitHoursOrMonths > 0
                ? boundaryAnchor - driver.fromHours(limitHoursOrMonths)
                : driver.addMonths(boundaryAnchor, limitHoursOrMonths);

        final TimestampSampler sampler;
        try {
            sampler = TimestampSamplerFactory.getInstance(
                    driver,
                    def.getSamplingInterval(),
                    def.getSamplingIntervalUnit(),
                    0
            );
        } catch (SqlException e) {
            // Stored sampler params are validated at create-time, so a failure
            // here is corruption -- prefer "no value" over surfacing a misleading
            // boundary.
            return Numbers.LONG_NULL;
        }
        sampler.setOffset(def.getFixedOffset());
        return sampler.round(rawBoundary);
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
            return;
        }
        final MatViewDefinition def = state.getViewDefinition();
        if (def == null) {
            return;
        }
        // No REFRESH LIMIT means no frozen zone exists, so a user could not
        // have reached the WAL writer via the entry-point gate
        // (canBackfillMatView). Low-level WAL-state tests and any future
        // internal write path can still legitimately commit DATA txns through
        // this writer, so let them through -- the entry-point gate remains the
        // contract. If a bug ever lets a real user backfill slip past the gate
        // the next refresh's REPLACE_RANGE will overwrite the stray rows.
        final long boundaryBucketFloor = computeFrozenBoundaryBucketFloor(engine, def);
        if (boundaryBucketFloor == Numbers.LONG_NULL) {
            // The helper returns LONG_NULL on sampler-construction failure too.
            // Let the txn through rather than fail every subsequent commit on
            // the table; refresh would skip it on the same grounds.
            return;
        }

        final TimestampDriver driver = def.getBaseTableTimestampDriver();
        // The sampler is allocated here rather than reused from the helper so
        // the helper stays a single-call utility. Cost: one extra small allocation
        // per validated commit -- negligible against the rest of the commit path.
        final TimestampSampler sampler;
        try {
            sampler = TimestampSamplerFactory.getInstance(
                    driver,
                    def.getSamplingInterval(),
                    def.getSamplingIntervalUnit(),
                    0
            );
        } catch (SqlException e) {
            return;
        }
        sampler.setOffset(def.getFixedOffset());
        final long maxRowBucketFloor = sampler.round(txnMaxTs);
        final long maxRowBucketEnd = sampler.nextTimestamp(maxRowBucketFloor);

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
}

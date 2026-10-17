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

package io.questdb.cairo.lv;

import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

/**
 * One repair's scan, runtime, splice eligibility and publication route. The repair plan
 * supplies bounds and disposition; {@code LiveViewRefreshJob.decideRepairRoute} selects
 * the route once. Executors may only downgrade a keyed scan through
 * {@link #declineKeyedScan}, which records its reason.
 * <p>
 * The open-segment key domain and pricing verdict belong to this repair; {@link #clear}
 * withdraws them. One worker reuses this object across non-nested repairs. A parked
 * repair copies its route into the session and restores it on resume, since changing
 * inputs between turns must not change a partially executed route.
 */
public final class LiveViewCheckpointRepairRoute implements Mutable {
    public static final int PUBLICATION_NONE = 0;
    // The replacement deletes every stored row in its range and inserts the replay's rows.
    public static final int PUBLICATION_REPLACE_RANGE = 1;
    // The replay's rows upsert onto the view's (timestamp, key) identity and every other
    // stored row stays where it is.
    public static final int PUBLICATION_SPARSE_UPSERT = 2;
    public static final int ROUTE_NONE = 0;
    // A whole-view rebuild from the view's START FROM boundary: a non-DATA trigger,
    // a timeline restore that failed, or a view that lost its base WAL.
    public static final int ROUTE_VIEW_REBUILD = 1;
    // The union range of a change set, or the residual behind a segment loop, read whole.
    public static final int ROUTE_RANGE_REBUILD = 2;
    // One closed anchor segment, read whole.
    public static final int ROUTE_SEGMENT_REBUILD = 3;
    // One closed anchor segment, read along the keys its correction touched.
    public static final int ROUTE_KEYED_CLOSED_SEGMENT = 4;
    // The open anchor segment, read along its key domain from the segment's start in an
    // isolated runtime that starts cold, then handed back key by key.
    public static final int ROUTE_KEYED_COLD_OPEN_SEGMENT = 5;
    // A resume from a sealed anchor root, read whole.
    public static final int ROUTE_RESUME_FROM_ANCHOR = 6;
    // A resume from a sealed anchor root, read along the open segment's key domain in an
    // isolated runtime, then handed back key by key.
    public static final int ROUTE_KEYED_RESUME_FROM_ANCHOR = 7;
    public static final int ROUTE_COUNT = 8;
    public static final int RUNTIME_NONE = 0;
    // The replay folds into the primary runtime after wiping it, or after restoring an
    // anchor root into it. The primary ends up holding the replay's state.
    public static final int RUNTIME_PRIMARY_REPLACED = 1;
    // The replay folds into the primary runtime after copying it aside, and the copy goes
    // back once the replacement publishes. Only a repair whose convergence boundary sits
    // at or below the runtime frontier may keep the pre-repair state.
    public static final int RUNTIME_PRIMARY_COPY_ASIDE = 2;
    // The primary runtime already stands exactly at the anchor root, so the resume folds
    // into it as it is and restores nothing.
    public static final int RUNTIME_PRIMARY_REUSED = 3;
    // The replay folds into the view's isolated repair runtime and leaves the primary as
    // the forward drain left it.
    public static final int RUNTIME_ISOLATED = 4;
    // Who asked for the repair, which decides the routes on offer. Set by the caller,
    // read by the decision; not part of what a parked repair carries.
    public static final int SCOPE_NONE = 0;
    // A change set's union range, or the residual behind a segment loop.
    public static final int SCOPE_CHANGE_SET = 1;
    // One closed segment of a segment loop.
    public static final int SCOPE_SEGMENT = 2;
    // A whole-view rebuild.
    public static final int SCOPE_VIEW = 3;
    // The next turn of a repair that parked.
    public static final int SCOPE_RESUMED = 4;
    private long emitLowTs = Numbers.LONG_NULL;
    private long insertedRowDelta;
    private boolean isFiniteHighBound;
    private boolean isKeyedScanDeclined;
    private boolean isLocalized;
    private boolean isOpenSegmentKeyDomainReady;
    private boolean isOpenSegmentKeyedScanCheaper;
    private boolean isOpenSegmentRestoreAwareCheaper;
    private boolean isRuntimeAnchorReusable;
    private boolean isTimelineSpliceable;
    private long keyedResumeDurableMaxTs = Numbers.LONG_NULL;
    private long keyedResumeDurableRows = Numbers.LONG_NULL;
    private CharSequence keyedScanDeclineReason;
    private int publication;
    private int route;
    private int runtime;
    private long scanLowTs = Numbers.LONG_NULL;
    private int scope;
    private long timelineHighTsExclusive = Long.MAX_VALUE;

    public static String routeName(int route) {
        return switch (route) {
            case ROUTE_VIEW_REBUILD -> "view rebuild";
            case ROUTE_RANGE_REBUILD -> "range rebuild";
            case ROUTE_SEGMENT_REBUILD -> "segment rebuild";
            case ROUTE_KEYED_CLOSED_SEGMENT -> "keyed closed segment";
            case ROUTE_KEYED_COLD_OPEN_SEGMENT -> "keyed cold open segment";
            case ROUTE_RESUME_FROM_ANCHOR -> "resume from anchor";
            case ROUTE_KEYED_RESUME_FROM_ANCHOR -> "keyed resume from anchor";
            default -> "none";
        };
    }

    public static String runtimeName(int runtime) {
        return switch (runtime) {
            case RUNTIME_PRIMARY_REPLACED -> "primary replaced";
            case RUNTIME_PRIMARY_COPY_ASIDE -> "primary copied aside";
            case RUNTIME_PRIMARY_REUSED -> "primary reused";
            case RUNTIME_ISOLATED -> "isolated";
            default -> "none";
        };
    }

    /**
     * Forgets the decision and the open segment's key domain together. Every repair entry
     * point starts here, so no repair reads a domain, a price or a route another one left.
     */
    @Override
    public void clear() {
        isOpenSegmentKeyDomainReady = false;
        clearDecision();
        scope = SCOPE_NONE;
    }

    /**
     * Takes the decision a parked repair carried, leaving the open segment's key domain and
     * the scope as they are: the domain belongs to whichever turn classified it, and the
     * resuming turn has not.
     */
    public void copyDecisionFrom(@NotNull LiveViewCheckpointRepairRoute other) {
        emitLowTs = other.emitLowTs;
        insertedRowDelta = other.insertedRowDelta;
        isFiniteHighBound = other.isFiniteHighBound;
        isKeyedScanDeclined = other.isKeyedScanDeclined;
        isLocalized = other.isLocalized;
        isOpenSegmentKeyedScanCheaper = other.isOpenSegmentKeyedScanCheaper;
        isOpenSegmentRestoreAwareCheaper = other.isOpenSegmentRestoreAwareCheaper;
        isRuntimeAnchorReusable = other.isRuntimeAnchorReusable;
        isTimelineSpliceable = other.isTimelineSpliceable;
        keyedResumeDurableMaxTs = other.keyedResumeDurableMaxTs;
        keyedResumeDurableRows = other.keyedResumeDurableRows;
        keyedScanDeclineReason = other.keyedScanDeclineReason;
        publication = other.publication;
        route = other.route;
        runtime = other.runtime;
        scanLowTs = other.scanLowTs;
        timelineHighTsExclusive = other.timelineHighTsExclusive;
    }

    /**
     * Decides how a keyed replay publishes, and is the only place that does. A sparse
     * publication upserts the replay's rows onto the view's (timestamp, key) identity and
     * leaves every other stored row standing, so it is correct only when:
     * <ul>
     *     <li>the replay bound its output as a sparse attempt, which needs the view to carry
     *     that identity over the key column the replay checks
     *     ({@link LiveViewCheckpointKeyedReplay#isSparse()});</li>
     *     <li>the replay recomputed at least one row;</li>
     *     <li>no (timestamp, key) pair repeats among the rows it emitted, or the upsert
     *     collapses them;</li>
     *     <li>every stored row of a recomputed key has its pair among them, or the upsert
     *     leaves a row standing that the replacement would delete. Only the keyed
     *     closed-segment route walks the stored interval and pairs rows - see
     *     {@link #isKeyedClosedSegment()} - so only it can find one unpaired. The two open-segment routes never walk it and admit only an
     *     insert-only change set on an unfiltered view over a non-dedup base, where every
     *     stored row is re-emitted; their count stays zero.</li>
     * </ul>
     * {@link #isSparsePublication()} reads the verdict.
     */
    public void decidePublication(
            @NotNull LiveViewCheckpointKeyedReplay keyedReplay,
            long appendedRows,
            @NotNull LiveViewCheckpointOutputUniqueness outputUniqueness
    ) {
        publication = keyedReplay.isSparse()
                && appendedRows > 0
                && outputUniqueness.isUnique()
                && keyedReplay.getUnpairedSupersededRows() == 0
                ? PUBLICATION_SPARSE_UPSERT
                : PUBLICATION_REPLACE_RANGE;
    }

    /**
     * Stops a keyed route from reading its key domain once its executor has started. The
     * one late check is the view's own stored-row scan, which only the live-view writer the
     * executor opens can vouch for. A closed-segment route becomes a whole read of the
     * segment, which nothing it has done so far depends on. The cold open-segment route
     * keeps its isolated runtime, its arithmetic row positions and its hand-back and reads
     * the segment whole: the runtime it set up starts cold at the segment's start, so a
     * whole read leaves it holding every key's state rather than the domain's.
     */
    public void declineKeyedScan(@NotNull CharSequence reason) {
        assert isKeyedScan();
        keyedScanDeclineReason = reason;
        if (route == ROUTE_KEYED_CLOSED_SEGMENT) {
            route = ROUTE_SEGMENT_REBUILD;
        } else {
            isKeyedScanDeclined = true;
        }
    }

    /**
     * The output floor {@code R} a head-miss route emits from: the plan's own when the
     * repair localized, the view's {@code START FROM} boundary otherwise.
     */
    public long getEmitLowTs() {
        return emitLowTs;
    }

    /**
     * The exact count of base rows the change set added above the durable point, which the
     * two open-segment routes derive their row positions from instead of counting stored
     * rows.
     */
    public long getInsertedRowDelta() {
        return insertedRowDelta;
    }

    /**
     * The view's maximum durable timestamp before a keyed resume, or
     * {@link Numbers#LONG_NULL} for an empty view or another route.
     */
    public long getKeyedResumeDurableMaxTs() {
        return keyedResumeDurableMaxTs;
    }

    /**
     * The view's durable row count before a keyed resume, or {@link Numbers#LONG_NULL} for
     * another route.
     */
    public long getKeyedResumeDurableRows() {
        return keyedResumeDurableRows;
    }

    /**
     * @return why the keyed scan was declined after its executor started, or null
     */
    public CharSequence getKeyedScanDeclineReason() {
        return keyedScanDeclineReason;
    }

    /**
     * @return one of the {@code PUBLICATION_*} codes, {@link #PUBLICATION_NONE} until the
     * executor commits a replacement
     */
    public int getPublication() {
        return publication;
    }

    /**
     * @return one of the {@code ROUTE_*} codes
     */
    public int getRoute() {
        return route;
    }

    /**
     * @return one of the {@code RUNTIME_*} codes
     */
    public int getRuntime() {
        return runtime;
    }

    /**
     * The read floor {@code L} a head-miss route scans from.
     */
    public long getScanLowTs() {
        return scanLowTs;
    }

    public int getScope() {
        return scope;
    }

    /**
     * The exclusive high bound a head-miss route's timeline repair re-versions up to: the
     * plan's {@code H} for a route that converges, {@link Long#MAX_VALUE} otherwise.
     */
    public long getTimelineHighTsExclusive() {
        return timelineHighTsExclusive;
    }

    /**
     * Whether the route's keyed replay holds state the repair still needs: a keyed scan's
     * key domain and merge, or the cold route's keys to hand back. A repair that parks
     * hands that replay to its session.
     */
    public boolean hasKeyedReplay() {
        return isKeyedScan() || isColdOpenSegment();
    }

    /**
     * Whether a cold keyed route bound the replay's starting point to the open segment's
     * start. True even after {@link #declineKeyedScan}, which leaves the route's runtime in
     * place.
     */
    public boolean isColdOpenSegment() {
        return route == ROUTE_KEYED_COLD_OPEN_SEGMENT;
    }

    /**
     * Whether a head-miss route stops at a finite {@code H} and keeps the pre-repair
     * runtime state: the plan localized, and its convergence boundary sits at or below the
     * runtime frontier.
     */
    public boolean isFiniteHighBound() {
        return isFiniteHighBound;
    }

    public boolean isIsolated() {
        return runtime == RUNTIME_ISOLATED;
    }

    /**
     * Whether a closed segment's replay reads the keys its correction touched. It is the
     * one route that walks the view's stored interval as it replays, pairing each stored
     * row of a recomputed key with the row that supersedes it and copying every other key's
     * row forward, so a sparse attempt that fails falls back on the merge it already
     * walked rather than walking the interval for the first time.
     */
    public boolean isKeyedClosedSegment() {
        return route == ROUTE_KEYED_CLOSED_SEGMENT;
    }

    /**
     * Whether the replay reads its key domain through the base's posting index rather than
     * the whole range.
     */
    public boolean isKeyedScan() {
        return switch (route) {
            case ROUTE_KEYED_CLOSED_SEGMENT, ROUTE_KEYED_COLD_OPEN_SEGMENT, ROUTE_KEYED_RESUME_FROM_ANCHOR ->
                    !isKeyedScanDeclined;
            default -> false;
        };
    }

    /**
     * Whether a head-miss route reads only the plan's localized interval rather than the
     * view from its boundary.
     */
    public boolean isLocalized() {
        return isLocalized;
    }

    /**
     * Whether {@code repairChangeSetSegments} classified the open segment's key domain for
     * this repair, row by row and within the key budget. Nothing else may follow the
     * domain's keys.
     */
    public boolean isOpenSegmentKeyDomainReady() {
        return isOpenSegmentKeyDomainReady;
    }

    /**
     * Whether the last pricing of a keyed open-segment read found it cheaper than the whole
     * range, on rows or on the elapsed-time model.
     */
    public boolean isOpenSegmentKeyedScanCheaper() {
        return isOpenSegmentKeyedScanCheaper;
    }

    /**
     * Whether the elapsed-time model, rather than the row count, priced the keyed read
     * cheaper.
     */
    public boolean isOpenSegmentRestoreAwareCheaper() {
        return isOpenSegmentRestoreAwareCheaper;
    }

    public boolean isResumeFromAnchor() {
        return route == ROUTE_RESUME_FROM_ANCHOR || route == ROUTE_KEYED_RESUME_FROM_ANCHOR;
    }

    /**
     * Whether the primary runtime stood exactly at the selected anchor root when the resume
     * was priced. Read by the pricing and by the elapsed-time sample, whichever route won.
     */
    public boolean isRuntimeAnchorReusable() {
        return isRuntimeAnchorReusable;
    }

    /**
     * Whether {@link #decidePublication} found the replacement may publish sparsely.
     */
    public boolean isSparsePublication() {
        return publication == PUBLICATION_SPARSE_UPSERT;
    }

    /**
     * Whether a head-miss route can re-version the roots in its range instead of
     * truncating the timeline at its floor.
     */
    public boolean isTimelineSpliceable() {
        return isTimelineSpliceable;
    }

    public boolean isViewRebuild() {
        return route == ROUTE_VIEW_REBUILD;
    }

    /**
     * Records the head-miss family's scope: the bounds the plan quotes for a localized
     * repair, or the view's boundary for one that is not.
     */
    public void ofHeadMissScope(
            boolean isLocalized,
            boolean isFiniteHighBound,
            long scanLowTs,
            long emitLowTs,
            long timelineHighTsExclusive
    ) {
        this.isLocalized = isLocalized;
        this.isFiniteHighBound = isFiniteHighBound;
        this.scanLowTs = scanLowTs;
        this.emitLowTs = emitLowTs;
        this.timelineHighTsExclusive = timelineHighTsExclusive;
    }

    /**
     * Records the measurements a keyed resume's arithmetic rests on: the view's durable
     * rows before the repair, their maximum timestamp, and the rows the change set added.
     */
    public void ofKeyedResumeBaseline(long durableRows, long durableMaxTs, long insertedRowDelta) {
        this.keyedResumeDurableRows = durableRows;
        this.keyedResumeDurableMaxTs = durableMaxTs;
        this.insertedRowDelta = insertedRowDelta;
    }

    public void ofOpenSegmentPricing(boolean isKeyedScanCheaper, boolean isRestoreAwareCheaper) {
        this.isOpenSegmentKeyedScanCheaper = isKeyedScanCheaper;
        this.isOpenSegmentRestoreAwareCheaper = isRestoreAwareCheaper;
    }

    /**
     * Records a replacement that no keyed replay could have published sparsely: the
     * empty-range delete of a rebuild whose range holds no row.
     */
    public void ofReplacePublication() {
        publication = PUBLICATION_REPLACE_RANGE;
    }

    public void ofResumeRoute(int route, int runtime, boolean isRuntimeAnchorReusable) {
        this.route = route;
        this.runtime = runtime;
        this.isRuntimeAnchorReusable = isRuntimeAnchorReusable;
    }

    /**
     * Restates the runtime of a resumed repair from the factory its session recorded. The
     * caller refuses a candidate whose runtime moved, so this agrees with the first turn.
     */
    public void ofResumedRuntime(boolean isIsolated) {
        assert isIsolated == (runtime == RUNTIME_ISOLATED) : "a resumed repair changed runtime";
        if (isIsolated) {
            runtime = RUNTIME_ISOLATED;
        } else if (runtime == RUNTIME_ISOLATED) {
            runtime = RUNTIME_PRIMARY_REPLACED;
        }
    }

    /**
     * Records the chosen route and runtime. {@code insertedRowDelta} is the cold route's
     * arithmetic input and zero for every other head-miss route.
     */
    public void ofRoute(int route, int runtime, boolean isTimelineSpliceable, long insertedRowDelta) {
        this.route = route;
        this.runtime = runtime;
        this.isTimelineSpliceable = isTimelineSpliceable;
        this.insertedRowDelta = insertedRowDelta;
    }

    /**
     * Starts a decision for a repair of the given scope, forgetting the previous decision.
     * The open segment's key domain survives: it was classified for the change set this
     * repair belongs to.
     */
    public LiveViewCheckpointRepairRoute ofScope(int scope) {
        clearDecision();
        this.scope = scope;
        return this;
    }

    public void setOpenSegmentKeyDomainReady(boolean isReady) {
        isOpenSegmentKeyDomainReady = isReady;
    }

    private void clearDecision() {
        emitLowTs = Numbers.LONG_NULL;
        insertedRowDelta = 0;
        isFiniteHighBound = false;
        isKeyedScanDeclined = false;
        isLocalized = false;
        isOpenSegmentKeyedScanCheaper = false;
        isOpenSegmentRestoreAwareCheaper = false;
        isRuntimeAnchorReusable = false;
        isTimelineSpliceable = false;
        keyedResumeDurableMaxTs = Numbers.LONG_NULL;
        keyedResumeDurableRows = Numbers.LONG_NULL;
        keyedScanDeclineReason = null;
        publication = PUBLICATION_NONE;
        route = ROUTE_NONE;
        runtime = RUNTIME_NONE;
        scanLowTs = Numbers.LONG_NULL;
        timelineHighTsExclusive = Long.MAX_VALUE;
    }
}

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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.lv.LiveViewCheckpointRecoveryPhase;
import io.questdb.cairo.lv.LiveViewCheckpointRepairPlan;
import io.questdb.cairo.lv.LiveViewDefinition;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewSegmentRepairEnvelope;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionRequirements;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

/**
 * {@code live_views()} catalogue. Exposes per-view operator state derived from
 * {@link LiveViewInstance}'s in-memory mirror of {@code _lv} + {@code _lv.s}, plus the
 * refresh worker's in-memory counters, which reset on restart.
 * <p>
 * Each column carries per-field freshness unless it belongs to one of the groups
 * {@code LiveViewsRecord.of()} reads once per row so that the group stays consistent:
 * <ul>
 *     <li>the {@code checkpoint_timeline_*} shape and the suspended repair's bounds, each
 *     published as a whole array;</li>
 *     <li>{@code checkpoint_recovery_phase} and {@code checkpoint_recovery_reason}, phase
 *     first, so a NULL phase always comes with a NULL reason;</li>
 *     <li>{@code base_apply_wait_seqtxn} and {@code base_apply_wait_micros}, NULL as a pair
 *     while the view is not waiting;</li>
 *     <li>{@code checkpoint_repair_last_disposition} and {@code checkpoint_repair_last_denial},
 *     read before the {@code o3_*} replay counters so a route never pairs with an older
 *     repair's cost.</li>
 * </ul>
 * The three adaptive-cadence columns are independent volatile fields with no joint guarantee.
 * A blocked view reports {@code view_status} as {@code invalid} and mirrors its block reason
 * into {@code invalidation_reason}; see
 * {@link io.questdb.cairo.lv.LiveViewCheckpointRecoveryPhase}.
 */
public class LiveViewsFunctionFactory implements FunctionFactory {

    static String getIntervalUnit(char unit) {
        return switch (unit) {
            case 'T' -> "MILLISECOND";
            case 's' -> "SECOND";
            case 'm' -> "MINUTE";
            case 'h' -> "HOUR";
            case 'd' -> "DAY";
            default -> null;
        };
    }

    /**
     * Renders the {@code REPAIR_PLAN_*} mask as the plan names a localized repair would union.
     * {@code none} means every out-of-order row below the head rebuilds the whole history; a view
     * that has not compiled its SELECT yet renders NULL instead.
     */
    static String getRepairPlan(int plans) {
        return switch (plans) {
            case LiveViewInstance.REPAIR_PLAN_NONE -> "none";
            case LiveViewInstance.REPAIR_PLAN_RANGE -> "range";
            case LiveViewInstance.REPAIR_PLAN_ROWS -> "rows";
            case LiveViewInstance.REPAIR_PLAN_RANGE | LiveViewInstance.REPAIR_PLAN_ROWS -> "range+rows";
            case LiveViewInstance.REPAIR_PLAN_ANCHOR -> "anchor";
            case LiveViewInstance.REPAIR_PLAN_RANGE | LiveViewInstance.REPAIR_PLAN_ANCHOR -> "range+anchor";
            case LiveViewInstance.REPAIR_PLAN_ROWS | LiveViewInstance.REPAIR_PLAN_ANCHOR -> "rows+anchor";
            case LiveViewInstance.REPAIR_PLAN_RANGE | LiveViewInstance.REPAIR_PLAN_ROWS
                         | LiveViewInstance.REPAIR_PLAN_ANCHOR -> "range+rows+anchor";
            default -> null;
        };
    }

    @Override
    public int getExecutionRequirements() {
        // lists only the objects the caller may see, see SqlExecutionRequirements
        return SqlExecutionRequirements.DISCLOSES_OBJECTS;
    }

    @Override
    public String getSignature() {
        return "live_views()";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        return new CursorFunction(new LiveViewsCursorFactory()) {
            @Override
            public boolean isRuntimeConstant() {
                return true;
            }
        };
    }

    private static class LiveViewsCursorFactory implements RecordCursorFactory {
        private static final int COLUMN_APPLIED_WATERMARK = 17;
        private static final int COLUMN_BASE_APPLY_WAIT_MICROS = 64;
        private static final int COLUMN_BASE_APPLY_WAIT_SEQTXN = 63;
        private static final int COLUMN_BASE_TABLE_NAME = 2;
        private static final int COLUMN_BELOW_LOWER_BOUND_COUNT = 13;
        private static final int COLUMN_CHECKPOINT_CORRECTION_DEPTH_SAMPLE_COUNT = 60;
        private static final int COLUMN_CHECKPOINT_DATA_SEGMENT_COUNT = 33;
        private static final int COLUMN_CHECKPOINT_EFFECTIVE_DURATION_MICROS = 58;
        private static final int COLUMN_CHECKPOINT_GC_LAG_GENERATIONS = 36;
        private static final int COLUMN_CHECKPOINT_LAST_CORRECTION_DEPTH_MICROS = 59;
        private static final int COLUMN_CHECKPOINT_LAST_LOOKUP_DEPTH = 40;
        private static final int COLUMN_CHECKPOINT_LAST_RESTORE_MICROS = 38;
        private static final int COLUMN_CHECKPOINT_LAST_WRITE_MICROS = 37;
        private static final int COLUMN_CHECKPOINT_LAST_WRITE_NEW_BYTES = 39;
        private static final int COLUMN_CHECKPOINT_OBSOLETE_SEGMENT_BYTES = 34;
        private static final int COLUMN_CHECKPOINT_OLDEST_PINNED_GENERATION = 35;
        private static final int COLUMN_CHECKPOINT_RECOVERY_PHASE = 61;
        private static final int COLUMN_CHECKPOINT_RECOVERY_REASON = 62;
        private static final int COLUMN_CHECKPOINT_REPAIR_CORRECTION_TIMESTAMP = 42;
        private static final int COLUMN_CHECKPOINT_REPAIR_FAILURES = 48;
        private static final int COLUMN_CHECKPOINT_REPAIR_HIGH_TIMESTAMP = 44;
        private static final int COLUMN_CHECKPOINT_REPAIR_IN_PROGRESS = 41;
        private static final int COLUMN_CHECKPOINT_REPAIR_LAST_DENIAL = 51;
        private static final int COLUMN_CHECKPOINT_REPAIR_LAST_DISPOSITION = 50;
        private static final int COLUMN_CHECKPOINT_REPAIR_LOW_TIMESTAMP = 43;
        private static final int COLUMN_CHECKPOINT_REPAIR_NEW_BYTES = 46;
        private static final int COLUMN_CHECKPOINT_REPAIR_PLAN = 49;
        private static final int COLUMN_CHECKPOINT_REPAIR_RESUMES = 47;
        private static final int COLUMN_CHECKPOINT_REPAIR_ROOTS_VERSIONED = 45;
        private static final int COLUMN_CHECKPOINT_ROW_COUNT_MISMATCHES = 55;
        private static final int COLUMN_CHECKPOINT_SEAL_FAILURES = 52;
        private static final int COLUMN_CHECKPOINT_TIMELINE_ENTRIES = 26;
        private static final int COLUMN_CHECKPOINT_TIMELINE_GENERATION = 25;
        private static final int COLUMN_CHECKPOINT_TIMELINE_LOGICAL_BYTES = 28;
        private static final int COLUMN_CHECKPOINT_TIMELINE_NORMALIZED_BASE_SEQTXN = 27;
        private static final int COLUMN_CHECKPOINT_TIMELINE_PHYSICAL_BYTES = 29;
        private static final int COLUMN_CHECKPOINT_TIMELINE_ROW_POSITION_DELTA_BYTES = 32;
        private static final int COLUMN_CHECKPOINT_TIMELINE_SHARED_BYTES = 30;
        private static final int COLUMN_CHECKPOINT_TIMELINE_SHARING_RATIO = 31;
        private static final int COLUMN_FLUSH_EVERY_INTERVAL = 6;
        private static final int COLUMN_FLUSH_EVERY_INTERVAL_UNIT = 7;
        private static final int COLUMN_INVALIDATION_REASON = 5;
        private static final int COLUMN_IN_MEMORY_INTERVAL = 8;
        private static final int COLUMN_IN_MEMORY_INTERVAL_UNIT = 9;
        private static final int COLUMN_IN_MEM_BYTES = 10;
        private static final int COLUMN_IN_MEM_ROWS = 11;
        private static final int COLUMN_KEYED_SCAN_GATE = 54;
        private static final int COLUMN_LAG_MICROS = 15;
        private static final int COLUMN_LAG_SEQTXN = 14;
        private static final int COLUMN_LAST_PROCESSED_SEQTXN = 16;
        private static final int COLUMN_LV_CONSUMED_SEQTXN = 18;
        private static final int COLUMN_O3_BOUNDARY_REPLAY_ROWS = 23;
        private static final int COLUMN_O3_OPEN_SEGMENT_COLD_KEYED_REPLAY_COUNT = 57;
        private static final int COLUMN_O3_OPEN_SEGMENT_KEYED_RESUME_COUNT = 56;
        private static final int COLUMN_O3_REJECTED_COUNT = 12;
        private static final int COLUMN_O3_REPLAY_SCAN_ROWS = 24;
        private static final int COLUMN_O3_RESUME_REPLAY_ROWS = 22;
        private static final int COLUMN_SEED_TARGET_SEQTXN = 21;
        private static final int COLUMN_SEGMENT_REPAIR_GATE = 53;
        private static final int COLUMN_VIEW_LOWER_BOUND_TIMESTAMP = 19;
        private static final int COLUMN_VIEW_NAME = 0;
        private static final int COLUMN_VIEW_SQL = 3;
        private static final int COLUMN_VIEW_STATUS = 4;
        private static final int COLUMN_VIEW_TABLE_DIR_NAME = 1;
        private static final int COLUMN_WRITER_STALL_MICROS = 20;
        private static final RecordMetadata METADATA;
        private final LiveViewsListCursor cursor = new LiveViewsListCursor();

        @Override
        public void close() {
            Misc.free(cursor);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            cursor.circuitBreaker = executionContext.getCircuitBreaker();
            cursor.securityContext = executionContext.getSecurityContext();
            cursor.toTop(executionContext.getCairoEngine());
            return cursor;
        }

        @Override
        public RecordMetadata getMetadata() {
            return METADATA;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("live_views()");
        }

        private static class LiveViewsListCursor implements NoRandomAccessRecordCursor {
            private final LiveViewsRecord record = new LiveViewsRecord();
            private final ObjList<LiveViewInstance> viewInstances = new ObjList<>();
            private SqlExecutionCircuitBreaker circuitBreaker;
            private CairoEngine engine;
            private SecurityContext securityContext;
            private int viewIndex = 0;

            @Override
            public void close() {
                // The factory is cached and outlives the query: drop the engine, the circuit
                // breaker, the security context and the collected instances, which may be views
                // the registry has already retired.
                viewInstances.clear();
                record.clear();
                circuitBreaker = null;
                engine = null;
                securityContext = null;
                viewIndex = 0;
            }

            @Override
            public Record getRecord() {
                return record;
            }

            @Override
            public boolean hasNext() {
                while (viewIndex < viewInstances.size()) {
                    circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                    final LiveViewInstance instance = viewInstances.getQuick(viewIndex++);
                    if (securityContext.isTableVisible(instance.getLiveViewToken())) {
                        record.of(engine, instance);
                        return true;
                    }
                }
                return false;
            }

            @Override
            public long preComputedStateSize() {
                return viewInstances.size();
            }

            @Override
            public long size() {
                return -1;
            }

            @Override
            public void toTop() {
                viewInstances.clear();
                engine.getLiveViewRegistry().getViews(viewInstances);
                viewIndex = 0;
            }

            private void toTop(CairoEngine engine) {
                this.engine = engine;
                toTop();
            }

            private static class LiveViewsRecord implements Record {
                private long applyLagDeferSinceUs;
                private long applyLagDeferTargetSeqTxn;
                private int checkpointRecoveryPhase;
                private String checkpointRecoveryReason;
                private long[] checkpointRepair;
                private long checkpointRepairOutcome;
                private long[] checkpointTimeline;
                private LiveViewDefinition definition;
                private CairoEngine engine;
                private LiveViewInstance instance;
                private boolean isCheckpointUpgradeRebuildPending;
                private long o3BoundaryReplayRows;
                private long o3ReplayScanRows;
                private long o3ResumeReplayRows;

                public void clear() {
                    applyLagDeferSinceUs = Numbers.LONG_NULL;
                    applyLagDeferTargetSeqTxn = Numbers.LONG_NULL;
                    checkpointRecoveryPhase = LiveViewCheckpointRecoveryPhase.NONE;
                    checkpointRecoveryReason = null;
                    checkpointRepair = null;
                    checkpointRepairOutcome = 0;
                    checkpointTimeline = null;
                    isCheckpointUpgradeRebuildPending = false;
                    definition = null;
                    engine = null;
                    instance = null;
                    o3BoundaryReplayRows = 0;
                    o3ReplayScanRows = 0;
                    o3ResumeReplayRows = 0;
                }

                @Override
                public boolean getBool(int col) {
                    if (instance.isStub()) {
                        return false;
                    }
                    if (col == COLUMN_CHECKPOINT_REPAIR_IN_PROGRESS) {
                        return checkpointRepair[LiveViewInstance.CHECKPOINT_REPAIR_IN_PROGRESS] != 0;
                    }
                    return false;
                }

                @Override
                public double getDouble(int col) {
                    if (instance.isStub() || col != COLUMN_CHECKPOINT_TIMELINE_SHARING_RATIO) {
                        return Double.NaN;
                    }
                    // Share of the logical state the timeline did not have to write. NULL rather
                    // than 0 while no generation exists, which is a different statement from
                    // "shares nothing".
                    final long logical = checkpointTimeline[LiveViewInstance.CHECKPOINT_TIMELINE_LOGICAL_BYTES];
                    if (checkpointTimeline[LiveViewInstance.CHECKPOINT_TIMELINE_GENERATION] == Numbers.LONG_NULL
                            || logical <= 0) {
                        return Double.NaN;
                    }
                    final long physical = checkpointTimeline[LiveViewInstance.CHECKPOINT_TIMELINE_PHYSICAL_BYTES];
                    return (double) Math.max(0, logical - physical) / logical;
                }

                @Override
                public long getLong(int col) {
                    if (instance.isStub()) {
                        // A stub has a null definition and default state: every numeric column is
                        // NULL.
                        return Numbers.LONG_NULL;
                    }
                    return switch (col) {
                        case COLUMN_FLUSH_EVERY_INTERVAL -> definition.getFlushEveryInterval();
                        case COLUMN_IN_MEMORY_INTERVAL -> definition.getInMemoryInterval();
                        case COLUMN_IN_MEM_BYTES -> {
                            // Peak-sticky allocated capacity across both slots, not live content:
                            // MemoryCARWImpl.reset() keeps its pages. Zero before the tier is
                            // allocated.
                            LiveViewInMemoryTier tier = instance.getInMemoryTier();
                            yield tier == null ? 0L : tier.footprintBytes();
                        }
                        case COLUMN_IN_MEM_ROWS -> {
                            // Rows in the published slot. They age out on a refresh-driven
                            // slow-path swap, not on wall-clock time, so an idle view keeps its
                            // last rows. Zero before the tier is allocated.
                            LiveViewInMemoryTier tier = instance.getInMemoryTier();
                            yield tier == null ? 0L : tier.publishedRowCount();
                        }
                        case COLUMN_LAG_SEQTXN -> {
                            // The base token can be unresolved on a replica whose view registered
                            // before its base table downloaded; report an unknown lag.
                            TableToken baseToken = definition.getBaseTableToken();
                            if (baseToken == null) {
                                yield Numbers.LONG_NULL;
                            }
                            SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(baseToken);
                            long head = tracker.getWriterTxn();
                            long lp = instance.getLastProcessedSeqTxn();
                            yield head < 0 || lp < 0 ? Numbers.LONG_NULL : Math.max(0, head - lp);
                        }
                        case COLUMN_LAG_MICROS -> {
                            // Now minus the last successful flush, a proxy for the age of the last
                            // processed base commit.
                            long lastFlushUs = instance.getLastFlushTimeUs();
                            if (lastFlushUs == Numbers.LONG_NULL) {
                                yield Numbers.LONG_NULL;
                            }
                            long nowUs = engine.getConfiguration().getMicrosecondClock().getTicks();
                            yield Math.max(0, nowUs - lastFlushUs);
                        }
                        // The base seqTxn the refresh waits to see applied, and for how long; NULL
                        // as a pair while the view is not waiting.
                        case COLUMN_BASE_APPLY_WAIT_SEQTXN -> applyLagDeferSinceUs == Numbers.LONG_NULL
                                ? Numbers.LONG_NULL
                                : applyLagDeferTargetSeqTxn;
                        case COLUMN_BASE_APPLY_WAIT_MICROS -> {
                            if (applyLagDeferSinceUs == Numbers.LONG_NULL
                                    || applyLagDeferTargetSeqTxn == Numbers.LONG_NULL) {
                                yield Numbers.LONG_NULL;
                            }
                            final long nowUs = engine.getConfiguration().getMicrosecondClock().getTicks();
                            yield Math.max(0, nowUs - applyLagDeferSinceUs);
                        }
                        case COLUMN_LAST_PROCESSED_SEQTXN -> instance.getLastProcessedSeqTxn();
                        case COLUMN_APPLIED_WATERMARK -> instance.getStateReader().getAppliedWatermark();
                        case COLUMN_LV_CONSUMED_SEQTXN -> instance.getStateReader().getLvConsumedSeqTxn();
                        // The newest published generation's shape, read as one tuple in of(); NULL
                        // while no generation is published.
                        case COLUMN_CHECKPOINT_TIMELINE_GENERATION ->
                                checkpointTimeline[LiveViewInstance.CHECKPOINT_TIMELINE_GENERATION];
                        case COLUMN_CHECKPOINT_TIMELINE_ENTRIES -> {
                            yield checkpointTimeline[LiveViewInstance.CHECKPOINT_TIMELINE_GENERATION] == Numbers.LONG_NULL
                                    ? Numbers.LONG_NULL
                                    : checkpointTimeline[LiveViewInstance.CHECKPOINT_TIMELINE_ENTRIES];
                        }
                        case COLUMN_CHECKPOINT_TIMELINE_NORMALIZED_BASE_SEQTXN ->
                                checkpointTimeline[LiveViewInstance.CHECKPOINT_TIMELINE_NORMALIZED_BASE_SEQ_TXN];
                        case COLUMN_CHECKPOINT_TIMELINE_LOGICAL_BYTES -> {
                            yield checkpointTimeline[LiveViewInstance.CHECKPOINT_TIMELINE_GENERATION] == Numbers.LONG_NULL
                                    ? Numbers.LONG_NULL
                                    : checkpointTimeline[LiveViewInstance.CHECKPOINT_TIMELINE_LOGICAL_BYTES];
                        }
                        case COLUMN_CHECKPOINT_TIMELINE_PHYSICAL_BYTES -> {
                            yield checkpointTimeline[LiveViewInstance.CHECKPOINT_TIMELINE_GENERATION] == Numbers.LONG_NULL
                                    ? Numbers.LONG_NULL
                                    : checkpointTimeline[LiveViewInstance.CHECKPOINT_TIMELINE_PHYSICAL_BYTES];
                        }
                        case COLUMN_CHECKPOINT_TIMELINE_SHARED_BYTES -> {
                            // Floored at zero: the metadata can outweigh the state it describes.
                            if (checkpointTimeline[LiveViewInstance.CHECKPOINT_TIMELINE_GENERATION] == Numbers.LONG_NULL) {
                                yield Numbers.LONG_NULL;
                            }
                            yield Math.max(
                                    0,
                                    checkpointTimeline[LiveViewInstance.CHECKPOINT_TIMELINE_LOGICAL_BYTES]
                                            - checkpointTimeline[LiveViewInstance.CHECKPOINT_TIMELINE_PHYSICAL_BYTES]
                            );
                        }
                        case COLUMN_CHECKPOINT_TIMELINE_ROW_POSITION_DELTA_BYTES -> {
                            yield checkpointTimeline[LiveViewInstance.CHECKPOINT_TIMELINE_GENERATION] == Numbers.LONG_NULL
                                    ? Numbers.LONG_NULL
                                    : checkpointTimeline[LiveViewInstance.CHECKPOINT_TIMELINE_ROW_POSITION_DELTA_BYTES];
                        }
                        case COLUMN_CHECKPOINT_OLDEST_PINNED_GENERATION ->
                                checkpointTimeline[LiveViewInstance.CHECKPOINT_TIMELINE_OLDEST_RETAINED_GENERATION];
                        case COLUMN_CHECKPOINT_GC_LAG_GENERATIONS -> {
                            // The A/B pair keeps the previous generation as its fallback, so 1 is
                            // healthy and a growing value means retirement has stalled.
                            final long current = checkpointTimeline[LiveViewInstance.CHECKPOINT_TIMELINE_GENERATION];
                            final long oldest =
                                    checkpointTimeline[LiveViewInstance.CHECKPOINT_TIMELINE_OLDEST_RETAINED_GENERATION];
                            yield current == Numbers.LONG_NULL || oldest == Numbers.LONG_NULL
                                    ? Numbers.LONG_NULL
                                    : Math.max(0, current - oldest);
                        }
                        // The last purge sweep's findings; LONG_NULL until one has run.
                        case COLUMN_CHECKPOINT_DATA_SEGMENT_COUNT -> instance.getCheckpointDataSegmentCount();
                        case COLUMN_CHECKPOINT_OBSOLETE_SEGMENT_BYTES -> instance.getCheckpointObsoleteSegmentBytes();
                        // LONG_NULL until the event first runs.
                        case COLUMN_CHECKPOINT_LAST_WRITE_MICROS -> instance.getHeadCheckpointWriteMicros();
                        case COLUMN_CHECKPOINT_LAST_RESTORE_MICROS -> instance.getHeadCheckpointRestoreMicros();
                        case COLUMN_CHECKPOINT_LAST_WRITE_NEW_BYTES -> {
                            yield checkpointTimeline[LiveViewInstance.CHECKPOINT_TIMELINE_GENERATION] == Numbers.LONG_NULL
                                    ? Numbers.LONG_NULL
                                    : checkpointTimeline[LiveViewInstance.CHECKPOINT_TIMELINE_LAST_WRITE_NEW_BYTES];
                        }
                        case COLUMN_CHECKPOINT_LAST_LOOKUP_DEPTH -> instance.getCheckpointLastLookupDepth();
                        // Bounds of a repair suspended across refresh turns, NULL when none is. The
                        // high bound is also NULL for a repair that converges only at EOF.
                        case COLUMN_CHECKPOINT_REPAIR_CORRECTION_TIMESTAMP -> toMicros(
                                checkpointRepair[LiveViewInstance.CHECKPOINT_REPAIR_CORRECTION_TS]
                        );
                        case COLUMN_CHECKPOINT_REPAIR_LOW_TIMESTAMP -> toMicros(
                                checkpointRepair[LiveViewInstance.CHECKPOINT_REPAIR_LOW_TS]
                        );
                        case COLUMN_CHECKPOINT_REPAIR_HIGH_TIMESTAMP -> toMicros(
                                checkpointRepair[LiveViewInstance.CHECKPOINT_REPAIR_HIGH_TS]
                        );
                        case COLUMN_CHECKPOINT_REPAIR_ROOTS_VERSIONED -> instance.getCheckpointRepairRootsVersioned();
                        case COLUMN_CHECKPOINT_REPAIR_NEW_BYTES -> instance.getCheckpointRepairNewBytes();
                        case COLUMN_CHECKPOINT_REPAIR_RESUMES -> instance.getCheckpointRepairResumes();
                        case COLUMN_CHECKPOINT_REPAIR_FAILURES -> instance.getCheckpointRepairFailures();
                        // The only in-band signal that a view serves correct results over stale
                        // restart-recovery state, since the refresh job swallows seal faults.
                        case COLUMN_CHECKPOINT_SEAL_FAILURES -> instance.getCheckpointSealFailures();
                        // At least the emit counters; a WHERE filter makes the scan exceed them.
                        case COLUMN_O3_REPLAY_SCAN_ROWS -> o3ReplayScanRows;
                        // The resume restores a root below the correction; the cold replay starts
                        // from the open anchor segment's origin because no such root exists.
                        case COLUMN_O3_OPEN_SEGMENT_KEYED_RESUME_COUNT -> instance.getO3OpenSegmentKeyedResumeCount();
                        case COLUMN_O3_OPEN_SEGMENT_COLD_KEYED_REPLAY_COUNT ->
                                instance.getO3OpenSegmentColdKeyedReplayCount();
                        // Independent volatile fields: the worker publishes nothing spanning the
                        // three, so of() has nothing to snapshot. With the feature off the
                        // effective value is the configured cadence.
                        case COLUMN_CHECKPOINT_EFFECTIVE_DURATION_MICROS -> {
                            final long configured =
                                    engine.getConfiguration().getLiveViewCheckpointMaxDurationMicros();
                            yield engine.getConfiguration().isLiveViewCheckpointAdaptiveCadenceEnabled()
                                    ? instance.getEffectiveCheckpointDurationMicros(configured)
                                    : configured;
                        }
                        case COLUMN_CHECKPOINT_LAST_CORRECTION_DEPTH_MICROS ->
                                instance.getAdaptiveCheckpointLastCorrectionDepthMicros();
                        case COLUMN_CHECKPOINT_CORRECTION_DEPTH_SAMPLE_COUNT ->
                                instance.getAdaptiveCheckpointCorrectionCount();
                        // Seals refused because the emitted and stored row counts disagreed; zero
                        // is the only healthy value. The view keeps serving, and a restart or O3
                        // correction rebuilds from the base.
                        case COLUMN_CHECKPOINT_ROW_COUNT_MISMATCHES -> instance.getCheckpointRowCountMismatches();
                        // START FROM BEGINNING persists LONG_NULL, which passes through as NULL.
                        case COLUMN_VIEW_LOWER_BOUND_TIMESTAMP -> toMicros(definition.getViewLowerBoundTimestamp());
                        case COLUMN_WRITER_STALL_MICROS -> {
                            // Set when both in-mem slots are reader-pinned, cleared on the next
                            // publish.
                            long stallStart = instance.getWriterStallStartUs();
                            if (stallStart == Numbers.LONG_NULL) {
                                yield 0L;
                            }
                            long nowUs = engine.getConfiguration().getMicrosecondClock().getTicks();
                            yield Math.max(0, nowUs - stallStart);
                        }
                        // LONG_NULL once the seed sweep completes and the view turns ACTIVE.
                        case COLUMN_SEED_TARGET_SEQTXN -> instance.getStateReader().getSeedTargetSeqTxn();
                        // Late O3 rows below viewLowerBoundTimestamp.
                        case COLUMN_O3_REJECTED_COUNT -> instance.getO3RejectedCount();
                        // In-order rows below viewLowerBoundTimestamp; disjoint from
                        // o3_rejected_count.
                        case COLUMN_BELOW_LOWER_BOUND_COUNT -> instance.getBelowLowerBoundCount();
                        // Rows re-emitted by bounded resume-from-anchor replays.
                        case COLUMN_O3_RESUME_REPLAY_ROWS -> o3ResumeReplayRows;
                        // Rows re-emitted by the O(view age) boundary rebuild; disjoint from
                        // o3_resume_replay_rows.
                        case COLUMN_O3_BOUNDARY_REPLAY_ROWS -> o3BoundaryReplayRows;
                        // A numeric column without an arm reads as NULL rather than 0, which a
                        // TIMESTAMP column would render as 1970-01-01.
                        default -> Numbers.LONG_NULL;
                    };
                }

                @Override
                public CharSequence getStrA(int col) {
                    if (instance.isStub()) {
                        // A stub has a null definition: surface only the name and the status.
                        return switch (col) {
                            case COLUMN_VIEW_NAME -> instance.getLiveViewToken().getTableName();
                            case COLUMN_VIEW_TABLE_DIR_NAME -> instance.getLiveViewToken().getDirName();
                            case COLUMN_VIEW_STATUS -> instance.getLifecycleState().catalogueName();
                            default -> null;
                        };
                    }
                    return switch (col) {
                        case COLUMN_VIEW_NAME -> definition.getViewName();
                        case COLUMN_VIEW_TABLE_DIR_NAME -> instance.getLiveViewToken().getDirName();
                        case COLUMN_BASE_TABLE_NAME -> definition.getBaseTableName();
                        case COLUMN_VIEW_STATUS -> instance.getLifecycleState().catalogueName();
                        case COLUMN_FLUSH_EVERY_INTERVAL_UNIT ->
                                getIntervalUnit(definition.getFlushEveryIntervalUnit());
                        case COLUMN_IN_MEMORY_INTERVAL_UNIT -> getIntervalUnit(definition.getInMemoryIntervalUnit());
                        case COLUMN_VIEW_SQL -> definition.getViewSql();
                        // A blocked view reports invalid status, so its block reason shows here
                        // too. It is read off the instance rather than persisted, so a state copy
                        // cannot turn a block into a terminal invalidation.
                        case COLUMN_INVALIDATION_REASON -> {
                            final CharSequence reason = instance.getInvalidationReason();
                            if (reason != null) {
                                yield reason;
                            }
                            yield LiveViewCheckpointRecoveryPhase.isBlocked(checkpointRecoveryPhase)
                                    ? checkpointRecoveryReason
                                    : null;
                        }
                        // NULL for a view whose recovery finished or never had to run.
                        case COLUMN_CHECKPOINT_RECOVERY_PHASE -> LiveViewCheckpointRecoveryPhase.name(
                                checkpointRecoveryPhase,
                                isCheckpointUpgradeRebuildPending
                        );
                        case COLUMN_CHECKPOINT_RECOVERY_REASON -> checkpointRecoveryReason;
                        // NULL until the view compiles its SELECT.
                        case COLUMN_CHECKPOINT_REPAIR_PLAN ->
                                getRepairPlan(instance.getCheckpointRepairDependencyPlans());
                        // Both NULL until the view runs a repair; the denial stays NULL for a
                        // repair that read exactly its localized interval.
                        case COLUMN_CHECKPOINT_REPAIR_LAST_DISPOSITION -> LiveViewCheckpointRepairPlan.dispositionName(
                                LiveViewInstance.repairDispositionOf(checkpointRepairOutcome),
                                LiveViewInstance.repairDenialReasonOf(checkpointRepairOutcome)
                        );
                        case COLUMN_CHECKPOINT_REPAIR_LAST_DENIAL -> LiveViewCheckpointRepairPlan.denialReasonName(
                                LiveViewInstance.repairDenialReasonOf(checkpointRepairOutcome)
                        );
                        // Both describe the view's SQL rather than any repair; NULL until the view
                        // compiles it.
                        case COLUMN_SEGMENT_REPAIR_GATE ->
                                LiveViewSegmentRepairEnvelope.gateName(instance.getSegmentScopeGate());
                        case COLUMN_KEYED_SCAN_GATE ->
                                LiveViewSegmentRepairEnvelope.gateName(instance.getKeyedScanGate());
                        default -> null;
                    };
                }

                @Override
                public CharSequence getStrB(int col) {
                    return getStrA(col);
                }

                @Override
                public int getStrLen(int col) {
                    return TableUtils.lengthOf(getStrA(col));
                }

                public void of(CairoEngine engine, LiveViewInstance instance) {
                    this.engine = engine;
                    this.instance = instance;
                    this.definition = instance.getDefinition();
                    // The writer replaces each tuple array whole, so one read per row keeps its
                    // columns consistent.
                    this.checkpointRepair = instance.getCheckpointRepair();
                    this.checkpointTimeline = instance.getCheckpointTimeline();
                    // Phase, then reason: writers set the reason before the phase and clear it
                    // after, so this order never pairs a phase with an older reason. The NONE gate
                    // drops a reason whose phase this read came too early to see.
                    this.checkpointRecoveryPhase = instance.getCheckpointRecoveryPhase();
                    this.checkpointRecoveryReason = checkpointRecoveryPhase == LiveViewCheckpointRecoveryPhase.NONE
                            ? null
                            : instance.getCheckpointRecoveryReason();
                    // A pending upgrade rebuild reports only where no phase does, and never for an
                    // invalid view, which will not refresh again. Flag, then reason, as above: a
                    // NULL reason beside a set flag is a clear in progress, so it reports as no
                    // longer pending.
                    if (checkpointRecoveryPhase == LiveViewCheckpointRecoveryPhase.NONE
                            && !instance.isInvalid()
                            && instance.isCheckpointUpgradeRebuildPending()) {
                        this.checkpointRecoveryReason = instance.getCheckpointUpgradeRebuildReason();
                        this.isCheckpointUpgradeRebuildPending = checkpointRecoveryReason != null;
                    } else {
                        this.isCheckpointUpgradeRebuildPending = false;
                    }
                    // Target, then stamp: the writer publishes the target first and clears the
                    // stamp first. A clear and re-arm between the two reads can still pair one
                    // wait's target with the next wait's stamp, for this read only.
                    this.applyLagDeferTargetSeqTxn = instance.getApplyLagDeferTargetSeqTxn();
                    this.applyLagDeferSinceUs = instance.getApplyLagDeferSinceUs();
                    // Outcome before counters: the worker bumps the o3_* counters before it
                    // publishes the outcome, so the disposition names a repair the counters already
                    // include. The writer half is asserted in
                    // LiveViewInstance.publishCheckpointRepairOutcome(); this read order is
                    // load-bearing but unasserted.
                    this.checkpointRepairOutcome = instance.getCheckpointRepairLastOutcome();
                    this.o3ResumeReplayRows = instance.getO3ResumeReplayRows();
                    this.o3BoundaryReplayRows = instance.getO3BoundaryReplayRows();
                    this.o3ReplayScanRows = instance.getO3ReplayScanRows();
                }

                /**
                 * Converts a base-unit timestamp to the catalogue's TIMESTAMP_MICRO. LONG_NULL
                 * passes through untouched, since the driver's rescale would turn the sentinel into
                 * an arbitrary timestamp.
                 */
                private long toMicros(long raw) {
                    return raw == Numbers.LONG_NULL
                            ? Numbers.LONG_NULL
                            : ColumnType.getTimestampDriver(definition.getBaseTimestampType()).toMicros(raw);
                }
            }
        }

        static {
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("view_name", ColumnType.STRING));                          // 0
            metadata.add(new TableColumnMetadata("view_table_dir_name", ColumnType.STRING));                // 1
            metadata.add(new TableColumnMetadata("base_table_name", ColumnType.STRING));                    // 2
            metadata.add(new TableColumnMetadata("view_sql", ColumnType.STRING));                           // 3
            metadata.add(new TableColumnMetadata("view_status", ColumnType.STRING));                        // 4
            metadata.add(new TableColumnMetadata("invalidation_reason", ColumnType.STRING));                // 5
            metadata.add(new TableColumnMetadata("flush_every_interval", ColumnType.LONG));                 // 6
            metadata.add(new TableColumnMetadata("flush_every_interval_unit", ColumnType.STRING));          // 7
            metadata.add(new TableColumnMetadata("in_memory_interval", ColumnType.LONG));                   // 8
            metadata.add(new TableColumnMetadata("in_memory_interval_unit", ColumnType.STRING));            // 9
            metadata.add(new TableColumnMetadata("in_mem_bytes", ColumnType.LONG));                         // 10
            metadata.add(new TableColumnMetadata("in_mem_rows", ColumnType.LONG));                          // 11
            metadata.add(new TableColumnMetadata("o3_rejected_count", ColumnType.LONG));                    // 12
            metadata.add(new TableColumnMetadata("below_lower_bound_count", ColumnType.LONG));              // 13
            metadata.add(new TableColumnMetadata("lag_seqtxn", ColumnType.LONG));                           // 14
            metadata.add(new TableColumnMetadata("lag_micros", ColumnType.LONG));                           // 15
            metadata.add(new TableColumnMetadata("last_processed_seqtxn", ColumnType.LONG));                // 16
            metadata.add(new TableColumnMetadata("applied_watermark", ColumnType.LONG));                    // 17
            metadata.add(new TableColumnMetadata("lv_consumed_seqtxn", ColumnType.LONG));                   // 18
            metadata.add(new TableColumnMetadata("view_lower_bound_timestamp", ColumnType.TIMESTAMP_MICRO));// 19
            metadata.add(new TableColumnMetadata("writer_stall_micros", ColumnType.LONG));                  // 20
            metadata.add(new TableColumnMetadata("seed_target_seqtxn", ColumnType.LONG));               // 21
            metadata.add(new TableColumnMetadata("o3_resume_replay_rows", ColumnType.LONG));                // 22
            metadata.add(new TableColumnMetadata("o3_boundary_replay_rows", ColumnType.LONG));              // 23
            metadata.add(new TableColumnMetadata("o3_replay_scan_rows", ColumnType.LONG));                  // 24
            metadata.add(new TableColumnMetadata("checkpoint_timeline_generation", ColumnType.LONG));       // 25
            metadata.add(new TableColumnMetadata("checkpoint_timeline_entries", ColumnType.LONG));          // 26
            metadata.add(new TableColumnMetadata("checkpoint_timeline_normalized_base_seqtxn", ColumnType.LONG)); // 27
            metadata.add(new TableColumnMetadata("checkpoint_timeline_logical_bytes", ColumnType.LONG));    // 28
            metadata.add(new TableColumnMetadata("checkpoint_timeline_physical_bytes", ColumnType.LONG));   // 29
            metadata.add(new TableColumnMetadata("checkpoint_timeline_shared_bytes", ColumnType.LONG));     // 30
            metadata.add(new TableColumnMetadata("checkpoint_timeline_sharing_ratio", ColumnType.DOUBLE));  // 31
            metadata.add(new TableColumnMetadata("checkpoint_timeline_row_position_delta_bytes", ColumnType.LONG)); // 32
            metadata.add(new TableColumnMetadata("checkpoint_data_segment_count", ColumnType.LONG));        // 33
            metadata.add(new TableColumnMetadata("checkpoint_obsolete_segment_bytes", ColumnType.LONG));    // 34
            metadata.add(new TableColumnMetadata("checkpoint_oldest_pinned_generation", ColumnType.LONG));  // 35
            metadata.add(new TableColumnMetadata("checkpoint_gc_lag_generations", ColumnType.LONG));        // 36
            metadata.add(new TableColumnMetadata("checkpoint_last_write_micros", ColumnType.LONG));         // 37
            metadata.add(new TableColumnMetadata("checkpoint_last_restore_micros", ColumnType.LONG));       // 38
            metadata.add(new TableColumnMetadata("checkpoint_last_write_new_bytes", ColumnType.LONG));      // 39
            metadata.add(new TableColumnMetadata("checkpoint_last_lookup_depth", ColumnType.LONG));         // 40
            metadata.add(new TableColumnMetadata("checkpoint_repair_in_progress", ColumnType.BOOLEAN));     // 41
            metadata.add(new TableColumnMetadata("checkpoint_repair_correction_timestamp", ColumnType.TIMESTAMP_MICRO)); // 42
            metadata.add(new TableColumnMetadata("checkpoint_repair_low_timestamp", ColumnType.TIMESTAMP_MICRO));  // 43
            metadata.add(new TableColumnMetadata("checkpoint_repair_high_timestamp", ColumnType.TIMESTAMP_MICRO)); // 44
            metadata.add(new TableColumnMetadata("checkpoint_repair_roots_versioned", ColumnType.LONG));    // 45
            metadata.add(new TableColumnMetadata("checkpoint_repair_new_bytes", ColumnType.LONG));          // 46
            metadata.add(new TableColumnMetadata("checkpoint_repair_resumes", ColumnType.LONG));            // 47
            metadata.add(new TableColumnMetadata("checkpoint_repair_failures", ColumnType.LONG));           // 48
            metadata.add(new TableColumnMetadata("checkpoint_repair_plan", ColumnType.STRING));             // 49
            metadata.add(new TableColumnMetadata("checkpoint_repair_last_disposition", ColumnType.STRING)); // 50
            metadata.add(new TableColumnMetadata("checkpoint_repair_last_denial", ColumnType.STRING));      // 51
            metadata.add(new TableColumnMetadata("checkpoint_seal_failures", ColumnType.LONG));            // 52
            metadata.add(new TableColumnMetadata("checkpoint_segment_repair_gate", ColumnType.STRING));          // 53
            metadata.add(new TableColumnMetadata("checkpoint_keyed_scan_gate", ColumnType.STRING));      // 54
            metadata.add(new TableColumnMetadata("checkpoint_row_count_mismatches", ColumnType.LONG));    // 55
            metadata.add(new TableColumnMetadata("o3_open_segment_keyed_resume_count", ColumnType.LONG)); // 56
            metadata.add(new TableColumnMetadata("o3_open_segment_cold_keyed_replay_count", ColumnType.LONG)); // 57
            metadata.add(new TableColumnMetadata("checkpoint_effective_duration_micros", ColumnType.LONG)); // 58
            metadata.add(new TableColumnMetadata("checkpoint_last_correction_depth_micros", ColumnType.LONG)); // 59
            metadata.add(new TableColumnMetadata("checkpoint_correction_depth_sample_count", ColumnType.LONG)); // 60
            metadata.add(new TableColumnMetadata("checkpoint_recovery_phase", ColumnType.STRING));         // 61
            metadata.add(new TableColumnMetadata("checkpoint_recovery_reason", ColumnType.STRING));        // 62
            metadata.add(new TableColumnMetadata("base_apply_wait_seqtxn", ColumnType.LONG));              // 63
            metadata.add(new TableColumnMetadata("base_apply_wait_micros", ColumnType.LONG));              // 64
            METADATA = metadata;
        }
    }
}

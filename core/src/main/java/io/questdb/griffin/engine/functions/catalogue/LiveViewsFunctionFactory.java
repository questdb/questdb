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
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.lv.LiveViewCheckpointRepairPlan;
import io.questdb.cairo.lv.LiveViewDefinition;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
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
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

/**
 * {@code live_views()} catalogue. Exposes per-view operator state derived from
 * {@link LiveViewInstance}'s in-memory mirror of {@code _lv} + {@code _lv.s}.
 * <p>
 * {@code o3_rejected_count}, {@code below_lower_bound_count},
 * {@code seed_target_seqtxn}, {@code writer_stall_micros}, and
 * {@code in_mem_bytes} are wired to live values. {@code o3_rejected_count} and
 * {@code below_lower_bound_count} split the rows a view drops for sitting below
 * its lower bound by arrival path - O3 replay vs in-order forward-append - so
 * the two are disjoint and sum to the total dropped.
 * {@code in_mem_bytes} is the peak-sticky native footprint (allocated
 * capacity across both slots); {@code in_mem_rows} is the live row count of the
 * published slot, so the two together separate a view actively buffering rows
 * from one holding arena capacity retained from a past burst.
 * {@code last_processed_seqtxn} and {@code applied_watermark} are
 * surfaced as debug columns; both are useful for operators tracking
 * refresh-worker progress before the corresponding {@code lvConsumed} flow
 * catches up.
 * {@code o3_resume_replay_rows} and {@code o3_boundary_replay_rows} trail after
 * them as O3-replay observability: they split the rows an O3 re-emits by path -
 * bounded resume-from-anchor replays versus the residual O(view age) boundary
 * rebuild - so the two are disjoint. A resume count that grows while the boundary
 * count stays flat is the checkpoint timeline bounding O3 cost as intended; a
 * growing boundary count flags late rows the timeline holds no boundary below.
 * {@code o3_replay_scan_rows} counts base rows the O3 replay paths scanned, which
 * equals the emit counters without a WHERE filter and exceeds them with one. All
 * three are in-memory counters that reset on restart.
 * <p>
 * The {@code checkpoint_*} group describes the versioned checkpoint timeline, and
 * splits four ways.
 * <ul>
 *     <li>The generation's shape - {@code checkpoint_timeline_generation},
 *     {@code _entries}, {@code _normalized_base_seqtxn}, {@code _logical_bytes},
 *     {@code _physical_bytes}, {@code _shared_bytes}, {@code _sharing_ratio} and
 *     {@code _row_position_delta_bytes} - read off the superblock whichever seam
 *     last committed or adopted a generation: a cadence seal, a repair splice or
 *     startup reconciliation. Logical bytes is what the roots would cost as
 *     complete independent state images and physical bytes is what the timeline
 *     actually wrote, so the difference between them is the sharing the
 *     persistent chunk layer and the copy-on-write trees buy. All are NULL for a
 *     view holding no published generation.</li>
 *     <li>Collection - {@code checkpoint_data_segment_count},
 *     {@code checkpoint_obsolete_segment_bytes},
 *     {@code checkpoint_oldest_pinned_generation} and
 *     {@code checkpoint_gc_lag_generations}. The first two come from the ordered
 *     catalogue walk the purge sweep makes, which runs at startup and at a
 *     retrying publication rather than per seal, so they carry the last sweep's
 *     verdict. The count is data segments alone; the obsolete bytes span the
 *     metadata segments the copy-on-write trees retire as well, because both
 *     kinds wait on the same fallback slot and reader pins before their files can
 *     go. The last two are per-publication: the A/B pair retains the previous
 *     generation as its recovery fallback, so a healthy lag is 1.</li>
 *     <li>Cost - {@code checkpoint_last_write_micros},
 *     {@code checkpoint_last_restore_micros},
 *     {@code checkpoint_last_write_new_bytes} and
 *     {@code checkpoint_last_lookup_depth}. The last one is the tree height a
 *     point lookup descended, which should track the logarithm of the checkpoint
 *     count rather than the count.</li>
 *     <li>Localized out-of-order repair - {@code checkpoint_repair_in_progress}
 *     plus the bounds {@code _correction_timestamp} ({@code C}),
 *     {@code _low_timestamp} ({@code L}) and {@code _high_timestamp} ({@code H},
 *     NULL when the repair converges only at EOF), which are populated while a
 *     repair is suspended across refresh turns. The counters
 *     {@code checkpoint_repair_roots_versioned}, {@code _new_bytes},
 *     {@code _resumes} and {@code _failures} are lifetime totals and reset on
 *     restart. {@code checkpoint_repair_plan} follows with the shape of
 *     the repair rather than one repair's progress: it names the dependency plans
 *     the view's window functions carry - {@code range}, {@code rows},
 *     {@code anchor} or a {@code +}-joined combination - and reads {@code none}
 *     for a view no plan covers, which rebuilds its whole history for every
 *     out-of-order row below the head. It is NULL until the view compiles its
 *     SELECT, and it describes what the SQL admits rather than what the next
 *     repair does: a named plan can still be denied per refresh, a ROWS one over
 *     a base that deduplicates most commonly. {@code checkpoint_repair_last_disposition}
 *     and {@code checkpoint_repair_last_denial} close the group with that runtime
 *     outcome. The first names which executor the view's last repair ran -
 *     {@code localized rebuild}, {@code boundary rebuild} or
 *     {@code resume from anchor} - and the second why it read more than a localized
 *     rebuild would, such as {@code dedup}, {@code incomplete dependency},
 *     {@code scan budget} or {@code resume cheaper}. Both are NULL until the view runs
 *     a repair; the denial stays NULL for a repair that read exactly its localized
 *     interval. So a view reporting a {@code rows} plan beside a
 *     {@code boundary rebuild} / {@code dedup} pair is one whose SQL admits a bound
 *     that its base denies at every refresh.</li>
 * </ul>
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
     * Renders the {@code REPAIR_PLAN_*} mask a view carries as the plan names a
     * localized repair would union - {@code none} when it would union nothing and
     * every out-of-order row below the head therefore rebuilds the whole view
     * history. NULL, rather than {@code none}, while the view has not compiled its
     * SELECT: the two say different things, and only the first is a latency cliff.
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
        private static final int COLUMN_BASE_TABLE_NAME = 2;
        private static final int COLUMN_BELOW_LOWER_BOUND_COUNT = 13;
        private static final int COLUMN_CHECKPOINT_DATA_SEGMENT_COUNT = 33;
        private static final int COLUMN_CHECKPOINT_GC_LAG_GENERATIONS = 36;
        private static final int COLUMN_CHECKPOINT_LAST_LOOKUP_DEPTH = 40;
        private static final int COLUMN_CHECKPOINT_LAST_RESTORE_MICROS = 38;
        private static final int COLUMN_CHECKPOINT_LAST_WRITE_MICROS = 37;
        private static final int COLUMN_CHECKPOINT_LAST_WRITE_NEW_BYTES = 39;
        private static final int COLUMN_CHECKPOINT_OBSOLETE_SEGMENT_BYTES = 34;
        private static final int COLUMN_CHECKPOINT_OLDEST_PINNED_GENERATION = 35;
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
        private static final int COLUMN_LAG_MICROS = 15;
        private static final int COLUMN_LAG_SEQTXN = 14;
        private static final int COLUMN_LAST_PROCESSED_SEQTXN = 16;
        private static final int COLUMN_LV_CONSUMED_SEQTXN = 18;
        private static final int COLUMN_O3_BOUNDARY_REPLAY_ROWS = 23;
        private static final int COLUMN_O3_REJECTED_COUNT = 12;
        private static final int COLUMN_O3_REPLAY_SCAN_ROWS = 24;
        private static final int COLUMN_O3_RESUME_REPLAY_ROWS = 22;
        private static final int COLUMN_SEED_TARGET_SEQTXN = 21;
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
            executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedTimeThrottled();
            cursor.circuitBreaker = executionContext.getCircuitBreaker();
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
            private int viewIndex = 0;

            @Override
            public void close() {
                // The factory is cached and outlives the query, so anything the last
                // scan touched stays reachable until the next one replaces it. Drop
                // the engine, the query's circuit breaker and every LiveViewInstance
                // the walk collected - the instances in particular can be dropped
                // views the registry has already retired.
                viewInstances.clear();
                record.clear();
                circuitBreaker = null;
                engine = null;
                viewIndex = 0;
            }

            @Override
            public Record getRecord() {
                return record;
            }

            @Override
            public boolean hasNext() {
                if (viewIndex < viewInstances.size()) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    record.of(engine, viewInstances.getQuick(viewIndex++));
                    return true;
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
                private long[] checkpointRepair;
                private long[] checkpointTimeline;
                private LiveViewDefinition definition;
                private CairoEngine engine;
                private LiveViewInstance instance;

                public void clear() {
                    checkpointRepair = null;
                    checkpointTimeline = null;
                    definition = null;
                    engine = null;
                    instance = null;
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
                    // Share of the logical state the timeline did not have to
                    // write, so 0 means every root paid for its own complete
                    // image. NULL rather than 0 while no generation exists, which
                    // is a different statement from "shares nothing".
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
                        // The stub has a null definition and default state; every
                        // numeric column is NULL.
                        return Numbers.LONG_NULL;
                    }
                    return switch (col) {
                        case COLUMN_FLUSH_EVERY_INTERVAL -> definition.getFlushEveryInterval();
                        case COLUMN_IN_MEMORY_INTERVAL -> definition.getInMemoryInterval();
                        case COLUMN_IN_MEM_BYTES -> {
                            // Peak-sticky native footprint of the in-mem tier (sum
                            // across both N=2 slots). Reports allocated capacity, not
                            // the live row content: MemoryCARWImpl grows by page and
                            // reset() retains its pages for the next refill, so this is
                            // a high-water mark that does not shrink once a burst has
                            // sized the arena. Pair with in_mem_rows to tell a view
                            // actively buffering a large lead from one holding capacity
                            // from a past spike. Zero when the tier has not been
                            // allocated yet (LV has not refreshed, or schema is
                            // var-length and the tier is unused).
                            LiveViewInMemoryTier tier = instance.getInMemoryTier();
                            yield tier == null ? 0L : tier.footprintBytes();
                        }
                        case COLUMN_IN_MEM_ROWS -> {
                            // Rows currently held in the published (reader-visible)
                            // slot - the live logical content of the in-mem tier. Rows
                            // age out of the IN MEMORY window on a slow-path swap, which
                            // a refresh cycle drives - not wall-clock time. An idle view
                            // holds its last-refreshed rows (which can exceed the IN
                            // MEMORY window) until the next refresh trims them, so this
                            // tracks refresh activity, not window age; in_mem_bytes
                            // stays pinned at the peak arena capacity regardless. Zero
                            // before the first refresh allocates the tier.
                            LiveViewInMemoryTier tier = instance.getInMemoryTier();
                            yield tier == null ? 0L : tier.publishedRowCount();
                        }
                        case COLUMN_LAG_SEQTXN -> {
                            // base.sequencer.head - last_processed. The base token can be
                            // transiently unresolved on a replica whose LV registered before
                            // its base table downloaded (the refresh scan heals it); report
                            // an unknown lag rather than NPE into the tracker lookup.
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
                            // Now minus the wall-clock of the last successful flush.
                            // lastFlushTimeUs is the
                            // closest proxy we keep — the LV refresh runs immediately after a
                            // base commit it can see, so this approximates "now - timestamp of
                            // last processed base commit" for both caught-up and lagging views.
                            long lastFlushUs = instance.getLastFlushTimeUs();
                            if (lastFlushUs == Numbers.LONG_NULL) {
                                yield Numbers.LONG_NULL;
                            }
                            long nowUs = engine.getConfiguration().getMicrosecondClock().getTicks();
                            yield Math.max(0, nowUs - lastFlushUs);
                        }
                        case COLUMN_LAST_PROCESSED_SEQTXN -> instance.getLastProcessedSeqTxn();
                        case COLUMN_APPLIED_WATERMARK -> instance.getStateReader().getAppliedWatermark();
                        case COLUMN_LV_CONSUMED_SEQTXN -> instance.getStateReader().getLvConsumedSeqTxn();
                        // Shape of the newest published timeline generation, read
                        // as one tuple so a fresh generation number cannot pair
                        // with the previous generation's byte totals. Every field
                        // is NULL for a view holding no published generation.
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
                            // Logical minus physical, floored at zero: a timeline
                            // whose metadata outweighs the state it describes
                            // shares nothing rather than a negative amount.
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
                            // Generations the purge floor sits behind the current
                            // one. The A/B pair keeps the previous generation as
                            // its recovery fallback, so 1 is the healthy value and
                            // a growing figure means retirement has stalled.
                            final long current = checkpointTimeline[LiveViewInstance.CHECKPOINT_TIMELINE_GENERATION];
                            final long oldest =
                                    checkpointTimeline[LiveViewInstance.CHECKPOINT_TIMELINE_OLDEST_RETAINED_GENERATION];
                            yield current == Numbers.LONG_NULL || oldest == Numbers.LONG_NULL
                                    ? Numbers.LONG_NULL
                                    : Math.max(0, current - oldest);
                        }
                        // What the last lifecycle reconciliation's purge sweep found
                        // while walking the catalogue. LONG_NULL until one has run.
                        case COLUMN_CHECKPOINT_DATA_SEGMENT_COUNT -> instance.getCheckpointDataSegmentCount();
                        case COLUMN_CHECKPOINT_OBSOLETE_SEGMENT_BYTES -> instance.getCheckpointObsoleteSegmentBytes();
                        // Seal / restore cost. The timings are LONG_NULL until the
                        // event first runs (no root sealed yet / view never
                        // restored), which passes through as NULL.
                        case COLUMN_CHECKPOINT_LAST_WRITE_MICROS -> instance.getHeadCheckpointWriteMicros();
                        case COLUMN_CHECKPOINT_LAST_RESTORE_MICROS -> instance.getHeadCheckpointRestoreMicros();
                        case COLUMN_CHECKPOINT_LAST_WRITE_NEW_BYTES -> {
                            yield checkpointTimeline[LiveViewInstance.CHECKPOINT_TIMELINE_GENERATION] == Numbers.LONG_NULL
                                    ? Numbers.LONG_NULL
                                    : checkpointTimeline[LiveViewInstance.CHECKPOINT_TIMELINE_LAST_WRITE_NEW_BYTES];
                        }
                        case COLUMN_CHECKPOINT_LAST_LOOKUP_DEPTH -> instance.getCheckpointLastLookupDepth();
                        // Bounds of a localized repair suspended across refresh
                        // turns, read as one tuple. All NULL when none is in
                        // flight; the high bound is also NULL for a repair that
                        // converges only at EOF, which is a tag rather than a
                        // timestamp.
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
                        // Cadence seals this view has failed since the process started.
                        // A growing value is the only in-band signal that the view is
                        // serving correct results over stale restart-recovery state:
                        // the refresh job swallows the fault, and once the streak
                        // exhausts its budget the timeline is retired and the WAL purge
                        // floor arms released. In-memory counter, resets on restart.
                        case COLUMN_CHECKPOINT_SEAL_FAILURES -> instance.getCheckpointSealFailures();
                        // Base rows the O3 replay paths scanned (>= the emit counters
                        // above; a WHERE filter makes scan exceed emit). In-memory
                        // counter, resets on restart.
                        case COLUMN_O3_REPLAY_SCAN_ROWS -> instance.getO3ReplayScanRows();
                        // START FROM BEGINNING has no lower bound and persists
                        // LONG_NULL, which passes through as NULL.
                        case COLUMN_VIEW_LOWER_BOUND_TIMESTAMP -> toMicros(definition.getViewLowerBoundTimestamp());
                        case COLUMN_WRITER_STALL_MICROS -> {
                            // Current uninterrupted stall duration.
                            // writerStallStartUs is set when the in-mem tier's slow-path
                            // tryAcquireWrite fails (both slots reader-pinned); cleared on
                            // the next successful publish. Zero when not stalled.
                            long stallStart = instance.getWriterStallStartUs();
                            if (stallStart == Numbers.LONG_NULL) {
                                yield 0L;
                            }
                            long nowUs = engine.getConfiguration().getMicrosecondClock().getTicks();
                            yield Math.max(0, nowUs - stallStart);
                        }
                        // Real target while the sweep is in progress; LONG_NULL
                        // once the sweep completes and the state flips to ACTIVE
                        // (the field is wiped on the SEEDING -> ACTIVE flip).
                        case COLUMN_SEED_TARGET_SEQTXN -> instance.getStateReader().getSeedTargetSeqTxn();
                        // Count of late O3 rows rejected for falling below
                        // viewLowerBoundTimestamp. In-memory counter, resets on
                        // restart.
                        case COLUMN_O3_REJECTED_COUNT -> instance.getO3RejectedCount();
                        // Count of in-order (forward-append) rows dropped for
                        // falling below viewLowerBoundTimestamp - back-dated /
                        // pre-CREATE data the floor excludes. In-memory counter,
                        // resets on restart. Disjoint from o3_rejected_count.
                        case COLUMN_BELOW_LOWER_BOUND_COUNT -> instance.getBelowLowerBoundCount();
                        // Rows re-emitted by bounded resume-from-anchor O3 replays -
                        // "the win": each replay stays bounded to the tail above the
                        // logical boundary it resumed from rather than recomputing the
                        // whole view. In-memory counter, resets on restart.
                        case COLUMN_O3_RESUME_REPLAY_ROWS -> instance.getO3ResumeReplayRows();
                        // Rows re-emitted by boundary-rebuild O3 replays - the residual
                        // O(view age) fallback taken when the timeline holds no boundary
                        // below the late row. In-memory counter, resets on restart.
                        // Disjoint from o3_resume_replay_rows.
                        case COLUMN_O3_BOUNDARY_REPLAY_ROWS -> instance.getO3BoundaryReplayRows();
                        // Every numeric column the metadata declares has an arm above.
                        // A column added without one reads as NULL rather than as 0,
                        // which for the TIMESTAMP columns would render 1970-01-01.
                        default -> Numbers.LONG_NULL;
                    };
                }

                @Override
                public CharSequence getStrA(int col) {
                    if (instance.isStub()) {
                        // The stub has a null definition; surface only the name
                        // (from the token) and the status, NULL for the rest.
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
                        case COLUMN_INVALIDATION_REASON -> instance.getInvalidationReason();
                        // The dependency plans a localized repair would union, read off
                        // the compiled SELECT. NULL until the view compiles one.
                        case COLUMN_CHECKPOINT_REPAIR_PLAN ->
                                getRepairPlan(instance.getCheckpointRepairDependencyPlans());
                        // What the last repair actually did, and why it read more than
                        // its plan admits. Both NULL until the view runs one; the denial
                        // stays NULL for a repair that read exactly its localized
                        // interval.
                        case COLUMN_CHECKPOINT_REPAIR_LAST_DISPOSITION -> LiveViewCheckpointRepairPlan.dispositionName(
                                instance.getCheckpointRepairLastDisposition(),
                                instance.getCheckpointRepairLastDenialReason()
                        );
                        case COLUMN_CHECKPOINT_REPAIR_LAST_DENIAL -> LiveViewCheckpointRepairPlan.denialReasonName(
                                instance.getCheckpointRepairLastDenialReason()
                        );
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
                    // Snapshot both tuples once per row. The writer publishes each of
                    // them by replacing the whole array, so one read per row gives the
                    // columns a consistent view; a read per column would let a fresh
                    // generation number pair with the previous generation's byte
                    // totals, which is what the column comments promise it cannot.
                    this.checkpointRepair = instance.getCheckpointRepair();
                    this.checkpointTimeline = instance.getCheckpointTimeline();
                }

                /**
                 * Converts a timestamp held in base-table units to the
                 * TIMESTAMP_MICRO the catalogue declares - identity for MICRO
                 * bases, rounding down to the MICRO grid for NS bases. LONG_NULL
                 * passes through untouched rather than through the driver, whose
                 * rescale would turn the sentinel into an arbitrary timestamp.
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
            METADATA = metadata;
        }
    }
}

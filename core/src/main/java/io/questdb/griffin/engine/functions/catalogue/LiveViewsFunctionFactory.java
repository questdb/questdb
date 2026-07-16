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
 * catches up. Three head-checkpoint columns trail the documented column set as
 * additional debug surface for head checkpoints.
 * {@code o3_resume_replay_rows} and {@code o3_boundary_replay_rows} trail after
 * them as O3-replay observability: they split the rows an O3 re-emits by path -
 * bounded resume-from-anchor replays versus the residual O(view age) boundary
 * rebuild - so the two are disjoint. A resume count that grows while the boundary
 * count stays flat is the retained-checkpoint ring bounding O3 cost as intended;
 * a growing boundary count flags late rows that predate the whole ring.
 * Five {@code checkpoint_ring_*} columns close that surface out with the durable
 * ring's own state - what restart recovered, and what the next restart would
 * recover. {@code checkpoint_ring_recovered_entries} and
 * {@code checkpoint_ring_recovery_fallback_count} report the trust verdict
 * {@code _checkpoints/_ring} got at restart; the two {@code manifest} columns
 * report the current durable claim, so an operator can watch
 * {@code checkpoint_ring_manifest_covered_seqtxn} track {@code applied_watermark}
 * and know the next restart will trust the ring rather than wait for it to not.
 * {@code checkpoint_ring_manifest_dirty} flags an in-memory ring that has run
 * ahead of the manifest on disk. All five are inert - NULL, zero, false - while
 * {@code cairo.live.view.checkpoint.ring.durable.enabled} is off, which publishes
 * and recovers nothing.
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
        private static final int COLUMN_SEED_TARGET_SEQTXN = 21;
        private static final int COLUMN_BASE_TABLE_NAME = 2;
        private static final int COLUMN_BELOW_LOWER_BOUND_COUNT = 13;
        private static final int COLUMN_CHECKPOINT_RING_MANIFEST_COVERED_SEQTXN = 29;
        private static final int COLUMN_CHECKPOINT_RING_MANIFEST_DIRTY = 30;
        private static final int COLUMN_CHECKPOINT_RING_MANIFEST_GENERATION = 28;
        private static final int COLUMN_CHECKPOINT_RING_RECOVERED_ENTRIES = 27;
        private static final int COLUMN_CHECKPOINT_RING_RECOVERY_FALLBACK_COUNT = 31;
        private static final int COLUMN_FLUSH_EVERY_INTERVAL = 6;
        private static final int COLUMN_FLUSH_EVERY_INTERVAL_UNIT = 7;
        private static final int COLUMN_HEAD_CHECKPOINT_LV_SEQTXN = 22;
        private static final int COLUMN_HEAD_CHECKPOINT_MAX_TS = 23;
        private static final int COLUMN_HEAD_CHECKPOINT_STATE_BYTES = 24;
        private static final int COLUMN_INVALIDATION_REASON = 5;
        private static final int COLUMN_IN_MEMORY_INTERVAL = 8;
        private static final int COLUMN_IN_MEMORY_INTERVAL_UNIT = 9;
        private static final int COLUMN_IN_MEM_BYTES = 10;
        private static final int COLUMN_IN_MEM_ROWS = 11;
        private static final int COLUMN_LAG_MICROS = 15;
        private static final int COLUMN_LAG_SEQTXN = 14;
        private static final int COLUMN_LAST_PROCESSED_SEQTXN = 16;
        private static final int COLUMN_LV_CONSUMED_SEQTXN = 18;
        private static final int COLUMN_O3_BOUNDARY_REPLAY_ROWS = 26;
        private static final int COLUMN_O3_REJECTED_COUNT = 12;
        private static final int COLUMN_O3_RESUME_REPLAY_ROWS = 25;
        private static final int COLUMN_VIEW_LOWER_BOUND_TIMESTAMP = 19;
        private static final int COLUMN_VIEW_NAME = 0;
        private static final int COLUMN_VIEW_SQL = 3;
        private static final int COLUMN_VIEW_STATUS = 4;
        private static final int COLUMN_VIEW_TABLE_DIR_NAME = 1;
        private static final int COLUMN_WRITER_STALL_MICROS = 20;
        private static final RecordMetadata METADATA;
        private final LiveViewsListCursor cursor = new LiveViewsListCursor();

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
                private LiveViewDefinition definition;
                private CairoEngine engine;
                private LiveViewInstance instance;

                @Override
                public boolean getBool(int col) {
                    // Whether the last _checkpoints/_ring publication failed, leaving
                    // the in-memory ring ahead of the manifest on disk. Diagnostic
                    // only: it never gates a replay, and it self-corrects on the next
                    // successful publication. Needs no stub guard - the field reads
                    // false there, which is the truth (a stub never publishes) rather
                    // than a default standing in for one, and BOOLEAN has no NULL to
                    // report the difference with anyway.
                    return col == COLUMN_CHECKPOINT_RING_MANIFEST_DIRTY && instance.isCheckpointRingDirty();
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
                        case COLUMN_HEAD_CHECKPOINT_LV_SEQTXN -> instance.getHeadCheckpointLvSeqTxn();
                        case COLUMN_HEAD_CHECKPOINT_MAX_TS -> {
                            // Stored in base-table timestamp units; surface as TIMESTAMP_MICRO,
                            // identity for MICRO bases and NS-to-MICRO rounding for NS bases.
                            // LONG_NULL passes through unchanged so operators see a clear
                            // "no head" sentinel.
                            long raw = instance.getHeadCheckpointMaxTs();
                            yield raw == Numbers.LONG_NULL ? Numbers.LONG_NULL :
                                    ColumnType
                                            .getTimestampDriver(definition.getBaseTimestampType())
                                            .toMicros(raw);
                        }
                        case COLUMN_HEAD_CHECKPOINT_STATE_BYTES -> instance.getHeadCheckpointStateBytes();
                        case COLUMN_VIEW_LOWER_BOUND_TIMESTAMP -> {
                            // Persisted in base-table units; convert back to
                            // TIMESTAMP_MICRO per the catalogue column's declared type. Identity for
                            // MICRO bases; rounds NS bases down to the MICRO grid.
                            // START FROM BEGINNING has no lower bound and persists LONG_NULL, which
                            // passes through as NULL rather than through the driver, whose rescale
                            // would turn the sentinel into an arbitrary timestamp.
                            long raw = definition.getViewLowerBoundTimestamp();
                            yield raw == Numbers.LONG_NULL ? Numbers.LONG_NULL :
                                    ColumnType
                                            .getTimestampDriver(definition.getBaseTimestampType())
                                            .toMicros(raw);
                        }
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
                        // sealed anchor rather than recomputing the whole view.
                        // In-memory counter, resets on restart.
                        case COLUMN_O3_RESUME_REPLAY_ROWS -> instance.getO3ResumeReplayRows();
                        // Rows re-emitted by boundary-rebuild O3 replays - the residual
                        // O(view age) fallback taken when the late row predates the whole
                        // retained-checkpoint ring. In-memory counter, resets on restart.
                        // Disjoint from o3_resume_replay_rows.
                        case COLUMN_O3_BOUNDARY_REPLAY_ROWS -> instance.getO3BoundaryReplayRows();
                        // Entries the restart rehydrated into the retained-checkpoint
                        // ring from a trusted _checkpoints/_ring manifest, after
                        // pruning to the running retention budget - the anchors this
                        // process came back with. NULL when no recovery decision was
                        // made: the view never restored (no head checkpoint), or the
                        // durable ring is disabled and no manifest was ever read.
                        // Zero splits two ways, and checkpoint_ring_recovery_fallback_count
                        // is what tells them apart: a fallback recovered nothing,
                        // while a trusted manifest that listed nothing withheld
                        // anchors without condemning the sweep's head.
                        case COLUMN_CHECKPOINT_RING_RECOVERED_ENTRIES -> instance.getCheckpointRingRecoveredEntries();
                        // Generation the last successful _checkpoints/_ring publication
                        // stamped, or the one recovery adopted from the manifest it
                        // trusted - the counter continues across restarts rather than
                        // restarting at 1. Zero means this process has neither
                        // published nor adopted, so a real manifest always reads >= 1.
                        case COLUMN_CHECKPOINT_RING_MANIFEST_GENERATION -> instance.getLastPublishedRingGeneration();
                        // coveredBaseSeqTxn of that manifest: the base seqTxn at which
                        // every listed entry is proven sealed. This is what the next
                        // restart compares against the reconciled applied floor, so a
                        // value trailing applied_watermark is the view soaking with a
                        // manifest that would not be trusted. NULL until this process
                        // publishes or adopts one.
                        case COLUMN_CHECKPOINT_RING_MANIFEST_COVERED_SEQTXN ->
                                instance.getLastPublishedRingCoveredBaseSeqTxn();
                        // Restarts whose ring recovery declined to trust a manifest and
                        // fell back to the highest checkpoint alone, each costing the
                        // first in-retention O3 after it a boundary rebuild. Counts
                        // only with the durable ring enabled - with the flag off there
                        // is no manifest to decline. Recovery is single-shot per view
                        // per process, so this reads 0 or 1; sum() it across the
                        // catalogue for a deployment-wide tally.
                        case COLUMN_CHECKPOINT_RING_RECOVERY_FALLBACK_COUNT ->
                                instance.getCheckpointRingRecoveryFallbackCount();
                        default -> 0;
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
            metadata.add(new TableColumnMetadata("head_checkpoint_lv_seqtxn", ColumnType.LONG));            // 22
            metadata.add(new TableColumnMetadata("head_checkpoint_max_ts", ColumnType.TIMESTAMP_MICRO));    // 23
            metadata.add(new TableColumnMetadata("head_checkpoint_state_bytes", ColumnType.LONG));          // 24
            metadata.add(new TableColumnMetadata("o3_resume_replay_rows", ColumnType.LONG));                // 25
            metadata.add(new TableColumnMetadata("o3_boundary_replay_rows", ColumnType.LONG));              // 26
            metadata.add(new TableColumnMetadata("checkpoint_ring_recovered_entries", ColumnType.LONG));    // 27
            metadata.add(new TableColumnMetadata("checkpoint_ring_manifest_generation", ColumnType.LONG));  // 28
            metadata.add(new TableColumnMetadata("checkpoint_ring_manifest_covered_seqtxn", ColumnType.LONG)); // 29
            metadata.add(new TableColumnMetadata("checkpoint_ring_manifest_dirty", ColumnType.BOOLEAN));    // 30
            metadata.add(new TableColumnMetadata("checkpoint_ring_recovery_fallback_count", ColumnType.LONG)); // 31
            METADATA = metadata;
        }
    }
}

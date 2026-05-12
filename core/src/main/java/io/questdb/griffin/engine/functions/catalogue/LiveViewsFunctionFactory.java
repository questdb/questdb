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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.lv.LiveViewDefinition;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
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
 * Phase 1 {@code live_views()} catalogue. Exposes per-view operator state derived
 * from {@link LiveViewInstance}'s in-memory mirror of {@code _lv} + {@code _lv.s}.
 * <p>
 * Phase 1 omits checkpoint columns (no {@code .cp}), backfill columns (BACKFILL is
 * rejected at CREATE), {@code symbol_translation_size} (no T table yet),
 * {@code o3_rejected_count} (O3 rejects land in a later phase), and
 * {@code in_mem_bytes} (no in-mem tier in Phase 1a). {@code writer_stall_micros}
 * is exposed but always zero in Phase 1a since stalls are an in-mem-tier concern
 * and that tier doesn't exist yet (Phase 1b will populate the field).
 * <p>
 * {@code last_processed_seqtxn} and {@code applied_watermark} are surfaced as
 * debug columns beyond the RFC's V1 set; both are useful for operators tracking
 * refresh worker progress before the corresponding {@code lvConsumed} flow
 * catches up.
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
        private static final int COLUMN_APPLIED_WATERMARK = 14;
        private static final int COLUMN_BASE_TABLE_NAME = 2;
        private static final int COLUMN_FLUSH_EVERY_INTERVAL = 5;
        private static final int COLUMN_FLUSH_EVERY_INTERVAL_UNIT = 6;
        private static final int COLUMN_INVALIDATION_REASON = 4;
        private static final int COLUMN_IN_MEMORY_INTERVAL = 7;
        private static final int COLUMN_IN_MEMORY_INTERVAL_UNIT = 8;
        private static final int COLUMN_IN_MEM_BYTES = 9;
        private static final int COLUMN_LAG_MICROS = 12;
        private static final int COLUMN_LAG_SEQTXN = 11;
        private static final int COLUMN_LAST_PROCESSED_SEQTXN = 13;
        private static final int COLUMN_LV_CONSUMED_SEQTXN = 15;
        private static final int COLUMN_VIEW_LOWER_BOUND_TIMESTAMP = 16;
        private static final int COLUMN_VIEW_NAME = 0;
        private static final int COLUMN_VIEW_SQL = 10;
        private static final int COLUMN_VIEW_STATUS = 3;
        private static final int COLUMN_VIEW_TABLE_DIR_NAME = 1;
        private static final int COLUMN_WRITER_STALL_MICROS = 17;
        private static final RecordMetadata METADATA;
        private final LiveViewsListCursor cursor = new LiveViewsListCursor();

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
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
                public long getLong(int col) {
                    return switch (col) {
                        case COLUMN_FLUSH_EVERY_INTERVAL -> definition.getFlushEveryInterval();
                        case COLUMN_IN_MEMORY_INTERVAL -> definition.getInMemoryInterval();
                        case COLUMN_IN_MEM_BYTES -> {
                            // RFC 123 §"Catalogue function live_views()": current in-mem
                            // tier footprint (sum across both N=2 slots). Zero when the
                            // tier has not been allocated yet (LV has not refreshed, or
                            // schema is var-length and the tier is unused).
                            io.questdb.cairo.lv.LiveViewInMemoryTier tier = instance.getInMemoryTier();
                            yield tier == null ? 0L : tier.footprintBytes();
                        }
                        case COLUMN_LAG_SEQTXN -> {
                            // base.sequencer.head - last_processed
                            SeqTxnTracker tracker = engine.getTableSequencerAPI()
                                    .getTxnTracker(definition.getBaseTableToken());
                            long head = tracker.getWriterTxn();
                            long lp = instance.getLastProcessedSeqTxn();
                            yield head < 0 || lp < 0 ? Numbers.LONG_NULL : Math.max(0, head - lp);
                        }
                        case COLUMN_LAG_MICROS -> {
                            // RFC 123 §"Catalogue function live_views()": now minus the
                            // wall-clock of the last successful flush. lastFlushTimeUs is the
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
                        case COLUMN_VIEW_LOWER_BOUND_TIMESTAMP ->
                                // Persisted in base-table units (RFC 123 §"On-disk tier"); convert back to
                                // TIMESTAMP_MICRO per the catalogue column's declared type. Identity for
                                // MICRO bases; rounds NS bases down to the MICRO grid (RFC §"Catalogue
                                // function live_views()").
                                io.questdb.cairo.ColumnType
                                        .getTimestampDriver(definition.getBaseTimestampType())
                                        .toMicros(definition.getViewLowerBoundTimestamp());
                        case COLUMN_WRITER_STALL_MICROS -> {
                            // RFC 123 §"Stall behavior": current uninterrupted stall duration.
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
                        default -> 0;
                    };
                }

                @Override
                public CharSequence getStrA(int col) {
                    return switch (col) {
                        case COLUMN_VIEW_NAME -> definition.getViewName();
                        case COLUMN_VIEW_TABLE_DIR_NAME -> instance.getLiveViewToken().getDirName();
                        case COLUMN_BASE_TABLE_NAME -> definition.getBaseTableName();
                        case COLUMN_VIEW_STATUS -> instance.getLifecycleState().catalogueName();
                        case COLUMN_FLUSH_EVERY_INTERVAL_UNIT -> getIntervalUnit(definition.getFlushEveryIntervalUnit());
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
            metadata.add(new TableColumnMetadata("view_status", ColumnType.STRING));                        // 3
            metadata.add(new TableColumnMetadata("invalidation_reason", ColumnType.STRING));                // 4
            metadata.add(new TableColumnMetadata("flush_every_interval", ColumnType.LONG));                 // 5
            metadata.add(new TableColumnMetadata("flush_every_interval_unit", ColumnType.STRING));          // 6
            metadata.add(new TableColumnMetadata("in_memory_interval", ColumnType.LONG));                   // 7
            metadata.add(new TableColumnMetadata("in_memory_interval_unit", ColumnType.STRING));            // 8
            metadata.add(new TableColumnMetadata("in_mem_bytes", ColumnType.LONG));                         // 9
            metadata.add(new TableColumnMetadata("view_sql", ColumnType.STRING));                           // 10
            metadata.add(new TableColumnMetadata("lag_seqtxn", ColumnType.LONG));                           // 11
            metadata.add(new TableColumnMetadata("lag_micros", ColumnType.LONG));                           // 12
            metadata.add(new TableColumnMetadata("last_processed_seqtxn", ColumnType.LONG));                // 13
            metadata.add(new TableColumnMetadata("applied_watermark", ColumnType.LONG));                    // 14
            metadata.add(new TableColumnMetadata("lv_consumed_seqtxn", ColumnType.LONG));                   // 15
            metadata.add(new TableColumnMetadata("view_lower_bound_timestamp", ColumnType.TIMESTAMP_MICRO));// 16
            metadata.add(new TableColumnMetadata("writer_stall_micros", ColumnType.LONG));                  // 17
            METADATA = metadata;
        }
    }
}

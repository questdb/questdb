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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

public class MatViewsFunctionFactory implements FunctionFactory {
    private static final Log LOG = LogFactory.getLog(MatViewsFunctionFactory.class);

    public static String getIntervalUnit(char unit) {
        switch (unit) {
            case 'm':
                return "MINUTE";
            case 'h':
                return "HOUR";
            case 'd':
                return "DAY";
            case 'w':
                return "WEEK";
            case 'y':
                return "YEAR";
            case 'M':
                return "MONTH";
            default:
                return null;
        }
    }

    @Override
    public String getSignature() {
        return "materialized_views()";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        return new CursorFunction(new MatViewsCursorFactory(sqlExecutionContext.getCairoEngine())) {
            @Override
            public boolean isRuntimeConstant() {
                return true;
            }
        };
    }

    private static class MatViewsCursorFactory implements RecordCursorFactory {
        private static final int COLUMN_VIEW_NAME = 0;
        private static final int COLUMN_REFRESH_TYPE = COLUMN_VIEW_NAME + 1;
        private static final int COLUMN_BASE_TABLE_NAME = COLUMN_REFRESH_TYPE + 1;
        private static final int COLUMN_LAST_REFRESH_START_TIMESTAMP = COLUMN_BASE_TABLE_NAME + 1;
        private static final int COLUMN_LAST_REFRESH_FINISH_TIMESTAMP = COLUMN_LAST_REFRESH_START_TIMESTAMP + 1;
        private static final int COLUMN_VIEW_SQL = COLUMN_LAST_REFRESH_FINISH_TIMESTAMP + 1;
        private static final int COLUMN_TABLE_DIR_NAME = COLUMN_VIEW_SQL + 1;
        private static final int COLUMN_INVALIDATION_REASON = COLUMN_TABLE_DIR_NAME + 1;
        private static final int COLUMN_VIEW_STATUS = COLUMN_INVALIDATION_REASON + 1;
        private static final int COLUMN_REFRESH_PERIOD_HI = COLUMN_VIEW_STATUS + 1;
        private static final int COLUMN_REFRESH_BASE_TABLE_TXN = COLUMN_REFRESH_PERIOD_HI + 1;
        private static final int COLUMN_APPLIED_BASE_TABLE_TXN = COLUMN_REFRESH_BASE_TABLE_TXN + 1;
        private static final int COLUMN_REFRESH_LIMIT = COLUMN_APPLIED_BASE_TABLE_TXN + 1;
        private static final int COLUMN_REFRESH_LIMIT_UNIT = COLUMN_REFRESH_LIMIT + 1;
        private static final int COLUMN_TIMER_TIME_ZONE = COLUMN_REFRESH_LIMIT_UNIT + 1;
        private static final int COLUMN_TIMER_START = COLUMN_TIMER_TIME_ZONE + 1;
        private static final int COLUMN_TIMER_INTERVAL = COLUMN_TIMER_START + 1;
        private static final int COLUMN_TIMER_INTERVAL_UNIT = COLUMN_TIMER_INTERVAL + 1;
        private static final int COLUMN_PERIOD_LENGTH = COLUMN_TIMER_INTERVAL_UNIT + 1;
        private static final int COLUMN_PERIOD_LENGTH_UNIT = COLUMN_PERIOD_LENGTH + 1;
        private static final int COLUMN_PERIOD_DELAY = COLUMN_PERIOD_LENGTH_UNIT + 1;
        private static final int COLUMN_PERIOD_DELAY_UNIT = COLUMN_PERIOD_DELAY + 1;
        private static final RecordMetadata METADATA;
        private final ViewsListCursor cursor;

        public MatViewsCursorFactory(CairoEngine engine) {
            this.cursor = new ViewsListCursor(engine);
        }

        @Override
        public void close() {
            Misc.free(cursor);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            cursor.toTop();
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
            sink.val("materialized_views()");
        }

        private static class ViewsListCursor implements NoRandomAccessRecordCursor {
            private final CairoEngine engine;
            private final Path path;
            private final MatViewsRecord record = new MatViewsRecord();
            private final BlockFileReader viewStateFileReader;
            private final MatViewStateReader viewStateReader = new MatViewStateReader();
            private final ObjList<TableToken> viewTokens = new ObjList<>();
            private int viewIndex = 0;

            public ViewsListCursor(CairoEngine engine) {
                this.engine = engine;
                this.viewStateFileReader = new BlockFileReader(engine.getConfiguration());
                this.path = new Path();
            }

            @Override
            public void close() {
                Misc.free(path);
                Misc.free(viewStateFileReader);
            }

            @Override
            public Record getRecord() {
                return record;
            }

            @Override
            public boolean hasNext() throws DataUnavailableException {
                final CairoConfiguration configuration = engine.getConfiguration();
                path.of(configuration.getDbRoot());
                final int pathLen = path.size();

                final int n = viewTokens.size();
                for (; viewIndex < n; viewIndex++) {
                    final TableToken viewToken = viewTokens.get(viewIndex);
                    if (engine.getTableTokenIfExists(viewToken.getTableName()) != null) {
                        final MatViewDefinition viewDefinition = engine.getMatViewGraph().getViewDefinition(viewToken);
                        if (viewDefinition == null) {
                            continue; // mat view was dropped concurrently
                        }

                        viewStateReader.clear();
                        final boolean isMatViewStateExists = TableUtils.isMatViewStateFileExists(configuration, path, viewToken.getDirName());
                        if (isMatViewStateExists) {
                            try {
                                viewStateFileReader.of(path.trimTo(pathLen).concat(viewToken.getDirName()).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME).$());
                                viewStateReader.of(viewStateFileReader, viewToken);
                            } catch (CairoException e) {
                                LOG.info().$("could not read materialized view state file [view=").$(viewToken)
                                        .$(", msg=").$safe(e.getFlyweightMessage())
                                        .$(", errno=").$(e.getErrno())
                                        .I$();
                                continue;
                            }
                        }

                        final long lastPeriodHi = viewDefinition.getBaseTableTimestampDriver().toMicros(viewStateReader.getLastPeriodHi());
                        final long lastRefreshedBaseTxn = viewStateReader.getLastRefreshBaseTxn();
                        final long lastRefreshTimestampUs = viewStateReader.getLastRefreshTimestampUs();

                        final TableToken baseTableToken = engine.getTableTokenIfExists(viewDefinition.getBaseTableName());
                        // Read base table txn after mat view's last refreshed txn to avoid
                        // showing obsolete base table txn.
                        final long lastAppliedBaseTxn = baseTableToken != null
                                ? engine.getTableSequencerAPI().getTxnTracker(baseTableToken).getWriterTxn() : -1;

                        final int refreshLimitHoursOrMonths = viewDefinition.getRefreshLimitHoursOrMonths();
                        final int periodLength = viewDefinition.getPeriodLength();
                        final char periodLengthUnit = viewDefinition.getPeriodLengthUnit();
                        final int periodDelay = viewDefinition.getPeriodDelay();
                        final char periodDelayUnit = viewDefinition.getPeriodDelayUnit();
                        long timerStartUs = Numbers.LONG_NULL;
                        int timerInterval = 0;
                        char timerIntervalUnit = 0;
                        if (viewDefinition.getRefreshType() == MatViewDefinition.REFRESH_TYPE_TIMER || periodLength > 0) {
                            timerStartUs = viewDefinition.getTimerStartUs();
                            timerInterval = viewDefinition.getTimerInterval();
                            timerIntervalUnit = viewDefinition.getTimerUnit();
                        }

                        final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
                        // start timestamp is not persisted
                        final long lastRefreshStartTimestamp = state != null ? state.getLastRefreshStartTimestampUs() : Numbers.LONG_NULL;

                        record.of(
                                viewDefinition,
                                lastRefreshStartTimestamp,
                                lastRefreshTimestampUs,
                                lastPeriodHi,
                                lastRefreshedBaseTxn,
                                lastAppliedBaseTxn,
                                viewStateReader.getInvalidationReason(),
                                viewStateReader.isInvalid(),
                                refreshLimitHoursOrMonths,
                                timerStartUs,
                                timerInterval,
                                timerIntervalUnit,
                                periodLength,
                                periodLengthUnit,
                                periodDelay,
                                periodDelayUnit
                        );
                        viewIndex++;
                        return true;
                    }
                }
                return false;
            }

            @Override
            public long preComputedStateSize() {
                return 0;
            }

            @Override
            public long size() throws DataUnavailableException {
                return -1;
            }

            @Override
            public void toTop() {
                viewTokens.clear();
                engine.getMatViewGraph().getViews(viewTokens);
                viewIndex = 0;
            }

            private static class MatViewsRecord implements Record {
                private final StringSink invalidationReason = new StringSink();
                private boolean invalid;
                private long lastAppliedBaseTxn;
                private long lastPeriodHi;
                private long lastRefreshFinishTimestamp;
                private long lastRefreshStartTimestamp;
                private long lastRefreshTxn;
                private int periodDelay;
                private char periodDelayUnit;
                private int periodLength;
                private char periodLengthUnit;
                private int refreshLimitHoursOrMonths;
                private int timerInterval;
                private char timerIntervalUnit;
                private long timerStart;
                private MatViewDefinition viewDefinition;

                @Override
                public int getInt(int col) {
                    switch (col) {
                        case COLUMN_REFRESH_LIMIT:
                            return TablesFunctionFactory.getTtlValue(refreshLimitHoursOrMonths);
                        case COLUMN_TIMER_INTERVAL:
                            return timerInterval;
                        case COLUMN_PERIOD_LENGTH:
                            return periodLength;
                        case COLUMN_PERIOD_DELAY:
                            return periodDelay;
                        default:
                            return 0;
                    }
                }

                @Override
                public long getLong(int col) {
                    switch (col) {
                        case COLUMN_LAST_REFRESH_START_TIMESTAMP:
                            return lastRefreshStartTimestamp;
                        case COLUMN_LAST_REFRESH_FINISH_TIMESTAMP:
                            return lastRefreshFinishTimestamp;
                        case COLUMN_REFRESH_PERIOD_HI:
                            return lastPeriodHi;
                        case COLUMN_REFRESH_BASE_TABLE_TXN:
                            return lastRefreshTxn;
                        case COLUMN_APPLIED_BASE_TABLE_TXN:
                            return lastAppliedBaseTxn;
                        case COLUMN_TIMER_START:
                            return timerStart;
                        default:
                            return 0;
                    }
                }

                @Override
                public CharSequence getStrA(int col) {
                    switch (col) {
                        case COLUMN_VIEW_NAME:
                            return viewDefinition.getMatViewToken().getTableName();
                        case COLUMN_REFRESH_TYPE:
                            switch (viewDefinition.getRefreshType()) {
                                case MatViewDefinition.REFRESH_TYPE_IMMEDIATE:
                                    return "immediate";
                                case MatViewDefinition.REFRESH_TYPE_TIMER:
                                    return "timer";
                                case MatViewDefinition.REFRESH_TYPE_MANUAL:
                                    return "manual";
                                default:
                                    return "unknown";
                            }
                        case COLUMN_BASE_TABLE_NAME:
                            return viewDefinition.getBaseTableName();
                        case COLUMN_VIEW_SQL:
                            return viewDefinition.getMatViewSql();
                        case COLUMN_TABLE_DIR_NAME:
                            return viewDefinition.getMatViewToken().getDirName();
                        case COLUMN_VIEW_STATUS:
                            return getViewStatus();
                        case COLUMN_INVALIDATION_REASON:
                            return invalidationReason.length() > 0 ? invalidationReason : null;
                        case COLUMN_REFRESH_LIMIT_UNIT:
                            if (refreshLimitHoursOrMonths == 0) {
                                return null;
                            }
                            return TablesFunctionFactory.getTtlUnit(refreshLimitHoursOrMonths);
                        case COLUMN_TIMER_INTERVAL_UNIT:
                            return getIntervalUnit(timerIntervalUnit);
                        case COLUMN_TIMER_TIME_ZONE:
                            return viewDefinition.getTimerTimeZone();
                        case COLUMN_PERIOD_LENGTH_UNIT:
                            return getIntervalUnit(periodLengthUnit);
                        case COLUMN_PERIOD_DELAY_UNIT:
                            return getIntervalUnit(periodDelayUnit);
                        default:
                            return null;
                    }
                }

                @Override
                public CharSequence getStrB(int col) {
                    return getStrA(col);
                }

                @Override
                public int getStrLen(int col) {
                    return TableUtils.lengthOf(getStrA(col));
                }

                public void of(
                        MatViewDefinition viewDefinition,
                        long lastRefreshStartTimestamp,
                        long lastRefreshFinishTimestamp,
                        long lastPeriodHi,
                        long lastRefreshTxn,
                        long lastAppliedBaseTxn,
                        CharSequence invalidationReason,
                        boolean invalid,
                        int refreshLimitHoursOrMonths,
                        long timerStart,
                        int timerInterval,
                        char timerIntervalUnit,
                        int periodLength,
                        char periodLengthUnit,
                        int periodDelay,
                        char periodDelayUnit
                ) {
                    this.viewDefinition = viewDefinition;
                    this.lastRefreshStartTimestamp = lastRefreshStartTimestamp;
                    this.lastRefreshFinishTimestamp = lastRefreshFinishTimestamp;
                    this.lastPeriodHi = lastPeriodHi;
                    this.lastRefreshTxn = lastRefreshTxn;
                    this.lastAppliedBaseTxn = lastAppliedBaseTxn;
                    this.invalidationReason.clear();
                    this.invalidationReason.put(invalidationReason);
                    this.invalid = invalid;
                    this.refreshLimitHoursOrMonths = refreshLimitHoursOrMonths;
                    this.timerStart = timerStart;
                    this.timerInterval = timerInterval;
                    this.timerIntervalUnit = timerIntervalUnit;
                    this.periodLength = periodLength;
                    this.periodLengthUnit = periodLengthUnit;
                    this.periodDelay = periodDelay;
                    this.periodDelayUnit = periodDelayUnit;
                }

                private CharSequence getViewStatus() {
                    if (invalid) {
                        return "invalid";
                    }
                    return (lastRefreshStartTimestamp != Numbers.LONG_NULL && lastRefreshStartTimestamp > lastRefreshFinishTimestamp)
                            ? "refreshing"
                            : "valid";
                }
            }
        }

        static {
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("view_name", ColumnType.STRING));
            metadata.add(new TableColumnMetadata("refresh_type", ColumnType.STRING));
            metadata.add(new TableColumnMetadata("base_table_name", ColumnType.STRING));
            metadata.add(new TableColumnMetadata("last_refresh_start_timestamp", ColumnType.TIMESTAMP_MICRO));
            metadata.add(new TableColumnMetadata("last_refresh_finish_timestamp", ColumnType.TIMESTAMP_MICRO));
            metadata.add(new TableColumnMetadata("view_sql", ColumnType.STRING));
            metadata.add(new TableColumnMetadata("view_table_dir_name", ColumnType.STRING));
            metadata.add(new TableColumnMetadata("invalidation_reason", ColumnType.STRING));
            metadata.add(new TableColumnMetadata("view_status", ColumnType.STRING));
            metadata.add(new TableColumnMetadata("refresh_period_hi", ColumnType.TIMESTAMP_MICRO));
            metadata.add(new TableColumnMetadata("refresh_base_table_txn", ColumnType.LONG));
            metadata.add(new TableColumnMetadata("base_table_txn", ColumnType.LONG));
            metadata.add(new TableColumnMetadata("refresh_limit", ColumnType.INT));
            metadata.add(new TableColumnMetadata("refresh_limit_unit", ColumnType.STRING));
            metadata.add(new TableColumnMetadata("timer_time_zone", ColumnType.STRING));
            metadata.add(new TableColumnMetadata("timer_start", ColumnType.TIMESTAMP_MICRO));
            metadata.add(new TableColumnMetadata("timer_interval", ColumnType.INT));
            metadata.add(new TableColumnMetadata("timer_interval_unit", ColumnType.STRING));
            metadata.add(new TableColumnMetadata("period_length", ColumnType.INT));
            metadata.add(new TableColumnMetadata("period_length_unit", ColumnType.STRING));
            metadata.add(new TableColumnMetadata("period_delay", ColumnType.INT));
            metadata.add(new TableColumnMetadata("period_delay_unit", ColumnType.STRING));
            METADATA = metadata;
        }
    }
}

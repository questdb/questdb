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
package io.questdb.griffin.engine.table;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.lv.LiveViewDefinition;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import org.jetbrains.annotations.NotNull;

public class ShowCreateLiveViewRecordCursorFactory extends AbstractRecordCursorFactory {
    public static final int N_DDL_COL = 0;
    private static final RecordMetadata METADATA;
    protected final int tokenPosition;
    protected final TableToken viewToken;
    private final ShowCreateLiveViewCursor cursor = new ShowCreateLiveViewCursor();

    public ShowCreateLiveViewRecordCursorFactory(TableToken viewToken, int tokenPosition) {
        super(METADATA);
        this.viewToken = viewToken;
        this.tokenPosition = tokenPosition;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedTimeThrottled();
        return cursor.of(executionContext, viewToken, tokenPosition);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("show_create_live_view");
        sink.meta("of").val(viewToken.getTableName());
    }

    @Override
    protected void _close() {
        super._close();
        Misc.free(cursor);
    }

    public static class ShowCreateLiveViewCursor implements NoRandomAccessRecordCursor {
        protected final Utf8StringSink sink = new Utf8StringSink();
        private final Path path;
        private final ShowCreateLiveViewRecord record = new ShowCreateLiveViewRecord();
        protected SqlExecutionContext executionContext;
        private boolean backfillRequested;
        private char flushEveryUnit;
        private long flushEveryValue;
        private boolean hasRun;
        private char inMemoryUnit;
        private long inMemoryValue;
        private int partitionBy;
        private BlockFileReader reader;
        private String viewSql;
        private TableToken viewToken;

        public ShowCreateLiveViewCursor() {
            this.path = new Path();
        }

        @Override
        public void close() {
            sink.clear();
            Misc.free(path);
            Misc.free(reader);
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            if (!hasRun) {
                sink.clear();
                showCreateLiveView();
                hasRun = true;
                return true;
            }
            return false;
        }

        public ShowCreateLiveViewCursor of(
                SqlExecutionContext executionContext,
                TableToken viewToken,
                int tokenPosition
        ) throws SqlException {
            this.viewToken = viewToken;
            this.executionContext = executionContext;

            if (!viewToken.isLiveView()) {
                throw SqlException.$(tokenPosition, "live view expected, got table");
            }

            CairoConfiguration configuration = executionContext.getCairoEngine().getConfiguration();
            path.of(configuration.getDbRoot());
            final int pathLen = path.size();
            if (reader == null) {
                reader = new BlockFileReader(configuration);
            }
            try {
                LiveViewDefinition def = LiveViewDefinition.readFrom(
                        reader,
                        path,
                        pathLen,
                        viewToken,
                        null,
                        new io.questdb.cairo.GenericRecordMetadata()
                );
                viewSql = def.getViewSql();
                flushEveryValue = def.getFlushEveryInterval();
                flushEveryUnit = def.getFlushEveryIntervalUnit();
                inMemoryValue = def.getInMemoryInterval();
                inMemoryUnit = def.getInMemoryIntervalUnit();
                partitionBy = def.getPartitionBy();
                backfillRequested = def.getBackfillRequested();
            } catch (CairoException e) {
                throw SqlException.$(tokenPosition, "could not read live view definition [view=").put(viewToken)
                        .put(", msg=").put(e)
                        .put(']');
            } finally {
                reader.close();
            }

            toTop();
            return this;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            sink.clear();
            hasRun = false;
        }

        private void showCreateLiveView() {
            sink.putAscii("CREATE LIVE VIEW '")
                    .put(viewToken.getTableName())
                    .putAscii('\'');
            if (flushEveryValue > 0) {
                sink.putAscii(" FLUSH EVERY ").put(flushEveryValue);
                appendUnitGrammar(flushEveryUnit);
            }
            if (inMemoryValue > 0) {
                sink.putAscii(" IN MEMORY ").put(inMemoryValue);
                appendUnitGrammar(inMemoryUnit);
            }
            // Always emit PARTITION BY: the persisted value is the resolved scheme
            // (parser sentinel collapsed at CREATE), so the round-trip target is
            // whatever the LV actually has on disk. Includes the explicit
            // PARTITION BY NONE case, which would otherwise round-trip to base's
            // scheme if the clause were omitted from SHOW CREATE.
            sink.putAscii(" PARTITION BY ").put(PartitionBy.toString(partitionBy));
            if (backfillRequested) {
                sink.putAscii(" BACKFILL");
            }
            sink.putAscii(" AS (\n")
                    .put(viewSql)
                    .putAscii("\n)");
            putAdditional();
            sink.putAscii(';');
        }

        // placeholder for ent, do not remove!
        protected void putAdditional() {
        }

        /**
         * Maps the internal duration-unit char ({@code 's'}, {@code 'm'}, {@code 'h'},
         * {@code 'd'}, {@code 'T'} for milliseconds) to the grammar token
         * {@code LiveViewDefinition.parseDurationUnit} accepts. Mirrors
         * {@code parseDurationUnit}'s set so the SHOW CREATE output round-trips back
         * through CREATE LIVE VIEW.
         * <p>
         * The {@code SqlParser.displayDurationUnit} helper used in error messages
         * cannot be reused here because it returns {@code "min"} for {@code 'm'} —
         * readable in errors but rejected by the parser, which only accepts the
         * single-char {@code 'm'} form for minutes.
         */
        private void appendUnitGrammar(char unit) {
            if (unit == 'T') {
                sink.putAscii("ms");
            } else {
                sink.putAscii(unit);
            }
        }

        private class ShowCreateLiveViewRecord implements Record {

            @Override
            @NotNull
            public Utf8Sequence getVarcharA(int col) {
                if (col == N_DDL_COL) {
                    return sink;
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public Utf8Sequence getVarcharB(int col) {
                return getVarcharA(col);
            }

            @Override
            public int getVarcharSize(int col) {
                return getVarcharA(col).size();
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("ddl", ColumnType.VARCHAR));
        METADATA = metadata;
    }
}

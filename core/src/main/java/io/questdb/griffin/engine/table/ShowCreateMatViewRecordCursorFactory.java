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
package io.questdb.griffin.engine.table;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CairoTable;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.MetadataCacheReader;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import org.jetbrains.annotations.NotNull;

import static io.questdb.griffin.engine.table.ShowCreateTableRecordCursorFactory.inVolumeToSink;
import static io.questdb.griffin.engine.table.ShowCreateTableRecordCursorFactory.ttlToSink;

public class ShowCreateMatViewRecordCursorFactory extends AbstractRecordCursorFactory {
    public static final int N_DDL_COL = 0;
    private static final RecordMetadata METADATA;
    protected final TableToken tableToken;
    protected final int tokenPosition;
    private final ShowCreateMatViewCursor cursor = new ShowCreateMatViewCursor();

    public ShowCreateMatViewRecordCursorFactory(TableToken tableToken, int tokenPosition) {
        super(METADATA);
        this.tableToken = tableToken;
        this.tokenPosition = tokenPosition;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        return cursor.of(executionContext, tableToken, tokenPosition);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("show_create_materialized_view");
        sink.meta("of").val(tableToken.getTableName());
    }

    @Override
    protected void _close() {
        super._close();
        Misc.free(cursor);
    }

    public static class ShowCreateMatViewCursor implements NoRandomAccessRecordCursor {
        protected final Utf8StringSink sink = new Utf8StringSink();
        protected final MatViewDefinition viewDefinition = new MatViewDefinition();
        private final Path path;
        private final ShowCreateMatViewRecord record = new ShowCreateMatViewRecord();
        protected SqlExecutionContext executionContext;
        protected CairoTable table;
        private boolean hasRun;
        private BlockFileReader reader;
        private TableToken tableToken;

        public ShowCreateMatViewCursor() {
            this.path = new Path();
        }

        @Override
        public void close() {
            sink.clear();
            viewDefinition.clear();
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
                final CairoConfiguration config = executionContext.getCairoEngine().getConfiguration();
                showCreateMatView(config);
                hasRun = true;
                return true;
            }
            return false;
        }

        public ShowCreateMatViewCursor of(
                SqlExecutionContext executionContext,
                TableToken tableToken,
                int tokenPosition
        ) throws SqlException {
            this.tableToken = tableToken;
            this.executionContext = executionContext;

            try (MetadataCacheReader metadataRO = executionContext.getCairoEngine().getMetadataCache().readLock()) {
                this.table = metadataRO.getTable(tableToken);
                if (table == null) {
                    throw SqlException.$(tokenPosition, "table does not exist [table=")
                            .put(tableToken.getTableName()).put(']');
                } else if (!tableToken.equals(table.getTableToken())) {
                    throw TableReferenceOutOfDateException.of(tableToken);
                }
            }

            if (!tableToken.isMatView()) {
                throw SqlException.$(tokenPosition, "materialized view expected, got table");
            }

            CairoConfiguration configuration = executionContext.getCairoEngine().getConfiguration();
            path.of(configuration.getDbRoot());
            final int pathLen = path.size();
            if (reader == null) {
                reader = new BlockFileReader(configuration);
            }
            try {
                MatViewDefinition.readFrom(
                        executionContext.getCairoEngine(),
                        viewDefinition,
                        reader,
                        path,
                        pathLen,
                        tableToken
                );
            } catch (CairoException e) {
                throw SqlException.$(tokenPosition, "could not read materialized view definition [table=").put(tableToken)
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

        private void showCreateMatView(CairoConfiguration configuration) {
            sink.putAscii("CREATE MATERIALIZED VIEW '")
                    .put(tableToken.getTableName())
                    .putAscii("' WITH BASE '")
                    .put(viewDefinition.getBaseTableName());
            sink.putAscii("' REFRESH");
            if (viewDefinition.getRefreshType() == MatViewDefinition.REFRESH_TYPE_TIMER) {
                sink.putAscii(" EVERY ");
                sink.put(viewDefinition.getTimerInterval());
                sink.putAscii(viewDefinition.getTimerUnit());
                if (viewDefinition.isDeferred()) {
                    sink.putAscii(" DEFERRED");
                }
                if (viewDefinition.getPeriodLength() == 0) {
                    sink.putAscii(" START '");
                    sink.putISODate(MicrosTimestampDriver.INSTANCE, viewDefinition.getTimerStartUs());
                    if (viewDefinition.getTimerTimeZone() != null) {
                        sink.putAscii("' TIME ZONE '");
                        sink.put(viewDefinition.getTimerTimeZone());
                    }
                    sink.putAscii('\'');
                }
            } else if (viewDefinition.getRefreshType() == MatViewDefinition.REFRESH_TYPE_IMMEDIATE) {
                sink.putAscii(" IMMEDIATE");
                if (viewDefinition.isDeferred()) {
                    sink.putAscii(" DEFERRED");
                }
            } else if (viewDefinition.getRefreshType() == MatViewDefinition.REFRESH_TYPE_MANUAL) {
                sink.putAscii(" MANUAL");
                if (viewDefinition.isDeferred()) {
                    sink.putAscii(" DEFERRED");
                }
            }
            if (viewDefinition.getPeriodLength() > 0) {
                sink.putAscii(" PERIOD (LENGTH ");
                sink.put(viewDefinition.getPeriodLength());
                sink.putAscii(viewDefinition.getPeriodLengthUnit());
                if (viewDefinition.getTimerTimeZone() != null) {
                    sink.putAscii(" TIME ZONE '");
                    sink.put(viewDefinition.getTimerTimeZone());
                    sink.putAscii('\'');
                }
                if (viewDefinition.getPeriodDelay() > 0) {
                    sink.putAscii(" DELAY ");
                    sink.put(viewDefinition.getPeriodDelay());
                    sink.putAscii(viewDefinition.getPeriodDelayUnit());
                }
                sink.putAscii(')');
            }
            sink.putAscii(" AS (\n")
                    .put(viewDefinition.getMatViewSql())
                    .putAscii('\n');
            sink.putAscii(") PARTITION BY ").put(table.getPartitionByName());
            ttlToSink(table.getTtlHoursOrMonths(), sink);
            inVolumeToSink(configuration, table, sink);
            putAdditional();
            sink.putAscii(';');
        }

        // placeholder for ent, do not remove!
        protected void putAdditional() {
        }

        public class ShowCreateMatViewRecord implements Record {

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

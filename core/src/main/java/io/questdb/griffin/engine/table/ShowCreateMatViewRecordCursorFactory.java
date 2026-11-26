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
import io.questdb.cairo.CairoColumn;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CairoTable;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.MetadataCacheReader;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlUtil;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import org.jetbrains.annotations.NotNull;

import static io.questdb.griffin.engine.table.ShowCreateTableRecordCursorFactory.inVolumeToSink;

public class ShowCreateMatViewRecordCursorFactory extends AbstractRecordCursorFactory {
    public static final int N_DDL_COL = 0;
    private static final RecordMetadata METADATA;
    // the view a SHOW written in a view reads the materialized view through, see
    // SqlExecutionContext.isTableFunctionVisible()
    protected final SqlExecutionContext.TableFunctionView tableFunctionView;
    protected final TableToken tableToken;
    protected final int tokenPosition;
    private ShowCreateMatViewCursor cursor = new ShowCreateMatViewCursor();

    public ShowCreateMatViewRecordCursorFactory(
            TableToken tableToken,
            int tokenPosition,
            SqlExecutionContext.TableFunctionView tableFunctionView
    ) {
        super(METADATA);
        this.tableToken = tableToken;
        this.tokenPosition = tokenPosition;
        this.tableFunctionView = tableFunctionView;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedTimeThrottledOrYield();
        return cursor.of(executionContext, tableToken, tokenPosition, tableFunctionView);
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
        final ShowCreateMatViewCursor cursor = this.cursor;
        this.cursor = null;
        Throwable failure = null;
        try {
            super._close();
        } catch (Throwable th) {
            failure = th;
        }
        failure = Misc.freeBestEffort(failure, cursor);
        CairoException.rethrowCleanupFailure(failure);
    }

    public static class ShowCreateMatViewCursor implements NoRandomAccessRecordCursor {
        protected final Utf8StringSink sink = new Utf8StringSink();
        protected final MatViewDefinition viewDefinition = new MatViewDefinition();
        private final Path path;
        private final ShowCreateMatViewRecord record = new ShowCreateMatViewRecord();
        // the output columns of the SQL that SHOW CREATE prints, see SqlCompiler.addExplicitColumnNames()
        private final GenericRecordMetadata sqlColumns = new GenericRecordMetadata();
        private final ObjList<CharSequence> viewColumnNames = new ObjList<>();
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
            viewColumnNames.clear();
            sqlColumns.clear();
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
                int tokenPosition,
                SqlExecutionContext.TableFunctionView tableFunctionView
        ) throws SqlException {
            // Compilation already hid the view from a principal who may not see it, but the
            // factory can come from a select cache that another principal populated.
            if (!executionContext.isTableFunctionVisible(tableToken, tableFunctionView)) {
                throw SqlException.matViewDoesNotExist(tokenPosition, tableToken.getTableName());
            }
            this.tableToken = tableToken;
            this.executionContext = executionContext;

            // The token is resolved from the synchronously loaded registry, but the
            // metadata cache is hydrated lazily; hydrate this materialized view on demand
            // (matviews have a _meta file and are cached) so we do not report it as
            // non-existent during the startup hydration window.
            executionContext.getCairoEngine().getMetadataCache().hydrateTableOnDemand(tableToken);
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

        // Returns the view's SQL as SHOW CREATE prints it, and puts its output columns into sqlColumns. The SQL of
        // a view created before CREATE required explicit column names may rely on generated names or call
        // zero-argument functions without parentheses; it comes with the names and parentheses that let it replay,
        // see SqlCompiler.addExplicitColumnNames(). Any other SQL, and one that no longer compiles, comes as stored.
        private CharSequence getShownSql() {
            viewColumnNames.clear();
            for (int i = 0, n = table.getColumnCount(); i < n; i++) {
                viewColumnNames.add(table.getColumnQuiet(i).getName());
            }
            try (SqlCompiler compiler = executionContext.getCairoEngine().getSqlCompiler()) {
                final String sql = compiler.addExplicitColumnNames(viewDefinition, viewColumnNames, executionContext, sqlColumns);
                return sql != null ? sql : viewDefinition.getMatViewSql();
            }
        }

        // True when CREATE MATERIALIZED VIEW without a column list, from the SQL that SHOW CREATE prints, builds the
        // view's columns: as many, with the same names in the same letter case, of the same types, in the same order.
        // Such a CREATE makes every SYMBOL column NOCACHE. sqlColumns holds no column when the SQL does not compile.
        private boolean isSqlBuildingViewColumns() {
            final int columnCount = table.getColumnCount();
            if (sqlColumns.getColumnCount() != columnCount) {
                return false;
            }
            for (int i = 0; i < columnCount; i++) {
                final CairoColumn column = table.getColumnQuiet(i);
                final int columnType = column.getType();
                if (!Chars.equals(column.getName(), sqlColumns.getColumnName(i))
                        || columnType != sqlColumns.getColumnType(i)
                        || (ColumnType.isSymbol(columnType) && column.isSymbolCached())) {
                    return false;
                }
            }
            return true;
        }

        // Prints the view's columns as the column list of CREATE MATERIALIZED VIEW. Each attribute the list states
        // is spelled out, so that a CREATE that runs it takes no default of the server it runs on: the SYMBOL cache
        // flag and the index with its type and BITMAP capacity. The symbol capacity is not stated, as SHOW CREATE
        // TABLE does not state it: the writer scales it with the number of distinct values.
        private void putColumnList() {
            sink.putAscii(" (\n");
            for (int i = 0, n = table.getColumnCount(); i < n; i++) {
                final CairoColumn column = table.getColumnQuiet(i);
                sink.putAscii('\t');
                SqlUtil.putIdentifier(sink, column.getName());
                sink.putAscii(' ').put(ColumnType.nameOf(column.getType()));
                if (ColumnType.isSymbol(column.getType())) {
                    sink.putAscii(column.isSymbolCached() ? " CACHE" : " NOCACHE");
                    if (column.isIndexed()) {
                        putIndex(column);
                    }
                }
                if (i < n - 1) {
                    sink.putAscii(',');
                }
                sink.putAscii('\n');
            }
            sink.putAscii(')');
        }

        private void putIndex(CairoColumn column) {
            final byte indexType = column.getIndexType();
            if (indexType == IndexType.BITMAP) {
                // CAPACITY without TYPE means BITMAP whatever the server's default index type. The list takes
                // capacities up to MAX_INDEX_VALUE_BLOCK_SIZE, ALTER statements store larger ones, and a larger one
                // comes as the largest the list takes. A capacity below 2 never reaches the metadata cache:
                // TableUtils.validateMeta() refuses it.
                sink.putAscii(" INDEX CAPACITY ").put(Math.min(column.getIndexBlockCapacity(), TableUtils.MAX_INDEX_VALUE_BLOCK_SIZE));
                return;
            }
            sink.putAscii(" INDEX TYPE ");
            IndexType.putName(sink, indexType);
            // the dense indexes of the covered columns, or none when the index covers no column
            final IntList coveredColumns = column.getCoveringColumnIndices();
            if (coveredColumns == null) {
                return;
            }
            boolean isIncludeOpen = false;
            for (int i = 0, n = coveredColumns.size(); i < n; i++) {
                final int coveredIndex = coveredColumns.getQuick(i);
                // DROP COLUMN leaves -1 in place of a covered column, and INCLUDE () does not parse
                if (coveredIndex > -1) {
                    sink.putAscii(isIncludeOpen ? ", " : " INCLUDE (");
                    SqlUtil.putIdentifier(sink, table.getColumnQuiet(coveredIndex).getName());
                    isIncludeOpen = true;
                }
            }
            if (isIncludeOpen) {
                sink.putAscii(')');
            }
        }

        private void showCreateMatView(CairoConfiguration configuration) {
            final CharSequence sql = getShownSql();
            // A view whose columns differ from those its SQL builds comes with its column list, which CREATE builds
            // the view's columns from, and with its designated timestamp, which CREATE would otherwise pick from
            // the query.
            final boolean isColumnListShown = !isSqlBuildingViewColumns();
            sink.putAscii("CREATE MATERIALIZED VIEW '")
                    .put(tableToken.getTableName())
                    .putAscii('\'');
            if (isColumnListShown) {
                putColumnList();
            }
            sink.putAscii(" WITH BASE '")
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
            sink.putAscii(" AS (\n");
            ShowCreateTableRecordCursorFactory.putTrimmed(sink, sql);
            sink.putAscii("\n)");
            if (isColumnListShown) {
                sink.putAscii(" TIMESTAMP(");
                SqlUtil.putIdentifier(sink, table.getTimestampName());
                sink.putAscii(')');
            }
            sink.putAscii(" PARTITION BY ").put(table.getPartitionByName());
            ttlToSink(sink);
            expireToSink(sink);
            inVolumeToSink(configuration, table, sink);
            sink.putAscii(';');
        }

        // overridden in ent, do not remove!
        protected void ttlToSink(CharSink<?> sink) {
            ShowCreateTableRecordCursorFactory.ttlToSink(table.getTtlHoursOrMonths(), sink);
        }

        // protected extension point (parallel to ttlToSink); ent may override, but currently does not --
        // EXPIRE ROWS renders identically in OSS and ent. Keep it protected so an override stays possible.
        protected void expireToSink(CharSink<?> sink) {
            ShowCreateTableRecordCursorFactory.expireToSink(table.getExpiryPredicate(), table.getExpiryCleanupIntervalMicros(), sink);
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

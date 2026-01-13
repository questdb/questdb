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
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import org.jetbrains.annotations.NotNull;

public class ShowCreateViewRecordCursorFactory extends AbstractRecordCursorFactory {
    public static final int N_DDL_COL = 0;
    private static final RecordMetadata METADATA;
    protected final int tokenPosition;
    protected final TableToken viewToken;
    private final ShowCreateViewCursor cursor = new ShowCreateViewCursor();

    public ShowCreateViewRecordCursorFactory(TableToken viewToken, int tokenPosition) {
        super(METADATA);
        this.viewToken = viewToken;
        this.tokenPosition = tokenPosition;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        return cursor.of(executionContext, viewToken, tokenPosition);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("show_create_view");
        sink.meta("of").val(viewToken.getTableName());
    }

    @Override
    protected void _close() {
        super._close();
        Misc.free(cursor);
    }

    public static class ShowCreateViewCursor implements NoRandomAccessRecordCursor {
        protected final Utf8StringSink sink = new Utf8StringSink();
        protected final ViewDefinition viewDefinition = new ViewDefinition();
        private final Path path;
        private final ShowCreateViewRecord record = new ShowCreateViewRecord();
        protected SqlExecutionContext executionContext;
        protected CairoTable view;
        private boolean hasRun;
        private BlockFileReader reader;
        private TableToken viewToken;

        public ShowCreateViewCursor() {
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
                showCreateView();
                hasRun = true;
                return true;
            }
            return false;
        }

        public ShowCreateViewCursor of(
                SqlExecutionContext executionContext,
                TableToken viewToken,
                int tokenPosition
        ) throws SqlException {
            this.viewToken = viewToken;
            this.executionContext = executionContext;
            try (MetadataCacheReader metadataRO = executionContext.getCairoEngine().getMetadataCache().readLock()) {
                this.view = metadataRO.getTable(viewToken);
                if (this.view == null) {
                    throw SqlException.$(tokenPosition, "view does not exist [view=")
                            .put(viewToken.getTableName()).put(']');
                } else if (!viewToken.equals(view.getTableToken())) {
                    throw TableReferenceOutOfDateException.of(viewToken);
                }
            }

            if (!viewToken.isView()) {
                throw SqlException.$(tokenPosition, "view expected, got table");
            }

            CairoConfiguration configuration = executionContext.getCairoEngine().getConfiguration();
            path.of(configuration.getDbRoot());
            final int pathLen = path.size();
            if (reader == null) {
                reader = new BlockFileReader(configuration);
            }
            try {
                ViewDefinition.readFrom(
                        viewDefinition,
                        reader,
                        path,
                        pathLen,
                        viewToken
                );
            } catch (CairoException e) {
                throw SqlException.$(tokenPosition, "could not read view definition [table=").put(viewToken)
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

        private void showCreateView() {
            sink.putAscii("CREATE VIEW '")
                    .put(viewToken.getTableName())
                    .putAscii("' AS ( ")
                    .putAscii('\n');
            sink.put(viewDefinition.getViewSql());
            sink.putAscii('\n');
            sink.putAscii(')');
            putAdditional();
            sink.putAscii(';');
        }

        // placeholder, do not remove!
        protected void putAdditional() {
        }

        public class ShowCreateViewRecord implements Record {

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

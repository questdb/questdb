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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;

/**
 * Emits the DDL needed to recreate the logical structure of the whole database,
 * one statement per row in the single {@code ddl} column. The open-source build
 * dumps tables, then materialized views, then views (so that base objects are
 * created before the objects that depend on them). The enterprise build extends
 * this with ACL principals (emitted first, ahead of any inline {@code OWNED BY})
 * and the trailing grants and memberships.
 * <p>
 * Each object's DDL is produced by delegating to the matching per-object
 * {@code SHOW CREATE ...} factory, so the dump stays in lock-step with those
 * commands without duplicating their formatting logic.
 */
public class ShowCreateDatabaseRecordCursorFactory extends AbstractRecordCursorFactory {
    public static final int N_DDL_COL = 0;
    private static final RecordMetadata METADATA;
    private static final Comparator<TableToken> TABLE_NAME_COMPARATOR =
            (a, b) -> a.getTableName().compareTo(b.getTableName());
    private final ShowCreateDatabaseCursor cursor = new ShowCreateDatabaseCursor();

    public ShowCreateDatabaseRecordCursorFactory() {
        super(METADATA);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        return cursor.of(executionContext);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("show_create_database");
    }

    // emitted after the object DDL; no-op in OSS, overridden in ent to add GRANT/ADD USER/ASSUME
    protected void appendAclGrants(ObjList<CharSequence> out, SqlExecutionContext executionContext) throws SqlException {
    }

    // emitted before the object DDL so that inline OWNED BY targets already exist;
    // no-op in OSS, overridden in ent to add CREATE USER/GROUP/SERVICE ACCOUNT
    protected void appendAclPrincipals(ObjList<CharSequence> out, SqlExecutionContext executionContext) throws SqlException {
    }

    protected void buildStatements(ObjList<CharSequence> out, SqlExecutionContext executionContext) throws SqlException {
        appendAclPrincipals(out, executionContext);
        appendObjects(out, executionContext);
        appendAclGrants(out, executionContext);
    }

    protected RecordCursorFactory matViewFactory(TableToken token) {
        return new ShowCreateMatViewRecordCursorFactory(token, 0);
    }

    protected RecordCursorFactory tableFactory(TableToken token) {
        return new ShowCreateTableRecordCursorFactory(token, 0);
    }

    protected RecordCursorFactory viewFactory(TableToken token) {
        return new ShowCreateViewRecordCursorFactory(token, 0);
    }

    @Override
    protected void _close() {
        super._close();
        Misc.free(cursor);
    }

    private static void appendObjectDdl(
            ObjList<CharSequence> out,
            RecordCursorFactory factory,
            SqlExecutionContext executionContext
    ) throws SqlException {
        try {
            try (RecordCursor cursor = factory.getCursor(executionContext)) {
                if (cursor.hasNext()) {
                    out.add(cursor.getRecord().getVarcharA(ShowCreateTableRecordCursorFactory.N_DDL_COL).toString());
                }
            }
        } finally {
            Misc.free(factory);
        }
    }

    private void appendObjects(ObjList<CharSequence> out, SqlExecutionContext executionContext) throws SqlException {
        final ObjHashSet<TableToken> tokens = new ObjHashSet<>();
        executionContext.getCairoEngine().getTableTokens(tokens, false);

        final ObjList<TableToken> tables = new ObjList<>();
        final ObjList<TableToken> matViews = new ObjList<>();
        final ObjList<TableToken> views = new ObjList<>();
        for (int i = 0, n = tokens.size(); i < n; i++) {
            final TableToken token = tokens.get(i);
            if (token.isSystem()) {
                continue;
            }
            if (token.isMatView()) {
                matViews.add(token);
            } else if (token.isView()) {
                views.add(token);
            } else {
                tables.add(token);
            }
        }

        // deterministic output; base objects precede dependents at the layer level
        // (tables -> materialized views -> views)
        tables.sort(TABLE_NAME_COMPARATOR);
        matViews.sort(TABLE_NAME_COMPARATOR);
        views.sort(TABLE_NAME_COMPARATOR);

        for (int i = 0, n = tables.size(); i < n; i++) {
            appendObjectDdl(out, tableFactory(tables.getQuick(i)), executionContext);
        }
        for (int i = 0, n = matViews.size(); i < n; i++) {
            appendObjectDdl(out, matViewFactory(matViews.getQuick(i)), executionContext);
        }
        for (int i = 0, n = views.size(); i < n; i++) {
            appendObjectDdl(out, viewFactory(views.getQuick(i)), executionContext);
        }
    }

    public class ShowCreateDatabaseCursor implements NoRandomAccessRecordCursor {
        private final ShowCreateDatabaseRecord record = new ShowCreateDatabaseRecord();
        private final Utf8StringSink sink = new Utf8StringSink();
        private final ObjList<CharSequence> statements = new ObjList<>();
        private int index;

        @Override
        public void close() {
            sink.clear();
            statements.clear();
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            if (index < statements.size()) {
                sink.clear();
                sink.put(statements.getQuick(index++));
                return true;
            }
            return false;
        }

        public ShowCreateDatabaseCursor of(SqlExecutionContext executionContext) throws SqlException {
            statements.clear();
            buildStatements(statements, executionContext);
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
            index = 0;
        }

        public class ShowCreateDatabaseRecord implements Record {

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

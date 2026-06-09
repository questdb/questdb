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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.view.ViewDefinition;
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
 * one statement per row in the single {@code ddl} column. Objects are emitted in
 * dependency order so that every object's dependencies (base tables, referenced
 * views/materialized views) precede it, which keeps the dump replayable for
 * view-on-view, materialized-view-on-table, and similar chains. The enterprise
 * build extends this with ACL principals (emitted first, ahead of any inline
 * {@code OWNED BY}) and the trailing grants and memberships.
 * <p>
 * Each object's DDL is produced by delegating to the matching per-object
 * {@code SHOW CREATE ...} factory, so the dump stays in lock-step with those
 * commands without duplicating their formatting logic. Objects the caller is not
 * authorized to read are skipped, so the dump never discloses DDL the caller
 * could not otherwise see.
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

    @Override
    protected void _close() {
        super._close();
        Misc.free(cursor);
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

    // returns false only when the caller is explicitly denied read access to the object;
    // under AllowAllSecurityContext (open-source builds) this never denies
    private static boolean isVisible(SecurityContext securityContext, TableToken token) {
        try {
            securityContext.authorizeSelectOnAnyColumn(token);
            return true;
        } catch (CairoException e) {
            if (e.isAuthorizationError()) {
                return false;
            }
            throw e;
        }
    }

    private void appendObjects(ObjList<CharSequence> out, SqlExecutionContext executionContext) throws SqlException {
        final CairoEngine engine = executionContext.getCairoEngine();
        final SecurityContext securityContext = executionContext.getSecurityContext();

        final ObjHashSet<TableToken> tokens = new ObjHashSet<>();
        engine.getTableTokens(tokens, false);

        // collect the non-system objects the caller is allowed to see
        final ObjList<TableToken> objects = new ObjList<>();
        final ObjHashSet<TableToken> visible = new ObjHashSet<>();
        for (int i = 0, n = tokens.size(); i < n; i++) {
            final TableToken token = tokens.get(i);
            if (token.isSystem() || !isVisible(securityContext, token)) {
                continue;
            }
            objects.add(token);
            visible.add(token);
        }

        // seed with a stable alphabetical order, then emit topologically so that an
        // object's dependencies always precede it (the view/mat-view graphs forbid
        // cycles, so this terminates)
        objects.sort(TABLE_NAME_COMPARATOR);
        final ObjHashSet<TableToken> emitted = new ObjHashSet<>();
        final ObjList<TableToken> ordered = new ObjList<>();
        for (int i = 0, n = objects.size(); i < n; i++) {
            topoEmit(objects.getQuick(i), engine, visible, emitted, ordered);
        }

        for (int i = 0, n = ordered.size(); i < n; i++) {
            appendObjectDdl(out, objectFactory(ordered.getQuick(i)), executionContext);
        }
    }

    private RecordCursorFactory objectFactory(TableToken token) {
        if (token.isMatView()) {
            return matViewFactory(token);
        }
        if (token.isView()) {
            return viewFactory(token);
        }
        return tableFactory(token);
    }

    private void topoEmit(
            TableToken token,
            CairoEngine engine,
            ObjHashSet<TableToken> visible,
            ObjHashSet<TableToken> emitted,
            ObjList<TableToken> ordered
    ) {
        if (emitted.contains(token)) {
            return;
        }
        // mark before recursing so a malformed cycle cannot cause infinite recursion
        emitted.add(token);
        if (token.isMatView()) {
            final MatViewDefinition definition = engine.getMatViewGraph().getViewDefinition(token);
            if (definition != null) {
                visitDependency(definition.getBaseTableName(), engine, visible, emitted, ordered);
            }
        } else if (token.isView()) {
            final ViewDefinition definition = engine.getViewGraph().getViewDefinition(token);
            if (definition != null) {
                final ObjList<CharSequence> dependencies = definition.getDependencies().keys();
                for (int i = 0, n = dependencies.size(); i < n; i++) {
                    visitDependency(dependencies.getQuick(i), engine, visible, emitted, ordered);
                }
            }
        }
        ordered.add(token);
    }

    private void visitDependency(
            CharSequence dependencyName,
            CairoEngine engine,
            ObjHashSet<TableToken> visible,
            ObjHashSet<TableToken> emitted,
            ObjList<TableToken> ordered
    ) {
        if (dependencyName == null) {
            return;
        }
        final TableToken dependency = engine.getTableTokenIfExists(dependencyName);
        // only order against objects that are part of this dump (visible and non-system)
        if (dependency != null && visible.contains(dependency)) {
            topoEmit(dependency, engine, visible, emitted, ordered);
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

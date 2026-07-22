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
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.griffin.BasePlanSink;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Interval;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;

/**
 * Emits the DDL needed to recreate the logical structure of the whole database,
 * one statement per row in the single {@code ddl} column. Objects are emitted in
 * dependency order so that every object's dependencies precede it, which keeps
 * the dump replayable for view-on-view, materialized-view-on-table and similar
 * chains. A view's dependencies come from the view graph; a materialized view's
 * dependencies are discovered by compiling its query and walking the plan for
 * the tables it reads (a materialized view may join tables beyond its declared
 * base). The enterprise build extends this with ACL principals (emitted first,
 * ahead of any inline {@code OWNED BY}) and the trailing grants and memberships.
 * <p>
 * Each object's DDL is produced by delegating to the matching per-object
 * {@code SHOW CREATE ...} factory, so the dump stays in lock-step with those
 * commands without duplicating their formatting logic. Objects the caller is not
 * authorized to read are skipped, so the dump never discloses DDL the caller
 * could not otherwise see. The token set is snapshotted up front, but each object's
 * liveness is re-checked just before its DDL is produced: an object dropped, renamed
 * or recreated in that window is skipped (with a logged warning) for every object
 * type alike, so the dump reflects the schema as of emit time rather than failing or
 * emitting stale DDL. This is best-effort, not a consistent snapshot - a skipped
 * object simply does not appear, and a later object that depended on it may then not
 * replay cleanly. Cancellation, timeouts and genuine errors still abort the dump.
 */
public class ShowCreateDatabaseRecordCursorFactory extends AbstractRecordCursorFactory {
    public static final int N_DDL_COL = 0;
    // category bits selected by the INCLUDE/EXCLUDE clause
    public static final int INCLUDE_TABLES = 1;
    public static final int INCLUDE_VIEWS = 1 << 1;
    public static final int INCLUDE_MATERIALIZED_VIEWS = 1 << 2;
    public static final int INCLUDE_USERS = 1 << 3;
    public static final int INCLUDE_GROUPS = 1 << 4;
    public static final int INCLUDE_SERVICE_ACCOUNTS = 1 << 5;
    public static final int INCLUDE_PERMISSIONS = 1 << 6;
    public static final int INCLUDE_SCHEMA = INCLUDE_TABLES | INCLUDE_VIEWS | INCLUDE_MATERIALIZED_VIEWS;
    public static final int INCLUDE_ACL = INCLUDE_USERS | INCLUDE_GROUPS | INCLUDE_SERVICE_ACCOUNTS | INCLUDE_PERMISSIONS;
    public static final int INCLUDE_ALL = INCLUDE_SCHEMA | INCLUDE_ACL;
    private static final Log LOG = LogFactory.getLog(ShowCreateDatabaseRecordCursorFactory.class);
    private static final RecordMetadata METADATA;
    private static final Comparator<TableToken> TABLE_NAME_COMPARATOR =
            (a, b) -> a.getTableName().compareTo(b.getTableName());
    protected final int includeMask;
    private final TableTokenCollector tableTokenCollector = new TableTokenCollector();
    private ShowCreateDatabaseCursor cursor = new ShowCreateDatabaseCursor();

    public ShowCreateDatabaseRecordCursorFactory(int includeMask) {
        super(METADATA);
        this.includeMask = includeMask;
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
        final ShowCreateDatabaseCursor cursor = this.cursor;
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

    // the per-object SHOW CREATE factories take a token position for error reporting; a database dump
    // has no per-object source position, so it passes 0. An object dropped between collection and emit
    // is skipped by appendObjectDdl, so this 0 surfaces only for a genuine error that aborts the dump.
    protected RecordCursorFactory matViewFactory(TableToken token) {
        return new ShowCreateMatViewRecordCursorFactory(token, 0);
    }

    protected RecordCursorFactory tableFactory(TableToken token) {
        return new ShowCreateTableRecordCursorFactory(token, 0);
    }

    protected RecordCursorFactory viewFactory(TableToken token) {
        return new ShowCreateViewRecordCursorFactory(token, 0);
    }

    private static int categoryBit(TableToken token) {
        if (token.isMatView()) {
            return INCLUDE_MATERIALIZED_VIEWS;
        }
        if (token.isView()) {
            return INCLUDE_VIEWS;
        }
        return INCLUDE_TABLES;
    }

    private static void fallBackToBaseTable(
            TableToken matView,
            MatViewDefinition definition,
            CairoEngine engine,
            ObjList<TableToken> out,
            CharSequence error
    ) {
        LOG.info().$("could not compile materialized view to resolve dependencies, ordering by base table only [view=")
                .$(matView).$(", error=").$safe(error).I$();
        final TableToken base = engine.getTableTokenIfExists(definition.getBaseTableName());
        if (base != null) {
            out.add(base);
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

    private static void logSkippedObject(TableToken token, CharSequence reason) {
        LOG.info().$("object dropped or replaced between snapshot and emit, skipping from database dump [object=")
                .$(token).$(", reason=").$safe(reason).I$();
    }

    private static void logUnreplayableDependency(TableToken object, TableToken dependency) {
        LOG.info().$("object depends on an object excluded from the database dump, its DDL may not replay as-is [object=")
                .$(object).$(", dependency=").$(dependency).I$();
    }

    // re-resolves the snapshotted name in the registry: a null or non-matching token means the
    // object was dropped, renamed or recreated between the token snapshot and this emit, so the
    // failing per-object SHOW CREATE is the vanish race rather than a real error to surface.
    private static boolean vanishedBetweenSnapshotAndEmit(TableToken token, SqlExecutionContext executionContext) {
        final TableToken live = executionContext.getCairoEngine().getTableTokenIfExists(token.getTableName());
        return live == null || !live.equals(token);
    }

    // Produces one object's DDL by delegating to its per-object SHOW CREATE factory. The token set was
    // snapshotted earlier (see appendObjects), so an object can be dropped, renamed or recreated before we
    // reach it. A positive liveness pre-check skips a vanished object up front, keeping the dump consistent
    // across object types and reflecting the schema as of emit time: in particular a dropped view, whose
    // on-disk definition can linger and still be readable, would otherwise emit stale DDL. The delegate can
    // still fail in the residual race between the pre-check and getCursor; the catch blocks skip that too,
    // while always rethrowing cancellation/interruption and genuine errors for a still-present object.
    private void appendObjectDdl(
            ObjList<CharSequence> out,
            TableToken token,
            SqlExecutionContext executionContext
    ) throws SqlException {
        executionContext.getCircuitBreaker().statefulThrowExceptionIfTripped();
        if (vanishedBetweenSnapshotAndEmit(token, executionContext)) {
            logSkippedObject(token, "object no longer present at emit time");
            return;
        }
        final RecordCursorFactory factory = objectFactory(token);
        try {
            try (RecordCursor cursor = factory.getCursor(executionContext)) {
                // per-object SHOW CREATE emits a single row today, but draining the cursor keeps the
                // dump complete if any per-object factory ever splits its DDL across multiple rows
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    out.add(record.getVarcharA(N_DDL_COL).toString());
                }
            }
        } catch (CairoException e) {
            // a cancelled or timed-out dump must surface, never be mistaken for a benign drop race
            if (e.isInterruption() || e.isCancellation() || !vanishedBetweenSnapshotAndEmit(token, executionContext)) {
                throw e;
            }
            logSkippedObject(token, e.getFlyweightMessage());
        } catch (TableReferenceOutOfDateException e) {
            // the object was recreated or renamed between snapshot and emit; its identity changed
            logSkippedObject(token, e.getFlyweightMessage());
        } catch (SqlException e) {
            if (!vanishedBetweenSnapshotAndEmit(token, executionContext)) {
                throw e;
            }
            logSkippedObject(token, e.getFlyweightMessage());
        } finally {
            Misc.free(factory);
        }
    }

    private void appendObjects(ObjList<CharSequence> out, SqlExecutionContext executionContext) throws SqlException {
        if ((includeMask & INCLUDE_SCHEMA) == 0) {
            return;
        }
        final CairoEngine engine = executionContext.getCairoEngine();
        final SecurityContext securityContext = executionContext.getSecurityContext();

        final ObjHashSet<TableToken> tokens = new ObjHashSet<>();
        engine.getTableTokens(tokens, false);

        // collect the non-system objects in a requested category that the caller is allowed to see
        final ObjList<TableToken> objects = new ObjList<>();
        final ObjHashSet<TableToken> visible = new ObjHashSet<>();
        for (int i = 0, n = tokens.size(); i < n; i++) {
            final TableToken token = tokens.get(i);
            if (token.isSystem() || (includeMask & categoryBit(token)) == 0 || !isVisible(securityContext, token)) {
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
            topoEmit(objects.getQuick(i), engine, executionContext, visible, emitted, ordered);
        }

        for (int i = 0, n = ordered.size(); i < n; i++) {
            appendObjectDdl(out, ordered.getQuick(i), executionContext);
        }
    }

    // collects, into out, the tokens of the objects the given object depends on
    private void collectDependencies(
            TableToken token,
            CairoEngine engine,
            SqlExecutionContext executionContext,
            ObjList<TableToken> out
    ) {
        if (token.isMatView()) {
            collectMatViewDependencies(token, engine, executionContext, out);
        } else if (token.isView()) {
            final ViewDefinition definition = engine.getViewGraph().getViewDefinition(token);
            if (definition != null) {
                final ObjList<CharSequence> names = definition.getDependencies().keys();
                for (int i = 0, n = names.size(); i < n; i++) {
                    final TableToken dependency = engine.getTableTokenIfExists(names.getQuick(i));
                    if (dependency != null) {
                        out.add(dependency);
                    }
                }
            }
        }
        // plain tables have no logical dependencies
    }

    // a materialized view may read tables beyond its declared base (e.g. via a join), so the full
    // dependency set is the set of tables the compiled query reads; fall back to the base table on
    // any compilation failure
    private void collectMatViewDependencies(
            TableToken matView,
            CairoEngine engine,
            SqlExecutionContext executionContext,
            ObjList<TableToken> out
    ) {
        final MatViewDefinition definition = engine.getMatViewGraph().getViewDefinition(matView);
        if (definition == null) {
            return;
        }
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            // Compile the mat view SQL once: build the execution model, then derive the factory
            // from that same model. The two walks below need different artifacts but share one
            // parse+optimise instead of paying for it twice.
            final ExecutionModel model = compiler.generateExecutionModel(definition.getMatViewSql(), executionContext);
            final IQueryModel queryModel = model.getQueryModel();
            if (queryModel != null) {
                // a view referenced by the mat view is inlined during compilation, so the plan walk
                // below only sees the view's physical base tables, never the view itself. Collect the
                // view (and mat-view) names from the parsed model first, before generating the factory
                // (which may mutate the model), so those objects are emitted ahead of this mat view
                // and the dump stays replayable.
                final ObjList<CharSequence> referencedViews = new ObjList<>();
                SqlUtil.collectAllTableAndViewNames(queryModel, referencedViews, true);
                for (int i = 0, n = referencedViews.size(); i < n; i++) {
                    final TableToken dependency = engine.getTableTokenIfExists(referencedViews.getQuick(i));
                    if (dependency != null) {
                        out.add(dependency);
                    }
                }
                // generate the factory from the already-compiled model (no second parse+optimise)
                // and walk its plan to collect the physical base tables the mat view reads.
                // NB: both walks are needed and neither subsumes the other. The model walk above sees
                // referenced views (which the plan never shows, as they are inlined) but the model only
                // exposes join/union/nested table names, missing tables referenced from sub-queries
                // (e.g. WHERE x IN (SELECT ... FROM t)); the plan walk reaches those via nested cursor
                // factories. So do not collapse this into a single collectAllTableAndViewNames(.., false).
                try (RecordCursorFactory factory = SqlUtil.generateFactory(compiler, model, executionContext)) {
                    if (factory != null) {
                        tableTokenCollector.collect(factory, executionContext);
                        final ObjList<TableToken> collected = tableTokenCollector.tables.getList();
                        for (int i = 0, n = collected.size(); i < n; i++) {
                            out.add(collected.getQuick(i));
                        }
                    }
                }
            }
        } catch (CairoException e) {
            // a cancelled or timed-out dump must not be swallowed as a benign compile failure
            if (e.isInterruption() || e.isCancellation()) {
                throw e;
            }
            fallBackToBaseTable(matView, definition, engine, out, e.getFlyweightMessage());
        } catch (SqlException e) {
            fallBackToBaseTable(matView, definition, engine, out, e.getFlyweightMessage());
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
            SqlExecutionContext executionContext,
            ObjHashSet<TableToken> visible,
            ObjHashSet<TableToken> emitted,
            ObjList<TableToken> ordered
    ) {
        if (emitted.contains(token)) {
            return;
        }
        // the whole dump is materialized eagerly, and resolving a mat view's dependencies compiles its
        // query; honour cancellation/timeout per object so a large schema stays interruptible
        executionContext.getCircuitBreaker().statefulThrowExceptionIfTripped();
        // mark before recursing so a malformed cycle cannot cause infinite recursion
        emitted.add(token);
        final ObjList<TableToken> dependencies = new ObjList<>();
        collectDependencies(token, engine, executionContext, dependencies);
        // deterministic output regardless of graph/plan iteration order
        dependencies.sort(TABLE_NAME_COMPARATOR);
        for (int i = 0, n = dependencies.size(); i < n; i++) {
            final TableToken dependency = dependencies.getQuick(i);
            if (dependency.equals(token)) {
                continue;
            }
            if (visible.contains(dependency)) {
                // order against objects that are part of this dump
                topoEmit(dependency, engine, executionContext, visible, emitted, ordered);
            } else {
                // the dependency was filtered out of this dump (a different INCLUDE/EXCLUDE category,
                // a system object, or one the caller is not authorized to read), so this object's DDL
                // references something the dump does not contain; warn that it may not replay as-is
                logUnreplayableDependency(token, dependency);
            }
        }
        ordered.add(token);
    }

    /**
     * Walks a compiled factory's execution plan to enumerate the table tokens it reads, without
     * rendering any plan text. Unlike {@link io.questdb.griffin.TextPlanSink}, every value- and
     * attribute-emitting method discards its input; only the {@code child(...)} and
     * {@code val(Plannable)}/{@code val(ObjList)} traversal is kept, so nested cursor factories are
     * still reached. This avoids building (and immediately discarding) the full textual plan on this
     * cold dump path.
     */
    private static final class TableTokenCollector extends BasePlanSink {
        private final ObjHashSet<TableToken> tables = new ObjHashSet<>();

        @Override
        public PlanSink attr(CharSequence name) {
            return this;
        }

        @Override
        public PlanSink child(CharSequence outer, Plannable inner) {
            return child(inner);
        }

        @Override
        public PlanSink child(Plannable p) {
            walk(p);
            return this;
        }

        public void collect(RecordCursorFactory factory, SqlExecutionContext executionContext) {
            tables.clear();
            addToken(factory.getTableToken());
            of(factory, executionContext);
        }

        @Override
        public void end() {
        }

        @Override
        public CharSequence getLine(int idx) {
            return null;
        }

        @Override
        public int getLineCount() {
            return 0;
        }

        @Override
        public PlanSink meta(CharSequence name) {
            return this;
        }

        @Override
        public void of(RecordCursorFactory factory, SqlExecutionContext executionContext) {
            clear();
            this.executionContext = executionContext;
            if (factory != null) {
                factoryStack.push(factory);
                factory.toPlan(this);
                factoryStack.pop();
            }
        }

        @Override
        public PlanSink type(CharSequence type) {
            return this;
        }

        @Override
        public PlanSink val(boolean b) {
            return this;
        }

        @Override
        public PlanSink val(char c) {
            return this;
        }

        @Override
        public PlanSink val(CharSequence cs) {
            return this;
        }

        @Override
        public PlanSink val(double d) {
            return this;
        }

        @Override
        public PlanSink val(float f) {
            return this;
        }

        @Override
        public PlanSink val(int i) {
            return this;
        }

        @Override
        public PlanSink val(long l) {
            return this;
        }

        @Override
        public PlanSink val(long hash, int geoHashBits) {
            return this;
        }

        @Override
        public PlanSink val(ObjList<?> list, int from, int to) {
            for (int i = from; i < to; i++) {
                if (list.getQuick(i) instanceof Plannable plannable) {
                    walk(plannable);
                }
            }
            return this;
        }

        @Override
        public PlanSink val(Plannable s) {
            walk(s);
            return this;
        }

        @Override
        public PlanSink val(Sinkable s) {
            return this;
        }

        @Override
        public PlanSink val(Utf8Sequence utf8) {
            return this;
        }

        @Override
        public PlanSink valDecimal(long value, int precision, int scale) {
            return this;
        }

        @Override
        public PlanSink valDecimal(long hi, long lo, int precision, int scale) {
            return this;
        }

        @Override
        public PlanSink valDecimal(long hh, long hl, long lh, long ll, int precision, int scale) {
            return this;
        }

        @Override
        public PlanSink valIPv4(int ip) {
            return this;
        }

        @Override
        public PlanSink valISODate(TimestampDriver driver, long l) {
            return this;
        }

        @Override
        public PlanSink valInterval(Interval interval, int intervalType) {
            return this;
        }

        @Override
        public PlanSink valLong256(long long0, long long1, long long2, long long3) {
            return this;
        }

        @Override
        public PlanSink valUuid(long lo, long hi) {
            return this;
        }

        private void addToken(TableToken token) {
            if (token != null) {
                tables.add(token);
            }
        }

        // Traverses a plan node, collecting the table token of any nested cursor factory regardless
        // of whether it is reached via child(...) or val(...). Funnelling all three entry points
        // through here means a factory exposed through val() rather than child() still contributes
        // its base table to the dependency set, so the mat view never sorts ahead of a table it reads.
        private void walk(Plannable p) {
            if (p instanceof RecordCursorFactory factory) {
                addToken(factory.getTableToken());
                factoryStack.push(factory);
                p.toPlan(this);
                factoryStack.pop();
            } else if (p != null) {
                p.toPlan(this);
            }
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

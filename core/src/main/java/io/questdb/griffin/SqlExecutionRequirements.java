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

package io.questdb.griffin;

import io.questdb.cairo.CairoException;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

public final class SqlExecutionRequirements implements Mutable {
    /**
     * The function lists database objects, or reads their metadata, as far as the security context
     * of the caller allows: it returns only the objects the caller may see (see
     * {@link io.questdb.cairo.SecurityContext#isTableVisible(io.questdb.cairo.TableToken)}), or it
     * lists them to admins only. The SHOW statements that list objects or read their metadata count
     * as such functions, see {@link FunctionResolver#addObjectDisclosingStatement}.
     * <p>
     * A materialized or live view refreshes detached from any caller, under a context that sees
     * every object, protected ones included. Every reader of the view reads what the refresh saw, and
     * SELECT on the view is the only control over who does. So a materialized or live view may use
     * such a function only where a SYSTEM ADMIN writes it, in the view's own SQL: CREATE requires
     * SYSTEM ADMIN, and CREATE and the refresh alike reject the function when it comes from a regular
     * view that the SQL reads, because anyone who may alter that view can change what it reads after
     * CREATE. The refresh compiles the stored SQL and its views anew, so it is where that rule holds.
     * A system view counts as the view's own SQL: no user may alter, replace or drop it, so what it
     * reads is fixed when the SYSTEM ADMIN creates the materialized or live view.
     */
    public static final int DISCLOSES_OBJECTS = 1 << 2;
    public static final int NONE = 0;
    /**
     * The function needs the Enterprise security context of the caller, which the refresh of a
     * materialized or live view does not have, or it has side effects. Materialized and live views
     * reject such a function, at CREATE and at refresh alike.
     */
    public static final int REQUIRES_ENTERPRISE_SECURITY_CONTEXT = 1 << 1;
    public static final int REQUIRES_LIVE_WAL_PROGRESS = 1;
    private static final int IS_SOURCE = 1 << 29;
    private static final int IS_STATEMENT = 1 << 30;
    private static final int IS_VIEW_ALTERABLE = 1 << 28;
    private static final int STORED_VIEW_REQUIREMENTS = DISCLOSES_OBJECTS | REQUIRES_ENTERPRISE_SECURITY_CONTEXT;
    // the requirements of each deferred function or SHOW statement, with the IS_* facts about it
    private final IntList deferred = new IntList();
    private final ObjList<String> deferredNames = new ObjList<>();
    private final IntList deferredPositions = new IntList();
    private final ObjList<String> deferredViewNames = new ObjList<>();
    private int liveWalProgressPosition = -1;

    /**
     * Throws when a materialized or live view may not store the result of a function, or a SHOW
     * statement, with the given requirements, see {@link #DISCLOSES_OBJECTS} and
     * {@link #REQUIRES_ENTERPRISE_SECURITY_CONTEXT}. The context compiles the SQL of the view, at
     * CREATE or at refresh.
     *
     * @param requirements     the requirements of the function or statement
     * @param position         where the requirements surface in the compiled SQL
     * @param name             the name of the function, or the SHOW statement
     * @param isStatement      whether name is a SHOW statement rather than a function
     * @param view             the view that the function or statement is written in, or null when the
     *                         compiled SQL itself writes it; a system view counts as the compiled SQL
     * @param executionContext the context compiling the SQL of the materialized or live view
     * @throws SqlException when the view may not store the result
     */
    public static void checkStoredView(
            int requirements,
            int position,
            CharSequence name,
            boolean isStatement,
            @Nullable SqlExecutionContext.TableFunctionView view,
            SqlExecutionContext executionContext
    ) throws SqlException {
        checkStoredView(requirements, position, name, isStatement, viewNameOf(view), isAlterable(view), executionContext);
    }

    public void add(int requirements, int position) {
        if ((requirements & REQUIRES_LIVE_WAL_PROGRESS) != 0 && liveWalProgressPosition < 0) {
            liveWalProgressPosition = position;
        }
    }

    /**
     * Decides the functions and SHOW statements {@link #defer} recorded while CREATE MATERIALIZED VIEW bound its SQL,
     * in the order the materialized view rejects them: of the table functions and SHOW statements, an administrative
     * one first, then a catalogue one from a view users may alter, then any other catalogue one; then the first
     * other function the view may not store, in the order binding created them, unless binding created a
     * non-deterministic function before it, which the materialized view rejects instead.
     *
     * @param nonDeterministicCallIndex    the number of functions and SHOW statements recorded before binding created
     *                                     its first non-deterministic function, or -1 when it created none
     * @param nonDeterministicCallPosition the position of that non-deterministic function
     * @param nonDeterministicCallName     the name of that non-deterministic function
     * @throws SqlException when the materialized view may not store the result of a recorded function or statement
     */
    public void checkDeferred(
            int nonDeterministicCallIndex,
            int nonDeterministicCallPosition,
            @Nullable CharSequence nonDeterministicCallName,
            SqlExecutionContext executionContext
    ) throws SqlException {
        int alterableSource = -1;
        int catalogueSource = -1;
        for (int i = 0, n = deferred.size(); i < n; i++) {
            final int requirements = deferred.getQuick(i);
            if ((requirements & IS_SOURCE) != 0) {
                if ((requirements & REQUIRES_ENTERPRISE_SECURITY_CONTEXT) != 0) {
                    checkDeferredAt(i, executionContext);
                }
                if (catalogueSource < 0) {
                    catalogueSource = i;
                }
                if (alterableSource < 0 && (requirements & IS_VIEW_ALTERABLE) != 0) {
                    alterableSource = i;
                }
            }
        }
        if (catalogueSource > -1) {
            checkDeferredAt(alterableSource > -1 ? alterableSource : catalogueSource, executionContext);
        }
        for (int i = 0, n = deferred.size(); i < n; i++) {
            if ((deferred.getQuick(i) & IS_SOURCE) == 0) {
                try {
                    checkDeferredAt(i, executionContext);
                } catch (SqlException e) {
                    if (nonDeterministicCallIndex > -1 && nonDeterministicCallIndex <= i) {
                        throw SqlException.nonDeterministicColumn(nonDeterministicCallPosition, nonDeterministicCallName, objectKind(executionContext));
                    }
                    throw e;
                }
            }
        }
    }

    @Override
    public void clear() {
        deferred.clear();
        deferredNames.clear();
        deferredPositions.clear();
        deferredViewNames.clear();
        liveWalProgressPosition = -1;
    }

    /**
     * Records a function, or a SHOW statement, the materialized view may not store, for
     * {@link #checkDeferred} to decide once binding finished.
     *
     * @param view the view that the function or statement is written in, or null when the compiled
     *             SQL itself writes it; a system view counts as the compiled SQL
     */
    public void defer(
            int requirements,
            int position,
            CharSequence name,
            boolean isStatement,
            @Nullable SqlExecutionContext.TableFunctionView view
    ) {
        if ((requirements & STORED_VIEW_REQUIREMENTS) == 0) {
            return;
        }
        deferred.add((requirements & STORED_VIEW_REQUIREMENTS) | (isStatement ? IS_STATEMENT | IS_SOURCE : 0) | (isAlterable(view) ? IS_VIEW_ALTERABLE : 0));
        deferredNames.add(Chars.toString(name));
        deferredPositions.add(position);
        deferredViewNames.add(viewNameOf(view));
    }

    /**
     * The number of functions and SHOW statements deferred so far, to pass to {@link #markSources(int)}.
     */
    public int getDeferredCount() {
        return deferred.size();
    }

    /**
     * The position of the first function that requires {@link #REQUIRES_LIVE_WAL_PROGRESS}, or -1.
     */
    public int getLiveWalProgressPosition() {
        return liveWalProgressPosition;
    }

    /**
     * Marks the functions and SHOW statements deferred since {@code deferredCount} as parts of a table function or a
     * SHOW statement, which the materialized view decides before any other function.
     */
    public void markSources(int deferredCount) {
        for (int i = deferredCount, n = deferred.size(); i < n; i++) {
            deferred.setQuick(i, deferred.getQuick(i) | IS_SOURCE);
        }
    }

    private static void checkStoredView(
            int requirements,
            int position,
            CharSequence name,
            boolean isStatement,
            @Nullable CharSequence viewName,
            boolean isViewAlterable,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final CharSequence objectKind = objectKind(executionContext);
        if ((requirements & REQUIRES_ENTERPRISE_SECURITY_CONTEXT) != 0) {
            throw SqlException.functionNotAllowed(position)
                    .put("administrative function cannot be used in ")
                    .put(objectKind)
                    .put(": ")
                    .put(name);
        }
        if ((requirements & DISCLOSES_OBJECTS) == 0) {
            return;
        }
        final CharSequence what = isStatement ? "catalogue statement" : "catalogue function";
        if (isViewAlterable) {
            // The definition of a regular view may change after CREATE, by anyone who may alter it,
            // and the refresh would store what the changed definition lists, see DISCLOSES_OBJECTS.
            throw SqlException.functionNotAllowed(position)
                    .put(what)
                    .put(" from view ")
                    .put(viewName)
                    .put(" cannot be used in ")
                    .put(objectKind)
                    .put(": ")
                    .put(name);
        }
        if (executionContext.isMatViewRefresh() || executionContext.isLiveViewRefresh()) {
            // the refresh compiles what CREATE accepted, it has no caller to authorize
            return;
        }
        try {
            // Not isSystemAdmin(): the built-in admin stays one after it assumes a service account,
            // whose permissions decide from then on.
            executionContext.getSecurityContext().authorizeSystemAdmin();
        } catch (CairoException e) {
            if (!e.isAuthorizationError()) {
                throw e;
            }
            final SqlException exception = SqlException.functionNotAllowed(position).put(what);
            if (viewName != null) {
                // a system view, whose SQL the principal did not write
                exception.put(" from view ").put(viewName);
            }
            throw exception
                    .put(" cannot be used in ")
                    .put(objectKind)
                    .put(" without SYSTEM ADMIN: ")
                    .put(name);
        }
    }

    // Whether users may alter the view that the function or statement is written in, and so change what it reads
    // after CREATE. No user may alter, replace or drop a system view, see DISCLOSES_OBJECTS.
    private static boolean isAlterable(@Nullable SqlExecutionContext.TableFunctionView view) {
        return view != null && !view.definition().getViewToken().isSystem();
    }

    private static CharSequence objectKind(SqlExecutionContext executionContext) {
        return executionContext.isLiveViewCompile() ? "live view" : "materialized view";
    }

    private static @Nullable String viewNameOf(@Nullable SqlExecutionContext.TableFunctionView view) {
        return view != null ? view.definition().getViewToken().getTableName() : null;
    }

    private void checkDeferredAt(int index, SqlExecutionContext executionContext) throws SqlException {
        final int requirements = deferred.getQuick(index);
        checkStoredView(requirements, deferredPositions.getQuick(index), deferredNames.getQuick(index),
                (requirements & IS_STATEMENT) != 0, deferredViewNames.getQuick(index), (requirements & IS_VIEW_ALTERABLE) != 0,
                executionContext);
    }
}

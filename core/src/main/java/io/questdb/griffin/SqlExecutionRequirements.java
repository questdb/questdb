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
import io.questdb.std.Mutable;
import org.jetbrains.annotations.Nullable;

public final class SqlExecutionRequirements implements Mutable {
    /**
     * The function lists database objects, or reads their metadata, as far as the security context
     * of the caller allows: it returns only the objects the caller may see (see
     * {@link io.questdb.cairo.SecurityContext#isTableVisible(io.questdb.cairo.TableToken)}), or it
     * lists them to admins only. The SHOW statements that list objects or read their metadata count
     * as such functions, see {@link FunctionParser#addObjectDisclosingStatement}.
     * <p>
     * A materialized or live view refreshes detached from any caller, under a context that sees
     * every object, protected ones included. Every reader of the view reads what the refresh saw, and
     * SELECT on the view is the only control over who does. So a materialized or live view may use
     * such a function only where a SYSTEM ADMIN writes it, in the view's own SQL: CREATE requires
     * SYSTEM ADMIN, and CREATE and the refresh alike reject the function when it comes from a regular
     * view that the SQL reads, because anyone who may alter that view can change what it reads after
     * CREATE. The refresh compiles the stored SQL and its views anew, so it is where that rule holds.
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
    private String disclosingName;
    private int disclosingPosition = -1;
    private boolean disclosingStatement;
    private String disclosingViewName;
    private String enterpriseSecurityContextFunctionName;
    private int enterpriseSecurityContextPosition = -1;
    private int flags;
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
     * @param view             the regular view that the function or statement is written in, or null
     *                         when the compiled SQL itself writes it
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
        checkStoredView0(requirements, position, name, isStatement, viewNameOf(view), executionContext);
    }

    /**
     * Records the requirements of a function, or a SHOW statement, for checks that run after the
     * compile created it, e.g. CREATE MATERIALIZED VIEW checks what the optimiser created, see
     * {@link #checkStoredView(SqlExecutionContext)}.
     *
     * @param view the regular view that the function or statement is written in, or null when the
     *             compiled SQL itself writes it
     */
    public void add(
            int requirements,
            int position,
            CharSequence name,
            boolean isStatement,
            @Nullable SqlExecutionContext.TableFunctionView view
    ) {
        flags |= requirements;
        if ((requirements & REQUIRES_ENTERPRISE_SECURITY_CONTEXT) != 0 && enterpriseSecurityContextPosition < 0) {
            enterpriseSecurityContextFunctionName = Chars.toString(name);
            enterpriseSecurityContextPosition = position;
        }
        // one that comes from a regular view replaces one the SQL writes: a view rejects it outright
        if ((requirements & DISCLOSES_OBJECTS) != 0
                && (disclosingPosition < 0 || (disclosingViewName == null && view != null))) {
            disclosingName = Chars.toString(name);
            disclosingPosition = position;
            disclosingStatement = isStatement;
            disclosingViewName = viewNameOf(view);
        }
        if ((requirements & REQUIRES_LIVE_WAL_PROGRESS) != 0 && liveWalProgressPosition < 0) {
            liveWalProgressPosition = position;
        }
    }

    /**
     * Like {@link #checkStoredView(int, int, CharSequence, boolean, SqlExecutionContext.TableFunctionView, SqlExecutionContext)},
     * for the functions and SHOW statements recorded so far.
     */
    public void checkStoredView(SqlExecutionContext executionContext) throws SqlException {
        if (enterpriseSecurityContextPosition > -1) {
            checkStoredView0(
                    REQUIRES_ENTERPRISE_SECURITY_CONTEXT,
                    enterpriseSecurityContextPosition,
                    enterpriseSecurityContextFunctionName,
                    false,
                    null,
                    executionContext
            );
        }
        if (disclosingPosition > -1) {
            checkStoredView0(
                    DISCLOSES_OBJECTS,
                    disclosingPosition,
                    disclosingName,
                    disclosingStatement,
                    disclosingViewName,
                    executionContext
            );
        }
    }

    @Override
    public void clear() {
        disclosingName = null;
        disclosingPosition = -1;
        disclosingStatement = false;
        disclosingViewName = null;
        enterpriseSecurityContextFunctionName = null;
        enterpriseSecurityContextPosition = -1;
        flags = NONE;
        liveWalProgressPosition = -1;
    }

    public int getPosition(int requirement) {
        if (requirement == REQUIRES_ENTERPRISE_SECURITY_CONTEXT
                && (flags & REQUIRES_ENTERPRISE_SECURITY_CONTEXT) != 0) {
            return enterpriseSecurityContextPosition;
        }
        if (requirement == REQUIRES_LIVE_WAL_PROGRESS && (flags & REQUIRES_LIVE_WAL_PROGRESS) != 0) {
            return liveWalProgressPosition;
        }
        return -1;
    }

    private static void checkStoredView0(
            int requirements,
            int position,
            CharSequence name,
            boolean isStatement,
            @Nullable CharSequence viewName,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final CharSequence objectKind = executionContext.isLiveViewCompile() ? "live view" : "materialized view";
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
        if (viewName != null) {
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
            throw SqlException.functionNotAllowed(position)
                    .put(what)
                    .put(" cannot be used in ")
                    .put(objectKind)
                    .put(" without SYSTEM ADMIN: ")
                    .put(name);
        }
    }

    private static @Nullable String viewNameOf(@Nullable SqlExecutionContext.TableFunctionView view) {
        return view != null ? view.definition().getViewToken().getTableName() : null;
    }
}

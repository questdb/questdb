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

package io.questdb.cutlass;

import io.questdb.cairo.SecurityContext;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Mutable;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The key of a statement in the server-wide select and insert caches, which share compiled statements across
 * connections. It holds the select cache scope of the principal the statement runs for, see
 * {@link SecurityContext#getSelectCacheScope()}, and the planner whose plans the key reaches: a lookup holds the
 * planner the server names at the time, and a compile replaces it with the planner that compiled the statement, so the
 * statement returns to the key of its own planner even when a reload flips the switch in between.
 * <p>
 * The logical-plan planner keys the shared {@code null} scope by the SQL text itself. Every other key goes to the
 * sink: a NUL, or a SOH for the legacy planner, which no SQL text starts with; then the scope after its length, or a
 * '-', which no length starts with, for the shared scope; then the SQL text. So no planner, scope and text spell the
 * key of another.
 */
public final class SelectCacheKey implements Mutable {
    private final StringSink sink = new StringSink();
    private boolean isLegacyPlanner;
    private @Nullable CharSequence scope;

    @Override
    public void clear() {
        scope = null;
        isLegacyPlanner = false;
    }

    /**
     * Holds the scope and the planner of another key, for a statement that shares the plan of the other's.
     */
    public void copyOf(@NotNull SelectCacheKey other) {
        scope = other.scope;
        isLegacyPlanner = other.isLegacyPlanner;
    }

    /**
     * Returns the insert cache key of the SQL text for the planner this key holds. The insert caches share a statement
     * across principals, so the key carries no scope.
     */
    public CharSequence insertKey(@NotNull CharSequence sqlText) {
        return build(null, sqlText);
    }

    /**
     * Returns the select cache key of the SQL text in the scope and for the planner this key holds.
     */
    public CharSequence key(@NotNull CharSequence sqlText) {
        return build(scope, sqlText);
    }

    /**
     * Starts the lookup of a statement the context runs: holds the select cache scope of the context's principal and
     * the planner the server names now.
     */
    public SelectCacheKey of(@NotNull SqlExecutionContext executionContext) {
        return of(
                executionContext.getSecurityContext().getSelectCacheScope(),
                executionContext.getCairoEngine().getConfiguration().isSqlLegacyPlannerEnabled()
        );
    }

    /**
     * Holds the scope, which must be immutable, see {@link SecurityContext#getSelectCacheScope()}, and the planner.
     */
    public SelectCacheKey of(@Nullable CharSequence scope, boolean isLegacyPlanner) {
        this.scope = scope;
        this.isLegacyPlanner = isLegacyPlanner;
        return this;
    }

    /**
     * Holds the planner that compiled the statement the compiler compiled last, the one whose key the statement
     * returns to.
     */
    public void setCompiledBy(@NotNull SqlCompiler compiler) {
        isLegacyPlanner = compiler.isCompiledByLegacyPlanner();
    }

    private CharSequence build(@Nullable CharSequence scope, @NotNull CharSequence sqlText) {
        if (scope == null && !isLegacyPlanner) {
            return sqlText;
        }
        sink.clear();
        sink.put(isLegacyPlanner ? '\u0001' : '\u0000');
        if (scope == null) {
            sink.put('-');
        } else {
            sink.put(scope.length()).put(':').put(scope);
        }
        sink.put(sqlText);
        return sink;
    }
}

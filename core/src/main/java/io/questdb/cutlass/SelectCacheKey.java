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
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Builds the keys of the server-wide select caches, which share compiled statements across
 * connections, see {@link SecurityContext#getSelectCacheScope()}.
 */
public final class SelectCacheKey {

    private SelectCacheKey() {
    }

    /**
     * Returns the key of the SQL text in the given scope, for the planner that compiles it. The logical-plan planner
     * keys the shared {@code null} scope by the SQL text itself. Every other key goes to the sink: a NUL, or a SOH for
     * the legacy planner, which no SQL text starts with; then the scope after its length, or a '-', which no length
     * starts with, for the shared scope; then the SQL text. So no planner, scope and text spell the key of another.
     */
    public static CharSequence of(@Nullable CharSequence scope, boolean isLegacyPlanner, @NotNull CharSequence sqlText, @NotNull StringSink sink) {
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

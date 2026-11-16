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
     * Returns the key of the SQL text in the given scope: the SQL text itself for the shared
     * {@code null} scope, otherwise the text qualified by the scope, written to the sink. The
     * qualified key starts with a NUL, which no SQL text starts with, and length-prefixes the
     * scope, so no scope and text pair can spell the key of another.
     */
    public static CharSequence of(@Nullable CharSequence scope, @NotNull CharSequence sqlText, @NotNull StringSink sink) {
        if (scope == null) {
            return sqlText;
        }
        sink.clear();
        sink.put('\u0000').put(scope.length()).put(':').put(scope).put(sqlText);
        return sink;
    }

    /**
     * Returns the key of the SQL text compiled with the variables the client declared outside it,
     * see {@link io.questdb.griffin.QueryVariables#getDeclarations()}, in the given scope. The
     * compiled statement holds the values of the variables, so the same text with other values
     * takes another key. Without variables, the key is the one {@link #of(CharSequence,
     * CharSequence, StringSink)} returns. With them, the key starts with a NUL and a 'V', which
     * no other key starts with, and length-prefixes the variables and the scope, so no
     * combination can spell the key of another.
     */
    public static CharSequence of(
            @Nullable CharSequence scope,
            @NotNull CharSequence sqlText,
            @NotNull CharSequence variables,
            @NotNull StringSink sink
    ) {
        if (variables.length() == 0) {
            return of(scope, sqlText, sink);
        }
        sink.clear();
        sink.put('\u0000').put('V').put(variables.length()).put(':').put(variables);
        if (scope == null) {
            sink.put('-');
        } else {
            sink.put(scope.length()).put(':').put(scope);
        }
        sink.put(sqlText);
        return sink;
    }
}

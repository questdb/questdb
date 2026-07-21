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

package io.questdb.test.griffin.fuzz.expr;

import io.questdb.std.str.StringSink;
import io.questdb.test.griffin.fuzz.types.ColumnKind;

/**
 * An expression node. Stateless after construction: {@link #appendSql}
 * can be called multiple times so the same expression can be emitted in
 * SELECT and re-emitted in GROUP BY / ORDER BY. The {@code BindContext}
 * overload is used by the differential bind-variable runner to emit a
 * second copy of the same tree with bindable {@link ConstantExpr} nodes
 * replaced by {@code ?} placeholders.
 */
public interface FuzzExpr {

    /**
     * Render the expression as SQL in literal form. Equivalent to
     * {@code appendSql(sink, null)}.
     */
    default void appendSql(StringSink sink) {
        appendSql(sink, null);
    }

    /**
     * Render the expression as SQL. When {@code ctx} is non-null, bindable
     * constant leaves may emit {@code ?::TYPE} and register their value with
     * the context; all other nodes thread {@code ctx} to their children
     * unchanged. Parenthesises composite nodes so nested rewrites never
     * lose operator precedence.
     */
    void appendSql(StringSink sink, BindContext ctx);

    /**
     * Approximate type category of the expression result. Generators
     * use it to pick compatible operators and to avoid aggregate-over-
     * array noise. Exact runtime types (e.g. BYTE vs LONG) are all
     * collapsed to {@link ColumnKind#NUMERIC}.
     */
    ColumnKind getKind();
}

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
import io.questdb.test.griffin.fuzz.types.FuzzColumnType;

/**
 * {@code (CASE WHEN col IS NULL THEN NULL END)}, a CASE whose only value is
 * NULL: it is NULL on every row and of the NULL type. Bare, it reaches its
 * parent as a NULL-typed argument the parent resolves; with a cast it reaches
 * the parent as the slot's type. The compiler folds it to the NULL literal,
 * and its condition over a column keeps it from being written as one: a CASE
 * the compiler left unfolded threw from every read.
 */
public final class NullCaseExpr implements FuzzExpr {
    // null for the bare CASE
    private final FuzzColumnType castType;
    private final FuzzExpr conditionColumn;
    private final ColumnKind kind;

    public NullCaseExpr(FuzzExpr conditionColumn, ColumnKind kind, FuzzColumnType castType) {
        this.conditionColumn = conditionColumn;
        this.kind = kind;
        this.castType = castType;
    }

    @Override
    public void appendSql(StringSink sink, BindContext ctx) {
        sink.put("(CASE WHEN ");
        conditionColumn.appendSql(sink, ctx);
        sink.put(" IS NULL THEN NULL END)");
        if (castType != null) {
            sink.put("::").put(castType.getDdl());
        }
    }

    @Override
    public ColumnKind getKind() {
        return kind;
    }
}

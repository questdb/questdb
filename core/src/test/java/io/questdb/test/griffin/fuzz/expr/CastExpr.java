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
 * {@code expr::TYPE}, or {@code cast(expr AS TYPE)} when {@code isCastKeyword}. The keyword form
 * spells DOUBLE and TIMESTAMP with their multi-word aliases, {@code DOUBLE PRECISION} and
 * {@code TIMESTAMP WITH TIME ZONE}; its plain spelling (see
 * {@link BindContext#plainSpelling()}) is the {@code ::} form. The result kind is the target
 * type's kind so the caller can satisfy a typed slot (e.g. a numeric aggregate argument) by
 * casting any reachable inner expression.
 */
public final class CastExpr implements FuzzExpr {
    private final FuzzExpr inner;
    private final boolean isCastKeyword;
    private final FuzzColumnType targetType;

    public CastExpr(FuzzExpr inner, FuzzColumnType targetType, boolean isCastKeyword) {
        this.inner = inner;
        this.targetType = targetType;
        this.isCastKeyword = isCastKeyword;
    }

    @Override
    public void appendSql(StringSink sink, BindContext ctx) {
        if (isCastKeyword && !BindContext.isPlainSpelling(ctx)) {
            sink.put("cast(");
            inner.appendSql(sink, ctx);
            final String ddl = targetType.getDdl();
            sink.put(" AS ").put(switch (ddl) {
                case "DOUBLE" -> "DOUBLE PRECISION";
                case "TIMESTAMP" -> "TIMESTAMP WITH TIME ZONE";
                default -> ddl;
            }).put(')');
            return;
        }
        sink.put('(');
        inner.appendSql(sink, ctx);
        // as in PostgreSQL, '::FLOAT' casts to DOUBLE (SqlParser.rewritePgCast), while
        // 'cast(x AS FLOAT)' casts to FLOAT; FLOAT4 is the '::' name of FLOAT
        final String ddl = targetType.getDdl();
        sink.put(")::").put(isCastKeyword && "FLOAT".equals(ddl) ? "FLOAT4" : ddl);
    }

    @Override
    public ColumnKind getKind() {
        return targetType.getKind();
    }
}

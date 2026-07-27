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
 * Typed literal expression. The literal string is generated once by the
 * owning {@link io.questdb.test.griffin.fuzz.types.FuzzColumnType} and
 * frozen in the node so re-emission is stable. When the constant is
 * bindable (i.e. {@code FuzzConstant.isBindable()}) and a non-null
 * {@link BindContext} is threaded into {@link #appendSql}, the node may
 * emit {@code ?::TYPE} instead of the literal text and register its
 * value with the context.
 */
public final class ConstantExpr implements FuzzExpr {
    private final FuzzConstant constant;
    private final ColumnKind kind;

    public ConstantExpr(ColumnKind kind, FuzzConstant constant) {
        this.kind = kind;
        this.constant = constant;
    }

    public ConstantExpr(ColumnKind kind, String literal) {
        this(kind, FuzzConstant.nonBindable(literal));
    }

    @Override
    public void appendSql(StringSink sink, BindContext ctx) {
        if (ctx != null && constant.isBindable() && ctx.shouldBind()) {
            String name = ctx.addBinding(constant.bindValue());
            // Named bind variables avoid the bare-? vs column-ref ambiguity
            // the parser hits when a positional ? appears in a projection
            // slot.
            sink.put(':').put(name).put("::").put(constant.bindCastDdl());
            return;
        }
        sink.put(constant.literal());
    }

    @Override
    public ColumnKind getKind() {
        return kind;
    }
}

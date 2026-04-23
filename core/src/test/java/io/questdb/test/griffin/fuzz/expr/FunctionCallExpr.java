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
 * Single-argument scalar function call, e.g. {@code abs(c3)},
 * {@code upper(sym)}, {@code length(c7)}. The generator picks from a
 * curated list so the argument kind matches what the function accepts
 * and the result kind is known statically.
 */
public final class FunctionCallExpr implements FuzzExpr {
    private final FuzzExpr arg;
    private final String name;
    private final ColumnKind resultKind;

    public FunctionCallExpr(String name, ColumnKind resultKind, FuzzExpr arg) {
        this.name = name;
        this.resultKind = resultKind;
        this.arg = arg;
    }

    @Override
    public void appendSql(StringSink sink) {
        sink.put(name).put('(');
        arg.appendSql(sink);
        sink.put(')');
    }

    @Override
    public ColumnKind getKind() {
        return resultKind;
    }
}

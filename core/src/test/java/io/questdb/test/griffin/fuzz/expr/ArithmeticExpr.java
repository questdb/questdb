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
 * Numeric {@code lhs op rhs}. Both operands are expected to be numeric;
 * the result is reported as {@link ColumnKind#NUMERIC} regardless of the
 * precise promoted type.
 */
public final class ArithmeticExpr implements FuzzExpr {
    private final FuzzExpr lhs;
    private final String op;
    private final FuzzExpr rhs;

    public ArithmeticExpr(FuzzExpr lhs, String op, FuzzExpr rhs) {
        this.lhs = lhs;
        this.op = op;
        this.rhs = rhs;
    }

    @Override
    public void appendSql(StringSink sink) {
        sink.put('(');
        lhs.appendSql(sink);
        sink.put(' ').put(op).put(' ');
        rhs.appendSql(sink);
        sink.put(')');
    }

    @Override
    public ColumnKind getKind() {
        return ColumnKind.NUMERIC;
    }
}

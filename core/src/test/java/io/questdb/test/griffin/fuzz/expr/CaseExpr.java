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

import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.griffin.fuzz.types.ColumnKind;

/**
 * {@code CASE [operand] WHEN ... THEN ... [ELSE ...] END}. Without an operand this is the
 * searched form and each WHEN holds a condition; with one, each WHEN holds an INT literal key.
 * <p>
 * The plain spelling (see {@link BindContext#plainSpelling()}) writes the simple form as the
 * searched form, {@code CASE WHEN operand = key THEN ...}. The parser must tell the two forms
 * apart, and the keyword spelling evaluates through {@code switch()} while the plain one, for
 * an operand other than a column, compares with {@code =}. The searched form keeps its
 * spelling; the spelling axis still checks the operands inside it, such as
 * {@code extract(day FROM ts)} in a WHEN condition.
 */
public final class CaseExpr implements FuzzExpr {
    private final FuzzExpr elseValue;
    private final ColumnKind kind;
    private final FuzzExpr operand;
    private final ObjList<FuzzExpr> thens;
    private final ObjList<FuzzExpr> whens;

    /**
     * @param operand   the operand of the simple form, or null for the searched form
     * @param elseValue the ELSE value, or null for none
     */
    public CaseExpr(ColumnKind kind, FuzzExpr operand, ObjList<FuzzExpr> whens, ObjList<FuzzExpr> thens, FuzzExpr elseValue) {
        assert whens.size() > 0 && whens.size() == thens.size();
        this.kind = kind;
        this.operand = operand;
        this.whens = whens;
        this.thens = thens;
        this.elseValue = elseValue;
    }

    @Override
    public void appendSql(StringSink sink, BindContext ctx) {
        final boolean isSearchedSpelling = operand == null || BindContext.isPlainSpelling(ctx);
        sink.put("CASE");
        if (!isSearchedSpelling) {
            sink.put(' ');
            operand.appendSql(sink, ctx);
        }
        for (int i = 0, n = whens.size(); i < n; i++) {
            sink.put(" WHEN ");
            if (operand != null && isSearchedSpelling) {
                sink.put('(');
                operand.appendSql(sink, ctx);
                sink.put(") = ");
            }
            whens.getQuick(i).appendSql(sink, ctx);
            sink.put(" THEN ");
            thens.getQuick(i).appendSql(sink, ctx);
        }
        if (elseValue != null) {
            sink.put(" ELSE ");
            elseValue.appendSql(sink, ctx);
        }
        sink.put(" END");
    }

    @Override
    public ColumnKind getKind() {
        return kind;
    }

    /**
     * {@code lhs > rhs}, the WHEN condition of the searched form.
     */
    public static final class GreaterThan implements FuzzExpr {
        private final FuzzExpr lhs;
        private final FuzzExpr rhs;

        public GreaterThan(FuzzExpr lhs, FuzzExpr rhs) {
            this.lhs = lhs;
            this.rhs = rhs;
        }

        @Override
        public void appendSql(StringSink sink, BindContext ctx) {
            lhs.appendSql(sink, ctx);
            sink.put(" > ");
            rhs.appendSql(sink, ctx);
        }

        @Override
        public ColumnKind getKind() {
            return ColumnKind.BOOLEAN;
        }
    }
}

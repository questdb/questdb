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

package io.questdb.test.griffin.fuzz;

import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.griffin.fuzz.expr.ExpressionGenerator;
import io.questdb.test.griffin.fuzz.types.ColumnKind;

/**
 * Produces WHERE-clause predicates over a given column list. The
 * top-level structure (NOT / AND / OR) is built here; every leaf
 * (column, literal, cast, arithmetic, function call) is delegated to
 * {@link ExpressionGenerator}, so WHERE predicates get the same
 * expression coverage as projection slots.
 * <p>
 * Both sides of a comparison share a {@link ColumnKind} drawn from a
 * column actually present in the table, which keeps the "cannot compare
 * X with Y" noise down without going all the way to static typing.
 */
public final class PredicateGenerator {

    private static final String[] COMPARISON_OPS = {"=", "!=", "<", "<=", ">", ">="};
    private static final String[] EQUALITY_OPS = {"=", "!="};

    private final int maxDepth;
    private final Rnd rnd;

    public PredicateGenerator(Rnd rnd, int maxDepth) {
        this.rnd = rnd;
        this.maxDepth = maxDepth;
    }

    public String generate(ObjList<FuzzColumn> columns, String qualifier) {
        StringSink sink = new StringSink();
        ExpressionGenerator exprGen = new ExpressionGenerator(rnd, columns, qualifier, 2);
        appendPredicate(sink, columns, exprGen, 0);
        return sink.toString();
    }

    private void appendInPredicate(StringSink sink, ExpressionGenerator exprGen, ColumnKind kind) {
        exprGen.generateOfKind(kind).appendSql(sink);
        sink.put(" IN (");
        int n = 1 + rnd.nextInt(3);
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                sink.put(", ");
            }
            exprGen.generateOfKind(kind).appendSql(sink);
        }
        sink.put(')');
    }

    private void appendLeafPredicate(StringSink sink, ObjList<FuzzColumn> columns, ExpressionGenerator exprGen) {
        if (columns.size() == 0) {
            sink.put("true");
            return;
        }
        // Anchor the predicate's kind to a real column's kind so
        // ExpressionGenerator can always find a compatible leaf.
        FuzzColumn anchor = columns.getQuick(rnd.nextInt(columns.size()));
        ColumnKind kind = anchor.getType().getKind();

        int choice = rnd.nextInt(10);
        // 0-1: IS NULL / IS NOT NULL; 2: IN; 3-9: comparison or boolean-alone
        if (choice < 2) {
            exprGen.generateOfKind(kind).appendSql(sink);
            sink.put(rnd.nextBoolean() ? " IS NULL" : " IS NOT NULL");
            return;
        }
        if (choice == 2 && kind != ColumnKind.ARRAY) {
            appendInPredicate(sink, exprGen, kind);
            return;
        }

        if (kind == ColumnKind.BOOLEAN && rnd.nextBoolean()) {
            if (rnd.nextBoolean()) {
                sink.put("NOT ");
            }
            exprGen.generateOfKind(ColumnKind.BOOLEAN).appendSql(sink);
            return;
        }

        if (kind == ColumnKind.ARRAY) {
            exprGen.generateOfKind(ColumnKind.ARRAY).appendSql(sink);
            sink.put(rnd.nextBoolean() ? " IS NULL" : " IS NOT NULL");
            return;
        }

        String[] ops = kind.isOrderable() ? COMPARISON_OPS : EQUALITY_OPS;
        String op = ops[rnd.nextInt(ops.length)];
        exprGen.generateOfKind(kind).appendSql(sink);
        sink.put(' ').put(op).put(' ');
        exprGen.generateOfKind(kind).appendSql(sink);
    }

    private void appendPredicate(
            StringSink sink,
            ObjList<FuzzColumn> columns,
            ExpressionGenerator exprGen,
            int depth
    ) {
        if (depth >= maxDepth || rnd.nextInt(3) == 0) {
            appendLeafPredicate(sink, columns, exprGen);
            return;
        }
        int choice = rnd.nextInt(6);
        if (choice == 0) {
            sink.put("NOT (");
            appendPredicate(sink, columns, exprGen, depth + 1);
            sink.put(')');
            return;
        }
        String op = rnd.nextBoolean() ? " AND " : " OR ";
        sink.put('(');
        appendPredicate(sink, columns, exprGen, depth + 1);
        sink.put(op);
        appendPredicate(sink, columns, exprGen, depth + 1);
        sink.put(')');
    }
}

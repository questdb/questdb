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

import io.questdb.griffin.engine.functions.test.TestFaultFunctionFactory;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.griffin.fuzz.expr.BindContext;
import io.questdb.test.griffin.fuzz.expr.ColumnRefExpr;
import io.questdb.test.griffin.fuzz.expr.ExpressionGenerator;
import io.questdb.test.griffin.fuzz.types.CharType;
import io.questdb.test.griffin.fuzz.types.ColumnKind;
import io.questdb.test.griffin.fuzz.types.SymbolType;

/**
 * Produces WHERE-clause predicates over a given column list. The
 * top-level structure (NOT / AND / OR) is built here; every leaf
 * (column, literal, cast, arithmetic, function call) is delegated to
 * {@link ExpressionGenerator}, so WHERE predicates get the same
 * expression coverage as projection slots.
 * <p>
 * Both sides of a comparison share a {@link ColumnKind} drawn from a
 * column actually present in the table, which keeps the "cannot compare
 * X with Y" noise down without going all the way to static typing. The
 * kind also decides the operator set: {@link ColumnKind#isOrderable()}
 * admits {@code <}, {@code <=}, {@code >} and {@code >=} alongside
 * equality.
 * <p>
 * One leaf shape does not go through {@link ExpressionGenerator}: a SYMBOL
 * or CHAR column compared against a number spelled without quotes. Nothing
 * in the typed grammar produces it, because the constant is not of the
 * column's type, yet QuestDB accepts both -- a symbol resolves its key from
 * the literal's text, a char takes the code point.
 */
public final class PredicateGenerator {
    private static final String[] COMPARISON_OPS = {"=", "!=", "<", "<=", ">", ">="};
    private static final String[] EQUALITY_OPS = {"=", "!="};
    // Numbers spelled without quotes, for comparison against a SYMBOL or CHAR
    // column. The parser splits a negative one into a unary minus over a bare
    // token, so the constant reaches the filter compiler without its sign
    // attached and each column type's arm has to put it back. "5" and "-5" are
    // members of SymbolType.DOMAIN and match stored rows; the other pair
    // resolves to a symbol key that is not in the table, which is a separate
    // (deferred) code path.
    private static final String[] UNQUOTED_NUMBER_LITERALS = {"5", "-5", "42", "-42"};

    private final int maxDepth;
    private final Rnd rnd;

    public PredicateGenerator(Rnd rnd, int maxDepth) {
        this.rnd = rnd;
        this.maxDepth = maxDepth;
    }

    /**
     * Emits an optional {@code WHERE} clause onto {@code sql}. With probability
     * 1/2 it appends {@code WHERE <predicate>} over {@code columns}, the
     * predicate built at {@code maxDepth}. When {@code injectFaultFn} is set the
     * fuzzer's {@code test_fault()} function is woven in as the first conjunct --
     * so it is evaluated for every scanned row -- forcing a WHERE even when none
     * would otherwise be emitted. The rnd draw sequence is identical regardless
     * of {@code injectFaultFn}, so non-fault generation stays byte-identical and
     * the bind-variant determinism invariant holds.
     */
    public static void appendWhere(
            StringSink sql,
            Rnd rnd,
            ObjList<FuzzColumn> columns,
            String qualifier,
            int maxDepth,
            BindContext ctx,
            boolean injectFaultFn
    ) {
        boolean hasWhere = rnd.nextBoolean();
        if (!hasWhere && !injectFaultFn) {
            return;
        }
        sql.put(" WHERE ");
        if (injectFaultFn) {
            sql.put(TestFaultFunctionFactory.CALL);
            if (hasWhere) {
                sql.put(" AND ").put(new PredicateGenerator(rnd, maxDepth).generate(columns, qualifier, ctx));
            }
        } else {
            sql.put(new PredicateGenerator(rnd, maxDepth).generate(columns, qualifier, ctx));
        }
    }

    public String generate(ObjList<FuzzColumn> columns, String qualifier, BindContext ctx) {
        StringSink sink = new StringSink();
        ExpressionGenerator exprGen = new ExpressionGenerator(rnd, columns, qualifier, 2);
        appendPredicate(sink, columns, exprGen, qualifier, ctx, 0);
        return sink.toString();
    }

    /**
     * True for the column types that compare against a number spelled without
     * quotes. STRING and VARCHAR are out: they coerce the whole comparison to
     * the number's type and die on the first non-numeric stored value, which
     * the oracle swallows as a skip and costs a query.
     */
    private static boolean acceptsUnquotedNumber(FuzzColumn column) {
        return column.getType() == SymbolType.INSTANCE || column.getType() == CharType.INSTANCE;
    }

    private void appendInPredicate(StringSink sink, ExpressionGenerator exprGen, BindContext ctx, ColumnKind kind) {
        exprGen.generateOfKind(kind).appendSql(sink, ctx);
        // NOT IN compiles down a different branch of the JIT filter serializer
        // than IN, so both spellings are worth emitting.
        sink.put(rnd.nextInt(3) == 0 ? " NOT IN (" : " IN (");
        int n = 1 + rnd.nextInt(3);
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                sink.put(", ");
            }
            exprGen.generateOfKind(kind).appendSql(sink, ctx);
        }
        sink.put(')');
    }

    private void appendLeafPredicate(
            StringSink sink,
            ObjList<FuzzColumn> columns,
            ExpressionGenerator exprGen,
            String qualifier,
            BindContext ctx
    ) {
        if (columns.size() == 0) {
            sink.put("true");
            return;
        }
        // Anchor the predicate's kind to a real column's kind so
        // ExpressionGenerator can always find a compatible leaf.
        FuzzColumn anchor = columns.getQuick(rnd.nextInt(columns.size()));
        ColumnKind kind = anchor.getType().getKind();

        int choice = rnd.nextInt(10);
        // 0-1: IS NULL / IS NOT NULL; 2: IN / NOT IN; 3-4: unquoted number,
        // when the anchor is a SYMBOL or a CHAR; 3-9: comparison or
        // boolean-alone
        if (choice < 2) {
            exprGen.generateOfKind(kind).appendSql(sink, ctx);
            sink.put(rnd.nextBoolean() ? " IS NULL" : " IS NOT NULL");
            return;
        }
        if (choice == 2 && kind != ColumnKind.ARRAY) {
            appendInPredicate(sink, exprGen, ctx, kind);
            return;
        }
        if ((choice == 3 || choice == 4) && acceptsUnquotedNumber(anchor)) {
            appendUnquotedNumberPredicate(sink, anchor, qualifier, ctx);
            return;
        }

        if (kind == ColumnKind.BOOLEAN && rnd.nextBoolean()) {
            if (rnd.nextBoolean()) {
                sink.put("NOT ");
            }
            exprGen.generateOfKind(ColumnKind.BOOLEAN).appendSql(sink, ctx);
            return;
        }

        if (kind == ColumnKind.ARRAY) {
            exprGen.generateOfKind(ColumnKind.ARRAY).appendSql(sink, ctx);
            sink.put(rnd.nextBoolean() ? " IS NULL" : " IS NOT NULL");
            return;
        }

        String[] ops = kind.isOrderable() ? COMPARISON_OPS : EQUALITY_OPS;
        String op = ops[rnd.nextInt(ops.length)];
        exprGen.generateOfKind(kind).appendSql(sink, ctx);
        sink.put(' ').put(op).put(' ');
        exprGen.generateOfKind(kind).appendSql(sink, ctx);
    }

    private void appendPredicate(
            StringSink sink,
            ObjList<FuzzColumn> columns,
            ExpressionGenerator exprGen,
            String qualifier,
            BindContext ctx,
            int depth
    ) {
        if (depth >= maxDepth || rnd.nextInt(3) == 0) {
            appendLeafPredicate(sink, columns, exprGen, qualifier, ctx);
            return;
        }
        int choice = rnd.nextInt(6);
        if (choice == 0) {
            sink.put("NOT (");
            appendPredicate(sink, columns, exprGen, qualifier, ctx, depth + 1);
            sink.put(')');
            return;
        }
        String op = rnd.nextBoolean() ? " AND " : " OR ";
        sink.put('(');
        appendPredicate(sink, columns, exprGen, qualifier, ctx, depth + 1);
        sink.put(op);
        appendPredicate(sink, columns, exprGen, qualifier, ctx, depth + 1);
        sink.put(')');
    }

    /**
     * Emits {@code sym <op> -5}: a SYMBOL or CHAR column against a number
     * spelled without quotes. QuestDB accepts both -- a symbol resolves its key
     * from the literal's text, a char takes the code point -- so the constant
     * lands in the column type's arm of the filter compiler in a spelling the
     * typed grammar never produces. Both operand orders are emitted, and the
     * literal pool overlaps {@link SymbolType#DOMAIN} so the row set is not
     * always empty.
     */
    private void appendUnquotedNumberPredicate(StringSink sink, FuzzColumn anchor, String qualifier, BindContext ctx) {
        ColumnRefExpr column = new ColumnRefExpr(rnd, anchor, qualifier);
        String literal = UNQUOTED_NUMBER_LITERALS[rnd.nextInt(UNQUOTED_NUMBER_LITERALS.length)];
        String op = EQUALITY_OPS[rnd.nextInt(EQUALITY_OPS.length)];
        if (rnd.nextBoolean()) {
            column.appendSql(sink, ctx);
            sink.put(' ').put(op).put(' ').put(literal);
        } else {
            sink.put(literal).put(' ').put(op).put(' ');
            column.appendSql(sink, ctx);
        }
    }
}

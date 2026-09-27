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
 * <p>
 * A comparison can also stand as the operand of another comparison --
 * {@code (a < b) = true}, {@code (a < b) = (c < d)}. The typed grammar reaches
 * that shape no better: {@link ExpressionGenerator} carries no comparison node,
 * so a BOOLEAN operand it builds is only ever a column, a constant or a cast.
 * {@link #appendNestedComparisonPredicate} names the defect the shape stands
 * for.
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

    /**
     * Emits one BOOLEAN operand of the outer comparison in
     * {@link #appendNestedComparisonPredicate}: either the bare {@code true} /
     * {@code false} spelling, or whatever {@link ExpressionGenerator} makes of
     * the BOOLEAN kind -- a boolean column, a boolean constant, or a cast.
     * <p>
     * The bare spelling stays out of the typed grammar's hands on purpose. A
     * generated BOOLEAN constant is bindable, so the bind variant rewrites it to
     * {@code :bN::BOOLEAN}; the unbound literal beside an ordering comparison is
     * the operand the JIT serializer stubs and then declines on, and only this
     * spelling puts it in front of that code path in both variants of the query.
     */
    private void appendBooleanOperand(StringSink sink, ExpressionGenerator exprGen, BindContext ctx) {
        if (rnd.nextBoolean()) {
            sink.put(rnd.nextBoolean() ? "true" : "false");
            return;
        }
        exprGen.generateOfKind(ColumnKind.BOOLEAN).appendSql(sink, ctx);
    }

    /**
     * Emits {@code <expr> <op> <expr>} with both operands drawn from
     * {@code kind}, which keeps the comparison type-valid by construction.
     * {@link ColumnKind#isOrderable()} decides whether the operator may be an
     * ordering one or has to stay an equality.
     */
    private void appendComparison(StringSink sink, ExpressionGenerator exprGen, BindContext ctx, ColumnKind kind) {
        String[] ops = kind.isOrderable() ? COMPARISON_OPS : EQUALITY_OPS;
        String op = ops[rnd.nextInt(ops.length)];
        exprGen.generateOfKind(kind).appendSql(sink, ctx);
        sink.put(' ').put(op).put(' ');
        exprGen.generateOfKind(kind).appendSql(sink, ctx);
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
        // 0-1: IS NULL / IS NOT NULL; 2: IN / NOT IN, unless the anchor is an
        // ARRAY; 3-4: unquoted number, but only when the anchor is a SYMBOL or a
        // CHAR; 5: a comparison nested as the operand of a comparison, unless the
        // anchor is an ARRAY; 6-9, plus every draw above whose guard turned it
        // down: a plain comparison, a boolean-alone predicate when the anchor is
        // BOOLEAN, or IS [NOT] NULL when it is an ARRAY.
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
        if (choice == 5 && kind != ColumnKind.ARRAY) {
            // A non-array anchor is all pickComparableKind needs to find a kind
            // for each inner comparison, so the guard doubles as its
            // precondition.
            appendNestedComparisonPredicate(sink, columns, exprGen, ctx);
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

        appendComparison(sink, exprGen, ctx, kind);
    }

    /**
     * Emits {@code (<expr> <op> <expr>)} -- one comparison in parentheses, ready
     * to stand as a BOOLEAN operand. It draws its own kind instead of inheriting
     * the leaf's anchor, so the two halves of a {@code (<cmp>) = (<cmp>)} pair
     * ordering against equality, and CHAR against IPv4 against UUID against
     * numeric, on their own.
     */
    private void appendNestedComparison(
            StringSink sink,
            ObjList<FuzzColumn> columns,
            ExpressionGenerator exprGen,
            BindContext ctx
    ) {
        sink.put('(');
        appendComparison(sink, exprGen, ctx, pickComparableKind(columns));
        sink.put(')');
    }

    /**
     * Emits a comparison nested as the operand of another comparison:
     * {@code (c_char < c_char2) = true}, {@code true != (c_ip <= c_ip2)},
     * {@code (c_char < c_char2) = (c_uuid = c_uuid2)}.
     * <p>
     * {@code CompiledFilterIRSerializer} expands a CHAR or IPv4 ordering
     * comparison by emitting its operands naively, rewinding the IR stream, then
     * emitting the real expansion. It rewound to the offset the PREDICATE began
     * at rather than the one the ordering node began at, so an ordering node
     * with an already-emitted sibling ahead of it erased that sibling's IR and
     * handed the native backend an operand stack one value short. Both the avx2
     * and the x86 backend answer that with an out-of-bounds pop, which aborts the
     * JVM instead of declining the filter (issues 7547, 7549).
     * <p>
     * {@code PostOrderTreeTraversalAlgo} descends the rhs first, so the nested
     * comparison has to appear on BOTH sides: with it only ever on the right the
     * predicate's start offset coincides with the ordering node's and the defect
     * stays invisible. The outer operator comes from {@link #EQUALITY_OPS}
     * because both its operands are BOOLEAN and QuestDB orders no booleans -- an
     * ordering operator there would only burn queries on skips.
     * <p>
     * Nesting stops one level down. The inner comparison's operands come from
     * {@link ExpressionGenerator}, which has no comparison node, so the shape is
     * exactly one comparison deep whatever {@code maxDepth} is. The cap is
     * deliberately separate from {@link #appendPredicate}'s depth budget: that
     * budget counts NOT / AND / OR levels and is already spent by the time a leaf
     * runs, and sharing it would make the shape's frequency swing with how deep
     * the enclosing predicate happened to go.
     */
    private void appendNestedComparisonPredicate(
            StringSink sink,
            ObjList<FuzzColumn> columns,
            ExpressionGenerator exprGen,
            BindContext ctx
    ) {
        String op = EQUALITY_OPS[rnd.nextInt(EQUALITY_OPS.length)];
        int shape = rnd.nextInt(4);
        if (shape == 0) {
            // The crashing order. The rhs is traversed first, so the nested
            // comparison expands with its sibling's IR already in the stream.
            appendNestedComparison(sink, columns, exprGen, ctx);
            sink.put(' ').put(op).put(' ');
            appendBooleanOperand(sink, exprGen, ctx);
            return;
        }
        if (shape == 1) {
            // The order that accidentally worked. It is the control: it tells a
            // truncated IR stream apart from a merely wrong one.
            appendBooleanOperand(sink, exprGen, ctx);
            sink.put(' ').put(op).put(' ');
            appendNestedComparison(sink, columns, exprGen, ctx);
            return;
        }
        appendNestedComparison(sink, columns, exprGen, ctx);
        sink.put(' ').put(op).put(' ');
        appendNestedComparison(sink, columns, exprGen, ctx);
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

    /**
     * Draws the {@link ColumnKind} of a column whose comparison the SQL compiler
     * resolves, i.e. anything but {@link ColumnKind#ARRAY}. An array does have
     * {@code =} -- {@code EqDoubleArrayFunctionFactory} registers
     * {@code =(D[]D[])} and {@code FunctionFactoryCache} derives {@code !=} and
     * {@code <>} from every {@code =} -- but it resolves only between two arrays
     * of the SAME dimensionality, and QuestDB encodes dimensionality in the column
     * type, so {@code DOUBLE[]} and {@code DOUBLE[][]} are distinct types.
     * {@link #appendComparison} draws each operand independently and
     * {@code DoubleArrayType.random} redraws the dimension count every time, so
     * half the pairs would come out as {@code ARRAY[..] = ARRAY[[..]]}, which the
     * compiler rejects; {@link ExpressionGenerator}'s cast arm can also hand back
     * {@code (<numeric>)::DOUBLE[]}, which never compiles either. Both push
     * QueryFuzzTest's per-shape accepted rate down for no coverage in return.
     * A single draw picks the starting column and the scan walks forward from it,
     * so the cost stays flat however many arrays the schema carries. The caller
     * has already drawn a non-array anchor, so the scan always finds one.
     */
    private ColumnKind pickComparableKind(ObjList<FuzzColumn> columns) {
        final int n = columns.size();
        final int start = rnd.nextInt(n);
        for (int i = 0; i < n; i++) {
            final ColumnKind kind = columns.getQuick((start + i) % n).getType().getKind();
            if (kind != ColumnKind.ARRAY) {
                return kind;
            }
        }
        throw new AssertionError("every column is an ARRAY; appendLeafPredicate drew a non-ARRAY anchor from the same list");
    }
}

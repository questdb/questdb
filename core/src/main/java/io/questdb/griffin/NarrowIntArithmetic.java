/*******************************************************************************
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

package io.questdb.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.Nullable;

/**
 * Refuses narrow-integer arithmetic whose 32-bit (or 16-, or 8-bit) result differs from its
 * mathematical one, in the places where the engine turns such a result into a quantity the
 * writer of the statement cannot see.
 * <p>
 * INT arithmetic wraps modulo 2^32 in every context, so the seconds-to-micros idiom
 * {@code epoch_secs * 1000000} produces a small number - of either sign - rather than a timestamp.
 * The wrap itself stays exactly as it is everywhere else: {@code SELECT 86400 * 1000000} answers
 * 500654080 and every consumer that reads a projected value reads that. This guard covers only the
 * consumers that read the value as a width and then never show it - a DROP PARTITION WHERE clause,
 * whose record exposes nothing but the designated timestamp; a window frame width, which has no
 * projected spelling at all; and a SAMPLE BY stride, which has neither a projected spelling nor a
 * plan line, since {@code EXPLAIN} prints the same three lines for an eight-minute stride and a
 * one-day one.
 * <p>
 * The rule is a value-domain one and it runs over compiled constants, not over AST tokens. That is
 * what makes it see through every spelling of an operand: a quoted numeric literal
 * ({@code '1720468802' * 1000000}, which overload resolution reads as a number), a narrowing cast,
 * an INT-returning function call, a CASE. It is also why it fires on the arithmetic wherever the
 * arithmetic sits, not only at the bound: {@code ts > to_utc('1720468802'::int * 1000000, 'UTC')}
 * and {@code ts - '1720468802' * 1000000 > 0} wrap one level below a 64-bit-typed parent and are
 * refused all the same.
 * <p>
 * Two outcomes are accepted. Arithmetic proven to stay in range keeps working, which is what
 * preserves {@code dateadd('d', 2 * 7, now())}, {@code ts + 1000000 * 60},
 * {@code ts > abs(5) * 2}, the bare bounds {@code ts > 0} / {@code >= 0} / {@code > -1} that the
 * codebase's "drop everything" idiom relies on, and the ordinary frame widths
 * {@code RANGE BETWEEN 60 * 1000000 PRECEDING} and {@code ROWS BETWEEN 2 * 3 PRECEDING}.
 * Arithmetic proven to evaluate to NULL is accepted too: every narrow-int factory answers NULL for
 * a NULL operand or a zero divisor, and a NULL bound matches no partition floor and no frame, so
 * it cannot over-match - the consumer's own NULL handling reports it.
 * <p>
 * A unary {@code +} or {@code -} whose operand is only known once the statement runs is accepted
 * too, and that is a proof rather than a concession: {@code +} is the identity and {@code -} maps
 * the width onto itself, so the node's value set equals the operand's own. The one input that
 * wraps is MIN, where {@code -MIN} is MIN again - still a value the operand alone can carry, and
 * the operand alone is a shape this guard accepts. That is what keeps the retention idiom
 * {@code dateadd('d', -$1, now())} working: {@code dateadd}'s stride is INT-only, so the widening
 * the refusal used to name is not expressible there at all.
 * <p>
 * Anything else is refused. A narrow-int BINARY arithmetic node whose operand is only known once
 * the statement runs - a bind variable, a value read off the timestamp column, or a master column
 * in a dynamic {@code WINDOW JOIN} bound - cannot be proven either way, and this deliberately
 * fails closed: the refusal names the remedies that fix it. That costs the arithmetic that could
 * never have wrapped, {@code RANGE BETWEEN (k + 1) MINUTES PRECEDING} included, and it is the
 * price of having one answer for both spellings of a frame the caller cannot see.
 * <p>
 * The guard is self-contained: it needs an expression, a metadata describing the record the
 * expression will be compiled against, and a compiling context. Extending it to another statement
 * is a matter of adding a call and a subject, not of moving logic.
 */
final class NarrowIntArithmetic {
    /**
     * A DROP / DETACH / CONVERT PARTITION WHERE clause. {@code partitionFunctionRec} exposes
     * nothing but the designated timestamp, so {@code WHERE ts > <wrapped>} is true for every
     * partition floor of a table holding modern data, and DROP PARTITION removes the whole table
     * while reporting success.
     */
    static final int SUBJECT_PARTITION_FILTER = 0;
    /**
     * The stride of a {@code SAMPLE BY <expr> <unit>} clause - the {@code n} of
     * {@code SAMPLE BY n U}. {@code SqlCodeGenerator.generateSampleBy} folds the expression and
     * reads it at 64 bits with {@code getLong()}, so a wrapped product hands
     * {@code TimestampSamplerFactory} a bucket width nobody wrote: {@code 86400 * 1000000} buckets
     * by 500654080us, about eight and a third minutes, rather than by a day. Every row of the
     * output is then a different aggregate, over timestamps that still look plausible because they
     * remain in the right era.
     * <p>
     * Nothing shows the operator which width the engine used. The stride has no projected
     * spelling, and unlike the bare-period spelling - which {@code SqlOptimiser.rewriteSampleBy}
     * turns into a visible {@code timestamp_floor_utc('1d', ts)} key function - this spelling is
     * excluded from that rewrite ({@code sampleByUnit == null} is one of its preconditions) and
     * runs on the {@code Sample By} cursor, whose plan names the fill mode and the aggregates and
     * never the stride. A wrapped stride and an exact one therefore print byte-identical plans.
     * <p>
     * The stride reader already requires a folded constant, so the guard sits after that check:
     * a bind variable or a column operand is refused first, and with its own message. One shape
     * still reaches the unproven arm - a constant whose type resolves into INT arithmetic but is
     * not on {@code isReadableAsNarrowInt}'s list, SYMBOL being the only such type, as in
     * {@code SAMPLE BY '1d'::symbol * 1_000_000 U}. Refusing it costs nothing a caller could have
     * wanted: {@code SymbolFunction.getInt()} answers the symbol key rather than a number, so that
     * statement used to throw a raw {@code ArithmeticException: / by zero} or bucket by the key.
     */
    static final int SUBJECT_SAMPLE_BY_INTERVAL = 2;
    /**
     * A window frame width - the {@code n} of {@code RANGE / ROWS BETWEEN n PRECEDING}, in an
     * {@code OVER (...)} clause or a {@code WINDOW JOIN}. {@code SqlOptimiser} reads it at 64 bits
     * off the folded constant, so a wrapped product that lands positive silently narrows the frame
     * and changes the aggregate on every row, and one that lands negative is reported as a sign
     * error rather than as the wrap that produced it.
     * <p>
     * A {@code WINDOW JOIN} bound may instead reference a master column, and that spelling never
     * reaches the constant reader at all - {@code tryEvalNonNegativeLongConstant} answers "dynamic"
     * on the first column reference it sees. {@code SqlCodeGenerator} compiles it against the
     * master metadata and {@code AsyncWindowJoinRecordCursorFactory.computeEffectiveBound()} reads
     * it per master row with the same {@code getLong()}, where a positive wrap narrows the frame
     * and a negative one is clamped to a zero-width frame. Nothing about a column operand can be
     * proven at compile time, so the guard fails closed on that path and refuses the arithmetic
     * whether or not it could wrap - which is what keeps the constant and column spellings of one
     * frame answering the same way.
     */
    static final int SUBJECT_WINDOW_FRAME_BOUND = 1;
    private static final String PARTITION_FILTER_REMEDIES = "widen an operand (1_000_000L, expr::long), use a timestamp literal, or bind the computed value itself";
    private static final String PARTITION_FILTER_UNPROVEN_COST = "match partitions the statement did not mean to name";
    private static final String PARTITION_FILTER_WRAP_COST = "matches partitions the statement did not mean to name";
    private static final String SAMPLE_BY_REMEDIES = "widen an operand (1_000_000L, expr::long) or write the interval with a unit suffix (1d, 24h)";
    private static final String SAMPLE_BY_UNPROVEN_COST = "bucket rows by an interval the statement did not mean to ask for";
    private static final String SAMPLE_BY_WRAP_COST = "buckets rows by an interval the statement did not mean to ask for";
    // Sentinel returned by foldExact when it cannot prove a value. It is unreachable as a real
    // result: operands are read at 32 bits or less, so the largest magnitude a single operation
    // can produce is 2^62.
    private static final long VALUE_NOT_PROVEN = Long.MAX_VALUE;
    private static final String WINDOW_FRAME_REMEDIES = "widen an operand (1_000_000L, expr::long) or write the width as a single literal";
    private static final String WINDOW_FRAME_UNPROVEN_COST = "measure a frame the statement did not mean to ask for";
    private static final String WINDOW_FRAME_WRAP_COST = "measures a frame the statement did not mean to ask for";

    private NarrowIntArithmetic() {
    }

    /**
     * Walks {@code node} and refuses the first narrow-int arithmetic node whose engine-computed
     * value differs from its mathematical one, or that cannot be proven either way.
     * <p>
     * Children are walked before parents, which is what makes reading an operand back at the
     * parent's own width exact: by the time a parent is judged, every descendant has already been
     * proven not to wrap.
     * <p>
     * Only nodes {@link #isNarrowIntArithmetic} accepts are compiled, which is deliberate: an
     * expression tree holds nodes that mean nothing on their own - the type name in
     * {@code x::timestamp}, the {@code epoch} field name in {@code extract(epoch from x)} - and
     * compiling one of those in isolation would fail with an error the statement does not deserve.
     * <p>
     * Compiling a sub-expression a second time defines nothing twice and shifts no positional
     * index: {@code FunctionParser.createIndexParameter} derives the index from the {@code $n}
     * token and only reads {@code BindVariableService}.
     *
     * @param functionParser   the caller's parser
     * @param node             root of the expression to walk, or {@code null}
     * @param metadata         metadata describing the record the expression compiles against
     * @param executionContext the compiling context
     * @param subject          one of {@link #SUBJECT_PARTITION_FILTER},
     *                         {@link #SUBJECT_SAMPLE_BY_INTERVAL},
     *                         {@link #SUBJECT_WINDOW_FRAME_BOUND}; decides what the refusal says
     *                         the wrap costs
     * @throws SqlException positioned at the arithmetic operator that wrapped or could not be
     *                      proven
     */
    static void rejectWrapped(
            FunctionParser functionParser,
            @Nullable ExpressionNode node,
            RecordMetadata metadata,
            SqlExecutionContext executionContext,
            int subject
    ) throws SqlException {
        if (node == null) {
            return;
        }
        if (node.paramCount > 2) {
            for (int i = 0, n = node.args.size(); i < n; i++) {
                rejectWrapped(functionParser, node.args.getQuick(i), metadata, executionContext, subject);
            }
        } else {
            rejectWrapped(functionParser, node.lhs, metadata, executionContext, subject);
            rejectWrapped(functionParser, node.rhs, metadata, executionContext, subject);
        }
        if (!isNarrowIntArithmetic(node)) {
            return;
        }
        final int width;
        final Function nodeFunction = functionParser.parseFunction(node, metadata, executionContext);
        try {
            width = nodeFunction != null ? narrowIntWidthOf(nodeFunction.getType()) : 0;
        } finally {
            Misc.free(nodeFunction);
        }
        if (width == 0) {
            return;
        }
        final long exact = foldExact(functionParser, node, width, metadata, executionContext);
        if (exact == Numbers.LONG_NULL) {
            return;
        }
        if (exact == VALUE_NOT_PROVEN) {
            if (node.paramCount == 1) {
                // Unary + and - over an operand only known once the statement runs. Neither can
                // reach a value the operand alone cannot: + is the identity, and - maps the width
                // onto itself, wrapping for the single input MIN, where -MIN is MIN again. So every
                // bound this node can produce is a bound the operand produces on its own, and the
                // operand on its own is a shape the guard accepts - it either carries no arithmetic
                // at all, or it is arithmetic this walk already judged before reaching the parent.
                // Refusing here therefore buys no protection and costs the retention idiom
                // dateadd('d', -$1, now()), whose INT-only stride has no widening to offer.
                return;
            }
            throw SqlException.$(node.position, "INT arithmetic overflow in ")
                    .put(subjectOf(subject))
                    .put(" cannot be ruled out: this computes at ")
                    .put(width)
                    .put(" bits and an operand is only known once the statement runs, so it can wrap and ")
                    .put(unprovenCostOf(subject))
                    .put("; ")
                    .put(remediesOf(subject));
        }
        final long wrapped = truncateToNarrowInt(exact, width);
        if (wrapped != exact) {
            throw SqlException.$(node.position, "INT arithmetic overflow in ")
                    .put(subjectOf(subject))
                    .put(": this computes at ")
                    .put(width)
                    .put(" bits and wraps to ")
                    .put(wrapped)
                    .put(" instead of ")
                    .put(exact)
                    .put(", which ")
                    .put(wrapCostOf(subject))
                    .put("; ")
                    .put(remediesOf(subject));
        }
    }

    /**
     * Evaluates one narrow-int arithmetic node at 64 bits, WITHOUT the wrapping the engine
     * applies, so {@link #rejectWrapped} can tell whether the engine's narrow computation lost
     * anything.
     * <p>
     * The caller walks children before parents, so every descendant of {@code node} has already
     * been proven not to wrap by the time this runs. That is what makes reading an operand back
     * at the node's own width exact: an operand that is itself arithmetic carries the same value
     * narrow and wide, and an operand that is a leaf is read with the very getter the arithmetic
     * factory calls on it.
     *
     * @param functionParser   the caller's parser
     * @param node             a node {@link #isNarrowIntArithmetic} accepted, whose compiled type
     *                         is {@code width} bits wide
     * @param width            8, 16 or 32
     * @param metadata         metadata describing the record the expression compiles against
     * @param executionContext the compiling context
     * @return the exact value; {@link Numbers#LONG_NULL} when the node evaluates to NULL, which
     * cannot over-match; or {@link #VALUE_NOT_PROVEN} when an operand is not a constant this
     * method can read
     * @throws SqlException propagated from compiling an operand
     */
    private static long foldExact(
            FunctionParser functionParser,
            ExpressionNode node,
            int width,
            RecordMetadata metadata,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (node.paramCount == 1) {
            final long operand = foldOperand(functionParser, node.rhs, width, metadata, executionContext);
            if (operand == VALUE_NOT_PROVEN || operand == Numbers.LONG_NULL) {
                return operand;
            }
            return Chars.equals(node.token, '-') ? -operand : operand;
        }
        final long lhs = foldOperand(functionParser, node.lhs, width, metadata, executionContext);
        if (lhs == VALUE_NOT_PROVEN || lhs == Numbers.LONG_NULL) {
            return lhs;
        }
        final long rhs = foldOperand(functionParser, node.rhs, width, metadata, executionContext);
        if (rhs == VALUE_NOT_PROVEN || rhs == Numbers.LONG_NULL) {
            return rhs;
        }
        if (Chars.equals(node.token, '+')) {
            return lhs + rhs;
        }
        if (Chars.equals(node.token, '-')) {
            return lhs - rhs;
        }
        if (Chars.equals(node.token, '*')) {
            return lhs * rhs;
        }
        if (rhs == 0) {
            // DivIntFunctionFactory and RemIntFunctionFactory both answer NULL for a zero divisor
            // rather than failing, so the node evaluates to NULL and cannot over-match
            return Numbers.LONG_NULL;
        }
        return Chars.equals(node.token, '/') ? lhs / rhs : lhs % rhs;
    }

    /**
     * Reads one operand of a narrow-int arithmetic node as the exact 64-bit number the engine
     * will feed to that arithmetic.
     * <p>
     * The read goes through the same getter the arithmetic factory uses - {@code getInt()} for a
     * 32-bit node - so a STRING constant is converted by the engine's own implicit cast rather
     * than by a rule this method invents, and a nested arithmetic operand yields the value it
     * already proved. A constant is required: a bind variable and any other runtime constant hold
     * no readable value until {@code init()} runs, so they report NOT-PROVEN and the caller
     * refuses the statement rather than guessing.
     *
     * @param functionParser   the caller's parser
     * @param operand          the operand node, or {@code null}
     * @param width            the parent node's width in bits: 8, 16 or 32
     * @param metadata         metadata describing the record the expression compiles against
     * @param executionContext the compiling context
     * @return the operand's value; {@link Numbers#LONG_NULL} when it is NULL; or
     * {@link #VALUE_NOT_PROVEN} when it is not a readable constant
     * @throws SqlException propagated from compiling the operand
     */
    private static long foldOperand(
            FunctionParser functionParser,
            @Nullable ExpressionNode operand,
            int width,
            RecordMetadata metadata,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (operand == null) {
            return VALUE_NOT_PROVEN;
        }
        final Function operandFunction = functionParser.parseFunction(operand, metadata, executionContext);
        try {
            if (operandFunction == null
                    || !operandFunction.isConstant()
                    || !isReadableAsNarrowInt(operandFunction.getType())) {
                return VALUE_NOT_PROVEN;
            }
            if (width == 8) {
                return operandFunction.getByte(null);
            }
            if (width == 16) {
                return operandFunction.getShort(null);
            }
            final int value = operandFunction.getInt(null);
            return value == Numbers.INT_NULL ? Numbers.LONG_NULL : value;
        } finally {
            Misc.free(operandFunction);
        }
    }

    /**
     * Reports whether the node is an arithmetic operator whose result this guard has to prove:
     * binary {@code + - * / %}, or the unary {@code + -} the expression parser builds over a
     * signed operand.
     */
    private static boolean isNarrowIntArithmetic(@Nullable ExpressionNode node) {
        if (node == null || node.type != ExpressionNode.OPERATION || node.token == null) {
            return false;
        }
        if (node.paramCount == 1) {
            return Chars.equals(node.token, '+') || Chars.equals(node.token, '-');
        }
        return node.paramCount == 2
                && (Chars.equals(node.token, '+')
                || Chars.equals(node.token, '-')
                || Chars.equals(node.token, '*')
                || Chars.equals(node.token, '/')
                || Chars.equals(node.token, '%'));
    }

    /**
     * Reports whether a constant of this type can be read back as the narrow integer that its
     * parent arithmetic reads.
     * <p>
     * Overload resolution inserts no cast function for these. {@code MulIntFunctionFactory} and
     * its siblings call {@code getInt()} straight on whatever argument they were handed, and
     * {@code StrFunction.getInt()} answers with {@code SqlUtil.implicitCastStrAsInt}. That is why
     * {@code '1720468802' * 1000000} is INT arithmetic over a STRING operand, and why reading the
     * operand back the same way reproduces the engine's own value instead of guessing at a cast
     * that lives in the parent's overload resolution.
     * <p>
     * Every other type - DECIMAL, DOUBLE, LONG, an array - is reported as unreadable, so the fold
     * reports NOT-PROVEN and the statement is refused rather than judged on a value this method
     * invented. None of those can in fact reach a narrow-int arithmetic node, because an operand of
     * any of them makes the parent resolve to a wider factory. SYMBOL is the one type that does
     * reach it - its overload list stops at INT - and {@code SymbolFunction.getInt()} answers a
     * symbol key rather than a number, so refusing it is a decision, not a safety net.
     */
    private static boolean isReadableAsNarrowInt(int type) {
        final short tag = ColumnType.tagOf(type);
        return tag == ColumnType.BYTE
                || tag == ColumnType.SHORT
                || tag == ColumnType.INT
                || tag == ColumnType.CHAR
                || tag == ColumnType.STRING
                || tag == ColumnType.VARCHAR
                || tag == ColumnType.NULL;
    }

    /**
     * Returns the number of bits an expression of this type computes in when it is too narrow to
     * hold a timestamp - 8 for BYTE, 16 for SHORT, 32 for INT - or 0 for every type that is wide
     * enough, or is not an integer at all.
     * <p>
     * Only these three wrap below 64 bits, and only these three reach a 64-bit consumer through a
     * getter that sign-extends a narrow value rather than recomputing at 64 bits, which is what
     * makes a wrapped bound match every partition floor of a table holding modern data, or measure
     * a frame nobody asked for.
     */
    private static int narrowIntWidthOf(int type) {
        final short tag = ColumnType.tagOf(type);
        if (tag == ColumnType.BYTE) {
            return 8;
        }
        if (tag == ColumnType.SHORT) {
            return 16;
        }
        return tag == ColumnType.INT ? 32 : 0;
    }

    private static String remediesOf(int subject) {
        return switch (subject) {
            case SUBJECT_PARTITION_FILTER -> PARTITION_FILTER_REMEDIES;
            case SUBJECT_SAMPLE_BY_INTERVAL -> SAMPLE_BY_REMEDIES;
            default -> WINDOW_FRAME_REMEDIES;
        };
    }

    private static String subjectOf(int subject) {
        return switch (subject) {
            case SUBJECT_PARTITION_FILTER -> "partition filter";
            case SUBJECT_SAMPLE_BY_INTERVAL -> "SAMPLE BY interval";
            default -> "window frame bound";
        };
    }

    private static String unprovenCostOf(int subject) {
        return switch (subject) {
            case SUBJECT_PARTITION_FILTER -> PARTITION_FILTER_UNPROVEN_COST;
            case SUBJECT_SAMPLE_BY_INTERVAL -> SAMPLE_BY_UNPROVEN_COST;
            default -> WINDOW_FRAME_UNPROVEN_COST;
        };
    }

    private static String wrapCostOf(int subject) {
        return switch (subject) {
            case SUBJECT_PARTITION_FILTER -> PARTITION_FILTER_WRAP_COST;
            case SUBJECT_SAMPLE_BY_INTERVAL -> SAMPLE_BY_WRAP_COST;
            default -> WINDOW_FRAME_WRAP_COST;
        };
    }

    /**
     * Truncates a 64-bit value to the given narrow width, exactly as the engine's arithmetic
     * does. {@code truncateToNarrowInt(v, width) == v} is therefore the test for "this
     * computation did not wrap", and when it did wrap the value returned here is the one the
     * engine actually produced - which is what the error message quotes back to the operator.
     */
    private static long truncateToNarrowInt(long value, int width) {
        if (width == 8) {
            return (byte) value;
        }
        return width == 16 ? (short) value : (int) value;
    }
}

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

package io.questdb.griffin.engine.window;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.TypeExpression;
import io.questdb.std.Long256;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;

/**
 * The canonical identity of one compiled PARTITION BY term: the string two window functions
 * have to render the same for a {@link WindowMapSpec} to claim they key their partition state
 * the same way.
 * <p>
 * It exists because a compiled {@link Function} has no identity of its own worth the name. A
 * factory folds constant arguments into the state of the function it builds, so
 * {@code timestamp_floor('h', ts)} and {@code timestamp_floor('d', ts)} compile to one class over
 * one argument, and a proof built on the compiled tree would fuse the two. What is canonical here
 * is the <b>bound tree</b> the function was compiled from, resolved against the input layout the
 * compile ran under:
 * <ul>
 *     <li>a <b>column</b> renders as its index in the input and its type, so every spelling of
 *     one column is one identity and a column the input lacks gets none;</li>
 *     <li>a <b>constant</b> renders as its type and its value, so two constants that bind to one
 *     value are one identity whatever text they were written as;</li>
 *     <li>a <b>type</b> argument renders as the type;</li>
 *     <li>a <b>function call</b> renders as its name, the signature of the overload it binds to,
 *     its arity and its arguments in order - so the operation itself is part of the identity
 *     rather than merely the operands;</li>
 *     <li><b>everything else declines</b>: a bind variable, a subquery, an outer column. Each of
 *     them could be given an identity later, and none of them can be given one by omission.</li>
 * </ul>
 * The compiled function is read for one thing the tree cannot say: whether evaluating it twice
 * answers twice the same. A {@code rnd_*} call renders identically to itself and produces a
 * different partition on every evaluation, so a non-deterministic or random term declines
 * however canonical its tree is.
 *
 * <h2>Why an identical tree is an identical function</h2>
 * The two terms this compares were bound against one input inside one statement, which is the
 * only scope a {@link WindowMapSpec} is ever compared in. The tree names the overload each call
 * binds to, and building a function from an overload, its arguments and a configuration is
 * deterministic, so two identical trees compile to two functions that agree on every row - and
 * the type they compile to is carried by the spec's key column types beside this, rather than
 * assumed here.
 * <p>
 * The rendering is deliberately not SQL. It carries resolved column indexes rather than the
 * names the query wrote, and it is never parsed back - it is compared, and printed when a plan
 * has to be explained.
 */
public final class WindowKeyExpressionIdentity {
    /**
     * Separates the terms of one PARTITION BY list. It cannot occur inside a rendered term:
     * a term's own punctuation is the bracket, the comma and the two leading markers below.
     */
    public static final char TERM_SEPARATOR = ';';
    /**
     * The recursion bound. A PARTITION BY expression nested deeper than this declines rather
     * than risking the stack - the tree came from a parser with no depth limit of its own.
     */
    private static final int MAX_DEPTH = 32;

    private WindowKeyExpressionIdentity() {
    }

    /**
     * Renders {@code term}'s canonical identity into {@code sink}, and reports whether this
     * build can name it at all.
     * <p>
     * A false answer leaves {@code sink} holding whatever partial rendering got that far, so a
     * caller that keeps the sink must discard it. Every false is an ordinary answer: the window
     * forms no group and its functions keep the private maps they own outside one.
     *
     * @param term     the bound PARTITION BY term
     * @param compiled the function {@code term} compiled to, read only for whether it evaluates
     *                 to the same value twice
     * @param input    the input layout {@code term} was bound against, which is what a column
     *                 resolves through
     */
    public static boolean render(BoundExpression term, Function compiled, OutputSchema input, CharSink<?> sink) {
        return compiled != null && !compiled.isNonDeterministic() && !compiled.isRandom()
                && renderNode(term, input, sink, 0);
    }

    /**
     * Renders the identity of a term that is a direct column reference, which a caller holding
     * the compiled function knows without a tree: the index it reads and the type it reads it
     * as.
     * <p>
     * It is the same rendering {@link #render} produces for a bound column, and it is here so
     * that one place defines it - a caller that resolved the column off the compiled function and
     * one that resolved it off the bound tree have to agree, or two spellings of one key would be
     * two groups.
     */
    public static void renderColumn(int columnIndex, int columnType, @NotNull CharSink<?> sink) {
        sink.putAscii('#').put(columnIndex).putAscii(':').put(columnType);
    }

    private static boolean renderNode(BoundExpression node, OutputSchema input, CharSink<?> sink, int depth) {
        if (depth > MAX_DEPTH) {
            return false;
        }
        if (node instanceof ColumnExpression column) {
            final int index = input.getColumnIndexById(column.getColumnId());
            if (index < 0) {
                return false;
            }
            renderColumn(index, column.getDataType(), sink);
            return true;
        }
        if (node instanceof ConstantExpression constant) {
            sink.putAscii('=').put(constant.getDataType()).putAscii(':');
            switch (ColumnType.tagOf(constant.getDataType())) {
                case ColumnType.STRING, ColumnType.SYMBOL -> {
                    final String value = constant.getStrValue();
                    sink.put(value == null ? -1 : value.length()).putAscii(':').put(value);
                }
                case ColumnType.VARCHAR -> {
                    final Utf8Sequence value = constant.getVarcharValue();
                    sink.put(value == null ? -1 : value.size()).putAscii(':');
                    if (value != null) {
                        for (int i = 0; i < value.size(); i++) {
                            sink.put(value.byteAt(i)).putAscii(',');
                        }
                    }
                }
                case ColumnType.UUID, ColumnType.LONG128 ->
                        sink.put(constant.getLong128Lo()).putAscii(',').put(constant.getLong128Hi());
                case ColumnType.LONG256 -> {
                    final Long256 value = constant.getLong256Value();
                    sink.put(value.getLong0()).putAscii(',').put(value.getLong1()).putAscii(',')
                            .put(value.getLong2()).putAscii(',').put(value.getLong3());
                }
                case ColumnType.DECIMAL128, ColumnType.DECIMAL256 -> sink.put(constant.getDecimalHh()).putAscii(',')
                        .put(constant.getDecimalHl()).putAscii(',').put(constant.getDecimalLh()).putAscii(',')
                        .put(constant.getLongValue());
                case ColumnType.INTERVAL ->
                        sink.put(constant.getLongValue()).putAscii(',').put(constant.getIntervalHi());
                default -> sink.put(constant.getLongValue());
            }
            return true;
        }
        if (node instanceof TypeExpression) {
            sink.putAscii('T').put(node.getDataType());
            return true;
        }
        if (node instanceof FunctionExpression call) {
            final String name = call.getName();
            final String signature = call.getSignature();
            sink.putAscii('!').put(name.length()).putAscii(':').put(name)
                    .putAscii('/').put(signature.length()).putAscii(':').put(signature)
                    .putAscii('/').put(call.getArgumentCount()).putAscii('(');
            for (int i = 0; i < call.getArgumentCount(); i++) {
                if (i > 0) {
                    sink.putAscii(',');
                }
                if (!renderNode(call.argumentAt(i), input, sink, depth + 1)) {
                    return false;
                }
            }
            sink.putAscii(')');
            return true;
        }
        return false;
    }

}

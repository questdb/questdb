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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.Chars;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The canonical identity of one compiled PARTITION BY term: the string two window functions
 * have to render the same for a {@link WindowMapSpec} to claim they key their partition state
 * the same way.
 * <p>
 * It exists because a compiled {@link Function} has no identity of its own worth the name.
 * {@link Function#isEquivalentTo} is documented as best-effort and compares a node's arguments
 * without comparing the node - two {@link io.questdb.griffin.engine.functions.BinaryFunction}s
 * over one pair of columns answer true whether they add or subtract - so a proof built on it
 * would fuse {@code partition by a + b} with {@code partition by a - b}. What is canonical here
 * is the <b>parsed tree</b> the compiler handed the function parser, resolved against the
 * metadata that parse ran under:
 * <ul>
 *     <li>a <b>column reference</b> renders as its resolved index and that column's type, so
 *     {@code k}, {@code K} and {@code t.k} are one identity and no unresolvable name gets
 *     one;</li>
 *     <li>a <b>constant</b> renders as its token verbatim, case and quoting included, because
 *     the token is what the parser folds into a value;</li>
 *     <li>an <b>operator or function call</b> renders as its lower-cased token, its arity, and
 *     its children in order - so the operation itself is part of the identity rather than
 *     merely the operands;</li>
 *     <li><b>everything else declines</b>: a bind variable, a subquery, an array access, a
 *     member access, a set operation. Each of them could be given an identity later, and none
 *     of them can be given one by omission.</li>
 * </ul>
 * The compiled function is read for one thing the tree cannot say: whether evaluating it twice
 * answers twice the same. A {@code rnd_*} call renders identically to itself and produces a
 * different partition on every evaluation, so a non-deterministic or random term declines
 * however canonical its tree is.
 *
 * <h2>Why an identical tree is an identical function</h2>
 * The two terms this compares were parsed by one {@code FunctionParser} against one metadata
 * inside one statement, which is the only scope a {@link WindowMapSpec} is ever compared in.
 * Signature resolution, implicit casts and constant folding are all deterministic functions of
 * that triple, so two identical trees compile to two functions that agree on every row - and
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
     * Renders the identity of a term that is a direct column reference, which a caller holding
     * the compiled function knows without a tree: the index it reads and the type it reads it
     * as.
     * <p>
     * It is the same rendering {@link #render} produces for a LITERAL, and it is here so that
     * one place defines it - a caller that resolved the column off the compiled function and
     * one that resolved it off the parsed name have to agree, or two spellings of one key would
     * be two groups.
     */
    public static void renderColumn(int columnIndex, int columnType, @NotNull CharSink<?> sink) {
        sink.putAscii('#').put(columnIndex).putAscii(':').put(columnType);
    }

    /**
     * Renders {@code term}'s canonical identity into {@code sink}, and reports whether this
     * build can name it at all.
     * <p>
     * A false answer leaves {@code sink} holding whatever partial rendering got that far, so a
     * caller that keeps the sink must discard it. Every false is an ordinary answer: the window
     * forms no group and its functions keep the private maps they own outside one.
     *
     * @param term     the PARTITION BY term as parsed
     * @param compiled the function {@code term} compiled to, read only for whether it evaluates
     *                 to the same value twice
     * @param metadata the metadata {@code term} was compiled against, which is what a column
     *                 name resolves through
     */
    public static boolean render(
            @Nullable ExpressionNode term,
            @Nullable Function compiled,
            @NotNull RecordMetadata metadata,
            @NotNull CharSink<?> sink
    ) {
        if (compiled == null || compiled.isNonDeterministic() || compiled.isRandom()) {
            return false;
        }
        return renderNode(term, metadata, sink, 0);
    }

    private static boolean renderChildren(
            @NotNull ExpressionNode node,
            @NotNull RecordMetadata metadata,
            @NotNull CharSink<?> sink,
            int depth
    ) {
        // The argument invariant ExpressionNode states and its own comparison reads: fewer than
        // three arguments live in lhs/rhs, three or more in args. The number rendered is how
        // many child slots follow - two for the lhs/rhs form, whatever args holds otherwise -
        // so two arities cannot collide and neither can the two forms.
        final int argCount = node.args.size();
        sink.putAscii('/').put(argCount < 3 ? 2 : argCount).putAscii('(');
        if (argCount < 3) {
            if (!renderChild(node.lhs, metadata, sink, depth)) {
                return false;
            }
            sink.putAscii(',');
            if (!renderChild(node.rhs, metadata, sink, depth)) {
                return false;
            }
        } else {
            for (int i = 0; i < argCount; i++) {
                if (i > 0) {
                    sink.putAscii(',');
                }
                if (!renderChild(node.args.getQuick(i), metadata, sink, depth)) {
                    return false;
                }
            }
        }
        sink.putAscii(')');
        return true;
    }

    /**
     * Renders one child slot, which an operator may legitimately leave empty - a unary
     * operation carries its operand in {@code rhs} and nothing in {@code lhs}.
     */
    private static boolean renderChild(
            @Nullable ExpressionNode child,
            @NotNull RecordMetadata metadata,
            @NotNull CharSink<?> sink,
            int depth
    ) {
        if (child == null) {
            sink.putAscii('_');
            return true;
        }
        return renderNode(child, metadata, sink, depth + 1);
    }

    private static boolean renderNode(
            @Nullable ExpressionNode node,
            @NotNull RecordMetadata metadata,
            @NotNull CharSink<?> sink,
            int depth
    ) {
        if (node == null || node.token == null || depth > MAX_DEPTH) {
            return false;
        }
        switch (node.type) {
            case ExpressionNode.LITERAL: {
                final int columnIndex = SqlUtil.getColumnIndexQuiet(metadata, node.token);
                if (columnIndex < 0) {
                    return false;
                }
                // The index and the type together: the index is which column the term reads and
                // the type is what it reads it as, and a metadata this identity outlives could
                // otherwise be read back through the wrong one.
                renderColumn(columnIndex, metadata.getColumnType(columnIndex), sink);
                return true;
            }
            case ExpressionNode.CONSTANT:
                // Verbatim, quoting and case included: 'a' and 'A' are two values, and the
                // parser folds the token rather than a normalization of it.
                sink.putAscii('=').put(node.token);
                return true;
            case ExpressionNode.OPERATION:
            case ExpressionNode.FUNCTION: {
                // Lower-cased for the reason ExpressionNode's own exact comparison compares a
                // FUNCTION token case-insensitively: SQL resolves the name that way.
                sink.putAscii('!');
                for (int i = 0, n = node.token.length(); i < n; i++) {
                    sink.putAscii(Chars.toLowerCaseAscii(node.token.charAt(i)));
                }
                return renderChildren(node, metadata, sink, depth);
            }
            default:
                // A bind variable, a subquery, an array or member access, a set operation, and
                // whatever the parser grows next. Declining what is not named is what keeps the
                // proof a proof.
                return false;
        }
    }
}

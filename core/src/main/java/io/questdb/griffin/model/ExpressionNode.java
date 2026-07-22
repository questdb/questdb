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

package io.questdb.griffin.model;

import io.questdb.griffin.OperatorExpression;
import io.questdb.griffin.OperatorRegistry;
import io.questdb.griffin.SqlKeywords;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;
import io.questdb.std.ObjectPool;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public class ExpressionNode implements Mutable, Sinkable {
    public static final int ARRAY_ACCESS = 1;
    public static final int ARRAY_CONSTRUCTOR = ARRAY_ACCESS + 1;
    public static final int BIND_VARIABLE = ARRAY_CONSTRUCTOR + 1;
    public static final int CONSTANT = BIND_VARIABLE + 1;
    public static final int CONTROL = CONSTANT + 1;
    public static final int FUNCTION = CONTROL + 1;
    public static final int LITERAL = FUNCTION + 1;
    public static final int MEMBER_ACCESS = LITERAL + 1;
    public static final int OPERATION = MEMBER_ACCESS + 1;
    public static final int QUERY = OPERATION + 1;
    public static final int SET_OPERATION = QUERY + 1;
    public static final ExpressionNodeFactory FACTORY = new ExpressionNodeFactory();
    public static final int UNKNOWN = 0;
    public final ObjList<ExpressionNode> args = new ObjList<>(4);
    public boolean implemented;
    public boolean innerPredicate = false;
    public int intrinsicValue = IntrinsicModel.UNDEFINED;
    public boolean isConstantExpression;
    public int lateralDepth;
    public ExpressionNode lhs;
    // The expression parser (ExpressionParser.onNode) guarantees:
    // - paramCount == 1: rhs is non-null, lhs is null.
    // - paramCount == 2: both lhs and rhs are non-null.
    // - paramCount > 2: children are stored in args; each entry is non-null.
    // No later transformation violates these invariants.
    public int paramCount;
    public int position;
    public int precedence;
    public IQueryModel queryModel;
    public ExpressionNode rhs;
    public CharSequence token;
    public int type;
    public WindowExpression windowExpression;
    // Cached constant-fold results for reassociateConstants, valid only while
    // isConstantExpression is true. cacheConstantFold populates them bottom-up the
    // moment a node is marked constant, so isReassociationSafe reads a subtree's fold
    // in O(1) instead of re-walking the accumulating constant chain at every level
    // (which is O(n^2) overall).
    private long constFoldLongValue;  // LONG-width fold, meaningful iff isConstFoldLongValid
    private boolean isConstFoldLongValid;
    private boolean isConstFoldWidening;

    // IMPORTANT: update deepClone method after adding a new field
    private ExpressionNode() {
    }

    public static boolean compareNodesExact(ExpressionNode a, ExpressionNode b) {
        if (a == null && b == null) {
            return true;
        }
        if (a == null || b == null || a.type != b.type) {
            return false;
        }
        return (a.type == FUNCTION || a.type == LITERAL ? Chars.equalsIgnoreCase(a.token, b.token) : Chars.equals(a.token, b.token))
                && compareArgsExact(a, b)
                && compareWindowExpressions(a.windowExpression, b.windowExpression);
    }

    public static boolean compareNodesGroupBy(
            ExpressionNode groupByExpr,
            ExpressionNode columnExpr,
            IQueryModel translatingModel
    ) {
        if (groupByExpr == null && columnExpr == null) {
            return true;
        }

        if (groupByExpr == null || columnExpr == null || groupByExpr.type != columnExpr.type) {
            return false;
        }

        if (!Chars.equals(groupByExpr.token, columnExpr.token)) {
            int index = translatingModel.getAliasToColumnMap().keyIndex(columnExpr.token);
            if (index > -1) {
                return false;
            }

            final QueryColumn qc = translatingModel.getAliasToColumnMap().valueAt(index);
            final CharSequence tok = groupByExpr.token;
            final CharSequence qcTok = qc.getAst().token;
            if (Chars.equals(qcTok, tok)) {
                return true;
            }

            int dot = Chars.indexOfLastUnquoted(tok, '.');
            if (dot > -1
                    && translatingModel.getModelAliasIndex(tok, 0, dot) > -1
                    && Chars.equals(qcTok, tok, dot + 1, tok.length())) {
                return compareArgs(groupByExpr, columnExpr, translatingModel);
            }

            return false;
        }

        return compareArgs(groupByExpr, columnExpr, translatingModel);
    }

    public static boolean compareWindowExpressions(WindowExpression a, WindowExpression b) {
        if (a == null && b == null) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }
        // Compare frame specification
        if (a.getFramingMode() != b.getFramingMode()
                || a.getRowsLo() != b.getRowsLo()
                || a.getRowsHi() != b.getRowsHi()
                || a.getRowsLoKind() != b.getRowsLoKind()
                || a.getRowsHiKind() != b.getRowsHiKind()
                || a.getRowsLoExprTimeUnit() != b.getRowsLoExprTimeUnit()
                || a.getRowsHiExprTimeUnit() != b.getRowsHiExprTimeUnit()
                || a.getExclusionKind() != b.getExclusionKind()
                || a.isIgnoreNulls() != b.isIgnoreNulls()) {
            return false;
        }
        // Compare frame boundary expressions
        if (!compareNodesExact(a.getRowsLoExpr(), b.getRowsLoExpr())
                || !compareNodesExact(a.getRowsHiExpr(), b.getRowsHiExpr())) {
            return false;
        }
        // Compare PARTITION BY
        ObjList<ExpressionNode> aPartitionBy = a.getPartitionBy();
        ObjList<ExpressionNode> bPartitionBy = b.getPartitionBy();
        if (aPartitionBy.size() != bPartitionBy.size()) {
            return false;
        }
        for (int i = 0, n = aPartitionBy.size(); i < n; i++) {
            if (!compareNodesExact(aPartitionBy.getQuick(i), bPartitionBy.getQuick(i))) {
                return false;
            }
        }
        // Compare ORDER BY
        ObjList<ExpressionNode> aOrderBy = a.getOrderBy();
        ObjList<ExpressionNode> bOrderBy = b.getOrderBy();
        IntList aOrderByDir = a.getOrderByDirection();
        IntList bOrderByDir = b.getOrderByDirection();
        if (aOrderBy.size() != bOrderBy.size()) {
            return false;
        }
        for (int i = 0, n = aOrderBy.size(); i < n; i++) {
            if (!compareNodesExact(aOrderBy.getQuick(i), bOrderBy.getQuick(i))
                    || aOrderByDir.getQuick(i) != bOrderByDir.getQuick(i)) {
                return false;
            }
        }
        return true;
    }

    public static ExpressionNode deepClone(final ObjectPool<ExpressionNode> pool, final ExpressionNode node) {
        if (node == null) {
            return null;
        }
        ExpressionNode copy = pool.next();
        for (int i = 0, n = node.args.size(); i < n; i++) {
            copy.args.add(ExpressionNode.deepClone(pool, node.args.get(i)));
        }
        copy.token = node.token;
        copy.queryModel = node.queryModel;
        copy.precedence = node.precedence;
        copy.position = node.position;
        copy.lhs = ExpressionNode.deepClone(pool, node.lhs);
        copy.rhs = ExpressionNode.deepClone(pool, node.rhs);
        copy.type = node.type;
        copy.paramCount = node.paramCount;
        copy.intrinsicValue = node.intrinsicValue;
        copy.isConstantExpression = node.isConstantExpression;
        copy.innerPredicate = node.innerPredicate;
        copy.implemented = node.implemented;
        copy.windowExpression = node.windowExpression; // shallow copy - WindowColumn is pooled
        copy.lateralDepth = node.lateralDepth;
        copy.constFoldLongValue = node.constFoldLongValue;
        copy.isConstFoldLongValid = node.isConstFoldLongValid;
        copy.isConstFoldWidening = node.isConstFoldWidening;
        return copy;
    }

    /**
     * Computes a hash code for an expression node tree that is consistent with compareNodesExact().
     * Two nodes that compare equal will have the same hash code.
     */
    public static int deepHashCode(ExpressionNode node) {
        if (node == null) {
            return 0;
        }
        int hash = node.type;
        if (node.token != null) {
            // Use content-based hash (Chars.lowerCaseHashCode) for all node types.
            // This is consistent with compareNodesExact which uses Chars.equalsIgnoreCase
            // for FUNCTION/LITERAL and Chars.equals for other types - equal strings always
            // have equal lowercase hashes, satisfying the hash/equality contract.
            hash = 31 * hash + Chars.lowerCaseHashCode(node.token);
        }
        // Hash children - must be consistent with compareArgsExact()
        // When args.size() < 3, comparison uses lhs/rhs; otherwise uses args
        int argsSize = node.args.size();
        if (argsSize < 3) {
            hash = 31 * hash + deepHashCode(node.lhs);
            hash = 31 * hash + deepHashCode(node.rhs);
        } else {
            for (int i = 0; i < argsSize; i++) {
                hash = 31 * hash + deepHashCode(node.args.getQuick(i));
            }
        }
        // Hash window expression
        hash = 31 * hash + hashWindowExpression(node.windowExpression);
        return hash;
    }

    /**
     * Computes a hash code for a WindowExpression that is consistent with compareWindowExpressions().
     */
    public static int hashWindowExpression(WindowExpression w) {
        if (w == null) {
            return 0;
        }
        int hash = w.getFramingMode();
        hash = 31 * hash + Long.hashCode(w.getRowsLo());
        hash = 31 * hash + Long.hashCode(w.getRowsHi());
        hash = 31 * hash + w.getRowsLoKind();
        hash = 31 * hash + w.getRowsHiKind();
        hash = 31 * hash + w.getRowsLoExprTimeUnit();
        hash = 31 * hash + w.getRowsHiExprTimeUnit();
        hash = 31 * hash + w.getExclusionKind();
        hash = 31 * hash + (w.isIgnoreNulls() ? 1 : 0);
        // Hash frame boundary expressions
        hash = 31 * hash + deepHashCode(w.getRowsLoExpr());
        hash = 31 * hash + deepHashCode(w.getRowsHiExpr());
        // Hash PARTITION BY
        ObjList<ExpressionNode> partitionBy = w.getPartitionBy();
        for (int i = 0, n = partitionBy.size(); i < n; i++) {
            hash = 31 * hash + deepHashCode(partitionBy.getQuick(i));
        }
        // Hash ORDER BY (including direction)
        ObjList<ExpressionNode> orderBy = w.getOrderBy();
        IntList orderByDir = w.getOrderByDirection();
        for (int i = 0, n = orderBy.size(); i < n; i++) {
            hash = 31 * hash + deepHashCode(orderBy.getQuick(i));
            hash = 31 * hash + orderByDir.getQuick(i);
        }
        return hash;
    }

    @Override
    public void clear() {
        args.clear();
        token = null;
        precedence = 0;
        position = 0;
        lhs = null;
        rhs = null;
        type = UNKNOWN;
        paramCount = 0;
        intrinsicValue = IntrinsicModel.UNDEFINED;
        isConstantExpression = false;
        queryModel = null;
        innerPredicate = false;
        implemented = false;
        windowExpression = null;
        lateralDepth = 0;
        constFoldLongValue = 0;
        isConstFoldLongValid = false;
        isConstFoldWidening = false;
    }

    public ExpressionNode copyFrom(final ExpressionNode other) {
        this.clear();
        for (int i = 0, n = other.args.size(); i < n; i++) {
            this.args.add(other.args.get(i));
        }
        this.token = other.token;
        this.queryModel = other.queryModel;
        this.precedence = other.precedence;
        this.position = other.position;
        this.lhs = other.lhs;
        this.rhs = other.rhs;
        this.type = other.type;
        this.paramCount = other.paramCount;
        this.intrinsicValue = other.intrinsicValue;
        this.isConstantExpression = other.isConstantExpression;
        this.innerPredicate = other.innerPredicate;
        this.windowExpression = other.windowExpression;
        this.lateralDepth = other.lateralDepth;
        this.constFoldLongValue = other.constFoldLongValue;
        this.isConstFoldLongValid = other.isConstFoldLongValid;
        this.isConstFoldWidening = other.isConstFoldWidening;
        return this;
    }

    public boolean isWildcard() {
        return type == LITERAL && Chars.endsWith(token, '*');
    }

    public boolean noLeafs() {
        return lhs == null || rhs == null;
    }

    public ExpressionNode of(int type, CharSequence token, int precedence, int position) {
        clear();
        // override literal with bind variable
        if (
                type == LITERAL
                        && token != null
                        && !token.isEmpty()
                        && ((token.charAt(0) == '$' && Numbers.isDecimal(token, 1)) || token.charAt(0) == ':')
        ) {
            this.type = BIND_VARIABLE;
        } else {
            this.type = type;
        }
        this.precedence = precedence;
        this.token = token;
        this.position = position;
        return this;
    }

    /**
     * Walks this expression tree bottom-up and regroups adjacent constant
     * operands of associative (and, where needed, commutative) binary operators
     * so that constant folding can collapse them into a single constant.
     *
     * <p>For example, {@code (col + 1) + 4} is rewritten to {@code col + (1 + 4)},
     * which the function parser then folds to {@code col + 5}.</p>
     *
     * <p>The method handles four structural patterns. In each pattern, {@code A}
     * is a non-constant subtree, {@code C1} and {@code C2} are constant subtrees,
     * and {@code op} is the same binary operator at both levels:</p>
     *
     * <ul>
     *   <li><b>Pattern A</b> — {@code (A op C1) op C2 → A op (C1 op C2)}.
     *       Requires only associativity (natural left-associative chain).</li>
     *   <li><b>Pattern B</b> — {@code (C1 op A) op C2 → A op (C1 op C2)}.
     *       Also requires commutativity to move {@code A} to the outer position.</li>
     *   <li><b>Mirror A</b> — {@code C2 op (A op C1) → A op (C2 op C1)}.
     *       Also requires commutativity to swap the outer operands.</li>
     *   <li><b>Mirror B</b> — {@code C2 op (C1 op A) → (C2 op C1) op A}.
     *       Requires only associativity (pure regrouping).</li>
     * </ul>
     *
     * <p>The rewrite is purely structural: it relinks existing {@link ExpressionNode}
     * instances without allocating new nodes.</p>
     *
     * <p>Numeric reassociation is deliberately conservative. Floating-point and DECIMAL
     * arithmetic is not associative under rounding, overflow, precision, and scale rules.
     * Integer arithmetic is also non-associative in QuestDB because wrapped intermediate values
     * can equal the reserved INT_NULL or LONG_NULL sentinel for row values unavailable to this
     * pre-type-resolution pass. Consequently, only non-numeric associative operators are eligible
     * for reassociation.</p>
     *
     * @return {@code true} if this subtree is entirely constant (every leaf is a
     * constant and every interior node is a binary operation on constants),
     * {@code false} otherwise
     */
    public boolean reassociateConstants(boolean cairoSqlLegacyOperatorPrecedence) {
        if (type == CONSTANT) {
            isConstantExpression = true;
            cacheConstantFold();
            return true;
        }

        if (paramCount > 2) {
            // For n-ary operators, we reassociate inner arguments without changing the tree structure.
            for (int i = 0; i < paramCount; i++) {
                // Every args child is guaranteed non-null by the expression parser (ExpressionParser.onNode)
                // and no later transformation violates this invariant.
                args.getQuick(i).reassociateConstants(cairoSqlLegacyOperatorPrecedence);
            }
            return false;
        }

        // Recurse bottom-up. Each child caches its result in isConstantExpression (and its
        // fold triple via cacheConstantFold), so grandchild constancy checks and the
        // reassociation-safety guard below are O(1) field reads.
        boolean lhsConst = lhs != null && lhs.reassociateConstants(cairoSqlLegacyOperatorPrecedence);
        boolean rhsConst = rhs != null && rhs.reassociateConstants(cairoSqlLegacyOperatorPrecedence);

        if (type != OPERATION || paramCount != 2) {
            return false;
        }

        if (lhsConst && rhsConst) {
            isConstantExpression = true;
            cacheConstantFold();
            return true;
        }

        // op is never null: every OPERATION node with paramCount == 2 gets its token
        // from the operator registry during parsing or optimization, and the same
        // registry is selected here via cairoSqlLegacyOperatorPrecedence.
        OperatorExpression op = OperatorExpression.chooseRegistry(cairoSqlLegacyOperatorPrecedence).getOperatorDefinition(token);
        if (!op.isAssociative()) {
            return false;
        }

        // In every pattern below, !lhsConst (or !rhsConst) guarantees that the
        // inner OPERATION node has at most one constant child. So when we confirm
        // which grandchild IS constant, the other one is implicitly NOT constant
        // — no need to check it.

        if (rhsConst && lhs.type == OPERATION
                && lhs.paramCount == 2
                && lhs.token.equals(token)) {
            if (lhs.rhs.isConstantExpression) {
                // Pattern A: (A op C1) op C2 → A op (C1 op C2)
                if (isReassociationSafe(lhs.rhs, rhs)) {
                    ExpressionNode inner = lhs;
                    ExpressionNode a = inner.lhs;
                    ExpressionNode c1 = inner.rhs;
                    ExpressionNode c2 = rhs;
                    this.lhs = a;
                    this.rhs = inner;
                    inner.lhs = c1;
                    inner.rhs = c2;
                    inner.isConstantExpression = true;
                    inner.cacheConstantFold();
                }
            } else if (op.isCommutative() && lhs.lhs.isConstantExpression) {
                // Pattern B: (C1 op A) op C2 → A op (C1 op C2)
                if (isReassociationSafe(lhs.lhs, rhs)) {
                    ExpressionNode inner = lhs;
                    ExpressionNode c1 = inner.lhs;
                    ExpressionNode a = inner.rhs;
                    ExpressionNode c2 = rhs;
                    this.lhs = a;
                    this.rhs = inner;
                    inner.lhs = c1;
                    inner.rhs = c2;
                    inner.isConstantExpression = true;
                    inner.cacheConstantFold();
                }
            }

            return false;
        }

        if (lhsConst && rhs.type == OPERATION
                && rhs.paramCount == 2
                && rhs.token.equals(token)) {
            if (op.isCommutative() && rhs.rhs.isConstantExpression) {
                // Mirror A: C2 op (A op C1) → A op (C2 op C1)
                if (isReassociationSafe(lhs, rhs.rhs)) {
                    ExpressionNode inner = rhs;
                    ExpressionNode c2 = lhs;
                    this.lhs = inner.lhs;
                    inner.lhs = c2;
                    inner.isConstantExpression = true;
                    inner.cacheConstantFold();
                }
            } else if (rhs.lhs.isConstantExpression) {
                // Mirror B: C2 op (C1 op A) → (C2 op C1) op A
                if (isReassociationSafe(lhs, rhs.lhs)) {
                    ExpressionNode inner = rhs;
                    ExpressionNode c2 = lhs;
                    ExpressionNode c1 = inner.lhs;
                    this.rhs = inner.rhs;
                    this.lhs = inner;
                    inner.lhs = c2;
                    inner.rhs = c1;
                    inner.isConstantExpression = true;
                    inner.cacheConstantFold();
                }
            }
        }

        return false;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        // note: it's safe to take any registry (new or old) because we don't use precedence here
        OperatorRegistry registry = OperatorExpression.getRegistry();
        char openBracket = '(';
        char closeBracket = ')';
        if (token != null && SqlKeywords.isArrayKeyword(token)) {
            openBracket = '[';
            closeBracket = ']';
        }

        switch (paramCount) {
            case 0:
                if (queryModel != null) {
                    sink.putAscii('(').put(queryModel).putAscii(')');
                } else {
                    sink.put(token);
                    if (type == FUNCTION) {
                        sink.putAscii("()");
                    }
                }
                break;
            case 1:
                sink.put(token);
                sink.putAscii(openBracket);
                toSink(sink, rhs);
                sink.putAscii(closeBracket);
                break;
            case 2:
                if (registry.isOperator(token)) {
                    // an operator child might have an higher precedence than the parent
                    // if it was wrapped in parentheses.
                    final boolean lhsParent = lhs.type == OPERATION && lhs.precedence > precedence;
                    if (lhsParent) {
                        sink.putAscii('(');
                    }
                    toSink(sink, lhs);
                    if (lhsParent) {
                        sink.putAscii(')');
                    }
                    sink.putAscii(' ');
                    sink.put(token);
                    sink.putAscii(' ');
                    final boolean rhsParent = rhs.type == OPERATION && rhs.precedence >= precedence;
                    if (rhsParent) {
                        sink.putAscii('(');
                    }
                    toSink(sink, rhs);
                    if (rhsParent) {
                        sink.putAscii(')');
                    }
                } else if (token.length() == 2 && token.charAt(0) == '[' && token.charAt(1) == ']') {
                    // for array dereference we want to display them as lhs[rhs] instead of [](lhs, rhs)
                    sink.put(lhs);
                    sink.put('[');
                    sink.put(rhs);
                    sink.put(']');
                } else if (SqlKeywords.isCaseKeyword(token)) {
                    // for case we want to display them as 'case when lhs then rhs end' instead of case(lhs, rhs)
                    sink.put("case when ");
                    sink.put(lhs);
                    sink.put(" then ");
                    sink.put(rhs);
                    sink.put(" end");
                } else if (SqlKeywords.isCastKeyword(token)) {
                    // for cast we want to display them as lhs::rhs instead of cast(lhs, rhs)
                    // in some cases the casted parameter may contains space which makes it hard to understand when the
                    // cast is applied, in such case we wrap lhs in parentheses.
                    final boolean parent = lhs.type == OPERATION || SqlKeywords.isCaseKeyword(lhs.token) || SqlKeywords.isBetweenKeyword(lhs.token);
                    if (parent) {
                        sink.put('(');
                        sink.put(lhs);
                        sink.put(')');
                    } else {
                        sink.put(lhs);
                    }
                    sink.put(':');
                    sink.put(':');
                    sink.put(rhs);
                } else {
                    sink.put(token);
                    sink.putAscii(openBracket);
                    toSink(sink, lhs);
                    sink.putAscii(',');
                    sink.putAscii(' ');
                    toSink(sink, rhs);
                    sink.putAscii(closeBracket);
                }
                break;
            default:
                int n = args.size();
                if (registry.isOperator(token) && n > 0) {
                    // special case for "in"
                    toSink(sink, args.getQuick(n - 1));
                    sink.putAscii(' ');
                    sink.put(token);
                    sink.putAscii(' ');
                    sink.putAscii('(');
                    for (int i = n - 2; i > -1; i--) {
                        if (i < n - 2) {
                            sink.putAscii(',');
                            sink.putAscii(' ');
                        }
                        toSink(sink, args.getQuick(i));
                    }
                    sink.putAscii(')');
                } else if (SqlKeywords.isCaseKeyword(token)) {
                    // For the case keyword we want to display it as 'case [when x then x-1] [else x] end'.
                    sink.put("case");
                    for (int i = n - 1; i > 0; i -= 2) {
                        sink.put(" when ");
                        sink.put(args.getQuick(i));
                        sink.put(" then ");
                        toSink(sink, args.getQuick(i - 1));
                    }
                    if (n % 2 == 1) {
                        sink.put(" else ");
                        toSink(sink, args.getQuick(0));
                    }
                    sink.put(" end");
                } else {
                    sink.put(token);
                    sink.putAscii(openBracket);
                    for (int i = n - 1; i > -1; i--) {
                        if (i < n - 1) {
                            sink.putAscii(',');
                            sink.putAscii(' ');
                        }
                        toSink(sink, args.getQuick(i));
                    }
                    sink.putAscii(closeBracket);
                }
                break;
        }
    }

    @Override
    public String toString() {
        return Objects.toString(token);
    }

    /**
     * Applies one LONG arithmetic operator at LONG width, wrapping mod 2^64 and
     * propagating the LONG_NULL sentinel exactly as the runtime AddLong / SubLong /
     * MulLong / DivLong / RemLong / Bitwise{And,Or,Xor}Long functions do. A zero
     * divisor folds to LONG_NULL, matching DivLong / RemLong getLong(). Throws
     * {@link NumericException} for an operator outside that set so the LONG-width
     * fold in {@link #cacheConstantFold} bails like a non-constant operand.
     * <p>
     * The JIT models the same operator table in {@code CompiledFilterIRSerializer#tryFoldConstantArith},
     * and the two look like they disagree on division by zero: this one folds to LONG_NULL,
     * that one throws. They agree observably - the throw means "decline to fold", so the JIT
     * emits the division as IR instead, and the native int64_div (see impl/x86.h) returns
     * LONG_NULL for a zero divisor. Keep both arms as they are: neither is a bug to fix by
     * copying the other.
     */
    private static long applyLongFold(CharSequence opToken, long a, long b) {
        if (a == Numbers.LONG_NULL || b == Numbers.LONG_NULL) {
            return Numbers.LONG_NULL;
        }
        if (opToken.length() != 1) {
            throw NumericException.INSTANCE;
        }
        switch (opToken.charAt(0)) {
            case '+':
                return a + b;
            case '-':
                return a - b;
            case '*':
                return a * b;
            case '/':
                return b == 0 ? Numbers.LONG_NULL : a / b;
            case '%':
                return b == 0 ? Numbers.LONG_NULL : a % b;
            case '&':
                return a & b;
            case '|':
                return a | b;
            case '^':
                return a ^ b;
            default:
                throw NumericException.INSTANCE;
        }
    }

    private static boolean compareArgs(
            ExpressionNode groupByExpr,
            ExpressionNode columnExpr,
            IQueryModel translatingModel
    ) {
        final int groupByArgsSize = groupByExpr.args.size();
        final int selectNodeArgsSize = columnExpr.args.size();

        if (groupByArgsSize != selectNodeArgsSize) {
            return false;
        }

        if (groupByArgsSize < 3) {
            return compareNodesGroupBy(groupByExpr.lhs, columnExpr.lhs, translatingModel)
                    && compareNodesGroupBy(groupByExpr.rhs, columnExpr.rhs, translatingModel);
        }

        for (int i = 0; i < groupByArgsSize; i++) {
            if (!compareNodesGroupBy(groupByExpr.args.get(i), columnExpr.args.get(i), translatingModel)) {
                return false;
            }
        }
        return true;
    }

    private static boolean compareArgsExact(ExpressionNode a, ExpressionNode b) {
        final int groupByArgsSize = a.args.size();
        final int selectNodeArgsSize = b.args.size();

        if (groupByArgsSize != selectNodeArgsSize) {
            return false;
        }

        if (groupByArgsSize < 3) {
            return compareNodesExact(a.lhs, b.lhs) && compareNodesExact(a.rhs, b.rhs);
        }

        for (int i = 0; i < groupByArgsSize; i++) {
            if (!compareNodesExact(a.args.get(i), b.args.get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Screens out a constant token that no numeric parse can accept. Every numeric literal starts
     * with a digit, a sign or a decimal point (see {@link Numbers#parseInt} / {@link Numbers#parseLong}
     * / {@link Numbers#parseDouble}), so a token that starts with anything else - a quoted string, a
     * geohash, a type keyword, {@code null} / {@code true} / {@code false} - folds to nothing and must
     * not pay for the parses. A failed parse is not free: {@link NumericException} formats a message
     * into a sink, and under {@code -ea} it also allocates a fresh exception and fills in its stack
     * trace. {@link #reassociateConstants} runs over every expression of every compiled query, so a
     * long IN list of string literals would otherwise throw thousands of them per compile.
     */
    private static boolean isNumericConstantToken(CharSequence token) {
        if (token == null || token.length() == 0) {
            return false;
        }
        final char first = token.charAt(0);
        return (first >= '0' && first <= '9') || first == '-' || first == '+' || first == '.';
    }

    /**
     * Reports whether regrouping the constant pair is safe without resolved operand types or row
     * value ranges. Numeric pairs are excluded: floating-point and DECIMAL operations may change
     * through rounding or scale, while integer intermediates may wrap onto NULL sentinels.
     */
    private static boolean isReassociationSafe(ExpressionNode a, ExpressionNode b) {
        if (a.isConstFoldWidening || b.isConstFoldWidening) {
            return false;
        }
        // Integer arithmetic is not associative in QuestDB because INT_NULL and LONG_NULL are
        // reserved sentinel values. The original intermediate can hit a sentinel for a row value
        // that is unavailable to this pre-type-resolution pass, even when the constant pair does
        // not fold to NULL. Only non-integer operators, such as boolean logic and concatenation,
        // remain eligible here.
        return !(a.isConstFoldLongValid && b.isConstFoldLongValid);
    }

    private static void toSink(CharSink<?> sink, ExpressionNode e) {
        if (e == null) {
            sink.putAscii("null");
        } else {
            e.toSink(sink);
        }
    }

    /**
     * Caches this constant node's fold results into the primitive {@code constFold*} /
     * {@code isConstFold*} fields, so {@link #isReassociationSafe} reads a constant
     * subtree's fold in O(1) rather than re-walking it. Runs the moment
     * {@link #isConstantExpression} is set, bottom-up: a CONSTANT leaf parses its token; a
     * binary-operation constant pair combines its children's already-cached folds. The
     * cached values mirror the runtime function semantics that the deleted recursive folds
     * modeled:
     * <ul>
     *   <li>{@code constFoldLongValue} / {@code isConstFoldLongValid} - the LONG-width fold
     *   (wrapping mod 2^64, as LongFunction getLong()) for an INT / LONG integer subtree;
     *   invalid for a floating-point / DECIMAL / non-numeric leaf or an unmodeled
     *   operator.</li>
     *   <li>{@code isConstFoldWidening} - set for any non-integer numeric-looking leaf that is not
     *   reassociation-safe: a floating-point or DECIMAL leaf (which widens an INT operation, since
     *   +, -, *, / promote to the wider type when either operand is wider) as well as a LONG256
     *   (0x...) hex leaf (whose '+' is non-associative under the NULL_LONG256 sentinel). A widening
     *   leaf anywhere in the subtree marks the whole subtree.</li>
     * </ul>
     */
    private void cacheConstantFold() {
        if (type == CONSTANT) {
            if (!isNumericConstantToken(token)) {
                isConstFoldLongValid = false;
                // A quoted literal is not numeric-looking, but overload resolution still casts it
                // to a number when the other operand is one, so l * '02' * 4 is integer arithmetic
                // whose regrouping changes the result exactly as l * 2 * 4 would. Marking it
                // widening keeps the guard closed, so the two spellings agree. It also keeps
                // d + '0.1' + 1 evaluating left to right, where '0.1' resolves against the DOUBLE
                // column, rather than regrouping to d + ('0.1' + 1) and failing to cast '0.1' to
                // INT. Unquoted non-numeric tokens - null, true, false, a geohash, a type keyword -
                // cannot become an arithmetic operand this way and stay reassociable.
                isConstFoldWidening = Chars.isQuoted(token);
                return;
            }
            try {
                // parseInt rejects an 'L' suffix, a decimal/exponent, and out-of-INT-range
                // literals, so only genuine INT constants land here; wider ones fall through to
                // the parseLong below. The two accept overlapping but INCOMPARABLE token sets -
                // parseInt takes a leading '+' that parseLong rejects, parseLong takes an 'L'
                // suffix and the out-of-INT-range literals that parseInt rejects - so a token is
                // an integer literal when EITHER accepts it, and both are asked. An INT literal
                // is trivially a LONG one, so this value is already the long-width fold.
                constFoldLongValue = Numbers.parseInt(token);
                isConstFoldLongValid = true;
                // An integer literal never widens; integer pairs are excluded from reassociation.
                isConstFoldWidening = false;
                return;
            } catch (NumericException notIntLiteral) {
                // not an INT literal; the long-width parse below may still take it
            }
            try {
                // parseLong rejects a decimal/exponent and a DECIMAL 'm' suffix, so only
                // genuine LONG constants fold at long width; wider types are invalid.
                constFoldLongValue = Numbers.parseLong(token);
                isConstFoldLongValid = true;
                isConstFoldWidening = false;
                return;
            } catch (NumericException notLongLiteral) {
                isConstFoldLongValid = false;
            }
            // Neither integer parse took the token, so what remains is a non-integer numeric-looking
            // constant: a floating-point or DECIMAL literal, or a LONG256 (0x...) hex literal. None is
            // reassociation-safe - float/decimal folds can change through rounding or scale, and
            // LONG256 '+' is non-associative under NULL_LONG256 sentinel propagation (a hex pair
            // summing mod 2^256 to the sentinel would fold to a NULL operand) - so mark the leaf
            // non-reassociable. isReassociationSafe gates purely on this flag and isConstFoldLongValid,
            // so treating every such leaf as widening keeps the guard closed without a second parse.
            isConstFoldWidening = true;
            return;
        }
        // Binary OPERATION constant pair: combine the children's caches at O(1). Both
        // children are already constant (their isConstantExpression is set), so their
        // caches are populated. A widening leaf anywhere makes the pair widening.
        isConstFoldWidening = lhs.isConstFoldWidening || rhs.isConstFoldWidening;
        if (token != null && lhs.isConstFoldLongValid && rhs.isConstFoldLongValid) {
            try {
                constFoldLongValue = applyLongFold(token, lhs.constFoldLongValue, rhs.constFoldLongValue);
                isConstFoldLongValid = true;
            } catch (NumericException unmodeledOperator) {
                isConstFoldLongValid = false;
            }
        } else {
            isConstFoldLongValid = false;
        }
    }

    public static final class ExpressionNodeFactory implements ObjectFactory<ExpressionNode> {
        @Override
        public ExpressionNode newInstance() {
            return new ExpressionNode();
        }
    }
}

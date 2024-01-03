/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.std.*;
import io.questdb.std.str.CharSinkBase;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public class ExpressionNode implements Mutable, Sinkable {

    public static final int ARRAY_ACCESS = 9;
    public static final int BIND_VARIABLE = 6;
    public static final int CONSTANT = 2;
    public static final int CONTROL = 16;
    public final static ExpressionNodeFactory FACTORY = new ExpressionNodeFactory();
    public static final int FUNCTION = 8;
    public static final int LITERAL = 4;
    public static final int MEMBER_ACCESS = 5;
    public static final int OPERATION = 1;
    public static final int QUERY = 65;
    public static final int SET_OPERATION = 32;
    public static final int UNKNOWN = 0;
    public final ObjList<ExpressionNode> args = new ObjList<>(4);
    public boolean innerPredicate = false;
    public int intrinsicValue = IntrinsicModel.UNDEFINED;
    public ExpressionNode lhs;
    public int paramCount;
    public int position;
    public int precedence;
    public QueryModel queryModel;
    public ExpressionNode rhs;
    public CharSequence token;
    public int type;

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
        return (a.type == FUNCTION || a.type == LITERAL ? Chars.equalsIgnoreCase(a.token, b.token) : Chars.equals(a.token, b.token)) &&
                compareArgsExact(a, b);
    }

    public static boolean compareNodesGroupBy(
            ExpressionNode groupByExpr,
            ExpressionNode columnExpr,
            QueryModel translatingModel
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

            int dot = Chars.indexOf(tok, '.');
            if (dot > -1
                    && translatingModel.getModelAliasIndex(tok, 0, dot) > -1
                    && Chars.equals(qcTok, tok, dot + 1, tok.length())) {
                return compareArgs(groupByExpr, columnExpr, translatingModel);
            }

            return false;
        }

        return compareArgs(groupByExpr, columnExpr, translatingModel);
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
        copy.innerPredicate = node.innerPredicate;
        return copy;
    }

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
        queryModel = null;
        innerPredicate = false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExpressionNode that = (ExpressionNode) o;
        return precedence == that.precedence
                && position == that.position
                && type == that.type
                && paramCount == that.paramCount
                && intrinsicValue == that.intrinsicValue
                && innerPredicate == that.innerPredicate
                && Objects.equals(args, that.args)
                && Objects.equals(token, that.token)
                && Objects.equals(queryModel, that.queryModel)
                && Objects.equals(lhs, that.lhs)
                && Objects.equals(rhs, that.rhs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(args, token, queryModel, precedence, position, lhs, rhs, type, paramCount, intrinsicValue, innerPredicate);
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
        if (type == LITERAL && token != null && token.length() != 0 && (token.charAt(0) == '$' || token.charAt(0) == ':')) {
            this.type = BIND_VARIABLE;
        } else {
            this.type = type;
        }
        this.precedence = precedence;
        this.token = token;
        this.position = position;
        return this;
    }

    @Override
    public void toSink(@NotNull CharSinkBase<?> sink) {
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
                sink.putAscii('(');
                toSink(sink, rhs);
                sink.putAscii(')');
                break;
            case 2:
                if (OperatorExpression.isOperator(token)) {
                    toSink(sink, lhs);
                    sink.putAscii(' ');
                    sink.put(token);
                    sink.putAscii(' ');
                    toSink(sink, rhs);
                } else {
                    sink.put(token);
                    sink.putAscii('(');
                    toSink(sink, lhs);
                    sink.putAscii(',');
                    toSink(sink, rhs);
                    sink.putAscii(')');
                }
                break;
            default:
                int n = args.size();
                if (OperatorExpression.isOperator(token) && n > 0) {
                    // special case for "in"
                    toSink(sink, args.getQuick(n - 1));
                    sink.putAscii(' ');
                    sink.put(token);
                    sink.putAscii(' ');
                    sink.putAscii('(');
                    for (int i = n - 2; i > -1; i--) {
                        if (i < n - 2) {
                            sink.putAscii(',');
                        }
                        toSink(sink, args.getQuick(i));
                    }
                    sink.putAscii(')');
                } else {
                    sink.put(token);
                    sink.putAscii('(');
                    for (int i = n - 1; i > -1; i--) {
                        if (i < n - 1) {
                            sink.putAscii(',');
                        }
                        toSink(sink, args.getQuick(i));
                    }
                    sink.putAscii(')');
                }
                break;
        }
    }

    private static boolean compareArgs(
            ExpressionNode groupByExpr,
            ExpressionNode columnExpr,
            QueryModel translatingModel
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

    private static void toSink(CharSinkBase<?> sink, ExpressionNode e) {
        if (e == null) {
            sink.putAscii("null");
        } else {
            e.toSink(sink);
        }
    }

    public static final class ExpressionNodeFactory implements ObjectFactory<ExpressionNode> {
        @Override
        public ExpressionNode newInstance() {
            return new ExpressionNode();
        }
    }
}

/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.model;

import com.questdb.griffin.OperatorExpression;
import com.questdb.std.Mutable;
import com.questdb.std.ObjList;
import com.questdb.std.ObjectFactory;
import com.questdb.std.Sinkable;
import com.questdb.std.str.CharSink;

public class ExpressionNode implements Mutable, Sinkable {

    public final static ExpressionNodeFactory FACTORY = new ExpressionNodeFactory();
    public static final int OPERATION = 1;
    public static final int CONSTANT = 2;
    public static final int LITERAL = 4;
    public static final int FUNCTION = 8;
    public static final int CONTROL = 16;
    public static final int SET_OPERATION = 32;
    public static final int QUERY = 65;
    public static final int UNKNOWN = 0;
    public final ObjList<ExpressionNode> args = new ObjList<>(4);
    public CharSequence token;
    public QueryModel queryModel;
    public int precedence;
    public int position;
    public ExpressionNode lhs;
    public ExpressionNode rhs;
    public int type;
    public int paramCount;
    public int intrinsicValue = IntrinsicModel.UNDEFINED;

    private ExpressionNode() {
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
    }

    public ExpressionNode of(int type, CharSequence token, int precedence, int position) {
        this.type = type;
        this.precedence = precedence;
        this.token = token;
        this.position = position;
        return this;
    }

    @Override
    public void toSink(CharSink sink) {
        switch (paramCount) {
            case 0:
                if (queryModel != null) {
                    sink.put('(').put(queryModel).put(')');
                } else {
                    sink.put(token);
                    if (type == FUNCTION) {
                        sink.put("()");
                    }
                }
                break;
            case 1:
                sink.put(token);
                sink.put('(');
                rhs.toSink(sink);
                sink.put(')');
                break;
            case 2:
                if (OperatorExpression.isOperator(token)) {
                    lhs.toSink(sink);
                    sink.put(' ');
                    sink.put(token);
                    sink.put(' ');
                    rhs.toSink(sink);
                } else {
                    sink.put(token);
                    sink.put('(');
                    lhs.toSink(sink);
                    sink.put(',');
                    rhs.toSink(sink);
                    sink.put(')');
                }
                break;
            default:
                int n = args.size();
                if (OperatorExpression.isOperator(token) && n > 0) {
                    // special case for "in"
                    args.getQuick(n - 1).toSink(sink);
                    sink.put(' ');
                    sink.put(token);
                    sink.put(' ');
                    sink.put('(');
                    for (int i = n - 2; i > -1; i--) {
                        if (i < n - 2) {
                            sink.put(',');
                        }
                        args.getQuick(i).toSink(sink);
                    }
                    sink.put(')');
                } else {
                    sink.put(token);
                    sink.put('(');
                    for (int i = n - 1; i > -1; i--) {
                        if (i < n - 1) {
                            sink.put(',');
                        }
                        args.getQuick(i).toSink(sink);
                    }
                    sink.put(')');
                }
                break;
        }
    }

    private static final class ExpressionNodeFactory implements ObjectFactory<ExpressionNode> {
        @Override
        public ExpressionNode newInstance() {
            return new ExpressionNode();
        }
    }
}

/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;
import io.questdb.std.Sinkable;
import io.questdb.std.str.CharSink;

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
        clear();
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

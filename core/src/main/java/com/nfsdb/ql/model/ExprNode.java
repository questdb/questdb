/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.ql.model;

import com.nfsdb.io.sink.CharSink;
import com.nfsdb.std.Mutable;
import com.nfsdb.std.ObjList;
import com.nfsdb.std.ObjectFactory;

public class ExprNode implements Mutable {

    public final static ExprNodeFactory FACTORY = new ExprNodeFactory();
    public final ObjList<ExprNode> args = new ObjList<>(4);
    public String token;
    public int precedence;
    public int position;
    public ExprNode lhs;
    public ExprNode rhs;
    public NodeType type;
    public int paramCount;
    public IntrinsicValue intrinsicValue = IntrinsicValue.UNDEFINED;

    private ExprNode() {
    }

    public void clear() {
        args.clear();
        token = null;
        precedence = 0;
        position = 0;
        lhs = null;
        rhs = null;
        type = null;
        paramCount = 0;
        intrinsicValue = IntrinsicValue.UNDEFINED;
    }

    public ExprNode of(NodeType type, String token, int precedence, int position) {
        this.type = type;
        this.precedence = precedence;
        this.token = token;
        this.position = position;
        return this;
    }

    @Override
    public String toString() {
        return "ExprNode{" +
                "token='" + token + '\'' +
                ", precedence=" + precedence +
                ", lhs=" + lhs +
                ", rhs=" + rhs +
                ", type=" + type +
                ", paramCount=" + paramCount +
                ", args=" + args +
                ", position=" + position +
                '}';
    }

    public void toString(CharSink sink) {
        switch (paramCount) {
            case 0:
                sink.put(token);
                break;
            case 1:
            case 2:
                if (lhs != null) {
                    lhs.toString(sink);
                }
                sink.put(' ');
                sink.put(token);
                sink.put(' ');
                if (rhs != null) {
                    rhs.toString(sink);
                }
                break;
            default:
                sink.put(token);
                sink.put('(');
                for (int i = 0, n = args.size(); i < n; i++) {
                    if (i > 0) {
                        sink.put(',');
                    }
                    args.getQuick(i).toString(sink);
                }
                sink.put(')');
                break;
        }
    }

    public enum NodeType {
        OPERATION, CONSTANT, LITERAL, FUNCTION, CONTROL, SET_OPERATION, LAMBDA
    }

    private static final class ExprNodeFactory implements ObjectFactory<ExprNode> {
        @Override
        public ExprNode newInstance() {
            return new ExprNode();
        }
    }
}

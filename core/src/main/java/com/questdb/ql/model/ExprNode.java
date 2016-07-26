/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.ql.model;

import com.questdb.std.*;

public class ExprNode implements Mutable, Sinkable {

    public final static ExprNodeFactory FACTORY = new ExprNodeFactory();
    public final ObjList<ExprNode> args = new ObjList<>(4);
    public String token;
    public int precedence;
    public int position;
    public ExprNode lhs;
    public ExprNode rhs;
    public NodeType type;
    public int paramCount;
    public int intrinsicValue = IntrinsicValue.UNDEFINED;

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
    public void toSink(CharSink sink) {
        switch (paramCount) {
            case 0:
                sink.put(token);
                break;
            case 1:
            case 2:
                if (lhs != null) {
                    lhs.toSink(sink);
                }
                sink.put(' ');
                sink.put(token);
                sink.put(' ');
                if (rhs != null) {
                    rhs.toSink(sink);
                }
                break;
            default:
                sink.put(token);
                sink.put('(');
                for (int i = 0, n = args.size(); i < n; i++) {
                    if (i > 0) {
                        sink.put(',');
                    }
                    args.getQuick(i).toSink(sink);
                }
                sink.put(')');
                break;
        }
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

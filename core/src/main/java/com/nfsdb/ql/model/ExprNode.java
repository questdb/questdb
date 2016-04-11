/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
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

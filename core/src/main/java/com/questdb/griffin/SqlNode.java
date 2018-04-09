/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.griffin;

import com.questdb.griffin.model.IntrinsicModel;
import com.questdb.std.Mutable;
import com.questdb.std.ObjList;
import com.questdb.std.ObjectFactory;
import com.questdb.std.Sinkable;
import com.questdb.std.str.CharSink;

public class SqlNode implements Mutable, Sinkable {

    public final static ExprNodeFactory FACTORY = new ExprNodeFactory();
    public static final int OPERATION = 1;
    public static final int CONSTANT = 2;
    public static final int LITERAL = 4;
    public static final int FUNCTION = 8;
    public static final int CONTROL = 16;
    public static final int SET_OPERATION = 32;
    public static final int LAMBDA = 65;
    public static final int UNKNOWN = 0;
    public final ObjList<SqlNode> args = new ObjList<>(4);
    public CharSequence token;
    public int precedence;
    public int position;
    public SqlNode lhs;
    public SqlNode rhs;
    public int type;
    public int paramCount;
    public int intrinsicValue = IntrinsicModel.UNDEFINED;

    private SqlNode() {
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
    }

    public SqlNode of(int type, CharSequence token, int precedence, int position) {
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
                if (type == FUNCTION) {
                    sink.put("()");
                }
                break;
            case 1:
                sink.put(token);
                sink.put('(');
                rhs.toSink(sink);
                sink.put(')');
                break;
            case 2:
                lhs.toSink(sink);
                sink.put(' ');
                sink.put(token);
                sink.put(' ');
                rhs.toSink(sink);
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

    private static final class ExprNodeFactory implements ObjectFactory<SqlNode> {
        @Override
        public SqlNode newInstance() {
            return new SqlNode();
        }
    }
}

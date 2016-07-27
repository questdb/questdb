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

package com.questdb.ql.parser;

import com.questdb.ex.NoSuchColumnException;
import com.questdb.ex.NumericException;
import com.questdb.ex.ParserException;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Chars;
import com.questdb.misc.Numbers;
import com.questdb.ql.model.ExprNode;
import com.questdb.ql.model.QueryModel;
import com.questdb.ql.ops.*;
import com.questdb.ql.ops.col.*;
import com.questdb.ql.ops.constant.*;
import com.questdb.std.CharSequenceIntHashMap;
import com.questdb.std.CharSequenceObjHashMap;
import com.questdb.std.ObjList;
import com.questdb.std.ObjectFactory;
import com.questdb.store.ColumnType;

import java.util.ArrayDeque;

class VirtualColumnBuilder implements PostOrderTreeTraversalAlgo.Visitor {
    private final static NullConstant nullConstant = new NullConstant();
    private final ObjList<VirtualColumn> mutableArgs = new ObjList<>();
    private final Signature mutableSig = new Signature();
    private final ArrayDeque<VirtualColumn> stack = new ArrayDeque<>();
    private final PostOrderTreeTraversalAlgo algo;
    private RecordMetadata metadata;
    private CharSequenceIntHashMap columnNameHistogram;
    private CharSequenceObjHashMap<Parameter> parameterMap;

    VirtualColumnBuilder(PostOrderTreeTraversalAlgo algo) {
        this.algo = algo;
    }

    @Override
    public void visit(ExprNode node) throws ParserException {
        int argCount = node.paramCount;
        if (argCount == 0) {
            switch (node.type) {
                case ExprNode.LITERAL:
                    if (Chars.startsWith(node.token, ':')) {
                        stack.push(Parameter.getOrCreate(node, parameterMap));
                    } else {
                        // lookup column
                        stack.push(lookupColumn(node));
                    }
                    break;
                case ExprNode.CONSTANT:
                    stack.push(parseConstant(node));
                    break;
                default:
                    // lookup zero arg function from symbol table
                    mutableSig.clear();
                    stack.push(lookupFunction(node, mutableSig.setName(node.token).setParamCount(0), null));
                    break;
            }
        } else {
            mutableSig.clear();
            mutableArgs.clear();
            mutableArgs.ensureCapacity(argCount);
            mutableSig.setName(node.token).setParamCount(argCount);
            for (int n = 0; n < argCount; n++) {
                VirtualColumn c = stack.poll();
                if (c == null) {
                    throw QueryError.$(node.position, "Too few arguments");
                }
                mutableSig.paramType(n, c.getType(), c.isConstant());
                mutableArgs.setQuick(n, c);
            }
            stack.push(lookupFunction(node, mutableSig, mutableArgs));
        }
    }

    VirtualColumn createVirtualColumn(QueryModel model, ExprNode node, RecordMetadata metadata) throws ParserException {
        this.columnNameHistogram = model.getColumnNameHistogram();
        this.parameterMap = model.getParameterMap();
        this.metadata = metadata;
        algo.traverse(node, this);
        return stack.poll();
    }

    private VirtualColumn lookupColumn(ExprNode node) throws ParserException {
        try {
            if (columnNameHistogram.get(node.token) > 0) {
                throw QueryError.ambiguousColumn(node.position);
            }
            int index = metadata.getColumnIndex(node.token);
            switch (metadata.getColumnQuick(index).getType()) {
                case ColumnType.DOUBLE:
                    return new DoubleRecordSourceColumn(index);
                case ColumnType.INT:
                    return new IntRecordSourceColumn(index);
                case ColumnType.LONG:
                    return new LongRecordSourceColumn(index);
                case ColumnType.STRING:
                    return new StrRecordSourceColumn(index);
                case ColumnType.SYMBOL:
                    return new SymRecordSourceColumn(index);
                case ColumnType.BYTE:
                    return new ByteRecordSourceColumn(index);
                case ColumnType.FLOAT:
                    return new FloatRecordSourceColumn(index);
                case ColumnType.BOOLEAN:
                    return new BoolRecordSourceColumn(index);
                case ColumnType.SHORT:
                    return new ShortRecordSourceColumn(index);
                case ColumnType.BINARY:
                    return new BinaryRecordSourceColumn(index);
                case ColumnType.DATE:
                    return new DateRecordSourceColumn(index);
                default:
                    throw QueryError.$(node.position, "Not yet supported type");
            }
        } catch (NoSuchColumnException e) {
            throw QueryError.invalidColumn(node.position, node.token);
        }
    }

    private VirtualColumn lookupFunction(ExprNode node, Signature sig, ObjList<VirtualColumn> args) throws ParserException {
        if (node.type == ExprNode.LAMBDA) {
            throw QueryError.$(node.position, "Cannot use lambda in this context");
        }
        ObjectFactory<Function> factory = FunctionFactories.find(sig, args);
        if (factory == null) {
            throw QueryError.$(node.position, "No such function: " + sig.userReadable());
        }

        Function f = factory.newInstance();
        if (args != null) {
            int n = node.paramCount;
            for (int i = 0; i < n; i++) {
                f.setArg(i, args.getQuick(i));
            }
        }
        return f.isConstant() ? processConstantExpression(f) : f;
    }

    private VirtualColumn parseConstant(ExprNode node) throws ParserException {

        if ("null".equals(node.token)) {
            return nullConstant;
        }

        String s = Chars.stripQuotes(node.token);

        // by ref comparison
        //noinspection StringEquality
        if (s != node.token) {
            return new StrConstant(s);
        }

        try {
            return new IntConstant(Numbers.parseInt(node.token));
        } catch (NumericException ignore) {

        }

        try {
            return new LongConstant(Numbers.parseLong(node.token));
        } catch (NumericException ignore) {

        }

        try {
            return new DoubleConstant(Numbers.parseDouble(node.token));
        } catch (NumericException ignore) {

        }

        throw QueryError.$(node.position, "Unknown value type: " + node.token);
    }

    private VirtualColumn processConstantExpression(Function f) {
        switch (f.getType()) {
            case ColumnType.INT:
                return new IntConstant(f.getInt(null));
            case ColumnType.DOUBLE:
                return new DoubleConstant(f.getDouble(null));
            case ColumnType.BOOLEAN:
                return new BooleanConstant(f.getBool(null));
            default:
                return f;
        }
    }
}

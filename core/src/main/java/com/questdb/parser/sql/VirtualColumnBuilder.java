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

package com.questdb.parser.sql;

import com.questdb.BootstrapEnv;
import com.questdb.common.ColumnType;
import com.questdb.common.NoSuchColumnException;
import com.questdb.common.RecordMetadata;
import com.questdb.ex.ParserException;
import com.questdb.parser.sql.model.ExprNode;
import com.questdb.parser.sql.model.QueryModel;
import com.questdb.ql.ops.*;
import com.questdb.ql.ops.col.*;
import com.questdb.ql.ops.constant.*;
import com.questdb.std.*;

import java.util.ArrayDeque;

class VirtualColumnBuilder implements PostOrderTreeTraversalAlgo.Visitor {
    private final ObjList<VirtualColumn> mutableArgs = new ObjList<>();
    private final Signature mutableSig = new Signature();
    private final ArrayDeque<VirtualColumn> stack = new ArrayDeque<>();
    private final PostOrderTreeTraversalAlgo algo;
    private final BootstrapEnv env;
    private RecordMetadata metadata;
    private CharSequenceIntHashMap columnNameHistogram;
    private CharSequenceObjHashMap<Parameter> parameterMap;
    private QueryModel model;

    VirtualColumnBuilder(PostOrderTreeTraversalAlgo algo, BootstrapEnv env) {
        this.algo = algo;
        this.env = env;
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
            mutableArgs.setPos(argCount);
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
        this.model = model;
        this.metadata = metadata;
        algo.traverse(node, this);
        return stack.poll();
    }

    private VirtualColumn lookupColumn(ExprNode node) throws ParserException {
        CharSequence column = model.translateAlias(node.token);
        try {
            if (columnNameHistogram.get(column) > 0) {
                throw QueryError.ambiguousColumn(node.position);
            }
            int index = metadata.getColumnIndex(column);
            switch (metadata.getColumnQuick(index).getType()) {
                case ColumnType.DOUBLE:
                    return new DoubleRecordSourceColumn(index, node.position);
                case ColumnType.INT:
                    return new IntRecordSourceColumn(index, node.position);
                case ColumnType.LONG:
                    return new LongRecordSourceColumn(index, node.position);
                case ColumnType.STRING:
                    return new StrRecordSourceColumn(index, node.position);
                case ColumnType.SYMBOL:
                    return new SymRecordSourceColumn(index, node.position);
                case ColumnType.BYTE:
                    return new ByteRecordSourceColumn(index, node.position);
                case ColumnType.FLOAT:
                    return new FloatRecordSourceColumn(index, node.position);
                case ColumnType.BOOLEAN:
                    return new BoolRecordSourceColumn(index, node.position);
                case ColumnType.SHORT:
                    return new ShortRecordSourceColumn(index, node.position);
                case ColumnType.BINARY:
                    return new BinaryRecordSourceColumn(index, node.position);
                case ColumnType.DATE:
                    return new DateRecordSourceColumn(index, node.position);
                default:
                    throw QueryError.$(node.position, "Not yet supported type");
            }
        } catch (NoSuchColumnException e) {
            throw QueryError.invalidColumn(node.position, column);
        }
    }

    private VirtualColumn lookupFunction(ExprNode node, Signature sig, ObjList<VirtualColumn> args) throws ParserException {
        if (node.type == ExprNode.LAMBDA) {
            throw QueryError.$(node.position, "Cannot use lambda in this context");
        }
        VirtualColumnFactory<Function> factory = FunctionFactories.find(sig, args);
        if (factory == null) {
            throw QueryError.$(node.position, "No such function: " + sig.userReadable());
        }

        Function f = factory.newInstance(node.position, env);
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
            return new NullConstant(node.position);
        }

        String s = Chars.stripQuotes(node.token);

        // by ref comparison
        //noinspection StringEquality
        if (s != node.token) {
            return new StrConstant(s, node.position);
        }

        try {
            return new IntConstant(Numbers.parseInt(node.token), node.position);
        } catch (NumericException ignore) {
        }

        try {
            return new LongConstant(Numbers.parseLong(node.token), node.position);
        } catch (NumericException ignore) {
        }

        try {
            return new DoubleConstant(Numbers.parseDouble(node.token), node.position);
        } catch (NumericException ignore) {
        }

        try {
            return new FloatConstant(Numbers.parseFloat(node.token), node.position);
        } catch (NumericException ignore) {
        }

        throw QueryError.$(node.position, "Unknown value type: " + node.token);
    }

    private VirtualColumn processConstantExpression(Function f) {
        switch (f.getType()) {
            case ColumnType.INT:
                return new IntConstant(f.getInt(null), f.getPosition());
            case ColumnType.DOUBLE:
                return new DoubleConstant(f.getDouble(null), f.getPosition());
            case ColumnType.FLOAT:
                return new FloatConstant(f.getFloat(null), f.getPosition());
            case ColumnType.BOOLEAN:
                return new BooleanConstant(f.getBool(null), f.getPosition());
            case ColumnType.STRING:
                CharSequence cs = f.getFlyweightStr(null);
                return cs == null ? new NullConstant(f.getPosition()) : new StrConstant(cs.toString(), f.getPosition());
            case ColumnType.LONG:
                return new LongConstant(f.getLong(null), f.getPosition());
            case ColumnType.DATE:
                return new DateConstant(f.getDate(null), f.getPosition());
            default:
                return f;
        }
    }
}

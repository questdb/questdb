/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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
                case LITERAL:
                    if (Chars.startsWith(node.token, ':')) {
                        stack.push(Parameter.getOrCreate(node, parameterMap));
                    } else {
                        // lookup column
                        stack.push(lookupColumn(node));
                    }
                    break;
                case CONSTANT:
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

    @SuppressFBWarnings({"LEST_LOST_EXCEPTION_STACK_TRACE"})
    private VirtualColumn lookupColumn(ExprNode node) throws ParserException {
        try {
            if (columnNameHistogram.get(node.token) > 0) {
                throw QueryError.$(node.position, "Ambiguous column name");
            }
            int index = metadata.getColumnIndex(node.token);
            switch (metadata.getColumnQuick(index).getType()) {
                case DOUBLE:
                    return new DoubleRecordSourceColumn(index);
                case INT:
                    return new IntRecordSourceColumn(index);
                case LONG:
                    return new LongRecordSourceColumn(index);
                case STRING:
                    return new StrRecordSourceColumn(index);
                case SYMBOL:
                    return new SymRecordSourceColumn(index);
                case BYTE:
                    return new ByteRecordSourceColumn(index);
                case FLOAT:
                    return new FloatRecordSourceColumn(index);
                case BOOLEAN:
                    return new BoolRecordSourceColumn(index);
                case SHORT:
                    return new ShortRecordSourceColumn(index);
                case BINARY:
                    return new BinaryRecordSourceColumn(index);
                case DATE:
                    return new DateRecordSourceColumn(index);
                default:
                    throw QueryError.$(node.position, "Not yet supported type");
            }
        } catch (NoSuchColumnException e) {
            throw QueryError.invalidColumn(node.position, node.token);
        }
    }

    private VirtualColumn lookupFunction(ExprNode node, Signature sig, ObjList<VirtualColumn> args) throws ParserException {
        if (node.type == ExprNode.NodeType.LAMBDA) {
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

    @SuppressFBWarnings({"ES_COMPARING_STRINGS_WITH_EQ"})
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
            case INT:
                return new IntConstant(f.getInt(null));
            case DOUBLE:
                return new DoubleConstant(f.getDouble(null));
            case BOOLEAN:
                return new BooleanConstant(f.getBool(null));
            default:
                return f;
        }
    }
}

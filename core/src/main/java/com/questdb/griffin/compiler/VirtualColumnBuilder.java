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

package com.questdb.griffin.compiler;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.common.ColumnType;
import com.questdb.common.NoSuchColumnException;
import com.questdb.common.NumericException;
import com.questdb.common.RecordMetadata;
import com.questdb.griffin.common.ExprNode;
import com.questdb.griffin.common.PostOrderTreeTraversalAlgo;
import com.questdb.griffin.engine.Function;
import com.questdb.griffin.engine.FunctionFactory;
import com.questdb.griffin.engine.FunctionRepository;
import com.questdb.griffin.engine.Signature;
import com.questdb.griffin.engine.functions.*;
import com.questdb.griffin.lexer.ParserException;
import com.questdb.std.CharSequenceObjHashMap;
import com.questdb.std.Chars;
import com.questdb.std.Numbers;
import com.questdb.std.ObjList;

import java.util.ArrayDeque;

public class VirtualColumnBuilder implements PostOrderTreeTraversalAlgo.Visitor {
    private final ObjList<Function> mutableArgs = new ObjList<>();
    private final Signature mutableSig = new Signature();
    private final ArrayDeque<Function> stack = new ArrayDeque<>();
    private final PostOrderTreeTraversalAlgo algo;
    private final CairoConfiguration configuration;
    private RecordMetadata metadata;
    private CharSequenceObjHashMap<Parameter> parameterMap;

    VirtualColumnBuilder(PostOrderTreeTraversalAlgo algo, CairoConfiguration configuration) {
        this.algo = algo;
        this.configuration = configuration;
    }

    public static Function getOrCreate(ExprNode node, CharSequenceObjHashMap<Parameter> parameterMap) {
        Parameter p = parameterMap.get(node.token);
        if (p == null) {
            parameterMap.put(node.token, p = new Parameter(node.position));
            p.setName(node.token.toString());
        }
        return p;
    }

    public Function buildFrom(ExprNode node, RecordMetadata metadata, CharSequenceObjHashMap<Parameter> parameterMap) throws ParserException {
        this.parameterMap = parameterMap;
        this.metadata = metadata;
        algo.traverse(node, this);
        return stack.poll();
    }

    @Override
    public void visit(ExprNode node) throws ParserException {
        int argCount = node.paramCount;
        if (argCount == 0) {
            switch (node.type) {
                case ExprNode.LITERAL:
                    if (Chars.startsWith(node.token, ':')) {
                        stack.push(getOrCreate(node, parameterMap));
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
                Function c = stack.poll();
                if (c == null) {
                    throw ParserException.$(node.position, "Too few arguments");
                }
                mutableSig.paramType(n, c.getType(), c.isConstant());
                mutableArgs.setQuick(n, c);
            }
            stack.push(lookupFunction(node, mutableSig, mutableArgs));
        }
    }

    private Function lookupColumn(ExprNode node) throws ParserException {
        try {
            final int index = metadata.getColumnIndex(node.token);
            return new RecordColumn(metadata.getColumnQuick(index).getType(), index, node.position);
        } catch (NoSuchColumnException e) {
            throw ParserException.invalidColumn(node.position, node.token);
        }
    }

    private Function lookupFunction(ExprNode node, Signature sig, ObjList<Function> args) throws ParserException {
        if (node.type == ExprNode.LAMBDA) {
            throw ParserException.$(node.position, "Cannot use lambda in this context");
        }
        FunctionFactory<Function> factory = FunctionRepository.find(sig, args);
        if (factory == null) {
            throw ParserException.$(node.position, "No such function: " + sig.userReadable());
        }

        Function f = factory.newInstance(args, node.position, configuration);
        return f.isConstant() ? processConstantExpression(f) : f;
    }

    private Function parseConstant(ExprNode node) throws ParserException {

        if (Chars.equals("null", node.token)) {
            return new NullConstant(node.position);
        }

        if (Chars.isQuoted(node.token)) {
            return new StrConstant(node.token, node.position);
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

        throw ParserException.$(node.position, "Unknown value type: " + node.token);
    }

    private Function processConstantExpression(Function f) {
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
                CharSequence cs = f.getStr(null);
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

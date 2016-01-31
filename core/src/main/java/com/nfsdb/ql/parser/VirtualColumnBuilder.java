/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

package com.nfsdb.ql.parser;

import com.nfsdb.ex.InvalidColumnException;
import com.nfsdb.ex.NoSuchColumnException;
import com.nfsdb.ex.NumericException;
import com.nfsdb.ex.ParserException;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.misc.Chars;
import com.nfsdb.misc.Numbers;
import com.nfsdb.ql.model.ExprNode;
import com.nfsdb.ql.ops.*;
import com.nfsdb.ql.ops.col.*;
import com.nfsdb.ql.ops.constant.*;
import com.nfsdb.std.CharSequenceIntHashMap;
import com.nfsdb.std.ObjList;
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

    public VirtualColumnBuilder(PostOrderTreeTraversalAlgo algo) {
        this.algo = algo;
    }

    public VirtualColumn createVirtualColumn(
            ExprNode node,
            RecordMetadata metadata,
            CharSequenceIntHashMap columnNameHistogram) throws ParserException {
        this.columnNameHistogram = columnNameHistogram;
        this.metadata = metadata;
        algo.traverse(node, this);
        return stack.poll();
    }

    @Override
    public void visit(ExprNode node) throws ParserException {
        int argCount = node.paramCount;
        if (argCount == 0) {
            switch (node.type) {
                case LITERAL:
                    // lookup column
                    stack.push(lookupColumn(node));
                    break;
                case CONSTANT:
                    stack.push(parseConstant(node));
                    break;
                default:
                    // lookup zero arg function from symbol table
                    mutableSig.clear();
                    stack.push(lookupFunction(node, mutableSig.setName(node.token).setParamCount(0), null));
            }
        } else {
            mutableSig.clear();
            mutableArgs.clear();
            mutableArgs.ensureCapacity(argCount);
            mutableSig.setName(node.token).setParamCount(argCount);
            for (int n = 0; n < argCount; n++) {
                VirtualColumn c = stack.poll();
                if (c == null) {
                    throw new ParserException(node.position, "Too few arguments");
                }
                mutableSig.paramType(n, c.getType(), c.isConstant());
                mutableArgs.setQuick(n, c);
            }
            stack.push(lookupFunction(node, mutableSig, mutableArgs));
        }
    }

    @SuppressFBWarnings({"LEST_LOST_EXCEPTION_STACK_TRACE"})
    private VirtualColumn lookupColumn(ExprNode node) throws ParserException {
        try {
            if (columnNameHistogram.get(node.token) > 0) {
                throw new ParserException(node.position, "Ambiguous column name");
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
                    throw new ParserException(node.position, "Not yet supported type");
            }
        } catch (NoSuchColumnException e) {
            throw new InvalidColumnException(node.position);
        }
    }

    private VirtualColumn lookupFunction(ExprNode node, Signature sig, ObjList<VirtualColumn> args) throws ParserException {
        if (node.type == ExprNode.NodeType.LAMBDA) {
            throw new ParserException(node.position, "Cannot use lambda in this context");
        }
        FunctionFactory factory = FunctionFactories.find(sig, args);
        if (factory == null) {
            throw new ParserException(node.position, "No such function: " + sig.userReadable());
        }

        Function f = factory.newInstance(args);
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

        throw new ParserException(node.position, "Unknown value type: " + node.token);
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

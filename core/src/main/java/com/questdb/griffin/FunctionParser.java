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

import com.questdb.cairo.CairoConfiguration;
import com.questdb.common.ColumnType;
import com.questdb.common.NoSuchColumnException;
import com.questdb.common.RecordMetadata;
import com.questdb.griffin.common.ExprNode;
import com.questdb.griffin.common.PostOrderTreeTraversalAlgo;
import com.questdb.griffin.engine.functions.Parameter;
import com.questdb.griffin.engine.functions.columns.*;
import com.questdb.griffin.engine.functions.constants.*;
import com.questdb.griffin.lexer.ParserException;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.*;

import java.util.ArrayDeque;

public class FunctionParser implements PostOrderTreeTraversalAlgo.Visitor {
    private static final Log LOG = LogFactory.getLog(FunctionParser.class);
    private static IntHashSet invalidFunctionNameChars = new IntHashSet();
    private final ObjList<Function> mutableArgs = new ObjList<>();
    private final ArrayDeque<Function> stack = new ArrayDeque<>();
    private final PostOrderTreeTraversalAlgo algo = new PostOrderTreeTraversalAlgo();
    private final CairoConfiguration configuration;
    private final CharSequenceObjHashMap<ObjList<FunctionFactory>> factories = new CharSequenceObjHashMap<>();
    private RecordMetadata metadata;
    private CharSequenceObjHashMap<Parameter> parameterMap;

    FunctionParser(CairoConfiguration configuration, Iterable<FunctionFactory> functionFactories) {
        this.configuration = configuration;
        loadFunctionFactories(functionFactories);
    }

    public static Function getOrCreate(ExprNode node, CharSequenceObjHashMap<Parameter> parameterMap) {
        Parameter p = parameterMap.get(node.token);
        if (p == null) {
            parameterMap.put(node.token, p = new Parameter());
            p.setName(node.token.toString());
        }
        return p;
    }

    public Function parseFunction(ExprNode node, RecordMetadata metadata, CharSequenceObjHashMap<Parameter> parameterMap) throws ParserException {
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
                    stack.push(getFunction(node, null));
                    break;
            }
        } else {
            mutableArgs.clear();
            mutableArgs.setPos(argCount);
            for (int n = 0; n < argCount; n++) {
                Function c = stack.poll();
                if (c == null) {
                    throw ParserException.$(node.position, "Too few arguments");
                }
                mutableArgs.setQuick(n, c);
            }
            stack.push(getFunction(node, mutableArgs));
        }
    }

    private static ParserException invalidFunction(CharSequence message, ExprNode node, ObjList<Function> args) {
        ParserException ex = ParserException.position(node.position);
        ex.put(message);
        ex.put(": ");
        ex.put(node.token);
        ex.put('(');
        for (int i = 0, n = args.size(); i < n; i++) {
            if (i > 0) {
                ex.put(',');
            }
            ex.put(ColumnType.nameOf(args.getQuick(i).getType()));
        }
        ex.put(')');
        return ex;
    }

    private boolean functionNameIsValid(String sig, int end) {
        int c = sig.charAt(0);
        if (c >= '0' && c <= '9') {
            return false;
        }

        for (int i = 0; i < end; i++) {
            if (invalidFunctionNameChars.contains(sig.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private Function getFunction(ExprNode node, ObjList<Function> args) throws ParserException {
        if (node.type == ExprNode.LAMBDA) {
            throw ParserException.$(node.position, "Cannot use lambda in this context");
        }
        final Function f = getFunction0(node, args);
        return f.isConstant() ? toConstant(f) : f;
    }

    private Function getFunction0(ExprNode node, ObjList<Function> args) throws ParserException {
        ObjList<FunctionFactory> overload = factories.get(node.token);
        if (overload == null) {
            throw invalidFunction("unknown function name", node, args);
        }

        final int argCount = args.size();
        FunctionFactory candidate = null;
        int matchCount = 0;
        for (int i = 0, n = overload.size(); i < n; i++) {
            final FunctionFactory factory = overload.getQuick(i);
            final String signature = factory.getSignature();
            final int sigArgOffset = signature.indexOf('(') + 1;
            final int sigArgCount = (signature.length() - 1 - sigArgOffset) / 2;

            if (argCount == 0 && sigArgCount == 0) {
                // this is no-arg function, match right away
                return factory.newInstance(args, node.position, configuration);
            }

            // otherwise, is number of arguments the same?
            if (sigArgCount == argCount) {
                int match = 2; // match

                for (int k = 0; k < argCount; k++) {
                    final Function arg = args.getQuick(k);
                    final boolean sigArgConst = signature.charAt(sigArgOffset + k * 2) == '!';
                    final int sigArgType;

                    switch (signature.charAt(sigArgOffset + k * 2 + 1)) {
                        case 'D':
                            sigArgType = ColumnType.DOUBLE;
                            break;
                        case 'B':
                            sigArgType = ColumnType.BYTE;
                            break;
                        case 'E':
                            sigArgType = ColumnType.SHORT;
                            break;
                        case 'F':
                            sigArgType = ColumnType.FLOAT;
                            break;
                        case 'I':
                            sigArgType = ColumnType.INT;
                            break;
                        case 'L':
                            sigArgType = ColumnType.LONG;
                            break;
                        case 'S':
                            sigArgType = ColumnType.STRING;
                            break;
                        case 'T':
                            sigArgType = ColumnType.BOOLEAN;
                            break;
                        case 'K':
                            sigArgType = ColumnType.SYMBOL;
                            break;
                        case 'M':
                            sigArgType = ColumnType.DATE;
                            break;
                        case 'N':
                            sigArgType = ColumnType.TIMESTAMP;
                            break;
                        default:
                            sigArgType = -1;
                            break;
                    }

                    if (sigArgConst && !arg.isConstant()) {
                        match = 0; // no match
                        break;
                    }

                    if (sigArgType == arg.getType()) {
                        continue;
                    }

                    // can we use overload mechanism?
                    if (arg.getType() >= ColumnType.BYTE
                            && arg.getType() <= ColumnType.DOUBLE
                            && sigArgType >= ColumnType.BYTE
                            && sigArgType <= ColumnType.DOUBLE
                            && arg.getType() < sigArgType) {
                        match = 1; // fuzzy match
                    } else {
                        // types mismatch
                        match = 0;
                        break;
                    }
                }

                if (match == 2) {
                    // exact match?
                    return factory.newInstance(args, node.position, configuration);
                } else if (match == 1) {
                    // fuzzy match
                    if (candidate == null) {
                        candidate = factory;
                    }
                    matchCount++;
                }
            }
        }
        if (matchCount > 1) {
            // ambiguous invocation target
            throw invalidFunction("ambiguous function call", node, args);
        }

        if (matchCount == 0) {
            // no signature match
            throw invalidFunction("no signature match", node, args);
        }

        return candidate.newInstance(args, node.position, configuration);
    }

    private void loadFunctionFactories(Iterable<FunctionFactory> functionFactories) {
        for (FunctionFactory factory : functionFactories) {

            final String sig = factory.getSignature();
            int openBraceIndex = sig.indexOf('(');
            if (openBraceIndex == -1) {
                LOG.error().$("open brace expected [sig=").$(sig).$(", class=").$(factory.getClass().getName()).$();
                continue;
            }

            if (openBraceIndex == 0) {
                LOG.error().$("empty function name [sig=").$(sig).$(", class=").$(factory.getClass().getName()).$();
                continue;
            }

            if (sig.charAt(sig.length() - 1) != ')') {
                LOG.error().$("close brace expected [sig=").$(sig).$(", class=").$(factory.getClass().getName()).$();
                continue;
            }

            if (functionNameIsValid(sig, openBraceIndex)) {
                String name = sig.substring(0, openBraceIndex);
                final int index = factories.keyIndex(name);
                final ObjList<FunctionFactory> overload;
                if (index < 0) {
                    overload = factories.valueAt(index);
                } else {
                    overload = new ObjList<>(4);
                    factories.putAt(index, name, overload);
                }
                overload.add(factory);
            } else {
                LOG.error().$("invalid function name sig=").$(sig).$(", class=").$(factory.getClass().getName()).$();
            }
        }
    }

    private Function lookupColumn(ExprNode node) throws ParserException {
        try {
            final int index = metadata.getColumnIndex(node.token);
            switch (metadata.getColumnQuick(index).getType()) {
                case ColumnType.BOOLEAN:
                    return new BooleanColumn(index);
                case ColumnType.BYTE:
                    return new ByteColumn(index);
                case ColumnType.SHORT:
                    return new ShortColumn(index);
                case ColumnType.INT:
                    return new IntColumn(index);
                case ColumnType.LONG:
                    return new LongColumn(index);
                case ColumnType.FLOAT:
                    return new FloatColumn(index);
                case ColumnType.DOUBLE:
                    return new DoubleColumn(index);
                case ColumnType.STRING:
                    return new StrColumn(index);
                case ColumnType.SYMBOL:
                    return new SymColumn(index);
                case ColumnType.BINARY:
                    return new BinColumn(index);
                case ColumnType.DATE:
                    return new DateColumn(index);
                default:
                    return new TimestampColumn(index);

            }
        } catch (NoSuchColumnException e) {
            throw ParserException.invalidColumn(node.position, node.token);
        }
    }

    private Function parseConstant(ExprNode node) throws ParserException {

        if (Chars.equals("null", node.token)) {
            return NullConstant.INSTANCE;
        }

        if (Chars.isQuoted(node.token)) {
            return new StrConstant(node.token);
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

        throw ParserException.$(node.position, "Unknown value type: " + node.token);
    }

    private Function toConstant(Function f) {
        switch (f.getType()) {
            case ColumnType.INT:
                return (f instanceof IntConstant) ? f : new IntConstant(f.getInt(null));
            case ColumnType.DOUBLE:
                return (f instanceof DoubleConstant) ? f : new DoubleConstant(f.getDouble(null));
            case ColumnType.FLOAT:
                return (f instanceof FloatConstant) ? f : new FloatConstant(f.getFloat(null));
            case ColumnType.BOOLEAN:
                return (f instanceof BooleanConstant) ? f : BooleanConstant.of(f.getBool(null));
            case ColumnType.STRING:
                if (f instanceof StrConstant) {
                    return f;
                }
                CharSequence cs = f.getStr(null);
                return cs == null ? NullConstant.INSTANCE : new StrConstant(cs);
            case ColumnType.LONG:
                return (f instanceof LongConstant) ? f : new LongConstant(f.getLong(null));
            case ColumnType.DATE:
                return (f instanceof DateConstant) ? f : new DateConstant(f.getDate(null));
            default:
                return f;
        }
    }

    static {
        invalidFunctionNameChars.add('.');
        invalidFunctionNameChars.add(',');
        invalidFunctionNameChars.add(' ');
        invalidFunctionNameChars.add('\"');
        invalidFunctionNameChars.add('\'');
    }
}

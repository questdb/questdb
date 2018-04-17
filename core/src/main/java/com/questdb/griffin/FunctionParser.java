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
import com.questdb.griffin.engine.functions.columns.*;
import com.questdb.griffin.engine.functions.constants.*;
import com.questdb.griffin.engine.params.Parameter;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.*;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayDeque;

public class FunctionParser implements PostOrderTreeTraversalAlgo.Visitor {
    public static final int VAR_ARG = 100;
    private static final Log LOG = LogFactory.getLog(FunctionParser.class);
    private static IntHashSet invalidFunctionNameChars = new IntHashSet();
    private final ObjList<Function> mutableArgs = new ObjList<>();
    private final ArrayDeque<Function> stack = new ArrayDeque<>();
    private final PostOrderTreeTraversalAlgo algo = new PostOrderTreeTraversalAlgo();
    private final CairoConfiguration configuration;
    private final CharSequenceObjHashMap<ObjList<FunctionFactory>> factories = new CharSequenceObjHashMap<>();
    private RecordMetadata metadata;
    private CharSequenceObjHashMap<Parameter> parameterMap;

    public FunctionParser(CairoConfiguration configuration, Iterable<FunctionFactory> functionFactories) {
        this.configuration = configuration;
        loadFunctionFactories(functionFactories);
    }

    public static Function createParameter(SqlNode node, CharSequenceObjHashMap<Parameter> parameterMap) {
        int index = parameterMap.keyIndex(node.token);
        if (index > -1) {
            Parameter p = new Parameter(node.position);
            parameterMap.putAt(index, node.token.toString(), p);
            return p;
        }
        return parameterMap.valueAt(index);
    }

    public static int getArgType(char c) {
        int sigArgType;
        switch (Character.toUpperCase(c)) {
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
            case 'U':
                sigArgType = ColumnType.BINARY;
                break;
            case 'V':
                sigArgType = VAR_ARG;
                break;
            default:
                sigArgType = -1;
                break;
        }
        return sigArgType;
    }

    public static int validateSignatureAndGetNameSeparator(String sig) throws SqlException {
        if (sig == null) {
            throw SqlException.$(0, "NULL signature");
        }

        int openBraceIndex = sig.indexOf('(');
        if (openBraceIndex == -1) {
            throw SqlException.$(0, "open brace expected");
        }

        if (openBraceIndex == 0) {
            throw SqlException.$(0, "empty function name");
        }

        if (sig.charAt(sig.length() - 1) != ')') {
            throw SqlException.$(0, "close brace expected");
        }

        int c = sig.charAt(0);
        if (c >= '0' && c <= '9') {
            throw SqlException.$(0, "name must not start with digit");
        }

        for (int i = 0; i < openBraceIndex; i++) {
            char cc = sig.charAt(i);
            if (invalidFunctionNameChars.contains(cc)) {
                throw SqlException.position(0).put("invalid character: ").put(cc);
            }
        }

        // validate data types
        for (int i = openBraceIndex + 1, n = sig.length() - 1; i < n; i++) {
            char cc = sig.charAt(i);
            if (getArgType(cc) == -1) {
                throw SqlException.position(0).put("illegal argument type: ").put(cc);
            }
        }
        return openBraceIndex;
    }

    public Function parseFunction(SqlNode node, RecordMetadata metadata, CharSequenceObjHashMap<Parameter> parameterMap) throws SqlException {
        this.parameterMap = parameterMap;
        this.metadata = metadata;
        algo.traverse(node, this);
        return stack.poll();
    }

    @Override
    public void visit(SqlNode node) throws SqlException {
        int argCount = node.paramCount;
        if (argCount == 0) {
            switch (node.type) {
                case SqlNode.LITERAL:
                    if (Chars.startsWith(node.token, ':')) {
                        stack.push(createParameter(node, parameterMap));
                    } else {
                        // lookup column
                        stack.push(createColumn(node));
                    }
                    break;
                case SqlNode.CONSTANT:
                    stack.push(createConstant(node));
                    break;
                default:
                    // lookup zero arg function from symbol table
                    stack.push(createFunction(node, null));
                    break;
            }
        } else {
            mutableArgs.clear();
            mutableArgs.setPos(argCount);
            for (int n = 0; n < argCount; n++) {
                Function c = stack.poll();
                if (c == null) {
                    throw SqlException.position(node.position).put("too few arguments [found=").put(n).put(",expected=").put(argCount).put(']');
                }
                mutableArgs.setQuick(n, c);
            }
            stack.push(createFunction(node, mutableArgs));
        }
    }

    private static SqlException invalidFunction(CharSequence message, SqlNode node, ObjList<Function> args) {
        SqlException ex = SqlException.position(node.position);
        ex.put(message);
        ex.put(": ");
        ex.put(node.token);
        ex.put('(');
        if (args != null) {
            for (int i = 0, n = args.size(); i < n; i++) {
                if (i > 0) {
                    ex.put(',');
                }
                ex.put(ColumnType.nameOf(args.getQuick(i).getType()));
            }
        }
        ex.put(')');
        return ex;
    }

    private Function checkAndCreateFunction(FunctionFactory factory, ObjList<Function> args, int position, CairoConfiguration configuration) throws SqlException {
        Function function;
        try {
            function = factory.newInstance(args, position, configuration);
        } catch (SqlException e) {
            throw e;
        } catch (Throwable e) {
            throw SqlException.position(position).put("exception in function factory");
        }

        if (function == null) {
            LOG.error().$("NULL function").$(" [signature=").$(factory.getSignature()).$(",class=").$(factory.getClass().getName()).$(']').$();
            throw SqlException.position(position).put("bad function factory (NULL), check log");
        }

        if (function.isConstant()) {
            return functionToConstant(position, function);
        }
        return function;
    }

    private Function createColumn(SqlNode node) throws SqlException {
        try {
            final int index = metadata.getColumnIndex(node.token);
            switch (metadata.getColumnQuick(index).getType()) {
                case ColumnType.BOOLEAN:
                    return new BooleanColumn(node.position, index);
                case ColumnType.BYTE:
                    return new ByteColumn(node.position, index);
                case ColumnType.SHORT:
                    return new ShortColumn(node.position, index);
                case ColumnType.INT:
                    return new IntColumn(node.position, index);
                case ColumnType.LONG:
                    return new LongColumn(node.position, index);
                case ColumnType.FLOAT:
                    return new FloatColumn(node.position, index);
                case ColumnType.DOUBLE:
                    return new DoubleColumn(node.position, index);
                case ColumnType.STRING:
                    return new StrColumn(node.position, index);
                case ColumnType.SYMBOL:
                    return new SymbolColumn(node.position, index);
                case ColumnType.BINARY:
                    return new BinColumn(node.position, index);
                case ColumnType.DATE:
                    return new DateColumn(node.position, index);
                default:
                    return new TimestampColumn(node.position, index);

            }
        } catch (NoSuchColumnException e) {
            throw SqlException.invalidColumn(node.position, node.token);
        }
    }

    private Function createConstant(SqlNode node) throws SqlException {

        if (Chars.equalsIgnoreCase(node.token, "null")) {
            return new NullConstant(node.position);
        }

        if (Chars.isQuoted(node.token)) {
            return new StrConstant(node.position, node.token);
        }

        if (Chars.equalsIgnoreCase(node.token, "true")) {
            return new BooleanConstant(node.position, true);
        }

        if (Chars.equalsIgnoreCase(node.token, "false")) {
            return new BooleanConstant(node.position, false);
        }

        try {
            return new IntConstant(node.position, Numbers.parseInt(node.token));
        } catch (NumericException ignore) {
        }

        try {
            return new LongConstant(node.position, Numbers.parseLong(node.token));
        } catch (NumericException ignore) {
        }

        try {
            return new DoubleConstant(node.position, Numbers.parseDouble(node.token));
        } catch (NumericException ignore) {
        }

        throw SqlException.position(node.position).put("invalid constant: ").put(node.token);
    }

    private Function createFunction(SqlNode node, ObjList<Function> args) throws SqlException {
        if (node.type == SqlNode.LAMBDA) {
            throw SqlException.$(node.position, "Cannot use lambda in this context");
        }
        ObjList<FunctionFactory> overload = factories.get(node.token);
        if (overload == null) {
            throw invalidFunction("unknown function name", node, args);
        }

        final int argCount;
        if (args == null) {
            argCount = 0;
        } else {
            argCount = args.size();
        }
        FunctionFactory candidate = null;
        int matchCount = 0;
        for (int i = 0, n = overload.size(); i < n; i++) {
            final FunctionFactory factory = overload.getQuick(i);
            final String signature = factory.getSignature();
            final int sigArgOffset = signature.indexOf('(') + 1;
            int sigArgCount = signature.length() - 1 - sigArgOffset;

            final boolean sigVarArg;
            final boolean sigVarArgConst;

            if (sigArgCount > 0) {
                char c = signature.charAt(sigArgOffset + sigArgCount - 1);
                sigVarArg = getArgType(Character.toUpperCase(c)) == VAR_ARG;
                sigVarArgConst = Character.isLowerCase(c);
            } else {
                sigVarArg = false;
                sigVarArgConst = false;
            }

            if (sigVarArg) {
                sigArgCount--;
            }

            if (argCount == 0 && sigArgCount == 0) {
                // this is no-arg function, match right away
                return checkAndCreateFunction(factory, args, node.position, configuration);
            }

            // otherwise, is number of arguments the same?
            if (sigArgCount == argCount || (sigVarArg && argCount >= sigArgCount)) {
                int match = 2; // match


                OUT:
                for (int k = 0; k < sigArgCount; k++) {
                    final Function arg = args.getQuick(k);
                    final char c = signature.charAt(sigArgOffset + k);

                    if (Character.isLowerCase(c) && !arg.isConstant()) {
                        match = 0; // no match
                        break;
                    }

                    final int sigArgType = getArgType(c);

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
                    } else if (arg.getType() == ColumnType.DOUBLE && arg.isConstant() && Double.isNaN(arg.getDouble(null))) {
                        // special case for NaN handling.
                        // NaN should be assignable to LONG and INT, but we need to make sure type of constant is
                        // correct for function call.

                        // replace constant inline! It is hacky but better than creating new arg list.
                        switch (sigArgType) {
                            case ColumnType.LONG:
                                args.setQuick(k, new LongConstant(arg.getPosition(), Numbers.LONG_NaN));
                                match = 1;
                                break;
                            case ColumnType.INT:
                                args.setQuick(k, new IntConstant(arg.getPosition(), Numbers.INT_NaN));
                                match = 1;
                                break;
                            default:
                                match = 0;
                                break OUT;
                        }
                    } else if (arg.getType() == ColumnType.PARAMETER) {
                        match = 1;
                    } else {
                        // types mismatch
                        match = 0;
                        break;
                    }
                }

                if (match == 2) {
                    // exact match?
                    // special case - if signature enforces constant vararg we
                    // have to ensure all args are indeed constant

                    if (sigVarArgConst && args != null) {
                        for (int k = sigArgCount; k < argCount; k++) {
                            Function func = args.getQuick(k);
                            if (!func.isConstant()) {
                                throw SqlException.$(func.getPosition(), "constant expected");
                            }
                        }
                    }

                    return checkAndCreateFunction(factory, args, node.position, configuration);
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

        return checkAndCreateFunction(candidate, args, node.position, configuration);
    }

    @NotNull
    private Function functionToConstant(int position, Function function) {
        switch (function.getType()) {
            case ColumnType.INT:
                if (function instanceof IntConstant) {
                    return function;
                } else {
                    return new IntConstant(position, function.getInt(null));
                }
            case ColumnType.BOOLEAN:
                if (function instanceof BooleanConstant) {
                    return function;
                } else {
                    return new BooleanConstant(position, function.getBool(null));
                }
            case ColumnType.BYTE:
                if (function instanceof ByteConstant) {
                    return function;
                } else {
                    return new ByteConstant(position, function.getByte(null));
                }
            case ColumnType.SHORT:
                if (function instanceof ShortConstant) {
                    return function;
                } else {
                    return new ShortConstant(position, function.getShort(null));
                }
            case ColumnType.FLOAT:
                if (function instanceof FloatConstant) {
                    return function;
                } else {
                    return new FloatConstant(position, function.getFloat(null));
                }
            case ColumnType.DOUBLE:
                if (function instanceof DoubleConstant) {
                    return function;
                } else {
                    return new DoubleConstant(position, function.getDouble(null));
                }
            case ColumnType.LONG:
                if (function instanceof LongConstant) {
                    return function;
                } else {
                    return new LongConstant(position, function.getLong(null));
                }
            case ColumnType.DATE:
                if (function instanceof DateConstant) {
                    return function;
                } else {
                    return new DateConstant(position, function.getDate(null));
                }
            case ColumnType.STRING:
                if (function instanceof StrConstant || function instanceof NullConstant) {
                    return function;
                } else {
                    final CharSequence value = function.getStr(null);
                    if (value == null) {
                        return new NullConstant(position);
                    }
                    return new StrConstant(position, value);
                }
            case ColumnType.SYMBOL:
                CharSequence value = function.getSymbol(null);
                if (value == null) {
                    return new NullConstant(position);
                }
                return new StrConstant(position, value);
            case ColumnType.TIMESTAMP:
                if (function instanceof TimestampConstant) {
                    return function;
                } else {
                    return new TimestampConstant(position, function.getTimestamp(null));
                }
            default:
                return function;
        }
    }

    int getFunctionCount() {
        return factories.size();
    }

    private void loadFunctionFactories(Iterable<FunctionFactory> functionFactories) {
        for (FunctionFactory factory : functionFactories) {

            final String sig = factory.getSignature();
            final int openBraceIndex;
            try {
                openBraceIndex = validateSignatureAndGetNameSeparator(sig);
            } catch (SqlException e) {
                LOG.error().$((Sinkable) e).$(" [signature=").$(factory.getSignature()).$(",class=").$(factory.getClass().getName()).$(']').$();
                continue;
            }

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

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
import com.questdb.griffin.engine.functions.Parameter;
import com.questdb.griffin.engine.functions.columns.*;
import com.questdb.griffin.engine.functions.constants.*;
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

    public static Function getOrCreate(SqlNode node, CharSequenceObjHashMap<Parameter> parameterMap) {
        Parameter p = parameterMap.get(node.token);
        if (p == null) {
            parameterMap.put(node.token, p = new Parameter());
            p.setName(node.token.toString());
        }
        return p;
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
                        stack.push(getOrCreate(node, parameterMap));
                    } else {
                        // lookup column
                        stack.push(lookupColumn(node));
                    }
                    break;
                case SqlNode.CONSTANT:
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
                    throw SqlException.position(node.position).put("too few arguments [found=").put(n).put(",expected=").put(argCount).put(']');
                }
                mutableArgs.setQuick(n, c);
            }
            stack.push(getFunction(node, mutableArgs));
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

    private static int validateSignatureAndGetNameSeparator(String sig) throws SqlException {
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

    private static int getArgType(char c) {
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
            default:
                sigArgType = -1;
                break;
        }
        return sigArgType;
    }

    private Function getFunction(SqlNode node, ObjList<Function> args) throws SqlException {
        if (node.type == SqlNode.LAMBDA) {
            throw SqlException.$(node.position, "Cannot use lambda in this context");
        }
        return getFunction0(node, args);
    }

    private Function getFunction0(SqlNode node, ObjList<Function> args) throws SqlException {
        ObjList<FunctionFactory> overload = factories.get(node.token);
        if (overload == null) {
            throw invalidFunction("unknown function name", node, args);
        }

        final int argCount = args == null ? 0 : args.size();
        FunctionFactory candidate = null;
        int matchCount = 0;
        for (int i = 0, n = overload.size(); i < n; i++) {
            final FunctionFactory factory = overload.getQuick(i);
            final String signature = factory.getSignature();
            final int sigArgOffset = signature.indexOf('(') + 1;
            final int sigArgCount = signature.length() - 1 - sigArgOffset;

            if (argCount == 0 && sigArgCount == 0) {
                // this is no-arg function, match right away
                return factory.newInstance(args, node.position, configuration);
            }

            // otherwise, is number of arguments the same?
            if (sigArgCount == argCount) {
                int match = 2; // match

                for (int k = 0; k < argCount; k++) {
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
                LOG.error().$("skipped: ").$(sig).$(" [class=").$(factory.getClass().getName()).$(']').$();
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

    private Function lookupColumn(SqlNode node) throws SqlException {
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
            throw SqlException.invalidColumn(node.position, node.token);
        }
    }

    private Function parseConstant(SqlNode node) throws SqlException {

        if (Chars.equalsIgnoreCase(node.token, "null")) {
            return NullConstant.INSTANCE;
        }

        if (Chars.isQuoted(node.token)) {
            return new StrConstant(node.token);
        }

        if (Chars.equalsIgnoreCase(node.token, "true")) {
            return BooleanConstant.TRUE;
        }

        if (Chars.equalsIgnoreCase(node.token, "false")) {
            return BooleanConstant.FALSE;
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

        throw SqlException.position(node.position).put("invalid constant: ").put(node.token);
    }

    static {
        invalidFunctionNameChars.add('.');
        invalidFunctionNameChars.add(',');
        invalidFunctionNameChars.add(' ');
        invalidFunctionNameChars.add('\"');
        invalidFunctionNameChars.add('\'');
    }
}

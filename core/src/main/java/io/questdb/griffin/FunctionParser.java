/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.engine.functions.AbstractUnaryTimestampFunction;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.functions.bind.IndexedParameterLinkFunction;
import io.questdb.griffin.engine.functions.bind.NamedParameterLinkFunction;
import io.questdb.griffin.engine.functions.cast.CastStrToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastSymbolToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.columns.*;
import io.questdb.griffin.engine.functions.constants.*;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayDeque;

import static io.questdb.griffin.SqlKeywords.isNullKeyword;

public class FunctionParser implements PostOrderTreeTraversalAlgo.Visitor {
    private static final Log LOG = LogFactory.getLog(FunctionParser.class);

    // order of values matters here, partial match must have greater value than fuzzy match
    private static final int MATCH_NO_MATCH = 0;
    private static final int MATCH_FUZZY_MATCH = 1;
    private static final int MATCH_PARTIAL_MATCH = 2;
    private static final int MATCH_EXACT_MATCH = 3;

    private final ObjList<Function> mutableArgs = new ObjList<>();
    private final IntList mutableArgPositions = new IntList();
    private final ArrayDeque<Function> functionStack = new ArrayDeque<>();
    private final IntStack positionStack = new IntStack();
    private final PostOrderTreeTraversalAlgo traverseAlgo = new PostOrderTreeTraversalAlgo();
    private final CairoConfiguration configuration;
    private final ArrayDeque<RecordMetadata> metadataStack = new ArrayDeque<>();
    private final FunctionFactoryCache functionFactoryCache;
    private final IntList undefinedVariables = new IntList();
    private RecordMetadata metadata;
    private SqlCodeGenerator sqlCodeGenerator;
    private SqlExecutionContext sqlExecutionContext;

    public FunctionParser(CairoConfiguration configuration, FunctionFactoryCache functionFactoryCache) {
        this.configuration = configuration;
        this.functionFactoryCache = functionFactoryCache;
    }

    @NotNull
    public static ScalarFunction createColumn(int position, CharSequence name, RecordMetadata metadata) throws SqlException {
        final int index = metadata.getColumnIndexQuiet(name);

        if (index == -1) {
            throw SqlException.invalidColumn(position, name);
        }

        switch (metadata.getColumnType(index)) {
            case ColumnType.BOOLEAN:
                return BooleanColumn.newInstance(index);
            case ColumnType.BYTE:
                return ByteColumn.newInstance(index);
            case ColumnType.SHORT:
                return ShortColumn.newInstance(index);
            case ColumnType.CHAR:
                return CharColumn.newInstance(index);
            case ColumnType.INT:
                return IntColumn.newInstance(index);
            case ColumnType.LONG:
                return LongColumn.newInstance(index);
            case ColumnType.FLOAT:
                return FloatColumn.newInstance(index);
            case ColumnType.DOUBLE:
                return DoubleColumn.newInstance(index);
            case ColumnType.STRING:
                return StrColumn.newInstance(index);
            case ColumnType.SYMBOL:
                return new SymbolColumn(index, metadata.isSymbolTableStatic(index));
            case ColumnType.BINARY:
                return BinColumn.newInstance(index);
            case ColumnType.DATE:
                return DateColumn.newInstance(index);
            case ColumnType.TIMESTAMP:
                return TimestampColumn.newInstance(index);
            case ColumnType.RECORD:
                return new RecordColumn(index, metadata.getMetadata(index));
            default:
                return Long256Column.newInstance(index);
        }
    }

    public Function createBindVariable(int position, CharSequence name) throws SqlException {
        if (name != null && name.length() > 0) {
            switch (name.charAt(0)) {
                case ':':
                    return createNamedParameter(position, name);
                case '$':
                    return parseIndexedParameter(position, name);
                default:
                    return new StrConstant(name);
            }
        }
        return StrConstant.NULL;
    }

    public Function createBindVariable0(int position, CharSequence name) throws SqlException {
        if (name.charAt(0) != ':') {
            return parseIndexedParameter(position, name);
        }
        return createNamedParameter(position, name);
    }

    public Function createIndexParameter(int variableIndex, int position) throws SqlException {
        Function function = getBindVariableService().getFunction(variableIndex);
        if (function == null) {
            // bind variable is undefined
            return new IndexedParameterLinkFunction(variableIndex, -1, position);
        }
        return new IndexedParameterLinkFunction(variableIndex, function.getType(), position);
    }

    public Function createNamedParameter(int position, CharSequence name) throws SqlException {
        Function function = getBindVariableService().getFunction(name);
        if (function == null) {
            throw SqlException.position(position).put("undefined bind variable: ").put(name);
        }
        return new NamedParameterLinkFunction(Chars.toString(name), function.getType());
    }

    public int getFunctionCount() {
        return functionFactoryCache.getFunctionCount();
    }

    public FunctionFactoryCache getFunctionFactoryCache() {
        return functionFactoryCache;
    }

    public boolean isCursor(CharSequence token) {
        return functionFactoryCache.isCursor(token);
    }

    public boolean isGroupBy(CharSequence token) {
        return functionFactoryCache.isGroupBy(token);
    }

    public boolean isRuntimeConstant(CharSequence token) {
        return functionFactoryCache.isRuntimeConstant(token);
    }

    public boolean isValidNoArgFunction(ExpressionNode node) {
        final ObjList<FunctionFactoryDescriptor> overload = functionFactoryCache.getOverloadList(node.token);
        if (overload == null) {
            return false;
        }

        for (int i = 0, n = overload.size(); i < n; i++) {
            FunctionFactoryDescriptor ffd = overload.getQuick(i);
            if (ffd.getSigArgCount() == 0) {
                return true;
            }
        }

        return false;
    }

    /**
     * Creates function instance. When node type is {@link ExpressionNode#LITERAL} a column or parameter
     * function is returned. We will be using the supplied {@link #metadata} to resolve type of column. When node token
     * begins with ':' parameter is looked up from the supplied bindVariableService.
     * <p>
     * When node type is {@link ExpressionNode#CONSTANT} a constant function is returned. Type of constant is
     * inferred from value of node token.
     * <p>
     * When node type is {@link ExpressionNode#QUERY} a cursor function is returned. Cursor function can be wrapping
     * stateful instance of {@link RecordCursorFactory} that has to be closed when disposed of.
     * Such instances are added to the supplied list of {@link java.io.Closeable} items.
     * <p>
     * For any other node type a function instance is created using {@link FunctionFactory}
     *
     * @param node             expression node
     * @param metadata         metadata for resolving types of columns.
     * @param executionContext for resolving parameters, which are ':' prefixed literals and creating cursors
     * @return function instance
     * @throws SqlException when function cannot be created. Can be one of list but not limited to
     *                      <ul>
     *                      <li>column not found</li>
     *                      <li>parameter not found</li>
     *                      <li>unknown function name</li>
     *                      <li>function argument mismatch</li>
     *                      <li>sql compilation errors in case of lambda</li>
     *                      </ul>
     */
    public Function parseFunction(
            ExpressionNode node,
            RecordMetadata metadata,
            SqlExecutionContext executionContext
    ) throws SqlException {

        this.sqlExecutionContext = executionContext;

        if (this.metadata != null) {
            metadataStack.push(this.metadata);
        }
        try {
            this.metadata = metadata;
            traverseAlgo.traverse(node, this);
            final Function function = functionStack.poll();
            positionStack.pop();
            assert positionStack.size() == functionStack.size();
            if (function != null && function.isConstant() && (function instanceof ScalarFunction)) {
                try {
                    return functionToConstant(function);
                } finally {
                    function.close();
                }
            }
            return function;
        } finally {
            if (metadataStack.size() == 0) {
                this.metadata = null;
            } else {
                this.metadata = metadataStack.poll();
            }
        }
    }

    public void setSqlCodeGenerator(SqlCodeGenerator sqlCodeGenerator) {
        this.sqlCodeGenerator = sqlCodeGenerator;
    }

    @Override
    public void visit(ExpressionNode node) throws SqlException {
        int argCount = node.paramCount;
        if (argCount == 0) {
            switch (node.type) {
                case ExpressionNode.LITERAL:
                    functionStack.push(createColumn(node.position, node.token));
                    break;
                case ExpressionNode.BIND_VARIABLE:
                    functionStack.push(createBindVariable0(node.position, node.token));
                    break;
                case ExpressionNode.MEMBER_ACCESS:
                    functionStack.push(new StrConstant(node.token));
                    break;
                case ExpressionNode.CONSTANT:
                    functionStack.push(createConstant(node.position, node.token));
                    break;
                case ExpressionNode.QUERY:
                    functionStack.push(createCursorFunction(node));
                    break;
                default:
                    // lookup zero arg function from symbol table
                    functionStack.push(createFunction(node, null, null));
                    break;
            }
        } else {
            mutableArgs.clear();
            mutableArgs.setPos(argCount);
            mutableArgPositions.clear();
            mutableArgPositions.setPos(argCount);
            for (int n = 0; n < argCount; n++) {
                mutableArgs.setQuick(n, functionStack.poll());
                mutableArgPositions.setQuick(n, positionStack.pop());
            }
            functionStack.push(createFunction(node, mutableArgs, mutableArgPositions));
        }
        positionStack.push(node.position);
    }

    private static SqlException invalidFunction(ExpressionNode node, ObjList<Function> args) {
        SqlException ex = SqlException.position(node.position);
        ex.put("unknown function name");
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

    private static SqlException invalidArgument(ExpressionNode node, ObjList<Function> args, FunctionFactoryDescriptor descriptor) {
        SqlException ex = SqlException.position(node.position);
        ex.put("unexpected argument for function: ");
        ex.put(node.token);
        ex.put(". expected args: ");
        ex.put('(');
        if (descriptor != null) {
            for (int i = 0, n = descriptor.getSigArgCount(); i < n; i++) {
                if (i > 0) {
                    ex.put(',');
                }
                final int mask = descriptor.getArgTypeMask(i);
                ex.put(ColumnType.nameOf(FunctionFactoryDescriptor.toType(mask)));
                if (FunctionFactoryDescriptor.isArray(mask)) {
                    ex.put("[]");
                }
                if (FunctionFactoryDescriptor.isConstant(mask)) {
                    ex.put(" constant");
                }
            }
        }
        ex.put("). actual args: ");
        ex.put('(');
        if (args != null) {
            for (int i = 0, n = args.size(); i < n; i++) {
                if (i > 0) {
                    ex.put(',');
                }
                Function arg = args.getQuick(i);
                ex.put(ColumnType.nameOf(arg.getType()));
                if (arg.isConstant()) {
                    ex.put(" constant");
                }
            }
        }
        ex.put(')');
        return ex;
    }

    private Function checkAndCreateFunction(
            FunctionFactory factory,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            int position,
            CairoConfiguration configuration
    ) throws SqlException {
        Function function;
        try {
            function = factory.newInstance(position, args, argPositions, configuration, sqlExecutionContext);
        } catch (SqlException e) {
            throw e;
        } catch (Throwable e) {
            LOG.error().$("exception in function factory: ").$(e).$();
            throw SqlException.position(position).put("exception in function factory");
        }

        if (function == null) {
            LOG.error().$("NULL function").$(" [signature=").$(factory.getSignature()).$(",class=").$(factory.getClass().getName()).$(']').$();
            throw SqlException.position(position).put("bad function factory (NULL), check log");
        }
        return function;
    }

    private long convertToTimestamp(CharSequence str, int position) throws SqlException {
        try {
            return IntervalUtils.parseFloorPartialDate(str);
        } catch (NumericException e) {
            throw SqlException.invalidDate(position);
        }
    }

    private Function createColumn(int position, CharSequence columnName) throws SqlException {
        return createColumn(position, columnName, metadata);
    }

    private Function createConstant(int position, final CharSequence tok) throws SqlException {

        final int len = tok.length();

        if (isNullKeyword(tok)) {
            return StrConstant.NULL;
        }

        if (Chars.isQuoted(tok)) {
            if (len == 3) {
                // this is 'x' - char
                return CharConstant.newInstance(tok.charAt(1));
            }

            if (len == 2) {
                // empty
                return CharConstant.ZERO;
            }
            return new StrConstant(tok);
        }

        // special case E'str'
        // we treat it like normal string for now
        if (len > 2 && tok.charAt(0) == 'E' && tok.charAt(1) == '\'') {
            return new StrConstant(Chars.toString(tok, 2, len - 1));
        }

        if (SqlKeywords.isTrueKeyword(tok)) {
            return BooleanConstant.TRUE;
        }

        if (SqlKeywords.isFalseKeyword(tok)) {
            return BooleanConstant.FALSE;
        }

        try {
            return IntConstant.newInstance(Numbers.parseInt(tok));
        } catch (NumericException ignore) {
        }

        try {
            return LongConstant.newInstance(Numbers.parseLong(tok));
        } catch (NumericException ignore) {
        }

        try {
            return DoubleConstant.newInstance(Numbers.parseDouble(tok));
        } catch (NumericException ignore) {
        }

        try {
            return FloatConstant.newInstance(Numbers.parseFloat(tok));
        } catch (NumericException ignore) {
        }

        // type constant for 'CAST' operation

        final int columnType = ColumnType.columnTypeOf(tok);

        if (columnType > -1) {
            return Constants.getTypeConstant(columnType);
        }

        throw SqlException.position(position).put("invalid constant: ").put(tok);
    }

    private Function createCursorFunction(ExpressionNode node) throws SqlException {
        assert node.queryModel != null;
        return new CursorFunction(sqlCodeGenerator.generate(node.queryModel, sqlExecutionContext));
    }

    private Function createFunction(
            ExpressionNode node,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions
    ) throws SqlException {
        final ObjList<FunctionFactoryDescriptor> overload = functionFactoryCache.getOverloadList(node.token);
        if (overload == null) {
            throw invalidFunction(node, args);
        }

        final int argCount;
        if (args == null) {
            argCount = 0;
        } else {
            argCount = args.size();
        }

        FunctionFactory candidate = null;
        FunctionFactoryDescriptor candidateDescriptor = null;
        boolean candidateSigVarArgConst = false;
        int candidateSigArgCount = 0;
        int candidateSigArgTypeSum = -1;
        int bestMatch = MATCH_NO_MATCH;

        undefinedVariables.clear();

        // find all undefined args for the purpose of setting
        // their types when we find suitable candidate function
        for (int i = 0; i < argCount; i++) {
            if (args.getQuick(i).isUndefined()) {
                undefinedVariables.add(i);
            }
        }

        for (int i = 0, n = overload.size(); i < n; i++) {
            final FunctionFactoryDescriptor descriptor = overload.getQuick(i);
            final FunctionFactory factory = descriptor.getFactory();
            int sigArgCount = descriptor.getSigArgCount();

            final boolean sigVarArg;
            final boolean sigVarArgConst;

            if (candidateDescriptor == null) {
                candidateDescriptor = descriptor;
            }

            if (sigArgCount > 0) {
                final int lastSigArgMask = descriptor.getArgTypeMask(sigArgCount - 1);
                sigVarArg = FunctionFactoryDescriptor.toType(lastSigArgMask) == ColumnType.VAR_ARG;
                sigVarArgConst = FunctionFactoryDescriptor.isConstant(lastSigArgMask);
            } else {
                sigVarArg = false;
                sigVarArgConst = false;
            }

            if (sigVarArg) {
                sigArgCount--;
            }

            if (argCount == 0 && sigArgCount == 0) {
                // this is no-arg function, match right away
                return checkAndCreateFunction(factory, args, argPositions, node.position, configuration);
            }

            // otherwise, is number of arguments the same?
            if (sigArgCount == argCount || (sigVarArg && argCount >= sigArgCount)) {
                int match = MATCH_NO_MATCH; // no match
                if (sigArgCount == 0) {
                    match = MATCH_EXACT_MATCH;
                }

                int sigArgTypeSum = 0;
                for (int k = 0; k < sigArgCount; k++) {
                    final Function arg = args.getQuick(k);
                    final boolean undefined = arg.isUndefined();
                    final int sigArgTypeMask = descriptor.getArgTypeMask(k);

                    if (FunctionFactoryDescriptor.isConstant(sigArgTypeMask) && !arg.isConstant()) {
                        match = MATCH_NO_MATCH; // no match
                        break;
                    }

                    final boolean isArray = FunctionFactoryDescriptor.isArray(sigArgTypeMask);
                    final boolean isScalar = arg instanceof ScalarFunction;
                    if ((isArray && isScalar) || (!isArray && !isScalar)) {
                        match = MATCH_NO_MATCH; // no match
                        break;
                    }

                    final int sigArgType = FunctionFactoryDescriptor.toType(sigArgTypeMask);


                    if (sigArgType == arg.getType()) {
                        switch (match) {
                            case MATCH_NO_MATCH: // was it no match
                                match = MATCH_EXACT_MATCH;
                                break;
                            case MATCH_FUZZY_MATCH: // was it fuzzy match ?
                                match = MATCH_PARTIAL_MATCH; // this is mixed match, fuzzy and exact
                                break;
                            default:
                                // don't change match otherwise
                                break;
                        }
                        continue;
                    }

                    final int argType = arg.getType();

                    int overloadDistance = ColumnType.overloadDistance(argType, sigArgType);
                    sigArgTypeSum += overloadDistance;
                    // Overload with cast to higher precision
                    boolean overloadPossible = overloadDistance != ColumnType.NO_OVERLOAD;

                    // Overload when arg is double NaN to func which accepts INT, LONG
                    overloadPossible |= argType == ColumnType.DOUBLE &&
                            arg.isConstant() &&
                            Double.isNaN(arg.getDouble(null)) &&
                            (sigArgType == ColumnType.LONG || sigArgType == ColumnType.INT);

                    // Implicit cast from CHAR to STRING
                    overloadPossible |= argType == ColumnType.CHAR &&
                            sigArgType == ColumnType.STRING;

                    // Implicit cast from STRING to TIMESTAMP
                    overloadPossible |= argType == ColumnType.STRING &&
                            sigArgType == ColumnType.TIMESTAMP && !factory.isGroupBy();

                    // Implicit cast from SYMBOL to TIMESTAMP
                    overloadPossible |= argType == ColumnType.SYMBOL &&
                            sigArgType == ColumnType.TIMESTAMP && !factory.isGroupBy();

                    overloadPossible |= undefined;

                    // can we use overload mechanism?
                    if (overloadPossible) {
                        switch (match) {
                            case MATCH_NO_MATCH: // no match?
                                match = MATCH_FUZZY_MATCH; // upgrade to fuzzy match
                                break;
                            case MATCH_EXACT_MATCH: // was it full match so far? ? oh, well, fuzzy now
                                match = MATCH_PARTIAL_MATCH; // downgrade
                                break;
                            default:
                                break; // don't change match otherwise
                        }
                    } else {
                        // types mismatch
                        match = MATCH_NO_MATCH;
                        break;
                    }
                }

                if (match == MATCH_NO_MATCH) {
                    continue;
                }

                if (match == MATCH_EXACT_MATCH || match >= bestMatch) {
                    // exact match may be?
                    // special case - if signature enforces constant vararg we
                    // have to ensure all args are indeed constant


                    if (match != MATCH_EXACT_MATCH) {
                        if (candidateSigArgTypeSum > sigArgTypeSum || bestMatch < match) {
                            candidate = factory;
                            candidateDescriptor = descriptor;
                            candidateSigArgCount = sigArgCount;
                            candidateSigVarArgConst = sigVarArgConst;
                            candidateSigArgTypeSum = sigArgTypeSum;
                        }
                        bestMatch = match;
                    } else {
                        candidate = factory;
                        candidateDescriptor = descriptor;
                        candidateSigArgCount = sigArgCount;
                        candidateSigVarArgConst = sigVarArgConst;
                        break;
                    }
                }
            }
        }

        if (candidate == null) {
            // no signature match
            throw invalidArgument(node, args, candidateDescriptor);
        }

        if (candidateSigVarArgConst) {
            for (int k = candidateSigArgCount; k < argCount; k++) {
                Function func = args.getQuick(k);
                if (!func.isConstant()) {
                    throw SqlException.$(argPositions.getQuick(k), "constant expected");
                }
            }
        }

        // substitute NaNs with appropriate types
        for (int k = 0; k < candidateSigArgCount; k++) {
            final Function arg = args.getQuick(k);
            final int sigArgType = FunctionFactoryDescriptor.toType(candidateDescriptor.getArgTypeMask(k));
            if (arg.getType() == ColumnType.DOUBLE && arg.isConstant() && Double.isNaN(arg.getDouble(null))) {
                if (sigArgType == ColumnType.LONG) {
                    args.setQuick(k, LongConstant.NULL);
                } else if (sigArgType == ColumnType.INT) {
                    args.setQuick(k, IntConstant.NULL);
                }
            }
        }

        // it is possible that we have more undefined variables than
        // args in the descriptor, in case of vararg for example
        for (int i = 0, n = undefinedVariables.size(); i < n; i++) {
            final int pos = undefinedVariables.getQuick(i);
            if (pos < candidateSigArgCount) {
                final int sigArgType = FunctionFactoryDescriptor.toType(candidateDescriptor.getArgTypeMask(pos));
                args.getQuick(pos).assignType(sigArgType, sqlExecutionContext.getBindVariableService());
            } else {
                args.getQuick(pos).assignType(ColumnType.VAR_ARG, sqlExecutionContext.getBindVariableService());
            }
        }

        // convert arguments if necessary
        for (int k = 0; k < candidateSigArgCount; k++) {
            final Function arg = args.getQuick(k);
            final int sigArgType = FunctionFactoryDescriptor.toType(candidateDescriptor.getArgTypeMask(k));
            if ((arg.getType() == ColumnType.STRING  || arg.getType() == ColumnType.SYMBOL) && sigArgType == ColumnType.TIMESTAMP) {
                int position = argPositions.getQuick(k);
                if (arg.isConstant()) {
                    long timestamp = convertToTimestamp(arg.getStr(null), position);
                    args.set(k, TimestampConstant.newInstance(timestamp));
                } else {
                    AbstractUnaryTimestampFunction castFn;
                    if (arg.getType() == ColumnType.STRING) {
                        castFn = new CastStrToTimestampFunctionFactory.Func(position, arg);
                    } else {
                        castFn = new CastSymbolToTimestampFunctionFactory.Func(arg);
                    }
                    args.set(k, castFn);
                }
            }
        }

        LOG.debug().$("call ").$(node).$(" -> ").$(candidate.getSignature()).$();
        return checkAndCreateFunction(candidate, args, argPositions, node.position, configuration);
    }

    private Function functionToConstant(Function function) {
        switch (function.getType()) {
            case ColumnType.INT:
                if (function instanceof IntConstant) {
                    return function;
                } else {
                    return IntConstant.newInstance(function.getInt(null));
                }
            case ColumnType.BOOLEAN:
                if (function instanceof BooleanConstant) {
                    return function;
                } else {
                    return BooleanConstant.of(function.getBool(null));
                }
            case ColumnType.BYTE:
                if (function instanceof ByteConstant) {
                    return function;
                } else {
                    return ByteConstant.newInstance(function.getByte(null));
                }
            case ColumnType.SHORT:
                if (function instanceof ShortConstant) {
                    return function;
                } else {
                    return ShortConstant.newInstance(function.getShort(null));
                }
            case ColumnType.CHAR:
                if (function instanceof CharConstant) {
                    return function;
                } else {
                    return CharConstant.newInstance(function.getChar(null));
                }
            case ColumnType.FLOAT:
                if (function instanceof FloatConstant) {
                    return function;
                } else {
                    return FloatConstant.newInstance(function.getFloat(null));
                }
            case ColumnType.DOUBLE:
                if (function instanceof DoubleConstant) {
                    return function;
                } else {
                    return DoubleConstant.newInstance(function.getDouble(null));
                }
            case ColumnType.LONG:
                if (function instanceof LongConstant) {
                    return function;
                } else {
                    return LongConstant.newInstance(function.getLong(null));
                }
            case ColumnType.LONG256:
                if (function instanceof Long256Constant) {
                    return function;
                } else {
                    return new Long256Constant(function.getLong256A(null));
                }
            case ColumnType.DATE:
                if (function instanceof DateConstant) {
                    return function;
                } else {
                    return DateConstant.getInstance(function.getDate(null));
                }
            case ColumnType.STRING:
                if (function instanceof StrConstant) {
                    return function;
                } else {
                    return StrConstant.newInstance(function.getStr(null));
                }
            case ColumnType.SYMBOL:
                if (function instanceof SymbolConstant) {
                    return function;
                }
                return SymbolConstant.newInstance(function.getSymbol(null));
            case ColumnType.TIMESTAMP:
                if (function instanceof TimestampConstant) {
                    return function;
                } else {
                    return TimestampConstant.newInstance(function.getTimestamp(null));
                }
            default:
                return function;
        }
    }

    @NotNull
    private BindVariableService getBindVariableService() throws SqlException {
        final BindVariableService bindVariableService = sqlExecutionContext.getBindVariableService();
        if (bindVariableService == null) {
            throw SqlException.$(0, "bind variable service is not provided");
        }
        return bindVariableService;
    }

    private Function parseIndexedParameter(int position, CharSequence name) throws SqlException {
        // get variable index from token
        try {
            final int variableIndex = Numbers.parseInt(name, 1, name.length());
            if (variableIndex < 1) {
                throw SqlException.$(position, "invalid bind variable index [value=").put(variableIndex).put(']');
            }
            return createIndexParameter(variableIndex - 1, position);
        } catch (NumericException e) {
            throw SqlException.$(position, "invalid bind variable index [value=").put(name).put(']');
        }
    }

    static {
        for (int i = 0, n = SqlCompiler.sqlControlSymbols.size(); i < n; i++) {
            FunctionFactoryCache.invalidFunctionNames.add(SqlCompiler.sqlControlSymbols.getQuick(i));
        }
        FunctionFactoryCache.invalidFunctionNameChars.add(' ');
        FunctionFactoryCache.invalidFunctionNameChars.add('\"');
        FunctionFactoryCache.invalidFunctionNameChars.add('\'');
    }
}

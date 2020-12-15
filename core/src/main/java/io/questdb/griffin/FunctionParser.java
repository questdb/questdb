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
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.functions.bind.IndexedParameterLinkFunction;
import io.questdb.griffin.engine.functions.bind.NamedParameterLinkFunction;
import io.questdb.griffin.engine.functions.columns.*;
import io.questdb.griffin.engine.functions.constants.*;
import io.questdb.griffin.model.ExpressionNode;
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
    private final ArrayDeque<Function> stack = new ArrayDeque<>();
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

    public Function createIndexParameter(int variableIndex, ExpressionNode node) throws SqlException {
        Function function = getBindVariableService().getFunction(variableIndex);
        if (function == null) {
            // bind variable is undefined
            return new IndexedParameterLinkFunction(variableIndex, -1, node.position);
        }
        return new IndexedParameterLinkFunction(variableIndex, function.getType(), node.position);
    }

    public Function createNamedParameter(ExpressionNode node) throws SqlException {
        Function function = getBindVariableService().getFunction(node.token);
        if (function == null) {
            throw SqlException.position(node.position).put("undefined bind variable: ").put(node.token);
        }
        return new NamedParameterLinkFunction(Chars.toString(node.token), function.getType(), node.position);
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
            final Function function = stack.poll();
            if (function != null && function.isConstant() && (function instanceof ScalarFunction)) {
                try (function) {
                    return functionToConstant(function);
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
                    stack.push(createColumn(node));
                    break;
                case ExpressionNode.BIND_VARIABLE:
                    if (Chars.startsWith(node.token, ':')) {
                        stack.push(createNamedParameter(node));
                    } else {
                        // get variable index from token
                        try {
                            final int variableIndex = Numbers.parseInt(node.token, 1, node.token.length());
                            if (variableIndex < 1) {
                                throw SqlException.$(node.position, "invalid bind variable index [value=").put(variableIndex).put(']');
                            }
                            stack.push(createIndexParameter(variableIndex - 1, node));
                        } catch (NumericException e) {
                            // not a number - must be a column
                            stack.push(createColumn(node));
                        }
                    }
                    break;
                case ExpressionNode.MEMBER_ACCESS:
                    stack.push(new StrConstant(node.position, node.token));
                    break;
                case ExpressionNode.CONSTANT:
                    stack.push(createConstant(node));
                    break;
                case ExpressionNode.QUERY:
                    stack.push(createCursorFunction(node));
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
                mutableArgs.setQuick(n, stack.poll());
            }
            stack.push(createFunction(node, mutableArgs));
        }
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
            for (int i = 0, n = descriptor.getSigCount(); i < n; i++) {
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
            int position,
            CairoConfiguration configuration,
            boolean isNegated,
            boolean isFlipped
    ) throws SqlException {
        Function function;
        try {
            if (factory instanceof AbstractBooleanFunctionFactory) {
                ((AbstractBooleanFunctionFactory) factory).setNegated(isNegated);
            }
            if (isFlipped) {
                Function tmp = args.getQuick(0);
                args.setQuick(0, args.getQuick(1));
                args.setQuick(1, tmp);
            }
            function = factory.newInstance(args, position, configuration, sqlExecutionContext);
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

    private Function createColumn(ExpressionNode node) throws SqlException {
        final int index = metadata.getColumnIndexQuiet(node.token);

        if (index == -1) {
            throw SqlException.invalidColumn(node.position, node.token);
        }

        switch (metadata.getColumnType(index)) {
            case ColumnType.BOOLEAN:
                return new BooleanColumn(node.position, index);
            case ColumnType.BYTE:
                return new ByteColumn(node.position, index);
            case ColumnType.SHORT:
                return new ShortColumn(node.position, index);
            case ColumnType.CHAR:
                return new CharColumn(node.position, index);
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
                return new SymbolColumn(node.position, index, metadata.isSymbolTableStatic(index));
            case ColumnType.BINARY:
                return new BinColumn(node.position, index);
            case ColumnType.DATE:
                return new DateColumn(node.position, index);
            case ColumnType.TIMESTAMP:
                return new TimestampColumn(node.position, index);
            case ColumnType.RECORD:
                return new RecordColumn(node.position, index, metadata.getMetadata(index));
            default:
                return new Long256Column(node.position, index);
        }
    }

    private Function createConstant(ExpressionNode node) throws SqlException {

        final int len = node.token.length();
        final CharSequence tok = node.token;

        if (isNullKeyword(tok)) {
            return new NullStrConstant(node.position);
        }

        if (Chars.isQuoted(tok)) {
            if (len == 3) {
                // this is 'x' - char
                return new CharConstant(node.position, tok.charAt(1));
            }

            if (len == 2) {
                // empty
                return new CharConstant(node.position, (char) 0);
            }
            return new StrConstant(node.position, tok);
        }

        // special case E'str'
        // we treat it like normal string for now
        if (len > 2 && tok.charAt(0) == 'E' && tok.charAt(1) == '\'') {
            return new StrConstant(node.position, Chars.toString(tok, 2, len - 1));
        }

        if (SqlKeywords.isTrueKeyword(tok)) {
            return new BooleanConstant(node.position, true);
        }

        if (SqlKeywords.isFalseKeyword(tok)) {
            return new BooleanConstant(node.position, false);
        }

        try {
            return new IntConstant(node.position, Numbers.parseInt(tok));
        } catch (NumericException ignore) {
        }

        try {
            return new LongConstant(node.position, Numbers.parseLong(tok));
        } catch (NumericException ignore) {
        }

        try {
            return new DoubleConstant(node.position, Numbers.parseDouble(tok));
        } catch (NumericException ignore) {
        }

        try {
            return new FloatConstant(node.position, Numbers.parseFloat(tok));
        } catch (NumericException ignore) {
        }

        // type constant for 'CAST' operation

        final int columnType = ColumnType.columnTypeOf(tok);

        if (columnType > -1) {
            return Constants.getTypeConstant(columnType);
        }

        throw SqlException.position(node.position).put("invalid constant: ").put(tok);
    }

    private Function createCursorFunction(ExpressionNode node) throws SqlException {
        assert node.queryModel != null;
        return new CursorFunction(node.position, sqlCodeGenerator.generate(node.queryModel, sqlExecutionContext));
    }

    private Function createFunction(
            ExpressionNode node,
            @Transient ObjList<Function> args
    ) throws SqlException {
        final ObjList<FunctionFactoryDescriptor> overload = functionFactoryCache.getOverloadList(node.token);
        boolean isNegated = functionFactoryCache.isNegated(node.token);
        boolean isFlipped = functionFactoryCache.isFlipped(node.token);
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
            int sigArgCount = descriptor.getSigCount();

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
                return checkAndCreateFunction(factory, args, node.position, configuration, isNegated, isFlipped);
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
                        candidateDescriptor = descriptor;
                        match = MATCH_NO_MATCH; // no match
                        break;
                    }

                    final boolean isArray = FunctionFactoryDescriptor.isArray(sigArgTypeMask);
                    final boolean isScalar = arg instanceof ScalarFunction;
                    if ((isArray && isScalar) || (!isArray && !isScalar)) {
                        candidateDescriptor = descriptor;
                        match = MATCH_NO_MATCH; // no match
                        break;
                    }

                    final int sigArgType = FunctionFactoryDescriptor.toType(sigArgTypeMask);

                    sigArgTypeSum += ColumnType.widthPrecedenceOf(sigArgType);

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
                    final boolean overloadPossible =
                            argType >= ColumnType.BYTE &&
                                    sigArgType >= ColumnType.BYTE &&
                                    sigArgType <= ColumnType.DOUBLE &&
                                    argType < sigArgType ||
                                    argType == ColumnType.DOUBLE &&
                                            arg.isConstant() &&
                                            Double.isNaN(arg.getDouble(null)) &&
                                            (sigArgType == ColumnType.LONG || sigArgType == ColumnType.INT) ||
                                    argType == ColumnType.CHAR &&
                                            sigArgType == ColumnType.STRING
                                    || undefined;


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
                        if (candidateSigArgTypeSum < sigArgTypeSum || bestMatch < match) {
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
                    throw SqlException.$(func.getPosition(), "constant expected");
                }
            }
        }

        // substitute NaNs with appropriate types
        for (int k = 0; k < candidateSigArgCount; k++) {
            final Function arg = args.getQuick(k);
            final int sigArgType = FunctionFactoryDescriptor.toType(candidateDescriptor.getArgTypeMask(k));
            if (arg.getType() == ColumnType.DOUBLE && arg.isConstant() && Double.isNaN(arg.getDouble(null))) {
                if (sigArgType == ColumnType.LONG) {
                    args.setQuick(k, new LongConstant(arg.getPosition(), Numbers.LONG_NaN));
                } else if (sigArgType == ColumnType.INT) {
                    args.setQuick(k, new IntConstant(arg.getPosition(), Numbers.INT_NaN));
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

        LOG.debug().$("call ").$(node).$(" -> ").$(candidate.getSignature()).$();
        return checkAndCreateFunction(candidate, args, node.position, configuration, isNegated, isFlipped);
    }

    private Function functionToConstant(Function function) {
        final int position = function.getPosition();
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
            case ColumnType.CHAR:
                if (function instanceof CharConstant) {
                    return function;
                } else {
                    return new CharConstant(position, function.getChar(null));
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
            case ColumnType.LONG256:
                if (function instanceof Long256Constant) {
                    return function;
                } else {
                    return new Long256Constant(position, function.getLong256A(null));
                }
            case ColumnType.DATE:
                if (function instanceof DateConstant) {
                    return function;
                } else {
                    return new DateConstant(position, function.getDate(null));
                }
            case ColumnType.STRING:
                if (function instanceof StrConstant || function instanceof NullStrConstant) {
                    return function;
                } else {
                    final CharSequence value = function.getStr(null);
                    if (value == null) {
                        return new NullStrConstant(position);
                    }
                    return new StrConstant(position, value);
                }
            case ColumnType.SYMBOL:
                if (function instanceof SymbolConstant) {
                    return function;
                }
                return new SymbolConstant(position, Chars.toString(function.getSymbol(null)), 0);
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

    @NotNull
    private BindVariableService getBindVariableService() throws SqlException {
        final BindVariableService bindVariableService = sqlExecutionContext.getBindVariableService();
        if (bindVariableService == null) {
            throw SqlException.$(0, "bind variable service is not provided");
        }
        return bindVariableService;
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

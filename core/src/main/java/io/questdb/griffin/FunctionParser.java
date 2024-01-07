/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.bind.IndexedParameterLinkFunction;
import io.questdb.griffin.engine.functions.bind.NamedParameterLinkFunction;
import io.questdb.griffin.engine.functions.cast.*;
import io.questdb.griffin.engine.functions.columns.*;
import io.questdb.griffin.engine.functions.constants.*;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;

import static io.questdb.griffin.SqlKeywords.isNullKeyword;
import static io.questdb.griffin.SqlKeywords.startsWithGeoHashKeyword;

public class FunctionParser implements PostOrderTreeTraversalAlgo.Visitor, Mutable {
    private static final Log LOG = LogFactory.getLog(FunctionParser.class);
    private static final int MATCH_EXACT_MATCH = 3;
    private static final int MATCH_FUZZY_MATCH = 1;
    // order of values matters here, partial match must have greater value than fuzzy match
    private static final int MATCH_NO_MATCH = 0;
    private static final int MATCH_PARTIAL_MATCH = 2;
    private final CairoConfiguration configuration;
    private final FunctionFactoryCache functionFactoryCache;
    private final ArrayDeque<Function> functionStack = new ArrayDeque<>();
    private final Long256Impl long256Sink = new Long256Impl();
    private final ArrayDeque<RecordMetadata> metadataStack = new ArrayDeque<>();
    private final IntList mutableArgPositions = new IntList();
    private final ObjList<Function> mutableArgs = new ObjList<>();
    private final IntStack positionStack = new IntStack();
    private final PostOrderTreeTraversalAlgo traverseAlgo = new PostOrderTreeTraversalAlgo();
    private final IntList undefinedVariables = new IntList();
    private RecordMetadata metadata;
    private SqlCodeGenerator sqlCodeGenerator;
    private SqlExecutionContext sqlExecutionContext;

    public FunctionParser(CairoConfiguration configuration, FunctionFactoryCache functionFactoryCache) {
        this.configuration = configuration;
        this.functionFactoryCache = functionFactoryCache;
    }

    @NotNull
    public static ScalarFunction createColumn(
            int position,
            CharSequence name,
            RecordMetadata metadata
    ) throws SqlException {
        final int index = metadata.getColumnIndexQuiet(name);

        if (index == -1) {
            throw SqlException.invalidColumn(position, name);
        }

        int columnType = metadata.getColumnType(index);
        switch (ColumnType.tagOf(columnType)) {
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
            case ColumnType.GEOBYTE:
                return GeoByteColumn.newInstance(index, columnType);
            case ColumnType.GEOSHORT:
                return GeoShortColumn.newInstance(index, columnType);
            case ColumnType.GEOINT:
                return GeoIntColumn.newInstance(index, columnType);
            case ColumnType.GEOLONG:
                return GeoLongColumn.newInstance(index, columnType);
            case ColumnType.NULL:
                return NullConstant.NULL;
            case ColumnType.LONG256:
                return Long256Column.newInstance(index);
            case ColumnType.LONG128:
                return Long128Column.newInstance(index);
            case ColumnType.UUID:
                return UuidColumn.newInstance(index);
            case ColumnType.IPv4:
                return IPv4Column.newInstance(index);
            default:
                throw SqlException.position(position)
                        .put("unsupported column type ")
                        .put(ColumnType.nameOf(columnType));
        }
    }

    @Override
    public void clear() {
        this.sqlExecutionContext = null;
    }

    public Function createBindVariable(SqlExecutionContext sqlExecutionContext, int position, CharSequence name, int expressionType) throws SqlException {
        this.sqlExecutionContext = sqlExecutionContext;
        if (name != null) {
            if (name.length() > 0) {
                if (expressionType != ExpressionNode.BIND_VARIABLE) {
                    return new StrConstant(name);
                }
                switch (name.charAt(0)) {
                    case ':':
                        return createNamedParameter(position, name);
                    case '$':
                        return parseIndexedParameter(position, name);
                    default:
                        return new StrConstant(name);
                }
            } else return StrConstant.EMPTY;
        }
        return NullConstant.NULL;
    }

    public Function createImplicitCast(int position, Function function, int toType) throws SqlException {
        Function cast = createImplicitCastOrNull(position, function, toType);
        if (cast != null && cast.isConstant()) {
            Function constant = functionToConstant(cast);
            // incoming function is now converted to a constant and can be closed here
            // since the returning constant will not use the function as underlying arg
            function.close();
            return constant;
        }
        // Do not close incoming function if cast is not a constant
        // it will be used inside the cast as an argument
        return cast;
    }

    public boolean findNoArgFunction(ExpressionNode node) {
        final ObjList<FunctionFactoryDescriptor> overload = functionFactoryCache.getOverloadList(node.token);
        if (overload != null) {
            for (int i = 0, n = overload.size(); i < n; i++) {
                if (overload.getQuick(i).getSigArgCount() == 0) {
                    return true;
                }
            }
        }
        return false;
    }

    public FunctionFactoryCache getFunctionFactoryCache() {
        return functionFactoryCache;
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
            try {
                traverseAlgo.traverse(node, this);
            } catch (Exception e) {
                // release parsed functions
                for (int i = functionStack.size(); i > 0; i--) {
                    Misc.free(functionStack.poll());
                }
                positionStack.clear();
                throw e;
            }

            final Function function = functionStack.poll();
            positionStack.pop();
            assert positionStack.size() == functionStack.size();
            if (function != null && function.isConstant() && (function instanceof ScalarFunction)) {
                return functionToConstant(function);
            }
            return function;
        } finally {
            if (metadataStack.isEmpty()) {
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
                    functionStack.push(createColumn(node.position, node.token, metadata));
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
                final Function arg = functionStack.poll();
                final int pos = positionStack.pop();

                mutableArgs.setQuick(n, arg);
                mutableArgPositions.setQuick(n, pos);

                if (arg instanceof GroupByFunction) {
                    Misc.freeObjList(mutableArgs);
                    throw SqlException.position(pos).put("Aggregate function cannot be passed as an argument");
                }
            }
            functionStack.push(createFunction(node, mutableArgs, mutableArgPositions));
        }
        positionStack.push(node.position);
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
        Misc.freeObjList(args);
        return ex;
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
        Misc.freeObjList(args);
        return ex;
    }

    private static long parseDate(CharSequence str, int position) throws SqlException {
        try {
            return DateFormatUtils.parseDate(str);
        } catch (NumericException e) {
            throw SqlException.invalidDate(str, position);
        }
    }

    private Function checkAndCreateFunction(
            FunctionFactory factory,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            @Transient ExpressionNode node,
            CairoConfiguration configuration
    ) throws SqlException {
        final int position = node.position;
        Function function;
        try {
            LOG.debug().$("call ").$(node).$(" -> ").$(factory.getSignature()).$();
            function = factory.newInstance(position, args, argPositions, configuration, sqlExecutionContext);
        } catch (SqlException | ImplicitCastException e) {
            Misc.freeObjList(args);
            throw e;
        } catch (Throwable e) {
            LOG.error().$("exception in function factory: ").$(e).$();
            Misc.freeObjList(args);
            throw SqlException.position(position).put("exception in function factory");
        }

        if (function == null) {
            LOG.error().$("NULL function")
                    .$(" [signature=").$(factory.getSignature())
                    .$(", class=").$(factory.getClass().getName()).$(']')
                    .$();
            Misc.freeObjList(args);
            throw SqlException.position(position).put("bad function factory (NULL), check log");
        }
        return function;
    }

    private Function createBindVariable0(int position, CharSequence name) throws SqlException {
        if (name.charAt(0) != ':') {
            return parseIndexedParameter(position, name);
        }
        return createNamedParameter(position, name);
    }

    private Function createConstant(int position, final CharSequence tok) throws SqlException {
        final int len = tok.length();

        if (isNullKeyword(tok)) {
            return NullConstant.NULL;
        }

        if (Chars.isQuoted(tok)) {
            switch (len) {
                case 3: // this is 'x' - char
                    return CharConstant.newInstance(tok.charAt(1));
                case 2: // this is '' - char
                    return StrConstant.EMPTY;
                default:
                    return new StrConstant(tok);
            }
        }

        // special case E'str' - we treat it like normal string for now
        if (len > 2 && tok.charAt(0) == 'E' && tok.charAt(1) == '\'' && tok.charAt(len - 1) == '\'') {
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

        final short columnType = ColumnType.tagOf(tok);

        if (
                (columnType >= ColumnType.BOOLEAN && columnType <= ColumnType.BINARY)
                        || columnType == ColumnType.REGCLASS
                        || columnType == ColumnType.REGPROCEDURE
                        || columnType == ColumnType.ARRAY_STRING
                        || columnType == ColumnType.UUID
                        || columnType == ColumnType.IPv4
        ) {
            return Constants.getTypeConstant(columnType);
        }

        // geohash type constant

        if (startsWithGeoHashKeyword(tok)) {
            return GeoHashTypeConstant.getInstanceByPrecision(
                    GeoHashUtil.parseGeoHashBits(position, 7, tok));
        }

        if (len > 1 && tok.charAt(0) == '#') {
            ConstantFunction geoConstant = GeoHashUtil.parseGeoHashConstant(position, tok, len);
            if (geoConstant != null) {
                return geoConstant;
            }
        }

        // long256
        if (Numbers.extractLong256(tok, len, long256Sink)) {
            return new Long256Constant(long256Sink); // values are copied from this sink
        }

        throw SqlException.position(position).put("invalid constant: ").put(tok);
    }

    private Function createCursorFunction(ExpressionNode node) throws SqlException {
        assert node.queryModel != null;
        // Disable async offload for in (select ...) sub-queries to avoid infinite loops
        // due to nested reduce calls. See SqlCodeGenerator#testBug484() for the reproducer.
        boolean currentFilterEnabled = sqlExecutionContext.isParallelFilterEnabled();
        sqlExecutionContext.setParallelFilterEnabled(false);
        // Make sure to override timestamp required flag from base query.
        sqlExecutionContext.pushTimestampRequiredFlag(false);
        try {
            return new CursorFunction(sqlCodeGenerator.generate(node.queryModel, sqlExecutionContext));
        } finally {
            sqlExecutionContext.setParallelFilterEnabled(currentFilterEnabled);
            sqlExecutionContext.popTimestampRequiredFlag();
        }
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

        final int argCount = args == null ? 0 : args.size();
        FunctionFactory candidate = null;
        FunctionFactoryDescriptor candidateDescriptor = null;
        boolean candidateSigVarArgConst = false;
        boolean candidateSigVarArg = true;
        int candidateSigArgCount = 0;
        int candidateSigArgTypeScore = -1;
        int bestMatch = MATCH_NO_MATCH;
        boolean isWindowContext = !sqlExecutionContext.getWindowContext().isEmpty();

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

            // this is no-arg function, match right away
            if (argCount == 0 && sigArgCount == 0) {
                return checkAndCreateFunction(factory, args, argPositions, node, configuration);
            }

            if (candidateDescriptor == null) {
                candidateDescriptor = descriptor;
            }

            // otherwise, is number of arguments the same?
            if (sigArgCount == argCount || (sigVarArg && argCount >= sigArgCount)) {
                int match = sigArgCount == 0 ? MATCH_EXACT_MATCH : MATCH_NO_MATCH;
                int sigArgTypeScore = 0;
                for (int argIdx = 0; argIdx < sigArgCount; argIdx++) {
                    final Function arg = args.getQuick(argIdx);
                    final int sigArgTypeMask = descriptor.getArgTypeMask(argIdx);

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

                    final short sigArgType = FunctionFactoryDescriptor.toType(sigArgTypeMask);
                    final int argType = arg.getType();
                    final short argTypeTag = ColumnType.tagOf(argType);
                    final short sigArgTypeTag = ColumnType.tagOf(sigArgType);

                    if (sigArgTypeTag == argTypeTag ||
                            (argTypeTag == ColumnType.CHAR &&              // 'a' could also be a string literal, so it should count as proper match
                                    sigArgTypeTag == ColumnType.STRING &&  // for both string and char, otherwise ? > 'a' matches char function even though    
                                    arg.isConstant() &&                    // bind variable parameter might be a string and throw error during execution.     
                                    arg != CharTypeConstant.INSTANCE) ||   // Ignore type constant to keep cast(X as char) working     
                            (sigArgTypeTag == ColumnType.GEOHASH && ColumnType.isGeoHash(argType))) {
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

                    boolean overloadPossible = false;
                    // we do not want to use any overload when checking the output of a cast() function.
                    // the output must be the exact type as specified by a user. that's the whole point of casting. 
                    // for all other functions, else, we want to explore possible casting opportunities
                    //
                    // output of a cast() function is always the 2nd argument in a function signature
                    if (argIdx != 1 || !Chars.equals("cast", node.token)) {
                        int overloadDistance = ColumnType.overloadDistance(argTypeTag, sigArgType); // NULL to any is 0

                        if (argTypeTag == ColumnType.STRING &&
                                sigArgTypeTag == ColumnType.CHAR) {
                            if (arg.isConstant()) {
                                // string longer than 1 char can't be cast to char implicitly
                                if (arg.getStrLen(null) > 1) {
                                    overloadDistance = ColumnType.OVERLOAD_NONE;
                                }
                            } else {
                                // prefer CHAR -> STRING to STRING -> CHAR conversion
                                overloadDistance = 2 * overloadDistance;
                            }
                        }

                        sigArgTypeScore += overloadDistance;
                        // Overload with cast to higher precision
                        overloadPossible = overloadDistance != ColumnType.OVERLOAD_NONE;

                        // Overload when arg is double NaN to func which accepts INT, LONG
                        overloadPossible |= argTypeTag == ColumnType.DOUBLE &&
                                arg.isConstant() &&
                                Double.isNaN(arg.getDouble(null)) &&
                                (sigArgTypeTag == ColumnType.LONG || sigArgTypeTag == ColumnType.INT);

                        // Implicit cast from CHAR to STRING
                        overloadPossible |= argTypeTag == ColumnType.CHAR &&
                                sigArgTypeTag == ColumnType.STRING;

                        // Implicit cast from STRING to TIMESTAMP
                        overloadPossible |= argTypeTag == ColumnType.STRING && arg.isConstant() &&
                                sigArgTypeTag == ColumnType.TIMESTAMP && !factory.isGroupBy();

                        // Implicit cast from STRING to DATE
                        overloadPossible |= argTypeTag == ColumnType.STRING && arg.isConstant() &&
                                sigArgTypeTag == ColumnType.DATE && !factory.isGroupBy();

                        // Implicit cast from STRING to GEOHASH
                        overloadPossible |= argTypeTag == ColumnType.STRING &&
                                sigArgTypeTag == ColumnType.GEOHASH && !factory.isGroupBy();

                        // Implicit cast from SYMBOL to TIMESTAMP
                        overloadPossible |= argTypeTag == ColumnType.SYMBOL && arg.isConstant() &&
                                sigArgTypeTag == ColumnType.TIMESTAMP && !factory.isGroupBy();

                        overloadPossible |= arg.isUndefined();
                    }
                    // can we use overload mechanism?
                    if (overloadPossible) {
                        switch (match) {
                            case MATCH_NO_MATCH: // no match?
                                if (argTypeTag == ColumnType.NULL) {
                                    match = MATCH_PARTIAL_MATCH;
                                } else {
                                    match = MATCH_FUZZY_MATCH; // upgrade to fuzzy match
                                }
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

                if (factory.isWindow()) {
                    // prefer window functions in window context, otherwise non-window functions
                    if (isWindowContext) {
                        // choose window-ed avg(D) over group by implementation that matches arg type better
                        match = MATCH_EXACT_MATCH;
                        sigArgTypeScore -= 10;
                    } else {
                        sigArgTypeScore += 10;
                    }
                }

                if (match == MATCH_EXACT_MATCH || match >= bestMatch) {
                    // exact match may be?
                    // special case - if signature enforces constant vararg we
                    // have to ensure all args are indeed constant

                    // when match is the same, prefer non-var-arg functions
                    if (match == bestMatch && sigVarArg && !candidateSigVarArg) {
                        continue;
                    }

                    if (match != MATCH_EXACT_MATCH) {
                        if (candidateSigArgTypeScore > sigArgTypeScore || bestMatch < match) {
                            candidate = factory;
                            candidateDescriptor = descriptor;
                            candidateSigArgCount = sigArgCount;
                            candidateSigVarArg = sigVarArg;
                            candidateSigVarArgConst = sigVarArgConst;
                            candidateSigArgTypeScore = sigArgTypeScore;
                        }
                        bestMatch = match;
                    } else {
                        candidate = factory;
                        candidateDescriptor = descriptor;
                        candidateSigArgCount = sigArgCount;
                        candidateSigVarArg = sigVarArg;
                        candidateSigVarArgConst = sigVarArgConst;
                        bestMatch = match;
                        if (isWindowContext == factory.isWindow()) {
                            break;
                        }
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
                if (!(func.isConstant() || func.isRuntimeConstant())) {
                    Misc.freeObjList(args);
                    throw SqlException.$(argPositions.getQuick(k), "constant expected");
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

        for (int k = 0; k < candidateSigArgCount; k++) {
            final Function arg = args.getQuick(k);
            final short sigArgTypeTag = FunctionFactoryDescriptor.toType(candidateDescriptor.getArgTypeMask(k));
            final short argTypeTag = ColumnType.tagOf(arg.getType());

            if (argTypeTag == ColumnType.DOUBLE && arg.isConstant() && Double.isNaN(arg.getDouble(null))) {
                // substitute NaNs with appropriate types
                if (sigArgTypeTag == ColumnType.LONG) {
                    args.setQuick(k, LongConstant.NULL);
                } else if (sigArgTypeTag == ColumnType.INT) {
                    args.setQuick(k, IntConstant.NULL);
                }
            } else if ((argTypeTag == ColumnType.STRING || argTypeTag == ColumnType.SYMBOL) && arg.isConstant()) {
                if (sigArgTypeTag == ColumnType.TIMESTAMP) {
                    int position = argPositions.getQuick(k);
                    long timestamp = parseTimestamp(arg.getStr(null), position);
                    args.set(k, TimestampConstant.newInstance(timestamp));
                } else if (sigArgTypeTag == ColumnType.DATE) {
                    int position = argPositions.getQuick(k);
                    long millis = parseDate(arg.getStr(null), position);
                    args.set(k, DateConstant.newInstance(millis));
                }
            } else if (argTypeTag == ColumnType.UUID && sigArgTypeTag == ColumnType.STRING) {
                args.setQuick(k, new CastUuidToStrFunctionFactory.Func(arg));
            }
        }
        return checkAndCreateFunction(candidate, args, argPositions, node, configuration);
    }

    @Nullable
    private Function createImplicitCastOrNull(int position, Function function, int toType) throws SqlException {
        int fromType = function.getType();
        switch (fromType) {
            case ColumnType.STRING:
            case ColumnType.SYMBOL:
                if (toType == ColumnType.UUID) {
                    return new CastStrToUuidFunctionFactory.Func(function);
                }
                if (toType == ColumnType.TIMESTAMP) {
                    return new CastStrToTimestampFunctionFactory.Func(function);
                }
                if (ColumnType.isGeoHash(toType)) {
                    return CastStrToGeoHashFunctionFactory.newInstance(position, toType, function);
                }
                break;
            case ColumnType.UUID:
                if (toType == ColumnType.STRING) {
                    return new CastUuidToStrFunctionFactory.Func(function);
                }
                break;
            case ColumnType.CHAR:
                if (toType == ColumnType.SYMBOL) {
                    return new CastCharToSymbolFunctionFactory.Func(function);
                }
                break;
            default:
                if (ColumnType.isGeoHash(fromType)) {
                    int fromGeoBits = ColumnType.getGeoHashBits(fromType);
                    int toGeoBits = ColumnType.getGeoHashBits(toType);
                    if (ColumnType.isGeoHash(toType) && toGeoBits < fromGeoBits) {
                        return CastGeoHashToGeoHashFunctionFactory.newInstance(position, function, toType, fromType);
                    }
                }
                break;
        }
        return null;
    }

    private Function createIndexParameter(int variableIndex, int position) throws SqlException {
        Function function = getBindVariableService().getFunction(variableIndex);
        if (function == null) {
            // bind variable is undefined
            return new IndexedParameterLinkFunction(variableIndex, ColumnType.UNDEFINED, position);
        }
        return new IndexedParameterLinkFunction(variableIndex, function.getType(), position);
    }

    private Function createNamedParameter(int position, CharSequence name) throws SqlException {
        Function function = getBindVariableService().getFunction(name);
        if (function == null) {
            throw SqlException.position(position).put("undefined bind variable: ").put(name);
        }
        return new NamedParameterLinkFunction(Chars.toString(name), function.getType());
    }

    private Function functionToConstant(Function function) {
        Function newFunction = functionToConstant0(function);
        // Sometimes functionToConstant0 returns same instance as passed in parameter
        if (newFunction != function) {
            // and we want to close underlying function only in case it's different form returned newFunction
            function.close();
        }
        return newFunction;
    }

    private Function functionToConstant0(Function function) {
        int type = function.getType();
        switch (ColumnType.tagOf(type)) {
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
            case ColumnType.GEOBYTE:
                if (function instanceof GeoByteConstant) {
                    return function;
                } else {
                    return new GeoByteConstant(function.getGeoByte(null), type);
                }
            case ColumnType.GEOSHORT:
                if (function instanceof GeoShortConstant) {
                    return function;
                } else {
                    return new GeoShortConstant(function.getGeoShort(null), type);
                }
            case ColumnType.GEOINT:
                if (function instanceof GeoIntConstant) {
                    return function;
                } else {
                    return new GeoIntConstant(function.getGeoInt(null), type);
                }
            case ColumnType.GEOLONG:
                if (function instanceof GeoLongConstant) {
                    return function;
                } else {
                    return new GeoLongConstant(function.getGeoLong(null), type);
                }
            case ColumnType.DATE:
                if (function instanceof DateConstant) {
                    return function;
                } else {
                    return DateConstant.newInstance(function.getDate(null));
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
            case ColumnType.UUID:
                if (function instanceof UuidConstant) {
                    return function;
                } else {
                    return new UuidConstant(function.getLong128Lo(null), function.getLong128Hi(null));
                }
            case ColumnType.IPv4:
                if (function instanceof IPv4Constant) {
                    return function;
                } else {
                    return IPv4Constant.newInstance(function.getIPv4(null));
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

    private long parseTimestamp(CharSequence str, int position) throws SqlException {
        try {
            return IntervalUtils.parseFloorPartialTimestamp(str);
        } catch (NumericException e) {
            throw SqlException.invalidDate(str, position);
        }
    }

    static {
        for (int i = 0, n = SqlCompilerImpl.sqlControlSymbols.size(); i < n; i++) {
            FunctionFactoryCache.invalidFunctionNames.add(SqlCompilerImpl.sqlControlSymbols.getQuick(i));
        }
        FunctionFactoryCache.invalidFunctionNameChars.add(' ');
        FunctionFactoryCache.invalidFunctionNameChars.add('\"');
        FunctionFactoryCache.invalidFunctionNameChars.add('\'');
    }
}

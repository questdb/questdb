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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.functions.bind.BindVariableService;
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

public class FunctionParser implements PostOrderTreeTraversalAlgo.Visitor {
    private static final Log LOG = LogFactory.getLog(FunctionParser.class);

    // order of values matters here, partial match must have greater value than fuzzy match
    private static final int MATCH_NO_MATCH = 0;
    private static final int MATCH_FUZZY_MATCH = 1;
    private static final int MATCH_PARTIAL_MATCH = 2;
    private static final int MATCH_EXACT_MATCH = 3;

    private static final IntHashSet invalidFunctionNameChars = new IntHashSet();
    private static final CharSequenceHashSet invalidFunctionNames = new CharSequenceHashSet();

    static {
        for (int i = 0, n = SqlCompiler.sqlControlSymbols.size(); i < n; i++) {
            invalidFunctionNames.add(SqlCompiler.sqlControlSymbols.getQuick(i));
        }

//        invalidFunctionNameChars.add('.');
        invalidFunctionNameChars.add(' ');
        invalidFunctionNameChars.add('\"');
        invalidFunctionNameChars.add('\'');
    }

    private final ObjList<Function> mutableArgs = new ObjList<>();
    private final ArrayDeque<Function> stack = new ArrayDeque<>();
    private final PostOrderTreeTraversalAlgo traverseAlgo = new PostOrderTreeTraversalAlgo();
    private final CairoConfiguration configuration;
    private final CharSequenceObjHashMap<ObjList<FunctionFactory>> factories = new CharSequenceObjHashMap<>();
    private final CharSequenceHashSet groupByFunctionNames = new CharSequenceHashSet();
    private final ArrayDeque<RecordMetadata> metadataStack = new ArrayDeque<>();
    private RecordMetadata metadata;
    private SqlCodeGenerator sqlCodeGenerator;
    private SqlExecutionContext sqlExecutionContext;

    public FunctionParser(CairoConfiguration configuration, Iterable<FunctionFactory> functionFactories) {
        this.configuration = configuration;
        loadFunctionFactories(functionFactories);
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
            case 'A':
                sigArgType = ColumnType.CHAR;
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
                sigArgType = TypeEx.VAR_ARG;
                break;
            case 'C':
                sigArgType = TypeEx.CURSOR;
                break;
            case 'H':
                sigArgType = ColumnType.LONG256;
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

        if (invalidFunctionNames.keyIndex(sig, 0, openBraceIndex) < 0) {
            throw SqlException.position(0).put("invalid function name character: ").put(sig);
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

    private static SqlException invalidFunction(CharSequence message, ExpressionNode node, ObjList<Function> args) {
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

    public Function createIndexParameter(int variableIndex, ExpressionNode node) throws SqlException {
        Function function = getBindVariableService().getFunction(variableIndex);
        if (function == null) {
            throw SqlException.position(node.position).put("no bind variable defined at index ").put(variableIndex);
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

    @NotNull
    private BindVariableService getBindVariableService() throws SqlException {
        final BindVariableService bindVariableService = sqlExecutionContext.getBindVariableService();
        if (bindVariableService == null) {
            throw SqlException.$(0, "bind variable service is not provided");
        }
        return bindVariableService;
    }

    public boolean isGroupBy(CharSequence name) {
        return groupByFunctionNames.contains(name);
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
            return stack.poll();
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
                    if (Chars.startsWith(node.token, ':')) {
                        stack.push(createNamedParameter(node));
                    } else if (Chars.startsWith(node.token, '$')) {
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
                    } else {
                        // lookup column
                        stack.push(createColumn(node));
                    }
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

    private Function checkAndCreateFunction(
            FunctionFactory factory,
            @Transient ObjList<Function> args,
            int position,
            CairoConfiguration configuration
    ) throws SqlException {
        Function function;
        try {
            function = factory.newInstance(args, position, configuration);
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

        if (function.isConstant()) {
            return functionToConstant(position, function);
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
            default:
                return new Long256Column(node.position, index);
        }
    }

    private Function createConstant(ExpressionNode node) throws SqlException {

        if (Chars.equalsLowerCaseAscii(node.token, "null")) {
            return new NullStrConstant(node.position);
        }

        if (Chars.isQuoted(node.token)) {
            if (node.token.length() == 3) {
                // this is 'x' - char
                return new CharConstant(node.position, node.token.charAt(1));
            }

            if (node.token.length() == 2) {
                // empty
                return new CharConstant(node.position, (char) 0);
            }
            return new StrConstant(node.position, node.token);
        }

        if (Chars.equalsLowerCaseAscii(node.token, "true")) {
            return new BooleanConstant(node.position, true);
        }

        if (Chars.equalsLowerCaseAscii(node.token, "false")) {
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

        try {
            return new FloatConstant(node.position, Numbers.parseFloat(node.token));
        } catch (NumericException ignore) {
        }

        // type constant for 'CAST' operation

        final int columnType = ColumnType.columnTypeOf(node.token);

        if (columnType > -1) {
            return Constants.getTypeConstant(columnType);
        }

        throw SqlException.position(node.position).put("invalid constant: ").put(node.token);
    }

    private Function createCursorFunction(ExpressionNode node) throws SqlException {
        assert node.queryModel != null;
        return new CursorFunction(node.position, sqlCodeGenerator.generate(node.queryModel, sqlExecutionContext)
        );
    }

    private Function createFunction(
            ExpressionNode node,
            @Transient ObjList<Function> args
    ) throws SqlException {
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
        String candidateSignature = null;
        boolean candidateSigVarArgConst = false;
        int candidateSigArgCount = 0;

        int fuzzyMatchCount = 0;
        int bestMatch = MATCH_NO_MATCH;

        for (int i = 0, n = overload.size(); i < n; i++) {
            final FunctionFactory factory = overload.getQuick(i);
            final String signature = factory.getSignature();
            final int sigArgOffset = signature.indexOf('(') + 1;
            int sigArgCount = signature.length() - 1 - sigArgOffset;

            final boolean sigVarArg;
            final boolean sigVarArgConst;

            if (sigArgCount > 0) {
                char c = signature.charAt(sigArgOffset + sigArgCount - 1);
                sigVarArg = getArgType(Character.toUpperCase(c)) == TypeEx.VAR_ARG;
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
                int match = MATCH_NO_MATCH; // no match
                if (sigArgCount == 0) {
                    match = MATCH_EXACT_MATCH;
                }

                for (int k = 0; k < sigArgCount; k++) {
                    final Function arg = args.getQuick(k);
                    final char c = signature.charAt(sigArgOffset + k);

                    if (Character.isLowerCase(c) && !arg.isConstant()) {
                        match = MATCH_NO_MATCH; // no match
                        break;
                    }

                    final int sigArgType = getArgType(c);

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

                    final boolean overloadPossible = (
                            (arg.getType() >= ColumnType.BYTE)
                                    && (arg.getType() <= ColumnType.DOUBLE)
                                    && (sigArgType >= ColumnType.BYTE)
                                    && (sigArgType <= ColumnType.DOUBLE)
                                    && (arg.getType() < sigArgType)
                    ) || ((
                            (arg.getType() == ColumnType.DOUBLE)
                                    && arg.isConstant()
                                    && Double.isNaN(arg.getDouble(null))
                                    && ((sigArgType == ColumnType.LONG) || (sigArgType == ColumnType.INT))
                    ) && (!(arg instanceof TypeConstant) || (arg.getType() != sigArgType)));

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

                    candidate = factory;
                    candidateSignature = signature;
                    candidateSigArgCount = sigArgCount;
                    candidateSigVarArgConst = sigVarArgConst;

                    if (match != MATCH_EXACT_MATCH) {
                        if (match == MATCH_FUZZY_MATCH) {
                            fuzzyMatchCount++;
                        } else {
                            fuzzyMatchCount = 0;
                        }
                        bestMatch = match;
                    } else {
                        fuzzyMatchCount = 0;
                        break;
                    }
                }
            }
        }

        if (fuzzyMatchCount > 1) {
            // ambiguous invocation target
            throw invalidFunction("ambiguous function call", node, args);
        }

        if (candidate == null) {
            // no signature match
            throw invalidFunction("unknown function", node, args);
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
        final int sigArgOffset = candidateSignature.indexOf('(') + 1;
        for (int k = 0; k < candidateSigArgCount; k++) {
            final Function arg = args.getQuick(k);
            final char c = candidateSignature.charAt(sigArgOffset + k);

            final int sigArgType = getArgType(c);
            if (arg.getType() == ColumnType.DOUBLE && arg.isConstant() && Double.isNaN(arg.getDouble(null))) {
                if (sigArgType == ColumnType.LONG) {
                    args.setQuick(k, new LongConstant(arg.getPosition(), Numbers.LONG_NaN));
                } else if (sigArgType == ColumnType.INT) {
                    args.setQuick(k, new IntConstant(arg.getPosition(), Numbers.INT_NaN));
                }
            }
        }

        LOG.info().$("call ").$(node).$(" -> ").$(candidate.getSignature()).$();
        return checkAndCreateFunction(candidate, args, node.position, configuration);
    }

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
                overload = factories.valueAtQuick(index);
            } else {
                overload = new ObjList<>(4);
                factories.putAt(index, name, overload);
            }
            overload.add(factory);

            if (factory.isGroupBy()) {
                groupByFunctionNames.add(name);
            }
//            LOG.info().$("func: ").$(factory.getSignature()).$();
        }
    }
}

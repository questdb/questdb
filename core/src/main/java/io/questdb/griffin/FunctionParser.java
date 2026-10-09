/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.IntList;
import io.questdb.std.IntStack;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

import java.util.ArrayDeque;

/**
 * Builds the function tree of an expression eagerly: walks the AST in post-order and resolves and constructs each
 * node through its {@link FunctionResolver}.
 */
public class FunctionParser implements PostOrderTreeTraversalAlgo.Visitor, Mutable {
    private final CairoConfiguration configuration;
    private final ArrayDeque<Function> functionStack = new ArrayDeque<>();
    private final ArrayDeque<RecordMetadata> metadataStack = new ArrayDeque<>();
    private final IntList mutableArgPositions = new IntList();
    private final ObjList<Function> mutableArgs = new ObjList<>();
    private final IntStack positionStack = new IntStack();
    private final FunctionResolver resolver;
    private final PostOrderTreeTraversalAlgo traverseAlgo = new PostOrderTreeTraversalAlgo();
    private RecordMetadata metadata;
    private SqlExecutionContext sqlExecutionContext;
    private SubqueryCompiler subqueryCompiler;

    public FunctionParser(CairoConfiguration configuration, FunctionFactoryCache functionFactoryCache) {
        this(configuration, new FunctionResolver(configuration, functionFactoryCache));
    }

    public FunctionParser(CairoConfiguration configuration, FunctionResolver resolver) {
        this.configuration = configuration;
        this.resolver = resolver;
    }

    @Override
    public void clear() {
        resolver.clear();
        this.positionStack.clear();
        this.functionStack.clear();
        this.sqlExecutionContext = null;
    }

    public FunctionFactoryCache getFunctionFactoryCache() {
        return resolver.getFunctionFactoryCache();
    }

    /**
     * The resolver this parser constructs functions with; it also records the execution requirements and cursor
     * construction of every function built through it.
     */
    public FunctionResolver getFunctionResolver() {
        return resolver;
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
            if (node != null) {
                node.reassociateConstants(configuration.getCairoSqlLegacyOperatorPrecedence());
            }
            try {
                traverseAlgo.traverse(node, this);
            } catch (Exception e) {
                // Release parsed functions best-effort: keep closing the rest even if one close()
                // throws, and fold close failures into e as suppressed instead of masking it.
                for (int i = functionStack.size(); i > 0; i--) {
                    Misc.free(functionStack.poll(), e);
                }
                positionStack.clear();
                throw e;
            }

            Function function = functionStack.poll();
            positionStack.pop();
            assert positionStack.size() == functionStack.size();
            if (function != null && function.isConstant() && function.extendedOps() == null) {
                function = resolver.functionToConstant(function);
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

    @Override
    public void visit(ExpressionNode node) throws SqlException {
        int argCount = node.paramCount;
        if (argCount == 0) {
            switch (node.type) {
                case ExpressionNode.LITERAL:
                    functionStack.push(FunctionResolver.createColumn(node.position, node.token, metadata));
                    break;
                case ExpressionNode.BIND_VARIABLE:
                    functionStack.push(resolver.createBindVariable(node.position, node.token, sqlExecutionContext));
                    break;
                case ExpressionNode.MEMBER_ACCESS:
                    functionStack.push(new StrConstant(node.token));
                    break;
                case ExpressionNode.CONSTANT:
                    functionStack.push(resolver.createConstant(node.position, node.token, sqlExecutionContext));
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
                Function arg = functionStack.poll();
                final int pos = positionStack.pop();

                try {
                    if (arg != null && arg.isConstant() && arg.extendedOps() == null && !(arg instanceof TypeConstant)) {
                        arg = resolver.functionToConstant(arg);
                    }
                } catch (Throwable th) {
                    // these args were already popped from functionStack.
                    // Best-effort cleanup: fold any close() failure into th as suppressed instead
                    // of masking it, and keep closing later args even if one close() throws.
                    Misc.freeObjList(mutableArgs, th);
                    throw th;
                }

                mutableArgs.setQuick(n, arg);
                mutableArgPositions.setQuick(n, pos);
                FunctionResolver.rejectAggregateArgument(mutableArgs, n, pos);
            }
            FunctionResolver.wrapRuntimeConstants(mutableArgs);
            functionStack.push(createFunction(node, mutableArgs, mutableArgPositions));
        }
        positionStack.push(node.position);
    }

    private Function createCursorFunction(ExpressionNode node) throws SqlException {
        assert node.queryModel != null;
        if (subqueryCompiler == null) {
            throw SqlException.$(node.position, "sub-query is not supported in this context");
        }
        return new CursorFunction(subqueryCompiler.compileSubqueryFactory(node.queryModel, node.position, sqlExecutionContext));
    }

    private Function createFunction(
            ExpressionNode node,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions
    ) throws SqlException {
        final ExpressionNode literal = node.paramCount == 2 && node.lhs.type == ExpressionNode.CONSTANT ? node.lhs : null;
        while (true) {
            final Function cast = resolver.resolveCast(node.token, args, literal != null ? literal.token : null,
                    literal != null ? literal.position : 0, sqlExecutionContext);
            if (cast != null) {
                return cast;
            }
            final FunctionFactoryDescriptor overload = resolver.selectOverload(node.token, node.position, args, argPositions, sqlExecutionContext);
            if (overload != null) {
                resolver.coerceArguments(overload, node.position, node.token, args, argPositions, sqlExecutionContext);
                return resolver.createFunction(overload, node.position, node.token, args, argPositions, sqlExecutionContext);
            }
        }
    }

    /**
     * Installs the compiler that compiles the sub-queries met outside function binding while it binds a statement,
     * e.g. in table-function arguments, and returns the previous one for the caller to restore.
     */
    SubqueryCompiler swapSubqueryCompiler(SubqueryCompiler compiler) {
        final SubqueryCompiler previous = subqueryCompiler;
        subqueryCompiler = compiler;
        return previous;
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

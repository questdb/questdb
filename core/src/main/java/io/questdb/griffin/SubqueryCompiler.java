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
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.BoolList;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * The compile layer of one query level: owns the level's {@link SqlBinder} and the lifecycle of the sub-queries
 * the level binds. Each sub-query is a nested level with its own instance, pooled across statements up to
 * {@link #MAX_RETAINED_SUBQUERY_DEPTH} levels deep. A sub-query is completed (its own sub-queries completed, then
 * optimised, authorized and generated) once every level is bound, or early where binding consumes its rows
 * (PIVOT IN, table-function arguments) or its errors (arguments of a call that failed to resolve). The first
 * consumer takes the generated factory; later consumers regenerate one.
 */
final class SubqueryCompiler implements Closeable, Mutable {
    private static final Log LOG = LogFactory.getLog(SubqueryCompiler.class);
    private static final int MAX_RETAINED_SUBQUERY_DEPTH = 8;
    private final SqlBinder binder;
    private final SqlCompilerImpl compiler;
    private final CairoConfiguration configuration;
    private final FunctionParser functionParser;
    private final SubqueryMetadataFactory metadata = new SubqueryMetadataFactory();
    private final BoolList pendingSubqueries = new BoolList();
    private final ObjList<RecordCursorFactory> subqueryFactories = new ObjList<>();
    private final ObjList<SubqueryCompiler> subqueryLevels = new ObjList<>();
    private final IntList subqueryPositions = new IntList();
    private int depth;
    private int subqueryCount;

    SubqueryCompiler(CairoConfiguration configuration, FunctionParser functionParser, SqlCompilerImpl compiler) {
        this.compiler = compiler;
        this.configuration = configuration;
        this.functionParser = functionParser;
        this.binder = new SqlBinder(configuration, functionParser, compiler, this);
    }

    @Override
    public void clear() {
        binder.clear();
        final Throwable failure = clearSubqueries(null);
        if (failure != null) {
            LOG.error().$("could not free subquery resources [error=").$(failure).I$();
        }
    }

    @Override
    public void close() {
        clear();
        Misc.freeObjListAndClear(subqueryLevels);
    }

    private static boolean hasSchema(RecordMetadata metadata, OutputSchema output) {
        if (metadata.getColumnCount() != output.getColumnCount()) {
            return false;
        }
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            if (metadata.getColumnType(i) != output.getColumnType(i)) {
                return false;
            }
        }
        return true;
    }

    private Throwable clearSubqueries(Throwable primary) {
        for (int i = 0; i < subqueryCount; i++) {
            primary = Misc.freeBestEffort(primary, subqueryFactories.getQuick(i));
            try {
                subqueryLevels.getQuick(i).clear();
            } catch (Throwable th) {
                primary = Misc.foldCleanupFailure(primary, th);
            }
        }
        subqueryFactories.clear();
        pendingSubqueries.clear();
        subqueryPositions.clear();
        subqueryCount = 0;
        if (depth >= MAX_RETAINED_SUBQUERY_DEPTH - 1 && subqueryLevels.size() > 0) {
            // One deeply nested query must not pin a level per nesting level for the compiler lifetime.
            primary = Misc.freeObjListBestEffort(primary, subqueryLevels);
            subqueryLevels.clear();
        }
        return primary;
    }

    /**
     * Binds the level's model into an unoptimised plan, with this instance compiling the sub-queries the
     * function parser meets outside function binding, e.g. in table-function arguments.
     */
    void bind(QueryModel model, SqlParserCallback parserCallback, SqlExecutionContext executionContext) throws SqlException {
        clear();
        final SubqueryCompiler previous = functionParser.swapSubqueryCompiler(this);
        final LogicalPlan plan;
        try {
            plan = binder.bind(model, parserCallback, executionContext);
        } finally {
            functionParser.swapSubqueryCompiler(previous);
        }
        binder.setRoot(plan);
    }

    /**
     * Binds a standalone expression over the metadata, completes its sub-queries and instantiates it.
     */
    Function compileExpression(ExpressionNode expression, RecordMetadata metadata, int preferredType, SqlExecutionContext executionContext) throws SqlException {
        assert binder.getRoot() == null;
        if (expression == null) {
            return null;
        }
        final BindContext ctx = binder.ctx;
        ctx.tmpScope.clear();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            ctx.tmpScope.add(i, metadata.getColumnName(i), metadata.getColumnType(i), true);
            ctx.tmpScope.setSymbolTableStatic(i, metadata.isSymbolTableStatic(i));
        }
        Function function = null;
        try {
            final BoundExpression bound = ctx.functionBinder.bind(expression, ctx.tmpScope, null, preferredType, executionContext);
            completeSubqueries(executionContext);
            function = ctx.functionInstantiator.instantiate(bound, ctx.tmpScope, metadata, executionContext);
            // Only the executable closure escapes a standalone expression. Reuse
            // preparation storage per VALUES cell, not once per entire statement.
            ctx.clearExpressions();
            return function;
        } catch (Throwable th) {
            Misc.free(function, th);
            final Throwable failure = ctx.preparedFunctions.closePrepared(th);
            assert failure == th;
            throw th;
        } finally {
            ctx.tmpScope.clear();
        }
    }

    /**
     * Binds a sub-query of this level as a nested level and returns its index; the sub-query stays pending until
     * it is completed.
     */
    int compileSubquery(QueryModel model, int position, SqlExecutionContext executionContext) throws SqlException {
        if (subqueryLevels.size() == subqueryCount) {
            subqueryLevels.add(new SubqueryCompiler(configuration, functionParser, compiler));
        }
        final int index = subqueryCount++;
        subqueryFactories.extendAndSet(index, null);
        pendingSubqueries.extendAndSet(index, false);
        subqueryPositions.extendAndSet(index, position);
        final SubqueryCompiler subquery = subqueryLevels.getQuick(index);
        subquery.depth = depth + 1;
        final boolean isWindowContextPushed = !executionContext.getWindowContext().isEmpty();
        if (isWindowContextPushed) {
            executionContext.pushWindowContext();
        }
        try {
            subquery.bind(model, binder.getParserCallback(), executionContext);
        } catch (Throwable th) {
            binder.ctx.isSubqueryFailed = true;
            throw th;
        } finally {
            if (isWindowContextPushed) {
                executionContext.popWindowContext();
            }
        }
        subquery.metadata.of(subquery.binder.getRoot().getOutput());
        pendingSubqueries.set(index, true);
        return index;
    }

    /**
     * Completes, in bind order, the pending sub-queries among the arguments of a call that failed to resolve or
     * construct: a sub-query argument is generated before its call, so its errors precede the call's.
     */
    void completeArgumentSubqueries(ObjList<BoundExpression> arguments, SqlExecutionContext executionContext) throws SqlException {
        for (int i = arguments.size() - 1; i > -1; i--) {
            if (arguments.getQuick(i) instanceof CursorExpression cursor) {
                completeSubquery(cursor.getSubqueryIndex(), executionContext);
            }
        }
    }

    /**
     * Completes, in bind order, every sub-query bound but not complete yet. Runs once the statement is bound, so
     * binding errors of every level precede sub-query optimisation and generation errors, and before the level is
     * optimised, so its optimiser sees the stability the generated factories prove.
     */
    void completeSubqueries(SqlExecutionContext executionContext) throws SqlException {
        for (int i = 0; i < subqueryCount; i++) {
            completeSubquery(i, executionContext);
        }
    }

    /**
     * Completes the sub-query when it is bound only: completes its own sub-queries, optimises it, generates its
     * factory for the first consumer and hands its consumers the optimised plan and the factory's stability.
     */
    void completeSubquery(int index, SqlExecutionContext executionContext) throws SqlException {
        if (!pendingSubqueries.get(index)) {
            return;
        }
        pendingSubqueries.set(index, false);
        final SubqueryCompiler subquery = subqueryLevels.getQuick(index);
        final RecordCursorFactory factory;
        try {
            subquery.completeSubqueries(executionContext);
            compiler.optimisePlan(subquery.binder, executionContext);
            assert hasSchema(subquery.metadata.getMetadata(), subquery.binder.getRoot().getOutput()) : "optimised sub-query output differs from its bound output";
            factory = generateSubquery(index, executionContext);
        } catch (Throwable th) {
            binder.ctx.isSubqueryFailed = true;
            throw th;
        }
        subqueryFactories.setQuick(index, factory);
        binder.ctx.functionBinder.completeCursors(index, subquery.binder.getRoot(), factory.isStableWithinExecution());
    }

    /**
     * Frees resources a compilation left in flight when no failure is pending, and returns any
     * cleanup failure to the caller.
     */
    Throwable freeResourcesInFlight() {
        return clearSubqueries(binder.closePrepared(null));
    }

    /**
     * Frees resources a failed compilation left in flight; cleanup failures become suppressed
     * exceptions of the primary.
     */
    void freeResourcesInFlight(@NotNull Throwable primary) {
        final Throwable failure = clearSubqueries(binder.closePrepared(primary));
        assert failure == primary;
    }

    /**
     * Generates a new, owned factory of the sub-query.
     */
    RecordCursorFactory generateSubquery(int index, SqlExecutionContext executionContext) throws SqlException {
        final SqlBinder subquery = subqueryLevels.getQuick(index).binder;
        executionContext.pushTimestampRequiredFlag(false);
        boolean hasPushedWindowContext = false;
        try {
            if (!executionContext.getWindowContext().isEmpty()) {
                executionContext.pushWindowContext();
                hasPushedWindowContext = true;
            }
            final RecordCursorFactory factory = compiler.generatePlan(subquery, executionContext);
            assert hasSchema(factory.getMetadata(), subquery.getRoot().getOutput()) : "generated sub-query metadata differs from its plan";
            assert !LogicalPlans.isSequenceStable(subquery.getRoot(), executionContext) || factory.isStableWithinExecution()
                    : "generated sub-query is less stable than its plan";
            if (!executionContext.allowNonDeterministicFunctions() && factory.usesExternalDataSource()) {
                final SqlException exception = SqlException.nonDeterministicColumn(subqueryPositions.getQuick(index), "sub-query",
                        executionContext.isLiveViewCompile() ? "live view" : "materialized view");
                Misc.free(factory, exception);
                throw exception;
            }
            return factory;
        } finally {
            if (hasPushedWindowContext) {
                executionContext.popWindowContext();
            }
            executionContext.popTimestampRequiredFlag();
        }
    }

    SqlBinder getBinder() {
        return binder;
    }

    int getScalarBoundDepth() {
        return depth;
    }

    int getSubqueryFirstColumnPosition(int index) {
        return subqueryLevels.getQuick(index).binder.getOutputColumnPosition(0);
    }

    RecordCursorFactory getSubqueryMetadata(int index) {
        return subqueryLevels.getQuick(index).metadata;
    }

    LogicalPlan getSubqueryPlan(int index) {
        return subqueryLevels.getQuick(index).binder.getRoot();
    }

    /**
     * Returns an owned factory of the sub-query: the one generated for its first consumer, otherwise a new one.
     */
    RecordCursorFactory takeSubquery(int index, SqlExecutionContext executionContext) throws SqlException {
        completeSubquery(index, executionContext);
        final RecordCursorFactory factory = subqueryFactories.getQuick(index);
        if (factory != null) {
            subqueryFactories.setQuick(index, null);
            return factory;
        }
        return generateSubquery(index, executionContext);
    }
}

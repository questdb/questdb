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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.griffin.engine.table.AdaptiveSymbolPatternRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncJitFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.griffin.engine.table.FilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.RuntimeConstGateRecordCursorFactory;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.jit.CompiledCountOnlyFilter;
import io.questdb.jit.CompiledFilter;
import io.questdb.jit.CompiledFilterIRSerializer;
import io.questdb.jit.JitUtil;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ANY;

final class FilterFactoryGenerator {
    private static final Log LOG = LogFactory.getLog(FilterFactoryGenerator.class);
    private final CairoConfiguration configuration;
    private final boolean enableJitDebug;
    private final MemoryCARW jitIRMem;
    private final CompiledFilterIRSerializer jitIRSerializer;
    private final PageFrameReduceTaskFactory reduceTaskFactory;
    private boolean enableJitNullChecks = true;

    FilterFactoryGenerator(
            CairoConfiguration configuration,
            CharacterStore characterStore,
            MemoryCARW jitIRMem,
            PageFrameReduceTaskFactory reduceTaskFactory,
            StringSink scratchSink,
            IntList indexScratch,
            IntList valueScratch,
            IntList masterKeyScratch,
            IntList slaveKeyScratch,
            LongList longScratch
    ) {
        this.configuration = configuration;
        this.jitIRMem = jitIRMem;
        this.reduceTaskFactory = reduceTaskFactory;
        this.jitIRSerializer = new CompiledFilterIRSerializer(characterStore, scratchSink, indexScratch, valueScratch, masterKeyScratch, slaveKeyScratch, longScratch);
        this.enableJitDebug = configuration.isSqlJitDebugEnabled();
    }

    private static RecordCursorFactory addParallel(GenerationFrame frame, RecordCursorFactory factory, BoundExpression predicate) {
        frame.parallelFilterFactories.add(factory);
        frame.parallelFilterPredicates.add(predicate);
        return factory;
    }

    private RecordCursorFactory generate(
            GenerationFrame frame,
            BoundExpression predicate,
            OutputSchema input,
            RecordCursorFactory base,
            Function filter,
            FunctionInstantiator instantiator,
            SqlExecutionContext executionContext,
            boolean isUpdate,
            LimitPlan limitAdvice,
            boolean enablePreTouch,
            boolean isConstantFolded
    ) throws SqlException {
        ObjList<Function> workerFilters = null;
        Function limit = null;
        boolean isAdopted = false;
        try {
            if (filter.isConstant() && isConstantFolded) {
                isAdopted = true;
                return generateConstantFilter(base, filter);
            }
            if (filter.isRuntimeConstant()) {
                final RecordCursorFactory result = new RuntimeConstGateRecordCursorFactory(base, filter);
                isAdopted = true;
                return result;
            }
            if (!canUseParallelFilter(base, executionContext)) {
                isAdopted = true;
                return generateJavaFilter(base, filter, false, null, null, null, 0, false, executionContext);
            }
            final IntHashSet columns = new IntHashSet();
            collectColumnIndexes(predicate, input, columns);
            final RecordCursorFactory jitFactory = tryGenerateJitFilter(
                    base, filter, columns, executionContext,
                    predicate, input, instantiator, isUpdate, enablePreTouch, limitAdvice
            );
            if (jitFactory != null) {
                isAdopted = true;
                return addParallel(frame, jitFactory, predicate);
            }
            if (limitAdvice != null && limitAdvice.getHi() == null) {
                limit = instantiator.instantiate(limitAdvice.getLo(), input, executionContext);
            }
            workerFilters = compileWorkers(predicate, input, base.getMetadata(), filter, instantiator, executionContext);
            isAdopted = true;
            return addParallel(frame, generateJavaFilter(base, filter, true, columns, workerFilters, limit,
                    limitAdvice == null ? 0 : limitAdvice.getLo().getPosition(), enablePreTouch, executionContext), predicate);
        } catch (Throwable th) {
            if (!isAdopted) {
                Misc.freeObjList(workerFilters, th);
                Misc.free(limit, th);
                Misc.free(filter, th);
                Misc.free(base, th);
            }
            throw th;
        }
    }

    static boolean canUseParallelFilter(RecordCursorFactory base, SqlExecutionContext executionContext) {
        return executionContext.isParallelFilterEnabled() && base.supportsPageFrameCursor();
    }

    static void collectColumnIndexes(BoundExpression expression, OutputSchema input, IntHashSet columns) {
        if (expression instanceof ColumnExpression column) {
            final int index = input.getColumnIndexById(column.getColumnId());
            if (index < 0) {
                throw new IllegalStateException("bound filter input has changed");
            }
            columns.add(index);
        } else if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                collectColumnIndexes(call.argumentAt(i), input, columns);
            }
        }
    }

    static ObjList<Function> compileWorkers(
            BoundExpression predicate,
            OutputSchema input,
            RecordMetadata metadata,
            Function filter,
            FunctionInstantiator instantiator,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (filter.isThreadSafe()) {
            return null;
        }
        final int count = executionContext.getSharedQueryWorkerCount();
        final ObjList<Function> workers = new ObjList<>(count);
        instantiator.beginWorkerClones();
        try {
            for (int i = 0; i < count; i++) {
                workers.add(instantiator.instantiate(predicate, input, metadata, executionContext));
            }
            return workers;
        } catch (Throwable th) {
            Misc.freeObjList(workers, th);
            throw th;
        } finally {
            instantiator.endWorkerClones();
        }
    }

    /**
     * Consumes the factory and a constant Boolean filter, including on failure. The empty result
     * copies the metadata container before closing its source; closeable source metadata may own
     * lookup state that the result must not retain.
     */
    static RecordCursorFactory generateConstantFilter(RecordCursorFactory factory, Function filter) {
        final RecordCursorFactory result;
        try {
            result = filter.getBool(null)
                    ? factory
                    : new EmptyTableRecordCursorFactory(GenericRecordMetadata.copyOfNew(factory.getMetadata()));
        } catch (Throwable th) {
            Misc.free(factory, th);
            Misc.free(filter, th);
            throw th;
        }

        Throwable failure = result != factory ? Misc.freeBestEffort(null, factory) : null;
        failure = Misc.freeBestEffort(failure, filter);
        if (failure != null) {
            Misc.free(result, failure);
            CairoException.rethrowCleanupFailure(failure);
        }
        return result;
    }

    /**
     * Returns the residual predicate a parallel filter of this frame compiled, or null.
     */
    static BoundExpression getParallelPredicate(GenerationFrame frame, RecordCursorFactory factory) {
        for (int i = 0, n = frame.parallelFilterFactories.size(); i < n; i++) {
            if (frame.parallelFilterFactories.getQuick(i) == factory) {
                return frame.parallelFilterPredicates.getQuick(i);
            }
        }
        return null;
    }

    static boolean isParallelFilter(RecordCursorFactory factory) {
        return !factory.implementsLimit() && (factory instanceof AsyncFilteredRecordCursorFactory
                || factory instanceof AsyncJitFilteredRecordCursorFactory
                || factory instanceof AdaptiveSymbolPatternRecordCursorFactory adaptive && adaptive.isSelfFiltering());
    }

    /**
     * Returns the filter a parallel operator steals from a filter factory. Every stealable factory,
     * a {@link #isParallelFilter parallel filter} or a {@link FilteredRecordCursorFactory}, carries one.
     */
    static @NotNull Function stolenFilter(RecordCursorFactory factory) {
        final Function filter = factory.getFilter();
        assert filter != null;
        return filter;
    }

    boolean canUseParallelCoveringFilter(
            CoveringIndexRecordCursorFactory base,
            @Nullable Function limit,
            SqlExecutionContext executionContext
    ) throws SqlException {
        // Multi-key covering scans cannot serve the backward frames required by a negative limit.
        return canUseParallelFilter(base, executionContext)
                && (limit == null || base.supportsNegativeLimitPageFrame() || !mayBeNegativeLimit(limit, executionContext));
    }

    RecordCursorFactory generate(
            GenerationFrame frame,
            BoundExpression predicate,
            OutputSchema input,
            RecordCursorFactory base,
            Function filter,
            FunctionInstantiator instantiator,
            SqlExecutionContext executionContext,
            boolean isUpdate,
            LimitPlan limitAdvice,
            boolean enablePreTouch
    ) throws SqlException {
        return generate(frame, predicate, input, base, filter, instantiator, executionContext, isUpdate, limitAdvice, enablePreTouch, true);
    }

    /**
     * Consumes both executable roots on entry, including on failure.
     */
    RecordCursorFactory generate(
            GenerationFrame frame,
            BoundExpression predicate,
            OutputSchema input,
            RecordCursorFactory base,
            Function filter,
            FunctionInstantiator instantiator,
            SqlExecutionContext executionContext,
            boolean isUpdate
    ) throws SqlException {
        return generate(frame, predicate, input, base, filter, instantiator, executionContext, isUpdate, null, false);
    }

    /**
     * Consumes the covering factory and filter, including on failure.
     */
    RecordCursorFactory generateCovering(
            BoundExpression predicate, OutputSchema input, CoveringIndexRecordCursorFactory base, Function filter,
            FunctionInstantiator instantiator, SqlExecutionContext executionContext, LimitPlan limitAdvice, boolean enablePreTouch
    ) throws SqlException {
        Function limit = null;
        ObjList<Function> workers = null;
        boolean isAdopted = false;
        try {
            boolean isParallel = canUseParallelFilter(base, executionContext);
            IntHashSet columns = null;
            if (isParallel) {
                if (limitAdvice != null && limitAdvice.getHi() == null) {
                    limit = instantiator.instantiate(limitAdvice.getLo(), input, executionContext);
                }
                isParallel = canUseParallelCoveringFilter(base, limit, executionContext);
                if (isParallel) {
                    columns = new IntHashSet();
                    collectColumnIndexes(predicate, input, columns);
                    workers = compileWorkers(predicate, input, base.getMetadata(), filter, instantiator, executionContext);
                }
            }
            final int limitPosition = limitAdvice == null ? 0 : limitAdvice.getLo().getPosition();
            isAdopted = true;
            return generateJavaFilter(base, filter, isParallel, columns, workers,
                    limit, limitPosition, enablePreTouch, executionContext);
        } catch (Throwable th) {
            if (!isAdopted) {
                Misc.freeObjList(workers, th);
                Misc.free(limit, th);
                Misc.free(filter, th);
                Misc.free(base, th);
            }
            throw th;
        }
    }

    /**
     * Consumes the base, filter, worker functions and limit on entry, including on failure.
     */
    RecordCursorFactory generateJavaFilter(
            RecordCursorFactory base,
            Function filter,
            boolean isParallel,
            @Nullable IntHashSet columns,
            @Nullable ObjList<Function> workers,
            @Nullable Function limit,
            int limitPosition,
            boolean enablePreTouch,
            SqlExecutionContext executionContext
    ) {
        try {
            if (isParallel) {
                assert columns != null;
                return new AsyncFilteredRecordCursorFactory(
                        executionContext.getCairoEngine(), configuration, executionContext.getMessageBus(),
                        base, filter, columns, reduceTaskFactory, workers, limit, limitPosition,
                        executionContext.getSharedQueryWorkerCount(), enablePreTouch
                );
            }
            assert workers == null;
            final Function unusedLimit = limit;
            limit = null;
            Misc.free(unusedLimit);
            return new FilteredRecordCursorFactory(base, filter);
        } catch (Throwable th) {
            // Async construction nulls the worker slots it closes; the remaining slots stay ours.
            Misc.freeObjList(workers, th);
            Misc.free(limit, th);
            Misc.free(filter, th);
            Misc.free(base, th);
            throw th;
        }
    }

    /**
     * Like a join-level filter, keeps a constant folded from functions as a filter; only a literal constant folds.
     */
    RecordCursorFactory generatePostJoin(
            GenerationFrame frame,
            BoundExpression predicate,
            OutputSchema input,
            RecordCursorFactory base,
            Function filter,
            FunctionInstantiator instantiator,
            SqlExecutionContext executionContext
    ) throws SqlException {
        return generate(frame, predicate, input, base, filter, instantiator, executionContext, false, null, false,
                predicate instanceof ConstantExpression constant && constant.isLiteral());
    }

    /**
     * Whether the limit lo function might evaluate to a negative value. A
     * non-constant (e.g. bind-variable) limit has an unknown sign at compile
     * time, so we conservatively treat it as possibly negative.
     */
    boolean mayBeNegativeLimit(Function limitLoFunction, SqlExecutionContext executionContext) throws SqlException {
        if (!limitLoFunction.isConstant()) {
            return true;
        }
        limitLoFunction.init(null, executionContext);
        final long limit = limitLoFunction.getLong(null);
        return limit != Numbers.LONG_NULL && limit < 0;
    }

    void setEnableJitNullChecks(boolean value) {
        enableJitNullChecks = value;
    }

    /**
     * Adopts base and filter on success; leaves them with the caller on decline or failure.
     * Limit advice is borrowed; an instantiated limit belongs here until factory adoption.
     */
    @Nullable RecordCursorFactory tryGenerateJitFilter(
            RecordCursorFactory base,
            Function filter,
            IntHashSet columns,
            SqlExecutionContext executionContext,
            BoundExpression predicate,
            OutputSchema input,
            FunctionInstantiator instantiator,
            boolean isUpdate,
            boolean enablePreTouch,
            @Nullable LimitPlan limitAdvice
    ) throws SqlException {
        if (!canUseParallelFilter(base, executionContext)
                || executionContext.getJitMode() == SqlJitMode.JIT_MODE_DISABLED
                || isUpdate && !executionContext.isWalApplication()
                || !JitUtil.isJitSupported()) {
            return null;
        }
        CompiledFilter compiledFilter = null;
        CompiledCountOnlyFilter compiledCountOnlyFilter = null;
        final ObjList<Function> bindVarFunctions = new ObjList<>();
        ObjList<Function> workers = null;
        Function limit = null;
        try {
            final int jitOptions;
            Throwable scratchFailure = null;
            try {
                try (PageFrameCursor cursor = base.getPageFrameCursor(executionContext, ORDER_ANY)) {
                    final boolean forceScalar = executionContext.getJitMode() == SqlJitMode.JIT_MODE_FORCE_SCALAR;
                    jitIRSerializer.of(jitIRMem, executionContext, base.getMetadata(), input, cursor, bindVarFunctions);
                    jitOptions = jitIRSerializer.serialize(predicate, forceScalar, enableJitDebug, enableJitNullChecks);
                }
                compiledFilter = new CompiledFilter();
                compiledFilter.compile(jitIRMem, jitOptions);
                compiledCountOnlyFilter = new CompiledCountOnlyFilter();
                compiledCountOnlyFilter.compile(jitIRMem, jitOptions);
            } catch (Throwable th) {
                scratchFailure = th;
                throw th;
            } finally {
                final boolean hasPrimary = scratchFailure != null;
                scratchFailure = Misc.clearBestEffort(scratchFailure, jitIRSerializer);
                try {
                    jitIRMem.truncate();
                } catch (Throwable th) {
                    scratchFailure = Misc.foldCleanupFailure(scratchFailure, th);
                }
                if (!hasPrimary) {
                    CairoException.rethrowCleanupFailure(scratchFailure);
                }
            }

            final int limitPosition;
            if (limitAdvice != null && limitAdvice.getHi() == null) {
                limit = instantiator.instantiate(limitAdvice.getLo(), input, executionContext);
                limitPosition = limitAdvice.getLo().getPosition();
            } else {
                limitPosition = 0;
            }
            LOG.debug().$("JIT enabled for (sub)query [fd=").$(executionContext.getRequestFd()).I$();
            workers = FilterFactoryGenerator.compileWorkers(predicate, input, base.getMetadata(), filter, instantiator, executionContext);
            return new AsyncJitFilteredRecordCursorFactory(
                    executionContext.getCairoEngine(), configuration, executionContext.getMessageBus(),
                    base, bindVarFunctions, compiledFilter, compiledCountOnlyFilter, filter, columns,
                    reduceTaskFactory, workers, limit, limitPosition,
                    executionContext.getSharedQueryWorkerCount(), enablePreTouch
            );
        } catch (SqlException | LimitOverflowException decline) {
            Throwable cleanup = Misc.freeBestEffort(null, compiledFilter);
            cleanup = Misc.freeBestEffort(cleanup, compiledCountOnlyFilter);
            cleanup = Misc.freeObjListBestEffort(cleanup, bindVarFunctions);
            cleanup = Misc.freeBestEffort(cleanup, limit);
            cleanup = Misc.freeObjListBestEffort(cleanup, workers);
            if (cleanup != null) {
                decline.addSuppressed(cleanup);
                throw decline;
            }
            LOG.debug()
                    .$("JIT cannot be applied to (sub)query [ex=").$safe(decline.getFlyweightMessage())
                    .$(", fd=").$(executionContext.getRequestFd()).I$();
            return null;
        } catch (Throwable th) {
            // Failed async construction nulls any worker slots it has already released.
            Misc.freeObjList(workers, th);
            Misc.free(limit, th);
            Misc.free(compiledFilter, th);
            Misc.free(compiledCountOnlyFilter, th);
            Misc.freeObjList(bindVarFunctions, th);
            throw th;
        }
    }
}

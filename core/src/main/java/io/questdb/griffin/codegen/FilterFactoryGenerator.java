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

package io.questdb.griffin.codegen;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.CharacterStore;
import io.questdb.griffin.FunctionInstantiator;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncJitFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.griffin.engine.table.FilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.RuntimeConstGateRecordCursorFactory;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.GeneratedShapes;
import io.questdb.griffin.plan.logical.JoinInput;
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
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ANY;

final class FilterFactoryGenerator {
    private static final Log LOG = LogFactory.getLog(FilterFactoryGenerator.class);
    private final CairoConfiguration configuration;
    private final boolean enableJitDebug;
    private final ObjList<Function> jitBindVarFunctions = new ObjList<>();
    private final MemoryCARW jitIRMem;
    private final CompiledFilterIRSerializer jitIRSerializer;
    private final PageFrameReduceTaskFactory reduceTaskFactory;
    private boolean enableJitNullChecks = true;

    FilterFactoryGenerator(
            CairoConfiguration configuration,
            CharacterStore characterStore,
            MemoryCARW jitIRMem,
            PageFrameReduceTaskFactory reduceTaskFactory,
            StringSink tmpSink,
            IntList tmpIndexes,
            IntList tmpValues,
            IntList tmpMasterKeys,
            IntList tmpSlaveKeys,
            LongList tmpLongs
    ) {
        this.configuration = configuration;
        this.jitIRMem = jitIRMem;
        this.reduceTaskFactory = reduceTaskFactory;
        this.jitIRSerializer = new CompiledFilterIRSerializer(characterStore, tmpSink, tmpIndexes, tmpValues, tmpMasterKeys, tmpSlaveKeys, tmpLongs);
        this.enableJitDebug = configuration.isSqlJitDebugEnabled();
    }

    /**
     * Compiles the predicate over the page frames of the factory into the prepared filter's JIT filters and bind
     * variables. Throws the JIT's decline, leaving what it compiled in the prepared filter.
     */
    private void compileJit(PreparedFilter target, RecordCursorFactory base, BoundExpression predicate, OutputSchema input,
                            SqlExecutionContext executionContext) throws SqlException {
        CompiledFilter compiledFilter = null;
        CompiledCountOnlyFilter compiledCountOnlyFilter = null;
        try {
            final int jitOptions;
            Throwable cleanupFailure = null;
            try {
                try (PageFrameCursor cursor = base.getPageFrameCursor(executionContext, ORDER_ANY)) {
                    final boolean forceScalar = executionContext.getJitMode() == SqlJitMode.JIT_MODE_FORCE_SCALAR;
                    jitIRSerializer.of(jitIRMem, executionContext, base.getMetadata(), input, cursor, jitBindVarFunctions);
                    jitOptions = jitIRSerializer.serialize(predicate, forceScalar, enableJitDebug, enableJitNullChecks);
                }
                compiledFilter = new CompiledFilter();
                compiledFilter.compile(jitIRMem, jitOptions);
                compiledCountOnlyFilter = new CompiledCountOnlyFilter();
                compiledCountOnlyFilter.compile(jitIRMem, jitOptions);
            } catch (Throwable th) {
                cleanupFailure = th;
                throw th;
            } finally {
                final boolean hasPrimary = cleanupFailure != null;
                cleanupFailure = Misc.clearBestEffort(cleanupFailure, jitIRSerializer);
                try {
                    jitIRMem.truncate();
                } catch (Throwable th) {
                    cleanupFailure = Misc.foldCleanupFailure(cleanupFailure, th);
                }
                if (!hasPrimary) {
                    CairoException.rethrowCleanupFailure(cleanupFailure);
                }
            }
        } catch (Throwable th) {
            target.setJit(compiledFilter, compiledCountOnlyFilter, null);
            final Throwable cleanup = Misc.freeObjListBestEffort(null, jitBindVarFunctions);
            jitBindVarFunctions.clear();
            if (cleanup != null) {
                th.addSuppressed(cleanup);
            }
            throw th;
        }
        target.setJit(compiledFilter, compiledCountOnlyFilter, new ObjList<>(jitBindVarFunctions));
        jitBindVarFunctions.clear();
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
            boolean isParallel,
            BoundExpression limitCount,
            boolean enablePreTouch,
            boolean isConstantFolded
    ) throws SqlException {
        if (filter.isConstant() && isConstantFolded) {
            return generateConstantFilter(base, filter);
        }
        if (filter.isRuntimeConstant()) {
            return new RuntimeConstGateRecordCursorFactory(base, filter);
        }
        final IntHashSet columns = new IntHashSet();
        try {
            if (isParallel) {
                collectColumnIndexes(predicate, input, columns);
            }
        } catch (Throwable th) {
            Misc.free(filter, th);
            Misc.free(base, th);
            throw th;
        }
        if (!isParallel) {
            return generateJavaFilter(base, filter, false, null, null, null, 0, false, executionContext);
        }
        final RecordCursorFactory jitFactory = tryGenerateJitFilter(
                frame, base, filter, columns, executionContext,
                predicate, input, instantiator, isUpdate, enablePreTouch, limitCount
        );
        if (jitFactory != null) {
            return jitFactory;
        }
        Function limit = null;
        final ObjList<Function> workerFilters;
        try {
            if (limitCount != null) {
                limit = instantiator.instantiate(limitCount, input, executionContext);
            }
            workerFilters = instantiator.instantiateWorkers(predicate, input, base.getMetadata(), filter,
                    executionContext.getSharedQueryWorkerCount(), executionContext);
        } catch (Throwable th) {
            Misc.free(limit, th);
            Misc.free(filter, th);
            Misc.free(base, th);
            throw th;
        }
        return generateJavaFilter(base, filter, true, columns, workerFilters, limit,
                limitCount == null ? 0 : limitCount.getPosition(), enablePreTouch, executionContext);
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
     * Consumes both executable roots on entry, including on failure; builds the parallel filter when
     * {@code isParallel}, as operator planning records it, else the serial one.
     */
    RecordCursorFactory generate(
            GenerationFrame frame,
            BoundExpression predicate,
            OutputSchema input,
            RecordCursorFactory base,
            Function filter,
            FunctionInstantiator instantiator,
            SqlExecutionContext executionContext,
            boolean isUpdate,
            boolean isParallel,
            BoundExpression limitCount,
            boolean enablePreTouch
    ) throws SqlException {
        return generate(frame, predicate, input, base, filter, instantiator, executionContext, isUpdate, isParallel, limitCount, enablePreTouch, true);
    }

    /**
     * Consumes the covering factory and filter, including on failure; builds the parallel filter when
     * {@code isParallel}, as operator planning records it, else the serial one.
     */
    RecordCursorFactory generateCovering(
            BoundExpression predicate, OutputSchema input, CoveringIndexRecordCursorFactory base, Function filter,
            FunctionInstantiator instantiator, SqlExecutionContext executionContext, boolean isParallel, BoundExpression limitCount,
            boolean enablePreTouch
    ) throws SqlException {
        Function limit = null;
        ObjList<Function> workers = null;
        boolean isAdopted = false;
        try {
            IntHashSet columns = null;
            if (isParallel) {
                if (limitCount != null) {
                    limit = instantiator.instantiate(limitCount, input, executionContext);
                }
                columns = new IntHashSet();
                collectColumnIndexes(predicate, input, columns);
                workers = instantiator.instantiateWorkers(predicate, input, base.getMetadata(), filter,
                        executionContext.getSharedQueryWorkerCount(), executionContext);
            }
            final int limitPosition = limitCount == null ? 0 : limitCount.getPosition();
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
        if (isParallel) {
            assert columns != null;
            return new AsyncFilteredRecordCursorFactory(
                    executionContext.getCairoEngine(), configuration, executionContext.getMessageBus(),
                    base, filter, columns, reduceTaskFactory, workers, limit, limitPosition,
                    executionContext.getSharedQueryWorkerCount(), enablePreTouch
            );
        }
        assert workers == null;
        try {
            Misc.free(limit);
        } catch (Throwable th) {
            Misc.free(filter, th);
            Misc.free(base, th);
            throw th;
        }
        return new FilteredRecordCursorFactory(base, filter);
    }

    /**
     * Instantiates the filter the join step applies to its joined rows over the factory and filters the factory with
     * it, see {@link #generatePostJoin(GenerationFrame, JoinInput, BoundExpression, RecordCursorFactory, Function, SqlExecutionContext)}.
     * Consumes the factory on entry, including on failure.
     */
    RecordCursorFactory generatePostJoin(
            GenerationFrame frame,
            JoinInput step,
            BoundExpression predicate,
            RecordCursorFactory base,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final Function filter;
        try {
            filter = frame.functionInstantiator.instantiate(predicate, step.getOutput(), base.getMetadata(), executionContext);
        } catch (Throwable th) {
            Misc.free(base, th);
            throw th;
        }
        return generatePostJoin(frame, step, predicate, base, filter, executionContext);
    }

    /**
     * Filters the factory on one thread with the filter the join step applies to its joined rows, folding a constant
     * as {@link LogicalPlans#isPostJoinFilterFolded} decides. Consumes both executable roots on entry, including on
     * failure.
     */
    RecordCursorFactory generatePostJoin(
            GenerationFrame frame,
            JoinInput step,
            BoundExpression predicate,
            RecordCursorFactory base,
            Function filter,
            SqlExecutionContext executionContext
    ) throws SqlException {
        return generate(frame, predicate, step.getOutput(), base, filter, frame.functionInstantiator, executionContext, false, false, null,
                false, GeneratedShapes.isPostJoinFilterFolded(step, predicate));
    }

    /**
     * Fills the prepared filter's column set, its JIT filter where the prepared filter allows one, with the bind
     * variable memory the consumer reads them from, and its per-worker clones.
     */
    void prepareParallel(PreparedFilter target, RecordCursorFactory leaf, FunctionInstantiator instantiator, SqlExecutionContext executionContext)
            throws SqlException {
        final BoundExpression predicate = target.getPredicate();
        final OutputSchema input = target.getInput();
        final IntHashSet columns = new IntHashSet();
        collectColumnIndexes(predicate, input, columns);
        target.setColumns(columns);
        if (target.isJitAllowed() && executionContext.getJitMode() != SqlJitMode.JIT_MODE_DISABLED && JitUtil.isJitSupported()) {
            try {
                compileJit(target, leaf, predicate, input, executionContext);
            } catch (SqlException | LimitOverflowException decline) {
                Throwable cleanup = Misc.freeBestEffort(null, target.getCompiledFilter());
                cleanup = Misc.freeBestEffort(cleanup, target.getCompiledCountOnlyFilter());
                cleanup = Misc.freeObjListBestEffort(cleanup, target.getBindVarFunctions());
                target.setJit(null, null, null);
                if (cleanup != null) {
                    decline.addSuppressed(cleanup);
                    throw decline;
                }
                LOG.debug()
                        .$("JIT cannot be applied to (sub)query [ex=").$safe(decline.getFlyweightMessage())
                        .$(", fd=").$(executionContext.getRequestFd()).I$();
            }
            if (target.getCompiledFilter() != null) {
                final CompiledCountOnlyFilter countOnlyFilter = target.getCompiledCountOnlyFilter();
                target.setJit(target.getCompiledFilter(), null, target.getBindVarFunctions());
                Misc.free(countOnlyFilter);
                target.setBindVarMemory(Vm.getCARWInstance(
                        configuration.getSqlJitBindVarsMemoryPageSize(),
                        configuration.getSqlJitBindVarsMemoryMaxPages(),
                        MemoryTag.NATIVE_JIT
                ));
            }
        }
        target.setWorkers(instantiator.instantiateWorkers(predicate, input, leaf.getMetadata(), target.getFilter(),
                executionContext.getSharedQueryWorkerCount(), executionContext));
    }

    void setEnableJitNullChecks(boolean value) {
        enableJitNullChecks = value;
    }

    /**
     * Returns null, leaving base and filter with the caller, when the JIT declines the predicate;
     * otherwise consumes them, including on failure. The limit count is borrowed.
     */
    @Nullable RecordCursorFactory tryGenerateJitFilter(
            GenerationFrame frame,
            RecordCursorFactory base,
            Function filter,
            IntHashSet columns,
            SqlExecutionContext executionContext,
            BoundExpression predicate,
            OutputSchema input,
            FunctionInstantiator instantiator,
            boolean isUpdate,
            boolean enablePreTouch,
            @Nullable BoundExpression limitCount
    ) throws SqlException {
        if (executionContext.getJitMode() == SqlJitMode.JIT_MODE_DISABLED
                || isUpdate && !executionContext.isWalApplication()
                || !JitUtil.isJitSupported()) {
            return null;
        }
        final PreparedFilter jit = frame.pushPreparedFilter();
        ObjList<Function> workers = null;
        Function limit = null;
        final int limitPosition;
        try {
            compileJit(jit, base, predicate, input, executionContext);
            if (limitCount != null) {
                limit = instantiator.instantiate(limitCount, input, executionContext);
                limitPosition = limitCount.getPosition();
            } else {
                limitPosition = 0;
            }
            LOG.debug().$("JIT enabled for (sub)query [fd=").$(executionContext.getRequestFd()).I$();
            workers = instantiator.instantiateWorkers(predicate, input, base.getMetadata(), filter,
                    executionContext.getSharedQueryWorkerCount(), executionContext);
        } catch (SqlException | LimitOverflowException decline) {
            Throwable cleanup = Misc.freeBestEffort(null, limit);
            cleanup = Misc.freeObjListBestEffort(cleanup, workers);
            try {
                frame.popPreparedFilter();
            } catch (Throwable th) {
                cleanup = Misc.foldCleanupFailure(cleanup, th);
            }
            if (cleanup != null) {
                decline.addSuppressed(cleanup);
                Misc.free(filter, decline);
                Misc.free(base, decline);
                throw decline;
            }
            LOG.debug()
                    .$("JIT cannot be applied to (sub)query [ex=").$safe(decline.getFlyweightMessage())
                    .$(", fd=").$(executionContext.getRequestFd()).I$();
            return null;
        } catch (Throwable th) {
            Misc.freeObjList(workers, th);
            Misc.free(limit, th);
            frame.popPreparedFilter(th);
            Misc.free(filter, th);
            Misc.free(base, th);
            throw th;
        }
        final CompiledFilter compiledFilter = jit.getCompiledFilter();
        final CompiledCountOnlyFilter compiledCountOnlyFilter = jit.getCompiledCountOnlyFilter();
        final ObjList<Function> bindVarFunctions = jit.getBindVarFunctions();
        jit.adopt();
        frame.popPreparedFilter();
        return new AsyncJitFilteredRecordCursorFactory(
                executionContext.getCairoEngine(), configuration, executionContext.getMessageBus(),
                base, bindVarFunctions, compiledFilter, compiledCountOnlyFilter, filter, columns,
                reduceTaskFactory, workers, limit, limitPosition,
                executionContext.getSharedQueryWorkerCount(), enablePreTouch
        );
    }
}

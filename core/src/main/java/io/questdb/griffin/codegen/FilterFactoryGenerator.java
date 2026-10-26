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
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.CharacterStore;
import io.questdb.griffin.FunctionInstantiator;
import io.questdb.griffin.PlanTable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.griffin.engine.functions.regex.MatchSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.regex.SymbolKeySetProvider;
import io.questdb.griffin.engine.table.AdaptiveSymbolPatternRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncFilterResources;
import io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncJitFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.griffin.engine.table.FilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestByCompiledFilter;
import io.questdb.griffin.engine.table.MasterSuffixLimitFunction;
import io.questdb.griffin.engine.table.RuntimeConstGateRecordCursorFactory;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PlanToFactoryRules;
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
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ANY;

final class FilterFactoryGenerator {
    private static final Log LOG = LogFactory.getLog(FilterFactoryGenerator.class);
    private final CairoConfiguration configuration;
    private final boolean enableJitDebug;
    private final FunctionInstantiator functionInstantiator;
    private final MemoryCARW jitIRMem;
    private final CompiledFilterIRSerializer jitIRSerializer;
    private final IntHashSet latestByColumns = new IntHashSet();
    private final ReaderSymbolTables readerSymbolTables = new ReaderSymbolTables();
    private final PageFrameReduceTaskFactory reduceTaskFactory;
    private boolean enableJitNullChecks = true;

    FilterFactoryGenerator(
            CairoConfiguration configuration,
            CharacterStore characterStore,
            MemoryCARW jitIRMem,
            PageFrameReduceTaskFactory reduceTaskFactory,
            FunctionInstantiator functionInstantiator,
            StringSink tmpSink,
            IntList tmpIndexes,
            IntList tmpValues,
            LongList tmpLongs
    ) {
        this.configuration = configuration;
        this.jitIRMem = jitIRMem;
        this.reduceTaskFactory = reduceTaskFactory;
        this.functionInstantiator = functionInstantiator;
        this.jitIRSerializer = new CompiledFilterIRSerializer(characterStore, tmpSink, tmpIndexes, tmpValues, tmpLongs);
        this.enableJitDebug = configuration.isSqlJitDebugEnabled();
    }

    private static boolean isNegated(FunctionExpression pattern) {
        return pattern.getArgumentCount() == 1 || "!~".equals(pattern.getName());
    }

    private void clearJitScratch(Throwable failure) {
        final boolean hasPrimary = failure != null;
        failure = Misc.clearBestEffort(failure, jitIRSerializer);
        try {
            jitIRMem.truncate();
        } catch (Throwable th) {
            failure = Misc.foldCleanupFailure(failure, th);
        }
        if (!hasPrimary) {
            CairoException.rethrowCleanupFailure(failure);
        }
    }

    /**
     * Compiles a page-frame filter and its count-only variant, or a LATEST BY filter whose symbol tables come from
     * the table reader. Throws the JIT's decline, leaving what it compiled in the prepared filter.
     */
    private void compileJit(GenerationFrame frame, PreparedFilter target, @Nullable RecordCursorFactory base, BoundExpression predicate,
                            OutputSchema input, RecordMetadata metadata, @Nullable PlanTable table, @Nullable IntList columnIndexes,
                            SqlExecutionContext executionContext) throws SqlException {
        final ObjList<Function> bindVarFunctions = frame.functions;
        bindVarFunctions.clear();
        CompiledFilter compiledFilter = null;
        CompiledCountOnlyFilter compiledCountOnlyFilter = null;
        try {
            final int jitOptions;
            Throwable cleanupFailure = null;
            try {
                final boolean forceScalar = executionContext.getJitMode() == SqlJitMode.JIT_MODE_FORCE_SCALAR;
                if (base != null) {
                    try (PageFrameCursor cursor = base.getPageFrameCursor(executionContext, ORDER_ANY)) {
                        jitIRSerializer.of(jitIRMem, executionContext, metadata, input, cursor, bindVarFunctions);
                        jitOptions = jitIRSerializer.serialize(predicate, forceScalar, enableJitDebug, enableJitNullChecks);
                    }
                } else {
                    assert table != null && columnIndexes != null;
                    try (TableReader reader = executionContext.getReader(table.getTableToken(), table.getMetadataVersion())) {
                        jitIRSerializer.of(jitIRMem, executionContext, metadata, input, readerSymbolTables.of(reader, columnIndexes), bindVarFunctions);
                        jitOptions = jitIRSerializer.serialize(predicate, forceScalar, enableJitDebug, enableJitNullChecks);
                    }
                }
                compiledFilter = new CompiledFilter();
                compiledFilter.compile(jitIRMem, jitOptions);
                if (base != null) {
                    compiledCountOnlyFilter = new CompiledCountOnlyFilter();
                    compiledCountOnlyFilter.compile(jitIRMem, jitOptions);
                }
            } catch (Throwable th) {
                cleanupFailure = th;
                throw th;
            } finally {
                readerSymbolTables.clear();
                clearJitScratch(cleanupFailure);
            }
            target.setJit(compiledFilter, compiledCountOnlyFilter, new ObjList<>(bindVarFunctions));
        } catch (Throwable th) {
            target.setJit(compiledFilter, compiledCountOnlyFilter, null);
            final Throwable cleanup = Misc.freeObjListBestEffort(null, bindVarFunctions);
            bindVarFunctions.clear();
            if (cleanup != null) {
                th.addSuppressed(cleanup);
            }
            throw th;
        }
        bindVarFunctions.clear();
    }

    private RecordCursorFactory generate(
            GenerationFrame frame,
            BoundExpression predicate,
            OutputSchema input,
            RecordCursorFactory base,
            Function filter,
            int producerLo,
            SqlExecutionContext executionContext,
            boolean isUpdate,
            boolean isParallel,
            BoundExpression limitCount,
            boolean enablePreTouch,
            boolean isConstantFolded
    ) throws SqlException {
        if (filter.isConstant() && isConstantFolded) {
            return generateConstantFilter(frame, producerLo, base, filter);
        }
        if (filter.isRuntimeConstant()) {
            return new RuntimeConstGateRecordCursorFactory(base, filter);
        }
        if (!isParallel) {
            return generateJavaFilter(base, filter, false, null, null, null, 0, false, executionContext);
        }
        final IntHashSet columns = new IntHashSet();
        try {
            collectColumnIndexes(predicate, input, columns);
        } catch (Throwable th) {
            Misc.free(filter, th);
            Misc.free(base, th);
            throw th;
        }
        final RecordCursorFactory jitFactory = tryGenerateJitFilter(
                frame, base, filter, columns, executionContext,
                predicate, input, isUpdate, enablePreTouch, limitCount
        );
        if (jitFactory != null) {
            return jitFactory;
        }
        Function limit = null;
        final ObjList<Function> workerFilters;
        try {
            if (limitCount != null) {
                limit = instantiateLimit(frame, limitCount, input, executionContext);
            }
            workerFilters = functionInstantiator.instantiateWorkers(predicate, input, base.getMetadata(), filter,
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

    private Function positivePattern(GenerationFrame frame, FunctionExpression pattern, OutputSchema input, RecordMetadata metadata,
                                     SqlExecutionContext executionContext) throws SqlException {
        if (!isNegated(pattern)) {
            return functionInstantiator.instantiate(pattern, input, metadata, executionContext);
        }
        if (pattern.getArgumentCount() == 1) {
            return functionInstantiator.instantiate(pattern.argumentAt(0), input, metadata, executionContext);
        }
        final ObjList<Function> arguments = frame.functions;
        final IntList positions = frame.intListA;
        arguments.clear();
        positions.clear();
        try {
            for (int i = 0; i < 2; i++) {
                arguments.add(functionInstantiator.instantiate(pattern.argumentAt(i), input, metadata, executionContext));
                positions.add(pattern.getArgumentPosition(i));
            }
            final Function match = MatchSymbolFunctionFactory.positivePatternFactory(pattern).newInstance(pattern.getPosition(),
                    arguments, positions, configuration, executionContext);
            arguments.clear();
            return match;
        } catch (Throwable th) {
            Misc.freeObjList(arguments, th);
            throw th;
        } finally {
            arguments.clear();
        }
    }

    /**
     * Compiles the predicate into the prepared filter's JIT filters, see {@link #compileJit}, and returns whether the
     * JIT accepted it. On its decline frees what it compiled and logs the decline, after which the caller builds the
     * Java filter; a failure to free it fails with the decline.
     */
    private boolean tryCompileJit(GenerationFrame frame, PreparedFilter target, @Nullable RecordCursorFactory base, BoundExpression predicate,
                                  OutputSchema input, RecordMetadata metadata, @Nullable PlanTable table, @Nullable IntList columnIndexes,
                                  SqlExecutionContext executionContext) throws SqlException {
        try {
            compileJit(frame, target, base, predicate, input, metadata, table, columnIndexes, executionContext);
            return true;
        } catch (SqlException | LimitOverflowException decline) {
            final Throwable cleanup = target.freeJit();
            if (cleanup != null) {
                decline.addSuppressed(cleanup);
                throw decline;
            }
            LOG.debug()
                    .$("JIT cannot be applied to (sub)query [ex=").$safe(decline.getFlyweightMessage())
                    .$(", fd=").$(executionContext.getRequestFd()).I$();
            return false;
        }
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
     * Consumes the factory, which holds the decorrelation domains the generator placed from {@code producerLo} on, and
     * a constant Boolean filter, including on failure: keeps the factory when the filter is true, else replaces it with
     * an empty one, see {@link GenerationFrame#emptied}.
     */
    static RecordCursorFactory generateConstantFilter(GenerationFrame frame, int producerLo, RecordCursorFactory factory, Function filter) {
        final boolean isTrue;
        try {
            isTrue = filter.getBool(null);
        } catch (Throwable th) {
            Misc.free(factory, th);
            Misc.free(filter, th);
            throw th;
        }
        final Throwable failure = Misc.freeBestEffort(null, filter);
        if (failure != null) {
            Misc.free(factory, failure);
            CairoException.rethrowCleanupFailure(failure);
        }
        return isTrue ? factory : frame.emptied(producerLo, frame.producerMark(), factory);
    }

    /**
     * Whether the JIT may compile a filter: the execution context enables it, the platform supports it, and the filter
     * reads the target of an UPDATE only while WAL application runs the UPDATE.
     */
    static boolean isJitApplicable(boolean isUpdate, SqlExecutionContext executionContext) {
        return executionContext.getJitMode() != SqlJitMode.JIT_MODE_DISABLED && JitUtil.isJitSupported()
                && (!isUpdate || executionContext.isWalApplication());
    }

    /**
     * Wraps the filter of a LATEST BY scan in a {@link LatestByCompiledFilter}, which adopts it, when the JIT compiles
     * the predicate over the columns of {@code table}; otherwise returns the filter. A reader of the table serves the
     * serializer's symbol tables while it serializes the predicate.
     */
    Function compileLatestBy(
            GenerationFrame frame,
            Function filter,
            BoundExpression predicate,
            OutputSchema input,
            RecordMetadata metadata,
            PlanTable table,
            IntList columnIndexes,
            boolean isUpdate,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (!configuration.isSqlLatestByJitEnabled() || !isJitApplicable(isUpdate, executionContext) || filter.isConstant()
                || filter.isRuntimeConstant() || !filter.isStableWithinExecution()) {
            return filter;
        }
        final PreparedFilter jit = frame.pushPreparedFilter();
        Throwable failure = null;
        try {
            if (!tryCompileJit(frame, jit, null, predicate, input, metadata, table, columnIndexes, executionContext)) {
                return filter;
            }
            latestByColumns.clear();
            collectColumnIndexes(predicate, input, latestByColumns);
            final IntList filterColumnIndexes = new IntList(latestByColumns.size());
            for (int i = 0, n = latestByColumns.size(); i < n; i++) {
                filterColumnIndexes.add(latestByColumns.get(i));
            }
            final Function result = new LatestByCompiledFilter(configuration, filter, jit.getCompiledFilter(), jit.getBindVarFunctions(), filterColumnIndexes);
            jit.setJit(null, null, null);
            return result;
        } catch (Throwable th) {
            failure = th;
            throw th;
        } finally {
            if (failure == null) {
                frame.popPreparedFilter();
            } else {
                frame.popPreparedFilter(failure);
            }
        }
    }

    /**
     * Consumes both executable roots on entry, including on failure; builds the parallel filter when
     * {@code isParallel}, as operator planning records it, else the serial one. The base holds the decorrelation
     * domains the generator placed from {@code producerLo} on, see {@link GenerationFrame#producerMark}.
     */
    RecordCursorFactory generate(
            GenerationFrame frame,
            BoundExpression predicate,
            OutputSchema input,
            RecordCursorFactory base,
            Function filter,
            int producerLo,
            SqlExecutionContext executionContext,
            boolean isUpdate,
            boolean isParallel,
            BoundExpression limitCount,
            boolean enablePreTouch
    ) throws SqlException {
        return generate(frame, predicate, input, base, filter, producerLo, executionContext, isUpdate, isParallel, limitCount, enablePreTouch, true);
    }

    /**
     * Consumes the covering factory and filter, including on failure; builds the parallel filter when
     * {@code isParallel}, as operator planning records it, else the serial one.
     */
    RecordCursorFactory generateCovering(
            GenerationFrame frame, BoundExpression predicate, OutputSchema input, CoveringIndexRecordCursorFactory base, Function filter,
            SqlExecutionContext executionContext, boolean isParallel, BoundExpression limitCount, boolean enablePreTouch
    ) throws SqlException {
        Function limit = null;
        ObjList<Function> workers = null;
        boolean isAdopted = false;
        try {
            IntHashSet columns = null;
            if (isParallel) {
                if (limitCount != null) {
                    limit = instantiateLimit(frame, limitCount, input, executionContext);
                }
                columns = new IntHashSet();
                collectColumnIndexes(predicate, input, columns);
                workers = functionInstantiator.instantiateWorkers(predicate, input, base.getMetadata(), filter,
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
     * it, see {@link #generatePostJoin(GenerationFrame, JoinInput, BoundExpression, RecordCursorFactory, Function, int, SqlExecutionContext)}.
     * Consumes the factory on entry, including on failure.
     */
    RecordCursorFactory generatePostJoin(
            GenerationFrame frame,
            JoinInput step,
            BoundExpression predicate,
            RecordCursorFactory base,
            int producerLo,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final Function filter;
        try {
            filter = functionInstantiator.instantiate(predicate, step.getOutput(), base.getMetadata(), executionContext);
        } catch (Throwable th) {
            Misc.free(base, th);
            throw th;
        }
        return generatePostJoin(frame, step, predicate, base, filter, producerLo, executionContext);
    }

    /**
     * Filters the factory, which holds the decorrelation domains the generator placed from {@code producerLo} on, on one
     * thread with the filter the join step applies to its joined rows, folding a constant as
     * {@link PlanToFactoryRules#isPostJoinFilterFolded} decides. Consumes both executable roots on entry, including on
     * failure.
     */
    RecordCursorFactory generatePostJoin(
            GenerationFrame frame,
            JoinInput step,
            BoundExpression predicate,
            RecordCursorFactory base,
            Function filter,
            int producerLo,
            SqlExecutionContext executionContext
    ) throws SqlException {
        return generate(frame, predicate, step.getOutput(), base, filter, producerLo, executionContext, false, false, null,
                false, PlanToFactoryRules.isPostJoinFilterFolded(step, predicate));
    }

    /**
     * The LIMIT count a parallel filter over the scan with output {@code input} stops at; over the master scan of a
     * MASTER_SUFFIX LIMIT, the count that LIMIT sets, recorded on the frame.
     */
    Function instantiateLimit(GenerationFrame frame, BoundExpression limitCount, OutputSchema input, SqlExecutionContext executionContext)
            throws SqlException {
        final Function limit = functionInstantiator.instantiate(limitCount, input, executionContext);
        if (frame.masterSuffixScan == null || frame.masterSuffixScan.getOutput() != input) {
            return limit;
        }
        return frame.masterSuffixLimit = new MasterSuffixLimitFunction(limit, configuration.getSqlMaxNegativeLimit());
    }

    /**
     * Builds the filter of a symbol pattern over the key set of its column, and over the residual of the other
     * conjuncts of the scan, if any. Returns null when the pattern does not compile to a symbol key-set provider.
     */
    @Nullable AdaptiveSymbolPatternRecordCursorFactory.PreparedSymbolPatternFilter instantiatePatternFilter(
            GenerationFrame frame,
            FunctionExpression pattern,
            @Nullable BoundExpression residual,
            OutputSchema input,
            RecordMetadata metadata,
            SqlExecutionContext executionContext
    ) throws SqlException {
        Function provider = null;
        Function residualFilter = null;
        try {
            provider = positivePattern(frame, pattern, input, metadata, executionContext);
            if (!(provider instanceof SymbolKeySetProvider)) {
                Misc.free(provider);
                return null;
            }
            if (residual != null) {
                residualFilter = functionInstantiator.instantiate(residual, input, metadata, executionContext);
            }
            final FunctionExpression positive = pattern.getArgumentCount() == 1 ? (FunctionExpression) pattern.argumentAt(0) : pattern;
            final int keyIndex = input.getColumnIndexById(((ColumnExpression) positive.argumentAt(0)).getColumnId());
            final AdaptiveSymbolPatternRecordCursorFactory.PreparedSymbolPatternFilter filter =
                    new AdaptiveSymbolPatternRecordCursorFactory.PreparedSymbolPatternFilter(provider, residualFilter, isNegated(pattern), keyIndex);
            provider = null;
            residualFilter = null;
            return filter;
        } catch (Throwable th) {
            Misc.free(provider, th);
            Misc.free(residualFilter, th);
            throw th;
        }
    }

    /**
     * Builds one clone of the symbol pattern filter per worker, in its shape, so that the filter donates its prepared
     * key set and its sub-query values to the clones. Returns null when the filter is thread-safe.
     */
    @Nullable ObjList<Function> instantiatePatternWorkers(
            GenerationFrame frame,
            FunctionExpression pattern,
            @Nullable BoundExpression residual,
            OutputSchema input,
            RecordMetadata metadata,
            Function filter,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (filter.isThreadSafe()) {
            return null;
        }
        final int workerCount = executionContext.getSharedQueryWorkerCount();
        final ObjList<Function> workers = new ObjList<>(workerCount);
        functionInstantiator.beginWorkerClones();
        try {
            for (int i = 0; i < workerCount; i++) {
                workers.add(instantiatePatternFilter(frame, pattern, residual, input, metadata, executionContext));
            }
            return workers;
        } catch (Throwable th) {
            Misc.freeObjList(workers, th);
            throw th;
        } finally {
            functionInstantiator.endWorkerClones();
        }
    }

    /**
     * Fills the prepared filter's column set, its JIT filter where the prepared filter allows one, with the bind
     * variable memory the consumer reads them from, and its per-worker clones in the shape of its filter.
     */
    void prepareParallel(GenerationFrame frame, PreparedFilter target, RecordCursorFactory leaf, SqlExecutionContext executionContext)
            throws SqlException {
        final BoundExpression predicate = target.getPredicate();
        final OutputSchema input = target.getInput();
        final IntHashSet columns = new IntHashSet();
        collectColumnIndexes(predicate, input, columns);
        target.setColumns(columns);
        if (target.isJitAllowed() && tryCompileJit(frame, target, leaf, predicate, input, leaf.getMetadata(), null, null, executionContext)) {
            final CompiledCountOnlyFilter countOnlyFilter = target.getCompiledCountOnlyFilter();
            target.setJit(target.getCompiledFilter(), null, target.getBindVarFunctions());
            Misc.free(countOnlyFilter);
            target.setBindVarMemory(Vm.getCARWInstance(
                    configuration.getSqlJitBindVarsMemoryPageSize(),
                    configuration.getSqlJitBindVarsMemoryMaxPages(),
                    MemoryTag.NATIVE_JIT
            ));
        }
        final FunctionExpression pattern = target.getPattern();
        target.setWorkers(pattern == null
                ? functionInstantiator.instantiateWorkers(predicate, input, leaf.getMetadata(), target.getFilter(),
                executionContext.getSharedQueryWorkerCount(), executionContext)
                : instantiatePatternWorkers(frame, pattern, target.getPatternResidual(), input, leaf.getMetadata(),
                target.getFilter(), executionContext));
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
            boolean isUpdate,
            boolean enablePreTouch,
            @Nullable BoundExpression limitCount
    ) throws SqlException {
        if (!isJitApplicable(isUpdate, executionContext)) {
            return null;
        }
        final PreparedFilter jit = frame.pushPreparedFilter();
        final boolean isCompiled;
        try {
            isCompiled = tryCompileJit(frame, jit, base, predicate, input, base.getMetadata(), null, null, executionContext);
        } catch (Throwable th) {
            frame.popPreparedFilter(th);
            Misc.free(filter, th);
            Misc.free(base, th);
            throw th;
        }
        if (!isCompiled) {
            frame.popPreparedFilter();
            return null;
        }
        LOG.debug().$("JIT enabled for (sub)query [fd=").$(executionContext.getRequestFd()).I$();
        jit.of(predicate, input, filter, true);
        jit.setColumns(columns);
        Function limit = null;
        final int limitPosition;
        final AsyncFilterResources resources;
        try {
            if (limitCount != null) {
                limit = instantiateLimit(frame, limitCount, input, executionContext);
                limitPosition = limitCount.getPosition();
            } else {
                limitPosition = 0;
            }
            jit.setWorkers(functionInstantiator.instantiateWorkers(predicate, input, base.getMetadata(), filter,
                    executionContext.getSharedQueryWorkerCount(), executionContext));
            resources = jit.adopt();
        } catch (Throwable th) {
            Misc.free(limit, th);
            frame.popPreparedFilter(th);
            Misc.free(base, th);
            throw th;
        }
        frame.popPreparedFilter();
        return new AsyncJitFilteredRecordCursorFactory(
                executionContext.getCairoEngine(), configuration, executionContext.getMessageBus(),
                base, resources, reduceTaskFactory, limit, limitPosition,
                executionContext.getSharedQueryWorkerCount(), enablePreTouch
        );
    }

    private static final class ReaderSymbolTables implements SymbolTableSource, Mutable {
        private IntList columnIndexes;
        private TableReader reader;

        @Override
        public void clear() {
            columnIndexes = null;
            reader = null;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return reader.getSymbolTable(columnIndexes.getQuick(columnIndex));
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return reader.newSymbolTable(columnIndexes.getQuick(columnIndex));
        }

        ReaderSymbolTables of(TableReader reader, IntList columnIndexes) {
            this.reader = reader;
            this.columnIndexes = columnIndexes;
            return this;
        }
    }
}

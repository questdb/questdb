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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.ImplicitCastException;
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
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.plan.logical.BindVariableExpression;
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
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.*;

final class FilterFactoryGenerator {
    private static final Log LOG = LogFactory.getLog(FilterFactoryGenerator.class);
    private final CharacterStore characterStore;
    private final CairoConfiguration configuration;
    private final boolean enableJitDebug;
    private final ObjectPool<ExpressionNode> expressionNodePool;
    private final MemoryCARW jitIRMem;
    private final CompiledFilterIRSerializer jitIRSerializer = new CompiledFilterIRSerializer();
    private final StringSink jitText;
    private final PageFrameReduceTaskFactory reduceTaskFactory;
    private boolean enableJitNullChecks = true;

    FilterFactoryGenerator(
            CairoConfiguration configuration,
            ObjectPool<ExpressionNode> expressionNodePool,
            CharacterStore characterStore,
            MemoryCARW jitIRMem,
            PageFrameReduceTaskFactory reduceTaskFactory,
            StringSink jitText
    ) {
        this.configuration = configuration;
        this.expressionNodePool = expressionNodePool;
        this.characterStore = characterStore;
        this.jitIRMem = jitIRMem;
        this.reduceTaskFactory = reduceTaskFactory;
        this.jitText = jitText;
        this.enableJitDebug = configuration.isSqlJitDebugEnabled();
    }

    void setEnableJitNullChecks(boolean value) {
        enableJitNullChecks = value;
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

    /** Returns the residual predicate a parallel filter of this frame compiled, or null. */
    static BoundExpression getParallelPredicate(GenerationFrame frame, RecordCursorFactory factory) {
        for (int i = 0, n = frame.parallelFilterFactories.size(); i < n; i++) {
            if (frame.parallelFilterFactories.getQuick(i) == factory) {
                return frame.parallelFilterPredicates.getQuick(i);
            }
        }
        return null;
    }

    /** Consumes both executable roots on entry, including on failure. */
    RecordCursorFactory generate(
            GenerationFrame frame,
            BoundExpression predicate,
            OutputSchema input,
            RecordCursorFactory base,
            Function filter,
            FunctionBinder binder,
            SqlExecutionContext executionContext,
            boolean isUpdate
    ) throws SqlException {
        return generate(frame, predicate, input, base, filter, binder, executionContext, isUpdate, null, false);
    }

    /** Like a join-level filter, keeps a constant folded from functions as a filter; only a literal constant folds. */
    RecordCursorFactory generatePostJoin(
            GenerationFrame frame,
            BoundExpression predicate,
            OutputSchema input,
            RecordCursorFactory base,
            Function filter,
            FunctionBinder binder,
            SqlExecutionContext executionContext
    ) throws SqlException {
        return generate(frame, predicate, input, base, filter, binder, executionContext, false, null, false,
                predicate instanceof ConstantExpression constant && constant.isLiteral());
    }

    RecordCursorFactory generate(
            GenerationFrame frame,
            BoundExpression predicate,
            OutputSchema input,
            RecordCursorFactory base,
            Function filter,
            FunctionBinder binder,
            SqlExecutionContext executionContext,
            boolean isUpdate,
            LimitPlan limitAdvice,
            boolean enablePreTouch
    ) throws SqlException {
        return generate(frame, predicate, input, base, filter, binder, executionContext, isUpdate, limitAdvice, enablePreTouch, true);
    }

    private RecordCursorFactory generate(
            GenerationFrame frame,
            BoundExpression predicate,
            OutputSchema input,
            RecordCursorFactory base,
            Function filter,
            FunctionBinder binder,
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
                    predicate, input, binder, isUpdate, enablePreTouch, limitAdvice
            );
            if (jitFactory != null) {
                isAdopted = true;
                return addParallel(frame, jitFactory, predicate);
            }
            if (limitAdvice != null && limitAdvice.getHi() == null) {
                limit = binder.instantiate(limitAdvice.getLo(), input, executionContext);
            }
            workerFilters = compileWorkers(predicate, input, base.getMetadata(), filter, binder, executionContext);
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

    /** Consumes the covering factory and filter, including on failure. */
    RecordCursorFactory generateCovering(
            BoundExpression predicate, OutputSchema input, CoveringIndexRecordCursorFactory base, Function filter,
            FunctionBinder binder, SqlExecutionContext executionContext, LimitPlan limitAdvice, boolean enablePreTouch
    ) throws SqlException {
        Function limit = null;
        ObjList<Function> workers = null;
        boolean isAdopted = false;
        try {
            boolean isParallel = canUseParallelFilter(base, executionContext);
            IntHashSet columns = null;
            if (isParallel) {
                if (limitAdvice != null && limitAdvice.getHi() == null) {
                    limit = binder.instantiate(limitAdvice.getLo(), input, executionContext);
                }
                isParallel = canUseParallelCoveringFilter(base, limit, executionContext);
                if (isParallel) {
                    columns = new IntHashSet();
                    collectColumnIndexes(predicate, input, columns);
                    workers = compileWorkers(predicate, input, base.getMetadata(), filter, binder, executionContext);
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

    private static RecordCursorFactory addParallel(GenerationFrame frame, RecordCursorFactory factory, BoundExpression predicate) {
        frame.parallelFilterFactories.add(factory);
        frame.parallelFilterPredicates.add(predicate);
        return factory;
    }

    static ObjList<Function> compileWorkers(
            BoundExpression predicate,
            OutputSchema input,
            RecordMetadata metadata,
            Function filter,
            FunctionBinder binder,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (filter.isThreadSafe()) {
            return null;
        }
        final int count = executionContext.getSharedQueryWorkerCount();
        final ObjList<Function> workers = new ObjList<>(count);
        binder.beginWorkerClones();
        try {
            for (int i = 0; i < count; i++) {
                workers.add(binder.instantiate(predicate, input, metadata, executionContext));
            }
            return workers;
        } catch (Throwable th) {
            Misc.freeObjList(workers, th);
            throw th;
        } finally {
            binder.endWorkerClones();
        }
    }

    static boolean canUseParallelFilter(RecordCursorFactory base, SqlExecutionContext executionContext) {
        return executionContext.isParallelFilterEnabled() && base.supportsPageFrameCursor();
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
     * A timestamp constant is serialized at the precision of the operand it meets; text the binder
     * could not convert to that operand's type stays with the Java comparison.
     */
    private static boolean hasJitCompatibleTimestampConstants(FunctionExpression call) {
        int timestampType = ColumnType.UNDEFINED;
        for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
            final BoundExpression argument = call.argumentAt(i);
            if (!(argument instanceof ConstantExpression) && ColumnType.isTimestamp(argument.getDataType())) {
                timestampType = argument.getDataType();
                break;
            }
        }
        for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
            if (call.argumentAt(i) instanceof ConstantExpression constant) {
                if (timestampType == ColumnType.UNDEFINED) {
                    if (ColumnType.isTimestamp(constant.getDataType())) {
                        return false;
                    }
                    continue;
                }
                final int type = constant.getDataType();
                if (ColumnType.isVarcharOrString(type) && (!"in".equals(call.getName()) || call.getArgumentCount() == 1
                        || call.getArgumentCount() > 2 && !isJitTimestampText(constant, timestampType))) {
                    return false;
                }
                if (ColumnType.isTimestamp(type) && type != timestampType && (constant.getTimestampText() == null
                        || FunctionParser.getAdaptiveTimestampType(constant.getTimestampText(), timestampType) != timestampType
                        || !isTimestampConvertible(constant.getLongValue(), type, timestampType))) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean isJitOperator(String name) {
        return switch (name) {
            case "=", "!=", "<>", "<", "<=", ">", ">=", "+", "-", "*", "/", "%", "and", "or" -> true;
            default -> false;
        };
    }

    private static boolean isJitTimestampText(ConstantExpression constant, int timestampType) {
        final CharSequence text = constant.getDataType() == ColumnType.VARCHAR
                ? constant.getVarcharValue() == null ? null : constant.getVarcharValue().asAsciiCharSequence()
                : constant.getStrValue();
        if (text == null || FunctionParser.getAdaptiveTimestampType(text, timestampType) != timestampType) {
            return false;
        }
        try {
            ColumnType.getTimestampDriver(timestampType).parseFloorLiteral(text);
            return true;
        } catch (NumericException e) {
            return false;
        }
    }

    private static boolean isTimestampConvertible(long value, int fromType, int toType) {
        try {
            ColumnType.getTimestampDriver(toType).from(value, fromType);
            return true;
        } catch (ImplicitCastException e) {
            return false;
        }
    }

    private @Nullable CharSequence jitConstantToken(ConstantExpression constant) {
        final int type = constant.getDataType();
        final long longValue = constant.getLongValue();
        return switch (ColumnType.tagOf(type)) {
            case ColumnType.NULL -> "null";
            case ColumnType.BOOLEAN -> Boolean.toString(longValue != 0);
            case ColumnType.INT -> longValue == Numbers.INT_NULL ? "null" : numberToken((int) longValue, (char) 0);
            case ColumnType.LONG -> longValue == Numbers.LONG_NULL ? "null" : numberToken(longValue, 'L');
            case ColumnType.DOUBLE -> {
                final double value = constant.getDoubleValue();
                if (!Double.isFinite(value)) {
                    yield null;
                }
                final CharacterStoreEntry token = characterStore.newEntry();
                token.put(value);
                yield token.toImmutable();
            }
            case ColumnType.FLOAT -> {
                final float value = constant.getFloatValue();
                if (!Float.isFinite(value)) {
                    yield null;
                }
                final CharacterStoreEntry token = characterStore.newEntry();
                token.put(value).put('f');
                yield token.toImmutable();
            }
            case ColumnType.GEOBYTE, ColumnType.GEOSHORT, ColumnType.GEOINT, ColumnType.GEOLONG -> {
                if (longValue == GeoHashes.NULL) {
                    yield "null";
                }
                final CharacterStoreEntry token = characterStore.newEntry();
                token.put("##");
                for (int i = ColumnType.getGeoHashBits(type) - 1; i >= 0; i--) {
                    token.put((longValue >>> i & 1) == 0 ? '0' : '1');
                }
                yield token.toImmutable();
            }
            case ColumnType.CHAR -> {
                if (longValue == 0) {
                    yield null;
                }
                jitText.clear();
                jitText.put((char) longValue);
                yield quoted(jitText);
            }
            case ColumnType.STRING -> constant.getStrValue() == null ? "null" : quoted(constant.getStrValue());
            case ColumnType.VARCHAR -> constant.getVarcharValue() == null ? "null" : quoted(utf16(constant.getVarcharValue()));
            case ColumnType.DATE -> {
                if (longValue == Numbers.LONG_NULL) {
                    yield "null";
                }
                jitText.clear();
                DateFormatUtils.appendDateTime(jitText, longValue);
                yield quoted(jitText);
            }
            case ColumnType.TIMESTAMP -> constant.getTimestampText() != null ? quoted(constant.getTimestampText())
                    : longValue == Numbers.LONG_NULL ? "null" : numberToken(longValue, 'L');
            default -> null;
        };
    }

    private CharSequence numberToken(long value, char suffix) {
        final CharacterStoreEntry token = characterStore.newEntry();
        token.put(value);
        if (suffix != 0) {
            token.put(suffix);
        }
        return token.toImmutable();
    }

    private CharSequence quoted(CharSequence value) {
        final CharacterStoreEntry token = characterStore.newEntry();
        token.put('\'');
        for (int i = 0, n = value.length(); i < n; i++) {
            final char c = value.charAt(i);
            if (c == '\'') {
                token.put('\'');
            }
            token.put(c);
        }
        token.put('\'');
        return token.toImmutable();
    }

    private CharSequence utf16(Utf8Sequence value) {
        jitText.clear();
        Utf8s.utf8ToUtf16(value, jitText);
        return jitText;
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
            FunctionBinder binder,
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
                    jitIRSerializer.of(jitIRMem, executionContext, base.getMetadata(), cursor, bindVarFunctions);
                    final ExpressionNode expression = jitExpression(predicate, input, base.getMetadata());
                    if (expression == null) {
                        return null;
                    }
                    jitOptions = jitIRSerializer.serialize(expression, forceScalar, enableJitDebug, enableJitNullChecks);
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
                limit = binder.instantiate(limitAdvice.getLo(), input, executionContext);
                limitPosition = limitAdvice.getLo().getPosition();
            } else {
                limitPosition = 0;
            }
            LOG.debug().$("JIT enabled for (sub)query [fd=").$(executionContext.getRequestFd()).I$();
            workers = FilterFactoryGenerator.compileWorkers(predicate, input, base.getMetadata(), filter, binder, executionContext);
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

    /**
     * Spells a bound predicate as the parser would have, so the JIT serializer applies one set of
     * width and NULL rules to both planners. Returns null for shapes no plain SQL literal expresses.
     */
    private @Nullable ExpressionNode jitExpression(BoundExpression expression, OutputSchema input, RecordMetadata metadata) {
        if (expression instanceof ColumnExpression column) {
            final int index = input.getColumnIndexById(column.getColumnId());
            if (!column.isDirectReference() || index < 0 || metadata.getColumnIndexQuiet(metadata.getColumnName(index)) != index) {
                return null;
            }
            return expressionNodePool.next().of(ExpressionNode.LITERAL, metadata.getColumnName(index), 0, column.getPosition());
        }
        if (expression instanceof ConstantExpression constant) {
            if (constant.getSource() != null) {
                return jitExpression(constant.getSource(), input, metadata);
            }
            final CharSequence token = constant.isLiteral() ? jitConstantToken(constant) : null;
            return token == null ? null : expressionNodePool.next().of(ExpressionNode.CONSTANT, token, 0, constant.getPosition());
        }
        if (expression instanceof BindVariableExpression parameter) {
            return !parameter.isDirectReference() ? null
                    : expressionNodePool.next().of(ExpressionNode.BIND_VARIABLE, parameter.getName(), 0, parameter.getPosition());
        }
        if (!(expression instanceof FunctionExpression call)) {
            return null;
        }
        final String name = call.getName();
        final int count = call.getArgumentCount();
        if (count == 2 && SqlKeywords.isInKeyword(name) && !(call.argumentAt(0) instanceof ConstantExpression)
                && ColumnType.isTimestamp(call.argumentAt(0).getDataType())
                && call.argumentAt(1) instanceof ConstantExpression interval && interval.isLiteral()
                && ColumnType.isVarcharOrString(interval.getDataType())) {
            final CharSequence text = ColumnType.isVarchar(interval.getDataType())
                    ? (interval.getVarcharValue() == null ? null : utf16(interval.getVarcharValue())) : interval.getStrValue();
            final CharSequence token = text == null ? null : quoted(text);
            final ExpressionNode in = expressionNodePool.next().of(ExpressionNode.SET_OPERATION, "in", 0, call.getPosition());
            in.paramCount = 2;
            in.lhs = jitExpression(call.argumentAt(0), input, metadata);
            in.rhs = token == null ? null : expressionNodePool.next().of(ExpressionNode.CONSTANT, token, 0, interval.getPosition());
            return in.lhs == null || in.rhs == null ? null : in;
        }
        if (count >= 2 && !hasJitCompatibleTimestampConstants(call)) {
            return null;
        }
        if (SqlKeywords.isInKeyword(name) && count >= 2) {
            final ExpressionNode in = expressionNodePool.next().of(call.isSetOperation() ? ExpressionNode.SET_OPERATION : ExpressionNode.FUNCTION,
                    "in", 0, call.getPosition());
            in.paramCount = count;
            if (count == 2) {
                in.lhs = jitExpression(call.argumentAt(0), input, metadata);
                in.rhs = jitExpression(call.argumentAt(1), input, metadata);
                return in.lhs == null || in.rhs == null ? null : in;
            }
            for (int i = count - 1; i >= 0; i--) {
                final ExpressionNode argument = jitExpression(call.argumentAt(i == 0 ? 0 : count - i), input, metadata);
                if (argument == null) {
                    return null;
                }
                in.args.add(argument);
            }
            return in;
        }
        final boolean isUnary = count == 1 && ("-".equals(name) || SqlKeywords.isNotKeyword(name));
        if (!isUnary && (count != 2 || !isJitOperator(name))) {
            return null;
        }
        final ExpressionNode node = expressionNodePool.next().of(ExpressionNode.OPERATION,
                "<>".equals(name) ? "!=" : name, 0, call.getPosition());
        node.paramCount = count;
        if (isUnary) {
            node.rhs = jitExpression(call.argumentAt(0), input, metadata);
            return node.rhs == null ? null : node;
        }
        node.lhs = jitExpression(call.argumentAt(0), input, metadata);
        node.rhs = jitExpression(call.argumentAt(1), input, metadata);
        return node.lhs == null || node.rhs == null ? null : node;
    }
}

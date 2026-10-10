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

import io.questdb.ParanoiaState;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.SampleBySortStrategy;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SingleSymbolFilter;
import io.questdb.griffin.FunctionInstantiator;
import io.questdb.griffin.FunctionResolver;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.griffin.engine.functions.groupby.InterpolationGroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.SampleByFillNoneNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillNoneRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillValueNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFirstLastRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByInterpolateRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByUtcBoundFunction;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.griffin.engine.orderby.EncodedSortLightRecordCursorFactory;
import io.questdb.griffin.engine.orderby.EncodedSortRecordCursorFactory;
import io.questdb.griffin.engine.orderby.RecordComparatorCompiler;
import io.questdb.griffin.engine.orderby.SortKeyEncoder;
import io.questdb.griffin.engine.orderby.SortedLightRecordCursorFactory;
import io.questdb.griffin.engine.orderby.SortedRecordCursorFactory;
import io.questdb.griffin.engine.table.DeferredSingleSymbolFilterPageFrameRecordCursorFactory;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.ColumnType.*;
import static io.questdb.griffin.SqlKeywords.*;

/**
 * Builds SAMPLE BY factories and the FILL stage that completes their missing buckets.
 */
final class SampleByFactoryGenerator {
    private final BytecodeAssembler asm;
    private final SqlCodeGenerator codeGenerator;
    private final IntList columnIndexes = new IntList();
    private final CairoConfiguration configuration;
    private final EntityColumnFilter entityColumnFilter;
    private final IntList firstLastKinds = new IntList();
    private final IntList firstLastPositions = new IntList();
    private final FunctionResolver functionResolver;
    private final IntList groupByFunctionPositions = new IntList();
    private final RecordComparatorCompiler recordComparatorCompiler;

    SampleByFactoryGenerator(
            CairoConfiguration configuration,
            SqlCodeGenerator codeGenerator,
            FunctionResolver functionResolver,
            BytecodeAssembler asm,
            EntityColumnFilter entityColumnFilter,
            RecordComparatorCompiler recordComparatorCompiler
    ) {
        this.configuration = configuration;
        this.codeGenerator = codeGenerator;
        this.functionResolver = functionResolver;
        this.asm = asm;
        this.entityColumnFilter = entityColumnFilter;
        this.recordComparatorCompiler = recordComparatorCompiler;
    }

    private static void assignUndefinedType(Function func, int type, SqlExecutionContext context) throws SqlException {
        if (isUndefined(func.getType())) {
            func.assignType(type, context.getBindVariableService());
        }
    }

    @NotNull
    private static ObjList<Function> createSampleByFillPlaceholders(
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            @NotNull @Transient ObjList<CharSequence> fillTokens,
            ObjList<Function> fillConstants
    ) {
        final ObjList<Function> placeholderFunctions = new ObjList<>();
        int fillIndex = 0;
        final int fillValueCount = fillTokens.size();
        for (int i = 0, n = recordFunctions.size(); i < n; i++) {
            Function function = recordFunctions.getQuick(i);
            if (function instanceof GroupByFunction) {
                assert fillIndex < fillValueCount;
                final CharSequence fillToken = fillTokens.getQuick(fillIndex++);
                if (isNullKeyword(fillToken)) {
                    final Function placeholder = GroupByUtils.nullFillConstant(function.getType());
                    assert placeholder != null;
                    placeholderFunctions.add(placeholder);
                } else if (isPrevKeyword(fillToken)) {
                    placeholderFunctions.add(function);
                } else if (isLinearKeyword(fillToken)) {
                    GroupByFunction interpolation = InterpolationGroupByFunction.newInstance((GroupByFunction) function);
                    placeholderFunctions.add(interpolation);
                    groupByFunctions.set(fillIndex - 1, interpolation);
                    recordFunctions.set(i, interpolation);
                } else {
                    placeholderFunctions.add(fillConstants.getQuick(fillIndex - 1));
                    fillConstants.setQuick(fillIndex - 1, null);
                }
            } else {
                placeholderFunctions.add(function);
            }
        }
        return placeholderFunctions;
    }

    /**
     * The SAMPLE BY factory the generator would pick from the built base, which order planning must have recorded.
     * Reads the base without converting it.
     */
    private static SampleByPlan.Algorithm generatorAlgorithm(
            RecordCursorFactory base,
            ObjList<CharSequence> fillTokens,
            boolean isFirstLastShape,
            int symbolKeyIndex,
            SqlExecutionContext executionContext
    ) {
        final int fillCount = fillTokens.size();
        if (fillCount == 1 && isLinearKeyword(fillTokens.getQuick(0))) {
            return SampleByPlan.Algorithm.INTERPOLATE;
        }
        if (isFirstLastShape && !base.hasParquetConvertedColumns(executionContext)
                && base instanceof DeferredSingleSymbolFilterPageFrameRecordCursorFactory indexScan) {
            final int symbolIndex = indexScan.getSymbolColumnIndex();
            if (IndexType.isBitmap(base.getMetadata().getColumnIndexType(symbolIndex)) && (symbolKeyIndex == -1 || symbolIndex == symbolKeyIndex)) {
                return SampleByPlan.Algorithm.FIRST_LAST_INDEX;
            }
        }
        return fillCount == 0 || fillCount == 1 && isNoneKeyword(fillTokens.getQuick(0))
                ? SampleByPlan.Algorithm.FILL_NONE : SampleByPlan.Algorithm.FILL_VALUE;
    }

    /**
     * Consumes the function: a constant bound converts now, a runtime-constant one converts once it has its value.
     */
    private static Function toSampleByUtc(Function function, TimestampDriver driver, TimeZoneRules rules, int timestampType) {
        if (function == driver.getTimestampConstantNull()) {
            return function;
        }
        if (!function.isConstant()) {
            return new SampleByUtcBoundFunction(function, rules, timestampType);
        }
        final long timestamp = driver.from(function.getTimestamp(null), ColumnType.getTimestampType(function.getType()));
        if (timestamp == Numbers.LONG_NULL) {
            return function;
        }
        Misc.free(function);
        return TimestampConstant.newInstance(driver.toUTC(timestamp, rules), timestampType);
    }

    /**
     * Consumes the input and all function lists/parameters, including on failure.
     */
    private RecordCursorFactory generateFillFactory(
            GenerationFrame frame,
            FillPlan.Algorithm algorithm,
            RecordCursorFactory groupByFactory,
            int timestampIndex,
            int timestampType,
            long samplingInterval,
            char samplingIntervalUnit,
            TimestampSampler timestampSampler,
            IntList fillModes,
            ObjList<Function> constantFills,
            ObjList<Function> fillValues,
            Function fillFromFunc,
            Function fillToFunc,
            int fillToPosition,
            Function offsetFunc,
            int offsetFuncPos,
            Function tzFunc,
            int tzFuncPos
    ) throws SqlException {
        boolean isTransferred = false;
        try {
            if (algorithm == null) {
                throw new IllegalStateException("fill algorithm is not planned");
            }
            final RecordMetadata groupByMetadata = groupByFactory.getMetadata();
            final int columnCount = groupByMetadata.getColumnCount();
            // A SAMPLE BY cursor that keeps its latest rows readable serves the fill
            // its keys and PREV values directly.
            final boolean isSampleBySource = algorithm == FillPlan.Algorithm.SAMPLE_BY_ROWS;
            if (ParanoiaState.PLAN_PARANOIA_MODE && (isSampleBySource != (groupByFactory instanceof SampleByFillNoneRecordCursorFactory
                    || groupByFactory instanceof SampleByFillNoneNotKeyedRecordCursorFactory)
                    || (algorithm == FillPlan.Algorithm.SORTED) != (groupByFactory.getMetadata().getTimestampIndex() != timestampIndex))) {
                throw new AssertionError("recorded fill algorithm differs from the generator's choice");
            }
            final IntList keyColIndices = new IntList();
            for (int col = 0; col < columnCount; col++) {
                if (fillModes.getQuick(col) == SampleByFillRecordCursorFactory.FILL_KEY) {
                    keyColIndices.add(col);
                }
            }

            // SYMBOL columns are stored as INT in the keysMap.
            final ArrayColumnTypes mapKeyTypes = new ArrayColumnTypes();
            for (int i = 0, n = keyColIndices.size(); i < n; i++) {
                int col = keyColIndices.getQuick(i);
                int colType = groupByMetadata.getColumnType(col);
                if (ColumnType.tagOf(colType) == ColumnType.SYMBOL) {
                    mapKeyTypes.add(ColumnType.INT);
                } else {
                    mapKeyTypes.add(colType);
                }
            }

            // populateMapValueTypes owns the fixed-width value header layout so
            // slot indices stay in sync with the cursor's constants.
            final ArrayColumnTypes mapValueTypes = new ArrayColumnTypes();
            SampleByFillRecordCursorFactory.populateMapValueTypes(mapValueTypes);

            // Cached prev-value slots, allocated per unique non-key FILL_PREV
            // source col with slot-eligible type:
            // - fixedPrevSrcCols[i]  = source col index for slot i
            // - fixedPrevTypeTags[i] = source type tag (SYMBOL kept as SYMBOL so
            //                          the cursor resolves via getSymbolTable)
            // - prevValueSlot[outputCol] = slot index, or -1 if ineligible
            // - needsPrevPositioning = some FILL_PREV col still needs prevRecord
            //                          positioned via recordAt (variable-width
            //                          or multi-slot wide types).
            final IntList fixedPrevSrcCols = new IntList();
            final IntList fixedPrevTypeTags = new IntList();
            final IntList prevValueSlot = new IntList(columnCount);
            for (int col = 0; col < columnCount; col++) {
                prevValueSlot.add(-1);
            }
            boolean needsPrevPositioning = false;
            // Cached fixed-size PREV slots live in the keysMap value section for
            // keyed queries and in a single SimpleMapValue for non-keyed queries
            // (allocated by the cursor when keysMap is null). The slot layout is
            // identical across both cases, so the cursor reads through a uniform
            // Record-typed prevCacheRecord regardless of mode.
            for (int col = 0; col < columnCount; col++) {
                int mode = fillModes.getQuick(col);
                if (mode != SampleByFillRecordCursorFactory.FILL_PREV_SELF && mode < 0) {
                    continue;
                }
                int srcCol = (mode == SampleByFillRecordCursorFactory.FILL_PREV_SELF) ? col : mode;
                // Source is a key column -- read goes through keysMapRecord directly.
                if (fillModes.getQuick(srcCol) == SampleByFillRecordCursorFactory.FILL_KEY) {
                    continue;
                }
                int srcType = groupByMetadata.getColumnType(srcCol);
                int srcTag = ColumnType.tagOf(srcType);
                // Multi-slot wide types and variable-width fall back to recordAt; a
                // SAMPLE BY source serves every PREV value from its own rows.
                if (isSampleBySource || !SampleByFillRecordCursorFactory.isPrevSlotEligible(srcTag)) {
                    needsPrevPositioning = true;
                    continue;
                }
                // Deduplicate slots per source col.
                int slot = -1;
                for (int j = 0, n = fixedPrevSrcCols.size(); j < n; j++) {
                    if (fixedPrevSrcCols.getQuick(j) == srcCol) {
                        slot = mapValueTypes.getColumnCount() - fixedPrevSrcCols.size() + j;
                        break;
                    }
                }
                if (slot < 0) {
                    slot = mapValueTypes.getColumnCount();
                    fixedPrevSrcCols.add(srcCol);
                    fixedPrevTypeTags.add(srcTag);
                    mapValueTypes.add(srcTag == ColumnType.SYMBOL ? ColumnType.INT : srcType);
                }
                prevValueSlot.setQuick(col, slot);
            }

            RecordSink keySink = null;
            if (keyColIndices.size() > 0 && !isSampleBySource) {
                final ListColumnFilter keyColFilter = new ListColumnFilter();
                for (int i = 0, n = keyColIndices.size(); i < n; i++) {
                    keyColFilter.add(keyColIndices.getQuick(i) + 1); // 1-based
                }
                keySink = RecordSinkFactory.getInstance(
                        configuration, asm, groupByMetadata, keyColFilter
                );
            }

            // Maps every map column (values then keys) to the base SYMBOL col it
            // resolves against, or -1 if not a SYMBOL. Header slots and non-SYMBOL
            // slots are -1; SYMBOL prev-value slots and SYMBOL key columns carry
            // the source col index so MapRecord resolves via the right base column.
            final IntList symbolTableColIndices = new IntList();
            if (isSampleBySource) {
                // Gap rows read the source row at the output positions.
                for (int col = 0; col < columnCount; col++) {
                    symbolTableColIndices.add(ColumnType.tagOf(groupByMetadata.getColumnType(col)) == ColumnType.SYMBOL ? col : -1);
                }
            } else {
                for (int i = 0, n = mapValueTypes.getColumnCount() - fixedPrevSrcCols.size(); i < n; i++) {
                    symbolTableColIndices.add(-1); // header slots (KEY_INDEX, HAS_PREV, PREV_ROWID)
                }
                for (int i = 0, n = fixedPrevSrcCols.size(); i < n; i++) {
                    if (fixedPrevTypeTags.getQuick(i) == ColumnType.SYMBOL) {
                        symbolTableColIndices.add(fixedPrevSrcCols.getQuick(i));
                    } else {
                        symbolTableColIndices.add(-1);
                    }
                }
                for (int i = 0, n = keyColIndices.size(); i < n; i++) {
                    int col = keyColIndices.getQuick(i);
                    if (ColumnType.tagOf(groupByMetadata.getColumnType(col)) == ColumnType.SYMBOL) {
                        symbolTableColIndices.add(col);
                    } else {
                        symbolTableColIndices.add(-1);
                    }
                }
            }

            // The fill cursor requires input sorted by timestamp, and a group-by
            // factory emits buckets in hash order, so sort here. A SAMPLE BY cursor
            // already emits buckets in timestamp order.
            // Strategy comes from cairo.sql.sampleby.fill.sort.strategy; startup
            // validation makes the switch exhaustive.
            if (algorithm == FillPlan.Algorithm.SORTED) {
                final RecordMetadata sortMetadata = groupByFactory.getMetadata();
                final ListColumnFilter listColumnFilterA = frame.listColumnFilterA;
                listColumnFilterA.clear();
                listColumnFilterA.add(timestampIndex + 1); // positive = ascending
                entityColumnFilter.of(sortMetadata.getColumnCount());
                final int sortStrategy = configuration.getSampleByFillSortStrategy();
                switch (sortStrategy) {
                    case SampleBySortStrategy.LIGHT_ENCODED -> {
                        assert SortKeyEncoder.isSupported(sortMetadata, listColumnFilterA)
                                && groupByFactory.recordCursorSupportsRandomAccess();
                        final ListColumnFilter filterCopy = listColumnFilterA.copy();
                        final RecordCursorFactory base = groupByFactory;
                        groupByFactory = null;
                        groupByFactory = new EncodedSortLightRecordCursorFactory(
                                configuration,
                                sortMetadata,
                                base,
                                filterCopy
                        );
                    }
                    case SampleBySortStrategy.FULL_ENCODED -> {
                        assert SortKeyEncoder.isSupported(sortMetadata, listColumnFilterA);
                        final RecordSink recordSink = RecordSinkFactory.getInstance(configuration, asm, sortMetadata, entityColumnFilter);
                        final ListColumnFilter filterCopy = listColumnFilterA.copy();
                        final RecordCursorFactory base = groupByFactory;
                        groupByFactory = null;
                        groupByFactory = new EncodedSortRecordCursorFactory(
                                configuration,
                                sortMetadata,
                                base,
                                recordSink,
                                filterCopy
                        );
                    }
                    case SampleBySortStrategy.LIGHT_RECORDCHAIN -> {
                        assert groupByFactory.recordCursorSupportsRandomAccess();
                        final RecordComparator comparator = recordComparatorCompiler.newInstance(sortMetadata, listColumnFilterA);
                        final ListColumnFilter filterCopy = listColumnFilterA.copy();
                        final RecordCursorFactory base = groupByFactory;
                        groupByFactory = null;
                        groupByFactory = new SortedLightRecordCursorFactory(
                                configuration,
                                sortMetadata,
                                base,
                                comparator,
                                filterCopy
                        );
                    }
                    case SampleBySortStrategy.FULL_RECORDCHAIN -> {
                        final RecordSink recordSink = RecordSinkFactory.getInstance(configuration, asm, sortMetadata, entityColumnFilter);
                        final RecordComparator comparator = recordComparatorCompiler.newInstance(sortMetadata, listColumnFilterA);
                        final ListColumnFilter filterCopy = listColumnFilterA.copy();
                        final RecordCursorFactory base = groupByFactory;
                        groupByFactory = null;
                        groupByFactory = new SortedRecordCursorFactory(
                                configuration,
                                sortMetadata,
                                base,
                                recordSink,
                                comparator,
                                filterCopy
                        );
                    }
                    default -> throw new IllegalStateException("unknown sample-by fill sort strategy: "
                            + SampleBySortStrategy.toString(sortStrategy));
                }
            }

            if (ParanoiaState.PLAN_PARANOIA_MODE && needsPrevPositioning && !isSampleBySource && !groupByFactory.recordCursorSupportsRandomAccess()) {
                throw new AssertionError("fill re-reads rows of a base without random access");
            }
            final GenericRecordMetadata fillMetadata = GenericRecordMetadata.copyOfNew(groupByFactory.getMetadata());
            fillMetadata.setTimestampIndex(timestampIndex);

            // Transferred slots were nulled in the per-column branch; this frees
            // any residual non-transferred fill functions. Detach each slot before
            // close so the outer rollback cannot retry a function whose close throws.
            final Throwable cleanupFailure = Misc.freeObjListBestEffort(null, fillValues);
            fillValues = null;
            CairoException.rethrowCleanupFailure(cleanupFailure);
            isTransferred = true;
            return new SampleByFillRecordCursorFactory(
                    configuration,
                    fillMetadata,
                    groupByFactory,
                    fillFromFunc,
                    fillToFunc,
                    fillToPosition,
                    samplingInterval,
                    samplingIntervalUnit,
                    timestampSampler,
                    fillModes,
                    constantFills,
                    timestampIndex,
                    timestampType,
                    keySink,
                    mapKeyTypes,
                    mapValueTypes,
                    keyColIndices,
                    symbolTableColIndices,
                    offsetFunc,
                    offsetFuncPos,
                    tzFunc,
                    tzFuncPos,
                    fixedPrevSrcCols,
                    fixedPrevTypeTags,
                    prevValueSlot,
                    needsPrevPositioning,
                    isSampleBySource
            );
        } catch (Throwable th) {
            if (!isTransferred) {
                Misc.freeObjList(fillValues, th);
                Misc.freeObjList(constantFills, th);
                Misc.free(fillFromFunc, th);
                if (fillToFunc != fillFromFunc) {
                    Misc.free(fillToFunc, th);
                }
                if (offsetFunc != fillFromFunc && offsetFunc != fillToFunc) {
                    Misc.free(offsetFunc, th);
                }
                if (tzFunc != fillFromFunc && tzFunc != fillToFunc && tzFunc != offsetFunc) {
                    Misc.free(tzFunc, th);
                }
                Misc.free(groupByFactory, th);
            }
            throw th;
        }
    }

    /**
     * Consumes the base, assembled functions and temporal parameters, including on failure.
     */
    private RecordCursorFactory generateSampleByFactory(
            GenerationFrame frame,
            SampleByPlan.Algorithm algorithm,
            RecordCursorFactory base,
            GenericRecordMetadata projectionMetadata,
            TimestampSampler timestampSampler,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            int timestampIndex,
            int timestampType,
            ObjList<CharSequence> fillTokens,
            ObjList<Function> fillConstants,
            IntList firstLastIndexes,
            boolean isFirstLastShape,
            @Nullable IntList firstLastKinds,
            @Nullable IntList firstLastPositions,
            IntList groupByFunctionPositions,
            int symbolKeyIndex,
            Function timezoneNameFunc,
            int timezoneNameFuncPos,
            Function offsetFunc,
            int offsetFuncPos,
            Function sampleFromFunc,
            int sampleFromFuncPos,
            Function sampleToFunc,
            int sampleToFuncPos,
            SqlExecutionContext executionContext
    ) throws SqlException {
        boolean isTransferred = false;
        try {
            if (algorithm == null) {
                throw new IllegalStateException("SAMPLE BY algorithm is not planned");
            }
            final RecordMetadata baseMetadata = base.getMetadata();
            final ArrayColumnTypes keyTypes = frame.keyTypes;
            final ArrayColumnTypes valueTypes = frame.valueTypes;
            final int fillCount = fillTokens.size();
            if (ParanoiaState.PLAN_PARANOIA_MODE
                    && algorithm != generatorAlgorithm(base, fillTokens, isFirstLastShape, symbolKeyIndex, executionContext)) {
                throw new AssertionError("recorded SAMPLE BY algorithm differs from the generator's choice");
            }
            if (algorithm == SampleByPlan.Algorithm.INTERPOLATE) {
                isTransferred = true;
                return new SampleByInterpolateRecordCursorFactory(
                        asm,
                        configuration,
                        base,
                        projectionMetadata,
                        groupByFunctions,
                        recordFunctions,
                        timestampSampler,
                        frame.listColumnFilterA,
                        keyTypes,
                        valueTypes,
                        entityColumnFilter,
                        groupByFunctionPositions,
                        timestampIndex,
                        timestampType,
                        timezoneNameFunc,
                        timezoneNameFuncPos,
                        offsetFunc,
                        offsetFuncPos,
                        sampleFromFunc,
                        sampleToFunc
                );
            }
            if (algorithm == SampleByPlan.Algorithm.FIRST_LAST_INDEX) {
                final SingleSymbolFilter symbolFilter = base.convertToSampleByIndexPageFrameCursorFactory();
                if (symbolFilter == null) {
                    throw new IllegalStateException("SAMPLE BY base does not read a symbol index");
                }
                final int searchPageSize = configuration.getSampleByIndexSearchPageSize();
                final ObjList<Function> records = recordFunctions;
                recordFunctions = null;
                GroupByUtils.freeAssembledProjectionFunctions(records, null);
                isTransferred = true;
                return new SampleByFirstLastRecordCursorFactory(
                        configuration, base, timestampSampler, projectionMetadata,
                        firstLastIndexes, firstLastKinds, firstLastPositions, baseMetadata,
                        timezoneNameFunc, timezoneNameFuncPos, offsetFunc, offsetFuncPos,
                        timestampIndex, symbolFilter, searchPageSize, sampleFromFunc,
                        sampleFromFuncPos, sampleToFunc, sampleToFuncPos
                );
            }
            if (algorithm == SampleByPlan.Algorithm.FILL_NONE) {
                if (keyTypes.getColumnCount() == 0) {
                    isTransferred = true;
                    return new SampleByFillNoneNotKeyedRecordCursorFactory(
                            asm,
                            configuration,
                            base,
                            timestampSampler,
                            projectionMetadata,
                            groupByFunctions,
                            recordFunctions,
                            valueTypes.getColumnCount(),
                            timestampIndex,
                            timestampType,
                            timezoneNameFunc,
                            timezoneNameFuncPos,
                            offsetFunc,
                            offsetFuncPos,
                            sampleFromFunc,
                            sampleFromFuncPos,
                            sampleToFunc,
                            sampleToFuncPos
                    );
                }

                isTransferred = true;
                return new SampleByFillNoneRecordCursorFactory(
                        asm,
                        configuration,
                        base,
                        projectionMetadata,
                        groupByFunctions,
                        recordFunctions,
                        timestampSampler,
                        frame.listColumnFilterA,
                        keyTypes,
                        valueTypes,
                        timestampIndex,
                        timestampType,
                        timezoneNameFunc,
                        timezoneNameFuncPos,
                        offsetFunc,
                        offsetFuncPos,
                        sampleFromFunc,
                        sampleFromFuncPos,
                        sampleToFunc,
                        sampleToFuncPos
                );
            }

            assert fillCount > 0 && keyTypes.getColumnCount() == 0;
            final ObjList<Function> placeholders = createSampleByFillPlaceholders(
                    groupByFunctions, recordFunctions, fillTokens, fillConstants
            );

            isTransferred = true;
            return new SampleByFillValueNotKeyedRecordCursorFactory(
                    asm,
                    configuration,
                    base,
                    timestampSampler,
                    placeholders,
                    projectionMetadata,
                    groupByFunctions,
                    recordFunctions,
                    valueTypes.getColumnCount(),
                    timestampIndex,
                    timestampType,
                    timezoneNameFunc,
                    timezoneNameFuncPos,
                    offsetFunc,
                    offsetFuncPos,
                    sampleFromFunc,
                    sampleFromFuncPos,
                    sampleToFunc,
                    sampleToFuncPos
            );
        } catch (Throwable th) {
            if (!isTransferred) {
                GroupByUtils.freeAssembledProjectionFunctions(recordFunctions, null, th);
                Misc.freeObjList(fillConstants, th);
                Misc.free(base, th);
                Misc.free(timezoneNameFunc, th);
                if (offsetFunc != timezoneNameFunc) {
                    Misc.free(offsetFunc, th);
                }
                if (sampleFromFunc != timezoneNameFunc && sampleFromFunc != offsetFunc) {
                    Misc.free(sampleFromFunc, th);
                }
                if (sampleToFunc != timezoneNameFunc && sampleToFunc != offsetFunc && sampleToFunc != sampleFromFunc) {
                    Misc.free(sampleToFunc, th);
                }
            }
            throw th;
        }
    }

    private Function instantiateFillValue(
            BoundExpression value,
            int targetType,
            OutputSchema input,
            RecordMetadata metadata,
            FunctionInstantiator instantiator,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final Function fill = instantiator.instantiate(value, input, metadata, executionContext);
        if (fill.getType() == targetType || !ColumnType.isArray(targetType)) {
            return fill;
        }
        final Function cast = functionResolver.createImplicitCast(value.getPosition(), fill, targetType, executionContext);
        return cast == null ? fill : cast;
    }

    /**
     * Consumes the aggregate input on entry, including on failure.
     */
    RecordCursorFactory generateFill(
            GenerationFrame frame,
            FillPlan plan,
            OutputSchema input,
            RecordCursorFactory groupByFactory,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final FunctionInstantiator instantiator = frame.functionInstantiator;
        ObjList<Function> values = null;
        ObjList<Function> constants = null;
        Function from = null;
        Function to = null;
        Function timezone = null;
        Function offset = null;
        boolean isTransferred = false;
        try {
            final RecordMetadata metadata = groupByFactory.getMetadata();
            final int entryCount = plan.getTargetColumnIds().size();
            values = new ObjList<>(entryCount);
            values.setPos(entryCount);
            for (int i = 0; i < entryCount; i++) {
                if (plan.getModes().getQuick(i) == FillPlan.FILL_VALUE) {
                    final int targetType = metadata.getColumnType(input.getColumnIndexById(plan.getTargetColumnIds().getQuick(i)));
                    values.setQuick(i, instantiateFillValue(plan.getValues().getQuick(i), targetType, input, metadata, instantiator, executionContext));
                }
            }
            final int timestampIndex = input.getColumnIndexById(plan.getTimestampColumnId());
            final int timestampType = metadata.getColumnType(timestampIndex);
            final TimestampDriver driver = getTimestampDriver(timestampType);
            from = plan.getFrom() == null ? driver.getTimestampConstantNull()
                    : instantiator.instantiate(plan.getFrom(), input, metadata, executionContext);
            assignUndefinedType(from, timestampType, executionContext);
            to = plan.getTo() == null ? driver.getTimestampConstantNull()
                    : instantiator.instantiate(plan.getTo(), input, metadata, executionContext);
            assignUndefinedType(to, timestampType, executionContext);
            final int intervalEnd = TimestampSamplerFactory.findPositiveIntervalEndIndex(plan.getPeriodToken(), plan.getPeriodPosition(), "sample");
            final long interval = TimestampSamplerFactory.parsePositiveInterval(plan.getPeriodToken(), intervalEnd,
                    plan.getPeriodPosition(), "sample", Numbers.INT_NULL, ' ');
            final char unit = plan.getPeriodToken().charAt(intervalEnd);
            final TimestampSampler sampler = TimestampSamplerFactory.getInstance(driver, interval, unit, plan.getPeriodPosition());
            if (plan.getTimezone() != null) {
                timezone = instantiator.instantiate(plan.getTimezone(), input, metadata, executionContext);
                assignUndefinedType(timezone, STRING, executionContext);
            }
            offset = plan.getOffset() == null ? StrConstant.NULL
                    : instantiator.instantiate(plan.getOffset(), input, metadata, executionContext);
            assignUndefinedType(offset, STRING, executionContext);

            final int columnCount = metadata.getColumnCount();
            final IntList columnToEntry = columnIndexes;
            columnToEntry.setAll(columnCount, -1);
            for (int i = 0; i < entryCount; i++) {
                columnToEntry.setQuick(input.getColumnIndexById(plan.getTargetColumnIds().getQuick(i)), i);
            }
            final IntList modes = new IntList(columnCount);
            constants = new ObjList<>(columnCount);
            for (int col = 0; col < columnCount; col++) {
                final int entry = columnToEntry.getQuick(col);
                if (col == timestampIndex) {
                    modes.add(SampleByFillRecordCursorFactory.FILL_CONSTANT);
                    constants.add(NullConstant.NULL);
                } else if (entry < 0) {
                    modes.add(SampleByFillRecordCursorFactory.FILL_KEY);
                    constants.add(NullConstant.NULL);
                } else {
                    switch (plan.getModes().getQuick(entry)) {
                        case FillPlan.FILL_NULL -> {
                            modes.add(SampleByFillRecordCursorFactory.FILL_CONSTANT);
                            constants.add(NullConstant.NULL);
                        }
                        case FillPlan.FILL_PREV -> {
                            modes.add(SampleByFillRecordCursorFactory.FILL_PREV_SELF);
                            constants.add(NullConstant.NULL);
                        }
                        case FillPlan.FILL_PREV_COLUMN -> {
                            final int sourceIndex = input.getColumnIndexById(plan.getSourceColumnIds().getQuick(entry));
                            assert sourceIndex >= 0 && sourceIndex != col && sourceIndex != timestampIndex;
                            modes.add(sourceIndex);
                            constants.add(NullConstant.NULL);
                        }
                        case FillPlan.FILL_VALUE -> {
                            modes.add(SampleByFillRecordCursorFactory.FILL_CONSTANT);
                            constants.add(values.getQuick(entry));
                            values.setQuick(entry, null);
                        }
                        default -> throw new IllegalArgumentException("invalid fill mode");
                    }
                }
            }
            isTransferred = true;
            return generateFillFactory(frame, plan.getAlgorithm(), groupByFactory, timestampIndex, timestampType, interval, unit, sampler,
                    modes, constants, values, from, to, plan.getToPosition(), offset, plan.getOffsetPosition(),
                    timezone, plan.getTimezonePosition());
        } catch (Throwable th) {
            if (!isTransferred) {
                Misc.freeObjList(values, th);
                Misc.freeObjList(constants, th);
                Misc.free(from, th);
                if (to != from) {
                    Misc.free(to, th);
                }
                if (offset != from && offset != to) {
                    Misc.free(offset, th);
                }
                if (timezone != from && timezone != to && timezone != offset) {
                    Misc.free(timezone, th);
                }
                Misc.free(groupByFactory, th);
            }
            throw th;
        }
    }

    RecordCursorFactory generateSampleBy(GenerationFrame frame, SampleByPlan sample, SqlExecutionContext executionContext) throws SqlException {
        final LogicalPlan sampled = sample.getInput();
        final RecordCursorFactory base = codeGenerator.generate(frame,
                !sample.isTimestampRequired() && LogicalPlans.isTimestampDeclarationOnly(sampled) ? sampled.inputAt(0) : sampled, executionContext);
        return generateSampleBy(frame, sample, base, executionContext);
    }

    /**
     * Consumes the input factory on entry, including on failure.
     */
    @NotNull
    RecordCursorFactory generateSampleBy(
            GenerationFrame frame,
            SampleByPlan plan,
            RecordCursorFactory base,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final FunctionInstantiator instantiator = frame.functionInstantiator;
        Function timezone = null;
        Function offset = null;
        Function from = null;
        Function to = null;
        ObjList<Function> records = null;
        ObjList<Function> fillConstants = null;
        boolean isTransferred = false;
        try {
            final OutputSchema input = plan.getInput().getOutput();
            final RecordMetadata baseMetadata = base.getMetadata();
            timezone = plan.getTimezone() == null ? StrConstant.NULL
                    : instantiator.instantiate(plan.getTimezone(), input, baseMetadata, executionContext);
            assignUndefinedType(timezone, STRING, executionContext);
            offset = plan.getOffset() == null ? StrConstant.NULL
                    : instantiator.instantiate(plan.getOffset(), input, baseMetadata, executionContext);
            assignUndefinedType(offset, STRING, executionContext);
            final int timestampIndex = plan.isTimestampRequired()
                    ? baseMetadata.getTimestampIndex() : input.getColumnIndexById(plan.getTimestampColumnId());
            final int timestampType = baseMetadata.getColumnType(timestampIndex);
            final TimestampDriver driver = getTimestampDriver(timestampType);
            from = plan.getFrom() == null ? driver.getTimestampConstantNull()
                    : instantiator.instantiate(plan.getFrom(), input, baseMetadata, executionContext);
            assignUndefinedType(from, timestampType, executionContext);
            to = plan.getTo() == null ? driver.getTimestampConstantNull()
                    : instantiator.instantiate(plan.getTo(), input, baseMetadata, executionContext);
            assignUndefinedType(to, timestampType, executionContext);
            if (plan.getTimezone() != null && CommonUtils.isSubDayUnit(plan.getPeriodUnit())) {
                final CharSequence zone = timezone.getStrA(null);
                if (zone != null) {
                    final TimeZoneRules rules = driver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, zone);
                    from = toSampleByUtc(from, driver, rules, timestampType);
                    to = toSampleByUtc(to, driver, rules, timestampType);
                }
            }
            final TimestampSampler sampler = plan.getPeriod() instanceof ConstantExpression period
                    ? TimestampSamplerFactory.getInstance(driver, period.getLongValue(), plan.getPeriodUnit(), plan.getPeriodUnitPosition())
                    : TimestampSamplerFactory.getInstance(driver, plan.getPeriodToken(), plan.getPeriodPosition());

            final ArrayColumnTypes keyTypes = frame.keyTypes;
            final ArrayColumnTypes valueTypes = frame.valueTypes;
            final ListColumnFilter listColumnFilterA = frame.listColumnFilterA;
            keyTypes.clear();
            valueTypes.clear();
            listColumnFilterA.clear();
            valueTypes.add(plan.getFillMode() == SampleByPlan.FILL_LINEAR ? BYTE : timestampType);
            final ObjList<BoundExpression> keys = plan.getGroupingExpressions();
            final ObjList<FunctionExpression> calls = plan.getAggregates();
            final ObjList<GroupByFunction> aggregates = new ObjList<>(calls.size());
            final OutputSchema output = plan.getOutput();
            records = new ObjList<>(output.getColumnCount());
            records.setPos(output.getColumnCount());
            for (int i = 0, n = calls.size(); i < n; i++) {
                final FunctionExpression call = calls.getQuick(i);
                final GroupByFunction function = (GroupByFunction) instantiator.instantiateAggregate(call, input, baseMetadata, executionContext);
                records.setQuick(keys.size() + i, function);
                aggregates.add(function);
                function.initValueTypes(valueTypes);
            }
            final int fillCount = plan.getFillTokens().size();
            fillConstants = new ObjList<>(fillCount);
            fillConstants.setPos(fillCount);
            for (int k = 0, n = Math.min(fillCount, aggregates.size()); k < n; k++) {
                final BoundExpression value = plan.getFillValues().getQuick(k);
                if (value != null) {
                    fillConstants.setQuick(k, instantiateFillValue(value, aggregates.getQuick(k).getType(), input, baseMetadata, instantiator, executionContext));
                }
            }
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            int lastKeyIndex = -1;
            int symbolKeyIndex = -1;
            boolean isFirstLast = plan.getFillMode() == SampleByPlan.FILL_NONE;
            final IntList firstLastIndexes = columnIndexes;
            final IntList firstLastKinds = this.firstLastKinds;
            final IntList firstLastPositions = this.firstLastPositions;
            final IntList groupByFunctionPositions = this.groupByFunctionPositions;
            firstLastIndexes.clear();
            firstLastKinds.clear();
            firstLastPositions.clear();
            groupByFunctionPositions.clear();
            for (int i = 0, n = keys.size(); i < n; i++) {
                final ColumnExpression column = (ColumnExpression) keys.getQuick(i);
                final int index = input.getColumnIndexById(column.getColumnId());
                final int type = baseMetadata.getColumnType(index);
                if (column.getColumnId() == plan.getTimestampColumnId()) {
                    metadata.setTimestampIndex(i);
                } else {
                    if (lastKeyIndex != index) {
                        listColumnFilterA.add(index + 1);
                        keyTypes.add(type);
                        lastKeyIndex = index;
                    }
                    records.setQuick(i, GroupByUtils.createColumnFunction(baseMetadata,
                            valueTypes.getColumnCount() + keyTypes.getColumnCount(), type, index));
                }
                final String name = Chars.toString(output.getColumnName(i));
                metadata.add(new TableColumnMetadata(name, type, baseMetadata.getColumnIndexType(index),
                        baseMetadata.getIndexValueBlockCapacity(index), baseMetadata.isSymbolTableStatic(index), baseMetadata.getMetadata(index)));
                firstLastIndexes.add(index);
                firstLastKinds.add(SampleByFirstLastRecordCursorFactory.KEY);
                firstLastPositions.add(column.getPosition());
                if (index == timestampIndex && index == baseMetadata.getTimestampIndex()) {
                    continue;
                }
                if (type != SYMBOL || symbolKeyIndex != -1 && symbolKeyIndex != index || !column.isDirectReference()) {
                    isFirstLast = false;
                }
                symbolKeyIndex = index;
            }
            for (int i = 0, n = calls.size(); i < n; i++) {
                final FunctionExpression call = calls.getQuick(i);
                final Function function = aggregates.getQuick(i);
                groupByFunctionPositions.add(call.getPosition());
                metadata.add(new TableColumnMetadata(Chars.toString(output.getColumnName(keys.size() + i)), function.getType(),
                        IndexType.NONE, 0, function instanceof SymbolFunction symbol && symbol.isSymbolTableStatic(), function.getMetadata()));
                if (call.getArgumentCount() != 1 || !(call.argumentAt(0) instanceof ColumnExpression column)
                        || !column.isDirectReference() || ColumnType.isArray(column.getDataType())
                        || !isFirstKeyword(call.getName()) && !isLastKeyword(call.getName())) {
                    isFirstLast = false;
                    continue;
                }
                firstLastIndexes.add(input.getColumnIndexById(column.getColumnId()));
                firstLastKinds.add(isFirstKeyword(call.getName())
                        ? SampleByFirstLastRecordCursorFactory.FIRST : SampleByFirstLastRecordCursorFactory.LAST);
                firstLastPositions.add(call.getPosition());
            }
            isTransferred = true;
            return generateSampleByFactory(frame, plan.getAlgorithm(), base, metadata, sampler, aggregates, records,
                    timestampIndex, timestampType, plan.getFillTokens(), fillConstants,
                    firstLastIndexes, isFirstLast, firstLastKinds, firstLastPositions, groupByFunctionPositions, symbolKeyIndex,
                    timezone, plan.getTimezonePosition(), offset, plan.getOffsetPosition(),
                    from, plan.getFromPosition(), to, plan.getToPosition(), executionContext);
        } catch (Throwable th) {
            if (!isTransferred) {
                Misc.freeObjList(records, th);
                Misc.freeObjList(fillConstants, th);
                Misc.free(base, th);
                Misc.free(timezone, th);
                if (offset != timezone) {
                    Misc.free(offset, th);
                }
                if (from != timezone && from != offset) {
                    Misc.free(from, th);
                }
                if (to != timezone && to != offset && to != from) {
                    Misc.free(to, th);
                }
            }
            throw th;
        }
    }
}

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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.ImplicitCastException;
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
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.functions.constants.ByteConstant;
import io.questdb.griffin.engine.functions.constants.CharConstant;
import io.questdb.griffin.engine.functions.constants.DateConstant;
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.griffin.engine.functions.constants.FloatConstant;
import io.questdb.griffin.engine.functions.constants.GeoByteConstant;
import io.questdb.griffin.engine.functions.constants.GeoIntConstant;
import io.questdb.griffin.engine.functions.constants.GeoLongConstant;
import io.questdb.griffin.engine.functions.constants.GeoShortConstant;
import io.questdb.griffin.engine.functions.constants.IPv4Constant;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.constants.Long256NullConstant;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.griffin.engine.functions.constants.NullArrayConstant;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.griffin.engine.functions.constants.ShortConstant;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.constants.SymbolConstant;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.griffin.engine.functions.constants.UuidConstant;
import io.questdb.griffin.engine.functions.constants.VarcharConstant;
import io.questdb.griffin.engine.functions.groupby.InterpolationGroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.SampleByFillNoneNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillNoneRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillValueNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFirstLastRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByInterpolateRecordCursorFactory;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.griffin.engine.orderby.EncodedSortLightRecordCursorFactory;
import io.questdb.griffin.engine.orderby.EncodedSortRecordCursorFactory;
import io.questdb.griffin.engine.orderby.RecordComparatorCompiler;
import io.questdb.griffin.engine.orderby.SortKeyEncoder;
import io.questdb.griffin.engine.orderby.SortedLightRecordCursorFactory;
import io.questdb.griffin.engine.orderby.SortedRecordCursorFactory;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
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
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.Transient;
import io.questdb.std.Uuid;
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
    private final CairoConfiguration configuration;
    private final EntityColumnFilter entityColumnFilter;
    private final Uuid fillUuid = new Uuid();
    private final FunctionParser functionParser;
    private final IntList groupByFunctionPositions = new IntList();
    private final ObjectPool<IntList> intListPool;
    private final ArrayColumnTypes keyTypes;
    private final ListColumnFilter listColumnFilterA;
    private final RecordComparatorCompiler recordComparatorCompiler;
    private final IntList recordFunctionPositions = new IntList();
    private final boolean validateSampleByFillType;
    private final ArrayColumnTypes valueTypes;

    SampleByFactoryGenerator(
            CairoConfiguration configuration,
            SqlCodeGenerator codeGenerator,
            FunctionParser functionParser,
            BytecodeAssembler asm,
            EntityColumnFilter entityColumnFilter,
            ObjectPool<IntList> intListPool,
            ArrayColumnTypes keyTypes,
            ArrayColumnTypes valueTypes,
            ListColumnFilter listColumnFilterA,
            RecordComparatorCompiler recordComparatorCompiler
    ) {
        this.configuration = configuration;
        this.codeGenerator = codeGenerator;
        this.functionParser = functionParser;
        this.asm = asm;
        this.entityColumnFilter = entityColumnFilter;
        this.intListPool = intListPool;
        this.keyTypes = keyTypes;
        this.valueTypes = valueTypes;
        this.listColumnFilterA = listColumnFilterA;
        this.recordComparatorCompiler = recordComparatorCompiler;
        this.validateSampleByFillType = configuration.isValidateSampleByFillType();
    }

    private static void coerceRuntimeConstantType(Function func, int type, SqlExecutionContext context, CharSequence message, int pos) throws SqlException {
        if (isUndefined(func.getType())) {
            func.assignType(type, context.getBindVariableService());
        } else if ((!func.isConstant() && !func.isRuntimeConstant()) || !ColumnType.isConvertibleFrom(func.getType(), type)) {
            throw SqlException.$(pos, message);
        }
    }

    private static Function createNullFillPlaceholder(IntList recordFunctionPositions, int index, int type) throws SqlException {
        return switch (ColumnType.tagOf(type)) {
            case ColumnType.INT -> IntConstant.NULL;
            case ColumnType.IPv4 -> IPv4Constant.NULL;
            case ColumnType.LONG -> LongConstant.NULL;
            case ColumnType.FLOAT -> FloatConstant.NULL;
            case ColumnType.DOUBLE -> DoubleConstant.NULL;
            case ColumnType.BYTE -> ByteConstant.ZERO;
            case ColumnType.SHORT -> ShortConstant.ZERO;
            case ColumnType.GEOBYTE -> GeoByteConstant.NULL;
            case ColumnType.GEOSHORT -> GeoShortConstant.NULL;
            case ColumnType.GEOINT -> GeoIntConstant.NULL;
            case ColumnType.GEOLONG -> GeoLongConstant.NULL;
            case ColumnType.UUID -> UuidConstant.NULL;
            case ColumnType.DATE -> DateConstant.NULL;
            case ColumnType.LONG256 -> Long256NullConstant.INSTANCE;
            case ColumnType.STRING -> StrConstant.NULL;
            case ColumnType.VARCHAR -> VarcharConstant.NULL;
            case ColumnType.SYMBOL -> SymbolConstant.NULL;
            case ColumnType.TIMESTAMP -> ColumnType.getTimestampDriver(type).getTimestampConstantNull();
            default -> {
                if (ColumnType.isArray(type)) {
                    yield new NullArrayConstant(type);
                }
                if (ColumnType.isDecimal(type)) {
                    yield DecimalUtil.createNullDecimalConstant(
                            ColumnType.getDecimalPrecision(type),
                            ColumnType.getDecimalScale(type)
                    );
                }
                throw SqlException.$(recordFunctionPositions.getQuick(index), "Unsupported type: ").put(ColumnType.nameOf(type));
            }
        };
    }

    @NotNull
    private static ObjList<Function> createSampleByFillPlaceholders(
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            @Transient IntList recordFunctionPositions,
            @NotNull @Transient ObjList<CharSequence> fillTokens,
            @Transient IntList fillPositions,
            ObjList<Function> fillConstants
    ) throws SqlException {
        final ObjList<Function> placeholderFunctions = new ObjList<>();
        int fillIndex = 0;
        final int fillValueCount = fillTokens.size();
        for (int i = 0, n = recordFunctions.size(); i < n; i++) {
            Function function = recordFunctions.getQuick(i);
            if (function instanceof GroupByFunction) {
                if (fillIndex == fillValueCount) {
                    throw SqlException.position(fillPositions.getQuick(fillIndex - 1))
                            .put("insufficient fill values for SAMPLE BY FILL: expected ")
                            .put(groupByFunctions.size())
                            .put(" values but only ")
                            .put(fillValueCount)
                            .put(" provided");
                }
                final CharSequence fillToken = fillTokens.getQuick(fillIndex++);
                if (isNullKeyword(fillToken)) {
                    placeholderFunctions.add(createNullFillPlaceholder(recordFunctionPositions, i, function.getType()));
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

    // Fixed-size scalars and wide types that MapValue can put/get directly.
    // SYMBOL is cached as the int symbol id. UUID, INTERVAL, and variable-width
    // types fall back to the recordAt path -- MapValue lacks symmetric put APIs
    // for those.
    private static boolean isFixedSizePrevSlotEligible(int srcTag) {
        return switch (srcTag) {
            case ColumnType.BOOLEAN, ColumnType.BYTE, ColumnType.CHAR, ColumnType.DATE, ColumnType.DECIMAL128,
                 ColumnType.DECIMAL16, ColumnType.DECIMAL256, ColumnType.DECIMAL32, ColumnType.DECIMAL64,
                 ColumnType.DECIMAL8, ColumnType.DOUBLE, ColumnType.FLOAT, ColumnType.GEOBYTE, ColumnType.GEOINT,
                 ColumnType.GEOLONG, ColumnType.GEOSHORT, ColumnType.INT, ColumnType.IPv4, ColumnType.LONG,
                 ColumnType.LONG128, ColumnType.LONG256, ColumnType.SHORT, ColumnType.SYMBOL,
                 ColumnType.TIMESTAMP -> true;
            default -> false;
        };
    }

    private static int resolveFillPrevMode(
            RecordMetadata metadata, int timestampIndex, int targetIndex, int sourceIndex,
            CharSequence sourceName, int position
    ) throws SqlException {
        if (sourceIndex < 0) {
            throw SqlException.$(position, "PREV(col): column not found in output: ").put(sourceName);
        }
        if (sourceIndex == timestampIndex) {
            throw SqlException.$(position, "PREV cannot reference the designated timestamp column");
        }
        if (sourceIndex == targetIndex) {
            return SampleByFillRecordCursorFactory.FILL_PREV_SELF;
        }
        final int targetType = metadata.getColumnType(targetIndex);
        final int sourceType = metadata.getColumnType(sourceIndex);
        final int targetTag = ColumnType.tagOf(targetType);
        final int sourceTag = ColumnType.tagOf(sourceType);
        if (targetTag == ColumnType.SYMBOL || sourceTag == ColumnType.SYMBOL) {
            throw SqlException.$(position, "FILL(PREV(").put(sourceName)
                    .put(")) is not supported on SYMBOL columns; use bare FILL(PREV) instead");
        }
        final boolean needsExactType = ColumnType.isDecimal(targetType) || ColumnType.isGeoHash(targetType)
                || targetTag == ColumnType.ARRAY || targetTag == ColumnType.TIMESTAMP || targetTag == ColumnType.INTERVAL;
        if (needsExactType ? targetType != sourceType : targetTag != sourceTag) {
            throw SqlException.$(position, "FILL(PREV(").put(sourceName).put(")): source type ")
                    .put(ColumnType.nameOf(sourceType)).put(" cannot fill target column of type ")
                    .put(ColumnType.nameOf(targetType));
        }
        return sourceIndex;
    }

    // Reads the value through the getter SampleByFillRecordCursorFactory's record uses for the target type.
    private static Function toFillConstant(Function fill, int targetType, CharSequence fillToken, int fillPosition) throws SqlException {
        try {
            return switch (ColumnType.tagOf(targetType)) {
                case ColumnType.BOOLEAN -> BooleanConstant.of(fill.getBool(null));
                case ColumnType.BYTE -> ByteConstant.newInstance((byte) fill.getInt(null));
                case ColumnType.SHORT -> ShortConstant.newInstance((short) fill.getInt(null));
                case ColumnType.CHAR -> CharConstant.newInstance(fill.getChar(null));
                case ColumnType.INT -> IntConstant.newInstance(fill.getInt(null));
                case ColumnType.IPv4 -> IPv4Constant.newInstance(fill.getIPv4(null));
                case ColumnType.LONG -> LongConstant.newInstance(fill.getLong(null));
                case ColumnType.DATE -> DateConstant.newInstance(fill.getLong(null));
                case ColumnType.FLOAT -> FloatConstant.newInstance(fill.getFloat(null));
                case ColumnType.DOUBLE -> DoubleConstant.newInstance(fill.getDouble(null));
                case ColumnType.LONG256 -> {
                    fill.getLong256A(null);
                    yield fill;
                }
                default -> fill;
            };
        } catch (ImplicitCastException | UnsupportedOperationException e) {
            throw GroupByUtils.invalidSampleByFillValue(fillToken, fillPosition);
        }
    }

    private static Function toSampleByUtc(Function function, TimestampDriver driver, TimeZoneRules rules, int timestampType) {
        if (function != driver.getTimestampConstantNull()) {
            final long timestamp = driver.from(function.getTimestamp(null), ColumnType.getTimestampType(function.getType()));
            if (timestamp != Numbers.LONG_NULL) {
                return TimestampConstant.newInstance(driver.toUTC(timestamp, rules), timestampType);
            }
        }
        return function;
    }

    private static void validateFillNull(int type, int position) throws SqlException {
        final int tag = ColumnType.tagOf(type);
        if (tag == ColumnType.BOOLEAN || tag == ColumnType.CHAR) {
            throw SqlException.$(position, "fill value of type NULL cannot fill column of type ")
                    .put(ColumnType.nameOf(type));
        }
    }

    private static void validateFillPrevChains(IntList modes, IntList positions) throws SqlException {
        for (int i = 0, n = modes.size(); i < n; i++) {
            final int source = modes.getQuick(i);
            if (source >= 0) {
                final int sourceMode = modes.getQuick(source);
                if (sourceMode >= 0) {
                    throw SqlException.$(positions.getQuick(i),
                            "FILL(PREV) chains are not supported: source column is itself a cross-column PREV");
                }
                if (sourceMode == SampleByFillRecordCursorFactory.FILL_CONSTANT) {
                    throw SqlException.$(positions.getQuick(i),
                            "FILL(PREV) cannot reference a column that is itself filled with a constant");
                }
            }
        }
    }

    /**
     * Consumes the input and all function lists/parameters, including on failure.
     */
    private RecordCursorFactory generateFillFactory(
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
        try {
            final RecordMetadata groupByMetadata = groupByFactory.getMetadata();
            final int columnCount = groupByMetadata.getColumnCount();
            // A SAMPLE BY cursor that keeps its latest rows readable serves the fill
            // its keys and PREV values directly.
            final boolean isSampleBySource = groupByFactory instanceof SampleByFillNoneRecordCursorFactory
                    || groupByFactory instanceof SampleByFillNoneNotKeyedRecordCursorFactory;
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
                if (isSampleBySource || !isFixedSizePrevSlotEligible(srcTag)) {
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
            if (groupByFactory.getMetadata().getTimestampIndex() != timestampIndex) {
                final RecordMetadata sortMetadata = groupByFactory.getMetadata();
                listColumnFilterA.clear();
                listColumnFilterA.add(timestampIndex + 1); // positive = ascending
                entityColumnFilter.of(sortMetadata.getColumnCount());
                final int sortStrategy = configuration.getSampleByFillSortStrategy();
                // LIGHT_RECORDCHAIN and FULL_RECORDCHAIN need null-before-risky:
                // SortedLight/SortedRecordCursorFactory's ctor catch cascades codeGenerator.close()
                // and frees `base`, so the outer catch's Misc.free(groupByFactory)
                // would double-free unless we null first. Risky-arg calls (newInstance,
                // RecordSinkFactory.getInstance, copy) run BEFORE the null so the
                // outer catch still owns base on their failure. LIGHT_ENCODED and
                // FULL_ENCODED have no such ctor catch -- the caller is the single
                // owner there.
                switch (sortStrategy) {
                    case SampleBySortStrategy.LIGHT_ENCODED -> {
                        assert SortKeyEncoder.isSupported(sortMetadata, listColumnFilterA)
                                && groupByFactory.recordCursorSupportsRandomAccess();
                        groupByFactory = new EncodedSortLightRecordCursorFactory(
                                configuration,
                                sortMetadata,
                                groupByFactory,
                                listColumnFilterA.copy()
                        );
                    }
                    case SampleBySortStrategy.FULL_ENCODED -> {
                        assert SortKeyEncoder.isSupported(sortMetadata, listColumnFilterA);
                        groupByFactory = new EncodedSortRecordCursorFactory(
                                configuration,
                                sortMetadata,
                                groupByFactory,
                                RecordSinkFactory.getInstance(configuration, asm, sortMetadata, entityColumnFilter),
                                listColumnFilterA.copy()
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

            if (needsPrevPositioning && !isSampleBySource && !groupByFactory.recordCursorSupportsRandomAccess()) {
                throw CairoException.critical(0).put("FILL(PREV) cannot re-read rows of a base without random access");
            }
            final GenericRecordMetadata fillMetadata = GenericRecordMetadata.copyOfNew(groupByFactory.getMetadata());
            fillMetadata.setTimestampIndex(timestampIndex);

            // Transferred slots were nulled in the per-column branch; this frees
            // any residual non-transferred fill functions. Detach each slot before
            // close so the outer rollback cannot retry a function whose close throws.
            final Throwable cleanupFailure = Misc.freeObjListBestEffort(null, fillValues);
            fillValues = null;
            CairoException.rethrowCleanupFailure(cleanupFailure);
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
            throw th;
        }
    }

    /**
     * Consumes the base, assembled functions and temporal parameters, including on failure.
     */
    private RecordCursorFactory generateSampleByFactory(
            RecordCursorFactory base,
            GenericRecordMetadata projectionMetadata,
            TimestampSampler timestampSampler,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            int timestampIndex,
            int timestampType,
            ObjList<CharSequence> fillTokens,
            IntList fillPositions,
            ObjList<Function> fillConstants,
            @Nullable IntList firstLastIndexes,
            @Nullable IntList firstLastKinds,
            @Nullable IntList firstLastPositions,
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
            final RecordMetadata baseMetadata = base.getMetadata();
            final int fillCount = fillTokens.size();
            if (fillCount == 1 && isLinearKeyword(fillTokens.getQuick(0))) {
                isTransferred = true;
                return new SampleByInterpolateRecordCursorFactory(
                        asm,
                        configuration,
                        base,
                        projectionMetadata,
                        groupByFunctions,
                        recordFunctions,
                        timestampSampler,
                        listColumnFilterA,
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
            final boolean isFillNone = fillCount == 0 || fillCount == 1 && isNoneKeyword(fillTokens.getQuick(0));
            if (firstLastIndexes != null && !base.hasParquetConvertedColumns(executionContext)) {
                final SingleSymbolFilter symbolFilter = base.convertToSampleByIndexPageFrameCursorFactory();
                if (symbolFilter != null) {
                    // Posting indexes do not expose the raw row-id frame cursor used by first/last.
                    if (IndexType.isBitmap(baseMetadata.getColumnIndexType(symbolFilter.getColumnIndex()))
                            && (symbolKeyIndex == -1 || symbolFilter.getColumnIndex() == symbolKeyIndex)) {
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
                    base.revertFromSampleByIndexPageFrameCursorFactory();
                }
            }
            if (isFillNone) {
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
                        listColumnFilterA,
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

            assert fillCount > 0;

            if (keyTypes.getColumnCount() == 0) {
                final ObjList<Function> placeholders = createSampleByFillPlaceholders(
                        groupByFunctions, recordFunctions, recordFunctionPositions, fillTokens, fillPositions, fillConstants
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
            }

            throw SqlException.position(0).put("linear interpolation is not supported when using fill values for keyed sample by expression");
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

    /**
     * Owns the FILL value rule of both SAMPLE BY shapes: replaces the instantiated value at
     * {@code fillIndex} with a constant of {@code targetType}, or fails at {@code fillPosition}.
     */
    private void prepareFillValue(
            ObjList<Function> fillValues,
            int fillIndex,
            int targetType,
            CharSequence fillToken,
            int fillPosition
    ) throws SqlException {
        if (ColumnType.isTimestamp(targetType)) {
            // functionParser produces TIMESTAMP_NANO for any string
            // literal, which drifts by 1000x against a MICRO target.
            // Re-parse with the target driver to keep units correct.
            if (!Chars.isQuoted(fillToken)) {
                throw SqlException.position(fillPosition).put("Invalid fill value: '").put(fillToken)
                        .put("'. Timestamp fill value must be in quotes. Example: '2019-01-01T00:00:00.000Z'");
            }
            final long parsed;
            try {
                parsed = ColumnType.getTimestampDriver(targetType).parseQuotedLiteral(fillToken);
            } catch (NumericException e) {
                throw GroupByUtils.invalidSampleByFillValue(fillToken, fillPosition);
            }
            // Null-then-free: if Misc.free's codeGenerator.close() throws, the
            // outer catch must not double-close the same instance.
            Function staleFunc = fillValues.getQuick(fillIndex);
            fillValues.setQuick(fillIndex, null);
            Misc.free(staleFunc);
            fillValues.setQuick(fillIndex, TimestampConstant.newInstance(parsed, targetType));
            return;
        }
        Function fillFunc = fillValues.getQuick(fillIndex);
        final int fillType = fillFunc.getType();
        if (fillType != ColumnType.UNDEFINED && !ColumnType.isConvertibleFrom(fillType, targetType)) {
            throw SqlException.$(fillPosition, "fill value of type ").put(ColumnType.nameOf(fillType))
                    .put(" cannot fill column of type ").put(ColumnType.nameOf(targetType));
        }
        if (fillFunc.isNonDeterministic() || !fillFunc.isConstant()) {
            throw SqlException.$(fillPosition, "fill value must be a constant expression");
        }
        if (ColumnType.tagOf(targetType) == ColumnType.UUID) {
            validateUuidFill(fillFunc, fillToken, fillPosition);
        }
        // An implicit cast factory turns e.g. INT into DECIMAL; without it the
        // fill record would call a getter the value's function does not implement.
        if (fillType != targetType && !ColumnType.isBuiltInWideningCast(fillType, targetType)) {
            fillValues.setQuick(fillIndex, null);
            final Function cast;
            try {
                cast = functionParser.createImplicitCast(fillPosition, fillFunc, targetType);
            } catch (ImplicitCastException e) {
                throw GroupByUtils.invalidSampleByFillValue(fillToken, fillPosition);
            }
            if (cast != null) {
                fillFunc = cast;
            }
            fillValues.setQuick(fillIndex, fillFunc);
        }
        if (fillFunc.getType() != ColumnType.NULL && ColumnType.tagOf(fillFunc.getType()) != ColumnType.tagOf(targetType)) {
            final Function constant = toFillConstant(fillFunc, targetType, fillToken, fillPosition);
            if (constant != fillFunc) {
                fillValues.setQuick(fillIndex, constant);
                Misc.free(fillFunc);
            }
        }
    }

    private void validateUuidFill(Function fill, CharSequence fillToken, int fillPosition) throws SqlException {
        try {
            switch (ColumnType.tagOf(fill.getType())) {
                case ColumnType.STRING, ColumnType.SYMBOL ->
                        SqlUtil.implicitCastStrAsUuid(fill.getStrA(null), fillUuid);
                case ColumnType.VARCHAR -> SqlUtil.implicitCastStrAsUuid(fill.getVarcharA(null), fillUuid);
                default -> {
                }
            }
        } catch (ImplicitCastException e) {
            throw GroupByUtils.invalidSampleByFillValue(fillToken, fillPosition);
        }
    }

    /**
     * Consumes the aggregate input on entry, including on failure.
     */
    RecordCursorFactory generateFill(
            FillPlan plan,
            OutputSchema input,
            RecordCursorFactory groupByFactory,
            FunctionInstantiator instantiator,
            SqlExecutionContext executionContext
    ) throws SqlException {
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
                    values.setQuick(i, instantiator.instantiate(plan.getValues().getQuick(i), input, metadata, executionContext));
                }
            }
            final int timestampIndex = input.getColumnIndexById(plan.getTimestampColumnId());
            final int timestampType = metadata.getColumnType(timestampIndex);
            final TimestampDriver driver = getTimestampDriver(timestampType);
            from = plan.getFrom() == null ? driver.getTimestampConstantNull()
                    : instantiator.instantiate(plan.getFrom(), input, metadata, executionContext);
            coerceRuntimeConstantType(from, timestampType, executionContext,
                    "from lower bound must be a constant expression convertible to a TIMESTAMP", plan.getFromPosition());
            to = plan.getTo() == null ? driver.getTimestampConstantNull()
                    : instantiator.instantiate(plan.getTo(), input, metadata, executionContext);
            coerceRuntimeConstantType(to, timestampType, executionContext,
                    "to upper bound must be a constant expression convertible to a TIMESTAMP", plan.getToPosition());
            final int intervalEnd = TimestampSamplerFactory.findPositiveIntervalEndIndex(plan.getPeriodToken(), plan.getPeriodPosition(), "sample");
            final long interval = TimestampSamplerFactory.parsePositiveInterval(plan.getPeriodToken(), intervalEnd,
                    plan.getPeriodPosition(), "sample", Numbers.INT_NULL, ' ');
            final char unit = plan.getPeriodToken().charAt(intervalEnd);
            final TimestampSampler sampler = TimestampSamplerFactory.getInstance(driver, interval, unit, plan.getPeriodPosition());
            if (plan.getTimezone() != null) {
                timezone = instantiator.instantiate(plan.getTimezone(), input, metadata, executionContext);
                coerceRuntimeConstantType(timezone, STRING, executionContext,
                        "TIME ZONE must be a constant expression of STRING or CHAR type", plan.getTimezonePosition());
            }
            offset = plan.getOffset() == null ? StrConstant.NULL
                    : instantiator.instantiate(plan.getOffset(), input, metadata, executionContext);
            coerceRuntimeConstantType(offset, STRING, executionContext,
                    "offset must be a constant expression of STRING or CHAR type", plan.getOffsetPosition());

            final int columnCount = metadata.getColumnCount();
            final IntList columnToEntry = intListPool.next();
            columnToEntry.setAll(columnCount, -1);
            for (int i = 0; i < entryCount; i++) {
                columnToEntry.setQuick(input.getColumnIndexById(plan.getTargetColumnIds().getQuick(i)), i);
            }
            final IntList modes = new IntList(columnCount);
            final IntList positions = intListPool.next();
            positions.setAll(columnCount, 0);
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
                    final int position = plan.getPositions().getQuick(entry);
                    positions.setQuick(col, position);
                    switch (plan.getModes().getQuick(entry)) {
                        case FillPlan.FILL_NULL -> {
                            validateFillNull(metadata.getColumnType(col), position);
                            modes.add(SampleByFillRecordCursorFactory.FILL_CONSTANT);
                            constants.add(NullConstant.NULL);
                        }
                        case FillPlan.FILL_PREV -> {
                            modes.add(SampleByFillRecordCursorFactory.FILL_PREV_SELF);
                            constants.add(NullConstant.NULL);
                        }
                        case FillPlan.FILL_PREV_COLUMN -> {
                            final int sourceIndex = input.getColumnIndexById(plan.getSourceColumnIds().getQuick(entry));
                            modes.add(resolveFillPrevMode(metadata, timestampIndex, col, sourceIndex,
                                    plan.getTokens().getQuick(entry), plan.getSourcePositions().getQuick(entry)));
                            constants.add(NullConstant.NULL);
                        }
                        case FillPlan.FILL_VALUE -> {
                            prepareFillValue(values, entry, metadata.getColumnType(col), plan.getTokens().getQuick(entry), position);
                            modes.add(SampleByFillRecordCursorFactory.FILL_CONSTANT);
                            constants.add(values.getQuick(entry));
                            values.setQuick(entry, null);
                        }
                        default -> throw new IllegalArgumentException("invalid fill mode");
                    }
                }
            }
            validateFillPrevChains(modes, positions);
            isTransferred = true;
            return generateFillFactory(groupByFactory, timestampIndex, timestampType, interval, unit, sampler,
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

    int generateSampleBy(GenerationFrame frame, SampleByPlan sample, SqlExecutionContext executionContext) throws SqlException {
        final LogicalPlan sampled = sample.getInput();
        final int inputSlot = codeGenerator.generateJoinInput(frame, !sample.isTimestampRequired() && SqlCodeGenerator.isTimestampDeclarationOnly(sampled) ? sampled.inputAt(0) : sampled, executionContext,
                sample.isTimestampRequired(), OrderByMnemonic.ORDER_BY_REQUIRED);
        final int slot = frame.resources.reserve();
        final RecordCursorFactory base = frame.resources.detachFactory(inputSlot);
        frame.resources.own(slot, generateSampleBy(sample, base, frame.functionInstantiator, executionContext));
        return slot;
    }

    /**
     * Consumes the input factory on entry, including on failure.
     */
    @NotNull
    RecordCursorFactory generateSampleBy(
            SampleByPlan plan,
            RecordCursorFactory base,
            FunctionInstantiator instantiator,
            SqlExecutionContext executionContext
    ) throws SqlException {
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
            coerceRuntimeConstantType(timezone, STRING, executionContext,
                    "timezone must be a constant expression of STRING or CHAR type", plan.getTimezonePosition());
            offset = plan.getOffset() == null ? StrConstant.NULL
                    : instantiator.instantiate(plan.getOffset(), input, baseMetadata, executionContext);
            coerceRuntimeConstantType(offset, STRING, executionContext,
                    "offset must be a constant expression of STRING or CHAR type", plan.getOffsetPosition());
            final int timestampIndex = plan.isTimestampRequired()
                    ? baseMetadata.getTimestampIndex() : input.getColumnIndexById(plan.getTimestampColumnId());
            if (timestampIndex < 0) {
                throw SqlException.$(plan.getPosition(), "base query does not provide designated TIMESTAMP column");
            }
            if (base.getScanDirection() != RecordCursorFactory.SCAN_DIRECTION_FORWARD) {
                throw SqlException.$(plan.getPosition(), plan.isJoinInput()
                        ? "ASC order over TIMESTAMP column is required but not provided"
                        : "base query does not provide ASC order over designated TIMESTAMP column");
            }
            final int timestampType = baseMetadata.getColumnType(timestampIndex);
            final TimestampDriver driver = getTimestampDriver(timestampType);
            from = plan.getFrom() == null ? driver.getTimestampConstantNull()
                    : instantiator.instantiate(plan.getFrom(), input, baseMetadata, executionContext);
            coerceRuntimeConstantType(from, timestampType, executionContext,
                    "from lower bound must be a constant expression convertible to a TIMESTAMP", plan.getFromPosition());
            to = plan.getTo() == null ? driver.getTimestampConstantNull()
                    : instantiator.instantiate(plan.getTo(), input, baseMetadata, executionContext);
            coerceRuntimeConstantType(to, timestampType, executionContext,
                    "to upper bound must be a constant expression convertible to a TIMESTAMP", plan.getToPosition());
            if (plan.getTimezone() != null && CommonUtils.isSubDayUnit(plan.getPeriodUnit())) {
                final CharSequence zone = timezone.getStrA(null);
                if (zone != null) {
                    try {
                        final TimeZoneRules rules = driver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, zone);
                        final Function oldFrom = from;
                        from = toSampleByUtc(from, driver, rules, timestampType);
                        if (from != oldFrom) {
                            Misc.free(oldFrom);
                        }
                        final Function oldTo = to;
                        to = toSampleByUtc(to, driver, rules, timestampType);
                        if (to != oldTo) {
                            Misc.free(oldTo);
                        }
                    } catch (NumericException ex) {
                        throw SqlException.$(plan.getTimezonePosition(), "invalid timezone: ").put(zone);
                    }
                }
            }
            final TimestampSampler sampler;
            if (plan.getPeriod() == null) {
                sampler = TimestampSamplerFactory.getInstance(driver, plan.getPeriodToken(), plan.getPeriodPosition());
            } else {
                try (Function period = instantiator.instantiate(plan.getPeriod(), input, baseMetadata, executionContext)) {
                    if (!period.isConstant() || period.getType() != LONG && period.getType() != INT) {
                        throw SqlException.$(plan.getPeriodPosition(), "sample by period must be a constant expression of INT or LONG type");
                    }
                    sampler = TimestampSamplerFactory.getInstance(driver, period.getLong(null), plan.getPeriodUnit(), plan.getPeriodUnitPosition());
                }
            }

            keyTypes.clear();
            valueTypes.clear();
            listColumnFilterA.clear();
            groupByFunctionPositions.clear();
            recordFunctionPositions.clear();
            valueTypes.add(plan.getFillMode() == SampleByPlan.FILL_LINEAR ? BYTE : timestampType);
            final ObjList<BoundExpression> keys = plan.getGroupingExpressions();
            final ObjList<FunctionExpression> calls = plan.getAggregates();
            final ObjList<GroupByFunction> aggregates = new ObjList<>(calls.size());
            final OutputSchema output = plan.getOutput();
            records = new ObjList<>(output.getColumnCount());
            records.setPos(output.getColumnCount());
            for (int i = 0, n = keys.size(); i < n; i++) {
                recordFunctionPositions.add(keys.getQuick(i).getPosition());
            }
            for (int i = 0, n = calls.size(); i < n; i++) {
                final FunctionExpression call = calls.getQuick(i);
                final GroupByFunction function = (GroupByFunction) instantiator.instantiateAggregate(call, input, baseMetadata, executionContext);
                records.setQuick(keys.size() + i, function);
                aggregates.add(function);
                recordFunctionPositions.add(call.getPosition());
                groupByFunctionPositions.add(call.getPosition());
            }
            final int fillCount = plan.getFillTokens().size();
            if (validateSampleByFillType && fillCount > 1 && fillCount < aggregates.size()) {
                boolean hasNone = false;
                for (int i = 0; i < fillCount; i++) {
                    hasNone |= isNoneKeyword(plan.getFillTokens().getQuick(i));
                }
                if (!hasNone) {
                    throw SqlException.$(plan.getFillPositions().getQuick(0), "not enough fill values");
                }
            }
            for (int i = 0, n = aggregates.size(); i < n; i++) {
                final GroupByFunction function = aggregates.getQuick(i);
                final int position = groupByFunctionPositions.getQuick(i);
                GroupByUtils.validateTimestampOrder(function, timestampIndex, SqlCodeGenerator.isBaseTimestampAscending(base, timestampIndex), position);
                if (validateSampleByFillType && fillCount > 0) {
                    final int fillIndex = Math.min(i, fillCount - 1);
                    final CharSequence unsupportedFill = GroupByUtils.getUnsupportedSampleByFill(function, plan.getFillTokens().getQuick(fillIndex));
                    if (unsupportedFill != null) {
                        throw SqlException.$(plan.getFillPositions().getQuick(fillIndex), "support for ").put(unsupportedFill)
                                .put(" fill is not yet implemented [function=").put(plan.getAggregateSql().getQuick(i))
                                .put(", class=").put(function.getClass().getName()).put(']');
                    }
                }
                function.initValueTypes(valueTypes);
            }
            fillConstants = new ObjList<>(fillCount);
            fillConstants.setPos(fillCount);
            for (int k = 0, n = Math.min(fillCount, aggregates.size()); k < n; k++) {
                final CharSequence fillToken = plan.getFillTokens().getQuick(k);
                final int fillPosition = plan.getFillPositions().getQuick(k);
                final BoundExpression value = plan.getFillValues().getQuick(k);
                if (value != null) {
                    fillConstants.setQuick(k, instantiator.instantiate(value, input, baseMetadata, executionContext));
                    prepareFillValue(fillConstants, k, aggregates.getQuick(k).getType(), fillToken, fillPosition);
                } else if (isNullKeyword(fillToken)) {
                    for (int i = fillCount == 1 ? 0 : k, m = fillCount == 1 ? aggregates.size() : k + 1; i < m; i++) {
                        validateFillNull(aggregates.getQuick(i).getType(), fillPosition);
                    }
                }
            }
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            int lastKeyIndex = -1;
            int symbolKeyIndex = -1;
            boolean isFirstLast = plan.getFillMode() == SampleByPlan.FILL_NONE;
            final IntList firstLastIndexes = intListPool.next();
            final IntList firstLastKinds = intListPool.next();
            final IntList firstLastPositions = intListPool.next();
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
            return generateSampleByFactory(base, metadata, sampler, aggregates, records,
                    timestampIndex, timestampType, plan.getFillTokens(), plan.getFillPositions(), fillConstants,
                    isFirstLast ? firstLastIndexes : null, firstLastKinds, firstLastPositions, symbolKeyIndex,
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

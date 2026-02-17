/*******************************************************************************
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

import io.questdb.TelemetryEvent;
import io.questdb.TelemetryOrigin;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnFilter;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.FullPartitionFrameCursorFactory;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IntervalPartitionFrameCursorFactory;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.ProjectableRecordCursorFactory;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.map.RecordValueSink;
import io.questdb.cairo.map.RecordValueSinkFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursorFactory;
import io.questdb.cairo.sql.SingleSymbolFilter;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.griffin.engine.ExplainPlanFactory;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.griffin.engine.LimitRecordCursorFactory;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.cast.CastByteToCharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastByteToDecimalFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastByteToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastByteToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDateToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDateToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDateToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDecimalToDecimalFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDecimalToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDoubleArrayToDoubleArrayFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDoubleArrayToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDoubleArrayToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDoubleToDoubleArray;
import io.questdb.griffin.engine.functions.cast.CastDoubleToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDoubleToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastFloatToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastFloatToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastGeoHashToGeoHashFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIPv4ToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIPv4ToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToDecimalFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntervalToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLong256ToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLong256ToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToDecimalFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToDecimalFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToDecimalFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToGeoHashFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastSymbolToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastSymbolToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastTimestampToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastTimestampToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastTimestampToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastUuidToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastUuidToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToDecimalFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToGeoHashFunctionFactory;
import io.questdb.griffin.engine.functions.columns.ArrayColumn;
import io.questdb.griffin.engine.functions.columns.BinColumn;
import io.questdb.griffin.engine.functions.columns.BooleanColumn;
import io.questdb.griffin.engine.functions.columns.ByteColumn;
import io.questdb.griffin.engine.functions.columns.CharColumn;
import io.questdb.griffin.engine.functions.columns.DateColumn;
import io.questdb.griffin.engine.functions.columns.DecimalColumn;
import io.questdb.griffin.engine.functions.columns.DoubleColumn;
import io.questdb.griffin.engine.functions.columns.FloatColumn;
import io.questdb.griffin.engine.functions.columns.GeoByteColumn;
import io.questdb.griffin.engine.functions.columns.GeoIntColumn;
import io.questdb.griffin.engine.functions.columns.GeoLongColumn;
import io.questdb.griffin.engine.functions.columns.GeoShortColumn;
import io.questdb.griffin.engine.functions.columns.IPv4Column;
import io.questdb.griffin.engine.functions.columns.IntColumn;
import io.questdb.griffin.engine.functions.columns.IntervalColumn;
import io.questdb.griffin.engine.functions.columns.Long256Column;
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.functions.columns.ShortColumn;
import io.questdb.griffin.engine.functions.columns.StrColumn;
import io.questdb.griffin.engine.functions.columns.SymbolColumn;
import io.questdb.griffin.engine.functions.columns.TimestampColumn;
import io.questdb.griffin.engine.functions.columns.UuidColumn;
import io.questdb.griffin.engine.functions.columns.VarcharColumn;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.constants.SymbolConstant;
import io.questdb.griffin.engine.functions.date.TimestampFloorFunctionFactory;
import io.questdb.griffin.engine.functions.decimal.Decimal64LoaderFunctionFactory;
import io.questdb.griffin.engine.functions.memoization.ArrayFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.BooleanFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.ByteFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.CharFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.DateFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.DoubleFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.FloatFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.IPv4FunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.IntFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.Long256FunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.LongFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.ShortFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.StrFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.SymbolFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.TimestampFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.UuidFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.VarcharFunctionMemoizer;
import io.questdb.griffin.engine.groupby.CountRecordCursorFactory;
import io.questdb.griffin.engine.groupby.DistinctRecordCursorFactory;
import io.questdb.griffin.engine.groupby.DistinctTimeSeriesRecordCursorFactory;
import io.questdb.griffin.engine.groupby.FillRangeRecordCursorFactory;
import io.questdb.griffin.engine.groupby.GroupByNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.SampleByFillNoneNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillNoneRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillNullNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillNullRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillPrevNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillPrevRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillValueNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillValueRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFirstLastRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByInterpolateRecordCursorFactory;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.griffin.engine.groupby.vect.AvgDoubleVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.AvgIntVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.AvgLongVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.AvgShortVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.CountDoubleVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.CountIntVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.CountLongVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.CountVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.GroupByNotKeyedVectorRecordCursorFactory;
import io.questdb.griffin.engine.groupby.vect.GroupByRecordCursorFactory;
import io.questdb.griffin.engine.groupby.vect.KSumDoubleVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.MaxDateVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.MaxDoubleVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.MaxIntVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.MaxLongVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.MaxShortVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.MaxTimestampVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.MinDateVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.MinDoubleVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.MinIntVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.MinLongVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.MinShortVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.MinTimestampVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.NSumDoubleVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.SumDoubleVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.SumIntVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.SumLong256VectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.SumLongVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.SumShortVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.VectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.VectorAggregateFunctionConstructor;
import io.questdb.griffin.engine.join.AsOfJoinDenseRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinDenseSingleSymbolRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinFastRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinIndexedRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinLightNoKeyRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinMemoizedRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinNoKeyFastRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.AsyncWindowJoinFastRecordCursorFactory;
import io.questdb.griffin.engine.join.AsyncWindowJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.ChainedSymbolShortCircuit;
import io.questdb.griffin.engine.join.CrossJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.FilteredAsOfJoinFastRecordCursorFactory;
import io.questdb.griffin.engine.join.FilteredAsOfJoinNoKeyFastRecordCursorFactory;
import io.questdb.griffin.engine.join.HashJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.join.HashJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.HashOuterJoinFilteredLightRecordCursorFactory;
import io.questdb.griffin.engine.join.HashOuterJoinFilteredRecordCursorFactory;
import io.questdb.griffin.engine.join.HashOuterJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.join.HashOuterJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.griffin.engine.join.LtJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.join.LtJoinNoKeyFastRecordCursorFactory;
import io.questdb.griffin.engine.join.LtJoinNoKeyRecordCursorFactory;
import io.questdb.griffin.engine.join.LtJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.MarkoutHorizonRecordCursorFactory;
import io.questdb.griffin.engine.join.NestedLoopFullJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.NestedLoopLeftJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.NestedLoopRightJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.NoopSymbolShortCircuit;
import io.questdb.griffin.engine.join.NullRecordFactory;
import io.questdb.griffin.engine.join.RecordAsAFieldRecordCursorFactory;
import io.questdb.griffin.engine.join.SpliceJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.join.StringToSymbolJoinKeyMapping;
import io.questdb.griffin.engine.join.SymbolJoinKeyMapping;
import io.questdb.griffin.engine.join.SymbolKeyMappingRecordCopier;
import io.questdb.griffin.engine.join.SymbolShortCircuit;
import io.questdb.griffin.engine.join.SymbolToSymbolJoinKeyMapping;
import io.questdb.griffin.engine.join.VarcharToSymbolJoinKeyMapping;
import io.questdb.griffin.engine.join.WindowJoinFastRecordCursorFactory;
import io.questdb.griffin.engine.join.WindowJoinRecordCursorFactory;
import io.questdb.griffin.engine.orderby.LimitedSizeSortedLightRecordCursorFactory;
import io.questdb.griffin.engine.orderby.LongSortedLightRecordCursorFactory;
import io.questdb.griffin.engine.orderby.LongTopKRecordCursorFactory;
import io.questdb.griffin.engine.orderby.RecordComparatorCompiler;
import io.questdb.griffin.engine.orderby.SortedLightRecordCursorFactory;
import io.questdb.griffin.engine.orderby.SortedRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncGroupByNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncJitFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncTopKRecordCursorFactory;
import io.questdb.griffin.engine.table.DeferredSingleSymbolFilterPageFrameRecordCursorFactory;
import io.questdb.griffin.engine.table.DeferredSymbolIndexFilteredRowCursorFactory;
import io.questdb.griffin.engine.table.DeferredSymbolIndexRowCursorFactory;
import io.questdb.griffin.engine.table.ExtraNullColumnCursorFactory;
import io.questdb.griffin.engine.table.FilterOnExcludedValuesRecordCursorFactory;
import io.questdb.griffin.engine.table.FilterOnSubQueryRecordCursorFactory;
import io.questdb.griffin.engine.table.FilterOnValuesRecordCursorFactory;
import io.questdb.griffin.engine.table.FilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestByAllFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestByAllIndexedRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestByAllSymbolsFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestByDeferredListValuesFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestByLightRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestByRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestBySubQueryRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestByValueDeferredFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestByValueDeferredIndexedFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestByValueDeferredIndexedRowCursorFactory;
import io.questdb.griffin.engine.table.LatestByValueFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestByValueIndexedFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestByValueIndexedRowCursorFactory;
import io.questdb.griffin.engine.table.LatestByValuesIndexedFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.PageFrameRecordCursorFactory;
import io.questdb.griffin.engine.table.PageFrameRowCursorFactory;
import io.questdb.griffin.engine.table.SelectedRecordCursorFactory;
import io.questdb.griffin.engine.table.SortedSymbolIndexRecordCursorFactory;
import io.questdb.griffin.engine.table.SymbolIndexFilteredRowCursorFactory;
import io.questdb.griffin.engine.table.SymbolIndexRowCursorFactory;
import io.questdb.griffin.engine.table.VirtualRecordCursorFactory;
import io.questdb.griffin.engine.union.ExceptAllRecordCursorFactory;
import io.questdb.griffin.engine.union.ExceptRecordCursorFactory;
import io.questdb.griffin.engine.union.IntersectAllRecordCursorFactory;
import io.questdb.griffin.engine.union.IntersectRecordCursorFactory;
import io.questdb.griffin.engine.union.SetRecordCursorFactoryConstructor;
import io.questdb.griffin.engine.union.UnionAllRecordCursorFactory;
import io.questdb.griffin.engine.union.UnionRecordCursorFactory;
import io.questdb.griffin.engine.window.CachedWindowRecordCursorFactory;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.ExplainModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IntrinsicModel;
import io.questdb.griffin.model.JoinContext;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.RuntimeIntervalModel;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.griffin.model.WindowJoinContext;
import io.questdb.jit.CompiledCountOnlyFilter;
import io.questdb.jit.CompiledFilter;
import io.questdb.jit.CompiledFilterIRSerializer;
import io.questdb.jit.JitUtil;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.BitSet;
import io.questdb.std.BufferWindowCharSequence;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Chars;
import io.questdb.std.Decimals;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.LongList;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.ObjObjHashMap;
import io.questdb.std.ObjectPool;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.Arrays;

import static io.questdb.cairo.ColumnType.*;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.*;
import static io.questdb.griffin.SqlKeywords.*;
import static io.questdb.griffin.model.ExpressionNode.*;
import static io.questdb.griffin.model.QueryModel.*;

public class SqlCodeGenerator implements Mutable, Closeable {
    public static final int GKK_MICRO_HOUR_INT = 1;
    public static final int GKK_NANO_HOUR_INT = 2;
    public static final int GKK_VANILLA_INT = 0;
    public static final boolean[] joinsRequiringTimestamp = new boolean[JOIN_MAX + 1];
    private static final VectorAggregateFunctionConstructor COUNT_CONSTRUCTOR = (keyKind, columnIndex, timestampIndex, workerCount) -> new CountVectorAggregateFunction(keyKind);
    private static final FullFatJoinGenerator CREATE_FULL_FAT_AS_OF_JOIN = SqlCodeGenerator::createFullFatAsOfJoin;
    private static final FullFatJoinGenerator CREATE_FULL_FAT_LT_JOIN = SqlCodeGenerator::createFullFatLtJoin;
    private static final Log LOG = LogFactory.getLog(SqlCodeGenerator.class);
    private static final ModelOperator RESTORE_WHERE_CLAUSE = QueryModel::restoreWhereClause;
    private static final SetRecordCursorFactoryConstructor SET_EXCEPT_ALL_CONSTRUCTOR = ExceptAllRecordCursorFactory::new;
    private static final SetRecordCursorFactoryConstructor SET_EXCEPT_CONSTRUCTOR = ExceptRecordCursorFactory::new;
    private static final SetRecordCursorFactoryConstructor SET_INTERSECT_ALL_CONSTRUCTOR = IntersectAllRecordCursorFactory::new;
    private static final SetRecordCursorFactoryConstructor SET_INTERSECT_CONSTRUCTOR = IntersectRecordCursorFactory::new;
    // @formatter:off
    private static final SetRecordCursorFactoryConstructor SET_UNION_CONSTRUCTOR = UnionRecordCursorFactory::new;
    /**
     * Autogenerated. See `SqlCodeGeneratorTest.testUnionCastMatrix`.
     * <p>
     * The UNION_CAST_MATRIX captures all the combinations of "left" and "right" column types
     * in a set operation (UNION etc.), providing the desired output type. Since there are many
     * special cases in the conversion logic, we decided to use a matrix of literals instead.
     * The matrix doesn't cover generic types (e.g. geohash) since they have a more complex structure.
     */
    private static final int[][] UNION_CAST_MATRIX = new int[][]{
            { 0, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, -1, -1, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11,  0}, //  0 = unknown
            {11,  1, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, -1, -1, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11,  1}, //  1 = BOOLEAN
            {11, 11,  2,  3, 11,  5,  6,  7,  8,  9, 10, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, -1, -1, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11,  2}, //  2 = BYTE
            {11, 11,  3,  3,  3,  5,  6,  7,  8,  9, 10, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, -1, -1, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11,  3}, //  3 = SHORT
            {11, 11, 11,  3,  4,  5,  6,  7,  8,  9, 10, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, -1, -1, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11}, //  4 = CHAR
            {11, 11,  5,  5,  5,  5,  6,  7,  8,  9, 10, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, -1, -1, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11,  5}, //  5 = INT
            {11, 11,  6,  6,  6,  6,  6,  7,  8,  9, 10, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, -1, -1, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11,  6}, //  6 = LONG
            {11, 11,  7,  7,  7,  7,  7,  7,  8,  9, 10, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, -1, -1, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11,  7}, //  7 = DATE
            {11, 11,  8,  8,  8,  8,  8,  8,  8,  9, 10, 11,  8, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, -1, -1, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11,  8}, //  8 = TIMESTAMP
            {11, 11,  9,  9,  9,  9,  9,  9,  9,  9, 10, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, -1, -1, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11,  9}, //  9 = FLOAT
            {11, 11, 10, 10, 10, 10, 10, 10, 10, 10, 10, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, -1, -1, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 10}, // 10 = DOUBLE
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11}, // 11 = STRING
            {11, 11, 11, 11, 11, 11, 11, 11,  8, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, -1, -1, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11}, // 12 = SYMBOL
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 13, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, -1, -1, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 13}, // 13 = LONG256
            {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}, // 14 = unknown
            {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}, // 15 = unknown
            {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}, // 16 = unknown
            {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}, // 17 = unknown
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 18, 11, 11, 11, 11, 11, 11, 11, 26, 11, -1, -1, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 18}, // 18 = BINARY
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 19, 11, 11, 11, 11, 11, 11, 26, 11, -1, -1, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 19}, // 19 = UUID
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 20, 11, 11, 11, 11, 11, 26, 11, -1, -1, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 20}, // 20 = CURSOR
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 21, 11, 11, 11, 11, 26, 11, -1, -1, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 21}, // 21 = VARARG
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 22, 11, 11, 11, 26, 11, -1, -1, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 22}, // 22 = RECORD
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 23, 11, 11, 26, 11, -1, -1, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 23}, // 23 = GEOHASH
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 24, 11, 26, 11, -1, -1, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 24}, // 24 = LONG128
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 25, 26, 11, -1, -1, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 25}, // 25 = IPv4
            {26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 11, 26, 26, -1, -1, -1, -1, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, -1, -1, -1, -1, -1, -1, 26, 26, 26, 26, 26, 26, 26}, // 26 = VARCHAR
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 27, -1, -1, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 27}, // 27 = ARRAY
            {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}, // 28 = unknown
            {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}, // 29 = unknown
            {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}, // 30 = unknown
            {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}, // 31 = unknown
            {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}, // 32 = unknown
            {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}, // 33 = unknown
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, -1, -1, -1, -1, -1, -1, 34, 11, 11, 11, 11, 11, 34}, // 34 = DECIMAL
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, -1, -1, -1, -1, -1, -1, 11, 35, 11, 11, 11, 11, 35}, // 35 = regclass
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, -1, -1, -1, -1, -1, -1, 11, 11, 36, 11, 11, 11, 36}, // 36 = regprocedure
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, -1, -1, -1, -1, -1, -1, 11, 11, 11, 37, 11, 11, 37}, // 37 = text[]
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, -1, -1, -1, -1, -1, -1, 11, 11, 11, 11, 38, 11, 38}, // 38 = PARAMETER
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, -1, -1, -1, -1, -1, -1, 11, 11, 11, 11, 11, 39, 39}, // 39 = INTERVAL
            { 0,  1,  2,  3, 11,  5,  6,  7,  8,  9, 10, 11, 11, 13, -1, -1, -1, -1, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, -1, -1, -1, -1, -1, -1, 34, 35, 36, 37, 38, 39, 40}  // 40 = NULL
    };
    // @formatter:on
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> avgConstructors = new IntObjHashMap<>();
    private static final ModelOperator backupWhereClauseRef = QueryModel::backupWhereClause;
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> countConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> ksumConstructors = new IntObjHashMap<>();
    private static final IntHashSet limitTypes = new IntHashSet();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> maxConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> minConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> nsumConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> sumConstructors = new IntObjHashMap<>();
    public static boolean ALLOW_FUNCTION_MEMOIZATION = true;
    private final ArrayColumnTypes arrayColumnTypes = new ArrayColumnTypes();
    private final BytecodeAssembler asm = new BytecodeAssembler();
    private final CairoConfiguration configuration;
    private final ObjList<TableColumnMetadata> deferredWindowMetadata = new ObjList<>();
    private final boolean enableJitDebug;
    private final EntityColumnFilter entityColumnFilter = new EntityColumnFilter();
    private final ObjectPool<ExpressionNode> expressionNodePool;
    private final FunctionParser functionParser;
    private final IntList groupByFunctionPositions = new IntList();
    private final ObjObjHashMap<IntList, ObjList<WindowFunction>> groupedWindow = new ObjObjHashMap<>();
    private final IntHashSet intHashSet = new IntHashSet();
    private final ObjectPool<IntList> intListPool = new ObjectPool<>(IntList::new, 4);
    private final MemoryCARW jitIRMem;
    private final CompiledFilterIRSerializer jitIRSerializer = new CompiledFilterIRSerializer();
    private final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
    // this list is used to generate record sinks
    private final ListColumnFilter listColumnFilterA = new ListColumnFilter();
    private final ListColumnFilter listColumnFilterB = new ListColumnFilter();
    private final MarkoutHorizonInfo markoutHorizonInfo = new MarkoutHorizonInfo();
    private final LongList prefixes = new LongList();
    private final ObjectPool<QueryColumn> queryColumnPool;
    private final RecordComparatorCompiler recordComparatorCompiler;
    private final IntList recordFunctionPositions = new IntList();
    private final PageFrameReduceTaskFactory reduceTaskFactory;
    private final ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
    private final ArrayDeque<ExpressionNode> sqlNodeStack2 = new ArrayDeque<>();
    private final WhereClauseSymbolEstimator symbolEstimator = new WhereClauseSymbolEstimator();
    private final IntList tempAggIndex = new IntList();
    private final ObjList<QueryColumn> tempColumnsList = new ObjList<>();
    private final ObjList<ExpressionNode> tempExpressionNodeList = new ObjList<>();
    private final ObjList<Function> tempInnerProjectionFunctions = new ObjList<>();
    private final IntList tempKeyIndex = new IntList();
    private final IntList tempKeyIndexesInBase = new IntList();
    private final IntList tempKeyKinds = new IntList();
    private final GenericRecordMetadata tempMetadata = new GenericRecordMetadata();
    private final ObjList<Function> tempOuterProjectionFunctions = new ObjList<>();
    private final IntList tempSymbolSkewIndexes = new IntList();
    private final ObjList<VectorAggregateFunction> tempVaf = new ObjList<>();
    private final IntList tempVecConstructorArgIndexes = new IntList();
    private final ObjList<VectorAggregateFunctionConstructor> tempVecConstructors = new ObjList<>();
    private final PostOrderTreeTraversalAlgo traversalAlgo;
    private final boolean validateSampleByFillType;
    private final ArrayColumnTypes valueTypes = new ArrayColumnTypes();
    private final WhereClauseParser whereClauseParser = new WhereClauseParser();
    private final WindowJoinAggColumnVectorizedCheck windowJoinAggColumnVectorizedCheck = new WindowJoinAggColumnVectorizedCheck();
    private final WindowJoinColCheckVisitor windowJoinColCheckVisitor = new WindowJoinColCheckVisitor();
    // a bitset of string/symbol columns forced to be serialised as varchar
    private final BitSet writeStringAsVarcharA = new BitSet();
    private final BitSet writeStringAsVarcharB = new BitSet();
    private final BitSet writeSymbolAsString = new BitSet();
    // bitsets for timestamp conversion to higher precision type
    private final BitSet writeTimestampAsNanosA = new BitSet();
    private final BitSet writeTimestampAsNanosB = new BitSet();
    private boolean enableJitNullChecks = true;
    private boolean fullFatJoins = false;
    // Used to pass ORDER BY context from outer query down to join generation for markout horizon optimization
    // Tracks the last model with non-empty ORDER BY as we descend through nested models
    private QueryModel lastSeenOrderByModel;

    public SqlCodeGenerator(
            CairoConfiguration configuration,
            FunctionParser functionParser,
            PostOrderTreeTraversalAlgo postOrderTreeTraversalAlgo,
            ObjectPool<QueryColumn> queryColumnPool,
            ObjectPool<ExpressionNode> expressionNodePool
    ) {
        try {
            this.configuration = configuration;
            this.functionParser = functionParser;
            this.recordComparatorCompiler = new RecordComparatorCompiler(asm);
            this.enableJitDebug = configuration.isSqlJitDebugEnabled();
            this.traversalAlgo = postOrderTreeTraversalAlgo;
            this.queryColumnPool = queryColumnPool;
            this.jitIRMem = Vm.getCARWInstance(
                    configuration.getSqlJitIRMemoryPageSize(),
                    configuration.getSqlJitIRMemoryMaxPages(),
                    MemoryTag.NATIVE_SQL_COMPILER
            );
            // Pre-touch JIT IR memory to avoid false positive memory leak detections.
            jitIRMem.putByte((byte) 0);
            jitIRMem.truncate();
            this.expressionNodePool = expressionNodePool;
            this.reduceTaskFactory = () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_SQL_COMPILER);
            this.validateSampleByFillType = configuration.isValidateSampleByFillType();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @TestOnly
    public static int[][] actualUnionCastMatrix() {
        return UNION_CAST_MATRIX;
    }

    @TestOnly
    public static int[][] expectedUnionCastMatrix() {
        final int[][] expected = new int[NULL + 1][NULL + 1];
        for (int typeA = 0; typeA <= NULL; typeA++) {
            for (int typeB = 0; typeB <= NULL; typeB++) {
                final int outType = (isGeoType(typeA) || isGeoType(typeB) || isDecimalType(typeA) || isDecimalType(typeB)) ? -1 : commonWideningType(typeA, typeB);
                expected[typeA][typeB] = outType;
            }
        }
        return expected;
    }

    public static int getUnionCastType(int typeA, int typeB) throws SqlException {
        short tagA = tagOf(typeA);
        short tagB = tagOf(typeB);

        final boolean aIsArray = tagA == ARRAY;
        final boolean bIsArray = tagB == ARRAY;
        if (aIsArray && bIsArray) {
            short elementTypeA = decodeArrayElementType(typeA);
            if (elementTypeA != decodeArrayElementType(typeB)) {
                return VARCHAR;
            }
            int dimsA = decodeWeakArrayDimensionality(typeA);
            int dimsB = decodeWeakArrayDimensionality(typeB);
            if (dimsA == -1 || dimsB == -1) {
                throw SqlException.$(0, "array bind variables are not supported in UNION queries");
            }
            return encodeArrayType(elementTypeA, Math.max(dimsA, dimsB));
        } else if (aIsArray) {
            if (tagB == DOUBLE) {
                // if B is scalar then we coarse it to array of the same dimensionality as A is
                return typeA;
            }
            return (tagB == STRING) ? STRING : VARCHAR;
        } else if (bIsArray) {
            if (tagA == DOUBLE) {
                return typeB;
            }
            return (tagA == STRING ? STRING : VARCHAR);
        }

        int geoBitsA = getGeoHashBits(typeA);
        int geoBitsB = getGeoHashBits(typeB);
        boolean isGeoHashA = ColumnType.isGeoHash(typeA);
        boolean isGeoHashB = ColumnType.isGeoHash(typeB);
        if (isGeoHashA != isGeoHashB) {
            // One type is geohash, the other isn't. Since a stringy type can be parsed
            // into geohash, output geohash when the other type is stringy. If not,
            // cast both types to string.
            return isStringyType(typeA) ? typeB
                    : isStringyType(typeB) ? typeA
                    : STRING;
        }
        if (isGeoHashA) {
            // Both types are geohash, resolve to the one with fewer geohash bits.
            return geoBitsA < geoBitsB ? typeA : typeB;
        }

        boolean isDecimalA = ColumnType.isDecimalType(tagA);
        boolean isDecimalB = ColumnType.isDecimalType(tagB);
        if (isDecimalA || isDecimalB) {
            int a = DecimalUtil.getTypePrecisionScale(typeA);
            int b = DecimalUtil.getTypePrecisionScale(typeB);
            if (a != 0 && b != 0) {
                // Both types are decimal/implicitly castable to decimal.
                int precisionA = Numbers.decodeLowShort(a);
                int scaleA = Numbers.decodeHighShort(a);
                int precisionB = Numbers.decodeLowShort(b);
                int scaleB = Numbers.decodeHighShort(b);
                final int scale = Math.max(scaleA, scaleB);
                final int precision = Math.min(Math.max(precisionA - scaleA, precisionB - scaleB) + scale, Decimals.MAX_PRECISION);
                return ColumnType.getDecimalType(precision, scale);
            }
            // We also support casting between decimal and stringy types.
            return isStringyType(typeA) ? typeB :
                    isStringyType(typeB) ? typeA :
                            ColumnType.STRING; // Fallback should be supported by any type
        }

        if (tagA == INTERVAL || tagB == INTERVAL) {
            if (tagA == INTERVAL && tagB == INTERVAL) {
                return Math.max(typeA, typeB);
            }
            if (tagA == INTERVAL && tagB == NULL) {
                return typeA;
            }
            if (tagB == INTERVAL && tagA == NULL) {
                return typeB;
            }
        }

        // Neither type is geohash, use the type cast matrix to resolve.
        int result = UNION_CAST_MATRIX[tagA][tagB];

        //  indicate at least one of typeA or typeB is timestamp(timestamp_ns) type
        if (result == TIMESTAMP) {
            if (isTimestamp(typeA) && isTimestamp(typeB)) {
                return getHigherPrecisionTimestampType(typeA, typeB);
            } else if (isTimestamp(typeA)) {
                return typeA;
            } else {
                return typeB;
            }
        }
        return result;
    }

    @Override
    public void clear() {
        whereClauseParser.clear();
        symbolEstimator.clear();
        intListPool.clear();
    }

    @Override
    public void close() {
        Misc.free(jitIRMem);
    }

    @NotNull
    public Function compileBooleanFilter(
            ExpressionNode expr,
            RecordMetadata metadata,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final Function filter = functionParser.parseFunction(expr, metadata, executionContext);
        if (isBoolean(filter.getType())) {
            return filter;
        }
        Misc.free(filter);
        throw SqlException.$(expr.position, "boolean expression expected");
    }

    @NotNull
    public Function compileJoinFilter(
            ExpressionNode expr,
            RecordMetadata metadata,
            SqlExecutionContext executionContext
    ) throws SqlException {
        try {
            return compileBooleanFilter(expr, metadata, executionContext);
        } catch (Throwable t) {
            if (metadata instanceof JoinRecordMetadata that) {
                Misc.free(that);
            }
            throw t;
        }
    }

    public RecordCursorFactory generate(@Transient QueryModel model, @Transient SqlExecutionContext executionContext) throws SqlException {
        return generateQuery(model, executionContext, true);
    }

    public RecordCursorFactory generateExplain(@Transient ExplainModel model, @Transient SqlExecutionContext executionContext) throws SqlException {
        final ExecutionModel innerModel = model.getInnerExecutionModel();
        final QueryModel queryModel = innerModel.getQueryModel();
        RecordCursorFactory factory;
        if (queryModel != null) {
            factory = generate(queryModel, executionContext);
            if (innerModel.getModelType() != QueryModel.QUERY) {
                factory = new RecordCursorFactoryStub(innerModel, factory);
            }
        } else {
            factory = new RecordCursorFactoryStub(innerModel, null);
        }

        return new ExplainPlanFactory(factory, model.getFormat());
    }

    public RecordCursorFactory generateExplain(QueryModel model, RecordCursorFactory factory, int format) {
        RecordCursorFactory recordCursorFactory = new RecordCursorFactoryStub(model, factory);
        return new ExplainPlanFactory(recordCursorFactory, format);
    }

    public BytecodeAssembler getAsm() {
        return asm;
    }

    public EntityColumnFilter getEntityColumnFilter() {
        return entityColumnFilter;
    }

    public ListColumnFilter getIndexColumnFilter() {
        return listColumnFilterA;
    }

    public RecordComparatorCompiler getRecordComparatorCompiler() {
        return recordComparatorCompiler;
    }

    public IntList toOrderIndices(RecordMetadata m, ObjList<ExpressionNode> orderBy, IntList orderByDirection) throws SqlException {
        final IntList indices = intListPool.next();
        for (int i = 0, n = orderBy.size(); i < n; i++) {
            ExpressionNode tok = orderBy.getQuick(i);
            int index = m.getColumnIndexQuiet(tok.token);
            if (index == -1) {
                throw SqlException.invalidColumn(tok.position, tok.token);
            }

            // shift index by 1 to use sign as sort direction
            index++;

            // negative column index means descending order of sort
            if (orderByDirection.getQuick(i) == ORDER_DIRECTION_DESCENDING) {
                index = -index;
            }

            indices.add(index);
        }
        return indices;
    }

    private static boolean allGroupsFirstLastWithSingleSymbolFilter(QueryModel model, RecordMetadata metadata) {
        final ObjList<QueryColumn> columns = model.getColumns();
        CharSequence symbolToken = null;
        int timestampIdx = metadata.getTimestampIndex();

        for (int i = 0, n = columns.size(); i < n; i++) {
            final QueryColumn column = columns.getQuick(i);
            final ExpressionNode node = column.getAst();

            if (node.type == LITERAL) {
                int idx = metadata.getColumnIndex(node.token);
                int columnType = metadata.getColumnType(idx);
                if (isTimestamp(columnType)) {
                    if (idx != timestampIdx) {
                        return false;
                    }
                } else if (columnType == SYMBOL) {
                    if (symbolToken == null) {
                        symbolToken = node.token;
                    } else if (!Chars.equalsIgnoreCase(symbolToken, node.token)) {
                        return false; // more than one key symbol column
                    }
                } else {
                    return false;
                }
            } else {
                ExpressionNode columnAst = column.getAst();
                CharSequence token = columnAst.token;
                if (!isFirstKeyword(token) && !isLastKeyword(token)) {
                    return false;
                }

                if (columnAst.rhs.type != LITERAL || metadata.getColumnIndex(columnAst.rhs.token) < 0) {
                    return false;
                }
            }
        }

        return true;
    }

    private static void coerceRuntimeConstantType(Function func, int type, SqlExecutionContext context, CharSequence message, int pos) throws SqlException {
        if (isUndefined(func.getType())) {
            func.assignType(type, context.getBindVariableService());
        } else if ((!func.isConstant() && !func.isRuntimeConstant()) || !ColumnType.isConvertibleFrom(func.getType(), type)) {
            throw SqlException.$(pos, message);
        }
    }

    private static void collectColumnIndexes(
            ArrayDeque<ExpressionNode> sqlNodeStack,
            RecordMetadata metadata,
            ExpressionNode node,
            IntHashSet outIndexes
    ) {
        sqlNodeStack.clear();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        while (!sqlNodeStack.isEmpty() || node != null) {
            if (node != null) {
                if (node.type == LITERAL) {
                    int index = metadata.getColumnIndexQuiet(node.token);
                    if (index < 0) {
                        int dot = Chars.indexOfLastUnquoted(node.token, '.');
                        if (dot > -1) {
                            index = metadata.getColumnIndexQuiet(node.token, dot + 1, node.token.length());
                        }
                    }
                    if (index >= 0) {
                        outIndexes.add(index);
                    }
                    node = null;
                    continue;
                } else {
                    for (int i = 0, n = node.args.size(); i < n; i++) {
                        sqlNodeStack.add(node.args.getQuick(i));
                    }
                    if (node.rhs != null) {
                        sqlNodeStack.push(node.rhs);
                    }
                }
                node = node.lhs;
            } else {
                node = sqlNodeStack.poll();
            }
        }
    }

    private static RecordCursorFactory createFullFatAsOfJoin(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            @Transient ColumnTypes mapKeyTypes,
            @Transient ColumnTypes mapValueTypes,
            @Transient ColumnTypes slaveColumnTypes,
            RecordSink masterKeySink,
            RecordSink slaveKeySink,
            int columnSplit,
            RecordValueSink slaveValueSink,
            IntList columnIndex,
            JoinContext joinContext,
            ColumnFilter masterTableKeyColumns,
            long toleranceInterval,
            int slaveValueTimestampIndex
    ) {
        return new AsOfJoinRecordCursorFactory(
                configuration,
                metadata,
                masterFactory,
                slaveFactory,
                mapKeyTypes,
                mapValueTypes,
                slaveColumnTypes,
                masterKeySink,
                slaveKeySink,
                columnSplit,
                slaveValueSink,
                columnIndex,
                joinContext,
                masterTableKeyColumns,
                toleranceInterval,
                slaveValueTimestampIndex
        );
    }

    private static RecordCursorFactory createFullFatLtJoin(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            @Transient ColumnTypes mapKeyTypes,
            @Transient ColumnTypes mapValueTypes,
            @Transient ColumnTypes slaveColumnTypes,
            RecordSink masterKeySink,
            RecordSink slaveKeySink,
            int columnSplit,
            RecordValueSink slaveValueSink,
            IntList columnIndex,
            JoinContext joinContext,
            ColumnFilter masterTableKeyColumns,
            long toleranceInterval,
            int slaveValueTimestampIndex
    ) {
        return new LtJoinRecordCursorFactory(
                configuration,
                metadata,
                masterFactory,
                slaveFactory,
                mapKeyTypes,
                mapValueTypes,
                slaveColumnTypes,
                masterKeySink,
                slaveKeySink,
                columnSplit,
                slaveValueSink,
                columnIndex,
                joinContext,
                masterTableKeyColumns,
                toleranceInterval,
                slaveValueTimestampIndex
        );
    }

    @SuppressWarnings("unchecked")
    private static <T extends Function> @Nullable ObjList<ObjList<T>> extractWorkerFunctionsConditionally(
            ObjList<Function> projectionFunctions,
            IntList projectionFunctionFlags,
            ObjList<ObjList<Function>> perThreadFunctions,
            int flag
    ) {
        ObjList<ObjList<T>> perThreadKeyFunctions = null;
        boolean keysThreadSafe = true;
        for (int i = 0, n = projectionFunctions.size(); i < n; i++) {
            if ((flag == GroupByUtils.PROJECTION_FUNCTION_FLAG_ANY || projectionFunctionFlags.get(i) == flag) && !projectionFunctions.getQuick(i).isThreadSafe()) {
                keysThreadSafe = false;
                break;
            }
        }

        if (!keysThreadSafe) {
            assert perThreadFunctions != null;
            perThreadKeyFunctions = new ObjList<>();
            for (int i = 0, n = perThreadFunctions.size(); i < n; i++) {
                ObjList<T> threadFunctions = new ObjList<>();
                perThreadKeyFunctions.add(threadFunctions);
                ObjList<Function> funcs = perThreadFunctions.getQuick(i);
                for (int j = 0, m = funcs.size(); j < m; j++) {
                    if (flag == GroupByUtils.PROJECTION_FUNCTION_FLAG_ANY || projectionFunctionFlags.get(j) == flag) {
                        threadFunctions.add((T) funcs.getQuick(j));
                    }
                }
            }
        }
        return perThreadKeyFunctions;
    }

    private static int getOrderByDirectionOrDefault(QueryModel model, int index) {
        final IntList direction = model.getOrderByDirectionAdvice();
        return index >= direction.size() ? ORDER_DIRECTION_ASCENDING : direction.getQuick(index);
    }

    private static @Nullable String getViewName(ExpressionNode viewExpr) {
        return Chars.toString(viewExpr != null ? viewExpr.token : null);
    }

    private static int getViewPosition(ExpressionNode viewExpr) {
        return viewExpr != null ? viewExpr.position : 0;
    }

    private static boolean isSingleColumnFunction(ExpressionNode ast, CharSequence name) {
        return ast.type == FUNCTION && ast.paramCount == 1 && Chars.equalsIgnoreCase(ast.token, name) && ast.rhs.type == LITERAL;
    }

    private static boolean isSingleSymbolJoin(SymbolShortCircuit symbolShortCircuit, ListColumnFilter joinColumns) {
        return joinColumns.getColumnCount() == 1 &&
                symbolShortCircuit != NoopSymbolShortCircuit.INSTANCE &&
                !(symbolShortCircuit instanceof ChainedSymbolShortCircuit);
    }

    private static long tolerance(QueryModel slaveModel, int leftTimestamp, int rightTimestampType) throws SqlException {
        ExpressionNode tolerance = slaveModel.getAsOfJoinTolerance();
        long toleranceInterval = Numbers.LONG_NULL;
        if (tolerance != null) {
            int k = TimestampSamplerFactory.findIntervalEndIndex(tolerance.token, tolerance.position, "tolerance");
            assert tolerance.token.length() > k;
            char unit = tolerance.token.charAt(k);
            TimestampDriver timestampDriver = getTimestampDriver(getHigherPrecisionTimestampType(leftTimestamp, rightTimestampType));
            long multiplier;
            switch (unit) {
                case 'n':
                    toleranceInterval = TimestampSamplerFactory.parseInterval(tolerance.token, k, tolerance.position, "tolerance", Integer.MAX_VALUE, unit);
                    return timestampDriver.fromNanos(toleranceInterval);
                case 'U':
                    multiplier = timestampDriver.fromMicros(1);
                    break;
                case 'T':
                    multiplier = timestampDriver.fromMillis(1);
                    break;
                case 's':
                    multiplier = timestampDriver.fromSeconds(1);
                    break;
                case 'm':
                    multiplier = timestampDriver.fromMinutes(1);
                    break;
                case 'h':
                    multiplier = timestampDriver.fromHours(1);
                    break;
                case 'd':
                    multiplier = timestampDriver.fromDays(1);
                    break;
                case 'w':
                    multiplier = timestampDriver.fromWeeks(1);
                    break;
                default:
                    throw SqlException.$(tolerance.position, "unsupported TOLERANCE unit [unit=").put(unit).put(']');
            }
            int maxValue = (int) Math.min(Long.MAX_VALUE / multiplier, Integer.MAX_VALUE);
            toleranceInterval = TimestampSamplerFactory.parseInterval(tolerance.token, k, tolerance.position, "tolerance", maxValue, unit);
            toleranceInterval *= multiplier;
        }
        return toleranceInterval;
    }

    private static int validateAndGetSlaveTimestampIndex(RecordMetadata slaveMetadata, RecordCursorFactory slaveBase) {
        int slaveTimestampIndex = slaveMetadata.getTimestampIndex();

        // slave.supportsFilterStealing() means slave is nothing but a filter.
        // if slave is just a filter, then it must have the same metadata as its base,
        // that includes the timestamp index.
        assert slaveBase.getMetadata().getTimestampIndex() == slaveTimestampIndex;
        return slaveTimestampIndex;
    }

    private static RecordMetadata widenSetMetadata(RecordMetadata typesA, RecordMetadata typesB) throws SqlException {
        int columnCount = typesA.getColumnCount();
        assert columnCount == typesB.getColumnCount();

        GenericRecordMetadata metadata = new GenericRecordMetadata();
        for (int i = 0; i < columnCount; i++) {
            int typeA = typesA.getColumnType(i);
            int typeB = typesB.getColumnType(i);
            int targetType = getUnionCastType(typeA, typeB);
            metadata.add(new TableColumnMetadata(typesA.getColumnName(i), targetType));
        }
        return metadata;
    }

    private VectorAggregateFunctionConstructor assembleFunctionReference(RecordMetadata metadata, ExpressionNode ast) {
        int columnIndex;
        if (ast.type == FUNCTION && ast.paramCount == 1 && isSumKeyword(ast.token) && ast.rhs.type == LITERAL) {
            columnIndex = metadata.getColumnIndex(ast.rhs.token);
            tempVecConstructorArgIndexes.add(columnIndex);
            return sumConstructors.get(metadata.getColumnType(columnIndex));
        } else if (ast.type == FUNCTION && isCountKeyword(ast.token)
                && (ast.paramCount == 0 || (ast.paramCount == 1 && ast.rhs.type == CONSTANT && !isNullKeyword(ast.rhs.token)))) {
            // count() is a no-arg function, count(1) is the same as count(*)
            tempVecConstructorArgIndexes.add(-1);
            return COUNT_CONSTRUCTOR;
        } else if (isSingleColumnFunction(ast, "count")) {
            columnIndex = metadata.getColumnIndex(ast.rhs.token);
            tempVecConstructorArgIndexes.add(columnIndex);
            return countConstructors.get(metadata.getColumnType(columnIndex));
        } else if (isSingleColumnFunction(ast, "ksum")) {
            columnIndex = metadata.getColumnIndex(ast.rhs.token);
            tempVecConstructorArgIndexes.add(columnIndex);
            return ksumConstructors.get(metadata.getColumnType(columnIndex));
        } else if (isSingleColumnFunction(ast, "nsum")) {
            columnIndex = metadata.getColumnIndex(ast.rhs.token);
            tempVecConstructorArgIndexes.add(columnIndex);
            return nsumConstructors.get(metadata.getColumnType(columnIndex));
        } else if (isSingleColumnFunction(ast, "avg")) {
            columnIndex = metadata.getColumnIndex(ast.rhs.token);
            tempVecConstructorArgIndexes.add(columnIndex);
            return avgConstructors.get(metadata.getColumnType(columnIndex));
        } else if (isSingleColumnFunction(ast, "min")) {
            columnIndex = metadata.getColumnIndex(ast.rhs.token);
            tempVecConstructorArgIndexes.add(columnIndex);
            return minConstructors.get(metadata.getColumnType(columnIndex));
        } else if (isSingleColumnFunction(ast, "max")) {
            columnIndex = metadata.getColumnIndex(ast.rhs.token);
            tempVecConstructorArgIndexes.add(columnIndex);
            return maxConstructors.get(metadata.getColumnType(columnIndex));
        }
        return null;
    }

    private boolean assembleKeysAndFunctionReferences(
            ObjList<QueryColumn> columns,
            RecordMetadata metadata,
            int hourFunctionIndex
    ) throws SqlException {
        tempVaf.clear();
        tempMetadata.clear();
        tempSymbolSkewIndexes.clear();
        tempVecConstructors.clear();
        tempVecConstructorArgIndexes.clear();
        tempAggIndex.clear();

        for (int i = 0, n = columns.size(); i < n; i++) {
            final QueryColumn qc = columns.getQuick(i);
            final ExpressionNode ast = qc.getAst();
            if (ast.type == LITERAL) {
                // when "hour" index is not set (-1) we assume we should be looking for
                // intrinsic cases, such as "columnRef, sum(col)"
                if (hourFunctionIndex == -1) {
                    final int columnIndex = metadata.getColumnIndexQuiet(ast.token);
                    if (columnIndex < 0) {
                        throw SqlException.invalidColumn(ast.position, ast.token);
                    }
                    final int type = metadata.getColumnType(columnIndex);
                    if (isInt(type)) {
                        tempKeyIndexesInBase.add(columnIndex);
                        tempKeyIndex.add(i);
                        arrayColumnTypes.add(INT);
                        tempKeyKinds.add(GKK_VANILLA_INT);
                    } else if (isSymbol(type)) {
                        tempKeyIndexesInBase.add(columnIndex);
                        tempKeyIndex.add(i);
                        tempSymbolSkewIndexes.extendAndSet(i, columnIndex);
                        arrayColumnTypes.add(SYMBOL);
                        tempKeyKinds.add(GKK_VANILLA_INT);
                    } else {
                        return false;
                    }
                }
            } else if (i != hourFunctionIndex) { // we exclude "hour" call from aggregate lookups
                final VectorAggregateFunctionConstructor constructor = assembleFunctionReference(metadata, ast);
                if (constructor != null) {
                    tempVecConstructors.add(constructor);
                    tempAggIndex.add(i);
                } else {
                    return false;
                }
            }
        }
        return true;
    }

    private void backupWhereClause(ExpressionNode node) {
        processNodeQueryModels(node, backupWhereClauseRef);
    }

    private GenericRecordMetadata buildQueryMetadata(
            @Transient QueryModel model,
            @Transient SqlExecutionContext executionContext,
            @Transient RecordMetadata metadata,
            int readerTimestampIndex,
            boolean requiresTimestamp,
            @Nullable IntList columnIndexes,
            @Nullable IntList columnSizeShifts
    ) throws SqlException {
        final ObjList<QueryColumn> topDownColumns = model.getTopDownColumns();
        final int topDownColumnCount = topDownColumns.size();
        final GenericRecordMetadata queryMeta = new GenericRecordMetadata();

        try {
            if (requiresTimestamp) {
                executionContext.pushTimestampRequiredFlag(true);
            }

            boolean contextTimestampRequired = executionContext.isTimestampRequired();
            // some "sample by" queries don't select any cols but needs timestamp col selected
            // for example "select count() from x sample by 1h" implicitly needs timestamp column selected
            if (topDownColumnCount > 0 || contextTimestampRequired || model.isUpdate()) {
                for (int i = 0; i < topDownColumnCount; i++) {
                    QueryColumn column = topDownColumns.getQuick(i);
                    int columnIndex = metadata.getColumnIndexQuiet(column.getName());
                    if (columnIndex == -1) {
                        throw SqlException.invalidColumn(column.getAst().position, column.getName());
                    }
                    int type = metadata.getColumnType(columnIndex);

                    if (columnIndexes != null) {
                        columnIndexes.add(columnIndex);
                    }
                    if (columnSizeShifts != null) {
                        int typeSize = ColumnType.sizeOf(type);
                        columnSizeShifts.add(Numbers.msb(typeSize));
                    }

                    queryMeta.add(new TableColumnMetadata(
                            metadata.getColumnName(columnIndex),
                            type,
                            metadata.isColumnIndexed(columnIndex),
                            metadata.getIndexValueBlockCapacity(columnIndex),
                            metadata.isSymbolTableStatic(columnIndex),
                            metadata.getMetadata(columnIndex),
                            -1,
                            false,
                            0,
                            metadata.getColumnMetadata(columnIndex).isSymbolCacheFlag(),
                            metadata.getColumnMetadata(columnIndex).getSymbolCapacity()
                    ));

                    if (columnIndex == readerTimestampIndex) {
                        queryMeta.setTimestampIndex(queryMeta.getColumnCount() - 1);
                    }
                }

                // select timestamp when it is required but not already selected
                if (readerTimestampIndex != -1 && queryMeta.getTimestampIndex() == -1 && contextTimestampRequired) {
                    int timestampType = metadata.getColumnType(readerTimestampIndex);
                    queryMeta.add(new TableColumnMetadata(
                            metadata.getColumnName(readerTimestampIndex),
                            timestampType,
                            metadata.getMetadata(readerTimestampIndex)
                    ));
                    queryMeta.setTimestampIndex(queryMeta.getColumnCount() - 1);

                    if (columnIndexes != null) {
                        columnIndexes.add(readerTimestampIndex);
                    }

                    if (columnSizeShifts != null) {
                        columnSizeShifts.add(Numbers.msb(ColumnType.sizeOf(timestampType)));
                    }
                }
            }
        } finally {
            if (requiresTimestamp) {
                executionContext.popTimestampRequiredFlag();
            }
        }

        return queryMeta;
    }

    // Checks if lo, hi is set and lo >= 0 while hi < 0 (meaning - return whole result set except some rows at start and some at the end)
    // because such case can't really be optimized by topN/bottomN
    private boolean canSortAndLimitBeOptimized(QueryModel model, SqlExecutionContext context, Function loFunc, Function hiFunc) {
        if (model.getLimitLo() == null && model.getLimitHi() == null) {
            return false;
        }

        if (loFunc != null && loFunc.isConstant()
                && hiFunc != null && hiFunc.isConstant()) {
            try {
                loFunc.init(null, context);
                hiFunc.init(null, context);

                return !(loFunc.getLong(null) >= 0 && hiFunc.getLong(null) < 0);
            } catch (SqlException ex) {
                LOG.error().$("Failed to initialize lo or hi functions [").$("error=").$safe(ex.getMessage()).I$();
            }
        }

        return true;
    }

    private boolean checkIfSetCastIsRequired(RecordMetadata metadataA, RecordMetadata metadataB, boolean symbolDisallowed) {
        int columnCount = metadataA.getColumnCount();
        assert columnCount == metadataB.getColumnCount();

        for (int i = 0; i < columnCount; i++) {
            int typeA = metadataA.getColumnType(i);
            int typeB = metadataB.getColumnType(i);
            if (typeA != typeB || (typeA == SYMBOL && symbolDisallowed)) {
                return true;
            }
        }
        return false;
    }

    @Nullable
    private Function compileFilter(
            IntrinsicModel intrinsicModel,
            RecordMetadata readerMeta,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (intrinsicModel.filter != null) {
            return compileBooleanFilter(intrinsicModel.filter, readerMeta, executionContext);
        }
        return null;
    }

    private @Nullable ObjList<ObjList<Function>> compilePerWorkerInnerProjectionFunctions(
            SqlExecutionContext executionContext,
            ObjList<QueryColumn> queryColumns,
            ObjList<Function> innerProjectionFunctions,
            int workerCount,
            RecordMetadata metadata
    ) throws SqlException {
        boolean threadSafe = true;

        assert innerProjectionFunctions.size() == queryColumns.size();

        for (int i = 0, n = innerProjectionFunctions.size(); i < n; i++) {
            if (!innerProjectionFunctions.getQuick(i).isThreadSafe()) {
                threadSafe = false;
                break;
            }
        }
        if (!threadSafe) {
            ObjList<ObjList<Function>> allWorkerKeyFunctions = new ObjList<>();
            int columnCount = queryColumns.size();
            for (int i = 0; i < workerCount; i++) {
                ObjList<Function> workerKeyFunctions = new ObjList<>(columnCount);
                allWorkerKeyFunctions.add(workerKeyFunctions);
                for (int j = 0; j < columnCount; j++) {
                    final Function func = functionParser.parseFunction(
                            queryColumns.getQuick(j).getAst(),
                            metadata,
                            executionContext
                    );
                    if (func instanceof GroupByFunction) {
                        // ensure value indexes are set correctly
                        ((GroupByFunction) func).initValueIndex(((GroupByFunction) innerProjectionFunctions.getQuick(j)).getValueIndex());
                    }
                    workerKeyFunctions.add(func);
                }
            }
            return allWorkerKeyFunctions;
        }
        return null;
    }

    private @Nullable ObjList<Function> compileWorkerFilterConditionally(
            SqlExecutionContext executionContext,
            @Nullable Function filter,
            int sharedQueryWorkerCount,
            @Nullable ExpressionNode filterExpr,
            RecordMetadata metadata
    ) throws SqlException {
        if (filter != null && !filter.isThreadSafe() && sharedQueryWorkerCount > 0) {
            assert filterExpr != null;
            ObjList<Function> workerFilters = new ObjList<>();
            for (int i = 0; i < sharedQueryWorkerCount; i++) {
                restoreWhereClause(filterExpr); // restore original filters in node query models
                Function workerFilter = compileBooleanFilter(filterExpr, metadata, executionContext);
                workerFilters.extendAndSet(i, workerFilter);
                assert filter.getClass() == workerFilter.getClass();
            }
            return workerFilters;
        }
        return null;
    }

    private @Nullable ObjList<ObjList<GroupByFunction>> compileWorkerGroupByFunctionsConditionally(
            SqlExecutionContext executionContext,
            QueryModel model,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            int workerCount,
            RecordMetadata metadata
    ) throws SqlException {
        boolean threadSafe = true;
        for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
            if (!groupByFunctions.getQuick(i).isThreadSafe()) {
                threadSafe = false;
                break;
            }
        }
        if (!threadSafe) {
            ObjList<ObjList<GroupByFunction>> allWorkerGroupByFunctions = new ObjList<>();
            for (int i = 0; i < workerCount; i++) {
                ObjList<GroupByFunction> workerGroupByFunctions = new ObjList<>(groupByFunctions.size());
                allWorkerGroupByFunctions.extendAndSet(i, workerGroupByFunctions);
                GroupByUtils.prepareWorkerGroupByFunctions(
                        model,
                        metadata,
                        functionParser,
                        executionContext,
                        groupByFunctions,
                        workerGroupByFunctions
                );
            }
            return allWorkerGroupByFunctions;
        }
        return null;
    }

    @NotNull
    private RecordCursorFactory createFullFatJoin(
            RecordCursorFactory master,
            RecordMetadata masterMetadata,
            CharSequence masterAlias,
            RecordCursorFactory slave,
            RecordMetadata slaveMetadata,
            CharSequence slaveAlias,
            int joinPosition,
            FullFatJoinGenerator generator,
            JoinContext joinContext,
            long toleranceInterval
    ) throws SqlException {
        // create hash set of key columns to easily find them
        intHashSet.clear();
        for (int i = 0, n = listColumnFilterA.getColumnCount(); i < n; i++) {
            intHashSet.add(listColumnFilterA.getColumnIndexFactored(i));
        }

        // map doesn't support variable length types in map value, which is ok
        // when we join tables on strings - technically string is the key,
        // and we do not need to store it in value, but we will still reject
        //
        // never mind, this is a stop-gap measure until I understand the problem
        // fully

        for (int k = 0, m = slaveMetadata.getColumnCount(); k < m; k++) {
            if (intHashSet.excludes(k)) {
                // if a slave column is not in key, it must be of fixed length.
                // why? our maps do not support variable length types in values, only in keys
                if (isVarSize(slaveMetadata.getColumnType(k))) {
                    throw SqlException
                            .position(joinPosition).put("right side column '")
                            .put(slaveMetadata.getColumnName(k)).put("' is of unsupported type");
                }
            }
        }

        // at this point listColumnFilterB has column indexes of the master record that are JOIN keys
        // so masterCopier writes key columns of master record to a sink
        RecordSink masterCopier = createRecordCopierMaster(masterMetadata);

        // This metadata allocates native memory, it has to be closed in case join
        // generation is unsuccessful. The exception can be thrown anywhere between
        // try...catch
        JoinRecordMetadata metadata = new JoinRecordMetadata(
                configuration,
                masterMetadata.getColumnCount() + slaveMetadata.getColumnCount()
        );

        try {
            // metadata will have master record verbatim
            metadata.copyColumnMetadataFrom(masterAlias, masterMetadata);

            // slave record is split across key and value of map
            // the rationale is not to store columns twice
            // especially when map value does not support variable
            // length types

            final IntList columnIndex = new IntList(slaveMetadata.getColumnCount());
            // In map record value columns go first, so at this stage
            // we add to metadata all slave columns that are not keys.
            // Add the same columns to filter while we are in this loop.

            // We clear listColumnFilterB because after this loop it will
            // contain indexes of slave table columns that are not keys.
            ColumnFilter masterTableKeyColumns = listColumnFilterB.copy();
            listColumnFilterB.clear();
            valueTypes.clear();
            ArrayColumnTypes slaveTypes = new ArrayColumnTypes();
            int slaveTimestampIndex = slaveMetadata.getTimestampIndex();
            int slaveValueTimestampIndex = -1;
            for (int i = 0, n = slaveMetadata.getColumnCount(); i < n; i++) {
                if (intHashSet.excludes(i)) {
                    // this is not a key column. Add it to metadata as it is. Symbols columns are kept as symbols
                    final TableColumnMetadata m = slaveMetadata.getColumnMetadata(i);
                    metadata.add(slaveAlias, m);
                    listColumnFilterB.add(i + 1);
                    columnIndex.add(i);
                    valueTypes.add(m.getColumnType());
                    slaveTypes.add(m.getColumnType());
                    if (i == slaveTimestampIndex) {
                        slaveValueTimestampIndex = valueTypes.getColumnCount() - 1;
                    }
                }
            }
            assert slaveValueTimestampIndex != -1;

            // now add key columns to metadata
            for (int i = 0, n = listColumnFilterA.getColumnCount(); i < n; i++) {
                int index = listColumnFilterA.getColumnIndexFactored(i);
                final TableColumnMetadata m = slaveMetadata.getColumnMetadata(index);
                metadata.add(slaveAlias, m);
                slaveTypes.add(m.getColumnType());
                columnIndex.add(index);
            }

            if (masterMetadata.getTimestampIndex() != -1) {
                metadata.setTimestampIndex(masterMetadata.getTimestampIndex());
            }
            return generator.create(
                    configuration,
                    metadata,
                    master,
                    slave,
                    keyTypes,
                    valueTypes,
                    slaveTypes,
                    masterCopier,
                    createRecordCopierSlave(slaveMetadata),
                    masterMetadata.getColumnCount(),
                    RecordValueSinkFactory.getInstance(asm, slaveMetadata, listColumnFilterB), // slaveValueSink
                    columnIndex,
                    joinContext,
                    masterTableKeyColumns,
                    toleranceInterval,
                    slaveValueTimestampIndex
            );

        } catch (Throwable e) {
            Misc.free(metadata);
            throw e;
        }
    }

    private RecordCursorFactory createHashJoin(
            JoinRecordMetadata metadata,
            RecordCursorFactory master,
            RecordCursorFactory slave,
            int joinType,
            Function filter,
            JoinContext context
    ) {
        /*
         * JoinContext provides the following information:
         * a/bIndexes - index of model where join column is coming from
         * a/bNames - name of columns in respective models, these column names are not prefixed with table aliases
         * a/bNodes - the original column references, that can include table alias. Sometimes it doesn't when column name is unambiguous
         *
         * a/b are "inverted" in that "a" for slave and "b" for master
         *
         * The issue is when we use model indexes and vanilla column names they would only work on single-table
         * record cursor but original names with prefixed columns will only work with JoinRecordMetadata
         */
        final RecordMetadata masterMetadata = master.getMetadata();
        final RecordMetadata slaveMetadata = slave.getMetadata();
        final RecordSink masterKeyCopier = createRecordCopierMaster(masterMetadata);
        final RecordSink slaveKeyCopier = createRecordCopierSlave(slaveMetadata);

        if (slave.recordCursorSupportsRandomAccess() && !fullFatJoins) {
            valueTypes.clear();
            valueTypes.add(INT); // chain tail offset

            if (joinType == JOIN_INNER) {
                // For inner join we can also store per-key count to speed up size calculation.
                valueTypes.add(INT); // record count for the key

                return new HashJoinLightRecordCursorFactory(
                        configuration,
                        metadata,
                        master,
                        slave,
                        keyTypes,
                        valueTypes,
                        masterKeyCopier,
                        slaveKeyCopier,
                        masterMetadata.getColumnCount(),
                        context
                );
            }

            if (joinType == JOIN_RIGHT_OUTER || joinType == JOIN_FULL_OUTER) {
                valueTypes.add(BOOLEAN);
            }
            if (filter != null) {
                return new HashOuterJoinFilteredLightRecordCursorFactory(
                        configuration,
                        metadata,
                        master,
                        slave,
                        keyTypes,
                        valueTypes,
                        masterKeyCopier,
                        slaveKeyCopier,
                        masterMetadata.getColumnCount(),
                        filter,
                        context,
                        joinType
                );
            }

            return new HashOuterJoinLightRecordCursorFactory(
                    configuration,
                    metadata,
                    master,
                    slave,
                    keyTypes,
                    valueTypes,
                    masterKeyCopier,
                    slaveKeyCopier,
                    masterMetadata.getColumnCount(),
                    context,
                    joinType
            );
        }

        valueTypes.clear();
        valueTypes.add(LONG); // chain head offset
        valueTypes.add(LONG); // chain tail offset
        valueTypes.add(LONG); // record count for the key
        if (filter == null && (joinType == JOIN_RIGHT_OUTER || joinType == JOIN_FULL_OUTER)) {
            valueTypes.add(BOOLEAN);
        }

        entityColumnFilter.of(slaveMetadata.getColumnCount());
        RecordSink slaveSink = RecordSinkFactory.getInstance(asm, slaveMetadata, entityColumnFilter, configuration);

        if (joinType == JOIN_INNER) {
            return new HashJoinRecordCursorFactory(
                    configuration,
                    metadata,
                    master,
                    slave,
                    keyTypes,
                    valueTypes,
                    masterKeyCopier,
                    slaveKeyCopier,
                    slaveSink,
                    masterMetadata.getColumnCount(),
                    context
            );
        }

        if (filter != null) {
            return new HashOuterJoinFilteredRecordCursorFactory(
                    configuration,
                    metadata,
                    master,
                    slave,
                    keyTypes,
                    valueTypes,
                    masterKeyCopier,
                    slaveKeyCopier,
                    slaveSink,
                    masterMetadata.getColumnCount(),
                    filter,
                    context,
                    joinType
            );
        }

        return new HashOuterJoinRecordCursorFactory(
                configuration,
                metadata,
                master,
                slave,
                keyTypes,
                valueTypes,
                masterKeyCopier,
                slaveKeyCopier,
                slaveSink,
                masterMetadata.getColumnCount(),
                context,
                joinType
        );
    }

    @NotNull
    private JoinRecordMetadata createJoinMetadata(
            CharSequence masterAlias,
            RecordMetadata masterMetadata,
            CharSequence slaveAlias,
            RecordMetadata slaveMetadata
    ) {
        return createJoinMetadata(
                masterAlias,
                masterMetadata,
                slaveAlias,
                slaveMetadata,
                masterMetadata.getTimestampIndex()
        );
    }

    @NotNull
    private JoinRecordMetadata createJoinMetadata(
            CharSequence masterAlias,
            RecordMetadata masterMetadata,
            CharSequence slaveAlias,
            RecordMetadata slaveMetadata,
            int timestampIndex
    ) {
        JoinRecordMetadata metadata;
        metadata = new JoinRecordMetadata(
                configuration,
                masterMetadata.getColumnCount() + slaveMetadata.getColumnCount()
        );

        try {
            metadata.copyColumnMetadataFrom(masterAlias, masterMetadata);
            metadata.copyColumnMetadataFrom(slaveAlias, slaveMetadata);
        } catch (Throwable th) {
            Misc.free(metadata);
            throw th;
        }

        if (timestampIndex != -1) {
            metadata.setTimestampIndex(timestampIndex);
        }
        return metadata;
    }

    private @NotNull RecordSink createRecordCopierMaster(RecordMetadata masterMetadata) {
        return RecordSinkFactory.getInstance(
                asm,
                masterMetadata,
                listColumnFilterB,
                writeSymbolAsString,
                writeStringAsVarcharB,
                writeTimestampAsNanosB,
                configuration
        );
    }

    private @NotNull RecordSink createRecordCopierSlave(RecordMetadata slaveMetadata) {
        return RecordSinkFactory.getInstance(
                asm,
                slaveMetadata,
                listColumnFilterA,
                writeSymbolAsString,
                writeStringAsVarcharA,
                writeTimestampAsNanosA,
                configuration
        );
    }

    private RecordCursorFactory createSpliceJoin(
            RecordMetadata metadata,
            RecordCursorFactory master,
            RecordSink masterKeySink,
            RecordCursorFactory slave,
            RecordSink slaveKeySink,
            int columnSplit,
            JoinContext context
    ) {
        valueTypes.clear();
        valueTypes.add(LONG); // master previous
        valueTypes.add(LONG); // master current
        valueTypes.add(LONG); // slave previous
        valueTypes.add(LONG); // slave current

        return new SpliceJoinLightRecordCursorFactory(
                configuration,
                metadata,
                master,
                slave,
                keyTypes,
                valueTypes,
                masterKeySink,
                slaveKeySink,
                columnSplit,
                context
        );
    }

    private @NotNull SymbolShortCircuit createSymbolShortCircuit(
            RecordMetadata masterMetadata,
            RecordMetadata slaveMetadata,
            boolean isSelfJoin
    ) {
        SymbolShortCircuit symbolShortCircuit = NoopSymbolShortCircuit.INSTANCE;
        assert listColumnFilterA.getColumnCount() == listColumnFilterB.getColumnCount();
        SymbolJoinKeyMapping[] mappings = null;
        for (int i = 0, n = listColumnFilterA.getColumnCount(); i < n; i++) {
            int masterIndex = listColumnFilterB.getColumnIndexFactored(i);
            int slaveIndex = listColumnFilterA.getColumnIndexFactored(i);
            if (slaveMetadata.getColumnType(slaveIndex) == ColumnType.SYMBOL && slaveMetadata.isSymbolTableStatic(slaveIndex)) {
                int masterColType = masterMetadata.getColumnType(masterIndex);
                SymbolJoinKeyMapping newMapping;
                switch (masterColType) {
                    case SYMBOL:
                        if (isSelfJoin && masterIndex == slaveIndex) {
                            // self join on the same column -> there is no point in attempting short-circuiting
                            // NOTE: This check is naive, it can generate false positives
                            //       For example 'select t1.s, t2.s2 from t as t1 asof join t as t2 on t1.s = t2.s2'
                            //       This is deemed as a self-join (which it is), and due to the way columns are projected
                            //       it will take this branch (even when it fact it's comparing different columns)
                            //       and won't create a short circuit. This is OK from correctness perspective,
                            //       but it is a missed opportunity for performance optimization. Doing a perfect check
                            //       would require a more complex logic, which is not worth it for now
                            continue;
                        }
                        newMapping = new SymbolToSymbolJoinKeyMapping(configuration, masterIndex, slaveIndex);
                        break;
                    case VARCHAR:
                        newMapping = new VarcharToSymbolJoinKeyMapping(masterIndex, slaveIndex);
                        break;
                    case STRING:
                        newMapping = new StringToSymbolJoinKeyMapping(masterIndex, slaveIndex);
                        break;
                    default:
                        // unsupported type for short circuit
                        continue;
                }
                if (symbolShortCircuit == NoopSymbolShortCircuit.INSTANCE) {
                    // ok, a single symbol short circuit
                    symbolShortCircuit = (SymbolShortCircuit) newMapping;
                } else if (mappings == null) {
                    // 2 symbol mappings, we need to chain them
                    mappings = new SymbolJoinKeyMapping[2];
                    mappings[0] = (SymbolJoinKeyMapping) symbolShortCircuit;
                    mappings[1] = newMapping;
                    symbolShortCircuit = new ChainedSymbolShortCircuit(mappings);
                } else {
                    // ok, this is pretty uncommon - a join key with more than 2 symbol short circuits
                    // this allocates arrays, but it should be very rare
                    int size = mappings.length;
                    SymbolJoinKeyMapping[] newMappings = Arrays.copyOf(mappings, size + 1);
                    newMappings[size] = newMapping;
                    symbolShortCircuit = new ChainedSymbolShortCircuit(newMappings);
                    mappings = newMappings;
                }
            }
        }
        return symbolShortCircuit;
    }

    /**
     * Attempts to detect the "markout horizon" pattern in a cross-join with ORDER BY.
     * <p>
     * The pattern consists of:
     * <ol>
     *   <li>A cross-join between a table with a designated timestamp and an arithmetic sequence
     *   <li>An ORDER BY clause on the sum of the timestamp and the sequence element
     * </ol>
     * <p>
     * Detection isn't foolproof and can result in false positives. We do not attempt it unless
     * the hint {@value SqlHints#MARKOUT_HORIZON_HINT} is present in the query.
     * <p>
     * These are the prerequisites for the Markout Horizon optimization to kick in:
     * <ol>
     *  <li><code>markout_horizon(lhs rhs)</code> hint is present
     *  <li>LHS (master) cursor supports random access
     *  <li>RHS (slave) cursor originates from the <code>long_sequence</code> function
     *  <li>LHS table has a designated timestamp column
     *  <li><code>ORDER BY</code> is a sum of two operands
     *  <li>one operand is the designated timestamp of LHS table
     *  <li>the other operand is a column of the RHS table
     * </ol>
     *
     * @param masterAlias         alias for the master LHS of the join
     * @param masterModel         QueryModel for the LHS of the join
     * @param masterMetadata      Metadata for the LHS of the join
     * @param masterCursorFactory the LHS cursor factory
     * @param slaveModel          QueryModel for the RHS of the join
     * @param slaveMetadata       Metadata for the RHS of the join (should be an arithmetic sequence)
     * @param slaveCursorFactory  the RHS cursor factory (should produce an arithmetic sequence)
     * @return MarkoutHorizonInfo if pattern is detected, null otherwise
     */
    private MarkoutHorizonInfo detectMarkoutHorizonPattern(
            CharSequence masterAlias,
            QueryModel masterModel,
            RecordMetadata masterMetadata,
            RecordCursorFactory masterCursorFactory,
            QueryModel slaveModel,
            RecordMetadata slaveMetadata,
            RecordCursorFactory slaveCursorFactory
    ) {
        // Detect the markout_horizon hint, and ensure the master cursor supports random access
        if (!SqlHints.hasMarkoutHorizonHint(masterModel, masterAlias, slaveModel.getName()) ||
                !masterCursorFactory.recordCursorSupportsRandomAccess()
        ) {
            return null;
        }

        // Ensure the slave table is `long_sequence()`, or based on it
        RecordCursorFactory rootFac = slaveCursorFactory;
        while (!rootFac.getClass().getSimpleName().contains("LongSequence")) {
            rootFac = rootFac.getBaseFactory();
            if (rootFac == null) {
                return null;
            }
        }
        // Ensure the master table has a designated timestamp column
        int designatedTimestampIndex = masterMetadata.getTimestampIndex();
        if (designatedTimestampIndex == -1) {
            return null;
        }

        // Ensure we have an ORDER BY clause.
        // lastSeenOrderByModel was captured as we were descending through nested models
        if (lastSeenOrderByModel == null) {
            return null;
        }
        // Ensure there's only one sort column
        if (lastSeenOrderByModel.getOrderBy().size() != 1) {
            return null;
        }
        // Ensure sort direction is ascending
        IntList dirs = lastSeenOrderByModel.getOrderByDirection();
        int dir = (dirs.size() > 0) ? dirs.getQuick(0) : ORDER_DIRECTION_ASCENDING;
        if (dir != ORDER_DIRECTION_ASCENDING) {
            return null;
        }

        // The ORDER BY node may be a column alias (LITERAL) - resolve it to the actual expression
        ExpressionNode orderByNode = lastSeenOrderByModel.getOrderBy().getQuick(0);
        ExpressionNode orderByExpr = orderByNode;
        if (orderByNode.type == ExpressionNode.LITERAL) {
            QueryColumn queryColumn = lastSeenOrderByModel.getAliasToColumnMap().get(orderByNode.token);
            if (queryColumn != null) {
                orderByExpr = queryColumn.getAst();
            }
        }
        // Ensure that ORDER BY is an addition operation
        if (orderByExpr.type != ExpressionNode.OPERATION || !"+".contentEquals(orderByExpr.token)) {
            return null;
        }
        // The ORDER BY should be timestamp_column + offset_column, or vice versa
        ExpressionNode lhs = orderByExpr.lhs;
        ExpressionNode rhs = orderByExpr.rhs;
        if (lhs == null || rhs == null) {
            return null;
        }

        // Try to identify which operand is the timestamp from master and which is from slave
        int timestampColumnIndex = -1;
        ExpressionNode slaveColumnNode = null;

        // Check if LHS is the designated timestamp column from master
        if (lhs.type == ExpressionNode.LITERAL) {
            int colIndex = masterMetadata.getColumnIndexQuiet(lhs.token);
            if (colIndex >= 0 && colIndex == designatedTimestampIndex) {
                timestampColumnIndex = colIndex;
                slaveColumnNode = rhs;
            }
        }

        // If not found, check if RHS is the designated timestamp column from master and LHS is from slave
        if (timestampColumnIndex == -1 && rhs.type == ExpressionNode.LITERAL) {
            int colIndex = masterMetadata.getColumnIndexQuiet(rhs.token);
            if (colIndex >= 0 && colIndex == designatedTimestampIndex) {
                timestampColumnIndex = colIndex;
                slaveColumnNode = lhs;
            }
        }

        if (slaveColumnNode == null || slaveColumnNode.type != ExpressionNode.LITERAL) {
            return null;
        }
        int slaveColumnIndex = slaveMetadata.getColumnIndexQuiet(slaveColumnNode.token);
        if (slaveColumnIndex == -1) {
            return null; // Slave column not found
        }
        return markoutHorizonInfo.of(timestampColumnIndex, slaveColumnIndex);
    }

    private @NotNull ObjList<Function> extractVirtualFunctionsFromProjection(ObjList<Function> projectionFunctions, IntList projectionFunctionFlags) {
        final ObjList<Function> result = new ObjList<>();
        for (int i = 0, n = projectionFunctions.size(); i < n; i++) {
            if (projectionFunctionFlags.getQuick(i) == GroupByUtils.PROJECTION_FUNCTION_FLAG_VIRTUAL) {
                result.add(projectionFunctions.getQuick(i));
            }
        }
        return result;
    }

    private ObjList<Function> generateCastFunctions(
            SqlExecutionContext executionContext,
            RecordMetadata castToMetadata,
            RecordMetadata castFromMetadata,
            int modelPosition
    ) throws SqlException {
        int columnCount = castToMetadata.getColumnCount();
        ObjList<Function> castFunctions = new ObjList<>();
        for (int i = 0; i < columnCount; i++) {
            int toType = castToMetadata.getColumnType(i);
            int fromType = castFromMetadata.getColumnType(i);
            int toTag = tagOf(toType);
            int fromTag = tagOf(fromType);
            if (fromTag == NULL) {
                castFunctions.add(NullConstant.NULL);
            } else {
                switch (toTag) {
                    case BOOLEAN:
                        castFunctions.add(BooleanColumn.newInstance(i));
                        break;
                    case BYTE:
                        castFunctions.add(ByteColumn.newInstance(i));
                        break;
                    case SHORT:
                        switch (fromTag) {
                            // BOOLEAN will not be cast to CHAR
                            // in cast of BOOLEAN -> CHAR combination both will be cast to STRING
                            case BYTE:
                                castFunctions.add(ByteColumn.newInstance(i));
                                break;
                            case CHAR:
                                castFunctions.add(new CharColumn(i));
                                break;
                            case SHORT:
                                castFunctions.add(ShortColumn.newInstance(i));
                                break;
                            // wider types are not possible here
                            // SHORT will be cast to wider types, not other way around
                            // Wider types tested are: SHORT, INT, LONG, FLOAT, DOUBLE, DATE, TIMESTAMP, SYMBOL, STRING, LONG256
                            // GEOBYTE, GEOSHORT, GEOINT, GEOLONG
                        }
                        break;
                    case CHAR:
                        switch (fromTag) {
                            // BOOLEAN will not be cast to CHAR
                            // in cast of BOOLEAN -> CHAR combination both will be cast to STRING
                            case BYTE:
                                castFunctions.add(new CastByteToCharFunctionFactory.Func(ByteColumn.newInstance(i)));
                                break;
                            case CHAR:
                                castFunctions.add(new CharColumn(i));
                                break;
                            // wider types are not possible here
                            // CHAR will be cast to wider types, not other way around
                            // Wider types tested are: SHORT, INT, LONG, FLOAT, DOUBLE, DATE, TIMESTAMP, SYMBOL, STRING, LONG256
                            // GEOBYTE, GEOSHORT, GEOINT, GEOLONG
                            default:
                        }
                        break;
                    case INT:
                        switch (fromTag) {
                            // BOOLEAN will not be cast to INT
                            // in cast of BOOLEAN -> INT combination both will be cast to STRING
                            case BYTE:
                                castFunctions.add(ByteColumn.newInstance(i));
                                break;
                            case SHORT:
                                castFunctions.add(ShortColumn.newInstance(i));
                                break;
                            case CHAR:
                                castFunctions.add(new CharColumn(i));
                                break;
                            case INT:
                                castFunctions.add(IntColumn.newInstance(i));
                                break;
                            // wider types are not possible here
                            // INT will be cast to wider types, not other way around
                            // Wider types tested are: LONG, FLOAT, DOUBLE, DATE, TIMESTAMP, SYMBOL, STRING, LONG256
                            // GEOBYTE, GEOSHORT, GEOINT, GEOLONG
                        }
                        break;
                    case IPv4:
                        if (fromTag == IPv4) {
                            castFunctions.add(IPv4Column.newInstance(i));
                        } else {
                            throw SqlException.unsupportedCast(
                                    modelPosition,
                                    castFromMetadata.getColumnName(i),
                                    fromType,
                                    toType
                            );
                        }
                        break;
                    case LONG:
                        switch (fromTag) {
                            // BOOLEAN will not be cast to LONG
                            // in cast of BOOLEAN -> LONG combination both will be cast to STRING
                            case BYTE:
                                castFunctions.add(ByteColumn.newInstance(i));
                                break;
                            case SHORT:
                                castFunctions.add(ShortColumn.newInstance(i));
                                break;
                            case CHAR:
                                castFunctions.add(new CharColumn(i));
                                break;
                            case INT:
                                castFunctions.add(IntColumn.newInstance(i));
                                break;
                            case LONG:
                                castFunctions.add(LongColumn.newInstance(i));
                                break;
                            default:
                                throw SqlException.unsupportedCast(
                                        modelPosition,
                                        castFromMetadata.getColumnName(i),
                                        fromType,
                                        toType
                                );
                                // wider types are not possible here
                                // LONG will be cast to wider types, not other way around
                                // Wider types tested are: FLOAT, DOUBLE, DATE, TIMESTAMP, SYMBOL, STRING, LONG256
                                // GEOBYTE, GEOSHORT, GEOINT, GEOLONG
                        }
                        break;
                    case DATE:
                        if (fromTag == DATE) {
                            castFunctions.add(DateColumn.newInstance(i));
                        } else {
                            throw SqlException.unsupportedCast(
                                    modelPosition,
                                    castFromMetadata.getColumnName(i),
                                    fromType,
                                    toType
                            );
                        }
                        break;
                    case UUID:
                        assert fromTag == UUID;
                        castFunctions.add(UuidColumn.newInstance(i));
                        break;
                    case TIMESTAMP:
                        switch (fromTag) {
                            case DATE:
                                castFunctions.add(new CastDateToTimestampFunctionFactory.Func(DateColumn.newInstance(i), toType));
                                break;
                            case TIMESTAMP:
                                if (fromType == toType) {
                                    castFunctions.add(TimestampColumn.newInstance(i, fromType));
                                } else {
                                    castFunctions.add(new CastTimestampToTimestampFunctionFactory.Func(TimestampColumn.newInstance(i, fromType), fromType, toType));
                                }
                                break;
                            default:
                                throw SqlException.unsupportedCast(
                                        modelPosition,
                                        castFromMetadata.getColumnName(i),
                                        fromType,
                                        toType
                                );
                        }
                        break;
                    case FLOAT:
                        switch (fromTag) {
                            case BYTE:
                                castFunctions.add(ByteColumn.newInstance(i));
                                break;
                            case SHORT:
                                castFunctions.add(ShortColumn.newInstance(i));
                                break;
                            case INT:
                                castFunctions.add(IntColumn.newInstance(i));
                                break;
                            case LONG:
                                castFunctions.add(LongColumn.newInstance(i));
                                break;
                            case FLOAT:
                                castFunctions.add(FloatColumn.newInstance(i));
                                break;
                            default:
                                throw SqlException.unsupportedCast(
                                        modelPosition,
                                        castFromMetadata.getColumnName(i),
                                        fromType,
                                        toType
                                );
                        }
                        break;
                    case DOUBLE:
                        switch (fromTag) {
                            case BYTE:
                                castFunctions.add(ByteColumn.newInstance(i));
                                break;
                            case SHORT:
                                castFunctions.add(ShortColumn.newInstance(i));
                                break;
                            case INT:
                                castFunctions.add(IntColumn.newInstance(i));
                                break;
                            case LONG:
                                castFunctions.add(LongColumn.newInstance(i));
                                break;
                            case FLOAT:
                                castFunctions.add(FloatColumn.newInstance(i));
                                break;
                            case DOUBLE:
                                castFunctions.add(DoubleColumn.newInstance(i));
                                break;
                            default:
                                throw SqlException.unsupportedCast(
                                        modelPosition,
                                        castFromMetadata.getColumnName(i),
                                        fromType,
                                        toType
                                );
                        }
                        break;
                    case STRING:
                        switch (fromTag) {
                            case BOOLEAN:
                                castFunctions.add(BooleanColumn.newInstance(i));
                                break;
                            case BYTE:
                                castFunctions.add(new CastByteToStrFunctionFactory.Func(ByteColumn.newInstance(i)));
                                break;
                            case SHORT:
                                castFunctions.add(new CastShortToStrFunctionFactory.Func(ShortColumn.newInstance(i)));
                                break;
                            case CHAR:
                                // CharFunction has built-in cast to String
                                castFunctions.add(new CharColumn(i));
                                break;
                            case INT:
                                castFunctions.add(new CastIntToStrFunctionFactory.Func(IntColumn.newInstance(i)));
                                break;
                            case LONG:
                                castFunctions.add(new CastLongToStrFunctionFactory.Func(LongColumn.newInstance(i)));
                                break;
                            case DATE:
                                castFunctions.add(new CastDateToStrFunctionFactory.Func(DateColumn.newInstance(i)));
                                break;
                            case TIMESTAMP:
                                castFunctions.add(new CastTimestampToStrFunctionFactory.Func(TimestampColumn.newInstance(i, fromType)));
                                break;
                            case FLOAT:
                                castFunctions.add(new CastFloatToStrFunctionFactory.Func(
                                        FloatColumn.newInstance(i)
                                ));
                                break;
                            case DOUBLE:
                                castFunctions.add(new CastDoubleToStrFunctionFactory.Func(
                                        DoubleColumn.newInstance(i)
                                ));
                                break;
                            case STRING:
                                castFunctions.add(new StrColumn(i));
                                break;
                            case VARCHAR:
                                // VarcharFunction has built-in cast to string
                                castFunctions.add(new VarcharColumn(i));
                                break;
                            case UUID:
                                castFunctions.add(new CastUuidToStrFunctionFactory.Func(UuidColumn.newInstance(i)));
                                break;
                            case SYMBOL:
                                castFunctions.add(
                                        new CastSymbolToStrFunctionFactory.Func(
                                                new SymbolColumn(i, castFromMetadata.isSymbolTableStatic(i))
                                        )
                                );
                                break;
                            case LONG256:
                                castFunctions.add(
                                        new CastLong256ToStrFunctionFactory.Func(
                                                Long256Column.newInstance(i)
                                        )
                                );
                                break;
                            case GEOBYTE:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.getGeoByteToStrCastFunction(
                                                GeoByteColumn.newInstance(i, fromType),
                                                getGeoHashBits(fromType)
                                        )
                                );
                                break;
                            case GEOSHORT:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.getGeoShortToStrCastFunction(
                                                GeoShortColumn.newInstance(i, fromType),
                                                getGeoHashBits(fromType)
                                        )
                                );
                                break;
                            case GEOINT:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.getGeoIntToStrCastFunction(
                                                GeoIntColumn.newInstance(i, fromType),
                                                getGeoHashBits(fromType)
                                        )
                                );
                                break;
                            case GEOLONG:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.getGeoLongToStrCastFunction(
                                                GeoLongColumn.newInstance(i, fromType),
                                                getGeoHashBits(fromType)
                                        )
                                );
                                break;
                            case ColumnType.DECIMAL8:
                            case ColumnType.DECIMAL16:
                            case ColumnType.DECIMAL32:
                            case ColumnType.DECIMAL64:
                                castFunctions.add(
                                        new CastDecimalToStrFunctionFactory.Func64(
                                                Decimal64LoaderFunctionFactory.getInstance(DecimalColumn.newInstance(i, fromType))
                                        )
                                );
                                break;
                            case ColumnType.DECIMAL128:
                                castFunctions.add(
                                        new CastDecimalToStrFunctionFactory.Func128(DecimalColumn.newInstance(i, fromType))
                                );
                                break;
                            case ColumnType.DECIMAL256:
                                castFunctions.add(
                                        new CastDecimalToStrFunctionFactory.Func(DecimalColumn.newInstance(i, fromType))
                                );
                                break;
                            case INTERVAL:
                                castFunctions.add(new CastIntervalToStrFunctionFactory.Func(IntervalColumn.newInstance(i, fromType)));
                                break;
                            case BINARY:
                                throw SqlException.unsupportedCast(
                                        modelPosition,
                                        castFromMetadata.getColumnName(i),
                                        fromType,
                                        toType
                                );
                            case ARRAY:
                                int arrayType = decodeArrayElementType(fromType);
                                if (arrayType != DOUBLE) {
                                    throw SqlException.unsupportedCast(
                                            modelPosition,
                                            castFromMetadata.getColumnName(i),
                                            fromType,
                                            toType
                                    );
                                }
                                castFunctions.add(new CastDoubleArrayToStrFunctionFactory.Func(ArrayColumn.newInstance(i, fromType)));
                                break;
                            case IPv4:
                                castFunctions.add(new CastIPv4ToStrFunctionFactory.Func(IPv4Column.newInstance(i)));
                                break;
                        }
                        break;
                    case SYMBOL:
                        castFunctions.add(new CastSymbolToStrFunctionFactory.Func(
                                new SymbolColumn(
                                        i,
                                        castFromMetadata.isSymbolTableStatic(i)
                                )
                        ));
                        break;
                    case LONG256:
                        castFunctions.add(Long256Column.newInstance(i));
                        break;
                    case GEOBYTE:
                        switch (fromTag) {
                            case STRING:
                                castFunctions.add(
                                        CastStrToGeoHashFunctionFactory.newInstance(
                                                0,
                                                toType,
                                                new StrColumn(i)
                                        )
                                );
                                break;
                            case VARCHAR:
                                castFunctions.add(
                                        CastVarcharToGeoHashFunctionFactory.newInstance(
                                                0,
                                                toType,
                                                new VarcharColumn(i)
                                        )
                                );
                                break;
                            case GEOBYTE:
                                castFunctions.add(GeoByteColumn.newInstance(i, fromType));
                                break;
                            case GEOSHORT:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.newInstance(
                                                0,
                                                GeoShortColumn.newInstance(i, fromType),
                                                toType,
                                                fromType
                                        )
                                );
                                break;
                            case GEOINT:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.newInstance(
                                                0,
                                                GeoIntColumn.newInstance(i, fromType),
                                                toType,
                                                fromType
                                        )
                                );
                                break;
                            case GEOLONG:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.newInstance(
                                                0,
                                                GeoLongColumn.newInstance(i, fromType),
                                                toType,
                                                fromType
                                        )
                                );
                                break;
                            default:
                                throw SqlException.unsupportedCast(
                                        modelPosition,
                                        castFromMetadata.getColumnName(i),
                                        fromType,
                                        toType
                                );
                        }
                        break;
                    case GEOSHORT:
                        switch (fromTag) {
                            case STRING:
                                castFunctions.add(
                                        CastStrToGeoHashFunctionFactory.newInstance(
                                                0,
                                                toType,
                                                new StrColumn(i)
                                        )
                                );
                                break;
                            case VARCHAR:
                                castFunctions.add(
                                        CastVarcharToGeoHashFunctionFactory.newInstance(
                                                0,
                                                toType,
                                                new VarcharColumn(i)
                                        )
                                );
                                break;
                            case GEOSHORT:
                                castFunctions.add(GeoShortColumn.newInstance(i, toType));
                                break;
                            case GEOINT:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.newInstance(
                                                0,
                                                GeoIntColumn.newInstance(i, fromType),
                                                toType,
                                                fromType
                                        )
                                );
                                break;
                            case GEOLONG:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.newInstance(
                                                0,
                                                GeoLongColumn.newInstance(i, fromType),
                                                toType,
                                                fromType
                                        )
                                );
                                break;
                            default:
                                throw SqlException.unsupportedCast(
                                        modelPosition,
                                        castFromMetadata.getColumnName(i),
                                        fromType,
                                        toType
                                );
                        }
                        break;
                    case GEOINT:
                        switch (fromTag) {
                            case STRING:
                                castFunctions.add(
                                        CastStrToGeoHashFunctionFactory.newInstance(
                                                0,
                                                toType,
                                                new StrColumn(i)
                                        )
                                );
                                break;
                            case VARCHAR:
                                castFunctions.add(
                                        CastVarcharToGeoHashFunctionFactory.newInstance(
                                                0,
                                                toType,
                                                new VarcharColumn(i)
                                        )
                                );
                                break;
                            case GEOINT:
                                castFunctions.add(GeoIntColumn.newInstance(i, fromType));
                                break;
                            case GEOLONG:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.newInstance(
                                                0,
                                                GeoLongColumn.newInstance(i, fromType),
                                                toType,
                                                fromType
                                        )
                                );
                                break;
                            default:
                                throw SqlException.unsupportedCast(
                                        modelPosition,
                                        castFromMetadata.getColumnName(i),
                                        fromType,
                                        toType
                                );
                        }
                        break;
                    case GEOLONG:
                        switch (fromTag) {
                            case STRING:
                                castFunctions.add(
                                        CastStrToGeoHashFunctionFactory.newInstance(
                                                0,
                                                toType,
                                                new StrColumn(i)
                                        )
                                );
                                break;
                            case VARCHAR:
                                castFunctions.add(
                                        CastVarcharToGeoHashFunctionFactory.newInstance(
                                                0,
                                                toType,
                                                new VarcharColumn(i)
                                        )
                                );
                                break;
                            case GEOLONG:
                                castFunctions.add(GeoLongColumn.newInstance(i, fromType));
                                break;
                            default:
                                throw SqlException.unsupportedCast(
                                        modelPosition,
                                        castFromMetadata.getColumnName(i),
                                        fromType,
                                        toType
                                );
                        }
                        break;
                    case ColumnType.DECIMAL8:
                    case ColumnType.DECIMAL16:
                    case ColumnType.DECIMAL32:
                    case ColumnType.DECIMAL64:
                    case ColumnType.DECIMAL128:
                    case ColumnType.DECIMAL256:
                        if (ColumnType.isDecimalType(fromTag)) {
                            if (fromType == toType) {
                                castFunctions.add(DecimalColumn.newInstance(i, fromType));
                                break;
                            }
                            castFunctions.add(
                                    CastDecimalToDecimalFunctionFactory.newInstance(
                                            0,
                                            new DecimalColumn(i, fromType),
                                            toType,
                                            executionContext
                                    )
                            );
                            break;
                        }
                        switch (fromTag) {
                            case INT:
                                castFunctions.add(
                                        CastIntToDecimalFunctionFactory.newInstance(
                                                0,
                                                IntColumn.newInstance(i),
                                                toType,
                                                executionContext
                                        )
                                );
                                break;
                            case SHORT:
                                castFunctions.add(
                                        CastShortToDecimalFunctionFactory.newInstance(
                                                0,
                                                ShortColumn.newInstance(i),
                                                toType,
                                                executionContext
                                        )
                                );
                                break;
                            case LONG:
                                castFunctions.add(
                                        CastLongToDecimalFunctionFactory.newInstance(
                                                0,
                                                LongColumn.newInstance(i),
                                                toType,
                                                executionContext.getDecimal256()
                                        )
                                );
                                break;
                            case BYTE:
                                castFunctions.add(
                                        CastByteToDecimalFunctionFactory.newInstance(
                                                0,
                                                ByteColumn.newInstance(i),
                                                toType,
                                                executionContext
                                        )
                                );
                                break;
                            case STRING:
                                castFunctions.add(
                                        CastStrToDecimalFunctionFactory.newInstance(
                                                executionContext.getDecimal256(),
                                                0,
                                                toType,
                                                new StrColumn(i)
                                        )
                                );
                                break;
                            case VARCHAR:
                                castFunctions.add(
                                        CastVarcharToDecimalFunctionFactory.newInstance(
                                                executionContext.getDecimal256(),
                                                0,
                                                toType,
                                                new VarcharColumn(i)
                                        )
                                );
                                break;
                            default:
                                throw SqlException.unsupportedCast(
                                        modelPosition,
                                        castFromMetadata.getColumnName(i),
                                        fromType,
                                        toType
                                );
                        }
                    case BINARY:
                        castFunctions.add(BinColumn.newInstance(i));
                        break;
                    case VARCHAR:
                        switch (fromTag) {
                            case BOOLEAN:
                                castFunctions.add(BooleanColumn.newInstance(i));
                                break;
                            case BYTE:
                                castFunctions.add(new CastByteToVarcharFunctionFactory.Func(ByteColumn.newInstance(i)));
                                break;
                            case SHORT:
                                castFunctions.add(new CastShortToVarcharFunctionFactory.Func(ShortColumn.newInstance(i)));
                                break;
                            case CHAR:
                                // CharFunction has built-in cast to varchar
                                castFunctions.add(new CharColumn(i));
                                break;
                            case INT:
                                castFunctions.add(new CastIntToVarcharFunctionFactory.Func(IntColumn.newInstance(i)));
                                break;
                            case LONG:
                                castFunctions.add(new CastLongToVarcharFunctionFactory.Func(LongColumn.newInstance(i)));
                                break;
                            case DATE:
                                castFunctions.add(new CastDateToVarcharFunctionFactory.Func(DateColumn.newInstance(i)));
                                break;
                            case TIMESTAMP:
                                castFunctions.add(new CastTimestampToVarcharFunctionFactory.Func(TimestampColumn.newInstance(i, fromType), fromType));
                                break;
                            case FLOAT:
                                castFunctions.add(new CastFloatToVarcharFunctionFactory.Func(
                                        FloatColumn.newInstance(i)
                                ));
                                break;
                            case DOUBLE:
                                castFunctions.add(new CastDoubleToVarcharFunctionFactory.Func(
                                        DoubleColumn.newInstance(i)
                                ));
                                break;
                            case STRING:
                                // StrFunction has built-in cast to varchar
                                castFunctions.add(new StrColumn(i));
                                break;
                            case VARCHAR:
                                castFunctions.add(new VarcharColumn(i));
                                break;
                            case UUID:
                                castFunctions.add(new CastUuidToVarcharFunctionFactory.Func(UuidColumn.newInstance(i)));
                                break;
                            case IPv4:
                                castFunctions.add(new CastIPv4ToVarcharFunctionFactory.Func(IPv4Column.newInstance(i)));
                                break;
                            case SYMBOL:
                                castFunctions.add(
                                        new CastSymbolToVarcharFunctionFactory.Func(
                                                new SymbolColumn(i, castFromMetadata.isSymbolTableStatic(i))
                                        )
                                );
                                break;
                            case LONG256:
                                castFunctions.add(
                                        new CastLong256ToVarcharFunctionFactory.Func(
                                                Long256Column.newInstance(i)
                                        )
                                );
                                break;
                            case GEOBYTE:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.getGeoByteToVarcharCastFunction(
                                                GeoShortColumn.newInstance(i, toTag),
                                                getGeoHashBits(fromType)
                                        )
                                );
                                break;
                            case GEOSHORT:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.getGeoShortToVarcharCastFunction(
                                                GeoShortColumn.newInstance(i, toTag),
                                                getGeoHashBits(castFromMetadata.getColumnType(i))
                                        )
                                );
                                break;
                            case GEOINT:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.getGeoIntToVarcharCastFunction(
                                                GeoIntColumn.newInstance(i, toTag),
                                                getGeoHashBits(castFromMetadata.getColumnType(i))
                                        )
                                );
                                break;
                            case GEOLONG:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.getGeoLongToVarcharCastFunction(
                                                GeoLongColumn.newInstance(i, toTag),
                                                getGeoHashBits(castFromMetadata.getColumnType(i))
                                        )
                                );
                                break;
                            case BINARY:
                                throw SqlException.unsupportedCast(
                                        modelPosition,
                                        castFromMetadata.getColumnName(i),
                                        fromType,
                                        toType
                                );
                            case ARRAY:
                                int arrayType = decodeArrayElementType(fromType);
                                if (arrayType != DOUBLE) {
                                    throw SqlException.unsupportedCast(
                                            modelPosition,
                                            castFromMetadata.getColumnName(i),
                                            fromType,
                                            toType
                                    );
                                }
                                castFunctions.add(new CastDoubleArrayToVarcharFunctionFactory.Func(ArrayColumn.newInstance(i, fromType)));
                                break;
                            default:
                                assert false;
                        }
                        break;
                    case INTERVAL:
                        castFunctions.add(IntervalColumn.newInstance(i, toType));
                        break;
                    case ARRAY:
                        switch (fromTag) {
                            case ARRAY:
                                assert decodeArrayElementType(fromType) == DOUBLE;
                                assert decodeArrayElementType(toType) == DOUBLE;
                                final int fromDims = decodeWeakArrayDimensionality(fromType);
                                final int toDims = decodeWeakArrayDimensionality(toType);
                                if (toDims == -1) {
                                    throw SqlException.$(modelPosition, "cast to array bind variable type is not supported [column=")
                                            .put(castFromMetadata.getColumnName(i)).put(']');
                                }
                                if (fromDims == toDims) {
                                    castFunctions.add(ArrayColumn.newInstance(i, fromType));
                                } else {
                                    if (fromDims > toDims) {
                                        throw SqlException.$(modelPosition, "array cast to lower dimensionality is not supported [column=")
                                                .put(castFromMetadata.getColumnName(i)).put(']');
                                    }
                                    if (fromDims == -1) {
                                        // must be a bind variable, i.e. weak dimensionality case
                                        castFunctions.add(new CastDoubleArrayToDoubleArrayFunctionFactory.WeakDimsFunc(ArrayColumn.newInstance(i, fromType), toType, modelPosition));
                                    } else {
                                        castFunctions.add(new CastDoubleArrayToDoubleArrayFunctionFactory.Func(ArrayColumn.newInstance(i, fromType), toType, toDims - fromDims));
                                    }
                                }
                                break;
                            case DOUBLE:
                                assert decodeArrayElementType(toType) == DOUBLE;
                                final int dims = decodeWeakArrayDimensionality(toType);
                                if (dims == -1) {
                                    throw SqlException
                                            .$(modelPosition, "cast to array bind variable type is not supported [column=").put(castFromMetadata.getColumnName(i))
                                            .put(']');
                                }
                                castFunctions.add(new CastDoubleToDoubleArray.Func(DoubleColumn.newInstance(i), toType));
                                break;
                            default:
                                assert false;
                        }
                }
            }
        }
        return castFunctions;
    }

    private RecordCursorFactory generateFill(QueryModel model, RecordCursorFactory groupByFactory, SqlExecutionContext executionContext) throws SqlException {
        // locate fill
        QueryModel curr = model;

        while (curr != null && curr.getFillStride() == null) {
            curr = curr.getNestedModel();
        }

        if (curr == null || curr.getFillStride() == null) {
            return groupByFactory;
        }

        ObjList<Function> fillValues = null;
        final ExpressionNode fillFrom = curr.getFillFrom();
        final ExpressionNode fillTo = curr.getFillTo();
        final ExpressionNode fillStride = curr.getFillStride();
        ObjList<ExpressionNode> fillValuesExprs = curr.getFillValues();
        Function fillFromFunc = null;
        Function fillToFunc = null;

        try {
            if (fillValuesExprs == null) {
                throw SqlException.$(-1, "fill values were null");
            }

            fillValues = new ObjList<>(fillValuesExprs.size());

            ExpressionNode expr;
            for (int i = 0, n = fillValuesExprs.size(); i < n; i++) {
                expr = fillValuesExprs.getQuick(0);
                if (isNoneKeyword(expr.token)) {
                    Misc.freeObjList(fillValues);
                    return groupByFactory;
                }
                final Function fillValueFunc = functionParser.parseFunction(expr, EmptyRecordMetadata.INSTANCE, executionContext);
                fillValues.add(fillValueFunc);
            }

            if (fillValues.size() == 0 || (fillValues.size() == 1 && isNoneKeyword(fillValues.getQuick(0).getName()))) {
                Misc.freeObjList(fillValues);
                return groupByFactory;
            }


            QueryModel temp = model;
            ExpressionNode timestamp = model.getTimestamp();

            while (timestamp == null && temp != null) {
                temp = temp.getNestedModel();

                if (temp != null) {
                    timestamp = temp.getTimestamp();
                }
            }

            assert timestamp != null;

            // look for timestamp_floor to check for an alias
            CharSequence alias = timestamp.token;
            final CharSequence currTimestamp = curr.getTimestamp().token;
            for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
                final QueryColumn col = model.getBottomUpColumns().getQuick(i);
                final ExpressionNode ast = col.getAst();
                if (ast.type == FUNCTION && Chars.equalsIgnoreCase(TimestampFloorFunctionFactory.NAME, ast.token)) {
                    final CharSequence ts;
                    // there are three timestamp_floor() overloads, so check all of them
                    if (ast.paramCount == 3 || ast.paramCount == 5) {
                        final int idx = ast.paramCount - 2;
                        ts = ast.args.getQuick(idx).token;
                    } else {
                        ts = ast.rhs.token;
                    }
                    if (Chars.equalsIgnoreCase(ts, currTimestamp)) {
                        alias = col.getAlias();
                    }
                }
            }

            int timestampIndex = groupByFactory.getMetadata().getColumnIndexQuiet(alias);
            int timestampType = groupByFactory.getMetadata().getColumnType(timestampIndex);
            TimestampDriver driver = getTimestampDriver(timestampType);
            fillFromFunc = driver.getTimestampConstantNull();
            fillToFunc = driver.getTimestampConstantNull();
            if (fillFrom != null) {
                fillFromFunc = functionParser.parseFunction(fillFrom, EmptyRecordMetadata.INSTANCE, executionContext);
                coerceRuntimeConstantType(fillFromFunc, timestampType, executionContext, "from lower bound must be a constant expression convertible to a TIMESTAMP", fillFrom.position);
            }

            if (fillTo != null) {
                fillToFunc = functionParser.parseFunction(fillTo, EmptyRecordMetadata.INSTANCE, executionContext);
                coerceRuntimeConstantType(fillToFunc, timestampType, executionContext, "to upper bound must be a constant expression convertible to a TIMESTAMP", fillTo.position);
            }

            int samplingIntervalEnd = TimestampSamplerFactory.findIntervalEndIndex(fillStride.token, fillStride.position, "sample");
            long samplingInterval = TimestampSamplerFactory.parseInterval(fillStride.token, samplingIntervalEnd, fillStride.position, "sample", Numbers.INT_NULL, ' ');
            assert samplingInterval > 0;
            assert samplingIntervalEnd < fillStride.token.length();
            char samplingIntervalUnit = fillStride.token.charAt(samplingIntervalEnd);
            TimestampSampler timestampSampler = TimestampSamplerFactory.getInstance(driver, samplingInterval, samplingIntervalUnit, fillStride.position);

            return new FillRangeRecordCursorFactory(
                    groupByFactory.getMetadata(),
                    groupByFactory,
                    fillFromFunc,
                    fillToFunc,
                    samplingInterval,
                    samplingIntervalUnit,
                    timestampSampler,
                    fillValues,
                    timestampIndex,
                    timestampType

            );
        } catch (Throwable e) {
            Misc.freeObjList(fillValues);
            Misc.free(fillFromFunc);
            Misc.free(fillToFunc);
            Misc.free(groupByFactory);
            throw e;
        }
    }

    private RecordCursorFactory generateFilter(RecordCursorFactory factory, QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        return model.getWhereClause() == null ? factory : generateFilter0(factory, model, executionContext);
    }

    @NotNull
    private RecordCursorFactory generateFilter0(
            RecordCursorFactory factory,
            QueryModel model,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final ExpressionNode filterExpr = model.getWhereClause();

        // back up in case if the above factory steals the filter
        model.setBackupWhereClause(deepClone(expressionNodePool, filterExpr));
        // back up in case filters need to be compiled again
        backupWhereClause(filterExpr);
        model.setWhereClause(null);

        final Function filter;
        try {
            filter = compileBooleanFilter(filterExpr, factory.getMetadata(), executionContext);
        } catch (Throwable e) {
            Misc.free(factory);
            throw e;
        }

        if (filter.isConstant()) {
            try {
                if (filter.getBool(null)) {
                    return factory;
                }
                RecordMetadata metadata = factory.getMetadata();
                assert (metadata instanceof GenericRecordMetadata);
                Misc.free(factory);
                return new EmptyTableRecordCursorFactory(metadata);
            } finally {
                filter.close();
            }
        }

        final boolean enableParallelFilter = executionContext.isParallelFilterEnabled();
        final boolean enablePreTouch = SqlHints.hasEnablePreTouchHint(model, model.getName());
        if (enableParallelFilter && factory.supportsPageFrameCursor()) {
            IntHashSet filterUsedColumnIndexes = new IntHashSet();
            collectColumnIndexes(sqlNodeStack, factory.getMetadata(), filterExpr, filterUsedColumnIndexes);

            final boolean useJit = executionContext.getJitMode() != SqlJitMode.JIT_MODE_DISABLED
                    && (!model.isUpdate() || executionContext.isWalApplication());
            final boolean canCompile = factory.supportsPageFrameCursor() && JitUtil.isJitSupported();
            if (useJit && canCompile) {
                CompiledFilter compiledFilter = null;
                CompiledCountOnlyFilter compiledCountOnlyFilter = null;
                try {
                    int jitOptions;
                    final ObjList<Function> bindVarFunctions = new ObjList<>();
                    try (PageFrameCursor cursor = factory.getPageFrameCursor(executionContext, ORDER_ANY)) {
                        final boolean forceScalar = executionContext.getJitMode() == SqlJitMode.JIT_MODE_FORCE_SCALAR;
                        jitIRSerializer.of(jitIRMem, executionContext, factory.getMetadata(), cursor, bindVarFunctions);
                        jitOptions = jitIRSerializer.serialize(filterExpr, forceScalar, enableJitDebug, enableJitNullChecks);
                    }

                    compiledFilter = new CompiledFilter();
                    compiledFilter.compile(jitIRMem, jitOptions);

                    compiledCountOnlyFilter = new CompiledCountOnlyFilter();
                    compiledCountOnlyFilter.compile(jitIRMem, jitOptions);

                    final Function limitLoFunction = getLimitLoFunctionOnly(model, executionContext);
                    final int limitLoPos = model.getLimitAdviceLo() != null ? model.getLimitAdviceLo().position : 0;

                    LOG.debug()
                            .$("JIT enabled for (sub)query [tableName=").$safe(model.getName())
                            .$(", fd=").$(executionContext.getRequestFd())
                            .I$();
                    return new AsyncJitFilteredRecordCursorFactory(
                            executionContext.getCairoEngine(),
                            configuration,
                            executionContext.getMessageBus(),
                            factory,
                            bindVarFunctions,
                            compiledFilter,
                            compiledCountOnlyFilter,
                            filter,
                            filterUsedColumnIndexes,
                            reduceTaskFactory,
                            compileWorkerFilterConditionally(
                                    executionContext,
                                    filter,
                                    executionContext.getSharedQueryWorkerCount(),
                                    filterExpr,
                                    factory.getMetadata()
                            ),
                            deepClone(expressionNodePool, filterExpr),
                            limitLoFunction,
                            limitLoPos,
                            executionContext.getSharedQueryWorkerCount(),
                            enablePreTouch
                    );
                } catch (SqlException | LimitOverflowException ex) {
                    // for these errors we are intentionally **not** rethrowing the exception
                    // if a JIT filter cannot be used, we will simply use a Java filter
                    Misc.free(compiledFilter);
                    Misc.free(compiledCountOnlyFilter);
                    LOG.debug()
                            .$("JIT cannot be applied to (sub)query [tableName=").$safe(model.getName())
                            .$(", ex=").$safe(ex.getFlyweightMessage())
                            .$(", fd=").$(executionContext.getRequestFd()).$(']').$();
                } catch (Throwable t) {
                    // other errors are fatal -> rethrow them
                    Misc.free(compiledFilter);
                    Misc.free(compiledCountOnlyFilter);
                    Misc.free(filter);
                    Misc.free(factory);
                    throw t;
                } finally {
                    jitIRSerializer.clear();
                    jitIRMem.truncate();
                }
            }

            // Use Java filter.
            try {
                final Function limitLoFunction = getLimitLoFunctionOnly(model, executionContext);
                final int limitLoPos = model.getLimitAdviceLo() != null ? model.getLimitAdviceLo().position : 0;
                return new AsyncFilteredRecordCursorFactory(
                        executionContext.getCairoEngine(),
                        configuration,
                        executionContext.getMessageBus(),
                        factory,
                        filter,
                        filterUsedColumnIndexes,
                        reduceTaskFactory,
                        compileWorkerFilterConditionally(
                                executionContext,
                                filter,
                                executionContext.getSharedQueryWorkerCount(),
                                filterExpr,
                                factory.getMetadata()
                        ),
                        deepClone(expressionNodePool, filterExpr),
                        limitLoFunction,
                        limitLoPos,
                        executionContext.getSharedQueryWorkerCount(),
                        enablePreTouch
                );
            } catch (Throwable e) {
                Misc.free(filter);
                Misc.free(factory);
                throw e;
            }
        }
        return new FilteredRecordCursorFactory(factory, filter);
    }

    private RecordCursorFactory generateFunctionQuery(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        RecordCursorFactory tableFactory = model.getTableNameFunction();
        if (tableFactory != null) {
            // We're transferring ownership of the tableFactory's factory to another factory
            // setting tableFactory to NULL will prevent double-ownership.
            // We should not release tableFactory itself, they typically just a lightweight factory wrapper.
            model.setTableNameFunction(null);
        } else {
            // when tableFactory is null we have to recompile it from scratch, including creating new factory
            tableFactory = TableUtils.createCursorFunction(functionParser, model, executionContext).getRecordCursorFactory();
        }

        if (tableFactory instanceof ProjectableRecordCursorFactory factory) {
            RecordMetadata metadata = factory.getMetadata();
            int readerTimestampIndex = getTimestampIndex(model, metadata);
            boolean requiresTimestamp = joinsRequiringTimestamp[model.getJoinType()];
            factory.setQueryProjectedMetadata(buildQueryMetadata(
                    model,
                    executionContext,
                    tableFactory.getMetadata(),
                    readerTimestampIndex,
                    requiresTimestamp,
                    null,
                    null
            ));
        }

        return tableFactory;
    }

    private RecordCursorFactory generateIntersectOrExceptAllFactory(
            QueryModel model,
            SqlExecutionContext executionContext,
            RecordCursorFactory factoryA,
            RecordCursorFactory factoryB,
            ObjList<Function> castFunctionsA,
            ObjList<Function> castFunctionsB,
            RecordMetadata unionMetadata,
            SetRecordCursorFactoryConstructor constructor
    ) throws SqlException {
        writeSymbolAsString.clear();
        valueTypes.clear();
        // Remap symbol columns to string type since that's how recordSink copies them.
        keyTypes.clear();
        for (int i = 0, n = unionMetadata.getColumnCount(); i < n; i++) {
            final int columnType = unionMetadata.getColumnType(i);
            if (isSymbol(columnType)) {
                keyTypes.add(STRING);
                writeSymbolAsString.set(i);
            } else {
                keyTypes.add(columnType);
            }
        }

        entityColumnFilter.of(factoryA.getMetadata().getColumnCount());
        final RecordSink recordSink = RecordSinkFactory.getInstance(
                asm,
                unionMetadata,
                entityColumnFilter,
                writeSymbolAsString,
                configuration
        );

        RecordCursorFactory unionAllFactory = constructor.create(
                configuration,
                unionMetadata,
                factoryA,
                factoryB,
                castFunctionsA,
                castFunctionsB,
                recordSink,
                keyTypes,
                valueTypes
        );

        if (model.getUnionModel().getUnionModel() != null) {
            return generateSetFactory(model.getUnionModel(), unionAllFactory, executionContext);
        }
        return unionAllFactory;
    }

    private RecordCursorFactory generateJoinAsof(
            final boolean isSelfJoin,
            final QueryModel model,
            final QueryModel slaveModel,
            final RecordCursorFactory master,
            final RecordMetadata masterMetadata,
            final CharSequence masterAlias,
            final RecordCursorFactory slave,
            final RecordMetadata slaveMetadata
    ) throws SqlException {
        long toleranceInterval = tolerance(slaveModel, masterMetadata.getTimestampType(), slaveMetadata.getTimestampType());
        CharSequence slaveAlias = slaveModel.getName();

        if (fullFatJoins || !slave.recordCursorSupportsRandomAccess()) {
            return createFullFatJoin(
                    master,
                    masterMetadata,
                    masterAlias,
                    slave,
                    slaveMetadata,
                    slaveAlias,
                    slaveModel.getJoinKeywordPosition(),
                    CREATE_FULL_FAT_AS_OF_JOIN,
                    slaveModel.getJoinContext(),
                    toleranceInterval
            );
        }

        final JoinRecordMetadata joinMetadata = createJoinMetadata(masterAlias, masterMetadata, slaveAlias, slaveMetadata);
        try {
            boolean hasLinearHint = SqlHints.hasAsOfLinearHint(model, masterAlias, slaveAlias);
            if (isKeyedTemporalJoin(masterMetadata, slaveMetadata)) {
                SymbolShortCircuit symbolShortCircuit = createSymbolShortCircuit(masterMetadata, slaveMetadata, isSelfJoin);
                int joinColumnSplit = masterMetadata.getColumnCount();
                JoinContext slaveContext = slaveModel.getJoinContext();
                if (!hasLinearHint) {
                    if (slave.supportsTimeFrameCursor()) {
                        boolean isSingleSymbolJoin = isSingleSymbolJoin(symbolShortCircuit, listColumnFilterA);
                        boolean hasDenseHint = SqlHints.hasAsOfDenseHint(model, masterAlias, slaveModel.getName());
                        if (hasDenseHint) {
                            if (isSingleSymbolJoin) {
                                int slaveSymbolColumnIndex = listColumnFilterA.getColumnIndexFactored(0);
                                return new AsOfJoinDenseSingleSymbolRecordCursorFactory(
                                        configuration,
                                        joinMetadata,
                                        master,
                                        slave,
                                        joinColumnSplit,
                                        slaveSymbolColumnIndex,
                                        (SymbolJoinKeyMapping) symbolShortCircuit,
                                        slaveContext,
                                        toleranceInterval
                                );
                            }
                            return new AsOfJoinDenseRecordCursorFactory(
                                    configuration,
                                    joinMetadata,
                                    master,
                                    createRecordCopierMaster(masterMetadata),
                                    slave,
                                    createRecordCopierSlave(slaveMetadata),
                                    joinColumnSplit,
                                    keyTypes,
                                    slaveContext,
                                    toleranceInterval
                            );
                        }
                        RecordSink recordCopierMaster;
                        if (isSingleSymbolJoin) {
                            SymbolJoinKeyMapping symbolJoinKeyMapping = (SymbolJoinKeyMapping) symbolShortCircuit;
                            int slaveSymbolColumnIndex = listColumnFilterA.getColumnIndexFactored(0);
                            boolean hasIndexHint = SqlHints.hasAsOfIndexHint(model, masterAlias, slaveAlias);
                            if (hasIndexHint && slaveMetadata.isColumnIndexed(slaveSymbolColumnIndex)) {
                                return new AsOfJoinIndexedRecordCursorFactory(
                                        configuration,
                                        joinMetadata,
                                        master,
                                        slave,
                                        joinColumnSplit,
                                        slaveSymbolColumnIndex,
                                        symbolJoinKeyMapping,
                                        slaveContext,
                                        toleranceInterval
                                );
                            }
                            boolean hasMemoizedHint = SqlHints.hasAsOfMemoizedHint(model, masterAlias, slaveAlias);
                            boolean hasMemoizedDrivebyHint = SqlHints.hasAsOfMemoizedDrivebyHint(model, masterAlias, slaveAlias);
                            if (hasMemoizedHint || hasMemoizedDrivebyHint) {
                                return new AsOfJoinMemoizedRecordCursorFactory(
                                        configuration,
                                        joinMetadata,
                                        master,
                                        slave,
                                        joinColumnSplit,
                                        slaveSymbolColumnIndex,
                                        symbolJoinKeyMapping,
                                        slaveContext,
                                        toleranceInterval,
                                        hasMemoizedDrivebyHint
                                );
                            }

                            // We're falling back to the default Fast scan. We can still optimize one thing:
                            // join key equality check. Instead of comparing symbols as strings, compare symbol keys.
                            // For that to work, we need code that maps master symbol key to slave symbol key.
                            writeSymbolAsString.unset(slaveSymbolColumnIndex);
                            recordCopierMaster = new SymbolKeyMappingRecordCopier((SymbolJoinKeyMapping) symbolShortCircuit);
                        } else {
                            recordCopierMaster = createRecordCopierMaster(masterMetadata);
                        }
                        return new AsOfJoinFastRecordCursorFactory(
                                configuration,
                                joinMetadata,
                                master,
                                recordCopierMaster,
                                slave,
                                createRecordCopierSlave(slaveMetadata),
                                joinColumnSplit,
                                symbolShortCircuit,
                                slaveContext,
                                toleranceInterval
                        );
                    } else if (slave.supportsFilterStealing() && slave.getBaseFactory().supportsTimeFrameCursor()) {
                        RecordCursorFactory slaveBase = slave.getBaseFactory();
                        int slaveTimestampIndex = validateAndGetSlaveTimestampIndex(slaveMetadata, slaveBase);

                        Function stolenFilter = slave.getFilter();
                        assert stolenFilter != null;

                        Misc.free(slave.getCompiledFilter());
                        Misc.free(slave.getBindVarMemory());
                        Misc.freeObjList(slave.getBindVarFunctions());
                        slave.halfClose();

                        return new FilteredAsOfJoinFastRecordCursorFactory(
                                configuration,
                                joinMetadata,
                                master,
                                createRecordCopierMaster(masterMetadata),
                                slaveBase,
                                createRecordCopierSlave(slaveMetadata),
                                stolenFilter,
                                masterMetadata.getColumnCount(),
                                NullRecordFactory.getInstance(slaveMetadata),
                                null,
                                slaveTimestampIndex,
                                toleranceInterval
                        );
                    } else if (slave.isProjection()) {
                        RecordCursorFactory projectionBase = slave.getBaseFactory();
                        // We know projectionBase does not support supportsTimeFrameCursor, because
                        // Projections forward this call to its base factory and if we are in this branch
                        // then slave.supportsTimeFrameCursor() returned false in one the previous branches.
                        // There is still chance that projectionBase is just a filter
                        // and its own base supports timeFrameCursor. let's see.
                        if (projectionBase.supportsFilterStealing()) {
                            // ok cool, it's used only as a filter.
                            RecordCursorFactory filterStealingBase = projectionBase.getBaseFactory();
                            if (filterStealingBase.supportsTimeFrameCursor()) {
                                IntList stolenCrossIndex = slave.getColumnCrossIndex();
                                assert stolenCrossIndex != null;
                                Function stolenFilter = projectionBase.getFilter();
                                assert stolenFilter != null;

                                // index *after* applying the projection
                                int slaveTimestampIndex = slaveMetadata.getTimestampIndex();
                                assert stolenCrossIndex.get(slaveTimestampIndex) == filterStealingBase.getMetadata().getTimestampIndex();

                                Misc.free(projectionBase.getCompiledFilter());
                                Misc.free(projectionBase.getBindVarMemory());
                                Misc.freeObjList(projectionBase.getBindVarFunctions());
                                projectionBase.halfClose();

                                return new FilteredAsOfJoinFastRecordCursorFactory(
                                        configuration,
                                        joinMetadata,
                                        master,
                                        createRecordCopierMaster(masterMetadata),
                                        filterStealingBase,
                                        createRecordCopierSlave(slaveMetadata),
                                        stolenFilter,
                                        masterMetadata.getColumnCount(),
                                        NullRecordFactory.getInstance(slaveMetadata),
                                        stolenCrossIndex,
                                        slaveTimestampIndex,
                                        toleranceInterval
                                );
                            }
                        }
                    }
                }

                // fallback for keyed join when no optimizations are applicable, or when asof_linear hint is used:
                if (isSingleSymbolJoin(symbolShortCircuit, listColumnFilterA)) {
                    // We're falling back to the default Light scan. We can still optimize one thing:
                    // join key equality check. Instead of comparing symbols as strings, compare symbol keys.
                    // For that to work, we need code that maps master symbol key to slave symbol key.
                    int slaveSymbolColumnIndex = listColumnFilterA.getColumnIndexFactored(0);
                    writeSymbolAsString.unset(slaveSymbolColumnIndex);
                    SymbolJoinKeyMapping joinKeyMapping = (SymbolJoinKeyMapping) symbolShortCircuit;
                    keyTypes.clear();
                    keyTypes.add(ColumnType.INT);
                    return new AsOfJoinLightRecordCursorFactory(
                            configuration,
                            joinMetadata,
                            master,
                            slave,
                            keyTypes,
                            new SymbolKeyMappingRecordCopier(joinKeyMapping),
                            createRecordCopierSlave(slaveMetadata),
                            joinKeyMapping,
                            joinColumnSplit,
                            slaveContext,
                            toleranceInterval
                    );
                } else {
                    return new AsOfJoinLightRecordCursorFactory(
                            configuration,
                            joinMetadata,
                            master,
                            slave,
                            keyTypes,
                            createRecordCopierMaster(masterMetadata),
                            createRecordCopierSlave(slaveMetadata),
                            null,
                            joinColumnSplit,
                            slaveContext,
                            toleranceInterval
                    );
                }
            }

            // reaching this point means the join is non-keyed
            if (!hasLinearHint) {
                if (slave.supportsTimeFrameCursor()) {
                    return new AsOfJoinNoKeyFastRecordCursorFactory(
                            configuration,
                            joinMetadata,
                            master,
                            slave,
                            masterMetadata.getColumnCount(),
                            toleranceInterval
                    );
                }
                if (slave.supportsFilterStealing() && slave.getBaseFactory().supportsTimeFrameCursor()) {
                    // Try to steal the filter from the slave. This downgrades to
                    // single-threaded Java-level filtering, so it's only worth it if the filter
                    // selectivity is low. We don't have statistics to tell selectivity, so
                    // we allow the user to disable this with the asof_linear_search hint.
                    RecordCursorFactory slaveBase = slave.getBaseFactory();
                    int slaveTimestampIndex = validateAndGetSlaveTimestampIndex(slaveMetadata, slaveBase);

                    Function stolenFilter = slave.getFilter();
                    assert stolenFilter != null;

                    Misc.free(slave.getCompiledFilter());
                    Misc.free(slave.getBindVarMemory());
                    Misc.freeObjList(slave.getBindVarFunctions());
                    slave.halfClose();
                    return new FilteredAsOfJoinNoKeyFastRecordCursorFactory(
                            configuration,
                            joinMetadata,
                            master,
                            slaveBase,
                            stolenFilter,
                            masterMetadata.getColumnCount(),
                            NullRecordFactory.getInstance(slaveMetadata),
                            null,
                            slaveTimestampIndex,
                            toleranceInterval
                    );
                }
                if (slave.isProjection()) {
                    RecordCursorFactory projectionBase = slave.getBaseFactory();
                    // We know projectionBase does not support supportsTimeFrameCursor, because
                    // projections forward this call to its base factory, and if we are in this branch,
                    // slave.supportsTimeFrameCursor() returned false in a previous branch.
                    // There is still chance that projectionBase is just a filter
                    // and its own base supports timeFrameCursor. Let's see.
                    if (projectionBase.supportsFilterStealing()) {
                        // ok, cool, it's used only as a filter
                        RecordCursorFactory filterStealingBase = projectionBase.getBaseFactory();
                        if (filterStealingBase.supportsTimeFrameCursor()) {
                            IntList stolenCrossIndex = slave.getColumnCrossIndex();
                            assert stolenCrossIndex != null;
                            Function stolenFilter = projectionBase.getFilter();
                            assert stolenFilter != null;

                            // index *after* applying the projection
                            int slaveTimestampIndex = slaveMetadata.getTimestampIndex();
                            assert stolenCrossIndex.get(slaveTimestampIndex) == filterStealingBase.getMetadata().getTimestampIndex();

                            Misc.free(projectionBase.getCompiledFilter());
                            Misc.free(projectionBase.getBindVarMemory());
                            Misc.freeObjList(projectionBase.getBindVarFunctions());
                            projectionBase.halfClose();

                            return new FilteredAsOfJoinNoKeyFastRecordCursorFactory(
                                    configuration,
                                    joinMetadata,
                                    master,
                                    filterStealingBase,
                                    stolenFilter,
                                    masterMetadata.getColumnCount(),
                                    NullRecordFactory.getInstance(slaveMetadata),
                                    stolenCrossIndex,
                                    slaveTimestampIndex,
                                    toleranceInterval
                            );
                        }
                    }
                }
            }
            // fallback for non-keyed join when no optimizations are applicable, or the asof_linear hint is used:
            return new AsOfJoinLightNoKeyRecordCursorFactory(
                    joinMetadata,
                    master,
                    slave,
                    masterMetadata.getColumnCount(),
                    toleranceInterval
            );
        } catch (Throwable t) {
            Misc.free(joinMetadata);
            throw t;
        }
    }

    private @NotNull RecordCursorFactory generateJoinLt(
            QueryModel model,
            QueryModel slaveModel,
            RecordCursorFactory master,
            RecordMetadata masterMetadata,
            CharSequence masterAlias,
            RecordCursorFactory slave,
            RecordMetadata slaveMetadata
    ) throws SqlException {
        long toleranceInterval = tolerance(slaveModel, masterMetadata.getTimestampType(), slaveMetadata.getTimestampType());
        CharSequence slaveAlias = slaveModel.getName();

        if (!slave.recordCursorSupportsRandomAccess() || fullFatJoins) {
            return createFullFatJoin(
                    master,
                    masterMetadata,
                    masterAlias,
                    slave,
                    slaveMetadata,
                    slaveAlias,
                    slaveModel.getJoinKeywordPosition(),
                    CREATE_FULL_FAT_LT_JOIN,
                    slaveModel.getJoinContext(),
                    toleranceInterval
            );
        }

        JoinRecordMetadata joinMetadata = createJoinMetadata(masterAlias, masterMetadata, slaveAlias, slaveMetadata);
        try {
            if (isKeyedTemporalJoin(masterMetadata, slaveMetadata)) {
                RecordSink masterKeyCopier = createRecordCopierMaster(masterMetadata);
                RecordSink slaveKeyCopier = createRecordCopierSlave(slaveMetadata);
                int columnSplit = masterMetadata.getColumnCount();
                JoinContext joinContext = slaveModel.getJoinContext();
                valueTypes.clear();
                valueTypes.add(LONG);
                return new LtJoinLightRecordCursorFactory(
                        configuration,
                        joinMetadata,
                        master,
                        slave,
                        keyTypes,
                        valueTypes,
                        masterKeyCopier,
                        slaveKeyCopier,
                        columnSplit,
                        joinContext,
                        toleranceInterval
                );
            }

            boolean hasLinearHint = SqlHints.hasAsOfLinearHint(model, masterAlias, slaveAlias);
            if (!hasLinearHint && slave.supportsTimeFrameCursor()) {
                return new LtJoinNoKeyFastRecordCursorFactory(
                        configuration,
                        joinMetadata,
                        master,
                        slave,
                        masterMetadata.getColumnCount(),
                        toleranceInterval
                );
            }

            return new LtJoinNoKeyRecordCursorFactory(
                    joinMetadata,
                    master,
                    slave,
                    masterMetadata.getColumnCount(),
                    toleranceInterval
            );
        } catch (Throwable t) {
            Misc.free(joinMetadata);
            throw t;
        }
    }

    private RecordCursorFactory generateJoins(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        final ObjList<QueryModel> joinModels = model.getJoinModels();
        IntList ordered = model.getOrderedJoinModels();
        JoinRecordMetadata joinMetadata = null;
        RecordCursorFactory master = null;
        CharSequence masterAlias = null;

        try {
            int n = ordered.size();
            assert n > 1;
            for (int i = 0; i < n; i++) {
                int index = ordered.getQuick(i);
                QueryModel slaveModel = joinModels.getQuick(index);

                if (i > 0) {
                    executionContext.pushTimestampRequiredFlag(joinsRequiringTimestamp[slaveModel.getJoinType()]);
                    executionContext.popHasInterval();
                    executionContext.pushHasInterval(1);
                } else { // i == 0
                    // This is first model in the sequence of joins
                    // TS requirement is symmetrical on both right and left sides
                    // check if next join requires a timestamp
                    int nextJointType = joinModels.getQuick(ordered.getQuick(1)).getJoinType();
                    executionContext.pushTimestampRequiredFlag(joinsRequiringTimestamp[nextJointType]);
                    // For successive JOIN operations, if the left table requires timestamp,
                    // it must be the timestamp from the first table in the JOIN chain
                    executionContext.pushHasInterval(0);
                    executionContext.pushIntervalModel(null);
                }

                RecordCursorFactory slave = null;
                Function joinFilter = null;
                ObjList<GroupByFunction> groupByFunctions = null;
                boolean closeSlaveOnFailure = true;
                try {
                    // compile
                    slave = generateQuery(slaveModel, executionContext, index > 0);

                    // check if this is the root of joins
                    if (master == null) {
                        // This is an opportunistic check of order by clause
                        // to determine if we can get away ordering main record source only
                        // Ordering main record source could benefit from rowid access thus
                        // making it faster compared to ordering of join record source that
                        // doesn't allow rowid access.
                        master = slave;
                        closeSlaveOnFailure = false;
                        masterAlias = slaveModel.getName();
                    } else {
                        // not the root, join to "master"
                        final int joinType = slaveModel.getJoinType();
                        final RecordMetadata masterMetadata = master.getMetadata();
                        final RecordMetadata slaveMetadata = slave.getMetadata();

                        switch (joinType) {
                            case JOIN_CROSS_LEFT:
                            case JOIN_CROSS_RIGHT:
                            case JOIN_CROSS_FULL:
                                assert slaveModel.getOuterJoinExpressionClause() != null;
                                joinMetadata = createJoinMetadata(
                                        masterAlias,
                                        masterMetadata,
                                        slaveModel.getName(),
                                        slaveMetadata,
                                        joinType == JOIN_CROSS_LEFT ? masterMetadata.getTimestampIndex() : -1
                                );
                                joinFilter = compileJoinFilter(slaveModel.getOuterJoinExpressionClause(), joinMetadata, executionContext);
                                master = switch (joinType) {
                                    case JOIN_CROSS_LEFT -> new NestedLoopLeftJoinRecordCursorFactory(
                                            joinMetadata,
                                            master,
                                            slave,
                                            masterMetadata.getColumnCount(),
                                            joinFilter,
                                            NullRecordFactory.getInstance(slaveMetadata)
                                    );
                                    case JOIN_CROSS_RIGHT -> new NestedLoopRightJoinRecordCursorFactory(
                                            joinMetadata,
                                            master,
                                            slave,
                                            masterMetadata.getColumnCount(),
                                            joinFilter,
                                            NullRecordFactory.getInstance(masterMetadata)
                                    );
                                    case JOIN_CROSS_FULL -> new NestedLoopFullJoinRecordCursorFactory(
                                            configuration,
                                            joinMetadata,
                                            master,
                                            slave,
                                            masterMetadata.getColumnCount(),
                                            joinFilter,
                                            NullRecordFactory.getInstance(masterMetadata),
                                            NullRecordFactory.getInstance(slaveMetadata)
                                    );
                                    default -> throw new AssertionError("unreachable");
                                };
                                masterAlias = null;
                                break;
                            case JOIN_CROSS:
                                validateOuterJoinExpressions(slaveModel, "CROSS");

                                // Try to detect the markout horizon pattern
                                MarkoutHorizonInfo horizonInfo = detectMarkoutHorizonPattern(
                                        masterAlias,
                                        model,
                                        masterMetadata,
                                        master,
                                        slaveModel,
                                        slaveMetadata,
                                        slave
                                );

                                if (horizonInfo != null) {
                                    // Create RecordSink for materializing slave records
                                    entityColumnFilter.of(slaveMetadata.getColumnCount());
                                    RecordSink slaveRecordSink = RecordSinkFactory.getInstance(asm, slaveMetadata, entityColumnFilter, configuration);

                                    // Use the optimized MarkoutHorizonRecordCursorFactory
                                    master = new MarkoutHorizonRecordCursorFactory(
                                            configuration,
                                            createJoinMetadata(masterAlias, masterMetadata, slaveModel.getName(), slaveMetadata, -1),
                                            master,
                                            slave,
                                            masterMetadata.getColumnCount(),
                                            horizonInfo.masterTimestampColumnIndex,
                                            horizonInfo.slaveSequenceColumnIndex,
                                            slaveRecordSink
                                    );
                                } else {
                                    // Fall back to standard CrossJoinRecordCursorFactory
                                    master = new CrossJoinRecordCursorFactory(
                                            createJoinMetadata(masterAlias, masterMetadata, slaveModel.getName(), slaveMetadata),
                                            master,
                                            slave,
                                            masterMetadata.getColumnCount()
                                    );
                                }
                                masterAlias = null;
                                break;
                            case JOIN_ASOF:
                            case JOIN_LT:
                                validateBothTimestamps(slaveModel, masterMetadata, slaveMetadata);
                                validateOuterJoinExpressions(slaveModel, joinType == JOIN_ASOF ? "ASOF" : "LT");
                                validateBothTimestampOrders(master, slave, slaveModel.getJoinKeywordPosition());
                                // isSelfJoin is imperfect and might generate false negatives when using subqueries:
                                //  - if `true`, the join is a self-join for sure
                                //  - if `false`, the join may or may not be self-join
                                boolean isSelfJoin = isSameTable(master, slave);
                                processJoinContext(index == 1, isSelfJoin, slaveModel.getJoinContext(), masterMetadata, slaveMetadata);
                                validateTimestampNotInJoinKeys(slaveModel, masterMetadata, slaveMetadata);
                                master = joinType == JOIN_ASOF
                                        ? generateJoinAsof(isSelfJoin, model, slaveModel, master, masterMetadata, masterAlias, slave, slaveMetadata)
                                        : generateJoinLt(model, slaveModel, master, masterMetadata, masterAlias, slave, slaveMetadata);
                                masterAlias = null;
                                // from now on, master owns slave, so we don't have to close it
                                closeSlaveOnFailure = false;
                                break;
                            case JOIN_SPLICE:
                                validateBothTimestamps(slaveModel, masterMetadata, slaveMetadata);
                                validateBothTimestampOrders(master, slave, slaveModel.getJoinKeywordPosition());
                                validateOuterJoinExpressions(slaveModel, "SPLICE");
                                processJoinContext(index == 1, isSameTable(master, slave), slaveModel.getJoinContext(), masterMetadata, slaveMetadata);
                                if (slave.recordCursorSupportsRandomAccess() && master.recordCursorSupportsRandomAccess() && !fullFatJoins) {
                                    master = createSpliceJoin(
                                            // splice join result does not have timestamp
                                            createJoinMetadata(masterAlias, masterMetadata, slaveModel.getName(), slaveMetadata, -1),
                                            master,
                                            createRecordCopierMaster(masterMetadata),
                                            slave,
                                            createRecordCopierSlave(slaveMetadata),
                                            masterMetadata.getColumnCount(),
                                            slaveModel.getJoinContext()
                                    );
                                    // from now on, master owns slave, so we don't have to close it
                                    closeSlaveOnFailure = false;
                                } else {
                                    if (!master.recordCursorSupportsRandomAccess()) {
                                        throw SqlException.position(slaveModel.getJoinKeywordPosition()).put("left side of splice join doesn't support random access");
                                    } else if (!slave.recordCursorSupportsRandomAccess()) {
                                        throw SqlException.position(slaveModel.getJoinKeywordPosition()).put("right side of splice join doesn't support random access");
                                    } else {
                                        throw SqlException.position(slaveModel.getJoinKeywordPosition()).put("splice join doesn't support full fat mode");
                                    }
                                }
                                break;
                            case JOIN_WINDOW:
                                validateBothTimestamps(slaveModel, masterMetadata, slaveMetadata);
                                validateBothTimestampOrders(master, slave, slaveModel.getJoinKeywordPosition());
                                final WindowJoinContext context = slaveModel.getWindowJoinContext();
                                final TimestampDriver timestampDriver = getTimestampDriver(masterMetadata.getTimestampType());
                                long hi = context.getHi();
                                if (context.getHiExprTimeUnit() != 0) {
                                    hi = timestampDriver.from(hi, context.getHiExprTimeUnit());
                                }
                                long lo = context.getLo();
                                if (context.getLoExprTimeUnit() != 0) {
                                    lo = timestampDriver.from(lo, context.getLoExprTimeUnit());
                                }

                                if (hi < lo * -1) {
                                    throw SqlException.position(Math.max(context.getHiExprPos(), context.getLoExprPos())).put("WINDOW join hi value cannot be less than lo value");
                                }

                                // For multiple consecutive window joins:
                                // - Intermediate window joins return metadata combining master metadata + aggregated columns from current slave model
                                // - The last window join returns metadata mapped according to the final projection
                                final boolean isLastWindowJoin = i + 1 == n;
                                processJoinContext(index == 1, isSameTable(master, slave), slaveModel.getJoinContext(), masterMetadata, slaveMetadata);
                                joinMetadata = createJoinMetadata(masterAlias, masterMetadata, slaveModel.getName(), slaveMetadata, masterMetadata.getTimestampIndex());
                                masterAlias = null;
                                ObjList<QueryColumn> aggregateCols = new ObjList<>();
                                final int splitIndex = masterMetadata.getColumnCount();
                                int timestampIndex = -1;
                                IntList columnIndex = null;
                                final QueryModel aggModel = context.getParentModel();
                                final ObjList<QueryColumn> columns = aggModel.getColumns();

                                if (!isLastWindowJoin) {
                                    // intermediate window join - keep only aggregate columns
                                    for (int j = 0, m = columns.size(); j < m; j++) {
                                        ExpressionNode ast = columns.get(j).getAst();
                                        if (!functionParser.getFunctionFactoryCache().isGroupBy(ast.token)) {
                                            int colIndex = joinMetadata.getColumnIndexQuiet(ast.token);
                                            if (colIndex >= splitIndex) {
                                                throw SqlException.position(ast.position).put("WINDOW join cannot reference right table non-aggregate column: ").put(ast.token);
                                            }
                                        } else {
                                            sqlNodeStack.clear();
                                            windowJoinColCheckVisitor.of(joinMetadata);
                                            traversalAlgo.traverse(ast, windowJoinColCheckVisitor);
                                            if (windowJoinColCheckVisitor.shouldInclude) {
                                                QueryColumn column = columns.get(j);
                                                aggregateCols.add(columns.get(j));
                                                aggModel.replaceColumn(j, SqlUtil.nextColumn(queryColumnPool, expressionNodePool, column.getAlias(), column.getAlias(), ast.position));
                                            } else if (windowJoinColCheckVisitor.hasIncludeCol) {
                                                throw SqlException.position(ast.position).put("WINDOW join aggregate function cannot reference columns from multiple models");
                                            }
                                        }
                                    }
                                } else {
                                    columnIndex = new IntList(columns.size());
                                    int groupByCnt = 0;

                                    // validate columns in aggregate model
                                    for (int j = 0, m = columns.size(); j < m; j++) {
                                        final ExpressionNode ast = columns.get(j).getAst();
                                        if (!functionParser.getFunctionFactoryCache().isGroupBy(ast.token)) {
                                            int colIndex = joinMetadata.getColumnIndexQuiet(ast.token);
                                            if (colIndex == -1) {
                                                throw SqlException.invalidColumn(ast.position, ast.token);
                                            }
                                            if (colIndex >= splitIndex) {
                                                throw SqlException.position(ast.position).put("WINDOW join cannot reference right table non-aggregate column: ").put(ast.token);
                                            }
                                            if (colIndex == masterMetadata.getTimestampIndex()) {
                                                timestampIndex = colIndex;
                                            }
                                            columnIndex.add(colIndex);
                                        } else {
                                            columnIndex.add(splitIndex + groupByCnt);
                                            groupByCnt++;
                                            aggregateCols.add(columns.get(j));
                                        }
                                    }

                                    if (columnIndex.size() == splitIndex + groupByCnt) {
                                        boolean skipColumnIndex = true;
                                        for (int j = 0, m = columnIndex.size(); j < m; j++) {
                                            if (j != columnIndex.getQuick(j)) {
                                                skipColumnIndex = false;
                                                break;
                                            }
                                        }
                                        if (skipColumnIndex) {
                                            columnIndex = null;
                                        }
                                    }
                                }

                                // assemble groupBy function
                                keyTypes.clear();
                                valueTypes.clear();
                                listColumnFilterA.clear();
                                final int columnCount = isLastWindowJoin ? columns.size() : aggregateCols.size();
                                groupByFunctions = new ObjList<>(columnCount);
                                tempInnerProjectionFunctions.clear();
                                tempOuterProjectionFunctions.clear();
                                GenericRecordMetadata outerProjectionMetadata = new GenericRecordMetadata();
                                final IntList projectionFunctionFlags = new IntList(columnCount);
                                GroupByUtils.assembleGroupByFunctions(
                                        functionParser,
                                        sqlNodeStack,
                                        aggModel,
                                        executionContext,
                                        joinMetadata,
                                        masterMetadata.getTimestampIndex(),
                                        true,
                                        groupByFunctions,
                                        groupByFunctionPositions,
                                        tempOuterProjectionFunctions,
                                        tempInnerProjectionFunctions,
                                        recordFunctionPositions,
                                        projectionFunctionFlags,
                                        outerProjectionMetadata,
                                        valueTypes,
                                        keyTypes,
                                        listColumnFilterA,
                                        null,
                                        validateSampleByFillType,
                                        isLastWindowJoin ? columns : aggregateCols
                                );

                                if (!isLastWindowJoin) {
                                    final GenericRecordMetadata metadata = GenericRecordMetadata.copyOfNew(joinMetadata, splitIndex);
                                    for (int j = 0, m = outerProjectionMetadata.getColumnCount(); j < m; j++) {
                                        metadata.add(outerProjectionMetadata.getColumnMetadata(j));
                                    }
                                    outerProjectionMetadata = metadata;
                                } else {
                                    outerProjectionMetadata.setTimestampIndex(timestampIndex);
                                }

                                ExpressionNode node = slaveModel.getOuterJoinExpressionClause();
                                // try to extract symbols equal function from join filter
                                int leftSymbolIndex = -1;
                                int rightSymbolIndex = -1;
                                ExpressionNode parent = null;
                                if (node != null) {
                                    final int columnSplit = master.getMetadata().getColumnCount();
                                    ExpressionNode nn = node;
                                    sqlNodeStack.clear();
                                    sqlNodeStack2.clear();
                                    boolean isLeft = false;
                                    while (!sqlNodeStack.isEmpty() || nn != null) {
                                        if (nn != null) {
                                            if (Chars.equals(nn.token, "=")) {
                                                final ExpressionNode l = nn.lhs;
                                                final ExpressionNode r = nn.rhs;
                                                if (l != null && r != null && l.type == ExpressionNode.LITERAL && r.type == ExpressionNode.LITERAL) {
                                                    final int leftColumnIndex = joinMetadata.getColumnIndexQuiet(l.token);
                                                    final int rightColumnIndex = joinMetadata.getColumnIndexQuiet(r.token);
                                                    if (leftColumnIndex >= 0 && rightColumnIndex >= 0) {
                                                        final boolean leftIsSymbol = joinMetadata.getColumnType(leftColumnIndex) == ColumnType.SYMBOL;
                                                        final boolean rightIsSymbol = joinMetadata.getColumnType(rightColumnIndex) == ColumnType.SYMBOL;
                                                        final boolean leftIsMaster = leftColumnIndex < columnSplit;
                                                        final boolean rightIsMaster = rightColumnIndex < columnSplit;
                                                        isLeft = parent != null && parent.lhs == nn;

                                                        if (leftIsSymbol && rightIsSymbol && leftIsMaster != rightIsMaster) {
                                                            leftSymbolIndex = leftIsMaster ? leftColumnIndex : rightColumnIndex;
                                                            rightSymbolIndex = leftIsMaster ? rightColumnIndex : leftColumnIndex;
                                                            rightSymbolIndex = rightSymbolIndex - columnSplit;
                                                            break;
                                                        }
                                                    }
                                                }
                                                nn = null;
                                            } else if (Chars.equals(nn.token, "and")) {
                                                if (nn.rhs != null) {
                                                    sqlNodeStack.push(nn.rhs);
                                                    sqlNodeStack2.push(nn);
                                                }
                                                parent = nn;
                                                nn = nn.lhs;
                                            } else {
                                                nn = null;
                                            }
                                        } else {
                                            nn = sqlNodeStack.poll();
                                            parent = sqlNodeStack2.pop();
                                        }
                                    }

                                    if (leftSymbolIndex != -1) {
                                        // extract success!
                                        if (parent != null && Chars.equals(parent.token, "and")) {
                                            if (isLeft) {
                                                parent.copyFrom(parent.rhs);
                                            } else {
                                                parent.copyFrom(parent.lhs);
                                            }
                                            parent = node;
                                        } else {
                                            parent = null;
                                        }
                                    } else {
                                        parent = node;
                                    }
                                    if (parent != null) {
                                        joinFilter = compileJoinFilter(parent, joinMetadata, executionContext);
                                    }
                                }

                                if (joinFilter != null && joinFilter.isConstant()) {
                                    joinFilter.init(null, executionContext);
                                    if (!joinFilter.getBool(null)) {
                                        RecordCursorFactory factory;
                                        if (isLastWindowJoin) {
                                            if (columnIndex == null) {
                                                factory = new ExtraNullColumnCursorFactory(outerProjectionMetadata, masterMetadata.getColumnCount(), master);
                                            } else {
                                                GenericRecordMetadata metadata = GenericRecordMetadata.copyOfNew(masterMetadata);
                                                for (int k = 0, m = aggregateCols.size(); k < m; k++) {
                                                    metadata.add(new TableColumnMetadata(aggregateCols.get(k).getAlias().toString(), groupByFunctions.get(k).getType()));
                                                }
                                                factory = new SelectedRecordCursorFactory(outerProjectionMetadata, columnIndex, new ExtraNullColumnCursorFactory(metadata, masterMetadata.getColumnCount(), master));
                                            }
                                        } else {
                                            factory = new ExtraNullColumnCursorFactory(outerProjectionMetadata, masterMetadata.getColumnCount(), master);
                                        }

                                        Misc.free(slave);
                                        Misc.free(joinMetadata);
                                        Misc.free(joinFilter);
                                        master = factory;
                                        break;
                                    } else {
                                        joinFilter = Misc.free(joinFilter);
                                        parent = null;
                                    }
                                }

                                // check that all group by functions can be vectorized
                                boolean allVectorized = joinFilter == null;
                                if (allVectorized) {
                                    if (aggregateCols.size() == 0) {
                                        allVectorized = false;
                                    } else {
                                        windowJoinAggColumnVectorizedCheck.of(joinMetadata, splitIndex);
                                        for (int j = 0, m = groupByFunctions.size(); j < m; j++) {
                                            var func = groupByFunctions.getQuick(j);
                                            if (!func.supportsBatchComputation()) {
                                                allVectorized = false;
                                                break;
                                            }
                                            traversalAlgo.traverse(aggregateCols.get(j).getAst(), windowJoinAggColumnVectorizedCheck);
                                            if (!windowJoinAggColumnVectorizedCheck.vectorized) {
                                                allVectorized = false;
                                                break;
                                            }
                                        }
                                    }
                                }

                                // is parallel windowJoin?
                                final boolean parallelWindowJoinEnabled = executionContext.isParallelWindowJoinEnabled();
                                final boolean masterSupportsPageFrames = master.supportsPageFrameCursor()
                                        || (master.supportsFilterStealing() && master.getBaseFactory().supportsPageFrameCursor());
                                if (parallelWindowJoinEnabled && masterSupportsPageFrames && slave.supportsTimeFrameCursor()) {
                                    // try to steal master filter
                                    CompiledFilter compiledFilter = null;
                                    MemoryCARW bindVarMemory = null;
                                    ObjList<Function> bindVarFunctions = null;
                                    Function masterFilter = null;
                                    ExpressionNode masterFilterExpr = null;
                                    IntHashSet masterFilterUsedColumnIndexes = null;
                                    if (master.supportsFilterStealing() && master.getBaseFactory().supportsPageFrameCursor()) {
                                        RecordCursorFactory filterFactory = master;
                                        master = master.getBaseFactory();
                                        compiledFilter = filterFactory.getCompiledFilter();
                                        bindVarMemory = filterFactory.getBindVarMemory();
                                        bindVarFunctions = filterFactory.getBindVarFunctions();
                                        masterFilter = filterFactory.getFilter();
                                        masterFilterExpr = filterFactory.getStealFilterExpr();
                                        masterFilterUsedColumnIndexes = new IntHashSet();
                                        collectColumnIndexes(sqlNodeStack, master.getMetadata(), masterFilterExpr, masterFilterUsedColumnIndexes);
                                        filterFactory.halfClose();
                                    }

                                    master.changePageFrameSizes(configuration.getSqlSmallPageFrameMinRows(), configuration.getSqlSmallPageFrameMaxRows());
                                    if (leftSymbolIndex != -1) {
                                        assert rightSymbolIndex != -1;
                                        master = new AsyncWindowJoinFastRecordCursorFactory(
                                                executionContext.getCairoEngine(),
                                                asm,
                                                configuration,
                                                executionContext.getMessageBus(),
                                                joinMetadata,
                                                outerProjectionMetadata,
                                                columnIndex,
                                                master,
                                                slave,
                                                joinFilter,
                                                compileWorkerFilterConditionally(
                                                        executionContext,
                                                        joinFilter,
                                                        executionContext.getSharedQueryWorkerCount(),
                                                        parent,
                                                        joinMetadata
                                                ),
                                                context.isIncludePrevailing(),
                                                leftSymbolIndex,
                                                rightSymbolIndex,
                                                lo,
                                                hi,
                                                valueTypes,
                                                groupByFunctions,
                                                compileWorkerGroupByFunctionsConditionally(
                                                        executionContext,
                                                        aggModel,
                                                        groupByFunctions,
                                                        executionContext.getSharedQueryWorkerCount(),
                                                        joinMetadata
                                                ),
                                                compiledFilter,
                                                bindVarMemory,
                                                bindVarFunctions,
                                                masterFilter,
                                                compileWorkerFilterConditionally(
                                                        executionContext,
                                                        masterFilter,
                                                        executionContext.getSharedQueryWorkerCount(),
                                                        masterFilterExpr,
                                                        master.getMetadata()
                                                ),
                                                masterFilterUsedColumnIndexes,
                                                allVectorized,
                                                reduceTaskFactory,
                                                executionContext.getSharedQueryWorkerCount()
                                        );
                                    } else {
                                        master = new AsyncWindowJoinRecordCursorFactory(
                                                executionContext.getCairoEngine(),
                                                asm,
                                                configuration,
                                                executionContext.getMessageBus(),
                                                joinMetadata,
                                                outerProjectionMetadata,
                                                columnIndex,
                                                master,
                                                slave,
                                                context.isIncludePrevailing(),
                                                joinFilter,
                                                compileWorkerFilterConditionally(
                                                        executionContext,
                                                        joinFilter,
                                                        executionContext.getSharedQueryWorkerCount(),
                                                        node,
                                                        joinMetadata
                                                ),
                                                lo,
                                                hi,
                                                valueTypes,
                                                groupByFunctions,
                                                compileWorkerGroupByFunctionsConditionally(
                                                        executionContext,
                                                        aggModel,
                                                        groupByFunctions,
                                                        executionContext.getSharedQueryWorkerCount(),
                                                        joinMetadata
                                                ),
                                                compiledFilter,
                                                bindVarMemory,
                                                bindVarFunctions,
                                                masterFilter,
                                                compileWorkerFilterConditionally(
                                                        executionContext,
                                                        masterFilter,
                                                        executionContext.getSharedQueryWorkerCount(),
                                                        masterFilterExpr,
                                                        master.getMetadata()
                                                ),
                                                masterFilterUsedColumnIndexes,
                                                allVectorized,
                                                reduceTaskFactory,
                                                executionContext.getSharedQueryWorkerCount()
                                        );
                                    }
                                    executionContext.storeTelemetry(TelemetryEvent.PARALLEL_WINDOW_JOIN, TelemetryOrigin.NO_MATTERS);
                                } else if (slave.supportsTimeFrameCursor()) {
                                    if (leftSymbolIndex != -1) {
                                        master = new WindowJoinFastRecordCursorFactory(
                                                asm,
                                                configuration,
                                                outerProjectionMetadata,
                                                joinMetadata,
                                                master,
                                                slave,
                                                columnIndex,
                                                context.isIncludePrevailing(),
                                                lo,
                                                hi,
                                                groupByFunctions,
                                                valueTypes,
                                                rightSymbolIndex,
                                                leftSymbolIndex,
                                                joinFilter,
                                                allVectorized
                                        );
                                    } else {
                                        master = new WindowJoinRecordCursorFactory(
                                                asm,
                                                configuration,
                                                outerProjectionMetadata,
                                                joinMetadata,
                                                master,
                                                slave,
                                                context.isIncludePrevailing(),
                                                columnIndex,
                                                lo,
                                                hi,
                                                groupByFunctions,
                                                valueTypes,
                                                joinFilter
                                        );
                                    }
                                    executionContext.storeTelemetry(TelemetryEvent.SINGLE_THREAD_WINDOW_JOIN, TelemetryOrigin.NO_MATTERS);
                                } else {
                                    throw SqlException.position(slaveModel.getJoinKeywordPosition()).put("right side of window join must be a table, not sub-query");
                                }
                                break;
                            default:
                                processJoinContext(index == 1, isSameTable(master, slave), slaveModel.getJoinContext(), masterMetadata, slaveMetadata);
                                joinMetadata = createJoinMetadata(masterAlias, masterMetadata, slaveModel.getName(), slaveMetadata, joinType == JOIN_RIGHT_OUTER || joinType == JOIN_FULL_OUTER ? -1 : masterMetadata.getTimestampIndex());
                                if (slaveModel.getOuterJoinExpressionClause() != null) {
                                    joinFilter = compileJoinFilter(slaveModel.getOuterJoinExpressionClause(), joinMetadata, executionContext);
                                }

                                if (joinFilter != null && joinFilter.isConstant() && !joinFilter.getBool(null)) {
                                    if (joinType == JOIN_LEFT_OUTER) {
                                        Misc.free(slave);
                                        slave = new EmptyTableRecordCursorFactory(slaveMetadata);
                                    } else if (joinType == JOIN_INNER) {
                                        Misc.free(master);
                                        Misc.free(slave);
                                        return new EmptyTableRecordCursorFactory(joinMetadata);
                                    } else if (joinType == JOIN_RIGHT_OUTER) {
                                        Misc.free(master);
                                        master = new EmptyTableRecordCursorFactory(masterMetadata);
                                    }
                                }

                                if (joinType == JOIN_INNER) {
                                    validateOuterJoinExpressions(slaveModel, "INNER");
                                }

                                master = createHashJoin(
                                        joinMetadata,
                                        master,
                                        slave,
                                        joinType,
                                        joinFilter,
                                        slaveModel.getJoinContext()
                                );
                                masterAlias = null;
                                break;
                        }
                    }
                } catch (Throwable th) {
                    Misc.free(joinMetadata);
                    Misc.free(joinFilter);
                    Misc.freeObjList(groupByFunctions);
                    master = Misc.free(master);
                    if (closeSlaveOnFailure) {
                        Misc.free(slave);
                    }
                    throw th;
                } finally {
                    executionContext.popTimestampRequiredFlag();
                }

                // check if there are post-filters
                ExpressionNode filterExpr = slaveModel.getPostJoinWhereClause();
                if (filterExpr != null) {
                    if (executionContext.isParallelFilterEnabled() && master.supportsPageFrameCursor()) {
                        final Function filter = compileJoinFilter(
                                filterExpr,
                                master.getMetadata(),
                                executionContext
                        );
                        IntHashSet filterUsedColumnIndexes = new IntHashSet();
                        collectColumnIndexes(sqlNodeStack, master.getMetadata(), filterExpr, filterUsedColumnIndexes);

                        master = new AsyncFilteredRecordCursorFactory(
                                executionContext.getCairoEngine(),
                                configuration,
                                executionContext.getMessageBus(),
                                master,
                                filter,
                                filterUsedColumnIndexes,
                                reduceTaskFactory,
                                compileWorkerFilterConditionally(
                                        executionContext,
                                        filter,
                                        executionContext.getSharedQueryWorkerCount(),
                                        filterExpr,
                                        master.getMetadata()
                                ),
                                deepClone(expressionNodePool, filterExpr),
                                null,
                                0,
                                executionContext.getSharedQueryWorkerCount(),
                                SqlHints.hasEnablePreTouchHint(model, masterAlias)
                        );
                    } else {
                        master = new FilteredRecordCursorFactory(
                                master,
                                compileJoinFilter(filterExpr, master.getMetadata(), executionContext)
                        );
                    }
                }
            }

            // unfortunately we had to go all out to create join metadata
            // now it is time to check if we have constant conditions
            ExpressionNode constFilterExpr = model.getConstWhereClause();
            if (constFilterExpr != null) {
                Function filter = functionParser.parseFunction(constFilterExpr, null, executionContext);
                if (!isBoolean(filter.getType())) {
                    throw SqlException.position(constFilterExpr.position).put("boolean expression expected");
                }
                filter.init(null, executionContext);
                if (filter.isConstant()) {
                    if (!filter.getBool(null)) {
                        // do not copy metadata here
                        // this would have been JoinRecordMetadata, which is new instance anyway
                        // we have to make sure that this metadata is safely transitioned
                        // to empty cursor factory
                        RecordMetadata metadata = master.getMetadata();
                        if (metadata instanceof JoinRecordMetadata that) {
                            that.incrementRefCount();
                        }
                        RecordCursorFactory factory = new EmptyTableRecordCursorFactory(metadata);
                        Misc.free(master);
                        return factory;
                    }
                } else {
                    // make it a post-join filter (same as for post join where clause above)
                    if (executionContext.isParallelFilterEnabled() && master.supportsPageFrameCursor()) {
                        IntHashSet filterUsedColumnIndexes = new IntHashSet();
                        collectColumnIndexes(sqlNodeStack, master.getMetadata(), constFilterExpr, filterUsedColumnIndexes);

                        master = new AsyncFilteredRecordCursorFactory(
                                executionContext.getCairoEngine(),
                                configuration,
                                executionContext.getMessageBus(),
                                master,
                                filter,
                                filterUsedColumnIndexes,
                                reduceTaskFactory,
                                compileWorkerFilterConditionally(
                                        executionContext,
                                        filter,
                                        executionContext.getSharedQueryWorkerCount(),
                                        constFilterExpr,
                                        master.getMetadata()
                                ),
                                deepClone(expressionNodePool, constFilterExpr),
                                null,
                                0,
                                executionContext.getSharedQueryWorkerCount(),
                                SqlHints.hasEnablePreTouchHint(model, masterAlias)
                        );
                    } else {
                        master = new FilteredRecordCursorFactory(master, filter);
                    }
                }
            }
            return master;
        } catch (Throwable e) {
            Misc.free(master);
            executionContext.popIntervalModel();
            executionContext.popHasInterval();
            throw e;
        }
    }

    @NotNull
    private RecordCursorFactory generateLatestBy(RecordCursorFactory factory, QueryModel model) throws SqlException {
        final ObjList<ExpressionNode> latestBy = model.getLatestBy();
        if (latestBy.size() == 0) {
            return factory;
        }

        // We require timestamp with any order.
        final int timestampIndex;
        try {
            timestampIndex = getTimestampIndex(model, factory);
            if (timestampIndex == -1) {
                throw SqlException.$(model.getModelPosition(), "latest by query does not provide dedicated TIMESTAMP column");
            }
        } catch (Throwable e) {
            Misc.free(factory);
            throw e;
        }

        final RecordMetadata metadata = factory.getMetadata();
        prepareLatestByColumnIndexes(latestBy, metadata);

        if (!factory.recordCursorSupportsRandomAccess()) {
            return new LatestByRecordCursorFactory(
                    configuration,
                    factory,
                    RecordSinkFactory.getInstance(asm, metadata, listColumnFilterA, configuration),
                    keyTypes,
                    timestampIndex
            );
        }

        boolean orderedByTimestampAsc = false;
        final QueryModel nested = model.getNestedModel();
        assert nested != null;
        final LowerCaseCharSequenceIntHashMap orderBy = nested.getOrderHash();
        CharSequence timestampColumn = metadata.getColumnName(timestampIndex);
        if (orderBy.get(timestampColumn) == ORDER_DIRECTION_ASCENDING) {
            // ORDER BY the timestamp column case.
            orderedByTimestampAsc = true;
        } else if (timestampIndex == metadata.getTimestampIndex() && orderBy.size() == 0) {
            // Empty ORDER BY, but the timestamp column in the designated timestamp.
            orderedByTimestampAsc = true;
        }

        return new LatestByLightRecordCursorFactory(
                configuration,
                factory,
                RecordSinkFactory.getInstance(asm, metadata, listColumnFilterA, configuration),
                keyTypes,
                timestampIndex,
                orderedByTimestampAsc
        );
    }

    @NotNull
    private RecordCursorFactory generateLatestByTableQuery(
            QueryModel model,
            @Transient TableReader reader,
            RecordMetadata metadata,
            TableToken tableToken,
            IntrinsicModel intrinsicModel,
            Function filter,
            @Transient SqlExecutionContext executionContext,
            int timestampIndex,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizeShifts,
            @NotNull LongList prefixes,
            int hasInterval
    ) throws SqlException {
        final ExpressionNode viewExpr = model.getViewNameExpr();
        final PartitionFrameCursorFactory partitionFrameCursorFactory;
        if (intrinsicModel.hasIntervalFilters()) {
            RuntimeIntrinsicIntervalModel intervalModel = intrinsicModel.buildIntervalModel();
            if (hasInterval == 0) {
                executionContext.popIntervalModel();
                executionContext.pushIntervalModel(intervalModel);
            }
            partitionFrameCursorFactory = new IntervalPartitionFrameCursorFactory(
                    tableToken,
                    model.getMetadataVersion(),
                    intervalModel,
                    timestampIndex,
                    GenericRecordMetadata.copyOfNew(reader.getMetadata()),
                    ORDER_DESC,
                    getViewName(viewExpr),
                    getViewPosition(viewExpr),
                    model.isUpdate()
            );
        } else {
            partitionFrameCursorFactory = new FullPartitionFrameCursorFactory(
                    tableToken,
                    model.getMetadataVersion(),
                    GenericRecordMetadata.copyOfNew(reader.getMetadata()),
                    ORDER_DESC,
                    getViewName(viewExpr),
                    getViewPosition(viewExpr),
                    model.isUpdate()
            );
        }

        assert model.getLatestBy() != null && model.getLatestBy().size() > 0;
        ObjList<ExpressionNode> latestBy = new ObjList<>(model.getLatestBy().size());
        latestBy.addAll(model.getLatestBy());
        final ExpressionNode latestByNode = latestBy.get(0);
        final int latestByIndex = metadata.getColumnIndexQuiet(latestByNode.token);
        final boolean indexed = metadata.isColumnIndexed(latestByIndex);

        // 'latest by' clause takes over the filter and the latest by nodes,
        // so that the later generateFilter() and generateLatestBy() are no-op
        model.setWhereClause(null);
        model.getLatestBy().clear();

        // if there are > 1 columns in the latest by statement, we cannot use indexes
        if (latestBy.size() > 1 || !isSymbol(metadata.getColumnType(latestByIndex))) {
            boolean symbolKeysOnly = true;
            for (int i = 0, n = keyTypes.getColumnCount(); i < n; i++) {
                symbolKeysOnly &= isSymbol(keyTypes.getColumnType(i));
            }
            if (symbolKeysOnly) {
                final IntList partitionByColumnIndexes = new IntList(listColumnFilterA.size());
                for (int i = 0, n = listColumnFilterA.size(); i < n; i++) {
                    partitionByColumnIndexes.add(listColumnFilterA.getColumnIndexFactored(i));
                }
                final IntList partitionBySymbolCounts = symbolEstimator.estimate(
                        model,
                        intrinsicModel.filter,
                        metadata,
                        partitionByColumnIndexes
                );
                return new LatestByAllSymbolsFilteredRecordCursorFactory(
                        configuration,
                        metadata,
                        partitionFrameCursorFactory,
                        RecordSinkFactory.getInstance(asm, metadata, listColumnFilterA, configuration),
                        keyTypes,
                        partitionByColumnIndexes,
                        partitionBySymbolCounts,
                        filter,
                        columnIndexes,
                        columnSizeShifts
                );
            }
            return new LatestByAllFilteredRecordCursorFactory(
                    configuration,
                    metadata,
                    partitionFrameCursorFactory,
                    RecordSinkFactory.getInstance(asm, metadata, listColumnFilterA, configuration),
                    keyTypes,
                    filter,
                    columnIndexes,
                    columnSizeShifts
            );
        }

        if (intrinsicModel.keyColumn != null) {
            // key column must always be the same as latest by column
            assert latestByIndex == metadata.getColumnIndexQuiet(intrinsicModel.keyColumn);

            if (intrinsicModel.keySubQuery != null) {
                RecordCursorFactory rcf = null;
                final Record.CharSequenceFunction func;
                try {
                    rcf = generate(intrinsicModel.keySubQuery, executionContext);
                    func = validateSubQueryColumnAndGetGetter(intrinsicModel, rcf.getMetadata());
                } catch (Throwable th) {
                    Misc.free(partitionFrameCursorFactory);
                    Misc.free(rcf);
                    throw th;
                }

                return new LatestBySubQueryRecordCursorFactory(
                        configuration,
                        metadata,
                        partitionFrameCursorFactory,
                        latestByIndex,
                        rcf,
                        filter,
                        indexed,
                        func,
                        columnIndexes,
                        columnSizeShifts
                );
            }

            final int nKeyValues = intrinsicModel.keyValueFuncs.size();
            final int nExcludedKeyValues = intrinsicModel.keyExcludedValueFuncs.size();
            if (indexed && nExcludedKeyValues == 0) {
                assert nKeyValues > 0;
                // deal with key values as a list
                // 1. resolve each value of the list to "int"
                // 2. get first row in index for each value (stream)

                final SymbolMapReader symbolMapReader = reader.getSymbolMapReader(columnIndexes.getQuick(latestByIndex));
                final RowCursorFactory rcf;
                if (nKeyValues == 1) {
                    final Function symbolValueFunc = intrinsicModel.keyValueFuncs.get(0);
                    final int symbol = symbolValueFunc.isRuntimeConstant()
                            ? SymbolTable.VALUE_NOT_FOUND
                            : symbolMapReader.keyOf(symbolValueFunc.getStrA(null));

                    if (filter == null) {
                        if (symbol == SymbolTable.VALUE_NOT_FOUND) {
                            rcf = new LatestByValueDeferredIndexedRowCursorFactory(
                                    latestByIndex,
                                    symbolValueFunc,
                                    false
                            );
                        } else {
                            rcf = new LatestByValueIndexedRowCursorFactory(
                                    latestByIndex,
                                    symbol,
                                    false
                            );
                        }
                        return new PageFrameRecordCursorFactory(
                                configuration,
                                metadata,
                                partitionFrameCursorFactory,
                                rcf,
                                false,
                                null,
                                false,
                                columnIndexes,
                                columnSizeShifts,
                                true,
                                true
                        );
                    }

                    if (symbol == SymbolTable.VALUE_NOT_FOUND) {
                        return new LatestByValueDeferredIndexedFilteredRecordCursorFactory(
                                configuration,
                                metadata,
                                partitionFrameCursorFactory,
                                latestByIndex,
                                symbolValueFunc,
                                filter,
                                columnIndexes,
                                columnSizeShifts
                        );
                    }
                    return new LatestByValueIndexedFilteredRecordCursorFactory(
                            configuration,
                            metadata,
                            partitionFrameCursorFactory,
                            latestByIndex,
                            symbol,
                            filter,
                            columnIndexes,
                            columnSizeShifts
                    );
                }

                return new LatestByValuesIndexedFilteredRecordCursorFactory(
                        configuration,
                        metadata,
                        partitionFrameCursorFactory,
                        latestByIndex,
                        intrinsicModel.keyValueFuncs,
                        symbolMapReader,
                        filter,
                        columnIndexes,
                        columnSizeShifts
                );
            }

            assert nKeyValues > 0 || nExcludedKeyValues > 0;

            // we have "latest by" column values, but no index

            if (nKeyValues > 1 || nExcludedKeyValues > 0) {
                return new LatestByDeferredListValuesFilteredRecordCursorFactory(
                        configuration,
                        metadata,
                        partitionFrameCursorFactory,
                        latestByIndex,
                        intrinsicModel.keyValueFuncs,
                        intrinsicModel.keyExcludedValueFuncs,
                        filter,
                        columnIndexes,
                        columnSizeShifts
                );
            }

            assert nExcludedKeyValues == 0;

            // we have a single symbol key
            final Function symbolKeyFunc = intrinsicModel.keyValueFuncs.get(0);
            final SymbolMapReader symbolMapReader = reader.getSymbolMapReader(columnIndexes.getQuick(latestByIndex));
            final int symbolKey = symbolKeyFunc.isRuntimeConstant()
                    ? SymbolTable.VALUE_NOT_FOUND
                    : symbolMapReader.keyOf(symbolKeyFunc.getStrA(null));
            if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                return new LatestByValueDeferredFilteredRecordCursorFactory(
                        configuration,
                        metadata,
                        partitionFrameCursorFactory,
                        latestByIndex,
                        symbolKeyFunc,
                        filter,
                        columnIndexes,
                        columnSizeShifts
                );
            }

            return new LatestByValueFilteredRecordCursorFactory(
                    configuration,
                    metadata,
                    partitionFrameCursorFactory,
                    latestByIndex,
                    symbolKey,
                    filter,
                    columnIndexes,
                    columnSizeShifts
            );
        }
        // we select all values of "latest by" column

        assert intrinsicModel.keyValueFuncs.size() == 0;
        // get the latest rows for all values of "latest by" column

        if (indexed && filter == null && configuration.useWithinLatestByOptimisation()) {
            return new LatestByAllIndexedRecordCursorFactory(
                    executionContext.getCairoEngine(),
                    configuration,
                    metadata,
                    partitionFrameCursorFactory,
                    latestByIndex,
                    columnIndexes,
                    columnSizeShifts,
                    prefixes
            );
        } else {
            return new LatestByDeferredListValuesFilteredRecordCursorFactory(
                    configuration,
                    metadata,
                    partitionFrameCursorFactory,
                    latestByIndex,
                    filter,
                    columnIndexes,
                    columnSizeShifts
            );
        }
    }

    private RecordCursorFactory generateLimit(
            RecordCursorFactory factory,
            QueryModel model,
            SqlExecutionContext executionContext
    ) throws SqlException {
        ExpressionNode limitLo = model.getLimitLo();
        ExpressionNode limitHi = model.getLimitHi();

        // we've to check model otherwise we could be skipping limit in outer query that's actually different from the one in inner query!
        if ((limitLo == null && limitHi == null) || (factory.implementsLimit() && (limitLo != null && limitLo.implemented))) {
            return factory;
        }

        try {
            final Function loFunc = getLoFunction(model, executionContext);
            final Function hiFunc = getHiFunction(model, executionContext);
            return new LimitRecordCursorFactory(
                    factory, loFunc, hiFunc, limitLo != null ? limitLo.position : limitHi.position
            );
        } catch (Throwable e) {
            Misc.free(factory);
            throw e;
        }
    }

    private RecordCursorFactory generateNoSelect(
            QueryModel model,
            SqlExecutionContext executionContext
    ) throws SqlException {
        ExpressionNode tableNameExpr = model.getTableNameExpr();
        if (tableNameExpr != null) {
            if (tableNameExpr.type == FUNCTION) {
                return generateFunctionQuery(model, executionContext);
            } else {
                return generateTableQuery(model, executionContext);
            }
        }
        return generateSubQuery(model, executionContext);
    }

    private RecordCursorFactory generateOrderBy(
            RecordCursorFactory recordCursorFactory,
            QueryModel model,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (recordCursorFactory.followedOrderByAdvice()) {
            return recordCursorFactory;
        }
        try {
            final LowerCaseCharSequenceIntHashMap orderByColumnNameToIndexMap = model.getOrderHash();
            final ObjList<CharSequence> orderByColumnNames = orderByColumnNameToIndexMap.keys();
            final int orderByColumnCount = orderByColumnNames.size();

            if (orderByColumnCount > 0) {
                final RecordMetadata metadata = recordCursorFactory.getMetadata();
                final int timestampIndex = metadata.getTimestampIndex();

                listColumnFilterA.clear();
                intHashSet.clear();

                int orderedByTimestampIndex = -1;
                // column index sign indicates a direction;
                // therefore, 0 index is not allowed
                for (int i = 0; i < orderByColumnCount; i++) {
                    final CharSequence column = orderByColumnNames.getQuick(i);
                    int index = metadata.getColumnIndexQuiet(column);

                    // check if the column type is supported
                    final int columnType = metadata.getColumnType(index);
                    if (!isComparable(columnType)) {
                        // find position of offending column
                        ObjList<ExpressionNode> nodes = model.getOrderBy();
                        int position = 0;
                        for (int j = 0, y = nodes.size(); j < y; j++) {
                            if (Chars.equals(column, nodes.getQuick(i).token)) {
                                position = nodes.getQuick(i).position;
                                break;
                            }
                        }
                        throw SqlException.$(position, ColumnType.nameOf(columnType)).put(" is not a supported type in ORDER BY clause");
                    }

                    // we also maintain a unique set of column indexes for better performance
                    if (intHashSet.add(index)) {
                        if (orderByColumnNameToIndexMap.get(column) == ORDER_DIRECTION_DESCENDING) {
                            listColumnFilterA.add(-index - 1);
                        } else {
                            listColumnFilterA.add(index + 1);
                        }
                        if (i == 0 && isTimestamp(metadata.getColumnType(index))) {
                            orderedByTimestampIndex = index;
                        }
                    }
                }

                boolean preSortedByTs = false;
                // if first column index is the same as timestamp of underlying record cursor factory
                // we could have two possibilities:
                // 1. if we only have one column to order by - the cursor would already be ordered
                //    by timestamp (either ASC or DESC); we have nothing to do
                // 2. metadata of the new cursor will have the timestamp
                if (timestampIndex != -1) {
                    CharSequence column = orderByColumnNames.getQuick(0);
                    int index = metadata.getColumnIndexQuiet(column);
                    if (index == timestampIndex) {
                        if (orderByColumnCount == 1) {
                            if (orderByColumnNameToIndexMap.get(column) == ORDER_DIRECTION_ASCENDING
                                    && recordCursorFactory.getScanDirection() == RecordCursorFactory.SCAN_DIRECTION_FORWARD) {
                                return recordCursorFactory;
                            } else if (orderByColumnNameToIndexMap.get(column) == ORDER_DIRECTION_DESCENDING
                                    && recordCursorFactory.getScanDirection() == RecordCursorFactory.SCAN_DIRECTION_BACKWARD) {
                                return recordCursorFactory;
                            }
                        } else { // orderByColumnCount > 1
                            preSortedByTs = (orderByColumnNameToIndexMap.get(column) == ORDER_DIRECTION_ASCENDING && recordCursorFactory.getScanDirection() == RecordCursorFactory.SCAN_DIRECTION_FORWARD)
                                    || (orderByColumnNameToIndexMap.get(column) == ORDER_DIRECTION_DESCENDING && recordCursorFactory.getScanDirection() == RecordCursorFactory.SCAN_DIRECTION_BACKWARD);
                        }
                    }
                }

                GenericRecordMetadata orderedMetadata;
                int firstOrderByColumnIndex = metadata.getColumnIndexQuiet(orderByColumnNames.getQuick(0));
                if (firstOrderByColumnIndex == timestampIndex) {
                    orderedMetadata = GenericRecordMetadata.copyOf(metadata);
                } else {
                    orderedMetadata = GenericRecordMetadata.copyOfSansTimestamp(metadata);
                    orderedMetadata.setTimestampIndex(orderedByTimestampIndex);
                }
                final Function loFunc = getLoFunction(model, executionContext);
                final Function hiFunc = getHiFunction(model, executionContext);

                if (recordCursorFactory.recordCursorSupportsRandomAccess()) {
                    if (canSortAndLimitBeOptimized(model, executionContext, loFunc, hiFunc)) {
                        if (!preSortedByTs && loFunc.isConstant() && hiFunc == null) {
                            final long lo = loFunc.getLong(null);
                            if (lo > 0 && lo <= Integer.MAX_VALUE) {
                                // Long top K has decent performance even though being single-threaded,
                                // so we prefer it over the parallel top K factory.
                                if (listColumnFilterA.size() == 1) {
                                    final int index = listColumnFilterA.getQuick(0);
                                    final int columnIndex = (index > 0 ? index : -index) - 1;
                                    if (recordCursorFactory.recordCursorSupportsLongTopK(columnIndex)) {
                                        return new LongTopKRecordCursorFactory(
                                                orderedMetadata,
                                                recordCursorFactory,
                                                columnIndex,
                                                (int) lo,
                                                index > 0
                                        );
                                    }
                                }

                                final boolean parallelTopKEnabled = executionContext.isParallelTopKEnabled();
                                if (parallelTopKEnabled && (recordCursorFactory.supportsPageFrameCursor() || recordCursorFactory.supportsFilterStealing())) {
                                    QueryModel.restoreWhereClause(expressionNodePool, model);

                                    RecordCursorFactory baseFactory = recordCursorFactory;
                                    CompiledFilter compiledFilter = null;
                                    MemoryCARW bindVarMemory = null;
                                    ObjList<Function> bindVarFunctions = null;
                                    Function filter = null;
                                    ExpressionNode filterExpr = null;
                                    IntHashSet filterUsedColumnIndexes = null;
                                    if (recordCursorFactory.supportsFilterStealing()) {
                                        baseFactory = recordCursorFactory.getBaseFactory();
                                        compiledFilter = recordCursorFactory.getCompiledFilter();
                                        bindVarMemory = recordCursorFactory.getBindVarMemory();
                                        bindVarFunctions = recordCursorFactory.getBindVarFunctions();
                                        filter = recordCursorFactory.getFilter();
                                        filterExpr = recordCursorFactory.getStealFilterExpr();
                                        filterUsedColumnIndexes = new IntHashSet();
                                        collectColumnIndexes(sqlNodeStack, baseFactory.getMetadata(), filterExpr, filterUsedColumnIndexes);
                                        recordCursorFactory.halfClose();
                                    }

                                    final QueryModel nested = model.getNestedModel();
                                    assert nested != null;

                                    return new AsyncTopKRecordCursorFactory(
                                            executionContext.getCairoEngine(),
                                            configuration,
                                            executionContext.getMessageBus(),
                                            orderedMetadata,
                                            baseFactory,
                                            filter,
                                            filterUsedColumnIndexes,
                                            compileWorkerFilterConditionally(
                                                    executionContext,
                                                    filter,
                                                    executionContext.getSharedQueryWorkerCount(),
                                                    filterExpr,
                                                    baseFactory.getMetadata()
                                            ),
                                            compiledFilter,
                                            bindVarMemory,
                                            bindVarFunctions,
                                            recordComparatorCompiler,
                                            listColumnFilterA.copy(),
                                            metadata,
                                            lo,
                                            executionContext.getSharedQueryWorkerCount()
                                    );
                                }
                            }
                        }

                        final int baseCursorTimestampIndex = preSortedByTs ? timestampIndex : -1;
                        return new LimitedSizeSortedLightRecordCursorFactory(
                                configuration,
                                orderedMetadata,
                                recordCursorFactory,
                                recordComparatorCompiler.newInstance(metadata, listColumnFilterA),
                                loFunc,
                                hiFunc,
                                listColumnFilterA.copy(),
                                baseCursorTimestampIndex
                        );
                    } else {
                        final int columnType = orderedMetadata.getColumnType(firstOrderByColumnIndex);
                        if (
                                configuration.isSqlOrderBySortEnabled()
                                        && orderByColumnNames.size() == 1
                                        && LongSortedLightRecordCursorFactory.isSupportedColumnType(columnType)
                        ) {
                            return new LongSortedLightRecordCursorFactory(
                                    configuration,
                                    orderedMetadata,
                                    recordCursorFactory,
                                    listColumnFilterA.copy()
                            );
                        }

                        return new SortedLightRecordCursorFactory(
                                configuration,
                                orderedMetadata,
                                recordCursorFactory,
                                recordComparatorCompiler.newInstance(metadata, listColumnFilterA),
                                listColumnFilterA.copy()
                        );
                    }
                }

                // when base record cursor does not support random access
                // we have to copy entire record into ordered structure

                entityColumnFilter.of(orderedMetadata.getColumnCount());
                return new SortedRecordCursorFactory(
                        configuration,
                        orderedMetadata,
                        recordCursorFactory,
                        RecordSinkFactory.getInstance(asm, orderedMetadata, entityColumnFilter, configuration),
                        recordComparatorCompiler.newInstance(metadata, listColumnFilterA),
                        listColumnFilterA.copy()
                );
            }

            return recordCursorFactory;
        } catch (SqlException | CairoException e) {
            recordCursorFactory.close();
            throw e;
        }
    }

    private RecordCursorFactory generateQuery(QueryModel model, SqlExecutionContext executionContext, boolean processJoins) throws SqlException {
        RecordCursorFactory factory = generateQuery0(model, executionContext, processJoins);
        if (model.getUnionModel() != null) {
            return generateSetFactory(model, factory, executionContext);
        }
        return factory;
    }

    private RecordCursorFactory generateQuery0(QueryModel model, SqlExecutionContext executionContext, boolean processJoins) throws SqlException {
        // Remember the last model with non-empty ORDER BY as we descend through nested models.
        // We need the ORDER BY clause in the Markout Horizon Join optimization, but it's stored
        // several levels up from the model that holds the join clause.
        boolean pushed = false;
        final QueryModel savedOrderByModel = lastSeenOrderByModel;
        try {
            final ObjList<ExpressionNode> orderBy = model.getOrderBy();
            if (orderBy != null && orderBy.size() > 0) {
                lastSeenOrderByModel = model;

                // when order-by specific here it would be pointless to require timestamp from the
                // nested models
                executionContext.pushTimestampRequiredFlag(false);
                pushed = true;
            }
            RecordCursorFactory factory;

            factory = generateSelect(model, executionContext, processJoins);
            factory = generateFilter(factory, model, executionContext);
            factory = generateLatestBy(factory, model);
            factory = generateOrderBy(factory, model, executionContext);
            factory = generateLimit(factory, model, executionContext);

            return factory;
        } finally {
            lastSeenOrderByModel = savedOrderByModel;
            if (pushed) {
                executionContext.popTimestampRequiredFlag();
            }
        }
    }

    @NotNull
    private RecordCursorFactory generateSampleBy(
            QueryModel model,
            SqlExecutionContext executionContext,
            ExpressionNode sampleByNode,
            ExpressionNode sampleByUnits
    ) throws SqlException {
        final ExpressionNode timezoneName = model.getSampleByTimezoneName();
        final Function timezoneNameFunc;
        final int timezoneNameFuncPos;
        final ExpressionNode offset = model.getSampleByOffset();
        final Function offsetFunc;
        final int offsetFuncPos;
        final Function sampleFromFunc;
        final int sampleFromFuncPos;
        final Function sampleToFunc;
        final int sampleToFuncPos;

        if (timezoneName != null) {
            timezoneNameFunc = functionParser.parseFunction(
                    timezoneName,
                    EmptyRecordMetadata.INSTANCE,
                    executionContext
            );
            timezoneNameFuncPos = timezoneName.position;
            coerceRuntimeConstantType(timezoneNameFunc, STRING, executionContext, "timezone must be a constant expression of STRING or CHAR type", timezoneNameFuncPos);
        } else {
            timezoneNameFunc = StrConstant.NULL;
            timezoneNameFuncPos = 0;
        }

        if (offset != null) {
            offsetFunc = functionParser.parseFunction(
                    offset,
                    EmptyRecordMetadata.INSTANCE,
                    executionContext
            );
            offsetFuncPos = offset.position;
            coerceRuntimeConstantType(offsetFunc, STRING, executionContext, "offset must be a constant expression of STRING or CHAR type", offsetFuncPos);
        } else {
            offsetFunc = StrConstant.NULL;
            offsetFuncPos = 0;
        }

        RecordCursorFactory factory = null;
        // We require timestamp with asc order.
        final int timestampIndex;
        // Require timestamp in sub-query when it's not additionally specified as timestamp(col).
        executionContext.pushTimestampRequiredFlag(model.getTimestamp() == null);
        try {
            factory = generateSubQuery(model, executionContext);
            timestampIndex = getTimestampIndex(model, factory);
            if (timestampIndex == -1) {
                throw SqlException.$(model.getModelPosition(), "base query does not provide designated TIMESTAMP column");
            }
            if (factory.getScanDirection() != RecordCursorFactory.SCAN_DIRECTION_FORWARD) {
                throw SqlException.$(model.getModelPosition(), "base query does not provide ASC order over designated TIMESTAMP column");
            }
        } catch (Throwable e) {
            Misc.free(factory);
            throw e;
        } finally {
            executionContext.popTimestampRequiredFlag();
        }

        final RecordMetadata baseMetadata = factory.getMetadata();
        ObjList<ExpressionNode> sampleByFill = model.getSampleByFill();
        final int timestampType = baseMetadata.getColumnType(timestampIndex);
        final TimestampDriver timestampDriver = getTimestampDriver(timestampType);

        if (model.getSampleByFrom() != null) {
            sampleFromFunc = functionParser.parseFunction(model.getSampleByFrom(), EmptyRecordMetadata.INSTANCE, executionContext);
            sampleFromFuncPos = model.getSampleByFrom().position;
            coerceRuntimeConstantType(sampleFromFunc, timestampType, executionContext, "from lower bound must be a constant expression convertible to a TIMESTAMP", sampleFromFuncPos);
        } else {
            sampleFromFunc = timestampDriver.getTimestampConstantNull();
            sampleFromFuncPos = 0;
        }

        if (model.getSampleByTo() != null) {
            sampleToFunc = functionParser.parseFunction(model.getSampleByTo(), EmptyRecordMetadata.INSTANCE, executionContext);
            sampleToFuncPos = model.getSampleByTo().position;
            coerceRuntimeConstantType(sampleToFunc, timestampType, executionContext, "to upper bound must be a constant expression convertible to a TIMESTAMP", sampleToFuncPos);
        } else {
            sampleToFunc = timestampDriver.getTimestampConstantNull();
            sampleToFuncPos = 0;
        }

        final boolean isFromTo = sampleFromFunc != timestampDriver.getTimestampConstantNull() || sampleToFunc != timestampDriver.getTimestampConstantNull();
        final TimestampSampler timestampSampler;
        int fillCount = sampleByFill.size();

        // sampleByFill is originally set up based on GroupByFunctions in BottomUpColumns,
        // but TopDownColumns may have different order and count with BottomUpColumns.
        // Need to reorganize sampleByFill according to the position relationship between
        // TopDownColumns and BottomUpColumns to ensure correct fill value alignment.
        if (fillCount > 1 && model.getTopDownColumns().size() != 0) {
            tempColumnsList.clear();
            for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
                final QueryColumn column = model.getBottomUpColumns().getQuick(i);
                if (!column.isWindowExpression()) {
                    final ExpressionNode node = column.getAst();
                    if (node.type == FUNCTION && functionParser.getFunctionFactoryCache().isGroupBy(node.token)) {
                        tempColumnsList.add(column);
                    }
                }
            }

            tempExpressionNodeList.clear();
            for (int i = 0, n = model.getTopDownColumns().size(); i < n; i++) {
                int index = tempColumnsList.indexOf(model.getTopDownColumns().getQuick(i));
                if (index != -1 && fillCount > index) {
                    tempExpressionNodeList.add(sampleByFill.getQuick(index));
                }
            }
            sampleByFill = tempExpressionNodeList;
            fillCount = sampleByFill.size();
        }

        try {
            if (sampleByUnits == null) {
                timestampSampler = TimestampSamplerFactory.getInstance(timestampDriver, sampleByNode.token, sampleByNode.position);
            } else {
                Function sampleByPeriod = functionParser.parseFunction(
                        sampleByNode,
                        EmptyRecordMetadata.INSTANCE,
                        executionContext
                );
                if (!sampleByPeriod.isConstant() || (sampleByPeriod.getType() != LONG && sampleByPeriod.getType() != INT)) {
                    Misc.free(sampleByPeriod);
                    throw SqlException.$(sampleByNode.position, "sample by period must be a constant expression of INT or LONG type");
                }
                long period = sampleByPeriod.getLong(null);
                sampleByPeriod.close();
                timestampSampler = TimestampSamplerFactory.getInstance(timestampDriver, period, sampleByUnits.token, sampleByUnits.position);
            }

            keyTypes.clear();
            valueTypes.clear();
            listColumnFilterA.clear();

            if (fillCount == 1 && Chars.equalsLowerCaseAscii(sampleByFill.getQuick(0).token, "linear")) {
                valueTypes.add(BYTE); // gap flag

                final int columnCount = baseMetadata.getColumnCount();
                final ObjList<GroupByFunction> groupByFunctions = new ObjList<>(columnCount);
                tempOuterProjectionFunctions.clear();
                tempInnerProjectionFunctions.clear();
                final GenericRecordMetadata projectionMetadata = new GenericRecordMetadata();
                final IntList projectionFunctionFlags = new IntList(columnCount);

                GroupByUtils.assembleGroupByFunctions(
                        functionParser,
                        sqlNodeStack,
                        model,
                        executionContext,
                        baseMetadata,
                        timestampIndex,
                        false,
                        groupByFunctions,
                        groupByFunctionPositions,
                        tempOuterProjectionFunctions,
                        tempInnerProjectionFunctions,
                        recordFunctionPositions,
                        projectionFunctionFlags,
                        projectionMetadata,
                        valueTypes,
                        keyTypes,
                        listColumnFilterA,
                        sampleByFill,
                        validateSampleByFillType,
                        model.getColumns()
                );

                return new SampleByInterpolateRecordCursorFactory(
                        asm,
                        configuration,
                        factory,
                        projectionMetadata,
                        groupByFunctions,
                        new ObjList<>(tempOuterProjectionFunctions),
                        timestampSampler,
                        model,
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
                        offsetFuncPos
                );
            }

            valueTypes.add(timestampType); // first value is always timestamp

            final int columnCount = model.getColumns().size();
            final ObjList<GroupByFunction> groupByFunctions = new ObjList<>(columnCount);
            tempInnerProjectionFunctions.clear();
            final ObjList<Function> outerProjectionFunctions = new ObjList<>(columnCount);
            final GenericRecordMetadata projectionMetadata = new GenericRecordMetadata();
            final IntList projectionFunctionFlags = new IntList(columnCount);

            GroupByUtils.assembleGroupByFunctions(
                    functionParser,
                    sqlNodeStack,
                    model,
                    executionContext,
                    baseMetadata,
                    timestampIndex,
                    false,
                    groupByFunctions,
                    groupByFunctionPositions,
                    outerProjectionFunctions,
                    tempInnerProjectionFunctions,
                    recordFunctionPositions,
                    projectionFunctionFlags,
                    projectionMetadata,
                    valueTypes,
                    keyTypes,
                    listColumnFilterA,
                    sampleByFill,
                    validateSampleByFillType,
                    model.getColumns()
            );

            boolean isFillNone = fillCount == 0 || fillCount == 1 && Chars.equalsLowerCaseAscii(sampleByFill.getQuick(0).token, "none");
            boolean allGroupsFirstLast = isFillNone && allGroupsFirstLastWithSingleSymbolFilter(model, baseMetadata);
            if (allGroupsFirstLast) {
                SingleSymbolFilter symbolFilter = factory.convertToSampleByIndexPageFrameCursorFactory();
                if (symbolFilter != null) {
                    int symbolColIndex = getSampleBySymbolKeyIndex(model, baseMetadata);
                    if (symbolColIndex == -1 || symbolFilter.getColumnIndex() == symbolColIndex) {
                        return new SampleByFirstLastRecordCursorFactory(
                                configuration,
                                factory,
                                timestampSampler,
                                projectionMetadata,
                                model.getColumns(),
                                baseMetadata,
                                timezoneNameFunc,
                                timezoneNameFuncPos,
                                offsetFunc,
                                offsetFuncPos,
                                timestampIndex,
                                symbolFilter,
                                configuration.getSampleByIndexSearchPageSize(),
                                sampleFromFunc,
                                sampleFromFuncPos,
                                sampleToFunc,
                                sampleToFuncPos
                        );
                    }
                    factory.revertFromSampleByIndexPageFrameCursorFactory();
                }
            }

            if (fillCount == 1 && Chars.equalsLowerCaseAscii(sampleByFill.getQuick(0).token, "prev")) {
                if (keyTypes.getColumnCount() == 0) {
                    return new SampleByFillPrevNotKeyedRecordCursorFactory(
                            asm,
                            configuration,
                            factory,
                            timestampSampler,
                            projectionMetadata,
                            groupByFunctions,
                            outerProjectionFunctions,
                            timestampIndex,
                            timestampType,
                            valueTypes.getColumnCount(),
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

                guardAgainstFromToWithKeyedSampleBy(isFromTo);

                return new SampleByFillPrevRecordCursorFactory(
                        asm,
                        configuration,
                        factory,
                        timestampSampler,
                        listColumnFilterA,
                        keyTypes,
                        valueTypes,
                        projectionMetadata,
                        groupByFunctions,
                        outerProjectionFunctions,
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

            if (isFillNone) {
                if (keyTypes.getColumnCount() == 0) {
                    // this sample by is not keyed
                    return new SampleByFillNoneNotKeyedRecordCursorFactory(
                            asm,
                            configuration,
                            factory,
                            timestampSampler,
                            projectionMetadata,
                            groupByFunctions,
                            outerProjectionFunctions,
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

                guardAgainstFromToWithKeyedSampleBy(isFromTo);

                return new SampleByFillNoneRecordCursorFactory(
                        asm,
                        configuration,
                        factory,
                        projectionMetadata,
                        groupByFunctions,
                        outerProjectionFunctions,
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

            if (fillCount == 1 && isNullKeyword(sampleByFill.getQuick(0).token)) {
                if (keyTypes.getColumnCount() == 0) {
                    return new SampleByFillNullNotKeyedRecordCursorFactory(
                            asm,
                            configuration,
                            factory,
                            timestampSampler,
                            projectionMetadata,
                            groupByFunctions,
                            outerProjectionFunctions,
                            recordFunctionPositions,
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

                guardAgainstFromToWithKeyedSampleBy(isFromTo);

                return new SampleByFillNullRecordCursorFactory(
                        asm,
                        configuration,
                        factory,
                        timestampSampler,
                        listColumnFilterA,
                        keyTypes,
                        valueTypes,
                        projectionMetadata,
                        groupByFunctions,
                        outerProjectionFunctions,
                        recordFunctionPositions,
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
                return new SampleByFillValueNotKeyedRecordCursorFactory(
                        asm,
                        configuration,
                        factory,
                        timestampSampler,
                        sampleByFill,
                        projectionMetadata,
                        groupByFunctions,
                        outerProjectionFunctions,
                        recordFunctionPositions,
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

            guardAgainstFromToWithKeyedSampleBy(isFromTo);

            return new SampleByFillValueRecordCursorFactory(
                    asm,
                    configuration,
                    factory,
                    timestampSampler,
                    listColumnFilterA,
                    sampleByFill,
                    keyTypes,
                    valueTypes,
                    projectionMetadata,
                    groupByFunctions,
                    outerProjectionFunctions,
                    recordFunctionPositions,
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
        } catch (Throwable e) {
            Misc.free(factory);
            throw e;
        } finally {
            tempInnerProjectionFunctions.clear();
            tempOuterProjectionFunctions.clear();
        }
    }

    private RecordCursorFactory generateSelect(
            QueryModel model,
            SqlExecutionContext executionContext,
            boolean shouldProcessJoins
    ) throws SqlException {
        return switch (model.getSelectModelType()) {
            case SELECT_MODEL_CHOOSE -> generateSelectChoose(model, executionContext);
            case SELECT_MODEL_GROUP_BY -> generateSelectGroupBy(model, executionContext);
            case SELECT_MODEL_VIRTUAL -> generateSelectVirtual(model, executionContext);
            case SELECT_MODEL_WINDOW -> generateSelectWindow(model, executionContext);
            case SELECT_MODEL_WINDOW_JOIN -> generateSelectWindowJoin(model, executionContext);
            case SELECT_MODEL_DISTINCT -> generateSelectDistinct(model, executionContext);
            case SELECT_MODEL_CURSOR -> generateSelectCursor(model, executionContext);
            case SELECT_MODEL_SHOW -> model.getTableNameFunction();
            default -> shouldProcessJoins && model.getJoinModels().size() > 1
                    ? generateJoins(model, executionContext)
                    : generateNoSelect(model, executionContext);
        };
    }

    private RecordCursorFactory generateSelectChoose(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        boolean overrideTimestampRequired = model.hasExplicitTimestamp() && executionContext.isTimestampRequired();
        final RecordCursorFactory factory;
        try {
            // if model uses explicit timestamp (e.g. select * from X timestamp(ts))
            // then we shouldn't expect the inner models to produce one
            if (overrideTimestampRequired) {
                executionContext.pushTimestampRequiredFlag(false);
            }
            factory = generateSubQuery(model, executionContext);
        } finally {
            if (overrideTimestampRequired) {
                executionContext.popTimestampRequiredFlag();
            }
        }

        final RecordMetadata metadata = factory.getMetadata();
        final ObjList<QueryColumn> columns = model.getColumns();
        final int selectColumnCount = columns.size();
        final ExpressionNode timestamp = model.getTimestamp();

        // If this is update query and column types don't match exactly
        // to the column type of table to be updated we have to fall back to
        // select-virtual
        if (model.isUpdate()) {
            boolean columnTypeMismatch = false;
            ObjList<CharSequence> updateColumnNames = model.getUpdateTableColumnNames();
            IntList updateColumnTypes = model.getUpdateTableColumnTypes();

            for (int i = 0, n = columns.size(); i < n; i++) {
                QueryColumn queryColumn = columns.getQuick(i);
                CharSequence columnName = queryColumn.getAlias();
                int index = metadata.getColumnIndexQuiet(queryColumn.getAst().token);
                assert index > -1 : "wtf? " + queryColumn.getAst().token;

                int updateColumnIndex = updateColumnNames.indexOf(columnName);
                int updateColumnType = updateColumnTypes.get(updateColumnIndex);

                if (updateColumnType != metadata.getColumnType(index)) {
                    columnTypeMismatch = true;
                    break;
                }
            }

            if (columnTypeMismatch) {
                return generateSelectVirtualWithSubQuery(model, executionContext, factory);
            }
        }

        boolean entity;
        // the model is considered entity when it doesn't add any value to its nested model
        if (timestamp == null && metadata.getColumnCount() == selectColumnCount) {
            entity = true;
            for (int i = 0; i < selectColumnCount; i++) {
                QueryColumn qc = columns.getQuick(i);
                if (
                        !Chars.equals(metadata.getColumnName(i), qc.getAst().token) ||
                                (qc.getAlias() != null && !Chars.equals(qc.getAlias(), qc.getAst().token))
                ) {
                    entity = false;
                    break;
                }
            }
        } else {
            final int tsIndex = metadata.getTimestampIndex();
            entity = timestamp != null && tsIndex != -1 && Chars.equalsIgnoreCase(timestamp.token, metadata.getColumnName(tsIndex));
        }

        if (entity) {
            model.setSkipped(true);
            return factory;
        }

        // We require timestamp with asc order.
        final int timestampIndex;
        try {
            timestampIndex = getTimestampIndex(model, factory);
            if (executionContext.isTimestampRequired()) {
                if (timestampIndex == -1) {
                    throw SqlException.$(model.getModelPosition(), "TIMESTAMP column is required but not provided");
                }
                if (factory.getScanDirection() != RecordCursorFactory.SCAN_DIRECTION_FORWARD) {
                    throw SqlException.$(model.getModelPosition(), "ASC order over TIMESTAMP column is required but not provided");
                }
            }
        } catch (Throwable e) {
            Misc.free(factory);
            throw e;
        }

        CharSequence timestampName = null;
        int timestampNameDot = -1;
        if (timestampIndex != -1) {
            timestampName = factory.getMetadata().getColumnName(timestampIndex);
            timestampNameDot = Chars.indexOfLastUnquoted(timestampName, '.');
        }
        final CharSequence firstOrderByColumn = model.getOrderBy().size() > 0 && model.getOrderBy().getQuick(0).type == LITERAL
                ? model.getOrderBy().getQuick(0).token
                : null;

        final IntList columnCrossIndex = new IntList(selectColumnCount);
        final GenericRecordMetadata queryMetadata = new GenericRecordMetadata();
        boolean timestampSet = false;
        for (int i = 0; i < selectColumnCount; i++) {
            final QueryColumn queryColumn = columns.getQuick(i);
            int index = metadata.getColumnIndexQuiet(queryColumn.getAst().token);
            assert index > -1 : "wtf? " + queryColumn.getAst().token;
            columnCrossIndex.add(index);

            if (queryColumn.getAlias() == null) {
                queryMetadata.add(metadata.getColumnMetadata(index));
            } else {
                queryMetadata.add(
                        new TableColumnMetadata(
                                Chars.toString(queryColumn.getAlias()),
                                metadata.getColumnType(index),
                                metadata.isColumnIndexed(index),
                                metadata.getIndexValueBlockCapacity(index),
                                metadata.isSymbolTableStatic(index),
                                metadata.getMetadata(index)
                        )
                );
            }

            if (index == timestampIndex) {
                // Always prefer the column matching the first ORDER BY column as the designated
                // timestamp in case of multiple timestamp aliases, e.g. `select ts, ts as ts1, ...`.
                // That's to choose the optimal plan in generateOrderBy().
                // Otherwise, prefer columns with aliases matching the base column name, e.g.
                // prefer `t1.ts as ts` over `t1.ts as ts2`.
                if (Chars.equalsIgnoreCaseNc(queryColumn.getAlias(), firstOrderByColumn)
                        || Chars.equalsIgnoreCase(queryColumn.getAlias(), timestampName, timestampNameDot + 1, timestampName.length())
                        || !timestampSet) {
                    queryMetadata.setTimestampIndex(i);
                    timestampSet = true;
                }
            }
        }

        if (!timestampSet && executionContext.isTimestampRequired()) {
            TableColumnMetadata colMetadata = metadata.getColumnMetadata(timestampIndex);
            queryMetadata.add(
                    new TableColumnMetadata(
                            "", // implicitly added timestamp - should never be referenced by a user, we only need the timestamp index position
                            colMetadata.getColumnType(),
                            colMetadata.isSymbolIndexFlag(),
                            colMetadata.getIndexValueBlockCapacity(),
                            colMetadata.isSymbolTableStatic(),
                            metadata
                    )
            );
            queryMetadata.setTimestampIndex(queryMetadata.getColumnCount() - 1);
            columnCrossIndex.add(timestampIndex);
        }

        return new SelectedRecordCursorFactory(queryMetadata, columnCrossIndex, factory);
    }

    private RecordCursorFactory generateSelectCursor(
            @Transient QueryModel model,
            @Transient SqlExecutionContext executionContext
    ) throws SqlException {
        // sql parser ensures this type of model always has only one column
        return new RecordAsAFieldRecordCursorFactory(
                generate(model.getNestedModel(), executionContext),
                model.getColumns().getQuick(0).getAlias()
        );
    }

    private RecordCursorFactory generateSelectDistinct(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        final RecordCursorFactory factory = generateSubQuery(model, executionContext);
        try {
            if (factory.recordCursorSupportsRandomAccess() && factory.getMetadata().getTimestampIndex() != -1) {
                return new DistinctTimeSeriesRecordCursorFactory(
                        configuration,
                        factory,
                        entityColumnFilter,
                        asm
                );
            }

            final Function limitLoFunc;
            final Function limitHiFunc;
            if (model.getOrderBy().size() == 0) {
                limitLoFunc = getLoFunction(model, executionContext);
                limitHiFunc = getHiFunction(model, executionContext);
            } else {
                limitLoFunc = null;
                limitHiFunc = null;
            }

            return new DistinctRecordCursorFactory(
                    configuration,
                    factory,
                    entityColumnFilter,
                    asm,
                    limitLoFunc,
                    limitHiFunc
            );
        } catch (Throwable e) {
            factory.close();
            throw e;
        }
    }

    private RecordCursorFactory generateSelectGroupBy(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        final ExpressionNode sampleByNode = model.getSampleBy();
        if (sampleByNode != null) {
            return generateSampleBy(model, executionContext, sampleByNode, model.getSampleByUnit());
        }

        RecordCursorFactory factory = null;
        try {
            ObjList<QueryColumn> columns;
            ExpressionNode columnExpr;

            // generate special case plan for "select count() from somewhere"
            columns = model.getColumns();
            if (columns.size() == 1) {
                CharSequence columnName = columns.getQuick(0).getName();
                columnExpr = columns.getQuick(0).getAst();
                if (columnExpr.type == FUNCTION && columnExpr.paramCount == 0 && isCountKeyword(columnExpr.token)) {
                    // check if count() was not aliased, if it was, we need to generate new baseMetadata, bummer
                    final RecordMetadata metadata = isCountKeyword(columnName)
                            ? CountRecordCursorFactory.DEFAULT_COUNT_METADATA :
                            new GenericRecordMetadata().add(new TableColumnMetadata(Chars.toString(columnName), LONG));
                    return new CountRecordCursorFactory(metadata, generateSubQuery(model, executionContext));
                }
            }

            tempKeyIndexesInBase.clear();
            tempKeyIndex.clear();
            arrayColumnTypes.clear();
            tempKeyKinds.clear();

            boolean pageFramingSupported = false;

            QueryModel.backupWhereClause(expressionNodePool, model);

            final QueryModel nested = model.getNestedModel();
            assert nested != null;

            // check for special case time function aggregations
            // check if underlying model has reference to hour(column) function
            // condition - direct aggregation against table, e.g.
            // 1. there is "hour(timestamp)" function somewhere
            // 2. all other columns are functions that can be potentially resolved into known vector aggregate functions
            // we also need to collect the index of the "hour(timestamp)" call so that we do not try to make it
            // vector aggregate function and fail (or fallback to default impl)

            int hourIndex = -1; // assume "hour(timestamp) does not exist
            int timestampType = 0;
            for (int i = 0, n = columns.size(); i < n; i++) {
                QueryColumn qc = columns.getQuick(i);
                if (qc.getAst() == null || qc.getAst().type != FUNCTION) {
                    // tough luck, no special case
                    // reset hourIndex in case we found it before
                    hourIndex = -1;
                    break;

                }
                columnExpr = qc.getAst();

                if (isHourKeyword(columnExpr.token) && columnExpr.paramCount == 1 && columnExpr.rhs.type == LITERAL) {
                    // check the column type via aliasToColumnMap
                    QueryColumn tableColumn = nested.getAliasToColumnMap().get(columnExpr.rhs.token);
                    timestampType = tableColumn.getColumnType();
                    if (isTimestamp(timestampType)) {
                        hourIndex = i;
                    }
                }
            }

            if (hourIndex != -1) {
                factory = generateSubQuery(model, executionContext);
                pageFramingSupported = factory.supportsPageFrameCursor();
                if (pageFramingSupported) {
                    columnExpr = columns.getQuick(hourIndex).getAst();
                    // find position of the hour() argument in the factory meta
                    tempKeyIndexesInBase.add(factory.getMetadata().getColumnIndex(columnExpr.rhs.token));
                    tempKeyIndex.add(hourIndex);
                    // storage dimension for Rosti is INT when we use hour(). This function produces INT.
                    tempKeyKinds.add(getTimestampDriver(timestampType).getGKKHourInt());
                    arrayColumnTypes.add(INT);
                } else {
                    factory = Misc.free(factory);
                }
            }

            if (factory == null) {
                // we generated subquery for "hour" intrinsic, but that did not work out
                if (hourIndex != -1) {
                    QueryModel.restoreWhereClause(expressionNodePool, model);
                }
                factory = generateSubQuery(model, executionContext);
                pageFramingSupported = factory.supportsPageFrameCursor();
            }

            RecordMetadata baseMetadata = factory.getMetadata();

            boolean enableParallelGroupBy = executionContext.isParallelGroupByEnabled();
            // Inspect model for possibility of vector aggregate intrinsics.
            if (enableParallelGroupBy && pageFramingSupported && assembleKeysAndFunctionReferences(columns, baseMetadata, hourIndex)) {
                // Create baseMetadata from everything we've gathered.
                GenericRecordMetadata meta = new GenericRecordMetadata();

                // Start with keys.
                for (int i = 0, n = tempKeyIndex.size(); i < n; i++) {
                    final int indexInThis = tempKeyIndex.getQuick(i);
                    final int indexInBase = tempKeyIndexesInBase.getQuick(i);
                    final int type = arrayColumnTypes.getColumnType(i);

                    if (isSymbol(type)) {
                        meta.add(
                                indexInThis,
                                new TableColumnMetadata(
                                        Chars.toString(columns.getQuick(indexInThis).getName()),
                                        type,
                                        false,
                                        0,
                                        baseMetadata.isSymbolTableStatic(indexInBase),
                                        null
                                )
                        );
                    } else {
                        meta.add(
                                indexInThis,
                                new TableColumnMetadata(
                                        Chars.toString(columns.getQuick(indexInThis).getName()),
                                        type,
                                        null
                                )
                        );
                    }
                }

                // Add the aggregate functions.
                for (int i = 0, n = tempVecConstructors.size(); i < n; i++) {
                    VectorAggregateFunctionConstructor constructor = tempVecConstructors.getQuick(i);
                    int indexInBase = tempVecConstructorArgIndexes.getQuick(i);
                    int indexInThis = tempAggIndex.getQuick(i);
                    VectorAggregateFunction vaf = constructor.create(
                            tempKeyKinds.size() == 0 ? 0 : tempKeyKinds.getQuick(0),
                            indexInBase,
                            baseMetadata.getTimestampIndex(),
                            executionContext.getSharedQueryWorkerCount()
                    );
                    tempVaf.add(vaf);
                    meta.add(
                            indexInThis,
                            new TableColumnMetadata(
                                    Chars.toString(columns.getQuick(indexInThis).getName()),
                                    vaf.getType(),
                                    null
                            )
                    );
                }

                if (tempKeyIndexesInBase.size() == 0) {
                    // vectorized non-keyed tasks are lightweight, so it's fine to use larger frame sizes
                    factory.changePageFrameSizes(
                            Math.min(2 * configuration.getSqlPageFrameMinRows(), configuration.getSqlPageFrameMaxRows()),
                            configuration.getSqlPageFrameMaxRows()
                    );
                    return new GroupByNotKeyedVectorRecordCursorFactory(
                            executionContext.getCairoEngine(),
                            configuration,
                            factory,
                            meta,
                            executionContext.getSharedQueryWorkerCount(),
                            tempVaf
                    );
                }

                if (tempKeyIndexesInBase.size() == 1) {
                    for (int i = 0, n = tempVaf.size(); i < n; i++) {
                        tempVaf.getQuick(i).pushValueTypes(arrayColumnTypes);
                    }

                    if (tempVaf.size() == 0) { // similar to DistinctKeyRecordCursorFactory, handles e.g. select id from tab group by id
                        int keyKind = hourIndex != -1 ? getTimestampDriver(timestampType).getGKKHourInt() : SqlCodeGenerator.GKK_VANILLA_INT;
                        CountVectorAggregateFunction countFunction = new CountVectorAggregateFunction(keyKind);
                        countFunction.pushValueTypes(arrayColumnTypes);
                        tempVaf.add(countFunction);

                        tempSymbolSkewIndexes.clear();
                        tempSymbolSkewIndexes.add(0);
                    }

                    try {
                        GroupByUtils.validateGroupByColumns(sqlNodeStack, model, 1);
                    } catch (Throwable e) {
                        Misc.freeObjList(tempVaf);
                        throw e;
                    }

                    guardAgainstFillWithKeyedGroupBy(model, keyTypes);

                    return generateFill(
                            model,
                            new GroupByRecordCursorFactory(
                                    executionContext.getCairoEngine(),
                                    configuration,
                                    factory,
                                    meta,
                                    arrayColumnTypes,
                                    executionContext.getSharedQueryWorkerCount(),
                                    tempVaf,
                                    tempKeyIndexesInBase.getQuick(0),
                                    tempKeyIndex.getQuick(0),
                                    tempSymbolSkewIndexes
                            ),
                            executionContext
                    );
                }

                // Free the vector aggregate functions since we didn't use them.
                Misc.freeObjList(tempVaf);
            }

            if (hourIndex != -1) {
                // uh-oh, we had special case keys, but could not find implementation for the functions
                // release factory we created unnecessarily
                factory = Misc.free(factory);
                // create factory on top level model
                QueryModel.restoreWhereClause(expressionNodePool, model);
                factory = generateSubQuery(model, executionContext);
                // and reset baseMetadata
                baseMetadata = factory.getMetadata();
            }

            final int timestampIndex = getTimestampIndex(model, factory);

            keyTypes.clear();
            valueTypes.clear();
            listColumnFilterA.clear();

            final int columnCount = model.getColumns().size();
            final ObjList<GroupByFunction> groupByFunctions = new ObjList<>(columnCount);
            tempInnerProjectionFunctions.clear();
            tempOuterProjectionFunctions.clear();
            final GenericRecordMetadata outerProjectionMetadata = new GenericRecordMetadata();
            final IntList projectionFunctionFlags = new IntList(columnCount);

            GroupByUtils.assembleGroupByFunctions(
                    functionParser,
                    sqlNodeStack,
                    model,
                    executionContext,
                    baseMetadata,
                    timestampIndex,
                    true,
                    groupByFunctions,
                    groupByFunctionPositions,
                    tempOuterProjectionFunctions,
                    tempInnerProjectionFunctions,
                    recordFunctionPositions,
                    projectionFunctionFlags,
                    outerProjectionMetadata,
                    valueTypes,
                    keyTypes,
                    listColumnFilterA,
                    null,
                    validateSampleByFillType,
                    model.getColumns()
            );

            // Check if we have a non-keyed query with all early exit aggregate functions (e.g. count_distinct(symbol))
            // and no filter. In such a case, use single-threaded factories instead of the multithreaded ones.
            if (
                    enableParallelGroupBy
                            && keyTypes.getColumnCount() == 0
                            && GroupByUtils.isEarlyExitSupported(groupByFunctions)
                            && factory.getFilter() == null
            ) {
                enableParallelGroupBy = false;
            }

            ObjList<Function> keyFunctions = extractVirtualFunctionsFromProjection(tempInnerProjectionFunctions, projectionFunctionFlags);
            if (
                    enableParallelGroupBy
                            && SqlUtil.isParallelismSupported(keyFunctions)
                            && GroupByUtils.isParallelismSupported(groupByFunctions)
            ) {
                boolean supportsParallelism = factory.supportsPageFrameCursor();
                CompiledFilter compiledFilter = null;
                MemoryCARW bindVarMemory = null;
                ObjList<Function> bindVarFunctions = null;
                Function filter = null;
                ExpressionNode filterExpr = null;
                IntHashSet filterUsedColumnIndexes = null;
                // Try to steal the filter from the nested factory, if possible.
                // We aim for simple cases such as select key, avg(value) from t where value > 0
                if (!supportsParallelism && factory.supportsFilterStealing()) {
                    RecordCursorFactory filterFactory = factory;
                    factory = factory.getBaseFactory();
                    assert factory.supportsPageFrameCursor();
                    compiledFilter = filterFactory.getCompiledFilter();
                    bindVarMemory = filterFactory.getBindVarMemory();
                    bindVarFunctions = filterFactory.getBindVarFunctions();
                    filter = filterFactory.getFilter();
                    supportsParallelism = true;
                    filterExpr = filterFactory.getStealFilterExpr();
                    filterUsedColumnIndexes = new IntHashSet();
                    collectColumnIndexes(sqlNodeStack, factory.getMetadata(), filterExpr, filterUsedColumnIndexes);
                    filterFactory.halfClose();
                }

                if (supportsParallelism) {
                    QueryModel.restoreWhereClause(expressionNodePool, model);

                    // back up required lists as generateSubQuery or compileWorkerFilterConditionally may overwrite them
                    ArrayColumnTypes keyTypesCopy = new ArrayColumnTypes().addAll(keyTypes);
                    ArrayColumnTypes valueTypesCopy = new ArrayColumnTypes().addAll(valueTypes);
                    ListColumnFilter listColumnFilterCopy = listColumnFilterA.copy();

                    if (keyTypesCopy.getColumnCount() == 0) {
                        assert keyFunctions.size() == 0;
                        assert tempOuterProjectionFunctions.size() == groupByFunctions.size();

                        return new AsyncGroupByNotKeyedRecordCursorFactory(
                                executionContext.getCairoEngine(),
                                asm,
                                configuration,
                                executionContext.getMessageBus(),
                                factory,
                                outerProjectionMetadata,
                                groupByFunctions,
                                compileWorkerGroupByFunctionsConditionally(
                                        executionContext,
                                        model,
                                        groupByFunctions,
                                        executionContext.getSharedQueryWorkerCount(),
                                        factory.getMetadata()
                                ),
                                valueTypesCopy.getColumnCount(),
                                compiledFilter,
                                bindVarMemory,
                                bindVarFunctions,
                                filter,
                                filterUsedColumnIndexes,
                                compileWorkerFilterConditionally(
                                        executionContext,
                                        filter,
                                        executionContext.getSharedQueryWorkerCount(),
                                        filterExpr,
                                        factory.getMetadata()
                                ),
                                executionContext.getSharedQueryWorkerCount()
                        );
                    }

                    guardAgainstFillWithKeyedGroupBy(model, keyTypes);

                    ObjList<ObjList<Function>> perWorkerInnerProjectionFunctions = compilePerWorkerInnerProjectionFunctions(
                            executionContext,
                            model.getColumns(),
                            tempInnerProjectionFunctions,
                            executionContext.getSharedQueryWorkerCount(),
                            baseMetadata
                    );

                    return generateFill(
                            model,
                            new AsyncGroupByRecordCursorFactory(
                                    executionContext.getCairoEngine(),
                                    asm,
                                    configuration,
                                    executionContext.getMessageBus(),
                                    factory,
                                    outerProjectionMetadata,
                                    listColumnFilterCopy,
                                    keyTypesCopy,
                                    valueTypesCopy,
                                    groupByFunctions,
                                    extractWorkerFunctionsConditionally(
                                            tempInnerProjectionFunctions,
                                            projectionFunctionFlags,
                                            perWorkerInnerProjectionFunctions,
                                            GroupByUtils.PROJECTION_FUNCTION_FLAG_GROUP_BY
                                    ),
                                    keyFunctions,
                                    extractWorkerFunctionsConditionally(
                                            tempInnerProjectionFunctions,
                                            projectionFunctionFlags,
                                            perWorkerInnerProjectionFunctions,
                                            GroupByUtils.PROJECTION_FUNCTION_FLAG_VIRTUAL
                                    ),
                                    new ObjList<>(tempOuterProjectionFunctions),
                                    compiledFilter,
                                    bindVarMemory,
                                    bindVarFunctions,
                                    filter,
                                    filterUsedColumnIndexes,
                                    compileWorkerFilterConditionally(
                                            executionContext,
                                            filter,
                                            executionContext.getSharedQueryWorkerCount(),
                                            filterExpr,
                                            factory.getMetadata()
                                    ),
                                    executionContext.getSharedQueryWorkerCount()
                            ),
                            executionContext
                    );
                }
            }

            if (keyTypes.getColumnCount() == 0) {
                assert tempOuterProjectionFunctions.size() == groupByFunctions.size();
                return new GroupByNotKeyedRecordCursorFactory(
                        asm,
                        configuration,
                        factory,
                        outerProjectionMetadata,
                        groupByFunctions,
                        valueTypes.getColumnCount()
                );
            }

            guardAgainstFillWithKeyedGroupBy(model, keyTypes);

            return generateFill(
                    model,
                    new io.questdb.griffin.engine.groupby.GroupByRecordCursorFactory(
                            asm,
                            configuration,
                            factory,
                            listColumnFilterA,
                            keyTypes,
                            valueTypes,
                            outerProjectionMetadata,
                            groupByFunctions,
                            keyFunctions,
                            new ObjList<>(tempOuterProjectionFunctions)
                    ),
                    executionContext
            );
        } catch (Throwable e) {
            Misc.free(factory);
            throw e;
        }
    }

    private RecordCursorFactory generateSelectVirtual(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        final RecordCursorFactory factory = generateSubQuery(model, executionContext);
        return generateSelectVirtualWithSubQuery(model, executionContext, factory);
    }

    @NotNull
    private VirtualRecordCursorFactory generateSelectVirtualWithSubQuery(
            QueryModel model,
            SqlExecutionContext executionContext,
            RecordCursorFactory factory
    ) throws SqlException {
        final ObjList<QueryColumn> columns = model.getColumns();
        final int columnCount = columns.size();
        final ObjList<Function> functions = new ObjList<>(columnCount);
        final RecordMetadata baseMetadata = factory.getMetadata();
        // Lookup metadata will resolve column references, prioritising references to the projection
        // over the references to the base table. +1 accounts for timestamp, which can be added conditionally later.
        final int virtualColumnReservedSlots = columnCount + 1;
        final PriorityMetadata priorityMetadata = new PriorityMetadata(virtualColumnReservedSlots, baseMetadata);
        final GenericRecordMetadata virtualMetadata = new GenericRecordMetadata();
        try {
            // attempt to preserve timestamp on new data set
            CharSequence timestampColumn;
            final int timestampIndex = baseMetadata.getTimestampIndex();
            if (timestampIndex > -1) {
                timestampColumn = baseMetadata.getColumnName(timestampIndex);
            } else {
                timestampColumn = null;
            }

            // Check if the model has a detected timestamp offset - if so, use that column as timestamp
            int modelTimestampIndex = model.getTimestampColumnIndex();

            for (int i = 0; i < columnCount; i++) {
                final QueryColumn column = columns.getQuick(i);
                final ExpressionNode node = column.getAst();
                if (modelTimestampIndex == i) {
                    // Model explicitly indicates this column should be the timestamp
                    virtualMetadata.setTimestampIndex(i);
                } else if (modelTimestampIndex < 0 && node.type == LITERAL && Chars.equalsNc(node.token, timestampColumn)) {
                    // Only use literal match if model hasn't specified a timestamp index
                    virtualMetadata.setTimestampIndex(i);
                }

                Function function = functionParser.parseFunction(
                        column.getAst(),
                        priorityMetadata,
                        executionContext
                );

                int targetColumnType = -1;
                if (model.isUpdate()) {
                    // Check the type of the column to be updated
                    int columnIndex = model.getUpdateTableColumnNames().indexOf(column.getAlias());
                    targetColumnType = model.getUpdateTableColumnTypes().get(columnIndex);
                }

                // define "undefined" functions as string unless it's update.
                if (model.isUpdate()) {
                    if (isUndefined(function.getType())) {
                        function.assignType(targetColumnType, executionContext.getBindVariableService());
                    }
                } else if (function.isUndefined()) {
                    function.assignType(STRING, executionContext.getBindVariableService());
                }

                int columnType = function.getType();
                if (columnType == CURSOR) {
                    throw SqlException.$(node.position, "cursor function cannot be used as a column [column=").put(column.getAlias()).put(']');
                }

                if (targetColumnType != -1 && targetColumnType != columnType) {
                    // This is an update and the target column does not match with column the update is trying to perform
                    if (isBuiltInWideningCast(function.getType(), targetColumnType)) {
                        // All functions will be able to getLong() if they support getInt(), no need to generate cast here
                        columnType = targetColumnType;
                    } else {
                        Function castFunction = functionParser.createImplicitCast(column.getAst().position, function, targetColumnType);
                        if (castFunction != null) {
                            function = castFunction;
                            columnType = targetColumnType;
                        }
                        // else - update code will throw incompatibility exception. It will have better chance close resources then
                    }
                }

                functions.add(function);
                TableColumnMetadata m = null;
                if (columnType == SYMBOL) {
                    if (function instanceof SymbolFunction) {
                        m = new TableColumnMetadata(
                                Chars.toString(column.getAlias()),
                                function.getType(),
                                false,
                                0,
                                ((SymbolFunction) function).isSymbolTableStatic(),
                                function.getMetadata()
                        );
                    } else if (function instanceof NullConstant) {
                        m = new TableColumnMetadata(
                                Chars.toString(column.getAlias()),
                                SYMBOL,
                                false,
                                0,
                                false,
                                function.getMetadata()
                        );
                        // Replace with symbol null constant
                        functions.setQuick(functions.size() - 1, SymbolConstant.NULL);
                    }
                } else if (columnType == TIMESTAMP && (function.getType() == STRING || function.getType() == VARCHAR)) {
                    m = new TableColumnMetadata(
                            Chars.toString(column.getAlias()),
                            function.getType(),
                            false,
                            0,
                            false,
                            function.getMetadata()
                    );
                } else {
                    m = new TableColumnMetadata(
                            Chars.toString(column.getAlias()),
                            columnType,
                            function.getMetadata()
                    );
                }
                assert m != null;
                virtualMetadata.add(m);
                priorityMetadata.add(m);
            }

            // if timestamp was required and present in the base model but
            // not selected, we will need to add it
            if (
                    executionContext.isTimestampRequired()
                            && timestampColumn != null
                            && virtualMetadata.getTimestampIndex() == -1
            ) {
                final Function timestampFunction = FunctionParser.createColumn(
                        0,
                        timestampColumn,
                        priorityMetadata
                );
                functions.add(timestampFunction);

                // here the base timestamp column name can name-clash with one of the
                // functions, so we have to use bottomUpColumns to lookup alias we should
                // be using. Bottom up column should have our timestamp because optimiser puts it there

                for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
                    QueryColumn qc = model.getBottomUpColumns().getQuick(i);
                    if (qc.getAst().type == LITERAL && Chars.equals(timestampColumn, qc.getAst().token)) {
                        virtualMetadata.setTimestampIndex(virtualMetadata.getColumnCount());
                        TableColumnMetadata m;
                        m = new TableColumnMetadata(
                                Chars.toString(qc.getAlias()),
                                timestampFunction.getType(),
                                timestampFunction.getMetadata()
                        );
                        virtualMetadata.add(m);
                        priorityMetadata.add(m);
                        break;
                    }
                }
            }

            if (ALLOW_FUNCTION_MEMOIZATION) {
                for (int i = 0, n = columns.size(); i < n; i++) {
                    Function function = functions.getQuick(i);
                    if (function != null && !function.isConstant()
                            && (model.getRefCount(columns.getQuick(i).getAlias()) > 1 || function.shouldMemoize())) {
                        switch (ColumnType.tagOf(function.getType())) {
                            case ColumnType.LONG:
                                functions.set(i, new LongFunctionMemoizer(function));
                                break;
                            case ColumnType.INT:
                                functions.set(i, new IntFunctionMemoizer(function));
                                break;
                            case ColumnType.TIMESTAMP:
                                functions.set(i, new TimestampFunctionMemoizer(function));
                                break;
                            case ColumnType.DOUBLE:
                                functions.set(i, new DoubleFunctionMemoizer(function));
                                break;
                            case ColumnType.SHORT:
                                functions.set(i, new ShortFunctionMemoizer(function));
                                break;
                            case ColumnType.BOOLEAN:
                                functions.set(i, new BooleanFunctionMemoizer(function));
                                break;
                            case ColumnType.BYTE:
                                functions.set(i, new ByteFunctionMemoizer(function));
                                break;
                            case ColumnType.CHAR:
                                functions.set(i, new CharFunctionMemoizer(function));
                                break;
                            case ColumnType.DATE:
                                functions.set(i, new DateFunctionMemoizer(function));
                                break;
                            case ColumnType.FLOAT:
                                functions.set(i, new FloatFunctionMemoizer(function));
                                break;
                            case ColumnType.IPv4:
                                functions.set(i, new IPv4FunctionMemoizer(function));
                                break;
                            case ColumnType.UUID:
                                functions.set(i, new UuidFunctionMemoizer(function));
                                break;
                            case ColumnType.LONG256:
                                functions.set(i, new Long256FunctionMemoizer(function));
                                break;
                            case ColumnType.ARRAY:
                                functions.set(i, new ArrayFunctionMemoizer(function));
                                break;
                            case ColumnType.STRING:
                                functions.set(i, new StrFunctionMemoizer(function));
                                break;
                            case ColumnType.VARCHAR:
                                functions.set(i, new VarcharFunctionMemoizer(function));
                                break;
                            case ColumnType.SYMBOL:
                                functions.set(i, new SymbolFunctionMemoizer(function));
                                break;
                            // other types do not have memoization yet
                        }
                    }
                }
            }

            return new VirtualRecordCursorFactory(
                    virtualMetadata,
                    priorityMetadata,
                    functions,
                    factory,
                    virtualColumnReservedSlots
            );
        } catch (SqlException | CairoException e) {
            Misc.freeObjList(functions);
            factory.close();
            throw e;
        }
    }

    private RecordCursorFactory generateSelectWindow(
            QueryModel model,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final RecordCursorFactory base = generateSubQuery(model, executionContext);
        final RecordMetadata baseMetadata = base.getMetadata();
        final ObjList<QueryColumn> columns = model.getColumns();
        final int columnCount = columns.size();
        groupedWindow.clear();

        valueTypes.clear();
        ArrayColumnTypes chainTypes = valueTypes;
        GenericRecordMetadata chainMetadata = new GenericRecordMetadata();
        GenericRecordMetadata factoryMetadata = new GenericRecordMetadata();

        ObjList<Function> functions = new ObjList<>();
        ObjList<WindowFunction> naturalOrderFunctions = null;
        ObjList<Function> partitionByFunctions = null;
        try {
            // if all window function don't require sorting or more than one pass then use streaming factory
            boolean isFastPath = true;

            for (int i = 0; i < columnCount; i++) {
                final QueryColumn qc = columns.getQuick(i);
                if (qc.isWindowExpression()) {
                    final WindowExpression ac = (WindowExpression) qc;
                    final ExpressionNode ast = qc.getAst();

                    partitionByFunctions = null;
                    int psz = ac.getPartitionBy().size();
                    if (psz > 0) {
                        partitionByFunctions = new ObjList<>(psz);
                        for (int j = 0; j < psz; j++) {
                            final Function function = functionParser.parseFunction(ac.getPartitionBy().getQuick(j), baseMetadata, executionContext);
                            partitionByFunctions.add(function);
                            if (function instanceof GroupByFunction) {
                                throw SqlException.$(ast.position, "aggregate functions in partition by are not supported");
                            }
                        }
                    }

                    final VirtualRecord partitionByRecord;
                    final RecordSink partitionBySink;

                    if (partitionByFunctions != null) {
                        partitionByRecord = new VirtualRecord(partitionByFunctions);
                        keyTypes.clear();
                        final int partitionByCount = partitionByFunctions.size();

                        for (int j = 0; j < partitionByCount; j++) {
                            keyTypes.add(partitionByFunctions.getQuick(j).getType());
                        }
                        entityColumnFilter.of(partitionByCount);
                        partitionBySink = RecordSinkFactory.getInstance(asm, keyTypes, entityColumnFilter, configuration);
                    } else {
                        partitionByRecord = null;
                        partitionBySink = null;
                    }

                    final int osz = ac.getOrderBy().size();

                    // analyze order by clause on the current model and optimise out
                    // order by on window function if it matches the one on the model
                    final LowerCaseCharSequenceIntHashMap orderHash = model.getOrderHash();
                    boolean dismissOrder = false;
                    int timestampIdx = base.getMetadata().getTimestampIndex();
                    int orderByPos = osz > 0 ? ac.getOrderBy().getQuick(0).position : -1;

                    if (base.followedOrderByAdvice() && osz > 0 && orderHash.size() > 0) {
                        dismissOrder = true;
                        for (int j = 0; j < osz; j++) {
                            ExpressionNode node = ac.getOrderBy().getQuick(j);
                            int direction = ac.getOrderByDirection().getQuick(j);
                            if (!Chars.equalsIgnoreCase(node.token, orderHash.keys().get(j)) ||
                                    orderHash.get(node.token) != direction) {
                                dismissOrder = false;
                                break;
                            }
                        }
                    }
                    if (!dismissOrder && osz == 1 && timestampIdx != -1 && orderHash.size() < 2) {
                        ExpressionNode orderByNode = ac.getOrderBy().getQuick(0);
                        int orderByDirection = ac.getOrderByDirection().getQuick(0);

                        if (baseMetadata.getColumnIndexQuiet(orderByNode.token) == timestampIdx &&
                                ((orderByDirection == ORDER_ASC && base.getScanDirection() == RecordCursorFactory.SCAN_DIRECTION_FORWARD) ||
                                        (orderByDirection == ORDER_DESC && base.getScanDirection() == RecordCursorFactory.SCAN_DIRECTION_BACKWARD))) {
                            dismissOrder = true;
                        }
                    }

                    executionContext.configureWindowContext(
                            partitionByRecord,
                            partitionBySink,
                            keyTypes,
                            osz > 0,
                            dismissOrder ? base.getScanDirection() : RecordCursorFactory.SCAN_DIRECTION_OTHER,
                            orderByPos,
                            base.recordCursorSupportsRandomAccess(),
                            ac.getFramingMode(),
                            ac.getRowsLo(),
                            ac.getRowsLoExprTimeUnit(),
                            ac.getRowsLoKindPos(),
                            ac.getRowsHi(),
                            ac.getRowsHiExprTimeUnit(),
                            ac.getRowsHiKindPos(),
                            ac.getExclusionKind(),
                            ac.getExclusionKindPos(),
                            baseMetadata.getTimestampIndex(),
                            baseMetadata.getTimestampType(),
                            ac.isIgnoreNulls(),
                            ac.getNullsDescPos()
                    );
                    final Function f;
                    try {
                        f = functionParser.parseFunction(ast, baseMetadata, executionContext);
                        if (!(f instanceof WindowFunction af)) {
                            Misc.free(f);
                            throw SqlException.$(ast.position, "non-window function called in window context");
                        }

                        functions.extendAndSet(i, f);

                        // sorting and/or multiple passes are required, so fall back to old implementation
                        if ((osz > 0 && !dismissOrder) || af.getPassCount() != WindowFunction.ZERO_PASS) {
                            isFastPath = false;
                            break;
                        }
                    } finally {
                        executionContext.clearWindowContext();
                    }

                    WindowFunction windowFunction = (WindowFunction) f;
                    windowFunction.setColumnIndex(i);

                    factoryMetadata.add(new TableColumnMetadata(
                            Chars.toString(qc.getAlias()),
                            windowFunction.getType(),
                            false,
                            0,
                            false,
                            null
                    ));
                } else { // column
                    final int columnIndex = baseMetadata.getColumnIndexQuiet(qc.getAst().token);
                    final TableColumnMetadata m = baseMetadata.getColumnMetadata(columnIndex);

                    Function function = functionParser.parseFunction(
                            qc.getAst(),
                            baseMetadata,
                            executionContext
                    );
                    functions.extendAndSet(i, function);

                    if (baseMetadata.getTimestampIndex() != -1 && baseMetadata.getTimestampIndex() == columnIndex) {
                        factoryMetadata.setTimestampIndex(i);
                    }

                    if (Chars.equalsIgnoreCase(qc.getAst().token, qc.getAlias())) {
                        factoryMetadata.add(i, m);
                    } else { // keep alias
                        factoryMetadata.add(i, new TableColumnMetadata(
                                        Chars.toString(qc.getAlias()),
                                        m.getColumnType(),
                                        m.isSymbolIndexFlag(),
                                        m.getIndexValueBlockCapacity(),
                                        m.isSymbolTableStatic(),
                                        baseMetadata
                                )
                        );
                    }
                }
            }

            if (isFastPath) {
                for (int i = 0, size = functions.size(); i < size; i++) {
                    Function func = functions.getQuick(i);
                    if (func instanceof WindowFunction) {
                        WindowExpression qc = (WindowExpression) columns.getQuick(i);
                        if (qc.getOrderBy().size() > 0) {
                            chainTypes.clear();
                            ((WindowFunction) func).initRecordComparator(this, baseMetadata, chainTypes, null,
                                    qc.getOrderBy(), qc.getOrderByDirection());
                        }
                    }
                }
                return new WindowRecordCursorFactory(base, factoryMetadata, functions);
            } else {
                factoryMetadata.clear();
                Misc.freeObjListAndClear(functions);
            }

            listColumnFilterA.clear();
            listColumnFilterB.clear();

            // we need two passes over columns because partitionBy and orderBy clauses of
            // the window function must reference the metadata of "this" factory.

            // pass #1 assembles metadata of non-window columns

            // set of column indexes in the base metadata that has already been added to the main
            // metadata instance
            intHashSet.clear();
            final IntList columnIndexes = new IntList();
            for (int i = 0; i < columnCount; i++) {
                final QueryColumn qc = columns.getQuick(i);
                if (!qc.isWindowExpression()) {
                    final int columnIndex = baseMetadata.getColumnIndexQuiet(qc.getAst().token);
                    final TableColumnMetadata m = baseMetadata.getColumnMetadata(columnIndex);
                    chainMetadata.addIfNotExists(i, m);
                    if (Chars.equalsIgnoreCase(qc.getAst().token, qc.getAlias())) {
                        factoryMetadata.add(i, m);
                    } else { // keep alias
                        factoryMetadata.add(i, new TableColumnMetadata(
                                        Chars.toString(qc.getAlias()),
                                        m.getColumnType(),
                                        m.isSymbolIndexFlag(),
                                        m.getIndexValueBlockCapacity(),
                                        m.isSymbolTableStatic(),
                                        baseMetadata
                                )
                        );
                    }
                    chainTypes.add(i, m.getColumnType());
                    listColumnFilterA.extendAndSet(i, i + 1);
                    listColumnFilterB.extendAndSet(i, columnIndex);
                    intHashSet.add(columnIndex);
                    columnIndexes.extendAndSet(i, columnIndex);

                    if (baseMetadata.getTimestampIndex() != -1 && baseMetadata.getTimestampIndex() == columnIndex) {
                        factoryMetadata.setTimestampIndex(i);
                    }
                }
            }

            // pass #2 - add remaining base metadata column that are not in intHashSet already
            // we need to pay attention to stepping over window column slots
            // Chain metadata is assembled in such way that all columns the factory
            // needs to provide are at the beginning of the metadata so the record the factory cursor
            // returns can be chain record, because the chain record is always longer than record needed out of the
            // cursor and relevant columns are 0..n limited by factory metadata

            int addAt = columnCount;
            for (int i = 0, n = baseMetadata.getColumnCount(); i < n; i++) {
                if (intHashSet.excludes(i)) {
                    final TableColumnMetadata m = baseMetadata.getColumnMetadata(i);
                    chainMetadata.add(addAt, m);
                    chainTypes.add(addAt, m.getColumnType());
                    listColumnFilterA.extendAndSet(addAt, addAt + 1);
                    listColumnFilterB.extendAndSet(addAt, i);
                    columnIndexes.extendAndSet(addAt, i);
                    addAt++;
                }
            }

            // pass #3 assembles window column metadata into a list
            // not main metadata to avoid partitionBy functions accidentally looking up
            // window columns recursively

            deferredWindowMetadata.clear();
            for (int i = 0; i < columnCount; i++) {
                final QueryColumn qc = columns.getQuick(i);
                if (qc.isWindowExpression()) {
                    final WindowExpression ac = (WindowExpression) qc;
                    final ExpressionNode ast = qc.getAst();

                    partitionByFunctions = null;
                    int psz = ac.getPartitionBy().size();
                    if (psz > 0) {
                        partitionByFunctions = new ObjList<>(psz);
                        for (int j = 0; j < psz; j++) {
                            final Function function = functionParser.parseFunction(ac.getPartitionBy().getQuick(j), chainMetadata, executionContext);
                            partitionByFunctions.add(function);
                            if (function instanceof GroupByFunction) {
                                throw SqlException.$(ast.position, "aggregate functions in partition by are not supported");
                            }
                        }
                    }

                    final VirtualRecord partitionByRecord;
                    final RecordSink partitionBySink;

                    if (partitionByFunctions != null) {
                        partitionByRecord = new VirtualRecord(partitionByFunctions);
                        keyTypes.clear();
                        final int partitionByCount = partitionByFunctions.size();

                        for (int j = 0; j < partitionByCount; j++) {
                            keyTypes.add(partitionByFunctions.getQuick(j).getType());
                        }
                        entityColumnFilter.of(partitionByCount);
                        // create sink
                        partitionBySink = RecordSinkFactory.getInstance(asm, keyTypes, entityColumnFilter, configuration);
                    } else {
                        partitionByRecord = null;
                        partitionBySink = null;
                    }

                    final int osz = ac.getOrderBy().size();

                    // analyze order by clause on the current model and optimise out
                    // order by on window function if it matches the one on the model
                    final LowerCaseCharSequenceIntHashMap orderHash = model.getOrderHash();
                    boolean dismissOrder = false;
                    int timestampIdx = base.getMetadata().getTimestampIndex();
                    int orderByPos = osz > 0 ? ac.getOrderBy().getQuick(0).position : -1;

                    if (base.followedOrderByAdvice() && osz > 0 && orderHash.size() > 0) {
                        dismissOrder = true;
                        for (int j = 0; j < osz; j++) {
                            ExpressionNode node = ac.getOrderBy().getQuick(j);
                            int direction = ac.getOrderByDirection().getQuick(j);
                            if (!Chars.equalsIgnoreCase(node.token, orderHash.keys().get(j))
                                    || orderHash.get(node.token) != direction) {
                                dismissOrder = false;
                                break;
                            }
                        }
                    }
                    if (osz == 1 && timestampIdx != -1 && orderHash.size() < 2) {
                        ExpressionNode orderByNode = ac.getOrderBy().getQuick(0);
                        int orderByDirection = ac.getOrderByDirection().getQuick(0);

                        if (baseMetadata.getColumnIndexQuiet(orderByNode.token) == timestampIdx
                                && ((orderByDirection == ORDER_ASC && base.getScanDirection() == RecordCursorFactory.SCAN_DIRECTION_FORWARD)
                                || (orderByDirection == ORDER_DESC && base.getScanDirection() == RecordCursorFactory.SCAN_DIRECTION_BACKWARD))) {
                            dismissOrder = true;
                        }
                    }

                    executionContext.configureWindowContext(
                            partitionByRecord,
                            partitionBySink,
                            keyTypes,
                            osz > 0,
                            dismissOrder ? base.getScanDirection() : RecordCursorFactory.SCAN_DIRECTION_OTHER,
                            orderByPos,
                            base.recordCursorSupportsRandomAccess(),
                            ac.getFramingMode(),
                            ac.getRowsLo(),
                            ac.getRowsLoExprTimeUnit(),
                            ac.getRowsLoKindPos(),
                            ac.getRowsHi(),
                            ac.getRowsHiExprTimeUnit(),
                            ac.getRowsHiKindPos(),
                            ac.getExclusionKind(),
                            ac.getExclusionKindPos(),
                            baseMetadata.getTimestampIndex(),
                            baseMetadata.getTimestampType(),
                            ac.isIgnoreNulls(),
                            ac.getNullsDescPos()
                    );
                    final Function f;
                    try {
                        // function needs to resolve args against chain metadata
                        f = functionParser.parseFunction(ast, chainMetadata, executionContext);
                        if (!(f instanceof WindowFunction)) {
                            Misc.free(f);
                            throw SqlException.$(ast.position, "non-window function called in window context");
                        }
                    } finally {
                        executionContext.clearWindowContext();
                    }

                    WindowFunction windowFunction = (WindowFunction) f;

                    if (osz > 0 && !dismissOrder) {
                        IntList directions = ac.getOrderByDirection();
                        if (windowFunction.getPass1ScanDirection() == WindowFunction.Pass1ScanDirection.BACKWARD) {
                            for (int j = 0, size = directions.size(); j < size; j++) {
                                directions.set(j, 1 - directions.getQuick(j));
                            }
                        }

                        IntList order = toOrderIndices(chainMetadata, ac.getOrderBy(), ac.getOrderByDirection());
                        // init comparator if we need
                        windowFunction.initRecordComparator(this, chainMetadata, chainTypes, order, null, null);
                        ObjList<WindowFunction> funcs = groupedWindow.get(order);
                        if (funcs == null) {
                            groupedWindow.put(order, funcs = new ObjList<>());
                        }
                        funcs.add(windowFunction);
                    } else {
                        if (osz > 0) {
                            windowFunction.initRecordComparator(this, chainMetadata, chainTypes, null, ac.getOrderBy(), ac.getOrderByDirection());
                        }

                        if (naturalOrderFunctions == null) {
                            naturalOrderFunctions = new ObjList<>();
                        }
                        naturalOrderFunctions.add(windowFunction);
                    }

                    windowFunction.setColumnIndex(i);

                    deferredWindowMetadata.extendAndSet(i, new TableColumnMetadata(
                            Chars.toString(qc.getAlias()),
                            windowFunction.getType(),
                            false,
                            0,
                            false,
                            null
                    ));

                    listColumnFilterA.extendAndSet(i, -i - 1);
                }
            }

            // after all columns are processed we can re-insert deferred metadata
            for (int i = 0, n = deferredWindowMetadata.size(); i < n; i++) {
                TableColumnMetadata m = deferredWindowMetadata.getQuick(i);
                if (m != null) {
                    chainTypes.add(i, m.getColumnType());
                    factoryMetadata.add(i, m);
                }
            }

            final ObjList<RecordComparator> windowComparators = new ObjList<>(groupedWindow.size());
            final ObjList<ObjList<WindowFunction>> functionGroups = new ObjList<>(groupedWindow.size());
            final ObjList<IntList> keys = new ObjList<>();
            for (ObjObjHashMap.Entry<IntList, ObjList<WindowFunction>> e : groupedWindow) {
                windowComparators.add(recordComparatorCompiler.newInstance(chainTypes, e.key));
                functionGroups.add(e.value);
                keys.add(e.key);
            }

            final RecordSink recordSink = RecordSinkFactory.getInstance(
                    asm,
                    chainTypes,
                    listColumnFilterA,
                    null,
                    listColumnFilterB,
                    null,
                    null,
                    configuration);

            return new CachedWindowRecordCursorFactory(
                    configuration,
                    base,
                    recordSink,
                    factoryMetadata,
                    chainTypes,
                    windowComparators,
                    functionGroups,
                    naturalOrderFunctions,
                    columnIndexes,
                    keys,
                    chainMetadata
            );
        } catch (Throwable th) {
            for (ObjObjHashMap.Entry<IntList, ObjList<WindowFunction>> e : groupedWindow) {
                Misc.freeObjList(e.value);
            }
            Misc.free(base);
            Misc.freeObjList(functions);
            Misc.freeObjList(naturalOrderFunctions);
            Misc.freeObjList(partitionByFunctions);
            throw th;
        }
    }

    private RecordCursorFactory generateSelectWindowJoin(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        QueryModel child = model.getNestedModel();
        if (!isWindowJoin(child)) {
            throw SqlException.$(0, "expected window join model");
        }
        return generate(child, executionContext);
    }

    /**
     * Generates chain of parent factories each of which takes only two argument factories.
     * Parent factory will perform one of SET operations on its arguments, such as UNION, UNION ALL,
     * INTERSECT or EXCEPT
     *
     * @param model            incoming model is expected to have a chain of models via its QueryModel.getUnionModel() function
     * @param factoryA         is compiled first argument
     * @param executionContext execution context for authorization and parallel execution purposes
     * @return factory that performs a SET operation
     * @throws SqlException when query contains syntax errors
     */
    private RecordCursorFactory generateSetFactory(
            QueryModel model,
            RecordCursorFactory factoryA,
            SqlExecutionContext executionContext
    ) throws SqlException {
        RecordCursorFactory factoryB = null;
        ObjList<Function> castFunctionsA = null;
        ObjList<Function> castFunctionsB = null;
        try {
            factoryB = generateQuery0(model.getUnionModel(), executionContext, true);

            final RecordMetadata metadataA = factoryA.getMetadata();
            final RecordMetadata metadataB = factoryB.getMetadata();
            final int positionA = model.getModelPosition();
            final int positionB = model.getUnionModel().getModelPosition();

            switch (model.getSetOperationType()) {
                case SET_OPERATION_UNION: {
                    final boolean castIsRequired = checkIfSetCastIsRequired(metadataA, metadataB, true);
                    final RecordMetadata unionMetadata = castIsRequired ? widenSetMetadata(metadataA, metadataB) : GenericRecordMetadata.removeTimestamp(metadataA);
                    if (castIsRequired) {
                        castFunctionsA = generateCastFunctions(executionContext, unionMetadata, metadataA, positionA);
                        castFunctionsB = generateCastFunctions(executionContext, unionMetadata, metadataB, positionB);
                    }

                    return generateUnionFactory(
                            model,
                            executionContext,
                            factoryA,
                            factoryB,
                            castFunctionsA,
                            castFunctionsB,
                            unionMetadata,
                            SET_UNION_CONSTRUCTOR
                    );
                }
                case SET_OPERATION_UNION_ALL: {
                    final boolean castIsRequired = checkIfSetCastIsRequired(metadataA, metadataB, true);
                    final RecordMetadata unionMetadata = castIsRequired ? widenSetMetadata(metadataA, metadataB) : GenericRecordMetadata.removeTimestamp(metadataA);
                    if (castIsRequired) {
                        castFunctionsA = generateCastFunctions(executionContext, unionMetadata, metadataA, positionA);
                        castFunctionsB = generateCastFunctions(executionContext, unionMetadata, metadataB, positionB);
                    }

                    return generateUnionAllFactory(
                            model,
                            executionContext,
                            factoryA,
                            factoryB,
                            castFunctionsA,
                            castFunctionsB,
                            unionMetadata
                    );
                }
                case SET_OPERATION_EXCEPT: {
                    final boolean castIsRequired = checkIfSetCastIsRequired(metadataA, metadataB, false);
                    final RecordMetadata unionMetadata = castIsRequired ? widenSetMetadata(metadataA, metadataB) : metadataA;
                    if (castIsRequired) {
                        castFunctionsA = generateCastFunctions(executionContext, unionMetadata, metadataA, positionA);
                        castFunctionsB = generateCastFunctions(executionContext, unionMetadata, metadataB, positionB);
                    }

                    return generateUnionFactory(
                            model,
                            executionContext,
                            factoryA,
                            factoryB,
                            castFunctionsA,
                            castFunctionsB,
                            unionMetadata,
                            SET_EXCEPT_CONSTRUCTOR
                    );
                }
                case SET_OPERATION_EXCEPT_ALL: {
                    final boolean castIsRequired = checkIfSetCastIsRequired(metadataA, metadataB, false);
                    final RecordMetadata unionMetadata = castIsRequired ? widenSetMetadata(metadataA, metadataB) : metadataA;
                    if (castIsRequired) {
                        castFunctionsA = generateCastFunctions(executionContext, unionMetadata, metadataA, positionA);
                        castFunctionsB = generateCastFunctions(executionContext, unionMetadata, metadataB, positionB);
                    }

                    return generateIntersectOrExceptAllFactory(
                            model,
                            executionContext,
                            factoryA,
                            factoryB,
                            castFunctionsA,
                            castFunctionsB,
                            unionMetadata,
                            SET_EXCEPT_ALL_CONSTRUCTOR
                    );
                }
                case SET_OPERATION_INTERSECT: {
                    final boolean castIsRequired = checkIfSetCastIsRequired(metadataA, metadataB, false);
                    final RecordMetadata unionMetadata = castIsRequired ? widenSetMetadata(metadataA, metadataB) : metadataA;
                    if (castIsRequired) {
                        castFunctionsA = generateCastFunctions(executionContext, unionMetadata, metadataA, positionA);
                        castFunctionsB = generateCastFunctions(executionContext, unionMetadata, metadataB, positionB);
                    }

                    return generateUnionFactory(
                            model,
                            executionContext,
                            factoryA,
                            factoryB,
                            castFunctionsA,
                            castFunctionsB,
                            unionMetadata,
                            SET_INTERSECT_CONSTRUCTOR
                    );
                }
                case SET_OPERATION_INTERSECT_ALL: {
                    final boolean castIsRequired = checkIfSetCastIsRequired(metadataA, metadataB, false);
                    final RecordMetadata unionMetadata = castIsRequired ? widenSetMetadata(metadataA, metadataB) : metadataA;
                    if (castIsRequired) {
                        castFunctionsA = generateCastFunctions(executionContext, unionMetadata, metadataA, positionA);
                        castFunctionsB = generateCastFunctions(executionContext, unionMetadata, metadataB, positionB);
                    }

                    return generateIntersectOrExceptAllFactory(
                            model,
                            executionContext,
                            factoryA,
                            factoryB,
                            castFunctionsA,
                            castFunctionsB,
                            unionMetadata,
                            SET_INTERSECT_ALL_CONSTRUCTOR
                    );
                }
                default:
                    assert false;
                    return null;
            }
        } catch (Throwable e) {
            Misc.free(factoryA);
            Misc.free(factoryB);
            Misc.freeObjList(castFunctionsA);
            Misc.freeObjList(castFunctionsB);
            throw e;
        }
    }

    private RecordCursorFactory generateSubQuery(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        assert model.getNestedModel() != null;
        return generateQuery(model.getNestedModel(), executionContext, true);
    }

    private RecordCursorFactory generateTableQuery(
            QueryModel model,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final ObjList<ExpressionNode> latestBy = model.getLatestBy();

        final boolean supportsRandomAccess;
        CharSequence tableName = model.getTableName();
        if (Chars.startsWith(tableName, NO_ROWID_MARKER)) {
            final BufferWindowCharSequence tab = (BufferWindowCharSequence) tableName;
            tab.shiftLo(NO_ROWID_MARKER.length());
            supportsRandomAccess = false;
        } else {
            supportsRandomAccess = true;
        }

        final TableToken tableToken = executionContext.getTableToken(tableName);
        if (model.isUpdate() && !executionContext.isWalApplication() && executionContext.getCairoEngine().isWalTable(tableToken)) {
            // two phase update execution, this is client-side branch. It has to execute against the sequencer metadata
            // to allow the client to succeed even if WAL apply does not run.
            try (TableRecordMetadata metadata = executionContext.getMetadataForWrite(tableToken, model.getMetadataVersion())) {
                // it is not enough to rely on execution context to be different for WAL APPLY;
                // in WAL APPLY we also must supply reader, outside of WAL APPLY reader is null
                return generateTableQuery0(model, executionContext, latestBy, supportsRandomAccess, null, metadata);
            }
        } else {
            // this is server side execution of the update. It executes against the reader metadata, which by now
            // has to be fully up-to-date due to WAL apply execution order.
            try (TableReader reader = executionContext.getReader(tableToken, model.getMetadataVersion())) {
                return generateTableQuery0(model, executionContext, latestBy, supportsRandomAccess, reader, reader.getMetadata());
            }
        }
    }

    private RecordCursorFactory generateTableQuery0(
            @Transient QueryModel model,
            @Transient SqlExecutionContext executionContext,
            ObjList<ExpressionNode> latestBy,
            boolean supportsRandomAccess,
            @Transient @Nullable TableReader reader,
            @Transient TableRecordMetadata metadata
    ) throws SqlException {
        // create metadata based on top-down columns that are required

        final IntList columnIndexes = new IntList();
        final IntList columnSizeShifts = new IntList();
        int readerTimestampIndex = getTimestampIndex(model, metadata);
        // Latest by on a table requires the provided timestamp column to be the designated timestamp.
        if (latestBy.size() > 0 && readerTimestampIndex != metadata.getTimestampIndex()) {
            throw SqlException.$(model.getTimestamp().position, "latest by over a table requires designated TIMESTAMP");
        }

        boolean requiresTimestamp = joinsRequiringTimestamp[model.getJoinType()];
        final GenericRecordMetadata queryMeta = buildQueryMetadata(
                model,
                executionContext,
                metadata,
                readerTimestampIndex,
                requiresTimestamp,
                columnIndexes,
                columnSizeShifts
        );

        if (reader == null) {
            // This is WAL serialisation compilation. We don't need to read data from table
            // and don't need optimisation for query validation.
            return new EmptyTableRecordCursorFactory(queryMeta, metadata.getTableToken());
        }

        GenericRecordMetadata dfcFactoryMeta = GenericRecordMetadata.copyOfNew(metadata);
        final int latestByColumnCount = prepareLatestByColumnIndexes(latestBy, queryMeta);
        final TableToken tableToken = metadata.getTableToken();
        ExpressionNode withinExtracted;

        if (latestByColumnCount > 0 && configuration.useWithinLatestByOptimisation()) {
            withinExtracted = whereClauseParser.extractWithin(
                    model,
                    model.getWhereClause(),
                    queryMeta,
                    functionParser,
                    executionContext,
                    prefixes
            );

            boolean allSymbolsAreIndexed = true;
            if (prefixes.size() > 0) {
                for (int i = 0; i < latestByColumnCount; i++) {
                    int idx = listColumnFilterA.getColumnIndexFactored(i);
                    if (!isSymbol(queryMeta.getColumnType(idx)) || !queryMeta.isColumnIndexed(idx)) {
                        allSymbolsAreIndexed = false;
                    }
                }
            }

            if (allSymbolsAreIndexed) {
                model.setWhereClause(withinExtracted);
            }
        }

        int hasInterval = -1;
        RuntimeIntrinsicIntervalModel pushedIntervalModel = null;
        boolean inJoin = model.getJoinModels().size() > 0 || model.getJoinType() != JOIN_NONE;
        if (inJoin) {
            pushedIntervalModel = executionContext.peekIntervalModel();
            hasInterval = executionContext.hasInterval();
        }

        ExpressionNode viewExpr = model.getViewNameExpr();
        ExpressionNode whereClause = model.getWhereClause();
        if (whereClause != null || executionContext.isOverriddenIntrinsics(reader.getTableToken()) || pushedIntervalModel != null) {
            final IntrinsicModel intrinsicModel;
            if (whereClause != null) {
                CharSequence preferredKeyColumn = null;
                if (latestByColumnCount == 1) {
                    final int latestByIndex = listColumnFilterA.getColumnIndexFactored(0);
                    if (isSymbol(queryMeta.getColumnType(latestByIndex))) {
                        preferredKeyColumn = latestBy.getQuick(0).token;
                    }
                }

                intrinsicModel = whereClauseParser.extract(
                        model,
                        expressionNodePool,
                        whereClause,
                        metadata,
                        preferredKeyColumn,
                        metadata.getTimestampIndex(),
                        functionParser,
                        queryMeta,
                        executionContext,
                        latestByColumnCount > 1,
                        reader
                );
            } else {
                intrinsicModel = whereClauseParser.getEmpty(
                        reader.getMetadata().getTimestampType(),
                        reader.getPartitionedBy(),
                        executionContext.getCairoEngine().getConfiguration()
                );
            }

            // When we run materialized view refresh we want to restrict queries to the base table
            // to the timestamp range that is updated by the previous transactions.
            executionContext.overrideWhereIntrinsics(reader.getTableToken(), intrinsicModel, reader.getMetadata().getTimestampType());

            // TODO: In theory, we can apply similar optimizations for ASOF, SPLICE and LT joins
            if (model.getJoinType() == JOIN_WINDOW && pushedIntervalModel != null) {
                WindowJoinContext windowJoinContext = model.getWindowJoinContext();
                TimestampDriver driver = ColumnType.getTimestampDriver(reader.getMetadata().getTimestampType());
                long hi = windowJoinContext.getHi();
                if (windowJoinContext.getHiExprTimeUnit() != 0) {
                    hi = driver.from(hi, windowJoinContext.getHiExprTimeUnit());
                }
                long lo;
                if (windowJoinContext.isIncludePrevailing()) {
                    lo = Numbers.LONG_NULL;
                } else {
                    lo = windowJoinContext.getLo();
                    if (windowJoinContext.getLoExprTimeUnit() != 0) {
                        lo = driver.from(lo, windowJoinContext.getLoExprTimeUnit());
                    }
                }
                intrinsicModel.mergeIntervalModel((RuntimeIntervalModel) pushedIntervalModel, lo, hi);
            }

            // intrinsic parser can collapse where clause when removing parts it can replace
            // need to make sure that filter is updated on the model in case it is processed up the call stack
            //
            // At this juncture filter can use used up by one of the implementations below.
            // We will clear it preemptively. If nothing picks filter up we will set model "where"
            // to the downsized filter
            model.setWhereClause(null);

            if (intrinsicModel.intrinsicValue == IntrinsicModel.FALSE) {
                return new EmptyTableRecordCursorFactory(queryMeta);
            }

            PartitionFrameCursorFactory dfcFactory;

            if (latestByColumnCount > 0) {
                Function filter = compileFilter(intrinsicModel, queryMeta, executionContext);
                if (filter != null && filter.isConstant() && !filter.getBool(null)) {
                    // 'latest by' clause takes over the latest by nodes, so that the later generateLatestBy() is no-op
                    model.getLatestBy().clear();
                    Misc.free(filter);
                    return new EmptyTableRecordCursorFactory(queryMeta);
                }

                // a sub-query present in the filter may have used the latest by
                // column index lists, so we need to regenerate them
                prepareLatestByColumnIndexes(latestBy, queryMeta);

                return generateLatestByTableQuery(
                        model,
                        reader,
                        queryMeta,
                        tableToken,
                        intrinsicModel,
                        filter,
                        executionContext,
                        metadata.getTimestampIndex(),
                        columnIndexes,
                        columnSizeShifts,
                        prefixes,
                        hasInterval
                );
            }

            // below code block generates index-based filter
            final boolean intervalHitsOnlyOnePartition;
            final int order = model.isForceBackwardScan() ? ORDER_DESC : ORDER_ASC;

            if (intrinsicModel.hasIntervalFilters()) {
                RuntimeIntrinsicIntervalModel intervalModel = intrinsicModel.buildIntervalModel();
                if (hasInterval == 0) {
                    executionContext.popIntervalModel();
                    executionContext.pushIntervalModel(intervalModel);
                }
                dfcFactory = new IntervalPartitionFrameCursorFactory(
                        tableToken,
                        model.getMetadataVersion(),
                        intervalModel,
                        metadata.getTimestampIndex(),
                        dfcFactoryMeta,
                        order,
                        getViewName(viewExpr),
                        getViewPosition(viewExpr),
                        model.isUpdate()
                );
                intervalHitsOnlyOnePartition = intervalModel.allIntervalsHitOnePartition();
            } else {
                dfcFactory = new FullPartitionFrameCursorFactory(
                        tableToken,
                        model.getMetadataVersion(),
                        dfcFactoryMeta,
                        order,
                        getViewName(viewExpr),
                        getViewPosition(viewExpr),
                        model.isUpdate()
                );
                intervalHitsOnlyOnePartition = reader.getPartitionedBy() == PartitionBy.NONE;
            }

            if (intrinsicModel.keyColumn != null) {
                // existence of column would have been already validated
                final int keyColumnIndex = queryMeta.getColumnIndexQuiet(intrinsicModel.keyColumn);
                final int nKeyValues = intrinsicModel.keyValueFuncs.size();
                final int nKeyExcludedValues = intrinsicModel.keyExcludedValueFuncs.size();

                if (intrinsicModel.keySubQuery != null) {
                    RecordCursorFactory rcf = null;
                    final Record.CharSequenceFunction func;
                    Function filter;
                    try {
                        rcf = generate(intrinsicModel.keySubQuery, executionContext);
                        func = validateSubQueryColumnAndGetGetter(intrinsicModel, rcf.getMetadata());
                        filter = compileFilter(intrinsicModel, queryMeta, executionContext);
                    } catch (Throwable th) {
                        Misc.free(dfcFactory);
                        Misc.free(rcf);
                        throw th;
                    }

                    if (filter != null && filter.isConstant() && !filter.getBool(null)) {
                        Misc.free(dfcFactory);
                        return new EmptyTableRecordCursorFactory(queryMeta);
                    }
                    return new FilterOnSubQueryRecordCursorFactory(
                            configuration,
                            queryMeta,
                            dfcFactory,
                            rcf,
                            keyColumnIndex,
                            filter,
                            func,
                            columnIndexes,
                            columnSizeShifts
                    );
                }
                assert nKeyValues > 0 || nKeyExcludedValues > 0;

                boolean orderByKeyColumn = false;
                int indexDirection = BitmapIndexReader.DIR_FORWARD;
                if (intervalHitsOnlyOnePartition) {
                    final ObjList<ExpressionNode> orderByAdvice = model.getOrderByAdvice();
                    final int orderByAdviceSize = orderByAdvice.size();
                    if (orderByAdviceSize > 0 && orderByAdviceSize < 3) {
                        guardAgainstDotsInOrderByAdvice(model);
                        // todo: when order by coincides with keyColumn and there is index we can incorporate
                        //    ordering in the code that returns rows from index rather than having an
                        //    "overhead" order by implementation, which would be trying to oder already ordered symbols
                        if (Chars.equals(orderByAdvice.getQuick(0).token, intrinsicModel.keyColumn)) {
                            queryMeta.setTimestampIndex(-1);
                            if (orderByAdviceSize == 1) {
                                orderByKeyColumn = true;
                            } else if (Chars.equals(orderByAdvice.getQuick(1).token, model.getTimestamp().token)) {
                                orderByKeyColumn = true;
                                if (getOrderByDirectionOrDefault(model, 1) == ORDER_DIRECTION_DESCENDING) {
                                    indexDirection = BitmapIndexReader.DIR_BACKWARD;
                                }
                            }
                        }
                    }
                }
                boolean orderByTimestamp = false;
                // we can use skip sorting by timestamp if we:
                // - query index with a single value or
                // - query index with multiple values but use table order with forward scan (heap row cursor factory doesn't support backward scan)
                // it doesn't matter if we hit one or more partitions
                if (!orderByKeyColumn && isOrderByDesignatedTimestampOnly(model)) {
                    int orderByDirection = getOrderByDirectionOrDefault(model, 0);
                    if (nKeyValues == 1 || (nKeyValues > 1 && orderByDirection == ORDER_DIRECTION_ASCENDING)) {
                        orderByTimestamp = true;

                        if (orderByDirection == ORDER_DIRECTION_DESCENDING) {
                            indexDirection = BitmapIndexReader.DIR_BACKWARD;
                        }
                    } else if (nKeyExcludedValues > 0 && orderByDirection == ORDER_DIRECTION_ASCENDING) {
                        orderByTimestamp = true;
                    }
                }

                if (nKeyExcludedValues == 0) {
                    Function filter;
                    try {
                        filter = compileFilter(intrinsicModel, queryMeta, executionContext);
                    } catch (Throwable th) {
                        Misc.free(dfcFactory);
                        throw th;
                    }
                    if (filter != null && filter.isConstant()) {
                        try {
                            if (!filter.getBool(null)) {
                                Misc.free(dfcFactory);
                                return new EmptyTableRecordCursorFactory(queryMeta);
                            }
                        } finally {
                            filter = Misc.free(filter);
                        }
                    }

                    if (nKeyValues == 1) {
                        final RowCursorFactory rcf;
                        final Function symbolFunc = intrinsicModel.keyValueFuncs.get(0);
                        final SymbolMapReader symbolMapReader = reader.getSymbolMapReader(columnIndexes.getQuick(keyColumnIndex));
                        final int symbolKey = symbolFunc.isRuntimeConstant()
                                ? SymbolTable.VALUE_NOT_FOUND
                                : symbolMapReader.keyOf(symbolFunc.getStrA(null));

                        if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                            if (filter == null) {
                                rcf = new DeferredSymbolIndexRowCursorFactory(
                                        keyColumnIndex,
                                        symbolFunc,
                                        true,
                                        indexDirection
                                );
                            } else {
                                rcf = new DeferredSymbolIndexFilteredRowCursorFactory(
                                        keyColumnIndex,
                                        symbolFunc,
                                        filter,
                                        true,
                                        indexDirection
                                );
                            }
                        } else {
                            if (filter == null) {
                                rcf = new SymbolIndexRowCursorFactory(
                                        keyColumnIndex,
                                        symbolKey,
                                        true,
                                        indexDirection,
                                        null
                                );
                            } else {
                                rcf = new SymbolIndexFilteredRowCursorFactory(
                                        keyColumnIndex,
                                        symbolKey,
                                        filter,
                                        true,
                                        indexDirection,
                                        null
                                );
                            }
                        }

                        if (filter == null) {
                            // This special case factory can later be disassembled to framing and index
                            // cursors in SAMPLE BY processing
                            return new DeferredSingleSymbolFilterPageFrameRecordCursorFactory(
                                    configuration,
                                    keyColumnIndex,
                                    symbolFunc,
                                    rcf,
                                    queryMeta,
                                    dfcFactory,
                                    orderByKeyColumn || orderByTimestamp,
                                    columnIndexes,
                                    columnSizeShifts,
                                    supportsRandomAccess
                            );
                        }
                        return new PageFrameRecordCursorFactory(
                                configuration,
                                queryMeta,
                                dfcFactory,
                                rcf,
                                orderByKeyColumn || orderByTimestamp,
                                filter,
                                false,
                                columnIndexes,
                                columnSizeShifts,
                                supportsRandomAccess,
                                false
                        );
                    }

                    if (orderByKeyColumn) {
                        queryMeta.setTimestampIndex(-1);
                    }

                    return new FilterOnValuesRecordCursorFactory(
                            configuration,
                            queryMeta,
                            dfcFactory,
                            intrinsicModel.keyValueFuncs,
                            keyColumnIndex,
                            reader,
                            filter,
                            model.getOrderByAdviceMnemonic(),
                            orderByKeyColumn,
                            orderByTimestamp,
                            getOrderByDirectionOrDefault(model, 0),
                            indexDirection,
                            columnIndexes,
                            columnSizeShifts
                    );
                } else if (nKeyExcludedValues > 0) {
                    if (reader.getSymbolMapReader(columnIndexes.getQuick(keyColumnIndex)).getSymbolCount() < configuration.getMaxSymbolNotEqualsCount()) {
                        Function filter;
                        try {
                            filter = compileFilter(intrinsicModel, queryMeta, executionContext);
                        } catch (Throwable th) {
                            Misc.free(dfcFactory);
                            throw th;
                        }
                        if (filter != null && filter.isConstant()) {
                            try {
                                if (!filter.getBool(null)) {
                                    Misc.free(dfcFactory);
                                    return new EmptyTableRecordCursorFactory(queryMeta);
                                }
                            } finally {
                                filter = Misc.free(filter);
                            }
                        }

                        return new FilterOnExcludedValuesRecordCursorFactory(
                                configuration,
                                queryMeta,
                                dfcFactory,
                                intrinsicModel.keyExcludedValueFuncs,
                                keyColumnIndex,
                                filter,
                                model.getOrderByAdviceMnemonic(),
                                orderByKeyColumn,
                                orderByTimestamp,
                                getOrderByDirectionOrDefault(model, 0),
                                indexDirection,
                                columnIndexes,
                                columnSizeShifts,
                                configuration.getMaxSymbolNotEqualsCount()
                        );
                    } else if (intrinsicModel.keyExcludedNodes.size() > 0) {
                        // restore filter
                        ExpressionNode root = intrinsicModel.keyExcludedNodes.getQuick(0);

                        for (int i = 1, n = intrinsicModel.keyExcludedNodes.size(); i < n; i++) {
                            ExpressionNode expression = intrinsicModel.keyExcludedNodes.getQuick(i);

                            OperatorExpression andOp = OperatorExpression.chooseRegistry(configuration.getCairoSqlLegacyOperatorPrecedence()).getOperatorDefinition("and");
                            ExpressionNode newRoot = expressionNodePool.next().of(OPERATION, andOp.operator.token, andOp.precedence, 0);
                            newRoot.paramCount = 2;
                            newRoot.lhs = expression;
                            newRoot.rhs = root;

                            root = newRoot;
                        }

                        if (intrinsicModel.filter == null) {
                            intrinsicModel.filter = root;
                        } else {
                            OperatorExpression andOp = OperatorExpression.chooseRegistry(configuration.getCairoSqlLegacyOperatorPrecedence()).getOperatorDefinition("and");
                            ExpressionNode filter = expressionNodePool.next().of(OPERATION, andOp.operator.token, andOp.precedence, 0);
                            filter.paramCount = 2;
                            filter.lhs = intrinsicModel.filter;
                            filter.rhs = root;
                            intrinsicModel.filter = filter;
                        }
                    }
                }
            }

            if (intervalHitsOnlyOnePartition && intrinsicModel.filter == null) {
                final ObjList<ExpressionNode> orderByAdvice = model.getOrderByAdvice();
                final int orderByAdviceSize = orderByAdvice.size();
                if (orderByAdviceSize > 0 && orderByAdviceSize < 3 && intrinsicModel.hasIntervalFilters()) {
                    // This function cannot handle dotted aliases
                    guardAgainstDotsInOrderByAdvice(model);

                    // we can only deal with 'order by symbol, timestamp' at best
                    // skip this optimisation if order by is more extensive
                    final int columnIndex = queryMeta.getColumnIndexQuiet(model.getOrderByAdvice().getQuick(0).token);
                    assert columnIndex > -1;

                    // this is our kind of column
                    if (queryMeta.isColumnIndexed(columnIndex)) {
                        boolean orderByKeyColumn = false;
                        int indexDirection = BitmapIndexReader.DIR_FORWARD;
                        if (orderByAdviceSize == 1) {
                            orderByKeyColumn = true;
                        } else if (Chars.equals(orderByAdvice.getQuick(1).token, model.getTimestamp().token)) {
                            orderByKeyColumn = true;
                            if (getOrderByDirectionOrDefault(model, 1) == ORDER_DIRECTION_DESCENDING) {
                                indexDirection = BitmapIndexReader.DIR_BACKWARD;
                            }
                        }

                        if (orderByKeyColumn) {
                            // check that intrinsicModel.intervals hit only one partition
                            queryMeta.setTimestampIndex(-1);
                            return new SortedSymbolIndexRecordCursorFactory(
                                    configuration,
                                    queryMeta,
                                    dfcFactory,
                                    columnIndex,
                                    getOrderByDirectionOrDefault(model, 0) == ORDER_DIRECTION_ASCENDING,
                                    indexDirection,
                                    columnIndexes,
                                    columnSizeShifts
                            );
                        }
                    }
                }
            }

            final RowCursorFactory rowFactory = new PageFrameRowCursorFactory(model.isForceBackwardScan() ? ORDER_DESC : ORDER_ASC);

            model.setWhereClause(intrinsicModel.filter);
            return new PageFrameRecordCursorFactory(
                    configuration,
                    queryMeta,
                    dfcFactory,
                    rowFactory,
                    false,
                    null,
                    true,
                    columnIndexes,
                    columnSizeShifts,
                    supportsRandomAccess,
                    false
            );
        }

        // no where clause
        if (latestByColumnCount == 0) {
            // construct new metadata, which is a copy of what we constructed just above, but
            // in the interest of isolating problems we will only affect this factory

            final int order = model.isForceBackwardScan() ? ORDER_DESC : ORDER_ASC;

            PartitionFrameCursorFactory cursorFactory = new FullPartitionFrameCursorFactory(
                    tableToken,
                    model.getMetadataVersion(),
                    dfcFactoryMeta,
                    order,
                    getViewName(viewExpr),
                    getViewPosition(viewExpr),
                    model.isUpdate()
            );
            RowCursorFactory rowCursorFactory = new PageFrameRowCursorFactory(order);

            return new PageFrameRecordCursorFactory(
                    configuration,
                    queryMeta,
                    cursorFactory,
                    rowCursorFactory,
                    model.isOrderDescendingByDesignatedTimestampOnly(),
                    null,
                    true,
                    columnIndexes,
                    columnSizeShifts,
                    supportsRandomAccess,
                    false
            );
        }

        // 'latest by' clause takes over the latest by nodes, so that the later generateLatestBy() is no-op
        model.getLatestBy().clear();

        // listColumnFilterA = latest by column indexes
        if (latestByColumnCount == 1) {
            int latestByColumnIndex = listColumnFilterA.getColumnIndexFactored(0);
            if (queryMeta.isColumnIndexed(latestByColumnIndex)) {
                return new LatestByAllIndexedRecordCursorFactory(
                        executionContext.getCairoEngine(),
                        configuration,
                        queryMeta,
                        new FullPartitionFrameCursorFactory(
                                tableToken,
                                model.getMetadataVersion(),
                                dfcFactoryMeta,
                                ORDER_DESC,
                                getViewName(viewExpr),
                                getViewPosition(viewExpr),
                                model.isUpdate()
                        ),
                        listColumnFilterA.getColumnIndexFactored(0),
                        columnIndexes,
                        columnSizeShifts,
                        prefixes
                );
            }

            if (isSymbol(queryMeta.getColumnType(latestByColumnIndex))
                    && queryMeta.isSymbolTableStatic(latestByColumnIndex)) {
                // we have "latest by" symbol column values, but no index
                return new LatestByDeferredListValuesFilteredRecordCursorFactory(
                        configuration,
                        queryMeta,
                        new FullPartitionFrameCursorFactory(tableToken,
                                model.getMetadataVersion(),
                                dfcFactoryMeta,
                                ORDER_DESC,
                                getViewName(viewExpr),
                                getViewPosition(viewExpr),
                                model.isUpdate()
                        ),
                        latestByColumnIndex,
                        null,
                        columnIndexes,
                        columnSizeShifts
                );
            }
        }

        boolean symbolKeysOnly = true;
        for (int i = 0, n = keyTypes.getColumnCount(); i < n; i++) {
            symbolKeysOnly &= isSymbol(keyTypes.getColumnType(i));
        }
        if (symbolKeysOnly) {
            IntList partitionByColumnIndexes = new IntList(listColumnFilterA.size());
            for (int i = 0, n = listColumnFilterA.size(); i < n; i++) {
                partitionByColumnIndexes.add(listColumnFilterA.getColumnIndexFactored(i));
            }
            return new LatestByAllSymbolsFilteredRecordCursorFactory(
                    configuration,
                    queryMeta,
                    new FullPartitionFrameCursorFactory(
                            tableToken,
                            model.getMetadataVersion(),
                            dfcFactoryMeta,
                            ORDER_DESC,
                            getViewName(viewExpr),
                            getViewPosition(viewExpr),
                            model.isUpdate()
                    ),
                    RecordSinkFactory.getInstance(asm, queryMeta, listColumnFilterA, configuration),
                    keyTypes,
                    partitionByColumnIndexes,
                    null,
                    null,
                    columnIndexes,
                    columnSizeShifts
            );
        }

        return new LatestByAllFilteredRecordCursorFactory(
                configuration,
                queryMeta,
                new FullPartitionFrameCursorFactory(
                        tableToken,
                        model.getMetadataVersion(),
                        dfcFactoryMeta,
                        ORDER_DESC,
                        getViewName(viewExpr),
                        getViewPosition(viewExpr),
                        model.isUpdate()
                ),
                RecordSinkFactory.getInstance(asm, queryMeta, listColumnFilterA, configuration),
                keyTypes,
                null,
                columnIndexes,
                columnSizeShifts
        );
    }

    private RecordCursorFactory generateUnionAllFactory(
            QueryModel model,
            SqlExecutionContext executionContext,
            RecordCursorFactory factoryA,
            RecordCursorFactory factoryB,
            ObjList<Function> castFunctionsA,
            ObjList<Function> castFunctionsB,
            RecordMetadata unionMetadata
    ) throws SqlException {
        final RecordCursorFactory unionFactory = new UnionAllRecordCursorFactory(
                unionMetadata,
                factoryA,
                factoryB,
                castFunctionsA,
                castFunctionsB
        );

        if (model.getUnionModel().getUnionModel() != null) {
            return generateSetFactory(model.getUnionModel(), unionFactory, executionContext);
        }
        return unionFactory;
    }

    private RecordCursorFactory generateUnionFactory(
            QueryModel model,
            SqlExecutionContext executionContext,
            RecordCursorFactory factoryA,
            RecordCursorFactory factoryB,
            ObjList<Function> castFunctionsA,
            ObjList<Function> castFunctionsB,
            RecordMetadata unionMetadata,
            SetRecordCursorFactoryConstructor constructor
    ) throws SqlException {
        writeSymbolAsString.clear();
        valueTypes.clear();
        // Remap symbol columns to string type since that's how recordSink copies them.
        keyTypes.clear();
        for (int i = 0, n = unionMetadata.getColumnCount(); i < n; i++) {
            final int columnType = unionMetadata.getColumnType(i);
            if (isSymbol(columnType)) {
                keyTypes.add(STRING);
                writeSymbolAsString.set(i);
            } else {
                keyTypes.add(columnType);
            }
        }

        entityColumnFilter.of(factoryA.getMetadata().getColumnCount());
        final RecordSink recordSink = RecordSinkFactory.getInstance(
                asm,
                unionMetadata,
                entityColumnFilter,
                writeSymbolAsString,
                configuration
        );

        RecordCursorFactory unionFactory = constructor.create(
                configuration,
                unionMetadata,
                factoryA,
                factoryB,
                castFunctionsA,
                castFunctionsB,
                recordSink,
                keyTypes,
                valueTypes
        );

        if (model.getUnionModel().getUnionModel() != null) {
            return generateSetFactory(model.getUnionModel(), unionFactory, executionContext);
        }
        return unionFactory;
    }

    @Nullable
    private Function getHiFunction(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        return toLimitFunction(executionContext, model.getLimitHi(), null);
    }

    @Nullable
    private Function getLimitLoFunctionOnly(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        if (model.getLimitAdviceLo() != null && model.getLimitAdviceHi() == null) {
            return toLimitFunction(executionContext, model.getLimitAdviceLo(), LongConstant.ZERO);
        }
        return null;
    }

    @NotNull
    private Function getLoFunction(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        return toLimitFunction(executionContext, model.getLimitLo(), LongConstant.ZERO);
    }

    private int getSampleBySymbolKeyIndex(QueryModel model, RecordMetadata metadata) {
        final ObjList<QueryColumn> columns = model.getColumns();

        for (int i = 0, n = columns.size(); i < n; i++) {
            final QueryColumn column = columns.getQuick(i);
            final ExpressionNode node = column.getAst();

            if (node.type == LITERAL) {
                int idx = metadata.getColumnIndex(node.token);
                int columnType = metadata.getColumnType(idx);

                if (columnType == SYMBOL) {
                    return idx;
                }
            }
        }

        return -1;
    }

    private int getTimestampIndex(QueryModel model, RecordCursorFactory factory) throws SqlException {
        return getTimestampIndex(model, factory.getMetadata());
    }

    private int getTimestampIndex(QueryModel model, RecordMetadata metadata) throws SqlException {
        final ExpressionNode timestamp = model.getTimestamp();
        if (timestamp != null) {
            int timestampIndex = metadata.getColumnIndexQuiet(timestamp.token);
            if (timestampIndex == -1) {
                throw SqlException.invalidColumn(timestamp.position, timestamp.token);
            }
            if (!isTimestamp(metadata.getColumnType(timestampIndex))) {
                throw SqlException.$(timestamp.position, "not a TIMESTAMP");
            }
            return timestampIndex;
        }
        return metadata.getTimestampIndex();
    }

    private void guardAgainstDotsInOrderByAdvice(QueryModel model) throws SqlException {
        ObjList<ExpressionNode> advice = model.getOrderByAdvice();
        for (int i = 0, n = advice.size(); i < n; i++) {
            if (Chars.indexOf(advice.getQuick(i).token, '.') > -1) {
                throw SqlException.$(advice.getQuick(i).position, "cannot use table-prefixed names in order by");
            }
        }
    }

    private void guardAgainstFillWithKeyedGroupBy(QueryModel model, ArrayColumnTypes keyTypes) throws SqlException {
        // locate fill
        QueryModel curr = model;
        while (curr != null && curr.getFillStride() == null) {
            curr = curr.getNestedModel();
        }

        if (curr == null || curr.getFillStride() == null || curr.getFillValues() == null || curr.getFillValues().size() == 0) {
            return;
        }

        if (curr.getFillValues().size() == 1 && isNoneKeyword(curr.getFillValues().getQuick(0).token)) {
            return;
        }

        if (keyTypes.getColumnCount() == 1) {
            return;
        }

        throw SqlException.$(0, "cannot use FILL with a keyed GROUP BY");
    }

    private void guardAgainstFromToWithKeyedSampleBy(boolean isFromTo) throws SqlException {
        if (isFromTo) {
            throw SqlException.$(0, "FROM-TO intervals are not supported for keyed SAMPLE BY queries");
        }
    }

    private boolean isKeyedTemporalJoin(RecordMetadata masterMetadata, RecordMetadata slaveMetadata) {
        // Check if we can simplify ASOF JOIN ON (ts) to ASOF JOIN.
        if (listColumnFilterA.size() == 1 && listColumnFilterB.size() == 1) {
            int masterIndex = listColumnFilterB.getColumnIndexFactored(0);
            int slaveIndex = listColumnFilterA.getColumnIndexFactored(0);
            return masterIndex != masterMetadata.getTimestampIndex() || slaveIndex != slaveMetadata.getTimestampIndex();
        }
        return listColumnFilterA.size() > 0 && listColumnFilterB.size() > 0;
    }

    private boolean isOrderByDesignatedTimestampOnly(QueryModel model) {
        return model.getOrderByAdvice().size() == 1
                && model.getTimestamp() != null
                && Chars.equalsIgnoreCase(model.getOrderByAdvice().getQuick(0).token, model.getTimestamp().token);
    }

    private boolean isSameTable(RecordCursorFactory masterFactory, RecordCursorFactory slaveFactory) {
        return masterFactory.getTableToken() != null && masterFactory.getTableToken().equals(slaveFactory.getTableToken());
    }

    private void lookupColumnIndexes(
            ListColumnFilter filter,
            ObjList<ExpressionNode> columnNames,
            RecordMetadata metadata
    ) throws SqlException {
        filter.clear();
        for (int i = 0, n = columnNames.size(); i < n; i++) {
            final CharSequence columnName = columnNames.getQuick(i).token;
            int columnIndex = metadata.getColumnIndexQuiet(columnName);
            if (columnIndex > -1) {
                filter.add(columnIndex + 1);
            } else {
                int dot = Chars.indexOfLastUnquoted(columnName, '.');
                if (dot > -1) {
                    columnIndex = metadata.getColumnIndexQuiet(columnName, dot + 1, columnName.length());
                    if (columnIndex > -1) {
                        filter.add(columnIndex + 1);
                        return;
                    }
                }
                throw SqlException.invalidColumn(columnNames.getQuick(i).position, columnName);
            }
        }
    }

    private void lookupColumnIndexesUsingVanillaNames(
            ListColumnFilter filter,
            ObjList<CharSequence> columnNames,
            RecordMetadata metadata
    ) {
        filter.clear();
        for (int i = 0, n = columnNames.size(); i < n; i++) {
            filter.add(metadata.getColumnIndex(columnNames.getQuick(i)) + 1);
        }
    }

    private int prepareLatestByColumnIndexes(ObjList<ExpressionNode> latestBy, RecordMetadata myMeta) throws SqlException {
        keyTypes.clear();
        listColumnFilterA.clear();

        final int latestByColumnCount = latestBy.size();
        if (latestByColumnCount > 0) {
            // validate the latest by against the current reader
            // first check if column is valid
            for (int i = 0; i < latestByColumnCount; i++) {
                final ExpressionNode latestByNode = latestBy.getQuick(i);
                final int index = myMeta.getColumnIndexQuiet(latestByNode.token);
                if (index == -1) {
                    throw SqlException.invalidColumn(latestByNode.position, latestByNode.token);
                }

                // check the type of the column, not all are supported
                int columnType = myMeta.getColumnType(index);
                switch (tagOf(columnType)) {
                    case BOOLEAN:
                    case BYTE:
                    case CHAR:
                    case SHORT:
                    case INT:
                    case IPv4:
                    case LONG:
                    case DATE:
                    case TIMESTAMP:
                    case FLOAT:
                    case DOUBLE:
                    case LONG256:
                    case STRING:
                    case VARCHAR:
                    case SYMBOL:
                    case UUID:
                    case GEOBYTE:
                    case GEOSHORT:
                    case GEOINT:
                    case GEOLONG:
                    case LONG128:
                        // we are reusing collections which leads to confusing naming for this method
                        // keyTypes are types of columns we collect 'latest by' for
                        keyTypes.add(columnType);
                        // listColumnFilterA are indexes of columns we collect 'latest by' for
                        listColumnFilterA.add(index + 1);
                        break;

                    default:
                        throw SqlException
                                .position(latestByNode.position)
                                .put(latestByNode.token)
                                .put(" (")
                                .put(ColumnType.nameOf(columnType))
                                .put("): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON");
                }
            }
        }
        return latestByColumnCount;
    }

    private void processJoinContext(
            boolean vanillaMaster,
            boolean isSelfJoin,
            JoinContext jc,
            RecordMetadata masterMetadata,
            RecordMetadata slaveMetadata
    ) throws SqlException {
        lookupColumnIndexesUsingVanillaNames(listColumnFilterA, jc.aNames, slaveMetadata);
        if (vanillaMaster) {
            lookupColumnIndexesUsingVanillaNames(listColumnFilterB, jc.bNames, masterMetadata);
        } else {
            lookupColumnIndexes(listColumnFilterB, jc.bNodes, masterMetadata);
        }

        // compare types and populate keyTypes
        keyTypes.clear();
        writeSymbolAsString.clear();
        writeStringAsVarcharA.clear();
        writeStringAsVarcharB.clear();
        writeTimestampAsNanosA.clear();
        writeTimestampAsNanosB.clear();
        for (int k = 0, m = listColumnFilterA.getColumnCount(); k < m; k++) {
            // Don't use tagOf(columnType) to compare the types.
            // Key types have too much exactly except SYMBOL and STRING special case
            final int columnIndexA = listColumnFilterA.getColumnIndexFactored(k);
            final int columnIndexB = listColumnFilterB.getColumnIndexFactored(k);
            final int columnTypeA = slaveMetadata.getColumnType(columnIndexA);
            final String columnNameA = slaveMetadata.getColumnName(columnIndexA);
            final int columnTypeB = masterMetadata.getColumnType(columnIndexB);
            final String columnNameB = masterMetadata.getColumnName(columnIndexB);
            if (columnTypeB != columnTypeA &&
                    !(isSymbolOrStringOrVarchar(columnTypeB) && isSymbolOrStringOrVarchar(columnTypeA)) &&
                    !(isTimestamp(columnTypeB) && isTimestamp(columnTypeA))
            ) {
                // index in column filter and join context is the same
                throw SqlException.$(jc.aNodes.getQuick(k).position, "join column type mismatch");
            }
            if (isVarchar(columnTypeA) || isVarchar(columnTypeB)) {
                keyTypes.add(VARCHAR);
                if (isVarchar(columnTypeA)) {
                    writeStringAsVarcharB.set(columnIndexB);
                } else {
                    writeStringAsVarcharA.set(columnIndexA);
                }
                writeSymbolAsString.set(columnIndexA);
                writeSymbolAsString.set(columnIndexB);
            } else if (columnTypeB == ColumnType.SYMBOL) {
                if (isSelfJoin && Chars.equalsIgnoreCase(columnNameA, columnNameB)) {
                    keyTypes.add(ColumnType.SYMBOL);
                } else {
                    keyTypes.add(STRING);
                    writeSymbolAsString.set(columnIndexA);
                    writeSymbolAsString.set(columnIndexB);
                }
            } else if (isString(columnTypeA) || isString(columnTypeB)) {
                keyTypes.add(columnTypeB);
                writeSymbolAsString.set(columnIndexA);
                writeSymbolAsString.set(columnIndexB);
            } else if (columnTypeA != columnTypeB &&
                    isTimestamp(columnTypeA) && isTimestamp(columnTypeB)
            ) {
                keyTypes.add(TIMESTAMP_NANO);
                // Mark columns that need conversion to nanoseconds
                if (!isTimestampNano(columnTypeA)) {
                    writeTimestampAsNanosA.set(columnIndexA);
                }
                if (!isTimestampNano(columnTypeB)) {
                    writeTimestampAsNanosB.set(columnIndexB);
                }
            } else {
                keyTypes.add(columnTypeB);
            }
        }
    }

    private void processNodeQueryModels(ExpressionNode node, ModelOperator operator) {
        sqlNodeStack.clear();
        while (node != null) {
            if (node.queryModel != null) {
                operator.operate(expressionNodePool, node.queryModel);
            }

            if (node.lhs != null) {
                sqlNodeStack.push(node.lhs);
            }

            if (node.rhs != null) {
                node = node.rhs;
            } else {
                if (!sqlNodeStack.isEmpty()) {
                    node = sqlNodeStack.poll();
                } else {
                    node = null;
                }
            }
        }
    }

    private void restoreWhereClause(ExpressionNode node) {
        processNodeQueryModels(node, RESTORE_WHERE_CLAUSE);
    }

    private Function toLimitFunction(
            SqlExecutionContext executionContext,
            ExpressionNode limit,
            ConstantFunction defaultValue
    ) throws SqlException {
        if (limit == null) {
            return defaultValue;
        }

        final Function limitFunc = functionParser.parseFunction(limit, EmptyRecordMetadata.INSTANCE, executionContext);

        // coerce to a convertible type
        coerceRuntimeConstantType(limitFunc, LONG, executionContext, "LIMIT expressions must be convertible to INT", limit.position);

        // also rule out string, varchar etc.
        int limitFuncType = limitFunc.getType();
        if (limitTypes.excludes(limitFuncType)) {
            throw SqlException.$(limit.position, "invalid type: ").put(ColumnType.nameOf(limitFuncType));
        }

        limit.implemented = true;

        return limitFunc;
    }

    private void validateBothTimestampOrders(RecordCursorFactory masterFactory, RecordCursorFactory slaveFactory, int position) throws SqlException {
        if (masterFactory.getScanDirection() != RecordCursorFactory.SCAN_DIRECTION_FORWARD) {
            throw SqlException.$(position, "left side of time series join doesn't have ASC timestamp order");
        }
        if (slaveFactory.getScanDirection() != RecordCursorFactory.SCAN_DIRECTION_FORWARD) {
            throw SqlException.$(position, "right side of time series join doesn't have ASC timestamp order");
        }
    }

    private void validateBothTimestamps(QueryModel slaveModel, RecordMetadata masterMetadata, RecordMetadata slaveMetadata) throws SqlException {
        if (masterMetadata.getTimestampIndex() == -1) {
            throw SqlException.$(slaveModel.getJoinKeywordPosition(), "left side of time series join has no timestamp");
        }
        if (slaveMetadata.getTimestampIndex() == -1) {
            throw SqlException.$(slaveModel.getJoinKeywordPosition(), "right side of time series join has no timestamp");
        }
    }

    private void validateOuterJoinExpressions(QueryModel model, CharSequence joinType) throws SqlException {
        if (model.getOuterJoinExpressionClause() != null) {
            throw SqlException.$(model.getOuterJoinExpressionClause().position, "unsupported ").put(joinType).put(" join expression ")
                    .put("[expr='").put(model.getOuterJoinExpressionClause()).put("']");
        }
    }

    private Record.CharSequenceFunction validateSubQueryColumnAndGetGetter(IntrinsicModel intrinsicModel, RecordMetadata metadata) throws SqlException {
        int columnType = metadata.getColumnType(0);
        switch (columnType) {
            case STRING:
                return Record.GET_STR;
            case SYMBOL:
                return Record.GET_SYM;
            case VARCHAR:
                return Record.GET_VARCHAR;
            default:
                assert intrinsicModel.keySubQuery.getColumns() != null;
                assert intrinsicModel.keySubQuery.getColumns().size() > 0;
                throw SqlException
                        .position(intrinsicModel.keySubQuery.getColumns().getQuick(0).getAst().position)
                        .put("unsupported column type: ")
                        .put(metadata.getColumnName(0))
                        .put(": ")
                        .put(ColumnType.nameOf(columnType));
        }
    }

    private void validateTimestampNotInJoinKeys(QueryModel slaveModel, RecordMetadata masterMetadata, RecordMetadata slaveMetadata) throws SqlException {
        // When timestamp is the ONLY join key, isKeyedTemporalJoin() will return false and simplify
        // "ASOF JOIN ON (ts)" to a non-keyed join, which is harmless. But when timestamp is combined
        // with other keys like "ON (sym, ts)", it causes problems in the join implementation.
        int keyCount = listColumnFilterA.getColumnCount();
        if (keyCount <= 1) {
            return; // Single key (or no key) is fine - handled by isKeyedTemporalJoin()
        }
        // listColumnFilterA contains slave column indices, listColumnFilterB contains master column indices
        int slaveTimestampIndex = slaveMetadata.getTimestampIndex();
        int masterTimestampIndex = masterMetadata.getTimestampIndex();
        for (int i = 0; i < keyCount; i++) {
            if (listColumnFilterA.getColumnIndexFactored(i) == slaveTimestampIndex ||
                    listColumnFilterB.getColumnIndexFactored(i) == masterTimestampIndex) {
                throw SqlException.$(slaveModel.getJoinKeywordPosition(),
                        "ASOF/LT JOIN cannot use designated timestamp as a join key");
            }
        }
    }

    // used in tests
    void setEnableJitNullChecks(boolean value) {
        enableJitNullChecks = value;
    }

    void setFullFatJoins(boolean fullFatJoins) {
        this.fullFatJoins = fullFatJoins;
    }

    @FunctionalInterface
    public interface FullFatJoinGenerator {
        RecordCursorFactory create(
                CairoConfiguration configuration,
                RecordMetadata metadata,
                RecordCursorFactory masterFactory,
                RecordCursorFactory slaveFactory,
                @Transient ColumnTypes mapKeyTypes,
                @Transient ColumnTypes mapValueTypes,
                @Transient ColumnTypes slaveColumnTypes,
                RecordSink masterKeySink,
                RecordSink slaveKeySink,
                int columnSplit,
                RecordValueSink slaveValueSink,
                IntList columnIndex,
                JoinContext joinContext,
                ColumnFilter masterTableKeyColumns,
                long toleranceInterval,
                int slaveValueTimestampIndex
        );
    }

    @FunctionalInterface
    interface ModelOperator {
        void operate(ObjectPool<ExpressionNode> pool, QueryModel model);
    }

    /**
     * Container class to hold the detected parameters of a markout horizon pattern.
     */
    private static class MarkoutHorizonInfo {
        int masterTimestampColumnIndex;
        int slaveSequenceColumnIndex;

        MarkoutHorizonInfo of(int timestampColumnIndex, int slaveColumnIndex) {
            this.masterTimestampColumnIndex = timestampColumnIndex;
            this.slaveSequenceColumnIndex = slaveColumnIndex;
            return this;
        }
    }

    private static class RecordCursorFactoryStub implements RecordCursorFactory {
        final ExecutionModel model;
        RecordCursorFactory factory;

        protected RecordCursorFactoryStub(ExecutionModel model, RecordCursorFactory factory) {
            this.model = model;
            this.factory = factory;
        }

        @Override
        public void close() {
            factory = Misc.free(factory);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
            if (factory != null) {
                return factory.getCursor(executionContext);
            } else {
                return null;
            }
        }

        @Override
        public RecordMetadata getMetadata() {
            return null;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type(model.getTypeName());

            final CharSequence tableName = model.getTableName();
            if (tableName != null) {
                sink.meta(model.getModelType() == CREATE_MAT_VIEW ? "view" : "table").val(tableName);
            }
            if (factory != null) {
                sink.child(factory);
            }
        }
    }

    private static class WindowJoinAggColumnVectorizedCheck implements PostOrderTreeTraversalAlgo.Visitor {
        int columnSplit;
        RecordMetadata metadata;
        boolean vectorized;

        @Override
        public void visit(ExpressionNode node) {
            if (!vectorized) {
                return;
            }
            if (node.type == LITERAL) {
                int columnIndex = metadata.getColumnIndexQuiet(node.token);
                if (columnIndex < columnSplit) {
                    vectorized = false;
                }
            }
        }

        void of(RecordMetadata metadata, int columnSplit) {
            this.metadata = metadata;
            this.columnSplit = columnSplit;
            this.vectorized = true;
        }
    }

    private static class WindowJoinColCheckVisitor implements PostOrderTreeTraversalAlgo.Visitor {
        boolean hasIncludeCol;
        RecordMetadata metadata;
        boolean shouldInclude;

        @Override
        public void visit(ExpressionNode node) {
            if (node.type == LITERAL) {
                int columnIndex = metadata.getColumnIndexQuiet(node.token);
                if (columnIndex == -1) {
                    shouldInclude = false;
                } else {
                    hasIncludeCol = true;
                }
            }
        }

        void of(RecordMetadata metadata) {
            this.metadata = metadata;
            this.shouldInclude = true;
            this.hasIncludeCol = false;
        }
    }

    static {
        joinsRequiringTimestamp[JOIN_ASOF] = true;
        joinsRequiringTimestamp[JOIN_SPLICE] = true;
        joinsRequiringTimestamp[JOIN_LT] = true;
        joinsRequiringTimestamp[JOIN_WINDOW] = true;
    }

    static {
        limitTypes.add(LONG);
        limitTypes.add(BYTE);
        limitTypes.add(SHORT);
        limitTypes.add(INT);
        limitTypes.add(UNDEFINED);
    }

    static {
        countConstructors.put(DOUBLE, CountDoubleVectorAggregateFunction::new);
        countConstructors.put(INT, CountIntVectorAggregateFunction::new);
        countConstructors.put(LONG, CountLongVectorAggregateFunction::new);
        countConstructors.put(DATE, CountLongVectorAggregateFunction::new);
        countConstructors.put(TIMESTAMP_MICRO, CountLongVectorAggregateFunction::new);
        countConstructors.put(TIMESTAMP_NANO, CountLongVectorAggregateFunction::new);

        sumConstructors.put(DOUBLE, SumDoubleVectorAggregateFunction::new);
        sumConstructors.put(INT, SumIntVectorAggregateFunction::new);
        sumConstructors.put(LONG, SumLongVectorAggregateFunction::new);
        sumConstructors.put(LONG256, SumLong256VectorAggregateFunction::new);
        sumConstructors.put(SHORT, SumShortVectorAggregateFunction::new);

        ksumConstructors.put(DOUBLE, KSumDoubleVectorAggregateFunction::new);
        nsumConstructors.put(DOUBLE, NSumDoubleVectorAggregateFunction::new);

        avgConstructors.put(DOUBLE, AvgDoubleVectorAggregateFunction::new);
        avgConstructors.put(LONG, AvgLongVectorAggregateFunction::new);
        avgConstructors.put(INT, AvgIntVectorAggregateFunction::new);
        avgConstructors.put(SHORT, AvgShortVectorAggregateFunction::new);

        minConstructors.put(DOUBLE, MinDoubleVectorAggregateFunction::new);
        minConstructors.put(LONG, MinLongVectorAggregateFunction::new);
        minConstructors.put(DATE, MinDateVectorAggregateFunction::new);
        minConstructors.put(TIMESTAMP_MICRO, (int keyKind, int columnIndex, int timestampIndex, int workerCount) -> new MinTimestampVectorAggregateFunction(keyKind, columnIndex, TIMESTAMP_MICRO, timestampIndex));
        minConstructors.put(TIMESTAMP_NANO, (int keyKind, int columnIndex, int timestampIndex, int workerCount) -> new MinTimestampVectorAggregateFunction(keyKind, columnIndex, TIMESTAMP_NANO, timestampIndex));
        minConstructors.put(INT, MinIntVectorAggregateFunction::new);
        minConstructors.put(SHORT, MinShortVectorAggregateFunction::new);

        maxConstructors.put(DOUBLE, MaxDoubleVectorAggregateFunction::new);
        maxConstructors.put(LONG, MaxLongVectorAggregateFunction::new);
        maxConstructors.put(DATE, MaxDateVectorAggregateFunction::new);
        maxConstructors.put(TIMESTAMP_MICRO, (int keyKind, int columnIndex, int timestampIndex, int workerCount) -> new MaxTimestampVectorAggregateFunction(keyKind, columnIndex, TIMESTAMP_MICRO, timestampIndex));
        maxConstructors.put(TIMESTAMP_NANO, (int keyKind, int columnIndex, int timestampIndex, int workerCount) -> new MaxTimestampVectorAggregateFunction(keyKind, columnIndex, TIMESTAMP_NANO, timestampIndex));
        maxConstructors.put(INT, MaxIntVectorAggregateFunction::new);
        maxConstructors.put(SHORT, MaxShortVectorAggregateFunction::new);
    }
}

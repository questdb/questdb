/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.AbstractPartitionFrameCursorFactory;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnFilter;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.FullBwdPartitionFrameCursorFactory;
import io.questdb.cairo.FullFwdPartitionFrameCursorFactory;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IntervalBwdPartitionFrameCursorFactory;
import io.questdb.cairo.IntervalFwdPartitionFrameCursorFactory;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
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
import io.questdb.griffin.engine.functions.cast.CastByteToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastByteToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDateToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDateToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDateToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDoubleToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDoubleToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastFloatToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastFloatToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastGeoHashToGeoHashFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIPv4ToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIPv4ToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntervalToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLong256ToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLong256ToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToGeoHashFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastSymbolToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastSymbolToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastTimestampToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastTimestampToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastUuidToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastUuidToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToGeoHashFunctionFactory;
import io.questdb.griffin.engine.functions.columns.BinColumn;
import io.questdb.griffin.engine.functions.columns.BooleanColumn;
import io.questdb.griffin.engine.functions.columns.ByteColumn;
import io.questdb.griffin.engine.functions.columns.CharColumn;
import io.questdb.griffin.engine.functions.columns.DateColumn;
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
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.griffin.engine.groupby.CountRecordCursorFactory;
import io.questdb.griffin.engine.groupby.DistinctIntKeyRecordCursorFactory;
import io.questdb.griffin.engine.groupby.DistinctRecordCursorFactory;
import io.questdb.griffin.engine.groupby.DistinctSymbolRecordCursorFactory;
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
import io.questdb.griffin.engine.join.AsOfJoinFastRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinNoKeyFastRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinNoKeyRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.CrossJoinRecordCursorFactory;
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
import io.questdb.griffin.engine.join.NestedLoopLeftJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.NullRecordFactory;
import io.questdb.griffin.engine.join.RecordAsAFieldRecordCursorFactory;
import io.questdb.griffin.engine.join.SpliceJoinLightRecordCursorFactory;
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
import io.questdb.griffin.engine.table.BwdPageFrameRowCursorFactory;
import io.questdb.griffin.engine.table.DeferredSingleSymbolFilterPageFrameRecordCursorFactory;
import io.questdb.griffin.engine.table.DeferredSymbolIndexFilteredRowCursorFactory;
import io.questdb.griffin.engine.table.DeferredSymbolIndexRowCursorFactory;
import io.questdb.griffin.engine.table.FilterOnExcludedValuesRecordCursorFactory;
import io.questdb.griffin.engine.table.FilterOnSubQueryRecordCursorFactory;
import io.questdb.griffin.engine.table.FilterOnValuesRecordCursorFactory;
import io.questdb.griffin.engine.table.FilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.FwdPageFrameRowCursorFactory;
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
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.griffin.model.WindowColumn;
import io.questdb.jit.CompiledFilter;
import io.questdb.jit.CompiledFilterIRSerializer;
import io.questdb.jit.JitUtil;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.BitSet;
import io.questdb.std.BufferWindowCharSequence;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Chars;
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
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.ArrayDeque;

import static io.questdb.cairo.ColumnType.getGeoHashBits;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.*;
import static io.questdb.griffin.SqlKeywords.*;
import static io.questdb.griffin.model.ExpressionNode.*;
import static io.questdb.griffin.model.QueryModel.QUERY;
import static io.questdb.griffin.model.QueryModel.*;

public class SqlCodeGenerator implements Mutable, Closeable {
    public static final int GKK_HOUR_INT = 1;
    public static final int GKK_VANILLA_INT = 0;
    private static final VectorAggregateFunctionConstructor COUNT_CONSTRUCTOR = (keyKind, columnIndex, workerCount) -> new CountVectorAggregateFunction(keyKind);
    private static final FullFatJoinGenerator CREATE_FULL_FAT_AS_OF_JOIN = SqlCodeGenerator::createFullFatAsOfJoin;
    private static final FullFatJoinGenerator CREATE_FULL_FAT_LT_JOIN = SqlCodeGenerator::createFullFatLtJoin;
    private static final Log LOG = LogFactory.getLog(SqlCodeGenerator.class);
    private static final ModelOperator RESTORE_WHERE_CLAUSE = QueryModel::restoreWhereClause;
    private static final SetRecordCursorFactoryConstructor SET_EXCEPT_ALL_CONSTRUCTOR = ExceptAllRecordCursorFactory::new;
    private static final SetRecordCursorFactoryConstructor SET_EXCEPT_CONSTRUCTOR = ExceptRecordCursorFactory::new;
    private static final SetRecordCursorFactoryConstructor SET_INTERSECT_ALL_CONSTRUCTOR = IntersectAllRecordCursorFactory::new;
    private static final SetRecordCursorFactoryConstructor SET_INTERSECT_CONSTRUCTOR = IntersectRecordCursorFactory::new;
    private static final SetRecordCursorFactoryConstructor SET_UNION_CONSTRUCTOR = UnionRecordCursorFactory::new;
    // @formatter:off
    /**
     * Autogenerated. See `SqlCodeGeneratorTest.testUnionCastMatrix`.
     * <p>
     * The UNION_CAST_MATRIX captures all the combinations of "left" and "right" column types
     * in a set operation (UNION etc.), providing the desired output type. Since there are many
     * special cases in the conversion logic, we decided to use a matrix of literals instead.
     * The matrix doesn't cover generic types (e.g. geohash) since they have a more complex structure.
     * Initially, we used the code below to print out the values for the matrix:
     */
    private static final int[][] UNION_CAST_MATRIX = new int[][]{
            { 0, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, 11, 11, 11, 11,  0}, //  0 = unknown
            {11,  1, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, 11, 11, 11, 11,  1}, //  1 = BOOLEAN
            {11, 11,  2,  3, 11,  5,  6,  7,  8,  9, 10, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, 11, 11, 11, 11,  2}, //  2 = BYTE
            {11, 11,  3,  3,  3,  5,  6,  7,  8,  9, 10, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, 11, 11, 11, 11,  3}, //  3 = SHORT
            {11, 11, 11,  3,  4,  5,  6,  7,  8,  9, 10, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, 11, 11, 11, 11, 11}, //  4 = CHAR
            {11, 11,  5,  5,  5,  5,  6,  7,  8,  9, 10, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, 11, 11, 11, 11,  5}, //  5 = INT
            {11, 11,  6,  6,  6,  6,  6,  7,  8,  9, 10, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, 11, 11, 11, 11,  6}, //  6 = LONG
            {11, 11,  7,  7,  7,  7,  7,  7,  8,  9, 10, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, 11, 11, 11, 11,  7}, //  7 = DATE
            {11, 11,  8,  8,  8,  8,  8,  8,  8,  9, 10, 11,  8, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, 11, 11, 11, 11,  8}, //  8 = TIMESTAMP
            {11, 11,  9,  9,  9,  9,  9,  9,  9,  9, 10, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, 11, 11, 11, 11,  9}, //  9 = FLOAT
            {11, 11, 10, 10, 10, 10, 10, 10, 10, 10, 10, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, 11, 11, 11, 11, 10}, // 10 = DOUBLE
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11}, // 11 = STRING
            {11, 11, 11, 11, 11, 11, 11, 11,  8, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, 11, 11, 11, 11, 11}, // 12 = SYMBOL
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 13, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, 11, 11, 11, 11, 13}, // 13 = LONG256
            {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}, // 14 = unknown
            {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}, // 15 = unknown
            {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}, // 16 = unknown
            {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}, // 17 = unknown
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 18, 11, 11, 11, 11, 11, 11, 11, 26, 11, 11, 11, 11, 11, 18}, // 18 = BINARY
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 19, 11, 11, 11, 11, 11, 11, 26, 11, 11, 11, 11, 11, 19}, // 19 = UUID
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 20, 11, 11, 11, 11, 11, 26, 11, 11, 11, 11, 11, 20}, // 20 = CURSOR
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 21, 11, 11, 11, 11, 26, 11, 11, 11, 11, 11, 21}, // 21 = VARARG
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 22, 11, 11, 11, 26, 11, 11, 11, 11, 11, 22}, // 22 = RECORD
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 23, 11, 11, 26, 11, 11, 11, 11, 11, 23}, // 23 = GEOHASH
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 24, 11, 26, 11, 11, 11, 11, 11, 24}, // 24 = LONG128
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 25, 26, 11, 11, 11, 11, 11, 25}, // 25 = IPv4
            {26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 11, 26, 26, -1, -1, -1, -1, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26}, // 26 = VARCHAR
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 27, 11, 11, 11, 11, 27}, // 27 = regclass
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, 28, 11, 11, 11, 28}, // 28 = regprocedure
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, 11, 29, 11, 11, 29}, // 29 = text[]
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, 11, 11, 30, 11, 30}, // 30 = PARAMETER
            {11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, -1, -1, -1, -1, 11, 11, 11, 11, 11, 11, 11, 11, 26, 11, 11, 11, 11, 31, 31}, // 31 = INTERVAL
            { 0,  1,  2,  3, 11,  5,  6,  7,  8,  9, 10, 11, 11, 13, -1, -1, -1, -1, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}  // 32 = NULL
    };
    // @formatter:on
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> avgConstructors = new IntObjHashMap<>();
    private static final ModelOperator backupWhereClauseRef = QueryModel::backupWhereClause;
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> countConstructors = new IntObjHashMap<>();
    private static final boolean[] joinsRequiringTimestamp = new boolean[JOIN_MAX + 1];
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> ksumConstructors = new IntObjHashMap<>();
    private static final IntHashSet limitTypes = new IntHashSet();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> maxConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> minConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> nsumConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> sumConstructors = new IntObjHashMap<>();
    private final ArrayColumnTypes arrayColumnTypes = new ArrayColumnTypes();
    private final BytecodeAssembler asm = new BytecodeAssembler();
    private final CairoConfiguration configuration;
    private final ObjList<TableColumnMetadata> deferredWindowMetadata = new ObjList<>();
    private final boolean enableJitDebug;
    private final CairoEngine engine;
    private final EntityColumnFilter entityColumnFilter = new EntityColumnFilter();
    private final ObjectPool<ExpressionNode> expressionNodePool;
    private final boolean fastAsOfJoins;
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
    private final LongList prefixes = new LongList();
    private final RecordComparatorCompiler recordComparatorCompiler;
    private final IntList recordFunctionPositions = new IntList();
    private final PageFrameReduceTaskFactory reduceTaskFactory;
    private final ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
    private final WhereClauseSymbolEstimator symbolEstimator = new WhereClauseSymbolEstimator();
    private final IntList tempAggIndex = new IntList();
    private final IntList tempKeyIndex = new IntList();
    private final IntList tempKeyIndexesInBase = new IntList();
    private final IntList tempKeyKinds = new IntList();
    private final GenericRecordMetadata tempMetadata = new GenericRecordMetadata();
    private final IntList tempSymbolSkewIndexes = new IntList();
    private final ObjList<VectorAggregateFunction> tempVaf = new ObjList<>();
    private final IntList tempVecConstructorArgIndexes = new IntList();
    private final ObjList<VectorAggregateFunctionConstructor> tempVecConstructors = new ObjList<>();
    private final ArrayColumnTypes valueTypes = new ArrayColumnTypes();
    private final WhereClauseParser whereClauseParser = new WhereClauseParser();
    // a bitset of string/symbol columns forced to be serialised as varchar
    private final BitSet writeStringAsVarcharA = new BitSet();
    private final BitSet writeStringAsVarcharB = new BitSet();
    private final BitSet writeSymbolAsString = new BitSet();
    private boolean enableJitNullChecks = true;
    private boolean fullFatJoins = false;
    private boolean validateSampleByFillType;

    public SqlCodeGenerator(
            CairoEngine engine,
            CairoConfiguration configuration,
            FunctionParser functionParser,
            ObjectPool<ExpressionNode> expressionNodePool
    ) {
        try {
            this.engine = engine;
            this.configuration = configuration;
            this.functionParser = functionParser;
            this.recordComparatorCompiler = new RecordComparatorCompiler(asm);
            this.enableJitDebug = configuration.isSqlJitDebugEnabled();
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
            this.fastAsOfJoins = configuration.useFastAsOfJoin();
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
        final int[][] expected = new int[ColumnType.NULL + 1][ColumnType.NULL + 1];
        for (int typeA = 0; typeA <= ColumnType.NULL; typeA++) {
            for (int typeB = 0; typeB <= ColumnType.NULL; typeB++) {
                final int outType = (isGeoType(typeA) || isGeoType(typeB)) ? -1 : castToType(typeA, typeB);
                expected[typeA][typeB] = outType;
            }
        }
        return expected;
    }

    public static int getUnionCastType(int typeA, int typeB) {
        short tagA = ColumnType.tagOf(typeA);
        short tagB = ColumnType.tagOf(typeB);
        int geoBitsA = getGeoHashBits(typeA);
        int geoBitsB = getGeoHashBits(typeB);
        boolean isGeoHashA = geoBitsA != 0;
        boolean isGeoHashB = geoBitsB != 0;
        if (isGeoHashA != isGeoHashB) {
            // One type is geohash, the other isn't. Since a stringy type can be parsed
            // into geohash, output geohash when the other type is stringy. If not,
            // cast both types to string.
            return isStringyType(typeA) ? typeB
                    : isStringyType(typeB) ? typeA
                    : ColumnType.STRING;
        }
        if (isGeoHashA) {
            // Both types are geohash, resolve to the one with less geohash bits.
            return geoBitsA < geoBitsB ? typeA : typeB;
        }
        // Neither type is geohash, use the type cast matrix to resolve.
        return UNION_CAST_MATRIX[tagA][tagB];
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
        if (ColumnType.isBoolean(filter.getType())) {
            return filter;
        }
        Misc.free(filter);
        throw SqlException.$(expr.position, "boolean expression expected");
    }

    @NotNull
    public Function compileJoinFilter(
            ExpressionNode expr,
            JoinRecordMetadata metadata,
            SqlExecutionContext executionContext
    ) throws SqlException {
        try {
            return compileBooleanFilter(expr, metadata, executionContext);
        } catch (Throwable t) {
            Misc.free(metadata);
            throw t;
        }
    }

    public RecordCursorFactory generate(@Transient QueryModel model, @Transient SqlExecutionContext executionContext) throws SqlException {
        return generateQuery(model, executionContext, true);
    }

    public RecordCursorFactory generateExplain(@Transient ExplainModel model, @Transient SqlExecutionContext executionContext) throws SqlException {
        ExecutionModel innerModel = model.getInnerExecutionModel();
        QueryModel queryModel = innerModel.getQueryModel();
        RecordCursorFactory factory;
        if (queryModel != null) {
            factory = generate(queryModel, executionContext);
            if (innerModel.getModelType() != QUERY) {
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
                if (columnType == ColumnType.TIMESTAMP) {
                    if (idx != timestampIdx) {
                        return false;
                    }
                } else if (columnType == ColumnType.SYMBOL) {
                    if (symbolToken == null) {
                        symbolToken = node.token;
                    } else if (!Chars.equalsIgnoreCase(symbolToken, node.token)) {
                        return false; //more than one key symbol column
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

    private static int castToType(int typeA, int typeB) {
        return (typeA == typeB && typeA != ColumnType.SYMBOL) ? typeA
                : (isStringyType(typeA) && isStringyType(typeB)) ? ColumnType.STRING
                : (isStringyType(typeA) && isParseableType(typeB)) ? typeA
                : (isStringyType(typeB) && isParseableType(typeA)) ? typeB

                // NULL casts to any other nullable type, except for symbols which can't cross symbol tables.
                : ((typeA == ColumnType.NULL) && (typeB != ColumnType.CHAR) && (typeB != ColumnType.SYMBOL)) ? typeB
                : ((typeB == ColumnType.NULL) && (typeA != ColumnType.CHAR) && (typeA != ColumnType.SYMBOL)) ? typeA

                // cast long and timestamp to timestamp in unions instead of longs.
                : ((typeA == ColumnType.TIMESTAMP) && (typeB == ColumnType.LONG)) ? ColumnType.TIMESTAMP
                : ((typeA == ColumnType.LONG) && (typeB == ColumnType.TIMESTAMP)) ? ColumnType.TIMESTAMP

                // Varchars take priority over strings, but strings over most types.
                : (typeA == ColumnType.VARCHAR || typeB == ColumnType.VARCHAR) ? ColumnType.VARCHAR
                : ((typeA == ColumnType.STRING) || (typeB == ColumnType.STRING)) ? ColumnType.STRING

                // cast booleans vs anything other than varchars to strings.
                : ((typeA == ColumnType.BOOLEAN) || (typeB == ColumnType.BOOLEAN)) ? ColumnType.STRING

                : (ColumnType.isToSameOrWider(typeB, typeA) && typeA != ColumnType.SYMBOL && typeA != ColumnType.CHAR) ? typeA
                : (ColumnType.isToSameOrWider(typeA, typeB) && typeB != ColumnType.SYMBOL && typeB != ColumnType.CHAR) ? typeB
                : ColumnType.STRING;
    }

    private static void coerceRuntimeConstantType(Function func, short type, SqlExecutionContext context, CharSequence message, int pos) throws SqlException {
        if (ColumnType.isUndefined(func.getType())) {
            func.assignType(type, context.getBindVariableService());
        } else if ((!func.isConstant() && !func.isRuntimeConstant()) || !ColumnType.isAssignableFrom(func.getType(), type)) {
            throw SqlException.$(pos, message);
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
            ColumnFilter masterTableKeyColumns
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
                masterTableKeyColumns
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
            ColumnFilter masterTableKeyColumns
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
                masterTableKeyColumns
        );
    }

    private static int getOrderByDirectionOrDefault(QueryModel model, int index) {
        IntList direction = model.getOrderByDirectionAdvice();
        if (index >= direction.size()) {
            return ORDER_DIRECTION_ASCENDING;
        } else {
            return direction.get(index);
        }
    }

    private static boolean isGeoType(int colType) {
        return colType >= ColumnType.GEOBYTE && colType <= ColumnType.GEOLONG;
    }

    private static boolean isParseableType(int colType) {
        return colType == ColumnType.TIMESTAMP || colType == ColumnType.LONG256;
    }

    private static boolean isSingleColumnFunction(ExpressionNode ast, CharSequence name) {
        return ast.type == FUNCTION && ast.paramCount == 1 && Chars.equalsIgnoreCase(ast.token, name) && ast.rhs.type == LITERAL;
    }

    private static boolean isStringyType(int colType) {
        return colType == ColumnType.VARCHAR || colType == ColumnType.STRING;
    }

    private static RecordMetadata widenSetMetadata(RecordMetadata typesA, RecordMetadata typesB) {
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
    ) {
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
                    final int columnIndex = metadata.getColumnIndex(ast.token);
                    final int type = metadata.getColumnType(columnIndex);
                    if (ColumnType.isInt(type)) {
                        tempKeyIndexesInBase.add(columnIndex);
                        tempKeyIndex.add(i);
                        arrayColumnTypes.add(ColumnType.INT);
                        tempKeyKinds.add(GKK_VANILLA_INT);
                    } else if (ColumnType.isSymbol(type)) {
                        tempKeyIndexesInBase.add(columnIndex);
                        tempKeyIndex.add(i);
                        tempSymbolSkewIndexes.extendAndSet(i, columnIndex);
                        arrayColumnTypes.add(ColumnType.SYMBOL);
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
                LOG.error().$("Failed to initialize lo or hi functions [").$("error=").$(ex.getMessage()).I$();
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
            if (typeA != typeB || (typeA == ColumnType.SYMBOL && symbolDisallowed)) {
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

    private @Nullable ObjList<Function> compileWorkerFilterConditionally(
            SqlExecutionContext executionContext,
            @Nullable Function filter,
            int workerCount,
            @Nullable ExpressionNode filterExpr,
            RecordMetadata metadata
    ) throws SqlException {
        if (filter != null && !filter.isThreadSafe()) {
            assert filterExpr != null;
            ObjList<Function> workerFilters = new ObjList<>();
            for (int i = 0; i < workerCount; i++) {
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

    private @Nullable ObjList<ObjList<Function>> compileWorkerKeyFunctionsConditionally(
            SqlExecutionContext executionContext,
            @NotNull ObjList<Function> keyFunctions,
            int workerCount,
            @NotNull ObjList<ExpressionNode> keyFunctionNodes,
            RecordMetadata metadata
    ) throws SqlException {
        boolean threadSafe = true;
        for (int i = 0, n = keyFunctions.size(); i < n; i++) {
            if (!keyFunctions.getQuick(i).isThreadSafe()) {
                threadSafe = false;
                break;
            }
        }
        if (!threadSafe) {
            ObjList<ObjList<Function>> allWorkerKeyFunctions = new ObjList<>();
            for (int i = 0; i < workerCount; i++) {
                ObjList<Function> workerKeyFunctions = new ObjList<>(keyFunctionNodes.size());
                allWorkerKeyFunctions.extendAndSet(i, workerKeyFunctions);
                for (int j = 0, n = keyFunctionNodes.size(); j < n; j++) {
                    final Function func = functionParser.parseFunction(
                            keyFunctionNodes.getQuick(j),
                            metadata,
                            executionContext
                    );
                    workerKeyFunctions.add(func);
                }
            }
            return allWorkerKeyFunctions;
        }
        return null;
    }

    private RecordCursorFactory createAsOfJoin(
            RecordMetadata metadata,
            RecordCursorFactory master,
            RecordSink masterKeySink,
            RecordCursorFactory slave,
            RecordSink slaveKeySink,
            int columnSplit,
            JoinContext joinContext
    ) {
        valueTypes.clear();
        valueTypes.add(ColumnType.LONG);
        valueTypes.add(ColumnType.LONG);

        return new AsOfJoinLightRecordCursorFactory(
                configuration,
                metadata,
                master,
                slave,
                keyTypes,
                valueTypes,
                masterKeySink,
                slaveKeySink,
                columnSplit,
                joinContext
        );
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
            JoinContext joinContext
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
                if (ColumnType.isVarSize(slaveMetadata.getColumnType(k))) {
                    throw SqlException
                            .position(joinPosition).put("right side column '")
                            .put(slaveMetadata.getColumnName(k)).put("' is of unsupported type");
                }
            }
        }

        // at this point listColumnFilterB has column indexes of the master record that are JOIN keys
        // so masterSink writes key columns of master record to a sink
        RecordSink masterSink = RecordSinkFactory.getInstance(
                asm,
                masterMetadata,
                listColumnFilterB,
                writeSymbolAsString,
                writeStringAsVarcharB
        );

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
            for (int i = 0, n = slaveMetadata.getColumnCount(); i < n; i++) {
                if (intHashSet.excludes(i)) {
                    // this is not a key column. Add it to metadata as it is. Symbols columns are kept as symbols
                    final TableColumnMetadata m = slaveMetadata.getColumnMetadata(i);
                    metadata.add(slaveAlias, m);
                    listColumnFilterB.add(i + 1);
                    columnIndex.add(i);
                    valueTypes.add(m.getColumnType());
                    slaveTypes.add(m.getColumnType());
                }
            }

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
                    masterSink,
                    RecordSinkFactory.getInstance( // slaveKeySink
                            asm,
                            slaveMetadata,
                            listColumnFilterA,
                            writeSymbolAsString,
                            writeStringAsVarcharA
                    ),
                    masterMetadata.getColumnCount(),
                    RecordValueSinkFactory.getInstance(asm, slaveMetadata, listColumnFilterB), // slaveValueSink
                    columnIndex,
                    joinContext,
                    masterTableKeyColumns
            );

        } catch (Throwable e) {
            Misc.free(metadata);
            throw e;
        }
    }

    private RecordCursorFactory createHashJoin(
            RecordMetadata metadata,
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
        final RecordSink masterKeySink = RecordSinkFactory.getInstance(
                asm,
                masterMetadata,
                listColumnFilterB,
                writeSymbolAsString,
                writeStringAsVarcharB
        );

        final RecordSink slaveKeySink = RecordSinkFactory.getInstance(
                asm,
                slaveMetadata,
                listColumnFilterA,
                writeSymbolAsString,
                writeStringAsVarcharA
        );

        if (slave.recordCursorSupportsRandomAccess() && !fullFatJoins) {
            valueTypes.clear();
            valueTypes.add(ColumnType.INT); // chain tail offset

            if (joinType == JOIN_INNER) {
                // For inner join we can also store per-key count to speed up size calculation.
                valueTypes.add(ColumnType.INT); // record count for the key

                return new HashJoinLightRecordCursorFactory(
                        configuration,
                        metadata,
                        master,
                        slave,
                        keyTypes,
                        valueTypes,
                        masterKeySink,
                        slaveKeySink,
                        masterMetadata.getColumnCount(),
                        context
                );
            }

            if (filter != null) {
                return new HashOuterJoinFilteredLightRecordCursorFactory(
                        configuration,
                        metadata,
                        master,
                        slave,
                        keyTypes,
                        valueTypes,
                        masterKeySink,
                        slaveKeySink,
                        masterMetadata.getColumnCount(),
                        filter,
                        context
                );
            }

            return new HashOuterJoinLightRecordCursorFactory(
                    configuration,
                    metadata,
                    master,
                    slave,
                    keyTypes,
                    valueTypes,
                    masterKeySink,
                    slaveKeySink,
                    masterMetadata.getColumnCount(),
                    context
            );
        }

        valueTypes.clear();
        valueTypes.add(ColumnType.LONG); // chain head offset
        valueTypes.add(ColumnType.LONG); // chain tail offset
        valueTypes.add(ColumnType.LONG); // record count for the key

        entityColumnFilter.of(slaveMetadata.getColumnCount());
        RecordSink slaveSink = RecordSinkFactory.getInstance(asm, slaveMetadata, entityColumnFilter);

        if (joinType == JOIN_INNER) {
            return new HashJoinRecordCursorFactory(
                    configuration,
                    metadata,
                    master,
                    slave,
                    keyTypes,
                    valueTypes,
                    masterKeySink,
                    slaveKeySink,
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
                    masterKeySink,
                    slaveKeySink,
                    slaveSink,
                    masterMetadata.getColumnCount(),
                    filter,
                    context
            );
        }

        return new HashOuterJoinRecordCursorFactory(
                configuration,
                metadata,
                master,
                slave,
                keyTypes,
                valueTypes,
                masterKeySink,
                slaveKeySink,
                slaveSink,
                masterMetadata.getColumnCount(),
                context
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

    private RecordCursorFactory createLtJoin(
            RecordMetadata metadata,
            RecordCursorFactory master,
            RecordSink masterKeySink,
            RecordCursorFactory slave,
            RecordSink slaveKeySink,
            int columnSplit,
            JoinContext joinContext
    ) {
        valueTypes.clear();
        valueTypes.add(ColumnType.LONG);
        valueTypes.add(ColumnType.LONG);

        return new LtJoinLightRecordCursorFactory(
                configuration,
                metadata,
                master,
                slave,
                keyTypes,
                valueTypes,
                masterKeySink,
                slaveKeySink,
                columnSplit,
                joinContext
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
        valueTypes.add(ColumnType.LONG); // master previous
        valueTypes.add(ColumnType.LONG); // master current
        valueTypes.add(ColumnType.LONG); // slave previous
        valueTypes.add(ColumnType.LONG); // slave current

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

    private ObjList<Function> generateCastFunctions(
            RecordMetadata castToMetadata,
            RecordMetadata castFromMetadata,
            int modelPosition
    ) throws SqlException {
        int columnCount = castToMetadata.getColumnCount();
        ObjList<Function> castFunctions = new ObjList<>();
        for (int i = 0; i < columnCount; i++) {
            int toType = castToMetadata.getColumnType(i);
            int fromType = castFromMetadata.getColumnType(i);
            int toTag = ColumnType.tagOf(toType);
            int fromTag = ColumnType.tagOf(fromType);
            if (fromTag == ColumnType.NULL) {
                castFunctions.add(NullConstant.NULL);
            } else {
                switch (toTag) {
                    case ColumnType.BOOLEAN:
                        castFunctions.add(new BooleanColumn(i));
                        break;
                    case ColumnType.BYTE:
                        castFunctions.add(new ByteColumn(i));
                        break;
                    case ColumnType.SHORT:
                        switch (fromTag) {
                            // BOOLEAN will not be cast to CHAR
                            // in cast of BOOLEAN -> CHAR combination both will be cast to STRING
                            case ColumnType.BYTE:
                                castFunctions.add(new ByteColumn(i));
                                break;
                            case ColumnType.CHAR:
                                castFunctions.add(new CharColumn(i));
                                break;
                            case ColumnType.SHORT:
                                castFunctions.add(new ShortColumn(i));
                                break;
                            // wider types are not possible here
                            // SHORT will be cast to wider types, not other way around
                            // Wider types tested are: SHORT, INT, LONG, FLOAT, DOUBLE, DATE, TIMESTAMP, SYMBOL, STRING, LONG256
                            // GEOBYTE, GEOSHORT, GEOINT, GEOLONG
                        }
                        break;
                    case ColumnType.CHAR:
                        switch (fromTag) {
                            // BOOLEAN will not be cast to CHAR
                            // in cast of BOOLEAN -> CHAR combination both will be cast to STRING
                            case ColumnType.BYTE:
                                castFunctions.add(new CastByteToCharFunctionFactory.Func(new ByteColumn(i)));
                                break;
                            case ColumnType.CHAR:
                                castFunctions.add(new CharColumn(i));
                                break;
                            // wider types are not possible here
                            // CHAR will be cast to wider types, not other way around
                            // Wider types tested are: SHORT, INT, LONG, FLOAT, DOUBLE, DATE, TIMESTAMP, SYMBOL, STRING, LONG256
                            // GEOBYTE, GEOSHORT, GEOINT, GEOLONG
                            default:
                        }
                        break;
                    case ColumnType.INT:
                        switch (fromTag) {
                            // BOOLEAN will not be cast to INT
                            // in cast of BOOLEAN -> INT combination both will be cast to STRING
                            case ColumnType.BYTE:
                                castFunctions.add(new ByteColumn(i));
                                break;
                            case ColumnType.SHORT:
                                castFunctions.add(new ShortColumn(i));
                                break;
                            case ColumnType.CHAR:
                                castFunctions.add(new CharColumn(i));
                                break;
                            case ColumnType.INT:
                                castFunctions.add(new IntColumn(i));
                                break;
                            // wider types are not possible here
                            // INT will be cast to wider types, not other way around
                            // Wider types tested are: LONG, FLOAT, DOUBLE, DATE, TIMESTAMP, SYMBOL, STRING, LONG256
                            // GEOBYTE, GEOSHORT, GEOINT, GEOLONG
                        }
                        break;
                    case ColumnType.IPv4:
                        if (fromTag == ColumnType.IPv4) {
                            castFunctions.add(new IPv4Column(i));
                        } else {
                            throw SqlException.unsupportedCast(
                                    modelPosition,
                                    castFromMetadata.getColumnName(i),
                                    fromType,
                                    toType
                            );
                        }
                        break;
                    case ColumnType.LONG:
                        switch (fromTag) {
                            // BOOLEAN will not be cast to LONG
                            // in cast of BOOLEAN -> LONG combination both will be cast to STRING
                            case ColumnType.BYTE:
                                castFunctions.add(new ByteColumn(i));
                                break;
                            case ColumnType.SHORT:
                                castFunctions.add(new ShortColumn(i));
                                break;
                            case ColumnType.CHAR:
                                castFunctions.add(new CharColumn(i));
                                break;
                            case ColumnType.INT:
                                castFunctions.add(new IntColumn(i));
                                break;
                            case ColumnType.LONG:
                                castFunctions.add(new LongColumn(i));
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
                    case ColumnType.DATE:
                        if (fromTag == ColumnType.DATE) {
                            castFunctions.add(new DateColumn(i));
                        } else {
                            throw SqlException.unsupportedCast(
                                    modelPosition,
                                    castFromMetadata.getColumnName(i),
                                    fromType,
                                    toType
                            );
                        }
                        break;
                    case ColumnType.UUID:
                        assert fromTag == ColumnType.UUID;
                        castFunctions.add(new UuidColumn(i));
                        break;
                    case ColumnType.TIMESTAMP:
                        switch (fromTag) {
                            case ColumnType.DATE:
                                castFunctions.add(new CastDateToTimestampFunctionFactory.CastDateToTimestampFunction(new DateColumn(i)));
                                break;
                            case ColumnType.TIMESTAMP:
                                castFunctions.add(new TimestampColumn(i));
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
                    case ColumnType.FLOAT:
                        switch (fromTag) {
                            case ColumnType.BYTE:
                                castFunctions.add(new ByteColumn(i));
                                break;
                            case ColumnType.SHORT:
                                castFunctions.add(new ShortColumn(i));
                                break;
                            case ColumnType.INT:
                                castFunctions.add(new IntColumn(i));
                                break;
                            case ColumnType.LONG:
                                castFunctions.add(new LongColumn(i));
                                break;
                            case ColumnType.FLOAT:
                                castFunctions.add(new FloatColumn(i));
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
                    case ColumnType.DOUBLE:
                        switch (fromTag) {
                            case ColumnType.BYTE:
                                castFunctions.add(new ByteColumn(i));
                                break;
                            case ColumnType.SHORT:
                                castFunctions.add(new ShortColumn(i));
                                break;
                            case ColumnType.INT:
                                castFunctions.add(new IntColumn(i));
                                break;
                            case ColumnType.LONG:
                                castFunctions.add(new LongColumn(i));
                                break;
                            case ColumnType.FLOAT:
                                castFunctions.add(new FloatColumn(i));
                                break;
                            case ColumnType.DOUBLE:
                                castFunctions.add(new DoubleColumn(i));
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
                    case ColumnType.STRING:
                        switch (fromTag) {
                            case ColumnType.BOOLEAN:
                                castFunctions.add(new BooleanColumn(i));
                                break;
                            case ColumnType.BYTE:
                                castFunctions.add(new CastByteToStrFunctionFactory.Func(new ByteColumn(i)));
                                break;
                            case ColumnType.SHORT:
                                castFunctions.add(new CastShortToStrFunctionFactory.Func(new ShortColumn(i)));
                                break;
                            case ColumnType.CHAR:
                                // CharFunction has built-in cast to String
                                castFunctions.add(new CharColumn(i));
                                break;
                            case ColumnType.INT:
                                castFunctions.add(new CastIntToStrFunctionFactory.Func(new IntColumn(i)));
                                break;
                            case ColumnType.LONG:
                                castFunctions.add(new CastLongToStrFunctionFactory.Func(new LongColumn(i)));
                                break;
                            case ColumnType.DATE:
                                castFunctions.add(new CastDateToStrFunctionFactory.Func(new DateColumn(i)));
                                break;
                            case ColumnType.TIMESTAMP:
                                castFunctions.add(new CastTimestampToStrFunctionFactory.Func(new TimestampColumn(i)));
                                break;
                            case ColumnType.FLOAT:
                                castFunctions.add(new CastFloatToStrFunctionFactory.Func(
                                        new FloatColumn(i),
                                        configuration.getFloatToStrCastScale()
                                ));
                                break;
                            case ColumnType.DOUBLE:
                                castFunctions.add(new CastDoubleToStrFunctionFactory.Func(
                                        new DoubleColumn(i),
                                        configuration.getDoubleToStrCastScale()
                                ));
                                break;
                            case ColumnType.STRING:
                                castFunctions.add(new StrColumn(i));
                                break;
                            case ColumnType.VARCHAR:
                                // VarcharFunction has built-in cast to string
                                castFunctions.add(new VarcharColumn(i));
                                break;
                            case ColumnType.UUID:
                                castFunctions.add(new CastUuidToStrFunctionFactory.Func(new UuidColumn(i)));
                                break;
                            case ColumnType.SYMBOL:
                                castFunctions.add(
                                        new CastSymbolToStrFunctionFactory.Func(
                                                new SymbolColumn(i, castFromMetadata.isSymbolTableStatic(i))
                                        )
                                );
                                break;
                            case ColumnType.LONG256:
                                castFunctions.add(
                                        new CastLong256ToStrFunctionFactory.Func(
                                                new Long256Column(i)
                                        )
                                );
                                break;
                            case ColumnType.GEOBYTE:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.getGeoByteToStrCastFunction(
                                                new GeoByteColumn(i, toTag),
                                                getGeoHashBits(fromType)
                                        )
                                );
                                break;
                            case ColumnType.GEOSHORT:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.getGeoShortToStrCastFunction(
                                                new GeoShortColumn(i, toTag),
                                                getGeoHashBits(castFromMetadata.getColumnType(i))
                                        )
                                );
                                break;
                            case ColumnType.GEOINT:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.getGeoIntToStrCastFunction(
                                                new GeoIntColumn(i, toTag),
                                                getGeoHashBits(castFromMetadata.getColumnType(i))
                                        )
                                );
                                break;
                            case ColumnType.GEOLONG:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.getGeoLongToStrCastFunction(
                                                new GeoLongColumn(i, toTag),
                                                getGeoHashBits(castFromMetadata.getColumnType(i))
                                        )
                                );
                                break;
                            case ColumnType.INTERVAL:
                                castFunctions.add(new CastIntervalToStrFunctionFactory.Func(new IntervalColumn(i)));
                                break;
                            case ColumnType.BINARY:
                                throw SqlException.unsupportedCast(
                                        modelPosition,
                                        castFromMetadata.getColumnName(i),
                                        fromType,
                                        toType
                                );
                            case ColumnType.IPv4:
                                castFunctions.add(new CastIPv4ToStrFunctionFactory.Func(new IPv4Column(i)));
                                break;
                        }
                        break;
                    case ColumnType.SYMBOL:
                        castFunctions.add(new CastSymbolToStrFunctionFactory.Func(
                                new SymbolColumn(
                                        i,
                                        castFromMetadata.isSymbolTableStatic(i)
                                )
                        ));
                        break;
                    case ColumnType.LONG256:
                        castFunctions.add(new Long256Column(i));
                        break;
                    case ColumnType.GEOBYTE:
                        switch (fromTag) {
                            case ColumnType.STRING:
                                castFunctions.add(
                                        CastStrToGeoHashFunctionFactory.newInstance(
                                                0,
                                                toType,
                                                new StrColumn(i)
                                        )
                                );
                                break;
                            case ColumnType.VARCHAR:
                                castFunctions.add(
                                        CastVarcharToGeoHashFunctionFactory.newInstance(
                                                0,
                                                toType,
                                                new VarcharColumn(i)
                                        )
                                );
                                break;
                            case ColumnType.GEOBYTE:
                                castFunctions.add(new GeoByteColumn(i, fromType));
                                break;
                            case ColumnType.GEOSHORT:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.newInstance(
                                                0,
                                                new GeoShortColumn(i, fromType),
                                                toType,
                                                fromType
                                        )
                                );
                                break;
                            case ColumnType.GEOINT:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.newInstance(
                                                0,
                                                new GeoIntColumn(i, fromType),
                                                toType,
                                                fromType
                                        )
                                );
                                break;
                            case ColumnType.GEOLONG:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.newInstance(
                                                0,
                                                new GeoLongColumn(i, fromType),
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
                    case ColumnType.GEOSHORT:
                        switch (fromTag) {
                            case ColumnType.STRING:
                                castFunctions.add(
                                        CastStrToGeoHashFunctionFactory.newInstance(
                                                0,
                                                toType,
                                                new StrColumn(i)
                                        )
                                );
                                break;
                            case ColumnType.VARCHAR:
                                castFunctions.add(
                                        CastVarcharToGeoHashFunctionFactory.newInstance(
                                                0,
                                                toType,
                                                new VarcharColumn(i)
                                        )
                                );
                                break;
                            case ColumnType.GEOSHORT:
                                castFunctions.add(new GeoShortColumn(i, toType));
                                break;
                            case ColumnType.GEOINT:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.newInstance(
                                                0,
                                                new GeoIntColumn(i, fromType),
                                                toType,
                                                fromType
                                        )
                                );
                                break;
                            case ColumnType.GEOLONG:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.newInstance(
                                                0,
                                                new GeoLongColumn(i, fromType),
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
                    case ColumnType.GEOINT:
                        switch (fromTag) {
                            case ColumnType.STRING:
                                castFunctions.add(
                                        CastStrToGeoHashFunctionFactory.newInstance(
                                                0,
                                                toType,
                                                new StrColumn(i)
                                        )
                                );
                                break;
                            case ColumnType.VARCHAR:
                                castFunctions.add(
                                        CastVarcharToGeoHashFunctionFactory.newInstance(
                                                0,
                                                toType,
                                                new VarcharColumn(i)
                                        )
                                );
                                break;
                            case ColumnType.GEOINT:
                                castFunctions.add(new GeoIntColumn(i, fromType));
                                break;
                            case ColumnType.GEOLONG:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.newInstance(
                                                0,
                                                new GeoLongColumn(i, fromType),
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
                    case ColumnType.GEOLONG:
                        switch (fromTag) {
                            case ColumnType.STRING:
                                castFunctions.add(
                                        CastStrToGeoHashFunctionFactory.newInstance(
                                                0,
                                                toType,
                                                new StrColumn(i)
                                        )
                                );
                                break;
                            case ColumnType.VARCHAR:
                                castFunctions.add(
                                        CastVarcharToGeoHashFunctionFactory.newInstance(
                                                0,
                                                toType,
                                                new VarcharColumn(i)
                                        )
                                );
                                break;
                            case ColumnType.GEOLONG:
                                castFunctions.add(new GeoLongColumn(i, fromType));
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
                    case ColumnType.BINARY:
                        castFunctions.add(new BinColumn(i));
                        break;
                    case ColumnType.VARCHAR:
                        switch (fromTag) {
                            case ColumnType.BOOLEAN:
                                castFunctions.add(new BooleanColumn(i));
                                break;
                            case ColumnType.BYTE:
                                castFunctions.add(new CastByteToVarcharFunctionFactory.Func(new ByteColumn(i)));
                                break;
                            case ColumnType.SHORT:
                                castFunctions.add(new CastShortToVarcharFunctionFactory.Func(new ShortColumn(i)));
                                break;
                            case ColumnType.CHAR:
                                // CharFunction has built-in cast to varchar
                                castFunctions.add(new CharColumn(i));
                                break;
                            case ColumnType.INT:
                                castFunctions.add(new CastIntToVarcharFunctionFactory.Func(new IntColumn(i)));
                                break;
                            case ColumnType.LONG:
                                castFunctions.add(new CastLongToVarcharFunctionFactory.Func(new LongColumn(i)));
                                break;
                            case ColumnType.DATE:
                                castFunctions.add(new CastDateToVarcharFunctionFactory.Func(new DateColumn(i)));
                                break;
                            case ColumnType.TIMESTAMP:
                                castFunctions.add(new CastTimestampToVarcharFunctionFactory.Func(new TimestampColumn(i)));
                                break;
                            case ColumnType.FLOAT:
                                castFunctions.add(new CastFloatToVarcharFunctionFactory.Func(
                                        new FloatColumn(i),
                                        configuration.getFloatToStrCastScale()
                                ));
                                break;
                            case ColumnType.DOUBLE:
                                castFunctions.add(new CastDoubleToVarcharFunctionFactory.Func(
                                        new DoubleColumn(i),
                                        configuration.getDoubleToStrCastScale()
                                ));
                                break;
                            case ColumnType.STRING:
                                // StrFunction has built-in cast to varchar
                                castFunctions.add(new StrColumn(i));
                                break;
                            case ColumnType.VARCHAR:
                                castFunctions.add(new VarcharColumn(i));
                                break;
                            case ColumnType.UUID:
                                castFunctions.add(new CastUuidToVarcharFunctionFactory.Func(new UuidColumn(i)));
                                break;
                            case ColumnType.IPv4:
                                castFunctions.add(new CastIPv4ToVarcharFunctionFactory.Func(new IPv4Column(i)));
                                break;
                            case ColumnType.SYMBOL:
                                castFunctions.add(
                                        new CastSymbolToVarcharFunctionFactory.Func(
                                                new SymbolColumn(i, castFromMetadata.isSymbolTableStatic(i))
                                        )
                                );
                                break;
                            case ColumnType.LONG256:
                                castFunctions.add(
                                        new CastLong256ToVarcharFunctionFactory.Func(
                                                new Long256Column(i)
                                        )
                                );
                                break;
                            case ColumnType.GEOBYTE:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.getGeoByteToVarcharCastFunction(
                                                new GeoByteColumn(i, toTag),
                                                getGeoHashBits(fromType)
                                        )
                                );
                                break;
                            case ColumnType.GEOSHORT:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.getGeoShortToVarcharCastFunction(
                                                new GeoShortColumn(i, toTag),
                                                getGeoHashBits(castFromMetadata.getColumnType(i))
                                        )
                                );
                                break;
                            case ColumnType.GEOINT:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.getGeoIntToVarcharCastFunction(
                                                new GeoIntColumn(i, toTag),
                                                getGeoHashBits(castFromMetadata.getColumnType(i))
                                        )
                                );
                                break;
                            case ColumnType.GEOLONG:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.getGeoLongToVarcharCastFunction(
                                                new GeoLongColumn(i, toTag),
                                                getGeoHashBits(castFromMetadata.getColumnType(i))
                                        )
                                );
                                break;
                            case ColumnType.BINARY:
                                throw SqlException.unsupportedCast(
                                        modelPosition,
                                        castFromMetadata.getColumnName(i),
                                        fromType,
                                        toType
                                );
                            default:
                                assert false;
                        }
                        break;
                    case ColumnType.INTERVAL:
                        castFunctions.add(new IntervalColumn(i));
                        break;
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
        Function fillFromFunc = TimestampConstant.NULL;
        Function fillToFunc = TimestampConstant.NULL;

        final ExpressionNode fillFrom = curr.getFillFrom();
        final ExpressionNode fillTo = curr.getFillTo();
        final ExpressionNode fillStride = curr.getFillStride();
        ObjList<ExpressionNode> fillValuesExprs = curr.getFillValues();

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

            if (fillFrom != null) {
                fillFromFunc = functionParser.parseFunction(fillFrom, EmptyRecordMetadata.INSTANCE, executionContext);
                coerceRuntimeConstantType(fillFromFunc, ColumnType.TIMESTAMP, executionContext, "from lower bound must be a constant expression convertible to a TIMESTAMP", fillFrom.position);
            }

            if (fillTo != null) {
                fillToFunc = functionParser.parseFunction(fillTo, EmptyRecordMetadata.INSTANCE, executionContext);
                coerceRuntimeConstantType(fillToFunc, ColumnType.TIMESTAMP, executionContext, "to upper bound must be a constant expression convertible to a TIMESTAMP", fillTo.position);
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
                final QueryColumn col = model.getColumns().getQuick(i);
                final ExpressionNode ast = col.getAst();
                if (Chars.equalsIgnoreCase("timestamp_floor", ast.token)) {
                    final CharSequence ts = ast.paramCount == 3 ? ast.args.getQuick(1).token : ast.rhs.token;
                    if (Chars.equals(ts, currTimestamp)) {
                        alias = col.getAlias();
                    }
                }
            }

            int timestampIndex = groupByFactory.getMetadata().getColumnIndexQuiet(alias);

            int samplingIntervalEnd = TimestampSamplerFactory.findIntervalEndIndex(fillStride.token, fillStride.position);
            long samplingInterval = TimestampSamplerFactory.parseInterval(fillStride.token, samplingIntervalEnd, fillStride.position);
            assert samplingInterval > 0;
            assert samplingIntervalEnd < fillStride.token.length();
            char samplingIntervalUnit = fillStride.token.charAt(samplingIntervalEnd);
            TimestampSampler timestampSampler = TimestampSamplerFactory.getInstance(samplingInterval, samplingIntervalUnit, fillStride.position);

            return new FillRangeRecordCursorFactory(
                    groupByFactory.getMetadata(),
                    groupByFactory,
                    fillFromFunc,
                    fillToFunc,
                    samplingInterval,
                    samplingIntervalUnit,
                    timestampSampler,
                    fillValues,
                    timestampIndex
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
        final boolean preTouchColumns = configuration.isSqlParallelFilterPreTouchEnabled();
        if (enableParallelFilter && factory.supportsPageFrameCursor()) {
            final boolean useJit = executionContext.getJitMode() != SqlJitMode.JIT_MODE_DISABLED
                    && (!model.isUpdate() || executionContext.isWalApplication());
            final boolean canCompile = factory.supportsPageFrameCursor() && JitUtil.isJitSupported();
            if (useJit && canCompile) {
                CompiledFilter compiledFilter = null;
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

                    final Function limitLoFunction = getLimitLoFunctionOnly(model, executionContext);
                    final int limitLoPos = model.getLimitAdviceLo() != null ? model.getLimitAdviceLo().position : 0;

                    LOG.debug()
                            .$("JIT enabled for (sub)query [tableName=").utf8(model.getName())
                            .$(", fd=").$(executionContext.getRequestFd())
                            .I$();
                    return new AsyncJitFilteredRecordCursorFactory(
                            configuration,
                            executionContext.getMessageBus(),
                            factory,
                            bindVarFunctions,
                            compiledFilter,
                            filter,
                            reduceTaskFactory,
                            compileWorkerFilterConditionally(
                                    executionContext,
                                    filter,
                                    executionContext.getSharedWorkerCount(),
                                    filterExpr,
                                    factory.getMetadata()
                            ),
                            limitLoFunction,
                            limitLoPos,
                            preTouchColumns,
                            executionContext.getSharedWorkerCount()
                    );
                } catch (SqlException | LimitOverflowException ex) {
                    Misc.free(compiledFilter);
                    LOG.debug()
                            .$("JIT cannot be applied to (sub)query [tableName=").utf8(model.getName())
                            .$(", ex=").$(ex.getFlyweightMessage())
                            .$(", fd=").$(executionContext.getRequestFd()).$(']').$();
                } finally {
                    jitIRSerializer.clear();
                    jitIRMem.truncate();
                }
            }

            // Use Java filter.
            final Function limitLoFunction;
            try {
                limitLoFunction = getLimitLoFunctionOnly(model, executionContext);
                final int limitLoPos = model.getLimitAdviceLo() != null ? model.getLimitAdviceLo().position : 0;
                return new AsyncFilteredRecordCursorFactory(
                        configuration,
                        executionContext.getMessageBus(),
                        factory,
                        filter,
                        reduceTaskFactory,
                        compileWorkerFilterConditionally(
                                executionContext,
                                filter,
                                executionContext.getSharedWorkerCount(),
                                filterExpr,
                                factory.getMetadata()
                        ),
                        limitLoFunction,
                        limitLoPos,
                        preTouchColumns,
                        executionContext.getSharedWorkerCount()
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
        final RecordCursorFactory tableFactory = model.getTableNameFunction();
        if (tableFactory != null) {
            // We're transferring ownership of the tableFactory's factory to another factory
            // setting tableFactory to NULL will prevent double-ownership.
            // We should not release tableFactory itself, they typically just a lightweight factory wrapper.
            model.setTableNameFunction(null);
            return tableFactory;
        } else {
            // when tableFactory is null we have to recompile it from scratch, including creating new factory
            return TableUtils.createCursorFunction(functionParser, model, executionContext).getRecordCursorFactory();
        }
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
            if (ColumnType.isSymbol(columnType)) {
                keyTypes.add(ColumnType.STRING);
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
                writeSymbolAsString
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
                } else { // i == 0
                    // This is first model in the sequence of joins
                    // TS requirement is symmetrical on both right and left sides
                    // check if next join requires a timestamp
                    int nextJointType = joinModels.getQuick(ordered.getQuick(1)).getJoinType();
                    executionContext.pushTimestampRequiredFlag(joinsRequiringTimestamp[nextJointType]);
                }

                RecordCursorFactory slave = null;
                boolean releaseSlave = true;
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
                        releaseSlave = false;
                        masterAlias = slaveModel.getName();
                    } else {
                        // not the root, join to "master"
                        final int joinType = slaveModel.getJoinType();
                        final RecordMetadata masterMetadata = master.getMetadata();
                        final RecordMetadata slaveMetadata = slave.getMetadata();
                        Function filter = null;

                        switch (joinType) {
                            case JOIN_CROSS_LEFT:
                                assert slaveModel.getOuterJoinExpressionClause() != null;
                                joinMetadata = createJoinMetadata(masterAlias, masterMetadata, slaveModel.getName(), slaveMetadata);
                                filter = compileJoinFilter(slaveModel.getOuterJoinExpressionClause(), joinMetadata, executionContext);

                                master = new NestedLoopLeftJoinRecordCursorFactory(
                                        joinMetadata,
                                        master,
                                        slave,
                                        masterMetadata.getColumnCount(),
                                        filter,
                                        NullRecordFactory.getInstance(slaveMetadata)
                                );
                                masterAlias = null;
                                break;
                            case JOIN_CROSS:
                                validateOuterJoinExpressions(slaveModel, "CROSS");
                                master = new CrossJoinRecordCursorFactory(
                                        createJoinMetadata(masterAlias, masterMetadata, slaveModel.getName(), slaveMetadata),
                                        master,
                                        slave,
                                        masterMetadata.getColumnCount()
                                );
                                masterAlias = null;
                                break;
                            case JOIN_ASOF:
                                validateBothTimestamps(slaveModel, masterMetadata, slaveMetadata);
                                validateOuterJoinExpressions(slaveModel, "ASOF");
                                processJoinContext(index == 1, isSameTable(master, slave), slaveModel.getContext(), masterMetadata, slaveMetadata);
                                if (slave.recordCursorSupportsRandomAccess() && !fullFatJoins) {
                                    if (isKeyedTemporalJoin(masterMetadata, slaveMetadata)) {
                                        RecordSink masterSink = RecordSinkFactory.getInstance(
                                                asm,
                                                masterMetadata,
                                                listColumnFilterB,
                                                writeSymbolAsString,
                                                writeStringAsVarcharB
                                        );
                                        RecordSink slaveSink = RecordSinkFactory.getInstance(
                                                asm,
                                                slaveMetadata,
                                                listColumnFilterA,
                                                writeSymbolAsString,
                                                writeStringAsVarcharA
                                        );
                                        if (slave.supportsTimeFrameCursor() && fastAsOfJoins) {
                                            master = new AsOfJoinFastRecordCursorFactory(
                                                    configuration,
                                                    createJoinMetadata(masterAlias, masterMetadata, slaveModel.getName(), slaveMetadata),
                                                    master,
                                                    masterSink,
                                                    slave,
                                                    slaveSink,
                                                    masterMetadata.getColumnCount(),
                                                    slaveModel.getContext()
                                            );
                                        } else {
                                            master = createAsOfJoin(
                                                    createJoinMetadata(masterAlias, masterMetadata, slaveModel.getName(), slaveMetadata),
                                                    master,
                                                    masterSink,
                                                    slave,
                                                    slaveSink,
                                                    masterMetadata.getColumnCount(),
                                                    slaveModel.getContext()
                                            );
                                        }
                                    } else {
                                        if (slave.supportsTimeFrameCursor()) {
                                            master = new AsOfJoinNoKeyFastRecordCursorFactory(
                                                    configuration,
                                                    createJoinMetadata(masterAlias, masterMetadata, slaveModel.getName(), slaveMetadata),
                                                    master,
                                                    slave,
                                                    masterMetadata.getColumnCount()
                                            );
                                        } else {
                                            master = new AsOfJoinNoKeyRecordCursorFactory(
                                                    createJoinMetadata(masterAlias, masterMetadata, slaveModel.getName(), slaveMetadata),
                                                    master,
                                                    slave,
                                                    masterMetadata.getColumnCount()
                                            );
                                        }
                                    }
                                } else {
                                    master = createFullFatJoin(
                                            master,
                                            masterMetadata,
                                            masterAlias,
                                            slave,
                                            slaveMetadata,
                                            slaveModel.getName(),
                                            slaveModel.getJoinKeywordPosition(),
                                            CREATE_FULL_FAT_AS_OF_JOIN,
                                            slaveModel.getContext()
                                    );
                                }
                                masterAlias = null;
                                // if we fail after this step, master will release slave
                                releaseSlave = false;
                                validateBothTimestampOrders(master, slave, slaveModel.getJoinKeywordPosition());
                                break;
                            case JOIN_LT:
                                validateBothTimestamps(slaveModel, masterMetadata, slaveMetadata);
                                validateOuterJoinExpressions(slaveModel, "LT");
                                processJoinContext(index == 1, isSameTable(master, slave), slaveModel.getContext(), masterMetadata, slaveMetadata);
                                if (slave.recordCursorSupportsRandomAccess() && !fullFatJoins) {
                                    if (isKeyedTemporalJoin(masterMetadata, slaveMetadata)) {
                                        master = createLtJoin(
                                                createJoinMetadata(masterAlias, masterMetadata, slaveModel.getName(), slaveMetadata),
                                                master,
                                                RecordSinkFactory.getInstance(
                                                        asm,
                                                        masterMetadata,
                                                        listColumnFilterB,
                                                        writeSymbolAsString,
                                                        writeStringAsVarcharB
                                                ),
                                                slave,
                                                RecordSinkFactory.getInstance(
                                                        asm,
                                                        slaveMetadata,
                                                        listColumnFilterA,
                                                        writeSymbolAsString,
                                                        writeStringAsVarcharA
                                                ),
                                                masterMetadata.getColumnCount(),
                                                slaveModel.getContext()
                                        );
                                    } else {
                                        if (slave.supportsTimeFrameCursor()) {
                                            master = new LtJoinNoKeyFastRecordCursorFactory(
                                                    configuration,
                                                    createJoinMetadata(masterAlias, masterMetadata, slaveModel.getName(), slaveMetadata),
                                                    master,
                                                    slave,
                                                    masterMetadata.getColumnCount()
                                            );
                                        } else {
                                            master = new LtJoinNoKeyRecordCursorFactory(
                                                    createJoinMetadata(masterAlias, masterMetadata, slaveModel.getName(), slaveMetadata),
                                                    master,
                                                    slave,
                                                    masterMetadata.getColumnCount()
                                            );
                                        }
                                    }
                                } else {
                                    master = createFullFatJoin(
                                            master,
                                            masterMetadata,
                                            masterAlias,
                                            slave,
                                            slaveMetadata,
                                            slaveModel.getName(),
                                            slaveModel.getJoinKeywordPosition(),
                                            CREATE_FULL_FAT_LT_JOIN,
                                            slaveModel.getContext()
                                    );
                                }
                                masterAlias = null;
                                // if we fail after this step, master will release slave
                                releaseSlave = false;
                                validateBothTimestampOrders(master, slave, slaveModel.getJoinKeywordPosition());
                                break;
                            case JOIN_SPLICE:
                                validateBothTimestamps(slaveModel, masterMetadata, slaveMetadata);
                                validateOuterJoinExpressions(slaveModel, "SPLICE");
                                processJoinContext(index == 1, isSameTable(master, slave), slaveModel.getContext(), masterMetadata, slaveMetadata);
                                if (slave.recordCursorSupportsRandomAccess() && master.recordCursorSupportsRandomAccess() && !fullFatJoins) {
                                    master = createSpliceJoin(
                                            // splice join result does not have timestamp
                                            createJoinMetadata(masterAlias, masterMetadata, slaveModel.getName(), slaveMetadata, -1),
                                            master,
                                            RecordSinkFactory.getInstance(
                                                    asm,
                                                    masterMetadata,
                                                    listColumnFilterB,
                                                    writeSymbolAsString,
                                                    writeStringAsVarcharB
                                            ),
                                            slave,
                                            RecordSinkFactory.getInstance(
                                                    asm,
                                                    slaveMetadata,
                                                    listColumnFilterA,
                                                    writeSymbolAsString,
                                                    writeStringAsVarcharA
                                            ),
                                            masterMetadata.getColumnCount(),
                                            slaveModel.getContext()
                                    );
                                    // if we fail after this step, master will release slave
                                    releaseSlave = false;
                                    validateBothTimestampOrders(master, slave, slaveModel.getJoinKeywordPosition());
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
                            default:
                                processJoinContext(index == 1, isSameTable(master, slave), slaveModel.getContext(), masterMetadata, slaveMetadata);

                                joinMetadata = createJoinMetadata(masterAlias, masterMetadata, slaveModel.getName(), slaveMetadata);
                                if (slaveModel.getOuterJoinExpressionClause() != null) {
                                    filter = compileJoinFilter(slaveModel.getOuterJoinExpressionClause(), joinMetadata, executionContext);
                                }

                                if (joinType == JOIN_OUTER
                                        && filter != null && filter.isConstant() && !filter.getBool(null)) {
                                    Misc.free(slave);
                                    slave = new EmptyTableRecordCursorFactory(slaveMetadata);
                                }

                                if (joinType == JOIN_INNER) {
                                    validateOuterJoinExpressions(slaveModel, "INNER");
                                }

                                master = createHashJoin(
                                        joinMetadata,
                                        master,
                                        slave,
                                        joinType,
                                        filter,
                                        slaveModel.getContext()
                                );
                                masterAlias = null;
                                break;
                        }
                    }
                } catch (Throwable th) {
                    Misc.free(joinMetadata);
                    master = Misc.free(master);
                    if (releaseSlave) {
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
                                (JoinRecordMetadata) master.getMetadata(),
                                executionContext
                        );

                        master = new AsyncFilteredRecordCursorFactory(
                                configuration,
                                executionContext.getMessageBus(),
                                master,
                                filter,
                                reduceTaskFactory,
                                compileWorkerFilterConditionally(
                                        executionContext,
                                        filter,
                                        executionContext.getSharedWorkerCount(),
                                        filterExpr,
                                        master.getMetadata()
                                ),
                                null,
                                0,
                                false,
                                executionContext.getSharedWorkerCount()
                        );
                    } else {
                        master = new FilteredRecordCursorFactory(
                                master,
                                compileJoinFilter(filterExpr, (JoinRecordMetadata) master.getMetadata(), executionContext)
                        );
                    }
                }
            }

            // unfortunately we had to go all out to create join metadata
            // now it is time to check if we have constant conditions
            ExpressionNode constFilterExpr = model.getConstWhereClause();
            if (constFilterExpr != null) {
                Function filter = functionParser.parseFunction(constFilterExpr, null, executionContext);
                if (!ColumnType.isBoolean(filter.getType())) {
                    throw SqlException.position(constFilterExpr.position).put("boolean expression expected");
                }
                filter.init(null, executionContext);
                if (filter.isConstant()) {
                    if (!filter.getBool(null)) {
                        // do not copy metadata here
                        // this would have been JoinRecordMetadata, which is new instance anyway
                        // we have to make sure that this metadata is safely transitioned
                        // to empty cursor factory
                        JoinRecordMetadata metadata = (JoinRecordMetadata) master.getMetadata();
                        metadata.incrementRefCount();
                        RecordCursorFactory factory = new EmptyTableRecordCursorFactory(metadata);
                        Misc.free(master);
                        return factory;
                    }
                } else {
                    // make it a post-join filter (same as for post join where clause above)
                    if (executionContext.isParallelFilterEnabled() && master.supportsPageFrameCursor()) {
                        master = new AsyncFilteredRecordCursorFactory(
                                configuration,
                                executionContext.getMessageBus(),
                                master,
                                filter,
                                reduceTaskFactory,
                                compileWorkerFilterConditionally(
                                        executionContext,
                                        filter,
                                        executionContext.getSharedWorkerCount(),
                                        constFilterExpr,
                                        master.getMetadata()
                                ),
                                null,
                                0,
                                false,
                                executionContext.getSharedWorkerCount()
                        );
                    } else {
                        master = new FilteredRecordCursorFactory(master, filter);
                    }
                }
            }
            return master;
        } catch (Throwable e) {
            Misc.free(master);
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
                    RecordSinkFactory.getInstance(asm, metadata, listColumnFilterA),
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
                RecordSinkFactory.getInstance(asm, metadata, listColumnFilterA),
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
            @NotNull LongList prefixes
    ) throws SqlException {
        final PartitionFrameCursorFactory partitionFrameCursorFactory;
        if (intrinsicModel.hasIntervalFilters()) {
            partitionFrameCursorFactory = new IntervalBwdPartitionFrameCursorFactory(
                    tableToken,
                    model.getMetadataVersion(),
                    intrinsicModel.buildIntervalModel(),
                    timestampIndex,
                    GenericRecordMetadata.deepCopyOf(reader.getMetadata())
            );
        } else {
            partitionFrameCursorFactory = new FullBwdPartitionFrameCursorFactory(
                    tableToken,
                    model.getMetadataVersion(),
                    GenericRecordMetadata.deepCopyOf(reader.getMetadata())
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
        if (latestBy.size() > 1 || !ColumnType.isSymbol(metadata.getColumnType(latestByIndex))) {
            boolean symbolKeysOnly = true;
            for (int i = 0, n = keyTypes.getColumnCount(); i < n; i++) {
                symbolKeysOnly &= ColumnType.isSymbol(keyTypes.getColumnType(i));
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
                        RecordSinkFactory.getInstance(asm, metadata, listColumnFilterA),
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
                    RecordSinkFactory.getInstance(asm, metadata, listColumnFilterA),
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

        if (indexed && filter == null) {
            return new LatestByAllIndexedRecordCursorFactory(
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
        if (factory.followedLimitAdvice()) {
            return factory;
        }

        ExpressionNode limitLo = model.getLimitLo();
        ExpressionNode limitHi = model.getLimitHi();

        // we've to check model otherwise we could be skipping limit in outer query that's actually different from the one in inner query!
        if ((limitLo == null && limitHi == null) || (factory.implementsLimit() && model.isLimitImplemented())) {
            return factory;
        }

        try {
            final Function loFunc = getLoFunction(model, executionContext);
            final Function hiFunc = getHiFunction(model, executionContext);

            return new LimitRecordCursorFactory(factory, loFunc, hiFunc);
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
                // column index sign indicates direction
                // therefore 0 index is not allowed
                for (int i = 0; i < orderByColumnCount; i++) {
                    final CharSequence column = orderByColumnNames.getQuick(i);
                    int index = metadata.getColumnIndexQuiet(column);

                    // check if column type is supported
                    final int columnType = metadata.getColumnType(index);
                    if (!ColumnType.isComparable(columnType)) {
                        // find position of offending column
                        ObjList<ExpressionNode> nodes = model.getOrderBy();
                        int position = 0;
                        for (int j = 0, y = nodes.size(); j < y; j++) {
                            if (Chars.equals(column, nodes.getQuick(i).token)) {
                                position = nodes.getQuick(i).position;
                                break;
                            }
                        }
                        throw SqlException.$(position, "unsupported column type: ").put(ColumnType.nameOf(columnType));
                    }

                    // we also maintain unique set of column indexes for better performance
                    if (intHashSet.add(index)) {
                        if (orderByColumnNameToIndexMap.get(column) == ORDER_DIRECTION_DESCENDING) {
                            listColumnFilterA.add(-index - 1);
                        } else {
                            listColumnFilterA.add(index + 1);
                            if (i == 0 && metadata.getColumnType(index) == ColumnType.TIMESTAMP) {
                                orderedByTimestampIndex = index;
                            }
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
                        model.setLimitImplemented(true);
                        int baseCursorTimestampIndex = preSortedByTs ? timestampIndex : -1;
                        if (
                                !preSortedByTs
                                        && loFunc.isConstant()
                                        && hiFunc == null
                                        && listColumnFilterA.size() == 1
                                        && recordCursorFactory.recordCursorSupportsLongTopK()
                        ) {
                            final long lo = loFunc.getLong(null);
                            final int index = listColumnFilterA.getQuick(0);
                            final int columnIndex = (index > 0 ? index : -index) - 1;
                            if (lo > 0 && lo <= Integer.MAX_VALUE && metadata.getColumnType(columnIndex) == ColumnType.LONG) {
                                return new LongTopKRecordCursorFactory(
                                        orderedMetadata,
                                        recordCursorFactory,
                                        columnIndex,
                                        (int) lo,
                                        index > 0
                                );
                            }
                        }
                        return new LimitedSizeSortedLightRecordCursorFactory(
                                configuration,
                                orderedMetadata,
                                recordCursorFactory,
                                recordComparatorCompiler.compile(metadata, listColumnFilterA),
                                loFunc,
                                hiFunc,
                                listColumnFilterA.copy(),
                                baseCursorTimestampIndex
                        );
                    } else {
                        final int columnType = orderedMetadata.getColumnType(firstOrderByColumnIndex);
                        if (configuration.isSqlOrderBySortEnabled()
                                && orderByColumnNames.size() == 1
                                && LongSortedLightRecordCursorFactory.isSupportedColumnType(columnType)) {
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
                                recordComparatorCompiler.compile(metadata, listColumnFilterA),
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
                        RecordSinkFactory.getInstance(asm, orderedMetadata, entityColumnFilter),
                        recordComparatorCompiler.compile(metadata, listColumnFilterA),
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
        return generateLimit(
                generateOrderBy(
                        generateLatestBy(
                                generateFilter(
                                        generateSelect(
                                                model,
                                                executionContext,
                                                processJoins
                                        ),
                                        model,
                                        executionContext
                                ),
                                model
                        ),
                        model,
                        executionContext
                ),
                model,
                executionContext
        );
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
            coerceRuntimeConstantType(timezoneNameFunc, ColumnType.STRING, executionContext, "timezone must be a constant expression of STRING or CHAR type", timezoneNameFuncPos);
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
            coerceRuntimeConstantType(offsetFunc, ColumnType.STRING, executionContext, "offset must be a constant expression of STRING or CHAR type", offsetFuncPos);
        } else {
            offsetFunc = StrConstant.NULL;
            offsetFuncPos = 0;
        }

        if (model.getSampleByFrom() != null) {
            sampleFromFunc = functionParser.parseFunction(model.getSampleByFrom(), EmptyRecordMetadata.INSTANCE, executionContext);
            sampleFromFuncPos = model.getSampleByFrom().position;
            coerceRuntimeConstantType(sampleFromFunc, ColumnType.TIMESTAMP, executionContext, "from lower bound must be a constant expression convertible to a TIMESTAMP", sampleFromFuncPos);
        } else {
            sampleFromFunc = TimestampConstant.NULL;
            sampleFromFuncPos = 0;
        }

        if (model.getSampleByTo() != null) {
            sampleToFunc = functionParser.parseFunction(model.getSampleByTo(), EmptyRecordMetadata.INSTANCE, executionContext);
            sampleToFuncPos = model.getSampleByTo().position;
            coerceRuntimeConstantType(sampleToFunc, ColumnType.TIMESTAMP, executionContext, "to upper bound must be a constant expression convertible to a TIMESTAMP", sampleToFuncPos);
        } else {
            sampleToFunc = TimestampConstant.NULL;
            sampleToFuncPos = 0;
        }

        final boolean isFromTo = sampleFromFunc != TimestampConstant.NULL || sampleToFunc != TimestampConstant.NULL;

        RecordCursorFactory factory = null;
        // We require timestamp with asc order.
        final int timestampIndex;
        // Require timestamp in sub-query when it's not additionally specified as timestamp(col).
        executionContext.pushTimestampRequiredFlag(model.getTimestamp() == null);
        try {
            factory = generateSubQuery(model, executionContext);
            timestampIndex = getTimestampIndex(model, factory);
            if (timestampIndex == -1 || factory.getScanDirection() != RecordCursorFactory.SCAN_DIRECTION_FORWARD) {
                throw SqlException.$(model.getModelPosition(), "base query does not provide ASC order over dedicated TIMESTAMP column");
            }
        } catch (Throwable e) {
            Misc.free(factory);
            throw e;
        } finally {
            executionContext.popTimestampRequiredFlag();
        }

        final RecordMetadata metadata = factory.getMetadata();
        final ObjList<ExpressionNode> sampleByFill = model.getSampleByFill();
        final TimestampSampler timestampSampler;
        final int fillCount = sampleByFill.size();
        try {
            if (sampleByUnits == null) {
                timestampSampler = TimestampSamplerFactory.getInstance(sampleByNode.token, sampleByNode.position);
            } else {
                Function sampleByPeriod = functionParser.parseFunction(
                        sampleByNode,
                        EmptyRecordMetadata.INSTANCE,
                        executionContext
                );
                if (!sampleByPeriod.isConstant() || (sampleByPeriod.getType() != ColumnType.LONG && sampleByPeriod.getType() != ColumnType.INT)) {
                    Misc.free(sampleByPeriod);
                    throw SqlException.$(sampleByNode.position, "sample by period must be a constant expression of INT or LONG type");
                }
                long period = sampleByPeriod.getLong(null);
                sampleByPeriod.close();
                timestampSampler = TimestampSamplerFactory.getInstance(period, sampleByUnits.token, sampleByUnits.position);
            }

            keyTypes.clear();
            valueTypes.clear();
            listColumnFilterA.clear();

            if (fillCount == 1 && Chars.equalsLowerCaseAscii(sampleByFill.getQuick(0).token, "linear")) {
                valueTypes.add(ColumnType.BYTE); // gap flag

                final int columnCount = metadata.getColumnCount();
                final ObjList<GroupByFunction> groupByFunctions = new ObjList<>(columnCount);
                final ObjList<Function> recordFunctions = new ObjList<>(columnCount);
                final GenericRecordMetadata groupByMetadata = new GenericRecordMetadata();
                GroupByUtils.assembleGroupByFunctions(
                        functionParser,
                        sqlNodeStack,
                        model,
                        executionContext,
                        metadata,
                        timestampIndex,
                        false,
                        groupByFunctions,
                        groupByFunctionPositions,
                        recordFunctions,
                        recordFunctionPositions,
                        groupByMetadata,
                        null,
                        null,
                        valueTypes,
                        keyTypes,
                        listColumnFilterA,
                        sampleByFill,
                        validateSampleByFillType
                );

                return new SampleByInterpolateRecordCursorFactory(
                        asm,
                        configuration,
                        factory,
                        groupByMetadata,
                        groupByFunctions,
                        recordFunctions,
                        timestampSampler,
                        model,
                        listColumnFilterA,
                        keyTypes,
                        valueTypes,
                        entityColumnFilter,
                        groupByFunctionPositions,
                        timestampIndex,
                        timezoneNameFunc,
                        timezoneNameFuncPos,
                        offsetFunc,
                        offsetFuncPos
                );
            }

            valueTypes.add(ColumnType.TIMESTAMP); // first value is always timestamp

            final int columnCount = model.getColumns().size();
            final ObjList<GroupByFunction> groupByFunctions = new ObjList<>(columnCount);
            final ObjList<Function> recordFunctions = new ObjList<>(columnCount);
            final GenericRecordMetadata groupByMetadata = new GenericRecordMetadata();
            GroupByUtils.assembleGroupByFunctions(
                    functionParser,
                    sqlNodeStack,
                    model,
                    executionContext,
                    metadata,
                    timestampIndex,
                    false,
                    groupByFunctions,
                    groupByFunctionPositions,
                    recordFunctions,
                    recordFunctionPositions,
                    groupByMetadata,
                    null,
                    null,
                    valueTypes,
                    keyTypes,
                    listColumnFilterA,
                    sampleByFill,
                    validateSampleByFillType
            );

            boolean isFillNone = fillCount == 0 || fillCount == 1 && Chars.equalsLowerCaseAscii(sampleByFill.getQuick(0).token, "none");
            boolean allGroupsFirstLast = isFillNone && allGroupsFirstLastWithSingleSymbolFilter(model, metadata);
            if (allGroupsFirstLast) {
                SingleSymbolFilter symbolFilter = factory.convertToSampleByIndexPageFrameCursorFactory();
                if (symbolFilter != null) {
                    int symbolColIndex = getSampleBySymbolKeyIndex(model, metadata);
                    if (symbolColIndex == -1 || symbolFilter.getColumnIndex() == symbolColIndex) {
                        return new SampleByFirstLastRecordCursorFactory(
                                configuration,
                                factory,
                                timestampSampler,
                                groupByMetadata,
                                model.getColumns(),
                                metadata,
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
                            groupByMetadata,
                            groupByFunctions,
                            recordFunctions,
                            timestampIndex,
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
                        groupByMetadata,
                        groupByFunctions,
                        recordFunctions,
                        timestampIndex,
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
                            groupByMetadata,
                            groupByFunctions,
                            recordFunctions,
                            valueTypes.getColumnCount(),
                            timestampIndex,
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
                        groupByMetadata,
                        groupByFunctions,
                        recordFunctions,
                        timestampSampler,
                        listColumnFilterA,
                        keyTypes,
                        valueTypes,
                        timestampIndex,
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
                            groupByMetadata,
                            groupByFunctions,
                            recordFunctions,
                            recordFunctionPositions,
                            valueTypes.getColumnCount(),
                            timestampIndex,
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
                        groupByMetadata,
                        groupByFunctions,
                        recordFunctions,
                        recordFunctionPositions,
                        timestampIndex,
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
                        groupByMetadata,
                        groupByFunctions,
                        recordFunctions,
                        recordFunctionPositions,
                        valueTypes.getColumnCount(),
                        timestampIndex,
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
                    groupByMetadata,
                    groupByFunctions,
                    recordFunctions,
                    recordFunctionPositions,
                    timestampIndex,
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
        }
    }

    private RecordCursorFactory generateSelect(
            QueryModel model,
            SqlExecutionContext executionContext,
            boolean processJoins
    ) throws SqlException {
        switch (model.getSelectModelType()) {
            case SELECT_MODEL_CHOOSE:
                return generateSelectChoose(model, executionContext);
            case SELECT_MODEL_GROUP_BY:
                return generateSelectGroupBy(model, executionContext);
            case SELECT_MODEL_VIRTUAL:
                return generateSelectVirtual(model, executionContext);
            case SELECT_MODEL_WINDOW:
                return generateSelectWindow(model, executionContext);
            case SELECT_MODEL_DISTINCT:
                return generateSelectDistinct(model, executionContext);
            case SELECT_MODEL_CURSOR:
                return generateSelectCursor(model, executionContext);
            case SELECT_MODEL_SHOW:
                return model.getTableNameFunction();
            default:
                if (model.getJoinModels().size() > 1 && processJoins) {
                    return generateJoins(model, executionContext);
                }
                return generateNoSelect(model, executionContext);
        }
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
                        !Chars.equalsIgnoreCase(metadata.getColumnName(i), qc.getAst().token) ||
                                (qc.getAlias() != null && !Chars.equalsIgnoreCase(qc.getAlias(), qc.getAst().token))
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
            if (executionContext.isTimestampRequired() && (timestampIndex == -1 || factory.getScanDirection() != RecordCursorFactory.SCAN_DIRECTION_FORWARD)) {
                throw SqlException.$(model.getModelPosition(), "ASC order over TIMESTAMP column is required but not provided");
            }
        } catch (Throwable e) {
            Misc.free(factory);
            throw e;
        }

        final IntList columnCrossIndex = new IntList(selectColumnCount);
        final GenericRecordMetadata selectMetadata = new GenericRecordMetadata();
        boolean timestampSet = false;
        for (int i = 0; i < selectColumnCount; i++) {
            final QueryColumn queryColumn = columns.getQuick(i);
            int index = metadata.getColumnIndexQuiet(queryColumn.getAst().token);
            assert index > -1 : "wtf? " + queryColumn.getAst().token;
            columnCrossIndex.add(index);

            if (queryColumn.getAlias() == null) {
                selectMetadata.add(metadata.getColumnMetadata(index));
            } else {
                selectMetadata.add(
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
                selectMetadata.setTimestampIndex(i);
                timestampSet = true;
            }
        }

        if (!timestampSet && executionContext.isTimestampRequired()) {
            TableColumnMetadata colMetadata = metadata.getColumnMetadata(timestampIndex);
            int dot = Chars.indexOf(colMetadata.getColumnName(), '.');
            if (dot > -1) {//remove inner table alias
                selectMetadata.add(
                        new TableColumnMetadata(
                                Chars.toString(colMetadata.getColumnName(), dot + 1, colMetadata.getColumnName().length()),
                                colMetadata.getColumnType(),
                                colMetadata.isSymbolIndexFlag(),
                                colMetadata.getIndexValueBlockCapacity(),
                                colMetadata.isSymbolTableStatic(),
                                metadata
                        )
                );
            } else {
                if (selectMetadata.getColumnIndexQuiet(colMetadata.getColumnName()) < 0) {
                    selectMetadata.add(colMetadata);
                } else {
                    // avoid clashing with other columns using timestamp column name as alias
                    StringSink sink = Misc.getThreadLocalSink();
                    sink.put(colMetadata.getColumnName());
                    int len = sink.length();
                    int sequence = 0;

                    do {
                        sink.trimTo(len);
                        sink.put(sequence++);
                    } while (selectMetadata.getColumnIndexQuiet(sink) > -1);

                    selectMetadata.add(
                            new TableColumnMetadata(
                                    sink.toString(),
                                    colMetadata.getColumnType(),
                                    colMetadata.isSymbolIndexFlag(),
                                    colMetadata.getIndexValueBlockCapacity(),
                                    colMetadata.isSymbolTableStatic(),
                                    metadata
                            )
                    );
                }
            }
            selectMetadata.setTimestampIndex(selectMetadata.getColumnCount() - 1);
            columnCrossIndex.add(timestampIndex);
        }

        return new SelectedRecordCursorFactory(selectMetadata, columnCrossIndex, factory);
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
        QueryModel nested;
        QueryModel twoDeepNested;
        ExpressionNode tableNameEn;

        if (
                model.getColumns().size() == 1
                        && (nested = model.getNestedModel()) != null
                        && model.getNestedModel().getSelectModelType() == SELECT_MODEL_CHOOSE
                        && (twoDeepNested = model.getNestedModel().getNestedModel()) != null
                        && twoDeepNested.getLatestBy().size() == 0
                        && (tableNameEn = twoDeepNested.getTableNameExpr()) != null
                        && tableNameEn.type == LITERAL
                        && twoDeepNested.getWhereClause() == null
                        && twoDeepNested.getJoinModels().size() == 1 // no joins
        ) {
            CharSequence tableName = tableNameEn.token;
            TableToken tableToken = executionContext.getTableToken(tableName);
            try (TableReader reader = executionContext.getReader(tableToken)) {
                QueryColumn queryColumn = nested.getBottomUpColumns().get(0);
                CharSequence physicalColumnName = queryColumn.getAst().token;
                TableReaderMetadata readerMetadata = reader.getMetadata();
                int columnIndex = readerMetadata.getColumnIndex(physicalColumnName);
                int columnType = readerMetadata.getColumnType(columnIndex);

                final GenericRecordMetadata distinctColumnMetadata = new GenericRecordMetadata();
                distinctColumnMetadata.add(readerMetadata.getColumnMetadata(columnIndex));
                if (ColumnType.isSymbol(columnType)) {
                    final RecordCursorFactory factory = generateSubQuery(model.getNestedModel(), executionContext);
                    try {
                        return new DistinctSymbolRecordCursorFactory(engine.getConfiguration(), factory);
                    } catch (Throwable t) {
                        Misc.free(factory);
                        throw t;
                    }
                } else if (columnType == ColumnType.INT) {
                    final RecordCursorFactory factory = generateSubQuery(model.getNestedModel(), executionContext);
                    if (factory.supportsPageFrameCursor()) {
                        try {
                            return new DistinctIntKeyRecordCursorFactory(
                                    engine.getConfiguration(),
                                    factory,
                                    distinctColumnMetadata,
                                    arrayColumnTypes,
                                    tempVaf,
                                    executionContext.getSharedWorkerCount()
                            );
                        } catch (Throwable t) {
                            Misc.free(factory);
                            throw t;
                        }
                    } else {
                        // Shouldn't really happen, we cannot recompile below, QueryModel is changed during compilation
                        Misc.free(factory);
                        throw CairoException.critical(0).put("Optimization error, incorrect path chosen, please contact support.");
                    }
                }
            }
        }

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
            return new DistinctRecordCursorFactory(
                    configuration,
                    factory,
                    entityColumnFilter,
                    asm
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
                    // check if count() was not aliased, if it was, we need to generate new metadata, bummer
                    final RecordMetadata metadata = isCountKeyword(columnName) ? CountRecordCursorFactory.DEFAULT_COUNT_METADATA :
                            new GenericRecordMetadata().add(new TableColumnMetadata(Chars.toString(columnName), ColumnType.LONG));
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
                    if (tableColumn != null && tableColumn.getColumnType() == ColumnType.TIMESTAMP) {
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
                    tempKeyKinds.add(GKK_HOUR_INT);
                    arrayColumnTypes.add(ColumnType.INT);
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

            RecordMetadata metadata = factory.getMetadata();

            boolean enableParallelGroupBy = configuration.isSqlParallelGroupByEnabled();
            // Inspect model for possibility of vector aggregate intrinsics.
            if (enableParallelGroupBy && pageFramingSupported && assembleKeysAndFunctionReferences(columns, metadata, hourIndex)) {
                // Create metadata from everything we've gathered.
                GenericRecordMetadata meta = new GenericRecordMetadata();

                // Start with keys.
                for (int i = 0, n = tempKeyIndex.size(); i < n; i++) {
                    final int indexInThis = tempKeyIndex.getQuick(i);
                    final int indexInBase = tempKeyIndexesInBase.getQuick(i);
                    final int type = arrayColumnTypes.getColumnType(i);

                    if (ColumnType.isSymbol(type)) {
                        meta.add(
                                indexInThis,
                                new TableColumnMetadata(
                                        Chars.toString(columns.getQuick(indexInThis).getName()),
                                        type,
                                        false,
                                        0,
                                        metadata.isSymbolTableStatic(indexInBase),
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
                            executionContext.getSharedWorkerCount()
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
                    return new GroupByNotKeyedVectorRecordCursorFactory(
                            configuration,
                            factory,
                            meta,
                            executionContext.getSharedWorkerCount(),
                            tempVaf
                    );
                }

                if (tempKeyIndexesInBase.size() == 1) {
                    for (int i = 0, n = tempVaf.size(); i < n; i++) {
                        tempVaf.getQuick(i).pushValueTypes(arrayColumnTypes);
                    }

                    if (tempVaf.size() == 0) { // similar to DistinctKeyRecordCursorFactory, handles e.g. select id from tab group by id
                        int keyKind = hourIndex != -1 ? SqlCodeGenerator.GKK_HOUR_INT : SqlCodeGenerator.GKK_VANILLA_INT;
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
                                    configuration,
                                    factory,
                                    meta,
                                    arrayColumnTypes,
                                    executionContext.getSharedWorkerCount(),
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
                // and reset metadata
                metadata = factory.getMetadata();
            }

            final int timestampIndex = getTimestampIndex(model, factory);

            keyTypes.clear();
            valueTypes.clear();
            listColumnFilterA.clear();

            final int columnCount = model.getColumns().size();
            final ObjList<GroupByFunction> groupByFunctions = new ObjList<>(columnCount);
            final ObjList<Function> keyFunctions = new ObjList<>(columnCount);
            final ObjList<ExpressionNode> keyFunctionNodes = new ObjList<>(columnCount);
            final ObjList<Function> recordFunctions = new ObjList<>(columnCount);
            final GenericRecordMetadata groupByMetadata = new GenericRecordMetadata();

            GroupByUtils.assembleGroupByFunctions(
                    functionParser,
                    sqlNodeStack,
                    model,
                    executionContext,
                    metadata,
                    timestampIndex,
                    true,
                    groupByFunctions,
                    groupByFunctionPositions,
                    recordFunctions,
                    recordFunctionPositions,
                    groupByMetadata,
                    keyFunctions,
                    keyFunctionNodes,
                    valueTypes,
                    keyTypes,
                    listColumnFilterA,
                    null,
                    validateSampleByFillType
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
                        assert recordFunctions.size() == groupByFunctions.size();

                        return new AsyncGroupByNotKeyedRecordCursorFactory(
                                asm,
                                configuration,
                                executionContext.getMessageBus(),
                                factory,
                                groupByMetadata,
                                groupByFunctions,
                                compileWorkerGroupByFunctionsConditionally(
                                        executionContext,
                                        model,
                                        groupByFunctions,
                                        executionContext.getSharedWorkerCount(),
                                        factory.getMetadata()
                                ),
                                valueTypesCopy.getColumnCount(),
                                compiledFilter,
                                bindVarMemory,
                                bindVarFunctions,
                                filter,
                                reduceTaskFactory,
                                compileWorkerFilterConditionally(
                                        executionContext,
                                        filter,
                                        executionContext.getSharedWorkerCount(),
                                        locatePotentiallyFurtherNestedWhereClause(nested),
                                        factory.getMetadata()
                                ),
                                executionContext.getSharedWorkerCount()
                        );
                    }

                    guardAgainstFillWithKeyedGroupBy(model, keyTypes);

                    return generateFill(
                            model,
                            new AsyncGroupByRecordCursorFactory(
                                    asm,
                                    configuration,
                                    executionContext.getMessageBus(),
                                    factory,
                                    groupByMetadata,
                                    listColumnFilterCopy,
                                    keyTypesCopy,
                                    valueTypesCopy,
                                    groupByFunctions,
                                    compileWorkerGroupByFunctionsConditionally(
                                            executionContext,
                                            model,
                                            groupByFunctions,
                                            executionContext.getSharedWorkerCount(),
                                            factory.getMetadata()
                                    ),
                                    keyFunctions,
                                    compileWorkerKeyFunctionsConditionally(
                                            executionContext,
                                            keyFunctions,
                                            executionContext.getSharedWorkerCount(),
                                            keyFunctionNodes,
                                            factory.getMetadata()
                                    ),
                                    recordFunctions,
                                    compiledFilter,
                                    bindVarMemory,
                                    bindVarFunctions,
                                    filter,
                                    reduceTaskFactory,
                                    compileWorkerFilterConditionally(
                                            executionContext,
                                            filter,
                                            executionContext.getSharedWorkerCount(),
                                            locatePotentiallyFurtherNestedWhereClause(nested),
                                            factory.getMetadata()
                                    ),
                                    executionContext.getSharedWorkerCount()
                            ),
                            executionContext
                    );
                }
            }

            if (keyTypes.getColumnCount() == 0) {
                assert recordFunctions.size() == groupByFunctions.size();
                return new GroupByNotKeyedRecordCursorFactory(
                        asm,
                        configuration,
                        factory,
                        groupByMetadata,
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
                            groupByMetadata,
                            groupByFunctions,
                            keyFunctions,
                            recordFunctions
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
    private VirtualRecordCursorFactory generateSelectVirtualWithSubQuery(QueryModel model, SqlExecutionContext executionContext, RecordCursorFactory factory) throws SqlException {
        final ObjList<QueryColumn> columns = model.getColumns();
        final int columnCount = columns.size();
        final ObjList<Function> functions = new ObjList<>(columnCount);
        final RecordMetadata metadata = factory.getMetadata();
        final GenericRecordMetadata virtualMetadata = new GenericRecordMetadata();
        try {
            // attempt to preserve timestamp on new data set
            CharSequence timestampColumn;
            final int timestampIndex = metadata.getTimestampIndex();
            if (timestampIndex > -1) {
                timestampColumn = metadata.getColumnName(timestampIndex);
            } else {
                timestampColumn = null;
            }

            for (int i = 0; i < columnCount; i++) {
                final QueryColumn column = columns.getQuick(i);
                final ExpressionNode node = column.getAst();
                if (node.type == LITERAL && Chars.equalsNc(node.token, timestampColumn)) {
                    virtualMetadata.setTimestampIndex(i);
                }

                Function function = functionParser.parseFunction(
                        column.getAst(),
                        metadata,
                        executionContext
                );

                int targetColumnType = -1;
                if (model.isUpdate()) {
                    // Check the type of the column to be updated
                    int columnIndex = model.getUpdateTableColumnNames().indexOf(column.getAlias());
                    targetColumnType = model.getUpdateTableColumnTypes().get(columnIndex);
                }

                // define "undefined" functions as string unless it's update.
                if (function.isUndefined()) {
                    if (!model.isUpdate()) {
                        function.assignType(ColumnType.STRING, executionContext.getBindVariableService());
                    } else {
                        // Set bind variable the type of the column
                        function.assignType(targetColumnType, executionContext.getBindVariableService());
                    }
                }

                int columnType = function.getType();
                if (columnType == ColumnType.CURSOR) {
                    throw SqlException.$(node.position, "cursor function cannot be used as a column [column=").put(column.getAlias()).put(']');
                }

                if (targetColumnType != -1 && targetColumnType != columnType) {
                    // This is an update and the target column does not match with column the update is trying to perform
                    if (ColumnType.isBuiltInWideningCast(function.getType(), targetColumnType)) {
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

                if (columnType == ColumnType.SYMBOL) {
                    if (function instanceof SymbolFunction) {
                        virtualMetadata.add(
                                new TableColumnMetadata(
                                        Chars.toString(column.getAlias()),
                                        function.getType(),
                                        false,
                                        0,
                                        ((SymbolFunction) function).isSymbolTableStatic(),
                                        function.getMetadata()
                                )
                        );
                    } else if (function instanceof NullConstant) {
                        virtualMetadata.add(
                                new TableColumnMetadata(
                                        Chars.toString(column.getAlias()),
                                        ColumnType.SYMBOL,
                                        false,
                                        0,
                                        false,
                                        function.getMetadata()
                                )
                        );
                        // Replace with symbol null constant
                        functions.setQuick(functions.size() - 1, SymbolConstant.NULL);
                    }
                } else {
                    virtualMetadata.add(
                            new TableColumnMetadata(
                                    Chars.toString(column.getAlias()),
                                    columnType,
                                    function.getMetadata()
                            )
                    );
                }
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
                        metadata
                );
                functions.add(timestampFunction);

                // here the base timestamp column name can name-clash with one of the
                // functions, so we have to use bottomUpColumns to lookup alias we should
                // be using. Bottom up column should have our timestamp because optimiser puts it there

                for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
                    QueryColumn qc = model.getBottomUpColumns().getQuick(i);
                    if (qc.getAst().type == LITERAL && Chars.equals(timestampColumn, qc.getAst().token)) {
                        virtualMetadata.setTimestampIndex(virtualMetadata.getColumnCount());
                        virtualMetadata.add(
                                new TableColumnMetadata(
                                        Chars.toString(qc.getAlias()),
                                        timestampFunction.getType(),
                                        timestampFunction.getMetadata()
                                )
                        );
                        break;
                    }
                }
            }
            return new VirtualRecordCursorFactory(virtualMetadata, functions, factory);
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
                if (qc.isWindowColumn()) {
                    final WindowColumn ac = (WindowColumn) qc;
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
                        partitionBySink = RecordSinkFactory.getInstance(asm, keyTypes, entityColumnFilter);
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
                            ac.getRowsLoKindPos(),
                            ac.getRowsHi(),
                            ac.getRowsHiKindPos(),
                            ac.getExclusionKind(),
                            ac.getExclusionKindPos(),
                            baseMetadata.getTimestampIndex(),
                            ac.isIgnoreNulls(),
                            ac.getNullsDescPos()
                    );
                    final Function f;
                    try {
                        f = functionParser.parseFunction(ast, baseMetadata, executionContext);
                        if (!(f instanceof WindowFunction)) {
                            Misc.free(f);
                            throw SqlException.$(ast.position, "non-window function called in window context");
                        }

                        WindowFunction af = (WindowFunction) f;
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
                if (!qc.isWindowColumn()) {
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
                if (qc.isWindowColumn()) {
                    final WindowColumn ac = (WindowColumn) qc;
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
                        partitionBySink = RecordSinkFactory.getInstance(asm, keyTypes, entityColumnFilter);
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
                            ac.getRowsLoKindPos(),
                            ac.getRowsHi(),
                            ac.getRowsHiKindPos(),
                            ac.getExclusionKind(),
                            ac.getExclusionKindPos(),
                            chainMetadata.getTimestampIndex(),
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
                        windowFunction.initRecordComparator(recordComparatorCompiler, chainTypes, order);
                        ObjList<WindowFunction> funcs = groupedWindow.get(order);
                        if (funcs == null) {
                            groupedWindow.put(order, funcs = new ObjList<>());
                        }
                        funcs.add(windowFunction);
                    } else {
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
                windowComparators.add(recordComparatorCompiler.compile(chainTypes, e.key));
                functionGroups.add(e.value);
                keys.add(e.key);
            }

            final RecordSink recordSink = RecordSinkFactory.getInstance(
                    asm,
                    chainTypes,
                    listColumnFilterA,
                    listColumnFilterB,
                    null
            );

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
                        castFunctionsA = generateCastFunctions(unionMetadata, metadataA, positionA);
                        castFunctionsB = generateCastFunctions(unionMetadata, metadataB, positionB);
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
                        castFunctionsA = generateCastFunctions(unionMetadata, metadataA, positionA);
                        castFunctionsB = generateCastFunctions(unionMetadata, metadataB, positionB);
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
                        castFunctionsA = generateCastFunctions(unionMetadata, metadataA, positionA);
                        castFunctionsB = generateCastFunctions(unionMetadata, metadataB, positionB);
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
                        castFunctionsA = generateCastFunctions(unionMetadata, metadataA, positionA);
                        castFunctionsB = generateCastFunctions(unionMetadata, metadataB, positionB);
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
                        castFunctionsA = generateCastFunctions(unionMetadata, metadataA, positionA);
                        castFunctionsB = generateCastFunctions(unionMetadata, metadataB, positionB);
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
                        castFunctionsA = generateCastFunctions(unionMetadata, metadataA, positionA);
                        castFunctionsB = generateCastFunctions(unionMetadata, metadataB, positionB);
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

        final ObjList<QueryColumn> topDownColumns = model.getTopDownColumns();
        final int topDownColumnCount = topDownColumns.size();
        final IntList columnIndexes = new IntList();
        final IntList columnSizeShifts = new IntList();

        // topDownColumnCount can be 0 for 'select count()' queries

        int readerTimestampIndex;
        readerTimestampIndex = getTimestampIndex(model, metadata);

        // Latest by on a table requires the provided timestamp column to be the designated timestamp.
        if (latestBy.size() > 0 && readerTimestampIndex != metadata.getTimestampIndex()) {
            throw SqlException.$(model.getTimestamp().position, "latest by over a table requires designated TIMESTAMP");
        }

        boolean requiresTimestamp = joinsRequiringTimestamp[model.getJoinType()];
        final GenericRecordMetadata myMeta = new GenericRecordMetadata();
        try {
            if (requiresTimestamp) {
                executionContext.pushTimestampRequiredFlag(true);
            }

            boolean contextTimestampRequired = executionContext.isTimestampRequired();
            // some "sample by" queries don't select any cols but needs timestamp col selected
            // for example "select count() from x sample by 1h" implicitly needs timestamp column selected
            if (topDownColumnCount > 0 || contextTimestampRequired || model.isUpdate()) {
                for (int i = 0; i < topDownColumnCount; i++) {
                    int columnIndex = metadata.getColumnIndexQuiet(topDownColumns.getQuick(i).getName());
                    int type = metadata.getColumnType(columnIndex);
                    int typeSize = ColumnType.sizeOf(type);

                    columnIndexes.add(columnIndex);
                    columnSizeShifts.add(Numbers.msb(typeSize));

                    myMeta.add(new TableColumnMetadata(
                            Chars.toString(topDownColumns.getQuick(i).getName()),
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
                        myMeta.setTimestampIndex(myMeta.getColumnCount() - 1);
                    }
                }

                // select timestamp when it is required but not already selected
                if (readerTimestampIndex != -1 && myMeta.getTimestampIndex() == -1 && contextTimestampRequired) {
                    myMeta.add(new TableColumnMetadata(
                            metadata.getColumnName(readerTimestampIndex),
                            metadata.getColumnType(readerTimestampIndex),
                            metadata.getMetadata(readerTimestampIndex)
                    ));
                    myMeta.setTimestampIndex(myMeta.getColumnCount() - 1);

                    columnIndexes.add(readerTimestampIndex);
                    columnSizeShifts.add((Numbers.msb(ColumnType.TIMESTAMP)));
                }
            }
        } finally {
            if (requiresTimestamp) {
                executionContext.popTimestampRequiredFlag();
            }
        }

        if (reader == null) {
            // This is WAL serialisation compilation. We don't need to read data from table
            // and don't need optimisation for query validation.
            return new AbstractRecordCursorFactory(myMeta) {
                @Override
                public boolean recordCursorSupportsRandomAccess() {
                    return false;
                }

                @Override
                public boolean supportsUpdateRowId(TableToken tableToken) {
                    return metadata.getTableToken() == tableToken;
                }
            };
        }

        GenericRecordMetadata dfcFactoryMeta = GenericRecordMetadata.deepCopyOf(metadata);
        final int latestByColumnCount = prepareLatestByColumnIndexes(latestBy, myMeta);
        final TableToken tableToken = metadata.getTableToken();

        final ExpressionNode withinExtracted = whereClauseParser.extractWithin(
                model,
                model.getWhereClause(),
                myMeta,
                functionParser,
                executionContext,
                prefixes
        );

        if (prefixes.size() > 0) {
            if (latestByColumnCount < 1) {
                throw SqlException.$(whereClauseParser.getWithinPosition(), "WITHIN clause requires LATEST BY clause");
            } else {
                for (int i = 0; i < latestByColumnCount; i++) {
                    int idx = listColumnFilterA.getColumnIndexFactored(i);
                    if (!ColumnType.isSymbol(myMeta.getColumnType(idx)) || !myMeta.isColumnIndexed(idx)) {
                        throw SqlException.$(whereClauseParser.getWithinPosition(), "WITHIN clause requires LATEST BY using only indexed symbol columns");
                    }
                }
            }
        }

        model.setWhereClause(withinExtracted);

        // Either this: `ORDER BY timestamp DESC`
        // Or this: `ORDER BY timestamp DESC, side DESC LIMIT 3`
        // In the latter case, we would like to generate a partially sorted cursor in `generateOrderBy`
        // So we need to return a bwd scan here first.
        boolean orderDescendingByDesignatedTimestampOnly = isOrderDescendingByDesignatedTimestampOnly(model);

        boolean shouldGenerateBackwardsScan = orderDescendingByDesignatedTimestampOnly
                || isOrderByStartingWithDescDesignatedTimestampAndLimited(model);

        if (withinExtracted != null) {
            CharSequence preferredKeyColumn = null;
            if (latestByColumnCount == 1) {
                final int latestByIndex = listColumnFilterA.getColumnIndexFactored(0);

                if (ColumnType.isSymbol(myMeta.getColumnType(latestByIndex))) {
                    preferredKeyColumn = latestBy.getQuick(0).token;
                }
            }

            final IntrinsicModel intrinsicModel = whereClauseParser.extract(
                    model,
                    withinExtracted,
                    metadata,
                    preferredKeyColumn,
                    readerTimestampIndex,
                    functionParser,
                    myMeta,
                    executionContext,
                    latestByColumnCount > 1,
                    reader
            );

            // intrinsic parser can collapse where clause when removing parts it can replace
            // need to make sure that filter is updated on the model in case it is processed up the call stack
            //
            // At this juncture filter can use used up by one of the implementations below.
            // We will clear it preemptively. If nothing picks filter up we will set model "where"
            // to the downsized filter
            model.setWhereClause(null);

            if (intrinsicModel.intrinsicValue == IntrinsicModel.FALSE) {
                return new EmptyTableRecordCursorFactory(myMeta);
            }

            PartitionFrameCursorFactory dfcFactory;

            if (latestByColumnCount > 0) {
                Function filter = compileFilter(intrinsicModel, myMeta, executionContext);
                if (filter != null && filter.isConstant() && !filter.getBool(null)) {
                    // 'latest by' clause takes over the latest by nodes, so that the later generateLatestBy() is no-op
                    model.getLatestBy().clear();
                    Misc.free(filter);
                    return new EmptyTableRecordCursorFactory(myMeta);
                }

                if (prefixes.size() > 0 && filter != null) {
                    throw SqlException.$(whereClauseParser.getWithinPosition(), "WITHIN clause doesn't work with filters");
                }

                // a sub-query present in the filter may have used the latest by
                // column index lists, so we need to regenerate them
                prepareLatestByColumnIndexes(latestBy, myMeta);

                return generateLatestByTableQuery(
                        model,
                        reader,
                        myMeta,
                        tableToken,
                        intrinsicModel,
                        filter,
                        executionContext,
                        readerTimestampIndex,
                        columnIndexes,
                        columnSizeShifts,
                        prefixes
                );
            }

            // below code block generates index-based filter
            final boolean intervalHitsOnlyOnePartition;
            if (intrinsicModel.hasIntervalFilters()) {
                RuntimeIntrinsicIntervalModel intervalModel = intrinsicModel.buildIntervalModel();
                if (shouldGenerateBackwardsScan) {
                    dfcFactory = new IntervalBwdPartitionFrameCursorFactory(
                            tableToken,
                            model.getMetadataVersion(),
                            intervalModel,
                            readerTimestampIndex,
                            dfcFactoryMeta
                    );
                } else {
                    dfcFactory = new IntervalFwdPartitionFrameCursorFactory(
                            tableToken,
                            model.getMetadataVersion(),
                            intervalModel,
                            readerTimestampIndex,
                            dfcFactoryMeta
                    );
                }
                intervalHitsOnlyOnePartition = intervalModel.allIntervalsHitOnePartition(reader.getPartitionedBy());
            } else {
                if (shouldGenerateBackwardsScan) {
                    dfcFactory = new FullBwdPartitionFrameCursorFactory(tableToken, model.getMetadataVersion(), dfcFactoryMeta);
                } else {
                    dfcFactory = new FullFwdPartitionFrameCursorFactory(tableToken, model.getMetadataVersion(), dfcFactoryMeta);
                }
                intervalHitsOnlyOnePartition = reader.getPartitionedBy() == PartitionBy.NONE;
            }

            if (intrinsicModel.keyColumn != null) {
                // existence of column would have been already validated
                final int keyColumnIndex = myMeta.getColumnIndexQuiet(intrinsicModel.keyColumn);
                final int nKeyValues = intrinsicModel.keyValueFuncs.size();
                final int nKeyExcludedValues = intrinsicModel.keyExcludedValueFuncs.size();

                if (intrinsicModel.keySubQuery != null) {
                    RecordCursorFactory rcf = null;
                    final Record.CharSequenceFunction func;
                    Function filter;
                    try {
                        rcf = generate(intrinsicModel.keySubQuery, executionContext);
                        func = validateSubQueryColumnAndGetGetter(intrinsicModel, rcf.getMetadata());
                        filter = compileFilter(intrinsicModel, myMeta, executionContext);
                    } catch (Throwable th) {
                        Misc.free(dfcFactory);
                        Misc.free(rcf);
                        throw th;
                    }

                    if (filter != null && filter.isConstant() && !filter.getBool(null)) {
                        Misc.free(dfcFactory);
                        return new EmptyTableRecordCursorFactory(myMeta);
                    }
                    return new FilterOnSubQueryRecordCursorFactory(
                            configuration,
                            myMeta,
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
                            myMeta.setTimestampIndex(-1);
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
                    Function filter = compileFilter(intrinsicModel, myMeta, executionContext);
                    if (filter != null && filter.isConstant()) {
                        try {
                            if (!filter.getBool(null)) {
                                Misc.free(dfcFactory);
                                return new EmptyTableRecordCursorFactory(myMeta);
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
                                    myMeta,
                                    dfcFactory,
                                    orderByKeyColumn || orderByTimestamp,
                                    columnIndexes,
                                    columnSizeShifts,
                                    supportsRandomAccess
                            );
                        }
                        return new PageFrameRecordCursorFactory(
                                configuration,
                                myMeta,
                                dfcFactory,
                                rcf,
                                orderByKeyColumn || orderByTimestamp,
                                filter,
                                false,
                                columnIndexes,
                                columnSizeShifts,
                                supportsRandomAccess
                        );
                    }

                    if (orderByKeyColumn) {
                        myMeta.setTimestampIndex(-1);
                    }

                    return new FilterOnValuesRecordCursorFactory(
                            configuration,
                            myMeta,
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
                        Function filter = compileFilter(intrinsicModel, myMeta, executionContext);
                        if (filter != null && filter.isConstant()) {
                            try {
                                if (!filter.getBool(null)) {
                                    Misc.free(dfcFactory);
                                    return new EmptyTableRecordCursorFactory(myMeta);
                                }
                            } finally {
                                filter = Misc.free(filter);
                            }
                        }

                        return new FilterOnExcludedValuesRecordCursorFactory(
                                configuration,
                                myMeta,
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
                    final int columnIndex = myMeta.getColumnIndexQuiet(model.getOrderByAdvice().getQuick(0).token);
                    assert columnIndex > -1;

                    // this is our kind of column
                    if (myMeta.isColumnIndexed(columnIndex)) {
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
                            myMeta.setTimestampIndex(-1);
                            return new SortedSymbolIndexRecordCursorFactory(
                                    configuration,
                                    myMeta,
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

            RowCursorFactory rowFactory;
            if (shouldGenerateBackwardsScan) {
                rowFactory = new BwdPageFrameRowCursorFactory();
            } else {
                rowFactory = new FwdPageFrameRowCursorFactory();
            }

            model.setWhereClause(intrinsicModel.filter);
            return new PageFrameRecordCursorFactory(
                    configuration,
                    myMeta,
                    dfcFactory,
                    rowFactory,
                    false,
                    null,
                    true,
                    columnIndexes,
                    columnSizeShifts,
                    supportsRandomAccess
            );
        }

        // no where clause
        if (latestByColumnCount == 0) {
            // construct new metadata, which is a copy of what we constructed just above, but
            // in the interest of isolating problems we will only affect this factory

            AbstractPartitionFrameCursorFactory cursorFactory;
            RowCursorFactory rowCursorFactory;

            if (shouldGenerateBackwardsScan) {
                cursorFactory = new FullBwdPartitionFrameCursorFactory(tableToken, model.getMetadataVersion(), dfcFactoryMeta);
                rowCursorFactory = new BwdPageFrameRowCursorFactory();
            } else {
                cursorFactory = new FullFwdPartitionFrameCursorFactory(tableToken, model.getMetadataVersion(), dfcFactoryMeta);
                rowCursorFactory = new FwdPageFrameRowCursorFactory();
            }

            return new PageFrameRecordCursorFactory(
                    configuration,
                    myMeta,
                    cursorFactory,
                    rowCursorFactory,
                    orderDescendingByDesignatedTimestampOnly,
                    null,
                    true,
                    columnIndexes,
                    columnSizeShifts,
                    supportsRandomAccess
            );
        }

        // 'latest by' clause takes over the latest by nodes, so that the later generateLatestBy() is no-op
        model.getLatestBy().clear();

        // listColumnFilterA = latest by column indexes
        if (latestByColumnCount == 1) {
            int latestByColumnIndex = listColumnFilterA.getColumnIndexFactored(0);
            if (myMeta.isColumnIndexed(latestByColumnIndex)) {
                return new LatestByAllIndexedRecordCursorFactory(
                        configuration,
                        myMeta,
                        new FullBwdPartitionFrameCursorFactory(tableToken, model.getMetadataVersion(), dfcFactoryMeta),
                        listColumnFilterA.getColumnIndexFactored(0),
                        columnIndexes,
                        columnSizeShifts,
                        prefixes
                );
            }

            if (ColumnType.isSymbol(myMeta.getColumnType(latestByColumnIndex))
                    && myMeta.isSymbolTableStatic(latestByColumnIndex)) {
                // we have "latest by" symbol column values, but no index
                return new LatestByDeferredListValuesFilteredRecordCursorFactory(
                        configuration,
                        myMeta,
                        new FullBwdPartitionFrameCursorFactory(tableToken, model.getMetadataVersion(), dfcFactoryMeta),
                        latestByColumnIndex,
                        null,
                        columnIndexes,
                        columnSizeShifts
                );
            }
        }

        boolean symbolKeysOnly = true;
        for (int i = 0, n = keyTypes.getColumnCount(); i < n; i++) {
            symbolKeysOnly &= ColumnType.isSymbol(keyTypes.getColumnType(i));
        }
        if (symbolKeysOnly) {
            IntList partitionByColumnIndexes = new IntList(listColumnFilterA.size());
            for (int i = 0, n = listColumnFilterA.size(); i < n; i++) {
                partitionByColumnIndexes.add(listColumnFilterA.getColumnIndexFactored(i));
            }
            return new LatestByAllSymbolsFilteredRecordCursorFactory(
                    configuration,
                    myMeta,
                    new FullBwdPartitionFrameCursorFactory(tableToken, model.getMetadataVersion(), dfcFactoryMeta),
                    RecordSinkFactory.getInstance(asm, myMeta, listColumnFilterA),
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
                myMeta,
                new FullBwdPartitionFrameCursorFactory(tableToken, model.getMetadataVersion(), dfcFactoryMeta),
                RecordSinkFactory.getInstance(asm, myMeta, listColumnFilterA),
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
            if (ColumnType.isSymbol(columnType)) {
                keyTypes.add(ColumnType.STRING);
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
                writeSymbolAsString
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

                if (columnType == ColumnType.SYMBOL) {
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
            if (!ColumnType.isTimestamp(metadata.getColumnType(timestampIndex))) {
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
                && Chars.equalsIgnoreCase(model.getOrderByAdvice().getQuick(0).token,
                model.getTimestamp().token);
    }

    private boolean isOrderByStartingWithDescDesignatedTimestampAndLimited(QueryModel model) {
        return model.getOrderByAdvice().size() > 1
                && model.getTimestamp() != null
                && Chars.equalsIgnoreCase(model.getOrderByAdvice().getQuick(0).token,
                model.getTimestamp().token)
                && model.getLimitLo() != null && !Chars.equals(model.getLimitLo().token, '-');
    }

    private boolean isOrderDescendingByDesignatedTimestampOnly(QueryModel model) {
        return isOrderByDesignatedTimestampOnly(model) &&
                getOrderByDirectionOrDefault(model, 0) == ORDER_DIRECTION_DESCENDING;
    }

    private boolean isSameTable(RecordCursorFactory masterFactory, RecordCursorFactory slaveFactory) {
        return masterFactory.getTableToken() != null && masterFactory.getTableToken().equals(slaveFactory.getTableToken());
    }

    // skips skipped models until finding a WHERE clause
    private ExpressionNode locatePotentiallyFurtherNestedWhereClause(QueryModel model) {
        QueryModel curr = model;
        ExpressionNode expr = curr.getWhereClause();

        while (curr.isSkipped() && expr == null) {
            expr = curr.getWhereClause();
            curr = curr.getNestedModel();
        }

        if (expr == null) {
            expr = curr.getWhereClause();
        }

        return expr;
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
                int dot = Chars.indexOf(columnName, '.');
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
                switch (ColumnType.tagOf(columnType)) {
                    case ColumnType.BOOLEAN:
                    case ColumnType.BYTE:
                    case ColumnType.CHAR:
                    case ColumnType.SHORT:
                    case ColumnType.INT:
                    case ColumnType.IPv4:
                    case ColumnType.LONG:
                    case ColumnType.DATE:
                    case ColumnType.TIMESTAMP:
                    case ColumnType.FLOAT:
                    case ColumnType.DOUBLE:
                    case ColumnType.LONG256:
                    case ColumnType.STRING:
                    case ColumnType.VARCHAR:
                    case ColumnType.SYMBOL:
                    case ColumnType.UUID:
                    case ColumnType.GEOBYTE:
                    case ColumnType.GEOSHORT:
                    case ColumnType.GEOINT:
                    case ColumnType.GEOLONG:
                    case ColumnType.LONG128:
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
            boolean selfJoin,
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
        for (int k = 0, m = listColumnFilterA.getColumnCount(); k < m; k++) {
            // Don't use tagOf(columnType) to compare the types.
            // Key types have too much exactly except SYMBOL and STRING special case
            final int columnIndexA = listColumnFilterA.getColumnIndexFactored(k);
            final int columnIndexB = listColumnFilterB.getColumnIndexFactored(k);
            final int columnTypeA = slaveMetadata.getColumnType(columnIndexA);
            final String columnNameA = slaveMetadata.getColumnName(columnIndexA);
            final int columnTypeB = masterMetadata.getColumnType(columnIndexB);
            final String columnNameB = masterMetadata.getColumnName(columnIndexB);
            if (columnTypeB != columnTypeA && !(ColumnType.isSymbolOrString(columnTypeB) && ColumnType.isSymbolOrString(columnTypeA))) {
                // index in column filter and join context is the same
                throw SqlException.$(jc.aNodes.getQuick(k).position, "join column type mismatch");
            }
            if (ColumnType.isVarchar(columnTypeA) || ColumnType.isVarchar(columnTypeB)) {
                keyTypes.add(ColumnType.VARCHAR);
                if (ColumnType.isVarchar(columnTypeA)) {
                    writeStringAsVarcharB.set(columnIndexB);
                } else {
                    writeStringAsVarcharA.set(columnIndexA);
                }
                writeSymbolAsString.set(columnIndexA);
                writeSymbolAsString.set(columnIndexB);
            } else if (columnTypeB == ColumnType.SYMBOL) {
                if (selfJoin && Chars.equalsIgnoreCase(columnNameA, columnNameB)) {
                    keyTypes.add(ColumnType.SYMBOL);
                } else {
                    keyTypes.add(ColumnType.STRING);
                    writeSymbolAsString.set(columnIndexA);
                    writeSymbolAsString.set(columnIndexB);
                }
            } else if (ColumnType.isString(columnTypeA) || ColumnType.isString(columnTypeB)) {
                keyTypes.add(columnTypeB);
                writeSymbolAsString.set(columnIndexA);
                writeSymbolAsString.set(columnIndexB);
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

        final Function func = functionParser.parseFunction(limit, EmptyRecordMetadata.INSTANCE, executionContext);
        final int type = func.getType();
        if (limitTypes.excludes(type)) {
            throw SqlException.$(limit.position, "invalid type: ").put(ColumnType.nameOf(type));
        }
        return func;
    }

    private IntList toOrderIndices(RecordMetadata m, ObjList<ExpressionNode> orderBy, IntList orderByDirection) throws SqlException {
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
            case ColumnType.STRING:
                return Record.GET_STR;
            case ColumnType.SYMBOL:
                return Record.GET_SYM;
            case ColumnType.VARCHAR:
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
                ColumnFilter masterTableKeyColumns
        );
    }

    @FunctionalInterface
    interface ModelOperator {
        void operate(ObjectPool<ExpressionNode> pool, QueryModel model);
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

            CharSequence tableName = model.getTableName();
            if (tableName != null) {
                sink.meta("table").val(tableName);
            }
            if (factory != null) {
                sink.child(factory);
            }
        }
    }

    static {
        joinsRequiringTimestamp[JOIN_INNER] = false;
        joinsRequiringTimestamp[JOIN_OUTER] = false;
        joinsRequiringTimestamp[JOIN_CROSS] = false;
        joinsRequiringTimestamp[JOIN_ASOF] = true;
        joinsRequiringTimestamp[JOIN_SPLICE] = true;
        joinsRequiringTimestamp[JOIN_LT] = true;
        joinsRequiringTimestamp[JOIN_ONE] = false;
    }

    static {
        limitTypes.add(ColumnType.LONG);
        limitTypes.add(ColumnType.BYTE);
        limitTypes.add(ColumnType.SHORT);
        limitTypes.add(ColumnType.INT);
        limitTypes.add(ColumnType.UNDEFINED);
    }

    static {
        countConstructors.put(ColumnType.DOUBLE, CountDoubleVectorAggregateFunction::new);
        countConstructors.put(ColumnType.INT, CountIntVectorAggregateFunction::new);
        countConstructors.put(ColumnType.LONG, CountLongVectorAggregateFunction::new);
        countConstructors.put(ColumnType.DATE, CountLongVectorAggregateFunction::new);
        countConstructors.put(ColumnType.TIMESTAMP, CountLongVectorAggregateFunction::new);

        sumConstructors.put(ColumnType.DOUBLE, SumDoubleVectorAggregateFunction::new);
        sumConstructors.put(ColumnType.INT, SumIntVectorAggregateFunction::new);
        sumConstructors.put(ColumnType.LONG, SumLongVectorAggregateFunction::new);
        sumConstructors.put(ColumnType.LONG256, SumLong256VectorAggregateFunction::new);
        sumConstructors.put(ColumnType.SHORT, SumShortVectorAggregateFunction::new);

        ksumConstructors.put(ColumnType.DOUBLE, KSumDoubleVectorAggregateFunction::new);
        nsumConstructors.put(ColumnType.DOUBLE, NSumDoubleVectorAggregateFunction::new);

        avgConstructors.put(ColumnType.DOUBLE, AvgDoubleVectorAggregateFunction::new);
        avgConstructors.put(ColumnType.LONG, AvgLongVectorAggregateFunction::new);
        avgConstructors.put(ColumnType.INT, AvgIntVectorAggregateFunction::new);
        avgConstructors.put(ColumnType.SHORT, AvgShortVectorAggregateFunction::new);

        minConstructors.put(ColumnType.DOUBLE, MinDoubleVectorAggregateFunction::new);
        minConstructors.put(ColumnType.LONG, MinLongVectorAggregateFunction::new);
        minConstructors.put(ColumnType.DATE, MinDateVectorAggregateFunction::new);
        minConstructors.put(ColumnType.TIMESTAMP, MinTimestampVectorAggregateFunction::new);
        minConstructors.put(ColumnType.INT, MinIntVectorAggregateFunction::new);
        minConstructors.put(ColumnType.SHORT, MinShortVectorAggregateFunction::new);

        maxConstructors.put(ColumnType.DOUBLE, MaxDoubleVectorAggregateFunction::new);
        maxConstructors.put(ColumnType.LONG, MaxLongVectorAggregateFunction::new);
        maxConstructors.put(ColumnType.DATE, MaxDateVectorAggregateFunction::new);
        maxConstructors.put(ColumnType.TIMESTAMP, MaxTimestampVectorAggregateFunction::new);
        maxConstructors.put(ColumnType.INT, MaxIntVectorAggregateFunction::new);
        maxConstructors.put(ColumnType.SHORT, MaxShortVectorAggregateFunction::new);
    }
}

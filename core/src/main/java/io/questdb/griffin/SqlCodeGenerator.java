/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.cairo.map.RecordValueSink;
import io.questdb.cairo.map.RecordValueSinkFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.griffin.engine.LimitRecordCursorFactory;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.analytic.AnalyticFunction;
import io.questdb.griffin.engine.analytic.CachedAnalyticRecordCursorFactory;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.bind.IndexedParameterLinkFunction;
import io.questdb.griffin.engine.functions.bind.NamedParameterLinkFunction;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.groupby.*;
import io.questdb.griffin.engine.groupby.vect.GroupByRecordCursorFactory;
import io.questdb.griffin.engine.groupby.vect.*;
import io.questdb.griffin.engine.join.*;
import io.questdb.griffin.engine.orderby.LimitedSizeSortedLightRecordCursorFactory;
import io.questdb.griffin.engine.orderby.RecordComparatorCompiler;
import io.questdb.griffin.engine.orderby.SortedLightRecordCursorFactory;
import io.questdb.griffin.engine.orderby.SortedRecordCursorFactory;
import io.questdb.griffin.engine.table.*;
import io.questdb.griffin.engine.union.*;
import io.questdb.griffin.model.*;
import io.questdb.jit.CompiledFilter;
import io.questdb.jit.CompiledFilterIRSerializer;
import io.questdb.jit.JitUtil;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

import static io.questdb.cairo.sql.DataFrameCursorFactory.ORDER_ANY;
import static io.questdb.griffin.SqlKeywords.*;
import static io.questdb.griffin.model.ExpressionNode.FUNCTION;
import static io.questdb.griffin.model.ExpressionNode.LITERAL;
import static io.questdb.griffin.model.QueryModel.*;

public class SqlCodeGenerator implements Mutable, Closeable {
    public static final int GKK_VANILLA_INT = 0;
    public static final int GKK_HOUR_INT = 1;
    private static final Log LOG = LogFactory.getLog(SqlCodeGenerator.class);
    private static final IntHashSet limitTypes = new IntHashSet();
    private static final FullFatJoinGenerator CREATE_FULL_FAT_LT_JOIN = SqlCodeGenerator::createFullFatLtJoin;
    private static final FullFatJoinGenerator CREATE_FULL_FAT_AS_OF_JOIN = SqlCodeGenerator::createFullFatAsOfJoin;
    private static final boolean[] joinsRequiringTimestamp = new boolean[JOIN_MAX + 1];
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> sumConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> ksumConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> nsumConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> avgConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> minConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> maxConstructors = new IntObjHashMap<>();
    private static final VectorAggregateFunctionConstructor COUNT_CONSTRUCTOR = (keyKind, columnIndex, workerCount) -> new CountVectorAggregateFunction(keyKind);
    private static final SetRecordCursorFactoryConstructor SET_UNION_CONSTRUCTOR = UnionRecordCursorFactory::new;
    private static final SetRecordCursorFactoryConstructor SET_INTERSECT_CONSTRUCTOR = IntersectRecordCursorFactory::new;
    private static final SetRecordCursorFactoryConstructor SET_EXCEPT_CONSTRUCTOR = ExceptRecordCursorFactory::new;
    private final WhereClauseParser whereClauseParser = new WhereClauseParser();
    private final CompiledFilterIRSerializer jitIRSerializer = new CompiledFilterIRSerializer();
    private final MemoryCARW jitIRMem;
    private final boolean enableJitDebug;
    private final FunctionParser functionParser;
    private final CairoEngine engine;
    private final BytecodeAssembler asm = new BytecodeAssembler();
    // this list is used to generate record sinks
    private final ListColumnFilter listColumnFilterA = new ListColumnFilter();
    private final ListColumnFilter listColumnFilterB = new ListColumnFilter();
    private final CairoConfiguration configuration;
    private final RecordComparatorCompiler recordComparatorCompiler;
    private final IntHashSet intHashSet = new IntHashSet();
    private final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
    private final ArrayColumnTypes valueTypes = new ArrayColumnTypes();
    private final EntityColumnFilter entityColumnFilter = new EntityColumnFilter();
    private final ObjList<VectorAggregateFunction> tempVaf = new ObjList<>();
    private final GenericRecordMetadata tempMetadata = new GenericRecordMetadata();
    private final ArrayColumnTypes arrayColumnTypes = new ArrayColumnTypes();
    private final IntList tempKeyIndexesInBase = new IntList();
    private final IntList tempSymbolSkewIndexes = new IntList();
    private final IntList tempKeyIndex = new IntList();
    private final IntList tempAggIndex = new IntList();
    private final ObjList<VectorAggregateFunctionConstructor> tempVecConstructors = new ObjList<>();
    private final IntList tempVecConstructorArgIndexes = new IntList();
    private final IntList tempKeyKinds = new IntList();
    private final ObjObjHashMap<IntList, ObjList<AnalyticFunction>> groupedAnalytic = new ObjObjHashMap<>();
    private final IntList recordFunctionPositions = new IntList();
    private final IntList groupByFunctionPositions = new IntList();
    private final LongList prefixes = new LongList();
    private boolean enableJitNullChecks = true;
    private boolean fullFatJoins = false;
    private final ObjectPool<ExpressionNode> expressionNodePool;
    private final WeakAutoClosableObjectPool<PageFrameReduceTask> reduceTaskPool;

    public SqlCodeGenerator(
            CairoEngine engine,
            CairoConfiguration configuration,
            FunctionParser functionParser,
            ObjectPool<ExpressionNode> expressionNodePool
    ) {
        this.engine = engine;
        this.configuration = configuration;
        this.functionParser = functionParser;
        this.recordComparatorCompiler = new RecordComparatorCompiler(asm);
        this.enableJitDebug = configuration.isSqlJitDebugEnabled();
        this.jitIRMem = Vm.getCARWInstance(configuration.getSqlJitIRMemoryPageSize(),
                configuration.getSqlJitIRMemoryMaxPages(), MemoryTag.NATIVE_JIT);
        // Pre-touch JIT IR memory to avoid false positive memory leak detections.
        jitIRMem.putByte((byte) 0);
        jitIRMem.truncate();
        this.expressionNodePool = expressionNodePool;
        this.reduceTaskPool = new WeakAutoClosableObjectPool<>(
                (p) -> new PageFrameReduceTask(configuration),
                configuration.getPageFrameReduceTaskPoolCapacity()
        );
    }

    @Override
    public void clear() {
        whereClauseParser.clear();
    }

    @Override
    public void close() {
        jitIRMem.close();
        Misc.free(reduceTaskPool);
    }

    @NotNull
    public Function compileFilter(ExpressionNode expr, RecordMetadata metadata, SqlExecutionContext executionContext) throws SqlException {
        final Function filter = functionParser.parseFunction(expr, metadata, executionContext);
        if (ColumnType.isBoolean(filter.getType())) {
            return filter;
        }
        Misc.free(filter);
        throw SqlException.$(expr.position, "boolean expression expected");
    }

    private static RecordCursorFactory createFullFatAsOfJoin(CairoConfiguration configuration,
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
                                                             IntList columnIndex) {
        return new AsOfJoinRecordCursorFactory(configuration, metadata, masterFactory, slaveFactory, mapKeyTypes, mapValueTypes, slaveColumnTypes, masterKeySink, slaveKeySink, columnSplit, slaveValueSink, columnIndex);
    }

    private static RecordCursorFactory createFullFatLtJoin(CairoConfiguration configuration,
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
                                                           IntList columnIndex) {
        return new LtJoinRecordCursorFactory(configuration, metadata, masterFactory, slaveFactory, mapKeyTypes, mapValueTypes, slaveColumnTypes, masterKeySink, slaveKeySink, columnSplit, slaveValueSink, columnIndex);
    }

    private static int getOrderByDirectionOrDefault(QueryModel model, int index) {
        IntList direction = model.getOrderByDirectionAdvice();
        if (index >= direction.size()) {
            return ORDER_DIRECTION_ASCENDING;
        }
        return model.getOrderByDirectionAdvice().getQuick(index);
    }

    private static boolean allGroupsFirstLastWithSingleSymbolFilter(QueryModel model, RecordMetadata metadata) {
        final ObjList<QueryColumn> columns = model.getColumns();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final QueryColumn column = columns.getQuick(i);
            final ExpressionNode node = column.getAst();

            if (node.type != ExpressionNode.LITERAL) {
                ExpressionNode columnAst = column.getAst();
                CharSequence token = columnAst.token;
                if (!SqlKeywords.isFirstKeyword(token) && !SqlKeywords.isLastFunction(token)) {
                    return false;
                }

                if (columnAst.rhs.type != ExpressionNode.LITERAL || metadata.getColumnIndex(columnAst.rhs.token) < 0) {
                    return false;
                }
            }
        }

        return true;
    }

    private VectorAggregateFunctionConstructor assembleFunctionReference(RecordMetadata metadata, ExpressionNode ast) {
        int columnIndex;
        if (ast.type == FUNCTION && ast.paramCount == 1 && SqlKeywords.isSumKeyword(ast.token) && ast.rhs.type == LITERAL) {
            columnIndex = metadata.getColumnIndex(ast.rhs.token);
            tempVecConstructorArgIndexes.add(columnIndex);
            return sumConstructors.get(metadata.getColumnType(columnIndex));
        } else if (ast.type == FUNCTION && ast.paramCount == 0 && SqlKeywords.isCountKeyword(ast.token)) {
            // count() is a no-arg function
            tempVecConstructorArgIndexes.add(-1);
            return COUNT_CONSTRUCTOR;
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
            boolean checkLiterals
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
                if (checkLiterals) {
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
            } else {
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

    private RecordMetadata calculateSetMetadata(RecordMetadata masterMetadata) {
        return GenericRecordMetadata.removeTimestamp(masterMetadata);
    }

    // Check if lo, hi is set and lo >=0 while hi < 0 (meaning - return whole result set except some rows at start and some at the end)
    // because such case can't really be optimized by topN/bottomN
    private boolean canBeOptimized(QueryModel model, SqlExecutionContext context, Function loFunc, Function hiFunc) {
        if (model.getLimitLo() == null && model.getLimitHi() == null) {
            return false;
        }

        if (loFunc != null && loFunc.isConstant() &&
                hiFunc != null && hiFunc.isConstant()) {
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

    @Nullable
    private Function compileFilter(IntrinsicModel intrinsicModel, RecordMetadata readerMeta, SqlExecutionContext executionContext) throws SqlException {
        if (intrinsicModel.filter != null) {
            return compileFilter(intrinsicModel.filter, readerMeta, executionContext);
        }
        return null;
    }

    private RecordCursorFactory createAsOfJoin(
            RecordMetadata metadata,
            RecordCursorFactory master,
            RecordSink masterKeySink,
            RecordCursorFactory slave,
            RecordSink slaveKeySink,
            int columnSplit
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
                columnSplit
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
            FullFatJoinGenerator generator) throws SqlException {

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
                if (ColumnType.isVariableLength(slaveMetadata.getColumnType(k))) {
                    throw SqlException
                            .position(joinPosition).put("right side column '")
                            .put(slaveMetadata.getColumnName(k)).put("' is of unsupported type");
                }
            }
        }

        RecordSink masterSink = RecordSinkFactory.getInstance(
                asm,
                masterMetadata,
                listColumnFilterB,
                true
        );

        JoinRecordMetadata metadata = new JoinRecordMetadata(
                configuration,
                masterMetadata.getColumnCount() + slaveMetadata.getColumnCount()
        );

        // metadata will have master record verbatim
        metadata.copyColumnMetadataFrom(masterAlias, masterMetadata);

        // slave record is split across key and value of map
        // the rationale is not to store columns twice
        // especially when map value does not support variable
        // length types


        final IntList columnIndex = new IntList(slaveMetadata.getColumnCount());
        // In map record value columns go first, so at this stage
        // we add to metadata all slave columns that are not keys.
        // Add same columns to filter while we are in this loop.
        listColumnFilterB.clear();
        valueTypes.clear();
        ArrayColumnTypes slaveTypes = new ArrayColumnTypes();
        if (slaveMetadata instanceof BaseRecordMetadata) {
            for (int i = 0, n = slaveMetadata.getColumnCount(); i < n; i++) {
                if (intHashSet.excludes(i)) {
                    final TableColumnMetadata m = ((BaseRecordMetadata) slaveMetadata).getColumnQuick(i);
                    metadata.add(slaveAlias, m);
                    listColumnFilterB.add(i + 1);
                    columnIndex.add(i);
                    valueTypes.add(m.getType());
                    slaveTypes.add(m.getType());
                }
            }

            // now add key columns to metadata
            for (int i = 0, n = listColumnFilterA.getColumnCount(); i < n; i++) {
                int index = listColumnFilterA.getColumnIndexFactored(i);
                final TableColumnMetadata m = ((BaseRecordMetadata) slaveMetadata).getColumnQuick(index);
                if (ColumnType.isSymbol(m.getType())) {
                    metadata.add(
                            slaveAlias,
                            m.getName(),
                            m.getHash(),
                            ColumnType.STRING,
                            false,
                            0,
                            false,
                            null
                    );
                    slaveTypes.add(ColumnType.STRING);
                } else {
                    metadata.add(slaveAlias, m);
                    slaveTypes.add(m.getType());
                }
                columnIndex.add(index);
            }
        } else {
            for (int i = 0, n = slaveMetadata.getColumnCount(); i < n; i++) {
                if (intHashSet.excludes(i)) {
                    int type = slaveMetadata.getColumnType(i);
                    metadata.add(
                            slaveAlias,
                            slaveMetadata.getColumnName(i),
                            slaveMetadata.getColumnHash(i),
                            type,
                            slaveMetadata.isColumnIndexed(i),
                            slaveMetadata.getIndexValueBlockCapacity(i),
                            slaveMetadata.isSymbolTableStatic(i),
                            slaveMetadata.getMetadata(i)
                    );
                    listColumnFilterB.add(i + 1);
                    columnIndex.add(i);
                    valueTypes.add(type);
                    slaveTypes.add(type);
                }
            }

            // now add key columns to metadata
            for (int i = 0, n = listColumnFilterA.getColumnCount(); i < n; i++) {
                int index = listColumnFilterA.getColumnIndexFactored(i);
                int type = slaveMetadata.getColumnType(index);
                if (ColumnType.isSymbol(type)) {
                    type = ColumnType.STRING;
                }
                metadata.add(
                        slaveAlias,
                        slaveMetadata.getColumnName(index),
                        slaveMetadata.getColumnHash(index),
                        type,
                        slaveMetadata.isColumnIndexed(i),
                        slaveMetadata.getIndexValueBlockCapacity(i),
                        slaveMetadata.isSymbolTableStatic(i),
                        slaveMetadata.getMetadata(i)
                );
                columnIndex.add(index);
                slaveTypes.add(type);
            }
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
                RecordSinkFactory.getInstance(
                        asm,
                        slaveMetadata,
                        listColumnFilterA,
                        true
                ),
                masterMetadata.getColumnCount(),
                RecordValueSinkFactory.getInstance(asm, slaveMetadata, listColumnFilterB),
                columnIndex
        );
    }

    private RecordCursorFactory createHashJoin(
            RecordMetadata metadata,
            RecordCursorFactory master,
            RecordCursorFactory slave,
            int joinType
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
                true
        );

        final RecordSink slaveKeySink = RecordSinkFactory.getInstance(
                asm,
                slaveMetadata,
                listColumnFilterA,
                true
        );

        valueTypes.clear();
        valueTypes.add(ColumnType.LONG);
        valueTypes.add(ColumnType.LONG);

        if (slave.recordCursorSupportsRandomAccess() && !fullFatJoins) {
            if (joinType == JOIN_INNER) {
                return new HashJoinLightRecordCursorFactory(
                        configuration,
                        metadata,
                        master,
                        slave,
                        keyTypes,
                        valueTypes,
                        masterKeySink,
                        slaveKeySink,
                        masterMetadata.getColumnCount()
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
                    masterMetadata.getColumnCount()
            );
        }

        entityColumnFilter.of(slaveMetadata.getColumnCount());
        RecordSink slaveSink = RecordSinkFactory.getInstance(
                asm,
                slaveMetadata,
                entityColumnFilter,
                false
        );

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
                    masterMetadata.getColumnCount()
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
                masterMetadata.getColumnCount()
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

        metadata.copyColumnMetadataFrom(masterAlias, masterMetadata);
        metadata.copyColumnMetadataFrom(slaveAlias, slaveMetadata);

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
            int columnSplit
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
                columnSplit
        );
    }

    private RecordCursorFactory createSpliceJoin(
            RecordMetadata metadata,
            RecordCursorFactory master,
            RecordSink masterKeySink,
            RecordCursorFactory slave,
            RecordSink slaveKeySink,
            int columnSplit
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
                columnSplit
        );
    }

    RecordCursorFactory generate(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        return generateQuery(model, executionContext, true);
    }

    private RecordCursorFactory generateFilter(RecordCursorFactory factory, QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        final ExpressionNode filter = model.getWhereClause();
        return filter == null ? factory : generateFilter0(factory, model, executionContext, filter);
    }

    @NotNull
    private RecordCursorFactory generateFilter0(RecordCursorFactory factory, QueryModel model, SqlExecutionContext executionContext, ExpressionNode filter) throws SqlException {
        model.setWhereClause(null);

        final Function f = compileFilter(filter, factory.getMetadata(), executionContext);
        if (f.isConstant()) {
            try {
                if (f.getBool(null)) {
                    return factory;
                }
                // metadata is always a GenericRecordMetadata instance
                return new EmptyTableRecordCursorFactory(factory.getMetadata());
            } finally {
                f.close();
            }
        }

        final boolean enableParallelFilter = configuration.isSqlParallelFilterEnabled();
        final boolean useJit = executionContext.getJitMode() != SqlJitMode.JIT_MODE_DISABLED;
        if (enableParallelFilter && useJit) {
            final boolean optimize = factory.supportPageFrameCursor() && JitUtil.isJitSupported();
            if (optimize) {
                try {
                    int jitOptions;
                    final ObjList<Function> bindVarFunctions = new ObjList<>();
                    try (PageFrameCursor cursor = factory.getPageFrameCursor(executionContext, ORDER_ANY)) {
                        final boolean forceScalar = executionContext.getJitMode() == SqlJitMode.JIT_MODE_FORCE_SCALAR;
                        jitIRSerializer.of(jitIRMem, executionContext, factory.getMetadata(), cursor, bindVarFunctions);
                        jitOptions = jitIRSerializer.serialize(filter, forceScalar, enableJitDebug, enableJitNullChecks);
                    }

                    final CompiledFilter jitFilter = new CompiledFilter();
                    jitFilter.compile(jitIRMem, jitOptions);

                    final Function limitLoFunction = getLimitLoFunctionOnly(model, executionContext);
                    final int limitLoPos = model.getLimitAdviceLo() != null ? model.getLimitAdviceLo().position : 0;

                    LOG.info()
                            .$("JIT enabled for (sub)query [tableName=").utf8(model.getName())
                            .$(", fd=").$(executionContext.getRequestFd()).$(']').$();
                    return new AsyncJitFilteredRecordCursorFactory(
                            configuration,
                            executionContext.getMessageBus(),
                            factory,
                            bindVarFunctions,
                            f,
                            jitFilter,
                            reduceTaskPool,
                            limitLoFunction,
                            limitLoPos
                    );
                } catch (SqlException | LimitOverflowException ex) {
                    LOG.debug()
                            .$("JIT cannot be applied to (sub)query [tableName=").utf8(model.getName())
                            .$(", ex=").$(ex.getFlyweightMessage())
                            .$(", fd=").$(executionContext.getRequestFd()).$(']').$();
                } finally {
                    jitIRSerializer.clear();
                    jitIRMem.truncate();
                }
            }
        }

        if (enableParallelFilter && factory.supportPageFrameCursor()) {
            final Function limitLoFunction = getLimitLoFunctionOnly(model, executionContext);
            final int limitLoPos = model.getLimitAdviceLo() != null ? model.getLimitAdviceLo().position : 0;
            return new AsyncFilteredRecordCursorFactory(
                    configuration,
                    executionContext.getMessageBus(),
                    factory,
                    f,
                    reduceTaskPool,
                    limitLoFunction,
                    limitLoPos
            );
        }
        return new FilteredRecordCursorFactory(factory, f);
    }

    private RecordCursorFactory generateFunctionQuery(QueryModel model) throws SqlException {
        final Function function = model.getTableNameFunction();
        assert function != null;
        if (!ColumnType.isCursor(function.getType())) {
            throw SqlException.position(model.getTableName().position).put("function must return CURSOR [actual=").put(ColumnType.nameOf(function.getType())).put(']');
        }
        return function.getRecordCursorFactory();
    }

    private RecordCursorFactory generateJoins(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        final ObjList<QueryModel> joinModels = model.getJoinModels();
        IntList ordered = model.getOrderedJoinModels();
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
                        masterAlias = slaveModel.getName();
                    } else {
                        // not the root, join to "master"
                        final int joinType = slaveModel.getJoinType();
                        final RecordMetadata masterMetadata = master.getMetadata();
                        final RecordMetadata slaveMetadata = slave.getMetadata();

                        switch (joinType) {
                            case JOIN_CROSS:
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
                                processJoinContext(index == 1, slaveModel.getContext(), masterMetadata, slaveMetadata);
                                if (slave.recordCursorSupportsRandomAccess() && !fullFatJoins) {
                                    if (listColumnFilterA.size() > 0 && listColumnFilterB.size() > 0) {
                                        master = createAsOfJoin(
                                                createJoinMetadata(masterAlias, masterMetadata, slaveModel.getName(), slaveMetadata),
                                                master,
                                                RecordSinkFactory.getInstance(
                                                        asm,
                                                        masterMetadata,
                                                        listColumnFilterB,
                                                        true
                                                ),
                                                slave,
                                                RecordSinkFactory.getInstance(
                                                        asm,
                                                        slaveMetadata,
                                                        listColumnFilterA,
                                                        true
                                                ),
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
                                } else {
                                    master = createFullFatJoin(
                                            master,
                                            masterMetadata,
                                            masterAlias,
                                            slave,
                                            slaveMetadata,
                                            slaveModel.getName(),
                                            slaveModel.getJoinKeywordPosition(),
                                            CREATE_FULL_FAT_AS_OF_JOIN
                                    );
                                }
                                masterAlias = null;
                                validateBothTimestampOrders(master, slave, slaveModel.getJoinKeywordPosition());
                                break;
                            case JOIN_LT:
                                validateBothTimestamps(slaveModel, masterMetadata, slaveMetadata);
                                processJoinContext(index == 1, slaveModel.getContext(), masterMetadata, slaveMetadata);
                                if (slave.recordCursorSupportsRandomAccess() && !fullFatJoins) {
                                    if (listColumnFilterA.size() > 0 && listColumnFilterB.size() > 0) {
                                        master = createLtJoin(
                                                createJoinMetadata(masterAlias, masterMetadata, slaveModel.getName(), slaveMetadata),
                                                master,
                                                RecordSinkFactory.getInstance(
                                                        asm,
                                                        masterMetadata,
                                                        listColumnFilterB,
                                                        true
                                                ),
                                                slave,
                                                RecordSinkFactory.getInstance(
                                                        asm,
                                                        slaveMetadata,
                                                        listColumnFilterA,
                                                        true
                                                ),
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
                                } else {
                                    master = createFullFatJoin(
                                            master,
                                            masterMetadata,
                                            masterAlias,
                                            slave,
                                            slaveMetadata,
                                            slaveModel.getName(),
                                            slaveModel.getJoinKeywordPosition(),
                                            CREATE_FULL_FAT_LT_JOIN
                                    );
                                }
                                masterAlias = null;
                                validateBothTimestampOrders(master, slave, slaveModel.getJoinKeywordPosition());
                                break;
                            case JOIN_SPLICE:
                                validateBothTimestamps(slaveModel, masterMetadata, slaveMetadata);
                                processJoinContext(index == 1, slaveModel.getContext(), masterMetadata, slaveMetadata);
                                if (slave.recordCursorSupportsRandomAccess() && master.recordCursorSupportsRandomAccess() && !fullFatJoins) {
                                    master = createSpliceJoin(
                                            // splice join result does not have timestamp
                                            createJoinMetadata(masterAlias, masterMetadata, slaveModel.getName(), slaveMetadata, -1),
                                            master,
                                            RecordSinkFactory.getInstance(
                                                    asm,
                                                    masterMetadata,
                                                    listColumnFilterB,
                                                    true
                                            ),
                                            slave,
                                            RecordSinkFactory.getInstance(
                                                    asm,
                                                    slaveMetadata,
                                                    listColumnFilterA,
                                                    true
                                            ),
                                            masterMetadata.getColumnCount()
                                    );
                                    validateBothTimestampOrders(master, slave, slaveModel.getJoinKeywordPosition());
                                } else {
                                    assert false;
                                }
                                break;
                            default:
                                processJoinContext(index == 1, slaveModel.getContext(), masterMetadata, slaveMetadata);
                                master = createHashJoin(
                                        createJoinMetadata(masterAlias, masterMetadata, slaveModel.getName(), slaveMetadata),
                                        master,
                                        slave,
                                        joinType
                                );
                                masterAlias = null;
                                break;
                        }
                    }
                } catch (Throwable th) {
                    master = Misc.free(master);
                    Misc.free(slave);
                    throw th;
                } finally {
                    executionContext.popTimestampRequiredFlag();
                }

                // check if there are post-filters
                ExpressionNode filter = slaveModel.getPostJoinWhereClause();
                if (filter != null) {
                    if (configuration.isSqlParallelFilterEnabled() && master.supportPageFrameCursor()) {
                        master = new AsyncFilteredRecordCursorFactory(
                                configuration,
                                executionContext.getMessageBus(),
                                master,
                                functionParser.parseFunction(filter, master.getMetadata(), executionContext),
                                reduceTaskPool,
                                null,
                                0
                        );
                    } else {
                        master = new FilteredRecordCursorFactory(master, functionParser.parseFunction(filter, master.getMetadata(), executionContext));
                    }
                }
            }

            // unfortunately we had to go all out to create join metadata
            // now it is time to check if we have constant conditions
            ExpressionNode constFilter = model.getConstWhereClause();
            if (constFilter != null) {
                Function function = functionParser.parseFunction(constFilter, null, executionContext);
                if (!function.getBool(null)) {
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
        final int timestampIndex = getTimestampIndex(model, factory);
        if (timestampIndex == -1) {
            Misc.free(factory);
            throw SqlException.$(model.getModelPosition(), "latest by query does not provide dedicated TIMESTAMP column");
        }

        final RecordMetadata metadata = factory.getMetadata();
        prepareLatestByColumnIndexes(latestBy, metadata);

        if (!factory.recordCursorSupportsRandomAccess()) {
            return new LatestByRecordCursorFactory(
                    configuration,
                    factory,
                    RecordSinkFactory.getInstance(asm, metadata, listColumnFilterA, false),
                    keyTypes,
                    timestampIndex
            );
        }

        boolean orderedByTimestampAsc = false;
        final QueryModel nested = model.getNestedModel();
        assert nested != null;
        final LowerCaseCharSequenceIntHashMap orderBy = nested.getOrderHash();
        CharSequence timestampColumn = metadata.getColumnName(timestampIndex);
        if (orderBy.get(timestampColumn) == QueryModel.ORDER_DIRECTION_ASCENDING) {
            // ORDER BY the timestamp column case.
            orderedByTimestampAsc = true;
        } else if (timestampIndex == metadata.getTimestampIndex() && orderBy.size() == 0) {
            // Empty ORDER BY, but the timestamp column in the designated timestamp.
            orderedByTimestampAsc = true;
        }

        return new LatestByLightRecordCursorFactory(
                configuration,
                factory,
                RecordSinkFactory.getInstance(asm, metadata, listColumnFilterA, false),
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
            String tableName,
            IntrinsicModel intrinsicModel,
            Function filter,
            SqlExecutionContext executionContext,
            int timestampIndex,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizes,
            @NotNull LongList prefixes
    ) throws SqlException {
        final DataFrameCursorFactory dataFrameCursorFactory;
        if (intrinsicModel.hasIntervalFilters()) {
            dataFrameCursorFactory = new IntervalBwdDataFrameCursorFactory(engine, tableName, model.getTableId(), model.getTableVersion(), intrinsicModel.buildIntervalModel(), timestampIndex);
        } else {
            dataFrameCursorFactory = new FullBwdDataFrameCursorFactory(engine, tableName, model.getTableId(), model.getTableVersion());
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

        // if there are > 1 columns in the latest by statement we cannot use indexes
        if (latestBy.size() > 1 || !ColumnType.isSymbol(metadata.getColumnType(latestByIndex))) {
            return new LatestByAllFilteredRecordCursorFactory(
                    metadata,
                    configuration,
                    dataFrameCursorFactory,
                    RecordSinkFactory.getInstance(asm, metadata, listColumnFilterA, false),
                    keyTypes,
                    filter,
                    columnIndexes
            );
        }

        if (intrinsicModel.keyColumn != null) {
            // key column must always be the same as latest by column
            assert latestByIndex == metadata.getColumnIndexQuiet(intrinsicModel.keyColumn);

            if (intrinsicModel.keySubQuery != null) {

                final RecordCursorFactory rcf = generate(intrinsicModel.keySubQuery, executionContext);
                final Record.CharSequenceFunction func = validateSubQueryColumnAndGetGetter(intrinsicModel, rcf.getMetadata());

                return new LatestBySubQueryRecordCursorFactory(
                        configuration,
                        metadata,
                        dataFrameCursorFactory,
                        latestByIndex,
                        rcf,
                        filter,
                        indexed,
                        func,
                        columnIndexes
                );
            }

            final int nKeyValues = intrinsicModel.keyValueFuncs.size();
            if (indexed) {

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
                            : symbolMapReader.keyOf(symbolValueFunc.getStr(null));

                    if (filter == null) {
                        if (symbol == SymbolTable.VALUE_NOT_FOUND) {
                            rcf = new LatestByValueDeferredIndexedRowCursorFactory(
                                    columnIndexes.getQuick(latestByIndex),
                                    symbolValueFunc,
                                    false
                            );
                        } else {
                            rcf = new LatestByValueIndexedRowCursorFactory(
                                    columnIndexes.getQuick(latestByIndex),
                                    symbol,
                                    false
                            );
                        }
                        return new DataFrameRecordCursorFactory(
                                configuration,
                                metadata,
                                dataFrameCursorFactory,
                                rcf,
                                false,
                                null,
                                false,
                                columnIndexes,
                                columnSizes
                        );
                    }

                    if (symbol == SymbolTable.VALUE_NOT_FOUND) {
                        return new LatestByValueDeferredIndexedFilteredRecordCursorFactory(
                                metadata,
                                dataFrameCursorFactory,
                                latestByIndex,
                                symbolValueFunc,
                                filter,
                                columnIndexes
                        );
                    }
                    return new LatestByValueIndexedFilteredRecordCursorFactory(
                            metadata,
                            dataFrameCursorFactory,
                            latestByIndex,
                            symbol,
                            filter,
                            columnIndexes
                    );
                }

                return new LatestByValuesIndexedFilteredRecordCursorFactory(
                        configuration,
                        metadata,
                        dataFrameCursorFactory,
                        latestByIndex,
                        intrinsicModel.keyValueFuncs,
                        symbolMapReader,
                        filter,
                        columnIndexes
                );
            }

            assert nKeyValues > 0;

            // we have "latest by" column values, but no index
            final SymbolMapReader symbolMapReader = reader.getSymbolMapReader(columnIndexes.getQuick(latestByIndex));

            if (nKeyValues > 1) {
                return new LatestByDeferredListValuesFilteredRecordCursorFactory(
                        configuration,
                        metadata,
                        dataFrameCursorFactory,
                        latestByIndex,
                        intrinsicModel.keyValueFuncs,
                        filter,
                        columnIndexes
                );
            }

            // we have a single symbol key
            final Function symbolKeyFunc = intrinsicModel.keyValueFuncs.get(0);
            final int symbolKey = symbolKeyFunc.isRuntimeConstant()
                    ? SymbolTable.VALUE_NOT_FOUND
                    : symbolMapReader.keyOf(symbolKeyFunc.getStr(null));
            if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                return new LatestByValueDeferredFilteredRecordCursorFactory(
                        metadata,
                        dataFrameCursorFactory,
                        latestByIndex,
                        symbolKeyFunc,
                        filter,
                        columnIndexes
                );
            }

            return new LatestByValueFilteredRecordCursorFactory(
                    metadata,
                    dataFrameCursorFactory,
                    latestByIndex,
                    symbolKey,
                    filter,
                    columnIndexes
            );
        }
        // we select all values of "latest by" column

        assert intrinsicModel.keyValueFuncs.size() == 0;
        // get the latest rows for all values of "latest by" column

        if (indexed && filter == null) {
            return new LatestByAllIndexedRecordCursorFactory(
                    metadata,
                    configuration,
                    dataFrameCursorFactory,
                    latestByIndex,
                    columnIndexes,
                    prefixes
            );
        } else {
            return new LatestByDeferredListValuesFilteredRecordCursorFactory(
                    configuration,
                    metadata,
                    dataFrameCursorFactory,
                    latestByIndex,
                    filter,
                    columnIndexes
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

        //we've to check model otherwise we could be skipping limit in outer query that's actually different from the one in inner query!
        if ((limitLo == null && limitHi == null) || (factory.implementsLimit() && model.isLimitImplemented())) {
            return factory;
        }

        final Function loFunc = getLoFunction(model, executionContext);
        final Function hiFunc = getHiFunction(model, executionContext);

        return new LimitRecordCursorFactory(factory, loFunc, hiFunc);
    }

    private RecordCursorFactory generateNoSelect(
            QueryModel model,
            SqlExecutionContext executionContext
    ) throws SqlException {
        ExpressionNode tableName = model.getTableName();
        if (tableName != null) {
            if (tableName.type == FUNCTION) {
                return generateFunctionQuery(model);
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
            final LowerCaseCharSequenceIntHashMap orderBy = model.getOrderHash();
            final ObjList<CharSequence> columnNames = orderBy.keys();
            final int orderByColumnCount = columnNames.size();

            if (orderByColumnCount > 0) {

                final RecordMetadata metadata = recordCursorFactory.getMetadata();
                final int timestampIndex = metadata.getTimestampIndex();

                listColumnFilterA.clear();
                intHashSet.clear();

                // column index sign indicates direction
                // therefore 0 index is not allowed
                for (int i = 0; i < orderByColumnCount; i++) {
                    final CharSequence column = columnNames.getQuick(i);
                    int index = metadata.getColumnIndexQuiet(column);

                    // check if column type is supported
                    if (ColumnType.isBinary(metadata.getColumnType(index))) {
                        // find position of offending column

                        ObjList<ExpressionNode> nodes = model.getOrderBy();
                        int position = 0;
                        for (int j = 0, y = nodes.size(); j < y; j++) {
                            if (Chars.equals(column, nodes.getQuick(i).token)) {
                                position = nodes.getQuick(i).position;
                                break;
                            }
                        }
                        throw SqlException.$(position, "unsupported column type: ").put(ColumnType.nameOf(metadata.getColumnType(index)));
                    }

                    // we also maintain unique set of column indexes for better performance
                    if (intHashSet.add(index)) {
                        if (orderBy.get(column) == QueryModel.ORDER_DIRECTION_DESCENDING) {
                            listColumnFilterA.add(-index - 1);
                        } else {
                            listColumnFilterA.add(index + 1);
                        }
                    }
                }

                // if first column index is the same as timestamp of underling record cursor factory
                // we could have two possibilities:
                // 1. if we only have one column to order by - the cursor would already be ordered
                //    by timestamp (either ASC or DESC); we have nothing to do
                // 2. metadata of the new cursor will have the timestamp

                RecordMetadata orderedMetadata;
                if (timestampIndex != -1) {
                    CharSequence column = columnNames.getQuick(0);
                    int index = metadata.getColumnIndexQuiet(column);
                    if (index == timestampIndex) {
                        if (orderByColumnCount == 1) {
                            if (orderBy.get(column) == QueryModel.ORDER_DIRECTION_ASCENDING) {
                                return recordCursorFactory;
                            } else if (orderBy.get(column) == ORDER_DIRECTION_DESCENDING &&
                                    recordCursorFactory.hasDescendingOrder()) {
                                return recordCursorFactory;
                            }
                        }
                    }
                }

                orderedMetadata = GenericRecordMetadata.copyOfSansTimestamp(metadata);

                final Function loFunc = getLoFunction(model, executionContext);
                final Function hiFunc = getHiFunction(model, executionContext);

                if (recordCursorFactory.recordCursorSupportsRandomAccess()) {
                    if (canBeOptimized(model, executionContext, loFunc, hiFunc)) {
                        model.setLimitImplemented(true);
                        return new LimitedSizeSortedLightRecordCursorFactory(
                                configuration,
                                orderedMetadata,
                                recordCursorFactory,
                                recordComparatorCompiler.compile(metadata, listColumnFilterA),
                                loFunc,
                                hiFunc
                        );
                    } else {
                        return new SortedLightRecordCursorFactory(
                                configuration,
                                orderedMetadata,
                                recordCursorFactory,
                                recordComparatorCompiler.compile(metadata, listColumnFilterA)
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
                        RecordSinkFactory.getInstance(
                                asm,
                                orderedMetadata,
                                entityColumnFilter,
                                false
                        ),
                        recordComparatorCompiler.compile(metadata, listColumnFilterA)
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
        executionContext.pushTimestampRequiredFlag(true);
        try {
            final ExpressionNode timezoneName = model.getSampleByTimezoneName();
            final Function timezoneNameFunc;
            final int timezoneNameFuncPos;
            final ExpressionNode offset = model.getSampleByOffset();
            final Function offsetFunc;
            final int offsetFuncPos;

            if (timezoneName != null) {
                timezoneNameFunc = functionParser.parseFunction(
                        timezoneName,
                        EmptyRecordMetadata.INSTANCE,
                        executionContext
                );
                timezoneNameFuncPos = timezoneName.position;
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
            } else {
                offsetFunc = StrConstant.NULL;
                offsetFuncPos = 0;
            }

            final RecordCursorFactory factory = generateSubQuery(model, executionContext);

            // We require timestamp with asc order.
            final int timestampIndex = getTimestampIndex(model, factory);
            if (timestampIndex == -1 || factory.hasDescendingOrder()) {
                Misc.free(factory);
                throw SqlException.$(model.getModelPosition(), "base query does not provide ASC order over dedicated TIMESTAMP column");
            }

            final RecordMetadata metadata = factory.getMetadata();
            final ObjList<ExpressionNode> sampleByFill = model.getSampleByFill();
            final TimestampSampler timestampSampler;
            if (sampleByUnits == null) {
                timestampSampler = TimestampSamplerFactory.getInstance(sampleByNode.token, sampleByNode.position);
            } else {
                Function sampleByPeriod = functionParser.parseFunction(
                        sampleByNode,
                        EmptyRecordMetadata.INSTANCE,
                        executionContext
                );
                if (!sampleByPeriod.isConstant() || (sampleByPeriod.getType() != ColumnType.LONG && sampleByPeriod.getType() != ColumnType.INT)) {
                    sampleByPeriod.close();
                    throw SqlException.$(sampleByNode.position, "sample by period must be a constant expression of INT or LONG type");
                }
                long period = sampleByPeriod.getLong(null);
                sampleByPeriod.close();
                timestampSampler = TimestampSamplerFactory.getInstance(period, sampleByUnits.token, sampleByUnits.position);
            }

            final int fillCount = sampleByFill.size();
            try {
                keyTypes.clear();
                valueTypes.clear();
                listColumnFilterA.clear();

                if (fillCount == 1 && Chars.equalsLowerCaseAscii(sampleByFill.getQuick(0).token, "linear")) {
                    return new SampleByInterpolateRecordCursorFactory(
                            configuration,
                            factory,
                            timestampSampler,
                            model,
                            listColumnFilterA,
                            functionParser,
                            executionContext,
                            asm,
                            keyTypes,
                            valueTypes,
                            entityColumnFilter,
                            recordFunctionPositions,
                            groupByFunctionPositions,
                            timestampIndex
                    );
                }

                final int columnCount = model.getColumns().size();
                final ObjList<GroupByFunction> groupByFunctions = new ObjList<>(columnCount);
                valueTypes.add(ColumnType.TIMESTAMP); // first value is always timestamp

                GroupByUtils.prepareGroupByFunctions(
                        model,
                        metadata,
                        functionParser,
                        executionContext,
                        groupByFunctions,
                        groupByFunctionPositions,
                        valueTypes
                );

                final ObjList<Function> recordFunctions = new ObjList<>(columnCount);
                final GenericRecordMetadata groupByMetadata = new GenericRecordMetadata();

                GroupByUtils.prepareGroupByRecordFunctions(
                        model,
                        metadata,
                        listColumnFilterA,
                        groupByFunctions,
                        groupByFunctionPositions,
                        recordFunctions,
                        recordFunctionPositions,
                        groupByMetadata,
                        keyTypes,
                        valueTypes.getColumnCount(),
                        false,
                        timestampIndex
                );


                boolean isFillNone = fillCount == 0 || fillCount == 1 && Chars.equalsLowerCaseAscii(sampleByFill.getQuick(0).token, "none");
                boolean allGroupsFirstLast = isFillNone && allGroupsFirstLastWithSingleSymbolFilter(model, metadata);
                if (allGroupsFirstLast) {
                    SingleSymbolFilter symbolFilter = factory.convertToSampleByIndexDataFrameCursorFactory();
                    if (symbolFilter != null) {
                        return new SampleByFirstLastRecordCursorFactory(
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
                                configuration.getSampleByIndexSearchPageSize()
                        );
                    }
                }

                if (fillCount == 1 && Chars.equalsLowerCaseAscii(sampleByFill.getQuick(0).token, "prev")) {
                    if (keyTypes.getColumnCount() == 0) {
                        return new SampleByFillPrevNotKeyedRecordCursorFactory(
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
                                offsetFuncPos
                        );
                    }

                    return new SampleByFillPrevRecordCursorFactory(
                            configuration,
                            factory,
                            timestampSampler,
                            listColumnFilterA,
                            asm,
                            keyTypes,
                            valueTypes,
                            groupByMetadata,
                            groupByFunctions,
                            recordFunctions,
                            timestampIndex,
                            timezoneNameFunc,
                            timezoneNameFuncPos,
                            offsetFunc,
                            offsetFuncPos
                    );
                }

                if (isFillNone) {

                    if (keyTypes.getColumnCount() == 0) {
                        // this sample by is not keyed
                        return new SampleByFillNoneNotKeyedRecordCursorFactory(
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
                                offsetFuncPos
                        );
                    }

                    return new SampleByFillNoneRecordCursorFactory(
                            configuration,
                            factory,
                            groupByMetadata,
                            groupByFunctions,
                            recordFunctions,
                            timestampSampler,
                            listColumnFilterA,
                            asm,
                            keyTypes,
                            valueTypes,
                            timestampIndex,
                            timezoneNameFunc,
                            timezoneNameFuncPos,
                            offsetFunc,
                            offsetFuncPos
                    );
                }

                if (fillCount == 1 && isNullKeyword(sampleByFill.getQuick(0).token)) {
                    if (keyTypes.getColumnCount() == 0) {
                        return new SampleByFillNullNotKeyedRecordCursorFactory(
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
                                offsetFuncPos
                        );
                    }

                    return new SampleByFillNullRecordCursorFactory(
                            configuration,
                            factory,
                            timestampSampler,
                            listColumnFilterA,
                            asm,
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
                            offsetFuncPos
                    );
                }

                assert fillCount > 0;

                if (keyTypes.getColumnCount() == 0) {
                    return new SampleByFillValueNotKeyedRecordCursorFactory(
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
                            offsetFuncPos
                    );
                }

                return new SampleByFillValueRecordCursorFactory(
                        configuration,
                        factory,
                        timestampSampler,
                        listColumnFilterA,
                        asm,
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
                        offsetFuncPos
                );
            } catch (Throwable e) {
                factory.close();
                throw e;
            }
        } finally {
            executionContext.popTimestampRequiredFlag();
        }
    }

    private RecordCursorFactory generateSelect(
            QueryModel model,
            SqlExecutionContext executionContext,
            boolean processJoins
    ) throws SqlException {
        switch (model.getSelectModelType()) {
            case QueryModel.SELECT_MODEL_CHOOSE:
                return generateSelectChoose(model, executionContext);
            case QueryModel.SELECT_MODEL_GROUP_BY:
                return generateSelectGroupBy(model, executionContext);
            case QueryModel.SELECT_MODEL_VIRTUAL:
                return generateSelectVirtual(model, executionContext);
            case QueryModel.SELECT_MODEL_ANALYTIC:
                return generateSelectAnalytic(model, executionContext);
            case QueryModel.SELECT_MODEL_DISTINCT:
                return generateSelectDistinct(model, executionContext);
            case QueryModel.SELECT_MODEL_CURSOR:
                return generateSelectCursor(model, executionContext);
            default:
                if (model.getJoinModels().size() > 1 && processJoins) {
                    return generateJoins(model, executionContext);
                }
                return generateNoSelect(model, executionContext);
        }
    }

    private RecordCursorFactory generateSelectAnalytic(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        final RecordCursorFactory base = generateSubQuery(model, executionContext);
        final RecordMetadata baseMetadata = base.getMetadata();
        final ObjList<QueryColumn> columns = model.getColumns();
        final int columnCount = columns.size();
        groupedAnalytic.clear();
        ObjList<AnalyticFunction> naturalOrderFunctions = null;

        valueTypes.clear();
        ArrayColumnTypes chainTypes = valueTypes;
        GenericRecordMetadata chainMetadata = new GenericRecordMetadata();
        GenericRecordMetadata factoryMetadata = new GenericRecordMetadata();

        listColumnFilterA.clear();
        listColumnFilterB.clear();

        // we need two passes over columns because partitionBy and orderBy clauses of
        // the analytical function must reference the metadata of "this" factory.

        // pass #1 assembles metadata of non-analytic columns

        // set of column indexes in the base metadata that has already been added to the main
        // metadata instance
        // todo: reuse this set
        IntHashSet columnSet = new IntHashSet();
        for (int i = 0; i < columnCount; i++) {
            final QueryColumn qc = columns.getQuick(i);
            if (!(qc instanceof AnalyticColumn)) {
                final int columnIndex = baseMetadata.getColumnIndexQuiet(qc.getAst().token);
                final TableColumnMetadata m = BaseRecordMetadata.copyOf(baseMetadata, columnIndex);
                chainMetadata.add(i, m);
                factoryMetadata.add(i, m);
                chainTypes.add(i, m.getType());
                listColumnFilterA.extendAndSet(i, i + 1);
                listColumnFilterB.extendAndSet(i, columnIndex);
                columnSet.add(columnIndex);
            }
        }

        // pass #2 - add remaining base metadata column that are not in columnSet already
        // we need to pay attention to stepping over analytic column slots
        // Chain metadata is assembled in such way that all columns the factory
        // needs to provide are at the beginning of the metadata so the record the factory cursor
        // returns can be chain record, because it chain record is always longer than record needed out of the
        // cursor and relevant columns are 0..n limited by factory metadata

        int addAt = columnCount;
        for (int i = 0, n = baseMetadata.getColumnCount(); i < n; i++) {
            if (columnSet.excludes(i)) {
                final TableColumnMetadata m = BaseRecordMetadata.copyOf(baseMetadata, i);
                chainMetadata.add(addAt, m);
                chainTypes.add(addAt, m.getType());
                listColumnFilterA.extendAndSet(addAt, addAt + 1);
                listColumnFilterB.extendAndSet(addAt, i);
                addAt++;
            }
        }

        // pass #3 assembles analytic column metadata into a list
        // not main metadata to avoid partitionBy functions accidentally looking up
        // analytic columns recursively

        // todo: these ar transient list, we can cache and reuse
        final ObjList<TableColumnMetadata> deferredAnalyticMetadata = new ObjList<>();

        for (int i = 0; i < columnCount; i++) {
            final QueryColumn qc = columns.getQuick(i);
            if (qc instanceof AnalyticColumn) {
                final AnalyticColumn ac = (AnalyticColumn) qc;
                final ExpressionNode ast = qc.getAst();
                if (ast.paramCount > 1) {
                    throw SqlException.$(ast.position, "Too many arguments");
                }

                ObjList<Function> partitionBy = null;
                int psz = ac.getPartitionBy().size();
                if (psz > 0) {
                    partitionBy = new ObjList<>(psz);
                    for (int j = 0; j < psz; j++) {
                        partitionBy.add(
                                functionParser.parseFunction(ac.getPartitionBy().getQuick(j), chainMetadata, executionContext)
                        );
                    }
                }

                final VirtualRecord partitionByRecord;
                final RecordSink partitionBySink;

                if (partitionBy != null) {
                    partitionByRecord = new VirtualRecord(partitionBy);
                    keyTypes.clear();
                    final int partitionByCount = partitionBy.size();

                    for (int j = 0; j < partitionByCount; j++) {
                        keyTypes.add(partitionBy.getQuick(j).getType());
                    }
                    entityColumnFilter.of(partitionByCount);
                    // create sink
                    partitionBySink = RecordSinkFactory.getInstance(
                            asm,
                            keyTypes,
                            entityColumnFilter,
                            false
                    );
                } else {
                    partitionByRecord = null;
                    partitionBySink = null;
                }


                final int osz = ac.getOrderBy().size();
                executionContext.configureAnalyticContext(
                        partitionByRecord,
                        partitionBySink,
                        keyTypes,
                        osz > 0,
                        base.recordCursorSupportsRandomAccess()
                );

                final Function f = functionParser.parseFunction(ac.getAst(), baseMetadata, executionContext);
                // todo: throw an error when non-analytic function is called in analytic context
                assert f instanceof AnalyticFunction;
                AnalyticFunction analyticFunction = (AnalyticFunction) f;

                // analyze order by clause on the current model and optimise out
                // order by on analytic function if it matches the one on the model
                final LowerCaseCharSequenceIntHashMap orderHash = model.getOrderHash();
                boolean dismissOrder;
                if (osz > 0 && orderHash.size() > 0) {
                    dismissOrder = true;
                    for (int j = 0; j < osz; j++) {
                        ExpressionNode node = ac.getOrderBy().getQuick(j);
                        int direction = ac.getOrderByDirection().getQuick(j);
                        if (orderHash.get(node.token) != direction) {
                            dismissOrder = false;
                            break;
                        }
                    }
                } else {
                    dismissOrder = false;
                }

                if (osz > 0 && !dismissOrder) {
                    IntList order = toOrderIndices(chainMetadata, ac.getOrderBy(), ac.getOrderByDirection());
                    ObjList<AnalyticFunction> funcs = groupedAnalytic.get(order);
                    if (funcs == null) {
                        groupedAnalytic.put(order, funcs = new ObjList<>());
                    }
                    funcs.add(analyticFunction);
                } else {
                    if (naturalOrderFunctions == null) {
                        naturalOrderFunctions = new ObjList<>();
                    }
                    naturalOrderFunctions.add(analyticFunction);
                }

                analyticFunction.setColumnIndex(i);

                deferredAnalyticMetadata.extendAndSet(i, new TableColumnMetadata(
                        Chars.toString(qc.getAlias()),
                        0, // transient column hash is 0
                        analyticFunction.getType(),
                        false,
                        0,
                        false,
                        null
                ));

                listColumnFilterA.extendAndSet(i, -i - 1);
            }
        }

        // after all columns are processed we can re-insert deferred metadata
        for (int i = 0, n = deferredAnalyticMetadata.size(); i < n; i++) {
            TableColumnMetadata m = deferredAnalyticMetadata.getQuick(i);
            if (m != null) {
                chainTypes.add(i, m.getType());
                factoryMetadata.add(i, m);
            }
        }

        final ObjList<RecordComparator> analyticComparators = new ObjList<>(groupedAnalytic.size());
        final ObjList<ObjList<AnalyticFunction>> functionGroups = new ObjList<>(groupedAnalytic.size());
        for (ObjObjHashMap.Entry<IntList, ObjList<AnalyticFunction>> e : groupedAnalytic) {
            analyticComparators.add(recordComparatorCompiler.compile(chainTypes, e.key));
            functionGroups.add(e.value);
        }

        final RecordSink recordSink = RecordSinkFactory.getInstance(
                asm,
                chainTypes,
                listColumnFilterA,
                false,
                listColumnFilterB
        );

        return new CachedAnalyticRecordCursorFactory(
                configuration,
                base,
                recordSink,
                factoryMetadata,
                chainTypes,
                analyticComparators,
                functionGroups,
                naturalOrderFunctions
        );
    }

    private RecordCursorFactory generateSelectChoose(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        final RecordCursorFactory factory = generateSubQuery(model, executionContext);

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
                return generateSelectVirtualWithSubquery(model, executionContext, factory);
            }
        }

        boolean entity;
        // the model is considered entity when it doesn't add any value to its nested model
        //
        if (timestamp == null && metadata.getColumnCount() == selectColumnCount) {
            entity = true;
            for (int i = 0; i < selectColumnCount; i++) {
                QueryColumn qc = columns.getQuick(i);
                if (
                        !Chars.equals(metadata.getColumnName(i), qc.getAst().token) ||
                                qc.getAlias() != null && !(Chars.equals(qc.getAlias(), qc.getAst().token))
                ) {
                    entity = false;
                    break;
                }
            }
        } else {
            entity = false;
        }

        if (entity) {
            return factory;
        }

        // We require timestamp with asc order.
        final int timestampIndex = getTimestampIndex(model, factory);
        if (executionContext.isTimestampRequired() && (timestampIndex == -1 || factory.hasDescendingOrder())) {
            Misc.free(factory);
            throw SqlException.$(model.getModelPosition(), "ASC order over TIMESTAMP column is required but not provided");
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
                selectMetadata.add(BaseRecordMetadata.copyOf(metadata, index));
            } else {
                selectMetadata.add(
                        new TableColumnMetadata(
                                Chars.toString(queryColumn.getAlias()),
                                metadata.getColumnHash(index),
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
            selectMetadata.add(BaseRecordMetadata.copyOf(metadata, timestampIndex));
            selectMetadata.setTimestampIndex(selectMetadata.getColumnCount() - 1);
            columnCrossIndex.add(timestampIndex);
        }

        return new SelectedRecordCursorFactory(selectMetadata, columnCrossIndex, factory);
    }

    private RecordCursorFactory generateSelectCursor(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        // sql parser ensures this type of model always has only one column
        return new RecordAsAFieldRecordCursorFactory(
                generate(model.getNestedModel(), executionContext),
                model.getColumns().getQuick(0).getAlias()
        );
    }

    private RecordCursorFactory generateSelectDistinct(QueryModel model, SqlExecutionContext executionContext) throws SqlException {

        QueryModel twoDeepNested;
        ExpressionNode tableNameEn;
        if (
                model.getColumns().size() == 1
                        && model.getNestedModel() != null
                        && model.getNestedModel().getSelectModelType() == QueryModel.SELECT_MODEL_CHOOSE
                        && (twoDeepNested = model.getNestedModel().getNestedModel()) != null
                        && twoDeepNested.getWhereClause() == null
                        && twoDeepNested.getLatestBy().size() == 0
                        && (tableNameEn = twoDeepNested.getTableName()) != null
        ) {
            CharSequence tableName = tableNameEn.token;
            try (TableReader reader = engine.getReader(executionContext.getCairoSecurityContext(), tableName)) {
                CharSequence columnName = model.getBottomUpColumnNames().get(0);
                TableReaderMetadata readerMetadata = reader.getMetadata();
                int columnIndex = readerMetadata.getColumnIndex(columnName);
                int columnType = readerMetadata.getColumnType(columnIndex);
                if (readerMetadata.getVersion() >= 416 && ColumnType.isSymbol(columnType)) {
                    final GenericRecordMetadata distinctSymbolMetadata = new GenericRecordMetadata();
                    distinctSymbolMetadata.add(BaseRecordMetadata.copyOf(readerMetadata, columnIndex));
                    return new DistinctSymbolRecordCursorFactory(
                            engine,
                            distinctSymbolMetadata,
                            Chars.toString(tableName),
                            columnIndex,
                            reader.getMetadata().getId(),
                            reader.getVersion()
                    );
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

        // fail fast if we cannot create timestamp sampler

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
                            new GenericRecordMetadata().add(new TableColumnMetadata(Chars.toString(columnName), 0, ColumnType.LONG));
                    return new CountRecordCursorFactory(metadata, generateSubQuery(model, executionContext));
                }
            }

            tempKeyIndexesInBase.clear();
            tempKeyIndex.clear();
            arrayColumnTypes.clear();
            tempKeyKinds.clear();

            boolean pageFramingSupported = false;
            boolean specialCaseKeys = false;

            // check for special case time function aggregations
            final QueryModel nested = model.getNestedModel();
            assert nested != null;
            // check if underlying model has reference to hour(column) function
            if (nested.getSelectModelType() == QueryModel.SELECT_MODEL_VIRTUAL
                    && (columnExpr = nested.getColumns().getQuick(0).getAst()).type == FUNCTION
                    && isHourKeyword(columnExpr.token)
                    && columnExpr.paramCount == 1
                    && columnExpr.rhs.type == LITERAL
            ) {
                specialCaseKeys = true;
                QueryModel.backupWhereClause(expressionNodePool, model);
                factory = generateSubQuery(nested, executionContext);
                pageFramingSupported = factory.supportPageFrameCursor();
                if (pageFramingSupported) {

                    // find position of the hour() argument in the factory meta
                    tempKeyIndexesInBase.add(factory.getMetadata().getColumnIndex(columnExpr.rhs.token));

                    // find position of hour() alias in selected columns
                    // also make sure there are no other literal column than our function reference
                    final CharSequence functionColumnName = columns.getQuick(0).getName();
                    for (int i = 0, n = columns.size(); i < n; i++) {
                        columnExpr = columns.getQuick(i).getAst();
                        if (columnExpr.type == LITERAL) {
                            if (Chars.equals(columnExpr.token, functionColumnName)) {
                                tempKeyIndex.add(i);
                                // storage dimension for Rosti is INT when we use hour(). This function produces INT.
                                tempKeyKinds.add(GKK_HOUR_INT);
                                arrayColumnTypes.add(ColumnType.INT);
                            } else {
                                // there is something else here, fallback to default implementation
                                pageFramingSupported = false;
                                break;
                            }
                        }
                    }
                } else {
                    factory = Misc.free(factory);
                }
            }

            if (factory == null) {
                if (specialCaseKeys) {
                    QueryModel.restoreWhereClause(model);
                }
                factory = generateSubQuery(model, executionContext);
                pageFramingSupported = factory.supportPageFrameCursor();
            }

            RecordMetadata metadata = factory.getMetadata();

            // inspect model for possibility of vector aggregate intrinsics
            if (pageFramingSupported && assembleKeysAndFunctionReferences(columns, metadata, !specialCaseKeys)) {
                // create metadata from everything we've gathered
                GenericRecordMetadata meta = new GenericRecordMetadata();

                // start with keys
                for (int i = 0, n = tempKeyIndex.size(); i < n; i++) {
                    final int indexInThis = tempKeyIndex.getQuick(i);
                    final int indexInBase = tempKeyIndexesInBase.getQuick(i);
                    final int type = arrayColumnTypes.getColumnType(i);

                    if (ColumnType.isSymbol(type)) {
                        meta.add(
                                indexInThis,
                                new TableColumnMetadata(
                                        Chars.toString(columns.getQuick(indexInThis).getName())
                                        , 0
                                        , type
                                        , false
                                        , 0
                                        , metadata.isSymbolTableStatic(indexInBase),
                                        null
                                )
                        );
                    } else {
                        meta.add(
                                indexInThis,
                                new TableColumnMetadata(
                                        Chars.toString(columns.getQuick(indexInThis).getName()),
                                        0,
                                        type,
                                        null
                                )
                        );
                    }
                }

                // add aggregates
                for (int i = 0, n = tempVecConstructors.size(); i < n; i++) {
                    VectorAggregateFunctionConstructor constructor = tempVecConstructors.getQuick(i);
                    int indexInBase = tempVecConstructorArgIndexes.getQuick(i);
                    int indexInThis = tempAggIndex.getQuick(i);
                    VectorAggregateFunction vaf = constructor.create(tempKeyKinds.size() == 0 ? 0 : tempKeyKinds.getQuick(0), indexInBase, executionContext.getWorkerCount());
                    tempVaf.add(vaf);
                    meta.add(indexInThis,
                            new TableColumnMetadata(
                                    Chars.toString(columns.getQuick(indexInThis).getName()),
                                    0,
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
                            tempVaf
                    );
                }

                if (tempKeyIndexesInBase.size() == 1) {
                    for (int i = 0, n = tempVaf.size(); i < n; i++) {
                        tempVaf.getQuick(i).pushValueTypes(arrayColumnTypes);
                    }

                    try {
                        GroupByUtils.validateGroupByColumns(model, 1);
                    } catch (Throwable e) {
                        Misc.freeObjList(tempVaf);
                        throw e;
                    }

                    return new GroupByRecordCursorFactory(
                            configuration,
                            factory,
                            meta,
                            arrayColumnTypes,
                            executionContext.getWorkerCount(),
                            tempVaf,
                            tempKeyIndexesInBase.getQuick(0),
                            tempKeyIndex.getQuick(0),
                            tempSymbolSkewIndexes
                    );
                }
            }

            Misc.freeObjList(tempVaf);

            if (specialCaseKeys) {
                // uh-oh, we had special case keys, but could not find implementation for the functions
                // release factory we created unnecessarily
                Misc.free(factory);
                // create factory on top level model
                QueryModel.restoreWhereClause(model);
                factory = generateSubQuery(model, executionContext);
                // and reset metadata
                metadata = factory.getMetadata();

            }

            final int timestampIndex = getTimestampIndex(model, factory);

            keyTypes.clear();
            valueTypes.clear();
            listColumnFilterA.clear();

            final int columnCount = model.getColumns().size();
            ObjList<GroupByFunction> groupByFunctions = new ObjList<>(columnCount);
            try {
                GroupByUtils.prepareGroupByFunctions(
                        model,
                        metadata,
                        functionParser,
                        executionContext,
                        groupByFunctions,
                        groupByFunctionPositions,
                        valueTypes
                );
            } catch (Throwable e) {
                Misc.freeObjList(groupByFunctions);
                throw e;
            }

            final ObjList<Function> recordFunctions = new ObjList<>(columnCount);
            final GenericRecordMetadata groupByMetadata = new GenericRecordMetadata();
            try {
                GroupByUtils.prepareGroupByRecordFunctions(
                        model,
                        metadata,
                        listColumnFilterA,
                        groupByFunctions,
                        groupByFunctionPositions,
                        recordFunctions,
                        recordFunctionPositions,
                        groupByMetadata,
                        keyTypes,
                        valueTypes.getColumnCount(),
                        true,
                        timestampIndex
                );
            } catch (Throwable e) {
                Misc.freeObjList(recordFunctions);
                throw e;
            }

            if (keyTypes.getColumnCount() == 0) {
                return new GroupByNotKeyedRecordCursorFactory(
                        factory,
                        groupByMetadata,
                        groupByFunctions,
                        recordFunctions,
                        valueTypes.getColumnCount()
                );
            }

            return new io.questdb.griffin.engine.groupby.GroupByRecordCursorFactory(
                    configuration,
                    factory,
                    listColumnFilterA,
                    asm,
                    keyTypes,
                    valueTypes,
                    groupByMetadata,
                    groupByFunctions,
                    recordFunctions
            );

        } catch (Throwable e) {
            Misc.free(factory);
            throw e;
        }
    }

    private RecordCursorFactory generateSelectVirtual(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        final RecordCursorFactory factory = generateSubQuery(model, executionContext);
        return generateSelectVirtualWithSubquery(model, executionContext, factory);
    }

    @NotNull
    private VirtualRecordCursorFactory generateSelectVirtualWithSubquery(QueryModel model, SqlExecutionContext executionContext, RecordCursorFactory factory) throws SqlException {
        try {
            final ObjList<QueryColumn> columns = model.getColumns();
            final int columnCount = columns.size();
            final RecordMetadata metadata = factory.getMetadata();
            final ObjList<Function> functions = new ObjList<>(columnCount);
            final GenericRecordMetadata virtualMetadata = new GenericRecordMetadata();

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
                if (node.type == ExpressionNode.LITERAL && Chars.equalsNc(node.token, timestampColumn)) {
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

                // define "undefined" functions as string unless it's update. Leave Undefined if update
                if (function.isUndefined()) {
                    if (!model.isUpdate()) {
                        function.assignType(ColumnType.STRING, executionContext.getBindVariableService());
                    } else {
                        // Set bind variable the type of the column
                        function.assignType(targetColumnType, executionContext.getBindVariableService());
                    }
                }

                int columnType = function.getType();
                if (targetColumnType != -1 && targetColumnType != columnType) {
                    // This is an update and the target column does not match with column the update is trying to perform
                    if (SqlCompiler.builtInFunctionCast(targetColumnType, function.getType())) {
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

                if (function instanceof SymbolFunction && columnType == ColumnType.SYMBOL) {
                    virtualMetadata.add(
                            new TableColumnMetadata(
                                    Chars.toString(column.getAlias()),
                                    configuration.getRandom().nextLong(),
                                    function.getType(),
                                    false,
                                    0,
                                    ((SymbolFunction) function).isSymbolTableStatic(),
                                    function.getMetadata()
                            )
                    );
                } else {
                    virtualMetadata.add(
                            new TableColumnMetadata(
                                    Chars.toString(column.getAlias()),
                                    configuration.getRandom().nextLong(),
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
                        0, timestampColumn, metadata
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
                                        0,
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
            factory.close();
            throw e;
        }
    }

    /**
     * Generates chain of parent factories each of which takes only two argument factories.
     * Parent factory will perform one of SET operations on its arguments, such as UNION, UNION ALL,
     * INTERSECT or EXCEPT
     *
     * @param model            incoming model is expected to have a chain of models via its QueryModel.getUnionModel() function
     * @param masterFactory    is compiled first argument
     * @param executionContext execution context for authorization and parallel execution purposes
     * @return factory that performs a SET operation
     * @throws SqlException when query contains syntax errors
     */
    private RecordCursorFactory generateSetFactory(
            QueryModel model,
            RecordCursorFactory masterFactory,
            SqlExecutionContext executionContext
    ) throws SqlException {
        RecordCursorFactory slaveFactory = generateQuery0(model.getUnionModel(), executionContext, true);
        switch (model.getSetOperationType()) {
            case QueryModel.SET_OPERATION_UNION:
                return generateUnionFactory(model, masterFactory, executionContext, slaveFactory, SET_UNION_CONSTRUCTOR);
            case QueryModel.SET_OPERATION_UNION_ALL:
                return generateUnionAllFactory(model, masterFactory, executionContext, slaveFactory);
            case QueryModel.SET_OPERATION_EXCEPT:
                return generateUnionFactory(model, masterFactory, executionContext, slaveFactory, SET_EXCEPT_CONSTRUCTOR);
            case QueryModel.SET_OPERATION_INTERSECT:
                return generateUnionFactory(model, masterFactory, executionContext, slaveFactory, SET_INTERSECT_CONSTRUCTOR);
            default:
                assert false;
                return null;
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

        try (TableReader reader = engine.getReader(
                executionContext.getCairoSecurityContext(),
                model.getTableName().token,
                model.getTableId(),
                model.getTableVersion())
        ) {
            final RecordMetadata readerMeta = reader.getMetadata();

            // create metadata based on top-down columns that are required

            final ObjList<QueryColumn> topDownColumns = model.getTopDownColumns();
            final int topDownColumnCount = topDownColumns.size();
            final IntList columnIndexes = new IntList();
            final IntList columnSizes = new IntList();

            // topDownColumnCount can be 0 for 'select count()' queries

            int readerTimestampIndex;
            readerTimestampIndex = getTimestampIndex(model, readerMeta);

            // Latest by on a table requires provided timestamp column to be the designated timestamp.
            if (latestBy.size() > 0 && readerTimestampIndex != readerMeta.getTimestampIndex()) {
                throw SqlException.$(model.getTimestamp().position, "latest by over a table requires designated TIMESTAMP");
            }

            boolean requiresTimestamp = joinsRequiringTimestamp[model.getJoinType()];
            final GenericRecordMetadata myMeta = new GenericRecordMetadata();
            boolean framingSupported;
            try {
                if (requiresTimestamp) {
                    executionContext.pushTimestampRequiredFlag(true);
                }

                boolean contextTimestampRequired = executionContext.isTimestampRequired();
                // some "sample by" queries don't select any cols but needs timestamp col selected
                // for example "select count() from x sample by 1h" implicitly needs timestamp column selected
                if (topDownColumnCount > 0 || contextTimestampRequired || model.isUpdate()) {
                    framingSupported = true;
                    for (int i = 0; i < topDownColumnCount; i++) {
                        int columnIndex = readerMeta.getColumnIndexQuiet(topDownColumns.getQuick(i).getName());
                        int type = readerMeta.getColumnType(columnIndex);
                        int typeSize = ColumnType.sizeOf(type);

                        columnIndexes.add(columnIndex);
                        columnSizes.add(Numbers.msb(typeSize));

                        myMeta.add(new TableColumnMetadata(
                                Chars.toString(topDownColumns.getQuick(i).getName()),
                                readerMeta.getColumnHash(columnIndex),
                                type,
                                readerMeta.isColumnIndexed(columnIndex),
                                readerMeta.getIndexValueBlockCapacity(columnIndex),
                                readerMeta.isSymbolTableStatic(columnIndex),
                                readerMeta.getMetadata(columnIndex)
                        ));

                        if (columnIndex == readerTimestampIndex) {
                            myMeta.setTimestampIndex(myMeta.getColumnCount() - 1);
                        }
                    }

                    // select timestamp when it is required but not already selected
                    if (readerTimestampIndex != -1 && myMeta.getTimestampIndex() == -1 && contextTimestampRequired) {
                        myMeta.add(new TableColumnMetadata(
                                readerMeta.getColumnName(readerTimestampIndex),
                                readerMeta.getColumnHash(readerTimestampIndex),
                                readerMeta.getColumnType(readerTimestampIndex),
                                readerMeta.getMetadata(readerTimestampIndex)
                        ));
                        myMeta.setTimestampIndex(myMeta.getColumnCount() - 1);

                        columnIndexes.add(readerTimestampIndex);
                        columnSizes.add((Numbers.msb(ColumnType.TIMESTAMP)));
                    }
                } else {
                    framingSupported = false;
                }
            } finally {
                if (requiresTimestamp) {
                    executionContext.popTimestampRequiredFlag();
                }
            }

            final int latestByColumnCount = prepareLatestByColumnIndexes(latestBy, myMeta);
            final String tableName = reader.getTableName();

            final ExpressionNode withinExtracted = whereClauseParser.extractWithin(
                    model,
                    model.getWhereClause(),
                    readerMeta,
                    functionParser,
                    executionContext,
                    prefixes
            );

            model.setWhereClause(withinExtracted);

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
                        readerMeta,
                        preferredKeyColumn,
                        readerTimestampIndex,
                        functionParser,
                        myMeta,
                        executionContext,
                        latestByColumnCount > 1
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

                DataFrameCursorFactory dfcFactory;

                if (latestByColumnCount > 0) {
                    Function f = compileFilter(intrinsicModel, myMeta, executionContext);
                    if (f != null && f.isConstant() && !f.getBool(null)) {
                        // 'latest by' clause takes over the latest by nodes, so that the later generateLatestBy() is no-op
                        model.getLatestBy().clear();

                        return new EmptyTableRecordCursorFactory(myMeta);
                    }

                    // a sub-query present in the filter may have used the latest by
                    // column index lists, so we need to regenerate them
                    prepareLatestByColumnIndexes(latestBy, myMeta);

                    return generateLatestByTableQuery(
                            model,
                            reader,
                            myMeta,
                            tableName,
                            intrinsicModel,
                            f,
                            executionContext,
                            readerTimestampIndex,
                            columnIndexes,
                            columnSizes,
                            prefixes
                    );
                }

                // below code block generates index-based filter
                final boolean intervalHitsOnlyOnePartition;
                if (intrinsicModel.hasIntervalFilters()) {
                    RuntimeIntrinsicIntervalModel intervalModel = intrinsicModel.buildIntervalModel();
                    dfcFactory = new IntervalFwdDataFrameCursorFactory(engine, tableName, model.getTableId(), model.getTableVersion(), intervalModel, readerTimestampIndex);
                    intervalHitsOnlyOnePartition = intervalModel.allIntervalsHitOnePartition(reader.getPartitionedBy());
                } else {
                    dfcFactory = new FullFwdDataFrameCursorFactory(engine, tableName, model.getTableId(), model.getTableVersion());
                    intervalHitsOnlyOnePartition = false;
                }

                if (intrinsicModel.keyColumn != null) {
                    // existence of column would have been already validated
                    final int keyColumnIndex = reader.getMetadata().getColumnIndexQuiet(intrinsicModel.keyColumn);
                    final int nKeyValues = intrinsicModel.keyValueFuncs.size();
                    final int nKeyExcludedValues = intrinsicModel.keyExcludedValueFuncs.size();

                    if (intrinsicModel.keySubQuery != null) {
                        final RecordCursorFactory rcf = generate(intrinsicModel.keySubQuery, executionContext);
                        final Record.CharSequenceFunction func = validateSubQueryColumnAndGetGetter(intrinsicModel, rcf.getMetadata());

                        Function f = compileFilter(intrinsicModel, myMeta, executionContext);
                        if (f != null && f.isConstant() && !f.getBool(null)) {
                            return new EmptyTableRecordCursorFactory(myMeta);
                        }
                        return new FilterOnSubQueryRecordCursorFactory(
                                myMeta,
                                dfcFactory,
                                rcf,
                                keyColumnIndex,
                                f,
                                func,
                                columnIndexes
                        );
                    }
                    assert nKeyValues > 0 || nKeyExcludedValues > 0;

                    boolean orderByKeyColumn = false;
                    int indexDirection = BitmapIndexReader.DIR_FORWARD;
                    if (intervalHitsOnlyOnePartition) {
                        final ObjList<ExpressionNode> orderByAdvice = model.getOrderByAdvice();
                        final int orderByAdviceSize = orderByAdvice.size();
                        if (orderByAdviceSize > 0 && orderByAdviceSize < 3) {
                            // todo: when order by coincides with keyColumn and there is index we can incorporate
                            //    ordering in the code that returns rows from index rather than having an
                            //    "overhead" order by implementation, which would be trying to oder already ordered symbols
                            if (Chars.equals(orderByAdvice.getQuick(0).token, intrinsicModel.keyColumn)) {
                                myMeta.setTimestampIndex(-1);
                                if (orderByAdviceSize == 1) {
                                    orderByKeyColumn = true;
                                } else if (Chars.equals(orderByAdvice.getQuick(1).token, model.getTimestamp().token)) {
                                    orderByKeyColumn = true;
                                    if (getOrderByDirectionOrDefault(model, 1) == QueryModel.ORDER_DIRECTION_DESCENDING) {
                                        indexDirection = BitmapIndexReader.DIR_BACKWARD;
                                    }
                                }
                            }
                        }
                    }

                    if (intrinsicModel.keyExcludedValueFuncs.size() == 0) {
                        Function f = compileFilter(intrinsicModel, myMeta, executionContext);
                        if (f != null && f.isConstant()) {
                            try {
                                if (!f.getBool(null)) {
                                    return new EmptyTableRecordCursorFactory(myMeta);
                                }
                            } finally {
                                f = Misc.free(f);
                            }
                        }
                        if (nKeyValues == 1) {
                            final RowCursorFactory rcf;
                            final Function symbolFunc = intrinsicModel.keyValueFuncs.get(0);
                            final SymbolMapReader symbolMapReader = reader.getSymbolMapReader(keyColumnIndex);
                            final int symbolKey = symbolFunc.isRuntimeConstant()
                                    ? SymbolTable.VALUE_NOT_FOUND
                                    : symbolMapReader.keyOf(symbolFunc.getStr(null));

                            if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                                if (f == null) {
                                    rcf = new DeferredSymbolIndexRowCursorFactory(keyColumnIndex,
                                            symbolFunc,
                                            true,
                                            indexDirection
                                    );
                                } else {
                                    rcf = new DeferredSymbolIndexFilteredRowCursorFactory(
                                            keyColumnIndex,
                                            symbolFunc,
                                            f,
                                            true,
                                            indexDirection,
                                            columnIndexes
                                    );
                                }
                            } else {
                                if (f == null) {
                                    rcf = new SymbolIndexRowCursorFactory(keyColumnIndex, symbolKey, true, indexDirection, null);
                                } else {
                                    rcf = new SymbolIndexFilteredRowCursorFactory(keyColumnIndex, symbolKey, f, true, indexDirection, columnIndexes, null);
                                }
                            }

                            if (f == null) {
                                // This special case factory can later be disassembled to framing and index
                                // cursors in Sample By processing
                                return new DeferredSingleSymbolFilterDataFrameRecordCursorFactory(
                                        configuration,
                                        keyColumnIndex,
                                        symbolFunc,
                                        rcf,
                                        myMeta,
                                        dfcFactory,
                                        orderByKeyColumn,
                                        columnIndexes,
                                        columnSizes
                                );
                            }
                            return new DataFrameRecordCursorFactory(
                                    configuration,
                                    myMeta,
                                    dfcFactory,
                                    rcf,
                                    orderByKeyColumn,
                                    f,
                                    false,
                                    columnIndexes,
                                    columnSizes
                            );
                        }

                        if (orderByKeyColumn) {
                            myMeta.setTimestampIndex(-1);
                        }

                        return new FilterOnValuesRecordCursorFactory(
                                myMeta,
                                dfcFactory,
                                intrinsicModel.keyValueFuncs,
                                keyColumnIndex,
                                reader,
                                f,
                                model.getOrderByAdviceMnemonic(),
                                orderByKeyColumn,
                                getOrderByDirectionOrDefault(model, 0),
                                indexDirection,
                                columnIndexes
                        );

                    } else if (
                            intrinsicModel.keyExcludedValueFuncs.size() > 0
                                    && reader.getSymbolMapReader(keyColumnIndex).getSymbolCount() < configuration.getMaxSymbolNotEqualsCount()
                    ) {
                        Function f = compileFilter(intrinsicModel, myMeta, executionContext);
                        if (f != null && f.isConstant()) {
                            try {
                                if (!f.getBool(null)) {
                                    return new EmptyTableRecordCursorFactory(myMeta);
                                }
                            } finally {
                                f = Misc.free(f);
                            }
                        }

                        return new FilterOnExcludedValuesRecordCursorFactory(
                                myMeta,
                                dfcFactory,
                                intrinsicModel.keyExcludedValueFuncs,
                                keyColumnIndex,
                                f,
                                model.getOrderByAdviceMnemonic(),
                                orderByKeyColumn,
                                indexDirection,
                                columnIndexes,
                                configuration.getMaxSymbolNotEqualsCount()
                        );
                    }
                }

                if (intervalHitsOnlyOnePartition && intrinsicModel.filter == null) {
                    final ObjList<ExpressionNode> orderByAdvice = model.getOrderByAdvice();
                    final int orderByAdviceSize = orderByAdvice.size();
                    if (orderByAdviceSize > 0 && orderByAdviceSize < 3 && intrinsicModel.hasIntervalFilters()) {
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
                                if (getOrderByDirectionOrDefault(model, 1) == QueryModel.ORDER_DIRECTION_DESCENDING) {
                                    indexDirection = BitmapIndexReader.DIR_BACKWARD;
                                }
                            }

                            if (orderByKeyColumn) {
                                // check that intrinsicModel.intervals hit only one partition
                                myMeta.setTimestampIndex(-1);
                                return new SortedSymbolIndexRecordCursorFactory(
                                        myMeta,
                                        dfcFactory,
                                        columnIndex,
                                        getOrderByDirectionOrDefault(model, 0) == QueryModel.ORDER_DIRECTION_ASCENDING,
                                        indexDirection,
                                        columnIndexes
                                );
                            }
                        }
                    }
                }

                boolean isOrderByTimestampDesc = isOrderDescendingByDesignatedTimestampOnly(model);
                RowCursorFactory rowFactory;

                if (isOrderByTimestampDesc && !intrinsicModel.hasIntervalFilters()) {
                    dfcFactory = new FullBwdDataFrameCursorFactory(engine, tableName, model.getTableId(), model.getTableVersion());
                    rowFactory = new BwdDataFrameRowCursorFactory();
                } else {
                    rowFactory = new DataFrameRowCursorFactory();
                }

                model.setWhereClause(intrinsicModel.filter);
                return new DataFrameRecordCursorFactory(
                        configuration,
                        myMeta,
                        dfcFactory,
                        rowFactory,
                        false,
                        null,
                        framingSupported,
                        columnIndexes,
                        columnSizes
                );
            }

            // no where clause
            if (latestByColumnCount == 0) {
                // construct new metadata, which is a copy of what we constructed just above, but
                // in the interest of isolating problems we will only affect this factory

                AbstractDataFrameCursorFactory cursorFactory;
                RowCursorFactory rowCursorFactory;

                if (isOrderDescendingByDesignatedTimestampOnly(model)) {
                    cursorFactory = new FullBwdDataFrameCursorFactory(engine, tableName, model.getTableId(), model.getTableVersion());
                    rowCursorFactory = new BwdDataFrameRowCursorFactory();
                } else {
                    cursorFactory = new FullFwdDataFrameCursorFactory(engine, tableName, model.getTableId(), model.getTableVersion());
                    rowCursorFactory = new DataFrameRowCursorFactory();
                }

                return new DataFrameRecordCursorFactory(
                        configuration,
                        myMeta,
                        cursorFactory,
                        rowCursorFactory,
                        false,
                        null,
                        framingSupported,
                        columnIndexes,
                        columnSizes
                );
            }

            // 'latest by' clause takes over the latest by nodes, so that the later generateLatestBy() is no-op
            model.getLatestBy().clear();

            // listColumnFilterA = latest by column indexes
            if (latestByColumnCount == 1) {
                int latestByColumnIndex = listColumnFilterA.getColumnIndexFactored(0);
                if (myMeta.isColumnIndexed(latestByColumnIndex)) {
                    return new LatestByAllIndexedRecordCursorFactory(
                            myMeta,
                            configuration,
                            new FullBwdDataFrameCursorFactory(engine, tableName, model.getTableId(), model.getTableVersion()),
                            listColumnFilterA.getColumnIndexFactored(0),
                            columnIndexes,
                            prefixes
                    );
                }

                if (ColumnType.isSymbol(myMeta.getColumnType(latestByColumnIndex))
                        && myMeta.isSymbolTableStatic(latestByColumnIndex)) {
                    // we have "latest by" symbol column values, but no index
                    return new LatestByDeferredListValuesFilteredRecordCursorFactory(
                            configuration,
                            myMeta,
                            new FullBwdDataFrameCursorFactory(engine, tableName, model.getTableId(), model.getTableVersion()),
                            latestByColumnIndex,
                            null,
                            columnIndexes
                    );
                }
            }

            return new LatestByAllFilteredRecordCursorFactory(
                    myMeta,
                    configuration,
                    new FullBwdDataFrameCursorFactory(engine, tableName, model.getTableId(), model.getTableVersion()),
                    RecordSinkFactory.getInstance(asm, myMeta, listColumnFilterA, false),
                    keyTypes,
                    null,
                    columnIndexes
            );
        }
    }

    private RecordCursorFactory generateUnionAllFactory(
            QueryModel model,
            RecordCursorFactory masterFactory,
            SqlExecutionContext executionContext,
            RecordCursorFactory slaveFactory
    ) throws SqlException {
        validateJoinColumnTypes(model, masterFactory, slaveFactory);
        final RecordCursorFactory unionAllFactory = new UnionAllRecordCursorFactory(
                calculateSetMetadata(masterFactory.getMetadata()),
                masterFactory,
                slaveFactory
        );

        if (model.getUnionModel().getUnionModel() != null) {
            return generateSetFactory(model.getUnionModel(), unionAllFactory, executionContext);
        }
        return unionAllFactory;
    }

    private RecordCursorFactory generateUnionFactory(
            QueryModel model,
            RecordCursorFactory masterFactory,
            SqlExecutionContext executionContext,
            RecordCursorFactory slaveFactory,
            SetRecordCursorFactoryConstructor constructor
    ) throws SqlException {
        validateJoinColumnTypes(model, masterFactory, slaveFactory);
        entityColumnFilter.of(masterFactory.getMetadata().getColumnCount());
        final RecordSink recordSink = RecordSinkFactory.getInstance(
                asm,
                masterFactory.getMetadata(),
                entityColumnFilter,
                true
        );

        valueTypes.clear();

        RecordCursorFactory unionFactory = constructor.create(
                configuration,
                calculateSetMetadata(masterFactory.getMetadata()),
                masterFactory,
                slaveFactory,
                recordSink,
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

    @NotNull
    private Function getLoFunction(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        return toLimitFunction(executionContext, model.getLimitLo(), LongConstant.ZERO);
    }

    @Nullable
    private Function getLimitLoFunctionOnly(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        if (model.getLimitAdviceLo() != null && model.getLimitAdviceHi() == null) {
            return toLimitFunction(executionContext, model.getLimitAdviceLo(), LongConstant.ZERO);
        }
        return null;
    }

    private int getTimestampIndex(QueryModel model, RecordCursorFactory factory) throws SqlException {
        final RecordMetadata metadata = factory.getMetadata();
        try {
            return getTimestampIndex(model, metadata);
        } catch (SqlException e) {
            Misc.free(factory);
            throw e;
        }
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

    private boolean isOrderDescendingByDesignatedTimestampOnly(QueryModel model) {
        return model.getOrderByAdvice().size() == 1 && model.getTimestamp() != null &&
                Chars.equalsIgnoreCase(model.getOrderByAdvice().getQuick(0).token, model.getTimestamp().token) &&
                getOrderByDirectionOrDefault(model, 0) == ORDER_DIRECTION_DESCENDING;
    }

    private boolean isSingleColumnFunction(ExpressionNode ast, CharSequence name) {
        return ast.type == FUNCTION && ast.paramCount == 1 && Chars.equals(ast.token, name) && ast.rhs.type == LITERAL;
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
                    case ColumnType.CHAR:
                    case ColumnType.SHORT:
                    case ColumnType.INT:
                    case ColumnType.LONG:
                    case ColumnType.LONG256:
                    case ColumnType.STRING:
                    case ColumnType.SYMBOL:
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
                                .put("): invalid type, only [BOOLEAN, SHORT, INT, LONG, LONG256, CHAR, STRING, SYMBOL] are supported in LATEST BY");
                }
            }
        }
        return latestByColumnCount;
    }

    private void processJoinContext(
            boolean vanillaMaster,
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
        for (int k = 0, m = listColumnFilterA.getColumnCount(); k < m; k++) {
            // Don't use tagOf(columnType) to compare the types.
            // Key types have too much exactly except SYMBOL and STRING special case
            int columnTypeA = slaveMetadata.getColumnType(listColumnFilterA.getColumnIndexFactored(k));
            int columnTypeB = masterMetadata.getColumnType(listColumnFilterB.getColumnIndexFactored(k));
            if (columnTypeB != columnTypeA && !(ColumnType.isSymbolOrString(columnTypeB) && ColumnType.isSymbolOrString(columnTypeA))) {
                // index in column filter and join context is the same
                throw SqlException.$(jc.aNodes.getQuick(k).position, "join column type mismatch");
            }
            keyTypes.add(columnTypeB == ColumnType.SYMBOL ? ColumnType.STRING : columnTypeB);
        }
    }

    // used in tests
    void setEnableJitNullChecks(boolean value) {
        enableJitNullChecks = value;
    }

    void setFullFatJoins(boolean fullFatJoins) {
        this.fullFatJoins = fullFatJoins;
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
            if (type == ColumnType.UNDEFINED) {
                if (func instanceof IndexedParameterLinkFunction) {
                    executionContext.getBindVariableService().setLong(((IndexedParameterLinkFunction) func).getVariableIndex(), defaultValue.getLong(null));
                    return func;
                }

                if (func instanceof NamedParameterLinkFunction) {
                    executionContext.getBindVariableService().setLong(((NamedParameterLinkFunction) func).getVariableName(), defaultValue.getLong(null));
                    return func;
                }
            }
            throw SqlException.$(limit.position, "invalid type: ").put(ColumnType.nameOf(type));
        }
        return func;
    }

    private IntList toOrderIndices(RecordMetadata m, ObjList<ExpressionNode> orderBy, IntList orderByDirection) throws SqlException {
        // todo: pool
        final IntList indices = new IntList();
        for (int i = 0, n = orderBy.size(); i < n; i++) {
            ExpressionNode tok = orderBy.getQuick(i);
            int index = m.getColumnIndexQuiet(tok.token);
            if (index == -1) {
                throw SqlException.invalidColumn(tok.position, tok.token);
            }

            // shift index by 1 to use sign as sort direction
            index++;

            // negative column index means descending order of sort
            if (orderByDirection.getQuick(i) == QueryModel.ORDER_DIRECTION_DESCENDING) {
                index = -index;
            }

            indices.add(index);
        }
        return indices;
    }

    private void validateBothTimestamps(QueryModel slaveModel, RecordMetadata masterMetadata, RecordMetadata slaveMetadata) throws SqlException {
        if (masterMetadata.getTimestampIndex() == -1) {
            throw SqlException.$(slaveModel.getJoinKeywordPosition(), "left side of time series join has no timestamp");
        }

        if (slaveMetadata.getTimestampIndex() == -1) {
            throw SqlException.$(slaveModel.getJoinKeywordPosition(), "right side of time series join has no timestamp");
        }
    }

    private void validateBothTimestampOrders(RecordCursorFactory masterFactory, RecordCursorFactory slaveFactory, int position) throws SqlException {
        if (masterFactory.hasDescendingOrder()) {
            throw SqlException.$(position, "left side of time series join has DESC timestamp order");
        }

        if (slaveFactory.hasDescendingOrder()) {
            throw SqlException.$(position, "right side of time series join has DESC timestamp order");
        }
    }

    private void validateJoinColumnTypes(QueryModel model, RecordCursorFactory masterFactory, RecordCursorFactory slaveFactory) throws SqlException {
        final RecordMetadata metadata = masterFactory.getMetadata();
        final RecordMetadata slaveMetadata = slaveFactory.getMetadata();
        final int columnCount = metadata.getColumnCount();

        for (int i = 0; i < columnCount; i++) {
            if (metadata.getColumnType(i) != slaveMetadata.getColumnType(i)) {
                throw SqlException
                        .$(model.getUnionModel().getModelPosition(), "column type mismatch [index=").put(i)
                        .put(", A=").put(ColumnType.nameOf(metadata.getColumnType(i)))
                        .put(", B=").put(ColumnType.nameOf(slaveMetadata.getColumnType(i)))
                        .put(']');
            }
        }
    }

    private Record.CharSequenceFunction validateSubQueryColumnAndGetGetter(IntrinsicModel intrinsicModel, RecordMetadata metadata) throws SqlException {
        int columnType = metadata.getColumnType(0);
        if (!ColumnType.isSymbolOrString(columnType)) {
            assert intrinsicModel.keySubQuery.getColumns() != null;
            assert intrinsicModel.keySubQuery.getColumns().size() > 0;

            throw SqlException
                    .position(intrinsicModel.keySubQuery.getColumns().getQuick(0).getAst().position)
                    .put("unsupported column type: ")
                    .put(metadata.getColumnName(0))
                    .put(": ")
                    .put(ColumnType.nameOf(columnType));
        }

        return ColumnType.isString(columnType) ? Record.GET_STR : Record.GET_SYM;
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
                IntList columnIndex
        );
    }

    static {
        joinsRequiringTimestamp[JOIN_INNER] = false;
        joinsRequiringTimestamp[JOIN_OUTER] = false;
        joinsRequiringTimestamp[JOIN_CROSS] = false;
        joinsRequiringTimestamp[JOIN_ASOF] = true;
        joinsRequiringTimestamp[JOIN_SPLICE] = true;
        joinsRequiringTimestamp[JOIN_LT] = true;
    }

    static {
        limitTypes.add(ColumnType.LONG);
        limitTypes.add(ColumnType.BYTE);
        limitTypes.add(ColumnType.SHORT);
        limitTypes.add(ColumnType.INT);
    }

    static {
        limitTypes.add(ColumnType.LONG);
        limitTypes.add(ColumnType.BYTE);
        limitTypes.add(ColumnType.SHORT);
        limitTypes.add(ColumnType.INT);
    }

    static {
        sumConstructors.put(ColumnType.DOUBLE, SumDoubleVectorAggregateFunction::new);
        sumConstructors.put(ColumnType.INT, SumIntVectorAggregateFunction::new);
        sumConstructors.put(ColumnType.LONG, SumLongVectorAggregateFunction::new);
        sumConstructors.put(ColumnType.LONG256, SumLong256VectorAggregateFunction::new);
        sumConstructors.put(ColumnType.DATE, SumDateVectorAggregateFunction::new);
        sumConstructors.put(ColumnType.TIMESTAMP, SumTimestampVectorAggregateFunction::new);

        ksumConstructors.put(ColumnType.DOUBLE, KSumDoubleVectorAggregateFunction::new);
        nsumConstructors.put(ColumnType.DOUBLE, NSumDoubleVectorAggregateFunction::new);

        avgConstructors.put(ColumnType.DOUBLE, AvgDoubleVectorAggregateFunction::new);
        avgConstructors.put(ColumnType.LONG, AvgLongVectorAggregateFunction::new);
        avgConstructors.put(ColumnType.TIMESTAMP, AvgLongVectorAggregateFunction::new);
        avgConstructors.put(ColumnType.DATE, AvgLongVectorAggregateFunction::new);
        avgConstructors.put(ColumnType.INT, AvgIntVectorAggregateFunction::new);

        minConstructors.put(ColumnType.DOUBLE, MinDoubleVectorAggregateFunction::new);
        minConstructors.put(ColumnType.LONG, MinLongVectorAggregateFunction::new);
        minConstructors.put(ColumnType.DATE, MinDateVectorAggregateFunction::new);
        minConstructors.put(ColumnType.TIMESTAMP, MinTimestampVectorAggregateFunction::new);
        minConstructors.put(ColumnType.INT, MinIntVectorAggregateFunction::new);

        maxConstructors.put(ColumnType.DOUBLE, MaxDoubleVectorAggregateFunction::new);
        maxConstructors.put(ColumnType.LONG, MaxLongVectorAggregateFunction::new);
        maxConstructors.put(ColumnType.DATE, MaxDateVectorAggregateFunction::new);
        maxConstructors.put(ColumnType.TIMESTAMP, MaxTimestampVectorAggregateFunction::new);
        maxConstructors.put(ColumnType.INT, MaxIntVectorAggregateFunction::new);
    }
}

/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.*;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.bind.IndexedParameterLinkFunction;
import io.questdb.griffin.engine.functions.bind.NamedParameterLinkFunction;
import io.questdb.griffin.engine.functions.cast.*;
import io.questdb.griffin.engine.functions.columns.*;
import io.questdb.griffin.engine.functions.constants.*;
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
import io.questdb.griffin.engine.window.CachedWindowRecordCursorFactory;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.griffin.model.*;
import io.questdb.jit.CompiledFilter;
import io.questdb.jit.CompiledFilterIRSerializer;
import io.questdb.jit.JitUtil;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.ArrayDeque;

import static io.questdb.cairo.sql.DataFrameCursorFactory.*;
import static io.questdb.griffin.SqlKeywords.*;
import static io.questdb.griffin.model.ExpressionNode.*;
import static io.questdb.griffin.model.QueryModel.QUERY;
import static io.questdb.griffin.model.QueryModel.*;

public class SqlCodeGenerator implements Mutable, Closeable {
    public static final int GKK_HOUR_INT = 1;
    public static final int GKK_VANILLA_INT = 0;
    private static final ModelOperator BACKUP_WHERE_CLAUSE = QueryModel::backupWhereClause;
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
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> avgConstructors = new IntObjHashMap<>();
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
    private boolean enableJitNullChecks = true;
    private boolean fullFatJoins = false;

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

    public RecordCursorFactory generateExplain(QueryModel model, RecordCursorFactory factory, int format) {
        RecordCursorFactory recordCursorFactory = new RecordCursorFactoryStub(model, factory);
        return new ExplainPlanFactory(recordCursorFactory, format);
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
                if (!SqlKeywords.isFirstKeyword(token) && !SqlKeywords.isLastKeyword(token)) {
                    return false;
                }

                if (columnAst.rhs.type != ExpressionNode.LITERAL || metadata.getColumnIndex(columnAst.rhs.token) < 0) {
                    return false;
                }
            }
        }

        return true;
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
        return new AsOfJoinRecordCursorFactory(configuration, metadata, masterFactory, slaveFactory, mapKeyTypes,
                mapValueTypes, slaveColumnTypes, masterKeySink, slaveKeySink, columnSplit, slaveValueSink, columnIndex,
                joinContext, masterTableKeyColumns
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
        return new LtJoinRecordCursorFactory(configuration, metadata, masterFactory, slaveFactory, mapKeyTypes,
                mapValueTypes, slaveColumnTypes, masterKeySink, slaveKeySink, columnSplit, slaveValueSink,
                columnIndex, joinContext, masterTableKeyColumns
        );
    }

    private static int getOrderByDirectionOrDefault(QueryModel model, int index) {
        IntList direction = model.getOrderByDirectionAdvice();
        if (index >= direction.size()) {
            return ORDER_DIRECTION_ASCENDING;
        }
        return model.getOrderByDirectionAdvice().getQuick(index);
    }

    private static boolean isSingleColumnFunction(ExpressionNode ast, CharSequence name) {
        return ast.type == FUNCTION && ast.paramCount == 1 && Chars.equalsIgnoreCase(ast.token, name) && ast.rhs.type == LITERAL;
    }

    private VectorAggregateFunctionConstructor assembleFunctionReference(RecordMetadata metadata, ExpressionNode ast) {
        int columnIndex;
        if (ast.type == FUNCTION && ast.paramCount == 1 && SqlKeywords.isSumKeyword(ast.token) && ast.rhs.type == LITERAL) {
            columnIndex = metadata.getColumnIndex(ast.rhs.token);
            tempVecConstructorArgIndexes.add(columnIndex);
            return sumConstructors.get(metadata.getColumnType(columnIndex));
        } else if (ast.type == FUNCTION && SqlKeywords.isCountKeyword(ast.token)
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

    private void backupWhereClause(ExpressionNode node) {
        processNodeQueryModels(node, BACKUP_WHERE_CLAUSE);
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
            @Nullable Function filter,
            int workerCount,
            @Nullable ExpressionNode filterExpr,
            RecordMetadata metadata,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (filter != null && !filter.isReadThreadSafe()) {
            ObjList<Function> workerFilters = new ObjList<>();
            for (int i = 0; i < workerCount; i++) {
                restoreWhereClause(filterExpr); // restore original filters in node query models
                workerFilters.extendAndSet(i, compileBooleanFilter(filterExpr, metadata, executionContext));
            }
            return workerFilters;
        }
        return null;
    }

    private @Nullable ObjList<ObjList<Function>> compileWorkerKeyFunctionsConditionally(
            @NotNull ObjList<Function> keyFunctions,
            int workerCount,
            @NotNull ObjList<ExpressionNode> keyFunctionNodes,
            RecordMetadata metadata,
            SqlExecutionContext executionContext
    ) throws SqlException {
        boolean threadSafe = true;
        for (int i = 0, n = keyFunctions.size(); i < n; i++) {
            if (!keyFunctions.getQuick(i).isReadThreadSafe()) {
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
                if (ColumnType.isVariableLength(slaveMetadata.getColumnType(k))) {
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
                true
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
                    valueTypes.add(m.getType());
                    slaveTypes.add(m.getType());
                }
            }

            // now add key columns to metadata
            for (int i = 0, n = listColumnFilterA.getColumnCount(); i < n; i++) {
                int index = listColumnFilterA.getColumnIndexFactored(i);
                final TableColumnMetadata m = slaveMetadata.getColumnMetadata(index);
                metadata.add(slaveAlias, m);
                slaveTypes.add(m.getType());
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
                            true
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
        valueTypes.add(ColumnType.LONG); // record count for the key

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
                                castFunctions.add(new CastByteToCharFunctionFactory.CastByteToCharFunction(new ByteColumn(i)));
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
                                castFunctions.add(new CastByteToStrFunctionFactory.CastByteToStrFunction(new ByteColumn(i)));
                                break;
                            case ColumnType.SHORT:
                                castFunctions.add(new CastShortToStrFunctionFactory.CastShortToStrFunction(new ShortColumn(i)));
                                break;
                            case ColumnType.CHAR:
                                // CharFunction has built-in cast to String
                                castFunctions.add(new CharColumn(i));
                                break;
                            case ColumnType.INT:
                                castFunctions.add(new CastIntToStrFunctionFactory.CastIntToStrFunction(new IntColumn(i)));
                                break;
                            case ColumnType.LONG:
                                castFunctions.add(new CastLongToStrFunctionFactory.CastLongToStrFunction(new LongColumn(i)));
                                break;
                            case ColumnType.DATE:
                                castFunctions.add(new CastDateToStrFunctionFactory.CastDateToStrFunction(new DateColumn(i)));
                                break;
                            case ColumnType.TIMESTAMP:
                                castFunctions.add(new CastTimestampToStrFunctionFactory.CastTimestampToStrFunction(new TimestampColumn(i)));
                                break;
                            case ColumnType.FLOAT:
                                castFunctions.add(new CastFloatToStrFunctionFactory.CastFloatToStrFunction(
                                        new FloatColumn(i),
                                        configuration.getFloatToStrCastScale()
                                ));
                                break;
                            case ColumnType.DOUBLE:
                                castFunctions.add(new CastDoubleToStrFunctionFactory.CastDoubleToStrFunction(
                                        new DoubleColumn(i),
                                        configuration.getDoubleToStrCastScale()
                                ));
                                break;
                            case ColumnType.STRING:
                                castFunctions.add(new StrColumn(i));
                                break;
                            case ColumnType.UUID:
                                castFunctions.add(new CastUuidToStrFunctionFactory.Func(new UuidColumn(i)));
                                break;
                            case ColumnType.SYMBOL:
                                castFunctions.add(
                                        new CastSymbolToStrFunctionFactory.CastSymbolToStrFunction(
                                                new SymbolColumn(i, castFromMetadata.isSymbolTableStatic(i))
                                        )
                                );
                                break;
                            case ColumnType.LONG256:
                                castFunctions.add(
                                        new CastLong256ToStrFunctionFactory.CastLong256ToStrFunction(
                                                new Long256Column(i)
                                        )
                                );
                                break;
                            case ColumnType.GEOBYTE:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.getGeoByteToStrCastFunction(
                                                new GeoByteColumn(i, toTag),
                                                ColumnType.getGeoHashBits(fromType)
                                        )
                                );
                                break;
                            case ColumnType.GEOSHORT:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.getGeoShortToStrCastFunction(
                                                new GeoShortColumn(i, toTag),
                                                ColumnType.getGeoHashBits(castFromMetadata.getColumnType(i))
                                        )
                                );
                                break;
                            case ColumnType.GEOINT:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.getGeoIntToStrCastFunction(
                                                new GeoIntColumn(i, toTag),
                                                ColumnType.getGeoHashBits(castFromMetadata.getColumnType(i))
                                        )
                                );
                                break;
                            case ColumnType.GEOLONG:
                                castFunctions.add(
                                        CastGeoHashToGeoHashFunctionFactory.getGeoLongToStrCastFunction(
                                                new GeoLongColumn(i, toTag),
                                                ColumnType.getGeoHashBits(castFromMetadata.getColumnType(i))
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
                        }
                        break;
                    case ColumnType.SYMBOL:
                        castFunctions.add(new CastSymbolToStrFunctionFactory.CastSymbolToStrFunction(
                                new SymbolColumn(
                                        i,
                                        castFromMetadata.isSymbolTableStatic(i)
                                )));
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
                }
            }
        }
        return castFunctions;
    }

    private RecordCursorFactory generateFilter(RecordCursorFactory factory, QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        final ExpressionNode filter = model.getWhereClause();
        return filter == null ? factory : generateFilter0(factory, model, executionContext, filter);
    }

    @NotNull
    private RecordCursorFactory generateFilter0(
            RecordCursorFactory factory,
            QueryModel model,
            SqlExecutionContext executionContext,
            ExpressionNode filterExpr
    ) throws SqlException {
        backupWhereClause(filterExpr); // back up in case filters need to be compiled again
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
        if (enableParallelFilter && factory.supportPageFrameCursor()) {
            final boolean useJit = executionContext.getJitMode() != SqlJitMode.JIT_MODE_DISABLED
                    && (!model.isUpdate() || executionContext.isWalApplication());
            final boolean canCompile = factory.supportPageFrameCursor() && JitUtil.isJitSupported();
            if (useJit && canCompile) {
                CompiledFilter jitFilter = null;
                try {
                    int jitOptions;
                    final ObjList<Function> bindVarFunctions = new ObjList<>();
                    try (PageFrameCursor cursor = factory.getPageFrameCursor(executionContext, ORDER_ANY)) {
                        final boolean forceScalar = executionContext.getJitMode() == SqlJitMode.JIT_MODE_FORCE_SCALAR;
                        jitIRSerializer.of(jitIRMem, executionContext, factory.getMetadata(), cursor, bindVarFunctions);
                        jitOptions = jitIRSerializer.serialize(filterExpr, forceScalar, enableJitDebug, enableJitNullChecks);
                    }

                    jitFilter = new CompiledFilter();
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
                            filter,
                            reduceTaskFactory,
                            compileWorkerFilterConditionally(
                                    filter,
                                    executionContext.getSharedWorkerCount(),
                                    filterExpr,
                                    factory.getMetadata(),
                                    executionContext
                            ),
                            jitFilter,
                            limitLoFunction,
                            limitLoPos,
                            preTouchColumns,
                            executionContext.getSharedWorkerCount()
                    );
                } catch (SqlException | LimitOverflowException ex) {
                    Misc.free(jitFilter);
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
            } catch (Throwable e) {
                Misc.free(filter);
                Misc.free(factory);
                throw e;
            }
            final int limitLoPos = model.getLimitAdviceLo() != null ? model.getLimitAdviceLo().position : 0;
            return new AsyncFilteredRecordCursorFactory(
                    configuration,
                    executionContext.getMessageBus(),
                    factory,
                    filter,
                    reduceTaskFactory,
                    compileWorkerFilterConditionally(
                            filter,
                            executionContext.getSharedWorkerCount(),
                            filterExpr,
                            factory.getMetadata(),
                            executionContext
                    ),
                    limitLoFunction,
                    limitLoPos,
                    preTouchColumns,
                    executionContext.getSharedWorkerCount()
            );
        }
        return new FilteredRecordCursorFactory(factory, filter);
    }

    private RecordCursorFactory generateFunctionQuery(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        final RecordCursorFactory tableFactory = model.getTableNameFunction();
        if (tableFactory != null) {
            // We're transferring ownership of the tableFactory's factory to another factory
            // setting tableFactory to NULL will prevent double-ownership.
            // We should not release tableFactory itself, they typically just a lightweight factory wrapper.
            model.setTableFactory(null);
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
        entityColumnFilter.of(factoryA.getMetadata().getColumnCount());
        final RecordSink recordSink = RecordSinkFactory.getInstance(
                asm,
                unionMetadata,
                entityColumnFilter,
                true
        );
        valueTypes.clear();
        // Remap symbol columns to string type since that's how recordSink copies them.
        keyTypes.clear();
        for (int i = 0, n = unionMetadata.getColumnCount(); i < n; i++) {
            final int columnType = unionMetadata.getColumnType(i);
            if (ColumnType.isSymbol(columnType)) {
                keyTypes.add(ColumnType.STRING);
            } else {
                keyTypes.add(columnType);
            }
        }
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
                        JoinRecordMetadata joinMetadata;

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
                                                masterMetadata.getColumnCount(),
                                                slaveModel.getContext()
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
                                                masterMetadata.getColumnCount(),
                                                slaveModel.getContext()
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
                                processJoinContext(index == 1, slaveModel.getContext(), masterMetadata, slaveMetadata);

                                joinMetadata = createJoinMetadata(masterAlias, masterMetadata, slaveModel.getName(), slaveMetadata);
                                if (slaveModel.getOuterJoinExpressionClause() != null) {
                                    filter = compileJoinFilter(slaveModel.getOuterJoinExpressionClause(), joinMetadata, executionContext);
                                }

                                if (joinType == JOIN_OUTER &&
                                        filter != null && filter.isConstant() && !filter.getBool(null)) {
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
                } catch (Throwable e) {
                    master = Misc.free(master);
                    if (releaseSlave) {
                        Misc.free(slave);
                    }
                    throw e;
                } finally {
                    executionContext.popTimestampRequiredFlag();
                }

                // check if there are post-filters
                ExpressionNode filterExpr = slaveModel.getPostJoinWhereClause();
                if (filterExpr != null) {
                    if (executionContext.isParallelFilterEnabled() && master.supportPageFrameCursor()) {
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
                                        filter,
                                        executionContext.getSharedWorkerCount(),
                                        filterExpr,
                                        master.getMetadata(),
                                        executionContext
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
            ExpressionNode constFilter = model.getConstWhereClause();
            if (constFilter != null) {
                Function function = functionParser.parseFunction(constFilter, null, executionContext);
                if (!ColumnType.isBoolean(function.getType())) {
                    throw SqlException.position(constFilter.position).put("boolean expression expected");
                }
                function.init(null, executionContext);
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
            TableToken tableToken,
            IntrinsicModel intrinsicModel,
            Function filter,
            @Transient SqlExecutionContext executionContext,
            int timestampIndex,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizes,
            @NotNull LongList prefixes
    ) throws SqlException {
        final DataFrameCursorFactory dataFrameCursorFactory;
        if (intrinsicModel.hasIntervalFilters()) {
            dataFrameCursorFactory = new IntervalBwdDataFrameCursorFactory(
                    tableToken,
                    model.getMetadataVersion(),
                    intrinsicModel.buildIntervalModel(),
                    timestampIndex,
                    GenericRecordMetadata.deepCopyOf(reader.getMetadata())
            );
        } else {
            dataFrameCursorFactory = new FullBwdDataFrameCursorFactory(
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
                        metadata,
                        configuration,
                        dataFrameCursorFactory,
                        RecordSinkFactory.getInstance(asm, metadata, listColumnFilterA, false),
                        keyTypes,
                        partitionByColumnIndexes,
                        partitionBySymbolCounts,
                        filter,
                        columnIndexes
                );
            }
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
                final RecordCursorFactory rcf;
                final Record.CharSequenceFunction func;
                try {
                    rcf = generate(intrinsicModel.keySubQuery, executionContext);
                    func = validateSubQueryColumnAndGetGetter(intrinsicModel, rcf.getMetadata());
                } catch (Throwable e) {
                    Misc.free(dataFrameCursorFactory);
                    throw e;
                }

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
                                columnSizes,
                                true
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

            assert nKeyValues > 0 || nExcludedKeyValues > 0;

            // we have "latest by" column values, but no index

            if (nKeyValues > 1 || nExcludedKeyValues > 0) {
                return new LatestByDeferredListValuesFilteredRecordCursorFactory(
                        configuration,
                        metadata,
                        dataFrameCursorFactory,
                        latestByIndex,
                        intrinsicModel.keyValueFuncs,
                        intrinsicModel.keyExcludedValueFuncs,
                        filter,
                        columnIndexes
                );
            }

            assert nExcludedKeyValues == 0;

            // we have a single symbol key
            final Function symbolKeyFunc = intrinsicModel.keyValueFuncs.get(0);
            final SymbolMapReader symbolMapReader = reader.getSymbolMapReader(columnIndexes.getQuick(latestByIndex));
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

                boolean preSortedByTs = false;
                // if first column index is the same as timestamp of underlying record cursor factory
                // we could have two possibilities:
                // 1. if we only have one column to order by - the cursor would already be ordered
                //    by timestamp (either ASC or DESC); we have nothing to do
                // 2. metadata of the new cursor will have the timestamp
                if (timestampIndex != -1) {
                    CharSequence column = columnNames.getQuick(0);
                    int index = metadata.getColumnIndexQuiet(column);
                    if (index == timestampIndex) {
                        if (orderByColumnCount == 1) {
                            if (orderBy.get(column) == QueryModel.ORDER_DIRECTION_ASCENDING
                                    && recordCursorFactory.getScanDirection() == RecordCursorFactory.SCAN_DIRECTION_FORWARD) {
                                return recordCursorFactory;
                            } else if (orderBy.get(column) == ORDER_DIRECTION_DESCENDING
                                    && recordCursorFactory.getScanDirection() == RecordCursorFactory.SCAN_DIRECTION_BACKWARD) {
                                return recordCursorFactory;
                            }
                        } else { //orderByColumnCount > 1
                            preSortedByTs = (orderBy.get(column) == QueryModel.ORDER_DIRECTION_ASCENDING && recordCursorFactory.getScanDirection() == RecordCursorFactory.SCAN_DIRECTION_FORWARD) ||
                                    (orderBy.get(column) == ORDER_DIRECTION_DESCENDING && recordCursorFactory.getScanDirection() == RecordCursorFactory.SCAN_DIRECTION_BACKWARD);
                        }
                    }
                }

                RecordMetadata orderedMetadata;
                if (metadata.getColumnIndexQuiet(columnNames.getQuick(0)) == timestampIndex) {
                    orderedMetadata = GenericRecordMetadata.copyOf(metadata);
                } else {
                    orderedMetadata = GenericRecordMetadata.copyOfSansTimestamp(metadata);
                }
                final Function loFunc = getLoFunction(model, executionContext);
                final Function hiFunc = getHiFunction(model, executionContext);

                if (recordCursorFactory.recordCursorSupportsRandomAccess()) {
                    if (canSortAndLimitBeOptimized(model, executionContext, loFunc, hiFunc)) {
                        model.setLimitImplemented(true);
                        int baseCursorTimestampIndex = preSortedByTs ? timestampIndex : -1;
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
                        RecordSinkFactory.getInstance(
                                asm,
                                orderedMetadata,
                                entityColumnFilter,
                                false
                        ),
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

        if (ColumnType.isUndefined(timezoneNameFunc.getType())) {
            timezoneNameFunc.assignType(ColumnType.STRING, executionContext.getBindVariableService());
        } else if ((!timezoneNameFunc.isConstant() && !timezoneNameFunc.isRuntimeConstant())
                || !ColumnType.isAssignableFrom(timezoneNameFunc.getType(), ColumnType.STRING)) {
            throw SqlException.$(timezoneNameFuncPos, "timezone must be a constant expression of STRING or CHAR type");
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

        if (ColumnType.isUndefined(offsetFunc.getType())) {
            offsetFunc.assignType(ColumnType.STRING, executionContext.getBindVariableService());
        } else if ((!offsetFunc.isConstant() && !offsetFunc.isRuntimeConstant())
                || !ColumnType.isAssignableFrom(offsetFunc.getType(), ColumnType.STRING)) {
            throw SqlException.$(offsetFuncPos, "offset must be a constant expression of STRING or CHAR type");
        }

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
                try {
                    GroupByUtils.prepareGroupByFunctions(
                            model,
                            metadata,
                            functionParser,
                            executionContext,
                            groupByFunctions,
                            groupByFunctionPositions,
                            null,
                            null,
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
                            sqlNodeStack,
                            model,
                            metadata,
                            listColumnFilterA,
                            groupByFunctions,
                            groupByFunctionPositions,
                            null,
                            recordFunctions,
                            recordFunctionPositions,
                            groupByMetadata,
                            keyTypes,
                            valueTypes.getColumnCount(),
                            false,
                            timestampIndex
                    );
                } catch (Throwable e) {
                    Misc.freeObjList(recordFunctions); // groupByFunctions are included in recordFunctions
                    throw e;
                }

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
                        timestampIndex
                );
            }

            valueTypes.add(ColumnType.TIMESTAMP); // first value is always timestamp

            final int columnCount = model.getColumns().size();
            final ObjList<GroupByFunction> groupByFunctions = new ObjList<>(columnCount);
            try {
                GroupByUtils.prepareGroupByFunctions(
                        model,
                        metadata,
                        functionParser,
                        executionContext,
                        groupByFunctions,
                        groupByFunctionPositions,
                        null,
                        null,
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
                        sqlNodeStack,
                        model,
                        metadata,
                        listColumnFilterA,
                        groupByFunctions,
                        groupByFunctionPositions,
                        null,
                        recordFunctions,
                        recordFunctionPositions,
                        groupByMetadata,
                        keyTypes,
                        valueTypes.getColumnCount(),
                        false,
                        timestampIndex
                );
            } catch (Throwable e) {
                Misc.freeObjList(recordFunctions); // groupByFunctions are included in recordFunctions
                throw e;
            }

            boolean isFillNone = fillCount == 0 || fillCount == 1 && Chars.equalsLowerCaseAscii(sampleByFill.getQuick(0).token, "none");
            boolean allGroupsFirstLast = isFillNone && allGroupsFirstLastWithSingleSymbolFilter(model, metadata);
            if (allGroupsFirstLast) {
                SingleSymbolFilter symbolFilter = factory.convertToSampleByIndexDataFrameCursorFactory();
                if (symbolFilter != null) {
                    int symbolColIndex = getSampleBySymbolKeyIndex(model, metadata);
                    if (symbolColIndex == -1 || symbolFilter.getColumnIndex() == symbolColIndex) {
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
                    factory.revertFromSampleByIndexDataFrameCursorFactory();
                }
            }

            if (fillCount == 1 && Chars.equalsLowerCaseAscii(sampleByFill.getQuick(0).token, "prev")) {
                if (keyTypes.getColumnCount() == 0) {
                    return new SampleByFillPrevNotKeyedRecordCursorFactory(
                            asm,
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
                        offsetFuncPos
                );
            }

            if (isFillNone) {
                if (keyTypes.getColumnCount() == 0) {
                    // this sample by is not keyed
                    return new SampleByFillNoneNotKeyedRecordCursorFactory(
                            asm,
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
                        offsetFuncPos
                );
            }

            if (fillCount == 1 && isNullKeyword(sampleByFill.getQuick(0).token)) {
                if (keyTypes.getColumnCount() == 0) {
                    return new SampleByFillNullNotKeyedRecordCursorFactory(
                            asm,
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
                        offsetFuncPos
                );
            }

            assert fillCount > 0;

            if (keyTypes.getColumnCount() == 0) {
                return new SampleByFillValueNotKeyedRecordCursorFactory(
                        asm,
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
                    offsetFuncPos
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
            case QueryModel.SELECT_MODEL_CHOOSE:
                return generateSelectChoose(model, executionContext);
            case QueryModel.SELECT_MODEL_GROUP_BY:
                return generateSelectGroupBy(model, executionContext);
            case QueryModel.SELECT_MODEL_VIRTUAL:
                return generateSelectVirtual(model, executionContext);
            case QueryModel.SELECT_MODEL_WINDOW:
                return generateSelectWindow(model, executionContext);
            case QueryModel.SELECT_MODEL_DISTINCT:
                return generateSelectDistinct(model, executionContext);
            case QueryModel.SELECT_MODEL_CURSOR:
                return generateSelectCursor(model, executionContext);
            case QueryModel.SELECT_MODEL_SHOW:
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
                        !Chars.equals(metadata.getColumnName(i), qc.getAst().token) ||
                                qc.getAlias() != null && !(Chars.equals(qc.getAlias(), qc.getAst().token))
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
            int dot = Chars.indexOf(colMetadata.getName(), '.');
            if (dot > -1) {//remove inner table alias
                selectMetadata.add(
                        new TableColumnMetadata(
                                Chars.toString(colMetadata.getName(), dot + 1, colMetadata.getName().length()),
                                colMetadata.getType(),
                                colMetadata.isIndexed(),
                                colMetadata.getIndexValueBlockCapacity(),
                                colMetadata.isSymbolTableStatic(),
                                metadata
                        )
                );
            } else {
                if (selectMetadata.getColumnIndexQuiet(colMetadata.getName()) < 0) {
                    selectMetadata.add(colMetadata);
                } else {
                    // avoid clashing with other columns using timestamp column name as alias
                    StringSink sink = Misc.getThreadLocalSink();
                    sink.put(colMetadata.getName());
                    int len = sink.length();
                    int sequence = 0;

                    do {
                        sink.trimTo(len);
                        sink.put(sequence++);
                    } while (selectMetadata.getColumnIndexQuiet(sink) > -1);

                    selectMetadata.add(
                            new TableColumnMetadata(
                                    sink.toString(),
                                    colMetadata.getType(),
                                    colMetadata.isIndexed(),
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
                        && model.getNestedModel().getSelectModelType() == QueryModel.SELECT_MODEL_CHOOSE
                        && (twoDeepNested = model.getNestedModel().getNestedModel()) != null
                        && twoDeepNested.getLatestBy().size() == 0
                        && (tableNameEn = twoDeepNested.getTableNameExpr()) != null
                        && tableNameEn.type == ExpressionNode.LITERAL
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
                    if (factory.supportPageFrameCursor()) {
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
            boolean specialCaseKeys = false;

            // check for special case time function aggregations
            final QueryModel nested = model.getNestedModel();
            assert nested != null;
            // check if underlying model has reference to hour(column) function
            if (
                    nested.getSelectModelType() == QueryModel.SELECT_MODEL_VIRTUAL
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

            ExpressionNode nestedFilterExpr = null;
            if (factory == null) {
                if (specialCaseKeys) {
                    QueryModel.restoreWhereClause(expressionNodePool, model);
                }
                nestedFilterExpr = ExpressionNode.deepClone(expressionNodePool, nested.getWhereClause());
                factory = generateSubQuery(model, executionContext);
                pageFramingSupported = factory.supportPageFrameCursor();
            }

            RecordMetadata metadata = factory.getMetadata();

            // Inspect model for possibility of vector aggregate intrinsics.
            if (pageFramingSupported && assembleKeysAndFunctionReferences(columns, metadata, !specialCaseKeys)) {
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
                        int keyKind = specialCaseKeys ? SqlCodeGenerator.GKK_HOUR_INT : SqlCodeGenerator.GKK_VANILLA_INT;
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

                    return new GroupByRecordCursorFactory(
                            configuration,
                            factory,
                            meta,
                            arrayColumnTypes,
                            executionContext.getSharedWorkerCount(),
                            tempVaf,
                            tempKeyIndexesInBase.getQuick(0),
                            tempKeyIndex.getQuick(0),
                            tempSymbolSkewIndexes
                    );
                }

                // Free the vector aggregate functions since we didn't use them.
                Misc.freeObjList(tempVaf);
            }

            if (specialCaseKeys) {
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
            try {
                GroupByUtils.prepareGroupByFunctions(
                        model,
                        metadata,
                        functionParser,
                        executionContext,
                        groupByFunctions,
                        groupByFunctionPositions,
                        keyFunctions,
                        keyFunctionNodes,
                        valueTypes
                );
            } catch (Throwable e) {
                Misc.freeObjList(groupByFunctions);
                Misc.freeObjList(keyFunctions);
                throw e;
            }

            final ObjList<Function> recordFunctions = new ObjList<>(columnCount);
            final GenericRecordMetadata groupByMetadata = new GenericRecordMetadata();
            try {
                GroupByUtils.prepareGroupByRecordFunctions(
                        sqlNodeStack,
                        model,
                        metadata,
                        listColumnFilterA,
                        groupByFunctions,
                        groupByFunctionPositions,
                        keyFunctions,
                        recordFunctions,
                        recordFunctionPositions,
                        groupByMetadata,
                        keyTypes,
                        valueTypes.getColumnCount(),
                        true,
                        timestampIndex
                );
            } catch (Throwable e) {
                Misc.freeObjList(recordFunctions); // groupByFunctions are included in recordFunctions
                Misc.freeObjList(keyFunctions);
                throw e;
            }

            final boolean enableParallelGroupBy = configuration.isSqlParallelGroupByEnabled();
            if (enableParallelGroupBy && GroupByUtils.supportParallelism(groupByFunctions)) {
                boolean supportsParallelism = factory.supportPageFrameCursor();
                Function nestedFilter = null;
                ArrayColumnTypes keyTypesCopy = keyTypes;
                ArrayColumnTypes valueTypesCopy = valueTypes;
                ListColumnFilter listColumnFilterCopy = listColumnFilterA;
                // Try to steal the filter from the nested model, if possible.
                // We aim for simple cases such as select key, avg(value) from t where value > 0
                if (
                        !supportsParallelism
                                && !factory.usesIndex() // favor index-based access
                                && SqlUtil.isPlainSelect(nested)
                                && nested.getWhereClause() == null && nestedFilterExpr != null // filtered factories "steal" the filter
                ) {
                    try {
                        // back up required lists as generateSubQuery or compileWorkerFilterConditionally may overwrite them
                        keyTypesCopy = new ArrayColumnTypes().addAll(keyTypes);
                        valueTypesCopy = new ArrayColumnTypes().addAll(valueTypes);
                        listColumnFilterCopy = listColumnFilterA.copy();

                        RecordCursorFactory candidateFactory = generateSubQuery(model, executionContext);
                        if (candidateFactory.supportPageFrameCursor()) {
                            Misc.free(factory);
                            factory = candidateFactory;
                            restoreWhereClause(nestedFilterExpr);
                            nestedFilter = compileBooleanFilter(nestedFilterExpr, factory.getMetadata(), executionContext);
                            supportsParallelism = true;
                        } else {
                            Misc.free(candidateFactory);
                            // restore the lists
                            keyTypes.clear();
                            keyTypes.addAll(keyTypesCopy);
                            valueTypes.clear();
                            valueTypes.addAll(valueTypesCopy);
                            listColumnFilterA.clear();
                            listColumnFilterA.addAll(listColumnFilterCopy);
                        }
                    } catch (Throwable e) {
                        Misc.freeObjList(recordFunctions); // groupByFunctions are included in recordFunctions
                        Misc.freeObjList(keyFunctions);
                        throw e;
                    }
                }

                if (supportsParallelism) {
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
                                valueTypesCopy.getColumnCount(),
                                nestedFilter,
                                reduceTaskFactory,
                                compileWorkerFilterConditionally(
                                        nestedFilter,
                                        executionContext.getSharedWorkerCount(),
                                        nestedFilterExpr,
                                        factory.getMetadata(),
                                        executionContext
                                ),
                                executionContext.getSharedWorkerCount()
                        );
                    }

                    return new AsyncGroupByRecordCursorFactory(
                            asm,
                            configuration,
                            executionContext.getMessageBus(),
                            factory,
                            groupByMetadata,
                            listColumnFilterCopy,
                            keyTypesCopy,
                            valueTypesCopy,
                            groupByFunctions,
                            keyFunctions,
                            compileWorkerKeyFunctionsConditionally(
                                    keyFunctions,
                                    executionContext.getSharedWorkerCount(),
                                    keyFunctionNodes,
                                    factory.getMetadata(),
                                    executionContext
                            ),
                            recordFunctions,
                            nestedFilter,
                            reduceTaskFactory,
                            compileWorkerFilterConditionally(
                                    nestedFilter,
                                    executionContext.getSharedWorkerCount(),
                                    nestedFilterExpr,
                                    factory.getMetadata(),
                                    executionContext
                            ),
                            executionContext.getSharedWorkerCount()
                    );
                }
            }

            if (keyTypes.getColumnCount() == 0) {
                assert recordFunctions.size() == groupByFunctions.size();
                return new GroupByNotKeyedRecordCursorFactory(
                        asm,
                        factory,
                        groupByMetadata,
                        groupByFunctions,
                        valueTypes.getColumnCount()
                );
            }

            return new io.questdb.griffin.engine.groupby.GroupByRecordCursorFactory(
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
        ObjList<WindowFunction> naturalOrderFunctions = null;

        valueTypes.clear();
        ArrayColumnTypes chainTypes = valueTypes;
        GenericRecordMetadata chainMetadata = new GenericRecordMetadata();
        GenericRecordMetadata factoryMetadata = new GenericRecordMetadata();

        ObjList<Function> functions = new ObjList<>();

        // if all window function don't require sorting or more than one pass then use streaming factory
        boolean isFastPath = true;

        for (int i = 0; i < columnCount; i++) {
            final QueryColumn qc = columns.getQuick(i);
            if (qc.isWindowColumn()) {
                final WindowColumn ac = (WindowColumn) qc;
                final ExpressionNode ast = qc.getAst();
                if (ast.paramCount > 1) {
                    Misc.free(base);
                    throw SqlException.$(ast.position, "too many arguments");
                }

                ObjList<Function> partitionBy = null;
                int psz = ac.getPartitionBy().size();
                if (psz > 0) {
                    partitionBy = new ObjList<>(psz);
                    for (int j = 0; j < psz; j++) {
                        partitionBy.add(
                                functionParser.parseFunction(ac.getPartitionBy().getQuick(j), baseMetadata, executionContext)
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
                if (!dismissOrder
                        && osz == 1
                        && timestampIdx != -1
                        && orderHash.size() < 2) {
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
                        baseMetadata.getTimestampIndex()
                );
                final Function f;
                try {
                    f = functionParser.parseFunction(ast, baseMetadata, executionContext);
                    if (!(f instanceof WindowFunction)) {
                        Misc.free(base);
                        Misc.free(f);
                        throw SqlException.$(ast.position, "non-window function called in window context");
                    }

                    WindowFunction af = (WindowFunction) f;
                    functions.extendAndSet(i, f);

                    // sorting and/or  multiple passes are required, so fall back to old implementation
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

                if (baseMetadata.getTimestampIndex() != -1 &&
                        baseMetadata.getTimestampIndex() == columnIndex) {
                    factoryMetadata.setTimestampIndex(i);
                }

                if (Chars.equals(qc.getAst().token, qc.getAlias())) {
                    factoryMetadata.add(i, m);
                } else { // keep alias
                    factoryMetadata.add(i, new TableColumnMetadata(
                                    Chars.toString(qc.getAlias()),
                                    m.getType(),
                                    m.isIndexed(),
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
            Misc.freeObjList(functions);
            functions.clear();
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
                chainMetadata.add(i, m);
                if (Chars.equals(qc.getAst().token, qc.getAlias())) {
                    factoryMetadata.add(i, m);
                } else { // keep alias
                    factoryMetadata.add(i, new TableColumnMetadata(
                                    Chars.toString(qc.getAlias()),
                                    m.getType(),
                                    m.isIndexed(),
                                    m.getIndexValueBlockCapacity(),
                                    m.isSymbolTableStatic(),
                                    baseMetadata
                            )
                    );
                }
                chainTypes.add(i, m.getType());
                listColumnFilterA.extendAndSet(i, i + 1);
                listColumnFilterB.extendAndSet(i, columnIndex);
                intHashSet.add(columnIndex);
                columnIndexes.extendAndSet(i, columnIndex);

                if (baseMetadata.getTimestampIndex() != -1 &&
                        baseMetadata.getTimestampIndex() == columnIndex) {
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
                chainTypes.add(addAt, m.getType());
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
                if (ast.paramCount > 1) {
                    Misc.free(base);
                    throw SqlException.$(ast.position, "too many arguments");
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
                if (osz == 1 && timestampIdx != -1 && orderHash.size() < 2) {
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
                        chainMetadata.getTimestampIndex()
                );
                final Function f;
                try {
                    // function needs to resolve args against chain metadata
                    f = functionParser.parseFunction(ast, chainMetadata, executionContext);
                    if (!(f instanceof WindowFunction)) {
                        Misc.free(base);
                        throw SqlException.$(ast.position, "non-window function called in window context");
                    }
                } finally {
                    executionContext.clearWindowContext();
                }

                WindowFunction windowFunction = (WindowFunction) f;

                if (osz > 0 && !dismissOrder) {
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
                chainTypes.add(i, m.getType());
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
                false,
                listColumnFilterB
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
        final RecordCursorFactory factoryB = generateQuery0(model.getUnionModel(), executionContext, true);
        ObjList<Function> castFunctionsA = null;
        ObjList<Function> castFunctionsB = null;
        try {
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

        final GenericLexer.FloatingSequence tab = (GenericLexer.FloatingSequence) model.getTableName();
        final boolean supportsRandomAccess;
        if (Chars.startsWith(tab, NO_ROWID_MARKER)) {
            tab.setLo(tab.getLo() + NO_ROWID_MARKER.length());
            supportsRandomAccess = false;
        } else {
            supportsRandomAccess = true;
        }

        final TableToken tableToken = executionContext.getTableToken(tab);
        if (model.isUpdate() && !executionContext.isWalApplication() && executionContext.getCairoEngine().isWalTable(tableToken)) {
            // two phase update execution, this is client-side branch. It has to execute against the sequencer metadata
            // to allow the client to succeed even if WAL apply does not run.
            try (TableMetadata metadata = executionContext.getMetadataForWrite(tableToken, model.getMetadataVersion())) {
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
        final IntList columnSizes = new IntList();

        // topDownColumnCount can be 0 for 'select count()' queries

        int readerTimestampIndex;
        readerTimestampIndex = getTimestampIndex(model, metadata);

        // Latest by on a table requires the provided timestamp column to be the designated timestamp.
        if (latestBy.size() > 0 && readerTimestampIndex != metadata.getTimestampIndex()) {
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
                    int columnIndex = metadata.getColumnIndexQuiet(topDownColumns.getQuick(i).getName());
                    int type = metadata.getColumnType(columnIndex);
                    int typeSize = ColumnType.sizeOf(type);

                    columnIndexes.add(columnIndex);
                    columnSizes.add(Numbers.msb(typeSize));

                    myMeta.add(new TableColumnMetadata(
                            Chars.toString(topDownColumns.getQuick(i).getName()),
                            type,
                            metadata.isColumnIndexed(columnIndex),
                            metadata.getIndexValueBlockCapacity(columnIndex),
                            metadata.isSymbolTableStatic(columnIndex),
                            metadata.getMetadata(columnIndex)
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
                metadata,
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

        boolean orderDescendingByDesignatedTimestampOnly = isOrderDescendingByDesignatedTimestampOnly(model);
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

            DataFrameCursorFactory dfcFactory;

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
                        columnSizes,
                        prefixes
                );
            }

            // below code block generates index-based filter
            final boolean intervalHitsOnlyOnePartition;
            if (intrinsicModel.hasIntervalFilters()) {
                RuntimeIntrinsicIntervalModel intervalModel = intrinsicModel.buildIntervalModel();
                if (orderDescendingByDesignatedTimestampOnly) {
                    dfcFactory = new IntervalBwdDataFrameCursorFactory(
                            tableToken,
                            model.getMetadataVersion(),
                            intervalModel,
                            readerTimestampIndex,
                            dfcFactoryMeta
                    );
                } else {
                    dfcFactory = new IntervalFwdDataFrameCursorFactory(
                            tableToken,
                            model.getMetadataVersion(),
                            intervalModel,
                            readerTimestampIndex,
                            dfcFactoryMeta
                    );
                }
                intervalHitsOnlyOnePartition = intervalModel.allIntervalsHitOnePartition(reader.getPartitionedBy());
            } else {
                if (orderDescendingByDesignatedTimestampOnly) {
                    dfcFactory = new FullBwdDataFrameCursorFactory(tableToken, model.getMetadataVersion(), dfcFactoryMeta);
                } else {
                    dfcFactory = new FullFwdDataFrameCursorFactory(tableToken, model.getMetadataVersion(), dfcFactoryMeta);
                }
                intervalHitsOnlyOnePartition = reader.getPartitionedBy() == PartitionBy.NONE;
            }

            if (intrinsicModel.keyColumn != null) {
                // existence of column would have been already validated
                final int keyColumnIndex = metadata.getColumnIndexQuiet(intrinsicModel.keyColumn);
                final int nKeyValues = intrinsicModel.keyValueFuncs.size();
                final int nKeyExcludedValues = intrinsicModel.keyExcludedValueFuncs.size();

                if (intrinsicModel.keySubQuery != null) {
                    final RecordCursorFactory rcf = generate(intrinsicModel.keySubQuery, executionContext);
                    final Record.CharSequenceFunction func = validateSubQueryColumnAndGetGetter(intrinsicModel, rcf.getMetadata());

                    Function filter = compileFilter(intrinsicModel, myMeta, executionContext);
                    if (filter != null && filter.isConstant() && !filter.getBool(null)) {
                        Misc.free(dfcFactory);
                        return new EmptyTableRecordCursorFactory(myMeta);
                    }
                    return new FilterOnSubQueryRecordCursorFactory(
                            myMeta,
                            dfcFactory,
                            rcf,
                            keyColumnIndex,
                            filter,
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
                        final SymbolMapReader symbolMapReader = reader.getSymbolMapReader(keyColumnIndex);
                        final int symbolKey = symbolFunc.isRuntimeConstant()
                                ? SymbolTable.VALUE_NOT_FOUND
                                : symbolMapReader.keyOf(symbolFunc.getStr(null));

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
                                        indexDirection,
                                        columnIndexes
                                );
                            }
                        } else {
                            if (filter == null) {
                                rcf = new SymbolIndexRowCursorFactory(keyColumnIndex, symbolKey, true, indexDirection, null);
                            } else {
                                rcf = new SymbolIndexFilteredRowCursorFactory(keyColumnIndex, symbolKey, filter, true, indexDirection, columnIndexes, null);
                            }
                        }

                        if (filter == null) {
                            // This special case factory can later be disassembled to framing and index
                            // cursors in SAMPLE BY processing
                            return new DeferredSingleSymbolFilterDataFrameRecordCursorFactory(
                                    configuration,
                                    keyColumnIndex,
                                    symbolFunc,
                                    rcf,
                                    myMeta,
                                    dfcFactory,
                                    orderByKeyColumn || orderByTimestamp,
                                    columnIndexes,
                                    columnSizes,
                                    supportsRandomAccess
                            );
                        }
                        return new DataFrameRecordCursorFactory(
                                configuration,
                                myMeta,
                                dfcFactory,
                                rcf,
                                orderByKeyColumn || orderByTimestamp,
                                filter,
                                false,
                                columnIndexes,
                                columnSizes,
                                supportsRandomAccess
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
                            filter,
                            model.getOrderByAdviceMnemonic(),
                            orderByKeyColumn,
                            orderByTimestamp,
                            getOrderByDirectionOrDefault(model, 0),
                            indexDirection,
                            columnIndexes
                    );
                } else if (nKeyExcludedValues > 0) {
                    if (reader.getSymbolMapReader(keyColumnIndex).getSymbolCount() < configuration.getMaxSymbolNotEqualsCount()) {
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
                                configuration.getMaxSymbolNotEqualsCount()
                        );
                    } else if (intrinsicModel.keyExcludedNodes.size() > 0) {
                        // restore filter
                        ExpressionNode root = intrinsicModel.keyExcludedNodes.getQuick(0);

                        for (int i = 1, n = intrinsicModel.keyExcludedNodes.size(); i < n; i++) {
                            ExpressionNode expression = intrinsicModel.keyExcludedNodes.getQuick(i);

                            ExpressionNode newRoot = expressionNodePool.next();
                            newRoot.token = "and";
                            newRoot.type = OPERATION;
                            newRoot.precedence = 0;
                            newRoot.paramCount = 2;
                            newRoot.lhs = expression;
                            newRoot.rhs = root;

                            root = newRoot;
                        }

                        if (intrinsicModel.filter == null) {
                            intrinsicModel.filter = root;
                        } else {
                            ExpressionNode filter = expressionNodePool.next();
                            filter.token = "and";
                            filter.type = OPERATION;
                            filter.precedence = 0;
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

            RowCursorFactory rowFactory;
            if (orderDescendingByDesignatedTimestampOnly) {
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
                    columnSizes,
                    supportsRandomAccess
            );
        }

        // no where clause
        if (latestByColumnCount == 0) {
            // construct new metadata, which is a copy of what we constructed just above, but
            // in the interest of isolating problems we will only affect this factory

            AbstractDataFrameCursorFactory cursorFactory;
            RowCursorFactory rowCursorFactory;

            if (orderDescendingByDesignatedTimestampOnly) {
                cursorFactory = new FullBwdDataFrameCursorFactory(tableToken, model.getMetadataVersion(), dfcFactoryMeta);
                rowCursorFactory = new BwdDataFrameRowCursorFactory();
            } else {
                cursorFactory = new FullFwdDataFrameCursorFactory(tableToken, model.getMetadataVersion(), dfcFactoryMeta);
                rowCursorFactory = new DataFrameRowCursorFactory();
            }

            return new DataFrameRecordCursorFactory(
                    configuration,
                    myMeta,
                    cursorFactory,
                    rowCursorFactory,
                    orderDescendingByDesignatedTimestampOnly || isOrderByDesignatedTimestampOnly(model),
                    null,
                    framingSupported,
                    columnIndexes,
                    columnSizes,
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
                        myMeta,
                        configuration,
                        new FullBwdDataFrameCursorFactory(tableToken, model.getMetadataVersion(), dfcFactoryMeta),
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
                        new FullBwdDataFrameCursorFactory(tableToken, model.getMetadataVersion(), dfcFactoryMeta),
                        latestByColumnIndex,
                        null,
                        columnIndexes
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
                    myMeta,
                    configuration,
                    new FullBwdDataFrameCursorFactory(tableToken, model.getMetadataVersion(), dfcFactoryMeta),
                    RecordSinkFactory.getInstance(asm, myMeta, listColumnFilterA, false),
                    keyTypes,
                    partitionByColumnIndexes,
                    null,
                    null,
                    columnIndexes
            );
        }

        return new LatestByAllFilteredRecordCursorFactory(
                myMeta,
                configuration,
                new FullBwdDataFrameCursorFactory(tableToken, model.getMetadataVersion(), dfcFactoryMeta),
                RecordSinkFactory.getInstance(asm, myMeta, listColumnFilterA, false),
                keyTypes,
                null,
                columnIndexes
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
        entityColumnFilter.of(factoryA.getMetadata().getColumnCount());
        final RecordSink recordSink = RecordSinkFactory.getInstance(
                asm,
                unionMetadata,
                entityColumnFilter,
                true
        );
        valueTypes.clear();
        // Remap symbol columns to string type since that's how recordSink copies them.
        keyTypes.clear();
        for (int i = 0, n = unionMetadata.getColumnCount(); i < n; i++) {
            final int columnType = unionMetadata.getColumnType(i);
            if (ColumnType.isSymbol(columnType)) {
                keyTypes.add(ColumnType.STRING);
            } else {
                keyTypes.add(columnType);
            }
        }

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

    private boolean isOrderByDesignatedTimestampOnly(QueryModel model) {
        return model.getOrderByAdvice().size() == 1 && model.getTimestamp() != null &&
                Chars.equalsIgnoreCase(model.getOrderByAdvice().getQuick(0).token, model.getTimestamp().token);
    }

    private boolean isOrderDescendingByDesignatedTimestampOnly(QueryModel model) {
        return isOrderByDesignatedTimestampOnly(model) &&
                getOrderByDirectionOrDefault(model, 0) == ORDER_DIRECTION_DESCENDING;
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
                                .put("): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON");
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
            if (orderByDirection.getQuick(i) == QueryModel.ORDER_DIRECTION_DESCENDING) {
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

    private RecordMetadata widenSetMetadata(RecordMetadata typesA, RecordMetadata typesB) {
        int columnCount = typesA.getColumnCount();
        assert columnCount == typesB.getColumnCount();

        GenericRecordMetadata metadata = new GenericRecordMetadata();
        for (int i = 0; i < columnCount; i++) {
            int typeA = typesA.getColumnType(i);
            int typeB = typesB.getColumnType(i);

            if (typeA == typeB && typeA != ColumnType.SYMBOL) {
                metadata.add(typesA.getColumnMetadata(i));
            } else if (ColumnType.isToSameOrWider(typeB, typeA) && typeA != ColumnType.SYMBOL && typeA != ColumnType.CHAR) {
                // CHAR is "specially" assignable from SHORT, but we don't want that
                metadata.add(typesA.getColumnMetadata(i));
            } else if (ColumnType.isToSameOrWider(typeA, typeB) && typeB != ColumnType.SYMBOL) {
                // even though A is assignable to B (e.g. A union B)
                // set metadata will use A column names
                metadata.add(new TableColumnMetadata(
                        typesA.getColumnName(i),
                        typeB
                ));
            } else {
                // we can cast anything to string
                metadata.add(new TableColumnMetadata(
                        typesA.getColumnName(i),
                        ColumnType.STRING
                ));
            }
        }

        return metadata;
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
        sumConstructors.put(ColumnType.DATE, SumDateVectorAggregateFunction::new);
        sumConstructors.put(ColumnType.TIMESTAMP, SumTimestampVectorAggregateFunction::new);
        sumConstructors.put(ColumnType.SHORT, SumShortVectorAggregateFunction::new);

        ksumConstructors.put(ColumnType.DOUBLE, KSumDoubleVectorAggregateFunction::new);
        nsumConstructors.put(ColumnType.DOUBLE, NSumDoubleVectorAggregateFunction::new);

        avgConstructors.put(ColumnType.DOUBLE, AvgDoubleVectorAggregateFunction::new);
        avgConstructors.put(ColumnType.LONG, AvgLongVectorAggregateFunction::new);
        avgConstructors.put(ColumnType.TIMESTAMP, AvgLongVectorAggregateFunction::new);
        avgConstructors.put(ColumnType.DATE, AvgLongVectorAggregateFunction::new);
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

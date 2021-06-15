/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.cairo.sql.*;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.griffin.engine.LimitRecordCursorFactory;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.analytic.AnalyticFunction;
import io.questdb.griffin.engine.analytic.CachedAnalyticRecordCursorFactory;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.griffin.engine.groupby.*;
import io.questdb.griffin.engine.groupby.vect.GroupByRecordCursorFactory;
import io.questdb.griffin.engine.groupby.vect.*;
import io.questdb.griffin.engine.join.*;
import io.questdb.griffin.engine.orderby.RecordComparatorCompiler;
import io.questdb.griffin.engine.orderby.SortedLightRecordCursorFactory;
import io.questdb.griffin.engine.orderby.SortedRecordCursorFactory;
import io.questdb.griffin.engine.table.*;
import io.questdb.griffin.engine.union.*;
import io.questdb.griffin.model.*;
import io.questdb.std.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.SqlKeywords.*;
import static io.questdb.griffin.model.ExpressionNode.FUNCTION;
import static io.questdb.griffin.model.ExpressionNode.LITERAL;

public class SqlCodeGenerator implements Mutable {
    public static final int GKK_VANILLA_INT = 0;
    public static final int GKK_HOUR_INT = 1;
    private static final IntHashSet limitTypes = new IntHashSet();
    private static final FullFatJoinGenerator CREATE_FULL_FAT_LT_JOIN = SqlCodeGenerator::createFullFatLtJoin;
    private static final FullFatJoinGenerator CREATE_FULL_FAT_AS_OF_JOIN = SqlCodeGenerator::createFullFatAsOfJoin;
    private static final boolean[] joinsRequiringTimestamp = {false, false, false, true, true, false, true};
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> sumConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> ksumConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> nsumConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> avgConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> minConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> maxConstructors = new IntObjHashMap<>();
    private static final VectorAggregateFunctionConstructor countConstructor = (keyKind, columnIndex, workerCount) -> new CountVectorAggregateFunction(keyKind);
    private static final SetRecordCursorFactoryConstructor SET_UNION_CONSTRUCTOR = UnionRecordCursorFactory::new;
    private static final SetRecordCursorFactoryConstructor SET_INTERSECT_CONSTRUCTOR = IntersectRecordCursorFactory::new;
    private static final SetRecordCursorFactoryConstructor SET_EXCEPT_CONSTRUCTOR = ExceptRecordCursorFactory::new;
    private final WhereClauseParser whereClauseParser = new WhereClauseParser();
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
    private final ObjList<Function> symbolValueList = new ObjList<>();
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
    private final ObjObjHashMap<IntList, ObjList<AnalyticFunction>> grouppedAnalytic = new ObjObjHashMap<>();
    private final IntList recordFunctionPositions = new IntList();
    private final IntList groupByFunctionPositions = new IntList();
    private boolean fullFatJoins = false;

    public SqlCodeGenerator(
            CairoEngine engine,
            CairoConfiguration configuration,
            FunctionParser functionParser
    ) {
        this.engine = engine;
        this.configuration = configuration;
        this.functionParser = functionParser;
        this.recordComparatorCompiler = new RecordComparatorCompiler(asm);
    }

    @Override
    public void clear() {
        whereClauseParser.clear();
    }

    @NotNull
    public Function compileFilter(ExpressionNode expr, RecordMetadata metadata, SqlExecutionContext executionContext) throws SqlException {
        final Function filter = functionParser.parseFunction(expr, metadata, executionContext);
        if (filter.getType() == ColumnType.BOOLEAN) {
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
            return 0;
        }
        return model.getOrderByDirectionAdvice().getQuick(index);
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
            return countConstructor;
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
                    if (type == ColumnType.INT) {
                        tempKeyIndexesInBase.add(columnIndex);
                        tempKeyIndex.add(i);
                        arrayColumnTypes.add(ColumnType.INT);
                        tempKeyKinds.add(GKK_VANILLA_INT);
                    } else if (type == ColumnType.SYMBOL) {
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
        // when we join tables on strings - technically string is the key
        // and we do not need to store it in value, but we will still reject
        //
        // never mind, this is a stop-gap measure until I understand the problem
        // fully

        for (int k = 0, m = slaveMetadata.getColumnCount(); k < m; k++) {
            if (intHashSet.excludes(k)) {
                int type = slaveMetadata.getColumnType(k);
                if (type == ColumnType.STRING || type == ColumnType.BINARY) {
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
                if (m.getType() == ColumnType.SYMBOL) {
                    metadata.add(
                            slaveAlias,
                            m.getName(),
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
                if (type == ColumnType.SYMBOL) {
                    type = ColumnType.STRING;
                }
                metadata.add(
                        slaveAlias,
                        slaveMetadata.getColumnName(index),
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
            if (joinType == QueryModel.JOIN_INNER) {
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

        if (joinType == QueryModel.JOIN_INNER) {
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
        return new FilteredRecordCursorFactory(factory, f);
    }

    private RecordCursorFactory generateFunctionQuery(QueryModel model) throws SqlException {
        final Function function = model.getTableNameFunction();
        assert function != null;
        if (function.getType() != ColumnType.CURSOR) {
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
            assert n > 0;
            for (int i = 0; i < n; i++) {
                int index = ordered.getQuick(i);
                QueryModel slaveModel = joinModels.getQuick(index);

                // compile
                RecordCursorFactory slave = generateQuery(slaveModel, executionContext, index > 0);

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
                        case QueryModel.JOIN_CROSS:
                            master = new CrossJoinRecordCursorFactory(
                                    createJoinMetadata(masterAlias, masterMetadata, slaveModel.getName(), slaveMetadata),
                                    master,
                                    slave,
                                    masterMetadata.getColumnCount()
                            );
                            masterAlias = null;
                            break;
                        case QueryModel.JOIN_ASOF:
                            executionContext.pushTimestampRequiredFlag(true);
                            try {
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
                            } finally {
                                executionContext.popTimestampRequiredFlag();
                            }
                            break;
                        case QueryModel.JOIN_LT:
                            executionContext.pushTimestampRequiredFlag(true);
                            try {
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
                            } finally {
                                executionContext.popTimestampRequiredFlag();
                            }
                            break;
                        case QueryModel.JOIN_SPLICE:
                            executionContext.pushTimestampRequiredFlag(true);
                            try {
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
                                } else {
                                    assert false;
                                }
                            } finally {
                                executionContext.popTimestampRequiredFlag();
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

                // check if there are post-filters
                ExpressionNode filter = slaveModel.getPostJoinWhereClause();
                if (filter != null) {
                    master = new FilteredRecordCursorFactory(master, functionParser.parseFunction(filter, master.getMetadata(), executionContext));
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
    private RecordCursorFactory generateLatestByQuery(
            QueryModel model,
            TableReader reader,
            RecordMetadata metadata,
            String tableName,
            IntrinsicModel intrinsicModel,
            Function filter,
            SqlExecutionContext executionContext,
            int timestampIndex,
            @NotNull IntList columnIndexes
    ) throws SqlException {
        final DataFrameCursorFactory dataFrameCursorFactory;
        if (intrinsicModel.hasIntervalFilters()) {
            dataFrameCursorFactory = new IntervalBwdDataFrameCursorFactory(engine, tableName, model.getTableVersion(), intrinsicModel.buildIntervalModel(), timestampIndex);
        } else {
            dataFrameCursorFactory = new FullBwdDataFrameCursorFactory(engine, tableName, model.getTableVersion());
        }

        // 'latest by' clause takes over the filter
        model.setWhereClause(null);

        if (listColumnFilterA.size() == 1) {
            final int latestByIndex = listColumnFilterA.getColumnIndexFactored(0);
            final boolean indexed = metadata.isColumnIndexed(latestByIndex);

            if (metadata.getColumnType(latestByIndex) != ColumnType.SYMBOL) {
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

                final int nKeyValues = intrinsicModel.keyValues.size();
                if (indexed) {

                    assert nKeyValues > 0;
                    // deal with key values as a list
                    // 1. resolve each value of the list to "int"
                    // 2. get first row in index for each value (stream)

                    final SymbolMapReader symbolMapReader = reader.getSymbolMapReader(latestByIndex);
                    final RowCursorFactory rcf;
                    if (nKeyValues == 1) {
                        final CharSequence symbolValue = intrinsicModel.keyValues.get(0);
                        final int symbol = symbolMapReader.keyOf(symbolValue);

                        if (filter == null) {
                            if (symbol == SymbolTable.VALUE_NOT_FOUND) {
                                rcf = new LatestByValueDeferredIndexedRowCursorFactory(latestByIndex, Chars.toString(symbolValue), false);
                            } else {
                                rcf = new LatestByValueIndexedRowCursorFactory(latestByIndex, symbol, false);
                            }
                            return new DataFrameRecordCursorFactory(metadata, dataFrameCursorFactory, rcf, false, null, false, columnIndexes, null);
                        }

                        if (symbol == SymbolTable.VALUE_NOT_FOUND) {
                            return new LatestByValueDeferredIndexedFilteredRecordCursorFactory(
                                    metadata,
                                    dataFrameCursorFactory,
                                    latestByIndex,
                                    Chars.toString(symbolValue),
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
                            intrinsicModel.keyValues,
                            symbolMapReader,
                            filter,
                            columnIndexes
                    );
                }

                assert nKeyValues > 0;

                // we have "latest by" column values, but no index
                final SymbolMapReader symbolMapReader = reader.getSymbolMapReader(latestByIndex);

                if (nKeyValues > 1) {
                    return new LatestByValuesFilteredRecordCursorFactory(
                            configuration,
                            metadata,
                            dataFrameCursorFactory,
                            latestByIndex,
                            intrinsicModel.keyValues,
                            symbolMapReader,
                            filter,
                            columnIndexes
                    );
                }

                // we have a single symbol key
                int symbolKey = symbolMapReader.keyOf(intrinsicModel.keyValues.get(0));
                if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                    return new LatestByValueDeferredFilteredRecordCursorFactory(
                            metadata,
                            dataFrameCursorFactory,
                            latestByIndex,
                            Chars.toString(intrinsicModel.keyValues.get(0)),
                            filter,
                            columnIndexes
                    );
                }

                return new LatestByValueFilteredRecordCursorFactory(metadata, dataFrameCursorFactory, latestByIndex, symbolKey, filter, columnIndexes);
            }
            // we select all values of "latest by" column

            assert intrinsicModel.keyValues.size() == 0;
            // get latest rows for all values of "latest by" column

            if (indexed) {
                return new LatestByAllIndexedFilteredRecordCursorFactory(
                        configuration,
                        metadata,
                        dataFrameCursorFactory,
                        latestByIndex,
                        filter,
                        columnIndexes
                );
            }
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

    private RecordCursorFactory generateLimit(RecordCursorFactory factory, QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        ExpressionNode limitLo = model.getLimitLo();
        ExpressionNode limitHi = model.getLimitHi();

        if (limitLo == null && limitHi == null) {
            return factory;
        }

        final Function loFunc;
        final Function hiFunc;

        if (limitLo == null) {
            loFunc = LongConstant.ZERO;
        } else {
            loFunc = functionParser.parseFunction(limitLo, EmptyRecordMetadata.INSTANCE, executionContext);
            final int type = loFunc.getType();
            if (limitTypes.excludes(type)) {
                throw SqlException.$(limitLo.position, "invalid type: ").put(ColumnType.nameOf(type));
            }
        }

        if (limitHi != null) {
            hiFunc = functionParser.parseFunction(limitHi, EmptyRecordMetadata.INSTANCE, executionContext);
            final int type = hiFunc.getType();
            if (limitTypes.excludes(type)) {
                throw SqlException.$(limitHi.position, "invalid type: ").put(ColumnType.nameOf(type));
            }
        } else {
            hiFunc = null;
        }
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

    private RecordCursorFactory generateOrderBy(RecordCursorFactory recordCursorFactory, QueryModel model) throws SqlException {
        if (recordCursorFactory.followedOrderByAdvice()) {
            return recordCursorFactory;
        }
        try {
            final LowerCaseCharSequenceIntHashMap orderBy = model.getOrderHash();
            final ObjList<CharSequence> columnNames = orderBy.keys();
            final int size = columnNames.size();

            if (size > 0) {

                final RecordMetadata metadata = recordCursorFactory.getMetadata();
                listColumnFilterA.clear();
                intHashSet.clear();

                // column index sign indicates direction
                // therefore 0 index is not allowed
                for (int i = 0; i < size; i++) {
                    final CharSequence column = columnNames.getQuick(i);
                    int index = metadata.getColumnIndexQuiet(column);

                    // check if column type is supported
                    if (metadata.getColumnType(index) == ColumnType.BINARY) {
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
                //    by timestamp; we have nothing to do
                // 2. metadata of the new cursor will have timestamp

                RecordMetadata orderedMetadata;
                if (metadata.getTimestampIndex() != -1) {
                    CharSequence column = columnNames.getQuick(0);
                    int index = metadata.getColumnIndexQuiet(column);
                    if (index == metadata.getTimestampIndex()) {
                        if (size == 1 && orderBy.get(column) == QueryModel.ORDER_DIRECTION_ASCENDING) {
                            return recordCursorFactory;
                        }
                    }
                }
                orderedMetadata = GenericRecordMetadata.copyOfSansTimestamp(metadata);

                if (recordCursorFactory.recordCursorSupportsRandomAccess()) {
                    return new SortedLightRecordCursorFactory(
                            configuration,
                            orderedMetadata,
                            recordCursorFactory,
                            recordComparatorCompiler.compile(metadata, listColumnFilterA)
                    );
                }

                // when base record cursor does not support random access
                // we have to copy entire record into ordered structure

                entityColumnFilter.of(orderedMetadata.getColumnCount());

                return new SortedRecordCursorFactory(
                        configuration,
                        orderedMetadata,
                        recordCursorFactory,
                        orderedMetadata,
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
        );
    }

    @NotNull
    private RecordCursorFactory generateSampleBy(QueryModel model, SqlExecutionContext executionContext, ExpressionNode sampleByNode) throws SqlException {
        executionContext.pushTimestampRequiredFlag(true);
        try {
            final RecordCursorFactory factory = generateSubQuery(model, executionContext);

            // we require timestamp
            final int timestampIndex = getTimestampIndex(model, factory);
            if (timestampIndex == -1) {
                Misc.free(factory);
                throw SqlException.$(model.getModelPosition(), "base query does not provide dedicated TIMESTAMP column");
            }

            final RecordMetadata metadata = factory.getMetadata();
            final ObjList<ExpressionNode> sampleByFill = model.getSampleByFill();
            final TimestampSampler timestampSampler = TimestampSamplerFactory.getInstance(sampleByNode.token, sampleByNode.position);

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

                if (fillCount == 1 && Chars.equalsLowerCaseAscii(sampleByFill.getQuick(0).token, "prev")) {
                    if (keyTypes.getColumnCount() == 0) {
                        return new SampleByFillPrevNotKeyedRecordCursorFactory(
                                factory,
                                timestampSampler,
                                groupByMetadata,
                                groupByFunctions,
                                recordFunctions,
                                timestampIndex,
                                valueTypes.getColumnCount()
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
                            timestampIndex
                    );
                }

                if (fillCount == 0 || fillCount == 1 && Chars.equalsLowerCaseAscii(sampleByFill.getQuick(0).token, "none")) {

                    if (keyTypes.getColumnCount() == 0) {
                        // this sample by is not keyed
                        return new SampleByFillNoneNotKeyedRecordCursorFactory(
                                factory,
                                timestampSampler,
                                groupByMetadata,
                                groupByFunctions,
                                recordFunctions,
                                valueTypes.getColumnCount(),
                                timestampIndex
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
                            timestampIndex
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
                                timestampIndex
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
                            timestampIndex
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
                            timestampIndex
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
                        timestampIndex
                );
            } catch (SqlException | CairoException e) {
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
        grouppedAnalytic.clear();
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
                    ObjList<AnalyticFunction> funcs = grouppedAnalytic.get(order);
                    if (funcs == null) {
                        grouppedAnalytic.put(order, funcs = new ObjList<>());
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

        final ObjList<RecordComparator> analyticComparators = new ObjList<>(grouppedAnalytic.size());
        final ObjList<ObjList<AnalyticFunction>> functionGroups = new ObjList<>(grouppedAnalytic.size());
        for (ObjObjHashMap.Entry<IntList, ObjList<AnalyticFunction>> e : grouppedAnalytic) {
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

        final int timestampIndex = getTimestampIndex(model, factory);
        if (timestampIndex == -1 && executionContext.isTimestampRequired()) {
            Misc.free(factory);
            throw SqlException.$(model.getModelPosition(), "TIMESTAMP column is required but not provided");
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
                if (readerMetadata.getVersion() >= 416 && columnType == ColumnType.SYMBOL) {
                    final GenericRecordMetadata distinctSymbolMetadata = new GenericRecordMetadata();
                    distinctSymbolMetadata.add(BaseRecordMetadata.copyOf(readerMetadata, columnIndex));
                    return new DistinctSymbolRecordCursorFactory(
                            engine,
                            distinctSymbolMetadata,
                            Chars.toString(tableName),
                            columnIndex,
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
            return generateSampleBy(model, executionContext, sampleByNode);
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
                            new GenericRecordMetadata().add(new TableColumnMetadata(Chars.toString(columnName), ColumnType.LONG, null));
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

                    if (type == ColumnType.SYMBOL) {
                        meta.add(
                                indexInThis,
                                new TableColumnMetadata(
                                        Chars.toString(columns.getQuick(indexInThis).getName())
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

                    GroupByUtils.validateGroupByColumns(model, 1);

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

            if (specialCaseKeys) {
                // uh-oh, we had special case keys, but could not find implementation for the functions
                // release factory we created unnecessarily
                Misc.free(factory);
                // create factory on top level model
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
                    true,
                    timestampIndex
            );

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

                final Function function = functionParser.parseFunction(
                        column.getAst(),
                        metadata,
                        executionContext
                );
                // define "undefined" functions as string
                if (function.isUndefined()) {
                    function.assignType(ColumnType.STRING, executionContext.getBindVariableService());
                }
                functions.add(function);

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
                } else {
                    virtualMetadata.add(
                            new TableColumnMetadata(
                                    Chars.toString(column.getAlias()),
                                    function.getType(),
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
        final ExpressionNode whereClause = model.getWhereClause();

        try (TableReader reader = engine.getReader(
                executionContext.getCairoSecurityContext(),
                model.getTableName().token,
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
            try {
                readerTimestampIndex = getTimestampIndex(model, readerMeta);
            } catch (SqlException e) {
                Misc.free(reader);
                throw e;
            }

            boolean requiresTimestamp = joinsRequiringTimestamp[model.getJoinType()];
            final GenericRecordMetadata myMeta = new GenericRecordMetadata();
            boolean framingSupported;
            try {
                if (requiresTimestamp) {
                    executionContext.pushTimestampRequiredFlag(true);
                }

                if (topDownColumnCount > 0) {
                    framingSupported = true;
                    for (int i = 0; i < topDownColumnCount; i++) {
                        int columnIndex = readerMeta.getColumnIndexQuiet(topDownColumns.getQuick(i).getName());
                        int type = readerMeta.getColumnType(columnIndex);
                        int typeSize = ColumnType.sizeOf(type);

                        if (framingSupported && (typeSize < Byte.BYTES || typeSize > Double.BYTES)) {
                            // we don't frame non-primitive types yet
                            framingSupported = false;
                        }
                        columnIndexes.add(columnIndex);
                        columnSizes.add((Numbers.msb(typeSize)));

                        myMeta.add(new TableColumnMetadata(
                                Chars.toString(topDownColumns.getQuick(i).getName()),
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
                    if (readerTimestampIndex != -1 && myMeta.getTimestampIndex() == -1 && executionContext.isTimestampRequired()) {
                        myMeta.add(new TableColumnMetadata(
                                readerMeta.getColumnName(readerTimestampIndex),
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

            listColumnFilterA.clear();
            final int latestByColumnCount = latestBy.size();

            if (latestByColumnCount > 0) {
                // validate latest by against current reader
                // first check if column is valid
                for (int i = 0; i < latestByColumnCount; i++) {
                    final int index = myMeta.getColumnIndexQuiet(latestBy.getQuick(i).token);
                    if (index == -1) {
                        throw SqlException.invalidColumn(latestBy.getQuick(i).position, latestBy.getQuick(i).token);
                    }

                    // we are reusing collections which leads to confusing naming for this method
                    // keyTypes are types of columns we collect 'latest by' for
                    keyTypes.add(myMeta.getColumnType(index));
                    // columnFilterA are indexes of columns we collect 'latest by' for
                    listColumnFilterA.add(index + 1);
                }
            }

            final String tableName = reader.getTableName();

            if (whereClause != null) {
                CharSequence preferredKeyColumn = null;

                if (listColumnFilterA.size() == 1) {
                    final int latestByIndex = listColumnFilterA.getColumnIndexFactored(0);

                    if (myMeta.getColumnType(latestByIndex) == ColumnType.SYMBOL) {
                        preferredKeyColumn = latestBy.getQuick(0).token;
                    }
                }

                final IntrinsicModel intrinsicModel = whereClauseParser.extract(
                        model,
                        whereClause,
                        readerMeta,
                        preferredKeyColumn,
                        readerTimestampIndex,
                        functionParser,
                        myMeta,
                        executionContext
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
                        return new EmptyTableRecordCursorFactory(myMeta);
                    }

                    return generateLatestByQuery(
                            model,
                            reader,
                            myMeta,
                            tableName,
                            intrinsicModel,
                            f,
                            executionContext,
                            readerTimestampIndex,
                            columnIndexes
                    );
                }

                // below code block generates index-based filter

                final boolean intervalHitsOnlyOnePartition;
                if (intrinsicModel.hasIntervalFilters()) {
                    RuntimeIntrinsicIntervalModel intervalModel = intrinsicModel.buildIntervalModel();
                    dfcFactory = new IntervalFwdDataFrameCursorFactory(engine, tableName, model.getTableVersion(), intervalModel, readerTimestampIndex);
                    intervalHitsOnlyOnePartition = intervalModel.allIntervalsHitOnePartition(reader.getPartitionedBy());
                } else {
                    dfcFactory = new FullFwdDataFrameCursorFactory(engine, tableName, model.getTableVersion());
                    intervalHitsOnlyOnePartition = false;
                }

                if (intrinsicModel.keyColumn != null) {
                    // existence of column would have been already validated
                    final int keyColumnIndex = reader.getMetadata().getColumnIndexQuiet(intrinsicModel.keyColumn);
                    final int nKeyValues = intrinsicModel.keyValues.size();
                    final int nKeyExcludedValues = intrinsicModel.keyExcludedValues.size();

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

                    if (intrinsicModel.keyExcludedValues.size() == 0) {
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
                            final CharSequence symbol = intrinsicModel.keyValues.get(0);
                            final int symbolKey = reader.getSymbolMapReader(keyColumnIndex).keyOf(symbol);

                            if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                                if (f == null) {
                                    rcf = new DeferredSymbolIndexRowCursorFactory(keyColumnIndex,
                                            functionParser.createBindVariable(intrinsicModel.keyValuePositions.getQuick(0), symbol),
                                            true,
                                            indexDirection
                                    );
                                } else {
                                    rcf = new DeferredSymbolIndexFilteredRowCursorFactory(
                                            keyColumnIndex,
                                            functionParser.createBindVariable(intrinsicModel.keyValuePositions.getQuick(0), symbol),
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
                            return new DataFrameRecordCursorFactory(myMeta, dfcFactory, rcf, orderByKeyColumn, f, false, columnIndexes, columnSizes);
                        }

                        symbolValueList.clear();

                        for (int i = 0, n = intrinsicModel.keyValues.size(); i < n; i++) {
                            symbolValueList.add(functionParser.createBindVariable(intrinsicModel.keyValuePositions.getQuick(i), intrinsicModel.keyValues.get(i)));
                        }

                        if (orderByKeyColumn) {
                            myMeta.setTimestampIndex(-1);
                        }

                        return new FilterOnValuesRecordCursorFactory(
                                myMeta,
                                dfcFactory,
                                symbolValueList,
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
                            intrinsicModel.keyExcludedValues.size() > 0
                                    && reader.getSymbolMapReader(keyColumnIndex).size() < configuration.getMaxSymbolNotEqualsCount()
                    ) {
                        symbolValueList.clear();
                        for (int i = 0, n = intrinsicModel.keyExcludedValues.size(); i < n; i++) {
                            symbolValueList.add(functionParser.createBindVariable(intrinsicModel.keyExcludedValuePositions.getQuick(i), intrinsicModel.keyExcludedValues.get(i)));
                        }
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
                                symbolValueList,
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

                model.setWhereClause(intrinsicModel.filter);
                return new DataFrameRecordCursorFactory(myMeta, dfcFactory, new DataFrameRowCursorFactory(), false, null, framingSupported, columnIndexes, columnSizes);
            }

            // no where clause
            if (latestByColumnCount == 0) {

                // construct new metadata, which is a copy of what we constructed just above, but
                // in the interest of isolating problems we will only affect this factory

                return new TableReaderRecordCursorFactory(
                        myMeta,
                        engine,
                        tableName,
                        model.getTableVersion(),
                        columnIndexes,
                        columnSizes,
                        framingSupported
                );
            }

            if (latestByColumnCount == 1 && myMeta.isColumnIndexed(listColumnFilterA.getColumnIndexFactored(0))) {
                return new LatestByAllIndexedFilteredRecordCursorFactory(
                        configuration,
                        myMeta,
                        new FullBwdDataFrameCursorFactory(engine, tableName, model.getTableVersion()),
                        listColumnFilterA.getColumnIndexFactored(0),
                        null,
                        columnIndexes
                );
            }

            return new LatestByAllFilteredRecordCursorFactory(
                    myMeta,
                    configuration,
                    new FullBwdDataFrameCursorFactory(engine, tableName, model.getTableVersion()),
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
            if (metadata.getColumnType(timestampIndex) != ColumnType.TIMESTAMP) {
                throw SqlException.$(timestamp.position, "not a TIMESTAMP");
            }
            return timestampIndex;
        }
        return metadata.getTimestampIndex();
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
            int columnTypeA = slaveMetadata.getColumnType(listColumnFilterA.getColumnIndexFactored(k));
            int columnTypeB = masterMetadata.getColumnType(listColumnFilterB.getColumnIndexFactored(k));
            if (columnTypeB != columnTypeA && !(isSymbolOrString(columnTypeB) && isSymbolOrString(columnTypeA))) {
                // index in column filter and join context is the same
                throw SqlException.$(jc.aNodes.getQuick(k).position, "join column type mismatch");
            }
            keyTypes.add(columnTypeB == ColumnType.SYMBOL ? ColumnType.STRING : columnTypeB);
        }
    }

    private boolean isSymbolOrString(int columnType) {
        return (columnType == ColumnType.SYMBOL) || (columnType == ColumnType.STRING);
    }

    void setFullFatJoins(boolean fullFatJoins) {
        this.fullFatJoins = fullFatJoins;
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
        final int zeroColumnType = metadata.getColumnType(0);
        if (zeroColumnType != ColumnType.STRING && zeroColumnType != ColumnType.SYMBOL) {
            assert intrinsicModel.keySubQuery.getColumns() != null;
            assert intrinsicModel.keySubQuery.getColumns().size() > 0;

            throw SqlException
                    .position(intrinsicModel.keySubQuery.getColumns().getQuick(0).getAst().position)
                    .put("unsupported column type: ")
                    .put(metadata.getColumnName(0))
                    .put(": ")
                    .put(ColumnType.nameOf(zeroColumnType));
        }

        return zeroColumnType == ColumnType.STRING ? Record.GET_STR : Record.GET_SYM;
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

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
import io.questdb.cairo.map.RecordValueSinkFactory;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.griffin.engine.LimitRecordCursorFactory;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.griffin.engine.groupby.*;
import io.questdb.griffin.engine.groupby.vect.*;
import io.questdb.griffin.engine.join.*;
import io.questdb.griffin.engine.orderby.RecordComparatorCompiler;
import io.questdb.griffin.engine.orderby.SortedLightRecordCursorFactory;
import io.questdb.griffin.engine.orderby.SortedRecordCursorFactory;
import io.questdb.griffin.engine.table.*;
import io.questdb.griffin.engine.union.UnionAllRecordCursorFactory;
import io.questdb.griffin.engine.union.UnionRecordCursorFactory;
import io.questdb.griffin.model.*;
import io.questdb.std.*;
import io.questdb.std.microtime.Timestamps;
import org.jetbrains.annotations.NotNull;

import static io.questdb.griffin.SqlKeywords.isCountKeyword;
import static io.questdb.griffin.SqlKeywords.isNullKeyword;
import static io.questdb.griffin.model.ExpressionNode.FUNCTION;
import static io.questdb.griffin.model.ExpressionNode.LITERAL;

public class SqlCodeGenerator implements Mutable {
    private static final IntHashSet limitTypes = new IntHashSet();
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
    private final ObjList<CharSequence> symbolValueList = new ObjList<>();
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

    private GenericRecordMetadata copyMetadata(RecordMetadata that) {
        // todo: this metadata is immutable. Ideally we shouldn't be creating metadata for the same table over and over
        return GenericRecordMetadata.copyOf(that);
    }

    private RecordCursorFactory createAsOfJoin(
            RecordMetadata metadata,
            RecordCursorFactory master,
            RecordSink masterKeySink,
            RecordCursorFactory slave,
            RecordSink slaveKeySink,
            int columnSplit
    ) {
        valueTypes.reset();
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
    private RecordCursorFactory createFullFatAsOfJoin(
            RecordCursorFactory master,
            RecordMetadata masterMetadata,
            CharSequence masterAlias,
            RecordCursorFactory slave,
            RecordMetadata slaveMetadata,
            CharSequence slaveAlias,
            int joinPosition
    ) throws SqlException {

        // create hash set of key columns to easily find them
        intHashSet.clear();
        for (int i = 0, n = listColumnFilterA.getColumnCount(); i < n; i++) {
            intHashSet.add(listColumnFilterA.getColumnIndex(i));
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
        valueTypes.reset();
        ArrayColumnTypes slaveTypes = new ArrayColumnTypes();
        for (int i = 0, n = slaveMetadata.getColumnCount(); i < n; i++) {
            if (intHashSet.excludes(i)) {
                int type = slaveMetadata.getColumnType(i);
                metadata.add(
                        slaveAlias,
                        slaveMetadata.getColumnName(i),
                        type,
                        slaveMetadata.isColumnIndexed(i),
                        slaveMetadata.getIndexValueBlockCapacity(i),
                        slaveMetadata.isSymbolTableStatic(i)
                );
                listColumnFilterB.add(i);
                columnIndex.add(i);
                valueTypes.add(type);
                slaveTypes.add(type);
            }
        }

        // now add key columns to metadata
        for (int i = 0, n = listColumnFilterA.getColumnCount(); i < n; i++) {
            int index = listColumnFilterA.getColumnIndex(i);
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
                    slaveMetadata.isSymbolTableStatic(i)
            );
            columnIndex.add(index);
            slaveTypes.add(type);
        }

        if (masterMetadata.getTimestampIndex() != -1) {
            metadata.setTimestampIndex(masterMetadata.getTimestampIndex());
        }

        master = new AsOfJoinRecordCursorFactory(
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
        return master;
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

        valueTypes.reset();
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

    private RecordCursorFactory createSpliceJoin(
            RecordMetadata metadata,
            RecordCursorFactory master,
            RecordSink masterKeySink,
            RecordCursorFactory slave,
            RecordSink slaveKeySink,
            int columnSplit
    ) {
        valueTypes.reset();
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

    private ObjList<VectorAggregateFunction> createVectorAggregateFunctions(
            ObjList<QueryColumn> columns,
            RecordMetadata metadata,
            int workerCount
    ) {
        ObjList<VectorAggregateFunction> vafList = new ObjList<>();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final QueryColumn qc = columns.getQuick(i);
            final ExpressionNode ast = qc.getAst();
            if (isSingleColumnFunction(ast, "sum")) {
                final int columnIndex = metadata.getColumnIndex(ast.rhs.token);
                final int type = metadata.getColumnType(columnIndex);
                if (type == ColumnType.DOUBLE) {
                    vafList.add(new SumDoubleVectorAggregateFunction(ast.rhs.position, columnIndex, workerCount));
                    continue;
                } else if (type == ColumnType.INT) {
                    vafList.add(new SumIntVectorAggregateFunction(ast.rhs.position, columnIndex));
                    continue;
                } else if (type == ColumnType.LONG) {
                    vafList.add(new SumLongVectorAggregateFunction(ast.rhs.position, columnIndex));
                    continue;
                } else if (type == ColumnType.DATE) {
                    vafList.add(new SumDateVectorAggregateFunction(ast.rhs.position, columnIndex));
                    continue;
                } else if (type == ColumnType.TIMESTAMP) {
                    vafList.add(new SumTimestampVectorAggregateFunction(ast.rhs.position, columnIndex));
                    continue;
                }
            } else if (isSingleColumnFunction(ast, "ksum")) {
                final int columnIndex = metadata.getColumnIndex(ast.rhs.token);
                final int type = metadata.getColumnType(columnIndex);
                if (type == ColumnType.DOUBLE) {
                    vafList.add(new KSumDoubleVectorAggregateFunction(ast.rhs.position, columnIndex, workerCount));
                    continue;
                }
            } else if (isSingleColumnFunction(ast, "nsum")) {
                final int columnIndex = metadata.getColumnIndex(ast.rhs.token);
                final int type = metadata.getColumnType(columnIndex);
                if (type == ColumnType.DOUBLE) {
                    vafList.add(new NSumDoubleVectorAggregateFunction(ast.rhs.position, columnIndex, workerCount));
                    continue;
                }
            } else if (isSingleColumnFunction(ast, "avg")) {
                final int columnIndex = metadata.getColumnIndex(ast.rhs.token);
                final int type = metadata.getColumnType(columnIndex);
                if (type == ColumnType.DOUBLE) {
                    vafList.add(new AvgDoubleVectorAggregateFunction(ast.rhs.position, columnIndex));
                    continue;
                } else if (type == ColumnType.INT) {
                    vafList.add(new AvgIntVectorAggregateFunction(ast.rhs.position, columnIndex));
                    continue;
                } else if (type == ColumnType.LONG || type == ColumnType.TIMESTAMP || type == ColumnType.DATE) {
                    vafList.add(new AvgLongVectorAggregateFunction(ast.rhs.position, columnIndex));
                    continue;
                }
            } else if (isSingleColumnFunction(ast, "min")) {
                final int columnIndex = metadata.getColumnIndex(ast.rhs.token);
                final int type = metadata.getColumnType(columnIndex);
                if (type == ColumnType.DOUBLE) {
                    vafList.add(new MinDoubleVectorAggregateFunction(ast.rhs.position, columnIndex));
                    continue;
                } else if (type == ColumnType.INT) {
                    vafList.add(new MinIntVectorAggregateFunction(ast.rhs.position, columnIndex));
                    continue;
                } else if (type == ColumnType.LONG) {
                    vafList.add(new MinLongVectorAggregateFunction(ast.rhs.position, columnIndex));
                    continue;
                } else if (type == ColumnType.DATE) {
                    vafList.add(new MinDateVectorAggregateFunction(ast.rhs.position, columnIndex));
                    continue;
                } else if (type == ColumnType.TIMESTAMP) {
                    vafList.add(new MinTimestampVectorAggregateFunction(ast.rhs.position, columnIndex));
                    continue;
                }
            } else if (isSingleColumnFunction(ast, "max")) {
                final int columnIndex = metadata.getColumnIndex(ast.rhs.token);
                final int type = metadata.getColumnType(columnIndex);
                if (type == ColumnType.DOUBLE) {
                    vafList.add(new MaxDoubleVectorAggregateFunction(ast.rhs.position, columnIndex));
                    continue;
                } else if (type == ColumnType.INT) {
                    vafList.add(new MaxIntVectorAggregateFunction(ast.rhs.position, columnIndex));
                    continue;
                } else if (type == ColumnType.LONG) {
                    vafList.add(new MaxLongVectorAggregateFunction(ast.rhs.position, columnIndex));
                    continue;
                } else if (type == ColumnType.DATE) {
                    vafList.add(new MaxDateVectorAggregateFunction(ast.rhs.position, columnIndex));
                    continue;
                } else if (type == ColumnType.TIMESTAMP) {
                    vafList.add(new MaxTimestampVectorAggregateFunction(ast.rhs.position, columnIndex));
                    continue;
                }
            }
            return null;
        }
        return vafList;
    }

    RecordCursorFactory generate(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        return generateQuery(model, executionContext, true);
    }

    private RecordCursorFactory generateFilter(RecordCursorFactory factory, QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        final ExpressionNode filter = model.getWhereClause();
        if (filter != null) {
            return new FilteredRecordCursorFactory(factory, functionParser.parseFunction(filter, factory.getMetadata(), executionContext));
        }
        return factory;
    }

    private RecordCursorFactory generateFunctionQuery(QueryModel model) throws SqlException {
        final Function function = model.getTableNameFunction();
        assert function != null;
        if (function.getType() != TypeEx.CURSOR) {
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
                RecordCursorFactory slave = generateQuery(slaveModel, executionContext, i > 0);

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
                            return new CrossJoinRecordCursorFactory(
                                    createJoinMetadata(masterAlias, masterMetadata, slaveModel.getName(), slaveMetadata),
                                    master,
                                    slave,
                                    masterMetadata.getColumnCount()
                            );
                        case QueryModel.JOIN_ASOF:
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
                                master = createFullFatAsOfJoin(
                                        master,
                                        masterMetadata,
                                        masterAlias,
                                        slave,
                                        slaveMetadata,
                                        slaveModel.getName(),
                                        slaveModel.getJoinKeywordPosition()
                                );
                            }
                            masterAlias = null;
                            break;
                        case QueryModel.JOIN_SPLICE:
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
        } catch (CairoException | SqlException e) {
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
            int timestampIndex
    ) throws SqlException {
        final DataFrameCursorFactory dataFrameCursorFactory;
        if (intrinsicModel.intervals != null) {
            dataFrameCursorFactory = new IntervalBwdDataFrameCursorFactory(engine, tableName, model.getTableVersion(), intrinsicModel.intervals, timestampIndex);
        } else {
            dataFrameCursorFactory = new FullBwdDataFrameCursorFactory(engine, tableName, model.getTableVersion());
        }

        // 'latest by' clause takes over the filter
        model.setWhereClause(null);

        if (listColumnFilterA.size() == 1) {
            final int latestByIndex = listColumnFilterA.getColumnIndex(0);
            final boolean indexed = metadata.isColumnIndexed(latestByIndex);

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
                            func
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
                            return new DataFrameRecordCursorFactory(metadata, dataFrameCursorFactory, rcf, false, null);
                        }

                        if (symbol == SymbolTable.VALUE_NOT_FOUND) {
                            return new LatestByValueDeferredIndexedFilteredRecordCursorFactory(
                                    metadata,
                                    dataFrameCursorFactory,
                                    latestByIndex,
                                    Chars.toString(symbolValue),
                                    filter);
                        }
                        return new LatestByValueIndexedFilteredRecordCursorFactory(
                                metadata,
                                dataFrameCursorFactory,
                                latestByIndex,
                                symbol,
                                filter
                        );
                    }

                    return new LatestByValuesIndexedFilteredRecordCursorFactory(
                            configuration,
                            metadata,
                            dataFrameCursorFactory,
                            latestByIndex,
                            intrinsicModel.keyValues,
                            symbolMapReader,
                            filter
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
                            filter
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
                            filter
                    );
                }

                return new LatestByValueFilteredRecordCursorFactory(metadata, dataFrameCursorFactory, latestByIndex, symbolKey, filter);
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
                        filter);
            }
        }

        return new LatestByAllFilteredRecordCursorFactory(
                metadata,
                configuration,
                dataFrameCursorFactory,
                RecordSinkFactory.getInstance(asm, metadata, listColumnFilterA, false),
                keyTypes,
                filter
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
            loFunc = new LongConstant(0, 0L);
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
            final CharSequenceIntHashMap orderBy = model.getOrderHash();
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
                if (metadata.getTimestampIndex() == -1) {
                    orderedMetadata = GenericRecordMetadata.copyOfSansTimestamp(metadata);
                } else {
                    int index = metadata.getColumnIndexQuiet(columnNames.getQuick(0));
                    if (index == metadata.getTimestampIndex()) {

                        if (size == 1) {
                            return recordCursorFactory;
                        }

                        orderedMetadata = copyMetadata(metadata);

                    } else {
                        orderedMetadata = GenericRecordMetadata.copyOfSansTimestamp(metadata);
                    }
                }

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
            // todo: this looks like generic code
            final int timestampIndex = getTimestampIndex(model, factory);
            if (timestampIndex == -1) {
                Misc.free(factory);
                throw SqlException.$(model.getModelPosition(), "base query does not provide dedicated TIMESTAMP column");
            }

            final RecordMetadata metadata = factory.getMetadata();
            final ObjList<ExpressionNode> sampleByFill = model.getSampleByFill();
            final TimestampSampler timestampSampler = TimestampSamplerFactory.getInstance(sampleByNode.token, sampleByNode.position);

            assert model.getNestedModel() != null;
            final int fillCount = sampleByFill.size();
            try {
                keyTypes.reset();
                valueTypes.reset();
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
                        valueTypes
                );

                final ObjList<Function> recordFunctions = new ObjList<>(columnCount);
                final GenericRecordMetadata groupByMetadata = new GenericRecordMetadata();
                final IntList symbolTableSkewIndex = GroupByUtils.prepareGroupByRecordFunctions(
                        model,
                        metadata,
                        listColumnFilterA,
                        groupByFunctions,
                        recordFunctions,
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
                                symbolTableSkewIndex,
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
                            symbolTableSkewIndex,
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
                                symbolTableSkewIndex,
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
                            symbolTableSkewIndex,
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
                                symbolTableSkewIndex,
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
                            symbolTableSkewIndex,
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
                            symbolTableSkewIndex,
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
                        symbolTableSkewIndex,
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
            default:
                if (model.getJoinModels().size() > 1 && processJoins) {
                    return generateJoins(model, executionContext);
                }
                return generateNoSelect(model, executionContext);
        }
    }

    private RecordCursorFactory generateSelectAnalytic(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        return generateSubQuery(model, executionContext);
    }

    private RecordCursorFactory generateSelectChoose(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        assert model.getNestedModel() != null;
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

            selectMetadata.add(
                    new TableColumnMetadata(
                            Chars.toString(queryColumn.getName()),
                            metadata.getColumnType(index),
                            metadata.isColumnIndexed(index),
                            metadata.getIndexValueBlockCapacity(index),
                            metadata.isSymbolTableStatic(index)
                    )
            );

            if (index == timestampIndex) {
                selectMetadata.setTimestampIndex(i);
                timestampSet = true;
            }
        }

        if (!timestampSet && executionContext.isTimestampRequired()) {
            selectMetadata.add(
                    new TableColumnMetadata(
                            Chars.toString(metadata.getColumnName(timestampIndex)),
                            metadata.getColumnType(timestampIndex),
                            metadata.isColumnIndexed(timestampIndex),
                            metadata.getIndexValueBlockCapacity(timestampIndex),
                            metadata.isSymbolTableStatic(timestampIndex)
                    )
            );
        }

        return new SelectedRecordCursorFactory(selectMetadata, columnCrossIndex, factory);
    }

    private RecordCursorFactory generateSelectDistinct(QueryModel model, SqlExecutionContext executionContext) throws SqlException {

        QueryModel twoDeepNested;
        ExpressionNode tableNameEn;
        if (
                model.getBottomUpColumns().size() == 1
                        && model.getNestedModel() != null
                        && (twoDeepNested = model.getNestedModel().getNestedModel()) != null
                        && twoDeepNested.getWhereClause() == null
                        && twoDeepNested.getLatestBy().size() == 0
                        && (tableNameEn = twoDeepNested.getTableName()) != null
        ) {
            CharSequence tableName = tableNameEn.token;
            try (TableReader reader = engine.getReader(executionContext.getCairoSecurityContext(), tableName)) {
                CharSequence columnName = model.getBottomUpColumnNames().get(0);
                TableReaderMetadata readerMetadata = (TableReaderMetadata) reader.getMetadata();
                int columnIndex = readerMetadata.getColumnIndex(columnName);
                int columnType = readerMetadata.getColumnType(columnIndex);
                if (readerMetadata.getVersion() >= 416 && columnType == ColumnType.SYMBOL) {
                    final GenericRecordMetadata distinctSymbolMetadata = new GenericRecordMetadata();
                    long tableVersion = reader.getVersion();
                    distinctSymbolMetadata.add(
                            new TableColumnMetadata(
                                    Chars.toString(columnName),
                                    readerMetadata.getColumnType(columnIndex),
                                    readerMetadata.isColumnIndexed(columnIndex),
                                    readerMetadata.getIndexValueBlockCapacity(columnIndex),
                                    readerMetadata.isSymbolTableStatic(columnIndex)
                            )
                    );
                    return new DistinctSymbolRecordCursorFactory(
                            engine,
                            distinctSymbolMetadata,
                            Chars.toString(tableName),
                            columnIndex,
                            tableVersion
                    );
                }
            }
        }

        final RecordCursorFactory factory = generateSubQuery(model, executionContext);
        try {
            return new DistinctRecordCursorFactory(
                    configuration,
                    factory,
                    entityColumnFilter,
                    asm
            );
        } catch (CairoException e) {
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

        final RecordCursorFactory factory = generateSubQuery(model, executionContext);
        try {

            // generate special case plan for "select count() from somewhere"
            ObjList<QueryColumn> columns = model.getBottomUpColumns();
            if (columns.size() == 1) {
                QueryColumn column = columns.getQuick(0);
                if (column.getAst().type == FUNCTION && isCountKeyword(column.getAst().token)) {
                    if (isCountKeyword(column.getName())) {
                        return new CountRecordCursorFactory(CountRecordCursorFactory.DEFAULT_COUNT_METADATA, factory);
                    }

                    GenericRecordMetadata metadata = new GenericRecordMetadata();
                    metadata.add(new TableColumnMetadata(Chars.toString(column.getName()), ColumnType.LONG));
                    return new CountRecordCursorFactory(metadata, factory);
                }
            }

            final RecordMetadata metadata = factory.getMetadata();

            // inspect model for possibility of vector aggregate intrinsics
            if (factory.supportPageFrameCursor()) {
                final ObjList<VectorAggregateFunction> vafList = createVectorAggregateFunctions(columns, metadata, executionContext.getWorkerCount());
                if (vafList != null) {
                    final GenericRecordMetadata m = new GenericRecordMetadata();
                    for (int i = 0, n = vafList.size(); i < n; i++) {
                        m.add(new TableColumnMetadata(
                                Chars.toString(columns.getQuick(i).getName()),
                                vafList.getQuick(i).getType()
                        ));
                    }
                    return new GroupByNotKeyedVectorRecordCursorFactory(factory, m, vafList);
                }
            }

            final int timestampIndex = getTimestampIndex(model, factory);

            keyTypes.reset();
            valueTypes.reset();
            listColumnFilterA.clear();

            final int columnCount = model.getBottomUpColumns().size();
            ObjList<GroupByFunction> groupByFunctions = new ObjList<>(columnCount);
            GroupByUtils.prepareGroupByFunctions(
                    model,
                    metadata,
                    functionParser,
                    executionContext,
                    groupByFunctions,
                    valueTypes
            );

            final ObjList<Function> recordFunctions = new ObjList<>(columnCount);
            final GenericRecordMetadata groupByMetadata = new GenericRecordMetadata();
            final IntList symbolTableSkewIndex = GroupByUtils.prepareGroupByRecordFunctions(
                    model,
                    metadata,
                    listColumnFilterA,
                    groupByFunctions,
                    recordFunctions,
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

            return new GroupByRecordCursorFactory(
                    configuration,
                    factory,
                    listColumnFilterA,
                    asm,
                    keyTypes,
                    valueTypes,
                    groupByMetadata,
                    groupByFunctions,
                    recordFunctions,
                    symbolTableSkewIndex
            );

        } catch (CairoException e) {
            factory.close();
            throw e;
        }
    }

    private RecordCursorFactory generateSelectVirtual(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        assert model.getNestedModel() != null;
        final RecordCursorFactory factory = generateSubQuery(model, executionContext);

        try {
            final int columnCount = model.getBottomUpColumns().size();
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
                final QueryColumn column = model.getBottomUpColumns().getQuick(i);
                ExpressionNode node = column.getAst();
                if (timestampColumn != null && node.type == ExpressionNode.LITERAL && Chars.equals(timestampColumn, node.token)) {
                    virtualMetadata.setTimestampIndex(i);
                }

                final Function function = functionParser.parseFunction(
                        column.getAst(),
                        metadata,
                        executionContext
                );
                functions.add(function);


                if (function instanceof SymbolFunction) {
                    virtualMetadata.add(
                            new TableColumnMetadata(
                                    Chars.toString(column.getAlias()),
                                    function.getType(),
                                    false,
                                    0,
                                    ((SymbolFunction) function).isSymbolTableStatic()
                            )
                    );
                } else {
                    virtualMetadata.add(
                            new TableColumnMetadata(
                                    Chars.toString(column.getAlias()),
                                    function.getType()
                            )
                    );
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
        if (model.getUnionModelType() == QueryModel.UNION_MODEL_DISTINCT) {
            return generateUnionFactory(model, masterFactory, executionContext, slaveFactory);
        } else if (model.getUnionModelType() == QueryModel.UNION_MODEL_ALL) {
            return generateUnionAllFactory(model, masterFactory, executionContext, slaveFactory);
        }
        assert false;
        return null;
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

            final int readerTimestampIndex;
            try {
                readerTimestampIndex = getTimestampIndex(model, readerMeta);
            } catch (SqlException e) {
                Misc.free(reader);
                throw e;
            }

            final GenericRecordMetadata myMeta = new GenericRecordMetadata();
            boolean framingSupported;
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
                            readerMeta.isSymbolTableStatic(columnIndex)
                    ));

                    if (columnIndex == readerTimestampIndex) {
                        myMeta.setTimestampIndex(myMeta.getColumnCount() - 1);
                    }
                }

                // select timestamp when it is required but not already selected
                if (readerTimestampIndex != -1 && myMeta.getTimestampIndex() == -1 && executionContext.isTimestampRequired()) {
                    myMeta.add(new TableColumnMetadata(
                            readerMeta.getColumnName(readerTimestampIndex),
                            readerMeta.getColumnType(readerTimestampIndex)
                    ));
                    myMeta.setTimestampIndex(myMeta.getColumnCount() - 1);

                    columnIndexes.add(readerTimestampIndex);
                    columnSizes.add((Numbers.msb(ColumnType.TIMESTAMP)));
                }
            } else {
                framingSupported = false;
            }

            // done with myMeta

            final GenericRecordMetadata metadata = copyMetadata(readerMeta);
            final int timestampIndex;

            final ExpressionNode timestamp = model.getTimestamp();
            if (timestamp != null) {
                timestampIndex = metadata.getColumnIndexQuiet(timestamp.token);
                metadata.setTimestampIndex(timestampIndex);
            } else {
                timestampIndex = -1;
            }

            listColumnFilterA.clear();
            final int latestByColumnCount = latestBy.size();

            if (latestByColumnCount > 0) {
                // validate latest by against current reader
                // first check if column is valid
                for (int i = 0; i < latestByColumnCount; i++) {
                    final int index = metadata.getColumnIndexQuiet(latestBy.getQuick(i).token);
                    if (index == -1) {
                        throw SqlException.invalidColumn(latestBy.getQuick(i).position, latestBy.getQuick(i).token);
                    }

                    // we are reusing collections which leads to confusing naming for this method
                    // keyTypes are types of columns we collect 'latest by' for
                    keyTypes.add(metadata.getColumnType(index));
                    // columnFilterA are indexes of columns we collect 'latest by' for
                    listColumnFilterA.add(index);
                }
            }

            final String tableName = reader.getTableName();

            if (whereClause != null) {

                final IntrinsicModel intrinsicModel = whereClauseParser.extract(
                        model,
                        whereClause,
                        readerMeta,
                        latestByColumnCount > 0 ? latestBy.getQuick(0).token : null,
                        timestampIndex
                );

                // intrinsic parser can collapse where clause when removing parts it can replace
                // need to make sure that filter is updated on the model in case it is processed up the call stack
                //
                // At this juncture filter can use used up by one of the implementations below.
                // We will clear it preventively. If nothing picks filter up we will set model "where"
                // to the downsized filter
                model.setWhereClause(null);

                if (intrinsicModel.intrinsicValue == IntrinsicModel.FALSE) {
                    return new EmptyTableRecordCursorFactory(metadata);
                }

                Function filter;

                if (intrinsicModel.filter != null) {
                    filter = functionParser.parseFunction(intrinsicModel.filter, readerMeta, executionContext);

                    if (filter.getType() != ColumnType.BOOLEAN) {
                        throw SqlException.$(intrinsicModel.filter.position, "boolean expression expected");
                    }

                    if (filter.isConstant()) {
                        // can pass null to constant function
                        if (filter.getBool(null)) {
                            // filter is constant "true", do not evaluate for every row
                            filter = null;
                        } else {
                            return new EmptyTableRecordCursorFactory(metadata);
                        }
                    }
                } else {
                    filter = null;
                }

                DataFrameCursorFactory dfcFactory;

                if (latestByColumnCount > 0) {
                    return generateLatestByQuery(
                            model,
                            reader,
                            metadata,
                            tableName,
                            intrinsicModel,
                            filter,
                            executionContext,
                            timestampIndex
                    );
                }

                // below code block generates index-based filter

                final boolean intervalHitsOnlyOnePartition;
                if (intrinsicModel.intervals != null) {
                    dfcFactory = new IntervalFwdDataFrameCursorFactory(engine, tableName, model.getTableVersion(), intrinsicModel.intervals, timestampIndex);
                    switch (reader.getPartitionedBy()) {
                        case PartitionBy.DAY:
                            intervalHitsOnlyOnePartition = isFocused(intrinsicModel.intervals, Timestamps.FLOOR_DD);
                            break;
                        case PartitionBy.MONTH:
                            intervalHitsOnlyOnePartition = isFocused(intrinsicModel.intervals, Timestamps.FLOOR_MM);
                            break;
                        case PartitionBy.YEAR:
                            intervalHitsOnlyOnePartition = isFocused(intrinsicModel.intervals, Timestamps.FLOOR_YYYY);
                            break;
                        default:
                            intervalHitsOnlyOnePartition = true;
                            break;
                    }
                } else {
                    dfcFactory = new FullFwdDataFrameCursorFactory(engine, tableName, model.getTableVersion());
                    intervalHitsOnlyOnePartition = false;
                }

                if (intrinsicModel.keyColumn != null) {
                    // existence of column would have been already validated
                    final int keyColumnIndex = reader.getMetadata().getColumnIndexQuiet(intrinsicModel.keyColumn);
                    final int nKeyValues = intrinsicModel.keyValues.size();

                    if (intrinsicModel.keySubQuery != null) {
                        final RecordCursorFactory rcf = generate(intrinsicModel.keySubQuery, executionContext);
                        final Record.CharSequenceFunction func = validateSubQueryColumnAndGetGetter(intrinsicModel, rcf.getMetadata());

                        return new FilterOnSubQueryRecordCursorFactory(
                                metadata,
                                dfcFactory,
                                rcf,
                                keyColumnIndex,
                                filter,
                                func
                        );
                    }
                    assert nKeyValues > 0;

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
                                metadata.setTimestampIndex(-1);
                                if (orderByAdviceSize == 1) {
                                    orderByKeyColumn = true;
                                } else if (Chars.equals(orderByAdvice.getQuick(1).token, model.getTimestamp().token)) {
                                    orderByKeyColumn = true;
                                    if (model.getOrderByDirectionAdvice().getQuick(1) == QueryModel.ORDER_DIRECTION_DESCENDING) {
                                        indexDirection = BitmapIndexReader.DIR_BACKWARD;
                                    }
                                }
                            }
                        }
                    }

                    if (nKeyValues == 1) {
                        final RowCursorFactory rcf;
                        final CharSequence symbol = intrinsicModel.keyValues.get(0);
                        final int symbolKey = reader.getSymbolMapReader(keyColumnIndex).keyOf(symbol);
                        if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                            if (filter == null) {
                                rcf = new DeferredSymbolIndexRowCursorFactory(keyColumnIndex, Chars.toString(symbol), true, indexDirection);
                            } else {
                                rcf = new DeferredSymbolIndexFilteredRowCursorFactory(keyColumnIndex, Chars.toString(symbol), filter, true, indexDirection);
                            }
                        } else {
                            if (filter == null) {
                                rcf = new SymbolIndexRowCursorFactory(keyColumnIndex, symbolKey, true, indexDirection);
                            } else {
                                rcf = new SymbolIndexFilteredRowCursorFactory(keyColumnIndex, symbolKey, filter, true, indexDirection);
                            }
                        }
                        return new DataFrameRecordCursorFactory(metadata, dfcFactory, rcf, orderByKeyColumn, filter);
                    }

                    symbolValueList.clear();

                    for (int i = 0, n = intrinsicModel.keyValues.size(); i < n; i++) {
                        symbolValueList.add(intrinsicModel.keyValues.get(i));
                    }

                    if (orderByKeyColumn) {
                        metadata.setTimestampIndex(-1);
                        if (model.getOrderByDirectionAdvice().getQuick(0) == QueryModel.ORDER_DIRECTION_ASCENDING) {
                            symbolValueList.sort(Chars.CHAR_SEQUENCE_COMPARATOR);
                        } else {
                            symbolValueList.sort(Chars.CHAR_SEQUENCE_COMPARATOR_DESC);
                        }
                    }

                    return new FilterOnValuesRecordCursorFactory(
                            metadata,
                            dfcFactory,
                            symbolValueList,
                            keyColumnIndex,
                            reader,
                            filter,
                            model.getOrderByAdviceMnemonic(),
                            orderByKeyColumn,
                            indexDirection
                    );
                }

                // nothing used our filter
                // time to set "where" clause to the downsized filter (after intrinsic parser pass)
                model.setWhereClause(intrinsicModel.filter);

                if (intervalHitsOnlyOnePartition && filter == null) {
                    final ObjList<ExpressionNode> orderByAdvice = model.getOrderByAdvice();
                    final int orderByAdviceSize = orderByAdvice.size();
                    if (orderByAdviceSize > 0 && orderByAdviceSize < 3 && intrinsicModel.intervals != null) {
                        // we can only deal with 'order by symbol, timestamp' at best
                        // skip this optimisation if order by is more extensive
                        final int columnIndex = metadata.getColumnIndexQuiet(model.getOrderByAdvice().getQuick(0).token);
                        assert columnIndex > -1;

                        // this is our kind of column
                        if (metadata.isColumnIndexed(columnIndex)) {
                            boolean orderByKeyColumn = false;
                            int indexDirection = BitmapIndexReader.DIR_FORWARD;
                            if (orderByAdviceSize == 1) {
                                orderByKeyColumn = true;
                            } else if (Chars.equals(orderByAdvice.getQuick(1).token, model.getTimestamp().token)) {
                                orderByKeyColumn = true;
                                if (model.getOrderByDirectionAdvice().getQuick(1) == QueryModel.ORDER_DIRECTION_DESCENDING) {
                                    indexDirection = BitmapIndexReader.DIR_BACKWARD;
                                }
                            }

                            if (orderByKeyColumn) {
                                // check that intrinsicModel.intervals hit only one partition
                                metadata.setTimestampIndex(-1);
                                return new SortedSymbolIndexRecordCursorFactory(
                                        metadata,
                                        dfcFactory,
                                        columnIndex,
                                        model.getOrderByDirectionAdvice().getQuick(0) == QueryModel.ORDER_DIRECTION_ASCENDING,
                                        indexDirection
                                );
                            }
                        }
                    }
                }
                return new DataFrameRecordCursorFactory(metadata, dfcFactory, new DataFrameRowCursorFactory(), false, null);
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

            if (latestByColumnCount == 1 && metadata.isColumnIndexed(listColumnFilterA.getQuick(0))) {
                return new LatestByAllIndexedFilteredRecordCursorFactory(
                        configuration,
                        metadata,
                        new FullBwdDataFrameCursorFactory(engine, tableName, model.getTableVersion()),
                        listColumnFilterA.getQuick(0),
                        null
                );
            }

            return new LatestByAllFilteredRecordCursorFactory(
                    metadata,
                    configuration,
                    new FullBwdDataFrameCursorFactory(engine, tableName, model.getTableVersion()),
                    RecordSinkFactory.getInstance(asm, readerMeta, listColumnFilterA, false),
                    keyTypes,
                    null
            );
        }
    }

    private RecordCursorFactory generateUnionAllFactory(QueryModel model, RecordCursorFactory masterFactory, SqlExecutionContext executionContext, RecordCursorFactory slaveFactory) throws SqlException {
        validateJoinColumnTypes(model, masterFactory, slaveFactory);
        final RecordCursorFactory unionAllFactory = new UnionAllRecordCursorFactory(masterFactory, slaveFactory);

        if (model.getUnionModel().getUnionModel() != null) {
            return generateSetFactory(model.getUnionModel(), unionAllFactory, executionContext);
        }
        return unionAllFactory;
    }

    private RecordCursorFactory generateUnionFactory(QueryModel model, RecordCursorFactory masterFactory, SqlExecutionContext executionContext, RecordCursorFactory slaveFactory) throws SqlException {
        validateJoinColumnTypes(model, masterFactory, slaveFactory);
        entityColumnFilter.of(masterFactory.getMetadata().getColumnCount());
        final RecordSink recordSink = RecordSinkFactory.getInstance(
                asm,
                masterFactory.getMetadata(),
                entityColumnFilter,
                true
        );

        valueTypes.reset();

        RecordCursorFactory unionFactory = new UnionRecordCursorFactory(
                configuration,
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

    private boolean isFocused(LongList intervals, Timestamps.TimestampFloorMethod floorMethod) {
        long floor = floorMethod.floor(intervals.getQuick(0));
        for (int i = 1, n = intervals.size(); i < n; i++) {
            if (floor != floorMethod.floor(intervals.getQuick(i))) {
                return false;
            }
        }
        return true;
    }

    private boolean isSingleColumnFunction(ExpressionNode ast, CharSequence name) {
        return ast.type == FUNCTION && ast.paramCount == 1 && Chars.equals(ast.token, name) && ast.rhs.type == LITERAL;
    }

    private void lookupColumnIndexes(
            ListColumnFilter filter,
            ObjList<ExpressionNode> columnNames,
            RecordMetadata masterMetadata
    ) {
        filter.clear();
        for (int i = 0, n = columnNames.size(); i < n; i++) {
            filter.add(masterMetadata.getColumnIndex(columnNames.getQuick(i).token));
        }
    }

    private void lookupColumnIndexesUsingVanillaNames(
            ListColumnFilter filter,
            ObjList<CharSequence> columnNames,
            RecordMetadata metadata
    ) {
        filter.clear();
        for (int i = 0, n = columnNames.size(); i < n; i++) {
            filter.add(metadata.getColumnIndex(columnNames.getQuick(i)));
        }
    }

    private void processJoinContext(boolean vanillaMaster, JoinContext jc, RecordMetadata masterMetadata, RecordMetadata slaveMetadata) throws SqlException {
        lookupColumnIndexesUsingVanillaNames(listColumnFilterA, jc.aNames, slaveMetadata);
        if (vanillaMaster) {
            lookupColumnIndexesUsingVanillaNames(listColumnFilterB, jc.bNames, masterMetadata);
        } else {
            lookupColumnIndexes(listColumnFilterB, jc.bNodes, masterMetadata);
        }

        // compare types and populate keyTypes
        keyTypes.reset();
        for (int k = 0, m = listColumnFilterA.getColumnCount(); k < m; k++) {
            int columnType = masterMetadata.getColumnType(listColumnFilterB.getColumnIndex(k));
            if (columnType != slaveMetadata.getColumnType(listColumnFilterA.getColumnIndex(k))) {
                // index in column filter and join context is the same
                throw SqlException.$(jc.aNodes.getQuick(k).position, "join column type mismatch");
            }
            keyTypes.add(columnType == ColumnType.SYMBOL ? ColumnType.STRING : columnType);
        }
    }

    void setFullFatJoins(boolean fullFatJoins) {
        this.fullFatJoins = fullFatJoins;
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
            assert intrinsicModel.keySubQuery.getBottomUpColumns() != null;
            assert intrinsicModel.keySubQuery.getBottomUpColumns().size() > 0;

            throw SqlException
                    .position(intrinsicModel.keySubQuery.getBottomUpColumns().getQuick(0).getAst().position)
                    .put("unsupported column type: ")
                    .put(metadata.getColumnName(0))
                    .put(": ")
                    .put(ColumnType.nameOf(zeroColumnType));
        }

        return zeroColumnType == ColumnType.STRING ? Record.GET_STR : Record.GET_SYM;
    }

    static {
        limitTypes.add(ColumnType.LONG);
        limitTypes.add(ColumnType.BYTE);
        limitTypes.add(ColumnType.SHORT);
        limitTypes.add(ColumnType.INT);
    }
}

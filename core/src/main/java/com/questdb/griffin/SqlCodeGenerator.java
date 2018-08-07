/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin;

import com.questdb.cairo.*;
import com.questdb.cairo.sql.*;
import com.questdb.common.ColumnType;
import com.questdb.common.SymbolTable;
import com.questdb.griffin.engine.RecordComparatorCompiler;
import com.questdb.griffin.engine.SortedLightRecordCursorFactory;
import com.questdb.griffin.engine.functions.columns.SymbolColumn;
import com.questdb.griffin.engine.table.*;
import com.questdb.griffin.model.ExpressionNode;
import com.questdb.griffin.model.IntrinsicModel;
import com.questdb.griffin.model.QueryColumn;
import com.questdb.griffin.model.QueryModel;
import com.questdb.std.*;
import org.jetbrains.annotations.NotNull;

public class SqlCodeGenerator {
    private final WhereClauseParser filterAnalyser = new WhereClauseParser();
    private final FunctionParser functionParser;
    private final CairoEngine engine;
    private final BytecodeAssembler asm = new BytecodeAssembler();
    // this list is used to generate record sinks
    private final ListColumnFilter listColumnFilter = new ListColumnFilter();
    private final SingleColumnType latestByColumnTypes = new SingleColumnType();
    private final CairoConfiguration configuration;
    private final RecordComparatorCompiler recordComparatorCompiler;
    private final IntHashSet intHashSet = new IntHashSet();

    public SqlCodeGenerator(CairoEngine engine, CairoConfiguration configuration, FunctionParser functionParser) {
        this.engine = engine;
        this.configuration = configuration;
        this.functionParser = functionParser;
        this.recordComparatorCompiler = new RecordComparatorCompiler(asm);
    }

    private void clearState() {
        // todo: clear
    }

    private RecordMetadata copyMetadata(RecordMetadata that) {
        // todo: this metadata is immutable. Ideally we shouldn't be creating metadata for the same table over and over
        return GenericRecordMetadata.copyOf(that);
    }

    RecordCursorFactory generate(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        clearState();
        return generateQuery(model, executionContext);
    }

    private RecordCursorFactory generateFunctionQuery(QueryModel model) throws SqlException {
        final Function function = model.getTableNameFunction();
        assert function != null;
        if (function.getType() != TypeEx.CURSOR) {
            throw SqlException.position(model.getTableName().position).put("function must return CURSOR [actual=").put(ColumnType.nameOf(function.getType())).put(']');
        }

        return function.getRecordCursorFactory();
    }

    @NotNull
    private RecordCursorFactory generateLatestByQuery(
            QueryModel model,
            TableReader reader,
            RecordMetadata metadata,
            int latestByIndex,
            String tableName,
            IntrinsicModel intrinsicModel,
            Function filter,
            SqlExecutionContext executionContext) throws SqlException {
        final boolean indexed = metadata.isColumnIndexed(latestByIndex);
        final DataFrameCursorFactory dataFrameCursorFactory;
        if (intrinsicModel.intervals != null) {
            dataFrameCursorFactory = new IntervalBwdDataFrameCursorFactory(engine, tableName, model.getTableVersion(), intrinsicModel.intervals);
        } else {
            dataFrameCursorFactory = new FullBwdDataFrameCursorFactory(engine, tableName, model.getTableVersion());
        }

        if (intrinsicModel.keyColumn != null) {
            // key column must always be the same as latest by column
            assert latestByIndex == metadata.getColumnIndexQuiet(intrinsicModel.keyColumn);

            if (intrinsicModel.keySubQuery != null) {

                final RecordCursorFactory rcf = generate(intrinsicModel.keySubQuery, executionContext);
                final int firstColumnType = validateSubQueryColumnAndGetType(intrinsicModel, rcf.getMetadata());

                return new LatestBySubQueryRecordCursorFactory(
                        configuration,
                        metadata,
                        dataFrameCursorFactory,
                        latestByIndex,
                        rcf,
                        filter,
                        indexed,
                        firstColumnType
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
                    final int symbol = symbolMapReader.getQuick(symbolValue);

                    if (filter == null) {
                        if (symbol == SymbolTable.VALUE_NOT_FOUND) {
                            rcf = new LatestByValueDeferredIndexedRowCursorFactory(latestByIndex, Chars.toString(symbolValue), false);
                        } else {
                            rcf = new LatestByValueIndexedRowCursorFactory(latestByIndex, symbol, false);
                        }
                        return new DataFrameRecordCursorFactory(copyMetadata(metadata), dataFrameCursorFactory, rcf, null);
                    }

                    if (symbol == SymbolTable.VALUE_NOT_FOUND) {
                        return new LatestByValueDeferredIndexedFilteredRecordCursorFactory(
                                copyMetadata(metadata),
                                dataFrameCursorFactory,
                                latestByIndex,
                                Chars.toString(symbolValue),
                                filter);
                    }
                    return new LatestByValueIndexedFilteredRecordCursorFactory(
                            copyMetadata(metadata),
                            dataFrameCursorFactory,
                            latestByIndex,
                            symbol,
                            filter);
                }

                return new LatestByValuesIndexedFilteredRecordCursorFactory(
                        configuration,
                        copyMetadata(metadata),
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
                        copyMetadata(metadata),
                        dataFrameCursorFactory,
                        latestByIndex,
                        intrinsicModel.keyValues,
                        symbolMapReader,
                        filter
                );
            }

            // we have a single symbol key
            int symbolKey = symbolMapReader.getQuick(intrinsicModel.keyValues.get(0));
            if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                return new LatestByValueDeferredFilteredRecordCursorFactory(
                        copyMetadata(metadata),
                        dataFrameCursorFactory,
                        latestByIndex,
                        Chars.toString(intrinsicModel.keyValues.get(0)),
                        filter
                );
            }

            return new LatestByValueFilteredRecordCursorFactory(copyMetadata(metadata), dataFrameCursorFactory, latestByIndex, symbolKey, filter);
        }
        // we select all values of "latest by" column

        assert intrinsicModel.keyValues.size() == 0;
        // get latest rows for all values of "latest by" column

        if (indexed) {
            return new LatestByAllIndexedFilteredRecordCursorFactory(
                    configuration,
                    copyMetadata(metadata),
                    dataFrameCursorFactory,
                    latestByIndex,
                    filter);
        }

        listColumnFilter.clear();
        listColumnFilter.add(latestByIndex);
        return new LatestByAllFilteredRecordCursorFactory(
                copyMetadata(metadata),
                configuration,
                dataFrameCursorFactory,
                RecordSinkFactory.getInstance(asm, metadata, listColumnFilter, false),
                latestByColumnTypes.of(metadata.getColumnType(latestByIndex)),
                filter
        );
    }

    private RecordCursorFactory generateNoSelect(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        ExpressionNode tableName = model.getTableName();
        if (tableName != null) {
            if (tableName.type == ExpressionNode.FUNCTION) {
                return generateFunctionQuery(model);
            } else {
                return generateTableQuery(model, executionContext);
            }

        }
        assert model.getNestedModel() != null;
        return generateQuery(model.getNestedModel(), executionContext);
    }

    private RecordCursorFactory generateOrderBy(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        final RecordCursorFactory recordCursorFactory = generateSelect(model, executionContext);
        final ObjList<ExpressionNode> orderBy = model.getOrderBy();
        final int size = orderBy.size();

        if (size > 0) {

            final RecordMetadata metadata = recordCursorFactory.getMetadata();
            final IntList orderByDirection = model.getOrderByDirection();
            listColumnFilter.clear();
            intHashSet.clear();

            // column index sign indicates direction
            // therefore 0 index is not allowed
            for (int i = 0; i < size; i++) {
                ExpressionNode node = orderBy.getQuick(i);
                int index = metadata.getColumnIndexQuiet(node.token);

                // check if column type is supported
                if (metadata.getColumnType(index) == ColumnType.BINARY) {
                    throw SqlException.$(node.position, "unsupported column type: ").put(ColumnType.nameOf(metadata.getColumnType(index)));
                }

                // we also maintain unique set of column indexes for better performance
                if (intHashSet.add(index)) {
                    if (orderByDirection.getQuick(i) == QueryModel.ORDER_DIRECTION_DESCENDING) {
                        listColumnFilter.add(-index - 1);
                    } else {
                        listColumnFilter.add(index + 1);
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
                int index = metadata.getColumnIndexQuiet(orderBy.getQuick(0).token);
                if (index == metadata.getTimestampIndex()) {

                    if (size == 1) {
                        return recordCursorFactory;
                    }

                    orderedMetadata = copyMetadata(metadata);

                } else {
                    orderedMetadata = GenericRecordMetadata.copyOfSansTimestamp(metadata);
                }
            }

            return new SortedLightRecordCursorFactory(
                    configuration,
                    orderedMetadata,
                    recordCursorFactory,
                    recordComparatorCompiler.compile(metadata, listColumnFilter)
            );
        }

        return recordCursorFactory;
    }

    private RecordCursorFactory generateQuery(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        return generateOrderBy(model, executionContext);
    }

    private RecordCursorFactory generateSelect(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        switch (model.getSelectModelType()) {
            case QueryModel.SELECT_MODEL_CHOOSE:
                return generateSelectChoose(model, executionContext);
            case QueryModel.SELECT_MODEL_GROUP_BY:
                return generateSelectGroupBy(model, executionContext);
            case QueryModel.SELECT_MODEL_VIRTUAL:
                return generateSelectVirtual(model, executionContext);
            case QueryModel.SELECT_MODEL_ANALYTIC:
                return generateSelectAnalytic(model, executionContext);
            default:
                return generateNoSelect(model, executionContext);
        }
    }

    private RecordCursorFactory generateSelectAnalytic(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        assert model.getNestedModel() != null;
        return generateQuery(model.getNestedModel(), executionContext);
    }

    private RecordCursorFactory generateSelectChoose(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        assert model.getNestedModel() != null;
        final RecordCursorFactory factory = generateQuery(model.getNestedModel(), executionContext);
        final RecordMetadata metadata = factory.getMetadata();
        final int selectColumnCount = model.getColumns().size();
        final ExpressionNode timestamp = model.getTimestamp();

        boolean entity;
        // the model is considered entity when it doesn't add any value to its nested model
        //
        if (timestamp == null && metadata.getColumnCount() == selectColumnCount) {
            entity = true;
            for (int i = 0; i < selectColumnCount; i++) {
                if (!Chars.equals(metadata.getColumnName(i), model.getColumns().getQuick(i).getAst().token)) {
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

        IntList columnCrossIndex = new IntList(selectColumnCount);
        GenericRecordMetadata selectMetadata = new GenericRecordMetadata();
        final int timestampIndex;
        if (timestamp == null) {
            timestampIndex = metadata.getTimestampIndex();
        } else {
            timestampIndex = metadata.getColumnIndex(timestamp.token);
        }
        for (int i = 0; i < selectColumnCount; i++) {
            int index = metadata.getColumnIndexQuiet(model.getColumns().getQuick(i).getAst().token);
            assert index > -1 : "wtf? " + model.getColumns().getQuick(i).getAst().token;
            columnCrossIndex.add(index);

            selectMetadata.add(new TableColumnMetadata(
                    Chars.toString(model.getColumns().getQuick(i).getName()),
                    metadata.getColumnType(index),
                    metadata.isColumnIndexed(index),
                    metadata.getIndexValueBlockCapacity(index)
            ));

            if (index == timestampIndex) {
                selectMetadata.setTimestampIndex(i);
            }
        }

        return new SelectedRecordCursorFactory(selectMetadata, columnCrossIndex, factory);
    }

    private RecordCursorFactory generateSelectGroupBy(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        assert model.getNestedModel() != null;
        return generateQuery(model.getNestedModel(), executionContext);
    }

    private RecordCursorFactory generateSelectVirtual(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        assert model.getNestedModel() != null;
        RecordCursorFactory factory = generateQuery(model.getNestedModel(), executionContext);

        final int columnCount = model.getColumns().size();
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

        IntList symbolTableCrossIndex = null;

        for (int i = 0; i < columnCount; i++) {
            final QueryColumn column = model.getColumns().getQuick(i);
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


            virtualMetadata.add(new TableColumnMetadata(
                    Chars.toString(column.getAlias()),
                    function.getType()
            ));

            if (function instanceof SymbolColumn) {
                if (symbolTableCrossIndex == null) {
                    symbolTableCrossIndex = new IntList(columnCount);
                }
                symbolTableCrossIndex.extendAndSet(i, ((SymbolColumn) function).getColumnIndex());
            }
        }

        return new VirtualRecordCursorFactory(virtualMetadata, functions, factory, symbolTableCrossIndex);
    }

    @SuppressWarnings("ConstantConditions")
    private RecordCursorFactory generateTableQuery(QueryModel model, SqlExecutionContext executionContext) throws SqlException {

//        applyLimit(model);

        final ExpressionNode latestBy = model.getLatestBy();
        final ExpressionNode whereClause = model.getWhereClause();

        try (TableReader reader = engine.getReader(model.getTableName().token, model.getTableVersion())) {
            final RecordMetadata metadata = reader.getMetadata();

            final int latestByIndex;
            if (latestBy != null) {
                // validate latest by against current reader
                // first check if column is valid
                latestByIndex = metadata.getColumnIndexQuiet(latestBy.token);
                if (latestByIndex == -1) {
                    throw SqlException.invalidColumn(latestBy.position, latestBy.token);
                }
            } else {
                latestByIndex = -1;
            }

            final String tableName = Chars.toString(model.getTableName().token);

            if (whereClause != null) {

                final int timestampIndex;

                final ExpressionNode timestamp = model.getTimestamp();
                if (timestamp != null) {
                    timestampIndex = metadata.getColumnIndexQuiet(timestamp.token);
                } else {
                    timestampIndex = -1;
                }

                final IntrinsicModel intrinsicModel = filterAnalyser.extract(model, whereClause, metadata, latestBy != null ? latestBy.token : null, timestampIndex);

                if (intrinsicModel.intrinsicValue == IntrinsicModel.FALSE) {
                    return new EmptyTableRecordCursorFactory(copyMetadata(metadata));
                }

                Function filter;

                if (intrinsicModel.filter != null) {
                    filter = functionParser.parseFunction(intrinsicModel.filter, metadata, executionContext);

                    if (filter.getType() != ColumnType.BOOLEAN) {
                        throw SqlException.$(intrinsicModel.filter.position, "boolean expression expected");
                    }

                    if (filter.isConstant()) {
                        // can pass null to constant function
                        if (filter.getBool(null)) {
                            // filter is constant "true", do not evaluate for every row
                            filter = null;
                        } else {
                            return new EmptyTableRecordCursorFactory(copyMetadata(metadata));
                        }
                    }
                } else {
                    filter = null;
                }

                DataFrameCursorFactory dfcFactory;

                if (latestByIndex > -1) {
                    return generateLatestByQuery(
                            model,
                            reader,
                            metadata,
                            latestByIndex,
                            tableName,
                            intrinsicModel,
                            filter,
                            executionContext);
                }


                // below code block generates index-based filter

                if (intrinsicModel.intervals != null) {
                    dfcFactory = new IntervalFwdDataFrameCursorFactory(engine, tableName, model.getTableVersion(), intrinsicModel.intervals);
                } else {
                    dfcFactory = new FullFwdDataFrameCursorFactory(engine, tableName, model.getTableVersion());
                }

                if (intrinsicModel.keyColumn != null) {
                    // existence of column would have been already validated
                    final int keyColumnIndex = reader.getMetadata().getColumnIndexQuiet(intrinsicModel.keyColumn);
                    final int nKeyValues = intrinsicModel.keyValues.size();

                    if (intrinsicModel.keySubQuery != null) {
                        final RecordCursorFactory rcf = generate(intrinsicModel.keySubQuery, executionContext);
                        final int firstColumnType = validateSubQueryColumnAndGetType(intrinsicModel, rcf.getMetadata());

                        return new FilterOnSubQueryRecordCursorFactory(
                                metadata,
                                dfcFactory,
                                rcf,
                                keyColumnIndex,
                                filter,
                                firstColumnType
                        );
                    }
                    assert nKeyValues > 0;

                    if (nKeyValues == 1) {
                        final RowCursorFactory rcf;
                        final CharSequence symbol = intrinsicModel.keyValues.get(0);
                        final int symbolKey = reader.getSymbolMapReader(keyColumnIndex).getQuick(symbol);
                        if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                            if (filter == null) {
                                rcf = new DeferredSymbolIndexRowCursorFactory(keyColumnIndex, Chars.toString(symbol), true);
                            } else {
                                rcf = new DeferredSymbolIndexFilteredRowCursorFactory(keyColumnIndex, Chars.toString(symbol), filter, true);
                            }
                        } else {
                            if (filter == null) {
                                rcf = new SymbolIndexRowCursorFactory(keyColumnIndex, symbolKey, true);
                            } else {
                                rcf = new SymbolIndexFilteredRowCursorFactory(keyColumnIndex, symbolKey, filter, true);
                            }
                        }
                        return new DataFrameRecordCursorFactory(copyMetadata(metadata), dfcFactory, rcf, filter);
                    }

                    return new FilterOnValuesRecordCursorFactory(
                            metadata,
                            dfcFactory,
                            intrinsicModel.keyValues,
                            keyColumnIndex,
                            reader,
                            filter
                    );
                }

                if (filter != null) {
                    // filter lifecycle is managed by top level
                    return new FilteredRecordCursorFactory(new DataFrameRecordCursorFactory(copyMetadata(metadata), dfcFactory, new DataFrameRowCursorFactory(), null), filter);
                }
                return new DataFrameRecordCursorFactory(copyMetadata(metadata), dfcFactory, new DataFrameRowCursorFactory(), filter);
            }

            // no where clause
            if (latestByIndex == -1) {
                return new TableReaderRecordCursorFactory(copyMetadata(metadata), engine, tableName, model.getTableVersion());
            }

            if (metadata.isColumnIndexed(latestByIndex)) {
                return new LatestByAllIndexedFilteredRecordCursorFactory(
                        configuration,
                        copyMetadata(metadata),
                        new FullBwdDataFrameCursorFactory(engine, tableName, model.getTableVersion()),
                        latestByIndex,
                        null);
            }

            listColumnFilter.clear();
            listColumnFilter.add(latestByIndex);
            return new LatestByAllFilteredRecordCursorFactory(
                    copyMetadata(metadata),
                    configuration,
                    new FullBwdDataFrameCursorFactory(engine, tableName, model.getTableVersion()),
                    RecordSinkFactory.getInstance(asm, metadata, listColumnFilter, false),
                    latestByColumnTypes.of(metadata.getColumnType(latestByIndex)),
                    null
            );
        }
    }

    private int validateSubQueryColumnAndGetType(IntrinsicModel intrinsicModel, RecordMetadata metadata) throws SqlException {
        final int firstColumnType = metadata.getColumnType(0);
        if (firstColumnType != ColumnType.STRING && firstColumnType != ColumnType.SYMBOL) {
            assert intrinsicModel.keySubQuery.getColumns() != null;
            assert intrinsicModel.keySubQuery.getColumns().size() > 0;

            throw SqlException
                    .position(intrinsicModel.keySubQuery.getColumns().getQuick(0).getAst().position)
                    .put("unsupported column type: ")
                    .put(metadata.getColumnName(0))
                    .put(": ")
                    .put(ColumnType.nameOf(firstColumnType));
        }
        return firstColumnType;
    }
}

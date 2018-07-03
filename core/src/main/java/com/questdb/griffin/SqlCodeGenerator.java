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
import com.questdb.cairo.map.RecordSinkFactory;
import com.questdb.cairo.map.SingleColumnType;
import com.questdb.cairo.sql.*;
import com.questdb.common.ColumnType;
import com.questdb.common.SymbolTable;
import com.questdb.griffin.engine.functions.bind.BindVariableService;
import com.questdb.griffin.engine.table.*;
import com.questdb.griffin.model.IntrinsicModel;
import com.questdb.griffin.model.QueryColumn;
import com.questdb.griffin.model.QueryModel;
import com.questdb.std.BytecodeAssembler;
import com.questdb.std.Chars;
import com.questdb.std.IntList;
import com.questdb.std.ObjList;

public class SqlCodeGenerator {
    private final WhereClauseParser filterAnalyser = new WhereClauseParser();
    private final FunctionParser functionParser;
    private final CairoEngine engine;
    private final BytecodeAssembler asm = new BytecodeAssembler();
    // this list is used to generate record sinks
    private final ListColumnFilter listColumnFilter = new ListColumnFilter();
    private final SingleColumnType latestByColumnTypes = new SingleColumnType();
    private final CairoConfiguration configuration;

    public SqlCodeGenerator(CairoEngine engine, CairoConfiguration configuration, FunctionParser functionParser) {
        this.engine = engine;
        this.configuration = configuration;
        this.functionParser = functionParser;
    }

    private void clearState() {
        // todo: clear
    }

    private RecordMetadata copyMetadata(RecordMetadata that) {
        // todo: this metadata is ummutable. Ideally we shouldn't be creating metadata for the same table over and over
        return GenericRecordMetadata.copyOf(that);
    }

    RecordCursorFactory generate(QueryModel model, BindVariableService bindVariableService) throws SqlException {
        clearState();
        return generateQuery(model, bindVariableService);
    }

    private RecordCursorFactory generateFunctionQuery(QueryModel model) throws SqlException {
        final Function function = model.getTableNameFunction();
        assert function != null;
        if (function.getType() != TypeEx.CURSOR) {
            throw SqlException.position(model.getTableName().position).put("function must return CURSOR [actual=").put(ColumnType.nameOf(function.getType())).put(']');
        }

        return function.getRecordCursorFactory(null);
    }

    private RecordCursorFactory generateNoSelect(QueryModel model, BindVariableService bindVariableService) throws SqlException {
        SqlNode tableName = model.getTableName();
        if (tableName != null) {
            if (tableName.type == SqlNode.FUNCTION) {
                return generateFunctionQuery(model);
            } else {
                return generateTableQuery(model, bindVariableService);
            }

        }
        assert model.getNestedModel() != null;
        return generateQuery(model.getNestedModel(), bindVariableService);
    }

    private RecordCursorFactory generateQuery(QueryModel model, BindVariableService bindVariableService) throws SqlException {
        switch (model.getSelectModelType()) {
            case QueryModel.SELECT_MODEL_CHOOSE:
                return generateSelectChoose(model, bindVariableService);
            case QueryModel.SELECT_MODEL_GROUP_BY:
                return generateSelectGroupBy(model, bindVariableService);
            case QueryModel.SELECT_MODEL_VIRTUAL:
                return generateSelectVirtual(model, bindVariableService);
            case QueryModel.SELECT_MODEL_ANALYTIC:
                return generateSelectAnalytic(model, bindVariableService);
            default:
                return generateNoSelect(model, bindVariableService);
        }
    }

    private RecordCursorFactory generateSelectAnalytic(QueryModel model, BindVariableService bindVariableService) throws SqlException {
        assert model.getNestedModel() != null;
        return generateQuery(model.getNestedModel(), bindVariableService);
    }

    private RecordCursorFactory generateSelectChoose(QueryModel model, BindVariableService bindVariableService) throws SqlException {
        assert model.getNestedModel() != null;
        final RecordCursorFactory factory = generateQuery(model.getNestedModel(), bindVariableService);
        final RecordMetadata metadata = factory.getMetadata();
        final int selectColumnCount = model.getColumns().size();
        final SqlNode timestamp = model.getTimestamp();

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
                    model.getColumns().getQuick(i).getName().toString(),
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

    private RecordCursorFactory generateSelectGroupBy(QueryModel model, BindVariableService bindVariableService) throws SqlException {
        assert model.getNestedModel() != null;
        return generateQuery(model.getNestedModel(), bindVariableService);
    }

    private RecordCursorFactory generateSelectVirtual(QueryModel model, BindVariableService bindVariableService) throws SqlException {
        assert model.getNestedModel() != null;
        RecordCursorFactory factory = generateQuery(model.getNestedModel(), bindVariableService);

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

        for (int i = 0; i < columnCount; i++) {
            final QueryColumn column = model.getColumns().getQuick(i);
            SqlNode node = column.getAst();
            if (timestampColumn != null && node.type == SqlNode.LITERAL && Chars.equals(timestampColumn, node.token)) {
                virtualMetadata.setTimestampIndex(i);
            }

            Function function = functionParser.parseFunction(column.getAst(), metadata, bindVariableService);
            functions.add(function);

            virtualMetadata.add(new TableColumnMetadata(
                    column.getAlias().toString(),
                    function.getType()
            ));
        }

        return new VirtualRecordCursorFactory(virtualMetadata, functions, factory);
    }

    @SuppressWarnings("ConstantConditions")
    private RecordCursorFactory generateTableQuery(QueryModel model, BindVariableService bindVariableService) throws SqlException {

//        applyLimit(model);

        final SqlNode latestBy = model.getLatestBy();
        final SqlNode whereClause = model.getWhereClause();

        try (TableReader reader = engine.getReader(model.getTableName().token, model.getTableVersion())) {
            final RecordMetadata metadata = reader.getMetadata();

            final int latestByIndex;
            if (latestBy != null) {
                // validate latest by against current reader
                // first check if column is valid
                latestByIndex = metadata.getColumnIndexQuiet(latestBy.token);
            } else {
                latestByIndex = -1;
            }


            if (whereClause != null) {

                final int timestampIndex;

                SqlNode timestamp = model.getTimestamp();
                if (timestamp != null) {
                    timestampIndex = metadata.getColumnIndexQuiet(timestamp.token);
                } else {
                    timestampIndex = -1;
                }

                final IntrinsicModel intrinsicModel = filterAnalyser.extract(model, whereClause, metadata, latestBy != null ? latestBy.token : null, timestampIndex);

                if (intrinsicModel.intrinsicValue == IntrinsicModel.FALSE) {
                    return new EmptyTableRecordCursorFactory(copyMetadata(metadata), engine, model.getTableName().token.toString(), model.getTableVersion());
                }

                Function filter;

                if (intrinsicModel.filter != null) {
                    filter = functionParser.parseFunction(intrinsicModel.filter, metadata, bindVariableService);
                } else {
                    filter = null;
                }

                // validate filter
                if (filter != null) {
                    if (filter.getType() != ColumnType.BOOLEAN) {
                        throw SqlException.$(intrinsicModel.filter.position, "Boolean expression expected");
                    }

                    if (filter.isConstant()) {
                        // can pass null to constant function
                        if (filter.getBool(null)) {
                            // filter is constant "true", do not evaluate for every row
                            filter = null;
                        } else {
                            return new EmptyTableRecordCursorFactory(copyMetadata(metadata), engine, model.getTableName().token.toString(), model.getTableVersion());
                        }
                    }
                }

                DataFrameCursorFactory dfcFactory;

                if (latestByIndex > -1) {
                    // this is everything "latest by"
                    if (intrinsicModel.intervals != null) {
                        dfcFactory = new IntervalBwdDataFrameCursorFactory(engine, model.getTableName().token.toString(), model.getTableVersion(), intrinsicModel.intervals);
                    } else {
                        dfcFactory = new FullBwdDataFrameCursorFactory(engine, model.getTableName().token.toString(), model.getTableVersion());
                    }

                    if (intrinsicModel.keyColumn != null) {
                        // we also have key lookup, is the the same column as "latest by"
                        // note: key column is always indexed
                        int keyColumnIndex = metadata.getColumnIndexQuiet(intrinsicModel.keyColumn);

                        assert intrinsicModel.keyValues != null;
                        int nKeyValues = intrinsicModel.keyValues.size();

                        if (keyColumnIndex == latestByIndex) {
                            // we somewhat in luck
                            if (intrinsicModel.keySubQuery != null) {
                                // treat key values as lambda
                                // 1. get lambda cursor
                                // 2. for each value of first column of lambda: resolve to "int" of symbol, find first row in index
                                assert nKeyValues == 1;
                            } else {
                                assert nKeyValues > 0;
                                // deal with key values as a list
                                // 1. resolve each value of the list to "int"
                                // 2. get first row in index for each value (stream)

                                if (nKeyValues == 1) {
                                    final RowCursorFactory rcf;
                                    final int symbolKey = reader.getSymbolMapReader(keyColumnIndex).getQuick(intrinsicModel.keyValues.get(0));
                                    if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                                        rcf = EmptyRowCursorFactory.INSTANCE;
                                    } else {
                                        rcf = new SymbolIndexLatestValueRowCursorFactory(keyColumnIndex, symbolKey, false);
                                    }
                                    return new DataFrameRecordCursorFactory(copyMetadata(metadata), dfcFactory, rcf);
                                }
                            }
                        } else {
                            // this could only happen when "latest by" is not indexed
                            // this is because "latest by" is preferred key column for filter analyzer
                            // and filter analyzer always picks indexed column as key
                            if (intrinsicModel.keySubQuery != null) {
//                                assert intrinsicModel.keyValues.size() == 1;
                            } else {
                                assert intrinsicModel.keyValues.size() > 0;
                            }
                        }
                    } else {
                        assert intrinsicModel.keyValues.size() == 0;
                        // get latest rows for all values of "latest by" column
                    }
                } else {

                    if (intrinsicModel.intervals != null) {
                        dfcFactory = new IntervalFwdDataFrameCursorFactory(engine, model.getTableName().token.toString(), model.getTableVersion(), intrinsicModel.intervals);
                    } else {
                        dfcFactory = new FullFwdDataFrameCursorFactory(engine, model.getTableName().token.toString(), model.getTableVersion());
                    }

                    // no "latest by" clause
                    if (intrinsicModel.keyColumn != null) {

                        final int keyColumnIndex = reader.getMetadata().getColumnIndexQuiet(intrinsicModel.keyColumn);
                        if (keyColumnIndex == -1) {
                            // todo: obtain key column position
                            throw SqlException.invalidColumn(0, intrinsicModel.keyColumn);
                        }

                        final int nKeyValues = intrinsicModel.keyValues.size();
                        if (intrinsicModel.keySubQuery != null) {
                            // perform lambda based key lookup
                            assert nKeyValues == 1;
                        } else {
                            assert nKeyValues > 0;

                            if (nKeyValues == 1) {
                                final RowCursorFactory rcf;
                                final int symbolKey = reader.getSymbolMapReader(keyColumnIndex).getQuick(intrinsicModel.keyValues.get(0));
                                if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                                    rcf = EmptyRowCursorFactory.INSTANCE;
                                } else {
                                    if (filter == null) {
                                        rcf = new SymbolIndexRowCursorFactory(keyColumnIndex, symbolKey, true);
                                    } else {
                                        rcf = new SymbolIndexFilteredRowCursorFactory(keyColumnIndex, symbolKey, filter, true);
                                    }
                                }
                                return new DataFrameRecordCursorFactory(copyMetadata(metadata), dfcFactory, rcf);
                            }
                            // multiple key values
                            final ObjList<RowCursorFactory> cursorFactories = new ObjList<>(nKeyValues);
                            if (filter == null) {
                                // without filter
                                final SymbolMapReader symbolMapReader = reader.getSymbolMapReader(keyColumnIndex);
                                for (int i = 0; i < nKeyValues; i++) {
                                    final int symbolKey = symbolMapReader.getQuick(intrinsicModel.keyValues.get(i));
                                    if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
                                        cursorFactories.add(new SymbolIndexRowCursorFactory(keyColumnIndex, symbolKey, i == 0));
                                    }
                                }
                            } else {
                                // with filter
                                final SymbolMapReader symbolMapReader = reader.getSymbolMapReader(keyColumnIndex);
                                for (int i = 0; i < nKeyValues; i++) {
                                    final int symbolKey = symbolMapReader.getQuick(intrinsicModel.keyValues.get(i));
                                    if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
                                        cursorFactories.add(new SymbolIndexFilteredRowCursorFactory(keyColumnIndex, symbolKey, filter, i == 0));
                                    }
                                }
                            }
                            return new DataFrameRecordCursorFactory(copyMetadata(metadata), dfcFactory, new HeapRowCursorFactory(cursorFactories));
                        }
                    } else {
                        RecordCursorFactory factory = new DataFrameRecordCursorFactory(copyMetadata(metadata), dfcFactory, new DataFrameRowCursorFactory());
                        if (filter != null) {
                            return new FilteredRecordCursorFactory(factory, filter);
                        }
                        return factory;
                    }
                }

                // after we dealt with "latest by" clause and key lookups we must apply filter if we have one
                // NOTE! when "latest by" is present filter must be applied *before* latest by is evaluated
                if (filter != null) {
                    // apply filter
                }

                return null;

            } else {
                if (latestByIndex == -1) {
                    return new TableReaderRecordCursorFactory(copyMetadata(metadata), engine, Chars.toString(model.getTableName().token), model.getTableVersion());
                } else {
                    if (metadata.isColumnIndexed(latestByIndex)) {
                        return new LatestByRecordCursorFactory(
                                copyMetadata(metadata),
                                new FullBwdDataFrameCursorFactory(engine, model.getTableName().token.toString(), model.getTableVersion()),
                                latestByIndex);
                    }

                    listColumnFilter.clear();
                    listColumnFilter.add(latestByIndex);
                    return new LatestBySansIndexRecordCursorFactory(
                            copyMetadata(metadata),
                            configuration,
                            new FullBwdDataFrameCursorFactory(engine, model.getTableName().token.toString(), model.getTableVersion()),
                            RecordSinkFactory.getInstance(asm, metadata, listColumnFilter, false),
                            latestByColumnTypes.of(metadata.getColumnType(latestByIndex))
                    );
                }
            }
        }
    }
}

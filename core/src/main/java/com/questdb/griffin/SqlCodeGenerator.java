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
import com.questdb.std.*;

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
                    return new EmptyTableRecordCursorFactory(copyMetadata(metadata));
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
                        // key column must always be the same as latest by column
                        assert latestByIndex == metadata.getColumnIndexQuiet(intrinsicModel.keyColumn);
                        assert intrinsicModel.keyValues != null;
                        int nKeyValues = intrinsicModel.keyValues.size();

                        if (metadata.isColumnIndexed(latestByIndex)) {
                            // we somewhat in luck
                            if (intrinsicModel.keySubQuery != null) {
                                // treat key values as lambda
                                // 1. get lambda cursor
                                // 2. for each value of first column of lambda: resolve to "int" of symbol, find first row in index
                                assert nKeyValues == 1;
                                assert false : "not implemented";
                            } else {
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
                                        return new DataFrameRecordCursorFactory(copyMetadata(metadata), dfcFactory, rcf);
                                    }

                                    if (symbol == SymbolTable.VALUE_NOT_FOUND) {
                                        return new LatestByValueDeferredIndexedFilteredRecordCursorFactory(
                                                copyMetadata(metadata),
                                                dfcFactory,
                                                latestByIndex,
                                                Chars.toString(symbolValue),
                                                filter);
                                    }
                                    return new LatestByValueIndexedFilteredRecordCursorFactory(
                                            copyMetadata(metadata),
                                            dfcFactory,
                                            latestByIndex,
                                            symbol,
                                            filter);
                                } else {
                                    // we need two data structures, int hash set for symbol keys we can resolve here
                                    // and CharSequence hash set for symbols we cannot resolve
                                    // we could pass all symbols to factory to resolve, but this would lead to
                                    // creating Strings for symbols that we may be able to avoid doing so

                                    final IntHashSet symbolKeys = new IntHashSet();
                                    CharSequenceHashSet deferredSymbols = null;

                                    for (int i = 0; i < nKeyValues; i++) {
                                        CharSequence symbol = intrinsicModel.keyValues.get(i);
                                        int symbolKey = symbolMapReader.getQuick(symbol);
                                        if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                                            if (deferredSymbols == null) {
                                                deferredSymbols = new CharSequenceHashSet();
                                            }
                                            deferredSymbols.add(Chars.toString(symbol));
                                        } else {
                                            symbolKeys.add(symbolKey + 1);
                                        }
                                    }

                                    if (filter == null) {
                                        return new LatestByValuesIndexedRecordCursorFactory(
                                                copyMetadata(metadata),
                                                dfcFactory,
                                                latestByIndex,
                                                symbolKeys,
                                                deferredSymbols);
                                    }

                                    return new LatestByValuesIndexedFilteredRecordCursorFactory(
                                            copyMetadata(metadata),
                                            dfcFactory,
                                            latestByIndex,
                                            symbolKeys,
                                            deferredSymbols,
                                            filter
                                    );
                                }
                            }
                        } else {
                            // we have "latest by" column values, but no index
                            final SymbolMapReader symbolMapReader = reader.getSymbolMapReader(latestByIndex);

                            assert intrinsicModel.keyValues.size() > 0;

                            if (intrinsicModel.keyValues.size() > 1) {

                                // we need two data structures, int hash set for symbol keys we can resolve here
                                // and CharSequence hash set for symbols we cannot resolve
                                // we could pass all symbols to factory to resolve, but this would lead to
                                // creating Strings for symbols that we may be able to avoid doing so

                                final IntHashSet symbolKeys = new IntHashSet();
                                CharSequenceHashSet deferredSymbols = null;

                                for (int i = 0; i < nKeyValues; i++) {
                                    CharSequence symbol = intrinsicModel.keyValues.get(i);
                                    int symbolKey = symbolMapReader.getQuick(symbol);
                                    if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                                        if (deferredSymbols == null) {
                                            deferredSymbols = new CharSequenceHashSet();
                                        }
                                        deferredSymbols.add(Chars.toString(symbol));
                                    } else {
                                        symbolKeys.add(symbolKey);
                                    }
                                }

                                if (filter != null) {
                                    return new LatestByValuesFilteredRecordCursorFactory(
                                            copyMetadata(metadata),
                                            dfcFactory,
                                            latestByIndex,
                                            symbolKeys,
                                            filter,
                                            deferredSymbols
                                    );
                                }
                                return new LatestByValuesRecordCursorFactory(
                                        copyMetadata(metadata),
                                        dfcFactory,
                                        latestByIndex,
                                        symbolKeys,
                                        deferredSymbols
                                );
                            }

                            // we have a single symbol key
                            int symbolKey = symbolMapReader.getQuick(intrinsicModel.keyValues.get(0));

                            if (filter == null) {
                                if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                                    return new LatestByValueDeferredRecordCursorFactory(copyMetadata(metadata), dfcFactory, latestByIndex, Chars.toString(intrinsicModel.keyValues.get(0)));
                                }

                                return new LatestByValueRecordCursorFactory(copyMetadata(metadata), dfcFactory, latestByIndex, symbolKey);
                            }

                            if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                                return new LatestByValueDeferredFilteredRecordCursorFactory(
                                        copyMetadata(metadata),
                                        dfcFactory,
                                        latestByIndex,
                                        Chars.toString(intrinsicModel.keyValues.get(0)),
                                        filter
                                );
                            }

                            return new LatestByValueFilteredRecordCursorFactory(copyMetadata(metadata), dfcFactory, latestByIndex, symbolKey, filter);
                        }
                    } else {
                        // we select all values of "latest by" column

                        assert intrinsicModel.keyValues.size() == 0;
                        // get latest rows for all values of "latest by" column

                        if (filter == null) {
                            // this is almost the same code when we don't have where clause at all
                            if (metadata.isColumnIndexed(latestByIndex)) {
                                return new LatestByAllIndexedRecordCursorFactory(
                                        copyMetadata(metadata),
                                        dfcFactory,
                                        latestByIndex);
                            }

                            listColumnFilter.clear();
                            listColumnFilter.add(latestByIndex);
                            return new LatestByAllRecordCursorFactory(
                                    copyMetadata(metadata),
                                    configuration,
                                    dfcFactory,
                                    RecordSinkFactory.getInstance(asm, metadata, listColumnFilter, false),
                                    latestByColumnTypes.of(metadata.getColumnType(latestByIndex))
                            );
                        } else {
                            if (metadata.isColumnIndexed(latestByIndex)) {
                                return new LatestByAllIndexedFilteredRecordCursorFactory(
                                        copyMetadata(metadata),
                                        dfcFactory,
                                        latestByIndex,
                                        filter);
                            }
                            listColumnFilter.clear();
                            listColumnFilter.add(latestByIndex);
                            return new LatestByAllFilteredRecordCursorFactory(
                                    copyMetadata(metadata),
                                    configuration,
                                    dfcFactory,
                                    RecordSinkFactory.getInstance(asm, metadata, listColumnFilter, false),
                                    latestByColumnTypes.of(metadata.getColumnType(latestByIndex)),
                                    filter
                            );
                        }
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
                            assert false : "not implemented";
                        } else {
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

                assert false : "not implemented";

                return null;

            } else {
                if (latestByIndex == -1) {
                    return new TableReaderRecordCursorFactory(copyMetadata(metadata), engine, Chars.toString(model.getTableName().token), model.getTableVersion());
                } else {
                    if (metadata.isColumnIndexed(latestByIndex)) {
                        return new LatestByAllIndexedRecordCursorFactory(
                                copyMetadata(metadata),
                                new FullBwdDataFrameCursorFactory(engine, model.getTableName().token.toString(), model.getTableVersion()),
                                latestByIndex);
                    }

                    listColumnFilter.clear();
                    listColumnFilter.add(latestByIndex);
                    return new LatestByAllRecordCursorFactory(
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

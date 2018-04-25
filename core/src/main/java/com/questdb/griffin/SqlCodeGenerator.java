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

import com.questdb.cairo.FullTableFrameCursorFactory;
import com.questdb.cairo.IntervalFrameCursorFactory;
import com.questdb.cairo.TableReader;
import com.questdb.cairo.sql.CairoEngine;
import com.questdb.cairo.sql.DataFrameCursorFactory;
import com.questdb.cairo.sql.RecordCursorFactory;
import com.questdb.cairo.sql.RowCursorFactory;
import com.questdb.common.ColumnType;
import com.questdb.common.RecordColumnMetadata;
import com.questdb.common.RecordMetadata;
import com.questdb.griffin.engine.functions.bind.BindVariableService;
import com.questdb.griffin.engine.table.FilteredTableRecordCursorFactory;
import com.questdb.griffin.engine.table.SymbolIndexFilteredRowCursorFactory;
import com.questdb.griffin.engine.table.SymbolIndexRowCursorFactory;
import com.questdb.griffin.model.IntrinsicModel;
import com.questdb.griffin.model.QueryModel;

public class SqlCodeGenerator {
    private final WhereClauseParser filterAnalyser = new WhereClauseParser();
    private final FunctionParser functionParser;
    private final CairoEngine engine;

    public SqlCodeGenerator(CairoEngine engine, FunctionParser functionParser) {
        this.engine = engine;
        this.functionParser = functionParser;
    }

    private void clearState() {
        // todo: clear
    }

    RecordCursorFactory generate(QueryModel model, BindVariableService bindVariableService) throws SqlException {
        clearState();
        return generateQuery(model, bindVariableService);
    }

    private RecordCursorFactory generateNoSelect(QueryModel model, BindVariableService bindVariableService) throws SqlException {
        if (model.getTableName() != null) {
            return generateTableQuery(model, bindVariableService);

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
        return generateQuery(model.getNestedModel(), bindVariableService);
    }

    private RecordCursorFactory generateSelectGroupBy(QueryModel model, BindVariableService bindVariableService) throws SqlException {
        assert model.getNestedModel() != null;
        return generateQuery(model.getNestedModel(), bindVariableService);
    }

    private RecordCursorFactory generateSelectVirtual(QueryModel model, BindVariableService bindVariableService) throws SqlException {
        assert model.getNestedModel() != null;
        return generateQuery(model.getNestedModel(), bindVariableService);
    }

    @SuppressWarnings("ConstantConditions")
    private RecordCursorFactory generateTableQuery(QueryModel model, BindVariableService bindVariableService) throws SqlException {

//        applyLimit(model);

        final SqlNode latestBy = model.getLatestBy();
        final SqlNode whereClause = model.getWhereClause();

        try (TableReader reader = engine.getReader(model.getTableName().token)) {
            if (whereClause != null) {

                final RecordMetadata metadata = reader.getMetadata();
                final int timestampIndex;

                SqlNode timestamp = model.getTimestamp();
                if (timestamp != null) {
                    timestampIndex = metadata.getColumnIndex(timestamp.token);
                } else {
                    timestampIndex = -1;
                }

                final IntrinsicModel intrinsicModel = filterAnalyser.extract(model, whereClause, reader.getMetadata(), latestBy != null ? latestBy.token : null, timestampIndex);

                if (intrinsicModel.intrinsicValue == IntrinsicModel.FALSE) {
                    // todo: return empty factory
                    return null;
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
                            //todo: return factory, which would create empty record cursor for the given table
                            return null;
                        }
                    }
                }

                DataFrameCursorFactory dfcFactory;
                if (intrinsicModel.intervals != null) {
                    dfcFactory = new IntervalFrameCursorFactory(engine, model.getTableName().token.toString(), intrinsicModel.intervals);
                } else {
                    dfcFactory = new FullTableFrameCursorFactory(engine, model.getTableName().token.toString());
                }

                if (latestBy != null) {
                    // this is everything "latest by"

                    // first check if column is valid
                    int latestByIndex = metadata.getColumnIndex(latestBy.token);
                    RecordColumnMetadata latestByMeta = metadata.getColumnQuick(latestByIndex);
                    if (latestByMeta.getType() != ColumnType.SYMBOL) {
                        throw SqlException.$(latestBy.position, "has to be SYMBOL");
                    }

                    if (intrinsicModel.keyColumn != null) {
                        // we also have key lookup, is the the same column as "latest by"
                        // note: key column is always indexed
                        int keyColumnIndex = metadata.getColumnIndex(intrinsicModel.keyColumn);

                        if (keyColumnIndex == latestByIndex) {
                            // we somewhat in luck
                            if (intrinsicModel.keySubQuery != null) {
                                // treat key values as lambda
                                // 1. get lambda cursor
                                // 2. for each value of first column of lambda: resolve to "int" of symbol, find first row in index
                                assert intrinsicModel.keyValues.size() == 1;
                            } else {
                                assert intrinsicModel.keyValues != null && intrinsicModel.keyValues.size() > 0;
                                // deal with key values as a list
                                // 1. resolve each value of the list to "int"
                                // 2. get first row in index for each value (stream)
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
                    // no "latest by" clause
                    if (intrinsicModel.keyColumn != null) {
                        if (intrinsicModel.keySubQuery != null) {
                            // perform lambda based key lookup
                            assert intrinsicModel.keyValues.size() == 1;
                        } else {
                            assert intrinsicModel.keyValues.size() > 0;
                            if (intrinsicModel.keyValues.size() == 1) {
                                if (filter == null) {
                                    RowCursorFactory rcf = new SymbolIndexRowCursorFactory(
                                            engine,
                                            model.getTableName().token,
                                            intrinsicModel.keyColumn,
                                            intrinsicModel.keyValues.get(0));
                                    return new FilteredTableRecordCursorFactory(dfcFactory, rcf);
                                } else {
                                    RowCursorFactory rcf = new SymbolIndexFilteredRowCursorFactory(
                                            engine,
                                            model.getTableName().token,
                                            intrinsicModel.keyColumn,
                                            intrinsicModel.keyValues.get(0),
                                            filter
                                    );
                                    return new FilteredTableRecordCursorFactory(dfcFactory, rcf);
                                }
                            } else {
                                // multiple key values
                                if (filter == null) {
                                    // without filter
                                } else {
                                    // with filter
                                }
                            }
                        }
                    }
                }

                // after we dealt with "latest by" clause and key lookups we must apply filter if we have one
                // NOTE! when "latest by" is present filter must be applied *before* latest by is evaluated
                if (filter != null) {
                    // apply filter
                }

                return null;

            }
        }
        return null;
    }
}

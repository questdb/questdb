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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.*;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.columns.*;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public class GroupByUtils {

    public static void prepareGroupByFunctions(
            QueryModel model,
            RecordMetadata metadata,
            FunctionParser functionParser,
            SqlExecutionContext executionContext,
            ObjList<GroupByFunction> groupByFunctions,
            @Transient IntList groupByFunctionPositions,
            ArrayColumnTypes valueTypes
    ) throws SqlException {

        groupByFunctionPositions.clear();

        final ObjList<QueryColumn> columns = model.getColumns();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final QueryColumn column = columns.getQuick(i);
            final ExpressionNode node = column.getAst();

            if (node.type != ExpressionNode.LITERAL) {
                // this can fail
                ExpressionNode columnAst = column.getAst();

                final Function function = functionParser.parseFunction(
                        columnAst,
                        metadata,
                        executionContext
                );

                // configure map value columns for group-by functions
                // some functions may need more than one column in values
                // so we have them do all the work
                assert function instanceof GroupByFunction;
                GroupByFunction func = (GroupByFunction) function;
                func.pushValueTypes(valueTypes);
                groupByFunctions.add(func);
                groupByFunctionPositions.add(columnAst.position);
            }
        }
    }

    public static void prepareGroupByRecordFunctions(
            @NotNull QueryModel model,
            RecordMetadata metadata,
            @NotNull ListColumnFilter listColumnFilter,
            ObjList<GroupByFunction> groupByFunctions,
            @Transient IntList groupByFunctionPositions,
            ObjList<Function> recordFunctions,
            @Transient IntList recordFunctionPositions,
            GenericRecordMetadata groupByMetadata,
            ArrayColumnTypes keyTypes,
            int keyColumnIndex,
            boolean timestampUnimportant,
            int timestampIndex
    ) throws SqlException {

        recordFunctionPositions.clear();

        // Process group-by functions first to get the idea of
        // how many map values we will have.
        // Map value count is needed to calculate offsets for
        // map key columns.

        ObjList<QueryColumn> columns = model.getColumns();
        int valueColumnIndex = 0;
        int inferredKeyColumnCount = 0;

        // when we have same column several times in a row
        // we only add it once to map keys
        int lastIndex = -1;
        for (int i = 0, n = columns.size(); i < n; i++) {
            final QueryColumn column = columns.getQuick(i);
            final ExpressionNode node = column.getAst();
            final int type;

            if (node.type == ExpressionNode.LITERAL) {
                // this is key
                int index = metadata.getColumnIndexQuiet(node.token);
                if (index == -1) {
                    throw SqlException.invalidColumn(node.position, node.token);
                }

                type = metadata.getColumnType(index);
                if (index != timestampIndex || timestampUnimportant) {
                    if (lastIndex != index) {
                        listColumnFilter.add(index + 1);
                        keyTypes.add(type);
                        keyColumnIndex++;
                        lastIndex = index;
                    }

                    final Function fun;
                    switch (type) {
                        case ColumnType.BOOLEAN:
                            fun = BooleanColumn.newInstance(keyColumnIndex - 1);
                            break;
                        case ColumnType.BYTE:
                            fun = ByteColumn.newInstance(keyColumnIndex - 1);
                            break;
                        case ColumnType.SHORT:
                            fun = ShortColumn.newInstance(keyColumnIndex - 1);
                            break;
                        case ColumnType.CHAR:
                            fun = CharColumn.newInstance(keyColumnIndex - 1);
                            break;
                        case ColumnType.INT:
                            fun = IntColumn.newInstance(keyColumnIndex - 1);
                            break;
                        case ColumnType.LONG:
                            fun = LongColumn.newInstance(keyColumnIndex - 1);
                            break;
                        case ColumnType.FLOAT:
                            fun = FloatColumn.newInstance(keyColumnIndex - 1);
                            break;
                        case ColumnType.DOUBLE:
                            fun = DoubleColumn.newInstance(keyColumnIndex - 1);
                            break;
                        case ColumnType.STRING:
                            fun = StrColumn.newInstance(keyColumnIndex - 1);
                            break;
                        case ColumnType.SYMBOL:
                            fun = new MapSymbolColumn(keyColumnIndex - 1, index, metadata.isSymbolTableStatic(index));
                            break;
                        case ColumnType.DATE:
                            fun = DateColumn.newInstance(keyColumnIndex - 1);
                            break;
                        case ColumnType.TIMESTAMP:
                            fun = TimestampColumn.newInstance(keyColumnIndex - 1);
                            break;
                        case ColumnType.LONG256:
                            fun = Long256Column.newInstance(keyColumnIndex - 1);
                            break;
                        default:
                            fun = BinColumn.newInstance(keyColumnIndex - 1);
                            break;
                    }

                    recordFunctions.add(fun);
                    recordFunctionPositions.add(node.position);

                } else {
                    // set this function to null, cursor will replace it with an instance class
                    // timestamp function returns value of class member which makes it impossible
                    // to create these columns in advance of cursor instantiation
                    recordFunctions.add(null);
                    groupByFunctionPositions.add(0);
                    if (groupByMetadata.getTimestampIndex() == -1) {
                        groupByMetadata.setTimestampIndex(i);
                    }
                    assert type == ColumnType.TIMESTAMP;
                }

                // and finish with populating metadata for this factory
                if (column.getAlias() == null) {
                    groupByMetadata.add(BaseRecordMetadata.copyOf(metadata, index));
                } else {
                    groupByMetadata.add(
                            new TableColumnMetadata(
                                    Chars.toString(column.getAlias()),
                                    type,
                                    metadata.isColumnIndexed(index),
                                    metadata.getIndexValueBlockCapacity(index),
                                    metadata.isSymbolTableStatic(index),
                                    metadata.getMetadata(index)
                            )
                    );
                }
                inferredKeyColumnCount++;

            } else {
                // add group-by function as a record function as well
                // so it can produce column values
                final GroupByFunction groupByFunction = groupByFunctions.getQuick(valueColumnIndex);
                recordFunctions.add(groupByFunction);
                recordFunctionPositions.add(groupByFunctionPositions.getQuick(valueColumnIndex++));
                type = groupByFunction.getType();

                // and finish with populating metadata for this factory
                groupByMetadata.add(
                        new TableColumnMetadata(
                                Chars.toString(column.getName()),
                                type,
                                false,
                                0,
                                groupByFunction instanceof SymbolFunction && (((SymbolFunction) groupByFunction).isSymbolTableStatic()),
                                groupByFunction.getMetadata()
                        )
                );
            }
        }
        validateGroupByColumns(model, inferredKeyColumnCount);
    }

    public static void toTop(ObjList<? extends Function> args) {
        for (int i = 0, n = args.size(); i < n; i++) {
            args.getQuick(i).toTop();
        }
    }

    public static void updateExisting(ObjList<GroupByFunction> groupByFunctions, int n, MapValue value, Record record) {
        for (int i = 0; i < n; i++) {
            groupByFunctions.getQuick(i).computeNext(value, record);
        }
    }

    public static void updateNew(ObjList<GroupByFunction> groupByFunctions, int n, MapValue value, Record record) {
        for (int i = 0; i < n; i++) {
            groupByFunctions.getQuick(i).computeFirst(value, record);
        }
    }

    public static void updateEmpty(ObjList<GroupByFunction> groupByFunctions, int n, MapValue value) {
        for (int i = 0; i < n; i++) {
            groupByFunctions.getQuick(i).setEmpty(value);
        }
    }

    public static void validateGroupByColumns(@NotNull QueryModel model, int inferredKeyColumnCount) throws SqlException {
        final ObjList<ExpressionNode> groupByColumns = model.getGroupBy();
        int explicitKeyColumnCount = groupByColumns.size();
        if (explicitKeyColumnCount == 0) {
            return;
        }

        final QueryModel nested = model.getNestedModel();
        QueryModel chooseModel = model;
        while (chooseModel != null && chooseModel.getSelectModelType() != QueryModel.SELECT_MODEL_CHOOSE && chooseModel.getSelectModelType() != QueryModel.SELECT_MODEL_NONE) {
            chooseModel = chooseModel.getNestedModel();
        }

        for (int i = 0; i < explicitKeyColumnCount; i++) {
            final ExpressionNode key = groupByColumns.getQuick(i);
            switch (key.type) {
                case ExpressionNode.LITERAL:
                    final int dotIndex = Chars.indexOf(key.token, '.');

                    if (dotIndex > -1) {
                        int aliasIndex = model.getAliasIndex(key.token, 0, dotIndex);
                        if (aliasIndex > -1) {
                            // we should now check against main model
                            int refColumn = model.getAliasToColumnMap().keyIndex(key.token);
                            if (refColumn > -1) {
                                // a.x not found, look for "x"
                                refColumn = model.getAliasToColumnMap().keyIndex(key.token, dotIndex + 1, key.token.length());
                            }

                            if (refColumn > -1) {
                                throw SqlException.$(key.position, "group by column does not match any key column is select statement");
                            }

                        } else {

                            // the table alias could be referencing join model
                            // we need to descend down to first NONE model and see if that can resolve columns we are
                            // looking for

                            if (chooseModel != null && chooseModel.getColumnNameToAliasMap().keyIndex(key.token) < 0) {
                                continue;
                            }
                            throw SqlException.$(key.position, "invalid column reference");
                        }
                    } else {
                        int refColumn = model.getAliasToColumnMap().keyIndex(key.token);
                        if (refColumn > -1) {
                            throw SqlException.$(key.position, "group by column does not match any key column is select statement");
                        }
                        QueryColumn qc = model.getAliasToColumnMap().valueAt(refColumn);
                        if (qc.getAst().type != ExpressionNode.LITERAL && qc.getAst().type != ExpressionNode.CONSTANT) {
                            throw SqlException.$(key.position, "group by column references aggregate expression");
                        }
                    }
                    break;
                case ExpressionNode.BIND_VARIABLE:
                    throw SqlException.$(key.position, "bind variable is not allowed here");
                case ExpressionNode.FUNCTION:
                case ExpressionNode.OPERATION:
                case ExpressionNode.CONSTANT:
                    final ObjList<QueryColumn> availableColumns = nested.getTopDownColumns();
                    boolean invalid = true;
                    for (int j = 0, n = availableColumns.size(); j < n; j++) {
                        final QueryColumn qc = availableColumns.getQuick(j);
                        if (qc.getAst().type == key.type) {
                            if (ExpressionNode.compareNodesGroupBy(key, qc.getAst(), chooseModel)) {
                                invalid = false;
                                break;
                            }
                        }
                    }
                    if (invalid) {
                        throw SqlException.$(key.position, "group by expression does not match anything select in statement");
                    }
                    break;
                default:
                    throw SqlException.$(key.position, "unsupported type of expression");

            }
        }


        if (explicitKeyColumnCount < inferredKeyColumnCount) {
            throw SqlException.$(model.getModelPosition(), "not enough columns in group by");
        }
    }

    static void updateFunctions(ObjList<GroupByFunction> groupByFunctions, int n, MapValue value, Record record) {
        if (value.isNew()) {
            updateNew(groupByFunctions, n, value, record);
        } else {
            updateExisting(groupByFunctions, n, value, record);
        }
    }
}

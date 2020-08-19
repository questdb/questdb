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
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.columns.*;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

public class GroupByUtils {
    public static void checkGroupBy(ObjList<ExpressionNode> groupBys, ObjList<QueryColumn> selectColumns, int groupByColumnCount, ExpressionNode alias) throws SqlException {
        int matchingColumnCount = 0;
        int groupBySize = groupBys.size();
        if (groupBySize == 0) {
            return;
        }
        int selectColumnCount = selectColumns.size();
        for (int i = 0; i < selectColumnCount; i++) {
            for (int j = 0; j < groupBySize; j++) {
                ExpressionNode groupByNode = groupBys.getQuick(j);
                QueryColumn selectColumn = selectColumns.get(i);
                ExpressionNode selectNode = selectColumn.getAst();
                if (groupByNode.type == ExpressionNode.LITERAL && selectNode.type == ExpressionNode.LITERAL) {
                    if (Chars.equals(groupByNode.token, selectNode.token) || Chars.equals(groupByNode.token, selectColumn.getAlias())) {
                        matchingColumnCount++;
                        break;
                    }

                    int dotIndex = Chars.indexOf(groupByNode.token, '.');
                    if (dotIndex > -1) {
                        if (alias != null
                                && Chars.equals(alias.token, groupByNode.token, 0, dotIndex)
                                && (
                                Chars.equals(selectNode.token, groupByNode.token, dotIndex + 1, groupByNode.token.length())
                                        ||
                                        Chars.equals(selectColumn.getAlias(), groupByNode.token, dotIndex + 1, groupByNode.token.length())
                        )
                        ) {
                            matchingColumnCount++;
                            break;
                        }
                    }
                } else {
                    if (compareNodes(groupByNode, selectNode)) {
                        matchingColumnCount++;
                        break;
                    }
                }
            }
        }

        if (matchingColumnCount != groupBySize || matchingColumnCount != groupByColumnCount) {
            throw SqlException.$(0, "group by column does not match key column is select statement ");
        }
    }

    public static boolean compareNodes(ExpressionNode groupBy, ExpressionNode selectNode) {
        if (groupBy == null && selectNode == null) {
            return true;
        }

        if (groupBy == null || selectNode == null || groupBy.type != selectNode.type) {
            return false;
        }

        int dotIndex = groupBy.token != null ? Chars.indexOf(groupBy.token, '.') : -1;
        if ((dotIndex < 0 && !Chars.equals(groupBy.token, selectNode.token))
                || (dotIndex > -1 && !Chars.equals(selectNode.token, groupBy.token, dotIndex + 1, groupBy.token.length()))) {
            return false;
        }

        int groupByArgsSize = groupBy.args.size();
        int selectNodeArgsSize = selectNode.args.size();

        if (groupByArgsSize != selectNodeArgsSize) {
            return false;
        }

        if (groupByArgsSize < 3) {
            return compareNodes(groupBy.lhs, selectNode.lhs) && compareNodes(groupBy.rhs, selectNode.rhs);
        }

        for (int i = 0; i < groupByArgsSize; i++) {
            if (!compareNodes(groupBy.args.get(i), selectNode.args.get(i))) {
                return false;
            }
        }
        return true;
    }

    public static void prepareGroupByFunctions(
            QueryModel model,
            RecordMetadata metadata,
            FunctionParser functionParser,
            SqlExecutionContext executionContext,
            ObjList<GroupByFunction> groupByFunctions,
            ArrayColumnTypes valueTypes
    ) throws SqlException {

        final ObjList<QueryColumn> columns = model.getColumns();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final QueryColumn column = columns.getQuick(i);
            final ExpressionNode node = column.getAst();

            if (node.type != ExpressionNode.LITERAL) {
                // this can fail
                final Function function = functionParser.parseFunction(
                        column.getAst(),
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
            }
        }
    }

    public static void prepareGroupByRecordFunctions(
            @NotNull QueryModel model,
            RecordMetadata metadata,
            @NotNull ListColumnFilter listColumnFilter,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            GenericRecordMetadata groupByMetadata,
            ArrayColumnTypes keyTypes,
            int keyColumnIndex,
            boolean timestampUnimportant,
            int timestampIndex
    ) throws SqlException {

        // Process group-by functions first to get the idea of
        // how many map values we will have.
        // Map value count is needed to calculate offsets for
        // map key columns.

        ObjList<QueryColumn> columns = model.getColumns();
        int valueColumnIndex = 0;
        int groupByColumnCount = 0;

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
                        listColumnFilter.add(index);
                        keyTypes.add(type);
                        keyColumnIndex++;
                        lastIndex = index;
                    }

                    final Function fun;
                    switch (type) {
                        case ColumnType.BOOLEAN:
                            fun = new BooleanColumn(node.position, keyColumnIndex - 1);
                            break;
                        case ColumnType.BYTE:
                            fun = new ByteColumn(node.position, keyColumnIndex - 1);
                            break;
                        case ColumnType.SHORT:
                            fun = new ShortColumn(node.position, keyColumnIndex - 1);
                            break;
                        case ColumnType.CHAR:
                            fun = new CharColumn(node.position, keyColumnIndex - 1);
                            break;
                        case ColumnType.INT:
                            fun = new IntColumn(node.position, keyColumnIndex - 1);
                            break;
                        case ColumnType.LONG:
                            fun = new LongColumn(node.position, keyColumnIndex - 1);
                            break;
                        case ColumnType.FLOAT:
                            fun = new FloatColumn(node.position, keyColumnIndex - 1);
                            break;
                        case ColumnType.DOUBLE:
                            fun = new DoubleColumn(node.position, keyColumnIndex - 1);
                            break;
                        case ColumnType.STRING:
                            fun = new StrColumn(node.position, keyColumnIndex - 1);
                            break;
                        case ColumnType.SYMBOL:
                            fun = new MapSymbolColumn(node.position, keyColumnIndex - 1, index, metadata.isSymbolTableStatic(index));
                            break;
                        case ColumnType.DATE:
                            fun = new DateColumn(node.position, keyColumnIndex - 1);
                            break;
                        case ColumnType.TIMESTAMP:
                            fun = new TimestampColumn(node.position, keyColumnIndex - 1);
                            break;
                        case ColumnType.LONG256:
                            fun = new Long256Column(node.position, keyColumnIndex - 1);
                            break;
                        default:
                            fun = new BinColumn(node.position, keyColumnIndex - 1);
                            break;
                    }

                    recordFunctions.add(fun);

                } else {
                    // set this function to null, cursor will replace it with an instance class
                    // timestamp function returns value of class member which makes it impossible
                    // to create these columns in advance of cursor instantiation
                    recordFunctions.add(null);
                    if (groupByMetadata.getTimestampIndex() == -1) {
                        groupByMetadata.setTimestampIndex(i);
                    }
                    assert type == ColumnType.TIMESTAMP;
                }

                // and finish with populating metadata for this factory
                groupByMetadata.add(
                        new TableColumnMetadata(
                                Chars.toString(column.getName()),
                                type,
                                metadata.isColumnIndexed(index),
                                metadata.getIndexValueBlockCapacity(index),
                                metadata.isSymbolTableStatic(index)
                        )
                );
                groupByColumnCount++;

            } else {
                // add group-by function as a record function as well
                // so it can produce column values
                final GroupByFunction groupByFunction = groupByFunctions.getQuick(valueColumnIndex++);
                recordFunctions.add(groupByFunction);
                type = groupByFunction.getType();

                // and finish with populating metadata for this factory
                groupByMetadata.add(
                        new TableColumnMetadata(
                                Chars.toString(column.getName()),
                                type,
                                false,
                                0,
                                groupByFunction instanceof SymbolFunction && (((SymbolFunction) groupByFunction).isSymbolTableStatic())
                        )
                );
            }
        }
        validateGroupByColumns(model, columns, groupByColumnCount);
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

    public static void validateGroupByColumns(@NotNull QueryModel model, ObjList<QueryColumn> columns, int groupByColumnCount) throws SqlException {
        final ObjList<ExpressionNode> groupBys = model.getGroupBy();
        QueryModel next = model.getNestedModel();
        while (next.getSelectModelType() != QueryModel.SELECT_MODEL_NONE) {
            if (next.getSelectModelType() == QueryModel.SELECT_MODEL_VIRTUAL) {
                columns = next.getColumns();
                break;
            }
            next = next.getNestedModel();
        }
        //
        ExpressionNode alias = next.getAlias();
        GroupByUtils.checkGroupBy(groupBys, columns, groupByColumnCount, alias);
    }

    static void updateFunctions(ObjList<GroupByFunction> groupByFunctions, int n, MapValue value, Record record) {
        if (value.isNew()) {
            updateNew(groupByFunctions, n, value, record);
        } else {
            updateExisting(groupByFunctions, n, value, record);
        }
    }
}

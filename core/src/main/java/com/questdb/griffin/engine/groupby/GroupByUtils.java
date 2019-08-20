/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.griffin.engine.groupby;

import com.questdb.cairo.*;
import com.questdb.cairo.map.MapValue;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.griffin.FunctionParser;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.SqlExecutionContext;
import com.questdb.griffin.engine.functions.GroupByFunction;
import com.questdb.griffin.engine.functions.columns.*;
import com.questdb.griffin.model.ExpressionNode;
import com.questdb.griffin.model.QueryColumn;
import com.questdb.griffin.model.QueryModel;
import com.questdb.std.Chars;
import com.questdb.std.IntIntHashMap;
import com.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

class GroupByUtils {
    static void prepareGroupByFunctions(
            QueryModel model,
            RecordMetadata metadata,
            FunctionParser functionParser,
            SqlExecutionContext executionContext,
            ObjList<GroupByFunction> groupByFunctions,
            ArrayColumnTypes valueTypes) throws SqlException {

        final ObjList<QueryColumn> columns = model.getColumns();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final QueryColumn column = columns.getQuick(i);
            ExpressionNode node = column.getAst();

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

    static void prepareGroupByRecordFunctions(
            @NotNull QueryModel model,
            RecordMetadata metadata,
            @NotNull ListColumnFilter listColumnFilter,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            GenericRecordMetadata groupByMetadata,
            ArrayColumnTypes keyTypes,
            ArrayColumnTypes valueTypes,
            IntIntHashMap symbolTableIndex,
            boolean timestampUnimportant
    ) {

        // Process group-by functions first to get the idea of
        // how many map values we will have.
        // Map value count is needed to calculate offsets for
        // map key columns.

        final int timestampIndex = metadata.getTimestampIndex();
        final ObjList<QueryColumn> columns = model.getColumns();
//        assert timestampIndex != -1;

        int keyColumnIndex = valueTypes.getColumnCount();
        int valueColumnIndex = 0;

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
                assert index != -1;
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
                            symbolTableIndex.put(i, index);
                            fun = new MapSymbolColumn(node.position, keyColumnIndex - 1, i);
                            break;
                        case ColumnType.DATE:
                            fun = new DateColumn(node.position, keyColumnIndex - 1);
                            break;
                        case ColumnType.TIMESTAMP:
                            fun = new TimestampColumn(node.position, keyColumnIndex - 1);
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
            } else {
                // add group-by function as a record function as well
                // so it can produce column values
                final GroupByFunction groupByFunction = groupByFunctions.getQuick(valueColumnIndex++);
                recordFunctions.add(groupByFunction);
                type = groupByFunction.getType();
            }

            // and finish with populating metadata for this factory
            groupByMetadata.add(new TableColumnMetadata(
                    Chars.toString(column.getName()),
                    type
            ));
        }
    }

    static void updateFunctions(ObjList<GroupByFunction> groupByFunctions, int n, MapValue value, Record record) {
        if (value.isNew()) {
            for (int i = 0; i < n; i++) {
                groupByFunctions.getQuick(i).computeFirst(value, record);
            }
        } else {
            for (int i = 0; i < n; i++) {
                groupByFunctions.getQuick(i).computeNext(value, record);
            }
        }
    }

    static void closeGroupByFunctions(ObjList<GroupByFunction> groupByFunctions) {
        for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
            groupByFunctions.getQuick(i).close();
        }
    }
}

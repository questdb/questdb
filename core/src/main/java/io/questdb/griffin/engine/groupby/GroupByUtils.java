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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Function;
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
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;

public class GroupByUtils {

    public static void prepareGroupByFunctions(
            @NotNull QueryModel model,
            @NotNull RecordMetadata metadata,
            @NotNull FunctionParser functionParser,
            @NotNull SqlExecutionContext executionContext,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            @Transient @NotNull IntList groupByFunctionPositions,
            @Nullable ObjList<Function> keyFunctions,
            @Nullable ObjList<ExpressionNode> keyFunctionNodes,
            @NotNull ArrayColumnTypes valueTypes
    ) throws SqlException {
        groupByFunctionPositions.clear();

        final ObjList<QueryColumn> columns = model.getColumns();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final QueryColumn column = columns.getQuick(i);
            final ExpressionNode node = column.getAst();

            if (node.type != ExpressionNode.LITERAL) {
                // this can fail
                final Function function = functionParser.parseFunction(
                        node,
                        metadata,
                        executionContext
                );

                if (function instanceof GroupByFunction) {
                    // configure map value columns for group-by functions
                    // some functions may need more than one column in values,
                    // so we have them do all the work
                    GroupByFunction func = (GroupByFunction) function;
                    func.pushValueTypes(valueTypes);
                    groupByFunctions.add(func);
                    groupByFunctionPositions.add(node.position);
                } else {
                    // it's a key function
                    assert keyFunctions != null && keyFunctionNodes != null : "key functions are supported in group by only";
                    keyFunctions.add(function);
                    keyFunctionNodes.add(node);
                }
            }
        }
    }

    // prepareGroupByFunctions must be called first to get the idea of how many map values
    // we will have. Map value count is needed to calculate offsets for map key columns.
    public static void prepareGroupByRecordFunctions(
            @NotNull ArrayDeque<ExpressionNode> sqlNodeStack,
            @NotNull QueryModel model,
            @NotNull RecordMetadata metadata,
            @NotNull ListColumnFilter listColumnFilter,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            @Transient @NotNull IntList groupByFunctionPositions,
            @Nullable ObjList<Function> keyFunctions,
            @NotNull ObjList<Function> recordFunctions,
            @Transient @NotNull IntList recordFunctionPositions,
            GenericRecordMetadata groupByMetadata,
            ArrayColumnTypes keyTypes,
            int valueCount,
            boolean timestampUnimportant,
            int timestampIndex
    ) throws SqlException {
        recordFunctionPositions.clear();

        final ObjList<QueryColumn> columns = model.getColumns();

        // first, calculate the number of column keys;
        // that's to be able to index function keys in the map
        // since we place them after the column keys
        int columnKeyCount = 0;
        int lastIndex = -1;
        for (int i = 0, n = columns.size(); i < n; i++) {
            final QueryColumn column = columns.getQuick(i);
            final ExpressionNode node = column.getAst();

            if (node.type == ExpressionNode.LITERAL) {
                int index = metadata.getColumnIndexQuiet(node.token);
                if (index == -1) {
                    throw SqlException.invalidColumn(node.position, node.token);
                }

                if (index != timestampIndex || timestampUnimportant) {
                    // when we have same column several times in a row
                    // we only add it once to map keys
                    if (lastIndex != index) {
                        columnKeyCount++;
                        lastIndex = index;
                    }
                }
            }
        }

        int keyColumnIndex = valueCount;
        int functionKeyColumnIndex = valueCount + columnKeyCount;
        int groupByFunctionIndex = 0;
        int keyFunctionIndex = 0;
        int inferredKeyColumnCount = 0;

        lastIndex = -1;
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
                        keyTypes.add(keyColumnIndex - valueCount, type);
                        keyColumnIndex++;
                        lastIndex = index;
                    }

                    final Function func = createColumnFunction(metadata, keyColumnIndex, type, index);
                    recordFunctions.add(func);
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
                    assert ColumnType.tagOf(type) == ColumnType.TIMESTAMP;
                }

                // and finish with populating metadata for this factory
                if (column.getAlias() == null) {
                    groupByMetadata.add(metadata.getColumnMetadata(index));
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
                Function func;
                if (groupByFunctionIndex < groupByFunctionPositions.size()
                        && node.position == groupByFunctionPositions.getQuick(groupByFunctionIndex)) {
                    // group-by function
                    // add group-by function as a record function as well,
                    // so it can produce column values
                    func = groupByFunctions.getQuick(groupByFunctionIndex++);
                    type = func.getType();
                    recordFunctions.add(func);
                    recordFunctionPositions.add(node.position);
                } else {
                    // key function
                    assert keyFunctions != null && keyFunctionIndex < keyFunctions.size();
                    Function keyFunc = keyFunctions.getQuick(keyFunctionIndex++);
                    type = keyFunc.getType();
                    // create a function to be used to access Map column
                    functionKeyColumnIndex++;
                    func = createColumnFunction(null, functionKeyColumnIndex, type, -1);
                    keyTypes.add(functionKeyColumnIndex - valueCount - 1, func.getType());

                    recordFunctions.add(func);
                    recordFunctionPositions.add(node.position);

                    inferredKeyColumnCount++;
                }

                // and finish with populating metadata for this factory
                groupByMetadata.add(
                        new TableColumnMetadata(
                                Chars.toString(column.getName()),
                                type,
                                false,
                                0,
                                func instanceof SymbolFunction && (((SymbolFunction) func).isSymbolTableStatic()),
                                func.getMetadata()
                        )
                );
            }
        }
        validateGroupByColumns(sqlNodeStack, model, inferredKeyColumnCount);
    }

    public static boolean supportParallelism(ObjList<GroupByFunction> groupByFunctions) {
        for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
            if (!groupByFunctions.getQuick(i).isParallelismSupported()) {
                return false;
            }
        }
        return true;
    }

    public static void toTop(ObjList<? extends Function> args) {
        for (int i = 0, n = args.size(); i < n; i++) {
            args.getQuick(i).toTop();
        }
    }

    public static void validateGroupByColumns(
            @NotNull ArrayDeque<ExpressionNode> sqlNodeStack,
            @NotNull QueryModel model,
            int inferredKeyColumnCount
    ) throws SqlException {
        final ObjList<ExpressionNode> groupByColumns = model.getGroupBy();
        int explicitKeyColumnCount = groupByColumns.size();
        if (explicitKeyColumnCount == 0) {
            return;
        }

        final QueryModel nested = model.getNestedModel();
        QueryModel chooseModel = model;
        while (chooseModel != null
                && chooseModel.getSelectModelType() != QueryModel.SELECT_MODEL_CHOOSE
                && chooseModel.getSelectModelType() != QueryModel.SELECT_MODEL_NONE) {
            chooseModel = chooseModel.getNestedModel();
        }

        for (int i = 0; i < explicitKeyColumnCount; i++) {
            final ExpressionNode key = groupByColumns.getQuick(i);
            switch (key.type) {
                case ExpressionNode.LITERAL:
                    final int dotIndex = Chars.indexOf(key.token, '.');

                    if (dotIndex > -1) {
                        int aliasIndex = model.getModelAliasIndex(key.token, 0, dotIndex);
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
                            // we need to descend to first NONE model and see if that can resolve columns we are
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
                        if (qc.getAst().type != ExpressionNode.LITERAL && qc.getAst().type != ExpressionNode.CONSTANT
                                && qc.getAst().type != ExpressionNode.FUNCTION && qc.getAst().type != ExpressionNode.OPERATION) {
                            throw SqlException.$(key.position, "group by column references aggregate expression");
                        }
                    }
                    break;
                case ExpressionNode.BIND_VARIABLE:
                    throw SqlException.$(key.position, "bind variable is not allowed here");
                case ExpressionNode.FUNCTION:
                case ExpressionNode.OPERATION:
                    ObjList<QueryColumn> availableColumns = nested.getTopDownColumns();
                    if (availableColumns.size() == 0) {
                        // Looks like we have merged inner virtual model in to the group-by one,
                        // so we should look up in the group-by model.
                        availableColumns = model.getTopDownColumns();
                    }
                    boolean invalid = true;
                    for (int j = 0, n = availableColumns.size(); j < n; j++) {
                        final QueryColumn qc = availableColumns.getQuick(j);
                        if (qc.getAst().type == key.type) {
                            if (ExpressionNode.compareNodesGroupBy(key, qc.getAst(), chooseModel)) {
                                invalid = false;
                                break;
                            }
                        } else if ( // might be a function or operation key's argument
                                qc.getAst().type == ExpressionNode.LITERAL
                                        && compareNodesGroupByFunctionKey(sqlNodeStack, chooseModel, key, qc.getAst())
                        ) {
                            invalid = false;
                            break;
                        }
                    }
                    if (invalid) {
                        throw SqlException.$(key.position, "group by expression does not match anything select in statement");
                    }
                    break;
                case ExpressionNode.CONSTANT:
                    // ignore
                    break;
                default:
                    throw SqlException.$(key.position, "unsupported type of expression");
            }
        }

        if (explicitKeyColumnCount < inferredKeyColumnCount) {
            throw SqlException.$(model.getModelPosition(), "not enough columns in group by");
        }
    }

    private static boolean compareNodesGroupByFunctionKey(
            ArrayDeque<ExpressionNode> sqlNodeStack,
            QueryModel chooseModel,
            ExpressionNode functionKey,
            ExpressionNode arg
    ) {
        sqlNodeStack.clear();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        while (!sqlNodeStack.isEmpty() || functionKey != null) {
            if (functionKey != null) {
                if (functionKey.paramCount < 3) {
                    if (functionKey.rhs != null) {
                        if (ExpressionNode.compareNodesGroupBy(functionKey.rhs, arg, chooseModel)) {
                            return true;
                        }
                        sqlNodeStack.push(functionKey.rhs);
                    }

                    if (functionKey.lhs != null) {
                        if (ExpressionNode.compareNodesGroupBy(functionKey.lhs, arg, chooseModel)) {
                            return true;
                        }
                    }
                    functionKey = functionKey.lhs;
                } else {
                    for (int i = 1, k = functionKey.paramCount; i < k; i++) {
                        ExpressionNode e = functionKey.args.getQuick(i);
                        if (ExpressionNode.compareNodesGroupBy(e, arg, chooseModel)) {
                            return true;
                        }
                        sqlNodeStack.push(e);
                    }

                    final ExpressionNode e = functionKey.args.getQuick(0);
                    if (ExpressionNode.compareNodesGroupBy(e, arg, chooseModel)) {
                        return true;
                    }
                    functionKey = e;
                }
            } else {
                functionKey = sqlNodeStack.poll();
            }
        }

        return false;
    }

    private static Function createColumnFunction(
            @Nullable RecordMetadata metadata,
            int keyColumnIndex,
            int type,
            int index
    ) {
        final Function func;
        switch (ColumnType.tagOf(type)) {
            case ColumnType.BOOLEAN:
                func = BooleanColumn.newInstance(keyColumnIndex - 1);
                break;
            case ColumnType.BYTE:
                func = ByteColumn.newInstance(keyColumnIndex - 1);
                break;
            case ColumnType.SHORT:
                func = ShortColumn.newInstance(keyColumnIndex - 1);
                break;
            case ColumnType.CHAR:
                func = CharColumn.newInstance(keyColumnIndex - 1);
                break;
            case ColumnType.INT:
                func = IntColumn.newInstance(keyColumnIndex - 1);
                break;
            case ColumnType.IPv4:
                func = IPv4Column.newInstance(keyColumnIndex - 1);
                break;
            case ColumnType.LONG:
                func = LongColumn.newInstance(keyColumnIndex - 1);
                break;
            case ColumnType.FLOAT:
                func = FloatColumn.newInstance(keyColumnIndex - 1);
                break;
            case ColumnType.DOUBLE:
                func = DoubleColumn.newInstance(keyColumnIndex - 1);
                break;
            case ColumnType.STRING:
                func = StrColumn.newInstance(keyColumnIndex - 1);
                break;
            case ColumnType.SYMBOL:
                if (metadata != null) {
                    // must be a column key
                    func = new MapSymbolColumn(keyColumnIndex - 1, index, metadata.isSymbolTableStatic(index));
                } else {
                    // must be a function key, so we treat symbols as strings
                    func = StrColumn.newInstance(keyColumnIndex - 1);
                }
                break;
            case ColumnType.DATE:
                func = DateColumn.newInstance(keyColumnIndex - 1);
                break;
            case ColumnType.TIMESTAMP:
                func = TimestampColumn.newInstance(keyColumnIndex - 1);
                break;
            case ColumnType.LONG256:
                func = Long256Column.newInstance(keyColumnIndex - 1);
                break;
            case ColumnType.GEOBYTE:
                func = GeoByteColumn.newInstance(keyColumnIndex - 1, type);
                break;
            case ColumnType.GEOSHORT:
                func = GeoShortColumn.newInstance(keyColumnIndex - 1, type);
                break;
            case ColumnType.GEOINT:
                func = GeoIntColumn.newInstance(keyColumnIndex - 1, type);
                break;
            case ColumnType.GEOLONG:
                func = GeoLongColumn.newInstance(keyColumnIndex - 1, type);
                break;
            case ColumnType.LONG128:
                func = Long128Column.newInstance(keyColumnIndex - 1);
                break;
            case ColumnType.UUID:
                func = UuidColumn.newInstance(keyColumnIndex - 1);
                break;
            default:
                func = BinColumn.newInstance(keyColumnIndex - 1);
                break;
        }
        return func;
    }
}

/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.cast.CastStrToSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.columns.BinColumn;
import io.questdb.griffin.engine.functions.columns.BooleanColumn;
import io.questdb.griffin.engine.functions.columns.ByteColumn;
import io.questdb.griffin.engine.functions.columns.CharColumn;
import io.questdb.griffin.engine.functions.columns.DateColumn;
import io.questdb.griffin.engine.functions.columns.DoubleColumn;
import io.questdb.griffin.engine.functions.columns.FloatColumn;
import io.questdb.griffin.engine.functions.columns.GeoByteColumn;
import io.questdb.griffin.engine.functions.columns.GeoIntColumn;
import io.questdb.griffin.engine.functions.columns.GeoLongColumn;
import io.questdb.griffin.engine.functions.columns.GeoShortColumn;
import io.questdb.griffin.engine.functions.columns.IPv4Column;
import io.questdb.griffin.engine.functions.columns.IntColumn;
import io.questdb.griffin.engine.functions.columns.IntervalColumn;
import io.questdb.griffin.engine.functions.columns.Long128Column;
import io.questdb.griffin.engine.functions.columns.Long256Column;
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.functions.columns.ShortColumn;
import io.questdb.griffin.engine.functions.columns.StrColumn;
import io.questdb.griffin.engine.functions.columns.TimestampColumn;
import io.questdb.griffin.engine.functions.columns.UuidColumn;
import io.questdb.griffin.engine.functions.columns.VarcharColumn;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;

import static io.questdb.griffin.model.ExpressionNode.LITERAL;

public class GroupByUtils {

    public static void assembleGroupByFunctions(
            @NotNull FunctionParser functionParser,
            @NotNull ArrayDeque<ExpressionNode> sqlNodeStack,
            QueryModel model,
            SqlExecutionContext executionContext,
            RecordMetadata baseMetadata,
            int timestampIndex,
            boolean timestampUnimportant,
            ObjList<GroupByFunction> outGroupByFunctions,
            IntList outGroupByFunctionPositions,
            ObjList<Function> outRecordFunctions,
            IntList outRecordFunctionPositions,
            GenericRecordMetadata outGroupByMetadata,
            @Nullable ObjList<Function> outKeyFunctions,
            @Nullable ObjList<ExpressionNode> outKeyFunctionNodes,
            ArrayColumnTypes outValueTypes,
            ArrayColumnTypes outKeyTypes,
            ListColumnFilter outColumnFilter,
            @Nullable ObjList<ExpressionNode> sampleByFill, // fill mode for sample by functions, for validation
            boolean validateFill
    ) throws SqlException {
        try {
            outGroupByFunctionPositions.clear();
            outRecordFunctionPositions.clear();
            int fillCount = sampleByFill != null ? sampleByFill.size() : 0;

            int columnKeyCount = 0;
            int lastIndex = -1;
            final ObjList<QueryColumn> columns = model.getColumns();

            // There are two iterations over the model's columns. The first iterations creates value
            // slots for the group-by functions. They are added first because each group-by function is likely
            // to require several slots. The number of slots for each function is not known upfront and
            // is effectively evaluates in the first loop.
            for (int i = 0, n = columns.size(); i < n; i++) {
                final QueryColumn column = columns.getQuick(i);
                final ExpressionNode node = column.getAst();

                if (node.type != LITERAL) {
                    // this can fail
                    final Function function = functionParser.parseFunction(
                            node,
                            baseMetadata,
                            executionContext
                    );

                    if (model.isMatView() && function.isNonDeterministic()) {
                        throw SqlException.nonDeterministicColumn(node.position, node.token);
                    }

                    // record functions will have all model function, including consecutive duplicates
                    outRecordFunctions.add(function);

                    if (function instanceof GroupByFunction) {
                        // configure map value columns for group-by functions
                        // some functions may need more than one column in values,
                        // so we have them do all the work
                        GroupByFunction func = (GroupByFunction) function;

                        // insert the function into our function list even before we validate it support a given
                        // fill type. it's to close the function properly when the validation fails
                        outGroupByFunctions.add(func);
                        outGroupByFunctionPositions.add(node.position);
                        if (fillCount > 0) {
                            // index of the function relative to the list of fill values
                            // we might have the same fill value for all functions
                            int funcIndex = outGroupByFunctions.size();
                            int sampleByFlags = func.getSampleByFlags();
                            ExpressionNode fillNode = sampleByFill.getQuick(Math.min(funcIndex, fillCount - 1));
                            if (validateFill) {
                                if (SqlKeywords.isNullKeyword(fillNode.token) && (sampleByFlags & GroupByFunction.SAMPLE_BY_FILL_NULL) == 0) {
                                    throw SqlException.$(node.position, "support for NULL fill is not yet implemented [function=").put(node)
                                            .put(", class=").put(func.getClass().getName())
                                            .put(']');
                                } else if (SqlKeywords.isPrevKeyword(fillNode.token) && (sampleByFlags & GroupByFunction.SAMPLE_BY_FILL_PREVIOUS) == 0) {
                                    throw SqlException.$(node.position, "support for PREV fill is not yet implemented [function=").put(node)
                                            .put(", class=").put(func.getClass().getName())
                                            .put(']');
                                } else if (SqlKeywords.isLinearKeyword(fillNode.token) && (sampleByFlags & GroupByFunction.SAMPLE_BY_FILL_LINEAR) == 0) {
                                    throw SqlException.$(node.position, "support for LINEAR fill is not yet implemented [function=").put(node)
                                            .put(", class=").put(func.getClass().getName())
                                            .put(']');
                                } else if (SqlKeywords.isNoneKeyword(fillNode.token) && (sampleByFlags & GroupByFunction.SAMPLE_BY_FILL_NONE) == 0) {
                                    throw SqlException.$(node.position, "support for NONE fill is not yet implemented [function=").put(node)
                                            .put(", class=").put(func.getClass().getName())
                                            .put(']');
                                } else if ((sampleByFlags & GroupByFunction.SAMPLE_BY_FILL_VALUE) == 0) {
                                    throw SqlException.$(node.position, "support for VALUE fill is not yet implemented [function=").put(node)
                                            .put(", class=").put(func.getClass().getName())
                                            .put(']');
                                }
                            }
                        }
                        func.initValueTypes(outValueTypes);
                    } else {
                        // it's a key function
                        if (outKeyFunctions == null || outKeyFunctionNodes == null) {
                            throw SqlException.$(node.position, "key functions are supported in GROUP BY only [function=").put(node).put(']');
                        }
                        outKeyFunctions.add(function);
                        outKeyFunctionNodes.add(node);
                    }
                } else {
                    // function is unknown at this iteration, because we cannot create function not knowing
                    // the slot in the map it will occupy.
                    outRecordFunctions.add(null);

                    int index = baseMetadata.getColumnIndexQuiet(node.token);
                    if (index == -1) {
                        throw SqlException.invalidColumn(node.position, node.token);
                    }

                    if (index != timestampIndex) {
                        // when we have same column several times in a row
                        // we only add it once to map keys
                        if (lastIndex != index) {
                            columnKeyCount++;
                            lastIndex = index;
                        }
                    }
                }
                outRecordFunctionPositions.add(node.position);
            }

            int valueCount = outValueTypes.getColumnCount();
            int keyColumnIndex = valueCount;
            int functionKeyColumnIndex = valueCount + columnKeyCount;
            int inferredKeyColumnCount = 0;

            lastIndex = -1;
            for (int i = 0, n = columns.size(); i < n; i++) {
                final QueryColumn column = columns.getQuick(i);
                final ExpressionNode node = column.getAst();
                final int type;

                if (node.type == LITERAL) {
                    // column index has already been validated
                    int index = baseMetadata.getColumnIndexQuiet(node.token);
                    type = baseMetadata.getColumnType(index);
                    if (index != timestampIndex || timestampUnimportant) {
                        if (lastIndex != index) {
                            outColumnFilter.add(index + 1);
                            outKeyTypes.add(keyColumnIndex - valueCount, type);
                            keyColumnIndex++;
                            lastIndex = index;
                        }
                        outRecordFunctions.set(i, createColumnFunction(baseMetadata, keyColumnIndex, type, index));
                    } else {
                        // set this function to null, cursor will replace it with an instance class
                        // timestamp function returns value of class member which makes it impossible
                        // to create these columns in advance of cursor instantiation
                        if (outGroupByMetadata.getTimestampIndex() == -1) {
                            outGroupByMetadata.setTimestampIndex(i);
                        }
                        assert ColumnType.tagOf(type) == ColumnType.TIMESTAMP;
                    }

                    // and finish with populating metadata for this factory
                    if (column.getAlias() == null) {
                        outGroupByMetadata.add(baseMetadata.getColumnMetadata(index));
                    } else {
                        outGroupByMetadata.add(
                                new TableColumnMetadata(
                                        Chars.toString(column.getAlias()),
                                        type,
                                        baseMetadata.isColumnIndexed(index),
                                        baseMetadata.getIndexValueBlockCapacity(index),
                                        baseMetadata.isSymbolTableStatic(index),
                                        baseMetadata.getMetadata(index)
                                )
                        );
                    }
                    inferredKeyColumnCount++;
                } else {
                    Function func = outRecordFunctions.getQuick(i);

                    if (!(func instanceof GroupByFunction)) {
                        // leave group-by function alone but re-write non-group-by functions as column references
                        functionKeyColumnIndex++;
                        Function columnRefFunc = createColumnFunction(null, functionKeyColumnIndex, func.getType(), -1);
                        outKeyTypes.add(functionKeyColumnIndex - valueCount - 1, columnRefFunc.getType());
                        if (func.getType() == ColumnType.SYMBOL && columnRefFunc.getType() == ColumnType.STRING) {
                            // must be a function key, so we need to cast it to symbol
                            columnRefFunc = new CastStrToSymbolFunctionFactory.Func(columnRefFunc);
                        }

                        // override function with column ref function
                        func = columnRefFunc;
                        outRecordFunctions.set(i, columnRefFunc);
                        inferredKeyColumnCount++;
                    }

                    // and finish with populating metadata for this factory
                    outGroupByMetadata.add(
                            new TableColumnMetadata(
                                    Chars.toString(column.getName()),
                                    func.getType(),
                                    false,
                                    0,
                                    func instanceof SymbolFunction && (((SymbolFunction) func).isSymbolTableStatic()),
                                    func.getMetadata()
                            )
                    );
                }
            }
            validateGroupByColumns(sqlNodeStack, model, inferredKeyColumnCount);
        } catch (Throwable e) {
            Misc.freeObjList(outGroupByFunctions);
            Misc.freeObjList(outKeyFunctions);
            throw e;
        }
    }

    public static Function createColumnFunction(
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
            case ColumnType.VARCHAR:
                func = VarcharColumn.newInstance(keyColumnIndex - 1);
                break;
            case ColumnType.SYMBOL:
                if (metadata != null) {
                    // must be a column key
                    func = new MapSymbolColumn(keyColumnIndex - 1, index, metadata.isSymbolTableStatic(index));
                } else {
                    // must be a function key, so we treat symbols as strings
                    func = new StrColumn(keyColumnIndex - 1);
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
            case ColumnType.INTERVAL:
                func = IntervalColumn.newInstance(keyColumnIndex - 1);
                break;
            default:
                func = BinColumn.newInstance(keyColumnIndex - 1);
                break;
        }
        return func;
    }

    // prepareGroupByFunctions must be called first to get the idea of how many map values
    // we will have. Map value count is needed to calculate offsets for map key columns.

    public static boolean isEarlyExitSupported(ObjList<GroupByFunction> functions) {
        for (int i = 0, n = functions.size(); i < n; i++) {
            if (!functions.getQuick(i).isEarlyExitSupported()) {
                return false;
            }
        }
        return true;
    }

    public static boolean isParallelismSupported(ObjList<GroupByFunction> functions) {
        for (int i = 0, n = functions.size(); i < n; i++) {
            if (!functions.getQuick(i).supportsParallelism()) {
                return false;
            }
        }
        return true;
    }

    public static void prepareWorkerGroupByFunctions(
            @NotNull QueryModel model,
            @NotNull RecordMetadata metadata,
            @NotNull FunctionParser functionParser,
            @NotNull SqlExecutionContext executionContext,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            @NotNull ObjList<GroupByFunction> workerGroupByFunctions
    ) throws SqlException {
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
                    workerGroupByFunctions.add(func);
                } else {
                    // it's a key function; we don't need it
                    Misc.free(function);
                }
            }
        }

        assert groupByFunctions.size() == workerGroupByFunctions.size();
        for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
            final GroupByFunction workerGroupByFunction = workerGroupByFunctions.getQuick(i);
            final GroupByFunction groupByFunction = groupByFunctions.getQuick(i);
            workerGroupByFunction.initValueIndex(groupByFunction.getValueIndex());
        }
    }

    public static void setAllocator(ObjList<GroupByFunction> functions, GroupByAllocator allocator) {
        for (int i = 0, n = functions.size(); i < n; i++) {
            functions.getQuick(i).setAllocator(allocator);
        }
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
                    ObjList<QueryColumn> availableColumns = model.getBottomUpColumns();
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
}

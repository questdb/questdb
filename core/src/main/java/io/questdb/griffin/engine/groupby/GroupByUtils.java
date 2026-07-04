/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.cairo.IndexType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.cast.CastStrToSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.columns.ArrayColumn;
import io.questdb.griffin.engine.functions.columns.BinColumn;
import io.questdb.griffin.engine.functions.columns.BooleanColumn;
import io.questdb.griffin.engine.functions.columns.ByteColumn;
import io.questdb.griffin.engine.functions.columns.CharColumn;
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
import io.questdb.griffin.engine.functions.columns.DateColumn;
import io.questdb.griffin.engine.functions.columns.DecimalColumn;
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
import io.questdb.griffin.engine.functions.groupby.SparklineGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.TwapGroupByFunction;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;

import static io.questdb.griffin.model.ExpressionNode.LITERAL;

public class GroupByUtils {
    public static final int PROJECTION_FUNCTION_FLAG_COLUMN = 0;
    public static final int PROJECTION_FUNCTION_FLAG_GROUP_BY = 2;
    public static final int PROJECTION_FUNCTION_FLAG_VIRTUAL = 1;

    public static void assembleGroupByFunctions(
            @NotNull FunctionParser functionParser,
            @NotNull ArrayDeque<ExpressionNode> sqlNodeStack,
            IQueryModel model,
            SqlExecutionContext executionContext,
            RecordMetadata baseMetadata,
            int timestampIndex,
            boolean isBaseTimestampAscending,
            boolean timestampUnimportant,
            ObjList<GroupByFunction> outGroupByFunctions,
            IntList outGroupByFunctionPositions,
            ObjList<Function> outerProjectionFunctions, // projection presented by the group-by execution, values are typically read from the map
            ObjList<Function> innerProjectionFunctions, // projection used by the group-by function to build maps
            IntList projectionFunctionPositions,
            IntList projectionFunctionFlags,
            GenericRecordMetadata projectionMetadata,
            ArrayColumnTypes outValueTypes,
            ArrayColumnTypes outKeyTypes,
            ListColumnFilter outColumnFilter,
            @Nullable ObjList<ExpressionNode> sampleByFill, // fill mode for sample by functions, for validation
            boolean validateFill,
            ObjList<QueryColumn> columns,
            @Nullable ObjList<ObjList<Function>> extraOuterProjectionFunctions
    ) throws SqlException {
        try {
            outGroupByFunctionPositions.clear();
            projectionFunctionPositions.clear();
            int fillCount = sampleByFill != null ? sampleByFill.size() : 0;

            int columnKeyCount = 0;
            int lastIndex = -1;

            // compile functions upfront and assemble the metadata for group-by
            for (int i = 0, n = columns.size(); i < n; i++) {
                final QueryColumn column = columns.getQuick(i);
                final ExpressionNode node = column.getAst();
                int index = SqlUtil.getColumnIndexQuiet(baseMetadata, node.token);
                TableColumnMetadata m = null;
                if (node.type != LITERAL || index != timestampIndex || timestampUnimportant) {
                    final Function func = functionParser.parseFunction(
                            node,
                            baseMetadata,
                            executionContext
                    );

                    outerProjectionFunctions.add(func);
                    if (extraOuterProjectionFunctions != null) {
                        for (int d = 0, dn = extraOuterProjectionFunctions.size(); d < dn; d++) {
                            Function extraFunc = functionParser.parseFunction(node, baseMetadata, executionContext);
                            extraOuterProjectionFunctions.getQuick(d).add(extraFunc);
                        }
                    }
                    innerProjectionFunctions.add(func);

                    index = findColumnKeyIndex(node, func, baseMetadata);
                    if (index != -1) {
                        // it's a column key
                        projectionFunctionFlags.add(PROJECTION_FUNCTION_FLAG_COLUMN);
                    } else {
                        // it's either a function key or an aggregate function
                        m = new TableColumnMetadata(
                                SqlUtil.toColumnName(column.getName()),
                                func.getType(),
                                IndexType.NONE,
                                0,
                                func instanceof SymbolFunction && (((SymbolFunction) func).isSymbolTableStatic()),
                                func.getMetadata()
                        );

                        if (func instanceof GroupByFunction) {
                            projectionFunctionFlags.add(PROJECTION_FUNCTION_FLAG_GROUP_BY);
                        } else {
                            projectionFunctionFlags.add(PROJECTION_FUNCTION_FLAG_VIRTUAL);
                        }
                    }
                } else {
                    // set this function to null, cursor will replace it with an instance class;
                    // timestamp function returns value of class member which makes it impossible
                    // to create these columns in advance of cursor instantiation
                    outerProjectionFunctions.add(null);
                    if (extraOuterProjectionFunctions != null) {
                        for (int d = 0, dn = extraOuterProjectionFunctions.size(); d < dn; d++) {
                            extraOuterProjectionFunctions.getQuick(d).add(null);
                        }
                    }
                    projectionFunctionFlags.add(PROJECTION_FUNCTION_FLAG_COLUMN);

                    if (projectionMetadata.getTimestampIndex() == -1) {
                        projectionMetadata.setTimestampIndex(i);
                    }
                }

                if (m == null) {
                    if (column.getAlias() == null) {
                        m = baseMetadata.getColumnMetadata(index);
                    } else {
                        m = new TableColumnMetadata(
                                SqlUtil.toColumnName(column.getAlias()),
                                baseMetadata.getColumnType(index),
                                baseMetadata.getColumnIndexType(index),
                                baseMetadata.getIndexValueBlockCapacity(index),
                                baseMetadata.isSymbolTableStatic(index),
                                baseMetadata.getMetadata(index)
                        );
                    }
                }
                projectionMetadata.add(m);
            }

            // When the user provided more than one FILL entry but fewer than the
            // aggregate count, reject up front with the precise "not enough fill
            // values" error. The per-aggregate validator below would otherwise
            // clamp later aggregates onto fill[fillCount - 1] and could fire a
            // misleading "support for X fill is not yet implemented" for any
            // aggregate that does not accept the clamped fill type. The same
            // count condition is re-checked in SqlCodeGenerator.generateFill as
            // a backstop for the non-keyed FILL(value) rewrite path. Defer when
            // FILL(NONE) is mixed with other fills -- that case has a more
            // specific error ("FILL(NONE) cannot be combined with other fill
            // values") that fires later in generateFill and should win. Only
            // outerProjectionFunctions has been populated by this point, so the
            // count uses GroupByFunction instances from that list (key columns
            // and CAST-wrapped column functions never report as GroupByFunction
            // per findColumnKeyIndex).
            if (validateFill && fillCount > 1) {
                boolean hasNoneMixed = false;
                for (int i = 0; i < fillCount; i++) {
                    if (SqlKeywords.isNoneKeyword(sampleByFill.getQuick(i).token)) {
                        hasNoneMixed = true;
                        break;
                    }
                }
                if (!hasNoneMixed) {
                    int aggregateCount = 0;
                    for (int i = 0, n = outerProjectionFunctions.size(); i < n; i++) {
                        if (outerProjectionFunctions.getQuick(i) instanceof GroupByFunction) {
                            aggregateCount++;
                        }
                    }
                    if (fillCount < aggregateCount) {
                        throw SqlException.$(sampleByFill.getQuick(0).position, "not enough fill values");
                    }
                }
            }

            // There are two iterations over the model's columns. The first iterations create value
            // slots for the group-by functions. They are added first because each group-by function is likely
            // to require several slots. The number of slots for each function is not known upfront and
            // is effectively evaluated in the first loop.
            for (int i = 0, n = columns.size(); i < n; i++) {
                final QueryColumn column = columns.getQuick(i);
                final ExpressionNode node = column.getAst();
                final Function func = outerProjectionFunctions.getQuick(i);

                final int index = findColumnKeyIndex(node, func, baseMetadata);
                if (index != -1) {
                    if (index != timestampIndex || timestampUnimportant) {
                        // when we have the same column several times in a row,
                        // we only add it once to map keys
                        if (lastIndex != index) {
                            columnKeyCount++;
                            lastIndex = index;
                        }
                    }
                } else {
                    if (node.type == LITERAL) {
                        // looks like we failed to find the column by the literal
                        throw SqlException.invalidColumn(node.position, node.token);
                    }

                    if (func instanceof GroupByFunction groupByFunc) {
                        // configure map value columns for group-by functions
                        // some functions may need more than one column in values,
                        // so we have them do all the work

                        // insert the function into our function list even before we validate it support a given
                        // fill type. it's to close the function properly when the validation fails
                        outGroupByFunctions.add(groupByFunc);
                        outGroupByFunctionPositions.add(node.position);
                        if (groupByFunc instanceof TwapGroupByFunction twapFunc) {
                            twapFunc.validateTimestampArg(timestampIndex, isBaseTimestampAscending, node.position);
                        } else if (groupByFunc instanceof SparklineGroupByFunction sparklineFunc) {
                            sparklineFunc.validateScanDirection(isBaseTimestampAscending, node.position);
                        }
                        if (fillCount > 0) {
                            // 0-based index of the just-added aggregate. The Math.min clamp
                            // below covers only the single-fill broadcast case (FILL(NULL)
                            // applied to N aggregates -- every iteration lands on fill[0]).
                            // The multi-fill count mismatch (fillCount > 1, fillCount <
                            // aggregateCount) is rejected as "not enough fill values" up
                            // front, so it cannot reach this loop.
                            int funcIndex = outGroupByFunctions.size() - 1;
                            int sampleByFlags = groupByFunc.getSampleByFlags();
                            ExpressionNode fillNode = sampleByFill.getQuick(Math.min(funcIndex, fillCount - 1));
                            if (validateFill) {
                                assert (sampleByFlags & GroupByFunction.SAMPLE_BY_FILL_NONE) != 0 :
                                        "aggregate must support FILL(NONE): " + groupByFunc.getClass().getName();
                                if (SqlKeywords.isNullKeyword(fillNode.token) && (sampleByFlags & GroupByFunction.SAMPLE_BY_FILL_NULL) == 0) {
                                    throw SqlException.$(fillNode.position, "support for NULL fill is not yet implemented [function=").put(node)
                                            .put(", class=").put(groupByFunc.getClass().getName())
                                            .put(']');
                                } else if (SqlKeywords.isPrevKeyword(fillNode.token) && (sampleByFlags & GroupByFunction.SAMPLE_BY_FILL_PREVIOUS) == 0) {
                                    throw SqlException.$(fillNode.position, "support for PREV fill is not yet implemented [function=").put(node)
                                            .put(", class=").put(groupByFunc.getClass().getName())
                                            .put(']');
                                } else if (SqlKeywords.isLinearKeyword(fillNode.token) && (sampleByFlags & GroupByFunction.SAMPLE_BY_FILL_LINEAR) == 0) {
                                    throw SqlException.$(fillNode.position, "support for LINEAR fill is not yet implemented [function=").put(node)
                                            .put(", class=").put(groupByFunc.getClass().getName())
                                            .put(']');
                                } else if (
                                        !SqlKeywords.isNullKeyword(fillNode.token) &&
                                                !SqlKeywords.isPrevKeyword(fillNode.token) &&
                                                !SqlKeywords.isLinearKeyword(fillNode.token) &&
                                                !SqlKeywords.isNoneKeyword(fillNode.token) &&
                                                (sampleByFlags & GroupByFunction.SAMPLE_BY_FILL_VALUE) == 0
                                ) {
                                    throw SqlException.$(fillNode.position, "support for VALUE fill is not yet implemented [function=").put(node)
                                            .put(", class=").put(groupByFunc.getClass().getName())
                                            .put(']');
                                }
                            }
                        }
                        groupByFunc.initValueTypes(outValueTypes);
                        if (extraOuterProjectionFunctions != null) {
                            for (int d = 0, dn = extraOuterProjectionFunctions.size(); d < dn; d++) {
                                Function extraFunc = extraOuterProjectionFunctions.getQuick(d).getQuick(i);
                                if (extraFunc instanceof GroupByFunction extraGbf) {
                                    extraGbf.initSharedFrom(groupByFunc);
                                }
                            }
                        }
                    }
                }
                projectionFunctionPositions.add(node.position);
            }

            int valueCount = outValueTypes.getColumnCount();
            int keyColumnIndex = valueCount;
            int functionKeyColumnIndex = valueCount + columnKeyCount;
            int inferredKeyColumnCount = 0;

            lastIndex = -1;
            for (int i = 0, n = columns.size(); i < n; i++) {
                final QueryColumn column = columns.getQuick(i);
                final ExpressionNode node = column.getAst();
                final Function func = outerProjectionFunctions.getQuick(i);

                final int index = findColumnKeyIndex(node, func, baseMetadata);
                if (index != -1) {
                    final int type = baseMetadata.getColumnType(index);

                    if (index != timestampIndex || timestampUnimportant) {
                        if (lastIndex != index) {
                            outColumnFilter.add(index + 1);
                            outKeyTypes.add(keyColumnIndex - valueCount, type);
                            keyColumnIndex++;
                            lastIndex = index;
                        }
                        outerProjectionFunctions.set(i, createColumnFunction(baseMetadata, keyColumnIndex, type, index));
                        if (extraOuterProjectionFunctions != null) {
                            for (int d = 0, dn = extraOuterProjectionFunctions.size(); d < dn; d++) {
                                extraOuterProjectionFunctions.getQuick(d).set(i, createColumnFunction(baseMetadata, keyColumnIndex, type, index));
                            }
                        }
                    }

                    // and finish with populating metadata for this factory
                    inferredKeyColumnCount++;
                } else if (!(func instanceof GroupByFunction)) {
                    // leave group-by function alone but re-write non-group-by functions as column references
                    functionKeyColumnIndex++;
                    Function columnRefFunc = createColumnFunction(null, functionKeyColumnIndex, func.getType(), -1);
                    outKeyTypes.add(functionKeyColumnIndex - valueCount - 1, columnRefFunc.getType());
                    if (func.getType() == ColumnType.SYMBOL && columnRefFunc.getType() == ColumnType.STRING) {
                        // must be a function key, so we need to cast it to symbol
                        columnRefFunc = new CastStrToSymbolFunctionFactory.Func(columnRefFunc);
                    }
                    // override function with column ref function
                    outerProjectionFunctions.set(i, columnRefFunc);

                    // Currently unreachable: VirtualRecordCursorFactory does not support shared cursors currently.
                    // This code becomes reachable if VirtualRecordCursorFactory gains supportsSharedCursors() support in the future.
                    if (extraOuterProjectionFunctions != null) {
                        for (int d = 0, dn = extraOuterProjectionFunctions.size(); d < dn; d++) {
                            Function extraRef = createColumnFunction(null, functionKeyColumnIndex, func.getType(), -1);
                            if (func.getType() == ColumnType.SYMBOL && extraRef.getType() == ColumnType.STRING) {
                                extraRef = new CastStrToSymbolFunctionFactory.Func(extraRef);
                            }
                            extraOuterProjectionFunctions.getQuick(d).set(i, extraRef);
                        }
                    }
                    inferredKeyColumnCount++;
                }
            }
            validateGroupByColumns(sqlNodeStack, model, inferredKeyColumnCount);
        } catch (Throwable th) {
            // The first loop adds each parsed Function to both lists, so they share
            // references. The timestamp column appends null to outer but skips inner,
            // so subsequent entries sit at outer[i] and inner[i-1]. The third loop
            // may also replace outer entries with column-ref Functions, leaving the
            // original parsed Function reachable only via inner. Free outer first
            // (Misc.free is null-safe), keeping the list as a reference-identity
            // index, then walk inner and free only references not already in outer.
            // Closing the same Function twice would underflow allocator counters.
            for (int i = 0, n = outerProjectionFunctions.size(); i < n; i++) {
                Misc.free(outerProjectionFunctions.getQuick(i));
            }
            for (int i = 0, n = innerProjectionFunctions.size(); i < n; i++) {
                Function f = innerProjectionFunctions.getQuick(i);
                if (f != null && !containsIdentity(outerProjectionFunctions, f)) {
                    Misc.free(f);
                }
            }
            outerProjectionFunctions.clear();
            innerProjectionFunctions.clear();
            // Every group-by function was also added to outerProjectionFunctions (same
            // instance) and freed by the loop above. Clear the list so callers that free
            // it on their own error path (the JOIN callsites call
            // Misc.freeObjList(groupByFunctions)) don't close the same instance twice.
            outGroupByFunctions.clear();
            if (extraOuterProjectionFunctions != null) {
                for (int i = 0, n = extraOuterProjectionFunctions.size(); i < n; i++) {
                    Misc.freeObjListAndClear(extraOuterProjectionFunctions.getQuick(i));
                }
                extraOuterProjectionFunctions.clear();
            }
            throw th;
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
                func = new CharColumn(keyColumnIndex - 1);
                break;
            case ColumnType.INT:
                func = IntColumn.newInstance(keyColumnIndex - 1);
                break;
            case ColumnType.IPv4:
                func = new IPv4Column(keyColumnIndex - 1);
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
                func = new StrColumn(keyColumnIndex - 1);
                break;
            case ColumnType.VARCHAR:
                func = new VarcharColumn(keyColumnIndex - 1);
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
                func = TimestampColumn.newInstance(keyColumnIndex - 1, type);
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
                func = IntervalColumn.newInstance(keyColumnIndex - 1, type);
                break;
            case ColumnType.ARRAY:
                func = new ArrayColumn(keyColumnIndex - 1, type);
                break;
            case ColumnType.DECIMAL8:
            case ColumnType.DECIMAL16:
            case ColumnType.DECIMAL32:
            case ColumnType.DECIMAL64:
            case ColumnType.DECIMAL128:
            case ColumnType.DECIMAL256:
                func = DecimalColumn.newInstance(keyColumnIndex - 1, type);
                break;
            default:
                func = BinColumn.newInstance(keyColumnIndex - 1);
                break;
        }
        return func;
    }

    /**
     * Returns the page-frame column index when {@code arg} is a direct
     * {@link ColumnFunction} reference whose native storage matches
     * {@code expectedType}, allowing the batched GROUP BY fast path to read
     * values straight from page-frame memory. Returns -1 otherwise, signalling
     * that the caller must fall back to {@code arg.getXxx(record)}.
     * <p>
     * The type check guards against silent type reinterpretation: a function
     * like {@code avg(long_col)} keeps the raw {@code LongColumn} as its arg
     * (no explicit cast, because {@link io.questdb.griffin.engine.functions.LongFunction#getDouble}
     * already widens), but reading that column's 8-byte storage as a
     * {@code double} would produce meaningless denormal values.
     */
    public static int directArgColumnIndex(Function arg, int expectedType) {
        if (arg instanceof ColumnFunction cf && arg.getType() == expectedType) {
            return cf.getColumnIndex();
        }
        return -1;
    }

    /**
     * Variant of {@link #directArgColumnIndex} that matches by column type tag
     * rather than by full type. Useful for parameterised types such as geohashes
     * whose full {@code arg.getType()} value packs storage bits into the upper
     * half and so never equals a bare tag like {@link ColumnType#GEOBYTE}.
     */
    public static int directArgColumnIndexByTag(Function arg, int expectedTag) {
        if (arg instanceof ColumnFunction cf && ColumnType.tagOf(arg.getType()) == expectedTag) {
            return cf.getColumnIndex();
        }
        return -1;
    }

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

    // assembleGroupByFunctions must be called before this call to get the idea of how many map values
    // we will have. Map value count is needed to calculate offsets for map key columns.
    public static void prepareWorkerGroupByFunctions(
            @NotNull IQueryModel model,
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

                if (function instanceof GroupByFunction func) {
                    // configure map value columns for group-by functions
                    // some functions may need more than one column in values,
                    // so we have them do all the work
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
            @NotNull IQueryModel model,
            int inferredKeyColumnCount
    ) throws SqlException {
        final ObjList<ExpressionNode> groupByColumns = model.getGroupBy();
        int explicitKeyColumnCount = groupByColumns.size();
        if (explicitKeyColumnCount == 0) {
            return;
        }

        IQueryModel chooseModel = model;
        while (chooseModel != null
                && chooseModel.getSelectModelType() != IQueryModel.SELECT_MODEL_CHOOSE
                && chooseModel.getSelectModelType() != IQueryModel.SELECT_MODEL_NONE) {
            chooseModel = chooseModel.getNestedModel();
        }

        for (int i = 0; i < explicitKeyColumnCount; i++) {
            final ExpressionNode key = groupByColumns.getQuick(i);
            switch (key.type) {
                case ExpressionNode.LITERAL:
                    final int dotIndex = Chars.indexOfLastUnquoted(key.token, '.');

                    if (dotIndex > -1) {
                        int aliasIndex = model.getModelAliasIndex(key.token, 0, dotIndex);
                        if (aliasIndex > -1) {
                            // we should now check against the main model
                            int refColumn = model.getAliasToColumnMap().keyIndex(key.token);
                            if (refColumn > -1) {
                                // a.x not found, look for "x"
                                refColumn = model.getAliasToColumnMap().keyIndex(key.token, dotIndex + 1, key.token.length());
                            }

                            if (refColumn > -1) {
                                throw SqlException.$(key.position, "group by column does not match any key column is select statement");
                            }
                        } else {
                            // the table alias could be referencing a join model
                            // we need to descend to the first NONE model and see if that can resolve columns we are
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
                        if (qc.getAst().type != ExpressionNode.LITERAL
                                && qc.getAst().type != ExpressionNode.CONSTANT
                                && qc.getAst().type != ExpressionNode.BIND_VARIABLE
                                && qc.getAst().type != ExpressionNode.FUNCTION
                                && qc.getAst().type != ExpressionNode.OPERATION) {
                            throw SqlException.$(key.position, "group by column references aggregate expression");
                        }
                    }
                    break;
                case ExpressionNode.BIND_VARIABLE:
                    // a bind variable is constant per query, so GROUP BY by one
                    // collapses to a single bucket - same semantics as a CONSTANT key.
                    break;
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
            IQueryModel chooseModel,
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

    private static boolean containsIdentity(ObjList<Function> list, Function target) {
        for (int i = 0, n = list.size(); i < n; i++) {
            if (list.getQuick(i) == target) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the column index from the given base metadata in two cases:
     * 1. it's a column literal
     * 2. cast(a_column as same_type) which is compiled as a_column (the case is no-op)
     */
    private static int findColumnKeyIndex(ExpressionNode node, Function func, RecordMetadata baseMetadata) {
        if (node.type == LITERAL) {
            return SqlUtil.getColumnIndexQuiet(baseMetadata, node.token);
        }
        if (SqlKeywords.isCastKeyword(node.token) && func instanceof ColumnFunction) {
            return ((ColumnFunction) func).getColumnIndex();
        }
        return -1;
    }
}

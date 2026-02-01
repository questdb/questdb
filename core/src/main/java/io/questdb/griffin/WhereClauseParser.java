/*******************************************************************************
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

package io.questdb.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.functions.AbstractGeoHashFunction;
import io.questdb.griffin.model.AliasTranslator;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.griffin.model.IntrinsicModel;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.str.FlyweightCharSequence;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;

import static io.questdb.griffin.SqlKeywords.*;

/**
 * Extracts most important predicates from where clause :
 * - designated timestamp expressions to use for interval scan
 * - indexed symbol column expressions to use for index scan
 **/
public final class WhereClauseParser implements Mutable {
    // Internal optimization marker for timestamp predicates pushed through dateadd transforms.
    // and_offset(predicate, unit, offset) wraps predicates that need offset adjustment.
    // This is NOT a user-facing function - it's injected by SqlOptimiser during predicate pushdown.
    private static final int INTRINSIC_OP_AND_OFFSET = 10;
    private static final int INTRINSIC_OP_BETWEEN = 9;
    private static final int INTRINSIC_OP_EQUAL = 6;
    private static final int INTRINSIC_OP_GREATER = 2;
    private static final int INTRINSIC_OP_GREATER_EQ = 3;
    private static final int INTRINSIC_OP_IN = 1;
    private static final int INTRINSIC_OP_LESS = 4;
    private static final int INTRINSIC_OP_LESS_EQ = 5;
    private static final int INTRINSIC_OP_NOT = 8;
    private static final int INTRINSIC_OP_NOT_EQ = 7;
    private static final CharSequenceIntHashMap intrinsicOps = new CharSequenceIntHashMap();
    // TODO: configure size
    private final ObjectPool<FlyweightCharSequence> csPool = new ObjectPool<>(FlyweightCharSequence.FACTORY, 64);
    private final ObjList<ExpressionNode> keyExclNodes = new ObjList<>();
    private final ObjList<ExpressionNode> keyNodes = new ObjList<>();
    // TODO: configure size
    private final ObjectPool<IntrinsicModel> models = new ObjectPool<>(IntrinsicModel.FACTORY, 8);
    private final ArrayDeque<ExpressionNode> stack = new ArrayDeque<>();
    private final CharSequenceHashSet tempK = new CharSequenceHashSet();
    private final IntList tempKeyExcludedValuePos = new IntList();
    // expression node types (literal, bind var, etc.) of excluded keys used  when comparing values and generating functions
    private final IntList tempKeyExcludedValueType = new IntList();
    // assumption: either tempKeyExcludedValues or tempKeyValues has to be empty, otherwise sql code generator will produce wrong factory
    private final CharSequenceHashSet tempKeyExcludedValues = new CharSequenceHashSet();
    private final IntList tempKeyValuePos = new IntList();
    // expression node types (literal, bind var, etc.) of tempKeys used  when comparing values
    private final IntList tempKeyValueType = new IntList();
    private final CharSequenceHashSet tempKeyValues = new CharSequenceHashSet();
    private final CharSequenceHashSet tempKeys = new CharSequenceHashSet();
    private final ObjList<ExpressionNode> tempNodes = new ObjList<>();
    private final IntList tempP = new IntList();
    private final IntList tempPos = new IntList();
    private final IntList tempT = new IntList();
    private final IntList tempType = new IntList();
    private final ObjList<Function> tmpFunctions = new ObjList<>();
    private boolean allKeyExcludedValuesAreKnown = true;
    private boolean allKeyValuesAreKnown = true;
    private boolean isConstFunction;
    private CharSequence preferredKeyColumn;
    private CharSequence timestamp;

    @Override
    public void clear() {
        models.clear();
        stack.clear();
        keyNodes.clear();
        keyExclNodes.clear();
        tempNodes.clear();
        tempKeys.clear();
        tempPos.clear();
        tempType.clear();
        tempK.clear();
        tempP.clear();
        tempT.clear();
        tmpFunctions.clear();
        clearKeys();
        clearExcludedKeys();
        csPool.clear();
        timestamp = null;
        preferredKeyColumn = null;
        allKeyValuesAreKnown = true;
        allKeyExcludedValuesAreKnown = true;
    }

    public IntrinsicModel extract(
            @NotNull AliasTranslator translator,
            @NotNull ObjectPool<ExpressionNode> expressionNodePool,
            ExpressionNode node,
            @NotNull RecordMetadata m,
            CharSequence preferredKeyColumn,
            int timestampIndex,
            @NotNull FunctionParser functionParser,
            @NotNull RecordMetadata metadata,
            @NotNull SqlExecutionContext executionContext,
            boolean latestByMultiColumn,
            @NotNull TableReader reader
    ) throws SqlException {
        clearKeys();
        clearExcludedKeys();

        this.timestamp = timestampIndex < 0 ? null : m.getColumnName(timestampIndex);
        this.preferredKeyColumn = preferredKeyColumn;

        // Extracts designated timestamp argument from dateadd predicates, if any.
        rewriteDateaddTimestamp(expressionNodePool, node);

        IntrinsicModel model = models.next();
        int timestampType = reader.getMetadata().getTimestampType();
        model.of(timestampType, reader.getPartitionedBy(), executionContext.getCairoEngine().getConfiguration());
        final TimestampDriver timestampDriver = ColumnType.getTimestampDriver(timestampType);

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        if (removeAndIntrinsics(
                timestampDriver,
                translator,
                model,
                node,
                m,
                functionParser,
                metadata,
                executionContext,
                latestByMultiColumn, reader)) {
            createKeyValueBindVariables(model, functionParser, executionContext);
            return model;
        }

        ExpressionNode root = node;
        stack.clear();
        while (!stack.isEmpty() || node != null) {
            if (node != null) {
                if (isAndKeyword(node.token)) {
                    if (!removeAndIntrinsics(
                            timestampDriver,
                            translator,
                            model,
                            node.rhs,
                            m,
                            functionParser,
                            metadata,
                            executionContext,
                            latestByMultiColumn,
                            reader)) {
                        // Check if rhs is an OR of timestamp intrinsics
                        if (!tryExtractOrTimestampIntrinsics(timestampDriver, model, node.rhs, functionParser, metadata, executionContext)) {
                            stack.push(node.rhs);
                        }
                    }
                    if (removeAndIntrinsics(
                            timestampDriver,
                            translator,
                            model,
                            node.lhs,
                            m,
                            functionParser,
                            metadata,
                            executionContext,
                            latestByMultiColumn,
                            reader)) {
                        node = null;
                    } else if (tryExtractOrTimestampIntrinsics(timestampDriver, model, node.lhs, functionParser, metadata, executionContext)) {
                        // lhs was an OR of timestamp intrinsics, successfully extracted
                        node = null;
                    } else {
                        node = node.lhs;
                    }
                } else if (isOrKeyword(node.token) && tryExtractOrTimestampIntrinsics(timestampDriver, model, node, functionParser, metadata, executionContext)) {
                    // Entire OR tree was extracted as timestamp intrinsics
                    node = stack.poll();
                } else {
                    node = stack.poll();
                }
            } else {
                node = stack.poll();
            }
        }
        applyKeyExclusions(translator, functionParser, metadata, executionContext, model);
        model.filter = collapseIntrinsicNodes(root);
        createKeyValueBindVariables(model, functionParser, executionContext);
        return model;
    }

    public IntrinsicModel getEmpty(int timestampType, int partitionBy, CairoConfiguration configuration) {
        IntrinsicModel model = models.next();
        model.of(timestampType, partitionBy, configuration);
        return model;
    }

    private static short adjustComparison(boolean equalsTo, boolean isLo) {
        return equalsTo ? 0 : isLo ? (short) 1 : (short) -1;
    }

    private static boolean canCastToTimestamp(int type) {
        final int typeTag = ColumnType.tagOf(type);
        return typeTag == ColumnType.TIMESTAMP
                || typeTag == ColumnType.DATE
                || typeTag == ColumnType.STRING
                || typeTag == ColumnType.SYMBOL
                || typeTag == ColumnType.LONG
                || typeTag == ColumnType.VARCHAR;
    }

    private static void checkNodeValid(ExpressionNode node) throws SqlException {
        if (node.lhs == null || node.rhs == null) {
            throw SqlException.$(node.position, "Argument expected");
        }
    }

    private static long getTimestampFromConstFunction(
            TimestampDriver timestampDriver,
            Function function,
            int functionPosition,
            boolean detectIntervals
    ) throws SqlException {
        int type = function.getType();
        if (ColumnType.isSymbolOrString(type)) {
            CharSequence str = function.getStrA(null);
            return str == null ? Numbers.LONG_NULL : parseStringAsTimestamp(timestampDriver, str, functionPosition, detectIntervals);
        } else if (type == ColumnType.VARCHAR) {
            Utf8Sequence varchar = function.getVarcharA(null);
            return varchar == null
                    ? Numbers.LONG_NULL
                    : parseStringAsTimestamp(timestampDriver, varchar.asAsciiCharSequence(), functionPosition, detectIntervals);
        } else {
            return timestampDriver.from(function.getTimestamp(null), ColumnType.getTimestampType(type));
        }
    }

    private static boolean isFunc(ExpressionNode n) {
        return n.type == ExpressionNode.FUNCTION
                || n.type == ExpressionNode.BIND_VARIABLE
                || n.type == ExpressionNode.OPERATION;
    }

    /**
     * Checks if a symbol column with idx index has more distinct values
     * or has higher capacity than the current key column.
     */
    private static boolean isMoreSelective(IntrinsicModel model, RecordMetadata meta, TableReader reader, int idx) {
        SymbolMapReader colReader = reader.getSymbolMapReader(idx);
        SymbolMapReader keyReader = reader.getSymbolMapReader(meta.getColumnIndex(model.keyColumn));
        int colCount = colReader.getSymbolCount();
        int keyCount = keyReader.getSymbolCount();
        return colCount > keyCount
                || (colCount == keyCount && colReader.getSymbolCapacity() > keyReader.getSymbolCapacity());
    }

    private static boolean isTypeMismatch(int typeA, int typeB) {
        return (typeA == ExpressionNode.BIND_VARIABLE) != (typeB == ExpressionNode.BIND_VARIABLE);
    }

    private static boolean nodesEqual(ExpressionNode left, ExpressionNode right) {
        return (left.type == ExpressionNode.LITERAL || left.type == ExpressionNode.CONSTANT)
                && (right.type == ExpressionNode.LITERAL || right.type == ExpressionNode.CONSTANT)
                && Chars.equals(left.token, right.token);
    }

    private static long parseStringAsTimestamp(
            TimestampDriver timestampDriver,
            @Nullable CharSequence str,
            int position,
            boolean detectIntervals
    ) throws SqlException {
        try {
            return timestampDriver.parseFloorLiteral(str);
        } catch (NumericException ignore) {
            if (detectIntervals && str != null) {
                for (int i = 0, lim = str.length(); i < lim; i++) {
                    if (str.charAt(i) == ';') {
                        throw SqlException.$(position, "Not a date, use IN keyword with intervals");
                    }
                }
            }
            throw SqlException.invalidDate(str, position);
        }
    }

    private static long parseTokenAsTimestamp(TimestampDriver timestampDriver, ExpressionNode lo) throws SqlException {
        try {
            if (!isNullKeyword(lo.token)) {
                return timestampDriver.parseQuotedLiteral(lo.token);
            }
            return Numbers.LONG_NULL;
        } catch (NumericException e1) {
            try {
                return Numbers.parseLong(lo.token);
            } catch (NumericException ignore) {
                throw SqlException.invalidDate(lo.position);
            }
        }
    }

    private static void revertNodes(ObjList<ExpressionNode> nodes) {
        for (int n = 0, k = nodes.size(); n < k; n++) {
            nodes.getQuick(n).intrinsicValue = IntrinsicModel.UNDEFINED;
        }
        nodes.clear();
    }

    private void addExcludedValue(ExpressionNode parentNode, ExpressionNode valueNode, CharSequence value) {
        if (tempKeyExcludedValues.add(value)) {
            tempKeyExcludedValuePos.add(valueNode.position);
            tempKeyExcludedValueType.add(valueNode.type);
            allKeyExcludedValuesAreKnown &= (valueNode.type != ExpressionNode.BIND_VARIABLE);
        }
        parentNode.intrinsicValue = IntrinsicModel.TRUE;
        keyExclNodes.add(parentNode);
    }

    private void addValue(ExpressionNode parentNode, ExpressionNode valueNode, CharSequence value) {
        if (tempKeyValues.add(value)) {
            tempKeyValuePos.add(valueNode.position);
            tempKeyValueType.add(valueNode.type);
            allKeyValuesAreKnown &= (valueNode.type != ExpressionNode.BIND_VARIABLE);
        }
        parentNode.intrinsicValue = IntrinsicModel.TRUE;
        keyNodes.add(parentNode);
    }

    /**
     * Analyzes an and_offset node, which wraps a timestamp predicate that needs offset adjustment.
     * The and_offset function has the form: and_offset(predicate, unit, offset)
     * where:
     * - predicate is the inner timestamp predicate (e.g., ts in '2022')
     * - unit is the time unit character (e.g., 'h', 'd', 'm')
     * - offset is the offset value to apply (in the given unit)
     *
     * @return true if intervals were extracted and offset applied
     */
    private boolean analyzeAndOffset(
            TimestampDriver timestampDriver,
            AliasTranslator translator,
            IntrinsicModel model,
            ExpressionNode node,
            RecordMetadata m,
            FunctionParser functionParser,
            RecordMetadata metadata,
            SqlExecutionContext executionContext,
            boolean latestByMultiColumn,
            TableReader reader
    ) throws SqlException {
        // and_offset args are stored in reverse order: [offset, unit, predicate]
        // This matches how ExpressionNode.toSink renders function args
        ObjList<ExpressionNode> args = node.args;
        if (args.size() != 3) {
            return false;
        }

        ExpressionNode predicate = args.getQuick(2);
        ExpressionNode unitNode = args.getQuick(1);
        ExpressionNode offsetNode = args.getQuick(0);

        // Parse unit character - must be a constant single-character token
        // Valid forms: 'h' (quoted single char) or h (unquoted single char)
        if (unitNode.type != ExpressionNode.CONSTANT) {
            return false;  // Reject bind variables, columns, etc.
        }
        CharSequence unitToken = unitNode.token;
        if (unitToken == null || unitToken.isEmpty()) {
            return false;  // Reject empty tokens
        }
        char unit;
        int len = unitToken.length();
        if (len == 3 && unitToken.charAt(0) == '\'' && unitToken.charAt(2) == '\'') {
            // Quoted single char: 'h'
            unit = unitToken.charAt(1);
        } else if (len == 1) {
            // Unquoted single char: h
            unit = unitToken.charAt(0);
        } else {
            // Reject multi-char tokens like 'ms', ':v', etc.
            return false;
        }

        // Parse offset value
        int offsetValue;
        try {
            offsetValue = Numbers.parseInt(offsetNode.token);
        } catch (NumericException e) {
            return false;
        }

        // Get the add method for this unit from the timestamp driver
        TimestampDriver.TimestampAddMethod addMethod = timestampDriver.getAddMethod(unit);
        if (addMethod == null) {
            return false;  // Unknown unit
        }

        // Create a temporary model to extract intervals from the inner predicate
        IntrinsicModel tempModel = models.next();
        int timestampType = reader.getMetadata().getTimestampType();
        tempModel.of(timestampType, reader.getPartitionedBy(), executionContext.getCairoEngine().getConfiguration());

        // Process the inner predicate recursively
        boolean extracted = removeAndIntrinsics(
                timestampDriver,
                translator,
                tempModel,
                predicate,
                m,
                functionParser,
                metadata,
                executionContext,
                latestByMultiColumn,
                reader
        );

        if (extracted || tempModel.hasIntervalFilters()) {
            // Merge directly from the temp model without allocating an intermediate RuntimeIntervalModel.
            // This applies the offset to each interval boundary using the timestamp driver's add method,
            // which correctly handles variable-length units like months and years.
            model.mergeIntervalModelWithAddMethod(tempModel, addMethod, offsetValue);
            node.intrinsicValue = IntrinsicModel.TRUE;
            return true;
        }

        return false;
    }

    private boolean analyzeBetween(
            TimestampDriver timestampDriver,
            AliasTranslator translator,
            IntrinsicModel model,
            ExpressionNode node,
            RecordMetadata m,
            FunctionParser functionParser,
            RecordMetadata metadata,
            SqlExecutionContext executionContext
    ) throws SqlException {
        ExpressionNode col = node.args.getLast();
        if (col.type != ExpressionNode.LITERAL) {
            return false;
        }
        CharSequence column = translator.translateAlias(col.token);
        if (m.getColumnIndexQuiet(column) == -1) {
            throw SqlException.invalidColumn(col.position, col.token);
        }
        return analyzeBetween0(timestampDriver, model, col, node, false, functionParser, metadata, executionContext);
    }

    private boolean analyzeBetween0(
            TimestampDriver timestampDriver,
            IntrinsicModel model,
            ExpressionNode col,
            ExpressionNode between,
            boolean isNegated,
            FunctionParser functionParser,
            RecordMetadata metadata,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (!isTimestamp(col)) {
            return false;
        }

        ExpressionNode lo = between.args.getQuick(1);
        ExpressionNode hi = between.args.getQuick(0);

        try {
            model.setBetweenNegated(isNegated);
            boolean isBetweenTranslated = translateBetweenToTimestampModel(timestampDriver, model, functionParser, metadata, executionContext, lo);
            if (isBetweenTranslated) {
                isBetweenTranslated = translateBetweenToTimestampModel(timestampDriver, model, functionParser, metadata, executionContext, hi);
            }

            if (isBetweenTranslated) {
                between.intrinsicValue = IntrinsicModel.TRUE;
                return true;
            }
        } finally {
            model.clearBetweenTempParsing();
        }

        return false;
    }

    private boolean analyzeEquals(
            TimestampDriver timestampDriver,
            AliasTranslator translator,
            IntrinsicModel model,
            ExpressionNode node,
            RecordMetadata m,
            FunctionParser functionParser,
            SqlExecutionContext executionContext,
            boolean latestByMultiColumn,
            TableReader reader
    ) throws SqlException {
        checkNodeValid(node);
        return analyzeEquals0( // left == right
                timestampDriver,
                translator,
                model,
                node,
                node.lhs,
                node.rhs,
                m,
                functionParser,
                executionContext,
                latestByMultiColumn,
                reader
        )
                ||
                analyzeEquals0( // right == left
                        timestampDriver,
                        translator,
                        model,
                        node,
                        node.rhs,
                        node.lhs,
                        m,
                        functionParser,
                        executionContext,
                        latestByMultiColumn,
                        reader
                );
    }

    private boolean analyzeEquals0(
            TimestampDriver timestampDriver,
            AliasTranslator translator,
            IntrinsicModel model,
            ExpressionNode node,
            ExpressionNode a,
            ExpressionNode b,
            RecordMetadata m,
            FunctionParser functionParser,
            SqlExecutionContext executionContext,
            boolean latestByMultiColumn,
            TableReader reader
    ) throws SqlException {
        if (nodesEqual(a, b)) {
            node.intrinsicValue = IntrinsicModel.TRUE;
            return true;
        }

        if (a.type == ExpressionNode.LITERAL && (b.type == ExpressionNode.CONSTANT || isFunc(b))) {
            if (isTimestamp(a)) {
                if (b.type == ExpressionNode.CONSTANT) {
                    if (isNullKeyword(b.token)) {
                        node.intrinsicValue = IntrinsicModel.FALSE;
                        return false;
                    }
                    model.intersectTimestamp(b.token, 1, b.token.length() - 1, b.position);
                    node.intrinsicValue = IntrinsicModel.TRUE;
                    return true;
                }
                final Function func = functionParser.parseFunction(b, m, executionContext);
                try {
                    checkFunctionCanBeTimestamp(m, executionContext, func, b.position);
                    return analyzeTimestampEqualsFunction(timestampDriver, model, node, func, b.position);
                } catch (Throwable th) {
                    Misc.free(func);
                    throw th;
                }
            } else {
                CharSequence columnName = translator.translateAlias(a.token);
                int index = m.getColumnIndexQuiet(columnName);
                if (index == -1) {
                    throw SqlException.invalidColumn(a.position, a.token);
                }

                switch (ColumnType.tagOf(m.getColumnType(index))) {
                    case ColumnType.VARCHAR:
                    case ColumnType.SYMBOL:
                    case ColumnType.STRING:
                    case ColumnType.LONG:
                    case ColumnType.INT:
                        if (columnIsPreferredOrIndexedAndNotPartOfMultiColumnLatestBy(columnName, m, latestByMultiColumn)) {
                            CharSequence value = isNullKeyword(b.token) ? null : unquote(b.token);
                            if (Chars.equalsIgnoreCaseNc(columnName, model.keyColumn)) {
                                if (!isCorrectType(b.type)) {
                                    node.intrinsicValue = IntrinsicModel.FALSE;
                                    return false;
                                }
                                // IN sets can't be merged if either contains a bind variable (even if it's the same),
                                // so we've to push new set to filter
                                if (!allKeyValuesAreKnown || (b.type == ExpressionNode.BIND_VARIABLE && tempKeyValues.size() > 0)) {
                                    node.intrinsicValue = IntrinsicModel.FALSE;
                                    return false;
                                }
                                if (b.type == ExpressionNode.FUNCTION) {
                                    CharSequence testValue = getStrFromFunction(functionParser, b, m, executionContext);
                                    if (!isConstFunction) {
                                        node.intrinsicValue = IntrinsicModel.FALSE;
                                        return false;
                                    }
                                    value = testValue;
                                }

                                // we can refer to the accumulated key values, compute overlap of values
                                // if values do overlap, keep only our value otherwise invalidate entire model
                                if (tempKeyValues.contains(value)) {
                                    // x in ('a,'b') and x = 'a' then x = 'b' can't happen
                                    if (tempKeyValues.size() > 1) {
                                        clearKeys();
                                        addValue(node, b, value);
                                    }
                                } else if (tempKeyValues.size() > 0) {
                                    // "x in ('a','b') and x = 'c' means we have a conflicting predicates
                                    clearKeys();
                                    node.intrinsicValue = IntrinsicModel.TRUE;
                                    model.intrinsicValue = IntrinsicModel.FALSE;
                                    return false;
                                }

                                // x not in ('a', 'b') and x = 'a'  or
                                // x not in ($1, 'a') and x = $1  means we have conflicting predicates
                                if (tempKeyExcludedValues.contains(value)) {
                                    if (value != null) {
                                        int idx = tempKeyExcludedValues.getListIndexOf(value);
                                        if (!isTypeMismatch(tempKeyExcludedValueType.get(idx), b.type)) {
                                            // clear all excluded values because conflict was detected
                                            clearExcludedKeys();
                                            node.intrinsicValue = IntrinsicModel.TRUE;
                                            model.intrinsicValue = IntrinsicModel.FALSE;
                                            return false;
                                        }
                                    }
                                }

                                // no conflicts detected, so just add the value
                                addValue(node, b, value);
                                return true;
                            } else if (model.keyColumn == null || isMoreSelective(model, m, reader, index)) {
                                if (!isCorrectType(b.type)) {
                                    b.intrinsicValue = IntrinsicModel.FALSE;
                                    return false;
                                }
                                if (b.type == ExpressionNode.FUNCTION) {
                                    CharSequence testValue = getStrFromFunction(functionParser, b, m, executionContext);
                                    if (!isConstFunction) {
                                        node.intrinsicValue = IntrinsicModel.FALSE;
                                        return false;
                                    }
                                    value = testValue;
                                }

                                model.keyColumn = columnName;
                                clearKeys();
                                clearExcludedKeys();
                                resetNodes();
                                addValue(node, b, value);
                                return true;
                            }
                            keyNodes.add(node);
                            return true;
                        }
                        // fall through
                    default:
                        return false;
                }
            }
        }
        // special case for ts = (<subquery>) and similar cases
        if (a.type == ExpressionNode.LITERAL && isTimestamp(a) && b.type == ExpressionNode.QUERY) {
            final Function func = functionParser.parseFunction(b, m, executionContext);
            try {
                if (checkCursorFunctionReturnsSingleTimestamp(func)) {
                    return analyzeTimestampEqualsFunction(timestampDriver, model, node, func, b.position);
                }
                Misc.free(func);
            } catch (Throwable th) {
                Misc.free(func);
                throw th;
            }
        }
        return false;
    }

    private boolean analyzeGreater(
            TimestampDriver timestampDriver,
            IntrinsicModel model,
            ExpressionNode node,
            boolean equalsTo,
            FunctionParser functionParser,
            RecordMetadata metadata,
            SqlExecutionContext executionContext
    ) throws SqlException {
        checkNodeValid(node);

        if (nodesEqual(node.lhs, node.rhs)) {
            model.intrinsicValue = equalsTo ? IntrinsicModel.TRUE : IntrinsicModel.FALSE;
            return false;
        }

        if (timestamp == null) {
            return false;
        }

        if (node.lhs.type == ExpressionNode.LITERAL && Chars.equalsIgnoreCase(node.lhs.token, timestamp)) {
            return analyzeTimestampGreater(timestampDriver, model, node, equalsTo, functionParser, metadata, executionContext, node.rhs);
        } else if (node.rhs.type == ExpressionNode.LITERAL && Chars.equalsIgnoreCase(node.rhs.token, timestamp)) {
            return analyzeTimestampLess(timestampDriver, model, node, equalsTo, functionParser, metadata, executionContext, node.lhs);
        }

        return false;
    }

    private boolean analyzeIn(
            TimestampDriver timestampDriver,
            AliasTranslator translator,
            IntrinsicModel model,
            ExpressionNode node,
            RecordMetadata metadata,
            FunctionParser functionParser,
            SqlExecutionContext executionContext,
            boolean latestByMultiColumn,
            TableReader reader
    ) throws SqlException {
        if (node.paramCount < 2) {
            throw SqlException.$(node.position, "Too few arguments for 'in'");
        }

        final ExpressionNode col = node.paramCount < 3 ? node.lhs : node.args.getLast();
        if (col.type != ExpressionNode.LITERAL) {
            return false;
        }

        final CharSequence column = translator.translateAlias(col.token);
        if (metadata.getColumnIndexQuiet(column) == -1) {
            throw SqlException.invalidColumn(col.position, col.token);
        }
        return analyzeInInterval(timestampDriver, model, col, node, false, functionParser, metadata, executionContext)
                || analyzeListOfValues(model, column, metadata, node, latestByMultiColumn, reader, functionParser, executionContext)
                || analyzeInLambda(model, column, metadata, node, latestByMultiColumn, reader);
    }

    private boolean analyzeInInterval(
            TimestampDriver timestampDriver,
            IntrinsicModel model,
            ExpressionNode col,
            ExpressionNode in,
            boolean isNegated,
            FunctionParser functionParser,
            RecordMetadata metadata,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (!isTimestamp(col)) {
            return false;
        }
        int oldIntervalFuncType = executionContext.getIntervalFunctionType();
        try {
            executionContext.setIntervalFunctionType(IntervalUtils.getIntervalType(timestampDriver.getTimestampType()));
            if (in.paramCount == 2) {
                ExpressionNode inArg = in.rhs;
                if (inArg.type == ExpressionNode.CONSTANT) {
                    // Single value ts in '2010-01-01' - treat string literal as an interval, not single Timestamp point
                    if (isNullKeyword(inArg.token)) {
                        if (!isNegated) {
                            model.intersectIntervals(Numbers.LONG_NULL, Numbers.LONG_NULL);
                        } else {
                            model.subtractIntervals(Numbers.LONG_NULL, Numbers.LONG_NULL);
                        }
                    } else {
                        if (!isNegated) {
                            model.intersectIntervals(inArg.token, 1, inArg.token.length() - 1, inArg.position);
                        } else {
                            model.subtractIntervals(inArg.token, 1, inArg.token.length() - 1, inArg.position);
                        }
                    }
                    in.intrinsicValue = IntrinsicModel.TRUE;
                    return true;
                } else if (isFunc(inArg)) {
                    // Single value ts in $1 - treat string literal as an interval, not single Timestamp point
                    final Function func = functionParser.parseFunction(inArg, metadata, executionContext);
                    try {
                        if (checkFunctionCanBeStrInterval(executionContext, func)) {
                            if (func.isConstant()) {
                                CharSequence funcVal = func.getStrA(null);
                                if (!isNegated) {
                                    model.intersectIntervals(funcVal, 0, funcVal.length(), inArg.position);
                                } else {
                                    model.subtractIntervals(funcVal, 0, funcVal.length(), inArg.position);
                                }
                                Misc.free(func);
                            } else if (func.isRuntimeConstant()) {
                                if (!isNegated) {
                                    model.intersectRuntimeIntervals(func);
                                } else {
                                    model.subtractRuntimeIntervals(func);
                                }
                            } else {
                                Misc.free(func);
                                return false;
                            }
                            in.intrinsicValue = IntrinsicModel.TRUE;
                            return true;
                        } else if (checkFunctionCanBeInterval(func)) {
                            if (func.isConstant()) {
                                Interval interval = timestampDriver.fixInterval(func.getInterval(null), func.getType());
                                if (!isNegated) {
                                    model.intersectIntervals(interval.getLo(), interval.getHi());
                                } else {
                                    model.subtractIntervals(interval.getLo(), interval.getHi());
                                }
                                in.intrinsicValue = IntrinsicModel.TRUE;
                                Misc.free(func);
                                return true;
                            } else if (func.isRuntimeConstant()) {
                                if (!isNegated) {
                                    model.intersectRuntimeIntervals(func);
                                } else {
                                    model.subtractRuntimeIntervals(func);
                                }
                                in.intrinsicValue = IntrinsicModel.TRUE;
                                return true;
                            }
                            Misc.free(func);
                            return false;
                        } else {
                            checkFunctionCanBeTimestamp(metadata, executionContext, func, inArg.position);
                            // This is IN (TIMESTAMP) one value which is timestamp and not a STRING
                            // This is same as equals
                            return analyzeTimestampEqualsFunction(timestampDriver, model, in, func, inArg.position);
                        }
                    } catch (Throwable th) {
                        Misc.free(func);
                        throw th;
                    }
                }
            } else {
                // Multiple values treat as multiple Timestamp points
                // Only possible to translate if it's the only timestamp restriction atm
                // NOT IN can be translated in any case as series of subtractions
                if (!model.hasIntervalFilters() || isNegated) {
                    int n = in.args.size() - 1;
                    tmpFunctions.clear();
                    try {
                        for (int i = 0; i < n; i++) {
                            ExpressionNode inListItem = in.args.getQuick(i);
                            if (inListItem.type != ExpressionNode.CONSTANT) {
                                if (inListItem.type != ExpressionNode.FUNCTION) {
                                    return false;
                                }
                                final Function func = functionParser.parseFunction(inListItem, metadata, executionContext);
                                tmpFunctions.add(func);
                                if (!func.isConstant() || !(checkFunctionCanBeStrInterval(executionContext, func) || checkFunctionCanBeTimestamp(metadata, executionContext, func))) {
                                    return false;
                                }
                            }
                        }

                        int nonConstCountFuncIndex = 0;

                        for (int i = 0; i < n; i++) {
                            ExpressionNode inListItem = in.args.getQuick(i);
                            long ts;
                            if (inListItem.type == ExpressionNode.CONSTANT) {
                                ts = parseTokenAsTimestamp(timestampDriver, inListItem);
                            } else {
                                final Function func = tmpFunctions.getQuick(nonConstCountFuncIndex++);
                                ts = getTimestampFromConstFunction(timestampDriver, func, inListItem.position, false);
                            }
                            if (!isNegated) {
                                if (i == 0) {
                                    model.intersectIntervals(ts, ts);
                                } else {
                                    model.unionIntervals(ts, ts);
                                }
                            } else {
                                model.subtractIntervals(ts, ts);
                            }
                        }
                    } finally {
                        Misc.freeObjListAndClear(tmpFunctions);
                    }

                    in.intrinsicValue = IntrinsicModel.TRUE;
                    return true;
                }
            }
        } finally {
            executionContext.setIntervalFunctionType(oldIntervalFuncType);
        }

        return false;
    }

    private boolean analyzeInLambda(
            IntrinsicModel model,
            CharSequence columnName,
            RecordMetadata m,
            ExpressionNode node,
            boolean latestByMultiColumn,
            TableReader reader
    ) throws SqlException {
        int columnIndex = m.getColumnIndex(columnName);
        if (columnIsPreferredOrIndexedAndNotPartOfMultiColumnLatestBy(columnName, m, latestByMultiColumn)) {
            if (preferredKeyColumn != null && !Chars.equalsIgnoreCase(columnName, preferredKeyColumn)) {
                return false;
            }

            if (node.rhs == null || node.rhs.type != ExpressionNode.QUERY) {
                return false;
            }

            // check if we already have indexed column, and it is of worse selectivity
            if (model.keyColumn != null
                    && (!Chars.equalsIgnoreCase(model.keyColumn, columnName))
                    && !isMoreSelective(model, m, reader, columnIndex)) {
                return false;
            }

            if ((Chars.equalsIgnoreCaseNc(columnName, model.keyColumn) && model.keySubQuery != null) || node.paramCount > 2) {
                throw SqlException.$(node.position, "Multiple lambda expressions not supported");
            }

            clearKeys();
            tempKeyValuePos.add(node.position);
            tempKeyValueType.add(node.type);
            model.keySubQuery = node.rhs.queryModel;

            // revert previously processed nodes
            revertNodes(keyNodes);
            model.keyColumn = columnName;
            keyNodes.add(node);
            node.intrinsicValue = IntrinsicModel.TRUE;
            return true;
        }
        return false;
    }

    private boolean analyzeLess(
            TimestampDriver timestampDriver,
            IntrinsicModel model,
            ExpressionNode node,
            boolean equalsTo,
            FunctionParser functionParser,
            RecordMetadata metadata,
            SqlExecutionContext executionContext
    ) throws SqlException {

        checkNodeValid(node);

        if (nodesEqual(node.lhs, node.rhs)) {
            model.intrinsicValue = equalsTo ? IntrinsicModel.TRUE : IntrinsicModel.FALSE;
            return false;
        }

        if (timestamp == null) {
            return false;
        }

        if (node.lhs.type == ExpressionNode.LITERAL && Chars.equalsIgnoreCase(node.lhs.token, timestamp)) {
            return analyzeTimestampLess(timestampDriver, model, node, equalsTo, functionParser, metadata, executionContext, node.rhs);
        } else if (node.rhs.type == ExpressionNode.LITERAL && Chars.equalsIgnoreCase(node.rhs.token, timestamp)) {
            return analyzeTimestampGreater(timestampDriver, model, node, equalsTo, functionParser, metadata, executionContext, node.lhs);
        }

        return false;
    }

    // checks and merges given in list with temp keys
    // NOTE: sets containing bind variables can't be merged
    private boolean analyzeListOfValues(
            IntrinsicModel model,
            CharSequence columnName,
            RecordMetadata meta,
            ExpressionNode node,
            boolean latestByMultiColumn,
            TableReader reader,
            FunctionParser functionParser,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final int columnIndex = meta.getColumnIndex(columnName);
        boolean newColumn = true;

        // Note: "preferred" is an unfortunate name, the actual meaning is a "column from a 'LATEST ON' clause".
        // Moreover, it is only populated when "latest on" has a single column.
        // Q: Why are we checking if we have multi-column latest by here?
        // A: When using multi-column LATEST BY, we cannot use index-based scans because the indexed column
        //    alone doesn't provide enough information to determine the "latest" record. The "latest" determination
        //    requires all columns in the LATEST BY clause, so we must disable index usage in such cases.
        if (columnIsPreferredOrIndexedAndNotPartOfMultiColumnLatestBy(columnName, meta, latestByMultiColumn)) {
            // check if we already have indexed column, and it is of worse selectivity
            if (model.keyColumn != null
                    && (newColumn = !Chars.equalsIgnoreCase(model.keyColumn, columnName))
                    && !isMoreSelective(model, meta, reader, columnIndex)) {
                return false;
            }

            // if key values contain bind variable then we can't merge it with any other set and have to push this list to filter,
            // we can only create a new list of keys (=newColumn)
            if (!allKeyValuesAreKnown && !newColumn) {
                return false;
            }

            int i = node.paramCount - 1;
            tempKeys.clear();
            tempPos.clear();
            tempType.clear();
            boolean tmpAllKeyValuesAreKnown = true;

            if (i == 1) {
                if (node.rhs == null ||
                        (node.rhs.type != ExpressionNode.CONSTANT && node.rhs.type != ExpressionNode.BIND_VARIABLE && node.rhs.type != ExpressionNode.FUNCTION) ||
                        (!newColumn && node.rhs.type == ExpressionNode.BIND_VARIABLE && tempKeyValues.size() > 0)) {
                    return false;
                }

                CharSequence value;

                if (node.rhs.type == ExpressionNode.FUNCTION) {
                    CharSequence testValue = getStrFromFunction(functionParser, node.rhs, meta, executionContext);
                    if (!isConstFunction) {
                        node.intrinsicValue = IntrinsicModel.FALSE;
                        return false;
                    }
                    value = testValue;
                } else if (isNullKeyword(node.rhs.token)) {
                    value = null;
                } else {
                    value = unquote(node.rhs.token);
                }

                if (tempKeys.add(value)) {
                    tempPos.add(node.position);
                    tempType.add(node.rhs.type);
                    tmpAllKeyValuesAreKnown = (node.rhs.type != ExpressionNode.BIND_VARIABLE);
                }
            } else {
                for (i--; i > -1; i--) {
                    ExpressionNode c = node.args.getQuick(i);
                    // Reject if the value is not a constant/bind variable/function.
                    // Also reject bind variables when merging with existing keys (!newColumn && tempKeyValues.size() > 0),
                    // since we can't merge bind variables with already collected key values.
                    if ((c.type != ExpressionNode.CONSTANT && c.type != ExpressionNode.BIND_VARIABLE && c.type != ExpressionNode.FUNCTION) ||
                            (!newColumn && c.type == ExpressionNode.BIND_VARIABLE && tempKeyValues.size() > 0)) {
                        return false;
                    }

                    if (isNullKeyword(c.token)) {
                        if (tempKeys.add((CharSequence) null)) {
                            tempPos.add(c.position);
                            tempType.add(c.type);
                        }
                    } else {
                        CharSequence value;
                        if (c.type == ExpressionNode.FUNCTION) {
                            CharSequence testValue = getStrFromFunction(functionParser, c, meta, executionContext);
                            if (!isConstFunction) {
                                node.intrinsicValue = IntrinsicModel.FALSE;
                                return false;
                            }
                            value = testValue;
                        } else {
                            value = unquote(c.token);
                        }

                        if (tempKeys.add(value)) {
                            tempPos.add(c.position);
                            tempType.add(c.type);
                            tmpAllKeyValuesAreKnown &= (c.type != ExpressionNode.BIND_VARIABLE);
                        }
                    }
                }
            }

            // clear values if this is new column and reset intrinsic values on nodes associated with old column
            if (newColumn) {
                clearAllKeys();
                tempKeyValues.addAll(tempKeys);
                tempKeyValuePos.addAll(tempPos);
                tempKeyValueType.addAll(tempType);
                allKeyValuesAreKnown = tmpAllKeyValuesAreKnown;
                model.keyColumn = columnName;
                keyNodes.add(node);
                node.intrinsicValue = IntrinsicModel.TRUE;
                return true;
            }

            if (tempKeyValues.size() == 0) {
                tempKeyValues.addAll(tempKeys);
                tempKeyValuePos.addAll(tempPos);
                tempKeyValueType.addAll(tempType);
            } else if (!tmpAllKeyValuesAreKnown) {
                node.intrinsicValue = IntrinsicModel.FALSE;
                return false;
            }

            allKeyValuesAreKnown &= tmpAllKeyValuesAreKnown;

            if (model.keySubQuery == null) {
                // calculate overlap of values
                mergeKeys(model, true);
                //we can't merge sets with bind vars so push excluded keys to filter
                if (tempKeyExcludedValues.size() > 0 && (!allKeyValuesAreKnown || !allKeyExcludedValuesAreKnown)) {
                    clearExcludedKeys();
                    resetExcludedNodes();
                }

                keyNodes.add(node);
                node.intrinsicValue = IntrinsicModel.TRUE;
                return true;
            }
        }
        return false;
    }

    private boolean analyzeNotBetween(
            TimestampDriver timestampDriver,
            AliasTranslator translator,
            IntrinsicModel model,
            ExpressionNode notNode,
            RecordMetadata m,
            FunctionParser functionParser,
            RecordMetadata metadata,
            SqlExecutionContext executionContext,
            boolean latestByMultiColumn,
            TableReader reader
    ) throws SqlException {

        ExpressionNode node = notNode.rhs;
        ExpressionNode col = node.args.getLast();
        if (col.type != ExpressionNode.LITERAL) {
            return false;
        }
        CharSequence column = translator.translateAlias(col.token);
        if (m.getColumnIndexQuiet(column) == -1) {
            throw SqlException.invalidColumn(col.position, col.token);
        }

        boolean ok = analyzeBetween0(timestampDriver, model, col, node, true, functionParser, metadata, executionContext);
        if (ok) {
            notNode.intrinsicValue = IntrinsicModel.TRUE;
        } else {
            analyzeNotListOfValues(model, column, m, node, notNode, latestByMultiColumn, reader, functionParser, executionContext);
        }

        return ok;
    }

    private boolean analyzeNotEquals(
            TimestampDriver timestampDriver,
            AliasTranslator translator,
            IntrinsicModel model,
            ExpressionNode node,
            RecordMetadata m,
            FunctionParser functionParser,
            SqlExecutionContext executionContext,
            boolean canUseIndex,
            TableReader reader
    ) throws SqlException {
        checkNodeValid(node);
        return analyzeNotEquals0(timestampDriver, translator, model, node, node.lhs, node.rhs, m, functionParser, executionContext, canUseIndex, reader)
                || analyzeNotEquals0(timestampDriver, translator, model, node, node.rhs, node.lhs, m, functionParser, executionContext, canUseIndex, reader);
    }

    private boolean analyzeNotEquals0(
            TimestampDriver timestampDriver,
            AliasTranslator translator,
            IntrinsicModel model,
            ExpressionNode node,
            ExpressionNode a,
            ExpressionNode b,
            RecordMetadata m,
            FunctionParser functionParser,
            SqlExecutionContext executionContext,
            boolean latestByMultiColumn,
            TableReader reader
    ) throws SqlException {
        if (nodesEqual(a, b) && a.noLeafs() && b.noLeafs()) {
            model.intrinsicValue = IntrinsicModel.FALSE;
            return true;
        }

        if (a.type == ExpressionNode.LITERAL && (b.type == ExpressionNode.CONSTANT || isFunc(b))) {
            if (isTimestamp(a)) {
                if (b.type == ExpressionNode.CONSTANT) {
                    if (isNullKeyword(b.token)) {
                        node.intrinsicValue = IntrinsicModel.FALSE;
                        return false;
                    }
                    model.subtractIntervals(b.token, 1, b.token.length() - 1, b.position);
                    node.intrinsicValue = IntrinsicModel.TRUE;
                    return true;
                }
                Function func = functionParser.parseFunction(b, m, executionContext);
                try {
                    checkFunctionCanBeTimestamp(m, executionContext, func, b.position);
                    return analyzeTimestampNotEqualsFunction(timestampDriver, model, node, func, b.position);
                } catch (Throwable th) {
                    Misc.free(func);
                    throw th;
                }
            } else {
                CharSequence columnName = translator.translateAlias(a.token);
                int index = m.getColumnIndexQuiet(columnName);
                if (index == -1) {
                    throw SqlException.invalidColumn(a.position, a.token);
                }

                switch (ColumnType.tagOf(m.getColumnType(index))) {
                    case ColumnType.VARCHAR:
                    case ColumnType.SYMBOL:
                    case ColumnType.STRING:
                    case ColumnType.LONG:
                    case ColumnType.INT:
                        if (columnIsPreferredOrIndexedAndNotPartOfMultiColumnLatestBy(columnName, m, latestByMultiColumn)) {
                            CharSequence value = isNullKeyword(b.token) ? null : unquote(b.token);
                            if (Chars.equalsIgnoreCaseNc(columnName, model.keyColumn)) {
                                if (!isCorrectType(b.type)) {
                                    node.intrinsicValue = IntrinsicModel.FALSE;
                                    return false;
                                }
                                if (b.type == ExpressionNode.FUNCTION) {
                                    CharSequence testValue = getStrFromFunction(functionParser, b, m, executionContext);
                                    if (!isConstFunction) {
                                        node.intrinsicValue = IntrinsicModel.FALSE;
                                        return false;
                                    }
                                    value = testValue;
                                }

                                if (tempKeyExcludedValues.contains(value)) {
                                    // x not in ('a,'b') and x != 'a' means x not in ('a,'b')
                                    if (value != null) {
                                        int idx = tempKeyExcludedValues.getListIndexOf(value);
                                        // don't mix bind var with literal, push new node to filter
                                        if (isTypeMismatch(tempKeyExcludedValueType.get(idx), b.type)) {
                                            node.intrinsicValue = IntrinsicModel.FALSE;
                                            return false;
                                        }
                                    }
                                    node.intrinsicValue = IntrinsicModel.TRUE;
                                    keyExclNodes.add(node);
                                } else if (tempKeyValues.contains(value)
                                        && (allKeyValuesAreKnown && b.type != ExpressionNode.BIND_VARIABLE)) {
                                    // sets can't be merged if either contains a bind variable
                                    int listIdx;
                                    if (value == null) {
                                        listIdx = tempKeyValues.removeNull();
                                    } else {
                                        int hashIdx = tempKeyValues.keyIndex(value);
                                        listIdx = tempKeyValues.getListIndexAt(hashIdx);
                                        tempKeyValues.removeAt(hashIdx);
                                    }
                                    tempKeyValuePos.removeIndex(listIdx);
                                    tempKeyValueType.removeIndex(listIdx);
                                    removeNodes(b, keyNodes);
                                    node.intrinsicValue = IntrinsicModel.TRUE;

                                    // in set is empty
                                    if (tempKeyValues.size() == 0) {
                                        model.intrinsicValue = IntrinsicModel.FALSE;
                                    }
                                    keyExclNodes.add(node);
                                } else {
                                    addExcludedValue(node, b, value);
                                }
                            } else if (model.keyColumn == null || isMoreSelective(model, m, reader, index)) {
                                if (!isCorrectType(b.type)) {
                                    node.intrinsicValue = IntrinsicModel.FALSE;
                                    return false;
                                }
                                if (b.type == ExpressionNode.FUNCTION) {
                                    CharSequence testValue = getStrFromFunction(functionParser, b, m, executionContext);
                                    if (!isConstFunction) {
                                        node.intrinsicValue = IntrinsicModel.FALSE;
                                        return false;
                                    }

                                    value = testValue;
                                }

                                model.keyColumn = columnName;
                                clearKeys();
                                clearExcludedKeys();
                                resetNodes();
                                addExcludedValue(node, b, value);
                                return true;
                            }
                            return true;
                        } else if (Chars.equalsIgnoreCaseNc(columnName, preferredKeyColumn)) {
                            keyExclNodes.add(node);
                            return false;
                        }
                        // fall through
                    default:
                        return false;
                }
            }
        }
        return false;
    }

    private boolean analyzeNotIn(
            TimestampDriver timestampDriver,
            AliasTranslator translator,
            IntrinsicModel model,
            ExpressionNode notNode,
            RecordMetadata m,
            FunctionParser functionParser,
            RecordMetadata metadata,
            SqlExecutionContext executionContext,
            boolean latestByMultiColumn,
            TableReader reader
    ) throws SqlException {
        ExpressionNode node = notNode.rhs;

        if (node.paramCount < 2) {
            throw SqlException.$(node.position, "Too few arguments for 'in'");
        }

        ExpressionNode col = node.paramCount < 3 ? node.lhs : node.args.getLast();

        if (col.type != ExpressionNode.LITERAL) {
            return false;
        }

        CharSequence column = translator.translateAlias(col.token);

        if (m.getColumnIndexQuiet(column) == -1) {
            throw SqlException.invalidColumn(col.position, col.token);
        }

        boolean ok = analyzeInInterval(timestampDriver, model, col, node, true, functionParser, metadata, executionContext);
        if (ok) {
            notNode.intrinsicValue = IntrinsicModel.TRUE;
        } else {
            analyzeNotListOfValues(model, column, m, node, notNode, latestByMultiColumn, reader, functionParser, executionContext);
        }

        return ok;
    }

    private void analyzeNotListOfValues(
            IntrinsicModel model,
            CharSequence columnName,
            RecordMetadata m,
            ExpressionNode node,
            ExpressionNode notNode,
            boolean latestByMultiColumn,
            TableReader reader,
            FunctionParser functionParser,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final int columnIndex = m.getColumnIndex(columnName);
        boolean newColumn = true;
        if (columnIsPreferredOrIndexedAndNotPartOfMultiColumnLatestBy(columnName, m, latestByMultiColumn)) {
            if (model.keyColumn != null
                    && (newColumn = !Chars.equalsIgnoreCase(model.keyColumn, columnName))
                    && !isMoreSelective(model, m, reader, columnIndex)) {
                return;
            }

            int i = node.paramCount - 1;
            tempKeys.clear();
            tempPos.clear();
            tempType.clear();
            boolean tmpAllKeyExcludedValuesAreKnown = true;

            // collect and analyze values of indexed field
            // if any of values is not an indexed constant - bail out
            if (i == 1) {
                if (node.rhs == null ||
                        (node.rhs.type != ExpressionNode.CONSTANT &&
                                node.rhs.type != ExpressionNode.FUNCTION &&
                                node.rhs.type != ExpressionNode.BIND_VARIABLE)) {
                    return;
                }
                CharSequence value;

                if (node.rhs.type == ExpressionNode.FUNCTION) {
                    CharSequence testValue = getStrFromFunction(functionParser, node.rhs, m, executionContext);
                    if (!isConstFunction) {
                        node.intrinsicValue = IntrinsicModel.FALSE;
                        return;
                    }
                    value = testValue;
                } else if (isNullKeyword(node.rhs.token)) {
                    value = null;
                } else {
                    value = unquote(node.rhs.token);
                }

                if (tempKeys.add(value)) {
                    tempPos.add(node.position);
                    tempType.add(node.rhs.type);
                    tmpAllKeyExcludedValuesAreKnown = (node.rhs.type != ExpressionNode.BIND_VARIABLE);
                }
            } else {
                for (i--; i > -1; i--) {
                    ExpressionNode c = node.args.getQuick(i);
                    if (c.type != ExpressionNode.CONSTANT && c.type != ExpressionNode.FUNCTION && c.type != ExpressionNode.BIND_VARIABLE) {
                        return;
                    }

                    if (isNullKeyword(c.token)) {
                        if (tempKeys.add((CharSequence) null)) {
                            tempPos.add(c.position);
                            tempType.add(c.type);
                        }
                    } else {
                        CharSequence value;

                        if (c.type == ExpressionNode.FUNCTION) {
                            CharSequence testValue = getStrFromFunction(functionParser, c, m, executionContext);
                            if (!isConstFunction) {
                                node.intrinsicValue = IntrinsicModel.FALSE;
                                return;
                            }
                            value = testValue;
                        } else {
                            value = unquote(c.token);
                        }

                        if (tempKeys.add(value)) {
                            tempPos.add(c.position);
                            tempType.add(c.type);
                            tmpAllKeyExcludedValuesAreKnown &= (c.type != ExpressionNode.BIND_VARIABLE);
                        }
                    }
                }
            }

            // clear values if this is new column and reset intrinsic values on nodes associated with old column
            if (newColumn) {
                clearAllKeys();

                model.keyColumn = columnName;
                keyExclNodes.add(notNode);
                notNode.intrinsicValue = IntrinsicModel.TRUE;

                tempKeyExcludedValues.addAll(tempKeys);
                tempKeyExcludedValuePos.addAll(tempPos);
                tempKeyExcludedValueType.addAll(tempType);
                allKeyExcludedValuesAreKnown = tmpAllKeyExcludedValuesAreKnown;

                return;
            }

            if (tempKeyExcludedValues.size() == 0) {
                tempKeyExcludedValues.addAll(tempKeys);
                tempKeyExcludedValuePos.addAll(tempPos);
                tempKeyExcludedValueType.addAll(tempType);
            }

            allKeyExcludedValuesAreKnown &= tmpAllKeyExcludedValuesAreKnown;

            if (model.keySubQuery == null) {
                // calculate sum of values
                if (mergeKeys(model, false)) {
                    keyExclNodes.add(notNode);
                    notNode.intrinsicValue = IntrinsicModel.TRUE;
                }
            }
        }
    }

    private boolean analyzeTimestampEqualsFunction(
            TimestampDriver timestampDriver,
            IntrinsicModel model,
            ExpressionNode node,
            Function func,
            int functionPosition
    ) throws SqlException {
        if (func.isConstant()) {
            long value = getTimestampFromConstFunction(timestampDriver, func, functionPosition, true);
            if (value == Numbers.LONG_NULL) {
                // make it empty set
                model.intersectEmpty();
            } else {
                model.intersectIntervals(value, value);
            }
            node.intrinsicValue = IntrinsicModel.TRUE;
            Misc.free(func);
            return true;
        } else if (func.isRuntimeConstant()) {
            model.intersectRuntimeTimestamp(func);
            node.intrinsicValue = IntrinsicModel.TRUE;
            return true;
        } else if (func.getType() == ColumnType.CURSOR) {
            model.intersectRuntimeTimestamp(func);
            node.intrinsicValue = IntrinsicModel.TRUE;
            return true;
        }
        Misc.free(func);
        return false;
    }

    private boolean analyzeTimestampGreater(
            TimestampDriver timestampDriver,
            IntrinsicModel model,
            ExpressionNode node,
            boolean equalsTo,
            FunctionParser functionParser,
            RecordMetadata metadata,
            SqlExecutionContext executionContext,
            ExpressionNode compareWithNode
    ) throws SqlException {
        long lo;
        if (compareWithNode.type == ExpressionNode.CONSTANT) {
            if (isNullKeyword(compareWithNode.token)) {
                node.intrinsicValue = IntrinsicModel.FALSE;
                return false;
            }
            try {
                lo = parseFullOrPartialDate(timestampDriver, equalsTo, compareWithNode, true);
            } catch (NumericException e) {
                throw SqlException.invalidDate(compareWithNode.token, compareWithNode.position);
            }
            model.intersectIntervals(lo, Long.MAX_VALUE);
            node.intrinsicValue = IntrinsicModel.TRUE;
            return true;
        } else if (isFunc(compareWithNode)) {
            final Function func = functionParser.parseFunction(compareWithNode, metadata, executionContext);
            try {
                checkFunctionCanBeTimestamp(metadata, executionContext, func, compareWithNode.position);
                if (func.isConstant()) {
                    lo = getTimestampFromConstFunction(timestampDriver, func, compareWithNode.position, false);
                    if (lo == Numbers.LONG_NULL) {
                        // make it empty set
                        model.intersectEmpty();
                    } else {
                        model.intersectIntervals(lo + adjustComparison(equalsTo, true), Long.MAX_VALUE);
                    }
                    node.intrinsicValue = IntrinsicModel.TRUE;
                    Misc.free(func);
                    return true;
                } else if (func.isRuntimeConstant()) {
                    model.intersectIntervals(func, Long.MAX_VALUE, adjustComparison(equalsTo, true));
                    node.intrinsicValue = IntrinsicModel.TRUE;
                    return true;
                }
                Misc.free(func);
            } catch (Throwable th) {
                Misc.free(func);
                throw th;
            }
        } else if (compareWithNode.type == ExpressionNode.QUERY) {
            // special case for ts = (<subquery>) and similar cases
            final Function func = functionParser.parseFunction(compareWithNode, metadata, executionContext);
            try {
                if (checkCursorFunctionReturnsSingleTimestamp(func)) {
                    model.intersectIntervals(func, Long.MAX_VALUE, adjustComparison(equalsTo, true));
                    node.intrinsicValue = IntrinsicModel.TRUE;
                    return true;
                }
                Misc.free(func);
            } catch (Throwable th) {
                Misc.free(func);
                throw th;
            }
        }
        return false;
    }

    private boolean analyzeTimestampLess(
            TimestampDriver timestampDriver,
            IntrinsicModel model,
            ExpressionNode node,
            boolean equalsTo,
            FunctionParser functionParser,
            RecordMetadata metadata,
            SqlExecutionContext executionContext,
            ExpressionNode compareWithNode
    ) throws SqlException {
        if (compareWithNode.type == ExpressionNode.CONSTANT) {
            if (isNullKeyword(compareWithNode.token)) {
                node.intrinsicValue = IntrinsicModel.FALSE;
                return false;
            }
            try {
                long hi = parseFullOrPartialDate(timestampDriver, equalsTo, compareWithNode, false);
                model.intersectIntervals(Long.MIN_VALUE, hi);
                node.intrinsicValue = IntrinsicModel.TRUE;
            } catch (NumericException e) {
                throw SqlException.invalidDate(compareWithNode.token, compareWithNode.position);
            }
            return true;
        } else if (isFunc(compareWithNode)) {
            final Function func = functionParser.parseFunction(compareWithNode, metadata, executionContext);
            try {
                checkFunctionCanBeTimestamp(metadata, executionContext, func, compareWithNode.position);
                if (func.isConstant()) {
                    long hi = getTimestampFromConstFunction(timestampDriver, func, compareWithNode.position, false);
                    if (hi == Numbers.LONG_NULL) {
                        model.intersectEmpty();
                    } else {
                        model.intersectIntervals(Long.MIN_VALUE, hi + adjustComparison(equalsTo, false));
                    }
                    node.intrinsicValue = IntrinsicModel.TRUE;
                    Misc.free(func);
                    return true;
                } else if (func.isRuntimeConstant()) {
                    model.intersectIntervals(Long.MIN_VALUE, func, adjustComparison(equalsTo, false));
                    node.intrinsicValue = IntrinsicModel.TRUE;
                    return true;
                }
                Misc.free(func);
            } catch (Throwable th) {
                Misc.free(func);
                throw th;
            }
        } else if (compareWithNode.type == ExpressionNode.QUERY) {
            // special case for ts = (<subquery>) and similar cases
            final Function func = functionParser.parseFunction(compareWithNode, metadata, executionContext);
            try {
                if (checkCursorFunctionReturnsSingleTimestamp(func)) {
                    model.intersectIntervals(Long.MIN_VALUE, func, adjustComparison(equalsTo, false));
                    node.intrinsicValue = IntrinsicModel.TRUE;
                    return true;
                }
                Misc.free(func);
            } catch (Throwable th) {
                Misc.free(func);
                throw th;
            }
        }
        return false;
    }

    private boolean analyzeTimestampNotEqualsFunction(
            TimestampDriver timestampDriver,
            IntrinsicModel model,
            ExpressionNode node,
            Function function,
            int functionPosition
    ) throws SqlException {
        if (function.isConstant()) {
            long value = getTimestampFromConstFunction(timestampDriver, function, functionPosition, true);
            model.subtractIntervals(value, value);
            node.intrinsicValue = IntrinsicModel.TRUE;
            return true;
        } else if (function.isRuntimeConstant()) {
            model.subtractEquals(function);
            node.intrinsicValue = IntrinsicModel.TRUE;
            return true;
        }
        return false;
    }

    private void applyKeyExclusions(
            AliasTranslator translator,
            FunctionParser functionParser,
            RecordMetadata metadata,
            SqlExecutionContext executionContext,
            IntrinsicModel model
    ) throws SqlException {
        if (model.keyColumn != null && tempKeyValues.size() > 0 && keyExclNodes.size() > 0) {
            if (allKeyValuesAreKnown && allKeyExcludedValuesAreKnown) {
                OUT:
                for (int i = 0, n = keyExclNodes.size(); i < n; i++) {
                    ExpressionNode parent = keyExclNodes.getQuick(i);

                    ExpressionNode node = isNotKeyword(parent.token) ? parent.rhs : parent;
                    // this could either be '=' or 'in'

                    if (node.paramCount == 2) {
                        ExpressionNode col;
                        ExpressionNode val;

                        if (node.lhs.type == ExpressionNode.LITERAL) {
                            col = node.lhs;
                            val = node.rhs;
                        } else {
                            col = node.rhs;
                            val = node.lhs;
                        }

                        final CharSequence column = translator.translateAlias(col.token);
                        if (Chars.equalsIgnoreCase(column, model.keyColumn)) {
                            excludeKeyValue(model, functionParser, metadata, executionContext, val);
                            parent.intrinsicValue = IntrinsicModel.TRUE;
                            if (model.intrinsicValue == IntrinsicModel.FALSE) {
                                break;
                            }
                        }
                    }

                    if (node.paramCount > 2) {
                        ExpressionNode col = node.args.getQuick(node.paramCount - 1);
                        final CharSequence column = translator.translateAlias(col.token);
                        if (Chars.equalsIgnoreCase(column, model.keyColumn)) {
                            for (int j = node.paramCount - 2; j > -1; j--) {
                                ExpressionNode val = node.args.getQuick(j);
                                excludeKeyValue(model, functionParser, metadata, executionContext, val);
                                if (model.intrinsicValue == IntrinsicModel.FALSE) {
                                    break OUT;
                                }
                            }
                            parent.intrinsicValue = IntrinsicModel.TRUE;
                        }
                    }
                }
            }

            // sql code generator assumes only one set has items so clear NOT IN set
            if (tempKeyValues.size() > 0 && tempKeyExcludedValues.size() > 0) {
                // if both sets are constant values it's fine but
                // if any set contains bind variables then we've to push not in set to filter
                if (!allKeyValuesAreKnown || !allKeyExcludedValuesAreKnown) {
                    resetExcludedNodes();
                }
                clearExcludedKeys();
                allKeyExcludedValuesAreKnown = true;

            }
        }
        model.keyExcludedNodes.addAll(keyExclNodes);
        keyExclNodes.clear();
    }

    private boolean checkCursorFunctionReturnsSingleTimestamp(Function function) {
        final RecordCursorFactory factory = function.getRecordCursorFactory();
        if (factory != null) {
            final RecordMetadata metadata = factory.getMetadata();
            return metadata.getColumnCount() == 1 && ColumnType.isTimestamp(metadata.getColumnType(0));
        }
        return false;
    }

    private boolean checkFunctionCanBeInterval(Function function) {
        return ColumnType.isInterval(function.getType());
    }

    private boolean checkFunctionCanBeStrInterval(SqlExecutionContext executionContext, Function function) {
        int type = function.getType();
        if (ColumnType.isUndefined(type)) {
            try {
                function.assignType(ColumnType.STRING, executionContext.getBindVariableService());
            } catch (Throwable e) {
                return false;
            }
            return true;
        }
        return ColumnType.isString(type) || ColumnType.isVarchar(type);
    }

    private void checkFunctionCanBeTimestamp(
            RecordMetadata metadata,
            SqlExecutionContext executionContext,
            Function function,
            int functionPosition
    ) throws SqlException {
        if (ColumnType.isUndefined(function.getType())) {
            int timestampType = metadata.getColumnType(metadata.getTimestampIndex());
            function.assignType(timestampType, executionContext.getBindVariableService());
        } else if (!canCastToTimestamp(function.getType())) {
            throw SqlException.invalidDate(functionPosition);
        }
    }

    private boolean checkFunctionCanBeTimestamp(
            RecordMetadata metadata,
            SqlExecutionContext executionContext,
            Function function
    ) {
        if (ColumnType.isUndefined(function.getType())) {
            try {
                int timestampType = metadata.getColumnType(metadata.getTimestampIndex());
                function.assignType(timestampType, executionContext.getBindVariableService());
            } catch (Throwable ignored) {
                return false;
            }
            return true;
        } else {
            return canCastToTimestamp(function.getType());
        }
    }

    private void clearAllKeys() {
        clearKeys();
        revertNodes(keyNodes);
        clearExcludedKeys();
        revertNodes(keyExclNodes);
    }

    private void clearExcludedKeys() {
        tempKeyExcludedValues.clear();
        tempKeyExcludedValuePos.clear();
        tempKeyExcludedValueType.clear();
        allKeyExcludedValuesAreKnown = true;
    }

    private void clearKeys() {
        tempKeyValues.clear();
        tempKeyValuePos.clear();
        tempKeyValueType.clear();
        allKeyValuesAreKnown = true;
    }

    // removes nodes extracted into special symbol/key or timestamp filters from given node
    private ExpressionNode collapseIntrinsicNodes(ExpressionNode node) {
        if (node == null || node.intrinsicValue == IntrinsicModel.TRUE) {
            return null;
        }
        node.lhs = collapseIntrinsicNodes(collapseNulls0(node.lhs));
        node.rhs = collapseIntrinsicNodes(collapseNulls0(node.rhs));
        return collapseNulls0(node);
    }

    private ExpressionNode collapseNulls0(ExpressionNode node) {
        if (node == null || node.intrinsicValue == IntrinsicModel.TRUE) {
            return null;
        }
        if (node.queryModel == null && isAndKeyword(node.token)) {
            if (node.lhs == null || node.lhs.intrinsicValue == IntrinsicModel.TRUE) {
                return node.rhs;
            }
            if (node.rhs == null || node.rhs.intrinsicValue == IntrinsicModel.TRUE) {
                return node.lhs;
            }
        }
        return node;
    }

    private ExpressionNode collapseWithin0(ExpressionNode node) {
        if (node == null || isWithinKeyword(node.token)) {
            return null;
        }
        if (node.queryModel == null && (isAndKeyword(node.token) || isOrKeyword(node.token))) {
            if (node.lhs == null || isWithinKeyword(node.lhs.token)) {
                return node.rhs;
            }
            if (node.rhs == null || isWithinKeyword(node.rhs.token)) {
                return node.lhs;
            }
        }
        return node;
    }

    private ExpressionNode collapseWithinNodes(ExpressionNode node) {
        if (node == null || isWithinKeyword(node.token)) {
            return null;
        }
        node.lhs = collapseWithinNodes(collapseWithin0(node.lhs));
        node.rhs = collapseWithinNodes(collapseWithin0(node.rhs));
        return collapseWithin0(node);
    }

    private boolean columnIsPreferredOrIndexedAndNotPartOfMultiColumnLatestBy(
            CharSequence columnName,
            RecordMetadata m,
            boolean latestByMultiColumn
    ) {
        return !latestByMultiColumn &&
                (
                        Chars.equalsIgnoreCaseNc(columnName, preferredKeyColumn)
                                || (preferredKeyColumn == null && m.isColumnIndexed(m.getColumnIndex(columnName)))
                );
    }

    /**
     * Creates a new dateadd node with negated stride: dateadd(period, -stride, value)
     *
     * @param originalDateadd the original dateadd node to get period and stride from
     * @param value           the value to wrap in the inverse dateadd
     * @return the new dateadd node, or null if creation fails
     */
    private ExpressionNode createInverseDateadd(
            ObjectPool<ExpressionNode> expressionNodePool,
            ExpressionNode originalDateadd,
            ExpressionNode value
    ) {
        final ExpressionNode periodArg = originalDateadd.args.getQuick(2);
        final ExpressionNode strideArg = originalDateadd.args.getQuick(1);

        // Extract and negate the stride (handles both constants and unary minus)
        final int stride;
        try {
            stride = extractConstantInt(strideArg);
        } catch (NumericException ne) {
            return null;
        }
        final int negatedStride = -stride;

        // Create negated stride node
        final ExpressionNode negatedStrideNode = expressionNodePool.next().of(
                ExpressionNode.CONSTANT,
                Integer.toString(negatedStride),
                0,
                strideArg.position
        );

        // Create the new dateadd function node
        final ExpressionNode newDateadd = expressionNodePool.next().of(
                ExpressionNode.FUNCTION,
                "dateadd",
                0,
                originalDateadd.position
        );
        newDateadd.paramCount = 3;
        // Args in reverse order: value (timestamp position), negated stride, period
        newDateadd.args.add(value);
        newDateadd.args.add(negatedStrideNode);
        newDateadd.args.add(periodArg);

        return newDateadd;
    }

    private Function createKeyValueBindVariable(
            FunctionParser functionParser,
            SqlExecutionContext executionContext,
            int position,
            CharSequence value,
            int expressionType
    ) throws SqlException {
        final Function func = functionParser.createBindVariable(executionContext, position, value, expressionType);
        if (func.isRuntimeConstant() && ColumnType.isUndefined(func.getType())) {
            func.assignType(ColumnType.STRING, executionContext.getBindVariableService());
        }
        func.init(null, executionContext);
        return func;
    }

    private void createKeyValueBindVariables(
            IntrinsicModel model,
            FunctionParser functionParser,
            SqlExecutionContext executionContext
    ) throws SqlException {
        for (int i = 0, n = tempKeyValues.size(); i < n; i++) {
            final Function func = createKeyValueBindVariable(
                    functionParser,
                    executionContext,
                    tempKeyValuePos.getQuick(i),
                    tempKeyValues.get(i),
                    tempKeyValueType.get(i)
            );
            model.keyValueFuncs.add(func);
        }
        clearKeys();

        for (int i = 0, n = tempKeyExcludedValues.size(); i < n; i++) {
            final Function func = createKeyValueBindVariable(
                    functionParser,
                    executionContext,
                    tempKeyExcludedValuePos.getQuick(i),
                    tempKeyExcludedValues.get(i),
                    tempKeyExcludedValueType.get(i)
            );
            model.keyExcludedValueFuncs.add(func);
        }
        model.keyExcludedNodes.addAll(keyExclNodes);
        clearExcludedKeys();
    }

    private void excludeKeyValue(IntrinsicModel model, FunctionParser functionParser, RecordMetadata m, SqlExecutionContext executionContext, ExpressionNode val) throws SqlException {
        int index;
        if (isNullKeyword(val.token)) {
            index = tempKeyValues.removeNull();
            if (index > -1) {
                tempKeyValuePos.removeIndex(index);
                tempKeyValueType.removeIndex(index);
            }
        } else if (isCorrectType(val.type)) {
            int keyIndex;

            if (val.type == ExpressionNode.FUNCTION) {
                CharSequence result = getStrFromFunction(functionParser, val, m, executionContext);
                if (!isConstFunction) {
                    return;
                }
                if (result != null) {
                    keyIndex = tempKeyValues.keyIndex(result);
                } else {
                    keyIndex = 0;
                    index = tempKeyValues.removeNull();
                    if (index > -1) {
                        tempKeyValuePos.removeIndex(index);
                        tempKeyValueType.removeIndex(index);
                    }
                }
            } else {
                keyIndex = Chars.isQuoted(val.token) ? tempKeyValues.keyIndex(val.token, 1, val.token.length() - 1) : tempKeyValues.keyIndex(val.token);
            }

            if (keyIndex < 0) {
                index = tempKeyValues.getListIndexAt(keyIndex);
                tempKeyValues.removeAt(keyIndex);
                tempKeyValuePos.removeIndex(index);
                tempKeyValueType.removeIndex(index);
            }
        }

        if (tempKeyValues.size() == 0) {
            model.intrinsicValue = IntrinsicModel.FALSE;
        }
    }

    /**
     * Extracts the integer value from a constant or unary minus of constant node.
     * Returns 0 if extraction fails.
     */
    private int extractConstantInt(ExpressionNode node) throws NumericException {
        if (node.type == ExpressionNode.CONSTANT) {
            return Numbers.parseInt(node.token);
        }
        // Handle unary minus
        if (node.type == ExpressionNode.OPERATION && Chars.equals(node.token, '-') && node.lhs == null) {
            if (node.rhs != null && node.rhs.type == ExpressionNode.CONSTANT) {
                return -Numbers.parseInt(node.rhs.token);
            }
        }
        throw NumericException.instance().put("integer constant expected");
    }

    /**
     * Extracts timestamp intervals from an OR tree and unions them into the model.
     * Assumes isOrOfTimestampIn() has already returned true for this node.
     * <p>
     * Returns true if intervals were successfully extracted and applied.
     */
    private boolean extractOrTimestampIntervals(
            TimestampDriver timestampDriver,
            IntrinsicModel model,
            ExpressionNode node,
            boolean leftFirst,
            FunctionParser functionParser,
            RecordMetadata metadata,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (isOrKeyword(node.token)) {
            // Process left side, then right side
            // The first interval uses intersect, subsequent ones use union
            if (!extractOrTimestampIntervals(timestampDriver, model, node.lhs, leftFirst, functionParser, metadata, executionContext)) {
                return false;
            }
            // After processing left side, we're no longer "first"
            if (!extractOrTimestampIntervals(timestampDriver, model, node.rhs, false, functionParser, metadata, executionContext)) {
                return false;
            }
            node.intrinsicValue = IntrinsicModel.TRUE;
            return true;
        }

        // Handle timestamp IN 'value' or timestamp IN ('v1', 'v2', ...) or timestamp IN now()
        if (isInKeyword(node.token)) {
            if (node.paramCount == 2) {
                // Single value: timestamp IN 'value' or timestamp IN now()
                ExpressionNode inArg = node.rhs;
                if (isNullKeyword(inArg.token)) {
                    if (leftFirst) {
                        model.intersectIntervals(Numbers.LONG_NULL, Numbers.LONG_NULL);
                    } else {
                        model.unionIntervals(Numbers.LONG_NULL, Numbers.LONG_NULL);
                    }
                } else if (isFunc(inArg)) {
                    // Function like now() - parse and handle as runtime timestamp
                    if (!tryAccumulateTimestampFunction(model, inArg, leftFirst, functionParser, metadata, executionContext)) {
                        return false;
                    }
                } else {
                    // Constant string - treat as interval (e.g., '2018-01-01' spans full day)
                    if (leftFirst) {
                        model.intersectIntervals(inArg.token, 1, inArg.token.length() - 1, inArg.position);
                    } else {
                        model.unionIntervals(inArg.token, 1, inArg.token.length() - 1, inArg.position);
                    }
                }
            } else {
                // Multiple values: timestamp IN ('v1', 'v2', ...) - treat as points
                int n = node.args.size() - 1;
                for (int i = 0; i < n; i++) {
                    ExpressionNode inListItem = node.args.getQuick(i);
                    boolean isFirst = leftFirst && i == 0;
                    if (isFunc(inListItem)) {
                        if (!tryAccumulateTimestampFunction(model, inListItem, isFirst, functionParser, metadata, executionContext)) {
                            return false;
                        }
                    } else {
                        long ts = parseTokenAsTimestamp(timestampDriver, inListItem);
                        if (isFirst) {
                            model.intersectIntervals(ts, ts);
                        } else {
                            model.unionIntervals(ts, ts);
                        }
                    }
                }
            }
            node.intrinsicValue = IntrinsicModel.TRUE;
            return true;
        }

        // Handle timestamp = 'value' or timestamp = now() - parse as point interval [ts, ts]
        if (Chars.equals(node.token, '=')) {
            ExpressionNode valueNode;
            if (node.lhs.type == ExpressionNode.LITERAL && isTimestamp(node.lhs)) {
                valueNode = node.rhs;
            } else {
                valueNode = node.lhs;
            }
            if (isFunc(valueNode)) {
                if (!tryAccumulateTimestampFunction(model, valueNode, leftFirst, functionParser, metadata, executionContext)) {
                    return false;
                }
            } else {
                long ts = parseTokenAsTimestamp(timestampDriver, valueNode);
                if (leftFirst) {
                    model.intersectIntervals(ts, ts);
                } else {
                    model.unionIntervals(ts, ts);
                }
            }
            node.intrinsicValue = IntrinsicModel.TRUE;
            return true;
        }

        return false;
    }

    private CharSequence getStrFromFunction(
            FunctionParser functionParser,
            ExpressionNode node,
            RecordMetadata metadata,
            SqlExecutionContext executionContext
    ) throws SqlException {
        try (Function func = functionParser.parseFunction(node, metadata, executionContext)) {
            if (!func.isConstant()) {
                isConstFunction = false;
                return null;
            }

            isConstFunction = true;
            final int funcType = func.getType();
            if (
                    funcType == ColumnType.SYMBOL
                            || funcType == ColumnType.STRING
                            || funcType == ColumnType.CHAR
                            || funcType == ColumnType.UNDEFINED
                            || funcType == ColumnType.NULL
                            || funcType == ColumnType.VARCHAR
            ) {
                return Chars.toString(func.getStrA(null));
            } else {
                throw SqlException.$(node.position, "Unexpected function type [").put(ColumnType.nameOf(funcType)).put("]");
            }
        }
    }

    /**
     * Checks if a node represents a constant integer value.
     * Handles both direct constant (e.g., "15") and unary minus of a constant (e.g. "-15").
     */
    private boolean isConstantInt(ExpressionNode node) {
        if (node.type == ExpressionNode.CONSTANT) {
            try {
                Numbers.parseInt(node.token);
                return true;
            } catch (NumericException e) {
                return false;
            }
        }
        // Handle unary minus: node is OPERATION with token "-" and rhs is a constant
        if (node.type == ExpressionNode.OPERATION && Chars.equals(node.token, '-') && node.lhs == null) {
            if (node.rhs != null && node.rhs.type == ExpressionNode.CONSTANT) {
                try {
                    Numbers.parseInt(node.rhs.token);
                    return true;
                } catch (NumericException e) {
                    return false;
                }
            }
        }
        return false;
    }

    private boolean isCorrectType(int type) {
        return type != ExpressionNode.OPERATION;
    }

    /**
     * Checks if node is dateadd(const_period, const_stride, designated_timestamp).
     *
     * @param node the expression node to check
     * @return true the dateadd node if it matches the pattern, false otherwise
     */
    private boolean isDateaddOnTimestamp(ExpressionNode node) {
        if (node == null || timestamp == null) {
            return false;
        }
        if (node.type != ExpressionNode.FUNCTION
                || !Chars.equalsLowerCaseAscii(node.token, "dateadd")
                || node.args.size() != 3) {
            return false;
        }
        // Args are in reverse order: args[0]=timestamp, args[1]=stride, args[2]=period
        final ExpressionNode timestampArg = node.args.getQuick(0);
        final ExpressionNode strideArg = node.args.getQuick(1);
        final ExpressionNode periodArg = node.args.getQuick(2);
        // Check timestamp arg is the designated timestamp column
        if (timestampArg.type != ExpressionNode.LITERAL || !isTimestamp(timestampArg)) {
            return false;
        }
        // Check period is a constant char (e.g., 'm', 'd', 'h')
        if (periodArg.type != ExpressionNode.CONSTANT) {
            return false;
        }
        // Check stride is a constant integer (either a direct constant or unary minus of constant)
        return isConstantInt(strideArg);
    }

    private boolean isGeoHashConstFunction(Function fn) {
        return (fn instanceof AbstractGeoHashFunction) && fn.isConstant();
    }

    private boolean isNull(ExpressionNode node) {
        return node == null || isNullKeyword(node.token);
    }

    /**
     * Checks if an expression node is an OR tree where all leaf predicates are
     * timestamp IN intrinsics that can be unioned together.
     * <p>
     * Supports:
     * - timestamp IN 'value'
     * - timestamp IN ('value1', 'value2', ...)
     * - timestamp = 'value'
     * - Nested OR of the above
     */
    private boolean isOrOfTimestampIn(ExpressionNode node) {
        if (node == null) {
            return false;
        }

        if (isOrKeyword(node.token)) {
            // Recursively check both sides of OR
            return isOrOfTimestampIn(node.lhs) && isOrOfTimestampIn(node.rhs);
        }

        // Check for timestamp IN 'value' or timestamp IN ('v1', 'v2', ...)
        if (isInKeyword(node.token)) {
            if (node.paramCount < 2) {
                return false;
            }
            ExpressionNode col = node.paramCount < 3 ? node.lhs : node.args.getLast();
            if (col.type != ExpressionNode.LITERAL || !isTimestamp(col)) {
                return false;
            }
            // Check that all values are constants, functions, or bind variables
            if (node.paramCount == 2) {
                // Single value: timestamp IN 'value' or timestamp IN now() or timestamp IN $1
                return node.rhs != null && (node.rhs.type == ExpressionNode.CONSTANT || isFunc(node.rhs));
            } else {
                // Multiple values: timestamp IN ('v1', 'v2', ...)
                for (int i = 0, n = node.args.size() - 1; i < n; i++) {
                    ExpressionNode arg = node.args.getQuick(i);
                    if (arg.type != ExpressionNode.CONSTANT && !isFunc(arg)) {
                        return false;
                    }
                }
                return true;
            }
        }

        // Check for timestamp = 'value' or timestamp = now() or timestamp = $1
        if (Chars.equals(node.token, '=')) {
            if (node.lhs == null || node.rhs == null) {
                return false;
            }
            // Check both orientations: timestamp = 'value' and 'value' = timestamp
            if (node.lhs.type == ExpressionNode.LITERAL && isTimestamp(node.lhs)
                    && (node.rhs.type == ExpressionNode.CONSTANT || isFunc(node.rhs))) {
                return true;
            }
            return node.rhs.type == ExpressionNode.LITERAL && isTimestamp(node.rhs)
                    && (node.lhs.type == ExpressionNode.CONSTANT || isFunc(node.lhs));
        }

        return false;
    }

    private boolean isTimestamp(ExpressionNode n) {
        return Chars.equalsIgnoreCaseNc(n.token, timestamp);
    }

    // calculates intersect of existing and new set for IN ()
    // and sum of existing and new set for NOT IN ()
    // returns false when there's a type clash between items with the same 'key', e.g. ':id' literal and :id bind variable
    private boolean mergeKeys(IntrinsicModel model, boolean includedValues) {
        CharSequenceHashSet values;
        IntList positions;
        IntList types;
        if (includedValues) {
            values = tempKeyValues;
            positions = tempKeyValuePos;
            types = tempKeyValueType;
        } else {
            values = tempKeyExcludedValues;
            positions = tempKeyExcludedValuePos;
            types = tempKeyExcludedValueType;
        }
        tempK.clear();
        tempP.clear();
        tempT.clear();

        if (includedValues) { // intersect
            for (int i = 0, k = tempKeys.size(); i < k; i++) {
                if (values.contains(tempKeys.get(i)) && tempK.add(tempKeys.get(i))) {
                    tempP.add(tempPos.get(i));
                    tempT.add(tempType.get(i));
                }
            }
        } else { // sum
            tempK.addAll(values);
            tempP.addAll(positions);
            tempT.addAll(types);

            for (int i = 0, k = tempKeys.size(); i < k; i++) {
                if (tempK.add(tempKeys.get(i))) {
                    tempP.add(tempPos.get(i));
                    tempT.add(tempType.get(i));
                } else if (tempKeys.get(i) != null) {
                    int listIdx = tempK.getListIndexOf(tempKeys.get(i));
                    if (isTypeMismatch(tempType.get(i), tempT.get(listIdx))) {
                        return false;
                    }
                }
            }
        }

        values.clear();
        positions.clear();
        types.clear();

        if (tempK.size() > 0) {
            values.addAll(tempK);
            positions.addAll(tempP);
            types.addAll(tempT);
        } else {
            model.intrinsicValue = IntrinsicModel.FALSE;
        }

        return true;
    }

    /**
     * Accumulates a timestamp function (like now()) into the OR interval model.
     * For constant functions, evaluates immediately. For runtime constants, defers to runtime.
     * Returns true on success, false if the function cannot be accumulated.
     */
    private boolean tryAccumulateTimestampFunction(
            IntrinsicModel model,
            ExpressionNode funcNode,
            boolean isFirst,
            FunctionParser functionParser,
            RecordMetadata metadata,
            SqlExecutionContext executionContext
    ) throws SqlException {
        Function func = functionParser.parseFunction(funcNode, metadata, executionContext);
        try {
            if (!ColumnType.isTimestamp(func.getType())) {
                Misc.free(func);
                return false;
            }
            if (func.isConstant()) {
                long ts = func.getTimestamp(null);
                Misc.free(func);
                if (isFirst) {
                    model.intersectIntervals(ts, ts);
                } else {
                    model.unionIntervals(ts, ts);
                }
            } else if (func.isRuntimeConstant()) {
                if (isFirst) {
                    model.intersectRuntimeTimestamp(func);
                } else {
                    model.unionRuntimeTimestamp(func);
                }
            } else {
                Misc.free(func);
                return false;
            }
            return true;
        } catch (Throwable th) {
            Misc.free(func);
            throw th;
        }
    }

    private long parseFullOrPartialDate(
            TimestampDriver timestampDriver,
            boolean equalsTo,
            ExpressionNode node,
            boolean isLo
    ) throws NumericException {
        long ts;
        try {
            // Timestamp string
            ts = timestampDriver.parseQuotedLiteral(node.token);
        } catch (NumericException e) {
            try {
                // Timestamp epoch (long)
                ts = Numbers.parseLong(node.token);
            } catch (NumericException e2) {
                // Timestamp format
                ts = timestampDriver.parseAnyFormat(node.token, 1, node.token.length() - 1);
            }
        }
        if (!equalsTo) {
            ts += isLo ? 1 : -1;
        }
        return ts;
    }

    private void processArgument(
            ExpressionNode inArg,
            RecordMetadata metadata,
            FunctionParser functionParser,
            SqlExecutionContext executionContext,
            int columnType,
            LongList prefixes
    ) throws SqlException {
        final int position = inArg.position;

        if (isNull(inArg)) {
            throw SqlException.$(position, "GeoHash value expected");
        }

        final int type;
        final long hash;

        if (isFunc(inArg)) {
            try (Function func = functionParser.parseFunction(inArg, metadata, executionContext)) {
                if (isGeoHashConstFunction(func)) {
                    type = func.getType();
                    hash = GeoHashes.getGeoLong(type, func, null);
                } else {
                    throw SqlException.$(inArg.position, "GeoHash const function expected");
                }
            }
        } else {
            final boolean isConstant = inArg.type == ExpressionNode.CONSTANT;
            final CharSequence token = inArg.token;
            final int len = token.length();

            final boolean isBitsPrefix = len > 2 && token.charAt(0) == '#' && token.charAt(1) == '#';
            final boolean isCharsPrefix = len > 1 && token.charAt(0) == '#';

            if (!(isConstant && (isBitsPrefix || isCharsPrefix))) {
                throw SqlException.$(position, "GeoHash literal expected");
            }

            try {
                if (!isBitsPrefix) {
                    final int sdd = ExpressionParser.extractGeoHashSuffix(position, token);
                    final int sddLen = Numbers.decodeLowShort(sdd);
                    final int bits = Numbers.decodeHighShort(sdd);
                    type = ColumnType.getGeoHashTypeWithBits(bits);
                    hash = GeoHashes.fromStringTruncatingNl(token, 1, len - sddLen, bits);
                } else {
                    int bits = len - 2;
                    if (bits <= ColumnType.GEOLONG_MAX_BITS) {
                        type = ColumnType.getGeoHashTypeWithBits(bits);
                        hash = GeoHashes.fromBitStringNl(token, 2);
                    } else {
                        throw SqlException.$(position, "GeoHash bits literal expected");
                    }
                }
            } catch (NumericException ignored) {
                throw SqlException.$(position, "GeoHash literal expected");
            }
        }
        try {
            GeoHashes.addNormalizedGeoPrefix(hash, type, columnType, prefixes);
        } catch (NumericException e) {
            throw SqlException.$(position, "GeoHash prefix precision mismatch");
        }
    }

    private boolean removeAndIntrinsics(
            TimestampDriver timestampDriver,
            AliasTranslator translator,
            IntrinsicModel model,
            ExpressionNode node,
            RecordMetadata m,
            FunctionParser functionParser,
            RecordMetadata metadata,
            SqlExecutionContext executionContext,
            boolean latestByMultiColumn,
            TableReader reader
    ) throws SqlException {
        return switch (intrinsicOps.get(node.token)) {
            case INTRINSIC_OP_IN ->
                    analyzeIn(timestampDriver, translator, model, node, m, functionParser, executionContext, latestByMultiColumn, reader);
            case INTRINSIC_OP_GREATER_EQ ->
                    analyzeGreater(timestampDriver, model, node, true, functionParser, metadata, executionContext);
            case INTRINSIC_OP_GREATER ->
                    analyzeGreater(timestampDriver, model, node, false, functionParser, metadata, executionContext);
            case INTRINSIC_OP_LESS_EQ ->
                    analyzeLess(timestampDriver, model, node, true, functionParser, metadata, executionContext);
            case INTRINSIC_OP_LESS ->
                    analyzeLess(timestampDriver, model, node, false, functionParser, metadata, executionContext);
            case INTRINSIC_OP_EQUAL ->
                    analyzeEquals(timestampDriver, translator, model, node, m, functionParser, executionContext, latestByMultiColumn, reader);
            case INTRINSIC_OP_NOT_EQ ->
                    analyzeNotEquals(timestampDriver, translator, model, node, m, functionParser, executionContext, latestByMultiColumn, reader);
            case INTRINSIC_OP_NOT -> (
                    isInKeyword(node.rhs.token) && analyzeNotIn(
                            timestampDriver,
                            translator,
                            model,
                            node,
                            m,
                            functionParser,
                            metadata,
                            executionContext,
                            latestByMultiColumn,
                            reader
                    ))
                    || (
                    isBetweenKeyword(node.rhs.token) && analyzeNotBetween(
                            timestampDriver,
                            translator,
                            model,
                            node,
                            m,
                            functionParser,
                            metadata,
                            executionContext,
                            latestByMultiColumn,
                            reader
                    )
            );
            case INTRINSIC_OP_BETWEEN ->
                    analyzeBetween(timestampDriver, translator, model, node, m, functionParser, metadata, executionContext);
            case INTRINSIC_OP_AND_OFFSET ->
                    analyzeAndOffset(timestampDriver, translator, model, node, m, functionParser, metadata, executionContext, latestByMultiColumn, reader);
            default -> false;
        };
    }

    private void removeNodes(ExpressionNode b, ObjList<ExpressionNode> nodes) {
        tempNodes.clear();
        for (int i = 0, size = nodes.size(); i < size; i++) {
            ExpressionNode node = nodes.get(i);
            if ((node.lhs != null && Chars.equals(node.lhs.token, b.token))
                    || (node.rhs != null && Chars.equals(node.rhs.token, b.token))) {
                node.intrinsicValue = IntrinsicModel.TRUE;
                tempNodes.add(node);
            }
        }
        for (int i = 0, size = tempNodes.size(); i < size; i++) {
            nodes.remove(tempNodes.get(i));
        }
    }

    private boolean removeWithin(
            AliasTranslator translator,
            ExpressionNode node,
            RecordMetadata metadata,
            FunctionParser functionParser,
            SqlExecutionContext executionContext,
            LongList prefixes) throws SqlException {

        if (isWithinKeyword(node.token)) {

            if (prefixes.size() > 0) {
                throw SqlException.$(node.position, "Multiple 'within' expressions not supported");
            }

            if (node.paramCount < 2) {
                throw SqlException.$(node.position, "Too few arguments for 'within'");
            }

            ExpressionNode col = node.paramCount < 3 ? node.lhs : node.args.getLast();

            if (col.type != ExpressionNode.LITERAL) {
                throw SqlException.unexpectedToken(col.position, col.token);
            }

            CharSequence column = translator.translateAlias(col.token);

            if (metadata.getColumnIndexQuiet(column) == -1) {
                throw SqlException.invalidColumn(col.position, col.token);
            }

            final int hashColumnIndex = metadata.getColumnIndex(column);
            final int hashColumnType = metadata.getColumnType(hashColumnIndex);

            if (!ColumnType.isGeoHash(hashColumnType)) {
                throw SqlException.$(node.position, "GeoHash column type expected");
            }

            if (prefixes.size() == 0) {
                prefixes.add(hashColumnIndex);
                prefixes.add(hashColumnType);
            }

            int c = node.paramCount - 1;

            if (c == 1) {
                ExpressionNode inArg = node.rhs;
                processArgument(inArg, metadata, functionParser, executionContext, hashColumnType, prefixes);
            } else {
                for (c--; c > -1; c--) {
                    ExpressionNode inArg = node.args.getQuick(c);
                    processArgument(inArg, metadata, functionParser, executionContext, hashColumnType, prefixes);
                }
            }
            return true;
        } else {
            return false;
        }
    }

    private void resetExcludedNodes() {
        revertNodes(keyExclNodes);
    }

    private void resetNodes() {
        revertNodes(keyNodes);
        resetExcludedNodes();
    }

    /**
     * Attempts to rewrite a single node if it contains a dateadd pattern on the designated timestamp.
     */
    private void rewriteDateaddNode(ObjectPool<ExpressionNode> expressionNodePool, ExpressionNode node) {
        // Check for comparison operators: =, !=, <, <=, >, >=
        if (node.type == ExpressionNode.OPERATION && node.paramCount == 2) {
            final CharSequence op = node.token;
            if (Chars.equals(op, '=') || Chars.equals(op, '<') || Chars.equals(op, "<=")
                    || Chars.equals(op, '>') || Chars.equals(op, ">=") || Chars.equals(op, "!=")) {
                tryRewriteDateaddComparison(expressionNodePool, node);
            }
        }
        // Check for BETWEEN: dateadd() BETWEEN lo AND hi
        if (isBetweenKeyword(node.token) && node.paramCount == 3 && node.args.size() >= 3) {
            tryRewriteDateaddBetween(expressionNodePool, node);
        }
    }

    /**
     * Rewrites the WHERE clause AST to transform dateadd patterns on the designated timestamp
     * into a form that the existing intrinsic analysis can recognize.
     * <p>
     * Example: dateadd('m', 15, ts) = value  ->  ts = dateadd('m', -15, value)
     * <p>
     * Uses pre-order iterative tree traversal to handle all AND-connected predicates.
     */
    private void rewriteDateaddTimestamp(ObjectPool<ExpressionNode> expressionNodePool, ExpressionNode node) {
        if (node == null || expressionNodePool == null) {
            return;
        }

        // Pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal
        stack.clear();
        while (!stack.isEmpty() || node != null) {
            if (node != null) {
                if (isAndKeyword(node.token)) {
                    stack.push(node.rhs);
                    node = node.lhs;
                } else {
                    rewriteDateaddNode(expressionNodePool, node);
                    node = stack.poll();
                }
            } else {
                node = stack.poll();
            }
        }
    }

    private boolean translateBetweenToTimestampModel(
            TimestampDriver timestampDriver,
            IntrinsicModel model,
            FunctionParser functionParser,
            RecordMetadata metadata,
            SqlExecutionContext executionContext,
            ExpressionNode node
    ) throws SqlException {
        if (node.type == ExpressionNode.CONSTANT) {
            model.setBetweenBoundary(parseTokenAsTimestamp(timestampDriver, node));
            return true;
        } else if (isFunc(node)) {
            final Function func = functionParser.parseFunction(node, metadata, executionContext);
            try {
                checkFunctionCanBeTimestamp(metadata, executionContext, func, node.position);
                if (func.isConstant()) {
                    long timestamp = getTimestampFromConstFunction(timestampDriver, func, node.position, false);
                    model.setBetweenBoundary(timestamp);
                    Misc.free(func);
                    return true;
                } else if (func.isRuntimeConstant()) {
                    model.setBetweenBoundary(func);
                    return true;
                }
                Misc.free(func);
            } catch (Throwable th) {
                Misc.free(func);
                throw th;
            }
        }
        return false;
    }

    /**
     * Tries to extract timestamp intervals from an OR tree.
     * Only succeeds if:
     * - The node is an OR tree consisting entirely of timestamp IN/= predicates
     * - The model doesn't already have interval filters (to allow union semantics)
     * <p>
     * Returns true if extraction was successful.
     */
    private boolean tryExtractOrTimestampIntrinsics(
            TimestampDriver timestampDriver,
            IntrinsicModel model,
            ExpressionNode node,
            FunctionParser functionParser,
            RecordMetadata metadata,
            SqlExecutionContext executionContext
    ) throws SqlException {
        // Only attempt OR extraction if we don't have existing interval filters
        // This allows us to use union semantics for the OR branches
        if (model.hasIntervalFilters()) {
            return false;
        }

        // Check if this is an OR tree of timestamp intrinsics
        if (!isOrOfTimestampIn(node)) {
            return false;
        }

        // Extract the intervals with union semantics
        return extractOrTimestampIntervals(timestampDriver, model, node, true, functionParser, metadata, executionContext);
    }

    /**
     * Tries to rewrite a BETWEEN node if the column is wrapped in dateadd.
     * <p>
     * Transforms: dateadd(p, s, ts) BETWEEN lo AND hi  ->  ts BETWEEN dateadd(p, -s, lo) AND dateadd(p, -s, hi)
     */
    private void tryRewriteDateaddBetween(ObjectPool<ExpressionNode> expressionNodePool, ExpressionNode node) {
        // Args are in reverse order: args[0]=hi, args[1]=lo, args[2]=col (or last element)
        final ExpressionNode col = node.args.getLast();
        if (isDateaddOnTimestamp(col)) {
            final ExpressionNode lo = node.args.getQuick(1);
            final ExpressionNode hi = node.args.getQuick(0);
            final ExpressionNode inverseLo = createInverseDateadd(expressionNodePool, col, lo);
            final ExpressionNode inverseHi = createInverseDateadd(expressionNodePool, col, hi);
            if (inverseLo != null && inverseHi != null) {
                // Replace the column with the unwrapped timestamp
                node.args.setQuick(node.args.size() - 1, col.args.getQuick(0));
                node.args.setQuick(1, inverseLo);
                node.args.setQuick(0, inverseHi);
            }
        }
    }

    /**
     * Tries to rewrite a comparison node if it contains dateadd(period, stride, designated_ts).
     * <p>
     * If LHS is dateadd(period, stride, ts), transforms to: ts OP dateadd(period, -stride, RHS)
     * If RHS is dateadd(period, stride, ts), transforms to: dateadd(period, -stride, LHS) OP ts
     */
    private void tryRewriteDateaddComparison(ObjectPool<ExpressionNode> expressionNodePool, ExpressionNode node) {
        if (isDateaddOnTimestamp(node.lhs)) {
            final ExpressionNode dateadd = node.lhs;
            final ExpressionNode inverseDateadd = createInverseDateadd(expressionNodePool, dateadd, node.rhs);
            if (inverseDateadd != null) {
                // Replace: dateadd(p, s, ts) OP value  ->  ts OP dateadd(p, -s, value)
                node.lhs = dateadd.args.getQuick(0); // the timestamp column
                node.rhs = inverseDateadd;
            }
        } else if (isDateaddOnTimestamp(node.rhs)) {
            final ExpressionNode dateadd = node.rhs;
            final ExpressionNode inverseDateadd = createInverseDateadd(expressionNodePool, dateadd, node.lhs);
            if (inverseDateadd != null) {
                // Replace: value OP dateadd(p, s, ts)  ->  dateadd(p, -s, value) OP ts
                node.lhs = inverseDateadd;
                node.rhs = dateadd.args.getQuick(0); // the timestamp column
            }
        }
    }

    /**
     * Removes quotes and creates immutable char sequence. When value is not quoted it is returned verbatim.
     *
     * @param value immutable character sequence.
     * @return immutable character sequence without surrounding quote marks.
     */
    private CharSequence unquote(CharSequence value) {
        if (Chars.isQuoted(value)) {
            return csPool.next().of(value, 1, value.length() - 2);
        }
        return value;
    }

    ExpressionNode extractWithin(
            AliasTranslator translator,
            ExpressionNode node,
            RecordMetadata metadata,
            FunctionParser functionParser,
            SqlExecutionContext executionContext,
            LongList prefixes
    ) throws SqlException {
        prefixes.clear();
        if (node == null) {
            return null;
        }

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        if (removeWithin(translator, node, metadata, functionParser, executionContext, prefixes)) {
            return collapseWithinNodes(node);
        }

        ExpressionNode root = node;
        stack.clear();
        while (!stack.isEmpty() || node != null) {
            if (node != null) {
                if (isAndKeyword(node.token) || isOrKeyword(node.token)) {
                    if (!removeWithin(translator, node.rhs, metadata, functionParser, executionContext, prefixes)) {
                        stack.push(node.rhs);
                    }
                    node = removeWithin(translator, node.lhs, metadata, functionParser, executionContext, prefixes) ? null : node.lhs;
                } else {
                    node = stack.poll();
                }
            } else {
                node = stack.poll();
            }
        }

        return collapseWithinNodes(root);
    }

    static {
        intrinsicOps.put("in", INTRINSIC_OP_IN);
        intrinsicOps.put(">", INTRINSIC_OP_GREATER);
        intrinsicOps.put(">=", INTRINSIC_OP_GREATER_EQ);
        intrinsicOps.put("<", INTRINSIC_OP_LESS);
        intrinsicOps.put("<=", INTRINSIC_OP_LESS_EQ);
        intrinsicOps.put("=", INTRINSIC_OP_EQUAL);
        intrinsicOps.put("!=", INTRINSIC_OP_NOT_EQ);
        intrinsicOps.put("not", INTRINSIC_OP_NOT);
        intrinsicOps.put("between", INTRINSIC_OP_BETWEEN);
        intrinsicOps.put("and_offset", INTRINSIC_OP_AND_OFFSET);
    }
}

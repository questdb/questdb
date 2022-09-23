/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.functions.AbstractGeoHashFunction;
import io.questdb.griffin.model.AliasTranslator;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.griffin.model.IntrinsicModel;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.FlyweightCharSequence;

import java.util.ArrayDeque;

import static io.questdb.griffin.SqlKeywords.*;

final class WhereClauseParser implements Mutable {
    private static final int INTRINSIC_OP_IN = 1;
    private static final int INTRINSIC_OP_GREATER = 2;
    private static final int INTRINSIC_OP_GREATER_EQ = 3;
    private static final int INTRINSIC_OP_LESS = 4;
    private static final int INTRINSIC_OP_LESS_EQ = 5;
    private static final int INTRINSIC_OP_EQUAL = 6;
    private static final int INTRINSIC_OP_NOT_EQ = 7;
    private static final int INTRINSIC_OP_NOT = 8;
    private static final int INTRINSIC_OP_BETWEEN = 9;
    private static final CharSequenceIntHashMap intrinsicOps = new CharSequenceIntHashMap();
    private final ArrayDeque<ExpressionNode> stack = new ArrayDeque<>();
    private final ObjList<ExpressionNode> keyNodes = new ObjList<>();
    private final ObjList<ExpressionNode> keyExclNodes = new ObjList<>();
    private final ObjList<ExpressionNode> tempNodes = new ObjList<>();

    // TODO: configure size
    private final ObjectPool<IntrinsicModel> models = new ObjectPool<>(IntrinsicModel.FACTORY, 8);
    private final CharSequenceHashSet tempKeys = new CharSequenceHashSet();
    private final CharSequenceHashSet tempKeyValues = new CharSequenceHashSet();
    private final CharSequenceHashSet tempKeyExcludedValues = new CharSequenceHashSet();
    private final IntList tempKeyValuePos = new IntList();
    private final IntList tempKeyExcludedValuePos = new IntList();
    private final IntList tempPos = new IntList();
    private final CharSequenceHashSet tempK = new CharSequenceHashSet();
    private final IntList tempP = new IntList();

    // TODO: configure size
    private final ObjectPool<FlyweightCharSequence> csPool = new ObjectPool<>(FlyweightCharSequence.FACTORY, 64);
    private CharSequence timestamp;
    private CharSequence preferredKeyColumn;

    @Override
    public void clear() {
        this.models.clear();
        this.stack.clear();
        this.keyNodes.clear();
        this.keyExclNodes.clear();
        this.tempNodes.clear();
        this.tempKeys.clear();
        this.tempPos.clear();
        this.tempK.clear();
        this.tempP.clear();
        this.tempKeyValues.clear();
        this.tempKeyExcludedValues.clear();
        this.tempKeyValuePos.clear();
        this.tempKeyExcludedValuePos.clear();
        this.csPool.clear();
        this.timestamp = null;
        this.preferredKeyColumn = null;
    }

    private static void checkNodeValid(ExpressionNode node) throws SqlException {
        if (node.lhs == null || node.rhs == null) {
            throw SqlException.$(node.position, "Argument expected");
        }
    }

    private static boolean nodesEqual(ExpressionNode left, ExpressionNode right) {
        return (left.type == ExpressionNode.LITERAL || left.type == ExpressionNode.CONSTANT) &&
                (right.type == ExpressionNode.LITERAL || right.type == ExpressionNode.CONSTANT) &&
                Chars.equals(left.token, right.token);
    }

    private boolean columnIsPreferredOrIndexedAndNotPartOfMultiColumnLatestBy(
            CharSequence columnName,
            RecordMetadata m,
            boolean latestByMultiColumn
    ) {
        return !latestByMultiColumn &&
                (Chars.equalsIgnoreCaseNc(preferredKeyColumn, columnName)
                        ||
                        (preferredKeyColumn == null && m.isColumnIndexed(m.getColumnIndex(columnName))));
    }

    private boolean analyzeEquals(
            AliasTranslator translator,
            IntrinsicModel model,
            ExpressionNode node,
            RecordMetadata m,
            FunctionParser functionParser,
            SqlExecutionContext executionContext,
            boolean latestByMultiColumn
    ) throws SqlException {
        checkNodeValid(node);
        return analyzeEquals0( // left == right
                translator,
                model,
                node,
                node.lhs,
                node.rhs,
                m,
                functionParser,
                executionContext,
                latestByMultiColumn
        )
                ||
                analyzeEquals0( // right == left
                        translator,
                        model,
                        node,
                        node.rhs,
                        node.lhs,
                        m,
                        functionParser,
                        executionContext,
                        latestByMultiColumn
                );
    }

    private boolean analyzeEquals0(
            AliasTranslator translator,
            IntrinsicModel model,
            ExpressionNode node,
            ExpressionNode a,
            ExpressionNode b,
            RecordMetadata m,
            FunctionParser functionParser,
            SqlExecutionContext executionContext,
            boolean latestByMultiColumn
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
                Function function = functionParser.parseFunction(b, m, executionContext);
                checkFunctionCanBeTimestamp(m, executionContext, function, b.position);
                return analyzeTimestampEqualsFunction(model, node, function, b.position);
            } else {
                CharSequence columnName = translator.translateAlias(a.token);
                int index = m.getColumnIndexQuiet(columnName);
                if (index == -1) {
                    throw SqlException.invalidColumn(a.position, a.token);
                }

                switch (ColumnType.tagOf(m.getColumnType(index))) {
                    case ColumnType.SYMBOL:
                    case ColumnType.STRING:
                    case ColumnType.LONG:
                    case ColumnType.INT:
                        if (columnIsPreferredOrIndexedAndNotPartOfMultiColumnLatestBy(columnName, m, latestByMultiColumn)) {
                            CharSequence value = isNullKeyword(b.token) ? null : unquote(b.token);
                            if (Chars.equalsIgnoreCaseNc(model.keyColumn, columnName)) {
                                // compute overlap of values
                                // if values do overlap, keep only our value
                                // otherwise invalidate entire model
                                if (tempKeyValues.contains(value)) {
                                    // when we have "x in ('a,'b') and x = 'a')" the x='b' can never happen
                                    // so we have to clear all other key values
                                    if (tempKeyValues.size() > 1) {
                                        tempKeyValues.clear();
                                        tempKeyValuePos.clear();
                                        tempKeyValues.add(value);
                                        tempKeyValuePos.add(b.position);
                                        node.intrinsicValue = IntrinsicModel.TRUE;
                                    }
                                } else {
                                    if (tempKeyExcludedValues.contains(value)) {
                                        if (tempKeyExcludedValues.size() > 1) {
                                            int removedIndex = tempKeyExcludedValues.remove(value);
                                            if (removedIndex > -1) {
                                                tempKeyExcludedValuePos.removeIndex(index);
                                            }
                                        } else {
                                            tempKeyExcludedValues.clear();
                                            tempKeyExcludedValuePos.clear();
                                        }
                                        removeNodes(b, keyExclNodes);
                                    }
                                    node.intrinsicValue = IntrinsicModel.TRUE;
                                    model.intrinsicValue = IntrinsicModel.FALSE;
                                    return false;
                                }
                            } else if (model.keyColumn == null || m.getIndexValueBlockCapacity(index) > m.getIndexValueBlockCapacity(model.keyColumn)) {
                                model.keyColumn = columnName;
                                tempKeyValues.clear();
                                tempKeyValuePos.clear();
                                tempKeyExcludedValues.clear();
                                tempKeyExcludedValuePos.clear();
                                tempKeyValues.add(value);
                                tempKeyValuePos.add(b.position);
                                resetNodes();
                                node.intrinsicValue = IntrinsicModel.TRUE;
                            }
                            keyNodes.add(node);
                            return true;
                        }
                        //fall through
                    default:
                        return false;
                }
            }
        }
        return false;
    }

    private boolean analyzeTimestampEqualsFunction(
            IntrinsicModel model,
            ExpressionNode node,
            Function function,
            int functionPosition
    ) throws SqlException {
        if (function.isConstant()) {
            long value = getTimestampFromConstFunction(function, functionPosition);
            if (value == Numbers.LONG_NaN) {
                // make it empty set
                model.intersectEmpty();
            } else {
                model.intersectIntervals(value, value);
            }
            node.intrinsicValue = IntrinsicModel.TRUE;
            return true;
        } else if (function.isRuntimeConstant()) {
            model.intersectEquals(function);
            node.intrinsicValue = IntrinsicModel.TRUE;
            return true;
        }
        return false;
    }

    private boolean analyzeTimestampNotEqualsFunction(
            IntrinsicModel model,
            ExpressionNode node,
            Function function,
            int functionPosition
    ) throws SqlException {
        if (function.isConstant()) {
            long value = getTimestampFromConstFunction(function, functionPosition);
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

    private boolean analyzeGreater(
            IntrinsicModel model,
            ExpressionNode node,
            boolean equalsTo,
            FunctionParser functionParser,
            RecordMetadata metadata,
            SqlExecutionContext executionContext
    ) throws SqlException {
        checkNodeValid(node);

        if (nodesEqual(node.lhs, node.rhs)) {
            model.intrinsicValue = IntrinsicModel.FALSE;
            return false;
        }

        if (timestamp == null) {
            return false;
        }

        if (node.lhs.type == ExpressionNode.LITERAL && Chars.equals(node.lhs.token, timestamp)) {
            return analyzeTimestampGreater(model, node, equalsTo, functionParser, metadata, executionContext, node.rhs);
        } else if (node.rhs.type == ExpressionNode.LITERAL && Chars.equals(node.rhs.token, timestamp)) {
            return analyzeTimestampLess(model, node, equalsTo, functionParser, metadata, executionContext, node.lhs);
        }

        return false;
    }

    private boolean analyzeTimestampGreater(
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
                lo = parseFullOrPartialDate(equalsTo, compareWithNode, true);
            } catch (NumericException e) {
                throw SqlException.invalidDate(compareWithNode.position);
            }
            model.intersectIntervals(lo, Long.MAX_VALUE);
            node.intrinsicValue = IntrinsicModel.TRUE;
            return true;
        } else if (isFunc(compareWithNode)) {
            Function function = functionParser.parseFunction(compareWithNode, metadata, executionContext);
            checkFunctionCanBeTimestamp(metadata, executionContext, function, compareWithNode.position);

            if (function.isConstant()) {
                lo = getTimestampFromConstFunction(function, compareWithNode.position);
                if (lo == Numbers.LONG_NaN) {
                    // make it empty set
                    model.intersectEmpty();
                } else {
                    model.intersectIntervals(lo + adjustComparison(equalsTo, true), Long.MAX_VALUE);
                }
                node.intrinsicValue = IntrinsicModel.TRUE;
                return true;
            } else if (function.isRuntimeConstant()) {
                model.intersectIntervals(function, Long.MAX_VALUE, adjustComparison(equalsTo, true));
                node.intrinsicValue = IntrinsicModel.TRUE;
                return true;
            }
        }

        return false;
    }

    private static boolean isFunc(ExpressionNode compareWithNode) {
        return compareWithNode.type == ExpressionNode.FUNCTION
                || compareWithNode.type == ExpressionNode.BIND_VARIABLE
                || compareWithNode.type == ExpressionNode.OPERATION;
    }

    private static long getTimestampFromConstFunction(
            Function function,
            int functionPosition
    ) throws SqlException {
        if (!ColumnType.isSymbolOrString(function.getType())) {
            return function.getTimestamp(null);
        }
        CharSequence str = function.getStr(null);
        return parseStringAsTimestamp(str, functionPosition);
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

    private boolean checkFunctionCanBeTimestampInterval(
            SqlExecutionContext executionContext,
            Function function
    ) throws SqlException {
        int type = function.getType();
        if (ColumnType.isUndefined(type)) {
            function.assignType(ColumnType.STRING, executionContext.getBindVariableService());
            return true;
        }
        return ColumnType.isString(type);
    }

    private boolean analyzeBetween(
            AliasTranslator translator,
            IntrinsicModel model,
            ExpressionNode node, RecordMetadata m,
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
        return analyzeBetween0(model, col, node, false, functionParser, metadata, executionContext);
    }

    private boolean analyzeIn(
            AliasTranslator translator,
            IntrinsicModel model,
            ExpressionNode node,
            RecordMetadata metadata,
            FunctionParser functionParser,
            SqlExecutionContext executionContext,
            boolean latestByMultiColumn
    ) throws SqlException {

        if (node.paramCount < 2) {
            throw SqlException.$(node.position, "Too few arguments for 'in'");
        }

        ExpressionNode col = node.paramCount < 3 ? node.lhs : node.args.getLast();

        if (col.type != ExpressionNode.LITERAL) {
            return false;
        }

        CharSequence column = translator.translateAlias(col.token);

        if (metadata.getColumnIndexQuiet(column) == -1) {
            throw SqlException.invalidColumn(col.position, col.token);
        }
        return analyzeInInterval(model, col, node, false, functionParser, metadata, executionContext)
                || analyzeListOfValues(model, column, metadata, node, latestByMultiColumn)
                || analyzeInLambda(model, column, metadata, node, latestByMultiColumn);
    }

    private boolean analyzeBetween0(
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
            boolean isBetweenTranslated = translateBetweenToTimestampModel(model, functionParser, metadata, executionContext, lo);
            if (isBetweenTranslated) {
                isBetweenTranslated = translateBetweenToTimestampModel(model, functionParser, metadata, executionContext, hi);
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

    private boolean translateBetweenToTimestampModel(
            IntrinsicModel model,
            FunctionParser functionParser,
            RecordMetadata metadata,
            SqlExecutionContext executionContext,
            ExpressionNode node
    ) throws SqlException {
        if (node.type == ExpressionNode.CONSTANT) {
            model.setBetweenBoundary(parseTokenAsTimestamp(node));
            return true;
        } else if (isFunc(node)) {
            Function function = functionParser.parseFunction(node, metadata, executionContext);
            checkFunctionCanBeTimestamp(metadata, executionContext, function, node.position);
            if (function.isConstant()) {
                long timestamp = getTimestampFromConstFunction(function, node.position);
                model.setBetweenBoundary(timestamp);
            } else if (function.isRuntimeConstant()) {
                model.setBetweenBoundary(function);
            } else {
                return false;
            }
            return true;
        }
        return false;
    }

    private static boolean canCastToTimestamp(int type) {
        final int typeTag = ColumnType.tagOf(type);
        return typeTag == ColumnType.TIMESTAMP
                || typeTag == ColumnType.DATE
                || typeTag == ColumnType.STRING
                || typeTag == ColumnType.SYMBOL;
    }

    private static long parseTokenAsTimestamp(ExpressionNode lo) throws SqlException {
        try {
            if (!isNullKeyword(lo.token)) {
                return IntervalUtils.parseFloorPartialDate(lo.token, 1, lo.token.length() - 1);
            }
            return Numbers.LONG_NaN;
        } catch (NumericException ignore) {
            throw SqlException.invalidDate(lo.position);
        }
    }

    private static long parseStringAsTimestamp(CharSequence str, int position) throws SqlException {
        try {
            return IntervalUtils.parseFloorPartialDate(str);
        } catch (NumericException ignore) {
            throw SqlException.invalidDate(position);
        }
    }

    private boolean analyzeInInterval(
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

        if (in.paramCount == 2) {
            // Single value ts in '2010-01-01' - treat string literal as an interval, not single Timestamp point
            ExpressionNode inArg = in.rhs;
            if (inArg.type == ExpressionNode.CONSTANT) {
                if (isNullKeyword(inArg.token)) {
                    if (!isNegated) {
                        model.intersectIntervals(Numbers.LONG_NaN, Numbers.LONG_NaN);
                    } else {
                        model.subtractIntervals(Numbers.LONG_NaN, Numbers.LONG_NaN);
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
                Function f1 = functionParser.parseFunction(inArg, metadata, executionContext);
                if (checkFunctionCanBeTimestampInterval(executionContext, f1)) {
                    if (f1.isConstant()) {
                        CharSequence funcVal = f1.getStr(null);
                        if (!isNegated) {
                            model.intersectIntervals(funcVal, 0, funcVal.length(), inArg.position);
                        } else {
                            model.subtractIntervals(funcVal, 0, funcVal.length(), inArg.position);
                        }
                    } else if (f1.isRuntimeConstant()) {
                        if (!isNegated) {
                            model.intersectRuntimeIntervals(f1);
                        } else {
                            model.subtractRuntimeIntervals(f1);
                        }
                    } else {
                        return false;
                    }

                    in.intrinsicValue = IntrinsicModel.TRUE;
                    return true;
                } else {
                    checkFunctionCanBeTimestamp(metadata, executionContext, f1, inArg.position);
                    // This is IN (TIMESTAMP) one value which is timestamp and not a STRING
                    // This is same as equals
                    return analyzeTimestampEqualsFunction(model, in, f1, inArg.position);
                }
            }
        } else {
            // Multiple values treat as multiple Timestamp points
            // Only possible to translate if it's the only timestamp restriction atm
            // NOT IN can be translated in any case as series of subtractions
            if (!model.hasIntervalFilters() || isNegated) {
                int n = in.args.size() - 1;
                for (int i = 0; i < n; i++) {
                    ExpressionNode inListItem = in.args.getQuick(i);
                    if (inListItem.type != ExpressionNode.CONSTANT) {
                        return false;
                    }
                }

                for (int i = 0; i < n; i++) {
                    ExpressionNode inListItem = in.args.getQuick(i);
                    long ts = parseTokenAsTimestamp(inListItem);
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
                in.intrinsicValue = IntrinsicModel.TRUE;
                return true;
            }
        }
        return false;
    }

    private boolean analyzeInLambda(
            IntrinsicModel model,
            CharSequence columnName,
            RecordMetadata m,
            ExpressionNode node,
            boolean latestByMultiColumn
    ) throws SqlException {

        int columnIndex = m.getColumnIndex(columnName);
        if (columnIsPreferredOrIndexedAndNotPartOfMultiColumnLatestBy(columnName, m, latestByMultiColumn)) {
            if (preferredKeyColumn != null && !Chars.equalsIgnoreCase(columnName, preferredKeyColumn)) {
                return false;
            }

            if (node.rhs == null || node.rhs.type != ExpressionNode.QUERY) {
                return false;
            }

            // check if we already have indexed column and it is of worse selectivity
            if (model.keyColumn != null
                    && (!Chars.equalsIgnoreCase(model.keyColumn, columnName))
                    && m.getIndexValueBlockCapacity(columnIndex) <= m.getIndexValueBlockCapacity(model.keyColumn)) {
                return false;
            }

            if ((Chars.equalsIgnoreCaseNc(model.keyColumn, columnName) && model.keySubQuery != null) || node.paramCount > 2) {
                throw SqlException.$(node.position, "Multiple lambda expressions not supported");
            }

            tempKeyValues.clear();
            tempKeyValuePos.clear();
            tempKeyValuePos.add(node.position);
            model.keySubQuery = node.rhs.queryModel;

            // revert previously processed nodes
            return revertProcessedNodes(keyNodes, model, columnName, node);
        }
        return false;
    }

    private boolean analyzeLess(
            IntrinsicModel model,
            ExpressionNode node,
            boolean equalsTo,
            FunctionParser functionParser,
            RecordMetadata metadata,
            SqlExecutionContext executionContext
    ) throws SqlException {

        checkNodeValid(node);

        if (nodesEqual(node.lhs, node.rhs)) {
            model.intrinsicValue = IntrinsicModel.FALSE;
            return false;
        }

        if (timestamp == null) {
            return false;
        }

        if (node.lhs.type == ExpressionNode.LITERAL && Chars.equals(node.lhs.token, timestamp)) {
            return analyzeTimestampLess(model, node, equalsTo, functionParser, metadata, executionContext, node.rhs);
        } else if (node.rhs.type == ExpressionNode.LITERAL && Chars.equals(node.rhs.token, timestamp)) {
            return analyzeTimestampGreater(model, node, equalsTo, functionParser, metadata, executionContext, node.lhs);
        }

        return false;
    }

    private boolean analyzeTimestampLess(
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
                long hi = parseFullOrPartialDate(equalsTo, compareWithNode, false);
                model.intersectIntervals(Long.MIN_VALUE, hi);
                node.intrinsicValue = IntrinsicModel.TRUE;
            } catch (NumericException e) {
                throw SqlException.invalidDate(compareWithNode.position);
            }
            return true;
        } else if (isFunc(compareWithNode)) {
            Function function = functionParser.parseFunction(compareWithNode, metadata, executionContext);
            checkFunctionCanBeTimestamp(metadata, executionContext, function, compareWithNode.position);

            if (function.isConstant()) {
                long hi = getTimestampFromConstFunction(function, compareWithNode.position);
                if (hi == Numbers.LONG_NaN) {
                    model.intersectEmpty();
                } else {
                    model.intersectIntervals(Long.MIN_VALUE, hi + adjustComparison(equalsTo, false));
                }
                node.intrinsicValue = IntrinsicModel.TRUE;
                return true;
            } else if (function.isRuntimeConstant()) {
                model.intersectIntervals(Long.MIN_VALUE, function, adjustComparison(equalsTo, false));
                node.intrinsicValue = IntrinsicModel.TRUE;
                return true;
            }
        }

        return false;
    }

    private static short adjustComparison(boolean equalsTo, boolean isLo) {
        return equalsTo ? 0 : isLo ? (short) 1 : (short) -1;
    }

    private boolean analyzeListOfValues(
            IntrinsicModel model,
            CharSequence columnName,
            RecordMetadata m,
            ExpressionNode node,
            boolean latestByMultiColumn
    ) {
        final int columnIndex = m.getColumnIndex(columnName);
        boolean newColumn = true;
        if (columnIsPreferredOrIndexedAndNotPartOfMultiColumnLatestBy(columnName, m, latestByMultiColumn)) {

            // check if we already have indexed column and it is of worse selectivity
            // "preferred" is an unfortunate name, this column is from "latest by" clause, I should name it better
            //
            if (model.keyColumn != null
                    && (newColumn = !Chars.equals(model.keyColumn, columnName))
                    && m.getIndexValueBlockCapacity(columnIndex) <= m.getIndexValueBlockCapacity(model.keyColumn)) {
                return false;
            }


            int i = node.paramCount - 1;
            tempKeys.clear();
            tempPos.clear();

            // collect and analyze values of indexed field
            // if any of values is not an indexed constant - bail out
            if (i == 1) {
                if (node.rhs == null || (node.rhs.type != ExpressionNode.CONSTANT && node.rhs.type != ExpressionNode.BIND_VARIABLE)) {
                    return false;
                }
                if (tempKeys.add(unquote(node.rhs.token))) {
                    tempPos.add(node.position);
                }
            } else {
                for (i--; i > -1; i--) {
                    ExpressionNode c = node.args.getQuick(i);
                    if (c.type != ExpressionNode.CONSTANT && c.type != ExpressionNode.BIND_VARIABLE) {
                        return false;
                    }

                    if (isNullKeyword(c.token)) {
                        if (tempKeys.add(null)) {
                            tempPos.add(c.position);
                        }
                    } else {
                        if (tempKeys.add(unquote(c.token))) {
                            tempPos.add(c.position);
                        }
                    }
                }
            }

            // clear values if this is new column
            // and reset intrinsic values on nodes associated with old column
            if (newColumn) {
                tempKeyValues.clear();
                tempKeyValuePos.clear();
                tempKeyValues.addAll(tempKeys);
                tempKeyValuePos.addAll(tempPos);
                return revertProcessedNodes(keyNodes, model, columnName, node);
            } else {
                if (tempKeyValues.size() == 0) {
                    tempKeyValues.addAll(tempKeys);
                    tempKeyValuePos.addAll(tempPos);
                }
            }

            if (model.keySubQuery == null) {
                // calculate overlap of values
                replaceAllWithOverlap(model, true);

                keyNodes.add(node);
                node.intrinsicValue = IntrinsicModel.TRUE;
                return true;
            }
        }
        return false;
    }

    private boolean analyzeNotEquals(
            AliasTranslator translator,
            IntrinsicModel model,
            ExpressionNode node,
            RecordMetadata m,
            FunctionParser functionParser,
            SqlExecutionContext executionContext,
            boolean canUseIndex
    ) throws SqlException {
        checkNodeValid(node);
        return analyzeNotEquals0(translator, model, node, node.lhs, node.rhs, m, functionParser, executionContext, canUseIndex)
                || analyzeNotEquals0(translator, model, node, node.rhs, node.lhs, m, functionParser, executionContext, canUseIndex);
    }

    private boolean analyzeNotEquals0(
            AliasTranslator translator,
            IntrinsicModel model,
            ExpressionNode node,
            ExpressionNode a,
            ExpressionNode b,
            RecordMetadata m,
            FunctionParser functionParser,
            SqlExecutionContext executionContext,
            boolean latestByMultiColumn
    ) throws SqlException {
        if (nodesEqual(a, b)) {
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
                Function function = functionParser.parseFunction(b, m, executionContext);
                checkFunctionCanBeTimestamp(m, executionContext, function, b.position);
                return analyzeTimestampNotEqualsFunction(model, node, function, b.position);
            } else {
                CharSequence columnName = translator.translateAlias(a.token);
                int index = m.getColumnIndexQuiet(columnName);
                if (index == -1) {
                    throw SqlException.invalidColumn(a.position, a.token);
                }

                switch (ColumnType.tagOf(m.getColumnType(index))) {
                    case ColumnType.SYMBOL:
                    case ColumnType.STRING:
                    case ColumnType.LONG:
                    case ColumnType.INT:
                        if (columnIsPreferredOrIndexedAndNotPartOfMultiColumnLatestBy(columnName, m, latestByMultiColumn)) {
                            CharSequence value = isNullKeyword(b.token) ? null : unquote(b.token);
                            if (Chars.equalsIgnoreCaseNc(model.keyColumn, columnName)) {
                                if (tempKeyExcludedValues.contains(value)) {
                                    // when we have "x not in ('a,'b') and x != 'a')" the x='b' can never happen
                                    // so we have to clear all other key values
                                    if (tempKeyExcludedValues.size() > 1) {
                                        tempKeyExcludedValues.clear();
                                        tempKeyExcludedValuePos.clear();
                                        tempKeyExcludedValues.add(value);
                                        tempKeyExcludedValuePos.add(b.position);
                                        node.intrinsicValue = IntrinsicModel.TRUE;
                                    }
                                } else {
                                    if (tempKeyValues.contains(value)) {
                                        if (tempKeyValues.size() > 1) {
                                            int removedIndex = tempKeyValues.remove(value);
                                            if (removedIndex > -1) {
                                                tempKeyValuePos.removeIndex(index);
                                            }
                                            tempKeyValuePos.remove(b.position);
                                        } else {
                                            tempKeyValues.clear();
                                            tempKeyValuePos.clear();
                                        }
                                        removeNodes(b, keyNodes);
                                    }
                                    tempKeyExcludedValues.add(value);
                                    tempKeyExcludedValuePos.add(b.position);
                                    node.intrinsicValue = IntrinsicModel.TRUE;
                                    return false;
                                }
                            } else if (model.keyColumn == null || m.getIndexValueBlockCapacity(index) > m.getIndexValueBlockCapacity(model.keyColumn)) {
                                model.keyColumn = columnName;
                                tempKeyValues.clear();
                                tempKeyValuePos.clear();
                                tempKeyExcludedValues.clear();
                                tempKeyExcludedValuePos.clear();
                                tempKeyExcludedValues.add(value);
                                tempKeyExcludedValuePos.add(b.position);
                                resetNodes();
                                node.intrinsicValue = IntrinsicModel.TRUE;
                            }
                            keyExclNodes.add(node);
                            return true;
                        } else if (Chars.equalsIgnoreCaseNc(preferredKeyColumn, columnName)) {
                            keyExclNodes.add(node);
                            return false;
                        }
                        //fall through
                    default:
                        return false;
                }
            }
        }
        return false;
    }

    private boolean analyzeNotIn(
            AliasTranslator translator,
            IntrinsicModel model,
            ExpressionNode notNode,
            RecordMetadata m,
            FunctionParser functionParser,
            RecordMetadata metadata,
            SqlExecutionContext executionContext,
            boolean latestByMultiColumn
    ) throws SqlException {

        ExpressionNode node = notNode.rhs;

        if (node.paramCount < 2) {
            throw SqlException.$(node.position, "Too few arguments for 'in'");
        }

        ExpressionNode col = node.paramCount < 3 ? node.lhs : node.args.getLast();

        if (col.type != ExpressionNode.LITERAL) {
            throw SqlException.$(col.position, "Column name expected");
        }

        CharSequence column = translator.translateAlias(col.token);

        if (m.getColumnIndexQuiet(column) == -1) {
            throw SqlException.invalidColumn(col.position, col.token);
        }

        boolean ok = analyzeInInterval(model, col, node, true, functionParser, metadata, executionContext);
        if (ok) {
            notNode.intrinsicValue = IntrinsicModel.TRUE;
        } else {
            analyzeNotListOfValues(model, column, m, node, notNode, latestByMultiColumn);
        }

        return ok;
    }

    private boolean analyzeNotBetween(
            AliasTranslator translator,
            IntrinsicModel model,
            ExpressionNode notNode,
            RecordMetadata m,
            FunctionParser functionParser,
            RecordMetadata metadata,
            SqlExecutionContext executionContext,
            boolean latestByMultiColumn
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

        boolean ok = analyzeBetween0(model, col, node, true, functionParser, metadata, executionContext);
        if (ok) {
            notNode.intrinsicValue = IntrinsicModel.TRUE;
        } else {
            analyzeNotListOfValues(model, column, m, node, notNode, latestByMultiColumn);
        }

        return ok;
    }

    private void analyzeNotListOfValues(
            IntrinsicModel model,
            CharSequence columnName,
            RecordMetadata m,
            ExpressionNode node,
            ExpressionNode notNode,
            boolean latestByMultiColumn
    ) {
        final int columnIndex = m.getColumnIndex(columnName);
        boolean newColumn = true;
        if (columnIsPreferredOrIndexedAndNotPartOfMultiColumnLatestBy(columnName, m, latestByMultiColumn)) {
            if (model.keyColumn != null
                    && (newColumn = !Chars.equals(model.keyColumn, columnName))
                    && m.getIndexValueBlockCapacity(columnIndex) <= m.getIndexValueBlockCapacity(model.keyColumn)) {
                return;
            }

            int i = node.paramCount - 1;
            tempKeys.clear();
            tempPos.clear();

            // collect and analyze values of indexed field
            // if any of values is not an indexed constant - bail out
            if (i == 1) {
                if (node.rhs == null || node.rhs.type != ExpressionNode.CONSTANT) {
                    return;
                }
                if (tempKeys.add(unquote(node.rhs.token))) {
                    tempPos.add(node.position);
                }
            } else {
                for (i--; i > -1; i--) {
                    ExpressionNode c = node.args.getQuick(i);
                    if (c.type != ExpressionNode.CONSTANT) {
                        return;
                    }

                    if (isNullKeyword(c.token)) {
                        if (tempKeys.add(null)) {
                            tempPos.add(c.position);
                        }
                    } else {
                        if (tempKeys.add(unquote(c.token))) {
                            tempPos.add(c.position);
                        }
                    }
                }
            }

            // clear values if this is new column
            // and reset intrinsic values on nodes associated with old column
            if (newColumn) {
                tempKeyExcludedValues.clear();
                tempKeyExcludedValuePos.clear();
                tempKeyExcludedValues.addAll(tempKeys);
                tempKeyExcludedValuePos.addAll(tempPos);
                revertProcessedNodes(keyExclNodes, model, columnName, notNode);
                return;
            } else {
                if (tempKeyExcludedValues.size() == 0) {
                    tempKeyExcludedValues.addAll(tempKeys);
                    tempKeyExcludedValuePos.addAll(tempPos);
                }
            }

            if (model.keySubQuery == null) {
                // calculate overlap of values
                replaceAllWithOverlap(model, false);

                keyExclNodes.add(notNode);
                notNode.intrinsicValue = IntrinsicModel.TRUE;
            }
        }
    }

    private void applyKeyExclusions(AliasTranslator translator, IntrinsicModel model) {
        if (model.keyColumn != null && tempKeyValues.size() > 0 && keyExclNodes.size() > 0) {
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
                    if (Chars.equals(column, model.keyColumn)) {
                        excludeKeyValue(model, val);
                        parent.intrinsicValue = IntrinsicModel.TRUE;
                        if (model.intrinsicValue == IntrinsicModel.FALSE) {
                            break;
                        }
                    }
                }

                if (node.paramCount > 2) {
                    ExpressionNode col = node.args.getQuick(node.paramCount - 1);
                    final CharSequence column = translator.translateAlias(col.token);
                    if (Chars.equals(column, model.keyColumn)) {
                        for (int j = node.paramCount - 2; j > -1; j--) {
                            ExpressionNode val = node.args.getQuick(j);
                            excludeKeyValue(model, val);
                            if (model.intrinsicValue == IntrinsicModel.FALSE) {
                                break OUT;
                            }
                        }
                        parent.intrinsicValue = IntrinsicModel.TRUE;
                    }
                }
            }
        }
        keyExclNodes.clear();
    }

    private void excludeKeyValue(IntrinsicModel model, ExpressionNode val) {
        final int index;
        if (isNullKeyword(val.token)) {
            index = tempKeyValues.removeNull();
            if (index > -1) {
                tempKeyValuePos.removeIndex(index);
            }
        } else {
            int keyIndex = Chars.isQuoted(val.token) ? tempKeyValues.keyIndex(val.token, 1, val.token.length() - 1) : tempKeyValues.keyIndex(val.token);
            if (keyIndex < 0) {
                index = tempKeyValues.getListIndexAt(keyIndex);
                tempKeyValues.removeAt(keyIndex);
            } else {
                index = -1;
            }
        }

        if (index > -1) {
            tempKeyValuePos.removeIndex(index);
        }

        if (tempKeyValues.size() == 0) {
            model.intrinsicValue = IntrinsicModel.FALSE;
        }
    }

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

    IntrinsicModel extract(
            AliasTranslator translator,
            ExpressionNode node,
            RecordMetadata m,
            CharSequence preferredKeyColumn,
            int timestampIndex,
            FunctionParser functionParser,
            RecordMetadata metadata,
            SqlExecutionContext executionContext,
            boolean latestByMultiColumn
    ) throws SqlException {
        this.tempKeyValues.clear();
        this.tempKeyExcludedValues.clear();
        this.tempKeyValuePos.clear();
        this.tempKeyExcludedValuePos.clear();

        this.timestamp = timestampIndex < 0 ? null : m.getColumnName(timestampIndex);
        this.preferredKeyColumn = preferredKeyColumn;

        IntrinsicModel model = models.next();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        if (removeAndIntrinsics(
                translator,
                model,
                node,
                m,
                functionParser,
                metadata,
                executionContext,
                latestByMultiColumn)) {
            createKeyValueBindVariables(model, functionParser, executionContext);
            return model;
        }
        ExpressionNode root = node;

        while (!stack.isEmpty() || node != null) {
            if (node != null) {
                if (isAndKeyword(node.token)) {
                    if (!removeAndIntrinsics(
                            translator,
                            model,
                            node.rhs,
                            m,
                            functionParser,
                            metadata,
                            executionContext,
                            latestByMultiColumn)) {
                        stack.push(node.rhs);
                    }
                    node = removeAndIntrinsics(
                            translator,
                            model,
                            node.lhs,
                            m,
                            functionParser,
                            metadata,
                            executionContext,
                            latestByMultiColumn) ? null : node.lhs;
                } else {
                    node = stack.poll();
                }
            } else {
                node = stack.poll();
            }
        }
        applyKeyExclusions(translator, model);
        model.filter = collapseIntrinsicNodes(root);
        createKeyValueBindVariables(model, functionParser, executionContext);
        return model;
    }

    private void createKeyValueBindVariables(
            IntrinsicModel model,
            FunctionParser functionParser,
            SqlExecutionContext executionContext
    ) throws SqlException {
        for (int i = 0, n = tempKeyValues.size(); i < n; i++) {
            Function func = createKeyValueBindVariable(
                    functionParser,
                    executionContext,
                    tempKeyValuePos.getQuick(i),
                    tempKeyValues.get(i));
            model.keyValueFuncs.add(func);
        }
        tempKeyValues.clear();
        tempKeyValuePos.clear();

        for (int i = 0, n = tempKeyExcludedValues.size(); i < n; i++) {
            Function func = createKeyValueBindVariable(
                    functionParser,
                    executionContext,
                    tempKeyExcludedValuePos.getQuick(i),
                    tempKeyExcludedValues.get(i));
            model.keyExcludedValueFuncs.add(func);
        }
        tempKeyExcludedValues.clear();
        tempKeyExcludedValuePos.clear();
    }

    private Function createKeyValueBindVariable(
            FunctionParser functionParser,
            SqlExecutionContext executionContext,
            int position,
            CharSequence value
    ) throws SqlException {
        Function func = functionParser.createBindVariable(executionContext, position, value);
        if (func.isRuntimeConstant() && ColumnType.isUndefined(func.getType())) {
            func.assignType(ColumnType.STRING, executionContext.getBindVariableService());
        }
        func.init(null, executionContext);
        return func;
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
        if (node == null) return null;

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        if (removeWithin(translator, node, metadata, functionParser, executionContext, prefixes)) {
            return collapseWithinNodes(node);
        }

        ExpressionNode root = node;
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
            try (Function f = functionParser.parseFunction(inArg, metadata, executionContext)) {
                if (isGeoHashConstFunction(f)) {
                    type = f.getType();
                    hash = GeoHashes.getGeoLong(type, f, null);
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
                    if (bits <= ColumnType.GEO_HASH_MAX_BITS_LENGTH) {
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

    private boolean isNull(ExpressionNode node) {
        return node == null || isNullKeyword(node.token);
    }

    private boolean isGeoHashConstFunction(Function fn) {
        return (fn instanceof AbstractGeoHashFunction) && fn.isConstant();
    }

    private ExpressionNode collapseWithinNodes(ExpressionNode node) {
        if (node == null || isWithinKeyword(node.token)) {
            return null;
        }
        node.lhs = collapseWithinNodes(collapseWithin0(node.lhs));
        node.rhs = collapseWithinNodes(collapseWithin0(node.rhs));
        return collapseWithin0(node);
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

    private boolean isTimestamp(ExpressionNode n) {
        return Chars.equalsNc(n.token, timestamp);
    }

    private long parseFullOrPartialDate(boolean equalsTo, ExpressionNode node, boolean isLo) throws NumericException {
        long ts;
        final int len = node.token.length();
        try {
            // Timestamp string
            ts = IntervalUtils.parseFloorPartialDate(node.token, 1, len - 1);
            if (!equalsTo) {
                ts += isLo ? 1 : -1;
            }
        } catch (NumericException e) {
            long inc = equalsTo ? 0 : isLo ? 1 : -1;
            ts = TimestampFormatUtils.tryParse(node.token, 1, node.token.length() - 1) + inc;
        }
        return ts;
    }

    private boolean removeAndIntrinsics(AliasTranslator translator,
                                        IntrinsicModel model,
                                        ExpressionNode node,
                                        RecordMetadata m,
                                        FunctionParser functionParser,
                                        RecordMetadata metadata,
                                        SqlExecutionContext executionContext,
                                        boolean latestByMultiColumn) throws SqlException {
        switch (intrinsicOps.get(node.token)) {
            case INTRINSIC_OP_IN:
                return analyzeIn(translator, model, node, m, functionParser, executionContext, latestByMultiColumn);
            case INTRINSIC_OP_GREATER:
                return analyzeGreater(model, node, false, functionParser, metadata, executionContext);
            case INTRINSIC_OP_GREATER_EQ:
                return analyzeGreater(model, node, true, functionParser, metadata, executionContext);
            case INTRINSIC_OP_LESS:
                return analyzeLess(model, node, false, functionParser, metadata, executionContext);
            case INTRINSIC_OP_LESS_EQ:
                return analyzeLess(model, node, true, functionParser, metadata, executionContext);
            case INTRINSIC_OP_EQUAL:
                return analyzeEquals(translator, model, node, m, functionParser, executionContext, latestByMultiColumn);
            case INTRINSIC_OP_NOT_EQ:
                return analyzeNotEquals(translator, model, node, m, functionParser, executionContext, latestByMultiColumn);
            case INTRINSIC_OP_NOT:
                return (isInKeyword(node.rhs.token) && analyzeNotIn(translator, model, node, m, functionParser, metadata, executionContext, latestByMultiColumn))
                        ||
                        (isBetweenKeyword(node.rhs.token) && analyzeNotBetween(translator, model, node, m, functionParser, metadata, executionContext, latestByMultiColumn));
            case INTRINSIC_OP_BETWEEN:
                return analyzeBetween(translator, model, node, m, functionParser, metadata, executionContext);
            default:
                return false;
        }
    }

    private void removeNodes(ExpressionNode b, ObjList<ExpressionNode> nodes) {
        tempNodes.clear();
        for (int i = 0, size = nodes.size(); i < size; i++) {
            ExpressionNode expressionNode = nodes.get(i);
            if ((expressionNode.lhs != null && Chars.equals(expressionNode.lhs.token, b.token)) || (expressionNode.rhs != null && Chars.equals(expressionNode.rhs.token, b.token))) {
                expressionNode.intrinsicValue = IntrinsicModel.TRUE;
                tempNodes.add(expressionNode);
            }
        }
        for (int i = 0, size = tempNodes.size(); i < size; i++) {
            nodes.remove(tempNodes.get(i));
        }
    }

    private void replaceAllWithOverlap(IntrinsicModel model, boolean includedValues) {
        CharSequenceHashSet values;
        IntList positions;
        if (includedValues) {
            values = tempKeyValues;
            positions = tempKeyValuePos;
        } else {
            values = tempKeyExcludedValues;
            positions = tempKeyExcludedValuePos;
        }
        tempK.clear();
        tempP.clear();
        for (int i = 0, k = tempKeys.size(); i < k; i++) {
            if (values.contains(tempKeys.get(i)) && tempK.add(tempKeys.get(i))) {
                tempP.add(tempPos.get(i));
            }
        }

        if (tempK.size() > 0) {
            values.clear();
            positions.clear();
            values.addAll(tempK);
            positions.addAll(tempP);
        } else {
            model.intrinsicValue = IntrinsicModel.FALSE;
        }
    }

    private void resetNodes() {
        for (int n = 0, k = keyNodes.size(); n < k; n++) {
            keyNodes.getQuick(n).intrinsicValue = IntrinsicModel.UNDEFINED;
        }
        keyNodes.clear();
        for (int n = 0, k = keyExclNodes.size(); n < k; n++) {
            keyExclNodes.getQuick(n).intrinsicValue = IntrinsicModel.UNDEFINED;
        }
        keyExclNodes.clear();
    }

    private boolean revertProcessedNodes(ObjList<ExpressionNode> nodes, IntrinsicModel model, CharSequence columnName, ExpressionNode node) {
        for (int n = 0, k = nodes.size(); n < k; n++) {
            nodes.getQuick(n).intrinsicValue = IntrinsicModel.UNDEFINED;
        }
        nodes.clear();
        model.keyColumn = columnName;
        nodes.add(node);
        node.intrinsicValue = IntrinsicModel.TRUE;
        return true;
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
    }
}

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

package io.questdb.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.model.AliasTranslator;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IntrinsicModel;
import io.questdb.std.*;
import io.questdb.std.microtime.TimestampFormatUtils;
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
    private static final CharSequenceIntHashMap intrinsicOps = new CharSequenceIntHashMap();
    private final ArrayDeque<ExpressionNode> stack = new ArrayDeque<>();
    private final ObjList<ExpressionNode> keyNodes = new ObjList<>();
    private final ObjList<ExpressionNode> keyExclNodes = new ObjList<>();
    private final ObjList<ExpressionNode> tempNodes = new ObjList<>();
    private final ObjectPool<IntrinsicModel> models = new ObjectPool<>(IntrinsicModel.FACTORY, 8);
    private final CharSequenceHashSet tempKeys = new CharSequenceHashSet();
    private final IntList tempPos = new IntList();
    private final CharSequenceHashSet tempK = new CharSequenceHashSet();
    private final IntList tempP = new IntList();
    private final ObjectPool<FlyweightCharSequence> csPool = new ObjectPool<>(FlyweightCharSequence.FACTORY, 64);
    private CharSequence timestamp;
    private CharSequence preferredKeyColumn;

    @Override
    public void clear() {
        this.models.clear();
        this.stack.clear();
        this.keyNodes.clear();
        this.keyExclNodes.clear();
        this.csPool.clear();
        this.tempNodes.clear();
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

    private boolean analyzeEquals(AliasTranslator translator, IntrinsicModel model, ExpressionNode node, RecordMetadata m) throws SqlException {
        checkNodeValid(node);
        return analyzeEquals0(translator, model, node, node.lhs, node.rhs, m) || analyzeEquals0(translator, model, node, node.rhs, node.lhs, m);
    }

    private boolean analyzeEquals0(AliasTranslator translator, IntrinsicModel model, ExpressionNode node, ExpressionNode a, ExpressionNode b, RecordMetadata m) throws SqlException {
        if (nodesEqual(a, b)) {
            node.intrinsicValue = IntrinsicModel.TRUE;
            return true;
        }

        if (a.type == ExpressionNode.LITERAL && b.type == ExpressionNode.CONSTANT) {
            if (isTimestamp(a)) {
                model.intersectIntervals(b.token, 1, b.token.length() - 1, b.position);
                node.intrinsicValue = IntrinsicModel.TRUE;
                return true;
            } else {
                CharSequence column = translator.translateAlias(a.token);
                int index = m.getColumnIndexQuiet(column);
                if (index == -1) {
                    throw SqlException.invalidColumn(a.position, a.token);
                }

                switch (m.getColumnType(index)) {
                    case ColumnType.SYMBOL:
                    case ColumnType.STRING:
                    case ColumnType.LONG:
                    case ColumnType.INT:
                        final boolean preferred = Chars.equalsIgnoreCaseNc(preferredKeyColumn, column);
                        final boolean indexed = m.isColumnIndexed(index);
                        if (preferred || (indexed && preferredKeyColumn == null)) {
                            CharSequence value = isNullKeyword(b.token) ? null : unquote(b.token);
                            if (Chars.equalsIgnoreCaseNc(model.keyColumn, column)) {
                                // compute overlap of values
                                // if values do overlap, keep only our value
                                // otherwise invalidate entire model
                                if (model.keyValues.contains(value)) {
                                    // when we have "x in ('a,'b') and x = 'a')" the x='b' can never happen
                                    // so we have to clear all other key values
                                    if (model.keyValues.size() > 1) {
                                        model.keyValues.clear();
                                        model.keyValuePositions.clear();
                                        model.keyValues.add(value);
                                        model.keyValuePositions.add(b.position);
                                        node.intrinsicValue = IntrinsicModel.TRUE;
                                    }
                                } else {
                                    if (model.keyExcludedValues.contains(value)) {
                                        if (model.keyExcludedValues.size() > 1) {
                                            int removedIndex = model.keyExcludedValues.remove(value);
                                            if (removedIndex > -1) {
                                                model.keyExcludedValuePositions.removeIndex(index);
                                            }
                                        } else {
                                            model.keyExcludedValues.clear();
                                            model.keyExcludedValuePositions.clear();
                                        }
                                        removeNodes(b, keyExclNodes);
                                    }
                                    node.intrinsicValue = IntrinsicModel.TRUE;
                                    model.intrinsicValue = IntrinsicModel.FALSE;
                                    return false;
                                }
                            } else if (model.keyColumn == null || m.getIndexValueBlockCapacity(index) > m.getIndexValueBlockCapacity(model.keyColumn)) {
                                model.keyColumn = column;
                                model.keyValues.clear();
                                model.keyValuePositions.clear();
                                model.keyExcludedValues.clear();
                                model.keyExcludedValuePositions.clear();
                                model.keyValues.add(value);
                                model.keyValuePositions.add(b.position);
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

    private boolean analyzeGreater(IntrinsicModel model, ExpressionNode node, boolean equalsTo) throws SqlException {
        checkNodeValid(node);

        if (nodesEqual(node.lhs, node.rhs)) {
            model.intrinsicValue = IntrinsicModel.FALSE;
            return false;
        }

        if (timestamp == null) {
            return false;
        }

        if (node.lhs.type == ExpressionNode.LITERAL && Chars.equals(node.lhs.token, timestamp)) {

            if (node.rhs.type != ExpressionNode.CONSTANT) {
                return false;
            }

            try {
                long lo = parseFullOrPartialDate(equalsTo, node.rhs, true);
                model.intersectIntervals(lo, Long.MAX_VALUE);
                node.intrinsicValue = IntrinsicModel.TRUE;
                return true;
            } catch (NumericException e) {
                throw SqlException.invalidDate(node.rhs.position);
            }
        }

        if (node.rhs.type == ExpressionNode.LITERAL && Chars.equals(node.rhs.token, timestamp)) {

            if (node.lhs.type != ExpressionNode.CONSTANT) {
                return false;
            }

            try {
                long hi = parseFullOrPartialDate(equalsTo, node.lhs, false);
                model.intersectIntervals(Long.MIN_VALUE, hi);
                node.intrinsicValue = IntrinsicModel.TRUE;
                return true;
            } catch (NumericException e) {
                throw SqlException.invalidDate(node.lhs.position);
            }
        }
        return false;
    }

    private boolean analyzeIn(AliasTranslator translator, IntrinsicModel model, ExpressionNode node, RecordMetadata metadata) throws SqlException {

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
        return analyzeInInterval(model, col, node, false)
                || analyzeListOfValues(model, column, metadata, node)
                || analyzeInLambda(model, column, metadata, node);
    }

    private boolean analyzeInInterval(IntrinsicModel model, ExpressionNode col, ExpressionNode in, boolean isNegated) throws SqlException {
        if (!isTimestamp(col)) {
            return false;
        }

        if (in.paramCount > 3) {
            throw SqlException.$(in.args.getQuick(0).position, "Too many args");
        }

        if (in.paramCount < 3) {
            throw SqlException.$(in.position, "Too few args");
        }

        ExpressionNode lo = in.args.getQuick(1);
        ExpressionNode hi = in.args.getQuick(0);

        if (lo.type == ExpressionNode.CONSTANT && hi.type == ExpressionNode.CONSTANT) {
            long loMillis;
            long hiMillis;

            try {
                loMillis = TimestampFormatUtils.tryParse(lo.token, 1, lo.token.length() - 1);
            } catch (NumericException ignore) {
                throw SqlException.invalidDate(lo.position);
            }

            try {
                hiMillis = TimestampFormatUtils.tryParse(hi.token, 1, hi.token.length() - 1);
            } catch (NumericException ignore) {
                throw SqlException.invalidDate(hi.position);
            }

            if (isNegated) {
                model.subtractIntervals(loMillis, hiMillis);
            } else {
                model.intersectIntervals(loMillis, hiMillis);
            }
            in.intrinsicValue = IntrinsicModel.TRUE;
            return true;
        }
        return false;
    }

    private boolean analyzeInLambda(IntrinsicModel model, CharSequence columnName, RecordMetadata meta, ExpressionNode node) throws SqlException {
        int columnIndex = meta.getColumnIndex(columnName);
        boolean preferred = Chars.equalsIgnoreCaseNc(preferredKeyColumn, columnName);

        if (preferred || (preferredKeyColumn == null && meta.isColumnIndexed(columnIndex))) {
            if (preferredKeyColumn != null && !Chars.equalsIgnoreCase(columnName, preferredKeyColumn)) {
                return false;
            }

            if (node.rhs == null || node.rhs.type != ExpressionNode.QUERY) {
                return false;
            }

            // check if we already have indexed column and it is of worse selectivity
            if (model.keyColumn != null
                    && (!Chars.equalsIgnoreCase(model.keyColumn, columnName))
                    && meta.getIndexValueBlockCapacity(columnIndex) <= meta.getIndexValueBlockCapacity(model.keyColumn)) {
                return false;
            }

            if ((Chars.equalsIgnoreCaseNc(model.keyColumn, columnName) && model.keySubQuery != null) || node.paramCount > 2) {
                throw SqlException.$(node.position, "Multiple lambda expressions not supported");
            }

            model.keyValues.clear();
            model.keyValuePositions.clear();
            model.keyValuePositions.add(node.position);
            model.keySubQuery = node.rhs.queryModel;

            // revert previously processed nodes
            return revertProcessedNodes(keyNodes, model, columnName, node);
        }
        return false;
    }

    private boolean analyzeLess(IntrinsicModel model, ExpressionNode node, boolean equalsTo) throws SqlException {

        checkNodeValid(node);

        if (nodesEqual(node.lhs, node.rhs)) {
            model.intrinsicValue = IntrinsicModel.FALSE;
            return false;
        }

        if (timestamp == null) {
            return false;
        }

        if (node.lhs.type == ExpressionNode.LITERAL && Chars.equals(node.lhs.token, timestamp)) {
            try {

                if (node.rhs.type != ExpressionNode.CONSTANT) {
                    return false;
                }
                long hi = parseFullOrPartialDate(equalsTo, node.rhs, false);
                model.intersectIntervals(Long.MIN_VALUE, hi);
                node.intrinsicValue = IntrinsicModel.TRUE;
                return true;
            } catch (NumericException e) {
                throw SqlException.invalidDate(node.rhs.position);
            }
        }

        if (node.rhs.type == ExpressionNode.LITERAL && Chars.equals(node.rhs.token, timestamp)) {
            try {
                if (node.lhs.type != ExpressionNode.CONSTANT) {
                    return false;
                }
                long lo = parseFullOrPartialDate(equalsTo, node.lhs, true);
                model.intersectIntervals(lo, Long.MAX_VALUE);
                node.intrinsicValue = IntrinsicModel.TRUE;
                return true;
            } catch (NumericException e) {
                throw SqlException.invalidDate(node.lhs.position);
            }
        }
        return false;
    }

    private boolean analyzeListOfValues(IntrinsicModel model, CharSequence columnName, RecordMetadata meta, ExpressionNode node) {
        final int columnIndex = meta.getColumnIndex(columnName);
        boolean newColumn = true;
        boolean preferred = Chars.equalsIgnoreCaseNc(preferredKeyColumn, columnName);

        if (preferred || (preferredKeyColumn == null && meta.isColumnIndexed(columnIndex))) {

            // check if we already have indexed column and it is of worse selectivity
            // "preferred" is an unfortunate name, this column is from "latest by" clause, I should name it better
            //
            if (model.keyColumn != null
                    && (newColumn = !Chars.equals(model.keyColumn, columnName))
                    && meta.getIndexValueBlockCapacity(columnIndex) <= meta.getIndexValueBlockCapacity(model.keyColumn)) {
                return false;
            }


            int i = node.paramCount - 1;
            tempKeys.clear();
            tempPos.clear();

            // collect and analyze values of indexed field
            // if any of values is not an indexed constant - bail out
            if (i == 1) {
                if (node.rhs == null || node.rhs.type != ExpressionNode.CONSTANT) {
                    return false;
                }
                if (tempKeys.add(unquote(node.rhs.token))) {
                    tempPos.add(node.position);
                }
            } else {
                for (i--; i > -1; i--) {
                    ExpressionNode c = node.args.getQuick(i);
                    if (c.type != ExpressionNode.CONSTANT) {
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
                model.keyValues.clear();
                model.keyValuePositions.clear();
                model.keyValues.addAll(tempKeys);
                model.keyValuePositions.addAll(tempPos);
                return revertProcessedNodes(keyNodes, model, columnName, node);
            } else {
                if (model.keyValues.size() == 0) {
                    model.keyValues.addAll(tempKeys);
                    model.keyValuePositions.addAll(tempPos);
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

    private boolean analyzeNotEquals(AliasTranslator translator, IntrinsicModel model, ExpressionNode node, RecordMetadata m) throws SqlException {
        checkNodeValid(node);
        return analyzeNotEquals0(translator, model, node, node.lhs, node.rhs, m)
                || analyzeNotEquals0(translator, model, node, node.rhs, node.lhs, m);
    }

    private boolean analyzeNotEquals0(AliasTranslator translator, IntrinsicModel model, ExpressionNode node, ExpressionNode a, ExpressionNode b, RecordMetadata m) throws SqlException {
        if (Chars.equals(a.token, b.token)) {
            model.intrinsicValue = IntrinsicModel.FALSE;
            return true;
        }

        if (a.type == ExpressionNode.LITERAL && b.type == ExpressionNode.CONSTANT) {
            if (isTimestamp(a)) {
                model.subtractIntervals(b.token, 1, b.token.length() - 1, b.position);
                node.intrinsicValue = IntrinsicModel.TRUE;
                return true;
            } else {
                CharSequence column = translator.translateAlias(a.token);
                int index = m.getColumnIndexQuiet(column);
                if (index == -1) {
                    throw SqlException.invalidColumn(a.position, a.token);
                }

                switch (m.getColumnType(index)) {
                    case ColumnType.SYMBOL:
                    case ColumnType.STRING:
                    case ColumnType.LONG:
                    case ColumnType.INT:
                        if (m.isColumnIndexed(index)) {
                            final boolean preferred = Chars.equalsIgnoreCaseNc(preferredKeyColumn, column);
                            final boolean indexed = m.isColumnIndexed(index);
                            if (indexed && preferredKeyColumn == null) {
                                CharSequence value = isNullKeyword(b.token) ? null : unquote(b.token);
                                if (Chars.equalsIgnoreCaseNc(model.keyColumn, column)) {
                                    if (model.keyExcludedValues.contains(value)) {
                                        // when we have "x not in ('a,'b') and x != 'a')" the x='b' can never happen
                                        // so we have to clear all other key values
                                        if (model.keyExcludedValues.size() > 1) {
                                            model.keyExcludedValues.clear();
                                            model.keyExcludedValuePositions.clear();
                                            model.keyExcludedValues.add(value);
                                            model.keyExcludedValuePositions.add(b.position);
                                            node.intrinsicValue = IntrinsicModel.TRUE;
                                            return true;
                                        }
                                    } else {
                                        if (model.keyValues.contains(value)) {
                                            if (model.keyValues.size() > 1) {
                                                int removedIndex = model.keyValues.remove(value);
                                                if (removedIndex > -1) {
                                                    model.keyValuePositions.removeIndex(index);
                                                }
                                                model.keyValuePositions.remove(b.position);
                                            } else {
                                                model.keyValues.clear();
                                                model.keyValuePositions.clear();
                                            }
                                            removeNodes(b, keyNodes);
                                        }
                                        node.intrinsicValue = IntrinsicModel.TRUE;
                                        model.intrinsicValue = IntrinsicModel.FALSE;
                                        return false;
                                    }
                                } else if (model.keyColumn == null || m.getIndexValueBlockCapacity(index) > m.getIndexValueBlockCapacity(model.keyColumn)) {
                                    model.keyColumn = column;
                                    model.keyValues.clear();
                                    model.keyValuePositions.clear();
                                    model.keyExcludedValues.clear();
                                    model.keyExcludedValuePositions.clear();
                                    model.keyExcludedValues.add(value);
                                    model.keyExcludedValuePositions.add(b.position);
                                    resetNodes();
                                    node.intrinsicValue = IntrinsicModel.TRUE;
                                }
                                keyExclNodes.add(node);
                                return true;
                            } else if (preferred) {
                                keyExclNodes.add(node);
                                return false;
                            }
                        }
                        return false;
                    default:
                        break;
                }
            }
        }
        return false;
    }

    private boolean analyzeNotIn(AliasTranslator translator, IntrinsicModel model, ExpressionNode notNode, RecordMetadata m) throws SqlException {

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

        boolean ok = analyzeInInterval(model, col, node, true);
        if (ok) {
            notNode.intrinsicValue = IntrinsicModel.TRUE;
        } else {
            analyzeNotListOfValues(model, column, m, node, notNode);
        }

        return ok;
    }

    private void analyzeNotListOfValues(IntrinsicModel model, CharSequence columnName, RecordMetadata meta, ExpressionNode node, ExpressionNode notNode) {
        final int columnIndex = meta.getColumnIndex(columnName);
        boolean newColumn = true;
        boolean preferred = Chars.equalsIgnoreCaseNc(preferredKeyColumn, columnName);

        if (preferred || (preferredKeyColumn == null && meta.isColumnIndexed(columnIndex))) {


            if (model.keyColumn != null
                    && (newColumn = !Chars.equals(model.keyColumn, columnName))
                    && meta.getIndexValueBlockCapacity(columnIndex) <= meta.getIndexValueBlockCapacity(model.keyColumn)) {
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
                model.keyExcludedValues.clear();
                model.keyExcludedValuePositions.clear();
                model.keyExcludedValues.addAll(tempKeys);
                model.keyExcludedValuePositions.addAll(tempPos);
                revertProcessedNodes(keyExclNodes, model, columnName, notNode);
                return;
            } else {
                if (model.keyExcludedValues.size() == 0) {
                    model.keyExcludedValues.addAll(tempKeys);
                    model.keyExcludedValuePositions.addAll(tempPos);
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
        if (model.keyColumn != null && model.keyValues.size() > 0 && keyExclNodes.size() > 0) {
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
                        model.excludeValue(val);
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
                            model.excludeValue(val);
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
            int timestampIndex
    ) throws SqlException {
        this.timestamp = timestampIndex < 0 ? null : m.getColumnName(timestampIndex);
        this.preferredKeyColumn = preferredKeyColumn;

        IntrinsicModel model = models.next();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        if (removeAndIntrinsics(translator, model, node, m)) {
            return model;
        }
        ExpressionNode root = node;

        while (!stack.isEmpty() || node != null) {
            if (node != null) {
                if (isAndKeyword(node.token)) {
                    if (!removeAndIntrinsics(translator, model, node.rhs, m)) {
                        stack.push(node.rhs);
                    }
                    node = removeAndIntrinsics(translator, model, node.lhs, m) ? null : node.lhs;
                } else {
                    node = stack.poll();
                }
            } else {
                node = stack.poll();
            }
        }
        applyKeyExclusions(translator, model);
        model.filter = collapseIntrinsicNodes(root);
        return model;
    }

    private boolean isTimestamp(ExpressionNode n) {
        return timestamp != null && Chars.equals(timestamp, n.token);
    }

    private long parseFullOrPartialDate(boolean equalsTo, ExpressionNode node, boolean isLo) throws NumericException {
        long ts;
        final int len = node.token.length();
        if (len - 2 < 20) {
            if (isLo) {
                if (equalsTo) {
                    ts = IntrinsicModel.parseFloorPartialDate(node.token, 1, len - 1);
                } else {
                    ts = IntrinsicModel.parseCCPartialDate(node.token, 1, len - 1);
                }
            } else {
                if (equalsTo) {
                    ts = IntrinsicModel.parseCCPartialDate(node.token, 1, len - 1) - 1;
                } else {
                    ts = IntrinsicModel.parseFloorPartialDate(node.token, 1, len - 1) - 1;
                }
            }
        } else {
            long inc = equalsTo ? 0 : isLo ? 1 : -1;
            ts = TimestampFormatUtils.tryParse(node.token, 1, node.token.length() - 1) + inc;
        }
        return ts;
    }

    private boolean removeAndIntrinsics(AliasTranslator translator, IntrinsicModel model, ExpressionNode node, RecordMetadata m) throws SqlException {
        switch (intrinsicOps.get(node.token)) {
            case INTRINSIC_OP_IN:
                return analyzeIn(translator, model, node, m);
            case INTRINSIC_OP_GREATER:
                return analyzeGreater(model, node, false);
            case INTRINSIC_OP_GREATER_EQ:
                return analyzeGreater(model, node, true);
            case INTRINSIC_OP_LESS:
                return analyzeLess(model, node, false);
            case INTRINSIC_OP_LESS_EQ:
                return analyzeLess(model, node, true);
            case INTRINSIC_OP_EQUAL:
                return analyzeEquals(translator, model, node, m);
            case INTRINSIC_OP_NOT_EQ:
                return analyzeNotEquals(translator, model, node, m);
            case INTRINSIC_OP_NOT:
                return isInKeyword(node.rhs.token) && analyzeNotIn(translator, model, node, m);
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
            values = model.keyValues;
            positions = model.keyValuePositions;
        } else {
            values = model.keyExcludedValues;
            positions = model.keyExcludedValuePositions;
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
    }
}

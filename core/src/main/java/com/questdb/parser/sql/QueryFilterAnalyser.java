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

package com.questdb.parser.sql;

import com.questdb.ex.ParserException;
import com.questdb.parser.sql.model.AliasTranslator;
import com.questdb.parser.sql.model.ExprNode;
import com.questdb.parser.sql.model.IntrinsicModel;
import com.questdb.parser.sql.model.IntrinsicValue;
import com.questdb.std.*;
import com.questdb.std.str.FlyweightCharSequence;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.store.ColumnType;
import com.questdb.store.RecordColumnMetadata;
import com.questdb.store.RecordMetadata;

import java.util.ArrayDeque;

final class QueryFilterAnalyser {

    private final ArrayDeque<ExprNode> stack = new ArrayDeque<>();
    private final FlyweightCharSequence quoteEraser = new FlyweightCharSequence();
    private final ObjList<ExprNode> keyNodes = new ObjList<>();
    private final ObjList<ExprNode> keyExclNodes = new ObjList<>();
    private final ObjectPool<IntrinsicModel> models = new ObjectPool<>(IntrinsicModel.FACTORY, 8);
    private final CharSequenceHashSet tempKeys = new CharSequenceHashSet();
    private final IntList tempPos = new IntList();
    private final CharSequenceHashSet tempK = new CharSequenceHashSet();
    private final IntList tempP = new IntList();
    private String timestamp;
    private String preferredKeyColumn;

    private static void checkNodeValid(ExprNode node) throws ParserException {
        if (node.lhs == null || node.rhs == null) {
            throw QueryError.$(node.position, "Argument expected");
        }
    }

    private boolean analyzeEquals(AliasTranslator translator, IntrinsicModel model, ExprNode node, RecordMetadata m) throws ParserException {
        checkNodeValid(node);
        return analyzeEquals0(translator, model, node, node.lhs, node.rhs, m) || analyzeEquals0(translator, model, node, node.rhs, node.lhs, m);
    }

    private boolean analyzeEquals0(AliasTranslator translator, IntrinsicModel model, ExprNode node, ExprNode a, ExprNode b, RecordMetadata m) throws ParserException {
        if (Chars.equals(a.token, b.token)) {
            node.intrinsicValue = IntrinsicValue.TRUE;
            return true;
        }

        if (a.type == ExprNode.LITERAL && b.type == ExprNode.CONSTANT) {
            if (isTimestamp(a)) {
                CharSequence seq = quoteEraser.ofQuoted(b.token);
                model.intersectIntervals(seq, 0, seq.length(), b.position);
                node.intrinsicValue = IntrinsicValue.TRUE;
                return true;
            } else {
                CharSequence column = translator.translateAlias(a.token);
                int index = m.getColumnIndexQuiet(column);
                if (index == -1) {
                    throw QueryError.invalidColumn(a.position, a.token);
                }
                RecordColumnMetadata meta = m.getColumnQuick(index);

                switch (meta.getType()) {
                    case ColumnType.SYMBOL:
                    case ColumnType.STRING:
                    case ColumnType.LONG:
                    case ColumnType.INT:
                        if (meta.isIndexed()) {

                            // check if we are limited by preferred column
                            if (preferredKeyColumn != null && !Chars.equals(preferredKeyColumn, column)) {
                                return false;
                            }

                            boolean newColumn = true;
                            // check if we already have indexed column and it is of worse selectivity
                            if (model.keyColumn != null
                                    && (newColumn = !Chars.equals(model.keyColumn, column))
                                    && meta.getBucketCount() <= m.getColumn(model.keyColumn).getBucketCount()) {
                                return false;
                            }

                            String value = Chars.equals("null", b.token) ? null : Chars.stripQuotes(b.token);
                            if (newColumn) {
                                model.keyColumn = column.toString();
                                model.keyValues.clear();
                                model.keyValuePositions.clear();
                                model.keyValues.add(value);
                                model.keyValuePositions.add(b.position);
                                for (int n = 0, k = keyNodes.size(); n < k; n++) {
                                    keyNodes.getQuick(n).intrinsicValue = IntrinsicValue.UNDEFINED;
                                }
                                keyNodes.clear();
                            } else {
                                // compute overlap of values
                                // if values do overlap, keep only our value
                                // otherwise invalidate entire model
                                if (model.keyValues.contains(value)) {
                                    model.keyValues.clear();
                                    model.keyValuePositions.clear();
                                    model.keyValues.add(value);
                                    model.keyValuePositions.add(b.position);
                                } else {
                                    model.intrinsicValue = IntrinsicValue.FALSE;
                                    return false;
                                }
                            }

                            keyNodes.add(node);
                            node.intrinsicValue = IntrinsicValue.TRUE;
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

    private boolean analyzeGreater(IntrinsicModel model, ExprNode node, int increment) throws ParserException {
        checkNodeValid(node);

        if (Chars.equals(node.lhs.token, node.rhs.token)) {
            model.intrinsicValue = IntrinsicValue.FALSE;
            return false;
        }

        if (timestamp == null) {
            return false;
        }

        if (node.lhs.type == ExprNode.LITERAL && node.lhs.token.equals(timestamp)) {

            if (node.rhs.type != ExprNode.CONSTANT) {
                return false;
            }

            try {
                model.intersectIntervals(DateFormatUtils.tryParse(quoteEraser.ofQuoted(node.rhs.token)) + increment, Long.MAX_VALUE);
                node.intrinsicValue = IntrinsicValue.TRUE;
                return true;
            } catch (NumericException e) {
                throw QueryError.$(node.rhs.position, "Not a date");
            }
        }

        if (node.rhs.type == ExprNode.LITERAL && node.rhs.token.equals(timestamp)) {

            if (node.lhs.type != ExprNode.CONSTANT) {
                return false;
            }

            try {
                model.intersectIntervals(Long.MIN_VALUE, DateFormatUtils.tryParse(quoteEraser.ofQuoted(node.lhs.token)) - increment);
                return true;
            } catch (NumericException e) {
                throw QueryError.$(node.lhs.position, "Not a date");
            }
        }
        return false;
    }

    private boolean analyzeIn(AliasTranslator translator, IntrinsicModel model, ExprNode node, RecordMetadata metadata) throws ParserException {

        if (node.paramCount < 2) {
            throw QueryError.$(node.position, "Too few arguments for 'in'");
        }

        ExprNode col = node.paramCount < 3 ? node.lhs : node.args.getLast();

        if (col.type != ExprNode.LITERAL) {
            throw QueryError.$(col.position, "Column name expected");
        }

        String column = translator.translateAlias(col.token).toString();

        if (metadata.getColumnIndexQuiet(column) == -1) {
            throw QueryError.invalidColumn(col.position, col.token);
        }
        return analyzeInInterval(model, col, node)
                || analyzeListOfValues(model, column, metadata, node)
                || analyzeInLambda(model, column, metadata, node);
    }

    private boolean analyzeInInterval(IntrinsicModel model, ExprNode col, ExprNode in) throws ParserException {
        if (!isTimestamp(col)) {
            return false;
        }

        if (in.paramCount > 3) {
            throw QueryError.$(in.args.getQuick(0).position, "Too many args");
        }

        if (in.paramCount < 3) {
            throw QueryError.$(in.position, "Too few args");
        }

        ExprNode lo = in.args.getQuick(1);
        ExprNode hi = in.args.getQuick(0);

        if (lo.type == ExprNode.CONSTANT && hi.type == ExprNode.CONSTANT) {
            long loMillis;
            long hiMillis;

            try {
                loMillis = DateFormatUtils.tryParse(quoteEraser.ofQuoted(lo.token));
            } catch (NumericException ignore) {
                throw QueryError.$(lo.position, "Unknown date format");
            }

            try {
                hiMillis = DateFormatUtils.tryParse(quoteEraser.ofQuoted(hi.token));
            } catch (NumericException ignore) {
                throw QueryError.$(hi.position, "Unknown date format");
            }

            model.intersectIntervals(loMillis, hiMillis);
            in.intrinsicValue = IntrinsicValue.TRUE;
            return true;
        }
        return false;
    }

    private boolean analyzeInLambda(IntrinsicModel model, String col, RecordMetadata meta, ExprNode node) throws ParserException {
        RecordColumnMetadata colMeta = meta.getColumn(col);
        if (colMeta.isIndexed()) {
            if (preferredKeyColumn != null && !col.equals(preferredKeyColumn)) {
                return false;
            }

            if (node.rhs == null || node.rhs.type != ExprNode.LAMBDA) {
                return false;
            }

            // check if we already have indexed column and it is of worse selectivity
            if (model.keyColumn != null
                    && (!model.keyColumn.equals(col))
                    && colMeta.getBucketCount() <= meta.getColumn(model.keyColumn).getBucketCount()) {
                // todo: no test hit
                return false;
            }

            // todo: this is going to fail if "in" args are functions
            if ((col.equals(model.keyColumn) && model.keyValuesIsLambda) || node.paramCount > 2) {
                throw QueryError.$(node.position, "Multiple lambda expressions not supported");
            }

            model.keyValues.clear();
            model.keyValuePositions.clear();
            model.keyValues.add(Chars.stripQuotes(node.rhs.token));
            model.keyValuePositions.add(node.position);
            model.keyValuesIsLambda = true;

            // revert previously processed nodes
            for (int n = 0, k = keyNodes.size(); n < k; n++) {
                keyNodes.getQuick(n).intrinsicValue = IntrinsicValue.UNDEFINED;
            }
            keyNodes.clear();
            model.keyColumn = col;
            keyNodes.add(node);
            node.intrinsicValue = IntrinsicValue.TRUE;
            return true;
        }
        return false;
    }

    private boolean analyzeLess(IntrinsicModel model, ExprNode node, int inc) throws ParserException {

        checkNodeValid(node);

        if (Chars.equals(node.lhs.token, node.rhs.token)) {
            model.intrinsicValue = IntrinsicValue.FALSE;
            return false;
        }

        if (timestamp == null) {
            return false;
        }

        if (node.lhs.type == ExprNode.LITERAL && node.lhs.token.equals(timestamp)) {
            try {

                if (node.rhs.type != ExprNode.CONSTANT) {
                    return false;
                }

                long hi = DateFormatUtils.tryParse(quoteEraser.ofQuoted(node.rhs.token)) - inc;
                model.intersectIntervals(Long.MIN_VALUE, hi);
                node.intrinsicValue = IntrinsicValue.TRUE;
                return true;
            } catch (NumericException e) {
                throw QueryError.$(node.rhs.position, "Not a date");
            }
        }

        if (node.rhs.type == ExprNode.LITERAL && node.rhs.token.equals(timestamp)) {
            try {
                if (node.lhs.type != ExprNode.CONSTANT) {
                    return false;
                }

                long lo = DateFormatUtils.tryParse(quoteEraser.ofQuoted(node.lhs.token)) + inc;
                model.intersectIntervals(lo, Long.MAX_VALUE);
                node.intrinsicValue = IntrinsicValue.TRUE;
                return true;
            } catch (NumericException e) {
                throw QueryError.$(node.lhs.position, "Not a date");
            }
        }
        return false;
    }

    private boolean analyzeListOfValues(IntrinsicModel model, String col, RecordMetadata meta, ExprNode node) {
        RecordColumnMetadata colMeta = meta.getColumn(col);
        if (colMeta.isIndexed()) {
            boolean newColumn = true;

            if (preferredKeyColumn != null && !col.equals(preferredKeyColumn)) {
                return false;
            }

            // check if we already have indexed column and it is of worse selectivity
            if (model.keyColumn != null
                    && (newColumn = !model.keyColumn.equals(col))
                    && colMeta.getBucketCount() <= meta.getColumn(model.keyColumn).getBucketCount()) {
                return false;
            }


            int i = node.paramCount - 1;
            tempKeys.clear();
            tempPos.clear();

            // collect and analyze values of indexed field
            // if any of values is not an indexed constant - bail out
            if (i == 1) {
                if (node.rhs == null || node.rhs.type != ExprNode.CONSTANT) {
                    return false;
                }
                if (tempKeys.add(Chars.stripQuotes(node.rhs.token))) {
                    tempPos.add(node.position);
                }
            } else {
                for (i--; i > -1; i--) {
                    ExprNode c = node.args.getQuick(i);
                    if (c.type != ExprNode.CONSTANT) {
                        return false;
                    }
                    if (tempKeys.add(Chars.stripQuotes(c.token))) {
                        tempPos.add(c.position);
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
                for (int n = 0, k = keyNodes.size(); n < k; n++) {
                    keyNodes.getQuick(n).intrinsicValue = IntrinsicValue.UNDEFINED;
                }
                keyNodes.clear();
                model.keyColumn = col;
                keyNodes.add(node);
                node.intrinsicValue = IntrinsicValue.TRUE;
                return true;

            } else if (!model.keyValuesIsLambda) {
                // calculate overlap of values
                replaceAllWithOverlap(model);

                keyNodes.add(node);
                node.intrinsicValue = IntrinsicValue.TRUE;
                return true;
            }
        }
        return false;
    }

    private boolean analyzeNotEquals(AliasTranslator translator, IntrinsicModel model, ExprNode node, RecordMetadata m) throws ParserException {
        checkNodeValid(node);
        return analyzeNotEquals0(translator, model, node, node.lhs, node.rhs, m)
                || analyzeNotEquals0(translator, model, node, node.rhs, node.lhs, m);
    }

    private boolean analyzeNotEquals0(AliasTranslator translator, IntrinsicModel model, ExprNode node, ExprNode a, ExprNode b, RecordMetadata m) throws ParserException {

        if (Chars.equals(a.token, b.token)) {
            model.intrinsicValue = IntrinsicValue.FALSE;
            return true;
        }

        if (a.type == ExprNode.LITERAL && b.type == ExprNode.CONSTANT) {
            if (isTimestamp(a)) {
                CharSequence seq = quoteEraser.ofQuoted(b.token);
                model.subtractIntervals(seq, 0, seq.length(), b.position);
                node.intrinsicValue = IntrinsicValue.TRUE;
                return true;
            } else {
                String column = translator.translateAlias(a.token).toString();
                int index = m.getColumnIndexQuiet(column);
                if (index == -1) {
                    throw QueryError.invalidColumn(a.position, a.token);
                }
                RecordColumnMetadata meta = m.getColumnQuick(index);

                switch (meta.getType()) {
                    case ColumnType.SYMBOL:
                    case ColumnType.STRING:
                    case ColumnType.LONG:
                    case ColumnType.INT:
                        if (meta.isIndexed()) {

                            // check if we are limited by preferred column
                            if (preferredKeyColumn != null && !preferredKeyColumn.equals(column)) {
                                return false;
                            }

                            keyExclNodes.add(node);
                            return false;
                        }
                        break;
                    default:
                        break;
                }
            }

        }
        return false;
    }

    private boolean analyzeNotIn(AliasTranslator translator, IntrinsicModel model, ExprNode notNode, RecordMetadata m) throws ParserException {

        ExprNode node = notNode.rhs;

        if (node.paramCount < 2) {
            throw QueryError.$(node.position, "Too few arguments for 'in'");
        }

        ExprNode col = node.paramCount < 3 ? node.lhs : node.args.getLast();

        if (col.type != ExprNode.LITERAL) {
            throw QueryError.$(col.position, "Column name expected");
        }

        String column = translator.translateAlias(col.token).toString();

        if (m.getColumnIndexQuiet(column) == -1) {
            throw QueryError.invalidColumn(col.position, col.token);
        }

        boolean ok = analyzeNotInInterval(model, col, node);
        if (ok) {
            notNode.intrinsicValue = IntrinsicValue.TRUE;
        } else {
            analyzeNotListOfValues(column, m, notNode);
        }

        return ok;
    }

    private boolean analyzeNotInInterval(IntrinsicModel model, ExprNode col, ExprNode in) throws ParserException {
        if (!isTimestamp(col)) {
            return false;
        }

        if (in.paramCount > 3) {
            throw QueryError.$(in.args.getQuick(0).position, "Too many args");
        }

        if (in.paramCount < 3) {
            throw QueryError.$(in.position, "Too few args");
        }

        ExprNode lo = in.args.getQuick(1);
        ExprNode hi = in.args.getQuick(0);

        if (lo.type == ExprNode.CONSTANT && hi.type == ExprNode.CONSTANT) {
            long loMillis;
            long hiMillis;

            try {
                loMillis = DateFormatUtils.tryParse(quoteEraser.ofQuoted(lo.token));
            } catch (NumericException ignore) {
                throw QueryError.$(lo.position, "Unknown date format");
            }

            try {
                hiMillis = DateFormatUtils.tryParse(quoteEraser.ofQuoted(hi.token));
            } catch (NumericException ignore) {
                throw QueryError.$(hi.position, "Unknown date format");
            }

            model.subtractIntervals(loMillis, hiMillis);
            in.intrinsicValue = IntrinsicValue.TRUE;
            return true;
        }
        return false;
    }

    private void analyzeNotListOfValues(String column, RecordMetadata m, ExprNode notNode) {
        RecordColumnMetadata meta = m.getColumn(column);

        switch (meta.getType()) {
            case ColumnType.SYMBOL:
            case ColumnType.STRING:
            case ColumnType.LONG:
            case ColumnType.INT:
                if (meta.isIndexed() && (preferredKeyColumn == null || preferredKeyColumn.equals(column))) {
                    keyExclNodes.add(notNode);
                }
                break;
            default:
                break;
        }
    }

    private void applyKeyExclusions(AliasTranslator translator, IntrinsicModel model) {
        if (model.keyColumn != null && keyExclNodes.size() > 0) {
            OUT:
            for (int i = 0, n = keyExclNodes.size(); i < n; i++) {
                ExprNode parent = keyExclNodes.getQuick(i);


                ExprNode node = "not".equals(parent.token) ? parent.rhs : parent;
                // this could either be '=' or 'in'

                if (node.paramCount == 2) {
                    ExprNode col;
                    ExprNode val;

                    if (node.lhs.type == ExprNode.LITERAL) {
                        col = node.lhs;
                        val = node.rhs;
                    } else {
                        col = node.rhs;
                        val = node.lhs;
                    }

                    final String column = translator.translateAlias(col.token).toString();
                    if (column.equals(model.keyColumn)) {
                        model.excludeValue(val);
                        parent.intrinsicValue = IntrinsicValue.TRUE;
                        if (model.intrinsicValue == IntrinsicValue.FALSE) {
                            break;
                        }
                    }
                }

                if (node.paramCount > 2) {
                    ExprNode col = node.args.getQuick(node.paramCount - 1);
                    final String column = translator.translateAlias(col.token).toString();
                    if (column.equals(model.keyColumn)) {
                        for (int j = node.paramCount - 2; j > -1; j--) {
                            ExprNode val = node.args.getQuick(j);
                            model.excludeValue(val);
                            if (model.intrinsicValue == IntrinsicValue.FALSE) {
                                break OUT;
                            }
                        }
                        parent.intrinsicValue = IntrinsicValue.TRUE;
                    }

                }
            }
        }
        keyExclNodes.clear();
    }

    private ExprNode collapseIntrinsicNodes(ExprNode node) {
        if (node == null || node.intrinsicValue == IntrinsicValue.TRUE) {
            return null;
        }
        node.lhs = collapseIntrinsicNodes(collapseNulls0(node.lhs));
        node.rhs = collapseIntrinsicNodes(collapseNulls0(node.rhs));
        return collapseNulls0(node);
    }

    private ExprNode collapseNulls0(ExprNode node) {
        if (node == null || node.intrinsicValue == IntrinsicValue.TRUE) {
            return null;
        }
        if ("and".equals(node.token)) {
            if (node.lhs == null || node.lhs.intrinsicValue == IntrinsicValue.TRUE) {
                return node.rhs;
            }
            if (node.rhs == null || node.rhs.intrinsicValue == IntrinsicValue.TRUE) {
                return node.lhs;
            }
        }
        return node;
    }

    IntrinsicModel extract(AliasTranslator translator, ExprNode node, RecordMetadata m, String preferredKeyColumn, int timestampIndex) throws ParserException {
        this.stack.clear();
        this.keyNodes.clear();
        this.timestamp = timestampIndex < 0 ? null : m.getColumnName(timestampIndex);
        this.preferredKeyColumn = preferredKeyColumn;

        IntrinsicModel model = models.next();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        if (removeAndIntrinsics(translator, model, node, m)) {
            return model;
        }
        ExprNode root = node;

        while (!stack.isEmpty() || node != null) {
            if (node != null) {
                switch (node.token) {
                    case "and":
                        if (!removeAndIntrinsics(translator, model, node.rhs, m)) {
                            stack.push(node.rhs);
                        }
                        node = removeAndIntrinsics(translator, model, node.lhs, m) ? null : node.lhs;
                        break;
                    default:
                        node = stack.poll();
                        break;
                }
            } else {
                node = stack.poll();
            }
        }
        applyKeyExclusions(translator, model);
        model.filter = collapseIntrinsicNodes(root);
        return model;
    }

    private boolean isTimestamp(ExprNode n) {
        return timestamp != null && timestamp.equals(n.token);
    }

    private boolean removeAndIntrinsics(AliasTranslator translator, IntrinsicModel model, ExprNode node, RecordMetadata m) throws ParserException {
        switch (node.token) {
            case "in":
                return analyzeIn(translator, model, node, m);
            case ">":
                return analyzeGreater(model, node, 1);
            case ">=":
                return analyzeGreater(model, node, 0);
            case "<":
                return analyzeLess(model, node, 1);
            case "<=":
                return analyzeLess(model, node, 0);
            case "=":
                return analyzeEquals(translator, model, node, m);
            case "!=":
                return analyzeNotEquals(translator, model, node, m);
            case "not":
                return "in".equals(node.rhs.token) && analyzeNotIn(translator, model, node, m);
            default:
                return false;
        }
    }

    private void replaceAllWithOverlap(IntrinsicModel model) {
        tempK.clear();
        tempP.clear();
        for (int i = 0, k = tempKeys.size(); i < k; i++) {
            if (model.keyValues.contains(tempKeys.get(i)) && tempK.add(tempKeys.get(i))) {
                tempP.add(tempPos.get(i));
            }
        }

        if (tempK.size() > 0) {
            model.keyValues.clear();
            model.keyValuePositions.clear();
            model.keyValues.addAll(tempK);
            model.keyValuePositions.addAll(tempP);
        } else {
            model.intrinsicValue = IntrinsicValue.FALSE;
        }
    }

    void reset() {
        this.models.clear();
    }
}

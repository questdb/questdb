/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.ql.parser;

import com.questdb.ex.NumericException;
import com.questdb.ex.ParserException;
import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Chars;
import com.questdb.misc.Dates;
import com.questdb.misc.Interval;
import com.questdb.misc.Numbers;
import com.questdb.ql.impl.interval.MillisIntervalSource;
import com.questdb.ql.impl.interval.MonthsIntervalSource;
import com.questdb.ql.impl.interval.YearIntervalSource;
import com.questdb.ql.model.ExprNode;
import com.questdb.ql.model.IntrinsicModel;
import com.questdb.ql.model.IntrinsicValue;
import com.questdb.std.*;

import java.util.ArrayDeque;

final class QueryFilterAnalyser {

    private final ArrayDeque<ExprNode> stack = new ArrayDeque<>();
    private final FlyweightCharSequence quoteEraser = new FlyweightCharSequence();
    private final ObjList<ExprNode> keyNodes = new ObjList<>();
    private final ObjList<ExprNode> timestampNodes = new ObjList<>();
    private final ObjectPool<IntrinsicModel> models = new ObjectPool<>(IntrinsicModel.FACTORY, 8);
    private final CharSequenceHashSet tempKeys = new CharSequenceHashSet();
    private final IntList tempPos = new IntList();
    private final CharSequenceHashSet tempK = new CharSequenceHashSet();
    private final IntList tempP = new IntList();
    private String timestamp;
    private String preferredKeyColumn;

    private boolean analyzeEquals(IntrinsicModel model, ExprNode node, RecordMetadata m) throws ParserException {
        return node.paramCount == 2 && (analyzeEquals0(model, node, node.lhs, node.rhs, m) || analyzeEquals0(model, node, node.rhs, node.lhs, m));
    }

    private boolean analyzeEquals0(IntrinsicModel model, ExprNode node, ExprNode a, ExprNode b, RecordMetadata m) throws ParserException {
        if (a == null || b == null) {
            throw QueryError.$(node.position, "Argument expected");
        }

        if (a.type == ExprNode.LITERAL && b.type == ExprNode.CONSTANT) {
            if (isTimestamp(a)) {
                boolean reversible = parseInterval(model, quoteEraser.ofQuoted(b.token), b.position);
                node.intrinsicValue = IntrinsicValue.TRUE;
                // exact timestamp matches will be returning FALSE
                // which means that they are irreversible and won't be added to timestampNodes.
                if (reversible) {
                    timestampNodes.add(node);
                }
                return true;
            } else {
                if (m.getColumnIndexQuiet(a.token) == -1) {
                    throw QueryError.invalidColumn(a.position, a.token);
                }
                RecordColumnMetadata meta = m.getColumn(a.token);

                switch (meta.getType()) {
                    case SYMBOL:
                    case STRING:
                    case INT:
                        if (meta.isIndexed()) {

                            // check if we are limited by preferred column
                            if (preferredKeyColumn != null && !preferredKeyColumn.equals(a.token)) {
                                return false;
                            }

                            boolean newColumn = true;
                            // check if we already have indexed column and it is of worse selectivity
                            if (model.keyColumn != null
                                    && (newColumn = !model.keyColumn.equals(a.token))
                                    && meta.getBucketCount() <= m.getColumn(model.keyColumn).getBucketCount()) {
                                return false;
                            }

                            String value = Chars.equals("null", b.token) ? null : Chars.stripQuotes(b.token);
                            if (newColumn) {
                                model.keyColumn = a.token;
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

    private boolean analyzeGreater(IntrinsicModel model, ExprNode node, int inc) throws ParserException {

        if (timestamp == null) {
            return false;
        }

        if (node.lhs.type == ExprNode.LITERAL && node.lhs.token.equals(timestamp)) {
            try {
                long lo = Dates.tryParse(quoteEraser.ofQuoted(node.rhs.token)) + inc;
                if (lo > model.intervalLo) {
                    model.intervalLo = lo;
                }
                node.intrinsicValue = IntrinsicValue.TRUE;
                return true;
            } catch (NumericException e) {
                throw QueryError.$(node.rhs.position, "Not a date");
            }
        }

        if (node.rhs.type == ExprNode.LITERAL && node.rhs.token.equals(timestamp)) {
            try {
                long hi = Dates.tryParse(quoteEraser.ofQuoted(node.lhs.token)) - inc;
                if (hi < model.intervalHi) {
                    model.intervalHi = hi;
                }
                node.intrinsicValue = IntrinsicValue.TRUE;
                return true;
            } catch (NumericException e) {
                throw QueryError.$(node.lhs.position, "Not a date");
            }
        }
        return false;
    }

    private boolean analyzeIn(IntrinsicModel model, ExprNode node, RecordMetadata metadata) throws ParserException {

        if (node.paramCount < 2) {
            throw QueryError.$(node.position, "Too few arguments for 'in'");
        }

        ExprNode col = node.paramCount < 3 ? node.lhs : node.args.getLast();

        if (col.type != ExprNode.LITERAL) {
            throw QueryError.$(col.position, "Column name expected");
        }

        if (metadata.getColumnIndexQuiet(col.token) == -1) {
            throw QueryError.invalidColumn(col.position, col.token);
        }
        return analyzeInInterval(model, col, node)
                || analyzeListOfValues(model, col.token, metadata, node)
                || analyzeInLambda(model, col.token, metadata, node);
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
                loMillis = Dates.tryParse(quoteEraser.ofQuoted(lo.token));
            } catch (NumericException ignore) {
                throw QueryError.$(lo.position, "Unknown date format");
            }

            try {
                hiMillis = Dates.tryParse(quoteEraser.ofQuoted(hi.token));
            } catch (NumericException ignore) {
                throw QueryError.$(hi.position, "Unknown date format");
            }

            model.overlapInterval(loMillis, hiMillis);
            in.intrinsicValue = IntrinsicValue.TRUE;
            timestampNodes.add(in);
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
        if (timestamp == null) {
            return false;
        }

        if (node.lhs.type == ExprNode.LITERAL && node.lhs.token.equals(timestamp)) {
            try {
                long hi = Dates.tryParse(quoteEraser.ofQuoted(node.rhs.token)) - inc;
                if (hi < model.intervalHi) {
                    model.intervalHi = hi;
                }
                node.intrinsicValue = IntrinsicValue.TRUE;
                timestampNodes.add(node);
                return true;
            } catch (NumericException e) {
                // todo: not hit by test
                throw QueryError.$(node.rhs.position, "Not a date");
            }
        }

        if (node.rhs.type == ExprNode.LITERAL && node.rhs.token.equals(timestamp)) {
            try {
                long lo = Dates.tryParse(quoteEraser.ofQuoted(node.lhs.token)) + inc;
                if (lo > model.intervalLo) {
                    model.intervalLo = lo;
                }
                node.intrinsicValue = IntrinsicValue.TRUE;
                timestampNodes.add(node);
                return true;
            } catch (NumericException e) {
                // todo: not hit by test
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

    IntrinsicModel extract(ExprNode node, RecordMetadata m, String preferredKeyColumn) throws ParserException {
        this.stack.clear();
        this.keyNodes.clear();
        this.timestampNodes.clear();
        int timestampIndex = m.getTimestampIndex();
        this.timestamp = timestampIndex == -1 ? null : m.getColumnName(timestampIndex);
        this.preferredKeyColumn = preferredKeyColumn;

        IntrinsicModel model = models.next();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        if (removeAndIntrinsics(model, node, m)) {
            return model;
        }
        ExprNode root = node;

        while (!stack.isEmpty() || node != null) {
            if (node != null) {
                switch (node.token) {
                    case "and":
                        if (!removeAndIntrinsics(model, node.rhs, m)) {
                            stack.push(node.rhs);
                        }
                        node = removeAndIntrinsics(model, node.lhs, m) ? null : node.lhs;
                        break;
                    default:
                        node = stack.poll();
                        break;
                }
            } else {
                node = stack.poll();
            }
        }
        model.filter = collapseIntrinsicNodes(root);
        return model;
    }

    private boolean isTimestamp(ExprNode n) {
        return timestamp != null && timestamp.equals(n.token);
    }

    private boolean parseInterval(IntrinsicModel model, CharSequence seq, int position) throws ParserException {
        return parseInterval(model, seq, 0, seq.length(), position);
    }

    private boolean parseInterval(IntrinsicModel model, CharSequence seq, int lo, int lim, int position) throws ParserException {
        int pos[] = new int[3];
        int p = -1;
        for (int i = lo; i < lim; i++) {
            if (seq.charAt(i) == ';') {
                if (p > 1) {
                    throw QueryError.$(position, "Invalid interval format");
                }
                pos[++p] = i;
            }
        }

        switch (p) {
            case -1:
                // no semicolons, just date part, which can be interval in itself
                try {
                    Interval interval = Dates.parseInterval(seq, lo, lim);
                    model.overlapInterval(interval.getLo(), interval.getHi());
                    return true;
                } catch (NumericException ignore) {
                    // this must be a date then?
                }

                // reset intrinsic value of previous timestamp nodes
                for (int i = 0, k = timestampNodes.size(); i < k; i++) {
                    timestampNodes.getQuick(i).intrinsicValue = IntrinsicValue.UNDEFINED;
                }
                timestampNodes.clear();

                try {
                    long millis = Dates.tryParse(seq, lo, lim);

                    if (model.millis != Long.MIN_VALUE && model.millis != millis) {
                        model.intrinsicValue = IntrinsicValue.FALSE;
                    }

                    model.millis = millis;
                    model.clearInterval();
                    return false;
                } catch (NumericException e) {
                    // todo: not hit by test
                    throw QueryError.$(position, "Not a date");
                }
            case 0:
                // single semicolon, expect period format after date
                Interval interval0 = parseInterval0(seq, lo, pos[0], lim, position);
                model.overlapInterval(interval0.getLo(), interval0.getHi());
                break;
            case 2:
                if (model.intervalSource != null) {
                    throw QueryError.$(position, "Duplicate interval filter is not supported");
                }
                Interval interval2 = parseInterval0(seq, lo, pos[0], pos[1], position);
                int period;
                try {
                    period = Numbers.parseInt(seq, pos[1] + 1, pos[2] - 1);
                } catch (NumericException e) {
                    throw QueryError.$(position, "Period not a number");
                }
                char type = seq.charAt(pos[2] - 1);
                int count;
                try {
                    count = Numbers.parseInt(seq, pos[2] + 1, seq.length());
                } catch (NumericException e) {
                    throw QueryError.$(position, "Count not a number");
                }
                switch (type) {
                    case 'y':
                        model.intervalSource = new YearIntervalSource(interval2, period, count);
                        break;
                    case 'M':
                        model.intervalSource = new MonthsIntervalSource(interval2, period, count);
                        break;
                    case 'h':
                        model.intervalSource = new MillisIntervalSource(interval2, period * Dates.HOUR_MILLIS, count);
                        break;
                    case 'm':
                        model.intervalSource = new MillisIntervalSource(interval2, period * Dates.MINUTE_MILLIS, count);
                        break;
                    case 's':
                        model.intervalSource = new MillisIntervalSource(interval2, period * Dates.SECOND_MILLIS, count);
                        break;
                    case 'd':
                        model.intervalSource = new MillisIntervalSource(interval2, period * Dates.DAY_MILLIS, count);
                        break;
                    default:
                        throw QueryError.$(position, "Unknown period: " + type + " at " + (p - 1));
                }
                break;
            default:
                throw QueryError.$(position, "Invalid interval format");
        }

        return true;
    }

    private Interval parseInterval0(CharSequence seq, int lo, int p, int lim, int position) throws ParserException {
        char type = seq.charAt(lim - 1);
        int period;
        try {
            period = Numbers.parseInt(seq, p + 1, lim - 1);
        } catch (NumericException e) {
            throw QueryError.$(position, "Period not a number");
        }
        try {
            Interval interval = Dates.parseInterval(seq, lo, p);
            return new Interval(interval.getLo(), Dates.addPeriod(interval.getHi(), type, period));
        } catch (NumericException ignore) {
            // try date instead
        }
        try {
            long loMillis = Dates.tryParse(seq, lo, p - 1);
            long hiMillis = Dates.addPeriod(loMillis, type, period);
            return new Interval(loMillis, hiMillis);
        } catch (NumericException e) {
            throw QueryError.$(position, "Neither interval nor date");
        }
    }

    private boolean removeAndIntrinsics(IntrinsicModel model, ExprNode node, RecordMetadata m) throws ParserException {
        if (node == null) {
            return true;
        }

        switch (node.token) {
            case "in":
                return analyzeIn(model, node, m);
            case ">":
                return analyzeGreater(model, node, 1);
            case ">=":
                return analyzeGreater(model, node, 0);
            case "<":
                return analyzeLess(model, node, 1);
            case "<=":
                return analyzeLess(model, node, 0);
            case "=":
                return analyzeEquals(model, node, m);
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

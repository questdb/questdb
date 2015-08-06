/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.ql.parser;

import com.nfsdb.collections.FlyweightCharSequence;
import com.nfsdb.collections.ObjHashSet;
import com.nfsdb.collections.ObjList;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.ql.RecordMetadata;
import com.nfsdb.ql.impl.MillisIntervalSource;
import com.nfsdb.ql.impl.MonthsIntervalSource;
import com.nfsdb.ql.impl.YearIntervalSource;
import com.nfsdb.ql.model.ExprNode;
import com.nfsdb.ql.model.IntrinsicModel;
import com.nfsdb.ql.model.IntrinsicValue;
import com.nfsdb.utils.Chars;
import com.nfsdb.utils.Dates;
import com.nfsdb.utils.Interval;
import com.nfsdb.utils.Numbers;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayDeque;

public class IntrinsicExtractor {

    private final ArrayDeque<ExprNode> stack = new ArrayDeque<>();
    private final FlyweightCharSequence quoteEraser = new FlyweightCharSequence();
    private final ObjList<ExprNode> keyNodes = new ObjList<>();
    private final ObjList<ExprNode> timestampNodes = new ObjList<>();
    private final IntrinsicModel model = new IntrinsicModel();
    private final ObjHashSet<String> tempKeys = new ObjHashSet<>();
    private RecordColumnMetadata timestamp;
    private String preferredKeyColumn;

    public IntrinsicModel extract(ExprNode node, RecordMetadata m, String preferredKeyColumn) throws ParserException {
        this.stack.clear();
        this.model.reset();
        this.keyNodes.clear();
        this.timestampNodes.clear();
        this.timestamp = m.getTimestampMetadata();
        this.preferredKeyColumn = preferredKeyColumn;

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        if (removeAndIntrinsics(node, m)) {
            return model;
        }
        ExprNode root = node;

        while (!stack.isEmpty() || node != null) {
            if (node != null) {
                switch (node.token) {
                    case "and":
                        if (!removeAndIntrinsics(node.rhs, m)) {
                            stack.push(node.rhs);
                        }
                        node = removeAndIntrinsics(node.lhs, m) ? null : node.lhs;
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

    private boolean analyzeEquals(ExprNode node, RecordMetadata m) throws ParserException {
        return node.paramCount == 2 && (analyzeEquals0(node, node.lhs, node.rhs, m) || analyzeEquals0(node, node.rhs, node.lhs, m));
    }

    private boolean analyzeEquals0(ExprNode node, ExprNode a, ExprNode b, RecordMetadata m) throws ParserException {
        if (a == null || b == null) {
            throw new ParserException(node.position, "Argument expected");
        }

        if (a.type == ExprNode.NodeType.LITERAL && b.type == ExprNode.NodeType.CONSTANT) {
            if (timestamp != null && timestamp.getName().equals(a.token)) {
                boolean reversible = parseInterval(quoteEraser.of(b.token), b.position);
                node.intrinsicValue = IntrinsicValue.TRUE;
                // exact timestamp matches will be returning FALSE
                // which means that they are irreversible and won't be added to timestampNodes.
                if (reversible) {
                    timestampNodes.add(node);
                }
                return true;
            } else {
                if (m.invalidColumn(a.token)) {
                    throw new InvalidColumnException(a.position);
                }
                RecordColumnMetadata meta = m.getColumn(a.token);

                switch (meta.getType()) {
                    case SYMBOL:
                    case STRING:
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
                                model.keyValues.add(value);
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
                                    model.keyValues.add(value);
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

    private boolean analyzeGreater(ExprNode node, int inc) {

        if (timestamp == null) {
            return false;
        }

        if (node.lhs.type == ExprNode.NodeType.LITERAL && node.lhs.token.equals(timestamp.getName())) {
            long lo = Dates.tryParse(quoteEraser.of(node.rhs.token)) + inc;
            if (lo > model.intervalLo) {
                model.intervalLo = lo;
            }
            node.intrinsicValue = IntrinsicValue.TRUE;
            return true;
        }

        if (node.rhs.type == ExprNode.NodeType.LITERAL && node.rhs.token.equals(timestamp.getName())) {
            long hi = Dates.tryParse(quoteEraser.of(node.lhs.token)) - inc;
            if (hi < model.intervalHi) {
                model.intervalHi = hi;
            }
            node.intrinsicValue = IntrinsicValue.TRUE;
            return true;
        }
        return false;
    }

    private boolean analyzeIn(ExprNode node, RecordMetadata metadata) throws ParserException {

        if (node.paramCount < 2) {
            throw new ParserException(node.position, "Too few arguments for 'in'");
        }

        ExprNode col = node.paramCount < 3 ? node.lhs : node.args.getLast();

        if (col.type != ExprNode.NodeType.LITERAL) {
            throw new ParserException(col.position, "Column name expected");
        }

        if (metadata.invalidColumn(col.token)) {
            throw new InvalidColumnException(col.position);
        }
        return analyzeInInterval(col, node) || analyzeListOfValues(col.token, metadata, node);
    }

    @SuppressFBWarnings({"LEST_LOST_EXCEPTION_STACK_TRACE", "LEST_LOST_EXCEPTION_STACK_TRACE"})
    private boolean analyzeInInterval(ExprNode col, ExprNode in) throws ParserException {
        if (timestamp == null || !Chars.equals(timestamp.getName(), col.token)) {
            return false;
        }

        if (in.paramCount > 3) {
            throw new ParserException(in.args.getQuick(0).position, "Too many args");
        }


        if (in.paramCount < 3) {
            throw new ParserException(in.position, "Too few args");
        }

        ExprNode lo = in.args.getQuick(1);
        ExprNode hi = in.args.getQuick(0);

        if (lo.type == ExprNode.NodeType.CONSTANT && hi.type == ExprNode.NodeType.CONSTANT) {
            long loMillis;
            long hiMillis;

            try {
                loMillis = Dates.tryParse(quoteEraser.of(lo.token));
            } catch (NumberFormatException ignore) {
                throw new ParserException(lo.position, "Unknown date format");
            }

            try {
                hiMillis = Dates.tryParse(quoteEraser.of(hi.token));
            } catch (NumberFormatException ignore) {
                throw new ParserException(hi.position, "Unknown date format");
            }

            model.overlapInterval(loMillis, hiMillis);
            in.intrinsicValue = IntrinsicValue.TRUE;
            timestampNodes.add(in);
            return true;
        }
        return false;
    }

    private boolean analyzeLess(ExprNode node, int inc) {
        if (timestamp == null) {
            return false;
        }

        if (node.lhs.type == ExprNode.NodeType.LITERAL && node.lhs.token.equals(timestamp.getName())) {
            long hi = Dates.tryParse(quoteEraser.of(node.rhs.token)) - inc;
            if (hi < model.intervalHi) {
                model.intervalHi = hi;
            }
            node.intrinsicValue = IntrinsicValue.TRUE;
            timestampNodes.add(node);
            return true;
        }

        if (node.rhs.type == ExprNode.NodeType.LITERAL && node.rhs.token.equals(timestamp.getName())) {
            long lo = Dates.tryParse(quoteEraser.of(node.lhs.token)) + inc;
            if (lo > model.intervalLo) {
                model.intervalLo = lo;
            }
            node.intrinsicValue = IntrinsicValue.TRUE;
            timestampNodes.add(node);
            return true;
        }
        return false;
    }

    private boolean analyzeListOfValues(String col, RecordMetadata meta, ExprNode node) {
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

            // collect and analyze values of indexed field
            // if any of values is not an indexed constant - bail out
            if (i == 1) {
                if (node.rhs == null || node.rhs.type != ExprNode.NodeType.CONSTANT) {
                    return false;
                }
                tempKeys.add(Chars.stripQuotes(node.rhs.token));
            } else {
                for (i--; i > -1; i--) {
                    ExprNode c = node.args.getQuick(i);
                    if (c.type != ExprNode.NodeType.CONSTANT) {
                        return false;
                    }
                    tempKeys.add(Chars.stripQuotes(c.token));
                }
            }

            // clear values if this is new column
            // and reset intrinsic values on nodes associated with old column
            if (newColumn) {
                model.keyValues.clear();
                model.keyValues.addAll(tempKeys);
                for (int n = 0, k = keyNodes.size(); n < k; n++) {
                    keyNodes.getQuick(n).intrinsicValue = IntrinsicValue.UNDEFINED;
                }
                keyNodes.clear();
                model.keyColumn = col;
            } else {
                // calculate overlap of values
                if (!model.keyValues.replaceAllWithOverlap(tempKeys)) {
                    model.intrinsicValue = IntrinsicValue.FALSE;
                }
            }

            keyNodes.add(node);
            node.intrinsicValue = IntrinsicValue.TRUE;
            return true;
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

    private boolean parseInterval(CharSequence seq, int position) throws ParserException {
        return parseInterval(seq, 0, seq.length(), position);
    }

    @SuppressFBWarnings({"CLI_CONSTANT_LIST_INDEX", "EXS_EXCEPTION_SOFTENING_RETURN_FALSE"})
    private boolean parseInterval(CharSequence seq, int lo, int lim, int position) throws ParserException {
        int pos[] = new int[3];
        int p = -1;
        for (int i = lo; i < lim; i++) {
            if (seq.charAt(i) == ';') {
                if (p > 1) {
                    throw new ParserException(position, "Invalid interval format");
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
                } catch (NumberFormatException ignore) {
                    // this must be a date then?
                }

                // reset intrinsic value of previous timestamp nodes
                for (int i = 0, k = timestampNodes.size(); i < k; i++) {
                    timestampNodes.getQuick(i).intrinsicValue = IntrinsicValue.UNDEFINED;
                }
                timestampNodes.clear();

                long millis = Dates.tryParse(seq, lo, lim);

                if (model.millis != Long.MIN_VALUE && model.millis != millis) {
                    model.intrinsicValue = IntrinsicValue.FALSE;
                }

                model.millis = millis;
                model.clearInterval();
                return false;
            case 0:
                // single semicolon, expect period format after date
                Interval interval0 = parseInterval0(seq, lo, pos[0], lim);
                model.overlapInterval(interval0.getLo(), interval0.getHi());
                break;
            case 2:
                if (model.intervalSource != null) {
                    throw new ParserException(position, "Duplicate interval filter is not supported");
                }
                Interval interval2 = parseInterval0(seq, lo, pos[0], pos[1]);
                int period = Numbers.parseInt(seq, pos[1] + 1, pos[2] - 1);
                char type = seq.charAt(pos[2] - 1);
                int count = Numbers.parseInt(seq, pos[2] + 1, seq.length());
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
                        throw new ParserException(position, "Unknown period: " + type + " at " + (p - 1));
                }
                break;
            default:
                throw new ParserException(position, "Invalid interval format");
        }

        return true;
    }

    private Interval parseInterval0(CharSequence seq, int lo, int p, int lim) {
        char type = seq.charAt(lim - 1);
        int period = Numbers.parseInt(seq, p + 1, lim - 1);
        try {
            Interval interval = Dates.parseInterval(seq, lo, p);
            return new Interval(interval.getLo(), Dates.addPeriod(interval.getHi(), type, period));
        } catch (NumberFormatException ignore) {
            // try date instead
        }
        long loMillis = Dates.tryParse(seq, lo, p - 1);
        long hiMillis = Dates.addPeriod(loMillis, type, period);
        return new Interval(loMillis, hiMillis);
    }

    private boolean removeAndIntrinsics(ExprNode node, RecordMetadata m) throws ParserException {
        if (node == null) {
            return true;
        }

        switch (node.token) {
            case "in":
                return analyzeIn(node, m);
            case ">":
                return analyzeGreater(node, 1);
            case ">=":
                return analyzeGreater(node, 0);
            case "<":
                return analyzeLess(node, 1);
            case "<=":
                return analyzeLess(node, 0);
            case "=":
                return analyzeEquals(node, m);
            default:
                return false;
        }
    }
}

/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

import com.nfsdb.Journal;
import com.nfsdb.collections.FlyweightCharSequence;
import com.nfsdb.collections.ObjHashSet;
import com.nfsdb.collections.ObjList;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.ql.impl.IntervalSource;
import com.nfsdb.ql.impl.MillisPeriodSource;
import com.nfsdb.ql.impl.MonthPeriodSource;
import com.nfsdb.ql.impl.YearPeriodSource;
import com.nfsdb.ql.model.ExprNode;
import com.nfsdb.ql.model.IntrinsicValue;
import com.nfsdb.utils.Chars;
import com.nfsdb.utils.Dates;
import com.nfsdb.utils.Interval;
import com.nfsdb.utils.Numbers;

import java.util.ArrayDeque;

public class IntrinsicExtractor {

    private final ArrayDeque<ExprNode> stack = new ArrayDeque<>();
    private final FlyweightCharSequence quoteEraser = new FlyweightCharSequence();
    private final ObjList<ExprNode> currentKeyNodes = new ObjList<>();
    private final IntrinsicModel model = new IntrinsicModel();
    private ColumnMetadata timestamp;

    private static String stripQuotes(String s) {
        int l;
        if (s == null || (l = s.length()) == 0) {
            return s;
        }

        char c = s.charAt(0);
        if (c == '\'' || c == '"') {
            return s.substring(1, l - 1);
        }

        return s;
    }

    public IntrinsicModel extract(ExprNode node, Journal journal) throws ParserException {
        JournalMetadata m = journal.getMetadata();
        this.stack.clear();
        this.model.reset();
        this.timestamp = m.getTimestampMetadata();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        if (removeIntrinsics(node, m)) {
            return model;
        }
        ExprNode root = node;

        while (!stack.isEmpty() || node != null) {
            if (node != null) {
                switch (node.token) {
                    case "and":
                        if (!removeIntrinsics(node.rhs, m)) {
                            stack.addFirst(node.rhs);
                        }
                        node = removeIntrinsics(node.lhs, m) ? null : node.lhs;
                        break;
                    default:
                        node = stack.pollFirst();
                        break;
                }
            } else {
                node = stack.pollFirst();
            }
        }
        model.filter = collapseIntrinsicNodes(root);
        return model;
    }

    private void parseInterval(CharSequence seq) {
        parseInterval(seq, 0, seq.length());
    }

    private void parseInterval(CharSequence seq, int lo, int lim) {
        int pos[] = new int[3];
        int p = -1;
        for (int i = lo; i < lim; i++) {
            if (seq.charAt(i) == ';') {
                if (p > 1) {
                    throw new NumberFormatException("Invalid interval format");
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
                    return;
                } catch (NumberFormatException ignore) {
                    // this must be a date then?
                }

                model.millis = Dates.tryParse(seq, lo, lim);
                model.clearInterval();
                return;
            case 0:
                // single semicolon, expect period format after date
                Interval interval0 = parseInterval0(seq, lo, pos[0], lim);
                model.overlapInterval(interval0.getLo(), interval0.getHi());
                break;
            case 2:
                if (model.intervalSource != null) {
                    throw new NumberFormatException("Duplicate interval filter is not supported");
                }
                Interval interval2 = parseInterval0(seq, lo, pos[0], pos[1]);
                int period = Numbers.parseInt(seq, pos[1] + 1, pos[2] - 1);
                char type = seq.charAt(pos[2] - 1);
                int count = Numbers.parseInt(seq, pos[2] + 1, seq.length());
                switch (type) {
                    case 'y':
                        model.intervalSource = new YearPeriodSource(interval2, period, count);
                        break;
                    case 'M':
                        model.intervalSource = new MonthPeriodSource(interval2, period, count);
                        break;
                    case 'h':
                        model.intervalSource = new MillisPeriodSource(interval2, period * Dates.HOUR_MILLIS, count);
                        break;
                    case 'm':
                        model.intervalSource = new MillisPeriodSource(interval2, period * Dates.MINUTE_MILLIS, count);
                        break;
                    case 's':
                        model.intervalSource = new MillisPeriodSource(interval2, period * Dates.SECOND_MILLIS, count);
                        break;
                    case 'd':
                        model.intervalSource = new MillisPeriodSource(interval2, period * Dates.DAY_MILLIS, count);
                        break;
                    default:
                        throw new NumberFormatException("Unknown period: " + type + " at " + (p - 1));
                }
                break;
            default:
                throw new NumberFormatException("Not implemented yet");
        }
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

    private boolean analyzeIn(ExprNode node, JournalMetadata metadata) throws ParserException {

        if (node.paramCount < 2) {
            throw new ParserException(node.position, "Too few arguments for 'in'");
        }

        ExprNode col = node.paramCount < 3 ? node.lhs : node.args.getLast();

        if (col.type != ExprNode.NodeType.LITERAL) {
            throw new ParserException(col.position, "Column name expected");
        }

        return analyzeInInterval(col, node) || analyzeListOfValues(col.token, metadata, node);
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
            return true;
        }

        if (node.rhs.type == ExprNode.NodeType.LITERAL && node.rhs.token.equals(timestamp.getName())) {
            long lo = Dates.tryParse(quoteEraser.of(node.lhs.token)) + inc;
            if (lo > model.intervalLo) {
                model.intervalLo = lo;
            }
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
        if (node.token.equals("and")) {
            if (node.lhs == null || node.lhs.intrinsicValue == IntrinsicValue.TRUE) {
                return node.rhs;
            }
            if (node.rhs == null || node.rhs.intrinsicValue == IntrinsicValue.TRUE) {
                return node.lhs;
            }
        }
        return node;
    }

    private boolean analyzeListOfValues(String col, JournalMetadata meta, ExprNode node) {
        ColumnMetadata colMeta = meta.getColumn(col);
        if (colMeta.indexed) {
            boolean newColumn = true;

            // check if we already have indexed column and it is of worse selectivity
            if (model.keyColumn != null
                    && (newColumn = !model.keyColumn.equals(col))
                    && colMeta.distinctCountHint <= meta.getColumn(model.keyColumn).distinctCountHint) {
                return false;
            }


            int i = node.paramCount - 1;
            ObjHashSet<String> keys = new ObjHashSet<>(i);

            // collect and analyze values of indexed field
            // if any of values is not an indexed constant - bail out
            if (i == 1) {
                if (node.rhs == null || node.rhs.type != ExprNode.NodeType.CONSTANT) {
                    return false;
                }
                keys.add(stripQuotes(node.rhs.token));
            } else {
                for (i--; i > -1; i--) {
                    ExprNode c = node.args.getQuick(i);
                    if (c.type != ExprNode.NodeType.CONSTANT) {
                        return false;
                    }
                    keys.add(stripQuotes(c.token));
                }
            }

            // clear values if this is new column
            // and reset intrinsic values on nodes associated with old column
            if (newColumn) {
                model.keyValues.clear();
                for (int n = 0, k = currentKeyNodes.size(); n < k; n++) {
                    currentKeyNodes.getQuick(n).intrinsicValue = IntrinsicValue.UNDEFINED;
                }
                currentKeyNodes.clear();
                model.keyColumn = col;
            }

            currentKeyNodes.add(node);
            node.intrinsicValue = IntrinsicValue.TRUE;
            model.keyValues.addAll(keys);
            return true;
        }
        return false;
    }

    private boolean analyzeInInterval(ExprNode col, ExprNode in) throws ParserException {
        if (timestamp == null || !Chars.equals(timestamp.name, col.token)) {
            return false;
        }

        if (in.paramCount > 3) {
            throw new ParserException(in.args.get(0).position, "Too many args");
        }


        if (in.paramCount < 3) {
            throw new ParserException(in.position, "Too few args");
        }

        ExprNode lo = in.args.get(1);
        ExprNode hi = in.args.get(0);

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
            return true;
        }
        return false;
    }

    private boolean removeIntrinsics(ExprNode node, JournalMetadata m) throws ParserException {
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
                return analyzeEquals(node);
            default:
                return false;
        }
    }

    private boolean analyzeEquals(ExprNode node) {
        if (node.paramCount > 2 || timestamp == null) {
            return false;
        }

        if (node.lhs.type == ExprNode.NodeType.LITERAL && timestamp.name.equals(node.lhs.token) && node.rhs.type == ExprNode.NodeType.CONSTANT) {
            parseInterval(stripQuotes(node.rhs.token));
            return true;
        }

        if (node.rhs.type == ExprNode.NodeType.LITERAL && timestamp.name.equals(node.rhs.token) && node.lhs.type == ExprNode.NodeType.CONSTANT) {
            parseInterval(stripQuotes(node.lhs.token));
            return true;
        }

        return false;
    }

    public static class IntrinsicModel {
        public final ObjHashSet<String> keyValues = new ObjHashSet<>();
        public String keyColumn;
        public long intervalLo = Long.MIN_VALUE;
        public long intervalHi = Long.MAX_VALUE;
        public ExprNode filter;
        public long millis = Long.MIN_VALUE;
        public IntervalSource intervalSource;

        public void clearInterval() {
            this.intervalLo = Long.MIN_VALUE;
            this.intervalHi = Long.MAX_VALUE;
        }

        public void overlapInterval(long lo, long hi) {
            if (lo > intervalLo) {
                intervalLo = lo;
            }

            if (hi < intervalHi) {
                intervalHi = hi;
            }
        }

        public void reset() {
            keyColumn = null;
            keyValues.clear();
            clearInterval();
            filter = null;
            millis = Long.MIN_VALUE;
            intervalSource = null;
        }

        @Override
        public String toString() {
            return "IntrinsicModel{" +
                    "keyValues=" + keyValues +
                    ", keyColumn='" + keyColumn + '\'' +
                    ", intervalLo=" + Dates.toString(intervalLo) +
                    ", intervalHi=" + Dates.toString(intervalHi) +
                    ", filter=" + filter +
                    ", millis=" + millis +
                    '}';
        }
    }
}

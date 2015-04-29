/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

    public IntrinsicModel extract(ExprNode node, Journal journal) throws ParserException {
        IntrinsicModel model = new IntrinsicModel();
        JournalMetadata m = journal.getMetadata();
        stack.clear();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        if (removeIntrinsics(node, model, m)) {
            return model;
        }
        ExprNode root = node;

        while (!stack.isEmpty() || node != null) {
            if (node != null) {
                switch (node.token) {
                    case "and":
                        if (!removeIntrinsics(node.rhs, model, m)) {
                            stack.addFirst(node.rhs);
                        }

                        node = removeIntrinsics(node.lhs, model, m) ? null : node.lhs;
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

    private static IntervalSource parseIntervalSourceOld(CharSequence seq) {
        int p1 = -1, p2 = -1;
        int l = seq.length();

        for (int i = 0; i < l; i++) {
            if (seq.charAt(i) == ';') {
                if (p1 == -1) {
                    p1 = i;
                } else {
                    p2 = i;
                    break;
                }
            }
        }

        if (p1 == -1) {
            throw new NumberFormatException("Invalid interval");
        }

        Interval interval = Dates.parseInterval(seq, 0, p1);
        int p = p2 == -1 ? l : p2;
        char type = seq.charAt(p - 1);
        int n = Numbers.parseInt(seq, p1 + 1, p - 1);

        int count;
        if (p2 == -1) {
            count = Integer.MAX_VALUE;
        } else {
            count = Numbers.parseInt(seq, p2 + 1, l);
        }

        switch (type) {
            case 'y':
                return new YearPeriodSource(interval, n, count);
            case 'M':
                return new MonthPeriodSource(interval, n, count);
            case 'h':
                return new MillisPeriodSource(interval, n * Dates.HOUR_MILLIS, count);
            case 'm':
                return new MillisPeriodSource(interval, n * Dates.MINUTE_MILLIS, count);
            case 's':
                return new MillisPeriodSource(interval, n * Dates.SECOND_MILLIS, count);
            case 'd':
                return new MillisPeriodSource(interval, n * Dates.DAY_MILLIS, count);
            default:
                throw new NumberFormatException("Unknown modifier: " + type + " at " + (p - 1));
        }
    }

    private boolean analyzeGreater(ExprNode node, IntrinsicModel model, JournalMetadata m, int inc) {
        if (node.lhs.type == ExprNode.NodeType.LITERAL && node.lhs.token.equals(m.getTimestampMetadata().getName())) {
            long lo = Dates.tryParse(quoteEraser.of(node.rhs.token)) + inc;
            if (lo > model.intervalLo) {
                model.intervalLo = lo;
            }
            node.intrinsicValue = IntrinsicValue.TRUE;
            return true;
        }

        if (node.rhs.type == ExprNode.NodeType.LITERAL && node.rhs.token.equals(m.getTimestampMetadata().getName())) {
            long hi = Dates.tryParse(quoteEraser.of(node.lhs.token)) - inc;
            if (hi < model.intervalHi) {
                model.intervalHi = hi;
            }
            node.intrinsicValue = IntrinsicValue.TRUE;
            return true;
        }
        return false;
    }

    private boolean analyzeIn(ExprNode node, IntrinsicModel model, JournalMetadata metadata) throws ParserException {

        if (node.paramCount < 2) {
            throw new ParserException(node.position, "Too few arguments for 'in'");
        }

        ExprNode col = node.paramCount < 3 ? node.lhs : node.args.getLast();

        if (col.type != ExprNode.NodeType.LITERAL) {
            throw new ParserException(col.position, "Column name expected");
        }

        return isInterval(col, node, model, metadata.getTimestampMetadata())
                || isIndexedField(col.token, metadata, node, model);
    }

    private boolean analyzeLess(ExprNode node, IntrinsicModel model, JournalMetadata m, int inc) {
        if (node.lhs.type == ExprNode.NodeType.LITERAL && node.lhs.token.equals(m.getTimestampMetadata().getName())) {
            long hi = Dates.tryParse(quoteEraser.of(node.rhs.token)) - inc;
            if (hi < model.intervalHi) {
                model.intervalHi = hi;
            }
            node.intrinsicValue = IntrinsicValue.TRUE;
            return true;
        }

        if (node.rhs.type == ExprNode.NodeType.LITERAL && node.rhs.token.equals(m.getTimestampMetadata().getName())) {
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

    private boolean isIndexedField(String col, JournalMetadata meta, ExprNode node, IntrinsicModel model) {
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

    private boolean isInterval(ExprNode col, ExprNode in, IntrinsicModel model, ColumnMetadata timestampMeta) throws ParserException {
        if (timestampMeta == null || !Chars.equals(timestampMeta.name, col.token)) {
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

    void parseInterval(CharSequence seq, int lo, int lim, IntrinsicModel model) {
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
                try {
                    Interval interval = Dates.parseInterval(seq, lo, pos[0]);
                } catch (NumberFormatException ignore) {
                    // try date instead
                }

                long millis = Dates.tryParse(seq, lo, pos[0] - 1);
                char type = seq.charAt(lim - 1);
                int period = Numbers.parseInt(seq, pos[0] + 1, lim - 1);

                switch (type) {
                    case 's':

                }
        }


    }

    private boolean removeIntrinsics(ExprNode node, IntrinsicModel model, JournalMetadata m) throws ParserException {
        if (node == null) {
            return true;
        }

        switch (node.token) {
            case "in":
                return analyzeIn(node, model, m);
            case ">":
                return analyzeGreater(node, model, m, 1);
            case ">=":
                return analyzeGreater(node, model, m, 0);
            case "<":
                return analyzeLess(node, model, m, 1);
            case "<=":
                return analyzeLess(node, model, m, 0);
            default:
                return false;
        }
    }

    public static class IntrinsicModel {
        public final ObjHashSet<String> keyValues = new ObjHashSet<>();
        public String keyColumn;
        public long intervalLo = Long.MIN_VALUE;
        public long intervalHi = Long.MAX_VALUE;
        public ExprNode filter;
        public long millis;

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
    }
}

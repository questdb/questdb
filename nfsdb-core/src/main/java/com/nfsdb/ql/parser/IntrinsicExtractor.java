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
import com.nfsdb.collections.ObjList;
import com.nfsdb.collections.ObjObjHashMap;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.ql.model.ExprNode;
import com.nfsdb.utils.Chars;
import com.nfsdb.utils.Dates;
import com.nfsdb.utils.Interval;

import java.util.ArrayDeque;

public class IntrinsicExtractor {

    private final ArrayDeque<ExprNode> stack = new ArrayDeque<>();
    private final FlyweightCharSequence quoteEraser = new FlyweightCharSequence();

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
        IntrinsicModel model = new IntrinsicModel();
        JournalMetadata m = journal.getMetadata();
        stack.clear();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        ExprNode root = node = removeIntrinsics(node, model, m);

        while (!stack.isEmpty() || node != null) {
            if (node != null) {
                switch (node.token) {
                    case "and":
                        if ((node.rhs = removeIntrinsics(node.rhs, model, m)) != null) {
                            stack.addFirst(node.rhs);
                        }
                        node = node.lhs = removeIntrinsics(node.lhs, model, m);
                        break;
                    default:
                        node = stack.pollFirst();
                        break;
                }
            } else {
                node = stack.pollFirst();
            }
        }
        model.filter = collapseNulls(root);
        return model;
    }

    private ExprNode collapseNulls(ExprNode node) {
        if (node == null) {
            return null;
        }
        node.lhs = collapseNulls(collapseNulls0(node.lhs));
        node.rhs = collapseNulls(collapseNulls0(node.rhs));
        return collapseNulls0(node);
    }

    private ExprNode collapseNulls0(ExprNode node) {
        if (node == null) {
            return null;
        }
        if (node.token.equals("and")) {
            if (node.lhs == null) {
                return node.rhs;
            }
            if (node.rhs == null) {
                return node.lhs;
            }
        }
        return node;
    }

    private ExprNode removeIntrinsics(ExprNode node, IntrinsicModel model, JournalMetadata m) throws ParserException {
        switch (node.token) {
            case "in":
                return analyzeIn(node, model, m) ? null : node;
        }
        return node;
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
                || isIndexedField(metadata.getColumn(col.token), node, model);
    }

    private boolean isIndexedField(ColumnMetadata colMeta, ExprNode node, IntrinsicModel model) {
        if (colMeta.indexed) {
            int i;
            ObjList<String> keys = new ObjList<>(i = node.paramCount - 1);
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
            model.inSets.put(colMeta.name, keys);
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
                loMillis = Dates.tryParse(quoteEraser.of(lo.token, 1, lo.token.length() - 2));
            } catch (NumberFormatException ignore) {
                throw new ParserException(lo.position, "Unknown date format");
            }

            try {
                hiMillis = Dates.tryParse(quoteEraser.of(hi.token, 1, hi.token.length() - 2));
            } catch (NumberFormatException ignore) {
                throw new ParserException(hi.position, "Unknown date format");
            }

            if (model.interval != null) {
                model.interval = model.interval.overlap(loMillis, hiMillis);
            } else {
                model.interval = new Interval(loMillis, hiMillis);
            }

            return true;
        }
        return false;
    }

    public static class IntrinsicModel {
        public final ObjObjHashMap<String, ObjList<String>> inSets = new ObjObjHashMap<>();
        public Interval interval;
        public ExprNode filter;
    }
}

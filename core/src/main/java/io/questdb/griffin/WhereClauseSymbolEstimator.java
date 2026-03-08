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

import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.model.AliasTranslator;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;

import java.util.ArrayDeque;

import static io.questdb.griffin.SqlKeywords.*;

public final class WhereClauseSymbolEstimator implements Mutable {

    private static final int OP_EQUAL = 1;
    private static final int OP_IN = 2;
    private static final int OP_NOT = 3;
    private static final int OP_NOT_EQ = 4;

    private static final CharSequenceIntHashMap ops = new CharSequenceIntHashMap();
    private final IntIntHashMap columnIndexesToListIndexes = new IntIntHashMap();
    private final ArrayDeque<ExpressionNode> stack = new ArrayDeque<>();
    private boolean gaveUp;
    private IntList symbolCounts;

    @Override
    public void clear() {
        this.stack.clear();
        this.columnIndexesToListIndexes.clear();
        this.symbolCounts = null;
        this.gaveUp = false;
    }

    /**
     * Estimates symbol counts for simple filters like the following one:
     * <code>
     * sym1 in ('a','b','c') and sym2 = 'd'
     * </code>
     * <p>
     * For more complex filters the estimates are guaranteed to be equal
     * or great than the actual count of filtered symbol values.
     */
    public IntList estimate(
            AliasTranslator translator,
            ExpressionNode node,
            RecordMetadata metadata,
            IntList columnIndexes
    ) throws SqlException {
        if (node == null) {
            return null;
        }

        clear();

        for (int i = 0, n = columnIndexes.size(); i < n; i++) {
            columnIndexesToListIndexes.put(columnIndexes.getQuick(i), i);
        }

        symbolCounts = new IntList(columnIndexes.size());
        symbolCounts.setAll(columnIndexes.size(), 0);

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        if (!analyze(translator, node, metadata)) {
            while (!stack.isEmpty() || node != null) {
                if (node != null) {
                    if (isAndKeyword(node.token)) {
                        if (!analyze(translator, node.rhs, metadata)) {
                            stack.push(node.rhs);
                        }
                        if (gaveUp) {
                            break;
                        }
                        node = analyze(translator, node.lhs, metadata) ? null : node.lhs;
                        if (gaveUp) {
                            break;
                        }
                    } else if (isOrKeyword(node.token)) {
                        giveUp();
                        break;
                    } else {
                        node = stack.poll();
                    }
                } else {
                    node = stack.poll();
                }
            }
        }

        for (int i = 0, n = symbolCounts.size(); i < n; i++) {
            if (symbolCounts.getQuick(i) == 0) {
                symbolCounts.setQuick(i, Integer.MAX_VALUE);
            }
        }
        return symbolCounts;
    }

    private boolean analyze(
            AliasTranslator translator,
            ExpressionNode node,
            RecordMetadata metadata
    ) throws SqlException {
        switch (ops.get(node.token)) {
            case OP_IN:
                analyzeIn(translator, node, metadata);
                return true;
            case OP_EQUAL:
                analyzeEquals(translator, node, metadata);
                return true;
            case OP_NOT_EQ:
                analyzeNotEquals(translator, node, metadata);
                return true;
            case OP_NOT:
                if (isInKeyword(node.rhs.token)) {
                    analyzeNotIn(translator, node.rhs, metadata);
                } else {
                    giveUp();
                }
                return true;
            default:
                return false;
        }
    }

    private void analyzeEquals(
            AliasTranslator translator,
            ExpressionNode node,
            RecordMetadata metadata
    ) throws SqlException {
        estimateEquals0(translator, node.lhs, node.rhs, metadata); // left == right
        estimateEquals0(translator, node.rhs, node.lhs, metadata); // right == left
    }

    private void analyzeIn(
            AliasTranslator translator,
            ExpressionNode node,
            RecordMetadata metadata
    ) throws SqlException {
        if (node.paramCount < 2) {
            return;
        }

        ExpressionNode col = node.paramCount < 3 ? node.lhs : node.args.getLast();
        if (col.type != ExpressionNode.LITERAL) {
            return;
        }

        CharSequence column = translator.translateAlias(col.token);
        int columnIndex = metadata.getColumnIndexQuiet(column);
        if (columnIndex == -1) {
            throw SqlException.invalidColumn(node.position, node.token);
        }

        int keyIndex = columnIndexesToListIndexes.keyIndex(columnIndex);
        if (keyIndex > -1) {
            return;
        }

        if (node.rhs != null && node.rhs.type == ExpressionNode.QUERY) {
            // give up in sym in (select ...) case
            giveUp();
            return;
        }

        int listIndex = columnIndexesToListIndexes.valueAt(keyIndex);
        symbolCounts.increment(listIndex, node.paramCount - 1);
    }

    private void analyzeNotEquals(
            AliasTranslator translator,
            ExpressionNode node,
            RecordMetadata metadata
    ) throws SqlException {
        estimateNotEquals0(translator, node.lhs, metadata); // left == right
        estimateNotEquals0(translator, node.rhs, metadata); // right == left
    }

    private void analyzeNotIn(
            AliasTranslator translator,
            ExpressionNode node,
            RecordMetadata metadata
    ) throws SqlException {
        if (node.paramCount < 2) {
            return;
        }

        ExpressionNode col = node.paramCount < 3 ? node.lhs : node.args.getLast();
        if (col.type != ExpressionNode.LITERAL) {
            return;
        }

        CharSequence column = translator.translateAlias(col.token);
        int columnIndex = metadata.getColumnIndexQuiet(column);
        if (columnIndex == -1) {
            throw SqlException.invalidColumn(node.position, node.token);
        }

        int keyIndex = columnIndexesToListIndexes.keyIndex(columnIndex);
        if (keyIndex > -1) {
            return;
        }

        // give up in not sym not in ('a','b','c') case
        giveUp();
    }

    private void estimateEquals0(
            AliasTranslator translator,
            ExpressionNode a,
            ExpressionNode b,
            RecordMetadata metadata
    ) throws SqlException {
        if (a == null || a.type != ExpressionNode.LITERAL) {
            return;
        }

        CharSequence column = translator.translateAlias(a.token);
        int columnIndex = metadata.getColumnIndexQuiet(column);
        if (columnIndex == -1) {
            throw SqlException.invalidColumn(a.position, a.token);
        }

        int keyIndex = columnIndexesToListIndexes.keyIndex(columnIndex);
        if (keyIndex > -1) {
            return;
        }

        if (b.type != ExpressionNode.CONSTANT && b.type != ExpressionNode.BIND_VARIABLE) {
            // give up in cases such as s1 = s2
            giveUp();
            return;
        }

        int listIndex = columnIndexesToListIndexes.valueAt(keyIndex);
        symbolCounts.increment(listIndex);
    }

    private void estimateNotEquals0(
            AliasTranslator translator,
            ExpressionNode node,
            RecordMetadata metadata
    ) throws SqlException {
        if (node == null || node.type != ExpressionNode.LITERAL) {
            return;
        }

        CharSequence column = translator.translateAlias(node.token);
        int columnIndex = metadata.getColumnIndexQuiet(column);
        if (columnIndex == -1) {
            throw SqlException.invalidColumn(node.position, node.token);
        }

        int keyIndex = columnIndexesToListIndexes.keyIndex(columnIndex);
        if (keyIndex > -1) {
            return;
        }

        // give up in sym != 'a' case
        giveUp();
    }

    private void giveUp() {
        gaveUp = true;
        symbolCounts.setAll(symbolCounts.size(), Integer.MAX_VALUE);
    }

    static {
        ops.put("=", OP_EQUAL);
        ops.put("in", OP_IN);
        ops.put("not", OP_NOT);
        ops.put("!=", OP_NOT_EQ);
    }
}

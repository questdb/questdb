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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;

import java.util.ArrayDeque;

/**
 * Extracts pushdown filter conditions from a filter expression.
 * <p>
 * These conditions can be used for Parquet row group bloom filter
 * to skip row groups that don't contain matching values.
 * <p>
 * Supported conditions:
 * 1. col = constant/bindVar (equality)
 * 2. col IN (constant/bindVar, ...) (IN list)
 * <p>
 * Multiple AND conditions form multiple PushdownFilterCondition entries.
 */
public class PushdownFilterExtractor implements Mutable {

    private final ObjList<PushdownFilterCondition> conditions = new ObjList<>();

    @Override
    public void clear() {
        conditions.clear();
    }

    public ObjList<PushdownFilterCondition> extract(
            ArrayDeque<ExpressionNode> stack,
            ExpressionNode filterExpr,
            RecordMetadata metadata
    ) {
        conditions.clear();
        if (filterExpr != null) {
            traverse(stack, filterExpr, metadata);
        }
        return conditions;
    }

    public ObjList<PushdownFilterCondition> getConditions() {
        return conditions;
    }

    private static boolean isValueNode(ExpressionNode node) {
        return node != null
                && (node.type == ExpressionNode.CONSTANT
                || node.type == ExpressionNode.BIND_VARIABLE);
    }

    private void traverse(ArrayDeque<ExpressionNode> stack, ExpressionNode node, RecordMetadata metadata) {
        stack.clear();

        while (!stack.isEmpty() || node != null) {
            if (node != null) {
                if (SqlKeywords.isAndKeyword(node.token)) {
                    if (node.rhs != null) {
                        stack.push(node.rhs);
                    }
                    node = node.lhs;
                } else {
                    if (Chars.equals(node.token, "=")) {
                        tryExtractEquality(node, metadata);
                    } else if (SqlKeywords.isInKeyword(node.token)) {
                        tryExtractIn(node, metadata);
                    }
                    node = null;
                }
            } else {
                node = stack.poll();
            }
        }
    }

    private void tryExtractEquality(ExpressionNode node, RecordMetadata metadata) {
        if (node.lhs == null || node.rhs == null) {
            return;
        }

        ExpressionNode colNode;
        ExpressionNode valueNode;
        if (node.lhs.type == ExpressionNode.LITERAL && isValueNode(node.rhs)) {
            colNode = node.lhs;
            valueNode = node.rhs;
        } else if (node.rhs.type == ExpressionNode.LITERAL && isValueNode(node.lhs)) {
            colNode = node.rhs;
            valueNode = node.lhs;
        } else {
            return;
        }
        int columnIndex = metadata.getColumnIndexQuiet(colNode.token);
        if (columnIndex < 0) {
            return;
        }

        int columnType = metadata.getColumnType(columnIndex);
        PushdownFilterCondition condition = new PushdownFilterCondition(
                columnIndex,
                columnType
        );
        condition.addValue(valueNode);
        conditions.add(condition);
    }

    private void tryExtractIn(ExpressionNode node, RecordMetadata metadata) {
        ExpressionNode colNode = node.paramCount < 3 ? node.lhs : node.args.getLast();
        if (colNode == null || colNode.type != ExpressionNode.LITERAL) {
            return;
        }

        int columnIndex = metadata.getColumnIndexQuiet(colNode.token);
        if (columnIndex < 0) {
            return;
        }

        int valueCount;
        if (node.paramCount < 3) {
            if (!isValueNode(node.rhs)) {
                return;
            }
            valueCount = 1;
        } else {
            valueCount = node.paramCount - 1;
            for (int i = 0; i < valueCount; i++) {
                ExpressionNode valueNode = node.args.getQuick(i);
                if (!isValueNode(valueNode)) {
                    return;
                }
            }
        }

        int columnType = metadata.getColumnType(columnIndex);
        PushdownFilterCondition condition = new PushdownFilterCondition(
                columnIndex,
                columnType
        );

        if (node.paramCount < 3) {
            condition.addValue(node.rhs);
        } else {
            for (int i = 0; i < valueCount; i++) {
                condition.addValue(node.args.getQuick(i));
            }
        }
        conditions.add(condition);
    }

    public static class PushdownFilterCondition implements QuietCloseable {
        private final int columnIndex;
        private final int columnType;
        private final ObjList<Function> valueFunctions = new ObjList<>();
        private final ObjList<ExpressionNode> values = new ObjList<>();

        public PushdownFilterCondition(int columnIndex, int columnType) {
            this.columnIndex = columnIndex;
            this.columnType = columnType;
        }

        public void addValue(ExpressionNode valueNode) {
            values.add(valueNode);
        }

        public void addValueFunction(Function valueFunction) {
            valueFunctions.add(valueFunction);
        }

        @Override
        public void close() {
            Misc.freeObjListAndClear(valueFunctions);
        }

        public int getColumnIndex() {
            return columnIndex;
        }

        public int getColumnType() {
            return columnType;
        }

        public ObjList<ExpressionNode> getValues() {
            return values;
        }
    }
}

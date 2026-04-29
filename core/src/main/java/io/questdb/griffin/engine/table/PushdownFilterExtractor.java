/*+*****************************************************************************
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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.Chars;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;

import java.util.ArrayDeque;

/**
 * Extracts pushdown filter conditions from a filter expression.
 * <p>
 * These conditions can be used for Parquet row group pruning
 * via bloom filters, min/max statistics, and null counts.
 * <p>
 * Supported conditions:
 * 1. col = expr (equality)
 * 2. col IN (expr, ...) (IN list)
 * 3. col &lt; expr, col &lt;= expr, col &gt; expr, col &gt;= expr (range)
 * 4. col BETWEEN lo AND hi (single OP_BETWEEN with two bounds)
 * 5. col IS NULL / col IS NOT NULL (null checks)
 * 6. col = v1 OR col = v2 OR ... (OR of equalities on same column)
 * <p>
 * Multiple AND conditions form multiple PushdownFilterCondition entries.
 * Value expressions are validated later (after parsing into Functions)
 * to ensure they are constant or runtime-constant.
 */
public class PushdownFilterExtractor implements Mutable {

    // Operation types for pushdown filter conditions.
    // Keep in sync with FILTER_OP_* constants in parquet_read/mod.rs.
    // Range semantics (LT/LE/GT/GE): skip row group if
    //   LT: min >= val, LE: min > val, GT: max <= val, GE: max < val.
    // BETWEEN: auto-swaps bounds, skip if max_stat < min(a,b) || min_stat > max(a,b).
    public static final int OP_BETWEEN = 7;
    public static final int OP_EQ = 0;
    public static final int OP_GE = 4;
    public static final int OP_GT = 3;
    public static final int OP_IS_NOT_NULL = 6;
    public static final int OP_IS_NULL = 5;
    public static final int OP_LE = 2;
    public static final int OP_LT = 1;

    private final ObjList<PushdownFilterCondition> conditions = new ObjList<>();
    private final ObjList<ExpressionNode> orValues = new ObjList<>();

    @Override
    public void clear() {
        conditions.clear();
        orValues.clear();
    }

    public ObjList<PushdownFilterCondition> extractAndCompile(
            ArrayDeque<ExpressionNode> stack,
            ArrayDeque<ExpressionNode> stack2,
            ExpressionNode filterExpr,
            RecordMetadata metadata,
            FunctionParser functionParser,
            SqlExecutionContext executionContext
    ) throws SqlException {
        conditions.clear();
        if (filterExpr != null) {
            traverse(stack, stack2, filterExpr, metadata);
        }

        ObjList<PushdownFilterCondition> result = null;
        PushdownFilterCondition condition = null;
        try {
            for (int i = 0, n = conditions.size(); i < n; i++) {
                condition = conditions.getQuick(i);
                ObjList<ExpressionNode> values = condition.getValues();
                boolean allConstant = true;
                for (int j = 0, m = values.size(); j < m; j++) {
                    ExpressionNode node = values.getQuick(j);
                    if (containsQuery(stack, node)) {
                        allConstant = false;
                        break;
                    }

                    Function f = functionParser.parseFunction(node, metadata, executionContext);
                    if (!f.isConstantOrRuntimeConstant()) {
                        condition.addValueFunction(f);
                        allConstant = false;
                        break;
                    }
                    // Pushdown reads the literal's raw value via getDecimal<N>(null)
                    // dispatched on the column's storage tag, then compares the bytes
                    // against parquet row group min/max statistics. For that to be
                    // correct the literal must share the column's storage tag and
                    // scale. Rescale the constant to match when possible; abandon
                    // the condition only when rescale would lose precision or
                    // overflow the column's storage range.
                    final int colType = condition.getColumnType();
                    if (ColumnType.isDecimal(colType) && ColumnType.isDecimal(f.getType())) {
                        Function rescaled = rescaleDecimalForPushdown(f, colType, executionContext);
                        if (rescaled == null) {
                            condition.addValueFunction(f);
                            allConstant = false;
                            break;
                        }
                        f = rescaled;
                    }
                    condition.addValueFunction(f);
                }
                if (allConstant) {
                    if (result == null) {
                        result = new ObjList<>();
                    }
                    result.add(condition);
                } else {
                    Misc.free(condition);
                }
            }
        } catch (Throwable e) {
            Misc.free(condition);
            Misc.freeObjList(result);
            throw e;
        }

        return result;
    }

    private static boolean containsQuery(ArrayDeque<ExpressionNode> stack, ExpressionNode node) {
        stack.clear();
        stack.push(node);
        while (!stack.isEmpty()) {
            ExpressionNode current = stack.poll();
            if (current.type == ExpressionNode.QUERY) {
                return true;
            }
            if (current.lhs != null) {
                stack.push(current.lhs);
            }
            if (current.rhs != null) {
                stack.push(current.rhs);
            }
            for (int i = 0, n = current.args.size(); i < n; i++) {
                ExpressionNode arg = current.args.getQuick(i);
                if (arg != null) {
                    stack.push(arg);
                }
            }
        }
        return false;
    }

    private static int flipComparison(int opType) {
        return switch (opType) {
            case OP_LT -> OP_GT;
            case OP_LE -> OP_GE;
            case OP_GT -> OP_LT;
            case OP_GE -> OP_LE;
            default -> opType;
        };
    }

    private static boolean isNullConstant(ExpressionNode node) {
        return node.type == ExpressionNode.CONSTANT && SqlKeywords.isNullKeyword(node.token);
    }

    /**
     * Rebuilds a constant DECIMAL function so its storage tag and scale match the
     * column's, which is what {@link ParquetRowGroupFilter#prepareFilterList} relies
     * on when it dispatches {@code getDecimal<N>} based on the column tag and pushes
     * the raw bytes against parquet row group statistics. Returns {@code null} when
     * the constant cannot be expressed at the column's scale (lossy scale-down) or
     * does not fit in the column's storage size, signalling the caller to skip
     * pushdown for the condition. Returns the input unchanged when scale and tag
     * already match. On a successful rebuild the original function is closed and
     * the new constant takes its place.
     */
    private static Function rescaleDecimalForPushdown(
            Function f,
            int colType,
            SqlExecutionContext executionContext
    ) {
        final int colTag = ColumnType.tagOf(colType);
        final int litTag = ColumnType.tagOf(f.getType());
        final int colScale = ColumnType.getDecimalScale(colType);
        final int litScale = ColumnType.getDecimalScale(f.getType());
        if (colTag == litTag && colScale == litScale) {
            return f;
        }

        final int colPrecision = ColumnType.getDecimalPrecision(colType);
        final Decimal256 d256 = executionContext.getDecimal256();
        final Decimal128 d128 = executionContext.getDecimal128();
        DecimalUtil.load(d256, d128, f, null);

        if (d256.isNull()) {
            Function nullFunc = DecimalUtil.createNullDecimalConstant(colPrecision, colScale);
            f.close();
            return nullFunc;
        }

        try {
            d256.rescale(colScale);
        } catch (NumericException e) {
            return null;
        }

        final int colStorageSizePow2 = Decimals.getStorageSizePow2(colPrecision);
        if (!d256.fitsInStorageSizePow2(colStorageSizePow2)) {
            return null;
        }

        Function newFunc = DecimalUtil.createDecimalConstant(d256, colPrecision, colScale);
        f.close();
        return newFunc;
    }

    private void traverse(ArrayDeque<ExpressionNode> stack, ArrayDeque<ExpressionNode> stack2, ExpressionNode node, RecordMetadata metadata) {
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
                    } else if (Chars.equals(node.token, "!=")) {
                        tryExtractNotEqual(node, metadata);
                    } else if (Chars.equals(node.token, "<")) {
                        tryExtractComparison(node, metadata, OP_LT);
                    } else if (Chars.equals(node.token, "<=")) {
                        tryExtractComparison(node, metadata, OP_LE);
                    } else if (Chars.equals(node.token, ">")) {
                        tryExtractComparison(node, metadata, OP_GT);
                    } else if (Chars.equals(node.token, ">=")) {
                        tryExtractComparison(node, metadata, OP_GE);
                    } else if (SqlKeywords.isInKeyword(node.token)) {
                        tryExtractIn(node, metadata);
                    } else if (SqlKeywords.isBetweenKeyword(node.token)) {
                        tryExtractBetween(node, metadata);
                    } else if (SqlKeywords.isOrKeyword(node.token)) {
                        tryExtractOrEqualities(stack2, node, metadata);
                    }
                    node = null;
                }
            } else {
                node = stack.poll();
            }
        }
    }

    private void tryExtractBetween(ExpressionNode node, RecordMetadata metadata) {
        if (node.paramCount != 3 || node.args.size() < 3) {
            return;
        }

        ExpressionNode colNode = node.args.getLast();
        if (colNode == null || colNode.type != ExpressionNode.LITERAL) {
            return;
        }

        int columnIndex = metadata.getColumnIndexQuiet(colNode.token);
        if (columnIndex < 0) {
            return;
        }

        ExpressionNode loNode = node.args.getQuick(1);
        ExpressionNode hiNode = node.args.getQuick(0);

        int columnType = metadata.getColumnType(columnIndex);
        PushdownFilterCondition cond = new PushdownFilterCondition(colNode.token, columnType, OP_BETWEEN);
        cond.addValue(loNode);
        cond.addValue(hiNode);
        conditions.add(cond);
    }

    private void tryExtractComparison(ExpressionNode node, RecordMetadata metadata, int opType) {
        if (node.lhs == null || node.rhs == null) {
            return;
        }

        ExpressionNode colNode;
        ExpressionNode valueNode;
        int effectiveOp = opType;
        if (node.lhs.type == ExpressionNode.LITERAL && node.rhs.type != ExpressionNode.LITERAL) {
            colNode = node.lhs;
            valueNode = node.rhs;
        } else if (node.rhs.type == ExpressionNode.LITERAL && node.lhs.type != ExpressionNode.LITERAL) {
            colNode = node.rhs;
            valueNode = node.lhs;
            effectiveOp = flipComparison(opType);
        } else {
            return;
        }

        int columnIndex = metadata.getColumnIndexQuiet(colNode.token);
        if (columnIndex < 0) {
            return;
        }

        int columnType = metadata.getColumnType(columnIndex);
        PushdownFilterCondition condition = new PushdownFilterCondition(colNode.token, columnType, effectiveOp);
        condition.addValue(valueNode);
        conditions.add(condition);
    }

    private void tryExtractEquality(ExpressionNode node, RecordMetadata metadata) {
        if (node.lhs == null || node.rhs == null) {
            return;
        }

        ExpressionNode colNode;
        ExpressionNode valueNode;
        if (node.lhs.type == ExpressionNode.LITERAL && node.rhs.type != ExpressionNode.LITERAL) {
            colNode = node.lhs;
            valueNode = node.rhs;
        } else if (node.rhs.type == ExpressionNode.LITERAL && node.lhs.type != ExpressionNode.LITERAL) {
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

        if (isNullConstant(valueNode)) {
            conditions.add(new PushdownFilterCondition(colNode.token, columnType, OP_IS_NULL));
            return;
        }

        PushdownFilterCondition condition = new PushdownFilterCondition(colNode.token, columnType);
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
            if (node.rhs == null) {
                return;
            }
            valueCount = 1;
        } else {
            valueCount = node.paramCount - 1;
        }

        int columnType = metadata.getColumnType(columnIndex);
        PushdownFilterCondition condition = new PushdownFilterCondition(
                colNode.token,
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

    private void tryExtractNotEqual(ExpressionNode node, RecordMetadata metadata) {
        if (node.lhs == null || node.rhs == null) {
            return;
        }

        ExpressionNode colNode;
        ExpressionNode valueNode;
        if (node.lhs.type == ExpressionNode.LITERAL && node.rhs.type != ExpressionNode.LITERAL) {
            colNode = node.lhs;
            valueNode = node.rhs;
        } else if (node.rhs.type == ExpressionNode.LITERAL && node.lhs.type != ExpressionNode.LITERAL) {
            colNode = node.rhs;
            valueNode = node.lhs;
        } else {
            return;
        }

        if (!isNullConstant(valueNode)) {
            return;
        }

        int columnIndex = metadata.getColumnIndexQuiet(colNode.token);
        if (columnIndex < 0) {
            return;
        }

        int columnType = metadata.getColumnType(columnIndex);
        conditions.add(new PushdownFilterCondition(colNode.token, columnType, OP_IS_NOT_NULL));
    }

    private void tryExtractOrEqualities(ArrayDeque<ExpressionNode> orStack, ExpressionNode node, RecordMetadata metadata) {
        orStack.clear();
        orValues.clear();
        CharSequence columnName = null;
        int columnType = -1;
        int resolvedColumnIndex = -1;

        ExpressionNode cur = node;
        while (cur != null || !orStack.isEmpty()) {
            if (cur == null) {
                cur = orStack.poll();
            }
            if (SqlKeywords.isOrKeyword(cur.token)) {
                if (cur.lhs == null || cur.rhs == null) {
                    return;
                }
                orStack.push(cur.rhs);
                cur = cur.lhs;
                continue;
            }

            if (!Chars.equals(cur.token, "=") || cur.lhs == null || cur.rhs == null) {
                return;
            }

            ExpressionNode colNode;
            ExpressionNode valueNode;
            if (cur.lhs.type == ExpressionNode.LITERAL && cur.rhs.type != ExpressionNode.LITERAL) {
                colNode = cur.lhs;
                valueNode = cur.rhs;
            } else if (cur.rhs.type == ExpressionNode.LITERAL && cur.lhs.type != ExpressionNode.LITERAL) {
                colNode = cur.rhs;
                valueNode = cur.lhs;
            } else {
                return;
            }

            int columnIndex = metadata.getColumnIndexQuiet(colNode.token);
            if (columnIndex < 0) {
                return;
            }
            if (resolvedColumnIndex < 0) {
                resolvedColumnIndex = columnIndex;
                columnName = colNode.token;
                columnType = metadata.getColumnType(columnIndex);
            } else if (columnIndex != resolvedColumnIndex) {
                return;
            }

            orValues.add(valueNode);
            cur = null;
        }

        if (orValues.size() > 0) {
            PushdownFilterCondition condition = new PushdownFilterCondition(columnName, columnType);
            condition.addValues(orValues);
            conditions.add(condition);
        }
    }

    // Not pooled: conditions are passed to RecordCursorFactory and live for the duration of query execution.
    public static class PushdownFilterCondition implements QuietCloseable {
        private final CharSequence columnName;
        private final int columnType;
        private final int operationType;
        private final ObjList<Function> valueFunctions = new ObjList<>();
        private final ObjList<ExpressionNode> values = new ObjList<>();

        public PushdownFilterCondition(CharSequence columnName, int columnType) {
            this(columnName, columnType, OP_EQ);
        }

        public PushdownFilterCondition(CharSequence columnName, int columnType, int operationType) {
            this.columnName = Chars.toString(columnName);
            this.columnType = columnType;
            this.operationType = operationType;
        }

        public void addValue(ExpressionNode valueNode) {
            values.add(valueNode);
        }

        public void addValueFunction(Function valueFunction) {
            valueFunctions.add(valueFunction);
        }

        public void addValues(ObjList<ExpressionNode> values1) {
            values.addAll(values1);
        }

        @Override
        public void close() {
            Misc.freeObjListAndClear(valueFunctions);
        }

        public CharSequence getColumnName() {
            return columnName;
        }

        public int getColumnType() {
            return columnType;
        }

        public int getOperationType() {
            return operationType;
        }

        public ObjList<Function> getValueFunctions() {
            return valueFunctions;
        }

        public ObjList<ExpressionNode> getValues() {
            return values;
        }

        public void init(SqlExecutionContext executionContext) throws SqlException {
            for (int i = 0, n = valueFunctions.size(); i < n; i++) {
                valueFunctions.getQuick(i).init(null, executionContext);
            }
        }
    }
}

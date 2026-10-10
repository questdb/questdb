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

package io.questdb.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.table.PushdownFilterExtractor.PushdownFilterCondition;
import io.questdb.griffin.engine.table.PushdownFilterExtractor;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.ExpressionVisitor;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

/**
 * Derives Parquet row-group pushdown conditions from a bound residual predicate.
 */
public final class ParquetPushdownExtractor {
    private static final ExpressionVisitor CURSORS = expression -> expression instanceof CursorExpression ? TreeWalk.STOP : TreeWalk.CONTINUE;
    private final ObjList<BoundExpression> conditionValues = new ObjList<>();
    private final ObjList<PushdownFilterCondition> conditions = new ObjList<>();
    private final IntList valueCounts = new IntList();

    /**
     * Returns the conditions the caller owns, or null when none survive compilation.
     *
     * @param sourceIndexes maps input positions to {@code source} positions; null when they coincide
     */
    public ObjList<PushdownFilterCondition> extract(
            BoundExpression predicate,
            OutputSchema input,
            RecordMetadata metadata,
            IntList sourceIndexes,
            RecordMetadata source,
            FunctionInstantiator instantiator,
            SqlExecutionContext executionContext
    ) throws SqlException {
        conditions.clear();
        conditionValues.clear();
        valueCounts.clear();
        ObjList<PushdownFilterCondition> result = null;
        try {
            collect(predicate, input, sourceIndexes, source);
            for (int i = 0, v = 0, n = conditions.size(); i < n; i++) {
                final PushdownFilterCondition condition = conditions.getQuick(i);
                final int count = valueCounts.getQuick(i);
                boolean isConstant = true;
                for (int k = 0; k < count && isConstant; k++) {
                    isConstant = addValue(condition, conditionValues.getQuick(v + k), input, metadata, instantiator, executionContext);
                }
                v += count;
                conditions.setQuick(i, null);
                if (isConstant) {
                    if (result == null) {
                        result = new ObjList<>();
                    }
                    result.add(condition);
                } else {
                    Misc.free(condition);
                }
            }
            return result;
        } catch (Throwable th) {
            Misc.freeObjList(conditions, th);
            Misc.freeObjList(result, th);
            throw th;
        } finally {
            conditions.clear();
            conditionValues.clear();
            valueCounts.clear();
        }
    }

    private static boolean addValue(
            PushdownFilterCondition condition,
            BoundExpression value,
            OutputSchema input,
            RecordMetadata metadata,
            FunctionInstantiator instantiator,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (hasCursor(value)) {
            return false;
        }
        Function function = instantiator.instantiate(value, input, metadata, executionContext);
        if (!function.isConstantOrRuntimeConstant()) {
            condition.addValueFunction(function);
            return false;
        }
        final int columnType = condition.getColumnType();
        if (ColumnType.isDecimal(columnType) && ColumnType.isDecimal(function.getType())) {
            final int type = function.getType();
            final boolean isSameStorage = ColumnType.tagOf(columnType) == ColumnType.tagOf(type)
                    && ColumnType.getDecimalScale(columnType) == ColumnType.getDecimalScale(type);
            final Function rescaled = isSameStorage ? function : function.isConstant()
                                                                 ? PushdownFilterExtractor.rescaleDecimalForPushdown(function, columnType, executionContext) : null;
            if (rescaled == null) {
                condition.addValueFunction(function);
                return false;
            }
            function = rescaled;
        }
        condition.addValueFunction(function);
        return true;
    }

    private static int flip(int operation) {
        return switch (operation) {
            case PushdownFilterExtractor.OP_LT -> PushdownFilterExtractor.OP_GT;
            case PushdownFilterExtractor.OP_LE -> PushdownFilterExtractor.OP_GE;
            case PushdownFilterExtractor.OP_GT -> PushdownFilterExtractor.OP_LT;
            case PushdownFilterExtractor.OP_GE -> PushdownFilterExtractor.OP_LE;
            default -> operation;
        };
    }

    private static boolean hasCursor(BoundExpression expression) {
        return !expression.walk(CURSORS);
    }

    private static boolean isNull(BoundExpression expression) {
        return expression instanceof ConstantExpression && expression.getDataType() == ColumnType.NULL;
    }

    private static int operation(String name) {
        return switch (name) {
            case "=" -> PushdownFilterExtractor.OP_EQ;
            case "<" -> PushdownFilterExtractor.OP_LT;
            case "<=" -> PushdownFilterExtractor.OP_LE;
            case ">" -> PushdownFilterExtractor.OP_GT;
            case ">=" -> PushdownFilterExtractor.OP_GE;
            case "!=", "<>" -> PushdownFilterExtractor.OP_IS_NOT_NULL;
            default -> PushdownFilterExtractor.OP_UNSUPPORTED;
        };
    }

    private static int sourceIndex(BoundExpression expression, OutputSchema input, IntList sourceIndexes) {
        if (!(expression instanceof ColumnExpression column) || !column.isDirectReference()) {
            return -1;
        }
        final int index = input.getColumnIndexById(column.getColumnId());
        return index < 0 || sourceIndexes == null ? index : sourceIndexes.getQuick(index);
    }

    private void add(RecordMetadata source, int index, int operation, BoundExpression value) {
        conditions.add(new PushdownFilterCondition(source.getColumnName(index), source.getWriterIndex(index),
                source.getColumnType(index), operation));
        valueCounts.add(value == null ? 0 : 1);
        if (value != null) {
            conditionValues.add(value);
        }
    }

    private void collect(BoundExpression expression, OutputSchema input, IntList sourceIndexes, RecordMetadata source) {
        if (!(expression instanceof FunctionExpression call)) {
            return;
        }
        final String name = call.getName();
        if (call.isAnd()) {
            collect(call.argumentAt(0), input, sourceIndexes, source);
            collect(call.argumentAt(1), input, sourceIndexes, source);
        } else if (call.isOr()) {
            collectOr(call, input, sourceIndexes, source);
        } else if ("in".equals(name) && call.getArgumentCount() >= 2) {
            final int index = sourceIndex(call.argumentAt(0), input, sourceIndexes);
            if (index >= 0) {
                conditions.add(new PushdownFilterCondition(source.getColumnName(index), source.getWriterIndex(index), source.getColumnType(index)));
                valueCounts.add(call.getArgumentCount() - 1);
                for (int i = 1, n = call.getArgumentCount(); i < n; i++) {
                    conditionValues.add(call.argumentAt(i));
                }
            }
        } else if ("between".equals(name) && call.getArgumentCount() == 3) {
            final int index = sourceIndex(call.argumentAt(0), input, sourceIndexes);
            if (index >= 0) {
                conditions.add(new PushdownFilterCondition(source.getColumnName(index), source.getWriterIndex(index),
                        source.getColumnType(index), PushdownFilterExtractor.OP_BETWEEN));
                valueCounts.add(2);
                conditionValues.add(call.argumentAt(1));
                conditionValues.add(call.argumentAt(2));
            }
        } else if (call.getArgumentCount() == 2) {
            final int operation = operation(name);
            if (operation == PushdownFilterExtractor.OP_UNSUPPORTED) {
                return;
            }
            int index = sourceIndex(call.argumentAt(0), input, sourceIndexes);
            BoundExpression value = call.argumentAt(1);
            int effective = operation;
            if (index < 0 || value instanceof ColumnExpression) {
                if (call.argumentAt(0) instanceof ColumnExpression) {
                    return;
                }
                index = sourceIndex(call.argumentAt(1), input, sourceIndexes);
                value = call.argumentAt(0);
                effective = flip(operation);
            }
            if (index < 0) {
                return;
            }
            final int type = source.getColumnType(index);
            if (operation == PushdownFilterExtractor.OP_IS_NOT_NULL) {
                if (isNull(value) && PushdownFilterExtractor.isNullOpPushable(type, PushdownFilterExtractor.OP_IS_NOT_NULL)) {
                    add(source, index, PushdownFilterExtractor.OP_IS_NOT_NULL, null);
                }
            } else if (operation == PushdownFilterExtractor.OP_EQ && isNull(value)) {
                if (PushdownFilterExtractor.isNullOpPushable(type, PushdownFilterExtractor.OP_IS_NULL)) {
                    add(source, index, PushdownFilterExtractor.OP_IS_NULL, null);
                }
            } else {
                add(source, index, effective, value);
            }
        }
    }

    private void collectOr(FunctionExpression call, OutputSchema input, IntList sourceIndexes, RecordMetadata source) {
        final int valueStart = conditionValues.size();
        final int index = collectOrValues(call, input, sourceIndexes, -1);
        if (index < 0) {
            conditionValues.setPos(valueStart);
            return;
        }
        conditions.add(new PushdownFilterCondition(source.getColumnName(index), source.getWriterIndex(index), source.getColumnType(index)));
        valueCounts.add(conditionValues.size() - valueStart);
    }

    /**
     * Returns the common source column of an OR chain of equalities, or -1.
     */
    private int collectOrValues(BoundExpression expression, OutputSchema input, IntList sourceIndexes, int index) {
        if (!(expression instanceof FunctionExpression call) || call.getArgumentCount() != 2) {
            return -1;
        }
        if (call.isOr()) {
            final int left = collectOrValues(call.argumentAt(0), input, sourceIndexes, index);
            return left < 0 ? -1 : collectOrValues(call.argumentAt(1), input, sourceIndexes, left);
        }
        if (!"=".equals(call.getName())) {
            return -1;
        }
        int column = sourceIndex(call.argumentAt(0), input, sourceIndexes);
        BoundExpression value = call.argumentAt(1);
        if (column < 0 || value instanceof ColumnExpression) {
            if (call.argumentAt(0) instanceof ColumnExpression) {
                return -1;
            }
            column = sourceIndex(call.argumentAt(1), input, sourceIndexes);
            value = call.argumentAt(0);
        }
        if (column < 0 || index >= 0 && column != index) {
            return -1;
        }
        conditionValues.add(value);
        return column;
    }
}

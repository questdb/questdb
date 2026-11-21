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

package io.questdb.griffin.optimiser;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.TimestampPrecisionUse;
import io.questdb.griffin.plan.logical.BindVariableExpression;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ComparisonOperator;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.LogicalPlans;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.TypeExpression;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Rewrites a conjunct over the designated timestamp of a set operation into the conjunct a branch evaluates over its
 * own column of that position, when the branch reads it at a coarser temporal type than the operation: DATE or
 * TIMESTAMP under a TIMESTAMP_NS operation, DATE under a TIMESTAMP one. The operation widens the branch's values
 * exactly, so the rewrite keeps the conjunct's meaning wherever the conjunct reads the column only as an instant:
 * <ul>
 * <li>an instant is the column, a cast of an instant to DATE or a timestamp, or a call that returns the same instant
 * at every precision ({@link TimestampPrecisionUse#PRESERVING}) over instants and exact arguments;</li>
 * <li>a comparison, IN or BETWEEN is exact when it compares instants only with each other and with timestamp values,
 * see {@link #isTimestampValue};</li>
 * <li>a call whose value does not depend on the precision ({@link TimestampPrecisionUse#INVARIANT}) is exact over
 * instants and exact arguments, and any other call is exact when every argument is exact or reads no column.</li>
 * </ul>
 * Every call over the column binds again over the branch's column through {@link OptimiserContext#bindCall}, as the
 * same SQL would bind at the branch's precision, so a comparison with a constant of the operation's precision takes
 * the binder's exact rounding: a lower bound rounds up and an upper bound down, an equality with a value the branch
 * cannot hold folds to false and its negation to true, and an IN element the branch cannot hold drops out. Bind
 * variables and runtime constants stay as they are; the comparison rounds them the same way when it evaluates them.
 * Parts that read no column are copied, so the branch filter shares no description with the conjunct.
 */
final class BranchPrecisionRewrite {
    // The expression reads the column as a value that does not depend on the precision.
    private static final int EXACT = 1;
    // The expression is an instant of the column, see the class comment.
    private static final int INSTANT = 2;
    // The expression reads the column at the operation's precision, or reads a sub-query.
    private static final int NOT_EXACT = 0;
    // The expression reads no column.
    private static final int VALUE = 3;
    // the arguments of the one call that binds at a time
    private final ObjList<BoundExpression> callArguments = new ObjList<>();
    private final ObjectPool<ColumnExpression> columns;
    private final OptimiserContext context;
    // the rebuilt arguments of the calls being rebuilt, a stack
    private final ObjList<BoundExpression> rebuiltArguments = new ObjList<>();
    private int branchColumnId;
    private int branchType;
    private int columnId;
    private boolean isDateBranch;

    BranchPrecisionRewrite(OptimiserContext context, ObjectPool<ColumnExpression> columns) {
        this.context = context;
        this.columns = columns;
    }

    /**
     * Whether a set operation of {@code type} widens a branch column of {@code branchType} exactly: a DATE under a
     * timestamp, or a TIMESTAMP under a TIMESTAMP_NS.
     */
    static boolean isWidenedTemporal(int branchType, int type) {
        return branchType == ColumnType.DATE ? ColumnType.isTimestamp(type)
                : ColumnType.isTimestampMicro(branchType) && ColumnType.isTimestampNano(type);
    }

    /**
     * The conjunct, which reads only the operation's column {@code columnId}, over the column at {@code columnIndex}
     * of the branch's {@code output}, whose type the operation widens exactly, see {@link #isWidenedTemporal}; null
     * when the conjunct reads the column at the operation's precision.
     */
    BoundExpression rewrite(BoundExpression conjunct, int columnId, OutputSchema output, int columnIndex) throws SqlException {
        this.columnId = columnId;
        branchColumnId = output.getColumnId(columnIndex);
        branchType = output.getColumnType(columnIndex);
        isDateBranch = branchType == ColumnType.DATE;
        return classify(conjunct) == EXACT ? rebuild(conjunct, output) : null;
    }

    private static boolean isComparison(FunctionExpression call) {
        final String name = call.getName();
        return ComparisonOperator.of(name) != null || SqlKeywords.isInKeyword(name) || SqlKeywords.isBetweenKeyword(name);
    }

    // A cast to DATE or a timestamp; a cast returns its target type.
    private static boolean isTemporalCast(FunctionExpression call) {
        final int target = call.getDataType();
        return SqlKeywords.isCastKeyword(call.getName()) && (target == ColumnType.DATE || ColumnType.isTimestamp(target));
    }

    /**
     * How the call's value depends on the precision of its TIMESTAMP argument, as its overload's factory answers for
     * its bound first argument. The call reads an instant, so it has an argument.
     */
    private static TimestampPrecisionUse precisionUse(FunctionExpression call) {
        return call.getOverload().getFactory().getTimestampPrecisionUse(call.argumentAt(0));
    }

    private int classify(BoundExpression expression) {
        return switch (expression) {
            case ColumnExpression column -> {
                assert column.getColumnId() == columnId : "set conjunct reads another column";
                yield INSTANT;
            }
            case FunctionExpression call -> classifyCall(call);
            case ConstantExpression _, BindVariableExpression _, TypeExpression _ -> VALUE;
            // A sub-query reads as a cursor, which is no timestamp value, so the conjunct stays above the branch;
            // decorrelation has removed every outer column.
            case CursorExpression _, OuterColumnExpression _ -> NOT_EXACT;
        };
    }

    private int classifyCall(FunctionExpression call) {
        boolean hasInstant = false;
        boolean isExact = true;
        boolean isTimestampCompared = true;
        boolean isValue = true;
        for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
            final BoundExpression argument = call.argumentAt(i);
            final int kind = classify(argument);
            hasInstant |= kind == INSTANT;
            isExact &= kind != NOT_EXACT;
            isTimestampCompared &= kind == INSTANT || kind == VALUE && isTimestampValue(argument);
            isValue &= kind == VALUE;
        }
        if (isValue) {
            return VALUE;
        }
        if (!isExact) {
            return NOT_EXACT;
        }
        if (!hasInstant) {
            return EXACT;
        }
        if (isComparison(call)) {
            return isTimestampCompared ? EXACT : NOT_EXACT;
        }
        if (isTemporalCast(call)) {
            return INSTANT;
        }
        return switch (precisionUse(call)) {
            case PRESERVING -> INSTANT;
            case INVARIANT -> EXACT;
            case DEPENDENT -> NOT_EXACT;
        };
    }

    /**
     * Whether the value, which reads no column, compares with an instant at the branch's precision as it does at the
     * operation's: a DATE or timestamp value, NULL, a text literal, or other text unless the branch reads the column as
     * DATE, whose typing truncates text to milliseconds. A number reads as a raw value of the operand's precision, so
     * it is not one.
     */
    private boolean isTimestampValue(BoundExpression value) {
        final int type = value.getDataType();
        if (type == ColumnType.DATE || ColumnType.isTimestamp(type) || ColumnType.isNull(type)) {
            return true;
        }
        final short tag = ColumnType.tagOf(type);
        return (tag == ColumnType.STRING || tag == ColumnType.VARCHAR)
                && (!isDateBranch || value instanceof ConstantExpression constant && constant.isLiteral());
    }

    /**
     * The expression over the branch's column: the column itself, a copy of a part that reads no column, or a call
     * bound again over its rebuilt arguments. A call's arguments wait on {@link #rebuiltArguments} until the last of
     * them is rebuilt, as rebuilding one binds the calls beneath it.
     */
    private BoundExpression rebuild(BoundExpression expression, OutputSchema output) throws SqlException {
        if (expression instanceof ColumnExpression column) {
            return columns.next().of(branchColumnId, branchType, column.getPosition());
        }
        if (!(expression instanceof FunctionExpression call) || !LogicalPlans.readsColumn(call, columnId)) {
            return context.getRewriter().copy(expression);
        }
        final int mark = rebuiltArguments.size();
        try {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                rebuiltArguments.add(rebuild(call.argumentAt(i), output));
            }
            callArguments.clear();
            for (int i = mark, n = rebuiltArguments.size(); i < n; i++) {
                callArguments.add(rebuiltArguments.getQuick(i));
            }
            return context.bindCall(call.getName(), call.getPosition(), callArguments, output);
        } finally {
            callArguments.clear();
            rebuiltArguments.setPos(mark);
        }
    }
}

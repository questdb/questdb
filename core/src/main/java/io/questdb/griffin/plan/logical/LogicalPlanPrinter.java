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

package io.questdb.griffin.plan.logical;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

/**
 * Renders a bound logical plan as an indented tree. Each node prints its operator and the
 * attributes that decide its semantics; expressions refer to columns by name. Sub-query plans
 * print after the main tree as numbered sections, each with its own column namespace.
 */
public final class LogicalPlanPrinter {
    private final IntObjHashMap<CharSequence> columnNames = new IntObjHashMap<>();
    private final IntObjHashMap<CharSequence> columnQualifiers = new IntObjHashMap<>();
    private final StringSink sink = new StringSink();
    private final ObjList<LogicalPlan> subqueries = new ObjList<>();

    public CharSequence print(LogicalPlan root) {
        sink.clear();
        subqueries.clear();
        printTree(root);
        for (int i = 0; i < subqueries.size(); i++) {
            sink.put("Subquery #").put(i + 1).put(":\n");
            printTree(subqueries.getQuick(i));
        }
        return sink;
    }

    private static boolean isOperator(CharSequence name) {
        return !name.isEmpty() && !Character.isLetterOrDigit(name.charAt(0)) && name.charAt(0) != '_';
    }

    private void attribute(int depth, String name) {
        indent(depth + 1);
        sink.put(name).put(": ");
    }

    private void column(int columnId) {
        final CharSequence name = columnNames.get(columnId);
        if (name == null) {
            sink.put('#').put(columnId);
            return;
        }
        final CharSequence qualifier = columnQualifiers.get(columnId);
        if (qualifier != null) {
            sink.put(qualifier).put('.');
        }
        sink.put(name);
    }

    private void columnList(IntList columnIds) {
        sink.put('[');
        for (int i = 0, n = columnIds.size(); i < n; i++) {
            if (i > 0) {
                sink.put(", ");
            }
            column(columnIds.getQuick(i));
        }
        sink.put(']');
    }

    private void constant(ConstantExpression constant) {
        final int type = constant.getDataType();
        switch (ColumnType.tagOf(type)) {
            case ColumnType.NULL -> sink.put("null");
            case ColumnType.BOOLEAN -> sink.put(constant.getLongValue() != 0);
            case ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.LONG, ColumnType.DATE -> sink.put(constant.getLongValue());
            case ColumnType.CHAR -> sink.put('\'').put((char) constant.getLongValue()).put('\'');
            case ColumnType.FLOAT -> sink.put(constant.getFloatValue());
            case ColumnType.DOUBLE -> sink.put(constant.getDoubleValue());
            case ColumnType.STRING, ColumnType.SYMBOL -> sink.put('\'').put(constant.getStrValue()).put('\'');
            case ColumnType.VARCHAR -> sink.put('\'').put(constant.getVarcharValue()).put('\'');
            case ColumnType.TIMESTAMP -> {
                if (constant.getTimestampText() != null) {
                    sink.put('\'').put(constant.getTimestampText()).put('\'');
                } else {
                    sink.put(constant.getLongValue());
                }
                sink.put("::").put(ColumnType.nameOf(type));
            }
            default -> sink.put(ColumnType.nameOf(type)).put(" constant");
        }
    }

    private void expression(BoundExpression expression) {
        switch (expression) {
            case null -> sink.put("null");
            case ColumnExpression column -> column(column.getColumnId());
            case ConstantExpression constant -> constant(constant);
            case BindVariableExpression bind -> sink.put(bind.getName());
            case CursorExpression cursor -> {
                subqueries.add(cursor.getPlan());
                sink.put("(subquery #").put(subqueries.size()).put(')');
            }
            case FunctionExpression function -> function(function);
            default -> sink.put(ColumnType.nameOf(expression.getDataType()));
        }
    }

    private void expressions(ObjList<? extends BoundExpression> expressions) {
        sink.put('[');
        for (int i = 0, n = expressions.size(); i < n; i++) {
            if (i > 0) {
                sink.put(", ");
            }
            expression(expressions.getQuick(i));
        }
        sink.put(']');
    }

    private void frame(WindowSpec spec) {
        switch (spec.getFramingMode()) {
            case WindowExpression.FRAMING_ROWS -> sink.put("rows");
            case WindowExpression.FRAMING_GROUPS -> sink.put("groups");
            default -> sink.put("range");
        }
        sink.put(" between ");
        frameBound(spec.getRowsLo(), spec.getRowsLoExprTimeUnit());
        sink.put(" and ");
        frameBound(spec.getRowsHi(), spec.getRowsHiExprTimeUnit());
        switch (spec.getExclusionKind()) {
            case WindowExpression.EXCLUDE_CURRENT_ROW -> sink.put(" exclude current row");
            case WindowExpression.EXCLUDE_GROUP -> sink.put(" exclude group");
            case WindowExpression.EXCLUDE_TIES -> sink.put(" exclude ties");
            default -> {
            }
        }
    }

    private void frameBound(long value, char timeUnit) {
        if (value == Long.MIN_VALUE) {
            sink.put("unbounded preceding");
            return;
        }
        if (value == Long.MAX_VALUE) {
            sink.put("unbounded following");
            return;
        }
        if (value == 0) {
            sink.put("current row");
            return;
        }
        sink.put(Math.abs(value));
        if (timeUnit != 0) {
            sink.put(timeUnit);
        }
        sink.put(value < 0 ? " preceding" : " following");
    }

    private void function(FunctionExpression function) {
        final String name = function.getName();
        final int count = function.getArgumentCount();
        if (isOperator(name) && count == 2) {
            operand(function.argumentAt(0));
            sink.put(' ').put(name).put(' ');
            operand(function.argumentAt(1));
            return;
        }
        if (isOperator(name) && count == 1) {
            sink.put(name);
            operand(function.argumentAt(0));
            return;
        }
        sink.put(name).put('(');
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                sink.put(", ");
            }
            expression(function.argumentAt(i));
        }
        sink.put(')');
    }

    private void indent(int depth) {
        for (int i = 0; i < depth; i++) {
            sink.put("  ");
        }
    }

    private void joinTypeName(int joinType) {
        switch (joinType) {
            case QueryModel.JOIN_INNER -> sink.put("INNER");
            case QueryModel.JOIN_LEFT_OUTER -> sink.put("LEFT");
            case QueryModel.JOIN_RIGHT_OUTER -> sink.put("RIGHT");
            case QueryModel.JOIN_FULL_OUTER -> sink.put("FULL");
            case QueryModel.JOIN_CROSS -> sink.put("CROSS");
            case QueryModel.JOIN_ASOF -> sink.put("ASOF");
            case QueryModel.JOIN_LT -> sink.put("LT");
            case QueryModel.JOIN_SPLICE -> sink.put("SPLICE");
            case QueryModel.JOIN_UNNEST -> sink.put("UNNEST");
            case QueryModel.JOIN_LATERAL_INNER -> sink.put("LATERAL INNER");
            case QueryModel.JOIN_LATERAL_LEFT -> sink.put("LATERAL LEFT");
            case QueryModel.JOIN_LATERAL_CROSS -> sink.put("LATERAL CROSS");
            case QueryModel.JOIN_CROSS_LEFT -> sink.put("CROSS LEFT");
            case QueryModel.JOIN_CROSS_RIGHT -> sink.put("CROSS RIGHT");
            case QueryModel.JOIN_CROSS_FULL -> sink.put("CROSS FULL");
            default -> sink.put("JOIN(").put(joinType).put(')');
        }
    }

    private void keyPairs(IntList masterIds, IntList slaveIds) {
        sink.put('[');
        for (int i = 0, n = masterIds.size(); i < n; i++) {
            if (i > 0) {
                sink.put(", ");
            }
            column(slaveIds.getQuick(i));
            sink.put(" = ");
            column(masterIds.getQuick(i));
        }
        sink.put(']');
    }

    private void named(BoundExpression expression, OutputSchema output, int index) {
        expression(expression);
        final CharSequence name = output.getColumnName(index);
        final CharSequence sourceName = expression instanceof ColumnExpression column ? columnNames.get(column.getColumnId()) : null;
        if (sourceName == null || !Chars.equals(name, sourceName)) {
            sink.put(" AS ").put(name);
        }
    }

    private void names(ObjList<CharSequence> names) {
        sink.put('[');
        for (int i = 0, n = names.size(); i < n; i++) {
            if (i > 0) {
                sink.put(", ");
            }
            sink.put(names.getQuick(i));
        }
        sink.put(']');
    }

    private void operand(BoundExpression expression) {
        final boolean isNested = expression instanceof FunctionExpression function
                && isOperator(function.getName()) && function.getArgumentCount() > 1;
        if (isNested) {
            sink.put('(');
        }
        expression(expression);
        if (isNested) {
            sink.put(')');
        }
    }

    private void outputNames(OutputSchema output) {
        sink.put('[');
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            if (i > 0) {
                sink.put(", ");
            }
            sink.put(output.getColumnName(i));
        }
        sink.put(']');
    }

    private void print(LogicalPlan plan, int depth) {
        indent(depth);
        switch (plan) {
            case ScanPlan scan -> printScan(scan, depth);
            case FunctionSourcePlan _ -> {
                sink.put("FunctionSource\n");
                attribute(depth, "columns");
                outputNames(plan.getOutput());
                sink.put('\n');
            }
            case FilterPlan filter -> {
                sink.put("Filter\n");
                attribute(depth, "predicate");
                expression(filter.getPredicate());
                sink.put('\n');
            }
            case ProjectPlan project -> {
                sink.put("Project\n");
                attribute(depth, "columns");
                sink.put('[');
                for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
                    if (i > 0) {
                        sink.put(", ");
                    }
                    named(project.getExpressions().getQuick(i), project.getOutput(), i);
                }
                sink.put("]\n");
            }
            case AggregatePlan aggregate -> printAggregate(aggregate, depth);
            case DistinctPlan _ -> sink.put("Distinct\n");
            case FillPlan fill -> printFill(fill, depth);
            case WindowPlan window -> printWindow(window, depth);
            case JoinPlan join -> {
                printJoin(join, depth);
                return;
            }
            case WindowJoinPlan windowJoin -> {
                printWindowJoin(windowJoin, depth);
                return;
            }
            case HorizonJoinPlan horizon -> {
                printHorizonJoin(horizon, depth);
                return;
            }
            case LatestByPlan latest -> {
                sink.put("LatestBy\n");
                attribute(depth, "keys");
                columnList(latest.getKeyColumnIds());
                sink.put('\n');
                attribute(depth, "timestamp");
                column(latest.getTimestampColumnId());
                sink.put('\n');
            }
            case SetOperationPlan operation -> {
                setOperationName(operation.getOperation());
                sink.put('\n');
            }
            case SortPlan sort -> {
                sink.put("Sort\n");
                attribute(depth, "keys");
                sink.put('[');
                for (int i = 0, n = sort.getColumnIds().size(); i < n; i++) {
                    if (i > 0) {
                        sink.put(", ");
                    }
                    column(sort.getColumnIds().getQuick(i));
                    if (sort.getDirections().getQuick(i) == QueryModel.ORDER_DIRECTION_DESCENDING) {
                        sink.put(" desc");
                    }
                }
                sink.put("]\n");
            }
            case LimitPlan limit -> {
                sink.put("Limit\n");
                attribute(depth, "lo");
                expression(limit.getLo());
                sink.put('\n');
                if (limit.getHi() != null) {
                    attribute(depth, "hi");
                    expression(limit.getHi());
                    sink.put('\n');
                }
            }
            default -> {
            }
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            print(plan.inputAt(i), depth + 1);
        }
    }

    private void printAggregate(AggregatePlan aggregate, int depth) {
        if (aggregate instanceof SampleByPlan sample) {
            sink.put("SampleBy\n");
            attribute(depth, "period");
            sink.put(sample.getPeriodToken()).put('\n');
        } else {
            sink.put("Aggregate\n");
        }
        final OutputSchema output = aggregate.getOutput();
        final int keyCount = aggregate.getGroupingExpressions().size();
        attribute(depth, "keys");
        sink.put('[');
        for (int i = 0; i < keyCount; i++) {
            if (i > 0) {
                sink.put(", ");
            }
            named(aggregate.getGroupingExpressions().getQuick(i), output, i);
        }
        sink.put("]\n");
        attribute(depth, "values");
        sink.put('[');
        for (int i = 0, n = aggregate.getAggregates().size(); i < n; i++) {
            if (i > 0) {
                sink.put(", ");
            }
            named(aggregate.getAggregates().getQuick(i), output, keyCount + i);
        }
        sink.put("]\n");
    }

    private void printFill(FillPlan fill, int depth) {
        sink.put("Fill\n");
        attribute(depth, "values");
        names(fill.getTokens());
        sink.put('\n');
        if (fill.getFrom() != null) {
            attribute(depth, "from");
            expression(fill.getFrom());
            sink.put('\n');
        }
        if (fill.getTo() != null) {
            attribute(depth, "to");
            expression(fill.getTo());
            sink.put('\n');
        }
    }

    private void printHorizonJoin(HorizonJoinPlan horizon, int depth) {
        sink.put("HorizonJoin\n");
        attribute(depth, "offsets");
        names(horizon.getOffsets());
        sink.put('\n');
        print(horizon.getMaster(), depth + 1);
        for (int i = 0, n = horizon.getSlaves().size(); i < n; i++) {
            final HorizonJoinSlave slave = horizon.getSlaves().getQuick(i);
            attribute(depth, "keys");
            keyPairs(slave.getMasterKeyColumnIds(), slave.getSlaveKeyColumnIds());
            sink.put('\n');
            print(slave.getInput(), depth + 1);
        }
    }

    private void printJoin(JoinPlan join, int depth) {
        sink.put("Join\n");
        final ObjList<JoinInput> inputs = join.getOrderedInputs().size() > 0 ? join.getOrderedInputs() : join.getInputs();
        for (int i = 0, n = inputs.size(); i < n; i++) {
            final JoinInput input = inputs.getQuick(i);
            indent(depth + 1);
            if (i == 0) {
                sink.put("Master");
            } else {
                joinTypeName(input.getJoinType());
            }
            if (input.getBindingAlias() != null) {
                sink.put(' ').put(input.getBindingAlias());
            }
            sink.put('\n');
            if (input.getMasterKeyColumnIds().size() > 0) {
                attribute(depth + 1, "keys");
                keyPairs(input.getMasterKeyColumnIds(), input.getSlaveKeyColumnIds());
                sink.put('\n');
            }
            if (input.getOnResidual() != null) {
                attribute(depth + 1, "on");
                expression(input.getOnResidual());
                sink.put('\n');
            }
            if (input.getPostJoinFilter() != null) {
                attribute(depth + 1, "filter");
                expression(input.getPostJoinFilter());
                sink.put('\n');
            }
            if (input.getUnnest() != null) {
                attribute(depth + 1, "expressions");
                expressions(input.getUnnest().getExpressions());
                sink.put('\n');
            } else if (input.getInput() != null) {
                print(input.getInput(), depth + 2);
            }
        }
    }

    private void printScan(ScanPlan scan, int depth) {
        sink.put("Scan\n");
        attribute(depth, "table");
        sink.put(scan.getTableToken().getTableName()).put('\n');
        if (scan.getViewName() != null) {
            attribute(depth, "view");
            sink.put(scan.getViewName()).put('\n');
        }
        attribute(depth, "columns");
        outputNames(scan.getOutput());
        sink.put('\n');
    }

    private void printTree(LogicalPlan root) {
        columnNames.clear();
        columnQualifiers.clear();
        registerNames(root);
        print(root, 0);
    }

    private void printWindow(WindowPlan window, int depth) {
        sink.put("Window\n");
        attribute(depth, "functions");
        sink.put('[');
        for (int i = 0, n = window.getFunctions().size(); i < n; i++) {
            if (i > 0) {
                sink.put(", ");
            }
            final WindowSpec spec = window.getSpecs().getQuick(i);
            function(window.getFunctions().getQuick(i));
            sink.put(" over (partition by ");
            expressions(spec.getPartitionBy());
            sink.put(" order by [");
            for (int k = 0, m = spec.getOrderByColumnIds().size(); k < m; k++) {
                if (k > 0) {
                    sink.put(", ");
                }
                column(spec.getOrderByColumnIds().getQuick(k));
                if (spec.getOrderByDirections().getQuick(k) == QueryModel.ORDER_DIRECTION_DESCENDING) {
                    sink.put(" desc");
                }
            }
            sink.put("] ");
            frame(spec);
            sink.put(") AS ");
            column(window.getFunctionColumnIds().getQuick(i));
        }
        sink.put("]\n");
    }

    private void printWindowJoin(WindowJoinPlan windowJoin, int depth) {
        sink.put("WindowJoin\n");
        print(windowJoin.getMaster(), depth + 1);
        for (int i = 0, n = windowJoin.getSteps().size(); i < n; i++) {
            final WindowJoinStep step = windowJoin.getSteps().getQuick(i);
            attribute(depth, "window");
            sink.put('[').put(step.getLo()).put(", ").put(step.getHi()).put(']');
            if (step.isIncludePrevailing()) {
                sink.put(" include prevailing");
            }
            sink.put('\n');
            if (step.getFilter() != null) {
                attribute(depth, "on");
                expression(step.getFilter());
                sink.put('\n');
            }
            attribute(depth, "values");
            expressions(step.getAggregates());
            sink.put('\n');
            print(step.getSlave(), depth + 1);
        }
    }

    private void register(OutputSchema output) {
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            final CharSequence qualifier = output.getColumnQualifier(i);
            final CharSequence name = output.getColumnName(i);
            columnNames.put(output.getColumnId(i), name);
            columnQualifiers.put(output.getColumnId(i), qualifier);
        }
    }

    private void registerNames(LogicalPlan plan) {
        if (plan instanceof JoinPlan join) {
            final ObjList<JoinInput> inputs = join.getInputs();
            for (int i = 0, n = inputs.size(); i < n; i++) {
                final JoinInput input = inputs.getQuick(i);
                if (input.getInput() != null) {
                    registerNames(input.getInput());
                }
                if (input.getUnnest() != null) {
                    register(input.getUnnest().getOutput());
                }
            }
            register(plan.getOutput());
            return;
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            registerNames(plan.inputAt(i));
        }
        if (plan instanceof WindowJoinPlan windowJoin) {
            final ObjList<WindowJoinStep> steps = windowJoin.getSteps();
            for (int i = 0, n = steps.size(); i < n; i++) {
                registerNames(steps.getQuick(i).getSlave());
            }
        }
        final OutputSchema output = plan.getOutput();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            if (columnNames.get(output.getColumnId(i)) == null) {
                columnNames.put(output.getColumnId(i), output.getColumnName(i));
            }
        }
    }

    private void setOperationName(int operation) {
        switch (operation) {
            case QueryModel.SET_OPERATION_UNION -> sink.put("Union");
            case QueryModel.SET_OPERATION_UNION_ALL -> sink.put("Union All");
            case QueryModel.SET_OPERATION_EXCEPT -> sink.put("Except");
            case QueryModel.SET_OPERATION_EXCEPT_ALL -> sink.put("Except All");
            case QueryModel.SET_OPERATION_INTERSECT -> sink.put("Intersect");
            case QueryModel.SET_OPERATION_INTERSECT_ALL -> sink.put("Intersect All");
            default -> sink.put("SetOperation(").put(operation).put(')');
        }
    }
}

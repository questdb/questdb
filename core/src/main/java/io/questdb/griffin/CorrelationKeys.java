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

import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

import static io.questdb.griffin.DecorrelationContext.isTrue;
import static io.questdb.griffin.DecorrelationContext.pairIndex;

/**
 * Turns equalities between outer and inner columns into mappings and join keys, and keys nullable join inputs
 * to the columns that map their outer columns.
 */
final class CorrelationKeys implements Mutable {
    final IntList deferredColumnIds = new IntList();
    final ObjList<JoinInput> deferredInputs = new ObjList<>();
    final IntList deferredOuterIds = new IntList();
    final IntList droppedEqualities = new IntList();
    private final DecorrelationContext ctx;

    CorrelationKeys(DecorrelationContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void clear() {
        deferredColumnIds.clear();
        deferredInputs.clear();
        deferredOuterIds.clear();
        droppedEqualities.clear();
    }

    private static int inputOrder(JoinPlan join, int columnId) {
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        for (int i = 0, n = ordered.size(); i < n; i++) {
            if (ordered.getQuick(i).getSourceOutput().getColumnIndexById(columnId) > -1) {
                return i;
            }
        }
        return Integer.MAX_VALUE;
    }

    private static void orderAfterProvider(ObjList<JoinInput> ordered, JoinInput input, int columnId) {
        final int index = ordered.indexOf(input);
        for (int i = index + 1, n = ordered.size(); i < n; i++) {
            if (ordered.getQuick(i).getSourceOutput().getColumnIndexById(columnId) > -1) {
                ordered.remove(index);
                ordered.insert(i, 1, input);
                return;
            }
        }
    }

    private void collectEquality(ColumnExpression column, OuterColumnExpression outer, OutputSchema input, int base) {
        final int outerId = outer.getColumnId();
        if (ctx.masterOuterIds.contains(outerId) && pairIndex(droppedEqualities, outerId) < 0 && ctx.mappedColumn(outerId, base, ctx.mappedOuterIds.size()) < 0
                && input.getColumnIndexById(column.getColumnId()) > -1 && column.getDataType() == outer.getDataType()) {
            droppedEqualities.add(outerId);
            droppedEqualities.add(column.getColumnId());
        }
    }

    /**
     * Moves the equalities of a remapped condition between a column of the input and a column of an input
     * joined before it into the input's keys; returns the rest of the condition.
     */
    private BoundExpression extractKeys(JoinPlan join, JoinInput input, BoundExpression condition) {
        if (condition instanceof FunctionExpression call && call.isAnd()) {
            final BoundExpression left = extractKeys(join, input, call.argumentAt(0));
            final BoundExpression right = extractKeys(join, input, call.argumentAt(1));
            if (left == null || right == null) {
                return left == null ? right : left;
            }
            return ctx.context.getRewriter().replaceConjunction(call, left, right);
        }
        if (!(condition instanceof FunctionExpression call) || !Chars.equals(call.getName(), '=') || call.getArgumentCount() != 2
                || !(call.argumentAt(0) instanceof ColumnExpression left) || !(call.argumentAt(1) instanceof ColumnExpression right)
                || left.isCast() || right.isCast()) {
            return condition;
        }
        final OutputSchema slaveOutput = input.getSourceOutput();
        final boolean isLeftSlave = slaveOutput.getColumnIndexById(left.getColumnId()) > -1;
        final ColumnExpression slave = isLeftSlave ? left : right;
        final ColumnExpression master = isLeftSlave ? right : left;
        final int slaveOrder = join.getOrderedInputs().indexOf(input);
        if (slaveOutput.getColumnIndexById(slave.getColumnId()) < 0 || inputOrder(join, master.getColumnId()) >= slaveOrder) {
            return condition;
        }
        final OutputSchema output = join.getOutput();
        JoinBinder.addJoinKey(input, master.getColumnId(), slave.getColumnId(), ctx.joinedName(output, master.getColumnId()),
                ctx.joinedName(output, slave.getColumnId()), condition.getPosition());
        return null;
    }

    private boolean isDroppedEquality(BoundExpression inner, BoundExpression outer) {
        if (inner instanceof ColumnExpression column && outer instanceof OuterColumnExpression reference) {
            final int index = pairIndex(droppedEqualities, reference.getColumnId());
            return index > -1 && droppedEqualities.getQuick(index + 1) == column.getColumnId();
        }
        return false;
    }

    static boolean hasOuterCondition(JoinInput input) {
        return switch (input.getJoinType()) {
            case CROSS -> input.getPostJoinFilter() != null && LogicalPlans.hasOuterColumn(input.getPostJoinFilter());
            case INNER -> input.getOnResidual() != null && LogicalPlans.hasOuterColumn(input.getOnResidual())
                    || input.getPostJoinFilter() != null && LogicalPlans.hasOuterColumn(input.getPostJoinFilter());
            case LEFT_OUTER -> input.getOnResidual() != null && LogicalPlans.hasOuterColumn(input.getOnResidual());
            default -> false;
        };
    }

    /**
     * A second key of the step on the same master column holds as an equality between the two columns of the
     * input.
     */
    void addKeyFilter(JoinInput step, int columnId, int keyId, OutputSchema output) throws SqlException {
        final int position = step.getPosition();
        final BoundExpression equality = ctx.bindCall("=", position, ctx.column(output, columnId, position), ctx.column(output, keyId, position), output);
        step.setKeyFilter(step.getKeyFilter() == null ? equality : ctx.context.getRewriter().combineConjunction(step.getKeyFilter(), equality, position));
    }

    void collectEqualities(BoundExpression predicate, OutputSchema input, int base) {
        if (!(predicate instanceof FunctionExpression call)) {
            return;
        }
        if (call.isAnd()) {
            collectEqualities(call.argumentAt(0), input, base);
            collectEqualities(call.argumentAt(1), input, base);
            return;
        }
        if (call.getArgumentCount() != 2 || !"=".equals(call.getName())) {
            return;
        }
        final BoundExpression left = call.argumentAt(0);
        final BoundExpression right = call.argumentAt(1);
        if (left instanceof ColumnExpression column && right instanceof OuterColumnExpression outer) {
            collectEquality(column, outer, input, base);
        } else if (right instanceof ColumnExpression column && left instanceof OuterColumnExpression outer) {
            collectEquality(column, outer, input, base);
        }
    }

    /**
     * Records the WHERE equalities between a column of the inputs before the step and an outer column of an
     * enclosing lateral: the step's body reads that column for the outer one.
     */
    void collectOuterAliases(BoundExpression predicate, JoinPlan join, int index) {
        if (!(predicate instanceof FunctionExpression call)) {
            return;
        }
        if (call.isAnd()) {
            collectOuterAliases(call.argumentAt(0), join, index);
            collectOuterAliases(call.argumentAt(1), join, index);
            return;
        }
        if (call.getArgumentCount() != 2 || !"=".equals(call.getName())) {
            return;
        }
        final BoundExpression left = call.argumentAt(0);
        final BoundExpression right = call.argumentAt(1);
        final ColumnExpression column = left instanceof ColumnExpression c ? c : right instanceof ColumnExpression c ? c : null;
        final OuterColumnExpression outer = left instanceof OuterColumnExpression o ? o : right instanceof OuterColumnExpression o ? o : null;
        if (column == null || outer == null || column.getDataType() != outer.getDataType() || ctx.outerAliases.keyIndex(outer.getColumnId()) < 0) {
            return;
        }
        for (int i = 0; i < index; i++) {
            if (join.getInputs().getQuick(i).getSourceOutput().getColumnIndexById(column.getColumnId()) > -1) {
                ctx.outerAliases.put(outer.getColumnId(), column.getColumnId());
                return;
            }
        }
    }

    /**
     * Moves the mapping of a nullable join input, whose correlated columns read NULL for unmatched rows, to
     * the deferred list: the block satisfies those outer columns itself and the input joins on them.
     */
    void deferMapping(JoinInput input, int inputBase) {
        for (int i = inputBase, n = ctx.mappedOuterIds.size(); i < n; i++) {
            deferredInputs.add(input);
            deferredOuterIds.add(ctx.mappedOuterIds.getQuick(i));
            deferredColumnIds.add(ctx.mappedColumnIds.getQuick(i));
        }
        ctx.mappedOuterIds.setPos(inputBase);
        ctx.mappedColumnIds.setPos(inputBase);
    }

    BoundExpression dropEqualities(BoundExpression predicate) {
        if (!(predicate instanceof FunctionExpression call)) {
            return predicate;
        }
        if (call.isAnd()) {
            final BoundExpression left = dropEqualities(call.argumentAt(0));
            final BoundExpression right = dropEqualities(call.argumentAt(1));
            if (left == null) {
                return right;
            }
            if (right == null) {
                return left;
            }
            return left == call.argumentAt(0) && right == call.argumentAt(1) ? call : ctx.context.getRewriter().replaceConjunction(call, left, right);
        }
        if (call.getArgumentCount() == 2 && "=".equals(call.getName())
                && (isDroppedEquality(call.argumentAt(0), call.argumentAt(1)) || isDroppedEquality(call.argumentAt(1), call.argumentAt(0)))) {
            return null;
        }
        return predicate;
    }

    boolean isEveryOuterColumnEquated(int chainOuterBase, int base) {
        for (int i = chainOuterBase, n = ctx.chainOuterIds.size(); i < n; i++) {
            final int outerId = ctx.chainOuterIds.getQuick(i);
            if (ctx.masterOuterIds.contains(outerId) && ctx.mappedColumn(outerId, base, ctx.mappedOuterIds.size()) < 0
                    && pairIndex(droppedEqualities, outerId) < 0) {
                return false;
            }
        }
        return true;
    }

    void joinMappedInput(JoinInput input, int inputBase, int base) {
        for (int i = inputBase; i < ctx.mappedOuterIds.size(); i++) {
            final int outerId = ctx.mappedOuterIds.getQuick(i);
            final int earlier = ctx.mappedColumn(outerId, base, inputBase);
            if (earlier > -1) {
                JoinBinder.addJoinKey(input, earlier, ctx.mappedColumnIds.getQuick(i), ctx.outerRefName(outerId), ctx.outerRefName(outerId), input.getPosition());
                if (input.getJoinType() == JoinKind.CROSS) {
                    input.setJoinType(JoinKind.INNER);
                }
                ctx.mappedOuterIds.removeIndex(i);
                ctx.mappedColumnIds.removeIndex(i);
                i--;
            }
        }
    }

    /**
     * Keys each deferred nullable input to the mapped column of its outer column, joining it after the input
     * that provides the column.
     */
    void keyDeferredInputs(JoinPlan join, int deferredBase, int base) {
        for (int i = deferredBase, n = deferredInputs.size(); i < n; i++) {
            final JoinInput input = deferredInputs.getQuick(i);
            final int outerId = deferredOuterIds.getQuick(i);
            final int columnId = ctx.mappedColumn(outerId, base, ctx.mappedOuterIds.size());
            JoinBinder.addJoinKey(input, columnId, deferredColumnIds.getQuick(i), ctx.outerRefName(outerId), ctx.outerRefName(outerId), input.getPosition());
            orderAfterProvider(join.getOrderedInputs(), input, columnId);
        }
    }

    /**
     * Keys the input by the equalities its remapped conditions hold with the inputs joined before it: those of
     * the ON condition, and of the WHERE conjuncts of an input that does not null-extend.
     */
    void keyOuterConditions(JoinPlan join, JoinInput input) {
        if (input.getJoinType() != JoinKind.LEFT_OUTER) {
            input.setPostJoinFilter(extractKeys(join, input, input.getPostJoinFilter()));
        }
        if (input.getJoinType() != JoinKind.CROSS) {
            input.setOnResidual(extractKeys(join, input, input.getOnResidual()));
        }
        if (input.getJoinType() == JoinKind.CROSS && input.getMasterKeyColumnIds().size() > 0) {
            input.setJoinType(JoinKind.INNER);
        }
    }

    /**
     * Takes the ON condition of a LEFT step, its first {@code keyCount} keys as equalities and its residual,
     * as one predicate over the join output; the step loses both. A scalar body evaluates it over its
     * empty-input values.
     */
    BoundExpression stepCondition(JoinPlan join, JoinInput step, int keyCount) throws SqlException {
        BoundExpression condition = step.getOnResidual();
        if (keyCount == 0 && isTrue(condition)) {
            return null;
        }
        final OutputSchema output = join.getOutput();
        for (int i = 0; i < keyCount; i++) {
            final int masterId = step.getMasterKeyColumnIds().getQuick(i);
            final int slaveId = step.getSlaveKeyColumnIds().getQuick(i);
            final int position = step.getKeyPositions().getQuick(i);
            final BoundExpression equality = ctx.bindCall("=", position,
                    ctx.planNodes.columns.next().of(slaveId, output.getColumnType(output.getColumnIndexById(slaveId)), position),
                    ctx.planNodes.columns.next().of(masterId, output.getColumnType(output.getColumnIndexById(masterId)), position), output);
            condition = condition == null ? equality : ctx.context.getRewriter().combineConjunction(condition, equality, step.getPosition());
        }
        for (int i = 0; i < keyCount; i++) {
            step.getMasterKeyColumnIds().removeIndex(0);
            step.getSlaveKeyColumnIds().removeIndex(0);
            step.getMasterKeyNames().remove(0);
            step.getSlaveKeyNames().remove(0);
            step.getKeyPositions().removeIndex(0);
        }
        step.setOnResidual(null);
        return condition;
    }
}

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
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SortDirection;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Reads the last rows of an ascending timestamp-led sort as the first rows of the reversed sort.
 */
final class NegativeLimitReversalPass {
    private final ObjectPool<ConstantExpression> constants;
    private final IntList limitKeyIds;
    private final ObjList<LogicalPlan> restoredSorts;
    private final ObjectPool<SortPlan> sorts;

    NegativeLimitReversalPass(ObjectPool<ConstantExpression> constants, ObjectPool<SortPlan> sorts, IntList limitKeyIds, ObjList<LogicalPlan> restoredSorts) {
        this.constants = constants;
        this.sorts = sorts;
        this.limitKeyIds = limitKeyIds;
        this.restoredSorts = restoredSorts;
    }

    private static boolean isTableSource(LogicalPlan plan) {
        plan = LogicalPlans.skipProjectsAndFilters(plan);
        return plan instanceof ScanPlan;
    }

    private static int liftColumnId(LogicalPlan plan, LogicalPlan target, int columnId) {
        if (plan == target) {
            return columnId;
        }
        final int id = liftColumnId(plan.inputAt(0), target, columnId);
        final int index = id < 0 ? -1 : LogicalPlans.projectedUncastColumnIndex((ProjectPlan) plan, id);
        return index < 0 ? -1 : plan.getOutput().getColumnId(index);
    }

    private LogicalPlan reverseNegativeLimits0(LogicalPlan plan) {
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            final LogicalPlan input = plan.inputAt(i);
            final LogicalPlan reversed = reverseNegativeLimits0(input);
            if (reversed != input) {
                plan.replaceInput(i, reversed);
            }
        }
        if (plan instanceof SortPlan) {
            LogicalPlan parent = plan;
            while (parent.inputAt(0) instanceof ProjectPlan) {
                parent = parent.inputAt(0);
            }
            if (restoredSorts.indexOf(parent.inputAt(0)) >= 0) {
                parent.replaceInput(0, parent.inputAt(0).inputAt(0));
            }
            return plan;
        }
        if (!(plan instanceof LimitPlan limit)) {
            return plan;
        }
        final LogicalPlan upperTop = limit.getInput();
        LogicalPlan upperBottom = limit;
        while (upperBottom.inputAt(0) instanceof ProjectPlan) {
            upperBottom = upperBottom.inputAt(0);
        }
        if (!(upperBottom.inputAt(0) instanceof SortPlan sort)) {
            return plan;
        }
        final IntList ids = sort.getColumnIds();
        final ObjList<SortDirection> directions = sort.getDirections();
        if (limit.getHi() != null || !(limit.getLo() instanceof ConstantExpression lo)
                || lo.getLongValue() >= 0 || lo.getLongValue() == Numbers.LONG_NULL
                || directions.size() < 2 || directions.getQuick(0) != SortDirection.ASCENDING
                || sort.isMarkoutHorizon() || ids.getQuick(0) != sort.getInput().getOutput().getTimestampColumnId()) {
            return plan;
        }
        final LogicalPlan lowerTop = sort.getInput();
        ProjectPlan lowerBottom = null;
        LogicalPlan source = lowerTop;
        while (source instanceof ProjectPlan project && LogicalPlans.isColumnProjection(project)) {
            lowerBottom = project;
            source = project.getInput();
        }
        if (!isTableSource(source)) {
            return plan;
        }
        final SortPlan restored = sorts.next().of(upperTop, sort.getPosition());
        final IntList sourceIds = limitKeyIds;
        sourceIds.clear();
        for (int i = 0, n = ids.size(); i < n; i++) {
            final int id = liftColumnId(upperTop, sort, ids.getQuick(i));
            int sourceId = ids.getQuick(i);
            for (LogicalPlan p = lowerTop; p != source && sourceId >= 0; p = p.inputAt(0)) {
                final int index = p.getOutput().getColumnIndexById(sourceId);
                sourceId = index >= 0 && ((ProjectPlan) p).getExpressions().getQuick(index) instanceof ColumnExpression column
                        && column.isDirectReference() && !column.isCast() ? column.getColumnId() : -1;
            }
            if (id < 0 || sourceId < 0) {
                return plan;
            }
            restored.getColumnIds().add(id);
            restored.getDirections().add(directions.getQuick(i));
            sourceIds.add(sourceId);
        }
        for (int i = 0, n = directions.size(); i < n; i++) {
            ids.setQuick(i, sourceIds.getQuick(i));
            directions.setQuick(i, directions.getQuick(i) == SortDirection.DESCENDING ? SortDirection.ASCENDING : SortDirection.DESCENDING);
        }
        sort.markReversal();
        sort.replaceInput(0, source);
        sort.deriveOutput();
        final BoundExpression count = lo.getDataType() == ColumnType.INT
                ? constants.next().ofInt((int) -lo.getLongValue(), lo.getPosition())
                : constants.next().ofLong(-lo.getLongValue(), lo.getPosition());
        limit.of(sort, count, null, limit.getPosition());
        limit.deriveOutput();
        final LogicalPlan lower = lowerBottom == null ? limit : lowerTop;
        if (lowerBottom != null) {
            lowerBottom.replaceInput(0, limit);
        }
        if (upperBottom != limit) {
            upperBottom.replaceInput(0, lower);
        }
        final LogicalPlan upper = upperBottom == limit ? lower : upperTop;
        restored.replaceInput(0, upper);
        restored.deriveOutput();
        restoredSorts.add(restored);
        return restored;
    }

    /**
     * Reads the last rows of an ascending timestamp-led multi-key sort as the
     * first rows of the reversed sort below the projection, then restores the order:
     * {@code ORDER BY ts, a LIMIT -n} becomes {@code ORDER BY ts, a} over {@code ORDER BY ts DESC, a DESC LIMIT n}.
     * An enclosing sort replaces the restoring one.
     */
    LogicalPlan reverseNegativeLimits(LogicalPlan root) {
        restoredSorts.clear();
        return reverseNegativeLimits0(root);
    }
}

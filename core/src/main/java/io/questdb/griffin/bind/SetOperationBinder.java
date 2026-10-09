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

package io.questdb.griffin.bind;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.SetOperationCasts;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.std.IntList;

import static io.questdb.cairo.ColumnType.*;

/**
 * Binding-side typing of UNION/EXCEPT/INTERSECT: output column types, branch casts, symbol columns and timestamp.
 */
public final class SetOperationBinder {
    private SetOperationBinder() {
    }

    private static boolean isSymbolColumn(LogicalPlan plan, int index) {
        return ColumnType.isSymbol(plan.getOutput().getColumnType(index))
                || plan instanceof SetOperationPlan operation && !operation.isSymbolRestorationRequired()
                && operation.getSymbolColumns().contains(index);
    }

    private static void validateCasts(OutputSchema branch, OutputSchema left, OutputSchema right, int position) throws SqlException {
        for (int i = 0, n = branch.getColumnCount(); i < n; i++) {
            SetOperationCasts.validateCast(branch.getColumnType(i), SetOperationCasts.getUnionCastType(left.getColumnType(i), right.getColumnType(i)), branch.getColumnName(i), position);
        }
    }

    static boolean isSymbolTableStatic(SetOperationPlan plan, int index) {
        return !plan.getOperation().isUnion() && !SetOperationCasts.isCastRequired(plan)
                && plan.getLeft().getOutput().isSymbolTableStatic(index);
    }

    static int resolveTimestampIndex(SetOperationPlan plan) {
        return plan.getOperation().isUnion() || SetOperationCasts.isCastRequired(plan)
                ? -1 : plan.getLeft().getOutput().getTimestampIndex();
    }

    static void resolveTypes(SetOperationPlan plan, IntList targetTypes) throws SqlException {
        final OutputSchema left = plan.getLeft().getOutput();
        final OutputSchema right = plan.getRight().getOutput();
        if (left.getColumnCount() != right.getColumnCount()) {
            throw SqlException.$(plan.getRightPosition(), "queries have different number of columns");
        }
        final boolean isUnion = plan.getOperation().isUnion();
        final boolean isCastRequired = SetOperationCasts.isCastRequired(plan);
        targetTypes.clear();
        plan.getSymbolColumns().clear();
        for (int i = 0, n = left.getColumnCount(); i < n; i++) {
            int type = isCastRequired
                    ? SetOperationCasts.getUnionCastType(left.getColumnType(i), right.getColumnType(i))
                    : left.getColumnType(i);
            if (isUnion && isSymbolColumn(plan.getLeft(), i) && isSymbolColumn(plan.getRight(), i)) {
                plan.getSymbolColumns().add(i);
                if (plan.isSymbolRestorationRequired()) {
                    type = ColumnType.SYMBOL;
                }
            }
            targetTypes.add(type);
        }
        if (isCastRequired) {
            validateCasts(left, left, right, plan.getPosition());
            validateCasts(right, left, right, plan.getRightPosition());
        }
    }
}

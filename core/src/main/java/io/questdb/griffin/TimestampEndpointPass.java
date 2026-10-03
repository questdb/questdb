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

import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.SortDirection;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.std.ObjectPool;

/**
 * Feeds a {@link LogicalPlans#isTimestampEndpoint(AggregatePlan) timestamp endpoint} only the first row of its
 * input in the endpoint's timestamp order. The aggregate stays in place, so an empty input still yields its
 * single NULL row.
 */
final class TimestampEndpointPass {
    private final ObjectPool<ConstantExpression> constants;
    private final ObjectPool<LimitPlan> limits;
    private final ObjectPool<SortPlan> sorts;

    TimestampEndpointPass(ObjectPool<ConstantExpression> constants, ObjectPool<LimitPlan> limits, ObjectPool<SortPlan> sorts) {
        this.constants = constants;
        this.limits = limits;
        this.sorts = sorts;
    }

    private void limitInput(AggregatePlan aggregate) {
        final FunctionExpression call = aggregate.getAggregates().getQuick(0);
        final int position = call.getPosition();
        LogicalPlan input = aggregate.getInput();
        if (LogicalPlans.isTimestampEndpointBackward(call)) {
            final SortPlan sort = sorts.next().of(input, position);
            sort.getColumnIds().add(input.getOutput().getTimestampColumnId());
            sort.getDirections().add(SortDirection.DESCENDING);
            sort.deriveOutput();
            input = sort;
        }
        final LimitPlan limit = limits.next().of(input, constants.next().ofInt(1, position), null, position);
        limit.deriveOutput();
        aggregate.replaceInput(0, limit);
    }

    void limitEndpointInputs(LogicalPlan plan) {
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            final LogicalPlan input = plan.inputAt(i);
            if (input != null) {
                limitEndpointInputs(input);
            }
        }
        if (plan instanceof AggregatePlan aggregate && LogicalPlans.isTimestampEndpoint(aggregate)) {
            limitInput(aggregate);
        }
    }
}

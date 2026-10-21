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

import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SortDirection;
import io.questdb.griffin.plan.logical.SortKeys;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.std.Mutable;
import io.questdb.std.ObjectFactory;

/**
 * What the consumer of a plan requires of the plan's rows, which {@link OrderPlanning} records on the plan and passes
 * on to its inputs: rows ordered by one column in one direction, an order of several keys an access path may deliver,
 * a LIMIT on the rows read and whether the row order matters at all, together with where the plan sits: under a join
 * slave, under a step that reads it by random access, or under an aggregate without keys.
 */
final class OrderRequirement implements Mutable {
    static final ObjectFactory<OrderRequirement> FACTORY = OrderRequirement::new;
    private final SortKeys order = new SortKeys();
    private SortDirection direction;
    private boolean isJoinSlaveInput;
    private boolean isRandomAccessRequired;
    private boolean isRowOrderRequired = true;
    private boolean isSingleGroupInput;
    private LimitPlan limit;
    private int orderColumnId = -1;
    private LogicalPlan orderedPlan;

    @Override
    public void clear() {
        order.clear();
        direction = null;
        isJoinSlaveInput = false;
        isRandomAccessRequired = false;
        isRowOrderRequired = true;
        isSingleGroupInput = false;
        limit = null;
        orderColumnId = -1;
        orderedPlan = null;
    }

    /**
     * The direction the rows are to be ordered by {@link #getOrderColumnId()} in, or null.
     */
    SortDirection getDirection() {
        return direction;
    }

    /**
     * The LIMIT that bounds the rows read, or null.
     */
    LimitPlan getLimit() {
        return limit;
    }

    /**
     * The keys of the order an access path may deliver; empty when none is requested.
     */
    SortKeys getOrder() {
        return order;
    }

    /**
     * The column the rows are to be ordered by in {@link #getDirection()}, or -1.
     */
    int getOrderColumnId() {
        return orderColumnId;
    }

    /**
     * Requires nothing of the rows beyond what the consumer's place in the plan implies.
     */
    OrderRequirement inputOf(OrderRequirement consumer) {
        isJoinSlaveInput = consumer.isJoinSlaveInput;
        isRandomAccessRequired = consumer.isRandomAccessRequired;
        isSingleGroupInput = consumer.isSingleGroupInput;
        return this;
    }

    /**
     * True when the plan is, or lies under, the input of a join step after the first.
     */
    boolean isJoinSlaveInput() {
        return isJoinSlaveInput;
    }

    /**
     * True when an order is required and the schema holds every key of it.
     */
    boolean isOrderIn(OutputSchema schema) {
        if (order.isEmpty()) {
            return false;
        }
        for (int i = 0, n = order.size(); i < n; i++) {
            if (schema.getColumnIndexById(order.getColumnIds().getQuick(i)) < 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * True when the order is the order a sort gives the rows of the plan, as a projection of them re-expresses it.
     */
    boolean isOrderOf(LogicalPlan plan) {
        return orderedPlan == plan;
    }

    /**
     * True when a join step reads the plan by random access through projections, filters, limits and sorts.
     */
    boolean isRandomAccessRequired() {
        return isRandomAccessRequired;
    }

    boolean isRowOrderRequired() {
        return isRowOrderRequired;
    }

    /**
     * True when an aggregate without keys reads the plan through projections and filters.
     */
    boolean isSingleGroupInput() {
        return isSingleGroupInput;
    }

    /**
     * Marks the plan as the input of a join step after the first, which reads it by random access when
     * {@code isRandomAccessRequired}.
     */
    OrderRequirement joinSlave(boolean isRandomAccessRequired) {
        this.isJoinSlaveInput = true;
        this.isRandomAccessRequired = isRandomAccessRequired;
        return this;
    }

    OrderRequirement limit(LimitPlan limit) {
        this.limit = limit;
        return this;
    }

    /**
     * Requires the order of the consumer's requirement, unchanged.
     */
    OrderRequirement order(OrderRequirement consumer) {
        order.copyFrom(consumer.order);
        orderedPlan = consumer.orderedPlan;
        return this;
    }

    /**
     * Requires the order of the consumer's requirement when the schema holds every key of it.
     */
    OrderRequirement order(OrderRequirement consumer, OutputSchema schema) {
        return consumer.isOrderIn(schema) ? order(consumer) : this;
    }

    /**
     * Requires the order of the consumer's requirement over the projection's output as an order over the projection's
     * input, unless the projection computes a key of it.
     */
    OrderRequirement order(ProjectPlan project, OrderRequirement consumer) {
        final SortKeys keys = consumer.order;
        if (keys.isEmpty()) {
            return this;
        }
        for (int i = 0, n = keys.size(); i < n; i++) {
            final ColumnExpression column = LogicalPlans.passedColumn(project, keys.getColumnIds().getQuick(i));
            if (column == null) {
                order.clear();
                return this;
            }
            order.getColumnIds().add(column.getColumnId());
            order.getDirections().add(keys.getDirections().getQuick(i));
        }
        orderedPlan = project.getChild();
        return this;
    }

    /**
     * Requires the order of the sort, which orders the rows of its input.
     */
    OrderRequirement order(SortPlan sort) {
        order.of(sort);
        orderedPlan = sort.getChild();
        return this;
    }

    OrderRequirement orderBy(int columnId, SortDirection direction) {
        this.orderColumnId = columnId;
        this.direction = direction;
        return this;
    }

    /**
     * Narrows where the plan sits to what reaches the plan and its inputs: the random access of a join step passes
     * through projections, filters, limits and sorts, and the single group of an aggregate without keys through
     * projections and filters to a scan.
     */
    void passTo(LogicalPlan plan) {
        isRandomAccessRequired &= plan instanceof ProjectPlan || plan instanceof FilterPlan || plan instanceof LimitPlan || plan instanceof SortPlan;
        isSingleGroupInput &= plan instanceof ProjectPlan || plan instanceof FilterPlan || plan instanceof ScanPlan;
    }

    OrderRequirement rowOrderRequired(boolean isRowOrderRequired) {
        this.isRowOrderRequired = isRowOrderRequired;
        return this;
    }

    OrderRequirement singleGroupInput(boolean isSingleGroupInput) {
        this.isSingleGroupInput = isSingleGroupInput;
        return this;
    }
}

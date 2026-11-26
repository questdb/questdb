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

import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.LogicalPlans;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PlanToFactoryRules;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortDirection;
import io.questdb.griffin.plan.logical.SortKeys;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.std.Mutable;
import io.questdb.std.ObjectFactory;

/**
 * What the consumer of a plan requires of the plan's rows, which {@link OrderPlanning} records on the plan and passes
 * on to its inputs: rows ordered by one column in one direction, an order of several keys an access path may deliver,
 * a LIMIT on the rows read and whether the row order matters at all, together with where the plan sits: under a join
 * slave, under a step that reads it by random access, under a nested loop that reads it again and again, or under an
 * aggregate without keys.
 */
final class OrderRequirement implements Mutable {
    static final ObjectFactory<OrderRequirement> FACTORY = OrderRequirement::new;
    private final SortKeys order = new SortKeys();
    private SortDirection direction;
    private LogicalPlan frameReader;
    private boolean isJoinSlaveInput;
    private boolean isRandomAccessRequired;
    private boolean isRandomAccessSorted;
    private boolean isReread;
    private boolean isRewound;
    private boolean isRowOrderRequired = true;
    private boolean isSingleGroupInput;
    private AggregatePlan.OutputCopy streamCopy = AggregatePlan.OutputCopy.NONE;
    private LimitPlan limit;
    private int orderColumnId = -1;
    private LogicalPlan orderedPlan;
    private LimitPlan topK;
    private int topKColumnId = -1;

    @Override
    public void clear() {
        order.clear();
        direction = null;
        frameReader = null;
        isJoinSlaveInput = false;
        isRandomAccessRequired = false;
        isRandomAccessSorted = false;
        isReread = false;
        isRewound = false;
        isRowOrderRequired = true;
        isSingleGroupInput = false;
        streamCopy = AggregatePlan.OutputCopy.NONE;
        limit = null;
        orderColumnId = -1;
        orderedPlan = null;
        topK = null;
        topKColumnId = -1;
    }

    // A selection or a computing projection reads its input's records by the row ids it hands out, unlike the window
    // and window join factories, which build their own cursors.
    private static boolean isRandomAccessPassedThrough(ProjectPlan project) {
        final PlanToFactoryRules.ProjectionFactory factory = PlanToFactoryRules.projectionFactory(project);
        return factory == PlanToFactoryRules.ProjectionFactory.SELECTION || factory == PlanToFactoryRules.ProjectionFactory.VIRTUAL;
    }

    /**
     * The direction the rows are to be ordered by {@link #getOrderColumnId()} in, or null.
     */
    SortDirection getDirection() {
        return direction;
    }

    /**
     * The nearest plan above that may read the page frames of the rows the plan passes on, while a consumer reads
     * them by row id, see {@link #isRandomAccessRequired}: a filter, which supplies random access when it runs in
     * parallel, or the HORIZON JOIN, or the aggregate over it, whose master they are; null when none does or a LIMIT
     * or a computing projection between cuts the page frames off.
     */
    LogicalPlan getFrameReader() {
        return frameReader;
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
     * The copy a stream GROUP BY that yields the rows gives a consumer that copies rows without row ids itself, into
     * memory a budget bounds or into a form that changes their values, see {@link AggregatePlan.OutputCopy}.
     * Projections, filters, LIMITs without a sort, the left input of EXCEPT and INTERSECT and a sort by one key pass it
     * on; the sort runs, and copies the rows itself, unless the GROUP BY delivers its order.
     */
    AggregatePlan.OutputCopy getStreamCopy() {
        return streamCopy;
    }

    /**
     * The LIMIT of a sort that orders the rows by {@link #getTopKColumnId()} alone and keeps its first rows, as the
     * projections between them pass the column on; null for none.
     */
    LimitPlan getTopK() {
        return topK;
    }

    /**
     * The column the sort of {@link #getTopK()} orders the rows by, -1 for none.
     */
    int getTopKColumnId() {
        return topKColumnId;
    }

    /**
     * Requires nothing of the rows beyond what the consumer's place in the plan implies.
     */
    OrderRequirement inputOf(OrderRequirement consumer) {
        frameReader = consumer.frameReader;
        isJoinSlaveInput = consumer.isJoinSlaveInput;
        isRandomAccessRequired = consumer.isRandomAccessRequired;
        isRandomAccessSorted = consumer.isRandomAccessSorted;
        isReread = consumer.isReread;
        isRewound = consumer.isRewound;
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
     * True when a consumer reads the plan by row id through projections, filters, limits, sorts and the first branch of
     * an EXCEPT or INTERSECT.
     */
    boolean isRandomAccessRequired() {
        return isRandomAccessRequired;
    }

    /**
     * True when the consumer that reads the plan by row id reads it through a sort, which copies the rows itself unless
     * the plan delivers the sort's order, see {@link #getStreamCopy()}.
     */
    boolean isRandomAccessSorted() {
        return isRandomAccessSorted;
    }

    /**
     * Whether a consumer reads the rows a second time and expects them in the same order, as a LATEST BY that finds
     * the latest row of each key and then reads the rows at their positions does.
     */
    boolean isReread() {
        return isReread;
    }

    /**
     * True when a nested loop reads the rows of the plan again for each row of its other input.
     */
    boolean isRewound() {
        return isRewound;
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
        this.isRandomAccessSorted = false;
        this.frameReader = null;
        return this;
    }

    /**
     * Marks the plan as read by row id when {@code isRandomAccessRequired}, through projections, filters, limits, sorts
     * and the first branch of an EXCEPT or INTERSECT, by a consumer that reads the page frames of the rows in parallel
     * instead when {@code frameReader} does, see {@link #getFrameReader}.
     */
    OrderRequirement randomAccessRequired(boolean isRandomAccessRequired, LogicalPlan frameReader) {
        this.isRandomAccessRequired = isRandomAccessRequired;
        this.isRandomAccessSorted = false;
        this.frameReader = isRandomAccessRequired ? frameReader : null;
        return this;
    }

    OrderRequirement streamCopy(AggregatePlan.OutputCopy streamCopy) {
        this.streamCopy = streamCopy;
        return this;
    }

    OrderRequirement topK(int columnId, LimitPlan limit) {
        this.topKColumnId = limit == null ? -1 : columnId;
        this.topK = columnId < 0 ? null : limit;
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
     * through projections that compute over their input's records, filters, limits, sorts and an EXCEPT or INTERSECT,
     * which reads its first branch by row id; a window or a window join builds its own cursor,
     * the frame reader through selections and sorts, a filter becoming the frame reader of its input, and the single
     * group of an aggregate without keys through projections and filters to a scan.
     */
    void passTo(LogicalPlan plan) {
        isRandomAccessRequired &= plan instanceof ProjectPlan project && isRandomAccessPassedThrough(project)
                || plan instanceof FilterPlan || plan instanceof LimitPlan || plan instanceof SortPlan
                || plan instanceof SetOperationPlan operation && !operation.getOperation().isUnion();
        frameReader = !isRandomAccessRequired ? null : switch (plan) {
            case FilterPlan filter when !PlanToFactoryRules.isFusedFilter(filter) -> filter;
            case ProjectPlan project when PlanToFactoryRules.projectionFactory(project) == PlanToFactoryRules.ProjectionFactory.SELECTION ->
                    frameReader;
            case SortPlan _ -> frameReader;
            default -> null;
        };
        isRandomAccessSorted &= isRandomAccessRequired;
        isSingleGroupInput &= plan instanceof ProjectPlan || plan instanceof FilterPlan || plan instanceof ScanPlan;
    }

    /**
     * Marks the plan as read by row id through a sort, see {@link #isRandomAccessSorted()}.
     */
    OrderRequirement randomAccessSorted() {
        this.isRandomAccessSorted = isRandomAccessRequired;
        return this;
    }

    /**
     * Marks the rows as read a second time in the same order when {@code isReread}, see {@link #isReread()}.
     */
    OrderRequirement reread(boolean isReread) {
        this.isReread |= isReread;
        return this;
    }

    /**
     * Marks the plan as read once by a consumer that keeps what it reads, as EXCEPT and INTERSECT hash their right
     * input: a consumer that rewinds them reads the plan no more.
     */
    OrderRequirement readOnce() {
        this.isRewound = false;
        this.isReread = false;
        return this;
    }

    /**
     * Marks the plan as read again for each row of the other input of a nested loop when {@code isRewound}; a plan under
     * a nested loop stays marked.
     */
    OrderRequirement rewound(boolean isRewound) {
        this.isRewound |= isRewound;
        return this;
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

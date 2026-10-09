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

import io.questdb.std.ObjList;

/**
 * Group keys followed by aggregate outputs, independent of executables; the shape of GROUP BY and SAMPLE BY.
 */
public abstract sealed class GroupingPlan extends UnaryPlan permits AggregatePlan, SampleByPlan {
    private final ObjList<FunctionExpression> aggregates = new ObjList<>();
    private final ObjList<BoundExpression> groupingExpressions = new ObjList<>();
    private boolean hasConstantLeadingGroupBy;
    private boolean hasDirectTableInput;
    private boolean hasExplicitGrouping;
    private boolean hasKeySpellingKept;
    private boolean hasSampleByBucket;

    @Override
    public void clear() {
        super.clear();
        aggregates.clear();
        groupingExpressions.clear();
        hasConstantLeadingGroupBy = false;
        hasDirectTableInput = false;
        hasExplicitGrouping = false;
        hasKeySpellingKept = false;
        hasSampleByBucket = false;
    }

    public ObjList<FunctionExpression> getAggregates() {
        return aggregates;
    }

    public ObjList<BoundExpression> getGroupingExpressions() {
        return groupingExpressions;
    }

    /**
     * True when the first GROUP BY expression is a constant, which forms no grouping key.
     */
    public boolean hasConstantLeadingGroupBy() {
        return hasConstantLeadingGroupBy;
    }

    /**
     * True when every FROM item of the aggregate's query level is a table or a table function, none a sub-query.
     */
    public boolean hasDirectTableInput() {
        return hasDirectTableInput;
    }

    public boolean hasExplicitGrouping() {
        return hasExplicitGrouping;
    }

    /**
     * True when an otherwise identity projection above keeps a grouping key's GROUP BY spelling
     * where the SELECT list names it in a different case.
     */
    public boolean hasKeySpellingKept() {
        return hasKeySpellingKept;
    }

    /**
     * True when a SAMPLE BY lowered to this grouping; its bucket key keeps the rewrite's position.
     */
    public boolean hasSampleByBucket() {
        return hasSampleByBucket;
    }

    public void setConstantLeadingGroupBy(boolean hasConstantLeadingGroupBy) {
        this.hasConstantLeadingGroupBy = hasConstantLeadingGroupBy;
    }

    public void setDirectTableInput(boolean hasDirectTableInput) {
        this.hasDirectTableInput = hasDirectTableInput;
    }

    public void setExplicitGrouping(boolean hasExplicitGrouping) {
        this.hasExplicitGrouping = hasExplicitGrouping;
    }

    public void setKeySpellingKept(boolean hasKeySpellingKept) {
        this.hasKeySpellingKept = hasKeySpellingKept;
    }

    public void setSampleByBucket(boolean hasSampleByBucket) {
        this.hasSampleByBucket = hasSampleByBucket;
    }

    @Override
    public void visitReads(PlanExpressionVisitor visitor) {
        PlanReads.expressions(groupingExpressions, visitor);
        PlanReads.functions(aggregates, visitor);
    }
}

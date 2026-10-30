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
 * <p>
 * A grouping a SAMPLE BY lowered to, in either shape, records the SAMPLE BY grid: its period, the time zone and
 * offset that align its buckets, the input column the buckets floor and the grouping key that outputs the bucket.
 */
public abstract sealed class GroupingPlan extends UnaryPlan permits AggregatePlan, SampleByPlan {
    private final ObjList<FunctionExpression> aggregates = new ObjList<>();
    private final ObjList<BoundExpression> groupingExpressions = new ObjList<>();
    private int bucketKeyIndex = -1;
    private boolean hasConstantLeadingGroupBy;
    private boolean hasDirectTableInput;
    private boolean hasExplicitGrouping;
    private boolean hasKeySpellingKept;
    private BoundExpression offset;
    private int periodPosition;
    private CharSequence periodToken;
    private char periodUnit;
    private int periodUnitPosition;
    private long stride;
    private int timestampColumnId = -1;
    private BoundExpression timezone;

    @Override
    public void clear() {
        super.clear();
        aggregates.clear();
        groupingExpressions.clear();
        bucketKeyIndex = -1;
        hasConstantLeadingGroupBy = false;
        hasDirectTableInput = false;
        hasExplicitGrouping = false;
        hasKeySpellingKept = false;
        offset = null;
        periodPosition = 0;
        periodToken = null;
        periodUnit = 0;
        periodUnitPosition = 0;
        stride = 0;
        timestampColumnId = -1;
        timezone = null;
    }

    public ObjList<FunctionExpression> getAggregates() {
        return aggregates;
    }

    /**
     * The index of the grouping key that outputs the SAMPLE BY bucket, the floor of
     * {@link #getTimestampColumnId() the timestamp} on the grid; -1 when no key does, which every grouping that no
     * SAMPLE BY lowered to reports.
     */
    public int getBucketKeyIndex() {
        return bucketKeyIndex;
    }

    public ObjList<BoundExpression> getGroupingExpressions() {
        return groupingExpressions;
    }

    /**
     * The WITH OFFSET of the SAMPLE BY, as the binder bound and validated it; null without one. The bucket key of an
     * {@link AggregatePlan} reads this expression as its offset argument.
     */
    public BoundExpression getOffset() {
        return offset;
    }

    public int getOffsetPosition() {
        return offset == null ? 0 : offset.getPosition();
    }

    public int getPeriodPosition() {
        return periodPosition;
    }

    /**
     * The SAMPLE BY period as written, such as {@code 15m}; null when a constant expression states the period's
     * {@link #getStride() stride}, and for a grouping that no SAMPLE BY lowered to.
     */
    public CharSequence getPeriodToken() {
        return periodToken;
    }

    public char getPeriodUnit() {
        return periodUnit;
    }

    public int getPeriodUnitPosition() {
        return periodUnitPosition;
    }

    /**
     * The number of {@link #getPeriodUnit() units} in a SAMPLE BY period, which the binder validated with the grammar
     * of the shape, whether the {@link #getPeriodToken() period token} or a constant expression states it.
     */
    public long getStride() {
        return stride;
    }

    /**
     * The input column the SAMPLE BY buckets floor; -1 for a grouping that no SAMPLE BY lowered to.
     */
    public int getTimestampColumnId() {
        return timestampColumnId;
    }

    /**
     * The TIME ZONE of the SAMPLE BY, as the binder bound and validated it; null without one. The bucket key of an
     * {@link AggregatePlan} floors in UTC instead where the binder converts the range to UTC: for a UTC zone, and for
     * a sub-day stride with FROM.
     */
    public BoundExpression getTimezone() {
        return timezone;
    }

    public int getTimezonePosition() {
        return timezone == null ? 0 : timezone.getPosition();
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

    public void setBucketKeyIndex(int bucketKeyIndex) {
        this.bucketKeyIndex = bucketKeyIndex;
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

    public void setOffset(BoundExpression offset) {
        this.offset = offset;
    }

    public void setPeriod(CharSequence token, long stride, int position, char unit, int unitPosition) {
        this.periodToken = token;
        this.stride = stride;
        this.periodPosition = position;
        this.periodUnit = unit;
        this.periodUnitPosition = unitPosition;
    }

    public void setTimestampColumnId(int timestampColumnId) {
        this.timestampColumnId = timestampColumnId;
    }

    public void setTimezone(BoundExpression timezone) {
        this.timezone = timezone;
    }

    @Override
    public void visitReads(PlanExpressionVisitor visitor) {
        PlanReads.expressions(groupingExpressions, visitor);
        PlanReads.functions(aggregates, visitor);
        timestampColumnId = PlanReads.columnId(timestampColumnId, -1, visitor);
    }
}

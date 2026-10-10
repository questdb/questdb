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

import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;

public final class FillPlan extends ForwardingPlan {
    public static final ObjectFactory<FillPlan> FACTORY = FillPlan::new;
    public static final int FILL_NULL = 0;
    public static final int FILL_PREV = 1;
    public static final int FILL_PREV_COLUMN = 3;
    public static final int FILL_VALUE = 2;
    private final IntList modes = new IntList();
    private final IntList sourceColumnIds = new IntList();
    private final IntList targetColumnIds = new IntList();
    private final ObjList<CharSequence> tokens = new ObjList<>();
    private final ObjList<BoundExpression> values = new ObjList<>();
    private Algorithm algorithm;
    private BoundExpression from;
    private BoundExpression offset;
    private int periodPosition;
    private CharSequence periodToken;
    private int timestampColumnId = -1;
    private BoundExpression timezone;
    private BoundExpression to;

    @Override
    public void clear() {
        super.clear();
        modes.clear();
        sourceColumnIds.clear();
        targetColumnIds.clear();
        tokens.clear();
        values.clear();
        algorithm = null;
        from = null;
        offset = null;
        periodPosition = 0;
        periodToken = null;
        timestampColumnId = -1;
        timezone = null;
        to = null;
    }

    /**
     * The bucket timestamp: the input, a hash aggregate, designates none.
     */
    @Override
    public int derivedTimestampIndex() {
        return getInput().getOutput().getColumnIndexById(timestampColumnId);
    }

    /**
     * The order the fill reads its input buckets in, which operator planning records; null before planning.
     */
    public Algorithm getAlgorithm() {
        return algorithm;
    }

    public BoundExpression getFrom() {
        return from;
    }

    public IntList getModes() {
        return modes;
    }

    public BoundExpression getOffset() {
        return offset;
    }

    public int getOffsetPosition() {
        return offset == null ? 0 : offset.getPosition();
    }

    public int getPeriodPosition() {
        return periodPosition;
    }

    public CharSequence getPeriodToken() {
        return periodToken;
    }

    public IntList getSourceColumnIds() {
        return sourceColumnIds;
    }

    public IntList getTargetColumnIds() {
        return targetColumnIds;
    }

    public int getTimestampColumnId() {
        return timestampColumnId;
    }

    public BoundExpression getTimezone() {
        return timezone;
    }

    public int getTimezonePosition() {
        return timezone == null ? 0 : timezone.getPosition();
    }

    public BoundExpression getTo() {
        return to;
    }

    public ObjList<CharSequence> getTokens() {
        return tokens;
    }

    public int getToPosition() {
        return to == null ? 0 : to.getPosition();
    }

    public ObjList<BoundExpression> getValues() {
        return values;
    }

    public FillPlan of(LogicalPlan input, int position) {
        configure(input, position);
        return this;
    }

    public void setAlgorithm(Algorithm algorithm) {
        this.algorithm = algorithm;
    }

    public void setFrom(BoundExpression from) {
        this.from = from;
    }

    public void setOffset(BoundExpression offset) {
        this.offset = offset;
    }

    public void setPeriod(CharSequence token, int position) {
        periodToken = token;
        periodPosition = position;
    }

    public void setTimestampColumnId(int timestampColumnId) {
        this.timestampColumnId = timestampColumnId;
    }

    public void setTimezone(BoundExpression timezone) {
        this.timezone = timezone;
    }

    public void setTo(BoundExpression to) {
        this.to = to;
    }

    @Override
    public void visitReads(PlanExpressionVisitor visitor) {
        PlanReads.expressions(values, visitor);
        from = PlanReads.expression(from, visitor);
        to = PlanReads.expression(to, visitor);
        offset = PlanReads.expression(offset, visitor);
        timezone = PlanReads.expression(timezone, visitor);
        PlanReads.columnIds(sourceColumnIds, null, visitor);
        PlanReads.columnIds(targetColumnIds, null, visitor);
        timestampColumnId = PlanReads.columnId(timestampColumnId, -1, visitor);
    }

    /**
     * How the fill reads the buckets of its input: from the rows of a SAMPLE BY cursor without a fill, which keeps its
     * latest rows readable; in the timestamp order the input designates; or after sorting them by timestamp.
     */
    public enum Algorithm {
        INPUT_ORDER, SAMPLE_BY_ROWS, SORTED
    }
}

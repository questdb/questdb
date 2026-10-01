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

public final class SampleByPlan extends AggregatePlan {
    public static final ObjectFactory<SampleByPlan> FACTORY = SampleByPlan::new;
    public static final int FILL_NONE = 0;
    public static final int FILL_PREV = 1;
    public static final int FILL_NULL = 2;
    public static final int FILL_LINEAR = 3;
    public static final int FILL_VALUE = 4;
    private final ObjList<CharSequence> aggregateSql = new ObjList<>();
    private final IntList fillPositions = new IntList();
    private final ObjList<CharSequence> fillTokens = new ObjList<>();
    private int fillMode;
    private BoundExpression from;
    private boolean isJoinInput;
    private boolean isTimestampRequired = true;
    private BoundExpression offset;
    private BoundExpression period;
    private int periodPosition;
    private CharSequence periodToken;
    private char periodUnit;
    private int periodUnitPosition;
    private int timestampColumnId = -1;
    private BoundExpression timezone;
    private BoundExpression to;

    @Override
    public void clear() {
        super.clear();
        aggregateSql.clear();
        fillPositions.clear();
        fillTokens.clear();
        fillMode = FILL_NONE;
        from = null;
        isJoinInput = false;
        isTimestampRequired = true;
        offset = null;
        period = null;
        periodPosition = 0;
        periodToken = null;
        periodUnit = 0;
        periodUnitPosition = 0;
        timestampColumnId = -1;
        timezone = null;
        to = null;
    }

    public ObjList<CharSequence> getAggregateSql() {
        return aggregateSql;
    }

    public int getFillMode() {
        return fillMode;
    }

    public IntList getFillPositions() {
        return fillPositions;
    }

    public ObjList<CharSequence> getFillTokens() {
        return fillTokens;
    }

    public BoundExpression getFrom() {
        return from;
    }

    public int getFromPosition() {
        return from == null ? 0 : from.getPosition();
    }

    public BoundExpression getOffset() {
        return offset;
    }

    public int getOffsetPosition() {
        return offset == null ? 0 : offset.getPosition();
    }

    public BoundExpression getPeriod() {
        return period;
    }

    public int getPeriodPosition() {
        return periodPosition;
    }

    public CharSequence getPeriodToken() {
        return periodToken;
    }

    public char getPeriodUnit() {
        return periodUnit;
    }

    public int getPeriodUnitPosition() {
        return periodUnitPosition;
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

    public int getToPosition() {
        return to == null ? 0 : to.getPosition();
    }

    @Override
    public Type getType() {
        return Type.SAMPLE_BY;
    }

    public boolean isJoinInput() {
        return isJoinInput;
    }

    public boolean isTimestampRequired() {
        return isTimestampRequired;
    }

    @Override
    public SampleByPlan of(LogicalPlan input, int position) {
        super.of(input, position);
        return this;
    }

    public void setFillMode(int fillMode) {
        this.fillMode = fillMode;
    }

    public void setFrom(BoundExpression from) {
        this.from = from;
    }

    public void setJoinInput(boolean isJoinInput) {
        this.isJoinInput = isJoinInput;
    }

    public void setOffset(BoundExpression offset) {
        this.offset = offset;
    }

    public void setPeriod(CharSequence token, BoundExpression period, int position, char unit, int unitPosition) {
        this.periodToken = token;
        this.period = period;
        this.periodPosition = position;
        this.periodUnit = unit;
        this.periodUnitPosition = unitPosition;
    }

    public void setTimestampColumnId(int timestampColumnId) {
        this.timestampColumnId = timestampColumnId;
    }

    public void setTimestampRequired(boolean isTimestampRequired) {
        this.isTimestampRequired = isTimestampRequired;
    }

    public void setTimezone(BoundExpression timezone) {
        this.timezone = timezone;
    }

    public void setTo(BoundExpression to) {
        this.to = to;
    }
}

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

public final class FillPlan extends UnaryPlan {
    public static final ObjectFactory<FillPlan> FACTORY = FillPlan::new;
    public static final int FILL_NULL = 0;
    public static final int FILL_PREV = 1;
    public static final int FILL_VALUE = 2;
    public static final int FILL_PREV_COLUMN = 3;
    private final IntList modes = new IntList();
    private final IntList positions = new IntList();
    private final IntList sourceColumnIds = new IntList();
    private final IntList sourcePositions = new IntList();
    private final IntList targetColumnIds = new IntList();
    private final ObjList<CharSequence> tokens = new ObjList<>();
    private final ObjList<BoundExpression> values = new ObjList<>();
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
        positions.clear();
        sourceColumnIds.clear();
        sourcePositions.clear();
        targetColumnIds.clear();
        tokens.clear();
        values.clear();
        from = null;
        offset = null;
        periodPosition = 0;
        periodToken = null;
        timestampColumnId = -1;
        timezone = null;
        to = null;
    }

    public BoundExpression getFrom() {
        return from;
    }

    public int getFromPosition() {
        return from == null ? 0 : from.getPosition();
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

    public IntList getPositions() {
        return positions;
    }

    public IntList getSourceColumnIds() {
        return sourceColumnIds;
    }

    public IntList getSourcePositions() {
        return sourcePositions;
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

    public int getToPosition() {
        return to == null ? 0 : to.getPosition();
    }

    public ObjList<CharSequence> getTokens() {
        return tokens;
    }

    public ObjList<BoundExpression> getValues() {
        return values;
    }

    @Override
    public Type getType() {
        return Type.FILL;
    }

    public FillPlan of(LogicalPlan input, int position) {
        configure(input, position);
        getOutput().copyFrom(input.getOutput());
        return this;
    }

    public void setFrom(BoundExpression from) {
        this.from = from;
    }

    public void setOffset(BoundExpression offset) {
        this.offset = offset;
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

    public void setPeriod(CharSequence token, int position) {
        periodToken = token;
        periodPosition = position;
    }
}

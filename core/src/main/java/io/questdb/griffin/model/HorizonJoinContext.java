/*******************************************************************************
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

package io.questdb.griffin.model;

import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

/**
 * Context class for HORIZON JOIN clause configuration.
 * <p>
 * Supports two modes for specifying offsets:
 * <ul>
 *   <li>RANGE: RANGE FROM &lt;from&gt; TO &lt;to&gt; STEP &lt;step&gt;</li>
 *   <li>LIST: LIST (&lt;offset1&gt;, &lt;offset2&gt;, ...)</li>
 * </ul>
 * <p>
 * The horizon pseudo-table has two columns:
 * <ul>
 *   <li>offset - the offset value in micros/nanos</li>
 *   <li>timestamp - the computed horizon timestamp (master.ts + offset)</li>
 * </ul>
 */
public class HorizonJoinContext implements Mutable {
    public static final int MODE_LIST = 2;
    public static final int MODE_NONE = 0;
    public static final int MODE_RANGE = 1;
    private final ObjList<ExpressionNode> listOffsets = new ObjList<>();
    private ExpressionNode alias;
    private int aliasPosition;
    private int mode = MODE_NONE;
    private QueryModel parentModel;
    private ExpressionNode rangeFrom;
    private int rangeFromPosition;
    private ExpressionNode rangeStep;
    private int rangeStepPosition;
    private ExpressionNode rangeTo;

    public void addListOffset(ExpressionNode offset) {
        listOffsets.add(offset);
    }

    @Override
    public void clear() {
        mode = MODE_NONE;
        rangeFrom = null;
        rangeFromPosition = 0;
        rangeTo = null;
        rangeStep = null;
        rangeStepPosition = 0;
        listOffsets.clear();
        alias = null;
        aliasPosition = 0;
        parentModel = null;
    }

    public void copyFrom(HorizonJoinContext other) {
        this.mode = other.mode;
        this.rangeFrom = other.rangeFrom;
        this.rangeFromPosition = other.rangeFromPosition;
        this.rangeTo = other.rangeTo;
        this.rangeStep = other.rangeStep;
        this.rangeStepPosition = other.rangeStepPosition;
        this.listOffsets.clear();
        this.listOffsets.addAll(other.listOffsets);
        this.alias = other.alias;
        this.aliasPosition = other.aliasPosition;
    }

    public ExpressionNode getAlias() {
        return alias;
    }

    public int getAliasPosition() {
        return aliasPosition;
    }

    public ObjList<ExpressionNode> getListOffsets() {
        return listOffsets;
    }

    public int getMode() {
        return mode;
    }

    public QueryModel getParentModel() {
        return parentModel;
    }

    public ExpressionNode getRangeFrom() {
        return rangeFrom;
    }

    public int getRangeFromPosition() {
        return rangeFromPosition;
    }

    public ExpressionNode getRangeStep() {
        return rangeStep;
    }

    public int getRangeStepPosition() {
        return rangeStepPosition;
    }

    public ExpressionNode getRangeTo() {
        return rangeTo;
    }

    public void setAlias(ExpressionNode alias, int position) {
        this.alias = alias;
        this.aliasPosition = position;
    }

    public void setMode(int mode) {
        this.mode = mode;
    }

    public void setParentModel(QueryModel parentModel) {
        this.parentModel = parentModel;
    }

    public void setRangeFrom(ExpressionNode rangeFrom, int position) {
        this.rangeFrom = rangeFrom;
        this.rangeFromPosition = position;
    }

    public void setRangeStep(ExpressionNode rangeStep, int position) {
        this.rangeStep = rangeStep;
        this.rangeStepPosition = position;
    }

    public void setRangeTo(ExpressionNode rangeTo) {
        this.rangeTo = rangeTo;
    }
}

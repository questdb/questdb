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

import io.questdb.griffin.engine.window.LiveViewWindowDescription;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;

/**
 * Bound keys and normalized frame parameters; no parser nodes or executable state.
 */
public final class WindowSpec implements Mutable {
    public static final ObjectFactory<WindowSpec> FACTORY = WindowSpec::new;
    private final IntList orderByColumnIds = new IntList();
    private final IntList orderByDirections = new IntList();
    private final ObjList<CharSequence> orderByNames = new ObjList<>();
    private final IntList orderByPositions = new IntList();
    private final ObjList<BoundExpression> partitionBy = new ObjList<>();
    private int exclusionKind;
    private int exclusionKindPos;
    private int framingMode;
    private boolean isIgnoreNulls;
    private boolean isSubsampleKeepFlag;
    private LiveViewWindowDescription liveViewDescription;
    private int nullsDescPos;
    private long rowsHi;
    private int rowsHiExprPos;
    private char rowsHiExprTimeUnit;
    private int rowsHiKindPos;
    private long rowsLo;
    private int rowsLoExprPos;
    private char rowsLoExprTimeUnit;
    private int rowsLoKindPos;

    @Override
    public void clear() {
        orderByColumnIds.clear();
        orderByDirections.clear();
        orderByNames.clear();
        orderByPositions.clear();
        partitionBy.clear();
        exclusionKind = 0;
        exclusionKindPos = 0;
        framingMode = 0;
        isIgnoreNulls = false;
        isSubsampleKeepFlag = false;
        liveViewDescription = null;
        nullsDescPos = 0;
        rowsHi = 0;
        rowsHiExprPos = 0;
        rowsHiExprTimeUnit = 0;
        rowsHiKindPos = 0;
        rowsLo = 0;
        rowsLoExprPos = 0;
        rowsLoExprTimeUnit = 0;
        rowsLoKindPos = 0;
    }

    public int getExclusionKind() {
        return exclusionKind;
    }

    public int getExclusionKindPos() {
        return exclusionKindPos;
    }

    public int getFramingMode() {
        return framingMode;
    }

    /** The syntax a live-view checkpoint identity derives from; null outside a live-view compile. */
    public LiveViewWindowDescription getLiveViewDescription() {
        return liveViewDescription;
    }


    public int getNullsDescPos() {
        return nullsDescPos;
    }

    public IntList getOrderByColumnIds() {
        return orderByColumnIds;
    }

    public IntList getOrderByDirections() {
        return orderByDirections;
    }

    public ObjList<CharSequence> getOrderByNames() {
        return orderByNames;
    }

    public IntList getOrderByPositions() {
        return orderByPositions;
    }

    public ObjList<BoundExpression> getPartitionBy() {
        return partitionBy;
    }

    public long getRowsHi() {
        return rowsHi;
    }

    public int getRowsHiExprPos() {
        return rowsHiExprPos;
    }

    public char getRowsHiExprTimeUnit() {
        return rowsHiExprTimeUnit;
    }

    public int getRowsHiKindPos() {
        return rowsHiKindPos;
    }

    public long getRowsLo() {
        return rowsLo;
    }

    public int getRowsLoExprPos() {
        return rowsLoExprPos;
    }

    public char getRowsLoExprTimeUnit() {
        return rowsLoExprTimeUnit;
    }

    public int getRowsLoKindPos() {
        return rowsLoKindPos;
    }

    public boolean isIgnoreNulls() {
        return isIgnoreNulls;
    }

    public boolean isSubsampleKeepFlag() {
        return isSubsampleKeepFlag;
    }

    public void setLiveViewDescription(LiveViewWindowDescription liveViewDescription) {
        this.liveViewDescription = liveViewDescription;
    }

    public WindowSpec of(WindowExpression expression) {
        exclusionKind = expression.getExclusionKind();
        exclusionKindPos = expression.getExclusionKindPos();
        framingMode = expression.getFramingMode();
        isIgnoreNulls = expression.isIgnoreNulls();
        isSubsampleKeepFlag = expression.isSubsampleKeepFlag();
        nullsDescPos = expression.getNullsDescPos();
        rowsHi = expression.getRowsHi();
        rowsHiExprPos = expression.getRowsHiExprPos();
        rowsHiExprTimeUnit = expression.getRowsHiExprTimeUnit();
        rowsHiKindPos = expression.getRowsHiKindPos();
        rowsLo = expression.getRowsLo();
        rowsLoExprPos = expression.getRowsLoExprPos();
        rowsLoExprTimeUnit = expression.getRowsLoExprTimeUnit();
        rowsLoKindPos = expression.getRowsLoKindPos();
        return this;
    }
}

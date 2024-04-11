/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;

public final class WindowColumn extends QueryColumn {

    public static final int CURRENT = 3;
    public static final int EXCLUDE_CURRENT_ROW = 1;
    public static final int EXCLUDE_GROUP = 2;
    public static final int EXCLUDE_NO_OTHERS = 4;
    public static final int EXCLUDE_TIES = 3;
    public final static ObjectFactory<WindowColumn> FACTORY = WindowColumn::new;
    public static final int FOLLOWING = 2;
    public static final int FRAMING_RANGE = 1;//1
    public static final int FRAMING_ROWS = FRAMING_RANGE + 1;//2
    public static final int FRAMING_GROUPS = FRAMING_ROWS + 1;//3
    public static final int PRECEDING = 1;
    public static final long TIME_UNIT_MICROSECOND = 1L;
    public static final long TIME_UNIT_MILLISECOND = 1000 * TIME_UNIT_MICROSECOND;
    public static final long TIME_UNIT_SECOND = 1000L * TIME_UNIT_MILLISECOND;
    public static final long TIME_UNIT_MINUTE = 60 * TIME_UNIT_SECOND;
    public static final long TIME_UNIT_HOUR = 60 * TIME_UNIT_MINUTE;
    public static final long TIME_UNIT_DAY = 24 * TIME_UNIT_HOUR;
    public static int ITME_UNIT_MICROSECOND = 1;
    private final ObjList<ExpressionNode> orderBy = new ObjList<>(2);
    private final IntList orderByDirection = new IntList(2);
    private final ObjList<ExpressionNode> partitionBy = new ObjList<>(2);
    private int exclusionKind = EXCLUDE_NO_OTHERS;
    private int exclusionKindPos;
    private int framingMode = FRAMING_RANGE;//default mode is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT
    private long rowsHi = Long.MAX_VALUE;
    private ExpressionNode rowsHiExpr;
    private int rowsHiExprPos;
    private long rowsHiExprTimeUnit;
    private int rowsHiExprTimeUnitPos;
    private int rowsHiKind = CURRENT;
    private int rowsHiKindPos = 0;
    private long rowsLo = Long.MIN_VALUE;
    private ExpressionNode rowsLoExpr;
    private int rowsLoExprPos;
    private long rowsLoExprTimeUnit;
    private int rowsLoExprTimeUnitPos;
    private int rowsLoKind = PRECEDING;
    private int rowsLoKindPos = 0;

    private WindowColumn() {
    }

    public void addOrderBy(ExpressionNode node, int direction) {
        orderBy.add(node);
        orderByDirection.add(direction);
    }

    @Override
    public void clear() {
        super.clear();
        partitionBy.clear();
        orderBy.clear();
        orderByDirection.clear();
        rowsLoExpr = null;
        rowsLoExprPos = 0;
        rowsLoExprTimeUnit = 1;
        rowsLoExprTimeUnitPos = 0;
        rowsHiExpr = null;
        rowsHiExprPos = 0;
        rowsHiExprTimeUnit = 1;
        rowsHiExprTimeUnitPos = 0;
        rowsLoKind = PRECEDING;
        rowsLoKindPos = 0;
        rowsHiKind = CURRENT;
        rowsHiKindPos = 0;
        framingMode = FRAMING_RANGE;
        rowsLo = Long.MIN_VALUE;
        rowsHi = Long.MAX_VALUE;
        exclusionKind = EXCLUDE_NO_OTHERS;
        exclusionKindPos = 0;
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

    public ObjList<ExpressionNode> getOrderBy() {
        return orderBy;
    }

    public IntList getOrderByDirection() {
        return orderByDirection;
    }

    public ObjList<ExpressionNode> getPartitionBy() {
        return partitionBy;
    }

    public long getRowsHi() {
        return rowsHi;
    }

    public ExpressionNode getRowsHiExpr() {
        return rowsHiExpr;
    }

    public int getRowsHiExprPos() {
        return rowsHiExprPos;
    }

    public long getRowsHiExprTimeUnit() {
        return rowsHiExprTimeUnit;
    }

    public int getRowsHiExprTimeUnitPos() {
        return rowsHiExprTimeUnitPos;
    }

    public int getRowsHiKind() {
        return rowsHiKind;
    }

    public int getRowsHiKindPos() {
        return rowsHiKindPos;
    }

    public long getRowsLo() {
        return rowsLo;
    }

    public ExpressionNode getRowsLoExpr() {
        return rowsLoExpr;
    }

    public int getRowsLoExprPos() {
        return rowsLoExprPos;
    }

    public long getRowsLoExprTimeUnit() {
        return rowsLoExprTimeUnit;
    }

    public int getRowsLoExprTimeUnitPos() {
        return rowsLoExprTimeUnitPos;
    }

    public int getRowsLoKind() {
        return rowsLoKind;
    }

    public int getRowsLoKindPos() {
        return rowsLoKindPos;
    }

    public boolean isNonDefaultFrame() {
        // default mode is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT
        // anything other than that is custom
        return framingMode != FRAMING_RANGE || rowsLoKind != PRECEDING || rowsHiKind != CURRENT || rowsHiExpr != null || rowsLoExpr != null;
    }

    @Override
    public boolean isWindowColumn() {
        return true;
    }

    @Override
    public WindowColumn of(CharSequence alias, ExpressionNode ast) {
        return (WindowColumn) super.of(alias, ast);
    }

    public boolean requiresOrderBy() {
        return framingMode == FRAMING_RANGE && (rowsLoKind != PRECEDING || (rowsHiKind != CURRENT && rowsHiKind != FOLLOWING) || rowsHiExpr != null || rowsLoExpr != null) ||
                framingMode == FRAMING_GROUPS;
    }

    public void setExclusionKind(int exclusionKind, int exclusionKindPos) {
        this.exclusionKind = exclusionKind;
        this.exclusionKindPos = exclusionKindPos;
    }

    public void setFramingMode(int framingMode) {
        this.framingMode = framingMode;
    }

    public void setRowsHi(long rowsHi) {
        this.rowsHi = rowsHi;
    }

    public void setRowsHiExpr(ExpressionNode rowsHiExpr, int rowsHiExprPos) {
        this.rowsHiExpr = rowsHiExpr;
        this.rowsHiExprPos = rowsHiExprPos;
    }

    public void setRowsHiExprTimeUnit(long unit) {
        this.rowsHiExprTimeUnit = unit;
    }

    public void setRowsHiExprTimeUnit(long rowsHiExprTimeUnit, int rowsHiExprTimeUnitPos) {
        this.rowsHiExprTimeUnit = rowsHiExprTimeUnit;
        this.rowsHiExprTimeUnitPos = rowsHiExprTimeUnitPos;
    }

    public void setRowsHiExprTimeUnitPos(int rowsHiExprTimeUnitPos) {
        this.rowsHiExprTimeUnitPos = rowsHiExprTimeUnitPos;
    }

    public void setRowsHiKind(int rowsHiKind, int rowsHiKindPos) {
        this.rowsHiKind = rowsHiKind;
        this.rowsHiKindPos = rowsHiKindPos;
    }

    public void setRowsLo(long rowsLo) {
        this.rowsLo = rowsLo;
    }

    public void setRowsLoExpr(ExpressionNode rowsLoExpr, int rowsLoExprPos) {
        this.rowsLoExpr = rowsLoExpr;
        this.rowsLoExprPos = rowsLoExprPos;
    }

    public void setRowsLoExprTimeUnit(long rowsLoExprTimeUnit, int rowsLoExprTimeUnitPos) {
        this.rowsLoExprTimeUnit = rowsLoExprTimeUnit;
        this.rowsLoExprTimeUnitPos = rowsLoExprTimeUnitPos;
    }

    public void setRowsLoExprTimeUnitPos(int rowsLoExprTimeUnitPos) {
        this.rowsLoExprTimeUnitPos = rowsLoExprTimeUnitPos;
    }

    public void setRowsLoKind(int rowsLoKind, int rowsLoKindPos) {
        this.rowsLoKind = rowsLoKind;
        this.rowsLoKindPos = rowsLoKindPos;
    }
}

/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

public final class AnalyticColumn extends QueryColumn {
    public static final int CURRENT = 3;
    public static final int EXCLUDE_CURRENT_ROW = 1;
    public static final int EXCLUDE_GROUP = 2;
    public static final int EXCLUDE_NO_OTHERS = 4;
    public static final int EXCLUDE_TIES = 3;
    public final static ObjectFactory<AnalyticColumn> FACTORY = AnalyticColumn::new;
    public static final int FOLLOWING = 2;
    public static final int FRAMING_GROUP = 3;
    public static final int FRAMING_RANGE = 1;
    public static final int FRAMING_ROWS = 2;
    public static final int PRECEDING = 1;
    private final ObjList<ExpressionNode> orderBy = new ObjList<>(2);
    private final IntList orderByDirection = new IntList(2);
    private final ObjList<ExpressionNode> partitionBy = new ObjList<>(2);
    private int exclusionKind = EXCLUDE_NO_OTHERS;
    private int framingMode = FRAMING_RANGE;
    private long rowsHi = Long.MAX_VALUE;
    private ExpressionNode rowsHiExpr;
    private int rowsHiExprPos;
    private int rowsHiKind = CURRENT;
    private int rowsHiKindPos = 0;
    private long rowsLo = Long.MIN_VALUE;
    private ExpressionNode rowsLoExpr;
    private int rowsLoExprPos;
    private int rowsLoKind = PRECEDING;
    private int rowsLoKindPos = 0;

    private AnalyticColumn() {
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
        rowsHiExpr = null;
        rowsHiExprPos = 0;
        rowsLoKind = PRECEDING;
        rowsLoKindPos = 0;
        rowsHiKind = CURRENT;
        rowsHiKindPos = 0;
        framingMode = FRAMING_RANGE;
        rowsLo = Long.MIN_VALUE;
        rowsHi = Long.MAX_VALUE;
        exclusionKind = EXCLUDE_NO_OTHERS;
    }

    public int getExclusionKind() {
        return exclusionKind;
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

    public int getRowsLoKind() {
        return rowsLoKind;
    }

    public int getRowsLoKindPos() {
        return rowsLoKindPos;
    }

    public boolean isNonDefault() {
        // default mode is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT
        // anything other than that is custom
        return framingMode != FRAMING_RANGE || rowsLoKind != PRECEDING || rowsHiKind != CURRENT || rowsHiExpr != null || rowsLoExpr != null;
    }

    @Override
    public boolean isWindowColumn() {
        return true;
    }

    @Override
    public AnalyticColumn of(CharSequence alias, ExpressionNode ast) {
        return (AnalyticColumn) super.of(alias, ast);
    }

    public void setExclusionKind(int exclusionKind) {
        this.exclusionKind = exclusionKind;
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

    public void setRowsLoKind(int rowsLoKind, int rowsLoKindPos) {
        this.rowsLoKind = rowsLoKind;
        this.rowsLoKindPos = rowsLoKindPos;
    }
}

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

import io.questdb.griffin.engine.functions.window.DenseRankFunctionFactory;
import io.questdb.griffin.engine.functions.window.FirstValueDoubleWindowFunctionFactory;
import io.questdb.griffin.engine.functions.window.LastValueDoubleWindowFunctionFactory;
import io.questdb.griffin.engine.functions.window.LeadLagWindowFunctionFactoryHelper;
import io.questdb.griffin.engine.functions.window.RankFunctionFactory;
import io.questdb.griffin.engine.functions.window.RowNumberFunctionFactory;
import io.questdb.std.Chars;
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
    public static final char TIME_UNIT_DAY = 'd';
    public static final char TIME_UNIT_HOUR = 'h';
    public static final char TIME_UNIT_MICROSECOND = 'u';
    public static final char TIME_UNIT_MILLISECOND = 'T';
    public static final char TIME_UNIT_MINUTE = 'm';
    public static final char TIME_UNIT_NANOSECOND = 'n';
    public static final char TIME_UNIT_SECOND = 's';
    private final ObjList<ExpressionNode> orderBy = new ObjList<>(2);
    private final IntList orderByDirection = new IntList(2);
    private final ObjList<ExpressionNode> partitionBy = new ObjList<>(2);
    private int exclusionKind = EXCLUDE_NO_OTHERS;
    private int exclusionKindPos;
    private int framingMode = FRAMING_RANGE;//default mode is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT
    private boolean ignoreNulls = false;
    private int nullsDescPos = 0;
    private long rowsHi = Long.MAX_VALUE;
    private ExpressionNode rowsHiExpr;
    private int rowsHiExprPos;
    private char rowsHiExprTimeUnit;
    private int rowsHiKind = CURRENT;
    private int rowsHiKindPos = 0;
    private long rowsLo = Long.MIN_VALUE;
    private ExpressionNode rowsLoExpr;
    private int rowsLoExprPos;
    private char rowsLoExprTimeUnit;
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
        rowsLoExprTimeUnit = 0;
        rowsHiExpr = null;
        rowsHiExprPos = 0;
        rowsHiExprTimeUnit = 0;
        rowsLoKind = PRECEDING;
        rowsLoKindPos = 0;
        rowsHiKind = CURRENT;
        rowsHiKindPos = 0;
        framingMode = FRAMING_RANGE;
        rowsLo = Long.MIN_VALUE;
        rowsHi = Long.MAX_VALUE;
        exclusionKind = EXCLUDE_NO_OTHERS;
        exclusionKindPos = 0;
        ignoreNulls = false;
        nullsDescPos = 0;
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

    public int getNullsDescPos() {
        return nullsDescPos;
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

    public char getRowsHiExprTimeUnit() {
        return rowsHiExprTimeUnit;
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

    public char getRowsLoExprTimeUnit() {
        return rowsLoExprTimeUnit;
    }

    public int getRowsLoKind() {
        return rowsLoKind;
    }

    public int getRowsLoKindPos() {
        return rowsLoKindPos;
    }

    public boolean isIgnoreNulls() {
        return ignoreNulls;
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

    public void setIgnoreNulls(boolean ignoreNulls) {
        this.ignoreNulls = ignoreNulls;
    }

    public void setNullsDescPos(int nullsDescPos) {
        this.nullsDescPos = nullsDescPos;
    }

    public void setRowsHi(long rowsHi) {
        this.rowsHi = rowsHi;
    }

    public void setRowsHiExpr(ExpressionNode rowsHiExpr, int rowsHiExprPos) {
        this.rowsHiExpr = rowsHiExpr;
        this.rowsHiExprPos = rowsHiExprPos;
    }

    public void setRowsHiExprTimeUnit(char rowsHiExprTimeUnit) {
        this.rowsHiExprTimeUnit = rowsHiExprTimeUnit;
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

    public void setRowsLoExprTimeUnit(char rowsLoExprTimeUnit) {
        this.rowsLoExprTimeUnit = rowsLoExprTimeUnit;
    }

    public void setRowsLoKind(int rowsLoKind, int rowsLoKindPos) {
        this.rowsLoKind = rowsLoKind;
        this.rowsLoKindPos = rowsLoKindPos;
    }

    public boolean stopOrderByPropagate(ObjList<ExpressionNode> modelOrder, IntList modelOrderDirection) {
        CharSequence token = getAst().token;

        // If this is an 'order' sensitive window function and there is no ORDER BY, it may depend on its child's ORDER BY clause.
        if ((Chars.equalsIgnoreCase(token, FirstValueDoubleWindowFunctionFactory.NAME) ||
                Chars.equalsIgnoreCase(token, LastValueDoubleWindowFunctionFactory.NAME)) &&
                orderBy.size() == 0 && modelOrder.size() == 0) {
            return true;
        }

        // Range frames work correctly depending on the ORDER BY clause of the subquery, which cannot be removed by the optimizer.
        boolean stopOrderBy = framingMode == FRAMING_RANGE && isRangeFrameDependOnSubqueryOrderBy(getAst().token) &&
                orderBy.size() > 0 && ((rowsHi != 0 || rowsLo != Long.MIN_VALUE) && !(rowsHi == Long.MAX_VALUE && rowsLo == Long.MIN_VALUE));

        // Heuristic. If current recordCursor has orderBy column exactly same as orderBy of window frame, we continue to push the order.
        if (stopOrderBy) {
            boolean sameOrder = true;
            if (modelOrder.size() < orderBy.size()) {
                sameOrder = false;
            } else {
                for (int i = 0, max = orderBy.size(); i < max; i++) {
                    if (!Chars.equalsIgnoreCase(modelOrder.getQuick(i).token, orderBy.getQuick(i).token) ||
                            modelOrderDirection.getQuick(i) != orderByDirection.getQuick(i)) {
                        sameOrder = false;
                        break;
                    }
                }
            }
            stopOrderBy = !sameOrder;
        }
        return stopOrderBy;
    }

    private static boolean isRangeFrameDependOnSubqueryOrderBy(CharSequence funName) {
        return !Chars.equalsIgnoreCase(funName, RowNumberFunctionFactory.NAME)
                && !Chars.equalsIgnoreCase(funName, RankFunctionFactory.NAME)
                && !Chars.equalsIgnoreCase(funName, DenseRankFunctionFactory.NAME)
                && !Chars.equalsIgnoreCase(funName, LeadLagWindowFunctionFactoryHelper.LEAD_NAME)
                && !Chars.equalsIgnoreCase(funName, LeadLagWindowFunctionFactoryHelper.LAG_NAME);
    }
}

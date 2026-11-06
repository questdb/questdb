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
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;

/**
 * Bound keys and normalized frame parameters; no parser nodes or executable state.
 */
public final class WindowSpec implements PlanFactSource {
    public static final ObjectFactory<WindowSpec> FACTORY = WindowSpec::new;
    private final IntList orderByColumnIds = new IntList(0);
    private final ObjList<SortDirection> orderByDirections = new ObjList<>(0);
    private final ObjList<CharSequence> orderByNames = new ObjList<>(0);
    private final IntList orderByPositions = new IntList(0);
    private final ObjList<BoundExpression> partitionBy = new ObjList<>(0);
    private int exclusionKind;
    private int exclusionKindPos;
    private int framingMode;
    private boolean isIgnoreNulls;
    private boolean isSubsampleKeepFlag;
    private LiveViewWindowDescription liveViewDescription;
    private int nullsDescPos;
    private OrderSource orderSource;
    private long rowsHi;
    private int rowsHiExprPos;
    private char rowsHiExprTimeUnit;
    private int rowsHiKindPos;
    private long rowsLo;
    private int rowsLoExprPos;
    private char rowsLoExprTimeUnit;
    private int rowsLoKindPos;

    /**
     * Appends an ORDER BY key: the input column it reads, or -1 for an expression that is not a column.
     */
    public void addOrderBy(int columnId, SortDirection direction, CharSequence name, int position) {
        orderByColumnIds.add(columnId);
        orderByDirections.add(direction);
        orderByNames.add(name);
        orderByPositions.add(position);
    }

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
        orderSource = null;
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

    /**
     * The syntax a live-view checkpoint identity derives from; null outside a live-view compile.
     */
    public LiveViewWindowDescription getLiveViewDescription() {
        return liveViewDescription;
    }


    public int getNullsDescPos() {
        return nullsDescPos;
    }

    public IntList getOrderByColumnIds() {
        return orderByColumnIds;
    }

    public ObjList<SortDirection> getOrderByDirections() {
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

    /**
     * True when the input of the window delivers the rows in this window's ORDER BY, so the window need not order
     * them; operator planning decides it, see {@link OrderSource}, before which a read fails under
     * {@link io.questdb.ParanoiaState#PLAN_PARANOIA_MODE}.
     */
    public boolean isOrderDelivered() {
        PlanFacts.checkDecided(orderSource != null, "WindowSpec.orderSource");
        return orderSource == OrderSource.INPUT;
    }

    /**
     * True when the query order delivers the rows of each partition of the window in the window ORDER BY: the query
     * order starts with columns the window partitions by, in either direction, and goes on with the window ORDER BY.
     */
    public boolean isOrderedByQueryOrder(SortKeys queryOrder) {
        final int count = orderByColumnIds.size();
        if (count == 0) {
            return false;
        }
        for (int partitionKeyCount = 0, n = queryOrder.size() - count; partitionKeyCount <= n; partitionKeyCount++) {
            if (isQueryOrderAt(queryOrder, partitionKeyCount)) {
                return true;
            }
            if (!isPartitionColumn(queryOrder.getColumnIds().getQuick(partitionKeyCount))) {
                return false;
            }
        }
        return false;
    }

    public boolean isSubsampleKeepFlag() {
        return isSubsampleKeepFlag;
    }

    public WindowSpec ofFrame(
            int framingMode,
            long rowsLo,
            int rowsLoKindPos,
            int rowsLoExprPos,
            char rowsLoExprTimeUnit,
            long rowsHi,
            int rowsHiKindPos,
            int rowsHiExprPos,
            char rowsHiExprTimeUnit,
            int exclusionKind,
            int exclusionKindPos,
            boolean isIgnoreNulls,
            int nullsDescPos,
            boolean isSubsampleKeepFlag
    ) {
        this.exclusionKind = exclusionKind;
        this.exclusionKindPos = exclusionKindPos;
        this.framingMode = framingMode;
        this.isIgnoreNulls = isIgnoreNulls;
        this.isSubsampleKeepFlag = isSubsampleKeepFlag;
        this.nullsDescPos = nullsDescPos;
        this.rowsHi = rowsHi;
        this.rowsHiExprPos = rowsHiExprPos;
        this.rowsHiExprTimeUnit = rowsHiExprTimeUnit;
        this.rowsHiKindPos = rowsHiKindPos;
        this.rowsLo = rowsLo;
        this.rowsLoExprPos = rowsLoExprPos;
        this.rowsLoExprTimeUnit = rowsLoExprTimeUnit;
        this.rowsLoKindPos = rowsLoKindPos;
        return this;
    }

    /**
     * Takes the frame, exclusion, null handling and live-view description of the spec; the keys stay empty.
     */
    public WindowSpec ofFrame(WindowSpec spec) {
        exclusionKind = spec.exclusionKind;
        exclusionKindPos = spec.exclusionKindPos;
        framingMode = spec.framingMode;
        isIgnoreNulls = spec.isIgnoreNulls;
        isSubsampleKeepFlag = spec.isSubsampleKeepFlag;
        liveViewDescription = spec.liveViewDescription;
        nullsDescPos = spec.nullsDescPos;
        rowsHi = spec.rowsHi;
        rowsHiExprPos = spec.rowsHiExprPos;
        rowsHiExprTimeUnit = spec.rowsHiExprTimeUnit;
        rowsHiKindPos = spec.rowsHiKindPos;
        rowsLo = spec.rowsLo;
        rowsLoExprPos = spec.rowsLoExprPos;
        rowsLoExprTimeUnit = spec.rowsLoExprTimeUnit;
        rowsLoKindPos = spec.rowsLoKindPos;
        return this;
    }

    /**
     * Takes the frame of a window with no frame clause: RANGE from the first row of the partition to the last.
     */
    public WindowSpec ofUnboundedFrame() {
        exclusionKind = WindowExpression.EXCLUDE_NO_OTHERS;
        framingMode = WindowExpression.FRAMING_RANGE;
        rowsHi = Long.MAX_VALUE;
        rowsLo = Long.MIN_VALUE;
        return this;
    }

    public void setLiveViewDescription(LiveViewWindowDescription liveViewDescription) {
        this.liveViewDescription = liveViewDescription;
    }

    public void setOrderDelivered(boolean isOrderDelivered) {
        orderSource = isOrderDelivered ? OrderSource.INPUT : OrderSource.WINDOW;
    }

    @Override
    public void visitFacts(PlanFactRecorder visitor) {
        visitor.facts("orderByDirections", orderByDirections);
        visitor.facts("orderByNames", orderByNames);
        visitor.facts("orderByPositions", orderByPositions);
        visitor.fact("exclusionKind", exclusionKind);
        visitor.fact("exclusionKindPos", exclusionKindPos);
        visitor.fact("framingMode", framingMode);
        visitor.fact("isIgnoreNulls", isIgnoreNulls);
        visitor.fact("isSubsampleKeepFlag", isSubsampleKeepFlag);
        visitor.fact("liveViewDescription", liveViewDescription);
        visitor.fact("nullsDescPos", nullsDescPos);
        visitor.fact("orderSource", orderSource);
        visitor.fact("rowsHi", rowsHi);
        visitor.fact("rowsHiExprPos", rowsHiExprPos);
        visitor.fact("rowsHiExprTimeUnit", rowsHiExprTimeUnit);
        visitor.fact("rowsHiKindPos", rowsHiKindPos);
        visitor.fact("rowsLo", rowsLo);
        visitor.fact("rowsLoExprPos", rowsLoExprPos);
        visitor.fact("rowsLoExprTimeUnit", rowsLoExprTimeUnit);
        visitor.fact("rowsLoKindPos", rowsLoKindPos);
    }

    @Override
    public void visitReads(PlanExpressionVisitor visitor) {
        PlanReads.expressions(partitionBy, visitor);
        PlanReads.columnIds(orderByColumnIds, orderByPositions, visitor);
    }

    private boolean isPartitionColumn(int columnId) {
        for (int i = 0, n = partitionBy.size(); i < n; i++) {
            if (partitionBy.getQuick(i) instanceof ColumnExpression column && column.getColumnId() == columnId
                    && column.isDirectReference() && !column.isCast()) {
                return true;
            }
        }
        return false;
    }

    private boolean isQueryOrderAt(SortKeys queryOrder, int offset) {
        for (int i = 0, n = orderByColumnIds.size(); i < n; i++) {
            if (orderByColumnIds.getQuick(i) != queryOrder.getColumnIds().getQuick(offset + i)
                    || orderByDirections.getQuick(i) != queryOrder.getDirections().getQuick(offset + i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * What orders the rows a window function reads by the window's ORDER BY, which operator planning decides; null
     * until it has.
     */
    public enum OrderSource {
        /**
         * The input of the window delivers the order.
         */
        INPUT,
        /**
         * The window orders the rows itself.
         */
        WINDOW
    }
}

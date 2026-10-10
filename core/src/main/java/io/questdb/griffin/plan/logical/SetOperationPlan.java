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
import io.questdb.std.ObjectFactory;

import java.util.Objects;

public final class SetOperationPlan extends LogicalPlan {
    public static final ObjectFactory<SetOperationPlan> FACTORY = SetOperationPlan::new;
    private final IntList remappedSymbolColumns = new IntList();
    private final IntList symbolColumns = new IntList();
    private boolean isMerged;
    private boolean isSymbolRestorationRequired;
    private LogicalPlan left;
    private SetOperationKind operation;
    private LogicalPlan right;
    private int requestedOrderColumnId = -1;
    private SortDirection requestedOrderDirection;
    private SortPlan.Algorithm rightBranchSort;
    private int rightPosition = -1;

    @Override
    public void clear() {
        super.clear();
        left = null;
        right = null;
        operation = null;
        isMerged = false;
        isSymbolRestorationRequired = false;
        requestedOrderColumnId = -1;
        requestedOrderDirection = null;
        rightBranchSort = null;
        rightPosition = -1;
        symbolColumns.clear();
    }

    public LogicalPlan getLeft() {
        return left;
    }

    public SetOperationKind getOperation() {
        return operation;
    }

    /**
     * The output column the consumer would like the rows ordered by, or -1.
     */
    public int getRequestedOrderColumnId() {
        return requestedOrderColumnId;
    }

    /**
     * The direction of {@link #getRequestedOrderColumnId()}, or null when the consumer names none.
     */
    public SortDirection getRequestedOrderDirection() {
        return requestedOrderDirection;
    }

    public LogicalPlan getRight() {
        return right;
    }

    /**
     * How the generator sorts the right branch of a UNION ALL into the requested timestamp order, or null when it
     * does not sort it.
     */
    public SortPlan.Algorithm getRightBranchSort() {
        return rightBranchSort;
    }

    public int getRightPosition() {
        return rightPosition;
    }

    public IntList getSymbolColumns() {
        return symbolColumns;
    }

    @Override
    public LogicalPlan inputAt(int index) {
        return switch (index) {
            case 0 -> left;
            case 1 -> right;
            default -> throw new IndexOutOfBoundsException("input index: " + index);
        };
    }

    @Override
    public int inputCount() {
        return 2;
    }

    /**
     * True when the UNION ALL merges its branches, each in the requested timestamp order, instead of concatenating
     * them; operator planning decides it.
     */
    public boolean isMerged() {
        return isMerged;
    }

    public boolean isSymbolRestorationRequired() {
        return isSymbolRestorationRequired;
    }

    /**
     * True when every UNION ALL branch has a designated timestamp and the first one is the requested order column.
     */
    public boolean isTimestampOrderPushable(int orderIndex) {
        LogicalPlan plan = this;
        while (plan instanceof SetOperationPlan union && union.operation == SetOperationKind.UNION_ALL) {
            if (union.right.getOutput().getTimestampIndex() < 0) {
                return false;
            }
            plan = union.left;
        }
        return plan.getOutput().getTimestampIndex() == orderIndex;
    }

    public SetOperationPlan of(LogicalPlan left, LogicalPlan right, SetOperationKind operation, int leftPosition, int rightPosition, boolean isSymbolRestorationRequired) {
        this.left = Objects.requireNonNull(left);
        this.right = Objects.requireNonNull(right);
        this.operation = Objects.requireNonNull(operation);
        this.rightPosition = rightPosition;
        this.isSymbolRestorationRequired = isSymbolRestorationRequired;
        setPosition(leftPosition);
        return this;
    }

    /**
     * Reindexes symbol restoration after retaining original output ordinals in the given order.
     */
    public void remapSymbolColumns(IntList retainedColumnIndexes) {
        remappedSymbolColumns.clear();
        for (int i = 0, n = retainedColumnIndexes.size(); i < n; i++) {
            if (symbolColumns.binarySearchUniqueList(retainedColumnIndexes.getQuick(i)) >= 0) {
                remappedSymbolColumns.add(i);
            }
        }
        symbolColumns.clear();
        symbolColumns.addAll(remappedSymbolColumns);
        remappedSymbolColumns.clear();
    }

    @Override
    public void replaceInput(int index, LogicalPlan input) {
        switch (index) {
            case 0 -> left = Objects.requireNonNull(input);
            case 1 -> right = Objects.requireNonNull(input);
            default -> throw new IndexOutOfBoundsException("input index: " + index);
        }
    }

    public void setMerge(boolean isMerged, SortPlan.Algorithm rightBranchSort) {
        this.isMerged = isMerged;
        this.rightBranchSort = rightBranchSort;
    }

    public void setRequestedOrder(int columnId, SortDirection direction) {
        requestedOrderColumnId = columnId;
        requestedOrderDirection = direction;
    }

}

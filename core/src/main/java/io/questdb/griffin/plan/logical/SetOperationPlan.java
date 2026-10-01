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

import io.questdb.griffin.model.QueryModel;
import io.questdb.std.IntList;
import io.questdb.std.ObjectFactory;

import java.util.Objects;

public final class SetOperationPlan extends LogicalPlan {
    public static final ObjectFactory<SetOperationPlan> FACTORY = SetOperationPlan::new;
    private final IntList remappedSymbolColumns = new IntList();
    private final IntList symbolColumns = new IntList();
    private LogicalPlan left;
    private int operation = -1;
    private boolean restoreSymbols;
    private LogicalPlan right;
    private int rightPosition = -1;

    @Override
    public void clear() {
        super.clear();
        left = null;
        right = null;
        operation = -1;
        restoreSymbols = false;
        rightPosition = -1;
        symbolColumns.clear();
    }

    public LogicalPlan getLeft() {
        return left;
    }

    public int getOperation() {
        return operation;
    }

    public LogicalPlan getRight() {
        return right;
    }

    public int getRightPosition() {
        return rightPosition;
    }

    public IntList getSymbolColumns() {
        return symbolColumns;
    }

    @Override
    public Type getType() {
        return Type.SET_OPERATION;
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

    public boolean isSymbolRestorationRequired() {
        return restoreSymbols;
    }

    public SetOperationPlan of(LogicalPlan left, LogicalPlan right, int operation, int leftPosition, int rightPosition, boolean restoreSymbols) {
        if (operation < QueryModel.SET_OPERATION_UNION_ALL || operation > QueryModel.SET_OPERATION_INTERSECT_ALL) {
            throw new IllegalArgumentException("set operation: " + operation);
        }
        this.left = Objects.requireNonNull(left);
        this.right = Objects.requireNonNull(right);
        this.operation = operation;
        this.rightPosition = rightPosition;
        this.restoreSymbols = restoreSymbols;
        setPosition(leftPosition);
        return this;
    }

    /** Reindexes symbol restoration after retaining original output ordinals in the given order. */
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
}

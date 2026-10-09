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
import io.questdb.std.ObjList;

public final class ProjectPlan extends UnaryPlan {
    public static final ObjectFactory<ProjectPlan> FACTORY = ProjectPlan::new;
    private final ObjList<BoundExpression> expressions = new ObjList<>();
    private final IntList updateTargetTypes = new IntList();
    private boolean hasPrunedComputedColumns;
    private boolean hasTimestampDeclaration;
    private boolean isImplied;

    @Override
    public void clear() {
        super.clear();
        expressions.clear();
        updateTargetTypes.clear();
        hasPrunedComputedColumns = false;
        hasTimestampDeclaration = false;
        isImplied = false;
    }

    public ObjList<BoundExpression> getExpressions() {
        return expressions;
    }

    public IntList getUpdateTargetTypes() {
        return updateTargetTypes;
    }

    public boolean hasUpdateConversions() {
        return updateTargetTypes.size() > 0;
    }

    /**
     * True when column pruning removed every computed column of a computing projection. The
     * generator still evaluates it as a virtual record, which the optimiser does not merge with
     * column projections.
     */
    public boolean hasPrunedComputedColumns() {
        return hasPrunedComputedColumns;
    }

    public boolean hasTimestampDeclaration() {
        return hasTimestampDeclaration;
    }

    /**
     * True when the query text names none of the projection's columns: it stands for a bare table name, which SQL
     * reads as the table rather than as {@code SELECT *}.
     */
    public boolean isImplied() {
        return isImplied;
    }

    public void markImplied() {
        isImplied = true;
    }

    public void markTimestampDeclaration() {
        hasTimestampDeclaration = true;
    }

    public void setPrunedComputedColumns(boolean hasPrunedComputedColumns) {
        this.hasPrunedComputedColumns = hasPrunedComputedColumns;
    }

    public ProjectPlan of(LogicalPlan input, int position) {
        configure(input, position);
        return this;
    }
}

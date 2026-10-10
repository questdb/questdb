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

import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;

import java.util.Objects;

/**
 * Consecutive WINDOW JOIN steps over one master. Each step aggregates the slave rows inside its time
 * window for every master row; the output is the master columns followed by every step's aggregates.
 */
public final class WindowJoinPlan extends LogicalPlan {
    public static final ObjectFactory<WindowJoinPlan> FACTORY = WindowJoinPlan::new;
    private final ObjList<WindowJoinStep> steps = new ObjList<>();
    private boolean isEmpty;
    private LogicalPlan master;

    @Override
    public void clear() {
        super.clear();
        steps.clear();
        isEmpty = false;
        master = null;
    }

    public LogicalPlan getMaster() {
        return master;
    }

    public ObjList<WindowJoinStep> getSteps() {
        return steps;
    }

    @Override
    public LogicalPlan inputAt(int index) {
        if (index == 0) {
            return master;
        }
        return steps.getQuick(index - 1).getSlave();
    }

    @Override
    public int inputCount() {
        return steps.size() + 1;
    }

    public boolean isEmpty() {
        return isEmpty;
    }

    public WindowJoinPlan of(LogicalPlan master, int position) {
        this.master = Objects.requireNonNull(master);
        setPosition(position);
        return this;
    }

    @Override
    public void replaceInput(int index, LogicalPlan input) {
        if (index == 0) {
            master = Objects.requireNonNull(input);
        } else {
            steps.getQuick(index - 1).setSlave(input);
        }
    }

    public void setEmpty(boolean isEmpty) {
        this.isEmpty = isEmpty;
    }

    @Override
    public void visitReads(PlanExpressionVisitor visitor) {
        for (int i = 0, n = steps.size(); i < n; i++) {
            steps.getQuick(i).visitReads(visitor);
        }
    }
}

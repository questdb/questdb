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

import java.util.Objects;

public abstract sealed class UnaryPlan extends LogicalPlan
        permits ForwardingPlan, GroupingPlan, ProjectPlan, WindowPlan {
    private LogicalPlan child;

    @Override
    public void clear() {
        super.clear();
        child = null;
    }

    public LogicalPlan getChild() {
        return child;
    }

    @Override
    public LogicalPlan inputAt(int index) {
        checkInputIndex(index);
        return child;
    }

    @Override
    public int inputCount() {
        return 1;
    }

    @Override
    public void replaceInput(int index, LogicalPlan input) {
        checkInputIndex(index);
        this.child = Objects.requireNonNull(input);
    }

    private static void checkInputIndex(int index) {
        if (index != 0) {
            throw new IndexOutOfBoundsException("input index: " + index);
        }
    }

    protected void configure(LogicalPlan input, int position) {
        this.child = Objects.requireNonNull(input);
        setPosition(position);
    }
}

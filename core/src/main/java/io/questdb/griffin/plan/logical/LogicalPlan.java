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

import io.questdb.std.Mutable;

/**
 * A pooled logical operation, borrowed until its compiler is reset.
 * Clearing a node releases its own references, never its inputs or expressions.
 */
public abstract class LogicalPlan implements Mutable {
    private final OutputSchema output = new OutputSchema();
    private int position = -1;

    @Override
    public void clear() {
        output.clear();
        position = -1;
    }

    public OutputSchema getOutput() {
        return output;
    }

    public int getPosition() {
        return position;
    }

    public abstract Type getType();

    public abstract LogicalPlan inputAt(int index);

    public abstract int inputCount();

    public abstract void replaceInput(int index, LogicalPlan input);

    public void setPosition(int position) {
        this.position = position;
    }

    public enum Type {
        SCAN,
        FUNCTION_SOURCE,
        DISTINCT,
        AGGREGATE,
        SAMPLE_BY,
        FILL,
        WINDOW,
        JOIN,
        WINDOW_JOIN,
        HORIZON_JOIN,
        LATEST_BY,
        SET_OPERATION,
        FILTER,
        PROJECT,
        SORT,
        LIMIT
    }
}

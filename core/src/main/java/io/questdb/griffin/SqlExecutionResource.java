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


package io.questdb.griffin;

import io.questdb.std.Mutable;

/**
 * Per-execution state that a cached function keeps between executions, such as the dictionary of
 * a cast to SYMBOL. The function registers it from {@code init()}; the execution clears it when it
 * ends, so a cached query does not hold the state of its last run.
 */
public abstract class SqlExecutionResource implements Mutable {
    private SqlExecutionResources owner;

    /**
     * Registers this resource with the execution that runs on the given context.
     *
     * @return true when an execution owns the resource and clears it when it ends, false when no
     * execution runs on the context and the caller must clear the resource itself
     */
    public final boolean registerWith(SqlExecutionContext executionContext) {
        final SqlExecutionResources resources = executionContext.getExecutionResources();
        if (resources == null) {
            return false;
        }
        if (owner != resources) {
            resources.add(this);
            owner = resources;
        }
        return true;
    }

    final void release(SqlExecutionResources resources) {
        // A function initialized by a later execution belongs to that execution now.
        // The earlier execution must not clear its new state.
        if (owner == resources) {
            owner = null;
            clear();
        }
    }
}

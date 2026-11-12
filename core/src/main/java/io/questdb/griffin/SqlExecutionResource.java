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
 * Reusable execution state held by a cached function. The execution frees its contents directly,
 * without visiting the function's parents. Registration and release run on the query owner; worker
 * threads must finish using the resource before that owner ends the execution.
 */
public abstract class SqlExecutionResource implements Mutable {
    private SqlExecutionResources owner;

    protected final void registerWith(SqlExecutionContext executionContext) {
        final SqlExecutionResources resources = executionContext.getExecutionResources();
        if (resources != null && owner != resources) {
            resources.add(this);
            owner = resources;
        }
    }

    final void release(SqlExecutionResources resources) {
        // Compilation can hand a function to an execution before the compilation scope ends.
        // A stale registration must not clear the function's new execution state.
        if (owner == resources) {
            owner = null;
            clear();
        }
    }
}

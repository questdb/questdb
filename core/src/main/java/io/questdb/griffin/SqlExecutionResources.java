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

import io.questdb.cairo.CairoException;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.TestOnly;

/**
 * Owns temporary resources of one execution, not of its reusable SqlExecutionContext. QueryRegistry
 * keeps one scope per registration, including suspended portals, and closes it before its tracker.
 * The compiler uses a separate scope for functions evaluated while building a plan.
 */
public final class SqlExecutionResources implements QuietCloseable {
    private final ObjList<SqlExecutionResource> resources = new ObjList<>();
    private long generation;
    private boolean isOpen;
    private SqlExecutionResources previous;
    private long previousGeneration;

    public void begin() {
        assert !isOpen && resources.size() == 0;
        generation++;
        isOpen = true;
    }

    public void bind(SqlExecutionContext executionContext) {
        assert isOpen;
        previous = executionContext.getExecutionResources();
        previousGeneration = previous != null ? previous.generation : 0;
        executionContext.setExecutionResources(this);
    }

    @Override
    public void close() {
        isOpen = false;
        Throwable failure = null;
        for (int i = resources.size() - 1; i >= 0; i--) {
            final SqlExecutionResource resource = resources.getQuick(i);
            resources.setQuick(i, null);
            try {
                resource.release(this);
            } catch (Throwable th) {
                failure = Misc.foldCleanupFailure(failure, th);
            }
        }
        resources.clear();
        CairoException.rethrowCleanupFailure(failure);
    }

    @TestOnly
    public int getResourceCount() {
        return resources.size();
    }

    public boolean isOpen() {
        return isOpen;
    }

    public void unbind(SqlExecutionContext executionContext) {
        if (executionContext.getExecutionResources() == this) {
            // An older portal may already have finished and its registry entry may have been
            // reused. Never restore that entry's new execution onto the old portal's context.
            executionContext.setExecutionResources(previous != null && previous.isOpen
                    && previous.generation == previousGeneration ? previous : null);
        }
        previous = null;
    }

    void add(SqlExecutionResource resource) {
        assert isOpen;
        resources.add(resource);
    }

}

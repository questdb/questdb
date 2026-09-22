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

/**
 * The registry-owner side of one protocol statement: begun once the statement is classified,
 * mounted for every executable segment and ended together with the statement. An owner ID of
 * -1 means the engine runs the statement unmanaged; such an owner only ends.
 */
public final class SqlExecutionOwner {
    private static final long UNINITIALIZED = Long.MIN_VALUE;
    private SqlExecutionContext executionContext;
    private long id = UNINITIALIZED;
    private boolean isMounted;

    public void begin(CharSequence query, SqlExecutionContext executionContext, short compiledQueryType) {
        if (id != UNINITIALIZED) {
            throw new IllegalStateException("SQL execution owner is already initialized");
        }
        final long ownerId = executionContext.getCairoEngine().beginSqlExecution(query, executionContext, compiledQueryType);
        this.executionContext = executionContext;
        id = ownerId;
        isMounted = ownerId > -1;
    }

    public void end() {
        final long ownerId = id;
        if (ownerId == UNINITIALIZED) {
            return;
        }
        final SqlExecutionContext context = executionContext;
        try {
            context.getCairoEngine().endSqlExecution(ownerId, context);
        } finally {
            executionContext = null;
            id = UNINITIALIZED;
            isMounted = false;
        }
    }

    public long getId() {
        return id;
    }

    public boolean isStarted() {
        return id != UNINITIALIZED;
    }

    public void mount() {
        if (id > -1 && !isMounted) {
            executionContext.getCairoEngine().mountSqlExecution(id, executionContext);
            isMounted = true;
        }
    }

    public void publish(CharSequence query, boolean containsSecret) {
        if (id > -1) {
            executionContext.getCairoEngine().publishSqlExecutionQuery(id, query, containsSecret, executionContext);
        }
    }

    public void unmount() {
        if (id > -1 && isMounted) {
            executionContext.getCairoEngine().unmountSqlExecution(id, executionContext);
            isMounted = false;
        }
    }
}

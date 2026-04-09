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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;

/**
 * Thin wrapper that delegates to a shared factory's materialized data
 * via {@link RecordCursorFactory#getSharedCursor(SqlExecutionContext, int)}.
 * Multiple instances with different sharedIds share the same underlying
 * execution without re-scanning the base data.
 * <p>
 * This factory does not own {@code primaryFactory} and intentionally
 * does not override {@code _close()} — ownership belongs to the node
 * in the main factory tree that holds the primary factory directly.
 */
public class SharedRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory primaryFactory;
    private final int sharedId;

    public SharedRecordCursorFactory(RecordCursorFactory primaryFactory, int sharedId) {
        super(primaryFactory.getMetadata());
        this.primaryFactory = primaryFactory;
        this.sharedId = sharedId;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        return primaryFactory.getSharedCursor(executionContext, sharedId);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return primaryFactory.recordCursorSupportsRandomAccess();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("(Shared)");
        sink.child(primaryFactory);
    }
}

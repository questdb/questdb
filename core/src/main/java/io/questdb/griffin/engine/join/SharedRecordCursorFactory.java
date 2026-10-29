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
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

/**
 * A place of a factory tree that reads a {@link SharedPrimary} through a cursor of its own, see
 * {@link io.questdb.cairo.sql.RecordCursorFactory#getSharedCursor(SqlExecutionContext, int)}. Several such places
 * read the result that the primary computes once, without scanning its base again.
 * <p>
 * The factory does not own the primary, unless {@link SharedPrimary#handOver()} promotes it to the home of a primary
 * whose tree holds no other home: it then reads the cursor of the primary itself and closes the primary on close.
 */
public class SharedRecordCursorFactory extends AbstractRecordCursorFactory {
    private final SharedPrimary primary;
    private final int sharedId;
    private boolean isHome;

    public SharedRecordCursorFactory(SharedPrimary primary, int sharedId) {
        super(primary.getFactory().getMetadata());
        this.primary = primary;
        this.sharedId = sharedId;
        primary.addReader(this);
    }

    // The plan of a reader promoted to home is that of the primary, which names its columns through this factory, see
    // toPlan().
    @Override
    public String getBaseColumnName(int idx) {
        return primary.getFactory().getBaseColumnName(idx);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        if (isHome) {
            return primary.getFactory().getCursor(executionContext);
        }
        return primary.getFactory().getSharedCursor(executionContext, sharedId);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return primary.getFactory().recordCursorSupportsRandomAccess();
    }

    @Override
    public void toPlan(PlanSink sink) {
        if (isHome) {
            primary.getFactory().toPlan(sink);
        } else {
            sink.type("(Shared)");
            sink.child(primary.getFactory());
        }
    }

    // Shares a single underlying factory without exposing it through getBaseFactory(), so the
    // external-source property is propagated explicitly rather than via the fail-open default.
    //
    // This is a structural contract, not a load-bearing step in the mat-view rejection guard: the
    // home of the primary, a node of the main factory tree, gives the guard a second route to the
    // same leaf, so rejection does not depend on this override. It exists so the factory does not
    // misreport itself to a future consumer that walks a sub-tree in isolation. Pinned by
    // SharedRecordCursorFactoryExternalSourceTest, which asserts the property on the factory
    // directly - an end-to-end mat-view probe cannot distinguish its presence from its absence.
    @Override
    public boolean usesExternalDataSource() {
        return primary.getFactory().usesExternalDataSource();
    }

    @Override
    protected void _close() {
        primary.releaseReader(this);
        if (isHome) {
            Misc.free(primary.getFactory());
        }
    }

    void promoteToHome() {
        isHome = true;
    }
}

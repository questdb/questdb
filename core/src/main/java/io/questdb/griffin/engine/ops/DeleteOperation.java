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

package io.questdb.griffin.engine.ops;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.AsyncWriterCommand;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.MetadataService;
import io.questdb.std.Misc;
import io.questdb.tasks.TableWriterTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.tasks.TableWriterTask.CMD_DELETE_TABLE;

public class DeleteOperation extends AbstractOperation {
    public static final String MAT_VIEW_INVALIDATION_REASON = "delete operation";
    private RecordCursorFactory survivorFactory;

    public DeleteOperation(
            @NotNull TableToken tableToken,
            int tableId,
            long tableVersion,
            int tableNamePosition,
            @Nullable RecordCursorFactory survivorFactory
    ) {
        init(CMD_DELETE_TABLE, TableWriterTask.getCommandName(CMD_DELETE_TABLE), tableToken, tableId, tableVersion, tableNamePosition);
        this.survivorFactory = survivorFactory;
    }

    @Override
    public long apply(MetadataService svc, boolean contextAllowsAnyStructureChanges) {
        // v1 supports WAL tables only; the WAL-apply path uses OperationExecutor.executeDelete,
        // not this method. A direct (non-WAL) apply is rejected at compile time, so reaching here
        // is a programming error.
        throw CairoException.nonCritical()
                .put("DELETE is only supported on WAL tables [table=")
                .put(getTableToken().getTableName())
                .put(']');
    }

    @Override
    public void authorize() {
        final SecurityContext securityContext = this.securityContext;
        if (securityContext == null) {
            throw CairoException.nonCritical()
                    .put("delete security context is empty [table=")
                    .put(getTableToken().getTableName())
                    .put(']');
        }
        securityContext.authorizeTableDelete(getTableToken());
    }

    @Override
    public void close() {
        survivorFactory = Misc.free(survivorFactory);
    }

    @Override
    public AsyncWriterCommand deserialize(TableWriterTask task) {
        return task.getAsyncWriterCommand();
    }

    public RecordCursorFactory getSurvivorFactory() {
        return survivorFactory;
    }

    @Override
    public boolean isStructural() {
        return false;
    }

    @Override
    public String matViewInvalidationReason() {
        return MAT_VIEW_INVALIDATION_REASON;
    }

    @Override
    public void serialize(TableWriterTask task) {
        super.serialize(task);
        task.setAsyncWriterCommand(this);
    }

    @Override
    public void startAsync() {
        // DeleteOperation is WAL-only; async execution is handled by OperationExecutor.
        // This is a no-op in v1.
    }
}

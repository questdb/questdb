/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SCSequence;
import org.jetbrains.annotations.Nullable;

public class UpdateOperationSender implements OperationSender<UpdateOperation> {
    private final DoneOperationFuture doneFuture = new DoneOperationFuture();
    private final CairoEngine engine;
    private final OperationFutureImpl updateFuture;

    public UpdateOperationSender(CairoEngine engine) {
        this.engine = engine;
        this.updateFuture = new OperationFutureImpl(engine);
    }

    public OperationFuture execute(UpdateOperation operation, SqlExecutionContext sqlExecutionContext, @Nullable SCSequence eventSubSeq) throws SqlException {
        // storing execution context for UPDATE execution
        // writer thread will call `apply()` when thread is ready to do so
        // `apply()` will use context stored in the operation
        operation.withContext(sqlExecutionContext);
        try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), operation.getTableName(), "sync 'UPDATE' execution")) {
            return doneFuture.of(writer.getUpdateOperator().executeUpdate(sqlExecutionContext, operation));
        } catch (EntryUnavailableException busyException) {
            if (eventSubSeq == null) {
                throw busyException;
            }
            updateFuture.of(operation, sqlExecutionContext, eventSubSeq, operation.getTableNamePosition());
            return updateFuture;
        }
    }
}

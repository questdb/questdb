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
import io.questdb.std.WeakSelfReturningObjectPool;
import org.jetbrains.annotations.Nullable;

public class OperationDispatcher<T extends AbstractOperation> {

    private final CairoEngine engine;
    private final DoneOperationFuture doneFuture = new DoneOperationFuture();
    private final WeakSelfReturningObjectPool<OperationFutureImpl> futurePool;
    private final CharSequence lockReason;

    public OperationDispatcher(CairoEngine engine, CharSequence lockReason) {
        this.engine = engine;
        futurePool = new WeakSelfReturningObjectPool<>(pool -> new OperationFutureImpl(engine, pool), 2);
        this.lockReason = lockReason;
    }

    public OperationFuture execute(
            T operation,
            SqlExecutionContext sqlExecutionContext,
            @Nullable SCSequence eventSubSeq
    ) throws SqlException {
        // storing execution context for UPDATE, DROP INDEX execution
        // writer thread will call `apply()` when thread is ready to do so
        // `apply()` will use context stored in the operation
        operation.withContext(sqlExecutionContext);
        try (
                TableWriter writer = engine.getWriter(
                        sqlExecutionContext.getCairoSecurityContext(),
                        operation.getTableName(),
                        lockReason
                )
        ) {
            return doneFuture.of(operation.apply(writer, true));
        } catch (EntryUnavailableException busyException) {
            if (eventSubSeq == null) {
                throw busyException;
            }
            return futurePool.pop().of(
                    operation,
                    sqlExecutionContext,
                    eventSubSeq,
                    operation.getTableNamePosition()
            );
        }
    }
}

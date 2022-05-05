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
import io.questdb.std.WeakAutoClosableObjectPool;
import org.jetbrains.annotations.Nullable;

public class AlterOperationDispatcher implements OperationDispatcher<AlterOperation> {
    private final CairoEngine engine;
    private final DoneOperationFuture doneFuture = new DoneOperationFuture();
    private final WeakAutoClosableObjectPool<OperationFutureImpl> futurePool;

    public AlterOperationDispatcher(CairoEngine engine) {
        this.engine = engine;
        futurePool = new WeakAutoClosableObjectPool<>(pool -> new OperationFutureImpl(engine, pool), 2);
    }

    @Override
    public OperationFuture execute(
            AlterOperation operation,
            SqlExecutionContext sqlExecutionContext,
            @Nullable SCSequence eventSubSeq
    ) throws SqlException {
        try (
                TableWriter writer = engine.getWriter(
                        sqlExecutionContext.getCairoSecurityContext(),
                        operation.getTableName(),
                        "Alter table execute"
                )
        ) {
            return doneFuture.of(operation.apply(writer, true));
        } catch (EntryUnavailableException busyException) {
            if (eventSubSeq == null) {
                throw busyException;
            }
            OperationFutureImpl alterFut = futurePool.pop();
            alterFut.of(operation, sqlExecutionContext, eventSubSeq, operation.getTableNamePosition());
            return alterFut;
        }
    }
}

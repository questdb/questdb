/*******************************************************************************
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
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SCSequence;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

/**
 * Operation represents immutable user request to execute a DDL against the database. It aims to
 * separate compilation and execution phases of SQL query processing. Compilation phase is
 * text parsing with parse results stored in the Operation object. Execution phase is a process
 * of applying the request to the current context. Operation can be executed multiple times.
 * <p>
 * When operation is no longer needed, it must be closed to release resources.
 */
public interface Operation extends QuietCloseable {

    /**
     * Executes the operation. Typically, to execute an operation a compiler
     * infrastructure is going to be required. Operation objects are kept lightweight and
     * rely on shared compilers to execute. To keep things simple, compiler instance is
     * retrieved from the execution context.
     * <p>
     * Execution will typically cast the operation to the type it is familiar with using the
     * operation code.
     *
     * @param sqlExecutionContext execution context, which encapsulates security context and cairo engine
     * @param eventSubSeq         sequence to notify of operation completion
     * @return future that represents the result of the operation
     * @throws SqlException   if execution fails validation, for example creating table that already exists
     * @throws CairoException if execution fails are runtime, OS error etc.
     */
    OperationFuture execute(
            SqlExecutionContext sqlExecutionContext,
            @Nullable SCSequence eventSubSeq
    ) throws SqlException, CairoException;

    /**
     * Code of the operation to chose code execution branch. One can think of this code as a lambda,
     * expect the lambda would be hardcoded somewhere in the code.
     *
     * @return one of the operation codes
     * @see io.questdb.cairo.OperationCodes
     */
    int getOperationCode();

    OperationFuture getOperationFuture();
}

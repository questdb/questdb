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

package io.questdb.cairo.sql;

import io.questdb.griffin.SqlException;

import java.io.Closeable;

public interface OperationFuture extends Closeable {
    int QUERY_COMPLETE = 2;
    int QUERY_NO_RESPONSE = 0;
    int QUERY_STARTED = 1;

    /**
     * Blocking wait for query completion. Returns immediately if query was executed synchronously.
     * Waits in busy waiting for cairo.writer.alter.busy.wait.timeout milliseconds and throws timeout.
     *
     * @throws SqlException when query execution times out or fails
     */
    void await() throws SqlException;

    /**
     * Waits for completion within specified timeout. Can be called multiple times on the same OperationFuture instance.
     *
     * @param timeout - millisecond timeout
     * @return - QUERY_NO_RESPONSE if no writer response received
     * - QUERY_STARTED if writer command ACK received
     * - QUERY_COMPLETE if writer completed response received
     * @throws SqlException when query execution fails
     */
    int await(long timeout) throws SqlException;

    /**
     * In case of async execution close must be called to remove sequence.
     */
    @Override
    void close();

    /**
     * Returns the number of rows affected by the command run asynchronously.
     *
     * @return - number of rows changed
     */
    long getAffectedRowsCount();

    /**
     * True if operation completed, false otherwise.
     *
     * @return - QUERY_NO_RESPONSE if no writer response received
     * - QUERY_STARTED if writer command ACK received
     * - QUERY_COMPLETE if writer completed response received
     */
    int getStatus();
}

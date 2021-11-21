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

package io.questdb.griffin;

import java.io.Closeable;

import static io.questdb.griffin.CompiledQuery.QUERY_COMPLETE;

public interface QueryFuture extends Closeable {
    /***
     * Blocking wait for query completion. Returns immediately if query has executed synchronously
     * Waits in busy waiting for cairo.writer.alter.busy.wait.timeout.micro microseconds and throws timeout
     * @throws SqlException when query execution times out or fails
     */
    void await() throws SqlException;

    /***
     * Waits for completion within specified timeout. Can be called multiple times on the same QueryFuture instance.
     * @param timeout - microseconds timeout
     * @return true if complete, false otherwise
     * @throws SqlException when query execution fails
     */
    int await(long timeout) throws SqlException;

    /***
     * True if operation completed, false otherwise
     * @return true when done, false if needs awaiting
     */
    int getStatus();

    /***
     * In case of async execution close must be called to remove sequence
     */
    @Override
    void close();

    QueryFuture DONE = new QueryFuture() {
        @Override
        public void await() {
        }

        @Override
        public int await(long timeout) {
            return QUERY_COMPLETE;
        }

        @Override
        public int getStatus() {
            return QUERY_COMPLETE;
        }

        @Override
        public void close() {
        }
    };
}

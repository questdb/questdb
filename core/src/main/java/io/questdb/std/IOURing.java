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

package io.questdb.std;

import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

public interface IOURing extends Closeable {

    @Override
    void close();

    @TestOnly
    long enqueueNop();

    long enqueueFsync(long fd);

    void enqueueFsync(long fd, long userData);

    long enqueueRead(long fd, long offset, long bufPtr, int len);

    long enqueueWrite(long fd, long offset, long bufPtr, int len);

    void enqueueWrite(long fd, long offset, long bufPtr, int len, long userData);

    long getCqeId();

    int getCqeRes();

    /**
     * Checks if a cqe is ready and, if so, reads its data. Read data is
     * then available via {@link #getCqeId} and {@link #getCqeRes} methods.
     *
     * @return true - if cqe was read; false - otherwise.
     */
    boolean nextCqe();

    /**
     * Submits pending sqes, if any.
     *
     * @return number of submitted sqes.
     */
    int submit();

    /**
     * Submits pending sqes, if any, and blocks until at least one operation result
     * becomes available as a cqe.
     *
     * @return number of submitted sqes.
     */
    int submitAndWait();
}

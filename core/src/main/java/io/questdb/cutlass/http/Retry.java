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

package io.questdb.cutlass.http;

import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.network.ServerDisconnectException;

import java.io.Closeable;

public interface Retry extends Closeable {
    /**
     * Notify client that re-run failed
     *
     * @param selector processor selector
     * @param e        exception information
     */
    void fail(HttpRequestProcessorSelector selector, HttpException e) throws PeerIsSlowToReadException, ServerDisconnectException;

    /**
     * Provides retry information
     *
     * @return retry attributes
     */
    RetryAttemptAttributes getAttemptDetails();

    /**
     * Retries context that could not acquire resource during regular execution.
     *
     * @param selector          processor selector
     * @param rescheduleContext context to be retried
     * @return success indicator
     */
    boolean tryRerun(HttpRequestProcessorSelector selector, RescheduleContext rescheduleContext) throws PeerIsSlowToReadException, PeerIsSlowToWriteException, ServerDisconnectException;
}

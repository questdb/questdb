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

package io.questdb.network;

import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

/**
 * Used for generic single-time events that can be used with
 * {@link IODispatcher}. Such events aren't necessarily network I/O
 * related, but instead provide an efficient way of suspending a task
 * done in the IODispatcher's event loop and resume it later when another
 * thread sends us a notification through the event.
 * <p>
 * To be more specific, we use eventfd(2) in Linux and pipes in OS X.
 */
public interface SuspendEvent extends Closeable {
    /**
     * @throws NetworkError if the event wasn't triggered.
     */
    @TestOnly
    void checkTriggered();

    /**
     * Event is assumed to be held and then closed by two parties.
     * The underlying OS resources are freed on the second close call.
     */
    @Override
    void close();

    /**
     * Returns fd to be used to listen/wait for the event.
     */
    int getFd();

    /**
     * Sends the event to the receiving side.
     */
    void trigger();
}

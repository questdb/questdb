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
public abstract class SuspendEvent implements Closeable {

    private long deadline = -1;

    /**
     * Returns true if the event was triggered.
     */
    public abstract boolean checkTriggered();

    /**
     * Event is assumed to be held and then closed by two parties.
     * The underlying OS resources are freed on the second close call.
     */
    @Override
    public abstract void close();

    /**
     * Returns a deadline (epoch millis) for the event or -1 if the deadline is not set.
     */
    public long getDeadline() {
        return deadline;
    }

    /**
     * Returns fd to be used to listen/wait for the event.
     */
    public abstract int getFd();

    /**
     * Returns true if the deadline was set to an older value than the given timestamp,
     * false - otherwise.
     */
    public boolean isDeadlineMet(long timestamp) {
        return deadline > 0 && deadline <= timestamp;
    }

    /**
     * Sets a deadline (epoch millis) for the event.
     */
    public void setDeadline(long deadline) {
        this.deadline = deadline;
    }

    /**
     * Sends the event to the receiving side.
     */
    public abstract void trigger();
}

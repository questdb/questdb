/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.std.Unsafe;
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
public abstract class SuspendEvent implements Closeable {

    private static final long REF_COUNT_OFFSET;

    private long deadline = Long.MAX_VALUE;

    // set by using Unsafe, see REF_COUNT_OFFSET, close().
    @SuppressWarnings({"FieldCanBeLocal", "FieldMayBeFinal"})
    private volatile int refCount = 2;

    public abstract void _close();

    /**
     * Returns true if the event was triggered.
     *
     * @return true if the event was triggered
     */
    public abstract boolean checkTriggered();

    /**
     * Event is assumed to be held and then closed by two parties.
     * The underlying OS resources are freed on the second close call.
     */
    @Override
    public void close() {
        final int prevRefCount = Unsafe.getUnsafe().getAndAddInt(this, REF_COUNT_OFFSET, -1);
        if (prevRefCount == 1) {
            _close();
        }
    }

    /**
     * Event deadline.
     *
     * @return a deadline (epoch millis) for the event or Long.MAX_VALUE if the deadline is not set
     */
    public long getDeadline() {
        return deadline;
    }

    /**
     * Returns fd to be used to listen/wait for the event.
     *
     * @return fd to be used to listen/wait for the event
     */
    public abstract long getFd();

    @TestOnly
    public boolean isClosedByAtLeastOneSide() {
        return refCount <= 1;
    }

    /**
     * Returns true if the deadline was set to an older value than the given timestamp,
     * false - otherwise.
     *
     * @param timestamp timestamp to compare against
     * @return true if the deadline was set to an older value than the given timestamp
     */
    public boolean isDeadlineMet(long timestamp) {
        return deadline > 0 && deadline <= timestamp;
    }

    /**
     * Sets a deadline (epoch millis) for the event.
     *
     * @param deadline deadline (epoch millis) for the event
     */
    public void setDeadline(long deadline) {
        this.deadline = deadline;
    }

    /**
     * Sends the event to the receiving side.
     */
    public abstract void trigger();

    static {
        REF_COUNT_OFFSET = Unsafe.getFieldOffset(SuspendEvent.class, "refCount");
    }
}

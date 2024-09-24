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

/**
 * Atomic flag-based suspend event object. Used on Windows. Then events are checked
 * by {@link IODispatcherWindows} in a loop, so O(n), but that fine with we already
 * use an O(n) method (select) to check socket statuses.
 */
public class AtomicSuspendEvent extends SuspendEvent {

    private volatile boolean flag;

    @Override
    public void _close() {
        // no-op
    }

    @Override
    public boolean checkTriggered() {
        return flag;
    }

    @Override
    public long getFd() {
        // no-op
        return -1;
    }

    @Override
    public void trigger() {
        flag = true;
    }
}

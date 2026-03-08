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

package io.questdb.mp;

import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

public abstract class SynchronizedJob implements Job {
    private static final long LOCKED_OFFSET = Unsafe.getFieldOffset(SynchronizedJob.class, "locked");

    @SuppressWarnings({"unused", "FieldCanBeLocal"})
    private volatile int locked = 0;

    @Override
    public boolean run(int workerId, @NotNull RunStatus runStatus) {
        if (Unsafe.getUnsafe().compareAndSwapInt(this, LOCKED_OFFSET, 0, 1)) {
            try {
                return runSerially();
            } finally {
                locked = 0;
            }
        }
        return false;
    }

    @Override
    public boolean run(int workerId) {
        return run(workerId, Job.RUNNING_STATUS);
    }

    protected abstract boolean runSerially();
}

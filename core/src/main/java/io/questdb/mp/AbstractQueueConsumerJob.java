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

import io.questdb.std.Os;
import org.jetbrains.annotations.NotNull;

public abstract class AbstractQueueConsumerJob<T> implements Job {
    protected final RingQueue<T> queue;
    protected final Sequence subSeq;

    public AbstractQueueConsumerJob(RingQueue<T> queue, Sequence subSeq) {
        this.queue = queue;
        this.subSeq = subSeq;
    }

    @Override
    public boolean run(int workerId, @NotNull RunStatus runStatus) {
        if (!canRun()) {
            return false;
        }
        while (true) {
            final long cursor = subSeq.next();
            if (cursor == -1) {
                return false;
            }
            if (cursor > -1) {
                return doRun(workerId, cursor, runStatus);
            }
            Os.pause();
        }
    }

    protected boolean canRun() {
        return true;
    }

    protected abstract boolean doRun(int workerId, long cursor, RunStatus runStatus);
}

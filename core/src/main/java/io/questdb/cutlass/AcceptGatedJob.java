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

package io.questdb.cutlass;

import io.questdb.mp.EagerThreadSetup;
import io.questdb.mp.Job;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wraps a network dispatcher (or its reschedule context) so the worker pool
 * polls it only once the accept gate is open. The HTTP, pg-wire and line-TCP
 * servers all share this wrapper instead of carrying private copies, so the
 * eager-setup forwarding below cannot drift between them.
 *
 * <p>The gate guards {@link #run} but never {@link #setup}: a worker pre-allocates
 * the connection-context pool at start so the server can accept connections
 * without allocating once the gate opens, even under memory pressure. Forwarding
 * {@code setup()} to the delegate unconditionally is what preserves that
 * pre-allocation.
 */
public class AcceptGatedJob implements Job, EagerThreadSetup {
    private final AtomicBoolean acceptOpen;
    private final Job delegate;

    public AcceptGatedJob(Job delegate, AtomicBoolean acceptOpen) {
        this.delegate = delegate;
        this.acceptOpen = acceptOpen;
    }

    @Override
    public boolean run(@NotNull WorkerContext workerContext) {
        if (!acceptOpen.get()) {
            return false;
        }
        return delegate.run(workerContext);
    }

    @Override
    public void setup() {
        if (delegate instanceof EagerThreadSetup) {
            ((EagerThreadSetup) delegate).setup();
        }
    }
}

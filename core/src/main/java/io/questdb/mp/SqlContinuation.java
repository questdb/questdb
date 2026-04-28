/*+*****************************************************************************
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

import jdk.internal.vm.Continuation;
import jdk.internal.vm.ContinuationScope;

/**
 * Thin wrapper over {@link jdk.internal.vm.Continuation} that hides the JDK-internal API
 * from the rest of the codebase. A SqlContinuation represents a reified SQL call stack
 * that can be unmounted from one thread (via {@link #suspend()}) and remounted on a
 * different thread (via a subsequent {@link #run()} call from that thread).
 *
 * <p>Requires {@code --add-opens java.base/jdk.internal.vm=ALL-UNNAMED} on the JVM
 * command line.
 */
public final class SqlContinuation {
    public static final ContinuationScope SCOPE = new ContinuationScope("questdb-sql");
    private final Continuation cont;
    private volatile boolean shutdown;

    public SqlContinuation(Runnable body) {
        this.cont = new Continuation(SCOPE, body);
    }

    /**
     * True if there is a mounted SqlContinuation on the calling thread's stack.
     * A call to {@link #suspend()} is only legal when this returns true.
     */
    public static boolean isMounted() {
        return Continuation.getCurrentContinuation(SCOPE) != null;
    }

    /**
     * Unmounts the current call stack off the carrier thread, returning control to
     * whoever called {@link #run()}. Must only be called from a frame that was reached
     * through {@code run()} of a SqlContinuation.
     *
     * <p>Returns {@code true} if the carrier was unmounted (the body will only continue
     * on a future {@link #run()} call). Returns {@code false} if the JDK refused to
     * yield because the carrier is pinned, e.g., a {@code synchronized} block or a
     * native frame sits above this call on the stack. When {@code false} is returned,
     * the body keeps running on the same carrier; callers must not assume they have
     * been parked.
     */
    public static boolean suspend() {
        return Continuation.yield(SCOPE);
    }

    public boolean isDone() {
        return cont.isDone();
    }

    /**
     * Set when the owning context is closing. Suspending functions that loop on
     * suspend/wake (e.g. {@code wait_wal_table}) must check this and exit instead of
     * re-parking, so that {@link #run()} can drive the body all the way to completion.
     */
    public boolean isShutdown() {
        return shutdown;
    }

    /**
     * Starts the continuation body or resumes it on the calling thread. Returns when
     * the body either completes (then {@link #isDone()} is true) or calls
     * {@link #suspend()} (then {@link #isDone()} is false and the frames are parked
     * inside this object, ready to be remounted by a future {@code run()} call).
     */
    public void run() {
        cont.run();
    }

    /**
     * Marks this continuation as shutting down. Suspending functions that consult
     * {@link #isShutdown()} between suspends must terminate their wake loop. Combined
     * with a bounded {@link #run()} drive in the owning context's close path, this
     * guarantees the body unwinds and the parked native stack is released.
     */
    public void shutdown() {
        this.shutdown = true;
    }
}

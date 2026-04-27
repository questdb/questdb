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
     */
    public static void suspend() {
        Continuation.yield(SCOPE);
    }

    public boolean isDone() {
        return cont.isDone();
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
}

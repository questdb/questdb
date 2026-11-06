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

package io.questdb.mp.continuation;

/**
 * Opaque policy context captured by a controlled Fiber dispatch. The OSS runtime preserves
 * identity only; interpretation belongs to the installed {@link FiberDispatchController}.
 */
public interface FiberDispatchContext {
    /**
     * Returns the dispatch context inherited by query-parallel child work. Controllers that do
     * not distinguish coordinator work from its parallel children retain the current context.
     */
    default FiberDispatchContext getParallelDispatchContext() {
        return this;
    }

    /**
     * Returns how many published page frames of one query may be unfinished at once. A reducer
     * that claimed a frame and waits for CPU holds up later frames of every query on that ordered
     * queue, so a context with few CPU grants keeps few frames out.
     */
    default int getParallelFrameLimit() {
        return Integer.MAX_VALUE;
    }

    /**
     * Returns the query-registry owner represented by this context, or {@code -1} when the
     * context does not represent a SQL execution. QueryRegistry uses this opaque link to fold
     * execution-phase registrations into a protocol owner that was admitted before compilation.
     */
    default long getQueryRegistryOwnerId() {
        return -1;
    }

    /**
     * Returns whether query-parallel child work shares a limited number of CPU grants with its
     * owner. An owner that cannot park cannot give its grant up, so it does such work itself.
     */
    default boolean isParallelWorkLimited() {
        return false;
    }

    /**
     * Whether parallel work must run inline to keep waits for CPU off shared task queues.
     */
    default boolean isParallelWorkLocal() {
        return false;
    }
}

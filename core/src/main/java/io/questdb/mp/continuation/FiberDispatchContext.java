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
     * Returns an incarnation token captured before a dispatch ticket mounts. The runtime passes it
     * back to {@link #onDispatchContextReleased(long)} so pooled contexts can reject delayed
     * notifications from an earlier incarnation. Implementations must not throw.
     */
    default long getDispatchReleaseToken() {
        return 0;
    }

    /**
     * Returns the dispatch context inherited by query-parallel child work. Controllers that do
     * not distinguish coordinator work from its parallel children retain the current context.
     */
    default FiberDispatchContext getParallelDispatchContext() {
        return this;
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
     * Invoked after the dispatch ticket was settled, its request was completed, and the Fiber no
     * longer retains this context. Failures are contained by FiberRuntime and must remain local to
     * the context-owned state.
     */
    default void onDispatchContextReleased(long releaseToken) {
    }
}

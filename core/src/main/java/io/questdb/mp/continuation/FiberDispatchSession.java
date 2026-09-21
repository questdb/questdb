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

import org.jetbrains.annotations.Nullable;

/**
 * Per-runtime dispatch session. Implementations may grant a request synchronously or retain its
 * object and epoch for a later grant. Once quiesce begins, retained requests must be drained and
 * later shutdown-cleanup requests must continue to be granted without policy delay.
 */
public interface FiberDispatchSession extends FiberRuntimeQuiesceListener {
    /**
     * Completes the ticket after its carrier-local {@link FiberDispatchTicket#onUnmount} callback.
     * When redispatching, the request already describes the next epoch and the Fiber is safe to
     * publish. Implementations can settle the old ticket, enqueue the next request and select
     * grants in one transition. Otherwise this call only settles the old ticket, including when
     * driver failure prevented redispatch. The ticket and completedEpoch identify the old dispatch,
     * even when the request has advanced. Completion must not resample carrier CPU time.
     * <p>
     * The runtime invokes this once even if onUnmount threw. Implementations must settle captured
     * accounting before returning or throwing, including when requesting the next dispatch fails.
     */
    default void completeDispatch(
            FiberDispatchRequest request,
            FiberDispatchTicket ticket,
            long completedEpoch,
            boolean wasMounted,
            boolean isRedispatch
    ) {
        if (isRedispatch) {
            requestDispatch(request);
        }
    }

    default @Nullable FiberDispatchRequestState createRequestState() {
        return null;
    }

    void requestDispatch(FiberDispatchRequest request);

    /**
     * Returns a ticket only when the staged Fiber may mount on the calling carrier immediately.
     * A null result converts the request to {@link FiberDispatchRoute#DIRECT_PENDING} and submits
     * it through {@link #requestDispatch(FiberDispatchRequest)}.
     */
    @Nullable
    FiberDispatchTicket tryDispatchDirect(FiberDispatchRequest request);
}

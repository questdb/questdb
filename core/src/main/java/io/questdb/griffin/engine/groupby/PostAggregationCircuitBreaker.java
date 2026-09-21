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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.async.AsyncQueryErrorKind;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.NumericException;
import io.questdb.std.str.StringSink;

/**
 * Cancellation channel between the owner thread and the detached workers of a GROUP BY
 * post-aggregation phase - the parallel shard merge and the parallel long top K. Carries the
 * first error a worker raised alongside the flag, so the owner can rethrow it verbatim. Mirrors
 * {@link io.questdb.cairo.sql.async.UnorderedPageFrameSequence#setError(Throwable)} for the
 * reduce phase.
 */
public class PostAggregationCircuitBreaker extends AtomicBooleanCircuitBreaker {
    private final StringSink errorMsg = new StringSink();
    private int errno = CairoException.NON_CRITICAL;
    private byte errorKind = AsyncQueryErrorKind.KIND_NONE;
    private int errorMessagePosition;
    private boolean isErrorCancellation;
    private boolean isErrorInterruption;
    private boolean isErrorOutOfMemory;

    public PostAggregationCircuitBreaker(CairoEngine engine) {
        super(engine);
    }

    /**
     * Rebuilds the captured error. Call only when {@link #hasError()} returns true.
     */
    public synchronized RuntimeException buildError() {
        return switch (errorKind) {
            case AsyncQueryErrorKind.KIND_IMPLICIT_CAST ->
                    ImplicitCastException.instance().position(errorMessagePosition).put(errorMsg);
            case AsyncQueryErrorKind.KIND_NUMERIC ->
                    NumericException.instance().position(errorMessagePosition).put(errorMsg);
            default -> CairoException.critical(errno)
                    .position(errorMessagePosition)
                    .put(errorMsg)
                    .setCancellation(isErrorCancellation)
                    .setInterruption(isErrorInterruption)
                    .setOutOfMemory(isErrorOutOfMemory);
        };
    }

    /**
     * Records the error and trips the breaker. The first error wins. An error raised once the
     * breaker is already down is collateral - {@code PerWorkerLocks.acquireSlot} throws "query
     * aborted" purely because it observed the trip - and would mask the real reason, so it is
     * dropped. Thread-safe.
     */
    public synchronized void cancel(Throwable th) {
        if (errorMsg.isEmpty() && !checkIfTripped()) {
            errorKind = AsyncQueryErrorKind.of(th);
            if (th instanceof CairoException e) {
                errorMsg.put(e.getFlyweightMessage());
                errorMessagePosition = e.getPosition();
                errno = e.getErrno();
                isErrorCancellation = e.isCancellation();
                isErrorInterruption = e.isInterruption();
                isErrorOutOfMemory = e.isOutOfMemory();
            } else if (th instanceof FlyweightMessageContainer fmc) {
                errorMsg.put(fmc.getFlyweightMessage());
                errorMessagePosition = fmc.getPosition();
            } else {
                errorMsg.put("unexpected post-aggregation error");
                if (th.getMessage() != null) {
                    errorMsg.put(": ").put(th.getMessage());
                }
            }
        }
        cancel();
    }

    public synchronized boolean hasError() {
        return !errorMsg.isEmpty();
    }

    @Override
    public synchronized void reset() {
        super.reset();
        errorMsg.clear();
        errorKind = AsyncQueryErrorKind.KIND_NONE;
        errorMessagePosition = 0;
        errno = CairoException.NON_CRITICAL;
        isErrorCancellation = false;
        isErrorInterruption = false;
        isErrorOutOfMemory = false;
    }
}

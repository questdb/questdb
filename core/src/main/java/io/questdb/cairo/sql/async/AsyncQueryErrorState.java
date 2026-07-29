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

package io.questdb.cairo.sql.async;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.NumericException;
import io.questdb.std.str.StringSink;

public final class AsyncQueryErrorState {
    private int errno = CairoException.NON_CRITICAL;
    private byte errorKind = AsyncQueryErrorKind.KIND_NONE;
    private final StringSink errorMessage = new StringSink();
    private int errorMessagePosition;
    private volatile boolean hasError;
    private boolean isCancelled;
    private boolean isInterrupted;
    private boolean isOutOfMemory;

    public synchronized RuntimeException buildException() {
        if (!hasError) {
            throw new IllegalStateException("async query error is not set");
        }
        return switch (errorKind) {
            case AsyncQueryErrorKind.KIND_IMPLICIT_CAST ->
                    ImplicitCastException.instance().position(errorMessagePosition).put(errorMessage);
            case AsyncQueryErrorKind.KIND_NUMERIC ->
                    NumericException.instance().position(errorMessagePosition).put(errorMessage);
            default -> CairoException.critical(errno)
                    .position(errorMessagePosition)
                    .put(errorMessage)
                    .setCancellation(isCancelled)
                    .setInterruption(isInterrupted)
                    .setOutOfMemory(isOutOfMemory);
        };
    }

    public synchronized void clear() {
        errno = CairoException.NON_CRITICAL;
        errorKind = AsyncQueryErrorKind.KIND_NONE;
        errorMessage.clear();
        errorMessagePosition = 0;
        isCancelled = false;
        isInterrupted = false;
        isOutOfMemory = false;
        hasError = false;
    }

    public boolean hasError() {
        return hasError;
    }

    public synchronized void setError(Throwable th) {
        if (hasError) {
            return;
        }

        errorKind = AsyncQueryErrorKind.of(th);
        if (th instanceof CairoException e) {
            errno = e.getErrno();
            errorMessage.put(e.getFlyweightMessage());
            errorMessagePosition = e.getPosition();
            isCancelled = e.isCancellation();
            isInterrupted = e.isInterruption();
            isOutOfMemory = e.isOutOfMemory();
        } else if (th instanceof FlyweightMessageContainer e) {
            errorMessage.put(e.getFlyweightMessage());
            errorMessagePosition = e.getPosition();
        } else {
            errorMessage.put("unexpected async query error");
            final String message = th.getMessage();
            if (message != null) {
                errorMessage.put(": ").put(message);
            }
        }
        hasError = true;
    }
}

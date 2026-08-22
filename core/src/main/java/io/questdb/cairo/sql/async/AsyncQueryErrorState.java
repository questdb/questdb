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
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.network.NetworkError;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.NumericException;
import io.questdb.std.str.StringSink;

public final class AsyncQueryErrorState {
    private static final String DEFAULT_UNEXPECTED_ERROR_MESSAGE = "unexpected async query error";
    private final StringSink errorMessage = new StringSink();
    private final String unexpectedErrorMessage;
    private int errno = CairoException.NON_CRITICAL;
    private byte errorKind = AsyncQueryErrorKind.KIND_NONE;
    private int errorMessagePosition;
    private volatile boolean hasError;
    private int interruptionReason = SqlExecutionCircuitBreaker.STATE_OK;
    private boolean isOutOfMemory;
    private Throwable retainedError;

    public AsyncQueryErrorState() {
        this(DEFAULT_UNEXPECTED_ERROR_MESSAGE);
    }

    AsyncQueryErrorState(String unexpectedErrorMessage) {
        this.unexpectedErrorMessage = unexpectedErrorMessage;
    }

    public synchronized RuntimeException buildException() {
        if (!hasError) {
            throw new IllegalStateException("async query error is not set");
        }
        if (retainedError instanceof TableReferenceOutOfDateException exception) {
            return exception;
        }
        return switch (errorKind) {
            case AsyncQueryErrorKind.KIND_IMPLICIT_CAST ->
                    ImplicitCastException.instance().position(errorMessagePosition).put(errorMessage);
            case AsyncQueryErrorKind.KIND_NUMERIC ->
                    NumericException.instance().position(errorMessagePosition).put(errorMessage);
            default -> CairoException.critical(errno)
                    .position(errorMessagePosition)
                    .put(errorMessage)
                    .setInterruptionReason(interruptionReason)
                    .setOutOfMemory(isOutOfMemory);
        };
    }

    public synchronized void clear() {
        errno = CairoException.NON_CRITICAL;
        errorKind = AsyncQueryErrorKind.KIND_NONE;
        errorMessage.clear();
        errorMessagePosition = 0;
        interruptionReason = SqlExecutionCircuitBreaker.STATE_OK;
        isOutOfMemory = false;
        retainedError = null;
        hasError = false;
    }

    public boolean hasError() {
        return hasError;
    }

    public synchronized boolean setError(Throwable th) {
        if (hasError) {
            return false;
        }

        errorKind = AsyncQueryErrorKind.of(th);
        if (th instanceof CairoException e) {
            errno = e.getErrno();
            errorMessage.put(e.getFlyweightMessage());
            errorMessagePosition = e.getPosition();
            interruptionReason = e.getInterruptionReason();
            isOutOfMemory = e.isOutOfMemory();
        } else if (th instanceof TableReferenceOutOfDateException e) {
            retainedError = e;
            copyFlyweightMessage(e);
        } else if (th instanceof NetworkError e) {
            retainedError = e.detachedCopy();
            copyFlyweightMessage(e);
        } else if (th instanceof Error e) {
            retainedError = e;
            if (e instanceof FlyweightMessageContainer flyweight) {
                copyFlyweightMessage(flyweight);
            } else {
                copyUnexpectedMessage(e);
            }
        } else if (th instanceof FlyweightMessageContainer e) {
            copyFlyweightMessage(e);
        } else {
            // Neither a flyweight nor thread-confined, so the instance itself survives the hand-off
            // and throwError() can preserve its type.
            retainedError = th;
            copyUnexpectedMessage(th);
        }
        hasError = true;
        return true;
    }

    /**
     * Throws the recorded failure. An {@link Error}, a non-flyweight {@link RuntimeException}, or a
     * per-instance {@link TableReferenceOutOfDateException} is rethrown as the original instance.
     * Flyweight Cairo failures are rebuilt on the calling thread.
     */
    public void throwError() {
        final Throwable retained;
        synchronized (this) {
            if (!hasError) {
                throw new IllegalStateException("async query error is not set");
            }
            retained = retainedError;
        }
        if (retained instanceof Error error) {
            throw error;
        }
        if (retained instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        throw buildException();
    }

    private void copyFlyweightMessage(FlyweightMessageContainer error) {
        errorMessage.put(error.getFlyweightMessage());
        errorMessagePosition = error.getPosition();
    }

    private void copyUnexpectedMessage(Throwable error) {
        errorMessage.put(unexpectedErrorMessage);
        final String message = error.getMessage();
        if (message != null) {
            errorMessage.put(": ").put(message);
        }
    }
}

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

package io.questdb.cutlass.pgwire;

import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class PGMessageProcessingException extends Exception implements FlyweightMessageContainer {
    public static final PGMessageProcessingException INSTANCE = new PGMessageProcessingException();

    private static final StackTraceElement[] EMPTY_STACK_TRACE = {};
    private static final io.questdb.std.ThreadLocal<PGMessageProcessingException> tlException = new ThreadLocal<>(PGMessageProcessingException::new);
    private StringSink message;
    private PGPipelineEntry pe;

    public static PGMessageProcessingException instance(@NotNull PGPipelineEntry pe) {
        PGMessageProcessingException ex = tlException.get();
        // This is to have correct stack trace in local debugging with -ea option
        assert (ex = new PGMessageProcessingException()) != null;
        ex.message = pe.getErrorMessageSink();
        ex.pe = pe;
        return ex;
    }

    @Override
    public CharSequence getFlyweightMessage() {
        return message;
    }

    @Override
    public StackTraceElement[] getStackTrace() {
        StackTraceElement[] result = EMPTY_STACK_TRACE;
        // This is to have correct stack trace reported in CI
        assert (result = super.getStackTrace()) != null;
        return result;
    }

    public PGMessageProcessingException put(Throwable e) {
        if (e instanceof FlyweightMessageContainer) {
            message.put(((FlyweightMessageContainer) e).getFlyweightMessage());
            pe.setErrorMessagePosition(((FlyweightMessageContainer) e).getPosition());
        } else {
            message.put(e.getMessage());
        }
        return this;
    }

    public PGMessageProcessingException put(long value) {
        message.put(value);
        return this;
    }

    public PGMessageProcessingException put(double value) {
        message.put(value);
        return this;
    }

    public PGMessageProcessingException put(@Nullable CharSequence cs) {
        message.put(cs);
        return this;
    }

    public PGMessageProcessingException put(@Nullable Utf8Sequence us) {
        message.put(us);
        return this;
    }

    public PGMessageProcessingException put(Sinkable sinkable) {
        sinkable.toSink(message);
        return this;
    }

    public PGMessageProcessingException put(char c) {
        message.put(c);
        return this;
    }

    public PGMessageProcessingException put(boolean value) {
        message.put(value);
        return this;
    }
}

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

package io.questdb.cutlass.qwp.server.egress;

import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;

/**
 * Thrown when no row prefix fits the send buffer (a single row's wire
 * encoding exceeds the buffer, or the table-block header alone does).
 * Mapped to {@code STATUS_LIMIT_EXCEEDED}.
 * <p>
 * Thread-local flyweight, same convention as {@link io.questdb.cutlass.http.HttpException}.
 */
public final class QwpRowExceedsBufferException extends RuntimeException
        implements Sinkable, FlyweightMessageContainer {

    private static final ThreadLocal<QwpRowExceedsBufferException> tlException =
            ThreadLocal.withInitial(QwpRowExceedsBufferException::new);
    private final StringSink message = new StringSink();

    private QwpRowExceedsBufferException() {
    }

    public static QwpRowExceedsBufferException instance(int columnCount, int sendBufferSize, int rowsBuffered) {
        QwpRowExceedsBufferException ex = tlException.get();
        ex.message.clear();
        ex.message.put("egress: row prefix does not fit send buffer [colCount=")
                .put(columnCount).put(", bufferSize=").put(sendBufferSize)
                .put(", rowsBuffered=").put(rowsBuffered).put(']');
        return ex;
    }

    @Override
    public CharSequence getFlyweightMessage() {
        return message;
    }

    @Override
    public String getMessage() {
        return message.toString();
    }

    @Override
    public void toSink(CharSink<?> sink) {
        sink.put(message);
    }
}

/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cutlass.http.processors;

import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpMultipartContentListener;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.QueryPausedException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.str.Utf16Sink;

public interface RejectProcessor extends HttpRequestProcessor, HttpMultipartContentListener {

    void clear();

    boolean isRequestBeingRejected();

    default void onChunk(long lo, long hi) {
    }

    default void onPartBegin(HttpRequestHeader partHeader) {
    }

    default void onPartEnd() {
    }

    RejectProcessor reject(int rejectCode);

    RejectProcessor reject(int rejectCode, CharSequence rejectMessage);

    Utf16Sink getMessageSink();

    RejectProcessor withAuthenticationType(byte authenticationType);

    RejectProcessor withCookie(CharSequence cookieName, CharSequence cookieValue);

    RejectProcessor withShutdownWrite();

    default void resumeSend(HttpConnectionContext context)
            throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException {
        onRequestComplete(context);
    }
}

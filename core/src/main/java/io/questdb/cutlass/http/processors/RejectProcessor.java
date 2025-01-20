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

import io.questdb.cutlass.http.HttpMultipartContentListener;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.std.str.StringSink;

public interface RejectProcessor extends HttpRequestProcessor, HttpMultipartContentListener {

    void clear();

    boolean isRequestBeingRejected();

    RejectMessageBuilder newRejectMessageBuilder();

    default void onChunk(long lo, long hi) {
    }

    default void onPartBegin(HttpRequestHeader partHeader) {
    }

    default void onPartEnd() {
    }

    RejectProcessor reject(int rejectCode);

    RejectProcessor reject(int rejectCode, CharSequence rejectMessage);

    RejectProcessor withAuthenticationType(byte authenticationType);

    RejectProcessor withCookie(CharSequence cookieName, CharSequence cookieValue);

    RejectProcessor withShutdownWrite();

    class RejectMessageBuilder {
        private final StringSink rejectMessage = new StringSink();

        public RejectMessageBuilder $(CharSequence message) {
            rejectMessage.put(message);
            return this;
        }

        public RejectMessageBuilder $(long l) {
            rejectMessage.put(l);
            return this;
        }

        public RejectMessageBuilder $(char c) {
            rejectMessage.put(c);
            return this;
        }

        public CharSequence $() {
            return rejectMessage.toString();
        }

        public CharSequence I$() {
            return $(']').$();
        }
    }
}

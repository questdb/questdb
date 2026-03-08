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

package io.questdb.cutlass.http.client;

import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

public class HttpClientException extends RuntimeException {

    private final StringSink message = new StringSink();
    private int errno = Integer.MIN_VALUE;

    public HttpClientException(String message) {
        this.message.put(message);
    }

    public HttpClientException errno(int errno) {
        this.errno = errno;
        return this;
    }

    @Override
    public String getMessage() {
        if (errno == Integer.MIN_VALUE) {
            return message.toString();
        }
        String errNoRender = "[" + errno + "]";
        if (message.length() == 0) {
            return errNoRender;
        }
        return errNoRender + " " + message;
    }

    public HttpClientException put(char value) {
        message.put(value);
        return this;
    }

    public HttpClientException put(int value) {
        message.put(value);
        return this;
    }

    public HttpClientException put(long value) {
        message.put(value);
        return this;
    }

    public HttpClientException put(@Nullable CharSequence cs) {
        message.put(cs);
        return this;
    }

    public HttpClientException putSize(long value) {
        message.putSize(value);
        return this;
    }
}

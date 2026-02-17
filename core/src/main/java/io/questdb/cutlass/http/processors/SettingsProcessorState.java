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

package io.questdb.cutlass.http.processors;

import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.Mutable;
import io.questdb.std.str.DirectUtf8Sink;

import java.io.Closeable;

class SettingsProcessorState implements Mutable, Closeable {
    private final DirectUtf8Sink utf8Sink;
    private int statusCode = -1;

    SettingsProcessorState(int size) {
        utf8Sink = new DirectUtf8Sink(size);
    }

    @Override
    public void clear() {
        statusCode = -1;
        utf8Sink.clear();
    }

    @Override
    public void close() {
        utf8Sink.close();
    }

    DirectUtf8Sink getUtf8Sink() {
        return utf8Sink;
    }

    void send(HttpConnectionContext context) throws PeerIsSlowToReadException, PeerDisconnectedException {
        assert statusCode > 0;
        if (utf8Sink.size() > 0) {
            context.simpleResponse().sendStatusJsonContent(statusCode, utf8Sink);
        } else {
            context.simpleResponse().sendStatusJsonContent(statusCode);
        }
    }

    void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }
}

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

package io.questdb.cutlass.http.processors;

import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.pt.PayloadTransformDefinition;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Mutable;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;

import java.io.Closeable;

class IngestProcessorState implements Mutable, Closeable {
    private final DirectUtf8Sink bodySink;
    private final BytecodeAssembler bytecodeAssembler = new BytecodeAssembler();
    private final ListColumnFilter columnFilter = new ListColumnFilter();
    private final Utf8StringSink dlqSink = new Utf8StringSink();
    private final LowerCaseCharSequenceObjHashMap<CharSequence> overrides = new LowerCaseCharSequenceObjHashMap<>();
    private final StringSink payloadSink = new StringSink();
    private final DirectUtf8Sink responseSink;
    private final PayloadTransformDefinition transformDef = new PayloadTransformDefinition();
    private int statusCode = -1;

    IngestProcessorState(int initialBufferSize) {
        bodySink = new DirectUtf8Sink(initialBufferSize);
        responseSink = new DirectUtf8Sink(256);
    }

    @Override
    public void clear() {
        statusCode = -1;
        bodySink.clear();
        overrides.clear();
        payloadSink.clear();
        responseSink.clear();
    }

    @Override
    public void close() {
        bodySink.close();
        responseSink.close();
    }

    DirectUtf8Sink getBodySink() {
        return bodySink;
    }

    BytecodeAssembler getBytecodeAssembler() {
        return bytecodeAssembler;
    }

    ListColumnFilter getColumnFilter() {
        return columnFilter;
    }

    Utf8StringSink getDlqSink() {
        return dlqSink;
    }

    LowerCaseCharSequenceObjHashMap<CharSequence> getOverrides() {
        return overrides;
    }

    StringSink getPayloadSink() {
        return payloadSink;
    }

    DirectUtf8Sink getResponseSink() {
        return responseSink;
    }

    PayloadTransformDefinition getTransformDef() {
        return transformDef;
    }

    void send(HttpConnectionContext context) throws PeerIsSlowToReadException, PeerDisconnectedException {
        assert statusCode > 0;
        if (responseSink.size() > 0) {
            context.simpleResponse().sendStatusJsonContent(statusCode, responseSink);
        } else {
            context.simpleResponse().sendStatusJsonContent(statusCode);
        }
    }

    void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }
}

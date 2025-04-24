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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpException;
import io.questdb.cutlass.http.HttpMultipartContentListener;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.Misc;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;

import java.io.Closeable;

import static io.questdb.cutlass.http.HttpConstants.CONTENT_TYPE_JSON;

public class ConfigProcessor implements HttpRequestProcessor, HttpMultipartContentListener, Closeable {
    static final int MESSAGE_UNKNOWN = 3;
    static final int RESPONSE_JSON = 1;
    private static final String CONFIG_FILE_NAME = "_config~1";
    private static final int CONFIG_FORMAT_MSG_TYPE = 0;
    private static final Log LOG = LogFactory.getLog(ConfigProcessor.class);
    // Local value has to be static because each thread will have its own instance of
    // processor. For different threads to lookup the same value from local value map the key,
    // which is LV, has to be the same between processor instances
    private static final LocalValue<ConfigProcessorState> LV = new LocalValue<>();
    private static final int MESSAGE_CONFIG = 1;
    private static final int RESPONSE_COMPLETE = 6;
    private static final int RESPONSE_DONE = 5;
    private static final int RESPONSE_ERROR = 4;
    private final BlockFileWriter blockFileWriter;
    private final Path path = new Path();
    private final byte requiredAuthType;
    private final int rootLen;
    private HttpConnectionContext transientContext;
    private ConfigProcessorState transientState;

    public ConfigProcessor(CairoEngine engine, JsonQueryProcessorConfiguration configuration) {
        // TODO: move these into the state object !!!
        final CairoConfiguration config = engine.getConfiguration();
        path.of(config.getDbRoot());
        rootLen = path.size();
        blockFileWriter = new BlockFileWriter(config.getFilesFacade(), config.getCommitMode());

        requiredAuthType = configuration.getRequiredAuthType();
    }

    @Override
    public void close() {
        // TODO: move these into the state object !!!
        Misc.free(blockFileWriter);
        Misc.free(path);
    }

    @Override
    public void failRequest(HttpConnectionContext context, HttpException e) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        sendErr(e.getFlyweightMessage());
    }

    @Override
    public byte getRequiredAuthType() {
        return requiredAuthType;
    }

    @Override
    public void onChunk(long lo, long hi) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        if (hi > lo) {
            try {
                transientState.lo = lo;
                transientState.hi = hi;
                transientState.sink.putNonAscii(lo, hi);
            } catch (CairoException | CairoError e) {
                sendErr(e.getFlyweightMessage());
            }
        }
    }

    @Override
    public void onPartBegin(HttpRequestHeader partHeader) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        final DirectUtf8Sequence contentDisposition = partHeader.getContentDispositionName();
        LOG.debug().$("part begin [name=").$(contentDisposition).$(']').$();
        // TODO: accept a URL param called 'incremental'
        //  if it is present the config json is partial update, and not full replace of the config
        if (Utf8s.equalsNcAscii("config", contentDisposition)) {
            transientState.messagePart = MESSAGE_CONFIG;
        } else {
            if (partHeader.getContentDisposition() == null) {
                sendErr("'Content-Disposition' multipart header missing'");
            } else {
                sendErr("invalid value in 'Content-Disposition' multipart header");
            }
        }
    }

    @Override
    public void onPartEnd() throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        try {
            LOG.debug().$("part end").$();
            if (transientState.messagePart == MESSAGE_CONFIG) {
                try (BlockFileWriter writer = blockFileWriter) {
                    writer.of(path.trimTo(rootLen).concat(CONFIG_FILE_NAME).$());
                    final AppendableBlock block = writer.append();
                    // TODO: create a config json parser
                    block.putStr(transientState.sink.toString());
                    block.commit(CONFIG_FORMAT_MSG_TYPE);
                    writer.commit();
                }
                sendResponse(transientContext);
            }
        } catch (CairoException | CairoError e) {
            sendErr(e.getFlyweightMessage());
        }
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) {
        if (transientState != null) {
            transientState.clear();
        }
    }

    @Override
    public void onRequestRetry(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        this.transientContext = context;
        this.transientState = LV.get(context);
        onChunk(transientState.lo, transientState.hi);
    }

    @Override
    public void resumeRecv(HttpConnectionContext context) {
        this.transientContext = context;
        this.transientState = LV.get(context);
        if (transientState == null) {
            LOG.debug().$("new config state").$();
            LV.set(context, this.transientState = new ConfigProcessorState());
        }
    }

    @Override
    public void resumeSend(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        context.resumeResponseSend();
        doResumeSend(LV.get(context), context.getChunkedResponse());
    }

    private static void resumeJson(ConfigProcessorState state, HttpChunkedResponse response) throws PeerDisconnectedException, PeerIsSlowToReadException {
        switch (state.responseState) {
            case RESPONSE_JSON:
                response.putAscii('{')
                        .putAsciiQuoted("status").putAscii(':').putAsciiQuoted("OK")
                        .putAscii('}');
                state.responseState = RESPONSE_COMPLETE;
                response.sendChunk(true);
                break;
            case RESPONSE_DONE:
                state.responseState = RESPONSE_COMPLETE;
                response.done();
                break;
            default:
                break;
        }
    }

    private void doResumeSend(ConfigProcessorState state, HttpChunkedResponse socket) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        try {
            if (state.errorMessage != null) {
                resumeError(state, socket);
            } else {
                resumeJson(state, socket);
            }
        } catch (NoSpaceLeftInResponseBufferException ignored) {
            if (socket.resetToBookmark()) {
                socket.sendChunk(false);
            } else {
                // what we have here is out unit of data, column value or query
                // is larger that response content buffer
                // all we can do in this scenario is to log appropriately
                // and disconnect socket
                socket.shutdownWrite();
                throw ServerDisconnectException.INSTANCE;
            }
        }

        state.clear();
    }

    private void resumeError(ConfigProcessorState state, HttpChunkedResponse response) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        if (state.responseState == RESPONSE_ERROR) {
            response.bookmark();
            response.putAscii('{').putAsciiQuoted("status").putAscii(':').putQuoted(state.errorMessage).putAscii('}');
            state.responseState = RESPONSE_DONE;
            response.sendChunk(true);
        }
        response.shutdownWrite();
        throw ServerDisconnectException.INSTANCE;
    }

    private void sendErr(HttpConnectionContext context, CharSequence message, HttpChunkedResponse response) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        final ConfigProcessorState state = LV.get(context);
        state.responseState = RESPONSE_ERROR;
        state.errorMessage = message;
        response.status(200, CONTENT_TYPE_JSON);
        response.sendHeader();
        response.sendChunk(false);
        resumeError(state, response);
    }

    private void sendErr(CharSequence message) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        sendErr(transientContext, message, transientContext.getChunkedResponse());
    }

    private void sendResponse(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        final ConfigProcessorState state = LV.get(context);
        final HttpChunkedResponse response = context.getChunkedResponse();
        if (state.errorMessage == null) {
            response.status(200, CONTENT_TYPE_JSON);
            response.sendHeader();
            doResumeSend(state, response);
        } else {
            sendErr(context, state.errorMessage, context.getChunkedResponse());
        }
    }
}

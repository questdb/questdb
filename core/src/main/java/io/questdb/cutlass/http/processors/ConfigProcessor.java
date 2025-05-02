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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.config.ConfigStore;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpContentListener;
import io.questdb.cutlass.http.HttpException;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.cutlass.json.JsonException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.str.Utf8String;

import java.io.Closeable;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_OK;

public class ConfigProcessor implements HttpRequestProcessor, HttpContentListener, Closeable {
    private static final Log LOG = LogFactory.getLog(ConfigProcessor.class);
    // Local value has to be static because each thread will have its own instance of
    // processor. For different threads to lookup the same value from local value map the key,
    // which is LV, has to be the same between processor instances
    private static final LocalValue<ConfigProcessorState> LV = new LocalValue<>();
    private static final Utf8String URL_PARAM_MODE = new Utf8String("mode");
    private final ConfigStore configStore;
    private final byte requiredAuthType;
    private ConfigStore.Mode mode;
    private HttpConnectionContext transientContext;
    private ConfigProcessorState transientState;

    public ConfigProcessor(CairoEngine engine, JsonQueryProcessorConfiguration configuration) {
        configStore = engine.getConfigStore();
        requiredAuthType = configuration.getRequiredAuthType();
    }

    @Override
    public void close() {
    }

    @Override
    public void failRequest(HttpConnectionContext context, HttpException e) throws PeerDisconnectedException, PeerIsSlowToReadException {
        sendErr(e.getFlyweightMessage());
    }

    @Override
    public byte getRequiredAuthType() {
        return requiredAuthType;
    }

    @Override
    public void onContent(long lo, long hi) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        if (hi > lo) {
            try {
                transientState.sink.putNonAscii(lo, hi);
            } catch (CairoException | CairoError e) {
                sendErr(e.getFlyweightMessage());
            }
        }
    }

    @Override
    public void onHeadersReady(HttpConnectionContext context) {
        transientContext = context;
        transientState = LV.get(context);
        if (transientState == null) {
            LOG.debug().$("new config state").$();
            LV.set(context, transientState = new ConfigProcessorState());
        }

        mode = ConfigStore.Mode.of(context.getRequestHeader().getUrlParam(URL_PARAM_MODE));
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        // TODO: authorization, for now system admins can set config
        //   can be changed to a specific permission later
        context.getSecurityContext().authorizeSystemAdmin();

        try {
            configStore.save(transientState.sink, mode);
            sendOk();
        } catch (JsonException | CairoException | CairoError e) {
            LOG.error().$("error while saving config").$(e).$();
            sendErr(e.getFlyweightMessage());
        }
        transientState.clear();
    }

    @Override
    public void resumeRecv(HttpConnectionContext context) {
        transientContext = context;
        transientState = LV.get(context);
    }

    @Override
    public void resumeSend(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        context.resumeResponseSend();
    }

    private void sendErr(CharSequence message) throws PeerDisconnectedException, PeerIsSlowToReadException {
        transientContext.simpleResponse().sendStatusJsonContent(HTTP_BAD_REQUEST, message);
    }

    private void sendOk() throws PeerDisconnectedException, PeerIsSlowToReadException {
        transientContext.simpleResponse().sendStatusNoContent(HTTP_OK);
    }
}

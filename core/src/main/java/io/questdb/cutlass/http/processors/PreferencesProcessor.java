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
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpContentListener;
import io.questdb.cutlass.http.HttpException;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.cutlass.json.JsonException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.preferences.PreferencesStore;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;

import java.io.Closeable;

import static java.net.HttpURLConnection.*;

public class PreferencesProcessor implements HttpRequestProcessor, HttpContentListener, Closeable {
    private static final Log LOG = LogFactory.getLog(PreferencesProcessor.class);
    // Local value has to be static because each thread will have its own instance of
    // processor. For different threads to lookup the same value from local value map the key,
    // which is LV, has to be the same between processor instances
    private static final LocalValue<PreferencesProcessorState> LV = new LocalValue<>();
    private static final Utf8String URL_PARAM_MODE = new Utf8String("mode");
    private static final Utf8String URL_PARAM_VERSION = new Utf8String("version");
    private final HttpFullFatServerConfiguration configuration;
    private final PreferencesStore preferencesStore;
    private final byte requiredAuthType;
    private PreferencesStore.Mode mode;
    private HttpConnectionContext transientContext;
    private PreferencesProcessorState transientState;
    private long version;

    public PreferencesProcessor(CairoEngine engine, HttpFullFatServerConfiguration httpServerConfiguration) {
        configuration = httpServerConfiguration;
        preferencesStore = engine.getPreferencesStore();
        requiredAuthType = configuration.getJsonQueryProcessorConfiguration().getRequiredAuthType();
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
            LV.set(context, transientState = new PreferencesProcessorState(configuration.getRecvBufferSize()));
        }

        mode = PreferencesStore.Mode.of(context.getRequestHeader().getUrlParam(URL_PARAM_MODE));

        final Utf8Sequence versionStr = context.getRequestHeader().getUrlParam(URL_PARAM_VERSION);
        try {
            version = Numbers.parseLong(versionStr);
        } catch (NumericException e) {
            throw CairoException.nonCritical().put("Could not parse version, numeric value expected [version=").put(versionStr).put(']');
        }
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        try {
            context.getSecurityContext().authorizeSystemAdmin();

            preferencesStore.save(transientState.sink, mode, version);
            sendOk();
        } catch (JsonException | CairoError e) {
            LOG.error().$("could not save preferences").$((Throwable) e).$();
            sendErr(e.getFlyweightMessage());
        } catch (CairoException e) {
            LOG.error().$("could not save preferences").$((Throwable) e).$();
            sendErr(e.isAuthorizationError() ? HTTP_UNAUTHORIZED : HTTP_BAD_REQUEST, e.getFlyweightMessage());
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

    private void sendErr(int statusCode, CharSequence message) throws PeerDisconnectedException, PeerIsSlowToReadException {
        transientContext.simpleResponse().sendStatusJsonContent(statusCode, message);
    }

    private void sendErr(CharSequence message) throws PeerDisconnectedException, PeerIsSlowToReadException {
        sendErr(HTTP_BAD_REQUEST, message);
    }

    private void sendOk() throws PeerDisconnectedException, PeerIsSlowToReadException {
        transientContext.simpleResponse().sendStatusJsonContent(HTTP_OK, "");
    }
}

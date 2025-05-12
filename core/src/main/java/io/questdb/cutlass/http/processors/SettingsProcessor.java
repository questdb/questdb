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

import io.questdb.ServerConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpPostPutProcessor;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHeader;
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
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;

import static io.questdb.PropServerConfiguration.JsonPropertyValueFormatter.integer;
import static io.questdb.cutlass.http.HttpConstants.CONTENT_TYPE_JSON;
import static java.net.HttpURLConnection.*;

public class SettingsProcessor implements HttpRequestHandler {
    private static final Log LOG = LogFactory.getLog(SettingsProcessor.class);
    // Local value has to be static because each thread will have its own instance of
    // processor. For different threads to lookup the same value from local value map the key,
    // which is LV, has to be the same between processor instances
    private static final LocalValue<SettingsProcessorState> LV = new LocalValue<>();
    private static final String PREFERENCES_VERSION = "preferences.version";
    private static final Utf8String URL_PARAM_VERSION = new Utf8String("version");
    private static final ThreadLocal<Utf8StringSink> tlSink = new ThreadLocal<>(Utf8StringSink::new);
    private final GetProcessor getProcessor = new GetProcessor();
    private final PostPutProcessor postPutProcessor = new PostPutProcessor();
    private final PreferencesStore preferencesStore;
    private final byte requiredAuthTypeForUpdate;
    private final ServerConfiguration serverConfiguration;

    public SettingsProcessor(CairoEngine engine, ServerConfiguration serverConfiguration) {
        this.serverConfiguration = serverConfiguration;
        this.preferencesStore = engine.getPreferencesStore();

        requiredAuthTypeForUpdate = serverConfiguration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getRequiredAuthType();
    }

    @Override
    public HttpRequestProcessor getProcessor(HttpRequestHeader requestHeader) {
        return requestHeader.isGetRequest() ? getProcessor : postPutProcessor;
    }

    class GetProcessor implements HttpRequestProcessor {
        @Override
        public byte getRequiredAuthType() {
            return SecurityContext.AUTH_TYPE_NONE;
        }

        @Override
        public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
            final Utf8StringSink settings = tlSink.get();
            settings.clear();
            settings.putAscii('{');
            serverConfiguration.appendToSettingsSink(settings);
            integer(PREFERENCES_VERSION, preferencesStore.getVersion(), settings);
            preferencesStore.appendToSettingsSink(settings);
            settings.putAscii('}');

            final HttpChunkedResponse r = context.getChunkedResponse();
            r.status(HTTP_OK, CONTENT_TYPE_JSON);
            r.sendHeader();
            r.put(settings);
            r.sendChunk(true);
        }
    }

    class PostPutProcessor implements HttpPostPutProcessor {
        private HttpConnectionContext transientContext;
        private SettingsProcessorState transientState;

        @Override
        public byte getRequiredAuthType() {
            return requiredAuthTypeForUpdate;
        }

        @Override
        public void onChunk(long lo, long hi) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
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
                LOG.debug().$("new settings state").$();
                LV.set(context, transientState = new SettingsProcessorState(serverConfiguration.getHttpServerConfiguration().getRecvBufferSize()));
            }

            transientState.mode = PreferencesStore.Mode.of(context.getRequestHeader().getMethod());

            final Utf8Sequence versionStr = context.getRequestHeader().getUrlParam(URL_PARAM_VERSION);
            try {
                transientState.version = Numbers.parseLong(versionStr);
            } catch (NumericException e) {
                throw CairoException.nonCritical().put("Could not parse version, numeric value expected [version=").put(versionStr).put(']');
            }
        }

        @Override
        public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
            try {
                context.getSecurityContext().authorizeSystemAdmin();

                preferencesStore.save(transientState.sink, transientState.mode, transientState.version);
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
}

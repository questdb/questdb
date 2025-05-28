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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpPostPutProcessor;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.preferences.SettingsStore;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;

import static io.questdb.PropServerConfiguration.JsonPropertyValueFormatter.integer;
import static java.net.HttpURLConnection.*;

public class SettingsProcessor implements HttpRequestHandler {
    private static final Log LOG = LogFactory.getLog(SettingsProcessor.class);
    // Local value has to be static because each thread will have its own instance of
    // processor. For different threads to lookup the same value from local value map the key,
    // which is LV, has to be the same between processor instances
    private static final LocalValue<SettingsProcessorState> LV = new LocalValue<>();
    private static final LocalValue<Utf8StringSink> LV_GET = new LocalValue<>();
    private static final String PREFERENCES_VERSION = "preferences.version";
    private static final Utf8String URL_PARAM_VERSION = new Utf8String("version");
    private final GetProcessor getProcessor = new GetProcessor();
    private final PostPutProcessor postPutProcessor = new PostPutProcessor();
    private final byte requiredAuthTypeForUpdate;
    private final ServerConfiguration serverConfiguration;
    private final SettingsStore settingsStore;

    public SettingsProcessor(CairoEngine engine, ServerConfiguration serverConfiguration) {
        this.serverConfiguration = serverConfiguration;
        this.settingsStore = engine.getSettingsStore();

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
        public void onHeadersReady(HttpConnectionContext context) {
            final Utf8StringSink settings = LV_GET.get(context);
            if (settings == null) {
                LOG.debug().$("new settings sink").$();
                LV_GET.set(context, new Utf8StringSink());
            }
        }

        @Override
        public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
            final Utf8StringSink settings = LV_GET.get(context);
            settings.clear();
            settings.putAscii('{');
            serverConfiguration.exportConfiguration(settings);
            integer(PREFERENCES_VERSION, settingsStore.getVersion(), settings);
            settingsStore.exportPreferences(settings);
            settings.putAscii('}');

            context.simpleResponse().sendStatusJsonContent(HTTP_OK, settings, false);
        }

        @Override
        public void resumeSend(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
            final Utf8StringSink settings = LV_GET.get(context);
            context.simpleResponse().sendStatusJsonContent(HTTP_OK, settings, false);
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
                transientState.utf8Sink.putNonAscii(lo, hi);
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

            transientState.mode = SettingsStore.Mode.of(context.getRequestHeader().getMethod());

            transientState.utf16Sink.clear();
            transientState.utf16Sink.put(context.getRequestHeader().getUrlParam(URL_PARAM_VERSION));
        }

        @Override
        public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
            try {
                context.getSecurityContext().authorizeSettings();
                settingsStore.save(transientState.utf8Sink, transientState.mode, parseVersion(transientState.utf16Sink));
                sendOk();
            } catch (CairoException e) {
                LOG.error().$("could not save preferences").$((Throwable) e).$();
                sendErr(e);
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

        private long parseVersion(CharSequence version) {
            try {
                return Numbers.parseLong(version);
            } catch (NumericException e) {
                LOG.error().$("could not parse version, numeric value expected [version='").$(version).$('\'').I$();
                throw CairoException.nonCritical().put("Invalid version, numeric value expected [version='").put(version).put("']");
            }
        }

        private void sendErr(CairoException e) throws PeerDisconnectedException, PeerIsSlowToReadException {
            transientState.utf8Sink.clear();
            transientState.utf8Sink.put("{\"error\":\"").escapeJsonStr(e.getFlyweightMessage()).put("\"}");
            transientContext.simpleResponse().sendStatusJsonContent(
                    e.isPreferencesOutOfDateError() ? HTTP_CONFLICT
                            : e.isAuthorizationError() ? HTTP_UNAUTHORIZED
                            : HTTP_BAD_REQUEST,
                    transientState.utf8Sink
            );
        }

        private void sendOk() throws PeerDisconnectedException, PeerIsSlowToReadException {
            transientContext.simpleResponse().sendStatusJsonContent(HTTP_OK, Utf8String.EMPTY);
        }
    }
}

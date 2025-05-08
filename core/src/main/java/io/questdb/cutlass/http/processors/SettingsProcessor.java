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
import io.questdb.cairo.SecurityContext;
import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.preferences.PreferencesStore;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;

import static io.questdb.PropServerConfiguration.JsonPropertyValueFormatter.integer;
import static io.questdb.cutlass.http.HttpConstants.CONTENT_TYPE_JSON;
import static io.questdb.cutlass.http.HttpConstants.METHOD_GET;
import static java.net.HttpURLConnection.HTTP_OK;

public class SettingsProcessor implements HttpRequestProcessor {
    private static final String PREFERENCES_VERSION = "preferences.version";
    private static final ThreadLocal<Utf8StringSink> tlSink = new ThreadLocal<>(Utf8StringSink::new);
    private final PreferencesStore preferencesStore;
    private final byte requiredAuthType;
    private final ServerConfiguration serverConfiguration;

    public SettingsProcessor(ServerConfiguration serverConfiguration, PreferencesStore preferencesStore) {
        this.serverConfiguration = serverConfiguration;
        this.preferencesStore = preferencesStore;

        requiredAuthType = serverConfiguration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration().getRequiredAuthType();
    }

    @Override
    public byte getRequiredAuthType(Utf8Sequence method) {
        return Utf8s.equalsNcAscii(METHOD_GET, method) ? SecurityContext.AUTH_TYPE_NONE : requiredAuthType;
    }

    @Override
    public void onHeadersReady(HttpConnectionContext context) {
        context.getRequestHeader().getMethod();
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

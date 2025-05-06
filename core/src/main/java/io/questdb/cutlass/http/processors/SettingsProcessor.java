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
import io.questdb.std.str.Utf8StringSink;

import static io.questdb.cutlass.http.HttpConstants.CONTENT_TYPE_JSON;
import static java.net.HttpURLConnection.HTTP_OK;

public class SettingsProcessor implements HttpRequestProcessor {
    private static final ThreadLocal<Utf8StringSink> tlSink = new ThreadLocal<>(Utf8StringSink::new);
    private final PreferencesStore preferencesStore;
    private final ServerConfiguration serverConfiguration;

    public SettingsProcessor(ServerConfiguration serverConfiguration, PreferencesStore preferencesStore) {
        this.serverConfiguration = serverConfiguration;
        this.preferencesStore = preferencesStore;
    }

    @Override
    public byte getRequiredAuthType() {
        return SecurityContext.AUTH_TYPE_NONE;
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final Utf8StringSink sink = tlSink.get();

        sink.clear();
        sink.putAscii('{');
        serverConfiguration.getCairoConfiguration().populateSettings(sink);
        serverConfiguration.getPublicPassthroughConfiguration().populateSettings(sink);
        preferencesStore.populateSettings(sink);
        sink.clear(sink.size() - 1);
        sink.putAscii('}');

        final HttpChunkedResponse r = context.getChunkedResponse();
        r.status(HTTP_OK, CONTENT_TYPE_JSON);
        r.sendHeader();
        r.put(sink);
        r.sendChunk(true);
    }
}

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
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8StringSink;

import java.net.HttpURLConnection;

public class SettingsProcessor implements HttpRequestProcessor {
    private final Utf8StringSink sink = new Utf8StringSink();

    public SettingsProcessor(ServerConfiguration serverConfiguration) {
        final CharSequenceObjHashMap<CharSequence> settings = new CharSequenceObjHashMap<>();
        serverConfiguration.getCairoConfiguration().populateSettings(settings);
        serverConfiguration.getPublicPassthroughConfiguration().populateSettings(settings);
        sink.putAscii('{');
        final ObjList<CharSequence> keys = settings.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final CharSequence key = keys.getQuick(i);
            final CharSequence value = settings.get(key);
            sink.putQuoted(key).putAscii(':').put(value);
            if (i != n - 1) {
                sink.putAscii(',');
            }
        }
        sink.putAscii('}');
    }

    @Override
    public byte getRequiredAuthType() {
        return SecurityContext.AUTH_TYPE_NONE;
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final HttpChunkedResponse r = context.getChunkedResponse();
        r.status(HttpURLConnection.HTTP_OK, "application/json");
        r.sendHeader();
        r.put(sink);
        r.sendChunk(true);
    }
}

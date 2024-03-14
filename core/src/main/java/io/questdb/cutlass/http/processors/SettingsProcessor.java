/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.SecurityContext;
import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;

import java.net.HttpURLConnection;

public class SettingsProcessor implements HttpRequestProcessor {
    private static final Utf8String V2 = new Utf8String("v2");
    private static final Utf8String VERSION = new Utf8String("version");
    private final Utf8StringSink sink = new Utf8StringSink();
    private final Utf8StringSink sinkV2 = new Utf8StringSink();

    public SettingsProcessor(CairoConfiguration cairoConfiguration) {
        final CharSequenceObjHashMap<CharSequence> settings = new CharSequenceObjHashMap<>();
        cairoConfiguration.populateSettings(settings);

        sink.putAscii('{');
        sinkV2.putAscii('{');
        final ObjList<CharSequence> keys = settings.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final CharSequence key = keys.getQuick(i);
            final CharSequence value = settings.get(key);
            sink.putQuoted(key).putAscii(':');
            if (Chars.startsWith(value, '"')) {
                // json string type
                sink.put(value);
            } else {
                // other json types (boolean, number) need quotes in v1
                sink.putQuoted(value);
            }
            sinkV2.putQuoted(key).putAscii(':').put(value);
            if (i != n - 1) {
                sink.putAscii(',');
                sinkV2.putAscii(',');
            }
        }
        sink.putAscii('}');
        sinkV2.putAscii('}');
    }

    @Override
    public byte getRequiredAuthType() {
        return SecurityContext.AUTH_TYPE_NONE;
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final Utf8Sequence version = context.getRequestHeader().getUrlParam(VERSION);
        final Utf8StringSink payload = version != null && Utf8s.equals(version, V2) ? sinkV2 : sink;

        final HttpChunkedResponse r = context.getChunkedResponse();
        r.status(HttpURLConnection.HTTP_OK, "application/json");
        r.sendHeader();
        r.put(payload);
        r.sendChunk(true);
    }
}

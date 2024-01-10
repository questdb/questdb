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
import io.questdb.cutlass.http.HttpChunkedResponseSocket;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.ObjList;

public class SettingsProcessor implements HttpRequestProcessor {

    private final CairoConfiguration cairoConfiguration;
    private final CharSequenceObjHashMap<CharSequence> settings = new CharSequenceObjHashMap<>();

    public SettingsProcessor(CairoConfiguration cairoConfiguration) {
        this.cairoConfiguration = cairoConfiguration;
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        cairoConfiguration.populateSettings(settings);

        final HttpChunkedResponseSocket r = context.getChunkedResponseSocket();
        r.status(200, "application/json");
        r.sendHeader();
        r.putAscii('{');
        final ObjList<CharSequence> keys = settings.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final CharSequence key = keys.getQuick(i);
            final CharSequence value = settings.get(key);
            r.putQuoted(key).putAscii(':').putQuoted(value);
            if (i != n - 1) {
                r.putAscii(',');
            }
        }
        r.putAscii('}');
        r.sendChunk(true);
    }

    @Override
    public boolean requiresAuthentication() {
        return false;
    }
}

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
import io.questdb.cairo.TableUtils;
import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;

import java.io.Closeable;

import static io.questdb.cutlass.http.HttpConstants.URL_PARAM_STATUS_FORMAT;
import static io.questdb.cutlass.http.HttpConstants.URL_PARAM_STATUS_TABLE_NAME;

public class TableStatusCheckProcessor implements HttpRequestProcessor, Closeable {

    private final CairoEngine cairoEngine;
    private final String keepAliveHeader;
    private final Path path = new Path();
    private final byte requiredAuthType;
    private final StringSink utf16Sink = new StringSink();

    public TableStatusCheckProcessor(CairoEngine cairoEngine, JsonQueryProcessorConfiguration configuration) {
        this.cairoEngine = cairoEngine;
        this.keepAliveHeader = Chars.toString(configuration.getKeepAliveHeader());
        this.requiredAuthType = configuration.getRequiredAuthType();
    }

    @Override
    public void close() {
        Misc.free(path);
    }

    @Override
    public byte getRequiredAuthType() {
        return requiredAuthType;
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        DirectUtf8Sequence tableName = context.getRequestHeader().getUrlParam(URL_PARAM_STATUS_TABLE_NAME);
        if (tableName == null) {
            context.simpleResponse().sendStatusTextContent(200, "table name missing", null);
        } else {
            int check = TableUtils.TABLE_DOES_NOT_EXIST;
            utf16Sink.clear();
            if (Utf8s.utf8ToUtf16(tableName, utf16Sink)) {
                check = cairoEngine.getTableStatus(path, utf16Sink);
            }
            if (Utf8s.equalsNcAscii("json", context.getRequestHeader().getUrlParam(URL_PARAM_STATUS_FORMAT))) {
                HttpChunkedResponse response = context.getChunkedResponse();
                response.status(200, "application/json");

                response.headers().put(keepAliveHeader);
                response.sendHeader();

                response.put('{').putAsciiQuoted("status").putAscii(':').putAsciiQuoted(toResponse(check)).putAscii('}');
                response.sendChunk(true);
            } else {
                context.simpleResponse().sendStatusTextContent(200, toResponse(check), null);
            }
        }
    }

    private static String toResponse(int existenceCheckResult) {
        switch (existenceCheckResult) {
            case TableUtils.TABLE_EXISTS:
                return "Exists";
            case TableUtils.TABLE_DOES_NOT_EXIST:
                return "Does not exist";
            default:
                return "Reserved name";
        }
    }
}

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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cutlass.http.HttpChunkedResponseSocket;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class TableStatusCheckProcessor implements HttpRequestProcessor, Closeable {

    private final CairoEngine cairoEngine;
    private final String keepAliveHeader;
    private final Path path = new Path();

    public TableStatusCheckProcessor(CairoEngine cairoEngine, JsonQueryProcessorConfiguration configuration) {
        this.cairoEngine = cairoEngine;
        this.keepAliveHeader = Chars.toString(configuration.getKeepAliveHeader());
    }

    @Override
    public void close() {
        Misc.free(path);
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        CharSequence tableName = context.getRequestHeader().getUrlParam("j");
        if (tableName == null) {
            context.simpleResponse().sendStatus(200, "table name missing");
        } else {
            TableToken tableToken = cairoEngine.getTableTokenIfExists(tableName);
            int check = cairoEngine.getTableStatus(path, tableToken);
            if (Chars.equalsNc("json", context.getRequestHeader().getUrlParam("f"))) {
                HttpChunkedResponseSocket r = context.getChunkedResponseSocket();
                r.status(200, "application/json");

                r.headers().put(keepAliveHeader);
                r.sendHeader();

                r.put('{').putQuoted("status").put(':').putQuoted(toResponse(check)).put('}');
                r.sendChunk(true);
            } else {
                context.simpleResponse().sendStatus(200, toResponse(check));
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

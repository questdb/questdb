/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cutlass.http.processors;

import com.questdb.cairo.CairoEngine;
import com.questdb.cairo.TableUtils;
import com.questdb.cutlass.http.HttpChunkedResponseSocket;
import com.questdb.cutlass.http.HttpConnectionContext;
import com.questdb.cutlass.http.HttpRequestProcessor;
import com.questdb.network.IODispatcher;
import com.questdb.network.IOOperation;
import com.questdb.network.PeerDisconnectedException;
import com.questdb.network.PeerIsSlowToReadException;
import com.questdb.std.Chars;
import com.questdb.std.Misc;
import com.questdb.std.str.Path;

public class TableStatusCheckProcessor implements HttpRequestProcessor {

    private final CairoEngine cairoEngine;
    private final Path path = new Path();

    public TableStatusCheckProcessor(CairoEngine cairoEngine) {
        this.cairoEngine = cairoEngine;
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

    @Override
    public void onHeadersReady(HttpConnectionContext context) {
    }

    @Override
    public void resumeRecv(HttpConnectionContext context, IODispatcher<HttpConnectionContext> dispatcher) {
    }

    @Override
    public void resumeSend(HttpConnectionContext context, IODispatcher<HttpConnectionContext> dispatcher) {
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context, IODispatcher<HttpConnectionContext> dispatcher) throws PeerDisconnectedException, PeerIsSlowToReadException {
        CharSequence tableName = context.getRequestHeader().getUrlParam("j");
        if (tableName == null) {
            context.simpleResponse().sendStatus(400, "table name missing");
        } else {
            int check = cairoEngine.getStatus(context.getCairoSecurityContext(), path, tableName);
            if (Chars.equalsNc("json", context.getRequestHeader().getUrlParam("f"))) {
                HttpChunkedResponseSocket r = context.getChunkedResponseSocket();
                r.status(200, "application/json");

                // todo: configure this header externally
                r.headers().put("Keep-Alive: timeout=5, max=10000").put(Misc.EOL);
                r.sendHeader();

                r.put('{').putQuoted("status").put(':').putQuoted(toResponse(check)).put('}');
                r.sendChunk();
                r.done();
            } else {
                context.simpleResponse().sendStatus(200, toResponse(check));
            }
        }
        dispatcher.registerChannel(context, IOOperation.READ);
    }
}

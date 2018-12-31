/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.tuck.http.handlers;

import com.questdb.std.Chars;
import com.questdb.std.ex.DisconnectedChannelException;
import com.questdb.std.ex.SlowWritableChannelException;
import com.questdb.tuck.http.ChunkedResponse;
import com.questdb.tuck.http.Request;

public class QueryHandlerContext extends AbstractQueryContext {
    boolean fetchAll = false;
    boolean noMeta = false;

    public QueryHandlerContext(long fd, int cyclesBeforeCancel) {
        super(fd, cyclesBeforeCancel);
    }

    @Override
    public void clear() {
        super.clear();
        queryState = QUERY_PREFIX;
        fetchAll = false;
    }

    @Override
    public boolean parseUrl(ChunkedResponse r, Request request) throws DisconnectedChannelException, SlowWritableChannelException {
        if (super.parseUrl(r, request)) {
            noMeta = Chars.equalsNc("true", request.getUrlParam("nm"));
            fetchAll = Chars.equalsNc("true", request.getUrlParam("count"));
            return true;
        }
        return false;
    }

    @Override
    protected void header(ChunkedResponse r, int status) throws DisconnectedChannelException, SlowWritableChannelException {
        r.status(status, "application/json; charset=utf-8");
        r.sendHeader();
    }

    @Override
    protected void sendException(ChunkedResponse r, int position, CharSequence message, int status) throws DisconnectedChannelException, SlowWritableChannelException {
        header(r, status);
        r.put('{').
                putQuoted("query").put(':').encodeUtf8AndQuote(query == null ? "" : query).put(',').
                putQuoted("error").put(':').encodeUtf8AndQuote(message).put(',').
                putQuoted("position").put(':').put(position);
        r.put('}');
        r.sendChunk();
        r.done();
    }
}

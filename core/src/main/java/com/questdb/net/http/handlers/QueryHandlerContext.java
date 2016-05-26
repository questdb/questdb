/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.questdb.net.http.handlers;

import com.questdb.ex.DisconnectedChannelException;
import com.questdb.ex.SlowWritableChannelException;
import com.questdb.misc.Chars;
import com.questdb.net.http.ChunkedResponse;
import com.questdb.net.http.Request;

public class QueryHandlerContext extends AbstractQueryContext {
    boolean fetchAll = false;
    boolean noMeta = false;

    public QueryHandlerContext(long fd, int cyclesBeforeCancel) {
        super(fd, cyclesBeforeCancel);
    }

    @Override
    public void clear() {
        super.clear();
        state = QueryState.PREFIX;
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
                putQuoted("query").put(':').putUtf8EscapedAndQuoted(query == null ? "" : query).put(',').
                putQuoted("error").put(':').putQuoted(message).put(',').
                putQuoted("position").put(':').put(position);
        r.put('}');
        r.sendChunk();
        r.done();
    }
}

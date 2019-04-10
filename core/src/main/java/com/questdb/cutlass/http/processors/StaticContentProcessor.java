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

import com.questdb.cutlass.http.*;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.network.IODispatcher;
import com.questdb.std.*;
import com.questdb.std.str.FileNameExtractorCharSequence;
import com.questdb.std.str.LPSZ;
import com.questdb.std.str.PrefixedPath;

import java.io.Closeable;

public class StaticContentProcessor implements HttpRequestProcessor {

    private static final Log LOG = LogFactory.getLog(StaticContentProcessor.class);

    private final MimeTypesCache mimeTypes;
    private final LocalValue<StaticContentProcessorState> stateAccessor = new LocalValue<>();
    private final HttpRangeParser rangeParser = new HttpRangeParser();
    private final PrefixedPath prefixedPath;
    private final CharSequence indexFileName;
    private final FilesFacade ff;

    public StaticContentProcessor(StaticContentProcessorConfiguration configuration) {
        this.mimeTypes = configuration.getMimeTypesCache();
        this.prefixedPath = new PrefixedPath(configuration.getPublicDirectory());
        this.indexFileName = configuration.getIndexFileName();
        this.ff = configuration.getFilesFacade();
    }

    @Override
    public void onHeadersReady(HttpConnectionContext context) {
    }

    @Override
    public void onRequestComplete(
            HttpConnectionContext context,
            IODispatcher<HttpConnectionContext> dispatcher
    ) {
        HttpHeaders headers = context.getHeaders();
        CharSequence url = headers.getUrl();
        LOG.info().$("handling static: ").$(url).$();
        if (Chars.contains(url, "..")) {
            LOG.info().$("URL abuse: ").$(url).$();
            context.simpleResponse().send(404);
        } else {
            PrefixedPath path = prefixedPath.rewind();

            if (Chars.equals(url, '/')) {
                path.concat(indexFileName);
            } else {
                path.concat(url);
            }

            path.$();

            if (ff.exists(path)) {
                send(context, path, headers.getUrlParam("attachment") != null);
            } else {
                LOG.info().$("not found [path=").$(path).$(']').$();
                context.simpleResponse().send(404);
            }
        }
    }

    @Override
    public void resume(HttpConnectionContext context) {
        StaticContentProcessorState state = stateAccessor.get(context);

        if (state == null || state.fd == -1) {
            return;
        }

        HttpResponseSink.DirectBufferResponse r = context.getDirectBufferResponse();
        long wptr = r.getBuffer();
        int sz = r.getBufferSize();

        long l;
        // todo: check what happens when this code cannot read file
        while (state.bytesSent < state.sendMax && (l = ff.read(state.fd, wptr, sz, state.bytesSent)) > 0) {
            if (l + state.bytesSent > state.sendMax) {
                l = state.sendMax - state.bytesSent;
            }
            state.bytesSent += l;
            r.send((int) l);
        }
        // reached the end naturally?
        state.clear();
    }

    private void send(HttpConnectionContext context, LPSZ path, boolean asAttachment) {
        int n = Chars.lastIndexOf(path, '.');
        if (n == -1) {
            LOG.info().$("Missing extension: ").$(path).$();
            context.simpleResponse().send(404);
            return;
        }

        HttpHeaders headers = context.getHeaders();
        CharSequence contentType = mimeTypes.valueAt(mimeTypes.keyIndex(path, n + 1, path.length()));
        CharSequence val;
        if ((val = headers.getHeader("Range")) != null) {
            sendRange(context, val, path, contentType, asAttachment);
            return;
        }

        int l;
        // attempt not to send file when remote side already has
        // up-to-date version of the same
        if ((val = headers.getHeader("If-None-Match")) != null
                && (l = val.length()) > 2
                && val.charAt(0) == '"'
                && val.charAt(l - 1) == '"') {
            try {
                long that = Numbers.parseLong(val, 1, l - 1);
                if (that == ff.getLastModified(path)) {
                    context.simpleResponse().sendEmptyBody(304);
                    return;
                }
            } catch (NumericException e) {
                LOG.info().$("Received wrong tag [").$(val).$("] for ").$(path).$();
                context.simpleResponse().send(400);
                return;
            }
        }

        sendVanilla(context, path, contentType, asAttachment);
    }

    private void sendRange(
            HttpConnectionContext context,
            CharSequence range,
            LPSZ path,
            CharSequence contentType,
            boolean asAttachment) {
        if (rangeParser.of(range)) {

            StaticContentProcessorState state = stateAccessor.get(context);
            if (state == null) {
                stateAccessor.set(context, state = new StaticContentProcessorState());
            }

            state.fd = ff.openRO(path);
            if (state.fd == -1) {
                LOG.info().$("Cannot open file: ").$(path).$();
                context.simpleResponse().send(404);
                return;
            }

            state.bytesSent = 0;

            final long length = ff.length(path);
            final long lo = rangeParser.getLo();
            final long hi = rangeParser.getHi();
            if (lo > length || (hi != Long.MAX_VALUE && hi > length) || lo > hi) {
                context.simpleResponse().send(416);
            } else {
                state.bytesSent = lo;
                state.sendMax = hi == Long.MAX_VALUE ? length : hi;

                final HttpResponseSink.HeaderOnlyResponse r = context.getHeaderOnlyResponse();
                final HttpResponseHeaderSink headers = r.getHeaderSink();
                headers.status(206, contentType, state.sendMax - lo);
                if (asAttachment) {
                    headers.put("Content-Disposition: attachment; filename=\"").put(FileNameExtractorCharSequence.get(path)).put('\"').put(Misc.EOL);
                }
                headers.put("Accept-Ranges: bytes").put(Misc.EOL);
                headers.put("Content-Range: bytes ").put(lo).put('-').put(state.sendMax).put('/').put(length).put(Misc.EOL);
                headers.put("ETag: ").put(ff.getLastModified(path)).put(Misc.EOL);
                r.send();
                resume(context);
            }
        } else {
            context.simpleResponse().send(416);
        }
    }

    private void sendVanilla(HttpConnectionContext context, LPSZ path, CharSequence contentType, boolean asAttachment) {
        long fd = ff.openRO(path);
        if (fd == -1) {
            LOG.info().$("Cannot open file: ").$(path).$('(').$(Os.errno()).$(')').$();
            context.simpleResponse().send(404);
        } else {
            StaticContentProcessorState h = stateAccessor.get(context);
            if (h == null) {
                stateAccessor.set(context, h = new StaticContentProcessorState());
            }
            h.fd = fd;
            h.bytesSent = 0;
            final long length = ff.length(path);
            h.sendMax = length;

            final HttpResponseSink.HeaderOnlyResponse r = context.getHeaderOnlyResponse();
            final HttpResponseHeaderSink headers = r.getHeaderSink();
            headers.status(200, contentType, length);
            if (asAttachment) {
                headers.put("Content-Disposition: attachment; filename=\"").put(FileNameExtractorCharSequence.get(path)).put("\"").put(Misc.EOL);
            }
            headers.put("ETag: ").put('"').put(ff.getLastModified(path)).put('"').put(Misc.EOL);
            r.send();
            resume(context);
        }
    }

    private static class StaticContentProcessorState implements Mutable, Closeable {
        long fd = -1;
        long bytesSent;
        long sendMax;

        @Override
        public void clear() {
            if (fd > -1) {
                Files.close(fd);
                fd = -1;
            }
            bytesSent = 0;
            sendMax = Long.MAX_VALUE;
        }

        @Override
        public void close() {
            clear();
        }
    }
}

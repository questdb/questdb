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
import com.questdb.network.IOOperation;
import com.questdb.network.PeerDisconnectedException;
import com.questdb.network.PeerIsSlowToReadException;
import com.questdb.std.*;
import com.questdb.std.str.FileNameExtractorCharSequence;
import com.questdb.std.str.LPSZ;
import com.questdb.std.str.PrefixedPath;

import java.io.Closeable;

public class StaticContentProcessor implements HttpRequestProcessor, Closeable {
    private static final Log LOG = LogFactory.getLog(StaticContentProcessor.class);
    private static final LocalValue<StaticContentProcessorState> LV = new LocalValue<>();
    private final MimeTypesCache mimeTypes;
    private final HttpRangeParser rangeParser = new HttpRangeParser();
    private final PrefixedPath prefixedPath;
    private final CharSequence indexFileName;
    private final FilesFacade ff;
    private final String keepAliveHeader;

    public StaticContentProcessor(StaticContentProcessorConfiguration configuration) {
        this.mimeTypes = configuration.getMimeTypesCache();
        this.prefixedPath = new PrefixedPath(configuration.getPublicDirectory());
        this.indexFileName = configuration.getIndexFileName();
        this.ff = configuration.getFilesFacade();
        this.keepAliveHeader = configuration.getKeepAliveHeader();
    }

    @Override
    public void close() {
        Misc.free(prefixedPath);
    }

    @Override
    public void onHeadersReady(HttpConnectionContext context) {
    }

    @Override
    public void onRequestComplete(
            HttpConnectionContext context,
            IODispatcher<HttpConnectionContext> dispatcher
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        HttpRequestHeader headers = context.getRequestHeader();
        CharSequence url = headers.getUrl();
        LOG.info().$("incoming [url=").$(url).$(']').$();
        if (Chars.contains(url, "..")) {
            LOG.info().$("URL abuse: ").$(url).$();
            sendStatusWithDefaultMessage(context, dispatcher, 404);
        } else {
            PrefixedPath path = prefixedPath.rewind();

            if (Chars.equals(url, '/')) {
                path.concat(indexFileName);
            } else {
                path.concat(url);
            }

            path.$();

            if (ff.exists(path)) {
                send(context, dispatcher, path, headers.getUrlParam("attachment") != null);
            } else {
                LOG.info().$("not found [path=").$(path).$(']').$();
                sendStatusWithDefaultMessage(context, dispatcher, 404);
            }
        }
    }

    @Override
    public void resumeSend(HttpConnectionContext context, IODispatcher<HttpConnectionContext> dispatcher) throws PeerDisconnectedException, PeerIsSlowToReadException {
        LOG.debug().$("resumeSend").$();
        StaticContentProcessorState state = LV.get(context);

        if (state == null || state.fd == -1) {
            return;
        }

        final HttpRawSocket socket = context.getRawResponseSocket();
        long address = socket.getBufferAddress();
        int size = socket.getBufferSize();

        long l;
        // todo: check what happens when this code cannot read file
        while (state.bytesSent < state.sendMax && (l = ff.read(state.fd, address, size, state.bytesSent)) > 0) {
            if (l + state.bytesSent > state.sendMax) {
                l = state.sendMax - state.bytesSent;
            }
            state.bytesSent += l;
            socket.send((int) l);
        }
        // reached the end naturally?
        readyForNextRequest(context, dispatcher);
    }

    private void readyForNextRequest(HttpConnectionContext context, IODispatcher<HttpConnectionContext> dispatcher) {
        context.clear();
        dispatcher.registerChannel(context, IOOperation.READ);
    }

    private void send(HttpConnectionContext context, IODispatcher<HttpConnectionContext> dispatcher, LPSZ path, boolean asAttachment) throws PeerDisconnectedException, PeerIsSlowToReadException {
        int n = Chars.lastIndexOf(path, '.');
        if (n == -1) {
            LOG.info().$("Missing extension: ").$(path).$();
            sendStatusWithDefaultMessage(context, dispatcher, 404);
            return;
        }

        HttpRequestHeader headers = context.getRequestHeader();
        CharSequence contentType = mimeTypes.valueAt(mimeTypes.keyIndex(path, n + 1, path.length()));
        CharSequence val;
        if ((val = headers.getHeader("Range")) != null) {
            sendRange(context, dispatcher, val, path, contentType, asAttachment);
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
                    context.simpleResponse().sendStatus(304);
                    readyForNextRequest(context, dispatcher);
                    return;
                }
            } catch (NumericException e) {
                LOG.info().$("bad 'If-None-Match' [value=").$(val).$(']').$();
                sendStatusWithDefaultMessage(context, dispatcher, 400);
                return;
            }
        }

        sendVanilla(context, dispatcher, path, contentType, asAttachment);
    }

    private void sendRange(
            HttpConnectionContext context,
            IODispatcher<HttpConnectionContext> dispatcher,
            CharSequence range,
            LPSZ path,
            CharSequence contentType,
            boolean asAttachment) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (rangeParser.of(range)) {

            StaticContentProcessorState state = LV.get(context);
            if (state == null) {
                LV.set(context, state = new StaticContentProcessorState());
            }

            state.fd = ff.openRO(path);
            if (state.fd == -1) {
                LOG.info().$("Cannot open file: ").$(path).$();
                sendStatusWithDefaultMessage(context, dispatcher, 404);
                return;
            }

            state.bytesSent = 0;

            final long length = ff.length(path);
            final long lo = rangeParser.getLo();
            final long hi = rangeParser.getHi();
            if (lo > length || (hi != Long.MAX_VALUE && hi > length) || lo > hi) {
                sendStatusWithDefaultMessage(context, dispatcher, 416);
            } else {
                state.bytesSent = lo;
                state.sendMax = hi == Long.MAX_VALUE ? length : hi;

                final HttpResponseHeader header = context.getResponseHeader();
                header.status(206, contentType, state.sendMax - lo);
                if (asAttachment) {
                    header.put("Content-Disposition: attachment; filename=\"").put(FileNameExtractorCharSequence.get(path)).put('\"').put(Misc.EOL);
                }
                header.put("Accept-Ranges: bytes").put(Misc.EOL);
                header.put("Content-Range: bytes ").put(lo).put('-').put(state.sendMax).put('/').put(length).put(Misc.EOL);
                header.put("ETag: ").put(ff.getLastModified(path)).put(Misc.EOL);
                if (keepAliveHeader != null) {
                    header.put(keepAliveHeader);
                }
                header.send();
                resumeSend(context, dispatcher);
            }
        } else {
            sendStatusWithDefaultMessage(context, dispatcher, 416);
        }
    }

    private void sendStatusWithDefaultMessage(HttpConnectionContext context, IODispatcher<HttpConnectionContext> dispatcher, int code) throws PeerDisconnectedException, PeerIsSlowToReadException {
        context.simpleResponse().sendStatusWithDefaultMessage(code);
        readyForNextRequest(context, dispatcher);
    }

    private void sendVanilla(
            HttpConnectionContext context,
            IODispatcher<HttpConnectionContext> dispatcher,
            LPSZ path, CharSequence contentType,
            boolean asAttachment
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        long fd = ff.openRO(path);
        if (fd == -1) {
            LOG.info().$("Cannot open file: ").$(path).$('(').$(ff.errno()).$(')').$();
            sendStatusWithDefaultMessage(context, dispatcher, 404);
        } else {
            StaticContentProcessorState h = LV.get(context);
            if (h == null) {
                LV.set(context, h = new StaticContentProcessorState());
            }
            h.fd = fd;
            h.bytesSent = 0;
            final long length = ff.length(path);
            h.sendMax = length;

            final HttpResponseHeader header = context.getResponseHeader();
            header.status(200, contentType, length);
            if (asAttachment) {
                header.put("Content-Disposition: attachment; filename=\"").put(FileNameExtractorCharSequence.get(path)).put("\"").put(Misc.EOL);
            }
            header.put("ETag: ").put('"').put(ff.getLastModified(path)).put('"').put(Misc.EOL);
            header.setKeepAlive(keepAliveHeader);
            header.send();
            resumeSend(context, dispatcher);
        }
    }

}

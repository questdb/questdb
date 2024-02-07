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

import io.questdb.cairo.SecurityContext;
import io.questdb.cutlass.http.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.str.*;

import java.io.Closeable;

import static io.questdb.cutlass.http.HttpConstants.*;

public class StaticContentProcessor implements HttpRequestProcessor, Closeable {
    private static final Log LOG = LogFactory.getLog(StaticContentProcessor.class);
    private static final LocalValue<StaticContentProcessorState> LV = new LocalValue<>();
    private final FilesFacade ff;
    private final String httpProtocolVersion;
    private final CharSequence indexFileName;
    private final String keepAliveHeader;
    private final MimeTypesCache mimeTypes;
    private final PrefixedPath prefixedPath;
    private final HttpRangeParser rangeParser = new HttpRangeParser();
    private final byte requiredAuthType;

    public StaticContentProcessor(HttpServerConfiguration configuration) {
        this.mimeTypes = configuration.getStaticContentProcessorConfiguration().getMimeTypesCache();
        this.prefixedPath = new PrefixedPath(configuration.getStaticContentProcessorConfiguration().getPublicDirectory());
        this.indexFileName = configuration.getStaticContentProcessorConfiguration().getIndexFileName();
        this.ff = configuration.getStaticContentProcessorConfiguration().getFilesFacade();
        this.keepAliveHeader = configuration.getStaticContentProcessorConfiguration().getKeepAliveHeader();
        this.httpProtocolVersion = configuration.getHttpContextConfiguration().getHttpVersion();
        this.requiredAuthType = configuration.getStaticContentProcessorConfiguration().getRequiredAuthType();
    }

    @Override
    public void close() {
        Misc.free(prefixedPath);
    }

    @Override
    public byte getRequiredAuthType() {
        return requiredAuthType;
    }

    public LogRecord logInfoWithFd(HttpConnectionContext context) {
        return LOG.info().$('[').$(context.getFd()).$("] ");
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final HttpRequestHeader headers = context.getRequestHeader();
        Utf8Sequence url = headers.getUrl();
        logInfoWithFd(context).$("incoming [url=").$(url).$(']').$();
        if (Utf8s.containsAscii(url, "..")) {
            logInfoWithFd(context).$("URL abuse: ").$(url).$();
            sendStatusTextContent(context, 404);
        } else {
            PrefixedPath path = prefixedPath.rewind();

            if (url.size() == 1 && url.byteAt(0) == '/') {
                path.concat(indexFileName);
            } else {
                path.concat(url);
            }

            path.$();

            if (ff.exists(path)) {
                send(context, path, headers.getUrlParam(URL_PARAM_ATTACHMENT) != null);
            } else {
                logInfoWithFd(context).$("not found [path=").$(path).$(']').$();
                sendStatusTextContent(context, 404);
            }
        }
    }

    @Override
    public boolean requiresAuthentication() {
        return requiredAuthType == SecurityContext.AUTH_TYPE_CREDENTIALS;
    }

    @Override
    public void resumeSend(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        LOG.debug().$("resumeSend").$();
        StaticContentProcessorState state = LV.get(context);

        if (state == null || state.fd == -1) {
            return;
        }

        context.resumeResponseSend();

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
    }

    private static void sendStatusTextContent(HttpConnectionContext context, int code) throws PeerDisconnectedException, PeerIsSlowToReadException {
        context.simpleResponse().sendStatusTextContent(code);
    }

    private void send(HttpConnectionContext context, LPSZ path, boolean asAttachment) throws PeerDisconnectedException, PeerIsSlowToReadException {
        int n = Utf8s.lastIndexOfAscii(path, '.');
        if (n == -1) {
            logInfoWithFd(context).$("missing extension [file=").$(path).$(']').$();
            sendStatusTextContent(context, 404);
            return;
        }

        HttpRequestHeader headers = context.getRequestHeader();
        CharSequence contentType = mimeTypes.valueAt(mimeTypes.keyIndex(path, n + 1, path.size()));
        DirectUtf8Sequence val;
        if ((val = headers.getHeader(HEADER_RANGE)) != null) {
            sendRange(context, val, path, contentType, asAttachment);
            return;
        }

        int l;
        // attempt not to send file when remote side already has
        // up-to-date version of the same
        if (
                (val = headers.getHeader(HEADER_IF_NONE_MATCH)) != null
                        && (l = val.size()) > 2
                        && val.byteAt(0) == '"'
                        && val.byteAt(l - 1) == '"'
        ) {
            try {
                long that = Numbers.parseLong(val, 1, l - 1);
                if (that == ff.getLastModified(path)) {
                    context.simpleResponse().sendStatusNoContent(304);
                    return;
                }
            } catch (NumericException e) {
                LOG.info().$("bad 'If-None-Match' [value=").$(val).$(']').$();
                sendStatusTextContent(context, 400);
                return;
            }
        }

        sendVanilla(context, path, contentType, asAttachment);
    }

    private void sendRange(
            HttpConnectionContext context,
            DirectUtf8Sequence range,
            LPSZ path,
            CharSequence contentType,
            boolean asAttachment
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (rangeParser.of(range)) {
            StaticContentProcessorState state = LV.get(context);
            if (state == null) {
                LV.set(context, state = new StaticContentProcessorState());
            }

            state.fd = ff.openRO(path);
            if (state.fd == -1) {
                LOG.info().$("Cannot open file: ").$(path).$();
                sendStatusTextContent(context, 404);
                return;
            }

            state.bytesSent = 0;

            final long length = ff.length(path);
            final long lo = rangeParser.getLo();
            final long hi = rangeParser.getHi();
            if (lo > length || (hi != Long.MAX_VALUE && hi > length) || lo > hi) {
                sendStatusTextContent(context, 416);
            } else {
                state.bytesSent = lo;
                state.sendMax = hi == Long.MAX_VALUE ? length : hi;

                final HttpResponseHeader header = context.getResponseHeader();
                header.status(httpProtocolVersion, 206, contentType, state.sendMax - lo);
                if (asAttachment) {
                    header.put("Content-Disposition: attachment; filename=\"").put(FileNameExtractorUtf8Sequence.get(path)).put('\"').putEOL();
                }
                header.put("Accept-Ranges: bytes").putEOL();
                header.put("Content-Range: bytes ").put(lo).put('-').put(state.sendMax).put('/').put(length).putEOL();
                header.put("ETag: ").put(ff.getLastModified(path)).putEOL();
                if (keepAliveHeader != null) {
                    header.put(keepAliveHeader);
                }
                header.send();
                resumeSend(context);
            }
        } else {
            sendStatusTextContent(context, 416);
        }
    }

    private void sendVanilla(
            HttpConnectionContext context,
            LPSZ path,
            CharSequence contentType,
            boolean asAttachment
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        int fd = ff.openRO(path);
        if (fd == -1) {
            LOG.info().$("Cannot open file: ").$(path).$('(').$(ff.errno()).$(')').$();
            sendStatusTextContent(context, 404);
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
            header.status(httpProtocolVersion, 200, contentType, length);
            if (asAttachment) {
                header.put("Content-Disposition: attachment; filename=\"").put(FileNameExtractorUtf8Sequence.get(path)).put("\"").putEOL();
            }
            header.put("ETag: ").put('"').put(ff.getLastModified(path)).put('"').putEOL();
            header.setKeepAlive(keepAliveHeader);
            header.send();
            resumeSend(context);
        }
    }
}

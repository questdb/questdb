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

package com.questdb.net.http.handlers;

import com.questdb.BootstrapEnv;
import com.questdb.ServerConfiguration;
import com.questdb.common.NumericException;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.net.http.*;
import com.questdb.std.*;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.FileNameExtractorCharSequence;
import com.questdb.std.str.LPSZ;
import com.questdb.std.str.PrefixedPath;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ThreadLocal;
import java.nio.ByteBuffer;

public class StaticContentHandler implements ContextHandler {

    private static final Log LOG = LogFactory.getLog(StaticContentHandler.class);

    private final MimeTypes mimeTypes;
    private final ThreadLocal<PrefixedPath> tlPrefixedPath = new ThreadLocal<>();
    private final ThreadLocal<RangeParser> tlRangeParser = new ThreadLocal<>();

    private final LocalValue<FileDescriptorHolder> lvFd = new LocalValue<>();
    private final ServerConfiguration configuration;

    public StaticContentHandler(BootstrapEnv env) throws IOException {
        this.configuration = env.configuration;
        this.mimeTypes = new MimeTypes(configuration.getMimeTypes());
    }

    @Override
    public void handle(IOContext context) throws IOException {

        CharSequence url = context.request.getUrl();
        LOG.info().$("handling static: ").$(url).$();
        if (Chars.contains(url, "..")) {
            LOG.info().$("URL abuse: ").$(url).$();
            context.simpleResponse().send(404);
        } else {
            PrefixedPath path = tlPrefixedPath.get().rewind();

            if (Chars.equals(url, '/')) {
                path.concat(configuration.getHttpIndexFile());
            } else {
                path.concat(url);
            }

            path.$();

            if (Files.exists(path)) {
                send(context, path, context.request.getUrlParam("attachment") != null);
            } else {
                LOG.info().$("Not found: ").$(path).$();
                context.simpleResponse().send(404);
            }
        }
    }

    public void resume(IOContext context) throws IOException {
        FileDescriptorHolder h = lvFd.get(context);

        if (h == null || h.fd == -1) {
            return;
        }

        FragmentedResponse r = context.fixedSizeResponse();
        ByteBuffer out = r.out();
        long wptr = ByteBuffers.getAddress(out);
        int sz = out.remaining();

        long l;
        while (h.bytesSent < h.sendMax && (l = Files.read(h.fd, wptr, sz, h.bytesSent)) > 0) {
            if (l + h.bytesSent > h.sendMax) {
                l = h.sendMax - h.bytesSent;
            }
            out.limit((int) l);
            h.bytesSent += l;
            r.sendChunk();
        }
        r.done();
        // reached the end naturally?
        h.clear();
    }

    @Override
    public void setupThread() {
        tlRangeParser.set(RangeParser.FACTORY.newInstance());
        tlPrefixedPath.set(new PrefixedPath(configuration.getHttpPublic().getAbsolutePath()));
    }

    private void send(IOContext context, LPSZ path, boolean asAttachment) throws IOException {
        int n = Chars.lastIndexOf(path, '.');
        if (n == -1) {
            LOG.info().$("Missing extension: ").$(path).$();
            context.simpleResponse().send(404);
            return;
        }

        CharSequence contentType = mimeTypes.valueAt(mimeTypes.keyIndex(path, n + 1, path.length()));
        CharSequence val;
        if ((val = context.request.getHeader("Range")) != null) {
            sendRange(context, val, path, contentType, asAttachment);
            return;
        }

        int l;
        if ((val = context.request.getHeader("If-None-Match")) != null
                && (l = val.length()) > 2
                && val.charAt(0) == '"'
                && val.charAt(l - 1) == '"') {
            try {
                long that = Numbers.parseLong(val, 1, l - 1);
                if (that == Files.getLastModified(path)) {
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

    private void sendRange(IOContext context, CharSequence range, LPSZ path, CharSequence contentType, boolean asAttachment) throws IOException {
        RangeParser rangeParser = tlRangeParser.get();
        if (rangeParser.of(range)) {

            FileDescriptorHolder h = lvFd.get(context);
            if (h == null) {
                lvFd.set(context, h = new FileDescriptorHolder());
            }

            h.fd = Files.openRO(path);
            if (h.fd == -1) {
                LOG.info().$("Cannot open file: ").$(path).$();
                context.simpleResponse().send(404);
                return;
            }

            h.bytesSent = 0;

            final long length = Files.length(path);
            final long lo = rangeParser.getLo();
            final long hi = rangeParser.getHi();
            if (lo > length || (hi != Long.MAX_VALUE && hi > length) || lo > hi) {
                context.simpleResponse().send(416);
            } else {
                h.bytesSent = lo;
                h.sendMax = hi == Long.MAX_VALUE ? length : hi;

                final FixedSizeResponse r = context.fixedSizeResponse();

                r.status(206, contentType, h.sendMax - lo);

                final CharSink sink = r.headers();

                if (asAttachment) {
                    sink.put("Content-Disposition: attachment; filename=\"").put(FileNameExtractorCharSequence.get(path)).put('\"').put(Misc.EOL);
                }
                sink.put("Accept-Ranges: bytes").put(Misc.EOL);
                sink.put("Content-Range: bytes ").put(lo).put('-').put(h.sendMax).put('/').put(length).put(Misc.EOL);
                sink.put("ETag: ").put(Files.getLastModified(path)).put(Misc.EOL);
                r.sendHeader();
                resume(context);
            }
        } else {
            context.simpleResponse().send(416);
        }
    }

    private void sendVanilla(IOContext context, LPSZ path, CharSequence contentType, boolean asAttachment) throws IOException {
        long fd = Files.openRO(path);
        if (fd == -1) {
            LOG.info().$("Cannot open file: ").$(path).$('(').$(Os.errno()).$(')').$();
            context.simpleResponse().send(404);
        } else {
            FileDescriptorHolder h = lvFd.get(context);
            if (h == null) {
                lvFd.set(context, h = new FileDescriptorHolder());
            }
            h.fd = fd;
            h.bytesSent = 0;
            final long length = Files.length(path);
            h.sendMax = Long.MAX_VALUE;

            final FixedSizeResponse r = context.fixedSizeResponse();
            r.status(200, contentType, length);
            if (asAttachment) {
                r.headers().put("Content-Disposition: attachment; filename=\"").put(FileNameExtractorCharSequence.get(path)).put("\"").put(Misc.EOL);
            }
            r.headers().put("ETag: ").put('"').put(Files.getLastModified(path)).put('"').put(Misc.EOL);
            r.sendHeader();
            resume(context);
        }
    }

    private static class FileDescriptorHolder implements Mutable, Closeable {
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

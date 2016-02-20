/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.net.http.handlers;

import com.nfsdb.ex.NumericException;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.misc.*;
import com.nfsdb.net.http.*;
import com.nfsdb.std.*;
import com.nfsdb.std.ThreadLocal;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class StaticContentHandler implements ContextHandler {

    private final MimeTypes mimeTypes;
    private final ThreadLocal<PrefixedPath> tlPrefixedPath;
    private final ThreadLocal<RangeParser> tlRangeParser = new ThreadLocal<>(RangeParser.FACTORY);
    private final LocalValue<FileDescriptorHolder> lvFd = new LocalValue<>();

    public StaticContentHandler(final File publicDir, MimeTypes mimeTypes) {
        this.mimeTypes = mimeTypes;
        this.tlPrefixedPath = new ThreadLocal<>(new ObjectFactory<PrefixedPath>() {
            @Override
            public PrefixedPath newInstance() {
                return new PrefixedPath(publicDir.getAbsolutePath());
            }
        });
    }

    @Override
    public void handle(IOContext context) throws IOException {
        CharSequence url = context.request.getUrl();
        if (Chars.containts(url, "..")) {
            context.simpleResponse().send(404);
        } else {
            PrefixedPath path = tlPrefixedPath.get();
            if (Files.exists(path.of(url))) {
                send(context, path, false);
            } else {
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

    private void send(IOContext context, LPSZ path, boolean asAttachment) throws IOException {
        int n = Chars.lastIndexOf(path, '.');
        if (n == -1) {
            context.simpleResponse().send(404);
            return;
        }

        CharSequence contentType = mimeTypes.get(context.ext.of(path, n + 1, path.length() - n - 1));

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
                    //todo: extract name from path
                    sink.put("Content-Disposition: attachment; filename=\"").put(path).put('\"').put(Misc.EOL);
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
                // todo: extract name from path
                r.headers().put("Content-Disposition: attachment; filename=\"").put(path).put("\"").put(Misc.EOL);
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

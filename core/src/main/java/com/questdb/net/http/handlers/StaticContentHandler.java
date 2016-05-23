/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
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
 ******************************************************************************/

package com.questdb.net.http.handlers;

import com.questdb.ex.NumericException;
import com.questdb.misc.*;
import com.questdb.net.http.*;
import com.questdb.std.*;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.ThreadLocal;
import java.nio.ByteBuffer;

public class StaticContentHandler implements ContextHandler {

    private final MimeTypes mimeTypes;
    private final ThreadLocal<PrefixedPath> tlPrefixedPath = new ThreadLocal<>();
    private final ThreadLocal<RangeParser> tlRangeParser = new ThreadLocal<>();
    private final LocalValue<FileDescriptorHolder> lvFd = new LocalValue<>();
    private final File publicDir;

    public StaticContentHandler(final File publicDir, MimeTypes mimeTypes) {
        this.publicDir = publicDir;
        this.mimeTypes = mimeTypes;
    }

    @Override
    public void handle(IOContext context) throws IOException {
        CharSequence url = context.request.getUrl();
        if (Chars.contains(url, "..")) {
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

    @Override
    public void setupThread() {
        tlRangeParser.set(RangeParser.FACTORY.newInstance());
        tlPrefixedPath.set(new PrefixedPath(publicDir.getAbsolutePath()));
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

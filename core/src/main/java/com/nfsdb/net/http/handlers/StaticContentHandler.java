/*******************************************************************************
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
 ******************************************************************************/

package com.nfsdb.net.http.handlers;

import com.nfsdb.ex.NumericException;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.log.Log;
import com.nfsdb.log.LogFactory;
import com.nfsdb.misc.*;
import com.nfsdb.net.http.*;
import com.nfsdb.std.LPSZ;
import com.nfsdb.std.ObjectFactory;
import com.nfsdb.std.PrefixedPath;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class StaticContentHandler implements ContextHandler {

    private static final Log LOG = LogFactory.getLog(StaticContentHandler.class);

    private final MimeTypes mimeTypes;
    private final ObjectFactory<PrefixedPath> ppFactory;

    public StaticContentHandler(final File publicDir, MimeTypes mimeTypes) {
        this.mimeTypes = mimeTypes;
        this.ppFactory = new ObjectFactory<PrefixedPath>() {
            @Override
            public PrefixedPath newInstance() {
                return new PrefixedPath(publicDir.getAbsolutePath());
            }
        };
    }

    @Override
    public void handle(IOContext context) throws IOException {
        CharSequence url = context.request.getUrl();
        if (Chars.containts(url, "..")) {
            context.simpleResponse().send(404);
        } else {
            PrefixedPath path = context.getThreadLocal(IOWorkerContextKey.PP.name(), ppFactory);
            if (Files.exists(path.of(url))) {
                send(context, path, false);
            } else {
                context.simpleResponse().send(404);
            }
        }
    }

    public void resume(IOContext context) throws IOException {
        if (context.fd == -1) {
            return;
        }

        FragmentedResponse r = context.fixedSizeResponse();
        ByteBuffer out = r.out();
        long wptr = ByteBuffers.getAddress(out);
        int sz = out.remaining();

        long l;
        while (context.bytesSent < context.sendMax && (l = Files.read(context.fd, wptr, sz, context.bytesSent)) > 0) {
            if (l + context.bytesSent > context.sendMax) {
                l = context.sendMax - context.bytesSent;
            }
            out.limit((int) l);
            context.bytesSent += l;
            r.sendChunk();
        }
        r.done();

        // reached the end naturally?
        if (Files.close(context.fd) != 0) {
            LOG.error().$("Could not close file").$();
        }
        context.fd = -1;
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
        RangeParser rangeParser = context.getThreadLocal(IOWorkerContextKey.FP.name(), RangeParser.FACTORY);
        if (rangeParser.of(range)) {

            context.fd = Files.openRO(path);
            if (context.fd == -1) {
                context.simpleResponse().send(404);
                return;
            }

            context.bytesSent = 0;

            final long length = Files.length(path);
            final long l = rangeParser.getLo();
            final long h = rangeParser.getHi();
            if (l > length || (h != Long.MAX_VALUE && h > length) || l > h) {
                context.simpleResponse().send(416);
            } else {
                context.bytesSent = l;
                context.sendMax = h == Long.MAX_VALUE ? length : h;

                final FixedSizeResponse r = context.fixedSizeResponse();

                r.status(206, contentType, context.sendMax - l);

                final CharSink sink = r.headers();

                if (asAttachment) {
                    //todo: extract name from path
                    sink.put("Content-Disposition: attachment; filename=\"").put(path).put('\"').put(Misc.EOL);
                }
                sink.put("Accept-Ranges: bytes").put(Misc.EOL);
                sink.put("Content-Range: bytes ").put(l).put('-').put(context.sendMax).put('/').put(length).put(Misc.EOL);
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
            context.fd = fd;
            context.bytesSent = 0;
            final long length = Files.length(path);
            context.sendMax = Long.MAX_VALUE;

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
}

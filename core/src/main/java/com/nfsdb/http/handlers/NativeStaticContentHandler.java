/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

package com.nfsdb.http.handlers;

import com.nfsdb.collections.LPSZ;
import com.nfsdb.collections.ObjectFactory;
import com.nfsdb.collections.PrefixedPath;
import com.nfsdb.exceptions.NumericException;
import com.nfsdb.http.ContextHandler;
import com.nfsdb.http.IOContext;
import com.nfsdb.http.MimeTypes;
import com.nfsdb.http.RangeParser;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.misc.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class NativeStaticContentHandler implements ContextHandler {

    private final MimeTypes mimeTypes;
    private final ObjectFactory<PrefixedPath> ppFactory;

    public NativeStaticContentHandler(final File publicDir, MimeTypes mimeTypes) {
        this.mimeTypes = mimeTypes;
        this.ppFactory = new ObjectFactory<PrefixedPath>() {
            @Override
            public PrefixedPath newInstance() {
                return new PrefixedPath(publicDir.getAbsolutePath());
            }
        };
    }

    public void _continue(IOContext context) throws IOException {
        if (context.fd == -1) {
            return;
        }

        ByteBuffer out = context.response.getOut();
        long wptr = ByteBuffers.getAddress(out);
        int sz = out.remaining();

        long l;
        while ((l = Files.read(context.fd, wptr, sz, context.bytesSent)) > 0) {
            if (l + context.bytesSent > context.sendMax) {
                l = context.sendMax - context.bytesSent;
                out.limit((int) l);
                // do not refactor, placement is critical
                context.bytesSent += l;
                context.response.sendBody();
                break;
            } else {
                // do not refactor, placement is critical
                out.limit((int) l);
                context.bytesSent += l;
                context.response.sendBody();
            }
        }
        Files.close(context.fd);
        context.fd = -1;
    }

    @Override
    public void handle(IOContext context) throws IOException {
        CharSequence url = context.request.getUrl();
        if (Chars.containts(url, "..")) {
            context.response.simple(404);
        } else {
            PrefixedPath path = context.getThreadLocal(IOWorkerContextKey.PP.name(), ppFactory);
            if (Files.exists(path.of(url))) {
                send(context, path, false);
            } else {
                context.response.simple(404);
            }
        }
    }

    public void send(IOContext context, LPSZ path, boolean asAttachment) throws IOException {
        int n = Chars.lastIndexOf(path, '.');
        if (n == -1) {
            context.response.simple(404);
            return;
        }

        CharSequence contentType = mimeTypes.get(context.ext.of(path, n + 1, path.length() - n - 1));

        CharSequence val;
        if ((val = context.request.getHeader("Range")) != null) {
            sendRange(context, val, path, contentType, asAttachment);
            return;
        }

        int l;
        if ((val = context.request.getHeader("If-None-Match")) != null) {
            if ((l = val.length()) > 2 && val.charAt(0) == '"' && val.charAt(l - 1) == '"') {
                try {
                    long that = Numbers.parseLong(val, 1, l - 1);
                    if (that == Files.getLastModified(path)) {
                        context.response.status(304, null, -2);
                        context.response.sendHeader();
                        context.response.end();
                        return;
                    }
                } catch (NumericException e) {
                    context.response.simple(400);
                    return;
                }
            }
        }

        sendVanilla(context, path, contentType, asAttachment);
    }

    private void sendRange(IOContext context, CharSequence range, LPSZ path, CharSequence contentType, boolean asAttachment) throws IOException {
        RangeParser rangeParser = context.getThreadLocal(IOWorkerContextKey.FP.name(), RangeParser.FACTORY);
        if (rangeParser.of(range)) {

            context.fd = Files.openRO(path);
            if (context.fd == -1) {
                context.response.simple(404);
                return;
            }

            context.bytesSent = 0;

            final long length = Files.length(path);
            final long l = rangeParser.getLo();
            final long h = rangeParser.getHi();
            if (l > length || (h != Long.MAX_VALUE && h > length) || l > h) {
                context.response.simple(416);
            } else {
                context.bytesSent = l;
                context.sendMax = h == Long.MAX_VALUE ? length : h;
                context.response.status(206, contentType, context.sendMax - l);

                final CharSink sink = context.response.headers();

                if (asAttachment) {
                    //todo: extract name from path
                    sink.put("Content-Disposition: attachment; filename=\"").put(path).put('\"').put(Misc.EOL);
                }
                sink.put("Accept-Ranges: bytes").put(Misc.EOL);
                sink.put("Content-Range: bytes ").put(l).put('-').put(context.sendMax).put('/').put(length).put(Misc.EOL);
                sink.put("ETag: ").put(Files.getLastModified(path)).put(Misc.EOL);
                context.response.sendHeader();
                _continue(context);
            }
        } else {
            context.response.simple(416);
        }
    }

    private void sendVanilla(IOContext context, LPSZ path, CharSequence contentType, boolean asAttachment) throws IOException {
        long fd = Files.openRO(path);
        if (fd == -1) {
            context.response.simple(404);
        } else {
            context.fd = fd;
            context.bytesSent = 0;
            final long length = Files.length(path);
            context.sendMax = Long.MAX_VALUE;
            context.response.status(200, contentType, length);
            if (asAttachment) {
                // todo: extract name from path
                context.response.headers().put("Content-Disposition: attachment; filename=\"").put(path).put("\"").put(Misc.EOL);
            }
            context.response.headers().put("ETag: ").put('"').put(Files.getLastModified(path)).put('"').put(Misc.EOL);
            context.response.sendHeader();
            _continue(context);
        }
    }
}

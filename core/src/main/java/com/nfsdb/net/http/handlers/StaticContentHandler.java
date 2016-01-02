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

package com.nfsdb.net.http.handlers;

import com.nfsdb.exceptions.NumericException;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.misc.Chars;
import com.nfsdb.misc.Misc;
import com.nfsdb.misc.Numbers;
import com.nfsdb.net.http.ContextHandler;
import com.nfsdb.net.http.IOContext;
import com.nfsdb.net.http.MimeTypes;
import com.nfsdb.net.http.RangeParser;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class StaticContentHandler implements ContextHandler {

    private final File publicDir;
    private final MimeTypes mimeTypes;

    public StaticContentHandler(File publicDir, MimeTypes mimeTypes) {
        this.publicDir = publicDir;
        this.mimeTypes = mimeTypes;
    }

    public void _continue(IOContext context) throws IOException {
        if (context.raf == null) {
            return;
        }
        FileChannel ch = context.raf.getChannel();
        ByteBuffer out = context.response.getOut();

        while (ch.read(out) > 0) {
            out.flip();
            int l = out.remaining();
            if (l + context.bytesSent > context.sendMax) {
                l = (int) (context.sendMax - context.bytesSent);
                out.limit(l);
                // do not refactor, placement is critical
                context.bytesSent += l;
                context.response.sendBody();
                break;
            } else {
                // do not refactor, placement is critical
                context.bytesSent += l;
                context.response.sendBody();
            }
        }
        context.raf = Misc.free(context.raf);
    }

    @Override
    public void handle(IOContext context) throws IOException {
        CharSequence url = context.request.getUrl();
        if (Chars.containts(url, "..")) {
            context.response.simple(404);
        } else {
            File file = new File(publicDir, url.toString());
            if (file.exists()) {
                send(context, file, false);
            } else {
                context.response.simple(404);
            }
        }
    }

    public void send(IOContext context, File file, boolean asAttachment) throws IOException {
        CharSequence name = file.getName();

        int n = Chars.lastIndexOf(name, '.');
        if (n == -1) {
            context.response.simple(404);
            return;
        }

        CharSequence contentType = mimeTypes.get(context.ext.of(name, n + 1, name.length() - n - 1));

        CharSequence val;
        if ((val = context.request.getHeader("Range")) != null) {
            sendRange(context, val, file, contentType, asAttachment);
            return;
        }

        int l;
        if ((val = context.request.getHeader("If-None-Match")) != null) {
            if ((l = val.length()) > 2 && val.charAt(0) == '"' && val.charAt(l - 1) == '"') {
                try {
                    long that = Numbers.parseLong(val, 1, l - 1);
                    if (that == file.lastModified()) {
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

        sendVanilla(context, file, contentType, asAttachment);
    }

    private void sendRange(IOContext context, CharSequence range, File file, CharSequence contentType, boolean asAttachment) throws IOException {
        RangeParser rangeParser = context.getThreadLocal(IOWorkerContextKey.FP.name(), RangeParser.FACTORY);
        if (rangeParser.of(range)) {
            context.raf = new RandomAccessFile(file, "r");
            context.bytesSent = 0;

            final long length = context.raf.length();
            final long l = rangeParser.getLo();
            final long h = rangeParser.getHi();
            if (l > length || (h != Long.MAX_VALUE && h > length) || l > h) {
                context.response.simple(416);
            } else {
                context.raf.seek(l);
                context.sendMax = h == Long.MAX_VALUE ? length : h;
                context.response.status(206, contentType, context.sendMax - l);

                final CharSink sink = context.response.headers();

                if (asAttachment) {
                    sink.put("Content-Disposition: attachment; filename=\"").put(file.getName()).put('\"').put(Misc.EOL);
                }
                sink.put("Accept-Ranges: bytes").put(Misc.EOL);
                sink.put("Content-Range: bytes ").put(l).put('-').put(context.sendMax).put('/').put(length).put(Misc.EOL);
                sink.put("ETag: ").put(file.lastModified()).put(Misc.EOL);
                context.response.sendHeader();
                _continue(context);
            }
        } else {
            context.response.simple(416);
        }
    }

    private void sendVanilla(IOContext context, File file, CharSequence contentType, boolean asAttachment) throws IOException {
        context.raf = new RandomAccessFile(file, "r");
        context.bytesSent = 0;
        final long length = context.raf.length();
        context.sendMax = Long.MAX_VALUE;
        context.response.status(200, contentType, length);
        if (asAttachment) {
            context.response.headers().put("Content-Disposition: attachment; filename=\"").put(file.getName()).put("\"").put(Misc.EOL);
        }
        context.response.headers().put("ETag: ").put('"').put(file.lastModified()).put('"').put(Misc.EOL);
        context.response.sendHeader();
        _continue(context);
    }
}

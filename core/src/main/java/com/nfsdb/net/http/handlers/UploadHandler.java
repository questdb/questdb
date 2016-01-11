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

import com.nfsdb.JournalMode;
import com.nfsdb.collections.ByteSequence;
import com.nfsdb.collections.DirectByteCharSequence;
import com.nfsdb.misc.Misc;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.net.http.IOContext;
import com.nfsdb.net.http.RequestHeaderBuffer;
import com.nfsdb.net.http.ResponseSink;
import com.nfsdb.storage.PlainFile;

import java.io.File;
import java.io.IOException;

public class UploadHandler extends AbstractMultipartHandler {
    private final File path;

    public UploadHandler(File path) {
        this.path = path;
    }

    @Override
    protected void onComplete0(IOContext context) throws IOException {
        ResponseSink sink = context.responseSink();
        sink.status(200, "text/html; charset=utf-8");
        sink.put("OK, got it").put(Misc.EOL);
        sink.flush();
    }

    @Override
    protected void onData(IOContext context, RequestHeaderBuffer hb, ByteSequence data) {
        if (context.mf != null) {
            PlainFile mf = context.mf;

            int len = data.length();
            long mapAddr = mf.addressOf(context.wptr);
            int mapLen = mf.pageRemaining(context.wptr);
            if (len < mapLen) {
                write0(data, 0, mapAddr, len);
                context.wptr += len;
            } else {
                int p = 0;
                while (true) {
                    write0(data, p, mapAddr, mapLen);
                    context.wptr += mapLen;
                    len -= mapLen;
                    p += mapLen;

                    if (len > 0) {
                        mapAddr = mf.addressOf(context.wptr);
                        mapLen = mf.pageRemaining(context.wptr);
                        if (len < mapLen) {
                            mapLen = len;
                        }
                    } else {
                        break;
                    }
                }
            }
        }
    }

    @Override
    protected void onPartBegin(IOContext context, RequestHeaderBuffer hb) throws IOException {
        CharSequence file = hb.getContentDispositionFilename();
        if (file != null) {
            try {
                context.mf = new PlainFile(new File(path, file.toString()), 21, JournalMode.APPEND);
            } catch (IOException ignore) {
                sendError(context);
            }
        }
    }

    @Override
    protected void onPartEnd(IOContext context) throws IOException {
        if (context.mf != null) {
            context.mf.compact(context.wptr);
            context.mf = null;
            context.wptr = 0;
        }
    }

    private void sendError(IOContext context) throws IOException {
        ResponseSink sink = context.responseSink();
        sink.status(200, "text/html; charset=utf-8");
        sink.put("OOPS").put(Misc.EOL);
        sink.flush();
    }

    private void write0(ByteSequence data, int offset, long addr, int len) {
        if (data instanceof DirectByteCharSequence) {
            Unsafe.getUnsafe().copyMemory(((DirectByteCharSequence) data).getLo() + offset, addr, len);
        } else {
            for (int i = offset; i < len; i++) {
                Unsafe.getUnsafe().putByte(addr++, data.byteAt(i));
            }
        }
    }
}

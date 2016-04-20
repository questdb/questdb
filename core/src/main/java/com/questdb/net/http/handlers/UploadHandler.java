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

import com.questdb.JournalMode;
import com.questdb.misc.Misc;
import com.questdb.misc.Unsafe;
import com.questdb.net.http.IOContext;
import com.questdb.net.http.RequestHeaderBuffer;
import com.questdb.net.http.ResponseSink;
import com.questdb.std.ByteSequence;
import com.questdb.std.DirectByteCharSequence;
import com.questdb.std.LocalValue;
import com.questdb.std.Mutable;
import com.questdb.store.PlainFile;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public class UploadHandler extends AbstractMultipartHandler {
    private final File path;
    private final LocalValue<UploadContext> lvContext = new LocalValue<>();

    public UploadHandler(File path) {
        this.path = path;
    }

    @Override
    public void setup(IOContext context) {
        UploadContext h = lvContext.get(context);
        if (h == null) {
            lvContext.set(context, new UploadContext());
        }
    }

    @Override
    protected void onComplete0(IOContext context) throws IOException {
        ResponseSink sink = context.responseSink();
        sink.status(200, "text/html; charset=utf-8");
        sink.put("OK, got it").put(Misc.EOL);
        sink.flush();
    }

    @Override
    protected void onData(IOContext context, ByteSequence data) {
        UploadContext h = lvContext.get(context);
        if (h != null && h.mf != null) {
            PlainFile mf = h.mf;

            int len = data.length();
            long mapAddr = mf.addressOf(h.wptr);
            int mapLen = mf.pageRemaining(h.wptr);
            if (len < mapLen) {
                write0(data, 0, mapAddr, len);
                h.wptr += len;
            } else {
                int p = 0;
                while (true) {
                    write0(data, p, mapAddr, mapLen);
                    h.wptr += mapLen;
                    len -= mapLen;
                    p += mapLen;

                    if (len > 0) {
                        mapAddr = mf.addressOf(h.wptr);
                        mapLen = mf.pageRemaining(h.wptr);
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
                UploadContext h = lvContext.get(context);
                h.mf = new PlainFile(new File(path, file.toString()), 21, JournalMode.APPEND);
            } catch (IOException ignore) {
                sendError(context);
            }
        }
    }

    @Override
    protected void onPartEnd(IOContext context) throws IOException {
        UploadContext h = lvContext.get(context);
        if (h != null && h.mf != null) {
            h.mf.compact(h.wptr);
            h.clear();
        }
    }

    private void sendError(IOContext context) throws IOException {
        ResponseSink sink = context.responseSink();
        sink.status(200, "text/html; charset=utf-8");
        sink.put("OOPS").put(Misc.EOL);
        sink.flush();
    }

    private void write0(ByteSequence data, int offset, final long addr, int len) {
        if (data instanceof DirectByteCharSequence) {
            Unsafe.getUnsafe().copyMemory(((DirectByteCharSequence) data).getLo() + offset, addr, len);
        } else {
            long p = addr;
            for (int i = offset; i < len; i++) {
                Unsafe.getUnsafe().putByte(p++, data.byteAt(i));
            }
        }
    }

    private static class UploadContext implements Mutable, Closeable {
        private long wptr = 0;
        private PlainFile mf;

        @Override
        public void clear() {
            mf = Misc.free(mf);
            wptr = 0;
        }

        @Override
        public void close() {
            clear();
        }
    }
}

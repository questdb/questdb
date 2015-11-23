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
import com.nfsdb.misc.Unsafe;
import com.nfsdb.net.http.*;
import com.nfsdb.storage.PlainFile;

import java.io.File;
import java.io.IOException;

public class DummyFileUploadHandler implements ContextHandler, MultipartListener {
    private final File path;
    private PlainFile mf;
    private Response response;
    private long wptr = 0;

    public DummyFileUploadHandler(File path) {
        this.path = path;
    }

    @Override
    public void onChunk(RequestHeaderBuffer hb, ByteSequence data, boolean continued) throws IOException {
        if (hb.getContentDispositionFilename() != null) {
            if (!continued) {
                closeFile();
                openFile(hb.getContentDispositionFilename());
            }
            write(data);
        } else {
            closeFile();
        }
    }

    @Override
    public void onComplete() throws IOException {
        closeFile();
        response.status(200, "text/html; charset=utf-8");
        response.flushHeader();
        response.write("OK, got it");
        response.end();
    }

    @Override
    public void onHeaders(Request request, Response response) {
        this.response = response;
    }

    private void closeFile() throws IOException {
        if (this.mf != null) {
            mf.compact(wptr);
            mf = null;
            wptr = 0;
        }
    }

    private void openFile(CharSequence name) throws IOException {
        try {
            this.mf = new PlainFile(new File(path, name.toString()), 21, JournalMode.APPEND);
        } catch (IOException ignore) {
            sendError();
        }
    }

    private void sendError() throws IOException {
        response.status(200, "text/html; charset=utf-8");
        response.flushHeader();
        response.write("OOPS");
        response.end();
    }

    private void write(ByteSequence data) {
        int len = data.length();
        long mapAddr = mf.addressOf(wptr);
        int mapLen = mf.pageRemaining(wptr);
        if (len < mapLen) {
            write0(data, 0, mapAddr, len);
            wptr += len;
        } else {
            int p = 0;
            while (true) {
                write0(data, p, mapAddr, mapLen);
                wptr += mapLen;
                len -= mapLen;
                p += mapLen;

                if (len > 0) {
                    mapAddr = mf.addressOf(wptr);
                    mapLen = mf.pageRemaining(wptr);
                    if (len < mapLen) {
                        mapLen = len;
                    }
                } else {
                    break;
                }
            }
        }
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

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

package com.nfsdb.net.http;

import com.nfsdb.misc.ByteBuffers;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.std.CharSequenceObjHashMap;
import com.nfsdb.std.DirectByteCharSequence;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public final class MimeTypes extends CharSequenceObjHashMap<CharSequence> implements Closeable {
    private ByteBuffer buf;

    public MimeTypes(File file) throws IOException {
        try (FileInputStream fis = new FileInputStream(file)) {
            int sz;
            buf = ByteBuffer.allocateDirect(sz = fis.available());

            fis.getChannel().read(buf);

            long p = ByteBuffers.getAddress(buf);
            long hi = p + sz;
            long _lo = p;

            boolean newline = true;
            boolean comment = false;

            CharSequence contentType = null;

            while (p < hi) {
                char b = (char) Unsafe.getUnsafe().getByte(p++);

                switch (b) {
                    case '#':
                        comment = newline;
                        break;
                    case ' ':
                    case '\t':
                        if (!comment) {
                            if (newline || _lo == p - 1) {
                                _lo = p;
                            } else {
                                DirectByteCharSequence s = new DirectByteCharSequence().of(_lo, p - 1);
                                _lo = p;
                                if (contentType == null) {
                                    contentType = s;
                                } else {
                                    this.put(s, contentType);
                                }
                                newline = false;
                            }
                        }
                        break;
                    case '\n':
                    case '\r':
                        newline = true;
                        comment = false;
                        if (_lo < p - 1 && contentType != null) {
                            DirectByteCharSequence s = new DirectByteCharSequence().of(_lo, p - 1);
                            this.put(s, contentType);
                        }
                        contentType = null;
                        _lo = p;
                        break;
                    default:
                        if (newline) {
                            newline = false;
                        }
                        break;
                }

            }
        }
    }

    @Override
    public void close() throws IOException {
        buf = ByteBuffers.release(buf);
    }
}

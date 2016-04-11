/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 *
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
    public void close() {
        buf = ByteBuffers.release(buf);
    }
}

/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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
 ******************************************************************************/

package com.questdb.cutlass.http;

import com.questdb.std.CharSequenceObjHashMap;
import com.questdb.std.FilesFacade;
import com.questdb.std.Transient;
import com.questdb.std.Unsafe;
import com.questdb.std.str.DirectByteCharSequence;
import com.questdb.std.str.Path;

public final class MimeTypesCache extends CharSequenceObjHashMap<CharSequence> {

    public MimeTypesCache(@Transient FilesFacade ff, @Transient Path path) {

        long fd = ff.openRO(path);
        if (fd < 0) {
            throw HttpException.instance("could not open [file=").put(path).put(']');
        }

        final long fileSize = ff.length(fd);
        if (fileSize < 1 || fileSize > 1024 * 1024L) {
            ff.close(fd);
            throw HttpException.instance("wrong file size [file=").put(path).put(", size=").put(fileSize).put(']');
        }

        long buffer = Unsafe.malloc(fileSize);
        long read = ff.read(fd, buffer, fileSize, 0);
        try {
            if (read != fileSize) {
                Unsafe.free(buffer, fileSize);
                throw HttpException.instance("could not read [file=").put(path).put(", size=").put(fileSize).put(", read=").put(read).put(", errno=").put(ff.errno()).put(']');
            }
        } finally {
            ff.close(fd);
        }

        final DirectByteCharSequence dbcs = new DirectByteCharSequence();
        try {

            long p = buffer;
            long hi = p + fileSize;
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
                                String s = dbcs.of(_lo, p - 1).toString();
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
                            String s = dbcs.of(_lo, p - 1).toString();
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
        } finally {
            Unsafe.free(buffer, fileSize);
        }
    }
}

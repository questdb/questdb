/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cutlass.http;

import io.questdb.std.*;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.TestOnly;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class MimeTypesCache extends CharSequenceObjHashMap<CharSequence> {

    @TestOnly
    public MimeTypesCache(InputStream inputStream) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line = reader.readLine().trim();
            String regex = "\\t+";
            Pattern pattern = Pattern.compile(regex);

            while (line != null) {
                if (line.length() > 0 && line.charAt(0) != '#') {
                    Matcher matcher = pattern.matcher(line);
                    String l = matcher.replaceAll("\t");
                    String[] tuple = l.split("\t");
                    final String type = tuple[0].trim();
                    String[] suffixTuple = tuple[1].split(" ");
                    for (int i = 0, n = suffixTuple.length; i < n; i++) {
                        this.put(suffixTuple[i].trim(), type);
                    }
                }
                line = reader.readLine();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public MimeTypesCache(@Transient FilesFacade ff, @Transient Path path) {
        final int fd = ff.openRO(path);
        if (fd < 0) {
            throw HttpException.instance("could not open [file=").put(path).put(", errno=").put(ff.errno()).put(']');
        }

        final long fileSize = ff.length(fd);
        if (fileSize < 1 || fileSize > 1024 * 1024L) {
            ff.close(fd);
            throw HttpException.instance("wrong file size [file=").put(path).put(", size=").put(fileSize).put(']');
        }

        long buffer = Unsafe.malloc(fileSize, MemoryTag.NATIVE_HTTP_CONN);
        long read = ff.read(fd, buffer, fileSize, 0);
        try {
            if (read != fileSize) {
                Unsafe.free(buffer, fileSize, MemoryTag.NATIVE_HTTP_CONN);
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
                                String s = Chars.toString(dbcs.of(_lo, p - 1));
                                _lo = p;
                                if (contentType == null) {
                                    contentType = s;
                                } else {
                                    this.put(s, contentType);
                                }
                            }
                        }
                        break;
                    case '\n':
                    case '\r':
                        newline = true;
                        comment = false;
                        if (_lo < p - 1 && contentType != null) {
                            String s = Chars.toString(dbcs.of(_lo, p - 1));
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

            // if there is no new line after last entry we may have this
            if (contentType != null && _lo < p) {
                String s = Chars.toString(dbcs.of(_lo, p));
                this.put(s, contentType);
            }

        } finally {
            Unsafe.free(buffer, fileSize, MemoryTag.NATIVE_HTTP_CONN);
        }
    }
}

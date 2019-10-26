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

package io.questdb.std.str;

import io.questdb.std.Files;
import io.questdb.std.Unsafe;

import java.io.Closeable;

public final class StdoutSink extends AbstractCharSink implements Closeable {

    public static final StdoutSink INSTANCE = new StdoutSink();

    private final long stdout = Files.getStdOutFd();
    private final int bufferCapacity = 1024;
    private final long buffer = Unsafe.malloc(bufferCapacity);
    private final long limit = buffer + bufferCapacity;
    private long ptr = buffer;

    @Override
    public void close() {
        free();
    }

    @Override
    public void flush() {
        int len = (int) (ptr - buffer);
        if (len > 0) {
            Files.append(stdout, buffer, len);
            ptr = buffer;
        }
    }

    @Override
    public CharSink put(CharSequence cs) {
        if (cs != null) {
            for (int i = 0, len = cs.length(); i < len; i++) {
                put(cs.charAt(i));
            }
        }
        return this;
    }

    @Override
    public CharSink put(char c) {
        if (ptr == limit) {
            flush();
        }
        Unsafe.getUnsafe().putByte(ptr++, (byte) c);
        return this;
    }

    private void free() {
        Unsafe.free(buffer, bufferCapacity);
    }
}

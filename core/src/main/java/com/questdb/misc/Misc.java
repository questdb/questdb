/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
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
 ******************************************************************************/

package com.questdb.misc;

import com.questdb.ex.FatalError;
import com.questdb.ex.NumericException;
import com.questdb.std.*;
import com.questdb.std.ThreadLocal;

import java.io.Closeable;
import java.io.IOException;

public final class Misc {
    public static final String EOL = "\r\n";
    private final static ThreadLocal<StringBuilder> tlBuilder = new ThreadLocal<>(new ObjectFactory<StringBuilder>() {
        @Override
        public StringBuilder newInstance() {
            return new StringBuilder();
        }
    });

    private Misc() {
    }

    @SuppressWarnings("SameReturnValue")
    public static <T> T free(T object) {
        if (object instanceof Closeable) {
            try {
                ((Closeable) object).close();
            } catch (IOException e) {
                throw new FatalError(e);
            }
        }
        return null;
    }

    public static StringBuilder getThreadLocalBuilder() {
        StringBuilder b = tlBuilder.get();
        b.setLength(0);
        return b;
    }

    public static int urlDecode(long lo, long hi, CharSequenceObjHashMap<CharSequence> map, ObjectPool<DirectByteCharSequence> pool) {
        long _lo = lo;
        long rp = lo;
        long wp = lo;
        final DirectByteCharSequence temp = pool.next();
        int offset = 0;

        CharSequence name = null;

        while (rp < hi) {
            char b = (char) Unsafe.getUnsafe().getByte(rp++);

            switch (b) {
                case '=':
                    if (_lo < wp) {
                        name = pool.next().of(_lo, wp);
                    }
                    _lo = rp - offset;
                    break;
                case '&':
                    if (name != null) {
                        map.put(name, pool.next().of(_lo, wp));
                        name = null;
                    }
                    _lo = rp - offset;
                    break;
                case '+':
                    Unsafe.getUnsafe().putByte(wp++, (byte) ' ');
                    continue;
                case '%':
                    try {
                        if (rp + 1 < hi) {
                            Unsafe.getUnsafe().putByte(wp++, (byte) Numbers.parseHexInt(temp.of(rp, rp += 2)));
                            offset += 2;
                            continue;
                        }
                    } catch (NumericException ignore) {
                    }
                    name = null;
                    break;
                default:
                    break;
            }
            Unsafe.getUnsafe().putByte(wp++, (byte) b);
        }

        if (_lo < wp && name != null) {
            map.put(name, pool.next().of(_lo, wp));
        }

        return offset;
    }
}

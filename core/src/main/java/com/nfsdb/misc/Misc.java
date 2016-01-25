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

package com.nfsdb.misc;

import com.nfsdb.collections.CharSequenceObjHashMap;
import com.nfsdb.collections.DirectByteCharSequence;
import com.nfsdb.collections.ObjectPool;
import com.nfsdb.exceptions.NumericException;
import com.nfsdb.logging.Logger;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Closeable;
import java.io.IOException;

public final class Misc {
    public static final String EOL = "\r\n";
    private final static Logger LOGGER = Logger.getLogger(Misc.class);

    private Misc() {
    }

    @SuppressFBWarnings("CFS_CONFUSING_FUNCTION_SEMANTICS")
    public static <T> T free(T object) {
        if (object instanceof Closeable) {
            try {
                ((Closeable) object).close();
                return null;
            } catch (IOException e) {
                LOGGER.error("Failed to close", e);
            }
        }
        return object;
    }

    public static void urlDecode(long lo, long hi, CharSequenceObjHashMap<CharSequence> map, ObjectPool<DirectByteCharSequence> pool) {
        long _lo = lo;
        long rp = lo;
        long wp = lo;
        final DirectByteCharSequence temp = pool.next();

        CharSequence name = null;

        while (rp < hi) {
            char b = (char) Unsafe.getUnsafe().getByte(rp++);

            switch (b) {
                case '=':
                    if (_lo < wp) {
                        name = pool.next().of(_lo, wp);
                    }
                    _lo = rp;
                    wp = rp - 1;
                    break;
                case '&':
                    if (name != null) {
                        map.put(name, pool.next().of(_lo, wp));
                        name = null;
                    }
                    _lo = rp;
                    wp = rp - 1;
                    break;
                case '+':
                    Unsafe.getUnsafe().putByte(wp, (byte) ' ');
                    break;
                case '%':
                    try {
                        if (rp + 1 < hi) {
                            Unsafe.getUnsafe().putByte(wp++, (byte) Numbers.parseHexInt(temp.of(rp, rp += 2)));
                            continue;
                        }
                    } catch (NumericException ignore) {
                    }
                    name = null;
                    break;
                default:
                    Unsafe.getUnsafe().putByte(wp, (byte) b);
                    break;
            }
            wp++;
        }

        if (_lo < wp && name != null) {
            map.put(name, pool.next().of(_lo, wp));
        }
    }
}

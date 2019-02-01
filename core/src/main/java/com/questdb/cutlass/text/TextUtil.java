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

package com.questdb.cutlass.text;

import com.questdb.std.Unsafe;
import com.questdb.std.str.CharSink;

import static com.questdb.std.Chars.utf8DecodeMultiByte;

public class TextUtil {
    public static void utf8Decode(long lo, long hi, CharSink sink) throws Utf8Exception {
        long p = lo;
        int quoteCount = 0;

        while (p < hi) {
            byte b = Unsafe.getUnsafe().getByte(p);
            if (b < 0) {
                int n = utf8DecodeMultiByte(p, hi, b, sink);
                if (n == -1) {
                    // UTF8 error
                    throw Utf8Exception.INSTANCE;
                }
                p += n;
            } else {
                if (b == '"') {
                    if (quoteCount++ % 2 == 0) {
                        sink.put('"');
                    }
                } else {
                    sink.put((char) b);
                }
                ++p;
            }
        }
    }
}

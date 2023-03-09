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

package io.questdb.log;

import io.questdb.std.Chars;
import io.questdb.std.Sinkable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.AbstractCharSink;
import io.questdb.std.str.CharSink;

public class LogRecordSink extends AbstractCharSink implements Sinkable {
    protected final long address;
    protected final long lim;
    protected long _wptr;
    private int level;
    private int multibyteLength = 0;
    private boolean done = false;

    LogRecordSink(long address, long addressSize) {
        this.address = _wptr = address;
        this.lim = address + addressSize;
    }

    public void clear() {
        _wptr = address;
        done = false;
    }

    public long getAddress() {
        return address;
    }

    public int getLevel() {
        return level;
    }

    public int length() {
        return (int) (_wptr - address);
    }

    @Override
    public CharSink put(CharSequence cs) {
        int rem = (int) (lim - _wptr);
        int len = cs.length();
        int n = Math.min(rem, len);
        Chars.asciiStrCpy(cs, n, _wptr);
        _wptr += n;
        return this;
    }

    @Override
    public CharSink put(CharSequence cs, int lo, int hi) {
        int rem = (int) (lim - _wptr);
        int len = hi - lo;
        int n = Math.min(rem, len);
        Chars.asciiStrCpy(cs, lo, n, _wptr);
        _wptr += n;
        return this;
    }

    @Override
    public CharSink put(char c) {
        if (done) {
            return this;
        }

        final byte b = (byte) c;
        final long left = lim - _wptr;
        long needed = utf8charNeeded(b);
        if (needed == -1) {
            // Invalid UTF-8 char sentinel replacement.
            Unsafe.getUnsafe().putByte(_wptr++, (byte) '?');
            needed = 1;
        }
        if (left >= needed) {
            Unsafe.getUnsafe().putByte(_wptr++, b);
        }
        else {
            // Stop processing any further characters.
            done = true;
        }
        return this;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    @Override
    public void toSink(CharSink sink) {
        Chars.utf8Decode(address, _wptr, sink);
    }

    private long utf8charNeeded(byte b) {
        // Reference the table at:
        // https://en.wikipedia.org/wiki/UTF-8#Encoding

        if (b >= 0) {
            return 1;  // ASCII
        }

        if ((b & 0xC0) == 0x80) {
            // 0xC0 = 1100 0000, 0x80 = 1000 0000, check if starts with 10
            // This is a continuation byte.
            if (multibyteLength < 2) {
                // This if guards against broken UTF-8 strings.
                multibyteLength = 2;
            }
            --multibyteLength;

        } else if ((b & 0xE0) == 0xC0) {
            // 0xE0 = 1110 0000, 0xC0 = 1100 0000, check if starts with 110
            multibyteLength = 2;
        } else if ((b & 0xF0) == 0xE0) {
            // 0xF0 = 1111 0000, 0xE0 = 1110 0000m, check if starts with 1110
            multibyteLength = 3;
        } else if ((b & 0xF8) == 0xF0) {
            // 0xF8 = 1111 1000, 0xF0 = 1111 0000, check if starts with 1111 0
            multibyteLength = 4;
        } else {
            // Invalid UTF-8 char.
            return -1;
        }

        return multibyteLength;
    }
}

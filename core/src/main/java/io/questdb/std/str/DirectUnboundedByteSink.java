/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.std.str;

import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;

public class DirectUnboundedByteSink extends AbstractCharSink {
    private long address;
    private long _wptr;

    public DirectUnboundedByteSink() {
    }

    public DirectUnboundedByteSink(long address) {
        this.address = _wptr = address;
    }

    public void clear(int len) {
        _wptr = address + len;
    }

    public long getAddress() {
        return address;
    }

    public int length() {
        return (int) (_wptr - address);
    }

    public void of(long address) {
        this.address = _wptr = address;
    }

    @Override
    public CharSink put(CharSequence cs) {
        int len = cs.length();
        Chars.asciiStrCpy(cs, len, _wptr);
        _wptr += len;
        return this;
    }

    @Override
    public CharSink put(char c) {
        Unsafe.getUnsafe().putByte(_wptr++, (byte) c);
        return this;
    }

    @Override
    public CharSink put(char[] chars, int start, int len) {
        Chars.asciiCopyTo(chars, start, len, _wptr);
        _wptr += len;
        return this;
    }

    @Override
    public String toString() {
        CharSink b = Misc.getThreadLocalBuilder();
        for (long p = address, hi = _wptr; p < hi; p++) {
            b.put((char) Unsafe.getUnsafe().getByte(p));
        }
        return b.toString();
    }
}

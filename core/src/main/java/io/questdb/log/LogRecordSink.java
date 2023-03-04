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

    LogRecordSink(long address, long addressSize) {
        this.address = _wptr = address;
        this.lim = address + addressSize;
    }

    public void clear() {
        _wptr = address;
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
        if (_wptr < lim) {
            Unsafe.getUnsafe().putByte(_wptr++, (byte) c);
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
}

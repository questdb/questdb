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

package io.questdb.std;

import io.questdb.griffin.engine.functions.constants.UuidConstant;
import io.questdb.std.str.CharSink;

public final class MutableUuid implements Sinkable {
    private long hi = UuidConstant.NULL_HI_AND_LO;
    private long lo = UuidConstant.NULL_HI_AND_LO;

    public MutableUuid(long hi, long lo) {
        of(hi, lo);
    }

    public MutableUuid() {

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || o.getClass() != MutableUuid.class) {
            return false;
        }
        MutableUuid that = (MutableUuid) o;
        return lo == that.lo && hi == that.hi;
    }

    public long getHi() {
        return hi;
    }

    public long getLo() {
        return lo;
    }

    @Override
    public int hashCode() {
        return Hash.hash(hi, lo);
    }

    public void of(long hi, long lo) {
        this.hi = hi;
        this.lo = lo;
    }

    public void of(CharSequence uuid) {
        int len = uuid.length();
        int dash1 = Chars.indexOf(uuid, '-');
        int dash2 = Chars.indexOf(uuid, dash1 + 1, len, '-');
        int dash3 = Chars.indexOf(uuid, dash2 + 1, len, '-');
        int dash4 = Chars.indexOf(uuid, dash3 + 1, len, '-');

        // valid UUIDs have exactly 4 dashes
        if (dash4 < 0 || dash4 == len - 1 || Chars.indexOf(uuid, dash4 + 1, len, '-') > 0) {
            // todo: is allocating a new exception here a good idea?
            throw new IllegalArgumentException("invalid UUID [string=" + uuid + "]");
        }

        long hi1;
        long hi2;
        long hi3;
        long lo1;
        long lo2;
        try {
            hi1 = Numbers.parseHexLong(uuid, 0, dash1);
            hi2 = Numbers.parseHexLong(uuid, dash1 + 1, dash2);
            hi3 = Numbers.parseHexLong(uuid, dash2 + 1, dash3);
            lo1 = Numbers.parseHexLong(uuid, dash3 + 1, dash4);
            lo2 = Numbers.parseHexLong(uuid, dash4 + 1, len);
        } catch (NumericException e) {
            throw new IllegalArgumentException("invalid UUID [string=" + uuid + "]");
        }
        this.hi = (hi1 << 32) | (hi2 << 16) | hi3;
        this.lo = (lo1 << 48) | lo2;
    }


    @Override
    public void toSink(CharSink sink) {
        Numbers.appendUuid(hi, lo, sink);
    }
}

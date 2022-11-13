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
    private long leastSigBits = UuidConstant.NULL_MSB_AND_LSB;
    private long mostSigBits = UuidConstant.NULL_MSB_AND_LSB;

    public MutableUuid(long mostSig, long leastSig) {
        of(mostSig, leastSig);
    }

    public MutableUuid() {

    }

    public void copyFrom(MutableUuid uuid) {
        this.mostSigBits = uuid.mostSigBits;
        this.leastSigBits = uuid.leastSigBits;
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
        return leastSigBits == that.leastSigBits && mostSigBits == that.mostSigBits;
    }

    public long getLeastSigBits() {
        return leastSigBits;
    }

    public long getMostSigBits() {
        return mostSigBits;
    }

    @Override
    public int hashCode() {
        long hilo = mostSigBits ^ leastSigBits;
        return ((int) (hilo >> 32)) ^ (int) hilo;
    }

    public void of(long mostSigBits, long leastSigBits) {
        this.mostSigBits = mostSigBits;
        this.leastSigBits = leastSigBits;
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

        long msb1;
        long msb2;
        long msb3;
        long lsb1;
        long lsb2;
        try {
            msb1 = Numbers.parseHexLong(uuid, 0, dash1);
            msb2 = Numbers.parseHexLong(uuid, dash1 + 1, dash2);
            msb3 = Numbers.parseHexLong(uuid, dash2 + 1, dash3);
            lsb1 = Numbers.parseHexLong(uuid, dash3 + 1, dash4);
            lsb2 = Numbers.parseHexLong(uuid, dash4 + 1, len);
        } catch (NumericException e) {
            throw new IllegalArgumentException("invalid UUID [string=" + uuid + "]");
        }
        this.mostSigBits = (msb1 << 32) | (msb2 << 16) | msb3;
        this.leastSigBits = (lsb1 << 48) | lsb2;
    }


    @Override
    public void toSink(CharSink sink) {
        Numbers.appendUuid(mostSigBits, leastSigBits, sink);
    }
}

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

import io.questdb.std.str.CharSink;

public final class MutableUuid implements Sinkable {
    private long hi = UuidUtil.NULL_HI_AND_LO;
    private long lo = UuidUtil.NULL_HI_AND_LO;

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

    public void of(CharSequence uuid) throws NumericException {
        UuidUtil.checkDashesAndLength(uuid);
        this.hi = UuidUtil.parseHi(uuid);
        this.lo = UuidUtil.parseLo(uuid);
    }

    public void ofNull() {
        this.hi = UuidUtil.NULL_HI_AND_LO;
        this.lo = UuidUtil.NULL_HI_AND_LO;
    }

    @Override
    public void toSink(CharSink sink) {
        Numbers.appendUuid(hi, lo, sink);
    }
}

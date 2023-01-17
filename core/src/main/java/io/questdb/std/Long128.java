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

public final class Long128 {
    public static final int BYTES = Long.BYTES * 2;
    public static final Long128 NULL = new Long128();

    private long hi;
    private long lo;

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof Long128) {
            Long128 that = (Long128) obj;
            return this.lo == that.lo && this.hi == that.hi;
        }
        return false;
    }

    public long getHi() {
        return hi;
    }

    public long getLo() {
        return lo;
    }

    @Override
    public int hashCode() {
        return Hash.hash(lo, hi);
    }

    public void ofNull() {
        this.lo = Numbers.LONG_NaN;
        this.hi = Numbers.LONG_NaN;
    }

    public void setAll(long lo, long hi) {
        this.lo = lo;
        this.hi = hi;
    }

    static {
        NULL.setAll(Numbers.LONG_NaN, Numbers.LONG_NaN);
    }
}

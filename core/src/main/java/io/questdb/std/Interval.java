/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.IntervalOperation;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;

public class Interval implements Sinkable, Mutable {
    public static final Interval NULL = new Interval(Numbers.LONG_NULL, Numbers.LONG_NULL);

    private long hi = Numbers.LONG_NULL;
    private long lo = Numbers.LONG_NULL;

    public Interval() {
    }

    public Interval(long lo, long hi) {
        this.of(lo, hi);
    }

    @Override
    public void clear() {
        lo = Numbers.LONG_NULL;
        hi = Numbers.LONG_NULL;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || o.getClass() != Interval.class) {
            return false;
        }
        Interval that = (Interval) o;
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
        return Hash.hashLong128_32(lo, hi);
    }

    public Interval of(long lo, long hi) {
        this.lo = lo;
        this.hi = hi;
        return this;
    }

    public void of(CharSequence seq, LongList list) throws NumericException, SqlException {
        IntervalUtils.parseInterval(seq, 0, seq.length(), IntervalOperation.NONE, list);
        assert list.size() != 0;
        if (list.size() != 2) {
            throw SqlException.$(-1, "only compatible with simple intervals");
        }
        lo = list.get(0);
        hi = list.get(1);
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii('(');
        if (lo != Long.MIN_VALUE) {
            sink.putAscii('\'');
            sink.put(Timestamps.toString(lo));
            sink.putAscii('\'');
        } else {
            sink.putAscii("null");
        }
        sink.putAscii(", ");
        if (hi != Long.MIN_VALUE) {
            sink.putAscii('\'');
            sink.put(Timestamps.toString(hi));
            sink.putAscii('\'');
        } else {
            sink.putAscii("null");
        }
        sink.put(')');
    }
}

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
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

public class IntervalImpl implements Interval, Sinkable {
    public static final IntervalImpl EMPTY = new IntervalImpl();

    long hi = Long.MIN_VALUE;
    long lo = Long.MIN_VALUE;

    public IntervalImpl() {
    }

    public void clear() {
        lo = Long.MIN_VALUE;
        hi = Long.MIN_VALUE;
    }

    @Override
    public long getHi() {
        return hi;
    }

    @Override
    public long getLo() {
        return lo;
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

    public void of(long lo, long hi) {
        this.lo = lo;
        this.hi = hi;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.put('(');
        sink.put(Timestamps.toString(lo));
        sink.put(',');
        sink.put(Timestamps.toString(hi));
        sink.put(')');
    }

    @Override
    public String toString() {
        StringSink sink = new StringSink();
        toSink(sink);
        return sink.toString();
    }
}

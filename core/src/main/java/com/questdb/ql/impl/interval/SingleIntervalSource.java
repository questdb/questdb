/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.ql.impl.interval;

import com.questdb.misc.Interval;
import com.questdb.std.AbstractImmutableIterator;
import com.questdb.std.str.CharSink;

public class SingleIntervalSource extends AbstractImmutableIterator<Interval> implements IntervalSource {
    private final Interval interval;
    private Interval next;

    public SingleIntervalSource(Interval interval) {
        this.interval = interval;
        this.next = interval;
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public Interval next() {
        Interval v = next;
        next = null;
        return v;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("SingleIntervalSource").put(',');
        sink.putQuoted("interval").put(':').put(interval);
        sink.put('}');
    }

    @Override
    public void toTop() {
        next = interval;
    }
}

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

package com.questdb.iter;

import com.questdb.Journal;
import com.questdb.iter.clock.Clock;
import com.questdb.iter.clock.MilliClock;
import com.questdb.misc.Unsafe;
import com.questdb.std.AbstractImmutableIterator;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Iterator;

public class ReplayIterator<T> extends AbstractImmutableIterator<T> {

    private final Iterator<T> underlying;
    private final Clock clock;
    private final float speed;
    private final TimeSource<T> timeSource;
    private long lastObjTicks;
    private long lastTicks;

    @SuppressFBWarnings({"OCP_OVERLY_CONCRETE_PARAMETER"})
    @SuppressWarnings("unchecked")
    public ReplayIterator(final Journal journal, float speed) {
        this((JournalIterator<T>) journal.iterator(), speed);
    }

    public ReplayIterator(final JournalIterator<T> underlying, float speed) {
        this.underlying = underlying;
        this.clock = MilliClock.INSTANCE;
        this.speed = speed;
        this.timeSource = new TimeSource<T>() {
            private final long timestampOffset = underlying.getJournal().getMetadata().getTimestampMetadata().offset;

            @Override
            public long getTicks(T object) {
                return Unsafe.getUnsafe().getLong(object, timestampOffset);
            }
        };
    }

    public ReplayIterator(Iterable<T> underlying, Clock clock, float speed, TimeSource<T> timeSource) {
        this(underlying.iterator(), clock, speed, timeSource);
    }

    public ReplayIterator(Iterator<T> underlying, Clock clock, float speed, TimeSource<T> timeSource) {
        this.underlying = underlying;
        this.clock = clock;
        this.speed = speed;
        this.timeSource = timeSource;
    }

    @Override
    public boolean hasNext() {
        return underlying.hasNext();
    }

    @SuppressWarnings("all")
    @Override
    public T next() {
        T o = underlying.next();
        long t = timeSource.getTicks(o);
        if (lastObjTicks == 0) {
            lastObjTicks = t;
            lastTicks = clock.getTicks();
        } else {
            long delta = (long) ((t - lastObjTicks) * speed);
            while (clock.getTicks() - lastTicks < delta) {
                // busy spin
            }
        }
        return o;
    }
}

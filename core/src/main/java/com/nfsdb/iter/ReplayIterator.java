/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.iter;

import com.nfsdb.Journal;
import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.iter.clock.Clock;
import com.nfsdb.iter.clock.MilliClock;
import com.nfsdb.misc.Unsafe;
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

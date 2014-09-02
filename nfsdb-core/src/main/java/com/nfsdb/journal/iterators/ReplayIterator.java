/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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
 */

package com.nfsdb.journal.iterators;

import com.nfsdb.journal.collections.AbstractImmutableIterator;
import com.nfsdb.journal.iterators.clock.Clock;

import java.util.Iterator;

public class ReplayIterator<T> extends AbstractImmutableIterator<T> {

    private final Iterator<T> underlying;
    private final Clock clock;
    private final float speed;
    private final TickSource<T> tickSource;
    private long lastObjTicks;
    private long lastTicks;

    public ReplayIterator(Iterable<T> underlying, Clock clock, float speed, TickSource<T> tickSource) {
        this(underlying.iterator(), clock, speed, tickSource);
    }

    public ReplayIterator(Iterator<T> underlying, Clock clock, float speed, TickSource<T> tickSource) {
        this.underlying = underlying;
        this.clock = clock;
        this.speed = speed;
        this.tickSource = tickSource;
    }

    @Override
    public boolean hasNext() {
        return underlying.hasNext();
    }

    @SuppressWarnings("all")
    @Override
    public T next() {
        T o = underlying.next();
        long t = tickSource.getTicks(o);
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

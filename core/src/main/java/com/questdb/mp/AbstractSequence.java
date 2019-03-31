/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.mp;

import com.questdb.std.Unsafe;

class LhsPadding {
    protected long p1, p2, p3, p4, p5, p6, p7;
}

class Value extends LhsPadding {
    private final WaitStrategy waitStrategy;
    protected volatile long value = -1;
    protected long cache = -1;
    protected Barrier barrier = OpenBarrier.INSTANCE;

    public Value(WaitStrategy waitStrategy) {
        this.waitStrategy = waitStrategy == null ? NullWaitStrategy.INSTANCE : waitStrategy;
    }

    public WaitStrategy getWaitStrategy() {
        return waitStrategy;
    }
}

class RhsPadding extends Value {
    protected long p9, p10, p11, p12, p13, p14;

    public RhsPadding(WaitStrategy waitStrategy) {
        super(waitStrategy);
    }
}

public abstract class AbstractSequence extends RhsPadding {
    private static final long VALUE_OFFSET = Unsafe.getFieldOffset(Value.class, "value");
    private static final long CACHE_OFFSET = Unsafe.getFieldOffset(Value.class, "cache");

    public AbstractSequence(WaitStrategy waitStrategy) {
        super(waitStrategy);
    }

    protected boolean casValue(long expected, long value) {
        return Unsafe.cas(this, VALUE_OFFSET, expected, value);
    }

    protected long getValue() {
        return Unsafe.getUnsafe().getLong(this, VALUE_OFFSET);
    }

    protected void setCacheFenced(long cache) {
        Unsafe.getUnsafe().putOrderedLong(this, CACHE_OFFSET, cache);
    }
}

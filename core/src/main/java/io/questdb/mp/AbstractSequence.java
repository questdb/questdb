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

package io.questdb.mp;

import io.questdb.std.Unsafe;

@SuppressWarnings("unused")
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

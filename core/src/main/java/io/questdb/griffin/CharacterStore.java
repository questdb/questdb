/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.griffin;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.BufferWindowCharSequence;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjectPool;
import io.questdb.std.str.AbstractCharSequence;
import io.questdb.std.str.Utf16Sink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CharacterStore implements CharacterStoreEntry, Mutable, Utf16Sink {
    private static final Log LOG = LogFactory.getLog(CharacterStore.class);
    private final ObjectPool<NameAssemblerCharSequence> csPool;
    private int capacity;
    private char[] chars;
    private NameAssemblerCharSequence next = null;
    private int size = 0;

    public CharacterStore(int capacity, int poolCapacity) {
        this.capacity = capacity;
        this.chars = new char[Numbers.ceilPow2(capacity)];
        csPool = new ObjectPool<>(NameAssemblerCharSequence::new, poolCapacity);
    }

    @Override
    public void clear() {
        csPool.clear();
        size = 0;
        next = null;
    }

    @Override
    public int length() {
        return size;
    }

    public CharacterStoreEntry newEntry() {
        this.next = csPool.next();
        this.next.lo = size;
        return this;
    }

    @Override
    public Utf16Sink put(char c) {
        if (size < capacity) {
            chars[size++] = c;
        } else {
            resizeAndPut(c);
        }
        return this;
    }

    @Override
    public Utf16Sink put(char @NotNull [] chars, int start, int len) {
        for (int i = 0; i < len; i++) {
            put(chars[start + i]);
        }
        return this;
    }

    @Override
    public Utf16Sink put(@Nullable CharSequence cs) {
        if (cs != null) {
            put(cs, 0, cs.length());
        }
        return this;
    }

    @Override
    public CharSequence toImmutable() {
        next.hi = size;
        return next;
    }

    public void trimTo(int size) {
        this.size = size;
    }

    private void resizeAndPut(char c) {
        char[] next = new char[capacity * 2];
        System.arraycopy(chars, 0, next, 0, capacity);
        chars = next;
        capacity *= 2;
        chars[size++] = c;
        LOG.info().$("resize [capacity=").$(capacity).$(']').$();
    }

    public class NameAssemblerCharSequence extends AbstractCharSequence implements Mutable, BufferWindowCharSequence {
        int hi;
        int lo;

        @Override
        public char charAt(int index) {
            return chars[lo + index];
        }

        @Override
        public void clear() {
        }

        @Override
        public int length() {
            return hi - lo;
        }

        @Override
        public void shiftLo(int positiveOffset) {
            lo += positiveOffset;
        }

        @Override
        protected CharSequence _subSequence(int start, int end) {
            NameAssemblerCharSequence that = csPool.next();
            that.lo = lo + start;
            that.hi = lo + end;
            assert that.lo < that.hi;
            return that;
        }
    }
}

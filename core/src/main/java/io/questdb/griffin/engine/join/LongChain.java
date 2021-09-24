/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;

import java.io.Closeable;

public class LongChain implements Closeable, Mutable {

    private final MemoryARW valueChain;
    private final TreeCursor cursor;

    public LongChain(long valuePageSize, int valueMaxPages) {
        this.valueChain = Vm.getARWInstance(valuePageSize, valueMaxPages, MemoryTag.NATIVE_DEFAULT);
        this.cursor = new TreeCursor();
    }

    @Override
    public void clear() {
        valueChain.jumpTo(0);
    }

    @Override
    public void close() {
        valueChain.close();
    }

    public TreeCursor getCursor(long tailOffset) {
        cursor.of(tailOffset);
        return cursor;
    }

    public long put(long value, long parentOffset) {
        final long appendOffset = valueChain.getAppendOffset();
        if (parentOffset != -1) {
            valueChain.putLong(parentOffset, appendOffset);
        }
        valueChain.putLong128(-1, value);
        return appendOffset;
    }

    public class TreeCursor {
        private long nextOffset;

        public boolean hasNext() {
            return nextOffset != -1;
        }

        public long next() {
            long next = valueChain.getLong(nextOffset);
            long value = valueChain.getLong(nextOffset + 8);
            this.nextOffset = next;
            return value;
        }

        void of(long startOffset) {
            this.nextOffset = startOffset;
        }
    }
}

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

package com.questdb.griffin.engine.join;

import com.questdb.cairo.VirtualMemory;
import com.questdb.std.Mutable;

import java.io.Closeable;

public class LongChain implements Closeable, Mutable {

    private final VirtualMemory valueChain;
    private final TreeCursor cursor;

    public LongChain(long valuePageSize) {
        this.valueChain = new VirtualMemory(valuePageSize);
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
        valueChain.putLong(-1);
        valueChain.putLong(value);
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

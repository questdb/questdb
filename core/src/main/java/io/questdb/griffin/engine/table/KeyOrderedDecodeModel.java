/*+*****************************************************************************
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

package io.questdb.griffin.engine.table;

import io.questdb.std.DirectIntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

/**
 * A model of what a read ordered by keys decodes over its Parquet page frames, driven by the rows of the read in order:
 * a direct read moves to their page frames through a decode cache that keeps the page frames it moved to last, and a
 * gather decodes each page frame that holds a row of a window once for the window.
 */
public final class KeyOrderedDecodeModel implements QuietCloseable {
    // In triples for each page frame: the page frame used before and the page frame used after it in the cache, -1 for
    // none, so that a page frame is in the cache when it is its head or has one before it, and the window that decoded
    // it last, -1 before the first.
    private final DirectIntList frames = new DirectIntList(0, MemoryTag.NATIVE_DEFAULT, true);
    private int cachedCount;
    private int capacity;
    private long directDecodes;
    private long gatherDecodes;
    private int head;
    private int tail;

    @Override
    public void close() {
        frames.close();
    }

    public long getDirectDecodes() {
        return directDecodes;
    }

    public long getGatherDecodes() {
        return gatherDecodes;
    }

    /**
     * The direct read moves to the page frame, which it decodes unless the cache keeps it.
     */
    public void moveTo(int frame) {
        final long model = 3L * frame;
        final int prev = frames.get(model);
        if (frame == head || prev >= 0) {
            if (frame == tail) {
                return;
            }
            final int next = frames.get(model + 1);
            if (frame == head) {
                head = next;
            } else {
                frames.set(3L * prev + 1, next);
            }
            frames.set(3L * next, prev);
        } else {
            directDecodes++;
            if (cachedCount == capacity) {
                final int evicted = head;
                head = frames.get(3L * evicted + 1);
                frames.set(3L * evicted + 1, -1);
                if (head < 0) {
                    tail = -1;
                } else {
                    frames.set(3L * head, -1);
                }
            } else {
                cachedCount++;
            }
        }
        frames.set(model, tail);
        frames.set(model + 1, -1);
        if (tail < 0) {
            head = frame;
        } else {
            frames.set(3L * tail + 1, frame);
        }
        tail = frame;
    }

    /**
     * Starts a model of {@code frameCount} page frames with a cache of {@code capacity} of them.
     */
    public void of(int frameCount, int capacity) {
        frames.reopen();
        frames.setCapacity(3L * frameCount);
        frames.setPos(3L * frameCount);
        frames.zero(-1);
        this.capacity = capacity;
        cachedCount = 0;
        directDecodes = 0;
        gatherDecodes = 0;
        head = -1;
        tail = -1;
    }

    /**
     * The gather reads a row of the window in the page frame, which it decodes once for the window.
     */
    public void readInWindow(int frame, int window) {
        final long model = 3L * frame + 2;
        if (frames.get(model) != window) {
            frames.set(model, window);
            gatherDecodes++;
        }
    }

    public void setMemoryTracker(@Nullable MemoryTracker tracker) {
        frames.setMemoryTracker(tracker);
    }
}

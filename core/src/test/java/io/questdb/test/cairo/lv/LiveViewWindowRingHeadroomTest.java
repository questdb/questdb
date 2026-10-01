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

package io.questdb.test.cairo.lv;

import io.questdb.griffin.engine.window.WindowFunction;
import org.junit.Assert;
import org.junit.Test;

/**
 * A checkpoint restore rebuilds a partition's ring at
 * {@link WindowFunction#restoredRingCapacity(long, long)}, which every window
 * function's {@code restoreCheckpointRingState} / {@code restoreCheckpointState}
 * sizes its ring from.
 * <p>
 * Restoring at exactly the restored row count leaves the ring full, so the first
 * row the replay appends behind it doubles the ring and copies all of it - once
 * per partition, on every restore, and a live view over an out-of-order base
 * restores several times a second. The capacity therefore has to sit strictly
 * above the row count it restores.
 */
public class LiveViewWindowRingHeadroomTest {

    private static final int INITIAL_BUFFER_SIZE = 256;

    @Test
    public void testCapacityFloorsAtTheInitialBufferSize() {
        // A ring the initial size already covers takes the initial size, so a small
        // partition does not restore into a buffer smaller than a fresh one gets.
        Assert.assertEquals(INITIAL_BUFFER_SIZE, WindowFunction.restoredRingCapacity(0, INITIAL_BUFFER_SIZE));
        Assert.assertEquals(INITIAL_BUFFER_SIZE, WindowFunction.restoredRingCapacity(1, INITIAL_BUFFER_SIZE));
        Assert.assertEquals(
                INITIAL_BUFFER_SIZE,
                WindowFunction.restoredRingCapacity(2 * INITIAL_BUFFER_SIZE / 3, INITIAL_BUFFER_SIZE)
        );
    }

    @Test
    public void testCapacityLeavesRoomAboveTheRestoredRowCount() {
        // The sizes either side of the initial-buffer floor, a power of two - where a
        // capacity that merely rounded up would land back on the row count - and a
        // ring far larger than the floor.
        final long[] sizes = {
                INITIAL_BUFFER_SIZE,
                INITIAL_BUFFER_SIZE + 1,
                1024,
                4096,
                1_000_003,
                1L << 40
        };
        for (long size : sizes) {
            final long capacity = WindowFunction.restoredRingCapacity(size, INITIAL_BUFFER_SIZE);
            Assert.assertTrue(
                    "a restored ring of " + size + " rows must have room for the replay's next row, was " + capacity,
                    capacity > size
            );
        }
    }
}

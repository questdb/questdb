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

package io.questdb.test.cairo.o3;

import io.questdb.cairo.O3OpenColumnJob;
import io.questdb.mp.MCSequence;
import io.questdb.tasks.O3OpenColumnTask;
import org.junit.Assert;
import org.junit.Test;

public class O3OpenColumnJobTest {
    @Test
    public void testOpenColumnSnapshotsPinnedTailBeforeReleasingTask() {
        assertTailPinnedReadBeforeRelease(true);
    }

    @Test
    public void testOpenColumnSnapshotsUnpinnedTailBeforeReleasingTask() {
        assertTailPinnedReadBeforeRelease(false);
    }

    private static void assertTailPinnedReadBeforeRelease(boolean isTailPinned) {
        final ObservedTask task = new ObservedTask(isTailPinned);
        final long cursor = 7;
        final MCSequence subSeq = new MCSequence(8) {
            @Override
            public void done(long releasedCursor) {
                Assert.assertEquals(cursor, releasedCursor);
                Assert.assertEquals("snapshot isTailPinned before releasing the task", 1, task.tailPinnedReadCount);
                // Stop at the ownership boundary: the task has no downstream file resources.
                throw new StopAtRelease();
            }
        };

        // This narrow adapter test allocates no native memory and does not execute column I/O.
        try {
            O3OpenColumnJob.openColumn(task, cursor, subSeq);
            Assert.fail("openColumn did not release the task");
        } catch (StopAtRelease expected) {
            // Only a release after the flag snapshot may reach this sentinel.
        }
    }

    private static class ObservedTask extends O3OpenColumnTask {
        private final boolean isTailPinned;
        private int tailPinnedReadCount;

        private ObservedTask(boolean isTailPinned) {
            this.isTailPinned = isTailPinned;
        }

        @Override
        public boolean isTailPinned() {
            tailPinnedReadCount++;
            return isTailPinned;
        }
    }

    private static class StopAtRelease extends RuntimeException {
    }
}

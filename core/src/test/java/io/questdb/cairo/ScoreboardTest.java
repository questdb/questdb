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

package io.questdb.cairo;

import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ScoreboardTest extends AbstractCairoTest {
    @Test
    public void testScoreboard() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    Path path = new Path().of(root);
                    ScoreboardWriter w = new ScoreboardWriter(FilesFacadeImpl.INSTANCE, path, 0)
            ) {
                Assert.assertTrue(w.addPartition(100000, -1));
                Assert.assertTrue(w.addPartition(100000, 0));
                Assert.assertTrue(w.addPartition(100000, 1));
                Assert.assertTrue(w.addPartition(200000, -1));
                Assert.assertTrue(w.addPartition(300000, -1));
                w.acquireReadLock(300000, -1);
                Assert.assertEquals(1, w.getAccessCounter(300000, -1));

                Assert.assertTrue(w.addPartition(300000, 0));
                Assert.assertTrue(w.addPartition(300000, 1));
                Assert.assertTrue(w.addPartition(300000, 2));
                Assert.assertFalse(w.addPartition(300000, 1));
                // acquire read lock on last partition
                w.acquireReadLock(300000, 1);
                // assert that lock is still held in place
                // we are going to memmove this when we insert partition in the middle
                Assert.assertEquals(1, w.getAccessCounter(300000, 1));
                Assert.assertEquals(8, w.getPartitionCount());

                // inserting this partition should shift down partition with reader lock on
                Assert.assertTrue(w.addPartition(200000, 2));

                // check that access counters are intact
                Assert.assertEquals(1, w.getAccessCounter(300000, -1));
                Assert.assertEquals(1, w.getAccessCounter(300000, 1));

                w.acquireReadLock(100000, 0);
                Assert.assertFalse(w.acquireWriteLock(100000, 0));

                // now delete "reader-locked" partition
                Assert.assertTrue(w.removePartition(100000, 0));
                // and reader can release lock on removed partition
                w.releaseReadLock(100000, 0);
                // we should be able to release read lock twice on non-existent partition
                w.releaseReadLock(100000, 0);

                Assert.assertTrue(w.acquireWriteLock(100000, 1));

                Assert.assertFalse(w.acquireWriteLock(100000, 0));
                // and we sould be able to unlock non-existent partition
                w.releaseWriteLock(100000, 0);

                // check that after removing partition the access counter remains intact
                Assert.assertEquals(1, w.getAccessCounter(300000, 1));
            }
        });
    }

    static {
        Os.init();
    }
}

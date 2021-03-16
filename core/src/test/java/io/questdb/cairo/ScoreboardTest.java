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
    public void testScoreboardSize() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    Path path = new Path().of(root);
                    ScoreboardWriter w = new ScoreboardWriter(FilesFacadeImpl.INSTANCE, path, 0);
            ) {
                w.addPartition(100000, -1);
                w.addPartition(100000, 0);
                w.addPartition(100000, 1);
                w.addPartition(200000, -1);
                w.addPartition(300000, -1);
                w.addPartition(300000, 0);
                w.addPartition(300000, 1);
                w.addPartition(300000, 2);

                w.acquireReadLock(100000, 0);
                Assert.assertFalse(w.acquireWriteLock(100000, 0));
                Assert.assertTrue(w.acquireWriteLock(100000, 1));
                w.releaseReadLock(100000, 0);
                Assert.assertTrue(w.acquireWriteLock(100000, 0));
                System.out.println("ok");
            }
        });
    }

    static {
        Os.init();
    }
}

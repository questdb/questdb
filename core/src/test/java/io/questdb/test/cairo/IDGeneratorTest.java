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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.IDGenerator;
import io.questdb.cairo.IDGeneratorFactory;
import io.questdb.std.LongList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class IDGeneratorTest extends AbstractCairoTest {

    @Test
    public void testASyncMode() throws Exception {
        assertMemoryLeak(() -> {
            Set<Long> set = testGenerateId(configuration, CommitMode.ASYNC, 256, 5000);
            Assert.assertEquals(10000, set.size());
        });
    }

    @Test
    public void testGetCurrentAsync() {
        testGetCurrent(CommitMode.ASYNC);
    }

    @Test
    public void testGetCurrentNosync() {
        testGetCurrent(CommitMode.NOSYNC);
    }

    @Test
    public void testGetCurrentSync() {
        testGetCurrent(CommitMode.SYNC);
    }

    @Test
    public void testNoSyncMode() throws Exception {
        assertMemoryLeak(() -> {
            Set<Long> set = testGenerateId(configuration, CommitMode.NOSYNC, 1, 5000);
            Assert.assertEquals(10000, set.size());
        });
    }

    @Test
    public void testSwitchMode() throws Exception {
        assertMemoryLeak(() -> {
            int count = 10000;
            Set<Long> set1 = testGenerateId(configuration, CommitMode.SYNC, 512, count);
            Assert.assertEquals(count * 2, set1.size());
            Set<Long> set2 = testGenerateId(configuration, CommitMode.NOSYNC, 1, count);
            Assert.assertEquals(count * 2, set2.size());
            set1.addAll(set2);
            Assert.assertEquals(count * 4, set1.size());
            Set<Long> set3 = testGenerateId(configuration, CommitMode.ASYNC, 100, count);
            Assert.assertEquals(count * 2, set3.size());
            set1.addAll(set3);
            Assert.assertTrue(set1.size() <= count * 6);
        });
    }

    @Test
    public void testSyncMode() throws Exception {
        assertMemoryLeak(() -> {
            Set<Long> set = testGenerateId(configuration, CommitMode.SYNC, 512, 5000);
            Assert.assertEquals(10000, set.size());
        });
    }

    private Set<Long> testGenerateId(CairoConfiguration configuration, int commitMode, int step, int count) throws Exception {
        try (
                IDGenerator idGenerator = IDGeneratorFactory.newIDGenerator(
                        new CairoConfigurationWrapper(configuration) {
                            @Override
                            public int getCommitMode() {
                                return commitMode;
                            }
                        }, "id_generator.test", step
                )
        ) {
            idGenerator.open();
            final LongList listA = new LongList();
            final LongList listB = new LongList();
            final CyclicBarrier startBarrier = new CyclicBarrier(2);
            final AtomicInteger errors = new AtomicInteger();

            final Thread th = new Thread(() -> {
                try {
                    startBarrier.await();
                    for (int i = 0; i < count; i++) {
                        listA.add(idGenerator.getNextId());
                    }
                } catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                    errors.incrementAndGet();
                }
            });
            th.start();

            try {
                startBarrier.await();
                for (int i = 0; i < count; i++) {
                    listB.add(idGenerator.getNextId());
                }
                th.join();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
                errors.incrementAndGet();
            }

            Set<Long> set = new HashSet<>();
            // add both arrays to the set and assert that there are no duplicates
            for (int i = 0, n = listA.size(); i < n; i++) {
                Assert.assertTrue(set.add(listA.getQuick(i)));
            }
            for (int i = 0, n = listB.size(); i < n; i++) {
                Assert.assertTrue(set.add(listB.getQuick(i)));
            }
            return set;
        }
    }

    private void testGetCurrent(int commitMode) {
        try (
                IDGenerator idGenerator = IDGeneratorFactory.newIDGenerator(
                        new CairoConfigurationWrapper(configuration) {
                            @Override
                            public int getCommitMode() {
                                return commitMode;
                            }
                        }, "get_current.test", 7
                )
        ) {
            idGenerator.open();
            Assert.assertEquals(0, idGenerator.getCurrentId());
            Assert.assertEquals(1, idGenerator.getNextId());
            Assert.assertEquals(1, idGenerator.getCurrentId());
            Assert.assertEquals(2, idGenerator.getNextId());
            Assert.assertEquals(2, idGenerator.getCurrentId());
            Assert.assertEquals(3, idGenerator.getNextId());
            Assert.assertEquals(3, idGenerator.getCurrentId());
        }
    }
}
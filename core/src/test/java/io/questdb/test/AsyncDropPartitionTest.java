/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test;

import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.TableReader;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

public class AsyncDropPartitionTest extends AbstractCairoTest {

    @Before
    @Override
    public void setUp() {
        super.setUp();
        testMicrosClock = MicrosecondClockImpl.INSTANCE;
    }

    @Test
    public void testSplitPartition_enqueueDropPartition_mergePartition_applyTheDrop() throws Exception {
        assertMemoryLeak(() -> {
            try (O3PartitionPurgeJob purgeJob = new O3PartitionPurgeJob(engine, 1)) {
                ddl("create table tango (ts timestamp, n long) timestamp(ts) partition by hour wal");
                // create 3 hourly partitions: 0, 1, 2
                insert("insert into tango select (1000*1000*x)::timestamp, x from long_sequence(10799)");
                drainWalQueue();

                // when we lock the reader that will ensure that partition 1.0 is not purged
                TableReader r = engine.getReader("tango");
                // schedule to create a new version of partition 1 -> 1.1
                long baseTimestamp = TimeUnit.MINUTES.toMicros(61);
                insert(String.format("insert into tango values (%d::timestamp, 0)", baseTimestamp));
                insert(String.format("insert into tango select %d::timestamp, 0 from (tango limit 1) where sleep(300)", baseTimestamp));
                // schedule to drop partitions 0, 1.0
                ddl("alter table tango drop partition where ts < 7200*1000*1000");

                CyclicBarrier barrier = new CyclicBarrier(2);
                SOCountDownLatch haltLatch = new SOCountDownLatch(2);
                // horse #1
                new Thread(() -> {
                    try {
                        TestUtils.await(barrier);
                        drainWalQueue();
                    } finally {
                        Path.clearThreadLocals();
                        haltLatch.countDown();
                    }
                }

                ).start();

                // horse #2
                new Thread(() -> {
                    try {
                        TestUtils.await(barrier);
                        r.close();
                        purgeJob.drain(0);
                    } finally {
                        Path.clearThreadLocals();
                        haltLatch.countDown();
                    }
                }).start();
                haltLatch.await();

                assertSql(
                        "suspended\n" +
                                "false\n",
                        "select suspended from wal_tables where name = 'tango'"
                );
            }
        });
    }
}

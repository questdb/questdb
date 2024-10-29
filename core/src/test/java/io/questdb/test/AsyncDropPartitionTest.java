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

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
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
        staticOverrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 10);
        super.setUp();
    }

    @Test
    public void testSplitPartition_enqueueDropPartition_mergePartition_applyTheDrop() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tango (ts timestamp, n long) timestamp(ts) partition by hour wal");
            // create 3 hourly partitions: 0, 1, 2
            insert("insert into tango select (1000*1000*x)::timestamp, x from long_sequence(10799)");
            drainWalQueue();

            // acquire a table reader, this locks the table and prevents purging partition 1.0
            TableReader r = engine.getReader("tango");
            // create split in partition 1
            insert(String.format("insert into tango values (%d::timestamp, 0)", TimeUnit.MINUTES.toMicros(110)));
            drainWalQueue();
            // Schedule to get the WAL applier busy with some work, giving us time to run PartitionPurgeJob
            insert(String.format(
                    "insert into tango select (%d + rnd_long(0, 1000*1000*1000), 0)::timestamp, x from long_sequence(5*1000*1000)",
                    TimeUnit.MINUTES.toMicros(121)));
            // schedule to drop partitions 0, 1
            ddl("alter table tango drop partition where ts < 7200*1000*1000");

            CyclicBarrier barrier = new CyclicBarrier(2);

            // horse #1
            Thread drainWalQueue = new Thread(() -> {
                try {
                    TestUtils.await(barrier);
                    System.out.println("Drain WAL queue");
                    drainWalQueue();
                    System.out.println("DONE: Drain WAL queue");
                } finally {
                    Path.clearThreadLocals();
                }
            }
            );
            drainWalQueue.start();

            // horse #2
            TestUtils.await(barrier);
            Thread.sleep(1);
            System.out.println("Close reader");
            r.close();
//            System.out.println("Purge partition");
//            Path stalePartitionDir = Path.getThreadLocal(engine.getConfiguration().getRoot()).concat("tango~1/1970-01-01T014959-000001.1");
//            if (!FilesFacadeImpl.INSTANCE.unlinkOrRemove(stalePartitionDir, LOG)) {
//                fail("Didn't delete stale partition directory");
//            }
//            System.out.println("DONE: Purge partition");

            drainWalQueue.join();
            assertSql("suspended\nfalse\n", "select suspended from wal_tables where name = 'tango'");
        });
    }
}

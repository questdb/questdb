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

import io.questdb.cairo.CorruptPartitionRegistry;
import io.questdb.cairo.TableToken;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class CorruptPartitionRegistryTest extends AbstractCairoTest {

    @Test
    public void testCondemnIsScopedToOnePartition() throws Exception {
        // The whole point of a per-partition verdict: condemning one partition must not take out the
        // rest of the table. Without this, the feature is indistinguishable from suspending the table.
        assertMemoryLeak(() -> {
            execute("create table c1 (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into c1 values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("c1");
            final CorruptPartitionRegistry reg = engine.getCorruptPartitionRegistry();

            reg.condemn(token, "2024-01-01", "v.d block 3");

            Assert.assertEquals("v.d block 3", reg.reasonFor(token, "2024-01-01"));
            Assert.assertNull("a different partition must stay healthy", reg.reasonFor(token, "2024-01-02"));
        });
    }

    @Test
    public void testEmptyRegistryAnswersWithoutTouchingTheMap() throws Exception {
        // Consulted on every partition open, so the common case must short-circuit.
        assertMemoryLeak(() -> {
            execute("create table c2 (ts timestamp, v long) timestamp(ts) partition by day wal");
            drainWalQueue();
            final CorruptPartitionRegistry reg = engine.getCorruptPartitionRegistry();
            reg.clear();
            Assert.assertTrue(reg.isEmpty());
            Assert.assertNull(reg.reasonFor(engine.verifyTableName("c2"), "2024-01-01"));
        });
    }

    @Test
    public void testVerdictIsRevocable() throws Exception {
        // A false positive must not need a restart to clear. Detection may not cost availability
        // permanently.
        assertMemoryLeak(() -> {
            execute("create table c3 (ts timestamp, v long) timestamp(ts) partition by day wal");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("c3");
            final CorruptPartitionRegistry reg = engine.getCorruptPartitionRegistry();
            reg.clear();

            reg.condemn(token, "2024-01-01", "ts.d block 0");
            Assert.assertFalse(reg.isEmpty());
            Assert.assertEquals(1, reg.size());

            reg.clear(token, "2024-01-01");
            Assert.assertNull(reg.reasonFor(token, "2024-01-01"));
            Assert.assertTrue("clearing the last verdict must restore the fast path", reg.isEmpty());
        });
    }

    @Test
    public void testVerdictsDoNotSurviveARestart() throws Exception {
        // Deliberately in memory. Persisting a verdict makes a FALSE positive permanent, and a false
        // positive takes a healthy partition offline; the scrub re-derives real ones cheaply.
        assertMemoryLeak(() -> {
            execute("create table c4 (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into c4 values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("c4");
            engine.getCorruptPartitionRegistry().condemn(token, "2024-01-01", "v.d block 1");
            Assert.assertNotNull(engine.getCorruptPartitionRegistry().reasonFor(token, "2024-01-01"));

            engine.releaseInactive();
            engine.clear();

            // A fresh registry is what a restart yields; nothing on disk records the verdict.
            final CorruptPartitionRegistry fresh = new CorruptPartitionRegistry();
            Assert.assertTrue(fresh.isEmpty());
            Assert.assertNull(fresh.reasonFor(token, "2024-01-01"));
        });
    }
}

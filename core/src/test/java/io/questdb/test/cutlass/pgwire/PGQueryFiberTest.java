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

package io.questdb.test.cutlass.pgwire;

import io.questdb.cutlass.pgwire.PGConfiguration;
import io.questdb.cutlass.pgwire.PGServer;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Os;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;

/**
 * Runs PGWire with fiber-mode query execution enabled: every connection operation
 * executes as a QueryTask on a pooled fiber mounted by the network pool's workers.
 * Exercises the full production shape end-to-end over a real socket: dispatch job
 * launches the task, the fiber runs the pipeline, a sleep() query freezes the fiber
 * on a timer wait, the timer fires and the fiber resumes through the pool's
 * continuation queue to complete the response.
 */
public class PGQueryFiberTest extends BasePGTest {

    @Test
    public void testQueriesRunOnFiberHostPool() throws Exception {
        // end-state stack: fiber-mode dispatch on a FIBER-HOST pool -- the worker
        // loop is plain code (no worker-loop continuation, no handoff), and parked
        // fibers mount directly on the cont-free worker frame
        assertMemoryLeak(() -> {
            final PGConfiguration conf = new Port0PGConfiguration(-1) {
                @Override
                public boolean isFiberHost() {
                    return true;
                }
            };
            try (
                    PGServer server = createPGServer(conf);
                    WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                for (int i = 0; i < 60_000 && !server.isListening(); i++) {
                    Os.sleep(1);
                }
                Assert.assertTrue(server.isListening());
                try (Connection connection = getConnection(Mode.EXTENDED, server.getPort(), true)) {
                    try (ResultSet rs = connection.createStatement().executeQuery("select 42 x")) {
                        Assert.assertTrue(rs.next());
                        Assert.assertEquals(42, rs.getInt(1));
                    }
                    final long sleepStart = System.nanoTime();
                    try (ResultSet rs = connection.createStatement().executeQuery("select * from sleep(0.3)")) {
                        Assert.assertTrue(rs.next());
                    }
                    final long sleepMillis = (System.nanoTime() - sleepStart) / 1_000_000;
                    Assert.assertTrue("sleep completed too early: " + sleepMillis + "ms", sleepMillis >= 290);
                    try (ResultSet rs = connection.createStatement().executeQuery("select 43 x")) {
                        Assert.assertTrue(rs.next());
                        Assert.assertEquals(43, rs.getInt(1));
                    }
                }
            }
        });
    }

    @Test
    public void testQueriesRunOnPooledFibers() throws Exception {
        assertMemoryLeak(() -> {
            final PGConfiguration conf = new Port0PGConfiguration(-1);
            try (
                    PGServer server = createPGServer(conf);
                    WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                for (int i = 0; i < 60_000 && !server.isListening(); i++) {
                    Os.sleep(1);
                }
                Assert.assertTrue(server.isListening());
                try (Connection connection = getConnection(Mode.EXTENDED, server.getPort(), true)) {
                    // a plain query end-to-end on a fiber
                    try (ResultSet rs = connection.createStatement().executeQuery("select 42 x")) {
                        Assert.assertTrue(rs.next());
                        Assert.assertEquals(42, rs.getInt(1));
                    }
                    // a parking query: sleep() freezes the fiber on a timer wait; the
                    // timer fires and the frozen fiber resumes through the network
                    // pool's continuation queue to finish streaming the result
                    final long sleepStart = System.nanoTime();
                    try (ResultSet rs = connection.createStatement().executeQuery("select * from sleep(0.3)")) {
                        Assert.assertTrue(rs.next());
                    }
                    final long sleepMillis = (System.nanoTime() - sleepStart) / 1_000_000;
                    Assert.assertTrue("sleep completed too early: " + sleepMillis + "ms", sleepMillis >= 290);
                    // the same connection keeps reusing its task and the pooled fiber
                    try (ResultSet rs = connection.createStatement().executeQuery("select 43 x")) {
                        Assert.assertTrue(rs.next());
                        Assert.assertEquals(43, rs.getInt(1));
                    }
                }
            }
        });
    }
}

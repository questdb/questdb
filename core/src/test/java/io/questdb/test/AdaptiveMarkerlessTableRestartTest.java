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

package io.questdb.test;


import io.questdb.ServerMain;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableUtils;
import io.questdb.std.str.Path;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A WAL table's generation-zero epoch anchor is published at CREATE time
 * ({@code DurableEpochManifest.publishInitial} in {@code CairoEngine}'s create path), not by the first
 * write. Without it {@code RecoveryCoordinator.recoverTable} refuses to fall back to live state on the
 * next startup -- and that refusal fails the {@code engine} component, so a single markerless table takes
 * the whole instance down rather than just itself.
 *
 * <p>{@link TestUtils#createTable(io.questdb.cairo.CairoEngine, io.questdb.cairo.TableStructure)} builds a
 * table the low-level way: it writes the files, registers the token and the sequencer entry, and used to
 * skip the anchor. Any test that created a table that way and then restarted a server would blame the
 * restart -- which is exactly how this surfaced, in enterprise's AccessControlFuzzTest.
 *
 * <p>Both tests below assert the same invariant from the two creation paths, so they stay honest about
 * the utility being equivalent to the engine rather than merely close to it.
 */
public class AdaptiveMarkerlessTableRestartTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        // No commit-mode override: ADAPTIVE is the default on this branch, and passing extra properties
        // here displaces the harness's random-port defaults and collides with whatever holds 9090.
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testSqlCreatedEmptyWalTableRestartsCleanly() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();
                serverMain.getEngine().execute("create table x (a int, t timestamp) timestamp(t) partition by day wal");
                try (Path path = new Path()) {
                    path.of(serverMain.getEngine().getConfiguration().getDbRoot())
                            .concat(serverMain.getEngine().verifyTableName("x"))
                            .concat(TableUtils.SNAPSHOT_FILE_NAME);
                    Assert.assertTrue(
                            "SQL CREATE is expected to leave a durable epoch marker",
                            TestFilesFacadeImpl.INSTANCE.exists(path.$())
                    );
                }
            }
            // The control for the case below: same shape of table, same absence of writes, created the
            // way a user creates one. If this ever fails, the guard is not merely test-visible.
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();
                Assert.assertNotNull(serverMain.getEngine().verifyTableName("x"));
            }
        });
    }

    @Test
    public void testTestUtilsCreatedEmptyWalTableRestartsCleanly() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();
                final TableModel model = new TableModel(serverMain.getEngine().getConfiguration(), "z", PartitionBy.DAY)
                        .timestamp()
                        .wal()
                        .col("a", ColumnType.INT);
                TestUtils.createTable(serverMain.getEngine(), model);

                try (Path path = new Path()) {
                    path.of(serverMain.getEngine().getConfiguration().getDbRoot())
                            .concat(serverMain.getEngine().verifyTableName("z"))
                            .concat(TableUtils.SNAPSHOT_FILE_NAME);
                    Assert.assertTrue(
                            "TestUtils.createTable must leave the same durable epoch anchor as the engine's"
                                    + " create path, or the table cannot survive a restart",
                            TestFilesFacadeImpl.INSTANCE.exists(path.$())
                    );
                }
            }

            // The consequence, and the reason this matters: without the anchor RecoveryCoordinator refuses
            // the live-state fallback, and that refusal fails the engine component -- so one markerless
            // table used to take the whole instance down with it, which is how it surfaced (enterprise
            // AccessControlFuzzTest, whose tables are built this way, died on its restart leg).
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();
                Assert.assertNotNull(serverMain.getEngine().verifyTableName("z"));
            }
        });
    }
}

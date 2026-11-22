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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.griffin.SqlException;
import io.questdb.std.Files;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ViewCheckpointTest extends AbstractCairoTest {
    private static final TestFilesFacade testFilesFacade = new TestFilesFacade();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        ff = testFilesFacade;
        AbstractCairoTest.setUpStatic();
    }

    @Before
    public void setUp() {
        // Like CheckpointTest, this suite restores checkpoints, which Windows does not support.
        Assume.assumeFalse(Os.isWindows());
        super.setUp();
        ff = testFilesFacade;
        testFilesFacade.beforeViewWrite = null;
        testFilesFacade.checkpointTxnPath = null;
        testFilesFacade.isTxnOpenFailureEnabled = false;
        setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, "view-checkpoint");
    }

    @Test
    public void testAlterDuringCheckpointKeepsCapturedDefinition() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            execute("ALTER VIEW v AS SELECT x FROM base WHERE x > 7");
            testFilesFacade.beforeViewWrite = () -> {
                try {
                    execute("ALTER VIEW v AS SELECT x FROM base WHERE x > 9");
                    drainWalAndViewQueues();
                } catch (SqlException e) {
                    throw new RuntimeException(e);
                }
            };
            engine.checkpointCreate(sqlExecutionContext.getCircuitBreaker(), true);
            try {
                Assert.assertNull("the concurrent ALTER must have run", testFilesFacade.beforeViewWrite);
                assertQuery("v").noLeakCheck().returns("x\n10\n");
                assertCheckpoint(1, "SELECT x FROM base WHERE x > 7");
                restoreCheckpoint();
                assertQuery("v").noLeakCheck().returns("x\n8\n9\n10\n");
            } finally {
                engine.checkpointRelease();
            }
            assertAlterAfterRestore(2);
        });
    }

    @Test
    public void testAppliedViewAlters() throws Exception {
        assertViewAlters(true);
    }

    @Test
    public void testTxnWriteFailureAllowsRetry() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            execute("ALTER VIEW v AS SELECT x FROM base WHERE x > 7");
            testFilesFacade.isTxnOpenFailureEnabled = true;
            try {
                engine.checkpointCreate(sqlExecutionContext.getCircuitBreaker(), true);
                Assert.fail("checkpoint must report the failed _txn write");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not open read-write");
                TestUtils.assertContains(e.getFlyweightMessage(), "_txn");
            } finally {
                engine.checkpointRelease();
            }
            Assert.assertFalse("the _txn write must have been attempted", testFilesFacade.isTxnOpenFailureEnabled);
            assertQuery("v").noLeakCheck().returns("x\n8\n9\n10\n");
            engine.checkpointCreate(sqlExecutionContext.getCircuitBreaker(), true);
            try {
                assertCheckpoint(1, "SELECT x FROM base WHERE x > 7");
                restoreCheckpoint();
                assertQuery("v").noLeakCheck().returns("x\n8\n9\n10\n");
            } finally {
                engine.checkpointRelease();
            }
        });
    }

    @Test
    public void testUnappliedViewAlters() throws Exception {
        assertViewAlters(false);
    }

    private void assertAlterAfterRestore(long expectedSeqTxn) throws Exception {
        execute("ALTER VIEW v AS SELECT x FROM base WHERE x > 8");
        drainWalAndViewQueues();
        TableToken viewToken = engine.verifyTableName("v");
        Assert.assertEquals(expectedSeqTxn, engine.getViewGraph().getViewDefinition(viewToken).getSeqTxn());
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(viewToken));
        assertQuery("v").noLeakCheck().returns("x\n9\n10\n");
    }

    private void assertCheckpoint(long expectedSeqTxn, String expectedSql) {
        TableToken viewToken = engine.verifyTableName("v");
        try (Path path = new Path(); TxReader reader = new TxReader(ff); BlockFileReader blockReader = new BlockFileReader(configuration)) {
            path.of(configuration.getCheckpointRoot()).concat(configuration.getDbDirectory());
            final int rootLen = path.size();
            ViewDefinition definition = new ViewDefinition();
            ViewDefinition.readFrom(definition, blockReader, path, rootLen, viewToken);
            Assert.assertEquals(expectedSql, definition.getViewSql());
            Assert.assertEquals(expectedSeqTxn, definition.getSeqTxn());

            path.trimTo(rootLen).concat(viewToken).concat(TableUtils.TXN_FILE_NAME);
            Assert.assertTrue("view checkpoint must contain _txn", ff.exists(path.$()));
            reader.ofRO(path.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.NOT_APPLICABLE);
            TableUtils.safeReadTxn(reader, configuration.getMillisecondClock(), configuration.getSpinLockTimeout());
            Assert.assertEquals(expectedSeqTxn, reader.getSeqTxn());
            Assert.assertEquals(0, reader.getRowCount());
            Assert.assertEquals(0, reader.getSymbolColumnCount());

            path.trimTo(rootLen).concat(viewToken).concat(WalUtils.SEQ_DIR).concat(TableUtils.CHECKPOINT_SEQ_TXN_FILE_NAME);
            long fd = ff.openRO(path.$());
            Assert.assertTrue(fd > -1);
            try {
                Assert.assertEquals(expectedSeqTxn, ff.readNonNegativeLong(fd, 0));
            } finally {
                ff.close(fd);
            }
        }
    }

    private void assertViewAlters(boolean isWalApplied) throws Exception {
        assertMemoryLeak(() -> {
            createView();
            for (int txn = 0; txn <= 2; txn++) {
                String sql = "SELECT x FROM base WHERE x > " + (5 + txn);
                if (txn > 0) {
                    execute("ALTER VIEW v AS " + sql);
                }
                if (isWalApplied) {
                    drainWalAndViewQueues();
                }
                engine.checkpointCreate(sqlExecutionContext.getCircuitBreaker(), true);
                try {
                    assertCheckpoint(txn, sql);
                    if (txn == 2) {
                        // Keep later changes out of the checkpoint, then prove recovery rolls them back.
                        execute("ALTER VIEW v AS SELECT x FROM base WHERE x > 9");
                        drainWalAndViewQueues();
                        restoreCheckpoint();
                        assertQuery("v").noLeakCheck().returns("x\n8\n9\n10\n");
                    }
                } finally {
                    engine.checkpointRelease();
                }
            }
            assertAlterAfterRestore(3);
        });
    }

    private void createView() throws SqlException {
        execute("CREATE TABLE base AS (SELECT x FROM long_sequence(10))");
        execute("CREATE VIEW v AS SELECT x FROM base WHERE x > 5");
        try (Path path = new Path()) {
            testFilesFacade.checkpointTxnPath = path.of(configuration.getCheckpointRoot())
                    .concat(configuration.getDbDirectory()).concat(engine.verifyTableName("v"))
                    .concat(TableUtils.TXN_FILE_NAME).toString();
        }
    }

    private void restoreCheckpoint() throws Exception {
        engine.clear();
        engine.closeNameRegistry();
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).parent().concat(TableUtils.RESTORE_FROM_CHECKPOINT_TRIGGER_FILE_NAME);
            Assert.assertTrue(Files.touch(path.$()));
        }
        engine.checkpointRecover();
        engine.reloadTableNames();
        engine.getMetadataCache().onStartupAsyncHydrator();
        engine.buildViewGraphs();
        drainWalAndViewQueues();
    }

    private static class TestFilesFacade extends TestFilesFacadeImpl {
        private Runnable beforeViewWrite;
        private String checkpointTxnPath;
        private boolean isTxnOpenFailureEnabled;

        @Override
        public long openRW(LPSZ name, int opts) {
            if (Utf8s.containsAscii(name, TableUtils.CHECKPOINT_DIRECTORY)) {
                if (Utf8s.endsWithAscii(name, ViewDefinition.VIEW_DEFINITION_FILE_NAME) && beforeViewWrite != null) {
                    Runnable action = beforeViewWrite;
                    beforeViewWrite = null;
                    action.run();
                }
                if (isTxnOpenFailureEnabled && Utf8s.equalsUtf16(checkpointTxnPath, name)) {
                    isTxnOpenFailureEnabled = false;
                    return -1;
                }
            }
            return super.openRW(name, opts);
        }
    }
}

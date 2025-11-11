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

package io.questdb.test.cairo.fuzz;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.griffin.SqlException;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

public class CheckpointFuzzTest extends AbstractFuzzTest {
    static int SCOREBOARD_FORMAT = 1;
    private static Path triggerFilePath;

    public CheckpointFuzzTest() throws Exception {
        int scoreboardFormat = TestUtils.generateRandomForTestParams(LOG).nextBoolean() ? 1 : 2;
        if (scoreboardFormat != SCOREBOARD_FORMAT) {
            SCOREBOARD_FORMAT = scoreboardFormat;
            tearDownStatic();
            setUpStatic();
        }
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        setProperty(PropertyKey.CAIRO_TXN_SCOREBOARD_FORMAT, SCOREBOARD_FORMAT);
        AbstractFuzzTest.setUpStatic();
        triggerFilePath = new Path();
    }

    @AfterClass
    public static void tearDownStatic() {
        triggerFilePath = Misc.free(triggerFilePath);
        AbstractFuzzTest.tearDownStatic();
    }

    @Before
    public void setUp() {
        super.setUp();
        triggerFilePath.of(engine.getConfiguration().getDbRoot()).parent().concat(TableUtils.RESTORE_FROM_CHECKPOINT_TRIGGER_FILE_NAME);
    }

    @Test
    public void testCheckpointEjectedWalApply() throws Exception {
        Rnd rnd = generateRandom(LOG);
        fuzzer.setFuzzProbabilities(
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                1,
                0.5,
                0.0,
                0,
                0,
                0.5,
                0.01,
                0
        );

        fuzzer.setFuzzCounts(
                rnd.nextBoolean(),
                rnd.nextInt(2_000_000),
                rnd.nextInt(1000),
                rnd.nextInt(3),
                rnd.nextInt(5),
                rnd.nextInt(1000),
                rnd.nextInt(1_000_000),
                5 + rnd.nextInt(10)
        );

        setFuzzProperties(
                1,
                getRndO3PartitionSplit(rnd),
                getRndO3PartitionSplitMaxCount(rnd),
                10 * Numbers.SIZE_1MB,
                3
        );
        runFuzzWithCheckpoint(rnd);
    }

    @Test
    public void testCheckpointFrequentTableDrop() throws Exception {
        Rnd rnd = generateRandom(LOG);
        fuzzer.setFuzzProbabilities(
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0.1,
                0.5,
                0.0,
                0,
                1,
                0.0,
                0.01,
                0
        );

        fuzzer.setFuzzCounts(
                rnd.nextBoolean(),
                rnd.nextInt(2_000_000),
                rnd.nextInt(1000),
                rnd.nextInt(3),
                rnd.nextInt(5),
                rnd.nextInt(1000),
                rnd.nextInt(1_000_000),
                5 + rnd.nextInt(10)
        );

        setFuzzProperties(1, getRndO3PartitionSplit(rnd), getRndO3PartitionSplitMaxCount(rnd), 10 * Numbers.SIZE_1MB, 3);
        runFuzzWithCheckpoint(rnd);
    }

    @Test
    public void testCheckpointFullFuzz() throws Exception {
        Rnd rnd = generateRandom(LOG);
        fullFuzz(rnd);
        setFuzzProperties(rnd.nextLong(50), getRndO3PartitionSplit(rnd), getRndO3PartitionSplitMaxCount(rnd), 10 * Numbers.SIZE_1MB, 3);
        runFuzzWithCheckpoint(rnd);
    }

    private static void createTriggerFile() {
        Files.touch(triggerFilePath.$());
    }

    private void checkpointCreate(boolean legacy, boolean hardLinkCopy) throws SqlException {
        LOG.info().$("creating checkpoint").$();

        if (legacy) {
            execute("snapshot prepare");
        } else {
            execute("checkpoint create");
        }
        CairoConfiguration conf = engine.getConfiguration();

        FilesFacade ff = conf.getFilesFacade();
        Path snapshotPath = Path.getThreadLocal(conf.getDbRoot()).put(TableUtils.CHECKPOINT_META_FILE_NAME);
        Path rootPath = Path.getThreadLocal2(conf.getDbRoot());

        ff.mkdirs(snapshotPath, conf.getMkDirMode());

        if (hardLinkCopy) {
            LOG.info().$("hard linking data to the checkpoint [from=").$(rootPath).$(", to=").$(snapshotPath).$();
            hardLinkCopyRecursiveIgnoreErrors(ff, rootPath, snapshotPath, conf.getMkDirMode());
        } else {
            LOG.info().$("copying data to the checkpoint [from=").$(rootPath).$(", to=").$(snapshotPath).$();
            copyRecursiveIgnoreErrors(ff, rootPath, snapshotPath, conf.getMkDirMode());
        }

        if (legacy) {
            execute("snapshot complete");
        } else {
            execute("checkpoint release");
        }
    }

    private void checkpointRecover() {
        LOG.info().$("begin checkpoint restore").$();
        engine.releaseInactive();

        CairoConfiguration conf = engine.getConfiguration();
        FilesFacade ff = conf.getFilesFacade();
        Path snapshotPath = Path.getThreadLocal(conf.getDbRoot()).put(TableUtils.CHECKPOINT_META_FILE_NAME).slash();
        Path rootPath = Path.getThreadLocal2(conf.getDbRoot()).slash();

        ff.rmdir(rootPath);
        ff.rename(snapshotPath.$(), rootPath.$());

        LOG.info().$("recovering from the checkpoint").$();
        createTriggerFile();
        engine.checkpointRecover();
        engine.getTableSequencerAPI().releaseAll();
        engine.reloadTableNames();
    }

    private void copyRecursiveIgnoreErrors(FilesFacade ff, Path src, Path dst, int dirMode) {
        int dstLen = dst.size();
        int srcLen = src.size();
        int len = src.size();
        long p = ff.findFirst(src.$());

        if (!ff.exists(dst.$()) && -1 == ff.mkdir(dst.$(), dirMode)) {
            LOG.info().$("failed to copy, cannot create dst dir ").$(src).$(" to ").$(dst)
                    .$(", errno: ").$(ff.errno()).$();
        }

        if (p > 0) {
            try {
                int res;
                do {
                    long name = ff.findName(p);
                    if (Files.notDots(name)) {
                        int type = ff.findType(p);
                        src.trimTo(len);
                        src.concat(name);
                        dst.concat(name);
                        if (type == Files.DT_FILE) {
                            res = Files.copy(src.$(), dst.$());
                            if (res != 0) {
                                LOG.info().$("failed to copy ").$(src).$(" to ").$(dst)
                                        .$(", errno: ").$(ff.errno()).$();
                            }
                        } else {
                            ff.mkdir(dst.$(), dirMode);
                            copyRecursiveIgnoreErrors(ff, src, dst, dirMode);
                        }
                        src.trimTo(srcLen);
                        dst.trimTo(dstLen);
                    }
                } while (ff.findNext(p) > 0);
            } finally {
                ff.findClose(p);
                src.trimTo(srcLen);
                dst.trimTo(dstLen);
            }
        }
    }

    private void fullFuzz(Rnd rnd) {
        fuzzer.setFuzzProbabilities(
                0.5 * rnd.nextDouble(),
                rnd.nextDouble(),
                rnd.nextDouble(),
                0.5 * rnd.nextDouble(),
                rnd.nextDouble(),
                rnd.nextDouble(),
                rnd.nextDouble(),
                rnd.nextDouble(),
                rnd.nextDouble(),
                0.01,
                0.0,
                0.1 * rnd.nextDouble(),
                rnd.nextDouble(),
                0.0,
                0.01,
                0
        );

        fuzzer.setFuzzCounts(
                rnd.nextBoolean(),
                rnd.nextInt(2_000_000),
                rnd.nextInt(1000),
                rnd.nextInt(3),
                rnd.nextInt(5),
                rnd.nextInt(1000),
                rnd.nextInt(1_000_000),
                5 + rnd.nextInt(10)
        );
    }

    private String getTestTableName() {
        return testName.getMethodName().replace('[', '_').replace(']', '_');
    }

    private void hardLinkCopyRecursiveIgnoreErrors(FilesFacade ff, Path src, Path dst, int dirMode) {
        int dstLen = dst.size();
        int srcLen = src.size();
        int len = src.size();
        long p = ff.findFirst(src.$());

        if (!ff.exists(dst.$()) && -1 == ff.mkdir(dst.$(), dirMode)) {
            LOG.info().$("failed to copy, cannot create dst dir ").$(src).$(" to ").$(dst)
                    .$(", errno: ").$(ff.errno()).$();
        }

        if (p > 0) {
            try {
                int res;
                do {
                    long name = ff.findName(p);
                    if (Files.notDots(name)) {
                        int type = ff.findType(p);
                        src.trimTo(len);
                        src.concat(name);
                        dst.concat(name);
                        if (type == Files.DT_FILE) {
                            res = Files.hardLink(src.$(), dst.$());
                            if (res != 0) {
                                LOG.info().$("failed to copy ").$(src).$(" to ").$(dst)
                                        .$(", errno: ").$(ff.errno()).$();
                            }
                        } else {
                            ff.mkdir(dst.$(), dirMode);
                            hardLinkCopyRecursiveIgnoreErrors(ff, src, dst, dirMode);
                        }
                        src.trimTo(srcLen);
                        dst.trimTo(dstLen);
                    }
                } while (ff.findNext(p) > 0);
            } finally {
                ff.findClose(p);
                src.trimTo(srcLen);
                dst.trimTo(dstLen);
            }
        }
    }

    protected void runFuzzWithCheckpoint(Rnd rnd) throws Exception {
        // Snapshot is not supported on Windows.
        Assume.assumeFalse(Os.isWindows());
        boolean testHardLinkCheckpoint = rnd.nextBoolean();

        assertMemoryLeak(() -> {
            if (testHardLinkCheckpoint) {
                node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "100G");
            }

            String tableNameNonWal = getTestTableName() + "_non_wal";
            String tableNameWal = getTestTableName();
            TableToken walTable = fuzzer.createInitialTableWal(tableNameWal, fuzzer.initialRowCount);
            ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(tableNameWal, rnd);

            fuzzer.createInitialTableNonWal(tableNameNonWal, transactions);
            if (rnd.nextBoolean()) {
                drainWalQueue();
            }

            try {
                int snapshotIndex = 1 + rnd.nextInt(transactions.size() - 1);

                ObjList<FuzzTransaction> beforeSnapshot = new ObjList<>();
                beforeSnapshot.addAll(transactions, 0, snapshotIndex);
                ObjList<FuzzTransaction> afterSnapshot = new ObjList<>();
                afterSnapshot.addAll(transactions, snapshotIndex, transactions.size());

                fuzzer.applyToWal(beforeSnapshot, tableNameWal, rnd.nextInt(2) + 1, rnd);

                AtomicReference<Throwable> ex = new AtomicReference<>();
                Thread asyncWalApply = new Thread(() -> {
                    try {
                        drainWalQueue();
                    } catch (Throwable th) {
                        ex.set(th);
                    } finally {
                        Path.clearThreadLocals();
                    }
                });
                asyncWalApply.start();

                Os.sleep(rnd.nextLong(snapshotIndex * 50L));
                // Make snapshot here
                checkpointCreate((rnd.nextInt() >> 30) == 1, testHardLinkCheckpoint);

                asyncWalApply.join();

                if (ex.get() != null) {
                    throw new RuntimeException(ex.get());
                }

                // Restore snapshot here
                checkpointRecover();
                engine.notifyWalTxnRepublisher(engine.verifyTableName(tableNameWal));
                if (afterSnapshot.size() > 0) {
                    fuzzer.applyWal(afterSnapshot, tableNameWal, rnd.nextInt(2) + 1, rnd);
                } else {
                    drainWalQueue();
                }

                Assert.assertFalse("table suspended", engine.getTableSequencerAPI().isSuspended(walTable));

                // Write same data to non-wal table
                fuzzer.applyNonWal(transactions, tableNameNonWal, rnd);

                String limit = "";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, tableNameNonWal + limit, tableNameWal + limit, LOG);
                fuzzer.assertRandomIndexes(tableNameNonWal, tableNameWal, rnd);
            } finally {
                Misc.freeObjListAndClear(transactions);
            }
        });
    }
}

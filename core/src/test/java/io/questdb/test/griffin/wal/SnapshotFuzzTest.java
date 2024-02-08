/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.griffin.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableToken;
import io.questdb.griffin.SqlException;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

public class SnapshotFuzzTest extends AbstractFuzzTest {
    public TableToken createInitialTable(String tableName, boolean isWal, int rowCount) throws SqlException {
        return fuzzer.createInitialTable(tableName, isWal, rowCount);
//        if (engine.getTableTokenIfExists(tableName) == null) {
//            engine.ddl(
//                    "create table " + tableName + " as (" +
//                            "select x as c1, " +
//                            " rnd_symbol('AB', 'BC', 'CD') c2, " +
//                            " timestamp_sequence('2022-02-24', 1000000L) ts, " +
//                            " cast(x as int) c3," +
//                            " rnd_bin() c4," +
//                            " to_long128(3 * x, 6 * x) c5," +
//                            " rnd_str('a', 'bdece', null, ' asdflakji idid', 'dk')," +
//                            " rnd_boolean() bool1 " +
//                            " from long_sequence(" + rowCount + ")" +
//                            ") timestamp(ts) partition by DAY " + (isWal ? "WAL" : "BYPASS WAL"),
//                    sqlExecutionContext
//            );
//        }
//        return engine.verifyTableName(tableName);
    }

    @Test
    public void testFullFuzz() throws Exception {
        Rnd rnd = generateRandom(LOG, 319384652217708L, 1707406079199L);
        fullFuzz(rnd);
        runFuzzWithSnapshot(rnd);
    }

    private void createSnapshot() throws SqlException {
        setProperty(PropertyKey.CAIRO_SNAPSHOT_INSTANCE_ID, "id_1");
        LOG.info().$("starting snapshot").$();

        ddl("snapshot prepare");
        CairoConfiguration conf = engine.getConfiguration();

        FilesFacade ff = conf.getFilesFacade();
        Path snapshotPath = Path.getThreadLocal(conf.getRoot()).put("_snapshot").$();
        Path rootPath = Path.getThreadLocal2(conf.getRoot()).$();

        ff.mkdirs(snapshotPath, conf.getMkDirMode());

        LOG.info().$("copying data to snapshot [from=").$(rootPath).$(", to=").$(snapshotPath).$();
        ff.copyRecursive(rootPath, snapshotPath, conf.getMkDirMode());

        ddl("snapshot complete");
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
                0.1 * rnd.nextDouble(), 0.01,
                rnd.nextDouble()
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

    private void restoreSnapshot() {
        LOG.info().$("begin snapshot restore").$();
        engine.releaseInactive();

        CairoConfiguration conf = engine.getConfiguration();
        FilesFacade ff = conf.getFilesFacade();
        Path snapshotPath = Path.getThreadLocal(conf.getRoot()).put("_snapshot").slash$();
        Path rootPath = Path.getThreadLocal2(conf.getRoot()).slash$();

        ff.rmdir(rootPath);
        ff.rename(snapshotPath, rootPath);

        setProperty(PropertyKey.CAIRO_SNAPSHOT_INSTANCE_ID, "id_2");

        LOG.info().$("recovering from snapshot").$();
        engine.recoverSnapshot();
        engine.getTableSequencerAPI().releaseAll();
        engine.reloadTableNames();
    }

    protected void runFuzzWithSnapshot(Rnd rnd) throws Exception {
        int size = rnd.nextInt(16 * 1024 * 1024);
        node1.setProperty(PropertyKey.DEBUG_CAIRO_O3_COLUMN_MEMORY_SIZE, size);

        String tableNameNonWal = testName.getMethodName() + "_non_wal";
        createInitialTable(tableNameNonWal, false, 0);
        String tableNameWal = testName.getMethodName();
        TableToken walTable = createInitialTable(tableNameWal, true, fuzzer.initialRowCount);

        assertMemoryLeak(() -> {
            ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(tableNameNonWal, rnd);
            int snapshotIndex = rnd.nextInt(transactions.size());

            ObjList<FuzzTransaction> beforeSnapshot = new ObjList<>();
            beforeSnapshot.addAll(transactions, 0, snapshotIndex);
            ObjList<FuzzTransaction> afterSnapshot = new ObjList<>();
            afterSnapshot.addAll(transactions, snapshotIndex, transactions.size());

            fuzzer.applyToWal(beforeSnapshot, tableNameWal, 1, rnd);

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

            // Make snapshot here
            createSnapshot();

            asyncWalApply.join();

            if (ex.get() != null) {
                throw new RuntimeException(ex.get());
            }

            // Restore snapshot here
            restoreSnapshot();
            engine.notifyWalTxnRepublisher(engine.verifyTableName(tableNameWal));
            if (afterSnapshot.size() > 0) {
                fuzzer.applyWal(afterSnapshot, tableNameWal, 1, rnd);
            } else {
                drainWalQueue();
            }

            Assert.assertFalse("table suspended", engine.getTableSequencerAPI().isSuspended(walTable));

            // Write same data to non-wal table
            fuzzer.applyNonWal(transactions, tableNameNonWal, rnd);

            String limit = "";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, tableNameNonWal + limit, tableNameWal + limit, LOG);
            fuzzer.assertRandomIndexes(tableNameNonWal, tableNameWal, rnd);
        });
    }
}

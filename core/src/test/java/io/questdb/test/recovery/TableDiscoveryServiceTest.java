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

package io.questdb.test.recovery;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.recovery.DiscoveredTable;
import io.questdb.recovery.RecoveryIssueCode;
import io.questdb.recovery.TableDiscoveryService;
import io.questdb.recovery.TableDiscoveryState;
import io.questdb.std.FilesFacade;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

public class TableDiscoveryServiceTest extends AbstractCairoTest {
    private static final FilesFacade FF = TestFilesFacadeImpl.INSTANCE;

    @Test
    public void testDetectsHasTxnTable() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("disc_has_txn", false, 3);
            DiscoveredTable table = findByTableName(discover(), "disc_has_txn");
            Assert.assertNotNull(table);
            Assert.assertEquals(TableDiscoveryState.HAS_TXN, table.getState());
            Assert.assertTrue(table.isWalEnabledKnown());
            Assert.assertFalse(table.isWalEnabled());
        });
    }

    @Test
    public void testDetectsNoTxnTable() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "disc_no_txn";
            createTableWithRows(tableName, false, 3);
            removeTableFile(tableName, TableUtils.TXN_FILE_NAME);

            DiscoveredTable table = findByTableName(discover(), tableName);
            Assert.assertNotNull(table);
            Assert.assertEquals(TableDiscoveryState.NO_TXN, table.getState());
        });
    }

    @Test
    public void testDetectsWalOnlyTable() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "disc_wal_only";
            createTableWithRows(tableName, true, 3);
            removeTableFile(tableName, TableUtils.TXN_FILE_NAME);

            DiscoveredTable table = findByTableName(discover(), tableName);
            Assert.assertNotNull(table);
            Assert.assertEquals(TableDiscoveryState.WAL_ONLY, table.getState());
            Assert.assertTrue(table.isWalEnabledKnown());
            Assert.assertTrue(table.isWalEnabled());
        });
    }

    @Test
    public void testFallsBackToDirNameWhenNameFileMissing() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "disc_name_fallback";
            createTableWithRows(tableName, true, 2);
            removeTableFile(tableName, TableUtils.TABLE_NAME_FILE);

            TableToken token = engine.verifyTableName(tableName);
            DiscoveredTable table = findByDirName(discover(), token.getDirName());
            Assert.assertNotNull(table);
            Assert.assertEquals(tableName, table.getTableName());
        });
    }

    @Test
    public void testReportsCorruptMeta() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "disc_corrupt_meta";
            createTableWithRows(tableName, false, 1);

            try (Path metaPath = tablePathOf(tableName)) {
                metaPath.concat(TableUtils.META_FILE_NAME);
                long fd = FF.openRW(metaPath.$(), CairoConfiguration.O_NONE);
                Assert.assertTrue(fd > -1);
                try {
                    Assert.assertTrue(FF.truncate(fd, 8));
                } finally {
                    FF.close(fd);
                }
            }

            DiscoveredTable table = findByTableName(discover(), tableName);
            Assert.assertNotNull(table);
            Assert.assertTrue(hasIssue(table, RecoveryIssueCode.SHORT_FILE));
        });
    }

    @Test
    public void testDiscoverEmptyDatabase() throws Exception {
        assertMemoryLeak(() -> {
            io.questdb.std.ObjList<DiscoveredTable> tables = discover();
            // may contain telemetry/telemetry_config tables; just verify no crash
            for (int i = 0, n = tables.size(); i < n; i++) {
                Assert.assertNotNull(tables.getQuick(i).getDirName());
            }
        });
    }

    @Test
    public void testDiscoverMultipleTableTypes() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("disc_multi_wal", true, 2);
            createTableWithRows("disc_multi_nonwal", false, 2);

            io.questdb.std.ObjList<DiscoveredTable> tables = discover();
            DiscoveredTable wal = findByTableName(tables, "disc_multi_wal");
            Assert.assertNotNull("WAL table should be discovered", wal);
            Assert.assertTrue(wal.isWalEnabledKnown());
            Assert.assertTrue(wal.isWalEnabled());
            Assert.assertEquals(TableDiscoveryState.HAS_TXN, wal.getState());

            DiscoveredTable nonWal = findByTableName(tables, "disc_multi_nonwal");
            Assert.assertNotNull("non-WAL table should be discovered", nonWal);
            Assert.assertTrue(nonWal.isWalEnabledKnown());
            Assert.assertFalse(nonWal.isWalEnabled());
            Assert.assertEquals(TableDiscoveryState.HAS_TXN, nonWal.getState());
        });
    }

    @Test
    public void testDiscoverTableDirWithNoMetadata() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "disc_no_meta";
            createTableWithRows(tableName, false, 1);
            removeTableFile(tableName, TableUtils.TXN_FILE_NAME);
            removeTableFile(tableName, TableUtils.META_FILE_NAME);

            DiscoveredTable table = findByTableName(discover(), tableName);
            Assert.assertNotNull("table dir should still be discovered", table);
            Assert.assertEquals(TableDiscoveryState.NO_TXN, table.getState());
        });
    }

    @Test
    public void testDiscoverTableWithEmptyNameFile() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "disc_empty_name";
            createTableWithRows(tableName, false, 1);

            // truncate _name file to 0 bytes
            TableToken token = engine.verifyTableName(tableName);
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token).concat(TableUtils.TABLE_NAME_FILE);
                long fd = FF.openRW(path.$(), CairoConfiguration.O_NONE);
                Assert.assertTrue(fd > -1);
                try {
                    Assert.assertTrue(FF.truncate(fd, 0));
                } finally {
                    FF.close(fd);
                }
            }

            // table should still be discovered (fallback to dir name)
            DiscoveredTable table = findByDirName(discover(), token.getDirName());
            Assert.assertNotNull("table should be discovered even with empty _name", table);
        });
    }

    private static void createTableWithRows(String tableName, boolean walEnabled, int rowCount) throws SqlException {
        execute(
                "create table "
                        + tableName
                        + " (sym symbol, ts timestamp) timestamp(ts) partition by DAY "
                        + (walEnabled ? "WAL" : "BYPASS WAL")
        );
        execute(
                "insert into "
                        + tableName
                        + " select rnd_symbol('AA', 'BB', 'CC'), timestamp_sequence('1970-01-01', "
                        + Micros.DAY_MICROS
                        + "L) from long_sequence("
                        + rowCount
                        + ")"
        );
        waitForRows(tableName, rowCount, walEnabled);
    }

    private static long getRowCount(String tableName) throws SqlException {
        try (RecordCursorFactory factory = select("select count() from " + tableName)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                return cursor.getRecord().getLong(0);
            }
        }
    }

    private static void waitForRows(String tableName, int expectedRows, boolean walEnabled) throws SqlException {
        if (!walEnabled) {
            Assert.assertEquals(expectedRows, getRowCount(tableName));
            return;
        }

        for (int i = 0; i < 20; i++) {
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            drainWalQueue(engine);
            if (getRowCount(tableName) == expectedRows) {
                return;
            }
        }
        Assert.assertEquals(expectedRows, getRowCount(tableName));
    }

    private static io.questdb.std.ObjList<DiscoveredTable> discover() {
        return new TableDiscoveryService(FF).discoverTables(configuration.getDbRoot());
    }

    private static DiscoveredTable findByDirName(io.questdb.std.ObjList<DiscoveredTable> tables, String dirName) {
        for (int i = 0, n = tables.size(); i < n; i++) {
            DiscoveredTable table = tables.getQuick(i);
            if (dirName.equals(table.getDirName())) {
                return table;
            }
        }
        return null;
    }

    private static DiscoveredTable findByTableName(io.questdb.std.ObjList<DiscoveredTable> tables, String tableName) {
        for (int i = 0, n = tables.size(); i < n; i++) {
            DiscoveredTable table = tables.getQuick(i);
            if (tableName.equals(table.getTableName())) {
                return table;
            }
        }
        return null;
    }

    private static boolean hasIssue(DiscoveredTable table, RecoveryIssueCode issueCode) {
        for (int i = 0, n = table.getIssues().size(); i < n; i++) {
            if (table.getIssues().getQuick(i).getCode() == issueCode) {
                return true;
            }
        }
        return false;
    }

    private static void removeTableFile(String tableName, String fileName) {
        try (Path path = tablePathOf(tableName)) {
            Assert.assertTrue(FF.removeQuiet(path.concat(fileName).$()));
        }
    }

    private static Path tablePathOf(String tableName) {
        TableToken tableToken = engine.verifyTableName(tableName);
        return new Path().of(configuration.getDbRoot()).concat(tableToken);
    }
}

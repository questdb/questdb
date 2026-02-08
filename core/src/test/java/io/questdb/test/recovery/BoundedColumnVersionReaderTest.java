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
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.recovery.BoundedColumnVersionReader;
import io.questdb.recovery.ColumnVersionState;
import io.questdb.recovery.ReadIssue;
import io.questdb.recovery.RecoveryIssueCode;
import io.questdb.std.FilesFacade;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

public class BoundedColumnVersionReaderTest extends AbstractCairoTest {
    private static final FilesFacade FF = TestFilesFacadeImpl.INSTANCE;

    @Test
    public void testReadValidCvMinimalRecords() throws Exception {
        assertMemoryLeak(() -> {
            createSimpleTable("cv_valid", 3);

            ColumnVersionState state = readCvState("cv_valid");
            Assert.assertEquals(0, state.getIssues().size());
            Assert.assertNotNull(state.getCvPath());
        });
    }

    @Test
    public void testReadValidCvWithAddedColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table cv_added (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into cv_added values (1, '1970-01-01T00:00:00.000000Z')");
            execute("insert into cv_added values (2, '1970-01-02T00:00:00.000000Z')");
            waitForAppliedRows("cv_added", 2);

            execute("alter table cv_added add column new_col int");
            drainWalQueue(engine);

            execute("insert into cv_added values (3, '1970-01-03T00:00:00.000000Z', 42)");
            waitForAppliedRows("cv_added", 3);

            ColumnVersionState state = readCvState("cv_added");
            Assert.assertEquals(0, state.getIssues().size());
            Assert.assertTrue("should have cv records for added column", state.getRecordCount() > 0);
        });
    }

    @Test
    public void testReadMissingCvFile() throws Exception {
        assertMemoryLeak(() -> {
            createSimpleTable("cv_missing", 1);

            try (Path path = new Path()) {
                TableToken token = engine.verifyTableName("cv_missing");
                path.of(configuration.getDbRoot()).concat(token).concat("_cv");
                Assert.assertTrue(FF.removeQuiet(path.$()));
            }

            ColumnVersionState state = readCvState("cv_missing");
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.MISSING_FILE));
        });
    }

    @Test
    public void testReadTruncatedCvFile() throws Exception {
        assertMemoryLeak(() -> {
            createSimpleTable("cv_trunc", 2);

            try (Path path = new Path()) {
                TableToken token = engine.verifyTableName("cv_trunc");
                path.of(configuration.getDbRoot()).concat(token).concat("_cv");
                long fd = FF.openRW(path.$(), CairoConfiguration.O_NONE);
                Assert.assertTrue(fd > -1);
                try {
                    Assert.assertTrue(FF.truncate(fd, 10));
                } finally {
                    FF.close(fd);
                }
            }

            ColumnVersionState state = readCvState("cv_trunc");
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.SHORT_FILE));
        });
    }

    @Test
    public void testColumnTopLookupReturnsMinusOneForAbsentColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table cv_lookup (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into cv_lookup values (1, '1970-01-01T00:00:00.000000Z')");
            execute("insert into cv_lookup values (2, '1970-01-02T00:00:00.000000Z')");
            waitForAppliedRows("cv_lookup", 2);

            execute("alter table cv_lookup add column new_col int");
            drainWalQueue(engine);

            execute("insert into cv_lookup values (3, '1970-01-03T00:00:00.000000Z', 42)");
            waitForAppliedRows("cv_lookup", 3);

            ColumnVersionState state = readCvState("cv_lookup");
            // column 2 (new_col) was added after partition 1970-01-01; its column top
            // for old partitions should be -1 (not in partition) or have a column top value
            // For the newest partition it should exist
            long colTop = state.getColumnTop(2 * Micros.DAY_MICROS, 2);
            Assert.assertTrue("new column should exist in latest partition", colTop >= 0);
        });
    }

    @Test
    public void testCapsLargeRecordCount() throws Exception {
        assertMemoryLeak(() -> {
            createSimpleTable("cv_cap", 2);

            ColumnVersionState state = readCvStateWithCap("cv_cap", 1);
            // may have 0 or 1 records depending on whether table has cv entries
            // the key point is it doesn't blow up
            Assert.assertTrue(state.getRecordCount() <= 1);
        });
    }

    private static void createSimpleTable(String tableName, int rowCount) throws SqlException {
        execute("create table " + tableName + " (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
        try (WalWriter walWriter = getWalWriter(tableName)) {
            long ts = 0;
            for (int i = 0; i < rowCount; i++) {
                TableWriter.Row row = walWriter.newRow(ts);
                row.putSym(0, (i & 1) == 0 ? "AA" : "BB");
                row.append();
                ts += Micros.DAY_MICROS;
            }
            walWriter.commit();
        }
        waitForAppliedRows(tableName, rowCount);
    }

    private static long getRowCount(String tableName) throws SqlException {
        try (RecordCursorFactory factory = select("select count() from " + tableName)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                return cursor.getRecord().getLong(0);
            }
        }
    }

    private static boolean hasIssue(ColumnVersionState state, RecoveryIssueCode code) {
        for (int i = 0, n = state.getIssues().size(); i < n; i++) {
            if (state.getIssues().getQuick(i).getCode() == code) {
                return true;
            }
        }
        return false;
    }

    private static ColumnVersionState readCvState(String tableName) {
        try (Path path = new Path()) {
            TableToken token = engine.verifyTableName(tableName);
            path.of(configuration.getDbRoot()).concat(token).concat("_cv");
            return new BoundedColumnVersionReader(FF).read(path.$());
        }
    }

    private static ColumnVersionState readCvStateWithCap(String tableName, int maxRecords) {
        try (Path path = new Path()) {
            TableToken token = engine.verifyTableName(tableName);
            path.of(configuration.getDbRoot()).concat(token).concat("_cv");
            return new BoundedColumnVersionReader(FF, maxRecords).read(path.$());
        }
    }

    private static void waitForAppliedRows(String tableName, int expectedRows) throws SqlException {
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
}

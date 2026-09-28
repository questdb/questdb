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

package io.questdb.test.griffin.engine.functions.table;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncGroupByNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.FilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.RuntimeConstGateRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class WaitWalTableFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testNonWalUpdateRejectsLiveWalProgress() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE update_tab (v LONG)");
            final String sql = "UPDATE update_tab SET v = v + 1 WHERE wait_wal_table('update_tab')";
            assertException(
                    sql,
                    sql.indexOf("wait_wal_table"),
                    "UPDATE cannot require live WAL progress"
            );
        });
    }

    @Test
    public void testNonWalUpdateWithSeqTxnRejectsLiveWalProgress() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE update_tab (v LONG)");
            final String sql = "UPDATE update_tab SET v = v + 1 WHERE wait_wal_table('update_tab', 1L)";
            assertException(
                    sql,
                    sql.indexOf("wait_wal_table"),
                    "UPDATE cannot require live WAL progress"
            );
        });
    }

    @Test
    public void testWaitFunctionDoesNotDisableParallelFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE notwal (v LONG)");
            try (RecordCursorFactory factory = select("SELECT * FROM notwal WHERE wait_wal_table('notwal')")) {
                Assert.assertTrue(containsFactory(factory, RuntimeConstGateRecordCursorFactory.class));
                Assert.assertFalse(containsFactory(factory, FilteredRecordCursorFactory.class));
                Assert.assertTrue(factory.supportsPageFrameCursor());
            }
            try (RecordCursorFactory factory = select("SELECT * FROM notwal WHERE wait_wal_table('notwal') AND v > 0")) {
                Assert.assertTrue(containsFactory(factory, AsyncFilteredRecordCursorFactory.class));
                Assert.assertFalse(containsFactory(factory, FilteredRecordCursorFactory.class));
            }
        });
    }

    @Test
    public void testWaitFunctionDoesNotDisableParallelGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE notwal (v LONG)");
            try (RecordCursorFactory factory = select("SELECT first(wait_wal_table('notwal')) FROM notwal")) {
                Assert.assertTrue(
                        containsFactory(factory, AsyncGroupByNotKeyedRecordCursorFactory.class)
                                || containsFactory(factory, AsyncGroupByRecordCursorFactory.class)
                );
            }
        });
    }

    @Test
    public void testWaitFunctionInScalarSubQueryDoesNotDisableParallelFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE notwal (v INT)");
            try (
                    RecordCursorFactory factory = select("""
                            SELECT *
                            FROM notwal
                            WHERE v = (
                                SELECT CASE
                                    WHEN wait_wal_table('notwal') THEN 1
                                    ELSE 0
                                END
                            )
                            """)
            ) {
                Assert.assertTrue(containsFactory(factory, AsyncFilteredRecordCursorFactory.class));
                Assert.assertFalse(containsFactory(factory, FilteredRecordCursorFactory.class));
            }
        });
    }

    @Test
    public void testNonExistentTable() throws Exception {
        // verifyTableName throws during init() for a table that was never created.
        assertMemoryLeak(() -> {
            try (RecordCursorFactory factory = select("select wait_wal_table('does_not_exist')")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    cursor.hasNext();
                    Assert.fail("expected CairoException");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("does not exist"));
                }
            }
        });
    }

    @Test
    public void testNonWalTableReturnsTrueImmediately() throws Exception {
        // On a non-WAL table init() leaves seqTxnTracker null, so getBool returns true
        // without ever parking.
        assertMemoryLeak(() -> {
            execute("create table notwal (v long)");
            try (RecordCursorFactory factory = select("select wait_wal_table('notwal')")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertTrue(cursor.getRecord().getBool(0));
                    Assert.assertFalse(cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testNullTableNameOneArg() throws Exception {
        assertMemoryLeak(() -> {
            try {
                select("select wait_wal_table(null::string)").close();
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("tableName cannot be NULL"));
            }
        });
    }

    @Test
    public void testNullTableNameTwoArg() throws Exception {
        assertMemoryLeak(() -> {
            try {
                select("select wait_wal_table(null::string, 1)").close();
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("tableName cannot be NULL"));
            }
        });
    }

    @Test
    public void testSeqTxnAsColumnReferenceIsRejected() throws Exception {
        // A plain column reference is neither a constant nor a runtime constant. Overload resolution
        // admits it (the seq_txn slot is declared non-constant), so the factory's own guard rejects it
        // at compile time.
        assertMemoryLeak(() -> {
            execute("create table x (v long)");
            assertQuery("select wait_wal_table('x', v) from x")
                    .noLeakCheck()
                    .failsWith("seq_txn argument must be a constant or runtime constant");
        });
    }

    @Test
    public void testSeqTxnAsRuntimeConstantBindVariable() throws Exception {
        // $1 is a runtime constant (LONG bind variable), not a compile-time constant. Before the
        // seq_txn slot was declared non-constant, overload resolution rejected it as an unknown
        // signature; it must now reach newInstance() and pass the constant-or-runtime-constant guard.
        // A non-WAL table leaves seqTxnTracker null, so the function returns immediately and the wait
        // can never park -- the test verifies acceptance of the bind variable, not the wait mechanics.
        assertMemoryLeak(() -> {
            execute("create table notwal (v long)");
            bindVariableService.setLong(0, 42);
            assertQuery("select wait_wal_table('notwal', $1) waited")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            waited
                            true
                            """);
        });
    }

    @Test
    public void testWalUpdateRejectsLiveWalProgress() throws Exception {
        // Leak-checked like its two non-WAL siblings: this is the variant that reaches furthest
        // into generateUpdate, so it is the one that exercises the recordCursorFactory.close()
        // on the rejection path.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE update_tab (ts TIMESTAMP, v LONG)
                    TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            final String sql = "UPDATE update_tab SET v = v + 1 WHERE wait_wal_table('update_tab')";
            assertException(
                    sql,
                    sql.indexOf("wait_wal_table"),
                    "UPDATE cannot require live WAL progress"
            );
        });
    }

    private static boolean containsFactory(RecordCursorFactory factory, Class<?> factoryClass) {
        while (factory != null) {
            if (factoryClass.isInstance(factory)) {
                return true;
            }
            factory = factory.getBaseFactory();
        }
        return false;
    }
}

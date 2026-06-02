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
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class WaitWalTableFunctionFactoryTest extends AbstractCairoTest {

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
}

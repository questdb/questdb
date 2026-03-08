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

package io.questdb.test.cairo.view;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlException;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.*;

public class ViewModificationTest extends AbstractViewTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // override default to test copy
        inputRoot = getCsvRoot();
        inputWorkRoot = unchecked(() -> temp.newFolder("imports" + System.nanoTime()).getAbsolutePath());
        AbstractViewTest.setUpStatic();
    }

    @Test
    public void testCheckViewModification() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table prices (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by day wal"
            );

            createView("price_1h", "select sym, last(price) as price, ts from prices sample by 1h", "prices");

            // copy
            assertCannotModifyView("copy price_1h from 'test-numeric-headers.csv' with header true");
            // rename table
            assertCannotModifyView("rename table price_1h to price_1h_bak");
            // update
            assertCannotModifyView("update price_1h set price = 1.1");
            // insert
            assertCannotModifyView("insert into price_1h values('gbpusd', 1.319, '2024-09-10T12:05')");
            // insert as select
            assertCannotModifyView("insert into price_1h select sym, last(price) as price, ts from prices sample by 1h");
            // alter
            assertCannotModifyView("alter table price_1h add column x int");
            assertCannotModifyView("alter table price_1h rename column sym to sym2");
            assertCannotModifyView("alter table price_1h alter column sym type varchar");
            assertCannotModifyView("alter table price_1h drop column sym");
            assertCannotModifyView("alter table price_1h drop partition where ts > 0");
            assertCannotModifyView("alter table price_1h dedup disable");
            assertCannotModifyView("alter table price_1h set type bypass wal");
            assertCannotModifyView("alter table price_1h set ttl 3 weeks");
            assertCannotModifyView("alter table price_1h set param o3MaxLag = 20s");
            assertCannotModifyView("alter table price_1h resume wal");
            // reindex
            assertCannotModifyView("reindex table price_1h");
            // truncate
            assertCannotModifyView("truncate table price_1h");
            // vacuum
            assertCannotModifyView("vacuum table price_1h");
        });
    }

    @Test
    public void testFactoryIsInvalidatedOnViewAlter() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table prices (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by day wal"
            );

            createView("v", "select sym, last(price) as price, ts from prices sample by 1h", "prices");

            try (RecordCursorFactory select = engine.select("select * from v", sqlExecutionContext)) {
                Assert.assertNotNull(select);
                try (RecordCursor cursor = select.getCursor(sqlExecutionContext)) {
                    // sanity check - we can read from the view
                    cursor.hasNext();
                }

                execute("alter view v as select sym, last(price) as price, ts from prices sample by 30m");

                try (RecordCursor cursor = select.getCursor(sqlExecutionContext)) {
                    cursor.hasNext();
                    Assert.fail("should not be able to read from an altered view");
                } catch (TableReferenceOutOfDateException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "cached query plan cannot be used because table schema has changed");
                }
            }
        });
    }

    @Test
    public void testFactoryIsInvalidatedOnViewDrop() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table prices (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by day wal"
            );

            createView("v", "select sym, last(price) as price, ts from prices sample by 1h", "prices");

            try (RecordCursorFactory select = engine.select("select * from v", sqlExecutionContext)) {
                Assert.assertNotNull(select);
                try (RecordCursor cursor = select.getCursor(sqlExecutionContext)) {
                    // sanity check - we can read from the view
                    cursor.hasNext();
                }

                engine.execute("drop view v");

                try (RecordCursor cursor = select.getCursor(sqlExecutionContext)) {
                    cursor.hasNext();
                    Assert.fail("should not be able to read from a dropped view");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "does not exist");
                }
            }
        });
    }

    @Test
    public void testTryingToDropMatViewAsTable() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table prices (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by day wal"
            );

            createView("price_1h", "select sym, last(price) as price, ts from prices sample by 1h", "prices");

            try {
                execute("drop table price_1h");
                Assert.fail("Expected exception missing");
            } catch (SqlException e) {
                Assert.assertEquals(11, e.getPosition());
                Assert.assertTrue(e.getMessage().contains("table name expected, got view or materialized view name"));
            }
        });
    }

    private static void assertCannotModifyView(String dml) {
        try {
            execute(dml);
            Assert.fail("Expected exception missing");
        } catch (SqlException e) {
            assertContains(e.getMessage(), "cannot modify view [view=price_1h]");
        }
    }
}

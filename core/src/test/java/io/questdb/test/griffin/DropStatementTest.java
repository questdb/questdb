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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.ObjHashSet;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.TABLE_DOES_NOT_EXIST;
import static io.questdb.cairo.TableUtils.TABLE_EXISTS;
import static io.questdb.test.tools.TestUtils.getSystemTablesCount;

public class DropStatementTest extends AbstractCairoTest {
    /* **
     * DROP can be followed by:
     * - TABLE name [;]
     * - TABLES name(,name)* [;]
     * - ALL TABLES [;]
     */

    private final ObjHashSet<TableToken> tableBucket = new ObjHashSet<>();

    @Test
    public void testDropDatabase() throws Exception {
        String tab0 = "public table";
        String tab1 = "shy table";
        String tab2 = "japanese table 向上";
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE \"" + tab0 + "\" (s string)");
            ddl("CREATE TABLE \"" + tab1 + "\" (s string)");
            ddl("CREATE TABLE \"" + tab2 + "\" (s string)");

            drop("DROP ALL TABLES");
            tableBucket.clear();
            engine.getTableTokens(tableBucket, true);
            Assert.assertEquals(getSystemTablesCount(engine), tableBucket.size());
        });
    }

    @Test
    public void testDropTableBusyReader() throws Exception {
        String tab0 = "large table";
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE \"" + tab0 + "\" (a int)");

            try (RecordCursorFactory factory = select("\"" + tab0 + '"')) {
                try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
                    drop("DROP TABLE \"" + tab0 + '"');
                }
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not lock");
            }
        });
    }

    @Test
    public void testDropTableBusyWriter() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE \"large table\" (a int)");

            try (TableWriter ignored = getWriter("large table")) {
                assertException("DROP TABLE \"large table\"");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not lock");
            }
        });
    }

    @Test
    public void testDropTableExisting() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE instrument (a int)");
            Assert.assertEquals(TABLE_EXISTS, engine.getTableStatus("instrument"));
            drop("DROP TABLE instrument");
            Assert.assertEquals(TABLE_DOES_NOT_EXIST, engine.getTableStatus("instrument"));
        });
    }

    @Test
    public void testDropTableIfExists() throws Exception {
        assertMemoryLeak(() -> {
            // non existing table, must not fail
            Assert.assertEquals(TABLE_DOES_NOT_EXIST, engine.getTableStatus("una tabla de queso"));
            drop("DROP TABLE IF EXISTS \"una tabla de queso\";");
            ddl("create table \"una tabla de queso\"(a int)");
            Assert.assertEquals(TABLE_EXISTS, engine.getTableStatus("una tabla de queso"));
            drop("DROP TABLE IF EXISTS \"una tabla de queso\";");
            Assert.assertEquals(TABLE_DOES_NOT_EXIST, engine.getTableStatus("una tabla de queso"));
        });
    }

    @Test
    public void testDropTableMissingFrom() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertException("drop i_am_missing");
            } catch (SqlException e) {
                Assert.assertEquals(5, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "'table' or 'all tables' expected");
            }
        });
    }

    @Test
    public void testDropTableQuoted() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE \"large table\" (a int)");
            Assert.assertEquals(TABLE_EXISTS, engine.getTableStatus("large table"));
            drop("DROP TABLE \"large table\"");
            Assert.assertEquals(TABLE_DOES_NOT_EXIST, engine.getTableStatus("large table"));
        });
    }

    @Test
    public void testDropTableUtf8() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE научный (a int)");
            Assert.assertEquals(TABLE_EXISTS, engine.getTableStatus("научный"));

            drop("DROP TABLE научный");
            Assert.assertEquals(TABLE_DOES_NOT_EXIST, engine.getTableStatus("научный"));
        });
    }

    @Test
    public void testDropTableUtf8Quoted() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE \"научный руководитель\"(a int)");
            Assert.assertEquals(TABLE_EXISTS, engine.getTableStatus("научный руководитель"));

            drop("DROP TABLE \"научный руководитель\"");
            Assert.assertEquals(TABLE_DOES_NOT_EXIST, engine.getTableStatus("научный руководитель"));
        });
    }

    @Test
    public void testDropTableWithDotFailure() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE \"x.csv\" (a int)");
            Assert.assertEquals(TABLE_EXISTS, engine.getTableStatus("x.csv"));

            try {
                assertException("DROP TABLE x.csv");
            } catch (SqlException e) {
                Assert.assertEquals(12, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "unexpected token [.]");
            }

            drop("DROP TABLE \"x.csv\"");
            Assert.assertEquals(TABLE_DOES_NOT_EXIST, engine.getTableStatus("x.csv"));
        });
    }

    @Test
    public void testDropTablesBusyReader() throws Exception {
        String tab0 = "public table";
        String tab1 = "shy table";
        String tab2 = "japanese table 向上";
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE \"" + tab0 + "\" (s string)");
            ddl("CREATE TABLE \"" + tab1 + "\" (s string)");
            ddl("CREATE TABLE \"" + tab2 + "\" (s string)");

            try (RecordCursorFactory factory = select("\"" + tab0 + '"')) {
                try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
                    assertException("DROP ALL TABLES");
                }
            } catch (CairoException expected) {
                TestUtils.assertContains(
                        expected.getFlyweightMessage(),
                        "failed to drop tables ['public table': [-1] could not lock 'public table' [reason='busyReader']]"
                );
            }
            tableBucket.clear();
            engine.getTableTokens(tableBucket, true);
            Assert.assertEquals(1 + getSystemTablesCount(engine), tableBucket.size());
            assertTableBucketContains(tab0);
        });
    }

    @Test
    public void testDropTablesBusyWriter() throws Exception {
        String tab0 = "public table";
        String tab1 = "shy table";
        String tab2 = "japanese table 向上";
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE \"" + tab0 + "\" (s string)", sqlExecutionContext);
            ddl("CREATE TABLE \"" + tab1 + "\" (s string)", sqlExecutionContext);
            ddl("CREATE TABLE \"" + tab2 + "\" (s string)", sqlExecutionContext);

            try (TableWriter ignored = getWriter(tab0)) {
                ddl("DROP ALL TABLES;", sqlExecutionContext);
            } catch (CairoException expected) {
                TestUtils.assertContains(
                        expected.getFlyweightMessage(),
                        "failed to drop tables ['public table': [-1] could not lock 'public table' [reason='test']]"
                );
            }
            tableBucket.clear();
            engine.getTableTokens(tableBucket, true);
            Assert.assertEquals(1 + getSystemTablesCount(engine), tableBucket.size());
            assertTableBucketContains(tab0);
        });
    }

    private void assertTableBucketContains(String tableName) {
        for (int i = 0, n = tableBucket.size(); i < n; i++) {
            if (tableName.equals(tableBucket.get(i).getTableName())) {
                return;
            }
        }

        Assert.fail("Table name: " + tableName + " not found in table bucket");
    }
}

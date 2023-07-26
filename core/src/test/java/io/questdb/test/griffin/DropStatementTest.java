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
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.TABLE_DOES_NOT_EXIST;
import static io.questdb.cairo.TableUtils.TABLE_EXISTS;

public class DropStatementTest extends AbstractGriffinTest {
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
            alter("CREATE TABLE \"" + tab0 + "\" (s string)", sqlExecutionContext);
            alter("CREATE TABLE \"" + tab1 + "\" (s string)", sqlExecutionContext);
            alter("CREATE TABLE \"" + tab2 + "\" (s string)", sqlExecutionContext);

            alter("DROP ALL TABLES", sqlExecutionContext);
            tableBucket.clear();
            engine.getTableTokens(tableBucket, true);
            Assert.assertEquals(0, tableBucket.size());
        });
    }

    @Test
    public void testDropTableBusyReader() throws Exception {
        String tab0 = "large table";
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE \"" + tab0 + "\" (a int)");

            try (RecordCursorFactory factory = fact("\"" + tab0 + '"')) {
                try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
                    ddl("DROP TABLE \"" + tab0 + '"');
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
                fail("DROP TABLE \"large table\"");
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
            ddl("DROP TABLE instrument");
            Assert.assertEquals(TABLE_DOES_NOT_EXIST, engine.getTableStatus("instrument"));
        });
    }

    @Test
    public void testDropTableIfExists() throws Exception {
        assertMemoryLeak(() -> {
            // non existing table, must not fail
            Assert.assertEquals(TABLE_DOES_NOT_EXIST, engine.getTableStatus("una tabla de queso"));
            ddl("DROP TABLE IF EXISTS \"una tabla de queso\";");
            ddl("create tbale \"una tabla de queso\"(a int)");
            Assert.assertEquals(TABLE_EXISTS, engine.getTableStatus("una tabla de queso"));
            ddl("DROP TABLE IF EXISTS \"una tabla de queso\";");
            Assert.assertEquals(TABLE_DOES_NOT_EXIST, engine.getTableStatus("una tabla de queso"));
        });
    }

    @Test
    public void testDropTableMissingFrom() throws Exception {
        assertMemoryLeak(() -> {
            try {
                fail("drop i_am_missing");
            } catch (SqlException e) {
                Assert.assertEquals(5, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "'table' or 'all tables' expected");
            }
        });
    }

    @Test
    public void testDropTableQuoted() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE \"large table\" (a int)", sqlExecutionContext);
            Assert.assertEquals(TABLE_EXISTS, engine.getTableStatus("large table"));
            ddl("DROP TABLE \"large table\"", sqlExecutionContext);
            Assert.assertEquals(TABLE_DOES_NOT_EXIST, engine.getTableStatus("large table"));
        });
    }

    @Test
    public void testDropTableUtf8() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE научный (a int)");
            Assert.assertEquals(TABLE_EXISTS, engine.getTableStatus("научный"));

            ddl("DROP TABLE научный");
            Assert.assertEquals(TABLE_DOES_NOT_EXIST, engine.getTableStatus("научный"));
        });
    }

    @Test
    public void testDropTableUtf8Quoted() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE \"научный руководитель\"(a int)");
            Assert.assertEquals(TABLE_EXISTS, engine.getTableStatus("научный руководитель"));

            ddl("DROP TABLE \"научный руководитель\"");
            Assert.assertEquals(TABLE_DOES_NOT_EXIST, engine.getTableStatus("научный руководитель"));
        });
    }

    @Test
    public void testDropTableWithDotFailure() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE \"x.csv\" (a int)");
            Assert.assertEquals(TABLE_EXISTS, engine.getTableStatus("x.csv"));

            try {
                fail("DROP TABLE x.csv");
            } catch (SqlException e) {
                Assert.assertEquals(12, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "unexpected token [.]");
            }

            ddl("DROP TABLE \"x.csv\"");
            Assert.assertEquals(TABLE_DOES_NOT_EXIST, engine.getTableStatus("x.csv"));
        });
    }

    @Test
    public void testDropTablesBusyReader() throws Exception {
        String tab0 = "public table";
        String tab1 = "shy table";
        String tab2 = "japanese table 向上";
        assertMemoryLeak(() -> {
            alter("CREATE TABLE \"" + tab0 + "\" (s string)");
            alter("CREATE TABLE \"" + tab1 + "\" (s string)");
            alter("CREATE TABLE \"" + tab2 + "\" (s string)");

            try (RecordCursorFactory factory = fact("\"" + tab0 + '"')) {
                try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
                    fail("DROP ALL TABLES");
                }
            } catch (CairoException expected) {
                TestUtils.assertContains(
                        expected.getFlyweightMessage(),
                        "failed to drop tables ['public table': [-1] could not lock 'public table' [reason='busyReader']]"
                );
            }
            tableBucket.clear();
            engine.getTableTokens(tableBucket, true);
            Assert.assertEquals(1, tableBucket.size());
            Assert.assertEquals(tab0, tableBucket.get(0).getTableName());
        });
    }

    @Test
    public void testDropTablesBusyWriter() throws Exception {
        String tab0 = "public table";
        String tab1 = "shy table";
        String tab2 = "japanese table 向上";
        assertMemoryLeak(() -> {
            alter("CREATE TABLE \"" + tab0 + "\" (s string)", sqlExecutionContext);
            alter("CREATE TABLE \"" + tab1 + "\" (s string)", sqlExecutionContext);
            alter("CREATE TABLE \"" + tab2 + "\" (s string)", sqlExecutionContext);

            try (TableWriter ignored = getWriter(tab0)) {
                alter("DROP ALL TABLES;", sqlExecutionContext);
            } catch (CairoException expected) {
                TestUtils.assertContains(
                        expected.getFlyweightMessage(),
                        "failed to drop tables ['public table': [-1] could not lock 'public table' [reason='test']]"
                );
            }
            tableBucket.clear();
            engine.getTableTokens(tableBucket, true);
            Assert.assertEquals(1, tableBucket.size());
            Assert.assertEquals(tab0, tableBucket.get(0).getTableName());
        });
    }
}

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
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlException;
import io.questdb.std.ObjHashSet;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

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
            compile("CREATE TABLE \"" + tab0 + "\" (s string)", sqlExecutionContext);
            compile("CREATE TABLE \"" + tab1 + "\" (s string)", sqlExecutionContext);
            compile("CREATE TABLE \"" + tab2 + "\" (s string)", sqlExecutionContext);

            compile("DROP ALL TABLES", sqlExecutionContext);
            tableBucket.clear();
            engine.getTableTokens(tableBucket, true);
            Assert.assertEquals(0, tableBucket.size());
        });
    }

    @Test
    public void testDropTableBusyReader() throws Exception {
        String tab0 = "large table";
        assertMemoryLeak(() -> {
            CompiledQuery cc = compiler.compile("CREATE TABLE \"" + tab0 + "\" (a int)", sqlExecutionContext);
            Assert.assertEquals(CompiledQuery.CREATE_TABLE, cc.getType());

            try (RecordCursorFactory factory = compiler.compile("\"" + tab0 + '"', sqlExecutionContext).getRecordCursorFactory()) {
                try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
                    compiler.compile("DROP TABLE \"" + tab0 + '"', sqlExecutionContext);
                }
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not lock");
            }
        });
    }

    @Test
    public void testDropTableBusyWriter() throws Exception {
        assertMemoryLeak(() -> {
            CompiledQuery cc = compiler.compile("CREATE TABLE \"large table\" (a int)", sqlExecutionContext);
            Assert.assertEquals(CompiledQuery.CREATE_TABLE, cc.getType());

            try (TableWriter ignored = getWriter("large table")) {
                compiler.compile("DROP TABLE \"large table\"", sqlExecutionContext);
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not lock");
            }
        });
    }

    @Test
    public void testDropTableExisting() throws Exception {
        assertMemoryLeak(() -> {
            CompiledQuery cc = compiler.compile("CREATE TABLE instrument (a int)", sqlExecutionContext);
            Assert.assertEquals(CompiledQuery.CREATE_TABLE, cc.getType());

            cc = compiler.compile("DROP TABLE instrument", sqlExecutionContext);
            Assert.assertEquals(CompiledQuery.DROP, cc.getType());
        });
    }

    @Test
    public void testDropTableIfExists() throws Exception {
        assertMemoryLeak(() -> {
            CompiledQuery cc = compiler.compile("DROP TABLE IF EXISTS \"una tabla de queso\";", sqlExecutionContext);
            Assert.assertEquals(CompiledQuery.DROP, cc.getType());
        });
    }

    @Test
    public void testDropTableMissingFrom() throws Exception {
        assertMemoryLeak(() -> {
            try {
                compiler.compile("drop i_am_missing", sqlExecutionContext);
            } catch (SqlException e) {
                Assert.assertEquals(5, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "'table' or 'all tables' expected");
            }
        });
    }

    @Test
    public void testDropTableQuoted() throws Exception {
        assertMemoryLeak(() -> {
            CompiledQuery cc = compiler.compile("CREATE TABLE \"large table\" (a int)", sqlExecutionContext);
            Assert.assertEquals(CompiledQuery.CREATE_TABLE, cc.getType());

            cc = compiler.compile("DROP TABLE \"large table\"", sqlExecutionContext);
            Assert.assertEquals(CompiledQuery.DROP, cc.getType());
        });
    }

    @Test
    public void testDropTableUtf8() throws Exception {
        assertMemoryLeak(() -> {
            CompiledQuery cc = compiler.compile("CREATE TABLE научный (a int)", sqlExecutionContext);
            Assert.assertEquals(CompiledQuery.CREATE_TABLE, cc.getType());

            cc = compiler.compile("DROP TABLE научный", sqlExecutionContext);
            Assert.assertEquals(CompiledQuery.DROP, cc.getType());
        });
    }

    @Test
    public void testDropTableUtf8Quoted() throws Exception {
        assertMemoryLeak(() -> {
            CompiledQuery cc = compiler.compile("CREATE TABLE \"научный руководитель\"(a int)", sqlExecutionContext);
            Assert.assertEquals(CompiledQuery.CREATE_TABLE, cc.getType());

            cc = compiler.compile("DROP TABLE \"научный руководитель\"", sqlExecutionContext);
            Assert.assertEquals(CompiledQuery.DROP, cc.getType());
        });
    }

    @Test
    public void testDropTableWithDotFailure() throws Exception {
        assertMemoryLeak(() -> {
            CompiledQuery cc = compiler.compile("CREATE TABLE \"x.csv\" (a int)", sqlExecutionContext);
            Assert.assertEquals(CompiledQuery.CREATE_TABLE, cc.getType());

            try {
                compiler.compile("DROP TABLE x.csv", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(12, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "unexpected token [.]");
            }

            cc = compiler.compile("DROP TABLE \"x.csv\"", sqlExecutionContext);
            Assert.assertEquals(CompiledQuery.DROP, cc.getType());
        });
    }

    @Test
    public void testDropTablesBusyReader() throws Exception {
        String tab0 = "public table";
        String tab1 = "shy table";
        String tab2 = "japanese table 向上";
        assertMemoryLeak(() -> {
            compile("CREATE TABLE \"" + tab0 + "\" (s string)", sqlExecutionContext);
            compile("CREATE TABLE \"" + tab1 + "\" (s string)", sqlExecutionContext);
            compile("CREATE TABLE \"" + tab2 + "\" (s string)", sqlExecutionContext);

            try (RecordCursorFactory factory = compiler.compile("\"" + tab0 + '"', sqlExecutionContext).getRecordCursorFactory()) {
                try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
                    compile("DROP ALL TABLES", sqlExecutionContext);
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
            compile("CREATE TABLE \"" + tab0 + "\" (s string)", sqlExecutionContext);
            compile("CREATE TABLE \"" + tab1 + "\" (s string)", sqlExecutionContext);
            compile("CREATE TABLE \"" + tab2 + "\" (s string)", sqlExecutionContext);

            try (TableWriter ignored = getWriter(tab0)) {
                compile("DROP ALL TABLES;", sqlExecutionContext);
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

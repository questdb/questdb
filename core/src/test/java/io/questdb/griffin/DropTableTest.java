/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class DropTableTest extends AbstractGriffinTest {

    @Test
    public void testDropExisting() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CompiledQuery cc = compiler.compile("create table instrument (a int)");
            Assert.assertEquals(CompiledQuery.CREATE_TABLE, cc.getType());

            cc = compiler.compile("drop table instrument");
            Assert.assertEquals(CompiledQuery.DROP, cc.getType());

        });
    }

    @Test
    public void testDropUtf8() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CompiledQuery cc = compiler.compile("create table научный (a int)");
            Assert.assertEquals(CompiledQuery.CREATE_TABLE, cc.getType());

            cc = compiler.compile("drop table научный");
            Assert.assertEquals(CompiledQuery.DROP, cc.getType());
        });
    }

    @Test
    public void testDropUtf8Quoted() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CompiledQuery cc = compiler.compile("create table 'научный руководитель'(a int)");
            Assert.assertEquals(CompiledQuery.CREATE_TABLE, cc.getType());

            cc = compiler.compile("drop table 'научный руководитель'");
            Assert.assertEquals(CompiledQuery.DROP, cc.getType());
        });
    }

    @Test
    public void testDropQuoted() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CompiledQuery cc = compiler.compile("create table 'large table' (a int)");
            Assert.assertEquals(CompiledQuery.CREATE_TABLE, cc.getType());

            cc = compiler.compile("drop table 'large table'");
            Assert.assertEquals(CompiledQuery.DROP, cc.getType());
        });
    }

    @Test
    public void testDropBusyReader() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CompiledQuery cc = compiler.compile("create table 'large table' (a int)");
            Assert.assertEquals(CompiledQuery.CREATE_TABLE, cc.getType());

            try (RecordCursorFactory factory = compiler.compile("'large table'").getRecordCursorFactory()) {
                try (RecordCursor ignored = factory.getCursor()) {
                    compiler.compile("drop table 'large table'");
                }
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Could not lock");
            } finally {
                engine.releaseAllReaders();
            }
        });
    }

    @Test
    public void testDropBusyWriter() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CompiledQuery cc = compiler.compile("create table 'large table' (a int)");
            Assert.assertEquals(CompiledQuery.CREATE_TABLE, cc.getType());

            try (TableWriter ignored = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "large table")) {
                compiler.compile("drop table 'large table'");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Could not lock");
            } finally {
                engine.releaseAllWriters();
            }
        });
    }

    @Test
    public void testDropMissingFrom() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                compiler.compile("drop i_am_missing");
            } catch (SqlException e) {
                Assert.assertEquals(5, e.getPosition());
                TestUtils.assertContains("'table' expected", e.getFlyweightMessage());
            }
        });
    }
}

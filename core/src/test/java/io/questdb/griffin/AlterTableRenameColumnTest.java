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

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.griffin.CompiledQuery.ALTER;

public class AlterTableRenameColumnTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testBadSyntax() throws Exception {
        assertFailure("alter table x rename column l ,m", 30, "to' expected");
    }

    @Test
    public void testNewNameAlreadyExists() throws Exception {
        assertFailure("alter table x rename column l to l", 33, " column already exists");
    }

    @Test
    public void testRenameExpectColumnKeyword() throws Exception {
        assertFailure("alter table x rename", 20, "'column' expected");
    }

    @Test
    public void testRenameExpectColumnName() throws Exception {
        assertFailure("alter table x rename column", 27, "column name expected");
    }

    @Test
    public void testRenameColumn() throws Exception {
        TestUtils.assertMemoryLeak(
                () -> {
                    try {
                        createX();

                        Assert.assertEquals(ALTER, compiler.compile("alter table x rename column e to z", sqlExecutionContext).getType());

                        String expected = "{\"columnCount\":16,\"columns\":[{\"index\":0,\"name\":\"i\",\"type\":\"INT\"},{\"index\":1,\"name\":\"sym\",\"type\":\"SYMBOL\"},{\"index\":2,\"name\":\"amt\",\"type\":\"DOUBLE\"},{\"index\":3,\"name\":\"timestamp\",\"type\":\"TIMESTAMP\"},{\"index\":4,\"name\":\"b\",\"type\":\"BOOLEAN\"},{\"index\":5,\"name\":\"c\",\"type\":\"STRING\"},{\"index\":6,\"name\":\"d\",\"type\":\"DOUBLE\"},{\"index\":7,\"name\":\"z\",\"type\":\"FLOAT\"},{\"index\":8,\"name\":\"f\",\"type\":\"SHORT\"},{\"index\":9,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":10,\"name\":\"ik\",\"type\":\"SYMBOL\"},{\"index\":11,\"name\":\"j\",\"type\":\"LONG\"},{\"index\":12,\"name\":\"k\",\"type\":\"TIMESTAMP\"},{\"index\":13,\"name\":\"l\",\"type\":\"BYTE\"},{\"index\":14,\"name\":\"m\",\"type\":\"BINARY\"},{\"index\":15,\"name\":\"n\",\"type\":\"STRING\"}],\"timestampIndex\":3}";
                        try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x", TableUtils.ANY_TABLE_VERSION)) {
                            sink.clear();
                            reader.getMetadata().toJson(sink);
                            TestUtils.assertEquals(expected, sink);
                        }

                        Assert.assertEquals(0, engine.getBusyWriterCount());
                        Assert.assertEquals(0, engine.getBusyReaderCount());
                    } finally {
                        engine.releaseAllReaders();
                        engine.releaseAllWriters();
                    }
                }
        );
    }

    @Test
    public void testRenameColumnExistingReader() throws Exception {
        TestUtils.assertMemoryLeak(
                () -> {
                    try {
                        createX();

                        try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x", TableUtils.ANY_TABLE_VERSION)) {
                            Assert.assertEquals(ALTER, compiler.compile("alter table x rename column e to z", sqlExecutionContext).getType());
                            String expected = "{\"columnCount\":16,\"columns\":[{\"index\":0,\"name\":\"i\",\"type\":\"INT\"},{\"index\":1,\"name\":\"sym\",\"type\":\"SYMBOL\"},{\"index\":2,\"name\":\"amt\",\"type\":\"DOUBLE\"},{\"index\":3,\"name\":\"timestamp\",\"type\":\"TIMESTAMP\"},{\"index\":4,\"name\":\"b\",\"type\":\"BOOLEAN\"},{\"index\":5,\"name\":\"c\",\"type\":\"STRING\"},{\"index\":6,\"name\":\"d\",\"type\":\"DOUBLE\"},{\"index\":7,\"name\":\"z\",\"type\":\"FLOAT\"},{\"index\":8,\"name\":\"f\",\"type\":\"SHORT\"},{\"index\":9,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":10,\"name\":\"ik\",\"type\":\"SYMBOL\"},{\"index\":11,\"name\":\"j\",\"type\":\"LONG\"},{\"index\":12,\"name\":\"k\",\"type\":\"TIMESTAMP\"},{\"index\":13,\"name\":\"l\",\"type\":\"BYTE\"},{\"index\":14,\"name\":\"m\",\"type\":\"BINARY\"},{\"index\":15,\"name\":\"n\",\"type\":\"STRING\"}],\"timestampIndex\":3}";
                            sink.clear();
                            reader.reload();
                            reader.getMetadata().toJson(sink);
                            TestUtils.assertEquals(expected, sink);
                        }

                        Assert.assertEquals(0, engine.getBusyWriterCount());
                        Assert.assertEquals(0, engine.getBusyReaderCount());
                    } finally {
                        engine.releaseAllReaders();
                        engine.releaseAllWriters();
                    }
                }
        );
    }

    @Test
    public void testRenameColumnAndCheckOpenReader() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table x1 (a int, b double, t timestamp) timestamp(t)", sqlExecutionContext);

            try (TableReader reader = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), "x1")) {
                Assert.assertEquals("b", reader.getMetadata().getColumnName(1));

                try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x1")) {
                    Assert.assertEquals("b", writer.getMetadata().getColumnName(1));
                    writer.renameColumn("b", "bb");
                    Assert.assertEquals("bb", writer.getMetadata().getColumnName(1));
                }

                Assert.assertTrue(reader.reload());
                Assert.assertEquals("bb", reader.getMetadata().getColumnName(1));
            }
        });
    }


    @Test
    public void testExpectActionKeyword() throws Exception {
        assertFailure("alter table x", 13, "'add', 'alter' or 'drop' expected");
    }

    @Test
    public void testExpectTableKeyword() throws Exception {
        assertFailure("alter x", 6, "'table' expected");
    }

    @Test
    public void testExpectTableKeyword2() throws Exception {
        assertFailure("alter", 5, "'table' expected");
    }

    @Test
    public void testExpectTableName() throws Exception {
        assertFailure("alter table", 11, "table name expected");
    }

    @Test
    public void testInvalidColumn() throws Exception {
        assertFailure("alter table x rename column y yy", 28, "Invalid column: y");
    }

    @Test
    public void testTableDoesNotExist() throws Exception {
        assertFailure("alter table y", 12, "table 'y' does not");
    }

    private void assertFailure(String sql, int position, String message) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                createX();
                compiler.compile(sql, sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(position, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), message);
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();
        });
    }

    private void createX() throws SqlException {
        compiler.compile(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(0, 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n" +
                        " from long_sequence(10)" +
                        ") timestamp (timestamp)",
                sqlExecutionContext
        );
    }
}

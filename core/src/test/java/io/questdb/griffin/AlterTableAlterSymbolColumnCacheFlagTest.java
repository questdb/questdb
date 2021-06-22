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

import io.questdb.cairo.RecordCursorPrinter;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import io.questdb.std.str.CharSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AlterTableAlterSymbolColumnCacheFlagTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testAlterExpectColumnKeyword() throws Exception {
        assertFailure("alter table x alter", 19, "'column' expected");
    }

    @Test
    public void testAlterExpectColumnName() throws Exception {
        assertFailure("alter table x alter column", 26, "column name expected");
    }

    @Test
    public void testAlterFlagInNonSymbolColumn() throws Exception {
        assertFailure("alter table x alter column b cache", 29, "Invalid column type - Column should be of type symbol");
    }

    @Test
    public void testAlterSymbolCacheFlagToFalseAndCheckOpenReaderWithCursor() throws Exception {

        String expectedOrdered = "sym\n" +
                "googl\n" +
                "googl\n" +
                "googl\n" +
                "googl\n" +
                "googl\n" +
                "googl\n" +
                "ibm\n" +
                "ibm\n" +
                "msft\n" +
                "msft\n";

        String expectedChronological = "sym\n" +
                "msft\n" +
                "googl\n" +
                "googl\n" +
                "ibm\n" +
                "googl\n" +
                "ibm\n" +
                "googl\n" +
                "googl\n" +
                "googl\n" +
                "msft\n";

        final RecordCursorPrinter printer = new SingleColumnRecordCursorPrinter(1);

        assertMemoryLeak(() -> {

            assertMemoryLeak(this::createX);

            assertQueryPlain(expectedOrdered,
                    "select sym from x order by sym"
            );

            try (TableReader reader = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), "x")) {
                //check cursor before altering symbol column
                sink.clear();
                printer.print(reader.getCursor(), reader.getMetadata(), true, sink);
                Assert.assertEquals(expectedChronological, sink.toString());

                try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "testing")) {
                    writer.changeCacheFlag(1, false);
                }
                //reload reader
                Assert.assertTrue(reader.reload());
                //check cursor after reload
                sink.clear();
                printer.print(reader.getCursor(), reader.getMetadata(), true, sink);
                Assert.assertEquals(expectedChronological, sink.toString());

                try (TableReader reader2 = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), "x")) {
                    sink.clear();
                    printer.print(reader2.getCursor(), reader2.getMetadata(), true, sink);
                    Assert.assertEquals(expectedChronological, sink.toString());
                }
            }
        });

        assertQueryPlain(expectedOrdered,
                "select sym from x order by 1 asc"
        );
    }

    @Test
    public void testAlterSymbolCacheFlagToTrueCheckOpenReaderWithCursor() throws Exception {
        final RecordCursorPrinter printer = new SingleColumnRecordCursorPrinter(1);

        assertMemoryLeak(() -> {
            compiler.compile("create table x (i int, sym symbol nocache) ;", sqlExecutionContext);
            executeInsert("insert into x values (1, 'GBP')\"");
            executeInsert("insert into x values (2, 'CHF')\"");
            executeInsert("insert into x values (3, 'GBP')\"");
            executeInsert("insert into x values (4, 'JPY')\"");
            executeInsert("insert into x values (5, 'USD')\"");
            executeInsert("insert into x values (6, 'GBP')\"");
            executeInsert("insert into x values (7, 'GBP')\"");
            executeInsert("insert into x values (8, 'GBP')\"");
            executeInsert("insert into x values (9, 'GBP')\"");
        });

        String expectedOrdered = "sym\n" +
                "CHF\n" +
                "GBP\n" +
                "GBP\n" +
                "GBP\n" +
                "GBP\n" +
                "GBP\n" +
                "GBP\n" +
                "JPY\n" +
                "USD\n";

        String expectedChronological = "sym\n" +
                "GBP\n" +
                "CHF\n" +
                "GBP\n" +
                "JPY\n" +
                "USD\n" +
                "GBP\n" +
                "GBP\n" +
                "GBP\n" +
                "GBP\n";

        assertMemoryLeak(() -> {

            assertQueryPlain(expectedOrdered,
                    "select sym from x order by sym"
            );

            try (TableReader reader = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), "x")) {
                //check cursor before altering symbol column
                sink.clear();
                printer.print(reader.getCursor(), reader.getMetadata(), true, sink);
                Assert.assertEquals(expectedChronological, sink.toString());

                try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "testing")) {
                    writer.changeCacheFlag(1, true);
                }
                //reload reader
                Assert.assertTrue(reader.reload());
                //check cursor after reload
                sink.clear();
                printer.print(reader.getCursor(), reader.getMetadata(), true, sink);
                Assert.assertEquals(expectedChronological, sink.toString());

                try (TableReader reader2 = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), "x")) {
                    sink.clear();
                    printer.print(reader2.getCursor(), reader2.getMetadata(), true, sink);
                    Assert.assertEquals(expectedChronological, sink.toString());
                }

            }
        });

        assertQueryPlain(expectedOrdered,
                "select sym from x order by 1 asc"
        );
    }

    @Test
    public void testBadSyntax() throws Exception {
        assertFailure("alter table x alter column z", 28, "'add index' or 'cache' or 'nocache' expected");
    }

    @Test
    public void testInvalidColumn() throws Exception {
        assertFailure("alter table x alter column y cache", 29, "Invalid column: y");
    }

    @Test
    public void testWhenCacheOrNocacheAreNotInAlterStatement() throws Exception {
        assertFailure("alter table x alter column z ca", 29, "'cache' or 'nocache' expected");
    }

    private void assertFailure(String sql, int position, String message) throws Exception {
        assertMemoryLeak(() -> {
            try {
                createX();
                compiler.compile(sql, sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(position, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), message);
            }
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
                        ") timestamp (timestamp);",
                sqlExecutionContext
        );
    }

    static class SingleColumnRecordCursorPrinter extends RecordCursorPrinter {

        private final int columnIndex;

        public SingleColumnRecordCursorPrinter(int columnIndex) {
            super();
            this.columnIndex = columnIndex;
        }

        @Override
        public void print(Record r, RecordMetadata m, CharSink sink) {
            printColumn(r, m, columnIndex, sink);
            sink.put("\n");
            sink.flush();
        }

        @Override
        public void printHeader(RecordMetadata metadata, CharSink sink) {
            sink.put(metadata.getColumnName(columnIndex));
            sink.put('\n');
        }

    }
}

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

import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.Unsafe;
import io.questdb.std.str.InlinedVarchar;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8SplitString;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.sql.DataFrameCursorFactory.ORDER_ASC;

public class PageFrameCursorTest extends AbstractCairoTest {

    @Test
    public void testStringColumnWithColumnTop() throws Exception {
        assertMemoryLeak(
                () -> {
                    ddl("create table x as (select" +
                            " rnd_int() a," +
                            " rnd_str() b," +
                            " timestamp_sequence(to_timestamp('2022-01-13T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000L) t" +
                            " from long_sequence(10)" +
                            ") timestamp (t) partition by DAY"
                    );

                    ddl("alter table x add column c string");

                    insert(
                            "insert into x " +
                                    "select" +
                                    " rnd_int() a," +
                                    " rnd_str() b," +
                                    " timestamp_sequence(to_timestamp('2022-01-13T00:00:01', 'yyyy-MM-ddTHH:mm:ss'), 100000L) t," +
                                    " rnd_str() c" +
                                    " from long_sequence(10)"
                    );

                    TestUtils.printSql(
                            engine,
                            sqlExecutionContext,
                            "select b from x",
                            sink
                    );

                    final StringSink actualSink = new StringSink();
                    // header
                    actualSink.put("b\n");

                    try (RecordCursorFactory factory = select("x")) {
                        // test that we can read string column without using index
                        try (PageFrameCursor pageFrameCursor = factory.getPageFrameCursor(sqlExecutionContext, ORDER_ASC)) {
                            PageFrame frame;
                            while ((frame = pageFrameCursor.next()) != null) {
                                long size = frame.getPageSize(1);
                                long topOfVarAddress = frame.getPageAddress(1);
                                long fixAddress = frame.getIndexPageAddress(1);
                                long count = frame.getPartitionHi() - frame.getPartitionLo();
                                while (count > 0) {
                                    //validate that index column has correct offsets
                                    final long offset = Unsafe.getUnsafe().getLong(fixAddress);
                                    Assert.assertTrue(offset >= 0 && offset < size);
                                    fixAddress += 8;
                                    long varAddress = topOfVarAddress + offset;
                                    int len = Unsafe.getUnsafe().getInt(varAddress); // string len
                                    varAddress += 4;
                                    if (len != -1) {
                                        for (int i = 0; i < len; i++) {
                                            actualSink.put(Unsafe.getUnsafe().getChar(varAddress + i * 2L));
                                        }
                                    }
                                    actualSink.put('\n');
                                    count--;
                                }
                            }
                            TestUtils.assertEquals(sink, actualSink);
                        }
                    }
                }
        );
    }

    @Test
    public void testStringSimple() throws Exception {
        assertMemoryLeak(
                () -> {
                    ddl("create table x as (select" +
                            " rnd_int() a," +
                            " rnd_str() b," +
                            " timestamp_sequence(0, 100000000) t" +
                            " from long_sequence(1000)" +
                            ") timestamp (t) partition by DAY"
                    );

                    TestUtils.printSql(
                            engine,
                            sqlExecutionContext,
                            "select b from x",
                            sink
                    );

                    final StringSink actualSink = new StringSink();
                    // header
                    actualSink.put("b\n");

                    try (RecordCursorFactory factory = select("x")) {

                        // test that we can read string column without using index
                        try (PageFrameCursor pageFrameCursor = factory.getPageFrameCursor(sqlExecutionContext, ORDER_ASC)) {
                            PageFrame frame;
                            while ((frame = pageFrameCursor.next()) != null) {
                                long varAddress = frame.getPageAddress(1);
                                long fixAddress = frame.getIndexPageAddress(1);
                                long topOfVarAddress = varAddress;
                                long count = frame.getPartitionHi() - frame.getPartitionLo();
                                while (count > 0) {

                                    // validate that index column has correct offsets
                                    Assert.assertEquals(varAddress - topOfVarAddress, Unsafe.getUnsafe().getLong(fixAddress));
                                    fixAddress += 8;

                                    int len = Unsafe.getUnsafe().getInt(varAddress); // string len
                                    varAddress += 4;
                                    if (len != -1) {
                                        for (int i = 0; i < len; i++) {
                                            actualSink.put(Unsafe.getUnsafe().getChar(varAddress + i * 2L));
                                        }
                                        varAddress += len * 2L;
                                    }
                                    actualSink.put('\n');
                                    count--;
                                }
                                Assert.assertEquals(varAddress - topOfVarAddress, frame.getPageSize(1));
                            }
                            TestUtils.assertEquals(sink, actualSink);
                        }
                    }
                }
        );
    }

    @Test
    public void testVarcharLongColumnTop() throws Exception {
        assertMemoryLeak(() -> testColumnTop(40));
    }

    @Test
    public void testVarcharLongSimple() throws Exception {
        assertMemoryLeak(() -> testSimple(40));
    }

    @Test
    public void testVarcharShortColumnTop() throws Exception {
        assertMemoryLeak(() -> testColumnTop(1));
    }

    @Test
    public void testVarcharShortSimple() throws Exception {
        assertMemoryLeak(() -> testSimple(1));
    }

    private static void readFromPageFrame() throws SqlException {
        TestUtils.printSql(
                engine,
                sqlExecutionContext,
                "select b from x",
                sink
        );

        final Utf8SplitString utf8SplitView = new Utf8SplitString();
        final InlinedVarchar utf8view = new InlinedVarchar();

        final StringSink actualSink = new StringSink();
        // header
        actualSink.put("b\n");

        try (RecordCursorFactory factory = select("x")) {
            try (PageFrameCursor pageFrameCursor = factory.getPageFrameCursor(sqlExecutionContext, ORDER_ASC)) {
                PageFrame frame;
                while ((frame = pageFrameCursor.next()) != null) {
                    final long dataTopAddress = frame.getPageAddress(1);
                    final long auxTopAddress = frame.getIndexPageAddress(1);
                    final long count = frame.getPartitionHi() - frame.getPartitionLo();
                    for (int row = 0; row < count; row++) {
                        actualSink.put(VarcharTypeDriver.getValue(auxTopAddress, dataTopAddress, row, utf8view, utf8SplitView));
                        actualSink.put('\n');
                    }
                }
                TestUtils.assertEquals(sink, actualSink);
            }
        }
    }

    private void testColumnTop(int maxLength) throws SqlException {
        ddl("create table x as (select" +
                " rnd_int() a," +
                " rnd_varchar(1, " + maxLength + ", 1) b," +
                " timestamp_sequence(to_timestamp('2022-01-13T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000L) t" +
                " from long_sequence(10)" +
                ") timestamp (t) partition by DAY"
        );

        ddl("alter table x add column c varchar");

        insert(
                "insert into x " +
                        "select" +
                        " rnd_int() a," +
                        " rnd_varchar(1, " + maxLength + ", 1) b," +
                        " timestamp_sequence(to_timestamp('2022-01-13T00:00:01', 'yyyy-MM-ddTHH:mm:ss'), 100000L) t," +
                        " rnd_varchar(1, " + maxLength + ", 1) c," +
                        " from long_sequence(10)"
        );

        readFromPageFrame();
    }

    private void testSimple(int maxLength) throws SqlException {
        ddl("create table x as (select" +
                " rnd_int() a," +
                " rnd_varchar(1, " + maxLength + ", 1) b," +
                " timestamp_sequence(0, 100000000) t" +
                " from long_sequence(1000)" +
                ") timestamp (t) partition by DAY"
        );

        readFromPageFrame();
    }
}

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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableUtils;
import io.questdb.std.Files;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assume.assumeFalse;

public class LineTcpConnectionContextTest extends BaseLineTcpContextTest {
    private final boolean walEnabled;

    public LineTcpConnectionContextTest() {
        Rnd rnd = TestUtils.generateRandom(AbstractCairoTest.LOG);
        this.walEnabled = TestUtils.isWal(rnd);
        this.timestampType = TestUtils.getTimestampType(rnd);
    }

    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, walEnabled);
    }

    @Test
    public void testAddCastFieldColumnNoTable() throws Exception {
        String tableName = "addCastColumn";
        symbolAsFieldSupported = true;
        runInContext(() -> {
            recvBuffer = tableName + ",location=us-midwest,cast= temperature=82 1465839830100400200\n" +
                    tableName + ",location=us-eastcoast cast=\"cast\",temperature=81,humidity=23 1465839830101400200\n";
            handleIO();
            closeContext();
            drainWalQueue();
            String expected = """
                    location\tcast\ttemperature\ttimestamp\thumidity
                    us-midwest\t\t82.0\t2016-06-13T17:43:50.100400Z\tnull
                    us-eastcoast\tcast\t81.0\t2016-06-13T17:43:50.101400Z\t23.0
                    """;
            try (
                    TableReader reader = newOffPoolReader(configuration, tableName);
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
            ) {
                TableReaderMetadata meta = reader.getMetadata();
                assertCursorTwoPass(expected, cursor, meta);
                Assert.assertEquals(5, meta.getColumnCount());
                Assert.assertEquals(ColumnType.SYMBOL, meta.getColumnType("location"));
                Assert.assertEquals(ColumnType.DOUBLE, meta.getColumnType("temperature"));
                Assert.assertEquals(ColumnType.SYMBOL, meta.getColumnType("cast"));
                Assert.assertEquals(ColumnType.TIMESTAMP, meta.getColumnType("timestamp"));
                Assert.assertEquals(ColumnType.DOUBLE, meta.getColumnType("humidity"));
            }
        });
    }

    @Test
    public void testAddFieldColumn() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "addField";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81,humidity=23 1465839830101400200\n" +
                            table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp\thumidity
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\tnull
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\tnull
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\t23.0
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\tnull
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\tnull
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\tnull
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\tnull
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testAddFloatColumnAsDouble() throws Exception {
        floatDefaultColumnType = ColumnType.DOUBLE;
        testDefaultColumnType(ColumnType.DOUBLE, "24.3", "24.3", "null");
    }

    @Test
    public void testAddFloatColumnAsFloat() throws Exception {
        floatDefaultColumnType = ColumnType.FLOAT;
        testDefaultColumnType(ColumnType.FLOAT, "24.3", "24.3", "null");
    }

    @Test
    public void testAddIntegerColumnAsByte() throws Exception {
        integerDefaultColumnType = ColumnType.BYTE;
        testDefaultColumnType(ColumnType.BYTE, "21i", "21", "0");
    }

    @Test
    public void testAddIntegerColumnAsInt() throws Exception {
        integerDefaultColumnType = ColumnType.INT;
        testDefaultColumnType(ColumnType.INT, "21i", "21", "null");
    }

    @Test
    public void testAddIntegerColumnAsLong() throws Exception {
        integerDefaultColumnType = ColumnType.LONG;
        testDefaultColumnType(ColumnType.LONG, "21i", "21", "null");
    }

    @Test
    public void testAddIntegerColumnAsShort() throws Exception {
        integerDefaultColumnType = ColumnType.SHORT;
        testDefaultColumnType(ColumnType.SHORT, "21i", "21", "0");
    }

    @Test
    public void testAddTagColumn() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "addTag";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast,city=york temperature=81 1465839830101400200\n" +
                            table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp\tcity
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\t
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\t
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\tyork
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\t
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\t
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\t
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\t
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testAddToExistingTable() throws Exception {
        String table = "addToExisting";
        addTable(table);
        runInContext(() -> {
            recvBuffer = makeMessages(table);
            handleIO();
            closeContext();
            String expected = ColumnType.isTimestampMicro(timestampType.getTimestampType())
                    ? """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z
                    """
                    : """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400200Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500200Z
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400200Z
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300200Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400200Z
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400200Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500200Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testBadCast() throws Exception {
        String table = "badCast";
        addTable(table);
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81 1465839830101400200\n" +
                            table + ",location=us-midwest temperature=0x85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = ColumnType.isTimestampMicro(timestampType.getTimestampType())
                    ? """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z
                    """
                    : """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400200Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500200Z
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400200Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400200Z
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400200Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500200Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testBadLineSyntax1() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "badLineSyntax1";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81 1465839830101400200\n" +
                            table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 a=146583983102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleIO();

            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testBadLineSyntax2() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "badLineSyntax2";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81 1465839830101400200\n" +
                            table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast,broken temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleContextIO0();
            Assert.assertFalse(disconnected);
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testBadLineSyntax3() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "badLineSyntax3";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81 1465839830101400200\n" +
                            table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast broken=23 temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleContextIO0();
            Assert.assertFalse(disconnected);
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testBadLineSyntax4() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "badLineSyntax4";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81 1465839830101400200\n" +
                            table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast broken.col=aString,temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleIO();

            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testBadLineSyntax5() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "badLineSyntax5";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81 1465839830101400200\n" +
                            table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast,broken.col=aString temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testBadLineSyntax6() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "badLineSyntax6";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81 1465839830101400200\n" +
                            table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast broken.col=aString,temperature=80 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast,broken.col=aString temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testBadTimestamp() throws Exception {
        String table = "badTimestamp";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81 1465839830101400200\n" +
                            table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 146583983x102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleContextIO0();
            Assert.assertFalse(disconnected);
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testBooleans() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "badBooleans";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-eastcoast raining=true 1465839830100400200\n" +
                            table + ",location=us-midwest raining=false 1465839830100400200\n" +
                            table + ",location=us-midwest raining=f 1465839830100500200\n" +
                            table + ",location=us-midwest raining=t 1465839830102300200\n" +
                            table + ",location=us-eastcoast raining=T 1465839830102400200\n" +
                            table + ",location=us-eastcoast raining=F 1465839830102400200\n" +
                            table + ",location=us-westcost raining=False 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = """
                    location\training\ttimestamp
                    us-eastcoast\ttrue\t2016-06-13T17:43:50.100400Z
                    us-midwest\tfalse\t2016-06-13T17:43:50.100400Z
                    us-midwest\tfalse\t2016-06-13T17:43:50.100500Z
                    us-midwest\ttrue\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\ttrue\t2016-06-13T17:43:50.102400Z
                    us-eastcoast\tfalse\t2016-06-13T17:43:50.102400Z
                    us-westcost\tfalse\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testCairoExceptionOnAddColumn() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        assumeFalse(walEnabled);

        String table = "columnEx";
        runInContext(
                new TestFilesFacadeImpl() {
                    @Override
                    public long openRW(LPSZ name, int opts) {
                        if (Utf8s.endsWithAscii(name, "broken.d.1")) {
                            return -1;
                        }
                        return super.openRW(name, opts);
                    }
                },
                () -> {
                    recvBuffer =
                            table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                                    table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                                    table + ",location=us-eastcoast temperature=81,broken=23 1465839830101400200\n" +
                                    table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                                    table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                                    table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                                    table + ",location=us-westcost temperature=82 1465839830102500200\n";
                    handleIO();
                    closeContext();
                    String expected = """
                            location\ttemperature\ttimestamp
                            us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                            us-midwest\t83.0\t2016-06-13T17:43:50.100500Z
                            """;
                    assertTable(expected, table);
                }, null
        );
    }

    @Test
    public void testCairoExceptionOnCommit() throws Exception {
        assumeFalse(walEnabled);

        String table = "commitException";
        node1.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 1);
        maxRecvBufferSize.set(60);
        runInContext(
                new TestFilesFacadeImpl() {
                    @Override
                    public long openRW(LPSZ name, int opts) {
                        if (Utf8s.endsWithAscii(name, "1970-01-01.1" + Files.SEPARATOR + "temperature.d")) {
                            return -1;
                        }
                        return super.openRW(name, opts);
                    }
                },
                () -> {
                    recvBuffer =
                            table + ",location=us-midwest temperature=82 99000\n" +
                                    table + ",location=us-midwest temperature=83 90000\n" +
                                    table + ",location=us-eastcoast temperature=81,broken=23 80000\n" +
                                    table + ",location=us-midwest temperature=85 70000\n" +
                                    table + ",location=us-eastcoast temperature=89 60000\n" +
                                    table + ",location=us-eastcoast temperature=80 50000\n" +
                                    table + ",location=us-westcost temperature=82 40000\n";
                    do {
                        handleContextIO0();
                    } while (!disconnected && !recvBuffer.isEmpty());

                    Assert.assertTrue(disconnected);
                    Assert.assertFalse(recvBuffer.isEmpty());
                    closeContext();

                    String expected = """
                            location\ttemperature\ttimestamp
                            us-midwest\t82.0\t1970-01-01T00:00:00.000099Z
                            """;
                    assertTable(expected, table);
                },
                null
        );
    }

    @Test
    public void testCairoExceptionOnCreateTable() throws Exception {
        String table = "cairoEx";
        runInContext(
                new TestFilesFacadeImpl() {
                    @Override
                    public long openRW(LPSZ name, int opts) {
                        if (Utf8s.endsWithAscii(name, "broken.d")) {
                            return -1;
                        }
                        return super.openRW(name, opts);
                    }
                },
                () -> {
                    recvBuffer =
                            table + ",location=us-eastcoast temperature=81,broken=23 1465839830101400200\n" +
                                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                                    table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                                    table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                                    table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                                    table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                                    table + ",location=us-westcost temperature=82 1465839830102500200\n";
                    handleIO();
                    closeContext();
                    String expected = "location\ttemperature\tbroken\ttimestamp\n";
                    assertTable(expected, table);
                },
                null
        );
    }

    @Test
    public void testColumnConversion1() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        runInContext(() -> {
            TableModel model = new TableModel(configuration, "t_ilp21", PartitionBy.DAY)
                    .col("event", ColumnType.SHORT)
                    .col("id", ColumnType.LONG256)
                    .col("ts", ColumnType.TIMESTAMP)
                    .col("float1", ColumnType.FLOAT)
                    .col("int1", ColumnType.INT)
                    .col("date1", ColumnType.DATE)
                    .col("byte1", ColumnType.BYTE)
                    .timestamp();
            if (walEnabled) {
                model.wal();
            }
            AbstractCairoTest.create(model);
            timestampTicks = 1465839830102800L;
            recvBuffer = """
                    t_ilp21 event=12i,id=0x05a9796963abad00001e5f6bbdb38i,ts=1465839830102400i,float1=1.2,int1=23i,date1=1465839830102i,byte1=-7i
                    t_ilp21 event=12i,id=0x5a9796963abad00001e5f6bbdb38i,ts=1465839830102400i,float1=1e3,int1=-500000i,date1=1465839830102i,byte1=3i
                    """;
            handleContextIO0();
            Assert.assertFalse(disconnected);
            closeContext();
            String expected = """
                    event\tid\tts\tfloat1\tint1\tdate1\tbyte1\ttimestamp
                    12\t0x5a9796963abad00001e5f6bbdb38\t2016-06-13T17:43:50.102400Z\t1.2\t23\t2016-06-13T17:43:50.102Z\t-7\t2016-06-13T17:43:50.102800Z
                    12\t0x5a9796963abad00001e5f6bbdb38\t2016-06-13T17:43:50.102400Z\t1000.0\t-500000\t2016-06-13T17:43:50.102Z\t3\t2016-06-13T17:43:50.102800Z
                    """;
            assertTable(expected, "t_ilp21");
        });
    }

    @Test
    public void testColumnConversion2() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        assumeFalse(walEnabled); // Wal needs partitioning
        runInContext(() -> {
            TableModel model = new TableModel(configuration, "t_ilp21", PartitionBy.NONE).col("l", ColumnType.LONG);
            AbstractCairoTest.create(model);
            timestampTicks = 1465839830102800L;
            recvBuffer = """
                    t_ilp21 l=843530699759026177i
                    t_ilp21 l="843530699759026178"
                    t_ilp21 l=843530699759026179i
                    """;
            handleContextIO0();
            Assert.assertFalse(disconnected);
            closeContext();
            String expected = """
                    l
                    843530699759026177
                    843530699759026179
                    """;
            assertTable(expected, "t_ilp21");
        });
    }

    @Test
    public void testColumnNameWithSlash1() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "columnSlash";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81,no/way/humidity=23 1465839830101400200\n" +
                            table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testColumnNameWithSlash2() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "colSlash2";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-eastcoast temperature=81,no/way/humidity=23 1465839830101400200\n" +
                            table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testColumnTypeChange() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "typeChange";
        addTable(table);
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81 1465839830101400200\n" +
                            table + ",location=us-midwest temperature=85i 1465839830102300200\n" +
                            table + ",location=us-midwest temperature=tyu 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testDesignatedTimestampAsField() throws Exception {
        String table = "duplicateTimestamp";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81,timestamp=1465839830101600t\n" +
                            table + ",location=us-midwest temperature=85,timestamp=1465839830102300t,Timestamp=1465839830102800t\n" +
                            table + ",location=us-eastcoast temperature=89,Timestamp=1465839830102400t\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101600Z
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testDesignatedTimestampAsFieldInAllRows() throws Exception {
        String table = "duplicateTimestamp";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest timestamp=1465839830100400t,temperature=82\n" +
                            table + ",location=us-midwest timestamp=1465839830100500t,temperature=83\n" +
                            table + ",location=us-eastcoast timestamp=1465839830101600t,temperature=81\n" +
                            table + ",location=us-midwest timestamp=1465839830102300t,temperature=85\n" +
                            table + ",location=us-eastcoast timestamp=1465839830102400t,temperature=89\n" +
                            table + ",location=us-eastcoast timestamp=1465839830102400t,temperature=80\n" +
                            table + ",location=us-westcost timestamp=1465839830102500t,temperature=82\n";
            handleIO();
            closeContext();
            String expected = timestampType.getTimestampType() == ColumnType.TIMESTAMP_NANO
                    ? """
                    location\ttimestamp\ttemperature
                    us-midwest\t2016-06-13T17:43:50.100400000Z\t82.0
                    us-midwest\t2016-06-13T17:43:50.100500000Z\t83.0
                    us-eastcoast\t2016-06-13T17:43:50.101600000Z\t81.0
                    us-midwest\t2016-06-13T17:43:50.102300000Z\t85.0
                    us-eastcoast\t2016-06-13T17:43:50.102400000Z\t89.0
                    us-eastcoast\t2016-06-13T17:43:50.102400000Z\t80.0
                    us-westcost\t2016-06-13T17:43:50.102500000Z\t82.0
                    """
                    : """
                    location\ttimestamp\ttemperature
                    us-midwest\t2016-06-13T17:43:50.100400Z\t82.0
                    us-midwest\t2016-06-13T17:43:50.100500Z\t83.0
                    us-eastcoast\t2016-06-13T17:43:50.101600Z\t81.0
                    us-midwest\t2016-06-13T17:43:50.102300Z\t85.0
                    us-eastcoast\t2016-06-13T17:43:50.102400Z\t89.0
                    us-eastcoast\t2016-06-13T17:43:50.102400Z\t80.0
                    us-westcost\t2016-06-13T17:43:50.102500Z\t82.0
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testDesignatedTimestampAsFieldInFirstRow() throws Exception {
        String table = "duplicateTimestamp";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82,timestamp=1465839830100200t\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81 1465839830101600200\n" +
                            table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleIO();
            closeContext();

            String expected = timestampType.getTimestampType() == ColumnType.TIMESTAMP_NANO
                    ? """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100200000Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500200Z
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101600200Z
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300200Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400200Z
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400200Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500200Z
                    """
                    : """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100200Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101600Z
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testDesignatedTimestampNotCalledTimestampWhenTableExistAlready() throws Exception {
        String table = "tableExistAlready";
        runInContext(() -> {
            execute("create table " + table + " (location SYMBOL, temperature DOUBLE, time " + timestampType.getTypeName() + ") timestamp(time);");
            recvBuffer =
                    table + ",location=us-midwest temperature=82,time=1465839830100300t 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast,city=york temperature=81 1465839830101400200\n" +
                            table + ",location=us-midwest,city=london temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80,time=1465839830102500t\n" +
                            table + ",location=us-westcost temperature=82,time=1465839830102600t 1465839830102700200\n";
            handleIO();
            closeContext();
            String expected = ColumnType.isTimestampMicro(timestampType.getTimestampType())
                    ? """
                    location\ttemperature\ttime\tcity
                    us-midwest\t82.0\t2016-06-13T17:43:50.100300Z\t
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\t
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\tyork
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\tlondon
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\t
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102500Z\t
                    us-westcost\t82.0\t2016-06-13T17:43:50.102600Z\t
                    """
                    : """
                    location\ttemperature\ttime\tcity
                    us-midwest\t82.0\t2016-06-13T17:43:50.100300000Z\t
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500200Z\t
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400200Z\tyork
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300200Z\tlondon
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400200Z\t
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102500000Z\t
                    us-westcost\t82.0\t2016-06-13T17:43:50.102600000Z\t
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testDifferentCaseForExistingColumnWhenTableExistsAlready() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "tableExistAlready";
        runInContext(() -> {
            execute("create table " + table + " (location SYMBOL, temperature DOUBLE, timestamp TIMESTAMP) timestamp(timestamp);");
            recvBuffer =
                    table + ",location=us-midwest temperature=82,timestamp=1465839830100400t 1465839830100300200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast,city=york,city=london temperature=81,Temperature=89 1465839830101400200\n" +
                            table + ",location=us-midwest,LOCation=Europe,City=london,city=windsor temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast Temperature=89,temperature=88 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82,timestamp=1465839830102500t\n";
            handleIO();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp\tcity
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\t
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\t
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\tyork
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\tlondon
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\t
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\t
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\t
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testDuplicateField() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "dupField";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81,temperature=23 1465839830101400200\n" +
                            table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testDuplicateFieldIPv4() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "dupField";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=\"1.1.1.1\" 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=\"1.1.1.1\" 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=\"1.1.1.1\",temperature=\"2.2.2.2\" 1465839830101400200\n" +
                            table + ",location=us-midwest temperature=\"1.1.1.1\" 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=\"1.1.1.1\" 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=\"1.1.1.1\" 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=\"1.1.1.1\" 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t1.1.1.1\t2016-06-13T17:43:50.100400Z
                    us-midwest\t1.1.1.1\t2016-06-13T17:43:50.100500Z
                    us-eastcoast\t1.1.1.1\t2016-06-13T17:43:50.101400Z
                    us-midwest\t1.1.1.1\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t1.1.1.1\t2016-06-13T17:43:50.102400Z
                    us-eastcoast\t1.1.1.1\t2016-06-13T17:43:50.102400Z
                    us-westcost\t1.1.1.1\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testDuplicateFieldInFirstRow() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "dupField";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82,temperature=77 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81 1465839830101400200\n" +
                            table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testDuplicateFieldInFirstRowCaseInsensitivity() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "dupField";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82,TEMPERATURE=77,TemPerAture=76 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81 1465839830101400200\n" +
                            table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testDuplicateFieldNonASCII() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "dupField";
        runInContext(() -> {
            recvBuffer =
                    table + ",terület=us-midwest hőmérséklet=82,ветер=2.0 1465839830100400200\n" +
                            table + ",terület=us-midwest hőmérséklet=83,ветер=3.0,hőmérséklet=43 1465839830100500200\n" +
                            table + ",terület=us-eastcoast hőmérséklet=81,HŐMÉRSÉKLET=23,ветер=2.0 1465839830101400200\n" +
                            table + ",terület=us-midwest ветер=2.1,hőmérséklet=85,ветер=2.4 1465839830102300200\n" +
                            table + ",terület=us-eastcoast hőmérséklet=89 1465839830102400200\n" +
                            table + ",terület=us-eastcoast hőmérséklet=80 1465839830102400200\n" +
                            table + ",terület=us-westcost hőmérséklet=82,ветер=2.2 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = """
                    terület\thőmérséklet\tветер\ttimestamp
                    us-midwest\t82.0\t2.0\t2016-06-13T17:43:50.100400Z
                    us-midwest\t83.0\t3.0\t2016-06-13T17:43:50.100500Z
                    us-eastcoast\t81.0\t2.0\t2016-06-13T17:43:50.101400Z
                    us-midwest\t85.0\t2.1\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\tnull\t2016-06-13T17:43:50.102400Z
                    us-eastcoast\t80.0\tnull\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2.2\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testDuplicateFieldNonASCIIDifferentCaseFirstRow() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "dupField";
        runInContext(() -> {
            recvBuffer =
                    table + ",terület=us-midwest hőmérséklet=82,HŐmérséklet=84,ветер=2.5,ветер=2.4 1465839830100400200\n" +
                            table + ",terület=us-midwest hőmérséklet=83,ветер=3.0 1465839830100500200\n" +
                            table + ",terület=us-eastcoast hőmérséklet=81,HŐMÉRSÉKLET=23,ветер=2.0 1465839830101400200\n" +
                            table + ",terület=us-midwest ветер=2.1,hőmérséklet=85 1465839830102300200\n" +
                            table + ",terület=us-eastcoast hőmérséklet=89 1465839830102400200\n" +
                            table + ",terület=us-eastcoast hőmérséklet=80 1465839830102400200\n" +
                            table + ",terület=us-westcost hőmérséklet=82,ветер=2.2 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = """
                    terület\thőmérséklet\tветер\ttimestamp
                    us-midwest\t82.0\t2.5\t2016-06-13T17:43:50.100400Z
                    us-midwest\t83.0\t3.0\t2016-06-13T17:43:50.100500Z
                    us-eastcoast\t81.0\t2.0\t2016-06-13T17:43:50.101400Z
                    us-midwest\t85.0\t2.1\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\tnull\t2016-06-13T17:43:50.102400Z
                    us-eastcoast\t80.0\tnull\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2.2\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testDuplicateFieldNonASCIIFirstRow() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "dupField";
        runInContext(() -> {
            recvBuffer =
                    table + ",terület=us-midwest hőmérséklet=82,ветер=2.5,ветер=2.4 1465839830100400200\n" +
                            table + ",terület=us-midwest hőmérséklet=83,ветер=3.0 1465839830100500200\n" +
                            table + ",terület=us-eastcoast hőmérséklet=81,HŐMÉRSÉKLET=23,ветер=2.0 1465839830101400200\n" +
                            table + ",terület=us-midwest ветер=2.1,hőmérséklet=85 1465839830102300200\n" +
                            table + ",terület=us-eastcoast hőmérséklet=89 1465839830102400200\n" +
                            table + ",terület=us-eastcoast hőmérséklet=80 1465839830102400200\n" +
                            table + ",terület=us-westcost hőmérséklet=82,ветер=2.2 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = """
                    terület\thőmérséklet\tветер\ttimestamp
                    us-midwest\t82.0\t2.5\t2016-06-13T17:43:50.100400Z
                    us-midwest\t83.0\t3.0\t2016-06-13T17:43:50.100500Z
                    us-eastcoast\t81.0\t2.0\t2016-06-13T17:43:50.101400Z
                    us-midwest\t85.0\t2.1\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\tnull\t2016-06-13T17:43:50.102400Z
                    us-eastcoast\t80.0\tnull\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2.2\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testDuplicateFieldWhenTableExistsAlready() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "tableExistAlready";
        runInContext(() -> {
            execute("create table " + table + " (location SYMBOL, temperature DOUBLE, timestamp TIMESTAMP) timestamp(timestamp);");
            recvBuffer =
                    table + ",location=us-midwest temperature=82,timestamp=1465839830100100t 1465839830100300200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast,city=york,city=london temperature=81 1465839830101400200\n" +
                            table + ",location=us-midwest,city=london temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82,timestamp=1465839830102500t\n";
            handleIO();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp\tcity
                    us-midwest\t82.0\t2016-06-13T17:43:50.100100Z\t
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\t
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\tyork
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\tlondon
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\t
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\t
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\t
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testDuplicateFieldWhenTableExistsAlreadyNonASCIIFirstRow() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "dupField";
        runInContext(() -> {
            execute("create table " + table + " (terület SYMBOL, hőmérséklet DOUBLE, timestamp TIMESTAMP) timestamp(timestamp);");
            recvBuffer =
                    table + ",terület=us-midwest hőmérséklet=82,ветер=2.5,ВЕтеР=2.4 1465839830100400200\n" +
                            table + ",terület=us-midwest hőmérséklet=83,ветер=3.0 1465839830100500200\n" +
                            table + ",terület=us-eastcoast hőmérséklet=81,HŐMÉRSÉKLET=23,ветер=2.0 1465839830101400200\n" +
                            table + ",terület=us-midwest ветер=2.1,hőmérséklet=85 1465839830102300200\n" +
                            table + ",terület=us-eastcoast hőmérséklet=89 1465839830102400200\n" +
                            table + ",terület=us-eastcoast hőmérséklet=80 1465839830102400200\n" +
                            table + ",terület=us-westcost hőmérséklet=82,ветер=2.2 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = """
                    terület\thőmérséklet\ttimestamp\tветер
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\t2.5
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\t3.0
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\t2.0
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\t2.1
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\tnull
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\tnull
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\t2.2
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testDuplicateNewField() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "dupField";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81,humidity=24,humidity=26,humidity=25 1465839830101400200\n" +
                            table + ",location=us-midwest temperature=85,humidity=27 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp\thumidity
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\tnull
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\tnull
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\t24.0
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\t27.0
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\tnull
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\tnull
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\tnull
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testDuplicateNewFieldAlternating() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "dupField";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81,humidity=24,another=26,humidity=25,another=28,humidity=29 1465839830101400200\n" +
                            table + ",location=us-midwest temperature=85,humidity=27 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89,another=30,humidity=31 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp\thumidity\tanother
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\tnull\tnull
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\tnull\tnull
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\t24.0\t26.0
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\t27.0\tnull
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\t31.0\t30.0
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\tnull\tnull
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\tnull\tnull
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testDuplicateNewFieldCaseInsensitivity() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "dupField";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81,humidity=24,HUMIDITY=26,HuMiditY=25 1465839830101400200\n" +
                            table + ",location=us-midwest temperature=85,humidity=27 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89,HuMiditY=28,humidity=29 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp\thumidity
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\tnull
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\tnull
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\t24.0
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\t27.0
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\t28.0
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\tnull
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\tnull
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testDuplicateTimestamp() throws Exception {
        String table = "duplicateTimestamp";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81,timestamp=1465839830101500t 1465839830101600200\n" +
                            table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101500Z
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testDuplicateTimestampInFirstRow() throws Exception {
        String table = "duplicateTimestamp";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82,timestamp=1465839830100400t,TimeStamp=1465839830100450t 1465839830100700200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81 1465839830101600200\n" +
                            table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101600Z
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testEmptyLine() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        runInContext(() -> {
            recvBuffer = "\n";
            handleContextIO0();
            Assert.assertFalse(disconnected);
        });
    }

    @Test
    public void testExtremeFragmentation() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "extremeFrag";
        runInContext(() -> {
            String allMsgs = makeMessages(table);
            int n = 0;
            while (n < allMsgs.length()) {
                recvBuffer = allMsgs.substring(n, n + 1);
                n++;
                handleContextIO0();
                Assert.assertFalse(disconnected);
            }
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testFailure() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        final AtomicInteger nCommittedLines = new AtomicInteger(4);
        String table = "failure1";
        UnstableRunnable onCommitNewEvent = () -> {
            if (nCommittedLines.decrementAndGet() <= 0) {
                throw new RuntimeException("Failed");
            }
        };
        runInContext(
                () -> {
                    recvBuffer =
                            table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                                    table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                                    table + ",location=us-eastcoast temperature=81 1465839830101400200\n" +
                                    table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                                    table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                                    table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                                    table + ",location=us-westcost temperature=82 1465839830102500200\n";
                    handleContextIO0();
                    Assert.assertTrue(disconnected);
                    closeContext();
                    String expected = """
                            location\ttemperature\ttimestamp
                            us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                            us-midwest\t83.0\t2016-06-13T17:43:50.100500Z
                            us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z
                            """;
                    assertTable(expected, table);
                },
                onCommitNewEvent
        );
    }

    @Test
    public void testFragmentation12() throws Exception {
        testFragmentation("weather4,location=us-midwest temperature=".length(), "weather4");
    }

    @Test
    public void testFragmentation13() throws Exception {
        testFragmentation("weather5,location=us-midwest temperature=8".length(), "weather5");
    }

    @Test
    public void testFragmentation14() throws Exception {
        testFragmentation("weather6,location=us-midwest temperature=82".length(), "weather6");
    }

    @Test
    public void testFragmentation15() throws Exception {
        testFragmentation("weather7,location=us-midwest temperature=82 ".length(), "weather7");
    }

    @Test
    public void testFragmentation16() throws Exception {
        testFragmentation("weather8,location=us-midwest temperature=82 1465839830100400".length(), "weather8");
    }

    @Test
    public void testFragmentation17() throws Exception {
        testFragmentation("weather9,location=us-midwest temperature=82 1465839830100400200".length(), "weather9");
    }

    @Test
    public void testFragmentation2() throws Exception {
        testFragmentation("weather10".length(), "weather10");
    }

    @Test
    public void testFragmentation3() throws Exception {
        testFragmentation("weather11,".length(), "weather11");
    }

    @Test
    public void testFragmentation4() throws Exception {
        testFragmentation("weather12,locat".length(), "weather12");
    }

    @Test
    public void testFragmentation5() throws Exception {
        testFragmentation("weather13,location".length(), "weather13");
    }

    @Test
    public void testFragmentation6() throws Exception {
        testFragmentation("weather14,location=".length(), "weather14");
    }

    @Test
    public void testFragmentation7() throws Exception {
        testFragmentation("weather15,location=us-midw".length(), "weather15");
    }

    @Test
    public void testFragmentation8() throws Exception {
        testFragmentation("weather16,location=us-midwest".length(), "weather16");
    }

    @Test
    public void testFragmentation9() throws Exception {
        testFragmentation("weather17,location=us-midwest ".length(), "weather17");
    }

    @Test
    public void testFragmentationAtFieldNameEnd() throws Exception {
        testFragmentation("weather3,location=us-midwest temperature".length(), "weather3");
    }

    @Test
    public void testFragmentationMidFieldName() throws Exception {
        testFragmentation("weather2,location=us-midwest tempera".length(), "weather2");
    }

    @Test
    public void testFragmentationMidTableName() throws Exception {
        testFragmentation("weat".length(), "weather1");
    }

    @Test
    public void testInsertIntoExistingVarcharColumn() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "tableExistAlready";
        runInContext(() -> {
            execute("create table " + table + " (location SYMBOL, slog VARCHAR, timestamp TIMESTAMP) timestamp(timestamp);");
            recvBuffer =
                    table + ",location=us-midwest slog=\"82\",timestamp=1465839830100100t 1465839830100300200\n" +
                            table + ",location=us-midwest slog=\"hello\" 1465839830100500200\n" +
                            table + ",location=us-eastcoast,city=york,city=london slog=\"baba yaga\" 1465839830101400200\n" +
                            table + ",location=us-midwest,city=london slog=\"mexico\" 1465839830102300200\n" +
                            table + ",location=us-eastcoast slog=\"pub crawl\" 1465839830102400200\n" +
                            table + ",location=us-eastcoast slog=\"dont fix what's not broken\" 1465839830102400200\n" +
                            table + ",location=us-westcost slog=\"are we there yet?\",timestamp=1465839830102500t\n";
            handleIO();
            closeContext();
            String expected = """
                    location\tslog\ttimestamp\tcity
                    us-midwest\t82\t2016-06-13T17:43:50.100100Z\t
                    us-midwest\thello\t2016-06-13T17:43:50.100500Z\t
                    us-eastcoast\tbaba yaga\t2016-06-13T17:43:50.101400Z\tyork
                    us-midwest\tmexico\t2016-06-13T17:43:50.102300Z\tlondon
                    us-eastcoast\tpub crawl\t2016-06-13T17:43:50.102400Z\t
                    us-eastcoast\tdont fix what's not broken\t2016-06-13T17:43:50.102400Z\t
                    us-westcost\tare we there yet?\t2016-06-13T17:43:50.102500Z\t
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testInvalidTableName() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "testInvalidEmptyTableName";
        Files.touch(Path.getThreadLocal(configuration.getDbRoot()).concat(TableUtils.TXN_FILE_NAME).$());
        Files.touch(Path.getThreadLocal(configuration.getDbRoot()).concat(TableUtils.META_FILE_NAME).$());
        Files.touch(Path.getThreadLocal(configuration.getDbRoot()).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$());

        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82,timestamp=1465839830100200t\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81 1465839830101600200\n" +
                            table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n";
            do {
                handleContextIO0();
            } while (!recvBuffer.isEmpty());
        });

        engine.releaseInactive();

        runInContext(() -> {
            recvBuffer = ",location=us-midwest temperature=82,timestamp=1465839830100200t\n";
            do {
                handleContextIO0();
            } while (!recvBuffer.isEmpty());

            recvBuffer = ".,location=us-midwest temperature=82,timestamp=1465839830100200t\n";
            do {
                handleContextIO0();
            } while (!recvBuffer.isEmpty());

            recvBuffer = "..\\/dbRoot,location=us-midwest temperature=82,timestamp=1465839830100200t\n";
            do {
                handleContextIO0();
            } while (!recvBuffer.isEmpty());

            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100200Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101600Z
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testMaxMeasurementSize() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        Assume.assumeFalse(walEnabled);
        String table = "maxSize";
        runInContext(() -> {
            String longStr = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
            longStr += longStr;
            String longMeasurement = table + ",location=us-eastcoast" + longStr + " temperature=81 1465839830101400200\n";
            Assert.assertTrue(longMeasurement.length() > lineTcpConfiguration.getMaxMeasurementSize());
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            longMeasurement +
                            table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testMoreDuplicateNewFields() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "dupField";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81,humidity=24,humidity=26,humidity=25,pollution=2,pollution=3 1465839830101400200\n" +
                            table + ",location=us-midwest temperature=85,humidity=27,pollution=3,pollution=4 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89,pollution=5 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp\thumidity\tpollution
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\tnull\tnull
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\tnull\tnull
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\t24.0\t2.0
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\t27.0\t3.0
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\tnull\t5.0
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\tnull\tnull
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\tnull\tnull
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testMultipleMeasurements1() throws Exception {
        String table = "multipleMeasurements1";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81 1465839830101400200\n" +
                            table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testMultipleMeasurements2() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "multipleMeasurements1";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81 1465839830101400200\n";
            handleContextIO0();
            Assert.assertFalse(disconnected);
            recvBuffer =
                    table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleContextIO0();
            Assert.assertFalse(disconnected);
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testMultipleMeasurements3() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "multipleMeasurements3";
        runInContext(() -> {
            recvBuffer =
                    table + " temperature=82,pressure=100i 1465839830100400200\n" +
                            table + " temperature=83,pressure=100i 1465839830100500200\n" +
                            table + " temperature=81,pressure=102i 1465839830101400200\n" +
                            table + " temperature=85,pressure=103i 1465839830102300200\n" +
                            table + " temperature=89,pressure=101i 1465839830102400200\n" +
                            table + " temperature=80,pressure=100i 1465839830102400200\n" +
                            table + " temperature=82,pressure=100i 1465839830102500200\n";
            handleContextIO0();
            Assert.assertFalse(disconnected);
            closeContext();
            String expected = """
                    temperature\tpressure\ttimestamp
                    82.0\t100\t2016-06-13T17:43:50.100400Z
                    83.0\t100\t2016-06-13T17:43:50.100500Z
                    81.0\t102\t2016-06-13T17:43:50.101400Z
                    85.0\t103\t2016-06-13T17:43:50.102300Z
                    89.0\t101\t2016-06-13T17:43:50.102400Z
                    80.0\t100\t2016-06-13T17:43:50.102400Z
                    82.0\t100\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testMultipleMeasurements4() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "multipleMeasurements4";
        runInContext(() -> {
            recvBuffer =
                    table + " temperature=82,pressure=100i 1465839830100400200\n" +
                            table + " temperature=83,pressure=100i 1465839830100500200\n" +
                            table + " temperature=81,pressure=102i 1465839830101400200\n" +
                            table + " temperature=85,pressure=103i 1465839830102300200\n" +
                            table + " temperature=89,pressure=101i 1465839830102400200\n" +
                            table + " temperature=80,pressure=100i 1465839830102400200\n" +
                            table + " temperature=82,pressure=100i 1465839830102500200\n";
            handleContextIO0();
            Assert.assertFalse(disconnected);
            closeContext();
            String expected = """
                    temperature\tpressure\ttimestamp
                    82.0\t100\t2016-06-13T17:43:50.100400Z
                    83.0\t100\t2016-06-13T17:43:50.100500Z
                    81.0\t102\t2016-06-13T17:43:50.101400Z
                    85.0\t103\t2016-06-13T17:43:50.102300Z
                    89.0\t101\t2016-06-13T17:43:50.102400Z
                    80.0\t100\t2016-06-13T17:43:50.102400Z
                    82.0\t100\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testMultipleTablesWithMultipleWriterThreads() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        maxRecvBufferSize.set(4096);
        nWriterThreads = 3;
        int nTables = 5;
        int nIterations = 10_000;
        testThreading(nTables, nIterations);
    }

    @Test
    public void testMultipleTablesWithSingleWriterThread() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        maxRecvBufferSize.set(4096);
        nWriterThreads = 1;
        int nTables = 3;
        int nIterations = 10_000;
        testThreading(nTables, nIterations);
    }

    @Test
    public void testNewColumnsNotAllowed() throws Exception {
        String table = "testNewColumnsNotAllowed";
        autoCreateNewColumns = false;
        disconnectOnError = true;
        runInContext(() -> {
            recvBuffer = table + ",location=us-midwest temperature=82,timestamp=1465839830100200200t\n" +
                    table + ",location=us-eastcoast temperature=80 1465839830102400200\n";
            do {
                handleContextIO0();
            } while (!recvBuffer.isEmpty());

            Assert.assertTrue(disconnected);
        });

        try {
            engine.verifyTableName(table);
        } catch (CairoException ex) {
            TestUtils.assertContains(ex.getFlyweightMessage(), "table does not exist");
        }
    }

    @Test
    public void testNewColumnsNotAllowedExistingTable() throws Exception {
        String table = "testNewColumnsNotAllowed";
        autoCreateNewColumns = false;
        disconnectOnError = true;
        execute("create table " + table + " (location SYMBOL, timestamp TIMESTAMP) timestamp(timestamp);");

        engine.releaseInactive();
        runInContext(() -> {
            recvBuffer = table + ",location=us-midwest temperature=82,timestamp=1465839830100200200t\n" +
                    table + ",location=us-eastcoast temperature=80 1465839830102400200\n";
            do {
                handleContextIO0();
            } while (!recvBuffer.isEmpty());

            Assert.assertTrue(disconnected);
        });
        assertTable("location\ttimestamp\n", table);
    }

    @Test
    public void testNewTableNotAllowed() throws Exception {
        String table = "testNewTableNotAllowed";
        autoCreateNewTables = false;
        disconnectOnError = true;
        runInContext(() -> {
            recvBuffer = table + ",location=us-midwest temperature=82,timestamp=1465839830100200200t\n" +
                    table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                    table + ",location=us-eastcoast temperature=80 1465839830102400200\n";
            do {
                handleContextIO0();
            } while (!recvBuffer.isEmpty());

            Assert.assertTrue(disconnected);
        });

        try {
            engine.verifyTableName(table);
        } catch (CairoException ex) {
            TestUtils.assertContains(ex.getFlyweightMessage(), "table does not exist");
        }
    }

    @Test
    public void testNewTableNullType() throws Exception {
        runInContext(() -> {
            recvBuffer =
                    "vbw water_speed_longitudinal=0.07,water_speed_traversal=,water_speed_status=\"A\",ground_speed_longitudinal=0,ground_speed_traversal=0,ground_speed_status=\"A\",water_speed_stern_traversal=,water_speed_stern_traversal_status=\"V\",ground_speed_stern_traversal=0,ground_speed_stern_traversal_status=\"V\" 1627046637414969856\n";
            handleIO();
            closeContext();

            assertSql(
                    "id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\n",
                    "select id,table_name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables()"
            );
            execute("create table vbw(a int)");
        });
    }

    @Test
    public void testNoTimestamp() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "notimestamp";
        timestampTicks = Micros.DAY_MICROS;
        runInContext(() -> {
            recvBuffer =
                    table + ",platform=APP val=1\n" +
                            table + ",platform=APP val=2 \n" +
                            table + " val=3\n" +
                            table + " val=4 \n" +
                            table + ",platform=APP2 \n" +
                            table + ",platform=APP3\n";
            handleIO();
            closeContext();
            String expected = """
                    platform\tval\ttimestamp
                    APP\t1.0\t1970-01-02T00:00:00.000000Z
                    APP\t2.0\t1970-01-02T00:00:00.000000Z
                    \t3.0\t1970-01-02T00:00:00.000000Z
                    \t4.0\t1970-01-02T00:00:00.000000Z
                    APP2\tnull\t1970-01-02T00:00:00.000000Z
                    APP3\tnull\t1970-01-02T00:00:00.000000Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testNonAsciiTableNameWithUtf16SurrogateChar() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "\uD834\uDD1E g-clef";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81 1465839830101400200\n" +
                            table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleIO();
            closeContext();
            try (Path path = new Path().of(configuration.getDbRoot()).concat(table)) {
                Assert.assertFalse(configuration.getFilesFacade().exists(path.$()));
            }
        });
    }

    @Test
    public void testNonPrintableChars() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        char nonPrintable = 0x3000;
        char nonPrintable1 = 0x3080;
        char nonPrintable2 = 0x3a55;
        String table = "nonPrintable" + nonPrintable + "Chars";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-mid" + nonPrintable1 + "west temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast" + nonPrintable2 + " temperature=81 1465839830101400200\n" +
                            table + ",location=us-midwest temperature=85,hőmérséklet" + nonPrintable1 + "=24 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89,hőmérséklet" + nonPrintable2 + "=26 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80,hőmérséklet" + nonPrintable + "=25,hőmérséklet" + nonPrintable2 + "=23 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = "location\ttemperature\ttimestamp\thőmérséklet" + nonPrintable1 + "\thőmérséklet" + nonPrintable2 + "\thőmérséklet" + nonPrintable + "\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\tnull\tnull\tnull\n" +
                    "us-mid" + nonPrintable1 + "west\t83.0\t2016-06-13T17:43:50.100500Z\tnull\tnull\tnull\n" +
                    "us-eastcoast" + nonPrintable2 + "\t81.0\t2016-06-13T17:43:50.101400Z\tnull\tnull\tnull\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\t24.0\tnull\tnull\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\tnull\t26.0\tnull\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\tnull\t23.0\t25.0\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\tnull\tnull\tnull\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testOverflow() throws Exception {
        runInContext(() -> {
            long maxBufferSize = lineTcpConfiguration.getMaxRecvBufferSize();
            recvBuffer = "MAXBUFSIZ";
            while (recvBuffer.length() <= maxBufferSize) {
                recvBuffer += recvBuffer;
            }
            long nUnread = recvBuffer.length() - maxBufferSize;
            do {
                handleContextIO0();
                Assert.assertFalse(recvBuffer.isEmpty());
            } while (!disconnected);
            Assert.assertEquals(nUnread, recvBuffer.length());
        });
    }

    @Test
    public void testQuotes() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        runInContext(() -> {
            recvBuffer = """
                    tbl,t1=tv1,t2=tv2 f1="fv1",f2="Zen Internet Ltd" 1465839830100400200
                    tbl,t1=tv1,t2=tv2 f1="Zen Internet Ltd" 1465839830100400200
                    tbl,t1=tv1,t2=tv2 f1="Zen=Internet,Ltd" 1465839830100400200
                    tbl,t1=t\\"v1,t2=t"v2 f2="1" 1465839830100400200
                    tbl,t1="tv1",t2=tv2 f2="1" 1465839830100400200
                    tbl,t1=tv1",t2=tv2 f2="1" 1465839830100400200
                    tbl,t1="tv1,t2=tv2 f2="1" 1465839830100400200
                    tbl,t1=tv1,t2=tv2 f1="Zen Internet Ltd",f2="fv2" 1465839830100400200
                    """;
            handleIO();
            closeContext();
            String expected = """
                    t1\tt2\tf1\tf2\ttimestamp
                    tv1\ttv2\tfv1\tZen Internet Ltd\t2016-06-13T17:43:50.100400Z
                    tv1\ttv2\tZen Internet Ltd\t\t2016-06-13T17:43:50.100400Z
                    tv1\ttv2\tZen=Internet,Ltd\t\t2016-06-13T17:43:50.100400Z
                    t"v1\tt"v2\t\t1\t2016-06-13T17:43:50.100400Z
                    "tv1"\ttv2\t\t1\t2016-06-13T17:43:50.100400Z
                    tv1"\ttv2\t\t1\t2016-06-13T17:43:50.100400Z
                    "tv1\ttv2\t\t1\t2016-06-13T17:43:50.100400Z
                    tv1\ttv2\tZen Internet Ltd\tfv2\t2016-06-13T17:43:50.100400Z
                    """;
            assertTable(expected, "tbl");
        });
    }

    @Test
    public void testSingleMeasurement() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "singleMeasurement";
        runInContext(() -> {
            recvBuffer = table + ",location=us-midwest temperature=82 1465839830100400200\n";
            handleContextIO0();
            Assert.assertFalse(disconnected);
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testStrings() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "strings";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-eastcoast raining=\"true\" 1465839830100400200\n" +
                            table + ",location=us-midwest raining=\"false\" 1465839830100400200\n" +
                            table + ",location=us-midwest raining=\"f\" 1465839830100500200\n" +
                            table + ",location=us-midwest raining=\"t\" 1465839830102300200\n" +
                            table + ",location=us-eastcoast raining=\"T\" 1465839830102400200\n" +
                            table + ",location=us-eastcoast raining=\"F\" 1465839830102400200\n" +
                            table + ",location=us-westcost raining=\"False\" 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = """
                    location\training\ttimestamp
                    us-eastcoast\ttrue\t2016-06-13T17:43:50.100400Z
                    us-midwest\tfalse\t2016-06-13T17:43:50.100400Z
                    us-midwest\tf\t2016-06-13T17:43:50.100500Z
                    us-midwest\tt\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\tT\t2016-06-13T17:43:50.102400Z
                    us-eastcoast\tF\t2016-06-13T17:43:50.102400Z
                    us-westcost\tFalse\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testSymbolFileMapping() throws Exception {
        String table = "symbolMapping";
        runInContext(() -> {
            final StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 2039; i++) {
                sb.append(table).append(",location=").append(i).append(" raining=\"true\" 1465839830100400200\n");
            }
            recvBuffer = sb.toString();

            // ingesting 2038 rows -> size of location.o file will be 16384 bytes (pageSize)
            handleIO();
            closeContext();

            // with this line we are testing that mmap size is calculated correctly even in case of fileSize=pageSize
            newOffPoolReader(configuration, table, engine).close();
        });
    }

    @Test
    public void testSymbolOrder1() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "symbolOrder";
        addTable(table);
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest,sensor=type3 temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest,sensor=type1 temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast,sensor=type6 temperature=81 1465839830101400200\n" +
                            table + ",location=us-midwest,sensor=type1 temperature=85 1465839830102400200\n" +
                            table + ",location=us-eastcoast,sensor=type1 temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast,sensor=type3 temperature=80 1465839830102400200\n" +
                            table + ",sensor=type1,location=us-midwest temperature=85 1465839830102401200\n" +
                            table + ",location=us-eastcoast,sensor=type1 temperature=89 1465839830102402200\n" +
                            table + ",sensor=type3,location=us-eastcoast temperature=80 1465839830102403200\n" +
                            table + ",location=us-westcost,sensor=type1 temperature=82 1465839830102504200\n";
            handleIO();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp\tsensor
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\ttype3
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\ttype1
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\ttype6
                    us-midwest\t85.0\t2016-06-13T17:43:50.102400Z\ttype1
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\ttype1
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\ttype3
                    us-midwest\t85.0\t2016-06-13T17:43:50.102401Z\ttype1
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102402Z\ttype1
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102403Z\ttype3
                    us-westcost\t82.0\t2016-06-13T17:43:50.102504Z\ttype1
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    public void testTableParameterRetentionOnAddColumn() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String table = "retention";
        runInContext(() -> {
            execute("create table " + table + " (location SYMBOL, temperature DOUBLE, timestamp TIMESTAMP) timestamp(timestamp) partition by DAY WITH maxUncommittedRows=3, o3MaxLag=250ms;");
            try (TableReader reader = getReader(table)) {
                Assert.assertEquals(3, reader.getMetadata().getMaxUncommittedRows());
                Assert.assertEquals(250_000, reader.getMetadata().getO3MaxLag());
            }
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast,city=york temperature=81 1465839830101400200\n" +
                            table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleIO();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp\tcity
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\t
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\t
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\tyork
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\t
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\t
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\t
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\t
                    """;
            assertTable(expected, table);

            try (TableReader reader = getReader(table)) {
                Assert.assertEquals(3, reader.getMetadata().getMaxUncommittedRows());
                Assert.assertEquals(250_000, reader.getMetadata().getO3MaxLag());
            }
        });
    }

    @Test
    public void testUseReceivedTimestamp1() throws Exception {
        String table = "testAutoTimestamp";
        runInContext(() -> {
            timestampTicks = 0;
            recvBuffer =
                    table + ",location=us-midwest temperature=82\n" +
                            table + ",location=us-midwest temperature=83\n" +
                            table + ",location=us-eastcoast temperature=81\n" +
                            table + ",location=us-midwest temperature=85\n" +
                            table + ",location=us-eastcoast temperature=89\n" +
                            table + ",location=us-eastcoast temperature=80\n" +
                            table + ",location=us-westcost temperature=82\n";
            handleContextIO0();
            Assert.assertFalse(disconnected);
            closeContext();
            String expected = timestampType.getTimestampType() == ColumnType.TIMESTAMP_NANO
                    ? """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t1970-01-01T00:00:00.000000000Z
                    us-midwest\t83.0\t1970-01-01T00:00:00.000000000Z
                    us-eastcoast\t81.0\t1970-01-01T00:00:00.000000000Z
                    us-midwest\t85.0\t1970-01-01T00:00:00.000000000Z
                    us-eastcoast\t89.0\t1970-01-01T00:00:00.000000000Z
                    us-eastcoast\t80.0\t1970-01-01T00:00:00.000000000Z
                    us-westcost\t82.0\t1970-01-01T00:00:00.000000000Z
                    """
                    : """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t1970-01-01T00:00:00.000000Z
                    us-midwest\t83.0\t1970-01-01T00:00:00.000000Z
                    us-eastcoast\t81.0\t1970-01-01T00:00:00.000000Z
                    us-midwest\t85.0\t1970-01-01T00:00:00.000000Z
                    us-eastcoast\t89.0\t1970-01-01T00:00:00.000000Z
                    us-eastcoast\t80.0\t1970-01-01T00:00:00.000000Z
                    us-westcost\t82.0\t1970-01-01T00:00:00.000000Z
                    """;
            assertTable(expected, table);
        });
    }

    private void addTable(String table) {
        TableModel model = new TableModel(configuration, table, walEnabled ? PartitionBy.DAY : PartitionBy.NONE)
                .col("location", ColumnType.SYMBOL)
                .col("temperature", ColumnType.DOUBLE);

        if (ColumnType.isTimestampMicro(timestampType.getTimestampType())) {
            model = model.timestamp();
        } else {
            model = model.timestampNs();
        }
        if (walEnabled) {
            model.wal();
        }
        AbstractCairoTest.create(model);
        engine.releaseInactive();
    }

    private void assertTableCount(CharSequence tableName, int nExpectedRows, long maxExpectedTimestampNanos) {
        try (
                TableReader reader = newOffPoolReader(configuration, tableName);
                TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
        ) {
            Assert.assertEquals(maxExpectedTimestampNanos / 1000, reader.getMaxTimestamp());
            int timestampColIndex = reader.getMetadata().getTimestampIndex();
            int nRows = 0;
            long timestampNanos = 1465839830100400200L;
            while (cursor.hasNext()) {
                long actualTimestampInMicros = cursor.getRecord().getTimestamp(timestampColIndex);
                Assert.assertEquals(timestampNanos / 1000, actualTimestampInMicros);
                timestampNanos += 1000;
                nRows++;
            }
            Assert.assertEquals(nExpectedRows, nRows);
        }
    }

    private void handleIO() {
        do {
            handleContextIO0();
            Assert.assertFalse(disconnected);
        } while (!recvBuffer.isEmpty());
    }

    @NotNull
    private String makeMessages(String table) {
        return table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                table + ",location=us-eastcoast temperature=81 1465839830101400200\n" +
                table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                table + ",location=us-westcost temperature=82 1465839830102500200\n";
    }

    private void testDefaultColumnType(short expectedType, String ilpValue, String tableValue, String emptyValue) throws Exception {
        String table = "addDefColType";
        addTable(table);

        runInContext(() -> {
            recvBuffer = table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                    table + ",location=us-eastcoast temperature=81,newcol=" + ilpValue + " 1465839830101400200\n";
            handleIO();
            closeContext();
            if (walEnabled) {
                drainWalQueue();
            }
            String expected = ColumnType.isTimestampMicro(timestampType.getTimestampType())
                    ? "location\ttemperature\ttimestamp\tnewcol\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\t" + emptyValue + "\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\t" + tableValue + "\n"
                    : "location\ttemperature\ttimestamp\tnewcol\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400200Z\t" + emptyValue + "\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400200Z\t" + tableValue + "\n";
            try (
                    TableReader reader = newOffPoolReader(configuration, table);
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
            ) {
                assertCursorTwoPass(expected, cursor, reader.getMetadata());
                Assert.assertEquals(expectedType, ColumnType.tagOf(reader.getMetadata().getColumnType("newcol")));
            }
        });
    }

    private void testFragmentation(int breakPos, String table) throws Exception {
        runInContext(() -> {
            String allMsgs = makeMessages(table);
            recvBuffer = allMsgs.substring(0, breakPos);
            handleContextIO0();
            Assert.assertFalse(disconnected);
            recvBuffer = allMsgs.substring(breakPos);
            handleContextIO0();
            Assert.assertFalse(disconnected);
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, table);
        });
    }

    private void testThreading(int nTables, int nIterations) throws Exception {
        double[] lf = new double[nTables];
        Arrays.fill(lf, 1d);
        for (int n = 1; n < nTables; n++) {
            lf[n] += lf[n - 1];
        }
        final double[] loadFactors = lf;
        final double accLoadFactors = loadFactors[nTables - 1];
        Rnd rnd = new Rnd();
        int[] countByTable = new int[nTables];
        long[] maxTimestampByTable = new long[nTables];
        final long initialTimestampNanos = 1465839830100400200L;
        final long timestampIncrementInNanos = 1000;
        Arrays.fill(maxTimestampByTable, initialTimestampNanos);
        runInContext(() -> {
            int nTablesSelected = 0;
            int nTotalUpdates = 0;
            for (int nIter = 0; nIter < nIterations; nIter++) {
                int nLines = rnd.nextInt(50) + 1;
                sink.clear();
                for (int nLine = 0; nLine < nLines; nLine++) {
                    int nTable;
                    if (nTablesSelected < nTables) {
                        nTable = nTablesSelected++;
                    } else {
                        double tableSelector = rnd.nextDouble() * accLoadFactors;
                        nTable = nTables;
                        while (--nTable > 0) {
                            if (tableSelector > loadFactors[nTable - 1]) {
                                break;
                            }
                        }
                    }
                    long timestamp = maxTimestampByTable[nTable];
                    maxTimestampByTable[nTable] += timestampIncrementInNanos;
                    double temperature = 50.0 + (rnd.nextInt(500) / 10.0);
                    sink.put("weather").put(nTable)
                            .put(",location=us-midwest temperature=").put(temperature)
                            .put(' ').put(timestamp).put('\n');
                    countByTable[nTable]++;
                    nTotalUpdates++;
                }
                recvBuffer = sink.toString();
                do {
                    if (handleContextIO0()) {
                        Os.pause();
                    }
                } while (!recvBuffer.isEmpty());
            }
            waitForIOCompletion();
            closeContext();
            drainWalQueue();
            LOG.info().$("Completed ")
                    .$(nTotalUpdates)
                    .$(" measurements with ")
                    .$(nTables)
                    .$(" measurement types processed by ")
                    .$(nWriterThreads)
                    .$(" threads. ")
                    .$();
            for (int nTable = 0; nTable < nTables; nTable++) {
                assertTableCount("weather" + nTable, countByTable[nTable], maxTimestampByTable[nTable] - timestampIncrementInNanos);
            }
        });
    }

    @Override
    protected void assertTable(CharSequence expected, String tableName) {
        drainWalQueue();
        super.assertTable(expected, tableName);
    }
}

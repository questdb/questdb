/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.LPSZ;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class LineTcpConnectionContextTest extends BaseLineTcpContextTest {

    @Test
    public void testAddCastFieldColumnNoTable() throws Exception {
        String tableName = "addCastColumn";
        symbolAsFieldSupported = true;
        runInContext(() -> {
            recvBuffer = tableName + ",location=us-midwest temperature=82 1465839830100400200\n" +
                         tableName + ",location=us-eastcoast cast=cast,temperature=81,humidity=23 1465839830101400200\n";
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\tcast\thumidity\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\t\tNaN\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\tcast\t23.0\n";
            try (TableReader reader = new TableReader(configuration, tableName)) {
                TableReaderMetadata meta = reader.getMetadata();
                assertCursorTwoPass(expected, reader.getCursor(), meta);
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
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\thumidity\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\tNaN\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\tNaN\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\t23.0\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\tNaN\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\tNaN\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\tNaN\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\tNaN\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testAddTagColumn() throws Exception {
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
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\tcity\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\t\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\t\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\tyork\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\t\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\t\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\t\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\t\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testAddToExistingTable() throws Exception {
        String table = "addToExisting";
        addTable(table);
        runInContext(() -> {
            recvBuffer = makeMessages(table);
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
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
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testBadLineSyntax1() throws Exception {
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
            handleContextIO();
            Assert.assertFalse(disconnected);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testBadLineSyntax2() throws Exception {
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
            handleContextIO();
            Assert.assertFalse(disconnected);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testBadLineSyntax3() throws Exception {
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
            handleContextIO();
            Assert.assertFalse(disconnected);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testBadLineSyntax4() throws Exception {
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
            handleContextIO();
            Assert.assertFalse(disconnected);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testBadLineSyntax5() throws Exception {
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
            handleContextIO();
            Assert.assertFalse(disconnected);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testBadLineSyntax6() throws Exception {
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
            handleContextIO();
            Assert.assertFalse(disconnected);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
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
            handleContextIO();
            Assert.assertFalse(disconnected);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testBooleans() throws Exception {
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
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\training\ttimestamp\n" +
                    "us-eastcoast\ttrue\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\tfalse\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\tfalse\t2016-06-13T17:43:50.100500Z\n" +
                    "us-midwest\ttrue\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\ttrue\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\tfalse\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\tfalse\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testCairoExceptionOnAddColumn() throws Exception {
        String table = "columnEx";
        runInContext(
                new FilesFacadeImpl() {
                    @Override
                    public long openRW(LPSZ name) {
                        if (Chars.endsWith(name, "broken.d.1")) {
                            return -1;
                        }
                        return super.openRW(name);
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
                    do {
                        handleContextIO();
                        Assert.assertFalse(disconnected);
                    } while (recvBuffer.length() > 0);
                    closeContext();
                    String expected = "location\ttemperature\ttimestamp\n" +
                            "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                            "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                            "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                            "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                            "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\n" +
                            "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
                    assertTable(expected, table);
                }, null, null);
    }

    @Test
    public void testCairoExceptionOnCreateTable() throws Exception {
        String table = "cairoEx";
        runInContext(
                new FilesFacadeImpl() {
                    @Override
                    public long openRW(LPSZ name) {
                        if (Chars.endsWith(name, "broken.d")) {
                            return -1;
                        }
                        return super.openRW(name);
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
                    do {
                        handleContextIO();
                        Assert.assertFalse(disconnected);
                    } while (recvBuffer.length() > 0);
                    closeContext();
                    String expected = "location\ttemperature\tbroken\ttimestamp\n";
                    assertTable(expected, table);
                }, null, null);
    }

    @Test
    public void testColumnConversion1() throws Exception {
        runInContext(() -> {
            try (
                    @SuppressWarnings("resource")
                    TableModel model = new TableModel(configuration, "t_ilp21",
                            PartitionBy.NONE).col("event", ColumnType.SHORT).col("id", ColumnType.LONG256).col("ts", ColumnType.TIMESTAMP).col("float1", ColumnType.FLOAT).col("int1", ColumnType.INT)
                            .col("date1", ColumnType.DATE).col("byte1", ColumnType.BYTE).timestamp()) {
                CairoTestUtils.create(model);
            }
            microSecondTicks = 1465839830102800L;
            recvBuffer = "t_ilp21 event=12i,id=0x05a9796963abad00001e5f6bbdb38i,ts=1465839830102400i,float1=1.2,int1=23i,date1=1465839830102i,byte1=-7i\n" +
                    "t_ilp21 event=12i,id=0x5a9796963abad00001e5f6bbdb38i,ts=1465839830102400i,float1=1e3,int1=-500000i,date1=1465839830102i,byte1=3i\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
            closeContext();
            String expected = "event\tid\tts\tfloat1\tint1\tdate1\tbyte1\ttimestamp\n" +
                    "12\t0x5a9796963abad00001e5f6bbdb38\t2016-06-13T17:43:50.102400Z\t1.2000\t23\t2016-06-13T17:43:50.102Z\t-7\t2016-06-13T17:43:50.102800Z\n" +
                    "12\t0x5a9796963abad00001e5f6bbdb38\t2016-06-13T17:43:50.102400Z\t1000.0000\t-500000\t2016-06-13T17:43:50.102Z\t3\t2016-06-13T17:43:50.102800Z\n";
            assertTable(expected, "t_ilp21");
        });
    }

    @Test
    public void testColumnConversion2() throws Exception {
        runInContext(() -> {
            try (
                    @SuppressWarnings("resource")
                    TableModel model = new TableModel(configuration, "t_ilp21",
                            PartitionBy.NONE).col("l", ColumnType.LONG)) {
                CairoTestUtils.create(model);
            }
            microSecondTicks = 1465839830102800L;
            recvBuffer = "t_ilp21 l=843530699759026177i\n" +
                    "t_ilp21 l=\"843530699759026178\"\n" +
                    "t_ilp21 l=843530699759026179i\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
            closeContext();
            String expected = "l\n" +
                    "843530699759026177\n" +
                    "843530699759026179\n";
            assertTable(expected, "t_ilp21");
        });
    }

    @Test
    public void testColumnNameWithSlash1() throws Exception {
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
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testColumnNameWithSlash2() throws Exception {
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
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testColumnTypeChange() throws Exception {
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
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
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
                            table + ",location=us-eastcoast temperature=81,timestamp=1465839830101600200t\n" +
                            table + ",location=us-midwest temperature=85,timestamp=1465839830102300200t,Timestamp=1465839830102800200t\n" +
                            table + ",location=us-eastcoast temperature=89,Timestamp=1465839830102400200t\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101600Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testDesignatedTimestampAsFieldInAllRows() throws Exception {
        String table = "duplicateTimestamp";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest timestamp=1465839830100400200t,temperature=82\n" +
                            table + ",location=us-midwest timestamp=1465839830100500200t,temperature=83\n" +
                            table + ",location=us-eastcoast timestamp=1465839830101600200t,temperature=81\n" +
                            table + ",location=us-midwest timestamp=1465839830102300200t,temperature=85\n" +
                            table + ",location=us-eastcoast timestamp=1465839830102400200t,temperature=89\n" +
                            table + ",location=us-eastcoast timestamp=1465839830102400200t,temperature=80\n" +
                            table + ",location=us-westcost timestamp=1465839830102500200t,temperature=82\n";
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\ttimestamp\ttemperature\n" +
                    "us-midwest\t2016-06-13T17:43:50.100400Z\t82.0\n" +
                    "us-midwest\t2016-06-13T17:43:50.100500Z\t83.0\n" +
                    "us-eastcoast\t2016-06-13T17:43:50.101600Z\t81.0\n" +
                    "us-midwest\t2016-06-13T17:43:50.102300Z\t85.0\n" +
                    "us-eastcoast\t2016-06-13T17:43:50.102400Z\t89.0\n" +
                    "us-eastcoast\t2016-06-13T17:43:50.102400Z\t80.0\n" +
                    "us-westcost\t2016-06-13T17:43:50.102500Z\t82.0\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testDesignatedTimestampAsFieldInFirstRow() throws Exception {
        String table = "duplicateTimestamp";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82,timestamp=1465839830100200200t\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81 1465839830101600200\n" +
                            table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100200Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101600Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testDesignatedTimestampNotCalledTimestampWhenTableExistAlready() throws Exception {
        String table = "tableExistAlready";
        runInContext(() -> {
            try (
                    SqlCompiler compiler = new SqlCompiler(engine);
                    SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)) {
                compiler.compile(
                        "create table " + table + " (location SYMBOL, temperature DOUBLE, time TIMESTAMP) timestamp(time);",
                        sqlExecutionContext);
            } catch (SqlException ex) {
                throw new RuntimeException(ex);
            }
            recvBuffer =
                    table + ",location=us-midwest temperature=82,time=1465839830100300200t 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast,city=york temperature=81 1465839830101400200\n" +
                            table + ",location=us-midwest,city=london temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80,time=1465839830102500200t\n" +
                            table + ",location=us-westcost temperature=82,time=1465839830102600200t 1465839830102700200\n";
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\ttemperature\ttime\tcity\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100300Z\t\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\t\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\tyork\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\tlondon\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\t\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102500Z\t\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102600Z\t\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testDuplicateField() throws Exception {
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
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testDuplicateFieldInFirstRow() throws Exception {
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
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testDuplicateFieldInFirstRowCaseInsensitivity() throws Exception {
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
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testDuplicateFieldNonASCII() throws Exception {
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
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "terület\thőmérséklet\tветер\ttimestamp\n" +
                    "us-midwest\t82.0\t2.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t3.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2.1\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\tNaN\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\t80.0\tNaN\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2.2\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testDuplicateFieldNonASCIIFirstRow() throws Exception {
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
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "terület\thőmérséklet\tветер\ttimestamp\n" +
                    "us-midwest\t82.0\t2.5\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t3.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2.1\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\tNaN\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\t80.0\tNaN\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2.2\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testDuplicateFieldNonASCIIDifferentCaseFirstRow() throws Exception {
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
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "terület\thőmérséklet\tветер\ttimestamp\n" +
                    "us-midwest\t82.0\t2.5\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t3.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2.1\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\tNaN\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\t80.0\tNaN\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2.2\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testDuplicateFieldWhenTableExistsAlreadyNonASCIIFirstRow() throws Exception {
        String table = "dupField";
        runInContext(() -> {
            try (
                    SqlCompiler compiler = new SqlCompiler(engine);
                    SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)) {
                compiler.compile(
                        "create table " + table + " (terület SYMBOL, hőmérséklet DOUBLE, timestamp TIMESTAMP) timestamp(timestamp);",
                        sqlExecutionContext);
            } catch (SqlException ex) {
                throw new RuntimeException(ex);
            }
            recvBuffer =
                    table + ",terület=us-midwest hőmérséklet=82,ветер=2.5,ВЕтеР=2.4 1465839830100400200\n" +
                            table + ",terület=us-midwest hőmérséklet=83,ветер=3.0 1465839830100500200\n" +
                            table + ",terület=us-eastcoast hőmérséklet=81,HŐMÉRSÉKLET=23,ветер=2.0 1465839830101400200\n" +
                            table + ",terület=us-midwest ветер=2.1,hőmérséklet=85 1465839830102300200\n" +
                            table + ",terület=us-eastcoast hőmérséklet=89 1465839830102400200\n" +
                            table + ",terület=us-eastcoast hőmérséklet=80 1465839830102400200\n" +
                            table + ",terület=us-westcost hőmérséklet=82,ветер=2.2 1465839830102500200\n";
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "terület\thőmérséklet\ttimestamp\tветер\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\t2.5\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\t3.0\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\t2.0\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\t2.1\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\tNaN\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\tNaN\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\t2.2\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testDuplicateFieldWhenTableExistsAlready() throws Exception {
        String table = "tableExistAlready";
        runInContext(() -> {
            try (
                    SqlCompiler compiler = new SqlCompiler(engine);
                    SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)) {
                compiler.compile(
                        "create table " + table + " (location SYMBOL, temperature DOUBLE, timestamp TIMESTAMP) timestamp(timestamp);",
                        sqlExecutionContext);
            } catch (SqlException ex) {
                throw new RuntimeException(ex);
            }
            recvBuffer =
                    table + ",location=us-midwest temperature=82,timestamp=1465839830100400200t 1465839830100300200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast,city=york,city=london temperature=81 1465839830101400200\n" +
                            table + ",location=us-midwest,city=london temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82,timestamp=1465839830102500200t\n";
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\tcity\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\t\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\t\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\tyork\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\tlondon\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\t\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\t\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\t\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testDifferentCaseForExistingColumnWhenTableExistsAlready() throws Exception {
        String table = "tableExistAlready";
        runInContext(() -> {
            try (
                    SqlCompiler compiler = new SqlCompiler(engine);
                    SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)) {
                compiler.compile(
                        "create table " + table + " (location SYMBOL, temperature DOUBLE, timestamp TIMESTAMP) timestamp(timestamp);",
                        sqlExecutionContext);
            } catch (SqlException ex) {
                throw new RuntimeException(ex);
            }
            recvBuffer =
                    table + ",location=us-midwest temperature=82,timestamp=1465839830100400200t 1465839830100300200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast,city=york,city=london temperature=81,Temperature=89 1465839830101400200\n" +
                            table + ",location=us-midwest,LOCation=Europe,City=london,city=windsor temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast Temperature=89,temperature=88 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82,timestamp=1465839830102500200t\n";
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\tcity\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\t\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\t\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\tyork\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\tlondon\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\t\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\t\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\t\n";
            assertTable(expected, table);
        });
    }

    @Ignore
    @Test
    public void testNonAsciiTablenameWithUtf16SurrogateChar() throws Exception {
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
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testDuplicateNewField() throws Exception {
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
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\thumidity\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\tNaN\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\tNaN\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\t24.0\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\t27.0\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\tNaN\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\tNaN\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\tNaN\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testDuplicateNewFieldAlternating() throws Exception {
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
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\thumidity\tanother\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\tNaN\tNaN\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\tNaN\tNaN\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\t24.0\t26.0\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\t27.0\tNaN\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\t31.0\t30.0\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\tNaN\tNaN\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\tNaN\tNaN\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testDuplicateNewFieldCaseInsensitivity() throws Exception {
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
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\thumidity\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\tNaN\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\tNaN\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\t24.0\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\t27.0\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\t28.0\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\tNaN\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\tNaN\n";
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
                            table + ",location=us-eastcoast temperature=81,timestamp=1465839830101500200t 1465839830101600200\n" +
                            table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101500Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testDuplicateTimestampInFirstRow() throws Exception {
        String table = "duplicateTimestamp";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82,timestamp=1465839830100400200t,TimeStamp=1465839830100450200t 1465839830100700200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81 1465839830101600200\n" +
                            table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101600Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testEmptyLine() throws Exception {
        runInContext(() -> {
            recvBuffer = "\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
        });
    }

    @Test
    public void testExtremeFragmentation() throws Exception {
        String table = "extremeFrag";
        runInContext(() -> {
            String allMsgs = makeMessages(table);
            int n = 0;
            while (n < allMsgs.length()) {
                recvBuffer = allMsgs.substring(n, n + 1);
                n++;
                handleContextIO();
                Assert.assertFalse(disconnected);
            }
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testFailure() throws Exception {
        final AtomicInteger nCommitedLines = new AtomicInteger(4);
        String table = "failure1";
        Runnable onCommitNewEvent = () -> {
            if (nCommitedLines.decrementAndGet() <= 0) {
                throw new RuntimeException("Failed");
            }
        };
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81 1465839830101400200\n" +
                            table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleContextIO();
            Assert.assertTrue(disconnected);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n";
            assertTable(expected, table);
        }, onCommitNewEvent);
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
    public void testMaxSizes() throws Exception {
        String table = "maxSize";
        runInContext(() -> {
            String longMeasurement = table + ",location=us-eastcoastxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx temperature=81 1465839830101400200\n";
            Assert.assertFalse(longMeasurement.length() < lineTcpConfiguration.getMaxMeasurementSize());
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            longMeasurement +
                            table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            Assert.assertFalse(recvBuffer.length() < lineTcpConfiguration.getNetMsgBufferSize());
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoastxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testMoreDuplicateNewFields() throws Exception {
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
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\thumidity\tpollution\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\tNaN\tNaN\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\tNaN\tNaN\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\t24.0\t2.0\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\t27.0\t3.0\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\tNaN\t5.0\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\tNaN\tNaN\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\tNaN\tNaN\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testMultipleTablesWithMultipleWriterThreads() throws Exception {
        nWriterThreads = 5;
        int nTables = 12;
        int nIterations = 20_000;
        testThreading(nTables, nIterations);
    }

    @Test
    public void testMultipleTablesWithSingleWriterThread() throws Exception {
        nWriterThreads = 1;
        int nTables = 3;
        int nIterations = 20_000;
        testThreading(nTables, nIterations);
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
            handleContextIO();
            Assert.assertFalse(disconnected);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testMultipleMeasurements2() throws Exception {
        String table = "multipleMeasurements1";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast temperature=81 1465839830101400200\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
            recvBuffer =
                    table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testMultipleMeasurements3() throws Exception {
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
            handleContextIO();
            Assert.assertFalse(disconnected);
            closeContext();
            String expected = "temperature\tpressure\ttimestamp\n" +
                    "82.0\t100\t2016-06-13T17:43:50.100400Z\n" +
                    "83.0\t100\t2016-06-13T17:43:50.100500Z\n" +
                    "81.0\t102\t2016-06-13T17:43:50.101400Z\n" +
                    "85.0\t103\t2016-06-13T17:43:50.102300Z\n" +
                    "89.0\t101\t2016-06-13T17:43:50.102400Z\n" +
                    "80.0\t100\t2016-06-13T17:43:50.102400Z\n" +
                    "82.0\t100\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testMultipleMeasurements4() throws Exception {
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
            handleContextIO();
            Assert.assertFalse(disconnected);
            closeContext();
            String expected = "temperature\tpressure\ttimestamp\n" +
                    "82.0\t100\t2016-06-13T17:43:50.100400Z\n" +
                    "83.0\t100\t2016-06-13T17:43:50.100500Z\n" +
                    "81.0\t102\t2016-06-13T17:43:50.101400Z\n" +
                    "85.0\t103\t2016-06-13T17:43:50.102300Z\n" +
                    "89.0\t101\t2016-06-13T17:43:50.102400Z\n" +
                    "80.0\t100\t2016-06-13T17:43:50.102400Z\n" +
                    "82.0\t100\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testNewTableNullType() throws Exception {
        runInContext(() -> {
            recvBuffer =
                    "vbw water_speed_longitudinal=0.07,water_speed_transveral=,water_speed_status=\"A\",ground_speed_longitudinal=0,ground_speed_transveral=0,ground_speed_status=\"A\",water_speed_stern_transversal=,water_speed_stern_transversal_status=\"V\",ground_speed_stern_transversal=0,ground_speed_stern_transversal_status=\"V\" 1627046637414969856\n";
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();


            try (
                    final SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1);
                    SqlCompiler compiler = new SqlCompiler(engine)
            ) {
                try {
                    // must not create table
                    TestUtils.assertSql(
                            compiler,
                            context,
                            "tables()",
                            sink,
                            "id\tname\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\tcommitLag\n"
                    );
                    // should be able to create table after this (e.g. no debris left by ILP)
                    compiler.compile("create table vbw(a int)", context);

                } catch (SqlException e) {
                    Assert.fail();
                }
            }
        });
    }

    @Test
    public void testNonPrintableChars() throws Exception {
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
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\thőmérséklet" + nonPrintable1 + "\thőmérséklet" + nonPrintable2 + "\thőmérséklet" + nonPrintable + "\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\tNaN\tNaN\tNaN\n" +
                    "us-mid" + nonPrintable1 + "west\t83.0\t2016-06-13T17:43:50.100500Z\tNaN\tNaN\tNaN\n" +
                    "us-eastcoast" + nonPrintable2 + "\t81.0\t2016-06-13T17:43:50.101400Z\tNaN\tNaN\tNaN\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\t24.0\tNaN\tNaN\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\tNaN\t26.0\tNaN\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\tNaN\t23.0\t25.0\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\tNaN\tNaN\tNaN\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testOverflow() throws Exception {
        runInContext(() -> {
            int msgBufferSize = lineTcpConfiguration.getNetMsgBufferSize();
            recvBuffer = "A";
            while (recvBuffer.length() <= msgBufferSize) {
                recvBuffer += recvBuffer;
            }
            int nUnread = recvBuffer.length() - msgBufferSize;
            handleContextIO();
            Assert.assertTrue(disconnected);
            Assert.assertEquals(nUnread, recvBuffer.length());
        });
    }

    @Test
    public void testQuotes() throws Exception {
        runInContext(() -> {
            recvBuffer = "tbl,t1=tv1,t2=tv2 f1=\"fv1\",f2=\"Zen Internet Ltd\" 1465839830100400200\n" +
                    "tbl,t1=tv1,t2=tv2 f1=\"Zen Internet Ltd\" 1465839830100400200\n" +
                    "tbl,t1=tv1,t2=tv2 f1=\"Zen=Internet,Ltd\" 1465839830100400200\n" +
                    "tbl,t1=t\\\"v1,t2=t\"v2 f2=\"1\" 1465839830100400200\n" +
                    "tbl,t1=\"tv1\",t2=tv2 f2=\"1\" 1465839830100400200\n" +
                    "tbl,t1=tv1\",t2=tv2 f2=\"1\" 1465839830100400200\n" +
                    "tbl,t1=\"tv1,t2=tv2 f2=\"1\" 1465839830100400200\n" +
                    "tbl,t1=tv1,t2=tv2 f1=\"Zen Internet Ltd\",f2=\"fv2\" 1465839830100400200\n";
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "t1\tt2\tf1\tf2\ttimestamp\n" +
                    "tv1\ttv2\tfv1\tZen Internet Ltd\t2016-06-13T17:43:50.100400Z\n" +
                    "tv1\ttv2\tZen Internet Ltd\t\t2016-06-13T17:43:50.100400Z\n" +
                    "tv1\ttv2\tZen=Internet,Ltd\t\t2016-06-13T17:43:50.100400Z\n" +
                    "t\"v1\tt\"v2\t\t1\t2016-06-13T17:43:50.100400Z\n" +
                    "tv1\"\ttv2\t\t1\t2016-06-13T17:43:50.100400Z\n" +
                    "\"tv1\ttv2\t\t1\t2016-06-13T17:43:50.100400Z\n" +
                    "tv1\ttv2\tZen Internet Ltd\tfv2\t2016-06-13T17:43:50.100400Z\n";
            assertTable(expected, "tbl");
        });
    }

    @Test
    public void testSingleMeasurement() throws Exception {
        String table = "singleMeasurement";
        runInContext(() -> {
            recvBuffer = table + ",location=us-midwest temperature=82 1465839830100400200\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testStrings() throws Exception {
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
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\training\ttimestamp\n" +
                    "us-eastcoast\ttrue\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\tfalse\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\tf\t2016-06-13T17:43:50.100500Z\n" +
                    "us-midwest\tt\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\tT\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\tF\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\tFalse\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testSymbolOrder1() throws Exception {
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
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\tsensor\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\ttype3\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\ttype1\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\ttype6\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102400Z\ttype1\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\ttype1\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\ttype3\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102401Z\ttype1\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102402Z\ttype1\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102403Z\ttype3\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102504Z\ttype1\n";
            assertTable(expected, table);
        });
    }

    @Test
    public void testTableParameterRetentionOnAddColumn() throws Exception {
        String table = "retention";
        runInContext(() -> {
            try (
                    SqlCompiler compiler = new SqlCompiler(engine);
                    SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)) {
                compiler.compile(
                        "create table " + table + " (location SYMBOL, temperature DOUBLE, timestamp TIMESTAMP) timestamp(timestamp) partition by DAY WITH maxUncommittedRows=3, commitLag=250ms;",
                        sqlExecutionContext);
            } catch (SqlException ex) {
                throw new RuntimeException(ex);
            }
            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, table)) {
                Assert.assertEquals(3, reader.getMetadata().getMaxUncommittedRows());
                Assert.assertEquals(250_000, reader.getMetadata().getCommitLag());
            }
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-midwest temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast,city=york temperature=81 1465839830101400200\n" +
                            table + ",location=us-midwest temperature=85 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\tcity\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\t\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\t\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\tyork\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\t\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\t\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\t\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\t\n";
            assertTable(expected, table);
            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, table)) {
                Assert.assertEquals(3, reader.getMetadata().getMaxUncommittedRows());
                Assert.assertEquals(250_000, reader.getMetadata().getCommitLag());
            }
        });
    }

    @Test
    public void testUseReceivedTimestamp1() throws Exception {
        String table = "testAutoTimestamp";
        runInContext(() -> {
            microSecondTicks = 0;
            recvBuffer =
                    table + ",location=us-midwest temperature=82\n" +
                            table + ",location=us-midwest temperature=83\n" +
                            table + ",location=us-eastcoast temperature=81\n" +
                            table + ",location=us-midwest temperature=85\n" +
                            table + ",location=us-eastcoast temperature=89\n" +
                            table + ",location=us-eastcoast temperature=80\n" +
                            table + ",location=us-westcost temperature=82\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t1970-01-01T00:00:00.000000Z\n" +
                    "us-midwest\t83.0\t1970-01-01T00:00:00.000000Z\n" +
                    "us-eastcoast\t81.0\t1970-01-01T00:00:00.000000Z\n" +
                    "us-midwest\t85.0\t1970-01-01T00:00:00.000000Z\n" +
                    "us-eastcoast\t89.0\t1970-01-01T00:00:00.000000Z\n" +
                    "us-eastcoast\t80.0\t1970-01-01T00:00:00.000000Z\n" +
                    "us-westcost\t82.0\t1970-01-01T00:00:00.000000Z\n";
            assertTable(expected, table);
        });
    }

    private void addTable(String table) {
        try (
                TableModel model = new TableModel(configuration, table, PartitionBy.NONE)
                        .col("location", ColumnType.SYMBOL)
                        .col("temperature", ColumnType.DOUBLE)
                        .timestamp()
        ) {
            CairoTestUtils.create(model);
        }
    }

    private void assertTableCount(CharSequence tableName, int nExpectedRows, long maxExpectedTimestampNanos) {
        try (TableReader reader = new TableReader(configuration, tableName)) {
            Assert.assertEquals(maxExpectedTimestampNanos / 1000, reader.getMaxTimestamp());
            int timestampColIndex = reader.getMetadata().getTimestampIndex();
            TableReaderRecordCursor recordCursor = reader.getCursor();
            int nRows = 0;
            long timestampinNanos = 1465839830100400200L;
            while (recordCursor.hasNext()) {
                long actualTimestampInMicros = recordCursor.getRecord().getTimestamp(timestampColIndex);
                Assert.assertEquals(timestampinNanos / 1000, actualTimestampInMicros);
                timestampinNanos += 1000;
                nRows++;
            }
            Assert.assertEquals(nExpectedRows, nRows);
        }
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

    private void testFragmentation(int breakPos, String table) throws Exception {
        runInContext(() -> {
            String allMsgs = makeMessages(table);
            recvBuffer = allMsgs.substring(0, breakPos);
            handleContextIO();
            Assert.assertFalse(disconnected);
            recvBuffer = allMsgs.substring(breakPos);
            handleContextIO();
            Assert.assertFalse(disconnected);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
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
        Random random = new Random(0);
        int[] countByTable = new int[nTables];
        long[] maxTimestampByTable = new long[nTables];
        final long initialTimestampNanos = 1465839830100400200L;
        final long timestampIncrementInNanos = 1000;
        Arrays.fill(maxTimestampByTable, initialTimestampNanos);
        runInContext(() -> {
            int nTablesSelected = 0;
            int nTotalUpdates = 0;
            for (int nIter = 0; nIter < nIterations; nIter++) {
                int nLines = random.nextInt(50) + 1;
                sink.clear();
                for (int nLine = 0; nLine < nLines; nLine++) {
                    int nTable;
                    if (nTablesSelected < nTables) {
                        nTable = nTablesSelected++;
                    } else {
                        double tableSelector = random.nextDouble() * accLoadFactors;
                        nTable = nTables;
                        while (--nTable > 0) {
                            if (tableSelector > loadFactors[nTable - 1]) {
                                break;
                            }
                        }
                    }
                    long timestamp = maxTimestampByTable[nTable];
                    maxTimestampByTable[nTable] += timestampIncrementInNanos;
                    double temperature = 50.0 + (random.nextInt(500) / 10.0);
                    sink.put("weather").put(nTable)
                            .put(",location=us-midwest temperature=").put(temperature)
                            .put(' ').put(timestamp).put('\n');
                    countByTable[nTable]++;
                    nTotalUpdates++;
                }
                recvBuffer = sink.toString();
                do {
                    handleContextIO();
                    // Assert.assertFalse(disconnected);
                } while (recvBuffer.length() > 0);
            }
            waitForIOCompletion();
            closeContext();
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
}

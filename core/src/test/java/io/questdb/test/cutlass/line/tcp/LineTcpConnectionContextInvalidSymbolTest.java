/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import org.junit.Assert;
import org.junit.Test;

public class LineTcpConnectionContextInvalidSymbolTest extends BaseLineTcpContextTest {

    @Test
    public void testInvalidSymbol() throws Exception {
        testInvalidSymbol(false);
    }

    @Test
    public void testInvalidSymbolWithDisconnect() throws Exception {
        testInvalidSymbol(true);
    }

    private void testInvalidSymbol(boolean disconnectOnError) throws Exception {
        this.disconnectOnError = disconnectOnError;

        String table = "test";
        runInContext(() -> {
            recvBuffer =
                    table + ",ip_address=127.0.0.1 cpu=83 1465839830100500200\n" +
                            table + ",ip_address=Invalid IP address. cpu=13 1465839830101400200\n" +
                            table + ",ip_address=192.168.0.1 cpu=42 1465839830100500200\n";

            handleContextIO0();
            Assert.assertEquals(disconnectOnError, disconnected);
            closeContext();

            String expected = """
                    ip_address\tcpu\ttimestamp
                    127.0.0.1\t83.0\t2016-06-13T17:43:50.100500Z
                    """;
            if (!disconnectOnError) {
                // The very last measurement should be included if we tolerate invalid measurements.
                expected += "192.168.0.1\t42.0\t2016-06-13T17:43:50.100500Z\n";
            }
            try (
                    TableReader reader = newOffPoolReader(configuration, table);
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
            ) {
                TableReaderMetadata meta = reader.getMetadata();
                assertCursorTwoPass(expected, cursor, meta);
                Assert.assertEquals(3, meta.getColumnCount());
                Assert.assertEquals(ColumnType.SYMBOL, meta.getColumnType("ip_address"));
                Assert.assertEquals(ColumnType.DOUBLE, meta.getColumnType("cpu"));
                Assert.assertEquals(ColumnType.TIMESTAMP, meta.getColumnType("timestamp"));
            }
        });
    }
}

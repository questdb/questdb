/*+*****************************************************************************
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

import io.questdb.network.NetworkFacade;
import io.questdb.test.tools.LogCapture;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class LineTcpConnectionContextBrokenUTF8Test extends BaseLineTcpContextTest {
    private static final LogCapture capture = new LogCapture();

    @Test
    public void testBrokenUTF8Encoding() throws Exception {
        testBrokenUTF8Encoding(false);
    }

    @Test
    public void testBrokenUTF8EncodingWithDisconnect() throws Exception {
        testBrokenUTF8Encoding(true);
    }

    @Test
    public void testMalformedUtf8ColumnNameLeavesWriterHealthy() throws Exception {
        final String table = "malformedUtf8ColumnName";
        assertMalformedUtf8LeavesWriterHealthy(
                table,
                "LONG",
                table + " valu" + (char) 0xC3 + "e=1i 1465839830100400200\n" +
                        table + " value=42i 1465839830100400300\n",
                "42"
        );
    }

    @Test
    public void testMalformedUtf8StringIsRejected() throws Exception {
        final String table = "malformedUtf8String";
        runInContext(() -> {
            execute("CREATE TABLE " + table + " (value STRING, timestamp TIMESTAMP) " +
                    "TIMESTAMP(timestamp) PARTITION BY DAY WAL");
            // a malformed field value is rejected like malformed bytes anywhere else in the line
            // (see testBrokenUTF8Encoding); storing null in its place would drop the value the
            // client sent while still acknowledging the write
            recvBuffer = table + " value=\"1" + (char) 0xC3 + "\" 1465839830100400200\n" +
                    table + " value=\"ok\" 1465839830100400300\n";
            handleContextIO0();
            closeContext();
            drainWalQueue();

            assertQuery("SELECT value FROM " + table + " ORDER BY timestamp")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            value
                            ok
                            """);
        });
    }

    @Test
    public void testMalformedUtf8StringLeavesWriterHealthy() throws Exception {
        final String table = "malformedUtf8Lifecycle";
        assertMalformedUtf8LeavesWriterHealthy(
                table,
                "STRING",
                table + " value=\"1" + (char) 0xC3 + "\" 1465839830100400200\n" +
                        table + " value=\"2" + (char) 0xC3 + "\" 1465839830100400300\n" +
                        table + " value=\"ok\" 1465839830100400400\n",
                "ok"
        );
    }

    @Test
    public void testMalformedUtf8SymbolLeavesWriterHealthy() throws Exception {
        final String table = "malformedUtf8SymbolLifecycle";
        assertMalformedUtf8LeavesWriterHealthy(
                table,
                "SYMBOL",
                table + " value=\"1" + (char) 0xC3 + "\" 1465839830100400200\n" +
                        table + " value=\"2" + (char) 0xC3 + "\" 1465839830100400300\n" +
                        table + " value=\"ok\" 1465839830100400400\n",
                "ok"
        );
    }

    private void assertMalformedUtf8LeavesWriterHealthy(
            String table,
            String columnType,
            String input,
            String expectedValue
    ) throws Exception {
        runInContext(() -> {
            execute("CREATE TABLE " + table + " (value " + columnType + ", timestamp TIMESTAMP) " +
                    "TIMESTAMP(timestamp) PARTITION BY DAY WAL");
            // Rejecting malformed input must not cost the table its writer. A bare CairoException
            // reaches the scheduler's catch-all, which calls setWriterInError() and drops the
            // writer. Asserting on rows alone cannot see that because surviving lines still land.
            recvBuffer = input;
            capture.start();
            try {
                handleContextIO0();
                Assert.assertFalse("malformed UTF8 must not disconnect the client", disconnected);
                // handleWriterException() logs this immediately before setWriterInError() drops
                // the writer, so its absence is the assertion that the writer survived.
                capture.assertNotLogged("closing writer because of error");
            } finally {
                capture.stop();
            }

            closeContext();
            drainWalQueue();

            // Rejection happens before the malformed value reaches a WAL segment. Rejecting later,
            // at apply time, would suspend the table instead of failing the write.
            assertQuery("SELECT suspended FROM wal_tables() WHERE name = '" + table + "'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            suspended
                            false
                            """);
            assertQuery("SELECT value FROM " + table + " ORDER BY timestamp")
                    .noLeakCheck()
                    .expectSize()
                    .returns("value\n" + expectedValue + "\n");
        });
    }

    private void testBrokenUTF8Encoding(boolean disconnectOnError) throws Exception {
        this.disconnectOnError = disconnectOnError;

        char nonPrintable = 0x3000;
        char nonPrintable1 = 0x3080;
        char nonPrintable2 = 0x3a55;
        String table = "nonPrintableChars";
        runInContext(() -> {
            recvBuffer =
                    table + ",location=us-midwest temperature=82 1465839830100400200\n" +
                            table + ",location=us-mid" + nonPrintable1 + "west temperature=83 1465839830100500200\n" +
                            table + ",location=us-eastcoast" + nonPrintable2 + " temperature=81 1465839830101400200\n" +
                            table + ",location=us-midwest temperature=85,hőmérséklet=24 1465839830102300200\n" +
                            table + ",location=us-eastcoast temperature=89,hőmérséklet=26 1465839830102400200\n" +
                            table + ",location=us-eastcoast temperature=80,hőmérséklet=25" + nonPrintable + ",hőmérséklet=23 1465839830102400200\n" +
                            table + ",location=us-westcost temperature=82 1465839830102500200\n";

            handleContextIO0();
            Assert.assertEquals(disconnectOnError, disconnected);
            closeContext();
        });
    }

    NetworkFacade provideLineTcpNetworkFacade() {
        return new LineTcpNetworkFacadeBrokenUTF8Encoding();
    }

    class LineTcpNetworkFacadeBrokenUTF8Encoding extends LineTcpNetworkFacade {
        @Override
        byte[] getBytes(String recvBuffer) {
            return recvBuffer.getBytes(StandardCharsets.ISO_8859_1);
        }
    }
}

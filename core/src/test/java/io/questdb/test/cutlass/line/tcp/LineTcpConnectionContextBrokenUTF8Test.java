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

import io.questdb.network.NetworkFacade;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class LineTcpConnectionContextBrokenUTF8Test extends BaseLineTcpContextTest {

    @Test
    public void testBrokenUTF8Encoding() throws Exception {
        testBrokenUTF8Encoding(false);
    }

    @Test
    public void testBrokenUTF8EncodingWithDisconnect() throws Exception {
        testBrokenUTF8Encoding(true);
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

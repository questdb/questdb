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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cutlass.line.AbstractLineTcpSender;
import io.questdb.cutlass.line.LineTcpSenderV2;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.Net;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class LineTcpReceiverFuzzTest extends AbstractLineTcpReceiverFuzzTest {

    private static final Log LOG = LogFactory.getLog(LineTcpReceiverFuzzTest.class);

    @Test
    public void testAddColumns() throws Exception {
        initLoadParameters(15, 2, 2, 5, 75);
        initFuzzParameters(-1, -1, -1, 4, -1, false, true, false);
        runTest();
    }

    @Test
    public void testAddColumnsNoTagsStringsAsSymbol() throws Exception {
        initLoadParameters(15, 2, 2, 5, 75);
        initFuzzParameters(-1, -1, -1, 4, -1, false, false, false);
        runTest();
    }

    @Test
    public void testAllMixed() throws Exception {
        initLoadParameters(50, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(3, 4, 5, 10, 5, true, true, true);
        runTest();
    }

    @Test
    public void testAllMixedSplitPart() throws Exception {
        initLoadParameters(50, Os.isWindows() ? 3 : 5, 5, 1, 50);
        initFuzzParameters(-1, -1, -1, 10, -1, false, true, false);
        runTest();
    }

    @Test
    public void testDuplicatesReorderingColumns() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, 4, -1, -1, -1, true, true, false);
        runTest();
    }

    @Test
    public void testDuplicatesReorderingColumnsNoTagsStringsAsSymbol() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, 4, -1, -1, -1, true, false, false);
        runTest();
    }

    @Test
    public void testDuplicatesReorderingColumnsSendSymbolsWithSpace() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, 4, -1, -1, -1, true, true, true);
        runTest();
    }

    @Test
    public void testLoad() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 7, 12, 20);
        runTest();
    }

    @Test
    public void testLoadNoTagsStringsAsSymbol() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 7, 12, 20);
        initFuzzParameters(-1, -1, -1, -1, -1, false, false, false);
        runTest();
    }

    @Test
    public void testLoadSendSymbolsWithSpace() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 4, 8, 20);
        initFuzzParameters(-1, -1, -1, -1, -1, false, true, true);
        runTest();
    }

    @Test
    public void testOnSingleConnectionSingeWalUsed() throws Exception {
        Assume.assumeTrue(walEnabled);
        runInContext((receiver) -> {
            send("table", WAIT_ENGINE_TABLE_RELEASE, () -> {
                try (AbstractLineTcpSender lineTcpSender = LineTcpSenderV2.newSender(Net.parseIPv4("127.0.0.1"), bindPort, msgBufferSize)) {
                    for (int i = 0; i < 300; i++) {
                        lineTcpSender
                                .metric("table")
                                .longColumn("tag1", i)
                                .$();
                        lineTcpSender.flush();

                        if (i % 100 == 0) {
                            Os.sleep(20);
                        }

                    }
                }
            });

            // Assert all data went into WAL1 and WAL2 does not exist
            TableToken token = engine.verifyTableName("table");
            Path path = Path.getThreadLocal(engine.getConfiguration().getDbRoot()).concat(token).concat(WalUtils.WAL_NAME_BASE).put("2");
            Assert.assertFalse(engine.getConfiguration().getFilesFacade().exists(path.slash$()));
        });
    }

    @Test
    public void testReorderingAddSkipDuplicateColumnsWithNonAscii() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, 4, 4, -1, 4, true, true, false);
        runTest();
    }

    @Test
    public void testReorderingAddSkipDuplicateColumnsWithNonAsciiNoTagsStringsAsSymbol() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, 4, 4, -1, 4, true, false, false);
        runTest();
    }

    @Test
    public void testReorderingColumns() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(-1, 4, -1, -1, -1, false, true, false);
        runTest();
    }

    @Test
    public void testReorderingColumnsNoTagsStringsAsSymbol() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(-1, 4, -1, -1, -1, false, false, false);
        runTest();
    }

    @Override
    protected Log getLog() {
        return LOG;
    }
}

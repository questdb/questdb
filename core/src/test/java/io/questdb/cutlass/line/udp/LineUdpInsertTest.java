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

package io.questdb.cutlass.line.udp;

import io.questdb.cairo.*;
import io.questdb.cutlass.line.LineProtoSender;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.Os;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;

import java.util.concurrent.locks.LockSupport;

public abstract class LineUdpInsertTest extends AbstractCairoTest {

    protected static final LineUdpReceiverConfiguration RCVR_CONF = new DefaultLineUdpReceiverConfiguration();
    protected static final int LOCALHOST = Net.parseIPv4("127.0.0.1");
    protected static final int PORT = RCVR_CONF.getPort();

    protected static AbstractLineProtoReceiver createLineProtoReceiver(CairoEngine engine) {
        AbstractLineProtoReceiver lpr;
        if (Os.type == Os.LINUX_AMD64) {
            lpr = new LinuxMMLineProtoReceiver(RCVR_CONF, engine, null);
        } else {
            lpr = new LineProtoReceiver(RCVR_CONF, engine, null);
        }
        return lpr;
    }

    protected static LineProtoSender createLineProtoSender() {
        return new LineProtoSender(NetworkFacadeImpl.INSTANCE, 0, LOCALHOST, PORT, 1024, 1);
    }

    protected static void assertReader(String tableName, String expected) {
        assertReader(tableName, expected, (String[]) null);
    }

    protected static void assertReader(String tableName, String expected, String... expectedExtraStringColumns) {
        int numLines = expected.split("[\n]").length - 1;
        CairoException pendingRecoveryErr = null;
        for (int i = 0, n = 5; i < n; i++) {
            // aggressively demand a TableReader up to 5x
            try (TableReader reader = new TableReader(new DefaultCairoConfiguration(root), tableName)) {
                for (int attempts = 28_02_78; attempts > 0; attempts--) {
                    if (reader.size() >= numLines) {
                        break;
                    }
                    LockSupport.parkNanos(1);
                    reader.reload();
                }
                TestUtils.assertReader(expected, reader, sink);
                if (expectedExtraStringColumns != null) {
                    TableReaderMetadata meta = reader.getMetadata();
                    Assert.assertEquals(2 + expectedExtraStringColumns.length, meta.getColumnCount());
                    for (String colName : expectedExtraStringColumns) {
                        Assert.assertEquals(ColumnType.STRING, meta.getColumnType(colName));
                    }
                }
                pendingRecoveryErr = null;
                break;
            } catch (CairoException err) {
                pendingRecoveryErr = err;
                LockSupport.parkNanos(1000000); // 1 milli
            }
        }
        if (pendingRecoveryErr != null) {
            throw pendingRecoveryErr;
        }
    }
}
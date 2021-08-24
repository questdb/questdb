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
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cutlass.line.LineProtoSender;
import io.questdb.griffin.engine.functions.rnd.RndGeoHashFunctionFactory;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

abstract class LineUdpInsertGeoHashTest extends AbstractCairoTest {
    static final String tableName = "tracking";
    static final String targetColumnName = "geohash";

    private static final LineUdpReceiverConfiguration RCVR_CONF = new DefaultLineUdpReceiverConfiguration();
    private static final int LOCALHOST = Net.parseIPv4("127.0.0.1");
    private static final int PORT = RCVR_CONF.getPort();

    @Test
    public abstract void testGeoHashes() throws Exception;

    @Test
    public abstract void testGeoHashesTruncating() throws Exception;

    @Test
    public abstract void testTableHasGeoHashMessageDoesNot() throws Exception;

    @Test
    public abstract void testSeeminglyGoodLookingStringWhichIsTooLongToBeAGeoHash() throws Exception;

    @Test
    public abstract void testGeoHashesNotEnoughPrecision() throws Exception;

    @Test
    public abstract void testWrongCharGeoHashes() throws Exception;

    @Test
    public abstract void testNullGeoHash() throws Exception;

    protected static void assertGeoHash(int columnBits, int lineGeoSizeChars, int numLines, String expected) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (AbstractLineProtoReceiver receiver = createLineProtoReceiver(engine)) {
                    createTable(engine, columnBits);
                    receiver.start();
                    sendGeoHashLines(numLines, lineGeoSizeChars);
                    assertReader(expected);
                }
            }
        });
    }

    protected static AbstractLineProtoReceiver createLineProtoReceiver(CairoEngine engine) {
        AbstractLineProtoReceiver lpr;
        if (Os.type == Os.LINUX_AMD64) {
            lpr = new LinuxMMLineProtoReceiver(RCVR_CONF, engine, null);
        } else {
            lpr = new LineProtoReceiver(RCVR_CONF, engine, null);
        }
        return lpr;
    }

    protected static void createTable(CairoEngine engine, int bitsPrecision) {
        try (TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)) {
            CairoTestUtils.create(model.col(targetColumnName, ColumnType.geohashWithPrecision(bitsPrecision)).timestamp());
        }
        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "pleasure")) {
            writer.warmUp();
        }
    }

    protected static void sendGeoHashLine(String value) {
        try (LineProtoSender sender = new LineProtoSender(NetworkFacadeImpl.INSTANCE, 0, LOCALHOST, PORT, 256 * 1024, 1)) {
            sender.metric(tableName).field(targetColumnName, value).$(1_000_000_000);
            sender.flush();
        }
    }

    protected static LineProtoSender createLineProtoSender() {
        return new LineProtoSender(NetworkFacadeImpl.INSTANCE, 0, LOCALHOST, PORT, 256 * 1024, 1);
    }

    protected static void assertReader(String expected) {
        assertReader(expected, null);
    }

    protected static void assertReader(String expected, String... expectedExtraStringColumns) {
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
                LockSupport.parkNanos(200);
                continue;
            }
        }
        if (pendingRecoveryErr != null) {
            throw pendingRecoveryErr;
        }
    }

    private static void sendGeoHashLines(int numLines, int charsPrecision) {
        Supplier<String> rnd = randomGeoHashGenerator(charsPrecision);
        try (LineProtoSender sender = createLineProtoSender()) {
            for (int i = 0; i < numLines; i++) {
                sender.metric(tableName).field(targetColumnName, rnd.get()).$((long) ((i + 1) * 1e9));
            }
            sender.flush();
        }
    }

    private static Supplier<String> randomGeoHashGenerator(int chars) {
        final Rnd rnd = new Rnd();
        return () -> {
            StringSink sink = Misc.getThreadLocalBuilder();
            GeoHashes.toString(RndGeoHashFunctionFactory.nextGeoHash(rnd, chars * 5), chars, sink);
            return sink.toString();
        };
    }
}
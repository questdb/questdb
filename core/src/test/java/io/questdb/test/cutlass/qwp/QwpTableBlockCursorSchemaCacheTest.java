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

package io.questdb.test.cutlass.qwp;

import io.questdb.client.cutlass.qwp.client.QwpBufferWriter;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.Unsafe;
import io.questdb.cutlass.qwp.protocol.QwpColumnDef;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.cutlass.qwp.server.QwpStreamingDecoder;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_LONG;
import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_TIMESTAMP;
import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_TIMESTAMP_NANOS;
import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class QwpTableBlockCursorSchemaCacheTest {

    @Test
    public void testIdenticalSchemaReusesParseAndDifferentSchemaDoesNot() throws Exception {
        // assertMemoryLeak() tracks only the server's native memory counter. The
        // client encoder and table buffer allocate through the client's own.
        final long clientMemUsedBefore = Unsafe.getMemUsed();
        assertMemoryLeak(() -> {
            try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                ObjList<QwpColumnDef> first = decode(decoder, "val");
                Assert.assertEquals("val", first.getQuick(0).getName());
                // Same column names and types, next message: the cursor must hand
                // back the previous parse rather than allocate a new one.
                ObjList<QwpColumnDef> second = decode(decoder, "val");
                Assert.assertSame(first, second);
                // Same column count and name length, different bytes: a fresh parse.
                ObjList<QwpColumnDef> third = decode(decoder, "vbl");
                Assert.assertNotSame(first, third);
                Assert.assertEquals("vbl", third.getQuick(0).getName());
                // And back again: the cache now holds the latest schema.
                Assert.assertSame(third, decode(decoder, "vbl"));
                Assert.assertNotSame(third, decode(decoder, "val"));
            }
        });
        Assert.assertEquals("client native memory leaked", clientMemUsedBefore, Unsafe.getMemUsed());
    }

    @Test
    public void testSchemaDifferingInOneByteIsNotReused() throws Exception {
        // A name of n bytes makes an (n + 4)-byte section: [n][name][LONG][0][ts type].
        // Sweeping n moves both the last name byte and the final ts-type byte across
        // the prefix and every 8/4/2/1-byte step of the comparison.
        final long clientMemUsedBefore = Unsafe.getMemUsed();
        assertMemoryLeak(() -> {
            try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                for (int n = 1; n <= 24; n++) {
                    final String stem = "c".repeat(n - 1);
                    final ObjList<QwpColumnDef> cached = decode(decoder, stem + "a", TYPE_TIMESTAMP);
                    Assert.assertSame("n=" + n, cached, decode(decoder, stem + "a", TYPE_TIMESTAMP));

                    final ObjList<QwpColumnDef> lastNameByte = decode(decoder, stem + "b", TYPE_TIMESTAMP);
                    Assert.assertNotSame("n=" + n, cached, lastNameByte);
                    Assert.assertEquals(stem + "b", lastNameByte.getQuick(0).getName());

                    final ObjList<QwpColumnDef> finalByte = decode(decoder, stem + "b", TYPE_TIMESTAMP_NANOS);
                    Assert.assertNotSame("n=" + n, lastNameByte, finalByte);
                    Assert.assertEquals(TYPE_TIMESTAMP_NANOS, finalByte.getQuick(1).getTypeCode());
                }
            }
        });
        Assert.assertEquals("client native memory leaked", clientMemUsedBefore, Unsafe.getMemUsed());
    }

    private static ObjList<QwpColumnDef> decode(QwpStreamingDecoder decoder, String columnName) throws Exception {
        return decode(decoder, columnName, TYPE_TIMESTAMP);
    }

    private static ObjList<QwpColumnDef> decode(QwpStreamingDecoder decoder, String columnName, byte timestampType) throws Exception {
        // The encoder does not own the table buffer; both hold native memory.
        try (
                QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                QwpTableBuffer buffer = new QwpTableBuffer("test_table")
        ) {
            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn(columnName, TYPE_LONG, false);
            QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateDesignatedTimestampColumn(timestampType);
            for (int i = 0; i < 3; i++) {
                col.addLong(i);
                tsCol.addLong(1_000_000_000_000L + i);
                buffer.nextRow();
            }
            int size = encoder.encode(buffer);
            QwpBufferWriter buf = encoder.getBuffer();
            QwpMessageCursor msg = decoder.decode(buf.getBufferPtr(), size);
            Assert.assertTrue(msg.hasNextTable());
            QwpTableBlockCursor table = msg.nextTable();
            Assert.assertEquals(3, table.getRowCount());
            return table.getSchema();
        }
    }
}

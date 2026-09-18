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

package io.questdb.test.cutlass.qwp;

import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

public class QwpSchemaIdentityParserTest {

    @Test
    public void testKnownUnknownAndMultiTableIdentityReset() throws Exception {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
             QwpTableBuffer known = table("known");
             QwpTableBuffer unknown = table("unknown");
             QwpTableBuffer maximum = table("maximum")) {
            encoder.beginSchemaMessage(3, new GlobalSymbolDictionary(), -1, -1);
            encoder.addSchemaTable(known, 17, 29);
            encoder.addSchemaTable(unknown, -1, -1);
            encoder.addSchemaTable(maximum, Integer.MAX_VALUE, Long.MAX_VALUE);
            int length = encoder.finishMessage();

            QwpMessageCursor cursor = new QwpMessageCursor();
            cursor.of(encoder.getBuffer().getBufferPtr(), length, new ObjList<>());
            QwpTableBlockCursor first = cursor.nextTable();
            Assert.assertTrue(first.hasKnownSchemaIdentity());
            Assert.assertEquals(17, first.getSchemaTableId());
            Assert.assertEquals(29, first.getSchemaMetadataVersion());
            QwpTableBlockCursor second = cursor.nextTable();
            Assert.assertFalse(second.hasKnownSchemaIdentity());
            Assert.assertEquals(-1, second.getSchemaTableId());
            Assert.assertEquals(-1, second.getSchemaMetadataVersion());
            QwpTableBlockCursor third = cursor.nextTable();
            Assert.assertTrue(third.hasKnownSchemaIdentity());
            Assert.assertEquals(Integer.MAX_VALUE, third.getSchemaTableId());
            Assert.assertEquals(Long.MAX_VALUE, third.getSchemaMetadataVersion());
            Assert.assertFalse(cursor.hasNextTable());
        }
    }

    @Test
    public void testReservedAndNegativeKnownIdentityRejected() throws Exception {
        byte[] message = knownMessage();
        int identityOffset = identityOffset("t");
        message[identityOffset] = 2;
        assertParseError(message, message.length, QwpParseException.ErrorCode.INVALID_SCHEMA_IDENTITY);

        message = knownMessage();
        putInt(message, identityOffset + 1, -1);
        assertParseError(message, message.length, QwpParseException.ErrorCode.INVALID_SCHEMA_IDENTITY);

        message = knownMessage();
        putLong(message, identityOffset + 1 + Integer.BYTES, -1);
        assertParseError(message, message.length, QwpParseException.ErrorCode.INVALID_SCHEMA_IDENTITY);
    }

    @Test
    public void testTruncatedKnownIdentityRejectedAtEveryByte() throws Exception {
        byte[] complete = knownMessage();
        int identityOffset = identityOffset("t");
        for (int identityBytes = 0; identityBytes < 1 + Integer.BYTES + Long.BYTES; identityBytes++) {
            int length = identityOffset + identityBytes;
            byte[] truncated = new byte[length];
            System.arraycopy(complete, 0, truncated, 0, length);
            putInt(truncated, QwpConstants.HEADER_OFFSET_PAYLOAD_LENGTH, length - QwpConstants.HEADER_SIZE);
            assertParseError(truncated, length, QwpParseException.ErrorCode.HEADER_TOO_SHORT);
        }
    }

    @Test
    public void testIllegalSchemaMessageFlagsAndTablelessFrameRejected() throws Exception {
        byte[] message = knownMessage();
        message[QwpConstants.HEADER_OFFSET_FLAGS] |= QwpConstants.FLAG_CONTROL;
        assertParseError(message, message.length, QwpParseException.ErrorCode.INVALID_SCHEMA_IDENTITY);

        byte[] tableless = new byte[QwpConstants.HEADER_SIZE];
        putInt(tableless, 0, QwpConstants.MAGIC_MESSAGE);
        tableless[QwpConstants.HEADER_OFFSET_VERSION] = QwpConstants.VERSION;
        tableless[QwpConstants.HEADER_OFFSET_FLAGS] = QwpConstants.FLAG_SCHEMA;
        assertParseError(tableless, tableless.length, QwpParseException.ErrorCode.INVALID_SCHEMA_IDENTITY);
    }

    private static void assertParseError(byte[] bytes, int length, QwpParseException.ErrorCode errorCode) throws Exception {
        long address = Unsafe.malloc(length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < length; i++) {
                Unsafe.putByte(address + i, bytes[i]);
            }
            QwpMessageCursor cursor = new QwpMessageCursor();
            QwpParseException ex = Assert.assertThrows(QwpParseException.class, () -> {
                cursor.of(address, length, new ObjList<>());
                while (cursor.hasNextTable()) {
                    cursor.nextTable();
                }
            });
            Assert.assertEquals(errorCode, ex.getErrorCode());
        } finally {
            Unsafe.free(address, length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static int identityOffset(String tableName) {
        return QwpConstants.HEADER_SIZE + 1 + tableName.length();
    }

    private static byte[] knownMessage() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer table = table("t")) {
            int length = encoder.encodeSchema(table, 1, 2);
            byte[] bytes = new byte[length];
            long address = encoder.getBuffer().getBufferPtr();
            for (int i = 0; i < length; i++) {
                bytes[i] = Unsafe.getByte(address + i);
            }
            return bytes;
        }
    }

    private static void putInt(byte[] bytes, int offset, int value) {
        for (int i = 0; i < Integer.BYTES; i++) {
            bytes[offset + i] = (byte) (value >>> (8 * i));
        }
    }

    private static void putLong(byte[] bytes, int offset, long value) {
        for (int i = 0; i < Long.BYTES; i++) {
            bytes[offset + i] = (byte) (value >>> (8 * i));
        }
    }

    private static QwpTableBuffer table(String name) {
        QwpTableBuffer table = new QwpTableBuffer(name);
        table.getOrCreateColumn("n", QwpConstants.TYPE_LONG, true).addLong(1);
        table.nextRow();
        return table;
    }
}

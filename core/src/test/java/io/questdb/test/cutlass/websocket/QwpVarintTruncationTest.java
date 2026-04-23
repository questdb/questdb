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

package io.questdb.test.cutlass.websocket;

import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpSymbolColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpVarint;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.*;
import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

/**
 * Tests that varint values exceeding Integer.MAX_VALUE are rejected
 * rather than silently truncated to int. Without the fix, a varint
 * encoding e.g. 0x1_0000_0005 truncates to 5, which can pass bounds
 * checks and cause silent data corruption.
 */
public class QwpVarintTruncationTest extends AbstractWebSocketTest {

    @Test
    public void testDeltaDictCountExceedingIntRange() throws Exception {
        // deltaStartId = 0, deltaCount = 0x1_0000_0000 (exceeds int range).
        // Without the fix, truncates to 0 and silently produces an empty delta.
        assertMemoryLeak(() -> {
            long deltaStartId = 0;
            long deltaCount = 0x1_0000_0000L;

            int payloadSize = QwpVarint.encodedLength(deltaStartId)
                    + QwpVarint.encodedLength(deltaCount);
            int totalSize = HEADER_SIZE + payloadSize;
            long address = Unsafe.malloc(totalSize, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putInt(address + HEADER_OFFSET_MAGIC, MAGIC_MESSAGE);
                Unsafe.getUnsafe().putByte(address + HEADER_OFFSET_VERSION, VERSION_1);
                Unsafe.getUnsafe().putByte(address + HEADER_OFFSET_FLAGS, FLAG_DELTA_SYMBOL_DICT);
                Unsafe.getUnsafe().putShort(address + HEADER_OFFSET_TABLE_COUNT, (short) 0);
                Unsafe.getUnsafe().putInt(address + HEADER_OFFSET_PAYLOAD_LENGTH, payloadSize);

                long pos = address + HEADER_SIZE;
                pos = QwpVarint.encode(pos, deltaStartId);
                QwpVarint.encode(pos, deltaCount);

                QwpMessageCursor cursor = new QwpMessageCursor();
                ObjList<String> connectionDict = new ObjList<>();
                cursor.of(address, totalSize, null, connectionDict);
                Assert.fail("Expected QwpParseException for deltaCount exceeding int range");
            } catch (QwpParseException e) {
                Assert.assertTrue(e.getMessage().contains("int range"));
            } finally {
                Unsafe.free(address, totalSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDeltaDictStartIdExceedingIntRange() throws Exception {
        // deltaStartId = 0x1_0000_0005, deltaCount = 0.
        // Without the fix, deltaStartId truncates to 5.
        assertMemoryLeak(() -> {
            long deltaStartId = 0x1_0000_0005L;
            int deltaCount = 0;

            int payloadSize = QwpVarint.encodedLength(deltaStartId)
                    + QwpVarint.encodedLength(deltaCount);
            int totalSize = HEADER_SIZE + payloadSize;
            long address = Unsafe.malloc(totalSize, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putInt(address + HEADER_OFFSET_MAGIC, MAGIC_MESSAGE);
                Unsafe.getUnsafe().putByte(address + HEADER_OFFSET_VERSION, VERSION_1);
                Unsafe.getUnsafe().putByte(address + HEADER_OFFSET_FLAGS, FLAG_DELTA_SYMBOL_DICT);
                Unsafe.getUnsafe().putShort(address + HEADER_OFFSET_TABLE_COUNT, (short) 0);
                Unsafe.getUnsafe().putInt(address + HEADER_OFFSET_PAYLOAD_LENGTH, payloadSize);

                long pos = address + HEADER_SIZE;
                pos = QwpVarint.encode(pos, deltaStartId);
                QwpVarint.encode(pos, deltaCount);

                QwpMessageCursor cursor = new QwpMessageCursor();
                ObjList<String> connectionDict = new ObjList<>();
                cursor.of(address, totalSize, null, connectionDict);
                Assert.fail("Expected QwpParseException for deltaStartId exceeding int range");
            } catch (QwpParseException e) {
                Assert.assertTrue(e.getMessage().contains("int range"));
            } finally {
                Unsafe.free(address, totalSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testSymbolIndexExceedingIntRange() {
        // A varint encoding 0x1_0000_0005 (4_294_967_301) truncates to 5
        // when cast to int. With 6 dictionary entries the truncated index
        // passes the bounds check and silently returns the wrong symbol.
        // The fix detects the overflow before the cast.
        int size = 200;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = address;

            // no null bitmap
            Unsafe.getUnsafe().putByte(pos, (byte) 0);
            pos++;

            // Dictionary: 6 entries ("s0" .. "s5")
            pos = QwpVarint.encode(pos, 6);
            for (int i = 0; i < 6; i++) {
                byte[] sBytes = ("s" + i).getBytes(StandardCharsets.UTF_8);
                pos = QwpVarint.encode(pos, sBytes.length);
                for (byte b : sBytes) {
                    Unsafe.getUnsafe().putByte(pos++, b);
                }
            }

            // Value: index 0x1_0000_0005 -- truncates to 5 without the fix
            pos = QwpVarint.encode(pos, 0x1_0000_0005L);

            int actualSize = (int) (pos - address);
            QwpSymbolColumnCursor cursor = new QwpSymbolColumnCursor();
            cursor.of(address, actualSize, 1);
            cursor.advanceRow();
            Assert.fail("Expected QwpParseException for varint exceeding int range");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.INVALID_DICTIONARY_INDEX, e.getErrorCode());
            Assert.assertTrue(e.getMessage().contains("int range"));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }
}

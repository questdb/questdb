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

import io.questdb.cutlass.qwp.protocol.*;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.*;

/**
 * Verifies that all column cursors validate data bounds during initialization.
 * Each cursor's of() method receives dataLength and throws QwpParseException
 * when the wire data is truncated or contains out-of-bounds offsets.
 */
public class QwpCursorBoundsCheckTest {

    @Test
    public void testArrayCursorRejectsTruncatedData() {
        int bufferSize = 4;
        long address = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().setMemory(address, bufferSize, (byte) 0);
            Unsafe.getUnsafe().putByte(address, (byte) 1);

            QwpArrayColumnCursor cursor = new QwpArrayColumnCursor();
            cursor.of(address, bufferSize, 1, TYPE_DOUBLE_ARRAY, false);
            Assert.fail("expected QwpParseException for truncated array data");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("truncated"));
        } finally {
            Unsafe.free(address, bufferSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testBooleanCursorRejectsInflatedRowCount() {
        int bufferSize = 8;
        long address = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().setMemory(address, bufferSize, (byte) 0);

            QwpBooleanColumnCursor cursor = new QwpBooleanColumnCursor();
            cursor.of(address, bufferSize, 1000, false);
            Assert.fail("expected QwpParseException for inflated rowCount");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("truncated"));
        } finally {
            Unsafe.free(address, bufferSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecimalCursorRejectsInflatedRowCount() {
        int bufferSize = 16;
        long address = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().setMemory(address, bufferSize, (byte) 0);

            QwpDecimalColumnCursor cursor = new QwpDecimalColumnCursor();
            cursor.of(address, bufferSize, 100, TYPE_DECIMAL64, false);
            Assert.fail("expected QwpParseException for inflated rowCount");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("truncated"));
        } finally {
            Unsafe.free(address, bufferSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testFixedWidthCursorRejectsInflatedRowCount() {
        int bufferSize = 32;
        long address = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().setMemory(address, bufferSize, (byte) 0);

            QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
            cursor.of(address, bufferSize, 1000, TYPE_LONG, false);
            Assert.fail("expected QwpParseException for inflated rowCount");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("truncated"));
        } finally {
            Unsafe.free(address, bufferSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testStringCursorRejectsAttackerControlledOffset() throws QwpParseException {
        // 1 non-null string row: offset array = 8 bytes, string data = 5 bytes
        int legitimateSize = 13;
        int bufferSize = 256;
        long address = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().setMemory(address, bufferSize, (byte) 0);
            Unsafe.getUnsafe().putInt(address, 0);
            Unsafe.getUnsafe().putInt(address + 4, 5);

            QwpStringColumnCursor cursor = new QwpStringColumnCursor();
            int consumed = cursor.of(address, legitimateSize, 1, TYPE_STRING, false);
            Assert.assertEquals(13, consumed);

            // Attacker sets offset[1] = 200 — claims 200 bytes of string data
            Unsafe.getUnsafe().putInt(address + 4, 200);

            cursor = new QwpStringColumnCursor();
            try {
                cursor.of(address, legitimateSize, 1, TYPE_STRING, false);
                Assert.fail("expected QwpParseException for out-of-bounds string offset");
            } catch (QwpParseException e) {
                Assert.assertTrue(e.getMessage().contains("truncated"));
            }
        } finally {
            Unsafe.free(address, bufferSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testStringCursorRejectsInflatedRowCount() {
        int bufferSize = 16;
        long address = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().setMemory(address, bufferSize, (byte) 0);

            QwpStringColumnCursor cursor = new QwpStringColumnCursor();
            // rowCount=100 needs (101)*4 = 404 bytes for offset array alone
            cursor.of(address, bufferSize, 100, TYPE_STRING, false);
            Assert.fail("expected QwpParseException for inflated rowCount");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("truncated"));
        } finally {
            Unsafe.free(address, bufferSize, MemoryTag.NATIVE_DEFAULT);
        }
    }
}

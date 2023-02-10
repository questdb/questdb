/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.std;

import org.junit.Test;

import static org.junit.Assert.*;

public class VarIntTest {

    @Test
    public void testEncodeDecode() {
        final VarInt.Value value = new VarInt.Value();

        final long bufLo = Unsafe.malloc(9, MemoryTag.NATIVE_DEFAULT);
        final long bufHi = bufLo + 9;

        assertEquals(bufLo + 1, VarInt.encode(0, bufLo, bufHi));
        assertEquals(0x00, Unsafe.getUnsafe().getByte(bufLo));

        assertEquals(bufLo + 1, VarInt.decode(value, bufLo, bufHi));
        assertEquals(0, value.get());

        assertEquals(bufLo + 1, VarInt.encode(1, bufLo, bufHi));
        assertEquals(0x01, Unsafe.getUnsafe().getByte(bufLo));

        assertEquals(bufLo + 1, VarInt.decode(value, bufLo, bufHi));
        assertEquals(1, value.get());

        assertEquals(bufLo + 1, VarInt.encode(127, bufLo, bufHi));
        assertEquals(0x7f, Unsafe.getUnsafe().getByte(bufLo));

        assertEquals(bufLo + 1, VarInt.decode(value, bufLo, bufHi));
        assertEquals(127, value.get());

        assertEquals(bufLo + 2, VarInt.encode(128, bufLo, bufHi));
        assertEquals(-256 + 0x80, Unsafe.getUnsafe().getByte(bufLo));
        assertEquals(0x01, Unsafe.getUnsafe().getByte(bufLo + 1));

        assertEquals(bufLo + 2, VarInt.decode(value, bufLo, bufHi));
        assertEquals(128, value.get());

        assertEquals(bufLo + 2, VarInt.encode(255, bufLo, bufHi));
        assertEquals(-256 + 0xff, Unsafe.getUnsafe().getByte(bufLo));
        assertEquals(0x01, Unsafe.getUnsafe().getByte(bufLo + 1));

        assertEquals(bufLo + 2, VarInt.decode(value, bufLo, bufHi));
        assertEquals(255, value.get());

        assertEquals(bufLo + 2, VarInt.encode(256, bufLo, bufHi));
        assertEquals(-256 + 0x80, Unsafe.getUnsafe().getByte(bufLo));
        assertEquals(0x02, Unsafe.getUnsafe().getByte(bufLo + 1));

        assertEquals(bufLo + 2, VarInt.decode(value, bufLo, bufHi));
        assertEquals(256, value.get());

        assertEquals(bufLo + 2, VarInt.encode(1000, bufLo, bufHi));
        assertEquals(-256 + 0xe8, Unsafe.getUnsafe().getByte(bufLo));
        assertEquals(0x07, Unsafe.getUnsafe().getByte(bufLo + 1));

        assertEquals(bufLo + 2, VarInt.decode(value, bufLo, bufHi));
        assertEquals(1000, value.get());

        assertEquals(bufLo + 3, VarInt.encode(60000, bufLo, bufHi));
        assertEquals(-256 + 0xe0, Unsafe.getUnsafe().getByte(bufLo));
        assertEquals(-256 + 0xd4, Unsafe.getUnsafe().getByte(bufLo + 1));
        assertEquals(0x03, Unsafe.getUnsafe().getByte(bufLo + 2));

        assertEquals(bufLo + 3, VarInt.decode(value, bufLo, bufHi));
        assertEquals(60000, value.get());

        assertEquals(bufLo + 9, VarInt.encode(Long.MAX_VALUE, bufLo, bufHi));
        assertEquals(-256 + 0xff, Unsafe.getUnsafe().getByte(bufLo));
        assertEquals(-256 + 0xff, Unsafe.getUnsafe().getByte(bufLo + 1));
        assertEquals(-256 + 0xff, Unsafe.getUnsafe().getByte(bufLo + 2));
        assertEquals(-256 + 0xff, Unsafe.getUnsafe().getByte(bufLo + 3));
        assertEquals(-256 + 0xff, Unsafe.getUnsafe().getByte(bufLo + 4));
        assertEquals(-256 + 0xff, Unsafe.getUnsafe().getByte(bufLo + 5));
        assertEquals(-256 + 0xff, Unsafe.getUnsafe().getByte(bufLo + 6));
        assertEquals(-256 + 0xff, Unsafe.getUnsafe().getByte(bufLo + 7));
        assertEquals(0x7f, Unsafe.getUnsafe().getByte(bufLo + 8));

        assertEquals(bufLo + 9, VarInt.decode(value, bufLo, bufHi));
        assertEquals(Long.MAX_VALUE, value.get());
    }
}

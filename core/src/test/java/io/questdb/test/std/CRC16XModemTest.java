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

package io.questdb.test.std;

import io.questdb.std.CRC16XModem;
import io.questdb.std.DirectIntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.GcUtf8String;
import org.junit.Assert;
import org.junit.Test;

public class CRC16XModemTest {
    @Test
    public void test123456789() {
        GcUtf8String buf = new GcUtf8String("123456789");
        final short crc = CRC16XModem.finalize(
                CRC16XModem.update(CRC16XModem.init(), buf.ptr(), buf.size()));
        Assert.assertEquals((short) 0x31c3, crc);
    }

    @Test
    public void testEmpty() {
        final short crc = CRC16XModem.update(CRC16XModem.init(), 0, 0);
        Assert.assertEquals(0, crc);
    }

    @Test
    public void testHello() {
        GcUtf8String buf = new GcUtf8String("hello");
        final short crc = CRC16XModem.finalize(
                CRC16XModem.update(CRC16XModem.init(), buf.ptr(), buf.size()));
        Assert.assertEquals((short) 50018, crc);
    }

    @Test
    public void testInt() {
        short expected;
        try (DirectIntList ints = new DirectIntList(10, MemoryTag.NATIVE_DEFAULT)) {
            ints.add(100);
            ints.add(200);
            ints.add(300);
            expected = CRC16XModem.finalize(
                    CRC16XModem.update(CRC16XModem.init(), ints.asSlice().ptr(), ints.asSlice().size())
            );
        }
        Assert.assertEquals((short) 58611, expected);

        short actual = CRC16XModem.init();
        actual = CRC16XModem.update(actual, 100);
        actual = CRC16XModem.update(actual, 200);
        actual = CRC16XModem.update(actual, 300);
        actual = CRC16XModem.finalize(actual);

        Assert.assertEquals(expected, actual);
    }
}

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

package io.questdb.test.std;

import io.questdb.std.SwarUtils;
import org.junit.Assert;
import org.junit.Test;

public class SwarUtilsTest {

    @Test
    public void testBroadcast() {
        Assert.assertEquals(0, SwarUtils.broadcast((byte) 0));
        Assert.assertEquals(0x0101010101010101L, SwarUtils.broadcast((byte) 1));
        Assert.assertEquals(0x0202020202020202L, SwarUtils.broadcast((byte) 2));
        Assert.assertEquals(0x2a2a2a2a2a2a2a2aL, SwarUtils.broadcast((byte) 42));
        Assert.assertEquals(0x7f7f7f7f7f7f7f7fL, SwarUtils.broadcast(Byte.MAX_VALUE));
        Assert.assertEquals(0xffffffffffffffffL, SwarUtils.broadcast((byte) -1));
        Assert.assertEquals(0xfefefefefefefefeL, SwarUtils.broadcast((byte) -2));
        Assert.assertEquals(0x8080808080808080L, SwarUtils.broadcast(Byte.MIN_VALUE));
    }

    @Test
    public void testIndexOfFirstMarkedByte() {
        Assert.assertEquals(8, SwarUtils.indexOfFirstMarkedByte(0L));
        Assert.assertEquals(0, SwarUtils.indexOfFirstMarkedByte(0x8080808080808080L));
        Assert.assertEquals(0, SwarUtils.indexOfFirstMarkedByte(0x0000000000000080L));
        Assert.assertEquals(1, SwarUtils.indexOfFirstMarkedByte(0x0000000000008000L));
        Assert.assertEquals(2, SwarUtils.indexOfFirstMarkedByte(0x0000000000800000L));
        Assert.assertEquals(3, SwarUtils.indexOfFirstMarkedByte(0x0000000080000000L));
        Assert.assertEquals(4, SwarUtils.indexOfFirstMarkedByte(0x0000008000000000L));
        Assert.assertEquals(5, SwarUtils.indexOfFirstMarkedByte(0x0000800000000000L));
        Assert.assertEquals(6, SwarUtils.indexOfFirstMarkedByte(0x0080000000000000L));
        Assert.assertEquals(7, SwarUtils.indexOfFirstMarkedByte(0x8000000000000000L));
    }

    @Test
    public void testMarkZeroBytes() {
        Assert.assertEquals(0x0L, SwarUtils.markZeroBytes(-1L));
        Assert.assertEquals(0x0L, SwarUtils.markZeroBytes(Long.MAX_VALUE));
        Assert.assertEquals(0x8080808080808080L, SwarUtils.markZeroBytes(0x0000));
        Assert.assertEquals(0x8080808080808080L, SwarUtils.markZeroBytes(0L));
        Assert.assertEquals(0x8080808080808000L, SwarUtils.markZeroBytes(1L));
        Assert.assertEquals(0x8080808080800080L, SwarUtils.markZeroBytes(1L << 9));
        Assert.assertEquals(0x8080808080008080L, SwarUtils.markZeroBytes(1L << 17));
        Assert.assertEquals(0x8080808000808080L, SwarUtils.markZeroBytes(1L << 25));
        Assert.assertEquals(0x8080800080808080L, SwarUtils.markZeroBytes(1L << 33));
        Assert.assertEquals(0x8080008080808080L, SwarUtils.markZeroBytes(1L << 41));
        Assert.assertEquals(0x8000808080808080L, SwarUtils.markZeroBytes(1L << 49));
        Assert.assertEquals(0x0080808080808080L, SwarUtils.markZeroBytes(1L << 57));
        Assert.assertEquals(0x0080808080808080L, SwarUtils.markZeroBytes(Long.MIN_VALUE));
        // false positive:
        Assert.assertEquals(0x8080808080808080L, SwarUtils.markZeroBytes(0x0100));
    }
}

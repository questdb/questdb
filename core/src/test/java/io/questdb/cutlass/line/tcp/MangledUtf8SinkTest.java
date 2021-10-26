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

package io.questdb.cutlass.line.tcp;

import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class MangledUtf8SinkTest {
    // Reuse member vars to test there are no side effects from test to test
    private StringSink tempSink = new StringSink();
    private MangledUtf8Sink testSink = new MangledUtf8Sink(tempSink);

    @Test
    public void testMangledStringEqualsToDirectCharSequenceAscii() {
        testEquals("ascii string ok");
    }

    @Test
    public void testMangledStringEqualsToDirectCharSequenceUtf8() {
        testEquals("зачёт, незачёт");
    }

    @Test
    public void testMangledStringEqualsToDirectCharSequenceUtf8With3Bytes() {
        testEquals("लаблअца/लаблअца2");
    }

    @Test
    public void testUnsupported() {
        Assert.assertThrows(UnsupportedOperationException.class, () -> testSink.put(new char[]{'1'}, 0, 1));
    }

    private void testEquals(String testVal) {
        byte[] utf8 = testVal.getBytes(StandardCharsets.UTF_8);

        int bufSize = utf8.length;
        long buffer = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < bufSize; i++) {
            Unsafe.getUnsafe().putByte(buffer + i, utf8[i]);
        }

        DirectByteCharSequence directByteCharSequence = new DirectByteCharSequence();
        directByteCharSequence.of(buffer, buffer + bufSize);
        Assert.assertEquals(testVal, directByteCharSequence.toString());

        CharSequence result = testSink.encodeMangledUtf8(testVal);
        TestUtils.assertEquals(directByteCharSequence, result);
        Assert.assertEquals(directByteCharSequence.hashCode(), result.hashCode());

        // Same after materializing to a string
        String stringResult = result.toString();
        Assert.assertEquals(directByteCharSequence, result);
        Assert.assertEquals(directByteCharSequence.hashCode(), stringResult.hashCode());
    }

}

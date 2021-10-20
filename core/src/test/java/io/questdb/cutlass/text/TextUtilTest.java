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

package io.questdb.cutlass.text;

import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class TextUtilTest {

    @Test
    public void testQuotedTextParsing() throws Utf8Exception {
        StringSink query = new StringSink();

        String text = "select count(*) from \"file.csv\" abcd";
        copyToSinkWithTextUtil(query, text, false);

        Assert.assertEquals(text, query.toString());
    }

    @Test
    public void testDoubleQuotedTextBySingleQuoteParsing() throws Utf8Exception {
        StringSink query = new StringSink();

        String text = "select count(*) from \"\"file.csv\"\" abcd";
        copyToSinkWithTextUtil(query, text, false);

        Assert.assertEquals(text, query.toString());
    }

    @Test
    public void testDoubleQuotedTextParsing() throws Utf8Exception {
        StringSink query = new StringSink();

        String text = "select count(*) from \"\"file.csv\"\" abcd";
        copyToSinkWithTextUtil(query, text, true);

        Assert.assertEquals(text.replace("\"\"", "\""), query.toString());
    }

    private void copyToSinkWithTextUtil(StringSink query, String text, boolean doubleQuoteParse) throws Utf8Exception {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        long ptr = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < bytes.length; i++) {
            Unsafe.getUnsafe().putByte(ptr + i, bytes[i]);
        }

        if (doubleQuoteParse) {
            TextUtil.utf8DecodeEscConsecutiveQuotes(ptr, ptr + bytes.length, query);
        } else {
            TextUtil.utf8Decode(ptr, ptr + bytes.length, query);
        }
        Unsafe.free(ptr, bytes.length, MemoryTag.NATIVE_DEFAULT);
    }
}

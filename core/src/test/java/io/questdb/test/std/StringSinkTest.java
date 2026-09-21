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

package io.questdb.test.std;

import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class StringSinkTest {

    @Test
    public void testIndexOf() {
        StringSink ss = new StringSink();
        StringBuilder sb = new StringBuilder();

        Assert.assertEquals(sb.indexOf("abc"), ss.indexOf("abc"));

        String str = "foo bar baz foo";
        ss.put(str);
        sb.append(str);

        Assert.assertEquals(sb.indexOf("foo"), ss.indexOf("foo"));
        Assert.assertEquals(sb.indexOf("bar"), ss.indexOf("bar"));
        Assert.assertEquals(sb.indexOf("baz"), ss.indexOf("baz"));
        Assert.assertEquals(sb.indexOf("abc"), ss.indexOf("abc"));

        Assert.assertEquals(sb.lastIndexOf("foo"), ss.lastIndexOf("foo"));
        Assert.assertEquals(sb.lastIndexOf("bar"), ss.lastIndexOf("bar"));
        Assert.assertEquals(sb.lastIndexOf("baz"), ss.lastIndexOf("baz"));
        Assert.assertEquals(sb.lastIndexOf("abc"), ss.lastIndexOf("abc"));

        for (int i = 0; i < sb.length(); i++) {
            Assert.assertEquals("index: " + i, sb.indexOf("foo", i), ss.indexOf("foo", i));
            Assert.assertEquals("index: " + i, sb.indexOf("bar", i), ss.indexOf("bar", i));
            Assert.assertEquals("index: " + i, sb.indexOf("baz", i), ss.indexOf("baz", i));
            Assert.assertEquals("index: " + i, sb.indexOf("abc", i), ss.indexOf("abc", i));

            Assert.assertEquals("index: " + i, sb.lastIndexOf("foo", i), ss.lastIndexOf("foo", i));
            Assert.assertEquals("index: " + i, sb.lastIndexOf("bar", i), ss.lastIndexOf("bar", i));
            Assert.assertEquals("index: " + i, sb.lastIndexOf("baz", i), ss.lastIndexOf("baz", i));
            Assert.assertEquals("index: " + i, sb.lastIndexOf("abc", i), ss.lastIndexOf("abc", i));
        }
    }

    @Test
    public void testPutStringSinkCopiesSource() {
        final StringSink source = new StringSink(0);
        source.put("ā中\uD83D\uDE03");
        final StringSink sink = new StringSink(1);
        sink.put("prefix:");

        Assert.assertSame(sink, sink.putStringSink(source));
        TestUtils.assertEquals("prefix:ā中\uD83D\uDE03", sink);
        source.setCharAt(0, 'x');
        source.clear();
        source.put("replacement");
        TestUtils.assertEquals("prefix:ā中\uD83D\uDE03", sink);
        sink.setCharAt(7, 'y');
        TestUtils.assertEquals("replacement", source);
    }

    @Test
    public void testPutStringSinkNullAndEmpty() {
        final StringSink sink = new StringSink(0);
        final StringSink empty = new StringSink(0);
        Assert.assertSame(sink, sink.putStringSink(null));
        Assert.assertSame(sink, sink.putStringSink(empty));
        TestUtils.assertEquals("", sink);

        sink.put("prefix");
        sink.putStringSink(null);
        sink.putStringSink(empty);
        TestUtils.assertEquals("prefix", sink);
    }

    @Test
    public void testPutStringSinkSelfAppend() {
        for (int capacity : new int[]{3, 32}) {
            final StringSink sink = new StringSink(capacity);
            sink.put("abc");
            Assert.assertSame(sink, sink.putStringSink(sink));
            TestUtils.assertEquals("abcabc", sink);
        }
    }

    @Test
    public void testPutStringSinkSubclassUsesCharSequence() {
        final StringSink source = new StringSink() {
            @Override
            public char charAt(int index) {
                return Character.toUpperCase(super.charAt(index));
            }

            @Override
            public int length() {
                return super.length() - 1;
            }
        };
        source.put("abc!");
        final StringSink sink = new StringSink();
        sink.put("prefix:");
        sink.putStringSink(source);
        TestUtils.assertEquals("prefix:ABC", sink);
        Assert.assertEquals("abc!", source.toString());
    }

    @Test
    public void testPutUtf8Sequence() {
        StringSink utf16Sink = new StringSink();

        Utf8StringSink utf8Sink = new Utf8StringSink();
        utf8Sink.put("добре дошли у дома");

        utf16Sink.put(utf8Sink);
        TestUtils.assertEquals("добре дошли у дома", utf16Sink);

        utf16Sink.clear();
        utf16Sink.put(utf8Sink, 0, "добре дошли".getBytes(StandardCharsets.UTF_8).length);
        TestUtils.assertEquals("добре дошли", utf16Sink);
    }

    @Test
    public void testTrimTo() {
        StringSink ss = new StringSink();
        ss.put("1234567890");
        ss.trimTo(5);
        TestUtils.assertEquals("12345", ss);
    }

    @Test
    public void testUnprintable() {
        StringSink ss = new StringSink();
        ss.putAsPrintable("āabcdሴdef\u0012");

        TestUtils.assertEquals("āabcdሴdef\\u0012", ss.toString());
    }

    @Test
    public void testUnprintableNewLine() {
        StringSink ss = new StringSink();
        ss.putAsPrintable("\nasd+-~f\r\0 д");
        TestUtils.assertEquals("\\u000aasd+-~f\\u000d\\u0000 д", ss.toString());
    }
}

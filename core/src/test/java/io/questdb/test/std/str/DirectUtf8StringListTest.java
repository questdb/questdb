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

package io.questdb.test.std.str;

import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8StringList;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

public class DirectUtf8StringListTest extends AbstractTest {

    @Test
    public void testAddAndGetElements() {
        try (DirectUtf8StringList list = new DirectUtf8StringList(64, 4)) {
            Assert.assertEquals(0, list.size());

            list.put(new Utf8String("hello"));
            Assert.assertEquals(1, list.size());
            Assert.assertEquals("hello", list.getQuick(0).toString());

            list.put(new Utf8String("world"));
            Assert.assertEquals(2, list.size());
            Assert.assertEquals("hello", list.getQuick(0).toString());
            Assert.assertEquals("world", list.getQuick(1).toString());

            list.put(new Utf8String("test"));
            Assert.assertEquals(3, list.size());
            Assert.assertEquals("hello", list.getQuick(0).toString());
            Assert.assertEquals("world", list.getQuick(1).toString());
            Assert.assertEquals("test", list.getQuick(2).toString());
        }
    }

    @Test
    public void testAsciiFlagsAfterClear() {
        try (DirectUtf8StringList list = new DirectUtf8StringList(256, 8)) {
            list.put(new Utf8String("Привет"));
            Assert.assertFalse(list.getQuick(0).isAscii());
            list.clear();
            list.put(new Utf8String("hello"));
            Assert.assertTrue(list.getQuick(0).isAscii());
        }
    }

    @Test
    public void testAsciiFlagsManualConstruction() {
        try (DirectUtf8StringList list = new DirectUtf8StringList(64, 4)) {
            list.putAscii('h');
            list.putAscii('e');
            list.putAscii('l');
            list.putAscii('l');
            list.putAscii('o');
            list.setElem();
            Assert.assertTrue(list.getQuick(0).isAscii());
            list.put((byte) 0xD0);
            list.put((byte) 0x9F);
            list.setElem();
            Assert.assertFalse(list.getQuick(1).isAscii());
        }
    }

    @Test
    public void testAsciiFlagsMixedStrings() {
        try (DirectUtf8StringList list = new DirectUtf8StringList(256, 8)) {
            list.put(new Utf8String("hello"));
            list.put(new Utf8String("Привет"));
            list.put(new Utf8String("world"));
            list.put(new Utf8String("你好"));
            list.put(new Utf8String("test"));

            Assert.assertTrue(list.getQuick(0).isAscii());
            Assert.assertFalse(list.getQuick(1).isAscii());
            Assert.assertTrue(list.getQuick(2).isAscii());
            Assert.assertFalse(list.getQuick(3).isAscii());
            Assert.assertTrue(list.getQuick(4).isAscii());
        }
    }

    @Test
    public void testAsciiFlagsWithAsciiStrings() {
        try (DirectUtf8StringList list = new DirectUtf8StringList(64, 4)) {
            list.put(new Utf8String("hello"));
            list.put(new Utf8String("world"));
            list.put(new Utf8String("test123"));

            Assert.assertTrue(list.getQuick(0).isAscii());
            Assert.assertTrue(list.getQuick(1).isAscii());
            Assert.assertTrue(list.getQuick(2).isAscii());
        }
    }

    @Test
    public void testAsciiFlagsWithNonAsciiStrings() {
        try (DirectUtf8StringList list = new DirectUtf8StringList(256, 8)) {
            list.put(new Utf8String("Привет"));
            list.put(new Utf8String("你好"));
            list.put(new Utf8String("🌍"));

            Assert.assertFalse(list.getQuick(0).isAscii());
            Assert.assertFalse(list.getQuick(1).isAscii());
            Assert.assertFalse(list.getQuick(2).isAscii());
        }
    }

    @Test
    public void testClear() {
        try (DirectUtf8StringList list = new DirectUtf8StringList(64, 4)) {
            list.put(new Utf8String("hello"));
            list.put(new Utf8String("world"));
            Assert.assertEquals(2, list.size());

            list.clear();
            Assert.assertEquals(0, list.size());
            list.put(new Utf8String("new"));
            Assert.assertEquals(1, list.size());
            Assert.assertEquals("new", list.getQuick(0).toString());
        }
    }

    @Test
    public void testEmptyList() {
        try (DirectUtf8StringList list = new DirectUtf8StringList(64, 4)) {
            Assert.assertEquals(0, list.size());
        }
    }

    @Test
    public void testEmptyStrings() {
        try (DirectUtf8StringList list = new DirectUtf8StringList(64, 4)) {
            list.put(new Utf8String(""));
            Assert.assertEquals(1, list.size());
            Assert.assertEquals("", list.getQuick(0).toString());
            Assert.assertEquals(0, list.getQuick(0).size());

            list.put(new Utf8String("hello"));
            list.put(new Utf8String(""));
            Assert.assertEquals(3, list.size());
            Assert.assertEquals("", list.getQuick(0).toString());
            Assert.assertEquals("hello", list.getQuick(1).toString());
            Assert.assertEquals("", list.getQuick(2).toString());
        }
    }

    @Test
    public void testGetQuickReusesView() {
        try (DirectUtf8StringList list = new DirectUtf8StringList(64, 4)) {
            list.put(new Utf8String("first"));
            list.put(new Utf8String("second"));

            DirectUtf8Sequence view1 = list.getQuick(0);
            Assert.assertEquals("first", view1.toString());

            DirectUtf8Sequence view2 = list.getQuick(1);
            Assert.assertEquals("second", view2.toString());

            // view1 and view2 are the same object (reused)
            Assert.assertSame(view1, view2);

            // view1 now points to "second" because getQuick(1) was called
            Assert.assertEquals("second", view1.toString());
        }
    }

    @Test
    public void testGrowCapacity() {
        try (DirectUtf8StringList list = new DirectUtf8StringList(4, 2)) {
            for (int i = 0; i < 100; i++) {
                list.put(new Utf8String("element" + i));
            }
            Assert.assertEquals(100, list.size());

            for (int i = 0; i < 100; i++) {
                Assert.assertEquals("element" + i, list.getQuick(i).toString());
            }
        }
    }

    @Test
    public void testManualElementConstruction() {
        try (DirectUtf8StringList list = new DirectUtf8StringList(64, 4)) {
            list.putAscii('h');
            list.putAscii('e');
            list.putAscii('l');
            list.putAscii('l');
            list.putAscii('o');
            list.setElem();

            Assert.assertEquals(1, list.size());
            Assert.assertEquals("hello", list.getQuick(0).toString());

            // Add another element
            list.putAscii('w');
            list.putAscii('o');
            list.putAscii('r');
            list.putAscii('l');
            list.putAscii('d');
            list.setElem();

            Assert.assertEquals(2, list.size());
            Assert.assertEquals("hello", list.getQuick(0).toString());
            Assert.assertEquals("world", list.getQuick(1).toString());
        }
    }

    @Test
    public void testReopenAfterClose() {
        DirectUtf8StringList list = new DirectUtf8StringList(64, 4);
        list.put(new Utf8String("hello"));
        Assert.assertEquals(1, list.size());

        list.close();

        list.reopen();
        list.put(new Utf8String("world"));
        Assert.assertEquals(1, list.size());
        Assert.assertEquals("world", list.getQuick(0).toString());

        list.close();
    }

    @Test
    public void testUtf8Encoding() {
        try (DirectUtf8StringList list = new DirectUtf8StringList(256, 8)) {
            list.put(new Utf8String("Hello")); // ASCII
            list.put(new Utf8String("Привет")); // Russian
            list.put(new Utf8String("你好")); // Chinese
            list.put(new Utf8String("こんにちは")); // Japanese
            list.put(new Utf8String("🌍🌎🌏")); // Emojis (4-byte UTF-8)

            Assert.assertEquals(5, list.size());
            Assert.assertEquals("Hello", list.getQuick(0).toString());
            Assert.assertEquals("Привет", list.getQuick(1).toString());
            Assert.assertEquals("你好", list.getQuick(2).toString());
            Assert.assertEquals("こんにちは", list.getQuick(3).toString());
            Assert.assertEquals("🌍🌎🌏", list.getQuick(4).toString());
        }
    }

    @Test
    public void testUtf8StringSinkInput() {
        try (DirectUtf8StringList list = new DirectUtf8StringList(64, 4)) {
            Utf8StringSink sink = new Utf8StringSink();
            sink.put("hello world");

            list.put(sink);
            Assert.assertEquals(1, list.size());
            Assert.assertEquals("hello world", list.getQuick(0).toString());
        }
    }

    @Test
    public void testViewSize() {
        try (DirectUtf8StringList list = new DirectUtf8StringList(64, 4)) {
            list.put(new Utf8String("ab"));
            list.put(new Utf8String("cdef"));
            list.put(new Utf8String("g"));

            Assert.assertEquals(2, list.getQuick(0).size());
            Assert.assertEquals(4, list.getQuick(1).size());
            Assert.assertEquals(1, list.getQuick(2).size());
        }
    }
}

/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo;

import com.questdb.std.Numbers;
import com.questdb.std.NumericException;
import com.questdb.std.ObjList;
import com.questdb.std.Rnd;
import com.questdb.std.str.StringSink;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class QMapTest extends AbstractCairoTest {

    @Test
    public void testAppendExisting() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();
            int N = 10;
            try (QMap map = new QMap(1024 * 1024, 1, 1, N / 2, 0.9)) {
                ObjList<String> keys = new ObjList<>();
                for (int i = 0; i < N; i++) {
                    CharSequence s = rnd.nextChars(11);
                    keys.add(s.toString());
                    QMap.Key key = map.keyBuilder().putStr(s);
                    QMap.Value value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putLong(i + 1);
                }
                Assert.assertEquals(N, map.size());

                for (int i = 0, n = keys.size(); i < n; i++) {
                    QMap.Key key = map.keyBuilder().putStr(keys.getQuick(i));
                    QMap.Value value = key.createValue();
                    Assert.assertFalse(value.isNew());
                    Assert.assertEquals(i + 1, value.getLong(0));
                }
            }
        });
    }

    @Test
    public void testAppendUnique() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();
            int N = 10000000;
            try (QMap map = new QMap(16 * 1024 * 1024, 1, 1, N / 2, 0.9)) {
                for (int i = 0; i < N; i++) {
                    CharSequence s = rnd.nextChars(11);
                    QMap.Key key = map.keyBuilder().putStr(s);
                    QMap.Value value = key.createValue();
                    value.putLong(i + 1);
                }
                Assert.assertEquals(N, map.size());

                long expectedAppendOffset = map.getAppendOffset();

                rnd.reset();
                for (int i = 0; i < N; i++) {
                    CharSequence s = rnd.nextChars(11);
                    QMap.Key key = map.keyBuilder().putStr(s);
                    QMap.Value value = key.findValue();
                    Assert.assertNotNull(value);
                    Assert.assertEquals(i + 1, value.getLong(0));
                }
                Assert.assertEquals(N, map.size());
                Assert.assertEquals(expectedAppendOffset, map.getAppendOffset());
            }
        });
    }

    @Test
    public void testKeyLookup() {
        // This hash function will use first three characters as hash code
        // we need decent spread of hash codes making single character not enough
        class MockHash implements QMap.HashFunction {
            @Override
            public long hash(VirtualMemory mem, long offset, long size) {
                // string begins after 8-byte cell for key value
                CharSequence cs = mem.getStr(offset + 8);
                try {
                    return Numbers.parseLong(cs, 0, 3);
                } catch (NumericException e) {
                    // this must not happen unless test doesn't comply
                    // with key format
                    throw new RuntimeException();
                }
            }
        }

        // tweak capacity in such a way that we only have one spare slot before resize is needed
        // this way algo that shuffles "foreign" slots away should face problems
        double loadFactor = 0.9;
        try (QMap map = new QMap(4 * 1024 * 1024, 1, 1, 12, loadFactor, new MockHash())) {
            QMap.Key key;
            QMap.Value value;

            key = map.keyBuilder().putStr("000-ABCDE");
            value = key.createValue();
            Assert.assertTrue(value.isNew());
            value.putDouble(12.5);

            // difference in last character
            key = map.keyBuilder().putStr("000-ABCDG");
            value = key.createValue();
            Assert.assertTrue(value.isNew());
            value.putDouble(11.5);

            // different hash code
            key = map.keyBuilder().putStr("100-ABCDE");
            value = key.createValue();
            Assert.assertTrue(value.isNew());
            value.putDouble(10.5);

            // check that we cannot get value with traight up non-existing hash code
            Assert.assertNull(map.keyBuilder().putStr("200-ABCDE").findValue());

            // check that we don't get value when we go down the linked list
            // 004 will produce the same hashcode as 100
            Assert.assertNull(map.keyBuilder().putStr("004-ABCDE").findValue());

            // check that we don't get value when we go down the linked list
            // this will produce 001 hashcode, which should find that slot 1 is occupied by indirect hit
            Assert.assertNull(map.keyBuilder().putStr("017-ABCDE").findValue());
        }
    }

    @Test
    public void testUnableToFindFreeSlot() {

        // test what happens when map runs out of free slots while trying to
        // reshuffle "foreign" entries out of the way

        Rnd rnd = new Rnd();

        // these have to be power of two for on the limit testing
        // QMap will round capacity to next highest power of two!

        int N = 256;
        int M = 32;


        // This function must know about entry structure.
        // To make hash consistent we will assume that first character of
        // string is always a number and this number will be hash code of string.
        class MockHash implements QMap.HashFunction {
            @Override
            public long hash(VirtualMemory mem, long offset, long size) {
                // we have singe key field, which is string
                // the offset of string is 8 bytes for key cell + 4 bytes for string length, total is 12
                char c = mem.getChar(offset + 12);
                return c - '0';
            }
        }

        StringSink sink = new StringSink();
        // tweak capacity in such a way that we only have one spare slot before resize is needed
        // this way algo that shuffles "foreign" slots away should face problems
        double loadFactor = 0.9999999;
        try (QMap map = new QMap(4 * 1024 * 1024, 1, 1, (long) (N * loadFactor), loadFactor, new MockHash())) {

            // assert that key capacity is what we expect, otherwise this test would be useless
            Assert.assertEquals(N, map.getActualCapacity());

            long target = map.getKeyCapacity();

            sink.clear();
            sink.put('0');
            for (long i = 0; i < target - 1; i++) {
                // keep the first character
                sink.clear(1);
                rnd.nextChars(sink, 5);
                QMap.Key key = map.keyBuilder().putStr(sink);
                QMap.Value value = key.createValue();
                Assert.assertTrue(value.isNew());
                value.putLong(i + 1);
            }

            sink.clear();
            sink.put('1');

            for (long i = target - 1; i < M + N; i++) {
                // keep the first character
                sink.clear(1);
                rnd.nextChars(sink, 5);
                QMap.Key key = map.keyBuilder().putStr(sink);
                QMap.Value value = key.createValue();
                Assert.assertTrue(value.isNew());
                value.putLong(i + 1);
            }

            // assert result

            rnd.reset();

            sink.clear();
            sink.put('0');
            for (int i = 0; i < target - 1; i++) {
                // keep the first character
                sink.clear(1);
                rnd.nextChars(sink, 5);
                QMap.Key key = map.keyBuilder().putStr(sink);
                QMap.Value value = key.createValue();
                Assert.assertFalse(value.isNew());
                Assert.assertEquals(i + 1, value.getLong(0));
            }

            sink.clear();
            sink.put('1');

            for (long i = target - 1; i < M + N; i++) {
                // keep the first character
                sink.clear(1);
                rnd.nextChars(sink, 5);
                QMap.Key key = map.keyBuilder().putStr(sink);
                QMap.Value value = key.createValue();
                Assert.assertFalse(value.isNew());
                Assert.assertEquals(i + 1, value.getLong(0));
            }
        }
    }

    @Test
    public void testUnableToFindFreeSlot2() {

        // test that code that deals with key collection is always
        // protected by map capacity check.

        Rnd rnd = new Rnd();

        // these have to be power of two for on the limit testing
        // QMap will round capacity to next highest power of two!

        int N = 256;
        int M = 32;


        // This hash function will use first three characters as hash code
        // we need decent spread of hash codes making single character not enough
        class MockHash implements QMap.HashFunction {
            @Override
            public long hash(VirtualMemory mem, long offset, long size) {
                // string begins after 8-byte cell for key value
                CharSequence cs = mem.getStr(offset + 8);
                try {
                    return Numbers.parseLong(cs, 0, 3);
                } catch (NumericException e) {
                    // this must not happen unless test doesn't comply
                    // with key format
                    throw new RuntimeException();
                }
            }
        }

        StringSink sink = new StringSink();
        // tweak capacity in such a way that we only have one spare slot before resize is needed
        // this way algo that shuffles "foreign" slots away should face problems
        double loadFactor = 0.9999999;
        try (QMap map = new QMap(4 * 1024 * 1024, 1, 1, (long) (N * loadFactor), loadFactor, new MockHash())) {

            // assert that key capacity is what we expect, otherwise this test would be useless
            Assert.assertEquals(N, map.getActualCapacity());

            long target = map.getKeyCapacity();

            sink.clear();
            sink.put("000");
            for (long i = 0; i < target - 2; i++) {
                // keep the first character
                sink.clear(3);
                rnd.nextChars(sink, 5);
                QMap.Key key = map.keyBuilder().putStr(sink);
                QMap.Value value = key.createValue();
                Assert.assertTrue(value.isNew());
                value.putLong(i + 1);
            }

            sink.clear();
            sink.put(target - 2);

            for (long i = target - 1; i < M + N; i++) {
                // keep the first character
                sink.clear(3);
                rnd.nextChars(sink, 5);
                QMap.Key key = map.keyBuilder().putStr(sink);
                QMap.Value value = key.createValue();
                Assert.assertTrue(value.isNew());
                value.putLong(i + 1);
            }

            // assert result

            rnd.reset();

            sink.clear();
            sink.put("000");
            for (int i = 0; i < target - 2; i++) {
                // keep the first character
                sink.clear(3);
                rnd.nextChars(sink, 5);
                QMap.Key key = map.keyBuilder().putStr(sink);
                QMap.Value value = key.createValue();
                Assert.assertFalse(value.isNew());
                Assert.assertEquals(i + 1, value.getLong(0));
            }

            sink.clear();
            sink.put(target - 2);

            for (long i = target - 1; i < M + N; i++) {
                // keep the first character
                sink.clear(3);
                rnd.nextChars(sink, 5);
                QMap.Key key = map.keyBuilder().putStr(sink);
                QMap.Value value = key.createValue();
                Assert.assertFalse(value.isNew());
                Assert.assertEquals(i + 1, value.getLong(0));
            }
        }
    }
}
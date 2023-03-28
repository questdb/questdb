/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.std.*;
import io.questdb.std.str.ByteCharSequence;
import io.questdb.std.str.DirectByteCharSequence;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;

public class ByteCharSequenceObjHashMapTest {

    @Test
    public void testHashMapCompatibility() {
        final Rnd rnd = new Rnd();
        int N = 100_000;
        int memSize = 8 * 1024 * 1024;
        long mem = Unsafe.malloc(memSize, MemoryTag.NATIVE_DEFAULT);
        HashMap<String, Integer> hashMap = new HashMap<>();
        ArrayList<String> list = new ArrayList<>();
        final DirectByteCharSequence dbcs = new DirectByteCharSequence();
        ByteCharSequenceObjHashMap<Integer> ourMap = new ByteCharSequenceObjHashMap<>();
        try {
            // generate random strings and randomly add some of them to the HashMap
            long p = mem;
            for (int i = 0; i < N; i++) {
                String s = rnd.nextString(rnd.nextInt(10));
                list.add(s);
                if (rnd.nextBoolean()) {
                    Object v = hashMap.put(s, i);
                    boolean added = ourMap.put(new ByteCharSequence(s.getBytes(StandardCharsets.UTF_8)), i);

                    // if we can add string to HashMap, we must be able to add it to our map too.
                    // Opposite is true also. Both maps must behave the same
                    if (v == null) {
                        Assert.assertTrue(added);
                    } else {
                        Assert.assertFalse(added);
                    }
                }
                // copy each string to the memory
                int len = s.length();
                Unsafe.getUnsafe().putInt(p, len);
                Chars.asciiStrCpy(s, len, p + 4);
                p += 4 + len;
            }

            Assert.assertEquals(hashMap.size(), ourMap.size());

            // verify contains(), excludes() and get()
            // sequential access to our memory
            p = mem;
            for (int i = 0; i < N; i++) {
                String s = list.get(i);
                int len = s.length();
                p += 4;
                dbcs.of(p, p + len);
                Assert.assertEquals(hashMap.containsKey(s), ourMap.contains(dbcs));
                Assert.assertNotEquals(hashMap.containsKey(s), ourMap.excludes(dbcs));

                Object v = hashMap.get(s);
                Integer k = ourMap.get(dbcs);
                Assert.assertEquals(k, ourMap.get(new ByteCharSequence(s.getBytes(StandardCharsets.UTF_8))));

                if (v == null) {
                    Assert.assertNull(k);
                    Assert.assertTrue(ourMap.keyIndex(dbcs) > -1);
                } else {
                    Assert.assertEquals(v, k);
                    int keyIndex = ourMap.keyIndex(dbcs);
                    Assert.assertTrue(keyIndex < 0);
                    Assert.assertEquals(k, ourMap.valueAt(keyIndex));
                }
                p += len;
            }

            // verify iteration of keys
            for (int i = 0, n = ourMap.size(); i < n; i++) {
                int k = ourMap.valueQuick(i);
                Object v = hashMap.get(ourMap.keys().getQuick(i));
                Assert.assertNotNull(v);
                Assert.assertEquals((int) v, k);
            }

            // verify remove()
            p = mem;
            for (int i = 0; i < N; i++) {
                String s = list.get(i);
                int len = s.length();
                p += 4;
                dbcs.of(p, p + len);
                if (hashMap.containsKey(s)) {
                    if (rnd.nextBoolean()) {
                        Object v = hashMap.remove(s);
                        Assert.assertNotNull(v);
                        ourMap.remove(ByteCharSequence.newInstance(dbcs));
                    }
                } else {
                    Assert.assertEquals(-1, ourMap.remove(ByteCharSequence.newInstance(dbcs)));
                }
                p += len;
            }

            // compare HashMap to our map after random removal
            p = mem;
            for (int i = 0; i < N; i++) {
                String s = list.get(i);
                int len = s.length();
                p += 4;
                dbcs.of(p, p + len);
                Object v = hashMap.get(s);
                if (v != null) {
                    Assert.assertEquals(v, ourMap.get(dbcs));
                } else {
                    Assert.assertNull(ourMap.get(dbcs));
                }
                p += len;
            }
        } finally {
            Unsafe.free(mem, memSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testHashMapUtf8() {
        final int N = 256;
        final int memSize = 2 * N;
        long mem = Unsafe.malloc(memSize, MemoryTag.NATIVE_DEFAULT);
        final DirectByteCharSequence dbcs = new DirectByteCharSequence();
        ByteCharSequenceObjHashMap<Integer> map = new ByteCharSequenceObjHashMap<>();
        try {
            final String utf16Str = "ъ";
            final byte[] utf8Bytes = utf16Str.getBytes(StandardCharsets.UTF_8);
            assert utf8Bytes.length == 2;
            for (int i = 0; i < N; i++) {
                for (int j = 0; j < 2; j++) {
                    Unsafe.getUnsafe().putByte(mem + (long) 2 * i + j, utf8Bytes[j]);
                }
            }

            for (int i = 0; i < N; i++) {
                dbcs.of(mem, mem + (long) 2 * i);
                Assert.assertNull(map.get(dbcs));

                final ByteCharSequence bcs = ByteCharSequence.newInstance(dbcs);
                map.put(ByteCharSequence.newInstance(dbcs), i);
                Assert.assertEquals(i, (int) map.get(dbcs));
                Assert.assertEquals(i, (int) map.get(bcs));
            }

            for (int i = 0; i < N; i++) {
                Assert.assertEquals(N - i, map.size());

                dbcs.of(mem, mem + (long) 2 * i);
                map.remove(ByteCharSequence.newInstance(dbcs));
                Assert.assertEquals(N - i - 1, map.size());
            }
        } finally {
            Unsafe.free(mem, memSize, MemoryTag.NATIVE_DEFAULT);
        }
    }
}

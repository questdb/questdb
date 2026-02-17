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

import io.questdb.std.LowerCaseUtf8SequenceObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;

public class LowerCaseUtf8SequenceObjHashMapTest {

    @Test
    public void testHashMapCompatibility() {
        final Rnd rnd = new Rnd();
        int N = 100_000;
        int memSize = 8 * 1024 * 1024;
        long mem = Unsafe.malloc(memSize, MemoryTag.NATIVE_DEFAULT);
        HashMap<String, Integer> hashMap = new HashMap<>();
        ArrayList<String> list = new ArrayList<>();
        final DirectUtf8String dus = new DirectUtf8String();
        LowerCaseUtf8SequenceObjHashMap<Integer> ourMap = new LowerCaseUtf8SequenceObjHashMap<>();
        try {
            // generate random strings and randomly add some of them to the HashMap
            long p = mem;
            for (int i = 0; i < N; i++) {
                String s = rnd.nextString(rnd.nextInt(10));
                list.add(s);
                if (rnd.nextBoolean()) {
                    Object v = hashMap.put(s, i);
                    boolean added = ourMap.put(new Utf8String(s), i);

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
                Utf8s.strCpyAscii(s, len, p + 4);
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
                dus.of(p, p + len);
                Assert.assertEquals(hashMap.containsKey(s), ourMap.contains(dus));
                Assert.assertNotEquals(hashMap.containsKey(s), ourMap.excludes(dus));

                Object v = hashMap.get(s);
                Integer k = ourMap.get(dus);
                Assert.assertEquals(k, ourMap.get(new Utf8String(s)));

                if (v == null) {
                    Assert.assertNull(k);
                    Assert.assertTrue(ourMap.keyIndex(dus) > -1);
                } else {
                    Assert.assertEquals(v, k);
                    int keyIndex = ourMap.keyIndex(dus);
                    Assert.assertTrue(keyIndex < 0);
                    Assert.assertEquals(k, ourMap.valueAt(keyIndex));
                }
                p += len;
            }

            // verify iteration of keys
            for (int i = 0, n = ourMap.size(); i < n; i++) {
                int k = ourMap.valueQuick(i);
                Object v = hashMap.get(ourMap.keys().getQuick(i).toString());
                Assert.assertNotNull(v);
                Assert.assertEquals((int) v, k);
            }

            // verify remove()
            p = mem;
            for (int i = 0; i < N; i++) {
                String s = list.get(i);
                int len = s.length();
                p += 4;
                dus.of(p, p + len);
                if (hashMap.containsKey(s)) {
                    if (rnd.nextBoolean()) {
                        Object v = hashMap.remove(s);
                        Assert.assertNotNull(v);
                        ourMap.remove(dus);
                    }
                } else {
                    Assert.assertEquals(-1, ourMap.remove(dus));
                }
                p += len;
            }

            // compare HashMap to our map after random removal
            p = mem;
            for (int i = 0; i < N; i++) {
                String s = list.get(i);
                int len = s.length();
                p += 4;
                dus.of(p, p + len);
                Object v = hashMap.get(s);
                if (v != null) {
                    Assert.assertEquals(v, ourMap.get(dus));
                } else {
                    Assert.assertNull(ourMap.get(dus));
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
        final DirectUtf8String dus = new DirectUtf8String();
        LowerCaseUtf8SequenceObjHashMap<Integer> map = new LowerCaseUtf8SequenceObjHashMap<>();
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
                dus.of(mem, mem + (long) 2 * i);
                Assert.assertNull(map.get(dus));

                final Utf8String bcs = Utf8String.newInstance(dus);
                map.put(Utf8String.newInstance(dus), i);
                Assert.assertEquals(i, (int) map.get(dus));
                Assert.assertEquals(i, (int) map.get(bcs));
            }
        } finally {
            Unsafe.free(mem, memSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testLowerCaseKeys() {
        final int N = 256;
        LowerCaseUtf8SequenceObjHashMap<Integer> map = new LowerCaseUtf8SequenceObjHashMap<>();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < N; i++) {
            sb.append("A");
            Utf8String key = new Utf8String(sb.toString());
            map.put(key, i);
        }

        sb.setLength(0);
        for (int i = 0; i < N; i++) {
            sb.append("a");
            Utf8String key = new Utf8String(sb.toString());
            Assert.assertEquals(i, (int) map.get(key));
        }
    }

    @Test
    public void testPutImmutableRetainsKeyIdentity() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (DirectUtf8Sink directUtf8Sink = new DirectUtf8Sink(8)) {
                LowerCaseUtf8SequenceObjHashMap<Integer> map = new LowerCaseUtf8SequenceObjHashMap<>();

                directUtf8Sink.put("foo");
                Utf8String utf8String = new Utf8String("foo");

                Assert.assertTrue(map.putImmutable(directUtf8Sink, 1));
                Assert.assertEquals(1, (int) map.get(utf8String));
                int idx = map.keyIndex(utf8String);

                Assert.assertSame(directUtf8Sink, map.keyAt(idx));
                Assert.assertSame(directUtf8Sink, map.keys().get(0));

                Assert.assertFalse(map.putImmutable(utf8String, 2));
                Assert.assertEquals(2, (int) map.get(directUtf8Sink));

                map.remove(directUtf8Sink);
                Assert.assertEquals(0, map.size());
            }
        });


    }
}

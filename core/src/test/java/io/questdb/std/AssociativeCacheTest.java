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

import io.questdb.metrics.Gauge;
import io.questdb.metrics.GaugeImpl;
import io.questdb.std.str.DirectByteCharSequence;
import org.junit.Assert;
import org.junit.Test;

public class AssociativeCacheTest {

    @Test
    public void testBasic() {
        AssociativeCache<String> cache = new AssociativeCache<>(8, 64);
        cache.put("X", "1");
        cache.put("Y", "2");
        cache.put("Z", "3");
        Assert.assertEquals("1", cache.peek("X"));
        Assert.assertEquals("2", cache.peek("Y"));
        Assert.assertEquals("3", cache.peek("Z"));
        Assert.assertEquals("1", cache.poll("X"));
        Assert.assertEquals("2", cache.poll("Y"));
        Assert.assertEquals("3", cache.poll("Z"));
        Assert.assertNull(cache.poll("X"));
        Assert.assertNull(cache.poll("Y"));
        Assert.assertNull(cache.poll("Z"));
    }

    @Test
    public void testGaugeUpdates() {
        Gauge gauge = new GaugeImpl("foobar");
        AssociativeCache<String> cache = new AssociativeCache<>(8, 64, gauge);
        Assert.assertEquals(0, gauge.getValue());
        for (int i = 0; i < 10; i++) {
            cache.put(Integer.toString(i), Integer.toString(i));
            Assert.assertEquals(i + 1, gauge.getValue());
        }

        cache.poll("0");
        Assert.assertEquals(9, gauge.getValue());
        // Second poll() on the same key should be ignored.
        cache.poll("0");
        Assert.assertEquals(9, gauge.getValue());
        // put() should insert value for key-value pair cleared by poll().
        cache.put("0", "42");
        Assert.assertEquals(10, gauge.getValue());

        cache.clear();
        Assert.assertEquals(0, gauge.getValue());
    }

    @Test
    public void testMinSize() {
        AssociativeCache<String> cache = new AssociativeCache<>(1, 1);
        cache.put("X", "1");
        cache.put("Y", "2");
        cache.put("Z", "3");
        Assert.assertNull(cache.peek("X"));
        Assert.assertNull(cache.peek("Y"));
        Assert.assertEquals("3", cache.peek("Z"));
    }

    @Test
    public void testFull() {
        AssociativeCache<String> cache = new AssociativeCache<>(8, 64);
        CharSequenceHashSet all = new CharSequenceHashSet();
        CharSequenceHashSet reject = new CharSequenceHashSet();
        Rnd rnd = new Rnd();

        for (int i = 0; i < 16 * 64; i++) {
            CharSequence k = rnd.nextString(10);
            all.add(k);
            CharSequence o = cache.put(k, rnd.nextString(10));
            if (o != null) {
                reject.add(o);
            }
        }

        for (int i = 0; i < all.size(); i++) {
            CharSequence k = all.get(i);
            if (cache.peek(k) == null) {
                Assert.assertTrue(reject.contains(k));
            }
        }
        Assert.assertEquals(512, reject.size());
    }

    @Test
    public void testImmutableKeys() {
        final AssociativeCache<String> cache = new AssociativeCache<>(8, 8);
        long mem = Unsafe.malloc(1024, MemoryTag.NATIVE_DEFAULT);
        final DirectByteCharSequence dbcs = new DirectByteCharSequence();

        try {
            Unsafe.getUnsafe().putByte(mem, (byte) 'A');
            Unsafe.getUnsafe().putByte(mem + 1, (byte) 'B');

            cache.put(dbcs.of(mem, mem + 2), "hello1");

            Unsafe.getUnsafe().putByte(mem, (byte) 'C');
            Unsafe.getUnsafe().putByte(mem + 1, (byte) 'D');

            cache.put(dbcs, "hello2");

            Unsafe.getUnsafe().putByte(mem, (byte) 'A');
            Unsafe.getUnsafe().putByte(mem + 1, (byte) 'B');

            Assert.assertEquals("hello1", cache.peek(dbcs));

            Unsafe.getUnsafe().putByte(mem, (byte) 'C');
            Unsafe.getUnsafe().putByte(mem + 1, (byte) 'D');

            Assert.assertEquals("hello2", cache.peek(dbcs));
        } finally {
            Unsafe.free(mem, 1024, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testNoUnnecessaryShift() {
        final AssociativeCache<String> cache = new AssociativeCache<>(8, 8);
        String value = "myval";

        cache.put("x", value);
        Assert.assertEquals(value, cache.poll("x"));
        cache.put("x", value);
    }
}

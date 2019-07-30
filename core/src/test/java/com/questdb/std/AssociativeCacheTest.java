/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.std;

import com.questdb.std.str.DirectByteCharSequence;
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
        long mem = Unsafe.malloc(1024);
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
            Unsafe.free(mem, 1024);
        }
    }
}

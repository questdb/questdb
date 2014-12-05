/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.journal.lang.experimental;

import com.nfsdb.journal.collections.IntHashSet;
import com.nfsdb.journal.utils.Hash;
import com.nfsdb.journal.utils.MemoryBuffer;
import com.nfsdb.journal.utils.Rnd;
import com.nfsdb.journal.utils.Unsafe;
import org.junit.Assert;
import org.junit.Test;

public class HashTest {

    @Test
    public void testStringHash() throws Exception {
        MemoryBufferImpl buf = new MemoryBufferImpl(30);
        Rnd rnd = new Rnd();
        IntHashSet hashes = new IntHashSet(100000);


        for (int i = 0; i < 100000; i++) {
            buf.put(rnd.nextString(15));
            hashes.add(Hash.hashXX(buf, rnd.nextInt()));
        }

        Assert.assertTrue("Hash function distribution dropped", hashes.size() > 99990);

    }

    private static class MemoryBufferImpl implements MemoryBuffer {
        private final long address;
        private final int len;

        public MemoryBufferImpl(int len) {
            this.len = len;
            this.address = Unsafe.getUnsafe().allocateMemory(len);
        }

        @Override
        public int length() {
            return len;
        }

        @Override
        public int getInt(int p) {
            return Unsafe.getUnsafe().getInt(address + p);

        }

        @Override
        public byte getByte(int p) {
            return Unsafe.getUnsafe().getByte(address + p);
        }

        public void put(CharSequence value) {
            for (int i = 0; i < value.length(); i++) {
                Unsafe.getUnsafe().putChar(address + i * 2, value.charAt(i));
            }
        }
    }
}

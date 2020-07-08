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

package io.questdb.std;

import org.junit.Test;

public class VectTest {
    @Test
    public void testRadixSort() {
        Os.init();
        Rnd rnd = new Rnd();
        int count = 130_000_000;
        final long p = Unsafe.malloc(count * 2 * Long.BYTES);

        for (int i = 0; i < count; i++) {
            long z = rnd.nextPositiveLong();
            Unsafe.getUnsafe().putLong(p + i * 2 * Long.BYTES, z);
            Unsafe.getUnsafe().putLong(p + i * 2 * Long.BYTES + 8, i);
        }
        System.out.println("------------RADIX---------------------");
        long t = System.nanoTime();
        Vect.radixSort(p, count);
        System.out.println((System.nanoTime() - t));

        for (int i = 0; i < count; i++) {
            long z = rnd.nextPositiveLong();
            Unsafe.getUnsafe().putLong(p + i * 2 * Long.BYTES, z);
            Unsafe.getUnsafe().putLong(p + i * 2 * Long.BYTES + 8, i);
        }
        System.out.println("------------RADIX2---------------------");
        t = System.nanoTime();
        Vect.radixSort(p, count);
        System.out.println((System.nanoTime() - t));

        // verify
        long v = Unsafe.getUnsafe().getLong(p);
        for (int i = 1; i < count; i++) {
            long next = Unsafe.getUnsafe().getLong(p + i * 2 * Long.BYTES);
//            System.out.println(Unsafe.getUnsafe().getLong(index + i * Long.BYTES ));
            assert next >= v;
            v = next;
        }
    }
}

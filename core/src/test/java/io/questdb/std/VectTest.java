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
        SortAdaptor adaptor = new SortAdaptor(count);

        for (int i = 0; i < adaptor.size; i++) {
            long z = rnd.nextPositiveLong();
            Unsafe.getUnsafe().putLong(adaptor.p + i * 8, z);
        }
        System.out.println("------------RADIX---------------------");
        long t = System.nanoTime();
        Vect.radixSort(adaptor.p, adaptor.size);
        System.out.println((System.nanoTime() - t));

        for (int i = 0; i < adaptor.size; i++) {
            long z = rnd.nextPositiveLong();
            Unsafe.getUnsafe().putLong(adaptor.p + i * 8, z);
        }
        System.out.println("------------RADIX2---------------------");
        t = System.nanoTime();
        Vect.radixSort(adaptor.p, adaptor.size);
        System.out.println((System.nanoTime() - t));

        // verify
        long v = Unsafe.getUnsafe().getLong(adaptor.p);
        for (int i = 1; i < adaptor.size; i++) {
            long next = Unsafe.getUnsafe().getLong(adaptor.p + i * 8);
            assert next >= v;
            v = next;
        }
/*

        LongList ll = new LongList(count);
        for (int i = 0; i < adaptor.size; i++) {
            long z = rnd.nextPositiveLong();
            ll.add(z);
        }

        System.out.println("------------ HEAP WARMUP ------------------");
        t = System.nanoTime();
        ll.sort();
        System.out.println((System.nanoTime() - t));

        ll.clear();
        for (int i = 0; i < adaptor.size; i++) {
            long z = rnd.nextPositiveLong();
            ll.add(z);
        }

        System.out.println("------------ HEAP ------------------");
        t = System.nanoTime();
        ll.sort();
        System.out.println((System.nanoTime() - t));


        for (int i = 0; i < adaptor.size; i++) {
            long z = rnd.nextPositiveLong();
            Unsafe.getUnsafe().putLong(adaptor.p + i * 8, z);
        }
        System.out.println("------------ NATIVE ------------------");
        t = System.nanoTime();
        LongSort.sort(adaptor, 0, adaptor.size);
        System.out.println((System.nanoTime() - t));
*/
    }

    private static class SortAdaptor implements LongVec {
        private final long p;
        private final int size;

        public SortAdaptor(int size) {
            this.p = Unsafe.malloc(size * Long.BYTES);
            this.size = size;
        }

        @Override
        public long getQuick(int index) {
            return Unsafe.getUnsafe().getLong(p + index * Long.BYTES);
        }

        @Override
        public void setQuick(int index, long value) {
            Unsafe.getUnsafe().putLong(p + index * Long.BYTES, value);
        }

        @Override
        public LongVec newInstance() {
            return new SortAdaptor(size);
        }

        public int getSize() {
            return size;
        }
    }
}

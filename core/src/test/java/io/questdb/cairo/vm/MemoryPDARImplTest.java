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

package io.questdb.cairo.vm;

import io.questdb.cairo.AbstractCairoTest;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;

public class MemoryPDARImplTest extends AbstractCairoTest {

    @Test
    @Ignore
    public void testMulti1() {

        int n = 2;

        CyclicBarrier barrier = new CyclicBarrier(n);
        SOCountDownLatch latch = new SOCountDownLatch(n);
        for (int i = 0; i < n; i++) {
            int c = i;
            new Thread(() -> {
                TestUtils.await(barrier);
                testMemMap(c);
                latch.countDown();
            }).start();
        }

        latch.await();
    }

    public void testSimple(int c) {
        // write simple long
        try (
                Path path = new Path();
                MemoryPDARImpl mem = new MemoryPDARImpl(
                        FilesFacadeImpl.INSTANCE,
                        path.of(root).concat("x.d"+c).$(),
                        FilesFacadeImpl._16M,
                        MemoryTag.MMAP_DEFAULT
                )
        ) {
            Rnd rnd = new Rnd();
            long t = System.nanoTime();
            for (long i = 0; i < 1_000_000_000; i++) {
                mem.putLong(rnd.nextLong());
            }
            System.out.println(System.nanoTime() - t);
        }
    }

    public void testMemMap(int c) {
        // write simple long
        try (
                Path path = new Path();
                MemoryPMARImpl mem = new MemoryPMARImpl(
                        FilesFacadeImpl.INSTANCE,
                        path.of(root).concat("x.d"+c).$(),
                        FilesFacadeImpl._16M,
                        MemoryTag.MMAP_DEFAULT
                )
        ) {
            Rnd rnd = new Rnd();
            long t = System.nanoTime();
            for (long i = 0; i < 1_000_000_000; i++) {
                mem.putLong(rnd.nextLong());
            }
            System.out.println(System.nanoTime() - t);
        }
    }

}
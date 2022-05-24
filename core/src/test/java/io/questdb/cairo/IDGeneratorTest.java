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

package io.questdb.cairo;

import io.questdb.griffin.engine.table.LongTreeSet;
import io.questdb.std.LongList;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class IDGeneratorTest extends AbstractCairoTest {

    @Test
    public void testNextTableId() {
        try (
                CairoEngine engineA = new CairoEngine(configuration);
                CairoEngine engineB = new CairoEngine(configuration)
        ) {

            final LongList listA = new LongList();
            final LongList listB = new LongList();
            final CyclicBarrier startBarrier = new CyclicBarrier(2);
            final AtomicInteger errors = new AtomicInteger();

            final Thread threadB = new Thread(() -> {
                try {
                    startBarrier.await();
                    for (int i = 0; i < 1000; i++) {
                        listA.add(engineA.getTableIdGenerator().getNextId());
                    }
                } catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                    errors.incrementAndGet();
                }
            });
            threadB.start();

            try {
                startBarrier.await();
                for (int i = 0; i < 1000; i++) {
                    listB.add(engineB.getTableIdGenerator().getNextId());
                }
                threadB.join();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
                errors.incrementAndGet();
            }

            try (LongTreeSet set = new LongTreeSet(4 * 2048, Integer.MAX_VALUE)) {
                // add both arrays to the set and asset that there are no duplicates
                for (int i = 0, n = listA.size(); i < n; i++) {
                    Assert.assertTrue(set.put(listA.getQuick(i)));
                }
                for (int i = 0, n = listB.size(); i < n; i++) {
                    Assert.assertTrue(set.put(listB.getQuick(i)));
                }
            }
        }
    }
}
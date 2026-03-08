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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableWriter;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.Sequence;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.tasks.ColumnTask;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

public class O3CallbackFuzzTest extends AbstractCairoTest {

    @Test
    public void testO3CallbackQueue() throws InterruptedException {
        Rnd rnd = TestUtils.generateRandom(null);

        int columnCount = rnd.nextInt(200) + 2;
        int complexity = rnd.nextInt(20);
        int writers = rnd.nextInt(10) + 2;

        AtomicReference<Throwable> th = new AtomicReference<>();
        ObjList<WriterObj> writerObjList = new ObjList<>();

        CairoEngine cairoEngine = engine;
        for (int i = 0; i < writers; i++) {
            writerObjList.add(new WriterObj(new Rnd(rnd.nextLong(), rnd.nextLong()), th, columnCount, complexity, cairoEngine));
        }

        CyclicBarrier start = new CyclicBarrier(writers);
        ObjList<Thread> threadList = new ObjList<>();

        for (int thread = 0; thread < writers; thread++) {
            final WriterObj writerObj = writerObjList.getQuick(thread);
            threadList.add(new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }
                writerObj.o3ScheduleMoveUncommitted0();
            }));
            threadList.getLast().start();
        }

        for (int thread = 0; thread < writers; thread++) {
            threadList.get(thread).join();
        }

        if (th.get() != null) {
            throw new RuntimeException(th.get());
        }
    }

    public static class WriterObj {
        final SOUnboundedCountDownLatch o3DoneLatch = new SOUnboundedCountDownLatch();
        private final int complexity;
        private final CairoEngine engine;
        private final TableWriter.ColumnTaskHandler o3MoveUncommittedRef = this::o3MoveUncommitted0;
        private final Rnd rnd;
        private final AtomicReference<Throwable> th;
        int columnCount;

        public WriterObj(Rnd rnd, AtomicReference<Throwable> th, int columns, int complexity, CairoEngine engine) {
            this.engine = engine;
            this.rnd = rnd;
            columnCount = rnd.nextInt(columns) + 1;
            this.th = th;
            this.complexity = complexity;
        }

        public void o3ScheduleMoveUncommitted0() {
            try {
                o3DoneLatch.reset();
                int queuedCount = 0;
                final Sequence pubSeq = engine.getMessageBus().getColumnTaskPubSeq();
                final RingQueue<ColumnTask> queue = engine.getMessageBus().getColumnTaskQueue();

                for (int colIndex = 0; colIndex < columnCount; colIndex++) {
                    long cursor = pubSeq.next();

                    // Pass column index as -1 when it's designated timestamp column to o3 move method
                    if (cursor > -1) {
                        try {
                            final ColumnTask task = queue.get(cursor);
                            task.of(o3DoneLatch, rnd.nextInt(1000), 1, 0, 1, 1, 1, 1, 1, this.o3MoveUncommittedRef);
                        } finally {
                            queuedCount++;
                            pubSeq.done(cursor);
                        }
                    } else {
                        o3MoveUncommitted0(rnd.nextInt(1000), 1, 0, 1, 1, 1, 1, 1);
                    }
                }

                consumeO3CallbackQueue(queue, queuedCount);
            } catch (Throwable e) {
                e.printStackTrace();
                th.set(e);
            }
        }

        private void consumeO3CallbackQueue(RingQueue<ColumnTask> queue, int queuedCount) {
            try {
                // This is work stealing, can run tasks from other table writers
                final Sequence subSeq = engine.getMessageBus().getColumnTaskSubSeq();
                TableWriter.consumeColumnTasks0(queue, queuedCount, subSeq, o3DoneLatch);
                assert o3DoneLatch.getCount() == -queuedCount : "o3DoneLatch.getCount()=" + o3DoneLatch.getCount() + ", queuedCount=" + queuedCount;
            } catch (Throwable e) {
                e.printStackTrace();
                th.set(e);
            }
        }

        private void o3MoveUncommitted0(int rndInt, int i1, long timestampColumnIndex, long l, long l0, long l1, long l2, long l3) {
            //noinspection unused
            double result = 0;
            for (int i = 0; i < rndInt * complexity; i++) {
                result += Math.log(rndInt * Math.sin(rndInt * i) * Math.cos(rndInt * i) * Math.tan(rndInt * i) * Math.sqrt(rndInt * i));
            }
        }
    }
}

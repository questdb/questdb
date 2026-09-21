/*+*****************************************************************************
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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.Metrics;
import io.questdb.cutlass.line.tcp.DefaultColumnTypes;
import io.questdb.cutlass.line.tcp.LineTcpMeasurementEvent;
import io.questdb.cutlass.line.tcp.LineTcpMeasurementScheduler;
import io.questdb.mp.Job;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.Sequence;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;

public class LineTcpWriterJobTest extends AbstractCairoTest {

    @Test
    public void testRunStopsAfterOneQueueCycle() throws Exception {
        assertMemoryLeak(() -> {
            final int queueCapacity = 4;
            final ReplenishingSequence sequence = new ReplenishingSequence(queueCapacity);
            final Constructor<LineTcpMeasurementEvent> eventConstructor = LineTcpMeasurementEvent.class.getDeclaredConstructor(
                    long.class,
                    long.class,
                    byte.class,
                    DefaultColumnTypes.class,
                    boolean.class,
                    int.class,
                    boolean.class
            );
            eventConstructor.setAccessible(true);
            try (RingQueue<LineTcpMeasurementEvent> queue = new RingQueue<>(
                    () -> newEvent(eventConstructor),
                    queueCapacity
            )) {
                final Job job = newJob(queue, sequence);

                Assert.assertTrue(job.run(Job.RUNNING_STATUS));
                Assert.assertEquals(queueCapacity, sequence.getDoneCount());
                Assert.assertEquals(queueCapacity, sequence.getRemainingCount());
            }
        });
    }

    private static LineTcpMeasurementEvent newEvent(Constructor<LineTcpMeasurementEvent> constructor) {
        try {
            return constructor.newInstance(0L, 0L, (byte) 0, null, false, 0, false);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static Job newJob(
            RingQueue<LineTcpMeasurementEvent> queue,
            Sequence sequence
    ) throws ReflectiveOperationException {
        final Class<?> jobClass = Class.forName("io.questdb.cutlass.line.tcp.LineTcpWriterJob");
        final Constructor<?> constructor = jobClass.getDeclaredConstructor(
                int.class,
                RingQueue.class,
                Sequence.class,
                MillisecondClock.class,
                long.class,
                LineTcpMeasurementScheduler.class,
                Metrics.class,
                ObjList.class
        );
        constructor.setAccessible(true);
        return (Job) constructor.newInstance(
                1,
                queue,
                sequence,
                (MillisecondClock) () -> 0,
                Long.MAX_VALUE,
                null,
                Metrics.DISABLED,
                new ObjList<>()
        );
    }

    private static final class ReplenishingSequence extends SCSequence {
        private final long cursorLimit;
        private int doneCount;
        private long nextCursor;
        private long publishedCursorLimit;

        private ReplenishingSequence(int queueCapacity) {
            this.cursorLimit = 2L * queueCapacity;
            this.publishedCursorLimit = queueCapacity;
        }

        @Override
        public void done(long cursor) {
            doneCount++;
            if (publishedCursorLimit < cursorLimit) {
                publishedCursorLimit++;
            }
        }

        @Override
        public long next() {
            return nextCursor < publishedCursorLimit ? nextCursor++ : -1;
        }

        private int getDoneCount() {
            return doneCount;
        }

        private long getRemainingCount() {
            return publishedCursorLimit - nextCursor;
        }
    }
}

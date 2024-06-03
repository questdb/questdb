/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.std.DebouncingRunnable;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class DebouncingRunnableTest {

    @Test
    public void testDebouncingRunnable() {
        AtomicInteger callCount = new AtomicInteger();

        Runnable runnable = callCount::incrementAndGet;

        StatefulTestMicroClock clock = new StatefulTestMicroClock(1);
        long debouncePeriod = 1000 * 5;

        DebouncingRunnable debouncingRunnable = new DebouncingRunnable(runnable, debouncePeriod, clock);
        debouncingRunnable.run();
        Assert.assertEquals(1, callCount.get());

        debouncingRunnable.run();
        Assert.assertEquals(1, callCount.get());

        debouncingRunnable.run();
        Assert.assertEquals(1, callCount.get());

        clock.getTicks();
        clock.getTicks();
        clock.getTicks();

        debouncingRunnable.run();
        Assert.assertEquals(2, callCount.get());

    }

    public class StatefulTestMicroClock implements MicrosecondClock {
        private final long increment;
        private long micros;

        public StatefulTestMicroClock(long increment) {
            this.increment = increment * 1000;
        }

        @Override
        public long getTicks() {
            this.micros += this.increment;
            return this.micros;
        }
    }
}

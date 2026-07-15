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

package io.questdb.test.cairo.sql.async;

import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.async.AdaptiveWorkStealingStrategy;
import io.questdb.cairo.sql.async.WorkStealingStrategy;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class AdaptiveWorkStealingStrategyTest {

    @Test
    public void testTestHookSelectsInstrumentedStrategy() {
        for (java.lang.reflect.Field field : AdaptiveWorkStealingStrategy.class.getDeclaredFields()) {
            Assert.assertNotEquals(StatefulAtom.class, field.getType());
            Assert.assertNotEquals(boolean.class, field.getType());
        }

        final AdaptiveWorkStealingStrategy strategy = new AdaptiveWorkStealingStrategy(0, 0);
        final AtomicInteger startedCounter = new AtomicInteger();
        final WorkStealingStrategy normalStrategy = strategy.of(startedCounter, new StatefulAtom() {
        });
        Assert.assertSame(strategy, normalStrategy);

        final AtomicInteger awaitCount = new AtomicInteger();
        final StatefulAtom testAtom = new StatefulAtom() {
            @Override
            public boolean awaitTestSlotAcquire() {
                awaitCount.incrementAndGet();
                return true;
            }

            @Override
            public boolean isTestSlotAcquireWaitEnabled() {
                return true;
            }
        };
        final WorkStealingStrategy testStrategy = strategy.of(startedCounter, testAtom);
        Assert.assertNotSame(strategy, testStrategy);
        Assert.assertFalse(testStrategy.shouldSteal(0));
        Assert.assertEquals(1, awaitCount.get());
    }
}

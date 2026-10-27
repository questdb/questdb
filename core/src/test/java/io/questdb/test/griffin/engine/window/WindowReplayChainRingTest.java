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


package io.questdb.test.griffin.engine.window;

import io.questdb.cairo.RecordChain;
import io.questdb.griffin.engine.functions.window.ReplayableWindowFunction;
import io.questdb.griffin.engine.window.AsyncWindowSplitPlan;
import io.questdb.std.DirectLongList;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;

/**
 * The ring bound of the parallel window's off-thread pass ({@code AsyncWindowRecordCursor.ReplayChain}),
 * driven through the cursor's exact call sequence with {@code max.rounds=2} and two tasks per
 * round, i.e. a ring of 4 tasks, with a worker preempted between the pass's two writes of the
 * last task of a round: the task's {@code passDone} and the chain's {@code head}.
 * <p>
 * The query's thread may return a task as soon as it sees its {@code passDone}, release the
 * task's round and collect the round again, enqueuing two new tasks. When the pass published
 * {@code passDone} first, the second enqueue finds {@code tail - head == ring.length}, and the
 * ring's assertion fires in a legal state. The pass must advance {@code head} first.
 */
public class WindowReplayChainRingTest {
    private static final int RING = 4;

    @Test
    public void testReturnAtPassDoneKeepsRingBound() throws Exception {
        final Class<?> chainClass = Class.forName("io.questdb.griffin.engine.window.AsyncWindowRecordCursor$ReplayChain");
        final Class<?> taskClass = Class.forName("io.questdb.griffin.engine.window.AsyncWindowRecordCursor$Task");
        final Constructor<?> chainCtor = chainClass.getDeclaredConstructor(AsyncWindowSplitPlan.class, ReplayableWindowFunction[].class, int.class);
        chainCtor.setAccessible(true);
        // roundCount * tasksPerRound, as the cursor sizes it
        final Object chain = chainCtor.newInstance(AsyncWindowSplitPlan.NONE, new ReplayableWindowFunction[0], RING);
        final Constructor<?> taskCtor = taskClass.getDeclaredConstructor(RecordChain.class, DirectLongList.class);
        taskCtor.setAccessible(true);
        final Object[] t = new Object[RING];
        for (int i = 0; i < RING; i++) {
            t[i] = taskCtor.newInstance(null, null);
        }
        final Method enqueue = chainClass.getDeclaredMethod("enqueue", taskClass, long[].class);
        final Method computed = chainClass.getDeclaredMethod("computed", taskClass, boolean.class);
        final Method awaitPassed = chainClass.getDeclaredMethod("awaitPassed", taskClass);
        enqueue.setAccessible(true);
        computed.setAccessible(true);
        awaitPassed.setAccessible(true);
        final Field listenerField = chainClass.getDeclaredField("passPublishingListener");
        listenerField.setAccessible(true);
        final long[] carry = new long[0];

        // round A = {t0, t1}, round B = {t2, t3}
        for (int i = 0; i < RING; i++) {
            enqueue.invoke(chain, t[i], carry);
        }
        computed.invoke(chain, t[0], true);
        computed.invoke(chain, t[1], true);
        // the query's thread returns round A's tasks and collects round A again: walk positions 4, 5
        awaitPassed.invoke(chain, t[0]);
        awaitPassed.invoke(chain, t[1]);
        enqueue.invoke(chain, t[0], carry);
        enqueue.invoke(chain, t[1], carry);
        computed.invoke(chain, t[2], true);

        // A worker computed t3 and passes it. Between the pass's two writes of t3, the query's
        // thread looks: when it sees t3's passDone, it returns t2 and t3, releases round B and
        // collects it again, walk positions 6 and 7.
        final AtomicInteger listenerCalls = new AtomicInteger();
        final AssertionError[] fired = new AssertionError[1];
        final boolean[] sawPassDone = new boolean[1];
        final LongConsumer queryThread = passSeq -> {
            if (passSeq != 3) {
                return;
            }
            listenerCalls.incrementAndGet();
            try {
                Assert.assertEquals(1, ((AtomicInteger) field(chain, "lock")).get());
                sawPassDone[0] = (boolean) field(t[3], "passDone");
                if (sawPassDone[0]) {
                    awaitPassed.invoke(chain, t[2]);
                    awaitPassed.invoke(chain, t[3]);
                    enqueue.invoke(chain, t[2], carry);
                    enqueue.invoke(chain, t[3], carry);
                } else {
                    // the task is not returned yet, and once it is, the head is past it
                    Assert.assertEquals(4L, field(chain, "head"));
                }
            } catch (InvocationTargetException e) {
                if (e.getCause() instanceof AssertionError ae) {
                    fired[0] = ae;
                } else {
                    throw new RuntimeException(e.getCause());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        listenerField.set(null, queryThread);
        try {
            computed.invoke(chain, t[3], true);
        } finally {
            listenerField.set(null, null);
        }
        // the seam runs under -ea, as every test does
        Assert.assertEquals(1, listenerCalls.get());
        if (fired[0] != null) {
            Assert.fail("ring assertion fired in a reachable state: " + fired[0].getMessage());
        }
        Assert.assertEquals(4L, field(chain, "head"));
        if (!sawPassDone[0]) {
            // the query's thread returns t2 and t3 now, and collects round B again
            awaitPassed.invoke(chain, t[2]);
            awaitPassed.invoke(chain, t[3]);
            enqueue.invoke(chain, t[2], carry);
            enqueue.invoke(chain, t[3], carry);
        }
        Assert.assertEquals(8L, field(chain, "tail"));
        Assert.assertTrue((long) field(chain, "tail") - (long) field(chain, "head") <= RING);
    }

    private static Object field(Object o, String name) throws Exception {
        final Field f = o.getClass().getDeclaredField(name);
        f.setAccessible(true);
        return f.get(o);
    }
}

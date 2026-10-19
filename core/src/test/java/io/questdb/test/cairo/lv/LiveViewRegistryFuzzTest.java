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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewDefinition;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewLifecycleState;
import io.questdb.cairo.lv.LiveViewRegistry;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Pins the generation-aware primitives {@link LiveViewRegistry#registerViewIfAbsent} and
 * {@link LiveViewRegistry#removeView(CharSequence, LiveViewInstance)} - the CAS shapes
 * {@code TableNameRegistryRW} applies to table tokens, added here so a publisher or
 * remover that lost a drop-and-recreate race cannot displace another generation's entry.
 * <p>
 * The fuzz drives many threads through the real consumer lifecycle (build a generation,
 * try to publish it, hold it, conditionally remove it) over a handful of contended names,
 * asserting the ownership contract at every step:
 * <ul>
 *     <li>a successful {@code registerViewIfAbsent} means exclusive ownership - no other
 *     actor using the conditional primitives can displace or remove the entry, so the
 *     owner re-reads itself under the name until its own conditional remove;</li>
 *     <li>a refused publication changes NOTHING: the loser is in neither map, its
 *     conditional remove is a no-op returning {@code false}, and the reported owner is a
 *     live same-name instance;</li>
 *     <li>the name map and the base-table fan-out move together: a read-locked fan-out
 *     snapshot never holds two entries for one view name and never an entry filed under
 *     the wrong base;</li>
 *     <li>every publish paired with its conditional remove leaves the registry EMPTY at
 *     the end - no leaked name entries, no orphan fan-out entries.</li>
 * </ul>
 * A plain {@code registerView} put deliberately stays out of the concurrent mix: it is
 * specified only for callers holding the table-name lock, and its displacement semantics
 * are exactly what the conditional primitives exist to avoid.
 */
public class LiveViewRegistryFuzzTest extends AbstractTest {

    @Test
    public void testConcurrentGenerationChurn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final int threadCount = 2 + rnd.nextInt(6);
            final int iterations = 1_000 + rnd.nextInt(4_000);
            final int nameCount = 1 + rnd.nextInt(6);
            final int baseCount = 1 + rnd.nextInt(3);

            final String[] names = new String[nameCount];
            final String[] basesByName = new String[nameCount];
            final String[] bases = new String[baseCount];
            for (int i = 0; i < baseCount; i++) {
                bases[i] = "base_" + i;
            }
            for (int i = 0; i < nameCount; i++) {
                names[i] = "view_" + i;
                basesByName[i] = bases[i % baseCount];
            }

            final LiveViewRegistry registry = new LiveViewRegistry();
            final AtomicInteger idGen = new AtomicInteger();
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final AtomicBoolean done = new AtomicBoolean();
            final CyclicBarrier barrier = new CyclicBarrier(threadCount + 1);
            final ObjList<Thread> threads = new ObjList<>();

            for (int t = 0; t < threadCount; t++) {
                final Rnd threadRnd = new Rnd(rnd.nextLong(), rnd.nextLong());
                final Thread thread = new Thread(() -> {
                    final ObjList<LiveViewInstance> sink = new ObjList<>();
                    try {
                        barrier.await();
                        for (int i = 0; i < iterations && failure.get() == null; i++) {
                            final int n = threadRnd.nextInt(nameCount);
                            final String name = names[n];
                            final String base = basesByName[n];
                            final LiveViewInstance inst = newInstance(name, base, idGen.incrementAndGet());
                            final LiveViewInstance owner = registry.registerViewIfAbsent(inst);
                            if (owner == null) {
                                // Published: ownership is exclusive until our own conditional
                                // remove. No concurrent registerViewIfAbsent can displace the
                                // entry and no concurrent removeView(name, otherInstance) can
                                // take it out.
                                Assert.assertSame("published instance must own the name", inst, registry.getViewInstance(name));
                                if (threadRnd.nextInt(8) == 0) {
                                    Assert.assertEquals("owner must appear in its base fan-out exactly once",
                                            1, countByIdentity(registry, base, inst, sink));
                                }
                                if (threadRnd.nextInt(16) == 0) {
                                    Os.pause();
                                }
                                Assert.assertTrue("owner's conditional remove must succeed",
                                        registry.removeView(name, inst));
                                Assert.assertFalse("second conditional remove must no-op",
                                        registry.removeView(name, inst));
                                Assert.assertNotSame("removed instance must not resurface under the name",
                                        inst, registry.getViewInstance(name));
                                Assert.assertEquals("removed instance must leave no fan-out entry behind",
                                        0, countByIdentity(registry, base, inst, sink));
                            } else {
                                // Refused: the loser is in neither map and cannot touch the owner.
                                Assert.assertNotSame(inst, owner);
                                Assert.assertTrue("reported owner must be a same-name instance",
                                        Chars.equals(name, owner.getDefinition().getViewName()));
                                Assert.assertFalse("loser's conditional remove must not touch the owner",
                                        registry.removeView(name, inst));
                                Assert.assertNotSame("refused instance must never surface under the name",
                                        inst, registry.getViewInstance(name));
                                Assert.assertEquals("refused instance must not enter the fan-out",
                                        0, countByIdentity(registry, base, inst, sink));
                            }
                            Misc.free(inst);
                        }
                    } catch (Throwable th) {
                        failure.compareAndSet(null, th);
                    }
                }, "lv-registry-fuzz-actor-" + t);
                threads.add(thread);
                thread.start();
            }

            // Torn-state watcher: with only the conditional primitives in play, a read-locked
            // fan-out snapshot can never hold two entries for one view name (adds are gated by
            // the name-map putIfAbsent, removes drop the exact instance with its name entry,
            // both under the per-base write lock), and never an entry filed under another base.
            final Thread reader = new Thread(() -> {
                final ObjList<LiveViewInstance> sink = new ObjList<>();
                try {
                    barrier.await();
                    while (!done.get() && failure.get() == null) {
                        for (int b = 0; b < baseCount; b++) {
                            registry.getViewsForBaseTable(bases[b], sink);
                            for (int i = 0, size = sink.size(); i < size; i++) {
                                final LiveViewInstance left = sink.getQuick(i);
                                Assert.assertTrue("fan-out entry filed under the wrong base",
                                        Chars.equals(bases[b], left.getDefinition().getBaseTableName()));
                                for (int j = i + 1; j < size; j++) {
                                    Assert.assertFalse("two live generations under one name in a single fan-out snapshot",
                                            Chars.equals(left.getDefinition().getViewName(), sink.getQuick(j).getDefinition().getViewName()));
                                }
                            }
                        }
                        Os.pause();
                    }
                } catch (Throwable th) {
                    failure.compareAndSet(null, th);
                }
            }, "lv-registry-fuzz-reader");
            reader.start();

            for (int t = 0, size = threads.size(); t < size; t++) {
                threads.getQuick(t).join();
            }
            done.set(true);
            reader.join();

            if (failure.get() != null) {
                throw new AssertionError("fuzz failed", failure.get());
            }

            // Every publish was paired with a successful conditional remove, so nothing may
            // survive: no name entries, no orphan fan-out entries.
            final ObjList<LiveViewInstance> sink = new ObjList<>();
            registry.getViews(sink);
            Assert.assertEquals("name map must be empty after the churn", 0, sink.size());
            for (int b = 0; b < baseCount; b++) {
                registry.getViewsForBaseTable(bases[b], sink);
                Assert.assertEquals("fan-out must be empty after the churn", 0, sink.size());
            }
            registry.close();
        });
    }

    @Test
    public void testConditionalPrimitivesDeterministicSemantics() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final LiveViewRegistry registry = new LiveViewRegistry();
            final ObjList<LiveViewInstance> sink = new ObjList<>();

            // CAS publication: the first generation wins, the second is refused and reported
            // the current owner; the refusal leaves the fan-out untouched.
            final LiveViewInstance genA = newInstance("lv", "base", 1);
            final LiveViewInstance genB = newInstance("lv", "base", 2);
            Assert.assertNull(registry.registerViewIfAbsent(genA));
            Assert.assertSame(genA, registry.registerViewIfAbsent(genB));
            Assert.assertSame(genA, registry.getViewInstance("lv"));
            Assert.assertEquals(1, countByIdentity(registry, "base", genA, sink));
            Assert.assertEquals(0, countByIdentity(registry, "base", genB, sink));

            // Expected-value removal: the loser cannot take the owner out; the owner can,
            // exactly once, and the fan-out entry goes with it.
            Assert.assertFalse(registry.removeView("lv", genB));
            Assert.assertSame(genA, registry.getViewInstance("lv"));
            Assert.assertTrue(registry.removeView("lv", genA));
            Assert.assertNull(registry.getViewInstance("lv"));
            Assert.assertEquals(0, countByIdentity(registry, "base", genA, sink));
            Assert.assertFalse(registry.removeView("lv", genA));

            // The freed name accepts the next generation.
            Assert.assertNull(registry.registerViewIfAbsent(genB));
            Assert.assertSame(genB, registry.getViewInstance("lv"));
            Assert.assertTrue(registry.removeView("lv", genB));
            Misc.free(genA);
            Misc.free(genB);

            // Definition-less stubs live in the name map only; the conditional remove must
            // key on identity there too, without touching any fan-out list.
            final TableToken stubToken = liveViewToken("lv", 3);
            final LiveViewInstance stub = new LiveViewInstance(stubToken, LiveViewLifecycleState.STATE_UNREADABLE);
            final LiveViewInstance otherStub = new LiveViewInstance(stubToken, LiveViewLifecycleState.STATE_UNREADABLE);
            registry.registerStubView(stub);
            Assert.assertFalse(registry.removeView("lv", otherStub));
            Assert.assertSame(stub, registry.getViewInstance("lv"));
            Assert.assertTrue(registry.removeView("lv", stub));
            Assert.assertNull(registry.getViewInstance("lv"));
            Misc.free(stub);
            Misc.free(otherStub);

            registry.close();
        });
    }

    private static int countByIdentity(LiveViewRegistry registry, String base, LiveViewInstance needle, ObjList<LiveViewInstance> sink) {
        registry.getViewsForBaseTable(base, sink);
        int count = 0;
        for (int i = 0, size = sink.size(); i < size; i++) {
            if (sink.getQuick(i) == needle) {
                count++;
            }
        }
        return count;
    }

    private static TableToken liveViewToken(String name, int id) {
        return new TableToken(name, name + '~' + id, null, id, TableToken.Type.LIVE_VIEW, true, false, false, false);
    }

    private static LiveViewInstance newInstance(String name, String base, int id) {
        final LiveViewDefinition definition = new LiveViewDefinition(
                name,
                "SELECT * FROM " + base,
                base,
                null,
                0,
                0,
                's',
                0,
                's',
                0,
                0,
                (byte) 0,
                null,
                new ObjList<>(),
                new IntList(),
                null
        );
        return new LiveViewInstance(definition, liveViewToken(name, id));
    }
}

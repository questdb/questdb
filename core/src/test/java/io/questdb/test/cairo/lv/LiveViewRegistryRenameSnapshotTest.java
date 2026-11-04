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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewCheckpointOutputUniqueness;
import io.questdb.cairo.lv.LiveViewDefinition;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRegistry;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link LiveViewRegistry#renameView} re-keys {@code viewsByName} with a
 * {@code remove(oldName)} / {@code put(newName)} pair. Every other mutator rebuilds the
 * {@code allViews} snapshot afterwards; the rename used to skip it on the reasoning that the
 * snapshot holds instances rather than names. That reasoning holds only single-threaded: a
 * concurrent registration or removal can rebuild the snapshot from inside the remove/put gap and
 * publish a list missing the renamed instance - permanently, because the rename never republishes
 * to heal it. The instance then vanishes from {@code live_views()}, from the refresh pool's idle
 * scan, and from {@code clear()}'s free loop, so engine teardown never closes it.
 * <p>
 * These are narrow registry unit tests: they allocate no native memory, so they need no
 * {@code assertMemoryLeak()} (matching the other narrow tests in this package). The interleaving is
 * forced with latches through {@link LiveViewInstance#updateToken}, which {@code renameView} calls
 * inside the gap in both of its branches - no test-only hook is added to production code.
 */
public class LiveViewRegistryRenameSnapshotTest {

    @Test
    public void testConcurrentRegisterDuringRenameGapKeepsViewInSnapshot() throws Exception {
        final LiveViewRegistry registry = new LiveViewRegistry();
        final CountDownLatch inGap = new CountDownLatch(1);
        final CountDownLatch republished = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<>();

        final TableToken pendingToken = tokenOf("lv_pending", 1);
        final TestInstance renamed = new TestInstance(
                definitionOf("lv_pending", "base_a"),
                pendingToken,
                () -> {
                    inGap.countDown();
                    awaitLatch(republished, "the third party's republish");
                }
        );
        registry.registerView(renamed);

        // A second view over a DIFFERENT base table. registerView takes that base table's DepList
        // write lock, not the renamed view's, so it never contends with the rename and its
        // republishViews() lands squarely inside the remove/put gap.
        final TestInstance other = new TestInstance(definitionOf("lv_other", "base_b"), tokenOf("lv_other", 2), null);

        final Thread renamer = startRenamer(registry, "lv_pending", pendingToken.renamed("lv"), error);
        awaitLatch(inGap, "the rename to reach the remove/put gap");
        registry.registerView(other);
        republished.countDown();
        renamer.join(TimeUnit.SECONDS.toMillis(30));

        Assert.assertNull("the rename must not fail", error.get());
        Assert.assertFalse("the rename thread must finish", renamer.isAlive());
        Assert.assertSame("the rename must re-key the name map", renamed, registry.getViewInstance("lv"));

        final ObjList<LiveViewInstance> sink = new ObjList<>();
        // getViews is exactly what live_views() reads - LiveViewsFunctionFactory.
        registry.getViews(sink);
        Assert.assertTrue(
                "the renamed view must survive a concurrent republish in the live_views() snapshot",
                hasInstance(sink, renamed)
        );
        Assert.assertTrue(hasInstance(sink, other));
        Assert.assertEquals(2, sink.size());

        // getShardedViews is the refresh pool's idle-scan surface - LiveViewRefreshJob.
        registry.getShardedViews(sink, 0, 1);
        Assert.assertTrue("the renamed view must stay in the idle-scan shard", hasInstance(sink, renamed));

        // clear() frees instances by walking allViews, so a view missing from the snapshot is one
        // engine teardown never closes.
        registry.clear();
        Assert.assertTrue("engine teardown must free the renamed view", renamed.hasBeenClosed());
        Assert.assertTrue("engine teardown must free the other view", other.hasBeenClosed());
    }

    @Test
    public void testConcurrentRemoveDuringRenameGapKeepsViewInSnapshot() throws Exception {
        final LiveViewRegistry registry = new LiveViewRegistry();
        final CountDownLatch inGap = new CountDownLatch(1);
        final CountDownLatch republished = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<>();

        final TableToken pendingToken = tokenOf("lv_pending", 1);
        final TestInstance renamed = new TestInstance(
                definitionOf("lv_pending", "base_a"),
                pendingToken,
                () -> {
                    inGap.countDown();
                    awaitLatch(republished, "the third party's republish");
                }
        );
        final TestInstance doomed = new TestInstance(definitionOf("lv_doomed", "base_b"), tokenOf("lv_doomed", 2), null);
        registry.registerView(renamed);
        registry.registerView(doomed);

        final Thread renamer = startRenamer(registry, "lv_pending", pendingToken.renamed("lv"), error);
        awaitLatch(inGap, "the rename to reach the remove/put gap");
        // The DROP path's republish, from inside the gap.
        Assert.assertSame(doomed, registry.removeView("lv_doomed"));
        republished.countDown();
        renamer.join(TimeUnit.SECONDS.toMillis(30));

        Assert.assertNull("the rename must not fail", error.get());
        Assert.assertSame("the rename must re-key the name map", renamed, registry.getViewInstance("lv"));

        final ObjList<LiveViewInstance> sink = new ObjList<>();
        registry.getViews(sink);
        Assert.assertTrue("a concurrent DROP must not evict the renamed view", hasInstance(sink, renamed));
        Assert.assertFalse("the dropped view must be gone", hasInstance(sink, doomed));
        Assert.assertEquals(1, sink.size());

        registry.clear();
        Assert.assertTrue("engine teardown must free the renamed view", renamed.hasBeenClosed());
    }

    @Test
    public void testConcurrentStubRegisterDuringStubRenameGapKeepsViewInSnapshot() throws Exception {
        final LiveViewRegistry registry = new LiveViewRegistry();
        final CountDownLatch inGap = new CountDownLatch(1);
        final CountDownLatch republished = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<>();

        // A definition-less load-failure stub takes renameView's early-return branch, which holds
        // no DepList lock at all - so it is unprotected even against a same-base-table sibling.
        final TableToken pendingToken = tokenOf("lv_stub_pending", 1);
        final TestInstance renamed = new TestInstance(
                pendingToken,
                () -> {
                    inGap.countDown();
                    awaitLatch(republished, "the third party's republish");
                }
        );
        registry.registerStubView(renamed);

        final TestInstance other = new TestInstance(tokenOf("lv_stub_other", 2), null);

        final Thread renamer = startRenamer(registry, "lv_stub_pending", pendingToken.renamed("lv_stub"), error);
        awaitLatch(inGap, "the stub rename to reach the remove/put gap");
        registry.registerStubView(other);
        republished.countDown();
        renamer.join(TimeUnit.SECONDS.toMillis(30));

        Assert.assertNull("the rename must not fail", error.get());
        Assert.assertSame("the stub rename must re-key the name map", renamed, registry.getViewInstance("lv_stub"));
        Assert.assertNull("the pending name must be dead", registry.getViewInstance("lv_stub_pending"));

        final ObjList<LiveViewInstance> sink = new ObjList<>();
        registry.getViews(sink);
        Assert.assertTrue("the renamed stub must survive a concurrent republish", hasInstance(sink, renamed));
        Assert.assertTrue(hasInstance(sink, other));
        Assert.assertEquals(2, sink.size());

        registry.clear();
        Assert.assertTrue("engine teardown must free the renamed stub", renamed.hasBeenClosed());
        Assert.assertTrue("engine teardown must free the other stub", other.hasBeenClosed());
    }

    @Test
    public void testSingleThreadedRenameKeepsSnapshotConsistentWithNameMap() {
        // The uncontended baseline the old reasoning covered: it must keep working.
        final LiveViewRegistry registry = new LiveViewRegistry();
        final TableToken pendingToken = tokenOf("lv_pending", 1);
        final TestInstance instance = new TestInstance(definitionOf("lv_pending", "base_a"), pendingToken, null);
        registry.registerView(instance);

        Assert.assertSame(instance, registry.renameView("lv_pending", pendingToken.renamed("lv")));

        final ObjList<LiveViewInstance> sink = new ObjList<>();
        registry.getViews(sink);
        Assert.assertEquals(1, sink.size());
        Assert.assertSame(instance, sink.getQuick(0));
        Assert.assertSame(instance, registry.getViewInstance("lv"));
        Assert.assertNull(registry.getViewInstance("lv_pending"));
        Assert.assertEquals("lv", instance.getDefinition().getViewName());
        Assert.assertEquals("lv", instance.getLiveViewToken().getTableName());

        registry.clear();
        Assert.assertTrue(instance.hasBeenClosed());
    }

    private static void awaitLatch(CountDownLatch latch, String what) {
        try {
            Assert.assertTrue("timed out waiting for " + what, latch.await(30, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted waiting for " + what, e);
        }
    }

    private static LiveViewDefinition definitionOf(String viewName, String baseTableName) {
        return new LiveViewDefinition(
                viewName,
                "SELECT * FROM " + baseTableName,
                baseTableName,
                tokenOf(baseTableName, 1_000),
                ColumnType.TIMESTAMP,
                1,
                's',
                1,
                's',
                PartitionBy.DAY,
                0,
                LiveViewDefinition.START_FROM_NOW,
                null,
                new ObjList<>(),
                new IntList(),
                new GenericRecordMetadata()
        );
    }

    private static boolean hasInstance(ObjList<LiveViewInstance> views, LiveViewInstance instance) {
        for (int i = 0, n = views.size(); i < n; i++) {
            if (views.getQuick(i) == instance) {
                return true;
            }
        }
        return false;
    }

    private static Thread startRenamer(
            LiveViewRegistry registry,
            String oldName,
            TableToken updatedToken,
            AtomicReference<Throwable> error
    ) {
        final Thread thread = new Thread(() -> {
            try {
                registry.renameView(oldName, updatedToken);
            } catch (Throwable th) {
                error.set(th);
            }
        }, "lv-renamer");
        thread.start();
        return thread;
    }

    private static TableToken tokenOf(String name, int tableId) {
        return new TableToken(name, name, null, tableId, false, false, false);
    }

    private static final class TestInstance extends LiveViewInstance {
        private final Runnable inGapHook;
        private volatile boolean hasBeenClosed;
        private volatile boolean hasHookFired;

        private TestInstance(LiveViewDefinition definition, TableToken token, Runnable inGapHook) {
            super(definition, token, token.getTableId(), false, LiveViewCheckpointOutputUniqueness.NO_KEY_COLUMN);
            this.inGapHook = inGapHook;
        }

        private TestInstance(TableToken token, Runnable inGapHook) {
            super(token, null, token.getTableId());
            this.inGapHook = inGapHook;
        }

        @Override
        public void close() {
            // Records the teardown free instead of running the real one: these instances carry no
            // refresh state, and what the test needs to observe is only whether clear() reached
            // this instance at all.
            hasBeenClosed = true;
        }

        @Override
        public void updateToken(TableToken updatedToken) {
            // renameView calls this between viewsByName.remove(oldName) and
            // viewsByName.put(newName, this), in both of its branches. Parking here parks the
            // rename inside the gap the finding is about.
            if (inGapHook != null && !hasHookFired) {
                hasHookFired = true;
                inGapHook.run();
            }
            super.updateToken(updatedToken);
        }

        private boolean hasBeenClosed() {
            return hasBeenClosed;
        }
    }
}

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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewStateStoreImpl;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Pins the retarget contract of {@code CairoEngine#loadMatViewIntoStore} (the private-target
 * hydrate walk shared by
 * {@link io.questdb.cairo.CairoEngine#hydrateMatViewStateStore(io.questdb.cairo.mv.MatViewStateStore)}):
 * every enqueue call the walk makes -- invalidation and kickstart alike -- must land on the
 * {@code target} parameter, not the engine's own installed {@code matViewStateStore} field, and a
 * null target must be rejected up front. Each test hydrates into a {@link MatViewStateStoreImpl}
 * instance DISTINCT from the engine's own installed store, so a revert that hardcodes a call back
 * onto the engine field leaves the recording target's map/set untouched and the assertion fails.
 */
public class MatViewHydrateTargetTest extends AbstractCairoTest {

    @Test
    public void hydrateRejectsNullTargetWithNoViewsOnDisk() throws Exception {
        assertMemoryLeak(() -> {
            try {
                engine.hydrateMatViewStateStore(null);
                Assert.fail("hydrate must reject a null target even with no views on disk");
            } catch (NullPointerException expected) {
                // expected
            }
        });
    }

    @Test
    public void missingBaseTableInvalidatesOnDistinctTarget() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_missing (sym symbol, val double, ts timestamp) " +
                    "timestamp(ts) partition by DAY WAL");
            execute("create materialized view mv_missing_base as (" +
                    "select ts, count() cnt from base_missing sample by 1h) partition by DAY");
            drainWalQueue();
            execute("drop table base_missing");
            drainWalQueue();

            final TableToken viewToken = engine.verifyTableName("mv_missing_base");
            final RecordingMatViewStateStore target = new RecordingMatViewStateStore(engine);
            try {
                engine.hydrateMatViewStateStore(target);
                Assert.assertEquals("base table does not exist", target.invalidationReasonFor(viewToken));
            } finally {
                target.close();
            }
        });
    }

    /**
     * Guards a bug that shipped on this branch (commit {@code 31404c9c9b}, "Enqueue hydration
     * kickstart on the target store"): the no-persisted-state branch's
     * {@code enqueueIncrementalRefresh} kept targeting the engine's {@code matViewStateStore}
     * field while every sibling retarget had already moved to {@code target}. Boot hydration
     * hides the mistake because there the two are the same object; a role-promote rehydrate
     * builds a fresh store and the kickstart lands on a store nobody installs, so a
     * never-refreshed view comes back valid, empty, watermark -1, and never converges to its
     * base table.
     * <p>
     * Not redundant with the pre-existing {@code MatViewTest} never-refreshed-kickstart test
     * ({@code MatViewTest.java:3400+}): that one goes through the no-arg path where
     * {@code target == field}, so it stayed green through the whole life of the bug -- only a
     * distinct recording target sees it. Fragile across an upstream merge: an upstream refactor
     * of {@code loadMatViewIntoStore} that reintroduces a field reference on this branch would
     * not be caught by any no-arg-path test, only by this one.
     */
    @Test
    public void neverRefreshedViewKickstartsOnDistinctTarget() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_neverref (sym symbol, val double, ts timestamp) " +
                    "timestamp(ts) partition by DAY WAL");
            execute("create materialized view mv_neverref as (" +
                    "select ts, count() cnt from base_neverref sample by 1h) partition by DAY");
            execute("insert into base_neverref values ('a', 1.0, '2024-09-10T12:00')");
            // Drain the WAL queue only, never the mat-view queue: the view must reach the hydrate
            // walk genuinely never-refreshed, so nothing persists a MAT_VIEW state event and
            // readMatViewState returns false.
            drainWalQueue();

            final TableToken viewToken = engine.verifyTableName("mv_neverref");
            final RecordingMatViewStateStore target = new RecordingMatViewStateStore(engine);
            try {
                engine.hydrateMatViewStateStore(target);
                Assert.assertTrue(
                        "the never-refreshed view's initial refresh must be kickstarted on the hydration target",
                        target.wasKickstarted(viewToken));
            } finally {
                target.close();
            }
        });
    }

    @Test
    public void nonWalBaseTableInvalidatesOnDistinctTarget() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_nonwal (sym symbol, val double, ts timestamp) " +
                    "timestamp(ts) partition by DAY WAL");
            execute("create materialized view mv_nonwal_base as (" +
                    "select ts, count() cnt from base_nonwal sample by 1h) partition by DAY");
            drainWalQueue();

            execute("alter table base_nonwal set type bypass wal");
            engine.releaseInactive();
            engine.load();
            Assert.assertFalse(
                    "the base table must be non-WAL after the conversion",
                    engine.verifyTableName("base_nonwal").isWal());

            final TableToken viewToken = engine.verifyTableName("mv_nonwal_base");
            final RecordingMatViewStateStore target = new RecordingMatViewStateStore(engine);
            try {
                engine.hydrateMatViewStateStore(target);
                Assert.assertEquals("base table is not WAL table", target.invalidationReasonFor(viewToken));
            } finally {
                target.close();
            }
        });
    }

    // Real MatViewStateStoreImpl instance, distinct from the engine's own installed store, that
    // records every enqueueInvalidate(...) and enqueueIncrementalRefresh(...) call before
    // delegating -- reusing the production create/get-state machinery so the hydrate walk's
    // baseTableExists/isWal/watermark checks all run exactly as they do against the engine's own
    // store.
    private static final class RecordingMatViewStateStore extends MatViewStateStoreImpl {
        private final Map<TableToken, String> invalidations = new HashMap<>();
        private final Set<TableToken> kickstarted = new HashSet<>();

        RecordingMatViewStateStore(CairoEngine engine) {
            super(engine);
        }

        @Override
        public void enqueueIncrementalRefresh(TableToken matViewToken) {
            kickstarted.add(matViewToken);
            super.enqueueIncrementalRefresh(matViewToken);
        }

        @Override
        public void enqueueInvalidate(TableToken matViewToken, String invalidationReason) {
            invalidations.put(matViewToken, invalidationReason);
            super.enqueueInvalidate(matViewToken, invalidationReason);
        }

        String invalidationReasonFor(TableToken matViewToken) {
            return invalidations.get(matViewToken);
        }

        boolean wasKickstarted(TableToken matViewToken) {
            return kickstarted.contains(matViewToken);
        }
    }
}

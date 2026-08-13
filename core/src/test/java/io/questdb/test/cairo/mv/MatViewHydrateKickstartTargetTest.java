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

import java.util.HashSet;
import java.util.Set;

/**
 * Pins the {@code target.enqueueIncrementalRefresh(...)} kickstart on the no-persisted-state branch of
 * {@code CairoEngine#loadMatViewIntoStore}, the sibling of the invalidation retargets covered by
 * {@link MatViewHydrateInvalidationTargetTest}. A view CREATEd but never refreshed has no persisted
 * MAT_VIEW state, so the hydrate walk must schedule its initial incremental refresh on the store it
 * is hydrating -- otherwise a role promote rebuilds a valid, empty, watermark -1 view that nothing
 * ever kickstarts.
 * <p>
 * The test hydrates into a {@link MatViewStateStoreImpl} instance DISTINCT from the engine's own
 * installed store, so a revert that enqueues onto the engine's {@code matViewStateStore} field
 * (instead of the {@code target} parameter) leaves the recording target untouched and the assertion
 * fails.
 */
public class MatViewHydrateKickstartTargetTest extends AbstractCairoTest {

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

    // Real MatViewStateStoreImpl instance, distinct from the engine's own installed store, that
    // records every enqueueIncrementalRefresh(...) call before delegating.
    private static final class RecordingMatViewStateStore extends MatViewStateStoreImpl {
        private final Set<TableToken> kickstarted = new HashSet<>();

        RecordingMatViewStateStore(CairoEngine engine) {
            super(engine);
        }

        @Override
        public void enqueueIncrementalRefresh(TableToken matViewToken) {
            kickstarted.add(matViewToken);
            super.enqueueIncrementalRefresh(matViewToken);
        }

        boolean wasKickstarted(TableToken matViewToken) {
            return kickstarted.contains(matViewToken);
        }
    }
}

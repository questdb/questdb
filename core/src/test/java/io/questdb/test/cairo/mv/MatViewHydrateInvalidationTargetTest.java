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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewStateStoreImpl;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Pins three of the four {@code target.enqueueInvalidate(...)} retargets inside
 * {@code CairoEngine#loadMatViewIntoStore} (the private-target hydrate walk shared by
 * {@link io.questdb.cairo.CairoEngine#hydrateMatViewStateStore(io.questdb.cairo.mv.MatViewStateStore)}):
 * missing base table, non-WAL base table, and a view whose persisted watermark is ahead of the
 * base table's current txn. The fourth retarget (a truncate found in the base WAL gap) is already
 * exercised elsewhere.
 * <p>
 * Each test hydrates into a {@link MatViewStateStoreImpl} instance that is DISTINCT from the
 * engine's own installed store, so a revert that hardcodes the branch back onto the engine's
 * {@code matViewStateStore} field (instead of the {@code target} parameter) leaves the recording
 * target's map untouched -- the assertion fails because the invalidation was never recorded on it.
 */
public class MatViewHydrateInvalidationTargetTest extends AbstractCairoTest {

    @Test
    public void aheadOfBaseInvalidatesOnDistinctTarget() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        assertMemoryLeak(() -> {
            execute("create table base_ahead (sym symbol, val double, ts timestamp) " +
                    "timestamp(ts) partition by DAY WAL");
            execute("create materialized view mv_ahead as (" +
                    "select ts, count() cnt from base_ahead sample by 1h) partition by DAY");
            for (int i = 0; i < 5; i++) {
                execute("insert into base_ahead values ('a', " + i + ".0, '2024-09-10T12:0" + i + "')");
            }
            // A real incremental refresh persists the view's watermark against the base's txn
            // count at this point (comfortably above the two empty txns a rebase reseeds below).
            drainWalAndMatViewQueues();

            final TableToken oldBase = engine.verifyTableName("base_ahead");
            execute("alter table base_ahead suspend wal");
            execute("alter table base_ahead rebase wal");
            final TableToken newBase = engine.verifyTableName("base_ahead");
            Assert.assertNotEquals(
                    "the rebase must give the base table a fresh identity",
                    oldBase.getDirName(), newBase.getDirName());
            // Deliberately do NOT drain the mat-view queue: the rebase enqueues an invalidate of
            // mv_ahead on the ENGINE's own installed store, but that task must stay pending and
            // unprocessed so the view's persisted watermark stays untouched -- otherwise the view
            // would already be invalid on disk before hydrate ever gets to the ahead-of-base check.

            final TableToken viewToken = engine.verifyTableName("mv_ahead");
            final RecordingMatViewStateStore target = new RecordingMatViewStateStore(engine);
            try {
                engine.hydrateMatViewStateStore(target);
                Assert.assertEquals(
                        "materialized view is ahead of base table and cannot be synchronized",
                        target.reasonFor(viewToken));
            } finally {
                target.close();
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
                Assert.assertEquals("base table does not exist", target.reasonFor(viewToken));
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
                Assert.assertEquals("base table is not WAL table", target.reasonFor(viewToken));
            } finally {
                target.close();
            }
        });
    }

    // Real MatViewStateStoreImpl instance, distinct from the engine's own installed store, that
    // records every enqueueInvalidate(...) call before delegating -- reusing the production
    // create/get-state machinery so the hydrate walk's baseTableExists/isWal/watermark checks all
    // run exactly as they do against the engine's own store.
    private static final class RecordingMatViewStateStore extends MatViewStateStoreImpl {
        private final Map<TableToken, String> invalidations = new HashMap<>();

        RecordingMatViewStateStore(CairoEngine engine) {
            super(engine);
        }

        @Override
        public void enqueueInvalidate(TableToken matViewToken, String invalidationReason) {
            invalidations.put(matViewToken, invalidationReason);
            super.enqueueInvalidate(matViewToken, invalidationReason);
        }

        String reasonFor(TableToken matViewToken) {
            return invalidations.get(matViewToken);
        }
    }
}

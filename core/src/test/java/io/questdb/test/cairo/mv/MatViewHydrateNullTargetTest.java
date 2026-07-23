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

import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@link io.questdb.cairo.CairoEngine#hydrateMatViewStateStore(io.questdb.cairo.mv.MatViewStateStore)}
 * must reject a null target up front. Without the check, a null target with zero views on disk
 * never dereferences the argument at all (the walk loop simply never runs), and a null target
 * with views present would NPE only inside {@code loadMatViewIntoStore}'s per-view
 * {@code catch (Throwable)}, which logs and swallows it -- so a null target could otherwise
 * silently produce an empty, un-hydrated store.
 */
public class MatViewHydrateNullTargetTest extends AbstractCairoTest {

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
    public void hydrateRejectsNullTargetWithViewsOnDisk() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_null_target (sym symbol, val double, ts timestamp) " +
                    "timestamp(ts) partition by DAY WAL");
            execute("create materialized view mv_null_target as (" +
                    "select ts, count() cnt from base_null_target sample by 1h) partition by DAY");
            drainWalQueue();
            try {
                engine.hydrateMatViewStateStore(null);
                Assert.fail("hydrate must reject a null target when views are present");
            } catch (NullPointerException expected) {
                // expected
            }
        });
    }
}

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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.NoOpMatViewStateStore;
import io.questdb.std.ObjHashSet;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Isolates the two closing aborts in {@link CairoEngine#hydrateMatViewStateStore}, each
 * against a revert the other would mask. Entry-time: a close that already won the
 * flag-publication race must see the loader abort BEFORE its first engine-state read (the
 * {@code getTableTokens} registry walk) -- a revert of only the entry check stays green
 * under the per-token poll but fails on the recorded registry walk. Per-token: a close
 * landing MID-walk must abort the remaining iterations -- every entry-check witness
 * publishes closing before the walk starts, so a revert of only the poll stays green
 * everywhere except here, where closing flips after the first view has already loaded.
 * <p>
 * The zero-argument {@link CairoEngine#hydrateMatViewStateStore()} delegator is not
 * separately witnessed here: its delegation to the engine's own store is already covered
 * by the pre-existing {@code MatViewTest} hydrate tests ({@code MatViewTest.java:3432},
 * {@code :3528}).
 */
public class MatViewHydrateClosingAbortTest extends AbstractCairoTest {

    @Test
    public void hydrateAbortsBeforeFirstRegistryReadWhenEngineIsClosing() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicBoolean closing = new AtomicBoolean();
            final AtomicBoolean registryWalked = new AtomicBoolean();
            try (CairoEngine closingEngine = new CairoEngine(configuration) {
                @Override
                public void getTableTokens(ObjHashSet<TableToken> bucket, boolean includeDropped) {
                    registryWalked.set(true);
                    super.getTableTokens(bucket, includeDropped);
                }

                @Override
                public boolean isClosing() {
                    // Armed only after construction: boot-time registry walks are legitimate,
                    // and close() at the end of the try must run against the real flag.
                    return closing.get() || super.isClosing();
                }
            }) {
                closing.set(true);
                registryWalked.set(false);
                try {
                    closingEngine.hydrateMatViewStateStore(NoOpMatViewStateStore.INSTANCE);
                    Assert.fail("hydrate must abort when the engine is closing");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "engine is closing; mat-view hydration aborted");
                }
                Assert.assertFalse(
                        "the entry-time abort must fire BEFORE the first registry read",
                        registryWalked.get());
                closing.set(false);
            }
        });
    }

    @Test
    public void perTokenPollAbortsWalkWhenCloseLandsMidHydrate() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_price (sym string, price double, ts timestamp) " +
                    "timestamp(ts) partition by DAY WAL");
            execute("create materialized view price_1h as (" +
                    "select sym, last(price) price, ts from base_price sample by 1h) partition by DAY");
            execute("create materialized view price_1d as (" +
                    "select sym, last(price) price, ts from base_price sample by 1d) partition by DAY");
            drainWalQueue();

            final AtomicBoolean closing = new AtomicBoolean();
            final AtomicBoolean registryWalked = new AtomicBoolean();
            final AtomicInteger created = new AtomicInteger();
            // Counting target: the first view load flips closing, so the abort can only come
            // from the per-token poll -- the entry check has already passed by then. With two
            // views on disk, whatever the token order, at least one iteration follows the
            // first createViewState, so the poll always gets its chance to fire.
            final NoOpMatViewStateStore countingTarget = new NoOpMatViewStateStore() {
                @Override
                public void createViewState(MatViewDefinition viewDefinition) {
                    created.incrementAndGet();
                    closing.set(true);
                }
            };
            try (CairoEngine closingEngine = new CairoEngine(configuration) {
                @Override
                public void getTableTokens(ObjHashSet<TableToken> bucket, boolean includeDropped) {
                    registryWalked.set(true);
                    super.getTableTokens(bucket, includeDropped);
                }

                @Override
                public boolean isClosing() {
                    // Armed only by the counting target above: boot-time hydration inside the
                    // constructor runs against the real flag.
                    return closing.get() || super.isClosing();
                }
            }) {
                registryWalked.set(false);
                try {
                    closingEngine.hydrateMatViewStateStore(countingTarget);
                    Assert.fail("hydrate must abort when the engine starts closing mid-walk");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "engine is closing; mat-view hydration aborted");
                }
                Assert.assertTrue("the walk must have started -- the entry check passed", registryWalked.get());
                Assert.assertEquals("the poll must abort the walk before the second view loads", 1, created.get());
                closing.set(false);
            }
        });
    }
}

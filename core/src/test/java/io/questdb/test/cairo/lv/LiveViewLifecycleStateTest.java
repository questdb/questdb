/*******************************************************************************
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

import io.questdb.cairo.lv.LiveViewLifecycleState;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit coverage for the live-view lifecycle state machine that feeds
 * {@code live_views().view_status}.
 * <p>
 * The {@code creating} and {@code dropping} labels are not observable through the
 * {@code live_views()} catalogue in any deterministic way, so they are locked here at the enum level
 * instead of via a SQL query:
 * <ul>
 *   <li><b>creating</b> - the registry entry is locked but not yet committed, so the view is not in
 *   the registry the catalogue enumerates ({@code LiveViewsFunctionFactory} reads
 *   {@code getLiveViewRegistry().getViews(...)}). {@link LiveViewLifecycleState#derive} cannot even
 *   produce {@code CREATING}, because its caller already holds a committed instance.</li>
 *   <li><b>dropping</b> - {@code CairoEngine.dropLiveView} calls {@code liveViewRegistry.removeView}
 *   before it flips the instance's dropped flag, so the instance leaves the enumerated registry
 *   before it would ever report {@code dropping}.</li>
 * </ul>
 * These tests therefore lock the state-derivation truth table and the exact catalogue label strings
 * (a rename would silently break operator dashboards and tooling that match on them).
 */
public class LiveViewLifecycleStateTest {

    @Test
    public void testCatalogueNamesAreStableLowerCase() {
        // The exact strings surfaced by live_views().view_status. Locks all six, including the two
        // transient/internal states that no SQL query can observe.
        Assert.assertEquals("creating", LiveViewLifecycleState.CREATING.catalogueName());
        Assert.assertEquals("active", LiveViewLifecycleState.ACTIVE.catalogueName());
        Assert.assertEquals("backfilling", LiveViewLifecycleState.BACKFILLING.catalogueName());
        Assert.assertEquals("invalid", LiveViewLifecycleState.INVALID.catalogueName());
        Assert.assertEquals("dropping", LiveViewLifecycleState.DROPPING.catalogueName());
        Assert.assertEquals("version_unsupported", LiveViewLifecycleState.VERSION_UNSUPPORTED.catalogueName());
    }

    @Test
    public void testDeriveActiveAndBackfilling() {
        // Registry-visible, valid: the backfill signal alone chooses BACKFILLING vs ACTIVE.
        Assert.assertEquals(LiveViewLifecycleState.ACTIVE, LiveViewLifecycleState.derive(true, false, false));
        Assert.assertEquals(LiveViewLifecycleState.BACKFILLING, LiveViewLifecycleState.derive(true, false, true));
    }

    @Test
    public void testDeriveDroppingWhenNotRegistryVisible() {
        // A not-registry-visible (marked-dropped) instance is DROPPING regardless of the other signals.
        // This is the sole producer of DROPPING, hence the authoritative check for the dropping label.
        Assert.assertEquals(LiveViewLifecycleState.DROPPING, LiveViewLifecycleState.derive(false, false, false));
        Assert.assertEquals(LiveViewLifecycleState.DROPPING, LiveViewLifecycleState.derive(false, true, false));
        Assert.assertEquals(LiveViewLifecycleState.DROPPING, LiveViewLifecycleState.derive(false, false, true));
        Assert.assertEquals(LiveViewLifecycleState.DROPPING, LiveViewLifecycleState.derive(false, true, true));
    }

    @Test
    public void testDeriveInvalidTakesPrecedenceOverBackfilling() {
        // A registry-visible, invalid instance is INVALID even if the backfill signal is still set.
        Assert.assertEquals(LiveViewLifecycleState.INVALID, LiveViewLifecycleState.derive(true, true, false));
        Assert.assertEquals(LiveViewLifecycleState.INVALID, LiveViewLifecycleState.derive(true, true, true));
    }
}

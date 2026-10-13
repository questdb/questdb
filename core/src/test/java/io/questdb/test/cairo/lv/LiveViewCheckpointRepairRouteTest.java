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

import io.questdb.cairo.lv.LiveViewCheckpointKeyedReplay;
import io.questdb.cairo.lv.LiveViewCheckpointOutputUniqueness;
import io.questdb.cairo.lv.LiveViewCheckpointRepairRoute;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.std.Numbers;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.lv.LiveViewCheckpointRepairRoute.*;

/**
 * The repair route as a value: what a decision records, what survives a park, and the one
 * change an executor may make to it. Narrow unit tests over
 * {@link LiveViewCheckpointRepairRoute}; the routes the refresh job actually takes are
 * asserted beside the scenarios that drive them, through
 * {@code LiveViewRefreshJob.repairRouteCountForTest} and {@code repairRouteForTest}.
 */
public class LiveViewCheckpointRepairRouteTest {

    @Test
    public void testAClearedRouteHasNoDecisionAndNoKeyDomain() {
        final LiveViewCheckpointRepairRoute route = decidedColdRoute();
        route.setOpenSegmentKeyDomainReady(true);
        route.clear();
        Assert.assertFalse(route.isOpenSegmentKeyDomainReady());
        Assert.assertEquals(SCOPE_NONE, route.getScope());
        assertUndecided(route);
    }

    @Test
    public void testAClosedSegmentRouteReadsWholeOnceItsKeyedScanIsDeclined() {
        final LiveViewCheckpointRepairRoute route = new LiveViewCheckpointRepairRoute();
        route.ofScope(SCOPE_SEGMENT);
        route.ofHeadMissScope(true, true, 10, 20, 30);
        route.ofRoute(ROUTE_KEYED_CLOSED_SEGMENT, RUNTIME_ISOLATED, true, 0);
        Assert.assertTrue(route.isKeyedScan());
        Assert.assertTrue(route.isKeyedClosedSegment());
        Assert.assertTrue(route.hasKeyedReplay());

        route.declineKeyedScan("the replay's base scan does not admit an indexed substitution");
        Assert.assertEquals(ROUTE_SEGMENT_REBUILD, route.getRoute());
        Assert.assertFalse(route.isKeyedScan());
        Assert.assertFalse(route.isKeyedClosedSegment());
        Assert.assertFalse("nothing keyed is left for a park to hand over", route.hasKeyedReplay());
        // The rest of the decision stands: the segment is still read on the runtime and
        // inside the bounds the route was decided with.
        Assert.assertTrue(route.isIsolated());
        Assert.assertTrue(route.isTimelineSpliceable());
        Assert.assertEquals(10, route.getScanLowTs());
        Assert.assertEquals(20, route.getEmitLowTs());
        Assert.assertEquals(30, route.getTimelineHighTsExclusive());
    }

    @Test
    public void testAColdRouteKeepsItsRuntimeWhenItsKeyedScanIsDeclined() {
        // The cold route set up an isolated runtime that starts at the segment's origin, so a
        // late decline reads the segment whole on that runtime and still hands its keys back.
        final LiveViewCheckpointRepairRoute route = decidedColdRoute();
        Assert.assertTrue(route.isKeyedScan());
        route.declineKeyedScan("no scan of the view's own table");
        Assert.assertEquals(ROUTE_KEYED_COLD_OPEN_SEGMENT, route.getRoute());
        Assert.assertTrue(route.isColdOpenSegment());
        Assert.assertFalse(route.isKeyedScan());
        Assert.assertFalse(route.isKeyedClosedSegment());
        Assert.assertTrue("the hand-back still needs the replay", route.hasKeyedReplay());
        Assert.assertTrue(route.isIsolated());
        Assert.assertEquals("no scan of the view's own table", route.getKeyedScanDeclineReason().toString());
    }

    @Test
    public void testANewScopeForgetsTheDecisionButKeepsTheKeyDomain() {
        // The segment loop and the residual behind it decide one after the other on the
        // same change set, so the residual still reads the domain the decomposition
        // classified while every segment's decision is gone.
        final LiveViewCheckpointRepairRoute route = decidedColdRoute();
        route.setOpenSegmentKeyDomainReady(true);
        Assert.assertSame(route, route.ofScope(SCOPE_CHANGE_SET));
        Assert.assertTrue(route.isOpenSegmentKeyDomainReady());
        Assert.assertEquals(SCOPE_CHANGE_SET, route.getScope());
        assertUndecided(route);
    }

    @Test
    public void testAParkedRouteCarriesItsDecisionButNotTheKeyDomain() {
        // The session's copy is what a resumed turn continues on. The key domain belongs to
        // whichever turn classified it, and the resuming turn has not, so neither direction
        // of the copy moves it.
        final LiveViewCheckpointRepairRoute decided = decidedColdRoute();
        decided.setOpenSegmentKeyDomainReady(true);
        decided.declineKeyedScan("no scan of the view's own table");
        decided.ofOpenSegmentPricing(true, true);

        final LiveViewCheckpointRepairRoute parked = new LiveViewCheckpointRepairRoute();
        parked.copyDecisionFrom(decided);
        Assert.assertFalse(parked.isOpenSegmentKeyDomainReady());
        Assert.assertEquals(SCOPE_NONE, parked.getScope());

        final LiveViewCheckpointRepairRoute resumed = new LiveViewCheckpointRepairRoute();
        resumed.ofScope(SCOPE_RESUMED);
        resumed.copyDecisionFrom(parked);
        resumed.ofResumedRuntime(true);
        Assert.assertEquals(SCOPE_RESUMED, resumed.getScope());
        Assert.assertFalse(resumed.isOpenSegmentKeyDomainReady());
        Assert.assertEquals(ROUTE_KEYED_COLD_OPEN_SEGMENT, resumed.getRoute());
        Assert.assertEquals(RUNTIME_ISOLATED, resumed.getRuntime());
        Assert.assertFalse(resumed.isKeyedScan());
        Assert.assertTrue(resumed.isColdOpenSegment());
        Assert.assertEquals(42, resumed.getInsertedRowDelta());
        Assert.assertTrue(resumed.isLocalized());
        Assert.assertFalse(resumed.isFiniteHighBound());
        Assert.assertTrue(resumed.isTimelineSpliceable());
        Assert.assertEquals(100, resumed.getScanLowTs());
        Assert.assertEquals(200, resumed.getEmitLowTs());
        Assert.assertEquals(Long.MAX_VALUE, resumed.getTimelineHighTsExclusive());
        Assert.assertTrue(resumed.isOpenSegmentKeyedScanCheaper());
        Assert.assertTrue(resumed.isOpenSegmentRestoreAwareCheaper());
    }

    @Test
    public void testAReplayThatMadeNoSparseAttemptPublishesByReplacement() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final LiveViewCheckpointRepairRoute route = new LiveViewCheckpointRepairRoute();
            route.ofRoute(ROUTE_RANGE_REBUILD, RUNTIME_PRIMARY_REPLACED, false, 0);
            Assert.assertEquals(PUBLICATION_NONE, route.getPublication());
            final LiveViewCheckpointOutputUniqueness uniqueness = new LiveViewCheckpointOutputUniqueness();
            uniqueness.of(0);
            try (LiveViewCheckpointKeyedReplay replay = new LiveViewCheckpointKeyedReplay()) {
                // Rows and unique pairs are not enough on their own: the replay never bound
                // its output as a sparse attempt.
                route.decidePublication(replay, 10, uniqueness);
                Assert.assertFalse(route.isSparsePublication());
                Assert.assertEquals(PUBLICATION_REPLACE_RANGE, route.getPublication());
            }
            route.ofScope(SCOPE_VIEW);
            Assert.assertEquals(PUBLICATION_NONE, route.getPublication());
            route.ofReplacePublication();
            Assert.assertEquals(PUBLICATION_REPLACE_RANGE, route.getPublication());
        });
    }

    @Test
    public void testAResumeRouteRecordsItsBaselineAndRuntime() {
        final LiveViewCheckpointRepairRoute route = new LiveViewCheckpointRepairRoute();
        route.ofScope(SCOPE_CHANGE_SET);
        route.ofKeyedResumeBaseline(1_000, 2_000, 7);
        route.ofResumeRoute(ROUTE_KEYED_RESUME_FROM_ANCHOR, RUNTIME_ISOLATED, true);
        Assert.assertTrue(route.isResumeFromAnchor());
        Assert.assertTrue(route.isKeyedScan());
        Assert.assertTrue(route.hasKeyedReplay());
        Assert.assertFalse(route.isKeyedClosedSegment());
        Assert.assertFalse(route.isColdOpenSegment());
        Assert.assertTrue(route.isRuntimeAnchorReusable());
        Assert.assertEquals(1_000, route.getKeyedResumeDurableRows());
        Assert.assertEquals(2_000, route.getKeyedResumeDurableMaxTs());
        Assert.assertEquals(7, route.getInsertedRowDelta());

        route.ofScope(SCOPE_CHANGE_SET);
        route.ofResumeRoute(ROUTE_RESUME_FROM_ANCHOR, RUNTIME_PRIMARY_REUSED, true);
        Assert.assertTrue(route.isResumeFromAnchor());
        Assert.assertFalse(route.isKeyedScan());
        Assert.assertFalse(route.hasKeyedReplay());
        Assert.assertEquals(Numbers.LONG_NULL, route.getKeyedResumeDurableRows());
        Assert.assertEquals(Numbers.LONG_NULL, route.getKeyedResumeDurableMaxTs());
        Assert.assertEquals(0, route.getInsertedRowDelta());
    }

    @Test
    public void testAResumedPrimaryRouteKeepsTheRuntimeItsFirstTurnDecided() {
        final LiveViewCheckpointRepairRoute route = new LiveViewCheckpointRepairRoute();
        route.ofRoute(ROUTE_RANGE_REBUILD, RUNTIME_PRIMARY_COPY_ASIDE, false, 0);
        route.ofResumedRuntime(false);
        Assert.assertEquals(RUNTIME_PRIMARY_COPY_ASIDE, route.getRuntime());
        Assert.assertFalse(route.isIsolated());
    }

    @Test
    public void testEveryRouteAndRuntimeHasAName() {
        for (int route = ROUTE_VIEW_REBUILD; route < ROUTE_COUNT; route++) {
            Assert.assertNotEquals("route " + route, "none", routeName(route));
        }
        Assert.assertEquals("none", routeName(ROUTE_NONE));
        for (int runtime = RUNTIME_PRIMARY_REPLACED; runtime <= RUNTIME_ISOLATED; runtime++) {
            Assert.assertNotEquals("runtime " + runtime, "none", runtimeName(runtime));
        }
        Assert.assertEquals("none", runtimeName(RUNTIME_NONE));
    }

    /**
     * Asserts the route the job decided last, by name, so a failure reads as the route that
     * was taken rather than as a number.
     */
    static void assertLastRepairRoute(LiveViewRefreshJob job, int route, int runtime, int publication) {
        final LiveViewCheckpointRepairRoute decided = job.repairRouteForTest();
        Assert.assertEquals(routeName(route), routeName(decided.getRoute()));
        Assert.assertEquals(runtimeName(runtime), runtimeName(decided.getRuntime()));
        Assert.assertEquals(publication, decided.getPublication());
    }

    private static void assertUndecided(LiveViewCheckpointRepairRoute route) {
        Assert.assertEquals(ROUTE_NONE, route.getRoute());
        Assert.assertEquals(RUNTIME_NONE, route.getRuntime());
        Assert.assertEquals(PUBLICATION_NONE, route.getPublication());
        Assert.assertFalse(route.isKeyedScan());
        Assert.assertFalse(route.hasKeyedReplay());
        Assert.assertFalse(route.isLocalized());
        Assert.assertFalse(route.isFiniteHighBound());
        Assert.assertFalse(route.isTimelineSpliceable());
        Assert.assertFalse(route.isOpenSegmentKeyedScanCheaper());
        Assert.assertFalse(route.isOpenSegmentRestoreAwareCheaper());
        Assert.assertFalse(route.isRuntimeAnchorReusable());
        Assert.assertNull(route.getKeyedScanDeclineReason());
        Assert.assertEquals(0, route.getInsertedRowDelta());
        Assert.assertEquals(Numbers.LONG_NULL, route.getScanLowTs());
        Assert.assertEquals(Numbers.LONG_NULL, route.getEmitLowTs());
        Assert.assertEquals(Long.MAX_VALUE, route.getTimelineHighTsExclusive());
        Assert.assertEquals(Numbers.LONG_NULL, route.getKeyedResumeDurableRows());
        Assert.assertEquals(Numbers.LONG_NULL, route.getKeyedResumeDurableMaxTs());
    }

    private static LiveViewCheckpointRepairRoute decidedColdRoute() {
        final LiveViewCheckpointRepairRoute route = new LiveViewCheckpointRepairRoute();
        route.ofScope(SCOPE_CHANGE_SET);
        route.ofHeadMissScope(true, false, 100, 200, Long.MAX_VALUE);
        route.ofRoute(ROUTE_KEYED_COLD_OPEN_SEGMENT, RUNTIME_ISOLATED, true, 42);
        return route;
    }
}

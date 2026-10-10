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

import io.questdb.cairo.lv.LiveViewCheckpointSegmentLoop;
import io.questdb.std.DirectIntList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Coverage for the loop position itself, and specifically for the key domain it carries.
 * <p>
 * A parked loop is the only thing that outlives the turn that planned it. The segment
 * bounds have travelled on it since the yield shipped; {@code Q} - the keys one segment's
 * correction touched - travels on it because the change set those come out of is worker
 * scratch the next classification refills. The cases below are about that: the domain has
 * to be copied rather than referenced, it has to survive both legs of the park (scratch to
 * session on the way in, session to scratch on the way out), and it has to stay attached
 * to the segment it belongs to while the queue drains from its head.
 * <p>
 * The loop itself holds no native memory, but the change set it copies a domain from
 * keeps its keys in native lists, so the cases that build one run under
 * {@code assertMemoryLeak}.
 */
public class LiveViewCheckpointSegmentLoopTest {

    @Test
    public void testASegmentWithNoKeyDomainCarriesNone() {
        // The cost model turned this segment down, so the loop holds no keys for it and
        // the repair reads it whole. The null-key flag has to go with them: a segment with
        // no domain has no null key either, or a keyed scan would run over that one key
        // alone and repair nothing else.
        final LiveViewCheckpointSegmentLoop loop = new LiveViewCheckpointSegmentLoop();
        loop.ofChangeSet(0, 1, 2, 100, 200, Numbers.LONG_NULL, Numbers.LONG_NULL, true, 2);
        loop.addSegment(10, 11, 12, null, true);

        loop.removeFirstSegment();

        Assert.assertEquals(10, loop.getInFlightSegmentStart());
        Assert.assertNull(loop.getInFlightKeys());
        Assert.assertFalse(loop.hasInFlightNullKey());
    }

    @Test
    public void testAnUnpricedSegmentAheadOfAPricedOneKeepsThePricedOnesKeys() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (DirectIntList source = keys(7)) {
                assertAnUnpricedSegmentAheadOfAPricedOneKeepsThePricedOnesKeys(source);
            }
        });
    }

    @Test
    public void testTheKeyDomainIsCopiedRatherThanReferenced() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (DirectIntList source = keys(1, 2)) {
                assertTheKeyDomainIsCopiedRatherThanReferenced(source);
            }
        });
    }

    @Test
    public void testTheKeyDomainSurvivesBothLegsOfAPark() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    DirectIntList first = keys(1);
                    DirectIntList second = keys(2, 3);
                    DirectIntList other = keys(99)
            ) {
                assertTheKeyDomainSurvivesBothLegsOfAPark(first, second, other);
            }
        });
    }

    private static void assertAnUnpricedSegmentAheadOfAPricedOneKeepsThePricedOnesKeys(DirectIntList source) {
        // The fixture the production shape produces, and the one that leaves a gap in the
        // pool of key sets: the loop's first segment carries no domain and the one behind
        // it does. Everything that walks the pool - the copy across the park, the clear
        // that precedes the next fill - walks all of it.
        final LiveViewCheckpointSegmentLoop parked = new LiveViewCheckpointSegmentLoop();
        parked.ofChangeSet(0, 1, 2, 100, 200, Numbers.LONG_NULL, Numbers.LONG_NULL, true, 2);
        parked.addSegment(10, 11, 12, null, false);
        parked.addSegment(20, 21, 22, source, false);
        // The first segment is taken off the queue and parks mid-replay.
        parked.removeFirstSegment();

        final LiveViewCheckpointSegmentLoop resumed = new LiveViewCheckpointSegmentLoop();
        resumed.copyFrom(parked);
        resumed.segmentRepaired();
        resumed.removeFirstSegment();

        Assert.assertEquals(20, resumed.getInFlightSegmentStart());
        Assert.assertNotNull(resumed.getInFlightKeys());
        Assert.assertEquals(1, resumed.getInFlightKeys().size());
        Assert.assertEquals(7, resumed.getInFlightKeys().getQuick(0));
    }

    private static void assertTheKeyDomainIsCopiedRatherThanReferenced(DirectIntList source) {
        // The change set a loop takes its keys from is refilled by the next repair the
        // worker classifies, so a loop holding the set itself would arm against whatever
        // that turn collected. Clearing the source after the add is what a refill looks
        // like from here.
        final LiveViewCheckpointSegmentLoop loop = new LiveViewCheckpointSegmentLoop();
        loop.ofChangeSet(0, 1, 2, 100, 200, Numbers.LONG_NULL, Numbers.LONG_NULL, true, 2);
        loop.addSegment(10, 11, 12, source, true);

        source.clear();
        source.add(9);
        loop.removeFirstSegment();

        final IntList carried = loop.getInFlightKeys();
        Assert.assertNotNull(carried);
        Assert.assertEquals(2, carried.size());
        Assert.assertEquals(1, carried.getQuick(0));
        Assert.assertEquals(2, carried.getQuick(1));
        Assert.assertTrue(loop.hasInFlightNullKey());
    }

    private static void assertTheKeyDomainSurvivesBothLegsOfAPark(
            DirectIntList first,
            DirectIntList second,
            DirectIntList other
    ) {
        // Scratch to session on the way in, session back to scratch on the way out. Both
        // legs are a copyFrom, and the scratch on either end is reused by every repair the
        // worker plans, so a leg that referenced rather than copied would hand the resuming
        // turn a domain something else had since overwritten.
        final LiveViewCheckpointSegmentLoop scratch = new LiveViewCheckpointSegmentLoop();
        scratch.ofChangeSet(0, 1, 2, 100, 200, 30, 40, true, 2);
        scratch.addSegment(10, 11, 12, first, false);
        scratch.addSegment(20, 21, 22, second, true);
        scratch.removeFirstSegment();

        final LiveViewCheckpointSegmentLoop session = new LiveViewCheckpointSegmentLoop();
        session.copyFrom(scratch);
        // The worker plans something else entirely against the same scratch.
        scratch.ofChangeSet(0, 5, 6, 500, 600, Numbers.LONG_NULL, Numbers.LONG_NULL, false, 6);
        scratch.addSegment(90, 91, 92, other, false);
        scratch.copyFrom(session);

        Assert.assertEquals(10, scratch.getInFlightSegmentStart());
        Assert.assertEquals(1, scratch.size());
        Assert.assertEquals(20, scratch.getSegmentStart(0));
        Assert.assertEquals(30, scratch.getResidualMinTs());
        final IntList inFlight = scratch.getInFlightKeys();
        Assert.assertNotNull(inFlight);
        Assert.assertEquals(1, inFlight.size());
        Assert.assertEquals(1, inFlight.getQuick(0));

        scratch.segmentRepaired();
        scratch.removeFirstSegment();
        final IntList next = scratch.getInFlightKeys();
        Assert.assertNotNull(next);
        Assert.assertEquals(2, next.size());
        Assert.assertEquals(2, next.getQuick(0));
        Assert.assertEquals(3, next.getQuick(1));
        Assert.assertTrue(scratch.hasInFlightNullKey());
    }

    private static DirectIntList keys(int... values) {
        final DirectIntList list = new DirectIntList(values.length, MemoryTag.NATIVE_DEFAULT);
        for (int value : values) {
            list.add(value);
        }
        return list;
    }
}

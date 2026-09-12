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
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.Numbers;
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
 * A pure-Java carrier holding no native memory, so no {@code assertMemoryLeak}.
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
    public void testAnUnpricedSegmentAheadOfAPricedOneKeepsThePricedOnesKeys() {
        // The fixture the production shape produces, and the one that leaves a gap in the
        // pool of key sets: the loop's first segment carries no domain and the one behind
        // it does. Everything that walks the pool - the copy across the park, the clear
        // that precedes the next fill - walks all of it.
        final LiveViewCheckpointSegmentLoop parked = new LiveViewCheckpointSegmentLoop();
        parked.ofChangeSet(0, 1, 2, 100, 200, Numbers.LONG_NULL, Numbers.LONG_NULL, true, 2);
        parked.addSegment(10, 11, 12, null, false);
        parked.addSegment(20, 21, 22, keys("acct-7"), false);
        // The first segment is taken off the queue and parks mid-replay.
        parked.removeFirstSegment();

        final LiveViewCheckpointSegmentLoop resumed = new LiveViewCheckpointSegmentLoop();
        resumed.copyFrom(parked);
        resumed.segmentRepaired();
        resumed.removeFirstSegment();

        Assert.assertEquals(20, resumed.getInFlightSegmentStart());
        Assert.assertNotNull(resumed.getInFlightKeys());
        Assert.assertEquals(1, resumed.getInFlightKeys().size());
        Assert.assertTrue(resumed.getInFlightKeys().contains("acct-7"));
    }

    @Test
    public void testTheKeyDomainIsCopiedRatherThanReferenced() {
        // The change set a loop takes its keys from is refilled by the next repair the
        // worker classifies, so a loop holding the set itself would arm against whatever
        // that turn collected. Clearing the source after the add is what a refill looks
        // like from here.
        final CharSequenceHashSet source = keys("acct-1", "acct-2");
        final LiveViewCheckpointSegmentLoop loop = new LiveViewCheckpointSegmentLoop();
        loop.ofChangeSet(0, 1, 2, 100, 200, Numbers.LONG_NULL, Numbers.LONG_NULL, true, 2);
        loop.addSegment(10, 11, 12, source, true);

        source.clear();
        source.add("acct-9");
        loop.removeFirstSegment();

        final CharSequenceHashSet carried = loop.getInFlightKeys();
        Assert.assertNotNull(carried);
        Assert.assertEquals(2, carried.size());
        Assert.assertTrue(carried.contains("acct-1"));
        Assert.assertTrue(carried.contains("acct-2"));
        Assert.assertFalse(carried.contains("acct-9"));
        Assert.assertTrue(loop.hasInFlightNullKey());
    }

    @Test
    public void testTheKeyDomainSurvivesBothLegsOfAPark() {
        // Scratch to session on the way in, session back to scratch on the way out. Both
        // legs are a copyFrom, and the scratch on either end is reused by every repair the
        // worker plans, so a leg that referenced rather than copied would hand the resuming
        // turn a domain something else had since overwritten.
        final LiveViewCheckpointSegmentLoop scratch = new LiveViewCheckpointSegmentLoop();
        scratch.ofChangeSet(0, 1, 2, 100, 200, 30, 40, true, 2);
        scratch.addSegment(10, 11, 12, keys("acct-1"), false);
        scratch.addSegment(20, 21, 22, keys("acct-2", "acct-3"), true);
        scratch.removeFirstSegment();

        final LiveViewCheckpointSegmentLoop session = new LiveViewCheckpointSegmentLoop();
        session.copyFrom(scratch);
        // The worker plans something else entirely against the same scratch.
        scratch.ofChangeSet(0, 5, 6, 500, 600, Numbers.LONG_NULL, Numbers.LONG_NULL, false, 6);
        scratch.addSegment(90, 91, 92, keys("other"), false);
        scratch.copyFrom(session);

        Assert.assertEquals(10, scratch.getInFlightSegmentStart());
        Assert.assertEquals(1, scratch.size());
        Assert.assertEquals(20, scratch.getSegmentStart(0));
        Assert.assertEquals(30, scratch.getResidualMinTs());
        final CharSequenceHashSet inFlight = scratch.getInFlightKeys();
        Assert.assertNotNull(inFlight);
        Assert.assertTrue(inFlight.contains("acct-1"));

        scratch.segmentRepaired();
        scratch.removeFirstSegment();
        final CharSequenceHashSet next = scratch.getInFlightKeys();
        Assert.assertNotNull(next);
        Assert.assertEquals(2, next.size());
        Assert.assertTrue(next.contains("acct-2"));
        Assert.assertTrue(next.contains("acct-3"));
        Assert.assertTrue(scratch.hasInFlightNullKey());
    }

    private static CharSequenceHashSet keys(CharSequence... values) {
        final CharSequenceHashSet set = new CharSequenceHashSet();
        for (CharSequence value : values) {
            set.add(value);
        }
        return set;
    }
}

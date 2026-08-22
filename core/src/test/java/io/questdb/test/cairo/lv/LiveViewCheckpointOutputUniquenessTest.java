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

import io.questdb.cairo.lv.LiveViewCheckpointOutputUniqueness;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

/**
 * Coverage for the detector itself: whether a repair's qualifying output carries each
 * {@code (designated timestamp, projected partition key)} pair once.
 * <p>
 * The verdict has to be <b>exact</b> rather than conservative, and in both directions. A
 * false "duplicate" costs a sparse publication that could have been taken; a missed one
 * would have a sparse commit collapse two rows into one silently, which no reader detects
 * and which also corrupts the cadence ladder, whose cumulative positions count live-view
 * rows. So the cases below are about the three ways a cheaper check would be wrong: two
 * rows of one group whose keys are not adjacent, one key seen again under a different
 * timestamp, and a group split across the park a repair takes on its turn budget.
 * <p>
 * A pure-Java detector holding no native memory, so no {@code assertMemoryLeak}.
 */
public class LiveViewCheckpointOutputUniquenessTest {
    private static final int KEY_COLUMN = 1;

    @Test
    public void testADisarmedDetectorObservesNothing() {
        // A view whose output carries no key the pair can be named through. It is not a
        // denial of anything - the repair publishes its whole range as always - so the
        // detector has to stay quiet rather than answer for output it never saw.
        final LiveViewCheckpointOutputUniqueness uniqueness = new LiveViewCheckpointOutputUniqueness();
        uniqueness.of(LiveViewCheckpointOutputUniqueness.NO_KEY_COLUMN);

        Assert.assertFalse(uniqueness.isArmed());
        Assert.assertTrue(uniqueness.observe(1_000, 7));
        Assert.assertTrue(uniqueness.observe(1_000, 7));
        Assert.assertEquals(0, uniqueness.getCheckedRows());
        Assert.assertEquals(0, uniqueness.getDuplicateRows());
        Assert.assertEquals(0, uniqueness.getMaxGroupRows());
    }

    @Test
    public void testAKeyRepeatedUnderAnotherTimestampIsNotADuplicate() {
        // The pair is the identity, not the key. A key with one row per timestamp is
        // exactly the shape a sparse publication exists for, and a detector that remembered
        // keys across groups would reject every real view.
        final LiveViewCheckpointOutputUniqueness uniqueness = armed();

        for (int i = 0; i < 5; i++) {
            Assert.assertTrue(uniqueness.observe(1_000 + i, 7));
        }

        Assert.assertTrue(uniqueness.isUnique());
        Assert.assertEquals(5, uniqueness.getCheckedRows());
        Assert.assertEquals(1, uniqueness.getMaxGroupRows());
        Assert.assertEquals(Numbers.LONG_NULL, uniqueness.getFirstDuplicateTs());
        Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, uniqueness.getFirstDuplicateKey());
    }

    @Test
    public void testANonAdjacentRepeatInsideOneGroupIsFound() {
        // The case an adjacency comparison misses, and the reason the scratch is a set: the
        // rows of one timestamp arrive in whatever order the base holds them, so two rows
        // sharing a key need not be neighbours.
        final LiveViewCheckpointOutputUniqueness uniqueness = armed();

        Assert.assertTrue(uniqueness.observe(1_000, 7));
        Assert.assertTrue(uniqueness.observe(1_000, 9));
        Assert.assertTrue(uniqueness.observe(1_000, 11));
        Assert.assertFalse(uniqueness.observe(1_000, 7));

        Assert.assertFalse(uniqueness.isUnique());
        Assert.assertEquals(1, uniqueness.getDuplicateRows());
        Assert.assertEquals(1_000, uniqueness.getFirstDuplicateTs());
        Assert.assertEquals(7, uniqueness.getFirstDuplicateKey());
        Assert.assertEquals(4, uniqueness.getMaxGroupRows());
    }

    @Test
    public void testAdjacentRowsOfOneKeyAreADuplicate() {
        // Two qualifying rows of one key at one timestamp: the plain shape a sparse commit
        // would collapse. The first duplicate is recorded and the rest are counted.
        final LiveViewCheckpointOutputUniqueness uniqueness = armed();

        Assert.assertTrue(uniqueness.observe(1_000, 7));
        Assert.assertFalse(uniqueness.observe(1_000, 7));
        Assert.assertFalse(uniqueness.observe(1_000, 7));

        Assert.assertEquals(2, uniqueness.getDuplicateRows());
        Assert.assertEquals(3, uniqueness.getCheckedRows());
        Assert.assertEquals(1_000, uniqueness.getFirstDuplicateTs());
        Assert.assertEquals(7, uniqueness.getFirstDuplicateKey());
    }

    @Test
    public void testEqualTimestampsOnDistinctKeysAreNotDuplicates() {
        // A base stamping whole seconds puts every account's row of one instant into one
        // group, which the measured production shape does. That is a wide group, not a
        // duplicate, and reporting it as one would rule sparse publication out everywhere.
        final LiveViewCheckpointOutputUniqueness uniqueness = armed();

        for (int key = 0; key < 8; key++) {
            Assert.assertTrue(uniqueness.observe(1_000, key));
        }
        Assert.assertTrue(uniqueness.observe(2_000, 0));

        Assert.assertTrue(uniqueness.isUnique());
        Assert.assertEquals(9, uniqueness.getCheckedRows());
        Assert.assertEquals(8, uniqueness.getMaxGroupRows());
    }

    @Test
    public void testOfRearmsFromScratch() {
        // One worker's detector serves every repair it runs, so arming has to leave nothing
        // of the previous one behind - a carried group would make the next repair's first
        // row a duplicate of a repair that has already published.
        final LiveViewCheckpointOutputUniqueness uniqueness = armed();
        Assert.assertTrue(uniqueness.observe(1_000, 7));
        Assert.assertFalse(uniqueness.observe(1_000, 7));

        uniqueness.of(KEY_COLUMN);

        Assert.assertTrue(uniqueness.isArmed());
        Assert.assertTrue(uniqueness.isUnique());
        Assert.assertEquals(0, uniqueness.getCheckedRows());
        Assert.assertEquals(0, uniqueness.getMaxGroupRows());
        Assert.assertTrue(uniqueness.observe(1_000, 7));

        uniqueness.clear();

        Assert.assertFalse(uniqueness.isArmed());
        Assert.assertEquals(0, uniqueness.getCheckedRows());
        Assert.assertEquals(LiveViewCheckpointOutputUniqueness.NO_KEY_COLUMN, uniqueness.getKeyColumnIndex());
    }

    @Test
    public void testTheCheckSurvivesAPark() {
        // The carrier claim. A repair that spends its turn budget mid-replay hands its
        // state to the session and the turn that resumes puts it back, so a duplicate whose
        // two rows sit on either side of the park is still a duplicate. The group the park
        // stopped inside is the only place that can happen, and it is the group a resumed
        // turn re-enters by construction.
        final LiveViewCheckpointOutputUniqueness parked = armed();
        Assert.assertTrue(parked.observe(1_000, 7));
        Assert.assertTrue(parked.observe(1_000, 9));

        final LiveViewCheckpointOutputUniqueness resumed = new LiveViewCheckpointOutputUniqueness();
        resumed.copyFrom(parked);

        Assert.assertTrue(resumed.isArmed());
        Assert.assertEquals(KEY_COLUMN, resumed.getKeyColumnIndex());
        Assert.assertEquals(2, resumed.getCheckedRows());
        Assert.assertEquals(2, resumed.getMaxGroupRows());
        Assert.assertFalse("the group the park stopped inside must come back with it", resumed.observe(1_000, 7));
        Assert.assertEquals(1, resumed.getDuplicateRows());

        // The control: a detector re-armed instead of carried sees the same row as the
        // first of its group and calls the repair unique. That is the defect this carries
        // against, and it is silent.
        final LiveViewCheckpointOutputUniqueness rearmed = new LiveViewCheckpointOutputUniqueness();
        rearmed.of(KEY_COLUMN);
        Assert.assertTrue(rearmed.observe(1_000, 7));
        Assert.assertTrue(rearmed.isUnique());
    }

    @Test
    public void testTheCopyIsNotAReference() {
        // The session outlives the turn that parked; the worker's own detector is re-armed
        // by the next repair it classifies. A copy that shared the group would have that
        // re-arming empty the state a parked repair is standing on.
        final LiveViewCheckpointOutputUniqueness parked = armed();
        parked.observe(1_000, 7);
        parked.observe(1_000, 9);

        final LiveViewCheckpointOutputUniqueness carried = new LiveViewCheckpointOutputUniqueness();
        carried.copyFrom(parked);
        parked.of(KEY_COLUMN);
        parked.observe(5_000, 3);

        Assert.assertEquals(2, carried.getCheckedRows());
        Assert.assertFalse(carried.observe(1_000, 9));
    }

    @Test
    public void testTheNullKeyIsAKeyLikeAnyOther() {
        // The null account is a partition key of its own, and its integer is INT_NULL
        // rather than a value near the others. It must neither collide with the scratch's
        // own empty-slot marker nor be exempt from the check.
        final LiveViewCheckpointOutputUniqueness uniqueness = armed();

        Assert.assertTrue(uniqueness.observe(1_000, SymbolTable.VALUE_IS_NULL));
        Assert.assertTrue(uniqueness.observe(1_000, 0));
        Assert.assertFalse(uniqueness.observe(1_000, SymbolTable.VALUE_IS_NULL));

        Assert.assertEquals(1, uniqueness.getDuplicateRows());
        Assert.assertEquals(SymbolTable.VALUE_IS_NULL, uniqueness.getFirstDuplicateKey());
    }

    private static LiveViewCheckpointOutputUniqueness armed() {
        final LiveViewCheckpointOutputUniqueness uniqueness = new LiveViewCheckpointOutputUniqueness();
        uniqueness.of(KEY_COLUMN);
        return uniqueness;
    }
}

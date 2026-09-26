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
import io.questdb.std.AbstractIntHashSet;
import io.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

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
 * The cost has a contract too. One detector serves every repair its worker runs, and its
 * scratch table keeps the size the widest group it ever walked grew it to. The cases that
 * reach into that table check that emptying it stays proportional to the keys being
 * dropped, which is the only structural witness of that cost a test can hold without a
 * clock.
 * <p>
 * A pure-Java detector holding no native memory, so no {@code assertMemoryLeak}.
 */
public class LiveViewCheckpointOutputUniquenessTest {
    private static final int EMPTY_SLOT = -1;
    private static final int KEY_COLUMN = 1;
    private static final int WIDE_GROUP_ROWS = 4_096;

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
    public void testANarrowGroupPaysOnlyForItsOwnKeys() throws Exception {
        // The cost contract. A whole-second snapshot of many accounts grows the scratch
        // table for good, and one worker's detector carries that table into every later
        // repair it runs, for every view. Emptying the table by sweeping all of it charged
        // each later group of two for the widest group the worker ever saw - once per
        // closed group, and once more per re-arm and per resumed park. The slot stamped
        // below lies outside every cluster the narrow groups take, so only a sweep of the
        // whole table can reach it.
        final LiveViewCheckpointOutputUniqueness uniqueness = widened();
        final int[] table = groupKeyTable(uniqueness);
        Assert.assertTrue("the wide group must have grown the table past its keys", table.length > WIDE_GROUP_ROWS);
        final int untouchedSlot = table.length / 2;
        Assert.assertEquals(EMPTY_SLOT, table[untouchedSlot]);
        table[untouchedSlot] = untouchedSlot;

        // Groups of two within the same repair: each close drops the two keys it took.
        Assert.assertTrue(uniqueness.observe(2_000, 1));
        for (int ts = 2_001; ts < 2_100; ts++) {
            Assert.assertTrue(uniqueness.observe(ts, 0));
            Assert.assertTrue(uniqueness.observe(ts, 1));
        }
        Assert.assertTrue(uniqueness.isUnique());
        Assert.assertEquals(WIDE_GROUP_ROWS + 200, uniqueness.getCheckedRows());
        Assert.assertEquals(WIDE_GROUP_ROWS, uniqueness.getMaxGroupRows());
        Assert.assertEquals(
                "closing a group of two swept the table an earlier, wider group grew",
                untouchedSlot,
                groupKeyTable(uniqueness)[untouchedSlot]
        );

        // The next repair on the same worker re-arms while the last group still holds two
        // keys.
        uniqueness.of(KEY_COLUMN);
        Assert.assertEquals(
                "re-arming for the next repair swept the table an earlier, wider group grew",
                untouchedSlot,
                groupKeyTable(uniqueness)[untouchedSlot]
        );

        // A resumed park copies its group over whatever group this worker last walked.
        Assert.assertTrue(uniqueness.observe(5_000, 0));
        Assert.assertTrue(uniqueness.observe(5_000, 1));
        final LiveViewCheckpointOutputUniqueness parked = armed();
        Assert.assertTrue(parked.observe(9_000, 7));
        Assert.assertTrue(parked.observe(9_000, 9));
        uniqueness.copyFrom(parked);
        Assert.assertEquals(
                "resuming a park swept the table an earlier, wider group grew",
                untouchedSlot,
                groupKeyTable(uniqueness)[untouchedSlot]
        );
        Assert.assertTrue(uniqueness.observe(9_000, 0));
        Assert.assertFalse("the parked group must come back with the resume", uniqueness.observe(9_000, 9));
        Assert.assertEquals(9_000, uniqueness.getFirstDuplicateTs());
        Assert.assertEquals(9, uniqueness.getFirstDuplicateKey());
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
    public void testAParkCarriesOnlyTheGroupItStoppedInside() {
        // The scratch keeps its keys in a list beside its table, and the copy a park makes
        // takes as many listed keys as the table holds. A close that emptied the table but
        // not the list would hand the park a closed group's keys in place of the group it
        // stopped inside: a pair repeated across the park would pass as unique, and a key of
        // the closed group would come back as a false duplicate. The wide group closes by
        // sweeping the table and the group of two after it by erasing just its clusters, so
        // a park follows each kind of close.
        final LiveViewCheckpointOutputUniqueness uniqueness = widened();
        Assert.assertTrue(uniqueness.observe(2_000, 5));
        final LiveViewCheckpointOutputUniqueness parkedAfterSweep = new LiveViewCheckpointOutputUniqueness();
        parkedAfterSweep.copyFrom(uniqueness);
        Assert.assertFalse(
                "a park after a sweeping close must carry the group it stopped inside",
                parkedAfterSweep.observe(2_000, 5)
        );
        Assert.assertTrue(
                "a park after a sweeping close must leave the closed group's keys behind",
                parkedAfterSweep.observe(2_000, 1)
        );

        Assert.assertTrue(uniqueness.observe(3_000, 7));
        Assert.assertTrue(uniqueness.observe(3_000, 9));
        final LiveViewCheckpointOutputUniqueness parkedAfterErase = new LiveViewCheckpointOutputUniqueness();
        parkedAfterErase.copyFrom(uniqueness);
        Assert.assertFalse(
                "a park after a cluster-erasing close must carry the group it stopped inside",
                parkedAfterErase.observe(3_000, 9)
        );
        Assert.assertTrue(
                "a park after a cluster-erasing close must leave the closed group's keys behind",
                parkedAfterErase.observe(3_000, 5)
        );
    }

    @Test
    public void testAStrayKeyFallsBackToTheFullClear() throws Exception {
        // Emptying the table without sweeping all of it assumes each key sits in the
        // cluster that runs from its home slot. A key found anywhere else - which only a
        // change to the set's probe could produce - must not survive its group, or a later
        // group would be told that key is a duplicate. The close counts the slots it empties
        // and sweeps the whole table when that count comes up short.
        final LiveViewCheckpointOutputUniqueness uniqueness = widened();
        Assert.assertTrue(uniqueness.observe(2_000, 1));
        final int[] table = groupKeyTable(uniqueness);
        Assert.assertEquals(1, table[1]);
        final int strandedSlot = 5;
        Assert.assertEquals(EMPTY_SLOT, table[strandedSlot]);
        table[1] = EMPTY_SLOT;
        table[strandedSlot] = 1;

        Assert.assertTrue(uniqueness.observe(3_000, 1));

        Assert.assertEquals(
                "a key its home cluster did not hold must still leave with its group",
                EMPTY_SLOT,
                groupKeyTable(uniqueness)[strandedSlot]
        );
        Assert.assertTrue(uniqueness.observe(3_000, 0));
        Assert.assertFalse(uniqueness.observe(3_000, 1));
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
    public void testKeysSharingAClusterLeaveWithTheirGroup() throws Exception {
        // Keys that collide on a home slot pile into one cluster, and a cluster can run off
        // the end of the table and wrap to its start. The null key and the not-found key
        // are the sentinels most likely to do that: INT_NULL's home is slot zero and -2's is
        // the last slot but one. However the close empties the table, a key left behind in
        // such a cluster would make the same key in the next group a false duplicate, and a
        // real repeat inside that group must still be found. The stamped slot shows the
        // closes emptied those clusters themselves rather than sweeping the whole table.
        final LiveViewCheckpointOutputUniqueness uniqueness = widened();
        final int mask = groupKeyTable(uniqueness).length - 1;
        final int untouchedSlot = (mask + 1) / 2;
        Assert.assertEquals(EMPTY_SLOT, groupKeyTable(uniqueness)[untouchedSlot]);
        groupKeyTable(uniqueness)[untouchedSlot] = untouchedSlot;
        final int[] keys = {
                0,
                SymbolTable.VALUE_NOT_FOUND,
                SymbolTable.VALUE_IS_NULL,
                2 * mask,
                mask + 1,
                3 * mask + 1
        };
        Assert.assertTrue(uniqueness.observe(2_000, keys[1]));
        for (int i = 2; i < keys.length; i++) {
            Assert.assertTrue(uniqueness.observe(2_000, keys[i]));
        }
        final int[] table = groupKeyTable(uniqueness);
        Assert.assertNotEquals("the group must fill the table's last slot", EMPTY_SLOT, table[mask]);
        Assert.assertNotEquals("the group must wrap its cluster to the table's first slot", EMPTY_SLOT, table[0]);

        for (int i = keys.length - 1; i > -1; i--) {
            Assert.assertTrue("no key may outlive its group: " + keys[i], uniqueness.observe(3_000, keys[i]));
        }
        Assert.assertFalse(uniqueness.observe(3_000, SymbolTable.VALUE_IS_NULL));
        for (int key : keys) {
            Assert.assertTrue("no key may outlive its group: " + key, uniqueness.observe(4_000, key));
        }
        Assert.assertFalse(uniqueness.observe(4_000, SymbolTable.VALUE_NOT_FOUND));

        Assert.assertEquals(2, uniqueness.getDuplicateRows());
        Assert.assertEquals(3_000, uniqueness.getFirstDuplicateTs());
        Assert.assertEquals(SymbolTable.VALUE_IS_NULL, uniqueness.getFirstDuplicateKey());
        Assert.assertEquals(
                "closing a wrapped cluster swept the table an earlier, wider group grew",
                untouchedSlot,
                groupKeyTable(uniqueness)[untouchedSlot]
        );
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

    // The detector's scratch table, read afresh on every call so that an assertion never
    // stands on an array the set has since replaced.
    private static int[] groupKeyTable(
            LiveViewCheckpointOutputUniqueness uniqueness
    ) throws ReflectiveOperationException {
        final Field groupKeys = LiveViewCheckpointOutputUniqueness.class.getDeclaredField("groupKeys");
        groupKeys.setAccessible(true);
        final Field keys = AbstractIntHashSet.class.getDeclaredField("keys");
        keys.setAccessible(true);
        return (int[]) keys.get(groupKeys.get(uniqueness));
    }

    // An armed detector that has walked one wide group of distinct keys and closed it with
    // the first row, key 0, of the group at timestamp 2_000.
    private static LiveViewCheckpointOutputUniqueness widened() {
        final LiveViewCheckpointOutputUniqueness uniqueness = armed();
        for (int key = 0; key < WIDE_GROUP_ROWS; key++) {
            Assert.assertTrue(uniqueness.observe(1_000, key));
        }
        Assert.assertTrue(uniqueness.observe(2_000, 0));
        Assert.assertTrue(uniqueness.isUnique());
        return uniqueness;
    }
}

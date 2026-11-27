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

package io.questdb.test.griffin;

import io.questdb.griffin.OuterJoinOrderingEdges;
import io.questdb.std.IntHashSet;
import org.junit.Assert;
import org.junit.Test;

/**
 * The expected edges follow from the definitions: a join that pins its whole prefix has every model before it
 * as a parent, a join with unpinned models every other model before it, and a recorded non-equi join is a parent
 * of every later consumer, pending until the consumer edges are in force.
 */
public class OuterJoinOrderingEdgesTest {

    @Test
    public void testClearTrimsPartialArenaPastThreshold() {
        final OuterJoinOrderingEdges edges = new OuterJoinOrderingEdges();
        final int initialCapacity = edges.getPartialPinWordCapacity();
        // 3,998 joins that leave model 1 unpinned: the block of ordinals 64w..64w+63 holds a word per model below
        // its last join, 64w + 65, and the last block, of 30 joins, a word per model below 3,999
        edges.of(4_000);
        pinPrefixesExceptModel1(edges, 2, 3_999);
        Assert.assertEquals(32 * 62 * 61 + 65 * 62 + 3_999, edges.getPartialPinWordCount());
        Assert.assertEquals(129_053, edges.getPartialPinWordCount());
        Assert.assertTrue(edges.getPartialPinWordCapacity() > 65_536);
        edges.clear();
        Assert.assertEquals(initialCapacity, edges.getPartialPinWordCapacity());
        Assert.assertEquals(0, edges.getPartialPinWordCount());
        Assert.assertEquals(0, edges.getPinCount());

        // 2,048 such joins fill 32 blocks, 33,824 words, which the doubling of a LongList that grows a word at a
        // time, or a block at a time, holds in exactly 65,536: clear() keeps that capacity
        edges.of(2_100);
        pinPrefixesExceptModel1(edges, 2, 2_049);
        Assert.assertEquals(32 * 32 * 31 + 65 * 32, edges.getPartialPinWordCount());
        Assert.assertEquals(65_536, edges.getPartialPinWordCapacity());
        edges.clear();
        Assert.assertEquals(65_536, edges.getPartialPinWordCapacity());
        Assert.assertFalse(edges.isEdge(0, 2, true));
    }

    @Test
    public void testConsumerEdgeThatIsAlsoPinCountsOnce() {
        final OuterJoinOrderingEdges edges = new OuterJoinOrderingEdges();
        edges.of(8);
        edges.pinWholePrefix(4);
        Assert.assertTrue(edges.pinPrefix(5, set(1)));
        edges.addNullingJoinConsumer(4);
        edges.addNullingJoinConsumer(5);
        edges.addNullingJoinBoundary(1);
        edges.addNullingJoinBoundary(2);
        edges.putConsumerEdgesInForce();
        // the consumer edges 1 -> 4 and 2 -> 4 are pins of 4 as well, and 2 -> 5 a pin of 5
        Assert.assertEquals(4, edges.getParentCount(4));
        Assert.assertEquals("[0, 1, 2, 3]", parents(edges, 4, false));
        // 5 leaves 1 unpinned, which the consumer edge makes a parent
        Assert.assertEquals(5, edges.getParentCount(5));
        Assert.assertEquals("[0, 2, 3, 4, 1]", parents(edges, 5, false));
        Assert.assertEquals("[4, 5]", dependents(edges, 1));
        Assert.assertEquals("[4, 5]", dependents(edges, 2));
        Assert.assertEquals("[4, 5]", dependents(edges, 0));
        Assert.assertEquals(4 + 4 + 1, edges.getEdgeCount());
        Assert.assertEquals(edges.getEdgeCount(), countEdges(edges, 8));
    }

    @Test
    public void testConsumerEdgesPendingUntilInForce() {
        final OuterJoinOrderingEdges edges = new OuterJoinOrderingEdges();
        edges.of(8);
        edges.addNullingJoinConsumer(1);
        edges.addNullingJoinConsumer(3);
        edges.addNullingJoinConsumer(5);
        edges.addNullingJoinConsumer(6);
        Assert.assertTrue(edges.hasNullingJoinConsumerAfter(5));
        Assert.assertFalse(edges.hasNullingJoinConsumerAfter(6));
        edges.addNullingJoinBoundary(2);
        edges.addNullingJoinBoundary(4);

        Assert.assertFalse(edges.isEdge(2, 3, false));
        Assert.assertTrue(edges.isEdge(2, 3, true));
        Assert.assertFalse(edges.isEdge(4, 3, true));
        // consumer 1 comes before the first boundary
        Assert.assertFalse(edges.hasParents(1, true));
        Assert.assertFalse(edges.hasParents(5, false));
        Assert.assertTrue(edges.hasParents(5, true));
        Assert.assertEquals("[]", parents(edges, 6, false));
        Assert.assertEquals("[2, 4]", parents(edges, 6, true));
        Assert.assertEquals(0, edges.getParentCount(6));
        Assert.assertFalse(edges.hasDependents(2));
        Assert.assertEquals("[]", dependents(edges, 2));
        Assert.assertEquals(0, edges.getEdgeCount());

        edges.putConsumerEdgesInForce();
        Assert.assertTrue(edges.isEdge(2, 3, false));
        Assert.assertFalse(edges.isEdge(3, 5, false));
        Assert.assertEquals(2, edges.getParentCount(6));
        Assert.assertEquals(1, edges.getParentCount(3));
        Assert.assertEquals(0, edges.getParentCount(1));
        Assert.assertEquals("[2, 4]", parents(edges, 6, false));
        Assert.assertEquals("[3, 5, 6]", dependents(edges, 2));
        Assert.assertEquals("[5, 6]", dependents(edges, 4));
        Assert.assertTrue(edges.hasDependents(4));
        Assert.assertFalse(edges.hasDependents(3));
        Assert.assertFalse(edges.hasDependents(6));
        // the children with a parent in force
        for (int c = 0; c < 8; c++) {
            Assert.assertEquals("child " + c, c == 3 || c == 5 || c == 6, edges.hasParents(c, false));
        }
        Assert.assertEquals(1 + 2 + 2, edges.getEdgeCount());
        Assert.assertEquals(edges.getEdgeCount(), countEdges(edges, 8));
        // a boundary after the last consumer is a parent of none
        edges.addNullingJoinBoundary(7);
        Assert.assertFalse(edges.hasDependents(7));
        Assert.assertEquals("[]", dependents(edges, 7));
        Assert.assertEquals(1 + 2 + 2, edges.getEdgeCount());
    }

    @Test
    public void testEdgeCount() {
        final OuterJoinOrderingEdges edges = new OuterJoinOrderingEdges();
        edges.of(10);
        edges.pinWholePrefix(4);
        Assert.assertTrue(edges.pinPrefix(5, set(1, 3)));
        edges.addNullingJoinConsumer(2);
        edges.addNullingJoinConsumer(4);
        edges.addNullingJoinConsumer(5);
        edges.addNullingJoinConsumer(6);
        edges.addNullingJoinBoundary(1);
        edges.addNullingJoinBoundary(3);
        // the pins only, while the consumer edges are pending
        Assert.assertEquals(4 + 3, edges.getEdgeCount());
        Assert.assertEquals(4 + 3, edges.getPinCount());
        edges.putConsumerEdgesInForce();
        // 1 -> 2; none into 4, which pins 1 and 3; 1 -> 5 and 3 -> 5, which 5 leaves unpinned; 1 -> 6 and 3 -> 6
        Assert.assertEquals(4 + 3 + 1 + 2 + 2, edges.getEdgeCount());
        Assert.assertEquals(edges.getEdgeCount(), countEdges(edges, 10));
        Assert.assertEquals(4 + 3, edges.getPinCount());
    }

    @Test
    public void testEmptyLevelReadsNothing() {
        final OuterJoinOrderingEdges edges = new OuterJoinOrderingEdges();
        final int capacity = edges.getBufferCapacity();
        edges.of(1_000);
        edges.putConsumerEdgesInForce();
        for (int i = 0; i < 1_000; i++) {
            Assert.assertFalse(edges.hasParents(i, true));
            Assert.assertFalse(edges.hasDependents(i));
            Assert.assertFalse(edges.isWholePrefixPinned(i));
            Assert.assertFalse(edges.hasNullingJoinConsumerAfter(i));
            Assert.assertEquals(0, edges.getParentCount(i));
            edges.ofParents(i, true);
            Assert.assertEquals(-1, edges.nextParent());
            edges.ofDependents(i);
            Assert.assertEquals(-1, edges.nextDependent());
        }
        Assert.assertFalse(edges.isEdge(0, 999, true));
        Assert.assertEquals(0, edges.getEdgeCount());
        Assert.assertEquals(0, edges.getPinCount());
        Assert.assertEquals(capacity, edges.getBufferCapacity());
    }

    @Test
    public void testOfStartsEmptyLevel() {
        final OuterJoinOrderingEdges edges = new OuterJoinOrderingEdges();
        edges.of(200);
        edges.pinWholePrefix(3);
        edges.pinWholePrefix(150);
        pinPrefixesExceptModel1(edges, 4, 140);
        edges.addNullingJoinConsumer(160);
        edges.addNullingJoinBoundary(150);
        edges.putConsumerEdgesInForce();
        Assert.assertTrue(edges.isEdge(150, 160, false));

        edges.of(10);
        Assert.assertEquals(0, edges.getPinCount());
        Assert.assertEquals(0, edges.getPartialPinWordCount());
        Assert.assertEquals(0, edges.getEdgeCount());
        for (int i = 0; i < 10; i++) {
            Assert.assertFalse(edges.hasParents(i, true));
            Assert.assertFalse(edges.hasDependents(i));
            Assert.assertEquals("[]", parents(edges, i, true));
            Assert.assertEquals("[]", dependents(edges, i));
        }
        Assert.assertFalse(edges.isEdge(0, 3, true));
        Assert.assertFalse(edges.hasNullingJoinConsumerAfter(0));

        // the words of the earlier level hold set bits: a new block starts from zero
        Assert.assertTrue(edges.pinPrefix(6, set(2)));
        Assert.assertTrue(edges.pinPrefix(8, set(0, 7)));
        Assert.assertEquals("[0, 1, 3, 4, 5]", parents(edges, 6, false));
        Assert.assertEquals("[1, 2, 3, 4, 5, 6]", parents(edges, 8, false));
        Assert.assertEquals("[6]", dependents(edges, 0));
        Assert.assertEquals("[8]", dependents(edges, 6));
        Assert.assertEquals("[]", dependents(edges, 7));
        Assert.assertEquals(5 + 6, edges.getPinCount());
        Assert.assertEquals(8, edges.getPartialPinWordCount());
        Assert.assertEquals(5 + 6, countEdges(edges, 10));
    }

    @Test
    public void testPartialPinsAcrossBlocks() {
        final OuterJoinOrderingEdges edges = new OuterJoinOrderingEdges();
        edges.of(200);
        // ordinals 0 to 129: blocks 0 and 1 full, block 2 of two joins
        pinPrefixesExceptModel1(edges, 2, 131);
        for (int b = 2; b <= 131; b++) {
            for (int p = 0; p < 200; p++) {
                Assert.assertEquals("edge " + p + " -> " + b, p < b && p != 1, edges.isEdge(p, b, false));
            }
            Assert.assertEquals(b - 1, edges.getParentCount(b));
        }
        final StringBuilder expected = new StringBuilder("[");
        for (int b = 2; b <= 131; b++) {
            expected.append(b > 2 ? ", " : "").append(b);
        }
        Assert.assertEquals(expected.append(']').toString(), dependents(edges, 0));
        Assert.assertEquals("[]", dependents(edges, 1));
        Assert.assertFalse(edges.hasDependents(1));
        Assert.assertEquals("[131]", dependents(edges, 130));
        Assert.assertFalse(edges.hasDependents(131));
        expected.setLength(0);
        expected.append("[0");
        for (int p = 2; p < 131; p++) {
            expected.append(", ").append(p);
        }
        Assert.assertEquals(expected.append(']').toString(), parents(edges, 131, false));
        // a word per model below the last join of each block: 65, 129 and 131
        Assert.assertEquals(65 + 129 + 131, edges.getPartialPinWordCount());
        // every join from 2 to 131 pins the models before it except model 1
        Assert.assertEquals(130 * 131 / 2, edges.getPinCount());
        Assert.assertEquals(edges.getPinCount(), countEdges(edges, 200));
    }

    @Test
    public void testPartialPrefixPins() {
        final OuterJoinOrderingEdges edges = new OuterJoinOrderingEdges();
        edges.of(8);
        Assert.assertTrue(edges.pinPrefix(5, set(1, 3)));
        Assert.assertEquals("[0, 2, 4]", parents(edges, 5, false));
        Assert.assertEquals(3, edges.getParentCount(5));
        Assert.assertFalse(edges.isEdge(1, 5, false));
        Assert.assertTrue(edges.isEdge(2, 5, false));
        Assert.assertFalse(edges.isEdge(5, 5, false));
        Assert.assertFalse(edges.isWholePrefixPinned(5));
        Assert.assertEquals("[]", dependents(edges, 1));
        Assert.assertEquals("[5]", dependents(edges, 2));
        Assert.assertTrue(edges.hasDependents(4));
        Assert.assertFalse(edges.hasDependents(5));

        // every model before 6 unpinned: no pin
        Assert.assertFalse(edges.pinPrefix(6, set(0, 1, 2, 3, 4, 5)));
        Assert.assertFalse(edges.hasParents(6, true));
        Assert.assertEquals(0, edges.getParentCount(6));
        Assert.assertEquals("[]", parents(edges, 6, true));
        Assert.assertEquals(3, edges.getPinCount());
        Assert.assertEquals(5, edges.getPartialPinWordCount());

        // nothing unpinned: the whole prefix
        Assert.assertTrue(edges.pinPrefix(7, set()));
        Assert.assertTrue(edges.isWholePrefixPinned(7));
        Assert.assertEquals(3 + 7, edges.getPinCount());
        // the joins that pin the whole prefix come first
        Assert.assertEquals("[7, 5]", dependents(edges, 2));
        Assert.assertEquals("[7]", dependents(edges, 1));
        Assert.assertEquals(3 + 7, countEdges(edges, 8));
    }

    @Test
    public void testRepeatedLevelReusesBuffers() {
        final OuterJoinOrderingEdges edges = new OuterJoinOrderingEdges();
        recordLevel(edges);
        final int capacity = edges.getBufferCapacity();
        final int pinWordCapacity = edges.getPartialPinWordCapacity();
        edges.clear();
        recordLevel(edges);
        Assert.assertEquals(capacity, edges.getBufferCapacity());
        Assert.assertEquals(pinWordCapacity, edges.getPartialPinWordCapacity());
    }

    @Test
    public void testWholePrefixPins() {
        final OuterJoinOrderingEdges edges = new OuterJoinOrderingEdges();
        edges.of(10);
        edges.pinWholePrefix(4);
        for (int p = 0; p < 10; p++) {
            Assert.assertEquals("edge " + p + " -> 4", p < 4, edges.isEdge(p, 4, false));
        }
        Assert.assertFalse(edges.isEdge(0, 3, true));
        Assert.assertTrue(edges.isWholePrefixPinned(4));
        Assert.assertFalse(edges.isWholePrefixPinned(3));
        Assert.assertEquals(4, edges.getParentCount(4));
        Assert.assertEquals("[0, 1, 2, 3]", parents(edges, 4, false));
        Assert.assertTrue(edges.hasDependents(3));
        Assert.assertFalse(edges.hasDependents(4));
        Assert.assertEquals("[4]", dependents(edges, 2));
        Assert.assertEquals(4, edges.getPinCount());
        // constrainRightAndFullJoinsAfterPrefix and recordNullingJoinPrefix both pin a non-equi join
        edges.pinWholePrefix(4);
        Assert.assertEquals(4, edges.getPinCount());
        Assert.assertEquals("[4]", dependents(edges, 2));
        Assert.assertEquals(4, countEdges(edges, 10));
        Assert.assertEquals(0, edges.getPartialPinWordCount());
    }

    @Test
    public void testWholePrefixPinsAcrossWords() {
        final OuterJoinOrderingEdges edges = new OuterJoinOrderingEdges();
        edges.of(130);
        edges.pinWholePrefix(63);
        edges.pinWholePrefix(64);
        edges.pinWholePrefix(65);
        edges.pinWholePrefix(127);
        edges.pinWholePrefix(128);
        Assert.assertEquals("[63, 64, 65, 127, 128]", dependents(edges, 62));
        Assert.assertEquals("[65, 127, 128]", dependents(edges, 64));
        Assert.assertEquals("[128]", dependents(edges, 127));
        Assert.assertFalse(edges.hasDependents(128));
        Assert.assertTrue(edges.hasDependents(127));
        Assert.assertEquals(128, edges.getParentCount(128));
        Assert.assertFalse(edges.hasParents(129, true));
        Assert.assertEquals(63 + 64 + 65 + 127 + 128, edges.getPinCount());
        Assert.assertEquals(edges.getPinCount(), countEdges(edges, 130));
    }

    // the edges in force, pair by pair
    private static long countEdges(OuterJoinOrderingEdges edges, int modelCount) {
        long count = 0;
        for (int c = 0; c < modelCount; c++) {
            for (int p = 0; p < modelCount; p++) {
                if (edges.isEdge(p, c, false)) {
                    count++;
                }
            }
        }
        return count;
    }

    private static String dependents(OuterJoinOrderingEdges edges, int parentIndex) {
        final StringBuilder sb = new StringBuilder("[");
        edges.ofDependents(parentIndex);
        for (int c = edges.nextDependent(); c != -1; c = edges.nextDependent()) {
            sb.append(sb.length() > 1 ? ", " : "").append(c);
        }
        // the cursor stays at its end
        Assert.assertEquals(-1, edges.nextDependent());
        return sb.append(']').toString();
    }

    private static String parents(OuterJoinOrderingEdges edges, int childIndex, boolean isPendingIncluded) {
        final StringBuilder sb = new StringBuilder("[");
        edges.ofParents(childIndex, isPendingIncluded);
        for (int p = edges.nextParent(); p != -1; p = edges.nextParent()) {
            sb.append(sb.length() > 1 ? ", " : "").append(p);
        }
        Assert.assertEquals(-1, edges.nextParent());
        return sb.append(']').toString();
    }

    private static void pinPrefixesExceptModel1(OuterJoinOrderingEdges edges, int fromBoundary, int toBoundary) {
        final IntHashSet unpinned = set(1);
        for (int b = fromBoundary; b <= toBoundary; b++) {
            Assert.assertTrue(edges.pinPrefix(b, unpinned));
        }
    }

    private static void recordLevel(OuterJoinOrderingEdges edges) {
        edges.of(300);
        edges.pinWholePrefix(10);
        pinPrefixesExceptModel1(edges, 11, 200);
        edges.addNullingJoinConsumer(250);
        edges.addNullingJoinBoundary(220);
        edges.pinWholePrefix(220);
        edges.putConsumerEdgesInForce();
        Assert.assertEquals(10 + (199 * 200 / 2 - 9 * 10 / 2) + 220 + 1, edges.getEdgeCount());
    }

    private static IntHashSet set(int... values) {
        final IntHashSet set = new IntHashSet();
        for (int v : values) {
            set.add(v);
        }
        return set;
    }
}

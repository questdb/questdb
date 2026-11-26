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

package io.questdb.griffin;

import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Mutable;
import org.jetbrains.annotations.TestOnly;

/**
 * The ordering edges that RIGHT and FULL OUTER joins add to the join level that SqlOptimiser orders, each
 * stored once. A RIGHT or FULL join takes its whole logical prefix as its master, so every model before it is
 * an ordering parent of the join, a pin, except the models that a forward reference leaves unpinned. A
 * non-equi RIGHT or FULL join also runs before every later join that consumes its rows, a consumer edge.
 * One edge per pair in the join contexts and dependencies made a chain of such joins keep a quadratic
 * number of ints in the pooled sets and lists of each compiler. The store keeps instead:
 * <ul>
 *     <li>a bit per join whose whole prefix it pins;</li>
 *     <li>for a join that leaves some prefix models unpinned, a bit per prefix model, parent-major in blocks
 *     of 64 such joins: the word of a parent in a block holds a bit for each join of the block that the
 *     parent runs before, so the dependents of a model take one word per block to read;</li>
 *     <li>the consumer edges as a rule: every recorded non-equi join, see
 *     {@link #addNullingJoinBoundary(int)}, is a parent of every later consumer, see
 *     {@link #addNullingJoinConsumer(int)}.</li>
 * </ul>
 * Consumer edges stay pending until {@link #putConsumerEdgesInForce()}: SqlOptimiser applies them at its
 * first ordering pass, as it applies the edges it records without applying them, and before that only the
 * readers that ask for pending edges see them. Every reader yields each edge once, so a consumer edge that is
 * also a pin counts once. The parent and dependent cursors are fields, used one at a time. The store belongs to
 * one SqlOptimiser, which belongs to one compiler, which serves one thread at a time.
 */
public final class OuterJoinOrderingEdges implements Mutable {
    private static final int DEPENDENTS_CONSUMER = 2;
    private static final int DEPENDENTS_DONE = 3;
    private static final int DEPENDENTS_PARTIAL = 1;
    private static final int DEPENDENTS_WHOLE_PREFIX = 0;
    // clear() gives the partial pin words back past this capacity, which no ordinary statement reaches: a
    // level of 2,048 joins that each leave a prefix model unpinned fills half of it
    private static final int PARTIAL_PIN_WORDS_TRIM_CAPACITY = 1 << 16;
    // bit c: the model at c consumes the rows of an earlier non-equi RIGHT/FULL join
    private final LongList consumerWords = new LongList();
    // bit r: the non-equi RIGHT/FULL join at r is a parent of every later consumer
    private final LongList nullingBoundaryWords = new LongList();
    // by block of 64 partial joins: the position of the first word of the block in partialPinWords
    private final IntList partialBlockStarts = new IntList();
    // by ordinal, in ascending order: the model index of the join that leaves some prefix models unpinned
    private final IntList partialBoundaries = new IntList();
    // by model index, sized on the first partial join of a level: the ordinal of the model's partial join, or -1
    private final IntList partialOrdinals = new IntList();
    // by ordinal: the number of prefix models that the join pins
    private final IntList partialPinCounts = new IntList();
    // Blocks of words, see partialBlockStarts: a block holds a word per parent model below the last join of the
    // block, and bit (ordinal & 63) of the word of parent p is set when the join of the ordinal pins p.
    private final LongList partialPinWords = new LongList();
    // bit b: the join at b pins every model before it
    private final LongList wholePrefixWords = new LongList();
    private long dependentBits;
    private int dependentBlock;
    private int dependentNext;
    private int dependentParent;
    private int dependentPhase;
    // the lowest model index of nullingBoundaryWords, or modelCount when it holds none
    private int firstNullingBoundary;
    // true once the level records a pin or a nulling boundary: the readers of a level without them return at once
    private boolean hasEdges;
    private boolean hasParentConsumerPhase;
    private boolean isConsumerEdgeInForce;
    private boolean isParentConsumerPhase;
    private boolean isParentDone;
    private boolean isParentWholePrefix;
    // the highest model index of consumerWords, or -1 when it holds none
    private int lastConsumer;
    private int modelCount;
    private int parentChild;
    private int parentNext;
    private int parentOrdinal;
    private long pinCount;
    private int wordCount;

    public OuterJoinOrderingEdges() {
        of(0);
    }

    public void addNullingJoinBoundary(int boundaryIndex) {
        setBit(nullingBoundaryWords, boundaryIndex);
        firstNullingBoundary = Math.min(firstNullingBoundary, boundaryIndex);
        hasEdges = true;
    }

    public void addNullingJoinConsumer(int consumerIndex) {
        setBit(consumerWords, consumerIndex);
        lastConsumer = Math.max(lastConsumer, consumerIndex);
    }

    @Override
    public void clear() {
        of(0);
        if (partialPinWords.capacity() > PARTIAL_PIN_WORDS_TRIM_CAPACITY) {
            partialPinWords.restoreInitialCapacity();
        }
    }

    @TestOnly
    public int getBufferCapacity() {
        return consumerWords.capacity() + nullingBoundaryWords.capacity() + partialBlockStarts.capacity()
                + partialBoundaries.capacity() + partialPinCounts.capacity() + partialPinWords.capacity()
                + partialOrdinals.capacity() + wholePrefixWords.capacity();
    }

    /**
     * Returns the number of edges in force: the pins and the consumer edges that are not pins.
     */
    public long getEdgeCount() {
        long count = pinCount;
        if (isConsumerEdgeInForce) {
            for (int c = nextSetBit(consumerWords, firstNullingBoundary + 1); c != -1; c = nextSetBit(consumerWords, c + 1)) {
                count += getConsumerParentCount(c);
            }
        }
        return count;
    }

    /**
     * Returns the number of parents in force of the model at childIndex.
     */
    public int getParentCount(int childIndex) {
        if (!hasEdges) {
            return 0;
        }
        if (isWholePrefixPinned(childIndex)) {
            return childIndex;
        }
        final int ordinal = getPartialOrdinal(childIndex);
        int count = ordinal != -1 ? partialPinCounts.getQuick(ordinal) : 0;
        if (isConsumerEdgeInForce && isBitSet(consumerWords, childIndex)) {
            count += getConsumerParentCount(childIndex);
        }
        return count;
    }

    @TestOnly
    public int getPartialPinWordCapacity() {
        return partialPinWords.capacity();
    }

    @TestOnly
    public int getPartialPinWordCount() {
        return partialPinWords.size();
    }

    @TestOnly
    public long getPinCount() {
        return pinCount;
    }

    /**
     * Returns true when the model at parentIndex has a dependent in force.
     */
    public boolean hasDependents(int parentIndex) {
        if (!hasEdges) {
            return false;
        }
        if (nextSetBit(wholePrefixWords, parentIndex + 1) != -1) {
            return true;
        }
        for (int block = 0, n = partialBlockStarts.size(); block < n; block++) {
            if (parentIndex < getPartialBlockLength(block) && partialPinWords.getQuick(partialBlockStarts.getQuick(block) + parentIndex) != 0) {
                return true;
            }
        }
        return isConsumerEdgeInForce && isBitSet(nullingBoundaryWords, parentIndex) && lastConsumer > parentIndex;
    }

    public boolean hasNullingJoinConsumerAfter(int boundaryIndex) {
        return lastConsumer > boundaryIndex;
    }

    /**
     * Returns true when the model at childIndex has a parent in force or, with isPendingIncluded, a pending one.
     */
    public boolean hasParents(int childIndex, boolean isPendingIncluded) {
        return hasEdges && (isWholePrefixPinned(childIndex)
                || getPartialOrdinal(childIndex) != -1
                || ((isConsumerEdgeInForce || isPendingIncluded) && isBitSet(consumerWords, childIndex) && firstNullingBoundary < childIndex));
    }

    /**
     * Returns true when the store holds the edge from parentIndex to childIndex in force or, with
     * isPendingIncluded, pending.
     */
    public boolean isEdge(int parentIndex, int childIndex, boolean isPendingIncluded) {
        if (!hasEdges || parentIndex >= childIndex) {
            return false;
        }
        if (isWholePrefixPinned(childIndex)) {
            return true;
        }
        final int ordinal = getPartialOrdinal(childIndex);
        return (ordinal != -1 && isPartialPin(parentIndex, ordinal))
                || ((isConsumerEdgeInForce || isPendingIncluded) && isBitSet(consumerWords, childIndex) && isBitSet(nullingBoundaryWords, parentIndex));
    }

    public boolean isWholePrefixPinned(int boundaryIndex) {
        return isBitSet(wholePrefixWords, boundaryIndex);
    }

    /**
     * Returns the next dependent in force of the model that {@link #ofDependents(int)} names, or -1 after the
     * last one. The dependents of the pins come first, the joins that pin the whole prefix in ascending order,
     * then the others, followed by the consumers that the model is not a pin of.
     */
    public int nextDependent() {
        if (dependentPhase == DEPENDENTS_WHOLE_PREFIX) {
            final int boundaryIndex = nextSetBit(wholePrefixWords, dependentNext);
            if (boundaryIndex != -1) {
                dependentNext = boundaryIndex + 1;
                return boundaryIndex;
            }
            dependentPhase = DEPENDENTS_PARTIAL;
        }
        if (dependentPhase == DEPENDENTS_PARTIAL) {
            while (dependentBits == 0 && ++dependentBlock < partialBlockStarts.size()) {
                if (dependentParent < getPartialBlockLength(dependentBlock)) {
                    dependentBits = partialPinWords.getQuick(partialBlockStarts.getQuick(dependentBlock) + dependentParent);
                }
            }
            if (dependentBits != 0) {
                final int ordinal = (dependentBlock << 6) + Long.numberOfTrailingZeros(dependentBits);
                dependentBits &= dependentBits - 1;
                return partialBoundaries.getQuick(ordinal);
            }
            dependentPhase = isConsumerEdgeInForce && isBitSet(nullingBoundaryWords, dependentParent) ? DEPENDENTS_CONSUMER : DEPENDENTS_DONE;
            dependentNext = dependentParent + 1;
        }
        if (dependentPhase == DEPENDENTS_CONSUMER) {
            for (int c = nextSetBit(consumerWords, dependentNext); c != -1; c = nextSetBit(consumerWords, c + 1)) {
                if (!isPin(dependentParent, c)) {
                    dependentNext = c + 1;
                    return c;
                }
            }
            dependentPhase = DEPENDENTS_DONE;
        }
        return -1;
    }

    /**
     * Returns the next parent of the model that {@link #ofParents(int, boolean)} names, or -1 after the last
     * one: the pins in ascending order, then the consumer edges that are not pins.
     */
    public int nextParent() {
        if (isParentDone) {
            return -1;
        }
        if (!isParentConsumerPhase) {
            if (isParentWholePrefix) {
                return parentNext < parentChild ? parentNext++ : -1;
            }
            if (parentOrdinal != -1) {
                while (parentNext < parentChild) {
                    final int parentIndex = parentNext++;
                    if (isPartialPin(parentIndex, parentOrdinal)) {
                        return parentIndex;
                    }
                }
            }
            if (!hasParentConsumerPhase) {
                return -1;
            }
            isParentConsumerPhase = true;
            parentNext = 0;
        }
        for (int r = nextSetBit(nullingBoundaryWords, parentNext); r != -1 && r < parentChild; r = nextSetBit(nullingBoundaryWords, r + 1)) {
            if (parentOrdinal == -1 || !isPartialPin(r, parentOrdinal)) {
                parentNext = r + 1;
                return r;
            }
        }
        parentNext = parentChild;
        return -1;
    }

    /**
     * Starts the edges of a join level of modelCount models. A level that records no edge writes to no buffer.
     */
    public void of(int modelCount) {
        this.modelCount = modelCount;
        wordCount = (modelCount + 63) >>> 6;
        consumerWords.clear();
        nullingBoundaryWords.clear();
        partialBlockStarts.clear();
        partialBoundaries.clear();
        partialPinCounts.clear();
        partialPinWords.clear();
        partialOrdinals.clear();
        wholePrefixWords.clear();
        firstNullingBoundary = modelCount;
        hasEdges = false;
        isConsumerEdgeInForce = false;
        lastConsumer = -1;
        pinCount = 0;
    }

    /**
     * Starts reading the dependents in force of the model at parentIndex, see {@link #nextDependent()}.
     */
    public void ofDependents(int parentIndex) {
        dependentParent = parentIndex;
        dependentNext = parentIndex + 1;
        dependentPhase = hasEdges ? DEPENDENTS_WHOLE_PREFIX : DEPENDENTS_DONE;
        dependentBlock = -1;
        dependentBits = 0;
    }

    /**
     * Starts reading the parents in force of the model at childIndex or, with isPendingIncluded, its pending
     * parents too, see {@link #nextParent()}.
     */
    public void ofParents(int childIndex, boolean isPendingIncluded) {
        isParentDone = !hasEdges;
        if (hasEdges) {
            parentChild = childIndex;
            parentNext = 0;
            isParentWholePrefix = isWholePrefixPinned(childIndex);
            parentOrdinal = getPartialOrdinal(childIndex);
            isParentConsumerPhase = false;
            // the whole prefix holds every consumer edge into the model
            hasParentConsumerPhase = !isParentWholePrefix && (isConsumerEdgeInForce || isPendingIncluded) && isBitSet(consumerWords, childIndex);
        }
    }

    /**
     * Pins the models before the join at boundaryIndex except the ones in unpinned, which holds only models
     * before the join. Returns false when unpinned holds every model before the join, which leaves the join
     * without pins. Joins that leave some models unpinned come in ascending order of their index.
     */
    public boolean pinPrefix(int boundaryIndex, IntHashSet unpinned) {
        final int unpinnedCount = unpinned.size();
        if (unpinnedCount == 0) {
            pinWholePrefix(boundaryIndex);
            return true;
        }
        if (unpinnedCount == boundaryIndex) {
            return false;
        }
        assert unpinnedCount < boundaryIndex && !isWholePrefixPinned(boundaryIndex);
        if (partialOrdinals.size() == 0) {
            partialOrdinals.setAll(modelCount, -1);
        }
        final int ordinal = partialBoundaries.size();
        assert ordinal == 0 || partialBoundaries.getLast() < boundaryIndex;
        partialOrdinals.setQuick(boundaryIndex, ordinal);
        partialBoundaries.add(boundaryIndex);
        partialPinCounts.add(boundaryIndex - unpinnedCount);
        // The block of the ordinal is the last block, which holds a word per model below the last join of the
        // block so far, a join before this one: the block grows by the words of the models from that join on.
        final int blockStart;
        final int blockEnd = partialPinWords.size();
        if ((ordinal & 63) == 0) {
            blockStart = blockEnd;
            partialBlockStarts.add(blockStart);
        } else {
            blockStart = partialBlockStarts.getQuick(ordinal >>> 6);
        }
        assert blockStart + boundaryIndex > blockEnd;
        partialPinWords.setPos(blockStart + boundaryIndex);
        partialPinWords.fill(blockEnd, blockStart + boundaryIndex, 0);
        hasEdges = true;
        final long bit = 1L << ordinal;
        for (int parentIndex = 0; parentIndex < boundaryIndex; parentIndex++) {
            if (unpinned.excludes(parentIndex)) {
                final int wordIndex = blockStart + parentIndex;
                partialPinWords.setQuick(wordIndex, partialPinWords.getQuick(wordIndex) | bit);
            }
        }
        pinCount += boundaryIndex - unpinnedCount;
        return true;
    }

    /**
     * Pins every model before the join at boundaryIndex. Pinning a join again changes nothing.
     */
    public void pinWholePrefix(int boundaryIndex) {
        assert boundaryIndex > 0 && getPartialOrdinal(boundaryIndex) == -1;
        if (!isWholePrefixPinned(boundaryIndex)) {
            setBit(wholePrefixWords, boundaryIndex);
            pinCount += boundaryIndex;
            hasEdges = true;
        }
    }

    public void putConsumerEdgesInForce() {
        isConsumerEdgeInForce = true;
    }

    // the number of set bits below index
    private static int countBitsBelow(LongList words, int index) {
        if (words.size() == 0) {
            return 0;
        }
        int count = 0;
        final int lastWord = index >>> 6;
        for (int w = 0; w < lastWord; w++) {
            count += Long.bitCount(words.getQuick(w));
        }
        if ((index & 63) != 0) {
            count += Long.bitCount(words.getQuick(lastWord) & ((1L << index) - 1));
        }
        return count;
    }

    private static boolean isBitSet(LongList words, int index) {
        return words.size() > 0 && (words.getQuick(index >>> 6) & (1L << index)) != 0;
    }

    // the lowest set bit at or after from, or -1
    private static int nextSetBit(LongList words, int from) {
        int w = from >>> 6;
        final int n = words.size();
        if (w >= n) {
            return -1;
        }
        long word = words.getQuick(w) & (-1L << from);
        while (word == 0) {
            if (++w == n) {
                return -1;
            }
            word = words.getQuick(w);
        }
        return (w << 6) + Long.numberOfTrailingZeros(word);
    }

    // the number of consumer edges in force into the consumer at childIndex that are not pins
    private int getConsumerParentCount(int childIndex) {
        if (isWholePrefixPinned(childIndex)) {
            return 0;
        }
        final int ordinal = getPartialOrdinal(childIndex);
        if (ordinal == -1) {
            return countBitsBelow(nullingBoundaryWords, childIndex);
        }
        int count = 0;
        for (int r = nextSetBit(nullingBoundaryWords, 0); r != -1 && r < childIndex; r = nextSetBit(nullingBoundaryWords, r + 1)) {
            if (!isPartialPin(r, ordinal)) {
                count++;
            }
        }
        return count;
    }

    private int getPartialBlockLength(int block) {
        final int end = block + 1 < partialBlockStarts.size() ? partialBlockStarts.getQuick(block + 1) : partialPinWords.size();
        return end - partialBlockStarts.getQuick(block);
    }

    private int getPartialOrdinal(int boundaryIndex) {
        return partialOrdinals.size() > 0 ? partialOrdinals.getQuick(boundaryIndex) : -1;
    }

    // parentIndex is before the join of the ordinal
    private boolean isPartialPin(int parentIndex, int ordinal) {
        return (partialPinWords.getQuick(partialBlockStarts.getQuick(ordinal >>> 6) + parentIndex) & (1L << ordinal)) != 0;
    }

    // parentIndex is before childIndex
    private boolean isPin(int parentIndex, int childIndex) {
        if (isWholePrefixPinned(childIndex)) {
            return true;
        }
        final int ordinal = getPartialOrdinal(childIndex);
        return ordinal != -1 && isPartialPin(parentIndex, ordinal);
    }

    private void setBit(LongList words, int index) {
        if (words.size() == 0) {
            words.seed(wordCount, 0);
        }
        final int w = index >>> 6;
        words.setQuick(w, words.getQuick(w) | (1L << index));
    }
}

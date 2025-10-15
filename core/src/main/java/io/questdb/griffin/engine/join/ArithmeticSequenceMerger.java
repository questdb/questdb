/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.griffin.engine.join;

import io.questdb.std.LongList;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ThreadLocalRandom;

public class ArithmeticSequenceMerger {

    private int elementCount;
    private long step;

    public static void main(String[] args) {
        int lhsCount = 100_000;
        int rhsCount = 1201;
        LongList initialValues = createInitialValues(lhsCount);

        for (int i = 0; i < 3; i++) {
            long start = System.nanoTime();
            ArithmeticSequenceMerger merger = new ArithmeticSequenceMerger();
            LongList list = merger.mergeSequences(initialValues, 10, rhsCount);
            System.out.println("Time: " + (System.nanoTime() - start) / 1_000_000 + " ms");
            assertCorrectness(list, lhsCount, rhsCount);
        }

    }

    public LongList mergeSequences(LongList initialValues, long step, int elementCount) {
        this.step = step;
        this.elementCount = elementCount;
        if (initialValues == null || initialValues.size() == 0) {
            return new LongList(0);
        }
        final LongList result = new LongList(initialValues.size() * elementCount);
        int initialValueIndex = 0;
        SequenceIterator iter = new SequenceIterator(initialValues.getQuick(0));
        SequenceIterator prevIter = iter;
        while (true) {
            long value = iter.next();
            result.add(value);
            SequenceIterator nextIter = iter.nextIterator();
            if (initialValueIndex != initialValues.size() - 1) {
                // There's more iterators waiting to be used. Let's see if it's time to activate the next one.
                long nextIterValue = nextIter.peekNext();
                if (nextIterValue != -1) {
                    long nextInitValue = initialValues.getQuick(initialValueIndex + 1);
                    if (nextInitValue < nextIterValue) {
                        initialValueIndex++;
                        nextIter = iter.insertAfter(nextInitValue);
                    }
                } else {
                    // This only happens when nextIter == iter, it's the last one in the circular list, and
                    // it's exhausted. Completely ignore its existence and re-initialize everything
                    // to the new, and now only, iterator.
                    nextIter = new SequenceIterator(initialValues.getQuick(++initialValueIndex));
                    prevIter = iter = nextIter;
                }
            }
            if (iter.isEmpty()) {
                // We keep prevIter just to be able to remove the current iter. Without it,
                // we wouldn't have at hand the iterator whose nextIterator we must update.
                nextIter = prevIter.removeNextIterator();
                if (nextIter == null) {
                    break;
                }
            } else {
                prevIter = iter;
            }
            iter = nextIter;
        }
        return result;
    }

    private static void assertCorrectness(LongList list, int lhsCount, int rhsCount) {
        if (list.size() != lhsCount * rhsCount) {
            throw new AssertionError("Output size, expected: " + lhsCount * rhsCount + ", Actual: " + list.size());
        }
        long prev = -1;
        for (int i = 0; i < list.size(); i++) {
            long curr = list.getQuick(i);
            if (curr < prev) {
                throw new AssertionError("List not sorted");
            }
            prev = curr;
        }
    }

    private static @NotNull LongList createInitialValues(int lhsCount) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        LongList initialValues = new LongList(lhsCount);
        for (int i = 0, val = 1; i < lhsCount; i++) {
            initialValues.add(val);
            val += rnd.nextInt(20);
        }
        return initialValues;
    }

    class SequenceIterator {
        private SequenceIterator nextIterator;
        private long nextValue;
        private long remainingCount;

        SequenceIterator(long initialValue) {
            this.nextValue = initialValue;
            this.remainingCount = elementCount;
            this.nextIterator = this;
        }

        SequenceIterator insertAfter(long initialValue) {
            SequenceIterator iter = new SequenceIterator(initialValue);
            iter.nextIterator = this.nextIterator;
            this.nextIterator = iter;
            return iter;
        }

        boolean isEmpty() {
            return remainingCount == 0;
        }

        long next() {
            long result = nextValue;
            if (result != -1) {
                remainingCount--;
                if (remainingCount == 0) {
                    nextValue = -1;
                } else {
                    nextValue += step;
                }
            }
            return result;
        }

        SequenceIterator nextIterator() {
            return nextIterator;
        }

        long peekNext() {
            return nextValue;
        }

        SequenceIterator removeNextIterator() {
            if (nextIterator == this) {
                return null;
            }
            nextIterator = nextIterator.nextIterator;
            return nextIterator;
        }
    }
}

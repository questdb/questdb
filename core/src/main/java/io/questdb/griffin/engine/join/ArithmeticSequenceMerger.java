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

        long start = System.nanoTime();
        ArithmeticSequenceMerger merger = new ArithmeticSequenceMerger();
        LongList list = merger.mergeSequences(initialValues, 10, rhsCount);
        System.out.println("Time: " + (System.nanoTime() - start) / 1_000_000 + " ms");

        assertCorrectness(list, lhsCount, rhsCount);
    }

    public LongList mergeSequences(LongList initialValues, long step, int elementCount) {
        this.step = step;
        this.elementCount = elementCount;
        if (initialValues == null || initialValues.size() == 0) {
            return new LongList(0);
        }
        final LongList result = new LongList(initialValues.size() * elementCount);
        int initialValueIndex = 0;
        CircularListNode node = new CircularListNode(initialValues.getQuick(0));
        while (true) {
            long value = node.iterator.next();
            result.add(value);
            CircularListNode nextNode = node.next();
            if (initialValueIndex != initialValues.size() - 1) {
                long nextIterValue = nextNode.iterator.peekNext();
                long nextInitValue = initialValues.getQuick(initialValueIndex + 1);
                if (nextInitValue < nextIterValue) {
                    initialValueIndex++;
                    nextNode = node.insertAfter(nextInitValue);
                }
            }
            if (node.iterator.isEmpty()) {
                nextNode = node.remove();
                if (nextNode == null) {
                    break;
                }
            }
            node = nextNode;
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

    class CircularListNode {
        private final long initialValue;
        SequenceIterator iterator;
        private CircularListNode next;
        private CircularListNode prev;

        CircularListNode(long initialValue) {
            this.iterator = new SequenceIterator(initialValue);
            next = prev = this;
            this.initialValue = initialValue;
        }

        @Override
        public String toString() {
            return "[" +
                    initialValue + ',' +
                    iterator.remainingCount + ',' +
                    iterator.peekNext() +
                    ']';
        }

        CircularListNode insertAfter(long initialValue) {
            CircularListNode node = new CircularListNode(initialValue);
            node.next = this.next;
            this.next.prev = node;
            node.prev = this;
            this.next = node;
            return node;
        }

        CircularListNode next() {
            return next;
        }

        CircularListNode remove() {
            if (next == this) {
                return null;
            }
            prev.next = next;
            next.prev = prev;
            return next;
        }
    }

    class SequenceIterator {
        long nextValue;
        long remainingCount;

        SequenceIterator(long initialValue) {
            this.nextValue = initialValue;
            this.remainingCount = elementCount;
        }

        boolean isEmpty() {
            return remainingCount <= 0;
        }

        long next() {
            if (remainingCount == 0) {
                return -1;
            }
            long result = nextValue;
            nextValue += step;
            remainingCount--;
            return result;
        }

        long peekNext() {
            return nextValue;
        }
    }
}

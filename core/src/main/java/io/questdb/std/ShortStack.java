/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.std;

import java.util.Arrays;

public class ShortStack {
    private static final int DEFAULT_INITIAL_CAPACITY = 16;
    private static final int MIN_INITIAL_CAPACITY = 8;
    private static final short noEntryValue = -1;
    private short[] elements;
    private int head;
    private int mask;
    private int tail;

    public ShortStack() {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    public ShortStack(int initialCapacity) {
        allocateElements(initialCapacity);
    }

    public void clear() {
        if (head != tail) {
            head = tail = 0;
            Arrays.fill(elements, noEntryValue);
        }
    }

    public boolean notEmpty() {
        return head != tail;
    }

    public short pop() {
        int h = head;
        short result = elements[h];
        if (result == noEntryValue) {
            return noEntryValue;
        }
        elements[h] = noEntryValue;
        head = (h + 1) & mask;
        return result;
    }

    public void push(short e) {
        elements[head = (head - 1) & mask] = e;
        if (head == tail) {
            doubleCapacity();
        }
    }

    public int size() {
        return (tail - head) & mask;
    }

    private void allocateElements(int capacity) {
        capacity = capacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(capacity);
        elements = new short[capacity];
        mask = capacity - 1;
    }

    private void doubleCapacity() {
        assert head == tail;
        int h = head;
        int n = elements.length;
        int r = n - h;
        int newCapacity = n << 1;
        if (newCapacity < 0) {
            throw new IllegalStateException("Stack is too big");
        }
        short[] next = new short[newCapacity];
        System.arraycopy(elements, h, next, 0, r);
        System.arraycopy(elements, 0, next, r, h);
        Arrays.fill(next, r + h, newCapacity, noEntryValue);
        elements = next;
        head = 0;
        tail = n;
        mask = newCapacity - 1;
    }
}

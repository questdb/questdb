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

package io.questdb.std;

import java.util.Arrays;

public class IntStack implements Mutable {
    private static final int DEFAULT_INITIAL_CAPACITY = 16;
    private static final int MIN_INITIAL_CAPACITY = 8;
    private static final int NO_ENTRY_VALUE = -1;
    private int bottom;
    private int[] elements;
    private int head;
    private int mask;
    private int tail;

    public IntStack() {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    public IntStack(int initialCapacity) {
        allocateElements(initialCapacity);
    }

    public int bottom() {
        return bottom;
    }

    public void clear() {
        if (head != tail) {
            head = tail = 0;
            Arrays.fill(elements, NO_ENTRY_VALUE);
            bottom = 0;
        }
    }

    public boolean notEmpty() {
        return size() > 0;
    }

    public int peek() {
        return peek(0);
    }

    public int peek(int n) {
        return n < size() ? elements[(head + n) & mask] : NO_ENTRY_VALUE;
    }

    public int pollLast() {
        if (bottom != 0) {
            throw new IllegalStateException("pollLast() called while bottom != 0");
        }
        final int[] elems = elements;
        int newTail = tail;
        if (head != newTail && --newTail < 0) {
            newTail = mask;
        }
        final int elem = elems[newTail];
        tail = newTail;
        elems[newTail] = NO_ENTRY_VALUE;
        return elem;
    }

    public int pop() {
        if (size() == 0) {
            return NO_ENTRY_VALUE;
        }
        int h = head;
        int result = elements[h];
        if (result == NO_ENTRY_VALUE) {
            return NO_ENTRY_VALUE;
        }
        elements[h] = NO_ENTRY_VALUE;
        head = (h + 1) & mask;
        return result;
    }

    public void popAll() {
        int h = head;
        while (((tail - h) & mask) > bottom) {
            elements[h] = NO_ENTRY_VALUE;
            h = (h + 1) & mask;
        }
        head = h;
    }

    public void push(int e) {
        elements[head = (head - 1) & mask] = e;
        if (head == tail) {
            doubleCapacity();
        }
    }

    public void setBottom(int bottom) {
        if (bottom <= sizeRaw()) {
            this.bottom = bottom;
        } else {
            throw new IllegalStateException("Tried to set bottom beyond the top of the stack");
        }
    }

    public int size() {
        return sizeRaw() - bottom;
    }

    public int sizeRaw() {
        return (tail - head) & mask;
    }

    public void update(int e) {
        elements[head] = e;
    }

    private void allocateElements(int capacity) {
        capacity = capacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(capacity);
        elements = new int[capacity];
        mask = capacity - 1;
        Arrays.fill(elements, NO_ENTRY_VALUE);
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
        int[] next = new int[newCapacity];
        System.arraycopy(elements, h, next, 0, r);
        System.arraycopy(elements, 0, next, r, h);
        Arrays.fill(next, r + h, newCapacity, NO_ENTRY_VALUE);
        elements = next;
        head = 0;
        tail = n;
        mask = newCapacity - 1;
    }
}

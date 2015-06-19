/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.nfsdb.collections;


import com.nfsdb.utils.Numbers;
import com.nfsdb.utils.Unsafe;

import java.util.Arrays;

public class IntStack {
    private static final int noEntryValue = -1;
    private static final int MIN_INITIAL_CAPACITY = 8;
    private int[] elements;
    private int head;
    private int tail;
    private int mask;

    public IntStack() {
        allocateElements(16);
    }

    public void clear() {
        if (head != tail) {
            head = tail = 0;
            Arrays.fill(elements, noEntryValue);
        }
    }

    public boolean contains(int o) {
        if (o == noEntryValue) {
            return false;
        }
        int i = head;
        int x;
        while ((x = Unsafe.arrayGet(elements, i)) != noEntryValue) {
            if (o == x) {
                return true;
            }
            i = (i + 1) & mask;
        }
        return false;
    }

    public boolean notEmpty() {
        return head != tail;
    }

    public int peek() {
        return Unsafe.arrayGet(elements, head);
    }

    public int pop() {
        int h = head;
        int result = Unsafe.arrayGet(elements, h);
        if (result == noEntryValue) {
            return noEntryValue;
        }
        Unsafe.arrayPut(elements, h, noEntryValue);
        head = (h + 1) & mask;
        return result;
    }

    public void push(int e) {
        Unsafe.arrayPut(elements, head = (head - 1) & mask, e);
        if (head == tail) {
            doubleCapacity();
        }
    }

    public int size() {
        return (tail - head) & mask;
    }

    public void update(int e) {
        elements[head] = e;
    }

    private void allocateElements(int capacity) {
        capacity = capacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(capacity);
        elements = new int[capacity];
        mask = capacity - 1;
    }

    private void doubleCapacity() {
        assert head == tail;
        int p = head;
        int n = elements.length;
        int r = n - p;
        int newCapacity = n << 1;
        if (newCapacity < 0) {
            throw new IllegalStateException("Stack is too big");
        }
        int[] a = new int[newCapacity];
        System.arraycopy(elements, p, a, 0, r);
        System.arraycopy(elements, 0, a, r, p);
        elements = a;
        head = 0;
        tail = n;
        mask = newCapacity - 1;
    }
}

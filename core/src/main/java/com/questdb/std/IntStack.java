/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.std;


import java.util.Arrays;

public class IntStack implements Mutable {
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

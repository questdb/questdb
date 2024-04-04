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

package io.questdb.std;

import java.util.Arrays;

public class ObjStack<T> implements Mutable {
    private static final int DEFAULT_INITIAL_CAPACITY = 16;
    private static final int MIN_INITIAL_CAPACITY = 8;
    private T[] elements;
    private int head;
    private int mask;
    private int tail;

    public ObjStack() {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    public ObjStack(int initialCapacity) {
        allocateElements(initialCapacity);
    }

    public void clear() {
        if (head != tail) {
            head = tail = 0;
            Arrays.fill(elements, null);
        }
    }

    public boolean notEmpty() {
        return head != tail;
    }

    public T peek() {
        return elements[head];
    }

    public T peek(int n) {
        return elements[(head + n) & mask];
    }

    public T pop() {
        final int h = head;
        final T result = elements[h];
        if (result == null) {
            return null;
        }
        elements[h] = null;
        head = (h + 1) & mask;
        return result;
    }

    public void push(T e) {
        elements[head = (head - 1) & mask] = e;
        if (head == tail) {
            doubleCapacity();
        }
    }

    public int size() {
        return (tail - head) & mask;
    }

    public void update(T e) {
        elements[head] = e;
    }

    @SuppressWarnings("unchecked")
    private void allocateElements(int capacity) {
        capacity = capacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(capacity);
        elements = (T[]) new Object[capacity];
        mask = capacity - 1;
    }

    @SuppressWarnings("unchecked")
    private void doubleCapacity() {
        assert head == tail;
        int h = head;
        int n = elements.length;
        int r = n - h;
        int newCapacity = n << 1;
        if (newCapacity < 0) {
            throw new IllegalStateException("Stack is too big");
        }
        T[] next = (T[]) new Object[newCapacity];
        System.arraycopy(elements, h, next, 0, r);
        System.arraycopy(elements, 0, next, r, h);
        Arrays.fill(next, r + h, newCapacity, null);
        elements = next;
        head = 0;
        tail = n;
        mask = newCapacity - 1;
    }
}
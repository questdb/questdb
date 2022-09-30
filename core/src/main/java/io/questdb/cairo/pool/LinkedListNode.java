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

package io.questdb.cairo.pool;

import org.jetbrains.annotations.NotNull;

/**
 * Node for an (intrusive) linked list.
 * The use case is for callbacks to objects that then expose the next handler.
 * In general a concrete node type would both:
 *   * Extend `LinkedListNode<SomeNodeType>` and
 *   * Implement/extend `SomeNodeType`.
 *
 * In such case, the implementation for `getValue()` is simply:
 *     public SomeNodeType getValue() { return this; }
 */
public abstract class LinkedListNode<T> {
    private LinkedListNode<T> next = null;

    @NotNull
    public abstract T getValue();

    /** Get the next element in the list. May return `null`. */
    public LinkedListNode<T> getNext() {
        return this.next;
    }

    public synchronized LinkedListNode<T> setNext(LinkedListNode<T> next) {
        this.next = next;
        return this;
    }

    public static <V> LinkedListNode<V> fromValue(V value) {
        return new LinkedListNode<V>() {
            @Override
            public V getValue() {
                return value;
            }
        };
    }
}

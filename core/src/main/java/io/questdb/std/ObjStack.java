/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

public class ObjStack<T> implements Mutable {
    private final ObjArrayDequeue<T> dequeue;

    public ObjStack() {
        dequeue = new ObjArrayDequeue<>();
    }

    public ObjStack(int initialCapacity) {
        dequeue = new ObjArrayDequeue<>(initialCapacity);
    }

    public void clear() {
        dequeue.clear();
    }

    public boolean notEmpty() {
        return dequeue.notEmpty();
    }

    public T peek() {
        return dequeue.peekLast();
    }

    public T peek(int n) {
        return dequeue.peekLast(n);
    }

    public T pop() {
        return dequeue.popLast();
    }

    public void push(T e) {
        dequeue.push(e);
    }

    public int size() {
        return dequeue.size();
    }

    public void update(T e) {
        dequeue.update(e);
    }
}

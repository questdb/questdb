/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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
 */

package com.nfsdb.journal.collections;

import com.nfsdb.journal.utils.Unsafe;

public class DirectLongList extends AbstractDirectList {
    public DirectLongList() {
        super(3, 10);
    }

    public DirectLongList(int capacity) {
        super(3, capacity);
    }

    public void add(int x) {
        ensureCapacity();
        Unsafe.getUnsafe().putInt(pos, x);
        pos += 8;
    }

    public int get(int p) {
        return Unsafe.getUnsafe().getInt(start + (p << 3));
    }
}

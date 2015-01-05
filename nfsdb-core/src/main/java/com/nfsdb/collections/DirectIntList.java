/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.collections;

import com.nfsdb.utils.Unsafe;

public class DirectIntList extends AbstractDirectList {

    public DirectIntList() {
        super(2, 10);
    }

    public DirectIntList(long capacity) {
        super(2, capacity);
    }

    public void add(int x) {
        ensureCapacity();
        Unsafe.getUnsafe().putInt(pos, x);
        pos += 4;
    }

    public int get(long p) {
        return Unsafe.getUnsafe().getInt(start + (p << 2));
    }

    public void set(long p, int v) {
        Unsafe.getUnsafe().putInt(start + (p << 2), v);
    }

    public void extendAndSet(long p, int v) {
        setCapacity(p + 1);
        set(p, v);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (int i = 0; i < size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(get(i));
        }
        sb.append("}");
        return sb.toString();
    }
}

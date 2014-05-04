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

import gnu.trove.list.array.TLongArrayList;

public class LongArrayList extends TLongArrayList {

    public void setCapacity(int capacity) {
        if (capacity > _data.length) {
            long[] tmp = new long[capacity];
            System.arraycopy(_data, 0, tmp, 0, _data.length);
            _data = tmp;
        }
    }

    public void add(LongArrayList that) {
        int sz = that.size();
        setCapacity(sz);
        add(that._data, 0, sz);
    }

    public void setPos(int pos) {
        _pos = pos;
    }
}
